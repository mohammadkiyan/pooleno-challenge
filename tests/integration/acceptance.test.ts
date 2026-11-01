import { randomUUID } from 'node:crypto';
import { request as httpRequest } from 'node:http';
import type { AddressInfo } from 'node:net';
import type { NestFastifyApplication } from '@nestjs/platform-fastify';
import type { Pool, PoolClient } from 'pg';
import { pino } from 'pino';
import { Value } from 'typebox/value';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { buildApi } from '../../src/app/build-api.js';
import { createPool } from '../../src/infrastructure/db.js';
import { OrderAcceptedV1 } from '../../src/modules/messaging/order-accepted.schema.js';
import { canonicalizeOrder } from '../../src/modules/orders/canonicalize-order.js';
import { claimKey } from '../../src/modules/orders/idempotency.js';
import { fingerprintOrder } from '../../src/modules/orders/order-fingerprint.js';
import { insertOrder } from '../../src/modules/orders/order.repository.js';
import { postOrderRequest, uniqueKey, validOrder, type AcceptedBody } from '../support/orders.js';
import {
  countRows,
  readCounter,
  resetDatabase,
  TEST_DATABASE_URL,
  waitForBlockedBackend,
} from '../support/test-database.js';

const log = pino({ level: 'silent' });
const SHORT_LOCK_TIMEOUT_MS = 100;
const NO_ROWS = { orders: 0, receipts: 0, events: 0 };
const NO_KAFKA = () => Promise.resolve({ available: false as const });

let pool: Pool;
let app: NestFastifyApplication;

beforeAll(async () => {
  pool = createPool(TEST_DATABASE_URL, log);
  app = await buildApi({ databaseUrl: TEST_DATABASE_URL, log, lockTimeoutMs: 1_000, readKafkaLag: NO_KAFKA });
});

afterAll(async () => {
  await app.close();
  await pool.end();
});

beforeEach(() => resetDatabase(pool));

/** Claims a key in a transaction left open, as a concurrent request would mid-flight. */
async function holdKey(key: string): Promise<{ holder: PoolClient; orderId: string }> {
  const order = canonicalizeOrder(validOrder);
  const orderId = randomUUID();
  const location = `/v1/orders/${orderId}`;
  const holder = await pool.connect();

  await holder.query('BEGIN');
  await claimKey(holder, {
    scope: 'demo',
    idempotencyKey: key,
    fingerprint: fingerprintOrder('demo', order),
    orderId,
    receipt: { status: 202, location, body: { orderId, status: 'QUEUED', statusUrl: location } },
  });
  await insertOrder(holder, {
    ...order,
    id: orderId,
    scope: 'demo',
    acceptanceSeq: 1n,
    acceptedAt: new Date(),
  });
  return { holder, orderId };
}

async function endTransaction(holder: PoolClient, command: 'COMMIT' | 'ROLLBACK'): Promise<void> {
  await holder.query(command);
  holder.release();
}

describe('POST /v1/orders acceptance', () => {
  it('commits the order, receipt, counter and event together before answering 202', async () => {
    const response = await app.inject(postOrderRequest(uniqueKey()));

    expect(response.statusCode).toBe(202);
    const body = response.json<AcceptedBody>();
    expect(body).toEqual({
      orderId: expect.any(String),
      status: 'QUEUED',
      statusUrl: `/v1/orders/${body.orderId}`,
    });
    expect(response.headers.location).toBe(`/v1/orders/${body.orderId}`);

    expect(await countRows(pool)).toEqual({ orders: 1, receipts: 1, events: 1 });
    expect(await readCounter(pool, 'BTC-USD')).toBe('1');
  });

  it('writes an Outbox event that matches the OrderAccepted v1 contract', async () => {
    const response = await app.inject(postOrderRequest(uniqueKey()));
    const { orderId } = response.json<AcceptedBody>();

    const { rows } = await pool.query<{ payload: OrderAcceptedV1; published_at: Date | null }>(
      'SELECT payload, published_at FROM outbox_events WHERE order_id = $1',
      [orderId],
    );
    const event = rows[0];

    expect(Value.Check(OrderAcceptedV1, event?.payload)).toBe(true);
    expect(event?.published_at).toBeNull();
    expect(event?.payload).toMatchObject({
      eventType: 'OrderAccepted',
      schemaVersion: 1,
      orderId,
      symbol: 'BTC-USD',
      acceptanceSequence: '1',
      side: 'BUY',
      orderType: 'LIMIT',
      priceUnits: '10000',
      quantityUnits: '250000000',
      correlationId: expect.any(String),
    });
  });

  it('does not reorder the keys of the stored receipt', async () => {
    const key = uniqueKey();
    await app.inject(postOrderRequest(key));

    const { rows } = await pool.query<{ body: string }>(
      'SELECT response_body::text AS body FROM idempotency_requests WHERE idempotency_key = $1',
      [key],
    );

    expect(Object.keys(JSON.parse(rows[0]?.body ?? '{}') as object)).toEqual([
      'orderId',
      'status',
      'statusUrl',
    ]);
  });
});

describe('idempotent replay', () => {
  it('replays the receipt for an equivalent request written differently', async () => {
    const key = uniqueKey();
    const first = await app.inject(postOrderRequest(key));
    const second = await app.inject(
      postOrderRequest(key, {
        quantity: '2.5',
        price: '100',
        type: 'LIMIT',
        side: 'BUY',
        symbol: 'BTC-USD',
      }),
    );

    expect(second.statusCode).toBe(202);
    expect(second.body).toBe(first.body);
    expect(second.headers.location).toBe(first.headers.location);
    expect(await countRows(pool)).toEqual({ orders: 1, receipts: 1, events: 1 });
    expect(await readCounter(pool, 'BTC-USD')).toBe('1');
  });

  it('replays the original 202 after the order has moved on, while GET shows current state', async () => {
    const key = uniqueKey();
    const first = await app.inject(postOrderRequest(key));
    const { orderId } = first.json<AcceptedBody>();

    await pool.query(`UPDATE orders SET status = 'FILLED', remaining_units = 0 WHERE id = $1`, [
      orderId,
    ]);

    const replay = await app.inject(postOrderRequest(key));
    const current = await app.inject({ method: 'GET', url: `/v1/orders/${orderId}` });

    expect(replay.statusCode).toBe(202);
    expect(replay.body).toBe(first.body);
    expect(replay.json<AcceptedBody>().status).toBe('QUEUED');
    expect(current.json<{ status: string }>().status).toBe('FILLED');
  });

  it('rejects a changed payload with 422 and leaves the first request untouched', async () => {
    const key = uniqueKey();
    const first = await app.inject(postOrderRequest(key));
    const changed = await app.inject(postOrderRequest(key, { ...validOrder, price: '101.00' }));

    expect(changed.statusCode).toBe(422);
    expect(changed.json()).toMatchObject({ code: 'IDEMPOTENCY_KEY_REUSED' });
    expect(changed.headers.location).toBeUndefined();
    expect(await countRows(pool)).toEqual({ orders: 1, receipts: 1, events: 1 });

    const replay = await app.inject(postOrderRequest(key));
    expect(replay.body).toBe(first.body);
  });

  it('keeps separate keys separate for an identical order', async () => {
    const first = await app.inject(postOrderRequest(uniqueKey()));
    const second = await app.inject(postOrderRequest(uniqueKey()));

    expect(second.json<AcceptedBody>().orderId).not.toBe(first.json<AcceptedBody>().orderId);
    expect(await countRows(pool)).toEqual({ orders: 2, receipts: 2, events: 2 });
  });
});

describe('concurrency', () => {
  it('creates one order when the same request arrives 25 times at once', async () => {
    const key = uniqueKey();
    const responses = await Promise.all(
      Array.from({ length: 25 }, () => app.inject(postOrderRequest(key))),
    );

    expect(new Set(responses.map((response) => response.statusCode))).toEqual(new Set([202]));
    expect(new Set(responses.map((response) => response.body)).size).toBe(1);
    expect(await countRows(pool)).toEqual({ orders: 1, receipts: 1, events: 1 });
    expect(await readCounter(pool, 'BTC-USD')).toBe('1');
  });

  it('assigns gap-free, unique sequences to concurrent orders on one symbol', async () => {
    const count = 50;
    const responses = await Promise.all(
      Array.from({ length: count }, () => app.inject(postOrderRequest(uniqueKey()))),
    );
    expect(new Set(responses.map((response) => response.statusCode))).toEqual(new Set([202]));

    const { rows } = await pool.query<{ acceptance_seq: string; event_seq: string }>(
      `SELECT o.acceptance_seq, e.acceptance_seq AS event_seq
         FROM orders o JOIN outbox_events e ON e.order_id = o.id
        ORDER BY o.acceptance_seq`,
    );

    const expected = Array.from({ length: count }, (_, index) => String(index + 1));
    expect(rows.map((row) => row.acceptance_seq)).toEqual(expected);
    expect(rows.map((row) => row.event_seq)).toEqual(expected);
    expect(await readCounter(pool, 'BTC-USD')).toBe(String(count));
  });

  it('numbers each symbol independently', async () => {
    await app.inject(postOrderRequest(uniqueKey()));
    await app.inject(postOrderRequest(uniqueKey()));
    await app.inject(postOrderRequest(uniqueKey(), { ...validOrder, symbol: 'ETH-USD', quantity: '1.000' }));

    expect(await readCounter(pool, 'BTC-USD')).toBe('2');
    expect(await readCounter(pool, 'ETH-USD')).toBe('1');
  });

  it('makes a waiting request replay the receipt of the transaction it waited for', async () => {
    const key = uniqueKey();
    const { holder, orderId } = await holdKey(key);

    const waiting = app.inject(postOrderRequest(key));
    await waitForBlockedBackend(pool);
    await endTransaction(holder, 'COMMIT');

    const response = await waiting;
    expect(response.statusCode).toBe(202);
    expect(response.json<AcceptedBody>().orderId).toBe(orderId);
    expect((await countRows(pool))?.orders).toBe(1);
  });

  it('lets a waiting request take over the key when the holder rolls back', async () => {
    const key = uniqueKey();
    const { holder, orderId } = await holdKey(key);

    const waiting = app.inject(postOrderRequest(key));
    await waitForBlockedBackend(pool);
    await endTransaction(holder, 'ROLLBACK');

    const response = await waiting;
    expect(response.statusCode).toBe(202);
    expect(response.json<AcceptedBody>().orderId).not.toBe(orderId);
    expect(await countRows(pool)).toEqual({ orders: 1, receipts: 1, events: 1 });
  });

  describe('with a short lock budget', () => {
    let impatientApp: NestFastifyApplication;

    beforeAll(async () => {
      impatientApp = await buildApi({
        databaseUrl: TEST_DATABASE_URL,
        log,
        lockTimeoutMs: SHORT_LOCK_TIMEOUT_MS,
        readKafkaLag: NO_KAFKA,
      });
    });

    afterAll(() => impatientApp.close());

    it('answers 409 and Retry-After while the key is held, then accepts the retry', async () => {
      const key = uniqueKey();
      const { holder } = await holdKey(key);

      const inFlight = await impatientApp.inject(postOrderRequest(key));
      expect(inFlight.statusCode).toBe(409);
      expect(inFlight.json()).toMatchObject({ code: 'IDEMPOTENCY_REQUEST_IN_FLIGHT' });
      expect(inFlight.headers['retry-after']).toBe('1');
      expect(inFlight.headers.location).toBeUndefined();

      await endTransaction(holder, 'ROLLBACK');

      const retry = await impatientApp.inject(postOrderRequest(key));
      expect(retry.statusCode).toBe(202);
      expect(await countRows(pool)).toEqual({ orders: 1, receipts: 1, events: 1 });
    });

    it('answers 503, not 409, when the wait is for the symbol counter', async () => {
      await app.inject(postOrderRequest(uniqueKey()));

      const holder = await pool.connect();
      await holder.query('BEGIN');
      await holder.query(`SELECT 1 FROM symbol_sequences WHERE symbol = 'BTC-USD' FOR UPDATE`);

      const key = uniqueKey();
      const blocked = await impatientApp.inject(postOrderRequest(key));

      await endTransaction(holder, 'ROLLBACK');

      expect(blocked.statusCode).toBe(503);
      expect(blocked.json()).toMatchObject({ code: 'SERVICE_UNAVAILABLE' });
      expect(blocked.headers['retry-after']).toBe('1');
      expect(await countRows(pool)).toEqual({ orders: 1, receipts: 1, events: 1 });

      const retry = await impatientApp.inject(postOrderRequest(key));
      expect(retry.statusCode).toBe(202);
      expect(await readCounter(pool, 'BTC-USD')).toBe('2');
    });
  });
});

describe('atomicity when a write fails', () => {
  // The last write of the transaction fails, with no test hook in production code.
  async function injectOutboxFailure(): Promise<void> {
    await pool.query(`
      CREATE FUNCTION fail_outbox_insert() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN RAISE EXCEPTION 'injected Outbox failure'; END $$;
      CREATE TRIGGER fail_outbox_insert BEFORE INSERT ON outbox_events
        FOR EACH ROW EXECUTE FUNCTION fail_outbox_insert();
    `);
  }

  async function removeOutboxFailure(): Promise<void> {
    await pool.query(`
      DROP TRIGGER IF EXISTS fail_outbox_insert ON outbox_events;
      DROP FUNCTION IF EXISTS fail_outbox_insert();
    `);
  }

  afterEach(removeOutboxFailure);

  it('leaves nothing behind for a symbol with no counter yet, and the retry succeeds', async () => {
    const key = uniqueKey();
    await injectOutboxFailure();

    const failed = await app.inject(postOrderRequest(key));

    expect(failed.statusCode).toBe(503);
    expect(failed.json()).toMatchObject({ code: 'SERVICE_UNAVAILABLE' });
    expect(failed.json()).not.toHaveProperty('orderId');
    expect(failed.headers.location).toBeUndefined();
    expect(failed.headers['retry-after']).toBe('1');
    expect(await countRows(pool)).toEqual(NO_ROWS);
    expect(await readCounter(pool, 'BTC-USD')).toBeUndefined();

    await removeOutboxFailure();

    const retry = await app.inject(postOrderRequest(key));
    expect(retry.statusCode).toBe(202);
    expect(await readCounter(pool, 'BTC-USD')).toBe('1');
  });

  it('rolls back an advance of an existing counter, leaving no gap', async () => {
    await app.inject(postOrderRequest(uniqueKey()));
    await injectOutboxFailure();
    const key = uniqueKey();

    const failed = await app.inject(postOrderRequest(key));

    expect(failed.statusCode).toBe(503);
    expect(await countRows(pool)).toEqual({ orders: 1, receipts: 1, events: 1 });
    expect(await readCounter(pool, 'BTC-USD')).toBe('1');

    await removeOutboxFailure();

    const retry = await app.inject(postOrderRequest(key));
    expect(retry.statusCode).toBe(202);
    expect(await readCounter(pool, 'BTC-USD')).toBe('2');
  });
});

describe('request validation', () => {
  it.each<[string, object, string]>([
    ['an unsupported side', { ...validOrder, side: 'HOLD' }, 'INVALID_REQUEST'],
    ['an unsupported type', { ...validOrder, type: 'MARKET' }, 'INVALID_REQUEST'],
    ['a JSON number as price', { ...validOrder, price: 100.5 }, 'INVALID_REQUEST'],
    ['a JSON number as quantity', { ...validOrder, quantity: 2 }, 'INVALID_REQUEST'],
    ['an unknown property', { ...validOrder, clientTag: 'x' }, 'INVALID_REQUEST'],
    ['a missing property', { symbol: 'BTC-USD', side: 'BUY' }, 'INVALID_REQUEST'],
    ['an unsupported symbol', { ...validOrder, symbol: 'DOGE-USD' }, 'UNSUPPORTED_SYMBOL'],
    ['a zero price', { ...validOrder, price: '0' }, 'INVALID_PRICE'],
    ['a negative price', { ...validOrder, price: '-1' }, 'INVALID_PRICE'],
    ['an exponent', { ...validOrder, price: '1e2' }, 'INVALID_PRICE'],
    ['excess precision', { ...validOrder, price: '100.001' }, 'INVALID_PRICE'],
    ['a price above the maximum', { ...validOrder, price: '10000000.01' }, 'INVALID_PRICE'],
    ['a zero quantity', { ...validOrder, quantity: '0.0' }, 'INVALID_QUANTITY'],
    [
      'an off-tick price',
      { ...validOrder, symbol: 'ETH-USD', price: '100.03', quantity: '1.000' },
      'INVALID_PRICE',
    ],
  ])('rejects %s with 400 and writes nothing', async (_title, payload, code) => {
    const response = await app.inject(postOrderRequest(uniqueKey(), payload));

    expect(response.statusCode).toBe(400);
    expect(response.json()).toMatchObject({ code });
    expect(response.json()).not.toHaveProperty('orderId');
    expect(await countRows(pool)).toEqual(NO_ROWS);
  });

  it.each([
    ['missing', undefined],
    ['too short', 'short'],
    ['too long', 'k'.repeat(129)],
    ['using illegal characters', 'not a valid key at all'],
  ])('rejects an Idempotency-Key that is %s', async (_title, key) => {
    const response = await app.inject({
      method: 'POST',
      url: '/v1/orders',
      headers: key === undefined ? {} : { 'idempotency-key': key },
      payload: validOrder,
    });

    expect(response.statusCode).toBe(400);
    expect(response.json()).toMatchObject({ code: 'INVALID_REQUEST' });
    expect(await countRows(pool)).toEqual(NO_ROWS);
  });

  it('rejects a repeated Idempotency-Key header sent over a real socket', async () => {
    const server = await buildApi({
      databaseUrl: TEST_DATABASE_URL,
      log,
      lockTimeoutMs: 1_000,
      readKafkaLag: NO_KAFKA,
    });
    await server.listen({ host: '127.0.0.1', port: 0 });
    const { port } = server.getHttpServer().address() as AddressInfo;

    const statusCode = await new Promise<number | undefined>((resolve, reject) => {
      const outgoing = httpRequest(
        {
          host: '127.0.0.1',
          port,
          method: 'POST',
          path: '/v1/orders',
          headers: {
            'content-type': 'application/json',
            'idempotency-key': [uniqueKey(), uniqueKey()],
          },
        },
        (incoming) => {
          incoming.resume();
          resolve(incoming.statusCode);
        },
      );
      outgoing.on('error', reject);
      outgoing.end(JSON.stringify(validOrder));
    });

    await server.close();

    expect(statusCode).toBe(400);
    expect(await countRows(pool)).toEqual(NO_ROWS);
  });

  it('rejects malformed JSON', async () => {
    const response = await app.inject({
      method: 'POST',
      url: '/v1/orders',
      headers: { 'idempotency-key': uniqueKey(), 'content-type': 'application/json' },
      payload: '{"symbol":',
    });

    expect(response.statusCode).toBe(400);
    expect(response.json()).toMatchObject({ code: 'INVALID_REQUEST' });
  });

  it('rejects an oversized body without accepting it', async () => {
    const response = await app.inject(
      postOrderRequest(uniqueKey(), { ...validOrder, symbol: 'X'.repeat(20 * 1024) }),
    );

    expect(response.statusCode).toBe(413);
    expect(response.json()).toMatchObject({ code: 'INVALID_REQUEST' });
    expect(await countRows(pool)).toEqual(NO_ROWS);
  });
});

describe('GET /v1/orders/:orderId', () => {
  it('reports the current state with amounts at market scale', async () => {
    const created = await app.inject(postOrderRequest(uniqueKey()));
    const { orderId } = created.json<AcceptedBody>();

    const response = await app.inject({ method: 'GET', url: `/v1/orders/${orderId}` });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({
      orderId,
      symbol: 'BTC-USD',
      side: 'BUY',
      type: 'LIMIT',
      status: 'QUEUED',
      price: '100.00',
      originalQuantity: '2.50000000',
      executedQuantity: '0.00000000',
      remainingQuantity: '2.50000000',
      acceptanceSequence: '1',
      acceptedAt: expect.any(String),
      updatedAt: expect.any(String),
      trades: [],
    });
  });

  it('reports executed quantity from remaining quantity', async () => {
    const created = await app.inject(postOrderRequest(uniqueKey()));
    const { orderId } = created.json<AcceptedBody>();

    await pool.query(
      `UPDATE orders SET status = 'PARTIALLY_FILLED', remaining_units = 100000000 WHERE id = $1`,
      [orderId],
    );

    const response = await app.inject({ method: 'GET', url: `/v1/orders/${orderId}` });

    expect(response.json()).toMatchObject({
      status: 'PARTIALLY_FILLED',
      executedQuantity: '1.50000000',
      remainingQuantity: '1.00000000',
    });
  });

  it('answers 404 for an unknown order', async () => {
    const response = await app.inject({ method: 'GET', url: `/v1/orders/${randomUUID()}` });

    expect(response.statusCode).toBe(404);
    expect(response.json()).toMatchObject({ code: 'ORDER_NOT_FOUND' });
  });

  it('does not reveal an order from another scope', async () => {
    const orderId = randomUUID();
    await insertOrderInScope(orderId, 'other');

    const response = await app.inject({ method: 'GET', url: `/v1/orders/${orderId}` });

    expect(response.statusCode).toBe(404);
  });

  it.each(['not-a-uuid', 'urn:uuid:6f8a2f3e-9a8b-4c1d-8e2f-1a2b3c4d5e6f'])(
    'answers 400 for the malformed id %s',
    async (orderId) => {
      const response = await app.inject({ method: 'GET', url: `/v1/orders/${orderId}` });

      expect(response.statusCode).toBe(400);
      expect(response.json()).toMatchObject({ code: 'INVALID_REQUEST' });
    },
  );

  async function insertOrderInScope(orderId: string, scope: string): Promise<void> {
    const client = await pool.connect();
    try {
      await insertOrder(client, {
        ...canonicalizeOrder(validOrder),
        id: orderId,
        scope,
        acceptanceSeq: 1n,
        acceptedAt: new Date(),
      });
    } finally {
      client.release();
    }
  }
});

describe('when PostgreSQL is unreachable', () => {
  let unreachableApp: NestFastifyApplication;

  beforeAll(async () => {
    unreachableApp = await buildApi({
      databaseUrl: 'postgres://pooleno@127.0.0.1:1/none',
      log,
      lockTimeoutMs: 1_000,
      readKafkaLag: NO_KAFKA,
    });
  });

  afterAll(() => unreachableApp.close());

  it('never claims acceptance, and asks for a retry with the same key', async () => {
    const response = await unreachableApp.inject(postOrderRequest(uniqueKey()));

    expect(response.statusCode).toBe(503);
    expect(response.json()).toMatchObject({ code: 'SERVICE_UNAVAILABLE' });
    expect(response.json()).not.toHaveProperty('orderId');
    expect(response.headers.location).toBeUndefined();
    expect(response.headers['retry-after']).toBe('1');
  });

  it('answers GET with 503, not a misleading 404', async () => {
    const response = await unreachableApp.inject({
      method: 'GET',
      url: `/v1/orders/${randomUUID()}`,
    });

    expect(response.statusCode).toBe(503);
  });
});

describe('what Nest would otherwise add', () => {
  it('answers a form body with 415, because no form parser is registered', async () => {
    const response = await app.inject({
      method: 'POST',
      url: '/v1/orders',
      headers: {
        'content-type': 'application/x-www-form-urlencoded',
        'idempotency-key': uniqueKey(),
      },
      payload: 'symbol=BTC-USD',
    });

    expect(response.statusCode).toBe(415);
    expect(response.json()).toMatchObject({ code: 'INVALID_REQUEST' });
  });

  it('leaves a malformed URL to Fastify', async () => {
    const response = await app.inject({ method: 'GET', url: '/v1/orders/%FF' });

    expect(response.statusCode).toBe(400);
    expect(response.headers['content-type']).toBe('application/json');
    expect(response.json()).toMatchObject({ code: 'FST_ERR_BAD_URL' });
  });
});

describe('unknown routes', () => {
  it('answers with the same error shape', async () => {
    const response = await app.inject({ method: 'GET', url: '/nope' });

    expect(response.statusCode).toBe(404);
    expect(response.json()).toMatchObject({ code: 'NOT_FOUND' });
  });
});
