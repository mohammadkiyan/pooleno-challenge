import { randomUUID } from 'node:crypto';
import { Pool, type PoolClient } from 'pg';
import { pino } from 'pino';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { buildApi } from '../../src/app/build-api.js';
import { applyAcceptedOrder } from '../../src/modules/matching/apply-accepted-order.js';
import { OrderBook } from '../../src/modules/matching/order-book.js';
import { restoreBooks, type OrderBooks } from '../../src/modules/matching/restore-book.js';
import type { OrderAcceptedV1 } from '../../src/modules/messaging/order-accepted.schema.js';
import { parseOrderAccepted } from '../../src/modules/messaging/order-accepted.schema.js';
import type { OrderResponse, OrderSide } from '../../src/modules/orders/order.types.js';
import { acceptEvent, ethOrder } from '../support/orders.js';
import {
  countInbox,
  readOrder,
  readProgress,
  readTrades,
  resetDatabase,
  TEST_DATABASE_URL,
} from '../support/test-database.js';

const NO_KAFKA = () => Promise.resolve({ available: false as const });

const FAILS_ON_TRADE = `
  CREATE OR REPLACE FUNCTION fail_trade() RETURNS trigger AS $$
  BEGIN RAISE EXCEPTION 'simulated failure while inserting a trade'; END;
  $$ LANGUAGE plpgsql;

  CREATE TRIGGER fail_trade BEFORE INSERT ON trades
  FOR EACH ROW EXECUTE FUNCTION fail_trade();
`;
const REMOVES_THE_FAILURE = `
  DROP TRIGGER IF EXISTS fail_trade ON trades;
  DROP FUNCTION IF EXISTS fail_trade;
`;

let pool: Pool;
let books: OrderBooks;

beforeAll(() => {
  pool = new Pool({ connectionString: TEST_DATABASE_URL });
});

afterAll(() => pool.end());

beforeEach(async () => {
  await resetDatabase(pool);
  books = new Map();
});

afterEach(() => pool.query(REMOVES_THE_FAILURE));

function sellAt(price: string, quantity: string) {
  return acceptEvent(pool, ethOrder('SELL', price, quantity));
}

function buyAt(price: string, quantity: string) {
  return acceptEvent(pool, ethOrder('BUY', price, quantity));
}

function apply(event: OrderAcceptedV1) {
  return applyAcceptedOrder(pool, books, event);
}

/** Accepts and applies the handoff fixture on ETH-USD. */
async function runFixture() {
  const s1 = await sellAt('100.00', '0.0020');
  const s2 = await sellAt('100.00', '0.0030');
  const s3 = await sellAt('99.00', '0.0010');
  const b1 = await buyAt('100.00', '0.0040');
  for (const event of [s1, s2, s3, b1]) {
    await apply(event);
  }
  return { s1, s2, s3, b1 };
}

function restingOn(book: OrderBook | undefined, side: OrderSide) {
  return [...(book?.restingOrders(side) ?? [])].map((order) => [
    order.orderId,
    order.priceUnits,
    order.remainingUnits,
  ]);
}

/**
 * A pool whose COMMIT really commits and then reports failure, as when the connection
 * dies before the server's reply arrives. The caller cannot tell it succeeded.
 */
function poolWithLostCommitReply(): Pool {
  const uncertain = new Pool({ connectionString: TEST_DATABASE_URL });
  const connect = uncertain.connect.bind(uncertain) as () => Promise<PoolClient>;

  uncertain.connect = (async () => {
    const client = await connect();
    const query = client.query.bind(client) as (
      text: string,
      ...rest: unknown[]
    ) => Promise<unknown>;
    client.query = (async (text: string, ...rest: unknown[]) => {
      if (text === 'COMMIT') {
        await query(text);
        throw new Error('connection lost before the commit reply');
      }
      return query(text, ...rest);
    }) as typeof client.query;
    return client;
  }) as typeof uncertain.connect;

  return uncertain;
}

describe('applying accepted orders', () => {
  it('commits the fixture: the expected trades, order changes, Inbox and progress', async () => {
    const { s1, s2, s3, b1 } = await runFixture();

    const trades = await readTrades(pool);
    expect(
      trades
        .sort((a, b) => a.execution_index - b.execution_index)
        .map((trade) => [trade.maker_id, trade.quantity_units, trade.price_units]),
    ).toEqual([
      [s3.orderId, '10', '9900'],
      [s1.orderId, '20', '10000'],
      [s2.orderId, '10', '10000'],
    ]);
    expect(new Set(trades.map((trade) => trade.event_id))).toEqual(new Set([b1.eventId]));
    expect(new Set(trades.map((trade) => trade.taker_id))).toEqual(new Set([b1.orderId]));

    expect(await readOrder(pool, b1.orderId)).toEqual({ status: 'FILLED', remaining_units: '0' });
    expect(await readOrder(pool, s1.orderId)).toEqual({ status: 'FILLED', remaining_units: '0' });
    expect(await readOrder(pool, s3.orderId)).toEqual({ status: 'FILLED', remaining_units: '0' });
    expect(await readOrder(pool, s2.orderId)).toEqual({
      status: 'PARTIALLY_FILLED',
      remaining_units: '20',
    });

    expect(await readProgress(pool, 'ETH-USD')).toBe('4');
    expect(await countInbox(pool)).toBe(4);
    expect(restingOn(books.get('ETH-USD'), 'SELL')).toEqual([[s2.orderId, 10000n, 20n]]);
  });

  it('marks an order that rests without trading as OPEN', async () => {
    const event = await sellAt('100.00', '0.0020');

    await apply(event);

    expect(await readOrder(pool, event.orderId)).toEqual({ status: 'OPEN', remaining_units: '20' });
  });

  it('treats equivalent decimals as one price, and keeps exact units', async () => {
    const first = await sellAt('100', '0.002');
    const second = await sellAt('100.00', '0.0020');
    await apply(first);
    await apply(second);

    expect(restingOn(books.get('ETH-USD'), 'SELL')).toEqual([
      [first.orderId, 10000n, 20n],
      [second.orderId, 10000n, 20n],
    ]);
  });

  it('exposes the trades of both sides through GET, in execution order', async () => {
    const { s2, b1 } = await runFixture();
    const app = buildApi({ pool, log: pino({ level: 'silent' }), lockTimeoutMs: 1_000, readKafkaLag: NO_KAFKA });

    const taker = (await app.inject({ url: `/v1/orders/${b1.orderId}` })).json<OrderResponse>();
    const maker = (await app.inject({ url: `/v1/orders/${s2.orderId}` })).json<OrderResponse>();
    await app.close();

    expect(taker).toMatchObject({
      status: 'FILLED',
      originalQuantity: '0.0040',
      executedQuantity: '0.0040',
      remainingQuantity: '0.0000',
    });
    expect(taker.trades.map(({ role, price, quantity }) => [role, price, quantity])).toEqual([
      ['TAKER', '99.00', '0.0010'],
      ['TAKER', '100.00', '0.0020'],
      ['TAKER', '100.00', '0.0010'],
    ]);
    expect(maker).toMatchObject({
      status: 'PARTIALLY_FILLED',
      executedQuantity: '0.0010',
      remainingQuantity: '0.0020',
    });
    expect(maker.trades.map(({ role, price, quantity }) => [role, price, quantity])).toEqual([
      ['MAKER', '100.00', '0.0010'],
    ]);
  });
});

describe('redelivery', () => {
  it('treats a redelivered event as a no-op, even with a restored book', async () => {
    const { b1 } = await runFixture();
    const before = await readTrades(pool);

    books = await restoreBooks(pool);
    await expect(apply(b1)).resolves.toBeUndefined();

    expect(await readTrades(pool)).toEqual(before);
    expect(await readProgress(pool, 'ETH-USD')).toBe('4');
    expect(await countInbox(pool)).toBe(4);
  });

  it('keeps the original FIFO when an old event returns after newer ones', async () => {
    const { s1, s2 } = await runFixture();
    const s4 = await sellAt('100.00', '0.0050');
    await apply(s4);

    await expect(apply(s1)).resolves.toBeUndefined();

    expect(restingOn(books.get('ETH-USD'), 'SELL')).toEqual([
      [s2.orderId, 10000n, 20n],
      [s4.orderId, 10000n, 50n],
    ]);
    expect(await readOrder(pool, s1.orderId)).toEqual({ status: 'FILLED', remaining_units: '0' });
  });
});

describe('the sequence guard', () => {
  it('blocks on a gap and leaves no trace of the event', async () => {
    await sellAt('100.00', '0.0010');
    const third = await (async () => {
      await sellAt('100.00', '0.0010');
      return sellAt('100.00', '0.0010');
    })();

    await expect(apply(third)).rejects.toThrow('Sequence guard: ETH-USD expects 1');

    expect(await countInbox(pool)).toBe(0);
    expect(await readOrder(pool, third.orderId)).toMatchObject({ status: 'QUEUED' });
  });

  it('blocks an unknown event that carries a sequence already applied', async () => {
    const { s1 } = await runFixture();
    const stale = { ...s1, eventId: randomUUID() };

    await expect(apply(stale)).rejects.toThrow('Sequence guard: ETH-USD expects 5');

    expect(await countInbox(pool)).toBe(4);
  });

  it('refuses to match from a book that is behind the database', async () => {
    const { s1, s2 } = await (async () => {
      const first = await sellAt('100.00', '0.0010');
      const second = await sellAt('100.00', '0.0010');
      return { s1: first, s2: second };
    })();
    await apply(s1);
    books.set('ETH-USD', new OrderBook());

    await expect(apply(s2)).rejects.toThrow('book is at 0 but the database is at 1');

    expect(await readOrder(pool, s2.orderId)).toMatchObject({ status: 'QUEUED' });
  });

  it.each([
    ['its remainder', 'UPDATE orders SET remaining_units = 10 WHERE id = $1'],
    ['whether it rests', `UPDATE orders SET status = 'QUEUED' WHERE id = $1`],
  ])('refuses a book that disagrees with the database about %s', async (_subject, change) => {
    const seller = await sellAt('100.00', '0.0020');
    const buyer = await buyAt('100.00', '0.0020');
    await apply(seller);
    await pool.query(change, [seller.orderId]);

    await expect(apply(buyer)).rejects.toThrow('disagrees with the database');

    expect(await readTrades(pool)).toEqual([]);
    expect(await readOrder(pool, buyer.orderId)).toMatchObject({ status: 'QUEUED' });
  });
});

describe('failures around the commit', () => {
  it('rolls back everything when the transaction fails, and exposes nothing in RAM', async () => {
    const seller = await sellAt('100.00', '0.0020');
    const buyer = await buyAt('100.00', '0.0020');
    await apply(seller);
    await pool.query(FAILS_ON_TRADE);

    await expect(apply(buyer)).rejects.toThrow('simulated failure while inserting a trade');

    expect(await readOrder(pool, buyer.orderId)).toMatchObject({ status: 'QUEUED' });
    expect(await readOrder(pool, seller.orderId)).toEqual({ status: 'OPEN', remaining_units: '20' });
    expect(await readProgress(pool, 'ETH-USD')).toBe('1');
    expect(await countInbox(pool)).toBe(1);
    expect(restingOn(books.get('ETH-USD'), 'SELL')).toEqual([[seller.orderId, 10000n, 20n]]);

    await pool.query(REMOVES_THE_FAILURE);
    await apply(buyer);
    expect(await readTrades(pool)).toHaveLength(1);
  });

  it('stays correct when a commit succeeds but its reply is lost', async () => {
    const seller = await sellAt('100.00', '0.0030');
    const buyer = await buyAt('100.00', '0.0010');
    await apply(seller);
    const uncertain = poolWithLostCommitReply();

    await expect(applyAcceptedOrder(uncertain, books, buyer)).rejects.toThrow(
      'connection lost before the commit reply',
    );
    await uncertain.end();

    // Nothing reached RAM, although the database has the result.
    expect(restingOn(books.get('ETH-USD'), 'SELL')).toEqual([[seller.orderId, 10000n, 30n]]);
    expect(await readTrades(pool)).toHaveLength(1);

    books = await restoreBooks(pool);
    expect(restingOn(books.get('ETH-USD'), 'SELL')).toEqual([[seller.orderId, 10000n, 20n]]);

    await expect(apply(buyer)).resolves.toBeUndefined();
    expect(await readTrades(pool)).toHaveLength(1);
  });

  it('stops and restores correctly when RAM cannot apply a committed match', async () => {
    const seller = await sellAt('100.00', '0.0030');
    const buyer = await buyAt('100.00', '0.0010');
    await apply(seller);
    const book = books.get('ETH-USD');
    if (!book) {
      throw new Error('The book does not exist');
    }
    vi.spyOn(book, 'applyFills').mockImplementation(() => {
      throw new Error('simulated RAM failure');
    });

    await expect(apply(buyer)).rejects.toThrow('simulated RAM failure');

    books = await restoreBooks(pool);
    expect(restingOn(books.get('ETH-USD'), 'SELL')).toEqual([[seller.orderId, 10000n, 20n]]);
    await expect(apply(buyer)).resolves.toBeUndefined();
    expect(await readTrades(pool)).toHaveLength(1);
  });
});

describe('restoring books', () => {
  it('restores FIFO, partial fills and progress, and later orders queue behind them', async () => {
    const { s2 } = await runFixture();

    books = await restoreBooks(pool);

    expect(books.get('ETH-USD')?.lastAppliedSeq).toBe(4n);
    expect(restingOn(books.get('ETH-USD'), 'SELL')).toEqual([[s2.orderId, 10000n, 20n]]);

    const s4 = await sellAt('100.00', '0.0050');
    await apply(s4);
    const b2 = await buyAt('100.00', '0.0030');
    await apply(b2);

    const { rows } = await pool.query<{ maker_id: string; quantity_units: string }>(
      'SELECT maker_id, quantity_units FROM trades WHERE event_id = $1 ORDER BY execution_index',
      [b2.eventId],
    );
    expect(rows).toEqual([
      { maker_id: s2.orderId, quantity_units: '20' },
      { maker_id: s4.orderId, quantity_units: '10' },
    ]);
  });

  it('leaves queued orders to enter through their events', async () => {
    const applied = await sellAt('100.00', '0.0010');
    await apply(applied);
    const queued = await sellAt('99.00', '0.0010');

    books = await restoreBooks(pool);

    expect(restingOn(books.get('ETH-USD'), 'SELL')).toEqual([[applied.orderId, 10000n, 10n]]);

    await apply(queued);
    expect(restingOn(books.get('ETH-USD'), 'SELL')).toEqual([
      [queued.orderId, 9900n, 10n],
      [applied.orderId, 10000n, 10n],
    ]);
  });

  it('matches the live book after a random workload', async () => {
    const quantityTicks = [1, 2, 3, 5, 8];
    for (let index = 0; index < 150; index += 1) {
      const side = index % 3 === 0 ? 'BUY' : 'SELL';
      const price = (95 + ((index * 7) % 11)).toFixed(2);
      const quantity = ((quantityTicks[index % quantityTicks.length] ?? 1) / 1_000).toFixed(4);
      await apply(await acceptEvent(pool, ethOrder(side, price, quantity)));
    }

    const live = books.get('ETH-USD');
    const restored = (await restoreBooks(pool)).get('ETH-USD');

    expect(restored?.lastAppliedSeq).toBe(live?.lastAppliedSeq);
    expect(restingOn(restored, 'BUY')).toEqual(restingOn(live, 'BUY'));
    expect(restingOn(restored, 'SELL')).toEqual(restingOn(live, 'SELL'));
    expect(restingOn(live, 'BUY').length + restingOn(live, 'SELL').length).toBeGreaterThan(0);
    expect((await readTrades(pool)).length).toBeGreaterThan(0);
  });
});

describe('reading events from the wire', () => {
  it('accepts the exact event that acceptance produces', async () => {
    const event = await sellAt('100.00', '0.0010');

    expect(parseOrderAccepted(JSON.stringify(event))).toEqual(event);
  });

  it.each([
    ['text that is not JSON', 'not json'],
    ['an empty object', '{}'],
    ['a fractional quantity', JSON.stringify({ quantityUnits: '1.5' })],
  ])('rejects %s', (_name, raw) => {
    expect(() => parseOrderAccepted(raw)).toThrow();
  });

  it('rejects a valid event with a bad amount or an unexpected field', async () => {
    const event = await sellAt('100.00', '0.0010');

    expect(() => parseOrderAccepted(JSON.stringify({ ...event, priceUnits: '0' }))).toThrow(
      'Not a valid OrderAccepted v1 event',
    );
    expect(() => parseOrderAccepted(JSON.stringify({ ...event, extra: true }))).toThrow(
      'Not a valid OrderAccepted v1 event',
    );
  });
});
