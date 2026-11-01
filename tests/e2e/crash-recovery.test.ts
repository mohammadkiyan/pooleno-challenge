import { Pool, type PoolClient } from 'pg';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { createEventProducer } from '../../src/modules/messaging/kafka-producer.js';
import type { OrderAcceptedV1 } from '../../src/modules/messaging/order-accepted.schema.js';
import type { CreateOrderBody, OrderResponse } from '../../src/modules/orders/order.types.js';
import {
  committedMessages,
  createTestStream,
  deleteTestStream,
  joinAsRival,
  moveCommittedOffsetsToEnd,
  readTopic,
  type TestStream,
} from '../support/kafka.js';
import {
  acceptEvent,
  ethOrder,
  uniqueKey,
  validOrder,
  type AcceptedBody,
} from '../support/orders.js';
import {
  CLEAN_EXIT,
  killAllRoles,
  requestApi,
  RoleProcess,
  startApi,
  type RunningApi,
} from '../support/processes.js';
import {
  countInbox,
  countRows,
  readCounter,
  readOrder,
  readProgress,
  readTrades,
  resetDatabase,
  TEST_DATABASE_URL,
  terminateAdvisoryLockHolders,
  terminateBlockedBackends,
  waitForBlockedBackend,
} from '../support/test-database.js';

const CRASH_BEFORE_RESPONSE = './tests/e2e/crash-before-response.preload.ts';
const CRASH_BEFORE_OFFSET_COMMIT = './tests/e2e/crash-before-offset-commit.preload.ts';
const CLOSED_PORT = '127.0.0.1:1';
const SESSION_TIMEOUT_MS = 6_000;

// The statement in the operator procedures of docs/adr/0002-phase-2-messaging-and-matching.md.
const REPUBLISH_UNAPPLIED_EVENTS = `
  UPDATE outbox_events e
     SET published_at = NULL, next_attempt_at = now(), attempts = 0
   WHERE e.published_at IS NOT NULL
     AND e.acceptance_seq > COALESCE(
       (SELECT p.last_applied_seq FROM matching_progress p WHERE p.symbol = e.symbol), 0)`;

const heldRowLocks = new Set<PoolClient>();

let pool: Pool;

beforeAll(() => {
  pool = new Pool({ connectionString: TEST_DATABASE_URL });
});

afterAll(() => pool.end());

beforeEach(() => resetDatabase(pool));

afterEach(async () => {
  await killAllRoles();
  await Promise.all([...heldRowLocks].map(releaseRowLock));
});

function postOrder(
  api: RunningApi,
  key: string,
  body: CreateOrderBody = validOrder,
): Promise<Response> {
  return requestApi(api, '/v1/orders', {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'idempotency-key': key },
    body: JSON.stringify(body),
  });
}

/** Holds a row lock from outside the process under test, to park it at an exact moment. */
async function holdRowLock(sql: string, parameters: unknown[]): Promise<PoolClient> {
  const holder = await pool.connect();
  heldRowLocks.add(holder);
  await holder.query('BEGIN');
  await holder.query(sql, parameters);
  return holder;
}

async function releaseRowLock(holder: PoolClient): Promise<void> {
  heldRowLocks.delete(holder);
  await holder.query('ROLLBACK');
  holder.release();
}

describe('crash recovery of acceptance', () => {
  it('keeps an accepted order through a crash and restart', async () => {
    const key = uniqueKey();
    const firstProcess = await startApi();

    const accepted = await postOrder(firstProcess, key);
    expect(accepted.status).toBe(202);
    const receipt = (await accepted.json()) as AcceptedBody;

    const before = await requestApi(firstProcess, receipt.statusUrl);
    expect(before.status).toBe(200);
    const stateBefore: unknown = await before.json();

    await firstProcess.role.kill();
    const secondProcess = await startApi();

    const after = await requestApi(secondProcess, receipt.statusUrl);
    expect(after.status).toBe(200);
    expect(await after.json()).toEqual(stateBefore);

    const replay = await postOrder(secondProcess, key);
    expect(replay.status).toBe(202);
    expect(replay.headers.get('location')).toBe(accepted.headers.get('location'));
    expect(await replay.json()).toEqual(receipt);
    expect(await countRows(pool)).toEqual({ orders: 1, receipts: 1, events: 1 });
  });

  it('recovers when the process dies after commit but before the client hears back', async () => {
    const key = uniqueKey();
    const doomedProcess = await startApi(CRASH_BEFORE_RESPONSE);

    await expect(postOrder(doomedProcess, key)).rejects.toThrow();

    expect((await doomedProcess.role.exited).signal).toBe('SIGKILL');
    expect(await countRows(pool)).toEqual({ orders: 1, receipts: 1, events: 1 });

    const { rows } = await pool.query<{ id: string }>('SELECT id FROM orders');
    const storedOrderId = rows[0]?.id;

    const recoveredProcess = await startApi();
    const retry = await postOrder(recoveredProcess, key);

    expect(retry.status).toBe(202);
    expect(((await retry.json()) as AcceptedBody).orderId).toBe(storedOrderId);
    expect(await countRows(pool)).toEqual({ orders: 1, receipts: 1, events: 1 });
  });

  it('discards an open transaction when the process dies before commit', async () => {
    const firstProcess = await startApi();
    await postOrder(firstProcess, uniqueKey());

    // Holding the counter parks the next request mid-transaction, after it claims its key.
    const holder = await holdRowLock(
      `SELECT 1 FROM symbol_sequences WHERE symbol = 'BTC-USD' FOR UPDATE`,
      [],
    );

    const key = uniqueKey();
    const interrupted = expect(postOrder(firstProcess, key)).rejects.toThrow();
    await waitForBlockedBackend(pool);

    await firstProcess.role.kill();
    await interrupted;
    await releaseRowLock(holder);

    const secondProcess = await startApi();
    const retry = await postOrder(secondProcess, key);

    expect(retry.status).toBe(202);
    expect(await countRows(pool)).toEqual({ orders: 2, receipts: 2, events: 2 });
    expect(await readCounter(pool, 'BTC-USD')).toBe('2');
  });
});

describe('the full flow through Kafka', { timeout: 120_000 }, () => {
  let stream: TestStream;

  beforeEach(async () => {
    stream = await createTestStream();
  });

  afterEach(async () => {
    await killAllRoles();
    await deleteTestStream(stream);
  });

  function startRole(
    role: 'publisher' | 'matching',
    env: NodeJS.ProcessEnv = {},
    preload?: string,
  ): RoleProcess {
    return new RoleProcess(
      role,
      {
        KAFKA_BROKERS: stream.kafka.brokers.join(','),
        KAFKA_TOPIC: stream.kafka.topic,
        MATCHING_GROUP_ID: stream.groupId,
        MATCHING_SESSION_TIMEOUT_MS: String(SESSION_TIMEOUT_MS),
        MATCHING_RESTART_DELAY_MS: '100',
        OUTBOX_POLL_INTERVAL_MS: '20',
        OUTBOX_RETRY_BASE_MS: '100',
        OUTBOX_RETRY_CAP_MS: '400',
        ...env,
      },
      preload,
    );
  }

  function eventually(assertion: () => Promise<void>): Promise<void> {
    return vi.waitFor(assertion, { timeout: 45_000, interval: 50 });
  }

  async function waitForStatus(orderId: string, status: string): Promise<void> {
    await eventually(async () => {
      expect((await readOrder(pool, orderId))?.status).toBe(status);
    });
  }

  async function waitForCommittedMessages(count: number): Promise<void> {
    await eventually(async () => {
      expect(await committedMessages(stream)).toBe(count);
    });
  }

  async function waitForPublished(eventId: string): Promise<void> {
    await eventually(async () => {
      const { rows } = await pool.query<{ published: boolean }>(
        'SELECT published_at IS NOT NULL AS published FROM outbox_events WHERE event_id = $1',
        [eventId],
      );
      expect(rows[0]?.published).toBe(true);
    });
  }

  /** Parks the matcher's next transaction for a symbol that already has a progress row. */
  function holdProgressRow(symbol: string): Promise<PoolClient> {
    return holdRowLock('SELECT 1 FROM matching_progress WHERE symbol = $1 FOR UPDATE', [symbol]);
  }

  async function topicEventIds(): Promise<string[]> {
    const messages = await readTopic(stream);
    return messages.map((message) => (JSON.parse(message.value) as OrderAcceptedV1).eventId);
  }

  describe('the agreed fixture', () => {
    async function place(api: RunningApi, body: CreateOrderBody): Promise<string> {
      const response = await postOrder(api, uniqueKey(), body);
      expect(response.status).toBe(202);
      return ((await response.json()) as AcceptedBody).orderId;
    }

    async function getOrder(api: RunningApi, orderId: string): Promise<OrderResponse> {
      const response = await requestApi(api, `/v1/orders/${orderId}`);
      return (await response.json()) as OrderResponse;
    }

    it('produces the expected trades, and a replay of the whole topic adds none', async () => {
      const api = await startApi();
      startRole('publisher');
      const matcher = startRole('matching');

      const s1 = await place(api, ethOrder('SELL', '100.00', '0.0020'));
      const s2 = await place(api, ethOrder('SELL', '100.00', '0.0030'));
      const s3 = await place(api, ethOrder('SELL', '99.00', '0.0010'));
      const b1 = await place(api, ethOrder('BUY', '100.00', '0.0040'));
      await waitForStatus(b1, 'FILLED');

      const taker = await getOrder(api, b1);
      expect(taker.trades.map(({ price, quantity }) => [price, quantity])).toEqual([
        ['99.00', '0.0010'],
        ['100.00', '0.0020'],
        ['100.00', '0.0010'],
      ]);
      expect(await readTrades(pool)).toMatchObject([
        { maker_id: s3, taker_id: b1 },
        { maker_id: s1, taker_id: b1 },
        { maker_id: s2, taker_id: b1 },
      ]);
      expect(await getOrder(api, s2)).toMatchObject({
        status: 'PARTIALLY_FILLED',
        remainingQuantity: '0.0020',
      });
      const tradesBefore = await readTrades(pool);

      // A new group starts from the retained beginning, so every event is delivered again.
      await matcher.kill();
      const replaying = startRole('matching', { MATCHING_GROUP_ID: `${stream.groupId}-replay` });
      await replaying.waitForLog('event already handled', 4);
      expect(await readTrades(pool)).toEqual(tradesBefore);

      // A later sell at the same price queues behind S2's restored remainder.
      const s4 = await place(api, ethOrder('SELL', '100.00', '0.0050'));
      const b2 = await place(api, ethOrder('BUY', '100.00', '0.0030'));
      await waitForStatus(b2, 'FILLED');

      const { rows } = await pool.query(
        `SELECT maker_id, quantity_units FROM trades WHERE taker_id = $1 ORDER BY execution_index`,
        [b2],
      );
      expect(rows).toEqual([
        { maker_id: s2, quantity_units: '20' },
        { maker_id: s4, quantity_units: '10' },
      ]);
    });
  });

  describe('publishing', () => {
    it('accepts orders while Kafka is down, then publishes each symbol in order', async () => {
      const api = await startApi();
      const outage = startRole('publisher', { KAFKA_BROKERS: CLOSED_PORT });

      const bodies = [
        ethOrder('SELL', '100.00', '0.0010'),
        validOrder,
        ethOrder('SELL', '101.00', '0.0010'),
        ethOrder('SELL', '102.00', '0.0010'),
      ];
      for (const body of bodies) {
        expect((await postOrder(api, uniqueKey(), body)).status).toBe(202);
      }

      const readOutbox = async () => {
        const { rows } = await pool.query<{
          symbol: string;
          payload: unknown;
          published: boolean;
          attempts: number;
          last_error: string | null;
        }>(
          `SELECT symbol, payload, published_at IS NOT NULL AS published, attempts, last_error
             FROM outbox_events ORDER BY symbol, acceptance_seq`,
        );
        return rows;
      };

      // Both symbols' heads fail durably, and nothing behind a failed head is attempted.
      await eventually(async () => {
        const headAttempts = (await readOutbox()).map((event) => event.attempts);
        expect(headAttempts[0]).toBeGreaterThan(0);
        expect(headAttempts[1]).toBeGreaterThan(0);
      });
      const pending = await readOutbox();
      expect(pending.every((event) => !event.published)).toBe(true);
      expect(pending.map((event) => event.attempts).slice(2)).toEqual([0, 0]);
      expect(pending[0]?.last_error).toEqual(expect.any(String));

      expect(await outage.terminate()).toEqual(CLEAN_EXIT);

      startRole('publisher');
      await eventually(async () => {
        expect((await readOutbox()).every((event) => event.published)).toBe(true);
      });

      const messages = await readTopic(stream);
      const payloadsOf = (symbol: string) =>
        messages
          .filter((message) => message.key === symbol)
          .map((message) => JSON.parse(message.value) as unknown);
      const storedPayloadsOf = (symbol: string) =>
        pending.filter((event) => event.symbol === symbol).map((event) => event.payload);

      expect(messages).toHaveLength(4);
      expect(payloadsOf('BTC-USD')).toEqual(storedPayloadsOf('BTC-USD'));
      expect(payloadsOf('ETH-USD')).toEqual(storedPayloadsOf('ETH-USD'));
    });

    it('resends the same event after a crash before it is marked, with one effect', async () => {
      const seller = await acceptEvent(pool, ethOrder('SELL', '100.00', '0.0030'));
      const buyer = await acceptEvent(pool, ethOrder('BUY', '100.00', '0.0010'));

      // Holding the row parks the publisher right after Kafka acknowledged the send.
      const holder = await holdRowLock(
        'SELECT 1 FROM outbox_events WHERE event_id = $1 FOR UPDATE',
        [buyer.eventId],
      );
      const doomed = startRole('publisher');
      await waitForBlockedBackend(pool);
      expect(await topicEventIds()).toEqual([seller.eventId, buyer.eventId]);

      await doomed.kill();
      await terminateBlockedBackends(pool);
      await releaseRowLock(holder);

      const { rows } = await pool.query<{ published_at: Date | null }>(
        'SELECT published_at FROM outbox_events WHERE event_id = $1',
        [buyer.eventId],
      );
      expect(rows[0]?.published_at).toBeNull();

      startRole('publisher');
      await waitForPublished(buyer.eventId);

      const messages = await readTopic(stream);
      expect(await topicEventIds()).toEqual([seller.eventId, buyer.eventId, buyer.eventId]);
      expect(messages[2]).toEqual(messages[1]);

      const matcher = startRole('matching');
      await matcher.waitForLog('event already handled');

      expect(await readTrades(pool)).toHaveLength(1);
      expect(await readOrder(pool, seller.orderId)).toEqual({
        status: 'PARTIALLY_FILLED',
        remaining_units: '20',
      });
      expect(await readOrder(pool, buyer.orderId)).toEqual({
        status: 'FILLED',
        remaining_units: '0',
      });
    });
  });

  describe('matching', () => {
    it('creates no second trade when it dies between its commit and the offset', async () => {
      const seller = await acceptEvent(pool, ethOrder('SELL', '100.00', '0.0030'));
      await acceptEvent(pool, ethOrder('BUY', '100.00', '0.0010'));
      startRole('publisher');

      // The buyer's event has offset 1, so finishing it means committing offset 2.
      const doomed = startRole(
        'matching',
        { CRASH_BEFORE_COMMITTING_OFFSET: '2' },
        CRASH_BEFORE_OFFSET_COMMIT,
      );
      expect((await doomed.exited).signal).toBe('SIGKILL');

      expect(await readTrades(pool)).toHaveLength(1);
      expect(await committedMessages(stream)).toBe(1);

      const restarted = startRole('matching');
      await restarted.waitForLog('event already handled');
      await waitForCommittedMessages(2);
      expect(await readTrades(pool)).toHaveLength(1);

      // The restored book holds the correct remainder, which a new order can then consume.
      const next = await acceptEvent(pool, ethOrder('BUY', '100.00', '0.0020'));
      await waitForStatus(next.orderId, 'FILLED');
      expect(await readOrder(pool, seller.orderId)).toEqual({
        status: 'FILLED',
        remaining_units: '0',
      });
    });

    it('leaves nothing of a transaction when it dies before the commit', async () => {
      const seller = await acceptEvent(pool, ethOrder('SELL', '100.00', '0.0030'));
      startRole('publisher');
      const matcher = startRole('matching');
      await waitForStatus(seller.orderId, 'OPEN');
      await waitForCommittedMessages(1);

      const holder = await holdProgressRow('ETH-USD');
      const buyer = await acceptEvent(pool, ethOrder('BUY', '100.00', '0.0010'));
      await waitForBlockedBackend(pool);

      await matcher.kill();
      await releaseRowLock(holder);

      expect(await readOrder(pool, buyer.orderId)).toMatchObject({ status: 'QUEUED' });
      expect(await readTrades(pool)).toHaveLength(0);
      expect(await readProgress(pool, 'ETH-USD')).toBe('1');
      expect(await countInbox(pool)).toBe(1);
      expect(await committedMessages(stream)).toBe(1);

      startRole('matching');
      await waitForStatus(buyer.orderId, 'FILLED');
      await waitForCommittedMessages(2);
      expect(await readTrades(pool)).toHaveLength(1);
      expect(await readOrder(pool, seller.orderId)).toEqual({
        status: 'PARTIALLY_FILLED',
        remaining_units: '20',
      });
    });

    it('finishes its transaction on SIGTERM and commits nothing beyond durable work', async () => {
      const seller = await acceptEvent(pool, ethOrder('SELL', '100.00', '0.0030'));
      startRole('publisher');
      const matcher = startRole('matching');
      await waitForStatus(seller.orderId, 'OPEN');
      await waitForCommittedMessages(1);

      const holder = await holdProgressRow('ETH-USD');
      const buyer = await acceptEvent(pool, ethOrder('BUY', '100.00', '0.0010'));
      await waitForBlockedBackend(pool);

      const exit = matcher.terminate();
      await matcher.waitForLog('shutting down');
      expect(await readOrder(pool, buyer.orderId)).toMatchObject({ status: 'QUEUED' });

      await releaseRowLock(holder);

      expect(await exit).toEqual(CLEAN_EXIT);
      expect(await readTrades(pool)).toHaveLength(1);
      expect(await readProgress(pool, 'ETH-USD')).toBe('2');
      expect(await committedMessages(stream)).toBe(1);

      const restarted = startRole('matching');
      await restarted.waitForLog('event already handled');
      await waitForCommittedMessages(2);
      expect(await readTrades(pool)).toHaveLength(1);
    });

    it('commits no offset after a rebalance takes its partitions', async () => {
      const seller = await acceptEvent(pool, ethOrder('SELL', '100.00', '0.0030'));
      startRole('publisher');
      const matcher = startRole('matching');
      await waitForStatus(seller.orderId, 'OPEN');
      await waitForCommittedMessages(1);

      const holder = await holdProgressRow('ETH-USD');
      const buyer = await acceptEvent(pool, ethOrder('BUY', '100.00', '0.0010'));
      await waitForBlockedBackend(pool);

      const rival = joinAsRival(stream, SESSION_TIMEOUT_MS);
      await matcher.waitForLog('group membership changed; stopping this session');

      await releaseRowLock(holder);
      await waitForStatus(buyer.orderId, 'FILLED');
      await rival.joined;

      expect(await committedMessages(stream)).toBe(1);

      await rival.leave();
      await matcher.waitForLog('event already handled');
      await waitForCommittedMessages(2);
      expect(await readTrades(pool)).toHaveLength(1);
    });

    it('quarantines a malformed message and applies the order behind it', async () => {
      const producer = createEventProducer(stream.kafka);
      await producer.send({ key: 'ETH-USD', value: 'not an event' });
      await producer.close();
      const order = await acceptEvent(pool, ethOrder('SELL', '100.00', '0.0030'));

      startRole('publisher');
      await waitForPublished(order.eventId);
      const matcher = startRole('matching');

      await waitForStatus(order.orderId, 'OPEN');
      await waitForCommittedMessages(2);
      expect(matcher.countLogs('malformed message quarantined')).toBe(1);
      expect(matcher.countLogs('partition held; restoring its book before retrying')).toBe(0);
    });

    it('recovers a lost event from the Outbox by the documented procedure', async () => {
      const s1 = await acceptEvent(pool, ethOrder('SELL', '100.00', '0.0020'));
      const firstPublisher = startRole('publisher');
      const matcher = startRole('matching');
      await waitForStatus(s1.orderId, 'OPEN');
      expect(await firstPublisher.terminate()).toEqual(CLEAN_EXIT);

      // S2 counts as published but is no longer in Kafka, while B1 behind it still is.
      const s2 = await acceptEvent(pool, ethOrder('SELL', '100.00', '0.0030'));
      const b1 = await acceptEvent(pool, ethOrder('BUY', '100.00', '0.0040'));
      await pool.query('UPDATE outbox_events SET published_at = now() WHERE event_id = $1', [
        s2.eventId,
      ]);
      const publisher = startRole('publisher');
      await waitForPublished(b1.eventId);

      // The sequence guard refuses to skip ahead.
      await matcher.waitForLog('partition held; restoring its book before retrying', 2);
      expect(await readOrder(pool, b1.orderId)).toMatchObject({ status: 'QUEUED' });
      expect(await readProgress(pool, 'ETH-USD')).toBe('1');

      expect(await publisher.terminate()).toEqual(CLEAN_EXIT);
      expect(await matcher.terminate()).toEqual(CLEAN_EXIT);
      await moveCommittedOffsetsToEnd(stream);
      await pool.query(REPUBLISH_UNAPPLIED_EVENTS);
      startRole('publisher');
      startRole('matching');

      await waitForStatus(b1.orderId, 'FILLED');
      expect(await readOrder(pool, s2.orderId)).toEqual({
        status: 'PARTIALLY_FILLED',
        remaining_units: '10',
      });
      expect((await readTrades(pool)).map(({ maker_id }) => maker_id)).toEqual([
        s1.orderId,
        s2.orderId,
      ]);
      expect(await readProgress(pool, 'ETH-USD')).toBe('3');

      // The event already applied is not sent again, and B1's stale copy was skipped.
      expect(await topicEventIds()).toEqual([s1.eventId, b1.eventId, s2.eventId, b1.eventId]);
    });
  });

  describe.each(['publisher', 'matching'] as const)('single ownership of %s', (role) => {
    const otherRole = role === 'publisher' ? 'matching' : 'publisher';

    it('keeps a standby waiting until the owner dies, then hands over', async () => {
      startRole(otherRole);
      const owner = startRole(role);
      await owner.waitForLog('acquired ownership');

      const standby = startRole(role);
      await standby.waitForLog('another process owns this role; waiting');
      const first = await acceptEvent(pool, ethOrder('SELL', '100.00', '0.0010'));
      await waitForStatus(first.orderId, 'OPEN');
      expect(standby.countLogs('acquired ownership')).toBe(0);

      await owner.kill();
      await standby.waitForLog('acquired ownership');

      const second = await acceptEvent(pool, ethOrder('SELL', '100.00', '0.0010'));
      await waitForStatus(second.orderId, 'OPEN');
    });

    it('stops with an error when the connection holding ownership is lost', async () => {
      const owner = startRole(role);
      await owner.waitForLog('acquired ownership');

      expect(await terminateAdvisoryLockHolders(pool)).toBe(1);

      expect(await owner.exited).toEqual({ code: 1, signal: null });
    });

    it('leaves cleanly on SIGTERM while still waiting for ownership', async () => {
      const owner = startRole(role);
      await owner.waitForLog('acquired ownership');
      const standby = startRole(role);
      await standby.waitForLog('another process owns this role; waiting');

      expect(await standby.terminate()).toEqual(CLEAN_EXIT);
      expect(await owner.terminate()).toEqual(CLEAN_EXIT);
    });
  });
});
