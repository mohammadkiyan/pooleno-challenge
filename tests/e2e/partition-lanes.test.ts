import { Pool, type PoolClient } from 'pg';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  committedMessages,
  createTestStream,
  deleteTestStream,
  moveCommittedOffset,
  produceRaw,
  readTopic,
  type TestStream,
} from '../support/kafka.js';
import { readQuarantine } from '../support/external-jobs.js';
import { acceptEvent, ethOrder, validOrder } from '../support/orders.js';
import {
  CLEAN_EXIT,
  healthUrlOf,
  killAllRoles,
  readHealth,
  RoleProcess,
} from '../support/processes.js';
import {
  readOrder,
  readProgress,
  readTrades,
  resetDatabase,
  TEST_DATABASE_URL,
  waitForBlockedBackend,
} from '../support/test-database.js';

const HELD_LOG = 'partition held; restoring its book before retrying';
const SESSION_FAILURE_LOG = 'matching stopped; restoring from the database before retrying';
const BTC_SELL = { ...validOrder, side: 'SELL' as const, quantity: '1.00000000' };
const BTC_BUY = { ...validOrder, side: 'BUY' as const, quantity: '1.00000000' };

describe('matching with one lane per partition', () => {
  let pool: Pool;
  let stream: TestStream;
  let holder: PoolClient | undefined;

  beforeAll(() => {
    pool = new Pool({ connectionString: TEST_DATABASE_URL });
  });

  afterAll(() => pool.end());

  beforeEach(async () => {
    await resetDatabase(pool);
    stream = await createTestStream();
  });

  afterEach(async () => {
    await killAllRoles();
    await releaseHolder();
    await deleteTestStream(stream);
  });

  function startMatching(env: NodeJS.ProcessEnv = {}): RoleProcess {
    return new RoleProcess('matching', {
      KAFKA_BROKERS: stream.kafka.brokers.join(','),
      KAFKA_TOPIC: stream.kafka.topic,
      MATCHING_GROUP_ID: stream.groupId,
      MATCHING_SESSION_TIMEOUT_MS: '6000',
      MATCHING_RESTART_DELAY_MS: '100',
      ...env,
    });
  }

  function produceEvents(events: { symbol: string }[]): Promise<void> {
    return produceRaw(
      stream,
      events.map((event) => ({ key: event.symbol, value: JSON.stringify(event) })),
    );
  }

  function eventually(assertion: () => Promise<void>): Promise<void> {
    return vi.waitFor(assertion, { timeout: 25_000, interval: 50 });
  }

  async function waitForStatus(orderId: string, status: string): Promise<void> {
    await eventually(async () => {
      expect((await readOrder(pool, orderId))?.status).toBe(status);
    });
  }

  /** Makes BTC's next database write wait on a row lock, for as long as the test wants. */
  async function blockBtcWrites(): Promise<void> {
    await pool.query(`INSERT INTO matching_progress (symbol) VALUES ('BTC-USD')`);
    holder = await pool.connect();
    await holder.query('BEGIN');
    await holder.query(`SELECT 1 FROM matching_progress WHERE symbol = 'BTC-USD' FOR UPDATE`);
  }

  async function releaseHolder(): Promise<void> {
    if (holder) {
      await holder.query('ROLLBACK');
      holder.release();
      holder = undefined;
    }
  }

  async function assertSymbolsOnDifferentPartitions(): Promise<void> {
    const partitions = new Map(
      (await readTopic(stream)).map(({ key, partition }) => [key, partition]),
    );
    expect(partitions.get('BTC-USD')).toBeDefined();
    expect(partitions.get('ETH-USD')).toBeDefined();
    expect(partitions.get('BTC-USD')).not.toBe(partitions.get('ETH-USD'));
  }

  it('finishes ETH while a BTC database write is blocked, and then BTC in order', async () => {
    await blockBtcWrites();
    const btcSell = await acceptEvent(pool, BTC_SELL);
    const btcBuy = await acceptEvent(pool, BTC_BUY);
    const ethSell = await acceptEvent(pool, ethOrder('SELL', '100.00', '0.0020'));
    const ethBuy = await acceptEvent(pool, ethOrder('BUY', '100.00', '0.0020'));
    await produceEvents([btcSell, btcBuy, ethSell, ethBuy]);
    await assertSymbolsOnDifferentPartitions();

    const matcher = startMatching();
    await waitForBlockedBackend(pool);

    // BTC really is mid-write and blocked, while ETH on its own partition runs to completion.
    await waitForStatus(ethBuy.orderId, 'FILLED');
    expect(await readOrder(pool, ethSell.orderId)).toMatchObject({ status: 'FILLED' });
    expect(await readOrder(pool, btcSell.orderId)).toMatchObject({ status: 'QUEUED' });
    expect(await readOrder(pool, btcBuy.orderId)).toMatchObject({ status: 'QUEUED' });
    await eventually(async () => {
      expect(await committedMessages(stream)).toBe(2);
    });

    await releaseHolder();

    await waitForStatus(btcBuy.orderId, 'FILLED');
    expect(await readOrder(pool, btcSell.orderId)).toMatchObject({ status: 'FILLED' });
    expect(await readProgress(pool, 'BTC-USD')).toBe('2');
    await eventually(async () => {
      expect(await committedMessages(stream)).toBe(4);
    });
    expect(matcher.countLogs(HELD_LOG)).toBe(0);
    expect(matcher.countLogs(SESSION_FAILURE_LOG)).toBe(0);
  });

  it('holds only the partition of a failing event, and reports it through readiness', async () => {
    const ethSell = await acceptEvent(pool, ethOrder('SELL', '100.00', '0.0020'));
    const btcFirst = await acceptEvent(pool, BTC_SELL);
    const btcSecond = await acceptEvent(pool, BTC_SELL);
    // BTC's second event arrives first, so the sequence guard fails until it is moved past.
    await produceEvents([btcSecond, btcFirst, btcSecond, ethSell]);
    await assertSymbolsOnDifferentPartitions();

    const matcher = startMatching();
    const url = await healthUrlOf(matcher);
    await waitForStatus(ethSell.orderId, 'OPEN');
    await matcher.waitForLog(HELD_LOG, 2);

    expect(await readOrder(pool, btcFirst.orderId)).toMatchObject({ status: 'QUEUED' });
    expect(await committedMessages(stream)).toBe(1);
    expect(matcher.countLogs(SESSION_FAILURE_LOG)).toBe(0);
    await eventually(async () => {
      expect(await readHealth(url, '/health/ready')).toEqual({
        status: 503,
        body: { status: 'not_ready', reason: 'a partition is held on a failing event' },
      });
    });
    expect((await readHealth(url, '/health/live')).status).toBe(200);

    // The operator's existing repair still works.
    expect(await matcher.terminate()).toMatchObject({ code: 0 });
    const messages = await readTopic(stream);
    const held = messages.find(({ key }) => key === 'BTC-USD');
    await moveCommittedOffset(stream, held?.partition ?? 0, 1n);

    startMatching();
    await waitForStatus(btcSecond.orderId, 'OPEN');
    expect(await readOrder(pool, btcFirst.orderId)).toMatchObject({ status: 'OPEN' });
  });

  describe('a message that cannot be read as an event', () => {
    it('is quarantined, and the orders behind it on its partition still trade', async () => {
      const btcSell = await acceptEvent(pool, BTC_SELL);
      const btcBuy = await acceptEvent(pool, BTC_BUY);
      const ethSell = await acceptEvent(pool, ethOrder('SELL', '100.00', '0.0020'));
      await produceRaw(stream, [
        { key: 'BTC-USD', value: JSON.stringify(btcSell) },
        { key: 'BTC-USD', value: 'not an event' },
        { key: 'BTC-USD', value: JSON.stringify(btcBuy) },
        { key: 'ETH-USD', value: JSON.stringify(ethSell) },
      ]);

      const matcher = startMatching();
      const url = await healthUrlOf(matcher);

      await waitForStatus(btcBuy.orderId, 'FILLED');
      await waitForStatus(ethSell.orderId, 'OPEN');
      await eventually(async () => {
        expect(await committedMessages(stream)).toBe(4);
        expect((await readHealth(url, '/health/ready')).status).toBe(200);
      });

      const rows = await readQuarantine(pool);
      expect(rows.map(({ consumer, kafka_offset: offset, raw_value: value }) => ({
        consumer,
        offset,
        value: value.toString(),
      }))).toEqual([{ consumer: 'matching-v1', offset: '1', value: 'not an event' }]);
      expect(await readTrades(pool)).toHaveLength(1);
      expect(matcher.countLogs(HELD_LOG)).toBe(0);
      expect(matcher.countLogs(SESSION_FAILURE_LOG)).toBe(0);
    });

    it('holds its partition until it is durably recorded', async () => {
      const btc = await acceptEvent(pool, BTC_SELL);
      await pool.query(`
        CREATE OR REPLACE FUNCTION fail_quarantine() RETURNS trigger LANGUAGE plpgsql AS
          $$ BEGIN RAISE EXCEPTION 'the database refused the write'; END $$;
        CREATE TRIGGER fail_quarantine BEFORE INSERT ON quarantine
          FOR EACH ROW EXECUTE FUNCTION fail_quarantine();
      `);

      try {
        await produceRaw(stream, [
          { key: 'BTC-USD', value: 'not an event' },
          { key: 'BTC-USD', value: JSON.stringify(btc) },
        ]);

        const matcher = startMatching();
        await matcher.waitForLog(HELD_LOG, 2);
        expect(await committedMessages(stream)).toBe(0);
        expect(await readOrder(pool, btc.orderId)).toMatchObject({ status: 'QUEUED' });

        await pool.query('DROP TRIGGER fail_quarantine ON quarantine');

        await waitForStatus(btc.orderId, 'OPEN');
        await matcher.waitForLog('partition released');
        expect(await readQuarantine(pool)).toHaveLength(1);
      } finally {
        await pool.query(`
          DROP TRIGGER IF EXISTS fail_quarantine ON quarantine;
          DROP FUNCTION IF EXISTS fail_quarantine;
        `);
      }
    });

    it('cannot lose a real order, because the sequence guard stops at the next event', async () => {
      const damaged = await acceptEvent(pool, BTC_SELL);
      const next = await acceptEvent(pool, BTC_SELL);
      await produceRaw(stream, [
        { key: 'BTC-USD', value: JSON.stringify(damaged).slice(0, 20) },
        { key: 'BTC-USD', value: JSON.stringify(next) },
      ]);

      const matcher = startMatching();

      await matcher.waitForLog(HELD_LOG, 2);
      expect(await readQuarantine(pool)).toHaveLength(1);
      expect(await committedMessages(stream)).toBe(1);
      expect(await readOrder(pool, damaged.orderId)).toMatchObject({ status: 'QUEUED' });
      expect(await readOrder(pool, next.orderId)).toMatchObject({ status: 'QUEUED' });
      expect(await readProgress(pool, 'BTC-USD')).toBeUndefined();
    });
  });

  it('restores only the failing book and retries, with no repeated trade', async () => {
    const ethSell = await acceptEvent(pool, ethOrder('SELL', '100.00', '0.0020'));
    const ethBuy = await acceptEvent(pool, ethOrder('BUY', '100.00', '0.0020'));
    const btcSell = await acceptEvent(pool, BTC_SELL);
    const btcBuy = await acceptEvent(pool, BTC_BUY);
    await pool.query(`
      CREATE OR REPLACE FUNCTION fail_btc_trade() RETURNS trigger AS $$
      BEGIN
        IF NEW.symbol = 'BTC-USD' THEN RAISE EXCEPTION 'simulated failure'; END IF;
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;
      CREATE TRIGGER fail_btc_trade BEFORE INSERT ON trades
      FOR EACH ROW EXECUTE FUNCTION fail_btc_trade();
    `);

    try {
      await produceEvents([ethSell, btcSell, btcBuy, ethBuy]);
      const matcher = startMatching();

      await matcher.waitForLog(HELD_LOG, 2);
      await waitForStatus(ethBuy.orderId, 'FILLED');
      expect(await readOrder(pool, btcBuy.orderId)).toMatchObject({ status: 'QUEUED' });

      await pool.query(`
        DROP TRIGGER fail_btc_trade ON trades;
        DROP FUNCTION fail_btc_trade;
      `);

      await waitForStatus(btcBuy.orderId, 'FILLED');
      await matcher.waitForLog('partition released');
      expect(await readTrades(pool)).toHaveLength(2);
      expect(matcher.countLogs(SESSION_FAILURE_LOG)).toBe(0);
      expect(matcher.countLogs('books restored')).toBe(1);
      expect(await matcher.terminate()).toEqual(CLEAN_EXIT);
    } finally {
      await pool.query(`
        DROP TRIGGER IF EXISTS fail_btc_trade ON trades;
        DROP FUNCTION IF EXISTS fail_btc_trade;
      `);
    }
  });

  it('falls back to holding every partition once a blocked lane is full', async () => {
    await blockBtcWrites();
    const btcFirst = await acceptEvent(pool, BTC_SELL);
    const btcSecond = await acceptEvent(pool, BTC_SELL);
    await produceEvents([btcFirst, btcSecond]);

    startMatching({ MATCHING_LANE_CAPACITY: '1' });
    await waitForBlockedBackend(pool);

    const eth = await acceptEvent(pool, ethOrder('SELL', '100.00', '0.0020'));
    await produceEvents([eth]);
    await new Promise((resolve) => setTimeout(resolve, 2_000));
    expect(await readOrder(pool, eth.orderId)).toMatchObject({ status: 'QUEUED' });

    await releaseHolder();

    await waitForStatus(eth.orderId, 'OPEN');
    await waitForStatus(btcSecond.orderId, 'OPEN');
  });
});
