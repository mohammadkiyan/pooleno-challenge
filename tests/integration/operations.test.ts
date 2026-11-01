import { randomUUID } from 'node:crypto';
import { once } from 'node:events';
import { createServer, type AddressInfo } from 'node:net';
import type { Pool } from 'pg';
import { pino } from 'pino';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { buildApi } from '../../src/app/build-api.js';
import { createPool } from '../../src/infrastructure/db.js';
import {
  findDueJobs,
  markJobFailed,
  quarantineMessage,
  scheduleJobRetry,
} from '../../src/modules/external/external-job.repository.js';
import { scheduleNotification } from '../../src/modules/external/schedule-notification.js';
import { recordSendFailure } from '../../src/modules/messaging/outbox.repository.js';
import type { OrderAcceptedV1 } from '../../src/modules/messaging/order-accepted.schema.js';
import { readBacklog } from '../../src/modules/operations/backlog.repository.js';
import { readKafkaLag, type GroupLag } from '../../src/modules/operations/kafka-lag.js';
import {
  createTestStream,
  deleteTestStream,
  moveCommittedOffset,
  produceRaw,
  readTopic,
  type TestStream,
} from '../support/kafka.js';
import { acceptEvent, ethOrder } from '../support/orders.js';
import { resetDatabase, TEST_DATABASE_URL } from '../support/test-database.js';

const log = pino({ level: 'silent' });
const CLOSED_PORT = '127.0.0.1:1';
const SECRET_ERROR = 'connect to internal-host:5432 as admin:hunter2';

let pool: Pool;

beforeAll(() => {
  pool = createPool(TEST_DATABASE_URL, log);
});

afterAll(() => pool.end());

beforeEach(() => resetDatabase(pool));

function accept(side: 'BUY' | 'SELL' = 'SELL'): Promise<OrderAcceptedV1> {
  return acceptEvent(pool, ethOrder(side, '100.00', '0.0010'));
}

function schedule(event: OrderAcceptedV1): Promise<boolean> {
  return scheduleNotification(pool, { eventId: event.eventId, payload: JSON.stringify(event) });
}

async function dueJob(eventId: string) {
  const job = (await findDueJobs(pool, 100)).find((due) => due.eventId === eventId);
  if (!job) {
    throw new Error(`No due job for ${eventId}`);
  }
  return job;
}

describe('the backlog report', () => {
  it('reports an idle system as zeros, not as missing values', async () => {
    expect(await readBacklog(pool)).toEqual({
      outbox: {
        pending: 0,
        oldestPendingAgeMs: null,
        retrying: 0,
        totalAttempts: 0,
        blockedSymbols: [],
      },
      matching: { lag: 0, symbols: [] },
      external: {
        pending: 0,
        retrying: 0,
        failed: 0,
        oldestPendingAgeMs: null,
        quarantined: 0,
      },
    });
  });

  describe('Outbox', () => {
    it('counts unpublished events and reports the age of the oldest', async () => {
      const event = await accept();
      await accept();
      await pool.query(
        `UPDATE outbox_events SET created_at = now() - interval '1 hour' WHERE event_id = $1`,
        [event.eventId],
      );

      const { outbox } = await readBacklog(pool);

      expect(outbox.pending).toBe(2);
      expect(outbox.oldestPendingAgeMs).toBeGreaterThanOrEqual(3_600_000);
      expect(outbox.oldestPendingAgeMs).toBeLessThan(3_660_000);
      expect(outbox.retrying).toBe(0);
    });

    it('stops counting an event once it is published', async () => {
      const event = await accept();
      await pool.query('UPDATE outbox_events SET published_at = now() WHERE event_id = $1', [
        event.eventId,
      ]);

      expect((await readBacklog(pool)).outbox).toMatchObject({
        pending: 0,
        oldestPendingAgeMs: null,
      });
    });

    it('names a symbol whose earliest event is failing, without its error text', async () => {
      const head = await accept();
      await accept();
      await recordSendFailure(pool, head.eventId, { error: SECRET_ERROR, retryAfterMs: 60_000 });
      await recordSendFailure(pool, head.eventId, { error: SECRET_ERROR, retryAfterMs: 60_000 });

      const { outbox } = await readBacklog(pool);

      expect(outbox).toMatchObject({ pending: 2, retrying: 1, totalAttempts: 2 });
      expect(outbox.blockedSymbols).toEqual([
        {
          symbol: 'ETH-USD',
          acceptanceSequence: head.acceptanceSequence,
          attempts: 2,
          nextAttemptInMs: expect.any(Number) as number,
        },
      ]);
      expect(outbox.blockedSymbols[0]?.nextAttemptInMs).toBeGreaterThan(50_000);
      expect(JSON.stringify(outbox)).not.toContain('hunter2');
    });

    it('does not call a symbol blocked when only a later event has failed', async () => {
      await accept();
      const later = await accept();
      await recordSendFailure(pool, later.eventId, { error: 'failed', retryAfterMs: 60_000 });

      const { outbox } = await readBacklog(pool);

      expect(outbox.retrying).toBe(1);
      expect(outbox.blockedSymbols).toEqual([]);
    });
  });

  describe('matching lag', () => {
    it('counts accepted orders that matching has not applied, per symbol', async () => {
      const first = await accept();
      await accept();
      await acceptEvent(pool, { ...ethOrder('BUY', '100.00', '0.0010'), symbol: 'BTC-USD' });
      await pool.query(`UPDATE orders SET created_at = now() - interval '1 hour' WHERE id = $1`, [
        first.orderId,
      ]);

      const { matching } = await readBacklog(pool);

      expect(matching.lag).toBe(3);
      expect(matching.symbols.map(({ symbol, lag }) => ({ symbol, lag }))).toEqual([
        { symbol: 'BTC-USD', lag: 1 },
        { symbol: 'ETH-USD', lag: 2 },
      ]);
      const eth = matching.symbols.find(({ symbol }) => symbol === 'ETH-USD');
      expect(eth?.oldestUnappliedAgeMs).toBeGreaterThanOrEqual(3_600_000);
    });

    it('counts only what is left after progress, and leaves out a symbol that caught up', async () => {
      await accept();
      const second = await accept();
      await pool.query(`INSERT INTO matching_progress (symbol, last_applied_seq) VALUES ($1, $2)`, [
        'ETH-USD',
        '1',
      ]);

      const behind = await readBacklog(pool);
      expect(behind.matching.lag).toBe(1);

      await pool.query(`UPDATE matching_progress SET last_applied_seq = $1 WHERE symbol = $2`, [
        second.acceptanceSequence,
        'ETH-USD',
      ]);

      expect((await readBacklog(pool)).matching).toEqual({ lag: 0, symbols: [] });
    });
  });

  describe('external jobs', () => {
    it('separates pending, retrying and failed jobs, and counts quarantined messages', async () => {
      const [pending, retrying, failed] = [await accept(), await accept(), await accept()];
      await Promise.all([schedule(pending), schedule(retrying), schedule(failed)]);
      await pool.query(
        `UPDATE external_jobs SET created_at = now() - interval '1 hour' WHERE event_id = $1`,
        [pending.eventId],
      );

      await scheduleJobRetry(pool, await dueJob(retrying.eventId), {
        error: SECRET_ERROR,
        retryDelayMs: 60_000,
      });
      await markJobFailed(pool, await dueJob(failed.eventId), SECRET_ERROR);
      await quarantineMessage(
        pool,
        { topic: 'orders.accepted', partition: 0, offset: 7n, value: 'not an event' },
        SECRET_ERROR,
      );

      const { external } = await readBacklog(pool);

      expect(external).toMatchObject({ pending: 2, retrying: 1, failed: 1, quarantined: 1 });
      expect(external.oldestPendingAgeMs).toBeGreaterThanOrEqual(3_600_000);
      expect(JSON.stringify(external)).not.toContain('hunter2');
    });

    it('does not count a delivered job', async () => {
      const event = await accept();
      await schedule(event);
      await pool.query(
        `UPDATE external_jobs
            SET status = 'SUCCEEDED', next_attempt_at = NULL, completed_at = now()
          WHERE event_id = $1`,
        [event.eventId],
      );

      expect((await readBacklog(pool)).external).toMatchObject({
        pending: 0,
        failed: 0,
        oldestPendingAgeMs: null,
      });
    });
  });
});

describe('Kafka lag', () => {
  let stream: TestStream;

  beforeEach(async () => {
    stream = await createTestStream();
  });

  afterEach(() => deleteTestStream(stream));

  async function partitionOf(key: string): Promise<number> {
    const message = (await readTopic(stream)).find((candidate) => candidate.key === key);
    if (!message) {
      throw new Error(`No message with key ${key}`);
    }
    return message.partition;
  }

  async function lagOf(groupId: string): Promise<GroupLag> {
    const lag = await readKafkaLag(stream.kafka, [groupId], log);
    if (!lag.available) {
      throw new Error('Kafka lag should be available');
    }
    return lag.groups[0] as GroupLag;
  }

  it('counts every message for a group that has never committed', async () => {
    await produceRaw(stream, [
      { key: 'ETH-USD', value: 'a' },
      { key: 'ETH-USD', value: 'b' },
      { key: 'BTC-USD', value: 'c' },
    ]);

    const group = await lagOf(stream.groupId);

    expect(group.groupId).toBe(stream.groupId);
    expect(group.lag).toBe(3);
    expect(group.partitions).toHaveLength(3);
    expect(group.partitions.every(({ committedOffset }) => committedOffset === null)).toBe(true);
  });

  it('counts only what is behind the committed offset, partition by partition', async () => {
    await produceRaw(stream, [
      { key: 'ETH-USD', value: 'a' },
      { key: 'ETH-USD', value: 'b' },
      { key: 'ETH-USD', value: 'c' },
    ]);
    await moveCommittedOffset(stream, await partitionOf('ETH-USD'), 2n);

    const group = await lagOf(stream.groupId);

    expect(group.lag).toBe(1);
    expect(group.partitions.find(({ lag }) => lag === 1)).toMatchObject({
      endOffset: 3,
      committedOffset: 2,
    });
  });

  it('reports each group separately, because each has its own position', async () => {
    await produceRaw(stream, [{ key: 'ETH-USD', value: 'a' }]);
    await moveCommittedOffset(stream, await partitionOf('ETH-USD'), 1n);
    const other = `other-${randomUUID()}`;

    const lag = await readKafkaLag(stream.kafka, [stream.groupId, other], log);

    expect(lag).toMatchObject({
      available: true,
      groups: [
        { groupId: stream.groupId, lag: 0 },
        { groupId: other, lag: 1 },
      ],
    });
  });

  it('answers that it is unavailable, rather than fail, without Kafka', async () => {
    const lag = await readKafkaLag(
      { brokers: [CLOSED_PORT], topic: stream.kafka.topic },
      [stream.groupId],
      log,
    );

    expect(lag).toEqual({ available: false });
  });

  it('gives up quickly on a broker that accepts the connection and never answers', async () => {
    const hung = createServer((socket) => socket.on('error', () => undefined));
    hung.listen(0, '127.0.0.1');
    await once(hung, 'listening');
    const { port } = hung.address() as AddressInfo;
    const started = performance.now();

    const lag = await readKafkaLag(
      { brokers: [`127.0.0.1:${port}`], topic: stream.kafka.topic },
      [stream.groupId],
      log,
    );

    expect(lag).toEqual({ available: false });
    expect(performance.now() - started).toBeLessThan(3_000);
    hung.close();
  });
});

describe('the operational routes of the API', () => {
  const server = (dependencies: { pool: Pool; kafkaBrokers: string[] }) =>
    buildApi({
      pool: dependencies.pool,
      log,
      lockTimeoutMs: 1_000,
      readKafkaLag: () =>
        readKafkaLag(
          { brokers: dependencies.kafkaBrokers, topic: 'orders.accepted' },
          ['matching-v1'],
          log,
        ),
    });

  it('keeps reporting the backlog while Kafka is down', async () => {
    const app = server({ pool, kafkaBrokers: [CLOSED_PORT] });
    await accept();

    const response = await app.inject({ method: 'GET', url: '/ops/backlog' });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toMatchObject({
      outbox: { pending: 1 },
      matching: { lag: 1 },
      kafka: { available: false },
    });
    await app.close();
  });

  it('reports ready without Kafka, because acceptance does not need it', async () => {
    const app = server({ pool, kafkaBrokers: [CLOSED_PORT] });

    const live = await app.inject({ method: 'GET', url: '/health/live' });
    const ready = await app.inject({ method: 'GET', url: '/health/ready' });

    expect([live.statusCode, live.json()]).toEqual([200, { status: 'ok' }]);
    expect([ready.statusCode, ready.json()]).toEqual([200, { status: 'ready' }]);
    await app.close();
  });

  it('stays alive but not ready without PostgreSQL, and names no connection details', async () => {
    const unreachable = createPool('postgres://pooleno:secret@127.0.0.1:1/none', log);
    const app = server({ pool: unreachable, kafkaBrokers: [CLOSED_PORT] });

    const live = await app.inject({ method: 'GET', url: '/health/live' });
    const ready = await app.inject({ method: 'GET', url: '/health/ready' });

    expect(live.statusCode).toBe(200);
    expect(ready.statusCode).toBe(503);
    expect(ready.json()).toEqual({ status: 'not_ready', reason: 'database unavailable' });

    await app.close();
    await unreachable.end();
  });
});
