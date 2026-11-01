import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Pool, type PoolClient } from 'pg';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { scheduleNotification } from '../../src/modules/external/schedule-notification.js';
import type { OrderAcceptedV1 } from '../../src/modules/messaging/order-accepted.schema.js';
import { countJobs, readJob, readQuarantine, type StoredJob } from '../support/external-jobs.js';
import {
  committedMessages,
  createTestStream,
  deleteTestStream,
  joinAsRival,
  produceRaw,
  type TestStream,
} from '../support/kafka.js';
import {
  clearFaults,
  injectFault,
  mockApiAddress,
  postNotification,
  readEffects,
  startMockApi,
  type MockApiAddress,
} from '../support/mock-api.js';
import { acceptEvent, ethOrder } from '../support/orders.js';
import { CLEAN_EXIT, killAllRoles, RoleProcess } from '../support/processes.js';
import {
  countInbox,
  readOrder,
  readTrades,
  resetDatabase,
  TEST_DATABASE_URL,
  terminateAdvisoryLockHolders,
  waitForBlockedBackend,
} from '../support/test-database.js';

const CRASH_BEFORE_OFFSET_COMMIT = './tests/e2e/crash-before-offset-commit.preload.ts';
const CRASH_BEFORE_RECORDING_DELIVERY =
  './tests/integration/crash-before-recording-delivery.preload.ts';
const CLOSED_PORT = '127.0.0.1:1';
const SESSION_TIMEOUT_MS = 6_000;
// The mock's log of a request to its contract, rather than to its control endpoints.
const NOTIFICATION_REQUEST = /"url":"\/v1\/notifications"/;
const SELL = ethOrder('SELL', '100.00', '0.0030');
const BUY = ethOrder('BUY', '100.00', '0.0010');

const heldRows = new Set<PoolClient>();

let pool: Pool;
let stream: TestStream;
let directory: string;
let mock: MockApiAddress;

beforeAll(() => {
  pool = new Pool({ connectionString: TEST_DATABASE_URL });
});

afterAll(() => pool.end());

beforeEach(async () => {
  await resetDatabase(pool);
  stream = await createTestStream();
  directory = await mkdtemp(join(tmpdir(), 'pooleno-mock-api-'));
  mock = await mockApiAddress(join(directory, 'mock-api.sqlite'));
});

afterEach(async () => {
  await killAllRoles();
  await Promise.all([...heldRows].map(releaseRow));
  await pool.query(`
    DROP TRIGGER IF EXISTS fail_insert ON external_jobs;
    DROP TRIGGER IF EXISTS fail_insert ON external_quarantine;
    DROP FUNCTION IF EXISTS fail_insert;
  `);
  await deleteTestStream(stream);
  await rm(directory, { recursive: true });
});

function startExternal(env: NodeJS.ProcessEnv = {}, preload?: string): RoleProcess {
  return new RoleProcess(
    'external',
    {
      KAFKA_BROKERS: stream.kafka.brokers.join(','),
      KAFKA_TOPIC: stream.kafka.topic,
      EXTERNAL_GROUP_ID: stream.groupId,
      EXTERNAL_SESSION_TIMEOUT_MS: String(SESSION_TIMEOUT_MS),
      EXTERNAL_RESTART_DELAY_MS: '100',
      EXTERNAL_API_URL: mock.notificationsUrl,
      EXTERNAL_TIMEOUT_MS: '2000',
      EXTERNAL_POLL_INTERVAL_MS: '20',
      EXTERNAL_RETRY_BASE_MS: '50',
      EXTERNAL_RETRY_CAP_MS: '200',
      EXTERNAL_RETRY_JITTER_PERCENT: '0',
      ...env,
    },
    preload,
  );
}

const matchingGroup = (): TestStream => ({ ...stream, groupId: `${stream.groupId}-matching` });

function startMatching(): RoleProcess {
  return new RoleProcess('matching', {
    KAFKA_BROKERS: stream.kafka.brokers.join(','),
    KAFKA_TOPIC: stream.kafka.topic,
    MATCHING_GROUP_ID: matchingGroup().groupId,
    MATCHING_SESSION_TIMEOUT_MS: String(SESSION_TIMEOUT_MS),
    MATCHING_RESTART_DELAY_MS: '100',
  });
}

function startPublisher(): RoleProcess {
  return new RoleProcess('publisher', {
    KAFKA_BROKERS: stream.kafka.brokers.join(','),
    KAFKA_TOPIC: stream.kafka.topic,
    OUTBOX_POLL_INTERVAL_MS: '20',
  });
}

function eventually(assertion: () => Promise<void>): Promise<void> {
  return vi.waitFor(assertion, { timeout: 45_000, interval: 50 });
}

async function waitForJob(eventId: string, expected: Partial<StoredJob>): Promise<void> {
  await eventually(async () => {
    expect(await readJob(pool, eventId)).toMatchObject(expected);
  });
}

async function waitForCommittedMessages(count: number): Promise<void> {
  await eventually(async () => {
    expect(await committedMessages(stream)).toBe(count);
  });
}

function publish(...events: OrderAcceptedV1[]): Promise<void> {
  return produceRaw(
    stream,
    events.map((event) => ({ key: event.symbol, value: JSON.stringify(event) })),
  );
}

function runRedrive(...eventIds: string[]): Promise<{ code: number | null }> {
  return new RoleProcess('redrive-external', {}, undefined, eventIds).exited;
}

/** Holds the event's Inbox row in an open transaction, to park the consumer's handoff. */
async function holdInboxRow(eventId: string): Promise<PoolClient> {
  const holder = await pool.connect();
  heldRows.add(holder);
  await holder.query('BEGIN');
  await holder.query(
    `INSERT INTO consumer_inbox (consumer_name, event_id)
     VALUES ('external-notification-v1', $1)`,
    [eventId],
  );
  return holder;
}

async function releaseRow(holder: PoolClient): Promise<void> {
  heldRows.delete(holder);
  await holder.query('ROLLBACK');
  holder.release();
}

async function failInserts(table: 'external_jobs' | 'external_quarantine'): Promise<void> {
  await pool.query(`
    CREATE OR REPLACE FUNCTION fail_insert() RETURNS trigger LANGUAGE plpgsql AS
      $$ BEGIN RAISE EXCEPTION 'the database refused the write'; END $$;
    CREATE TRIGGER fail_insert BEFORE INSERT ON ${table}
      FOR EACH ROW EXECUTE FUNCTION fail_insert();
  `);
}

describe('external notifications through real processes', { timeout: 120_000 }, () => {
  describe('the gap between a remote effect and the local result', () => {
    it('repeats no effect when the worker dies before recording the answer', async () => {
      const destination = await startMockApi(mock);
      const event = await acceptEvent(pool, SELL);
      await scheduleNotification(pool, {
        eventId: event.eventId,
        payload: JSON.stringify(event),
      });

      const doomed = startExternal({}, CRASH_BEFORE_RECORDING_DELIVERY);

      expect((await doomed.exited).signal).toBe('SIGKILL');
      const effects = await readEffects(mock);
      expect(effects).toHaveLength(1);
      // A crash uses up none of the budget, and the job is still due.
      expect(await readJob(pool, event.eventId)).toMatchObject({ status: 'PENDING', attempts: 0 });

      startExternal();

      await waitForJob(event.eventId, { status: 'SUCCEEDED', attempts: 1 });
      await destination.waitForMatch(/"outcome":"replayed"/);
      expect(await readEffects(mock)).toEqual(effects);
    });

    it('retries a lost response under one key, with one effect, across a mock restart', async () => {
      const destination = await startMockApi(mock);
      await injectFault(mock, { mode: 'lost-response' });
      const event = await acceptEvent(pool, SELL);
      await publish(event);

      startExternal({ EXTERNAL_RETRY_BASE_MS: '2000', EXTERNAL_RETRY_CAP_MS: '2000' });
      await waitForJob(event.eventId, { status: 'PENDING', attempts: 1 });

      const effects = await readEffects(mock);
      expect(effects).toEqual([
        expect.objectContaining({
          idempotencyKey: `external-notify:${event.eventId}`,
          body: JSON.stringify(event),
        }),
      ]);
      expect((await readJob(pool, event.eventId))?.last_error).toContain('fetch failed');

      await destination.kill();
      const restarted = await startMockApi(mock);

      await waitForJob(event.eventId, { status: 'SUCCEEDED', last_error: null });
      await restarted.waitForMatch(/"outcome":"replayed"/);
      expect(await readEffects(mock)).toEqual(effects);
    });

    it('retries after a timeout that followed the remote commit, with one effect', async () => {
      await startMockApi(mock);
      await injectFault(mock, { mode: 'timeout-after-commit' });
      const event = await acceptEvent(pool, SELL);
      await publish(event);

      startExternal({
        EXTERNAL_TIMEOUT_MS: '500',
        EXTERNAL_RETRY_BASE_MS: '1000',
        EXTERNAL_RETRY_CAP_MS: '1000',
      });
      await waitForJob(event.eventId, { status: 'PENDING', attempts: 1 });
      expect((await readJob(pool, event.eventId))?.last_error).toContain('Timeout');
      expect(await readEffects(mock)).toHaveLength(1);

      await waitForJob(event.eventId, { status: 'SUCCEEDED' });
      expect(await readEffects(mock)).toHaveLength(1);
    });

    it('reclaims a job whose worker died during the call, with one effect', async () => {
      await startMockApi(mock);
      await injectFault(mock, { mode: 'timeout-after-commit' });
      const event = await acceptEvent(pool, SELL);
      await publish(event);

      const doomed = startExternal({ EXTERNAL_TIMEOUT_MS: '30000' });
      await eventually(async () => {
        expect(await readEffects(mock)).toHaveLength(1);
      });
      await doomed.kill();
      expect(await readJob(pool, event.eventId)).toMatchObject({
        status: 'PENDING',
        attempts: 0,
      });

      startExternal();

      await waitForJob(event.eventId, { status: 'SUCCEEDED', attempts: 1 });
      expect(await readEffects(mock)).toHaveLength(1);
    });
  });

  describe('independence from the destination and from matching', () => {
    it('keeps matching through an outage, and retries remain durable across a restart', async () => {
      startPublisher();
      const matcher = startMatching();
      const firstWorker = startExternal({
        EXTERNAL_RETRY_BASE_MS: '6000',
        EXTERNAL_RETRY_CAP_MS: '6000',
      });

      const seller = await acceptEvent(pool, SELL);
      const buyer = await acceptEvent(pool, BUY);

      await eventually(async () => {
        expect(await readOrder(pool, buyer.orderId)).toMatchObject({ status: 'FILLED' });
      });
      expect(await readTrades(pool)).toHaveLength(1);
      await waitForJob(seller.eventId, { status: 'PENDING', attempts: 1 });
      await waitForJob(buyer.eventId, { status: 'PENDING', attempts: 1 });
      expect(await readEffects(mock).catch(() => 'the destination is down')).toBe(
        'the destination is down',
      );

      await firstWorker.kill();
      const before = await readJob(pool, seller.eventId);
      const dueAt = before?.next_attempt_at as Date;
      expect(before?.due_in_ms).toBeGreaterThan(0);
      expect(before?.last_error).toContain('fetch failed');

      await startMockApi(mock);
      startExternal();

      // The restart forgets nothing, so the job neither loses its attempt nor goes early.
      await waitForJob(seller.eventId, { status: 'SUCCEEDED', attempts: 2 });
      await waitForJob(buyer.eventId, { status: 'SUCCEEDED', attempts: 2 });
      const after = await readJob(pool, seller.eventId);
      expect((after?.completed_at as Date).getTime()).toBeGreaterThanOrEqual(dueAt.getTime());

      expect((await readEffects(mock)).map((effect) => effect.idempotencyKey).sort()).toEqual(
        [`external-notify:${seller.eventId}`, `external-notify:${buyer.eventId}`].sort(),
      );
      expect(matcher.countLogs('partition held; restoring its book before retrying')).toBe(0);
    });

    it('delivers an event that matching is blocked behind, because the groups are separate', async () => {
      await startMockApi(mock);
      const event = await acceptEvent(pool, SELL);
      await produceRaw(stream, [
        { key: event.symbol, value: 'not an event' },
        { key: event.symbol, value: JSON.stringify(event) },
      ]);

      const matcher = startMatching();
      startExternal();

      await waitForJob(event.eventId, { status: 'SUCCEEDED' });
      await waitForCommittedMessages(2);
      await matcher.waitForLog('partition held; restoring its book before retrying', 2);
      expect(await committedMessages(matchingGroup())).toBe(0);
      expect(await readOrder(pool, event.orderId)).toMatchObject({ status: 'QUEUED' });
    });

    it('delivers existing jobs while Kafka is unreachable', async () => {
      await startMockApi(mock);
      const event = await acceptEvent(pool, SELL);
      await scheduleNotification(pool, {
        eventId: event.eventId,
        payload: JSON.stringify(event),
      });

      const worker = startExternal({ KAFKA_BROKERS: CLOSED_PORT });

      await waitForJob(event.eventId, { status: 'SUCCEEDED' });
      await worker.waitForLog('external consumer stopped; resuming from the committed offset');
      expect(await readEffects(mock)).toHaveLength(1);
    });

    it('leaves the order alone when retries run out', async () => {
      const event = await acceptEvent(pool, SELL);
      await publish(event);

      startExternal({ EXTERNAL_MAX_ATTEMPTS: '2' });

      await waitForJob(event.eventId, { status: 'FAILED', attempts: 2 });
      expect((await readJob(pool, event.eventId))?.last_error).toContain('fetch failed');
      expect(await readOrder(pool, event.orderId)).toMatchObject({ status: 'QUEUED' });
    });
  });

  describe('offsets of the handoff', () => {
    it('creates no second job when it dies between its commit and the offset', async () => {
      const event = await acceptEvent(pool, SELL);
      await publish(event);

      // The event has offset 0, so finishing it means committing offset 1.
      const doomed = startExternal(
        { CRASH_BEFORE_COMMITTING_OFFSET: '1' },
        CRASH_BEFORE_OFFSET_COMMIT,
      );
      expect((await doomed.exited).signal).toBe('SIGKILL');
      expect(await countJobs(pool)).toBe(1);
      expect(await committedMessages(stream)).toBe(0);

      const restarted = startExternal();

      await restarted.waitForLog('event already scheduled');
      await waitForCommittedMessages(1);
      expect(await countJobs(pool)).toBe(1);
    });

    it('leaves no job and no offset when it dies inside the handoff transaction', async () => {
      await startMockApi(mock);
      const event = await acceptEvent(pool, SELL);
      const holder = await holdInboxRow(event.eventId);
      await publish(event);

      const doomed = startExternal();
      await waitForBlockedBackend(pool);

      await doomed.kill();
      await releaseRow(holder);

      expect(await countJobs(pool)).toBe(0);
      expect(await committedMessages(stream)).toBe(0);

      startExternal();

      await waitForJob(event.eventId, { status: 'SUCCEEDED', attempts: 1 });
      await waitForCommittedMessages(1);
      expect(await countJobs(pool)).toBe(1);
      expect(await readEffects(mock)).toHaveLength(1);
    });

    it('finishes its transaction on SIGTERM and commits nothing beyond durable work', async () => {
      const event = await acceptEvent(pool, SELL);
      const holder = await holdInboxRow(event.eventId);
      await publish(event);

      const external = startExternal();
      await waitForBlockedBackend(pool);

      const exit = external.terminate();
      await external.waitForLog('shutting down');
      expect(await countJobs(pool)).toBe(0);
      await releaseRow(holder);

      expect(await exit).toEqual(CLEAN_EXIT);
      expect(await countJobs(pool)).toBe(1);
      expect(await committedMessages(stream)).toBe(0);

      const restarted = startExternal();

      await restarted.waitForLog('event already scheduled');
      await waitForCommittedMessages(1);
      expect(await countJobs(pool)).toBe(1);
    });

    it('commits no offset after a rebalance takes its partitions', async () => {
      const event = await acceptEvent(pool, SELL);
      const holder = await holdInboxRow(event.eventId);
      await publish(event);

      const external = startExternal();
      await waitForBlockedBackend(pool);

      const rival = joinAsRival(stream, SESSION_TIMEOUT_MS);
      await external.waitForLog('group membership changed; stopping this session');
      await releaseRow(holder);
      await eventually(async () => {
        expect(await countJobs(pool)).toBe(1);
      });
      await rival.joined;
      expect(await committedMessages(stream)).toBe(0);

      await rival.leave();

      await external.waitForLog('event already scheduled');
      await waitForCommittedMessages(1);
      expect(await countJobs(pool)).toBe(1);
    });

    it('does not advance the offset until the job is stored', async () => {
      await failInserts('external_jobs');
      const event = await acceptEvent(pool, SELL);
      await publish(event);

      const external = startExternal();

      await external.waitForLog('external consumer stopped; resuming from the committed offset', 2);
      expect(await committedMessages(stream)).toBe(0);
      expect(await countInbox(pool)).toBe(0);

      await pool.query('DROP TRIGGER fail_insert ON external_jobs');

      await waitForCommittedMessages(1);
      expect(await countJobs(pool)).toBe(1);
    });
  });

  describe('the event a job carries', () => {
    it('reaches the destination byte for byte as it arrived from Kafka', async () => {
      await startMockApi(mock);
      const event = await acceptEvent(pool, SELL);
      const spelledDifferently = JSON.stringify(event, null, 2);
      await produceRaw(stream, [{ key: event.symbol, value: spelledDifferently }]);

      startExternal();

      await waitForJob(event.eventId, { status: 'SUCCEEDED' });
      expect((await readEffects(mock)).map(({ body }) => body)).toEqual([spelledDifferently]);
    });
  });

  describe('malformed messages', () => {
    it('quarantines each one, then moves on to the valid event behind them', async () => {
      await startMockApi(mock);
      const event = await acceptEvent(pool, SELL);
      const key = event.symbol;
      const wrongShape = JSON.stringify({ eventId: 'not a uuid' });
      await produceRaw(stream, [
        { key, value: 'not\0json' },
        { key, value: wrongShape },
        { key },
        { key, value: JSON.stringify(event) },
      ]);

      const external = startExternal();

      await waitForJob(event.eventId, { status: 'SUCCEEDED' });
      await waitForCommittedMessages(4);
      expect(external.countLogs('malformed message quarantined')).toBe(3);

      const quarantined = await readQuarantine(pool);
      expect(quarantined.map((row) => row.kafka_offset)).toEqual(['0', '1', '2']);
      expect(quarantined.map((row) => row.raw_value.toString())).toEqual([
        'not\0json',
        wrongShape,
        '',
      ]);
      expect(quarantined.every((row) => row.topic === stream.kafka.topic)).toBe(true);
      expect(quarantined.every((row) => row.error.length > 0)).toBe(true);
    });

    it('does not move past one until it is durably recorded', async () => {
      await failInserts('external_quarantine');
      await produceRaw(stream, [{ key: 'ETH-USD', value: 'not an event' }]);

      const external = startExternal();

      await external.waitForLog('external consumer stopped; resuming from the committed offset', 2);
      expect(await committedMessages(stream)).toBe(0);

      await pool.query('DROP TRIGGER fail_insert ON external_quarantine');

      await waitForCommittedMessages(1);
      expect(await readQuarantine(pool)).toHaveLength(1);
    });
  });

  describe('classifying what the destination says', () => {
    it('honours Retry-After on a transient status, then delivers once', async () => {
      await startMockApi(mock);
      await injectFault(mock, { mode: 'status', status: 503, retryAfter: '2' });
      const event = await acceptEvent(pool, SELL);
      await publish(event);

      startExternal({ EXTERNAL_RETRY_CAP_MS: '5000' });

      await waitForJob(event.eventId, { status: 'SUCCEEDED', attempts: 2 });
      const job = await readJob(pool, event.eventId);
      const elapsed = (job?.completed_at as Date).getTime() - (job?.created_at as Date).getTime();
      expect(elapsed).toBeGreaterThanOrEqual(2_000);
      expect(await readEffects(mock)).toHaveLength(1);
    });

    it('fails on a conflicting body, and leaves the earlier effect alone', async () => {
      await startMockApi(mock);
      const event = await acceptEvent(pool, SELL);
      const idempotencyKey = `external-notify:${event.eventId}`;
      const seeded = await postNotification(mock, '{"another":"body"}', {
        'idempotency-key': idempotencyKey,
      });
      expect(seeded.status).toBe(201);
      await publish(event);

      startExternal();

      await waitForJob(event.eventId, { status: 'FAILED', attempts: 1 });
      expect((await readJob(pool, event.eventId))?.last_error).toContain('IDEMPOTENCY_KEY_REUSED');
      expect(await readEffects(mock)).toEqual([
        expect.objectContaining({ idempotencyKey, body: '{"another":"body"}' }),
      ]);
    });

    it('fails on a permanent status, and a manual redrive delivers under the same key', async () => {
      await startMockApi(mock);
      await injectFault(mock, { mode: 'status', status: 400 });
      const event = await acceptEvent(pool, SELL);
      await publish(event);

      startExternal();

      await waitForJob(event.eventId, { status: 'FAILED', attempts: 1 });
      expect((await readJob(pool, event.eventId))?.last_error).toContain('HTTP 400');
      expect(await readEffects(mock)).toEqual([]);

      await expect(runRedrive(event.eventId)).resolves.toMatchObject(CLEAN_EXIT);

      await waitForJob(event.eventId, {
        status: 'SUCCEEDED',
        attempts: 1,
        target_key: `external-notify:${event.eventId}`,
      });
      expect(await readEffects(mock)).toEqual([
        expect.objectContaining({
          idempotencyKey: `external-notify:${event.eventId}`,
          body: JSON.stringify(event),
        }),
      ]);
    });

    it('reports a redrive that did nothing as a failure', async () => {
      await expect(runRedrive()).resolves.toMatchObject({ code: 1 });
      await expect(runRedrive('00000000-0000-4000-8000-000000000000')).resolves.toMatchObject({
        code: 1,
      });
    });

    it('still redrives the other jobs when one event ID is not valid, and reports it', async () => {
      await startMockApi(mock);
      await injectFault(mock, { mode: 'status', status: 400 });
      const event = await acceptEvent(pool, SELL);
      await publish(event);
      startExternal();
      await waitForJob(event.eventId, { status: 'FAILED', attempts: 1 });

      await expect(runRedrive('not-a-uuid', event.eventId)).resolves.toMatchObject({ code: 1 });

      await waitForJob(event.eventId, { status: 'SUCCEEDED', attempts: 1 });
    });
  });

  describe('shutting down the worker', () => {
    it('finishes the attempt in flight on SIGTERM, within the timeout', async () => {
      const destination = await startMockApi(mock);
      await injectFault(mock, { mode: 'timeout' });
      const event = await acceptEvent(pool, SELL);
      await publish(event);

      const worker = startExternal({ EXTERNAL_TIMEOUT_MS: '1500' });
      await destination.waitForMatch(NOTIFICATION_REQUEST);

      await expect(worker.terminate()).resolves.toEqual(CLEAN_EXIT);

      expect(await readJob(pool, event.eventId)).toMatchObject({
        status: 'PENDING',
        attempts: 1,
      });
    });
  });

  describe('single ownership of the external role', () => {
    it('keeps a standby waiting until the owner dies, then hands over', async () => {
      await startMockApi(mock);
      const owner = startExternal();
      await owner.waitForLog('acquired ownership');
      const standby = startExternal();
      await standby.waitForLog('another process owns this role; waiting');

      const first = await acceptEvent(pool, SELL);
      await publish(first);
      await waitForJob(first.eventId, { status: 'SUCCEEDED' });
      expect(standby.countLogs('acquired ownership')).toBe(0);

      await owner.kill();
      await standby.waitForLog('acquired ownership');

      const second = await acceptEvent(pool, SELL);
      await publish(second);
      await waitForJob(second.eventId, { status: 'SUCCEEDED' });
    });

    it('stops with an error when the connection holding ownership is lost', async () => {
      const owner = startExternal();
      await owner.waitForLog('acquired ownership');

      expect(await terminateAdvisoryLockHolders(pool)).toBe(1);

      expect(await owner.exited).toEqual({ code: 1, signal: null });
    });

    it('leaves cleanly on SIGTERM while still waiting for ownership', async () => {
      const owner = startExternal();
      await owner.waitForLog('acquired ownership');
      const standby = startExternal();
      await standby.waitForLog('another process owns this role; waiting');

      expect(await standby.terminate()).toEqual(CLEAN_EXIT);
      expect(await owner.terminate()).toEqual(CLEAN_EXIT);
    });
  });

  describe('the mock destination', () => {
    it('replays the stored response for the same key and body', async () => {
      await startMockApi(mock);

      const first = await postNotification(mock, '{"a":1}');
      const second = await postNotification(mock, '{"a":1}');

      expect(first.status).toBe(201);
      expect(first.headers.get('idempotent-replayed')).toBeNull();
      expect(second.status).toBe(201);
      expect(second.headers.get('idempotent-replayed')).toBe('true');
      expect(await second.json()).toEqual(await first.json());
      expect(await readEffects(mock)).toHaveLength(1);
    });

    it('rejects a different body under the same key', async () => {
      await startMockApi(mock);
      await postNotification(mock, '{"a":1}');

      const conflict = await postNotification(mock, '{"a":2}');

      expect(conflict.status).toBe(422);
      expect(await conflict.json()).toEqual({ code: 'IDEMPOTENCY_KEY_REUSED' });
      expect(await readEffects(mock)).toHaveLength(1);
    });

    it('refuses a request with no key or an unreadable body', async () => {
      await startMockApi(mock);

      expect((await postNotification(mock, '{"a":1}', {})).status).toBe(400);
      expect((await postNotification(mock, 'not json')).status).toBe(400);
      expect(await readEffects(mock)).toEqual([]);
    });

    it('applies an injected fault the given number of times, until cleared', async () => {
      await startMockApi(mock);
      await injectFault(mock, { mode: 'status', status: 503, retryAfter: '9', times: 3 });

      const faulted = await postNotification(mock, '{"a":1}');
      expect(faulted.status).toBe(503);
      expect(faulted.headers.get('retry-after')).toBe('9');
      expect((await postNotification(mock, '{"a":1}')).status).toBe(503);

      await clearFaults(mock);

      expect((await postNotification(mock, '{"a":1}')).status).toBe(201);
    });

    it('stops cleanly on SIGTERM with a request left unanswered', async () => {
      const destination = await startMockApi(mock);
      await injectFault(mock, { mode: 'timeout' });
      const unanswered = postNotification(mock, '{"a":1}').catch(() => 'connection closed');
      await destination.waitForMatch(NOTIFICATION_REQUEST);

      await expect(destination.terminate()).resolves.toEqual(CLEAN_EXIT);
      await expect(unanswered).resolves.toBe('connection closed');
    });
  });
});
