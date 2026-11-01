import { randomUUID } from 'node:crypto';
import { Pool } from 'pg';
import { pino } from 'pino';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import type { DeliveryOutcome } from '../../src/modules/external/external-client.js';
import {
  findDueJobs,
  markJobSucceeded,
  redriveJob,
  type DueJob,
} from '../../src/modules/external/external-job.repository.js';
import {
  deliverDueJobs,
  runNotificationWorker,
} from '../../src/modules/external/notification-worker.js';
import { scheduleNotification } from '../../src/modules/external/schedule-notification.js';
import { claimInboxEvent } from '../../src/modules/messaging/inbox.repository.js';
import { withTransaction } from '../../src/infrastructure/transaction.js';
import { readJob } from '../support/external-jobs.js';
import { countInbox, resetDatabase, TEST_DATABASE_URL } from '../support/test-database.js';

const POLICY = { baseMs: 60_000, capMs: 120_000, jitterPercent: 0, maxAttempts: 3 };
const TRANSIENT: DeliveryOutcome = { kind: 'transient', error: 'down', retryAfterMs: undefined };
const silentLog = pino({ level: 'silent' });

let pool: Pool;

beforeAll(() => {
  pool = new Pool({ connectionString: TEST_DATABASE_URL });
});

afterAll(() => pool.end());

beforeEach(() => resetDatabase(pool));

afterEach(() =>
  pool.query(`
    DROP TRIGGER IF EXISTS fail_job ON external_jobs;
    DROP FUNCTION IF EXISTS fail_job;
  `),
);

async function schedule(payload = '{ "from" : "the event" }'): Promise<string> {
  const eventId = randomUUID();
  await scheduleNotification(pool, { eventId, payload });
  return eventId;
}

function runPass(
  deliver: (job: DueJob) => Promise<DeliveryOutcome>,
  retry = POLICY,
): Promise<number> {
  return deliverDueJobs({
    pool,
    deliver,
    retry,
    signal: new AbortController().signal,
    log: silentLog,
  });
}

function always(outcome: DeliveryOutcome) {
  return () => Promise.resolve(outcome);
}

/** Runs the retry forward, as if its delay had passed. */
async function makeDue(eventId: string): Promise<void> {
  await pool.query(
    `UPDATE external_jobs SET next_attempt_at = now() - interval '1 second' WHERE event_id = $1`,
    [eventId],
  );
}

async function installFailingJobTrigger(event: 'INSERT' | 'UPDATE'): Promise<void> {
  await pool.query(`
    CREATE FUNCTION fail_job() RETURNS trigger LANGUAGE plpgsql AS
      $$ BEGIN RAISE EXCEPTION 'the job could not be stored'; END $$;
    CREATE TRIGGER fail_job BEFORE ${event} ON external_jobs
      FOR EACH ROW EXECUTE FUNCTION fail_job();
  `);
}

describe('handing an event to the worker', () => {
  it('creates a due job holding the original bytes and the fixed key', async () => {
    const eventId = randomUUID();
    const payload = '{ "from" :  "the event" }';

    await expect(scheduleNotification(pool, { eventId, payload })).resolves.toBe(true);

    expect(await readJob(pool, eventId)).toMatchObject({
      target_key: `external-notify:${eventId}`,
      payload,
      status: 'PENDING',
      attempts: 0,
      last_error: null,
      completed_at: null,
    });
    expect((await findDueJobs(pool, 10)).map((job) => job.eventId)).toEqual([eventId]);
  });

  it('creates no second job when the event is redelivered', async () => {
    const eventId = randomUUID();

    await scheduleNotification(pool, { eventId, payload: '{"attempt":1}' });
    await expect(scheduleNotification(pool, { eventId, payload: '{"attempt":2}' })).resolves.toBe(
      false,
    );

    expect((await readJob(pool, eventId))?.payload).toBe('{"attempt":1}');
    expect(await countInbox(pool)).toBe(1);
  });

  it('does not mistake matching having handled an event for its own handoff', async () => {
    const eventId = randomUUID();
    await withTransaction(pool, (tx) => claimInboxEvent(tx, 'matching-v1', eventId));

    await expect(scheduleNotification(pool, { eventId, payload: '{}' })).resolves.toBe(true);

    expect(await countInbox(pool)).toBe(2);
  });

  it('records the Inbox row and the job together or not at all', async () => {
    await installFailingJobTrigger('INSERT');

    await expect(schedule()).rejects.toThrow('the job could not be stored');

    expect(await countInbox(pool)).toBe(0);
  });
});

describe('attempting due jobs', () => {
  it('finishes a delivered job, and sends the original payload under the fixed key', async () => {
    const eventId = await schedule('{ "from" : "the event" }');
    const sent: DueJob[] = [];

    await runPass((job) => {
      sent.push(job);
      return Promise.resolve({ kind: 'delivered' });
    });

    expect(sent).toEqual([
      {
        eventId,
        targetKey: `external-notify:${eventId}`,
        payload: '{ "from" : "the event" }',
        attempts: 0,
        logContext: { requestId: null, orderId: null, symbol: null, acceptanceSequence: null },
      },
    ]);
    const job = await readJob(pool, eventId);
    expect(job).toMatchObject({
      status: 'SUCCEEDED',
      attempts: 1,
      next_attempt_at: null,
      last_error: null,
    });
    expect(job?.completed_at).toBeInstanceOf(Date);
    await expect(runPass(always({ kind: 'delivered' }))).resolves.toBe(0);
  });

  it('records a transient failure and leaves the job alone until it is due', async () => {
    const eventId = await schedule();

    await runPass(always(TRANSIENT));

    const job = await readJob(pool, eventId);
    expect(job).toMatchObject({
      status: 'PENDING',
      attempts: 1,
      last_error: 'down',
      completed_at: null,
    });
    // With no jitter, the first delay is exactly the base, from this attempt.
    expect(job?.due_in_ms).toBeGreaterThan(POLICY.baseMs - 5_000);
    expect(job?.due_in_ms).toBeLessThanOrEqual(POLICY.baseMs);

    await expect(runPass(always({ kind: 'delivered' }))).resolves.toBe(0);
    expect(await readJob(pool, eventId)).toMatchObject({ status: 'PENDING', attempts: 1 });
  });

  it('clears the last error once a retry succeeds', async () => {
    const eventId = await schedule();
    await runPass(always(TRANSIENT));
    await makeDue(eventId);

    await runPass(always({ kind: 'delivered' }));

    expect(await readJob(pool, eventId)).toMatchObject({
      status: 'SUCCEEDED',
      attempts: 2,
      last_error: null,
    });
  });

  it('waits at least as long as Retry-After asks', async () => {
    const eventId = await schedule();

    await runPass(always({ kind: 'transient', error: 'busy', retryAfterMs: 90_000 }));

    const job = await readJob(pool, eventId);
    expect(job?.due_in_ms).toBeGreaterThan(85_000);
    expect(job?.due_in_ms).toBeLessThanOrEqual(90_000);
  });

  it('honours Retry-After only up to the cap', async () => {
    const eventId = await schedule();

    await runPass(always({ kind: 'transient', error: 'busy', retryAfterMs: 86_400_000 }));

    expect((await readJob(pool, eventId))?.due_in_ms).toBeLessThanOrEqual(POLICY.capMs);
  });

  it('fails a permanently rejected job at once, and leaves it to inspect', async () => {
    const eventId = await schedule();

    await runPass(always({ kind: 'permanent', error: 'HTTP 400: refused' }));

    const job = await readJob(pool, eventId);
    expect(job).toMatchObject({
      status: 'FAILED',
      attempts: 1,
      last_error: 'HTTP 400: refused',
      next_attempt_at: null,
    });
    expect(job?.completed_at).toBeInstanceOf(Date);
    await expect(runPass(always({ kind: 'delivered' }))).resolves.toBe(0);
  });

  it('fails a job once its attempts are used up', async () => {
    const eventId = await schedule();

    for (const attempt of [1, 2, 3]) {
      await runPass(always(TRANSIENT));
      expect((await readJob(pool, eventId))?.attempts).toBe(attempt);
      if (attempt < POLICY.maxAttempts) {
        expect((await readJob(pool, eventId))?.status).toBe('PENDING');
        await makeDue(eventId);
      }
    }

    expect(await readJob(pool, eventId)).toMatchObject({ status: 'FAILED', last_error: 'down' });
  });

  it('does not let a failing job hold back the others', async () => {
    const failing = await schedule();
    const healthy = await schedule();

    await runPass((job) =>
      Promise.resolve(job.eventId === failing ? TRANSIENT : { kind: 'delivered' }),
    );

    expect((await readJob(pool, failing))?.status).toBe('PENDING');
    expect((await readJob(pool, healthy))?.status).toBe('SUCCEEDED');
  });

  it('leaves the job due when its result cannot be recorded, to repeat under the same key', async () => {
    const eventId = await schedule();
    await installFailingJobTrigger('UPDATE');

    await expect(runPass(always({ kind: 'delivered' }))).rejects.toThrow(
      'the job could not be stored',
    );

    expect(await readJob(pool, eventId)).toMatchObject({ status: 'PENDING', attempts: 0 });
    await pool.query('DROP TRIGGER fail_job ON external_jobs');
    const sent: DueJob[] = [];
    await runPass((job) => {
      sent.push(job);
      return Promise.resolve({ kind: 'delivered' });
    });
    expect(sent[0]?.targetKey).toBe(`external-notify:${eventId}`);
    expect((await readJob(pool, eventId))?.status).toBe('SUCCEEDED');
  });

  it('does not let a stale attempt overwrite what a newer one recorded', async () => {
    const eventId = await schedule();
    const [stale] = await findDueJobs(pool, 1);

    await runPass(always(TRANSIENT));

    await expect(markJobSucceeded(pool, stale as DueJob)).resolves.toBe(false);
    expect(await readJob(pool, eventId)).toMatchObject({ status: 'PENDING', attempts: 1 });
  });

  it('does not let an error from outside stop the result being recorded', async () => {
    const eventId = await schedule();

    await runPass(always({ kind: 'permanent', error: `HTTP 400: a\0b${'c'.repeat(1_000)}` }));

    const job = await readJob(pool, eventId);
    expect(job?.status).toBe('FAILED');
    expect(job?.last_error).toMatch(/^HTTP 400: abc+$/);
    expect(job?.last_error).toHaveLength(500);
  });

  it('stops before the next job once told to', async () => {
    await schedule();
    await schedule();
    const controller = new AbortController();

    const attempted = await deliverDueJobs({
      pool,
      deliver: () => {
        controller.abort();
        return Promise.resolve({ kind: 'delivered' });
      },
      retry: POLICY,
      signal: controller.signal,
      log: silentLog,
    });

    expect(attempted).toBe(1);
  });
});

describe('the worker loop', () => {
  function startWorker(options: { assertHeld?: () => Promise<void>; deliver: () => void }) {
    const controller = new AbortController();
    const finished = runNotificationWorker({
      pool,
      deliver: () => {
        options.deliver();
        return Promise.resolve({ kind: 'delivered' });
      },
      retry: POLICY,
      pollIntervalMs: 5,
      restartDelayMs: 5,
      assertHeld: options.assertHeld ?? (() => Promise.resolve()),
      signal: controller.signal,
      log: silentLog,
    });
    return {
      stop: () => {
        controller.abort();
      },
      finished,
    };
  }

  it('survives a failed pass, and finishes the job from durable state', async () => {
    const eventId = await schedule();
    await installFailingJobTrigger('UPDATE');
    let calls = 0;

    const worker = startWorker({ deliver: () => (calls += 1) });
    await vi.waitFor(() => {
      expect(calls).toBeGreaterThan(1);
    });
    expect((await readJob(pool, eventId))?.status).toBe('PENDING');

    await pool.query('DROP TRIGGER fail_job ON external_jobs');
    await vi.waitFor(async () => {
      expect((await readJob(pool, eventId))?.status).toBe('SUCCEEDED');
    });

    worker.stop();
    await expect(worker.finished).resolves.toBeUndefined();
  });

  it('delivers nothing once ownership cannot be confirmed', async () => {
    await schedule();
    let checks = 0;
    let calls = 0;

    const worker = startWorker({
      assertHeld: () => {
        checks += 1;
        return Promise.reject(new Error('ownership was lost'));
      },
      deliver: () => (calls += 1),
    });
    await vi.waitFor(() => {
      expect(checks).toBeGreaterThan(1);
    });

    worker.stop();
    await worker.finished;
    expect(calls).toBe(0);
  });
});

describe('redriving a job', () => {
  it('returns a failed job with a new budget, and the same event and key', async () => {
    const eventId = await schedule();
    await runPass(always({ kind: 'permanent', error: 'HTTP 400: refused' }));

    await expect(redriveJob(pool, eventId)).resolves.toBe(true);

    expect(await readJob(pool, eventId)).toMatchObject({
      status: 'PENDING',
      attempts: 0,
      target_key: `external-notify:${eventId}`,
      completed_at: null,
      // Stays visible until the next attempt has a result of its own.
      last_error: 'HTTP 400: refused',
    });
    const sent: DueJob[] = [];
    await runPass((job) => {
      sent.push(job);
      return Promise.resolve({ kind: 'delivered' });
    });
    expect(sent.map((job) => job.eventId)).toEqual([eventId]);
    expect(await readJob(pool, eventId)).toMatchObject({ status: 'SUCCEEDED', last_error: null });
  });

  it('refuses a job that has not failed', async () => {
    const pending = await schedule();
    const succeeded = await schedule();
    await runPass((job) =>
      Promise.resolve(job.eventId === succeeded ? { kind: 'delivered' } : TRANSIENT),
    );

    await expect(redriveJob(pool, pending)).resolves.toBe(false);
    await expect(redriveJob(pool, succeeded)).resolves.toBe(false);
    await expect(redriveJob(pool, randomUUID())).resolves.toBe(false);

    expect((await readJob(pool, pending))?.attempts).toBe(1);
    expect((await readJob(pool, succeeded))?.status).toBe('SUCCEEDED');
  });
});
