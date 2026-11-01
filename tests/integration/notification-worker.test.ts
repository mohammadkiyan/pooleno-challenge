import { randomUUID } from 'node:crypto';
import { Pool } from 'pg';
import { pino } from 'pino';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import type { DeliveryOutcome } from '../../src/modules/external/external-client.js';
import type { DueJob } from '../../src/modules/external/external-job.repository.js';
import { deliverDueJobs } from '../../src/modules/external/notification-worker.js';
import { scheduleNotification } from '../../src/modules/external/schedule-notification.js';
import { readJob } from '../support/external-jobs.js';
import { resetDatabase, TEST_DATABASE_URL } from '../support/test-database.js';

const RETRY = { baseMs: 1_000, capMs: 60_000, jitterPercent: 0, maxAttempts: 2 };

interface LogLine {
  msg: string;
  [field: string]: unknown;
}

function recordingLog() {
  const lines: LogLine[] = [];
  const log = pino(
    { level: 'info' },
    {
      write: (line: string) => {
        lines.push(JSON.parse(line) as LogLine);
      },
    },
  );
  return { log, lines };
}

let pool: Pool;

beforeAll(() => {
  pool = new Pool({ connectionString: TEST_DATABASE_URL });
});

afterAll(() => pool.end());

beforeEach(() => resetDatabase(pool));

async function schedule(payload: string): Promise<string> {
  const eventId = randomUUID();
  await scheduleNotification(pool, { eventId, payload });
  return eventId;
}

describe('a stored payload that PostgreSQL cannot read values from', () => {
  it.each([
    ['a NUL', String.raw`{"orderId":"o-1","symbol":"BTC\u0000USD"}`],
    ['a lone surrogate', String.raw`{"orderId":"o-1","symbol":"BTC\ud800USD"}`],
    ['a NUL under a duplicate key', String.raw`{"orderId":"o-1","symbol":"\u0000","symbol":"BTC-USD"}`],
  ])('with %s stops no delivery, its own included', async (_name, payload) => {
    const poisoned = await schedule(payload);
    const healthy = await schedule('{"orderId":"o-2","symbol":"ETH-USD"}');
    const { log, lines } = recordingLog();
    const sent: string[] = [];

    const attempted = await deliverDueJobs({
      pool,
      deliver: (job) => {
        sent.push(job.payload);
        return Promise.resolve({ kind: 'delivered' });
      },
      retry: RETRY,
      signal: new AbortController().signal,
      log,
    });

    expect(attempted).toBe(2);
    expect(sent).toContain(payload);
    expect(await readJob(pool, poisoned)).toMatchObject({ status: 'SUCCEEDED' });
    expect(await readJob(pool, healthy)).toMatchObject({ status: 'SUCCEEDED' });
    expect(lines.map((line) => line.orderId).sort()).toEqual(['o-1', 'o-2']);
  });
});

describe('a result that arrives after the job has changed', () => {
  const OUTCOMES: [string, DeliveryOutcome, string][] = [
    ['delivered', { kind: 'delivered' }, 'notification delivered'],
    ['permanent', { kind: 'permanent', error: 'refused' }, 'notification rejected; job failed'],
    [
      'transient',
      { kind: 'transient', error: 'unavailable', retryAfterMs: undefined },
      'delivery failed; will retry',
    ],
  ];

  async function attemptWhileAnotherOwnerRecords(outcome: DeliveryOutcome, retry = RETRY) {
    const eventId = await schedule('{"orderId":"o-1","symbol":"BTC-USD"}');
    const { log, lines } = recordingLog();

    await deliverDueJobs({
      pool,
      deliver: async ({ eventId: id }: DueJob) => {
        await pool.query('UPDATE external_jobs SET attempts = attempts + 1 WHERE event_id = $1', [id]);
        return outcome;
      },
      retry,
      signal: new AbortController().signal,
      log,
    });
    return { eventId, lines };
  }

  it.each(OUTCOMES)('logs only that it went unrecorded, for %s', async (kind, outcome, claim) => {
    const { lines } = await attemptWhileAnotherOwnerRecords(outcome);

    expect(lines.map((line) => line.msg)).not.toContain(claim);
    expect(lines).toEqual([
      expect.objectContaining({
        msg: 'job changed during the attempt; its result was not recorded',
        outcome: kind,
        orderId: 'o-1',
      }),
    ]);
  });

  it('does not claim that retries ran out', async () => {
    const { lines } = await attemptWhileAnotherOwnerRecords(
      { kind: 'transient', error: 'unavailable', retryAfterMs: undefined },
      { ...RETRY, maxAttempts: 1 },
    );

    expect(lines.map((line) => line.msg)).toEqual([
      'job changed during the attempt; its result was not recorded',
    ]);
  });

  it('still logs a result that was recorded', async () => {
    const eventId = await schedule('{"orderId":"o-1","symbol":"BTC-USD"}');
    const { log, lines } = recordingLog();

    await deliverDueJobs({
      pool,
      deliver: () => Promise.resolve({ kind: 'delivered' }),
      retry: RETRY,
      signal: new AbortController().signal,
      log,
    });

    expect(lines.map((line) => line.msg)).toEqual(['notification delivered']);
    expect(await readJob(pool, eventId)).toMatchObject({ status: 'SUCCEEDED' });
  });
});
