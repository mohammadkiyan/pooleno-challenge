import type { Pool } from 'pg';
import type { Logger } from 'pino';
import type { NotificationRetryPolicy } from '../../app/config.js';
import { pause } from '../../infrastructure/pause.js';
import type { DeliveryOutcome } from './external-client.js';
import {
  findDueJobs,
  markJobFailed,
  markJobSucceeded,
  scheduleJobRetry,
  type DueJob,
} from './external-job.repository.js';

const BATCH_SIZE = 100;

export interface DeliveryPass {
  pool: Pool;
  deliver: (job: DueJob) => Promise<DeliveryOutcome>;
  retry: NotificationRetryPolicy;
  signal: AbortSignal;
  log: Logger;
}

/**
 * Attempts each due job once, one at a time, and records how each ended. A job that fails
 * only delays itself, because its next attempt moves behind the other due jobs. Shutdown
 * lets the attempt in flight finish, which the delivery timeout bounds. Resolves to the
 * number of jobs attempted.
 *
 * If recording a result fails, the job stays due and the next attempt uses the same key,
 * so the destination replays what it already did.
 */
export async function deliverDueJobs(pass: DeliveryPass): Promise<number> {
  const { pool, deliver, signal } = pass;
  const jobs = await findDueJobs(pool, BATCH_SIZE);

  let attempted = 0;
  for (const job of jobs) {
    if (signal.aborted) {
      break;
    }
    await recordOutcome(pass, job, await deliver(job));
    attempted += 1;
  }
  return attempted;
}

async function recordOutcome(
  { pool, retry, log }: DeliveryPass,
  job: DueJob,
  outcome: DeliveryOutcome,
): Promise<void> {
  const attempts = job.attempts + 1;
  const context = { eventId: job.eventId, attempts };
  let recorded: boolean;

  if (outcome.kind === 'delivered') {
    recorded = await markJobSucceeded(pool, job);
    log.info(context, 'notification delivered');
  } else if (outcome.kind === 'permanent') {
    recorded = await markJobFailed(pool, job, outcome.error);
    log.error({ ...context, error: outcome.error }, 'notification rejected; job failed');
  } else if (attempts >= retry.maxAttempts) {
    recorded = await markJobFailed(pool, job, outcome.error);
    log.error({ ...context, error: outcome.error }, 'retries exhausted; job failed');
  } else {
    const retryDelayMs = computeRetryDelayMs(attempts, retry, outcome.retryAfterMs);
    recorded = await scheduleJobRetry(pool, job, { error: outcome.error, retryDelayMs });
    log.warn({ ...context, error: outcome.error, retryDelayMs }, 'delivery failed; will retry');
  }

  if (!recorded) {
    log.warn(context, 'job changed during the attempt; its result was not recorded');
  }
}

/**
 * Exponential backoff up to the cap, less a random share set by the jitter, so that jobs which
 * failed together do not retry together. `Retry-After` is a minimum, honoured up to the cap,
 * so that a destination cannot park a job indefinitely.
 */
export function computeRetryDelayMs(
  attempts: number,
  { baseMs, capMs, jitterPercent }: NotificationRetryPolicy,
  retryAfterMs = 0,
  random: () => number = Math.random,
): number {
  const backoff = Math.min(capMs, baseMs * 2 ** (attempts - 1));
  const jittered = backoff * (1 - (jitterPercent / 100) * random());
  return Math.ceil(Math.max(jittered, Math.min(retryAfterMs, capMs)));
}

/**
 * Delivers until `signal` aborts. All progress is in the database, so a failed pass needs
 * only a pause before the next. Checking ownership first stops the loop if the lock's
 * connection has gone quiet without yet reporting an error.
 */
export async function runNotificationWorker(
  pass: DeliveryPass & {
    assertHeld: () => Promise<void>;
    pollIntervalMs: number;
    restartDelayMs: number;
  },
): Promise<void> {
  const { assertHeld, pollIntervalMs, restartDelayMs, signal, log } = pass;

  while (!signal.aborted) {
    try {
      await assertHeld();
      if ((await deliverDueJobs(pass)) === 0) {
        await pause(pollIntervalMs, signal);
      }
    } catch (error) {
      log.error({ err: error }, 'notification pass failed; resuming from durable state');
      await pause(restartDelayMs, signal);
    }
  }
}
