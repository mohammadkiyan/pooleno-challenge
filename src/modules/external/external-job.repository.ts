import type { Pool, PoolClient } from 'pg';

export interface DueJob {
  eventId: string;
  targetKey: string;
  /** The original event, unchanged, so that every attempt sends the same body. */
  payload: string;
  attempts: number;
  /** Copied from the payload for the logs. Null only for a payload that is not an event. */
  logContext: {
    requestId: string | null;
    orderId: string | null;
    symbol: string | null;
    acceptanceSequence: string | null;
  };
}

const MAX_STORED_ERROR_LENGTH = 500;

/** Text from outside this system can contain NUL, which PostgreSQL refuses in a text column. */
function toStoredError(error: string): string {
  return error.replaceAll('\0', '').slice(0, MAX_STORED_ERROR_LENGTH);
}

export async function insertExternalJob(
  tx: PoolClient,
  job: { eventId: string; targetKey: string; payload: string },
): Promise<void> {
  await tx.query('INSERT INTO external_jobs (event_id, target_key, payload) VALUES ($1, $2, $3)', [
    job.eventId,
    job.targetKey,
    job.payload,
  ]);
}

/**
 * The jobs due now, longest overdue first. A job whose attempt was cut short by a crash is
 * still pending and due, so the next owner reclaims it here with no separate step.
 */
export async function findDueJobs(pool: Pool, limit: number): Promise<DueJob[]> {
  const { rows } = await pool.query<{
    event_id: string;
    target_key: string;
    payload: string;
    attempts: number;
    request_id: string | null;
    order_id: string | null;
    symbol: string | null;
    acceptance_sequence: string | null;
  }>(
    `SELECT event_id, target_key, payload::text AS payload, attempts,
            payload->>'correlationId' AS request_id,
            payload->>'orderId' AS order_id,
            payload->>'symbol' AS symbol,
            payload->>'acceptanceSequence' AS acceptance_sequence
       FROM external_jobs
      WHERE status = 'PENDING' AND next_attempt_at <= now()
      ORDER BY next_attempt_at, event_id
      LIMIT $1`,
    [limit],
  );

  return rows.map((row) => ({
    eventId: row.event_id,
    targetKey: row.target_key,
    payload: row.payload,
    attempts: row.attempts,
    logContext: {
      requestId: row.request_id,
      orderId: row.order_id,
      symbol: row.symbol,
      acceptanceSequence: row.acceptance_sequence,
    },
  }));
}

/*
 * Each result below applies only if the job is still as this attempt found it, and resolves
 * to whether it did. A worker that froze and lost ownership therefore cannot overwrite what
 * its successor has since recorded. Its call was made under the same key, so it is harmless.
 */

export async function markJobSucceeded(pool: Pool, job: DueJob): Promise<boolean> {
  const { rowCount } = await pool.query(
    `UPDATE external_jobs
        SET status = 'SUCCEEDED', attempts = attempts + 1, last_error = NULL,
            next_attempt_at = NULL, completed_at = now()
      WHERE event_id = $1 AND status = 'PENDING' AND attempts = $2`,
    [job.eventId, job.attempts],
  );
  return rowCount === 1;
}

export async function scheduleJobRetry(
  pool: Pool,
  job: DueJob,
  { error, retryDelayMs }: { error: string; retryDelayMs: number },
): Promise<boolean> {
  const { rowCount } = await pool.query(
    `UPDATE external_jobs
        SET attempts = attempts + 1, last_error = $3,
            next_attempt_at = now() + $4 * interval '1 millisecond'
      WHERE event_id = $1 AND status = 'PENDING' AND attempts = $2`,
    [job.eventId, job.attempts, toStoredError(error), retryDelayMs],
  );
  return rowCount === 1;
}

export async function markJobFailed(pool: Pool, job: DueJob, error: string): Promise<boolean> {
  const { rowCount } = await pool.query(
    `UPDATE external_jobs
        SET status = 'FAILED', attempts = attempts + 1, last_error = $3,
            next_attempt_at = NULL, completed_at = now()
      WHERE event_id = $1 AND status = 'PENDING' AND attempts = $2`,
    [job.eventId, job.attempts, toStoredError(error)],
  );
  return rowCount === 1;
}

/**
 * Returns a failed job to the worker with a fresh retry budget. Its event, key and payload do
 * not change, so the destination still recognises it. Resolves to false unless the job failed.
 */
export async function redriveJob(pool: Pool, eventId: string): Promise<boolean> {
  const { rowCount } = await pool.query(
    `UPDATE external_jobs
        SET status = 'PENDING', attempts = 0, next_attempt_at = now(), completed_at = NULL
      WHERE event_id = $1 AND status = 'FAILED'`,
    [eventId],
  );
  return rowCount === 1;
}

export async function quarantineMessage(
  pool: Pool,
  message: { topic: string; partition: number; offset: bigint; value: string },
  error: string,
): Promise<void> {
  await pool.query(
    `INSERT INTO external_quarantine (topic, kafka_partition, kafka_offset, raw_value, error)
     VALUES ($1, $2, $3, $4, $5)`,
    [
      message.topic,
      message.partition,
      message.offset.toString(),
      Buffer.from(message.value),
      toStoredError(error),
    ],
  );
}
