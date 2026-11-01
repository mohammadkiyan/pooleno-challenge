import type { Pool } from 'pg';

export interface StoredJob {
  target_key: string;
  payload: string;
  status: string;
  attempts: number;
  next_attempt_at: Date | null;
  last_error: string | null;
  created_at: Date;
  completed_at: Date | null;
  /** How far ahead the next attempt is, by the database's clock. */
  due_in_ms: number | null;
}

export async function readJob(pool: Pool, eventId: string): Promise<StoredJob | undefined> {
  const { rows } = await pool.query<StoredJob>(
    `SELECT target_key, payload::text AS payload, status, attempts, next_attempt_at, last_error,
            created_at, completed_at,
            (extract(epoch FROM next_attempt_at - now()) * 1000)::float8 AS due_in_ms
       FROM external_jobs WHERE event_id = $1`,
    [eventId],
  );
  return rows[0];
}

export async function countJobs(pool: Pool): Promise<number | undefined> {
  const { rows } = await pool.query<{ count: number }>(
    'SELECT count(*)::int AS count FROM external_jobs',
  );
  return rows[0]?.count;
}

export async function readQuarantine(pool: Pool) {
  const { rows } = await pool.query<{
    consumer: string;
    topic: string;
    kafka_partition: number;
    kafka_offset: string;
    raw_value: Buffer;
    error: string;
  }>(
    `SELECT consumer, topic, kafka_partition, kafka_offset, raw_value, error
       FROM quarantine ORDER BY kafka_offset, id`,
  );
  return rows;
}
