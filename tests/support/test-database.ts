import type { Pool } from 'pg';
import { expect, vi } from 'vitest';

export const TEST_DATABASE_URL =
  process.env.TEST_DATABASE_URL ?? 'postgres://pooleno@127.0.0.1:55432/pooleno_test';

export async function resetDatabase(pool: Pool): Promise<void> {
  await pool.query(
    `TRUNCATE outbox_events, idempotency_requests, orders, symbol_sequences,
              consumer_inbox, matching_progress, trades`,
  );
}

export async function countRows(pool: Pool) {
  const { rows } = await pool.query<{ orders: number; receipts: number; events: number }>(
    `SELECT (SELECT count(*)::int FROM orders)               AS orders,
            (SELECT count(*)::int FROM idempotency_requests) AS receipts,
            (SELECT count(*)::int FROM outbox_events)        AS events`,
  );
  return rows[0];
}

export async function readCounter(pool: Pool, symbol: string): Promise<string | undefined> {
  const { rows } = await pool.query<{ last_seq: string }>(
    'SELECT last_seq FROM symbol_sequences WHERE symbol = $1',
    [symbol],
  );
  return rows[0]?.last_seq;
}

export async function readOrder(pool: Pool, orderId: string) {
  const { rows } = await pool.query<{ status: string; remaining_units: string }>(
    'SELECT status, remaining_units FROM orders WHERE id = $1',
    [orderId],
  );
  return rows[0];
}

export async function readTrades(pool: Pool) {
  const { rows } = await pool.query<{
    event_id: string;
    execution_index: number;
    maker_id: string;
    taker_id: string;
    price_units: string;
    quantity_units: string;
  }>(
    `SELECT event_id, execution_index, maker_id, taker_id, price_units, quantity_units
       FROM trades ORDER BY event_id, execution_index`,
  );
  return rows;
}

export async function readProgress(pool: Pool, symbol: string): Promise<string | undefined> {
  const { rows } = await pool.query<{ last_applied_seq: string }>(
    'SELECT last_applied_seq FROM matching_progress WHERE symbol = $1',
    [symbol],
  );
  return rows[0]?.last_applied_seq;
}

export async function countInbox(pool: Pool): Promise<number | undefined> {
  const { rows } = await pool.query<{ count: number }>(
    'SELECT count(*)::int AS count FROM consumer_inbox',
  );
  return rows[0]?.count;
}

/** Resolves once a backend is blocked on a lock, so a test need not guess with a delay. */
export async function waitForBlockedBackend(pool: Pool): Promise<void> {
  await vi.waitFor(
    async () => {
      const { rows } = await pool.query<{ blocked: number }>(
        `SELECT count(*)::int AS blocked FROM pg_stat_activity
          WHERE datname = current_database() AND wait_event_type = 'Lock'`,
      );
      expect(rows[0]?.blocked).toBeGreaterThan(0);
    },
    { timeout: 30_000, interval: 10 },
  );
}

/** Ends backends blocked on a lock, which would otherwise outlive a crashed client. */
export async function terminateBlockedBackends(pool: Pool): Promise<void> {
  await pool.query(
    `SELECT pg_terminate_backend(pid) FROM pg_stat_activity
      WHERE datname = current_database() AND wait_event_type = 'Lock'`,
  );
}

/** Ends the connection holding each advisory lock in this database, which frees the lock. */
export async function terminateAdvisoryLockHolders(pool: Pool): Promise<number> {
  const { rowCount } = await pool.query(
    `SELECT pg_terminate_backend(pid) FROM pg_locks
      WHERE locktype = 'advisory' AND granted
        AND database = (SELECT oid FROM pg_database WHERE datname = current_database())`,
  );
  return rowCount ?? 0;
}
