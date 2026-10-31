import type { Pool } from 'pg';
import { expect, vi } from 'vitest';

export const TEST_DATABASE_URL =
  process.env.TEST_DATABASE_URL ?? 'postgres://pooleno@127.0.0.1:55432/pooleno_test';

export async function resetDatabase(pool: Pool): Promise<void> {
  await pool.query('TRUNCATE outbox_events, idempotency_requests, orders, symbol_sequences');
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
    { timeout: 5_000, interval: 10 },
  );
}
