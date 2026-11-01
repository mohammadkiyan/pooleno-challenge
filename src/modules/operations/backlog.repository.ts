import type { Pool, PoolClient } from 'pg';
import { withSnapshot } from '../../infrastructure/transaction.js';
import { EXTERNAL_CONSUMER_NAME } from '../external/schedule-notification.js';
import { MATCHING_CONSUMER_NAME } from '../matching/apply-accepted-order.js';

export interface BlockedSymbol {
  symbol: string;
  acceptanceSequence: string;
  attempts: number;
  nextAttemptInMs: number;
}

export interface SymbolLag {
  symbol: string;
  lag: number;
  oldestUnappliedAgeMs: number | null;
}

export interface Backlog {
  outbox: {
    pending: number;
    oldestPendingAgeMs: number | null;
    retrying: number;
    totalAttempts: number;
    /** Symbols whose earliest event has failed to send, which holds back their later events. */
    blockedSymbols: BlockedSymbol[];
  };
  matching: {
    lag: number;
    /** Only the symbols that are behind. */
    symbols: SymbolLag[];
    quarantined: number;
  };
  external: {
    pending: number;
    retrying: number;
    failed: number;
    oldestPendingAgeMs: number | null;
    quarantined: number;
  };
}

/** One consistent view of the durable work still to do. Only counts and ages, never payloads. */
export function readBacklog(pool: Pool): Promise<Backlog> {
  return withSnapshot(pool, async (tx) => ({
    outbox: await readOutbox(tx),
    matching: await readMatching(tx),
    external: await readExternal(tx),
  }));
}

async function readOutbox(tx: PoolClient): Promise<Backlog['outbox']> {
  const { rows } = await tx.query<{
    pending: number;
    oldest_age_ms: string | null;
    retrying: number;
    total_attempts: string;
  }>(
    `SELECT count(*)::int AS pending,
            ${ageInMs('min(created_at)')} AS oldest_age_ms,
            count(*) FILTER (WHERE attempts > 0)::int AS retrying,
            COALESCE(sum(attempts), 0) AS total_attempts
       FROM outbox_events
      WHERE published_at IS NULL`,
  );
  const blocked = await tx.query<{
    symbol: string;
    acceptance_seq: string;
    attempts: number;
    next_attempt_in_ms: string;
  }>(
    `SELECT head.symbol, head.acceptance_seq, head.attempts,
            ceil(GREATEST(0, EXTRACT(EPOCH FROM head.next_attempt_at - now()) * 1000))
              AS next_attempt_in_ms
       FROM symbol_sequences s
      CROSS JOIN LATERAL (
        SELECT e.symbol, e.acceptance_seq, e.attempts, e.next_attempt_at
          FROM outbox_events e
         WHERE e.symbol = s.symbol AND e.published_at IS NULL
         ORDER BY e.acceptance_seq
         LIMIT 1
      ) head
      WHERE head.attempts > 0
      ORDER BY head.symbol`,
  );

  const row = rows[0] as (typeof rows)[number];
  return {
    pending: row.pending,
    oldestPendingAgeMs: toAge(row.oldest_age_ms),
    retrying: row.retrying,
    totalAttempts: Number(row.total_attempts),
    blockedSymbols: blocked.rows.map((head) => ({
      symbol: head.symbol,
      acceptanceSequence: head.acceptance_seq,
      attempts: head.attempts,
      nextAttemptInMs: Number(head.next_attempt_in_ms),
    })),
  };
}

async function readMatching(tx: PoolClient): Promise<Backlog['matching']> {
  const { rows } = await tx.query<{
    symbol: string;
    lag: string;
    oldest_age_ms: string | null;
  }>(
    `SELECT s.symbol,
            s.last_seq - COALESCE(p.last_applied_seq, 0) AS lag,
            ${ageInMs('next_order.created_at')} AS oldest_age_ms
       FROM symbol_sequences s
       LEFT JOIN matching_progress p ON p.symbol = s.symbol
       LEFT JOIN orders next_order
         ON next_order.symbol = s.symbol
        AND next_order.acceptance_seq = COALESCE(p.last_applied_seq, 0) + 1
      WHERE s.last_seq > COALESCE(p.last_applied_seq, 0)
      ORDER BY s.symbol`,
  );

  const symbols = rows.map((row) => ({
    symbol: row.symbol,
    lag: Number(row.lag),
    oldestUnappliedAgeMs: toAge(row.oldest_age_ms),
  }));
  const quarantine = await tx.query<{ quarantined: number }>(
    'SELECT count(*)::int AS quarantined FROM quarantine WHERE consumer = $1',
    [MATCHING_CONSUMER_NAME],
  );

  return {
    lag: symbols.reduce((total, { lag }) => total + lag, 0),
    symbols,
    quarantined: (quarantine.rows[0] as { quarantined: number }).quarantined,
  };
}

async function readExternal(tx: PoolClient): Promise<Backlog['external']> {
  // Separate subqueries, so that each uses its partial index and not every job ever delivered.
  const { rows } = await tx.query<{
    pending: number;
    retrying: number;
    oldest_age_ms: string | null;
    failed: number;
    quarantined: number;
  }>(
    `SELECT due.pending, due.retrying, due.oldest_age_ms,
            (SELECT count(*)::int FROM external_jobs WHERE status = 'FAILED') AS failed,
            (SELECT count(*)::int FROM quarantine WHERE consumer = $1) AS quarantined
       FROM (
         SELECT count(*)::int AS pending,
                count(*) FILTER (WHERE attempts > 0)::int AS retrying,
                ${ageInMs('min(created_at)')} AS oldest_age_ms
           FROM external_jobs
          WHERE status = 'PENDING'
       ) due`,
    [EXTERNAL_CONSUMER_NAME],
  );

  const row = rows[0] as (typeof rows)[number];
  return {
    pending: row.pending,
    retrying: row.retrying,
    failed: row.failed,
    oldestPendingAgeMs: toAge(row.oldest_age_ms),
    quarantined: row.quarantined,
  };
}

function ageInMs(timestamp: string): string {
  return `ceil(EXTRACT(EPOCH FROM now() - ${timestamp}) * 1000)`;
}

/** Never negative: `now()` is when this transaction began, and a row can carry a later time. */
function toAge(value: string | null): number | null {
  return value === null ? null : Math.max(0, Number(value));
}
