import type { Pool, PoolClient } from 'pg';
import type { OrderAcceptedV1 } from './order-accepted.schema.js';

export interface OutboxHead {
  eventId: string;
  symbol: string;
  acceptanceSeq: string;
  /** The stored payload, unchanged, so a resend is identical to the first send. */
  payload: string;
  attempts: number;
}

const MAX_STORED_ERROR_LENGTH = 500;

export async function insertOutboxEvent(tx: PoolClient, event: OrderAcceptedV1): Promise<void> {
  await tx.query(
    `INSERT INTO outbox_events
       (event_id, event_type, schema_version, order_id, symbol, acceptance_seq, payload)
     VALUES ($1, $2, $3, $4, $5, $6, $7)`,
    [
      event.eventId,
      event.eventType,
      event.schemaVersion,
      event.orderId,
      event.symbol,
      event.acceptanceSequence,
      JSON.stringify(event),
    ],
  );
}

/**
 * Finds each symbol's earliest unpublished event, if it is due. The due check sits
 * outside the subquery on purpose: a head that is waiting to retry must hide the
 * symbol's later events, not make way for them.
 */
export async function findDueHeads(pool: Pool): Promise<OutboxHead[]> {
  const { rows } = await pool.query<{
    event_id: string;
    symbol: string;
    acceptance_seq: string;
    payload: string;
    attempts: number;
  }>(
    `SELECT head.event_id, head.symbol, head.acceptance_seq, head.payload::text AS payload,
            head.attempts
       FROM symbol_sequences s
      CROSS JOIN LATERAL (
        SELECT e.*
          FROM outbox_events e
         WHERE e.symbol = s.symbol AND e.published_at IS NULL
         ORDER BY e.acceptance_seq
         LIMIT 1
      ) head
      WHERE head.next_attempt_at <= now()`,
  );

  return rows.map((row) => ({
    eventId: row.event_id,
    symbol: row.symbol,
    acceptanceSeq: row.acceptance_seq,
    payload: row.payload,
    attempts: row.attempts,
  }));
}

export async function markPublished(pool: Pool, eventId: string): Promise<void> {
  await pool.query(
    'UPDATE outbox_events SET published_at = now(), last_error = NULL WHERE event_id = $1',
    [eventId],
  );
}

export async function recordSendFailure(
  pool: Pool,
  eventId: string,
  { error, retryAfterMs }: { error: string; retryAfterMs: number },
): Promise<void> {
  await pool.query(
    `UPDATE outbox_events
        SET attempts = attempts + 1,
            last_error = $2,
            next_attempt_at = now() + $3 * interval '1 millisecond'
      WHERE event_id = $1`,
    [eventId, error.slice(0, MAX_STORED_ERROR_LENGTH), retryAfterMs],
  );
}
