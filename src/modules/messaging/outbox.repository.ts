import type { PoolClient } from 'pg';
import type { OrderAcceptedV1 } from './order-accepted.schema.js';

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
