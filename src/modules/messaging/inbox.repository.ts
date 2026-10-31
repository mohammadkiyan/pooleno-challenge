import type { PoolClient } from 'pg';

/**
 * Records that this consumer is handling an event. Resolves to false if it already
 * has. It commits or rolls back with the consumer's local result.
 */
export async function claimInboxEvent(
  tx: PoolClient,
  consumerName: string,
  eventId: string,
): Promise<boolean> {
  const { rowCount } = await tx.query(
    `INSERT INTO consumer_inbox (consumer_name, event_id)
     VALUES ($1, $2)
     ON CONFLICT DO NOTHING`,
    [consumerName, eventId],
  );
  return rowCount === 1;
}
