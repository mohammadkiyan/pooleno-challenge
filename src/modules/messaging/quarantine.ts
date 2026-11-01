import type { Pool } from 'pg';
import type { Logger } from 'pino';
import { toStoredError } from '../../infrastructure/stored-error.js';
import type { ConsumedMessage } from './consumer-session.js';
import { parseOrderAccepted, type OrderAcceptedV1 } from './order-accepted.schema.js';

/**
 * Reads a message as an event, or records it as a dead letter and resolves to undefined so
 * that its offset can advance. A failed write rejects, so the offset holds until it is recorded.
 */
export async function parseOrQuarantine(
  pool: Pool,
  consumer: string,
  message: ConsumedMessage,
  log: Logger,
): Promise<OrderAcceptedV1 | undefined> {
  try {
    return parseOrderAccepted(message.value);
  } catch (error) {
    await quarantineMessage(pool, consumer, message, String(error));
    log.error(
      { topic: message.topic, partition: message.partition, offset: String(message.offset) },
      'malformed message quarantined',
    );
    return undefined;
  }
}

export async function quarantineMessage(
  pool: Pool,
  consumer: string,
  message: Pick<ConsumedMessage, 'topic' | 'partition' | 'offset' | 'value'>,
  error: string,
): Promise<void> {
  await pool.query(
    `INSERT INTO quarantine (consumer, topic, kafka_partition, kafka_offset, raw_value, error)
     VALUES ($1, $2, $3, $4, $5, $6)
     ON CONFLICT (consumer, topic, kafka_partition, kafka_offset, md5(raw_value)) DO NOTHING`,
    [
      consumer,
      message.topic,
      message.partition,
      message.offset.toString(),
      Buffer.from(message.value),
      toStoredError(error),
    ],
  );
}
