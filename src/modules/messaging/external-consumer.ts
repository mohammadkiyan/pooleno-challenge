import type { Pool } from 'pg';
import type { Logger } from 'pino';
import type { KafkaConfig } from '../../app/config.js';
import { pause } from '../../infrastructure/pause.js';
import { quarantineMessage } from '../external/external-job.repository.js';
import { scheduleNotification } from '../external/schedule-notification.js';
import { runConsumerSession, type ConsumedMessage } from './consumer-session.js';
import { parseOrderAccepted, type OrderAcceptedV1 } from './order-accepted.schema.js';

export interface ExternalConsumerOptions {
  pool: Pool;
  kafka: KafkaConfig;
  groupId: string;
  sessionTimeoutMs: number;
  restartDelayMs: number;
  signal: AbortSignal;
  log: Logger;
}

/**
 * Turns each accepted order into a durable job, until `signal` aborts. Nothing here calls
 * the external API, so its outage cannot hold back this group. Any failure restarts from
 * the last committed offset, and the Inbox makes the redelivery a no-op.
 */
export async function runExternalConsumer(options: ExternalConsumerOptions): Promise<void> {
  const { pool, kafka, groupId, sessionTimeoutMs, restartDelayMs, signal, log } = options;

  while (!signal.aborted) {
    try {
      await runConsumerSession({
        kafka,
        clientId: 'pooleno-external',
        groupId,
        sessionTimeoutMs,
        signal,
        log,
        handle: (message) => handleMessage(pool, message, log),
      });
    } catch (error) {
      log.error({ err: error }, 'external consumer stopped; resuming from the committed offset');
      await pause(restartDelayMs, signal);
    }
  }
}

async function handleMessage(pool: Pool, message: ConsumedMessage, log: Logger): Promise<void> {
  let event: OrderAcceptedV1;
  try {
    event = parseOrderAccepted(message.value);
  } catch (error) {
    await quarantineMessage(pool, message, String(error));
    log.error(
      { topic: message.topic, partition: message.partition, offset: String(message.offset) },
      'malformed message quarantined',
    );
    return;
  }

  const { eventId, orderId } = event;
  const scheduled = await scheduleNotification(pool, { eventId, payload: message.value });
  log.info({ eventId, orderId }, scheduled ? 'notification scheduled' : 'event already scheduled');
}
