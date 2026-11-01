import type { Pool } from 'pg';
import type { Logger } from 'pino';
import type { KafkaConfig } from '../../app/config.js';
import { pause } from '../../infrastructure/pause.js';
import { EXTERNAL_CONSUMER_NAME, scheduleNotification } from '../external/schedule-notification.js';
import { runConsumerSession, type ConsumedMessage } from './consumer-session.js';
import { parseOrQuarantine } from './quarantine.js';

export interface ExternalConsumerOptions {
  pool: Pool;
  kafka: KafkaConfig;
  groupId: string;
  sessionTimeoutMs: number;
  restartDelayMs: number;
  signal: AbortSignal;
  log: Logger;
  onAssignmentChange: (assigned: boolean) => void;
}

/**
 * Turns each accepted order into a durable job, until `signal` aborts. Nothing here calls
 * the external API, so its outage cannot hold back this group. Any failure restarts from
 * the last committed offset, and the Inbox makes the redelivery a no-op.
 */
export async function runExternalConsumer(options: ExternalConsumerOptions): Promise<void> {
  const { pool, kafka, groupId, sessionTimeoutMs, restartDelayMs, signal, log, onAssignmentChange } =
    options;

  while (!signal.aborted) {
    try {
      await runConsumerSession({
        kafka,
        clientId: 'pooleno-external',
        groupId,
        sessionTimeoutMs,
        signal,
        log,
        onAssignmentChange,
        handle: (message) => handleMessage(pool, message, log),
      });
    } catch (error) {
      log.error({ err: error }, 'external consumer stopped; resuming from the committed offset');
      await pause(restartDelayMs, signal);
    }
  }
}

async function handleMessage(pool: Pool, message: ConsumedMessage, log: Logger): Promise<void> {
  const event = await parseOrQuarantine(pool, EXTERNAL_CONSUMER_NAME, message, log);
  if (!event) {
    return;
  }

  const { eventId, orderId, symbol, acceptanceSequence, correlationId: requestId } = event;
  const scheduled = await scheduleNotification(pool, { eventId, payload: message.value });
  log.info(
    { requestId, eventId, orderId, symbol, acceptanceSequence },
    scheduled ? 'notification scheduled' : 'event already scheduled',
  );
}
