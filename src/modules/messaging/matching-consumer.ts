import type { Pool } from 'pg';
import type { Logger } from 'pino';
import type { KafkaConfig } from '../../app/config.js';
import { pause } from '../../infrastructure/pause.js';
import { applyAcceptedOrder } from '../matching/apply-accepted-order.js';
import { restoreBooks, type OrderBooks } from '../matching/restore-book.js';
import { runConsumerSession } from './consumer-session.js';
import { parseOrderAccepted } from './order-accepted.schema.js';

export interface MatchingConsumerOptions {
  pool: Pool;
  kafka: KafkaConfig;
  groupId: string;
  sessionTimeoutMs: number;
  restartDelayMs: number;
  signal: AbortSignal;
  log: Logger;
  /** A session starts only after restoring its books, so being assigned implies a restored book. */
  onAssignmentChange: (assigned: boolean) => void;
}

/**
 * Consumes until `signal` aborts. Each session owns a book restored from the database,
 * so any failure, lost partition or rebalance discards that book and starts over from
 * durable state and the last committed offset. An event that keeps failing blocks
 * its stream here until an operator repairs it, which is the required behaviour.
 */
export async function runMatchingConsumer(options: MatchingConsumerOptions): Promise<void> {
  const { restartDelayMs, signal, log } = options;

  while (!signal.aborted) {
    try {
      await runSession(options);
    } catch (error) {
      log.error({ err: error }, 'matching stopped; restoring from the database before retrying');
      await pause(restartDelayMs, signal);
    }
  }
}

async function runSession(options: MatchingConsumerOptions): Promise<void> {
  const { pool, kafka, groupId, sessionTimeoutMs, signal, log, onAssignmentChange } = options;

  const books = await restoreBooks(pool);
  log.info({ symbols: [...books.keys()] }, 'books restored');

  await runConsumerSession({
    kafka,
    clientId: 'pooleno-matching',
    groupId,
    sessionTimeoutMs,
    signal,
    log,
    onAssignmentChange,
    handle: ({ value }) => handleMessage(pool, books, value, log),
  });
}

async function handleMessage(
  pool: Pool,
  books: OrderBooks,
  value: string,
  log: Logger,
): Promise<void> {
  const event = parseOrderAccepted(value);
  const context = {
    requestId: event.correlationId,
    eventId: event.eventId,
    orderId: event.orderId,
    symbol: event.symbol,
    acceptanceSequence: event.acceptanceSequence,
  };

  const match = await applyAcceptedOrder(pool, books, event);
  if (match) {
    log.info({ ...context, trades: match.trades.length }, 'order matched');
  } else {
    log.info(context, 'event already handled');
  }
}
