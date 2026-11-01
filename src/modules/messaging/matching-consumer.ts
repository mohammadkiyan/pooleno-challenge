import type { Pool } from 'pg';
import type { Logger } from 'pino';
import type { KafkaConfig } from '../../app/config.js';
import { pause } from '../../infrastructure/pause.js';
import { applyAcceptedOrder } from '../matching/apply-accepted-order.js';
import { restoreBook, restoreBooks, type OrderBooks } from '../matching/restore-book.js';
import { runConsumerSession, type ConsumedMessage } from './consumer-session.js';
import { parseOrderAccepted } from './order-accepted.schema.js';

export interface MatchingConsumerOptions {
  pool: Pool;
  kafka: KafkaConfig;
  groupId: string;
  sessionTimeoutMs: number;
  restartDelayMs: number;
  laneCapacity: number;
  signal: AbortSignal;
  log: Logger;
  /** A session starts only after restoring its books, so being assigned implies a restored book. */
  onAssignmentChange: (assigned: boolean) => void;
  /** Told when any partition starts or stops retrying a failing event. */
  onHeldChange: (held: boolean) => void;
}

/**
 * Consumes until `signal` aborts. Each session owns books restored from the database, so a
 * lost partition, rebalance or Kafka failure discards them and starts over from durable state
 * and the last committed offset.
 *
 * An event that fails holds only its partition, which restores the event's book and retries
 * until the event succeeds or an operator repairs the stream. Skipping an order would lose it,
 * and the sequence guard would stop its symbol anyway. Other partitions carry on, because each
 * symbol stays on one partition and so never shares a book with them.
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
  const { pool, kafka, groupId, sessionTimeoutMs, laneCapacity, signal, log } = options;
  const { onAssignmentChange, onHeldChange } = options;

  const books = await restoreBooks(pool);
  log.info({ symbols: [...books.keys()] }, 'books restored');

  const heldPartitions = new Set<number>();
  const setHeld = (partition: number, held: boolean): void => {
    if (held) {
      heldPartitions.add(partition);
    } else {
      heldPartitions.delete(partition);
    }
    onHeldChange(heldPartitions.size > 0);
  };

  try {
    await runConsumerSession({
      kafka,
      clientId: 'pooleno-matching',
      groupId,
      sessionTimeoutMs,
      signal,
      log,
      laneCapacity,
      onAssignmentChange,
      handle: (message, stopped) =>
        handleUntilApplied(options, books, message, stopped, (held) => {
          setHeld(message.partition, held);
        }),
    });
  } finally {
    onHeldChange(false);
  }
}

async function handleUntilApplied(
  { pool, restartDelayMs, log }: MatchingConsumerOptions,
  books: OrderBooks,
  { value, topic, partition, offset }: ConsumedMessage,
  stopped: AbortSignal,
  setHeld: (held: boolean) => void,
): Promise<void> {
  let bookMayBeBehind = false;

  for (;;) {
    try {
      const symbol = await handleMessage(pool, books, value, log, bookMayBeBehind);
      if (bookMayBeBehind) {
        log.info({ partition, symbol }, 'partition released');
        setHeld(false);
      }
      return;
    } catch (error) {
      log.error(
        { err: error, topic, partition, offset: String(offset) },
        'partition held; restoring its book before retrying',
      );
      setHeld(true);
      bookMayBeBehind = true;

      await pause(restartDelayMs, stopped);
      // The session ending is not a failure, and it commits nothing further.
      if (stopped.aborted) {
        return;
      }
    }
  }
}

async function handleMessage(
  pool: Pool,
  books: OrderBooks,
  value: string,
  log: Logger,
  restoreFirst: boolean,
): Promise<string> {
  const event = parseOrderAccepted(value);
  const context = {
    requestId: event.correlationId,
    eventId: event.eventId,
    orderId: event.orderId,
    symbol: event.symbol,
    acceptanceSequence: event.acceptanceSequence,
  };

  if (restoreFirst) {
    await restoreBook(pool, books, event.symbol);
  }

  const match = await applyAcceptedOrder(pool, books, event);
  if (match) {
    log.info({ ...context, trades: match.trades.length }, 'order matched');
  } else {
    log.info(context, 'event already handled');
  }
  return event.symbol;
}
