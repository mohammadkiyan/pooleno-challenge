import {
  Consumer,
  MessagesStreamFallbackModes,
  MessagesStreamModes,
  stringDeserializers,
} from '@platformatic/kafka';
import type { Pool } from 'pg';
import type { Logger } from 'pino';
import type { KafkaConfig } from '../../app/config.js';
import { pause } from '../../infrastructure/pause.js';
import { applyAcceptedOrder } from '../matching/apply-accepted-order.js';
import { restoreBooks, type OrderBooks } from '../matching/restore-book.js';
import { parseOrderAccepted } from './order-accepted.schema.js';

export interface MatchingConsumerOptions {
  pool: Pool;
  kafka: KafkaConfig;
  groupId: string;
  sessionTimeoutMs: number;
  restartDelayMs: number;
  signal: AbortSignal;
  log: Logger;
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
  const { pool, kafka, groupId, sessionTimeoutMs, signal, log } = options;

  const books = await restoreBooks(pool);
  log.info({ symbols: [...books.keys()] }, 'books restored');

  const consumer = new Consumer({
    clientId: 'pooleno-matching',
    groupId,
    bootstrapBrokers: kafka.brokers,
    deserializers: stringDeserializers,
    // With retries, the client rejoins the group and repeats a failed commit by itself.
    // Without them the failure reaches this code, which ends the session instead.
    retries: 0,
    // Joining waits for the coordinator to drop a crashed member, which takes up to the
    // session timeout, so the request must be allowed to outlast it.
    timeout: sessionTimeoutMs * 2,
  });
  // The client reports background failures here, and an unheard 'error' would end the process.
  consumer.on('error', (error) => {
    log.error({ err: error }, 'kafka consumer reported an error');
  });

  let stop = (): void => undefined;
  try {
    const stream = await consumer.consume({
      topics: [kafka.topic],
      autocommit: false,
      mode: MessagesStreamModes.COMMITTED,
      // Without this, a fresh group would start from the end and skip accepted orders.
      fallbackMode: MessagesStreamFallbackModes.EARLIEST,
      sessionTimeout: sessionTimeoutMs,
      rebalanceTimeout: sessionTimeoutMs,
      heartbeatInterval: Math.floor(sessionTimeoutMs / 3),
    });

    let stopped = false;
    // A function, because a stop arrives during an await, where a plain read of the
    // variable looks to TypeScript as if it could not have changed.
    const isStopped = (): boolean => stopped;
    stop = () => {
      stopped = true;
      void stream.close();
    };
    // After this the partitions may belong to another member, so this session must end
    // without committing. The next one starts from durable state and rejoins the group.
    const stopOnMembershipChange = (): void => {
      if (!stopped) {
        log.warn('group membership changed; stopping this session');
        stop();
      }
    };
    consumer.once('consumer:group:rebalance', stopOnMembershipChange);
    consumer.once('consumer:group:join', stopOnMembershipChange);
    signal.addEventListener('abort', stop, { once: true });
    if (signal.aborted) {
      stop();
    }

    for await (const message of stream) {
      // Closing the stream still delivers what it has buffered, which is no longer ours.
      if (isStopped()) {
        break;
      }

      const { topic, partition, offset, leaderEpoch } = message;
      await handleMessage(pool, books, message.value, log).catch((error: unknown) => {
        const position = `${topic}/${partition} offset ${offset}`;
        throw new Error(`Could not handle the message at ${position}`, { cause: error });
      });

      // A durable result is safe to leave uncommitted, because its redelivery is a no-op.
      if (isStopped()) {
        break;
      }
      await consumer.commit({
        offsets: [{ topic, partition, offset: offset + 1n, leaderEpoch }],
      });
    }
  } finally {
    signal.removeEventListener('abort', stop);
    await consumer.close(true).catch((error: unknown) => {
      log.warn({ err: error }, 'closing the kafka consumer failed');
    });
  }
}

async function handleMessage(
  pool: Pool,
  books: OrderBooks,
  value: string,
  log: Logger,
): Promise<void> {
  const event = parseOrderAccepted(value);
  const context = {
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
