import {
  Consumer,
  MessagesStreamFallbackModes,
  MessagesStreamModes,
  stringDeserializers,
} from '@platformatic/kafka';
import type { Logger } from 'pino';
import type { KafkaConfig } from '../../app/config.js';

export interface ConsumedMessage {
  topic: string;
  partition: number;
  offset: bigint;
  value: string;
}

export interface ConsumerSessionOptions {
  kafka: KafkaConfig;
  clientId: string;
  groupId: string;
  sessionTimeoutMs: number;
  signal: AbortSignal;
  log: Logger;
  /** Resolves once the message is durably handled, which is what lets its offset advance. */
  handle: (message: ConsumedMessage) => Promise<void>;
}

/**
 * One membership of a consumer group, handling messages one at a time and committing each
 * offset after its message is durably handled. It ends when `signal` aborts or the group's
 * membership changes, and rejects if handling or committing fails. Either way no offset is
 * committed afterwards, so the caller can start another session from the committed position.
 */
export async function runConsumerSession(options: ConsumerSessionOptions): Promise<void> {
  const { kafka, clientId, groupId, sessionTimeoutMs, signal, log, handle } = options;

  const consumer = new Consumer({
    clientId,
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
      // A message with no value arrives as undefined, despite the client's types.
      const value = (message.value as string | undefined) ?? '';
      await handle({ topic, partition, offset, value }).catch((error: unknown) => {
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
