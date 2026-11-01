import {
  Consumer,
  MessagesStreamFallbackModes,
  MessagesStreamModes,
  stringDeserializers,
} from '@platformatic/kafka';
import type { Logger } from 'pino';
import type { KafkaConfig } from '../../app/config.js';
import { PartitionLanes } from './partition-lanes.js';

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
  /**
   * Resolves once the message is durably handled, which is what lets its offset advance.
   * Partitions run concurrently, so this must be safe across them. `stopped` aborts when
   * the session ends, and work waiting to retry must then return. Nothing is committed after it.
   */
  handle: (message: ConsumedMessage, stopped: AbortSignal) => Promise<void>;
  /** Messages buffered per partition. The default of 1 handles one message at a time overall. */
  laneCapacity?: number;
  /** Told when this session gains partitions, and when it can no longer process them. */
  onAssignmentChange?: (assigned: boolean) => void;
}

/**
 * One membership of a consumer group. Each partition handles its messages one at a time and
 * commits each offset after its message is durably handled. It ends when `signal` aborts or the
 * group's membership changes, and rejects if handling or committing fails. Either way it waits
 * for work in progress and no offset is committed afterwards, so the caller can start another
 * session from the committed position.
 */
export async function runConsumerSession(options: ConsumerSessionOptions): Promise<void> {
  const {
    kafka,
    clientId,
    groupId,
    sessionTimeoutMs,
    signal,
    log,
    handle,
    laneCapacity = 1,
    onAssignmentChange,
  } = options;

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

  const stopped = new AbortController();
  let closeStream = (): void => undefined;
  let failure: { error: unknown } | undefined;

  const stop = (): void => {
    if (stopped.signal.aborted) {
      return;
    }
    stopped.abort();
    lanes.stop();
    onAssignmentChange?.(false);
    closeStream();
  };

  const lanes = new PartitionLanes<ConsumedMessage & { leaderEpoch: number }>({
    capacity: laneCapacity,
    work: async ({ leaderEpoch, ...message }) => {
      const { topic, partition, offset } = message;
      await handle(message, stopped.signal).catch((error: unknown) => {
        const position = `${topic}/${partition} offset ${offset}`;
        throw new Error(`Could not handle the message at ${position}`, { cause: error });
      });

      // A durable result is safe to leave uncommitted, because its redelivery is a no-op.
      if (!stopped.signal.aborted) {
        await consumer.commit({
          offsets: [{ topic, partition, offset: offset + 1n, leaderEpoch }],
        });
      }
    },
    onFailure: (error) => {
      failure ??= { error };
      stop();
    },
  });

  try {
    // Closing the consumer does not interrupt a join in flight, so a shutdown that arrives
    // while this waits out a dead member's session is bounded by the shutdown deadline.
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

    closeStream = () => void stream.close();
    // After this the partitions may belong to another member, so this session must end
    // without committing. The next one starts from durable state and rejoins the group.
    const stopOnMembershipChange = (): void => {
      if (!stopped.signal.aborted) {
        log.warn('group membership changed; stopping this session');
        stop();
      }
    };
    consumer.once('consumer:group:rebalance', stopOnMembershipChange);
    consumer.once('consumer:group:join', stopOnMembershipChange);
    signal.addEventListener('abort', stop, { once: true });
    if (signal.aborted) {
      stop();
    } else {
      onAssignmentChange?.(hasPartitions(consumer));
    }

    for await (const message of stream) {
      // Closing the stream still delivers what it has buffered, which is no longer ours.
      if (stopped.signal.aborted) {
        break;
      }

      const { topic, partition, offset, leaderEpoch } = message;
      // A message with no value arrives as undefined, despite the client's types.
      const value = (message.value as string | undefined) ?? '';
      await lanes.push(partition, { topic, partition, offset, value, leaderEpoch });
    }
  } finally {
    // The stream ending must not leave a lane writing while a successor starts.
    stop();
    await lanes.settled();
    signal.removeEventListener('abort', stop);
    await consumer.close(true).catch((error: unknown) => {
      log.warn({ err: error }, 'closing the kafka consumer failed');
    });
  }

  if (failure) {
    throw failure.error;
  }
}

function hasPartitions(consumer: Consumer<string, string, string, string>): boolean {
  return consumer.assignments?.some(({ partitions }) => partitions.length > 0) ?? false;
}
