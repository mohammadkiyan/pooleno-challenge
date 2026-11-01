import { Admin } from '@platformatic/kafka';
import type { Logger } from 'pino';
import type { KafkaConfig } from '../../app/config.js';

export interface PartitionLag {
  partition: number;
  endOffset: number;
  /** Null until the group first commits on this partition. Its lag then counts from zero. */
  committedOffset: number | null;
  lag: number;
}

export interface GroupLag {
  groupId: string;
  lag: number;
  partitions: PartitionLag[];
}

export type KafkaLag = { available: true; groups: GroupLag[] } | { available: false };

/** Asks for the end of each partition, rather than the time of an event. */
const LATEST = -1n;

/** The API must keep answering with Kafka down, so these are kept short and never retried. */
const REQUEST_TIMEOUT_MS = 2_000;

/**
 * How far each consumer group is behind the end of the topic. Failure is an answer and not an
 * error, because Kafka being down is exactly when the rest of the backlog report matters most.
 */
export async function readKafkaLag(
  kafka: KafkaConfig,
  groupIds: string[],
  log: Logger,
): Promise<KafkaLag> {
  const admin = new Admin({
    clientId: 'pooleno-api',
    bootstrapBrokers: kafka.brokers,
    retries: 0,
    connectTimeout: REQUEST_TIMEOUT_MS,
    requestTimeout: REQUEST_TIMEOUT_MS,
  });

  try {
    return { available: true, groups: await readGroups(admin, kafka.topic, groupIds) };
  } catch (error) {
    log.warn({ err: error }, 'could not read kafka lag');
    return { available: false };
  } finally {
    await admin.close().catch((error: unknown) => {
      log.warn({ err: error }, 'closing the kafka admin client failed');
    });
  }
}

async function readGroups(admin: Admin, topic: string, groupIds: string[]): Promise<GroupLag[]> {
  const metadata = await admin.metadata({ topics: [topic] });
  const partitionIndexes = (metadata.topics.get(topic)?.partitions ?? []).map((_, index) => index);

  const [ends, committed] = await Promise.all([
    admin.listOffsets({
      topics: [
        {
          name: topic,
          partitions: partitionIndexes.map((partitionIndex) => ({
            partitionIndex,
            timestamp: LATEST,
          })),
        },
      ],
    }),
    admin.listConsumerGroupOffsets({
      groups: groupIds.map((groupId) => ({
        groupId,
        topics: [{ name: topic, partitionIndexes }],
      })),
    }),
  ]);

  const endOffsets = new Map(
    ends
      .find(({ name }) => name === topic)
      ?.partitions.map(({ partitionIndex, offset }) => [partitionIndex, offset]),
  );

  return committed.map(({ groupId, topics }) => {
    const committedOffsets = new Map(
      topics
        .find(({ name }) => name === topic)
        ?.partitions.map(({ partitionIndex, committedOffset }) => [
          partitionIndex,
          committedOffset,
        ]),
    );

    const partitions = partitionIndexes.map((partition) => {
      const endOffset = endOffsets.get(partition) ?? 0n;
      const position = committedOffsets.get(partition) ?? -1n;
      const hasCommitted = position >= 0n;
      return {
        partition,
        endOffset: Number(endOffset),
        committedOffset: hasCommitted ? Number(position) : null,
        lag: Number(endOffset - (hasCommitted ? position : 0n)),
      };
    });

    return { groupId, lag: partitions.reduce((total, { lag }) => total + lag, 0), partitions };
  });
}
