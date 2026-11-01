import { randomUUID } from 'node:crypto';
import {
  Admin,
  compatibilityPartitioner,
  Consumer,
  MessagesStreamFallbackModes,
  MessagesStreamModes,
  ProduceAcks,
  Producer,
  stringDeserializers,
  stringSerializers,
} from '@platformatic/kafka';
import { pino } from 'pino';
import type { KafkaConfig } from '../../src/app/config.js';
import { ensureTopic } from '../../src/modules/messaging/kafka-topics.js';

const BROKERS = (process.env.TEST_KAFKA_BROKERS ?? '127.0.0.1:59092').split(',');

/** A topic and consumer group that belong to one test. */
export interface TestStream {
  kafka: KafkaConfig;
  groupId: string;
}

export interface TopicMessage {
  key: string;
  value: string;
  partition: number;
}

export async function assertKafkaIsReachable(): Promise<void> {
  try {
    await withAdmin((admin) => admin.metadata({ topics: [] }));
  } catch (error) {
    throw new Error(
      `Cannot reach Kafka at ${BROKERS.join(',')}. Start it with: docker compose up -d --wait kafka`,
      { cause: error },
    );
  }
}

export async function createTestStream(): Promise<TestStream> {
  const id = randomUUID();
  const kafka = { brokers: BROKERS, topic: `test.orders.${id}` };
  await ensureTopic(kafka, pino({ level: 'silent' }));
  return { kafka, groupId: `test-matching-${id}` };
}

export async function deleteTestStream({ kafka }: TestStream): Promise<void> {
  await withAdmin((admin) => admin.deleteTopics({ topics: [kafka.topic] }));
}

function createReader({ brokers }: KafkaConfig) {
  return new Consumer({
    clientId: 'pooleno-test-reader',
    groupId: `test-reader-${randomUUID()}`,
    bootstrapBrokers: brokers,
    deserializers: stringDeserializers,
  });
}

/** Puts messages on the topic as they are, in order. A missing value is a Kafka tombstone. */
export async function produceRaw(
  { kafka }: TestStream,
  messages: { key: string; value?: string }[],
): Promise<void> {
  const producer = new Producer({
    clientId: 'pooleno-test-producer',
    bootstrapBrokers: kafka.brokers,
    serializers: stringSerializers,
    // A new topic has no leader for a moment, and a plain retry then writes a batch twice.
    idempotent: true,
    retries: 3,
    acks: ProduceAcks.ALL,
    partitioner: compatibilityPartitioner,
  });

  try {
    await producer.send({
      messages: messages.map((message) => ({ topic: kafka.topic, ...message })),
    });
  } finally {
    await producer.close();
  }
}

/** Reads everything that is in the topic right now, with no effect on any group's progress. */
export async function readTopic({ kafka }: TestStream): Promise<TopicMessage[]> {
  const consumer = createReader(kafka);

  try {
    const ends = (await consumer.listOffsets({ topics: [kafka.topic] })).get(kafka.topic) ?? [];
    const total = ends.reduce((sum, end) => sum + Number(end), 0);
    const messages: TopicMessage[] = [];
    if (total === 0) {
      return messages;
    }

    const stream = await consumer.consume({
      topics: [kafka.topic],
      autocommit: false,
      mode: MessagesStreamModes.EARLIEST,
    });
    for await (const { key, value, partition } of stream) {
      messages.push({ key, value, partition });
      if (messages.length === total) {
        break;
      }
    }
    return messages;
  } finally {
    await consumer.close(true);
  }
}

/** How many messages the group has committed as finished, across all partitions. */
export async function committedMessages({ groupId }: TestStream): Promise<number> {
  const [group] = await withAdmin((admin) =>
    admin.listConsumerGroupOffsets({ groups: [groupId] }),
  );

  let total = 0;
  for (const topic of group?.topics ?? []) {
    for (const { committedOffset } of topic.partitions) {
      total += Math.max(0, Number(committedOffset));
    }
  }
  return total;
}

/** The repair an operator makes to pass a poison message: the group must have no members. */
export async function moveCommittedOffset(
  { kafka, groupId }: TestStream,
  partition: number,
  offset: bigint,
): Promise<void> {
  await withAdmin((admin) =>
    admin.alterConsumerGroupOffsets({
      groupId,
      topics: [{ name: kafka.topic, partitionOffsets: [{ partition, offset }] }],
    }),
  );
}

/** The repair an operator makes before republishing: the group must have no members. */
export async function moveCommittedOffsetsToEnd({ kafka, groupId }: TestStream): Promise<void> {
  const reader = createReader(kafka);
  try {
    const ends = (await reader.listOffsets({ topics: [kafka.topic] })).get(kafka.topic) ?? [];
    await withAdmin((admin) =>
      admin.alterConsumerGroupOffsets({
        groupId,
        topics: [
          {
            name: kafka.topic,
            partitionOffsets: ends.map((offset, partition) => ({ partition, offset })),
          },
        ],
      }),
    );
  } finally {
    await reader.close(true);
  }
}

/** Joins the group, which forces the existing member through a rebalance. */
export function joinAsRival({ kafka, groupId }: TestStream, sessionTimeoutMs: number) {
  const rival = new Consumer({
    clientId: 'pooleno-test-rival',
    groupId,
    bootstrapBrokers: kafka.brokers,
    deserializers: stringDeserializers,
    timeout: sessionTimeoutMs * 2,
  });
  rival.on('error', () => undefined);

  const joined = rival.consume({
    topics: [kafka.topic],
    autocommit: false,
    mode: MessagesStreamModes.COMMITTED,
    fallbackMode: MessagesStreamFallbackModes.EARLIEST,
    sessionTimeout: sessionTimeoutMs,
    rebalanceTimeout: sessionTimeoutMs,
    heartbeatInterval: Math.floor(sessionTimeoutMs / 3),
  });

  return {
    joined,
    leave: () => rival.close(true),
  };
}

async function withAdmin<T>(work: (admin: Admin) => Promise<T>): Promise<T> {
  const admin = new Admin({ clientId: 'pooleno-test-admin', bootstrapBrokers: BROKERS });
  try {
    return await work(admin);
  } finally {
    await admin.close();
  }
}
