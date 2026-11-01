import {
  compatibilityPartitioner,
  ProduceAcks,
  Producer,
  stringSerializers,
} from '@platformatic/kafka';
import type { KafkaConfig } from '../../app/config.js';

export interface KafkaMessage {
  key: string;
  value: string;
}

export interface EventProducer {
  /** Resolves only once every in-sync replica has acknowledged the message. */
  send(message: KafkaMessage): Promise<void>;
  /** Rejects unless a broker answers now. A cached answer would prove nothing. */
  assertReachable(): Promise<void>;
  close(): Promise<void>;
}

export function createEventProducer(kafka: KafkaConfig): EventProducer {
  const producer = new Producer({
    clientId: 'pooleno-publisher',
    bootstrapBrokers: kafka.brokers,
    serializers: stringSerializers,
    idempotent: true,
    acks: ProduceAcks.ALL,
    // Pinned, so a changed client default cannot move a symbol to another partition.
    partitioner: compatibilityPartitioner,
    autocreateTopics: false,
    // An idempotent producer left without a retry limit (or with 0 or 1) retries
    // forever. It must give up, so that a failed attempt can be recorded durably.
    retries: 3,
    retryDelay: 200,
  });

  return {
    send: async ({ key, value }) => {
      await producer.send({ messages: [{ topic: kafka.topic, key, value }] });
    },
    assertReachable: async () => {
      await producer.metadata({ topics: [kafka.topic], forceUpdate: true, retries: 0 });
    },
    close: () => producer.close(),
  };
}
