import { Admin, findErrorBy } from '@platformatic/kafka';
import type { Logger } from 'pino';
import type { KafkaConfig } from '../../app/config.js';

// Fixed for this version: changing it would move symbols between partitions and
// break their ordering across the change.
const PARTITIONS = 3;
// The local stack has one broker. A real deployment needs more, and a higher factor.
const REPLICAS = 1;
// Must cover the recovery horizon: the longest the matcher may be down without
// needing an operator to republish from the Outbox.
const RETENTION_MS = 30 * 24 * 60 * 60 * 1_000;

/** Creates the topic if it is missing, and refuses to continue if it has the wrong shape. */
export async function ensureTopic(kafka: KafkaConfig, log: Logger): Promise<void> {
  const { topic } = kafka;
  const admin = new Admin({ clientId: 'pooleno-kafka-init', bootstrapBrokers: kafka.brokers });

  try {
    await admin
      .createTopics({
        topics: [topic],
        partitions: PARTITIONS,
        replicas: REPLICAS,
        configs: [{ name: 'retention.ms', value: String(RETENTION_MS) }],
      })
      .catch((error: unknown) => {
        if (!findErrorBy(error as Error, 'apiId', 'TOPIC_ALREADY_EXISTS')) {
          throw error;
        }
      });

    const metadata = await admin.metadata({ topics: [topic] });
    const partitions = metadata.topics.get(topic)?.partitionsCount;
    if (partitions !== PARTITIONS) {
      throw new Error(`Topic ${topic} has ${partitions} partitions, expected ${PARTITIONS}`);
    }
    log.info({ topic, partitions }, 'topic is ready');
  } finally {
    await admin.close();
  }
}
