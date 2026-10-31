import { loadKafkaConfig } from '../app/config.js';
import { createLogger } from '../infrastructure/logger.js';
import { ensureTopic } from '../modules/messaging/kafka-topics.js';

const log = createLogger('kafka-init', process.env.LOG_LEVEL ?? 'info');

await ensureTopic(loadKafkaConfig(), log);
