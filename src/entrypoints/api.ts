import { buildApi } from '../app/build-api.js';
import { loadConfig } from '../app/config.js';
import { closeOnSignal } from '../app/lifecycle.js';
import { createLogger } from '../infrastructure/logger.js';
import { readKafkaLag } from '../modules/operations/kafka-lag.js';

const config = loadConfig();
const log = createLogger('api', config.logLevel);

const app = await buildApi({
  databaseUrl: config.databaseUrl,
  log,
  lockTimeoutMs: config.lockTimeoutMs,
  readKafkaLag: () => readKafkaLag(config.kafka, config.consumerGroups, log),
});

closeOnSignal({ log, deadlineMs: config.shutdownTimeoutMs }, () => app.close());

await app.listen({ host: config.host, port: config.port });
