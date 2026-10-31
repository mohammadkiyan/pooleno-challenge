import { loadExternalConfig } from '../app/config.js';
import { runAsSoleOwner } from '../app/sole-owner.js';
import { createPool } from '../infrastructure/db.js';
import { createLogger } from '../infrastructure/logger.js';
import { deliverNotification } from '../modules/external/external-client.js';
import { runNotificationWorker } from '../modules/external/notification-worker.js';
import { runExternalConsumer } from '../modules/messaging/external-consumer.js';

const EXTERNAL_LOCK_ID = 7_260_004;

const config = loadExternalConfig();
const log = createLogger('external', config.logLevel);
const target = { url: config.targetUrl, timeoutMs: config.timeoutMs };

await runAsSoleOwner(
  { databaseUrl: config.databaseUrl, lockId: EXTERNAL_LOCK_ID, log },
  async ({ signal, assertHeld }) => {
    const pool = createPool(config.databaseUrl, log);

    try {
      // Neither loop rejects, because each resumes from durable state until the signal
      // aborts. Kafka being down therefore stops no delivery, and the API being down no intake.
      await Promise.all([
        runExternalConsumer({
          pool,
          kafka: config.kafka,
          groupId: config.groupId,
          sessionTimeoutMs: config.sessionTimeoutMs,
          restartDelayMs: config.restartDelayMs,
          signal,
          log,
        }),
        runNotificationWorker({
          pool,
          deliver: (job) => deliverNotification(target, job),
          retry: config.retry,
          pollIntervalMs: config.pollIntervalMs,
          restartDelayMs: config.restartDelayMs,
          assertHeld,
          signal,
          log,
        }),
      ]);
    } finally {
      await pool.end();
    }
  },
);
