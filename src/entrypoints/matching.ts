import { loadMatchingConfig } from '../app/config.js';
import { runAsSoleOwner } from '../app/sole-owner.js';
import { createPool } from '../infrastructure/db.js';
import { createLogger } from '../infrastructure/logger.js';
import { runMatchingConsumer } from '../modules/messaging/matching-consumer.js';

const MATCHING_LOCK_ID = 7_260_003;

const config = loadMatchingConfig();
const log = createLogger('matching', config.logLevel);

await runAsSoleOwner(
  {
    databaseUrl: config.databaseUrl,
    lockId: MATCHING_LOCK_ID,
    log,
    shutdownTimeoutMs: config.shutdownTimeoutMs,
    health: config.health,
  },
  async ({ signal, reportReadiness }) => {
    const pool = createPool(config.databaseUrl, log, {
      statementTimeoutMs: config.statementTimeoutMs,
    });
    let assigned = false;
    let held = false;
    reportReadiness(() => {
      if (!assigned) {
        return Promise.resolve('no partitions assigned');
      }
      return Promise.resolve(held ? 'a partition is held on a failing event' : undefined);
    });

    try {
      await runMatchingConsumer({
        pool,
        kafka: config.kafka,
        groupId: config.groupId,
        sessionTimeoutMs: config.sessionTimeoutMs,
        restartDelayMs: config.restartDelayMs,
        laneCapacity: config.laneCapacity,
        signal,
        log,
        onAssignmentChange: (value) => {
          assigned = value;
        },
        onHeldChange: (value) => {
          held = value;
        },
      });
    } finally {
      await pool.end();
    }
  },
);
