import { loadPublisherConfig } from '../app/config.js';
import { runAsSoleOwner } from '../app/sole-owner.js';
import { createPool } from '../infrastructure/db.js';
import { createLogger } from '../infrastructure/logger.js';
import { createEventProducer } from '../modules/messaging/kafka-producer.js';
import { publishOutbox } from '../modules/messaging/publish-outbox.js';

const PUBLISHER_LOCK_ID = 7_260_002;

const config = loadPublisherConfig();
const log = createLogger('publisher', config.logLevel);

await runAsSoleOwner(
  {
    databaseUrl: config.databaseUrl,
    lockId: PUBLISHER_LOCK_ID,
    log,
    shutdownTimeoutMs: config.shutdownTimeoutMs,
    health: config.health,
  },
  async ({ signal, assertHeld, reportReadiness }) => {
    const pool = createPool(config.databaseUrl, log);
    const producer = createEventProducer(config.kafka);
    reportReadiness(() =>
      producer.assertReachable().then(
        () => undefined,
        () => 'kafka unreachable',
      ),
    );

    try {
      await publishOutbox({
        pool,
        producer,
        retry: { baseMs: config.retryBaseMs, capMs: config.retryCapMs },
        pollIntervalMs: config.pollIntervalMs,
        assertHeld,
        signal,
        log,
      });
    } finally {
      await producer.close();
      await pool.end();
    }
  },
);
