import { loadPublisherConfig } from '../app/config.js';
import { runRole } from '../app/run-role.js';
import { runAsSoleOwner } from '../app/sole-owner.js';
import { createLogger } from '../infrastructure/logger.js';
import { PublisherModule, PublisherRole } from '../modules/messaging/publisher.role.js';

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
  (ownership) =>
    runRole({
      module: PublisherModule.forRole(config, log, ownership),
      role: PublisherRole,
      log,
      ownership,
    }),
);
