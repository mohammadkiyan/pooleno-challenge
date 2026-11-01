import { loadExternalConfig } from '../app/config.js';
import { runRole } from '../app/run-role.js';
import { runAsSoleOwner } from '../app/sole-owner.js';
import { createLogger } from '../infrastructure/logger.js';
import { ExternalModule, ExternalRole } from '../modules/external/external.role.js';

const EXTERNAL_LOCK_ID = 7_260_004;

const config = loadExternalConfig();
const log = createLogger('external', config.logLevel);

await runAsSoleOwner(
  {
    databaseUrl: config.databaseUrl,
    lockId: EXTERNAL_LOCK_ID,
    log,
    shutdownTimeoutMs: config.shutdownTimeoutMs,
    health: config.health,
  },
  (ownership) =>
    runRole({
      module: ExternalModule.forRole(config, log, ownership),
      role: ExternalRole,
      log,
      ownership,
    }),
);
