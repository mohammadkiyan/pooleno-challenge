import { loadMatchingConfig } from '../app/config.js';
import { runRole } from '../app/run-role.js';
import { runAsSoleOwner } from '../app/sole-owner.js';
import { createLogger } from '../infrastructure/logger.js';
import { MatchingModule, MatchingRole } from '../modules/matching/matching.role.js';

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
  (ownership) =>
    runRole({
      module: MatchingModule.forRole(config, log, ownership),
      role: MatchingRole,
      log,
      ownership,
    }),
);
