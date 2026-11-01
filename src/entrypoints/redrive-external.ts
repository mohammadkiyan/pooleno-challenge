// The explicit, manual way to give failed external jobs another budget of attempts.
import { loadConfig } from '../app/config.js';
import { createPool } from '../infrastructure/db.js';
import { createLogger } from '../infrastructure/logger.js';
import { redriveJob } from '../modules/external/external-job.repository.js';

const eventIds = process.argv.slice(2);
if (eventIds.length === 0) {
  throw new Error('Usage: redrive-external <eventId> [<eventId> ...]');
}

const config = loadConfig();
const log = createLogger('redrive-external', config.logLevel);
const pool = createPool(config.databaseUrl, log);

try {
  for (const eventId of eventIds) {
    // One bad ID must not cost the others, and the exit status still reports it.
    try {
      if (await redriveJob(pool, eventId)) {
        log.info({ eventId }, 'job redriven');
      } else {
        log.error({ eventId }, 'no failed job with this event ID');
        process.exitCode = 1;
      }
    } catch (error) {
      log.error({ err: error, eventId }, 'redrive failed');
      process.exitCode = 1;
    }
  }
} finally {
  await pool.end();
}
