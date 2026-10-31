import { buildApi } from '../app/build-api.js';
import { loadConfig } from '../app/config.js';
import { closeOnSignal } from '../app/lifecycle.js';
import { createPool } from '../infrastructure/db.js';
import { createLogger } from '../infrastructure/logger.js';

const config = loadConfig();
const log = createLogger('api', config.logLevel);
const pool = createPool(config.databaseUrl, log);
const app = buildApi({ pool, log, lockTimeoutMs: config.lockTimeoutMs });

closeOnSignal(log, async () => {
  await app.close();
  await pool.end();
});

await app.listen({ host: config.host, port: config.port });
