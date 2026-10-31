import { Pool } from 'pg';
import type { Logger } from 'pino';

export function createPool(databaseUrl: string, log: Logger): Pool {
  const pool = new Pool({
    connectionString: databaseUrl,
    max: 10,
    connectionTimeoutMillis: 2_000,
    statement_timeout: 5_000,
    idle_in_transaction_session_timeout: 10_000,
  });

  // An idle client that loses its connection reports here, and an unhandled
  // 'error' event would end the process. The pool opens a new client on demand.
  pool.on('error', (error) => {
    log.error({ err: error }, 'idle database client failed');
  });

  return pool;
}
