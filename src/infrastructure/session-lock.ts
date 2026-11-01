import { Client } from 'pg';
import type { Logger } from 'pino';
import { pause } from './pause.js';

export interface SessionLock {
  /** Aborts when the connection holding the lock is lost, which also frees the lock. */
  lost: AbortSignal;
  /** Rejects unless the connection holding the lock still answers. */
  assertHeld(): Promise<void>;
  release(): Promise<void>;
}

const RETRY_MS = 1_000;

/**
 * Waits for a PostgreSQL session advisory lock, held on a dedicated connection for
 * as long as the caller works. Resolves to undefined if `signal` aborts first.
 */
export async function acquireSessionLock(
  databaseUrl: string,
  lockId: number,
  { signal, log }: { signal: AbortSignal; log: Logger },
): Promise<SessionLock | undefined> {
  const client = new Client({
    connectionString: databaseUrl,
    keepAlive: true,
    connectionTimeoutMillis: 2_000,
    query_timeout: 5_000,
  });

  const lost = new AbortController();
  let releasing = false;
  const markLost = (reason: Error): void => {
    if (!releasing) {
      lost.abort(reason);
    }
  };
  client.on('error', (error) => {
    log.error({ err: error }, 'ownership connection failed');
    markLost(error);
  });
  client.on('end', () => {
    markLost(new Error('The connection holding the lock ended'));
  });

  const release = async (): Promise<void> => {
    releasing = true;
    await client.end();
  };

  await client.connect();

  let announced = false;
  try {
    while (!signal.aborted) {
      const { rows } = await client.query<{ acquired: boolean }>(
        'SELECT pg_try_advisory_lock($1) AS acquired',
        [lockId],
      );
      if (rows[0]?.acquired) {
        return {
          lost: lost.signal,
          assertHeld: async () => {
            await client.query('SELECT 1');
          },
          release,
        };
      }

      if (!announced) {
        log.info('another process owns this role; waiting');
        announced = true;
      }
      await pause(RETRY_MS, signal);
    }
  } catch (error) {
    // A query that timed out here can still have run, and taken the lock for this session, so
    // the connection must go. Not awaited, so that a quiet socket can't hide this failure.
    void release();
    throw error;
  }

  await release();
  return undefined;
}
