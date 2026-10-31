import type { Logger } from 'pino';
import { acquireSessionLock } from '../infrastructure/session-lock.js';
import { shutdownSignal } from './lifecycle.js';

export interface Ownership {
  /** Aborts on shutdown and when ownership is lost. The role must then stop and return. */
  signal: AbortSignal;
  assertHeld: () => Promise<void>;
}

/**
 * Runs a role only while it is the single owner, as enforced by PostgreSQL. A
 * standby waits here for the lock. The lock is released only after the role has
 * returned, so a successor starts after this process has stopped working. Losing
 * ownership or failing exits non-zero, so a supervisor can start a fresh process.
 */
export async function runAsSoleOwner(
  options: { databaseUrl: string; lockId: number; log: Logger },
  role: (ownership: Ownership) => Promise<void>,
): Promise<void> {
  const { databaseUrl, lockId, log } = options;
  const shutdown = shutdownSignal(log);

  try {
    const lock = await acquireSessionLock(databaseUrl, lockId, { signal: shutdown, log });
    if (!lock) {
      return;
    }

    try {
      log.info('acquired ownership');
      await role({
        signal: AbortSignal.any([shutdown, lock.lost]),
        assertHeld: () => lock.assertHeld(),
      });
      if (lock.lost.aborted) {
        throw new Error('Ownership was lost', { cause: lock.lost.reason });
      }
    } finally {
      await lock.release();
    }
  } catch (error) {
    log.error({ err: error }, 'stopping');
    process.exitCode = 1;
  }
}
