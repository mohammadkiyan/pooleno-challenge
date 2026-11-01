import type { Logger } from 'pino';
import { acquireSessionLock, type SessionLock } from '../infrastructure/session-lock.js';
import {
  closeHealthServer,
  startHealthServer,
  type HealthAddress,
  type ReadinessCheck,
} from './health-server.js';
import { shutdownSignal } from './lifecycle.js';

export interface Ownership {
  /** Aborts on shutdown and when ownership is lost. The role must then stop and return. */
  signal: AbortSignal;
  assertHeld: () => Promise<void>;
  /** Adds what this role needs, beyond ownership, to be reported as ready. */
  reportReadiness: (check: ReadinessCheck) => void;
}

export interface SoleOwnerOptions {
  databaseUrl: string;
  lockId: number;
  log: Logger;
  shutdownTimeoutMs: number;
  health: HealthAddress;
}

/**
 * Runs a role only while it is the single owner, as enforced by PostgreSQL. A
 * standby waits here for the lock. The lock is released only after the role has
 * returned, so a successor starts after this process has stopped working. Losing
 * ownership or failing exits non-zero, so a supervisor can start a fresh process.
 *
 * A standby and a role shutting down are alive but not ready. Asking the lock's connection
 * for an answer also proves that PostgreSQL is reachable.
 */
export async function runAsSoleOwner(
  options: SoleOwnerOptions,
  role: (ownership: Ownership) => Promise<void>,
): Promise<void> {
  const { databaseUrl, lockId, log, shutdownTimeoutMs, health } = options;
  const shutdown = shutdownSignal({ log, deadlineMs: shutdownTimeoutMs });

  let lock: SessionLock | undefined;
  let roleCheck: ReadinessCheck = () => Promise.resolve(undefined);
  const isReady: ReadinessCheck = async () => {
    if (shutdown.aborted) {
      return 'shutting down';
    }
    if (!lock) {
      return 'waiting for ownership';
    }
    try {
      await lock.assertHeld();
    } catch {
      return 'ownership cannot be confirmed';
    }
    return roleCheck();
  };

  try {
    const server = await startHealthServer(health, isReady, log);
    try {
      lock = await acquireSessionLock(databaseUrl, lockId, { signal: shutdown, log });
      if (!lock) {
        return;
      }

      try {
        log.info('acquired ownership');
        await role({
          signal: AbortSignal.any([shutdown, lock.lost]),
          assertHeld: () => (lock as SessionLock).assertHeld(),
          reportReadiness: (check) => {
            roleCheck = check;
          },
        });
        if (lock.lost.aborted) {
          throw new Error('Ownership was lost', { cause: lock.lost.reason });
        }
      } finally {
        await lock.release();
      }
    } finally {
      await closeHealthServer(server);
    }
  } catch (error) {
    log.error({ err: error }, 'stopping');
    process.exitCode = 1;
  }
}
