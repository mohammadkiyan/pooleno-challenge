import { Pool } from 'pg';
import { pino } from 'pino';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { acquireSessionLock, type SessionLock } from '../../src/infrastructure/session-lock.js';
import { TEST_DATABASE_URL } from '../support/test-database.js';

const LOCK_ID = 7_260_900;
const log = pino({ level: 'silent' });

let pool: Pool;

beforeAll(() => {
  pool = new Pool({ connectionString: TEST_DATABASE_URL });
});

afterAll(() => pool.end());

async function acquire(signal = new AbortController().signal): Promise<SessionLock | undefined> {
  return acquireSessionLock(TEST_DATABASE_URL, LOCK_ID, { signal, log });
}

async function requireLock(): Promise<SessionLock> {
  const lock = await acquire();
  if (!lock) {
    throw new Error('The lock was not acquired');
  }
  return lock;
}

describe('session lock', () => {
  it('admits one owner, and the next only after the first has released', async () => {
    const first = await requireLock();
    let secondAcquired = false;
    const second = acquire().then((lock) => {
      secondAcquired = true;
      return lock;
    });

    // The waiter polls once right away, so give it a chance to be wrongly admitted.
    const { rows } = await pool.query<{ granted: number }>(
      `SELECT count(*)::int AS granted FROM pg_locks
        WHERE locktype = 'advisory' AND objid = $1 AND granted`,
      [LOCK_ID],
    );
    expect(rows[0]?.granted).toBe(1);
    expect(secondAcquired).toBe(false);

    await first.release();

    const successor = await second;
    expect(successor).toBeDefined();
    await successor?.release();
  });

  it('gives up waiting when told to stop', async () => {
    const owner = await requireLock();
    const stop = new AbortController();

    const waiting = acquire(stop.signal);
    stop.abort();

    await expect(waiting).resolves.toBeUndefined();
    await owner.release();
  });

  it('reports the loss when the connection holding the lock dies', async () => {
    const lock = await requireLock();
    await expect(lock.assertHeld()).resolves.toBeUndefined();

    await pool.query(
      `SELECT pg_terminate_backend(pid) FROM pg_locks
        WHERE locktype = 'advisory' AND objid = $1 AND granted`,
      [LOCK_ID],
    );

    await vi.waitFor(() => {
      expect(lock.lost.aborted).toBe(true);
    });
    await expect(lock.assertHeld()).rejects.toThrow();
    await expect(lock.release()).resolves.toBeUndefined();
  });

  it('does not report its own release as a loss', async () => {
    const lock = await requireLock();

    await lock.release();

    expect(lock.lost.aborted).toBe(false);
  });
});
