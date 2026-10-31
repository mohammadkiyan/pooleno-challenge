import type { Pool, PoolClient } from 'pg';

/** Commits when `work` resolves and rolls back when it rejects. */
export async function withTransaction<T>(
  pool: Pool,
  work: (tx: PoolClient) => Promise<T>,
): Promise<T> {
  const client = await pool.connect();
  let connectionBroken = false;

  try {
    await client.query('BEGIN');
    const result = await work(client);
    await client.query('COMMIT');
    return result;
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {
      connectionBroken = true;
    });
    throw error;
  } finally {
    // Destroy a client that could not roll back rather than reuse its connection.
    client.release(connectionBroken);
  }
}

/** Bounds every lock wait in the current transaction. PostgreSQL raises 55P03 on expiry. */
export async function setLockTimeout(tx: PoolClient, timeoutMs: number): Promise<void> {
  await tx.query(`SELECT set_config('lock_timeout', $1, true)`, [`${timeoutMs}ms`]);
}
