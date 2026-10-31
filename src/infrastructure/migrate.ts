import { readdir, readFile } from 'node:fs/promises';
import { Pool } from 'pg';
import { withTransaction } from './transaction.js';

const MIGRATIONS_DIR = new URL('../../migrations/', import.meta.url);
const MIGRATION_LOCK_ID = 7_260_001;

/**
 * Applies pending SQL migrations in filename order and returns their names.
 * They commit together, and the lock makes concurrent runs take turns.
 */
export async function applyMigrations(databaseUrl: string): Promise<string[]> {
  const pool = new Pool({ connectionString: databaseUrl, max: 1 });

  try {
    return await withTransaction(pool, async (tx) => {
      await tx.query('SELECT pg_advisory_xact_lock($1)', [MIGRATION_LOCK_ID]);
      await tx.query(`
        CREATE TABLE IF NOT EXISTS schema_migrations (
          name       text        PRIMARY KEY,
          applied_at timestamptz NOT NULL DEFAULT now()
        )`);

      const { rows } = await tx.query<{ name: string }>('SELECT name FROM schema_migrations');
      const applied = new Set(rows.map((row) => row.name));
      const files = await readdir(MIGRATIONS_DIR);
      const pending = files.filter((file) => file.endsWith('.sql') && !applied.has(file)).sort();

      for (const file of pending) {
        await tx.query(await readFile(new URL(file, MIGRATIONS_DIR), 'utf8'));
        await tx.query('INSERT INTO schema_migrations (name) VALUES ($1)', [file]);
      }

      return pending;
    });
  } finally {
    await pool.end();
  }
}
