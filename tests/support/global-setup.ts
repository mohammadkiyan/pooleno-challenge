import { Client } from 'pg';
import { applyMigrations } from '../../src/infrastructure/migrate.js';
import { assertKafkaIsReachable } from './kafka.js';
import { TEST_DATABASE_URL } from './test-database.js';

/** Recreates the test database, so every run starts from the real migrations. */
export default async function setup(): Promise<void> {
  await assertKafkaIsReachable();

  const url = new URL(TEST_DATABASE_URL);
  const databaseName = url.pathname.slice(1);
  url.pathname = '/postgres';

  const admin = new Client({ connectionString: url.href });
  try {
    await admin.connect();
  } catch (error) {
    throw new Error(
      `Cannot reach PostgreSQL at ${url.host}. Start it with: docker compose up -d --wait postgres`,
      { cause: error },
    );
  }

  try {
    await admin.query(`DROP DATABASE IF EXISTS "${databaseName}" WITH (FORCE)`);
    await admin.query(`CREATE DATABASE "${databaseName}"`);
  } finally {
    await admin.end();
  }

  await applyMigrations(TEST_DATABASE_URL);
}
