import { randomUUID } from 'node:crypto';
import { Pool } from 'pg';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { applyMigrations } from '../../src/infrastructure/migrate.js';
import { resetDatabase, TEST_DATABASE_URL } from '../support/test-database.js';

const UNIQUE_VIOLATION = { code: '23505' };
const CHECK_VIOLATION = { code: '23514' };
const FOREIGN_KEY_VIOLATION = { code: '23503' };

let pool: Pool;

beforeAll(() => {
  pool = new Pool({ connectionString: TEST_DATABASE_URL });
});

afterAll(() => pool.end());

beforeEach(() => resetDatabase(pool));

async function insertOrderRow(
  { seq = 1, original = '100', remaining = '100' } = {},
): Promise<string> {
  const id = randomUUID();
  await pool.query(
    `INSERT INTO orders
       (id, scope, symbol, side, order_type, price_units, original_units, remaining_units,
        status, acceptance_seq, created_at, updated_at)
     VALUES ($1, 'demo', 'BTC-USD', 'BUY', 'LIMIT', 1, $2, $3, 'QUEUED', $4, now(), now())`,
    [id, original, remaining, seq],
  );
  return id;
}

function insertOutboxRow(orderId: string) {
  return pool.query(
    `INSERT INTO outbox_events
       (event_id, event_type, schema_version, order_id, symbol, acceptance_seq, payload)
     VALUES ($1, 'OrderAccepted', 1, $2, 'BTC-USD', 1, '{}')`,
    [randomUUID(), orderId],
  );
}

function insertReceiptRow(orderId: string, key = `key-${randomUUID()}`) {
  return pool.query(
    `INSERT INTO idempotency_requests
       (scope, operation, idempotency_key, fingerprint, order_id,
        response_status, response_body, response_location)
     VALUES ('demo', 'POST /v1/orders', $1, $2, $3, 202, '{}', '/v1/orders/x')`,
    [key, 'a'.repeat(64), orderId],
  );
}

describe('database constraints, the last line of defence', () => {
  it('allows one order per symbol sequence', async () => {
    await insertOrderRow({ seq: 7 });

    await expect(insertOrderRow({ seq: 7 })).rejects.toMatchObject(UNIQUE_VIOLATION);
  });

  it.each([
    ['above the original', '101'],
    ['negative', '-1'],
  ])('rejects remaining units %s', async (_title, remaining) => {
    await expect(insertOrderRow({ remaining })).rejects.toMatchObject(CHECK_VIOLATION);
  });

  it('allows one acceptance event per order', async () => {
    const orderId = await insertOrderRow();
    await insertOutboxRow(orderId);

    await expect(insertOutboxRow(orderId)).rejects.toMatchObject(UNIQUE_VIOLATION);
  });

  it('allows one key per order', async () => {
    const orderId = await insertOrderRow();
    await insertReceiptRow(orderId);

    await expect(insertReceiptRow(orderId)).rejects.toMatchObject(UNIQUE_VIOLATION);
  });

  it('rejects a receipt whose order does not exist by commit', async () => {
    await expect(insertReceiptRow(randomUUID())).rejects.toMatchObject(FOREIGN_KEY_VIOLATION);
  });

  it('stores amounts exactly, far beyond the range of a float', async () => {
    const largest = '9'.repeat(38);
    const orderId = await insertOrderRow({ original: largest, remaining: largest });

    const { rows } = await pool.query<{ original_units: string }>(
      'SELECT original_units FROM orders WHERE id = $1',
      [orderId],
    );

    expect(rows[0]?.original_units).toBe(largest);
  });
});

describe('applyMigrations', () => {
  it('does nothing when the schema is current', async () => {
    await expect(applyMigrations(TEST_DATABASE_URL)).resolves.toEqual([]);
  });
});
