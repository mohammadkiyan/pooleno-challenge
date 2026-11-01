import { Pool } from 'pg';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { quarantineMessage } from '../../src/modules/messaging/quarantine.js';
import { readQuarantine } from '../support/external-jobs.js';
import { resetDatabase, TEST_DATABASE_URL } from '../support/test-database.js';

const MESSAGE = { topic: 'orders.accepted', partition: 0, offset: 7n, value: 'not an event' };

describe('the quarantine table', () => {
  let pool: Pool;

  beforeAll(() => {
    pool = new Pool({ connectionString: TEST_DATABASE_URL });
  });

  afterAll(() => pool.end());

  beforeEach(() => resetDatabase(pool));

  it('keeps one row when a redelivery records the same message again', async () => {
    await quarantineMessage(pool, 'matching-v1', MESSAGE, 'first error');
    await quarantineMessage(pool, 'matching-v1', MESSAGE, 'second error');

    const rows = await readQuarantine(pool);

    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({ consumer: 'matching-v1', error: 'first error' });
  });

  it('keeps one row for each consumer that reads the message', async () => {
    await quarantineMessage(pool, 'matching-v1', MESSAGE, 'error');
    await quarantineMessage(pool, 'external-notification-v1', MESSAGE, 'error');

    expect(await readQuarantine(pool)).toHaveLength(2);
  });

  it('keeps a different message at a reused position, as after Kafka loses its data', async () => {
    await quarantineMessage(pool, 'matching-v1', MESSAGE, 'error');
    await quarantineMessage(pool, 'matching-v1', { ...MESSAGE, value: 'something else' }, 'error');

    expect(await readQuarantine(pool)).toHaveLength(2);
  });

  it('accepts a value larger than an index entry can hold', async () => {
    const value = Array.from({ length: 2_000 }, (_, index) => index.toString(36)).join('').repeat(5);

    await quarantineMessage(pool, 'matching-v1', { ...MESSAGE, value }, 'error');

    expect((await readQuarantine(pool))[0]?.raw_value.toString()).toBe(value);
  });
});
