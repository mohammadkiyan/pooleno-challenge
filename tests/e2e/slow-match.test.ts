import { Pool } from 'pg';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { createTestStream, deleteTestStream, produceRaw, type TestStream } from '../support/kafka.js';
import { acceptEvent, ethOrder, validOrder } from '../support/orders.js';
import { killAllRoles, RoleProcess } from '../support/processes.js';
import { readOrder, resetDatabase, TEST_DATABASE_URL } from '../support/test-database.js';

const SESSION_TIMEOUT_MS = 6_000;

// Longer than the 5 s limit other roles use and than the Kafka session, so the write
// passes only with matching's own limit and only if heartbeats continue during it.
const SLOW_WRITE_SECONDS = 6.5;

const SLOWS_TRADE_INSERT = `
  CREATE OR REPLACE FUNCTION slow_trade() RETURNS trigger AS $$
  BEGIN PERFORM pg_sleep(${SLOW_WRITE_SECONDS}); RETURN NEW; END;
  $$ LANGUAGE plpgsql;

  CREATE TRIGGER slow_trade BEFORE INSERT ON trades
  FOR EACH ROW EXECUTE FUNCTION slow_trade();
`;
const REMOVES_THE_SLOWDOWN = `
  DROP TRIGGER IF EXISTS slow_trade ON trades;
  DROP FUNCTION IF EXISTS slow_trade;
`;

describe('a match that takes longer than 5 s to write', () => {
  let pool: Pool;
  let stream: TestStream;

  beforeAll(() => {
    pool = new Pool({ connectionString: TEST_DATABASE_URL });
  });

  afterAll(() => pool.end());

  beforeEach(async () => {
    await resetDatabase(pool);
    stream = await createTestStream();
    await pool.query(SLOWS_TRADE_INSERT);
  });

  afterEach(async () => {
    await killAllRoles();
    await pool.query(REMOVES_THE_SLOWDOWN);
    await deleteTestStream(stream);
  });

  it('completes first time, and the other symbol then proceeds', async () => {
    const maker = await acceptEvent(pool, ethOrder('SELL', '100.00', '0.0020'));
    const taker = await acceptEvent(pool, ethOrder('BUY', '100.00', '0.0020'));
    const otherSymbol = await acceptEvent(pool, validOrder);
    await produceRaw(
      stream,
      [maker, taker, otherSymbol].map((event) => ({
        key: event.symbol,
        value: JSON.stringify(event),
      })),
    );

    const role = new RoleProcess('matching', {
      KAFKA_BROKERS: stream.kafka.brokers.join(','),
      KAFKA_TOPIC: stream.kafka.topic,
      MATCHING_GROUP_ID: stream.groupId,
      MATCHING_SESSION_TIMEOUT_MS: String(SESSION_TIMEOUT_MS),
      MATCHING_RESTART_DELAY_MS: '100',
    });

    await vi.waitFor(
      async () => {
        expect((await readOrder(pool, taker.orderId))?.status).toBe('FILLED');
        expect((await readOrder(pool, otherSymbol.orderId))?.status).toBe('OPEN');
      },
      { timeout: 25_000, interval: 100 },
    );

    expect(role.countLogs('matching stopped; restoring from the database before retrying')).toBe(0);
    expect(role.countLogs('group membership changed; stopping this session')).toBe(0);
  });
});
