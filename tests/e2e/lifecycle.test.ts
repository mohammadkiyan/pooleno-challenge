import { Pool, type PoolClient } from 'pg';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  committedMessages,
  createTestStream,
  deleteTestStream,
  type TestStream,
} from '../support/kafka.js';
import { acceptEvent, ethOrder, uniqueKey, validOrder, type AcceptedBody } from '../support/orders.js';
import {
  CLEAN_EXIT,
  healthUrlOf,
  killAllRoles,
  readHealth,
  requestApi,
  RoleProcess,
  startApi,
} from '../support/processes.js';
import { startStallingProxy, type StallingProxy } from '../support/stalling-proxy.js';
import {
  readOrder,
  readTrades,
  resetDatabase,
  TEST_DATABASE_URL,
  terminateAdvisoryLockHolders,
  waitForBlockedBackend,
} from '../support/test-database.js';

const CLOSED_PORT = '127.0.0.1:1';
const UNREACHABLE_DESTINATION = `http://${CLOSED_PORT}/v1/notifications`;
const SESSION_TIMEOUT_MS = 6_000;
const SHORT_DEADLINE_MS = '300';

type ConsumingRole = 'publisher' | 'matching' | 'external';

const heldRows = new Set<PoolClient>();

let pool: Pool;
let stream: TestStream;

beforeAll(() => {
  pool = new Pool({ connectionString: TEST_DATABASE_URL });
});

afterAll(() => pool.end());

beforeEach(async () => {
  await resetDatabase(pool);
  stream = await createTestStream();
});

afterEach(async () => {
  await killAllRoles();
  await Promise.all([...heldRows].map(releaseRow));
  await deleteTestStream(stream);
});

function startRole(role: ConsumingRole, env: NodeJS.ProcessEnv = {}): RoleProcess {
  return new RoleProcess(role, {
    KAFKA_BROKERS: stream.kafka.brokers.join(','),
    KAFKA_TOPIC: stream.kafka.topic,
    MATCHING_GROUP_ID: stream.groupId,
    MATCHING_SESSION_TIMEOUT_MS: String(SESSION_TIMEOUT_MS),
    MATCHING_RESTART_DELAY_MS: '100',
    EXTERNAL_GROUP_ID: `${stream.groupId}-external`,
    EXTERNAL_SESSION_TIMEOUT_MS: String(SESSION_TIMEOUT_MS),
    EXTERNAL_RESTART_DELAY_MS: '100',
    EXTERNAL_API_URL: UNREACHABLE_DESTINATION,
    OUTBOX_POLL_INTERVAL_MS: '20',
    ...env,
  });
}

async function holdRow(sql: string, parameters: unknown[]): Promise<PoolClient> {
  const holder = await pool.connect();
  heldRows.add(holder);
  await holder.query('BEGIN');
  await holder.query(sql, parameters);
  return holder;
}

async function releaseRow(holder: PoolClient): Promise<void> {
  heldRows.delete(holder);
  await holder.query('ROLLBACK');
  holder.release();
}

function holdProgressRow(symbol: string): Promise<PoolClient> {
  return holdRow('SELECT 1 FROM matching_progress WHERE symbol = $1 FOR UPDATE', [symbol]);
}

async function waitForReadiness(
  role: RoleProcess,
  expected: { status: number; body: object },
): Promise<void> {
  const url = await healthUrlOf(role);
  await vi.waitFor(
    async () => {
      expect(await readHealth(url, '/health/ready')).toEqual(expected);
    },
    { timeout: 45_000, interval: 50 },
  );
}

const READY = { status: 200, body: { status: 'ready' } };

function notReady(reason: string): { status: number; body: object } {
  return { status: 503, body: { status: 'not_ready', reason } };
}

/** Starts the matcher and parks it inside the transaction of a buyer's event. */
async function parkMatcherInTransaction(env: NodeJS.ProcessEnv = {}) {
  const seller = await acceptEvent(pool, ethOrder('SELL', '100.00', '0.0030'));
  startRole('publisher');
  const matcher = startRole('matching', env);
  await vi.waitFor(
    async () => {
      expect(await readOrder(pool, seller.orderId)).toMatchObject({ status: 'OPEN' });
    },
    { timeout: 45_000, interval: 50 },
  );

  const holder = await holdProgressRow('ETH-USD');
  const buyer = await acceptEvent(pool, ethOrder('BUY', '100.00', '0.0010'));
  await waitForBlockedBackend(pool);
  return { matcher, holder, seller, buyer };
}

describe('graceful shutdown', { timeout: 120_000 }, () => {
  it('bounds a shutdown that arrives while joining the group', async () => {
    const first = startRole('matching');
    await waitForReadiness(first, READY);
    await first.kill();

    // The group still counts the dead member, so joining now waits out its session, and
    // closing the consumer cannot interrupt that.
    const joining = startRole('matching', { SHUTDOWN_TIMEOUT_MS: SHORT_DEADLINE_MS });
    await joining.waitForLog('books restored');

    const stoppedAt = Date.now();
    const exit = await joining.terminate();

    expect(exit).toEqual({ code: 1, signal: null });
    expect(joining.countLogs('shutdown deadline passed; exiting')).toBe(1);
    expect(Date.now() - stoppedAt).toBeLessThan(SESSION_TIMEOUT_MS / 2);
  });

  it('reports not ready, while still alive, as soon as a shutdown starts', async () => {
    const { matcher, holder } = await parkMatcherInTransaction();
    await waitForReadiness(matcher, READY);
    const url = await healthUrlOf(matcher);

    const exit = matcher.terminate();
    await matcher.waitForLog('shutting down');

    expect(await readHealth(url, '/health/ready')).toEqual(notReady('shutting down'));
    expect(await readHealth(url, '/health/live')).toEqual({
      status: 200,
      body: { status: 'ok' },
    });

    await releaseRow(holder);
    expect(await exit).toEqual(CLEAN_EXIT);
  });

  it('forces an exit at the deadline, and the abandoned work is replayed once', async () => {
    const { matcher, holder, seller, buyer } = await parkMatcherInTransaction({
      SHUTDOWN_TIMEOUT_MS: SHORT_DEADLINE_MS,
    });

    const exit = await matcher.terminate();

    expect(exit).toEqual({ code: 1, signal: null });
    expect(matcher.countLogs('shutdown deadline passed; exiting')).toBe(1);

    await releaseRow(holder);
    expect(await readOrder(pool, buyer.orderId)).toMatchObject({ status: 'QUEUED' });
    expect(await readTrades(pool)).toHaveLength(0);
    expect(await committedMessages(stream)).toBe(1);

    startRole('matching');
    await vi.waitFor(
      async () => {
        expect(await readOrder(pool, buyer.orderId)).toMatchObject({ status: 'FILLED' });
      },
      { timeout: 45_000, interval: 50 },
    );
    expect(await readTrades(pool)).toHaveLength(1);
    expect(await readOrder(pool, seller.orderId)).toEqual({
      status: 'PARTIALLY_FILLED',
      remaining_units: '20',
    });
  });

  it('lets a second signal end a stuck shutdown at once', async () => {
    const { matcher, holder } = await parkMatcherInTransaction();

    const exit = matcher.terminate();
    await matcher.waitForLog('shutting down');
    matcher.terminate().catch(() => undefined);

    expect(await exit).toEqual({ code: null, signal: 'SIGTERM' });
    await releaseRow(holder);
  });

  it('finishes a request in flight and refuses new connections', async () => {
    const api = await startApi();
    const first = await requestApi(api, '/v1/orders', {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'idempotency-key': uniqueKey() },
      body: JSON.stringify(validOrder),
    });
    expect(first.status).toBe(202);

    // Holding the counter parks the next request after its transaction has begun.
    const holder = await holdRow(
      `SELECT 1 FROM symbol_sequences WHERE symbol = 'BTC-USD' FOR UPDATE`,
      [],
    );
    const inFlight = requestApi(api, '/v1/orders', {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'idempotency-key': uniqueKey() },
      body: JSON.stringify(validOrder),
    });
    await waitForBlockedBackend(pool);

    const exit = api.role.terminate();
    await api.role.waitForLog('shutting down');
    await vi.waitFor(async () => {
      await expect(requestApi(api, '/health/live')).rejects.toThrow();
    });

    await releaseRow(holder);

    const answer = await inFlight;
    expect(answer.status).toBe(202);
    const { orderId } = (await answer.json()) as AcceptedBody;
    expect(await readOrder(pool, orderId)).toMatchObject({ status: 'QUEUED' });
    expect(await exit).toEqual(CLEAN_EXIT);
  });
});

describe('stopping with no signal', { timeout: 120_000 }, () => {
  let proxy: StallingProxy;

  beforeEach(async () => {
    proxy = await startStallingProxy(TEST_DATABASE_URL);
  });

  afterEach(async () => {
    await killAllRoles();
    await proxy.close();
  });

  async function expectSuccessorToAcquire(): Promise<void> {
    await startRole('publisher').waitForLog('acquired ownership');
  }

  it('ends a standby whose connection goes quiet, and its lock reaches a successor', async () => {
    const owner = startRole('publisher');
    await owner.waitForLog('acquired ownership');
    const standby = startRole('publisher', { DATABASE_URL: proxy.url });
    await standby.waitForLog('another process owns this role; waiting');

    // Its next attempt still takes the lock on the server. Only the answer is lost.
    proxy.stall('replies');
    await owner.kill();

    expect(await standby.exited).toEqual({ code: 1, signal: null });
    await expectSuccessorToAcquire();
  });

  it('ends an owner at the deadline when quiet sockets would keep it alive', async () => {
    const owner = startRole('publisher', {
      DATABASE_URL: proxy.url,
      SHUTDOWN_TIMEOUT_MS: SHORT_DEADLINE_MS,
      OUTBOX_POLL_INTERVAL_MS: '2000',
    });
    await owner.waitForLog('acquired ownership');

    // The pool's idle connection is what closes slowly, so it must be open, and the role must be
    // pausing between passes. A pass caught mid-query waits on, and never checks the lock again.
    await vi.waitFor(
      () => {
        expect(proxy.connections()).toBeGreaterThanOrEqual(2);
        expect(proxy.quietForMs()).toBeGreaterThan(200);
      },
      { timeout: 30_000, interval: 50 },
    );
    proxy.stall();

    expect(await owner.exited).toEqual({ code: 1, signal: null });
    expect(owner.countLogs('shutdown deadline passed; exiting')).toBe(1);

    await proxy.close();
    await expectSuccessorToAcquire();
  });

  it('bounds the drain after ownership is lost, while work is still parked', async () => {
    const { matcher, holder } = await parkMatcherInTransaction({
      SHUTDOWN_TIMEOUT_MS: SHORT_DEADLINE_MS,
    });

    await terminateAdvisoryLockHolders(pool);

    expect(await matcher.exited).toEqual({ code: 1, signal: null });
    expect(matcher.countLogs('shutdown deadline passed; exiting')).toBe(1);
    await releaseRow(holder);
  });
});

describe('structured logs', { timeout: 120_000 }, () => {
  /** The one log line with this message, parsed. */
  async function logLine(role: RoleProcess, message: string): Promise<Record<string, unknown>> {
    const [line] = await role.waitForMatch(new RegExp(`\\{[^\\n]*"msg":"${message}"[^\\n]*\\}`));
    return JSON.parse(line) as Record<string, unknown>;
  }

  it('follow one request across every role, without its idempotency key', async () => {
    const key = uniqueKey();
    const api = await startApi();
    const publisher = startRole('publisher');
    const matcher = startRole('matching');
    const external = startRole('external');

    const accepted = await requestApi(api, '/v1/orders', {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'idempotency-key': key },
      body: JSON.stringify(validOrder),
    });
    expect(accepted.status).toBe(202);

    const origin = await logLine(api.role, 'order accepted');
    const { requestId, orderId, eventId, symbol, acceptanceSequence } = origin;
    const shared = { requestId, orderId, eventId, symbol, acceptanceSequence };
    expect(Object.values(shared).every((value) => typeof value === 'string')).toBe(true);
    expect(origin).toMatchObject({ role: 'api' });

    expect(await logLine(publisher, 'event published')).toMatchObject({
      role: 'publisher',
      ...shared,
    });
    expect(await logLine(matcher, 'order matched')).toMatchObject({ role: 'matching', ...shared });
    expect(await logLine(external, 'notification scheduled')).toMatchObject({
      role: 'external',
      ...shared,
    });
    // The destination is closed here, so the attempt fails, which is also worth following.
    expect(await logLine(external, 'delivery failed; will retry')).toMatchObject(shared);

    for (const role of [api.role, publisher, matcher, external]) {
      expect(role.logsContain(key)).toBe(false);
    }
  });
});

describe('health of each role', { timeout: 120_000 }, () => {
  it('the API is ready with PostgreSQL alone, and stays silent about probes', async () => {
    const api = await startApi();

    const live = await requestApi(api, '/health/live');
    const ready = await requestApi(api, '/health/ready');

    expect(live.status).toBe(200);
    expect(ready.status).toBe(200);
    expect(api.role.logsContain('/health/')).toBe(false);
  });

  it.each(['publisher', 'matching', 'external'] as const)(
    'the %s role is ready once it can work, and shuts down cleanly',
    async (name) => {
      const role = startRole(name);

      await waitForReadiness(role, READY);

      expect(await role.terminate()).toEqual(CLEAN_EXIT);
    },
  );

  it('a standby is alive but not ready, until it takes over', async () => {
    const owner = startRole('publisher');
    await waitForReadiness(owner, READY);

    const standby = startRole('publisher');
    await standby.waitForLog('another process owns this role; waiting');
    const url = await healthUrlOf(standby);
    expect(await readHealth(url, '/health/ready')).toEqual(notReady('waiting for ownership'));
    expect((await readHealth(url, '/health/live')).status).toBe(200);

    await owner.kill();

    await waitForReadiness(standby, READY);
  });

  it.each([
    ['publisher', 'kafka unreachable'],
    ['matching', 'no partitions assigned'],
    ['external', 'no partitions assigned'],
  ] as const)('without Kafka the %s role is alive but not ready', async (name, reason) => {
    const role = startRole(name, { KAFKA_BROKERS: CLOSED_PORT });
    const url = await healthUrlOf(role);

    await waitForReadiness(role, notReady(reason));

    expect((await readHealth(url, '/health/live')).status).toBe(200);
    expect(role.countLogs('shutting down')).toBe(0);
  });

  it('an unreachable destination leaves the external role ready', async () => {
    const role = startRole('external', { EXTERNAL_API_URL: UNREACHABLE_DESTINATION });

    await waitForReadiness(role, READY);

    expect((await readHealth(await healthUrlOf(role), '/health/live')).status).toBe(200);
  });

  it('a role that cannot bind its health port stops, rather than run unobserved', async () => {
    const owner = startRole('publisher');
    const url = await healthUrlOf(owner);

    const clash = startRole('publisher', { HEALTH_PORT: new URL(url).port });

    expect(await clash.exited).toEqual({ code: 1, signal: null });
    expect(clash.countLogs('acquired ownership')).toBe(0);
  });
});
