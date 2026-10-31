import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process';
import { once } from 'node:events';
import { Pool } from 'pg';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { uniqueKey, validOrder, type AcceptedBody } from '../support/orders.js';
import {
  countRows,
  readCounter,
  resetDatabase,
  TEST_DATABASE_URL,
  waitForBlockedBackend,
} from '../support/test-database.js';

interface RunningApi {
  url: string;
  process: ChildProcessWithoutNullStreams;
}

const CRASH_BEFORE_RESPONSE = './tests/e2e/crash-before-response.preload.ts';
const running = new Set<RunningApi>();

let pool: Pool;

beforeAll(() => {
  pool = new Pool({ connectionString: TEST_DATABASE_URL });
});

afterAll(() => pool.end());

beforeEach(() => resetDatabase(pool));

afterEach(async () => {
  await Promise.all([...running].map(killApi));
});

/** Starts the real entrypoint as its own process, with PostgreSQL as its only state. */
async function startApi(preload?: string): Promise<RunningApi> {
  const child = spawn(
    process.execPath,
    ['--import', 'tsx', ...(preload ? ['--import', preload] : []), 'src/entrypoints/api.ts'],
    {
      env: {
        ...process.env,
        DATABASE_URL: TEST_DATABASE_URL,
        HOST: '127.0.0.1',
        PORT: '0',
        ACCEPTANCE_LOCK_TIMEOUT_MS: '30000',
      },
    },
  );

  const url = await new Promise<string>((resolve, reject) => {
    let output = '';
    child.stdout.on('data', (chunk: Buffer) => {
      output += chunk.toString();
      const match = /Server listening at (http:\/\/[^"]+)"/.exec(output);
      if (match?.[1]) {
        resolve(match[1]);
      }
    });
    child.stderr.on('data', (chunk: Buffer) => {
      output += chunk.toString();
    });
    child.once('exit', (code, signal) => {
      reject(new Error(`API exited early (${code ?? signal ?? 'unknown'}):\n${output}`));
    });
  });

  const api = { url, process: child };
  running.add(api);
  return api;
}

/** SIGKILL gives the process no chance to flush or clean up, as in a real crash. */
async function killApi(api: RunningApi): Promise<void> {
  running.delete(api);
  if (api.process.exitCode === null && api.process.signalCode === null) {
    const exited = once(api.process, 'exit');
    api.process.kill('SIGKILL');
    await exited;
  }
}

function postOrder(api: RunningApi, key: string): Promise<Response> {
  return fetch(new URL('/v1/orders', api.url), {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'idempotency-key': key },
    body: JSON.stringify(validOrder),
  });
}

describe('crash recovery', () => {
  it('keeps an accepted order through a crash and restart', async () => {
    const key = uniqueKey();
    const firstProcess = await startApi();

    const accepted = await postOrder(firstProcess, key);
    expect(accepted.status).toBe(202);
    const receipt = (await accepted.json()) as AcceptedBody;

    const before = await fetch(new URL(receipt.statusUrl, firstProcess.url));
    expect(before.status).toBe(200);
    const stateBefore: unknown = await before.json();

    await killApi(firstProcess);
    const secondProcess = await startApi();

    const after = await fetch(new URL(receipt.statusUrl, secondProcess.url));
    expect(after.status).toBe(200);
    expect(await after.json()).toEqual(stateBefore);

    const replay = await postOrder(secondProcess, key);
    expect(replay.status).toBe(202);
    expect(replay.headers.get('location')).toBe(accepted.headers.get('location'));
    expect(await replay.json()).toEqual(receipt);
    expect(await countRows(pool)).toEqual({ orders: 1, receipts: 1, events: 1 });
  });

  it('recovers when the process dies after commit but before the client hears back', async () => {
    const key = uniqueKey();
    const doomedProcess = await startApi(CRASH_BEFORE_RESPONSE);
    const exited = once(doomedProcess.process, 'exit');

    await expect(postOrder(doomedProcess, key)).rejects.toThrow();

    const [, signal] = (await exited) as [number | null, NodeJS.Signals | null];
    expect(signal).toBe('SIGKILL');
    expect(await countRows(pool)).toEqual({ orders: 1, receipts: 1, events: 1 });

    const { rows } = await pool.query<{ id: string }>('SELECT id FROM orders');
    const storedOrderId = rows[0]?.id;

    const recoveredProcess = await startApi();
    const retry = await postOrder(recoveredProcess, key);

    expect(retry.status).toBe(202);
    expect(((await retry.json()) as AcceptedBody).orderId).toBe(storedOrderId);
    expect(await countRows(pool)).toEqual({ orders: 1, receipts: 1, events: 1 });
  });

  it('discards an open transaction when the process dies before commit', async () => {
    const firstProcess = await startApi();
    await postOrder(firstProcess, uniqueKey());

    // Holding the counter parks the next request mid-transaction, after it claims its key.
    const holder = await pool.connect();
    await holder.query('BEGIN');
    await holder.query(`SELECT 1 FROM symbol_sequences WHERE symbol = 'BTC-USD' FOR UPDATE`);

    const key = uniqueKey();
    const interrupted = expect(postOrder(firstProcess, key)).rejects.toThrow();
    await waitForBlockedBackend(pool);

    await killApi(firstProcess);
    await interrupted;

    await holder.query('ROLLBACK');
    holder.release();

    const secondProcess = await startApi();
    const retry = await postOrder(secondProcess, key);

    expect(retry.status).toBe(202);
    expect(await countRows(pool)).toEqual({ orders: 2, receipts: 2, events: 2 });
    expect(await readCounter(pool, 'BTC-USD')).toBe('2');
  });
});
