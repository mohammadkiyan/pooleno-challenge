import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { IdempotencyStore } from '../../mock-api/idempotency-store.js';

const KEY = 'external-notify:event-1';

let directory: string;
let path: string;
let store: IdempotencyStore;

beforeEach(async () => {
  directory = await mkdtemp(join(tmpdir(), 'pooleno-mock-store-'));
  path = join(directory, 'mock.sqlite');
  store = new IdempotencyStore(path);
});

afterEach(async () => {
  store.close();
  await rm(directory, { recursive: true });
});

function reopen(): void {
  store.close();
  store = new IdempotencyStore(path);
}

describe('IdempotencyStore', () => {
  it('causes one effect for a new key', () => {
    const result = store.apply(KEY, '{"a":1}');

    expect(result.outcome).toBe('applied');
    expect(store.listEffects()).toEqual([
      expect.objectContaining({ idempotencyKey: KEY, body: '{"a":1}' }),
    ]);
  });

  it('replays the first response for the same key and body, with no second effect', () => {
    const first = store.apply(KEY, '{"a":1}');
    const second = store.apply(KEY, '{"a":1}');

    expect(second.outcome).toBe('replayed');
    expect(second).toHaveProperty('response', (first as { response: unknown }).response);
    expect(store.listEffects()).toHaveLength(1);
  });

  it('still recognises a key after a restart', () => {
    const first = store.apply(KEY, '{"a":1}');

    reopen();
    const second = store.apply(KEY, '{"a":1}');

    expect(second.outcome).toBe('replayed');
    expect(second).toHaveProperty('response', (first as { response: unknown }).response);
    expect(store.listEffects()).toHaveLength(1);
  });

  it('rejects a different body under a key, even one differing only in spacing', () => {
    store.apply(KEY, '{"a":1}');

    expect(store.apply(KEY, '{"a": 1}')).toEqual({ outcome: 'conflict' });
    expect(store.listEffects()).toHaveLength(1);
  });

  it('keeps separate keys separate', () => {
    store.apply(KEY, '{"a":1}');
    store.apply('external-notify:event-2', '{"a":1}');

    expect(store.listEffects()).toHaveLength(2);
  });

  it('records the key and the effect together or not at all', () => {
    const outside = new DatabaseSync(path);
    outside.exec(`
      CREATE TRIGGER fail_effect BEFORE INSERT ON effects
      BEGIN SELECT RAISE(ABORT, 'the effect failed'); END
    `);

    expect(() => store.apply(KEY, '{"a":1}')).toThrow('the effect failed');
    const { records } = outside
      .prepare('SELECT count(*) AS records FROM idempotency_records')
      .get() as { records: number };
    expect(records).toBe(0);

    outside.exec('DROP TRIGGER fail_effect');
    outside.close();

    expect(store.apply(KEY, '{"a":1}').outcome).toBe('applied');
    expect(store.listEffects()).toHaveLength(1);
  });
});
