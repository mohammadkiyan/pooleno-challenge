import { describe, expect, it } from 'vitest';
import { loadConfig } from '../../src/app/config.js';

const DATABASE_URL = 'postgres://pooleno@127.0.0.1:55432/pooleno';

describe('loadConfig', () => {
  it('applies defaults', () => {
    expect(loadConfig({ DATABASE_URL })).toEqual({
      databaseUrl: DATABASE_URL,
      host: '127.0.0.1',
      port: 8080,
      logLevel: 'info',
      lockTimeoutMs: 1_000,
    });
  });

  it('requires DATABASE_URL', () => {
    expect(() => loadConfig({})).toThrow('DATABASE_URL is required');
  });

  it('allows an ephemeral port', () => {
    expect(loadConfig({ DATABASE_URL, PORT: '0' }).port).toBe(0);
  });

  it.each([
    ['PORT', 'http'],
    ['PORT', '-1'],
    ['ACCEPTANCE_LOCK_TIMEOUT_MS', '1.5'],
    // In PostgreSQL a lock_timeout of 0 means no limit, which would unbound the wait.
    ['ACCEPTANCE_LOCK_TIMEOUT_MS', '0'],
  ])('rejects %s=%s', (name, value) => {
    expect(() => loadConfig({ DATABASE_URL, [name]: value })).toThrow(name);
  });
});
