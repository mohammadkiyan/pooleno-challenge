import { describe, expect, it } from 'vitest';
import {
  loadConfig,
  loadExternalConfig,
  loadKafkaConfig,
  loadMatchingConfig,
  loadPublisherConfig,
} from '../../src/app/config.js';

const DATABASE_URL = 'postgres://pooleno@127.0.0.1:55432/pooleno';

describe('loadConfig', () => {
  it('applies defaults', () => {
    expect(loadConfig({ DATABASE_URL })).toEqual({
      databaseUrl: DATABASE_URL,
      host: '127.0.0.1',
      port: 8080,
      logLevel: 'info',
      lockTimeoutMs: 1_000,
      shutdownTimeoutMs: 8_000,
      kafka: { brokers: ['127.0.0.1:59092'], topic: 'orders.accepted' },
      consumerGroups: ['matching-v1', 'external-notification-v1'],
    });
  });

  it('reports lag for the groups the roles actually use', () => {
    const config = loadConfig({
      DATABASE_URL,
      MATCHING_GROUP_ID: 'matching-b',
      EXTERNAL_GROUP_ID: 'external-b',
    });

    expect(config.consumerGroups).toEqual(['matching-b', 'external-b']);
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
    ['SHUTDOWN_TIMEOUT_MS', '0'],
  ])('rejects %s=%s', (name, value) => {
    expect(() => loadConfig({ DATABASE_URL, [name]: value })).toThrow(name);
  });
});

describe('loadKafkaConfig', () => {
  it('defaults to the local broker and the agreed topic', () => {
    expect(loadKafkaConfig({})).toEqual({
      brokers: ['127.0.0.1:59092'],
      topic: 'orders.accepted',
    });
  });

  it('accepts several brokers', () => {
    expect(loadKafkaConfig({ KAFKA_BROKERS: 'kafka-1:9092,kafka-2:9092' }).brokers).toEqual([
      'kafka-1:9092',
      'kafka-2:9092',
    ]);
  });
});

describe('loadPublisherConfig', () => {
  it('applies defaults', () => {
    expect(loadPublisherConfig({ DATABASE_URL })).toMatchObject({
      databaseUrl: DATABASE_URL,
      pollIntervalMs: 200,
      retryBaseMs: 500,
      retryCapMs: 30_000,
    });
  });

  it('requires DATABASE_URL', () => {
    expect(() => loadPublisherConfig({})).toThrow('DATABASE_URL is required');
  });

  it.each(['OUTBOX_POLL_INTERVAL_MS', 'OUTBOX_RETRY_BASE_MS', 'OUTBOX_RETRY_CAP_MS'])(
    'rejects %s=0',
    (name) => {
      expect(() => loadPublisherConfig({ DATABASE_URL, [name]: '0' })).toThrow(name);
    },
  );
});

describe('loadMatchingConfig', () => {
  it('applies defaults, including the agreed consumer group', () => {
    expect(loadMatchingConfig({ DATABASE_URL })).toMatchObject({
      databaseUrl: DATABASE_URL,
      groupId: 'matching-v1',
      sessionTimeoutMs: 10_000,
      restartDelayMs: 1_000,
      statementTimeoutMs: 60_000,
    });
  });

  it('requires DATABASE_URL', () => {
    expect(() => loadMatchingConfig({})).toThrow('DATABASE_URL is required');
  });

  it('reads the statement timeout', () => {
    const config = loadMatchingConfig({ DATABASE_URL, MATCHING_STATEMENT_TIMEOUT_MS: '120000' });

    expect(config.statementTimeoutMs).toBe(120_000);
  });

  // In PostgreSQL a statement_timeout of 0 means no limit.
  it.each(['0', '1.5'])('rejects a statement timeout of %s', (value) => {
    expect(() => loadMatchingConfig({ DATABASE_URL, MATCHING_STATEMENT_TIMEOUT_MS: value })).toThrow(
      'MATCHING_STATEMENT_TIMEOUT_MS',
    );
  });

  it('rejects a session timeout that the broker would refuse', () => {
    expect(() =>
      loadMatchingConfig({ DATABASE_URL, MATCHING_SESSION_TIMEOUT_MS: '5999' }),
    ).toThrow('MATCHING_SESSION_TIMEOUT_MS');
  });
});

describe.each([
  ['publisher', loadPublisherConfig, 8081],
  ['matching', loadMatchingConfig, 8082],
  ['external', loadExternalConfig, 8083],
])('lifecycle settings of the %s role', (_role, load, healthPort) => {
  it('stays on loopback and shuts down within the grace period by default', () => {
    expect(load({ DATABASE_URL })).toMatchObject({
      shutdownTimeoutMs: 8_000,
      health: { host: '127.0.0.1', port: healthPort },
    });
  });

  it('allows an ephemeral health port', () => {
    expect(load({ DATABASE_URL, HEALTH_PORT: '0' }).health.port).toBe(0);
  });

  it.each([
    ['HEALTH_PORT', '-1'],
    ['HEALTH_PORT', '65536'],
    ['SHUTDOWN_TIMEOUT_MS', '0'],
  ])('rejects %s=%s', (name, value) => {
    expect(() => load({ DATABASE_URL, [name]: value })).toThrow(name);
  });
});

describe('loadExternalConfig', () => {
  it('applies the documented defaults, including the agreed consumer group', () => {
    expect(loadExternalConfig({ DATABASE_URL })).toMatchObject({
      databaseUrl: DATABASE_URL,
      groupId: 'external-notification-v1',
      sessionTimeoutMs: 10_000,
      restartDelayMs: 1_000,
      targetUrl: 'http://127.0.0.1:58080/v1/notifications',
      timeoutMs: 5_000,
      pollIntervalMs: 500,
      retry: { baseMs: 1_000, capMs: 300_000, jitterPercent: 50, maxAttempts: 400 },
    });
  });

  it('requires DATABASE_URL', () => {
    expect(() => loadExternalConfig({})).toThrow('DATABASE_URL is required');
  });

  it.each([
    ['EXTERNAL_API_URL', 'not a url'],
    ['EXTERNAL_SESSION_TIMEOUT_MS', '5999'],
    ['EXTERNAL_RESTART_DELAY_MS', '0'],
    ['EXTERNAL_TIMEOUT_MS', '0'],
    ['EXTERNAL_POLL_INTERVAL_MS', '0'],
    ['EXTERNAL_RETRY_BASE_MS', '0'],
    ['EXTERNAL_RETRY_CAP_MS', '0'],
    ['EXTERNAL_RETRY_JITTER_PERCENT', '-1'],
    ['EXTERNAL_RETRY_JITTER_PERCENT', '101'],
    ['EXTERNAL_MAX_ATTEMPTS', '0'],
  ])('rejects %s=%s', (name, value) => {
    expect(() => loadExternalConfig({ DATABASE_URL, [name]: value })).toThrow(name);
  });

  it.each(['0', '100'])('accepts a jitter of %s percent', (value) => {
    const config = loadExternalConfig({ DATABASE_URL, EXTERNAL_RETRY_JITTER_PERCENT: value });

    expect(config.retry.jitterPercent).toBe(Number(value));
  });
});
