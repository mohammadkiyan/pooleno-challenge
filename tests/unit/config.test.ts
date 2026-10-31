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
    });
  });

  it('requires DATABASE_URL', () => {
    expect(() => loadMatchingConfig({})).toThrow('DATABASE_URL is required');
  });

  it('rejects a session timeout that the broker would refuse', () => {
    expect(() =>
      loadMatchingConfig({ DATABASE_URL, MATCHING_SESSION_TIMEOUT_MS: '5999' }),
    ).toThrow('MATCHING_SESSION_TIMEOUT_MS');
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
