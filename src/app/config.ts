export interface Config {
  databaseUrl: string;
  host: string;
  port: number;
  logLevel: string;
  lockTimeoutMs: number;
}

export interface KafkaConfig {
  brokers: string[];
  topic: string;
}

export interface PublisherConfig {
  databaseUrl: string;
  logLevel: string;
  kafka: KafkaConfig;
  pollIntervalMs: number;
  retryBaseMs: number;
  retryCapMs: number;
}

export interface MatchingConfig {
  databaseUrl: string;
  logLevel: string;
  kafka: KafkaConfig;
  groupId: string;
  sessionTimeoutMs: number;
  restartDelayMs: number;
}

export interface NotificationRetryPolicy {
  baseMs: number;
  capMs: number;
  jitterPercent: number;
  maxAttempts: number;
}

export interface ExternalConfig {
  databaseUrl: string;
  logLevel: string;
  kafka: KafkaConfig;
  groupId: string;
  sessionTimeoutMs: number;
  restartDelayMs: number;
  targetUrl: string;
  timeoutMs: number;
  pollIntervalMs: number;
  retry: NotificationRetryPolicy;
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  return {
    databaseUrl: readDatabaseUrl(env),
    host: env.HOST ?? '127.0.0.1',
    port: readInteger(env, 'PORT', { fallback: 8080, min: 0 }),
    logLevel: env.LOG_LEVEL ?? 'info',
    // PostgreSQL treats a lock_timeout of 0 as "wait forever", so the minimum is 1.
    lockTimeoutMs: readInteger(env, 'ACCEPTANCE_LOCK_TIMEOUT_MS', { fallback: 1_000, min: 1 }),
  };
}

export function loadKafkaConfig(env: NodeJS.ProcessEnv = process.env): KafkaConfig {
  return {
    brokers: (env.KAFKA_BROKERS ?? '127.0.0.1:59092').split(','),
    topic: env.KAFKA_TOPIC ?? 'orders.accepted',
  };
}

export function loadPublisherConfig(env: NodeJS.ProcessEnv = process.env): PublisherConfig {
  return {
    databaseUrl: readDatabaseUrl(env),
    logLevel: env.LOG_LEVEL ?? 'info',
    kafka: loadKafkaConfig(env),
    pollIntervalMs: readInteger(env, 'OUTBOX_POLL_INTERVAL_MS', { fallback: 200, min: 1 }),
    retryBaseMs: readInteger(env, 'OUTBOX_RETRY_BASE_MS', { fallback: 500, min: 1 }),
    retryCapMs: readInteger(env, 'OUTBOX_RETRY_CAP_MS', { fallback: 30_000, min: 1 }),
  };
}

export function loadMatchingConfig(env: NodeJS.ProcessEnv = process.env): MatchingConfig {
  return {
    databaseUrl: readDatabaseUrl(env),
    logLevel: env.LOG_LEVEL ?? 'info',
    kafka: loadKafkaConfig(env),
    groupId: env.MATCHING_GROUP_ID ?? 'matching-v1',
    // Kafka brokers refuse a session timeout below group.min.session.timeout.ms, 6000 by default.
    sessionTimeoutMs: readInteger(env, 'MATCHING_SESSION_TIMEOUT_MS', {
      fallback: 10_000,
      min: 6_000,
    }),
    restartDelayMs: readInteger(env, 'MATCHING_RESTART_DELAY_MS', { fallback: 1_000, min: 1 }),
  };
}

export function loadExternalConfig(env: NodeJS.ProcessEnv = process.env): ExternalConfig {
  return {
    databaseUrl: readDatabaseUrl(env),
    logLevel: env.LOG_LEVEL ?? 'info',
    kafka: loadKafkaConfig(env),
    groupId: env.EXTERNAL_GROUP_ID ?? 'external-notification-v1',
    sessionTimeoutMs: readInteger(env, 'EXTERNAL_SESSION_TIMEOUT_MS', {
      fallback: 10_000,
      min: 6_000,
    }),
    restartDelayMs: readInteger(env, 'EXTERNAL_RESTART_DELAY_MS', { fallback: 1_000, min: 1 }),
    targetUrl: readTargetUrl(env),
    timeoutMs: readInteger(env, 'EXTERNAL_TIMEOUT_MS', { fallback: 5_000, min: 1 }),
    pollIntervalMs: readInteger(env, 'EXTERNAL_POLL_INTERVAL_MS', { fallback: 500, min: 1 }),
    retry: {
      baseMs: readInteger(env, 'EXTERNAL_RETRY_BASE_MS', { fallback: 1_000, min: 1 }),
      capMs: readInteger(env, 'EXTERNAL_RETRY_CAP_MS', { fallback: 300_000, min: 1 }),
      jitterPercent: readInteger(env, 'EXTERNAL_RETRY_JITTER_PERCENT', {
        fallback: 50,
        min: 0,
        max: 100,
      }),
      maxAttempts: readInteger(env, 'EXTERNAL_MAX_ATTEMPTS', { fallback: 400, min: 1 }),
    },
  };
}

function readDatabaseUrl(env: NodeJS.ProcessEnv): string {
  const databaseUrl = env.DATABASE_URL;
  if (!databaseUrl) {
    throw new Error('DATABASE_URL is required');
  }
  return databaseUrl;
}

/** Checked at startup, because a bad URL would otherwise look like an outage for days. */
function readTargetUrl(env: NodeJS.ProcessEnv): string {
  const url = env.EXTERNAL_API_URL ?? 'http://127.0.0.1:58080/v1/notifications';
  if (!URL.canParse(url)) {
    // The value is left out, because a URL can carry credentials and this reaches the logs.
    throw new Error('EXTERNAL_API_URL must be an absolute URL');
  }
  return url;
}

function readInteger(
  env: NodeJS.ProcessEnv,
  name: string,
  { fallback, min, max = Infinity }: { fallback: number; min: number; max?: number },
): number {
  const raw = env[name];
  if (raw === undefined) {
    return fallback;
  }

  const value = Number(raw);
  if (!Number.isInteger(value) || value < min || value > max) {
    const range = max === Infinity ? `of at least ${min}` : `from ${min} to ${max}`;
    throw new Error(`${name} must be an integer ${range}, got "${raw}"`);
  }
  return value;
}
