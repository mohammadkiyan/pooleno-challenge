export interface Config {
  databaseUrl: string;
  host: string;
  port: number;
  logLevel: string;
  lockTimeoutMs: number;
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const databaseUrl = env.DATABASE_URL;
  if (!databaseUrl) {
    throw new Error('DATABASE_URL is required');
  }

  return {
    databaseUrl,
    host: env.HOST ?? '127.0.0.1',
    port: readInteger(env, 'PORT', { fallback: 8080, min: 0 }),
    logLevel: env.LOG_LEVEL ?? 'info',
    // PostgreSQL treats a lock_timeout of 0 as "wait forever", so the minimum is 1.
    lockTimeoutMs: readInteger(env, 'ACCEPTANCE_LOCK_TIMEOUT_MS', { fallback: 1_000, min: 1 }),
  };
}

function readInteger(
  env: NodeJS.ProcessEnv,
  name: string,
  { fallback, min }: { fallback: number; min: number },
): number {
  const raw = env[name];
  if (raw === undefined) {
    return fallback;
  }

  const value = Number(raw);
  if (!Number.isInteger(value) || value < min) {
    throw new Error(`${name} must be an integer of at least ${min}, got "${raw}"`);
  }
  return value;
}
