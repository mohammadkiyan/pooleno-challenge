import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process';
import { once } from 'node:events';
import { vi } from 'vitest';
import { TEST_DATABASE_URL } from './test-database.js';

export interface ExitStatus {
  code: number | null;
  signal: NodeJS.Signals | null;
}

export const CLEAN_EXIT: ExitStatus = { code: 0, signal: null };

// Allows for a process starting up and then waiting out a dead member's Kafka session.
const WAIT_MS = 45_000;

const ENTRYPOINTS = {
  api: 'src/entrypoints/api.ts',
  publisher: 'src/entrypoints/publisher.ts',
  matching: 'src/entrypoints/matching.ts',
  external: 'src/entrypoints/external.ts',
  'redrive-external': 'src/entrypoints/redrive-external.ts',
  'mock-api': 'mock-api/server.ts',
};

type Entrypoint = keyof typeof ENTRYPOINTS;

const running = new Set<RoleProcess>();

/** A real entrypoint running as its own process, so that it can really crash. */
export class RoleProcess {
  readonly exited: Promise<ExitStatus>;
  readonly #child: ChildProcessWithoutNullStreams;
  #output = '';

  constructor(
    entrypoint: Entrypoint,
    env: NodeJS.ProcessEnv,
    preload?: string,
    args: string[] = [],
  ) {
    const preloadArguments = preload ? ['--import', preload] : [];
    this.#child = spawn(
      process.execPath,
      ['--import', 'tsx', ...preloadArguments, ENTRYPOINTS[entrypoint], ...args],
      // Any free port, because several roles can run at once on one machine here.
      { env: { ...process.env, DATABASE_URL: TEST_DATABASE_URL, HEALTH_PORT: '0', ...env } },
    );

    const record = (chunk: Buffer): void => {
      this.#output += chunk.toString();
    };
    this.#child.stdout.on('data', record);
    this.#child.stderr.on('data', record);

    this.exited = once(this.#child, 'exit').then(([code, signal]) => {
      running.delete(this);
      return { code, signal };
    });
    running.add(this);
  }

  countLogs(message: string): number {
    return this.#output.split(`"msg":"${message}"`).length - 1;
  }

  logsContain(text: string): boolean {
    return this.#output.includes(text);
  }

  /** Resolves once the process has logged the message, without guessing with a delay. */
  async waitForLog(message: string, occurrences = 1): Promise<void> {
    await this.#waitForOutput(() => this.countLogs(message) >= occurrences, `"${message}"`);
  }

  async waitForMatch(pattern: RegExp): Promise<RegExpExecArray> {
    await this.#waitForOutput(() => pattern.test(this.#output), String(pattern));
    return pattern.exec(this.#output) as RegExpExecArray;
  }

  /** SIGKILL gives the process no chance to flush or clean up, as in a real crash. */
  kill(): Promise<ExitStatus> {
    this.#child.kill('SIGKILL');
    return this.exited;
  }

  /** Asks for the graceful shutdown that a supervisor would, and reports how it ended. */
  terminate(): Promise<ExitStatus> {
    this.#child.kill('SIGTERM');
    return this.exited;
  }

  async #waitForOutput(isPresent: () => boolean, description: string): Promise<void> {
    await vi.waitFor(
      () => {
        if (!isPresent()) {
          throw new Error(`Still waiting for ${description}. Output so far:\n${this.#output}`);
        }
      },
      { timeout: WAIT_MS, interval: 25 },
    );
  }
}

/** Where a role's own health server ended up, since the tests ask for any free port. */
export async function healthUrlOf(role: RoleProcess): Promise<string> {
  const [, port] = await role.waitForMatch(/"port":(\d+)[^\n]*"msg":"health server listening"/);
  return `http://127.0.0.1:${port as string}`;
}

export async function readHealth(
  baseUrl: string,
  path: '/health/live' | '/health/ready',
): Promise<{ status: number; body: { status?: string; reason?: string } }> {
  const response = await fetch(`${baseUrl}${path}`, { headers: { connection: 'close' } });
  return {
    status: response.status,
    body: (await response.json()) as { status?: string; reason?: string },
  };
}

export async function killAllRoles(): Promise<void> {
  await Promise.all([...running].map((role) => role.kill()));
}

export interface RunningApi {
  url: string;
  role: RoleProcess;
}

export async function startApi(preload?: string): Promise<RunningApi> {
  const role = new RoleProcess(
    'api',
    { HOST: '127.0.0.1', PORT: '0', ACCEPTANCE_LOCK_TIMEOUT_MS: '30000' },
    preload,
  );
  const [, url] = await role.waitForMatch(/Server listening at (http:\/\/[^"]+)"/);
  return { url: url as string, role };
}

/**
 * Node.js 26.7's `fetch` can stall for longer than a test may take when it reuses an idle
 * connection, so each request opens its own. See docs/adr/0002-messaging-and-matching.md.
 */
export function requestApi(
  api: RunningApi,
  path: string,
  init: RequestInit = {},
): Promise<Response> {
  const headers = new Headers(init.headers);
  headers.set('connection', 'close');
  return fetch(new URL(path, api.url), { ...init, headers });
}
