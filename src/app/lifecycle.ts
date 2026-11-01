import type { Logger } from 'pino';

export interface ShutdownOptions {
  log: Logger;
  /** How long a shutdown may take. Must be shorter than the supervisor's grace period. */
  deadlineMs: number;
}

/**
 * Ends the process at the deadline if it is still running then. Unreferenced, so that it never
 * keeps alive a process that has finished, though it still fires while something else does,
 * such as a socket that has gone quiet.
 */
export function exitAtDeadline({ log, deadlineMs }: ShutdownOptions): void {
  setTimeout(() => {
    log.error({ deadlineMs }, 'shutdown deadline passed; exiting');
    process.exit(1);
  }, deadlineMs).unref();
}

/**
 * Runs `close` on the first SIGINT or SIGTERM. A shutdown still running at the deadline
 * ends the process, and so does a second signal. Either is safe, because all state is
 * durable and what was cut short is replayed.
 */
export function closeOnSignal(options: ShutdownOptions, close: () => Promise<void>): void {
  const { log } = options;
  const shutdown = (signal: NodeJS.Signals): void => {
    process.removeListener('SIGINT', shutdown);
    process.removeListener('SIGTERM', shutdown);

    log.info({ signal }, 'shutting down');
    exitAtDeadline(options);

    close().catch((error: unknown) => {
      log.error({ err: error }, 'shutdown failed');
      process.exitCode = 1;
    });
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

/**
 * For a Nest shutdown hook. Nest logs what a hook rejects with and carries on, so `app.close()`
 * still resolves. The exit code is the only way left for the failure to reach the supervisor.
 */
export async function closeOrFail(log: Logger, close: () => Promise<void>): Promise<void> {
  try {
    await close();
  } catch (error) {
    log.error({ err: error }, 'shutdown failed');
    process.exitCode = 1;
  }
}

/** The same contract for a role that drains a loop: the signal aborts on SIGINT or SIGTERM. */
export function shutdownSignal(options: ShutdownOptions): AbortSignal {
  const shutdown = new AbortController();
  closeOnSignal(options, () => {
    shutdown.abort();
    return Promise.resolve();
  });
  return shutdown.signal;
}
