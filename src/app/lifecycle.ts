import type { Logger } from 'pino';

export interface ShutdownOptions {
  log: Logger;
  /** How long a shutdown may take. Must be shorter than the supervisor's grace period. */
  deadlineMs: number;
}

/**
 * Runs `close` on the first SIGINT or SIGTERM. A shutdown still running at the deadline
 * ends the process, and so does a second signal. Either is safe, because all state is
 * durable and what was cut short is replayed.
 */
export function closeOnSignal(
  { log, deadlineMs }: ShutdownOptions,
  close: () => Promise<void>,
): void {
  const shutdown = (signal: NodeJS.Signals): void => {
    process.removeListener('SIGINT', shutdown);
    process.removeListener('SIGTERM', shutdown);

    log.info({ signal }, 'shutting down');
    // Unreferenced, so that it never keeps alive a process that has finished.
    setTimeout(() => {
      log.error({ deadlineMs }, 'shutdown deadline passed; exiting');
      process.exit(1);
    }, deadlineMs).unref();

    close().catch((error: unknown) => {
      log.error({ err: error }, 'shutdown failed');
      process.exitCode = 1;
    });
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
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
