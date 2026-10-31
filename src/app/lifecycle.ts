import type { Logger } from 'pino';

/**
 * Runs `close` on SIGINT or SIGTERM. Repeating the signal terminates the process
 * at once, which is safe because all state is durable.
 */
export function closeOnSignal(log: Logger, close: () => Promise<void>): void {
  const shutdown = (signal: NodeJS.Signals): void => {
    log.info({ signal }, 'shutting down');
    close().catch((error: unknown) => {
      log.error({ err: error }, 'shutdown failed');
      process.exitCode = 1;
    });
  };

  process.once('SIGINT', shutdown);
  process.once('SIGTERM', shutdown);
}
