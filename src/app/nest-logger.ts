import type { LoggerService } from '@nestjs/common';
import type { Logger } from 'pino';

type Level = 'trace' | 'debug' | 'info' | 'warn' | 'error' | 'fatal';

/**
 * Keeps what Nest logs in the process's JSON stream. Nest reports its start-up, such as every
 * route it maps, at `log`. That says nothing the process's own logs don't, so it becomes debug.
 */
export class NestLogger implements LoggerService {
  constructor(private readonly logger: Logger) {}

  log(message: unknown, ...details: unknown[]): void {
    this.write('debug', message, details);
  }
  error(message: unknown, ...details: unknown[]): void {
    this.write('error', message, details);
  }
  warn(message: unknown, ...details: unknown[]): void {
    this.write('warn', message, details);
  }
  debug(message: unknown, ...details: unknown[]): void {
    this.write('debug', message, details);
  }
  verbose(message: unknown, ...details: unknown[]): void {
    this.write('trace', message, details);
  }
  fatal(message: unknown, ...details: unknown[]): void {
    this.write('fatal', message, details);
  }

  private write(level: Level, message: unknown, details: unknown[]): void {
    if (message instanceof Error) {
      this.logger[level]({ err: message, nest: details }, message.message);
    } else {
      this.logger[level]({ nest: details }, String(message));
    }
  }
}
