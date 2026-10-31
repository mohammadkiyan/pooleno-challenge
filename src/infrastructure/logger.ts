import { pino, type Logger } from 'pino';

export function createLogger(role: string, level: string): Logger {
  return pino({ level, base: { role } });
}
