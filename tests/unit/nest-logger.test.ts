import { pino } from 'pino';
import { describe, expect, it } from 'vitest';
import { NestLogger } from '../../src/app/nest-logger.js';

function recordingLogger() {
  const lines: Record<string, unknown>[] = [];
  const logger = new NestLogger(
    pino(
      { level: 'trace' },
      {
        write: (line: string) => {
          lines.push(JSON.parse(line) as Record<string, unknown>);
        },
      },
    ),
  );
  return { logger, lines };
}

describe('NestLogger', () => {
  it('keeps the fields of an object, rather than logging [object Object]', () => {
    const { logger, lines } = recordingLogger();

    logger.warn({ reason: 'slow', count: 2 }, 'NestApplication');

    expect(lines).toEqual([
      expect.objectContaining({
        msg: 'nest message',
        nest: [{ reason: 'slow', count: 2 }, 'NestApplication'],
      }),
    ]);
  });

  it('logs a string as the message, with its details alongside', () => {
    const { logger, lines } = recordingLogger();

    logger.error('it failed', 'stack', 'NestApplication');

    expect(lines).toEqual([
      expect.objectContaining({ msg: 'it failed', nest: ['stack', 'NestApplication'] }),
    ]);
  });

  it('logs an error with its stack', () => {
    const { logger, lines } = recordingLogger();

    logger.error(new Error('it failed'));

    expect(lines[0]).toMatchObject({ msg: 'it failed', err: { message: 'it failed' } });
  });

  it('logs null as text, because it has no fields to keep', () => {
    const { logger, lines } = recordingLogger();

    logger.warn(null);

    expect(lines[0]).toMatchObject({ msg: 'null' });
  });

  it("moves Nest's start-up messages down to debug", () => {
    const { logger, lines } = recordingLogger();

    logger.log('Mapped {/v1/orders, POST} route');

    expect(lines[0]).toMatchObject({ level: 20 });
  });
});
