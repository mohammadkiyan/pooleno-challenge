import { once } from 'node:events';
import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import type { AddressInfo } from 'node:net';
import { afterEach, describe, expect, it } from 'vitest';
import {
  deliverNotification,
  parseRetryAfter,
} from '../../src/modules/external/external-client.js';
import { computeRetryDelayMs } from '../../src/modules/external/notification-worker.js';

const JOB = { targetKey: 'external-notify:event-1', payload: '{"eventId":"event-1"}' };
const POLICY = { baseMs: 1_000, capMs: 8_000, jitterPercent: 0, maxAttempts: 10 };

describe('computeRetryDelayMs', () => {
  it.each([
    [1, 1_000],
    [2, 2_000],
    [3, 4_000],
    [4, 8_000],
    [5, 8_000],
    [1_000, 8_000],
  ])('after %i attempts waits %i ms', (attempts, expected) => {
    expect(computeRetryDelayMs(attempts, POLICY)).toBe(expected);
  });

  it('removes at most the jitter share, at random', () => {
    const policy = { ...POLICY, jitterPercent: 50 };

    expect(computeRetryDelayMs(2, policy, 0, () => 0)).toBe(2_000);
    expect(computeRetryDelayMs(2, policy, 0, () => 0.5)).toBe(1_500);
    expect(computeRetryDelayMs(2, policy, 0, () => 1)).toBe(1_000);
  });

  it('never goes above the cap, however much jitter is left out', () => {
    const policy = { ...POLICY, jitterPercent: 100 };

    expect(computeRetryDelayMs(50, policy, 0, () => 0)).toBe(8_000);
    expect(computeRetryDelayMs(50, policy, 0, () => 1)).toBe(0);
  });

  it('treats Retry-After as a minimum', () => {
    expect(computeRetryDelayMs(1, POLICY, 3_000)).toBe(3_000);
    expect(computeRetryDelayMs(3, POLICY, 1_000)).toBe(4_000);
  });

  it('applies Retry-After after the jitter, so the jitter cannot undercut it', () => {
    const policy = { ...POLICY, jitterPercent: 100 };

    expect(computeRetryDelayMs(1, policy, 3_000, () => 1)).toBe(3_000);
  });

  it('honours Retry-After only up to the cap', () => {
    expect(computeRetryDelayMs(1, POLICY, 3_600_000)).toBe(8_000);
  });
});

describe('parseRetryAfter', () => {
  const NOW = Date.parse('2026-10-31T12:00:00Z');

  it.each([
    ['seconds', '7', 7_000],
    ['zero seconds', '0', 0],
    ['a date ahead', 'Sat, 31 Oct 2026 12:00:30 GMT', 30_000],
    ['a date passed', 'Sat, 31 Oct 2026 11:00:00 GMT', 0],
  ])('reads %s', (_name, value, expected) => {
    expect(parseRetryAfter(value, NOW)).toBe(expected);
  });

  it.each([
    ['absent', null],
    ['nonsense', 'soon'],
    ['empty', ''],
  ])('ignores a value that is %s', (_name, value) => {
    expect(parseRetryAfter(value, NOW)).toBeUndefined();
  });
});

describe('deliverNotification', () => {
  let server: Server | undefined;

  afterEach(async () => {
    if (server) {
      server.closeAllConnections();
      server.close();
      await once(server, 'close');
      server = undefined;
    }
  });

  async function serve(
    handler: (request: IncomingMessage, response: ServerResponse) => void,
  ): Promise<string> {
    server = createServer(handler);
    server.listen(0, '127.0.0.1');
    await once(server, 'listening');
    return `http://127.0.0.1:${(server.address() as AddressInfo).port}/v1/notifications`;
  }

  function answerWith(status: number, body = '', headers: Record<string, string> = {}) {
    return serve((_request, response) => {
      response.writeHead(status, headers).end(body);
    });
  }

  it('sends the original body with the stable key', async () => {
    let received: { method?: string; key?: string; type?: string; body: string } | undefined;
    const url = await serve((request, response) => {
      let body = '';
      request.on('data', (chunk: Buffer) => (body += chunk.toString()));
      request.on('end', () => {
        const key = request.headers['idempotency-key'] as string;
        const type = request.headers['content-type'] as string;
        received = { method: request.method as string, key, type, body };
        response.writeHead(201).end();
      });
    });

    const outcome = await deliverNotification({ url, timeoutMs: 2_000 }, JOB);

    expect(outcome).toEqual({ kind: 'delivered' });
    expect(received).toEqual({
      method: 'POST',
      key: JOB.targetKey,
      type: 'application/json',
      body: JOB.payload,
    });
  });

  it.each([408, 409, 425, 429, 500, 502, 503, 504])('retries HTTP %i', async (status) => {
    const url = await answerWith(status, 'try later');

    await expect(deliverNotification({ url, timeoutMs: 2_000 }, JOB)).resolves.toEqual({
      kind: 'transient',
      error: `HTTP ${status}: try later`,
      retryAfterMs: undefined,
    });
  });

  it.each([301, 400, 401, 403, 404, 410, 422, 501])('does not retry HTTP %i', async (status) => {
    const url = await answerWith(status, 'no');

    await expect(deliverNotification({ url, timeoutMs: 2_000 }, JOB)).resolves.toEqual({
      kind: 'permanent',
      error: `HTTP ${status}: no`,
    });
  });

  it('reports a redirect as permanent rather than following it', async () => {
    const requests: (string | undefined)[] = [];
    const url = await serve((request, response) => {
      requests.push(request.url);
      response.writeHead(302, { location: '/elsewhere' }).end('moved');
    });

    await expect(deliverNotification({ url, timeoutMs: 2_000 }, JOB)).resolves.toEqual({
      kind: 'permanent',
      error: 'HTTP 302: moved',
    });
    expect(requests).toEqual(['/v1/notifications']);
  });

  it('reads no more of an error body than it keeps, however much is sent', async () => {
    let stoppedReading = false;
    const url = await serve((_request, response) => {
      response.writeHead(500);
      const timer = setInterval(() => response.write('x'.repeat(1_024)), 1);
      response.on('close', () => {
        clearInterval(timer);
        stoppedReading = true;
      });
    });

    const outcome = await deliverNotification({ url, timeoutMs: 2_000 }, JOB);

    expect(outcome).toEqual({
      kind: 'transient',
      error: `HTTP 500: ${'x'.repeat(500)}`,
      retryAfterMs: undefined,
    });
    await expect.poll(() => stoppedReading).toBe(true);
  });

  it('keeps the status of an error whose body is cut off', async () => {
    const url = await serve((_request, response) => {
      response.writeHead(503, { 'content-length': '100' });
      response.write('partial', () => response.destroy());
    });

    await expect(deliverNotification({ url, timeoutMs: 2_000 }, JOB)).resolves.toEqual({
      kind: 'transient',
      error: 'HTTP 503: partial',
      retryAfterMs: undefined,
    });
  });

  it('passes on Retry-After with a transient status', async () => {
    const url = await answerWith(503, '', { 'retry-after': '3' });

    await expect(deliverNotification({ url, timeoutMs: 2_000 }, JOB)).resolves.toMatchObject({
      kind: 'transient',
      retryAfterMs: 3_000,
    });
  });

  it('treats a refused connection as transient, and names the reason', async () => {
    const url = await answerWith(200);
    server?.close();
    server?.closeAllConnections();
    await once(server as Server, 'close');
    server = undefined;

    const outcome = await deliverNotification({ url, timeoutMs: 2_000 }, JOB);

    expect(outcome).toMatchObject({ kind: 'transient', error: expect.stringContaining('ECONN') });
  });

  it('gives up at the timeout when no response comes, as transient', async () => {
    const url = await serve(() => undefined);

    const started = Date.now();
    const outcome = await deliverNotification({ url, timeoutMs: 100 }, JOB);

    expect(outcome).toMatchObject({ kind: 'transient', error: expect.stringContaining('Timeout') });
    expect(Date.now() - started).toBeLessThan(2_000);
  });

  it('treats a connection reset before any response as transient', async () => {
    const url = await serve((request) => {
      request.socket.destroy();
    });

    await expect(deliverNotification({ url, timeoutMs: 2_000 }, JOB)).resolves.toMatchObject({
      kind: 'transient',
    });
  });
});
