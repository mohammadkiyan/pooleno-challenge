import { once } from 'node:events';
import { createServer, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import type { Logger } from 'pino';

/** Resolves to why the role cannot do its work now, or to undefined when it can. */
export type ReadinessCheck = () => Promise<string | undefined>;

export interface HealthAddress {
  host: string;
  port: number;
}

/** Shorter than the three-second limit that Compose gives the health check. */
const CHECK_TIMEOUT_MS = 2_000;

/**
 * Serves `/health/live` and `/health/ready` for a role that has no HTTP API of its own.
 * Liveness answers without looking at any dependency, so a slow dependency can never get a
 * running process restarted. Answers carry only the fixed reasons that the checks return,
 * never error text, because this port has no authentication.
 */
export async function startHealthServer(
  address: HealthAddress,
  isReady: ReadinessCheck,
  log: Logger,
): Promise<Server> {
  const server = createServer((request, response) => {
    void answer(request.url, isReady).then(({ status, body }) => {
      response.writeHead(status, { 'content-type': 'application/json', connection: 'close' });
      response.end(JSON.stringify(body));
    });
  });

  server.listen(address.port, address.host);
  await once(server, 'listening');
  log.info({ port: (server.address() as AddressInfo).port }, 'health server listening');
  return server;
}

async function answer(
  url: string | undefined,
  isReady: ReadinessCheck,
): Promise<{ status: number; body: object }> {
  switch (url) {
    case '/health/live':
      return { status: 200, body: { status: 'ok' } };
    case '/health/ready': {
      const reason = await checkWithin(isReady, CHECK_TIMEOUT_MS);
      return reason === undefined
        ? { status: 200, body: { status: 'ready' } }
        : { status: 503, body: { status: 'not_ready', reason } };
    }
    default:
      return { status: 404, body: { status: 'not_found' } };
  }
}

async function checkWithin(isReady: ReadinessCheck, timeoutMs: number): Promise<string | undefined> {
  let timer: NodeJS.Timeout | undefined;
  const timedOut = new Promise<string>((resolve) => {
    timer = setTimeout(() => {
      resolve('readiness check timed out');
    }, timeoutMs);
  });

  try {
    return await Promise.race([isReady().catch(() => 'readiness check failed'), timedOut]);
  } finally {
    clearTimeout(timer);
  }
}

export function closeHealthServer(server: Server): Promise<void> {
  return new Promise((resolve) => {
    server.close(() => {
      resolve();
    });
    // Every answer closes its connection, so only a stalled client can still hold one.
    server.closeAllConnections();
  });
}
