import { createServer } from 'node:net';
import { once } from 'node:events';
import type { Effect } from '../../mock-api/idempotency-store.js';
import { RoleProcess } from './processes.js';

export interface MockApiAddress {
  port: number;
  databasePath: string;
  notificationsUrl: string;
}

export interface Fault {
  mode: 'lost-response' | 'timeout-after-commit' | 'timeout' | 'status';
  status?: number;
  retryAfter?: string;
  times?: number;
}

/**
 * Reserves a port that is free now. A test needs to know it before the mock runs, because the
 * worker is pointed at it while the mock is down, and the mock returns to it after a restart.
 */
export async function freePort(): Promise<number> {
  const server = createServer();
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const { port } = server.address() as { port: number };
  server.close();
  await once(server, 'close');
  return port;
}

export async function mockApiAddress(databasePath: string): Promise<MockApiAddress> {
  const port = await freePort();
  return { port, databasePath, notificationsUrl: `http://127.0.0.1:${port}/v1/notifications` };
}

export async function startMockApi({ port, databasePath }: MockApiAddress): Promise<RoleProcess> {
  const role = new RoleProcess('mock-api', {
    MOCK_API_HOST: '127.0.0.1',
    MOCK_API_PORT: String(port),
    MOCK_API_DATABASE: databasePath,
  });
  await role.waitForMatch(/Server listening at /);
  return role;
}

// Each request opens its own connection, as in tests/support/processes.ts.
function requestMock({ port }: MockApiAddress, path: string, init: RequestInit = {}) {
  const headers = new Headers(init.headers);
  headers.set('connection', 'close');
  return fetch(`http://127.0.0.1:${port}${path}`, { ...init, headers });
}

export async function injectFault(address: MockApiAddress, fault: Fault): Promise<void> {
  const response = await requestMock(address, '/__control/faults', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(fault),
  });
  if (response.status !== 204) {
    throw new Error(`Could not inject the fault: HTTP ${response.status}`);
  }
}

export async function clearFaults(address: MockApiAddress): Promise<void> {
  await requestMock(address, '/__control/faults', { method: 'DELETE' });
}

export async function readEffects(address: MockApiAddress): Promise<Effect[]> {
  const response = await requestMock(address, '/__control/effects');
  return ((await response.json()) as { effects: Effect[] }).effects;
}

export function postNotification(
  address: MockApiAddress,
  body: string,
  headers: Record<string, string> = { 'idempotency-key': 'external-notify:test' },
): Promise<Response> {
  return requestMock(address, '/v1/notifications', {
    method: 'POST',
    headers: { 'content-type': 'application/json', ...headers },
    body,
  });
}
