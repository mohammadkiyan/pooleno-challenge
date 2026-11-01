// What the walkthrough scripts share: calling the running stack, and checking what it answers.
import { setTimeout as pause } from 'node:timers/promises';

const API_URL = process.env.API_URL ?? 'http://127.0.0.1:8080';
const MOCK_API_URL = process.env.MOCK_API_URL ?? 'http://127.0.0.1:18080';
const WAIT_ATTEMPTS = 120;
const WAIT_INTERVAL_MS = 500;

export interface OrderTrade {
  tradeId: string;
  role: string;
  price: string;
  quantity: string;
}

export interface OrderState {
  status: string;
  remainingQuantity: string;
  trades: OrderTrade[];
}

export interface Effect {
  idempotencyKey: string;
  body: string;
}

export interface Reply {
  status: number;
  body: Record<string, unknown>;
}

// Node.js 26.7's fetch can stall for many seconds when it reuses an idle connection, so each
// request opens its own. See docs/adr/0002-messaging-and-matching.md.
function request(target: string, init: RequestInit = {}): Promise<Response> {
  const headers = new Headers(init.headers);
  headers.set('connection', 'close');
  return fetch(new URL(target, API_URL), { ...init, headers });
}

export async function send(path: string, init: RequestInit = {}): Promise<Reply> {
  const response = await request(path, init);
  return { status: response.status, body: (await response.json()) as Record<string, unknown> };
}

export function postOrder(key: string, order: object): Promise<Reply> {
  return send('/v1/orders', {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'idempotency-key': key },
    body: JSON.stringify(order),
  });
}

export async function readOrder(orderId: string): Promise<OrderState> {
  const { body } = await send(`/v1/orders/${orderId}`);
  return body as unknown as OrderState;
}

export async function readAllEffects(): Promise<Effect[]> {
  const response = await request(`${MOCK_API_URL}/__control/effects`);
  const { effects } = (await response.json()) as { effects: Effect[] };
  return effects;
}

export async function readEffects(orderId: string): Promise<Effect[]> {
  const effects = await readAllEffects();
  return effects.filter(({ body }) => (JSON.parse(body) as { orderId: string }).orderId === orderId);
}

/** Polls until `read` returns a value, because each step after acceptance is asynchronous. */
export async function waitFor<T>(failure: string, read: () => Promise<T | undefined>): Promise<T> {
  for (let attempt = 0; attempt < WAIT_ATTEMPTS; attempt += 1) {
    const value = await read();
    if (value !== undefined) {
      return value;
    }
    await pause(WAIT_INTERVAL_MS);
  }
  throw new Error(failure);
}

export function waitForStatus(orderId: string, status: string): Promise<OrderState> {
  return waitFor(
    `Order ${orderId} did not reach ${status}. Are the publisher and matching processes running?`,
    async () => {
      const state = await readOrder(orderId);
      return state.status === status ? state : undefined;
    },
  );
}

/** Prints what was proven, or stops the script with what was expected and what came back. */
export function expectEqual(claim: string, actual: unknown, expected: unknown): void {
  const got = JSON.stringify(actual);
  const wanted = JSON.stringify(expected);
  if (got !== wanted) {
    throw new Error(`${claim}: expected ${wanted}, got ${got}`);
  }
  console.log(`  ok  ${claim}`);
}
