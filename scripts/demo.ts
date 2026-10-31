// Walks through the acceptance contract, the agreed matching fixture and the external
// notification against a running stack. Keys are fixed, so a rerun replays the same orders,
// trades nothing new and causes no second effect. Restart the stack between runs to see the
// results survive.
import { setTimeout as pause } from 'node:timers/promises';

const API_URL = process.env.API_URL ?? 'http://127.0.0.1:8080';
const MOCK_API_URL = process.env.MOCK_API_URL ?? 'http://127.0.0.1:58080';
const DEMO_KEY = 'demo-order-key-0001';
const WAIT_ATTEMPTS = 60;
const WAIT_INTERVAL_MS = 500;

const order = { symbol: 'BTC-USD', side: 'BUY', type: 'LIMIT', price: '100.00', quantity: '2.500' };

interface OrderState {
  status: string;
  remainingQuantity: string;
  trades: { role: string; price: string; quantity: string }[];
}

interface Effect {
  idempotencyKey: string;
  body: string;
}

// Node.js 26.7's fetch can stall for many seconds when it reuses an idle connection, so each
// request opens its own. See docs/adr/0002-phase-2-messaging-and-matching.md.
function request(target: string, init: RequestInit = {}): Promise<Response> {
  const headers = new Headers(init.headers);
  headers.set('connection', 'close');
  return fetch(new URL(target, API_URL), { ...init, headers });
}

async function send(title: string, method: string, path: string, init: RequestInit = {}) {
  const response = await request(path, { method, ...init });
  const body = (await response.json()) as Record<string, unknown>;

  console.log(`\n${title}\n  ${method} ${path} -> ${response.status}\n  ${JSON.stringify(body)}`);
  return body;
}

function postOrder(title: string, key: string, payload: object) {
  return send(title, 'POST', '/v1/orders', {
    headers: { 'content-type': 'application/json', 'idempotency-key': key },
    body: JSON.stringify(payload),
  });
}

async function placeOnEth(name: string, side: string, quantity: string, price: string) {
  const { orderId } = await postOrder(
    `${name}: ${side} ${quantity} @ ${price}`,
    `demo-fixture-${name}-0001`,
    { symbol: 'ETH-USD', side, type: 'LIMIT', price, quantity },
  );
  return String(orderId);
}

async function readOrder(orderId: string): Promise<OrderState> {
  const response = await request(`/v1/orders/${orderId}`);
  return (await response.json()) as OrderState;
}

async function readEffects(orderId: string): Promise<Effect[]> {
  const response = await request(`${MOCK_API_URL}/__control/effects`);
  const { effects } = (await response.json()) as { effects: Effect[] };
  return effects.filter(({ body }) => (JSON.parse(body) as { orderId: string }).orderId === orderId);
}

async function waitFor<T>(failure: string, read: () => Promise<T | undefined>): Promise<T> {
  for (let attempt = 0; attempt < WAIT_ATTEMPTS; attempt += 1) {
    const value = await read();
    if (value !== undefined) {
      return value;
    }
    await pause(WAIT_INTERVAL_MS);
  }
  throw new Error(failure);
}

const { orderId } = await postOrder('Accept an order', DEMO_KEY, order);

await postOrder('Retry with an equivalent spelling: same receipt', DEMO_KEY, {
  quantity: '2.5',
  price: '100',
  type: 'LIMIT',
  side: 'BUY',
  symbol: 'BTC-USD',
});

await postOrder('Reuse the key for a different order: rejected', DEMO_KEY, {
  ...order,
  price: '101.00',
});

await postOrder('Use more precision than the market allows: rejected', 'demo-order-key-0002', {
  ...order,
  price: '100.001',
});

await send('Read the order', 'GET', `/v1/orders/${String(orderId)}`);

console.log('\n--- Matching fixture on ETH-USD ---');

await placeOnEth('S1', 'SELL', '2', '100');
const s2 = await placeOnEth('S2', 'SELL', '3', '100');
await placeOnEth('S3', 'SELL', '1', '99');
const b1 = await placeOnEth('B1', 'BUY', '4', '100');

const taker = await waitFor(
  'B1 is not FILLED yet. Are the publisher and matching processes running?',
  async () => {
    const state = await readOrder(b1);
    return state.status === 'FILLED' ? state : undefined;
  },
);
console.log(
  '\nB1 is FILLED. Its trades, in execution order (on an empty book, 1 @ 99, 2 @ 100, 1 @ 100):',
);
for (const { quantity, price } of taker.trades) {
  console.log(`  ${quantity} @ ${price}`);
}

const maker = await readOrder(s2);
console.log(`\nS2 is ${maker.status} with ${maker.remainingQuantity} left, first at its price.`);

await placeOnEth('S4', 'SELL', '1', '100');
console.log('\nS4 arrived after S2 at the same price, so it queues behind S2.');

console.log('\n--- External notification ---');

const effects = await waitFor(
  'The destination has no effect for B1 yet. Are the external and mock-api processes running?',
  async () => {
    const found = await readEffects(b1);
    return found.length > 0 ? found : undefined;
  },
);
console.log(`\nThe destination holds ${effects.length} effect for B1:`);
for (const { idempotencyKey } of effects) {
  console.log(`  ${idempotencyKey}`);
}
