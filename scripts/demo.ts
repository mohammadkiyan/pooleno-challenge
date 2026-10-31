// Walks through the acceptance contract against a running API. The key is fixed, so
// a rerun replays the same order. Restart the stack between runs to see it survive.

const API_URL = process.env.API_URL ?? 'http://127.0.0.1:8080';
const DEMO_KEY = 'demo-order-key-0001';

const order = { symbol: 'BTC-USD', side: 'BUY', type: 'LIMIT', price: '100.00', quantity: '2.500' };

async function send(title: string, method: string, path: string, init: RequestInit = {}) {
  const response = await fetch(new URL(path, API_URL), { method, ...init });
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
