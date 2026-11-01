// Walks through the acceptance contract, the matching fixture and the external notification
// against a running stack, and fails if any answer is wrong. Keys are fixed, so a rerun replays
// the same orders, trades nothing new and causes no second effect. The last line is a summary
// that ./review.sh compares across a crash.
//
// The fixture assumes an empty ETH-USD book. If other orders have traded there, start clean with
// `docker compose down -v`, or run ./review.sh.
import {
  expectEqual,
  postOrder,
  readEffects,
  readOrder,
  send,
  waitFor,
  waitForStatus,
  type OrderState,
} from './stack-client.js';

const KEY = 'demo-order-key-0001';
const ORDER = { symbol: 'BTC-USD', side: 'BUY', type: 'LIMIT', price: '100.00', quantity: '2.500' };

async function placeOnEth(name: string, side: string, quantity: string, price: string) {
  const { status, body } = await postOrder(`demo-fixture-${name}-0001`, {
    symbol: 'ETH-USD',
    side,
    type: 'LIMIT',
    price,
    quantity,
  });
  expectEqual(`${name}: ${side} ${quantity} @ ${price} is accepted`, status, 202);
  return String(body.orderId);
}

function fills(state: OrderState): string[] {
  return state.trades.map(({ quantity, price }) => `${quantity} @ ${price}`);
}

console.log('Acceptance contract');

const accepted = await postOrder(KEY, ORDER);
const orderId = String(accepted.body.orderId);
expectEqual('A valid order is accepted with 202', accepted.status, 202);

const retried = await postOrder(KEY, {
  quantity: '2.5',
  price: '100',
  type: 'LIMIT',
  side: 'BUY',
  symbol: 'BTC-USD',
});
expectEqual(
  'A retry with another spelling of the same order replays the receipt',
  [retried.status, retried.body],
  [accepted.status, accepted.body],
);

const reused = await postOrder(KEY, { ...ORDER, price: '101.00' });
expectEqual(
  'The same key for a different order is rejected',
  [reused.status, reused.body.code],
  [422, 'IDEMPOTENCY_KEY_REUSED'],
);

const tooPrecise = await postOrder('demo-order-key-0002', { ...ORDER, price: '100.001' });
expectEqual(
  'More precision than the market allows is rejected, not rounded',
  [tooPrecise.status, tooPrecise.body.code],
  [400, 'INVALID_PRICE'],
);

const read = await send(`/v1/orders/${orderId}`);
expectEqual('The order can be read back', [read.status, read.body.orderId], [200, orderId]);

console.log('\nPrice priority, FIFO and partial execution on ETH-USD');

const s1 = await placeOnEth('S1', 'SELL', '2', '100');
const s2 = await placeOnEth('S2', 'SELL', '3', '100');
const s3 = await placeOnEth('S3', 'SELL', '1', '99');
const b1 = await placeOnEth('B1', 'BUY', '4', '100');

const taker = await waitForStatus(b1, 'FILLED');
expectEqual('B1 is FILLED: best price first, then oldest first, at the maker price', fills(taker), [
  '1.0000 @ 99.00',
  '2.0000 @ 100.00',
  '1.0000 @ 100.00',
]);

const maker = await readOrder(s2);
expectEqual(
  'S2 is partly executed and keeps the rest',
  [maker.status, maker.remainingQuantity],
  ['PARTIALLY_FILLED', '2.0000'],
);

const s4 = await placeOnEth('S4', 'SELL', '1', '100');
const queued = await waitForStatus(s4, 'OPEN');
expectEqual('S4 arrives later at the same price, and rests untouched', fills(queued), []);

console.log('\nExternal notification');

const effects = await waitFor(
  'The destination has no effect for B1. Are the external and mock-api processes running?',
  async () => {
    const found = await readEffects(b1);
    return found.length > 0 ? found : undefined;
  },
);
expectEqual(
  'The destination holds exactly one effect for B1, under its stable key',
  effects.length,
  1,
);

console.log(
  `Result: ${JSON.stringify({
    orders: [orderId, s1, s2, s3, b1, s4],
    trades: taker.trades.map(({ tradeId }) => tradeId),
    remaining: maker.remainingQuantity,
    effects: effects.map(({ idempotencyKey }) => idempotencyKey),
  })}`,
);
