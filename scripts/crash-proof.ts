// Shows that a confirmed order survives a crash. ./review.sh runs one phase at a time around
// real SIGKILLs. Keys are fixed, so each phase finds the orders again by replaying their receipts:
//
//   prepare   Two makers rest in the book, and a taker executes part of the first.
//   (the publisher is killed)
//   accept    An order is confirmed with 202 and saved, and nothing can publish its event.
//   (every process is killed, including PostgreSQL and Kafka, and everything restarts)
//   verify    That order was still published, matched against the restored book, and notified.
//
// It uses BTC-USD above the walkthrough's bid, so it leaves the walkthrough's results alone.
import {
  expectEqual,
  postOrder,
  readAllEffects,
  readEffects,
  readOrder,
  send,
  waitFor,
  waitForStatus,
} from './stack-client.js';

const orders = {
  makerA: { key: 'crash-proof-maker-a-01', side: 'SELL', quantity: '2' },
  makerB: { key: 'crash-proof-maker-b-01', side: 'SELL', quantity: '2' },
  firstTaker: { key: 'crash-proof-taker-1-01', side: 'BUY', quantity: '1' },
  savedTaker: { key: 'crash-proof-taker-2-01', side: 'BUY', quantity: '2' },
};

async function place({ key, side, quantity }: (typeof orders)[keyof typeof orders]) {
  const { status, body } = await postOrder(key, {
    symbol: 'BTC-USD',
    side,
    type: 'LIMIT',
    price: '200.00',
    quantity,
  });
  if (status !== 202) {
    throw new Error(`Placing ${key} answered ${status}: ${JSON.stringify(body)}`);
  }
  return String(body.orderId);
}

async function prepare(): Promise<void> {
  const makerA = await place(orders.makerA);
  const makerB = await place(orders.makerB);
  await waitForStatus(makerB, 'OPEN');
  const firstTaker = await place(orders.firstTaker);
  await waitForStatus(firstTaker, 'FILLED');

  const a = await readOrder(makerA);
  const b = await readOrder(makerB);
  expectEqual(
    'Before the crash, the older maker A has 1 left and maker B is untouched',
    [a.status, a.remainingQuantity, b.status, b.remainingQuantity],
    ['PARTIALLY_FILLED', '1.00000000', 'OPEN', '2.00000000'],
  );
}

async function accept(): Promise<void> {
  const savedTaker = await place(orders.savedTaker);
  console.log('  ok  With the publisher dead, a new order is still confirmed with 202');

  const { status } = await readOrder(savedTaker);
  expectEqual('It is saved and not matched', status, 'QUEUED');

  const { body } = await send('/ops/backlog');
  const { pending } = body.outbox as { pending: number };
  expectEqual('Its event is waiting in the Outbox', pending, 1);
}

async function verify(): Promise<void> {
  const savedTaker = await place(orders.savedTaker);
  const filled = await waitForStatus(savedTaker, 'FILLED');
  expectEqual(
    'After the restart the saved order was published and matched, not lost',
    filled.trades.length,
    2,
  );

  const a = await readOrder(await place(orders.makerA));
  const b = await readOrder(await place(orders.makerB));
  expectEqual(
    'The rebuilt book kept the queue and the remainder: A finished first, then B',
    [a.status, a.remainingQuantity, b.status, b.remainingQuantity],
    ['FILLED', '0.00000000', 'PARTIALLY_FILLED', '1.00000000'],
  );
  expectEqual('Maker A has one trade from each taker, none repeated', a.trades.length, 2);

  const effects = await waitFor('The saved order was never notified', async () => {
    const found = await readEffects(savedTaker);
    return found.length > 0 ? found : undefined;
  });
  expectEqual('The saved order caused exactly one effect at the destination', effects.length, 1);

  const keys = (await readAllEffects()).map(({ idempotencyKey }) => idempotencyKey);
  expectEqual('No notification key was applied twice', new Set(keys).size, keys.length);
}

const phases: Record<string, () => Promise<void>> = { prepare, accept, verify };
const phase = phases[process.argv[2] ?? ''];
if (!phase) {
  throw new Error('Usage: crash-proof <prepare|accept|verify>');
}
await phase();
