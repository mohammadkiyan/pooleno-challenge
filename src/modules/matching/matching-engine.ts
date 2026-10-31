import type { OrderSide } from '../orders/order.types.js';
import type { OrderBook } from './order-book.js';

export interface AcceptedOrder {
  orderId: string;
  side: OrderSide;
  priceUnits: bigint;
  quantityUnits: bigint;
  acceptanceSequence: bigint;
}

export interface Trade {
  makerId: string;
  takerId: string;
  priceUnits: bigint;
  quantityUnits: bigint;
}

export interface OrderUpdate {
  orderId: string;
  remainingUnits: bigint;
  status: 'OPEN' | 'PARTIALLY_FILLED' | 'FILLED';
}

/** Everything one accepted order changes. `trades[i]` filled the maker in `makerUpdates[i]`. */
export interface Match {
  trades: Trade[];
  makerUpdates: OrderUpdate[];
  takerUpdate: OrderUpdate;
}

function oppositeSide(side: OrderSide): OrderSide {
  return side === 'BUY' ? 'SELL' : 'BUY';
}

function crosses(taker: AcceptedOrder, makerPriceUnits: bigint): boolean {
  return taker.side === 'BUY'
    ? makerPriceUnits <= taker.priceUnits
    : makerPriceUnits >= taker.priceUnits;
}

function statusOf(remainingUnits: bigint, originalUnits: bigint): OrderUpdate['status'] {
  if (remainingUnits === 0n) {
    return 'FILLED';
  }
  return remainingUnits === originalUnits ? 'OPEN' : 'PARTIALLY_FILLED';
}

/**
 * Computes what an accepted order does to the book, without changing the book.
 * Synchronous and deterministic, so the result can be persisted before it becomes
 * visible in memory. Costs O(log P + K + L) for P levels, K makers and L levels touched.
 */
export function matchOrder(book: OrderBook, taker: AcceptedOrder): Match {
  const trades: Trade[] = [];
  const makerUpdates: OrderUpdate[] = [];
  let takerRemaining = taker.quantityUnits;

  for (const maker of book.restingOrders(oppositeSide(taker.side))) {
    if (takerRemaining === 0n || !crosses(taker, maker.priceUnits)) {
      break;
    }

    const quantityUnits =
      takerRemaining < maker.remainingUnits ? takerRemaining : maker.remainingUnits;
    const makerRemaining = maker.remainingUnits - quantityUnits;
    takerRemaining -= quantityUnits;

    trades.push({
      makerId: maker.orderId,
      takerId: taker.orderId,
      priceUnits: maker.priceUnits,
      quantityUnits,
    });
    makerUpdates.push({
      orderId: maker.orderId,
      remainingUnits: makerRemaining,
      // A maker that rests here has already been accepted, so any fill leaves it partly filled.
      status: makerRemaining === 0n ? 'FILLED' : 'PARTIALLY_FILLED',
    });
  }

  return {
    trades,
    makerUpdates,
    takerUpdate: {
      orderId: taker.orderId,
      remainingUnits: takerRemaining,
      status: statusOf(takerRemaining, taker.quantityUnits),
    },
  };
}

/** Makes a committed match visible in the book. Costs O(log P + K + L log P) in total. */
export function applyMatch(book: OrderBook, taker: AcceptedOrder, match: Match): void {
  book.applyFills(oppositeSide(taker.side), match.makerUpdates);

  const { remainingUnits } = match.takerUpdate;
  if (remainingUnits > 0n) {
    book.add(taker.side, { orderId: taker.orderId, priceUnits: taker.priceUnits, remainingUnits });
  }
  book.lastAppliedSeq = taker.acceptanceSequence;
}
