import { describe, expect, it } from 'vitest';
import {
  applyMatch,
  matchOrder,
  type AcceptedOrder,
  type Match,
} from '../../src/modules/matching/matching-engine.js';
import { OrderBook } from '../../src/modules/matching/order-book.js';
import type { OrderSide } from '../../src/modules/orders/order.types.js';

function place(
  book: OrderBook,
  orderId: string,
  side: OrderSide,
  priceUnits: bigint,
  quantityUnits: bigint,
): Match {
  const taker: AcceptedOrder = {
    orderId,
    side,
    priceUnits,
    quantityUnits,
    acceptanceSequence: book.lastAppliedSeq + 1n,
  };
  const match = matchOrder(book, taker);
  applyMatch(book, taker, match);
  return match;
}

function tradesOf(match: Match) {
  return match.trades.map((trade) => [trade.makerId, trade.quantityUnits, trade.priceUnits]);
}

function restingOn(book: OrderBook, side: OrderSide) {
  return [...book.restingOrders(side)].map((order) => [
    order.orderId,
    order.priceUnits,
    order.remainingUnits,
  ]);
}

describe('the handoff fixture', () => {
  function bookWithSellers(): OrderBook {
    const book = new OrderBook();
    place(book, 'S1', 'SELL', 100n, 2n);
    place(book, 'S2', 'SELL', 100n, 3n);
    place(book, 'S3', 'SELL', 99n, 1n);
    return book;
  }

  it('fills the best price first, then oldest first, always at the maker price', () => {
    const book = bookWithSellers();

    const match = place(book, 'B1', 'BUY', 100n, 4n);

    expect(tradesOf(match)).toEqual([
      ['S3', 1n, 99n],
      ['S1', 2n, 100n],
      ['S2', 1n, 100n],
    ]);
    expect(match.takerUpdate).toEqual({ orderId: 'B1', remainingUnits: 0n, status: 'FILLED' });
    expect(match.makerUpdates).toEqual([
      { orderId: 'S3', remainingUnits: 0n, status: 'FILLED' },
      { orderId: 'S1', remainingUnits: 0n, status: 'FILLED' },
      { orderId: 'S2', remainingUnits: 2n, status: 'PARTIALLY_FILLED' },
    ]);
    expect(restingOn(book, 'SELL')).toEqual([['S2', 100n, 2n]]);
    expect(restingOn(book, 'BUY')).toEqual([]);
  });

  it('queues a later sell behind the partly filled maker', () => {
    const book = bookWithSellers();
    place(book, 'B1', 'BUY', 100n, 4n);

    place(book, 'S4', 'SELL', 100n, 5n);

    expect(restingOn(book, 'SELL')).toEqual([
      ['S2', 100n, 2n],
      ['S4', 100n, 5n],
    ]);
  });
});

describe('matchOrder', () => {
  it('rests orders that do not cross, untouched and open', () => {
    const book = new OrderBook();

    const sell = place(book, 'S', 'SELL', 101n, 1n);
    const buy = place(book, 'B', 'BUY', 100n, 1n);

    expect(sell).toEqual({
      trades: [],
      makerUpdates: [],
      takerUpdate: { orderId: 'S', remainingUnits: 1n, status: 'OPEN' },
    });
    expect(buy.trades).toEqual([]);
    expect(restingOn(book, 'SELL')).toEqual([['S', 101n, 1n]]);
    expect(restingOn(book, 'BUY')).toEqual([['B', 100n, 1n]]);
  });

  it('is the mirror image for a sell taker: highest bid first, then oldest first', () => {
    const book = new OrderBook();
    place(book, 'B1', 'BUY', 100n, 2n);
    place(book, 'B2', 'BUY', 100n, 3n);
    place(book, 'B3', 'BUY', 101n, 1n);

    const match = place(book, 'S1', 'SELL', 100n, 4n);

    expect(tradesOf(match)).toEqual([
      ['B3', 1n, 101n],
      ['B1', 2n, 100n],
      ['B2', 1n, 100n],
    ]);
    expect(restingOn(book, 'BUY')).toEqual([['B2', 100n, 2n]]);
  });

  it.each<[string, OrderSide, bigint, OrderSide, bigint, boolean]>([
    ['BUY at exactly the best ask', 'SELL', 100n, 'BUY', 100n, true],
    ['BUY one unit below the best ask', 'SELL', 100n, 'BUY', 99n, false],
    ['BUY one unit above the best ask', 'SELL', 100n, 'BUY', 101n, true],
    ['SELL at exactly the best bid', 'BUY', 100n, 'SELL', 100n, true],
    ['SELL one unit above the best bid', 'BUY', 100n, 'SELL', 101n, false],
    ['SELL one unit below the best bid', 'BUY', 100n, 'SELL', 99n, true],
  ])('handles %s', (_name, makerSide, makerPrice, takerSide, takerPrice, trades) => {
    const book = new OrderBook();
    place(book, 'maker', makerSide, makerPrice, 1n);

    const match = place(book, 'taker', takerSide, takerPrice, 1n);

    expect(tradesOf(match)).toEqual(trades ? [['maker', 1n, makerPrice]] : []);
  });

  it('rests a partly filled taker at its own limit once nothing else crosses', () => {
    const book = new OrderBook();
    place(book, 'S1', 'SELL', 99n, 1n);
    place(book, 'S2', 'SELL', 105n, 1n);

    const match = place(book, 'B1', 'BUY', 100n, 3n);

    expect(match.takerUpdate).toEqual({
      orderId: 'B1',
      remainingUnits: 2n,
      status: 'PARTIALLY_FILLED',
    });
    expect(restingOn(book, 'BUY')).toEqual([['B1', 100n, 2n]]);
    expect(restingOn(book, 'SELL')).toEqual([['S2', 105n, 1n]]);
  });

  it('lets one maker span several takers while keeping its place in the queue', () => {
    const book = new OrderBook();
    place(book, 'S1', 'SELL', 100n, 5n);
    place(book, 'S2', 'SELL', 100n, 1n);

    expect(place(book, 'B1', 'BUY', 100n, 2n).makerUpdates).toEqual([
      { orderId: 'S1', remainingUnits: 3n, status: 'PARTIALLY_FILLED' },
    ]);
    expect(place(book, 'B2', 'BUY', 100n, 2n).makerUpdates).toEqual([
      { orderId: 'S1', remainingUnits: 1n, status: 'PARTIALLY_FILLED' },
    ]);
    expect(place(book, 'B3', 'BUY', 100n, 1n).makerUpdates).toEqual([
      { orderId: 'S1', remainingUnits: 0n, status: 'FILLED' },
    ]);

    expect(restingOn(book, 'SELL')).toEqual([['S2', 100n, 1n]]);
  });

  it('removes a price level once it is empty, and can start it again', () => {
    const book = new OrderBook();
    place(book, 'S1', 'SELL', 100n, 1n);
    place(book, 'S2', 'SELL', 101n, 1n);

    place(book, 'B1', 'BUY', 100n, 1n);
    expect(restingOn(book, 'SELL')).toEqual([['S2', 101n, 1n]]);

    place(book, 'S3', 'SELL', 100n, 1n);
    expect(restingOn(book, 'SELL')).toEqual([
      ['S3', 100n, 1n],
      ['S2', 101n, 1n],
    ]);
  });

  it('stays exact at the largest amounts the database can hold', () => {
    const maximum = 10n ** 38n - 1n;
    const book = new OrderBook();
    place(book, 'S1', 'SELL', maximum, maximum);

    const match = place(book, 'B1', 'BUY', maximum, maximum - 1n);

    expect(tradesOf(match)).toEqual([['S1', maximum - 1n, maximum]]);
    expect(restingOn(book, 'SELL')).toEqual([['S1', maximum, 1n]]);
  });

  it('leaves the book unchanged until the match is applied', () => {
    const book = new OrderBook();
    place(book, 'S1', 'SELL', 100n, 2n);

    matchOrder(book, {
      orderId: 'B1',
      side: 'BUY',
      priceUnits: 100n,
      quantityUnits: 1n,
      acceptanceSequence: 2n,
    });

    expect(restingOn(book, 'SELL')).toEqual([['S1', 100n, 2n]]);
    expect(book.lastAppliedSeq).toBe(1n);
  });

  it('gives the same result for the same book and order', () => {
    const run = () => {
      const book = new OrderBook();
      place(book, 'S1', 'SELL', 100n, 2n);
      place(book, 'S2', 'SELL', 99n, 2n);
      return place(book, 'B1', 'BUY', 100n, 3n);
    };

    expect(run()).toEqual(run());
  });
});

describe('applyMatch', () => {
  it('refuses a fill for an order that is not next in the book', () => {
    const book = new OrderBook();
    place(book, 'S1', 'SELL', 100n, 1n);
    place(book, 'S2', 'SELL', 100n, 1n);

    expect(() => {
      book.applyFills('SELL', [{ orderId: 'S2', remainingUnits: 0n }]);
    }).toThrow('S2 is not the next SELL maker');
  });

  it('records the sequence the book now reflects', () => {
    const book = new OrderBook(41n);

    place(book, 'S1', 'SELL', 100n, 1n);

    expect(book.lastAppliedSeq).toBe(42n);
  });
});

/** The obvious way to match: re-sort every candidate maker for every order. */
class SortedListMatcher {
  readonly #resting: {
    orderId: string;
    side: OrderSide;
    priceUnits: bigint;
    remainingUnits: bigint;
    sequence: number;
  }[] = [];
  #sequence = 0;

  place(orderId: string, side: OrderSide, priceUnits: bigint, quantityUnits: bigint) {
    this.#sequence += 1;
    const trades: [string, bigint, bigint][] = [];
    let remaining = quantityUnits;

    for (const maker of this.sorted(side === 'BUY' ? 'SELL' : 'BUY')) {
      const crosses = side === 'BUY' ? maker.priceUnits <= priceUnits : maker.priceUnits >= priceUnits;
      if (remaining === 0n || !crosses) {
        break;
      }
      const filled = remaining < maker.remainingUnits ? remaining : maker.remainingUnits;
      maker.remainingUnits -= filled;
      remaining -= filled;
      trades.push([maker.orderId, filled, maker.priceUnits]);
    }

    if (remaining > 0n) {
      this.#resting.push({
        orderId,
        side,
        priceUnits,
        remainingUnits: remaining,
        sequence: this.#sequence,
      });
    }
    return trades;
  }

  /** Best price first and oldest first, without filled makers. */
  sorted(side: OrderSide) {
    const direction = side === 'SELL' ? 1 : -1;
    return this.#resting
      .filter((order) => order.side === side && order.remainingUnits > 0n)
      .sort((a, b) => {
        if (a.priceUnits === b.priceUnits) {
          return a.sequence - b.sequence;
        }
        return a.priceUnits < b.priceUnits ? -direction : direction;
      });
  }
}

/** A small seeded generator, so that a failing workload can be run again. */
function seededRandom(seed: number): (limit: number) => number {
  let state = seed;
  return (limit) => {
    state = (state + 0x6d2b79f5) | 0;
    let mixed = Math.imul(state ^ (state >>> 15), 1 | state);
    mixed = (mixed + Math.imul(mixed ^ (mixed >>> 7), 61 | mixed)) ^ mixed;
    return Math.floor((((mixed ^ (mixed >>> 14)) >>> 0) / 4_294_967_296) * limit);
  };
}

describe('compared with a sorted-list reference matcher', () => {
  it.each([1, 2, 3, 4, 5])('agrees on every trade and every book, seed %i', (seed) => {
    const random = seededRandom(seed);
    const book = new OrderBook();
    const reference = new SortedListMatcher();
    let totalTrades = 0;

    for (let index = 0; index < 3_000; index += 1) {
      const orderId = `order-${index}`;
      const side: OrderSide = random(2) === 0 ? 'BUY' : 'SELL';
      const priceUnits = BigInt(95 + random(11));
      const quantityUnits = BigInt(1 + random(20));

      const match = place(book, orderId, side, priceUnits, quantityUnits);
      const expected = reference.place(orderId, side, priceUnits, quantityUnits);

      expect(tradesOf(match)).toEqual(expected);
      totalTrades += expected.length;

      // Quantity is conserved: what the taker lost is exactly what the makers lost.
      const traded = match.trades.reduce((sum, trade) => sum + trade.quantityUnits, 0n);
      expect(quantityUnits - match.takerUpdate.remainingUnits).toBe(traded);
      for (const update of [...match.makerUpdates, match.takerUpdate]) {
        expect(update.remainingUnits).toBeGreaterThanOrEqual(0n);
      }

      for (const restingSide of ['BUY', 'SELL'] as const) {
        expect(restingOn(book, restingSide)).toEqual(
          reference
            .sorted(restingSide)
            .map((order) => [order.orderId, order.priceUnits, order.remainingUnits]),
        );
      }
    }

    // Guards against a workload so tame that the comparison proves nothing.
    expect(totalTrades).toBeGreaterThan(1_000);
  });
});
