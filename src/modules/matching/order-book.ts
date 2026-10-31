import sortedBtree from 'sorted-btree';
import type { OrderSide } from '../orders/order.types.js';
import { PriceLevel, type RestingOrder } from './price-level.js';

const { default: BTree } = sortedBtree;

export interface MakerUpdate {
  orderId: string;
  remainingUnits: bigint;
}

type PriceLevels = InstanceType<typeof BTree<bigint, PriceLevel>>;

const lowestFirst = (a: bigint, b: bigint) => (a < b ? -1 : a > b ? 1 : 0);
const highestFirst = (a: bigint, b: bigint) => lowestFirst(b, a);

/**
 * One symbol's resting orders. Each side is a balanced tree of price levels, sorted
 * so that the best price comes first: the lowest ask and the highest bid.
 */
export class OrderBook {
  /** The sequence of the last accepted order this book reflects. */
  lastAppliedSeq: bigint;

  readonly #sides = {
    BUY: new BTree<bigint, PriceLevel>(undefined, highestFirst),
    SELL: new BTree<bigint, PriceLevel>(undefined, lowestFirst),
  };

  constructor(lastAppliedSeq = 0n) {
    this.lastAppliedSeq = lastAppliedSeq;
  }

  /** Queues an order behind the others at its price. O(log P), where P is the number of levels. */
  add(side: OrderSide, order: RestingOrder): void {
    const levels = this.#sides[side];
    let level = levels.get(order.priceUnits);
    if (!level) {
      level = new PriceLevel();
      levels.set(order.priceUnits, level);
    }
    level.append(order);
  }

  /** Walks one side in priority order, best price first and oldest first. It changes nothing. */
  *restingOrders(side: OrderSide): Generator<RestingOrder> {
    for (const [, level] of this.#sides[side].entries()) {
      yield* level;
    }
  }

  /**
   * Applies makers' fills, given in priority order. Each one must be the oldest
   * order at the best price, so a book that has drifted from the fills fails here
   * rather than silently diverging. The best level is looked up again only after
   * one has been emptied, which makes this O(log P + K + L log P) for K makers and
   * L emptied levels.
   */
  applyFills(side: OrderSide, fills: readonly MakerUpdate[]): void {
    const levels = this.#sides[side];
    let level = bestLevel(levels);

    for (const fill of fills) {
      const maker = level?.oldest;
      if (!level || !maker || maker.orderId !== fill.orderId) {
        throw new Error(`Order ${fill.orderId} is not the next ${side} maker in the book`);
      }

      if (fill.remainingUnits > 0n) {
        maker.remainingUnits = fill.remainingUnits;
        continue;
      }

      level.removeOldest();
      if (!level.oldest) {
        levels.delete(maker.priceUnits);
        level = bestLevel(levels);
      }
    }
  }
}

function bestLevel(levels: PriceLevels): PriceLevel | undefined {
  const priceUnits = levels.minKey();
  return priceUnits === undefined ? undefined : levels.get(priceUnits);
}
