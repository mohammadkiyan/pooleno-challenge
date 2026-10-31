import type { Pool } from 'pg';
import { withSnapshot } from '../../infrastructure/transaction.js';
import { findBookProgress, findRestingOrders } from './matching.repository.js';
import { OrderBook } from './order-book.js';

export type OrderBooks = Map<string, OrderBook>;

/**
 * Rebuilds every book from one consistent snapshot of the database. Queued orders
 * are left out because they enter the book through their events.
 */
export async function restoreBooks(pool: Pool): Promise<OrderBooks> {
  const { progress, orders } = await withSnapshot(pool, async (tx) => ({
    progress: await findBookProgress(tx),
    orders: await findRestingOrders(tx),
  }));

  const books: OrderBooks = new Map(
    progress.map(({ symbol, lastAppliedSeq }) => [symbol, new OrderBook(lastAppliedSeq)]),
  );

  for (const { symbol, side, orderId, priceUnits, remainingUnits } of orders) {
    const book = books.get(symbol);
    if (!book) {
      throw new Error(`Order ${orderId} rests on ${symbol}, which has no matching progress`);
    }
    book.add(side, { orderId, priceUnits, remainingUnits });
  }

  return books;
}
