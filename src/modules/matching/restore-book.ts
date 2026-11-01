import type { Pool } from 'pg';
import { withSnapshot } from '../../infrastructure/transaction.js';
import { findBookProgress, findRestingOrders } from './matching.repository.js';
import { OrderBook } from './order-book.js';

export type OrderBooks = Map<string, OrderBook>;

/**
 * Rebuilds every book, or only `symbol`'s, from one consistent snapshot of the database.
 * Queued orders are left out because they enter the book through their events.
 */
export async function restoreBooks(pool: Pool, symbol?: string): Promise<OrderBooks> {
  const { progress, orders } = await withSnapshot(pool, async (tx) => ({
    progress: await findBookProgress(tx, symbol),
    orders: await findRestingOrders(tx, symbol),
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

/** Replaces one symbol's book, leaving the others in use. A symbol not yet matched has no book. */
export async function restoreBook(pool: Pool, books: OrderBooks, symbol: string): Promise<void> {
  const book = (await restoreBooks(pool, symbol)).get(symbol);
  if (book) {
    books.set(symbol, book);
  } else {
    books.delete(symbol);
  }
}
