import type { Pool } from 'pg';
import { withTransaction } from '../../infrastructure/transaction.js';
import { claimInboxEvent } from '../messaging/inbox.repository.js';
import type { OrderAcceptedV1 } from '../messaging/order-accepted.schema.js';
import { applyMatch, matchOrder, type AcceptedOrder, type Match } from './matching-engine.js';
import { lockMatchingProgress, persistMatch } from './matching.repository.js';
import { OrderBook } from './order-book.js';
import type { OrderBooks } from './restore-book.js';

/** Fixed, so that a new Kafka group replaying the topic still recognises handled events. */
export const MATCHING_CONSUMER_NAME ='matching-v1';

/**
 * Applies one accepted order: the match is computed from the book, committed to the
 * database, and only then made visible in the book. Resolves to undefined if the event
 * was already handled.
 *
 * Any rejection leaves the book possibly behind the database, so the caller must
 * stop using it and restore. The caller may commit the Kafka offset only after this
 * resolves.
 */
export async function applyAcceptedOrder(
  pool: Pool,
  books: OrderBooks,
  event: OrderAcceptedV1,
): Promise<Match | undefined> {
  const { eventId, symbol } = event;
  const taker = toAcceptedOrder(event);
  const book = books.get(symbol) ?? new OrderBook();

  const match = await withTransaction(pool, async (tx) => {
    if (!(await claimInboxEvent(tx, MATCHING_CONSUMER_NAME, eventId))) {
      return undefined;
    }

    const lastAppliedSeq = await lockMatchingProgress(tx, symbol);
    if (taker.acceptanceSequence !== lastAppliedSeq + 1n) {
      throw new Error(
        `Sequence guard: ${symbol} expects ${lastAppliedSeq + 1n} but event ${eventId} has ` +
          `${taker.acceptanceSequence}. Matching is blocked until an operator recovers the stream`,
      );
    }
    if (book.lastAppliedSeq !== lastAppliedSeq) {
      throw new Error(
        `The ${symbol} book is at ${book.lastAppliedSeq} but the database is at ${lastAppliedSeq}`,
      );
    }

    const computed = matchOrder(book, taker);
    await persistMatch(tx, { eventId, symbol, taker }, computed);
    return computed;
  });

  if (match) {
    applyMatch(book, taker, match);
    books.set(symbol, book);
  }
  return match;
}

function toAcceptedOrder(event: OrderAcceptedV1): AcceptedOrder {
  return {
    orderId: event.orderId,
    side: event.side,
    priceUnits: BigInt(event.priceUnits),
    quantityUnits: BigInt(event.quantityUnits),
    acceptanceSequence: BigInt(event.acceptanceSequence),
  };
}
