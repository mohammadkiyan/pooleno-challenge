import type { PoolClient } from 'pg';
import type { OrderSide } from '../orders/order.types.js';
import type { AcceptedOrder, Match } from './matching-engine.js';

export interface BookProgress {
  symbol: string;
  lastAppliedSeq: bigint;
}

export interface StoredRestingOrder {
  orderId: string;
  symbol: string;
  side: OrderSide;
  priceUnits: bigint;
  remainingUnits: bigint;
}

/** Locks the symbol's progress row for the rest of the transaction, creating it on first use. */
export async function lockMatchingProgress(tx: PoolClient, symbol: string): Promise<bigint> {
  await tx.query('INSERT INTO matching_progress (symbol) VALUES ($1) ON CONFLICT DO NOTHING', [
    symbol,
  ]);
  const { rows } = await tx.query<{ last_applied_seq: string }>(
    'SELECT last_applied_seq FROM matching_progress WHERE symbol = $1 FOR UPDATE',
    [symbol],
  );

  const row = rows[0];
  if (!row) {
    throw new Error(`Locking the matching progress of ${symbol} returned no row`);
  }
  return BigInt(row.last_applied_seq);
}

/**
 * Writes a match and advances the symbol's progress. Every update names the state it
 * expects to replace, so a book that disagrees with the database fails the transaction.
 */
export async function persistMatch(
  tx: PoolClient,
  source: { eventId: string; symbol: string; taker: AcceptedOrder },
  match: Match,
): Promise<void> {
  const { eventId, symbol, taker } = source;
  const { takerUpdate, makerUpdates, trades } = match;

  const updatedTakers = await tx.query(
    `UPDATE orders
        SET remaining_units = $2, status = $3, updated_at = now()
      WHERE id = $1 AND status = 'QUEUED'`,
    [takerUpdate.orderId, takerUpdate.remainingUnits, takerUpdate.status],
  );
  if (updatedTakers.rowCount !== 1) {
    throw new Error(`Order ${taker.orderId} is not a queued order`);
  }

  if (trades.length === 0) {
    await advanceProgress(tx, symbol, taker.acceptanceSequence);
    return;
  }

  const updatedMakers = await tx.query(
    `UPDATE orders o
        SET remaining_units = fill.remaining_units, status = fill.status, updated_at = now()
       FROM unnest($1::uuid[], $2::numeric[], $3::numeric[], $4::text[])
            AS fill(order_id, remaining_units, quantity_units, status)
      WHERE o.id = fill.order_id
        AND o.remaining_units = fill.remaining_units + fill.quantity_units
        AND o.status IN ('OPEN', 'PARTIALLY_FILLED')`,
    [
      makerUpdates.map((maker) => maker.orderId),
      makerUpdates.map((maker) => maker.remainingUnits.toString()),
      trades.map((trade) => trade.quantityUnits.toString()),
      makerUpdates.map((maker) => maker.status),
    ],
  );
  if (updatedMakers.rowCount !== makerUpdates.length) {
    throw new Error(`The book disagrees with the database about the makers of ${taker.orderId}`);
  }

  await tx.query(
    `INSERT INTO trades
       (event_id, execution_index, maker_id, taker_id, symbol, price_units, quantity_units)
     SELECT $1, fill.position - 1, fill.maker_id, $2, $3, fill.price_units, fill.quantity_units
       FROM unnest($4::uuid[], $5::numeric[], $6::numeric[])
            WITH ORDINALITY AS fill(maker_id, price_units, quantity_units, position)`,
    [
      eventId,
      taker.orderId,
      symbol,
      trades.map((trade) => trade.makerId),
      trades.map((trade) => trade.priceUnits.toString()),
      trades.map((trade) => trade.quantityUnits.toString()),
    ],
  );

  await advanceProgress(tx, symbol, taker.acceptanceSequence);
}

async function advanceProgress(tx: PoolClient, symbol: string, sequence: bigint): Promise<void> {
  await tx.query(
    `UPDATE matching_progress SET last_applied_seq = $2, updated_at = now() WHERE symbol = $1`,
    [symbol, sequence],
  );
}

/** Covers every symbol, or only `symbol` when it is given. */
export async function findBookProgress(tx: PoolClient, symbol?: string): Promise<BookProgress[]> {
  const { rows } = await tx.query<{ symbol: string; last_applied_seq: string }>(
    `SELECT symbol, last_applied_seq FROM matching_progress
      WHERE $1::text IS NULL OR symbol = $1`,
    [symbol],
  );
  return rows.map((row) => ({
    symbol: row.symbol,
    lastAppliedSeq: BigInt(row.last_applied_seq),
  }));
}

/**
 * Covers every symbol, or only `symbol` when it is given. Oldest first, so that adding them
 * in this order restores each price level's queue.
 */
export async function findRestingOrders(
  tx: PoolClient,
  symbol?: string,
): Promise<StoredRestingOrder[]> {
  const { rows } = await tx.query<{
    id: string;
    symbol: string;
    side: OrderSide;
    price_units: string;
    remaining_units: string;
  }>(
    `SELECT id, symbol, side, price_units, remaining_units
       FROM orders
      WHERE status IN ('OPEN', 'PARTIALLY_FILLED') AND remaining_units > 0
        AND ($1::text IS NULL OR symbol = $1)
      ORDER BY symbol, acceptance_seq`,
    [symbol],
  );
  return rows.map((row) => ({
    orderId: row.id,
    symbol: row.symbol,
    side: row.side,
    priceUnits: BigInt(row.price_units),
    remainingUnits: BigInt(row.remaining_units),
  }));
}
