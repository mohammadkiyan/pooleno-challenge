import type { Pool, PoolClient } from 'pg';
import type { CanonicalOrder, OrderSide, OrderStatus, OrderType } from './order.types.js';

export interface NewOrder extends CanonicalOrder {
  id: string;
  scope: string;
  acceptanceSeq: bigint;
  acceptedAt: Date;
}

export interface StoredOrder {
  id: string;
  symbol: string;
  side: OrderSide;
  type: OrderType;
  status: OrderStatus;
  priceUnits: bigint;
  originalUnits: bigint;
  remainingUnits: bigint;
  acceptanceSeq: bigint;
  createdAt: Date;
  updatedAt: Date;
}

/**
 * Advances the symbol's counter, creating it on first use. The row lock taken
 * here is held until commit, which makes sequence order equal commit order.
 * A rollback restores the counter, so sequences have no gaps.
 */
export async function nextAcceptanceSequence(tx: PoolClient, symbol: string): Promise<bigint> {
  const { rows } = await tx.query<{ last_seq: string }>(
    `INSERT INTO symbol_sequences (symbol, last_seq)
     VALUES ($1, 1)
     ON CONFLICT (symbol) DO UPDATE SET last_seq = symbol_sequences.last_seq + 1
     RETURNING last_seq`,
    [symbol],
  );

  const row = rows[0];
  if (!row) {
    throw new Error(`Advancing the sequence of ${symbol} returned no row`);
  }
  return BigInt(row.last_seq);
}

export async function insertOrder(tx: PoolClient, order: NewOrder): Promise<void> {
  await tx.query(
    `INSERT INTO orders
       (id, scope, symbol, side, order_type, price_units, original_units, remaining_units,
        status, acceptance_seq, created_at, updated_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $7, 'QUEUED', $8, $9, $9)`,
    [
      order.id,
      order.scope,
      order.symbol,
      order.side,
      order.type,
      order.priceUnits,
      order.quantityUnits,
      order.acceptanceSeq,
      order.acceptedAt,
    ],
  );
}

export async function findOrder(
  pool: Pool,
  scope: string,
  orderId: string,
): Promise<StoredOrder | undefined> {
  const { rows } = await pool.query<{
    id: string;
    symbol: string;
    side: OrderSide;
    order_type: OrderType;
    status: OrderStatus;
    price_units: string;
    original_units: string;
    remaining_units: string;
    acceptance_seq: string;
    created_at: Date;
    updated_at: Date;
  }>(
    `SELECT id, symbol, side, order_type, status, price_units, original_units,
            remaining_units, acceptance_seq, created_at, updated_at
       FROM orders
      WHERE id = $1 AND scope = $2`,
    [orderId, scope],
  );

  const row = rows[0];
  if (!row) {
    return undefined;
  }

  return {
    id: row.id,
    symbol: row.symbol,
    side: row.side,
    type: row.order_type,
    status: row.status,
    priceUnits: BigInt(row.price_units),
    originalUnits: BigInt(row.original_units),
    remainingUnits: BigInt(row.remaining_units),
    acceptanceSeq: BigInt(row.acceptance_seq),
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}
