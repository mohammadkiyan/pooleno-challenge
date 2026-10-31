import type { Pool } from 'pg';
import { ApiError } from '../../app/api-error.js';
import { withSnapshot } from '../../infrastructure/transaction.js';
import { formatUnits } from './decimal.js';
import { findMarket } from './markets.js';
import { findOrder, findOrderTrades } from './order.repository.js';
import type { OrderResponse } from './order.types.js';

/** Reads the order and its trades from one snapshot, so that the two always agree. */
export async function getOrder(pool: Pool, scope: string, orderId: string): Promise<OrderResponse> {
  const { order, trades } = await withSnapshot(pool, async (tx) => {
    const found = await findOrder(tx, scope, orderId);
    return { order: found, trades: found ? await findOrderTrades(tx, found.id) : [] };
  });
  if (!order) {
    throw new ApiError(404, 'ORDER_NOT_FOUND', `Order ${orderId} does not exist`);
  }

  const market = findMarket(order.symbol);
  if (!market) {
    throw new Error(`Order ${order.id} has symbol ${order.symbol}, which is no longer configured`);
  }

  const price = (units: bigint) => formatUnits(units, market.price.scale);
  const quantity = (units: bigint) => formatUnits(units, market.quantity.scale);

  return {
    orderId: order.id,
    symbol: order.symbol,
    side: order.side,
    type: order.type,
    status: order.status,
    price: price(order.priceUnits),
    originalQuantity: quantity(order.originalUnits),
    executedQuantity: quantity(order.originalUnits - order.remainingUnits),
    remainingQuantity: quantity(order.remainingUnits),
    acceptanceSequence: order.acceptanceSeq.toString(),
    acceptedAt: order.createdAt.toISOString(),
    updatedAt: order.updatedAt.toISOString(),
    trades: trades.map((trade) => ({
      tradeId: trade.id,
      role: trade.role,
      price: price(trade.priceUnits),
      quantity: quantity(trade.quantityUnits),
      executedAt: trade.executedAt.toISOString(),
    })),
  };
}
