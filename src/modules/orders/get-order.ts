import type { Pool } from 'pg';
import { ApiError } from '../../app/api-error.js';
import { formatUnits } from './decimal.js';
import { findMarket } from './markets.js';
import { findOrder } from './order.repository.js';
import type { OrderResponse } from './order.types.js';

export async function getOrder(pool: Pool, scope: string, orderId: string): Promise<OrderResponse> {
  const order = await findOrder(pool, scope, orderId);
  if (!order) {
    throw new ApiError(404, 'ORDER_NOT_FOUND', `Order ${orderId} does not exist`);
  }

  const market = findMarket(order.symbol);
  if (!market) {
    throw new Error(`Order ${order.id} has symbol ${order.symbol}, which is no longer configured`);
  }

  const quantity = (units: bigint) => formatUnits(units, market.quantity.scale);

  return {
    orderId: order.id,
    symbol: order.symbol,
    side: order.side,
    type: order.type,
    status: order.status,
    price: formatUnits(order.priceUnits, market.price.scale),
    originalQuantity: quantity(order.originalUnits),
    executedQuantity: quantity(order.originalUnits - order.remainingUnits),
    remainingQuantity: quantity(order.remainingUnits),
    acceptanceSequence: order.acceptanceSeq.toString(),
    acceptedAt: order.createdAt.toISOString(),
    updatedAt: order.updatedAt.toISOString(),
  };
}
