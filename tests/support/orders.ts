import { randomUUID } from 'node:crypto';
import type { Pool } from 'pg';
import type { OrderAcceptedV1 } from '../../src/modules/messaging/order-accepted.schema.js';
import { acceptOrder } from '../../src/modules/orders/accept-order.js';
import { canonicalizeOrder } from '../../src/modules/orders/canonicalize-order.js';
import type {
  AcceptanceReceipt,
  CreateOrderBody,
  OrderSide,
} from '../../src/modules/orders/order.types.js';

export const validOrder: CreateOrderBody = {
  symbol: 'BTC-USD',
  side: 'BUY',
  type: 'LIMIT',
  price: '100.00',
  quantity: '2.50000000',
};

export function uniqueKey(): string {
  return `test-${randomUUID()}`;
}

export function postOrderRequest(key: string, payload: object = validOrder) {
  return {
    method: 'POST',
    url: '/v1/orders',
    headers: { 'idempotency-key': key },
    payload,
  } as const;
}

export type AcceptedBody = AcceptanceReceipt['body'];

/**
 * An order on the market that the matching fixture uses, whose price scale is 2 and
 * quantity scale is 4 with increments of 10, so that "0.0020" is 20 units.
 */
export function ethOrder(side: OrderSide, price: string, quantity: string): CreateOrderBody {
  return { symbol: 'ETH-USD', side, type: 'LIMIT', price, quantity };
}

/** Accepts an order through the real acceptance transaction and returns its event. */
export async function acceptEvent(pool: Pool, body: CreateOrderBody): Promise<OrderAcceptedV1> {
  const result = await acceptOrder(pool, 1_000, {
    scope: 'demo',
    idempotencyKey: uniqueKey(),
    order: canonicalizeOrder(body),
    correlationId: 'test',
  });
  if (result.replayed) {
    throw new Error('A unique key was replayed');
  }
  return result.event;
}
