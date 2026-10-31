import { randomUUID } from 'node:crypto';
import type { AcceptanceReceipt, CreateOrderBody } from '../../src/modules/orders/order.types.js';

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
