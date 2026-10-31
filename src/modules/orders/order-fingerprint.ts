import { createHash } from 'node:crypto';
import { CREATE_ORDER_OPERATION } from './idempotency.js';
import type { CanonicalOrder } from './order.types.js';

/**
 * Identifies the meaning of a request, not its spelling: it hashes canonical
 * values, so decimal formatting and JSON property order cannot affect it.
 */
export function fingerprintOrder(scope: string, order: CanonicalOrder): string {
  const fields = [
    scope,
    CREATE_ORDER_OPERATION,
    order.symbol,
    order.side,
    order.type,
    order.priceUnits.toString(),
    order.quantityUnits.toString(),
  ];

  return createHash('sha256').update(JSON.stringify(fields)).digest('hex');
}
