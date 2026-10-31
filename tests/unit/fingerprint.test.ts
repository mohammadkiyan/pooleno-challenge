import { describe, expect, it } from 'vitest';
import { fingerprintOrder } from '../../src/modules/orders/order-fingerprint.js';
import type { CanonicalOrder } from '../../src/modules/orders/order.types.js';

const order: CanonicalOrder = {
  symbol: 'BTC-USD',
  side: 'BUY',
  type: 'LIMIT',
  priceUnits: 10_000n,
  quantityUnits: 250_000_000n,
};

describe('fingerprintOrder', () => {
  it('is a stable SHA-256 hex digest', () => {
    const fingerprint = fingerprintOrder('demo', order);

    expect(fingerprint).toMatch(/^[0-9a-f]{64}$/);
    expect(fingerprintOrder('demo', { ...order })).toBe(fingerprint);
  });

  it.each<[string, Partial<CanonicalOrder>]>([
    ['symbol', { symbol: 'ETH-USD' }],
    ['side', { side: 'SELL' }],
    ['price', { priceUnits: 10_001n }],
    ['quantity', { quantityUnits: 250_000_001n }],
  ])('changes with the %s', (_field, change) => {
    expect(fingerprintOrder('demo', { ...order, ...change })).not.toBe(
      fingerprintOrder('demo', order),
    );
  });

  it('changes with the scope', () => {
    expect(fingerprintOrder('other', order)).not.toBe(fingerprintOrder('demo', order));
  });

  it('cannot be confused by moving text between fields', () => {
    expect(fingerprintOrder('demo', { ...order, symbol: 'BTC-USD"," BUY' })).not.toBe(
      fingerprintOrder('demo', order),
    );
  });
});
