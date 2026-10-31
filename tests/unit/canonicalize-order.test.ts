import { describe, expect, it } from 'vitest';
import { canonicalizeOrder } from '../../src/modules/orders/canonicalize-order.js';
import type { CreateOrderBody } from '../../src/modules/orders/order.types.js';

const btcOrder: CreateOrderBody = {
  symbol: 'BTC-USD',
  side: 'BUY',
  type: 'LIMIT',
  price: '100.00',
  quantity: '2.5',
};

describe('canonicalizeOrder', () => {
  it('converts amounts to the market units', () => {
    expect(canonicalizeOrder(btcOrder)).toEqual({
      symbol: 'BTC-USD',
      side: 'BUY',
      type: 'LIMIT',
      priceUnits: 10_000n,
      quantityUnits: 250_000_000n,
    });
  });

  it('rejects a symbol without a market', () => {
    expect(() => canonicalizeOrder({ ...btcOrder, symbol: 'DOGE-USD' })).toThrow(
      expect.objectContaining({ statusCode: 400, code: 'UNSUPPORTED_SYMBOL' }),
    );
  });

  it.each([
    ['zero', 'price', '0', 'INVALID_PRICE'],
    ['zero with a fraction', 'price', '0.00', 'INVALID_PRICE'],
    ['negative', 'price', '-1.00', 'INVALID_PRICE'],
    ['exponent', 'price', '1e2', 'INVALID_PRICE'],
    ['too precise', 'price', '100.001', 'INVALID_PRICE'],
    ['above the maximum', 'price', '10000000.01', 'INVALID_PRICE'],
    ['zero', 'quantity', '0', 'INVALID_QUANTITY'],
    ['too precise', 'quantity', '0.000000001', 'INVALID_QUANTITY'],
    ['above the maximum', 'quantity', '10000.00000001', 'INVALID_QUANTITY'],
  ])('rejects a %s %s', (_title, field, value, code) => {
    expect(() => canonicalizeOrder({ ...btcOrder, [field]: value })).toThrow(
      expect.objectContaining({ statusCode: 400, code }),
    );
  });

  it('accepts the exact limits of a market', () => {
    const order = canonicalizeOrder({ ...btcOrder, price: '10000000.00', quantity: '0.00000001' });

    expect(order.priceUnits).toBe(1_000_000_000n);
    expect(order.quantityUnits).toBe(1n);
  });

  it('enforces the price tick and quantity step', () => {
    const ethOrder = { ...btcOrder, symbol: 'ETH-USD', price: '100.05', quantity: '0.001' };

    expect(canonicalizeOrder(ethOrder).priceUnits).toBe(10_005n);
    expect(() => canonicalizeOrder({ ...ethOrder, price: '100.03' })).toThrow(
      expect.objectContaining({ code: 'INVALID_PRICE' }),
    );
    expect(() => canonicalizeOrder({ ...ethOrder, quantity: '0.0015' })).toThrow(
      expect.objectContaining({ code: 'INVALID_QUANTITY' }),
    );
  });
});
