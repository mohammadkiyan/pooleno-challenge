import { describe, expect, it } from 'vitest';
import {
  parseOrderAccepted,
  type OrderAcceptedV1,
} from '../../src/modules/messaging/order-accepted.schema.js';

const VALID: OrderAcceptedV1 = {
  eventId: '0b0f4c1e-7d55-4e9a-9a53-1f0e4b6a2c11',
  eventType: 'OrderAccepted',
  schemaVersion: 1,
  orderId: '5a1d9c32-2b7e-4f60-8c14-6e3a7d90b2f4',
  symbol: 'BTC-USD',
  acceptanceSequence: '42',
  side: 'BUY',
  orderType: 'LIMIT',
  priceUnits: '10000',
  quantityUnits: '250000000',
  acceptedAt: '2026-11-01T17:54:02.123Z',
  correlationId: '9f3b7a60-6c1d-4a2e-b8d5-0c4e2f7a1b93',
};

describe('parseOrderAccepted', () => {
  it('reads the event that acceptance writes', () => {
    expect(parseOrderAccepted(JSON.stringify(VALID))).toEqual(VALID);
  });

  it('accepts the widest amount the columns can hold', () => {
    const widest = '9'.repeat(38);

    expect(
      parseOrderAccepted(JSON.stringify({ ...VALID, priceUnits: widest, quantityUnits: widest })),
    ).toMatchObject({ priceUnits: widest, quantityUnits: widest });
  });

  it.each([
    ['a NUL in the symbol', { symbol: 'BTC\0USD' }],
    ['a lone surrogate in the symbol', { symbol: 'BTC\ud800USD' }],
    ['an empty symbol', { symbol: '' }],
    ['a symbol wider than the API allows', { symbol: 'X'.repeat(33) }],
    ['a NUL in the correlation ID', { correlationId: 'abc\0' }],
    ['a lone surrogate in the correlation ID', { correlationId: '\udfff' }],
    ['an empty correlation ID', { correlationId: '' }],
    ['a NUL in the timestamp', { acceptedAt: '2026-11-01T17:54:02.123Z\0' }],
    ['a lone surrogate in the timestamp', { acceptedAt: '\ud800' }],
    ['a timestamp in another form', { acceptedAt: 'yesterday' }],
    ['a price wider than its column', { priceUnits: '1'.repeat(39) }],
    ['a quantity wider than its column', { quantityUnits: '1'.repeat(39) }],
  ])('refuses %s', (_name, change) => {
    expect(() => parseOrderAccepted(JSON.stringify({ ...VALID, ...change }))).toThrow(
      'Not a valid OrderAccepted v1 event',
    );
  });
});
