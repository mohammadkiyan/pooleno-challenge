import Type, { type Static } from 'typebox';
import { Value } from 'typebox/value';
import { OrderSide, OrderType } from '../orders/order.schemas.js';

const Uuid = Type.String({ pattern: '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' });
const PositiveInteger = Type.String({ pattern: '^[1-9][0-9]*$' });
/** What a numeric(38, 0) column can hold. */
const Units = Type.String({ pattern: '^[1-9][0-9]*$', maxLength: 38 });

/**
 * The immutable event written to the Outbox when an order is accepted.
 *
 * The free-text fields are allow-lists because of what comes after this check. PostgreSQL refuses
 * NUL in a text value, and its `->>` on a json value fails on `\u0000` or a lone surrogate, so
 * an event carrying one would pass here and then fail on every attempt to process it.
 */
export const OrderAcceptedV1 = Type.Object(
  {
    eventId: Uuid,
    eventType: Type.Literal('OrderAccepted'),
    schemaVersion: Type.Literal(1),
    orderId: Uuid,
    symbol: Type.String({ pattern: '^[A-Z0-9-]{1,32}$' }),
    // Not limited to a bigint's width, because the sequence guard stops any wrong value.
    acceptanceSequence: PositiveInteger,
    side: OrderSide,
    orderType: OrderType,
    priceUnits: Units,
    quantityUnits: Units,
    acceptedAt: Type.String({ pattern: '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{3}Z$' }),
    correlationId: Type.String({ pattern: '^[A-Za-z0-9._:-]{1,128}$' }),
  },
  { additionalProperties: false },
);

export type OrderAcceptedV1 = Static<typeof OrderAcceptedV1>;

/** Reads an event from its wire form. Throws if it is not a valid OrderAccepted v1. */
export function parseOrderAccepted(raw: string): OrderAcceptedV1 {
  const event: unknown = JSON.parse(raw);
  if (!Value.Check(OrderAcceptedV1, event)) {
    const [firstError] = Value.Errors(OrderAcceptedV1, event);
    throw new Error(`Not a valid OrderAccepted v1 event: ${firstError?.message ?? 'unknown'}`);
  }
  return event;
}
