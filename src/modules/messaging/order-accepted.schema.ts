import Type, { type Static } from 'typebox';
import { Value } from 'typebox/value';
import { OrderSide, OrderType } from '../orders/order.schemas.js';

const Uuid = Type.String({ pattern: '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' });
const PositiveInteger = Type.String({ pattern: '^[1-9][0-9]*$' });

/** The immutable event written to the Outbox when an order is accepted. */
export const OrderAcceptedV1 = Type.Object(
  {
    eventId: Uuid,
    eventType: Type.Literal('OrderAccepted'),
    schemaVersion: Type.Literal(1),
    orderId: Uuid,
    symbol: Type.String(),
    acceptanceSequence: PositiveInteger,
    side: OrderSide,
    orderType: OrderType,
    priceUnits: PositiveInteger,
    quantityUnits: PositiveInteger,
    acceptedAt: Type.String(),
    correlationId: Type.String(),
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
