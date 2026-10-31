import Type, { type Static } from 'typebox';
import { OrderSide, OrderType } from '../orders/order.schemas.js';

/** The immutable event written to the Outbox when an order is accepted. */
export const OrderAcceptedV1 = Type.Object(
  {
    eventId: Type.String(),
    eventType: Type.Literal('OrderAccepted'),
    schemaVersion: Type.Literal(1),
    orderId: Type.String(),
    symbol: Type.String(),
    acceptanceSequence: Type.String(),
    side: OrderSide,
    orderType: OrderType,
    priceUnits: Type.String(),
    quantityUnits: Type.String(),
    acceptedAt: Type.String(),
    correlationId: Type.String(),
  },
  { additionalProperties: false },
);

export type OrderAcceptedV1 = Static<typeof OrderAcceptedV1>;
