import type { Static } from 'typebox';
import type {
  AcceptedOrderResponse,
  CreateOrderBody,
  OrderResponse,
  OrderSide,
  OrderStatus,
  OrderType,
} from './order.schemas.js';

export type OrderSide = Static<typeof OrderSide>;
export type OrderType = Static<typeof OrderType>;
export type OrderStatus = Static<typeof OrderStatus>;
export type CreateOrderBody = Static<typeof CreateOrderBody>;
export type OrderResponse = Static<typeof OrderResponse>;

/** A validated order in exact integer units. */
export interface CanonicalOrder {
  symbol: string;
  side: OrderSide;
  type: OrderType;
  priceUnits: bigint;
  quantityUnits: bigint;
}

/** The exact response of an acceptance, stored so that a retry can replay it. */
export interface AcceptanceReceipt {
  status: 202;
  location: string;
  body: Static<typeof AcceptedOrderResponse>;
}
