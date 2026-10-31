import Type from 'typebox';

export const OrderSide = Type.Enum(['BUY', 'SELL']);
export const OrderType = Type.Enum(['LIMIT']);
export const OrderStatus = Type.Enum(['QUEUED', 'OPEN', 'PARTIALLY_FILLED', 'FILLED', 'REJECTED']);

// A JSON string, because a JSON number has already lost precision by the time it
// is parsed. The exact syntax and the market's limits are checked after validation.
const DecimalString = Type.String({ minLength: 1, maxLength: 64 });

export const CreateOrderHeaders = Type.Object({
  // A comma is not allowed, so a repeated header (which Node joins with ", ") is rejected.
  'idempotency-key': Type.String({ pattern: '^[A-Za-z0-9_-]{16,128}$' }),
});

export const CreateOrderBody = Type.Object(
  {
    symbol: Type.String({ minLength: 1, maxLength: 32 }),
    side: OrderSide,
    type: OrderType,
    price: DecimalString,
    quantity: DecimalString,
  },
  { additionalProperties: false },
);

export const AcceptedOrderResponse = Type.Object({
  orderId: Type.String(),
  status: Type.Literal('QUEUED'),
  statusUrl: Type.String(),
});

export const OrderParams = Type.Object({
  // Not `format: 'uuid'`, which also accepts a "urn:uuid:" prefix that PostgreSQL rejects.
  orderId: Type.String({ pattern: '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' }),
});

export const OrderTrade = Type.Object({
  tradeId: Type.String(),
  role: Type.Enum(['MAKER', 'TAKER']),
  price: Type.String(),
  quantity: Type.String(),
  executedAt: Type.String(),
});

export const OrderResponse = Type.Object({
  orderId: Type.String(),
  symbol: Type.String(),
  side: OrderSide,
  type: OrderType,
  status: OrderStatus,
  price: Type.String(),
  originalQuantity: Type.String(),
  executedQuantity: Type.String(),
  remainingQuantity: Type.String(),
  acceptanceSequence: Type.String(),
  acceptedAt: Type.String(),
  updatedAt: Type.String(),
  trades: Type.Array(OrderTrade),
});
