import { ApiError } from '../../app/api-error.js';
import { formatUnits, parseUnits } from './decimal.js';
import { findMarket, type AmountRule } from './markets.js';
import type { CanonicalOrder, CreateOrderBody } from './order.types.js';

/** Applies the market's rules and converts the order to exact integer units. */
export function canonicalizeOrder(body: CreateOrderBody): CanonicalOrder {
  const market = findMarket(body.symbol);
  if (!market) {
    throw new ApiError(400, 'UNSUPPORTED_SYMBOL', `Symbol "${body.symbol}" is not supported`);
  }

  return {
    symbol: body.symbol,
    side: body.side,
    type: body.type,
    priceUnits: parseAmount('price', body.price, market.price),
    quantityUnits: parseAmount('quantity', body.quantity, market.quantity),
  };
}

function parseAmount(field: 'price' | 'quantity', value: string, rule: AmountRule): bigint {
  const units = parseUnits(value, rule.scale);

  if (
    units === undefined ||
    units <= 0n ||
    units > rule.maxUnits ||
    units % rule.incrementUnits !== 0n
  ) {
    const increment = formatUnits(rule.incrementUnits, rule.scale);
    const max = formatUnits(rule.maxUnits, rule.scale);
    throw new ApiError(
      400,
      `INVALID_${field.toUpperCase()}`,
      `${field} must be a plain decimal above zero, up to ${max}, in increments of ${increment}`,
    );
  }

  return units;
}
