export interface AmountRule {
  /** Number of fractional digits. One unit is 10^-scale. */
  scale: number;
  /** Amounts must be a multiple of this: the price tick or the quantity step. */
  incrementUnits: bigint;
  maxUnits: bigint;
}

export interface Market {
  price: AmountRule;
  quantity: AmountRule;
}

/**
 * Add markets freely, but never change the scale of one that has orders: stored
 * units would be read at the wrong scale, and old keys would stop replaying.
 * Limits stay far below the numeric(38, 0) columns that hold them.
 */
const MARKETS: ReadonlyMap<string, Market> = new Map([
  [
    'BTC-USD',
    {
      price: { scale: 2, incrementUnits: 1n, maxUnits: 1_000_000_000n }, // 0.01 up to 10,000,000.00
      quantity: { scale: 8, incrementUnits: 1n, maxUnits: 1_000_000_000_000n }, // 0.00000001 up to 10,000.00000000
    },
  ],
  [
    'ETH-USD',
    {
      price: { scale: 2, incrementUnits: 5n, maxUnits: 100_000_000n }, // 0.05 up to 1,000,000.00
      quantity: { scale: 4, incrementUnits: 10n, maxUnits: 1_000_000_000n }, // 0.0010 up to 100,000.0000
    },
  ],
]);

export function findMarket(symbol: string): Market | undefined {
  return MARKETS.get(symbol);
}
