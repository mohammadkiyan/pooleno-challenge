const PLAIN_DECIMAL = /^(0|[1-9]\d*)(?:\.(\d+))?$/;

/**
 * Converts a decimal string to integer units without passing through a float.
 * Returns undefined for anything that is not a plain unsigned decimal, or that
 * cannot be represented exactly at `scale`. Trailing zeros carry no precision,
 * so "100", "100.0" and "100.000" are all the same value at scale 2.
 */
export function parseUnits(value: string, scale: number): bigint | undefined {
  const match = PLAIN_DECIMAL.exec(value);
  if (!match) {
    return undefined;
  }

  const whole = match[1] ?? '';
  const fraction = (match[2] ?? '').replace(/0+$/, '');
  if (fraction.length > scale) {
    return undefined;
  }

  return BigInt(whole + fraction.padEnd(scale, '0'));
}

/** Formats integer units as a decimal string with exactly `scale` fractional digits. */
export function formatUnits(units: bigint, scale: number): string {
  if (scale === 0) {
    return units.toString();
  }

  const digits = units.toString().padStart(scale + 1, '0');
  const pointIndex = digits.length - scale;
  return `${digits.slice(0, pointIndex)}.${digits.slice(pointIndex)}`;
}
