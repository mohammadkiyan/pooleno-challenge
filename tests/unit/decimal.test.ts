import { describe, expect, it } from 'vitest';
import { formatUnits, parseUnits } from '../../src/modules/orders/decimal.js';

describe('parseUnits', () => {
  it.each([
    ['100', 2, 10_000n],
    ['100.5', 2, 10_050n],
    ['100.50', 2, 10_050n],
    ['0.01', 2, 1n],
    ['0', 2, 0n],
    ['42', 0, 42n],
  ])('reads %s at scale %i', (value, scale, expected) => {
    expect(parseUnits(value, scale)).toBe(expected);
  });

  it('treats trailing zeros as the same value, even beyond the scale', () => {
    expect(parseUnits('100.000000', 2)).toBe(parseUnits('100', 2));
  });

  it('is exact where binary floating point is not', () => {
    expect(0.07 * 100).not.toBe(7);
    expect(parseUnits('0.07', 2)).toBe(7n);
    expect(parseUnits('1.1', 2)).toBe(110n);
  });

  it('keeps integers above Number.MAX_SAFE_INTEGER', () => {
    expect(parseUnits('9007199254740993', 0)).toBe(9_007_199_254_740_993n);
    expect(parseUnits('12345678901234567890123456789012.12345', 5)).toBe(
      1_234_567_890_123_456_789_012_345_678_901_212_345n,
    );
  });

  it('rejects precision that it would otherwise have to round', () => {
    expect(parseUnits('100.001', 2)).toBeUndefined();
    expect(parseUnits('1.5', 0)).toBeUndefined();
  });

  it.each(['', ' 1', '1 ', '-1', '+1', '1e3', '1E3', '01', '00', '.5', '5.', '1.2.3', '1,5', '0x10', 'NaN', 'Infinity', '١'])(
    'rejects %j',
    (value) => {
      expect(parseUnits(value, 8)).toBeUndefined();
    },
  );
});

describe('formatUnits', () => {
  it.each([
    [10_050n, 2, '100.50'],
    [1n, 2, '0.01'],
    [0n, 2, '0.00'],
    [1n, 8, '0.00000001'],
    [42n, 0, '42'],
  ])('writes %s at scale %i as %s', (units, scale, expected) => {
    expect(formatUnits(units, scale)).toBe(expected);
  });

  it('round-trips with parseUnits', () => {
    const units = 1_234_567_890_123_456_789_012n;
    expect(parseUnits(formatUnits(units, 8), 8)).toBe(units);
  });
});
