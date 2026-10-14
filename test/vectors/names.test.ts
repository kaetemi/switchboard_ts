// Vectors from the wiki page "Polyverse Switchboard Test Vectors", section "Names".
import { describe, expect, test } from 'vitest';
import { hexBytes, hexString } from '../../src/core/bytes.ts';
import { Name } from '../../src/core/name.ts';

describe('names', () => {
  const vectors: [string, string][] = [
    ['', '00 00 00 00 00 00 00 00'],
    ['api', '61 70 69 00 00 00 00 00'],
    ['REGISTER', '52 45 47 49 53 54 45 52'],
    ['_ns', '5F 6E 73 00 00 00 00 00'],
    ['é', 'C3 A9 00 00 00 00 00 00'],
  ];

  test.each(vectors)('"%s" encodes as %s', (text, hex) => {
    const name = Name.of(text);
    expect(hexString(name.bytes)).toBe(hex);
    const decoded = Name.fromBytes(hexBytes(hex));
    expect(decoded.equals(name)).toBe(true);
    expect(decoded.key).toBe(name.key);
    expect(decoded.toString()).toBe(text);
    expect(Name.fromKey(name.key).equals(name)).toBe(true);
  });

  test('a string longer than 8 bytes of UTF-8 is rejected, not truncated', () => {
    expect(() => Name.of('REGISTERS')).toThrow(RangeError);
    expect(() => Name.of('ééééé')).toThrow(RangeError);
    expect(Name.of('éééé').length).toBe(8);
  });

  test('a zero byte inside a name is rejected', () => {
    expect(() => Name.of('a\0b')).toThrow(RangeError);
  });

  test('a non-zero byte after a zero byte is invalid', () => {
    expect(() => Name.fromBytes(hexBytes('61 00 62 00 00 00 00 00'))).toThrow(RangeError);
    expect(() => Name.fromBytes(hexBytes('61 00'))).toThrow(RangeError);
  });

  test('reserved and empty', () => {
    expect(Name.of('_ns').isReserved).toBe(true);
    expect(Name.of('ns').isReserved).toBe(false);
    expect(Name.empty.isEmpty).toBe(true);
    expect(Name.of('').equals(Name.empty)).toBe(true);
    expect(Name.of('api').length).toBe(3);
  });

  test('names are usable as map keys through key', () => {
    const map = new Map<string, number>();
    map.set(Name.of('api').key, 1);
    expect(map.get(Name.of('api').key)).toBe(1);
    expect(map.get(Name.of('npc').key)).toBeUndefined();
  });
});
