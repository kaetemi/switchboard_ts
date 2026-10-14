import { describe, expect, test } from 'vitest';
import {
  ByteFormatError,
  ByteReader,
  ByteWriter,
  bytesEqual,
  concatBytes,
  hexBytes,
  hexString,
  readU48,
  toPayload,
  writeU48,
} from '../../src/core/bytes.ts';
import { Name } from '../../src/core/name.ts';

describe('byte reader and writer', () => {
  test('round trip of every width, exact up to 48 bits', () => {
    const w = new ByteWriter(4)
      .u8(0xff)
      .u16(0xfffe)
      .u24(0xfffdfc)
      .u32(0xfffbfaf9)
      .u48(0xfff8f7f6f5f4)
      .name(Name.of('api'))
      .string8('héllo')
      .bytes([1, 2, 3]);
    const r = new ByteReader(w.toBytes());
    expect(r.u8()).toBe(0xff);
    expect(r.u16()).toBe(0xfffe);
    expect(r.u24()).toBe(0xfffdfc);
    expect(r.u32()).toBe(0xfffbfaf9);
    expect(r.u48()).toBe(0xfff8f7f6f5f4);
    expect(r.name().toString()).toBe('api');
    expect(r.string8()).toBe('héllo');
    expect(hexString(r.rest())).toBe('01 02 03');
    expect(r.isDone).toBe(true);
  });

  test('u32 with the top bit set is not negative', () => {
    expect(new ByteReader(hexBytes('FF FF FF FF')).u32()).toBe(0xffffffff);
    expect(new ByteReader(hexBytes('00 00 00 80')).u32()).toBe(0x80000000);
  });

  test('u48 helpers', () => {
    const b = new Uint8Array(8);
    writeU48(b, 1, 0x1234567890ab);
    expect(hexString(b)).toBe('00 AB 90 78 56 34 12 00');
    expect(readU48(b, 1)).toBe(0x1234567890ab);
  });

  test('truncation throws a ByteFormatError naming the field', () => {
    const r = new ByteReader(hexBytes('01'));
    expect(() => r.u16('channel id')).toThrow(ByteFormatError);
    expect(() => r.u16('channel id')).toThrow(/channel id/);
  });

  test('writes outside the range are refused', () => {
    expect(() => new ByteWriter().u8(256)).toThrow(RangeError);
    expect(() => new ByteWriter().u48(0x1000000000000)).toThrow(RangeError);
    expect(() => new ByteWriter().string8('x'.repeat(256))).toThrow(RangeError);
    expect(() => new ByteWriter().u32(-1)).toThrow(RangeError);
  });

  test('hex, equality, concatenation and payloads', () => {
    expect(hexString(hexBytes('0a0B 0c'))).toBe('0A 0B 0C');
    expect(() => hexBytes('abc')).toThrow(ByteFormatError);
    expect(bytesEqual([1, 2], hexBytes('01 02'))).toBe(true);
    expect(bytesEqual([1, 2], [1])).toBe(false);
    expect(hexString(concatBytes([1], [2, 3], []))).toBe('01 02 03');
    expect(hexString(toPayload('hi'))).toBe('68 69');
    expect(toPayload(undefined).length).toBe(0);
    const bytes = hexBytes('01');
    expect(toPayload(bytes)).toBe(bytes);
  });
});
