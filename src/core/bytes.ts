/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { Name } from './name.ts';

const utf8Encoder = new TextEncoder();
const utf8Decoder = new TextDecoder('utf-8', { fatal: false });

/** Largest `u48` value. Every Switchboard integer fits a JavaScript number exactly. */
export const maxU48 = 0xffffffffffff;

/** A truncated or malformed byte structure. */
export class ByteFormatError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ByteFormatError';
  }
}

/**
 * Sequential little-endian reader over a byte array.
 *
 * Every read throws {@link ByteFormatError} when the input is too short, so
 * codecs can decode without length checks of their own.
 */
export class ByteReader {
  readonly bytes: Uint8Array;
  offset: number;

  constructor(bytes: Uint8Array, offset = 0) {
    this.bytes = bytes;
    this.offset = offset;
  }

  get remaining(): number {
    return this.bytes.length - this.offset;
  }

  get isDone(): boolean {
    return this.offset >= this.bytes.length;
  }

  #need(n: number, what: string): void {
    if (this.offset + n > this.bytes.length) {
      throw new ByteFormatError(`truncated ${what} at offset ${this.offset}`);
    }
  }

  u8(what = 'u8'): number {
    this.#need(1, what);
    return this.bytes[this.offset++] as number;
  }

  u16(what = 'u16'): number {
    this.#need(2, what);
    const b = this.bytes;
    const o = this.offset;
    this.offset += 2;
    return (b[o] as number) | ((b[o + 1] as number) << 8);
  }

  u24(what = 'u24'): number {
    this.#need(3, what);
    const b = this.bytes;
    const o = this.offset;
    this.offset += 3;
    return (b[o] as number) | ((b[o + 1] as number) << 8) | ((b[o + 2] as number) << 16);
  }

  /** Unsigned: `>>> 0` keeps the top bit from making the value negative. */
  u32(what = 'u32'): number {
    this.#need(4, what);
    const b = this.bytes;
    const o = this.offset;
    this.offset += 4;
    return (
      ((b[o] as number) |
        ((b[o + 1] as number) << 8) |
        ((b[o + 2] as number) << 16) |
        ((b[o + 3] as number) << 24)) >>>
      0
    );
  }

  /** Assembled without shifting past 32 bits: exact as a JavaScript number. */
  u48(what = 'u48'): number {
    this.#need(6, what);
    const lo = this.u32(what);
    const hi = this.u16(what);
    return lo + hi * 0x100000000;
  }

  name(what = 'name'): Name {
    this.#need(Name.byteLength, what);
    const n = Name.fromBytes(this.bytes.subarray(this.offset, this.offset + Name.byteLength));
    this.offset += Name.byteLength;
    return n;
  }

  /** A `u8` length followed by that many bytes of UTF-8. */
  string8(what = 'string'): string {
    const len = this.u8(`${what} length`);
    this.#need(len, what);
    const s = utf8Decoder.decode(this.bytes.subarray(this.offset, this.offset + len));
    this.offset += len;
    return s;
  }

  /** Exactly [n] bytes as a view. */
  take(n: number, what = 'bytes'): Uint8Array {
    this.#need(n, what);
    const v = this.bytes.subarray(this.offset, this.offset + n);
    this.offset += n;
    return v;
  }

  /** Everything left, as a view. Empty if nothing is left. */
  rest(): Uint8Array {
    const v = this.bytes.subarray(this.offset);
    this.offset = this.bytes.length;
    return v;
  }
}

/** Growable little-endian writer. */
export class ByteWriter {
  #buf: Uint8Array;
  #len = 0;

  constructor(initialCapacity = 64) {
    this.#buf = new Uint8Array(initialCapacity < 8 ? 8 : initialCapacity);
  }

  get length(): number {
    return this.#len;
  }

  #ensure(n: number): void {
    if (this.#len + n <= this.#buf.length) {
      return;
    }
    let cap = this.#buf.length * 2;
    while (cap < this.#len + n) {
      cap *= 2;
    }
    const grown = new Uint8Array(cap);
    grown.set(this.#buf.subarray(0, this.#len));
    this.#buf = grown;
  }

  u8(v: number): this {
    checkRange(v, 0xff, 'u8');
    this.#ensure(1);
    this.#buf[this.#len++] = v;
    return this;
  }

  u16(v: number): this {
    checkRange(v, 0xffff, 'u16');
    this.#ensure(2);
    this.#buf[this.#len++] = v & 0xff;
    this.#buf[this.#len++] = (v >> 8) & 0xff;
    return this;
  }

  u24(v: number): this {
    checkRange(v, 0xffffff, 'u24');
    this.#ensure(3);
    this.#buf[this.#len++] = v & 0xff;
    this.#buf[this.#len++] = (v >> 8) & 0xff;
    this.#buf[this.#len++] = (v >> 16) & 0xff;
    return this;
  }

  u32(v: number): this {
    checkRange(v, 0xffffffff, 'u32');
    this.#ensure(4);
    this.#buf[this.#len++] = v & 0xff;
    this.#buf[this.#len++] = (v >>> 8) & 0xff;
    this.#buf[this.#len++] = (v >>> 16) & 0xff;
    this.#buf[this.#len++] = (v >>> 24) & 0xff;
    return this;
  }

  /** Split without shifting past 32 bits. */
  u48(v: number): this {
    checkRange(v, maxU48, 'u48');
    const hi = Math.floor(v / 0x100000000);
    const lo = v - hi * 0x100000000;
    this.u32(lo);
    this.u16(hi);
    return this;
  }

  name(n: Name): this {
    this.#ensure(Name.byteLength);
    n.writeTo(this.#buf, this.#len);
    this.#len += Name.byteLength;
    return this;
  }

  /** A `u8` length followed by UTF-8. @throws {RangeError} over 255 bytes. */
  string8(s: string): this {
    const encoded = utf8Encoder.encode(s);
    if (encoded.length > 0xff) {
      throw new RangeError('string longer than 255 bytes');
    }
    this.u8(encoded.length);
    this.bytes(encoded);
    return this;
  }

  bytes(b: ArrayLike<number>): this {
    this.#ensure(b.length);
    this.#buf.set(b, this.#len);
    this.#len += b.length;
    return this;
  }

  /** The written bytes as a copy of exactly {@link length} bytes. */
  toBytes(): Uint8Array {
    return this.#buf.slice(0, this.#len);
  }
}

function checkRange(v: number, max: number, what: string): void {
  if (!Number.isInteger(v) || v < 0 || v > max) {
    throw new RangeError(`${v} does not fit in ${what}`);
  }
}

/** Little-endian `u32` at [offset] of [bytes], unsigned. */
export function readU32(bytes: Uint8Array, offset: number): number {
  return (
    ((bytes[offset] as number) |
      ((bytes[offset + 1] as number) << 8) |
      ((bytes[offset + 2] as number) << 16) |
      ((bytes[offset + 3] as number) << 24)) >>>
    0
  );
}

/** Writes [value] as a little-endian `u32` at [offset] of [bytes]. */
export function writeU32(bytes: Uint8Array, offset: number, value: number): void {
  bytes[offset] = value & 0xff;
  bytes[offset + 1] = (value >>> 8) & 0xff;
  bytes[offset + 2] = (value >>> 16) & 0xff;
  bytes[offset + 3] = (value >>> 24) & 0xff;
}

/** Little-endian `u48` at [offset] of [bytes]. */
export function readU48(bytes: Uint8Array, offset: number): number {
  return readU32(bytes, offset) + readU16(bytes, offset + 4) * 0x100000000;
}

/** Writes [value], a `u48`, little endian at [offset] of [bytes]. */
export function writeU48(bytes: Uint8Array, offset: number, value: number): void {
  const hi = Math.floor(value / 0x100000000);
  writeU32(bytes, offset, value - hi * 0x100000000);
  bytes[offset + 4] = hi & 0xff;
  bytes[offset + 5] = (hi >> 8) & 0xff;
}

/** Little-endian `u16` at [offset] of [bytes]. */
export function readU16(bytes: Uint8Array, offset: number): number {
  return (bytes[offset] as number) | ((bytes[offset + 1] as number) << 8);
}

/** Parses a hex string like `02 02 00 01` (spaces optional) into bytes. For test vectors. */
export function hexBytes(hex: string): Uint8Array {
  const clean = hex.replace(/\s+/g, '');
  if (clean.length % 2 !== 0) {
    throw new ByteFormatError(`odd length hex: ${hex}`);
  }
  const out = new Uint8Array(clean.length / 2);
  for (let i = 0; i < out.length; i++) {
    const v = Number.parseInt(clean.substring(i * 2, i * 2 + 2), 16);
    if (Number.isNaN(v)) {
      throw new ByteFormatError(`invalid hex: ${hex}`);
    }
    out[i] = v;
  }
  return out;
}

/** Formats bytes as upper-case hex separated by spaces. */
export function hexString(bytes: ArrayLike<number>): string {
  return Array.from(bytes, (b) => b.toString(16).padStart(2, '0').toUpperCase()).join(' ');
}

/** Whether two byte arrays hold the same bytes. */
export function bytesEqual(a: ArrayLike<number>, b: ArrayLike<number>): boolean {
  if (a.length !== b.length) {
    return false;
  }
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) {
      return false;
    }
  }
  return true;
}

/** One array holding the given arrays in order. */
export function concatBytes(...parts: ArrayLike<number>[]): Uint8Array {
  let length = 0;
  for (const p of parts) {
    length += p.length;
  }
  const out = new Uint8Array(length);
  let offset = 0;
  for (const p of parts) {
    out.set(p, offset);
    offset += p.length;
  }
  return out;
}

/** UTF-8 bytes of [text]. */
export function utf8Encode(text: string): Uint8Array {
  return utf8Encoder.encode(text);
}

/** [bytes] decoded as UTF-8, malformed sequences replaced. */
export function utf8Decode(bytes: Uint8Array): string {
  return utf8Decoder.decode(bytes);
}

/**
 * A payload as the API accepts it: bytes, or text, which is sent as UTF-8.
 * `undefined` is the empty payload.
 */
export type PayloadLike = Uint8Array | string | undefined;

const emptyPayload = new Uint8Array(0);

/** [payload] as bytes. Text is encoded as UTF-8; undefined is empty. */
export function toPayload(payload: PayloadLike): Uint8Array {
  if (payload === undefined) {
    return emptyPayload;
  }
  return typeof payload === 'string' ? utf8Encoder.encode(payload) : payload;
}
