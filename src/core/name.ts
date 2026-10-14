/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

const utf8Encoder = new TextEncoder();
const utf8Decoder = new TextDecoder('utf-8', { fatal: false });

/**
 * An 8-byte name: UTF-8, zero padded on the right, compared bytewise.
 *
 * Used for procedure ids and service types. A name is immutable and cheap
 * to compare; the wire form is exactly 8 bytes.
 *
 * Names beginning with `_` are reserved for the protocol.
 *
 * JavaScript compares objects by reference, so a `Name` is not usable as a
 * `Map` key directly: use {@link Name.key}, a string that is equal exactly
 * when the names are, or {@link Name.equals}.
 */
export class Name {
  static readonly byteLength = 8;

  readonly #bytes: Uint8Array;

  /**
   * The name as a map key: the 8 wire bytes as a string of 8 code units
   * (0 to 255 each). Equal strings mean equal names.
   */
  readonly key: string;

  private constructor(bytes: Uint8Array) {
    this.#bytes = bytes;
    this.key = String.fromCharCode(...bytes);
  }

  /** The empty (all zero) name. */
  static readonly empty: Name = new Name(new Uint8Array(Name.byteLength));

  /**
   * Creates a name from a string of at most 8 bytes of UTF-8.
   *
   * @throws {RangeError} if the encoded string is longer than 8 bytes or
   * contains a zero byte.
   */
  static of(text: string): Name {
    const encoded = utf8Encoder.encode(text);
    if (encoded.length > Name.byteLength) {
      throw new RangeError(`name "${text}" longer than ${Name.byteLength} bytes of UTF-8`);
    }
    if (encoded.includes(0)) {
      throw new RangeError(`name "${text}" contains a zero byte`);
    }
    const bytes = new Uint8Array(Name.byteLength);
    bytes.set(encoded);
    return new Name(bytes);
  }

  /**
   * Creates a name from its 8 wire bytes (copied).
   *
   * @throws {RangeError} if [bytes] is not 8 long or if a non-zero byte
   * follows a zero byte.
   */
  static fromBytes(bytes: ArrayLike<number>): Name {
    if (bytes.length !== Name.byteLength) {
      throw new RangeError(`name must be ${Name.byteLength} bytes`);
    }
    const copy = Uint8Array.from(bytes);
    let seenZero = false;
    for (const b of copy) {
      if (b === 0) {
        seenZero = true;
      } else if (seenZero) {
        throw new RangeError('non-zero byte after zero byte in name');
      }
    }
    return new Name(copy);
  }

  /** A name from its {@link key}. The key must come from a Name. */
  static fromKey(key: string): Name {
    return Name.fromBytes(Uint8Array.from(key, (c) => c.charCodeAt(0)));
  }

  /** The 8 wire bytes. The returned view must not be modified. */
  get bytes(): Uint8Array {
    return this.#bytes;
  }

  get isEmpty(): boolean {
    return this.#bytes[0] === 0;
  }

  /** True for names starting with `_`, reserved for the protocol. */
  get isReserved(): boolean {
    return this.#bytes[0] === 0x5f;
  }

  /** Length of the name in bytes, without padding. */
  get length(): number {
    let n = 0;
    while (n < Name.byteLength && this.#bytes[n] !== 0) {
      n++;
    }
    return n;
  }

  /** Writes the 8 bytes into [out] at [offset]. */
  writeTo(out: Uint8Array, offset: number): void {
    out.set(this.#bytes, offset);
  }

  equals(other: unknown): boolean {
    if (!(other instanceof Name)) {
      return false;
    }
    for (let i = 0; i < Name.byteLength; i++) {
      if (this.#bytes[i] !== other.#bytes[i]) {
        return false;
      }
    }
    return true;
  }

  /** The text of the name, malformed UTF-8 replaced. */
  toString(): string {
    return utf8Decoder.decode(this.#bytes.subarray(0, this.length));
  }

  toJSON(): string {
    return this.toString();
  }
}

/** A name, or the text of one. */
export type NameLike = Name | string;

/** [name] as a {@link Name}: itself, or {@link Name.of} its text. */
export function toName(name: NameLike): Name {
  return typeof name === 'string' ? Name.of(name) : name;
}
