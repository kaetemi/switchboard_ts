/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * Cryptographically secure random bytes on every runtime: exported for
 * bindings, which draw their `NONCE` from it as the frame mux does. Ports
 * `secure_random.dart` (its web half: the Web Crypto API is the source on
 * Node, Bun, Deno and browsers alike).
 */

/** The most bytes `crypto.getRandomValues` fills in one call. */
const quota = 65536;

/**
 * [length] cryptographically secure random bytes, from
 * `globalThis.crypto.getRandomValues`, in calls of at most 65536 bytes (the
 * Web Crypto quota). Never falls back to a source that is not secure.
 *
 * @throws {RangeError} for a negative or fractional [length].
 * @throws {TypeError} where the runtime has no Web Crypto API.
 */
export function secureRandomBytes(length: number): Uint8Array {
  if (!Number.isInteger(length) || length < 0) {
    throw new RangeError(`length must be a non-negative integer, got ${length}`);
  }
  const source = globalThis.crypto as Crypto | undefined;
  if (source === undefined || typeof source.getRandomValues !== 'function') {
    throw new TypeError('no Web Crypto API for secure random bytes');
  }
  const bytes = new Uint8Array(length);
  for (let offset = 0; offset < length; offset += quota) {
    source.getRandomValues(bytes.subarray(offset, Math.min(offset + quota, length)));
  }
  return bytes;
}
