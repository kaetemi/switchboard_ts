/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * Ed25519 over WebCrypto (`crypto.subtle`), the same on Node 22 and later,
 * Bun, Deno and current browsers. Ports `ed25519.dart`, which uses the pure
 * Dart implementation of `package:cryptography` so that no platform crypto
 * is involved; here the platform's is the only one, and every operation is
 * asynchronous. Internal to the package; not exported.
 */

import { hexBytes } from '../bytes.ts';

/** Length of an Ed25519 public key. */
export const ed25519PublicKeyLength = 32;

/** Length of an Ed25519 signature. */
export const ed25519SignatureLength = 64;

/** Length of an Ed25519 seed: the private key. */
export const ed25519SeedLength = 32;

const algorithm = { name: 'Ed25519' } as const;

/** The PKCS#8 header of an Ed25519 private key (RFC 8410), before the 32 seed bytes. */
const pkcs8Prefix = hexBytes('30 2E 02 01 00 30 05 06 03 2B 65 70 04 22 04 20');

/** [bytes] as WebCrypto takes them: on an `ArrayBuffer`, copied when on a shared one. */
function buffer(bytes: Uint8Array): Uint8Array<ArrayBuffer> {
  return bytes.buffer instanceof ArrayBuffer
    ? (bytes as Uint8Array<ArrayBuffer>)
    : new Uint8Array(bytes);
}

/** An Ed25519 key pair: the private key stays inside WebCrypto. */
export interface Ed25519KeyPair {
  /** The non-extractable signing key. */
  readonly privateKey: CryptoKey;
  /** The 32-byte public key. */
  readonly publicKey: Uint8Array;
}

/**
 * The key pair of the 32-byte Ed25519 [seed] (the private key of RFC
 * 8032).
 *
 * @throws {RangeError} if [seed] is not 32 bytes.
 */
export async function ed25519KeyPairFromSeed(seed: Uint8Array): Promise<Ed25519KeyPair> {
  if (seed.length !== ed25519SeedLength) {
    throw new RangeError(`an Ed25519 seed is ${ed25519SeedLength} bytes, got ${seed.length}`);
  }
  const pkcs8 = new Uint8Array(pkcs8Prefix.length + seed.length);
  pkcs8.set(pkcs8Prefix);
  pkcs8.set(seed, pkcs8Prefix.length);
  // Imported extractable once to read the public key out of its JWK, then
  // again without export, so that the seed cannot be read back.
  const extractable = await crypto.subtle.importKey('pkcs8', pkcs8, algorithm, true, ['sign']);
  const jwk = await crypto.subtle.exportKey('jwk', extractable);
  pkcs8.fill(0);
  if (jwk.x === undefined || jwk.d === undefined) {
    throw new TypeError('WebCrypto did not export the Ed25519 key as expected');
  }
  const publicKey = fromBase64Url(jwk.x);
  const privateKey = await crypto.subtle.importKey(
    'jwk',
    { kty: 'OKP', crv: 'Ed25519', d: jwk.d, x: jwk.x },
    algorithm,
    false,
    ['sign'],
  );
  return { privateKey, publicKey };
}

/** The Ed25519 signature (64 bytes) of [message] with [privateKey]. */
export async function ed25519Sign(message: Uint8Array, privateKey: CryptoKey): Promise<Uint8Array> {
  return new Uint8Array(await crypto.subtle.sign(algorithm, privateKey, buffer(message)));
}

/**
 * Whether [signature] is a valid signature of [message] by [publicKey].
 * False for keys and signatures of the wrong length, and for anything the
 * implementation throws on.
 */
export async function ed25519Verify(
  publicKey: Uint8Array,
  message: Uint8Array,
  signature: Uint8Array,
): Promise<boolean> {
  if (publicKey.length !== ed25519PublicKeyLength || signature.length !== ed25519SignatureLength) {
    return false;
  }
  try {
    const key = await crypto.subtle.importKey('raw', buffer(publicKey), algorithm, false, [
      'verify',
    ]);
    return await crypto.subtle.verify(algorithm, key, buffer(signature), buffer(message));
  } catch {
    return false;
  }
}

/** Compares in time independent of where the first difference is. */
export function constantTimeEquals(a: ArrayLike<number>, b: ArrayLike<number>): boolean {
  if (a.length !== b.length) {
    return false;
  }
  let diff = 0;
  for (let i = 0; i < a.length; i++) {
    diff |= (a[i] as number) ^ (b[i] as number);
  }
  return diff === 0;
}

/** Decodes unpadded base64url, as JWK carries keys. */
function fromBase64Url(text: string): Uint8Array {
  const base64 = text.replace(/-/g, '+').replace(/_/g, '/');
  const binary = atob(base64 + '='.repeat((4 - (base64.length % 4)) % 4));
  const out = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) {
    out[i] = binary.charCodeAt(i);
  }
  return out;
}
