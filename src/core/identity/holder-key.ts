/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * The holder's Ed25519 key pair over WebCrypto. Ports `holder_key.dart`.
 * The contract's `HolderKey` is the shape `MuxConnection.identify` and the
 * node take (a public key and an async `sign`), so that a key kept
 * elsewhere (a hardware token, another process) can stand in;
 * {@link Ed25519HolderKey} is the implementation the package provides. A
 * bearer credential has no holder key: pass none.
 */

import type { HolderKey } from './contract.ts';
import {
  ed25519KeyPairFromSeed,
  ed25519PublicKeyLength,
  ed25519SeedLength,
  ed25519Sign,
} from './ed25519.ts';
import { secureRandomBytes } from './secure-random.ts';

/**
 * The Ed25519 key pair a credential holder keeps to prove possession of a
 * credential that names its public key (`Credential.holderKey`).
 *
 * The private key never leaves the holder, nor WebCrypto: the authority
 * issues the credential for {@link publicKey}, and the holder signs the
 * `IDENT` proof of every connection it identifies on
 * (`MuxConnection.identify`), which binds the proof to that connection's
 * nonces. Dart's `HolderKey`; named apart from the contract's `HolderKey`
 * interface, which it implements.
 */
export class Ed25519HolderKey implements HolderKey {
  readonly #privateKey: CryptoKey;

  /** The 32-byte public key, the `Credential.holderKey` of the credentials issued for this holder. Must not be modified. */
  readonly publicKey: Uint8Array;

  private constructor(privateKey: CryptoKey, publicKey: Uint8Array) {
    this.#privateKey = privateKey;
    this.publicKey = publicKey;
  }

  /** A new random key pair, from a 32-byte seed of the platform's secure random source. */
  static generate(): Promise<Ed25519HolderKey> {
    return Ed25519HolderKey.fromSeed(secureRandomBytes(ed25519SeedLength));
  }

  /**
   * The key pair of the 32-byte Ed25519 [seed] (the private key of RFC
   * 8032). The seed is imported as PKCS#8 into a key that cannot be
   * exported again.
   *
   * @throws {RangeError} (as a rejection) if [seed] is not 32 bytes.
   */
  static async fromSeed(seed: Uint8Array): Promise<Ed25519HolderKey> {
    const keys = await ed25519KeyPairFromSeed(seed);
    return new Ed25519HolderKey(keys.privateKey, keys.publicKey);
  }

  /**
   * Wraps a WebCrypto Ed25519 [keyPair], such as one of
   * `crypto.subtle.generateKey('Ed25519', false, ['sign', 'verify'])`, whose
   * private key may be non-extractable (and kept, for instance, in
   * IndexedDB). The public key is read with a raw export.
   *
   * @throws {TypeError} (as a rejection) for a key pair that is not
   *   Ed25519, a private key that cannot sign, or a public key that cannot
   *   be exported.
   */
  static async fromKeyPair(keyPair: CryptoKeyPair): Promise<Ed25519HolderKey> {
    const { privateKey, publicKey } = keyPair;
    if (
      privateKey.algorithm.name !== 'Ed25519' ||
      publicKey.algorithm.name !== 'Ed25519' ||
      privateKey.type !== 'private' ||
      !privateKey.usages.includes('sign')
    ) {
      throw new TypeError('not an Ed25519 key pair that can sign');
    }
    let raw: Uint8Array;
    try {
      raw = new Uint8Array(await crypto.subtle.exportKey('raw', publicKey));
    } catch (e) {
      throw new TypeError('the Ed25519 public key cannot be exported', { cause: e });
    }
    if (raw.length !== ed25519PublicKeyLength) {
      throw new TypeError(`an Ed25519 public key is ${ed25519PublicKeyLength} bytes`);
    }
    return new Ed25519HolderKey(privateKey, raw);
  }

  /** The Ed25519 signature (64 bytes) of [message]. */
  sign(message: Uint8Array): Promise<Uint8Array> {
    return ed25519Sign(message, this.#privateKey);
  }
}
