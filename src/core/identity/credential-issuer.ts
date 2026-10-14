/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * The mesh authority's signing key. Ports `credential_issuer.dart`, over
 * WebCrypto (HMAC-SHA256 and Ed25519); every signature is asynchronous.
 */

import { type Name, type NameLike, toName } from '../name.ts';
import type { CredentialKind, Scope as ScopeContract } from './contract.ts';
import { SignatureKind } from './contract.ts';
import { Credential, signatureKindName, signatureLength } from './credential.ts';
import { ed25519KeyPairFromSeed, ed25519PublicKeyLength, ed25519Sign } from './ed25519.ts';

/** What {@link CredentialIssuer.issue} takes. */
export interface IssueOptions {
  /** What the credential identifies. */
  readonly kind: CredentialKind;
  /** The holder's identity, at most 255 bytes of UTF-8. */
  readonly identity: string;
  /** What the holder may do; default none. */
  readonly scopes?: readonly ScopeContract[] | undefined;
  /**
   * The holder's Ed25519 public key (`Ed25519HolderKey.publicKey`) for a
   * credential that requires proof of possession; undefined for a bearer
   * credential.
   */
  readonly holderKey?: Uint8Array | undefined;
  /**
   * When it is issued, milliseconds since the epoch, rounded down to the
   * second; default the issuer's `wallTime`.
   */
  readonly issuedAt?: number | undefined;
  /**
   * How long it is valid, in milliseconds, rounded down to the second;
   * default {@link CredentialIssuer.defaultLifetime}; `null` issues a
   * credential that never expires (discouraged).
   */
  readonly lifetime?: number | null | undefined;
}

/** What the {@link CredentialIssuer} factories take beside the key. */
export interface CredentialIssuerOptions {
  /** Wall time in milliseconds since the epoch, the default issue time; default `Date.now`. */
  readonly wallTime?: (() => number) | undefined;
}

/** The internal reader of an issuer's verification key, set in the class's static block. */
let readVerificationKey: (issuer: CredentialIssuer) => Promise<Uint8Array>;

/**
 * The mesh authority's signing key: issues signed {@link Credential}s.
 *
 * Two kinds, chosen per mesh (wiki page "Polyverse Switchboard Identity and
 * Credentials"): {@link CredentialIssuer.hmac} with a key shared by the
 * mesh, cheapest, but anyone holding the key can mint and the credentials
 * are bearer only; {@link CredentialIssuer.ed25519} with the authority's key
 * pair, verified with the public key, and with holder keys for proof of
 * possession.
 */
export class CredentialIssuer {
  /** Shortest HMAC key accepted, in bytes. 32 random bytes are recommended. */
  static readonly minHmacKeyLength = 16;

  /** The default lifetime of an issued credential: 8 hours, in milliseconds. */
  static readonly defaultLifetime = 8 * 3600 * 1000;

  /** The key id written into every credential, by which receivers find the key to verify it with. */
  readonly keyId: Name;

  /** How credentials are signed. */
  readonly signatureKind: SignatureKind;

  /** The wall time the default issue time is read from (ms since the epoch). */
  readonly wallTime: () => number;

  readonly #hmacKey: Uint8Array | undefined;
  #hmacCryptoKey: Promise<CryptoKey> | undefined;
  readonly #privateKey: CryptoKey | undefined;
  #publicKey: Uint8Array | CryptoKey | undefined;

  static {
    readVerificationKey = (issuer) => issuer.#verificationKey();
  }

  private constructor(
    keyId: NameLike,
    signatureKind: SignatureKind,
    keys: {
      readonly hmacKey?: Uint8Array;
      readonly privateKey?: CryptoKey;
      readonly publicKey?: Uint8Array | CryptoKey;
    },
    options: CredentialIssuerOptions,
  ) {
    this.keyId = toName(keyId);
    this.signatureKind = signatureKind;
    this.wallTime = options.wallTime ?? Date.now;
    this.#hmacKey = keys.hmacKey;
    this.#privateKey = keys.privateKey;
    this.#publicKey = keys.publicKey;
  }

  /**
   * An issuer signing with HMAC-SHA256 under [key] (copied), known to
   * receivers as [keyId].
   *
   * @throws {RangeError} for a key shorter than
   *   {@link CredentialIssuer.minHmacKeyLength} bytes, or a key id
   *   `Name.of` refuses.
   */
  static hmac(
    keyId: NameLike,
    key: Uint8Array,
    options: CredentialIssuerOptions = {},
  ): CredentialIssuer {
    checkHmacKey(key);
    return new CredentialIssuer(keyId, SignatureKind.hmacSha256, { hmacKey: key.slice() }, options);
  }

  /**
   * An issuer signing with the WebCrypto Ed25519 [keyPair] (its private key
   * may be non-extractable), known to receivers as [keyId]. See also
   * {@link CredentialIssuer.ed25519FromSeed}.
   *
   * @throws {TypeError} for a key pair that is not Ed25519 or a private key
   *   that cannot sign; {@link publicKey} fails if the public key cannot be
   *   exported.
   */
  static ed25519(
    keyId: NameLike,
    keyPair: CryptoKeyPair,
    options: CredentialIssuerOptions = {},
  ): CredentialIssuer {
    const { privateKey, publicKey } = keyPair;
    if (
      privateKey.algorithm.name !== 'Ed25519' ||
      publicKey.algorithm.name !== 'Ed25519' ||
      privateKey.type !== 'private' ||
      !privateKey.usages.includes('sign')
    ) {
      throw new TypeError('not an Ed25519 key pair that can sign');
    }
    return new CredentialIssuer(keyId, SignatureKind.ed25519, { privateKey, publicKey }, options);
  }

  /**
   * An issuer signing with the Ed25519 key pair of the 32-byte [seed] (the
   * authority's private key, imported as PKCS#8 into a key that cannot be
   * exported again), known to receivers as [keyId].
   *
   * @throws {RangeError} (as a rejection) if [seed] is not 32 bytes.
   */
  static async ed25519FromSeed(
    keyId: NameLike,
    seed: Uint8Array,
    options: CredentialIssuerOptions = {},
  ): Promise<CredentialIssuer> {
    const name = toName(keyId);
    const keys = await ed25519KeyPairFromSeed(seed);
    return new CredentialIssuer(
      name,
      SignatureKind.ed25519,
      { privateKey: keys.privateKey, publicKey: keys.publicKey },
      options,
    );
  }

  /**
   * Issues a signed credential.
   *
   * @throws {RangeError} (as a rejection) for a holder key with an HMAC
   *   issuer (HMAC credentials are bearer only), for a lifetime that is not
   *   a positive number, and for everything the {@link Credential}
   *   constructor refuses, such as a credential too long for an `IDENT`
   *   ({@link Credential.maxLength}).
   */
  async issue(options: IssueOptions): Promise<Credential> {
    const { holderKey } = options;
    if (holderKey !== undefined && this.signatureKind === SignatureKind.hmacSha256) {
      throw new RangeError('HMAC credentials are bearer only');
    }
    const lifetime =
      options.lifetime === undefined ? CredentialIssuer.defaultLifetime : options.lifetime;
    if (lifetime !== null && !(lifetime > 0 && Number.isFinite(lifetime))) {
      throw new RangeError(`lifetime must be positive, got ${lifetime}`);
    }
    const issued = Math.floor((options.issuedAt ?? this.wallTime()) / 1000);
    const expires = lifetime === null ? 0 : issued + Math.floor(lifetime / 1000);
    const fields = {
      kind: options.kind,
      keyId: this.keyId,
      identity: options.identity,
      issuedAt: issued,
      expiresAt: expires,
      holderKey,
      scopes: options.scopes,
      signatureKind: this.signatureKind,
    };
    const draft = new Credential({
      ...fields,
      signature: new Uint8Array(signatureLength(this.signatureKind)),
    });
    return new Credential({ ...fields, signature: await this.#sign(draft.signedBytes) });
  }

  #sign(message: Uint8Array): Promise<Uint8Array> {
    const key = this.#hmacKey;
    if (key !== undefined) {
      this.#hmacCryptoKey ??= importHmacKey(key);
      return this.#hmacCryptoKey.then((k) => hmacSign(k, message));
    }
    return ed25519Sign(message, this.#privateKey as CryptoKey);
  }

  /**
   * The Ed25519 public key receivers verify with
   * (`CredentialVerifier.addEd25519Key`).
   *
   * @throws {Error} (as a rejection; Dart `StateError`) for an HMAC issuer,
   *   which has none; {@link TypeError} for a public key that cannot be
   *   exported.
   */
  async publicKey(): Promise<Uint8Array> {
    const key = this.#publicKey;
    if (this.signatureKind !== SignatureKind.ed25519 || key === undefined) {
      throw new Error('an HMAC issuer has no public key');
    }
    if (key instanceof Uint8Array) {
      return key.slice();
    }
    let raw: Uint8Array;
    try {
      raw = new Uint8Array(await crypto.subtle.exportKey('raw', key));
    } catch (e) {
      throw new TypeError('the Ed25519 public key cannot be exported', { cause: e });
    }
    if (raw.length !== ed25519PublicKeyLength) {
      throw new TypeError(`an Ed25519 public key is ${ed25519PublicKeyLength} bytes`);
    }
    this.#publicKey = raw;
    return raw.slice();
  }

  /** The key a verifier needs: the HMAC key, or the Ed25519 public key. */
  #verificationKey(): Promise<Uint8Array> {
    const key = this.#hmacKey;
    return key === undefined ? this.publicKey() : Promise.resolve(key.slice());
  }

  /** The kind and the key id; never the key. */
  toString(): string {
    return `CredentialIssuer(${signatureKindName(this.signatureKind)}, key ${this.keyId})`;
  }
}

/**
 * The key a verifier needs to check what [issuer] issues: its HMAC key, or
 * its Ed25519 public key. For `CredentialVerifier.addIssuer`; internal to
 * the package (Dart `@internal verificationKey`).
 */
export function issuerVerificationKey(issuer: CredentialIssuer): Promise<Uint8Array> {
  return readVerificationKey(issuer);
}

/**
 * @throws {RangeError} for an HMAC key shorter than
 *   {@link CredentialIssuer.minHmacKeyLength}. Internal to the package.
 */
export function checkHmacKey(key: Uint8Array): void {
  if (key.length < CredentialIssuer.minHmacKeyLength) {
    throw new RangeError(
      `HMAC keys are at least ${CredentialIssuer.minHmacKeyLength} bytes, got ${key.length}`,
    );
  }
}

/** [bytes] as WebCrypto takes them: on an `ArrayBuffer`, copied when on a shared one. */
function buffer(bytes: Uint8Array): Uint8Array<ArrayBuffer> {
  return bytes.buffer instanceof ArrayBuffer
    ? (bytes as Uint8Array<ArrayBuffer>)
    : new Uint8Array(bytes);
}

/** [key] as a WebCrypto HMAC-SHA256 signing key. Internal to the package. */
export function importHmacKey(key: Uint8Array): Promise<CryptoKey> {
  return crypto.subtle.importKey('raw', buffer(key), { name: 'HMAC', hash: 'SHA-256' }, false, [
    'sign',
  ]);
}

/** The HMAC-SHA256 (32 bytes) of [message] under [key] (from {@link importHmacKey}). Internal to the package. */
export async function hmacSign(key: CryptoKey, message: Uint8Array): Promise<Uint8Array> {
  return new Uint8Array(await crypto.subtle.sign('HMAC', key, buffer(message)));
}

/** The HMAC-SHA256 of [message] under the raw [key]. Internal to the package. */
export async function hmacSha256(key: Uint8Array, message: Uint8Array): Promise<Uint8Array> {
  return hmacSign(await importHmacKey(key), message);
}
