/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * Offline credential verification against the issuer keys a receiver
 * holds. Ports `credential_verifier.dart`, over WebCrypto: HMAC-SHA256
 * (compared in constant time) and Ed25519, both asynchronous.
 */

import { Name, type NameLike, toName } from '../name.ts';
import { ProtocolError, StatusCode, SwitchboardError } from '../status.ts';
import { type Logger, noopLogger } from '../util/logger.ts';
import {
  type CredentialVerifier as CredentialVerifierContract,
  SignatureKind,
} from './contract.ts';
import { Credential, signatureKindName } from './credential.ts';
import {
  type CredentialIssuer,
  checkHmacKey,
  hmacSign,
  importHmacKey,
  issuerVerificationKey,
} from './credential-issuer.ts';
import { constantTimeEquals, ed25519PublicKeyLength, ed25519Verify } from './ed25519.ts';

const logName = 'Switchboard.Identity';

/**
 * Keys by key id: a `Map` (or any iterable of pairs) with `Name` or text
 * keys, or a plain object whose property names are the key ids.
 */
export type CredentialKeys =
  | Iterable<readonly [NameLike, Uint8Array]>
  | Readonly<Record<string, Uint8Array>>;

/** What a {@link CredentialVerifier} takes. */
export interface CredentialVerifierOptions {
  /** HMAC-SHA256 keys by key id; each at least `CredentialIssuer.minHmacKeyLength` bytes. */
  readonly hmacKeys?: CredentialKeys | undefined;
  /** Ed25519 public keys (32 bytes) by key id. */
  readonly ed25519Keys?: CredentialKeys | undefined;
  /** Largest number of verified credentials kept; default {@link CredentialVerifier.defaultCacheSize}; 0 disables the cache. */
  readonly cacheSize?: number | undefined;
  /** Wall time in milliseconds since the epoch, for expiry when a call gives no `now`; default `Date.now`. */
  readonly wallTime?: (() => number) | undefined;
  /** Where the causes of refusals go (logger `Switchboard.Identity`, level `debug`); default discards. */
  readonly logger?: Logger | undefined;
}

interface VerifierKey {
  readonly kind: SignatureKind;
  readonly bytes: Uint8Array;
  /** The HMAC key imported into WebCrypto, once needed. */
  hmac?: Promise<CryptoKey>;
}

/** The pairs of [keys]. */
function entries(keys: CredentialKeys): [Name, Uint8Array][] {
  const pairs: Iterable<readonly [NameLike, Uint8Array]> =
    Symbol.iterator in keys
      ? (keys as Iterable<readonly [NameLike, Uint8Array]>)
      : Object.entries(keys as Readonly<Record<string, Uint8Array>>);
  return [...pairs].map(([id, key]) => [toName(id), key]);
}

/** [bytes] as a map key: one code unit per byte. */
function bytesKey(bytes: Uint8Array): string {
  let key = '';
  for (const b of bytes) {
    key += String.fromCharCode(b);
  }
  return key;
}

function invalid(): SwitchboardError {
  return SwitchboardError.of(StatusCode.unauthenticated, 'invalid credential');
}

/**
 * Checks credentials offline against the issuer keys it holds, by key id.
 *
 * A receiver keeps the current and the previous issuer key during a
 * rotation: add the new key under its own id, and remove the old one once
 * the credentials it signed have expired.
 *
 * Verified credentials are cached by their bytes (least recently used,
 * {@link cacheSize} entries), so the cost is one signature check per
 * credential, not per channel or connection; concurrent checks of the same
 * bytes share one signature check. Expiry is checked on every call.
 * Changing the keys clears the cache.
 *
 * Implements the contract's `CredentialVerifier`; the package exports this
 * class under that name.
 */
export class CredentialVerifier implements CredentialVerifierContract {
  /** Default {@link cacheSize}. */
  static readonly defaultCacheSize = 1024;

  /** Largest number of verified credentials kept; 0: none. */
  readonly cacheSize: number;

  /** The wall time expiry is checked at when a call gives none (ms since the epoch). */
  readonly wallTime: () => number;

  readonly #logger: Logger;
  readonly #keys = new Map<string, VerifierKey>();
  // Insertion ordered: least recently used first.
  readonly #cache = new Map<string, Credential>();
  readonly #pending = new Map<string, Promise<Credential | undefined>>();
  // Bumped whenever the keys change, so that a check that started under
  // other keys is not cached or trusted.
  #epoch = 0;

  /**
   * A verifier with the given HMAC keys and Ed25519 public keys, by key id.
   *
   * @throws {RangeError} for an HMAC key shorter than
   *   `CredentialIssuer.minHmacKeyLength`, an Ed25519 key that is not 32
   *   bytes, a key id given for both kinds, a key id `Name.of` refuses, or
   *   a cache size that is not a non-negative integer.
   */
  constructor(options: CredentialVerifierOptions = {}) {
    const cacheSize = options.cacheSize ?? CredentialVerifier.defaultCacheSize;
    if (!Number.isInteger(cacheSize) || cacheSize < 0) {
      throw new RangeError(`cacheSize must be a non-negative integer, got ${cacheSize}`);
    }
    this.cacheSize = cacheSize;
    this.wallTime = options.wallTime ?? Date.now;
    this.#logger = options.logger ?? noopLogger;
    const hmac = entries(options.hmacKeys ?? []);
    for (const [id, key] of hmac) {
      this.addHmacKey(id, key);
    }
    for (const [id, key] of entries(options.ed25519Keys ?? [])) {
      if (hmac.some(([other]) => other.equals(id))) {
        throw new RangeError(`key id ${id} used twice`);
      }
      this.addEd25519Key(id, key);
    }
  }

  /** The key ids this verifier knows, in the order they were added. */
  get keyIds(): Name[] {
    return [...this.#keys.keys()].map((key) => Name.fromKey(key));
  }

  /** Number of credentials in the cache. */
  get cachedCount(): number {
    return this.#cache.size;
  }

  /**
   * Adds (or replaces) the HMAC-SHA256 key [keyId] (copied).
   *
   * @throws {RangeError} for a key shorter than `CredentialIssuer.minHmacKeyLength`.
   */
  addHmacKey(keyId: NameLike, key: Uint8Array): void {
    checkHmacKey(key);
    this.#setKey(toName(keyId), { kind: SignatureKind.hmacSha256, bytes: key.slice() });
  }

  /**
   * Adds (or replaces) the Ed25519 public key [keyId] (copied).
   *
   * @throws {RangeError} for a key that is not 32 bytes.
   */
  addEd25519Key(keyId: NameLike, publicKey: Uint8Array): void {
    if (publicKey.length !== ed25519PublicKeyLength) {
      throw new RangeError(
        `Ed25519 public keys are ${ed25519PublicKeyLength} bytes, got ${publicKey.length}`,
      );
    }
    this.#setKey(toName(keyId), { kind: SignatureKind.ed25519, bytes: publicKey.slice() });
  }

  /**
   * Adds the key that verifies what [issuer] issues, under its key id: its
   * HMAC key, or its Ed25519 public key. For a mesh whose authority runs in
   * the same process (tests, a single process deployment).
   */
  async addIssuer(issuer: CredentialIssuer): Promise<void> {
    const key = await issuerVerificationKey(issuer);
    if (issuer.signatureKind === SignatureKind.hmacSha256) {
      this.addHmacKey(issuer.keyId, key);
    } else {
      this.addEd25519Key(issuer.keyId, key);
    }
  }

  /** Removes the key [keyId]; credentials it signed no longer verify. Returns whether there was one. */
  removeKey(keyId: NameLike): boolean {
    const removed = this.#keys.delete(toName(keyId).key);
    if (removed) {
      this.#changed();
    }
    return removed;
  }

  #setKey(keyId: Name, key: VerifierKey): void {
    this.#keys.set(keyId.key, key);
    this.#changed();
  }

  #changed(): void {
    this.#epoch++;
    this.#cache.clear();
    this.#pending.clear();
  }

  /**
   * The credential [bytes] verified earlier, if it is still cached and not
   * expired at `now` (ms since the epoch; default {@link wallTime});
   * undefined otherwise, in which case {@link verify} is the way to check
   * it. Synchronous and never throws, for paths that cannot wait, such as
   * a request handler that receives a credential relayed by another
   * instance. Does not refresh the entry's place in the cache.
   */
  cached(bytes: Uint8Array, options: { now?: number | undefined } = {}): Credential | undefined {
    if (bytes.length > Credential.maxLength) {
      return undefined;
    }
    const credential = this.#cache.get(bytesKey(bytes));
    if (credential === undefined || credential.isExpired(options.now ?? this.wallTime())) {
      return undefined;
    }
    return credential;
  }

  /**
   * Verifies [bytes] as a credential at `now` (ms since the epoch; default
   * {@link wallTime}) and returns it.
   *
   * @throws {SwitchboardError} (as a rejection) `UNAUTHENTICATED` with the
   *   generic reason `invalid credential` when the bytes are malformed, the
   *   key id is unknown, the signature kind is not the key's, the signature
   *   does not verify, an HMAC credential names a holder key (HMAC
   *   credentials are bearer only), or the credential has expired. The
   *   cause goes to the logger at `debug` (`Switchboard.Identity`), never to
   *   the peer.
   */
  async verify(bytes: Uint8Array, options: { now?: number | undefined } = {}): Promise<Credential> {
    if (bytes.length > Credential.maxLength) {
      this.#log(`malformed credential: credential of ${bytes.length} bytes exceeds the maximum`);
      throw invalid();
    }
    const key = bytesKey(bytes);
    let credential = this.#cache.get(key);
    if (credential !== undefined) {
      // Most recently used last.
      this.#cache.delete(key);
      this.#cache.set(key, credential);
    } else {
      credential = await this.#verifyShared(key, bytes.slice());
      if (credential === undefined) {
        throw invalid();
      }
    }
    if (credential.isExpired(options.now ?? this.wallTime())) {
      this.#log(`credential of "${credential.identity}" expired`);
      throw invalid();
    }
    return credential;
  }

  async #verifyShared(key: string, bytes: Uint8Array): Promise<Credential | undefined> {
    while (true) {
      const epoch = this.#epoch;
      let pending = this.#pending.get(key);
      if (pending === undefined) {
        pending = this.#check(bytes);
        this.#pending.set(key, pending);
      }
      const credential = await pending;
      if (this.#pending.get(key) === pending) {
        this.#pending.delete(key);
      }
      if (epoch !== this.#epoch) {
        // The keys changed meanwhile: check again under the new ones.
        continue;
      }
      if (credential !== undefined && this.cacheSize > 0) {
        this.#cache.set(key, credential);
        while (this.#cache.size > this.cacheSize) {
          const oldest = this.#cache.keys().next();
          if (oldest.done === true) {
            break;
          }
          this.#cache.delete(oldest.value);
        }
      }
      return credential;
    }
  }

  /** The credential in [bytes] if its signature verifies, else undefined (the cause logged). Never rejects. */
  async #check(bytes: Uint8Array): Promise<Credential | undefined> {
    let credential: Credential;
    try {
      credential = Credential.decode(bytes);
    } catch (e) {
      this.#log(
        `malformed credential: ${e instanceof ProtocolError ? e.status.reason : String(e)}`,
      );
      return undefined;
    }
    const key = this.#keys.get(credential.keyId.key);
    if (key === undefined) {
      this.#log(`credential of "${credential.identity}": unknown key id ${credential.keyId}`);
      return undefined;
    }
    if (key.kind !== credential.signatureKind) {
      this.#log(
        `credential of "${credential.identity}": signed with ` +
          `${signatureKindName(credential.signatureKind)}, key ${credential.keyId} is ` +
          `${signatureKindName(key.kind)}`,
      );
      return undefined;
    }
    const signed = credential.signedBytes;
    let valid: boolean;
    try {
      if (key.kind === SignatureKind.hmacSha256) {
        if (!credential.isBearer) {
          this.#log(`credential of "${credential.identity}": HMAC with a holder key`);
          return undefined;
        }
        key.hmac ??= importHmacKey(key.bytes);
        valid = constantTimeEquals(await hmacSign(await key.hmac, signed), credential.signature);
      } else {
        valid = await ed25519Verify(key.bytes, signed, credential.signature);
      }
    } catch (e) {
      this.#logger.log('warn', logName, `credential of "${credential.identity}": check failed`, e);
      return undefined;
    }
    if (!valid) {
      this.#log(`credential of "${credential.identity}": bad signature`);
      return undefined;
    }
    return credential;
  }

  #log(message: string): void {
    this.#logger.log('debug', logName, message);
  }

  /** The key ids and the cache's size; never a key. */
  toString(): string {
    return `CredentialVerifier(keys ${this.keyIds.join(', ')}, ${this.#cache.size} cached)`;
  }
}
