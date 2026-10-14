/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Credentials and connection identities without cryptography for the naming
// service's identity tests, standing in for the protocol core's credential
// codec, issuer and verifier and for the identity half of a `MuxConnection`
// until they land. Unlike the node thread's fakes (`../node/fake-identity.ts`,
// whose pattern match and peer identity are reused here), these credentials
// carry an issue time read from a wall clock, so that `RENEW` keeps their
// lifetime, and every party reads the same wall clock: a test on a
// `TestClock` passes `() => clock.now()` to the issuer, the verifier, the
// connections and the naming service's `wallTime`.

import { utf8Decode, utf8Encode } from '../../../src/core/bytes.ts';
import {
  type Credential,
  type CredentialKind,
  type CredentialVerifier,
  type PeerIdentity,
  Right,
  type Scope,
  SignatureKind,
} from '../../../src/core/identity/contract.ts';
import { Name } from '../../../src/core/name.ts';
import type {
  NamingConnection,
  NamingCredentialIssuer,
} from '../../../src/core/naming/naming-service.ts';
import { Status, StatusCode, SwitchboardError } from '../../../src/core/status.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';
import { patternMatches, peerIdentityOf } from '../node/fake-identity.ts';

/** Every fake credential by its encoding. */
const registry = new Map<string, NamingFakeCredential>();
let serial = 0;

class NamingFakeScope implements Scope {
  readonly right: Right;
  readonly pattern: Name;

  constructor(right: Right, pattern: Name) {
    this.right = right;
    this.pattern = pattern;
  }

  matches(name: Name): boolean {
    return patternMatches(this.pattern, name);
  }
}

/** A scope granting [right] over the types matching [pattern] (`Scope.of`). */
export function scopeOf(right: Right, pattern: string | Name): Scope {
  return new NamingFakeScope(right, typeof pattern === 'string' ? Name.of(pattern) : pattern);
}

interface CredentialFields {
  readonly kind: CredentialKind;
  readonly keyId: Name;
  readonly identity: string;
  readonly issuedAt: number;
  readonly expiresAt: number;
  readonly holderKey: Uint8Array | undefined;
  readonly scopes: readonly Scope[];
}

class NamingFakeCredential implements Credential {
  readonly version = 1;
  readonly kind: CredentialKind;
  readonly keyId: Name;
  readonly identity: string;
  readonly issuedAt: number;
  readonly expiresAt: number;
  readonly holderKey: Uint8Array | undefined;
  readonly scopes: readonly Scope[];
  readonly signatureKind: SignatureKind;
  readonly signature: Uint8Array;
  readonly #bytes: Uint8Array;

  constructor(fields: CredentialFields) {
    this.kind = fields.kind;
    this.keyId = fields.keyId;
    this.identity = fields.identity;
    this.issuedAt = fields.issuedAt;
    this.expiresAt = fields.expiresAt;
    this.holderKey = fields.holderKey?.slice();
    this.scopes = Object.freeze([...fields.scopes]);
    this.signatureKind =
      fields.holderKey === undefined ? SignatureKind.hmacSha256 : SignatureKind.ed25519;
    this.signature = new Uint8Array(this.signatureKind === SignatureKind.ed25519 ? 64 : 32);
    const key = `fake-ns:${++serial}`;
    this.#bytes = utf8Encode(key);
    registry.set(key, this);
  }

  get isBearer(): boolean {
    return this.holderKey === undefined;
  }

  isExpired(now: number = Date.now()): boolean {
    return this.expiresAt !== 0 && now >= this.expiresAt * 1000;
  }

  allows(right: Right, name: Name): boolean {
    return this.scopes.some(
      (scope) => scope.right === Right.admin || (scope.right === right && scope.matches(name)),
    );
  }

  get signedBytes(): Uint8Array {
    return this.#bytes;
  }

  encode(): Uint8Array {
    return this.#bytes.slice();
  }
}

/**
 * The credential [bytes] encode (`Credential.decode`, for the fakes).
 *
 * @throws {SwitchboardError} `INVALID_ARGUMENT` for bytes that are no fake credential.
 */
export function decodeFakeCredential(bytes: Uint8Array): Credential {
  const credential = registry.get(utf8Decode(bytes));
  if (credential === undefined) {
    throw SwitchboardError.of(StatusCode.invalidArgument, 'not a credential');
  }
  return credential;
}

/** What {@link FakeIssuer} takes. */
export interface FakeIssuerOptions {
  /** The issuer's key id; default `fake`. */
  readonly keyId?: string;
  /** Issues bearer credentials only, as an HMAC issuer does; default false. */
  readonly bearerOnly?: boolean;
  /** Wall time in ms since the epoch, the issue time; default `Date.now`. */
  readonly now?: () => number;
}

/**
 * Issues fake credentials (`CredentialIssuer.issue`): issued at the wall
 * time in whole seconds, expiring `lifetime` (ms, whole seconds counted)
 * later, or never for a null lifetime.
 */
export class FakeIssuer implements NamingCredentialIssuer {
  readonly keyId: Name;
  readonly bearerOnly: boolean;
  readonly now: () => number;
  /** Number of credentials issued. */
  issued = 0;

  constructor(options: FakeIssuerOptions = {}) {
    this.keyId = Name.of(options.keyId ?? 'fake');
    this.bearerOnly = options.bearerOnly ?? false;
    this.now = options.now ?? Date.now;
  }

  /** @throws {RangeError} for a holder key from a bearer-only issuer, or a lifetime that is not positive. */
  async issue(options: {
    kind: CredentialKind;
    identity: string;
    scopes: readonly Scope[];
    holderKey?: Uint8Array | undefined;
    lifetime: number | null;
  }): Promise<Credential> {
    if (options.holderKey !== undefined && this.bearerOnly) {
      throw new RangeError('HMAC credentials are bearer only');
    }
    const lifetime = options.lifetime;
    if (lifetime !== null && !(lifetime > 0)) {
      throw new RangeError(`lifetime must be positive: ${lifetime}`);
    }
    const issuedAt = Math.floor(this.now() / 1000);
    this.issued++;
    return new NamingFakeCredential({
      kind: options.kind,
      keyId: this.keyId,
      identity: options.identity,
      issuedAt,
      expiresAt: lifetime === null ? 0 : issuedAt + Math.trunc(lifetime / 1000),
      holderKey: options.holderKey,
      scopes: options.scopes,
    });
  }
}

/**
 * Verifies fake credentials (`CredentialVerifier`): accepts exactly the fake
 * credentials not expired at `options.now`, or at its own wall time.
 */
export class FakeNamingVerifier implements CredentialVerifier {
  readonly now: () => number;
  /** Number of verifications asked for. */
  calls = 0;

  constructor(options: { now?: () => number } = {}) {
    this.now = options.now ?? Date.now;
  }

  async verify(bytes: Uint8Array, options: { now?: number } = {}): Promise<Credential> {
    this.calls++;
    const credential = registry.get(utf8Decode(bytes));
    if (credential === undefined || credential.isExpired(options.now ?? this.now())) {
      throw SwitchboardError.of(StatusCode.unauthenticated, 'invalid credential');
    }
    return credential;
  }
}

/**
 * The identity half of a `MuxConnection` as the naming service reads it, on
 * the side that receives the peer's `IDENT`s: {@link identify} stands for a
 * valid `IDENT` of the peer, {@link goAway} records the GOAWAY this side
 * sends, which the peer reads as its `peerGoAwayStatus`.
 */
export class FakeNamingConnection implements NamingConnection {
  readonly now: () => number;
  /** Every GOAWAY sent on this connection, in order. */
  readonly goAways: Status[] = [];
  #identity: PeerIdentity | undefined;
  #open = true;
  #changed: Deferred<void> | undefined;

  /** A connection whose peer identified with `credential`, if given; `now` is the wall time (default `Date.now`). */
  constructor(options: { credential?: Credential | undefined; now?: () => number } = {}) {
    this.now = options.now ?? Date.now;
    if (options.credential !== undefined) {
      this.#identity = peerIdentityOf(options.credential, new Uint8Array(0), '');
    }
  }

  /** The last `IDENT`'s identity; undefined before one, and once its credential expired. */
  get peerIdentity(): PeerIdentity | undefined {
    const identity = this.#identity;
    return identity === undefined || identity.isExpired(this.now()) ? undefined : identity;
  }

  /** False once this side sent GOAWAY, or the connection closed. */
  get isOpen(): boolean {
    return this.#open;
  }

  /** The next {@link identify}, or the end; each read waits for the next change. */
  get identityChanged(): Promise<void> {
    this.#changed ??= new Deferred<void>();
    return this.#changed.promise;
  }

  /** The peer identifies (again) with [credential]: a valid `IDENT`. */
  async identify(
    credential: Credential,
    options: { intent?: Uint8Array; receiver?: string } = {},
  ): Promise<void> {
    if (!this.#open) {
      throw SwitchboardError.of(StatusCode.unavailable, 'connection closed');
    }
    this.#identity = peerIdentityOf(
      credential,
      options.intent ?? new Uint8Array(0),
      options.receiver ?? '',
    );
    this.#fire();
  }

  /** Records [status] as sent with GOAWAY and ends the connection. */
  async goAway(status: Status = Status.of(StatusCode.goingAway, 'going away')): Promise<void> {
    this.goAways.push(status);
    this.#end();
  }

  /** Ends the connection without GOAWAY. */
  async close(): Promise<void> {
    this.#end();
  }

  #end(): void {
    if (!this.#open) {
      return;
    }
    this.#open = false;
    this.#fire();
  }

  #fire(): void {
    const changed = this.#changed;
    this.#changed = undefined;
    changed?.resolve();
  }
}
