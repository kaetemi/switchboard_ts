/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Credentials and connection identities for the naming service's identity
// tests. The credentials are real (`Credential`, the core's codec and
// `Scope`s, so `Credential.decode` reads what the naming service answers),
// but signed with a placeholder and verified by membership: a
// `RegistryVerifier` accepts exactly the credentials a `RegistryIssuer` issued,
// by their bytes, unless expired. Real signatures cannot be used here: the
// Dart test runs on fake_async with Dart's own Ed25519, while WebCrypto
// signs and verifies on a thread pool, whose answers arrive after an
// unknown number of event-loop turns, out of reach of the `ManualClock` the
// tests step time with. (The RENEW case on real time uses the real issuer
// and verifier.) The connection identity stands for the node's: the naming
// tests run without nodes. Issuers, verifiers and connections read the
// wall time they are given, so that a test on a `ManualClock` passes
// `() => clock.now()` to all of them and to the naming service's
// `wallTime`.

import { hexString } from '../../../src/core/bytes.ts';
import {
  type Credential as CredentialContract,
  type CredentialKind,
  type CredentialVerifier,
  type PeerIdentity,
  type Right,
  type Scope as ScopeContract,
  SignatureKind,
} from '../../../src/core/identity/contract.ts';
import { Credential, signatureLength } from '../../../src/core/identity/credential.ts';
import { Scope } from '../../../src/core/identity/scope.ts';
import { Name } from '../../../src/core/name.ts';
import type {
  NamingConnection,
  NamingCredentialIssuer,
} from '../../../src/core/naming/naming-service.ts';
import { Status, StatusCode, SwitchboardError } from '../../../src/core/status.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';
import { peerIdentityOf } from '../node/test-authority.ts';

/** Every credential a {@link RegistryIssuer} issued, by its encoding. */
const registry = new Map<string, Credential>();

/** A scope granting [right] over the types matching [pattern] (`Scope.of`). */
export function scopeOf(right: Right, pattern: string | Name): Scope {
  return Scope.of(right, pattern);
}

/**
 * The credential [bytes] encode: `Credential.decode`, the core's codec.
 *
 * @throws as `Credential.decode` does, for bytes that are no credential.
 */
export function decodeCredential(bytes: Uint8Array): Credential {
  return Credential.decode(bytes);
}

/** What {@link RegistryIssuer} takes. */
export interface RegistryIssuerOptions {
  /** The issuer's key id; default `registry`. */
  readonly keyId?: string;
  /** Issues bearer credentials only, as an HMAC issuer does; default false. */
  readonly bearerOnly?: boolean;
  /** Wall time in ms since the epoch, the issue time; default `Date.now`. */
  readonly now?: () => number;
}

/**
 * Issues credentials as `CredentialIssuer.issue` lays them out (issued at
 * the wall time in whole seconds, expiring `lifetime` (ms, whole seconds
 * counted) later, or never for a null lifetime; Ed25519, or HMAC-SHA256 for
 * a bearer-only issuer), with a signature of zeros that only a
 * {@link RegistryVerifier} accepts.
 */
export class RegistryIssuer implements NamingCredentialIssuer {
  readonly keyId: Name;
  readonly bearerOnly: boolean;
  readonly now: () => number;
  /** Number of credentials issued. */
  issued = 0;

  constructor(options: RegistryIssuerOptions = {}) {
    this.keyId = Name.of(options.keyId ?? 'registry');
    this.bearerOnly = options.bearerOnly ?? false;
    this.now = options.now ?? Date.now;
  }

  /** @throws {RangeError} for a holder key from a bearer-only issuer, or a lifetime that is not positive. */
  async issue(options: {
    kind: CredentialKind;
    identity: string;
    scopes: readonly ScopeContract[];
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
    const signatureKind = this.bearerOnly ? SignatureKind.hmacSha256 : SignatureKind.ed25519;
    const credential = new Credential({
      kind: options.kind,
      keyId: this.keyId,
      identity: options.identity,
      issuedAt,
      expiresAt: lifetime === null ? 0 : issuedAt + Math.trunc(lifetime / 1000),
      holderKey: options.holderKey,
      scopes: options.scopes,
      signatureKind,
      signature: new Uint8Array(signatureLength(signatureKind)),
    });
    this.issued++;
    registry.set(hexString(credential.encode()), credential);
    return credential;
  }
}

/**
 * Verifies the credentials of the {@link RegistryIssuer}s (`CredentialVerifier`):
 * accepts exactly those, by their bytes, not expired at `options.now`, or at
 * its own wall time.
 */
export class RegistryVerifier implements CredentialVerifier {
  readonly now: () => number;
  /** Number of verifications asked for. */
  calls = 0;

  constructor(options: { now?: () => number } = {}) {
    this.now = options.now ?? Date.now;
  }

  async verify(bytes: Uint8Array, options: { now?: number } = {}): Promise<Credential> {
    this.calls++;
    const credential = registry.get(hexString(bytes));
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
export class ManualNamingConnection implements NamingConnection {
  readonly now: () => number;
  /** Every GOAWAY sent on this connection, in order. */
  readonly goAways: Status[] = [];
  #identity: PeerIdentity | undefined;
  #open = true;
  #changed: Deferred<void> | undefined;

  /** A connection whose peer identified with `credential`, if given; `now` is the wall time (default `Date.now`). */
  constructor(options: { credential?: CredentialContract | undefined; now?: () => number } = {}) {
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
    credential: CredentialContract,
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
