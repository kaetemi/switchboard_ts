/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import type { Name } from '../name.ts';

/**
 * Credentials and connection identity (wiki page "Polyverse Switchboard
 * Identity and Credentials"). The codecs and the cryptography
 * (`credential.ts`, `credential-issuer.ts`, `credential-verifier.ts`,
 * `holder-key.ts`, `ed25519.ts` over WebCrypto) are the "protocol core"
 * thread's; these are the shapes every other layer programs against.
 */

export const CredentialKind = { node: 1, client: 2, device: 3 } as const;
export type CredentialKind = (typeof CredentialKind)[keyof typeof CredentialKind];

export const Right = {
  register: 1,
  open: 2,
  watch: 3,
  claim: 4,
  migrate: 5,
  broker: 6,
  admin: 7,
} as const;
export type Right = (typeof Right)[keyof typeof Right];

export const SignatureKind = { hmacSha256: 1, ed25519: 2 } as const;
export type SignatureKind = (typeof SignatureKind)[keyof typeof SignatureKind];

/** A right over the service types matching a pattern: a trailing `*` matches any suffix, `*` alone matches all. */
export interface Scope {
  readonly right: Right;
  readonly pattern: Name;
  matches(name: Name): boolean;
}

/** A signed credential: who the holder is and what it may do. A decoded credential re-encodes to its exact bytes. */
export interface Credential {
  readonly version: number;
  readonly kind: CredentialKind;
  readonly keyId: Name;
  readonly identity: string;
  /** Unix seconds. */
  readonly issuedAt: number;
  /** Unix seconds; 0 = never. */
  readonly expiresAt: number;
  /** 32 bytes for a holder-key credential; undefined for a bearer credential. */
  readonly holderKey: Uint8Array | undefined;
  readonly scopes: readonly Scope[];
  readonly signatureKind: SignatureKind;
  readonly signature: Uint8Array;
  readonly isBearer: boolean;
  /** Whether `now` (milliseconds since the epoch; default `Date.now()`) is at or past `expiresAt`. */
  isExpired(now?: number): boolean;
  /** Whether a scope grants [right] for [name]; admin allows everything. */
  allows(right: Right, name: Name): boolean;
  readonly signedBytes: Uint8Array;
  encode(): Uint8Array;
}

/** The holder's Ed25519 key pair. */
export interface HolderKey {
  readonly publicKey: Uint8Array;
  sign(message: Uint8Array): Promise<Uint8Array>;
}

/** Verifies credentials against the issuer keys it knows, with a cache by credential bytes. */
export interface CredentialVerifier {
  /** @throws {SwitchboardError} UNAUTHENTICATED 'invalid credential' (the cause goes to the log). */
  verify(bytes: Uint8Array, options?: { now?: number }): Promise<Credential>;
}

/** The identity a connection's peer established with a valid IDENT. */
export interface PeerIdentity {
  readonly credential: Credential;
  readonly intent: Uint8Array;
  /** Milliseconds since the epoch. */
  readonly verifiedAt: number;
  /** This side's identity as the IDENT named it, or empty: none named. */
  readonly receiver: string;
  readonly identity: string;
  readonly kind: CredentialKind;
  readonly scopes: readonly Scope[];
  readonly provedPossession: boolean;
  allows(right: Right, name: Name): boolean;
  isExpired(now?: number): boolean;
}
