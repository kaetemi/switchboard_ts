/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { createPrivateKey, createPublicKey, sign } from 'node:crypto';
import { concatBytes, hexBytes, utf8Decode, utf8Encode } from '../../../src/core/bytes.ts';
import {
  type Credential,
  CredentialKind,
  type CredentialVerifier,
  type HolderKey,
  type PeerIdentity,
  Right,
  type Scope,
  SignatureKind,
} from '../../../src/core/identity/contract.ts';
import { Name } from '../../../src/core/name.ts';
import { StatusCode, SwitchboardError } from '../../../src/core/status.ts';

/**
 * Credentials without cryptography for the node thread's tests, standing in
 * for the protocol core's codec, issuer and verifier until they land. A
 * fake credential encodes to `fake:<serial>`; a {@link FakeVerifier}
 * accepts exactly the fake credentials not revoked and not expired.
 */

const registry = new Map<string, FakeCredential>();
let serial = 0;

/** A scope pattern match as the wiki defines it: a trailing `*` matches any suffix, `*` alone everything. */
export function patternMatches(pattern: Name, name: Name): boolean {
  const p = pattern.bytes;
  const n = name.bytes;
  const length = pattern.length;
  if (length > 0 && p[length - 1] === 0x2a) {
    for (let i = 0; i < length - 1; i++) {
      if (p[i] !== n[i]) return false;
    }
    return true;
  }
  return pattern.equals(name);
}

class FakeScope implements Scope {
  readonly right: Right;
  readonly pattern: Name;
  constructor(right: Right, pattern: string) {
    this.right = right;
    this.pattern = Name.of(pattern);
  }
  matches(name: Name): boolean {
    return patternMatches(this.pattern, name);
  }
}

/** What {@link fakeCredential} takes. */
export interface FakeCredentialInit {
  identity: string;
  kind?: CredentialKind;
  /** `[right, pattern]` pairs, such as `[Right.open, 'echo']`. */
  scopes?: readonly (readonly [Right, string])[];
  /** Unix seconds; 0 (default) never. */
  expiresAt?: number;
  /** The holder key's public key; undefined for a bearer credential. */
  holderKey?: Uint8Array;
}

class FakeCredential implements Credential {
  readonly version = 1;
  readonly kind: CredentialKind;
  readonly keyId = Name.of('fake');
  readonly identity: string;
  readonly issuedAt = 0;
  readonly expiresAt: number;
  readonly holderKey: Uint8Array | undefined;
  readonly scopes: readonly Scope[];
  readonly signatureKind = SignatureKind.ed25519;
  readonly signature = new Uint8Array(64);
  readonly #bytes: Uint8Array;
  revoked = false;

  constructor(init: FakeCredentialInit) {
    this.kind = init.kind ?? CredentialKind.node;
    this.identity = init.identity;
    this.expiresAt = init.expiresAt ?? 0;
    this.holderKey = init.holderKey;
    this.scopes = (init.scopes ?? []).map(([right, pattern]) => new FakeScope(right, pattern));
    const key = `fake:${++serial}`;
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

/** A credential the {@link FakeVerifier} accepts. */
export function fakeCredential(init: FakeCredentialInit): Credential {
  return new FakeCredential(init);
}

/** Makes [credential] (a fake one) invalid for every verifier from now on. */
export function revoke(credential: Credential): void {
  (credential as FakeCredential).revoked = true;
}

/** The PKCS#8 encoding of an Ed25519 private key, before its 32-byte seed. */
const pkcs8Ed25519 = hexBytes('302e020100300506032b657004220420');

/**
 * A real Ed25519 holder key whose private key is [seed] (32 bytes), so
 * that the proofs of possession it signs verify where the binding checks
 * them (the frame mux; the toy mux checks no proofs). Deterministic: one
 * seed, one key.
 */
export function fakeHolderKey(seed: Uint8Array = new Uint8Array(32).fill(7)): HolderKey {
  if (seed.length !== 32) {
    throw new RangeError(`an Ed25519 seed is 32 bytes, got ${seed.length}`);
  }
  const privateKey = createPrivateKey({
    key: Buffer.from(concatBytes(pkcs8Ed25519, seed)),
    format: 'der',
    type: 'pkcs8',
  });
  const spki = createPublicKey(privateKey).export({ format: 'der', type: 'spki' });
  return {
    publicKey: new Uint8Array(spki.subarray(spki.length - 32)),
    sign: async (message) => new Uint8Array(sign(null, message, privateKey)),
  };
}

/** Verifies fake credentials; counts its calls. */
export class FakeVerifier implements CredentialVerifier {
  calls = 0;

  async verify(bytes: Uint8Array, options: { now?: number } = {}): Promise<Credential> {
    this.calls++;
    const credential = registry.get(utf8Decode(bytes));
    if (
      credential === undefined ||
      credential.revoked ||
      credential.isExpired(options.now ?? Date.now())
    ) {
      throw SwitchboardError.of(StatusCode.unauthenticated, 'invalid credential');
    }
    return credential;
  }
}

/** The identity a verified IDENT establishes. */
export function peerIdentityOf(
  credential: Credential,
  intent: Uint8Array,
  receiver: string,
): PeerIdentity {
  return {
    credential,
    intent,
    verifiedAt: Date.now(),
    receiver,
    identity: credential.identity,
    kind: credential.kind,
    scopes: credential.scopes,
    provedPossession: !credential.isBearer,
    allows: (right, name) => credential.allows(right, name),
    isExpired: (now) => credential.isExpired(now),
  };
}
