/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import {
  type Credential as CredentialContract,
  CredentialKind,
  type PeerIdentity,
  type Right,
} from '../../../src/core/identity/contract.ts';
import { Credential } from '../../../src/core/identity/credential.ts';
import { CredentialIssuer } from '../../../src/core/identity/credential-issuer.ts';
import {
  CredentialVerifier,
  type CredentialVerifierOptions,
} from '../../../src/core/identity/credential-verifier.ts';
import { createPeerIdentity } from '../../../src/core/identity/peer-identity.ts';
import { Scope } from '../../../src/core/identity/scope.ts';

/**
 * The node suites' identity, as Dart's `identity_node_test.dart` has it:
 * real credentials of a test authority (`auth1`, an Ed25519 key pair from a
 * fixed seed), of a rogue key pair claiming the same key id, and of the
 * mesh's HMAC key (`mesh1`), verified by a real verifier that knows the
 * authority and the mesh key. Holder keys are `Ed25519HolderKey`s
 * (`Ed25519HolderKey.fromSeed` for deterministic ones). Every party reads
 * the wall time when it needs it, so that a test faking `Date` moves
 * issuance and expiry alike.
 */

/** The authority's Ed25519 seed (Dart's `authoritySeed`). */
export const authoritySeed: Uint8Array = Uint8Array.from({ length: 32 }, (_, i) => 3 * i);

/** The rogue key pair's seed (Dart's `rogueSeed`). */
export const rogueSeed: Uint8Array = Uint8Array.from({ length: 32 }, (_, i) => 5 * i + 1);

/** The mesh's HMAC key (Dart's `meshKey`). */
export const meshKey: Uint8Array = Uint8Array.from({ length: 32 }, (_, i) => i);

/** The wall time, read at each call (a faked `Date` included). */
const wallTime = (): number => Date.now();

/** The mesh authority: Ed25519, key id `auth1`. */
export const authority: CredentialIssuer = await CredentialIssuer.ed25519FromSeed(
  'auth1',
  authoritySeed,
  { wallTime },
);

/** Another key pair claiming the authority's key id: what it issues verifies nowhere. */
export const rogue: CredentialIssuer = await CredentialIssuer.ed25519FromSeed('auth1', rogueSeed, {
  wallTime,
});

/** The mesh's HMAC issuer, key id `mesh1`: bearer credentials only. */
export const hmacAuthority: CredentialIssuer = CredentialIssuer.hmac('mesh1', meshKey, {
  wallTime,
});

const authorityKey = await authority.publicKey();

/** What {@link issueCredential} takes. */
export interface TestCredentialInit {
  readonly identity: string;
  readonly kind?: CredentialKind;
  /** `[right, pattern]` pairs, such as `[Right.open, 'echo']`. */
  readonly scopes?: readonly (readonly [Right, string])[];
  /**
   * When it expires, in Unix seconds; 0 (default): the issuer's default
   * lifetime from now (8 hours). One in the past issues a credential that
   * has expired.
   */
  readonly expiresAt?: number;
  /** The holder key's public key; undefined for a bearer credential. */
  readonly holderKey?: Uint8Array;
  /** The issuer; default the {@link authority}. */
  readonly by?: CredentialIssuer;
}

/** A credential of [init.by] (the {@link authority} by default), as `CredentialIssuer.issue` makes it. */
export function issueCredential(init: TestCredentialInit): Promise<Credential> {
  const issuer = init.by ?? authority;
  const scopes = (init.scopes ?? []).map(([right, pattern]) => Scope.of(right, pattern));
  const expiresAt = init.expiresAt ?? 0;
  let issuedAt: number | undefined;
  let lifetime: number | undefined;
  if (expiresAt !== 0) {
    const now = Math.floor(Date.now() / 1000);
    const issued = expiresAt > now ? now : expiresAt - 3600;
    issuedAt = issued * 1000;
    lifetime = (expiresAt - issued) * 1000;
  }
  return issuer.issue({
    kind: init.kind ?? CredentialKind.node,
    identity: init.identity,
    scopes,
    holderKey: init.holderKey,
    issuedAt,
    lifetime,
  });
}

/**
 * [credential] with the last byte of its signature flipped, as Dart's
 * `identity_node_test.dart` forges one: a credential of the right
 * authority, holder and scopes whose IDENT nobody can verify.
 */
export function forged(credential: CredentialContract): Credential {
  const bytes = credential.encode();
  bytes[bytes.length - 1] = (bytes[bytes.length - 1] as number) ^ 0x01;
  return Credential.decode(bytes);
}

/**
 * The receivers' verifier: the {@link authority}'s public key and the mesh
 * key, checking expiry at the wall time of each call. Not the
 * {@link rogue}'s.
 */
export class TestVerifier extends CredentialVerifier {
  constructor(options: CredentialVerifierOptions = {}) {
    super({
      ed25519Keys: { auth1: authorityKey },
      hmacKeys: { mesh1: meshKey },
      wallTime,
      ...options,
    });
  }
}

/** The identity a verified IDENT establishes (`createPeerIdentity`, verified now). */
export function peerIdentityOf(
  credential: CredentialContract,
  intent: Uint8Array,
  receiver: string,
): PeerIdentity {
  return createPeerIdentity({ credential, intent, verifiedAt: Date.now(), receiver });
}
