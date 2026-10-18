/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// The peer's identity (README, "Identity"), as dart_peer/lib/src/identity.dart
// has it: the test authorities both peers share, the credential a peer
// presents, and the verifier and policies it applies.

import { createHash } from 'node:crypto';
import {
  ChannelPolicies,
  type ChannelPolicy,
  Credential,
  CredentialIssuer,
  CredentialKind,
  CredentialVerifier,
  credentialKindName,
  Ed25519HolderKey,
  type PeerIdentity,
  Right,
  Scope,
} from '../../../src/node/index.ts';
import {
  ArgumentProblem,
  arrayArg,
  boolArg,
  hex,
  intArg,
  type Json,
  objectArg,
  requiredString,
  stringArg,
} from './protocol.ts';

/** The Ed25519 authority both peers trust: key id `auth1`, seed `3 * i`. */
export const authoritySeed = Uint8Array.from({ length: 32 }, (_, i) => 3 * i);

/** An authority nobody trusts claiming the same key id: seed `5 * i + 1`. */
export const rogueSeed = Uint8Array.from({ length: 32 }, (_, i) => 5 * i + 1);

/** The HMAC key both peers trust: key id `mesh1`, bytes 0 to 31. */
export const meshKey = Uint8Array.from({ length: 32 }, (_, i) => i);

/** The holder key seed of [identity]: SHA-256 of `holder:<identity>`. */
export function holderSeed(identity: string): Uint8Array {
  return new Uint8Array(createHash('sha256').update(`holder:${identity}`).digest());
}

/** The credential a peer presents, from the `identity.credential` object. */
export interface CredentialSpec {
  /** The identity it names. */
  readonly identity: string;
  /** `ed25519` (`auth1`), `hmac` (`mesh1`) or `rogue` (an untrusted `auth1`). */
  readonly authority: string;
  /** Whether it names a holder key (proof of possession). */
  readonly holder: boolean;
  /** What it identifies. */
  readonly kind: CredentialKind;
  /** What it allows. */
  readonly scopes: readonly Scope[];
  /** Whether its last signature byte is flipped. */
  readonly forge: boolean;
}

/** The `identity` configuration. */
export interface IdentityConfig {
  /** Whether the peer verifies IDENTs with the trusted authorities. */
  readonly verify: boolean;
  /** The credential the peer presents, or undefined. */
  readonly credential: CredentialSpec | undefined;
  /** Whether the peer identifies on the connections it dials. */
  readonly identifyOutgoing: boolean;
  /** Whether the peer identifies in return on accepted connections whose peer asked. */
  readonly identifyInReturn: boolean;
  /** How long a channel waits for its connection's IDENT, in ms. */
  readonly identityTimeout: number;
  /** The listeners' policy: `scoped`, `requireIdentity`, `denyReserved`, or undefined. */
  readonly policy: string | undefined;
  /** The QUIC listeners' identification rules: `none` or `listener`. */
  readonly quicRules: string;
}

const defaultScopes: readonly Json[] = [
  { right: 'open', pattern: '*' },
  { right: 'register', pattern: '*' },
  { right: 'watch', pattern: '*' },
  { right: 'claim', pattern: '*' },
];

function enumValue<T extends Record<string, number>>(
  values: T,
  name: string,
  what: string,
): T[keyof T] {
  if (!Object.hasOwn(values, name)) {
    throw new ArgumentProblem(`unknown ${what} ${name}`);
  }
  return values[name] as T[keyof T];
}

function credentialSpec(json: Json): CredentialSpec {
  const authority = stringArg(json, 'authority') ?? 'ed25519';
  return {
    identity: requiredString(json, 'identity'),
    authority,
    holder: json['holder'] === undefined ? authority !== 'hmac' : boolArg(json, 'holder'),
    kind: enumValue(CredentialKind, stringArg(json, 'kind') ?? 'node', 'credential kind'),
    scopes: (arrayArg(json, 'scopes') ?? defaultScopes).map((s) =>
      Scope.of(
        enumValue(Right, requiredString(s as Json, 'right'), 'right'),
        requiredString(s as Json, 'pattern'),
      ),
    ),
    forge: boolArg(json, 'forge'),
  };
}

/** Reads the `identity` object; absent, no identity. */
export function identityConfig(json: Json | undefined): IdentityConfig {
  const j = json ?? {};
  const credential = objectArg(j, 'credential');
  return {
    verify: boolArg(j, 'verify'),
    credential: credential === undefined ? undefined : credentialSpec(credential),
    identifyOutgoing: boolArg(j, 'identifyOutgoing', true),
    identifyInReturn: boolArg(j, 'identifyInReturn'),
    identityTimeout: intArg(j, 'identityTimeoutMs') ?? 10_000,
    policy: stringArg(j, 'policy'),
    quicRules: stringArg(j, 'quicRules') ?? 'none',
  };
}

/** The listener policy of [config], or undefined. */
export function channelPolicy(config: IdentityConfig): ChannelPolicy | undefined {
  switch (config.policy) {
    case undefined:
      return undefined;
    case 'scoped':
      return ChannelPolicies.scoped();
    case 'requireIdentity':
      return ChannelPolicies.requireIdentity();
    case 'denyReserved':
      return ChannelPolicies.denyReserved;
    default:
      throw new ArgumentProblem(`unknown policy ${config.policy}`);
  }
}

/** What a node is made with: its credential, holder key and verifier. */
export interface NodeIdentity {
  readonly credential: Credential | undefined;
  readonly holderKey: Ed25519HolderKey | undefined;
  readonly verifier: CredentialVerifier | undefined;
}

/** Issues [config]'s credential and builds its verifier. */
export async function nodeIdentity(config: IdentityConfig): Promise<NodeIdentity> {
  const authority = await CredentialIssuer.ed25519FromSeed('auth1', authoritySeed);
  const hmac = CredentialIssuer.hmac('mesh1', meshKey);
  let verifier: CredentialVerifier | undefined;
  if (config.verify) {
    verifier = new CredentialVerifier();
    await verifier.addIssuer(authority);
    await verifier.addIssuer(hmac);
  }
  const spec = config.credential;
  if (spec === undefined) {
    return { credential: undefined, holderKey: undefined, verifier };
  }
  let issuer: CredentialIssuer;
  switch (spec.authority) {
    case 'ed25519':
      issuer = authority;
      break;
    case 'hmac':
      issuer = hmac;
      break;
    case 'rogue':
      issuer = await CredentialIssuer.ed25519FromSeed('auth1', rogueSeed);
      break;
    default:
      throw new ArgumentProblem(`unknown authority ${spec.authority}`);
  }
  const holderKey = spec.holder
    ? await Ed25519HolderKey.fromSeed(holderSeed(spec.identity))
    : undefined;
  let credential = await issuer.issue({
    kind: spec.kind,
    identity: spec.identity,
    scopes: spec.scopes,
    holderKey: holderKey?.publicKey,
  });
  if (spec.forge) {
    const bytes = credential.encode().slice();
    bytes[bytes.length - 1] = (bytes[bytes.length - 1] as number) ^ 0x01;
    credential = Credential.decode(bytes);
  }
  return { credential, holderKey, verifier };
}

/** [identity] as the `connection.identified` event reports it. */
export function identityJson(identity: PeerIdentity): Json {
  return {
    identity: identity.identity,
    kind: credentialKindName(identity.kind) ?? identity.kind,
    provedPossession: identity.provedPossession,
    intent: hex(identity.intent),
    receiver: identity.receiver,
  };
}
