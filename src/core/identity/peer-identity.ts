/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * The identity a connection's peer established with a valid `IDENT`
 * (`MuxConnection.peerIdentity`). Ports `peer_identity.dart`; the shape is
 * {@link PeerIdentity} of the identity contract.
 */

import type { Name } from '../name.ts';
import {
  type Credential,
  CredentialKind,
  type PeerIdentity,
  type Right,
  type Scope,
} from './contract.ts';

/** What {@link createPeerIdentity} takes. */
export interface PeerIdentityInit {
  /** The verified credential. */
  readonly credential: Credential;
  /** The opaque intent of the `IDENT`; empty when none. Kept as given. */
  readonly intent: Uint8Array;
  /** When the `IDENT` was verified, milliseconds since the epoch. */
  readonly verifiedAt: number;
  /** The receiver the `IDENT` named (this side's identity); default empty: none. */
  readonly receiver?: string | undefined;
}

const kindNames = new Map<number, string>(
  Object.entries(CredentialKind).map(([name, code]) => [code, name]),
);

class VerifiedPeerIdentity implements PeerIdentity {
  readonly credential: Credential;
  readonly intent: Uint8Array;
  readonly verifiedAt: number;
  readonly receiver: string;

  constructor(init: PeerIdentityInit) {
    this.credential = init.credential;
    this.intent = init.intent;
    this.verifiedAt = init.verifiedAt;
    this.receiver = init.receiver ?? '';
  }

  get identity(): string {
    return this.credential.identity;
  }

  get kind(): CredentialKind {
    return this.credential.kind;
  }

  get scopes(): readonly Scope[] {
    return this.credential.scopes;
  }

  get provedPossession(): boolean {
    return !this.credential.isBearer;
  }

  allows(right: Right, name: Name): boolean {
    return this.credential.allows(right, name);
  }

  isExpired(now?: number): boolean {
    return this.credential.isExpired(now);
  }

  toString(): string {
    const kind = kindNames.get(this.kind) ?? `kind ${this.kind}`;
    const intent = this.intent.length === 0 ? '' : `, intent of ${this.intent.length} bytes`;
    const receiver = this.receiver.length === 0 ? '' : `, to "${this.receiver}"`;
    return `PeerIdentity(${kind} "${this.identity}"${intent}${receiver})`;
  }
}

/**
 * The identity of a peer whose `IDENT` verified: the credential's identity,
 * kind, scopes and rights, the intent and receiver the `IDENT` carried, and
 * when it was verified. Whether the peer proved possession is whether the
 * credential has a holder key.
 */
export function createPeerIdentity(init: PeerIdentityInit): PeerIdentity {
  return new VerifiedPeerIdentity(init);
}
