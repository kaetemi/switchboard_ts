// A credential verifier that stands in for the real one (the credential
// codec and verifiers come with the identity port): it knows credentials
// by their exact bytes and answers with the credential registered for
// them, so that IDENT verification can be tested on its own.
import { bytesEqual, hexString } from '../../../src/core/bytes.ts';
import {
  type Credential,
  CredentialKind,
  type CredentialVerifier,
  type Right,
  type Scope,
  SignatureKind,
} from '../../../src/core/identity/contract.ts';
import { Name } from '../../../src/core/name.ts';
import { StatusCode, SwitchboardError } from '../../../src/core/status.ts';

/** What a stub credential is made of. */
export interface StubCredentialInit {
  readonly identity: string;
  readonly kind?: CredentialKind;
  /** 32 bytes for a holder-key credential; undefined for a bearer one. */
  readonly holderKey?: Uint8Array | undefined;
  /** Unix seconds; 0 = never. */
  readonly expiresAt?: number;
  /** The bytes that stand for the credential on the wire. */
  readonly bytes: Uint8Array;
}

/** A {@link Credential} with only the fields IDENT verification reads. */
export function stubCredential(init: StubCredentialInit): Credential {
  const expiresAt = init.expiresAt ?? 0;
  return {
    version: 1,
    kind: init.kind ?? CredentialKind.node,
    keyId: Name.of('stub'),
    identity: init.identity,
    issuedAt: 0,
    expiresAt,
    holderKey: init.holderKey,
    scopes: [] as readonly Scope[],
    signatureKind: init.holderKey === undefined ? SignatureKind.hmacSha256 : SignatureKind.ed25519,
    signature: new Uint8Array(0),
    isBearer: init.holderKey === undefined,
    isExpired: (now = Date.now()) => expiresAt !== 0 && now >= expiresAt * 1000,
    allows: (_right: Right, _name: Name) => false,
    signedBytes: init.bytes,
    encode: () => init.bytes.slice(),
  };
}

/** Verifies the credentials it was given, by their bytes; refuses everything else. */
export class StubVerifier implements CredentialVerifier {
  readonly #known: Credential[];
  /** The bytes and times of every call. */
  readonly calls: { readonly bytes: string; readonly now: number | undefined }[] = [];

  constructor(...known: Credential[]) {
    this.#known = known;
  }

  async verify(bytes: Uint8Array, options: { now?: number } = {}): Promise<Credential> {
    this.calls.push({ bytes: hexString(bytes), now: options.now });
    const credential = this.#known.find((c) => bytesEqual(c.encode(), bytes));
    if (credential === undefined || credential.isExpired(options.now)) {
      throw SwitchboardError.of(StatusCode.unauthenticated, 'invalid credential');
    }
    return credential;
  }
}
