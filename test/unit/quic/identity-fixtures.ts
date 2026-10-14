// Credentials, a verifier and a holder key for the binding's identity
// tests. The core's credential codecs and verifiers are not ported yet, so
// a credential here is a readable byte string the fake verifier accepts;
// the holder key is real Ed25519 over WebCrypto, so proofs of possession
// are signed and verified as they will be.
import { hexBytes, hexString } from '../../../src/core/bytes.ts';
import type {
  Credential,
  CredentialVerifier,
  HolderKey,
} from '../../../src/core/identity/contract.ts';
import { CredentialKind, SignatureKind } from '../../../src/core/identity/contract.ts';
import { Name } from '../../../src/core/name.ts';
import { StatusCode, SwitchboardError } from '../../../src/core/status.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const prefix = 'fake-credential:';

/** An Ed25519 holder key over WebCrypto. */
export async function generateHolderKey(): Promise<HolderKey> {
  const pair = (await crypto.subtle.generateKey({ name: 'Ed25519' }, true, [
    'sign',
    'verify',
  ])) as CryptoKeyPair;
  const publicKey = new Uint8Array(await crypto.subtle.exportKey('raw', pair.publicKey));
  return {
    publicKey,
    async sign(message: Uint8Array): Promise<Uint8Array> {
      return new Uint8Array(
        await crypto.subtle.sign('Ed25519', pair.privateKey, message as Uint8Array<ArrayBuffer>),
      );
    },
  };
}

/** A credential the fake verifier accepts: a holder-key one with [holderKey], a bearer one without. */
export function fakeCredential(identity: string, holderKey?: Uint8Array): Credential {
  const key = holderKey === undefined ? 'bearer' : hexString(holderKey).replaceAll(' ', '');
  const bytes = encoder.encode(`${prefix}${identity}:${key}`);
  return {
    version: 1,
    kind: CredentialKind.node,
    keyId: Name.of('fake'),
    identity,
    issuedAt: 0,
    expiresAt: 0,
    holderKey,
    scopes: [],
    signatureKind: holderKey === undefined ? SignatureKind.hmacSha256 : SignatureKind.ed25519,
    signature: new Uint8Array(0),
    isBearer: holderKey === undefined,
    isExpired: () => false,
    allows: () => false,
    signedBytes: bytes,
    encode: () => bytes.slice(),
  };
}

/** Verifies the credentials of {@link fakeCredential}; anything else is invalid. */
export class FakeVerifier implements CredentialVerifier {
  calls = 0;

  async verify(bytes: Uint8Array): Promise<Credential> {
    this.calls++;
    const text = decoder.decode(bytes);
    const split = text.lastIndexOf(':');
    if (!text.startsWith(prefix) || split < prefix.length) {
      throw SwitchboardError.of(StatusCode.unauthenticated, 'invalid credential');
    }
    const identity = text.slice(prefix.length, split);
    const key = text.slice(split + 1);
    return fakeCredential(identity, key === 'bearer' ? undefined : hexBytes(key));
  }
}

/** A verifier whose answer the test gives. */
export class HeldVerifier implements CredentialVerifier {
  readonly result = new Deferred<Credential>();
  calls = 0;

  verify(): Promise<Credential> {
    this.calls++;
    return this.result.promise;
  }
}
