// The binding tests' identity, as `switchboard_quic`'s `binding_test.dart`
// has it: real credentials of an Ed25519 test authority (`auth1`, from the
// seed 7 * i), bearer ones of an HMAC issuer (`k1`, 32 zero bytes), real
// verifiers that know both, and a verifier whose answer the test gives
// (Dart's `HeldVerifier`). Holder keys are `Ed25519HolderKey`s.
import type {
  Credential,
  CredentialVerifier as Verifier,
} from '../../../src/core/identity/contract.ts';
import { CredentialKind } from '../../../src/core/identity/contract.ts';
import { CredentialIssuer } from '../../../src/core/identity/credential-issuer.ts';
import { CredentialVerifier } from '../../../src/core/identity/credential-verifier.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';

/** The authority's Ed25519 seed (`binding_test.dart`). */
const authoritySeed = Uint8Array.from({ length: 32 }, (_, i) => 7 * i);

/** The HMAC key of the bearer credentials (`binding_test.dart`'s `k1`). */
export const bearerKey: Uint8Array = new Uint8Array(32);

/** The test authority: Ed25519, key id `auth1`. */
export const authority: CredentialIssuer = await CredentialIssuer.ed25519FromSeed(
  'auth1',
  authoritySeed,
);

/** The issuer of bearer credentials: HMAC-SHA256, key id `k1`. */
export const bearerIssuer: CredentialIssuer = CredentialIssuer.hmac('k1', bearerKey);

const authorityKey = await authority.publicKey();

/** A verifier of the {@link authority}'s credentials and the {@link bearerIssuer}'s. */
export function verifier(): CredentialVerifier {
  return new CredentialVerifier({
    ed25519Keys: { auth1: authorityKey },
    hmacKeys: { k1: bearerKey },
  });
}

/**
 * A node credential of [identity]: the {@link authority}'s naming
 * [holderKey], or without one a bearer credential of the
 * {@link bearerIssuer}.
 */
export function credentialOf(identity: string, holderKey?: Uint8Array): Promise<Credential> {
  return holderKey === undefined
    ? bearerIssuer.issue({ kind: CredentialKind.node, identity })
    : authority.issue({ kind: CredentialKind.node, identity, holderKey });
}

/** A verifier whose answer the test gives. */
export class HeldVerifier implements Verifier {
  readonly result = new Deferred<Credential>();
  calls = 0;

  verify(): Promise<Credential> {
    this.calls++;
    return this.result.promise;
  }
}
