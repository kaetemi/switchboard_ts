// IDENT verification on its own, with a stand-in verifier: the checks of
// `verifyIdent` and `identReceiverProblem` (Dart `mux_protocol.dart`), which
// the Dart suite reaches only through connections (`identity_test.dart`,
// `binding_protocol_test.dart`); those ports follow with the frame mux.
import { describe, expect, test } from 'vitest';
import { hexBytes, utf8Encode } from '../../../src/core/bytes.ts';
import type { Credential, CredentialVerifier } from '../../../src/core/identity/contract.ts';
import { ed25519KeyPairFromSeed, ed25519Sign } from '../../../src/core/identity/ed25519.ts';
import {
  identReceiverProblem,
  MuxIdent,
  type VerifyIdentOptions,
  verifyIdent,
} from '../../../src/core/mux/mux-protocol.ts';
import { Status, StatusCode, SwitchboardError } from '../../../src/core/status.ts';
import { holderSeed } from '../../support/identity/identity-vectors.ts';
import { StubVerifier, stubCredential } from '../../support/identity/stub-verifier.ts';

const senderNonce = new Uint8Array(32).fill(0xa1);
const receiverNonce = new Uint8Array(32).fill(0xb2);
const bearerBytes = hexBytes('B1 B2 B3');
const holderBytes = hexBytes('C1 C2 C3 C4');

async function fixture(): Promise<{
  verifier: StubVerifier;
  bearer: Credential;
  holder: Credential;
  sign: (intent: Uint8Array, receiver: Uint8Array) => Promise<Uint8Array>;
}> {
  const keys = await ed25519KeyPairFromSeed(holderSeed);
  const bearer = stubCredential({ identity: 'npc-7', bytes: bearerBytes });
  const holder = stubCredential({
    identity: 'worker-a1',
    holderKey: keys.publicKey,
    bytes: holderBytes,
  });
  return {
    verifier: new StubVerifier(bearer, holder),
    bearer,
    holder,
    sign: (intent, receiver) =>
      ed25519Sign(
        MuxIdent.proofMessage(senderNonce, receiverNonce, intent, receiver),
        keys.privateKey,
      ),
  };
}

function options(
  verifier: CredentialVerifier,
  more: Partial<VerifyIdentOptions> = {},
): VerifyIdentOptions {
  return { verifier, senderNonce, receiverNonce, ...more };
}

async function refusal(promise: Promise<unknown>): Promise<SwitchboardError> {
  const error = await promise.then(
    () => undefined,
    (e: unknown) => e,
  );
  expect(error).toBeInstanceOf(SwitchboardError);
  return error as SwitchboardError;
}

describe('verifyIdent', () => {
  test('a bearer credential without a proof establishes its identity', async () => {
    const { verifier } = await fixture();
    const identity = await verifyIdent(
      new MuxIdent({ credential: bearerBytes }),
      options(verifier, { now: 1234 }),
    );
    expect(identity.identity).toBe('npc-7');
    expect(identity.provedPossession).toBe(false);
    expect(identity.receiver).toBe('');
    expect(identity.intent).toHaveLength(0);
    expect(identity.verifiedAt).toBe(1234);
    expect(verifier.calls).toEqual([{ bytes: 'B1 B2 B3', now: 1234 }]);
    expect(`${identity}`).toBe('PeerIdentity(node "npc-7")');
  });

  test('a holder-key credential with its proof, an intent and this side named', async () => {
    const { verifier, sign } = await fixture();
    const intent = hexBytes('01 02');
    const receiver = utf8Encode('consumer');
    const ident = new MuxIdent({
      credential: holderBytes,
      intent,
      receiver,
      proof: await sign(intent, receiver),
    });
    const before = Date.now();
    const identity = await verifyIdent(ident, options(verifier, { localIdentity: 'consumer' }));
    expect(identity.identity).toBe('worker-a1');
    expect(identity.provedPossession).toBe(true);
    expect(identity.receiver).toBe('consumer');
    expect(identity.verifiedAt).toBeGreaterThanOrEqual(before);
    expect(identity.verifiedAt).toBeLessThanOrEqual(Date.now());
    // Without a time, the verifier checks at its own.
    expect(verifier.calls[0]?.now).toBeUndefined();
    // The intent is copied out of the frame it came in.
    expect(identity.intent).toEqual(intent);
    expect(identity.intent.buffer).not.toBe(intent.buffer);
    expect(`${identity}`).toBe('PeerIdentity(node "worker-a1", intent of 2 bytes, to "consumer")');
  });

  test('the verifier gets a copy of the credential bytes', async () => {
    const frame = hexBytes('00 B1 B2 B3 00');
    let seen: Uint8Array | undefined;
    const verifier: CredentialVerifier = {
      async verify(bytes) {
        seen = bytes;
        return stubCredential({ identity: 'npc-7', bytes: bearerBytes });
      },
    };
    await verifyIdent(new MuxIdent({ credential: frame.subarray(1, 4) }), options(verifier));
    expect(seen).toEqual(bearerBytes);
    expect(seen?.buffer).not.toBe(frame.buffer);
  });

  test('a receiver other than this side is refused before the credential is read', async () => {
    const { verifier } = await fixture();
    const named = new MuxIdent({ credential: bearerBytes, receiver: utf8Encode('somebody-else') });
    for (const localIdentity of ['receiver', undefined]) {
      const error = await refusal(verifyIdent(named, options(verifier, { localIdentity })));
      expect(error.code).toBe(StatusCode.unauthenticated);
      expect(error.status.reason).toBe('the IDENT names another receiver ("somebody-else")');
    }
    expect(verifier.calls).toHaveLength(0);
  });

  test('requireNamedIdent refuses an IDENT that names nobody', async () => {
    const { verifier } = await fixture();
    const error = await refusal(
      verifyIdent(
        new MuxIdent({ credential: bearerBytes }),
        options(verifier, { localIdentity: 'consumer', requireNamedIdent: true }),
      ),
    );
    expect(error.code).toBe(StatusCode.unauthenticated);
    expect(error.status.reason).toBe('the IDENT names no receiver');
    expect(verifier.calls).toHaveLength(0);
  });

  test('a bearer credential with a proof is refused', async () => {
    const { verifier } = await fixture();
    const error = await refusal(
      verifyIdent(
        new MuxIdent({ credential: bearerBytes, proof: new Uint8Array(64) }),
        options(verifier),
      ),
    );
    expect(error.code).toBe(StatusCode.unauthenticated);
    expect(error.status.reason).toBe('a proof with a bearer credential');
  });

  test('a holder-key credential needs a proof of 64 bytes that verifies', async () => {
    const { verifier, sign } = await fixture();
    const good = await sign(new Uint8Array(0), new Uint8Array(0));
    const flipped = good.slice();
    flipped[10] = (flipped[10] as number) ^ 0x01;
    for (const proof of [new Uint8Array(0), good.subarray(0, 63), flipped, new Uint8Array(64)]) {
      const error = await refusal(
        verifyIdent(new MuxIdent({ credential: holderBytes, proof }), options(verifier)),
      );
      expect(error.code).toBe(StatusCode.unauthenticated);
      expect(error.status.reason).toBe('proof of possession does not verify');
    }
    // Signed over the other side's nonces: what this side sent back.
    const error = await refusal(
      verifyIdent(
        new MuxIdent({ credential: holderBytes, proof: good }),
        options(verifier, { senderNonce: receiverNonce, receiverNonce: senderNonce }),
      ),
    );
    expect(error.code).toBe(StatusCode.unauthenticated);
    expect(
      (await verifyIdent(new MuxIdent({ credential: holderBytes, proof: good }), options(verifier)))
        .identity,
    ).toBe('worker-a1');
  });

  test("the verifier's refusal and its other errors propagate", async () => {
    const refused = SwitchboardError.of(StatusCode.permissionDenied, 'revoked');
    const broken = new TypeError('broken');
    for (const thrown of [refused, broken]) {
      const verifier: CredentialVerifier = {
        verify: () => Promise.reject(thrown),
      };
      await expect(
        verifyIdent(new MuxIdent({ credential: bearerBytes }), options(verifier)),
      ).rejects.toBe(thrown);
    }
  });

  test('an unknown credential is refused by the verifier', async () => {
    const { verifier } = await fixture();
    const error = await refusal(
      verifyIdent(new MuxIdent({ credential: hexBytes('00') }), options(verifier)),
    );
    expect(error.status.equals(Status.of(StatusCode.unauthenticated, 'invalid credential'))).toBe(
      true,
    );
  });
});

describe('identReceiverProblem', () => {
  test('nobody named is fine unless a name is required', () => {
    expect(identReceiverProblem(new Uint8Array(0))).toBeUndefined();
    expect(identReceiverProblem(new Uint8Array(0), { localIdentity: 'me' })).toBeUndefined();
    expect(identReceiverProblem(new Uint8Array(0), { requireNamedIdent: true })).toBe(
      'the IDENT names no receiver',
    );
  });

  test('a name must be this side, byte for byte', () => {
    expect(identReceiverProblem(utf8Encode('me'), { localIdentity: 'me' })).toBeUndefined();
    expect(identReceiverProblem(utf8Encode('héllo'), { localIdentity: 'héllo' })).toBeUndefined();
    expect(identReceiverProblem(utf8Encode('me'), { localIdentity: 'Me' })).toBeDefined();
    expect(identReceiverProblem(utf8Encode('me'))).toBe('the IDENT names another receiver ("me")');
    // Malformed UTF-8 never matches, and is shown with replacements.
    expect(identReceiverProblem(hexBytes('FF'), { localIdentity: '�' })).toBe(
      'the IDENT names another receiver ("�")',
    );
  });
});
