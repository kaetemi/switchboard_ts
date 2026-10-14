// The exporter-bound IDENT vector X of the wiki page "Polyverse Switchboard
// Test Vectors", and P checked against it. Ports
// `test/vectors/exporter_identity_vectors_test.dart`. The credential C2 is
// checked by a stand-in verifier that knows its bytes, until the credential
// verifier is ported; the proofs are checked for real.
import { describe, expect, test } from 'vitest';
import { hexBytes, hexString } from '../../src/core/bytes.ts';
import type { PeerIdentity } from '../../src/core/identity/contract.ts';
import { ed25519KeyPairFromSeed, ed25519Sign } from '../../src/core/identity/ed25519.ts';
import { secureRandomBytes } from '../../src/core/identity/secure-random.ts';
import { MuxIdent, verifyIdent } from '../../src/core/mux/mux-protocol.ts';
import { StatusCode, SwitchboardError } from '../../src/core/status.ts';
import {
  at,
  ed25519Credential,
  expiresAt,
  holderPublic,
  holderSeed,
  intent,
  proof,
  receiver,
  receiverNonce,
  senderNonce,
} from '../support/identity/identity-vectors.ts';
import { StubVerifier, stubCredential } from '../support/identity/stub-verifier.ts';

async function expectCode(promise: Promise<unknown>, code: StatusCode): Promise<void> {
  const error = await promise.then(
    () => undefined,
    (e: unknown) => e,
  );
  expect(error).toBeInstanceOf(SwitchboardError);
  expect((error as SwitchboardError).code).toBe(code);
}

describe('exporter-bound proof (QUIC and WebTransport binding, X)', () => {
  // The inputs of P with an exporter of 32 bytes 33.
  const exporter = new Uint8Array(32).fill(0x33);
  const message =
    '53 57 42 49 44 54 4C 53 11 11 11 11 11 11 11 11 ' +
    '11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 11 ' +
    '11 11 11 11 11 11 11 11 22 22 22 22 22 22 22 22 ' +
    '22 22 22 22 22 22 22 22 22 22 22 22 22 22 22 22 ' +
    '22 22 22 22 22 22 22 22 33 33 33 33 33 33 33 33 ' +
    '33 33 33 33 33 33 33 33 33 33 33 33 33 33 33 33 ' +
    '33 33 33 33 33 33 33 33 04 01 02 03 04 08 63 6F ' +
    '6E 73 75 6D 65 72';
  const x =
    '14 92 28 9E 3F C3 B3 C6 91 F3 B1 96 EE F2 E5 ED ' +
    'C1 A3 C1 86 A3 D8 9A 0E 4B 10 B8 93 4C 7E 8B 4B ' +
    'CA 10 FA 3E 47 92 D2 BD 69 67 22 21 92 FC FC 44 ' +
    '96 73 EB 16 BA 43 02 AF 58 FE 19 A6 AF 53 7E 03';

  const verifier = new StubVerifier(
    stubCredential({
      identity: 'worker-a1',
      holderKey: hexBytes(holderPublic),
      expiresAt,
      bytes: hexBytes(ed25519Credential),
    }),
  );

  const ident = (proofHex: string): MuxIdent =>
    new MuxIdent({
      credential: hexBytes(ed25519Credential),
      intent,
      receiver,
      proof: hexBytes(proofHex),
    });

  /** verifyIdent at the vectors' time, as `consumer` receiving. */
  const verify = (
    proofHex: string,
    options: { exporter?: Uint8Array; swapped?: boolean } = {},
  ): Promise<PeerIdentity> =>
    verifyIdent(ident(proofHex), {
      verifier,
      senderNonce: options.swapped === true ? receiverNonce : senderNonce,
      receiverNonce: options.swapped === true ? senderNonce : receiverNonce,
      localIdentity: 'consumer',
      exporter: options.exporter,
      now: at,
    });

  test('the signed message and its signature', async () => {
    const signed = MuxIdent.proofMessage(senderNonce, receiverNonce, intent, receiver, exporter);
    expect(hexString(signed)).toBe(message);
    expect(signed).toHaveLength(118);
    expect(hexString(MuxIdent.exporterProofLabel)).toBe(
      hexString(new TextEncoder().encode('SWBIDTLS')),
    );
    const holder = await ed25519KeyPairFromSeed(holderSeed);
    expect(hexString(await ed25519Sign(signed, holder.privateKey))).toBe(x);
  });

  test('X verifies as an exporter-bound proof', async () => {
    const identity = await verify(x, { exporter });
    expect(identity.identity).toBe('worker-a1');
    expect(identity.receiver).toBe('consumer');
    expect(hexString(identity.intent)).toBe(hexString(intent));
    expect(identity.verifiedAt).toBe(at);
    expect(identity.provedPossession).toBe(true);
    // P verifies as a nonce-bound one, the form it was made for.
    expect((await verify(proof)).identity).toBe('worker-a1');
  });

  test('X does not verify as a nonce-bound proof', async () => {
    await expectCode(verify(x), StatusCode.unauthenticated);
  });

  test('P does not verify as an exporter-bound proof', async () => {
    await expectCode(verify(proof, { exporter }), StatusCode.unauthenticated);
  });

  test('X with the nonces swapped (sent back to its sender) does not verify', async () => {
    await expectCode(verify(x, { exporter, swapped: true }), StatusCode.unauthenticated);
  });

  test('X with another exporter does not verify', async () => {
    await expectCode(
      verify(x, { exporter: new Uint8Array(32).fill(0x34) }),
      StatusCode.unauthenticated,
    );
  });

  test('an exporter of another length is refused', async () => {
    expect(() =>
      MuxIdent.proofMessage(senderNonce, receiverNonce, intent, receiver, new Uint8Array(31)),
    ).toThrow(RangeError);
    await expect(verify(x, { exporter: new Uint8Array(33) })).rejects.toThrow(RangeError);
  });
});

test('secure random bytes support lengths beyond the Web Crypto quota', () => {
  for (const length of [0, 32, 65536, 65537]) {
    expect(secureRandomBytes(length)).toHaveLength(length);
  }
  const big = secureRandomBytes(65537);
  // The last byte comes from a second call, not left at 0 (one in 2^64
  // chance of a false failure on the last 8 bytes).
  expect(big.subarray(65529).some((b) => b !== 0)).toBe(true);
  expect(() => secureRandomBytes(-1)).toThrow(RangeError);
});
