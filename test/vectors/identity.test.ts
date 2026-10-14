// Connection identity: the NONCE and IDENT rows of the wiki page
// "Polyverse Switchboard Test Vectors", section "Mux frames", with the
// proof P, and the IDENT payloads that section lists as protocol errors.
// Ports the "connection identity control messages" group of
// `test/vectors/identity_vectors_test.dart`; its credential groups are in
// `credential.test.ts`.
import { describe, expect, test } from 'vitest';
import { concatBytes, hexBytes, hexString } from '../../src/core/bytes.ts';
import { Ed25519HolderKey } from '../../src/core/identity/holder-key.ts';
import { MuxControlMessage, MuxFrame } from '../../src/core/mux/mux-frame.ts';
import { MuxIdent } from '../../src/core/mux/mux-protocol.ts';
import { ProtocolError } from '../../src/core/status.ts';
import {
  ed25519Credential,
  hmacCredential,
  holderPublic,
  holderSeed,
  intent,
  proof,
  receiver,
  receiverNonce,
  senderNonce,
} from '../support/identity/identity-vectors.ts';
import { wikiBlob, wikiBytes } from '../support/vectors.ts';

describe('connection identity control messages', () => {
  test('the shared inputs are the wiki blobs C1, C2 and P', () => {
    expect(hexString(wikiBlob('C1'))).toBe(hmacCredential);
    expect(hexString(wikiBlob('C2'))).toBe(ed25519Credential);
    expect(hexString(wikiBlob('C2.holder-seed'))).toBe(hexString(holderSeed));
    expect(hexString(wikiBlob('C2.holder-key'))).toBe(holderPublic);
    expect(hexString(wikiBlob('P'))).toBe(proof);
  });

  test('NONCE', () => {
    const frame = wikiBytes('test-vectors/mux-frames/control-nonce-32-bytes-of-11');
    expect(hexString(frame)).toBe(`02 00 00 05 ${hexString(senderNonce)}`);
    expect(hexString(MuxControlMessage.nonce(senderNonce).toFrame().encode())).toBe(
      hexString(frame),
    );
  });

  test('IDENT, bearer credential, no intent, no receiver, no proof', () => {
    const ident = new MuxIdent({ credential: hexBytes(hmacCredential) });
    const wiki = wikiBytes(
      'test-vectors/mux-frames/control-ident-credential-c1-below-no-intent-no-receiver-no-proof',
    );
    expect(hexString(wiki)).toBe(`02 00 00 06 56 00 ${hmacCredential} 00 00`);
    expect(hexString(MuxControlMessage.ident(ident).toFrame().encode())).toBe(hexString(wiki));
    const decoded = MuxIdent.decode(hexBytes(`56 00 ${hmacCredential} 00 00`));
    expect(hexString(decoded.credential)).toBe(hmacCredential);
    expect(decoded.intent).toHaveLength(0);
    expect(decoded.receiver).toHaveLength(0);
    expect(decoded.proof).toHaveLength(0);
  });

  test('IDENT, holder-key credential, intent, receiver, proof', async () => {
    const message = MuxIdent.proofMessage(senderNonce, receiverNonce, intent, receiver);
    expect(hexString(message)).toBe(
      `53 57 42 49 44 45 4E 54 ${hexString(senderNonce)} ` +
        `${hexString(receiverNonce)} 04 01 02 03 04 ` +
        '08 63 6F 6E 73 75 6D 65 72',
    );
    expect(message).toHaveLength(86);
    const holder = await Ed25519HolderKey.fromSeed(holderSeed);
    expect(hexString(holder.publicKey)).toBe(holderPublic);
    expect(hexString(await holder.sign(message))).toBe(proof);
    const ident = new MuxIdent({
      credential: hexBytes(ed25519Credential),
      intent,
      receiver,
      proof: hexBytes(proof),
    });
    const frame = hexString(
      wikiBytes(
        'test-vectors/mux-frames/control-ident-credential-c2-below-intent-01-02-03-04-receiver-consumer-proof-p',
      ),
    );
    expect(frame).toBe(
      `02 00 00 06 88 00 ${ed25519Credential} 04 01 02 03 04 ` +
        `08 63 6F 6E 73 75 6D 65 72 ${proof}`,
    );
    expect(hexString(MuxControlMessage.ident(ident).toFrame().encode())).toBe(frame);
    const decoded = MuxIdent.decode(
      MuxControlMessage.decode(MuxFrame.decode(hexBytes(frame)).payload).payload,
    );
    expect(hexString(decoded.credential)).toBe(ed25519Credential);
    expect(hexString(decoded.intent)).toBe('01 02 03 04');
    expect(hexString(decoded.receiver)).toBe(hexString(receiver));
    expect(hexString(decoded.proof)).toBe(proof);
  });

  test('the proof covers the field lengths', () => {
    // The same bytes split differently between intent and receiver sign
    // differently: an IDENT naming `consumer` cannot be passed off as one
    // naming nobody, with the name moved into the intent.
    expect(
      hexString(
        MuxIdent.proofMessage(
          senderNonce,
          receiverNonce,
          concatBytes(intent, receiver),
          new Uint8Array(0),
        ),
      ),
    ).not.toBe(hexString(MuxIdent.proofMessage(senderNonce, receiverNonce, intent, receiver)));
  });

  describe('negative (protocol error)', () => {
    // The wiki's IDENT payloads after the type byte; the intent of 65
    // bytes is its `00 00 41` followed by 65 bytes.
    for (const [name, bytes] of [
      ['truncated credential length', wikiBytes('test-vectors/mux-frames/prose-56')],
      [
        'credential longer than the payload',
        wikiBytes('test-vectors/mux-frames/prose-56-00-01-01'),
      ],
      ['missing intent length', wikiBytes('test-vectors/mux-frames/prose-02-00-aa-bb')],
      ['intent longer than the payload', wikiBytes('test-vectors/mux-frames/prose-00-00-05-01-02')],
      [
        'intent of 65 bytes',
        concatBytes(wikiBytes('test-vectors/mux-frames/prose-00-00-41'), new Uint8Array(65)),
      ],
      ['missing receiver length', wikiBytes('test-vectors/mux-frames/prose-00-00-01-aa')],
      [
        'receiver longer than the payload',
        wikiBytes('test-vectors/mux-frames/prose-00-00-00-04-61-62'),
      ],
    ] as const) {
      test(name, () => {
        expect(() => MuxIdent.decode(bytes)).toThrow(ProtocolError);
      });
    }
  });

  test('IDENT encode refusals', () => {
    const credential = new Uint8Array(10);
    expect(() => new MuxIdent({ credential, intent: new Uint8Array(65) }).encode()).toThrow(
      RangeError,
    );
    expect(() => new MuxIdent({ credential, receiver: new Uint8Array(256) }).encode()).toThrow(
      RangeError,
    );
    // 2 + 1018 + 1 + 1 + 2 = 1024 fits; one byte more does not.
    expect(
      new MuxIdent({ credential: new Uint8Array(1018), proof: new Uint8Array(2) }).encode(),
    ).toHaveLength(1024);
    expect(() =>
      new MuxIdent({ credential: new Uint8Array(1018), proof: new Uint8Array(3) }).encode(),
    ).toThrow(RangeError);
    expect(() =>
      MuxIdent.proofMessage(senderNonce, receiverNonce, new Uint8Array(256), receiver),
    ).toThrow(RangeError);
  });
});
