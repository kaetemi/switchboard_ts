// What every binding shares: IDENT verification gating the frames after it
// (over the frame mux) and the public status helpers. Ports
// `test/vectors/binding_protocol_test.dart`.
import { expect, test } from 'vitest';
import { utf8Encode } from '../../src/core/bytes.ts';
import type { Credential, CredentialVerifier } from '../../src/core/identity/contract.ts';
import { FrameMuxConnection } from '../../src/core/mux/frame-mux-connection.ts';
import { MuxControlMessage, MuxControlType, MuxFrame } from '../../src/core/mux/mux-frame.ts';
import {
  MuxIdent,
  maxControlPayloadLength,
  statusForWire,
  statusFromWire,
} from '../../src/core/mux/mux-protocol.ts';
import { MovedStatus } from '../../src/core/naming/naming-protocol.ts';
import { Status, StatusCode, SwitchboardError } from '../../src/core/status.ts';
import { Deferred } from '../../src/core/util/deferred.ts';
import { ManualClock } from '../../src/testing/manual-clock.ts';
import { stubCredential } from '../support/identity/stub-verifier.ts';
import {
  AnsweringTransport,
  expectCode,
  isControl,
  rawOptions,
  rawPair,
  settle,
} from '../unit/mux/support.ts';

/** A verifier that holds every verification until the test settles it. */
class HeldVerifier implements CredentialVerifier {
  readonly result = new Deferred<Credential>();
  readonly entered = new Deferred<void>();
  calls = 0;

  verify(_bytes: Uint8Array): Promise<Credential> {
    this.calls++;
    this.entered.resolve();
    return this.result.promise;
  }
}

function control(type: MuxControlType, payload: Uint8Array): Uint8Array {
  return new MuxControlMessage(type, payload).toFrame().encode();
}

for (const succeeds of [false, true]) {
  test(`IDENT verification gates OPEN, PING and a queued IDENT: ${succeeds}`, async () => {
    const verifier = new HeldVerifier();
    const [connection, raw] = rawPair({
      muxIsInitiator: false,
      options: { ...rawOptions, identityVerifier: verifier },
    });
    const openedAs: (string | undefined)[] = [];
    void (async () => {
      for await (const _ of connection.incoming) {
        openedAs.push(connection.peerIdentity?.identity);
      }
    })();
    raw.sendBytes(control(MuxControlType.nonce, new Uint8Array(32)));
    await raw.nextControl(MuxControlType.nonce);
    const ident = new MuxIdent({ credential: new Uint8Array(0) }).encode();
    raw.sendBytes(control(MuxControlType.ident, ident));
    raw.sendBytes(MuxFrame.open(2, new Uint8Array(1)).encode());
    raw.sendBytes(control(MuxControlType.ping, Uint8Array.of(7)));
    raw.sendBytes(control(MuxControlType.ident, ident));
    await verifier.entered.promise;
    await settle();
    expect(openedAs).toEqual([]);
    expect(verifier.calls).toBe(1);
    if (succeeds) {
      verifier.result.resolve(stubCredential({ identity: 'verified', bytes: new Uint8Array(0) }));
      const pong = await raw.nextControl(MuxControlType.pong);
      expect([...pong.payload]).toEqual([7]);
      // Opening and PONG delivery both happen after the new identity.
      expect(openedAs).toEqual(['verified']);
      await settle();
      expect(verifier.calls).toBe(2);
    } else {
      verifier.result.reject(SwitchboardError.of(StatusCode.unauthenticated, 'refused'));
      const frames = await raw.rest();
      expect(frames.some((frame) => isControl(frame, MuxControlType.pong))).toBe(false);
      expect(openedAs).toEqual([]);
      expect(verifier.calls).toBe(1);
      expectCode(await connection.done, StatusCode.unauthenticated);
    }
    await connection.close();
  });
}

test('wrong receiver closes synchronously without entering the verifier', async () => {
  // Dart feeds the frames synchronously and sees the connection closed when
  // the IDENT's write returns. The mux reads its transport in a loop, so
  // here it is closed once it has read the IDENT: the receiver check comes
  // before anything is awaited, and the verifier is never entered.
  const verifier = new HeldVerifier();
  const transport = new AnsweringTransport(() => []);
  const connection = new FrameMuxConnection(transport, {
    ...rawOptions,
    identityVerifier: verifier,
    clock: new ManualClock(),
    isInitiator: false,
  });
  connection.localIdentity = 'receiver';
  transport.deliver(control(MuxControlType.nonce, new Uint8Array(32)));
  await settle();
  expect(connection.isOpen).toBe(true);
  transport.deliver(
    control(
      MuxControlType.ident,
      new MuxIdent({
        credential: new Uint8Array(0),
        receiver: utf8Encode('somebody-else'),
      }).encode(),
    ),
  );
  await settle();
  expect(connection.isOpen).toBe(false);
  expect(verifier.calls).toBe(0);
  expectCode(await connection.done, StatusCode.unauthenticated);
});

test('public binding status helpers preserve payloads and map local codes', () => {
  const moved = new MovedStatus({
    owner: 0x123456789abc,
    epoch: 0xfedcba98,
    reason: 'é'.repeat(900),
  });
  const wire = statusForWire(moved.toStatus());
  expect(wire.encode().length).toBeLessThanOrEqual(maxControlPayloadLength);
  const decoded = MovedStatus.fromStatus(wire);
  expect(decoded.owner).toBe(moved.owner);
  expect(decoded.epoch).toBe(moved.epoch);
  expect(decoded.reason).not.toContain('�');
  expect(statusForWire(Status.of(StatusCode.connectionLost)).known).toBe(StatusCode.unavailable);
  expect(() => statusForWire(new Status(256))).toThrow(RangeError);
  expect(statusFromWire(new Status(256)).known).toBe(StatusCode.unknown);
});
