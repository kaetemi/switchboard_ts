// Review regressions of the QUIC binding, a port of `switchboard_quic`'s
// `test/review_cases.dart` (run there from `binding_test.dart`). The cases
// of `review_switchboard_test.dart` need the Switchboard node and wait on
// its port. The late bulk case of Talk is played at the mux level: the test
// does what Talk does to the connection (its admission rule after GOAWAY,
// its drain hold on an ended parent, its bulk routing) and writes Talk's
// frames by hand. Identity is the core's, as in Dart: a credential issuer, a
// holder key from a seed, a verifier of the issuer.
import { describe, expect, test } from 'vitest';
import { ChannelAddress } from '../../../src/core/address/channel-address.ts';
import { bytesEqual, concatBytes } from '../../../src/core/bytes.ts';
import { CredentialKind } from '../../../src/core/identity/contract.ts';
import { CredentialIssuer } from '../../../src/core/identity/credential-issuer.ts';
import { CredentialVerifier } from '../../../src/core/identity/credential-verifier.ts';
import { Ed25519HolderKey } from '../../../src/core/identity/holder-key.ts';
import type { MuxChannel, MuxConnection } from '../../../src/core/mux/contract.ts';
import { MuxControlMessage, MuxControlType } from '../../../src/core/mux/mux-frame.ts';
import { MuxIdent } from '../../../src/core/mux/mux-protocol.ts';
import { Name } from '../../../src/core/name.ts';
import { QuicError, type QuicStream } from '../../../src/core/quic/contract.ts';
import { QuicHello, quicHelloType } from '../../../src/core/quic/hello.ts';
import { QuicBindingConnection } from '../../../src/core/quic/quic-binding-connection.ts';
import { QuicIdentityRules } from '../../../src/core/quic/quic-identity-rules.ts';
import { RecordReader } from '../../../src/core/quic/record-reader.ts';
import {
  decodeCloseBody,
  decodeOpenBody,
  encodeCloseRecord,
  encodeDataRecord,
  encodeOpenRecord,
  type QuicRecord,
  QuicRecordCommand,
  quicDefaultRecordLimit,
} from '../../../src/core/quic/records.ts';
import { Status, StatusCode } from '../../../src/core/status.ts';
import { TalkBulkOpen, TalkBulkReference } from '../../../src/core/talk/talk-bulk.ts';
import { TalkFrame, TalkKind } from '../../../src/core/talk/talk-frame.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';
import {
  MemoryQuicSession,
  type MemoryQuicStream,
} from '../../../src/testing/memory-quic-provider.ts';
import {
  bindingAndRaw,
  bytes,
  closeAfter,
  connectionPair,
  expectCode,
  expectStatus,
  expectThrowsCode,
  fast,
  pattern,
  RawPeer,
  text,
  until,
  within,
} from './harness.ts';

/** The channel [connection] delivers next (Dart's `incoming.first`). */
function nextIncoming(connection: MuxConnection): Promise<MuxChannel> {
  return connection.incoming[Symbol.asyncIterator]()
    .next()
    .then((next) => {
      if (next.done === true) {
        throw new Error('no more channels');
      }
      return next.value;
    });
}

/** Expects [actual] to equal [expected], code, reason and fields. */
function expectSameStatus(actual: Status, expected: Status): void {
  expect(actual.equals(expected), `${actual}, expected ${expected}`).toBe(true);
}

/** The in-memory stream [connection] opened or accepted last. */
function lastStream(connection: QuicBindingConnection): MemoryQuicStream {
  const streams = (connection.session as MemoryQuicSession).streams;
  return streams[streams.length - 1] as MemoryQuicStream;
}

/** Everything [channel] delivers until its end, joined. */
async function readAll(channel: MuxChannel): Promise<Uint8Array> {
  const reader = channel.readable.getReader();
  const chunks: Uint8Array[] = [];
  for (;;) {
    const next = await reader.read();
    if (next.done) {
      return concatBytes(...chunks);
    }
    chunks.push(next.value);
  }
}

describe('review regressions', () => {
  test('CLOSE without FIN releases the stream after confirmation timeout', async () => {
    const [connection, raw] = await bindingAndRaw({
      options: { ...fast, closeConfirmTimeout: 60 },
    });
    const incoming = nextIncoming(connection);
    const stream = await raw.openRaw(
      concatBytes(encodeOpenRecord(2, []), encodeCloseRecord(Status.ok)),
    );
    const channel = await within(incoming);
    expectStatus(await within(channel.done), StatusCode.ok);
    expect(await within(stream.stopped)).toBe(StatusCode.deadlineExceeded);
    const reader = new RecordReader(stream);
    expect((await reader.next(quicDefaultRecordLimit))?.command).toBe(QuicRecordCommand.close);
    await reader.expectEnd();
    expect(connection.openChannelCount).toBe(0);
    expect(connection.isOpen).toBe(true);
  });

  test('control reset is observed while control reading is throttled', async () => {
    const [connection, raw] = await bindingAndRaw({
      rawEnd: { streamWindow: 32 },
      quicOptions: { controlOutputMark: 128 },
    });
    await raw.sendControl(MuxControlMessage.ping(new Uint8Array(125)));
    await raw.sendControl(MuxControlMessage.ping(new Uint8Array(125)));
    await until(() => connection.heldOutputBytes > 128);
    (raw.control as QuicStream).reset(StatusCode.cancelled);
    expectStatus(await within(connection.done), StatusCode.protocolError);
  });

  test('peer reset ends a channel whose application consumes nothing', async () => {
    const [connection, raw] = await bindingAndRaw();
    const incoming = nextIncoming(connection);
    const stream = await raw.openRaw(
      concatBytes(encodeOpenRecord(2, []), encodeDataRecord([1, 2, 3])),
    );
    const channel = await incoming;
    await until(() => channel.bufferedBytes === 7);
    // The binding waits on consumption and has no pending read.
    stream.reset(StatusCode.cancelled);
    expectStatus(await within(channel.done), StatusCode.cancelled);
    expect(connection.openChannelCount).toBe(0);
    expect(connection.isOpen).toBe(true);
  });

  test('no-code STOP_SENDING is not a successful acknowledgement', async () => {
    const [connection, raw] = await bindingAndRaw({ bindingIsInitiator: true });
    const channel = connection.open(new Uint8Array(0));
    const stream = await raw.nextStream();
    await channel.whenWritable;
    (stream as MemoryQuicStream).stopWithoutCode();
    await until(() => !channel.canSend);
    await stream.write(encodeCloseRecord(Status.of(StatusCode.unknown, 'stopped')));
    await stream.finish();
    expectSameStatus(await within(channel.done), Status.of(StatusCode.unknown, 'stopped'));
  });

  test('no-code STOP_SENDING on control is a protocol error', async () => {
    const [connection, raw] = await bindingAndRaw();
    await until(() => connection.peerHello !== undefined);
    (raw.control as MemoryQuicStream).stopWithoutCode();
    expectStatus(await within(connection.done), StatusCode.protocolError);
  });

  test('failed exporter is not silently negotiated as unavailable', async () => {
    const [a] = MemoryQuicSession.pair();
    a.exporterFailure = new Error('export failed');
    const connection = await QuicBindingConnection.start(a, { options: fast });
    closeAfter(connection);
    expectStatus(await within(connection.done), StatusCode.internal);
  });

  test('session loss interrupts an exporter still pending', async () => {
    const [a] = MemoryQuicSession.pair();
    const gate = new Deferred<void>();
    a.exporterGate = gate.promise;
    const starting = QuicBindingConnection.start(a, { options: fast });
    await until(() => a.exporterLabels.length > 0);
    a.lose();
    const connection = await within(starting);
    expectStatus(await within(connection.done), StatusCode.connectionLost);
    gate.resolve();
  });

  test('identity defaults gate unverified peers whenever identity is used', () => {
    const rules = QuicIdentityRules.initiator({
      certificateVerified: false,
      hasCredential: false,
      verifiesCredentials: true,
    });
    expect(rules.requireExporterBinding).toBe(true);
    expect(rules.awaitPeerIdentity).toBe(true);
    expect(rules.bearerOnlyToAuthenticatedPeer).toBe(true);
  });

  for (const sentGoAway of [true, false]) {
    test(`Talk waits for late bulk after parent CLOSE and GOAWAY (sent ${sentGoAway})`, async () => {
      const [connection, raw] = await bindingAndRaw();
      // What Talk does to the connection: admit, after GOAWAY, a `_bulk`
      // continuation of a channel still open or held for its drain, and
      // take the `_bulk` channels before `incoming` sees them.
      connection.admitAfterGoAway((openPayload) => {
        const address = ChannelAddress.decode(openPayload);
        if (!TalkBulkOpen.isBulk(address)) {
          return false;
        }
        const parentId = TalkBulkOpen.decode(address).parentId;
        return (
          connection.channelWithId(parentId) !== undefined || connection.retainedForDrain(parentId)
        );
      });
      const bulkArrived = new Deferred<MuxChannel>();
      connection.adoptIncoming((channel) => {
        if (!TalkBulkOpen.isBulk(ChannelAddress.decode(channel.openPayload))) {
          return false;
        }
        bulkArrived.resolve(channel);
        return true;
      });
      const parentArrived = nextIncoming(connection);
      const parent = await raw.openRaw(
        encodeOpenRecord(2, new ChannelAddress({ type: 'echo' }).encode()),
      );
      const talk = await within(parentArrived);
      // A receiver may grant ahead while the response payload is pending;
      // its parent's CLOSE is then visible before the bulk stream.
      talk.grant(65536);
      // Talk's request, and its response: a reference to bulk channel 1,
      // which holds the ended parent for its drain until the payload came.
      const message = (async () => {
        await talk.send(
          new TalkFrame({
            kind: TalkKind.message,
            procedure: Name.of('REQ'),
            requestId: 1,
          }).encode(),
        );
        const reader = talk.readable.getReader();
        const first = await reader.read();
        if (first.done) {
          throw new Error('the parent ended without a response');
        }
        const response = TalkFrame.decode(first.value);
        expect(response.responseId).toBe(1);
        const reference = response.bulkReference;
        const payload = new Deferred<Uint8Array>();
        connection.retainForDrain(
          talk.id,
          payload.promise.then(() => undefined),
        );
        const bulk = await bulkArrived.promise;
        const open = TalkBulkOpen.decode(ChannelAddress.decode(bulk.openPayload));
        expect(open.parentId).toBe(talk.id);
        expect(open.number).toBe(reference.number);
        payload.resolve(await readAll(bulk));
        return payload.promise;
      })();
      message.catch(() => {});
      const request = await new RecordReader(parent).next(quicDefaultRecordLimit);
      const requestId = TalkFrame.decode((request as QuicRecord).body).requestId;
      await parent.write(
        encodeDataRecord(
          new TalkFrame({ kind: TalkKind.message, responseId: requestId })
            .withBulk(new TalkBulkReference(1, 3))
            .encode(),
        ),
      );
      if (sentGoAway) {
        void connection.goAway();
        await raw.nextControlOf(MuxControlType.goAway);
        await raw.answerPing();
      } else {
        await raw.sendControl(MuxControlMessage.goAway(Status.of(StatusCode.goingAway)));
        await connection.peerGoAwayStatus;
      }
      await parent.write(encodeCloseRecord(Status.ok));
      await parent.finish();
      await until(() => connection.channelWithId(2) === undefined && connection.hasDrainHolds);
      const bulk = await raw.openRaw(
        concatBytes(
          encodeOpenRecord(4, new TalkBulkOpen(2, 1).encode()),
          encodeDataRecord([1, 2, 3]),
          encodeCloseRecord(Status.ok),
        ),
      );
      await bulk.finish();
      expect([...(await within(message))]).toEqual([1, 2, 3]);
      expectStatus(await within(connection.done), StatusCode.goingAway);
    });
  }

  test('failure close before opening still writes OPEN and CLOSE', async () => {
    const [connection, raw] = await bindingAndRaw({ bindingIsInitiator: true });
    const channel = connection.open(bytes('open'));
    const closing = channel.close(Status.of(StatusCode.notFound, 'gone'));
    const stream = await raw.nextStream();
    const [records, end] = await RawPeer.drain(stream);
    expect(records.map((r) => r.command)).toEqual([
      QuicRecordCommand.open,
      QuicRecordCommand.close,
    ]);
    const open = decodeOpenBody((records[0] as QuicRecord).body);
    expect(open.id).toBe(channel.id);
    expect(text(open.openPayload)).toBe('open');
    expectSameStatus(
      decodeCloseBody((records[records.length - 1] as QuicRecord).body),
      Status.of(StatusCode.notFound, 'gone'),
    );
    expect(end).toBe('fin');
    await within(closing);
  });

  test('concurrent error closes preserve each side first status', async () => {
    const [a, b] = await connectionPair();
    const incoming = nextIncoming(b);
    const local = a.open(new Uint8Array(0));
    const remote = await incoming;
    const first = local.close(Status.of(StatusCode.aborted, 'local'));
    const second = remote.close(Status.of(StatusCode.cancelled, 'remote'));
    await within(Promise.all([first, second]));
    expectSameStatus(await local.done, Status.of(StatusCode.aborted, 'local'));
    expectSameStatus(await remote.done, Status.of(StatusCode.cancelled, 'remote'));
    expect(a.isOpen && b.isOpen).toBe(true);
  });

  test('peer CLOSE interrupts a write stalled at the window', async () => {
    const [connection, raw] = await bindingAndRaw({
      bindingIsInitiator: true,
      rawEnd: { streamWindow: 100 },
    });
    const channel = connection.open(new Uint8Array(0));
    const stream = await raw.nextStream();
    const sending = channel.send(pattern(1000));
    const failed = expectCode(sending, StatusCode.failedPrecondition);
    await stream.write(encodeCloseRecord(Status.of(StatusCode.cancelled)));
    await stream.finish();
    expectStatus(await within(channel.done), StatusCode.cancelled);
    await failed;
    expect(lastStream(connection).resets).toEqual([0]);
  });

  test('second HELLO is a protocol error', async () => {
    const [connection, raw] = await bindingAndRaw();
    await raw.sendControl(new MuxControlMessage(quicHelloType, new QuicHello().encode()));
    expectStatus(await within(connection.done), StatusCode.protocolError);
    expect((await raw.end).code).toBe(StatusCode.protocolError);
  });

  test('control OPEN must have an empty payload', async () => {
    const [connection, raw] = await bindingAndRaw({ rawControl: false });
    await raw.openRaw(encodeOpenRecord(0, [1]));
    expectStatus(await within(connection.done), StatusCode.protocolError);
  });

  test('a record exactly at the limit makes progress over a small window', async () => {
    const [a, b] = await connectionPair({ acceptorEnd: { streamWindow: 100 } });
    const incoming = nextIncoming(b);
    const channel = a.open(new Uint8Array(0));
    const remote = await incoming;
    const data = remote.readable.getReader().read();
    await channel.send(pattern(quicDefaultRecordLimit));
    const chunk = await within(data);
    expect(chunk.done).toBe(false);
    expect(bytesEqual(chunk.value ?? [], pattern(quicDefaultRecordLimit))).toBe(true);
    expectThrowsCode(
      () => channel.send(pattern(quicDefaultRecordLimit + 1)),
      StatusCode.frameTooLarge,
    );
    await within(channel.close());
  });

  for (const initiator of [true, false]) {
    test(`missing HELLO is bounded (initiator ${initiator})`, async () => {
      const [a, b] = MemoryQuicSession.pair();
      const raw = new RawPeer(initiator ? b : a);
      const connection = await QuicBindingConnection.start(initiator ? a : b, {
        options: fast,
        quicOptions: { controlStreamTimeout: 100 },
      });
      closeAfter(connection);
      if (initiator) {
        await raw.acceptControl({ sendHello: false });
      } else {
        await raw.openControl({ sendHello: false });
      }
      expectStatus(await within(connection.done), StatusCode.protocolError);
    });
  }

  test('listener refuses nonce-bound IDENT even without a verifier', async () => {
    const [connection, raw] = await bindingAndRaw({
      quicOptions: { identity: QuicIdentityRules.listener },
    });
    await raw.sendControl(MuxControlMessage.nonce(pattern(32)));
    await raw.nextControlOf(MuxControlType.nonce);
    await raw.sendControl(MuxControlMessage.ident(new MuxIdent({ credential: new Uint8Array(0) })));
    expectStatus(await within(connection.done), StatusCode.unauthenticated);
  });

  test('mismatched exporters cannot authenticate', async () => {
    const issuer = await CredentialIssuer.ed25519FromSeed('issuer', pattern(32));
    const key = await Ed25519HolderKey.fromSeed(pattern(32, 1));
    const credential = await issuer.issue({
      kind: CredentialKind.node,
      identity: 'sender',
      holderKey: key.publicKey,
    });
    const verifier = new CredentialVerifier();
    await verifier.addIssuer(issuer);
    const [a, b] = MemoryQuicSession.pair();
    b.exporterXor = 1;
    const receiver = await QuicBindingConnection.start(b, {
      options: { ...fast, identityVerifier: verifier },
    });
    const sender = await QuicBindingConnection.start(a, { options: fast });
    closeAfter(sender);
    closeAfter(receiver);
    await expectCode(sender.identify(credential, { holderKey: key }), StatusCode.unauthenticated);
    expectStatus(await receiver.done, StatusCode.unauthenticated);
  });

  test('pending OPEN streams are capped and expire', async () => {
    const [connection, raw] = await bindingAndRaw({
      quicOptions: { maxPendingStreams: 1, openStreamTimeout: 200 },
    });
    await until(() => connection.peerHello !== undefined);
    const first = await raw.openRaw([6]);
    // A header byte has arrived; the first OPEN remains incomplete.
    await until(() => (connection.session as MemoryQuicSession).announced.length === 2);
    const second = await raw.openRaw([6]);
    expect(await within(second.stopped)).toBe(StatusCode.resourceExhausted);
    expect(await within(first.stopped)).toBe(StatusCode.deadlineExceeded);
    expect(connection.isOpen).toBe(true);
  });

  test('locally queued PING output is bounded without unhandled futures', async () => {
    const [connection] = await bindingAndRaw({
      bindingIsInitiator: true,
      rawEnd: { streamWindow: 32 },
      quicOptions: { controlOutputMark: 128 },
    });
    // Intentionally do not listen to the returned promises.
    for (let i = 0; i < 400 && connection.isOpen; i++) {
      void connection.ping(new Uint8Array(125));
    }
    expectStatus(await within(connection.done), StatusCode.resourceExhausted);
  });

  test('no-code reset is UNKNOWN without an arbitrary delay', async () => {
    const [connection, raw] = await bindingAndRaw();
    const incoming = nextIncoming(connection);
    const stream = await raw.openRaw(encodeOpenRecord(2, []));
    const channel = await incoming;
    (stream as MemoryQuicStream).resetWithoutCode();
    expectStatus(await within(channel.done), StatusCode.unknown);
    expect(connection.isOpen).toBe(true);
  });

  test('provider draining is GOAWAY while admitted work can finish', async () => {
    const [a, b] = await connectionPair();
    const incoming = nextIncoming(b);
    const channel = a.open(new Uint8Array(0));
    await incoming;
    (b.session as MemoryQuicSession).drainPeer();
    expectStatus(await within(a.peerGoAwayStatus), StatusCode.goingAway);
    expectThrowsCode(() => a.open(new Uint8Array(0)), StatusCode.failedPrecondition);
    await within(channel.close());
    expectStatus(await within(a.done), StatusCode.goingAway);
  });

  test('unidirectional streams are refused without losing the connection', async () => {
    const [a, b] = await connectionPair();
    const stream = (a.session as MemoryQuicSession).openUnidirectional();
    expect(await within(stream.stopped.promise)).toBe(StatusCode.unsupported);
    await within(b.ping());
    expect(a.isOpen && b.isOpen).toBe(true);
  });

  for (const operation of ['write', 'finish', 'read'] as const) {
    test(`provider ${operation} failure ends only its channel`, async () => {
      const [connection, raw] = await bindingAndRaw({ bindingIsInitiator: true });
      const channel = connection.open(new Uint8Array(0));
      await raw.nextStream();
      await channel.whenWritable;
      const stream = lastStream(connection);
      const error = new Error(`injected ${operation} failure`);
      if (operation === 'write') {
        stream.writeFailure = error;
        await expectCode(channel.send(bytes('data')), StatusCode.failedPrecondition);
      } else if (operation === 'finish') {
        stream.finishFailure = error;
        await channel.close();
      } else {
        stream.failRead(error);
      }
      expectStatus(await within(channel.done), StatusCode.internal);
      expect(connection.isOpen).toBe(true);
    });
  }

  test('stream open failure has a channel status, not CONNECTION_LOST', async () => {
    const [connection] = await bindingAndRaw({ bindingIsInitiator: true });
    (connection.session as MemoryQuicSession).openFailure = new QuicError('streamLimit', 'limit');
    const channel = connection.open(new Uint8Array(0));
    expectStatus(await within(channel.done), StatusCode.resourceExhausted);
    expect(connection.isOpen).toBe(true);
  });

  test('session loss resolves pending writes and writable waiters', async () => {
    const [connection, raw] = await bindingAndRaw({
      bindingIsInitiator: true,
      rawEnd: { streamWindow: 100 },
    });
    const channel = connection.open(new Uint8Array(0));
    await raw.nextStream();
    const sent = expectCode(channel.send(pattern(1000)), StatusCode.failedPrecondition);
    const ready = expectCode(channel.whenWritable, StatusCode.failedPrecondition);
    raw.session.lose();
    expectStatus(await within(channel.done), StatusCode.connectionLost);
    await Promise.all([sent, ready]);
    await connection.goAway();
  });

  test('incoming stream source failure ends the session', async () => {
    const [connection] = await bindingAndRaw();
    (connection.session as MemoryQuicSession).failIncoming(new Error('failed source'));
    expectStatus(await within(connection.done), StatusCode.connectionLost);
    await connection.goAway();
  });
});
