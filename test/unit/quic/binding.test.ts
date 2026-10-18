// QuicBindingConnection and QuicBindingChannel over the in-memory provider, a
// port of `switchboard_quic`'s `test/binding_test.dart`: channel ids,
// records, the close handshake (CLOSE records, resets, STOP_SENDING, the
// confirmation timeout), flow control, priorities, openAfter, GOAWAY and
// the ends of the connection, keep-alive, the resource limits of the
// binding page, and identification at the connection level.
import { beforeAll, describe, expect, onTestFinished, test } from 'vitest';
import { ChannelAddress } from '../../../src/core/address/channel-address.ts';
import { hexBytes, hexString, readU48, writeU32, writeU48 } from '../../../src/core/bytes.ts';
import type { HolderKey } from '../../../src/core/identity/contract.ts';
import { Ed25519HolderKey } from '../../../src/core/identity/holder-key.ts';
import type { MuxChannel } from '../../../src/core/mux/contract.ts';
import { MuxControlMessage, MuxControlType } from '../../../src/core/mux/mux-frame.ts';
import { MuxIdent, verifyIdent } from '../../../src/core/mux/mux-protocol.ts';
import { MovedStatus } from '../../../src/core/naming/naming-protocol.ts';
import { QuicStreamReset, rawQuicExporterLabel } from '../../../src/core/quic/contract.ts';
import { QuicHello, quicHelloType } from '../../../src/core/quic/hello.ts';
import {
  quicBulkPriority,
  quicControlPriority,
  quicOrdinaryPriority,
} from '../../../src/core/quic/quic-binding-channel.ts';
import { QuicBindingConnection } from '../../../src/core/quic/quic-binding-connection.ts';
import { QuicIdentityRules } from '../../../src/core/quic/quic-identity-rules.ts';
import { RecordReader } from '../../../src/core/quic/record-reader.ts';
import {
  decodeCloseBody,
  decodeOpenBody,
  encodeCloseRecord,
  encodeDataRecord,
  encodeOpenRecord,
  QuicRecordCommand,
  quicDefaultRecordLimit,
  writeQuicRecordHeader,
} from '../../../src/core/quic/records.ts';
import { Status, StatusCode } from '../../../src/core/status.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';
import {
  MemoryQuicNetwork,
  MemoryQuicSession,
  type MemoryQuicStream,
} from '../../../src/testing/memory-quic-provider.ts';
import {
  bindingAndRaw,
  bytes,
  closeAfter,
  collect,
  concat,
  connectionPair,
  expectChunks,
  expectCode,
  expectStatus,
  expectThrowsCode,
  fast,
  firstChunk,
  limit,
  listen,
  nextChannel,
  pattern,
  RawPeer,
  readAll,
  rejection,
  settle,
  text,
  until,
  within,
} from './harness.ts';
import {
  credentialOf as credentialFor,
  HeldVerifier,
  verifier as testVerifier,
} from './identity-fixtures.ts';

const empty = new Uint8Array(0);

/** Swallows [promise]'s rejection: Dart's `unawaited(... .catchError(...))`. */
function quietly(promise: Promise<unknown>): void {
  promise.catch(() => {});
}

describe('channel ids', () => {
  test(
    'even from 2 for the initiator, odd from 3 for the acceptor, increasing, ' +
      'the same at both ends, never reused',
    async () => {
      const [a, b] = await connectionPair();
      const atA = collect(a);
      const atB = collect(b);
      const fromA = [0, 1, 2].map(() => a.open(empty));
      const fromB = [0, 1].map(() => b.open(empty));
      expect(fromA.map((c) => c.id)).toEqual([2, 4, 6]);
      expect(fromB.map((c) => c.id)).toEqual([3, 5]);
      await until(() => atA.length === 2 && atB.length === 3);
      expect(atB.map((c) => c.id)).toEqual([2, 4, 6]);
      expect(atA.map((c) => c.id)).toEqual([3, 5]);
      expect(b.channelWithId(4)).toBe(atB[1]);
      // Closed both ways, an id is never used again.
      for (const c of atB) {
        listen(c);
      }
      await within((fromA[0] as MuxChannel).close());
      expect(a.channelWithId(2)).toBeUndefined();
      expect(a.open(empty).id).toBe(8);
    },
  );

  test('the reserved ids are skipped, and a side that used every id of its parity opens no more', async () => {
    const [a, b] = await connectionPair();
    b.setNextChannelIdForTesting(0xfffd);
    expect(b.open(empty).id).toBe(0xfffd);
    expect(b.open(empty).id).toBe(0x10001);
    a.setNextChannelIdForTesting(0xfffefffffffe);
    expect(a.open(empty).id).toBe(0xfffefffffffe);
    expect(a.open(empty).id).toBe(0xffff00000002);
    a.setNextChannelIdForTesting(0xfffffffffffe);
    expect(a.open(empty).id).toBe(0xfffffffffffe);
    expectThrowsCode(() => a.open(empty), StatusCode.resourceExhausted);
    expect(() => a.setNextChannelIdForTesting(3)).toThrow(RangeError);
    // The peer sees the long ids as they are.
    const seen = collect(b);
    await until(() => seen.length === 3);
    expect(seen.map((c) => c.id)).toEqual([0xfffefffffffe, 0xffff00000002, 0xfffffffffffe]);
  });

  test('the OPEN record is the first thing on the stream, written as the channel opens', async () => {
    const [connection, raw] = await bindingAndRaw({ bindingIsInitiator: true });
    const channel = connection.open(bytes('payload'));
    const stream = await raw.nextStream();
    const reader = new RecordReader(stream);
    const open = await within(reader.next(1 << 20));
    expect(open?.command).toBe(QuicRecordCommand.open);
    const body = decodeOpenBody(open?.body ?? empty);
    expect(body.id).toBe(2);
    expect(text(body.openPayload)).toBe('payload');
    await channel.send(bytes('x'));
    const data = await within(reader.next(1 << 20));
    expect(data?.command).toBe(QuicRecordCommand.data);
    expect(text(data?.body ?? empty)).toBe('x');
  });

  test('channels arriving before the control stream are handled under the defaults', async () => {
    const [connection, raw] = await bindingAndRaw({ rawControl: false });
    const arriving = nextChannel(connection);
    await raw.openRaw(
      concat([
        encodeOpenRecord(2, bytes('early')),
        encodeDataRecord(new Uint8Array(quicDefaultRecordLimit)),
      ]),
    );
    const channel = await arriving;
    expect(text(channel.openPayload)).toBe('early');
    expect((await firstChunk(channel)).length).toBe(quicDefaultRecordLimit);
    await raw.openControl();
    expect((await raw.nextControl()).type).toBe(quicHelloType);
    expect(connection.isOpen).toBe(true);
  });

  test('a stream reset before its OPEN record is complete is ignored', async () => {
    const [connection, raw] = await bindingAndRaw();
    const seen = collect(connection);
    const stream = await raw.openRaw(hexBytes('06 00 00'));
    await settle();
    stream.reset(1);
    await settle(50);
    expect(seen).toHaveLength(0);
    expect(connection.isOpen).toBe(true);
    await raw.sendControl(MuxControlMessage.ping(bytes('alive')));
    const pong = await raw.nextControlOf(MuxControlType.pong);
    expect(text(pong.payload)).toBe('alive');
  });
});

describe('subframes', () => {
  test("whole and in order, empty ones included, at their record's cost", async () => {
    const [a, b] = await connectionPair();
    const arriving = nextChannel(b);
    const channel = a.open(empty);
    expect(channel.sendCost(10)).toBe(14);
    expect(channel.sendCost(0)).toBe(4);
    const frames = [bytes('one'), empty, pattern(30000), bytes('four'), empty];
    for (const frame of frames) {
      void channel.send(frame);
    }
    // Read while closing: unread, the CLOSE record would wait for the
    // confirmation timeout.
    const receiving = readAll(await arriving);
    await within(channel.close());
    expectChunks(await receiving, frames);
  });

  test("the largest subframe is the peer's record limit, which its HELLO raises", async () => {
    const [a, b] = await connectionPair({ acceptorQuic: { recordLimit: 1 << 20 } });
    await until(() => a.peerHello !== undefined);
    expect(a.peerRecordLimit).toBe(1 << 20);
    expect(b.peerRecordLimit).toBe(quicDefaultRecordLimit);
    const arriving = nextChannel(b);
    const channel = a.open(empty);
    expect(channel.maxSubframeLength).toBe(1 << 20);
    const big = pattern(400000);
    await within(channel.send(big));
    const remote = await arriving;
    expectChunks([await firstChunk(remote)], [big]);
    expect(remote.maxSubframeLength).toBe(quicDefaultRecordLimit);
    expectThrowsCode(
      () => remote.send(new Uint8Array(quicDefaultRecordLimit + 1)),
      StatusCode.frameTooLarge,
    );
  });

  test('a record over the record limit ends the connection with FRAME_TOO_LARGE, its body unread', async () => {
    const [connection, raw] = await bindingAndRaw();
    collect(connection, [], (c) => listen(c));
    const header = new Uint8Array(4);
    writeQuicRecordHeader(header, 0, QuicRecordCommand.data, quicDefaultRecordLimit + 1);
    await raw.openRaw(concat([encodeOpenRecord(2, []), header]));
    expectStatus(await within(connection.done), StatusCode.frameTooLarge);
    const end = await raw.end;
    expect(end.code).toBe(StatusCode.frameTooLarge);
    expect(end.reason).toContain('32753');
  });

  test("a bulk channel sends a payload in records of at most the chunk size and the peer's limit", async () => {
    const [a, b] = await connectionPair({ initiator: { ...fast, bulkChunkSize: 10000 } });
    const arriving = nextChannel(b);
    const channel = a.open(empty);
    channel.priority = 'bulk';
    const payload = pattern(100000);
    const receiving = readAll(await arriving);
    await within(channel.send(payload));
    await within(channel.close());
    const chunks = await receiving;
    expect(chunks.every((c) => c.length <= 10000)).toBe(true);
    expectChunks([concat(chunks)], [payload]);
  });
});

describe('close', () => {
  test("a CLOSE record and FIN, confirmed by the peer's", async () => {
    const [connection, raw] = await bindingAndRaw({ bindingIsInitiator: true });
    const channel = connection.open(empty);
    void channel.send(bytes('last'));
    const closing = channel.close();
    const stream = await raw.nextStream();
    const [records, end] = await RawPeer.drain(stream);
    expect(records.map((r) => r.command)).toEqual([
      QuicRecordCommand.open,
      QuicRecordCommand.data,
      QuicRecordCommand.close,
    ]);
    expect(records.at(-1)?.body).toHaveLength(0);
    expect(end).toBe('fin');
    expect(channel.state).toBe('halfClosedLocal');
    await stream.write(encodeCloseRecord(Status.ok));
    await stream.finish();
    await within(closing);
    expect((await channel.done).equals(Status.ok)).toBe(true);
    expect(connection.openChannelCount).toBe(0);
  });

  test('a status with its reason and the fields of MOVED travel in the CLOSE record', async () => {
    const [a, b] = await connectionPair();
    const arriving = nextChannel(b);
    const channel = a.open(empty);
    const remote = await arriving;
    listen(remote);
    const moved = new MovedStatus({ owner: 0x1a2b, epoch: 7, reason: 'moved' });
    await within(remote.close(moved.toStatus()));
    const status = await within(channel.done);
    expect(status.known).toBe(StatusCode.moved);
    const fields = MovedStatus.fromStatus(status);
    expect([fields.owner, fields.epoch, fields.reason]).toEqual([0x1a2b, 7, 'moved']);
  });

  test("a rejection: a CLOSE record and STOP_SENDING with the status; the opener reports the record's status, reason included", async () => {
    const [connection, raw] = await bindingAndRaw();
    collect(connection, [], (c) => {
      void c.close(Status.of(StatusCode.notFound, 'no such service'));
    });
    const stream = await raw.openRaw(
      concat([encodeOpenRecord(2, []), encodeDataRecord(bytes('request'))]),
    );
    expect(await within(stream.stopped)).toBe(StatusCode.notFound);
    const [records, end] = await RawPeer.drain(stream);
    expect(records).toHaveLength(1);
    expect(records[0]?.command).toBe(QuicRecordCommand.close);
    expect(
      decodeCloseBody(records[0]?.body ?? empty).equals(
        Status.of(StatusCode.notFound, 'no such service'),
      ),
    ).toBe(true);
    expect(end).toBe('fin');
    // Between two bindings the opener reports the record's status.
    const [a, b] = await connectionPair();
    collect(b, [], (c) => {
      void c.close(Status.of(StatusCode.notFound, 'nope'));
    });
    const channel = a.open(empty);
    quietly(channel.send(bytes('request')));
    expect((await within(channel.done)).equals(Status.of(StatusCode.notFound, 'nope'))).toBe(true);
  });

  test("a reset is the peer's close by its code: a partial record is discarded, and this side closes in turn", async () => {
    const [connection, raw] = await bindingAndRaw();
    const arriving = nextChannel(connection);
    const stream = await raw.openRaw(
      concat([
        encodeOpenRecord(2, []),
        encodeDataRecord(bytes('whole')),
        hexBytes('05 00 00 00 01 02'),
      ]),
    );
    const channel = await arriving;
    const received = listen(channel);
    await until(() => received.length > 0);
    stream.reset(StatusCode.permissionDenied);
    expect((await within(channel.done)).equals(Status.of(StatusCode.permissionDenied))).toBe(true);
    expect(received.map(text)).toEqual(['whole']);
    const [records, end] = await RawPeer.drain(stream);
    expect(records).toHaveLength(1);
    expect(records[0]?.command).toBe(QuicRecordCommand.close);
    expect(records[0]?.body).toHaveLength(0);
    expect(end).toBe('fin');
  });

  test('a reset code above 255 reads as UNKNOWN', async () => {
    const [connection, raw] = await bindingAndRaw();
    const arriving = nextChannel(connection);
    const stream = await raw.openRaw(encodeOpenRecord(2, []));
    const channel = await arriving;
    stream.reset(300);
    const status = await within(channel.done);
    expect(status.known).toBe(StatusCode.unknown);
    expect(status.reason).toContain('300');
  });

  test("STOP_SENDING is the peer's close arriving early: this side resets with the code and reports the CLOSE record that follows", async () => {
    const [connection, raw] = await bindingAndRaw({ bindingIsInitiator: true });
    const channel = connection.open(empty);
    const stream = await raw.nextStream();
    const reader = new RecordReader(stream);
    expect((await within(reader.next(100)))?.command).toBe(QuicRecordCommand.open);
    stream.stop(StatusCode.notFound);
    const error = await rejection(reader.next(100));
    expect(error).toBeInstanceOf(QuicStreamReset);
    expect((error as QuicStreamReset).code).toBe(StatusCode.notFound);
    await until(() => !channel.canSend);
    expectThrowsCode(() => channel.send(bytes('late')), StatusCode.failedPrecondition);
    await stream.write(encodeCloseRecord(Status.of(StatusCode.notFound, 'gone')));
    await stream.finish();
    expect((await within(channel.done)).equals(Status.of(StatusCode.notFound, 'gone'))).toBe(true);
  });

  test('the confirmation timeout sends STOP_SENDING DEADLINE_EXCEEDED; the close is OK when it went out', async () => {
    const [connection, raw] = await bindingAndRaw({
      bindingIsInitiator: true,
      options: { ...fast, closeConfirmTimeout: 150 },
    });
    const channel = connection.open(empty);
    const closing = channel.close();
    const stream = await raw.nextStream();
    expect(await within(stream.stopped)).toBe(StatusCode.deadlineExceeded);
    await within(closing);
    expect((await channel.done).equals(Status.ok)).toBe(true);
    expect(connection.openChannelCount).toBe(0);
  });

  test("STOP_SENDING after the peer's CLOSE record ends a close in turn stalled at the peer's window at once", async () => {
    // The peer closed, then gave up waiting for the confirmation, which
    // waits behind a window the peer does not read: its STOP_SENDING ends
    // the channel here too, not this side's own timeout (5 s).
    const [connection, raw] = await bindingAndRaw({
      bindingIsInitiator: true,
      rawEnd: { streamWindow: 1000 },
    });
    const channel = connection.open(empty);
    listen(channel);
    // The OPEN record (10 bytes) and this DATA record fill the window.
    await within(channel.send(pattern(1000 - 10 - 4)));
    const stream = await raw.nextStream();
    await stream.write(encodeCloseRecord(Status.ok));
    await stream.finish();
    await until(() => channel.state === 'closed');
    stream.stop(StatusCode.deadlineExceeded);
    expectStatus(await within(channel.done, 2000), StatusCode.deadlineExceeded);
    expect(connection.openChannelCount).toBe(0);
  });

  test('the confirmation timeout resets a direction whose CLOSE is stuck behind data the peer does not read: DEADLINE_EXCEEDED', async () => {
    const [connection, raw] = await bindingAndRaw({
      bindingIsInitiator: true,
      options: { ...fast, closeConfirmTimeout: 150 },
      rawEnd: { streamWindow: 1000 },
    });
    const channel = connection.open(empty);
    const sending = channel.send(pattern(5000));
    quietly(sending);
    const closing = channel.close();
    const stream = await raw.nextStream();
    await within(closing);
    expectStatus(await channel.done, StatusCode.deadlineExceeded);
    await expectCode(sending, StatusCode.failedPrecondition);
    const [, end] = await RawPeer.drain(stream);
    expect(end).toBe(StatusCode.deadlineExceeded);
  });

  test('a close whose CLOSE record waits behind data the peer does not read is reset with its own status at the confirmation timeout', async () => {
    const [connection, raw] = await bindingAndRaw({
      bindingIsInitiator: true,
      options: { ...fast, closeConfirmTimeout: 150 },
      rawEnd: { streamWindow: 1000 },
    });
    const channel = connection.open(empty);
    quietly(channel.send(pattern(5000)));
    const stream = await raw.nextStream();
    await within(channel.close(Status.of(StatusCode.notFound, 'gone')));
    expect((await channel.done).equals(Status.of(StatusCode.notFound, 'gone'))).toBe(true);
    // The peer reads nothing until the confirmation timed out.
    await settle(300);
    const [, end] = await RawPeer.drain(stream);
    expect(end).toBe(StatusCode.notFound);
  });

  test("a side that closes without reading still finds the peer's CLOSE behind what it did not take", async () => {
    const [a, b] = await connectionPair({
      initiator: { ...fast, closeConfirmTimeout: limit },
      acceptor: { ...fast, closeConfirmTimeout: limit },
    });
    const arriving = nextChannel(b);
    const channel = a.open(empty);
    void channel.send(pattern(3000));
    const remote = await arriving;
    await settle();
    // Nobody listens on the remote side, which closes.
    const start = Date.now();
    await within(remote.close());
    expect(Date.now() - start).toBeLessThan(2000);
    expect((await remote.done).equals(Status.ok)).toBe(true);
    expect((await within(channel.done)).equals(Status.ok)).toBe(true);
  });

  test('a failure close does not wait: what the peer still sends is not read', async () => {
    const [a, b] = await connectionPair();
    const arriving = nextChannel(b);
    const channel = a.open(empty);
    const remote = await arriving;
    await within(channel.close(Status.of(StatusCode.aborted, 'enough')));
    expect((await channel.done).equals(Status.of(StatusCode.aborted, 'enough'))).toBe(true);
    listen(remote);
    expect((await within(remote.done)).equals(Status.of(StatusCode.aborted, 'enough'))).toBe(true);
  });
});

describe('flow control', () => {
  test('a reader that stops stalls its own channel and nothing else', async () => {
    const [a, b] = await connectionPair();
    const atB = collect(b);
    const slow = a.open(bytes('slow'));
    const other = a.open(bytes('other'));
    await until(() => atB.length === 2);
    // A reader that has not read yet: Dart's paused subscription.
    const paused = (atB[0] as MuxChannel).readable.getReader();
    const otherReceived = listen(atB[1] as MuxChannel);
    const stuck = slow.send(pattern(30000));
    for (let i = 0; i < 10; i++) {
      void slow.send(pattern(30000));
    }
    await settle(50);
    for (let i = 0; i < 20; i++) {
      await within(other.send(bytes(`${i}`)));
    }
    await until(() => otherReceived.length === 20);
    // The stalled channel holds at most one record beyond the window.
    expect(slow.isWritable).toBe(false);
    expect((atB[0] as MuxChannel).bufferedBytes).toBeLessThanOrEqual(30004);
    // Resumed.
    let reading = true;
    const resumed = (async () => {
      while (reading) {
        const result = await paused.read();
        if (result.done) {
          return;
        }
      }
    })();
    await within(stuck);
    await until(() => slow.isWritable);
    reading = false;
    await paused.cancel();
    await resumed.catch(() => {});
  });

  test('with manualCredit the next record is read once the previous one is consumed; grant reads further ahead', async () => {
    const [a, b] = await connectionPair();
    const arriving = nextChannel(b);
    const channel = a.open(empty);
    for (let i = 0; i < 20; i++) {
      void channel.send(new Uint8Array(1000));
    }
    const remote = await arriving;
    remote.manualCredit = true;
    const received = listen(remote);
    await settle(50);
    expect(received).toHaveLength(1);
    expect(remote.heldBytes).toBe(1004);
    remote.consumed(1000);
    await until(() => received.length === 2);
    // Read ahead while what is held stays below the grant.
    remote.grant(5000);
    await until(() => received.length === 6);
    await settle(30);
    expect(received).toHaveLength(6);
    expect(remote.heldBytes).toBe(5 * 1004);
    expect(remote.receiveWindow).toBeGreaterThan(0);
    remote.manualCredit = false;
    await until(() => received.length === 20);
  });
});

describe('priorities', () => {
  test('2 for the control stream, 1 for ordinary channels, 0 for bulk ones', async () => {
    const [connection, raw] = await bindingAndRaw({ bindingIsInitiator: true });
    const initiator = raw.session.peer;
    const control = initiator.streams[0];
    expect(control?.lastPriority).toBe(quicControlPriority);
    const channel = connection.open(empty);
    await until(() => initiator.streams.length === 2);
    await settle();
    const stream = initiator.streams[1];
    expect(stream?.lastPriority).toBe(quicOrdinaryPriority);
    channel.priority = 'bulk';
    expect(stream?.lastPriority).toBe(quicBulkPriority);
  });
});

describe('openAfter', () => {
  test('the child stream opens once what the parent was given before was taken, and may reach the peer first', async () => {
    const network = new MemoryQuicNetwork({ delay: (info) => (info.ordinal === 1 ? 80 : 0) });
    const [a, b] = await connectionPair({ network });
    const atB = collect(b);
    const parent = a.open(bytes('parent'));
    void parent.send(bytes('before'));
    const child = parent.openAfter(bytes('child'));
    void child.send(bytes('child data'));
    await until(() => atB.length > 0);
    expect(text((atB[0] as MuxChannel).openPayload)).toBe('child');
    await until(() => atB.length === 2);
    expect(text((atB[1] as MuxChannel).openPayload)).toBe('parent');
    expect(a.hasOrderedChannels).toBe(false);
  });

  test('a child whose parent stops sending before its turn is never opened: CANCELLED', async () => {
    const [connection, raw] = await bindingAndRaw({
      bindingIsInitiator: true,
      rawEnd: { streamWindow: 1000 },
    });
    const parent = connection.open(empty);
    quietly(parent.send(pattern(5000)));
    const child = parent.openAfter(bytes('child'));
    const stream = await raw.nextStream();
    stream.reset(StatusCode.aborted);
    expectStatus(await within(parent.done), StatusCode.aborted);
    expectStatus(await within(child.done), StatusCode.cancelled);
    expect(raw.session.announced).toHaveLength(2);
  });
});

/** The `_bulk` open payload of bulk channel [number] of channel [parentId]: Talk's `TalkBulkOpen`. */
function bulkOpen(parentId: number, number: number): Uint8Array {
  const payload = new Uint8Array(10);
  writeU48(payload, 0, parentId);
  writeU32(payload, 6, number);
  return new ChannelAddress({ type: '_bulk', payload }).encode();
}

/**
 * Admits the `_bulk` continuations of [connection]'s channels after its
 * GOAWAY, as the Talk layer's rule does: the parent is open, or retained
 * for the drain.
 */
function admitBulk(connection: QuicBindingConnection): void {
  connection.admitAfterGoAway((openPayload) => {
    const address = ChannelAddress.decode(openPayload);
    if (address.type?.toString() !== '_bulk' || address.payload.length < 10) {
      return false;
    }
    const parentId = readU48(address.payload, 0);
    return (
      connection.channelWithId(parentId) !== undefined || connection.retainedForDrain(parentId)
    );
  });
}

describe('GOAWAY and the end of the connection', () => {
  test('GOAWAY: no new channels either way, the open ones finish, then the session closes', async () => {
    const [a, b] = await connectionPair();
    admitBulk(b);
    const atB = collect(b);
    const channel = a.open(empty);
    await until(() => atB.length === 1);
    const going = b.goAway();
    await until(() => a.peerGoingAway);
    expectThrowsCode(() => a.open(empty), StatusCode.failedPrecondition);
    expectThrowsCode(() => b.open(empty), StatusCode.failedPrecondition);
    // A bulk channel for the open channel still goes.
    const bulk = channel.openAfter(bulkOpen(channel.id, 1));
    await until(() => atB.length === 2);
    listen(atB[1] as MuxChannel);
    await within(bulk.close());
    listen(atB[0] as MuxChannel);
    await within(channel.close());
    await within(going);
    expectStatus(await within(a.done), StatusCode.goingAway);
    expectStatus(await within(b.done), StatusCode.goingAway);
  });

  test('a channel the peer opened before it saw the GOAWAY is rejected GOING_AWAY, a bulk channel for an open one admitted', async () => {
    const [connection, raw] = await bindingAndRaw();
    admitBulk(connection);
    const atBinding = collect(connection);
    await raw.openRaw(encodeOpenRecord(2, []));
    await until(() => atBinding.length === 1);
    void connection.goAway();
    expectStatus(
      (await raw.nextControlOf(MuxControlType.goAway)).goAwayStatus,
      StatusCode.goingAway,
    );
    const late = await raw.openRaw(encodeOpenRecord(4, []));
    expect(await within(late.stopped)).toBe(StatusCode.goingAway);
    const [records] = await RawPeer.drain(late);
    expect(records).toHaveLength(1);
    expectStatus(decodeCloseBody(records[0]?.body ?? empty), StatusCode.goingAway);
    await raw.openRaw(encodeOpenRecord(6, bulkOpen(2, 1)));
    await until(() => atBinding.length === 2);
    expect(atBinding[1]?.id).toBe(6);
  });

  test('after GOAWAY, a bulk channel whose parent was admitted before it is admitted even once the parent ended; one for a parent never admitted is not', async () => {
    const [connection, raw] = await bindingAndRaw();
    admitBulk(connection);
    const atBinding = collect(connection, [], (c) => listen(c));
    const parent = await raw.openRaw(encodeOpenRecord(2, []));
    // Another channel keeps the connection busy meanwhile.
    await raw.openRaw(encodeOpenRecord(4, []));
    await until(() => atBinding.length === 2);
    void connection.goAway();
    await raw.nextControlOf(MuxControlType.goAway);
    const bulkPending = new Deferred<void>();
    connection.retainForDrain(2, bulkPending.promise);
    onTestFinished(() => {
      bulkPending.resolve();
    });
    // The parent's close overtakes its bulk stream.
    await parent.write(encodeCloseRecord(Status.ok));
    await parent.finish();
    await until(() => connection.channelWithId(2) === undefined);
    await raw.openRaw(encodeOpenRecord(6, bulkOpen(2, 1)));
    await until(() => atBinding.length === 3);
    expect(atBinding[2]?.id).toBe(6);
    const stray = await raw.openRaw(encodeOpenRecord(8, bulkOpen(10, 1)));
    expect(await within(stray.stopped)).toBe(StatusCode.goingAway);
  });

  test('a received application close counts as a GOAWAY with its status', async () => {
    const cases: [number, string, Status | undefined][] = [
      [0, '', Status.of(StatusCode.goingAway)],
      [14, 'maintenance', Status.of(StatusCode.unavailable, 'maintenance')],
      [300, 'odd', undefined],
    ];
    for (const [code, reason, expected] of cases) {
      const [connection, raw] = await bindingAndRaw();
      const arriving = nextChannel(connection);
      await raw.openRaw(encodeOpenRecord(2, []));
      const channel = await arriving;
      await within(raw.session.close(code, reason));
      const status = await within(connection.peerGoAwayStatus);
      if (expected !== undefined) {
        expect(status.equals(expected), `${status}`).toBe(true);
      } else {
        expect(status.known).toBe(StatusCode.unknown);
      }
      expectStatus(await within(connection.done), StatusCode.goingAway);
      expectStatus(await within(channel.done), StatusCode.goingAway);
    }
  });

  test('a lost session ends the connection and its channels with CONNECTION_LOST', async () => {
    const [connection, raw] = await bindingAndRaw();
    const arriving = nextChannel(connection);
    await raw.openRaw(encodeOpenRecord(2, []));
    const channel = await arriving;
    raw.session.lose();
    expectStatus(await within(connection.done), StatusCode.connectionLost);
    expectStatus(await within(channel.done), StatusCode.connectionLost);
  });

  test("close waits for the FIN of a channel still waiting for the peer's close", async () => {
    const network = new MemoryQuicNetwork({ delay: (info) => (info.ordinal === 1 ? 60 : 0) });
    const [a, b] = await connectionPair({ network });
    const arriving = nextChannel(b);
    const channel = a.open(empty);
    const remote = await arriving;
    listen(remote);
    void channel.close();
    await until(() => (network.pairs[0]?.[0].streams[1]?.finished ?? false) === true);
    await within(a.close());
    // The CLOSE arrived before the session ended.
    expect((await within(remote.done)).equals(Status.ok)).toBe(true);
  });

  test('close waits for the CLOSE records this side wrote to be acknowledged', async () => {
    const network = new MemoryQuicNetwork({ delay: (info) => (info.ordinal === 1 ? 60 : 0) });
    const [a, b] = await connectionPair({ network });
    const arriving = nextChannel(b);
    const channel = a.open(empty);
    const remote = await arriving;
    listen(remote);
    const remoteDone = remote.done;
    void remote.close();
    await until(() => channel.state !== 'open');
    await within(channel.close());
    await within(a.close());
    // The confirming CLOSE arrived before the session closed.
    expect((await within(remoteDone)).equals(Status.ok)).toBe(true);
  });
});

describe('keep-alive and the control stream', () => {
  test('a peer that does not answer PING is lost after the keep-alive timeout', async () => {
    const [connection, raw] = await bindingAndRaw({
      options: { ...fast, keepAliveInterval: 60, keepAliveTimeout: 60 },
    });
    const status = await within(connection.done);
    expect(status.known).toBe(StatusCode.connectionLost);
    expect(status.reason).toBe('keep-alive timeout');
    expect((await raw.end).code).toBe(StatusCode.deadlineExceeded);
  });

  test('two bindings with keep-alive stay connected', async () => {
    const options = { ...fast, keepAliveInterval: 30, keepAliveTimeout: 60 };
    const [a, b] = await connectionPair({ initiator: options, acceptor: options });
    await settle(300);
    expect(a.isOpen && b.isOpen).toBe(true);
  });

  test('an acceptor without a control stream in time closes with PROTOCOL_ERROR', async () => {
    const [connection, raw] = await bindingAndRaw({
      rawControl: false,
      quicOptions: { controlStreamTimeout: 100 },
    });
    expectStatus(await within(connection.done), StatusCode.protocolError);
    expect((await raw.end).code).toBe(StatusCode.protocolError);
  });

  test("over WebTransport, a control stream reset without a code (the peer's WT_SESSION_GONE) waits for the session's end, which ends the connection as the close says", async () => {
    const [connection, raw] = await bindingAndRaw({ isWebTransport: true });
    await until(() => connection.peerHello !== undefined);
    (raw.control as MemoryQuicStream).resetWithoutCode();
    await settle(100);
    expect(connection.isOpen).toBe(true);
    await within(raw.session.close(StatusCode.unavailable, 'upgrade'));
    // A received close: a GOAWAY with its status, then the end.
    expect(
      (await within(connection.peerGoAwayStatus)).equals(
        Status.of(StatusCode.unavailable, 'upgrade'),
      ),
    ).toBe(true);
    expectStatus(await within(connection.done), StatusCode.goingAway);
  });

  test('over WebTransport, STOP_SENDING without a code on the control stream that no session end follows is a protocol error after the grace', async () => {
    const [connection, raw] = await bindingAndRaw({ isWebTransport: true });
    await until(() => connection.peerHello !== undefined);
    const start = Date.now();
    (raw.control as MemoryQuicStream).stopWithoutCode();
    expectStatus(await within(connection.done, 10_000), StatusCode.protocolError);
    expect(Date.now() - start).toBeGreaterThanOrEqual(
      QuicBindingConnection.webTransportSessionGoneGrace - 50,
    );
  });

  test('over WebTransport, a control stream reset with a code is a protocol error at once', async () => {
    const [connection, raw] = await bindingAndRaw({ isWebTransport: true });
    await until(() => connection.peerHello !== undefined);
    raw.control?.reset(StatusCode.cancelled);
    expectStatus(await within(connection.done), StatusCode.protocolError);
  });

  test("the control stream is not read while this side's control output backs up", async () => {
    const [connection, raw] = await bindingAndRaw({
      quicOptions: { controlOutputMark: 200 },
      rawEnd: { streamWindow: 2000 },
      bindingEnd: { streamWindow: 4000 },
    });
    // The raw peer sends PINGs and reads nothing: PONGs fill its window,
    // then the binding's output queue, then the binding stops reading.
    let written = 0;
    const pinging = (async () => {
      for (let i = 0; i < 2000; i++) {
        await raw.sendControl(MuxControlMessage.ping(new Uint8Array(100)));
        written++;
      }
    })();
    await settle(200);
    expect(written).toBeLessThan(2000);
    const stalled = written;
    await settle(100);
    expect(written).toBe(stalled);
    expect(connection.isOpen).toBe(true);
    // Reading lets everything through.
    let pongs = 0;
    void (async () => {
      while (pongs < 2000) {
        const message = await raw.nextControl();
        if (message.type === MuxControlType.pong) {
          pongs++;
        }
      }
    })();
    await within(pinging);
    await until(() => pongs === 2000);
  });
});

describe('resource limits', () => {
  test('channels beyond maxChannels are rejected RESOURCE_EXHAUSTED, with STOP_SENDING', async () => {
    const [connection, raw] = await bindingAndRaw({ options: { ...fast, maxChannels: 2 } });
    await raw.openRaw(encodeOpenRecord(2, []));
    await raw.openRaw(encodeOpenRecord(4, []));
    const third = await raw.openRaw(encodeOpenRecord(6, []));
    expect(await within(third.stopped)).toBe(StatusCode.resourceExhausted);
    const [records, end] = await RawPeer.drain(third);
    expect(records).toHaveLength(1);
    expectStatus(decodeCloseBody(records[0]?.body ?? empty), StatusCode.resourceExhausted);
    expect(end).toBe('fin');
    expect(connection.openChannelCount).toBe(2);
  });

  test('open payloads beyond their budget are rejected RESOURCE_EXHAUSTED', async () => {
    const [connection, raw] = await bindingAndRaw({
      options: { ...fast, maxOpenPayloadBytes: 100 },
    });
    await raw.openRaw(encodeOpenRecord(2, new Uint8Array(80)));
    const second = await raw.openRaw(encodeOpenRecord(4, new Uint8Array(80)));
    expect(await within(second.stopped)).toBe(StatusCode.resourceExhausted);
    expect(connection.openPayloadBytes).toBe(80);
  });

  test("the peer's HELLO states its channels, which are advisory: an open beyond them waits for the stack's stream credit", async () => {
    const [connection, raw] = await bindingAndRaw({
      bindingIsInitiator: true,
      rawHello: new QuicHello({ channels: 2 }),
    });
    await until(() => connection.peerHello !== undefined);
    expect(connection.peerMaxChannels).toBe(2);
    for (let i = 0; i < 3; i++) {
      connection.open(empty);
    }
    // The stack granted more streams than the HELLO stated.
    for (let i = 0; i < 3; i++) {
      await raw.nextStream();
    }
  });

  test('a side states the channels it accepts: its stream limit less the control stream, within maxChannels', async () => {
    const [a, b] = await connectionPair({
      initiator: { ...fast, maxChannels: 50 },
      acceptorEnd: { maxStreams: 11 },
    });
    await until(() => a.peerHello !== undefined && b.peerHello !== undefined);
    expect(a.peerMaxChannels).toBe(10);
    expect(b.peerMaxChannels).toBe(50);
  });

  test('an open without stream credit waits, then fails RESOURCE_EXHAUSTED', async () => {
    const [connection] = await bindingAndRaw({
      bindingIsInitiator: true,
      rawEnd: { maxStreams: 2 },
      quicOptions: { openStreamTimeout: 100 },
    });
    const first = connection.open(empty);
    const second = connection.open(empty);
    expectStatus(await within(second.done), StatusCode.resourceExhausted);
    expect(first.state).toBe('open');
  });

  test('control messages held during an identification are bounded', async () => {
    const held = new HeldVerifier();
    const [connection, raw] = await bindingAndRaw({
      options: { ...fast, identityVerifier: held },
      quicOptions: { maxHeldControlMessages: 3 },
    });
    await raw.sendControl(MuxControlMessage.nonce(nonceOf(1)));
    await raw.nextControlOf(MuxControlType.nonce);
    await raw.sendControl(MuxControlMessage.ident(new MuxIdent({ credential: new Uint8Array(1) })));
    for (let i = 0; i < 4; i++) {
      await raw.sendControl(MuxControlMessage.ping(new Uint8Array(1)));
    }
    expectStatus(await within(connection.done), StatusCode.resourceExhausted);
  });
});

/** A nonce of 32 bytes, the first one [first]. */
function nonceOf(first: number): Uint8Array {
  const nonce = new Uint8Array(32);
  nonce[0] = first;
  return nonce;
}

describe('identity', () => {
  let holder: HolderKey;

  beforeAll(async () => {
    holder = await Ed25519HolderKey.generate();
  });

  const credentialOf = (identity: string) => credentialFor(identity, holder.publicKey);

  /** The exporter of [session]. */
  async function exporterOf(session: MemoryQuicSession): Promise<Uint8Array> {
    return (await session.exportKeyingMaterial(
      rawQuicExporterLabel,
      new Uint8Array(0),
      32,
    )) as Uint8Array;
  }

  /** The binding's IDENT as the raw peer sees it, after the nonces. */
  async function identOf(
    connection: QuicBindingConnection,
    raw: RawPeer,
  ): Promise<[MuxIdent, Uint8Array, Uint8Array]> {
    const identifying = connection.identify(await credentialOf('npc-7'), { holderKey: holder });
    const nonce = await raw.nextControlOf(MuxControlType.nonce);
    const ours = new Uint8Array(32).fill(0x42);
    await raw.sendControl(MuxControlMessage.nonce(ours));
    const ident = MuxIdent.decode((await raw.nextControlOf(MuxControlType.ident)).payload);
    const ping = await raw.nextControlOf(MuxControlType.ping);
    await raw.sendControl(MuxControlMessage.pong(ping.payload));
    await within(identifying);
    return [ident, nonce.payload, ours];
  }

  test("the exporter: RFC 9266's label on raw QUIC, the application label over WebTransport", async () => {
    for (const webTransport of [false, true]) {
      const [a, b] = MemoryQuicSession.pair();
      const starting = QuicBindingConnection.start(b, {
        options: fast,
        isWebTransport: webTransport,
      });
      const connection = await QuicBindingConnection.start(a, {
        options: fast,
        isWebTransport: webTransport,
      });
      const accepted = await starting;
      closeAfter(connection);
      closeAfter(accepted);
      expect(a.exporterLabels).toEqual([
        webTransport ? 'switchboard-ident' : 'EXPORTER-Channel-Binding',
      ]);
      expect(connection.hasExporter).toBe(true);
      await until(() => connection.exporterBound && accepted.exporterBound);
    }
    // A stack without one announces none.
    const [connection, raw] = await bindingAndRaw({
      bindingIsInitiator: true,
      bindingEnd: { exporter: false },
      rawHello: new QuicHello({ exporter: true }),
    });
    expect(connection.hasExporter).toBe(false);
    await until(() => connection.peerHello !== undefined);
    expect(connection.exporterBound).toBe(false);
    expect(raw.session.peer.exporterLabels).toHaveLength(1);
  });

  test('both HELLOs with EXPORTER: the proof signs the exporter', async () => {
    const [connection, raw] = await bindingAndRaw({
      bindingIsInitiator: true,
      rawHello: new QuicHello({ exporter: true }),
    });
    await until(() => connection.peerHello !== undefined);
    expect(connection.exporterBound).toBe(true);
    const [ident, senderNonce, receiverNonce] = await identOf(connection, raw);
    const exporter = await exporterOf(raw.session);
    const verifier = testVerifier();
    expect(
      (await verifyIdent(ident, { verifier, senderNonce, receiverNonce, exporter })).identity,
    ).toBe('npc-7');
    await expectCode(
      verifyIdent(ident, { verifier, senderNonce, receiverNonce }),
      StatusCode.unauthenticated,
    );
  });

  test('a HELLO without EXPORTER: the proof is nonce-bound', async () => {
    const [connection, raw] = await bindingAndRaw({
      bindingIsInitiator: true,
      rawHello: new QuicHello(),
    });
    await until(() => connection.peerHello !== undefined);
    expect(connection.exporterBound).toBe(false);
    const [ident, senderNonce, receiverNonce] = await identOf(connection, raw);
    expect(
      (await verifyIdent(ident, { verifier: testVerifier(), senderNonce, receiverNonce })).identity,
    ).toBe('npc-7');
  });

  test('a side that requires the binding neither sends nor accepts a nonce-bound IDENT', async () => {
    const rules = new QuicIdentityRules({ requireExporterBinding: true });
    // Sending: refused before anything is sent.
    const [sender, rawReceiver] = await bindingAndRaw({
      quicOptions: { identity: rules },
      rawHello: new QuicHello(),
    });
    await until(() => sender.peerHello !== undefined);
    const identifying = sender.identify(await credentialOf('npc-7'), { holderKey: holder });
    await rawReceiver.nextControlOf(MuxControlType.nonce);
    await rawReceiver.sendControl(MuxControlMessage.nonce(nonceOf(9)));
    await expectCode(identifying, StatusCode.unauthenticated);
    // Receiving: one is refused as failing verification.
    const [receiver, raw] = await bindingAndRaw({
      options: { ...fast, identityVerifier: testVerifier() },
      quicOptions: { identity: rules },
      rawHello: new QuicHello(),
    });
    await raw.sendControl(MuxControlMessage.nonce(nonceOf(1)));
    const theirs = await raw.nextControlOf(MuxControlType.nonce);
    const ours = nonceOf(1);
    const credential = await credentialOf('npc-7');
    await raw.sendControl(
      MuxControlMessage.ident(
        new MuxIdent({
          credential: credential.encode(),
          proof: await holder.sign(MuxIdent.proofMessage(ours, theirs.payload, empty, empty)),
        }),
      ),
    );
    expectStatus(await within(receiver.done), StatusCode.unauthenticated);
    expect((await raw.end).code).toBe(StatusCode.unauthenticated);
  });

  test('an initiator that requires the binding closes UNAUTHENTICATED on a HELLO without EXPORTER', async () => {
    const [connection, raw] = await bindingAndRaw({
      bindingIsInitiator: true,
      quicOptions: { identity: new QuicIdentityRules({ requireExporterBinding: true }) },
      rawHello: new QuicHello(),
    });
    expectStatus(await within(connection.done), StatusCode.unauthenticated);
    expect((await raw.end).code).toBe(StatusCode.unauthenticated);
  });

  test("an initiator awaiting the acceptor's identity opens no channel before it verified", async () => {
    const [connection, raw] = await bindingAndRaw({
      bindingIsInitiator: true,
      options: { ...fast, identityVerifier: testVerifier() },
      quicOptions: {
        identity: new QuicIdentityRules({ requireExporterBinding: true, awaitPeerIdentity: true }),
      },
      rawHello: new QuicHello({ exporter: true }),
    });
    connection.localIdentity = 'npc-7';
    const early = connection.open(bytes('early'));
    const identifying = connection.identify(await credentialOf('npc-7'), {
      holderKey: holder,
      receiver: 'server-1',
    });
    const theirs = await raw.nextControlOf(MuxControlType.nonce);
    const ours = nonceOf(3);
    await raw.sendControl(MuxControlMessage.nonce(ours));
    await raw.nextControlOf(MuxControlType.ident);
    const ping = await raw.nextControlOf(MuxControlType.ping);
    await raw.sendControl(MuxControlMessage.pong(ping.payload));
    await settle(50);
    // Identified, but the acceptor has not: nothing opened but the control
    // stream.
    expect(raw.session.announced).toEqual([0]);
    expect(early.isWritable).toBe(false);
    const exporter = await exporterOf(raw.session);
    await raw.sendControl(
      MuxControlMessage.ident(
        new MuxIdent({
          credential: (await credentialOf('server-1')).encode(),
          receiver: bytes('npc-7'),
          proof: await holder.sign(
            MuxIdent.proofMessage(ours, theirs.payload, empty, bytes('npc-7'), exporter),
          ),
        }),
      ),
    );
    await within(identifying);
    expect(connection.peerIdentity?.identity).toBe('server-1');
    expect(connection.peerIdentityBound).toBe(true);
    const stream = await raw.nextStream();
    const open = await within(new RecordReader(stream).next(100));
    expect(text(decodeOpenBody(open?.body ?? empty).openPayload)).toBe('early');
  });

  test('an acceptor identifies only to a peer that identified with an exporter-bound proof; a bearer credential only to an authenticated end', async () => {
    const [connection] = await bindingAndRaw({
      quicOptions: { identity: QuicIdentityRules.listener },
    });
    await expectCode(
      connection.identify(await credentialOf('server-1'), { holderKey: holder }),
      StatusCode.failedPrecondition,
    );
    const bearer = await credentialFor('bearer');
    const [initiator] = await bindingAndRaw({
      bindingIsInitiator: true,
      quicOptions: { identity: new QuicIdentityRules({ bearerOnlyToAuthenticatedPeer: true }) },
    });
    await expectCode(initiator.identify(bearer), StatusCode.failedPrecondition);
    expect(initiator.peerAuthenticated).toBe(false);
  });

  test('a holder key given with a bearer credential is ignored, its IDENT carries no proof; a holder-key credential without its key, or with another, is a TypeError', async () => {
    const [connection, raw] = await bindingAndRaw({
      bindingIsInitiator: true,
      rawHello: new QuicHello({ exporter: true }),
    });
    await until(() => connection.peerHello !== undefined);
    const npc7 = await credentialOf('npc-7');
    expect(() => connection.identify(npc7)).toThrow(TypeError);
    const other = await Ed25519HolderKey.generate();
    expect(() => connection.identify(npc7, { holderKey: other })).toThrow(TypeError);
    const bearer = await credentialFor('bearer');
    const identifying = connection.identify(bearer, { holderKey: holder });
    await raw.nextControlOf(MuxControlType.nonce);
    await raw.sendControl(MuxControlMessage.nonce(new Uint8Array(32).fill(0x42)));
    const ident = MuxIdent.decode((await raw.nextControlOf(MuxControlType.ident)).payload);
    expect(ident.proof).toHaveLength(0);
    expect(hexString(ident.credential)).toBe(hexString(bearer.encode()));
    const ping = await raw.nextControlOf(MuxControlType.ping);
    await raw.sendControl(MuxControlMessage.pong(ping.payload));
    await within(identifying);
  });

  test('a bearer IDENT is not exporter-bound: it authenticates nobody', async () => {
    const [connection, raw] = await bindingAndRaw({
      options: { ...fast, identityVerifier: testVerifier() },
      rawHello: new QuicHello({ exporter: true }),
    });
    await raw.nextControl();
    await raw.sendControl(MuxControlMessage.nonce(nonceOf(1)));
    await raw.nextControlOf(MuxControlType.nonce);
    const bearer = await credentialFor('bearer');
    await raw.sendControl(MuxControlMessage.ident(new MuxIdent({ credential: bearer.encode() })));
    await raw.sendControl(MuxControlMessage.ping(bytes('p')));
    await raw.nextControlOf(MuxControlType.pong);
    expect(connection.exporterBound).toBe(true);
    expect(connection.peerIdentity?.identity).toBe('bearer');
    expect(connection.peerIdentityBound).toBe(false);
    expect(connection.peerAuthenticated).toBe(false);
  });

  test("an initiator awaiting the acceptor's identity without a verifier could never use the connection: it closes at once", async () => {
    const [a] = MemoryQuicSession.pair();
    const connection = await QuicBindingConnection.start(a, {
      options: fast,
      quicOptions: { identity: new QuicIdentityRules({ awaitPeerIdentity: true }) },
    });
    expect(connection.isOpen).toBe(false);
    expectStatus(await within(connection.done), StatusCode.unauthenticated);
  });

  test('the control messages after an IDENT wait for its verification', async () => {
    const held = new HeldVerifier();
    const [connection, raw] = await bindingAndRaw({
      options: { ...fast, identityVerifier: held },
      rawHello: new QuicHello({ exporter: true }),
    });
    await raw.nextControl();
    await raw.sendControl(MuxControlMessage.nonce(nonceOf(1)));
    const theirs = await raw.nextControlOf(MuxControlType.nonce);
    const ours = nonceOf(1);
    const credential = await credentialOf('npc-7');
    const exporter = await exporterOf(raw.session);
    await raw.sendControl(
      MuxControlMessage.ident(
        new MuxIdent({
          credential: credential.encode(),
          proof: await holder.sign(
            MuxIdent.proofMessage(ours, theirs.payload, empty, empty, exporter),
          ),
        }),
      ),
    );
    await raw.sendControl(MuxControlMessage.ping(bytes('after')));
    await until(() => held.calls === 1);
    await settle(50);
    expect(connection.peerIdentity).toBeUndefined();
    held.result.resolve(credential);
    const pong = await raw.nextControlOf(MuxControlType.pong);
    expect(text(pong.payload)).toBe('after');
    expect(connection.peerIdentity?.identity).toBe('npc-7');
    expect(connection.peerIdentityBound).toBe(true);
  });

  test('an IDENT before the nonces, a second NONCE and a NONCE equal to ours are protocol errors', async () => {
    for (const mistake of ['early ident', 'second nonce', 'echoed nonce']) {
      const [connection, raw] = await bindingAndRaw();
      await raw.nextControl();
      switch (mistake) {
        case 'early ident':
          await raw.sendControl(
            MuxControlMessage.ident(new MuxIdent({ credential: new Uint8Array(1) })),
          );
          break;
        case 'second nonce':
          await raw.sendControl(MuxControlMessage.nonce(nonceOf(1)));
          await raw.sendControl(MuxControlMessage.nonce(nonceOf(2)));
          break;
        default: {
          connection.identify(await credentialOf('npc-7'), { holderKey: holder }).catch(() => {});
          const theirs = await raw.nextControlOf(MuxControlType.nonce);
          await raw.sendControl(MuxControlMessage.nonce(theirs.payload));
        }
      }
      expect((await within(connection.done)).code, mistake).toBe(StatusCode.protocolError);
    }
  });
});
