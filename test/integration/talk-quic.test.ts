/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Talk over the QUIC binding: QuicBindingConnections over the fake QUIC
// provider (test/unit/quic/harness.ts), where every channel is a stream
// of its own and streams are delivered independently, each in order, with
// a delay per stream. What differs from the frame mux, whose channels are
// ordered with each other (`hasOrderedChannels` true): Talk's bulk
// channels may overtake their references or come after them, and even
// before their parent's OPEN; a graceful close of the parent may come
// before its bulk payloads, which Talk waits for (lingering, with a drain
// hold on the connection); and after this side's GOAWAY, a `_bulk`
// continuation is admitted by the rule installBulkRouting installs
// (admitAfterGoAway), while another channel is refused GOING_AWAY. The
// network delivers on real time; Talk's timeouts are far off.

import { describe, expect, test } from 'vitest';
import { bytesEqual } from '../../src/core/bytes.ts';
import type { MuxChannel } from '../../src/core/mux/contract.ts';
import { MuxControlMessage, MuxControlType } from '../../src/core/mux/mux-frame.ts';
import { Name } from '../../src/core/name.ts';
import type { QuicStream } from '../../src/core/quic/contract.ts';
import type { QuicBindingConnection } from '../../src/core/quic/quic-binding-connection.ts';
import { RecordReader } from '../../src/core/quic/record-reader.ts';
import {
  decodeCloseBody,
  encodeCloseRecord,
  encodeDataRecord,
  encodeOpenRecord,
  type QuicRecord,
  QuicRecordCommand,
  quicDefaultRecordLimit,
} from '../../src/core/quic/records.ts';
import { Status, StatusCode } from '../../src/core/status.ts';
import type { TalkMessage, TalkOptions } from '../../src/core/talk/contract.ts';
import { TalkBulkOpen, TalkBulkReference } from '../../src/core/talk/talk-bulk.ts';
import { installBulkRouting } from '../../src/core/talk/talk-bulk-routes.ts';
import { TalkAbortError, TalkChannel } from '../../src/core/talk/talk-channel.ts';
import { TalkFrame, TalkKind } from '../../src/core/talk/talk-frame.ts';
import { Deferred } from '../../src/core/util/deferred.ts';
import { FakeQuicNetwork, type FakeStreamInfo } from '../../src/testing/fake-quic-provider.ts';
import { collect, generated, pattern } from '../support/talk/bulk-peers.ts';
import { bytes, Collector, rejectsStatus, serve } from '../support/talk/helpers.ts';
import {
  bindingAndRaw,
  concat,
  connectionPair,
  nextChannel,
  RawPeer,
  until,
  within,
} from '../unit/quic/harness.ts';

const empty = new Uint8Array(0);

/** Expects [actual] to hold the bytes of [expected]. */
function expectBytes(actual: Uint8Array, expected: Uint8Array): void {
  expect(actual.length).toBe(expected.length);
  expect(bytesEqual(actual, expected), 'bytes differ').toBe(true);
}

/**
 * A network whose streams the initiator writes are delivered after the
 * delay set for their ordinal in `delays` (0 by default), the order in
 * which the session's streams were opened, by either side: 0 is the
 * control stream, 1 the first channel.
 */
function delayedNetwork(): { network: FakeQuicNetwork; delays: Map<number, number> } {
  const delays = new Map<number, number>();
  const network = new FakeQuicNetwork({
    delay: (stream: FakeStreamInfo, fromInitiator: boolean) =>
      fromInitiator ? (delays.get(stream.ordinal) ?? 0) : 0,
  });
  return { network, delays };
}

/** A plain message "NOTE" referencing bulk [number], [length] bytes long. */
function note(number: number, length: number): Uint8Array {
  return new TalkFrame({ kind: TalkKind.message, procedure: Name.of('NOTE') })
    .withBulk(new TalkBulkReference(number, length))
    .encode();
}

interface QuicTalk {
  readonly client: QuicBindingConnection;
  readonly server: QuicBindingConnection;
  readonly raw: MuxChannel;
  readonly serverRaw: MuxChannel;
  readonly talk: TalkChannel;
  readonly responder: TalkChannel;
}

/**
 * Two binding connections over a fake session pair, bulk routing installed
 * on both (as a node does on every connection), and a Talk channel from the
 * initiator with its far end.
 */
async function quicTalk(
  init: { network?: FakeQuicNetwork; near?: Partial<TalkOptions>; far?: Partial<TalkOptions> } = {},
): Promise<QuicTalk> {
  const [client, server] = await connectionPair(
    init.network === undefined ? {} : { network: init.network },
  );
  installBulkRouting(client);
  installBulkRouting(server);
  const raw = client.open(empty);
  const talk = new TalkChannel(raw, init.near);
  const serverRaw = await nextChannel(server);
  const responder = new TalkChannel(serverRaw, init.far);
  return { client, server, raw, serverRaw, talk, responder };
}

describe('Talk over independent channels', () => {
  test('the binding reports unordered channels; requests, streams and aborts work as over the frame mux', async () => {
    const { client, talk, responder } = await quicTalk();
    expect(client.hasOrderedChannels).toBe(false);
    serve(responder, async (m) => {
      switch (m.procedureName) {
        case 'ECHO':
          await m.reply(m.payload, { procedure: 'PONG' });
          break;
        case 'LIST':
          for (let i = 0; i < 50; i++) {
            void m.replyItem(bytes([i]));
          }
          await m.reply(bytes([255]));
          break;
        default:
          await m.replyAbort(Status.of(StatusCode.notFound, m.procedureName));
      }
    });
    const echo = await within(talk.request('ECHO', bytes([1, 2, 3])));
    expect(echo.procedureName).toBe('PONG');
    expect([...echo.payload]).toEqual([1, 2, 3]);
    const s = talk.streamRequest('LIST', empty);
    const items: number[] = [];
    for await (const item of s) {
      items.push(item.payload[0] as number);
    }
    expect(items).toEqual(Array.from({ length: 50 }, (_, i) => i));
    expect([...(await s.done).payload]).toEqual([255]);
    const error = await rejectsStatus(talk.request('NOPE', empty), StatusCode.notFound);
    expect(error).toBeInstanceOf(TalkAbortError);
    expect(error.status.reason).toBe('NOPE');
    await talk.close();
    expect((await within(responder.done)).isOk).toBe(true);
  });

  test('a Talk channel whose consumer stalls holds up no other channel of the connection', async () => {
    const [client, server] = await connectionPair();
    const accepted: TalkChannel[] = [];
    void (async () => {
      for (;;) {
        const channel = await nextChannel(server).catch(() => undefined);
        if (channel === undefined) {
          return;
        }
        accepted.push(new TalkChannel(channel));
      }
    })();
    const stalled = new TalkChannel(client.open(empty));
    const other = new TalkChannel(client.open(empty));
    await until(() => accepted.length === 2);
    const [stalledFar, otherFar] = accepted as [TalkChannel, TalkChannel];
    const held = new Collector(stalledFar.messages, { paused: true });
    serve(otherFar, (m) => m.reply(m.payload));
    let sent = 0;
    for (let i = 0; i < 200; i++) {
      void stalled.send('FLOOD', new Uint8Array(1000)).then(() => {
        sent++;
      });
    }
    await until(() => sent > 0);
    // The stalled channel's stream window is spent: what it sends waits.
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(sent).toBeLessThan(200);
    expect(held.values).toHaveLength(0);
    const echo = await within(other.request('ECHO', bytes([9])));
    expect([...echo.payload]).toEqual([9]);
    held.resume();
    await until(() => held.values.length === 200);
    expect(sent).toBe(200);
    await held.cancel();
  });

  test('an ordered answer keeps its place among the messages of its channel', async () => {
    const { talk, responder } = await quicTalk();
    serve(responder, (m) => {
      void responder.send('EVENT', bytes([1]));
      void m.reply(pattern(200_000));
      void responder.send('EVENT', bytes([2]));
    });
    const log: string[] = [];
    serve(talk, (m) => log.push(`event ${m.payload[0]}`));
    const answer = await within(talk.request('SUB', empty, { ordered: true }));
    log.push(`answer ${answer.payload.length}`);
    await until(() => log.length === 3);
    expect(log).toEqual(['event 1', 'answer 200000', 'event 2']);
  });
});

describe('bulk payloads on streams of their own', () => {
  test('a request, a response, stream items and a plain message with bulk payloads', async () => {
    const { talk, responder } = await quicTalk({
      near: { streamBulk: (m) => m.frame.kind === TalkKind.streamItem },
    });
    const plain = new Deferred<TalkMessage>();
    serve(responder, async (m) => {
      switch (m.procedureName) {
        case 'PUT':
          expect(m.bulkLength).toBe(300_000);
          expectBytes(m.payload, pattern(300_000));
          await m.reply(bytes([30]));
          break;
        case 'PUTU':
          expect(m.bulkLength).toBeUndefined();
          expectBytes(m.payload, pattern(123_457));
          await m.reply(m.payload.subarray(0, 3));
          break;
        case 'GET':
          await m.reply(empty, { bulk: generated(200_000), bulkLength: 200_000 });
          break;
        case 'LIST':
          for (let i = 0; i < 3; i++) {
            void m.replyItem(empty, { bulk: generated(70_000 + i) });
          }
          void m.reply(bytes([9]));
          break;
        case 'NOTE':
          plain.resolve(m);
          break;
      }
    });
    const put = await within(
      talk.request('PUT', empty, { bulk: generated(300_000), bulkLength: 300_000 }),
    );
    expect([...put.payload]).toEqual([30]);
    const putu = await within(
      talk.request('PUTU', empty, { bulk: generated(123_457, { piece: 777 }) }),
    );
    expect([...putu.payload]).toEqual([0, 1, 2]);
    const got = await within(talk.request('GET', empty));
    expect(got.isBulk).toBe(true);
    expect(got.bulkLength).toBe(200_000);
    expectBytes(got.payload, pattern(200_000));
    const stream = talk.streamRequest('LIST', empty);
    const lengths: number[] = [];
    for await (const item of stream.items) {
      const received = await collect(item.bulk);
      expectBytes(received, pattern(received.length));
      lengths.push(received.length);
    }
    expect(lengths).toEqual([70_000, 70_001, 70_002]);
    expect([...(await stream.done).payload]).toEqual([9]);
    void talk.send('NOTE', pattern(100_000));
    const note = await within(plain.promise);
    expect(note.isBulk).toBe(true);
    expectBytes(note.payload, pattern(100_000));
  });

  test('a bulk channel that overtakes its reference is matched with it', async () => {
    const { network, delays } = delayedNetwork();
    const { talk, responder } = await quicTalk({ network });
    // The parent's stream (1) is slow: the bulk channel's (2) comes first.
    delays.set(1, 30);
    serve(responder, (m) => m.reply(bytes([Math.floor(m.payload.length / 1000)])));
    const answer = await within(talk.request('PUT', pattern(100_000)));
    expect([...answer.payload]).toEqual([100]);
  });

  test('a reference that overtakes its bulk channel waits for it', async () => {
    const { network, delays } = delayedNetwork();
    const { talk, responder } = await quicTalk({ network });
    // The bulk channel's stream (2) is slow.
    delays.set(2, 30);
    serve(responder, (m) => m.reply(bytes([Math.floor(m.payload.length / 1000)])));
    const answer = await within(talk.request('PUT', pattern(100_000)));
    expect([...answer.payload]).toEqual([100]);
  });

  test("a bulk channel that arrives before its parent's OPEN is held for it", async () => {
    const { network, delays } = delayedNetwork();
    const [client, server] = await connectionPair({ network });
    installBulkRouting(server);
    // The new channel's stream (1) is slow, its bulk channel's (2) is not:
    // the OPEN naming parent channel 2 comes before the parent.
    delays.set(1, 40);
    const talk = new TalkChannel(client.open(empty));
    const answer = talk.request('PUT', pattern(100_000));
    const responder = new TalkChannel(await nextChannel(server));
    serve(responder, (m) => m.reply(bytes([Math.floor(m.payload.length / 1000)])));
    expect([...(await within(answer)).payload]).toEqual([100]);
  });

  test('messages keep their order while their payloads arrive out of it', async () => {
    const { network, delays } = delayedNetwork();
    const { talk, responder } = await quicTalk({ network });
    // B's bulk channel (stream 2) is slower than D's (stream 3).
    delays.set(2, 40);
    const seen: string[] = [];
    serve(responder, (m) => seen.push(`${m.procedureName} ${m.payload.length}`));
    void talk.send('A', new Uint8Array(1));
    void talk.send('B', empty, { bulk: generated(500_000, { piece: 1000 }) });
    void talk.send('C', new Uint8Array(2));
    void talk.send('D', pattern(100_000));
    await until(() => seen.length === 4);
    expect(seen).toEqual(['A 1', 'B 500000', 'C 2', 'D 100000']);
  });
});

describe('a graceful close before the payloads', () => {
  // A Talk sender closes a channel once its bulk channels are mutually
  // closed, so these peers are raw: they close the parent before its bulk
  // channel even opens, as a peer whose bulk streams lag behind may. The
  // binding reads a channel's stream only as far as what it delivered was
  // consumed, or granted ahead (`grant`), as in Dart: a message Talk holds
  // for its payload holds the CLOSE behind it. With a grant ahead, the
  // CLOSE is read before the payload's bulk channel, and Talk waits for
  // the payload after the parent ended, holding the connection for its
  // drain (wiki page "Polyverse Switchboard QUIC and WebTransport
  // Binding", section "Bulk payloads").

  /** The raw peer of [raw] opens bulk channel [number] of channel 2 (id 2 + 2 [number]) with [data]. */
  async function sendBulk(raw: RawPeer, number: number, data: number[]): Promise<QuicStream> {
    const bulk = await raw.openRaw(
      concat([
        encodeOpenRecord(2 + 2 * number, new TalkBulkOpen(2, number).encode()),
        encodeDataRecord(data),
        encodeCloseRecord(Status.ok),
      ]),
    );
    await bulk.finish();
    return bulk;
  }

  test('a message Talk holds for its payload holds the CLOSE behind it; the payload comes, then the end', async () => {
    const [connection, raw] = await bindingAndRaw();
    installBulkRouting(connection);
    const arrived = nextChannel(connection);
    const parent = await raw.openRaw(
      concat([
        encodeOpenRecord(2, empty),
        encodeDataRecord(note(1, 3)),
        encodeCloseRecord(Status.ok),
      ]),
    );
    await parent.finish();
    const talk = new TalkChannel(await arrived);
    const delivered = new Deferred<TalkMessage>();
    serve(talk, (m) => delivered.resolve(m));
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(connection.channelWithId(2)?.state).toBe('open');
    expect(connection.hasDrainHolds).toBe(false);
    await sendBulk(raw, 1, [1, 2, 3]);
    expect([...(await within(delivered.promise)).payload]).toEqual([1, 2, 3]);
    expect((await within(talk.done)).isOk).toBe(true);
  });

  /**
   * A binding connection (the acceptor, bulk routing installed) whose raw
   * peer opened channel 2 and sent on it what [respond] makes of the
   * first record the Talk channel over it sent (if any), referencing bulk
   * 1, then closed it gracefully. The Talk channel grants a window ahead,
   * so the CLOSE is read: the parent has ended, and Talk waits for the
   * payload, with a drain hold on the connection.
   */
  async function closedBeforeItsPayload<T>(
    start: (talk: TalkChannel) => T,
    respond: (request: TalkFrame | undefined) => Uint8Array,
    reads: boolean,
  ): Promise<{ connection: QuicBindingConnection; raw: RawPeer; talk: TalkChannel; started: T }> {
    const [connection, raw] = await bindingAndRaw();
    installBulkRouting(connection);
    const arrived = nextChannel(connection);
    const parent = await raw.openRaw(encodeOpenRecord(2, empty));
    const channel = await arrived;
    const talk = new TalkChannel(channel);
    channel.grant(65536);
    const started = start(talk);
    let request: TalkFrame | undefined;
    if (reads) {
      const record = await within(new RecordReader(parent).next(quicDefaultRecordLimit));
      request = TalkFrame.decode((record as QuicRecord).body);
    }
    await parent.write(concat([encodeDataRecord(respond(request)), encodeCloseRecord(Status.ok)]));
    await parent.finish();
    await until(() => connection.channelWithId(2) === undefined && connection.hasDrainHolds);
    expect(connection.retainedForDrain(2)).toBe(true);
    expect(talk.isOpen).toBe(false);
    return { connection, raw, talk, started };
  }

  /** A plain message from the peer, its payload on bulk 1, then the close; what Talk delivers. */
  const noteThenClose = () =>
    closedBeforeItsPayload(
      (talk) => {
        const delivered = new Deferred<TalkMessage>();
        serve(talk, (m) => delivered.resolve(m));
        return delivered.promise;
      },
      () => note(1, 3),
      false,
    );

  test('a message whose bulk channel comes after the parent ended is delivered, a drain hold keeping the connection for it', async () => {
    const { connection, raw, talk, started } = await noteThenClose();
    await sendBulk(raw, 1, [1, 2, 3]);
    expect([...(await within(started)).payload]).toEqual([1, 2, 3]);
    expect((await within(talk.done)).isOk).toBe(true);
    await until(() => !connection.hasDrainHolds);
  });

  test('the answer to a request of this side, whose bulk channel comes after the parent ended, is delivered', async () => {
    const { connection, raw, started } = await closedBeforeItsPayload(
      (talk) => {
        const answer = talk.request('GET', empty);
        answer.catch(() => {});
        return answer;
      },
      (request) =>
        new TalkFrame({ kind: TalkKind.message, responseId: request?.requestId })
          .withBulk(new TalkBulkReference(1, 3))
          .encode(),
      true,
    );
    await sendBulk(raw, 1, [1, 2, 3]);
    expect([...(await within(started)).payload]).toEqual([1, 2, 3]);
    await until(() => !connection.hasDrainHolds);
  });

  test('a late bulk channel no message of the ended parent waits for is refused FAILED_PRECONDITION', async () => {
    const { raw, started } = await noteThenClose();
    const stray = await sendBulk(raw, 2, [9]);
    const [records] = await RawPeer.drain(stray);
    const close = records.find((r) => r.command === QuicRecordCommand.close);
    expect(decodeCloseBody(close?.body as Uint8Array).known).toBe(StatusCode.failedPrecondition);
    await sendBulk(raw, 1, [1, 2, 3]);
    expect([...(await within(started)).payload]).toEqual([1, 2, 3]);
  });

  test("after the peer's GOAWAY, the late payload is still delivered", async () => {
    const { connection, raw, started } = await noteThenClose();
    await raw.sendControl(MuxControlMessage.goAway(Status.of(StatusCode.goingAway)));
    await within(connection.peerGoAwayStatus);
    await sendBulk(raw, 1, [1, 2, 3]);
    expect([...(await within(started)).payload]).toEqual([1, 2, 3]);
    expect((await within(connection.done)).known).toBe(StatusCode.goingAway);
  });
});

describe('_bulk continuations after GOAWAY', () => {
  test('installBulkRouting admits the bulk channels of an open parent after this side sent GOAWAY', async () => {
    const { client, server, talk, responder } = await quicTalk();
    const served: TalkMessage[] = [];
    const arrived = new Deferred<void>();
    serve(responder, (m) => {
      served.push(m);
      arrived.resolve();
    });
    const pending = talk.request('PUT', empty, {
      bulk: generated(200_000),
      bulkLength: 200_000,
    });
    await within(arrived.promise);
    const m = served[0] as TalkMessage;
    // The server goes away while the request is open: the bulk channel of
    // its answer is not a new channel, nor is the bulk channel of another
    // message from the client on the same channel.
    const goingAway = server.goAway();
    await until(() => client.peerGoingAway);
    expectBytes(m.payload, pattern(200_000));
    void talk.send('MORE', pattern(100_000)).catch(() => {});
    await until(() => served.length === 2);
    expectBytes((served[1] as TalkMessage).payload, pattern(100_000));
    await m.reply(empty, { bulk: generated(150_000), bulkLength: 150_000 });
    expectBytes((await within(pending)).payload, pattern(150_000));
    await talk.close();
    await within(goingAway);
  });

  test('after this side sent GOAWAY, another channel is refused GOING_AWAY, a _bulk one for an open parent is not', async () => {
    const [connection, raw] = await bindingAndRaw();
    installBulkRouting(connection);
    const parentArrived = nextChannel(connection);
    const parent = await raw.openRaw(encodeOpenRecord(2, empty));
    const talk = new TalkChannel(await parentArrived);
    const delivered = new Deferred<TalkMessage>();
    serve(talk, (m) => delivered.resolve(m));
    void connection.goAway();
    await raw.nextControlOf(MuxControlType.goAway);
    // Not a continuation: refused.
    const other = await raw.openRaw(encodeOpenRecord(4, empty));
    const [records] = await RawPeer.drain(other);
    const close = records.find((r) => r.command === QuicRecordCommand.close);
    expect(decodeCloseBody(close?.body as Uint8Array).known).toBe(StatusCode.goingAway);
    // A bulk channel of the open parent: admitted, and the message
    // referencing it delivered with its payload.
    const bulk = await raw.openRaw(
      concat([
        encodeOpenRecord(6, new TalkBulkOpen(2, 1).encode()),
        encodeDataRecord([4, 5, 6]),
        encodeCloseRecord(Status.ok),
      ]),
    );
    await bulk.finish();
    await parent.write(encodeDataRecord(note(1, 3)));
    expect([...(await within(delivered.promise)).payload]).toEqual([4, 5, 6]);
    expect(talk.isOpen).toBe(true);
  });
});
