/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Talk over the real frame mux: two FrameMuxConnections on the memory
// transport pair. The port of the Dart `test/mux_talk_test.dart` (which
// runs there over the frame mux on the memory transport, as here), then
// the requester and responder sides of `talk_test.dart` and
// `talk_window_test.dart` again over a real mux channel: requests,
// replies, streams, aborts, cancels, timeouts and EXTEND, item windows,
// ordered answers, the end of the channel and of the connection. The unit
// tests in test/unit/talk run the same over the memory mux pair; these check
// that the frame mux carries what they assume. Time is a ManualClock
// throughout: the mux and Talk take it from the connection's options.
//
// Talk's credit and flow control over the frame mux are in
// talk-frame-mux-credit.test.ts, bulk payloads in
// talk-frame-mux-bulk.test.ts, forwarding in talk-frame-mux-forward.test.ts.

import { describe, expect, test } from 'vitest';
import { hexBytes, hexString } from '../../src/core/bytes.ts';
import type { MuxChannel } from '../../src/core/mux/contract.ts';
import { MuxCommand, MuxFrame } from '../../src/core/mux/mux-frame.ts';
import { Name } from '../../src/core/name.ts';
import { Status, StatusCode } from '../../src/core/status.ts';
import type { TalkMessage } from '../../src/core/talk/contract.ts';
import { TalkAbortError, TalkChannel } from '../../src/core/talk/talk-channel.ts';
import { TalkFrame, TalkKind } from '../../src/core/talk/talk-frame.ts';
import { Deferred } from '../../src/core/util/deferred.ts';
import { ManualClock } from '../../src/testing/manual-clock.ts';
import { until } from '../support/talk/bulk-peers.ts';
import {
  acceptAll,
  frameMuxPair,
  nextIncoming,
  rawMuxPair,
  WireLog,
} from '../support/talk/frame-mux-peers.ts';
import {
  bytes,
  Collector,
  expectStatus,
  flush,
  Outcome,
  Queue,
  rejectsStatus,
  serve,
  throwsStatus,
  toList,
} from '../support/talk/helpers.ts';

const empty = new Uint8Array(0);

/** A Talk channel from the initiator of a fresh frame mux pair, its acceptor end, and both connections. */
async function talkPair(
  init: { clock?: ManualClock; wire?: WireLog; options?: Parameters<typeof frameMuxPair>[0] } = {},
) {
  const [client, server] = frameMuxPair({
    ...init.options,
    options: {
      ...init.options?.options,
      ...(init.clock === undefined ? {} : { clock: init.clock }),
    },
    ...(init.wire === undefined ? {} : { wire: init.wire }),
  });
  const raw = client.open(empty);
  const talk = new TalkChannel(raw);
  const serverRaw = await nextIncoming(server);
  const responder = new TalkChannel(serverRaw);
  return { client, server, raw, serverRaw, talk, responder };
}

/** The single next message [channel] receives. */
async function nextMessage(channel: TalkChannel): Promise<TalkMessage> {
  for await (const message of channel.messages) {
    return message;
  }
  throw new Error('no message');
}

describe('mux_talk_test over the frame mux', () => {
  test('talk request over a mux channel, close status propagates', async () => {
    const [client, server] = frameMuxPair();
    const serverChannels = acceptAll(server, (channel) => {
      const talk = new TalkChannel(channel);
      serve(talk, (message) =>
        message.reply(bytes([...message.payload, 1]), { procedure: 'PONG' }),
      );
    });
    const channel = client.open(bytes([7]));
    const talk = new TalkChannel(channel);
    const reply = await talk.request('PING', bytes([2]));
    expect(reply.procedureName).toBe('PONG');
    expect([...reply.payload]).toEqual([2, 1]);
    await talk.close(Status.of(StatusCode.goingAway, 'bye'));
    expect((await talk.done).known).toBe(StatusCode.goingAway);
    expect((await channel.done).known).toBe(StatusCode.goingAway);
    await client.close();
    await server.close();
    await serverChannels;
  });

  test('talk abort maps application code before mux close', async () => {
    const wire = new WireLog();
    const [client, server] = frameMuxPair({ wire });
    const serverChannel = nextIncoming(server);
    const serverTalkDone = serverChannel.then((c) => new TalkChannel(c).done);
    const raw = client.open(empty);
    const talk = new TalkChannel(raw);
    talk.abort(new Status(300, 'app'));
    const status = await serverTalkDone;
    expect(status.code).toBe(300);
    expect(status.reason).toBe('app');
    // Added: on the wire, the Talk channel abort carries the application
    // code, and the mux CLOSE after it UNKNOWN, since a CLOSE must not
    // carry one.
    const sent = wire.talk('initiator', raw.id);
    expect(sent.map((f) => f.kind)).toEqual([TalkKind.abort]);
    expect(sent[0]?.status.code).toBe(300);
    expect(wire.closes('initiator', raw.id).map((s) => s.code)).toEqual([StatusCode.unknown]);
    expect((await (await serverChannel).done).known).toBe(StatusCode.unknown);
    await client.close();
    await server.close();
  });

  test('a talk protocol error closes only that channel', async () => {
    const [client, server] = frameMuxPair();
    const serverTalks: TalkChannel[] = [];
    const serverChannels = acceptAll(server, (channel) => {
      const talk = new TalkChannel(channel);
      serverTalks.push(talk);
      void talk.serve((m) => m.reply(bytes([1]))).catch(() => {});
    });
    const bad = client.open(empty);
    const good = new TalkChannel(client.open(empty));
    const sibling = new TalkChannel(client.open(empty));
    // BULK with an empty bulk reference: a channel protocol error.
    void bad.send(hexBytes('41 48 45 4C 4C 4F 00 00 00'));
    expect((await bad.done).known).toBe(StatusCode.protocolError);
    expect((await (serverTalks[0] as TalkChannel).done).known).toBe(StatusCode.protocolError);
    expect([...(await good.request('X', empty)).payload]).toEqual([1]);
    expect([...(await sibling.request('Y', empty)).payload]).toEqual([1]);
    expect(client.isOpen).toBe(true);
    expect(server.isOpen).toBe(true);
    await client.close();
    await server.close();
    await serverChannels;
  });

  test('CONNECTION_LOST goes on the wire as UNAVAILABLE', async () => {
    const wire = new WireLog();
    const [client, server] = frameMuxPair({ wire });
    const serverDone = nextIncoming(server).then((c) => c.done);
    const raw = client.open(empty);
    const talk = new TalkChannel(raw);
    await talk.close(Status.of(StatusCode.connectionLost, 'upstream gone'));
    expect((await talk.done).known).toBe(StatusCode.connectionLost);
    const remote = await serverDone;
    expect(remote.known).toBe(StatusCode.unavailable);
    expect(remote.reason).toContain('upstream gone');
    expect(wire.closes('initiator', raw.id).map((s) => s.code)).toEqual([StatusCode.unavailable]);
    await client.close();
    await server.close();
  });
});

describe('requests and replies', () => {
  test('request and reply both ways, on the wire as DATA of one channel', async () => {
    const wire = new WireLog();
    const { raw, talk, responder } = await talkPair({ wire });
    serve(responder, (m) => m.reply(Uint8Array.from(m.payload).reverse()));
    serve(talk, (m) => m.reply(bytes([m.payload.length])));
    const r1 = await talk.request('ECHO', bytes([1, 2, 3]));
    expect([...r1.payload]).toEqual([3, 2, 1]);
    const r2 = await responder.request('LEN', bytes([9, 9]));
    expect([...r2.payload]).toEqual([2]);
    const out = wire.talk('initiator', raw.id);
    expect(hexString((out[0] as TalkFrame).encode())).toBe(
      '03 45 43 48 4F 00 00 00 00 01 00 00 01 02 03',
    );
    expect(wire.talk('acceptor', raw.id)[0]?.responseId).toBe(1);
    for (const c of [talk, responder]) {
      expect(c.outgoingRequestCount).toBe(0);
      expect(c.incomingRequestCount).toBe(0);
    }
    await talk.close();
    expect((await responder.done).isOk).toBe(true);
  });

  test('plain messages keep their order with requests', async () => {
    const { talk, responder } = await talkPair();
    const seen: string[] = [];
    serve(responder, (m) => {
      seen.push(`${m.procedureName} ${m.payload[0]}`);
      if (m.expectsReply) {
        void m.reply(empty);
      }
    });
    for (let i = 0; i < 10; i++) {
      void talk.send('NOTE', bytes([i]));
      if (i % 3 === 0) {
        void talk.request('ASK', bytes([i]));
      }
    }
    await talk.request('LAST', bytes([99]));
    expect(seen).toEqual([
      'NOTE 0',
      'ASK 0',
      'NOTE 1',
      'NOTE 2',
      'NOTE 3',
      'ASK 3',
      'NOTE 4',
      'NOTE 5',
      'NOTE 6',
      'ASK 6',
      'NOTE 7',
      'NOTE 8',
      'NOTE 9',
      'ASK 9',
      'LAST 99',
    ]);
    await talk.close();
  });

  test('chained replies, several hops deep', async () => {
    const { talk, responder } = await talkPair();
    const checks: unknown[] = [];
    serve(responder, async (m1) => {
      checks.push(m1.procedureName);
      const r2 = await m1.replyRequest(bytes([2]), { procedure: 'Q2' });
      checks.push(r2.procedureName, r2.expectsReply);
      const r4 = await r2.replyRequest(bytes([4]));
      checks.push([...r4.payload], r4.expectsReply);
      void r4.reply(bytes([6]));
    });
    const r1 = await talk.request('Q1', bytes([1]));
    expect([...r1.payload]).toEqual([2]);
    expect(r1.procedureName).toBe('Q2');
    const r3 = await r1.replyRequest(bytes([3]), { procedure: 'Q3' });
    expect([...r3.payload]).toEqual([4]);
    const r5 = await r3.replyRequest(bytes([5]));
    expect([...r5.payload]).toEqual([6]);
    expect(r5.expectsReply).toBe(false);
    expect(checks).toEqual(['Q1', 'Q3', true, [5], true]);
    for (const c of [talk, responder]) {
      expect(c.outgoingRequestCount).toBe(0);
      expect(c.incomingRequestCount).toBe(0);
    }
    await talk.close();
  });

  test('interleaving: 200 requests in flight, replied in reverse', async () => {
    const { talk, responder } = await talkPair();
    const held: TalkMessage[] = [];
    const all = new Deferred<void>();
    serve(responder, (m) => {
      held.push(m);
      if (held.length === 200) {
        all.resolve();
      }
    });
    const replies: Promise<TalkMessage>[] = [];
    for (let i = 0; i < 200; i++) {
      replies.push(talk.request('N', bytes([i])));
    }
    await all.promise;
    for (const m of [...held].reverse()) {
      void m.reply(m.payload);
    }
    const answers = await Promise.all(replies);
    expect(answers.map((a) => a.payload[0])).toEqual(Array.from({ length: 200 }, (_, i) => i));
    await talk.close();
  });

  test('a payload just below the subframe limit goes inline both ways', async () => {
    const { raw, talk, responder } = await talkPair();
    serve(responder, (m) => m.reply(m.payload));
    // MESSAGE with a procedure and a request id: 12 bytes of header.
    const payload = new Uint8Array(raw.maxSubframeLength - 12).fill(7);
    const answer = await talk.request('ECHO', payload);
    expect(answer.isBulk).toBe(false);
    expect(answer.payload.length).toBe(payload.length);
    await talk.close();
  });

  test('several Talk channels on one connection are independent', async () => {
    const [client, server] = frameMuxPair();
    const accepted = acceptAll(server, (channel) => {
      const talk = new TalkChannel(channel);
      serve(talk, (m) => m.reply(bytes([channel.id % 256])));
    });
    const channels = Array.from({ length: 5 }, () => client.open(empty));
    const talks = channels.map((c) => new TalkChannel(c));
    const answers = await Promise.all(talks.map((t) => t.request('WHO', empty)));
    expect(answers.map((a) => a.payload[0])).toEqual(channels.map((c) => c.id % 256));
    await (talks[2] as TalkChannel).close(Status.of(StatusCode.aborted, 'just this one'));
    for (const [i, t] of talks.entries()) {
      if (i !== 2) {
        expect([...(await t.request('WHO', empty)).payload]).toEqual([
          (channels[i] as MuxChannel).id % 256,
        ]);
      }
    }
    await client.close();
    await server.close();
    await accepted;
  });
});

describe('streams', () => {
  test('items then a final response with trailing payload', async () => {
    const { talk, responder } = await talkPair();
    serve(responder, async (m) => {
      expect(m.expectsStream).toBe(true);
      for (let i = 0; i < 5; i++) {
        await m.replyItem(bytes([i]));
      }
      await m.reply(bytes([42]));
    });
    const s = talk.streamRequest('LIST', empty);
    const items = await toList(s.items);
    expect(items.map((i) => i.payload[0])).toEqual([0, 1, 2, 3, 4]);
    expect([...(await s.done).payload]).toEqual([42]);
    expect(talk.outgoingRequestCount).toBe(0);
    await talk.close();
  });

  test('replyStream sends every item, then the trailer', async () => {
    const { talk, responder } = await talkPair();
    serve(responder, (m) =>
      m.replyStream(
        (async function* () {
          for (let i = 0; i < 100; i++) {
            yield bytes([i]);
          }
        })(),
        { trailer: bytes([255]) },
      ),
    );
    const s = talk.streamRequest('LIST', empty);
    const items = await toList(s);
    expect(items.map((i) => i.payload[0])).toEqual(Array.from({ length: 100 }, (_, i) => i));
    expect([...(await s.done).payload]).toEqual([255]);
    await talk.close();
  });

  test('an item that is itself a request is answered on the same channel', async () => {
    const { talk, responder } = await talkPair();
    serve(responder, async (m) => {
      const answer = await m.replyItemRequest(bytes([1]), { procedure: 'CONFIRM' });
      await m.reply(answer.payload);
    });
    const s = talk.streamRequest('ASK', empty);
    const items = new Collector(s.items, {
      onValue: (item) => {
        expect(item.procedureName).toBe('CONFIRM');
        void item.reply(bytes([item.payload[0] as number, 2]));
      },
    });
    expect([...(await s.done).payload]).toEqual([1, 2]);
    await items.finished;
    expect(items.values).toHaveLength(1);
    await talk.close();
  });

  test('an abort ends the items with the exact status', async () => {
    const { talk, responder } = await talkPair();
    serve(responder, async (m) => {
      await m.replyItem(bytes([1]));
      await m.replyAbort(Status.of(StatusCode.notFound, 'gone'));
    });
    const s = talk.streamRequest('LIST', empty);
    const items = new Collector(s.items);
    await items.finished;
    expect(items.values.map((i) => i.payload[0])).toEqual([1]);
    expect(items.error).toBeInstanceOf(TalkAbortError);
    const error = await rejectsStatus(s.done, StatusCode.notFound);
    expect(error.status.reason).toBe('gone');
    await talk.close();
  });
});

describe('aborts and cancels', () => {
  test('abort response carries the exact status and reason', async () => {
    const { talk, responder } = await talkPair();
    serve(responder, (m) => m.replyAbort(Status.of(StatusCode.permissionDenied, 'not you')));
    const error = await rejectsStatus(talk.request('Q', empty), StatusCode.permissionDenied);
    expect(error).toBeInstanceOf(TalkAbortError);
    expect((error as TalkAbortError).isChannelAbort).toBe(false);
    expect(error.status.reason).toBe('not you');
    // The channel is still usable.
    expect(talk.isOpen).toBe(true);
    await talk.close();
  });

  test('an application status code crosses an abort response', async () => {
    const { talk, responder } = await talkPair();
    serve(responder, (m) => m.replyAbort(new Status(1234, 'custom')));
    const error = await rejectsStatus(talk.request('Q', empty), 1234 as StatusCode);
    expect(error.status.reason).toBe('custom');
    await talk.close();
  });

  test('handler exceptions are answered with ABORT INTERNAL', async () => {
    const { talk, responder } = await talkPair();
    serve(responder, (m) => {
      if (m.procedureName === 'THROW') {
        throw new Error('boom');
      }
      return Promise.reject(new Error('later boom'));
    });
    await rejectsStatus(talk.request('THROW', empty), StatusCode.internal);
    await rejectsStatus(talk.request('REJECT', empty), StatusCode.internal);
    expect(responder.incomingRequestCount).toBe(0);
    await talk.close();
  });

  test('requester cancels: the responder sees the cancel and its signal aborts', async () => {
    const wire = new WireLog();
    const { raw, talk, responder } = await talkPair({ wire });
    const served = nextMessage(responder);
    const request = talk.startRequest('SLOW', empty);
    const m = await served;
    expect(m.isCancelled).toBe(false);
    request.cancel();
    await rejectsStatus(request.response, StatusCode.cancelled);
    await m.onCancel;
    expect(m.isCancelled).toBe(true);
    expect(m.signal.aborted).toBe(true);
    // The responder answers the cancel CANCELLED itself; its reply after
    // throws.
    await flush();
    throwsStatus(() => m.reply(empty), StatusCode.failedPrecondition);
    const answers = wire.talk('acceptor', raw.id);
    expect(answers.map((f) => f.kind)).toEqual([TalkKind.abort]);
    expect(answers[0]?.status.code).toBe(StatusCode.cancelled);
    const cancel = wire.talk('initiator', raw.id).at(-1) as TalkFrame;
    expect(cancel.kind).toBe(TalkKind.abort);
    expect(cancel.requestId).toBe(request.requestId);
    expect(talk.outgoingRequestCount).toBe(0);
    expect(responder.incomingRequestCount).toBe(0);
    await talk.close();
  });

  test('an AbortSignal cancels a request and a stream request', async () => {
    const { talk, responder } = await talkPair();
    const served: TalkMessage[] = [];
    serve(responder, (m) => {
      served.push(m);
    });
    const controller = new AbortController();
    const one = talk.request('ONE', empty, { signal: controller.signal });
    const s = talk.streamRequest('MANY', empty, { signal: controller.signal });
    await flush();
    expect(served).toHaveLength(2);
    controller.abort();
    await rejectsStatus(one, StatusCode.cancelled);
    await rejectsStatus(s.done, StatusCode.cancelled);
    await Promise.all(served.map((m) => m.onCancel));
    await talk.close();
  });

  test('a channel abort fails what is outstanding on both sides', async () => {
    const { serverRaw, talk, responder } = await talkPair();
    const atResponder = new Queue(responder.messages);
    const theirs = talk.request('PENDING', empty);
    const pending = await atResponder.next();
    const ours = responder.request('MINE', empty);
    responder.abort(Status.of(StatusCode.unauthenticated, 'who are you'));
    const error = await rejectsStatus(theirs, StatusCode.unauthenticated);
    expect((error as TalkAbortError).isChannelAbort).toBe(true);
    await rejectsStatus(ours, StatusCode.unauthenticated);
    expect(pending.isCancelled).toBe(true);
    expect((await talk.done).known).toBe(StatusCode.unauthenticated);
    expect((await responder.done).known).toBe(StatusCode.unauthenticated);
    expect((await serverRaw.done).known).toBe(StatusCode.unauthenticated);
  });

  test('messages ends with a TalkAbortError after what arrived before the channel abort', async () => {
    const { talk, responder } = await talkPair();
    const atResponder = new Collector(responder.messages);
    void talk.send('A', empty);
    void talk.send('B', empty);
    talk.abort(Status.of(StatusCode.aborted, 'enough'));
    await atResponder.finished;
    expect(atResponder.values.map((m) => m.procedureName)).toEqual(['A', 'B']);
    expect(atResponder.error).toBeInstanceOf(TalkAbortError);
    expectStatus(atResponder.error, StatusCode.aborted);
  });

  test('a mux channel closed under Talk fails outstanding requests with its status', async () => {
    const { serverRaw, talk, responder } = await talkPair();
    serve(responder, () => {});
    const pending = talk.request('Q', empty);
    await flush();
    await serverRaw.close(Status.of(StatusCode.notFound, 'no such thing'));
    const error = await rejectsStatus(pending, StatusCode.notFound);
    expect(error).not.toBeInstanceOf(TalkAbortError);
    expect((await talk.done).known).toBe(StatusCode.notFound);
  });

  test('the connection ending fails outstanding requests with CONNECTION_LOST on both sides', async () => {
    const { client, talk, responder } = await talkPair();
    const atResponder = new Queue(responder.messages);
    const theirs = talk.request('Q', empty);
    await atResponder.next();
    const ours = responder.request('R', empty);
    await client.close();
    await rejectsStatus(theirs, StatusCode.connectionLost);
    await rejectsStatus(ours, StatusCode.connectionLost);
    expect((await talk.done).known).toBe(StatusCode.connectionLost);
    expect((await responder.done).known).toBe(StatusCode.connectionLost);
    expect(await atResponder.hasNext()).toBe(false);
  });

  test('requests are refused UNIMPLEMENTED when nobody listens to messages any more', async () => {
    const { talk, responder } = await talkPair();
    const atResponder = new Queue(responder.messages);
    void talk.send('FIRST', empty);
    await atResponder.next();
    await atResponder.cancel();
    await rejectsStatus(talk.request('Q', empty), StatusCode.unimplemented);
    await talk.close();
  });
});

describe('timeouts and EXTEND', () => {
  test('requester side: fails locally DEADLINE_EXCEEDED and sends a cancel', async () => {
    const clock = new ManualClock();
    const wire = new WireLog();
    const { raw, talk, responder } = await talkPair({ clock, wire });
    const served = nextMessage(responder);
    const pending = new Outcome(talk.request('SLOW', empty, { timeout: 500 }));
    const m = await served;
    await clock.elapseAsync(499);
    expect(pending.isDone).toBe(false);
    await clock.elapseAsync(1);
    expectStatus(pending.error, StatusCode.deadlineExceeded);
    await m.onCancel;
    const cancel = wire.talk('initiator', raw.id).at(-1) as TalkFrame;
    expect(cancel.kind).toBe(TalkKind.abort);
    expect(cancel.requestId).toBe(m.requestId);
    expect(cancel.status.code).toBe(StatusCode.cancelled);
    expect(talk.outgoingRequestCount).toBe(0);
    await talk.close();
  });

  test('responder side: ABORT DEADLINE_EXCEEDED, later replies throw', async () => {
    const clock = new ManualClock();
    const [client, server] = frameMuxPair({ options: { clock } });
    const talk = new TalkChannel(client.open(empty), { requestTimeout: 0 });
    const responder = new TalkChannel(await nextIncoming(server), { replyTimeout: 300 });
    const served = nextMessage(responder);
    const pending = new Outcome(talk.request('SLOW', empty));
    const m = await served;
    await clock.elapseAsync(299);
    expect(pending.isDone).toBe(false);
    await clock.elapseAsync(1);
    expectStatus(pending.error, StatusCode.deadlineExceeded);
    expect(pending.error).toBeInstanceOf(TalkAbortError);
    expect(m.isCancelled).toBe(true);
    throwsStatus(() => m.reply(empty), StatusCode.failedPrecondition);
    await talk.close();
  });

  test('extend restarts both timers', async () => {
    const clock = new ManualClock();
    const [client, server] = frameMuxPair({ options: { clock } });
    const talk = new TalkChannel(client.open(empty), { requestTimeout: 1000 });
    const responder = new TalkChannel(await nextIncoming(server), { replyTimeout: 1000 });
    const served = nextMessage(responder);
    const extensions: unknown[] = [];
    const pending = new Outcome(
      talk.request('WORK', empty, { onExtend: (e) => extensions.push(e) }),
    );
    const m = await served;
    for (let i = 0; i < 5; i++) {
      await clock.elapseAsync(900);
      await m.extend();
    }
    await flush();
    expect(pending.isDone).toBe(false);
    expect(extensions).toHaveLength(5);
    await m.reply(bytes([1]));
    await clock.elapseAsync(0);
    expect(pending.isDone).toBe(true);
    expect([...(pending.value as TalkMessage).payload]).toEqual([1]);
    await talk.close();
  });

  test('a declared deadline replaces the default gap; the responder gives up first', async () => {
    const clock = new ManualClock();
    const [client, server] = frameMuxPair({ options: { clock } });
    const talk = new TalkChannel(client.open(empty), { requestTimeout: 1000 });
    const responder = new TalkChannel(await nextIncoming(server), { extendBuffer: 2000 });
    const served = nextMessage(responder);
    const declared: (number | undefined)[] = [];
    const pending = new Outcome(
      talk.request('LONG', empty, { onExtend: ({ deadline }) => declared.push(deadline) }),
    );
    const m = await served;
    await m.extend({ deadline: 10_000 });
    await clock.elapseAsync(0);
    // On the wire with the buffer; the responder keeps its own 10 s.
    expect(declared).toEqual([12_000]);
    await clock.elapseAsync(9_999);
    expect(pending.isDone).toBe(false);
    expect(m.isCancelled).toBe(false);
    await clock.elapseAsync(1);
    // The responder's deadline expired: it answers DEADLINE_EXCEEDED.
    expectStatus(pending.error, StatusCode.deadlineExceeded);
    expect(pending.error).toBeInstanceOf(TalkAbortError);
    expect(m.isCancelled).toBe(true);
    await talk.close();
  });

  test('stream items restart the requester timeout', async () => {
    const clock = new ManualClock();
    const { talk, responder } = await talkPair({ clock });
    const served = nextMessage(responder);
    const s = talk.streamRequest('TICKS', empty, { timeout: 100 });
    const items = new Collector(s.items);
    const m = await served;
    for (let i = 0; i < 10; i++) {
      await clock.elapseAsync(90);
      await m.replyItem(bytes([i]));
    }
    await clock.elapseAsync(90);
    expect(items.isDone).toBe(false);
    await clock.elapseAsync(10);
    await items.finished;
    expect(items.values).toHaveLength(10);
    expectStatus(items.error, StatusCode.deadlineExceeded);
    await talk.close();
  });

  test('closing the channel leaves no Talk timer armed', async () => {
    const clock = new ManualClock();
    const { talk, responder } = await talkPair({ clock });
    serve(responder, () => {});
    const pending = talk.request('Q', empty);
    const s = talk.streamRequest('S', empty);
    await flush();
    expect(responder.incomingRequestCount).toBe(2);
    await talk.close();
    await rejectsStatus(pending, StatusCode.cancelled);
    await rejectsStatus(s.done, StatusCode.cancelled);
    await responder.done;
    await clock.elapseAsync(0);
    // Only the connections' own timers may remain (none with keep-alive off).
    expect(clock.pendingCount).toBe(0);
  });
});

describe('item windows', () => {
  test('a finite window delivers exactly that many items, then nothing until a grant', async () => {
    const wire = new WireLog();
    const { raw, talk, responder } = await talkPair({ wire });
    const served = nextMessage(responder);
    const s = talk.streamRequest('LIST', empty, { window: 3 });
    const m = await served;
    expect(m.window).toBe(3);
    const sent: Outcome<void>[] = [];
    for (let i = 0; i < 5; i++) {
      sent.push(new Outcome(m.replyItem(bytes([i]))));
    }
    const items: number[] = [];
    new Collector(s.items, { onValue: (item) => items.push(item.payload[0] as number) });
    await flush();
    expect(items).toEqual([0, 1, 2]);
    expect(sent.map((o) => o.isDone)).toEqual([true, true, true, false, false]);
    expect(
      wire.talk('acceptor', raw.id).filter((f) => f.kind === TalkKind.streamItem),
    ).toHaveLength(3);
    s.grant(1);
    await flush();
    expect(items).toEqual([0, 1, 2, 3]);
    const credit = wire.talk('initiator', raw.id).filter((f) => f.isCredit);
    expect(credit.map((c) => hexString(c.encode()))).toEqual(['32 01 00 00 01 00 00 00']);
    s.grant(5);
    await flush();
    expect(items).toEqual([0, 1, 2, 3, 4]);
    expect(s.window).toBe(4);
    void m.reply(empty);
    await s.done;
    await talk.close();
  });

  test('window 0 starts exhausted, and the first grant starts delivery', async () => {
    const { talk, responder } = await talkPair();
    const served = nextMessage(responder);
    const s = talk.streamRequest('LIST', empty, { window: 0 });
    const items: number[] = [];
    new Collector(s.items, { onValue: (item) => items.push(item.payload[0] as number) });
    const m = await served;
    const firstItem = new Outcome(m.replyItem(bytes([1])));
    await flush();
    expect(items).toEqual([]);
    expect(firstItem.isDone).toBe(false);
    s.grant(1);
    await flush();
    expect(items).toEqual([1]);
    expect(firstItem.isDone).toBe(true);
    void m.reply(empty);
    await s.done;
    await talk.close();
  });

  test('the credit bound: an exhausted window that is never granted gives DEADLINE_EXCEEDED', async () => {
    const clock = new ManualClock();
    const [client, server] = frameMuxPair({ options: { clock } });
    const talk = new TalkChannel(client.open(empty));
    const responder = new TalkChannel(await nextIncoming(server), { creditTimeout: 20_000 });
    const served = nextMessage(responder);
    const s = talk.streamRequest('LIST', empty, { window: 1 });
    const items = new Collector(s.items);
    const m = await served;
    await m.replyItem(bytes([1]));
    const waiting = new Outcome(m.replyItem(bytes([2])));
    // The requester timeout (15 s) does not run while the window is
    // exhausted; the responder's credit bound does.
    await clock.elapseAsync(talk.options.requestTimeout + 1000);
    expect(items.isDone).toBe(false);
    expect(waiting.isDone).toBe(false);
    await clock.elapseAsync(20_000 - talk.options.requestTimeout - 1001);
    expect(waiting.isDone).toBe(false);
    await clock.elapseAsync(1);
    expectStatus(waiting.error, StatusCode.deadlineExceeded);
    await items.finished;
    expect(items.values).toHaveLength(1);
    expectStatus(items.error, StatusCode.deadlineExceeded);
    await talk.close();
  });

  test('a bulk item counts as one item', async () => {
    // The Dart talk_window_test case that runs over a mux pair: every Talk
    // channel installs bulk routing on its connection, so the bulk channels
    // the peer opens go to their parent's Talk layer.
    const [client, server] = frameMuxPair({ routeBulk: false });
    const talk = new TalkChannel(client.open(empty));
    const responder = new TalkChannel(await nextIncoming(server));
    const served = nextMessage(responder);
    const s = talk.streamRequest('GET', empty, { window: 2 });
    const m = await served;
    const lengths: number[] = [];
    new Collector(s.items, { onValue: (i) => lengths.push(i.payload.length) });
    // A bulk payload of the reply API, and an item too large inline, which
    // goes bulk all the same: one item each.
    const first = new Outcome(
      m.replyItem(empty, { bulk: [new Uint8Array(100000)], bulkLength: 100000 }),
    );
    const second = new Outcome(m.replyItem(new Uint8Array(60000)));
    const third = new Outcome(m.replyItem(bytes([3])));
    expect(m.window).toBe(0);
    await until(() => lengths.length === 2);
    expect(lengths).toEqual([100000, 60000]);
    expect(first.error).toBeUndefined();
    expect(second.error).toBeUndefined();
    expect(third.isDone).toBe(false);
    expect(s.window).toBe(0);
    s.grant(1);
    await until(() => lengths.length === 3);
    expect(lengths[2]).toBe(1);
    expect(third.isDone).toBe(true);
    void m.reply(empty);
    await s.done;
    await talk.close();
    await responder.done;
  });

  test('a credit for a request that is not a stream request closes the channel PROTOCOL_ERROR', async () => {
    const [client, server] = frameMuxPair();
    const raw = client.open(empty);
    const responder = new TalkChannel(await nextIncoming(server));
    serve(responder, () => {});
    await raw.send(
      new TalkFrame({ kind: TalkKind.message, procedure: Name.of('ONE'), requestId: 1 }).encode(),
    );
    await raw.send(
      new TalkFrame({
        kind: TalkKind.extend,
        requestId: 1,
        payload: TalkFrame.creditPayload(1),
      }).encode(),
    );
    expect((await responder.done).known).toBe(StatusCode.protocolError);
    expect((await raw.done).known).toBe(StatusCode.protocolError);
    expect(client.isOpen).toBe(true);
  });
});

describe('ordered answers', () => {
  test('the answer comes after the messages sent before it, and code waiting on it runs before the messages sent after it', async () => {
    const { talk, responder } = await talkPair();
    serve(responder, (m) => {
      void responder.send('EVENT', bytes([1]));
      void m.reply(bytes([0]));
      void responder.send('EVENT', bytes([2]));
    });
    const log: string[] = [];
    serve(talk, (m) => {
      log.push(`event ${m.payload[0]}`);
    });
    const answer = await talk.request('SUB', empty, { ordered: true });
    log.push(`answer ${answer.payload[0]}`);
    await flush();
    expect(log).toEqual(['event 1', 'answer 0', 'event 2']);
    await talk.close();
  });

  test('an abort answer is ordered too', async () => {
    const { talk, responder } = await talkPair();
    serve(responder, (m) => {
      void responder.send('EVENT', bytes([1]));
      void m.replyAbort(Status.of(StatusCode.notFound));
      void responder.send('EVENT', bytes([2]));
    });
    const log: string[] = [];
    serve(talk, (m) => {
      log.push(`event ${m.payload[0]}`);
    });
    try {
      await talk.request('SUB', empty, { ordered: true });
    } catch (e) {
      log.push(`abort ${(e as TalkAbortError).status.code}`);
    }
    await flush();
    expect(log).toEqual(['event 1', `abort ${StatusCode.notFound}`, 'event 2']);
    await talk.close();
  });
});

describe('GOAWAY', () => {
  test('a request in flight completes across the peer GOAWAY; then no new channel opens', async () => {
    const clock = new ManualClock();
    const { client, server, talk, responder } = await talkPair({ clock });
    const atResponder = new Queue(responder.messages);
    const pending = talk.request('Q', empty);
    const m = await atResponder.next();
    const goingAway = server.goAway();
    await flush();
    expect(client.peerGoingAway).toBe(true);
    throwsStatus(() => client.open(empty), StatusCode.failedPrecondition);
    await m.reply(bytes([1]));
    expect([...(await pending).payload]).toEqual([1]);
    // The connection waits for the open channel, then closes.
    await talk.close();
    await clock.elapseAsync(0);
    await goingAway;
    expect((await client.done).known).toBe(StatusCode.goingAway);
  });

  test('the GOAWAY grace cuts what is still open: requests fail GOING_AWAY', async () => {
    const clock = new ManualClock();
    const { client, server, talk, responder } = await talkPair({ clock });
    serve(responder, () => {});
    const pending = new Outcome(talk.request('NEVER', empty, { timeout: 0 }));
    await flush();
    const goingAway = new Outcome(server.goAway());
    await clock.elapseAsync(server.options.goAwayGrace - 1);
    expect(pending.isDone).toBe(false);
    expect(goingAway.isDone).toBe(false);
    await clock.elapseAsync(1);
    await clock.elapseAsync(0);
    expect(goingAway.isDone).toBe(true);
    expectStatus(pending.error, StatusCode.goingAway);
    expect((await talk.done).known).toBe(StatusCode.goingAway);
    expect((await client.done).known).toBe(StatusCode.goingAway);
  });
});

describe('what the frame mux brings', () => {
  test("a transport's frame limit, announced in LIMITS, makes Talk send bulk earlier", async () => {
    // Each side's transport takes frames of at most 4096 bytes: the mux
    // announces that, and the peer's subframes stay within it.
    const [client, server] = frameMuxPair({ transport: { maxFrameSize: 4096 } });
    const raw = client.open(empty);
    const talk = new TalkChannel(raw);
    const responder = new TalkChannel(await nextIncoming(server));
    const seen: boolean[] = [];
    serve(responder, (m) => {
      seen.push(m.isBulk);
      return m.reply(Uint8Array.from(m.payload).reverse());
    });
    await talk.request('WARM', empty);
    expect(raw.maxSubframeLength).toBeLessThan(4096);
    const payload = Uint8Array.from({ length: 10_000 }, (_, i) => i % 251);
    const answer = await talk.request('ECHO', payload);
    expect(seen).toEqual([false, true]);
    expect(answer.isBulk).toBe(true);
    expect([...answer.payload]).toEqual([...payload].reverse());
    expect(client.isOpen).toBe(true);
    expect(server.isOpen).toBe(true);
    await talk.close();
  });

  test('keep-alive runs under a long request on a ManualClock, and the request still completes', async () => {
    const clock = new ManualClock();
    const [client, server] = frameMuxPair({
      options: { clock, keepAliveInterval: 1000, keepAliveTimeout: 1000 },
    });
    const talk = new TalkChannel(client.open(empty), { requestTimeout: 0 });
    const responder = new TalkChannel(await nextIncoming(server), { replyTimeout: 0 });
    const served = nextMessage(responder);
    const pending = new Outcome(talk.request('LONG', empty));
    const m = await served;
    // A minute of silence on the channel: the connections ping each other.
    for (let i = 0; i < 60; i++) {
      await clock.elapseAsync(1000);
    }
    expect(client.isOpen).toBe(true);
    expect(server.isOpen).toBe(true);
    expect(pending.isDone).toBe(false);
    await m.reply(bytes([1]));
    await clock.elapseAsync(0);
    expect([...(pending.value as TalkMessage).payload]).toEqual([1]);
    await talk.close();
  });

  test('a peer that goes silent is cut by keep-alive: the outstanding request fails, the channel ends', async () => {
    const clock = new ManualClock();
    const [connection, peer] = rawMuxPair({
      options: { clock, keepAliveInterval: 1000, keepAliveTimeout: 1000 },
    });
    peer.send(MuxFrame.open(2, empty));
    const talk = new TalkChannel(await nextIncoming(connection), { requestTimeout: 0 });
    const pending = new Outcome(talk.request('Q', empty));
    // The raw peer answers nothing, PINGs included.
    await clock.elapseAsync(1000);
    expect(pending.isDone).toBe(false);
    await clock.elapseAsync(1000);
    await clock.elapseAsync(0);
    expect(pending.isDone).toBe(true);
    expectStatus(pending.error, StatusCode.connectionLost);
    expect((await talk.done).known).toBe(StatusCode.connectionLost);
    expect(connection.isOpen).toBe(false);
  });
});

describe('the raw channel under Talk', () => {
  test('a Talk channel puts its mux channel in manual credit, and frames are not mux-visible', async () => {
    const { raw, serverRaw, talk } = await talkPair();
    expect(raw.manualCredit).toBe(true);
    expect(serverRaw.manualCredit).toBe(true);
    expect(talk.raw).toBe(raw);
    await talk.close();
  });

  test('frames the peer wrote raw are Talk frames to the Talk channel', async () => {
    const [client, server] = frameMuxPair();
    const raw = client.open(empty);
    const responder = new TalkChannel(await nextIncoming(server));
    const atResponder = new Queue(responder.messages);
    await raw.send(hexBytes('01 48 45 4C 4C 4F 00 00 00 01'));
    const m = await atResponder.next();
    expect(m.procedureName).toBe('HELLO');
    expect([...m.payload]).toEqual([1]);
    // A request written raw, answered by Talk: the answer is a raw frame here.
    await raw.send(
      new TalkFrame({ kind: TalkKind.message, procedure: Name.of('Q'), requestId: 7 }).encode(),
    );
    const q = await atResponder.next();
    void q.reply(bytes([9]));
    const reader = raw.readable.getReader();
    const answer = TalkFrame.decode((await reader.read()).value as Uint8Array);
    expect(answer.responseId).toBe(7);
    expect([...answer.payload]).toEqual([9]);
    reader.releaseLock();
    await responder.close();
    expect((await raw.done).isOk).toBe(true);
  });

  test('a graceful close confirms the CLOSE both ways and frees both ids', async () => {
    const wire = new WireLog();
    const { client, server, raw, talk, responder } = await talkPair({ wire });
    serve(responder, (m) => m.reply(empty));
    await talk.request('Q', empty);
    await talk.close();
    expect((await responder.done).isOk).toBe(true);
    expect(wire.of('initiator', raw.id, MuxCommand.close)).toHaveLength(1);
    expect(wire.of('acceptor', raw.id, MuxCommand.close)).toHaveLength(1);
    expect(client.openChannelCount).toBe(0);
    expect(server.openChannelCount).toBe(0);
  });
});
