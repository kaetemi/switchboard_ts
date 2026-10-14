/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Port of the Dart `test/talk_window_test.dart`, groups 'the window' and
// 'credits received': item windows on stream requests (wiki page
// "Polyverse Switchboard Talk", sections "Item windows" and "Credit"), the
// window a stream request opens with, the credits (EXTEND from the
// requester) that move it, and the credits a responder receives. The
// groups 'timeouts' and 'the responder' are in talk-window-responder.test.ts.

import { describe, expect, test } from 'vitest';
import { hexString } from '../../../src/core/bytes.ts';
import { Name } from '../../../src/core/name.ts';
import { Status, StatusCode } from '../../../src/core/status.ts';
import type { TalkChannel, TalkMessage, TalkRequest } from '../../../src/core/talk/contract.ts';
import { TalkFrame, TalkKind } from '../../../src/core/talk/talk-frame.ts';
import { TalkAbortError } from '../../../src/core/talk/talk-util.ts';
import { flush } from '../../support/talk/fake-clock.ts';
import { Pair, RawPeer } from '../../support/talk/frame-link.ts';
import {
  bytes,
  Collector,
  expectStatus,
  Outcome,
  Queue,
  rejectsStatus,
  throwsStatus,
} from '../../support/talk/helpers.ts';

const empty = new Uint8Array(0);

/** A credit for request [id] of [items] items. */
function credit(id: number, items: number): TalkFrame {
  return new TalkFrame({
    kind: TalkKind.extend,
    requestId: id,
    payload: TalkFrame.creditPayload(items),
  });
}

/** A stream request [id] "LIST" with [window]. */
function streamRequestFrame(id: number, window: number): TalkFrame {
  return new TalkFrame({
    kind: TalkKind.message,
    procedure: Name.of('LIST'),
    requestId: id,
    stream: true,
    window,
  });
}

/** A stream item for request [id]. */
function itemFrame(id: number, value: number): TalkFrame {
  return new TalkFrame({ kind: TalkKind.streamItem, responseId: id, payload: bytes([value]) });
}

/** The Dart `Pair` of this file: the support `Pair`, with the credits a sent and the items b sent. */
class WindowPair extends Pair {
  /** The credits [a] sent. */
  get credits(): TalkFrame[] {
    return this.aToB.filter((f) => f.isCredit);
  }

  /** The items [b] sent. */
  get items(): TalkFrame[] {
    return this.bToA.filter((f) => f.kind === TalkKind.streamItem);
  }
}

/** The frames [peer.talk] sent for the peer's request [id]. */
function repliesTo(peer: RawPeer, id: number): TalkFrame[] {
  return peer.received.filter((f) => f.responseId === id);
}

/** The single next request [channel] receives (Dart `channel.messages.first`). */
async function nextRequest(channel: TalkChannel): Promise<TalkMessage> {
  for await (const message of channel.messages) {
    return message;
  }
  throw new Error('no request');
}

/** Every value of [iterable] (Dart `stream.toList()`). */
async function collect<T>(iterable: AsyncIterable<T>): Promise<T[]> {
  const values: T[] = [];
  for await (const value of iterable) {
    values.push(value);
  }
  return values;
}

/** The first payload byte of [message]. */
function first(message: TalkMessage): number {
  return message.payload[0] as number;
}

/** What [action] throws (Dart `throwsA`). */
function thrownBy(action: () => unknown): unknown {
  try {
    action();
  } catch (e) {
    return e;
  }
  expect.fail('expected a throw');
}

/** Expects [promise] to reject with a {@link TalkAbortError} with [code] (Dart `isRemote`). */
async function rejectsRemote(promise: Promise<unknown>, code: StatusCode): Promise<void> {
  const error = await rejectsStatus(promise, code);
  expect(error).toBeInstanceOf(TalkAbortError);
}

describe('the window', () => {
  test('a finite window delivers exactly that many items, then nothing until a grant', async () => {
    const p = new WindowPair();
    const served = nextRequest(p.b);
    const s = p.a.streamRequest('LIST', empty, { window: 3 });
    expect(s.window).toBe(3);
    expect(p.aToB, 'not on the wire before a turn').toHaveLength(0);
    const m = await served;
    expect(p.aToB).toHaveLength(1);
    expect(p.aToB[0]?.window).toBe(3);
    expect(m.window).toBe(3);
    const sent: Outcome<void>[] = [];
    for (let i = 0; i < 5; i++) {
      sent.push(new Outcome(m.replyItem(bytes([i]))));
    }
    const items: number[] = [];
    new Collector(s.items, { onValue: (item) => items.push(first(item)) });
    await flush();
    expect(items).toEqual([0, 1, 2]);
    expect(p.items, 'nothing beyond the window').toHaveLength(3);
    expect(s.window).toBe(0);
    expect(m.window).toBe(0);
    expect(sent.map((o) => o.isDone)).toEqual([true, true, true, false, false]);
    // Nothing is granted on the application's behalf.
    expect(p.credits).toHaveLength(0);
    s.grant(1);
    await flush();
    expect(items).toEqual([0, 1, 2, 3]);
    expect(sent[3]?.isDone).toBe(true);
    expect(sent[4]?.isDone).toBe(false);
    expect(p.credits).toHaveLength(1);
    const granted = p.credits[0] as TalkFrame;
    expect(granted.requestId).toBe(s.requestId);
    expect(granted.creditItems).toBe(1);
    expect(hexString(granted.encode())).toBe('32 01 00 00 01 00 00 00');
    s.grant(5);
    await flush();
    expect(items).toEqual([0, 1, 2, 3, 4]);
    expect(sent[4]?.isDone).toBe(true);
    expect(s.window).toBe(4);
    expect(m.window).toBe(4);
    void m.reply(empty);
    await s.done;
    expect(p.a.outgoingRequestCount).toBe(0);
    expect(p.b.incomingRequestCount).toBe(0);
    await p.close();
  });

  test('grants add up, while exhausted and ahead of time', async () => {
    const p = new WindowPair();
    const served = nextRequest(p.b);
    const s = p.a.streamRequest('LIST', empty, { window: 1 });
    const items: number[] = [];
    new Collector(s.items, { onValue: (item) => items.push(first(item)) });
    const m = await served;
    for (let i = 0; i < 10; i++) {
      void m.replyItem(bytes([i]));
    }
    await flush();
    expect(items).toEqual([0]);
    // Two grants in one turn: both count.
    s.grant(2);
    s.grant(1);
    expect(s.window).toBe(3);
    await flush();
    expect(items).toEqual([0, 1, 2, 3]);
    expect(m.window).toBe(0);
    // A grant ahead of consumption adds to what is left.
    s.grant(2);
    await flush();
    expect(items).toHaveLength(6);
    expect(p.credits.map((c) => c.creditItems)).toEqual([2, 1, 2]);
    s.cancel();
    await p.close();
  });

  test('window 0 starts exhausted, and the first grant starts delivery', async () => {
    const p = new WindowPair();
    const served = nextRequest(p.b);
    const s = p.a.streamRequest('LIST', empty, { window: 0 });
    const items: number[] = [];
    new Collector(s.items, { onValue: (item) => items.push(first(item)) });
    const m = await served;
    expect(p.aToB).toHaveLength(1);
    expect(p.aToB[0]?.window).toBe(0);
    expect(hexString((p.aToB[0] as TalkFrame).encode()).endsWith('00 00 00 00')).toBe(true);
    expect(m.window).toBe(0);
    const firstItem = new Outcome(m.replyItem(bytes([1])));
    void m.replyItem(bytes([2]));
    await flush();
    expect(items).toEqual([]);
    expect(p.items, 'nothing on the wire').toHaveLength(0);
    expect(firstItem.isDone).toBe(false);
    s.grant(2);
    await flush();
    expect(items).toEqual([1, 2]);
    expect(firstItem.isDone).toBe(true);
    void m.reply(empty);
    await s.done;
    await p.close();
  });

  test('the final response, an abort and an extend pass at zero', async () => {
    const p = new WindowPair();
    const served = new Queue(p.b.messages);
    const declared: (number | undefined)[] = [];
    const s = p.a.streamRequest('LIST', empty, {
      window: 0,
      onExtend: ({ deadline }) => declared.push(deadline),
    });
    const aborted = p.a.streamRequest('LIST', empty, { window: 0 });
    const m = await served.next();
    const n = await served.next();
    // An item waits; the extend and the final do not.
    const item = new Outcome(m.replyItem(bytes([1])));
    await flush();
    expect(item.isDone).toBe(false);
    await m.extend({ deadline: 60_000 });
    await flush();
    expect(declared).toHaveLength(1);
    // A final made while an item waits goes after it: it does not wait
    // itself, but it does not overtake.
    const trailer = new Outcome(m.reply(bytes([0xee])));
    expect(m.canReply).toBe(false);
    throwsStatus(() => m.replyItem(bytes([2])), StatusCode.failedPrecondition);
    await flush();
    expect(trailer.isDone).toBe(false);
    const items: number[] = [];
    new Collector(s.items, { onValue: (i) => items.push(first(i)) });
    s.grant(1);
    const last = await s.done;
    expect(items).toEqual([1]);
    expect([...last.payload]).toEqual([0xee]);
    // The frame went before the requester saw it, but the promise of the
    // queued final settles a few microtasks after (it adopts the send's
    // promise), which here can be after `s.done`: let it.
    await flush();
    expect(trailer.isDone).toBe(true);
    expect(p.bToA.filter((f) => f.responseId === s.requestId).map((f) => f.kind)).toEqual([
      TalkKind.extend,
      TalkKind.streamItem,
      TalkKind.message,
    ]);
    // An abort goes at once, whatever waits: the items are not sent.
    const dropped = new Outcome(n.replyItem(bytes([3])));
    await n.replyAbort(Status.of(StatusCode.notFound, 'gone'));
    await rejectsRemote(aborted.done, StatusCode.notFound);
    await flush();
    expectStatus(dropped.error, StatusCode.notFound);
    expect(p.bToA.filter((f) => f.responseId === aborted.requestId).map((f) => f.kind)).toEqual([
      TalkKind.abort,
    ]);
    await served.cancel();
    await p.close();
  });

  test('an empty result completes under window 0, and so does one that produced only a trailer', async () => {
    const p = new WindowPair();
    const served = new Queue(p.b.messages);
    const emptyResult = p.a.streamRequest('LIST', empty, { window: 0 });
    const trailer = p.a.streamRequest('LIST', empty, { window: 0 });
    const m = await served.next();
    const n = await served.next();
    void m.replyStream([]);
    void n.reply(bytes([7]));
    expect(await collect(emptyResult.items)).toEqual([]);
    expect((await emptyResult.done).payload).toHaveLength(0);
    expect([...(await trailer.done).payload]).toEqual([7]);
    expect(p.credits).toHaveLength(0);
    await served.cancel();
    await p.close();
  });

  test('a page that exactly fills its window delivers its trailer while the requester still holds its items, and no grant is owed', async () => {
    const p = new WindowPair();
    const served = nextRequest(p.b);
    const s = p.a.streamRequest('PAGE', empty, { window: 3 });
    const m = await served;
    void m.replyStream([bytes([1]), bytes([2]), bytes([3])], { trailer: bytes([0xc0]) });
    // The requester has not taken a single item yet.
    const last = await s.done;
    expect([...last.payload]).toEqual([0xc0]);
    expect(s.window).toBe(0);
    // Granting now is a no-op: the request ended.
    s.grant(10);
    expect((await collect(s.items)).map(first)).toEqual([1, 2, 3]);
    await flush();
    expect(p.credits).toHaveLength(0);
    expect(p.b.incomingRequestCount).toBe(0);
    await p.close();
  });

  test('item requests count as one item each', async () => {
    const p = new WindowPair();
    const served = nextRequest(p.b);
    const s = p.a.streamRequest('LIST', empty, { window: 2 });
    const asked: number[] = [];
    new Collector(s.items, {
      onValue: (item) => {
        asked.push(first(item));
        void item.reply(bytes([first(item) + 100]));
      },
    });
    const m = await served;
    const answers = [0, 1, 2].map((i) => m.startReplyItemRequest(bytes([i]))) as [
      TalkRequest,
      TalkRequest,
      TalkRequest,
    ];
    // The handle of the waiting one exists at once, its id reserved.
    expect(answers[2].requestId).not.toBe(answers[1].requestId);
    expect(p.b.outgoingRequestCount).toBe(3);
    await flush();
    expect(asked).toEqual([0, 1]);
    expect(p.items).toHaveLength(2);
    expect([...(await answers[0].response).payload]).toEqual([100]);
    expect([...(await answers[1].response).payload]).toEqual([101]);
    // Answered or not, they took the window.
    expect(m.window).toBe(0);
    s.grant(1);
    expect([...(await answers[2].response).payload]).toEqual([102]);
    expect(asked).toEqual([0, 1, 2]);
    void m.reply(empty);
    await s.done;
    await p.close();
  });

  test('an item request cancelled while it waits for the window is sent, then cancelled, once the window lets it go', async () => {
    const p = new WindowPair();
    const served = nextRequest(p.b);
    const s = p.a.streamRequest('LIST', empty, { window: 0 });
    const seen: TalkMessage[] = [];
    new Collector(s.items, { onValue: (item) => seen.push(item) });
    const m = await served;
    const waiting = m.startReplyItemRequest(bytes([1]));
    waiting.cancel();
    await rejectsStatus(waiting.response, StatusCode.cancelled);
    await flush();
    expect(p.bToA).toHaveLength(0);
    s.grant(1);
    await flush();
    expect(p.bToA.filter((f) => f.requestId === waiting.requestId).map((f) => f.kind)).toEqual([
      TalkKind.streamItem,
      TalkKind.abort,
    ]);
    expect(seen).toHaveLength(1);
    expect(seen[0]?.isCancelled).toBe(true);
    await flush();
    expect(p.b.outgoingRequestCount).toBe(0);
    void m.reply(empty);
    await s.done;
    await p.close();
  });

  test('what arrives for a request that waits for the window, before its frame went, is for an unknown id', async () => {
    const peer = new RawPeer({ status: true });
    const requests = new Queue(peer.talk.messages);
    peer.send(streamRequestFrame(1, 0));
    const m = await requests.next();
    const item = new Outcome(m.replyItem(bytes([1])));
    const chained = m.startReplyRequest(bytes([2]), { procedure: 'MORE' });
    const answer = new Outcome(chained.response);
    const id = chained.requestId;
    // The peer cannot know of the id yet: whatever names it is stale, for
    // an earlier request of ours with the id, or invented.
    peer.send(new TalkFrame({ kind: TalkKind.message, responseId: id, payload: bytes([7]) }));
    peer.send(itemFrame(id, 8));
    peer.send(new TalkFrame({ kind: TalkKind.extend, responseId: id }));
    peer.send(
      new TalkFrame({
        kind: TalkKind.abort,
        responseId: id,
        payload: Status.of(StatusCode.notFound).encode(),
      }),
    );
    peer.send(
      new TalkFrame({
        kind: TalkKind.message,
        requestId: 5,
        responseId: id,
        payload: bytes([9]),
      }),
    );
    await flush();
    expect(peer.talk.isOpen).toBe(true);
    expect(answer.isDone).toBe(false);
    expect(peer.talk.outgoingRequestCount).toBe(1);
    // Only the chained response that is a request is answered, NOT_FOUND.
    expect(peer.received).toHaveLength(1);
    expect(peer.received[0]?.responseId).toBe(5);
    expect(peer.received[0]?.status.known).toBe(StatusCode.notFound);
    // The grant lets the item and then the request go; its answer counts.
    peer.send(credit(1, 1));
    await flush();
    expect(item.isDone && item.error === undefined).toBe(true);
    expect(repliesTo(peer, 1).map((f) => f.kind)).toEqual([TalkKind.streamItem, TalkKind.message]);
    expect(repliesTo(peer, 1).at(-1)?.requestId).toBe(id);
    peer.send(new TalkFrame({ kind: TalkKind.message, responseId: id, payload: bytes([3]) }));
    await flush();
    expect([...(answer.value as TalkMessage).payload]).toEqual([3]);
    expect(peer.talk.outgoingRequestCount).toBe(0);
    await requests.cancel();
    await peer.talk.close();
  });

  test('a stream request in a reply carries its window, and grants made while it waits for the window go with it', async () => {
    const p = new WindowPair();
    const served = nextRequest(p.b);
    const s = p.a.streamRequest('LIST', empty, { window: 0 });
    let complete!: (message: TalkMessage) => void;
    const nested = new Promise<TalkMessage>((resolve) => {
      complete = resolve;
    });
    new Collector(s.items, { onValue: (item) => complete(item) });
    const m = await served;
    const sub = m.replyItemStreamRequest(bytes([1]), { window: 2 });
    expect(sub.window).toBe(2);
    sub.grant(3);
    expect(sub.window).toBe(5);
    await flush();
    expect(p.bToA, 'it waits for the window of LIST').toHaveLength(0);
    s.grant(1);
    const asked = await nested;
    expect(asked.expectsStream).toBe(true);
    expect(asked.window).toBe(5);
    expect(
      p.bToA.filter((f) => f.isCredit),
      'the grant went with the request',
    ).toHaveLength(0);
    // A chained response that is a stream request carries one too.
    const chained = s.done;
    const last = m.replyStreamRequest(bytes([2]), { window: 4 });
    const answer = await chained;
    expect(answer.expectsStream).toBe(true);
    expect(answer.window).toBe(4);
    expect(last.window).toBe(4);
    void answer.reply(empty);
    void asked.reply(empty);
    await last.done;
    await sub.done;
    await p.close();
  });

  test('an unlimited window is the default, and nothing is counted', async () => {
    const p = new WindowPair();
    const served = nextRequest(p.b);
    const s = p.a.streamRequest('LIST', empty);
    const m = await served;
    expect(p.aToB).toHaveLength(1);
    expect(p.aToB[0]?.window).toBe(TalkFrame.unlimitedWindow);
    expect(s.window).toBeUndefined();
    expect(m.window).toBeUndefined();
    await m.whenCredit;
    for (let i = 0; i < 50; i++) {
      void m.replyItem(bytes([i]));
    }
    void m.reply(empty);
    expect(await collect(s.items)).toHaveLength(50);
    expect(s.window).toBeUndefined();
    // Dart StateError: a plain Error here.
    const error = thrownBy(() => s.grant(1));
    expect(error).toBeInstanceOf(Error);
    expect(error).not.toBeInstanceOf(RangeError);
    await p.close();
  });

  test('grant checks its argument and the window', async () => {
    const p = new WindowPair();
    const served = nextRequest(p.b);
    const s = p.a.streamRequest('LIST', empty, { window: TalkFrame.unlimitedWindow - 3 });
    const m = await served;
    // Dart ArgumentError: RangeError here.
    expect(() => s.grant(0)).toThrow(RangeError);
    expect(() => s.grant(-1)).toThrow(RangeError);
    // Up to 0xFFFFFFFE left, never 0xFFFFFFFF.
    expect(() => s.grant(3)).toThrow(RangeError);
    s.grant(2);
    expect(s.window).toBe(TalkFrame.unlimitedWindow - 1);
    await flush();
    expect(m.window).toBe(TalkFrame.unlimitedWindow - 1);
    expect(p.a.isOpen).toBe(true);
    expect(p.b.isOpen).toBe(true);
    // The window values the request API takes.
    expect(() => p.a.streamRequest('X', empty, { window: -1 })).toThrow(RangeError);
    expect(() => p.a.streamRequest('X', empty, { window: TalkFrame.unlimitedWindow })).toThrow(
      RangeError,
    );
    expect(p.a.outgoingRequestCount).toBe(1);
    void m.reply(empty);
    await s.done;
    // After the end: a no-op, though the arguments are still checked.
    s.grant(1);
    expect(() => s.grant(0)).toThrow(RangeError);
    await flush();
    expect(p.credits).toHaveLength(1);
    await p.close();
  });
});

describe('credits received', () => {
  test('a credit for a request that is not outstanding is ignored', async () => {
    const peer = new RawPeer({ status: true });
    const requests = new Queue(peer.talk.messages);
    peer.send(streamRequestFrame(1, 1));
    const m = await requests.next();
    await m.reply(empty);
    // For the request just answered, and for one never seen.
    peer.send(credit(1, 4));
    peer.send(credit(9, 4));
    await flush();
    expect(peer.talk.isOpen).toBe(true);
    // The channel still serves.
    peer.send(streamRequestFrame(2, 0));
    const n = await requests.next();
    expect(n.window).toBe(0);
    await requests.cancel();
    await peer.talk.close();
  });

  const malformed: [string, number, string][] = [
    ['a credit of 0', 3, '32 01 00 00 00 00 00 00'],
    ['a credit on an unlimited window', TalkFrame.unlimitedWindow, '32 01 00 00 01 00 00 00'],
    ['a credit that brings the window to 0xFFFFFFFF', 5, '32 01 00 00 FA FF FF FF'],
    [
      'a credit that overflows u32 arithmetic',
      TalkFrame.unlimitedWindow - 1,
      '32 01 00 00 FF FF FF FF',
    ],
    ['a credit with an 8-byte payload', 3, '32 01 00 00 A0 BB 0D 00 E0 93 04 00'],
  ];
  for (const [label, setup, hex] of malformed) {
    test(`${label} closes the channel PROTOCOL_ERROR`, async () => {
      const peer = new RawPeer({ status: true });
      const requests = new Queue(peer.talk.messages);
      peer.send(streamRequestFrame(1, setup));
      const m = await requests.next();
      peer.sendHex(hex);
      expect((await peer.talk.done).known).toBe(StatusCode.protocolError);
      expect(m.isCancelled).toBe(true);
      await requests.cancel();
    });
  }

  test('the largest credit that fits is no error', async () => {
    const peer = new RawPeer({ status: true });
    const requests = new Queue(peer.talk.messages);
    peer.send(streamRequestFrame(1, 5));
    const m = await requests.next();
    peer.sendHex('32 01 00 00 F9 FF FF FF');
    await flush();
    expect(peer.talk.isOpen).toBe(true);
    expect(m.window).toBe(TalkFrame.unlimitedWindow - 1);
    await requests.cancel();
    await peer.talk.close();
  });

  test('a credit for a request that is not a stream request closes the channel PROTOCOL_ERROR', async () => {
    const peer = new RawPeer({ status: true });
    const requests = new Queue(peer.talk.messages);
    peer.send(new TalkFrame({ kind: TalkKind.message, procedure: Name.of('GET'), requestId: 1 }));
    const m = await requests.next();
    expect(m.window).toBeUndefined();
    await m.whenCredit;
    peer.send(credit(1, 1));
    expect((await peer.talk.done).known).toBe(StatusCode.protocolError);
    await requests.cancel();
  });

  test('an item beyond the window is a channel protocol error on the responder', async () => {
    const peer = new RawPeer({ status: true });
    const s = peer.talk.streamRequest('LIST', empty, { window: 1 });
    const items: number[] = [];
    const failed = rejectsStatus(s.done, StatusCode.protocolError);
    // The error the items end with is the Collector's (Dart `onError`).
    new Collector(s.items, { onValue: (i) => items.push(first(i)) });
    await flush();
    expect(peer.received).toHaveLength(1);
    const id = (peer.received[0] as TalkFrame).requestId;
    // The first item fills the window; the second exceeds it.
    peer.send(itemFrame(id, 1));
    peer.send(itemFrame(id, 2));
    await flush();
    expect(items).toEqual([1]);
    expect((await peer.talk.done).known).toBe(StatusCode.protocolError);
    await failed;
    await peer.talk.close();
  });

  test('the first item of a request opened with window 0 is a channel protocol error on the responder', async () => {
    const peer = new RawPeer({ status: true });
    const s = peer.talk.streamRequest('LIST', empty, { window: 0 });
    const failed = rejectsStatus(s.done, StatusCode.protocolError);
    new Collector(s.items);
    await flush();
    expect(peer.received).toHaveLength(1);
    const id = (peer.received[0] as TalkFrame).requestId;
    peer.send(itemFrame(id, 1));
    expect((await peer.talk.done).known).toBe(StatusCode.protocolError);
    await failed;
    await peer.talk.close();
  });
});
