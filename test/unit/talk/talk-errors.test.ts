/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Port of the Dart `test/talk_test.dart`, groups 'protocol errors and
// channel end', 'abort statuses' and 'unhandled errors': malformed frames
// and kind violations, the raw channel ending or failing, the end status
// passed to and taken from a channel that ends with a status, channel
// aborts, the statuses aborts carry, and failures nobody listens for.

import { describe, expect, test } from 'vitest';
import { hexString } from '../../../src/core/bytes.ts';
import { Name } from '../../../src/core/name.ts';
import { Status, StatusCode } from '../../../src/core/status.ts';
import type { TalkMessage } from '../../../src/core/talk/contract.ts';
import { TalkAbortError } from '../../../src/core/talk/talk-channel.ts';
import { TalkFrame, TalkKind } from '../../../src/core/talk/talk-frame.ts';
import { FakeClock, flush } from '../../support/talk/fake-clock.ts';
import { Pair, RawPeer, type StatusFrameEnd } from '../../support/talk/frame-link.ts';
import {
  bytes,
  Collector,
  expectStatus,
  Outcome,
  rejectsStatus,
  serve,
  throwsStatus,
  uncaughtErrors,
} from '../../support/talk/helpers.ts';

const empty = new Uint8Array(0);

/** Expects [actual] to be the same status as [expected]: code and bytes. */
function expectSame(actual: Status | undefined, expected: Status): void {
  expect(actual?.equals(expected), `${actual} is ${expected}`).toBe(true);
}

/** The error [promise] rejects with; fails the test if it resolves. */
async function rejection(promise: Promise<unknown>): Promise<unknown> {
  try {
    await promise;
  } catch (e) {
    return e;
  }
  throw new Error('expected a rejection');
}

/** Every value of [iterable], as Dart's `Stream.toList`; marked handled until awaited. */
function toList<T>(iterable: AsyncIterable<T>): Promise<T[]> {
  const list = (async () => {
    const values: T[] = [];
    for await (const value of iterable) {
      values.push(value);
    }
    return values;
  })();
  list.catch(() => {});
  return list;
}

/** The status channel under a {@link RawPeer} made with `status: true`. */
function statusOf(peer: RawPeer): StatusFrameEnd {
  return peer.status as StatusFrameEnd;
}

describe('protocol errors and channel end', () => {
  test('STREAM_ITEM for a non-stream request: protocol error', async () => {
    const peer = new RawPeer({ status: true });
    const one = peer.talk.request('ONE', empty);
    const two = peer.talk.request('TWO', empty);
    await flush();
    peer.send(
      new TalkFrame({
        kind: TalkKind.streamItem,
        responseId: (peer.received[0] as TalkFrame).requestId,
      }),
    );
    await rejectsStatus(one, StatusCode.protocolError);
    await rejectsStatus(two, StatusCode.protocolError);
    const done = await peer.talk.done;
    expect(done.known).toBe(StatusCode.protocolError);
    expect(peer.talk.isOpen).toBe(false);
    expect(statusOf(peer).closedWith).toHaveLength(1);
    expect(statusOf(peer).closedWith[0]?.known).toBe(StatusCode.protocolError);
  });

  test('malformed frame: channel closed with protocol error', async () => {
    const peer = new RawPeer();
    const messages = new Collector(peer.talk.messages);
    const pending = peer.talk.request('X', empty);
    const s = peer.talk.streamRequest('Y', empty);
    const items = toList(s.items);
    await flush();
    peer.sendHex('41 48 45 4C 4C 4F 00 00 00');
    await rejectsStatus(pending, StatusCode.protocolError);
    await rejectsStatus(items, StatusCode.protocolError);
    await messages.finished;
    expect(messages.error).toBeUndefined();
    expect((await peer.talk.done).known).toBe(StatusCode.protocolError);
    await flush();
    expect(peer.ended, 'raw channel closed').toBe(true);
    throwsStatus(() => peer.talk.send('X', empty), StatusCode.failedPrecondition);
    throwsStatus(() => peer.talk.request('X', empty), StatusCode.failedPrecondition);
  });

  test('frames after a protocol error are not processed', async () => {
    const peer = new RawPeer();
    const messages: TalkMessage[] = [];
    serve(peer.talk, (m) => messages.push(m));
    peer.sendHex('00 01');
    peer.send(new TalkFrame({ kind: TalkKind.message, procedure: Name.of('A') }));
    await peer.talk.done;
    expect(messages).toHaveLength(0);
  });

  test('underlying channel closing: connectionLost, messages end, done', async () => {
    const peer = new RawPeer();
    let held: TalkMessage | undefined;
    const messages = new Collector(peer.talk.messages, {
      onValue: (m) => {
        held = m;
      },
    });
    const pending = peer.talk.request('X', empty);
    const s = peer.talk.streamRequest('Y', empty);
    const items = toList(s.items);
    peer.send(new TalkFrame({ kind: TalkKind.message, procedure: Name.of('Q'), requestId: 1 }));
    await flush();
    expect(held?.canReply).toBe(true);
    peer.raw.end();
    await rejectsStatus(pending, StatusCode.connectionLost);
    await rejectsStatus(items, StatusCode.connectionLost);
    await rejectsStatus(s.done, StatusCode.connectionLost);
    await messages.finished;
    expectSame(await peer.talk.done, Status.ok);
    expect(peer.talk.isOpen).toBe(false);
    const message = held as TalkMessage;
    expect(message.isCancelled).toBe(true);
    await message.onCancel;
    throwsStatus(() => message.reply(empty), StatusCode.failedPrecondition);
    await peer.talk.close();
  });

  test('an error on the raw stream fails the channel', async () => {
    const peer = new RawPeer();
    const pending = rejectsStatus(peer.talk.request('X', empty), StatusCode.connectionLost);
    await flush();
    peer.raw.fail(new Error('transport failed'));
    await pending;
    expect((await peer.talk.done).known).toBe(StatusCode.connectionLost);
    expect(peer.talk.isOpen).toBe(false);
  });

  test('mux channel end status is used for outstanding requests', async () => {
    const peer = new RawPeer({ status: true });
    const pending = peer.talk.request('X', empty);
    await flush();
    statusOf(peer).endFromPeer(peer.raw, Status.of(StatusCode.goingAway));
    await rejectsStatus(pending, StatusCode.goingAway);
    expect((await peer.talk.done).known).toBe(StatusCode.goingAway);
    expect(statusOf(peer).closedWith, 'mux confirms itself').toHaveLength(0);
  });

  test('close passes the status to a status-closable channel', async () => {
    const peer = new RawPeer({ status: true });
    const pending = rejectsStatus(peer.talk.request('X', empty), StatusCode.unavailable);
    await peer.talk.close(Status.of(StatusCode.unavailable, 'bye'));
    await pending;
    expect(statusOf(peer).closedWith).toHaveLength(1);
    expectSame(statusOf(peer).closedWith[0], Status.of(StatusCode.unavailable, 'bye'));
    expectSame(await peer.talk.done, Status.of(StatusCode.unavailable, 'bye'));
  });

  test('local abort sends a channel abort and closes', async () => {
    const peer = new RawPeer({ status: true });
    peer.talk.abort(new Status(1234, 'app'));
    await peer.talk.done;
    await flush();
    expect(peer.received).toHaveLength(1);
    expect(hexString((peer.received[0] as TalkFrame).encode())).toBe('20 D2 04 61 70 70');
    expect(statusOf(peer).closedWith).toHaveLength(1);
    expect(statusOf(peer).closedWith[0]?.known).toBe(StatusCode.unknown);
    expectSame(await peer.talk.done, new Status(1234, 'app'));
    peer.talk.abort(Status.of(StatusCode.internal));
    expect(peer.received).toHaveLength(1);
  });

  test('close: both sides end, outstanding work fails, no timers left', async () => {
    const clock = new FakeClock();
    const p = new Pair({
      a: { requestTimeout: 30, clock },
      b: { replyTimeout: 30, clock },
    });
    let held: TalkMessage | undefined;
    serve(p.b, (m) => {
      held = m;
    });
    const pending = new Outcome(p.a.request('X', empty));
    await flush();
    expect(clock.pendingCount).toBe(2);
    const closing = new Outcome(p.a.close());
    expect(p.a.close()).toBe(p.a.close());
    await flush();
    expectStatus(pending.error, StatusCode.cancelled);
    expect(closing.isDone).toBe(true);
    expect(held?.isCancelled).toBe(true);
    const aDone = new Outcome(p.a.done);
    const bDone = new Outcome(p.b.done);
    await flush();
    expectSame(aDone.value, Status.ok);
    expectSame(bDone.value, Status.ok);
    expect(p.b.isOpen).toBe(false);
    expect(clock.pendingCount).toBe(0);
  });
});

describe('abort statuses', () => {
  test('an empty abort response payload means UNKNOWN', async () => {
    const peer = new RawPeer();
    const pending = peer.talk.request('X', empty);
    await flush();
    peer.sendHex('24 01 00 00');
    const error = await rejection(pending);
    expect(error).toBeInstanceOf(TalkAbortError);
    expect((error as TalkAbortError).code).toBe(StatusCode.unknown);
    expect((error as TalkAbortError).isChannelAbort).toBe(false);
    await peer.talk.close();
  });

  test('an abort response carrying OK reads as UNKNOWN', async () => {
    const peer = new RawPeer();
    const pending = peer.talk.request('X', empty);
    await flush();
    peer.sendHex('24 01 00 00 00 00 6F 6B');
    const error = await rejection(pending);
    expect(error).toBeInstanceOf(TalkAbortError);
    expectSame((error as TalkAbortError).status, new Status(2, 'ok'));
    await peer.talk.close();
  });

  test('an empty channel abort closes the channel with UNKNOWN', async () => {
    const peer = new RawPeer({ status: true });
    const messages = new Collector(peer.talk.messages);
    const pending = peer.talk.request('X', empty);
    await flush();
    peer.sendHex('20');
    expect((await peer.talk.done).known).toBe(StatusCode.unknown);
    const error = await rejection(pending);
    expect(error).toBeInstanceOf(TalkAbortError);
    expect((error as TalkAbortError).code).toBe(StatusCode.unknown);
    expect((error as TalkAbortError).isChannelAbort).toBe(true);
    expect(messages.error).toBeInstanceOf(TalkAbortError);
    expect(statusOf(peer).closedWith).toHaveLength(1);
    expect(statusOf(peer).closedWith[0]?.known).toBe(StatusCode.unknown);
  });

  test('replyAbort and abort refuse OK', async () => {
    const p = new Pair();
    // Recorded here and checked below: serve() catches what the handler throws.
    const thrown: unknown[] = [];
    let canReply: boolean | undefined;
    serve(p.b, (m) => {
      for (const status of [Status.ok, new Status(0, 'fine')]) {
        try {
          void m.replyAbort(status);
        } catch (e) {
          thrown.push(e);
        }
      }
      canReply = m.canReply;
      void m.reply(bytes([1]));
    });
    expect([...(await p.a.request('X', empty)).payload]).toEqual([1]);
    expect(thrown).toHaveLength(2);
    for (const e of thrown) {
      expect(e).toBeInstanceOf(RangeError);
    }
    expect(canReply).toBe(true);
    expect(() => p.a.abort(Status.ok)).toThrow(RangeError);
    expect(p.a.isOpen).toBe(true);
    expect(p.aToB).toHaveLength(1);
    await p.close();
    expect(() => p.a.abort(Status.ok)).toThrow(RangeError);
  });

  test('CONNECTION_LOST never goes on the wire', async () => {
    const lost = Status.of(StatusCode.connectionLost, 'upstream gone');
    const peer = new RawPeer({ status: true });
    const held: TalkMessage[] = [];
    serve(peer.talk, (m) => held.push(m));
    peer.send(new TalkFrame({ kind: TalkKind.message, procedure: Name.of('A'), requestId: 1 }));
    const s = peer.talk.streamRequest('S', empty);
    await flush();
    expect(held).toHaveLength(1);
    void (held[0] as TalkMessage).replyAbort(lost);
    s.cancel(lost);
    await flush();
    const replyAbort = peer.received.find((f) => f.hasResponse) as TalkFrame;
    expect(replyAbort.status.known).toBe(StatusCode.unavailable);
    expect(replyAbort.status.reason).toContain('upstream gone');
    const cancel = peer.received.findLast((f) => f.kind === TalkKind.abort) as TalkFrame;
    expect(cancel.requestId).toBe(s.requestId);
    expect(cancel.status.known).toBe(StatusCode.unavailable);
    // Locally the cancel status is reported as given.
    await rejectsStatus(s.done, StatusCode.connectionLost);

    peer.talk.abort(lost);
    await peer.talk.done;
    await flush();
    const last = peer.received.at(-1) as TalkFrame;
    expect(last.hasRequest).toBe(false);
    expect(last.hasResponse).toBe(false);
    expect(last.status.known).toBe(StatusCode.unavailable);
    expect(statusOf(peer).closedWith).toHaveLength(1);
    expect(statusOf(peer).closedWith[0]?.known).toBe(StatusCode.unavailable);
    expectSame(await peer.talk.done, lost);
  });

  test('close with CONNECTION_LOST, or a peer channel abort carrying it, closes the mux channel with UNAVAILABLE', async () => {
    const closing = new RawPeer({ status: true });
    await closing.talk.close(Status.of(StatusCode.connectionLost));
    expect(statusOf(closing).closedWith).toHaveLength(1);
    expect(statusOf(closing).closedWith[0]?.known).toBe(StatusCode.unavailable);

    const aborted = new RawPeer({ status: true });
    serve(aborted.talk, () => {});
    aborted.send(
      new TalkFrame({
        kind: TalkKind.abort,
        payload: Status.of(StatusCode.connectionLost).encode(),
      }),
    );
    expect((await aborted.talk.done).known).toBe(StatusCode.connectionLost);
    expect(statusOf(aborted).closedWith).toHaveLength(1);
    expect(statusOf(aborted).closedWith[0]?.known).toBe(StatusCode.unavailable);
  });
});

describe('unhandled errors', () => {
  test('a messages listener without onError survives a channel abort', async () => {
    const errors = await uncaughtErrors(async () => {
      const p = new Pair();
      let ended = false;
      // serve() is the listener without an error handler: a channel abort
      // ends it, its status on done.
      void p.a
        .serve(() => {})
        .then(() => {
          ended = true;
        });
      p.b.abort(Status.of(StatusCode.unauthenticated, 'bad token'));
      expect((await p.a.done).known).toBe(StatusCode.unauthenticated);
      await flush();
      expect(ended).toBe(true);
    });
    expect(errors).toEqual([]);
  });

  // Not ported: 'onError(null) on the subscription logs instead': Dart's
  // StreamSubscription.onError replaces a listener's error handler; a TS
  // consumer is an iterator (the error is thrown into it) or serve() (no
  // error handler), so there is no handler to remove. The listener without
  // an error handler is the test above.

  test('items listeners without onError survive aborts, cancels and timeouts', async () => {
    const errors = await uncaughtErrors(async () => {
      const peer = new RawPeer();
      // An iteration throws the request's failure to its consumer, which
      // takes it here (Collector), as an error handler would: what is
      // checked is that nothing else is left unhandled.
      const aborted = peer.talk.streamRequest('A', empty);
      const abortedItems = new Collector(aborted.items);
      const cancelled = peer.talk.streamRequest('C', empty);
      const cancelledItems = new Collector(cancelled.items);
      // Only this request should expire while the event queue is pumped.
      const expired = peer.talk.streamRequest('T', empty, { timeout: 5 });
      const expiredItems = new Collector(expired.items);
      await flush();
      peer.send(
        new TalkFrame({
          kind: TalkKind.abort,
          responseId: aborted.requestId,
          payload: Status.of(StatusCode.notFound).encode(),
        }),
      );
      cancelled.cancel();
      await rejectsStatus(expired.done, StatusCode.deadlineExceeded);
      expect(await rejection(aborted.done)).toBeInstanceOf(TalkAbortError);
      await peer.talk.close();
      await flush();
      expect(abortedItems.error).toBeInstanceOf(TalkAbortError);
      expectStatus(cancelledItems.error, StatusCode.cancelled);
      expectStatus(expiredItems.error, StatusCode.deadlineExceeded);
    });
    expect(errors).toEqual([]);
  });

  test('an abort response to an unlistened items stream', async () => {
    const errors = await uncaughtErrors(async () => {
      const peer = new RawPeer();
      const s = peer.talk.streamRequest('S', empty);
      await flush();
      peer.send(
        new TalkFrame({
          kind: TalkKind.abort,
          responseId: s.requestId,
          payload: new Status(300, 'app').encode(),
        }),
      );
      await flush();
      await peer.talk.close();
      await flush();
      // The error is still there for a late listener.
      expect(await rejection(toList(s.items))).toBeInstanceOf(TalkAbortError);
    });
    expect(errors).toEqual([]);
  });

  test('dropped request futures fail quietly on close', async () => {
    const errors = await uncaughtErrors(async () => {
      const p = new Pair();
      const held: TalkMessage[] = [];
      serve(p.b, (m) => {
        held.push(m);
        if (m.procedureName === 'CHAIN') {
          void m.replyRequest(empty);
        } else if (m.procedureName === 'ITEMS') {
          void m.replyItemRequest(empty);
        }
      });
      void p.a.request('PLAIN', empty);
      void p.a.startRequest('HANDLE', empty).response;
      void p.a.request('CHAIN', empty);
      const s = p.a.streamRequest('ITEMS', empty);
      new Collector(s.items);
      await flush();
      expect(held).toHaveLength(4);
      expect(p.b.outgoingRequestCount).toBe(2);
      await p.close();
      await flush();
    });
    expect(errors).toEqual([]);
  });
});
