/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Port of the Dart `test/talk_test.dart`, groups 'aborts' and 'cancel':
// abort responses and channel aborts in both directions, handler
// failures, and cancels from either side of a request.

import { describe, expect, test } from 'vitest';
import { hexString } from '../../../src/core/bytes.ts';
import { Name } from '../../../src/core/name.ts';
import { Status, StatusCode, SwitchboardError } from '../../../src/core/status.ts';
import type { TalkMessage } from '../../../src/core/talk/contract.ts';
import { TalkAbortError } from '../../../src/core/talk/talk-channel.ts';
import { TalkFrame, TalkKind } from '../../../src/core/talk/talk-frame.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';
import { FakeClock, flush } from '../../support/talk/fake-clock.ts';
import { Pair, RawPeer } from '../../support/talk/frame-link.ts';
import {
  bytes,
  expectStatus,
  Outcome,
  Queue,
  rejectsStatus,
  serve,
  throwsStatus,
} from '../../support/talk/helpers.ts';

const empty = new Uint8Array(0);

/** Expects [actual] to be a {@link Status} equal to [expected]: code, reason and body. */
function expectSameStatus(actual: unknown, expected: Status): void {
  expect(actual).toBeInstanceOf(Status);
  expect((actual as Status).equals(expected), `${actual} is not ${expected}`).toBe(true);
}

/** Consumes [messages] to the end, collecting the error it ends with, if any; resolves at the end. */
async function drain(messages: AsyncIterable<TalkMessage>, errors: unknown[]): Promise<void> {
  try {
    for await (const _ of messages) {
      // Taken and dropped, as a Dart listener that ignores data.
    }
  } catch (e) {
    errors.push(e);
  }
}

describe('aborts', () => {
  test('abort response carries the exact status and reason', async () => {
    const p = new Pair();
    serve(p.b, (m) => {
      switch (m.procedureName) {
        case 'MISSING':
          void m.replyAbort(Status.of(StatusCode.notFound, 'nope'));
          break;
        case 'APP':
          void m.replyAbort(new Status(256, 'custom'));
          break;
        case 'APPMAX':
          void m.replyAbort(new Status(0xffff));
          break;
        default:
          void m.replyAbort(Status.of(StatusCode.unimplemented));
      }
    });
    const statusOf = async (procedure: string): Promise<Status> => {
      try {
        await p.a.request(procedure, empty);
      } catch (e) {
        if (e instanceof SwitchboardError) {
          return e.status;
        }
        throw e;
      }
      expect.fail(`request ${procedure} succeeded`);
    };

    expectSameStatus(await statusOf('MISSING'), Status.of(StatusCode.notFound, 'nope'));
    const app = await statusOf('APP');
    expectSameStatus(app, new Status(256, 'custom'));
    expect(app.isApplicationCode).toBe(true);
    expect(app.known).toBeUndefined();
    expectSameStatus(await statusOf('APPMAX'), new Status(0xffff));
    expect((await statusOf('OTHER')).known).toBe(StatusCode.unimplemented);
    expect(hexString(p.bToARaw[0] as Uint8Array)).toBe('24 01 00 00 05 00 6E 6F 70 65');
    expect(p.a.isOpen).toBe(true);
    await p.close();
  });

  test('channel abort from the peer: messages error and end, channel closes', async () => {
    const p = new Pair();
    const errors: unknown[] = [];
    const ended = drain(p.a.messages, errors);
    serve(p.b, () => {});
    const pending = rejectsStatus(p.a.request('WAIT', empty), StatusCode.unauthenticated);
    await flush();
    const status = new Status(16, 'bad token');
    p.b.abort(status);
    expect(p.b.isOpen).toBe(false);
    await ended;
    expect(errors).toHaveLength(1);
    expect(errors[0]).toBeInstanceOf(SwitchboardError);
    expectSameStatus((errors[0] as SwitchboardError).status, status);
    await pending;
    expectSameStatus(await p.a.done, status);
    expectSameStatus(await p.b.done, status);
    expect(p.a.isOpen).toBe(false);
    expect(hexString(p.bToARaw.at(-1) as Uint8Array)).toBe('20 10 00 62 61 64 20 74 6F 6B 65 6E');
  });

  test('channel abort with an application code', async () => {
    const peer = new RawPeer({ status: true });
    const errors: unknown[] = [];
    const ended = drain(peer.talk.messages, errors);
    peer.send(new TalkFrame({ kind: TalkKind.abort, payload: new Status(999, 'x').encode() }));
    expectSameStatus(await peer.talk.done, new Status(999, 'x'));
    await ended;
    expect(errors).toHaveLength(1);
    expect(errors[0]).toBeInstanceOf(SwitchboardError);
    expectSameStatus((errors[0] as SwitchboardError).status, new Status(999, 'x'));
    // Mux CLOSE must not carry application codes.
    const closedWith = peer.status?.closedWith ?? [];
    expect(closedWith).toHaveLength(1);
    expect(closedWith[0]?.known).toBe(StatusCode.unknown);
  });

  test('handler exceptions are answered with ABORT INTERNAL', async () => {
    const p = new Pair();
    void p.b.serve((m) => {
      if (m.procedureName === 'BOOM') {
        throw new Error('secret detail');
      }
      if (m.procedureName === 'LATE') {
        void m.reply(bytes([1]));
        throw new Error('after reply');
      }
      void m.reply(bytes([2]));
    });
    const boom = await rejectsStatus(p.a.request('BOOM', empty), StatusCode.internal);
    expect(boom.status.reason).not.toContain('secret');
    expect([...(await p.a.request('LATE', empty)).payload]).toEqual([1]);
    expect([...(await p.a.request('FINE', empty)).payload]).toEqual([2]);
    void p.a.send('BOOM', empty);
    await flush();
    expect(p.b.isOpen).toBe(true);
    expect(p.b.incomingRequestCount).toBe(0);
    await p.close();
  });

  test('a request whose handler threw is marked cancelled', async () => {
    const p = new Pair();
    const seen = new Map<string, TalkMessage>();
    void p.b.serve((m) => {
      seen.set(m.procedureName, m);
      if (m.procedureName === 'LATE') {
        void m.reply(bytes([1]));
      }
      throw new Error('handler bug');
    });
    await rejectsStatus(p.a.request('BOOM', empty), StatusCode.internal);
    expect([...(await p.a.request('LATE', empty)).payload]).toEqual([1]);
    const boom = seen.get('BOOM') as TalkMessage;
    const onCancel = new Outcome(boom.onCancel);
    await flush();
    expect(boom.isCancelled, 'work should stop').toBe(true);
    expect(onCancel.isDone).toBe(true);
    expect(boom.canReply).toBe(false);
    expect(seen.get('LATE')?.isCancelled, 'it was answered').toBe(false);
    expect(p.bToA.filter((f) => f.kind === TalkKind.abort)).toHaveLength(1);
    await p.close();
  });

  test('item listener exceptions abort the item request', async () => {
    // Adapted: items are pulled (`for await`), with no item listener for
    // Talk to guard as Dart's `items.listen` is guarded. A loop body that
    // throws stops the iteration, the error going to the loop's caller,
    // and stopping the iteration cancels the stream request (`TalkStream.
    // items`, design.md section 12, item 1). The item request the body was
    // handed is the application's: it is aborted by the responder timeout.
    const clock = new FakeClock();
    const p = new Pair({ a: { clock }, b: { clock } });
    const responder = new Deferred<TalkMessage>();
    const itemStatus = new Deferred<number>();
    serve(p.b, async (m) => {
      responder.resolve(m);
      try {
        await m.replyItemRequest(bytes([1]));
        itemStatus.resolve(0);
        void m.reply(bytes([0]));
      } catch (e) {
        if (!(e instanceof SwitchboardError)) {
          throw e;
        }
        itemStatus.resolve(e.status.code);
        if (m.canReply) {
          void m.reply(bytes([e.status.code]));
        }
      }
    });
    const s = p.a.streamRequest('S', empty);
    let loopError: unknown;
    try {
      for await (const _ of s.items) {
        throw new Error('bad item');
      }
    } catch (e) {
      loopError = e;
    }
    expect(loopError).toBeInstanceOf(Error);
    expect((loopError as Error).message).toBe('bad item');
    const done = await rejectsStatus(s.done, StatusCode.cancelled);
    expect(done, 'local').not.toBeInstanceOf(TalkAbortError);
    await flush();
    const cancel = p.aToB.at(-1) as TalkFrame;
    expect(cancel.kind).toBe(TalkKind.abort);
    expect(cancel.requestId).toBe(s.requestId);
    expect(cancel.status.known).toBe(StatusCode.cancelled);
    expect((await responder.promise).isCancelled).toBe(true);
    // The item request still waits for the application's answer.
    expect(p.a.incomingRequestCount).toBe(1);
    await clock.elapse(p.a.options.replyTimeout);
    expect(await itemStatus.promise).toBe(StatusCode.deadlineExceeded);
    expect(p.a.incomingRequestCount).toBe(0);
    expect(p.b.outgoingRequestCount).toBe(0);
    await p.close();
  });

  test('requests are refused when nobody listens to messages any more', async () => {
    const p = new Pair();
    const subscription = new Queue(p.b.messages);
    await subscription.cancel();
    await rejectsStatus(p.a.request('ANY', empty), StatusCode.unimplemented);
    await p.close();
  });
});

describe('cancel', () => {
  test('requester cancels: the channel answers CANCELLED at once and the responder sees onCancel', async () => {
    const p = new Pair();
    const cancelSeen = new Deferred<TalkMessage>();
    // What the handler saw: isCancelled before the cancel, then in onCancel.
    const cancelledStates: boolean[] = [];
    serve(p.b, (m) => {
      void m.replyItem(bytes([1]));
      cancelledStates.push(m.isCancelled);
      void m.onCancel.then(() => {
        cancelledStates.push(m.isCancelled);
        cancelSeen.resolve(m);
      });
    });
    const s = p.a.streamRequest('WATCH', empty);
    const got: number[] = [];
    let error: unknown;
    const ended = (async () => {
      try {
        for await (const m of s.items) {
          expect(m.payload).toHaveLength(1);
          got.push(m.payload[0] as number);
          s.cancel();
        }
      } catch (e) {
        error = e;
      }
    })();
    await ended;
    expect(got).toEqual([1]);
    expectStatus(error, StatusCode.cancelled);
    expect(error, 'local').not.toBeInstanceOf(TalkAbortError);
    await rejectsStatus(s.done, StatusCode.cancelled);
    const cancel = p.aToB.at(-1) as TalkFrame;
    expect(cancel.kind).toBe(TalkKind.abort);
    expect(cancel.requestId).toBe(s.requestId);
    expect(cancel.hasResponse).toBe(false);
    expect(cancel.status.known).toBe(StatusCode.cancelled);

    const m = await cancelSeen.promise;
    expect(cancelledStates).toEqual([false, true]);
    // The channel already sent the final; the request is finished.
    expect(m.canReply).toBe(false);
    const failed = StatusCode.failedPrecondition;
    throwsStatus(() => m.replyItem(bytes([2])), failed);
    throwsStatus(() => m.replyAbort(Status.of(StatusCode.cancelled)), failed);
    throwsStatus(() => m.extend(), failed);
    await flush();
    const aborts = p.bToA.filter((f) => f.kind === TalkKind.abort);
    expect(aborts).toHaveLength(1);
    expect(aborts[0]?.responseId).toBe(s.requestId);
    expect(aborts[0]?.status.known).toBe(StatusCode.cancelled);
    expect(got).toEqual([1]);
    expect(p.a.outgoingRequestCount).toBe(0);
    expect(p.b.incomingRequestCount).toBe(0);
    expect(p.a.isOpen).toBe(true);
    await p.close();
  });

  test('a peer cancel with no responder timeout still finishes the request', async () => {
    const peer = new RawPeer({ options: { replyTimeout: 0 } });
    let held: TalkMessage | undefined;
    serve(peer.talk, (m) => {
      held = m;
    });
    peer.send(new TalkFrame({ kind: TalkKind.message, procedure: Name.of('A'), requestId: 1 }));
    await flush();
    expect(peer.talk.incomingRequestCount).toBe(1);
    peer.sendHex('22 01 00 00 01 00');
    await flush();
    expect(peer.received).toHaveLength(1);
    const abort = peer.received[0] as TalkFrame;
    expect(abort.kind).toBe(TalkKind.abort);
    expect(abort.responseId).toBe(1);
    expect(abort.status.known).toBe(StatusCode.cancelled);
    expect(peer.talk.incomingRequestCount).toBe(0);
    const message = held as TalkMessage;
    expect(message.isCancelled).toBe(true);
    expect(message.canReply).toBe(false);
    throwsStatus(() => message.reply(empty), StatusCode.failedPrecondition);
    await peer.talk.close();
  });

  test('a cancelled request rejects a chained final', async () => {
    const peer = new RawPeer();
    const s = peer.talk.streamRequest('S', empty);
    s.done.catch(() => {});
    s.cancel(new Status(1, 'stop'));
    s.cancel();
    await flush();
    expectSameStatus(peer.received.at(-1)?.status, new Status(1, 'stop'));
    expect(peer.received).toHaveLength(2);
    // The id stays reserved until the final arrives.
    expect(peer.talk.outgoingRequestCount).toBe(1);
    peer.send(
      new TalkFrame({
        kind: TalkKind.message,
        procedure: Name.of('MORE'),
        requestId: 9,
        responseId: s.requestId,
      }),
    );
    await flush();
    const reject = peer.received.at(-1) as TalkFrame;
    expect(reject.kind).toBe(TalkKind.abort);
    expect(reject.responseId).toBe(9);
    expect(reject.status.known).toBe(StatusCode.cancelled);
    expect(peer.talk.outgoingRequestCount).toBe(0);
    await peer.talk.close();
  });

  test('cancel after the request ended does nothing', async () => {
    const p = new Pair();
    serve(p.b, (m) => m.reply(empty));
    const s = p.a.streamRequest('S', empty);
    await s.done;
    const sent = p.aToBRaw.length;
    s.cancel();
    await flush();
    expect(p.aToBRaw).toHaveLength(sent);
    await p.close();
  });

  test('a cancel for a request already answered is ignored', async () => {
    const peer = new RawPeer();
    serve(peer.talk, (m) => m.reply(empty));
    peer.send(new TalkFrame({ kind: TalkKind.message, procedure: Name.of('A'), requestId: 4 }));
    await flush();
    peer.sendHex('22 04 00 00 01 00');
    await flush();
    expect(peer.talk.isOpen).toBe(true);
    expect(peer.received).toHaveLength(1);
    expect(peer.received[0]?.responseId).toBe(4);
    await peer.talk.close();
  });
});
