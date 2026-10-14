/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Port of the Dart `test/talk_test.dart`, group 'timeouts': the requester
// and responder timeouts, EXTEND and stream items restarting them, the
// release of cancelled ids, and long timeouts armed in steps a JavaScript
// timer takes.

import { describe, expect, onTestFinished, test } from 'vitest';
import { hexString } from '../../../src/core/bytes.ts';
import { Name } from '../../../src/core/name.ts';
import { StatusCode } from '../../../src/core/status.ts';
import type { TalkMessage } from '../../../src/core/talk/contract.ts';
import { TalkFrame, TalkKind } from '../../../src/core/talk/talk-frame.ts';
import type { TimerHandle } from '../../../src/core/util/clock.ts';
import { ManualClock } from '../../../src/testing/manual-clock.ts';
import { Pair, RawPeer } from '../../support/talk/frame-link.ts';
import {
  bytes,
  expectStatus,
  flush,
  Outcome,
  serve,
  throwsStatus,
} from '../../support/talk/helpers.ts';

const empty = new Uint8Array(0);
const ms30 = 30;
const hour = 3_600_000;
const day = 24 * hour;

/**
 * The longest delay a JavaScript timer takes, a signed 32-bit number of
 * milliseconds (about 24.8 days); a longer one fires at once.
 */
const maxTimerDelay = 0x7fffffff;

/** A {@link ManualClock} that knows the delay of each pending timer, as `fake_async`'s `pendingTimers` do. */
class TimerClock extends ManualClock {
  readonly #delays = new Map<TimerHandle, number>();

  override setTimeout(callback: () => void, ms: number): TimerHandle {
    const handle = super.setTimeout(() => {
      this.#delays.delete(handle);
      callback();
    }, ms);
    this.#delays.set(handle, ms);
    return handle;
  }

  override clearTimeout(handle: TimerHandle): void {
    this.#delays.delete(handle);
    super.clearTimeout(handle);
  }

  /** The delays the pending timers were armed with. */
  get pendingDelays(): number[] {
    return [...this.#delays.values()];
  }
}

function expectTimersInRange(clock: TimerClock): void {
  for (const delay of clock.pendingDelays) {
    expect(delay, 'longer timers fire at once on JavaScript').toBeLessThanOrEqual(maxTimerDelay);
  }
}

/** Waits [ms] of real time. */
function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Every value of [iterable], as Dart's `Stream.toList`. */
async function toList<T>(iterable: AsyncIterable<T>): Promise<T[]> {
  const values: T[] = [];
  for await (const value of iterable) {
    values.push(value);
  }
  return values;
}

describe('timeouts', () => {
  test('long requester timeouts are armed in steps a JavaScript timer takes, and expire on time', async () => {
    const clock = new TimerClock();
    const peer = new RawPeer({
      options: { clock, requestTimeout: 30 * day, maxExtension: 0 },
    });
    const gap = new Outcome(peer.talk.request('Q', empty));
    const declared = new Outcome(peer.talk.request('Q', empty));
    await flush();
    peer.send(
      new TalkFrame({
        kind: TalkKind.extend,
        responseId: 2,
        payload: TalkFrame.extendPayload({ deadline: 40 * day }),
      }),
    );
    await flush();
    expectTimersInRange(clock);
    await clock.elapseAsync(29 * day);
    expectTimersInRange(clock);
    expect(gap.isDone).toBe(false);
    await clock.elapseAsync(day);
    expectStatus(gap.error, StatusCode.deadlineExceeded);
    await clock.elapseAsync(9 * day);
    expectTimersInRange(clock);
    expect(declared.isDone).toBe(false);
    await clock.elapseAsync(day);
    expectStatus(declared.error, StatusCode.deadlineExceeded);
    expect(peer.talk.outgoingRequestCount).toBe(0);
    expect(clock.pendingCount).toBe(0);
    await peer.talk.close();
  });

  test('long cancelled-id timeouts rearm until their deadline', async () => {
    const clock = new TimerClock();
    const peer = new RawPeer({ options: { clock, requestTimeout: 30 * day } });
    peer.talk.startRequest('Q', empty).cancel();
    expectTimersInRange(clock);
    await clock.elapseAsync(29 * day);
    expectTimersInRange(clock);
    expect(peer.talk.outgoingRequestCount).toBe(1);
    await clock.elapseAsync(day);
    expect(peer.talk.outgoingRequestCount).toBe(0);
    expect(clock.pendingCount).toBe(0);
    await peer.talk.close();
  });

  test('long responder deadlines are armed in steps a JavaScript timer takes, and expire on time', async () => {
    const clock = new TimerClock();
    const peer = new RawPeer({ options: { clock } });
    let held: TalkMessage | undefined;
    serve(peer.talk, (m) => {
      held = m;
    });
    peer.send(new TalkFrame({ kind: TalkKind.message, procedure: Name.of('Q'), requestId: 1 }));
    await flush();
    void held?.extend({ deadline: 30 * day });
    expectTimersInRange(clock);
    await clock.elapseAsync(29 * day);
    expectTimersInRange(clock);
    expect(held?.canReply).toBe(true);
    await clock.elapseAsync(day);
    expect(held?.canReply).toBe(false);
    const last = peer.received.at(-1) as TalkFrame;
    expect(last.kind).toBe(TalkKind.abort);
    expect(last.status.code).toBe(StatusCode.deadlineExceeded);
    expect(clock.pendingCount).toBe(0);
    await peer.talk.close();
  });

  // Over the system clock: a timer longer than a JavaScript timer takes
  // would fire at once (Node.js runs it after 1 ms), so a few milliseconds
  // of real time show it.
  test('long requester timeouts do not overflow the platform timer', async () => {
    const peer = new RawPeer({ options: { requestTimeout: 30 * day } });
    onTestFinished(() => peer.talk.close());
    const request = peer.talk.startRequest('Q', empty);
    const result = new Outcome(request.response);
    await delay(5);
    expect(result.isDone).toBe(false);
    request.cancel();
    await delay(5);
    expect(peer.talk.outgoingRequestCount, 'the cancelled id keeps its long release timeout').toBe(
      1,
    );
    await peer.talk.close();
  });

  test('long declared deadlines do not overflow the platform timer', async () => {
    const p = new Pair({ a: { maxExtension: 0 } });
    onTestFinished(() => p.close());
    const held: TalkMessage[] = [];
    serve(p.b, (m) => {
      held.push(m);
      void m.extend({ deadline: 30 * day });
    });
    const response = new Outcome(p.a.request('Q', empty));
    await flush();
    await delay(5);
    expect(held).toHaveLength(1);
    expect(held[0]?.canReply).toBe(true);
    expect(response.isDone).toBe(false);
    await p.close();
  });

  test('responder side: ABORT DEADLINE_EXCEEDED, later replies throw', async () => {
    const clock = new TimerClock();
    const p = new Pair({ a: { clock }, b: { clock, replyTimeout: ms30 } });
    let held: TalkMessage | undefined;
    serve(p.b, (m) => {
      held = m;
    });
    const result = new Outcome(p.a.request('SLOW', empty));
    await flush();
    expect(held).toBeDefined();
    expect(p.b.incomingRequestCount).toBe(1);
    await clock.elapseAsync(29);
    expect(result.isDone).toBe(false);
    await clock.elapseAsync(2);
    expectStatus(result.error, StatusCode.deadlineExceeded);
    expect(p.bToA).toHaveLength(1);
    const abort = p.bToA[0] as TalkFrame;
    expect(abort.kind).toBe(TalkKind.abort);
    expect(p.aToB).toHaveLength(1);
    expect(abort.responseId).toBe(p.aToB[0]?.requestId);
    expect(abort.status.code).toBe(StatusCode.deadlineExceeded);
    const message = held as TalkMessage;
    expect(message.canReply).toBe(false);
    expect(message.isCancelled, 'work should stop').toBe(true);
    throwsStatus(() => message.reply(empty), StatusCode.failedPrecondition);
    expect(p.b.incomingRequestCount).toBe(0);
    await p.close();
    expect(clock.pendingCount).toBe(0);
  });

  test('requester side: fails locally and sends a cancel', async () => {
    const clock = new TimerClock();
    const peer = new RawPeer({ options: { clock, requestTimeout: ms30 } });
    const result = new Outcome(peer.talk.request('X', empty));
    const s = peer.talk.streamRequest('Y', empty);
    const items = new Outcome(toList(s.items));
    await flush();
    expect(peer.received).toHaveLength(2);
    const id = peer.received[0]?.requestId as number;
    await clock.elapseAsync(31);
    expectStatus(result.error, StatusCode.deadlineExceeded);
    expectStatus(items.error, StatusCode.deadlineExceeded);
    expect(peer.received).toHaveLength(4);
    const cancel = peer.received[2] as TalkFrame;
    expect(cancel.kind).toBe(TalkKind.abort);
    expect(cancel.requestId).toBe(id);
    expect(cancel.hasResponse).toBe(false);
    expect(cancel.status.code).toBe(StatusCode.cancelled);
    expect(peer.received[3]?.requestId).toBe(s.requestId);
    // The id is released at once; a late answer is ignored.
    expect(peer.talk.outgoingRequestCount).toBe(0);
    peer.send(new TalkFrame({ kind: TalkKind.message, responseId: id }));
    await flush();
    expect(peer.talk.isOpen).toBe(true);
    expect(clock.pendingCount).toBe(0);
    await peer.talk.close();
  });

  test('extend restarts both timers', async () => {
    const clock = new TimerClock();
    const p = new Pair({
      a: { clock, requestTimeout: ms30 },
      b: { clock, replyTimeout: ms30 },
    });
    let held: TalkMessage | undefined;
    serve(p.b, (m) => {
      held = m;
    });
    const result = new Outcome(p.a.request('SLOW', empty));
    await flush();
    for (let i = 0; i < 5; i++) {
      await clock.elapseAsync(20);
      void held?.extend();
    }
    await clock.elapseAsync(20);
    expect(result.isDone).toBe(false);
    void held?.reply(bytes([1]));
    await flush();
    expect([...(result.value as TalkMessage).payload]).toEqual([1]);
    expect(p.bToA.filter((f) => f.kind === TalkKind.extend)).toHaveLength(5);
    expect(hexString(p.bToARaw[0] as Uint8Array)).toBe('34 01 00 00');
    await p.close();
    expect(clock.pendingCount).toBe(0);
  });

  test('stream items restart both timers', async () => {
    const clock = new TimerClock();
    const p = new Pair({
      a: { clock, requestTimeout: ms30 },
      b: { clock, replyTimeout: ms30 },
    });
    let held: TalkMessage | undefined;
    serve(p.b, (m) => {
      held = m;
    });
    const s = p.a.streamRequest('TICK', empty);
    const items = new Outcome(toList(s.items));
    const done = new Outcome(s.done);
    await flush();
    for (let i = 0; i < 5; i++) {
      await clock.elapseAsync(20);
      void held?.replyItem(bytes([i]));
    }
    await clock.elapseAsync(20);
    void held?.reply(empty);
    await flush();
    expect(items.value?.map((m) => m.payload[0])).toEqual([0, 1, 2, 3, 4]);
    expect(items.value?.every((m) => m.payload.length === 1)).toBe(true);
    expect(done.value).toBeDefined();
    await p.close();
    expect(clock.pendingCount).toBe(0);
  });

  test('per-request timeout override', async () => {
    const clock = new TimerClock();
    const p = new Pair({
      a: { clock, requestTimeout: ms30 },
      b: { clock, replyTimeout: 0 },
    });
    const held = new Map<string, TalkMessage>();
    serve(p.b, (m) => {
      held.set(m.procedureName, m);
    });
    const long = new Outcome(p.a.request('LONG', empty, { timeout: 100 }));
    const short = new Outcome(p.a.request('SHORT', empty, { timeout: 10 }));
    const never = new Outcome(p.a.request('NEVER', empty, { timeout: 0 }));
    await clock.elapseAsync(11);
    expectStatus(short.error, StatusCode.deadlineExceeded);
    await clock.elapseAsync(69);
    expect(long.isDone).toBe(false);
    void held.get('LONG')?.reply(bytes([1]));
    await flush();
    expect([...(long.value as TalkMessage).payload]).toEqual([1]);
    await clock.elapseAsync(hour);
    expect(never.isDone).toBe(false);
    await p.close();
    await flush();
    expectStatus(never.error, StatusCode.cancelled);
    expect(clock.pendingCount).toBe(0);
  });

  test('a cancelled request id is released on timeout', async () => {
    const clock = new TimerClock();
    const peer = new RawPeer({ options: { clock, requestTimeout: ms30 } });
    const s = peer.talk.streamRequest('S', empty);
    await flush();
    s.cancel();
    await flush();
    expect(peer.talk.outgoingRequestCount).toBe(1);
    await clock.elapseAsync(31);
    expect(peer.talk.outgoingRequestCount).toBe(0);
    // No second cancel.
    expect(peer.received).toHaveLength(2);
    await peer.talk.close();
  });
});
