/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Port of the Dart `test/talk_window_test.dart`, groups 'timeouts' and 'the
// responder': item windows on stream requests (wiki page "Polyverse
// Switchboard Talk", sections "Item windows", "Timeouts" and "Credit"), the
// timeouts that stop while the window is exhausted and the credit bound
// that runs instead, and the responder that sends nothing beyond the
// window. The groups 'the window' and 'credits received' are in
// `talk-window.test.ts`.

import { describe, expect, test } from 'vitest';
import { Status, StatusCode } from '../../../src/core/status.ts';
import type { TalkMessage } from '../../../src/core/talk/contract.ts';
import { TalkChannel } from '../../../src/core/talk/talk-channel.ts';
import { TalkFrame, TalkKind } from '../../../src/core/talk/talk-frame.ts';
import { ManualClock } from '../../../src/testing/manual-clock.ts';
import { until } from '../../support/talk/bulk-peers.ts';
import { frameLink, Pair, RawPeer } from '../../support/talk/frame-link.ts';
import { credit, itemFrame, streamRequestFrame } from '../../support/talk/frames.ts';
import {
  bytes,
  Collector,
  expectStatus,
  flush,
  last,
  Outcome,
  only,
  Queue,
  rejectsStatus,
  serve,
} from '../../support/talk/helpers.ts';
import { memoryMuxPair } from '../../support/talk/memory-mux.ts';
import { TimerClock } from '../../support/talk/timer-clock.ts';

const empty = new Uint8Array(0);
const second = 1000;
const hour = 3600 * second;
const day = 24 * hour;

/** A Dart `RawPeer`: over a channel that ends with a status, as the Dart `StatusClosableChannel` does. */
function rawPeer(options: ConstructorParameters<typeof RawPeer>[0] = {}): RawPeer {
  return new RawPeer({ ...options, status: true });
}

/** The frames [peer]'s Talk channel sent for the peer's request [id]. */
function repliesTo(peer: RawPeer, id: number): TalkFrame[] {
  return peer.received.filter((f) => f.responseId === id);
}

/** The credits [p.a] sent. */
function creditsOf(p: Pair): TalkFrame[] {
  return p.aToB.filter((f) => f.isCredit);
}

/** The items [p.b] sent. */
function itemsOf(p: Pair): TalkFrame[] {
  return p.bToA.filter((f) => f.kind === TalkKind.streamItem);
}

/** The single next request [channel] receives; the consumer stops after it, as Dart's `messages.first` does. */
async function nextRequest(channel: TalkChannel): Promise<TalkMessage> {
  const iterator = channel.messages[Symbol.asyncIterator]();
  const result = await iterator.next();
  await iterator.return?.();
  if (result.done === true) {
    throw new Error('no request');
  }
  return result.value;
}

/**
 * A byte source fed by hand, as a Dart `StreamController`: [cancelled]
 * once its consumer stopped.
 */
class Feed implements AsyncIterable<Uint8Array> {
  readonly #buffer: Uint8Array[] = [];
  #waiting: ((result: IteratorResult<Uint8Array>) => void) | undefined;
  #closed = false;
  cancelled = false;

  add(chunk: Uint8Array): this {
    const waiting = this.#waiting;
    if (waiting !== undefined) {
      this.#waiting = undefined;
      waiting({ value: chunk, done: false });
    } else {
      this.#buffer.push(chunk);
    }
    return this;
  }

  close(): void {
    this.#closed = true;
    this.#end();
  }

  #end(): void {
    const waiting = this.#waiting;
    this.#waiting = undefined;
    waiting?.({ value: undefined, done: true });
  }

  [Symbol.asyncIterator](): AsyncIterator<Uint8Array> {
    return {
      next: () => {
        const chunk = this.#buffer.shift();
        if (chunk !== undefined) {
          return Promise.resolve({ value: chunk, done: false });
        }
        if (this.#closed || this.cancelled) {
          return Promise.resolve({ value: undefined, done: true });
        }
        return new Promise((resolve) => {
          this.#waiting = resolve;
        });
      },
      return: () => {
        this.cancelled = true;
        this.#end();
        return Promise.resolve({ value: undefined, done: true });
      },
    };
  }
}

describe('timeouts', () => {
  test('the requester timeout stops when the last granted item arrives, and a grant restarts it', async () => {
    const clock = new ManualClock();
    const peer = rawPeer({ options: { requestTimeout: 10 * second, clock } });
    const s = peer.talk.streamRequest('LIST', empty, { window: 2 });
    const done = new Outcome(s.done);
    new Collector(s.items);
    await flush();
    const id = only(peer.received).requestId;
    expect(s.deadline).toBeDefined();
    await clock.elapseAsync(5 * second);
    peer.send(itemFrame(id, 1));
    await flush();
    // An item that leaves room restarts the gap, as before.
    await clock.elapseAsync(9 * second);
    expect(done.isDone).toBe(false);
    peer.send(itemFrame(id, 2));
    await flush();
    expect(s.window).toBe(0);
    expect(s.deadline).toBeUndefined();
    await clock.elapseAsync(2 * hour);
    expect(done.isDone, 'no timeout while exhausted').toBe(false);
    // An EXTEND while exhausted changes nothing while it lasts.
    peer.send(new TalkFrame({ kind: TalkKind.extend, responseId: id }));
    await flush();
    await clock.elapseAsync(2 * hour);
    expect(done.isDone).toBe(false);
    s.grant(1);
    await flush();
    expect(last(peer.received).isCredit).toBe(true);
    expect(s.deadline).toBeDefined();
    await clock.elapseAsync(9 * second);
    expect(done.isDone).toBe(false);
    await clock.elapseAsync(second);
    expectStatus(done.error, StatusCode.deadlineExceeded);
    expect(last(peer.received).kind).toBe(TalkKind.abort);
    void peer.talk.close();
    await flush();
  });

  test('a request opened with window 0 starts no requester timeout until its first grant', async () => {
    const clock = new ManualClock();
    const peer = rawPeer({ options: { requestTimeout: 10 * second, clock } });
    const s = peer.talk.streamRequest('LIST', empty, { window: 0 });
    const done = new Outcome(s.done);
    await flush();
    expect(s.deadline).toBeUndefined();
    await clock.elapseAsync(3 * day);
    expect(done.isDone).toBe(false);
    expect(clock.pendingCount).toBe(0);
    s.grant(1);
    await flush();
    await clock.elapseAsync(10 * second);
    expectStatus(done.error, StatusCode.deadlineExceeded);
    void peer.talk.close();
    await flush();
  });

  test('a cancel while exhausted keeps the id until the final, or the timeout', async () => {
    const clock = new ManualClock();
    const peer = rawPeer({ options: { requestTimeout: 10 * second, clock } });
    const s = peer.talk.streamRequest('LIST', empty, { window: 0 });
    const done = new Outcome(s.done);
    await flush();
    s.cancel();
    await flush();
    expectStatus(done.error, StatusCode.cancelled);
    expect(last(peer.received).kind).toBe(TalkKind.abort);
    expect(peer.received.filter((f) => f.isCredit)).toEqual([]);
    expect(peer.talk.outgoingRequestCount).toBe(1);
    await clock.elapseAsync(10 * second);
    expect(peer.talk.outgoingRequestCount).toBe(0);
    void peer.talk.close();
    await flush();
  });

  test('the responder timeout stops when the last granted item goes, and a grant restarts it', async () => {
    const clock = new ManualClock();
    const peer = rawPeer({ options: { replyTimeout: 10 * second, clock } });
    let held: TalkMessage | undefined;
    serve(peer.talk, (m) => {
      held = m;
    });
    peer.send(streamRequestFrame(1, 1));
    await flush();
    const m = held as TalkMessage;
    void m.replyItem(bytes([1]));
    await flush();
    expect(m.window).toBe(0);
    // Nothing waits for a grant: neither the responder timeout nor the
    // credit bound runs.
    await clock.elapseAsync(2 * hour);
    expect(repliesTo(peer, 1).map((f) => f.kind)).toEqual([TalkKind.streamItem]);
    expect(m.canReply).toBe(true);
    peer.send(credit(1, 1));
    await flush();
    await clock.elapseAsync(9 * second);
    expect(repliesTo(peer, 1)).toHaveLength(1);
    await clock.elapseAsync(second);
    const abort = last(repliesTo(peer, 1));
    expect(abort.kind).toBe(TalkKind.abort);
    expect(abort.status.known).toBe(StatusCode.deadlineExceeded);
    void peer.talk.close();
    await flush();
  });

  test('a declared deadline that passes while the window is exhausted fires right after the grant, on the requester', async () => {
    const clock = new ManualClock();
    const peer = rawPeer({ options: { requestTimeout: 10 * second, clock } });
    const s = peer.talk.streamRequest('LIST', empty, { window: 1 });
    const done = new Outcome(s.done);
    new Collector(s.items);
    await flush();
    const id = only(peer.received).requestId;
    // The responder declares a deadline of 5 s.
    peer.send(
      new TalkFrame({
        kind: TalkKind.extend,
        responseId: id,
        payload: TalkFrame.extendPayload({ deadline: 5 * second }),
      }),
    );
    await flush();
    expect(s.deadline).toBeDefined();
    peer.send(itemFrame(id, 1));
    await flush();
    // Exhausted: the suspension stops the clocks, the declared deadline
    // among them.
    expect(s.window).toBe(0);
    expect(s.deadline).toBeUndefined();
    await clock.elapseAsync(hour);
    expect(done.isDone, 'no timeout while exhausted').toBe(false);
    // The grant restarts the timeout; the deadline passed during the
    // wait, so it fires at once.
    s.grant(1);
    await flush();
    await clock.elapseAsync(1);
    expectStatus(done.error, StatusCode.deadlineExceeded);
    expect(last(peer.received).kind).toBe(TalkKind.abort);
    void peer.talk.close();
    await flush();
  });

  test('a declared deadline that passes while the window is exhausted fires right after the credit, on the responder', async () => {
    const clock = new ManualClock();
    const peer = rawPeer({ options: { replyTimeout: 10 * second, clock } });
    let held: TalkMessage | undefined;
    serve(peer.talk, (m) => {
      held = m;
    });
    peer.send(streamRequestFrame(1, 1));
    await flush();
    const m = held as TalkMessage;
    void m.extend({ deadline: 5 * second });
    await flush();
    void m.replyItem(bytes([1]));
    await flush();
    // Exhausted, and nothing waits for a grant: neither the declared
    // deadline nor the credit bound runs.
    expect(m.window).toBe(0);
    await clock.elapseAsync(hour);
    expect(repliesTo(peer, 1).map((f) => f.kind)).toEqual([TalkKind.extend, TalkKind.streamItem]);
    expect(m.canReply).toBe(true);
    // The credit restarts the timeout; the deadline passed during the
    // wait, so it fires at once.
    peer.send(credit(1, 1));
    await flush();
    await clock.elapseAsync(1);
    const abort = last(repliesTo(peer, 1));
    expect(abort.kind).toBe(TalkKind.abort);
    expect(abort.status.known).toBe(StatusCode.deadlineExceeded);
    void peer.talk.close();
    await flush();
  });

  test('the credit bound aborts DEADLINE_EXCEEDED after the default, while an item waits', async () => {
    const clock = new ManualClock();
    const peer = rawPeer({ options: { clock } });
    let held: TalkMessage | undefined;
    serve(peer.talk, (m) => {
      held = m;
    });
    peer.send(streamRequestFrame(1, 1));
    await flush();
    const m = held as TalkMessage;
    void m.replyItem(bytes([1]));
    await clock.elapseAsync(30 * second);
    // The bound starts when something waits for a grant.
    const waiting = new Outcome(m.replyItem(bytes([2])));
    await clock.elapseAsync(59 * second);
    expect(repliesTo(peer, 1)).toHaveLength(1);
    expect(m.isCancelled).toBe(false);
    await clock.elapseAsync(second);
    const abort = last(repliesTo(peer, 1));
    expect(abort.kind).toBe(TalkKind.abort);
    expect(abort.status.known).toBe(StatusCode.deadlineExceeded);
    expectStatus(waiting.error, StatusCode.deadlineExceeded);
    expect(m.isCancelled).toBe(true);
    expect(m.canReply).toBe(false);
    expect(peer.talk.incomingRequestCount).toBe(0);
    // A credit arriving afterwards is ignored.
    peer.send(credit(1, 1));
    await flush();
    expect(peer.talk.isOpen).toBe(true);
    expect(repliesTo(peer, 1)).toHaveLength(2);
    void peer.talk.close();
    await flush();
  });

  test('the credit bound runs from the arrival of a request opened with window 0', async () => {
    const clock = new ManualClock();
    const peer = rawPeer({ options: { creditTimeout: 3 * second, clock } });
    let held: TalkMessage | undefined;
    serve(peer.talk, (m) => {
      held = m;
    });
    peer.send(streamRequestFrame(1, 0));
    await flush();
    expect(held).toBeDefined();
    await clock.elapseAsync(2 * second);
    expect(repliesTo(peer, 1)).toEqual([]);
    await clock.elapseAsync(second);
    expect(only(repliesTo(peer, 1)).status.known).toBe(StatusCode.deadlineExceeded);
    expect(held?.isCancelled).toBe(true);
    // A grant in time stops it, and starts the responder timeout, which
    // did not run before.
    peer.send(streamRequestFrame(2, 0));
    await flush();
    await clock.elapseAsync(2 * second);
    peer.send(credit(2, 1));
    await flush();
    await clock.elapseAsync(9 * second);
    expect(repliesTo(peer, 2)).toEqual([]);
    await clock.elapseAsync(second);
    expect(
      only(repliesTo(peer, 2)).status.equals(
        Status.of(StatusCode.deadlineExceeded, 'no reply in time'),
      ),
    ).toBe(true);
    void peer.talk.close();
    await flush();
  });

  test('the credit bound per request: setCreditTimeout, replyStream and Duration.zero', async () => {
    const clock = new ManualClock();
    const peer = rawPeer({ options: { clock } });
    const held: TalkMessage[] = [];
    serve(peer.talk, (m) => {
      held.push(m);
    });
    for (let id = 1; id <= 3; id++) {
      peer.send(streamRequestFrame(id, 0));
    }
    await flush();
    const [held0, held1, held2] = held as [TalkMessage, TalkMessage, TalkMessage];
    held0.setCreditTimeout(5 * second);
    void held1.replyStream([bytes([1])], { creditTimeout: 7 * second });
    held2.setCreditTimeout(0);
    await clock.elapseAsync(5 * second);
    expect(only(repliesTo(peer, 1)).status.known).toBe(StatusCode.deadlineExceeded);
    expect(repliesTo(peer, 2)).toEqual([]);
    await clock.elapseAsync(2 * second);
    expect(only(repliesTo(peer, 2)).status.known).toBe(StatusCode.deadlineExceeded);
    void held2.replyItem(bytes([1]));
    await clock.elapseAsync(2 * day);
    expect(repliesTo(peer, 3)).toEqual([]);
    expect(held2.canReply).toBe(true);
    // Back to the channel default: it restarts from now.
    held2.setCreditTimeout(undefined);
    await clock.elapseAsync(59 * second);
    expect(repliesTo(peer, 3)).toEqual([]);
    await clock.elapseAsync(second);
    expect(only(repliesTo(peer, 3)).status.known).toBe(StatusCode.deadlineExceeded);
    expect(clock.pendingCount).toBe(0);
    void peer.talk.close();
    await flush();
  });

  test('waiting for whenCredit runs the credit bound', async () => {
    const clock = new ManualClock();
    const peer = rawPeer({ options: { creditTimeout: 4 * second, clock } });
    let held: TalkMessage | undefined;
    serve(peer.talk, (m) => {
      held = m;
    });
    peer.send(streamRequestFrame(1, 1));
    await flush();
    const m = held as TalkMessage;
    void m.replyItem(bytes([1]));
    await clock.elapseAsync(hour);
    expect(repliesTo(peer, 1)).toHaveLength(1);
    const granted = new Outcome(m.whenCredit);
    await clock.elapseAsync(4 * second);
    expect(granted.isDone).toBe(true);
    expect(m.isCancelled).toBe(true);
    expect(last(repliesTo(peer, 1)).status.known).toBe(StatusCode.deadlineExceeded);
    void peer.talk.close();
    await flush();
  });

  test('a credit that raced the end of a request by its credit bound is ignored, and one after the peer reused the id is for the new request', async () => {
    const clock = new ManualClock();
    const peer = rawPeer({ options: { creditTimeout: 3 * second, clock } });
    const held: TalkMessage[] = [];
    serve(peer.talk, (m) => {
      held.push(m);
    });
    peer.send(streamRequestFrame(1, 0));
    await flush();
    const old = new Outcome(only(held).replyItem(bytes([1])));
    await clock.elapseAsync(3 * second);
    expectStatus(old.error, StatusCode.deadlineExceeded);
    expect(only(repliesTo(peer, 1)).status.known).toBe(StatusCode.deadlineExceeded);
    // Sent before the abort reached the peer: ignored.
    peer.send(credit(1, 1));
    await flush();
    expect(peer.talk.isOpen).toBe(true);
    expect(repliesTo(peer, 1)).toHaveLength(1);
    // The peer reuses the id, released by the abort; then reuses it again
    // while it is outstanding here (its own timeout released it): the
    // earlier one is abandoned, what waited for its window fails, and
    // credits go to the request that holds the id now.
    peer.send(streamRequestFrame(1, 0));
    await flush();
    const reused = new Outcome((held[1] as TalkMessage).replyItem(bytes([2])));
    peer.send(streamRequestFrame(1, 0));
    await flush();
    expectStatus(reused.error, StatusCode.failedPrecondition);
    expect(held[1]?.isCancelled).toBe(true);
    const current = new Outcome((held[2] as TalkMessage).replyItem(bytes([3])));
    peer.send(credit(1, 1));
    await flush();
    expect(current.isDone && current.error === undefined).toBe(true);
    expect(
      repliesTo(peer, 1)
        .filter((f) => f.kind === TalkKind.streamItem)
        .map((f) => only([...f.payload])),
    ).toEqual([3]);
    expect(held[2]?.window).toBe(0);
    void peer.talk.close();
    await flush();
  });

  test('a long credit bound is armed in steps a JavaScript timer takes, and expires on time', async () => {
    // The longest delay a JavaScript timer takes, a signed 32-bit number
    // of milliseconds (about 24.8 days); a longer one fires at once.
    const maxTimerDelay = 0x7fffffff;
    const clock = new TimerClock();
    const expectTimersInRange = (): void => {
      for (const delay of clock.pendingDelays) {
        expect(delay).toBeLessThanOrEqual(maxTimerDelay);
      }
    };
    const peer = rawPeer({ options: { creditTimeout: 30 * day, clock } });
    let held: TalkMessage | undefined;
    serve(peer.talk, (m) => {
      held = m;
    });
    peer.send(streamRequestFrame(1, 0));
    await flush();
    expect(held).toBeDefined();
    expectTimersInRange();
    await clock.elapseAsync(29 * day);
    expectTimersInRange();
    expect(repliesTo(peer, 1)).toEqual([]);
    await clock.elapseAsync(day);
    expect(only(repliesTo(peer, 1)).status.known).toBe(StatusCode.deadlineExceeded);
    expect(clock.pendingCount).toBe(0);
    void peer.talk.close();
    await flush();
  });
});

describe('the responder', () => {
  test('window and whenCredit follow the grants', async () => {
    const p = new Pair();
    const served = new Queue(p.b.messages);
    const s = p.a.streamRequest('LIST', empty, { window: 1 });
    const plain = p.a.request('GET', empty);
    new Collector(s.items);
    const m = await served.next();
    const g = await served.next();
    expect(g.window).toBeUndefined();
    await g.whenCredit;
    expect(m.window).toBe(1);
    await m.whenCredit;
    void m.replyItem(bytes([1]));
    expect(m.window).toBe(0);
    const granted = new Outcome(m.whenCredit);
    await flush();
    expect(granted.isDone).toBe(false);
    s.grant(2);
    await flush();
    expect(granted.isDone).toBe(true);
    expect(m.window).toBe(2);
    // A grant used up at once by what waited does not complete it.
    void m.replyItem(bytes([2]));
    void m.replyItem(bytes([3]));
    void m.replyItem(bytes([4]));
    const next = new Outcome(m.whenCredit);
    s.grant(1);
    await flush();
    expect(m.window).toBe(0);
    expect(next.isDone).toBe(false);
    // The end of the request completes it too.
    s.cancel();
    await flush();
    expect(next.isDone).toBe(true);
    expect(next.error).toBeUndefined();
    expect(m.isCancelled).toBe(true);
    await g.reply(empty);
    await plain;
    await served.cancel();
    await p.close();
  });

  test('the future of replyItem waits at zero and completes on a grant', async () => {
    const p = new Pair();
    const served = nextRequest(p.b);
    const s = p.a.streamRequest('LIST', empty, { window: 0 });
    new Collector(s.items);
    const m = await served;
    const sent = new Outcome(m.replyItem(bytes([1])));
    const bulk = new Outcome(m.replyItem(empty, { bulk: [bytes([1, 2, 3])], bulkLength: 3 }));
    await flush();
    expect(sent.isDone).toBe(false);
    expect(bulk.isDone).toBe(false);
    s.grant(1);
    await flush();
    expect(sent.isDone).toBe(true);
    expect(sent.error).toBeUndefined();
    // The bulk item waits for its own grant; over a channel that is not a
    // mux channel it then fails as a bulk payload does there.
    expect(bulk.isDone).toBe(false);
    s.grant(1);
    await flush();
    expectStatus(bulk.error, StatusCode.unimplemented);
    expect(m.window, 'an item not sent gives its place back').toBe(1);
    void m.reply(empty);
    await s.done;
    await p.close();
  });

  test('replyStream pauses an async* source at its yield, and resumes it there on a grant', async () => {
    const p = new Pair();
    const served = nextRequest(p.b);
    const s = p.a.streamRequest('LIST', empty, { window: 2 });
    const items: number[] = [];
    new Collector(s.items, { onValue: (i) => items.push(only([...i.payload])) });
    const log: string[] = [];
    async function* source(): AsyncGenerator<Uint8Array> {
      for (let i = 0; i < 5; i++) {
        log.push(`yield ${i}`);
        yield bytes([i]);
        log.push(`resumed ${i}`);
      }
      log.push('end');
    }

    const m = await served;
    const answered = new Outcome(m.replyStream(source(), { trailer: bytes([9]) }));
    await flush();
    expect(items).toEqual([0, 1]);
    // Item 2 is read ahead and waits; the source is held at its yield.
    expect(last(log)).toBe('yield 2');
    expect(log).not.toContain('resumed 2');
    s.grant(1);
    await flush();
    expect(items).toEqual([0, 1, 2]);
    expect(last(log)).toBe('yield 3');
    s.grant(2);
    await flush();
    expect(items).toEqual([0, 1, 2, 3, 4]);
    // The end of the source is seen at zero: the trailer goes at once.
    expect(last(log)).toBe('end');
    expect(m.window).toBe(0);
    expect([...(await s.done).payload]).toEqual([9]);
    expect(answered.isDone).toBe(true);
    await p.close();
  });

  test('replyStream sees the end of its source at zero', async () => {
    const p = new Pair();
    const served = nextRequest(p.b);
    const s = p.a.streamRequest('LIST', empty, { window: 2 });
    const m = await served;
    let ended = false;
    async function* source(): AsyncGenerator<Uint8Array> {
      yield bytes([1]);
      yield bytes([2]);
      ended = true;
    }

    void m.replyStream(source(), { trailer: bytes([0xaa]) });
    const end = await s.done;
    expect(ended).toBe(true);
    expect([...end.payload]).toEqual([0xaa]);
    expect(await new Queue(s.items).rest()).toHaveLength(2);
    expect(creditsOf(p)).toEqual([]);
    await p.close();
  });

  test('a source that awaits whenCredit produces nothing it cannot deliver', async () => {
    const p = new Pair();
    const served = nextRequest(p.b);
    const s = p.a.streamRequest('LIST', empty, { window: 1 });
    new Collector(s.items);
    const m = await served;
    let produced = 0;
    async function* source(): AsyncGenerator<Uint8Array> {
      for (let i = 0; i < 3; i++) {
        await m.whenCredit;
        if (m.isCancelled) {
          return;
        }
        produced++;
        yield bytes([i]);
      }
    }

    void m.replyStream(source());
    await flush();
    expect(produced).toBe(1);
    s.grant(1);
    await flush();
    expect(produced).toBe(2);
    s.grant(1);
    await s.done;
    expect(produced).toBe(3);
    await p.close();
  });

  test('cancel while exhausted, without a grant', async () => {
    const p = new Pair();
    const served = nextRequest(p.b);
    const s = p.a.streamRequest('LIST', empty, { window: 1 });
    const m = await served;
    const source = new Feed();
    void m.replyStream(source);
    source
      .add(bytes([1]))
      .add(bytes([2]))
      .add(bytes([3]));
    const items: number[] = [];
    new Collector(s.items, { onValue: (i) => items.push(only([...i.payload])) });
    await flush();
    expect(items).toEqual([1]);
    expect(s.window).toBe(0);
    expect(itemsOf(p)).toHaveLength(1);
    s.cancel();
    await rejectsStatus(s.done, StatusCode.cancelled);
    await flush();
    expect(source.cancelled).toBe(true);
    expect(m.isCancelled).toBe(true);
    expect(creditsOf(p)).toEqual([]);
    // What waited for credit never went.
    expect(itemsOf(p)).toHaveLength(1);
    expect(p.a.outgoingRequestCount).toBe(0);
    expect(p.b.incomingRequestCount).toBe(0);
    source.close();
    await p.close();
  });

  test('a cancel while items and a chained final wait for the window is answered ABORT CANCELLED, the only final; nothing that waited goes', async () => {
    const peer = rawPeer();
    const requests = new Queue(peer.talk.messages);
    peer.send(streamRequestFrame(1, 0));
    const m = await requests.next();
    const item = new Outcome(m.replyItem(bytes([1])));
    const chained = m.replyStreamRequest(bytes([2]), { window: 3 });
    const chainedDone = new Outcome(chained.done);
    expect(m.canReply).toBe(false);
    expect(peer.talk.outgoingRequestCount).toBe(1);
    peer.send(
      new TalkFrame({
        kind: TalkKind.abort,
        requestId: 1,
        payload: Status.of(StatusCode.cancelled).encode(),
      }),
    );
    await flush();
    expect(only(repliesTo(peer, 1)).kind).toBe(TalkKind.abort);
    expect(only(repliesTo(peer, 1)).status.known).toBe(StatusCode.cancelled);
    expectStatus(item.error, StatusCode.cancelled);
    expectStatus(chainedDone.error, StatusCode.cancelled);
    expect(m.isCancelled).toBe(true);
    // The chained request never went: its id is free, and a grant for it
    // sends nothing.
    expect(peer.talk.outgoingRequestCount).toBe(0);
    chained.grant(1);
    // A credit for the cancelled request, sent before the peer saw the
    // abort, is ignored.
    peer.send(credit(1, 2));
    await flush();
    expect(peer.received).toHaveLength(1);
    expect(peer.talk.isOpen).toBe(true);
    await requests.cancel();
    await peer.talk.close();
  });

  test('a listener that throws after its final reply, waiting behind items for the window, has answered', async () => {
    const peer = rawPeer();
    let held: TalkMessage | undefined;
    const sent: Outcome<void>[] = [];
    serve(peer.talk, (message) => {
      held = message;
      sent.push(new Outcome(message.replyItem(bytes([1]))), new Outcome(message.reply(bytes([2]))));
      throw new Error('after the answer');
    });
    peer.send(streamRequestFrame(1, 0));
    await flush();
    const m = held as TalkMessage;
    // Nothing went, and no ABORT INTERNAL either: the request is answered,
    // its final waiting behind the item.
    expect(peer.received).toEqual([]);
    expect(m.canReply).toBe(false);
    expect(m.isCancelled).toBe(false);
    peer.send(credit(1, 1));
    await flush();
    expect(repliesTo(peer, 1).map((f) => f.kind)).toEqual([TalkKind.streamItem, TalkKind.message]);
    expect([...last(repliesTo(peer, 1)).payload]).toEqual([2]);
    expect(sent.every((o) => o.isDone && o.error === undefined)).toBe(true);
    expect(peer.talk.incomingRequestCount).toBe(0);
    await peer.talk.close();
  });

  test('the end of the channel while exhausted fails the request with its status on both sides', async () => {
    // The requester.
    const requester = rawPeer();
    const s = requester.talk.streamRequest('LIST', empty, { window: 0 });
    const failed = rejectsStatus(s.done, StatusCode.unavailable);
    await flush();
    requester.status?.endFromPeer(requester.raw, Status.of(StatusCode.unavailable, 'gone'));
    await failed;
    // The responder, with an item waiting for a grant.
    const responder = rawPeer();
    const requests = new Queue(responder.talk.messages);
    responder.send(streamRequestFrame(1, 0));
    const m = await requests.next();
    const waiting = new Outcome(m.replyItem(bytes([1])));
    const granted = new Outcome(m.whenCredit);
    responder.status?.endFromPeer(responder.raw, Status.of(StatusCode.unavailable, 'gone'));
    await flush();
    expectStatus(waiting.error, StatusCode.unavailable);
    expect(granted.isDone).toBe(true);
    expect(m.isCancelled).toBe(true);
    expect(repliesTo(responder, 1)).toEqual([]);
    await requests.cancel();
  });

  test('over a synchronous transport, a cancel sent as the first granted item arrives is answered after the items granted, and drops the rest', async () => {
    // Adapted: there is no synchronous transport here. The tap of the
    // frame link sees each frame as the Talk channel writes it, and the
    // peer's cancel is sent from there, while the credit that let the
    // item go is being handled; the channel reads it after.
    const [local, foreign] = frameLink();
    const talk = new TalkChannel(local);
    const sent: TalkFrame[] = [];
    local.tap = (data) => {
      const frame = TalkFrame.decode(data);
      sent.push(frame);
      if (frame.kind === TalkKind.streamItem && sent.length === 1) {
        foreign.add(
          new TalkFrame({
            kind: TalkKind.abort,
            requestId: 1,
            payload: Status.of(StatusCode.cancelled).encode(),
          }).encode(),
        );
      }
      return true;
    };
    let resolveHeld!: (m: TalkMessage) => void;
    const held = new Promise<TalkMessage>((resolve) => {
      resolveHeld = resolve;
    });
    serve(talk, (message) => resolveHeld(message));
    foreign.add(streamRequestFrame(1, 0).encode());
    const m = await held;
    const replies = [0, 1, 2].map((i) => new Outcome(m.replyItem(bytes([i]))));
    foreign.add(credit(1, 2).encode());
    await flush();
    expect(sent.map((f) => f.kind)).toEqual([
      TalkKind.streamItem,
      TalkKind.streamItem,
      TalkKind.abort,
    ]);
    expect(last(sent).status.known).toBe(StatusCode.cancelled);
    expect(replies[0]?.isDone && replies[0].error === undefined).toBe(true);
    expect(replies[1]?.isDone && replies[1].error === undefined).toBe(true);
    expectStatus(replies[2]?.error, StatusCode.cancelled);
    expect(m.isCancelled).toBe(true);
    expect(talk.incomingRequestCount).toBe(0);
    await talk.close();
  });

  test('a bulk item counts as one item', async () => {
    // Every Talk channel installs bulk routing on its connection: the bulk
    // channels the peer opens go to their parent's Talk layer without a
    // listener on `incoming` adopting them.
    const [client, server] = memoryMuxPair();
    const talk = new TalkChannel(client.open(empty));
    const responder = new TalkChannel(await server.accept());
    const served = nextRequest(responder);
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
    expect(last(lengths)).toBe(1);
    expect(third.isDone).toBe(true);
    void m.reply(empty);
    await s.done;
    await talk.close();
    await responder.done;
    await client.close();
    await server.close();
  });
});
