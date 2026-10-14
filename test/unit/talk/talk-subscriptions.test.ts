/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Port of the Dart `test/talk_test.dart`, groups 'subscriptions',
// 'chained and item requests', 'channel end races and protocol errors' and
// 'generated stub conveniences': leaving the iteration of messages and
// items early, the responder limits on chained and item requests, races at
// the channel end, and what generated stubs rely on (lossless names,
// replyStream).
//
// Dart subscriptions are async iteration here: delivery is pull-based, so
// cancelling a subscription is stopping the iteration (`return()`), and
// `Stream.first` takes one value and stops. The reply methods take a Name
// as their `procedure` where the Dart ones take a `name` beside it.

import { describe, expect, test } from 'vitest';
import { Name } from '../../../src/core/name.ts';
import { Status, StatusCode, SwitchboardError } from '../../../src/core/status.ts';
import type { TalkMessage } from '../../../src/core/talk/contract.ts';
import { TalkAbortError } from '../../../src/core/talk/talk-channel.ts';
import { TalkFrame, TalkKind } from '../../../src/core/talk/talk-frame.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';
import { FakeClock, flush } from '../../support/talk/fake-clock.ts';
import { Pair, RawPeer, type StatusFrameEnd } from '../../support/talk/frame-link.ts';
import {
  bytes,
  Collector,
  expectStatus,
  Outcome,
  Queue,
  rejectsStatus,
  serve,
  throwsStatus,
  uncaughtErrors,
} from '../../support/talk/helpers.ts';

const empty = new Uint8Array(0);

/** The first value of [iterable], then the iteration stops: Dart's `Stream.first`. */
async function first<T>(iterable: AsyncIterable<T>): Promise<T> {
  for await (const value of iterable) {
    return value;
  }
  throw new Error('no element');
}

/** Every value of [iterable]: Dart's `Stream.toList`. */
async function toList<T>(iterable: AsyncIterable<T>): Promise<T[]> {
  const values: T[] = [];
  for await (const value of iterable) {
    values.push(value);
  }
  return values;
}

/** [values], one per microtask: Dart's `Stream.fromIterable`. */
async function* fromIterable<T>(values: T[]): AsyncGenerator<T> {
  for (const value of values) {
    yield value;
  }
}

/** A source that fails at once with [error]: Dart's `Stream.error`. */
function failing<T>(error: unknown): AsyncIterable<T> {
  return {
    [Symbol.asyncIterator]: () => ({
      next: () => Promise.reject(error),
    }),
  };
}

type SourceEvent =
  | { readonly kind: 'value'; readonly value: Uint8Array }
  | { readonly kind: 'error'; readonly error: unknown }
  | { readonly kind: 'done' };

/**
 * A byte source driven by hand, as a Dart `StreamController`: what is
 * added waits until asked for; stopping the iteration (`return()`) is the
 * subscription's cancel, which [onCancel] hears of, its result being what
 * `return()` resolves or rejects with.
 */
class Source implements AsyncIterable<Uint8Array> {
  readonly #events: SourceEvent[] = [];
  readonly #onCancel: (() => unknown) | undefined;
  #waiting: Deferred<IteratorResult<Uint8Array>> | undefined;
  #listened = false;
  #cancelled = false;

  constructor(onCancel?: () => unknown) {
    this.#onCancel = onCancel;
  }

  /** Iterated, and not stopped. */
  get hasListener(): boolean {
    return this.#listened && !this.#cancelled;
  }

  /** The iteration was stopped. */
  get isCancelled(): boolean {
    return this.#cancelled;
  }

  add(value: Uint8Array): void {
    this.#push({ kind: 'value', value });
  }

  addError(error: unknown): void {
    this.#push({ kind: 'error', error });
  }

  close(): void {
    this.#push({ kind: 'done' });
  }

  #push(event: SourceEvent): void {
    if (this.#cancelled) {
      return;
    }
    const waiting = this.#waiting;
    if (waiting === undefined) {
      this.#events.push(event);
      return;
    }
    this.#waiting = undefined;
    Source.#settle(waiting, event);
  }

  static #settle(waiting: Deferred<IteratorResult<Uint8Array>>, event: SourceEvent): void {
    if (event.kind === 'value') {
      waiting.resolve({ value: event.value, done: false });
    } else if (event.kind === 'error') {
      waiting.reject(event.error);
    } else {
      waiting.resolve({ value: undefined, done: true });
    }
  }

  [Symbol.asyncIterator](): AsyncIterator<Uint8Array> {
    this.#listened = true;
    return {
      next: () => {
        const waiting = new Deferred<IteratorResult<Uint8Array>>();
        const event = this.#events.shift();
        if (this.#cancelled) {
          waiting.resolve({ value: undefined, done: true });
        } else if (event !== undefined) {
          Source.#settle(waiting, event);
        } else {
          this.#waiting = waiting;
        }
        return waiting.promise;
      },
      return: async () => {
        if (!this.#cancelled) {
          this.#cancelled = true;
          this.#events.length = 0;
          const waiting = this.#waiting;
          this.#waiting = undefined;
          waiting?.resolve({ value: undefined, done: true });
          await this.#onCancel?.();
        }
        return { value: undefined, done: true };
      },
    };
  }
}

describe('subscriptions', () => {
  test('leaving await for early cancels the stream request', async () => {
    const p = new Pair();
    const cancelled = new Deferred<void>();
    serve(p.b, (m) => {
      void m.replyItem(bytes([1]));
      void m.replyItem(bytes([2]));
      void m.replyItem(bytes([3]));
      void m.onCancel.then(() => cancelled.resolve());
    });
    const s = p.a.streamRequest('WATCH', empty);
    for await (const item of s.items) {
      if (item.payload[0] === 2) {
        break;
      }
    }
    await cancelled.promise;
    await flush();
    const cancel = p.aToB.at(-1);
    expect(cancel?.kind).toBe(TalkKind.abort);
    expect(cancel?.requestId).toBe(s.requestId);
    expect(cancel?.status.known).toBe(StatusCode.cancelled);
    expect(p.bToA.at(-1)?.kind).toBe(TalkKind.abort);
    expect(p.bToA.at(-1)?.responseId).toBe(s.requestId);
    await rejectsStatus(s.done, StatusCode.cancelled);
    expect(p.a.outgoingRequestCount).toBe(0);
    expect(p.b.incomingRequestCount).toBe(0);
    await p.close();
  });

  test('messages.first: buffered requests never delivered get UNIMPLEMENTED', async () => {
    const peer = new RawPeer({ options: { replyTimeout: 0 } });
    for (const [id, name] of [
      [1, 'A'],
      [2, 'B'],
      [3, 'C'],
    ] as const) {
      peer.send(new TalkFrame({ kind: TalkKind.message, procedure: Name.of(name), requestId: id }));
    }
    peer.send(new TalkFrame({ kind: TalkKind.message, procedure: Name.of('NOTE') }));
    await flush();
    const m = await first(peer.talk.messages);
    expect(m.procedureName).toBe('A');
    void m.reply(empty);
    await flush();
    const answers = new Map<number, StatusCode | undefined | null>();
    for (const f of peer.received) {
      answers.set(f.responseId, f.kind === TalkKind.abort ? f.status.known : null);
    }
    expect(answers).toEqual(
      new Map([
        [1, null],
        [2, StatusCode.unimplemented],
        [3, StatusCode.unimplemented],
      ]),
    );
    expect(peer.talk.incomingRequestCount).toBe(0);
    await peer.talk.close();
  });

  test('items.first: buffered item requests never delivered get CANCELLED', async () => {
    const peer = new RawPeer({ options: { replyTimeout: 0 } });
    const s = peer.talk.streamRequest('S', empty);
    await flush();
    for (const id of [5, 6]) {
      peer.send(
        new TalkFrame({ kind: TalkKind.streamItem, requestId: id, responseId: s.requestId }),
      );
    }
    await flush();
    const item = await first(s.items);
    expect(item.requestId).toBe(5);
    await flush();
    const refused = peer.received.find((f) => f.responseId === 6);
    expect(refused?.status.known).toBe(StatusCode.cancelled);
    const cancel = peer.received.find((f) => f.hasRequest);
    expect(cancel?.requestId).toBe(s.requestId);
    // Delivered items stay usable.
    expect(item.canReply).toBe(true);
    void item.reply(empty);
    expect(peer.talk.incomingRequestCount).toBe(0);
    peer.send(
      new TalkFrame({
        kind: TalkKind.abort,
        responseId: s.requestId,
        payload: Status.of(StatusCode.cancelled).encode(),
      }),
    );
    await flush();
    expect(peer.talk.outgoingRequestCount).toBe(0);
    await peer.talk.close();
  });

  test('items subscription cancelled, then an item request gets ABORT CANCELLED', async () => {
    const peer = new RawPeer();
    const s = peer.talk.streamRequest('S', empty);
    const subscription = s.items[Symbol.asyncIterator]();
    await subscription.return?.();
    await flush();
    expect(peer.received.at(-1)?.kind).toBe(TalkKind.abort);
    expect(peer.received.at(-1)?.requestId).toBe(s.requestId);
    peer.send(new TalkFrame({ kind: TalkKind.streamItem, requestId: 7, responseId: s.requestId }));
    await flush();
    expect(peer.received.at(-1)?.responseId).toBe(7);
    expect(peer.received.at(-1)?.status.known).toBe(StatusCode.cancelled);
    expect(peer.talk.incomingRequestCount).toBe(0);
    await peer.talk.close();
  });

  test('a STREAM_ITEM with HAS_REQUEST to a cancelled stream gets ABORT CANCELLED', async () => {
    const peer = new RawPeer();
    const s = peer.talk.streamRequest('S', empty);
    const items = new Collector(s.items);
    s.cancel();
    await flush();
    peer.send(new TalkFrame({ kind: TalkKind.streamItem, responseId: s.requestId }));
    peer.send(
      new TalkFrame({
        kind: TalkKind.streamItem,
        requestId: 8,
        responseId: s.requestId,
        stream: true,
      }),
    );
    await flush();
    expect(items.values).toEqual([]);
    expect(peer.received).toHaveLength(3);
    expect(peer.received.at(-1)?.responseId).toBe(8);
    expect(peer.received.at(-1)?.status.known).toBe(StatusCode.cancelled);
    expect(peer.talk.isOpen).toBe(true);
    await peer.talk.close();
  });
});

describe('chained and item requests', () => {
  test('the responder timeout applies to chained and item requests', async () => {
    const clock = new FakeClock();
    const p = new Pair({ a: { replyTimeout: 30, clock }, b: { clock } });
    const results = new Map<string, Outcome<TalkMessage>>();
    serve(p.b, (m) => {
      if (m.procedureName === 'CHAIN') {
        results.set('chain', new Outcome(m.replyRequest(empty)));
      } else {
        results.set('item', new Outcome(m.replyItemRequest(empty)));
      }
    });
    const chained = new Outcome(p.a.request('CHAIN', empty));
    const s = p.a.streamRequest('ITEMS', empty);
    const items = new Collector(s.items);
    await clock.elapse(29);
    expect(chained.value?.expectsReply).toBe(true);
    expect(items.values).toHaveLength(1);
    expect(items.values[0]?.expectsReply).toBe(true);
    expect(results.get('chain')?.isDone).toBe(false);
    await clock.elapse(2);
    for (const key of ['chain', 'item']) {
      const error = results.get(key)?.error;
      expect(error).toBeInstanceOf(TalkAbortError);
      expectStatus(error, StatusCode.deadlineExceeded);
    }
    expect(chained.value?.isCancelled).toBe(true);
    expect(items.values[0]?.canReply).toBe(false);
    expect(p.a.incomingRequestCount).toBe(0);
    void p.close();
    await flush();
    expect(clock.pendingCount).toBe(0);
  });

  test('maxIncomingRequests applies to chained and item requests', async () => {
    const p = new Pair({ a: { maxIncomingRequests: 1 } });
    const itemResults: Promise<TalkMessage>[] = [];
    serve(p.b, (m) => {
      if (m.procedureName === 'CHAIN') {
        itemResults.push(m.replyRequest(empty));
      } else {
        itemResults.push(m.replyItemRequest(bytes([1])));
        itemResults.push(m.replyItemRequest(bytes([2])));
      }
    });
    const s = p.a.streamRequest('ITEMS', empty);
    const items = new Collector(s.items);
    await flush();
    expect(items.values).toHaveLength(2);
    expect(items.values[0]?.canReply).toBe(true);
    expect(items.values[1]?.expectsReply).toBe(true);
    // Refused by the limit.
    expect(items.values[1]?.canReply).toBe(false);
    await rejectsStatus(itemResults[1] as Promise<TalkMessage>, StatusCode.resourceExhausted);
    // A chained final is refused the same way, but still completes the
    // request.
    const chained = await p.a.request('CHAIN', empty);
    expect(chained.expectsReply).toBe(true);
    expect(chained.canReply).toBe(false);
    await rejectsStatus(itemResults[2] as Promise<TalkMessage>, StatusCode.resourceExhausted);
    void items.values[0]?.reply(empty);
    expect((await (itemResults[0] as Promise<TalkMessage>)).expectsReply).toBe(false);
    await p.close();
  });
});

describe('channel end races and protocol errors', () => {
  test('close after the raw stream ended uses the end status', async () => {
    const peer = new RawPeer({ status: true });
    const mux = peer.status as StatusFrameEnd;
    const pending = peer.talk.request('X', empty);
    await flush();
    peer.raw.end();
    await flush();
    const closing = peer.talk.close(Status.of(StatusCode.internal));
    mux.completeDone(Status.of(StatusCode.goingAway));
    await rejectsStatus(pending, StatusCode.goingAway);
    await closing;
    expect((await peer.talk.done).known).toBe(StatusCode.goingAway);
    // Nothing to close.
    expect(mux.closedWith).toEqual([]);
  });

  test('a one-byte abort payload closes the channel with PROTOCOL_ERROR', async () => {
    const peer = new RawPeer({ status: true });
    const mux = peer.status as StatusFrameEnd;
    const pending = peer.talk.request('X', empty);
    await flush();
    peer.sendHex('24 01 00 00 05');
    await rejectsStatus(pending, StatusCode.protocolError);
    expect((await peer.talk.done).known).toBe(StatusCode.protocolError);
    expect(mux.closedWith.map((s) => s.known)).toEqual([StatusCode.protocolError]);
  });

  test('a request without procedure closes the channel with PROTOCOL_ERROR', async () => {
    const peer = new RawPeer({ status: true });
    const mux = peer.status as StatusFrameEnd;
    const messages: TalkMessage[] = [];
    serve(peer.talk, (m) => {
      messages.push(m);
    });
    peer.sendHex('02 01 00 00');
    expect((await peer.talk.done).known).toBe(StatusCode.protocolError);
    expect(messages).toEqual([]);
    expect(mux.closedWith.map((s) => s.known)).toEqual([StatusCode.protocolError]);
  });
});

describe('generated stub conveniences', () => {
  // Not valid UTF-8: a string cannot stand for these names.
  const odd = Name.fromBytes([0xff, 0x41, 0, 0, 0, 0, 0, 0]);
  const other = Name.fromBytes([0x80, 0, 0, 0, 0, 0, 0, 0]);
  const ignored = 'not used: longer than eight bytes';

  // The Dart reply methods take `procedure: ignored, name: other`; the TS
  // ones have no `name` option, their `procedure` taking a Name as is.
  test('a Name takes precedence over the procedure string', async () => {
    const p = new Pair();
    const seen: Name[] = [];
    const backs: number[][] = [];
    serve(p.b, async (m) => {
      seen.push(m.procedure);
      if (!m.expectsReply) {
        return;
      }
      if (m.expectsStream) {
        void m.replyItem(bytes([1]), { procedure: other });
        const answer = await m.replyItemRequest(bytes([2]), { procedure: other });
        void m.reply(answer.payload, { procedure: odd });
      } else {
        const back = await m.replyRequest(bytes([3]), { procedure: other });
        backs.push([...back.payload]);
      }
    });
    void p.a.send(ignored, bytes([0]), { name: odd });
    const chained = await p.a.request(ignored, bytes([0]), { name: odd });
    expect(chained.procedure.equals(other)).toBe(true);
    void chained.reply(bytes([4]));
    const stream = p.a.streamRequest(ignored, bytes([0]), { name: odd });
    const items = new Queue(stream.items);
    const item = await items.next();
    expect(item.procedure.equals(other)).toBe(true);
    const asking = await items.next();
    expect(asking.procedure.equals(other)).toBe(true);
    void asking.reply(bytes([5]));
    const end = await stream.done;
    expect(end.procedure.equals(odd)).toBe(true);
    expect([...end.payload]).toEqual([5]);
    await flush();
    expect(backs).toEqual([[4]]);
    expect(seen.map((name) => name.equals(odd))).toEqual([true, true, true]);
    const requests = p.aToB.filter((f) => f.responseId === 0);
    expect(requests.length).toBeGreaterThan(0);
    expect(requests.every((f) => f.procedure?.equals(odd) === true)).toBe(true);
    // Without a name the string still applies, and is still checked.
    expect(() => p.a.send(ignored, bytes([0]))).toThrow(RangeError);
    await p.close();
  });

  test('startRequest and the start/stream reply variants take a name too', async () => {
    const p = new Pair();
    serve(p.b, (m) => {
      if (m.expectsStream) {
        void m.startReplyItemRequest(bytes([1]), { procedure: other }).response;
        void m.replyItemStreamRequest(bytes([2]), { procedure: other }).done;
        void m.replyStreamRequest(bytes([3]), { procedure: odd }).done;
      } else {
        void m.startReplyRequest(bytes([4]), { procedure: odd }).response;
      }
    });
    const single = await p.a.startRequest(ignored, bytes([0]), { name: odd }).response;
    expect(single.procedure.equals(odd)).toBe(true);
    const stream = p.a.streamRequest('S', bytes([0]));
    const items = new Queue(stream.items);
    expect((await items.next()).procedure.equals(other)).toBe(true);
    expect((await items.next()).procedure.equals(other)).toBe(true);
    expect((await stream.done).procedure.equals(odd)).toBe(true);
    await items.cancel();
    await p.close();
  });

  test('replyStream: every item, then the trailer', async () => {
    const p = new Pair();
    let finished: Promise<void> | undefined;
    serve(p.b, (m) => {
      if (m.procedureName === 'NONE') {
        // Without a trailer the final reply is empty.
        void m.replyStream(fromIterable<Uint8Array>([]));
        return;
      }
      finished = m.replyStream(fromIterable([bytes([1]), bytes([2]), bytes([3])]), {
        trailer: bytes([9]),
        procedure: odd,
      });
    });
    const stream = p.a.streamRequest('LIST', empty);
    const items = await toList(stream.items);
    expect(items.map((m) => [...m.payload])).toEqual([[1], [2], [3]]);
    expect(items.every((m) => m.procedure.equals(odd))).toBe(true);
    const end = await stream.done;
    expect([...end.payload]).toEqual([9]);
    expect(end.procedure.equals(odd)).toBe(true);
    expect(finished).toBeDefined();
    await finished;
    const none = p.a.streamRequest('NONE', empty);
    expect(await toList(none.items)).toEqual([]);
    expect((await none.done).payload).toHaveLength(0);
    await p.close();
  });

  test('replyStream: a failing source aborts the request', async () => {
    const cases: [unknown, StatusCode][] = [
      [new Error('boom'), StatusCode.internal],
      [SwitchboardError.of(StatusCode.notFound), StatusCode.notFound],
      [new TalkAbortError(Status.of(StatusCode.dataLoss)), StatusCode.dataLoss],
      [new SwitchboardError(Status.ok), StatusCode.internal],
    ];
    for (const [error, code] of cases) {
      const p = new Pair();
      serve(p.b, (m) => {
        void m.replyStream(
          (async function* () {
            yield bytes([1]);
            throw error;
          })(),
        );
      });
      const stream = p.a.streamRequest('X', empty);
      const items = new Queue(stream.items);
      expect([...(await items.next()).payload]).toEqual([1]);
      await rejectsStatus(items.next(), code);
      await rejectsStatus(stream.done, code);
      await p.close();
    }
    const p = new Pair();
    serve(p.b, (m) => {
      void m.replyStream(failing<Uint8Array>(new SwitchboardError(new Status(300, 'app'))));
    });
    const error = await p.a.streamRequest('X', empty).done.then(
      () => undefined,
      (e: unknown) => e,
    );
    expect(error).toBeInstanceOf(TalkAbortError);
    expect((error as TalkAbortError).status.equals(new Status(300, 'app'))).toBe(true);
    await p.close();
  });

  test('replyStream: a cancel stops consuming the source', async () => {
    const p = new Pair();
    const source = new Source();
    let finished: Promise<void> | undefined;
    serve(p.b, (m) => {
      finished = m.replyStream(source);
    });
    const stream = p.a.streamRequest('X', empty);
    const items = new Queue(stream.items);
    source.add(bytes([1]));
    expect([...(await items.next()).payload]).toEqual([1]);
    stream.cancel();
    expect(finished).toBeDefined();
    await finished;
    expect(source.isCancelled).toBe(true);
    source.add(bytes([2]));
    await flush();
    const sent = p.bToA.filter((f) => f.kind === TalkKind.streamItem);
    expect(sent).toHaveLength(1);
    expect(p.bToA.at(-1)?.kind).toBe(TalkKind.abort);
    expect(p.bToA.at(-1)?.status.known).toBe(StatusCode.cancelled);
    source.close();
    await p.close();
  });

  for (const sourceFails of [false, true]) {
    test(`replyStream: cleanup errors after ${sourceFails ? 'a source error' : 'a peer cancel'} are handled`, async () => {
      const errors = await uncaughtErrors(async () => {
        const p = new Pair();
        let cancellations = 0;
        const source = new Source(() => {
          cancellations++;
          return Promise.reject(new Error('cleanup failed'));
        });
        let finished: Outcome<void> | undefined;
        serve(p.b, (m) => {
          finished = new Outcome(m.replyStream(source));
        });
        const stream = p.a.streamRequest('X', empty);
        const answer = new Outcome(stream.done);
        void new Collector(stream.items);
        await flush();
        if (sourceFails) {
          source.addError(new Error('source failed'));
        } else {
          stream.cancel();
        }
        await flush();
        expect(finished?.isDone).toBe(true);
        expect(finished?.error).toBeUndefined();
        expect(cancellations).toBe(1);
        expectStatus(answer.error, sourceFails ? StatusCode.internal : StatusCode.cancelled);
        expect(p.bToA.filter((f) => f.kind === TalkKind.abort)).toHaveLength(1);
        source.close();
        await p.close();
      });
      expect(errors).toEqual([]);
    });
  }

  test('replyStream: the channel closing stops consuming the source', async () => {
    const p = new Pair();
    const source = new Source();
    let finished: Promise<void> | undefined;
    serve(p.b, (m) => {
      finished = m.replyStream(source);
    });
    const stream = p.a.streamRequest('X', empty);
    source.add(bytes([1]));
    expect([...(await first(stream.items)).payload]).toEqual([1]);
    await p.b.close();
    expect(finished).toBeDefined();
    await finished;
    expect(source.isCancelled).toBe(true);
    source.close();
    await p.close();
  });

  test('replyStream throws at once when no stream answer can start', async () => {
    const p = new Pair();
    const source = new Source();
    const arrived = new Queue(p.b.messages);
    void p.a.send('PLAIN', empty);
    const plain = await arrived.next();
    throwsStatus(() => plain.replyStream(source), StatusCode.failedPrecondition);
    const answer = p.a.request('ONE', empty);
    const single = await arrived.next();
    throwsStatus(() => single.replyStream(source), StatusCode.failedPrecondition);
    void single.reply(empty);
    await answer;
    const stream = p.a.streamRequest('MANY', empty);
    const many = await arrived.next();
    void many.reply(empty);
    await stream.done;
    throwsStatus(() => many.replyStream(source), StatusCode.failedPrecondition);
    expect(source.hasListener).toBe(false);
    source.close();
    await arrived.cancel();
    await p.close();
  });
});
