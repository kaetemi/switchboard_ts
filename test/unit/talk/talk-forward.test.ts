/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Port of the Dart `test/talk_forward_test.dart`, all groups: message chain
// forwarding through one or more intermediaries (`forwardMessage`, wiki
// page "Switchboard Proxying") — '1 proxy', '2 proxies', 'timeouts',
// 'frame level', 'forwardMessage', 'bulk payloads', 'forwarding credit'
// and 'item windows'. There is no synchronous transport here: the groups
// and tests the Dart file runs over one are not ported (see the comments
// where they would be).

import { afterEach, beforeEach, describe, expect, onTestFinished, test } from 'vitest';
import { bytesEqual, hexBytes, hexString } from '../../../src/core/bytes.ts';
import { Name } from '../../../src/core/name.ts';
import { Status, StatusCode } from '../../../src/core/status.ts';
import type { TalkMessage, TalkOptions, TalkStream } from '../../../src/core/talk/contract.ts';
import { TalkAbortError, TalkChannel } from '../../../src/core/talk/talk-channel.ts';
import { forwardMessage } from '../../../src/core/talk/talk-forward.ts';
import { TalkFrame, TalkKind } from '../../../src/core/talk/talk-frame.ts';
import type { Clock } from '../../../src/core/util/clock.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';
import { ManualClock } from '../../../src/testing/manual-clock.ts';
import { until } from '../../support/talk/bulk-peers.ts';
import { Carrier } from '../../support/talk/carrier.ts';
import { type FrameEnd, frameLink } from '../../support/talk/frame-link.ts';
import {
  bytes,
  Collector,
  expectRemote,
  expectSame,
  expectStatus,
  first,
  flush,
  last,
  Outcome,
  only,
  Queue,
  rejection,
  rejectsRemote,
  rejectsStatus,
  toList,
} from '../../support/talk/helpers.ts';
import { costOf, type MemoryMuxChannel, memoryMuxPair } from '../../support/talk/memory-mux.ts';

const empty = new Uint8Array(0);
const hour = 3_600_000;

/** Lets everything in flight settle, as Dart's `pumpEventQueue`. */
const pumpEventQueue = (): Promise<void> => flush(20);

// ---------------------------------------------------------------------------
// Assertions made in handlers and listeners

/**
 * What handlers and listeners threw: an assertion that fails in one fails
 * the Dart test through its zone, here through the check after each test.
 */
const listenerErrors: unknown[] = [];

afterEach(() => {
  expect(listenerErrors.splice(0)).toEqual([]);
});

/** [handler], recording what it throws or rejects with in {@link listenerErrors}; it still throws. */
function guarded<T>(handler: (value: T) => unknown): (value: T) => unknown {
  return (value) => {
    let result: unknown;
    try {
      result = handler(value);
    } catch (e) {
      listenerErrors.push(e);
      throw e;
    }
    if (result instanceof Promise) {
      return result.catch((e: unknown) => {
        listenerErrors.push(e);
        throw e;
      });
    }
    return result;
  };
}

/** Replies to every request on [channel] with [respond]. */
function serve(channel: TalkChannel, respond: (message: TalkMessage) => unknown): void {
  void channel.serve(guarded(respond));
}

/**
 * Takes every value of [iterable] as it comes, as a Dart stream listener;
 * an error ends it, passed to [onError] (else recorded as a failure).
 */
function listen<T>(
  iterable: AsyncIterable<T>,
  onValue: (value: T) => unknown,
  onError?: (error: unknown) => void,
): void {
  const handle = guarded(onValue);
  void (async () => {
    try {
      for await (const value of iterable) {
        try {
          handle(value);
        } catch {
          // Recorded.
        }
      }
    } catch (e) {
      if (onError === undefined) {
        listenerErrors.push(e);
      } else {
        onError(e);
      }
    }
  })();
}

// ---------------------------------------------------------------------------
// Helpers

/** The single byte of [payload]. */
function single(payload: Uint8Array): number {
  expect(payload).toHaveLength(1);
  return payload[0] as number;
}

/** Values that arrive one by one, taken in order, as a Dart `StreamQueue` over a controller. */
class Events<T> {
  readonly #values: T[] = [];
  #waiter: Deferred<T> | undefined;

  add(value: T): void {
    const waiter = this.#waiter;
    if (waiter !== undefined) {
      this.#waiter = undefined;
      waiter.resolve(value);
      return;
    }
    this.#values.push(value);
  }

  next(): Promise<T> {
    if (this.#values.length > 0) {
      return Promise.resolve(this.#values.shift() as T);
    }
    this.#waiter = new Deferred<T>();
    return this.#waiter.promise;
  }
}

/** Reads the frames [source] carries into a list, calling [onFrame] for each as it arrives. */
function record(
  source: { readonly readable: ReadableStream<Uint8Array> },
  onFrame?: (frame: TalkFrame) => void,
): TalkFrame[] {
  const frames: TalkFrame[] = [];
  void (async () => {
    const reader = source.readable.getReader();
    try {
      for (;;) {
        const { value, done } = await reader.read();
        if (done) {
          return;
        }
        const frame = TalkFrame.decode(value);
        frames.push(frame);
        onFrame?.(frame);
      }
    } catch {
      // The link failed.
    }
  })();
  return frames;
}

// ---------------------------------------------------------------------------
// The harness

/**
 * An intermediary that forwards every message between [front] (toward the
 * client) and [back] (toward the backend), both ways, from the handlers of
 * `serve` (so that it takes over the credit of what it forwards). Dart's
 * `Proxy`, renamed: the name is a global here.
 */
class Intermediary {
  readonly front: TalkChannel;
  readonly back: TalkChannel;
  readonly forwards: Promise<void>[] = [];

  constructor(front: TalkChannel, back: TalkChannel) {
    this.front = front;
    this.back = back;
    void front.serve((m) => {
      this.forwards.push(forwardMessage(m, back));
    });
    void back.serve((m) => {
      this.forwards.push(forwardMessage(m, front));
    });
  }

  async close(): Promise<void> {
    await Promise.all([this.front.close(), this.back.close()]);
  }
}

interface ChainOptions {
  readonly proxies?: number;
  readonly client?: Partial<TalkOptions>;
  readonly proxy?: Partial<TalkOptions>;
  readonly backend?: Partial<TalkOptions>;
  /** The clock of every channel. */
  readonly clock?: Clock;
}

/** client ↔ proxy ↔ … ↔ proxy ↔ backend over frame links. */
class Chain {
  readonly client: TalkChannel;
  readonly backend: TalkChannel;
  readonly proxies: Intermediary[] = [];

  constructor(options: ChainOptions = {}) {
    const clock = options.clock;
    const withClock = (o: Partial<TalkOptions> | undefined): Partial<TalkOptions> =>
      clock === undefined ? { ...o } : { ...o, clock };
    const [local, foreign] = frameLink();
    this.client = new TalkChannel(local, withClock(options.client));
    let toward = foreign;
    for (let i = 0; i < (options.proxies ?? 1); i++) {
      const [linkLocal, linkForeign] = frameLink();
      this.proxies.push(
        new Intermediary(
          new TalkChannel(toward, withClock(options.proxy)),
          new TalkChannel(linkLocal, withClock(options.proxy)),
        ),
      );
      toward = linkForeign;
    }
    this.backend = new TalkChannel(toward, withClock(options.backend));
  }

  get proxy(): Intermediary {
    return this.proxies[0] as Intermediary;
  }

  get channels(): TalkChannel[] {
    return [this.client, ...this.proxies.flatMap((p) => [p.front, p.back]), this.backend];
  }

  /** Waits for every forwarded exchange to end. */
  async settle(): Promise<void> {
    await pumpEventQueue();
    for (const p of this.proxies) {
      await Promise.all(p.forwards);
    }
  }

  /** No request is outstanding anywhere. */
  expectIdle(): void {
    for (const channel of this.channels) {
      expect(channel.outgoingRequestCount).toBe(0);
      expect(channel.incomingRequestCount).toBe(0);
    }
  }

  async close(): Promise<void> {
    await this.client.close();
    for (const p of this.proxies) {
      await p.close();
    }
    await this.backend.close();
  }
}

/** A proxy between two hand driven peers, for frame level checks. */
class RawProxy {
  readonly proxy: Intermediary;
  readonly client: FrameEnd;
  readonly backend: FrameEnd;
  readonly atClient: TalkFrame[];
  readonly atBackend: TalkFrame[];

  constructor() {
    const [toClientLocal, toClientForeign] = frameLink();
    const [toBackendLocal, toBackendForeign] = frameLink();
    this.proxy = new Intermediary(new TalkChannel(toClientLocal), new TalkChannel(toBackendLocal));
    this.client = toClientForeign;
    this.backend = toBackendForeign;
    this.atClient = record(toClientForeign);
    this.atBackend = record(toBackendForeign);
  }

  fromClient(frame: TalkFrame): void {
    this.client.add(frame.encode());
  }

  fromBackend(frame: TalkFrame): void {
    this.backend.add(frame.encode());
  }
}

// ---------------------------------------------------------------------------

for (const hops of [1, 2]) {
  const label = `${hops} ${hops === 1 ? 'proxy' : 'proxies'}`;
  describe(label, () => {
    let chain: Chain;
    beforeEach(() => {
      chain = new Chain({ proxies: hops });
    });
    afterEach(() => chain.close());

    test('plain messages both ways', async () => {
      const atBackend = new Queue(chain.backend.messages);
      const atClient = new Queue(chain.client.messages);
      void chain.client.send('HELLO', bytes([1]));
      void chain.backend.send('BACK', bytes([2]));
      const m1 = await atBackend.next();
      expect(m1.procedureName).toBe('HELLO');
      expect([...m1.payload]).toEqual([1]);
      expect(m1.expectsReply).toBe(false);
      const m2 = await atClient.next();
      expect(m2.procedureName).toBe('BACK');
      expect([...m2.payload]).toEqual([2]);
      await atBackend.cancel();
      await atClient.cancel();
    });

    test('request and reply, with and without procedure', async () => {
      serve(chain.backend, (m) => {
        if (m.procedureName === 'NAMED') {
          void m.reply(Uint8Array.from(m.payload).reverse(), { procedure: 'PONG' });
        } else {
          void m.reply(bytes([9]));
        }
      });
      const named = await chain.client.request('NAMED', bytes([1, 2]));
      expect(named.procedureName).toBe('PONG');
      expect([...named.payload]).toEqual([2, 1]);
      const plain = await chain.client.request('PLAIN', empty);
      expect(plain.frame.procedure, 'absent stays absent').toBeUndefined();
      expect([...plain.payload]).toEqual([9]);
      await chain.settle();
      chain.expectIdle();
    });

    test('requests from the backend reach the client', async () => {
      serve(chain.client, (m) => m.reply(bytes([single(m.payload) + 1])));
      const r = await chain.backend.request('UP', bytes([41]));
      expect([...r.payload]).toEqual([42]);
      await chain.settle();
      chain.expectIdle();
    });

    test('stream items and a final with trailing payload', async () => {
      serve(chain.backend, (m) => {
        expect(m.expectsStream).toBe(true);
        void m.replyItem(bytes([1]), { procedure: 'ITEM' });
        void m.replyItem(bytes([2]));
        void m.replyItem(bytes([3]));
        void m.reply(bytes([0xff]), { procedure: 'END' });
      });
      const s = chain.client.streamRequest('LIST', empty);
      const items = await toList(s.items);
      expect(items.map((m) => single(m.payload))).toEqual([1, 2, 3]);
      expect(items[0]?.procedureName).toBe('ITEM');
      expect(items[1]?.frame.procedure).toBeUndefined();
      const end = await s.done;
      expect([...end.payload]).toEqual([0xff]);
      expect(end.procedureName).toBe('END');
      await chain.settle();
      chain.expectIdle();
    });

    test('chained replies several hops deep', async () => {
      serve(chain.backend, async (m1) => {
        expect(m1.procedureName).toBe('Q1');
        const r2 = await m1.replyRequest(bytes([2]), { procedure: 'Q2' });
        expect(r2.procedureName).toBe('Q3');
        expect(r2.expectsReply).toBe(true);
        const r4 = await r2.replyRequest(bytes([4]));
        expect([...r4.payload]).toEqual([5]);
        const back = r4.replyStreamRequest(bytes([6]), { procedure: 'S' });
        const items = await toList(back.items);
        expect(items.map((i) => single(i.payload))).toEqual([7, 8]);
        const end = await back.done;
        expect([...end.payload]).toEqual([9]);
        expect(end.expectsReply).toBe(false);
      });
      const r1 = await chain.client.request('Q1', bytes([1]));
      expect([...r1.payload]).toEqual([2]);
      expect(r1.procedureName).toBe('Q2');
      expect(r1.expectsReply).toBe(true);
      const r3 = await r1.replyRequest(bytes([3]), { procedure: 'Q3' });
      expect([...r3.payload]).toEqual([4]);
      expect(r3.frame.procedure).toBeUndefined();
      const r5 = await r3.replyRequest(bytes([5]));
      expect([...r5.payload]).toEqual([6]);
      expect(r5.procedureName).toBe('S');
      expect(r5.expectsStream).toBe(true);
      void r5.replyItem(bytes([7]));
      void r5.replyItem(bytes([8]));
      void r5.reply(bytes([9]));
      await chain.settle();
      chain.expectIdle();
    });

    test('item requests and item stream requests', async () => {
      serve(chain.backend, async (m) => {
        const answer = m.replyItemRequest(bytes([1]), { procedure: 'MORE' });
        const sub = m.replyItemStreamRequest(bytes([2]), { procedure: 'SUB' });
        const subItems = (await toList(sub.items)).map((i) => single(i.payload));
        const subEnd = await sub.done;
        const a = await answer;
        void m.reply(bytes([single(a.payload), ...subItems, subEnd.payload[0] as number]));
      });
      const s = chain.client.streamRequest('ASK', empty);
      listen(s.items, (item) => {
        expect(item.expectsReply).toBe(true);
        if (item.expectsStream) {
          expect(item.procedureName).toBe('SUB');
          void item.replyItem(bytes([7]));
          void item.replyItem(bytes([8]));
          void item.reply(bytes([9]));
        } else {
          expect(item.procedureName).toBe('MORE');
          void item.reply(bytes([single(item.payload) * 10]));
        }
      });
      expect([...(await s.done).payload]).toEqual([10, 7, 8, 9]);
      await chain.settle();
      chain.expectIdle();
    });

    test('an abort from the backend passes through', async () => {
      serve(chain.backend, (m) => {
        if (m.procedureName === 'APP') {
          void m.replyAbort(new Status(300, 'app'));
        } else {
          void m.replyItem(bytes([1]));
          void m.replyAbort(Status.of(StatusCode.notFound, 'gone'));
        }
      });
      const app = await rejection(chain.client.request('APP', empty));
      expect(app).toBeInstanceOf(TalkAbortError);
      expectSame((app as TalkAbortError).status, new Status(300, 'app'));
      const s = chain.client.streamRequest('S', empty);
      const got: number[] = [];
      listen(
        s.items,
        (i) => got.push(single(i.payload)),
        () => {},
      );
      const gone = await rejection(s.done);
      expect(gone).toBeInstanceOf(TalkAbortError);
      expectSame((gone as TalkAbortError).status, Status.of(StatusCode.notFound, 'gone'));
      expect(got).toEqual([1]);
      await chain.settle();
      chain.expectIdle();
    });

    test('a client cancel reaches the backend', async () => {
      const cancels: string[] = [];
      const events = new Events<string>();
      serve(chain.backend, (m) => {
        if (m.expectsStream) {
          void m.replyItem(bytes([1]));
        }
        void m.onCancel.then(() => {
          cancels.push(m.procedureName);
          events.add(m.procedureName);
        });
      });
      const s = chain.client.streamRequest('WATCH', empty);
      for await (const _item of s.items) {
        break;
      }
      expect(await events.next()).toBe('WATCH');
      const r = chain.client.startRequest('ONE', empty);
      await pumpEventQueue();
      r.cancel();
      expect(await events.next()).toBe('ONE');
      await rejectsStatus(r.response, StatusCode.cancelled);
      await chain.settle();
      chain.expectIdle();
    });

    test('losing the client channel cancels the forwarded request', async () => {
      const cancelled = new Deferred<void>();
      serve(chain.backend, (m) => {
        void m.onCancel.then(() => cancelled.resolve());
      });
      const pending = chain.client.request('X', empty);
      await pumpEventQueue();
      expect(chain.backend.incomingRequestCount).toBe(1);
      await chain.client.close();
      await rejectsStatus(pending, StatusCode.cancelled);
      await cancelled.promise;
      await chain.settle();
      chain.expectIdle();
    });

    test('losing the backend channel answers UNAVAILABLE', async () => {
      const held: TalkMessage[] = [];
      serve(chain.backend, (m) => {
        held.push(m);
      });
      const pending = rejectsRemote(chain.client.request('X', empty), StatusCode.unavailable);
      const s = chain.client.streamRequest('S', empty);
      const items = rejectsRemote(toList(s.items), StatusCode.unavailable);
      await pumpEventQueue();
      expect(held).toHaveLength(2);
      await chain.backend.close();
      await pending;
      await items;
      // The proxy cannot forward any more.
      await pumpEventQueue();
      await rejectsRemote(chain.client.request('Y', empty), StatusCode.unavailable);
      await chain.settle();
    });

    test('a channel abort from the backend answers UNAVAILABLE', async () => {
      serve(chain.backend, () => {});
      const pending = chain.client.request('X', empty);
      await pumpEventQueue();
      chain.backend.abort(Status.of(StatusCode.unauthenticated));
      await rejectsRemote(pending, StatusCode.unavailable);
      expect(chain.client.isOpen).toBe(true);
      await chain.settle();
    });
  });
}

// Not ported: '1 proxy, synchronous transport' and '2 proxies, synchronous
// transport' (the eleven tests above, each): the Dart file runs them again
// over synchronous stream channel controllers, which deliver a frame to the
// peer while its sender is still in `add`. A Talk channel here reads its
// raw channel through a `ReadableStream`, which always delivers in a later
// microtask, so there is no synchronous transport to run them over.

describe('timeouts', () => {
  test("a deadline the backend declares reaches the client with the backend's buffer only, through every hop", async () => {
    const clock = new ManualClock();
    const chain = new Chain({ proxies: 2, clock });
    const held: TalkMessage[] = [];
    serve(chain.backend, (m) => {
      held.push(m);
    });
    const seen: [number | undefined, number | undefined][] = [];
    const r = chain.client.startRequest('SLOW', empty, {
      onExtend: ({ deadline, renew }) => seen.push([deadline, renew]),
    });
    const result = new Outcome(r.response);
    await flush();
    const t0 = clock.now();
    void only(held).extend({ deadline: 60_000, renew: 20_000 });
    await flush();
    expect(seen).toEqual([[65_000, 25_000]]);
    expect(r.deadline).toBe(t0 + 65_000);
    // The backend gives up at its own deadline; its abort passes the
    // proxies, which time nothing out themselves.
    await clock.elapseAsync(60_000);
    expectRemote(result.error, StatusCode.deadlineExceeded);
    await chain.close();
    await flush();
    expect(clock.pendingCount).toBe(0);
  });

  test('EXTEND from the backend keeps the client waiting', async () => {
    const clock = new ManualClock();
    const chain = new Chain({
      proxies: 2,
      clock,
      client: { requestTimeout: 30 },
      proxy: { requestTimeout: 30, replyTimeout: 30 },
      backend: { replyTimeout: 30 },
    });
    const held: TalkMessage[] = [];
    serve(chain.backend, (m) => {
      held.push(m);
    });
    let extensions = 0;
    const r = chain.client.startRequest('SLOW', empty, {
      onExtend: () => {
        extensions++;
      },
    });
    const result = new Outcome(r.response);
    const s = chain.client.streamRequest('TICK', empty);
    const items = new Outcome(toList(s.items));
    await flush();
    expect(held).toHaveLength(2);
    const [slow, tick] = held as [TalkMessage, TalkMessage];
    for (let i = 0; i < 5; i++) {
      await clock.elapseAsync(20);
      void slow.extend();
      void tick.replyItem(bytes([i]));
    }
    await clock.elapseAsync(20);
    expect(result.isDone).toBe(false);
    expect(items.isDone).toBe(false);
    expect(extensions).toBe(5);
    void slow.reply(bytes([1]));
    void tick.reply(empty);
    await flush();
    expect([...(result.value as TalkMessage).payload]).toEqual([1]);
    expect((items.value as TalkMessage[]).map((i) => single(i.payload))).toEqual([0, 1, 2, 3, 4]);
    await chain.close();
    await flush();
    expect(clock.pendingCount).toBe(0);
  });

  test('the proxy imposes no timeout of its own', async () => {
    const clock = new ManualClock();
    const chain = new Chain({
      clock,
      client: { requestTimeout: 1000 },
      proxy: { requestTimeout: 10, replyTimeout: 10 },
      backend: { replyTimeout: 0 },
    });
    let held: TalkMessage | undefined;
    serve(chain.backend, (m) => {
      held = m;
    });
    const result = new Outcome(chain.client.request('SLOW', empty));
    await clock.elapseAsync(500);
    expect(result.isDone).toBe(false);
    expect(clock.pendingCount, 'the client only').toBe(1);
    void (held as TalkMessage).reply(bytes([1]));
    await flush();
    expect([...(result.value as TalkMessage).payload]).toEqual([1]);
    await chain.close();
    await flush();
    expect(clock.pendingCount).toBe(0);
  });

  test('the backend responder timeout reaches the client as DEADLINE_EXCEEDED', async () => {
    const clock = new ManualClock();
    const chain = new Chain({ clock, backend: { replyTimeout: 30 } });
    let held: TalkMessage | undefined;
    serve(chain.backend, (m) => {
      held = m;
    });
    const result = new Outcome(chain.client.request('SLOW', empty));
    await clock.elapseAsync(31);
    expectRemote(result.error, StatusCode.deadlineExceeded);
    expect((held as TalkMessage).isCancelled).toBe(true);
    chain.expectIdle();
    await chain.close();
    await flush();
    expect(clock.pendingCount).toBe(0);
  });

  test('the client timeout cancels the forwarded request', async () => {
    const clock = new ManualClock();
    const chain = new Chain({
      clock,
      client: { requestTimeout: 30 },
      backend: { replyTimeout: 0 },
    });
    let held: TalkMessage | undefined;
    serve(chain.backend, (m) => {
      held = m;
    });
    const result = new Outcome(chain.client.request('SLOW', empty));
    await clock.elapseAsync(31);
    expectStatus(result.error, StatusCode.deadlineExceeded);
    expect(result.error).not.toBeInstanceOf(TalkAbortError);
    expect((held as TalkMessage).isCancelled).toBe(true);
    chain.expectIdle();
    await chain.close();
    await flush();
    expect(clock.pendingCount).toBe(0);
  });
});

describe('frame level', () => {
  const odd = Name.fromBytes([0xff, 0x41, 0, 0, 0, 0, 0, 0]);
  const odd2 = Name.fromBytes([0xc3, 0x28, 0x80, 0, 0, 0, 0, 0]);

  test('an undeliverable synchronous item cancels the forwarded request', async () => {
    // Adapted: there is no synchronous transport, so the backend's item
    // cannot arrive while the proxy is still sending the request (before
    // it has the handle); it arrives after, through the same relay. The
    // proxy's channel to the client is a mux channel whose carrier refuses
    // the item FRAME_TOO_LARGE when sent, as `_LimitedSink` does: only a
    // mux channel (or one carrying it) refuses a frame when sending.
    const clock = new ManualClock();
    const [left, right] = memoryMuxPair({ mux: { clock } });
    const toClient = left.open(empty);
    const client = await right.accept();
    const [toBackendLocal, toBackendForeign] = frameLink();
    const proxy = new Intermediary(
      new TalkChannel(new Carrier(toClient, 64)),
      new TalkChannel(toBackendLocal, { clock }),
    );
    const atClient = record(client);
    const atBackend = record(toBackendForeign, (frame) => {
      if (frame.kind === TalkKind.message) {
        toBackendForeign.add(
          new TalkFrame({
            kind: TalkKind.streamItem,
            responseId: frame.requestId,
            payload: new Uint8Array(100),
          }).encode(),
        );
      } else if (frame.kind === TalkKind.abort && frame.hasRequest) {
        toBackendForeign.add(
          new TalkFrame({
            kind: TalkKind.abort,
            responseId: frame.requestId,
            payload: Status.of(StatusCode.cancelled).encode(),
          }).encode(),
        );
      }
    });
    void client.send(
      new TalkFrame({
        kind: TalkKind.message,
        procedure: Name.of('LIST'),
        requestId: 7,
        stream: true,
      }).encode(),
    );
    await pumpEventQueue();
    expect(only(atClient).status.known).toBe(StatusCode.unavailable);
    expect(atBackend.map((f) => f.kind)).toEqual([TalkKind.message, TalkKind.abort]);
    expect(proxy.front.incomingRequestCount).toBe(0);
    expect(proxy.back.outgoingRequestCount).toBe(0);
    const forwarded = new Outcome(only(proxy.forwards));
    await flush();
    expect(forwarded.isDone).toBe(true);
    await proxy.close();
    await flush();
    expect(clock.pendingCount).toBe(0);
    await left.close();
    await right.close();
  });

  test('a listener that throws after forwarding cancels the forwarded request', async () => {
    const [toClientLocal, toClientForeign] = frameLink();
    const [toBackendLocal, toBackendForeign] = frameLink();
    const front = new TalkChannel(toClientLocal);
    const back = new TalkChannel(toBackendLocal);
    const forwards: Promise<void>[] = [];
    void front.serve((m) => {
      forwards.push(forwardMessage(m, back));
      throw new Error('handler bug');
    });
    const atClient = record(toClientForeign);
    const atBackend = record(toBackendForeign, (frame) => {
      if (frame.kind === TalkKind.abort && frame.hasRequest) {
        toBackendForeign.add(
          new TalkFrame({
            kind: TalkKind.abort,
            responseId: frame.requestId,
            payload: Status.of(StatusCode.cancelled).encode(),
          }).encode(),
        );
      }
    });
    toClientForeign.add(
      new TalkFrame({
        kind: TalkKind.message,
        procedure: Name.of('LIST'),
        requestId: 7,
        stream: true,
      }).encode(),
    );
    await pumpEventQueue();
    const forwarded = new Outcome(only(forwards));
    await pumpEventQueue();
    expect(forwarded.isDone, 'forwarding ended').toBe(true);
    expect(only(atClient).kind).toBe(TalkKind.abort);
    expect(only(atClient).responseId).toBe(7);
    expect(only(atClient).status.known).toBe(StatusCode.internal);
    expect(
      atBackend.map((f) => [f.kind, f.requestId]),
      'the backend is told to stop',
    ).toEqual([
      [TalkKind.message, 1],
      [TalkKind.abort, 1],
    ]);
    expect(front.incomingRequestCount).toBe(0);
    expect(back.outgoingRequestCount).toBe(0);
    await Promise.all([front.close(), back.close()]);
  });

  test('non-UTF-8 procedure names pass through byte for byte', async () => {
    const p = new RawProxy();
    p.fromClient(new TalkFrame({ kind: TalkKind.message, procedure: odd, payload: bytes([1]) }));
    p.fromClient(
      new TalkFrame({
        kind: TalkKind.message,
        procedure: odd,
        requestId: 5,
        stream: true,
        payload: bytes([2]),
      }),
    );
    await pumpEventQueue();
    expect(p.atBackend).toHaveLength(2);
    const plain = p.atBackend[0] as TalkFrame;
    expect(plain.procedure?.equals(odd)).toBe(true);
    expect(hexString((plain.procedure as Name).bytes).startsWith('FF 41')).toBe(true);
    expect([...plain.payload]).toEqual([1]);
    const forwarded = p.atBackend[1] as TalkFrame;
    expect(forwarded.procedure?.equals(odd)).toBe(true);
    expect(forwarded.stream).toBe(true);
    expect([...forwarded.payload]).toEqual([2]);
    const id = forwarded.requestId;
    p.fromBackend(
      new TalkFrame({
        kind: TalkKind.streamItem,
        procedure: odd2,
        responseId: id,
        payload: bytes([3]),
      }),
    );
    p.fromBackend(new TalkFrame({ kind: TalkKind.extend, responseId: id }));
    p.fromBackend(
      new TalkFrame({
        kind: TalkKind.message,
        procedure: odd2,
        responseId: id,
        payload: bytes([4]),
      }),
    );
    await pumpEventQueue();
    expect(p.atClient.map((f) => hexString(f.encode()))).toEqual(
      [
        new TalkFrame({
          kind: TalkKind.streamItem,
          procedure: odd2,
          responseId: 5,
          payload: bytes([3]),
        }),
        new TalkFrame({ kind: TalkKind.extend, responseId: 5 }),
        new TalkFrame({
          kind: TalkKind.message,
          procedure: odd2,
          responseId: 5,
          payload: bytes([4]),
        }),
      ].map((f) => hexString(f.encode())),
    );
    await p.proxy.close();
  });

  test('EXTEND payloads pass through byte for byte', async () => {
    const p = new RawProxy();
    p.fromClient(new TalkFrame({ kind: TalkKind.message, procedure: odd, requestId: 5 }));
    await pumpEventQueue();
    const id = only(p.atBackend).requestId;
    const payloads = [
      hexBytes('A0 BB 0D 00 E0 93 04 00'),
      hexBytes('A0 BB 0D 00 00 00 00 00'),
      hexBytes('00 00 00 00 00 00 00 00'),
      new Uint8Array(0),
      hexBytes('01 00 00 00 FF FF FF FF'),
    ];
    for (const payload of payloads) {
      p.backend.add(new TalkFrame({ kind: TalkKind.extend, responseId: id, payload }).encode());
    }
    await pumpEventQueue();
    expect(p.atClient.map((f) => [f.kind, f.responseId])).toEqual(
      payloads.map(() => [TalkKind.extend, 5]),
    );
    expect(p.atClient.map((f) => hexString(f.payload))).toEqual(
      payloads.map((payload) => hexString(payload)),
    );
    await p.proxy.close();
  });

  for (const [name, code] of [
    ['moved', StatusCode.moved],
    ['relocated', StatusCode.relocated],
  ] as const) {
    test(`${name} abort fields and reason pass through byte for byte`, async () => {
      const p = new RawProxy();
      p.fromClient(
        new TalkFrame({ kind: TalkKind.message, procedure: Name.of('Q'), requestId: 7 }),
      );
      await pumpEventQueue();
      // The owner bytes and malformed UTF-8 reason must survive status
      // decoding. Rebuilding the status from its reason would corrupt it.
      const payload = bytes([
        code,
        0,
        0xff,
        0xee,
        0xdd,
        0xcc,
        0xbb,
        0xaa,
        0x78,
        0x56,
        0x34,
        0x12,
        0xff,
        0x80,
      ]);
      p.fromBackend(
        new TalkFrame({
          kind: TalkKind.abort,
          responseId: only(p.atBackend).requestId,
          payload,
        }),
      );
      await pumpEventQueue();
      await Promise.all(p.proxy.forwards);
      await pumpEventQueue();
      const response = only(p.atClient);
      expect(response.kind).toBe(TalkKind.abort);
      expect(response.responseId).toBe(7);
      expect(hexString(response.payload)).toBe(hexString(payload));
      expect(p.atBackend, 'neither status is retried').toHaveLength(1);
      expect(p.proxy.front.incomingRequestCount).toBe(0);
      expect(p.proxy.back.outgoingRequestCount).toBe(0);
      await p.proxy.close();
    });
  }

  test('a chained request from the backend gets its own id toward the client', async () => {
    const p = new RawProxy();
    p.fromClient(new TalkFrame({ kind: TalkKind.message, procedure: odd, requestId: 9 }));
    await pumpEventQueue();
    const r = only(p.atBackend).requestId;
    p.fromBackend(
      new TalkFrame({
        kind: TalkKind.message,
        procedure: odd2,
        requestId: 0x123456,
        responseId: r,
      }),
    );
    await pumpEventQueue();
    const chained = only(p.atClient);
    expect(chained.responseId).toBe(9);
    expect(chained.procedure?.equals(odd2)).toBe(true);
    expect(chained.hasRequest).toBe(true);
    p.fromClient(
      new TalkFrame({
        kind: TalkKind.abort,
        responseId: chained.requestId,
        payload: new Status(999, 'no').encode(),
      }),
    );
    await pumpEventQueue();
    const answer = last(p.atBackend);
    expect(answer.kind).toBe(TalkKind.abort);
    expect(answer.responseId).toBe(0x123456);
    expectSame(answer.status, new Status(999, 'no'));
    await Promise.all(p.proxy.forwards);
    await p.proxy.close();
  });

  test('a client cancel is forwarded with its status', async () => {
    const p = new RawProxy();
    p.fromClient(new TalkFrame({ kind: TalkKind.message, procedure: odd, requestId: 3 }));
    await pumpEventQueue();
    const r = only(p.atBackend).requestId;
    p.fromClient(
      new TalkFrame({
        kind: TalkKind.abort,
        requestId: 3,
        payload: new Status(1, 'stop').encode(),
      }),
    );
    await pumpEventQueue();
    expect(only(p.atClient).responseId).toBe(3);
    expect(only(p.atClient).status.known).toBe(StatusCode.cancelled);
    const cancel = last(p.atBackend);
    expect(cancel.requestId).toBe(r);
    expect(cancel.hasResponse).toBe(false);
    expectSame(cancel.status, new Status(1, 'stop'));
    expect(p.proxy.back.outgoingRequestCount, 'until the final').toBe(1);
    p.fromBackend(
      new TalkFrame({
        kind: TalkKind.abort,
        responseId: r,
        payload: Status.of(StatusCode.cancelled).encode(),
      }),
    );
    await pumpEventQueue();
    expect(p.proxy.back.outgoingRequestCount).toBe(0);
    expect(p.atClient, 'nothing more for the client').toHaveLength(1);
    await Promise.all(p.proxy.forwards);
    await p.proxy.close();
  });

  test('a chained request refused by the proxy limit answers the client UNAVAILABLE', async () => {
    const [toClientLocal, toClientForeign] = frameLink();
    const [toBackendLocal, toBackendForeign] = frameLink();
    const proxy = new Intermediary(
      new TalkChannel(toClientLocal),
      new TalkChannel(toBackendLocal, { maxIncomingRequests: 1 }),
    );
    const atClient = record(toClientForeign);
    const atBackend = record(toBackendForeign);
    // The backend holds one request toward the client: the limit is used.
    toBackendForeign.add(
      new TalkFrame({ kind: TalkKind.message, procedure: Name.of('HOLD'), requestId: 1 }).encode(),
    );
    toClientForeign.add(
      new TalkFrame({ kind: TalkKind.message, procedure: Name.of('ASK'), requestId: 7 }).encode(),
    );
    await pumpEventQueue();
    const ask = only(atBackend);
    toBackendForeign.add(
      new TalkFrame({ kind: TalkKind.message, requestId: 2, responseId: ask.requestId }).encode(),
    );
    await pumpEventQueue();
    expect(last(atBackend).responseId).toBe(2);
    expect(last(atBackend).status.known).toBe(StatusCode.resourceExhausted);
    const answer = atClient.find((f) => f.responseId === 7);
    expect(answer?.kind).toBe(TalkKind.abort);
    expect(answer?.status.known).toBe(StatusCode.unavailable);
    await proxy.close();
  });

  test('a chained request the proxy cannot send toward the client answers both sides UNAVAILABLE', async () => {
    const [toClientLocal, toClientForeign] = frameLink();
    const [toBackendLocal, toBackendForeign] = frameLink();
    const proxy = new Intermediary(
      new TalkChannel(toClientLocal, { maxOutgoingRequests: 1 }),
      new TalkChannel(toBackendLocal),
    );
    const atClient = record(toClientForeign);
    const atBackend = record(toBackendForeign);
    // A request of the backend's, forwarded to the client, holds the one
    // outgoing request the proxy may have toward the client.
    toBackendForeign.add(
      new TalkFrame({ kind: TalkKind.message, procedure: Name.of('HOLD'), requestId: 1 }).encode(),
    );
    toClientForeign.add(
      new TalkFrame({ kind: TalkKind.message, procedure: Name.of('ASK'), requestId: 7 }).encode(),
    );
    await pumpEventQueue();
    expect(proxy.front.outgoingRequestCount).toBe(1);
    const ask = only(atBackend);
    toBackendForeign.add(
      new TalkFrame({ kind: TalkKind.message, requestId: 2, responseId: ask.requestId }).encode(),
    );
    await pumpEventQueue();
    // The chained request is refused toward the backend, and the request
    // it was to answer still gets its final.
    expect(last(atBackend).responseId).toBe(2);
    expect(last(atBackend).status.known).toBe(StatusCode.unavailable);
    const answer = only(atClient.filter((f) => f.responseId === 7));
    expect(answer.kind).toBe(TalkKind.abort);
    expect(answer.status.known).toBe(StatusCode.unavailable);
    expect(proxy.front.incomingRequestCount).toBe(0);
    await proxy.close();
  });
});

describe('forwardMessage', () => {
  test('refuses foreign messages and double forwarding', async () => {
    const chain = new Chain({ proxies: 0 });
    // Dart's `ArgumentError` is a TypeError here.
    expect(() => forwardMessage({} as TalkMessage, chain.client)).toThrow(TypeError);
    const received = new Deferred<TalkMessage>();
    const other = new Chain({ proxies: 0 });
    serve(other.backend, (m) => m.reply(empty));
    serve(chain.backend, (m) => {
      received.resolve(m);
    });
    const pending = chain.client.request('X', empty);
    const m = await received.promise;
    const forwarded = forwardMessage(m, other.client);
    // Dart's `StateError` is an Error here.
    expect(() => forwardMessage(m, other.client)).toThrow(/already forwarded/);
    await forwarded;
    await pending;
    await chain.close();
    await other.close();
  });

  test('completes once nested exchanges have ended', async () => {
    const chain = new Chain();
    serve(chain.backend, async (m) => {
      const r = await m.replyRequest(bytes([1]));
      void r.reply(bytes([single(r.payload) + 1]));
    });
    const r1 = await chain.client.request('Q', empty);
    await pumpEventQueue();
    let done = false;
    void only(chain.proxy.forwards).then(() => {
      done = true;
    });
    await pumpEventQueue();
    expect(done, 'the chained request is outstanding').toBe(false);
    const r3 = await r1.replyRequest(bytes([5]));
    expect([...r3.payload]).toEqual([6]);
    await pumpEventQueue();
    expect(done).toBe(true);
    chain.expectIdle();
    await chain.close();
  });

  test('does nothing for a request that can no longer be answered', async () => {
    const chain = new Chain({ proxies: 0 });
    const other = new Chain({ proxies: 0 });
    const received = new Deferred<TalkMessage>();
    serve(chain.backend, (m) => {
      received.resolve(m);
    });
    const pending = chain.client.request('X', empty);
    const m = await received.promise;
    void m.reply(empty);
    await forwardMessage(m, other.client);
    await pending;
    expect(other.client.outgoingRequestCount).toBe(0);
    await chain.close();
    await other.close();
  });
});

describe('bulk payloads', () => {
  /** [n] bytes 0, 1, 2, ... modulo 251. */
  function pattern(n: number, offset = 0): Uint8Array {
    const out = new Uint8Array(n);
    for (let i = 0; i < n; i++) {
      out[i] = (offset + i) % 251;
    }
    return out;
  }

  /**
   * A mux connection whose `_bulk` channels are routed (every Talk channel
   * routes those of its connection): the near end of a Talk channel with
   * [near], and its far end with [far].
   */
  async function muxTalk(
    near: Partial<TalkOptions> = {},
    far: Partial<TalkOptions> = {},
  ): Promise<[TalkChannel, TalkChannel]> {
    const [left, right] = memoryMuxPair();
    onTestFinished(async () => {
      await left.close();
      await right.close();
    });
    const nearEnd = new TalkChannel(left.open(empty), near);
    return [nearEnd, new TalkChannel(await right.accept(), far)];
  }

  /**
   * client ↔ intermediary ↔ backend over mux connections; the intermediary
   * forwards every message both ways and streams bulk payloads unless
   * [reassemble]; [seen] gets what it forwarded toward the backend.
   */
  async function chain(
    options: { reassemble?: boolean; backendStreams?: boolean; seen?: TalkMessage[] } = {},
  ): Promise<[TalkChannel, TalkChannel]> {
    const proxyOptions: Partial<TalkOptions> =
      options.reassemble === true ? {} : { streamBulk: () => true };
    const [client, front] = await muxTalk({}, proxyOptions);
    const [back, backend] = await muxTalk(
      proxyOptions,
      options.backendStreams === true ? { streamBulk: () => true } : {},
    );
    void front.serve((m) => {
      options.seen?.push(m);
      void forwardMessage(m, back);
    });
    void back.serve((m) => void forwardMessage(m, front));
    return [client, backend];
  }

  /** A byte source fed by hand, as a Dart `StreamController`. */
  class Source {
    readonly readable: ReadableStream<Uint8Array>;
    #controller!: ReadableStreamDefaultController<Uint8Array>;
    /** Its reader stopped it, as a Dart controller loses its listener. */
    cancelled = false;

    constructor() {
      this.readable = new ReadableStream<Uint8Array>({
        start: (controller) => {
          this.#controller = controller;
        },
        cancel: () => {
          this.cancelled = true;
        },
      });
    }

    add(chunk: Uint8Array): void {
      this.#controller.enqueue(chunk);
    }

    close(): void {
      if (!this.cancelled) {
        this.#controller.close();
      }
    }
  }

  test('a request, its bulk response and bulk items are piped through, never reassembled by the intermediary', async () => {
    const seen: TalkMessage[] = [];
    const [client, backend] = await chain({ seen });
    serve(backend, (m) => {
      switch (m.procedureName) {
        case 'PUT':
          expect(bytesEqual(m.payload, pattern(300000))).toBe(true);
          void m.reply(bytes([Math.floor(m.payload.length / 10000)]));
          break;
        case 'GET':
          void m.reply(empty, { bulk: [pattern(150000)], bulkLength: 150000 });
          break;
        case 'LIST':
          for (let i = 0; i < 2; i++) {
            void m.replyItem(empty, { bulk: [pattern(60000 + i)] });
          }
          void m.reply(empty);
          break;
      }
    });
    const put = await client.request('PUT', empty, {
      bulk: [pattern(300000)],
      bulkLength: 300000,
    });
    expect([...put.payload]).toEqual([30]);
    expect(only(seen).isBulk).toBe(true);
    expect(only(seen).bulkLength).toBe(300000);
    // Streamed through, not reassembled. Dart's `StateError` is an Error.
    expect(() => only(seen).payload).toThrow(Error);
    const got = await client.request('GET', empty);
    expect(got.isBulk).toBe(true);
    expect(got.bulkLength).toBe(150000);
    expect(bytesEqual(got.payload, pattern(150000))).toBe(true);
    const list = client.streamRequest('LIST', empty);
    const items = await toList(list.items);
    expect(items.map((i) => i.payload.length)).toEqual([60000, 60001]);
    expect(items.every((i) => i.isBulk)).toBe(true);
    await list.done;
  });

  test('a plain bulk message, and forwarding a reassembled one', async () => {
    const [client, backend] = await chain({ reassemble: true });
    const plain = new Deferred<TalkMessage>();
    serve(backend, (m) => {
      if (m.expectsReply) {
        void m.reply(m.payload.subarray(0, 2));
      } else {
        plain.resolve(m);
      }
    });
    void client.send('NOTE', pattern(100000));
    const note = await plain.promise;
    expect(note.isBulk).toBe(true);
    expect(bytesEqual(note.payload, pattern(100000))).toBe(true);
    const answer = await client.request('PUT', pattern(100000, 7));
    expect([...answer.payload]).toEqual([7, 8]);
  });

  test('forwarding a plain message waits for its bulk payload', async () => {
    const [client, front] = await muxTalk({}, { streamBulk: () => true });
    const [back, backend] = await muxTalk();
    const source = new Source();
    let forwarded = false;
    void front.serve((message) => {
      void forwardMessage(message, back).then(() => {
        forwarded = true;
      });
    });
    const received = first(backend.messages);
    void client.send('NOTE', empty, { bulk: source.readable });
    await pumpEventQueue();
    expect(forwarded).toBe(false);
    source.add(pattern(100000));
    source.close();
    expect(bytesEqual((await received).payload, pattern(100000))).toBe(true);
    await pumpEventQueue();
    expect(forwarded).toBe(true);
  });

  test('forwarding stays active until the bulk reply has finished', async () => {
    const [client, front] = await muxTalk();
    const [back, backend] = await muxTalk();
    const source = new Source();
    let forwarded = false;
    void front.serve((message) => {
      void forwardMessage(message, back).then(() => {
        forwarded = true;
        // A forwarding owner may release its target when the exchange
        // ends, as the slot gate does after its idle delay.
        void back.close();
      });
    });
    serve(backend, (message) => message.reply(empty, { bulk: source.readable }));
    const response = client.request('GET', empty);
    await pumpEventQueue();
    expect(forwarded).toBe(false);
    source.add(pattern(100000));
    source.close();
    expect(bytesEqual((await response).payload, pattern(100000))).toBe(true);
    await pumpEventQueue();
    expect(forwarded).toBe(true);
  });

  test('a cancel stops the piped payload on both hops', async () => {
    const [client, backend] = await chain({ backendStreams: true });
    const failed = new Deferred<unknown>();
    const cancelled = new Deferred<void>();
    serve(backend, (m) => {
      void (async () => {
        const reader = m.bulk.getReader();
        try {
          for (;;) {
            const { done } = await reader.read();
            if (done) {
              return;
            }
          }
        } catch (e) {
          failed.resolve(e);
        }
      })();
      void m.onCancel.then(() => cancelled.resolve());
    });
    const source = new Source();
    source.add(pattern(1000));
    const pending = client.startRequest('PUT', empty, { bulk: source.readable });
    await pumpEventQueue();
    pending.cancel();
    await cancelled.promise;
    expectStatus(await failed.promise, StatusCode.cancelled);
    expect(source.cancelled).toBe(true);
    source.close();
  });
});

// Message chain forwarding passes flow control through (wiki page
// "Switchboard Proxying"): the credit of a forwarded frame goes back once
// the frame forwarded for it went out on the other channel, over a mux
// channel or a slot channel carrying one.
describe('forwarding credit', () => {
  /**
   * A mux connection and a channel on it: the opener's end (a slot channel
   * over it with [slot]) and the acceptor's end. Every Talk channel routes
   * the bulk channels of its connection.
   */
  async function link(slot = false): Promise<[MemoryMuxChannel | Carrier, MemoryMuxChannel]> {
    const [left, right] = memoryMuxPair();
    onTestFinished(async () => {
      await left.close();
      await right.close();
    });
    const opened = left.open(empty);
    const near = slot ? new Carrier(opened) : opened;
    return [near, await right.accept()];
  }

  /**
   * What the mux channel under [talk] holds for it, not consumed: at least
   * half a window once its sender is held back (credit goes back in
   * batches of half the window), nothing while it is not.
   */
  function heldOf(talk: TalkChannel): number {
    return (talk.raw as MemoryMuxChannel).heldBytes;
  }

  /**
   * client ↔ front, intermediary, back ↔ backend: the intermediary opens
   * both channels (a slot channel toward the client with [slotFront],
   * toward the backend with [slotBack]) and forwards what the client
   * sends, from the handler. The client's end is a raw mux channel nobody
   * reads yet.
   */
  async function relay(
    slotFront = false,
    slotBack = false,
  ): Promise<[MemoryMuxChannel, TalkChannel, TalkChannel, TalkChannel]> {
    const [frontRaw, client] = await link(slotFront);
    const [backRaw, backendRaw] = await link(slotBack);
    const front = new TalkChannel(frontRaw);
    const back = new TalkChannel(backRaw);
    const backend = new TalkChannel(backendRaw);
    void front.serve((m) => void forwardMessage(m, back));
    return [client, front, back, backend];
  }

  /** Sends request [id] on [client], a raw mux channel. */
  function request(client: MemoryMuxChannel, id: number, stream = false): void {
    void client.send(
      new TalkFrame({
        kind: TalkKind.message,
        procedure: Name.of('GET'),
        requestId: id,
        stream,
        payload: empty,
      }).encode(),
    );
  }

  /** Counts the subframes [client], a raw mux channel, receives from now on. */
  function countArrivals(client: MemoryMuxChannel): () => number {
    let answers = 0;
    void (async () => {
      const reader = client.readable.getReader();
      try {
        for (;;) {
          const { done } = await reader.read();
          if (done) {
            return;
          }
          answers++;
        }
      } catch {
        // The connection ended.
      }
    })();
    return () => answers;
  }

  for (const slot of [false, true]) {
    const over = slot ? 'over a slot channel' : 'over a mux channel';

    test(`forwarded items withhold credit while the requester does not read, ${over}`, async () => {
      const [frontRaw, clientRaw] = await link(slot);
      const [backRaw, backendRaw] = await link();
      const client = new TalkChannel(clientRaw);
      const back = new TalkChannel(backRaw);
      const backend = new TalkChannel(backendRaw);
      void new TalkChannel(frontRaw).serve((m) => void forwardMessage(m, back));
      serve(backend, (message) => {
        for (let i = 0; i < 300; i++) {
          void message.replyItem(new Uint8Array(1000));
        }
        void message.reply(empty);
      });
      const request = client.streamRequest('LIST', empty);
      let received = 0;
      const items = new Collector(request.items, {
        paused: true,
        onValue: () => {
          received++;
        },
      });
      await pumpEventQueue();
      // The backend is held back by the intermediary's window, which holds
      // what waits for the client's.
      expect(heldOf(back)).toBeGreaterThan(30000);
      expect((backend.raw as MemoryMuxChannel).sendWindow).toBeLessThan(costOf(1004));
      items.resume();
      await request.done;
      await items.finished;
      expect(received).toBe(300);
      await pumpEventQueue();
      expect(heldOf(back)).toBe(0);
      await items.cancel();
    });

    test(`a reply relayed behind items of the intermediary's own, that wait for the window, gives its credit back when it is dropped, ${over}`, async () => {
      const [frontRaw, clientRaw] = await link(slot);
      const [backRaw, backendRaw] = await link();
      const client = new TalkChannel(clientRaw);
      const back = new TalkChannel(backRaw);
      const backend = new TalkChannel(backendRaw);
      void new TalkChannel(frontRaw).serve((m) => {
        void m.replyItem(bytes([1]));
        void forwardMessage(m, back);
      });
      serve(backend, (m) => m.reply(new Uint8Array(1000)));
      const s = client.streamRequest('LIST', empty, { window: 0 });
      listen(
        s.items,
        () => {},
        () => {},
      );
      // The final waits behind the item, its frame's credit held.
      await until(() => heldOf(back) > 1000);
      s.cancel();
      await rejectsStatus(s.done, StatusCode.cancelled);
      await until(() => heldOf(back) === 0);
    });

    test(`forwarded final responses withhold credit, ${over}`, async () => {
      const [client, , back, backend] = await relay(slot);
      serve(backend, (m) => m.reply(new Uint8Array(1000)));
      for (let id = 1; id <= 200; id++) {
        request(client, id);
      }
      await pumpEventQueue();
      expect(heldOf(back)).toBeGreaterThan(30000);
      const answers = countArrivals(client);
      await until(() => answers() === 200);
      await pumpEventQueue();
      expect(heldOf(back)).toBe(0);
    });

    test(`forwarded chained requests withhold credit, ${over}`, async () => {
      const [client, , back, backend] = await relay(slot);
      serve(backend, (m) => {
        m.startReplyRequest(new Uint8Array(1000));
      });
      for (let id = 1; id <= 200; id++) {
        request(client, id);
      }
      await pumpEventQueue();
      expect(heldOf(back)).toBeGreaterThan(30000);
      const answers = countArrivals(client);
      await until(() => answers() === 200);
      await pumpEventQueue();
      expect(heldOf(back)).toBe(0);
    });

    test(`forwarded item requests withhold credit, ${over}`, async () => {
      const [client, , back, backend] = await relay(slot);
      serve(backend, (m) => {
        for (let i = 0; i < 200; i++) {
          m.startReplyItemRequest(new Uint8Array(1000));
        }
      });
      request(client, 1, true);
      await pumpEventQueue();
      expect(heldOf(back)).toBeGreaterThan(30000);
      const answers = countArrivals(client);
      await until(() => answers() === 200);
      await pumpEventQueue();
      expect(heldOf(back)).toBe(0);
    });

    test(`forwarded EXTENDs withhold credit, ${over}`, async () => {
      const [client, , back, backend] = await relay(slot);
      serve(backend, (m) => {
        for (let i = 0; i < 8000; i++) {
          void m.extend();
        }
      });
      request(client, 1);
      await pumpEventQueue();
      expect(heldOf(back)).toBeGreaterThan(30000);
      const answers = countArrivals(client);
      await until(() => answers() === 8000);
      await pumpEventQueue();
      expect(heldOf(back)).toBe(0);
    });

    test(`forwarded messages and requests withhold credit while the far end does not read, ${over}`, async () => {
      const [clientRaw, frontRaw] = await link();
      const [backRaw, backendRaw] = await link(slot);
      const client = new TalkChannel(clientRaw);
      const front = new TalkChannel(frontRaw);
      const back = new TalkChannel(backRaw);
      const backend = new TalkChannel(backendRaw);
      void front.serve((m) => void forwardMessage(m, back));
      let received = 0;
      const subscription = new Collector(backend.messages, {
        paused: true,
        onValue: (m) => {
          received++;
          if (m.expectsReply) {
            void m.reply(empty);
          }
        },
      });
      const answers: Promise<TalkMessage>[] = [];
      for (let i = 0; i < 100; i++) {
        void client.send('NOTE', new Uint8Array(1000));
        answers.push(client.request('PUT', new Uint8Array(1000)));
      }
      await pumpEventQueue();
      expect(heldOf(front)).toBeGreaterThan(30000);
      subscription.resume();
      await Promise.all(answers);
      await pumpEventQueue();
      expect(received).toBe(200);
      expect(heldOf(front)).toBe(0);
      await subscription.cancel();
    });
  }
});

describe('item windows', () => {
  /** A stream item for request [id]. */
  function item(id: number, value: number): TalkFrame {
    return new TalkFrame({ kind: TalkKind.streamItem, responseId: id, payload: bytes([value]) });
  }

  test('a credit is forwarded with the rewritten id, and the proxy grants nothing of its own', async () => {
    const r = new RawProxy();
    r.proxy.back.nextRequestId = 5;
    r.fromClient(
      new TalkFrame({
        kind: TalkKind.message,
        procedure: Name.of('LIST'),
        requestId: 2,
        stream: true,
        window: 2,
      }),
    );
    await pumpEventQueue();
    const forwarded = only(r.atBackend);
    expect(forwarded.requestId).toBe(5);
    expect(forwarded.stream).toBe(true);
    expect(forwarded.window).toBe(2);
    r.fromBackend(item(5, 1));
    r.fromBackend(item(5, 2));
    await pumpEventQueue();
    expect(
      r.atClient.filter((f) => f.kind === TalkKind.streamItem).map((f) => f.responseId),
    ).toEqual([2, 2]);
    // The items took the window; the proxy granted nothing.
    expect(r.atBackend).toHaveLength(1);
    // The wiki's vector: the credit for 2 reaches the backend for 5.
    r.client.add(hexBytes('32 02 00 00 08 00 00 00'));
    await pumpEventQueue();
    expect(hexString(last(r.atBackend).encode())).toBe('32 05 00 00 08 00 00 00');
    r.fromBackend(new TalkFrame({ kind: TalkKind.message, responseId: 5 }));
    await pumpEventQueue();
    expect(last(r.atClient).kind).toBe(TalkKind.message);
    expect(last(r.atClient).responseId).toBe(2);
    // A credit after the end goes nowhere.
    r.client.add(hexBytes('32 02 00 00 01 00 00 00'));
    await pumpEventQueue();
    expect(r.atBackend).toHaveLength(2);
    expect(r.proxy.front.isOpen).toBe(true);
    await Promise.all(r.proxy.forwards);
    await r.proxy.close();
  });

  test('credits that arrive before the request is forwarded go with it', async () => {
    const [toClientLocal, toClientForeign] = frameLink();
    const [toBackendLocal, toBackendForeign] = frameLink();
    const front = new TalkChannel(toClientLocal);
    const back = new TalkChannel(toBackendLocal);
    const atBackend = record(toBackendForeign);
    const held = new Collector(front.messages);
    toClientForeign.add(
      new TalkFrame({
        kind: TalkKind.message,
        procedure: Name.of('LIST'),
        requestId: 1,
        stream: true,
        window: 1,
      }).encode(),
    );
    toClientForeign.add(hexBytes('32 01 00 00 03 00 00 00'));
    await pumpEventQueue();
    expect(only(held.values).window).toBe(4);
    const forwarding = forwardMessage(only(held.values), back);
    await pumpEventQueue();
    expect(only(atBackend).window).toBe(4);
    expect(atBackend.filter((f) => f.isCredit)).toEqual([]);
    toClientForeign.add(hexBytes('32 01 00 00 02 00 00 00'));
    await pumpEventQueue();
    expect(last(atBackend).isCredit).toBe(true);
    expect(last(atBackend).creditItems).toBe(2);
    toBackendForeign.add(
      new TalkFrame({
        kind: TalkKind.message,
        responseId: (atBackend[0] as TalkFrame).requestId,
      }).encode(),
    );
    await forwarding;
    await held.cancel();
    await Promise.all([front.close(), back.close()]);
  });

  test('a credit the proxy cannot accept closes the client channel PROTOCOL_ERROR', async () => {
    const r = new RawProxy();
    r.fromClient(
      new TalkFrame({
        kind: TalkKind.message,
        procedure: Name.of('LIST'),
        requestId: 1,
        stream: true,
      }),
    );
    await pumpEventQueue();
    // A credit on an unlimited window.
    r.client.add(hexBytes('32 01 00 00 01 00 00 00'));
    expect((await r.proxy.front.done).known).toBe(StatusCode.protocolError);
    await pumpEventQueue();
    // The forwarded request was cancelled; nothing was forwarded.
    expect(r.atBackend.filter((f) => f.isCredit)).toEqual([]);
    expect(last(r.atBackend).kind).toBe(TalkKind.abort);
    await r.proxy.close();
  });

  test("an item beyond the window from the backend closes the proxy's channel to it PROTOCOL_ERROR; the client is answered UNAVAILABLE and its channel stays open", async () => {
    const r = new RawProxy();
    r.fromClient(
      new TalkFrame({
        kind: TalkKind.message,
        procedure: Name.of('LIST'),
        requestId: 2,
        stream: true,
        window: 1,
      }),
    );
    await pumpEventQueue();
    const id = only(r.atBackend).requestId;
    expect(only(r.atBackend).window).toBe(1);
    r.fromBackend(item(id, 1));
    r.fromBackend(item(id, 2));
    expect((await r.proxy.back.done).known).toBe(StatusCode.protocolError);
    await pumpEventQueue();
    const toClient = r.atClient.filter((f) => f.responseId === 2);
    expect(toClient.map((f) => f.kind)).toEqual([TalkKind.streamItem, TalkKind.abort]);
    expect([...(toClient[0] as TalkFrame).payload]).toEqual([1]);
    expect(last(toClient).status.known).toBe(StatusCode.unavailable);
    expect(r.proxy.front.isOpen).toBe(true);
    expect(r.proxy.front.incomingRequestCount).toBe(0);
    await Promise.all(r.proxy.forwards);
    await r.proxy.close();
  });

  test("a request forwarded while items of the intermediary's own wait for its window: they take the first credits, the far responder the rest", async () => {
    const [toFrontLocal, toFrontForeign] = frameLink();
    const client = new TalkChannel(toFrontLocal);
    const front = new TalkChannel(toFrontForeign);
    // The proxy's channel to the backend, its frames recorded.
    const [toBack, toBackend] = frameLink();
    const atBackend: TalkFrame[] = [];
    toBack.tap = (data) => {
      atBackend.push(TalkFrame.decode(data));
      return true;
    };
    const back = new TalkChannel(toBack);
    const backend = new TalkChannel(toBackend);
    const forwards: Promise<void>[] = [];
    void front.serve((m) => {
      // The intermediary answers two items itself, then forwards.
      void m.replyItem(bytes([100]));
      void m.replyItem(bytes([101]));
      forwards.push(forwardMessage(m, back));
    });
    serve(backend, (m) =>
      m.replyStream([bytes([0]), bytes([1]), bytes([2])], { trailer: bytes([9]) }),
    );
    const s = client.streamRequest('LIST', empty, { window: 0 });
    const items: number[] = [];
    listen(s.items, (i) => items.push(single(i.payload)));
    await pumpEventQueue();
    expect(only(atBackend).window).toBe(0);
    const credits = (): number[] => atBackend.filter((f) => f.isCredit).map((f) => f.creditItems);
    s.grant(1);
    await pumpEventQueue();
    expect(items).toEqual([100]);
    expect(credits()).toEqual([]);
    s.grant(2);
    await pumpEventQueue();
    expect(items).toEqual([100, 101, 0]);
    expect(credits()).toEqual([1]);
    s.grant(2);
    expect([...(await s.done).payload]).toEqual([9]);
    expect(items).toEqual([100, 101, 0, 1, 2]);
    expect(credits()).toEqual([1, 2]);
    expect(client.isOpen).toBe(true);
    expect(front.isOpen).toBe(true);
    await Promise.all(forwards);
    await Promise.all([client.close(), front.close(), back.close(), backend.close()]);
  });

  for (const hops of [1, 2]) {
    const label = `${hops} ${hops === 1 ? 'proxy' : 'proxies'}`;

    test(`a finite window with grants and a cancel, an unrelated request served meanwhile, ${label}`, async () => {
      const chain = new Chain({ proxies: hops });
      let produced = 0;
      const sourceCancelled = new Deferred<void>();
      const cancelled = new Deferred<void>();
      serve(chain.backend, (m) => {
        switch (m.procedureName) {
          case 'ECHO':
            void m.reply(m.payload);
            break;
          case 'TICKS': {
            async function* ticks(): AsyncGenerator<Uint8Array> {
              try {
                for (let i = 0; ; i++) {
                  produced++;
                  yield bytes([i]);
                }
              } finally {
                sourceCancelled.resolve();
              }
            }
            void m.onCancel.then(() => cancelled.resolve());
            void m.replyStream(ticks());
            break;
          }
        }
      });
      const s = chain.client.streamRequest('TICKS', empty, { window: 2 });
      const items: number[] = [];
      listen(
        s.items,
        (i) => items.push(single(i.payload)),
        () => {},
      );
      await pumpEventQueue();
      expect(items).toEqual([0, 1]);
      expect(s.window).toBe(0);
      // One item read ahead at the backend, held at its yield.
      expect(produced).toBe(3);
      // The channel carries other calls meanwhile.
      const echo = await chain.client.request('ECHO', bytes([7]));
      expect([...echo.payload]).toEqual([7]);
      s.grant(3);
      await pumpEventQueue();
      expect(items).toEqual([0, 1, 2, 3, 4]);
      expect(produced).toBe(6);
      // Every proxy kept the window as the client sees it, and granted
      // nothing of its own.
      expect(s.window).toBe(0);
      s.cancel();
      await rejectsStatus(s.done, StatusCode.cancelled);
      await cancelled.promise;
      await sourceCancelled.promise;
      await chain.settle();
      expect(items).toHaveLength(5);
      chain.expectIdle();
      await chain.close();
    });

    test(`a stream request nested in a reply keeps its window, and its requester's grants are forwarded, ${label}`, async () => {
      const chain = new Chain({ proxies: hops });
      const sub = new Deferred<TalkStream>();
      serve(chain.backend, (m) => {
        sub.resolve(m.replyItemStreamRequest(bytes([1]), { window: 1 }));
        void m.reply(empty);
      });
      const s = chain.client.streamRequest('SUB', empty);
      const asked = await first(s.items);
      expect(asked.expectsStream).toBe(true);
      expect(asked.window).toBe(1);
      for (let i = 0; i < 4; i++) {
        void asked.replyItem(bytes([i]));
      }
      const subscribed = await sub.promise;
      const got: number[] = [];
      listen(subscribed.items, (i) => got.push(single(i.payload)));
      await pumpEventQueue();
      expect(got).toEqual([0]);
      expect(asked.window).toBe(0);
      subscribed.grant(2);
      await pumpEventQueue();
      expect(got).toEqual([0, 1, 2]);
      subscribed.grant(1);
      await pumpEventQueue();
      expect(got).toEqual([0, 1, 2, 3]);
      void asked.reply(empty);
      await subscribed.done;
      await chain.settle();
      chain.expectIdle();
      await chain.close();
    });

    test(`the loss of the backend while exhausted fails the requester UNAVAILABLE, ${label}`, async () => {
      const chain = new Chain({ proxies: hops });
      const held = new Deferred<TalkMessage>();
      serve(chain.backend, (m) => {
        held.resolve(m);
      });
      const s = chain.client.streamRequest('LIST', empty, { window: 0 });
      const m = await held.promise;
      const waiting = m.replyItem(bytes([1]));
      await pumpEventQueue();
      await chain.backend.close(Status.of(StatusCode.aborted, 'gone'));
      await rejectsRemote(s.done, StatusCode.unavailable);
      // The backend's item that waited fails with its channel's status.
      await rejectsStatus(waiting, StatusCode.aborted);
      await chain.close();
    });
  }

  // Not ported: 'a finite window with grants and a cancel, an unrelated
  // request served meanwhile, 1 proxy, synchronous transport', 'a stream
  // request nested in a reply keeps its window, and its requester's grants
  // are forwarded, 1 proxy, synchronous transport' and 'the loss of the
  // backend while exhausted fails the requester UNAVAILABLE, 1 proxy,
  // synchronous transport': there is no synchronous transport here (see
  // the note after the proxy groups).

  test("the proxy runs no credit bound of its own; the backend's does", async () => {
    const clock = new ManualClock();
    const chain = new Chain({
      clock,
      proxy: { creditTimeout: 1000 },
      backend: { creditTimeout: 0 },
    });
    const held: TalkMessage[] = [];
    serve(chain.backend, (m) => {
      held.push(m);
    });
    const s = chain.client.streamRequest('LIST', empty, { window: 0 });
    const done = new Outcome(s.done);
    await clock.elapseAsync(hour);
    expect(done.isDone).toBe(false);
    only(held).setCreditTimeout(3000);
    await clock.elapseAsync(3000);
    expectRemote(done.error, StatusCode.deadlineExceeded);
    await chain.close();
    await flush();
    expect(clock.pendingCount).toBe(0);
  });
});
