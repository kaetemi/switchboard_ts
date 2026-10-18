/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Message chain forwarding (`forwardMessage`, wiki page "Switchboard
// Proxying") through intermediaries over the real frame mux: every hop is
// a Talk channel over its own pair of FrameMuxConnections on the memory
// transport, so the forwarded frames, their credit, the bulk channels
// piped through and the item windows all cross real mux channels. The
// groups of the Dart `test/talk_forward_test.dart` that run over mux
// connections there ('bulk payloads', 'forwarding credit', with Dart's
// SlotChannel as the slot channel) run here as in Dart; '1 proxy',
// '2 proxies', 'timeouts' and the item window tests over a chain, which
// the Dart file runs over stream channel controllers, run again over the
// frame mux. test/unit/talk/talk-forward.test.ts runs every group over
// frame links and the memory mux pair. Not here: 'frame level',
// 'forwardMessage' and the raw item window tests, which drive frames by
// hand on frame links and involve no mux.

import { afterEach, beforeEach, describe, expect, test } from 'vitest';
import { bytesEqual } from '../../src/core/bytes.ts';
import type { MuxChannel, MuxChannelCarrier } from '../../src/core/mux/contract.ts';
import { MuxCredit } from '../../src/core/mux/mux-frame.ts';
import { Name } from '../../src/core/name.ts';
import { SlotChannel } from '../../src/core/naming/slot-channel.ts';
import { Status, StatusCode } from '../../src/core/status.ts';
import type { TalkMessage, TalkOptions, TalkStream } from '../../src/core/talk/contract.ts';
import { TalkAbortError, TalkChannel } from '../../src/core/talk/talk-channel.ts';
import { forwardMessage } from '../../src/core/talk/talk-forward.ts';
import { TalkFrame, TalkKind } from '../../src/core/talk/talk-frame.ts';
import { Deferred } from '../../src/core/util/deferred.ts';
import { ManualClock } from '../../src/testing/manual-clock.ts';
import { until } from '../support/talk/bulk-peers.ts';
import { frameMuxPair, nextIncoming } from '../support/talk/frame-mux-peers.ts';
import {
  bytes,
  Collector,
  expectRemote,
  expectSame,
  expectStatus,
  first,
  flush,
  Outcome,
  only,
  Queue,
  rejection,
  rejectsRemote,
  rejectsStatus,
  toList,
} from '../support/talk/helpers.ts';

const empty = new Uint8Array(0);
const hour = 3_600_000;

/** The flow-control cost of a subframe of [length] bytes. */
const costOf = (length: number): number => MuxCredit.costOf(length);

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
  /** The clock of every connection, and so of every channel. */
  readonly clock?: ManualClock;
}

/**
 * A Talk channel over a fresh frame mux pair, from its initiator, and its
 * far end with [far]; the connections close when the test finishes.
 */
async function muxTalk(
  init: {
    near?: Partial<TalkOptions> | undefined;
    far?: Partial<TalkOptions> | undefined;
    clock?: ManualClock | undefined;
  } = {},
): Promise<[TalkChannel, TalkChannel]> {
  const [left, right] = frameMuxPair(
    init.clock === undefined ? {} : { options: { clock: init.clock } },
  );
  const nearEnd = new TalkChannel(left.open(empty), init.near);
  return [nearEnd, new TalkChannel(await nextIncoming(right), init.far)];
}

/** client ↔ proxy ↔ … ↔ proxy ↔ backend, each hop over a frame mux pair of its own. */
class Chain {
  readonly client: TalkChannel;
  readonly backend: TalkChannel;
  readonly proxies: Intermediary[];

  private constructor(client: TalkChannel, proxies: Intermediary[], backend: TalkChannel) {
    this.client = client;
    this.proxies = proxies;
    this.backend = backend;
  }

  static async connect(options: ChainOptions = {}): Promise<Chain> {
    const clock = options.clock === undefined ? {} : { clock: options.clock };
    const [client, firstFront] = await muxTalk({
      near: options.client,
      far: options.proxy,
      ...clock,
    });
    const proxies: Intermediary[] = [];
    let front = firstFront;
    const hops = options.proxies ?? 1;
    let backend: TalkChannel | undefined;
    for (let i = 0; i < hops; i++) {
      const last = i === hops - 1;
      const [back, next] = await muxTalk({
        near: options.proxy,
        far: last ? options.backend : options.proxy,
        ...clock,
      });
      proxies.push(new Intermediary(front, back));
      front = next;
      if (last) {
        backend = next;
      }
    }
    return new Chain(client, proxies, backend as TalkChannel);
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

// ---------------------------------------------------------------------------

for (const hops of [1, 2]) {
  const label = `${hops} ${hops === 1 ? 'proxy' : 'proxies'}`;
  describe(label, () => {
    let chain: Chain;
    beforeEach(async () => {
      chain = await Chain.connect({ proxies: hops });
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

// Not ported here: the synchronous transport variants, which have no TS
// counterpart (see test/unit/talk/talk-forward.test.ts).

describe('timeouts', () => {
  test("a deadline the backend declares reaches the client with the backend's buffer only, through every hop", async () => {
    const clock = new ManualClock();
    const chain = await Chain.connect({ proxies: 2, clock });
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
    const chain = await Chain.connect({
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
    const chain = await Chain.connect({
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
    const chain = await Chain.connect({ clock, backend: { replyTimeout: 30 } });
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
    const chain = await Chain.connect({
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
   * client ↔ intermediary ↔ backend over mux connections; the intermediary
   * forwards every message both ways and streams bulk payloads unless
   * [reassemble]; [seen] gets what it forwarded toward the backend.
   */
  async function chain(
    options: { reassemble?: boolean; backendStreams?: boolean; seen?: TalkMessage[] } = {},
  ): Promise<[TalkChannel, TalkChannel]> {
    const proxyOptions: Partial<TalkOptions> =
      options.reassemble === true ? {} : { streamBulk: () => true };
    const [client, front] = await muxTalk({ far: proxyOptions });
    const [back, backend] = await muxTalk({
      near: proxyOptions,
      far: options.backendStreams === true ? { streamBulk: () => true } : {},
    });
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
    const [client, front] = await muxTalk({ far: { streamBulk: () => true } });
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
  async function link(slot = false): Promise<[MuxChannel | MuxChannelCarrier, MuxChannel]> {
    const [left, right] = frameMuxPair();
    const opened = left.open(empty);
    // Dart's `SlotChannel(Name('svc'), 0, opened, (_) async => null)`.
    const near = slot
      ? new SlotChannel(Name.of('svc'), 0, opened, () => Promise.resolve(undefined))
      : opened;
    return [near, await nextIncoming(right)];
  }

  /**
   * What the mux channel under [talk] holds for it, not consumed: at least
   * half a window once its sender is held back (credit goes back in
   * batches of half the window), nothing while it is not.
   */
  function heldOf(talk: TalkChannel): number {
    return (talk.raw as MuxChannel).heldBytes;
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
  ): Promise<[MuxChannel, TalkChannel, TalkChannel, TalkChannel]> {
    const [frontRaw, client] = await link(slotFront);
    const [backRaw, backendRaw] = await link(slotBack);
    const front = new TalkChannel(frontRaw);
    const back = new TalkChannel(backRaw);
    const backend = new TalkChannel(backendRaw);
    void front.serve((m) => void forwardMessage(m, back));
    return [client, front, back, backend];
  }

  /** Sends request [id] on [client], a raw mux channel. */
  function request(client: MuxChannel, id: number, stream = false): void {
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
  function countArrivals(client: MuxChannel): () => number {
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
      expect((backend.raw as MuxChannel).sendWindow).toBeLessThan(costOf(1004));
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
  for (const hops of [1, 2]) {
    const label = `${hops} ${hops === 1 ? 'proxy' : 'proxies'}`;

    test(`a finite window with grants and a cancel, an unrelated request served meanwhile, ${label}`, async () => {
      const chain = await Chain.connect({ proxies: hops });
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
      const chain = await Chain.connect({ proxies: hops });
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
      const chain = await Chain.connect({ proxies: hops });
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

  test("the proxy runs no credit bound of its own; the backend's does", async () => {
    const clock = new ManualClock();
    const chain = await Chain.connect({
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
