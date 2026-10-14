/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Port of the Dart `slot_routing_test.dart`: the node's slot routing
// (`selectAndConnect` and the opens with a SlotResolver, `resolveSlotOwner`,
// the proxy) and the MOVED retry of `openChannelToSlot` and
// `openTalkToSlot`, over nodes on the in-memory transport (Dart: TCP), over
// the toy mux and, from `over-frame-mux/`, over the frame mux (the bulk
// case there only).

import { afterEach, beforeEach, describe, expect, test } from 'vitest';
import { type ChannelAddress, ServiceAddress } from '../../../src/core/address/index.ts';
import { utf8Decode } from '../../../src/core/bytes.ts';
import type { MuxChannel } from '../../../src/core/mux/contract.ts';
import { Name } from '../../../src/core/name.ts';
import {
  MovedStatus,
  SlotEntry,
  SlotMode,
  SlotSpace,
  serviceRecord,
} from '../../../src/core/naming/naming-protocol.ts';
import type { SlotChannel } from '../../../src/core/naming/slot-channel.ts';
import { Status, StatusCode } from '../../../src/core/status.ts';
import type { IncomingChannel, ServiceRecord } from '../../../src/core/switchboard/contract.ts';
import { proxyHandler } from '../../../src/core/switchboard/proxy.ts';
import { EndpointResolver, StaticResolver } from '../../../src/core/switchboard/resolver.ts';
import type { Switchboard } from '../../../src/core/switchboard/switchboard.ts';
import type { TalkMessage } from '../../../src/core/talk/index.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';
import { Pull } from '../../support/node/channels.ts';
import {
  bytes,
  node,
  onCleanup,
  overFrameMux,
  rejectsWith,
  svc,
  until,
} from '../../support/node/harness.ts';

const limit = 5000;

function text(data: Uint8Array): string {
  return utf8Decode(data);
}

/** A static resolver whose `LOCATE` answers are scripted. */
class LocatingResolver extends StaticResolver {
  readonly located: number[] = [];
  answer: (slot: number) => SlotEntry | undefined | Promise<SlotEntry | undefined> = () =>
    undefined;

  override async locateSlot(_type: Name, slot: number): Promise<SlotEntry | undefined> {
    this.located.push(slot);
    return this.answer(slot);
  }
}

/** How a backend treats the channels it receives. */
const Mode = {
  /** Sends `<name> <shard>` at once, then echoes `<name>:<data>`. */
  greet: 'greet',
  /** Rejects with MOVED carrying the backend's `moved` at once. */
  moved: 'moved',
  /** Rejects the first channel as `moved` does, then greets as `greet`. */
  movedOnce: 'movedOnce',
  /** Closes with RELOCATED carrying `moved` at once. */
  relocated: 'relocated',
  /** Rejects with MOVED once something arrived. */
  movedAfterData: 'movedAfterData',
  /** Sends a greeting, then closes with MOVED. */
  greetThenMoved: 'greetThenMoved',
  /** Talk: sends a `HELLO` message, answers requests with `<name>:<data>`. */
  talk: 'talk',
  /** Sends nothing; records what arrives in `received`. */
  collect: 'collect',
} as const;
type Mode = (typeof Mode)[keyof typeof Mode];

/** Reads every subframe of [stream], calling [onData] for each. */
async function readAll(
  stream: ReadableStream<Uint8Array>,
  onData: (data: Uint8Array) => void,
): Promise<void> {
  try {
    for await (const data of stream) onData(data);
  } catch {
    // The channel ended.
  }
}

class Backend {
  readonly name: string;
  readonly id: number;
  sb!: Switchboard;
  uri!: string;
  mode: Mode = Mode.greet;
  moved: MovedStatus = MovedStatus.unknown;
  readonly opened: ChannelAddress[] = [];
  /** What arrived in `collect` mode. */
  readonly received: string[] = [];
  /** The end status of every channel opened to it. */
  readonly ended: Status[] = [];

  constructor(name: string, id: number) {
    this.name = name;
    this.id = id;
  }

  get record(): ServiceRecord {
    return serviceRecord(new ServiceAddress(svc, this.id), { endpoints: [this.uri] });
  }

  async start(): Promise<void> {
    this.sb = node();
    this.sb.registerService(svc, (incoming) => this.#serve(incoming), { instance: this.id });
    this.uri = await this.sb.listenMemory();
  }

  #serve(incoming: IncomingChannel): void {
    this.opened.push(incoming.address);
    const channel = incoming.channel;
    void channel.done.then((status) => this.ended.push(status));
    const moved = this.moved.toStatus();
    switch (this.mode) {
      case Mode.greet:
        void channel.send(bytes(`${this.name} ${incoming.address.shard}`));
        void readAll(channel.readable, (d) => {
          void channel.send(bytes(`${this.name}:${text(d)}`)).catch(() => {});
        });
        break;
      case Mode.moved:
        void incoming.reject(moved);
        break;
      case Mode.movedOnce:
        this.mode = Mode.greet;
        void incoming.reject(moved);
        break;
      case Mode.relocated:
        void incoming.reject(this.moved.toStatus({ relocated: true }));
        break;
      case Mode.movedAfterData: {
        const reader = channel.readable.getReader();
        void reader.read().then(() => incoming.reject(moved));
        break;
      }
      case Mode.greetThenMoved:
        void channel.send(bytes(`${this.name} ${incoming.address.shard}`));
        void incoming.reject(moved);
        break;
      case Mode.talk: {
        const talk = incoming.talk();
        void talk.send('HELLO', this.name).catch(() => {});
        void talk.serve((m) => m.reply(`${this.name}:${m.text()}`));
        break;
      }
      case Mode.collect:
        void readAll(channel.readable, (d) => this.received.push(text(d)));
        break;
    }
  }
}

/** The subframes of a channel, read one by one as text. */
class Subframes {
  readonly #reader: ReadableStreamDefaultReader<Uint8Array>;

  constructor(channel: SlotChannel | MuxChannel) {
    this.#reader = channel.readable.getReader();
  }

  async next(): Promise<string> {
    const result = await this.#reader.read();
    if (result.done) throw new Error('the channel ended');
    return text(result.value);
  }

  async rest(): Promise<string[]> {
    const out: string[] = [];
    for (;;) {
      const result = await this.#reader.read();
      if (result.done) return out;
      out.push(text(result.value));
    }
  }
}

/** The greeting of a `greet` backend on [channel]. */
function greeting(channel: SlotChannel | MuxChannel): Promise<string> {
  return new Subframes(channel).next();
}

describe('slot routing', () => {
  let a: Backend;
  let b: Backend;
  let resolver: LocatingResolver;
  let router: Switchboard;

  beforeEach(async () => {
    a = new Backend('A', 1);
    b = new Backend('B', 2);
    await a.start();
    await b.start();
    resolver = new LocatingResolver([a.record, b.record]);
    router = node({ resolver, slotRefreshTimeout: 500 });
  });

  afterEach(async () => {
    await resolver.close();
  });

  function define(options: { mode?: SlotMode; count?: number } = {}): void {
    resolver.defineSlots(
      new SlotSpace(svc, { count: options.count ?? 8, mode: options.mode ?? SlotMode.managed }),
    );
  }

  /** The greeting of a channel opened to [slot] (and [instance]) of `svc`. */
  async function greetingOf(slot: number, instance = 0): Promise<string> {
    return greeting(await router.openChannel(new ServiceAddress(svc, instance), { shard: slot }));
  }

  describe('routing', () => {
    test('without a slot table: sorted[slot mod n]', async () => {
      expect(await greetingOf(2)).toBe('A 2');
      expect(await greetingOf(3)).toBe('B 3');
    });

    test('to the owner, the old owner while migrating', async () => {
      define();
      resolver.setSlot(svc, 2, SlotEntry.owned(2, { epoch: 1 }));
      resolver.setSlot(svc, 3, SlotEntry.migrating(1, 2, { epoch: 4 }));
      expect(await greetingOf(2)).toBe('B 2');
      expect(await greetingOf(3)).toBe('A 3');
      // The header names the owner, so that its dispatch is exact.
      expect(b.opened).toHaveLength(1);
      expect(b.opened[0]?.instance).toBe(2);
      expect(b.opened[0]?.shard).toBe(2);
      // An explicit instance is not rerouted.
      expect(await greetingOf(2, 1)).toBe('A 2');
      expect(resolver.located).toEqual([]);
    });

    test('a free slot: LOCATE in a managed space', async () => {
      define();
      resolver.answer = () => SlotEntry.owned(2, { epoch: 1 });
      expect(await greetingOf(5)).toBe('B 5');
      expect(resolver.located).toEqual([5]);
      resolver.answer = () => undefined;
      await rejectsWith(greetingOf(6), StatusCode.unavailable);
    });

    test('a free slot: UNAVAILABLE in a static space, no LOCATE', async () => {
      define({ mode: SlotMode.static });
      await rejectsWith(greetingOf(5), StatusCode.unavailable);
      expect(resolver.located).toEqual([]);
    });

    test('an owner that registered while the slot was located', async () => {
      define();
      const c = new Backend('C', 3);
      await c.start();
      resolver.answer = () => {
        // Not in the records the router read before asking.
        resolver.add(c.record);
        return SlotEntry.owned(3, { epoch: 1 });
      };
      expect(await greetingOf(4)).toBe('C 4');
      expect(resolver.located).toEqual([4]);
      expect(c.opened.map((o) => o.instance)).toEqual([3]);
    });

    test('a slot outside the space: OUT_OF_RANGE', async () => {
      define({ count: 4 });
      await rejectsWith(greetingOf(4), StatusCode.outOfRange);
    });

    test('an owner missing from the service table or unreachable: UNAVAILABLE, no other instance', async () => {
      define();
      resolver.setSlot(svc, 1, SlotEntry.owned(9, { epoch: 1 }));
      await rejectsWith(greetingOf(1), StatusCode.unavailable);
      resolver.setSlot(svc, 1, SlotEntry.owned(2, { epoch: 1 }));
      await b.sb.close();
      await rejectsWith(greetingOf(1), StatusCode.unavailable);
      expect(a.opened).toEqual([]);
    });

    test('resolveSlotOwner', async () => {
      define();
      resolver.setSlot(svc, 1, SlotEntry.owned(2, { epoch: 1 }));
      expect((await router.resolveSlotOwner(svc, 1)).equals(new ServiceAddress(svc, 2))).toBe(true);
      resolver.answer = () => SlotEntry.owned(1, { epoch: 2 });
      expect(
        (await router.resolveSlotOwner(svc, 1, { refresh: true })).equals(
          new ServiceAddress(svc, 1),
        ),
      ).toBe(true);
      expect(resolver.located).toEqual([1]);
      await rejectsWith(router.resolveSlotOwner(Name.of('none'), 1), StatusCode.notFound);
      router.resolver = new EndpointResolver(a.uri);
      await rejectsWith(router.resolveSlotOwner(svc, 1), StatusCode.failedPrecondition);
    });

    test('a proxy routes by slot', async () => {
      define();
      resolver.setSlot(svc, 3, SlotEntry.owned(2, { epoch: 1 }));
      router.catchAll = proxyHandler(router);
      const endpoint = await router.listenMemory();
      const client = node({ resolver: new EndpointResolver(endpoint) });
      const channel = await client.openChannel(new ServiceAddress(svc), { shard: 3 });
      expect(await greeting(channel)).toBe('B 3');
      expect(b.opened.map((o) => o.instance)).toEqual([2]);
    });
  });

  describe('MOVED', () => {
    beforeEach(() => {
      define();
      resolver.setSlot(svc, 1, SlotEntry.owned(1, { epoch: 1 }));
    });

    test('retried once at the owner the reason names', async () => {
      a.mode = Mode.moved;
      a.moved = new MovedStatus({ owner: 2, epoch: 2 });
      const channel = await router.openChannelToSlot(svc, 1);
      const answers = new Subframes(channel);
      expect(await answers.next()).toBe('B 1');
      expect(channel.retried).toBe(true);
      expect(channel.channel.connection.isOpen).toBe(true);
      expect(b.opened).toHaveLength(1);
      expect(b.opened[0]?.payload).toHaveLength(0);
      // The replacement carries on as the channel.
      void channel.send(bytes('x'));
      expect(await answers.next()).toBe('B:x');
      await channel.close();
      expect((await channel.done).isOk).toBe(true);
      expect(resolver.located).toEqual([]);
    });

    test('whenWritable waits for the replacement and follows it', async () => {
      a.mode = Mode.moved;
      a.moved = new MovedStatus({ owner: 2, epoch: 2 });
      const channel = await router.openChannelToSlot(svc, 1);
      const first = channel.channel;
      await first.done;
      // Rejected with nothing sent: a replacement is on its way.
      expect(channel.isWritable).toBe(false);
      await channel.whenWritable;
      expect(channel.retried).toBe(true);
      expect(channel.channel).not.toBe(first);
      expect(channel.isWritable).toBe(true);
      expect(await greeting(channel)).toBe('B 1');
      await channel.close();
      expect(channel.isWritable).toBe(false);
      await rejectsWith(channel.whenWritable, StatusCode.failedPrecondition);
    });

    test('whenWritable fails when no replacement comes', async () => {
      a.mode = Mode.moved;
      resolver.answer = () => SlotEntry.owned(1, { epoch: 1 });
      const channel = await router.openChannelToSlot(svc, 1);
      await channel.channel.done;
      await rejectsWith(channel.whenWritable, StatusCode.failedPrecondition);
      expect((await channel.done).code).toBe(StatusCode.moved);
    });

    test('the open payload is sent again', async () => {
      a.mode = Mode.moved;
      a.moved = new MovedStatus({ owner: 2, epoch: 2 });
      const channel = await router.openChannelToSlot(svc, 1, { payload: bytes('token') });
      expect(await greeting(channel)).toBe('B 1');
      expect(a.opened.map((o) => text(o.payload))).toEqual(['token']);
      expect(b.opened.map((o) => text(o.payload))).toEqual(['token']);
    });

    test('a reason without an owner: the router asks', async () => {
      a.mode = Mode.moved;
      resolver.answer = () => SlotEntry.owned(2, { epoch: 2 });
      const channel = await router.openChannelToSlot(svc, 1);
      expect(await greeting(channel)).toBe('B 1');
      expect(resolver.located).toEqual([1]);
    });

    test('a newer table entry wins over the reason', async () => {
      a.mode = Mode.moved;
      a.moved = new MovedStatus({ owner: 9, epoch: 2 });
      const channel = await router.openChannelToSlot(svc, 1);
      resolver.setSlot(svc, 1, SlotEntry.owned(2, { epoch: 3 }));
      expect(await greeting(channel)).toBe('B 1');
    });

    test('a newer table entry naming the rejecting owner wins too', async () => {
      a.mode = Mode.movedOnce;
      a.moved = new MovedStatus({ owner: 2, epoch: 2 });
      const channel = await router.openChannelToSlot(svc, 1);
      // The slot came back to A after it rejected the channel: the retry goes
      // to A, not to B, which the older MOVED names.
      resolver.setSlot(svc, 1, SlotEntry.owned(1, { epoch: 3 }));
      expect(await greeting(channel)).toBe('A 1');
      expect(channel.retried).toBe(true);
      expect(a.opened).toHaveLength(2);
      expect(b.opened).toHaveLength(0);
      expect(resolver.located).toEqual([]);
    });

    test('nowhere else to go: MOVED is surfaced', async () => {
      a.mode = Mode.moved;
      resolver.answer = () => SlotEntry.owned(1, { epoch: 1 });
      const channel = await router.openChannelToSlot(svc, 1);
      expect(await new Subframes(channel).rest()).toEqual([]);
      expect((await channel.done).code).toBe(StatusCode.moved);
      expect(channel.retried).toBe(true);
      expect(b.opened).toHaveLength(0);
    });

    test('only once', async () => {
      a.mode = Mode.moved;
      a.moved = new MovedStatus({ owner: 2, epoch: 2 });
      b.mode = Mode.moved;
      b.moved = new MovedStatus({ owner: 1, epoch: 3 });
      const channel = await router.openChannelToSlot(svc, 1);
      expect((await channel.done).code).toBe(StatusCode.moved);
      expect(a.opened).toHaveLength(1);
      expect(b.opened).toHaveLength(1);
    });

    test('never on RELOCATED: the slot moved after work started', async () => {
      a.mode = Mode.relocated;
      a.moved = new MovedStatus({ owner: 2, epoch: 2 });
      const channel = await router.openChannelToSlot(svc, 1);
      const status = await channel.done;
      expect(status.code).toBe(StatusCode.relocated);
      expect(MovedStatus.fromStatus(status).equals(new MovedStatus({ owner: 2, epoch: 2 }))).toBe(
        true,
      );
      expect(channel.retried).toBe(false);
      expect(b.opened).toHaveLength(0);
    });

    test('not after the first send; the fields still name the owner', async () => {
      a.mode = Mode.movedAfterData;
      a.moved = new MovedStatus({ owner: 2, epoch: 2 });
      const channel = await router.openChannelToSlot(svc, 1);
      void channel.send(bytes('x'));
      const status = await channel.done;
      expect(status.code).toBe(StatusCode.moved);
      expect(MovedStatus.fromStatus(status).equals(new MovedStatus({ owner: 2, epoch: 2 }))).toBe(
        true,
      );
      expect(channel.retried).toBe(false);
      expect(b.opened).toHaveLength(0);
    });

    test('not after the first subframe received', async () => {
      a.mode = Mode.greetThenMoved;
      a.moved = new MovedStatus({ owner: 2, epoch: 2 });
      const channel = await router.openChannelToSlot(svc, 1);
      expect(await new Subframes(channel).rest()).toHaveLength(1);
      expect((await channel.done).code).toBe(StatusCode.moved);
      expect(channel.retried).toBe(false);
      expect(b.opened).toHaveLength(0);
    });

    describe('subframes sent while the replacement is opened', () => {
      let small: Switchboard;
      let asked: Deferred<void>;
      let answer: Deferred<SlotEntry | undefined>;

      beforeEach(() => {
        // A window of 1000 bytes: a slot channel of this node holds at most
        // that much, each subframe counted as its length plus 16.
        small = node({ resolver, muxOptions: { initialWindow: 1000 }, slotRefreshTimeout: limit });
        // The owner names nobody; the LOCATE answers when the test says.
        a.mode = Mode.moved;
        b.mode = Mode.collect;
        asked = new Deferred<void>();
        answer = new Deferred<SlotEntry | undefined>();
        resolver.answer = () => {
          asked.resolve();
          return answer.promise;
        };
      });

      afterEach(() => {
        answer.resolve(undefined);
      });

      test("are sent on it, in order, within the node's buffer limit", async () => {
        const channel = await small.openChannelToSlot(svc, 1);
        await asked.promise;
        const held = [0, 1, 2].map((i) => `${i}`.repeat(300));
        for (const subframe of held) {
          void channel.send(bytes(subframe));
        }
        expect(channel.canSend).toBe(true);
        answer.resolve(SlotEntry.owned(2, { epoch: 2 }));
        await until(() => b.received.length === held.length, limit);
        expect(b.received).toEqual(held);
        expect(channel.retried).toBe(true);
        void channel.send(bytes('after'));
        await until(() => b.received.length > held.length, limit);
        expect(b.received.at(-1)).toBe('after');
        await channel.close();
        expect((await channel.done).isOk).toBe(true);
      });

      test('beyond it: RESOURCE_EXHAUSTED at once, the replacement closed unused', async () => {
        const channel = await small.openChannelToSlot(svc, 1);
        await asked.promise;
        for (let i = 0; i < 3; i++) {
          channel.send(bytes('x'.repeat(300))).catch(() => {});
        }
        expect(channel.canSend).toBe(true);
        // A fourth would hold 1264 bytes.
        channel.send(bytes('x'.repeat(300))).catch(() => {});
        expect(channel.canSend).toBe(false);
        // Before the LOCATE has answered.
        const status = await channel.done;
        expect(status.code).toBe(StatusCode.resourceExhausted);
        expect(await new Subframes(channel).rest()).toEqual([]);
        expect(() => channel.send(bytes('y'))).toThrow(/closing/);
        await channel.close();
        // The LOCATE answers after all: nothing is sent on the replacement.
        answer.resolve(SlotEntry.owned(2, { epoch: 2 }));
        await until(() => b.ended.length > 0, limit);
        expect(b.ended[0]?.code).toBe(StatusCode.resourceExhausted);
        expect(b.received).toEqual([]);
        expect(await channel.done).toBe(status);
      });
    });

    test('Talk: a server that speaks first gets the retry', async () => {
      a.mode = Mode.moved;
      a.moved = new MovedStatus({ owner: 2, epoch: 2 });
      b.mode = Mode.talk;
      const talk = await router.openTalkToSlot(svc, 1);
      const messages = new Pull<TalkMessage>(talk.messages);
      const hello = await messages.next();
      expect(hello.procedureName).toBe('HELLO');
      expect(hello.text()).toBe('B');
      expect((await talk.request('GET', 'k')).text()).toBe('B:k');
      await talk.close();
    });

    // Over the frame mux, whose window sends a payload this size bulk.
    if (overFrameMux()) {
      test('Talk: bulk payloads both ways on the retried slot channel', async () => {
        a.mode = Mode.moved;
        a.moved = new MovedStatus({ owner: 2, epoch: 2 });
        b.mode = Mode.talk;
        const talk = await router.openTalkToSlot(svc, 1);
        await new Pull<TalkMessage>(talk.messages).next();
        const big = 'k'.repeat(100_000);
        const answer = await talk.request('GET', big);
        expect(answer.isBulk).toBe(true);
        expect(utf8Decode(await answer.bytes())).toBe(`B:${big}`);
        await talk.close();
      });
    }

    test('through a resolver without slot tables: the same address once more', async () => {
      // A proxy without slot tables forwards MOVED; its client retries.
      const proxyTable = new StaticResolver([a.record]);
      onCleanup(() => proxyTable.close());
      const proxy = node();
      proxy.catchAll = proxyHandler(proxy, { resolver: proxyTable });
      const endpoint = await proxy.listenMemory();
      const client = node({ resolver: new EndpointResolver(endpoint) });
      // The slot moves while the first open is on its way.
      let opens = 0;
      a.sb.registerService(
        svc,
        (incoming) => {
          opens++;
          proxyTable.remove(new ServiceAddress(svc, 1));
          proxyTable.add(b.record);
          void incoming.reject(Status.of(StatusCode.moved));
        },
        { instance: 1 },
      );
      const channel = await client.openChannelToSlot(svc, 1);
      expect(await greeting(channel)).toBe('B 1');
      expect(opens).toBe(1);
      expect(channel.retried).toBe(true);
    });
  });
});
