/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Port of the Dart `slot_routing_test.dart`, group "MOVED": the retry of a
// slot channel the owner rejects, over nodes on the in-memory transport.
// The "routing" group tests the node's slot routing (`selectAndConnect`
// with a SlotResolver, `resolveSlotOwner`, the proxy), which lands with the
// node's slot-routed opens; until then `openChannelToSlot` below stands in
// for `Switchboard.openChannelToSlot`, routing the first channel to the
// owner in the table as the node will.

import { afterEach, beforeEach, describe, expect, test } from 'vitest';
import { ChannelAddress, ServiceAddress } from '../../../src/core/address/index.ts';
import { utf8Decode } from '../../../src/core/bytes.ts';
import type { Name } from '../../../src/core/name.ts';
import {
  MovedStatus,
  SlotEntry,
  SlotMode,
  SlotSpace,
  serviceRecord,
} from '../../../src/core/naming/naming-protocol.ts';
import { SlotChannel } from '../../../src/core/naming/slot-channel.ts';
import { reopenAtSlotOwner } from '../../../src/core/naming/slot-reopen.ts';
import { type SlotEvent, SlotTable } from '../../../src/core/naming/slot-table.ts';
import { type Status, StatusCode } from '../../../src/core/status.ts';
import type {
  IncomingChannel,
  ServiceEvent,
  ServiceRecord,
  SlotResolver,
} from '../../../src/core/switchboard/contract.ts';
import { StaticResolver } from '../../../src/core/switchboard/resolver.ts';
import type { Switchboard } from '../../../src/core/switchboard/switchboard.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';
import { bytes, node, rejectsWith, svc, until } from '../../support/node/harness.ts';

const limit = 5000;

function text(data: Uint8Array): string {
  return utf8Decode(data);
}

/** A static resolver with configured slot tables whose `LOCATE` answers are scripted. */
class LocatingResolver implements SlotResolver {
  readonly inner: StaticResolver;
  readonly located: number[] = [];
  answer: (slot: number) => SlotEntry | undefined | Promise<SlotEntry | undefined> = () =>
    undefined;
  readonly #tables = new Map<string, { table: SlotTable; entries: Map<number, SlotEntry> }>();

  constructor(records: ServiceRecord[]) {
    this.inner = new StaticResolver(records);
  }

  resolve(type: Name): Promise<ServiceRecord[]> {
    return this.inner.resolve(type);
  }

  get events(): AsyncIterable<ServiceEvent> {
    return this.inner.events;
  }

  get ready(): Promise<void> {
    return this.inner.ready;
  }

  close(): Promise<void> {
    return this.inner.close();
  }

  defineSlots(space: SlotSpace): void {
    const entries = new Map<number, SlotEntry>();
    this.#tables.set(space.type.key, { table: new SlotTable(space, entries), entries });
  }

  setSlot(type: Name, slot: number, entry: SlotEntry): void {
    this.#tables.get(type.key)?.entries.set(slot, entry);
  }

  slotTable(type: Name): SlotTable | undefined {
    return this.#tables.get(type.key)?.table;
  }

  slotOwner(type: Name, slot: number): SlotEntry | undefined {
    const entry = this.#tables.get(type.key)?.entries.get(slot);
    return entry === undefined || entry.isFree ? undefined : entry;
  }

  slotOwners(type: Name): Set<number> {
    return this.#tables.get(type.key)?.table.owners ?? new Set();
  }

  get slotEvents(): AsyncIterable<SlotEvent> {
    return { [Symbol.asyncIterator]: async function* () {} };
  }

  async locateSlot(_type: Name, slot: number): Promise<SlotEntry | undefined> {
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
      case Mode.collect:
        void readAll(channel.readable, (d) => this.received.push(text(d)));
        break;
    }
  }
}

/**
 * Stands in for `Switchboard.openChannelToSlot` until the node routes shard
 * slots: the first channel goes to the owner in the table, the `MOVED`
 * retry is the one the node will use.
 */
async function openChannelToSlot(
  router: Switchboard,
  type: Name,
  slot: number,
  options: { payload?: Uint8Array; where?: (record: ServiceRecord) => boolean } = {},
): Promise<SlotChannel> {
  const header = new ChannelAddress({
    type,
    shard: slot,
    ...(options.payload === undefined ? {} : { payload: options.payload }),
  });
  const resolver = router.resolver as LocatingResolver;
  const owner = resolver.slotOwner(type, slot)?.owner ?? 0;
  const implicitPayload = options.payload === undefined;
  const { channel: first, header: selected } = await router.openSelected(
    new ServiceAddress(type, owner),
    header,
    { where: options.where, implicitPayload, mayRelay: true },
  );
  const rejectedBy = selected.instance;
  return new SlotChannel(
    type,
    slot,
    first,
    (moved) =>
      reopenAtSlotOwner(router, header, rejectedBy, moved, {
        where: options.where,
        implicitPayload,
        mayRelay: true,
      }),
    { maxHeldBytes: router.muxOptions.initialWindow },
  );
}

/** The subframes of a slot channel, read one by one as text. */
class Subframes {
  readonly #reader: ReadableStreamDefaultReader<Uint8Array>;

  constructor(channel: SlotChannel) {
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
function greeting(channel: SlotChannel): Promise<string> {
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

  describe('routing', () => {
    // The node's slot routing (`selectAndConnect` with a SlotResolver,
    // `resolveSlotOwner`, the proxy) lands with the node's slot-routed opens.
    test.todo('without a slot table: sorted[slot mod n]');
    test.todo('to the owner, the old owner while migrating');
    test.todo('a free slot: LOCATE in a managed space');
    test.todo('a free slot: UNAVAILABLE in a static space, no LOCATE');
    test.todo('an owner that registered while the slot was located');
    test.todo('a slot outside the space: OUT_OF_RANGE');
    test.todo('an owner missing from the service table or unreachable: UNAVAILABLE');
    test.todo('resolveSlotOwner');
    test.todo('a proxy routes by slot');
  });

  describe('MOVED', () => {
    beforeEach(() => {
      define();
      resolver.setSlot(svc, 1, SlotEntry.owned(1, { epoch: 1 }));
    });

    test('retried once at the owner the reason names', async () => {
      a.mode = Mode.moved;
      a.moved = new MovedStatus({ owner: 2, epoch: 2 });
      const channel = await openChannelToSlot(router, svc, 1);
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
      const channel = await openChannelToSlot(router, svc, 1);
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
      const channel = await openChannelToSlot(router, svc, 1);
      await channel.channel.done;
      await rejectsWith(channel.whenWritable, StatusCode.failedPrecondition);
      expect((await channel.done).code).toBe(StatusCode.moved);
    });

    test('the open payload is sent again', async () => {
      a.mode = Mode.moved;
      a.moved = new MovedStatus({ owner: 2, epoch: 2 });
      const channel = await openChannelToSlot(router, svc, 1, { payload: bytes('token') });
      expect(await greeting(channel)).toBe('B 1');
      expect(a.opened.map((o) => text(o.payload))).toEqual(['token']);
      expect(b.opened.map((o) => text(o.payload))).toEqual(['token']);
    });

    test('a reason without an owner: the router asks', async () => {
      a.mode = Mode.moved;
      resolver.answer = () => SlotEntry.owned(2, { epoch: 2 });
      const channel = await openChannelToSlot(router, svc, 1);
      expect(await greeting(channel)).toBe('B 1');
      expect(resolver.located).toEqual([1]);
    });

    test('a newer table entry wins over the reason', async () => {
      a.mode = Mode.moved;
      a.moved = new MovedStatus({ owner: 9, epoch: 2 });
      const channel = await openChannelToSlot(router, svc, 1);
      resolver.setSlot(svc, 1, SlotEntry.owned(2, { epoch: 3 }));
      expect(await greeting(channel)).toBe('B 1');
    });

    test('a newer table entry naming the rejecting owner wins too', async () => {
      a.mode = Mode.movedOnce;
      a.moved = new MovedStatus({ owner: 2, epoch: 2 });
      const channel = await openChannelToSlot(router, svc, 1);
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
      const channel = await openChannelToSlot(router, svc, 1);
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
      const channel = await openChannelToSlot(router, svc, 1);
      expect((await channel.done).code).toBe(StatusCode.moved);
      expect(a.opened).toHaveLength(1);
      expect(b.opened).toHaveLength(1);
    });

    test('never on RELOCATED: the slot moved after work started', async () => {
      a.mode = Mode.relocated;
      a.moved = new MovedStatus({ owner: 2, epoch: 2 });
      const channel = await openChannelToSlot(router, svc, 1);
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
      const channel = await openChannelToSlot(router, svc, 1);
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
      const channel = await openChannelToSlot(router, svc, 1);
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
        const channel = await openChannelToSlot(small, svc, 1);
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
        const channel = await openChannelToSlot(small, svc, 1);
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

    // Talk over slot channels needs the Talk channel (and its bulk routes);
    // the proxy needs `proxyHandler`. Both land in other threads.
    test.todo('Talk: a server that speaks first gets the retry');
    test.todo('Talk: bulk payloads both ways on the retried slot channel');
    test.todo('through a resolver without slot tables: the same address once more');
  });
});
