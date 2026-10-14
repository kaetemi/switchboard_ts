/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Port of the Dart `test/integration/sharding/kv_store_test.dart`, use case
// 1 of the wiki page "Polyverse Switchboard Sharding": a key-value store
// (`kv`, eager managed, holder-only). The wiki has N = 1024; 64 slots keep
// the test fast and exercise the same paths (every migration is the same
// hand-over, only more of them). Over TCP and in-process `mem://`
// endpoints, as in Dart.
//
// Each instance keeps its slots in a "disk" that survives a reboot. A new
// owner fetches a slot from its holder over a channel without a shard slot
// (the gate's noSlotHandler). Clients open one channel per operation to
// the slot's owner and write, per key, a sequence number that must come
// back in order: no operation may be lost, applied twice or reordered while
// slots move. A node that leaves the mesh hands its slots over before it
// stops, so nothing it holds is lost.

import { afterAll, describe, expect, test } from 'vitest';
import { ServiceAddress } from '../../../src/core/address/index.ts';
import { Name } from '../../../src/core/name.ts';
import { AssignResult } from '../../../src/core/naming/naming-client-slots.ts';
import { SlotState } from '../../../src/core/naming/naming-protocol.ts';
import {
  type SlotGate,
  SlotLifecycle,
  type SlotLoadOptions,
} from '../../../src/core/naming/slot-gate.ts';
import { slotForText } from '../../../src/core/naming/slot-key.ts';
import { Status, StatusCode } from '../../../src/core/status.ts';
import type { ChannelHandler, IncomingChannel } from '../../../src/core/switchboard/contract.ts';
import type { MeshNode } from '../../../src/core/switchboard/mesh-node.ts';
import type { Switchboard } from '../../../src/core/switchboard/switchboard.ts';
import { expectNoMemoryListeners } from '../mesh-support.ts';
import {
  Cluster,
  delay,
  MigrationWatch,
  retryable,
  seededRandom,
  until,
  within,
} from './cluster.ts';

const kv = Name.of('kv');
const slots = 64;

/** slot -> key -> the sequence numbers applied to the key, in order. */
type Disk = Map<number, Map<string, number[]>>;

/** Counters shared by every instance of the store. */
class Stats {
  /** `load` calls by kind: `fresh`, `reclaim` (holder is this instance), `fetch` (from another holder). */
  readonly loads = new Map<string, number>();
  /** Puts whose sequence number was already applied (a client retry after a lost answer); answered without applying. */
  replays = 0;
  /** Puts that skipped a sequence number: never expected. */
  gaps = 0;

  count(kind: string): void {
    this.loads.set(kind, (this.loads.get(kind) ?? 0) + 1);
  }
}

/** One instance of the store. */
class KvStore extends SlotLifecycle {
  readonly disk: Disk;
  readonly stats: Stats;

  constructor(disk: Disk, stats: Stats) {
    super();
    this.disk = disk;
    this.stats = stats;
  }

  async load(slot: number, { holder }: SlotLoadOptions): Promise<AssignResult> {
    if (holder === this.gate.instance && this.disk.has(slot)) {
      this.stats.count('reclaim');
    } else if (holder === 0 || holder === this.gate.instance) {
      this.stats.count('fresh');
      this.disk.set(slot, new Map());
    } else {
      this.stats.count('fetch');
      const talk = await this.gate.switchboard.openTalk(new ServiceAddress(kv, holder));
      try {
        const answer = await talk.request('FETCH', `${slot}`);
        const entries = Object.entries(answer.json() as Record<string, number[]>);
        this.disk.set(slot, new Map(entries));
      } finally {
        await talk.close();
      }
    }
    return AssignResult.holding;
  }

  /**
   * The slot stays here for the new owner to fetch; a short pause stands
   * for flushing it, so that the lock is long enough for operations to
   * queue behind it.
   */
  override drain(): Promise<void> {
    return delay(3);
  }

  override unload(slot: number): void {
    // Kept while this instance is still the holder (it released the slot
    // keeping its storage); dropped once the storage moved on.
    const entry = this.gate.client.slotTable(kv)?.get(slot);
    if (entry?.holder !== this.gate.instance) {
      this.disk.delete(slot);
    }
  }

  override discard(discarded: readonly number[]): void {
    for (const slot of discarded) {
      this.disk.delete(slot);
    }
  }

  serve(channel: IncomingChannel, slot: number): void {
    void channel.talk().serve((m) => {
      const [key = '', ...rest] = m.text().split(' ');
      if (slotForText(key, slots) !== slot) {
        void m.replyAbort(Status.of(StatusCode.invalidArgument, 'wrong slot'));
        return;
      }
      const stored = this.disk.get(slot) as Map<string, number[]>;
      let history = stored.get(key);
      if (history === undefined) {
        history = [];
        stored.set(key, history);
      }
      const last = history.at(-1) ?? 0;
      switch (m.procedureName) {
        case 'PUT': {
          const seq = Number(rest[0]);
          if (seq === last + 1) {
            history.push(seq);
          } else if (seq <= last) {
            this.stats.replays++;
          } else {
            this.stats.gaps++;
          }
          void m.reply(`${seq}`);
          break;
        }
        case 'GET':
          void m.reply(`${last}`);
          break;
      }
    });
  }

  /** Serves `FETCH slot` to a new owner of a slot this instance holds. */
  readonly serveTransfer: ChannelHandler = (channel) => {
    void channel.talk().serve((m) => {
      const slot = Number(m.text());
      void m.reply(JSON.stringify(Object.fromEntries(this.disk.get(slot) ?? new Map())));
    });
  };
}

/** Starts a store instance with a stable id and the given [disk]. */
async function startStore(
  cluster: Cluster,
  id: number,
  disk: Disk,
  stats: Stats,
): Promise<[MeshNode, SlotGate]> {
  const mesh = await cluster.member();
  const store = new KvStore(disk, stats);
  const gate = await within(
    mesh.publishSharded(kv, store, {
      count: slots,
      instance: id,
      holding: disk.keys(),
      forwardGrace: 200,
      noSlotHandler: store.serveTransfer,
    }),
  );
  return [mesh, gate];
}

/** A client issuing operations on its own keys, one channel per operation. */
class Client {
  readonly node: Switchboard;
  readonly keys: readonly string[];
  readonly random: (n: number) => number;
  readonly written = new Map<string, number>();
  operations = 0;
  retries = 0;
  #stop = false;
  #done: Promise<void> = Promise.resolve();
  #failure: unknown;

  constructor(node: Switchboard, keys: readonly string[], random: (n: number) => number) {
    this.node = node;
    this.keys = keys;
    this.random = random;
  }

  async call(procedure: string, key: string, arg?: string): Promise<string> {
    const slot = slotForText(key, slots);
    for (;;) {
      try {
        const talk = await this.node.openTalkToSlot(kv, slot);
        try {
          const answer = await talk.request(procedure, arg === undefined ? key : `${key} ${arg}`);
          return answer.text();
        } finally {
          void talk.close();
        }
      } catch (e) {
        if (!retryable(e)) {
          throw e;
        }
        this.retries++;
        await delay(5);
      }
    }
  }

  start(): this {
    this.#done = (async () => {
      while (!this.#stop) {
        const key = this.keys[this.random(this.keys.length)] as string;
        const seq = (this.written.get(key) ?? 0) + 1;
        expect(await this.call('PUT', key, `${seq}`)).toBe(`${seq}`);
        this.written.set(key, seq);
        expect(await this.call('GET', key), key).toBe(`${seq}`);
        this.operations++;
      }
    })();
    this.#done.catch((e: unknown) => {
      this.#failure = e;
    });
    return this;
  }

  /** The operations so far; throws what stopped the client, if anything did. */
  get progress(): number {
    if (this.#failure !== undefined) {
      throw this.#failure;
    }
    return this.operations;
  }

  stop(): Promise<void> {
    this.#stop = true;
    return within(this.#done);
  }
}

/** The keys of [map] as a sorted list. */
function sortedKeys(map: ReadonlyMap<number, unknown>): number[] {
  return [...map.keys()].sort((a, b) => a - b);
}

// Every node is closed by its cluster's tear-down, which releases its ids.
afterAll(() => {
  expectNoMemoryListeners();
});

for (const scheme of ['tcp', 'mem'] as const) {
  describe(scheme, () => {
    test(`${scheme} kv: slots move one at a time while clients write; a reboot gets its slots back without transfer`, async () => {
      const cluster = new Cluster(scheme);
      await cluster.start();
      const stats = new Stats();
      const disks = new Map<number, Disk>([
        [0x11, new Map()],
        [0x12, new Map()],
        [0x13, new Map()],
      ]);
      const diskOf = (id: number): Disk => disks.get(id) as Disk;
      const clientMesh = await cluster.member({ listen: false });
      const watch = new MigrationWatch(clientMesh.client, kv);
      const shares = (): Map<number, number> => {
        const counts = new Map<number, number>();
        for (const entry of clientMesh.client.slotTable(kv)?.entries.values() ?? []) {
          if (entry.state === SlotState.owned) {
            counts.set(entry.owner, (counts.get(entry.owner) ?? 0) + 1);
          }
        }
        return counts;
      };

      // Two instances share the space.
      const [, second] = await Promise.all([
        startStore(cluster, 0x11, diskOf(0x11), stats),
        startStore(cluster, 0x12, diskOf(0x12), stats),
      ]);
      await until(
        () => watch.idle && shares().get(0x11) === slots / 2 && shares().get(0x12) === slots / 2,
        'two instances with half the slots each',
      );

      // Clients keep writing and reading.
      const random = seededRandom(7);
      const clients = [0, 1, 2].map((c) =>
        new Client(
          clientMesh.switchboard,
          Array.from({ length: 24 }, (_, k) => `c${c}-k${k}`),
          random,
        ).start(),
      );
      await until(() => clients.every((c) => c.progress >= 5));

      // A third instance joins: slots migrate to it one at a time.
      const movesBefore = watch.moves.length;
      const third = await startStore(cluster, 0x13, diskOf(0x13), stats);
      await until(
        () => watch.idle && (shares().get(0x13) ?? 0) >= Math.trunc(slots / 3),
        'the third instance has its share',
      );
      const moved = watch.moves.slice(movesBefore);
      expect(moved.length).toBeGreaterThanOrEqual(Math.trunc(slots / 3));
      expect(moved.every((m) => m[2] === 0x13)).toBe(true);
      expect(watch.maxConcurrent, 'one migration at a time').toBe(1);
      expect(third[1].servedSlots.size).toBe(shares().get(0x13));
      const opsAtJoin = clients.map((c) => c.operations);
      await until(() => clients.every((c, i) => c.progress - (opsAtJoin[i] as number) >= 5));

      // Old copies are dropped once the forwarding grace is over.
      await until(
        () => [...disks.values()].reduce((n, d) => n + d.size, 0) === slots,
        'one copy of every slot',
      );

      // While slots moved, no PUT reached an owner twice: MOVED means
      // nothing was processed, and a client only resends after MOVED.
      expect(stats.replays, 'no PUT applied twice').toBe(0);

      // The second instance reboots with its id and its disk.
      const held = sortedKeys(diskOf(0x12));
      expect(held).toEqual(clientMesh.client.slotTable(kv)?.slotsOf(0x12));
      const before = new Map(stats.loads);
      await cluster.crash(second[0]);
      await until(
        () => !clientMesh.client.slotOwners(kv).has(0x12),
        'the slots of the crashed instance are free',
      );
      // Holder-only: they wait for their holder.
      await delay(50);
      expect([...clientMesh.client.slotOwners(kv)].sort()).toEqual([0x11, 0x13]);
      const [rebooted] = await startStore(cluster, 0x12, diskOf(0x12), stats);
      await until(
        () => shares().get(0x12) === held.length,
        'the rebooted instance has its slots back',
      );
      expect(clientMesh.client.slotTable(kv)?.slotsOf(0x12)).toEqual(held);
      // Served before the naming service published them.
      expect(sortedKeys(rebooted.gates.get(kv)?.servedSlots ?? new Map())).toEqual(held);
      expect(stats.loads.get('reclaim')).toBe((before.get('reclaim') ?? 0) + held.length);
      expect(stats.loads.get('fetch'), 'no transfer').toBe(before.get('fetch'));
      expect(stats.loads.get('fresh')).toBe(before.get('fresh'));

      // Operations go on; then everything written is there, once, in order.
      const opsAtReboot = clients.map((c) => c.operations);
      await until(() => clients.every((c, i) => c.progress - (opsAtReboot[i] as number) >= 5));
      for (const client of clients) {
        await client.stop();
      }
      const table = clientMesh.client.slotTable(kv);
      for (const client of clients) {
        for (const [key, last] of client.written) {
          const slot = slotForText(key, slots);
          const owner = table?.get(slot).owner ?? 0;
          expect(diskOf(owner).get(slot)?.get(key)).toEqual(
            Array.from({ length: last }, (_, i) => i + 1),
          );
        }
      }
      expect(stats.gaps).toBe(0);
      // The crash may cut a PUT whose answer was on its way: the client
      // resends it, and the store answers without applying it again. At
      // most one per client.
      expect(stats.replays).toBeLessThanOrEqual(clients.length);
    });

    test(`${scheme} kv: a node that leaves hands its slots over before it stops; no key is lost`, async () => {
      const cluster = new Cluster(scheme);
      await cluster.start({ holderGrace: 300 });
      const stats = new Stats();
      const d11: Disk = new Map();
      const d12: Disk = new Map();
      const clientMesh = await cluster.member({ listen: false });
      const table = clientMesh.client;
      await startStore(cluster, 0x11, d11, stats);
      const [leaving, gate] = await startStore(cluster, 0x12, d12, stats);
      await until(
        () =>
          table.slotTable(kv)?.slotsOf(0x11).length === slots / 2 &&
          table.slotTable(kv)?.slotsOf(0x12).length === slots / 2,
        'two instances with half the slots each',
      );
      // One key in every slot.
      const keys = new Map<number, string>();
      for (let i = 0; keys.size < slots; i++) {
        const slot = slotForText(`k${i}`, slots);
        if (!keys.has(slot)) {
          keys.set(slot, `k${i}`);
        }
      }
      const client = new Client(clientMesh.switchboard, [], seededRandom(1));
      for (const key of keys.values()) {
        expect(await client.call('PUT', key, '1')).toBe('1');
      }
      const held = sortedKeys(gate.servedSlots);
      expect(held).toHaveLength(slots / 2);
      const fetches = stats.loads.get('fetch') ?? 0;

      const report = await leaving.leave();
      await leaving.switchboard.close();
      expect([...report.handedOver]).toEqual([[kv.key, held]]);
      expect(report.released.size).toBe(0);
      expect(report.dropped.size).toBe(0);
      expect(report.isComplete).toBe(true);
      // Taken over, fetched while the leaving node was still up.
      expect(stats.loads.get('fetch')).toBe(fetches + held.length);
      await until(
        () => table.slotTable(kv)?.slotsOf(0x11).length === slots,
        'every slot on the remaining instance',
      );
      for (const [slot, key] of keys) {
        expect(await client.call('GET', key), `slot ${slot}`).toBe('1');
        expect(d11.get(slot)?.get(key)).toEqual([1]);
      }
      expect(stats.replays).toBe(0);
      expect(stats.gaps).toBe(0);
    });
  });
}
