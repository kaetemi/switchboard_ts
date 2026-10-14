/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Port of the Dart `test/integration/sharding/zone_servers_test.dart`, use
// case 4 of the wiki page "Polyverse Switchboard Sharding": AI zone servers
// (`zone`, static with explicit MIGRATE, holder-only). An operator moves a
// zone to a fresh instance during play. The NPC state (a counter advanced
// by a timer) moves over a direct channel during DRAIN. Players attached
// through a frontend proxy keep their channels: a player attached before
// the move has its requests forwarded as message chains, a player that
// attaches while the zone is locked has its channel queued and piped to
// the new server. Over TCP and WebSocket as in Dart, and over in-process
// `mem://` endpoints.

import { afterAll, describe, expect, test } from 'vitest';
import { ServiceAddress } from '../../../src/core/address/index.ts';
import { Name } from '../../../src/core/name.ts';
import { AssignResult } from '../../../src/core/naming/naming-client-slots.ts';
import {
  migrationPhaseName,
  SlotEntry,
  SlotMode,
} from '../../../src/core/naming/naming-protocol.ts';
import {
  type SlotDrainOptions,
  SlotGateState,
  SlotLifecycle,
  type SlotLoadOptions,
} from '../../../src/core/naming/slot-gate.ts';
import { StatusCode, SwitchboardError } from '../../../src/core/status.ts';
import type { ChannelHandler, IncomingChannel } from '../../../src/core/switchboard/contract.ts';
import type { MeshNode } from '../../../src/core/switchboard/mesh-node.ts';
import { proxyHandler } from '../../../src/core/switchboard/proxy.ts';
import { EndpointResolver } from '../../../src/core/switchboard/resolver.ts';
import type { Switchboard } from '../../../src/core/switchboard/switchboard.ts';
import type { TalkChannel, TalkMessage } from '../../../src/core/talk/contract.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';
import { expectNoMemoryListeners } from '../mesh-support.ts';
import { Cluster, delay, errorCode, retryable, until, within } from './cluster.ts';

const zone = Name.of('zone');

/** One zone's NPC state on its server. */
class Zone {
  counter: number;
  timer: ReturnType<typeof setInterval> | undefined;
  readonly waiting: [number, Deferred<void>][] = [];

  constructor(counter: number) {
    this.counter = counter;
  }
}

/** A zone server. [ticks] (shared by every server) records each counter step as [server, zone, value]. */
class ZoneServer extends SlotLifecycle {
  readonly name: string;
  readonly ticks: [string, number, number][];
  readonly zones = new Map<number, Zone>();
  /** NPC state transferred by the previous server, until ASSIGN. */
  readonly arriving = new Map<number, number>();
  /** When set, DRAIN waits for it (lets the test act while locked). */
  holdDrain: Deferred<void> | undefined;

  constructor(name: string, ticks: [string, number, number][]) {
    super();
    this.name = name;
    this.ticks = ticks;
  }

  #run(slot: number, z: Zone): void {
    z.timer = setInterval(() => {
      z.counter++;
      this.ticks.push([this.name, slot, z.counter]);
      for (const waiter of [...z.waiting]) {
        if (z.counter > waiter[0]) {
          z.waiting.splice(z.waiting.indexOf(waiter), 1);
          waiter[1].resolve();
        }
      }
    }, 5);
  }

  load(slot: number, { holder }: SlotLoadOptions): Promise<AssignResult> {
    let counter: number;
    if (holder === 0 || holder === this.gate.instance) {
      counter = 0;
    } else {
      // Moved here: the state arrived during DRAIN.
      const transferred = this.arriving.get(slot);
      this.arriving.delete(slot);
      if (transferred === undefined) {
        return Promise.reject(SwitchboardError.of(StatusCode.unavailable, 'no state'));
      }
      counter = transferred;
    }
    const z = new Zone(counter);
    this.zones.set(slot, z);
    this.#run(slot, z);
    return Promise.resolve(AssignResult.holding);
  }

  override async drain(slot: number, { to, context }: SlotDrainOptions): Promise<void> {
    await this.holdDrain?.promise;
    const z = this.zones.get(slot) as Zone;
    clearInterval(z.timer);
    // The transfer starts: how long it may take is known now.
    context?.extend({ deadline: 10_000 });
    const talk = await this.gate.switchboard.openTalk(new ServiceAddress(zone, to));
    try {
      await talk.request('XFER', `${slot} ${z.counter}`);
    } finally {
      await talk.close();
    }
  }

  override unload(slot: number): void {
    clearInterval(this.zones.get(slot)?.timer);
    this.zones.delete(slot);
  }

  serve(channel: IncomingChannel, slot: number): void {
    // Players stay attached; their requests are gated one by one.
    this.gate.detach(channel);
    void channel.talk().serve((m) => this.gate.serveRequest(m, slot, (m) => this.#next(m, slot)));
  }

  /** `NEXT after`: the counter value following `after`, once reached. */
  async #next(m: TalkMessage, slot: number): Promise<void> {
    const after = Number(m.text());
    const z = this.zones.get(slot) as Zone;
    if (z.counter <= after) {
      const reached = new Deferred<void>();
      z.waiting.push([after, reached]);
      await reached.promise;
    }
    if (m.canReply) {
      void m.reply(`${this.name} ${after + 1}`);
    }
  }

  /** `XFER zone counter` from the previous server. */
  readonly serveTransfer: ChannelHandler = (channel) => {
    void channel.talk().serve((m) => {
      const [slot = '', counter = ''] = m.text().split(' ');
      this.arriving.set(Number(slot), Number(counter));
      void m.reply('ok');
    });
  };
}

/**
 * A player attached to a zone through the frontend: asks for every counter
 * value in turn; reattaches when told the zone moved.
 */
class Player {
  readonly node: Switchboard;
  readonly slot: number;
  #talk: TalkChannel | undefined;
  /** [server, value, channel number] of every answer. */
  readonly seen: [string, number, number][] = [];
  channels = 0;
  readonly reattached: (number | undefined)[] = [];
  #stop = false;
  #done: Promise<void> = Promise.resolve();
  #failure: unknown;

  constructor(node: Switchboard, slot: number) {
    this.node = node;
    this.slot = slot;
  }

  get talk(): TalkChannel {
    if (this.#talk === undefined) {
      throw new Error('not attached');
    }
    return this.#talk;
  }

  get last(): number {
    return this.seen.at(-1)?.[1] ?? 0;
  }

  async attach(): Promise<void> {
    this.#talk = await this.node.openTalkToSlot(zone, this.slot);
    this.channels++;
  }

  start(): void {
    this.#done = (async () => {
      while (!this.#stop) {
        try {
          const answer = await this.talk.request('NEXT', `${this.last}`);
          const [server = '', value = ''] = answer.text().split(' ');
          this.seen.push([server, Number(value), this.channels]);
        } catch (e) {
          if (this.#stop || !retryable(e)) {
            throw e;
          }
          this.reattached.push(errorCode(e));
          void this.talk.close();
          await this.attach();
        }
      }
      await this.talk.close();
    })();
    this.#done.catch((e: unknown) => {
      this.#failure = e;
    });
  }

  /** Throws what stopped the player, if anything did. */
  check(): void {
    if (this.#failure !== undefined) {
      throw this.#failure;
    }
  }

  stop(): Promise<void> {
    this.#stop = true;
    return within(this.#done);
  }
}

afterAll(() => {
  expectNoMemoryListeners();
});

for (const scheme of ['tcp', 'ws', 'mem'] as const) {
  describe(scheme, () => {
    test(`${scheme}: a hot zone moves to a fresh server during play`, async () => {
      const cluster = new Cluster(scheme);
      await cluster.start();
      const ticks: [string, number, number][] = [];
      const startServer = async (name: string, id: number): Promise<[MeshNode, ZoneServer]> => {
        const mesh = await cluster.member();
        const app = new ZoneServer(name, ticks);
        await within(
          mesh.publishSharded(zone, app, {
            count: 4,
            mode: SlotMode.static,
            instance: id,
            forwardGrace: 150,
            noSlotHandler: app.serveTransfer,
          }),
        );
        return [mesh, app];
      };

      const [z1, app1] = await startServer('Z1', 0x31);
      for (let slot = 0; slot < 4; slot++) {
        await within(z1.claimSlot(zone, slot, { holding: true }));
      }
      const [z3, app3] = await startServer('Z3', 0x33);

      // The frontend proxies players to the zone's server.
      const frontend = await cluster.member();
      frontend.switchboard.catchAll = proxyHandler(frontend.switchboard);
      const endpoint = frontend.switchboard.listeningEndpoints[0] as string;
      const player = async (): Promise<Player> => {
        const node = await cluster.node({
          resolver: new EndpointResolver(endpoint),
          listen: false,
        });
        return new Player(node, 2);
      };

      const early = await player();
      await early.attach();
      early.start();
      await until(() => {
        early.check();
        return early.seen.length >= 5;
      });

      // The operator moves zone 2; the old server holds it locked.
      const hold = new Deferred<void>();
      app1.holdDrain = hold;
      const phases = (async () => {
        const names: string[] = [];
        for await (const item of frontend.migrateSlot(zone, 2, { to: 0x33 })) {
          names.push(migrationPhaseName(item.phase));
        }
        return names;
      })();
      phases.catch(() => {});
      await until(() => z1.gates.get(zone)?.stateOf(2) === SlotGateState.locked);
      // A player attaching now is queued at the old server.
      const joiner = await player();
      await joiner.attach();
      const first = joiner.talk.request('NEXT', `${early.last}`);
      await delay(20);
      expect(app3.zones.size).toBe(0);
      hold.resolve();
      expect(await within(phases)).toEqual(['draining', 'assigning', 'forwarding', 'done']);
      const [server = '', value = ''] = (await within(first)).text().split(' ');
      expect(server).toBe('Z3');
      joiner.seen.push([server, Number(value), 1]);
      joiner.start();

      // The early player's requests are forwarded during the grace period,
      // then it is told MOVED and reattaches straight to Z3.
      await until(() => {
        early.check();
        return early.reattached.length > 0;
      });
      await until(() => {
        early.check();
        joiner.check();
        return early.seen.at(-1)?.[2] === 2 && joiner.seen.length >= 3;
      });
      await early.stop();
      await joiner.stop();
      expect(early.reattached).toEqual([StatusCode.moved]);
      const values = early.seen.map(([, v]) => v);
      expect(values).toEqual(Array.from({ length: values.length }, (_, i) => i + 1));
      const servers = early.seen.map(([s]) => s);
      const moved = servers.indexOf('Z3');
      expect(moved).toBeGreaterThan(0);
      expect(new Set(servers.slice(moved))).toEqual(new Set(['Z3']));
      // The first channel was kept across the move.
      expect(early.seen.some(([s, , c]) => s === 'Z3' && c === 1)).toBe(true);
      expect(joiner.channels).toBe(1);
      expect(joiner.reattached).toEqual([]);
      expect(joiner.seen.every(([s]) => s === 'Z3')).toBe(true);
      const joined = joiner.seen.map(([, v]) => v);
      expect(joined).toEqual(joined.map((_, i) => (joined[0] as number) + i));

      // The NPCs never skipped or repeated a step.
      const steps = ticks.filter(([, slot]) => slot === 2).map(([, , v]) => v);
      expect(steps).toEqual(Array.from({ length: steps.length }, (_, i) => i + 1));
      const lastOnZ1 = ticks.findLast(([s, slot]) => s === 'Z1' && slot === 2)?.[2] ?? 0;
      const firstOnZ3 = ticks.find(([s, slot]) => s === 'Z3' && slot === 2)?.[2];
      expect(firstOnZ3).toBe(lastOnZ1 + 1);

      // Z3 holds the zone now; Z1 dropped it after the grace period.
      await until(() => !app1.zones.has(2));
      const entry = frontend.client.slotOwner(zone, 2);
      expect(entry?.equals(SlotEntry.owned(0x33, { holder: 0x33, epoch: 2 })), `${entry}`).toBe(
        true,
      );
      expect([...(z1.gates.get(zone)?.servedSlots.keys() ?? [])].sort()).toEqual([0, 1, 3]);
      expect([...(z3.gates.get(zone)?.servedSlots ?? [])]).toEqual([[2, 2]]);
    });
  });
}
