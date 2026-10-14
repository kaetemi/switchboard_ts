/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Port of the Dart `test/integration/sharding/per_user_workers_test.dart`,
// use case 3 of the wiki page "Polyverse Switchboard Sharding": per-user
// workers (`userq`, lazy managed, holder-only, N = 65536), with the layered
// deployment of the same page: a web frontend and a Discord frontend both
// route user actions by user id and land on the same worker. Over TCP as
// in Dart, and over in-process `mem://` endpoints.
//
// A user's slot is assigned when the user first shows up (LOCATE). Each
// user's job queue runs on the owner, one job at a time. When a second
// worker starts, busy users move over one at a time: a job in progress
// completes on the old worker, the next one is answered by the new worker,
// and the user's job count goes on without a gap.

import { afterAll, describe, expect, test } from 'vitest';
import { ServiceAddress } from '../../../src/core/address/index.ts';
import { Name } from '../../../src/core/name.ts';
import {
  AssignResult,
  type SlotHandler,
  type SlotRequestContext,
} from '../../../src/core/naming/naming-client-slots.ts';
import {
  type AssignRequest,
  type DrainRequest,
  type ForwardRequest,
  type ResumeRequest,
  SlotMode,
} from '../../../src/core/naming/naming-protocol.ts';
import { SlotLifecycle, type SlotLoadOptions } from '../../../src/core/naming/slot-gate.ts';
import { slotForText } from '../../../src/core/naming/slot-key.ts';
import { Status, StatusCode } from '../../../src/core/status.ts';
import type { ChannelHandler, IncomingChannel } from '../../../src/core/switchboard/contract.ts';
import type { MeshNode } from '../../../src/core/switchboard/mesh-node.ts';
import { proxyHandler } from '../../../src/core/switchboard/proxy.ts';
import { EndpointResolver } from '../../../src/core/switchboard/resolver.ts';
import type { Switchboard } from '../../../src/core/switchboard/switchboard.ts';
import { expectNoMemoryListeners } from '../mesh-support.ts';
import { Cluster, delay, MigrationWatch, retryable, until, within } from './cluster.ts';

const userq = Name.of('userq');
const web = Name.of('web');
const discord = Name.of('discord');
const slots = 65536;

function slotOf(user: string): number {
  return slotForText(`user:${user}`, slots);
}

/** The queue of one user on its worker. */
class UserQueue {
  /** Jobs completed so far: the state that moves with the user. */
  completed: number;
  tail: Promise<void> = Promise.resolve();

  constructor(completed: number) {
    this.completed = completed;
  }
}

/** A worker instance. [log] is shared by every worker and frontend. */
class Worker extends SlotLifecycle {
  static readonly job = 25;
  static readonly slowJob = 300;

  readonly name: string;
  readonly log: string[];
  readonly users = new Map<number, UserQueue>();
  /** Users whose next job is slow, and how many slow jobs have started. */
  readonly slowNext = new Set<string>();
  slowStarted = 0;

  constructor(name: string, log: string[]) {
    super();
    this.name = name;
    this.log = log;
  }

  async load(slot: number, { holder }: SlotLoadOptions): Promise<AssignResult> {
    let completed = 0;
    if (holder !== 0 && holder !== this.gate.instance) {
      const talk = await this.gate.switchboard.openTalk(new ServiceAddress(userq, holder));
      try {
        completed = Number((await talk.request('FETCH', `${slot}`)).text());
      } finally {
        await talk.close();
      }
    }
    this.users.set(slot, new UserQueue(completed));
    this.log.push(`${this.name} load ${slot} from ${holder}`);
    return AssignResult.holding;
  }

  override unload(slot: number): void {
    if (this.gate.client.slotTable(userq)?.get(slot).holder !== this.gate.instance) {
      this.users.delete(slot);
    }
  }

  serve(channel: IncomingChannel, slot: number): void {
    void channel.talk().serve((m) => {
      const [verb, user = ''] = m.text().split(' ');
      if (slotOf(user) !== slot) {
        void m.replyAbort(Status.of(StatusCode.invalidArgument, 'wrong slot'));
        return;
      }
      const queue = this.users.get(slot) as UserQueue;
      switch (verb) {
        case 'WHO':
          void m.reply(this.name);
          break;
        case 'JOB': {
          const done = queue.tail.then(async () => {
            this.log.push(`${this.name} start ${user} ${queue.completed + 1}`);
            const slow = this.slowNext.delete(user);
            if (slow) {
              this.slowStarted++;
            }
            await delay(slow ? Worker.slowJob : Worker.job);
            queue.completed++;
            this.log.push(`${this.name} end ${user} ${queue.completed}`);
            return queue.completed;
          });
          queue.tail = done.then(() => {});
          void done.then((n) => m.reply(`${this.name} ${n}`));
          break;
        }
      }
    });
  }

  /** `FETCH slot` from a new owner: the user's job count. */
  readonly serveTransfer: ChannelHandler = (channel) => {
    void channel.talk().serve((m) => {
      const slot = Number(m.text());
      void m.reply(`${this.users.get(slot)?.completed ?? 0}`);
    });
  };
}

/** Records the naming service's requests to a worker before its gates handle them. */
class Recording implements SlotHandler {
  readonly inner: SlotHandler;
  readonly name: string;
  readonly log: string[];

  constructor(inner: SlotHandler, name: string, log: string[]) {
    this.inner = inner;
    this.name = name;
    this.log = log;
  }

  onAssign(r: AssignRequest, c: SlotRequestContext): AssignResult | Promise<AssignResult> {
    return this.inner.onAssign(r, c);
  }

  onDrain(r: DrainRequest, c: SlotRequestContext): void | Promise<void> {
    this.log.push(`${this.name} DRAIN ${r.slot}`);
    return this.inner.onDrain(r, c);
  }

  onForward(r: ForwardRequest, c: SlotRequestContext): void | Promise<void> {
    return this.inner.onForward(r, c);
  }

  onResume(r: ResumeRequest, c: SlotRequestContext): void | Promise<void> {
    return this.inner.onResume(r, c);
  }

  onRevoke(type: Name, slot: number): void | Promise<void> {
    return this.inner.onRevoke?.(type, slot);
  }
}

async function startWorker(
  cluster: Cluster,
  name: string,
  id: number,
  log: string[],
  options: { capacity?: number } = {},
): Promise<[MeshNode, Worker]> {
  const mesh = await cluster.member();
  const worker = new Worker(name, log);
  await within(
    mesh.publishSharded(userq, worker, {
      count: slots,
      lazy: true,
      capacity: options.capacity ?? 1,
      instance: id,
      forwardGrace: 300,
      noSlotHandler: worker.serveTransfer,
    }),
  );
  mesh.client.slotHandler = new Recording(mesh.gates, name, log);
  return [mesh, worker];
}

/**
 * Acts for users: routes their actions to their worker, by table on a mesh
 * node, or through the web frontend's proxy on a web client.
 */
class Router {
  readonly node: Switchboard;

  constructor(node: Switchboard) {
    this.node = node;
  }

  async call(verb: string, user: string): Promise<string> {
    for (;;) {
      try {
        const talk = await this.node.openTalkToSlot(userq, slotOf(user));
        try {
          return (await talk.request(verb, `${verb} ${user}`)).text();
        } finally {
          void talk.close();
        }
      } catch (e) {
        if (!retryable(e)) {
          throw e;
        }
        await delay(5);
      }
    }
  }
}

/** The Discord frontend's own shard; it only routes here. */
class Shard extends SlotLifecycle {
  load(): Promise<AssignResult> {
    return Promise.resolve(AssignResult.notHolding);
  }

  serve(channel: IncomingChannel): void {
    void channel.reject(Status.ok);
  }
}

afterAll(() => {
  expectNoMemoryListeners();
});

for (const scheme of ['tcp', 'mem'] as const) {
  describe(scheme, () => {
    test('users move one at a time, each job in order; every frontend lands on the same worker', async () => {
      const cluster = new Cluster(scheme);
      await cluster.start();
      const log: string[] = [];

      // The web frontend has no sharding ("connect anywhere"): web clients
      // reach it through its endpoint, and its proxy routes their channels
      // to the user's worker by the table. The Discord frontend is sharded
      // itself, by guild, statically, and routes for its users directly.
      const webMesh = await cluster.member();
      await within(webMesh.publish(web, (incoming) => incoming.reject(Status.ok)));
      webMesh.switchboard.catchAll = proxyHandler(webMesh.switchboard, {
        allow: (address) => address.type?.equals(userq) === true,
      });
      const webClient = new Router(
        await cluster.node({
          resolver: new EndpointResolver(webMesh.switchboard.listeningEndpoints[0] as string),
          listen: false,
        }),
      );
      const discordMesh = await cluster.member();
      await within(
        discordMesh.publishSharded(discord, new Shard(), { count: 1, mode: SlotMode.static }),
      );
      await within(discordMesh.claimSlot(discord, 0));
      const discordFrontend = new Router(discordMesh.switchboard);

      const [, worker1] = await startWorker(cluster, 'W1', 0x21, log);
      const users = Array.from({ length: 8 }, (_, i) => `u${i}`);
      expect(new Set(users.map(slotOf)).size).toBe(users.length);
      const watch = new MigrationWatch(webMesh.client, userq);
      const table = webMesh.client;
      const slotsOf = (id: number): number[] => table.slotTable(userq)?.slotsOf(id) ?? [];
      expect(table.slotTable(userq)?.entries.size).toBe(0);

      // Users show up: their slots are assigned on first use. Every user
      // keeps two jobs submitted, so that its queue never runs dry: the
      // next job waits in the queue while one runs.
      const answers = new Map<string, string[]>(users.map((u) => [u, []]));
      const answersOf = (user: string): string[] => answers.get(user) as string[];
      let stop = false;
      let failure: unknown;
      const loops = users.flatMap((user) =>
        [0, 1].map(async () => {
          try {
            while (!stop) {
              answersOf(user).push(await webClient.call('JOB', user));
            }
          } catch (e) {
            failure ??= e;
            throw e;
          }
        }),
      );
      for (const loop of loops) {
        loop.catch(() => {});
      }
      const answered = (n: number, since?: ReadonlyMap<string, number>): boolean => {
        if (failure !== undefined) {
          throw failure;
        }
        return users.every((u) => answersOf(u).length >= (since?.get(u) ?? 0) + n);
      };
      await until(() => answered(2));
      expect(table.slotTable(userq)?.entries.size).toBe(users.length);
      expect(table.slotOwners(userq)).toEqual(new Set([0x21]));

      // A second worker: half the users move over, one at a time. It takes
      // slots once every user is in the middle of a (slow) job, so that the
      // hand-overs find jobs running.
      const [w2] = await startWorker(cluster, 'W2', 0x22, log, { capacity: 0 });
      for (const user of users) {
        worker1.slowNext.add(user);
      }
      await until(() => worker1.slowStarted === users.length);
      await within(w2.client.defineSlots(userq, { count: slots, lazy: true }));
      await until(
        () => watch.idle && slotsOf(0x22).length === users.length / 2,
        'four users on the new worker',
      );
      expect(watch.maxConcurrent).toBe(1);
      const moved = new Set(watch.moves.map(([slot]) => slot));
      expect(moved.size).toBe(users.length / 2);
      const counts = new Map(users.map((u) => [u, answersOf(u).length]));
      await until(() => answered(2, counts));
      stop = true;
      await within(Promise.all(loops));

      let midJob = 0;
      for (const user of users) {
        // Each job answered once, numbered in order without a gap.
        const seen = answersOf(user)
          .map((a) => {
            const [owner = '', n = ''] = a.split(' ');
            return [Number(n), owner] as const;
          })
          .sort((a, b) => a[0] - b[0]);
        expect(
          seen.map(([n]) => n),
          user,
        ).toEqual(Array.from({ length: seen.length }, (_, i) => i + 1));
        const owners = seen.map(([, owner]) => owner);
        if (!moved.has(slotOf(user))) {
          expect(new Set(owners), user).toEqual(new Set(['W1']));
          continue;
        }
        // Jobs 1..last done by W1, every later one by W2.
        const last = owners.indexOf('W2');
        expect(last, user).toBeGreaterThan(0);
        expect(new Set(owners.slice(last)), user).toEqual(new Set(['W2']));
        const drain = log.indexOf(`W1 DRAIN ${slotOf(user)}`);
        expect(drain, user).not.toBe(-1);
        // The next job starts on W2 only after the last one on W1 ended.
        expect(log.indexOf(`W2 start ${user} ${last + 1}`), user).toBeGreaterThan(
          log.indexOf(`W1 end ${user} ${last}`),
        );
        // Was a job running when the hand-over began?
        for (let n = 1; n <= last; n++) {
          if (
            log.indexOf(`W1 start ${user} ${n}`) < drain &&
            drain < log.indexOf(`W1 end ${user} ${n}`)
          ) {
            midJob++;
          }
        }
      }
      // The hand-overs found the users mid-job.
      expect(midJob).toBeGreaterThan(0);

      // The layered deployment: both frontends reach the same worker.
      for (const user of users) {
        const owner = table.slotOwner(userq, slotOf(user))?.owner;
        const name = owner === 0x21 ? 'W1' : 'W2';
        expect(await webClient.call('WHO', user)).toBe(name);
        expect(await discordFrontend.call('WHO', user)).toBe(name);
      }
    });
  });
}
