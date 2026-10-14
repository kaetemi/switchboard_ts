/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Port of the Dart `test/integration/sharding/discord_frontend_test.dart`,
// use case 2 of the wiki page "Polyverse Switchboard Sharding": Discord
// frontends (`discord`, static, stateless, N = the shard count from
// configuration). Each frontend claims the shards it is configured for;
// other services route a guild to its shard by `(guild_id >> 22) mod N`
// through the table. Over TCP and WebSocket as in Dart, and over
// in-process `mem://` endpoints.
//
// Guild ids are snowflakes beyond 2^53, so they are `bigint`s here.

import { afterAll, beforeEach, describe, expect, test } from 'vitest';
import { Name } from '../../../src/core/name.ts';
import { AssignResult } from '../../../src/core/naming/naming-client-slots.ts';
import { SlotEntry, SlotMode } from '../../../src/core/naming/naming-protocol.ts';
import { SlotLifecycle } from '../../../src/core/naming/slot-gate.ts';
import { Status, StatusCode, SwitchboardError } from '../../../src/core/status.ts';
import type { IncomingChannel } from '../../../src/core/switchboard/contract.ts';
import type { MeshNode } from '../../../src/core/switchboard/mesh-node.ts';
import { rejectsWith } from '../../support/node/harness.ts';
import { expectNoMemoryListeners } from '../mesh-support.ts';
import { Cluster, delay, until, within } from './cluster.ts';

const discord = Name.of('discord');

/** The shard of [guild] among [count]. */
function shardOf(guild: bigint, count: number): number {
  return Number((guild >> 22n) % BigInt(count));
}

/**
 * A Discord gateway frontend: holds the gateway connections of its shards
 * (nothing to load: the state is Discord's) and posts messages for the
 * guilds on them.
 */
class Frontend extends SlotLifecycle {
  readonly name: string;
  readonly shards = new Set<number>();
  readonly posted: string[] = [];

  constructor(name: string) {
    super();
    this.name = name;
  }

  load(slot: number): Promise<AssignResult> {
    this.shards.add(slot);
    return Promise.resolve(AssignResult.notHolding);
  }

  override unload(slot: number): void {
    this.shards.delete(slot);
  }

  serve(channel: IncomingChannel, slot: number): void {
    void channel.talk().serve((m) => {
      const [guild = '', message = ''] = m.text().split(' ');
      // A guild on another shard does not belong here.
      const count = this.gate.client.slotTable(discord)?.count ?? 0;
      if (shardOf(BigInt(guild), count) !== slot) {
        void m.replyAbort(Status.of(StatusCode.invalidArgument, 'wrong shard'));
        return;
      }
      this.posted.push(`${guild} ${message}`);
      void m.reply(`${this.name} shard ${slot}`);
    });
  }
}

/**
 * Starts a frontend configured for [shards] of [count]; like the real one,
 * it refuses to start (leaves) when a shard is taken.
 */
async function startFrontend(
  cluster: Cluster,
  name: string,
  config: { count: number; shards: readonly number[] },
): Promise<[MeshNode, Frontend]> {
  const mesh = await cluster.member();
  const frontend = new Frontend(name);
  await within(
    mesh.publishSharded(discord, frontend, { count: config.count, mode: SlotMode.static }),
  );
  try {
    for (const shard of config.shards) {
      await within(mesh.claimSlot(discord, shard));
    }
  } catch (error) {
    await mesh.leave();
    throw error;
  }
  return [mesh, frontend];
}

/**
 * Posts [message] in [guild] from a service that knows nothing of the
 * frontends but the table; resolves with who posted it. A transient
 * `UNAVAILABLE` (a pooled connection replaced under load, say) is retried
 * a few times, as a real caller would.
 */
async function post(service: MeshNode, guild: bigint, message: string): Promise<string> {
  const count = service.client.slotTable(discord)?.count ?? 0;
  for (let attempt = 1; ; attempt++) {
    try {
      const talk = await service.switchboard.openTalkToSlot(discord, shardOf(guild, count));
      try {
        return (await talk.request('POST', `${guild} ${message}`)).text();
      } finally {
        await talk.close();
      }
    } catch (e) {
      if (!(e instanceof SwitchboardError) || e.code !== StatusCode.unavailable || attempt >= 5) {
        throw e;
      }
      await delay(20 * attempt);
    }
  }
}

/** Guild ids (snowflakes) and their shard of 4 and of 8. */
const guilds = new Map<bigint, [number, number]>([
  [81384788765712384n, [2, 2]],
  [(1001n << 22n) | 4242n, [1, 1]],
  [(1006n << 22n) | 99n, [2, 6]],
  [(1003n << 22n) | 7n, [3, 3]],
  [(1004n << 22n) | 1n, [0, 4]],
  [(1013n << 22n) | 31337n, [1, 5]],
]);

afterAll(() => {
  expectNoMemoryListeners();
});

for (const scheme of ['tcp', 'ws', 'mem'] as const) {
  describe(scheme, () => {
    let cluster: Cluster;
    let service: MeshNode;

    beforeEach(async () => {
      cluster = new Cluster(scheme);
      await cluster.start();
      service = await cluster.member({ listen: false });
    });

    test('guilds reach the frontend of their shard', async () => {
      for (const [guild, [four, eight]] of guilds) {
        expect(shardOf(guild, 4)).toBe(four);
        expect(shardOf(guild, 8)).toBe(eight);
      }
      const [a, frontA] = await startFrontend(cluster, 'A', { count: 4, shards: [0, 1] });
      const [b, frontB] = await startFrontend(cluster, 'B', { count: 4, shards: [2, 3] });
      await until(() => service.slotOwners(discord).size === 2);
      expect(service.slotOwners(discord)).toEqual(
        new Set([a.gates.get(discord)?.instance, b.gates.get(discord)?.instance]),
      );
      for (const [guild, [shard]] of guilds) {
        expect(await post(service, guild, 'hello')).toBe(`${shard < 2 ? 'A' : 'B'} shard ${shard}`);
      }
      expect(frontA.posted).toHaveLength(3);
      expect(frontB.posted).toHaveLength(3);
      expect(frontA.shards).toEqual(new Set([0, 1]));
      expect(frontB.shards).toEqual(new Set([2, 3]));
    });

    test('a second frontend for a taken shard refuses to start', async () => {
      const [a] = await startFrontend(cluster, 'A', { count: 4, shards: [0, 1, 2, 3] });
      await rejectsWith(
        startFrontend(cluster, 'C', { count: 4, shards: [1] }),
        StatusCode.alreadyExists,
      );
      await until(() => service.slotOwners(discord).size > 0);
      expect(service.slotOwners(discord)).toEqual(new Set([a.gates.get(discord)?.instance]));
      expect(await post(service, 81384788765712384n, 'still')).toBe('A shard 2');
    });

    test('a frontend with more shards than its naming channel takes requests leaves with every one released', async () => {
      // 16 requests at a time on the naming channel, 40 shards.
      const mesh = await cluster.member({
        talkOptions: { requestTimeout: 2000, replyTimeout: 1000, maxOutgoingRequests: 16 },
      });
      const frontend = new Frontend('A');
      await within(mesh.publishSharded(discord, frontend, { count: 40, mode: SlotMode.static }));
      const shards = Array.from({ length: 40 }, (_, i) => i);
      await within(Promise.all(shards.map((shard) => mesh.claimSlot(discord, shard))));
      expect(frontend.shards).toEqual(new Set(shards));
      const report = await within(mesh.leave());
      expect([...report.released]).toEqual([[discord.key, shards]]);
      expect(report.dropped.size).toBe(0);
      expect(report.handedOver.size).toBe(0);
      expect(frontend.shards.size).toBe(0);
      await until(() => cluster.naming.slotTable(discord)?.owners.size === 0, 'every shard free');
      // Released, not lost: the epoch is kept (stateless: no holder).
      const entries = [...(cluster.naming.slotTable(discord)?.entries.values() ?? [])];
      expect(entries).toHaveLength(40);
      for (const entry of entries) {
        expect(entry.equals(SlotEntry.free({ epoch: 1 })), `${entry}`).toBe(true);
      }
    });

    test('a frontend whose publish failed publishes again', async () => {
      await startFrontend(cluster, 'A', { count: 4, shards: [0] });
      const mesh = await cluster.member();
      const frontend = new Frontend('B');
      await rejectsWith(
        mesh.publishSharded(discord, frontend, { count: 8, mode: SlotMode.static }),
        StatusCode.failedPrecondition,
      );
      // The same lifecycle, with the right configuration.
      const gate = await within(
        mesh.publishSharded(discord, frontend, { count: 4, mode: SlotMode.static }),
      );
      expect(frontend.gate).toBe(gate);
      await within(mesh.claimSlot(discord, 1));
      expect(await post(service, (1001n << 22n) | 4242n, 'again')).toBe('B shard 1');
      const report = await mesh.leave();
      expect([...report.released]).toEqual([[discord.key, [1]]]);
      expect(report.handedOver.size).toBe(0);
    });

    test('a full restart with another shard count redefines the space', async () => {
      const first = [
        await startFrontend(cluster, 'A', { count: 4, shards: [0, 1] }),
        await startFrontend(cluster, 'B', { count: 4, shards: [2, 3] }),
      ];
      expect(await post(service, (1003n << 22n) | 7n, 'x')).toBe('B shard 3');
      // A frontend of the new configuration cannot join the old one.
      await rejectsWith(
        startFrontend(cluster, 'X', { count: 8, shards: [0] }),
        StatusCode.failedPrecondition,
      );
      for (const [mesh] of first) {
        await mesh.leave();
      }
      await until(() => service.slotOwners(discord).size === 0);
      // Stateless: every slot is free with no holder, so N may change.
      const [c, frontC] = await startFrontend(cluster, 'C', { count: 8, shards: [0, 1, 2, 3] });
      const [, frontD] = await startFrontend(cluster, 'D', { count: 8, shards: [4, 5, 6, 7] });
      await until(
        () =>
          service.client.slotTable(discord)?.count === 8 && service.slotOwners(discord).size === 2,
      );
      expect(cluster.naming.slotTable(discord)?.count).toBe(8);
      for (const [guild, [, shard]] of guilds) {
        expect(await post(service, guild, 'again')).toBe(`${shard < 4 ? 'C' : 'D'} shard ${shard}`);
      }
      expect(frontC.shards).toEqual(new Set([0, 1, 2, 3]));
      expect(frontD.shards).toEqual(new Set([4, 5, 6, 7]));
      expect([...(c.gates.get(discord)?.servedSlots.keys() ?? [])].sort()).toEqual([0, 1, 2, 3]);
    });
  });
}
