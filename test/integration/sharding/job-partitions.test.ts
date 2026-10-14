/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Port of the Dart `test/integration/sharding/job_partitions_test.dart`,
// use case 6 of the wiki page "Polyverse Switchboard Sharding": queue
// partitions (`jobs`, static, shared recovery). Workers claim the
// partitions their configuration names; a standby worker configured for
// every partition claims whatever is free. The partition logs and consumer
// positions are in shared storage, so when a worker goes down the standby
// takes its partitions over and continues where it stopped. Over TCP as in
// Dart, and over in-process `mem://` endpoints.

import { afterAll, describe, expect, test } from 'vitest';
import { Name } from '../../../src/core/name.ts';
import { AssignResult } from '../../../src/core/naming/naming-client-slots.ts';
import { SlotMode } from '../../../src/core/naming/naming-protocol.ts';
import { SlotLifecycle, type SlotLoadOptions } from '../../../src/core/naming/slot-gate.ts';
import { slotForText } from '../../../src/core/naming/slot-key.ts';
import { StatusCode, SwitchboardError } from '../../../src/core/status.ts';
import type { IncomingChannel } from '../../../src/core/switchboard/contract.ts';
import type { MeshNode } from '../../../src/core/switchboard/mesh-node.ts';
import { expectNoMemoryListeners } from '../mesh-support.ts';
import { addTearDown, Cluster, delay, retryable, until, within } from './cluster.ts';

const jobs = Name.of('jobs');
const partitions = 4;

/** Shared storage: the partition logs and the consumer positions. */
class Storage {
  readonly logs = new Map<number, string[]>(
    Array.from({ length: partitions }, (_, p) => [p, []] as [number, string[]]),
  );
  readonly positions = new Map<number, number>(
    Array.from({ length: partitions }, (_, p) => [p, 0] as [number, number]),
  );
  /** Every job processed: [worker, partition, job]. */
  readonly processed: [string, number, string][] = [];

  log(partition: number): string[] {
    return this.logs.get(partition) as string[];
  }

  position(partition: number): number {
    return this.positions.get(partition) as number;
  }
}

/** A worker: consumes the partitions it owns from the shared position. */
class Worker extends SlotLifecycle {
  readonly name: string;
  readonly storage: Storage;
  readonly consumers = new Map<number, ReturnType<typeof setInterval>>();
  /** Set when the process dies: it stops doing anything at once. */
  dead = false;

  constructor(name: string, storage: Storage) {
    super();
    this.name = name;
    this.storage = storage;
  }

  load(slot: number, { shared }: SlotLoadOptions): Promise<AssignResult> {
    expect(shared).toBe(true);
    this.consumers.set(
      slot,
      setInterval(() => {
        if (this.dead) {
          return;
        }
        // Processing a job and moving the position is one step.
        const log = this.storage.log(slot);
        while (this.storage.position(slot) < log.length) {
          const job = log[this.storage.position(slot)] as string;
          this.storage.processed.push([this.name, slot, job]);
          this.storage.positions.set(slot, this.storage.position(slot) + 1);
        }
      }, 2),
    );
    return Promise.resolve(AssignResult.notHolding);
  }

  override unload(slot: number): void {
    clearInterval(this.consumers.get(slot));
    this.consumers.delete(slot);
  }

  serve(channel: IncomingChannel, slot: number): void {
    void channel.talk().serve((m) => {
      if (this.dead) {
        return;
      }
      const job = m.text();
      const log = this.storage.log(slot);
      // A producer retrying after a lost answer pushes the job again.
      if (!log.includes(job)) {
        log.push(job);
      }
      void m.reply('ok');
    });
  }
}

async function startWorker(
  cluster: Cluster,
  name: string,
  storage: Storage,
  options: { claim?: readonly number[]; standby?: readonly number[] } = {},
): Promise<[MeshNode, Worker]> {
  const mesh = await cluster.member();
  const worker = new Worker(name, storage);
  await within(
    mesh.publishSharded(jobs, worker, {
      count: partitions,
      mode: SlotMode.static,
      shared: true,
    }),
  );
  for (const partition of options.claim ?? []) {
    await within(mesh.claimSlot(jobs, partition));
  }
  const standby = options.standby ?? [];
  if (standby.length > 0) {
    // Claims every configured partition that is (or becomes) free.
    const take = async (partition: number): Promise<void> => {
      try {
        await mesh.claimSlot(jobs, partition);
      } catch (e) {
        if (
          !(e instanceof SwitchboardError) ||
          (e.code !== StatusCode.alreadyExists && e.code !== StatusCode.failedPrecondition)
        ) {
          throw e;
        }
      }
    };
    const stop = mesh.client.on('slotEvent', (event) => {
      if (event.type.equals(jobs) && standby.includes(event.slot) && event.entry.isFree) {
        void take(event.slot);
      }
    });
    addTearDown(stop);
    for (const partition of standby) {
      if (mesh.client.slotOwner(jobs, partition) === undefined) {
        void take(partition);
      }
    }
  }
  return [mesh, worker];
}

afterAll(() => {
  expectNoMemoryListeners();
});

for (const scheme of ['tcp', 'mem'] as const) {
  describe(scheme, () => {
    test('a standby worker takes over the partitions of a worker that went down, from the shared position', async () => {
      const cluster = new Cluster(scheme);
      await cluster.start();
      const storage = new Storage();
      const [w1, worker1] = await startWorker(cluster, 'W1', storage, { claim: [0, 1] });
      await startWorker(cluster, 'W2', storage, { claim: [2, 3] });
      const [standbyMesh] = await startWorker(cluster, 'S', storage, { standby: [0, 1, 2, 3] });
      const standbyGate = (): Map<number, number> =>
        new Map(standbyMesh.gates.get(jobs)?.servedSlots ?? []);
      expect(standbyGate().size).toBe(0);

      // A producer pushes jobs to their partitions.
      const producer = await cluster.member({ listen: false });
      const total = 300;
      let pushed = 0;
      const push = async (job: string): Promise<void> => {
        const partition = slotForText(job, partitions);
        for (;;) {
          try {
            const talk = await producer.switchboard.openTalkToSlot(jobs, partition);
            try {
              await talk.request('PUSH', job);
              return;
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
      };
      const producing = (async () => {
        for (let i = 0; i < total; i++) {
          await push(`job${i}`);
          pushed++;
          if (i % 10 === 0) {
            await delay(1);
          }
        }
      })();
      producing.catch(() => {});

      // W1 goes down mid-stream.
      await until(() => pushed >= Math.trunc(total / 3));
      worker1.dead = true;
      await cluster.crash(w1);
      await until(() => standbyGate().size === 2, 'the standby took the partitions over');
      expect([...standbyGate().keys()].sort()).toEqual([0, 1]);
      await within(producing);
      await until(() => storage.processed.length === total, 'every job processed');

      // Each job once, in partition order; W1's partitions continued by S.
      for (let p = 0; p < partitions; p++) {
        const done = storage.processed.filter(([, partition]) => partition === p).map((e) => e[2]);
        expect(done).toEqual(storage.log(p));
        expect(storage.position(p)).toBe(storage.log(p).length);
        const workers = storage.processed
          .filter(([, partition]) => partition === p)
          .map((e) => e[0]);
        if (p < 2) {
          const takeover = workers.indexOf('S');
          expect(takeover, `partition ${p}`).toBeGreaterThan(0);
          expect(new Set(workers.slice(0, takeover))).toEqual(new Set(['W1']));
          expect(new Set(workers.slice(takeover))).toEqual(new Set(['S']));
        } else {
          expect(new Set(workers)).toEqual(new Set(['W2']));
        }
      }
      expect(new Set(storage.processed.map((e) => e[2])).size).toBe(total);
    });
  });
}
