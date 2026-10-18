/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Review finding (naming layer, MeshNode's sharded half): `leave()` bounds
// the hand-over and the releases by `leaveTimeout` (design 6.7: "each step
// bounded by leaveTimeout"; "Never rejects"), but then awaits every gate's
// `close()`, which awaits `SlotLifecycle.unload` of each slot still served
// without a bound. When the naming service is unreachable nothing is
// released first, so an `unload` that does not complete (a flush to a store
// that is down too) keeps `leave()`, and an `await using` of the mesh node,
// pending for ever. Dart's `leave` does the same (`Future.wait` over
// `gate.close()`). Over the toy mux on `mem` listeners.

import { expect, test } from 'vitest';
import { Name } from '../../../src/core/name.ts';
import { AssignResult } from '../../../src/core/naming/naming-client.ts';
import { Services, SlotMode } from '../../../src/core/naming/naming-protocol.ts';
import { NamingService } from '../../../src/core/naming/naming-service.ts';
import { SlotLifecycle } from '../../../src/core/naming/slot-gate.ts';
import { MeshNode } from '../../../src/core/switchboard/mesh-node.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';
import { node, onCleanup, until } from '../../support/node/harness.ts';

const npc = Name.of('npc');

/** Keeps its slots' storage; `unload` waits for [flushed]. */
class Flushing extends SlotLifecycle {
  readonly flushed = new Deferred<void>();
  unloading = 0;

  load(): Promise<AssignResult> {
    return Promise.resolve(AssignResult.holding);
  }

  override unload(): Promise<void> {
    this.unloading++;
    return this.flushed.promise;
  }

  serve(): void {}
}

/** [promise], or a rejection after [ms] of real time. */
function within<T>(promise: Promise<T>, ms: number): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`not settled within ${ms} ms`)), ms);
    promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (error: unknown) => {
        clearTimeout(timer);
        reject(error);
      },
    );
  });
}

test.fails('leave settles within its bounds when an unload hangs and the naming service is gone', async () => {
  const host = node();
  const service = new NamingService({ assignmentHold: 0, holdingSettle: 0 });
  host.registerService(Services.naming, service.handler, { instance: 1 });
  const uri = await host.listenMemory();
  const n = node();
  const joined = MeshNode.join(n, uri, { watch: false, reconnectDelay: 20, leaveTimeout: 100 });
  const lifecycle = new Flushing();
  onCleanup(() => lifecycle.flushed.resolve());
  await within(
    joined.publishSharded(npc, lifecycle, { count: 4, mode: SlotMode.static, endpoints: [] }),
    2000,
  );
  await within(joined.claimSlot(npc, 1), 2000);
  await service.close();
  await until(() => !joined.client.isConnected);
  const leaving = joined.leave();
  leaving.catch(() => {});
  // Two leaveTimeouts and then some: the hand-over and release steps are
  // skipped (not connected), and nothing else is meant to wait.
  const report = await within(leaving, 1000);
  expect([...report.dropped]).toEqual([[npc.key, [1]]]);
  expect(lifecycle.unloading).toBe(1);
});
