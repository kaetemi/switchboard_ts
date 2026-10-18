/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// The naming service side of sharding: slot spaces, ownership, the
// hand-over orchestration and the allocator, driven by scripted instances
// over in-memory links. Port of the Dart `sharding_test.dart`.

import { afterEach, beforeEach, describe, expect, onTestFinished, test } from 'vitest';
import { Name } from '../../../src/core/name.ts';
import {
  AssignResponse,
  ClaimRequest,
  ClaimResponse,
  HoldingRequest,
  HoldingResponse,
  LocateRequest,
  LocateResponse,
  MigrateRequest,
  ReleaseRequest,
  SlotEntry,
  SlotMode,
  SlotSpace,
  SlotState,
  SlotsRequest,
  WatchRequest,
} from '../../../src/core/naming/naming-protocol.ts';
import { NamingService } from '../../../src/core/naming/naming-service.ts';
import type { SlotTable } from '../../../src/core/naming/slot-table.ts';
import { isStatus, Status, StatusCode, SwitchboardError } from '../../../src/core/status.ts';
import {
  type TalkChannel,
  type TalkMessage,
  talkOptions,
} from '../../../src/core/talk/contract.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';
import { ManualClock } from '../../../src/testing/manual-clock.ts';
import {
  clientOptions,
  delay,
  Harness,
  ms10,
  ms50,
  pump,
  register,
  rejectsWith,
  serverOptions,
  until,
} from '../../support/naming/harness.ts';
import {
  describeItem,
  Instance,
  ItemWatch,
  kv,
  Lookup,
  Migration,
  room,
  userq,
  zone,
} from '../../support/naming/sharding-harness.ts';

function staticSpace(type: Name, count: number): SlotSpace {
  return new SlotSpace(type, { count, mode: SlotMode.static });
}

/** Number of slots routed to each instance. */
function counts(table: SlotTable): Record<number, number> {
  const out: Record<number, number> = {};
  for (const entry of table.entries.values()) {
    if (entry.owner !== 0) {
      out[entry.owner] = (out[entry.owner] ?? 0) + 1;
    }
  }
  return out;
}

function allOwned(table: SlotTable | undefined): boolean {
  if (table === undefined) {
    return false;
  }
  for (let s = 0; s < table.count; s++) {
    if (table.get(s).state !== SlotState.owned) {
      return false;
    }
  }
  return true;
}

/** The slot table of [type] in [harness]'s service; fails the test when there is none. */
function tableOf(harness: Harness, type: Name): SlotTable {
  const table = harness.service.slotTable(type);
  if (table === undefined) {
    throw new Error(`no slot table for ${type}`);
  }
  return table;
}

/** A promise that never settles (a Dart `Completer().future` nobody completes). */
function never<T = void>(): Promise<T> {
  return new Promise<T>(() => {});
}

/** Resolves after [ms] of [clock]'s time (a Dart `Future.delayed` inside `fakeAsync`). */
function after(clock: ManualClock, ms: number): Promise<void> {
  return new Promise((resolve) => {
    clock.setTimeout(resolve, ms);
  });
}

/** Reads and drops the items of a stream request, ignoring how it ends. */
function drain(items: AsyncIterable<TalkMessage>): void {
  void (async () => {
    try {
      for await (const _ of items) {
        // Dropped.
      }
    } catch {
      // Read through `done`.
    }
  })();
}

/** Closes [h] and checks that no timer of [clock] is left. */
async function closeWithoutTimers(clock: ManualClock, h: Harness): Promise<void> {
  void h.close();
  await pump();
  // The scripted instances' own EXTEND timers notice on their next tick.
  await clock.elapseAsync(ms50 * 2);
  expect(clock.pendingCount).toBe(0);
}

let h: Harness;
let log: string[];

beforeEach(() => {
  h = new Harness();
  log = [];
});

afterEach(async () => {
  await h.close();
});

function instance(type: Name, id: number, options: { harness?: Harness } = {}): Instance {
  return new Instance(options.harness ?? h, type, id, log);
}

describe('SLOTS', () => {
  test('defines, confirms, refuses a different definition', async () => {
    const a = instance(kv, 1);
    const b = instance(kv, 2);
    await a.start();
    await b.start();
    const watch = new ItemWatch(h.link()[0]);
    expect(await watch.take(3)).toEqual(['UP kv/1', 'UP kv/2', 'WATCHED']);
    await a.slots(new SlotSpace(kv, { count: 8 }), { capacity: 0 });
    expect(await watch.next).toBe('SLOTSPC kv 8 managed');
    await b.slots(new SlotSpace(kv, { count: 8 }), { capacity: 0 });
    // An explicit claim: the space is no longer blank.
    expect(await a.claim(0)).toBe(1);
    expect(await watch.next).toBe('SLOT kv/0 owned 1 holder 1 epoch 1');
    for (const other of [
      new SlotSpace(kv, { count: 16 }),
      new SlotSpace(kv, { count: 8, lazy: true }),
      new SlotSpace(kv, { count: 8, shared: true }),
      staticSpace(kv, 8),
    ]) {
      await rejectsWith(b.slots(other, { capacity: 0 }), StatusCode.failedPrecondition);
    }
    expect(tableOf(h, kv).space).toEqual(new SlotSpace(kv, { count: 8 }));
    expect([...h.service.slotTables.keys()]).toEqual([kv.key]);
    // A router or operator tool without a registration may confirm it.
    const [router] = h.link();
    await router.request(
      'SLOTS',
      new SlotsRequest(new SlotSpace(kv, { count: 8 }), { capacity: 5 }).encode(),
    );
  });

  test('redefines a space once every slot is free with holder 0', async () => {
    const discord = Name.of('discord');
    const a = instance(discord, 1);
    await a.start({ space: staticSpace(discord, 4), capacity: 0 });
    const [router] = h.link();
    const watch = new ItemWatch(router, { payload: new WatchRequest(discord).encode() });
    expect(await watch.take(3)).toEqual(['UP discord/1', 'SLOTSPC discord 4 static', 'WATCHED']);
    expect(await a.claim(1)).toBe(1);
    expect(await watch.next).toBe('SLOT discord/1 owned 1 holder 1 epoch 1');
    await rejectsWith(
      a.slots(staticSpace(discord, 8), { capacity: 0 }),
      StatusCode.failedPrecondition,
    );
    await a.release(1, { keepStorage: true });
    expect(await watch.next).toBe('SLOT discord/1 free holder 1 epoch 1');
    // A holder still counts.
    await rejectsWith(
      a.slots(staticSpace(discord, 8), { capacity: 0 }),
      StatusCode.failedPrecondition,
    );
    expect(await a.claim(1)).toBe(2);
    await a.release(1);
    expect(await watch.take(2)).toEqual([
      'SLOT discord/1 owned 1 holder 1 epoch 2',
      'SLOT discord/1 free holder 0 epoch 2',
    ]);
    await a.slots(staticSpace(discord, 8), { capacity: 0 });
    expect(await watch.next).toBe('SLOTSPC discord 8 static');
    const table = tableOf(h, discord);
    expect(table.count).toBe(8);
    expect(table.entries.size).toBe(0);
  });

  test('refuses invalid requests', async () => {
    const [c] = h.link();
    const slots = (payload: Uint8Array): Promise<unknown> => c.request('SLOTS', payload);
    await rejectsWith(
      slots(new SlotsRequest(new SlotSpace(kv, { count: 0 }), { capacity: 1 }).encode()),
      StatusCode.invalidArgument,
    );
    await rejectsWith(
      slots(new SlotsRequest(new SlotSpace(Name.of(''), { count: 1 }), { capacity: 1 }).encode()),
      StatusCode.invalidArgument,
    );
    await rejectsWith(
      slots(new SlotsRequest(new SlotSpace(kv, { count: 65537 }), { capacity: 1 }).encode()),
      StatusCode.outOfRange,
    );
    await rejectsWith(slots(new Uint8Array(5)), StatusCode.invalidArgument);
    // Mode 2.
    await rejectsWith(
      slots(Uint8Array.from([...kv.bytes, 1, 0, 0, 0, 2, 0, 0, 0, 0, 0])),
      StatusCode.invalidArgument,
    );
    for (const procedure of ['HOLDING', 'CLAIM', 'RELEASE', 'LOCATE']) {
      await rejectsWith(c.request(procedure, new Uint8Array(3)), StatusCode.invalidArgument);
    }
    await rejectsWith(
      c.streamRequest('MIGRATE', new Uint8Array(3)).done,
      StatusCode.invalidArgument,
    );
    expect(h.service.slotTables.size).toBe(0);
  });

  test('a channel with several instances of the type cannot shard it', async () => {
    const [c] = h.link();
    await register(c, 'kv', { instance: 1 });
    await register(c, 'kv', { instance: 2 });
    const several = async (promise: Promise<unknown>): Promise<void> => {
      const error = await rejectsWith(promise, StatusCode.failedPrecondition);
      expect(error.status.reason).toContain('several instances');
    };
    await several(
      c.request(
        'SLOTS',
        new SlotsRequest(new SlotSpace(kv, { count: 4 }), { capacity: 1 }).encode(),
      ),
    );
    // The space defined by an operator: the other slot procedures get
    // past NOT_FOUND to the same guard.
    const [op] = h.link();
    await op.request('SLOTS', new SlotsRequest(staticSpace(kv, 4), { capacity: 0 }).encode());
    const requests: [string, Uint8Array][] = [
      ['HOLDING', new HoldingRequest(kv, [1]).encode()],
      ['CLAIM', new ClaimRequest(kv, 1).encode()],
      ['RELEASE', new ReleaseRequest(kv, 1).encode()],
    ];
    for (const [procedure, payload] of requests) {
      await several(c.request(procedure, payload));
    }
    expect(tableOf(h, kv).entries.size).toBe(0);
  });
});

describe('CLAIM and RELEASE', () => {
  const discord = Name.of('discord');

  test('static claims, conflicts, release and holders', async () => {
    const a = instance(discord, 1);
    const b = instance(discord, 2);
    await a.start({ space: staticSpace(discord, 4), capacity: 0 });
    await b.start({ space: staticSpace(discord, 4), capacity: 0 });
    const table = tableOf(h, discord);
    expect(await a.claim(1)).toBe(1);
    expect(log).toEqual(['1 ASSIGN 1 e1 h0']);
    expect(table.get(1)).toEqual(SlotEntry.owned(1, { holder: 1, epoch: 1 }));
    // Claiming an owned slot again is a no-op.
    expect(await a.claim(1)).toBe(1);
    expect(log).toHaveLength(1);
    await rejectsWith(b.claim(1), StatusCode.alreadyExists);
    await rejectsWith(b.release(1), StatusCode.permissionDenied);
    await rejectsWith(b.release(2), StatusCode.permissionDenied);
    await rejectsWith(b.claim(4), StatusCode.notFound);
    const [raw] = h.link();
    await rejectsWith(
      raw.request('CLAIM', new ClaimRequest(Name.of('nope'), 0).encode()),
      StatusCode.notFound,
    );
    // Not registered as the type on this channel.
    await rejectsWith(
      raw.request('CLAIM', new ClaimRequest(discord, 0).encode()),
      StatusCode.failedPrecondition,
    );
    // Release keeping the storage: the next owner fetches from the holder.
    await a.release(1, { keepStorage: true });
    expect(table.get(1)).toEqual(SlotEntry.free({ holder: 1, epoch: 1 }));
    expect(await b.claim(1)).toBe(2);
    expect(log.at(-1)).toBe('2 ASSIGN 1 e2 h1');
    expect(table.get(1)).toEqual(SlotEntry.owned(2, { holder: 2, epoch: 2 }));
    // A claimant that holds the storage of a slot nobody else holds.
    expect(await a.claim(2, { holding: true })).toBe(1);
    expect(log.at(-1)).toBe('1 ASSIGN 2 e1 h1');
    // "Not holding" keeps the previous holder.
    await a.release(2, { keepStorage: true });
    b.onAssign = async () => new AssignResponse({ notHolding: true });
    expect(await b.claim(2)).toBe(2);
    expect(table.get(2)).toEqual(SlotEntry.owned(2, { holder: 1, epoch: 2 }));
    // A refused ASSIGN: the claim fails, the slot stays free.
    b.onAssign = async () => {
      throw SwitchboardError.of(StatusCode.unavailable, 'not ready');
    };
    await rejectsWith(b.claim(3), StatusCode.unavailable);
    expect(table.get(3)).toEqual(SlotEntry.unassigned);
    expect(log.at(-1)).toBe('2 ASSIGN 3 e1 h0');
  });

  test('an instance going down frees its slots, holder kept', async () => {
    const a = instance(discord, 1);
    const b = instance(discord, 2);
    await a.start({ space: staticSpace(discord, 4), capacity: 0 });
    await b.start({ space: staticSpace(discord, 4), capacity: 0 });
    await a.claim(0);
    b.onAssign = async () => new AssignResponse({ notHolding: true });
    await b.claim(1);
    await a.close();
    await until(() => tableOf(h, discord).get(0).isFree);
    const table = tableOf(h, discord);
    expect(table.get(0)).toEqual(SlotEntry.free({ holder: 1, epoch: 1 }));
    // Static: nothing is reassigned.
    await pump();
    expect(table.get(1)).toEqual(SlotEntry.owned(2, { epoch: 1 }));
    expect(log).toEqual(['1 ASSIGN 0 e1 h0', '2 ASSIGN 1 e1 h0']);
    // A standby claims it and fetches the state from the holder.
    expect(await b.claim(0)).toBe(2);
    expect(log.at(-1)).toBe('2 ASSIGN 0 e2 h1');
  });
});

describe('LOCATE', () => {
  test('lazy: assigns on first use, by spare capacity', async () => {
    const a = instance(userq, 1);
    const b = instance(userq, 2);
    const space = new SlotSpace(userq, { count: 64, lazy: true });
    await a.start({ space, capacity: 1 });
    await b.start({ space, capacity: 3 });
    await pump();
    expect(tableOf(h, userq).entries.size).toBe(0);
    const [router] = h.link();
    const r = new Instance(h, userq, 0, log);
    r.channel = router;
    const located: LocateResponse[] = [];
    for (let s = 0; s < 8; s++) {
      located.push(await r.locate(s));
    }
    expect(located.map((l) => l.state)).toEqual(Array(8).fill(SlotState.owned));
    expect(located.map((l) => l.epoch)).toEqual(Array(8).fill(1));
    expect(located.map((l) => l.owner)).toEqual([2, 1, 2, 2, 2, 1, 2, 2]);
    expect(counts(tableOf(h, userq))).toEqual({ 2: 6, 1: 2 });
    // Located again: the same answer, no new assignment.
    expect(await r.locate(0)).toEqual(new LocateResponse(SlotState.owned, 2, 1));
    expect(log).toHaveLength(8);
    await rejectsWith(r.locate(64), StatusCode.notFound);
  });

  test('prefers the holder over spare capacity', async () => {
    const a = instance(userq, 1);
    const b = instance(userq, 2);
    const space = new SlotSpace(userq, { count: 64, lazy: true });
    await a.start({ space, capacity: 1 });
    await b.start({ space, capacity: 10 });
    // a holds the storage of slot 20: it gets it back at once.
    expect(await a.holding([20])).toEqual([]);
    const [router] = h.link();
    const r = new Instance(h, userq, 0, log);
    r.channel = router;
    expect(await r.locate(20)).toEqual(new LocateResponse(SlotState.owned, 1, 1));
    expect(await r.locate(21)).toEqual(new LocateResponse(SlotState.owned, 2, 1));
    expect(log).toEqual(['1 ASSIGN 20 e1 h1', '2 ASSIGN 21 e1 h0']);
  });

  test('static: a free slot is answered as free', async () => {
    const a = instance(zone, 1);
    await a.start({ space: staticSpace(zone, 4), capacity: 1 });
    expect(await a.locate(2)).toEqual(new LocateResponse(SlotState.free, 0, 0));
    await a.claim(2);
    expect(await a.locate(2)).toEqual(new LocateResponse(SlotState.owned, 1, 1));
    expect(log).toEqual(['1 ASSIGN 2 e1 h0']);
  });

  test('capacity 0 takes no slots from LOCATE or the allocator', async () => {
    const a = instance(userq, 1);
    await a.start({ space: new SlotSpace(userq, { count: 16, lazy: true }), capacity: 0 });
    expect(await a.locate(5)).toEqual(new LocateResponse(SlotState.free, 0, 0));
    const b = instance(userq, 2);
    await b.start({ space: new SlotSpace(userq, { count: 16, lazy: true }) });
    expect(await a.locate(5)).toEqual(new LocateResponse(SlotState.owned, 2, 1));
    // Eager: everything goes to the instance with capacity.
    const c = instance(kv, 3);
    const d = instance(kv, 4);
    await c.start({ space: new SlotSpace(kv, { count: 4 }), capacity: 0 });
    await d.start({ space: new SlotSpace(kv, { count: 4 }), capacity: 1 });
    await until(() => allOwned(h.service.slotTable(kv)));
    expect(counts(tableOf(h, kv))).toEqual({ 4: 4 });
  });

  test('an ASSIGN refused fails LOCATE with UNAVAILABLE', async () => {
    const a = instance(userq, 1);
    await a.start({ space: new SlotSpace(userq, { count: 16, lazy: true }) });
    a.onAssign = async () => {
      throw SwitchboardError.of(StatusCode.unavailable, 'loading');
    };
    await rejectsWith(a.locate(3), StatusCode.unavailable);
    expect(tableOf(h, userq).get(3)).toEqual(SlotEntry.unassigned);
  });

  test('requests cancelled while an ASSIGN runs are forgotten at once; the ASSIGN goes on', async () => {
    const a = instance(userq, 1);
    const b = instance(userq, 2);
    const space = new SlotSpace(userq, { count: 16, lazy: true });
    await a.start({ space, capacity: 1 });
    await b.start({ space, capacity: 0 });
    const assigned = new Deferred<AssignResponse>();
    a.onAssign = () => assigned.promise;
    const [router] = h.link();
    const first = router.request('LOCATE', new LocateRequest(userq, 3).encode(), { timeout: 0 });
    await until(() => log.includes('1 ASSIGN 3 e1 h0'));
    // Each waits for the ASSIGN of slot 3: a LOCATE, a CLAIM and a
    // HOLDING by another instance, a RELEASE by the instance it goes to.
    // The Talk channel forgets each request its requester cancels, so
    // nothing but the naming service would bound how many it keeps.
    for (let i = 0; i < 50; i++) {
      const requests: [TalkChannel, string, Uint8Array][] = [
        [router, 'LOCATE', new LocateRequest(userq, 3).encode()],
        [b.channel, 'CLAIM', new ClaimRequest(userq, 3).encode()],
        [b.channel, 'HOLDING', new HoldingRequest(userq, [3]).encode()],
        [a.channel, 'RELEASE', new ReleaseRequest(userq, 3).encode()],
      ];
      for (const [channel, procedure, payload] of requests) {
        const request = channel.startRequest(procedure, payload, { timeout: 0 });
        request.response.catch(() => {});
        await pump();
        request.cancel();
      }
    }
    await until(() => h.service.waitingRequestCount === 1);
    assigned.resolve(new AssignResponse());
    expect(LocateResponse.decode((await first).payload)).toEqual(
      new LocateResponse(SlotState.owned, 1, 1),
    );
    expect(log).toEqual(['1 ASSIGN 3 e1 h0']);
    expect(h.service.waitingRequestCount).toBe(0);
  });

  test('requests cancelled during the assignment hold are forgotten at once', async () => {
    const h = new Harness({ assignmentHold: 1000 });
    onTestFinished(() => h.close());
    const a = instance(userq, 1, { harness: h });
    await a.start({ space: new SlotSpace(userq, { count: 16, lazy: true }) });
    const [router] = h.link();
    for (let i = 0; i < 50; i++) {
      const requests: [TalkChannel, string, Uint8Array][] = [
        [router, 'LOCATE', new LocateRequest(userq, 3).encode()],
        [a.channel, 'CLAIM', new ClaimRequest(userq, 3).encode()],
        [a.channel, 'HOLDING', new HoldingRequest(userq, [3]).encode()],
      ];
      for (const [channel, procedure, payload] of requests) {
        const request = channel.startRequest(procedure, payload, { timeout: 0 });
        request.response.catch(() => {});
        await pump();
        request.cancel();
      }
    }
    await until(() => h.service.waitingRequestCount === 0);
    expect(h.service.isHoldingAssignments).toBe(true);
    // Nothing of them is resolved when the hold ends.
    await until(() => !h.service.isHoldingAssignments);
    await delay(ms50);
    expect(tableOf(h, userq).entries.size).toBe(0);
    expect(log).toEqual([]);
  });
});

describe('allocator', () => {
  test('eager: fills every slot in proportion to capacity', async () => {
    const h = new Harness({ assignmentHold: ms50 });
    onTestFinished(() => h.close());
    const a = instance(kv, 1, { harness: h });
    const b = instance(kv, 2, { harness: h });
    await a.start({ space: new SlotSpace(kv, { count: 12 }), capacity: 1 });
    await b.start({ space: new SlotSpace(kv, { count: 12 }), capacity: 2 });
    expect(h.service.isHoldingAssignments).toBe(true);
    expect(tableOf(h, kv).entries.size).toBe(0);
    await until(() => allOwned(h.service.slotTable(kv)));
    expect(counts(tableOf(h, kv))).toEqual({ 1: 4, 2: 8 });
    expect(log.filter((l) => !l.endsWith('e1 h0'))).toEqual([]);
  });

  test('eager: 1024 slots over two equal instances', async () => {
    const h = new Harness({ assignmentHold: ms50 });
    onTestFinished(() => h.close());
    const space = new SlotSpace(kv, { count: 1024 });
    await instance(kv, 1, { harness: h }).start({ space });
    await instance(kv, 2, { harness: h }).start({ space });
    await until(() => allOwned(h.service.slotTable(kv)));
    expect(counts(tableOf(h, kv))).toEqual({ 1: 512, 2: 512 });
  });

  test('a third instance receives slots one migration at a time', async () => {
    const h = new Harness({ assignmentHold: ms50 });
    onTestFinished(() => h.close());
    const space = new SlotSpace(kv, { count: 12 });
    await instance(kv, 1, { harness: h }).start({ space });
    await instance(kv, 2, { harness: h }).start({ space });
    await until(() => allOwned(h.service.slotTable(kv)));
    expect(counts(tableOf(h, kv))).toEqual({ 1: 6, 2: 6 });
    log.length = 0;
    const migrating = new Set<number>();
    let maxConcurrent = 0;
    const unsubscribe = h.service.on('slotEvent', (item) => {
      if (item.entry.state === SlotState.migrating) {
        migrating.add(item.slot);
      } else {
        migrating.delete(item.slot);
      }
      if (migrating.size > maxConcurrent) {
        maxConcurrent = migrating.size;
      }
    });
    onTestFinished(unsubscribe);
    await instance(kv, 3, { harness: h }).start({ space });
    await until(
      () =>
        Object.values(counts(tableOf(h, kv))).every((n) => n === 4) &&
        migrating.size === 0 &&
        log.length === 12,
    );
    await pump();
    expect(maxConcurrent).toBe(1);
    // Strictly sequential: DRAIN, ASSIGN, FORWARD, then the next one,
    // alternately from the most over-share instance.
    expect(log).toEqual([
      '1 DRAIN 0 e2 to3',
      '3 ASSIGN 0 e2 h1',
      '1 FORWARD 0 e2 to3',
      '2 DRAIN 1 e2 to3',
      '3 ASSIGN 1 e2 h2',
      '2 FORWARD 1 e2 to3',
      '1 DRAIN 2 e2 to3',
      '3 ASSIGN 2 e2 h1',
      '1 FORWARD 2 e2 to3',
      '2 DRAIN 3 e2 to3',
      '3 ASSIGN 3 e2 h2',
      '2 FORWARD 3 e2 to3',
    ]);
  });

  test("holder-only: a down instance's slots wait for the grace", async () => {
    const clock = new ManualClock();
    const h = new Harness({ clock, assignmentHold: ms50 });
    const log: string[] = [];
    const a = new Instance(h, kv, 1, log);
    const b = new Instance(h, kv, 2, log);
    const space = new SlotSpace(kv, { count: 4 });
    void a.start({ space });
    void b.start({ space });
    await clock.elapseAsync(ms50 * 2);
    const table = tableOf(h, kv);
    expect(counts(table)).toEqual({ 1: 2, 2: 2 });
    expect(table.slotsOf(1)).toEqual([0, 2]);
    log.length = 0;
    void a.close();
    await clock.elapseAsync(ms10);
    expect(table.get(0)).toEqual(SlotEntry.free({ holder: 1, epoch: 1 }));
    expect(table.get(2)).toEqual(SlotEntry.free({ holder: 1, epoch: 1 }));
    await clock.elapseAsync(4 * 60_000 + 59_000);
    expect(log).toEqual([]);
    expect(table.get(0).isFree).toBe(true);
    await clock.elapseAsync(2000);
    // Reassigned with the holder cleared: the state starts fresh.
    expect(log).toEqual(['2 ASSIGN 0 e2 h0', '2 ASSIGN 2 e2 h0']);
    expect(table.get(0)).toEqual(SlotEntry.owned(2, { holder: 2, epoch: 2 }));
    expect(counts(table)).toEqual({ 2: 4 });
    void h.close();
    await pump();
    expect(clock.pendingCount).toBe(0);
  });

  test('holder-only: the holder returning within the grace reclaims', async () => {
    const clock = new ManualClock();
    const h = new Harness({ clock, assignmentHold: ms50 });
    const log: string[] = [];
    const space = new SlotSpace(kv, { count: 4 });
    void new Instance(h, kv, 1, log).start({ space });
    const b = new Instance(h, kv, 2, log);
    void b.start({ space });
    await clock.elapseAsync(ms50 * 2);
    void b.close();
    await clock.elapseAsync(60_000);
    log.length = 0;
    // Same id after a reboot.
    const again = new Instance(h, kv, 2, log);
    void again.start({ space });
    await clock.elapseAsync(ms10);
    expect(log).toEqual(['2 ASSIGN 1 e2 h2', '2 ASSIGN 3 e2 h2']);
    expect(counts(tableOf(h, kv))).toEqual({ 1: 2, 2: 2 });
    await clock.elapseAsync(600_000);
    expect(log).toHaveLength(2);
    void h.close();
    await pump();
    expect(clock.pendingCount).toBe(0);
  });

  test('shared recovery: reassigned at once, from the holder', async () => {
    const h = new Harness({ assignmentHold: ms50 });
    onTestFinished(() => h.close());
    const space = new SlotSpace(room, { count: 4, shared: true });
    const a = instance(room, 1, { harness: h });
    await a.start({ space });
    await instance(room, 2, { harness: h }).start({ space });
    await until(() => allOwned(h.service.slotTable(room)));
    log.length = 0;
    await a.close();
    await until(() => counts(tableOf(h, room))[2] === 4);
    expect(log).toEqual(['2 ASSIGN 0 e2 h1 shared', '2 ASSIGN 2 e2 h1 shared']);
  });
});

describe('HOLDING', () => {
  test('records holders, reclaims, lists what to discard', async () => {
    const a = instance(kv, 1);
    const b = instance(kv, 2);
    await a.start();
    await b.start();
    // Defined by an operator: no candidate yet, nothing assigned.
    const [op] = h.link();
    await op.request(
      'SLOTS',
      new SlotsRequest(new SlotSpace(kv, { count: 4 }), { capacity: 0 }).encode(),
    );
    expect(await a.holding([0, 1, 0])).toEqual([]);
    expect(await b.holding([1, 2, 7])).toEqual([1, 7]);
    const table = tableOf(h, kv);
    expect(table.get(1)).toEqual(SlotEntry.free({ holder: 1 }));
    expect(table.get(2)).toEqual(SlotEntry.free({ holder: 2 }));
    expect(log).toEqual([]);
    // a becomes a candidate: its slots come back first. b holds slot 2
    // but has not said whether it takes slots: the slot waits for it.
    await a.slots(new SlotSpace(kv, { count: 4 }));
    await until(() => table.get(3).state === SlotState.owned);
    await until(() => table.get(1).state === SlotState.owned);
    await pump();
    expect(table.get(2)).toEqual(SlotEntry.free({ holder: 2 }));
    // b takes no slots: the state is fetched from it.
    await b.slots(new SlotSpace(kv, { count: 4 }), { capacity: 0 });
    await until(() => allOwned(table));
    expect(log).toEqual([
      '1 ASSIGN 0 e1 h1',
      '1 ASSIGN 1 e1 h1',
      '1 ASSIGN 3 e1 h0',
      '1 ASSIGN 2 e1 h2',
    ]);
    await rejectsWith(
      op.request('HOLDING', new HoldingRequest(kv, [0]).encode()),
      StatusCode.failedPrecondition,
    );
    await rejectsWith(
      a.channel.request('HOLDING', new HoldingRequest(zone, [0]).encode()),
      StatusCode.notFound,
    );
  });

  test('a slot being assigned fresh elsewhere is answered once that is decided', async () => {
    const a = instance(kv, 1);
    const b = instance(kv, 2);
    const decide = new Deferred<void>();
    // Slot 0 is refused, slot 1 accepted, both once decided.
    b.onAssign = async (r) => {
      await decide.promise;
      if (r.slot === 0) {
        throw SwitchboardError.of(StatusCode.unavailable, 'no');
      }
      return new AssignResponse();
    };
    // a is registered but takes no slots yet (no SLOTS).
    await a.start();
    await b.start({ space: new SlotSpace(kv, { count: 2 }) });
    await until(() => log.length === 2);
    let discard: readonly number[] | undefined;
    void a.holding([0, 1]).then((d) => {
      discard = d;
    });
    await delay(ms50);
    expect(discard, 'waits for the ASSIGNs').toBeUndefined();
    decide.resolve();
    await until(() => discard !== undefined);
    // Slot 0 stayed free: a holds it and keeps its storage. Slot 1 went
    // to b, fresh: a's copy is stale.
    expect(discard).toEqual([1]);
    const table = tableOf(h, kv);
    expect(table.get(0)).toEqual(SlotEntry.free({ holder: 1 }));
    expect(table.get(1)).toEqual(SlotEntry.owned(2, { holder: 2, epoch: 1 }));
  });

  test('the holder declared during a failed fresh ASSIGN is the holder before a LOCATE tries the next instance', async () => {
    const space = new SlotSpace(userq, { count: 16, lazy: true });
    const a = instance(userq, 1);
    const b = instance(userq, 2);
    // Up and taking no slots: a holder others fetch from.
    const holder = instance(userq, 3);
    await a.start({ space, capacity: 1 });
    await b.start({ space, capacity: 1 });
    await holder.start({ space, capacity: 0 });
    const refuse = new Deferred<void>();
    a.onAssign = async () => {
      await refuse.promise;
      throw SwitchboardError.of(StatusCode.unavailable, 'not ready');
    };
    const [router] = h.link();
    const r = new Instance(h, userq, 0, log);
    r.channel = router;
    const located = r.locate(5);
    await until(() => log.includes('1 ASSIGN 5 e1 h0'));
    let discard: readonly number[] | undefined;
    void holder.holding([5]).then((d) => {
      discard = d;
    });
    await delay(ms50);
    expect(discard, 'waits for the ASSIGN to 1').toBeUndefined();
    refuse.resolve();
    expect(await located).toEqual(new LocateResponse(SlotState.owned, 2, 1));
    await until(() => discard !== undefined);
    // The ASSIGN to 1 failed: 3 holds the slot, which 2 fetches from it.
    expect(log).toEqual(['1 ASSIGN 5 e1 h0', '2 ASSIGN 5 e1 h3']);
    expect(discard).toEqual([]);
  });

  test('a slot being assigned fresh to the declaring instance keeps it as holder when that ASSIGN fails', async () => {
    const space = new SlotSpace(userq, { count: 16, lazy: true });
    const a = instance(userq, 1);
    const b = instance(userq, 2);
    await a.start({ space, capacity: 1 });
    await b.start({ space, capacity: 1 });
    const refuse = new Deferred<void>();
    a.onAssign = async () => {
      await refuse.promise;
      throw SwitchboardError.of(StatusCode.unavailable, 'not ready');
    };
    const [router] = h.link();
    const r = new Instance(h, userq, 0, log);
    r.channel = router;
    const located = r.locate(5);
    await until(() => log.includes('1 ASSIGN 5 e1 h0'));
    // Answered at once: the slot is being assigned to a itself.
    expect(await a.holding([5])).toEqual([]);
    refuse.resolve();
    expect(await located).toEqual(new LocateResponse(SlotState.owned, 2, 1));
    expect(log).toEqual(['1 ASSIGN 5 e1 h0', '2 ASSIGN 5 e1 h1']);
  });
});

describe('WATCH and LOOKUP', () => {
  test('snapshot: SLOTSPC before its slots, empty slots left out', async () => {
    const a = instance(kv, 1);
    await a.start({ space: staticSpace(kv, 4), capacity: 0 });
    await a.claim(1);
    await a.claim(2);
    await a.release(2, { keepStorage: true });
    await a.claim(3);
    await a.release(3);
    const [router] = h.link();
    const watch = new ItemWatch(router);
    expect(await watch.take(5)).toEqual([
      'UP kv/1',
      'SLOTSPC kv 4 static',
      'SLOT kv/1 owned 1 holder 1 epoch 1',
      'SLOT kv/2 free holder 1 epoch 1',
      'WATCHED',
    ]);
    await a.claim(0);
    expect(await watch.next).toBe('SLOT kv/0 owned 1 holder 1 epoch 1');
    await a.release(0);
    expect(await watch.next).toBe('SLOT kv/0 free holder 0 epoch 1');
    // Other types see none of it.
    const [other] = h.link();
    const zoneWatch = new ItemWatch(other, { payload: new WatchRequest(zone).encode() });
    expect(await zoneWatch.next).toBe('WATCHED');
    await a.claim(3);
    expect(await watch.next).toBe('SLOT kv/3 owned 1 holder 1 epoch 2');
    await zoneWatch.unwatch({ payload: new WatchRequest(zone).encode() });
    expect(await zoneWatch.next).toBe('UNWATCHED');
    await a.release(3);
    expect(await watch.next).toBe('SLOT kv/3 free holder 0 epoch 2');
    const lookup = new Lookup(router, kv.bytes);
    expect(await lookup.take(4)).toEqual([
      'UP kv/1',
      'SLOTSPC kv 4 static',
      'SLOT kv/1 owned 1 holder 1 epoch 1',
      'SLOT kv/2 free holder 1 epoch 1',
    ]);
    await lookup.stream.done;
  });
});

describe('MIGRATE', () => {
  let a: Instance;
  let b: Instance;
  let router: Instance;

  beforeEach(async () => {
    a = instance(zone, 1);
    b = instance(zone, 2);
    await a.start({ space: staticSpace(zone, 4), capacity: 0 });
    await b.start({ space: staticSpace(zone, 4), capacity: 0 });
    await a.claim(1);
    await a.claim(2);
    router = new Instance(h, zone, 0, log);
    router.channel = h.link()[0];
    log.length = 0;
  });

  test('hand-over: DRAIN, ASSIGN, table, FORWARD', async () => {
    // The old owner's own watch shows when it learns of the table.
    let live = false;
    a.onEvent = (m) => {
      if (live && m.procedureName === 'SLOT') {
        log.push(`1 sees ${describeItem(m)}`);
      }
    };
    await a.channel.request('WATCH', new Uint8Array(0), { ordered: true });
    live = true;
    const m = new Migration(router.channel, zone, 1, { to: 2 });
    expect(await m.done).toEqual(Status.ok);
    expect(m.phases).toEqual(['draining', 'assigning', 'forwarding', 'done']);
    expect(log).toEqual([
      '1 sees SLOT zone/1 migrating 1->2 holder 1 epoch 1',
      '1 DRAIN 1 e2 to2',
      '2 ASSIGN 1 e2 h1',
      '1 sees SLOT zone/1 owned 2 holder 2 epoch 2',
      '1 FORWARD 1 e2 to2',
    ]);
    expect(tableOf(h, zone).get(1)).toEqual(SlotEntry.owned(2, { holder: 2, epoch: 2 }));
  });

  test('one migration per space at a time; others queue', async () => {
    const gate = new Deferred<void>();
    a.onDrain = (r) => (r.slot === 1 ? gate.promise : Promise.resolve());
    const first = new Migration(router.channel, zone, 1, { to: 2 });
    await until(() => log.includes('1 DRAIN 1 e2 to2'));
    const second = new Migration(router.channel, zone, 2, { to: 2 });
    const again = new Migration(router.channel, zone, 1, { to: 2 });
    const queuedAgain = new Migration(router.channel, zone, 2, { to: 2 });
    expect((await again.done).known, 'already migrating').toBe(StatusCode.failedPrecondition);
    expect((await queuedAgain.done).known).toBe(StatusCode.failedPrecondition);
    // Longer than every timeout: kept alive meanwhile.
    await delay(500);
    expect(second.phases).toEqual([]);
    expect(log).toEqual(['1 DRAIN 1 e2 to2']);
    gate.resolve();
    expect(await first.done).toEqual(Status.ok);
    expect(await second.done).toEqual(Status.ok);
    expect(log).toEqual([
      '1 DRAIN 1 e2 to2',
      '2 ASSIGN 1 e2 h1',
      '1 FORWARD 1 e2 to2',
      '1 DRAIN 2 e2 to2',
      '2 ASSIGN 2 e2 h1',
      '1 FORWARD 2 e2 to2',
    ]);
  });

  test('ASSIGN refused: RESUME, epoch unchanged', async () => {
    b.onAssign = async () => {
      throw SwitchboardError.of(StatusCode.unavailable, 'not ready');
    };
    const m = new Migration(router.channel, zone, 1, { to: 2 });
    expect((await m.done).known).toBe(StatusCode.unavailable);
    expect(m.phases).toEqual(['draining', 'assigning', 'rolledBack']);
    await until(() => log.length === 3);
    expect(log).toEqual(['1 DRAIN 1 e2 to2', '2 ASSIGN 1 e2 h1', '1 RESUME 1 e1']);
    expect(tableOf(h, zone).get(1)).toEqual(SlotEntry.owned(1, { holder: 1, epoch: 1 }));
  });

  test('DRAIN refused: RESUME, nothing assigned', async () => {
    a.onDrain = async () => {
      throw SwitchboardError.of(StatusCode.aborted, 'busy');
    };
    const m = new Migration(router.channel, zone, 1, { to: 2 });
    expect((await m.done).known).toBe(StatusCode.unavailable);
    expect(m.phases).toEqual(['draining', 'rolledBack']);
    await until(() => log.length === 2);
    expect(log).toEqual(['1 DRAIN 1 e2 to2', '1 RESUME 1 e1']);
    expect(tableOf(h, zone).get(1)).toEqual(SlotEntry.owned(1, { holder: 1, epoch: 1 }));
  });

  test('the old owner going down mid-migration frees the slot', async () => {
    a.onDrain = () => never();
    const m = new Migration(router.channel, zone, 1, { to: 2 });
    await until(() => log.length > 0);
    await a.close();
    expect((await m.done).known).toBe(StatusCode.unavailable);
    expect(m.phases).toEqual(['draining', 'rolledBack']);
    const table = tableOf(h, zone);
    expect(table.get(1)).toEqual(SlotEntry.free({ holder: 1, epoch: 1 }));
    expect(table.get(2)).toEqual(SlotEntry.free({ holder: 1, epoch: 1 }));
    expect(log).toEqual(['1 DRAIN 1 e2 to2']);
  });

  test('the new owner going down mid-ASSIGN: RESUME to the old', async () => {
    b.onAssign = () => never();
    const m = new Migration(router.channel, zone, 1, { to: 2 });
    await until(() => log.length === 2);
    await b.close();
    expect((await m.done).known).toBe(StatusCode.unavailable);
    await until(() => log.length === 3);
    expect(log).toEqual(['1 DRAIN 1 e2 to2', '2 ASSIGN 1 e2 h1', '1 RESUME 1 e1']);
    expect(tableOf(h, zone).get(1)).toEqual(SlotEntry.owned(1, { holder: 1, epoch: 1 }));
  });

  test('queued MIGRATEs their requesters cancel are forgotten at once', async () => {
    // The old owner declares nothing while it drains, so that nothing
    // but the cancellation tells the service the requests are gone.
    const h = new Harness({ handoverTimeout: 5000 });
    onTestFinished(() => h.close());
    const a = instance(zone, 1, { harness: h });
    a.heartbeat = false;
    const b = instance(zone, 2, { harness: h });
    await a.start({ space: staticSpace(zone, 4), capacity: 0 });
    await b.start({ space: staticSpace(zone, 4), capacity: 0 });
    await a.claim(1);
    await a.claim(2);
    const [router] = h.link();
    log.length = 0;
    const gate = new Deferred<void>();
    a.onDrain = (r) => (r.slot === 1 ? gate.promise : Promise.resolve());
    const first = new Migration(router, zone, 1, { to: 2 });
    await until(() => log.includes('1 DRAIN 1 e2 to2'));
    for (let i = 0; i < 50; i++) {
      const cancelled = new Migration(router, zone, 2, { to: 2 });
      await pump();
      cancelled.stream.cancel();
    }
    await until(() => h.service.waitingRequestCount === 1);
    gate.resolve();
    expect(await first.done).toEqual(Status.ok);
    expect(log).toEqual(['1 DRAIN 1 e2 to2', '2 ASSIGN 1 e2 h1', '1 FORWARD 1 e2 to2']);
    expect(h.service.waitingRequestCount).toBe(0);
  });

  test('a queued MIGRATE its requester cancels leaves the queue; the next one starts', async () => {
    await a.claim(3);
    log.length = 0;
    const gate = new Deferred<void>();
    a.onDrain = (r) => (r.slot === 1 ? gate.promise : Promise.resolve());
    const first = new Migration(router.channel, zone, 1, { to: 2 });
    await until(() => log.includes('1 DRAIN 1 e2 to2'));
    const cancelled = new Migration(router.channel, zone, 2, { to: 2 });
    const next = new Migration(router.channel, zone, 3, { to: 2 });
    await delay(ms10);
    cancelled.stream.cancel();
    await delay(ms10);
    // Its place is given up: the slot can be queued again at once.
    const again = new Migration(router.channel, zone, 2, { to: 2 });
    await delay(ms50);
    expect(next.phases).toEqual([]);
    expect(again.phases).toEqual([]);
    gate.resolve();
    expect(await first.done).toEqual(Status.ok);
    expect(await next.done).toEqual(Status.ok);
    expect(await again.done).toEqual(Status.ok);
    expect((await cancelled.done).known).toBe(StatusCode.cancelled);
    expect(cancelled.phases).toEqual([]);
    expect(log).toEqual(
      [1, 3, 2].flatMap((slot) => [
        `1 DRAIN ${slot} e2 to2`,
        `2 ASSIGN ${slot} e2 h1`,
        `1 FORWARD ${slot} e2 to2`,
      ]),
    );
  });

  test('the service closing mid-migration sets the slot back, free with its holder once the owner is gone', async () => {
    // Stuck in DRAIN, then in ASSIGN.
    a.onDrain = (r) => (r.slot === 1 ? never() : Promise.resolve());
    b.onAssign = () => never();
    const draining = new Migration(router.channel, zone, 1, { to: 2 });
    await until(() => log.includes('1 DRAIN 1 e2 to2'));
    // The second one queues behind the first: closing answers it too.
    const queued = new Migration(router.channel, zone, 2, { to: 2 });
    await delay(ms10);
    const table = tableOf(h, zone);
    expect(table.get(1).state).toBe(SlotState.migrating);
    await h.service.close();
    expect((await draining.done).known).toBe(StatusCode.goingAway);
    expect((await queued.done).known).toBe(StatusCode.goingAway);
    // As every other slot of the instances, whose channels are gone.
    expect(table.get(1)).toEqual(SlotEntry.free({ holder: 1, epoch: 1 }));
    expect(table.get(2)).toEqual(SlotEntry.free({ holder: 1, epoch: 1 }));

    const h2 = new Harness();
    onTestFinished(() => h2.close());
    const a2 = instance(zone, 1, { harness: h2 });
    const b2 = instance(zone, 2, { harness: h2 });
    b2.onAssign = () => never();
    await a2.start({ space: staticSpace(zone, 4), capacity: 0 });
    await b2.start({ space: staticSpace(zone, 4), capacity: 0 });
    await a2.claim(1);
    log.length = 0;
    const assigning = new Migration(h2.link()[0], zone, 1, { to: 2 });
    await until(() => log.includes('2 ASSIGN 1 e2 h1'));
    await h2.service.close();
    expect((await assigning.done).known).toBe(StatusCode.goingAway);
    expect(tableOf(h2, zone).get(1)).toEqual(SlotEntry.free({ holder: 1, epoch: 1 }));
  });

  test('refusals', async () => {
    const migrate = async (type: Name, slot: number, to: number): Promise<StatusCode | undefined> =>
      (await new Migration(router.channel, type, slot, { to }).done).known;
    expect(await migrate(Name.of('nope'), 1, 2)).toBe(StatusCode.notFound);
    expect(await migrate(zone, 4, 2)).toBe(StatusCode.notFound);
    expect(await migrate(zone, 1, 1)).toBe(StatusCode.failedPrecondition);
    expect(await migrate(zone, 1, 99)).toBe(StatusCode.failedPrecondition);
    expect(await migrate(zone, 3, 2)).toBe(StatusCode.failedPrecondition);
    // Nobody with capacity to choose.
    expect(await migrate(zone, 1, 0)).toBe(StatusCode.failedPrecondition);
    await rejectsWith(
      router.channel.request('MIGRATE', new MigrateRequest(zone, 1, { to: 2 }).encode()),
      StatusCode.invalidArgument,
    );
    // The allocator chooses among the instances with capacity.
    await b.slots(staticSpace(zone, 4), { capacity: 1 });
    expect(await migrate(zone, 1, 0)).toBe(StatusCode.ok);
    expect(tableOf(h, zone).get(1).owner).toBe(2);
  });
});

describe('hand-over bound', () => {
  /** Two zone instances; 1 owns slots 1 and 2. Returns them and a router. */
  async function zones(
    clock: ManualClock,
    h: Harness,
    log: string[],
  ): Promise<[Instance, Instance, Instance]> {
    const a = new Instance(h, zone, 1, log);
    const b = new Instance(h, zone, 2, log);
    void a.start({ space: staticSpace(zone, 4), capacity: 0 });
    void b.start({ space: staticSpace(zone, 4), capacity: 0 });
    await clock.elapseAsync(ms10);
    void a.claim(1);
    void a.claim(2);
    await clock.elapseAsync(ms10);
    log.length = 0;
    const router = new Instance(h, zone, 0, log);
    router.channel = h.link()[0];
    return [a, b, router];
  }

  test('a FORWARD is not bounded: what it declares reaches the MIGRATE as declared', async () => {
    const clock = new ManualClock();
    const h = new Harness({ clock, handoverMaxDuration: 1000 });
    const log: string[] = [];
    const [a, , router] = await zones(clock, h, log);
    a.heartbeat = false;
    a.declare = (procedure) => (procedure === 'FORWARD' ? 30_000 : undefined);
    a.onForward = () => after(clock, 10_000);
    const declared: (number | undefined)[] = [];
    const m = router.channel.streamRequest(
      'MIGRATE',
      new MigrateRequest(zone, 1, { to: 2 }).encode(),
      { onExtend: (extension) => declared.push(extension.deadline) },
    );
    drain(m.items);
    let outcome: unknown;
    void m.done.then(
      () => {
        outcome = Status.ok;
      },
      (e: unknown) => {
        outcome = e;
      },
    );
    await clock.elapseAsync(11_000);
    expect(log).toEqual(['1 DRAIN 1 e2 to2', '2 ASSIGN 1 e2 h1', '1 FORWARD 1 e2 to2']);
    // As declared, plus the buffer each side adds to what it declares.
    expect(declared.at(-1)).toBe(
      30_000 + talkOptions(clientOptions).extendBuffer + talkOptions(serverOptions).extendBuffer,
    );
    expect(outcome).toBe(Status.ok);
    await closeWithoutTimers(clock, h);
  });

  test('a DRAIN running longer is cancelled and the migration rolled back', async () => {
    const clock = new ManualClock();
    const h = new Harness({ clock, handoverMaxDuration: 1000 });
    const log: string[] = [];
    const [a, , router] = await zones(clock, h, log);
    // Hangs, kept alive with EXTEND all along.
    a.onDrain = () => never();
    const m = new Migration(router.channel, zone, 1, { to: 2 });
    await clock.elapseAsync(990);
    expect(log).toEqual(['1 DRAIN 1 e2 to2']);
    let status: Status | undefined;
    void m.done.then((s) => {
      status = s;
    });
    await clock.elapseAsync(20);
    expect(status?.known).toBe(StatusCode.unavailable);
    expect(status?.reason).toContain('ran longer than');
    expect(m.phases).toEqual(['draining', 'rolledBack']);
    expect(log).toEqual(['1 DRAIN 1 e2 to2', '1 RESUME 1 e1']);
    expect(tableOf(h, zone).get(1)).toEqual(SlotEntry.owned(1, { holder: 1, epoch: 1 }));
    // The next migration of the space is not held up.
    a.onDrain = async () => {};
    const next = new Migration(router.channel, zone, 2, { to: 2 });
    await clock.elapseAsync(ms10);
    let nextStatus: Status | undefined;
    void next.done.then((s) => {
      nextStatus = s;
    });
    await pump();
    expect(nextStatus).toEqual(Status.ok);
    await closeWithoutTimers(clock, h);
  });

  test('an ASSIGN running longer: RESUME to the old owner', async () => {
    const clock = new ManualClock();
    const h = new Harness({ clock, handoverMaxDuration: 1000 });
    const log: string[] = [];
    const [, b, router] = await zones(clock, h, log);
    b.onAssign = () => never();
    const m = new Migration(router.channel, zone, 1, { to: 2 });
    await clock.elapseAsync(1010);
    let status: Status | undefined;
    void m.done.then((s) => {
      status = s;
    });
    await pump();
    expect(status?.known).toBe(StatusCode.unavailable);
    expect(m.phases).toEqual(['draining', 'assigning', 'rolledBack']);
    expect(log).toEqual(['1 DRAIN 1 e2 to2', '2 ASSIGN 1 e2 h1', '1 RESUME 1 e1']);
    expect(tableOf(h, zone).get(1).owner).toBe(1);
    await closeWithoutTimers(clock, h);
  });

  test('an allocator ASSIGN running longer frees the slot for the backoff', async () => {
    const clock = new ManualClock();
    const h = new Harness({ clock, handoverMaxDuration: 1000 });
    const log: string[] = [];
    const at: number[] = [];
    let first = true;
    const a = new Instance(h, kv, 1, log);
    a.onAssign = () => {
      at.push(clock.now());
      if (first) {
        first = false;
        return never();
      }
      return Promise.resolve(new AssignResponse());
    };
    void a.start({ space: new SlotSpace(kv, { count: 1 }) });
    await clock.elapseAsync(3000);
    // Cancelled after 1 s, offered again after the 200 ms backoff.
    expect(at).toEqual([0, 1200]);
    expect(tableOf(h, kv).get(0)).toEqual(SlotEntry.owned(1, { holder: 1, epoch: 1 }));
    await closeWithoutTimers(clock, h);
  });

  test('rejects a negative bound', () => {
    expect(() => new NamingService({ handoverMaxDuration: -1000 })).toThrow(RangeError);
    const service = new NamingService({ assignmentHold: 0 });
    expect(service.handoverMaxDuration).toBe(600_000);
    void service.close();
  });
});

describe('restart recovery', () => {
  test('claims made during the hold: higher epoch, then earlier', async () => {
    const h = new Harness({ assignmentHold: 100 });
    onTestFinished(() => h.close());
    const a = instance(zone, 1, { harness: h });
    const b = instance(zone, 2, { harness: h });
    const c = instance(zone, 3, { harness: h });
    for (const i of [a, b, c]) {
      await i.start({ space: staticSpace(zone, 8), capacity: 0 });
    }
    const ha = a.holding([3, 4]);
    const ca = a.claim(3, { holding: true, epoch: 5 });
    const cb = b.claim(3, { holding: true, epoch: 7 });
    const cc = c.claim(3, { epoch: 7 });
    const cc5 = c.claim(5, { epoch: 2 });
    await delay(ms50);
    expect(h.service.isHoldingAssignments).toBe(true);
    expect(log).toEqual([]);
    // Listened to at once: the refusals reach other channels than the
    // answer to b, in any order.
    const refusedA = rejectsWith(ca, StatusCode.alreadyExists);
    const refusedC = rejectsWith(cc, StatusCode.alreadyExists);
    expect(await cb).toBe(8);
    await refusedA;
    await refusedC;
    expect(await cc5).toBe(3);
    // HOLDING is answered after the claims.
    expect(await ha).toEqual([3]);
    const table = tableOf(h, zone);
    expect(table.get(3)).toEqual(SlotEntry.owned(2, { holder: 2, epoch: 8 }));
    expect(table.get(4)).toEqual(SlotEntry.free({ holder: 1 }));
    expect([...log].sort()).toEqual(['2 ASSIGN 3 e8 h2', '3 ASSIGN 5 e3 h0']);
  });

  test('LOCATE of a free managed slot waits for the hold', async () => {
    const h = new Harness({ assignmentHold: 100 });
    onTestFinished(() => h.close());
    const a = instance(userq, 1, { harness: h });
    await a.start({ space: new SlotSpace(userq, { count: 8, lazy: true }) });
    const started = performance.now();
    expect(await a.locate(1)).toEqual(new LocateResponse(SlotState.owned, 1, 1));
    expect(performance.now() - started).toBeGreaterThan(50);
  });
});

describe('ASSIGN backoff', () => {
  const notReady = (): SwitchboardError => SwitchboardError.of(StatusCode.unavailable, 'not ready');

  /**
   * Refuses the first [refusals] ASSIGNs, recording when each arrived (ms
   * of fake time) in [at].
   */
  function refusing(
    clock: ManualClock,
    at: number[],
    refusals = 1 << 30,
  ): () => Promise<AssignResponse> {
    let left = refusals;
    return async () => {
      at.push(clock.now());
      if (left > 0) {
        left--;
        throw notReady();
      }
      return new AssignResponse();
    };
  }

  test('a refusing candidate is retried at the backoff intervals', async () => {
    const clock = new ManualClock();
    const h = new Harness({ clock });
    const log: string[] = [];
    const at: number[] = [];
    const a = new Instance(h, kv, 1, log);
    a.onAssign = refusing(clock, at, 3);
    void a.start({ space: new SlotSpace(kv, { count: 1 }) });
    await clock.elapseAsync(30_000);
    // 200 ms, then doubling: not a burst of retries.
    expect(at).toEqual([0, 200, 600, 1400]);
    expect(log).toEqual(Array(4).fill('1 ASSIGN 0 e1 h0'));
    expect(tableOf(h, kv).get(0)).toEqual(SlotEntry.owned(1, { holder: 1, epoch: 1 }));
    await closeWithoutTimers(clock, h);
  });

  test('the backoff grows to its maximum and resets on success', async () => {
    const clock = new ManualClock();
    const h = new Harness({ clock, assignBackoff: ms50, assignBackoffMax: 120 });
    const log: string[] = [];
    const at: number[] = [];
    const a = new Instance(h, kv, 1, log);
    a.onAssign = refusing(clock, at, 4);
    void a.start({ space: new SlotSpace(kv, { count: 1 }) });
    await clock.elapseAsync(1000);
    expect(at).toEqual([0, 50, 150, 270, 390]);
    // Freed again and refused again: from 50 ms again.
    at.length = 0;
    a.onAssign = refusing(clock, at, 1);
    void a.release(0);
    await clock.elapseAsync(1000);
    expect(at).toEqual([1000, 1050]);
    expect(tableOf(h, kv).get(0).owner).toBe(1);
    await closeWithoutTimers(clock, h);
  });

  test('another candidate takes the slot meanwhile', async () => {
    const clock = new ManualClock();
    const h = new Harness({ clock, assignmentHold: ms50 });
    const log: string[] = [];
    const at: number[] = [];
    const a = new Instance(h, kv, 1, log);
    a.onAssign = refusing(clock, at);
    const b = new Instance(h, kv, 2, log);
    const space = new SlotSpace(kv, { count: 1 });
    void a.start({ space });
    void b.start({ space });
    await clock.elapseAsync(30_000);
    // a first (equal share, lower id); once it refuses, b at once.
    expect(log).toEqual(['1 ASSIGN 0 e1 h0', '2 ASSIGN 0 e1 h0']);
    expect(tableOf(h, kv).get(0)).toEqual(SlotEntry.owned(2, { holder: 2, epoch: 1 }));
    await closeWithoutTimers(clock, h);
  });

  test('LOCATE waits for the backoffs, then fails with UNAVAILABLE', async () => {
    const clock = new ManualClock();
    const h = new Harness({ clock });
    const log: string[] = [];
    const space = new SlotSpace(userq, { count: 16, lazy: true });
    const at: string[] = [];
    for (const id of [1, 2]) {
      const i = new Instance(h, userq, id, log);
      i.onAssign = async () => {
        at.push(`${id}@${clock.now()}`);
        throw notReady();
      };
      void i.start({ space });
    }
    await clock.elapseAsync(ms10);
    const router = new Instance(h, userq, 0, log);
    router.channel = h.link()[0];
    const start = clock.now();
    let status: Status | undefined;
    let failedAt: number | undefined;
    router.locate(3).then(
      () => {
        status = Status.ok;
      },
      (e: unknown) => {
        status = (e as SwitchboardError).status;
        failedAt = clock.now() - start;
      },
    );
    await clock.elapseAsync(30_000);
    expect(status?.known).toBe(StatusCode.unavailable);
    expect(status?.reason).toContain('not ready');
    // Both tried, both again when their backoff ended, then the
    // handover timeout (300 ms) is over: no more ASSIGNs.
    expect(at).toEqual([`1@${start}`, `2@${start}`, `1@${start + 200}`, `2@${start + 200}`]);
    expect(failedAt).toBe(300);
    expect(tableOf(h, userq).get(3)).toEqual(SlotEntry.unassigned);
    await closeWithoutTimers(clock, h);
  });

  test('a refused rebalancing migration waits for the backoff', async () => {
    const clock = new ManualClock();
    const h = new Harness({ clock, assignmentHold: ms50 });
    const log: string[] = [];
    const space = new SlotSpace(kv, { count: 4 });
    void new Instance(h, kv, 1, log).start({ space });
    void new Instance(h, kv, 2, log).start({ space });
    await clock.elapseAsync(ms50 * 2);
    const table = tableOf(h, kv);
    expect(counts(table)).toEqual({ 1: 2, 2: 2 });
    log.length = 0;
    const at: number[] = [];
    const c = new Instance(h, kv, 3, log);
    c.onAssign = refusing(clock, at, 3);
    const joined = clock.now();
    void c.start({ space });
    await clock.elapseAsync(30_000);
    expect(at.map((t) => t - joined)).toEqual([0, 200, 600, 1400]);
    expect(log).toEqual([
      ...[0, 1, 2].flatMap(() => ['1 DRAIN 0 e2 to3', '3 ASSIGN 0 e2 h1', '1 RESUME 0 e1']),
      '1 DRAIN 0 e2 to3',
      '3 ASSIGN 0 e2 h1',
      '1 FORWARD 0 e2 to3',
    ]);
    expect(counts(table)).toEqual({ 1: 1, 2: 2, 3: 1 });
    await closeWithoutTimers(clock, h);
  });

  test('a refused DRAIN of a rebalancing migration waits for a backoff', async () => {
    const clock = new ManualClock();
    const h = new Harness({ clock, assignmentHold: ms50 });
    const log: string[] = [];
    const at = new Map<number, number[]>();
    const a = new Instance(h, kv, 1, log);
    a.onDrain = async (r) => {
      const times = at.get(r.slot) ?? [];
      at.set(r.slot, times);
      times.push(clock.now());
      throw SwitchboardError.of(StatusCode.unavailable, 'busy');
    };
    const space = new SlotSpace(kv, { count: 2 });
    void a.start({ space });
    await clock.elapseAsync(ms50 * 2);
    expect(counts(tableOf(h, kv))).toEqual({ 1: 2 });
    const joined = clock.now();
    void new Instance(h, kv, 3, log).start({ space, capacity: 3 });
    await clock.elapseAsync(2000);
    // Each slot is retried at the backoff intervals, not at once.
    for (const slot of [0, 1]) {
      expect(at.get(slot)?.map((t) => t - joined)).toEqual([0, 200, 600, 1400]);
    }
    expect(counts(tableOf(h, kv))).toEqual({ 1: 2 });
    await closeWithoutTimers(clock, h);
  });

  test('rebalancing takes a slot from the next over-share instance when the first has none it can move', async () => {
    const clock = new ManualClock();
    const h = new Harness({ clock, assignmentHold: ms50 });
    const log: string[] = [];
    const space = new SlotSpace(kv, { count: 4 });
    const a = new Instance(h, kv, 1, log);
    a.onDrain = async () => {
      throw SwitchboardError.of(StatusCode.unavailable, 'busy');
    };
    void a.start({ space });
    void new Instance(h, kv, 2, log).start({ space });
    await clock.elapseAsync(ms50 * 2);
    const table = tableOf(h, kv);
    expect(counts(table)).toEqual({ 1: 2, 2: 2 });
    const [a0, a1] = table.slotsOf(1);
    const [b0] = table.slotsOf(2);
    log.length = 0;
    void new Instance(h, kv, 3, log).start({ space });
    await clock.elapseAsync(ms50);
    expect(log).toEqual([
      `1 DRAIN ${a0} e2 to3`,
      `1 RESUME ${a0} e1`,
      `1 DRAIN ${a1} e2 to3`,
      `1 RESUME ${a1} e1`,
      `2 DRAIN ${b0} e2 to3`,
      `3 ASSIGN ${b0} e2 h2`,
      `2 FORWARD ${b0} e2 to3`,
    ]);
    expect(counts(table)).toEqual({ 1: 2, 2: 1, 3: 1 });
    await closeWithoutTimers(clock, h);
  });

  test('close leaves no timers: backoffs, LOCATE, settle window', async () => {
    const clock = new ManualClock();
    const h = new Harness({ clock, handoverTimeout: 10_000, holdingSettle: 1000 });
    const log: string[] = [];
    const a = new Instance(h, kv, 1, log);
    a.onAssign = async () => {
      throw notReady();
    };
    void a.start({ space: new SlotSpace(kv, { count: 2 }) });
    const u = new Instance(h, userq, 2, log);
    u.onAssign = async () => {
      throw notReady();
    };
    void u.start({ space: new SlotSpace(userq, { count: 4, lazy: true }) });
    // Through the settle window: both kv slots in their backoff.
    await clock.elapseAsync(1100);
    expect(log).toHaveLength(2);
    let status: Status | undefined;
    u.locate(1).then(
      () => {
        status = Status.ok;
      },
      (e: unknown) => {
        status = (e as SwitchboardError).status;
      },
    );
    await clock.elapseAsync(ms10);
    // A new settle window.
    void a.slots(new SlotSpace(kv, { count: 2 }));
    await clock.elapseAsync(ms10);
    expect(status).toBeUndefined();
    await closeWithoutTimers(clock, h);
    expect(status?.known).toBe(StatusCode.goingAway);
  });

  test('rejects invalid options', () => {
    expect(() => new NamingService({ assignBackoff: 0 })).toThrow(RangeError);
    expect(() => new NamingService({ assignBackoff: 2000, assignBackoffMax: 1000 })).toThrow(
      RangeError,
    );
    expect(() => new NamingService({ holdingSettle: -1000 })).toThrow(RangeError);
    expect(() => new NamingService({ resumeAttempts: 0 })).toThrow(RangeError);
    const service = new NamingService({ assignmentHold: 0 });
    expect(service.resumeAttempts).toBe(5);
    expect(service.assignBackoff).toBe(200);
    expect(service.assignBackoffMax).toBe(10_000);
    expect(service.holdingSettle).toBe(1000);
    void service.close();
  });
});

describe('RESUME after a rollback', () => {
  /**
   * Two zone instances, 1 owning slot 1, 2 refusing every ASSIGN, at 100 ms
   * of fake time. The RESUMEs 1 gets are timed in [at] (ms); [answerFrom]
   * is the attempt (1 = the first) from which it answers them.
   */
  async function zones(
    clock: ManualClock,
    h: Harness,
    log: string[],
    at: number[],
    options: { answerFrom?: number } = {},
  ): Promise<[Instance, Instance]> {
    const answerFrom = options.answerFrom ?? 1 << 30;
    const a = new Instance(h, zone, 1, log);
    a.heartbeat = false;
    a.onResume = () => {
      at.push(clock.now());
      return at.length >= answerFrom ? Promise.resolve() : never();
    };
    const b = new Instance(h, zone, 2, log);
    b.onAssign = async () => {
      throw SwitchboardError.of(StatusCode.unavailable, 'no');
    };
    void a.start({ space: staticSpace(zone, 4), capacity: 0 });
    void b.start({ space: staticSpace(zone, 4), capacity: 0 });
    await clock.elapseAsync(ms10);
    void a.claim(1);
    await clock.elapseAsync(90);
    log.length = 0;
    return [a, b];
  }

  /** Migrates slot 1 to 2: rolled back, RESUME to 1 at once. */
  async function rollBack(h: Harness): Promise<Migration> {
    const m = new Migration(h.link()[0], zone, 1, { to: 2 });
    await pump();
    return m;
  }

  test('one not answered is sent again at the backoff intervals, then the slot is free with its holder', async () => {
    // Not answered within the handover timeout (300 ms): failed.
    const clock = new ManualClock();
    const h = new Harness({ clock });
    const log: string[] = [];
    const at: number[] = [];
    await zones(clock, h, log, at);
    const m = await rollBack(h);
    let status: Status | undefined;
    void m.done.then((s) => {
      status = s;
    });
    await clock.elapseAsync(ms10);
    expect(status?.known).toBe(StatusCode.unavailable);
    const table = tableOf(h, zone);
    expect(table.get(1)).toEqual(SlotEntry.owned(1, { holder: 1, epoch: 1 }));
    await clock.elapseAsync(10_000);
    // Each fails 300 ms later and is sent again after 200, 400, 800,
    // then 1600 ms.
    expect(at).toEqual([100, 600, 1300, 2400, 4300]);
    expect(log.filter((l) => l.includes('RESUME'))).toHaveLength(5);
    // The fifth failure: free, holder kept, nothing assigned (static).
    expect(table.get(1)).toEqual(SlotEntry.free({ holder: 1, epoch: 1 }));
    await closeWithoutTimers(clock, h);
  });

  test('one answered at the third attempt ends the retries', async () => {
    const clock = new ManualClock();
    const h = new Harness({ clock });
    const log: string[] = [];
    const at: number[] = [];
    await zones(clock, h, log, at, { answerFrom: 3 });
    await rollBack(h);
    await clock.elapseAsync(10_000);
    expect(at).toEqual([100, 600, 1300]);
    expect(tableOf(h, zone).get(1)).toEqual(SlotEntry.owned(1, { holder: 1, epoch: 1 }));
    await closeWithoutTimers(clock, h);
  });

  test('the attempts can be set; the owner going down ends them', async () => {
    const clock = new ManualClock();
    const h = new Harness({ clock, resumeAttempts: 2 });
    const log: string[] = [];
    const at: number[] = [];
    const [a] = await zones(clock, h, log, at);
    await rollBack(h);
    await clock.elapseAsync(2000);
    expect(at).toEqual([100, 600]);
    expect(tableOf(h, zone).get(1)).toEqual(SlotEntry.free({ holder: 1, epoch: 1 }));

    // Going down while a RESUME waits for its backoff: no more.
    at.length = 0;
    void a.claim(1);
    await clock.elapseAsync(ms10);
    await rollBack(h);
    await clock.elapseAsync(350);
    expect(at).toHaveLength(1);
    void a.close();
    await clock.elapseAsync(2000);
    expect(at).toHaveLength(1);
    expect(tableOf(h, zone).get(1).owner).toBe(0);
    await closeWithoutTimers(clock, h);
  });

  test('a managed slot set free goes to another instance first', async () => {
    const clock = new ManualClock();
    const h = new Harness({ clock, resumeAttempts: 1 });
    const log: string[] = [];
    // a owns slot 0, refuses to DRAIN it and never answers RESUME.
    const a = new Instance(h, kv, 1, log);
    a.heartbeat = false;
    a.onResume = () => never();
    a.onDrain = async () => {
      throw SwitchboardError.of(StatusCode.unavailable, 'busy');
    };
    const space = new SlotSpace(kv, { count: 1 });
    void a.start({ space });
    await clock.elapseAsync(ms10);
    expect(tableOf(h, kv).get(0).owner).toBe(1);
    void new Instance(h, kv, 2, log).start({ space });
    await clock.elapseAsync(ms10);
    log.length = 0;
    new Migration(h.link()[0], kv, 0, { to: 2 });
    await clock.elapseAsync(290);
    expect(log).toEqual(['1 DRAIN 0 e2 to2', '1 RESUME 0 e1']);
    expect(tableOf(h, kv).get(0).owner).toBe(1);
    // The RESUME failed: free with holder a, which is in its backoff
    // for it and passed over; b fetches it from a.
    await clock.elapseAsync(ms10);
    expect(log.at(-1)).toBe('2 ASSIGN 0 e2 h1');
    expect(tableOf(h, kv).get(0)).toEqual(SlotEntry.owned(2, { holder: 2, epoch: 2 }));
    await closeWithoutTimers(clock, h);
  });
});

describe('settle window after SLOTS', () => {
  test('a holder whose HOLDING comes late after the hold gets its slot', async () => {
    // A restarted naming service; its hold is over before anyone is
    // back.
    const clock = new ManualClock();
    const h = new Harness({ clock, assignmentHold: ms50, holdingSettle: 1000 });
    const log: string[] = [];
    const space = new SlotSpace(kv, { count: 4 });
    await clock.elapseAsync(ms50 * 2);
    expect(h.service.isHoldingAssignments).toBe(false);
    // b, which holds nothing, is back first with capacity.
    void new Instance(h, kv, 2, log).start({ space });
    await clock.elapseAsync(ms10);
    // a holds the storage of slot 2; its HOLDING comes 300 ms after
    // its SLOTS.
    const a = new Instance(h, kv, 1, log);
    void a.start({ space });
    await clock.elapseAsync(300);
    expect(log).toEqual([]);
    const discard = a.holding([2]);
    await clock.elapseAsync(ms10);
    expect(log).toEqual(['1 ASSIGN 2 e1 h1']);
    let discarded: readonly number[] | undefined;
    void discard.then((d) => {
      discarded = d;
    });
    await pump();
    expect(discarded).toEqual([]);
    // The rest once the window is over: 1 s after it opened (the most
    // it is extended to here, 5 x 50 ms being shorter).
    await clock.elapseAsync(600);
    expect(log).toHaveLength(1);
    await clock.elapseAsync(200);
    const table = tableOf(h, kv);
    expect(allOwned(table)).toBe(true);
    expect(table.get(2)).toEqual(SlotEntry.owned(1, { holder: 1, epoch: 1 }));
    expect(log.slice(1).filter((l) => !l.endsWith('e1 h0'))).toEqual([]);
    expect(counts(table)).toEqual({ 1: 2, 2: 2 });
    void h.close();
    await pump();
    await clock.elapseAsync(ms50 * 2);
    expect(clock.pendingCount).toBe(0);
  });

  test('arrivals extend the window until it goes quiet, up to a bound', async () => {
    // Quiet for 200 ms, at most 5 x 50 ms = 250 ms.
    const clock = new ManualClock();
    const h = new Harness({ clock, assignmentHold: ms50, holdingSettle: 200 });
    const log: string[] = [];
    await clock.elapseAsync(ms50 * 2);
    const a = new Instance(h, kv, 1, log);
    void a.start({ space: new SlotSpace(kv, { count: 4 }) });
    await clock.elapseAsync(150);
    expect(log).toEqual([]);
    // A CLAIM is served at once and extends the window past 300 ms.
    let epoch: number | undefined;
    void a.claim(3).then((e) => {
      epoch = e;
    });
    await clock.elapseAsync(ms10);
    expect(epoch).toBe(1);
    expect(log).toEqual(['1 ASSIGN 3 e1 h0']);
    await clock.elapseAsync(80);
    expect(log).toHaveLength(1);
    // 250 ms after it opened it is over all the same.
    await clock.elapseAsync(20);
    expect(log).toHaveLength(4);
    expect(allOwned(h.service.slotTable(kv))).toBe(true);
    void h.close();
    await pump();
    await clock.elapseAsync(ms50 * 2);
    expect(clock.pendingCount).toBe(0);
  });

  test('LOCATE assigns at once within the window', async () => {
    const clock = new ManualClock();
    const h = new Harness({ clock, holdingSettle: 1000 });
    const log: string[] = [];
    const space = new SlotSpace(kv, { count: 4 });
    const a = new Instance(h, kv, 1, log);
    void a.start({ space, capacity: 1 });
    void new Instance(h, kv, 2, log).start({ space, capacity: 3 });
    await clock.elapseAsync(ms10);
    void a.holding([1]);
    await clock.elapseAsync(ms10);
    // A slot with a known holder goes back to it within the window.
    expect(log).toEqual(['1 ASSIGN 1 e1 h1']);
    const router = new Instance(h, kv, 0, log);
    router.channel = h.link()[0];
    let located: LocateResponse | undefined;
    void router.locate(0).then((r) => {
      located = r;
    });
    await clock.elapseAsync(ms10);
    expect(located).toEqual(new LocateResponse(SlotState.owned, 2, 1));
    expect(log).toEqual(['1 ASSIGN 1 e1 h1', '2 ASSIGN 0 e1 h0']);
    void h.close();
    await pump();
    await clock.elapseAsync(ms50 * 2);
    expect(clock.pendingCount).toBe(0);
  });
});

describe('declared deadlines, no heartbeat', () => {
  const buffer = talkOptions(serverOptions).extendBuffer;
  const handover = 300;

  type Declared = [number | undefined, number | undefined];

  test('a HOLDING waiting for an ASSIGN elsewhere is told the handover timeout once', async () => {
    const clock = new ManualClock();
    const h = new Harness({ clock, handoverTimeout: handover });
    const log: string[] = [];
    const a = new Instance(h, kv, 1, log);
    const b = new Instance(h, kv, 2, log);
    b.heartbeat = false;
    const decide = new Deferred<void>();
    b.onAssign = async () => {
      await decide.promise;
      return new AssignResponse();
    };
    void a.start();
    void b.start({ space: new SlotSpace(kv, { count: 2 }) });
    await clock.elapseAsync(ms10);
    expect(log).toHaveLength(2);
    const declared: Declared[] = [];
    let discard: readonly number[] | undefined;
    void a.channel
      .startRequest('HOLDING', new HoldingRequest(kv, [0, 1]).encode(), {
        onExtend: (extension) => declared.push([extension.deadline, extension.renew]),
      })
      .response.then((r) => {
        discard = HoldingResponse.decode(r.payload).discard;
      });
    await clock.elapseAsync(ms10);
    // Both ASSIGNs started together: one bound covers both.
    expect(declared).toEqual([[handover - ms10 + buffer, undefined]]);
    decide.resolve();
    await clock.elapseAsync(ms10);
    expect(discard).toEqual([0, 1]);
    expect(declared).toHaveLength(1);
    await closeWithoutTimers(clock, h);
  });

  test('a RELEASE of a slot being assigned to the caller waits for the ASSIGN, told the handover timeout once', async () => {
    const clock = new ManualClock();
    const h = new Harness({ clock, handoverTimeout: handover });
    const log: string[] = [];
    const decide = new Deferred<void>();
    // Slot 0 is accepted, slot 1 refused, both once decided.
    const a = new Instance(h, kv, 1, log);
    a.heartbeat = false;
    a.onAssign = async (r) => {
      await decide.promise;
      if (r.slot === 1) {
        throw SwitchboardError.of(StatusCode.unavailable, 'no');
      }
      return new AssignResponse();
    };
    void a.start({ space: new SlotSpace(kv, { count: 2 }) });
    await clock.elapseAsync(ms10);
    expect(log).toEqual(['1 ASSIGN 0 e1 h0', '1 ASSIGN 1 e1 h0']);
    const declared = new Map<number, Declared[]>([
      [0, []],
      [1, []],
    ]);
    const outcome = new Map<number, unknown>();
    for (const slot of [0, 1]) {
      void a.channel
        .startRequest('RELEASE', new ReleaseRequest(kv, slot).encode(), {
          onExtend: (extension) => declared.get(slot)?.push([extension.deadline, extension.renew]),
        })
        .response.then(
          () => {
            outcome.set(slot, 'released');
          },
          (e: unknown) => {
            outcome.set(slot, e);
          },
        );
    }
    await clock.elapseAsync(ms10);
    expect(outcome.size, 'both wait for their ASSIGN').toBe(0);
    for (const slot of [0, 1]) {
      expect(declared.get(slot)).toEqual([[handover - ms10 + buffer, undefined]]);
    }
    decide.resolve();
    await pump();
    // Assigned: released. Refused: not this instance's.
    expect(outcome.get(0)).toBe('released');
    expect(isStatus(outcome.get(1), StatusCode.permissionDenied)).toBe(true);
    for (const slot of [0, 1]) {
      expect(declared.get(slot)).toHaveLength(1);
    }
    expect(tableOf(h, kv).get(1).state).toBe(SlotState.free);
    await closeWithoutTimers(clock, h);
  });

  test('a CLAIM made during the hold is told the time left in it', async () => {
    const clock = new ManualClock();
    const h = new Harness({ clock, assignmentHold: 2000 });
    const log: string[] = [];
    const a = new Instance(h, zone, 1, log);
    a.heartbeat = false;
    void a.start({ space: staticSpace(zone, 4), capacity: 0 });
    await clock.elapseAsync(500);
    const declared: Declared[] = [];
    let epoch: number | undefined;
    void a.channel
      .startRequest('CLAIM', new ClaimRequest(zone, 1).encode(), {
        onExtend: (extension) => declared.push([extension.deadline, extension.renew]),
      })
      .response.then((r) => {
        epoch = ClaimResponse.decode(r.payload).epoch;
      });
    await clock.elapseAsync(ms10);
    expect(declared).toEqual([[1500 + buffer, undefined]]);
    await clock.elapseAsync(1500);
    expect(epoch).toBe(1);
    // Then the ASSIGN to it: one more, the handover timeout.
    expect(declared).toEqual([
      [1500 + buffer, undefined],
      [handover + buffer, undefined],
    ]);
    await closeWithoutTimers(clock, h);
  });

  test('a LOCATE waiting out a backoff is told the wait it knows', async () => {
    const clock = new ManualClock();
    const h = new Harness({ clock, handoverTimeout: handover });
    const log: string[] = [];
    const space = new SlotSpace(userq, { count: 16, lazy: true });
    for (const id of [1, 2]) {
      const i = new Instance(h, userq, id, log);
      i.heartbeat = false;
      i.onAssign = async () => {
        throw SwitchboardError.of(StatusCode.unavailable);
      };
      void i.start({ space });
    }
    await clock.elapseAsync(ms10);
    // A first LOCATE puts both candidates in their backoff (200 ms).
    const [router] = h.link();
    router.request('LOCATE', new LocateRequest(userq, 3).encode()).catch(() => {});
    await clock.elapseAsync(ms50);
    expect(log).toHaveLength(2);
    const declared: Declared[] = [];
    let error: unknown;
    router
      .startRequest('LOCATE', new LocateRequest(userq, 3).encode(), {
        onExtend: (extension) => declared.push([extension.deadline, extension.renew]),
      })
      .response.then(
        () => {},
        (e: unknown) => {
          error = e;
        },
      );
    await clock.elapseAsync(ms10);
    // It waits for a backoff, at most the handover timeout.
    expect(log).toHaveLength(2);
    expect(declared).toEqual([[handover + buffer, undefined]]);
    await clock.elapseAsync(1000);
    expect(isStatus(error, StatusCode.unavailable)).toBe(true);
    await closeWithoutTimers(clock, h);
  });

  test('a MIGRATE queued behind another is told each step of it', async () => {
    const clock = new ManualClock();
    const h = new Harness({ clock, handoverTimeout: handover });
    const log: string[] = [];
    const a = new Instance(h, zone, 1, log);
    a.heartbeat = false;
    const b = new Instance(h, zone, 2, log);
    b.heartbeat = false;
    void a.start({ space: staticSpace(zone, 4), capacity: 0 });
    void b.start({ space: staticSpace(zone, 4), capacity: 0 });
    await clock.elapseAsync(ms10);
    void a.claim(1);
    void a.claim(2);
    await clock.elapseAsync(ms10);
    const draining = new Deferred<void>();
    a.onDrain = () => draining.promise;
    const [operator] = h.link();
    const first = operator.streamRequest(
      'MIGRATE',
      new MigrateRequest(zone, 1, { to: 2 }).encode(),
    );
    drain(first.items);
    first.done.catch(() => {});
    await clock.elapseAsync(ms10);
    const declared: Declared[] = [];
    const second = operator.streamRequest(
      'MIGRATE',
      new MigrateRequest(zone, 2, { to: 2 }).encode(),
      { onExtend: (extension) => declared.push([extension.deadline, extension.renew]) },
    );
    drain(second.items);
    let outcome: unknown;
    second.done.then(
      () => {
        outcome = 'done';
      },
      (e: unknown) => {
        outcome = e;
      },
    );
    await clock.elapseAsync(ms10);
    // Behind the first, in its DRAIN since 10 ms.
    expect(declared).toEqual([[handover - ms10 + buffer, undefined]]);
    draining.resolve();
    await clock.elapseAsync(ms50);
    expect(outcome).toBe('done');
    expect(log.filter((l) => l.includes('DRAIN'))).toHaveLength(2);
    await closeWithoutTimers(clock, h);
  });
});

test('close leaves no timers: grace, migration, held requests', async () => {
  const clock = new ManualClock();
  const h = new Harness({ clock, assignmentHold: ms50 });
  const log: string[] = [];
  const a = new Instance(h, kv, 1, log);
  const b = new Instance(h, kv, 2, log);
  const c = new Instance(h, zone, 3, log);
  const space = new SlotSpace(kv, { count: 4 });
  void a.start({ space });
  void b.start({ space });
  await clock.elapseAsync(ms50 * 2);
  // A grace timer.
  void a.close();
  await clock.elapseAsync(ms10);
  // A migration stuck in DRAIN.
  void c.start({ space: staticSpace(zone, 2), capacity: 0 });
  await clock.elapseAsync(ms10);
  void c.claim(0);
  await clock.elapseAsync(ms10);
  const d = new Instance(h, zone, 4, log);
  void d.start({ space: staticSpace(zone, 2), capacity: 0 });
  await clock.elapseAsync(ms10);
  c.onDrain = () => never();
  const m = new Migration(h.link()[0], zone, 0, { to: 4 });
  await clock.elapseAsync(ms10);
  expect(log.at(-1)).toBe('3 DRAIN 0 e2 to4');
  void h.close();
  // The scripted instances' own EXTEND timers notice on their next tick.
  await clock.elapseAsync(ms50 * 2);
  let status: Status | undefined;
  void m.done.then((s) => {
    status = s;
  });
  await pump();
  expect(status?.known).not.toBe(StatusCode.ok);
  expect(clock.pendingCount).toBe(0);
});
