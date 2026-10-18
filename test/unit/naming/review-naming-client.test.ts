/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Review findings (naming layer), the client half:
//
// 1. Fixed: design section 5 puts an AbortSignal on every operation that
//    waits ("connect, open, identify, ping, request, resolve"). NamingClient
//    took one for `renew`, `connectTo` and `migrate`, but not for the
//    requests that can wait longest: `locate` (an ASSIGN, the assignment
//    hold, the backoffs, up to `handoverTimeout`), `claim`, `release`,
//    `register`, `defineSlots` and `declareHolding`. They take one now: an
//    abort rejects with its reason, a call waiting for the request limit
//    leaves the queue, a sent LOCATE or CLAIM is cancelled, a sent RELEASE
//    runs to its answer, and an unanswered REGISTER is withdrawn.
//
// 2. `NamingClient.on('event')` listeners run synchronously, inside the
//    client's step (design section 5's Emitter), where Dart's broadcast
//    stream delivers after it. During a resync the DOWNs of the records the
//    new snapshot no longer has reach a TS listener before the step marks
//    the table synced, so the listener sees `isSynced` false (a Dart
//    listener sees it true). Design section 12 lists this for PeerSet
//    (item 16) and not for the naming client or service.

import { afterEach, beforeEach, expect, test } from 'vitest';
import { Name } from '../../../src/core/name.ts';
import { AssignResult, NamingClient } from '../../../src/core/naming/naming-client.ts';
import { SlotMode } from '../../../src/core/naming/naming-protocol.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';
import { ManualClock } from '../../../src/testing/manual-clock.ts';
import {
  Connector,
  clientOptions,
  Harness,
  isRequestFor,
  isSettled,
  ms10,
  reconnectDelay,
  register,
  until,
  within,
} from '../../support/naming/harness.ts';
import { RecordingHandler, zone } from '../../support/naming/sharding-harness.ts';

let h: Harness;
const clients: NamingClient[] = [];

beforeEach(() => {
  h = new Harness();
});

afterEach(async () => {
  for (const client of clients.splice(0)) {
    await client.close();
  }
  await h.close();
});

/** A client of [virtual] with [zone] registered and its space defined (managed, lazy), on [clock]. */
async function zoneClient(
  virtual: Harness,
  clock: ManualClock,
  options: { maxOutgoingRequests?: number } = {},
): Promise<{ client: NamingClient; handler: RecordingHandler; connector: Connector }> {
  const connector = new Connector(virtual, { options: { ...clientOptions, ...options } });
  const client = new NamingClient(connector.call, { reconnectDelay, clock });
  const handler = new RecordingHandler('A', []);
  client.slotHandler = handler;
  void client.start();
  void client.register(zone, [], { instance: 1 });
  await clock.elapseAsync(ms10);
  void client.defineSlots(zone, { count: 4, mode: SlotMode.managed, lazy: true });
  await clock.elapseAsync(ms10);
  return { client, handler, connector };
}

/** The requests for [procedure] the client side of [virtual]'s first link sent. */
function sentRequests(virtual: Harness, procedure: string): number {
  return (virtual.clients[0]?.sent ?? []).filter(isRequestFor(procedure)).length;
}

test('locate takes an AbortSignal, as the waiting calls of design section 5 do', async () => {
  const clock = new ManualClock();
  const virtual = new Harness({ clock, handoverTimeout: 60_000 });
  const client = new NamingClient(new Connector(virtual).call, { reconnectDelay, clock });
  const handler = new RecordingHandler('A', []);
  client.slotHandler = handler;
  void client.start();
  void client.register(zone, [], { instance: 1 });
  await clock.elapseAsync(ms10);
  void client.defineSlots(zone, { count: 4, mode: SlotMode.managed, lazy: true });
  await clock.elapseAsync(ms10);
  // The ASSIGN the LOCATE waits for is slow.
  const loading = new Deferred<AssignResult>();
  handler.assign = () => loading.promise;
  const controller = new AbortController();
  const located = client.locate(zone, 1, { signal: controller.signal });
  located.catch(() => {});
  await clock.elapseAsync(ms10);
  // The LOCATE waits at the naming service for the ASSIGN.
  expect(virtual.service.waitingRequestCount).toBe(1);
  controller.abort(new Error('caller gave up'));
  await clock.elapseAsync(ms10);
  try {
    expect(await isSettled(located)).toBe(true);
    await expect(located).rejects.toThrow('caller gave up');
    // Cancelled there too: the naming service no longer waits for it.
    expect(virtual.service.waitingRequestCount).toBe(0);
  } finally {
    loading.resolve(AssignResult.holding);
    void client.close();
    void virtual.close();
    await clock.elapseAsync(1000);
  }
});

test('an aborted signal rejects the slot calls with its reason and sends nothing', async () => {
  const clock = new ManualClock();
  const virtual = new Harness({ clock });
  const { client } = await zoneClient(virtual, clock);
  const reason = new Error('given up before');
  const signal = AbortSignal.abort(reason);
  try {
    const calls = [
      client.locate(zone, 1, { signal }),
      client.claim(zone, 2, { signal }),
      client.release(zone, 2, { signal }),
      client.declareHolding(zone, [3], { signal }),
      client.defineSlots(zone, { count: 4, mode: SlotMode.managed, lazy: true, signal }),
      client.register(zone, [], { instance: 2, signal }),
    ];
    for (const call of calls) {
      await expect(call).rejects.toBe(reason);
    }
    await clock.elapseAsync(ms10);
    expect(sentRequests(virtual, 'LOCATE')).toBe(0);
    expect(sentRequests(virtual, 'CLAIM')).toBe(0);
    expect(sentRequests(virtual, 'RELEASE')).toBe(0);
    expect(sentRequests(virtual, 'HOLDING')).toBe(0);
    expect(sentRequests(virtual, 'REGISTER')).toBe(1);
  } finally {
    void client.close();
    void virtual.close();
    await clock.elapseAsync(1000);
  }
});

test('a slot call waiting for the request limit leaves the queue when its signal aborts', async () => {
  const clock = new ManualClock();
  const virtual = new Harness({ clock, handoverTimeout: 60_000 });
  const { client, handler } = await zoneClient(virtual, clock, { maxOutgoingRequests: 1 });
  const loading = new Deferred<AssignResult>();
  handler.assign = () => loading.promise;
  try {
    // The first LOCATE takes the one request the channel allows, and waits.
    const first = client.locate(zone, 1);
    first.catch(() => {});
    await clock.elapseAsync(ms10);
    const controller = new AbortController();
    const second = client.locate(zone, 2, { signal: controller.signal });
    second.catch(() => {});
    await clock.elapseAsync(ms10);
    expect(sentRequests(virtual, 'LOCATE')).toBe(1);
    controller.abort(new Error('queued and given up'));
    await expect(second).rejects.toThrow('queued and given up');
    loading.resolve(AssignResult.holding);
    expect((await first).owner).toBe(1);
    await clock.elapseAsync(ms10);
    // It never went out.
    expect(sentRequests(virtual, 'LOCATE')).toBe(1);
  } finally {
    loading.resolve(AssignResult.holding);
    void client.close();
    void virtual.close();
    await clock.elapseAsync(1000);
  }
});

test('a RELEASE already sent runs to its answer when its signal aborts', async () => {
  const clock = new ManualClock();
  const virtual = new Harness({ clock });
  const { client } = await zoneClient(virtual, clock);
  try {
    const claimed = client.claim(zone, 1);
    await clock.elapseAsync(ms10);
    expect(await claimed).toBeGreaterThan(0);
    expect(client.servedSlots(zone).has(1)).toBe(true);
    const controller = new AbortController();
    const released = client.release(zone, 1, { signal: controller.signal });
    controller.abort(new Error('too late'));
    await clock.elapseAsync(ms10);
    await expect(released).resolves.toBeUndefined();
    // This client and the naming service agree: the slot is free.
    expect(client.servedSlots(zone).has(1)).toBe(false);
    expect(virtual.service.slotTable(zone)?.get(1).isFree).toBe(true);
  } finally {
    void client.close();
    void virtual.close();
    await clock.elapseAsync(1000);
  }
});

test('aborting a registration not answered yet withdraws it', async () => {
  const clock = new ManualClock();
  // The assignment hold keeps REGISTER for any id unanswered.
  const virtual = new Harness({ clock, assignmentHold: 1000 });
  const connector = new Connector(virtual);
  const client = new NamingClient(connector.call, { reconnectDelay, clock });
  void client.start();
  const assigned: number[] = [];
  const controller = new AbortController();
  const registered = client.register(Name.of('npc'), [], {
    signal: controller.signal,
    onAssigned: (id) => assigned.push(id),
  });
  registered.catch(() => {});
  try {
    await clock.elapseAsync(ms10);
    expect(sentRequests(virtual, 'REGISTER')).toBe(1);
    controller.abort(new Error('not wanted any more'));
    await expect(registered).rejects.toThrow('not wanted any more');
    // The hold ends: the naming service registers it, and the client, which
    // no longer tracks it, removes it at once.
    await clock.elapseAsync(1000);
    await clock.elapseAsync(ms10);
    expect(sentRequests(virtual, 'UNREGSTR')).toBe(1);
    expect(virtual.service.records('npc')).toEqual([]);
    expect(assigned).toEqual([]);
  } finally {
    void client.close();
    void virtual.close();
    await clock.elapseAsync(1000);
  }
});

test.fails('an event listener sees the resync it is told about as done, as a Dart listener does', async () => {
  const connector = new Connector(h);
  const client = new NamingClient(connector.call, { reconnectDelay });
  clients.push(client);
  await client.start();
  await within(client.synced);
  // Another channel's record, mirrored.
  const [other] = h.link();
  const id = await register(other, 'npc');
  await until(() => client.table.size === 1);
  // The naming service restarts without it.
  const old = h;
  h = new Harness();
  connector.harness = h;
  await old.close();
  const seen: boolean[] = [];
  client.on('event', (event) => {
    if (!event.up && event.record.address.instance === id) {
      seen.push(client.isSynced);
    }
  });
  await until(() => client.isSynced);
  expect(seen).toEqual([true]);
});

test('control: the same DOWN read from events after the resync', async () => {
  const connector = new Connector(h);
  const client = new NamingClient(connector.call, { reconnectDelay });
  clients.push(client);
  await client.start();
  await within(client.synced);
  const [other] = h.link();
  await register(other, 'npc');
  await until(() => client.table.size === 1);
  const events = client.events[Symbol.asyncIterator]();
  const old = h;
  h = new Harness();
  connector.harness = h;
  await old.close();
  const next = await within(events.next());
  expect(next.value?.up).toBe(false);
  // Read asynchronously, like Dart's listener: the resync is over by then.
  expect(client.isSynced).toBe(true);
  await events.return?.();
  expect(Name.of('npc').equals(next.value?.record.address.type ?? Name.empty)).toBe(true);
});
