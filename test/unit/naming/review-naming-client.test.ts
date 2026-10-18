/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Review findings (naming layer), the client half:
//
// 1. Design section 5 puts an AbortSignal on every operation that waits
//    ("connect, open, identify, ping, request, resolve"). NamingClient takes
//    one for `renew`, `connectTo` and `migrate`, but not for the requests
//    that can wait longest: `locate` (an ASSIGN, the assignment hold, the
//    backoffs, up to `handoverTimeout`), `claim`, `release`, `register`,
//    `defineSlots` and `declareHolding`. The case passes a signal the way
//    the other calls take it; it is ignored and the LOCATE waits on.
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
  Harness,
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

test.fails('locate takes an AbortSignal, as the waiting calls of design section 5 do', async () => {
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
  const locate = client.locate as (
    type: Name,
    slot: number,
    options: { signal: AbortSignal },
  ) => ReturnType<NamingClient['locate']>;
  const located = locate.call(client, zone, 1, { signal: controller.signal });
  located.catch(() => {});
  await clock.elapseAsync(ms10);
  controller.abort(new Error('caller gave up'));
  await clock.elapseAsync(ms10);
  try {
    expect(await isSettled(located)).toBe(true);
    await expect(located).rejects.toThrow('caller gave up');
  } finally {
    loading.resolve(AssignResult.holding);
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
