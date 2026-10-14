/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// StaticResolver (with its configured slot tables) and EndpointResolver. A
// port of the Dart resolver_test.dart.

import { describe, expect, test } from 'vitest';
import { ServiceAddress } from '../../../src/core/address/index.ts';
import { Name } from '../../../src/core/name.ts';
import { SlotEntry, SlotSpace, serviceRecord } from '../../../src/core/naming/naming-protocol.ts';
import { StatusCode } from '../../../src/core/status.ts';
import type { ServiceEvent } from '../../../src/core/switchboard/contract.ts';
import { EndpointResolver, StaticResolver } from '../../../src/core/switchboard/resolver.ts';
import { pump, rejectsWith } from '../../support/node/harness.ts';

const npc = Name.of('npc');

function closedError(action: () => unknown): void {
  let error: unknown;
  try {
    action();
  } catch (e) {
    error = e;
  }
  expect((error as { code?: unknown } | undefined)?.code).toBe(StatusCode.failedPrecondition);
}

describe('StaticResolver', () => {
  test('close ends resolution and refuses changes before mutating', async () => {
    const record = serviceRecord(new ServiceAddress(npc, 1));
    const r = new StaticResolver([record]);
    r.defineSlots(new SlotSpace(npc, { count: 2 }));
    r.setSlot(npc, 0, SlotEntry.owned(1, { epoch: 1 }));
    await r.close();
    await r.close();
    await rejectsWith(r.resolve(npc), StatusCode.failedPrecondition);
    await rejectsWith(r.locateSlot(npc, 0), StatusCode.failedPrecondition);
    closedError(() => r.add(serviceRecord(new ServiceAddress(npc, 2))));
    closedError(() => r.remove(record.address));
    closedError(() => r.defineSlots(new SlotSpace(npc, { count: 3 })));
    closedError(() => r.removeSlots(npc));
    closedError(() => r.setSlot(npc, 0, SlotEntry.unassigned));
    expect([...r.table.values()]).toEqual([record]);
    expect(r.slotTable(npc)?.count).toBe(2);
    expect(r.slotOwner(npc, 0)?.owner).toBe(1);
  });

  test('close completes while a consumer of events is not reading', async () => {
    const r = new StaticResolver();
    const events = r.events[Symbol.asyncIterator]();
    const slots = r.slotEvents[Symbol.asyncIterator]();
    r.add(serviceRecord(new ServiceAddress(npc, 1)));
    r.defineSlots(new SlotSpace(npc, { count: 2 }));
    r.setSlot(npc, 1, SlotEntry.owned(1, { epoch: 1 }));
    await r.close();
    // What was published before the close is still delivered, then the end.
    expect((await events.next()).value?.up).toBe(true);
    expect((await events.next()).done).toBe(true);
    expect((await slots.next()).value?.slot).toBe(1);
    expect((await slots.next()).done).toBe(true);
  });

  test('resolves by type and emits events', async () => {
    const r = new StaticResolver([
      serviceRecord(new ServiceAddress(npc, 1), { endpoints: ['tcp://a:1'] }),
      serviceRecord(new ServiceAddress(Name.of('api'), 2), { endpoints: ['tcp://b:1'] }),
    ]);
    await r.ready;
    const npcs = await r.resolve(npc);
    expect(npcs.map((record) => record.address.instance)).toEqual([1]);
    expect(await r.resolve(Name.of('none'))).toEqual([]);
    const events: ServiceEvent[] = [];
    const reading = (async () => {
      for await (const event of r.events) events.push(event);
    })();
    await pump(1);
    r.add(serviceRecord(new ServiceAddress(npc, 3), { endpoints: ['tcp://c:1'] }));
    r.remove(new ServiceAddress(npc, 1));
    r.remove(new ServiceAddress(npc, 1));
    await pump(1);
    expect(events.map((e) => e.up)).toEqual([true, false]);
    expect(events[1]?.record.address.equals(new ServiceAddress(npc, 1))).toBe(true);
    expect((await r.resolve(npc)).map((record) => record.address.instance)).toEqual([3]);
    await r.close();
    await reading;
  });
  // Not in the Dart file: the slot tables StaticResolver carries.
  test('slot tables: changes, redefinition and removal are reported', async () => {
    const r = new StaticResolver();
    const events = r.slotEvents[Symbol.asyncIterator]();
    expect(r.slotTable(npc)).toBeUndefined();
    expect(() => r.setSlot(npc, 0, SlotEntry.owned(1))).toThrow(/no slot space/);
    r.defineSlots(new SlotSpace(npc, { count: 4 }));
    expect(() => r.setSlot(npc, 4, SlotEntry.owned(1))).toThrow(RangeError);
    r.setSlot(npc, 1, SlotEntry.owned(2, { epoch: 1 }));
    r.setSlot(npc, 1, SlotEntry.owned(2, { epoch: 1 }));
    r.setSlot(npc, 2, SlotEntry.migrating(2, 3, { epoch: 2 }));
    r.setSlot(npc, 3, SlotEntry.free({ holder: 3 }));
    expect([...r.slotOwners(npc)]).toEqual([2]);
    expect(r.slotOwner(npc, 2)?.owner).toBe(2);
    expect(r.slotOwner(npc, 3)).toBeUndefined();
    expect(await r.locateSlot(npc, 1)).toBe(r.slotOwner(npc, 1));
    expect(r.slotTable(npc)?.get(3).holder).toBe(3);
    r.defineSlots(new SlotSpace(npc, { count: 8 }));
    expect(r.slotTable(npc)?.count).toBe(8);
    expect(r.slotOwner(npc, 1)).toBeUndefined();
    r.setSlot(npc, 5, SlotEntry.owned(1));
    r.removeSlots(npc);
    expect(r.slotTable(npc)).toBeUndefined();
    await r.close();
    const seen: string[] = [];
    for (;;) {
      const next = await events.next();
      if (next.done) break;
      seen.push(`${next.value.slot} ${next.value.previous.state}>${next.value.entry.state}`);
    }
    // Repeating an entry is no change; a redefinition or a removal frees
    // what the table held.
    expect(seen).toEqual(['1 0>1', '2 0>2', '3 0>0', '1 1>0', '2 2>0', '3 0>0', '5 0>1', '5 1>0']);
  });
});

test('EndpointResolver resolves everything to the endpoint', async () => {
  const r = new EndpointResolver('ws://ep/ws');
  const records = await r.resolve(Name.of('x'));
  expect(records).toHaveLength(1);
  expect(records[0]?.address.equals(new ServiceAddress(Name.of('x')))).toBe(true);
  expect(records[0]?.endpoints).toEqual(['ws://ep/ws']);
  await r.close();
  await r.close();
  await rejectsWith(r.resolve(Name.of('x')), StatusCode.failedPrecondition);
});
