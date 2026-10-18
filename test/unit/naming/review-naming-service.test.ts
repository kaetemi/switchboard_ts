/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Review finding (naming layer), fixed: `NamingService.close()` publishes
// the `DOWN`s of the channels' records on `events` (as Dart does, and as the
// existing "close ends subscriptions" case pins), and the `SLOT` changes
// that follow from them on `slotEvents`: the slot manager's `emitSlotItem`
// hook checked `#closed`, which `close()` sets first, where `#publish`
// checks the closed signal, aborted only once the channels are closed; both
// check the signal now. Dart's `_SlotManager.setEntry` adds to `events`
// until close closes the controller at its end, so a Dart listener sees the
// owned slots go free. The control pins the `events` half.

import { afterEach, beforeEach, expect, test } from 'vitest';
import { Name } from '../../../src/core/name.ts';
import { SlotMode, SlotSpace } from '../../../src/core/naming/naming-protocol.ts';
import { Harness, register, until } from '../../support/naming/harness.ts';
import { Instance } from '../../support/naming/sharding-harness.ts';

const discord = Name.of('discord');

let h: Harness;
let log: string[];

beforeEach(() => {
  h = new Harness();
  log = [];
});

afterEach(async () => {
  await h.close();
});

async function owner(): Promise<Instance> {
  const a = new Instance(h, discord, 1, log);
  await a.start({
    space: new SlotSpace(discord, { count: 4, mode: SlotMode.static }),
    capacity: 0,
  });
  expect(await a.claim(1)).toBe(1);
  return a;
}

test('control: close publishes the DOWN of an owner on events', async () => {
  await owner();
  const downs: string[] = [];
  h.service.on('event', (event) => {
    if (!event.up) downs.push(`${event.record.address}`);
  });
  await h.service.close();
  expect(downs).toEqual(['discord/1']);
});

test('close publishes the slot its owner frees on slotEvents, as it does on events', async () => {
  await owner();
  const items: string[] = [];
  h.service.on('slotEvent', (item) => items.push(`${item.type}/${item.slot} ${item.entry}`));
  await h.service.close();
  // The owner's slot went free with its holder kept, as on any loss.
  expect(h.service.slotTable(discord)?.get(1).isFree).toBe(true);
  expect(items).toEqual(['discord/1 free holder 1 epoch 1']);
});

test('close publishes the slot its owner frees on the slotEvents iterable', async () => {
  await owner();
  const items: string[] = [];
  const iterating = (async () => {
    for await (const item of h.service.slotEvents) {
      items.push(`${item.type}/${item.slot} ${item.entry}`);
    }
  })();
  await h.service.close();
  // The iteration ends with the service, after what the close published.
  await iterating;
  expect(items).toEqual(['discord/1 free holder 1 epoch 1']);
});

// Design 12.16: the service's listeners run synchronously but once the step
// is done, so a DOWN's listener sees the slots of the record freed, as a
// Dart listener (called after the step) does.
test('a DOWN listener sees the slots of the record it is told about freed', async () => {
  const a = await owner();
  const seen: string[] = [];
  h.service.on('event', (event) => {
    if (!event.up) {
      seen.push(`${event.record.address} ${h.service.slotTable(discord)?.get(1).isFree}`);
    }
  });
  await a.channel.close();
  await until(() => seen.length > 0);
  expect(seen).toEqual(['discord/1 true']);
});

test('a DOWN listener sees every record of the channel that ended gone', async () => {
  const [channel] = h.link();
  await register(channel, 'npc', { instance: 1 });
  await register(channel, 'npc', { instance: 2 });
  const seen: number[] = [];
  h.service.on('event', (event) => {
    if (!event.up) seen.push(h.service.records('npc').length);
  });
  await channel.close();
  await until(() => seen.length === 2);
  expect(seen).toEqual([0, 0]);
});
