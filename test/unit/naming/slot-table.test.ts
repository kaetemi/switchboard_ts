// SlotTable, the live read-only view of one type's slots, and SlotEvent.
import { expect, test } from 'vitest';
import { Name } from '../../../src/core/name.ts';
import { SlotEntry, SlotEvent, SlotSpace, SlotTable } from '../../../src/core/naming/index.ts';

const kv = Name.of('kv');

test('a view over the entries, live, with unassigned slots filled in', () => {
  const entries = new Map<number, SlotEntry>();
  const table = new SlotTable(new SlotSpace(kv, { count: 8 }), entries);
  expect(table.type.equals(kv)).toBe(true);
  expect(table.count).toBe(8);
  expect(table.get(3).equals(SlotEntry.unassigned)).toBe(true);
  entries.set(3, SlotEntry.owned(2, { holder: 2, epoch: 1 }));
  expect(table.get(3).equals(SlotEntry.owned(2, { holder: 2, epoch: 1 }))).toBe(true);
  expect(table.entries.size).toBe(1);
  expect(table.toString()).toContain('1 entries');
});

test('slots outside the space are refused', () => {
  const table = new SlotTable(new SlotSpace(kv, { count: 8 }), new Map());
  expect(() => table.get(8)).toThrow(RangeError);
  expect(() => table.get(-1)).toThrow(RangeError);
  expect(() => table.get(1.5)).toThrow(RangeError);
});

test('owners and the slots of an instance follow traffic, the old owner while migrating', () => {
  const entries = new Map<number, SlotEntry>([
    [5, SlotEntry.owned(2, { epoch: 1 })],
    [1, SlotEntry.owned(2, { epoch: 1 })],
    [2, SlotEntry.migrating(3, 4, { epoch: 2 })],
    [3, SlotEntry.free({ holder: 7, epoch: 3 })],
  ]);
  const table = new SlotTable(new SlotSpace(kv, { count: 8 }), entries);
  expect([...table.owners].sort()).toEqual([2, 3]);
  expect(table.slotsOf(2)).toEqual([1, 5]);
  expect(table.slotsOf(3)).toEqual([2]);
  expect(table.slotsOf(4)).toEqual([]);
  expect(table.slotsOf(7)).toEqual([]);
});

test('entries: free and empty', () => {
  expect(SlotEntry.unassigned.isFree).toBe(true);
  expect(SlotEntry.unassigned.isEmpty).toBe(true);
  expect(SlotEntry.free({ holder: 2 }).isEmpty).toBe(false);
  expect(SlotEntry.owned(2).isFree).toBe(false);
  expect(SlotEntry.migrating(2, 3).to).toBe(3);
  expect(SlotEntry.migrating(2, 3, { epoch: 4 }).toString()).toBe(
    'migrating 2->3 holder 0 epoch 4',
  );
});

test('an event names the slot, the new and the previous state', () => {
  const event = new SlotEvent(kv, 3, SlotEntry.owned(2), SlotEntry.unassigned);
  expect(event.toString()).toBe(
    'SlotEvent(kv/3: free holder 0 epoch 0 -> owned 2 holder 0 epoch 0)',
  );
});
