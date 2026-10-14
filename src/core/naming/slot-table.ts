/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import type { Name } from '../name.ts';
import { SlotEntry, type SlotSpace } from './naming-protocol.ts';

/**
 * A read-only view of the slot table of one type: its {@link space} and the
 * state of its slots.
 *
 * The naming service exposes the authoritative tables
 * (`NamingService.slotTables`), a naming client the mirrored ones
 * (`NamingClient.slotTables`). The view is live: it reflects later changes,
 * until the space is redefined, which replaces the view.
 */
export class SlotTable {
  /** The slot space definition. */
  readonly space: SlotSpace;

  readonly #entries: ReadonlyMap<number, SlotEntry>;

  /** A view over [entries], which the owner of the table keeps updating. Only for the naming service and client. */
  constructor(space: SlotSpace, entries: ReadonlyMap<number, SlotEntry>) {
    this.space = space;
    this.#entries = entries;
  }

  /**
   * The slots that have a recorded state, by slot number. A slot that is
   * missing is {@link SlotEntry.unassigned}. A mirror holds only the slots
   * that are not free with holder 0 (those are left out of snapshots); the
   * naming service also keeps free slots that only have an epoch.
   */
  get entries(): ReadonlyMap<number, SlotEntry> {
    return this.#entries;
  }

  /** The sharded type. */
  get type(): Name {
    return this.space.type;
  }

  /** The number of slots. */
  get count(): number {
    return this.space.count;
  }

  /** The state of [slot]. @throws {RangeError} unless `0 <= slot < count`. */
  get(slot: number): SlotEntry {
    if (!Number.isInteger(slot) || slot < 0 || slot >= this.count) {
      throw new RangeError(`slot ${slot} is outside 0 to ${this.count - 1}`);
    }
    return this.#entries.get(slot) ?? SlotEntry.unassigned;
  }

  /**
   * The distinct instances traffic is routed to: the owners of owned slots
   * and, while a slot migrates, its old owner (`from`). A new set, exact
   * when read.
   */
  get owners(): Set<number> {
    const owners = new Set<number>();
    for (const entry of this.#entries.values()) {
      if (entry.owner !== 0) {
        owners.add(entry.owner);
      }
    }
    return owners;
  }

  /** The slots routed to [instance] (owned by it, or migrating away from it), in ascending order. */
  slotsOf(instance: number): number[] {
    const slots: number[] = [];
    for (const [slot, entry] of this.#entries) {
      if (entry.owner === instance) {
        slots.push(slot);
      }
    }
    return slots.sort((a, b) => a - b);
  }

  toString(): string {
    return `SlotTable(${this.space}, ${this.#entries.size} entries)`;
  }
}

/** A change of one slot in a mirrored slot table. */
export class SlotEvent {
  /** The sharded type. */
  readonly type: Name;
  /** The slot number. */
  readonly slot: number;
  /** The new state ({@link SlotEntry.unassigned} when the slot left the table). */
  readonly entry: SlotEntry;
  /** The state before the change. */
  readonly previous: SlotEntry;

  constructor(type: Name, slot: number, entry: SlotEntry, previous: SlotEntry) {
    this.type = type;
    this.slot = slot;
    this.entry = entry;
    this.previous = previous;
  }

  toString(): string {
    return `SlotEvent(${this.type}/${this.slot}: ${this.previous} -> ${this.entry})`;
  }
}
