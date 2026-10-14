/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { ServiceAddress } from '../address/index.ts';
import type { Name } from '../name.ts';
import {
  downEvent,
  SlotEntry,
  type SlotSpace,
  serviceRecord,
  upEvent,
} from '../naming/naming-protocol.ts';
import { SlotEvent, SlotTable } from '../naming/slot-table.ts';
import { StatusCode, SwitchboardError } from '../status.ts';
import { Broadcast } from './broadcast.ts';
import type {
  BrokeringResolver,
  Resolver,
  ServiceEvent,
  ServiceRecord,
  SlotResolver,
} from './contract.ts';

/** Whether [resolver] can broker connections ({@link BrokeringResolver}). */
export function isBrokeringResolver(resolver: Resolver): resolver is BrokeringResolver {
  return typeof (resolver as Partial<BrokeringResolver>).connectTo === 'function';
}

/**
 * Whether [resolver] knows slot tables ({@link SlotResolver}): what the
 * node's slot routing, the proxy and the `MOVED` retry route through.
 */
export function isSlotResolver(resolver: Resolver | undefined): resolver is SlotResolver {
  const candidate = resolver as Partial<SlotResolver> | undefined;
  return (
    typeof candidate?.slotTable === 'function' &&
    typeof candidate.slotOwner === 'function' &&
    typeof candidate.locateSlot === 'function'
  );
}

function closedError(what: string): SwitchboardError {
  return SwitchboardError.of(StatusCode.failedPrecondition, `${what} closed`);
}

/** A configured slot table of {@link StaticResolver}. */
class StaticSlots {
  readonly space: SlotSpace;
  readonly entries = new Map<number, SlotEntry>();
  readonly view: SlotTable;

  constructor(space: SlotSpace) {
    this.space = space;
    this.view = new SlotTable(space, this.entries);
  }
}

/**
 * A configured, in-memory table. For tests, embedded style peers and
 * deployments without a naming service.
 *
 * It can also carry configured slot tables ({@link defineSlots},
 * {@link setSlot}), for routing shard slots without a naming service;
 * {@link locateSlot} then answers from the configured table, as there is no
 * authority to ask. After {@link close}, resolution and changes fail with
 * FAILED_PRECONDITION; the last table stays readable.
 */
export class StaticResolver implements SlotResolver {
  readonly #table = new Map<string, ServiceRecord>();
  readonly #events = new Broadcast<ServiceEvent>();
  readonly #slots = new Map<string, StaticSlots>();
  readonly #slotEvents = new Broadcast<SlotEvent>();
  #closed = false;

  /** A resolver pre-populated with [records]. */
  constructor(records: Iterable<ServiceRecord> = []) {
    for (const record of records) {
      this.#table.set(record.address.key, record);
    }
  }

  /** The records, by address. */
  get table(): ReadonlyMap<string, ServiceRecord> {
    return new Map(this.#table);
  }

  #checkOpen(): void {
    if (this.#closed) {
      throw closedError('static resolver');
    }
  }

  /** Adds or replaces [record] and emits an UP event. */
  add(record: ServiceRecord): void {
    this.#checkOpen();
    this.#table.set(record.address.key, record);
    this.#events.push(upEvent(record));
  }

  /** Removes the record of [address] if present and emits a DOWN event. */
  remove(address: ServiceAddress): void {
    this.#checkOpen();
    if (this.#table.delete(address.key)) {
      this.#events.push(downEvent(address));
    }
  }

  async resolve(type: Name): Promise<ServiceRecord[]> {
    this.#checkOpen();
    const records: ServiceRecord[] = [];
    for (const record of this.#table.values()) {
      if (record.address.type.equals(type)) {
        records.push(record);
      }
    }
    return records;
  }

  get events(): AsyncIterable<ServiceEvent> {
    return this.#events;
  }

  get ready(): Promise<void> {
    return Promise.resolve();
  }

  /**
   * Defines (or redefines) the slot space of [space]'s type with every slot
   * free. The slots of a previous definition are reported gone on
   * {@link slotEvents}.
   */
  defineSlots(space: SlotSpace): void {
    this.#checkOpen();
    const previous = this.#slots.get(space.type.key);
    if (previous !== undefined) {
      this.#clear(previous);
    }
    this.#slots.set(space.type.key, new StaticSlots(space));
  }

  /** Removes the slot space of [type]; its channels are routed by the plain rule again. */
  removeSlots(type: Name): void {
    this.#checkOpen();
    const previous = this.#slots.get(type.key);
    if (previous !== undefined) {
      this.#slots.delete(type.key);
      this.#clear(previous);
    }
  }

  /**
   * Sets the state of [slot] of [type] and reports it on {@link slotEvents}.
   *
   * @throws {Error} without a slot space for [type] ({@link defineSlots}).
   * @throws {RangeError} for a slot outside it.
   */
  setSlot(type: Name, slot: number, entry: SlotEntry): void {
    this.#checkOpen();
    const slots = this.#slots.get(type.key);
    if (slots === undefined) {
      throw new Error(`no slot space for ${type}`);
    }
    if (!Number.isInteger(slot) || slot < 0 || slot >= slots.space.count) {
      throw new RangeError(`slot ${slot} is outside 0 to ${slots.space.count - 1}`);
    }
    this.#set(slots, slot, entry);
  }

  #clear(slots: StaticSlots): void {
    for (const slot of [...slots.entries.keys()]) {
      this.#set(slots, slot, SlotEntry.unassigned);
    }
  }

  #set(slots: StaticSlots, slot: number, entry: SlotEntry): void {
    const previous = slots.entries.get(slot) ?? SlotEntry.unassigned;
    if (entry.isEmpty) {
      slots.entries.delete(slot);
    } else {
      slots.entries.set(slot, entry);
    }
    if (!previous.equals(entry)) {
      this.#slotEvents.push(new SlotEvent(slots.space.type, slot, entry, previous));
    }
  }

  slotTable(type: Name): SlotTable | undefined {
    return this.#slots.get(type.key)?.view;
  }

  slotOwner(type: Name, slot: number): SlotEntry | undefined {
    const entry = this.#slots.get(type.key)?.entries.get(slot);
    return entry !== undefined && entry.owner !== 0 ? entry : undefined;
  }

  slotOwners(type: Name): Set<number> {
    return this.#slots.get(type.key)?.view.owners ?? new Set<number>();
  }

  get slotEvents(): AsyncIterable<SlotEvent> {
    return this.#slotEvents;
  }

  /** The configured entry ({@link slotOwner}): a static table has no authority to ask. */
  async locateSlot(type: Name, slot: number): Promise<SlotEntry | undefined> {
    this.#checkOpen();
    return this.slotOwner(type, slot);
  }

  async close(): Promise<void> {
    this.#closed = true;
    this.#events.end();
    this.#slotEvents.end();
  }
}

/**
 * Resolves every type to one fixed endpoint, instance 0 (any): the
 * frontend client's resolver, where the endpoint's proxy does the real
 * resolution. Also what an embedded device uses.
 */
export class EndpointResolver implements Resolver {
  /** The endpoint every address resolves to. */
  readonly endpoint: string;
  #closed = false;

  constructor(endpoint: string) {
    this.endpoint = endpoint;
  }

  async resolve(type: Name): Promise<ServiceRecord[]> {
    if (this.#closed) {
      throw closedError('endpoint resolver');
    }
    return [serviceRecord(new ServiceAddress(type), { endpoints: [this.endpoint] })];
  }

  get events(): AsyncIterable<ServiceEvent> {
    return emptyEvents;
  }

  get ready(): Promise<void> {
    return Promise.resolve();
  }

  async close(): Promise<void> {
    this.#closed = true;
  }
}

const emptyEvents: AsyncIterable<ServiceEvent> = {
  [Symbol.asyncIterator]: () => ({
    next: () => Promise.resolve({ value: undefined, done: true }),
  }),
};
