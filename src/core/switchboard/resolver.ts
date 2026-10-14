/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { ServiceAddress } from '../address/index.ts';
import type { Name } from '../name.ts';
import { downEvent, serviceRecord, upEvent } from '../naming/naming-protocol.ts';
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

/** Whether [resolver] knows slot tables ({@link SlotResolver}). */
export function isSlotResolver(resolver: Resolver): resolver is SlotResolver {
  const r = resolver as Partial<SlotResolver>;
  return typeof r.slotTable === 'function' && typeof r.locateSlot === 'function';
}

function closedError(what: string): SwitchboardError {
  return SwitchboardError.of(StatusCode.failedPrecondition, `${what} closed`);
}

/**
 * A configured, in-memory table. For tests, embedded style peers and
 * deployments without a naming service. After {@link close}, resolution and
 * changes fail with FAILED_PRECONDITION; the last table stays readable.
 */
export class StaticResolver implements Resolver {
  readonly #table = new Map<string, ServiceRecord>();
  readonly #events = new Broadcast<ServiceEvent>();
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

  async close(): Promise<void> {
    this.#closed = true;
    this.#events.end();
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
