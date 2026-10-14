/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import type { ServiceAddress } from '../address/index.ts';
import { type Name, type NameLike, toName } from '../name.ts';
import { StatusCode, SwitchboardError } from '../status.ts';
import type {
  BrokeringResolver,
  ServiceEvent,
  ServiceRecord,
  SlotResolver,
} from '../switchboard/contract.ts';
import type { TimerHandle } from '../util/clock.ts';
import type { NamingClient } from './naming-client.ts';
import { SlotEntry } from './naming-protocol.ts';
import type { SlotEvent, SlotTable } from './slot-table.ts';

/** What a {@link NamingResolver} takes beside its client. */
export interface NamingResolverOptions {
  /** How long {@link NamingResolver.resolve} waits for the client to be synced, in ms; default 5000. */
  readonly resolveTimeout?: number;
}

/**
 * Resolves through the table a {@link NamingClient} mirrors from the naming
 * service. The normal backend resolver.
 *
 * Resolution waits until the client has been synced once, at most
 * `resolveTimeout`; see {@link resolve}. The resolver does not start the
 * client. It brokers connections to instances registered without endpoints
 * through the client's `CONNECT` ({@link connectTo}), and answers the slot
 * questions of a sharding router from the client's mirrored slot tables.
 */
export class NamingResolver implements SlotResolver, BrokeringResolver, AsyncDisposable {
  /** The client whose table this resolver reads. */
  readonly client: NamingClient;
  /** How long {@link resolve} waits for the client to be synced, in ms. */
  readonly resolveTimeout: number;
  #closed = false;

  /**
   * Creates a resolver over [client], which may be in any state: whether the
   * stale table is served depends only on `NamingClient.hasSynced`.
   */
  constructor(client: NamingClient, options: NamingResolverOptions = {}) {
    this.client = client;
    this.resolveTimeout = options.resolveTimeout ?? 5000;
  }

  /**
   * All instances of [type] in the client's table.
   *
   * Before the client's first sync (`NamingClient.hasSynced`) this waits for
   * `NamingClient.firstSynced`, at most `resolveTimeout`, and then rejects
   * with `UNAVAILABLE`. Once the client has been synced once, a later loss of
   * the naming service does not block resolution: the mirrored table is
   * served as it was (stale) until the client resynchronizes. Rejects with
   * `FAILED_PRECONDITION` after {@link close}, with `CANCELLED` if the client
   * is closed while waiting, and with the reason of `signal` when it aborts.
   */
  async resolve(type: NameLike, options: { signal?: AbortSignal } = {}): Promise<ServiceRecord[]> {
    this.#checkOpen();
    const name = toName(type);
    if (!this.client.hasSynced) {
      await this.#awaitFirstSync(options.signal);
      this.#checkOpen();
    }
    if (!this.client.isSynced) {
      this.client.log('debug', `serving stale table for ${name} while resynchronizing`);
    }
    return this.client.records(name);
  }

  #awaitFirstSync(signal: AbortSignal | undefined): Promise<void> {
    signal?.throwIfAborted();
    const clock = this.client.clock;
    const timeout = this.resolveTimeout;
    return new Promise<void>((resolve, reject) => {
      let timer: TimerHandle | undefined;
      const done = (): void => {
        if (timer !== undefined) clock.clearTimeout(timer);
        signal?.removeEventListener('abort', onAbort);
      };
      const onAbort = (): void => {
        done();
        reject(signal?.reason);
      };
      signal?.addEventListener('abort', onAbort, { once: true });
      timer = clock.setTimeout(() => {
        timer = undefined;
        done();
        reject(
          SwitchboardError.of(
            StatusCode.unavailable,
            `naming service not synced within ${timeout} ms`,
          ),
        );
      }, timeout);
      this.client.firstSynced.then(
        () => {
          done();
          resolve();
        },
        (error: unknown) => {
          done();
          reject(error);
        },
      );
    });
  }

  #checkOpen(): void {
    if (this.#closed || this.client.isClosed) {
      throw SwitchboardError.of(StatusCode.failedPrecondition, 'naming resolver closed');
    }
  }

  /** The client's `NamingClient.events`. */
  get events(): AsyncIterable<ServiceEvent> {
    return this.client.events;
  }

  /** The client's mirrored slot table of [type], or undefined without a slot space. */
  slotTable(type: NameLike): SlotTable | undefined {
    return this.client.slotTable(type);
  }

  /**
   * Where traffic for [slot] of [type] goes, from the client's mirror: the
   * entry if the slot is owned or migrating (route to `SlotEntry.owner`),
   * undefined otherwise.
   */
  slotOwner(type: NameLike, slot: number): SlotEntry | undefined {
    return this.client.slotOwner(type, slot);
  }

  /** The distinct instances the slots of [type] are routed to. */
  slotOwners(type: NameLike): Set<number> {
    return this.client.slotOwners(type);
  }

  /** The client's `NamingClient.slotEvents`. */
  get slotEvents(): AsyncIterable<SlotEvent> {
    return this.client.slotEvents;
  }

  /**
   * Asks the naming service for the owner of [slot] of [type]
   * (`NamingClient.locate`); in a managed space a free slot is assigned
   * first. Resolves with the mirrored entry if the mirror already shows an
   * owner at least as recent, else an entry built from the answer; undefined
   * when the slot stays free.
   */
  async locateSlot(type: NameLike, slot: number): Promise<SlotEntry | undefined> {
    const name: Name = toName(type);
    const located = await this.client.locate(name, slot);
    const mirrored = this.client.slotOwner(name, slot);
    if (mirrored !== undefined && mirrored.epoch >= located.epoch) {
      return mirrored;
    }
    if (located.owner === 0) {
      return undefined;
    }
    return new SlotEntry(located.state, { owner: located.owner, epoch: located.epoch });
  }

  /**
   * Has the instance at [address] dial [endpoint] through the naming service
   * (`NamingClient.connectTo`); resolves with the identity it presents there.
   * Rejects with `FAILED_PRECONDITION` after {@link close}.
   */
  async connectTo(
    address: ServiceAddress,
    endpoint: string,
    intent: Uint8Array,
    options: { timeout?: number | undefined; signal?: AbortSignal | undefined } = {},
  ): Promise<string> {
    this.#checkOpen();
    return this.client.connectTo(address.type, address.instance, endpoint, intent, options);
  }

  /**
   * The client's `NamingClient.firstSynced`: resolves once the table has been
   * synced at least once, and stays resolved through later losses of the
   * naming service (the stale table is served meanwhile). Rejects with
   * `CANCELLED` if the client is closed before its first sync.
   */
  get ready(): Promise<void> {
    return this.client.firstSynced;
  }

  /** Closes the resolver and its client: the client stops reconnecting and its registrations are dropped by the naming service. */
  async close(): Promise<void> {
    this.#closed = true;
    await this.client.close();
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }
}
