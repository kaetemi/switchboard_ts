/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { maxInstance, ServiceAddress } from '../address/index.ts';
import { type Name, type NameLike, toName } from '../name.ts';
import { ProtocolError, Status, StatusCode, SwitchboardError } from '../status.ts';
import type { ServiceEvent, ServiceRecord } from '../switchboard/contract.ts';
import type { TalkChannel, TalkMessage, TalkRequest } from '../talk/contract.ts';
import { type Clock, systemClock, type TimerHandle } from '../util/clock.ts';
import { Deferred } from '../util/deferred.ts';
import { Emitter, type Unsubscribe } from '../util/emitter.ts';
import { type Logger, type LogLevel, noopLogger } from '../util/logger.ts';
import {
  type ClientCall,
  ClientSlots,
  type Served,
  type SlotHandler,
  type SpaceDefinition,
} from './naming-client-slots.ts';
import {
  ConnectRequest,
  ConnectResponse,
  checkEndpoint,
  DialBackRequest,
  decodeDownEvent,
  decodeServiceRecord,
  downEvent,
  type LocateResponse,
  maxMetadataLength,
  maxU32,
  type PhaseItem,
  Procedures,
  RegisterRequest,
  RegisterResponse,
  type SlotEntry,
  SlotItem,
  type SlotMode,
  SlotSpace,
  SlotsRequest,
  serviceRecordsEqual,
  UnregisterRequest,
  upEvent,
  WatchRequest,
} from './naming-protocol.ts';
import type { SlotEvent, SlotTable } from './slot-table.ts';

export type { SlotHandler, SlotRequestContext } from './naming-client-slots.ts';
export { AssignResult } from './naming-client-slots.ts';

const logName = 'Switchboard.Naming';

/** Opens a fresh Talk channel to the naming service. */
export type TalkConnector = () => Promise<TalkChannel>;

/**
 * Serves a `CONNECT` the naming service relays to this instance over the
 * client's channel: dial `request.endpoint`, identify there with
 * `request.intent` naming `request.requester` as the receiver, and resolve
 * once that identification is confirmed. A failure, whatever it is, is
 * answered `ABORT UNAVAILABLE` (wiki page "Switchboard Identity and
 * Credentials", "Reverse connections"); its cause goes to the log.
 */
export type ConnectHandler = (request: DialBackRequest) => Promise<void>;

/** What a {@link NamingClient} takes beside its connector. */
export interface NamingClientOptions {
  /** Delay before each reconnect attempt, in ms; default 1000. */
  readonly reconnectDelay?: number;
  /**
   * Longest a slot handler call may run, whatever it declares, before the
   * request is answered `ABORT DEADLINE_EXCEEDED`, in ms; 0 for no bound.
   * Default 600 000 (10 minutes).
   */
  readonly slotHandlerMaxDuration?: number;
  /**
   * Whether the client mirrors the table (`WATCH` on every connect); default
   * true. False makes a client that only registers (a worker whose
   * credential has no `watch` scope): its table stays empty and it is never
   * synced.
   */
  readonly watch?: boolean;
  /** Time and timers; default the system clock. */
  readonly clock?: Clock;
  /** Diagnostics, under the name `Switchboard.Naming`; default none. */
  readonly logger?: Logger;
}

/** What {@link NamingClient.register} takes beside the type and endpoints. */
export interface RegisterOptions {
  /** The instance id to ask for; 0 (the default) has one assigned. */
  readonly instance?: number;
  /**
   * Called synchronously with the instance id whenever the registration gets
   * an id different from the one it had: once on the first success, just
   * before the returned promise resolves, and again after a reconnect that
   * had to assign a new id. Exceptions it throws are logged and ignored.
   */
  readonly onAssigned?: (instance: number) => void;
  /** Published with the record (copied; at most {@link maxMetadataLength} bytes); default empty. */
  readonly metadata?: Uint8Array;
}

/** What {@link NamingClient.defineSlots} takes beside the type. */
export interface DefineSlotsOptions {
  /** Number of slots, 1 to 0xFFFFFFFF. */
  readonly count: number;
  /** Default managed. */
  readonly mode?: SlotMode;
  /** Default false. */
  readonly lazy?: boolean;
  /** Default false. */
  readonly shared?: boolean;
  /** This instance's share weight; 0 takes no slots; default 1. */
  readonly capacity?: number;
}

/** The events of a {@link NamingClient}. */
export type NamingClientEvents = {
  /** A change of the mirrored table. */
  event: ServiceEvent;
  /** A change of the mirrored slot tables. */
  slotEvent: SlotEvent;
};

/**
 * A client of the naming service: registers this process's services and
 * mirrors the service table through a `WATCH` subscription.
 *
 * The client keeps one channel to the naming service. On every connect it
 * first re-registers the services registered through it (asking for the
 * ids they had), then subscribes to every type with `WATCH` and rebuilds
 * {@link table} from the snapshot: the events that arrive before the
 * `WATCH` reply are the snapshot, the reply marks the table synced, and the
 * events after it are live. The client keeps that single subscription to
 * every type for the life of the channel; it never sends `UNWATCH`.
 *
 * The client sends no heartbeat and expects none: the subscription lives as
 * long as the channel, and a naming service that stops answering is
 * detected by the connection under the channel (the mux keep-alive), which
 * then ends the channel. When the channel is lost (it ends, or the `WATCH`
 * request fails or times out) the client marks the table stale, keeps it,
 * and reconnects after `reconnectDelay`, until {@link close}.
 *
 * When a `REGISTER` or `UNREGSTR` has no clear outcome (no answer in time,
 * or an unreadable answer) the client drops the channel itself: the naming
 * service then discards everything registered on it, and the reconnect
 * registers the remembered set again. Neither a phantom record nor an
 * untracked one is left behind.
 *
 * Sharding (wiki page "Polyverse Switchboard Sharding"): the client mirrors
 * the slot tables too ({@link slotTables}, the `slotEvent` event), and a
 * sharded instance uses {@link defineSlots}, {@link declareHolding},
 * {@link claim}, {@link release}, {@link locate} and {@link migrate}, and
 * sets {@link slotHandler} to serve the `ASSIGN`, `DRAIN`, `FORWARD` and
 * `RESUME` requests the naming service sends over this client's channel.
 * The client serves the slots of a type for the one instance of that type
 * it registers. It remembers the slot space definitions, the declared
 * storage and the slots it serves; after a reconnect it registers again,
 * then re-sends `SLOTS` and `HOLDING` and claims every slot it was serving
 * with its last epoch; a request the channel refuses to send (its
 * `TalkOptions.maxOutgoingRequests`) is sent after `reconnectDelay`, with
 * what follows it. Calls of {@link claim}, {@link release} and
 * {@link locate} beyond that limit wait for their turn, in call order,
 * rather than fail with `RESOURCE_EXHAUSTED`. When the channel is lost, or
 * the client is closed, every slot locked by `DRAIN` is unlocked through
 * `SlotHandler.onResume`. Every `HOLDING` response's list of slots to
 * discard reaches `SlotHandler.onDiscard`.
 *
 * See the wiki page "Polyverse Switchboard Naming Service", section "Mirror
 * behaviour".
 */
export class NamingClient implements AsyncDisposable {
  /** Delay before each reconnect attempt, in ms. */
  readonly reconnectDelay: number;
  /** Longest a slot handler call may run, in ms; 0 for no bound. */
  readonly slotHandlerMaxDuration: number;
  /** Whether the client mirrors the table (`WATCH` on every connect). */
  readonly watch: boolean;
  /** The clock of the client's timers. */
  readonly clock: Clock;
  /** Where the client's diagnostics go. */
  readonly logger: Logger;

  /**
   * Serves the `CONNECT` requests the naming service relays to this
   * instance (a consumer asked it to dial back). Without one, they are
   * answered `ABORT UNIMPLEMENTED`.
   */
  connectHandler: ConnectHandler | undefined;

  readonly #connect: TalkConnector;
  readonly #emitter = new Emitter<NamingClientEvents>();
  readonly #slots: ClientSlots;
  readonly #table = new Map<string, ServiceRecord>();
  readonly #entries = new Set<Entry>();
  readonly #started = new Deferred<void>();
  #synced = new Deferred<void>();
  readonly #firstSynced = new Deferred<void>();
  readonly #closedSignal = new AbortController();
  #hasSynced = false;
  #isSynced = false;
  #session: ClientSession | undefined;
  #startPromise: Promise<void> | undefined;
  #closePromise: Promise<void> | undefined;
  #closed = false;
  #delayTimer: TimerHandle | undefined;
  #delay: Deferred<void> | undefined;
  #failedConnects = 0;

  /**
   * Creates a client. Nothing happens until {@link start}.
   *
   * [connect] opens a fresh Talk channel to the naming service; it is called
   * on start and again after a loss, with `reconnectDelay` between attempts.
   * The channel's `TalkOptions.requestTimeout` bounds every request, `WATCH`
   * included: its reply comes after the whole snapshot.
   *
   * @throws {RangeError} for a negative `reconnectDelay` or `slotHandlerMaxDuration`.
   */
  constructor(connect: TalkConnector, options: NamingClientOptions = {}) {
    this.#connect = connect;
    this.reconnectDelay = options.reconnectDelay ?? 1000;
    this.slotHandlerMaxDuration = options.slotHandlerMaxDuration ?? 600_000;
    this.watch = options.watch ?? true;
    this.clock = options.clock ?? systemClock;
    this.logger = options.logger ?? noopLogger;
    if (!(this.reconnectDelay >= 0)) {
      throw new RangeError(`reconnectDelay must not be negative: ${this.reconnectDelay}`);
    }
    if (!(this.slotHandlerMaxDuration >= 0)) {
      throw new RangeError(
        `slotHandlerMaxDuration must not be negative: ${this.slotHandlerMaxDuration}`,
      );
    }
    this.#emitter.onListenerError = (error) =>
      this.log('error', 'a naming client event listener threw', error);
    this.#slots = new ClientSlots({
      session: () => this.#session,
      closed: () => this.#closed,
      registeredOn: (session, type) => this.#registeredOn(session, type),
      registers: (type) => this.#registers(type),
      emitSlotEvent: (event) => this.#emitter.emit('slotEvent', event),
      client: this,
    });
  }

  /** @internal Logs under `Switchboard.Naming`. */
  log(level: LogLevel, message: string, error?: unknown): void {
    try {
      this.logger.log(level, logName, message, error);
    } catch {
      // A logger that throws must not break the client.
    }
  }

  // -------------------------------------------------------------------------
  // State

  /**
   * Read-only live view of the mirrored table, by `ServiceAddress.key`. It
   * is incomplete before the first `WATCH` reply ({@link hasSynced}) and
   * stale while not {@link isSynced}.
   */
  get table(): ReadonlyMap<string, ServiceRecord> {
    return this.#table;
  }

  /** The mirrored record of [address], if any. */
  record(address: ServiceAddress): ServiceRecord | undefined {
    return this.#table.get(address.key);
  }

  /** The mirrored records of [type]. */
  records(type: NameLike): ServiceRecord[] {
    const name = toName(type);
    return [...this.#table.values()].filter((r) => r.address.type.equals(name));
  }

  /**
   * Adds a listener: `event` for every change of {@link table} (`UP` when a
   * record appears or its endpoints or metadata change, `DOWN`, address
   * only, when it disappears, including records found missing when a new
   * snapshot arrives after a reconnect), `slotEvent` for every change of the
   * mirrored slot tables. Returns the function that removes it.
   */
  on<K extends 'event' | 'slotEvent'>(
    event: K,
    listener: (value: NamingClientEvents[K]) => void,
  ): Unsubscribe {
    return this.#emitter.on(event, listener);
  }

  /** As {@link on}, for one event. */
  once<K extends 'event' | 'slotEvent'>(
    event: K,
    listener: (value: NamingClientEvents[K]) => void,
  ): Unsubscribe {
    return this.#emitter.once(event, listener);
  }

  /** Removes a listener added with {@link on}. */
  off<K extends 'event' | 'slotEvent'>(
    event: K,
    listener: (value: NamingClientEvents[K]) => void,
  ): void {
    this.#emitter.off(event, listener);
  }

  /**
   * The changes of {@link table} as an async iterable, from the moment
   * iteration starts; it ends on {@link close}. Each `for await` gets its own
   * subscription.
   */
  get events(): AsyncIterable<ServiceEvent> {
    return this.#emitter.events('event', this.#closedSignal.signal);
  }

  /** The changes of the mirrored slot tables, as {@link events}. */
  get slotEvents(): AsyncIterable<SlotEvent> {
    return this.#emitter.events('slotEvent', this.#closedSignal.signal);
  }

  /** True while a channel to the naming service is open. */
  get isConnected(): boolean {
    return this.#session?.alive ?? false;
  }

  /** True once the `WATCH` of the current channel has been answered; false while disconnected. */
  get isSynced(): boolean {
    return this.#isSynced;
  }

  /**
   * True once any channel has been synced: {@link table} has been complete at
   * least once. A later loss of the naming service does not reset it; the
   * table is then stale ({@link isSynced} is false) but still served.
   */
  get hasSynced(): boolean {
    return this.#hasSynced;
  }

  /**
   * Resolves on the first sync ever, when {@link hasSynced} becomes true.
   * One-shot: unlike {@link synced} it is never replaced after a loss.
   * Rejects with `CANCELLED` if {@link close} is called before that.
   */
  get firstSynced(): Promise<void> {
    return this.#firstSynced.promise;
  }

  /**
   * Resolves once the current session is synced (its `WATCH` answered).
   * After a loss of the channel this returns a new promise for the next
   * session (unless the previous one never resolved, in which case it is the
   * same one). Rejects with `CANCELLED` if {@link close} is called first.
   */
  get synced(): Promise<void> {
    return this.#synced.promise;
  }

  /** True after {@link close}. */
  get isClosed(): boolean {
    return this.#closed;
  }

  // -------------------------------------------------------------------------
  // Sharding

  /** Serves the requests the naming service sends to this instance; without one they are answered `ABORT UNIMPLEMENTED`. */
  get slotHandler(): SlotHandler | undefined {
    return this.#slots.handler;
  }

  set slotHandler(handler: SlotHandler | undefined) {
    this.#slots.handler = handler;
  }

  /**
   * Read-only live view of the mirrored slot tables, by `Name.key` of the
   * type. Like {@link table}, incomplete before the first sync and stale
   * while not {@link isSynced}. A redefinition of a space replaces its
   * {@link SlotTable}.
   */
  get slotTables(): ReadonlyMap<string, SlotTable> {
    return this.#slots.views;
  }

  /** The mirrored slot table of [type], or undefined if it has no slot space. */
  slotTable(type: NameLike): SlotTable | undefined {
    return this.#slots.views.get(toName(type).key);
  }

  /**
   * The mirrored entry of [slot] of [type] if traffic for it has somewhere to
   * go: owned, or migrating (route to `SlotEntry.owner`, the old owner).
   * Undefined when the slot is free, out of range, or the type has no slot
   * space.
   */
  slotOwner(type: NameLike, slot: number): SlotEntry | undefined {
    return this.#slots.slotOwner(toName(type), slot);
  }

  /**
   * The distinct instances the slots of [type] are routed to (owners, and the
   * old owner of a migrating slot), from the mirror. A new set; empty without
   * a slot space.
   */
  slotOwners(type: NameLike): Set<number> {
    return this.slotTable(type)?.owners ?? new Set();
  }

  /**
   * The slots of [type] this instance serves (confirmed `ASSIGN`s, until
   * `FORWARD`, {@link release} or a revocation), with their epochs. A
   * snapshot.
   */
  servedSlots(type: NameLike): ReadonlyMap<number, number> {
    return this.#slots.servedSlots(toName(type));
  }

  /**
   * Defines or confirms the slot space of [type] (`SLOTS`) and offers
   * `capacity` as this instance's share weight. Capacity 0 takes no slots:
   * the call only defines or confirms the space, as a router or an operator
   * tool does; 1 or more makes this instance a candidate for the allocator
   * of a managed space.
   *
   * Remembered and sent again after every reconnect, once the registration
   * of [type] made through this client is back (right away if this client
   * registers no instance of [type]). Calling it again for the same type
   * replaces the remembered definition. While disconnected, or before the
   * registration of [type] completes, the promise resolves when the
   * definition is sent.
   *
   * Rejects with the naming service's {@link SwitchboardError}
   * (`FAILED_PRECONDITION` for a definition that conflicts with the existing
   * space, `OUT_OF_RANGE` for too many slots), and the definition is then
   * forgotten; with `INVALID_ARGUMENT` for an empty type,
   * `FAILED_PRECONDITION` after {@link close}, `CANCELLED` on
   * {@link close}; {@link RangeError} for a count or capacity outside `u32`,
   * or a count of 0.
   */
  defineSlots(type: NameLike, options: DefineSlotsOptions): Promise<void> {
    try {
      const name = toName(type);
      this.#checkSlotType(name);
      checkInterval(options.count, 1, maxU32, 'count');
      const capacity = options.capacity ?? 1;
      checkInterval(capacity, 0, maxU32, 'capacity');
      const space = new SlotSpace(name, {
        count: options.count,
        ...(options.mode !== undefined && { mode: options.mode }),
        lazy: options.lazy ?? false,
        shared: options.shared ?? false,
      });
      return this.#slots.defineSlots(new SlotsRequest(space, { capacity }));
    } catch (error) {
      return Promise.reject(error);
    }
  }

  /**
   * Declares that this instance holds the storage of [slots] of [type]
   * (`HOLDING`) and resolves with the slots among them whose holder is no
   * longer this instance; their local storage should be discarded. Free
   * slots it holds are given back to it first (managed mode).
   *
   * The declaration is remembered (minus the slots to discard) and sent again
   * after every reconnect, once the registration of [type] made through this
   * client is back. While disconnected, or before that registration is back,
   * the promise resolves when the declaration is sent and answered. A client
   * that registers no instance of [type] sends it right away, and the naming
   * service refuses it, since only a registered instance holds storage.
   * Rejects with the naming service's {@link SwitchboardError} (`NOT_FOUND`
   * without a slot space, `FAILED_PRECONDITION` without a registration of
   * [type] on the channel); those slots are then forgotten. Rejects with
   * `CANCELLED` on {@link close}.
   */
  declareHolding(type: NameLike, slots: readonly number[]): Promise<number[]> {
    try {
      const name = toName(type);
      this.#checkSlotType(name);
      for (const slot of slots) {
        checkInterval(slot, 0, maxU32, 'slot');
      }
      return this.#slots.declareHolding(name, [...slots]);
    } catch (error) {
      return Promise.reject(error);
    }
  }

  /**
   * Takes the free [slot] of [type] (`CLAIM`) and resolves with its epoch.
   * The naming service confirms it through `ASSIGN` to {@link slotHandler}
   * before answering. `holding` says this instance holds the slot's storage.
   * Claiming a slot this instance owns resolves with the current epoch.
   *
   * Rejects with the naming service's {@link SwitchboardError}:
   * `ALREADY_EXISTS` if another instance owns it, `FAILED_PRECONDITION`
   * while it migrates, `NOT_FOUND` without such a space or slot,
   * `UNAVAILABLE` if the `ASSIGN` failed. Rejects with `UNAVAILABLE` while
   * disconnected, or if the channel is lost while the call waits for the
   * channel's request limit.
   */
  claim(type: NameLike, slot: number, options: { holding?: boolean } = {}): Promise<number> {
    return this.#slotCall(type, slot, (name) =>
      this.#slots.claim(name, slot, options.holding ?? false),
    );
  }

  /**
   * Gives up [slot] of [type] (`RELEASE`). With `keepStorage` this instance
   * stays its holder (and keeps declaring it after reconnects).
   *
   * Rejects with `PERMISSION_DENIED` if this instance does not own it,
   * `FAILED_PRECONDITION` while it migrates, and `UNAVAILABLE` while
   * disconnected (or if the channel is lost while the call waits for the
   * channel's request limit).
   */
  release(type: NameLike, slot: number, options: { keepStorage?: boolean } = {}): Promise<void> {
    return this.#slotCall(type, slot, (name) =>
      this.#slots.release(name, slot, options.keepStorage ?? false),
    );
  }

  /**
   * Finds the owner of [slot] of [type] (`LOCATE`). In a managed space a free
   * slot is assigned first, and the answer comes once its new owner
   * confirmed. A free slot answers `SlotState.free` with owner 0.
   *
   * Rejects with `NOT_FOUND` without such a space or slot, `UNAVAILABLE` if
   * the assignment failed, while disconnected, or if the channel is lost
   * while the call waits for the channel's request limit.
   */
  locate(type: NameLike, slot: number): Promise<LocateResponse> {
    return this.#slotCall(type, slot, (name) => this.#slots.locate(name, slot));
  }

  /**
   * Moves [slot] of [type] to instance `to` (0, the default, lets the
   * allocator choose) through the hand-over (`MIGRATE`). The iterable yields
   * each {@link PhaseItem} and ends when the migration is done; at most one
   * migration per space runs at a time, others queue. The request is sent at
   * once; stopping the iteration (or `signal`) cancels it, and a migration
   * already running completes anyway.
   *
   * Iteration fails with `NOT_FOUND`, `FAILED_PRECONDITION` (already
   * migrating, `to` is the owner or not registered), `UNAVAILABLE` (rolled
   * back, after a `rolledBack` item; or disconnected).
   */
  migrate(
    type: NameLike,
    slot: number,
    options: { to?: number; signal?: AbortSignal } = {},
  ): AsyncIterable<PhaseItem> {
    try {
      const name = toName(type);
      this.#checkSlotType(name);
      checkInterval(slot, 0, maxU32, 'slot');
      const to = options.to ?? 0;
      checkInterval(to, 0, maxInstance, 'to');
      return this.#slots.migrate(name, slot, to, options.signal);
    } catch (error) {
      return failing(error);
    }
  }

  #slotCall<T>(type: NameLike, slot: number, call: (type: Name) => Promise<T>): Promise<T> {
    try {
      const name = toName(type);
      this.#checkSlotType(name);
      checkInterval(slot, 0, maxU32, 'slot');
      return call(name);
    } catch (error) {
      return Promise.reject(error);
    }
  }

  #checkSlotType(type: Name): void {
    if (this.#closed) {
      throw SwitchboardError.of(StatusCode.failedPrecondition, 'naming client closed');
    }
    if (type.isEmpty) {
      throw SwitchboardError.of(StatusCode.invalidArgument, 'empty service type');
    }
  }

  /** Whether this client registers an instance of [type]. */
  #registers(type: Name): boolean {
    for (const e of this.#entries) {
      if (e.type.equals(type)) return true;
    }
    return false;
  }

  /** Whether an instance of [type] is registered on [session]. */
  #registeredOn(session: ClientSession, type: Name): boolean {
    for (const e of this.#entries) {
      if (e.type.equals(type) && e.session === session) return true;
    }
    return false;
  }

  // -------------------------------------------------------------------------
  // Brokering

  /**
   * Asks the naming service to have instance [instance] of [type], which
   * registered without endpoints, dial [endpoint] and identify there with
   * [intent] (`CONNECT`; at most 64 bytes). Resolves once the instance has
   * done so, with the identity it presents: the caller matches the accepted
   * connection by that identity and [intent]. `timeout`, when given, bounds
   * the request whatever the naming service declares (it declares its own
   * bound with `EXTEND`); the request is then cancelled, and the naming
   * service cancels its request to the instance. `signal` cancels it too.
   *
   * Rejects with the naming service's {@link SwitchboardError} (`NOT_FOUND`
   * for no such record, or one with endpoints; `PERMISSION_DENIED` without
   * the `broker` right; `UNAVAILABLE` when the instance did not dial back in
   * time), with `DEADLINE_EXCEEDED` after `timeout`, with `UNAVAILABLE`
   * while disconnected, `FAILED_PRECONDITION` after {@link close}, and with
   * {@link TypeError} or {@link RangeError} for an endpoint
   * {@link checkEndpoint} refuses, an intent over 64 bytes, or an instance
   * outside `u48`.
   */
  async connectTo(
    type: NameLike,
    instance: number,
    endpoint: string,
    intent: Uint8Array,
    options: { timeout?: number | undefined; signal?: AbortSignal | undefined } = {},
  ): Promise<string> {
    const payload = new ConnectRequest(type, instance, endpoint, { intent }).encode();
    const request = this.#startRequest(Procedures.connect, payload, options.signal);
    const timeout = options.timeout;
    let timer: TimerHandle | undefined;
    let response = request.response;
    if (timeout !== undefined && timeout > 0) {
      response = Promise.race([
        response,
        new Promise<never>((_, reject) => {
          timer = this.clock.setTimeout(() => {
            const status = Status.of(
              StatusCode.deadlineExceeded,
              `CONNECT not answered within ${timeout} ms`,
            );
            request.cancel(status);
            reject(new SwitchboardError(status));
          }, timeout);
        }),
      ]);
    }
    try {
      return ConnectResponse.decode((await response).payload).identity;
    } finally {
      if (timer !== undefined) this.clock.clearTimeout(timer);
    }
  }

  /** Starts [procedure] on the live channel. @throws {SwitchboardError} when closed or disconnected, and like `TalkChannel.startRequest`. */
  #startRequest(procedure: Name, payload: Uint8Array, signal?: AbortSignal): TalkRequest {
    if (this.#closed) {
      throw SwitchboardError.of(StatusCode.failedPrecondition, 'naming client closed');
    }
    const session = this.#session;
    if (session === undefined || !session.usable) {
      throw SwitchboardError.of(StatusCode.unavailable, 'not connected to the naming service');
    }
    return session.channel.startRequest(procedure.toString(), payload, {
      name: procedure,
      ...(signal !== undefined && { signal }),
    });
  }

  /** Serves a `CONNECT` relayed by the naming service with {@link connectHandler}. */
  async #serveConnect(message: TalkMessage): Promise<void> {
    const handler = this.connectHandler;
    if (handler === undefined) {
      this.#abortRequest(
        message,
        Status.of(StatusCode.unimplemented, 'this instance does not dial back'),
      );
      return;
    }
    let request: DialBackRequest;
    try {
      request = DialBackRequest.decode(message.payload);
    } catch (error) {
      if (!(error instanceof ProtocolError)) throw error;
      this.#abortRequest(message, Status.of(StatusCode.invalidArgument, error.status.reason));
      return;
    }
    // The handler's dial and identification are bounded by the node; the
    // naming service bounds the request and cancels it when it gives up.
    message.setReplyTimeout(0);
    try {
      await handler(request);
    } catch (error) {
      if (error instanceof SwitchboardError) {
        this.log('info', `CONNECT to ${request.endpoint} failed: ${error.status}`);
      } else {
        this.log('warn', 'CONNECT handler failed', error);
      }
      this.#abortRequest(message, Status.of(StatusCode.unavailable, 'dial back failed'));
      return;
    }
    if (message.canReply) {
      settle(
        () => message.reply(new Uint8Array(0)),
        (e) => this.log('debug', `CONNECT reply failed: ${e}`),
      );
    }
  }

  #abortRequest(message: TalkMessage, status: Status): void {
    if (!message.canReply) {
      return;
    }
    settle(
      () => message.replyAbort(status),
      (e) => this.log('debug', `abort failed: ${e}`),
    );
  }

  // -------------------------------------------------------------------------
  // Lifecycle

  /**
   * Starts the connection loop: connects, re-registers remembered
   * registrations, starts `WATCH`. Resolves once the first connection is
   * established (not synced; see {@link synced}), or when {@link close} is
   * called before that. Calling it again returns the same promise.
   */
  start(): Promise<void> {
    this.#startPromise ??= this.#start();
    return this.#startPromise;
  }

  #start(): Promise<void> {
    if (!this.#closed) {
      void this.#run();
    }
    return this.#started.promise;
  }

  /**
   * Registers an instance of [type] reachable at [endpoints] and resolves
   * with the instance id the naming service assigned (`instance` 0) or
   * accepted. `metadata` (copied; at most {@link maxMetadataLength} bytes)
   * is published with the record for consumers to select on. Every watcher
   * of the naming table sees it, so a key placed there authenticates the
   * mesh to the instance, not the instance to the mesh.
   *
   * The endpoints need not be this process's: a registrar (a scaler, an
   * operator tool) may register any number of records for endpoints that
   * never talk to the naming service. Each record lives as long as this
   * client's channel, so the registrar retires dead endpoints itself with
   * {@link unregister}.
   *
   * The registration is remembered and made again after every reconnect,
   * asking for the same id; if that id has been taken meanwhile, a new one is
   * assigned (and logged). If the naming service refuses such a
   * re-registration for another reason, it is retried on the same channel
   * every `reconnectDelay`. While disconnected, and if the channel is lost
   * while the request is in flight, the returned promise resolves once the
   * registration succeeds after the next connect.
   *
   * Registering a non-zero `instance` of [type] again supersedes the earlier
   * registration of `type/instance` made through this client, whether or not
   * it has completed: the naming service replaces the endpoints and metadata
   * (publishing `UP` if either changed), only the newest registration is
   * remembered, and the earlier call's promise settles like this one. If the
   * replacement is refused while the earlier registration may be published
   * on the channel (it was made, or was in flight, on it), the client drops
   * the channel, so that the record it no longer tracks goes away with it.
   *
   * Rejects with the naming service's {@link SwitchboardError}
   * (`ALREADY_EXISTS` for a requested id in use, `INVALID_ARGUMENT`), with
   * `INVALID_ARGUMENT` for an empty type, `CANCELLED` if {@link unregister}
   * or {@link close} is called first, and `FAILED_PRECONDITION` after
   * {@link close}. Rejects with `DEADLINE_EXCEEDED` if the naming service
   * does not answer within the channel's request timeout: the outcome is
   * then unknown, so the client also drops the channel (the naming service
   * discards the record, if it made one) and reconnects; the registration is
   * not remembered. Rejects with {@link RangeError} for more than 255
   * endpoints, metadata longer than {@link maxMetadataLength}, or an instance
   * outside the `u48` range, and with {@link TypeError} for an endpoint
   * {@link checkEndpoint} refuses.
   */
  register(
    type: NameLike,
    endpoints: readonly string[],
    options: RegisterOptions = {},
  ): Promise<number> {
    if (this.#closed) {
      return Promise.reject(
        SwitchboardError.of(StatusCode.failedPrecondition, 'naming client closed'),
      );
    }
    let name: Name;
    const instance = options.instance ?? 0;
    const metadata = options.metadata;
    try {
      name = toName(type);
      if (name.isEmpty) {
        return Promise.reject(
          SwitchboardError.of(StatusCode.invalidArgument, 'empty service type'),
        );
      }
      if (!Number.isInteger(instance) || instance < 0 || instance > maxInstance) {
        throw new RangeError(`instance ${instance} outside 0..${maxInstance}`);
      }
      if (endpoints.length > 255) {
        throw new RangeError(`${endpoints.length} endpoints, over 255`);
      }
      for (const endpoint of endpoints) {
        checkEndpoint(endpoint);
      }
      if (metadata !== undefined && metadata.length > maxMetadataLength) {
        throw new RangeError(
          `metadata of ${metadata.length} bytes, more than ${maxMetadataLength}`,
        );
      }
    } catch (error) {
      return Promise.reject(error);
    }
    const entry = new Entry(
      name,
      Object.freeze([...endpoints]),
      metadata === undefined ? new Uint8Array(0) : metadata.slice(),
      instance,
      options.onAssigned,
    );
    if (instance !== 0) {
      // Registering an id again replaces its record: the older entry is
      // superseded, even while its own request is still in flight.
      for (const older of [...this.#entries]) {
        if (older.type.equals(name) && older.instance === instance) {
          this.#supersede(older, entry);
        }
      }
    }
    this.#entries.add(entry);
    const session = this.#session;
    if (session?.alive) {
      void this.#registerOn(session, entry);
    }
    return entry.completer.promise;
  }

  /**
   * Removes the registration of `type/instance` made through this client and
   * stops re-registering it. A {@link register} of it still in flight rejects
   * with `CANCELLED`; if the naming service registers it anyway, the client
   * unregisters it as soon as the answer arrives, and if one that replaced a
   * registration on the channel is refused, the client drops the channel,
   * which takes the replaced record with it.
   *
   * While disconnected it resolves immediately: the naming service already
   * dropped the record with the channel. Rejects with `NOT_FOUND` if this
   * client has no such registration, with the naming service's status if it
   * refuses, and with `FAILED_PRECONDITION` after {@link close}. If the
   * naming service refuses (other than with `NOT_FOUND`) or does not answer
   * in time (`DEADLINE_EXCEEDED`), the client also drops its channel, so
   * that the record goes away with it, and reconnects; either way the
   * registration is no longer remembered.
   */
  async unregister(type: NameLike, instance: number): Promise<void> {
    if (this.#closed) {
      throw SwitchboardError.of(StatusCode.failedPrecondition, 'naming client closed');
    }
    const name = toName(type);
    const matching = [...this.#entries].filter(
      (e) => e.type.equals(name) && e.instance === instance && instance !== 0,
    );
    if (matching.length === 0) {
      throw SwitchboardError.of(
        StatusCode.notFound,
        `no registration ${new ServiceAddress(name, instance)}`,
      );
    }
    const session = this.#session;
    let registeredHere = false;
    for (const entry of matching) {
      this.#entries.delete(entry);
      entry.completer.reject(SwitchboardError.of(StatusCode.cancelled, 'unregistered'));
      if (session !== undefined && entry.session === session) {
        registeredHere = true;
      }
    }
    if (session === undefined || !session.usable || !registeredHere) {
      // Not registered on the live channel. A registration still in flight
      // removes itself when it completes.
      return;
    }
    await this.#sendUnregister(session, name, instance);
  }

  /**
   * Stops reconnecting, closes the channel (the naming service then drops
   * this client's registrations and frees their slots), rejects pending
   * {@link register} promises and {@link synced} with `CANCELLED`, and ends
   * {@link events}. Slots locked by `DRAIN` are unlocked through
   * `SlotHandler.onResume` once their `DRAIN` handler is done, as on a loss
   * of the naming service. No timer is left running. Calling it again
   * returns the same promise.
   */
  close(): Promise<void> {
    this.#closePromise ??= this.#close();
    return this.#closePromise;
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }

  async #close(): Promise<void> {
    this.#closed = true;
    if (this.#delayTimer !== undefined) {
      this.clock.clearTimeout(this.#delayTimer);
      this.#delayTimer = undefined;
    }
    this.#delay?.resolve();
    this.#started.resolve();
    const cancelled = SwitchboardError.of(StatusCode.cancelled, 'naming client closed');
    for (const entry of this.#entries) {
      entry.completer.reject(cancelled);
    }
    this.#entries.clear();
    const session = this.#session;
    if (session !== undefined) {
      session.lose(Status.ok, 'naming client closed');
      await session.closed;
    }
    this.#isSynced = false;
    this.#synced.reject(cancelled);
    this.#firstSynced.reject(cancelled);
    this.#closedSignal.abort();
    this.#slots.close();
  }

  // -------------------------------------------------------------------------
  // Connection loop

  async #run(): Promise<void> {
    while (!this.#closed) {
      let channel: TalkChannel;
      try {
        channel = await this.#connect();
      } catch (error) {
        if (this.#closed) {
          break;
        }
        this.#failedConnects++;
        if (this.#failedConnects === 1) {
          this.log('warn', 'cannot connect to the naming service', error);
        } else {
          this.log('debug', `cannot connect to the naming service: ${error}`);
        }
        await this.#wait();
        continue;
      }
      if (this.#closed) {
        void channel.close().catch(() => {});
        break;
      }
      this.#failedConnects = 0;
      await this.#runSession(channel);
      if (!this.#closed) {
        await this.#wait();
      }
    }
  }

  /** Waits `reconnectDelay`; {@link close} cuts it short. */
  #wait(): Promise<void> {
    const delay = new Deferred<void>();
    this.#delay = delay;
    this.#delayTimer = this.clock.setTimeout(() => {
      this.#delayTimer = undefined;
      delay.resolve();
    }, this.reconnectDelay);
    return delay.promise;
  }

  async #runSession(channel: TalkChannel): Promise<void> {
    const session = new ClientSession(channel, this, () => this.#onLost());
    this.#session = session;
    this.log('info', 'connected to the naming service');
    this.#started.resolve();
    void this.#listen(session);
    channel.done.then(
      (status) => session.lose(Status.ok, `channel closed: ${status}`),
      () => session.lose(Status.ok, 'channel closed'),
    );

    // Register the ids we had (or asked for) first, so other services'
    // channels to them stay valid where possible and the snapshot already
    // holds them, then watch. Registrations that ask for any id are sent
    // after them (so that an assignment cannot take one of our ids), but not
    // waited for: a freshly started naming service may hold assignments for
    // a while (see NamingService.assignmentHold).
    const entries = [...this.#entries];
    const known: Promise<void>[] = [];
    for (const entry of entries) {
      if (entry.instance !== 0) known.push(this.#registerOn(session, entry));
    }
    for (const entry of entries) {
      if (entry.instance === 0) void this.#registerOn(session, entry);
    }
    await Promise.all(known);
    if (session.alive) {
      // SLOTS, HOLDING and the claims of served slots, for every type not
      // restored yet when its registration came back.
      this.#slots.restoreAll(session);
      if (this.watch) {
        this.#startWatch(session);
      }
    }
    await session.lost;
    if (this.#session === session) {
      this.#session = undefined;
    }
    await session.tearDown();
  }

  async #listen(session: ClientSession): Promise<void> {
    try {
      for await (const message of session.channel.messages) {
        this.#onServerMessage(session, message);
      }
      session.lose(Status.ok, 'channel closed');
    } catch (error) {
      session.lose(Status.ok, `channel aborted: ${error}`);
    }
  }

  /** Called synchronously when a session is lost: the table is stale from now on, and {@link synced} is a new promise for the next session. */
  #onLost(): void {
    this.#isSynced = false;
    if (this.#synced.isSettled && !this.#closed) {
      this.#synced = new Deferred();
    }
    this.#slots.onLost();
  }

  #onServerMessage(session: ClientSession, message: TalkMessage): void {
    if (!message.expectsReply) {
      this.#onEvent(session, message);
      return;
    }
    if (message.canReply && ClientSlots.isSlotProcedure(message.procedure)) {
      if (session.alive) {
        this.#slots.serve(session, message);
      }
      return;
    }
    if (message.canReply && message.procedure.equals(Procedures.connect)) {
      if (session.alive) {
        void this.#serveConnect(message);
      }
      return;
    }
    // The naming service sends no other requests.
    if (message.canReply) {
      this.#abortRequest(
        message,
        Status.of(StatusCode.unimplemented, `unknown procedure ${message.procedureName}`),
      );
    } else {
      this.log('debug', `ignoring request ${message.procedureName}`);
    }
  }

  // -------------------------------------------------------------------------
  // Registration

  /** Registers [entry] on [session], asking for [instance] (default: the entry's id). Never rejects. */
  async #registerOn(session: ClientSession, entry: Entry, instance?: number): Promise<void> {
    if (entry.session === session || entry.pending === session) {
      return;
    }
    entry.pending = session;
    const requested = instance ?? entry.instance;
    entry.pendingInstance = requested;
    let assigned: number;
    try {
      // Ordered: the UP of the record, which the naming service sends before
      // the reply, is in the table when this completes.
      const response = await session.channel.startRequest(
        Procedures.register.toString(),
        new RegisterRequest(entry.type, {
          requestedInstance: requested,
          endpoints: entry.endpoints,
          metadata: entry.metadata,
        }).encode(),
        { name: Procedures.register, ordered: true },
      ).response;
      assigned = RegisterResponse.decode(response.payload).instance;
      if (assigned === 0 || (requested !== 0 && assigned !== requested)) {
        throw new ProtocolError(
          `REGISTER for ${new ServiceAddress(entry.type, requested)} answered with id ` +
            assigned.toString(16),
        );
      }
    } catch (error) {
      if (entry.pending === session) {
        entry.pending = undefined;
      }
      await this.#onRegisterFailed(session, entry, requested, error);
      return;
    }
    if (entry.pending === session) {
      entry.pending = undefined;
    }
    await this.#onRegistered(session, entry, assigned);
  }

  async #onRegistered(session: ClientSession, entry: Entry, assigned: number): Promise<void> {
    if (entry.superseded || !this.#entries.has(entry)) {
      // Superseded by a newer registration, or unregistered, while the
      // request was in flight. The record is the newer registration's if that
      // one registers the same id on this channel: its REGISTER follows this
      // one, and replaces the record. Otherwise nothing tracks the record any
      // more (this one may have been given another id than the newer one
      // asked for), and it is removed.
      const successor = this.#successorOn(session, entry, assigned);
      if (successor !== undefined) {
        if (successor.session !== session) {
          // Should its REGISTER be refused, the record would stay.
          successor.replacesOn = session;
        }
        return;
      }
      if (session.usable) {
        try {
          await this.#sendUnregister(session, entry.type, assigned);
        } catch (error) {
          this.log('debug', `late unregister failed: ${error}`);
        }
      }
      return;
    }
    if (entry.completer.isSettled && entry.instance !== 0 && assigned !== entry.instance) {
      this.log(
        'warn',
        `re-registered ${new ServiceAddress(entry.type, entry.instance)} as ` +
          `${new ServiceAddress(entry.type, assigned)}`,
      );
    }
    const changed = !entry.completer.isSettled || assigned !== entry.instance;
    entry.instance = assigned;
    entry.session = session;
    entry.refusals = 0;
    // At most one remembered registration per address: an older one that
    // ended up with the same id on this channel (it asked for any id and was
    // given the one this entry asked for) is superseded by this, the later
    // answer. A remembered one that is not registered on this channel (it
    // waits to be registered again) has lost its id to this entry, and asks
    // for a new one when it is.
    for (const other of [...this.#entries]) {
      if (
        other !== entry &&
        other.type.equals(entry.type) &&
        other.instance === assigned &&
        other.completer.isSettled &&
        other.pending !== session
      ) {
        if (other.session === session) {
          this.#supersede(other, entry);
        } else {
          this.log(
            'warn',
            `${new ServiceAddress(entry.type, assigned)} went to another registration of ` +
              'this client; asking for a new id',
          );
          other.instance = 0;
        }
      }
    }
    // Whatever it replaced, it replaced.
    entry.replacesOn = undefined;
    this.log('debug', `registered ${new ServiceAddress(entry.type, assigned)}`);
    if (changed) {
      this.#notifyAssigned(entry, assigned);
    }
    entry.completer.resolve(assigned);
    this.#slots.restore(session, entry.type);
  }

  async #onRegisterFailed(
    session: ClientSession,
    entry: Entry,
    requested: number,
    error: unknown,
  ): Promise<void> {
    if (entry.superseded) {
      // Its successor carries the outcome.
      return;
    }
    if (!session.usable) {
      // The channel is gone, and with it anything the request registered. A
      // remembered or pending entry is registered again after the next
      // connect.
      return;
    }
    const address = new ServiceAddress(entry.type, requested);
    const indeterminate = isIndeterminate(error);
    if (!this.#entries.has(entry)) {
      // Unregistered while the request was in flight. If the naming service
      // may have registered it after all, or still publishes the record it
      // was to replace, the record must not outlive the channel unnoticed.
      if (indeterminate) {
        session.lose(
          Status.of(StatusCode.aborted, 'REGISTER outcome unknown'),
          `REGISTER ${address} has no clear outcome (${error})`,
        );
      } else if (entry.replacesOn === session) {
        session.lose(
          Status.of(StatusCode.aborted, 'replacement REGISTER failed'),
          `replacement REGISTER ${address} failed after unregister; dropping the old record`,
        );
      }
      return;
    }
    const remembered = entry.completer.isSettled;
    if (indeterminate) {
      // The naming service may or may not hold the record. Drop the channel:
      // it then discards everything registered on it, and the reconnect
      // registers the remembered set again.
      this.log('warn', `REGISTER ${address} has no clear outcome, dropping the channel`, error);
      if (!remembered) {
        this.#entries.delete(entry);
        entry.completer.reject(error);
      }
      session.lose(
        Status.of(StatusCode.aborted, 'REGISTER outcome unknown'),
        `REGISTER ${address} has no clear outcome`,
      );
      return;
    }
    if (
      remembered &&
      requested !== 0 &&
      error instanceof SwitchboardError &&
      error.code === StatusCode.alreadyExists
    ) {
      this.log('warn', `${address} is taken, asking for a new id`);
      await this.#registerOn(session, entry, 0);
      return;
    }
    if (remembered) {
      // Refused on a healthy channel: try again later on the same channel,
      // without disturbing the other registrations.
      entry.refusals++;
      const message = `re-registering ${address} failed; retrying in ${this.reconnectDelay} ms`;
      if (entry.refusals === 1) {
        this.log('error', message, error);
      } else {
        this.log('debug', `${message}: ${error}`);
      }
      session.later(this.reconnectDelay, () => {
        if (this.#entries.has(entry)) {
          void this.#registerOn(session, entry);
        }
      });
      return;
    }
    this.#entries.delete(entry);
    entry.completer.reject(error);
    if (entry.replacesOn === session) {
      session.lose(
        Status.of(StatusCode.aborted, 'replacement REGISTER failed'),
        `replacement REGISTER ${address} failed; dropping the old record`,
      );
    }
  }

  /** The registration other than [entry] that holds `type/assigned` on [session], or whose `REGISTER` of it is in flight there. */
  #successorOn(session: ClientSession, entry: Entry, assigned: number): Entry | undefined {
    for (const e of this.#entries) {
      if (
        e !== entry &&
        e.type.equals(entry.type) &&
        ((e.session === session && e.instance === assigned) ||
          (e.pending === session && e.pendingInstance === assigned))
      ) {
        return e;
      }
    }
    return undefined;
  }

  /** Replaces [older] by [newer], which registers the same address: [older] is forgotten, its late answer ignored, and its pending promise settles like [newer]'s. */
  #supersede(older: Entry, newer: Entry): void {
    this.#entries.delete(older);
    older.superseded = true;
    const live = this.#session;
    if (
      live !== undefined &&
      (older.session === live || older.pending === live || older.replacesOn === live)
    ) {
      newer.replacesOn = live;
    }
    if (!older.completer.isSettled) {
      newer.completer.promise.then(
        (value) => older.completer.resolve(value),
        (error: unknown) => older.completer.reject(error),
      );
    }
  }

  #notifyAssigned(entry: Entry, assigned: number): void {
    const callback = entry.onAssigned;
    if (callback === undefined) {
      return;
    }
    try {
      callback(assigned);
    } catch (error) {
      this.log('error', `onAssigned for ${new ServiceAddress(entry.type, assigned)} failed`, error);
    }
  }

  /**
   * Sends `UNREGSTR`. A lost channel counts as success: the record went away
   * with it. Any other failure but `NOT_FOUND` may leave the record
   * registered without the client tracking it, so the channel is dropped
   * (taking the record with it) before the error is rethrown.
   */
  async #sendUnregister(session: ClientSession, type: Name, instance: number): Promise<void> {
    try {
      await session.channel.startRequest(
        Procedures.unregister.toString(),
        new UnregisterRequest(type, instance).encode(),
        { name: Procedures.unregister, ordered: true },
      ).response;
    } catch (error) {
      if (!session.usable) {
        // The record went away with the channel.
        return;
      }
      if (!(error instanceof SwitchboardError) || error.code !== StatusCode.notFound) {
        session.lose(
          Status.of(StatusCode.aborted, 'UNREGSTR failed'),
          `UNREGSTR ${new ServiceAddress(type, instance)} failed (${error})`,
        );
      }
      throw error;
    }
  }

  // -------------------------------------------------------------------------
  // Watch

  /**
   * Subscribes to every type. The reply is ordered with the events: every
   * event before it is part of the snapshot, every event after it is live. A
   * `WATCH` the channel refuses for its request limit (the slot state
   * restored ahead of it fills it) is sent after `reconnectDelay`.
   */
  #startWatch(session: ClientSession): void {
    let reply: Promise<TalkMessage>;
    try {
      reply = session.channel.startRequest(
        Procedures.watch.toString(),
        new WatchRequest().encode(),
        { name: Procedures.watch, ordered: true },
      ).response;
    } catch (error) {
      if (isStatus(error, StatusCode.resourceExhausted) && session.usable) {
        this.log('info', `WATCH not sent yet, retrying in ${this.reconnectDelay} ms: ${error}`);
        session.later(this.reconnectDelay, () => this.#startWatch(session));
        return;
      }
      session.lose(Status.ok, `cannot watch: ${error}`);
      return;
    }
    reply.then(
      () => this.#onSynced(session),
      (error: unknown) => {
        const status =
          error instanceof SwitchboardError
            ? error.status
            : Status.of(StatusCode.unknown, String(error));
        if (status.known === StatusCode.permissionDenied && session.usable) {
          // The credential grants no watch scope: asking again would be
          // refused again. The registrations stay.
          this.log(
            'error',
            `the naming service refuses WATCH (${status}); the table is not mirrored`,
          );
          return;
        }
        session.lose(
          Status.of(StatusCode.unavailable, `WATCH failed: ${status}`),
          `WATCH failed: ${status}`,
        );
      },
    );
  }

  /** A one-way message from the naming service: an event of the subscription. Events before the `WATCH` reply belong to the snapshot. */
  #onEvent(session: ClientSession, event: TalkMessage): void {
    if (!session.alive) {
      return;
    }
    const procedure = event.procedure;
    try {
      if (procedure.equals(Procedures.up)) {
        const record = decodeServiceRecord(event.payload);
        if (session.syncing) {
          session.seen.add(record.address.key);
        }
        this.#applyUp(record);
      } else if (procedure.equals(Procedures.down)) {
        const address = decodeDownEvent(event.payload).record.address;
        session.seen.delete(address.key);
        this.#applyDown(address);
      } else if (procedure.equals(Procedures.slotSpace)) {
        this.#slots.onSpaceItem(session, SlotSpace.decode(event.payload));
      } else if (procedure.equals(Procedures.slot)) {
        this.#slots.onSlotItem(session, SlotItem.decode(event.payload));
      } else {
        this.log('debug', `ignoring message ${event.procedureName}`);
      }
    } catch (error) {
      if (!(error instanceof ProtocolError)) throw error;
      this.log('warn', `malformed ${event.procedureName} event: ${error.message}`);
    }
  }

  /** The `WATCH` reply: the snapshot is complete. Records, spaces and slots it did not mention are gone. */
  #onSynced(session: ClientSession): void {
    if (!session.alive || !session.syncing) {
      return;
    }
    session.syncing = false;
    const gone: ServiceAddress[] = [];
    for (const [key, record] of this.#table) {
      if (!session.seen.has(key)) gone.push(record.address);
    }
    session.seen.clear();
    for (const address of gone) {
      this.#applyDown(address);
    }
    this.#slots.onSynced(session);
    this.#isSynced = true;
    this.#hasSynced = true;
    this.log('info', `naming table synced, ${this.#table.size} records`);
    this.#synced.resolve();
    this.#firstSynced.resolve();
  }

  #applyUp(record: ServiceRecord): void {
    const previous = this.#table.get(record.address.key);
    this.#table.set(record.address.key, record);
    if (previous === undefined || !serviceRecordsEqual(previous, record)) {
      this.#emit(upEvent(record));
    }
  }

  #applyDown(address: ServiceAddress): void {
    if (this.#table.delete(address.key)) {
      this.#emit(downEvent(address));
    }
  }

  #emit(event: ServiceEvent): void {
    if (!this.#closed) {
      this.#emitter.emit('event', event);
    }
  }
}

/** Whether a failed request may still have taken effect: the naming service did not answer in time, or answered something unreadable. */
function isIndeterminate(error: unknown): boolean {
  return (
    !(error instanceof SwitchboardError) ||
    error instanceof ProtocolError ||
    error.code === StatusCode.deadlineExceeded
  );
}

function isStatus(error: unknown, code: StatusCode): boolean {
  return error instanceof SwitchboardError && error.code === code;
}

/** @throws {RangeError} unless [value] is an integer in `min..max`. */
function checkInterval(value: number, min: number, max: number, what: string): void {
  if (!Number.isInteger(value) || value < min || value > max) {
    throw new RangeError(`${what} ${value} outside ${min}..${max}`);
  }
}

/** Runs a reply that may throw synchronously or reject; either goes to [onError]. */
export function settle(action: () => Promise<unknown>, onError: (error: unknown) => void): void {
  try {
    action().catch(onError);
  } catch (error) {
    onError(error);
  }
}

/** An async iterable that fails with [error] when iterated. */
function failing<T>(error: unknown): AsyncIterable<T> {
  return {
    [Symbol.asyncIterator](): AsyncIterator<T> {
      return { next: () => Promise.reject(error) };
    },
  };
}

/** A registration remembered for re-registration. */
class Entry {
  readonly type: Name;
  readonly endpoints: readonly string[];
  /** A private copy of the caller's metadata. */
  readonly metadata: Uint8Array;
  /** Told about every new id. */
  readonly onAssigned: ((instance: number) => void) | undefined;
  /** The assigned id once registered, else the requested id (0: any). */
  instance: number;
  /** The session this entry is registered on. */
  session: ClientSession | undefined;
  /** The session a REGISTER for this entry is in flight on. */
  pending: ClientSession | undefined;
  /** The id that REGISTER asks for (0: any). */
  pendingInstance = 0;
  /** Replaced by a newer registration of the same address. */
  superseded = false;
  /**
   * The session on which a registration this entry superseded may still be
   * published (registered, or in flight, there): if this entry is refused on
   * it, the channel is dropped. Undefined once registered.
   */
  replacesOn: ClientSession | undefined;
  /** Refusals of re-registration in a row. */
  refusals = 0;
  /** Resolves with the first assigned id. */
  readonly completer = new Deferred<number>();

  constructor(
    type: Name,
    endpoints: readonly string[],
    metadata: Uint8Array,
    instance: number,
    onAssigned: ((instance: number) => void) | undefined,
  ) {
    this.type = type;
    this.endpoints = endpoints;
    this.metadata = metadata;
    this.instance = instance;
    this.onAssigned = onAssigned;
  }
}

/** @internal One channel to the naming service. */
export class ClientSession {
  readonly channel: TalkChannel;
  readonly #client: NamingClient;
  readonly #onLost: () => void;
  readonly #lost = new Deferred<void>();
  readonly #closed = new Deferred<void>();
  readonly #timers = new Set<TimerHandle>();
  #closeChannel: Promise<void> | undefined;

  /** Until the `WATCH` reply: events are the snapshot, and the addresses they mention are collected. */
  syncing = true;
  readonly seen = new Set<string>();

  /** Until the `WATCH` reply: slot spaces (by type key) and slots (`typeKey/slot`) seen in the snapshot. */
  readonly seenSpaces = new Set<string>();
  readonly seenSlots = new Set<string>();

  /**
   * Types (by key) whose slot state was sent on this channel: true once with
   * the registration of the type, false when sent without one. Set once
   * every request of the restoration went out.
   */
  readonly restored = new Map<string, boolean>();

  /**
   * What of the slot state went out on this channel: the definition sent per
   * type (and whether the type was registered on the channel then: only then
   * does it carry the instance's capacity), the types whose `HOLDING` was
   * sent with their registration, and those whose served slots were queued
   * for claiming. A restoration that resumes after a retry does not send
   * these again.
   */
  readonly sentSlots = new Map<string, [SpaceDefinition, boolean]>();
  readonly sentHolding = new Set<string>();
  readonly reclaimed = new Set<string>();

  /** Types whose restoration waits for a retry: one of its requests was refused before it was sent. */
  readonly retrying = new Set<string>();

  /** Claims of served slots waiting to be sent, and the number in flight. */
  readonly reclaims: [Name, number, Served][] = [];
  reclaiming = 0;
  /** The claims wait for a retry: one was refused before it was sent. */
  reclaimsPaused = false;

  /** Requests of the client's slot API waiting for the channel to take them, the number in flight, and whether they wait for a retry timer. */
  readonly calls: ClientCall[] = [];
  calling = 0;
  callsPaused = false;

  constructor(channel: TalkChannel, client: NamingClient, onLost: () => void) {
    this.channel = channel;
    this.#client = client;
    this.#onLost = onLost;
  }

  get alive(): boolean {
    return !this.#lost.isSettled;
  }

  /**
   * Alive and the channel still open. A failed request on a channel that is
   * no longer open is a loss of the channel, not a refusal, even before
   * {@link lose} runs.
   */
  get usable(): boolean {
    return this.alive && this.channel.isOpen;
  }

  /** Resolves when the session is lost. */
  get lost(): Promise<void> {
    return this.#lost.promise;
  }

  /** Resolves when the session is torn down and its channel closed. */
  get closed(): Promise<void> {
    return this.#closed.promise;
  }

  /** Runs [action] after [delay] ms unless the session is lost first. */
  later(delay: number, action: () => void): void {
    if (!this.alive) {
      return;
    }
    const clock = this.#client.clock;
    const timer = clock.setTimeout(() => {
      this.#timers.delete(timer);
      if (this.alive) {
        action();
      }
    }, delay);
    this.#timers.add(timer);
  }

  /** Marks the session lost and closes its channel with [status]. Idempotent. */
  lose(status: Status, reason: string): void {
    if (this.#lost.isSettled) {
      return;
    }
    this.#client.log('info', `naming service channel lost: ${reason}`);
    for (const timer of this.#timers) {
      this.#client.clock.clearTimeout(timer);
    }
    this.#timers.clear();
    const waiting = this.calls.splice(0);
    for (const call of waiting) {
      call.response.reject(
        SwitchboardError.of(StatusCode.unavailable, 'naming service channel lost'),
      );
    }
    try {
      this.#closeChannel = this.channel.close(status).catch(() => {});
    } catch {
      this.#closeChannel = Promise.resolve();
    }
    this.#lost.resolve();
    this.#onLost();
  }

  /** Waits for the channel to close and resolves {@link closed}. */
  async tearDown(): Promise<void> {
    await this.#closeChannel;
    this.#closed.resolve();
  }
}
