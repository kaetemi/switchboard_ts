/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { maxInstance, ServiceAddress } from '../address/index.ts';
import { bytesEqual, utf8Encode } from '../bytes.ts';
import type {
  Credential,
  CredentialKind,
  CredentialVerifier,
  Scope,
} from '../identity/contract.ts';
import { Right } from '../identity/contract.ts';
import type { MuxConnection } from '../mux/contract.ts';
import { Name, type NameLike, toName } from '../name.ts';
import { ProtocolError, Status, StatusCode, SwitchboardError } from '../status.ts';
import type {
  ChannelHandler,
  IncomingChannel,
  ServiceEvent,
  ServiceRecord,
} from '../switchboard/contract.ts';
import type { TalkChannel, TalkMessage, TalkRequest } from '../talk/contract.ts';
import { type Clock, systemClock, type TimerHandle } from '../util/clock.ts';
import { Emitter, type Unsubscribe } from '../util/emitter.ts';
import { type Logger, type LogLevel, noopLogger } from '../util/logger.ts';
import {
  ConnectRequest,
  ConnectResponse,
  checkEndpoint,
  DialBackRequest,
  downEvent,
  encodeServiceEvent,
  encodeServiceRecord,
  maxMetadataLength,
  maxU32,
  Procedures,
  RegisterRequest,
  RegisterResponse,
  Services,
  type SlotItem,
  serviceEventProcedure,
  serviceRecord,
  serviceRecordsEqual,
  serviceRecordToString,
  UnregisterRequest,
  upEvent,
  WatchRequest,
} from './naming-protocol.ts';
import { SlotManager, statusOf } from './naming-service-slots.ts';
import type { SlotTable } from './slot-table.ts';

const logName = 'Switchboard.Naming';

/**
 * Issues the renewed credentials of `RENEW`: the shape of the protocol
 * core's `CredentialIssuer.issue` that the naming service uses. `lifetime`
 * in ms; null for a credential that never expires.
 */
export interface NamingCredentialIssuer {
  issue(options: {
    kind: CredentialKind;
    identity: string;
    scopes: readonly Scope[];
    holderKey?: Uint8Array | undefined;
    lifetime: number | null;
  }): Promise<Credential>;
}

/** What the naming service reads of the connection a channel arrived on. */
export type NamingConnection = Pick<
  MuxConnection,
  'peerIdentity' | 'isOpen' | 'identityChanged' | 'goAway'
>;

/** What a {@link NamingService} takes; every member is optional. Durations are ms. */
export interface NamingServiceOptions {
  /**
   * How long, counted from construction, a `REGISTER` that asks for any id
   * (instance 0) waits before it is answered, and slot assignment waits for
   * claims; default 2000; 0 disables the hold. After a naming service
   * restart ids are assigned from 1 again, while surviving services
   * reconnect asking for the ids they had; holding assignments for a moment
   * lets them reclaim those ids before a fresh registration can take one. A
   * held request is sent one `EXTEND` declaring the time left in the hold.
   */
  readonly assignmentHold?: number;
  /** How long a free slot of a holder-only managed space waits for its holder to come back; default 300 000. */
  readonly holderGrace?: number;
  /**
   * The requester timeout of the requests the service sends to instances
   * (`ASSIGN`, `DRAIN`, `FORWARD`, `RESUME`), until the instance declares a
   * deadline or a renewal with `EXTEND`; default 60 000; 0 disables it.
   */
  readonly handoverTimeout?: number;
  /** The largest slot count of a space; `SLOTS` with more is refused with `OUT_OF_RANGE`; default 65 536. */
  readonly maxSlotCount?: number;
  /**
   * How long an instance that failed an `ASSIGN` of a slot is not offered
   * that slot again, the first time; doubled at every further failure up to
   * `assignBackoffMax`; default 200.
   */
  readonly assignBackoff?: number;
  /** Default 10 000. */
  readonly assignBackoffMax?: number;
  /**
   * After a `SLOTS` that gives an instance capacity, the allocator leaves the
   * free slots without a holder alone until no `SLOTS`, `HOLDING` or `CLAIM`
   * of the type has arrived for this long, and at most 5 times
   * `assignmentHold` (but at least this); default 1000; 0 disables it.
   */
  readonly holdingSettle?: number;
  /**
   * Bounds an `ASSIGN` or `DRAIN` overall, whatever the instance declares:
   * one still running after it is cancelled and handled as a failure;
   * default 600 000; 0 removes the bound.
   */
  readonly handoverMaxDuration?: number;
  /** How many times the `RESUME` of a rolled-back migration is sent before its slot is set free; default 5. */
  readonly resumeAttempts?: number;
  /** Verifies the credentials channels present in their open payload; default none (ignored). */
  readonly verifier?: CredentialVerifier | undefined;
  /** Refuse every request of a channel without a valid identity with `UNAUTHENTICATED`; default false. */
  readonly requireCredential?: boolean;
  /** Serves `RENEW`; without one it is answered `UNIMPLEMENTED`. */
  readonly issuer?: NamingCredentialIssuer | undefined;
  /** Whether a valid credential is renewed by `RENEW`; default every one. */
  readonly renewable?: ((credential: Credential) => boolean) | undefined;
  /** Longest a `CONNECT` waits for the instance to dial back and answer; default 30 000; 0 for no bound. */
  readonly brokerTimeout?: number;
  /** Time and timers; default the system clock. */
  readonly clock?: Clock;
  /** Wall time in ms since the epoch, for credential expiry; default `Date.now`. */
  readonly wallTime?: () => number;
  /** Diagnostics, under the name `Switchboard.Naming`; default none. */
  readonly logger?: Logger;
}

/** The events of a {@link NamingService}. */
export type NamingServiceEvents = {
  /** Every `UP` and `DOWN` the service publishes, in order. */
  event: ServiceEvent;
  /** Every `SLOT` event the service publishes, in order. */
  slotEvent: SlotItem;
};

/**
 * The naming service (`_ns`): hands out instance ids, keeps the
 * authoritative table of live service instances, and pushes changes to the
 * channels subscribed to them. See the wiki page "Polyverse Switchboard
 * Naming Service".
 *
 * The service speaks Talk over channels handed to {@link serve}, one per
 * client. A registration is owned by the channel it was made on; when that
 * channel closes for any reason, every record it owns is removed and `DOWN`
 * is published for each. This is the only liveness mechanism. A record
 * carries the liveness of the channel that made it, not of the endpoint it
 * names: one channel may register many records for endpoints that never
 * talk to the naming service (a registrar), and then retires them itself.
 *
 * Procedures: `REGISTER`, `UNREGSTR`, `WATCH`, `UNWATCH`, `RENEW`, `CONNECT`
 * (requests), `LOOKUP` (stream request), and for sharding `SLOTS`,
 * `HOLDING`, `CLAIM`, `RELEASE`, `LOCATE` (requests) and `MIGRATE` (stream
 * request). Requests for any other procedure are answered with
 * `ABORT UNIMPLEMENTED`; plain messages are ignored. Malformed payloads are
 * answered with `ABORT INVALID_ARGUMENT`.
 *
 * Subscriptions: `WATCH` sends the snapshot of the matching records and slot
 * spaces as one-way events (`UP`, then `SLOTSPC` and `SLOT`), then its
 * reply, and subscribes the channel; from then on every change is sent to it
 * as an event, at most once per channel however many of its subscriptions
 * match. A subscription is per channel and type filter and counts the
 * `WATCH`es that hold it: `UNWATCH` with the same filter gives one back, the
 * last one ends it, and every subscription of a channel ends when the
 * channel closes. There is no heartbeat.
 *
 * Sharding (wiki page "Polyverse Switchboard Sharding"): the service keeps a
 * slot table per sharded type ({@link slotTables}), publishes it to
 * watchers, assigns the slots of managed spaces, and orchestrates
 * hand-overs by sending `ASSIGN`, `DRAIN`, `FORWARD` and `RESUME` to
 * instances over their registration channels. During `assignmentHold` slot
 * assignment waits too: `CLAIM`s are collected and resolved when the hold
 * ends (higher claimed epoch first, then the earlier claim), `HOLDING`
 * declarations are answered after them, and `LOCATE` of a free slot, the
 * allocator and migrations wait.
 *
 * Identity (wiki page "Polyverse Switchboard Identity and Credentials"):
 * each served channel is identified by the peer identity of the connection
 * it arrived on (`IDENT`), else by a bearer credential in its open payload,
 * verified with `verifier`. An identified channel may do what its
 * credential's scopes grant; a channel without an identity may do
 * everything, unless `requireCredential`; a channel whose identity expires
 * is ended at the expiry unless it was renewed. `RENEW` re-issues the
 * channel's credential with `issuer`, and `CONNECT` has an instance
 * registered without endpoints dial the consumer that asks.
 *
 * The service registers nothing by itself; the host registers `_ns/1` with
 * {@link registerLocal} once it knows its listening endpoints.
 */
export class NamingService implements AsyncDisposable {
  /** Largest number of subscriptions (distinct type filters) one channel may hold. */
  static readonly maxWatchesPerChannel = 1024;

  readonly assignmentHold: number;
  readonly holderGrace: number;
  readonly handoverTimeout: number;
  readonly maxSlotCount: number;
  readonly assignBackoff: number;
  readonly assignBackoffMax: number;
  readonly handoverMaxDuration: number;
  readonly holdingSettle: number;
  readonly resumeAttempts: number;
  readonly verifier: CredentialVerifier | undefined;
  readonly requireCredential: boolean;
  readonly issuer: NamingCredentialIssuer | undefined;
  readonly renewable: ((credential: Credential) => boolean) | undefined;
  readonly brokerTimeout: number;
  readonly clock: Clock;
  readonly wallTime: () => number;
  readonly logger: Logger;

  readonly #slots: SlotManager;
  readonly #emitter = new Emitter<NamingServiceEvents>();
  readonly #closedSignal = new AbortController();
  readonly #table = new Map<string, ServiceRecord>();
  readonly #instances = new Map<number, Registration>();
  readonly #sessions = new Set<ServiceSession>();
  /** Sessions with at least one subscription. */
  readonly #watching = new Set<ServiceSession>();
  #nextInstance = 1;
  #closed = false;
  #closePromise: Promise<void> | undefined;
  #holdTimer: TimerHandle | undefined;
  #holdEnd = 0;
  readonly #held: Held[] = [];
  /**
   * The sessions served over each connection, whose `IDENT`s are followed by
   * one listener per connection however many channels it carries. An entry
   * stays, without sessions, while its listener waits.
   */
  readonly #connections = new Map<NamingConnection, Set<ServiceSession>>();

  /**
   * Creates an empty service.
   *
   * @throws {RangeError} if `assignBackoff` is not positive, if
   * `assignBackoffMax` is shorter than `assignBackoff`, if a duration is
   * negative, or if `maxSlotCount` or `resumeAttempts` is not positive.
   */
  constructor(options: NamingServiceOptions = {}) {
    this.assignmentHold = options.assignmentHold ?? 2000;
    this.holderGrace = options.holderGrace ?? 300_000;
    this.handoverTimeout = options.handoverTimeout ?? 60_000;
    this.maxSlotCount = options.maxSlotCount ?? 65_536;
    this.assignBackoff = options.assignBackoff ?? 200;
    this.assignBackoffMax = options.assignBackoffMax ?? 10_000;
    this.holdingSettle = options.holdingSettle ?? 1000;
    this.handoverMaxDuration = options.handoverMaxDuration ?? 600_000;
    this.resumeAttempts = options.resumeAttempts ?? 5;
    this.verifier = options.verifier;
    this.requireCredential = options.requireCredential ?? false;
    this.issuer = options.issuer;
    this.renewable = options.renewable;
    this.brokerTimeout = options.brokerTimeout ?? 30_000;
    this.clock = options.clock ?? systemClock;
    this.wallTime = options.wallTime ?? Date.now;
    this.logger = options.logger ?? noopLogger;
    if (!(this.assignBackoff > 0)) {
      throw new RangeError(`assignBackoff must be positive: ${this.assignBackoff}`);
    }
    if (!(this.assignBackoffMax >= this.assignBackoff)) {
      throw new RangeError(
        `assignBackoffMax must not be shorter than assignBackoff: ${this.assignBackoffMax}`,
      );
    }
    for (const [name, value] of [
      ['assignmentHold', this.assignmentHold],
      ['holderGrace', this.holderGrace],
      ['handoverTimeout', this.handoverTimeout],
      ['handoverMaxDuration', this.handoverMaxDuration],
      ['holdingSettle', this.holdingSettle],
      ['brokerTimeout', this.brokerTimeout],
    ] as const) {
      if (!(value >= 0)) {
        throw new RangeError(`${name} must not be negative: ${value}`);
      }
    }
    if (
      !Number.isInteger(this.maxSlotCount) ||
      this.maxSlotCount < 1 ||
      this.maxSlotCount > maxU32
    ) {
      throw new RangeError(`maxSlotCount out of range: ${this.maxSlotCount}`);
    }
    if (!(this.resumeAttempts >= 1)) {
      throw new RangeError(`resumeAttempts must be positive: ${this.resumeAttempts}`);
    }
    this.#emitter.onListenerError = (error) =>
      this.log('error', 'a naming service event listener threw', error);
    this.#slots = new SlotManager({
      service: this,
      instances: this.#instances,
      permits: (session, message, right, type) => this.#permits(session, message, right, type),
      permitsSpace: (session, message, type) => this.#permitsSpace(session, message, type),
      holdLeft: () => this.#holdLeft,
      publishItem: (type, procedure, payload) => this.#publishItem(type, procedure, payload),
      emitSlotItem: (item) => {
        // As #publish: the slot changes of the close itself still go out.
        if (!this.#closedSignal.signal.aborted) this.#emitter.emit('slotEvent', item);
      },
    });
    if (this.assignmentHold > 0) {
      this.#holdEnd = this.clock.now() + this.assignmentHold;
      this.#holdTimer = this.clock.setTimeout(() => this.#endHold(), this.assignmentHold);
    }
  }

  /** @internal Logs under `Switchboard.Naming`. */
  log(level: LogLevel, message: string, error?: unknown): void {
    try {
      this.logger.log(level, logName, message, error);
    } catch {
      // A logger that throws must not break the service.
    }
  }

  /** The time left in the assignment hold, in ms. */
  get #holdLeft(): number {
    return Math.max(0, this.#holdEnd - this.clock.now());
  }

  /** Read-only live view of the table: every registered record by `ServiceAddress.key`. */
  get table(): ReadonlyMap<string, ServiceRecord> {
    return this.#table;
  }

  /** The records of [type] in the table. */
  records(type: NameLike): ServiceRecord[] {
    const name = toName(type);
    return [...this.#table.values()].filter((r) => r.address.type.equals(name));
  }

  /**
   * Adds a listener: `event` for every `UP` and `DOWN` the service publishes,
   * `slotEvent` for every `SLOT` event. Returns the function that removes it.
   */
  on<K extends keyof NamingServiceEvents>(
    event: K,
    listener: (value: NamingServiceEvents[K]) => void,
  ): Unsubscribe {
    return this.#emitter.on(event, listener);
  }

  /** As {@link on}, for one event. */
  once<K extends keyof NamingServiceEvents>(
    event: K,
    listener: (value: NamingServiceEvents[K]) => void,
  ): Unsubscribe {
    return this.#emitter.once(event, listener);
  }

  /** Removes a listener added with {@link on}. */
  off<K extends keyof NamingServiceEvents>(
    event: K,
    listener: (value: NamingServiceEvents[K]) => void,
  ): void {
    this.#emitter.off(event, listener);
  }

  /** Every `UP` and `DOWN` the service publishes, from the moment iteration starts; it ends when the service is closed. */
  get events(): AsyncIterable<ServiceEvent> {
    return this.#emitter.events('event', this.#closedSignal.signal);
  }

  /** Every `SLOT` event the service publishes, as {@link events}. */
  get slotEvents(): AsyncIterable<SlotItem> {
    return this.#emitter.events('slotEvent', this.#closedSignal.signal);
  }

  /** Number of subscriptions over all channels: one per channel and type filter, however many `WATCH`es hold it. */
  get watchCount(): number {
    let count = 0;
    for (const session of this.#watching) {
      count += session.watches.size;
    }
    return count;
  }

  /** Read-only live view of the slot tables, by `Name.key` of the type. A redefinition of a space replaces its {@link SlotTable}. */
  get slotTables(): ReadonlyMap<string, SlotTable> {
    return this.#slots.views;
  }

  /** The slot table of [type], or undefined if it has no slot space. */
  slotTable(type: NameLike): SlotTable | undefined {
    return this.#slots.views.get(toName(type).key);
  }

  /** Number of channels currently served. */
  get channelCount(): number {
    return this.#sessions.size;
  }

  /**
   * @internal Number of requests kept while they wait for something slow: an
   * `ASSIGN` or a migration in progress, the assignment hold, a brokered
   * `CONNECT`.
   */
  get waitingRequestCount(): number {
    return this.#slots.waitingCount;
  }

  /** True while `REGISTER` requests for any id are held; see `assignmentHold`. */
  get isHoldingAssignments(): boolean {
    return this.#holdTimer !== undefined;
  }

  /** @internal The id the next assignment tries first. Ids in use are skipped. */
  get nextInstance(): number {
    return this.#nextInstance;
  }

  /** @internal */
  set nextInstance(id: number) {
    if (!Number.isInteger(id) || id < 1 || id > maxInstance) {
      throw new RangeError(`id ${id} outside 1..${maxInstance}`);
    }
    this.#nextInstance = id;
  }

  /**
   * A {@link ChannelHandler} that serves every incoming channel: it wraps the
   * channel with `IncomingChannel.talk` (the node's talk options) and passes
   * it to {@link serve}, with the connection and the open payload. Register it
   * for `_ns/1` on the hosting node, with the matching local record.
   */
  get handler(): ChannelHandler {
    return (incoming: IncomingChannel) =>
      this.serve(incoming.talk(), {
        connection: incoming.connection,
        credential: incoming.address.payload,
      });
  }

  /**
   * Serves one client over [channel] until it closes. Registrations made on
   * this channel are removed, with `DOWN` events, when it closes.
   *
   * The channel's identity is the peer identity of `connection` (read at
   * every request), else the credential in `credential` (the application
   * payload of its OPEN), verified with `verifier` before the first request
   * is handled, else none. Only a bearer credential identifies a channel in
   * its payload. Once the connection identifies, its credential remains
   * authoritative. Every later `IDENT` updates the expiry.
   *
   * A credential that replaces the one an identified channel had holds what
   * the channel set up to the new credential. One of another identity ends
   * the channel as its expiry would (without `GOAWAY`). One of the same
   * identity with other scopes ends every subscription it does not grant
   * `watch` for, removes every record of a type it does not grant `register`
   * for, and refuses a held `REGISTER` of such a type.
   *
   * Takes over `TalkChannel.messages`, so each channel can be served only
   * once: rethrows the channel's error if its messages are already taken, in
   * which case the service keeps nothing of it. After {@link close} the
   * channel is closed immediately with `GOING_AWAY`.
   */
  serve(
    channel: TalkChannel,
    options: {
      connection?: NamingConnection | undefined;
      credential?: Uint8Array | undefined;
    } = {},
  ): void {
    if (this.#closed) {
      void channel.close(Status.of(StatusCode.goingAway, 'naming service closed')).catch(() => {});
      return;
    }
    // Take the messages before keeping the session: if they are taken
    // already, this throws and nothing is left behind.
    const iterator = channel.messages[Symbol.asyncIterator]();
    const session = new ServiceSession(channel, options.connection, iterator);
    this.#sessions.add(session);
    channel.done.then(
      () => this.#drop(session),
      () => this.#drop(session),
    );
    this.#followIdentity(session);
    // Observe the connection before waiting for the payload verifier: its
    // credential can expire while that check is still in flight.
    this.#watchIdentity(session);
    const credential = options.credential;
    if (credential !== undefined && credential.length > 0 && this.verifier !== undefined) {
      // Requests wait until the credential is checked.
      session.verifying = this.#checkPayload(session, credential.slice());
    }
    void this.#listen(session);
  }

  async #listen(session: ServiceSession): Promise<void> {
    try {
      for (;;) {
        const next = await session.iterator.next();
        if (next.done === true) break;
        if (session.verifying !== undefined) {
          await session.verifying;
          session.verifying = undefined;
        }
        this.#onMessage(session, next.value);
      }
    } catch (error) {
      this.log('debug', `naming client channel failed: ${error}`);
    }
    this.#drop(session);
  }

  /**
   * Registers a record owned by the service itself rather than by a channel,
   * for example `_ns/1` with the service's own endpoints. It stays until
   * {@link unregisterLocal}; registering the same address locally again
   * replaces its endpoints and metadata (`UP` if either changed).
   *
   * `instance` 0 (the default) assigns an id, at once (`assignmentHold` does
   * not apply). `metadata` (copied) is published with the record. Returns the
   * instance id.
   *
   * @throws {SwitchboardError} `INVALID_ARGUMENT` for an empty type, more than
   * 255 endpoints, an endpoint {@link checkEndpoint} refuses, or metadata
   * longer than {@link maxMetadataLength}; `ALREADY_EXISTS` if `instance` is
   * in use by another registration; `RESOURCE_EXHAUSTED` if no id is left;
   * `FAILED_PRECONDITION` after {@link close}.
   * @throws {RangeError} for an instance outside the `u48` range.
   */
  registerLocal(
    type: NameLike,
    endpoints: readonly string[],
    options: { instance?: number; metadata?: Uint8Array } = {},
  ): number {
    if (this.#closed) {
      throw SwitchboardError.of(StatusCode.failedPrecondition, 'naming service closed');
    }
    const instance = options.instance ?? 0;
    if (!Number.isInteger(instance) || instance < 0 || instance > maxInstance) {
      throw new RangeError(`instance ${instance} outside 0..${maxInstance}`);
    }
    return this.#add(
      toName(type),
      instance,
      endpoints,
      options.metadata === undefined ? new Uint8Array(0) : options.metadata.slice(),
      undefined,
    );
  }

  /**
   * Removes a record made with {@link registerLocal} and publishes `DOWN`.
   *
   * @throws {SwitchboardError} `NOT_FOUND` if there is no such record and
   * `PERMISSION_DENIED` if it is owned by a channel.
   */
  unregisterLocal(address: ServiceAddress): void {
    const registration = this.#lookup(address.type, address.instance);
    if (registration.owner !== undefined) {
      throw SwitchboardError.of(StatusCode.permissionDenied, `${address} is owned by a channel`);
    }
    this.#remove(registration);
  }

  /**
   * Ends every subscription, aborts every held `REGISTER` with `GOING_AWAY`,
   * removes the records of every channel (publishing `DOWN` on the events,
   * and the slots their owners free on the slot events, not to the
   * channels), closes the channels with `GOING_AWAY`, and ends the events. Records made with {@link registerLocal} stay in {@link table}. No
   * timer is left running. Calling it again returns the same promise.
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
    const goingAway = Status.of(StatusCode.goingAway, 'naming service closed');
    this.#slots.close();
    if (this.#holdTimer !== undefined) {
      this.clock.clearTimeout(this.#holdTimer);
      this.#holdTimer = undefined;
    }
    const held = this.#held.splice(0);
    for (const h of held) {
      if (h.message.canReply) {
        this.abortRequest(h.message, goingAway);
      }
    }
    for (const session of [...this.#watching]) {
      this.#endWatches(session);
    }
    const sessions = [...this.#sessions];
    for (const session of sessions) {
      this.#drop(session);
    }
    this.#connections.clear();
    await Promise.all(sessions.map((s) => s.channel.close(goingAway).catch(() => {})));
    this.#closedSignal.abort();
  }

  // -------------------------------------------------------------------------
  // Dispatch

  #onMessage(session: ServiceSession, message: TalkMessage): void {
    if (!session.active) {
      return;
    }
    const procedure = message.procedure;
    if (!message.expectsReply) {
      this.log('debug', `ignoring plain message ${message.procedureName}`);
      return;
    }
    const identity = this.#identityOf(session);
    if (identity === undefined) {
      if (this.requireCredential || session.wasIdentified) {
        this.log(
          'info',
          `${message.procedureName} refused: ` +
            `${session.wasIdentified ? 'the credential expired' : 'no credential'}`,
        );
        this.abortRequest(message, unauthenticated);
        return;
      }
    } else if (
      !session.wasIdentified ||
      session.expiryTimer === undefined ||
      identity !== session.credential
    ) {
      // Not watched yet, or replaced (a request can overtake the listener of
      // the connection's IDENTs): checked before the request is.
      this.#watchIdentity(session);
      if (!session.active) {
        this.abortRequest(message, unauthenticated);
        return;
      }
    }
    if (procedure.equals(Procedures.register)) {
      this.#onRegister(session, message);
    } else if (procedure.equals(Procedures.unregister)) {
      this.#onUnregister(session, message);
    } else if (procedure.equals(Procedures.watch)) {
      this.#onWatch(session, message);
    } else if (procedure.equals(Procedures.unwatch)) {
      this.#onUnwatch(session, message);
    } else if (procedure.equals(Procedures.lookup)) {
      this.#onLookup(session, message);
    } else if (procedure.equals(Procedures.renew)) {
      this.#onRenew(session, message);
    } else if (procedure.equals(Procedures.connect)) {
      this.#onConnect(session, message);
    } else if (procedure.equals(Procedures.slots)) {
      this.#slots.onSlots(session, message);
    } else if (procedure.equals(Procedures.holding)) {
      this.#slots.onHolding(session, message);
    } else if (procedure.equals(Procedures.claim)) {
      this.#slots.onClaim(session, message);
    } else if (procedure.equals(Procedures.release)) {
      this.#slots.onRelease(session, message);
    } else if (procedure.equals(Procedures.locate)) {
      this.#slots.onLocate(session, message);
    } else if (procedure.equals(Procedures.migrate)) {
      this.#slots.onMigrate(session, message);
    } else {
      this.log('debug', `unknown procedure ${message.procedureName}`);
      this.abortRequest(
        message,
        Status.of(StatusCode.unimplemented, `unknown procedure ${message.procedureName}`),
      );
    }
  }

  #onRegister(session: ServiceSession, message: TalkMessage): void {
    let request: RegisterRequest;
    try {
      request = RegisterRequest.decode(message.payload);
    } catch (error) {
      this.#abortDecode(message, error);
      return;
    }
    if (
      !this.#permits(session, message, Right.register, request.type) ||
      !this.#permitsRelay(session, message, request)
    ) {
      return;
    }
    try {
      check(request.type, request.endpoints, request.metadata);
    } catch (error) {
      this.abortRequest(message, statusOf(error));
      return;
    }
    if (request.requestedInstance === 0 && this.#holdTimer !== undefined) {
      this.#hold(session, message, request);
      return;
    }
    this.#register(session, message, request);
  }

  #register(session: ServiceSession, message: TalkMessage, request: RegisterRequest): void {
    let instance: number;
    try {
      instance = this.#add(
        request.type,
        request.requestedInstance,
        request.endpoints,
        request.metadata,
        session,
      );
    } catch (error) {
      if (!(error instanceof SwitchboardError)) throw error;
      this.abortRequest(message, error.status);
      return;
    }
    this.replyTo(message, new RegisterResponse(instance).encode());
  }

  #onUnregister(session: ServiceSession, message: TalkMessage): void {
    let request: UnregisterRequest;
    try {
      request = UnregisterRequest.decode(message.payload);
    } catch (error) {
      this.#abortDecode(message, error);
      return;
    }
    try {
      const registration = this.#lookup(request.type, request.instance);
      if (registration.owner !== session) {
        throw SwitchboardError.of(
          StatusCode.permissionDenied,
          `${registration.address} is owned by another channel`,
        );
      }
      this.#remove(registration);
    } catch (error) {
      if (!(error instanceof SwitchboardError)) throw error;
      this.abortRequest(message, error.status);
      return;
    }
    this.replyTo(message, new Uint8Array(0));
  }

  /** The payload of a `WATCH` or `UNWATCH`, or undefined once [message] is answered `INVALID_ARGUMENT`. */
  #subscriptionRequest(message: TalkMessage): WatchRequest | undefined {
    if (message.expectsStream) {
      this.abortRequest(
        message,
        Status.of(StatusCode.invalidArgument, `${message.procedureName} is not a stream request`),
      );
      return undefined;
    }
    try {
      return WatchRequest.decode(message.payload);
    } catch (error) {
      this.#abortDecode(message, error);
      return undefined;
    }
  }

  /** Whether [session] may hold the subscription of [request]; answers [message] when not. */
  #permitsWatch(session: ServiceSession, message: TalkMessage, request: WatchRequest): boolean {
    const type = request.type;
    return type === undefined
      ? this.#permitsEvery(session, message, Right.watch)
      : this.#permits(session, message, Right.watch, type);
  }

  #onWatch(session: ServiceSession, message: TalkMessage): void {
    const request = this.#subscriptionRequest(message);
    if (request === undefined || !this.#permitsWatch(session, message, request)) {
      return;
    }
    const type = request.type;
    const key = watchKey(type);
    const count = session.watches.get(key)?.count;
    if (count === undefined && session.watches.size >= NamingService.maxWatchesPerChannel) {
      this.abortRequest(
        message,
        Status.of(
          StatusCode.resourceExhausted,
          `more than ${NamingService.maxWatchesPerChannel} subscriptions on one channel`,
        ),
      );
      return;
    }
    const matches = (recordType: Name): boolean => type === undefined || type.equals(recordType);
    // Snapshot, reply and subscription happen in one synchronous step: a
    // change is either in the snapshot or sent after the reply.
    for (const record of this.#table.values()) {
      if (
        matches(record.address.type) &&
        !session.send(Procedures.up, encodeServiceRecord(record), this)
      ) {
        return;
      }
    }
    for (const [procedure, payload] of this.#slots.snapshot(matches)) {
      if (!session.send(procedure, payload, this)) {
        return;
      }
    }
    try {
      message.reply(new Uint8Array(0)).catch(() => {});
    } catch (error) {
      this.log('debug', `WATCH reply failed: ${error}`);
      return;
    }
    session.watches.set(key, { type, count: (count ?? 0) + 1 });
    this.#watching.add(session);
    this.log('debug', `watch ${type ?? '*'}: ${count ?? 0} -> ${(count ?? 0) + 1}`);
  }

  #onUnwatch(session: ServiceSession, message: TalkMessage): void {
    const request = this.#subscriptionRequest(message);
    if (request === undefined || !this.#permitsWatch(session, message, request)) {
      return;
    }
    const type = request.type;
    const key = watchKey(type);
    const count = session.watches.get(key)?.count;
    if (count === undefined) {
      this.abortRequest(
        message,
        Status.of(
          StatusCode.notFound,
          `no subscription to ${type ?? 'every type'} on this channel`,
        ),
      );
      return;
    }
    // Ended before the reply: no event for it follows the reply.
    if (count > 1) {
      session.watches.set(key, { type, count: count - 1 });
    } else {
      session.watches.delete(key);
      if (session.watches.size === 0) {
        this.#watching.delete(session);
      }
    }
    this.log('debug', `watch ${type ?? '*'}: ${count} -> ${count - 1}`);
    this.replyTo(message, new Uint8Array(0));
  }

  /** Ends every subscription of [session]. */
  #endWatches(session: ServiceSession): void {
    session.watches.clear();
    this.#watching.delete(session);
  }

  #onLookup(session: ServiceSession, message: TalkMessage): void {
    if (!message.expectsStream) {
      this.abortRequest(
        message,
        Status.of(StatusCode.invalidArgument, 'LOOKUP is a stream request'),
      );
      return;
    }
    let type: Name | undefined;
    try {
      type = WatchRequest.decode(message.payload).type;
    } catch (error) {
      this.#abortDecode(message, error);
      return;
    }
    if (type === undefined) {
      this.abortRequest(message, Status.of(StatusCode.invalidArgument, 'LOOKUP needs a type'));
      return;
    }
    if (!this.#permits(session, message, Right.watch, type)) {
      return;
    }
    const wanted = type;
    try {
      for (const record of this.#table.values()) {
        if (record.address.type.equals(wanted)) {
          message
            .replyItem(encodeServiceRecord(record), { procedure: Procedures.up })
            .catch(() => {});
        }
      }
      for (const [procedure, payload] of this.#slots.snapshot((t) => t.equals(wanted))) {
        message.replyItem(payload, { procedure }).catch(() => {});
      }
      message.reply(new Uint8Array(0)).catch(() => {});
    } catch (error) {
      this.log('debug', `LOOKUP reply failed: ${error}`);
    }
  }

  // -------------------------------------------------------------------------
  // Identity

  /**
   * The valid credential identifying [session] now: its connection's peer
   * identity, else its payload credential only until the connection first
   * identifies, else undefined.
   */
  #identityOf(session: ServiceSession): Credential | undefined {
    const connection = session.connection?.peerIdentity?.credential;
    if (connection !== undefined) {
      session.byConnection = true;
      return connection;
    }
    if (session.byConnection) {
      return undefined;
    }
    const payload = session.payloadCredential;
    return payload !== undefined && !payload.isExpired(this.wallTime()) ? payload : undefined;
  }

  /** Checks the credential in the open payload of [session]; its requests wait for it. */
  async #checkPayload(session: ServiceSession, bytes: Uint8Array): Promise<void> {
    const verifier = this.verifier as CredentialVerifier;
    try {
      const credential = await verifier.verify(bytes, { now: this.wallTime() });
      if (credential.holderKey !== undefined) {
        this.log(
          'info',
          `ignoring the payload credential of "${credential.identity}": it names a holder ` +
            'key, which only IDENT proves',
        );
      } else {
        session.payloadCredential = credential;
      }
    } catch (error) {
      this.log('debug', `channel payload is not a valid credential: ${error}`);
    }
    if (!session.active) {
      return;
    }
    this.#watchIdentity(session);
  }

  /**
   * Notes that [session] is identified, if it is, and ends it when its
   * identity expires without being renewed. A credential that replaced the
   * one the session had is checked first, which may end the session.
   */
  #watchIdentity(session: ServiceSession): void {
    const credential = this.#identityOf(session);
    if (credential === undefined || !session.active) {
      return;
    }
    const previous = session.credential;
    if (credential !== previous) {
      session.credential = credential;
      if (previous !== undefined && !this.#replaced(session, previous, credential)) {
        return;
      }
    }
    session.wasIdentified = true;
    if (session.expiryTimer !== undefined) {
      this.clock.clearTimeout(session.expiryTimer);
      session.expiryTimer = undefined;
    }
    if (credential.expiresAt === 0) {
      return;
    }
    const left = Math.max(0, credential.expiresAt * 1000 - this.wallTime());
    session.expiryTimer = this.clock.setTimeout(() => {
      session.expiryTimer = undefined;
      this.#onExpiry(session);
    }, left);
  }

  /** Holds [session], identified by [previous], to [credential], which replaced it. Returns false when that ended the session. */
  #replaced(session: ServiceSession, previous: Credential, credential: Credential): boolean {
    if (credential.identity !== previous.identity) {
      this.log(
        'info',
        `naming channel ended: its identity changed from "${previous.identity}" to ` +
          `"${credential.identity}"`,
      );
      this.#drop(session);
      void session.channel.close(identityChanged).catch(() => {});
      return false;
    }
    this.#narrow(session, credential);
    return true;
  }

  /**
   * Ends what [session] set up that [credential], of the same identity as the
   * credential it replaced, does not grant: subscriptions without `watch`,
   * records without `register`, and held `REGISTER`s without `register`.
   */
  #narrow(session: ServiceSession, credential: Credential): void {
    const ended: string[] = [];
    for (const [key, watch] of session.watches) {
      const granted =
        watch.type === undefined
          ? grantsEvery(credential, Right.watch)
          : credential.allows(Right.watch, watch.type);
      if (!granted) ended.push(key);
    }
    for (const key of ended) {
      const type = session.watches.get(key)?.type;
      session.watches.delete(key);
      this.log(
        'info',
        `subscription to ${type ?? 'every type'} ended: "${credential.identity}" no longer has ` +
          'the watch scope',
      );
    }
    if (ended.length > 0 && session.watches.size === 0) {
      this.#watching.delete(session);
    }
    for (const h of [...this.#held]) {
      if (h.session === session && !credential.allows(Right.register, h.request.type)) {
        this.#held.splice(this.#held.indexOf(h), 1);
        this.abortRequest(h.message, permissionDenied);
      }
    }
    for (const registration of [...session.owned]) {
      if (!credential.allows(Right.register, registration.address.type)) {
        this.log(
          'info',
          `${registration.address} removed: "${credential.identity}" no longer has the ` +
            'register scope',
        );
        this.#remove(registration);
      }
    }
  }

  /** Follows the later `IDENT`s of the connection of [session], not only its requests. */
  #followIdentity(session: ServiceSession): void {
    const connection = session.connection;
    if (connection === undefined || !connection.isOpen) {
      return;
    }
    const sessions = this.#connections.get(connection);
    if (sessions !== undefined) {
      sessions.add(session);
      return;
    }
    this.#connections.set(connection, new Set([session]));
    this.#awaitIdentity(connection);
  }

  /**
   * Waits for the next `IDENT` of [connection], or its end, and has every
   * session it carries look at its identity again. Stops, forgetting the
   * connection, once it carries no session or has ended.
   */
  #awaitIdentity(connection: NamingConnection): void {
    connection.identityChanged.then(
      () => {
        const sessions = this.#connections.get(connection);
        if (sessions === undefined) {
          return;
        }
        if (sessions.size === 0 || !connection.isOpen) {
          this.#connections.delete(connection);
          return;
        }
        for (const session of [...sessions]) {
          this.#watchIdentity(session);
        }
        this.#awaitIdentity(connection);
      },
      () => {},
    );
  }

  #onExpiry(session: ServiceSession): void {
    if (!session.active) {
      return;
    }
    if (this.#identityOf(session) !== undefined) {
      // Renewed (another IDENT, or RENEW of a payload credential), or the
      // timer ran early.
      this.#watchIdentity(session);
      return;
    }
    const connection = session.connection;
    const byConnection = session.byConnection;
    this.log(
      'info',
      `naming channel ${byConnection ? 'and connection ' : ''}ended: the credential expired`,
    );
    const status = Status.of(StatusCode.unauthenticated, 'credential expired');
    this.#drop(session);
    void session.channel.close(status).catch(() => {});
    if (byConnection && connection?.isOpen === true) {
      void connection.goAway(status).catch(() => {});
    }
  }

  /** Whether [session] may use [right] on [type]; answers [message] `UNAUTHENTICATED` or `PERMISSION_DENIED` when it may not. */
  #permits(session: ServiceSession, message: TalkMessage, right: Right, type: Name): boolean {
    const credential = this.#identityOf(session);
    if (credential === undefined) {
      if (this.requireCredential || session.wasIdentified) {
        this.abortRequest(message, unauthenticated);
        return false;
      }
      return true;
    }
    if (credential.allows(right, type)) {
      return true;
    }
    this.log(
      'info',
      `${message.procedureName} refused: "${credential.identity}" has no ${rightName(right)} ` +
        `scope for ${type.isEmpty ? 'the empty type' : type}`,
    );
    this.abortRequest(message, permissionDenied);
    return false;
  }

  /** Whether [session] may use [right] on every type (a scope with the pattern `*`, or `admin`). */
  #permitsEvery(session: ServiceSession, message: TalkMessage, right: Right): boolean {
    const credential = this.#identityOf(session);
    if (credential === undefined) {
      return this.#permits(session, message, right, Name.empty);
    }
    if (grantsEvery(credential, right)) {
      return true;
    }
    this.log(
      'info',
      `${message.procedureName} refused: "${credential.identity}" has no ${rightName(right)} ` +
        'scope for every type',
    );
    this.abortRequest(message, permissionDenied);
    return false;
  }

  /**
   * Whether [session] may register [request]: a `_relay` record's metadata is
   * the identity consumers name as the receiver of their `IDENT` to the
   * relay, so an identified channel registers one only with its own identity
   * there.
   */
  #permitsRelay(session: ServiceSession, message: TalkMessage, request: RegisterRequest): boolean {
    if (!request.type.equals(Services.relay)) {
      return true;
    }
    const credential = this.#identityOf(session);
    if (credential === undefined || bytesEqual(utf8Encode(credential.identity), request.metadata)) {
      return true;
    }
    this.log(
      'info',
      `REGISTER refused: "${credential.identity}" registers a relay whose metadata names ` +
        'another identity',
    );
    this.abortRequest(message, permissionDenied);
    return false;
  }

  /** Whether [session] may define, redefine (while blank) or confirm the slot space of [type]: `claim` or `register` for it. */
  #permitsSpace(session: ServiceSession, message: TalkMessage, type: Name): boolean {
    const credential = this.#identityOf(session);
    if (credential === undefined) {
      return this.#permits(session, message, Right.claim, type);
    }
    if (credential.allows(Right.claim, type) || credential.allows(Right.register, type)) {
      return true;
    }
    this.log(
      'info',
      `SLOTS refused: "${credential.identity}" has no claim or register scope for ${type}`,
    );
    this.abortRequest(message, permissionDenied);
    return false;
  }

  // -------------------------------------------------------------------------
  // RENEW

  #onRenew(session: ServiceSession, message: TalkMessage): void {
    const issuer = this.issuer;
    if (issuer === undefined) {
      this.abortRequest(message, Status.of(StatusCode.unimplemented, 'RENEW is not served here'));
      return;
    }
    void this.#renew(session, message, issuer);
  }

  /**
   * Answers `RENEW` with a credential re-issued by [issuer]: the channel's own
   * (an empty payload, or its bytes), or the one in the payload, which must be
   * valid and, on an identified channel, carry its identity. The renewal of
   * the channel's own payload credential replaces it.
   */
  async #renew(
    session: ServiceSession,
    message: TalkMessage,
    issuer: NamingCredentialIssuer,
  ): Promise<void> {
    const own = this.#identityOf(session);
    let current = own;
    if (
      message.payload.length > 0 &&
      !(own !== undefined && bytesEqual(own.encode(), message.payload))
    ) {
      current = undefined;
      const verifier = this.verifier;
      if (verifier !== undefined) {
        try {
          current = await verifier.verify(message.payload.slice(), { now: this.wallTime() });
        } catch (error) {
          this.log('debug', `RENEW of an invalid credential: ${error}`);
        }
      }
      if (
        current !== undefined &&
        own !== undefined &&
        (current.identity !== own.identity || current.kind !== own.kind)
      ) {
        this.log('info', `RENEW refused: "${own.identity}" asked to renew "${current.identity}"`);
        this.abortRequest(message, permissionDenied);
        return;
      }
    }
    if (current === undefined) {
      this.abortRequest(message, unauthenticated);
      return;
    }
    // Only the channel's own payload credential is renewed in place: one sent
    // in the request may name a holder key, which the payload cannot prove,
    // or carry other scopes.
    const ownPayload = current === session.payloadCredential;
    const hook = this.renewable;
    if (hook !== undefined) {
      let allowed = false;
      try {
        allowed = hook(current);
      } catch (error) {
        this.log('warn', `renewable failed for "${current.identity}"`, error);
      }
      if (!allowed) {
        this.log('info', `RENEW refused for "${current.identity}"`);
        this.abortRequest(message, permissionDenied);
        return;
      }
    }
    let renewed: Credential;
    try {
      renewed = await issuer.issue({
        kind: current.kind,
        identity: current.identity,
        scopes: current.scopes,
        holderKey: current.holderKey,
        lifetime:
          current.expiresAt === 0 ? null : Math.max(1, current.expiresAt - current.issuedAt) * 1000,
      });
    } catch (error) {
      this.log('warn', `RENEW of "${current.identity}" failed: ${error}`);
      this.abortRequest(message, Status.of(StatusCode.failedPrecondition, 'cannot renew'));
      return;
    }
    if (session.active && ownPayload) {
      // The channel is identified by its payload: the renewal is its identity
      // from now on.
      session.payloadCredential = renewed;
      this.#watchIdentity(session);
    }
    this.log('debug', `renewed the credential of "${renewed.identity}"`);
    this.replyTo(message, renewed.encode());
  }

  // -------------------------------------------------------------------------
  // CONNECT

  #onConnect(session: ServiceSession, message: TalkMessage): void {
    let request: ConnectRequest;
    try {
      request = ConnectRequest.decode(message.payload);
    } catch (error) {
      this.#abortDecode(message, error);
      return;
    }
    if (request.type.isEmpty || request.instance === 0) {
      this.abortRequest(
        message,
        Status.of(StatusCode.invalidArgument, 'CONNECT needs a type and an instance'),
      );
      return;
    }
    if (!this.#permits(session, message, Right.broker, request.type)) {
      return;
    }
    const address = new ServiceAddress(request.type, request.instance);
    const registration = this.#instances.get(request.instance);
    const owner = registration?.owner;
    const record = this.#table.get(address.key);
    if (
      registration === undefined ||
      !registration.address.equals(address) ||
      owner === undefined ||
      !registration.up ||
      record === undefined ||
      record.endpoints.length > 0
    ) {
      this.log(
        'debug',
        `CONNECT ${address}: no such record, or it has endpoints, or it is not registered ` +
          'by a channel',
      );
      this.abortRequest(message, Status.of(StatusCode.notFound, 'not found'));
      return;
    }
    const identity = this.#identityOf(owner)?.identity;
    if (identity === undefined) {
      this.log('info', `CONNECT ${address}: its registration channel has no identity`);
      this.abortRequest(
        message,
        Status.of(StatusCode.failedPrecondition, 'the instance has no identity'),
      );
      return;
    }
    void this.#broker(
      message,
      registration,
      owner,
      new DialBackRequest(this.#identityOf(session)?.identity ?? '', request.endpoint, {
        intent: request.intent,
      }),
      identity,
    );
  }

  /**
   * Asks the instance of [registration] to dial back (`CONNECT` over its
   * registration channel, owned by [owner]), then answers [message] with
   * [identity], or `UNAVAILABLE` if it does not within `brokerTimeout`. Never
   * rejects.
   */
  async #broker(
    message: TalkMessage,
    registration: Registration,
    owner: ServiceSession,
    dialBack: DialBackRequest,
    identity: string,
  ): Promise<void> {
    const address = registration.address;
    let relayed: TalkRequest;
    try {
      relayed = owner.channel.startRequest(Procedures.connect.toString(), dialBack.encode(), {
        name: Procedures.connect,
        timeout: this.brokerTimeout,
      });
    } catch (error) {
      this.log('info', `CONNECT ${address} not relayed: ${error}`);
      this.abortRequest(message, Status.of(StatusCode.unavailable, 'unavailable'));
      return;
    }
    // The consumer's request waits for the instance; it is told the bound,
    // and answered GOING_AWAY if the service closes meanwhile.
    message.setReplyTimeout(0);
    this.#slots.wait(message, this.brokerTimeout > 0 ? this.brokerTimeout : undefined);
    const failure = await new Promise<Status | undefined>((resolve) => {
      let settled = false;
      let bound: TimerHandle | undefined;
      const finish = (status: Status | undefined): void => {
        if (settled) return;
        settled = true;
        registration.onGone.delete(onGone);
        if (bound !== undefined) this.clock.clearTimeout(bound);
        resolve(status);
      };
      const onGone = (): void => {
        relayed.cancel(Status.of(StatusCode.unavailable, 'instance went down'));
        finish(Status.of(StatusCode.unavailable, `${address} went down`));
      };
      registration.onGone.add(onGone);
      if (this.brokerTimeout > 0) {
        bound = this.clock.setTimeout(() => {
          bound = undefined;
          const status = Status.of(
            StatusCode.deadlineExceeded,
            `CONNECT to ${address} took longer than ${this.brokerTimeout} ms`,
          );
          relayed.cancel(status);
          finish(status);
        }, this.brokerTimeout);
      }
      void message.onCancel.then(() => {
        relayed.cancel(Status.of(StatusCode.cancelled, 'cancelled'));
        finish(Status.of(StatusCode.cancelled, 'cancelled by the consumer'));
      });
      relayed.response.then(
        () => finish(undefined),
        (error: unknown) => finish(statusOf(error)),
      );
    });
    if (!message.canReply) {
      this.#slots.unwait(message);
      return;
    }
    if (failure !== undefined) {
      this.log('info', `CONNECT ${address} failed: ${failure}`);
      this.#slots.abort(message, Status.of(StatusCode.unavailable, 'unavailable'));
      return;
    }
    this.log('debug', `CONNECT ${address}: the instance dialled ${dialBack.endpoint}`);
    this.#slots.reply(
      message,
      new ConnectResponse(this.#identityOf(owner)?.identity ?? identity).encode(),
    );
  }

  // -------------------------------------------------------------------------
  // Assignment hold

  /**
   * Keeps a `REGISTER` for any id until the hold ends, declaring the time left
   * in the hold as its deadline with one `EXTEND`. If the requester cancels it
   * meanwhile, it is answered `CANCELLED` and never registered.
   */
  #hold(session: ServiceSession, message: TalkMessage, request: RegisterRequest): void {
    const held: Held = { session, message, request };
    this.#held.push(held);
    this.log('debug', `holding REGISTER ${request.type} until the assignment hold ends`);
    this.#slots.declare(message, this.#holdLeft);
    void message.onCancel.then(() => {
      const index = this.#held.indexOf(held);
      if (index >= 0) {
        this.#held.splice(index, 1);
        this.abortRequest(message, Status.of(StatusCode.cancelled, 'cancelled'));
      }
    });
  }

  /** Answers the held requests, in arrival order. */
  #endHold(): void {
    this.#holdTimer = undefined;
    const held = this.#held.splice(0);
    if (held.length > 0) {
      this.log('info', `assignment hold over, assigning ${held.length} held ids`);
    }
    for (const h of held) {
      if (h.session.active && h.message.canReply && !h.message.isCancelled) {
        this.#register(h.session, h.message, h.request);
      }
    }
    this.#slots.onHoldEnded();
  }

  // -------------------------------------------------------------------------
  // Table

  /** Adds or replaces a registration and publishes `UP` (for a replacement only if the endpoints or the metadata changed). [owner] undefined is a local registration. */
  #add(
    type: Name,
    requested: number,
    endpoints: readonly string[],
    metadata: Uint8Array,
    owner: ServiceSession | undefined,
  ): number {
    check(type, endpoints, metadata);
    let address: ServiceAddress;
    if (requested !== 0) {
      const existing = this.#instances.get(requested);
      if (existing !== undefined) {
        if (existing.owner !== owner || !existing.address.type.equals(type)) {
          throw SwitchboardError.of(
            StatusCode.alreadyExists,
            `instance ${requested.toString(16)} is in use by ${existing.address}`,
          );
        }
        const record = serviceRecord(existing.address, { endpoints, metadata });
        const current = this.#table.get(existing.address.key);
        if (current === undefined || !serviceRecordsEqual(current, record)) {
          this.log('info', `re-registered ${serviceRecordToString(record)}`);
          this.#table.set(existing.address.key, record);
          this.#publish(upEvent(record));
        }
        return requested;
      }
      address = new ServiceAddress(type, requested);
    } else {
      address = new ServiceAddress(type, this.#allocate());
    }
    const record = serviceRecord(address, { endpoints, metadata });
    const registration = new Registration(address, owner);
    this.#instances.set(address.instance, registration);
    owner?.owned.add(registration);
    this.#table.set(address.key, record);
    this.log('info', `registered ${serviceRecordToString(record)}`);
    this.#publish(upEvent(record));
    return address.instance;
  }

  /** The next free instance id: increasing from 1, wrapping at the `u48` limit, skipping ids in use. */
  #allocate(): number {
    if (this.#instances.size >= maxInstance) {
      throw SwitchboardError.of(StatusCode.resourceExhausted, 'no instance ids left');
    }
    let id = this.#nextInstance;
    while (this.#instances.has(id)) {
      id = id >= maxInstance ? 1 : id + 1;
    }
    this.#nextInstance = id >= maxInstance ? 1 : id + 1;
    return id;
  }

  #lookup(type: Name, instance: number): Registration {
    const registration = this.#instances.get(instance);
    if (registration === undefined || !registration.address.type.equals(type)) {
      throw SwitchboardError.of(
        StatusCode.notFound,
        `no registration ${new ServiceAddress(type, instance)}`,
      );
    }
    return registration;
  }

  #remove(registration: Registration): void {
    const address = registration.address;
    this.#instances.delete(address.instance);
    this.#table.delete(address.key);
    registration.owner?.owned.delete(registration);
    registration.removed = true;
    this.log('info', `unregistered ${address}`);
    this.#publish(downEvent(address));
    this.#slots.onDown(registration);
  }

  /**
   * Sends an event about [type] to every channel subscribed to it, once per
   * channel. A channel that no longer takes events loses its subscriptions
   * (it is closing, and dropped when it has closed).
   */
  #publishItem(type: Name, procedure: Name, payload: Uint8Array): void {
    for (const session of [...this.#watching]) {
      if (session.watchesType(type) && !session.send(procedure, payload, this)) {
        this.#endWatches(session);
      }
    }
  }

  #publish(event: ServiceEvent): void {
    if (this.#watching.size > 0) {
      this.#publishItem(
        event.record.address.type,
        serviceEventProcedure(event),
        encodeServiceEvent(event),
      );
    }
    if (!this.#closedSignal.signal.aborted) {
      this.#emitter.emit('event', event);
    }
  }

  /** Forgets a channel: ends its subscriptions and removes its records. Idempotent. */
  #drop(session: ServiceSession): void {
    if (!this.#sessions.delete(session)) {
      return;
    }
    session.active = false;
    if (session.expiryTimer !== undefined) {
      this.clock.clearTimeout(session.expiryTimer);
      session.expiryTimer = undefined;
    }
    // The entry stays while its listener waits.
    if (session.connection !== undefined) {
      this.#connections.get(session.connection)?.delete(session);
    }
    for (let i = this.#held.length - 1; i >= 0; i--) {
      if ((this.#held[i] as Held).session === session) this.#held.splice(i, 1);
    }
    this.#endWatches(session);
    const owned = [...session.owned];
    if (owned.length > 0) {
      this.log('info', `naming client channel closed, removing ${owned.length}`);
    }
    for (const registration of owned) {
      this.#remove(registration);
    }
    session.cancel();
  }

  // -------------------------------------------------------------------------
  // Replies

  /** @internal Replies, logging a reply that cannot go out. */
  replyTo(message: TalkMessage, payload: Uint8Array): void {
    try {
      message.reply(payload).catch(() => {});
    } catch (error) {
      this.log('debug', `reply to ${message.procedureName} failed: ${error}`);
    }
  }

  /** @internal Aborts, logging an abort that cannot go out. */
  abortRequest(message: TalkMessage, status: Status): void {
    try {
      message.replyAbort(status).catch(() => {});
    } catch (error) {
      this.log('debug', `abort of ${message.procedureName} failed: ${error}`);
    }
  }

  #abortDecode(message: TalkMessage, error: unknown): void {
    if (!(error instanceof ProtocolError)) throw error;
    this.abortRequest(message, Status.of(StatusCode.invalidArgument, error.status.reason));
  }
}

const unauthenticated = Status.of(StatusCode.unauthenticated, 'unauthenticated');
const permissionDenied = Status.of(StatusCode.permissionDenied, 'permission denied');
const identityChanged = Status.of(StatusCode.unauthenticated, 'identity changed');

/** The pattern that matches every name. */
const everything = Name.of('*');

/** Whether [credential] grants [right] on every type: a scope with the pattern `*`, or `admin`. */
function grantsEvery(credential: Credential, right: Right): boolean {
  for (const scope of credential.scopes) {
    if (
      scope.right === Right.admin ||
      (scope.right === right && scope.pattern.equals(everything))
    ) {
      return true;
    }
  }
  return false;
}

const rightNames = new Map<number, string>(
  Object.entries(Right).map(([name, value]) => [value, name]),
);

function rightName(right: Right): string {
  return rightNames.get(right) ?? `right ${right}`;
}

/**
 * @throws {SwitchboardError} `INVALID_ARGUMENT` unless a record of [type]
 * with [endpoints] and [metadata] can be stored and published: every watcher
 * must be able to decode what the service accepts.
 */
function check(type: Name, endpoints: readonly string[], metadata: Uint8Array): void {
  if (type.isEmpty) {
    throw SwitchboardError.of(StatusCode.invalidArgument, 'empty service type');
  }
  if (endpoints.length > 255) {
    throw SwitchboardError.of(StatusCode.invalidArgument, 'more than 255 endpoints');
  }
  if (metadata.length > maxMetadataLength) {
    throw SwitchboardError.of(
      StatusCode.invalidArgument,
      `metadata longer than ${maxMetadataLength} bytes`,
    );
  }
  for (const endpoint of endpoints) {
    try {
      checkEndpoint(endpoint);
    } catch (error) {
      throw SwitchboardError.of(
        StatusCode.invalidArgument,
        `invalid endpoint: ${error instanceof Error ? error.message : String(error)}`,
      );
    }
  }
}

/** The key of a subscription filter: the type's key, or `*` for every type. */
function watchKey(type: Name | undefined): string {
  return type === undefined ? '*' : type.key;
}

/** A `REGISTER` for any id waiting for the assignment hold to end. */
interface Held {
  readonly session: ServiceSession;
  readonly message: TalkMessage;
  readonly request: RegisterRequest;
}

/** @internal A record in the table and the channel that owns it (undefined: local). */
export class Registration {
  readonly address: ServiceAddress;
  readonly owner: ServiceSession | undefined;
  /** Share weight in the slot space of its type, from `SLOTS`: 0 takes no slots, undefined until the instance sent `SLOTS`. */
  capacity: number | undefined;
  /** Removed from the table. */
  removed = false;
  /** Called once when the registration is removed. */
  readonly onGone = new Set<() => void>();

  constructor(address: ServiceAddress, owner: ServiceSession | undefined) {
    this.address = address;
    this.owner = owner;
  }

  /** `capacity`, 0 when not declared. */
  get weight(): number {
    return this.capacity ?? 0;
  }

  get instance(): number {
    return this.address.instance;
  }

  /** Registered on a channel that is still served. */
  get up(): boolean {
    return !this.removed && (this.owner?.active ?? false);
  }
}

/** @internal One served channel. */
export class ServiceSession {
  readonly channel: TalkChannel;
  /** The connection the channel arrived on, whose peer identity identifies it. */
  readonly connection: NamingConnection | undefined;
  readonly iterator: AsyncIterator<TalkMessage>;
  /** The check of the open payload's credential, while it runs. */
  verifying: Promise<void> | undefined;
  /** The verified bearer credential of the channel's open payload. */
  payloadCredential: Credential | undefined;
  /** Whether the channel was identified once: losing the identity then ends it. */
  wasIdentified = false;
  /** A connection identity has taken precedence over any OPEN credential. */
  byConnection = false;
  /** The credential that identified the channel when last looked at. */
  credential: Credential | undefined;
  /** Ends the channel when its identity expires. */
  expiryTimer: TimerHandle | undefined;
  readonly owned = new Set<Registration>();
  /** Subscriptions by filter key: the filter (undefined: every type) and the number of `WATCH`es holding it. */
  readonly watches = new Map<string, { type: Name | undefined; count: number }>();
  active = true;

  constructor(
    channel: TalkChannel,
    connection: NamingConnection | undefined,
    iterator: AsyncIterator<TalkMessage>,
  ) {
    this.channel = channel;
    this.connection = connection;
    this.iterator = iterator;
  }

  /** Whether a subscription of this channel covers [type]. */
  watchesType(type: Name): boolean {
    return this.watches.has('*') || this.watches.has(type.key);
  }

  /** Sends one event. Returns false if the channel no longer takes any. */
  send(procedure: Name, payload: Uint8Array, service: NamingService): boolean {
    try {
      // Not awaited: a watcher that does not read stalls its own channel at
      // its window, never the service (what the window does not take waits
      // in the channel).
      this.channel.send(procedure.toString(), payload, { name: procedure }).catch(() => {});
      return true;
    } catch (error) {
      service.log('debug', `${procedure} event not sent: ${error}`);
      return false;
    }
  }

  /** Stops listening to the channel. */
  cancel(): void {
    void this.iterator.return?.()?.catch(() => {});
  }
}
