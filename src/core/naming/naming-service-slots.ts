/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { ServiceAddress } from '../address/index.ts';
import { Right } from '../identity/contract.ts';
import type { Name } from '../name.ts';
import { ProtocolError, Status, StatusCode, SwitchboardError } from '../status.ts';
import type { TalkMessage, TalkRequest } from '../talk/contract.ts';
import type { Clock, TimerHandle } from '../util/clock.ts';
import { Deferred } from '../util/deferred.ts';
import type { LogLevel } from '../util/logger.ts';
import {
  AssignRequest,
  AssignResponse,
  ClaimRequest,
  ClaimResponse,
  DrainRequest,
  ForwardRequest,
  HoldingRequest,
  HoldingResponse,
  LocateRequest,
  LocateResponse,
  MigrateRequest,
  MigrationPhase,
  maxU32,
  PhaseItem,
  Procedures,
  ReleaseRequest,
  ResumeRequest,
  SlotEntry,
  SlotItem,
  SlotMode,
  type SlotSpace,
  SlotState,
  SlotsRequest,
} from './naming-protocol.ts';
import type { NamingService, Registration, ServiceSession } from './naming-service.ts';
import { SlotTable } from './slot-table.ts';

/** At most this many `ASSIGN`s the allocator keeps in flight per space. */
const maxConcurrentAssigns = 16;

/** The epoch after [epoch]; wraps to 1, since 0 is a slot never owned. */
function nextEpoch(epoch: number): number {
  return epoch >= maxU32 ? 1 : epoch + 1;
}

/** @internal The status of an error a request failed with. */
export function statusOf(error: unknown): Status {
  return error instanceof SwitchboardError
    ? error.status
    : Status.of(StatusCode.unknown, String(error));
}

/** The longer of two durations, at least one of them given. */
function longest(a: number | undefined, b: number | undefined): number {
  if (a === undefined) return b as number;
  return b === undefined || a >= b ? a : b;
}

function increment(map: Map<number, number>, key: number): void {
  map.set(key, (map.get(key) ?? 0) + 1);
}

function decrement(map: Map<number, number>, key: number): void {
  const n = (map.get(key) ?? 1) - 1;
  if (n === 0) {
    map.delete(key);
  } else {
    map.set(key, n);
  }
}

/** @internal What the slot manager needs from its naming service. */
export interface SlotHost {
  readonly service: NamingService;
  /** The registrations by instance id. */
  readonly instances: ReadonlyMap<number, Registration>;
  /** Whether [session] may use [right] on [type]; otherwise answers [message] and returns false. */
  permits(session: ServiceSession, message: TalkMessage, right: Right, type: Name): boolean;
  /** Whether [session] may define the slot space of [type]; otherwise answers [message]. */
  permitsSpace(session: ServiceSession, message: TalkMessage, type: Name): boolean;
  /** The time left in the assignment hold, in ms. */
  holdLeft(): number;
  /** Sends an event of [type] to its watchers. */
  publishItem(type: Name, procedure: Name, payload: Uint8Array): void;
  /** Emits the service's `slotEvent`. */
  emitSlotItem(item: SlotItem): void;
}

/**
 * @internal The slot tables of the naming service and everything that
 * changes them: the slot procedures, the hand-over orchestration and the
 * allocator. See the wiki page "Polyverse Switchboard Sharding".
 */
export class SlotManager {
  readonly service: NamingService;
  readonly #host: SlotHost;

  /** Slot spaces by type key. A space is never removed; a redefinition changes it in place. */
  readonly spaces = new Map<string, Space>();
  readonly views = new Map<string, SlotTable>();

  /**
   * Requests waiting for something slow (an `ASSIGN`, the end of the
   * assignment hold, a migration), answered `GOING_AWAY` on {@link close}.
   */
  readonly waiting = new Set<TalkMessage>();

  /** The latest instant (`clock.now()`) declared to each waiting request with `EXTEND`. */
  readonly #declaredUntil = new WeakMap<TalkMessage, number>();

  /** `CLAIM` and `HOLDING` requests made during the assignment hold. */
  readonly heldClaims: HeldClaim[] = [];
  readonly heldHoldings: HeldHolding[] = [];
  #claimSequence = 0;
  readonly #holdOver = new Deferred<void>();

  /** What wakes each request waiting for the end of the assignment hold ({@link #holdOverOrCancel}). */
  readonly #holdWaiters = new Set<Deferred<void>>();
  closed = false;

  /**
   * Deadlines of `LOCATE` requests waiting for a backoff to end, and of
   * `ASSIGN` and `DRAIN` requests (`NamingService.handoverMaxDuration`).
   */
  readonly #deadlines = new Set<TimerHandle>();

  constructor(host: SlotHost) {
    this.#host = host;
    this.service = host.service;
  }

  get holding(): boolean {
    return this.service.isHoldingAssignments;
  }

  get #clock(): Clock {
    return this.service.clock;
  }

  #now(): number {
    return this.service.clock.now();
  }

  #log(level: LogLevel, message: string): void {
    this.service.log(level, message);
  }

  /**
   * The distinct requests in {@link waiting}, waiting for an operation in
   * progress (`Busy.waiters`), or held until the assignment hold ends.
   */
  get waitingCount(): number {
    const all = new Set<TalkMessage>(this.waiting);
    for (const space of this.spaces.values()) {
      for (const busy of space.busy.values()) {
        for (const message of busy.waiters.keys()) all.add(message);
      }
    }
    for (const held of this.heldClaims) all.add(held.message);
    for (const held of this.heldHoldings) all.add(held.message);
    return all.size;
  }

  /** Runs [task] without awaiting it; a failure is logged. */
  #spawn(task: Promise<void>): void {
    task.catch((error: unknown) => this.service.log('error', 'slot operation failed', error));
  }

  /**
   * Resolves when the assignment hold ends, or as soon as [message] is
   * cancelled: nothing then keeps it, or the code waiting for it, until the
   * hold ends.
   */
  #holdOverOrCancel(message: TalkMessage): Promise<void> {
    if (!this.holding || this.#holdOver.isSettled || !message.canReply) {
      return Promise.resolve();
    }
    const waker = new Deferred<void>();
    this.#holdWaiters.add(waker);
    this.#forgetOnCancel(message, () => {
      if (this.#holdWaiters.delete(waker)) waker.resolve();
    });
    return waker.promise;
  }

  #wakeHoldWaiters(): void {
    const wakers = [...this.#holdWaiters];
    this.#holdWaiters.clear();
    for (const waker of wakers) waker.resolve();
  }

  /**
   * Requests held until the assignment hold ends are forgotten as soon as
   * their requester cancels them: [forget] removes [message], which leaves
   * {@link waiting} too.
   */
  #forgetOnCancel(message: TalkMessage, forget: () => void): void {
    void message.onCancel.then(() => {
      forget();
      this.unwait(message);
    });
  }

  // -------------------------------------------------------------------------
  // Replies and declared deadlines

  reply(message: TalkMessage, payload: Uint8Array): void {
    this.unwait(message);
    this.service.replyTo(message, payload);
  }

  abort(message: TalkMessage, status: Status): void {
    this.unwait(message);
    this.service.abortRequest(message, status);
  }

  abortCode(message: TalkMessage, code: StatusCode, reason: string): void {
    this.abort(message, Status.of(code, reason));
  }

  /**
   * Tells the requester of [message], with one `EXTEND`, that its answer
   * comes within [deadline] ms and, with `renew`, within `renew` of the last
   * `EXTEND`. Unless `exact`, nothing is sent when an earlier declaration
   * already lasts as long: a request waiting for several things at once is
   * told the latest of their bounds. The service's own responder timeout for
   * [message] is turned off, since the service answers it when the wait
   * ends.
   */
  declare(
    message: TalkMessage,
    deadline: number | undefined,
    options: { renew?: number | undefined; exact?: boolean } = {},
  ): void {
    const renew = options.renew;
    if (!message.canReply || (deadline === undefined && renew === undefined)) {
      return;
    }
    const until = this.#now() + longest(deadline, renew);
    const previous = this.#declaredUntil.get(message);
    if (options.exact !== true && previous !== undefined && previous >= until) {
      return;
    }
    this.#declaredUntil.set(message, until);
    message.setReplyTimeout(0);
    const extension: { deadline?: number; renew?: number } = {};
    if (deadline !== undefined) extension.deadline = deadline;
    if (renew !== undefined) extension.renew = renew;
    const notExtended = (error: unknown): void =>
      this.#log('debug', `${message.procedureName} not extended: ${error}`);
    try {
      message.extend(extension).catch(notExtended);
    } catch (error) {
      notExtended(error);
    }
  }

  /**
   * [message] waits for something slow: it is answered `GOING_AWAY` if the
   * service closes meanwhile, and told [deadline] if given.
   */
  wait(message: TalkMessage, deadline?: number): void {
    if (!message.canReply || this.closed) {
      return;
    }
    this.waiting.add(message);
    if (deadline !== undefined) {
      this.declare(message, deadline);
    }
  }

  unwait(message: TalkMessage): void {
    this.waiting.delete(message);
  }

  /**
   * Joins [message] to [busy] ({@link join}) and resolves when [busy] ends,
   * or as soon as [message] is cancelled or cannot be answered: nothing then
   * keeps it, or the code waiting for it, until [busy] ends.
   */
  waitFor(message: TalkMessage, busy: Busy): Promise<void> {
    this.join(message, busy);
    return busy.waiters.get(message)?.promise ?? Promise.resolve();
  }

  /**
   * [message] waits for [busy] to end: it is told the bound of the step in
   * progress now, and every later one ({@link stepStarted}, {@link relay}).
   * If its requester cancels it meanwhile, it is forgotten at once and woken
   * ({@link waitFor}); the operation goes on for whoever else needs it.
   */
  join(message: TalkMessage, busy: Busy): void {
    if (!message.canReply || this.closed || busy.released) {
      return;
    }
    this.waiting.add(message);
    if (!busy.waiters.has(message)) {
      busy.waiters.set(message, new Deferred<void>());
      void message.onCancel.then(() => {
        busy.leave(message);
        this.unwait(message);
      });
    }
    const until = busy.until;
    if (until !== undefined) {
      const left = until - this.#now();
      this.declare(message, left < 0 ? 0 : left);
    }
  }

  /**
   * The longest a hand-over request to an instance waits for its answer
   * until the instance declares otherwise: `handoverTimeout`, else, for a
   * request [bounded] overall (`ASSIGN`, `DRAIN`), `handoverMaxDuration`;
   * undefined when neither bounds it.
   */
  #stepBound(bounded: boolean): number | undefined {
    if (this.service.handoverTimeout > 0) {
      return this.service.handoverTimeout;
    }
    if (bounded && this.service.handoverMaxDuration > 0) {
      return this.service.handoverMaxDuration;
    }
    return undefined;
  }

  /** [value] lowered to `handoverMaxDuration` for a request [bounded] by it; as it is for another (`FORWARD`). */
  #clampHandover(value: number | undefined, bounded: boolean): number | undefined {
    const bound = this.service.handoverMaxDuration;
    return bounded && value !== undefined && bound > 0 && value > bound ? bound : value;
  }

  /**
   * A hand-over request for [busy] was sent to an instance ([bounded] as in
   * {@link ask}): its waiters are told how long the service waits for the
   * answer.
   */
  stepStarted(busy: Busy, bounded: boolean): void {
    const bound = this.#stepBound(bounded);
    busy.until = bound === undefined ? undefined : this.#now() + bound;
    if (bound !== undefined) {
      this.#tell(busy, bound, undefined);
    }
  }

  /**
   * The instance working on [busy] sent `EXTEND` declaring [deadline] and
   * [renew] (both undefined: it restarted the service's default timeout):
   * passed on to the waiters, the migration request it serves with the same
   * values. Only a request [bounded] by `handoverMaxDuration` has them
   * lowered to it.
   */
  relay(
    busy: Busy,
    deadline: number | undefined,
    renew: number | undefined,
    bounded: boolean,
  ): void {
    let told = this.#clampHandover(deadline, bounded);
    const renewal = this.#clampHandover(renew, bounded);
    if (told === undefined && renewal === undefined) {
      told = this.#stepBound(bounded);
      if (told === undefined) {
        return;
      }
    }
    const until = this.#now() + longest(told, renewal);
    const previous = busy.until;
    if (previous === undefined || until > previous) {
      busy.until = until;
    }
    this.#tell(busy, told, renewal);
  }

  #tell(busy: Busy, deadline: number | undefined, renew: number | undefined): void {
    const owner = busy.owner;
    if (owner !== undefined) {
      this.declare(owner, deadline, { renew, exact: true });
    }
    const most = longest(deadline, renew);
    for (const message of [...busy.waiters.keys()]) {
      if (!message.canReply) {
        busy.leave(message);
      } else if (message !== owner) {
        this.declare(message, most);
      }
    }
  }

  decode<T>(message: TalkMessage, decoder: (bytes: Uint8Array) => T): T | undefined {
    try {
      return decoder(message.payload);
    } catch (error) {
      if (!(error instanceof ProtocolError)) throw error;
      this.abortCode(message, StatusCode.invalidArgument, error.status.reason);
      return undefined;
    }
  }

  /** The space of [type] if [slot] is in it; otherwise answers `NOT_FOUND` and returns undefined. */
  lookup(message: TalkMessage, type: Name, slot: number): Space | undefined {
    const space = this.spaces.get(type.key);
    if (space === undefined) {
      this.abortCode(message, StatusCode.notFound, `no slot space ${type}`);
      return undefined;
    }
    if (slot >= space.space.count) {
      this.abortCode(
        message,
        StatusCode.notFound,
        `no slot ${slot} in ${type} (${space.space.count} slots)`,
      );
      return undefined;
    }
    return space;
  }

  /**
   * The registration of [type] on [session]; undefined if there is none.
   *
   * @throws {SwitchboardError} `FAILED_PRECONDITION` if there are several:
   * the requests from the naming service carry no instance id, so a channel
   * serves the slots of a type for one instance only.
   */
  registrationOf(session: ServiceSession, type: Name): Registration | undefined {
    let found: Registration | undefined;
    for (const registration of session.owned) {
      if (registration.address.type.equals(type)) {
        if (found !== undefined) {
          throw SwitchboardError.of(
            StatusCode.failedPrecondition,
            `several instances of ${type} registered on this channel`,
          );
        }
        found = registration;
      }
    }
    return found;
  }

  /** {@link registrationOf}, answering [message] with its failure: false once answered. */
  #registrationFor(
    session: ServiceSession,
    message: TalkMessage,
    type: Name,
  ): Registration | undefined | false {
    try {
      return this.registrationOf(session, type);
    } catch (error) {
      if (!(error instanceof SwitchboardError)) throw error;
      this.abort(message, error.status);
      return false;
    }
  }

  /** The live registration `type/instance`, or undefined. */
  live(type: Name, instance: number): Registration | undefined {
    const registration = this.#host.instances.get(instance);
    return registration?.address.type.equals(type) === true && registration.up
      ? registration
      : undefined;
  }

  /** Live instances of the space's type with capacity: the allocator's candidates, by ascending instance id. */
  candidates(space: Space): Registration[] {
    const found: Registration[] = [];
    for (const registration of this.#host.instances.values()) {
      if (
        registration.address.type.equals(space.type) &&
        registration.up &&
        registration.weight > 0
      ) {
        found.push(registration);
      }
    }
    return found.sort((a, b) => a.instance - b.instance);
  }

  // -------------------------------------------------------------------------
  // Table

  /** Sets the state of [slot], keeps the allocator's counters, and publishes a `SLOT` event if it changed. */
  setEntry(space: Space, slot: number, entry: SlotEntry): void {
    const old = space.entry(slot);
    if (old.equals(entry)) {
      return;
    }
    if (old.owner !== 0) {
      decrement(space.load, old.owner);
    }
    if (entry.owner !== 0) {
      increment(space.load, entry.owner);
    }
    if (entry.equals(SlotEntry.unassigned)) {
      space.entries.delete(slot);
    } else {
      space.entries.set(slot, entry);
    }
    space.unpark(slot, old.holder);
    space.updateOpen(slot, entry);
    space.wake(slot);
    this.#log('debug', `slot ${space.type}/${slot}: ${entry}`);
    const item = new SlotItem(space.type, slot, entry);
    this.#host.publishItem(space.type, Procedures.slot, item.encode());
    this.#host.emitSlotItem(item);
  }

  publishSpace(space: Space): void {
    this.#host.publishItem(space.type, Procedures.slotSpace, space.space.encode());
  }

  /**
   * `SLOTSPC` and `SLOT` events of the spaces [matches] accepts, for a
   * snapshot: each space before its slots, slots in ascending order, leaving
   * out free slots with holder 0.
   */
  *snapshot(matches: (type: Name) => boolean): Generator<[Name, Uint8Array]> {
    for (const space of this.spaces.values()) {
      if (!matches(space.type)) {
        continue;
      }
      yield [Procedures.slotSpace, space.space.encode()];
      const slots = [...space.entries.keys()].sort((a, b) => a - b);
      for (const slot of slots) {
        const entry = space.entries.get(slot) as SlotEntry;
        if (!entry.isEmpty) {
          yield [Procedures.slot, new SlotItem(space.type, slot, entry).encode()];
        }
      }
    }
  }

  /** Marks [slot] busy with an operation; `Busy.release` ends it. */
  markBusy(space: Space, slot: number): Busy {
    const busy = new Busy();
    space.busy.set(slot, busy);
    busy.onRelease = () => {
      if (space.busy.get(slot) === busy) {
        space.busy.delete(slot);
      }
    };
    return busy;
  }

  // -------------------------------------------------------------------------
  // SLOTS

  onSlots(session: ServiceSession, message: TalkMessage): void {
    const request = this.decode(message, SlotsRequest.decode);
    if (request === undefined) {
      return;
    }
    const definition = request.space;
    const type = definition.type;
    if (type.isEmpty) {
      this.abortCode(message, StatusCode.invalidArgument, 'empty slot space type');
      return;
    }
    if (definition.count === 0) {
      this.abortCode(message, StatusCode.invalidArgument, 'slot count is 0');
      return;
    }
    if (definition.count > this.service.maxSlotCount) {
      this.abortCode(
        message,
        StatusCode.outOfRange,
        `slot count ${definition.count} over ${this.service.maxSlotCount}`,
      );
      return;
    }
    if (!this.#host.permitsSpace(session, message, type)) {
      return;
    }
    const registration = this.#registrationFor(session, message, type);
    if (registration === false) {
      return;
    }
    let space = this.spaces.get(type.key);
    if (space === undefined) {
      space = new Space(definition, this.#clock);
      this.spaces.set(type.key, space);
      this.views.set(type.key, space.view);
      this.#log('info', `slot space defined: ${definition}`);
      this.publishSpace(space);
    } else if (!space.space.equals(definition)) {
      if (!space.isBlank) {
        this.abortCode(
          message,
          StatusCode.failedPrecondition,
          `slot space ${type} is defined as ${space.space}`,
        );
        return;
      }
      this.#log('info', `slot space redefined: ${space.space} -> ${definition}`);
      for (const timer of space.grace.values()) {
        this.#clock.clearTimeout(timer);
      }
      space.define(definition);
      space.wakeAll();
      this.views.set(type.key, space.view);
      this.publishSpace(space);
    }
    if (registration !== undefined) {
      registration.capacity = request.capacity;
      // The holder is back: its slots are looked at again, given back to it
      // if it takes slots, or assigned elsewhere fetching from it.
      const grace = space.grace.get(registration.instance);
      if (grace !== undefined) {
        space.grace.delete(registration.instance);
        this.#clock.clearTimeout(grace);
      }
      const parked = space.parked.get(registration.instance);
      if (parked !== undefined) {
        space.parked.delete(registration.instance);
        for (const slot of parked) space.open.add(slot);
      }
      this.#log('debug', `${registration.address} capacity ${request.capacity} for ${type}`);
      // The candidates changed: LOCATE requests waiting for a backoff look
      // again.
      space.wakeAll();
    }
    this.settle(space, registration !== undefined && request.capacity > 0);
    this.reply(message, new Uint8Array(0));
    this.scheduleBalance(space);
  }

  // -------------------------------------------------------------------------
  // HOLDING

  onHolding(session: ServiceSession, message: TalkMessage): void {
    const request = this.decode(message, HoldingRequest.decode);
    if (request === undefined || !this.#host.permits(session, message, Right.claim, request.type)) {
      return;
    }
    if (!this.#checkHolding(session, message, request)) {
      return;
    }
    this.settle(this.spaces.get(request.type.key) as Space);
    if (this.holding) {
      // Claims made during the hold are resolved first, so that a slot
      // someone was serving goes to its last owner rather than to a holder
      // of older storage.
      const held = new HeldHolding(session, message, request);
      this.heldHoldings.push(held);
      this.wait(message, this.#host.holdLeft());
      this.#forgetOnCancel(message, () => remove(this.heldHoldings, held));
      return;
    }
    this.#processHolding(session, message, request);
  }

  #checkHolding(session: ServiceSession, message: TalkMessage, request: HoldingRequest): boolean {
    if (!this.spaces.has(request.type.key)) {
      this.abortCode(message, StatusCode.notFound, `no slot space ${request.type}`);
      return false;
    }
    const registration = this.#registrationFor(session, message, request.type);
    if (registration === false) {
      return false;
    }
    if (registration === undefined) {
      this.abortCode(
        message,
        StatusCode.failedPrecondition,
        `not registered as ${request.type} on this channel`,
      );
      return false;
    }
    return true;
  }

  #processHolding(session: ServiceSession, message: TalkMessage, request: HoldingRequest): void {
    if (!message.canReply || !session.active) {
      this.unwait(message);
      return;
    }
    if (!this.#checkHolding(session, message, request)) {
      return;
    }
    const space = this.spaces.get(request.type.key) as Space;
    const me = (this.registrationOf(session, request.type) as Registration).instance;
    const discard: number[] = [];
    const seen = new Set<number>();
    const pending: number[] = [];
    for (const slot of request.slots) {
      if (seen.has(slot)) {
        continue;
      }
      seen.add(slot);
      if (slot >= space.space.count) {
        discard.push(slot);
        continue;
      }
      this.#declareHeld(space, slot, me, discard, pending);
    }
    if (pending.length === 0) {
      this.#answerHolding(message, space, me, discard);
      return;
    }
    // Being assigned fresh to another instance: answered once that is
    // decided, since a failed ASSIGN leaves the slot to this instance.
    this.wait(message);
    this.#spawn(this.#holdingAfterAssignments(session, message, space.type, me, discard, pending));
    this.scheduleBalance(space);
  }

  /**
   * Applies a `HOLDING` declaration of [slot] by instance [me]: a free slot
   * without holder gets [me] as holder; one being assigned fresh (holder 0)
   * gets [me] as holder if that `ASSIGN` fails (`Space.declared`), and goes
   * to [pending] when it is assigned to another instance; any other slot
   * held elsewhere goes to [discard].
   */
  #declareHeld(space: Space, slot: number, me: number, discard: number[], pending: number[]): void {
    const entry = space.entry(slot);
    if (entry.holder === me) {
      return;
    }
    if (entry.holder === 0 && entry.isFree) {
      const target = space.assigning.get(slot);
      if (target === undefined) {
        // Nothing recorded (a fresh or restarted naming service): this
        // instance becomes the holder, and gets the slot back first.
        this.setEntry(space, slot, SlotEntry.free({ holder: me, epoch: entry.epoch }));
        return;
      }
      // Decided by that ASSIGN: should it fail, the first instance that
      // declared the slot meanwhile holds it, before anything else (a LOCATE
      // trying the next instance) can assign the slot fresh again.
      if (!space.declared.has(slot)) space.declared.set(slot, me);
      if (target !== me) {
        pending.push(slot);
      }
      return;
    }
    discard.push(slot);
  }

  /**
   * Answers a `HOLDING` once the fresh assignments of [pending] to other
   * instances are decided: a slot whose `ASSIGN` failed is this instance's
   * again (it becomes the holder); one that went through is to discard.
   */
  async #holdingAfterAssignments(
    session: ServiceSession,
    message: TalkMessage,
    type: Name,
    me: number,
    discard: number[],
    pending: number[],
  ): Promise<void> {
    let slots = pending;
    for (;;) {
      const space = this.spaces.get(type.key) as Space;
      const waits: Promise<void>[] = [];
      for (const slot of slots) {
        const busy = space.busy.get(slot);
        if (busy !== undefined) waits.push(this.waitFor(message, busy));
      }
      await Promise.all(waits);
      if (this.closed || !message.canReply || !session.active) {
        this.unwait(message);
        return;
      }
      const next: number[] = [];
      for (const slot of slots) {
        if (slot < space.space.count) {
          this.#declareHeld(space, slot, me, discard, next);
        } else {
          discard.push(slot);
        }
      }
      if (next.length === 0) {
        this.#answerHolding(message, space, me, discard);
        return;
      }
      slots = next;
    }
  }

  #answerHolding(message: TalkMessage, space: Space, me: number, discard: number[]): void {
    if (discard.length > 0) {
      this.#log(
        'info',
        `${new ServiceAddress(space.type, me)} should discard ${discard.length} slots`,
      );
    }
    this.reply(message, new HoldingResponse(discard).encode());
    this.scheduleBalance(space);
  }

  // -------------------------------------------------------------------------
  // CLAIM

  onClaim(session: ServiceSession, message: TalkMessage): void {
    const request = this.decode(message, ClaimRequest.decode);
    if (
      request === undefined ||
      !this.#host.permits(session, message, Right.claim, request.type) ||
      this.lookup(message, request.type, request.slot) === undefined
    ) {
      return;
    }
    const registration = this.#registrationFor(session, message, request.type);
    if (registration === false) {
      return;
    }
    if (registration === undefined) {
      this.abortCode(
        message,
        StatusCode.failedPrecondition,
        `not registered as ${request.type} on this channel`,
      );
      return;
    }
    this.settle(this.spaces.get(request.type.key) as Space);
    if (this.holding) {
      const held = new HeldClaim(session, message, request, this.#claimSequence++);
      this.heldClaims.push(held);
      this.wait(message, this.#host.holdLeft());
      this.#forgetOnCancel(message, () => remove(this.heldClaims, held));
      return;
    }
    this.#spawn(this.#claim(session, message, request));
  }

  async #claim(
    session: ServiceSession,
    message: TalkMessage,
    request: ClaimRequest,
  ): Promise<void> {
    this.wait(message);
    const type = request.type;
    const slot = request.slot;
    for (;;) {
      if (!message.canReply || !session.active) {
        this.unwait(message);
        return;
      }
      if (this.closed) {
        this.abortCode(message, StatusCode.goingAway, 'naming service closed');
        return;
      }
      const space = this.lookup(message, type, slot);
      if (space === undefined) {
        return;
      }
      const claimant = this.#registrationFor(session, message, type);
      if (claimant === false) {
        return;
      }
      if (claimant === undefined) {
        this.abortCode(
          message,
          StatusCode.failedPrecondition,
          `not registered as ${type} on this channel`,
        );
        return;
      }
      const entry = space.entry(slot);
      if (entry.state === SlotState.migrating) {
        this.abortCode(message, StatusCode.failedPrecondition, `slot ${type}/${slot} is migrating`);
        return;
      }
      if (entry.state === SlotState.owned) {
        if (entry.owner === claimant.instance) {
          this.reply(message, new ClaimResponse(entry.epoch).encode());
        } else {
          this.abortCode(
            message,
            StatusCode.alreadyExists,
            `slot ${type}/${slot} is owned by ${new ServiceAddress(type, entry.owner)}`,
          );
        }
        return;
      }
      const busy = space.busy.get(slot);
      if (busy !== undefined) {
        await this.waitFor(message, busy);
        continue;
      }
      const holder = entry.holder !== 0 ? entry.holder : request.holding ? claimant.instance : 0;
      const failure = await this.assign(space, slot, claimant, {
        holder,
        minEpoch: request.epoch,
        waiter: message,
      });
      if (failure !== undefined) {
        this.abort(message, failure);
        return;
      }
    }
  }

  // -------------------------------------------------------------------------
  // RELEASE

  onRelease(session: ServiceSession, message: TalkMessage): void {
    const request = this.decode(message, ReleaseRequest.decode);
    if (
      request === undefined ||
      !this.#host.permits(session, message, Right.claim, request.type) ||
      this.lookup(message, request.type, request.slot) === undefined
    ) {
      return;
    }
    this.#spawn(this.#release(session, message, request));
  }

  async #release(
    session: ServiceSession,
    message: TalkMessage,
    request: ReleaseRequest,
  ): Promise<void> {
    const type = request.type;
    const slot = request.slot;
    for (;;) {
      if (!message.canReply || !session.active) {
        this.unwait(message);
        return;
      }
      if (this.closed) {
        this.abortCode(message, StatusCode.goingAway, 'naming service closed');
        return;
      }
      const space = this.lookup(message, type, slot);
      if (space === undefined) {
        return;
      }
      const caller = this.#registrationFor(session, message, type);
      if (caller === false) {
        return;
      }
      const entry = space.entry(slot);
      if (entry.state === SlotState.migrating) {
        this.abortCode(message, StatusCode.failedPrecondition, `slot ${type}/${slot} is migrating`);
        return;
      }
      if (
        entry.state === SlotState.owned &&
        caller !== undefined &&
        entry.owner === caller.instance
      ) {
        this.setEntry(
          space,
          slot,
          SlotEntry.free({ holder: request.keepStorage ? entry.holder : 0, epoch: entry.epoch }),
        );
        this.#log('info', `${caller.address} released ${type}/${slot}`);
        this.reply(message, new Uint8Array(0));
        this.scheduleBalance(space);
        return;
      }
      const busy = space.busy.get(slot);
      if (
        entry.isFree &&
        busy !== undefined &&
        caller !== undefined &&
        space.assigning.get(slot) === caller.instance
      ) {
        // Being assigned to the caller: release once that is decided.
        await this.waitFor(message, busy);
        continue;
      }
      this.abortCode(
        message,
        StatusCode.permissionDenied,
        `slot ${type}/${slot} is not owned by this instance`,
      );
      return;
    }
  }

  // -------------------------------------------------------------------------
  // LOCATE

  onLocate(session: ServiceSession, message: TalkMessage): void {
    const request = this.decode(message, LocateRequest.decode);
    if (
      request === undefined ||
      !this.#host.permits(session, message, Right.watch, request.type) ||
      this.lookup(message, request.type, request.slot) === undefined
    ) {
      return;
    }
    this.#spawn(this.#locate(message, request));
  }

  /**
   * Answers `LOCATE`, assigning a free slot of a managed space first. An
   * instance that refuses the `ASSIGN` is in its backoff for the slot, so
   * the next one is tried; once every instance that could take the slot is
   * in its backoff, the request waits for the first backoff to end, at most
   * `handoverTimeout` in all (unbounded if that is zero), and then fails
   * with `UNAVAILABLE`.
   */
  async #locate(message: TalkMessage, request: LocateRequest): Promise<void> {
    const type = request.type;
    const slot = request.slot;
    // Started when the request first has to wait for a backoff.
    let deadline: Deferred<void> | undefined;
    let deadlineTimer: TimerHandle | undefined;
    let deadlineAt: number | undefined;
    let failure: Status | undefined;
    try {
      for (;;) {
        if (!message.canReply) {
          this.unwait(message);
          return;
        }
        if (this.closed) {
          this.abortCode(message, StatusCode.goingAway, 'naming service closed');
          return;
        }
        const space = this.lookup(message, type, slot);
        if (space === undefined) {
          return;
        }
        const entry = space.entry(slot);
        if (!entry.isFree || space.space.mode === SlotMode.static) {
          this.reply(message, new LocateResponse(entry.state, entry.owner, entry.epoch).encode());
          return;
        }
        const busy = space.busy.get(slot);
        if (busy !== undefined) {
          await this.waitFor(message, busy);
          continue;
        }
        if (this.holding) {
          this.wait(message, this.#host.holdLeft());
          await this.#holdOverOrCancel(message);
          continue;
        }
        if (deadline?.isSettled === true) {
          this.abort(
            message,
            failure ??
              Status.of(StatusCode.unavailable, `no instance of ${type} took slot ${slot}`),
          );
          return;
        }
        const candidates = this.candidates(space);
        const target = this.targetFor(space, slot, entry, candidates, true);
        if (target === undefined) {
          if (candidates.length === 0 || this.#waitsForHolder(space, entry)) {
            this.reply(message, new LocateResponse(SlotState.free, 0, entry.epoch).encode());
            return;
          }
          // Every instance that could take the slot is in its backoff for it:
          // wait for the first backoff to end, or for anything else to change
          // the slot or the candidates.
          if (deadline === undefined) {
            const started = new Deferred<void>();
            deadline = started;
            const timeout = this.service.handoverTimeout;
            if (timeout > 0) {
              const timer = this.#clock.setTimeout(() => {
                this.#deadlines.delete(timer);
                started.resolve();
              }, timeout);
              this.#deadlines.add(timer);
              deadlineTimer = timer;
              deadlineAt = this.#now() + timeout;
            }
          }
          // The wait it knows: until it gives up, or, without that bound,
          // until a backoff ends at the latest.
          if (deadlineAt !== undefined) {
            const left = deadlineAt - this.#now();
            this.wait(message, left < 0 ? 0 : left);
          } else {
            this.wait(message, this.service.assignBackoffMax);
          }
          await Promise.race([space.changeOf(slot), deadline.promise, message.onCancel]);
          continue;
        }
        const result = await this.assign(space, slot, target, {
          holder: entry.holder,
          waiter: message,
        });
        if (result !== undefined) {
          // The target is in its backoff now; try the next one.
          failure = Status.of(StatusCode.unavailable, result.reason);
        }
      }
    } finally {
      if (deadlineTimer !== undefined) {
        this.#clock.clearTimeout(deadlineTimer);
        this.#deadlines.delete(deadlineTimer);
      }
    }
  }

  // -------------------------------------------------------------------------
  // MIGRATE

  onMigrate(session: ServiceSession, message: TalkMessage): void {
    if (!message.expectsStream) {
      this.abortCode(message, StatusCode.invalidArgument, 'MIGRATE is a stream request');
      return;
    }
    const request = this.decode(message, MigrateRequest.decode);
    if (
      request === undefined ||
      !this.#host.permits(session, message, Right.migrate, request.type)
    ) {
      return;
    }
    const space = this.lookup(message, request.type, request.slot);
    if (space === undefined) {
      return;
    }
    const problem = this.migrationProblem(space, request.slot, request.to);
    if (problem !== undefined) {
      this.abortCode(message, StatusCode.failedPrecondition, problem);
      return;
    }
    const migration = new Migration(request.slot, request.to, message);
    // Migrations can take long: the service answers when it ends, and tells
    // the requester how long each wait may last: the hold, the migration
    // ahead of it, then each step of its own.
    message.setReplyTimeout(0);
    const ahead = space.active?.busy;
    if (this.holding) {
      this.wait(message, this.#host.holdLeft());
    } else if (ahead !== undefined) {
      this.join(message, ahead);
    } else {
      this.wait(message);
    }
    space.queue.push(migration);
    void message.onCancel.then(() => {
      this.unwait(message);
      remove(space.queue, migration);
    });
    this.pumpMigrations(space);
  }

  /** Why [slot] cannot be migrated to [to] now, or undefined. */
  migrationProblem(space: Space, slot: number, to: number): string | undefined {
    const type = space.type;
    const entry = space.entry(slot);
    if (
      entry.state === SlotState.migrating ||
      space.active?.slot === slot ||
      space.queue.some((m) => m.slot === slot)
    ) {
      return `slot ${type}/${slot} is already migrating`;
    }
    if (entry.state !== SlotState.owned) {
      return `slot ${type}/${slot} is not owned`;
    }
    if (space.busy.has(slot)) {
      return `slot ${type}/${slot} is busy`;
    }
    if (to === 0) {
      const target = this.pickTarget(space, this.candidates(space), { exclude: entry.owner, slot });
      if (target === undefined) {
        return `no other instance of ${type} can take slot ${slot} now`;
      }
      return undefined;
    }
    if (to === entry.owner) {
      return `slot ${type}/${slot} is already owned by ${new ServiceAddress(type, to)}`;
    }
    if (this.live(type, to) === undefined) {
      return `${new ServiceAddress(type, to)} is not registered`;
    }
    return undefined;
  }

  /** Starts the next queued migration of [space], if none is running. */
  pumpMigrations(space: Space): void {
    if (this.closed || this.holding || space.active !== undefined) {
      return;
    }
    for (;;) {
      const migration = space.queue.shift();
      if (migration === undefined) {
        return;
      }
      const request = migration.request;
      if (request !== undefined && !request.canReply) {
        this.unwait(request);
        continue;
      }
      const problem = this.migrationProblem(space, migration.slot, migration.to);
      if (problem !== undefined) {
        if (request !== undefined) {
          this.abortCode(request, StatusCode.failedPrecondition, problem);
        }
        continue;
      }
      space.active = migration;
      this.#spawn(this.#migrate(space, migration));
      // The migrations queued behind it wait for its steps.
      const busy = migration.busy;
      if (busy !== undefined) {
        for (const queued of space.queue) {
          if (queued.request !== undefined) {
            this.join(queued.request, busy);
          }
        }
      }
      return;
    }
  }

  #phase(migration: Migration, phase: MigrationPhase): void {
    const message = migration.request;
    if (message === undefined || !message.canReply) {
      return;
    }
    const notSent = (error: unknown): void => this.#log('debug', `PHASE not sent: ${error}`);
    try {
      message
        .replyItem(new PhaseItem(phase).encode(), { procedure: Procedures.phase })
        .catch(notSent);
    } catch (error) {
      notSent(error);
    }
  }

  /** The hand-over, as in the wiki section "The hand-over, step by step". */
  async #migrate(space: Space, migration: Migration): Promise<void> {
    const type = space.type;
    const slot = migration.slot;
    const start = space.entry(slot);
    const epoch = start.epoch;
    const next = nextEpoch(epoch);
    const holder = start.holder;
    const from = this.#host.instances.get(start.owner) as Registration;
    const to = (
      migration.to !== 0
        ? this.live(type, migration.to)
        : this.pickTarget(space, this.candidates(space), { exclude: start.owner, slot })
    ) as Registration;
    // Synchronous up to the first await, so pumpMigrations sees the busy
    // marker right after starting the migration.
    const busy = this.markBusy(space, slot);
    migration.busy = busy;
    busy.owner = migration.request;
    const what = `${type}/${slot} from ${from.address} to ${to.address}`;
    try {
      this.#log('info', `migrating ${what}`);
      this.setEntry(
        space,
        slot,
        SlotEntry.migrating(from.instance, to.instance, { holder, epoch }),
      );
      this.#phase(migration, MigrationPhase.draining);
      let failure: Status | undefined;
      try {
        await this.ask(
          from,
          Procedures.drain,
          new DrainRequest(type, slot, { epoch: next, to: to.instance }).encode(),
          { bounded: true, busy },
        );
      } catch (error) {
        failure = statusOf(error);
      }
      if (this.closed) {
        this.#abandon(space, migration, from, holder, epoch);
        return;
      }
      if (!from.up) {
        this.#lost(space, slot, holder, epoch);
        this.#rollBack(migration, `${what}: ${from.address} went down`);
        return;
      }
      if (failure !== undefined) {
        // Not moved away from it again by the allocator before the backoff
        // ends.
        this.drainFailed(space, slot, from);
        this.#resume(space, slot, from, epoch, holder);
        this.#rollBack(migration, `${what}: DRAIN failed: ${failure}`);
        return;
      }
      this.drainSucceeded(space, slot, from.instance);
      this.#phase(migration, MigrationPhase.assigning);
      let response: TalkMessage | undefined;
      try {
        response = await this.ask(
          to,
          Procedures.assign,
          new AssignRequest(type, slot, {
            epoch: next,
            holder,
            shared: space.space.shared,
          }).encode(),
          { bounded: true, busy },
        );
      } catch (error) {
        failure = statusOf(error);
      }
      if (this.closed) {
        this.#abandon(space, migration, from, holder, epoch);
        return;
      }
      if (response === undefined || !to.up) {
        const reason =
          response === undefined ? `ASSIGN failed: ${failure}` : `${to.address} went down`;
        if (response === undefined) {
          // Not migrated to it again before its backoff ends.
          this.assignFailed(space, slot, to);
        }
        if (!from.up) {
          this.#lost(space, slot, holder, epoch);
        } else {
          this.#resume(space, slot, from, epoch, holder);
        }
        this.#rollBack(migration, `${what}: ${reason}`);
        return;
      }
      const notHolding = AssignResponse.decode(response.payload).notHolding;
      this.assignSucceeded(space, slot, to.instance);
      this.setEntry(
        space,
        slot,
        SlotEntry.owned(to.instance, { holder: notHolding ? holder : to.instance, epoch: next }),
      );
      if (from.up) {
        this.#phase(migration, MigrationPhase.forwarding);
        try {
          await this.ask(
            from,
            Procedures.forward,
            new ForwardRequest(type, slot, { epoch: next, to: to.instance }).encode(),
            { busy },
          );
        } catch (error) {
          // The new owner serves already; the old one is either gone or will
          // answer MOVED.
          this.#log('warn', `FORWARD of ${what} failed: ${statusOf(error)}`);
        }
      }
      this.#log('info', `migrated ${what}, epoch ${next}`);
      this.#phase(migration, MigrationPhase.done);
      const request = migration.request;
      if (request !== undefined) {
        this.reply(request, new Uint8Array(0));
      }
    } finally {
      busy.release();
      space.active = undefined;
      const request = migration.request;
      if (request !== undefined) {
        this.unwait(request);
      }
      this.pumpMigrations(space);
      this.scheduleBalance(space);
    }
  }

  /** The old owner went down during a migration: the slot is free with its holder kept, as for any slot of an instance that went down. */
  #lost(space: Space, slot: number, holder: number, epoch: number): void {
    this.setEntry(space, slot, SlotEntry.free({ holder, epoch }));
  }

  /**
   * Gives the slot back to [from] and tells it to unlock with `RESUME`.
   *
   * Nothing else unlocks the slot while the channel of [from] lives, and
   * `RESUME` is idempotent at the instance: one that fails (refused, or not
   * answered in time) is sent again once a backoff per slot and owner ends
   * (`assignBackoff`, doubling up to `assignBackoffMax`), as long as the slot
   * stays owned by [from] with [epoch] and no other operation starts on it.
   * After `resumeAttempts` failures in a row the slot is set free with its
   * holder kept, and [from] enters its `ASSIGN` backoff for it: the
   * allocator places it elsewhere first.
   */
  #resume(space: Space, slot: number, from: Registration, epoch: number, holder: number): void {
    this.setEntry(space, slot, SlotEntry.owned(from.instance, { holder, epoch }));
    const previous = space.resumes.get(slot);
    if (previous !== undefined) {
      space.resumes.delete(slot);
      if (previous.timer !== undefined) this.#clock.clearTimeout(previous.timer);
    }
    const resume = new Resume(from, epoch);
    space.resumes.set(slot, resume);
    this.#sendResume(space, slot, resume);
  }

  #sendResume(space: Space, slot: number, resume: Resume): void {
    this.ask(
      resume.owner,
      Procedures.resume,
      new ResumeRequest(space.type, slot, { epoch: resume.epoch }).encode(),
    ).then(
      () => this.#endResume(space, slot, resume),
      (error: unknown) => this.#resumeFailed(space, slot, resume, error),
    );
  }

  #resumeFailed(space: Space, slot: number, resume: Resume, error: unknown): void {
    const what = `RESUME ${space.type}/${slot} to ${resume.owner.address} failed: ${statusOf(error)}`;
    if (!this.#resuming(space, slot, resume)) {
      this.#log('debug', what);
      this.#endResume(space, slot, resume);
      return;
    }
    resume.failures++;
    if (resume.failures >= this.service.resumeAttempts) {
      this.#endResume(space, slot, resume);
      this.#log('warn', `${what} (${resume.failures} times); the slot is free, holder kept`);
      this.assignFailed(space, slot, resume.owner);
      const entry = space.entry(slot);
      this.setEntry(space, slot, SlotEntry.free({ holder: entry.holder, epoch: entry.epoch }));
      this.scheduleBalance(space);
      return;
    }
    const previous = resume.delay;
    const doubled = previous === undefined ? this.service.assignBackoff : previous * 2;
    const delay = Math.min(doubled, this.service.assignBackoffMax);
    resume.delay = delay;
    this.#log('warn', `${what}; sent again in ${delay} ms`);
    resume.timer = this.#clock.setTimeout(() => {
      resume.timer = undefined;
      if (this.#resuming(space, slot, resume)) {
        this.#sendResume(space, slot, resume);
      } else {
        this.#endResume(space, slot, resume);
      }
    }, delay);
  }

  /** Whether [resume] is still to be sent: the latest `RESUME` of [slot], whose owner still owns it with that epoch, with no operation on it. */
  #resuming(space: Space, slot: number, resume: Resume): boolean {
    const entry = space.entry(slot);
    return (
      !this.closed &&
      space.resumes.get(slot) === resume &&
      resume.owner.up &&
      entry.state === SlotState.owned &&
      entry.owner === resume.owner.instance &&
      entry.epoch === resume.epoch &&
      !space.busy.has(slot)
    );
  }

  #endResume(space: Space, slot: number, resume: Resume): void {
    if (resume.timer !== undefined) this.#clock.clearTimeout(resume.timer);
    resume.timer = undefined;
    if (space.resumes.get(slot) === resume) {
      space.resumes.delete(slot);
    }
  }

  #rollBack(migration: Migration, reason: string): void {
    this.#log('warn', `migration rolled back: ${reason}`);
    this.#phase(migration, MigrationPhase.rolledBack);
    const request = migration.request;
    if (request !== undefined) {
      this.abortCode(request, StatusCode.unavailable, reason);
    }
  }

  /**
   * The service closed during the migration: the slot is set back to [from],
   * or free with its holder kept once [from] is gone (as its other slots are
   * by then), as on a rollback, so that the table matches what the requester
   * is told. Nothing is sent: instances unlock their slots when they lose
   * the channel.
   */
  #abandon(
    space: Space,
    migration: Migration,
    from: Registration,
    holder: number,
    epoch: number,
  ): void {
    this.setEntry(
      space,
      migration.slot,
      from.up
        ? SlotEntry.owned(from.instance, { holder, epoch })
        : SlotEntry.free({ holder, epoch }),
    );
    const request = migration.request;
    if (request !== undefined) {
      this.abortCode(request, StatusCode.goingAway, 'naming service closed');
    }
  }

  // -------------------------------------------------------------------------
  // Requests to instances

  /**
   * Sends a request to [target] over its registration channel. Rejects with
   * the instance's abort, with `DEADLINE_EXCEEDED` when the requester
   * timeout expires (`handoverTimeout`, or what the instance declared with
   * `EXTEND`), and with `UNAVAILABLE` as soon as the registration goes away.
   * With `bounded` (`ASSIGN` and `DRAIN`) it also rejects with
   * `DEADLINE_EXCEEDED`, and the request is cancelled, once it has run for
   * `handoverMaxDuration`, whatever the instance declared. The requests
   * waiting for `busy` are told how long the request may take, when it is
   * sent and whenever the instance declares a deadline.
   */
  ask(
    target: Registration,
    procedure: Name,
    payload: Uint8Array,
    options: { bounded?: boolean; busy?: Busy } = {},
  ): Promise<TalkMessage> {
    const bounded = options.bounded ?? false;
    const busy = options.busy;
    const session = target.owner;
    if (session === undefined || !target.up) {
      return Promise.reject(
        SwitchboardError.of(StatusCode.unavailable, `${target.address} is down`),
      );
    }
    let request: TalkRequest;
    try {
      request = session.channel.startRequest(procedure, payload, {
        timeout: this.service.handoverTimeout,
        onExtend:
          busy === undefined
            ? undefined
            : (extension) => this.relay(busy, extension.deadline, extension.renew, bounded),
      });
    } catch (error) {
      return Promise.reject(error);
    }
    if (busy !== undefined) {
      this.stepStarted(busy, bounded);
    }
    const completer = new Deferred<TalkMessage>();
    const onGone = (): void => {
      if (!completer.isSettled) {
        request.cancel(Status.of(StatusCode.unavailable, 'instance went down'));
        completer.reject(
          SwitchboardError.of(StatusCode.unavailable, `${target.address} went down`),
        );
      }
    };
    target.onGone.add(onGone);
    let deadline: TimerHandle | undefined;
    const bound = this.service.handoverMaxDuration;
    if (bounded && bound > 0) {
      const timer = this.#clock.setTimeout(() => {
        this.#deadlines.delete(timer);
        if (completer.isSettled) {
          return;
        }
        target.onGone.delete(onGone);
        const status = Status.of(
          StatusCode.deadlineExceeded,
          `${procedure} to ${target.address} ran longer than ${bound} ms`,
        );
        this.#log('warn', status.reason);
        request.cancel(status);
        completer.reject(new SwitchboardError(status));
      }, bound);
      deadline = timer;
      this.#deadlines.add(timer);
    }
    const settled = (): void => {
      target.onGone.delete(onGone);
      if (deadline !== undefined) {
        this.#clock.clearTimeout(deadline);
        this.#deadlines.delete(deadline);
      }
    };
    request.response.then(
      (response) => {
        settled();
        completer.resolve(response);
      },
      (error: unknown) => {
        settled();
        completer.reject(error);
      },
    );
    return completer.promise;
  }

  /**
   * Assigns the free [slot] to [target] through `ASSIGN`. Resolves with
   * undefined once [target] owns it, or with the failure; the slot then
   * stays free with its holder unchanged. `waiter`, a request waiting for
   * the outcome, is told how long the `ASSIGN` may take.
   */
  async assign(
    space: Space,
    slot: number,
    target: Registration,
    options: { holder: number; minEpoch?: number; waiter?: TalkMessage },
  ): Promise<Status | undefined> {
    const busy = this.markBusy(space, slot);
    if (options.waiter !== undefined) {
      this.join(options.waiter, busy);
    }
    space.assigning.set(slot, target.instance);
    increment(space.pendingFor, target.instance);
    const type = space.type;
    try {
      const current = space.entry(slot);
      const epoch = nextEpoch(Math.max(current.epoch, options.minEpoch ?? 0));
      const request = new AssignRequest(type, slot, {
        epoch,
        holder: options.holder,
        shared: space.space.shared,
      });
      let response: TalkMessage;
      try {
        response = await this.ask(target, Procedures.assign, request.encode(), {
          bounded: true,
          busy,
        });
      } catch (error) {
        const status = statusOf(error);
        this.#log('info', `${request} to ${target.address} failed: ${status}`);
        this.assignFailed(space, slot, target);
        return Status.of(
          StatusCode.unavailable,
          `ASSIGN ${type}/${slot} to ${target.address} failed: ${status}`,
        );
      }
      if (this.closed) {
        return Status.of(StatusCode.goingAway, 'naming service closed');
      }
      if (!target.up) {
        return Status.of(StatusCode.unavailable, `${target.address} went down during ASSIGN`);
      }
      const notHolding = AssignResponse.decode(response.payload).notHolding;
      this.assignSucceeded(space, slot, target.instance);
      const now = space.entry(slot);
      this.setEntry(
        space,
        slot,
        SlotEntry.owned(target.instance, {
          holder: notHolding ? now.holder : target.instance,
          epoch,
        }),
      );
      this.#log('debug', `${type}/${slot} assigned to ${target.address}, epoch ${epoch}`);
      return undefined;
    } finally {
      space.assigning.delete(slot);
      decrement(space.pendingFor, target.instance);
      const declarer = space.declared.get(slot);
      space.declared.delete(slot);
      const now = space.entry(slot);
      if (
        declarer !== undefined &&
        !this.closed &&
        now.isFree &&
        now.holder === 0 &&
        this.live(type, declarer) !== undefined
      ) {
        // Failed: the instance that declared holding the slot meanwhile is
        // its holder, before the waiters wake.
        this.setEntry(space, slot, SlotEntry.free({ holder: declarer, epoch: now.epoch }));
      }
      busy.release();
      this.scheduleBalance(space);
    }
  }

  // -------------------------------------------------------------------------
  // Allocator

  scheduleBalance(space: Space): void {
    if (space.balanceScheduled || this.closed) {
      return;
    }
    space.balanceScheduled = true;
    queueMicrotask(() => {
      space.balanceScheduled = false;
      this.#balance(space);
    });
  }

  #balance(space: Space): void {
    if (
      this.closed ||
      this.holding ||
      space.space.mode !== SlotMode.managed ||
      this.spaces.get(space.type.key) !== space
    ) {
      return;
    }
    const candidates = this.candidates(space);
    if (candidates.length === 0) {
      return;
    }
    const parked: number[] = [];
    for (const slot of space.open) {
      if (space.assigning.size >= maxConcurrentAssigns) {
        break;
      }
      if (space.busy.has(slot)) {
        continue;
      }
      const entry = space.entry(slot);
      if (entry.holder === 0 && space.settling) {
        // Its holder may be about to declare it (see settle).
        continue;
      }
      const target = this.targetFor(space, slot, entry, candidates);
      if (target !== undefined) {
        this.#spawn(this.assign(space, slot, target, { holder: entry.holder }).then(() => {}));
      } else if (this.#waitsForHolder(space, entry)) {
        parked.push(slot);
      }
    }
    // Slots waiting for their holder are left out of later passes until it
    // registers again or its grace period ends.
    for (const slot of parked) {
      space.park(slot, space.entry(slot).holder);
    }
    this.#rebalance(space, candidates);
  }

  /** A free slot of a holder-only space whose holder is down, or back but without having sent `SLOTS` yet. */
  #waitsForHolder(space: Space, entry: SlotEntry): boolean {
    return (
      !space.space.shared &&
      entry.isFree &&
      entry.holder !== 0 &&
      this.live(space.type, entry.holder)?.capacity === undefined
    );
  }

  /**
   * Where the free [slot] (in state [entry]) goes now, or undefined to leave
   * it free.
   *
   * Its holder, if it is up and has capacity (the slot is reclaimed) and not
   * in its backoff for the slot. A holder that is down, or registered but
   * has not sent `SLOTS` yet, makes a holder-only slot wait for the holder's
   * grace period. Otherwise the candidate with the most spare capacity among
   * those not in their backoff for the slot (fetching the state from the
   * holder, if any); a lazy space assigns slots other than a holder's only
   * on `LOCATE` ([locate]).
   */
  targetFor(
    space: Space,
    slot: number,
    entry: SlotEntry,
    candidates: Registration[],
    locate = false,
  ): Registration | undefined {
    const holder = entry.holder;
    if (holder !== 0) {
      const registration = this.live(space.type, holder);
      if (registration !== undefined && registration.weight > 0) {
        if (!this.backingOff(space, slot, holder)) {
          return registration;
        }
      } else if (this.#waitsForHolder(space, entry)) {
        this.#startGrace(space, holder);
        return undefined;
      }
    }
    if (space.space.lazy && !locate) {
      return undefined;
    }
    return this.pickTarget(space, candidates, { slot });
  }

  /**
   * The candidate with the most spare capacity, counting assignments in
   * flight; ties go to the lower instance id. Leaves out `exclude` and, with
   * a `slot`, the candidates in their backoff for it.
   */
  pickTarget(
    space: Space,
    candidates: Registration[],
    options: { exclude?: number; slot?: number } = {},
  ): Registration | undefined {
    const exclude = options.exclude ?? 0;
    const slot = options.slot;
    let total = 0;
    let assigned = 1;
    for (const c of candidates) {
      total += c.weight;
      assigned += space.loadOf(c.instance);
    }
    let best: Registration | undefined;
    let bestSpare = 0;
    for (const c of candidates) {
      if (
        c.instance === exclude ||
        (slot !== undefined && this.backingOff(space, slot, c.instance))
      ) {
        continue;
      }
      // Spare share, scaled by the total capacity.
      const spare = c.weight * assigned - space.loadOf(c.instance) * total;
      if (best === undefined || spare > bestSpare) {
        best = c;
        bestSpare = spare;
      }
    }
    return best;
  }

  /**
   * Moves one slot to the most under-share candidate when that one is short
   * by at least one slot, from the most over-share candidate that has a slot
   * it can move (not busy, not waiting for the answer to a `RESUME`, and not
   * in the `DRAIN` backoff of that owner), else from the next most
   * over-share one, and so on among the candidates over their share. Runs
   * only when nothing else is in flight in the space, so the allocator's
   * migrations are strictly one at a time. A candidate in its backoff for
   * any slot of the space is not given an owned slot: a failed hand-over
   * costs the old owner a `DRAIN` and a `RESUME`, so the expiry of the
   * backoff paces the attempts; a refused `DRAIN` is paced the same way.
   */
  #rebalance(space: Space, candidates: Registration[]): void {
    if (
      candidates.length < 2 ||
      space.assigning.size > 0 ||
      space.active !== undefined ||
      space.queue.length > 0
    ) {
      return;
    }
    let total = 0;
    let assigned = 0;
    for (const c of candidates) {
      total += c.weight;
      assigned += space.loadOf(c.instance);
    }
    if (assigned === 0) {
      return;
    }
    // deficit = (target - load) * total, target = capacity * assigned / total
    const deficit = (c: Registration): number =>
      c.weight * assigned - space.loadOf(c.instance) * total;
    let under: Registration | undefined;
    for (const c of candidates) {
      if (!space.cooling.has(c.instance) && (under === undefined || deficit(c) > deficit(under))) {
        under = c;
      }
    }
    if (under === undefined || deficit(under) < total) {
      return;
    }
    // Most over-share first, ties to the lower id.
    const overs = candidates
      .filter((c) => c !== under && deficit(c) < 0)
      .sort((a, b) => deficit(a) - deficit(b) || a.instance - b.instance);
    for (const over of overs) {
      let slot: number | undefined;
      for (const [s, entry] of space.entries) {
        if (
          entry.state === SlotState.owned &&
          entry.owner === over.instance &&
          !space.busy.has(s) &&
          !space.resumes.has(s) &&
          !this.drainBackingOff(space, s, over.instance) &&
          (slot === undefined || s < slot)
        ) {
          slot = s;
        }
      }
      if (slot === undefined) {
        continue;
      }
      this.#log(
        'info',
        `rebalancing ${space.type}: slot ${slot} from ${over.address} to ${under.address}`,
      );
      space.queue.push(new Migration(slot, under.instance, undefined));
      this.pumpMigrations(space);
      return;
    }
  }

  // -------------------------------------------------------------------------
  // ASSIGN backoff

  /**
   * Whether [instance] is in its backoff for [slot]: it failed an `ASSIGN` of
   * the slot recently, and the allocator, `LOCATE` and rebalancing do not
   * offer it the slot until the backoff ends.
   */
  backingOff(space: Space, slot: number, instance: number): boolean {
    return space.backoffs.get(instance)?.get(slot)?.timer !== undefined;
  }

  /**
   * [target] failed an `ASSIGN` of [slot]: starts its backoff for the slot,
   * `assignBackoff` the first time, doubled at every further failure up to
   * `assignBackoffMax`. When it ends, the allocator runs and `LOCATE`
   * requests waiting for the slot look again. Nothing is kept for an
   * instance that went down (it starts afresh if it comes back).
   */
  assignFailed(space: Space, slot: number, target: Registration): void {
    const delay = this.#startBackoff(space, space.backoffs, slot, target, {
      onStart: () => increment(space.cooling, target.instance),
      onEnd: () => {
        space.cooled(target.instance);
        space.wake(slot);
      },
    });
    if (delay !== undefined) {
      this.#log(
        'info',
        `${target.address} not offered ${space.type}/${slot} again for ${delay} ms`,
      );
    }
  }

  /**
   * Starts or extends the backoff of [target] for [slot] in [backoffs]:
   * `assignBackoff` the first time, doubled at every further failure up to
   * `assignBackoffMax`. `onStart` runs when no backoff was running, `onEnd`
   * when it ends; then the allocator runs. Returns the delay, or undefined
   * when nothing is kept (the service is closed, or [target] went down).
   */
  #startBackoff(
    space: Space,
    backoffs: Map<number, Map<number, Backoff>>,
    slot: number,
    target: Registration,
    hooks: { onStart?: () => void; onEnd?: () => void } = {},
  ): number | undefined {
    if (this.closed || !target.up) {
      return undefined;
    }
    let bySlot = backoffs.get(target.instance);
    if (bySlot === undefined) {
      bySlot = new Map();
      backoffs.set(target.instance, bySlot);
    }
    let backoff = bySlot.get(slot);
    if (backoff === undefined) {
      backoff = new Backoff();
      bySlot.set(slot, backoff);
    }
    const previous = backoff.delay;
    const doubled = previous === undefined ? this.service.assignBackoff : previous * 2;
    const delay = Math.min(doubled, this.service.assignBackoffMax);
    backoff.delay = delay;
    const running = backoff.timer;
    if (running !== undefined) {
      this.#clock.clearTimeout(running);
    } else {
      hooks.onStart?.();
    }
    const started = backoff;
    started.timer = this.#clock.setTimeout(() => {
      started.timer = undefined;
      hooks.onEnd?.();
      this.scheduleBalance(space);
    }, delay);
    return delay;
  }

  /** Whether the allocator leaves [slot] with its owner [instance] for now: the owner refused or failed a `DRAIN` of it recently. */
  drainBackingOff(space: Space, slot: number, instance: number): boolean {
    return space.drainBackoffs.get(instance)?.get(slot)?.timer !== undefined;
  }

  /**
   * [from] refused or failed a `DRAIN` of [slot]: the allocator does not try
   * to move the slot away from it again until its backoff ends, with the
   * same intervals as the `ASSIGN` backoff.
   */
  drainFailed(space: Space, slot: number, from: Registration): void {
    const delay = this.#startBackoff(space, space.drainBackoffs, slot, from);
    if (delay !== undefined) {
      this.#log(
        'info',
        `${space.type}/${slot} not moved away from ${from.address} again for ${delay} ms`,
      );
    }
  }

  /** [instance] drained [slot]: its `DRAIN` backoff for the slot is over. */
  drainSucceeded(space: Space, slot: number, instance: number): void {
    const bySlot = space.drainBackoffs.get(instance);
    const backoff = bySlot?.get(slot);
    if (bySlot === undefined || backoff === undefined) {
      return;
    }
    bySlot.delete(slot);
    if (bySlot.size === 0) {
      space.drainBackoffs.delete(instance);
    }
    if (backoff.timer !== undefined) this.#clock.clearTimeout(backoff.timer);
  }

  /** [instance] accepted an `ASSIGN` of [slot]: its backoff for the slot is over and starts again from `assignBackoff`. */
  assignSucceeded(space: Space, slot: number, instance: number): void {
    const bySlot = space.backoffs.get(instance);
    const backoff = bySlot?.get(slot);
    if (bySlot === undefined || backoff === undefined) {
      return;
    }
    bySlot.delete(slot);
    if (bySlot.size === 0) {
      space.backoffs.delete(instance);
    }
    if (backoff.timer !== undefined) {
      this.#clock.clearTimeout(backoff.timer);
      space.cooled(instance);
    }
  }

  // -------------------------------------------------------------------------
  // Settling after SLOTS

  /**
   * Opens ([open]) or extends the settle window of [space].
   *
   * After a naming service restart the holders of free slots are only known
   * again from `HOLDING`, which an instance sends after `SLOTS`. If the
   * allocator assigned the free slots without a holder as soon as `SLOTS`
   * gave an instance capacity, a slot whose storage that instance (or
   * another one coming back) is about to declare would go elsewhere or start
   * fresh. So a `SLOTS` with capacity opens a window in which the allocator
   * leaves those slots alone; every `SLOTS`, `HOLDING` or `CLAIM` of the type
   * extends it, so that a burst of returning instances keeps it open until
   * it goes quiet for `holdingSettle`, but at most 5 times `assignmentHold`
   * (at least `holdingSettle`) after it opened. Slots with a known holder
   * still go back to it, and `LOCATE` (an explicit demand) still assigns.
   */
  settle(space: Space, open = false): void {
    const quiet = this.service.holdingSettle;
    if (this.closed || quiet === 0) {
      return;
    }
    if (space.settleQuiet === undefined) {
      // Only eager spaces assign slots without a holder on their own.
      if (!open || space.space.mode !== SlotMode.managed || space.space.lazy) {
        return;
      }
      const atMost = this.service.assignmentHold * 5;
      space.settleBound = this.#clock.setTimeout(
        () => this.#settled(space),
        atMost > quiet ? atMost : quiet,
      );
      this.#log('debug', `${space.type}: waiting for HOLDING before assigning`);
    }
    if (space.settleQuiet !== undefined) this.#clock.clearTimeout(space.settleQuiet);
    space.settleQuiet = this.#clock.setTimeout(() => this.#settled(space), quiet);
  }

  #settled(space: Space): void {
    space.stopSettling();
    this.scheduleBalance(space);
  }

  #startGrace(space: Space, holder: number): void {
    if (this.closed || space.grace.has(holder)) {
      return;
    }
    this.#log(
      'info',
      `waiting ${this.service.holderGrace} ms for holder ` +
        `${new ServiceAddress(space.type, holder)} to return`,
    );
    space.grace.set(
      holder,
      this.#clock.setTimeout(() => {
        space.grace.delete(holder);
        if (this.closed || this.spaces.get(space.type.key) !== space) {
          return;
        }
        if (this.live(space.type, holder)?.capacity === undefined) {
          this.#log(
            'warn',
            `holder ${new ServiceAddress(space.type, holder)} did not return; ` +
              'its slots start fresh',
          );
          const slots = new Set<number>([...(space.parked.get(holder) ?? []), ...space.open]);
          for (const slot of slots) {
            const entry = space.entry(slot);
            if (entry.isFree && entry.holder === holder && !space.busy.has(slot)) {
              this.setEntry(space, slot, SlotEntry.free({ epoch: entry.epoch }));
            }
          }
        }
        this.scheduleBalance(space);
      }, this.service.holderGrace),
    );
  }

  // -------------------------------------------------------------------------
  // Lifecycle

  /** [registration] was removed: its owned slots become free with their holder kept; running requests to it fail at once. */
  onDown(registration: Registration): void {
    for (const callback of [...registration.onGone]) {
      callback();
    }
    registration.onGone.clear();
    const space = this.spaces.get(registration.address.type.key);
    if (space === undefined) {
      return;
    }
    space.dropBackoffs(registration.instance);
    space.wakeAll();
    let freed = 0;
    for (const [slot, entry] of [...space.entries]) {
      // Migrating slots are left to the migration, which sees the loss.
      if (entry.state === SlotState.owned && entry.owner === registration.instance) {
        this.setEntry(space, slot, SlotEntry.free({ holder: entry.holder, epoch: entry.epoch }));
        freed++;
      }
    }
    if (freed > 0) {
      this.#log('info', `${registration.address} went down, ${freed} slots free`);
      if (space.space.mode === SlotMode.managed && !space.space.shared) {
        this.#startGrace(space, registration.instance);
      }
    }
    this.scheduleBalance(space);
  }

  /**
   * The assignment hold is over: resolves the claims made meanwhile (higher
   * epoch first, then the earlier claim), then the `HOLDING` declarations,
   * then lets the allocator and migrations run.
   */
  onHoldEnded(): void {
    if (this.closed) {
      return;
    }
    this.#holdOver.resolve();
    this.#wakeHoldWaiters();
    const claims = [...this.heldClaims].sort(
      (a, b) => b.request.epoch - a.request.epoch || a.sequence - b.sequence,
    );
    this.heldClaims.length = 0;
    if (claims.length > 0) {
      this.#log('info', `assignment hold over, resolving ${claims.length} claims`);
    }
    // In that order: the first claim of a slot marks it busy, and the others
    // wait for it and then see who owns the slot.
    for (const claim of claims) {
      this.#spawn(this.#claim(claim.session, claim.message, claim.request));
    }
    const holdings = [...this.heldHoldings];
    this.heldHoldings.length = 0;
    for (const held of holdings) {
      this.#processHolding(held.session, held.message, held.request);
    }
    for (const space of this.spaces.values()) {
      this.pumpMigrations(space);
      this.scheduleBalance(space);
    }
  }

  close(): void {
    this.closed = true;
    const goingAway = Status.of(StatusCode.goingAway, 'naming service closed');
    for (const held of this.heldClaims) {
      this.abort(held.message, goingAway);
    }
    this.heldClaims.length = 0;
    for (const held of this.heldHoldings) {
      this.abort(held.message, goingAway);
    }
    this.heldHoldings.length = 0;
    for (const timer of this.#deadlines) {
      this.#clock.clearTimeout(timer);
    }
    this.#deadlines.clear();
    for (const space of this.spaces.values()) {
      for (const timer of space.grace.values()) {
        this.#clock.clearTimeout(timer);
      }
      space.grace.clear();
      space.clearBackoffs();
      space.stopSettling();
      space.wakeAll();
      for (const migration of space.queue) {
        if (migration.request !== undefined) {
          this.abort(migration.request, goingAway);
        }
      }
      space.queue.length = 0;
    }
    this.#holdOver.resolve();
    this.#wakeHoldWaiters();
    for (const message of [...this.waiting]) {
      this.abort(message, goingAway);
    }
    this.waiting.clear();
  }
}

/** Removes the first [item] from [list], if there. */
function remove<T>(list: T[], item: T): void {
  const index = list.indexOf(item);
  if (index >= 0) list.splice(index, 1);
}

/**
 * The slot numbers below a bound that are in a set, iterated in ascending
 * order: the allocator's open slots. A bit per slot with a summary bit per
 * word, so that finding the next member skips empty stretches; iteration
 * looks up the next member after each one, so the set may change meanwhile.
 */
class SlotSet implements Iterable<number> {
  readonly #count: number;
  readonly #words: Uint32Array;
  readonly #summary: Uint32Array;
  #size = 0;

  constructor(count: number) {
    this.#count = count;
    const words = Math.ceil(count / 32);
    this.#words = new Uint32Array(words);
    this.#summary = new Uint32Array(Math.ceil(words / 32));
  }

  get size(): number {
    return this.#size;
  }

  has(slot: number): boolean {
    return (
      slot >= 0 &&
      slot < this.#count &&
      (((this.#words[slot >>> 5] as number) >>> (slot & 31)) & 1) === 1
    );
  }

  add(slot: number): void {
    if (slot < 0 || slot >= this.#count || this.has(slot)) {
      return;
    }
    const w = slot >>> 5;
    this.#words[w] = (this.#words[w] as number) | (1 << (slot & 31));
    this.#summary[w >>> 5] = (this.#summary[w >>> 5] as number) | (1 << (w & 31));
    this.#size++;
  }

  delete(slot: number): void {
    if (!this.has(slot)) {
      return;
    }
    const w = slot >>> 5;
    const word = (this.#words[w] as number) & ~(1 << (slot & 31));
    this.#words[w] = word;
    if (word === 0) {
      this.#summary[w >>> 5] = (this.#summary[w >>> 5] as number) & ~(1 << (w & 31));
    }
    this.#size--;
  }

  /** Adds every slot below the bound. */
  fill(): void {
    for (let slot = 0; slot < this.#count; slot++) {
      this.add(slot);
    }
  }

  /** The least member at or above [from], or -1. */
  next(from: number): number {
    if (from >= this.#count) {
      return -1;
    }
    let w = from >>> 5;
    const bits = ((this.#words[w] as number) >>> (from & 31)) << (from & 31);
    if (bits !== 0) {
      return (w << 5) + lowestBit(bits);
    }
    // The next word with a member, through the summary.
    w++;
    let s = w >>> 5;
    if (s >= this.#summary.length) {
      return -1;
    }
    let summary =
      (w & 31) === 0
        ? (this.#summary[s] as number)
        : ((this.#summary[s] as number) >>> (w & 31)) << (w & 31);
    while (summary === 0) {
      s++;
      if (s >= this.#summary.length) {
        return -1;
      }
      summary = this.#summary[s] as number;
    }
    w = (s << 5) + lowestBit(summary);
    return (w << 5) + lowestBit(this.#words[w] as number);
  }

  *[Symbol.iterator](): Iterator<number> {
    for (let slot = this.next(0); slot >= 0; slot = this.next(slot + 1)) {
      yield slot;
    }
  }
}

/** The index of the lowest set bit of [bits], which is not 0. */
function lowestBit(bits: number): number {
  return 31 - Math.clz32(bits & -bits);
}

/** One slot space and the allocator's bookkeeping for it. */
class Space {
  space: SlotSpace;
  readonly #clock: Clock;
  readonly entries = new Map<number, SlotEntry>();
  view: SlotTable;

  /** Slots routed to each instance (owned, or migrating away from it). */
  readonly load = new Map<number, number>();

  /** `ASSIGN`s in flight: slot to target instance, and count per target. */
  readonly assigning = new Map<number, number>();
  readonly pendingFor = new Map<number, number>();

  /**
   * The first instance that declared `HOLDING` of a slot while it was being
   * assigned fresh (holder 0), by slot: its holder if that `ASSIGN` fails.
   */
  readonly declared = new Map<number, number>();

  /** Slots with an operation in progress (an `ASSIGN` or a migration). */
  readonly busy = new Map<number, Busy>();

  /** Free slots the allocator looks at: every free slot of an eager managed space, the free slots with a holder of a lazy one. */
  open: SlotSet;

  active: Migration | undefined;
  readonly queue: Migration[] = [];

  /**
   * Free slots of a holder-only space waiting for their holder to return, by
   * holder: taken out of {@link open} until it does or its grace ends.
   */
  readonly parked = new Map<number, Set<number>>();

  /** Holder grace timers, by holder instance. */
  readonly grace = new Map<number, TimerHandle>();
  balanceScheduled = false;

  /** `ASSIGN` backoffs by instance, then slot (see `SlotManager.assignFailed`), and the number of running ones per instance. */
  readonly backoffs = new Map<number, Map<number, Backoff>>();
  readonly cooling = new Map<number, number>();

  /** `DRAIN` backoffs by owner, then slot (see `SlotManager.drainFailed`). */
  readonly drainBackoffs = new Map<number, Map<number, Backoff>>();

  /** `RESUME`s of rolled-back migrations not answered yet, by slot. */
  readonly resumes = new Map<number, Resume>();

  /** `LOCATE` requests waiting for a slot to change, by slot. */
  readonly #wakers = new Map<number, Deferred<void>>();

  /** The settle window after `SLOTS`: open while {@link settleQuiet} runs; {@link settleBound} ends it at the latest. */
  settleQuiet: TimerHandle | undefined;
  settleBound: TimerHandle | undefined;

  constructor(definition: SlotSpace, clock: Clock) {
    this.space = definition;
    this.#clock = clock;
    this.open = new SlotSet(definition.count);
    this.view = new SlotTable(definition, this.entries);
    this.define(definition);
  }

  get settling(): boolean {
    return this.settleQuiet !== undefined;
  }

  get type(): Name {
    return this.space.type;
  }

  entry(slot: number): SlotEntry {
    return this.entries.get(slot) ?? SlotEntry.unassigned;
  }

  loadOf(instance: number): number {
    return (this.load.get(instance) ?? 0) + (this.pendingFor.get(instance) ?? 0);
  }

  /** True when the space may be redefined: every slot free with holder 0 and nothing in progress. */
  get isBlank(): boolean {
    for (const entry of this.entries.values()) {
      if (!entry.isEmpty) return false;
    }
    return this.busy.size === 0 && this.active === undefined && this.queue.length === 0;
  }

  define(definition: SlotSpace): void {
    this.space = definition;
    this.clearBackoffs();
    this.entries.clear();
    this.declared.clear();
    this.load.clear();
    this.grace.clear();
    this.parked.clear();
    this.open = new SlotSet(definition.count);
    if (definition.mode === SlotMode.managed && !definition.lazy) {
      this.open.fill();
    }
    this.view = new SlotTable(definition, this.entries);
  }

  /** Resolves when {@link wake} is called for [slot] (or {@link wakeAll}). */
  changeOf(slot: number): Promise<void> {
    let waker = this.#wakers.get(slot);
    if (waker === undefined) {
      waker = new Deferred<void>();
      this.#wakers.set(slot, waker);
    }
    return waker.promise;
  }

  wake(slot: number): void {
    const waker = this.#wakers.get(slot);
    if (waker !== undefined) {
      this.#wakers.delete(slot);
      waker.resolve();
    }
  }

  wakeAll(): void {
    const wakers = [...this.#wakers.values()];
    this.#wakers.clear();
    for (const waker of wakers) waker.resolve();
  }

  /** A backoff of [instance] ended. */
  cooled(instance: number): void {
    decrement(this.cooling, instance);
  }

  /** Forgets the backoffs of [instance] (it went down), and the `RESUME`s to send it. */
  dropBackoffs(instance: number): void {
    this.cooling.delete(instance);
    for (const map of [this.backoffs, this.drainBackoffs]) {
      const bySlot = map.get(instance);
      map.delete(instance);
      for (const backoff of bySlot?.values() ?? []) {
        if (backoff.timer !== undefined) this.#clock.clearTimeout(backoff.timer);
      }
    }
    for (const [slot, resume] of [...this.resumes]) {
      if (resume.owner.instance === instance) {
        if (resume.timer !== undefined) this.#clock.clearTimeout(resume.timer);
        this.resumes.delete(slot);
      }
    }
  }

  clearBackoffs(): void {
    const instances = new Set<number>([...this.backoffs.keys(), ...this.drainBackoffs.keys()]);
    for (const resume of this.resumes.values()) instances.add(resume.owner.instance);
    for (const instance of instances) {
      this.dropBackoffs(instance);
    }
  }

  stopSettling(): void {
    if (this.settleQuiet !== undefined) this.#clock.clearTimeout(this.settleQuiet);
    this.settleQuiet = undefined;
    if (this.settleBound !== undefined) this.#clock.clearTimeout(this.settleBound);
    this.settleBound = undefined;
  }

  park(slot: number, holder: number): void {
    this.open.delete(slot);
    let slots = this.parked.get(holder);
    if (slots === undefined) {
      slots = new Set();
      this.parked.set(holder, slots);
    }
    slots.add(slot);
  }

  unpark(slot: number, holder: number): void {
    const slots = this.parked.get(holder);
    if (slots?.delete(slot) === true && slots.size === 0) {
      this.parked.delete(holder);
    }
  }

  updateOpen(slot: number, entry: SlotEntry): void {
    const wanted =
      this.space.mode === SlotMode.managed &&
      entry.isFree &&
      (!this.space.lazy || entry.holder !== 0);
    if (wanted) {
      this.open.add(slot);
    } else {
      this.open.delete(slot);
    }
  }
}

/** @internal An operation in progress on a slot (an `ASSIGN` or a migration), and the requests waiting for it to end. */
export class Busy {
  /** Takes the operation off its slot. */
  onRelease: (() => void) | undefined;

  /** Requests waiting for the operation, told the bound of each step, each with what wakes it when the operation ends. */
  readonly waiters = new Map<TalkMessage, Deferred<void>>();

  /** The `MIGRATE` request the operation serves, told the same values the instances declare. */
  owner: TalkMessage | undefined;

  /** The latest the step in progress may last, as told to the waiters (`clock.now()`); undefined when nothing bounds it. */
  until: number | undefined;

  /** The operation ended. */
  released = false;

  /** Ends the operation and wakes its waiters. */
  release(): void {
    this.onRelease?.();
    this.onRelease = undefined;
    if (this.released) {
      return;
    }
    this.released = true;
    const wakers = [...this.waiters.values()];
    this.waiters.clear();
    for (const waker of wakers) waker.resolve();
  }

  /** Forgets [message] and wakes it, if it waits for the operation. */
  leave(message: TalkMessage): void {
    const waker = this.waiters.get(message);
    if (waker !== undefined) {
      this.waiters.delete(message);
      waker.resolve();
    }
  }
}

/** The `ASSIGN` backoff of one instance for one slot. */
class Backoff {
  /** The length of the last backoff in ms; undefined before the first failure. */
  delay: number | undefined;
  /** Runs while the instance is not offered the slot. */
  timer: TimerHandle | undefined;
}

/** The `RESUME` of a rolled-back migration, sent until the old owner answers it. */
class Resume {
  readonly owner: Registration;
  /** The slot's epoch, unchanged by the rollback. */
  readonly epoch: number;
  /** Failed attempts in a row. */
  failures = 0;
  /** The last backoff in ms; undefined before the first failure. */
  delay: number | undefined;
  /** Runs until the next attempt. */
  timer: TimerHandle | undefined;

  constructor(owner: Registration, epoch: number) {
    this.owner = owner;
    this.epoch = epoch;
  }
}

/** A queued or running migration; [request] is undefined for the allocator's. */
class Migration {
  readonly slot: number;
  /** The target, 0 to let the allocator choose when it starts. */
  readonly to: number;
  readonly request: TalkMessage | undefined;
  /** The operation on the slot, once started. */
  busy: Busy | undefined;

  constructor(slot: number, to: number, request: TalkMessage | undefined) {
    this.slot = slot;
    this.to = to;
    this.request = request;
  }
}

/** A `CLAIM` made during the assignment hold. */
class HeldClaim {
  readonly session: ServiceSession;
  readonly message: TalkMessage;
  readonly request: ClaimRequest;
  readonly sequence: number;

  constructor(
    session: ServiceSession,
    message: TalkMessage,
    request: ClaimRequest,
    sequence: number,
  ) {
    this.session = session;
    this.message = message;
    this.request = request;
    this.sequence = sequence;
  }
}

/** A `HOLDING` made during the assignment hold. */
class HeldHolding {
  readonly session: ServiceSession;
  readonly message: TalkMessage;
  readonly request: HoldingRequest;

  constructor(session: ServiceSession, message: TalkMessage, request: HoldingRequest) {
    this.session = session;
    this.message = message;
    this.request = request;
  }
}
