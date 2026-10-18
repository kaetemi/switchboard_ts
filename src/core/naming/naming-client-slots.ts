/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { Name } from '../name.ts';
import { ProtocolError, Status, StatusCode, SwitchboardError } from '../status.ts';
import type { TalkMessage, TalkStream } from '../talk/contract.ts';
import type { TimerHandle } from '../util/clock.ts';
import { Deferred } from '../util/deferred.ts';
import type { ClientSession, NamingClient } from './naming-client.ts';
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
  PhaseItem,
  Procedures,
  ReleaseRequest,
  ResumeRequest,
  SlotEntry,
  type SlotItem,
  type SlotSpace,
  type SlotsRequest,
} from './naming-protocol.ts';
import { SlotEvent, SlotTable } from './slot-table.ts';

/** What an instance answers to `ASSIGN`. */
export const AssignResult = {
  /** The instance now holds the slot's storage (the usual answer of a stateful service). */
  holding: 'holding',
  /** The instance does not hold the slot's storage; the previous holder is kept. A stateless service always answers this. */
  notHolding: 'notHolding',
} as const;

/** What an instance answers to `ASSIGN`: one of the values of {@link AssignResult}. */
export type AssignResult = (typeof AssignResult)[keyof typeof AssignResult];

/**
 * One request from the naming service to a sharded instance (`ASSIGN`,
 * `DRAIN`, `FORWARD` or `RESUME`), as its {@link SlotHandler} method sees
 * it.
 *
 * The naming service waits for the answer its `handoverTimeout` (60 s by
 * default), unless the handler declares an estimate with {@link extend}.
 * Nothing is sent on the handler's behalf while it runs: a load or a drain
 * that may take longer declares how long, at the steps where it knows (the
 * state transfer started, a phase completed, the estimate changed).
 */
export interface SlotRequestContext {
  /** The slot's type. */
  readonly type: Name;
  /** The slot. */
  readonly slot: number;
  /** The epoch the request carries: for `ASSIGN`, `DRAIN` and `FORWARD` the slot's epoch at its new owner, for `RESUME` its current one. */
  readonly epoch: number;
  /**
   * True once the request is cancelled: the naming service gave up on it
   * (its timeout, its bound, a cancel), the handler ran past its declared
   * deadline or `NamingClient.slotHandlerMaxDuration`, the channel to the
   * naming service ended, or the client closed. Work on it should stop; its
   * outcome is ignored.
   */
  readonly isCancelled: boolean;
  /** Resolves when {@link isCancelled} becomes true; never if the request is answered first. */
  readonly onCancel: Promise<void>;
  /** Aborted when {@link isCancelled} becomes true: pass it to what the handler awaits. */
  readonly signal: AbortSignal;
  /**
   * Declares how long the work may still take, with one `EXTEND` (see
   * `TalkMessage.extend`): the answer comes within `deadline` ms from now
   * and, with `renew`, within `renew` ms of the last declaration. The naming
   * service then waits that long (plus `TalkOptions.extendBuffer`), instead
   * of its default timeout, and passes the estimate on to whoever waits for
   * the hand-over (the `MIGRATE` requester, a `LOCATE`). This instance gives
   * up at the declared time itself: the request is answered
   * `ABORT DEADLINE_EXCEEDED` and {@link onCancel} resolves. A later call
   * replaces the estimate, and may shorten it.
   *
   * The values are lowered to what is left of
   * `NamingClient.slotHandlerMaxDuration`. With neither value, sends an empty
   * `EXTEND`, which restarts the naming service's default timeout. Does
   * nothing (logged) once the request can no longer be answered.
   *
   * @throws {RangeError} for a negative value.
   */
  extend(options?: { deadline?: number; renew?: number }): void;
}

/**
 * Serves the requests the naming service sends to a sharded instance over
 * its registration channel. Set it with `NamingClient.slotHandler`. See the
 * wiki page "Polyverse Switchboard Sharding", section "Requests from the
 * naming service to instances".
 *
 * Each method receives the request's {@link SlotRequestContext}, through
 * which a slow handler declares how long it may take; the naming service
 * waits its `handoverTimeout` otherwise. The client sends nothing on the
 * handler's behalf while it runs. It bounds a handler call to
 * `NamingClient.slotHandlerMaxDuration` (then it answers
 * `ABORT DEADLINE_EXCEEDED` and ignores the handler's late outcome), answers
 * the request when the returned promise resolves, and answers `ABORT` with
 * the status of a thrown {@link SwitchboardError} (`UNAVAILABLE` for any
 * other error).
 */
export interface SlotHandler {
  /**
   * Load or initialise the slot and resolve once ready to serve it.
   *
   * `request.holder` says where the state is: 0 nowhere (start fresh), this
   * instance (reclaim local state), another instance (fetch it from there,
   * or from shared storage when `request.shared`). Throw
   * `SwitchboardError.of(StatusCode.unavailable)` when the slot cannot be
   * served; it then stays free.
   *
   * May arrive for a slot this instance already serves: after a naming
   * service restart, the claim of a slot it was serving is confirmed with an
   * `ASSIGN` carrying the new epoch.
   */
  onAssign(
    request: AssignRequest,
    context: SlotRequestContext,
  ): AssignResult | Promise<AssignResult>;

  /**
   * Lock the slot: queue new work for it, let work in flight complete, and
   * make its state available to `request.to` (transfer it or flush it to
   * shared storage). Resolve once nothing is in flight and the state is
   * flushed. The slot stays locked until {@link onForward} or
   * {@link onResume}.
   */
  onDrain(request: DrainRequest, context: SlotRequestContext): void | Promise<void>;

  /**
   * The new owner serves: forward the queued work to `request.to` and keep
   * forwarding late arrivals for a grace period, then answer `MOVED`.
   * Resolve once the queue has been handed over. The slot is no longer
   * served by this instance.
   */
  onForward(request: ForwardRequest, context: SlotRequestContext): void | Promise<void>;

  /**
   * The migration was abandoned, or the naming service was lost while the
   * slot was locked: unlock and serve the queued work. Must be idempotent: it
   * may come for a slot that was never locked, after a `DRAIN` that failed.
   */
  onResume(request: ResumeRequest, context: SlotRequestContext): void | Promise<void>;

  /**
   * This instance must stop serving [slot] of [type]: its claim was refused
   * after a reconnect (another instance owns it), or an `ASSIGN` it accepted
   * could not be confirmed (or ran longer than
   * `NamingClient.slotHandlerMaxDuration`). Optional.
   */
  onRevoke?(type: Name, slot: number): void | Promise<void>;

  /**
   * A `HOLDING` response listed [slots] of [type] as no longer held by this
   * instance (their holder is another instance now, or they are outside the
   * space): their local storage should be discarded. Called for every
   * `HOLDING` response with a non-empty list, those of
   * `NamingClient.declareHolding` and those the client sends again after a
   * reconnect, before the `declareHolding` promise resolves. Optional.
   */
  onDiscard?(type: Name, slots: readonly number[]): void;
}

/** At most this many `CLAIM`s of served slots are in flight per channel after a reconnect, well below the default Talk request limits (1024). */
const reclaimWindow = 256;

/** @internal What {@link ClientSlots} needs of its {@link NamingClient}. */
export interface SlotsHost {
  session(): ClientSession | undefined;
  closed(): boolean;
  registeredOn(session: ClientSession, type: Name): boolean;
  registers(type: Name): boolean;
  emitSlotEvent(event: SlotEvent): void;
  readonly client: NamingClient;
}

/** @internal A remembered `SLOTS`. */
export class SpaceDefinition {
  readonly request: SlotsRequest;
  readonly completer = new Deferred<void>();

  constructor(request: SlotsRequest) {
    this.request = request;
  }

  complete(): void {
    this.completer.resolve();
  }

  fail(error: unknown): void {
    this.completer.reject(error);
  }

  /** A newer definition of the same type replaces this one: a pending caller learns the newer one's outcome. */
  supersede(newer: SpaceDefinition): void {
    if (!this.completer.isSettled) {
      newer.completer.promise.then(
        () => this.complete(),
        (error: unknown) => this.fail(error),
      );
    }
  }
}

/** @internal A request of the client's slot API waiting for the channel to take it. */
export interface ClientCall {
  readonly procedure: Name;
  readonly payload: Uint8Array;
  readonly response: Deferred<TalkMessage>;
  /** The caller's signal: an abort takes the call out of the queue, and cancels it once sent when {@link cancelsSent}. */
  readonly signal: AbortSignal | undefined;
  /** Whether an abort cancels the request once sent; otherwise it runs to its answer. */
  readonly cancelsSent: boolean;
}

/** The remembered `HOLDING` declarations of one type. */
class Holdings {
  readonly slots = new Set<number>();
  /** Calls of `declareHolding` not sent yet. */
  readonly pending: PendingHolding[] = [];
}

class PendingHolding {
  readonly slots: Set<number>;
  readonly completer = new Deferred<number[]>();

  constructor(slots: Set<number>) {
    this.slots = slots;
  }
}

/** @internal A slot this instance serves. */
export class Served {
  epoch: number;
  /** The instance holds the slot's storage (it answered `ASSIGN` without "not holding"). */
  readonly holding: boolean;

  constructor(epoch: number, holding: boolean) {
    this.epoch = epoch;
    this.holding = holding;
  }
}

/** A slot locked by `DRAIN`. */
interface Draining {
  readonly type: Name;
  readonly slot: number;
  /** The slot's epoch before the migration. */
  readonly epoch: number;
  /** Resolves when the `DRAIN` handler is done. */
  readonly drained: Promise<void>;
}

/** The mirrored slot table of one type. */
class Mirror {
  readonly space: SlotSpace;
  readonly entries = new Map<number, SlotEntry>();
  readonly view: SlotTable;

  constructor(space: SlotSpace) {
    this.space = space;
    this.view = new SlotTable(space, this.entries);
  }
}

const slotKey = (type: Name, slot: number): string => `${type.key}/${slot}`;

const procedures = [Procedures.assign, Procedures.drain, Procedures.forward, Procedures.resume];

/**
 * @internal The slot state of a {@link NamingClient}: what it remembers to
 * restore after a reconnect, what it serves, and the mirrored slot tables.
 */
export class ClientSlots {
  readonly #host: SlotsHost;
  handler: SlotHandler | undefined;

  /** Remembered `SLOTS` per type key, re-sent after every reconnect. */
  readonly definitions = new Map<string, SpaceDefinition>();
  /** Remembered `HOLDING` per type key, re-sent after every reconnect. */
  readonly holdings = new Map<string, Holdings>();
  /** Slots this instance serves (confirmed `ASSIGN`), by type key and slot. */
  readonly served = new Map<string, Map<number, Served>>();
  /** Slots locked by `DRAIN`, until `FORWARD` or `RESUME`, by `typeKey/slot`. */
  readonly draining = new Map<string, Draining>();
  readonly mirrors = new Map<string, Mirror>();
  readonly views = new Map<string, SlotTable>();

  constructor(host: SlotsHost) {
    this.#host = host;
  }

  /** Whether [procedure] is one the naming service sends to a sharded instance. */
  static isSlotProcedure(procedure: Name): boolean {
    return procedures.some((p) => p.equals(procedure));
  }

  get #client(): NamingClient {
    return this.#host.client;
  }

  // -------------------------------------------------------------------------
  // Requests from the naming service

  serve(session: ClientSession, message: TalkMessage): void {
    const handler = this.handler;
    if (handler === undefined) {
      this.#abort(message, Status.of(StatusCode.unimplemented, 'this instance does not shard'));
      return;
    }
    let context: SlotContext;
    try {
      context = SlotContext.of(message, this.#client);
    } catch (error) {
      if (!(error instanceof ProtocolError)) throw error;
      this.#abort(message, Status.of(StatusCode.invalidArgument, error.status.reason));
      return;
    }
    // Loading or draining a slot may take long, as long as the handler
    // declares (context.extend); the naming service waits its own timeout
    // otherwise. No default gap here: this side gives up at a declared
    // deadline, and after slotHandlerMaxDuration in any case.
    const bound = this.#client.slotHandlerMaxDuration;
    message.setReplyTimeout(bound);
    const clock = this.#client.clock;
    let deadline: TimerHandle | undefined;
    if (bound > 0) {
      deadline = clock.setTimeout(() => {
        deadline = undefined;
        if (message.canReply) {
          this.#client.log(
            'warn',
            `${message.procedureName} handler still running after ${bound} ms; answered ` +
              'DEADLINE_EXCEEDED',
          );
          this.#abort(
            message,
            Status.of(StatusCode.deadlineExceeded, `slot handler ran longer than ${bound} ms`),
          );
        }
        context.cancel();
      }, bound);
    }
    const stop = (): void => {
      if (deadline !== undefined) {
        clock.clearTimeout(deadline);
        deadline = undefined;
      }
    };
    // A request that can no longer be answered (the channel closed, the
    // client too) needs no timer.
    void context.onCancel.then(stop);
    void this.#serve(session, message, handler, context).finally(stop);
  }

  async #serve(
    session: ClientSession,
    message: TalkMessage,
    handler: SlotHandler,
    context: SlotContext,
  ): Promise<void> {
    const procedure = message.procedure;
    try {
      if (procedure.equals(Procedures.assign)) {
        await this.#assign(session, message, handler, context);
      } else if (procedure.equals(Procedures.drain)) {
        await this.#drain(message, handler, context);
      } else if (procedure.equals(Procedures.forward)) {
        const request = ForwardRequest.decode(message.payload);
        this.draining.delete(slotKey(request.type, request.slot));
        this.served.get(request.type.key)?.delete(request.slot);
        await handler.onForward(request, context);
        this.#reply(message);
      } else {
        const request = ResumeRequest.decode(message.payload);
        this.draining.delete(slotKey(request.type, request.slot));
        await handler.onResume(request, context);
        this.#reply(message);
      }
    } catch (error) {
      if (error instanceof ProtocolError) {
        this.#abort(message, Status.of(StatusCode.invalidArgument, error.status.reason));
        return;
      }
      this.#client.log('warn', `${message.procedureName} handler failed`, error);
      this.#abort(message, failure(error));
    }
  }

  async #assign(
    session: ClientSession,
    message: TalkMessage,
    handler: SlotHandler,
    context: SlotContext,
  ): Promise<void> {
    const request = AssignRequest.decode(message.payload);
    const type = request.type;
    const slot = request.slot;
    const result = await handler.onAssign(request, context);
    let bySlot = this.served.get(type.key);
    if (bySlot === undefined) {
      bySlot = new Map();
      this.served.set(type.key, bySlot);
    }
    const before = bySlot.get(slot);
    // Recorded before the reply, so that the slot is served by the time the
    // naming service publishes the new owner.
    bySlot.set(slot, new Served(request.epoch, result === AssignResult.holding));
    let confirmed = message.canReply && session.usable;
    if (confirmed) {
      try {
        message
          .reply(new AssignResponse({ notHolding: result === AssignResult.notHolding }).encode())
          .catch(() => {});
      } catch {
        confirmed = false;
      }
    }
    if (!confirmed) {
      if (before !== undefined) {
        bySlot.set(slot, before);
        return;
      }
      bySlot.delete(slot);
      this.#client.log('info', `ASSIGN ${type}/${slot} not confirmed, revoking it`);
      this.revoke(type, slot);
    }
  }

  async #drain(message: TalkMessage, handler: SlotHandler, context: SlotContext): Promise<void> {
    const request = DrainRequest.decode(message.payload);
    const drained = new Deferred<void>();
    this.draining.set(slotKey(request.type, request.slot), {
      type: request.type,
      slot: request.slot,
      epoch: this.served.get(request.type.key)?.get(request.slot)?.epoch ?? 0,
      drained: drained.promise,
    });
    try {
      await handler.onDrain(request, context);
    } finally {
      drained.resolve();
    }
    this.#reply(message);
  }

  #reply(message: TalkMessage): void {
    try {
      message.reply(new Uint8Array(0)).catch(() => {});
    } catch (error) {
      this.#client.log('debug', `${message.procedureName} not answered: ${error}`);
    }
  }

  #abort(message: TalkMessage, status: Status): void {
    try {
      message.replyAbort(status).catch(() => {});
    } catch (error) {
      this.#client.log('debug', `${message.procedureName} not aborted: ${error}`);
    }
  }

  revoke(type: Name, slot: number): void {
    this.draining.delete(slotKey(type, slot));
    const handler = this.handler;
    if (handler === undefined) {
      return;
    }
    void Promise.resolve()
      .then(() => handler.onRevoke?.(type, slot))
      .catch((error: unknown) =>
        this.#client.log('warn', `onRevoke ${type}/${slot} failed`, error),
      );
  }

  /**
   * The channel to the naming service was lost, or the client closed: every
   * locked slot is unlocked and served (as `RESUME`), once its `DRAIN`
   * handler is done.
   */
  onLost(): void {
    const locked = [...this.draining.values()];
    this.draining.clear();
    const handler = this.handler;
    if (handler === undefined) {
      return;
    }
    for (const lock of locked) {
      const { type, slot, epoch } = lock;
      this.#client.log(
        'info',
        `${this.#host.closed() ? 'naming client closed' : 'naming service lost'} while ` +
          `${type}/${slot} was draining, resuming`,
      );
      void lock.drained
        .then(() =>
          handler.onResume(
            new ResumeRequest(type, slot, { epoch }),
            SlotContext.local(type, slot, epoch, this.#client),
          ),
        )
        .catch((error: unknown) =>
          this.#client.log('warn', `onResume ${type}/${slot} failed`, error),
        );
    }
  }

  // -------------------------------------------------------------------------
  // Restoring after a (re)connect

  /**
   * Sends what is remembered for [type] on [session], once per session:
   * `SLOTS` and `HOLDING`, then a `CLAIM` of every slot this instance serves,
   * pipelined (all in the same turn). Waits for the registration of [type]
   * on [session] if this client registers that type. Without any
   * registration of it (a router or an operator tool) only `SLOTS` is sent,
   * and the `HOLDING` of the `declareHolding` calls waiting to be sent, which
   * the naming service refuses.
   *
   * The `HOLDING` also lists the served slots whose storage this instance
   * holds. It is a single request, so it reaches a restarted naming service
   * within its assignment hold even when the claims (sent a window at a
   * time) do not; the service then gives those slots back to this instance
   * as their holder instead of assigning them fresh.
   *
   * `HOLDING` goes first when the mirror, synced on [session], shows the
   * space as defined: the holders are then known before `SLOTS` gives this
   * instance capacity, so the allocator never sees those slots without their
   * holder. Otherwise (after a reconnect the mirror is not synced yet, and a
   * restarted naming service may not know the space, so `HOLDING` would
   * fail) `SLOTS` goes first, with `HOLDING` right behind it; the naming
   * service's settle window after `SLOTS` covers the gap.
   *
   * A request refused before it was sent (the channel's
   * `TalkOptions.maxOutgoingRequests`) stops the restoration there, so that
   * nothing overtakes it, and a retry resumes it later without sending again
   * what went out. [type] counts as restored on [session] once everything
   * went out.
   */
  restore(session: ClientSession, type: Name): void {
    const key = type.key;
    if (!session.usable || session.retrying.has(key)) {
      return;
    }
    const registered = this.#host.registeredOn(session, type);
    if (!registered && this.#host.registers(type)) {
      return;
    }
    const previous = session.restored.get(key);
    if (previous !== undefined && (previous || !registered)) {
      return;
    }
    const definition = this.definitions.get(key);
    const slots = (): boolean => {
      const sent = session.sentSlots.get(key);
      if (
        definition === undefined ||
        (sent !== undefined && sent[0] === definition && sent[1] === registered)
      ) {
        return true;
      }
      return this.#sendSlots(session, definition);
    };

    let sent: boolean;
    if (!registered) {
      sent = slots() && this.#flushHolding(session, type, new Set());
    } else {
      const declared = this.holdings.get(key);
      const bySlot = this.served.get(key);
      const held = new Set<number>(declared?.slots ?? []);
      for (const [slot, state] of bySlot ?? []) {
        if (state.holding) held.add(slot);
      }
      const declare =
        (declared?.pending.length ?? 0) > 0 || (held.size > 0 && !session.sentHolding.has(key));
      const holding = (): boolean => {
        if (!declare) {
          return true;
        }
        if (!this.#flushHolding(session, type, held)) {
          return false;
        }
        session.sentHolding.add(key);
        return true;
      };
      const mirrored = this.mirrors.get(key)?.space;
      const holdingFirst =
        declare &&
        definition !== undefined &&
        !session.syncing &&
        mirrored?.equals(definition.request.space) === true;
      sent = holdingFirst ? holding() && slots() : slots() && holding();
      if (sent && !session.reclaimed.has(key)) {
        session.reclaimed.add(key);
        if (bySlot !== undefined) {
          for (const [slot, state] of bySlot) {
            session.reclaims.push([type, slot, state]);
          }
          this.#pumpReclaims(session);
        }
      }
    }
    if (sent) {
      session.restored.set(key, registered);
    } else {
      this.#retry(session, type);
    }
  }

  /**
   * A request restoring [type] on [session] was refused before it was sent:
   * [type] is not restored there, and {@link restore} sends the rest after
   * `reconnectDelay`, again until it all went out. Nothing is left of it when
   * the session is lost: the next one restores everything.
   */
  #retry(session: ClientSession, type: Name): void {
    const key = type.key;
    session.restored.delete(key);
    if (!session.usable || session.retrying.has(key)) {
      return;
    }
    session.retrying.add(key);
    const delay = this.#client.reconnectDelay;
    this.#client.log('info', `slot state of ${type} not sent yet, retrying in ${delay} ms`);
    session.later(delay, () => {
      session.retrying.delete(key);
      this.restore(session, type);
    });
  }

  /**
   * Sends queued claims of served slots, at most {@link reclaimWindow} at a
   * time. Claims beyond the window that reach a freshly restarted naming
   * service after its assignment hold are served first come, first served
   * rather than by epoch.
   */
  #pumpReclaims(session: ClientSession): void {
    while (
      !session.reclaimsPaused &&
      session.reclaiming < reclaimWindow &&
      session.reclaims.length > 0 &&
      session.usable
    ) {
      const [type, slot, state] = session.reclaims.shift() as [Name, number, Served];
      if (this.served.get(type.key)?.get(slot) !== state) {
        continue;
      }
      session.reclaiming++;
      void this.#reclaim(session, type, slot, state).finally(() => {
        session.reclaiming--;
        this.#pumpReclaims(session);
      });
    }
  }

  /** A claim was refused before it was sent: the claims left wait `reconnectDelay`, in order. */
  #pauseReclaims(session: ClientSession): void {
    if (session.reclaimsPaused) {
      return;
    }
    session.reclaimsPaused = true;
    const delay = this.#client.reconnectDelay;
    this.#client.log('info', `claims of served slots not sent yet, retrying in ${delay} ms`);
    session.later(delay, () => {
      session.reclaimsPaused = false;
      this.#pumpReclaims(session);
    });
  }

  restoreAll(session: ClientSession): void {
    const keys = new Set([
      ...this.definitions.keys(),
      ...this.holdings.keys(),
      ...this.served.keys(),
    ]);
    for (const key of keys) {
      this.restore(session, Name.fromKey(key));
    }
  }

  /** Whether requests about [type] can go out on [session] now. */
  #ready(session: ClientSession, type: Name): boolean {
    return (
      session.usable && (this.#host.registeredOn(session, type) || !this.#host.registers(type))
    );
  }

  /** Sends [definition] on [session]. Returns false if the request was refused before it was sent. */
  #sendSlots(session: ClientSession, definition: SpaceDefinition): boolean {
    const type = definition.request.space.type;
    let response: Promise<TalkMessage>;
    try {
      response = session.channel.startRequest(
        Procedures.slots.toString(),
        definition.request.encode(),
        { name: Procedures.slots },
      ).response;
    } catch (error) {
      this.#client.log('debug', `SLOTS ${type} not sent: ${error}`);
      return false;
    }
    session.sentSlots.set(type.key, [definition, this.#host.registeredOn(session, type)]);
    response.then(
      () => definition.complete(),
      (error: unknown) => {
        if (!session.usable) {
          // Sent again after the next connect.
          return;
        }
        this.#client.log('warn', `SLOTS ${type} refused: ${error}`);
        if (this.definitions.get(type.key) === definition) {
          this.definitions.delete(type.key);
        }
        definition.fail(error);
      },
    );
    return true;
  }

  /**
   * Sends one `HOLDING` of [type] on [session] for [slots] and the
   * `declareHolding` calls waiting to be sent, unless both are empty. Returns
   * false if the request was refused before it was sent; the calls then wait
   * for the next attempt.
   */
  #flushHolding(session: ClientSession, type: Name, slots: Set<number>): boolean {
    const declared = this.holdings.get(type.key);
    const pending = declared?.pending.splice(0) ?? [];
    if (slots.size === 0 && pending.length === 0) {
      return true;
    }
    const all = new Set(slots);
    for (const p of pending) {
      for (const slot of p.slots) all.add(slot);
    }
    return this.#sendHolding(
      session,
      type,
      [...all].sort((a, b) => a - b),
      pending,
    );
  }

  /**
   * Sends `HOLDING` of [slots] of [type] on [session], answering [pending].
   * Returns false if the request was refused before it was sent; [pending]
   * then waits for the next attempt.
   */
  #sendHolding(
    session: ClientSession,
    type: Name,
    slots: number[],
    pending: PendingHolding[],
  ): boolean {
    let response: Promise<TalkMessage>;
    try {
      response = session.channel.startRequest(
        Procedures.holding.toString(),
        new HoldingRequest(type, slots).encode(),
        { name: Procedures.holding },
      ).response;
    } catch (error) {
      this.#client.log('debug', `HOLDING ${type} not sent: ${error}`);
      this.holdings.get(type.key)?.pending.unshift(...pending);
      return false;
    }
    response.then(
      (message) => {
        let discard: readonly number[];
        try {
          discard = HoldingResponse.decode(message.payload).discard;
        } catch (error) {
          for (const p of pending) p.completer.reject(error);
          return;
        }
        const drop = new Set(discard);
        const declared = this.holdings.get(type.key);
        for (const slot of drop) declared?.slots.delete(slot);
        const handler = this.handler;
        if (discard.length > 0 && handler?.onDiscard !== undefined) {
          try {
            handler.onDiscard(type, Object.freeze([...discard]));
          } catch (error) {
            this.#client.log('warn', `onDiscard ${type} failed`, error);
          }
        }
        for (const p of pending) {
          p.completer.resolve([...p.slots].filter((s) => drop.has(s)).sort((a, b) => a - b));
        }
      },
      (error: unknown) => {
        if (!session.usable) {
          this.holdings.get(type.key)?.pending.push(...pending);
          return;
        }
        this.#client.log('warn', `HOLDING ${type} refused: ${error}`);
        const declared = this.holdings.get(type.key);
        for (const slot of slots) declared?.slots.delete(slot);
        for (const p of pending) p.completer.reject(error);
      },
    );
    return true;
  }

  async #reclaim(session: ClientSession, type: Name, slot: number, state: Served): Promise<void> {
    let response: Promise<TalkMessage>;
    try {
      response = session.channel.startRequest(
        Procedures.claim.toString(),
        new ClaimRequest(type, slot, { holding: state.holding, epoch: state.epoch }).encode(),
        { name: Procedures.claim },
      ).response;
    } catch (error) {
      this.#client.log('debug', `CLAIM ${type}/${slot} not sent: ${error}`);
      if (session.usable) {
        // Refused before it was sent (the request limit): first in line when
        // the claims go on.
        session.reclaims.unshift([type, slot, state]);
        this.#pauseReclaims(session);
      }
      return;
    }
    try {
      const message = await response;
      const current = this.served.get(type.key)?.get(slot);
      if (current !== undefined) {
        try {
          current.epoch = ClaimResponse.decode(message.payload).epoch;
        } catch (error) {
          this.#client.log('warn', `malformed CLAIM response: ${error}`);
        }
      }
    } catch (error) {
      if (!session.usable) {
        return;
      }
      const bySlot = this.served.get(type.key);
      if (bySlot === undefined || bySlot.get(slot) !== state) {
        return;
      }
      this.#client.log('warn', `lost ${type}/${slot} after reconnect: ${error}`);
      bySlot.delete(slot);
      this.revoke(type, slot);
    }
  }

  // -------------------------------------------------------------------------
  // Client API

  defineSlots(request: SlotsRequest): Promise<void> {
    const type = request.space.type;
    const definition = new SpaceDefinition(request);
    const previous = this.definitions.get(type.key);
    this.definitions.set(type.key, definition);
    previous?.supersede(definition);
    const session = this.#host.session();
    if (session !== undefined && this.#ready(session, type)) {
      if (!session.restored.has(type.key)) {
        // Sent with the rest of the slot state of the type.
        this.restore(session, type);
      } else if (!this.#sendSlots(session, definition)) {
        this.#retry(session, type);
      }
    }
    return definition.completer.promise;
  }

  declareHolding(type: Name, slots: number[]): Promise<number[]> {
    let declared = this.holdings.get(type.key);
    if (declared === undefined) {
      declared = new Holdings();
      this.holdings.set(type.key, declared);
    }
    for (const slot of slots) declared.slots.add(slot);
    const pending = new PendingHolding(new Set(slots));
    const session = this.#host.session();
    if (session === undefined || !this.#ready(session, type)) {
      // Sent with the slot state of the type, after the next connect or once
      // its registration is back.
      declared.pending.push(pending);
    } else if (!session.restored.has(type.key)) {
      declared.pending.push(pending);
      this.restore(session, type);
    } else if (!this.#sendHolding(session, type, slots, [pending])) {
      this.#retry(session, type);
    }
    return pending.completer.promise;
  }

  /**
   * Sends a request of the client's slot API on [session] and resolves with
   * its response. The channel's request limit
   * (`TalkOptions.maxOutgoingRequests`) never fails it: a request the channel
   * refuses for it waits here, with the requests made after it, and goes out
   * once one of these requests completes, or after `reconnectDelay` when none
   * is in flight (other requests hold the limit). Waiting requests reject
   * with `UNAVAILABLE` when the channel is lost.
   *
   * Aborting [signal] rejects the call with its reason: a call still waiting
   * leaves the queue unsent, and one sent is cancelled (Talk cancels the
   * request) unless [cancelsSent] is false, when it runs to its answer and
   * the abort has no effect.
   */
  call(
    session: ClientSession,
    procedure: Name,
    payload: Uint8Array,
    signal?: AbortSignal,
    cancelsSent = true,
  ): Promise<TalkMessage> {
    const call: ClientCall = { procedure, payload, response: new Deferred(), signal, cancelsSent };
    session.calls.push(call);
    if (signal !== undefined) {
      // Added before Talk's own listener (the request is started below), so
      // that the call rejects with the reason rather than Talk's CANCELLED.
      const onAbort = (): void => {
        const queued = session.calls.indexOf(call);
        if (queued >= 0) {
          session.calls.splice(queued, 1);
        } else if (!cancelsSent) {
          return;
        }
        call.response.reject(signal.reason);
      };
      signal.addEventListener('abort', onAbort, { once: true });
      const remove = (): void => signal.removeEventListener('abort', onAbort);
      call.response.promise.then(remove, remove);
    }
    this.#pumpCalls(session);
    return call.response.promise;
  }

  #pumpCalls(session: ClientSession): void {
    while (session.calls.length > 0 && !session.callsPaused) {
      const call = session.calls[0] as ClientCall;
      let response: Promise<TalkMessage>;
      try {
        response = session.channel.startRequest(call.procedure.toString(), call.payload, {
          name: call.procedure,
          ...(call.cancelsSent && call.signal !== undefined && { signal: call.signal }),
        }).response;
      } catch (error) {
        if (
          error instanceof SwitchboardError &&
          error.code === StatusCode.resourceExhausted &&
          session.usable
        ) {
          if (session.calling === 0) {
            // Nothing of these to wait for: try again later.
            session.callsPaused = true;
            session.later(this.#client.reconnectDelay, () => {
              session.callsPaused = false;
              this.#pumpCalls(session);
            });
          }
          return;
        }
        session.calls.shift();
        call.response.reject(error);
        continue;
      }
      session.calls.shift();
      session.calling++;
      response
        .then(
          (message) => call.response.resolve(message),
          (error: unknown) => call.response.reject(error),
        )
        .finally(() => {
          session.calling--;
          this.#pumpCalls(session);
        });
    }
  }

  usableSession(): ClientSession {
    const session = this.#host.session();
    if (this.#host.closed()) {
      throw SwitchboardError.of(StatusCode.failedPrecondition, 'naming client closed');
    }
    if (session === undefined || !session.usable) {
      throw SwitchboardError.of(StatusCode.unavailable, 'not connected to the naming service');
    }
    return session;
  }

  async claim(
    type: Name,
    slot: number,
    holding: boolean,
    signal: AbortSignal | undefined,
  ): Promise<number> {
    const session = this.usableSession();
    const response = await this.call(
      session,
      Procedures.claim,
      new ClaimRequest(type, slot, {
        holding,
        epoch: this.served.get(type.key)?.get(slot)?.epoch ?? 0,
      }).encode(),
      signal,
    );
    const epoch = ClaimResponse.decode(response.payload).epoch;
    const served = this.served.get(type.key)?.get(slot);
    if (served !== undefined) served.epoch = epoch;
    return epoch;
  }

  async release(
    type: Name,
    slot: number,
    keepStorage: boolean,
    signal: AbortSignal | undefined,
  ): Promise<void> {
    const session = this.usableSession();
    // Once sent, a RELEASE runs to its answer: cancelled, the slot could be
    // free at the naming service and still served here.
    await this.call(
      session,
      Procedures.release,
      new ReleaseRequest(type, slot, { keepStorage }).encode(),
      signal,
      false,
    );
    this.served.get(type.key)?.delete(slot);
    this.draining.delete(slotKey(type, slot));
    if (keepStorage) {
      let declared = this.holdings.get(type.key);
      if (declared === undefined) {
        declared = new Holdings();
        this.holdings.set(type.key, declared);
      }
      declared.slots.add(slot);
    } else {
      this.holdings.get(type.key)?.slots.delete(slot);
    }
  }

  async locate(type: Name, slot: number, signal: AbortSignal | undefined): Promise<LocateResponse> {
    const session = this.usableSession();
    const response = await this.call(
      session,
      Procedures.locate,
      new LocateRequest(type, slot).encode(),
      signal,
    );
    return LocateResponse.decode(response.payload);
  }

  migrate(type: Name, slot: number, to: number, signal?: AbortSignal): AsyncIterable<PhaseItem> {
    const stream: TalkStream = this.usableSession().channel.streamRequest(
      Procedures.migrate.toString(),
      new MigrateRequest(type, slot, { to }).encode(),
      { name: Procedures.migrate, ...(signal !== undefined && { signal }) },
    );
    return phases(stream);
  }

  servedSlots(type: Name): ReadonlyMap<number, number> {
    const out = new Map<number, number>();
    for (const [slot, state] of this.served.get(type.key) ?? []) {
      out.set(slot, state.epoch);
    }
    return out;
  }

  // -------------------------------------------------------------------------
  // Mirror

  onSpaceItem(session: ClientSession, space: SlotSpace): void {
    const key = space.type.key;
    if (session.syncing) {
      session.seenSpaces.add(key);
    }
    const old = this.mirrors.get(key);
    if (old?.space.equals(space)) {
      return;
    }
    if (old !== undefined) {
      this.#dropEntries(old);
    }
    const mirror = new Mirror(space);
    this.mirrors.set(key, mirror);
    this.views.set(key, mirror.view);
  }

  onSlotItem(session: ClientSession, item: SlotItem): void {
    const mirror = this.mirrors.get(item.type.key);
    if (mirror === undefined || item.slot >= mirror.space.count) {
      this.#client.log('warn', `SLOT event outside any known space: ${item}`);
      return;
    }
    if (session.syncing) {
      session.seenSlots.add(slotKey(item.type, item.slot));
    }
    this.#set(mirror, item.slot, item.entry);
  }

  /** On the `WATCH` reply: spaces and slots missing from the snapshot are gone. */
  onSynced(session: ClientSession): void {
    for (const [key, mirror] of [...this.mirrors]) {
      if (!session.seenSpaces.has(key)) {
        this.#dropEntries(mirror);
        this.mirrors.delete(key);
        this.views.delete(key);
        continue;
      }
      for (const slot of [...mirror.entries.keys()]) {
        if (!session.seenSlots.has(slotKey(mirror.space.type, slot))) {
          this.#set(mirror, slot, SlotEntry.unassigned);
        }
      }
    }
    session.seenSpaces.clear();
    session.seenSlots.clear();
  }

  #set(mirror: Mirror, slot: number, entry: SlotEntry): void {
    const previous = mirror.entries.get(slot) ?? SlotEntry.unassigned;
    if (entry.isEmpty) {
      mirror.entries.delete(slot);
    } else {
      mirror.entries.set(slot, entry);
    }
    if (!previous.equals(entry) && !this.#host.closed()) {
      this.#host.emitSlotEvent(new SlotEvent(mirror.space.type, slot, entry, previous));
    }
  }

  #dropEntries(mirror: Mirror): void {
    for (const slot of [...mirror.entries.keys()]) {
      this.#set(mirror, slot, SlotEntry.unassigned);
    }
  }

  slotOwner(type: Name, slot: number): SlotEntry | undefined {
    const entry = this.mirrors.get(type.key)?.entries.get(slot);
    return entry !== undefined && entry.owner !== 0 ? entry : undefined;
  }

  close(): void {
    const cancelled = SwitchboardError.of(StatusCode.cancelled, 'naming client closed');
    for (const definition of this.definitions.values()) {
      definition.fail(cancelled);
    }
    for (const declared of this.holdings.values()) {
      for (const p of declared.pending) {
        p.completer.reject(cancelled);
      }
      declared.pending.length = 0;
    }
    this.draining.clear();
  }
}

/** The phase items of a `MIGRATE` stream; stopping the iteration cancels the request. */
async function* phases(stream: TalkStream): AsyncGenerator<PhaseItem> {
  let ended = false;
  try {
    for await (const item of stream.items) {
      yield PhaseItem.decode(item.payload);
    }
    ended = true;
  } finally {
    if (!ended) {
      stream.cancel();
    }
  }
}

function failure(error: unknown): Status {
  if (error instanceof SwitchboardError && !error.status.isOk) {
    return error.status;
  }
  return Status.of(StatusCode.unavailable, String(error));
}

/**
 * The {@link SlotRequestContext} of one request from the naming service, or
 * of a resume the client makes by itself ({@link SlotContext.local}).
 */
class SlotContext implements SlotRequestContext {
  /** The request; undefined for a local resume. */
  readonly message: TalkMessage | undefined;
  readonly type: Name;
  readonly slot: number;
  readonly epoch: number;
  /** `slotHandlerMaxDuration`; 0 for no bound. */
  readonly bound: number;
  readonly #client: NamingClient;
  readonly #started: number;
  readonly #cancelled = new Deferred<void>();
  readonly #abort = new AbortController();

  private constructor(
    message: TalkMessage | undefined,
    type: Name,
    slot: number,
    epoch: number,
    bound: number,
    client: NamingClient,
  ) {
    this.message = message;
    this.type = type;
    this.slot = slot;
    this.epoch = epoch;
    this.bound = bound;
    this.#client = client;
    this.#started = client.clock.now();
    void message?.onCancel.then(() => this.cancel());
  }

  /** The context of a resume the client makes without a request (the naming service was lost): nothing to answer, nothing to extend. */
  static local(type: Name, slot: number, epoch: number, client: NamingClient): SlotContext {
    return new SlotContext(undefined, type, slot, epoch, 0, client);
  }

  /**
   * The context of [message], whose payload starts with the slot's type and
   * slot and carries its epoch.
   *
   * @throws {ProtocolError} for a malformed payload.
   */
  static of(message: TalkMessage, client: NamingClient): SlotContext {
    const procedure = message.procedure;
    const payload = message.payload;
    const bound = client.slotHandlerMaxDuration;
    let r: { type: Name; slot: number; epoch: number };
    if (procedure.equals(Procedures.assign)) {
      r = AssignRequest.decode(payload);
    } else if (procedure.equals(Procedures.drain)) {
      r = DrainRequest.decode(payload);
    } else if (procedure.equals(Procedures.forward)) {
      r = ForwardRequest.decode(payload);
    } else {
      r = ResumeRequest.decode(payload);
    }
    return new SlotContext(message, r.type, r.slot, r.epoch, bound, client);
  }

  cancel(): void {
    if (this.#cancelled.resolve()) {
      this.#abort.abort(SwitchboardError.of(StatusCode.cancelled, 'slot request cancelled'));
    }
  }

  get isCancelled(): boolean {
    return this.#cancelled.isSettled;
  }

  get onCancel(): Promise<void> {
    return this.#cancelled.promise;
  }

  get signal(): AbortSignal {
    return this.#abort.signal;
  }

  extend(options: { deadline?: number; renew?: number } = {}): void {
    let { deadline, renew } = options;
    for (const [name, value] of [
      ['deadline', deadline],
      ['renew', renew],
    ] as const) {
      if (value !== undefined && !(value >= 0)) {
        throw new RangeError(`${name} must not be negative: ${value}`);
      }
    }
    const message = this.message;
    if (message === undefined || !message.canReply) {
      this.#client.log(
        'debug',
        `${message?.procedureName ?? 'local RESUME'} ${this.type}/${this.slot} can no longer be ` +
          'answered; not extended',
      );
      return;
    }
    if (this.bound > 0) {
      const left = Math.max(0, this.bound - (this.#client.clock.now() - this.#started));
      if (deadline !== undefined && deadline > left) deadline = left;
      if (renew !== undefined && renew > left) renew = left;
    }
    try {
      message
        .extend({
          ...(deadline !== undefined && { deadline }),
          ...(renew !== undefined && { renew }),
        })
        .catch(() => {});
    } catch (error) {
      this.#client.log(
        'debug',
        `${message.procedureName} ${this.type}/${this.slot} not extended: ${error}`,
      );
    }
  }
}
