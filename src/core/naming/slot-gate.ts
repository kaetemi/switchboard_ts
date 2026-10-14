/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { ChannelAddress, ServiceAddress } from '../address/index.ts';
import type { MuxChannel } from '../mux/contract.ts';
import { type Name, type NameLike, toName } from '../name.ts';
import { ProtocolError, Status, StatusCode, SwitchboardError } from '../status.ts';
import type { ChannelHandler, IncomingChannel } from '../switchboard/contract.ts';
import { genericStatus } from '../switchboard/generic-status.ts';
import { pipeChannels } from '../switchboard/proxy.ts';
import type { Switchboard } from '../switchboard/switchboard.ts';
import type { TalkMessage } from '../talk/contract.ts';
import { TalkChannel } from '../talk/talk-channel.ts';
import { forwardMessage } from '../talk/talk-forward.ts';
import type { Clock, TimerHandle } from '../util/clock.ts';
import { Deferred } from '../util/deferred.ts';
import type { Logger, LogLevel } from '../util/logger.ts';
import type { NamingClient } from './naming-client.ts';
import { AssignResult, type SlotHandler, type SlotRequestContext } from './naming-client-slots.ts';
import {
  type AssignRequest,
  type DrainRequest,
  type ForwardRequest,
  MovedStatus,
  maxU32,
  type ResumeRequest,
} from './naming-protocol.ts';

const logName = 'Switchboard.Router';

/** The epoch after [epoch], as the naming service assigns it (wrapping to 1). */
function nextEpoch(epoch: number): number {
  return epoch >= maxU32 ? 1 : epoch + 1;
}

/**
 * How long, in milliseconds, a channel a gate forwards late requests on
 * stays open without a request in flight before it is closed (it is opened
 * again for the next one).
 */
const forwardIdle = 1000;

/**
 * How much longer, in milliseconds, than `SlotGate.drainTimeout` a `DRAIN`
 * that waits for work in flight declares, so that the naming client does
 * not give it up at the moment the gate stops waiting and drains anyway.
 */
const drainStart = 1000;

/** What {@link SlotLifecycle.load} takes beside the slot. */
export interface SlotLoadOptions {
  /** The slot's epoch at this instance. */
  readonly epoch: number;
  /** Where the slot's state is: 0 nowhere, this instance, or another instance. */
  readonly holder: number;
  /** Shared recovery: the state can be loaded from shared storage. */
  readonly shared: boolean;
  /** The `ASSIGN` request; undefined when the gate is driven without one. */
  readonly context?: SlotRequestContext | undefined;
}

/** What {@link SlotLifecycle.drain} takes beside the slot. */
export interface SlotDrainOptions {
  /** The slot's epoch at its new owner. */
  readonly epoch: number;
  /** The new owner. */
  readonly to: number;
  /** The `DRAIN` request; undefined when the gate is driven without one. */
  readonly context?: SlotRequestContext | undefined;
}

/** The gate each lifecycle is attached to. */
const attachedGates = new WeakMap<SlotLifecycle, SlotGate>();

/**
 * The loads and unloads of each slot still running, per lifecycle, by
 * whichever gate it was attached to: until they end, an `ASSIGN` of the
 * slot is refused, so that the cleanup of an earlier generation never
 * touches the state of a later one.
 */
const busySlots = new WeakMap<SlotLifecycle, Map<number, number>>();

function busyOf(lifecycle: SlotLifecycle): Map<number, number> {
  let busy = busySlots.get(lifecycle);
  if (busy === undefined) {
    busy = new Map();
    busySlots.set(lifecycle, busy);
  }
  return busy;
}

/**
 * What a sharded service does with its slots: the application side of a
 * {@link SlotGate}. Extend this class (the gate attaches itself to it, see
 * {@link gate}).
 *
 * The gate calls {@link load} when a slot is assigned to this instance,
 * {@link drain} when it moves away, {@link unload} when this instance no
 * longer serves it, and {@link serve} for every channel addressed to a slot
 * it serves. See the wiki page "Polyverse Switchboard Sharding", sections
 * "Requests from the naming service to instances" and "The hand-over, step
 * by step".
 */
export abstract class SlotLifecycle {
  /**
   * The gate this lifecycle is attached to.
   *
   * @throws {Error} before the gate is created.
   */
  get gate(): SlotGate {
    const gate = attachedGates.get(this);
    if (gate === undefined) {
      throw new Error(`${this.constructor.name} is not attached to a slot gate`);
    }
    return gate;
  }

  /**
   * Loads or initialises [slot] and resolves once it can be served.
   *
   * `holder` says where the slot's state is: 0 nowhere (start fresh), this
   * instance (reclaim local storage), another instance (fetch it from there,
   * for example over a channel to that instance without a shard slot, which
   * its gate hands to `noSlotHandler`; or, when `shared` is set, load it
   * from shared storage). Resolve with `AssignResult.holding` when this
   * instance now holds the slot's storage, `AssignResult.notHolding` when it
   * does not (a stateless service, or state kept in shared storage only).
   *
   * Throw `SwitchboardError.of(StatusCode.unavailable)` when the slot cannot
   * be served; it then stays free. Channels for the slot that arrive while
   * this runs are queued and served once it resolves.
   *
   * `context` is the `ASSIGN` request (undefined when the gate is driven
   * without one). The naming service waits its `handoverTimeout` (60 s by
   * default) for the answer; a load that may take longer declares how long
   * with `SlotRequestContext.extend`, at the steps where it knows (the
   * transfer started, its size is known, a phase completed), and stops when
   * `SlotRequestContext.onCancel` resolves.
   *
   * Until this resolves, and the {@link unload} that follows it if the slot
   * was revoked or the gate closed meanwhile, an `ASSIGN` of [slot] is
   * refused with `UNAVAILABLE` (the naming service offers it again after its
   * backoff, or to another instance), even on another gate this lifecycle is
   * attached to later: a load that never resolves keeps the slot from this
   * instance.
   */
  abstract load(slot: number, options: SlotLoadOptions): Promise<AssignResult>;

  /**
   * [slot] is moving to instance `to` and will have `epoch` there. The gate
   * has locked it (new channels and gated requests are queued) and waited
   * for the work in flight to end; make the state available to `to`
   * (transfer it over a channel to `to`, flush it to shared storage, or keep
   * it for `to` to fetch) and close the long-lived channels that the slot's
   * queued work must not overtake (with `SlotGate.relocatedStatus`, since
   * they were served). Throwing abandons the migration (the slot is resumed
   * here). The default does nothing.
   *
   * When the wait for the work in flight timed out (`SlotGate.drainTimeout`),
   * the gate has closed the slot's tracked channels with `RELOCATED` before
   * this is called; `SlotGate.serveRequest` handlers still running cannot be
   * stopped and may still act on the slot's state.
   *
   * `context` is the `DRAIN` request (undefined when the gate is driven
   * without one). A drain that may take longer than the naming service
   * waits, its `handoverTimeout` (60 s by default) from this call (the gate
   * restarts that timeout when it had to wait for work in flight), declares
   * how long with `SlotRequestContext.extend`.
   */
  drain(slot: number, options: SlotDrainOptions): void | Promise<void> {
    void slot;
    void options;
  }

  /**
   * This instance no longer serves [slot]: after the forwarding grace
   * period of a migration, after the slot was revoked or released, or when
   * the gate closes. Drop the slot's state unless this instance keeps its
   * storage (it is still the holder). The default does nothing. Until it
   * resolves, an `ASSIGN` of [slot] is refused with `UNAVAILABLE` (see
   * {@link load}).
   */
  unload(slot: number): void | Promise<void> {
    void slot;
  }

  /**
   * Serves [channel], addressed to [slot], which this instance serves. The
   * handler owns the channel as a `ChannelHandler` does, and may be
   * asynchronous: a throw, or a returned promise that rejects, closes it
   * with `INTERNAL` (logged).
   *
   * Unless the gate was created with `trackChannels: false`, the channel
   * counts as work in flight for [slot] until it ends (not until the
   * returned promise settles), so a migration waits for it
   * (`SlotGate.drainTimeout`); call `SlotGate.detach` for a long-lived
   * channel, and gate its requests with `SlotGate.serveRequest`.
   */
  abstract serve(channel: IncomingChannel, slot: number): unknown;

  /**
   * `HOLDING` reported [slots] as no longer held by this instance: their
   * local storage should be discarded. Called for every `HOLDING` response,
   * the one `MeshNode.publishSharded` sends and those the naming client
   * sends again after a reconnect (`SlotHandler.onDiscard`). May be
   * asynchronous; a throw, or a returned promise that rejects, is logged.
   * The default does nothing.
   */
  discard(slots: readonly number[]): unknown {
    void slots;
    return undefined;
  }
}

/** Where a {@link SlotGate} stands with one slot. */
export const SlotGateState = {
  /** `ASSIGN` arrived and `SlotLifecycle.load` runs; channels are queued. */
  loading: 'loading',
  /** Served here. */
  serving: 'serving',
  /** Locked by `DRAIN`, until `FORWARD` or `RESUME` (or an `ASSIGN`, see `SlotGate.onAssign`); channels are queued. */
  locked: 'locked',
  /** Handed over by `FORWARD`; channels and gated requests are forwarded to the new owner until the grace period ends. */
  forwarding: 'forwarding',
} as const;

/** Where a {@link SlotGate} stands with one slot: one of the values of {@link SlotGateState}. */
export type SlotGateState = (typeof SlotGateState)[keyof typeof SlotGateState];

/** What a {@link SlotGate} takes beside its node, naming client and type. Durations in milliseconds. */
export interface SlotGateOptions {
  /** The application side; the gate attaches to it. */
  readonly lifecycle: SlotLifecycle;
  /** How long channels and gated requests keep being forwarded to the new owner after `FORWARD`; default 30 000. */
  readonly forwardGrace?: number | undefined;
  /**
   * Longest wait for a locked slot's work in flight; then its tracked
   * channels are closed with `RELOCATED` and `SlotLifecycle.drain` is called
   * anyway. Default 30 000.
   */
  readonly drainTimeout?: number | undefined;
  /** Most channels queued at a time, over all slots, including forwarding opens waiting for a connection; default 1024. */
  readonly maxQueuedChannels?: number | undefined;
  /** Most requests queued by `serveRequest` at a time, over all slots, including those waiting for a forwarding channel; default 1024. */
  readonly maxQueuedRequests?: number | undefined;
  /** Whether served channels count as work in flight until they end; default true. */
  readonly trackChannels?: boolean | undefined;
  /** Receives the channels that carry no shard slot; undefined rejects them with `INVALID_ARGUMENT`. */
  readonly noSlotHandler?: ChannelHandler | undefined;
  /** The instance id the type is registered as; 0 until known. */
  readonly instance?: number | undefined;
  /** The gate's timers (the grace period, the drain wait, idle forwarding channels); default the node's clock. */
  readonly clock?: Clock | undefined;
  /** Diagnostics, logged as `Switchboard.Router`; default the node's logger. */
  readonly logger?: Logger | undefined;
}

/** A channel a gate forwards the requests for one slot that carry one credential on, opened to the new owner with that credential. */
class Forward {
  /** The credential as a string, one character per byte. */
  readonly key: string;
  /** The application payload of the channel's OPEN. */
  readonly payload: Uint8Array;
  /** The channel, once its open has been started. */
  channel: Promise<TalkChannel> | undefined;
  /** Requests being forwarded, from before the channel is open until they end. */
  pending = 0;
  /** Closes {@link channel} once no request was in flight for a while. */
  idle: TimerHandle | undefined;

  constructor(key: string, payload: Uint8Array) {
    this.key = key;
    this.payload = payload;
  }
}

/** A request queued by {@link SlotGate.serveRequest}. */
class Parked {
  readonly message: TalkMessage;
  readonly handler: (message: TalkMessage) => unknown;
  /** The credential to forward it with; undefined for its channel's. */
  readonly payload: Uint8Array | undefined;
  readonly done = new Deferred<void>();

  constructor(
    message: TalkMessage,
    handler: (message: TalkMessage) => unknown,
    payload: Uint8Array | undefined,
  ) {
    this.message = message;
    this.handler = handler;
    this.payload = payload;
  }

  finish(): void {
    this.done.resolve();
  }
}

/** A gate's state of one slot. */
class GateSlot {
  state: SlotGateState = SlotGateState.loading;
  epoch: number;
  result: AssignResult = AssignResult.holding;
  /** `SlotLifecycle.load` resolved and `SlotLifecycle.unload` has not run. */
  loaded = false;
  /** The new owner while locked (as announced by `DRAIN`) or forwarding. */
  to = 0;
  readonly channels: IncomingChannel[] = [];
  readonly requests: Parked[] = [];
  /** Served channels counted as work in flight. */
  readonly tracked = new Set<IncomingChannel>();
  /** `SlotGate.serveRequest` handlers running. */
  work = 0;
  /** Resolved when nothing is in flight, while `DRAIN` waits for it. */
  idle: Deferred<void> | undefined;
  grace: TimerHandle | undefined;
  /** The last forwarded channel's open, which the next one waits for. */
  opening: Promise<void> = Promise.resolve();
  /** The channels requests are forwarded on, by credential ({@link Forward.key}). */
  readonly forwards = new Map<string, Forward>();

  constructor(epoch: number) {
    this.epoch = epoch;
  }

  get isIdle(): boolean {
    return this.tracked.size === 0 && this.work === 0;
  }
}

function isPromiseLike(value: unknown): value is PromiseLike<unknown> {
  return (
    typeof value === 'object' &&
    value !== null &&
    typeof (value as { then?: unknown }).then === 'function'
  );
}

function checkCount(name: string, value: number): number {
  if (!Number.isInteger(value) || value < 0) {
    throw new RangeError(`${name} must be a non-negative integer: ${value}`);
  }
  return value;
}

/**
 * The instance side of sharding for one sharded type on a node: serves the
 * naming service's slot requests ({@link SlotHandler}) and gates the
 * channels addressed to the type's slots ({@link handler}).
 *
 * Register {@link handler} for the instance the type was registered as
 * (`switchboard.registerService(type, gate.handler, { instance: id })`),
 * and install the gate as the naming client's slot handler, through a
 * {@link SlotGates} when the client serves several sharded types.
 * `MeshNode.publishSharded` does all of it.
 *
 * Channels are dispatched by their shard slot (wiki page "Polyverse
 * Switchboard Sharding", sections "Requests from the naming service to
 * instances" and "Routing"):
 *
 * - no shard slot: to {@link noSlotHandler} (state transfer between
 *   instances, say), else rejected with `INVALID_ARGUMENT`; a slot outside
 *   the space is rejected with `OUT_OF_RANGE`;
 * - a slot served here: to `SlotLifecycle.serve`;
 * - a slot being loaded, or locked by `DRAIN`: queued, unread (the mux
 *   buffers its subframes), at most {@link maxQueuedChannels} over all
 *   slots, beyond which `UNAVAILABLE`; served here on `RESUME`, piped to
 *   the new owner on `FORWARD`;
 * - a slot handed over by `FORWARD`, during {@link forwardGrace}: piped to
 *   the new owner (`pipeChannels`) with the same open payload except the
 *   instance, which is set to the new owner's so that its dispatch is
 *   exact; the application payload (credential) is unchanged;
 * - any other slot: rejected with `MOVED` naming the owner from the naming
 *   client's mirror ({@link movedStatus}).
 *
 * Requests inside long-lived channels are gated with {@link serveRequest}
 * the same way, at the request level: served, queued, forwarded as message
 * chains (`forwardMessage`) over channels that carry the credential of the
 * channel each request arrived on, or answered `ABORT MOVED`.
 *
 * `MOVED` always means that nothing was processed: the channel was refused
 * at open, or queued and never read, or the request was never run, so the
 * client may send it again to the new owner. A channel that was served here
 * (handed to `SlotLifecycle.serve`) and is ended because the slot moved on
 * is closed with `RELOCATED` carrying the same owner and epoch fields
 * ({@link relocatedStatus}): its work may have taken effect.
 *
 * On `DRAIN` the gate locks the slot and waits until the slot's work in
 * flight has ended (its tracked channels have closed and its
 * {@link serveRequest} handlers have completed), at most
 * {@link drainTimeout}; when that wait times out, it closes the tracked
 * channels still open with `RELOCATED` naming the new owner and epoch (so
 * that nothing more is served on state that is being handed over) and logs
 * a warning for the {@link serveRequest} handlers still running, which
 * cannot be stopped. Then it calls `SlotLifecycle.drain`. On `FORWARD` it
 * pipes the queued channels to the new owner in arrival order, forwards
 * the queued requests, answers, and keeps forwarding for
 * {@link forwardGrace}, then calls `SlotLifecycle.unload`. On `RESUME` it
 * serves the queue itself, and so on an `ASSIGN` of the locked slot when no
 * other instance owned it since (the `RESUME` was lost, see
 * {@link onAssign}). On revocation (and {@link release}, {@link close}) it
 * rejects the queue with `MOVED`, closes the slot's tracked channels with
 * `RELOCATED` ({@link relocatedStatus}) and calls `SlotLifecycle.unload`. A
 * handler that locks or revokes the slot while its queue is being served
 * leaves the remaining work subject to that new state.
 */
export class SlotGate implements SlotHandler {
  /** The node channels are dispatched on and forwarded from. */
  readonly switchboard: Switchboard;
  /** The naming client whose slot requests this gate serves, and whose mirror names the owners in `MOVED` and `RELOCATED`. */
  readonly client: NamingClient;
  /** The sharded type. */
  readonly type: Name;
  /** The application side. */
  readonly lifecycle: SlotLifecycle;
  /** How long channels and gated requests keep being forwarded to the new owner after `FORWARD`, in milliseconds. */
  readonly forwardGrace: number;
  /** Longest wait for a locked slot's work in flight, in milliseconds. */
  readonly drainTimeout: number;
  /**
   * Most channels queued at a time, over all slots, including forwarding
   * opens waiting for a connection. A cancelled forwarding open keeps its
   * place in this bound until its open attempt settles.
   */
  readonly maxQueuedChannels: number;
  /**
   * Most requests queued by {@link serveRequest} at a time, over all slots;
   * a queued request leaves the queue when it can no longer be answered.
   * Also bounds requests waiting for a forwarding channel to open; those
   * keep their place until the open attempt settles, even if cancelled.
   */
  readonly maxQueuedRequests: number;
  /** Whether served channels count as work in flight until they end. */
  readonly trackChannels: boolean;
  /** Receives the channels that carry no shard slot; undefined rejects them with `INVALID_ARGUMENT`. */
  noSlotHandler: ChannelHandler | undefined;
  /** The instance id of {@link type} on this node; 0 until known. */
  instance: number;
  /** The gate's timers. */
  readonly clock: Clock;
  /** The gate's diagnostics. */
  readonly logger: Logger;

  readonly #slots = new Map<number, GateSlot>();
  readonly #servedBy = new WeakMap<IncomingChannel, GateSlot>();
  /** The OPEN application payload of the channels requests arrived on. */
  readonly #payloads = new WeakMap<object, Uint8Array>();
  #queuedChannels = 0;
  #queuedRequests = 0;
  #closed = false;

  /**
   * The gate of [type] on [switchboard], serving the slot requests [client]
   * receives, with the application's `options.lifecycle` (which it attaches
   * to; a lifecycle serves one gate at a time, and may be attached to a new
   * gate once its gate is closed; the new gate refuses to assign a slot
   * whose load or unload by the closed gate is still running, see
   * {@link onAssign}).
   *
   * @throws {Error} if the lifecycle is attached to another gate that is not closed.
   * @throws {RangeError} for negative bounds.
   */
  constructor(
    switchboard: Switchboard,
    client: NamingClient,
    type: NameLike,
    options: SlotGateOptions,
  ) {
    this.switchboard = switchboard;
    this.client = client;
    this.type = toName(type);
    this.lifecycle = options.lifecycle;
    this.forwardGrace = options.forwardGrace ?? 30_000;
    this.drainTimeout = options.drainTimeout ?? 30_000;
    this.maxQueuedChannels = checkCount('maxQueuedChannels', options.maxQueuedChannels ?? 1024);
    this.maxQueuedRequests = checkCount('maxQueuedRequests', options.maxQueuedRequests ?? 1024);
    this.trackChannels = options.trackChannels ?? true;
    this.noSlotHandler = options.noSlotHandler;
    this.instance = options.instance ?? 0;
    this.clock = options.clock ?? switchboard.clock;
    this.logger = options.logger ?? switchboard.logger;
    const attached = attachedGates.get(this.lifecycle);
    if (attached !== undefined && !attached.#closed) {
      throw new Error(`${this.lifecycle.constructor.name} is attached to another slot gate`);
    }
    attachedGates.set(this.lifecycle, this);
  }

  #log(level: LogLevel, message: string, error?: unknown): void {
    try {
      this.logger.log(level, logName, message, error);
    } catch {
      // A logger that throws must not break the gate.
    }
  }

  /** Dispatches channels addressed to {@link type}; register it with `Switchboard.registerService`. */
  readonly handler: ChannelHandler = (incoming) => this.#onChannel(incoming);

  /** The slots this gate serves (serving or locked), with their epochs. A new map, exact when read. */
  get servedSlots(): ReadonlyMap<number, number> {
    const served = new Map<number, number>();
    for (const [slot, s] of this.#slots) {
      if (s.state === SlotGateState.serving || s.state === SlotGateState.locked) {
        served.set(slot, s.epoch);
      }
    }
    return served;
  }

  /** Where the gate stands with [slot]; undefined when it has nothing to do with it. */
  stateOf(slot: number): SlotGateState | undefined {
    return this.#slots.get(slot)?.state;
  }

  /** Whether [slot] is served here now (not loading, locked or handed over). */
  serves(slot: number): boolean {
    return this.#slots.get(slot)?.state === SlotGateState.serving;
  }

  /**
   * The owner and epoch of [slot] for `MOVED` and `RELOCATED` (wiki page
   * "Polyverse Switchboard Sharding", section "Routing"): the new owner
   * while forwarding, else the owner in the naming client's mirror unless
   * that is this instance, else unknown.
   */
  movedTo(slot: number): MovedStatus {
    return this.#movedTo(slot, this.#slots.get(slot));
  }

  #movedTo(slot: number, s: GateSlot | undefined): MovedStatus {
    if (s !== undefined && s.state === SlotGateState.forwarding) {
      return new MovedStatus({ owner: s.to, epoch: s.epoch });
    }
    const entry = this.client.slotOwner(this.type, slot);
    if (entry !== undefined && entry.owner !== this.instance) {
      return new MovedStatus({ owner: entry.owner, epoch: entry.epoch });
    }
    return MovedStatus.unknown;
  }

  /**
   * The `MOVED` status for [slot], carrying {@link movedTo}: for channels
   * and requests about [slot] that were not processed here, which the
   * client may send again to the owner.
   */
  movedStatus(slot: number): Status {
    return this.movedTo(slot).toStatus();
  }

  /**
   * The `RELOCATED` status for [slot], carrying {@link movedTo} like
   * `MOVED`: for channels and requests about [slot] that were served here
   * and are ended because the slot moved on, whose work may have taken
   * effect.
   */
  relocatedStatus(slot: number): Status {
    return this.movedTo(slot).toStatus({ relocated: true });
  }

  /**
   * Stops counting [channel], served through `SlotLifecycle.serve`, as work
   * in flight of its slot: a long-lived channel whose requests the
   * application gates with {@link serveRequest}, or closes itself in
   * `SlotLifecycle.drain`. Does nothing for other channels.
   */
  detach(channel: IncomingChannel): void {
    const s = this.#servedBy.get(channel);
    if (s?.tracked.delete(channel) === true) {
      this.#checkIdle(s);
    }
  }

  // Channels ----------------------------------------------------------------

  #onChannel(incoming: IncomingChannel): unknown {
    const slot = incoming.address.shard;
    if (slot === undefined) {
      const other = this.noSlotHandler;
      if (other !== undefined) {
        return other(incoming);
      }
      this.#log('debug', `${this.type} gate: ${incoming} has no shard slot`);
      return incoming.reject(genericStatus(StatusCode.invalidArgument));
    }
    const count = this.client.slotTable(this.type)?.count;
    if (count !== undefined && slot >= count) {
      this.#log('debug', `${this.type} gate: ${incoming} outside the ${count} slots`);
      return incoming.reject(genericStatus(StatusCode.outOfRange));
    }
    const s = this.#slots.get(slot);
    switch (s?.state) {
      case undefined:
        return this.#rejectMoved(incoming, slot);
      case SlotGateState.loading:
      case SlotGateState.locked:
        this.#queue(s as GateSlot, incoming);
        return undefined;
      case SlotGateState.serving:
        this.#serve(s as GateSlot, incoming, slot);
        return undefined;
      case SlotGateState.forwarding:
        this.#forwardChannelOf(s as GateSlot, incoming);
        return undefined;
    }
  }

  #rejectMoved(incoming: IncomingChannel, slot: number): Promise<void> {
    const moved = this.movedTo(slot);
    this.#log('debug', `${this.type} gate: ${incoming} refused, moved: ${moved}`);
    return incoming.reject(moved.toStatus());
  }

  #queue(s: GateSlot, incoming: IncomingChannel): void {
    if (this.#queuedChannels >= this.maxQueuedChannels) {
      this.#log(
        'info',
        `${this.type} gate: ${incoming} refused, ${this.maxQueuedChannels} channels queued`,
      );
      void incoming.reject(genericStatus(StatusCode.unavailable));
      return;
    }
    this.#queuedChannels++;
    s.channels.push(incoming);
    // A queued channel its peer gives up on leaves the queue.
    void incoming.channel.done.then(() => {
      const at = s.channels.indexOf(incoming);
      if (at >= 0) {
        s.channels.splice(at, 1);
        this.#queuedChannels--;
      }
    });
  }

  #takeQueue(s: GateSlot): IncomingChannel[] {
    const queued = s.channels.filter((incoming) => incoming.channel.canSend);
    this.#queuedChannels -= s.channels.length;
    s.channels.length = 0;
    return queued;
  }

  #serve(s: GateSlot, incoming: IncomingChannel, slot: number): void {
    this.#servedBy.set(incoming, s);
    if (this.trackChannels) {
      s.tracked.add(incoming);
      void incoming.channel.done.then(() => {
        if (s.tracked.delete(incoming)) {
          this.#checkIdle(s);
        }
      });
    }
    let served: unknown;
    try {
      served = this.lifecycle.serve(incoming, slot);
    } catch (e) {
      this.#serveFailed(incoming, e);
      return;
    }
    if (isPromiseLike(served)) {
      served.then(
        () => {},
        (e: unknown) => this.#serveFailed(incoming, e),
      );
    }
  }

  #serveFailed(incoming: IncomingChannel, error: unknown): void {
    this.#log('error', `${this.type} gate: serving ${incoming} failed`, error);
    void incoming.reject(Status.of(StatusCode.internal, 'handler failed'));
  }

  /**
   * Pipes [incoming] to the new owner of [s] with its open payload, the
   * instance set to the new owner's. The outgoing channels are opened one
   * after the other, so the new owner sees them in arrival order.
   */
  #forwardChannelOf(s: GateSlot, incoming: IncomingChannel): void {
    if (this.#queuedChannels >= this.maxQueuedChannels) {
      void incoming.reject(genericStatus(StatusCode.unavailable));
      return;
    }
    this.#queuedChannels++;
    const to = s.to;
    s.opening = s.opening.then(async () => {
      try {
        if (!incoming.channel.canSend) {
          return;
        }
        const target = await this.#open(
          to,
          incoming.address.with({ instance: to, clearHost: true }),
        );
        this.#log('debug', `${this.type} gate: ${incoming} forwarded to ${this.type}/${to}`);
        void pipeChannels(incoming.channel, target);
      } catch (e) {
        this.#log('info', `${this.type} gate: cannot forward ${incoming} to ${to}: ${e}`);
        void incoming.reject(genericStatus(StatusCode.unavailable));
      } finally {
        this.#queuedChannels--;
      }
    });
  }

  /**
   * Opens a channel to instance [to] of {@link type} with [header], never to
   * this node's own listeners (a record of [to] that points back here would
   * dispatch the channel into this gate again).
   */
  async #open(to: number, header: ChannelAddress): Promise<MuxChannel> {
    const bytes = header.encode();
    for (let attempt = 0; ; attempt++) {
      const { connection } = await this.switchboard.selectAndConnect(
        new ServiceAddress(this.type, to),
        { excludeOwnEndpoints: true },
      );
      try {
        return connection.open(bytes);
      } catch (e) {
        // A pooled connection may have received GOAWAY, or filled up, in
        // the meantime; one retry replaces it.
        if (
          attempt > 0 ||
          !(e instanceof SwitchboardError) ||
          !(e.is(StatusCode.failedPrecondition) || e.is(StatusCode.resourceExhausted))
        ) {
          throw e;
        }
      }
    }
  }

  // Requests ----------------------------------------------------------------

  /**
   * Gates [message], a request (or plain message) about [slot] received on
   * a long-lived channel, by the state of [slot]:
   *
   * - served here: runs [handler], counting it as work in flight of the
   *   slot until its promise settles (a throw is logged and answered
   *   `ABORT INTERNAL` if still unanswered);
   * - being loaded, or locked by `DRAIN`: queued (at most
   *   {@link maxQueuedRequests} over all slots, beyond which
   *   `ABORT UNAVAILABLE`); run here on `RESUME`, forwarded on `FORWARD`;
   * - handed over, during {@link forwardGrace}: forwarded to the new owner
   *   as a message chain (`forwardMessage`) over a channel this gate opens
   *   to `(type, new owner)` with the slot and the payload as the
   *   application payload, so that the new owner sees the caller's
   *   credential and serves the request as it would on any channel to the
   *   slot. The requests for the slot with the same payload share that
   *   channel, which is closed after a second without a request in flight
   *   and opened again for the next one;
   * - a slot outside the space: answered `ABORT OUT_OF_RANGE`;
   * - otherwise: answered `ABORT MOVED` ({@link movedStatus}).
   *
   * [options.payload] defaults to the application payload of the OPEN of
   * the channel [message] arrived on (`ChannelAddress.payload`): the
   * credential its peer presented, passed on unchanged (an empty one stays
   * empty, the node's `Switchboard.defaultPayload` is never used). For a
   * channel this node opened, that is the payload this node sent; for a
   * Talk channel that is not over a mux channel, it is empty. Pass a payload
   * when the request's caller is not that channel's peer: an application
   * that relays requests it received elsewhere, or one whose callers
   * authenticate inside the channel.
   *
   * The promise resolves once the request has been handled, forwarded or
   * refused; it never rejects. A queued request that can no longer be
   * answered (cancelled, or its channel closed) leaves the queue and is
   * dropped.
   */
  async serveRequest(
    message: TalkMessage,
    slot: number,
    handler: (message: TalkMessage) => unknown,
    options: { payload?: Uint8Array | undefined } = {},
  ): Promise<void> {
    const s = this.#slots.get(slot);
    switch (s?.state) {
      case undefined: {
        const count = this.client.slotTable(this.type)?.count;
        if (count !== undefined && slot >= count) {
          this.#log('debug', `${this.type} gate: request for slot ${slot} outside ${count}`);
          this.#abort(message, genericStatus(StatusCode.outOfRange));
          return;
        }
        this.#abort(message, this.movedStatus(slot));
        return;
      }
      case SlotGateState.loading:
      case SlotGateState.locked: {
        const gateSlot = s as GateSlot;
        if (this.#queuedRequests >= this.maxQueuedRequests) {
          this.#log('info', `${this.type} gate: request refused, ${this.maxQueuedRequests} queued`);
          this.#abort(message, genericStatus(StatusCode.unavailable));
          return;
        }
        const parked = new Parked(message, handler, options.payload);
        this.#queuedRequests++;
        gateSlot.requests.push(parked);
        // A request that can no longer be answered leaves the queue.
        const gone: Promise<unknown> = message.expectsReply
          ? message.onCancel
          : message.channel.done;
        void gone.then(() => {
          const at = gateSlot.requests.indexOf(parked);
          if (at >= 0) {
            gateSlot.requests.splice(at, 1);
            this.#queuedRequests--;
            parked.finish();
          }
        });
        await parked.done.promise;
        return;
      }
      case SlotGateState.serving:
        await this.#run(s as GateSlot, message, handler);
        return;
      case SlotGateState.forwarding:
        await this.#forwardRequest(s as GateSlot, slot, message, options.payload);
        return;
    }
  }

  async #run(
    s: GateSlot,
    message: TalkMessage,
    handler: (message: TalkMessage) => unknown,
  ): Promise<void> {
    s.work++;
    try {
      await handler(message);
    } catch (e) {
      this.#log('error', `${this.type} gate: request handler failed`, e);
      this.#abort(message, Status.of(StatusCode.internal, 'handler failed'));
    } finally {
      s.work--;
      this.#checkIdle(s);
    }
  }

  /**
   * Forwards [message] to the new owner of [s] over the forwarding channel
   * of [payload], by default the payload of the channel [message] arrived
   * on.
   */
  async #forwardRequest(
    s: GateSlot,
    slot: number,
    message: TalkMessage,
    payload: Uint8Array | undefined,
  ): Promise<void> {
    if (message.expectsReply && !message.canReply) {
      return;
    }
    if (this.#queuedRequests >= this.maxQueuedRequests) {
      this.#abort(message, genericStatus(StatusCode.unavailable));
      return;
    }
    this.#queuedRequests++;
    const credential = payload ?? this.#arrivalPayload(message);
    // One forwarding channel per distinct credential: the bytes as the key.
    const key = String.fromCharCode(...credential);
    let f = s.forwards.get(key);
    if (f === undefined) {
      f = new Forward(key, credential.slice());
      s.forwards.set(key, f);
    }
    if (f.idle !== undefined) {
      this.clock.clearTimeout(f.idle);
      f.idle = undefined;
    }
    f.pending++;
    let target: TalkChannel;
    try {
      target = await this.#forwardChannel(s, slot, f);
    } catch (e) {
      this.#log('info', `${this.type} gate: cannot forward a request for ${slot}: ${e}`);
      this.#abort(message, genericStatus(StatusCode.unavailable));
      this.#relayEnded(s, f);
      return;
    } finally {
      this.#queuedRequests--;
    }
    try {
      await forwardMessage(message, target);
    } catch (e) {
      // The application forwarded it already: nothing more to do.
      this.#log('error', `${this.type} gate: forwarding a request for ${slot} failed`, e);
    } finally {
      this.#relayEnded(s, f);
    }
  }

  /**
   * The application payload of the OPEN of the channel [message] arrived on,
   * decoded once per channel; empty when that is not a mux channel (or a
   * slot channel carrying one).
   */
  #arrivalPayload(message: TalkMessage): Uint8Array {
    const channel = message.channel;
    let payload = this.#payloads.get(channel);
    if (payload === undefined) {
      payload = openPayloadOf(channel.raw);
      this.#payloads.set(channel, payload);
    }
    return payload;
  }

  /**
   * A forwarded request through [f] ended: once none is in flight for a
   * second, the channel is closed (the next request opens another). Once
   * [s] was retired, it is closed with its last request.
   */
  #relayEnded(s: GateSlot, f: Forward): void {
    if (--f.pending > 0) {
      return;
    }
    if (s.forwards.get(f.key) !== f) {
      closeForward(f);
      return;
    }
    if (f.idle !== undefined) {
      this.clock.clearTimeout(f.idle);
    }
    f.idle = this.clock.setTimeout(() => {
      f.idle = undefined;
      if (f.pending > 0 || s.forwards.get(f.key) !== f) {
        return;
      }
      s.forwards.delete(f.key);
      this.#log('debug', `${this.type} gate: forwarding channel idle, closed`);
      closeForward(f);
    }, forwardIdle);
  }

  /** The channel of [f], opened on first use and again after it closed. */
  #forwardChannel(s: GateSlot, slot: number, f: Forward): Promise<TalkChannel> {
    const current = f.channel;
    if (current === undefined) {
      return this.#openForward(s, slot, f);
    }
    // Requests that find it closed together open one replacement.
    const again = (): Promise<TalkChannel> =>
      f.channel === current ? this.#openForward(s, slot, f) : this.#forwardChannel(s, slot, f);
    return current.then(
      (channel) => (channel.isOpen ? channel : again()),
      // A failed open is retried by the next request.
      () => again(),
    );
  }

  /** Opens the channel of [f] to the new owner of [s], addressed to [slot] with [f]'s credential as the application payload. */
  #openForward(s: GateSlot, slot: number, f: Forward): Promise<TalkChannel> {
    const opening = this.#open(
      s.to,
      new ChannelAddress({ type: this.type, instance: s.to, shard: slot, payload: f.payload }),
    ).then((channel) => new TalkChannel(channel, this.switchboard.talkOptions));
    f.channel = opening;
    opening.catch(() => {});
    return opening;
  }

  #abort(message: TalkMessage, status: Status): void {
    if (!message.canReply) {
      return;
    }
    try {
      void message.replyAbort(status).catch(() => {});
    } catch (e) {
      this.#log('debug', `abort not sent: ${e instanceof SwitchboardError ? e.status : String(e)}`);
    }
  }

  // Work in flight ----------------------------------------------------------

  #checkIdle(s: GateSlot): void {
    const idle = s.idle;
    if (idle !== undefined && s.isIdle) {
      idle.resolve();
    }
  }

  /**
   * Waits until [s] has no work in flight, at most {@link drainTimeout}, or
   * until [context] is cancelled. When it has to wait, it declares the wait
   * to the naming service through [context], a second longer so that the
   * `DRAIN` outlives it, and once the wait is over restarts the naming
   * service's default timeout for `SlotLifecycle.drain`. Resolves with false
   * when the wait timed out.
   */
  async #waitIdle(
    s: GateSlot,
    slot: number,
    context: SlotRequestContext | undefined,
  ): Promise<boolean> {
    if (s.isIdle) {
      return true;
    }
    context?.extend({ deadline: this.drainTimeout + drainStart });
    const idle = new Deferred<void>();
    s.idle = idle;
    let timer: TimerHandle | undefined;
    const timedOut = new Promise<false>((resolve) => {
      timer = this.clock.setTimeout(() => resolve(false), this.drainTimeout);
    });
    try {
      const waits: Promise<unknown>[] = [idle.promise, timedOut];
      if (context !== undefined) {
        waits.push(context.onCancel);
      }
      if ((await Promise.race(waits)) === false) {
        this.#log(
          'warn',
          `${this.type} gate: slot ${slot} still has ${s.tracked.size} channels and ` +
            `${s.work} requests in flight after ${this.drainTimeout} ms; draining anyway`,
        );
        return false;
      }
      return true;
    } finally {
      if (timer !== undefined) {
        this.clock.clearTimeout(timer);
      }
      if (s.idle === idle) {
        s.idle = undefined;
      }
      if (context !== undefined && !context.isCancelled) {
        context.extend();
      }
    }
  }

  // SlotHandler -------------------------------------------------------------

  /**
   * `ASSIGN`: loads the slot with `SlotLifecycle.load` (queueing its
   * channels meanwhile), then serves it and its queue. For a slot already
   * served (a claim confirmed after a naming service restart) only the
   * epoch changes.
   *
   * A slot still locked whose `RESUME` was lost (the naming service set it
   * free after its retries, then assigned it back here) is unlocked as on
   * `RESUME`, with the new epoch, when no other instance owned it in
   * between: the epoch is the next one after the epoch it was locked with,
   * since only an assignment increments it. Its queue is served here and
   * the answer is that of its load.
   *
   * A slot that another instance owned since (a locked slot with a later
   * epoch, or a slot still being forwarded after a migration, which comes
   * back) has stale state here: its queue is refused with `MOVED`,
   * forwarding stops, `SlotLifecycle.unload` runs, then
   * `SlotLifecycle.load`, which receives [context]. Refused with
   * `UNAVAILABLE` after {@link close}, and while the lifecycle still runs a
   * load or unload of the slot, even for a generation that was revoked, or
   * for a closed gate it was attached to before (see `SlotLifecycle.load`).
   * A cancelled [context] never admits queued work: a load that finishes
   * after cancellation is unloaded, its queue refused with `MOVED`, and the
   * assignment fails with `CANCELLED`.
   */
  async onAssign(request: AssignRequest, context?: SlotRequestContext): Promise<AssignResult> {
    this.#checkType(request.type);
    const slot = request.slot;
    if (busyOf(this.lifecycle).has(slot)) {
      throw SwitchboardError.of(
        StatusCode.unavailable,
        `slot ${this.type}/${slot} is still loading or unloading`,
      );
    }
    this.#enterBusy(slot);
    try {
      return await this.#assign(request, context);
    } finally {
      this.#exitBusy(slot);
    }
  }

  #enterBusy(slot: number): void {
    const busy = busyOf(this.lifecycle);
    busy.set(slot, (busy.get(slot) ?? 0) + 1);
  }

  #exitBusy(slot: number): void {
    const busy = busyOf(this.lifecycle);
    const left = (busy.get(slot) ?? 1) - 1;
    if (left === 0) {
      busy.delete(slot);
    } else {
      busy.set(slot, left);
    }
  }

  async #assign(
    request: AssignRequest,
    context: SlotRequestContext | undefined,
  ): Promise<AssignResult> {
    const slot = request.slot;
    if (this.#closed) {
      throw SwitchboardError.of(StatusCode.unavailable, `${this.type} gate closed`);
    }
    if (context?.isCancelled ?? false) {
      throw SwitchboardError.of(StatusCode.cancelled, 'ASSIGN cancelled');
    }
    const existing = this.#slots.get(slot);
    switch (existing?.state) {
      case SlotGateState.serving: {
        const served = existing as GateSlot;
        served.epoch = request.epoch;
        return served.result;
      }
      case SlotGateState.locked: {
        const locked = existing as GateSlot;
        if (request.epoch === nextEpoch(locked.epoch)) {
          this.#log(
            'info',
            `${this.type} gate: slot ${slot} assigned back while locked (RESUME lost), ` +
              `serving it again, epoch ${request.epoch}`,
          );
          this.#unlock(locked, slot, request.epoch);
          return locked.result;
        }
        break;
      }
      case SlotGateState.loading:
        throw SwitchboardError.of(
          StatusCode.unavailable,
          `slot ${this.type}/${slot} is already being loaded`,
        );
      default:
        break;
    }
    const s = new GateSlot(request.epoch);
    this.#slots.set(slot, s);
    if (existing !== undefined) {
      // Owned by another instance since it was locked or handed over: its
      // state is stale, its queue (never read) refused with MOVED.
      await this.#retire(existing, slot);
      if (this.#slots.get(slot) !== s) {
        throw SwitchboardError.of(
          StatusCode.unavailable,
          `slot ${this.type}/${slot} stopped before loading`,
        );
      }
    }
    if (context?.isCancelled ?? false) {
      this.#slots.delete(slot);
      this.#refuseQueue(s, this.movedStatus(slot));
      throw SwitchboardError.of(StatusCode.cancelled, 'ASSIGN cancelled');
    }
    let result: AssignResult;
    try {
      result = await this.lifecycle.load(slot, {
        epoch: request.epoch,
        holder: request.holder,
        shared: request.shared,
        context,
      });
    } catch (e) {
      if (this.#slots.get(slot) === s) {
        this.#slots.delete(slot);
        this.#refuseQueue(s, this.movedStatus(slot));
      }
      throw e;
    }
    const cancelled = context?.isCancelled ?? false;
    if (this.#slots.get(slot) !== s || cancelled) {
      // Stopped while loading (closed, revoked or cancelled). Do not
      // release the queue before the naming client notices cancellation.
      if (this.#slots.get(slot) === s) {
        this.#slots.delete(slot);
        this.#refuseQueue(s, this.movedStatus(slot));
      }
      await this.#unload(slot);
      throw SwitchboardError.of(
        cancelled ? StatusCode.cancelled : StatusCode.unavailable,
        `slot ${this.type}/${slot} stopped while loading`,
      );
    }
    s.loaded = true;
    s.result = result;
    s.state = SlotGateState.serving;
    this.#log('debug', `${this.type} gate: serving slot ${slot}, epoch ${request.epoch}`);
    this.#release(s, slot);
    return result;
  }

  /**
   * `DRAIN`: locks the slot, waits for its work in flight (at most
   * {@link drainTimeout}; when there is work to wait for, the wait is
   * declared to the naming service through [context], a second longer, and
   * its default timeout restarted once the wait is over; after it the
   * slot's tracked channels still open are closed with `RELOCATED` naming
   * the new owner and epoch), then runs `SlotLifecycle.drain`, which
   * receives [context]. Rejects with `FAILED_PRECONDITION` for a slot not
   * served here, and for a new owner that is no instance (0) or this one,
   * leaving the slot served. A [context] cancelled while it waits (the
   * naming service gave the `DRAIN` up, or the channel to it was lost) ends
   * the wait and rejects with `CANCELLED`, leaving the slot locked until
   * `RESUME` with nothing closed or drained.
   */
  async onDrain(request: DrainRequest, context?: SlotRequestContext): Promise<void> {
    this.#checkType(request.type);
    const slot = request.slot;
    const selfForward = this.#selfForward('DRAIN', slot, request.to);
    if (selfForward !== undefined) {
      throw selfForward;
    }
    const s = this.#slots.get(slot);
    if (
      s === undefined ||
      (s.state !== SlotGateState.serving && s.state !== SlotGateState.locked)
    ) {
      throw SwitchboardError.of(
        StatusCode.failedPrecondition,
        `slot ${this.type}/${slot} is not served here`,
      );
    }
    s.state = SlotGateState.locked;
    s.to = request.to;
    this.#log('debug', `${this.type} gate: slot ${slot} locked for ${request.to}`);
    const idle = await this.#waitIdle(s, slot, context);
    if (this.#slots.get(slot) !== s || s.state !== SlotGateState.locked) {
      throw SwitchboardError.of(
        StatusCode.unavailable,
        `slot ${this.type}/${slot} stopped while draining`,
      );
    }
    if (context?.isCancelled ?? false) {
      // Rolled back (RESUME follows): nothing is handed over for it.
      throw SwitchboardError.of(StatusCode.cancelled, 'DRAIN cancelled');
    }
    if (!idle) {
      this.#relocateTracked(s, slot, request.to, request.epoch);
    }
    await this.lifecycle.drain(slot, { epoch: request.epoch, to: request.to, context });
  }

  /**
   * The wait for the work in flight of [slot] timed out during `DRAIN`: the
   * tracked channels still open are closed with `RELOCATED` naming [to] at
   * [epoch], so that nothing more is served here on state that is being
   * handed over. Running {@link serveRequest} handlers cannot be stopped;
   * they are logged.
   */
  #relocateTracked(s: GateSlot, slot: number, to: number, epoch: number): void {
    const relocated = new MovedStatus({ owner: to, epoch }).toStatus({ relocated: true });
    const channels = [...s.tracked];
    s.tracked.clear();
    if (channels.length > 0) {
      this.#log(
        'warn',
        `${this.type} gate: slot ${slot}: ${channels.length} channels still open ` +
          `after ${this.drainTimeout} ms closed with RELOCATED`,
      );
    }
    for (const incoming of channels) {
      void incoming.reject(relocated);
    }
    if (s.work > 0) {
      this.#log(
        'warn',
        `${this.type} gate: slot ${slot}: ${s.work} requests still running after ` +
          `${this.drainTimeout} ms cannot be stopped; they may act on state being handed over`,
      );
    }
  }

  /**
   * `FORWARD`: pipes the queued channels to the new owner in arrival order,
   * forwards the queued requests, and keeps forwarding for
   * {@link forwardGrace}; then `SlotLifecycle.unload`.
   *
   * A new owner that is no instance (0) or this one rejects with
   * `FAILED_PRECONDITION`: forwarding there would bring the slot's channels
   * back into this gate. The slot, which the naming client dropped on
   * `FORWARD`, then stops being served here as on revocation: its queue is
   * refused with `MOVED`, its tracked channels are closed with `RELOCATED`,
   * and `SlotLifecycle.unload` runs (a slot already being forwarded goes on
   * as it was).
   */
  async onForward(request: ForwardRequest, context?: SlotRequestContext): Promise<void> {
    void context;
    this.#checkType(request.type);
    const slot = request.slot;
    const s = this.#slots.get(slot);
    const selfForward = this.#selfForward('FORWARD', slot, request.to);
    if (selfForward !== undefined) {
      if (s !== undefined && s.state !== SlotGateState.forwarding) {
        await this.#stop(slot);
      }
      throw selfForward;
    }
    if (s === undefined) {
      return;
    }
    s.state = SlotGateState.forwarding;
    s.to = request.to;
    s.epoch = request.epoch;
    const channels = this.#takeQueue(s);
    const requests = this.#takeRequests(s);
    this.#log(
      'debug',
      `${this.type} gate: slot ${slot} handed to ${request.to}, forwarding ` +
        `${channels.length} channels and ${requests.length} requests`,
    );
    s.grace = this.clock.setTimeout(() => this.#graceOver(s, slot), this.forwardGrace);
    for (const incoming of channels) {
      this.#forwardChannelOf(s, incoming);
    }
    for (const parked of requests) {
      void this.#forwardRequest(s, slot, parked.message, parked.payload).finally(() =>
        parked.finish(),
      );
    }
    // Answered once the queued channels are handed over.
    await s.opening;
  }

  /** `RESUME`: unlocks the slot and serves its queue here. Does nothing for a slot that is not locked. */
  async onResume(request: ResumeRequest, context?: SlotRequestContext): Promise<void> {
    void context;
    this.#checkType(request.type);
    const slot = request.slot;
    const s = this.#slots.get(slot);
    if (s === undefined || s.state !== SlotGateState.locked) {
      return;
    }
    this.#log('debug', `${this.type} gate: slot ${slot} resumed`);
    this.#unlock(s, slot, request.epoch);
  }

  /** Unlocks the locked slot [s] at [epoch] and serves its queue here. */
  #unlock(s: GateSlot, slot: number, epoch: number): void {
    s.state = SlotGateState.serving;
    s.to = 0;
    s.epoch = epoch;
    this.#release(s, slot);
  }

  /** The slot was revoked: stops serving it (see {@link release}). */
  async onRevoke(type: Name, slot: number): Promise<void> {
    this.#checkType(type);
    this.#log('info', `${this.type} gate: slot ${slot} revoked`);
    await this.#stop(slot);
  }

  /** A `HOLDING` response listed [slots] as no longer held here: passes them to `SlotLifecycle.discard`, whose failure is logged. */
  onDiscard(type: Name, slots: readonly number[]): void {
    this.#checkType(type);
    this.#log('info', `${this.type} gate: discarding ${slots.length} slots`);
    let discarded: unknown;
    try {
      discarded = this.lifecycle.discard(slots);
    } catch (e) {
      this.#discardFailed(slots, e);
      return;
    }
    if (isPromiseLike(discarded)) {
      discarded.then(
        () => {},
        (e: unknown) => this.#discardFailed(slots, e),
      );
    }
  }

  #discardFailed(slots: readonly number[], error: unknown): void {
    this.#log('warn', `${this.type} gate: discarding slots ${slots.join(', ')} failed`, error);
  }

  /** The error for a [procedure] of [slot] whose new owner [to] is no instance or this one (logged), else undefined. */
  #selfForward(procedure: string, slot: number, to: number): SwitchboardError | undefined {
    if (to !== 0 && to !== this.instance) {
      return undefined;
    }
    const owner = to === 0 ? 'no instance' : 'this instance';
    this.#log('warn', `${this.type} gate: ${procedure} of slot ${slot} to ${owner} refused`);
    return SwitchboardError.of(
      StatusCode.failedPrecondition,
      `${procedure} of ${this.type}/${slot} names ${owner} as the new owner`,
    );
  }

  #checkType(requested: Name): void {
    if (!requested.equals(this.type)) {
      throw SwitchboardError.of(
        StatusCode.unavailable,
        `slot request for ${requested} reached the gate of ${this.type}`,
      );
    }
  }

  /** Serves what was queued while loading or locked. */
  #release(s: GateSlot, slot: number): void {
    const serving = (): boolean => this.#slots.get(slot) === s && s.state === SlotGateState.serving;
    // Remove one at a time: application handlers may lock or revoke the
    // slot synchronously, and the remaining queue belongs to that state.
    while (serving() && s.channels.length > 0) {
      const incoming = s.channels.shift() as IncomingChannel;
      this.#queuedChannels--;
      if (incoming.channel.canSend) {
        this.#serve(s, incoming, slot);
      }
    }
    while (serving() && s.requests.length > 0) {
      const parked = s.requests.shift() as Parked;
      this.#queuedRequests--;
      if (parked.message.expectsReply && !parked.message.canReply) {
        parked.finish();
        continue;
      }
      void this.#run(s, parked.message, parked.handler).finally(() => parked.finish());
    }
  }

  #takeRequests(s: GateSlot): Parked[] {
    const parked = s.requests.splice(0);
    this.#queuedRequests -= parked.length;
    return parked;
  }

  /** Refuses the queue of [s], which is no longer served here, with [moved]: nothing in it was processed. */
  #refuseQueue(s: GateSlot, moved: Status): void {
    for (const incoming of this.#takeQueue(s)) {
      void incoming.reject(moved);
    }
    for (const parked of this.#takeRequests(s)) {
      this.#abort(parked.message, moved);
      parked.finish();
    }
  }

  #graceOver(s: GateSlot, slot: number): void {
    if (this.#slots.get(slot) !== s) {
      return;
    }
    this.#slots.delete(slot);
    this.#log('debug', `${this.type} gate: forwarding of slot ${slot} over`);
    void this.#retire(s, slot);
  }

  /**
   * Ends what is left of [s] after it stopped being served: refuses its
   * queue with `MOVED`, closes its tracked channels, which were served, with
   * `RELOCATED`, closes each forwarding channel once its requests are
   * answered, and unloads the slot.
   */
  async #retire(s: GateSlot, slot: number): Promise<void> {
    if (s.grace !== undefined) {
      this.clock.clearTimeout(s.grace);
      s.grace = undefined;
    }
    s.idle?.resolve();
    // [s] is gone from the table, unless the slot came back (an ASSIGN
    // during the grace period): then the mirror tells.
    const moved = this.#movedTo(slot, this.#slots.has(slot) ? undefined : s);
    this.#refuseQueue(s, moved.toStatus());
    const relocated = moved.toStatus({ relocated: true });
    for (const incoming of [...s.tracked]) {
      void incoming.reject(relocated);
    }
    s.tracked.clear();
    // Out of the table, each closes with its last request (#relayEnded).
    const forwards = [...s.forwards.values()];
    s.forwards.clear();
    for (const f of forwards) {
      if (f.idle !== undefined) {
        this.clock.clearTimeout(f.idle);
        f.idle = undefined;
      }
      if (f.pending === 0) {
        closeForward(f);
      }
    }
    if (s.loaded) {
      s.loaded = false;
      await this.#unload(slot);
    }
  }

  async #unload(slot: number): Promise<void> {
    this.#enterBusy(slot);
    try {
      await this.lifecycle.unload(slot);
    } catch (e) {
      this.#log('warn', `${this.type} gate: unloading slot ${slot} failed`, e);
    } finally {
      this.#exitBusy(slot);
    }
  }

  async #stop(slot: number): Promise<void> {
    const s = this.#slots.get(slot);
    if (s !== undefined) {
      this.#slots.delete(slot);
      await this.#retire(s, slot);
    }
  }

  // Claims and release ------------------------------------------------------

  /** Takes the free [slot] (`CLAIM`, see `NamingClient.claim`) and resolves with its epoch; the slot is loaded through `ASSIGN` before this resolves. */
  claim(slot: number, options: { holding?: boolean } = {}): Promise<number> {
    return this.client.claim(this.type, slot, options);
  }

  /**
   * Gives up [slot] (`RELEASE`, see `NamingClient.release`), then stops
   * serving it here: its queue is refused with `MOVED`, its tracked
   * channels are closed with `RELOCATED` ({@link relocatedStatus}), and
   * `SlotLifecycle.unload` runs.
   */
  async release(slot: number, options: { keepStorage?: boolean } = {}): Promise<void> {
    await this.client.release(this.type, slot, options);
    await this.#stop(slot);
  }

  /**
   * Stops serving every slot here, as {@link release} does locally (the
   * naming service is not told), and refuses later `ASSIGN`s. Channels
   * arriving later are refused with `MOVED`, requests through
   * {@link serveRequest} answered `ABORT MOVED`. The lifecycle may then be
   * attached to a new gate (its `SlotLifecycle.gate` is this one until
   * then); loads and unloads still running here keep their slots from it
   * until they end.
   */
  async close(): Promise<void> {
    this.#closed = true;
    await Promise.all([...this.#slots.keys()].map((slot) => this.#stop(slot)));
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }

  toString(): string {
    return `SlotGate(${this.type}/${this.instance}, ${this.#slots.size} slots)`;
  }
}

/** The application payload of the OPEN of [mux], a copy; empty without one, or when it is not a Switchboard address. */
function openPayloadOf(mux: MuxChannel | undefined): Uint8Array {
  if (mux === undefined) {
    return new Uint8Array(0);
  }
  try {
    return ChannelAddress.decode(mux.openPayload).payload.slice();
  } catch (e) {
    if (e instanceof ProtocolError) {
      // Not a Switchboard address (a raw mux channel): no credential.
      return new Uint8Array(0);
    }
    throw e;
  }
}

/** Closes the forwarding channel of [f], once open; never rejects. */
function closeForward(f: Forward): void {
  const channel = f.channel;
  f.channel = undefined;
  channel?.then(
    (talk) => talk.close().catch(() => {}),
    () => {},
  );
}

/**
 * The {@link SlotHandler} of a naming client that serves several sharded
 * types: dispatches each request to the {@link SlotGate} of its type.
 *
 * `ASSIGN` for a type without a gate is refused with `UNAVAILABLE` (the slot
 * stays free), `DRAIN` with `FAILED_PRECONDITION`; `FORWARD`, `RESUME`,
 * revocations and discards of such a type are ignored.
 */
export class SlotGates implements SlotHandler {
  readonly #gates = new Map<string, SlotGate>();

  /** The gates, by type key (`Name.key`); a read-only view. */
  get gates(): ReadonlyMap<string, SlotGate> {
    return this.#gates;
  }

  /** The gate of [type], or undefined. */
  get(type: NameLike): SlotGate | undefined {
    return this.#gates.get(toName(type).key);
  }

  /**
   * Adds [gate].
   *
   * @throws {Error} if its type already has one.
   */
  add(gate: SlotGate): void {
    if (this.#gates.has(gate.type.key)) {
      throw new Error(`${gate.type} already has a slot gate`);
    }
    this.#gates.set(gate.type.key, gate);
  }

  /** Removes the gate of [type] and returns it. */
  remove(type: NameLike): SlotGate | undefined {
    const key = toName(type).key;
    const gate = this.#gates.get(key);
    this.#gates.delete(key);
    return gate;
  }

  async onAssign(request: AssignRequest, context?: SlotRequestContext): Promise<AssignResult> {
    const gate = this.#gates.get(request.type.key);
    if (gate === undefined) {
      throw SwitchboardError.of(StatusCode.unavailable, `no slot gate for ${request.type}`);
    }
    return gate.onAssign(request, context);
  }

  async onDrain(request: DrainRequest, context?: SlotRequestContext): Promise<void> {
    const gate = this.#gates.get(request.type.key);
    if (gate === undefined) {
      throw SwitchboardError.of(StatusCode.failedPrecondition, `no slot gate for ${request.type}`);
    }
    return gate.onDrain(request, context);
  }

  async onForward(request: ForwardRequest, context?: SlotRequestContext): Promise<void> {
    await this.#gates.get(request.type.key)?.onForward(request, context);
  }

  async onResume(request: ResumeRequest, context?: SlotRequestContext): Promise<void> {
    await this.#gates.get(request.type.key)?.onResume(request, context);
  }

  async onRevoke(type: Name, slot: number): Promise<void> {
    await this.#gates.get(type.key)?.onRevoke(type, slot);
  }

  onDiscard(type: Name, slots: readonly number[]): void {
    this.#gates.get(type.key)?.onDiscard(type, slots);
  }
}
