/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import type { ChannelAddress } from '../address/index.ts';
import type { MuxChannel } from '../mux/contract.ts';
import type { Name } from '../name.ts';
import { MovedStatus, SlotMode } from '../naming/naming-protocol.ts';
import { isKnownStatusCode, Status, StatusCode, SwitchboardError } from '../status.ts';
import {
  type BulkTarget,
  heldBulkTimeout,
  isBulkChannel,
  lingerBulkTarget,
  markBulkChannel,
  maxHeldBulk,
  registerBulkTarget,
} from '../talk/index.ts';
import { TalkBulkOpen } from '../talk/talk-bulk.ts';
import { TalkFrame } from '../talk/talk-frame.ts';
import type { Clock, TimerHandle } from '../util/clock.ts';
import type { LogLevel } from '../util/logger.ts';
import type { ChannelHandler, IncomingChannel, Resolver, SlotResolver } from './contract.ts';
import { ForwardingBound, type ForwardTarget, openForwarded, routerLogName } from './forwarding.ts';
import { genericStatus } from './generic-status.ts';
import { isSlotResolver } from './resolver.ts';
import { type SlotCarrier, slotCarrier } from './slot-layer.ts';
import type { Switchboard } from './switchboard.ts';

/**
 * Channel proxying: forwards every subframe between [a] and [b], in order
 * and unchanged, and forwards the close.
 *
 * Flow control passes through hop by hop: both channels are switched to
 * `MuxChannel.manualCredit`, and a subframe received on one counts as
 * consumed, and its credit goes back to that side's peer, only once the
 * other side's `MuxChannel.send` has taken it (its promise resolved). So a
 * slow consumer behind one side slows the sender behind the other, and
 * what the proxy holds for each direction is bounded by the window it
 * grants the sending side plus the window the receiving side grants it.
 * The output tier (`MuxChannel.priority`) of each side is its own; set it
 * on the channels before or after the call.
 *
 * When one side's stream ends (its peer sent CLOSE, or its connection was
 * lost) the other side is closed with that side's end status.
 * CONNECTION_LOST, which is local only, is sent as UNAVAILABLE, and
 * application codes (256 and above), which mux CLOSE must not carry, as
 * UNKNOWN. A subframe that cannot be forwarded (it exceeds the frame limit
 * the receiving peer announced, or costs more than half the window it
 * grants: `MuxChannel.maxSubframeLength`) closes both sides with that
 * error, including when LIMITS arrives after the send began waiting.
 * Subframes are forwarded whole, so a proxy should not grant its clients
 * more (window, frame size) than its backends grant it; a side whose
 * `MuxChannel.priority` is `'bulk'` splits what it sends instead, which
 * suits a byte stream (a Talk bulk payload).
 *
 * Talk bulk payloads (wiki page "Polyverse Switchboard Proxying", step 4):
 * a `_bulk` channel the peer of one side opens for its channel is
 * forwarded as a bulk channel toward the other side, opened in order with
 * that side's frames (`MuxChannel.openAfter`) with the same bulk number
 * and the other side's channel as its parent, so that the `BULK` message
 * passes unchanged; the two bulk channels are piped the same way, in the
 * bulk output tier of both connections. A side that cannot take one (it
 * is closing) refuses it UNAVAILABLE. A bulk channel's graceful close is
 * forwarded once the last subframe forwarded before it went out, as a
 * Talk sender closes a payload once the window took it: the CLOSE would
 * wait behind them anyway, but its confirmation is timed from the call,
 * which would cut a payload whose reader is slower than that. A failed
 * parent cancels its bulk twins in both directions; a graceful close
 * waits for the transfers it follows, then cancels any twins left when the
 * pipe ends. A drain that times out is forwarded as DEADLINE_EXCEEDED.
 * From a side whose bulk channels are independent streams
 * (`MuxConnection.hasOrderedChannels` false), the bulk channels are
 * forwarded in the order of their `BULK` references, which the proxy
 * reads from the frames it forwards, so that a frame mux on the other side
 * gets them in increasing number; and a graceful close also waits for the
 * bulk channels referenced before it to arrive (wiki page "Polyverse
 * Switchboard QUIC and WebTransport Binding", section "Proxying").
 *
 * Both streams are read before this returns, so subframes that arrived
 * before the call (buffered by the channels) are forwarded too. Neither
 * `readable` may be locked already (read by another reader, or wrapped in
 * a Talk channel); if one is, both channels are closed with INTERNAL.
 * Resolves when both channels are done; never rejects.
 *
 * See the wiki page "Polyverse Switchboard Proxying", section "Channel
 * proxying".
 */
export function pipeChannels(a: MuxChannel, b: MuxChannel): Promise<void> {
  return pipe(new MuxEnd(a), new MuxEnd(b));
}

/**
 * {@link pipeChannels} for a forwarder that bounds the channels it
 * forwards per client connection: the bulk channels [a]'s peer opens count
 * toward [bound] for [a]'s connection while they are piped (beyond it they
 * are refused RESOURCE_EXHAUSTED). Used by the relay.
 *
 * @internal
 */
export function pipeChannelsWithin(
  a: MuxChannel,
  b: MuxChannel,
  bound: ForwardingBound,
): Promise<void> {
  return pipe(new MuxEnd(a), new MuxEnd(b), { bulkBound: bound });
}

/** A bulk transfer forwarded one way: the two bulk channels and the pipe between them. */
interface BulkTwin {
  readonly source: MuxChannel;
  readonly target: MuxChannel;
  readonly done: Promise<void>;
}

/** What {@link pipe} takes beside the ends. */
interface PipeOptions {
  /** Rewrites the status [a] is closed with when [b] ends. */
  readonly toA?: ((status: Status) => Status) | undefined;
  /** Counts the bulk channels [a]'s peer opens against [a]'s connection. */
  readonly bulkBound?: ForwardingBound | undefined;
  /** [a] and [b] are bulk channels, closed gracefully only once what was forwarded went out. */
  readonly bulk?: boolean | undefined;
}

/** {@link pipeChannels} over {@link PipeEnd}s. */
function pipe(a: PipeEnd, b: PipeEnd, options: PipeOptions = {}): Promise<void> {
  // The bulk transfers forwarded each way, which a graceful close of the
  // channel they belong to follows.
  const towardB = new Set<BulkTwin>();
  const towardA = new Set<BulkTwin>();
  const cancelAll = (): void => {
    cancelTwins(towardB);
    cancelTwins(towardA);
  };
  for (const end of [a, b]) {
    void end.done.then((status) => {
      if (!status.isOk) cancelAll();
    });
  }
  const bulk = options.bulk ?? false;
  try {
    // Before reading: a bulk channel may wait for its parent's target.
    const orderA = routeBulk(a, b, towardB, options.bulkBound);
    const orderB = routeBulk(b, a, towardA, undefined);
    forward(a, b, { twins: towardB, bulk, bulkOrder: orderA });
    forward(b, a, { rewrite: options.toA, twins: towardA, bulk, bulkOrder: orderB });
  } catch (e) {
    log(a, 'warn', `proxy: cannot pipe channels ${a.id} and ${b.id}`, e);
    const status = Status.of(StatusCode.internal, 'cannot pipe channels');
    closeEnd(a, status);
    closeEnd(b, status);
  }
  return Promise.all([a.done, b.done]).then(cancelAll);
}

function cancelTwins(twins: Set<BulkTwin>): void {
  for (const twin of [...twins]) {
    quietly(twin.source.close(genericStatus(StatusCode.cancelled)));
    quietly(twin.target.close(genericStatus(StatusCode.cancelled)));
  }
}

/** One side of a pipe: a mux channel, or a slot carrier (which may replace its mux channel once). */
interface PipeEnd {
  /** The mux channel the subframes travel on now. */
  readonly mux: MuxChannel;
  /** No retry any more (a slot carrier): something was forwarded on it. */
  commit(): void;
  readonly readable: ReadableStream<Uint8Array>;
  readonly canSend: boolean;
  send(subframe: Uint8Array): Promise<void>;
  setManualCredit(value: boolean): void;
  consumed(bytes: number): void;
  close(status: Status): Promise<void>;
  readonly done: Promise<Status>;
  readonly id: number;
}

class MuxEnd implements PipeEnd {
  readonly channel: MuxChannel;

  constructor(channel: MuxChannel) {
    this.channel = channel;
  }

  get mux(): MuxChannel {
    return this.channel;
  }
  commit(): void {}
  get readable(): ReadableStream<Uint8Array> {
    return this.channel.readable;
  }
  get canSend(): boolean {
    return this.channel.canSend;
  }
  send(subframe: Uint8Array): Promise<void> {
    return this.channel.send(subframe);
  }
  setManualCredit(value: boolean): void {
    this.channel.manualCredit = value;
  }
  consumed(bytes: number): void {
    this.channel.consumed(bytes);
  }
  close(status: Status): Promise<void> {
    return this.channel.close(status);
  }
  get done(): Promise<Status> {
    return this.channel.done;
  }
  get id(): number {
    return this.channel.id;
  }
}

class SlotEnd implements PipeEnd {
  readonly carrier: SlotCarrier;

  constructor(carrier: SlotCarrier) {
    this.carrier = carrier;
  }

  get mux(): MuxChannel {
    return this.carrier.channel;
  }
  commit(): void {
    this.carrier.commit();
  }
  get readable(): ReadableStream<Uint8Array> {
    return this.carrier.readable;
  }
  get canSend(): boolean {
    return this.carrier.canSend;
  }
  send(subframe: Uint8Array): Promise<void> {
    return this.carrier.send(subframe);
  }
  setManualCredit(value: boolean): void {
    this.carrier.manualCredit = value;
  }
  consumed(bytes: number): void {
    this.carrier.consumed(bytes);
  }
  close(status: Status): Promise<void> {
    return this.carrier.close(status);
  }
  get done(): Promise<Status> {
    return this.carrier.done;
  }
  get id(): number {
    return this.carrier.channel.id;
  }
}

/**
 * Forwards the bulk channels the peer of [from] opens for it toward [to]
 * (see {@link pipeChannels}). A slot carrier that replaces [from]'s mux
 * channel keeps the route.
 */
function routeBulk(
  from: PipeEnd,
  to: PipeEnd,
  twins: Set<BulkTwin>,
  bound: ForwardingBound | undefined,
): BulkOrder | undefined {
  const mux = from.mux;
  if (isBulkChannel(mux)) {
    // A bulk channel is never a parent.
    return undefined;
  }
  const order = new BulkOrder(
    from,
    (bulk, number) => forwardBulk(bulk, number, from, to, twins, bound),
    bound,
  );
  registerBulkTarget(mux, order.open);
  for (const end of [from, to]) {
    void end.done.then((status) => {
      if (!status.isOk) order.clear();
    });
  }
  void Promise.all([from.done, to.done]).then(() => order.clear());
  return order;
}

/**
 * The order in which the bulk channels the peer of [from] opens are
 * forwarded. Where [from]'s connection orders them with the parent's
 * frames (`MuxConnection.hasOrderedChannels`, the frame mux), each is
 * forwarded as it arrives, as the frames are. Where it does not (a stream
 * each), they may arrive in any order, before or after the `BULK` message
 * that references them, or after the parent ended; the references arrive
 * in order on the parent, and the bulk channels are forwarded in their
 * order, each once its reference passed or arrives, so that a frame mux on
 * the other side gets them in increasing order, as it requires (a
 * reference forwarded before its bulk channel is waited for there, as
 * dispatch may lag). Subframes pass through unchanged and at once;
 * references are read from them, and anything that is not a Talk frame is
 * ignored. Bounded per parent: `maxHeldBulk` bulk channels waiting for their
 * references and as many references waiting for their bulk channels, each
 * at most `heldBulkTimeout`; waiting bulk channels count toward [bound].
 */
class BulkOrder {
  readonly from: PipeEnd;
  readonly forward: BulkTarget;
  readonly bound: ForwardingBound | undefined;
  readonly #opens = new Map<number, { channel: MuxChannel; timer: TimerHandle }>();
  readonly #references = new Map<number, TimerHandle>();
  #highestReference = 0;
  #ended = false;

  /** [from] ended gracefully and {@link settle} waits. */
  #settling = false;

  /** Resolved once no reference waits for its bulk channel any more, for a graceful close waiting for them. */
  #settled: { promise: Promise<void>; resolve: () => void; done: boolean } | undefined;

  constructor(from: PipeEnd, forward: BulkTarget, bound: ForwardingBound | undefined) {
    this.from = from;
    this.forward = forward;
    this.bound = bound;
  }

  get #ordered(): boolean {
    return this.from.mux.connection.hasOrderedChannels;
  }

  get #clock(): Clock {
    return this.from.mux.connection.options.clock;
  }

  /** Takes bulk channel [number] the peer opened. */
  readonly open = (channel: MuxChannel, number: number): void => {
    if (this.#ordered) {
      this.forward(channel, number);
      return;
    }
    if (this.#ended) {
      quietly(channel.close(genericStatus(StatusCode.cancelled)));
      return;
    }
    if (
      this.#opens.has(number) ||
      (number <= this.#highestReference && !this.#references.has(number))
    ) {
      // After the parent's end, only a referenced one is admitted.
      quietly(
        channel.close(
          genericStatus(this.#settling ? StatusCode.failedPrecondition : StatusCode.cancelled),
        ),
      );
      return;
    }
    if (this.#opens.size >= maxHeldBulk) {
      quietly(channel.close(genericStatus(StatusCode.resourceExhausted)));
      return;
    }
    if (this.bound !== undefined && !this.bound.enter(channel.connection)) {
      quietly(channel.close(genericStatus(StatusCode.resourceExhausted)));
      return;
    }
    const timer = this.#clock.setTimeout(() => {
      this.#opens.delete(number);
      this.bound?.exit(channel.connection);
      quietly(channel.close(genericStatus(StatusCode.cancelled)));
    }, heldBulkTimeout);
    this.#opens.set(number, { channel, timer });
    this.#drain();
  };

  /** [bytes], a subframe from [from], is about to be forwarded. */
  observe(bytes: Uint8Array): void {
    if (this.#ended || this.#ordered) return;
    let frame: TalkFrame;
    try {
      frame = TalkFrame.decode(bytes);
    } catch {
      return; // Non-Talk subframes still pass through unchanged.
    }
    if (!frame.bulk) return;
    let number: number;
    try {
      number = frame.bulkReference.number;
    } catch {
      return;
    }
    if (number <= this.#highestReference) return;
    this.#highestReference = number;
    if (this.#references.size < maxHeldBulk) {
      this.#references.set(
        number,
        this.#clock.setTimeout(() => {
          this.#references.delete(number);
          this.#discardUnreferenced();
          this.#drain();
        }, heldBulkTimeout),
      );
    }
    this.#discardUnreferenced();
    this.#drain();
  }

  /**
   * [from] ended gracefully: the bulk channels its peer finished before its
   * close may still arrive, and go on being forwarded meanwhile
   * (`lingerBulkTarget`). Returns what resolves once no reference waits for its bulk
   * channel any more, or undefined when none does (the frame mux: never)
   * or [from]'s connection has too many such parents already. Bulk
   * channels no reference names will not be named any more: they are
   * closed CANCELLED.
   */
  settle(): Promise<void> | undefined {
    if (this.#ended || this.#ordered) return undefined;
    this.#settling = true;
    // Every reference arrived: nothing is above the last one.
    this.#highestReference = 0xffffffff;
    this.#discardUnreferenced();
    if (this.#references.size === 0) return undefined;
    if (this.#settled === undefined) {
      let resolve = (): void => {};
      const promise = new Promise<void>((r) => {
        resolve = r;
      });
      this.#settled = { promise, resolve, done: false };
    }
    const settled = this.#settled;
    if (!lingerBulkTarget(this.from.mux, this.open, settled.promise)) {
      this.clear();
      return undefined;
    }
    return settled.promise;
  }

  #discardUnreferenced(): void {
    for (const [number, { channel, timer }] of [...this.#opens]) {
      if (number <= this.#highestReference && !this.#references.has(number)) {
        this.#opens.delete(number);
        this.#clock.clearTimeout(timer);
        this.bound?.exit(channel.connection);
        quietly(channel.close(genericStatus(StatusCode.cancelled)));
      }
    }
    this.#checkSettled();
  }

  #drain(): void {
    for (const [number, reference] of this.#references) {
      const open = this.#opens.get(number);
      if (open === undefined) break;
      this.#opens.delete(number);
      this.#references.delete(number);
      this.#clock.clearTimeout(reference);
      this.#clock.clearTimeout(open.timer);
      this.bound?.exit(open.channel.connection);
      this.forward(open.channel, number);
    }
    this.#checkSettled();
  }

  #checkSettled(): void {
    const settled = this.#settled;
    if (settled !== undefined && !settled.done && this.#references.size === 0) {
      settled.done = true;
      settled.resolve();
    }
  }

  clear(): void {
    this.#ended = true;
    for (const { channel, timer } of this.#opens.values()) {
      this.#clock.clearTimeout(timer);
      this.bound?.exit(channel.connection);
      quietly(channel.close(genericStatus(StatusCode.cancelled)));
    }
    this.#opens.clear();
    for (const timer of this.#references.values()) {
      this.#clock.clearTimeout(timer);
    }
    this.#references.clear();
    this.#checkSettled();
  }
}

/** Opens the twin of [bulk] (bulk number [number]) on [to]'s side, in order with its frames, and pipes the two. */
function forwardBulk(
  bulk: MuxChannel,
  number: number,
  from: PipeEnd,
  to: PipeEnd,
  twins: Set<BulkTwin>,
  bound: ForwardingBound | undefined,
): void {
  bulk.priority = 'bulk';
  const client = bulk.connection;
  if (bound !== undefined && !bound.enter(client)) {
    log(
      from,
      'info',
      `proxy: bulk channel ${bulk.id} refused, ${bound.countOf(client)} channels of this ` +
        'connection are being forwarded',
    );
    quietly(bulk.close(genericStatus(StatusCode.resourceExhausted)));
    return;
  }
  const parent = to.mux;
  let twin: MuxChannel;
  try {
    twin = parent.openAfter(new TalkBulkOpen(parent.id, number).encode());
  } catch (e) {
    bound?.exit(client);
    log(from, 'debug', `proxy: bulk channel ${bulk.id} not forwarded to channel ${parent.id}`, e);
    quietly(bulk.close(genericStatus(StatusCode.unavailable)));
    return;
  }
  // The message referencing it follows on the slot carrier: no retry
  // elsewhere may separate them, whichever side opened the bulk channel.
  from.commit();
  to.commit();
  twin.priority = 'bulk';
  markBulkChannel(twin);
  const piped = pipe(new MuxEnd(bulk), new MuxEnd(twin), { bulk: true });
  const transfer: BulkTwin = { source: bulk, target: twin, done: piped };
  twins.add(transfer);
  void piped.then(() => {
    twins.delete(transfer);
    bound?.exit(client);
  });
}

/**
 * How long a graceful CLOSE forwarded toward [to] waits for the bulk
 * payloads forwarded before it: the close confirmation timeout of [to]'s
 * connection (30 s when that waits for ever).
 */
function twinWait(to: PipeEnd): number {
  const timeout = to.mux.connection.options.closeConfirmTimeout;
  return timeout > 0 ? timeout : 30_000;
}

/** Returns the credit of a subframe of [length] bytes [from] delivered. */
function consumed(from: PipeEnd, length: number): void {
  try {
    from.consumed(length);
  } catch (e) {
    log(from, 'warn', `proxy: credit accounting of channel ${from.id}`, e);
  }
}

/** What {@link forward} takes beside the ends. */
interface ForwardOptions {
  readonly rewrite?: ((status: Status) => Status) | undefined;
  /** The bulk transfers a graceful close follows. */
  readonly twins: Set<BulkTwin>;
  /** The channels are bulk channels, whose graceful close also follows the last send. */
  readonly bulk: boolean;
  readonly bulkOrder: BulkOrder | undefined;
}

/**
 * Forwards what [from] receives to [to], and its end. Takes [from]'s
 * reader at once (throws if it is locked).
 */
function forward(from: PipeEnd, to: PipeEnd, options: ForwardOptions): void {
  // Credit for what [from] receives goes back once [to] has taken it.
  from.setManualCredit(true);
  const reader = from.readable.getReader();
  const { bulkOrder, twins } = options;
  // The last forward; sends complete in order. Never rejects.
  let sending: Promise<void> = Promise.resolve();
  const failed = (error: unknown, length: number): void => {
    consumed(from, length);
    if (error instanceof SwitchboardError && error.code === StatusCode.failedPrecondition) {
      // The destination ended while waiting; its end closes [from] too.
      return;
    }
    log(
      from,
      'warn',
      `proxy: cannot forward ${length} bytes from channel ${from.id} to channel ${to.id}`,
      error,
    );
    const code =
      error instanceof SwitchboardError && isKnownStatusCode(error.code)
        ? error.code
        : StatusCode.internal;
    const status = genericStatus(code);
    closeEnd(to, status);
    closeEnd(from, status);
  };
  const ended = async (): Promise<void> => {
    const clock = to.mux.connection.options.clock;
    let forwarded = closeStatusFor(await from.done);
    if (forwarded.isOk && options.bulk) {
      // Closed once the window took the payload, as a Talk sender closes
      // one: the CLOSE would wait behind what still waits for credit, but
      // its confirmation is timed from the call, and would cut a reader
      // slower than that. A reader that stalls is bounded by the parent:
      // its graceful close waits for this transfer at most the close
      // confirmation timeout, then cancels it, as its failure and the end
      // of its pipe do.
      await sending;
    }
    // Independent streams: bulk channels referenced before the close may
    // arrive after it, and are forwarded before it goes on.
    const settling = forwarded.isOk ? bulkOrder?.settle() : undefined;
    let left: number | undefined;
    if (settling !== undefined) {
      const wait = twinWait(to);
      const start = clock.now();
      if (await within(clock, settling, wait)) {
        left = wait - (clock.now() - start);
      } else {
        log(to, 'debug', `proxy: channel ${to.id} closed with bulk channels still awaited`);
        forwarded = genericStatus(StatusCode.deadlineExceeded);
        bulkOrder?.clear();
        cancelTwins(twins);
      }
    }
    if (forwarded.isOk && twins.size > 0) {
      // A graceful close follows the bulk payloads forwarded before it, as
      // it follows the subframes: the receiver must have them whole before
      // the channel they belong to ends.
      const transfers = Promise.all([...twins].map((twin) => twin.done));
      if (!(await within(clock, transfers, left ?? twinWait(to)))) {
        log(to, 'debug', `proxy: channel ${to.id} closed with bulk payloads still being forwarded`);
        forwarded = genericStatus(StatusCode.deadlineExceeded);
        cancelTwins(twins);
      }
    }
    await to.close(options.rewrite === undefined ? forwarded : options.rewrite(forwarded));
  };
  void (async () => {
    try {
      for (;;) {
        const { value: subframe, done } = await reader.read();
        if (done) break;
        const length = subframe.length;
        if (!to.canSend) {
          // The other side is closing; its CLOSE ends this side too.
          consumed(from, length);
          continue;
        }
        try {
          bulkOrder?.observe(subframe);
          sending = to.send(subframe).then(
            () => consumed(from, length),
            (error: unknown) => failed(error, length),
          );
        } catch (e) {
          failed(e, length);
        }
      }
    } catch (e) {
      log(from, 'debug', `proxy: channel ${from.id} failed`, e);
    }
    try {
      await ended();
    } catch (e) {
      log(to, 'debug', `proxy: closing channel ${to.id}`, e);
    }
  })();
}

/**
 * The status a proxy sends in CLOSE when the channel on the other side
 * ended with [status]: CONNECTION_LOST becomes UNAVAILABLE (with a generic
 * reason, as the local reason concerns the proxy's own connection) and
 * application codes become UNKNOWN; everything else, which the peer on the
 * other side sent, is forwarded as is.
 */
function closeStatusFor(status: Status): Status {
  if (status.known === StatusCode.connectionLost) {
    return Status.of(StatusCode.unavailable, 'connection lost');
  }
  if (status.isApplicationCode) {
    const reason = status.reason === '' ? '' : `: ${status.reason}`;
    return Status.of(StatusCode.unknown, `application status ${status.code}${reason}`);
  }
  return status;
}

/** What {@link proxyHandler} takes beside the node. Durations in milliseconds. */
export interface ProxyHandlerOptions {
  /**
   * Which address headers are proxied; a false or a throw rejects the
   * channel PERMISSION_DENIED. Default: every type outside the reserved
   * `_` namespace.
   */
  readonly allow?: ((address: ChannelAddress) => boolean) | undefined;
  /**
   * Authorizes a channel and returns the address to forward to (its own
   * or a rewritten one); undefined or null: UNAUTHENTICATED; a throw:
   * PERMISSION_DENIED.
   */
  readonly authorize?:
    | ((
        incoming: IncomingChannel,
      ) => ChannelAddress | null | undefined | Promise<ChannelAddress | null | undefined>)
    | undefined;
  /** Bound on {@link authorize}; default 10 000; 0: none. */
  readonly authorizeTimeout?: number | undefined;
  /** The resolver of the destinations; default the node's. */
  readonly resolver?: Resolver | undefined;
  /** Channels forwarded or being authorized at once per client connection; default 256; 0: no bound. */
  readonly maxChannelsPerConnection?: number | undefined;
  /** Keep an instance the address names for a slot-routed type instead of routing by the table. */
  readonly allowExplicitInstance?: boolean | undefined;
  /** Forward MOVED and RELOCATED byte for byte instead of without their owner and epoch. */
  readonly revealOwners?: boolean | undefined;
  /** Size of each client connection's LOCATE bucket; default 32; 0: no bound. */
  readonly maxLocatesPerConnection?: number | undefined;
  /** One LOCATE token comes back every this long; default 1000; 0: never. */
  readonly locateRefillInterval?: number | undefined;
  /** Which slot-routed types admit channels without a shard slot; default none. */
  readonly allowNoSlot?: ((type: Name) => boolean) | undefined;
}

/**
 * A {@link ChannelHandler} that proxies channels to the service they
 * address, for use as `Switchboard.catchAll` on a frontend endpoint or a
 * relay.
 *
 * For each channel:
 *
 * 1. If `allow` returns false for the address header, or throws, the
 *    channel is rejected with PERMISSION_DENIED. The default `allow`
 *    refuses the reserved types (`_ns` and the rest of the `_` namespace),
 *    so that the naming service of the mesh behind the proxy is not
 *    reachable through it; pass an explicit `allow` to proxy those
 *    deliberately. A channel without a service type is rejected with
 *    NOT_FOUND.
 * 2. If the client connection the channel arrived on already has
 *    `maxChannelsPerConnection` channels being forwarded or authorized (0
 *    = no limit), the channel is rejected with RESOURCE_EXHAUSTED. All
 *    clients share the proxy's pooled connections to a backend, so without
 *    this bound one client could use up the channels a backend accepts for
 *    everyone.
 * 3. If `authorize` is given, it is called with the channel and may take
 *    its time, for example to verify the session credential in the
 *    application payload with an authentication service; meanwhile the
 *    channel is not read (the mux buffers what the client sends, within
 *    the window it grants the client). It returns the address to forward
 *    to: the channel's own `IncomingChannel.address`, or a rewritten one
 *    (the shard slot set from the verified account, the payload replaced
 *    by a credential the backends trust, and so on), which steps 4 and 5
 *    use instead of the client's. It returns undefined (or null) to reject
 *    the channel with UNAUTHENTICATED; a throw rejects it with
 *    PERMISSION_DENIED; an address without a service type is rejected with
 *    NOT_FOUND. If it has not answered within `authorizeTimeout` (default
 *    10 s, 0 for no bound) the channel is rejected with UNAUTHENTICATED and
 *    its late answer is ignored. A channel whose client closed it
 *    meanwhile is dropped.
 * 4. If the header carries a host hint and `Switchboard.allowHostHint` is
 *    set, the proxy connects to that host: a URI, or `host:port` meaning
 *    `tcp://host:port`. Otherwise the hint is ignored and the destination
 *    is resolved with `resolver` (default: `Switchboard.resolver`) as in
 *    `Switchboard.selectAndConnect`. Destinations that are this node's own
 *    listeners (`Switchboard.isOwnEndpoint`) are never connected to: a
 *    record or hint pointing back at the proxy would otherwise make it
 *    forward to itself without end. A failure rejects the channel with its
 *    status code (NOT_FOUND, UNAVAILABLE, ...); a missing resolver or a
 *    closing node is UNAVAILABLE.
 *    A channel with a shard slot whose type has a slot table in the
 *    resolver goes to the slot's owner (the old owner while the slot
 *    migrates; see `Switchboard.selectAndConnect`). Unless
 *    `allowExplicitInstance` is set, an instance in its address (the
 *    client's, or one `authorize` set) is ignored for such a channel: it
 *    is routed by the slot table, and the instance is replaced by the
 *    selected owner, so that a client cannot reach an instance that is
 *    still loading the slot, or one that handed it over, and overtake the
 *    hand-over. When the table has no owner for the slot and the proxy
 *    would have to ask the naming service (`LOCATE`, which assigns a free
 *    slot of a managed space), the client connection must have a token
 *    left in its bucket of `maxLocatesPerConnection` tokens, refilled by
 *    one every `locateRefillInterval`; otherwise the channel is rejected
 *    with RESOURCE_EXHAUSTED. The `LOCATE` itself waits at most
 *    `Switchboard.slotRefreshTimeout` (then UNAVAILABLE).
 *    A channel *without* a shard slot (in the address as `authorize`
 *    returned it) whose type has a slot table in the resolver is rejected
 *    with PERMISSION_DENIED unless `allowNoSlot` returns true for the
 *    type: on a sharded type's instances such a channel reaches the
 *    `noSlotHandler` of their `SlotGate`, the state transfer between
 *    instances, which clients have no business with. The default refuses
 *    every such channel. An admitted one is routed like a channel to a
 *    type without a slot table: to the instance its address names, else
 *    round robin over the type's instances, the selected instance filled
 *    in. The slot tables are looked at once the resolver is ready (its
 *    `resolve` answered; a failure rejects the channel with the code the
 *    forwarding would fail with), so that a resolver still syncing lets
 *    neither a channel without a slot nor an instance the client named
 *    past a table. Channels relayed to a host hint are not checked: they
 *    bypass the resolver and its tables.
 * 5. A channel is opened to the destination with the same open payload (as
 *    rewritten by `authorize`), except that the host hint is removed and
 *    the instance is set to the selected one. The application payload is
 *    forwarded unchanged; the proxy's own `Switchboard.defaultPayload` is
 *    not applied.
 * 6. The two channels are joined with {@link pipeChannels}. For a slot
 *    routed by a slot table, if the owner rejects the channel with CLOSE
 *    MOVED before any subframe has been piped either way, the proxy opens
 *    it once more at the new owner (named by the rejection, or a newer
 *    table entry, or found with `SlotResolver.locateSlot` within
 *    `Switchboard.slotRefreshTimeout`) with the same open payload, the
 *    instance set to the new owner's, and the client does not notice.
 *    What the client sends meanwhile is held, and its credit goes back
 *    only once the replacement has taken it, so the client waits once it
 *    has used the window the proxy grants it: the proxy holds at most that
 *    window. After the first subframe, or when no other owner is found,
 *    the MOVED is forwarded to the client, which may open the channel
 *    again and resend, since MOVED means nothing was processed (see
 *    `Switchboard.openChannelToSlot`). A RELOCATED (the owner was serving
 *    the channel when the slot moved) is forwarded and never retried. The
 *    retry counts as the same channel for `maxChannelsPerConnection`, and
 *    a `LOCATE` it needs takes a token as in step 4.
 *
 * A MOVED or RELOCATED a backend closes the channel with reaches the
 * client with its owner and epoch fields set to 0 (unknown) and no reason,
 * unless `revealOwners` is set: instance ids are internal to the mesh, and
 * a client behind the proxy reopens through the proxy, which routes by
 * its own table. With `revealOwners` the status is forwarded byte for
 * byte. Statuses inside the channel (a Talk `ABORT`) are not looked at.
 *
 * Rejections carry the status code and a generic reason only; the details
 * (instance ids, endpoints, resolver state) are logged locally.
 *
 * Honouring host hints makes the node an open relay; see
 * `Switchboard.allowHostHint`.
 *
 * If `allowNoSlot` throws, the error is logged and the channel is refused,
 * as when it returns false.
 *
 * @throws {RangeError} if `maxChannelsPerConnection`,
 * `maxLocatesPerConnection`, `authorizeTimeout` or `locateRefillInterval`
 * is negative.
 */
export function proxyHandler(
  switchboard: Switchboard,
  options: ProxyHandlerOptions = {},
): ChannelHandler {
  const authorizeTimeout = options.authorizeTimeout ?? 10_000;
  const maxChannelsPerConnection = options.maxChannelsPerConnection ?? 256;
  const maxLocatesPerConnection = options.maxLocatesPerConnection ?? 32;
  const locateRefillInterval = options.locateRefillInterval ?? 1000;
  for (const [name, value] of [
    ['maxChannelsPerConnection', maxChannelsPerConnection],
    ['maxLocatesPerConnection', maxLocatesPerConnection],
    ['authorizeTimeout', authorizeTimeout],
    ['locateRefillInterval', locateRefillInterval],
  ] as const) {
    if (!(value >= 0)) {
      throw new RangeError(`${name} must not be negative: ${value}`);
    }
  }
  const permitted = options.allow ?? notReserved;
  const { authorize, resolver, allowNoSlot } = options;
  const allowExplicitInstance = options.allowExplicitInstance ?? false;
  // Channels being forwarded, per client connection.
  const forwarding = new ForwardingBound(maxChannelsPerConnection);
  const locates = new LocateBuckets(
    maxLocatesPerConnection,
    locateRefillInterval,
    switchboard.clock,
  );
  const hide = (options.revealOwners ?? false) ? undefined : hideOwner;
  const logger = switchboard.logger;
  const debug = (message: string, error?: unknown): void =>
    logger.log('debug', routerLogName, message, error);
  const info = (message: string): void => logger.log('info', routerLogName, message);
  return async (incoming) => {
    let address = incoming.address;
    let allowed: boolean;
    try {
      allowed = permitted(address);
    } catch (e) {
      logger.log('warn', routerLogName, `proxy: allow filter threw for ${incoming}`, e);
      allowed = false;
    }
    if (!allowed) {
      debug(`proxy: ${incoming} not allowed`);
      await incoming.reject(genericStatus(StatusCode.permissionDenied));
      return;
    }
    if (address.type === undefined) {
      debug(`proxy: ${incoming} has no service type`);
      await incoming.reject(genericStatus(StatusCode.notFound));
      return;
    }
    const client = incoming.connection;
    if (!forwarding.enter(client)) {
      info(
        `proxy: ${incoming} refused, ${forwarding.countOf(client)} channels of this ` +
          'connection are being forwarded',
      );
      await incoming.reject(genericStatus(StatusCode.resourceExhausted));
      return;
    }
    try {
      if (authorize !== undefined) {
        const answer = await authorizeWith(incoming, authorize, authorizeTimeout, switchboard);
        if (answer.address === undefined) {
          await incoming.reject(genericStatus(answer.refusal));
          return;
        }
        if (!incoming.channel.canSend) {
          debug(`proxy: ${incoming} closed while being authorized`);
          return;
        }
        address = answer.address;
      }
      const type = address.type;
      if (type === undefined) {
        debug(`proxy: ${incoming} authorized without a service type`);
        await incoming.reject(genericStatus(StatusCode.notFound));
        return;
      }
      const r = resolver ?? switchboard.resolver;
      const shard = address.shard;
      const relaying = address.host !== undefined && switchboard.allowHostHint;
      const slots = !relaying && r !== undefined && isSlotResolver(r) ? r : undefined;
      if (slots !== undefined) {
        // The slot tables are complete once the resolver answers: one still
        // syncing has none yet, and would let a channel without a slot, or
        // a client-named instance, past its table.
        const failure = await resolved(slots, type, incoming, debug);
        if (failure !== undefined) {
          await incoming.reject(genericStatus(failure));
          return;
        }
        const table = slots.slotTable(type);
        if (
          table !== undefined &&
          shard === undefined &&
          !admitNoSlot(type, allowNoSlot, incoming, switchboard)
        ) {
          await incoming.reject(genericStatus(StatusCode.permissionDenied));
          return;
        }
        if (table !== undefined && shard !== undefined) {
          if (address.instance !== 0 && !allowExplicitInstance) {
            debug(`proxy: ${incoming} routed by the slot table instead`);
            address = address.with({ instance: 0 });
          }
          if (
            address.instance === 0 &&
            shard < table.count &&
            table.space.mode === SlotMode.managed &&
            slots.slotOwner(type, shard) === undefined &&
            !locates.take(client)
          ) {
            info(`proxy: ${incoming} refused, too many LOCATEs`);
            await incoming.reject(genericStatus(StatusCode.resourceExhausted));
            return;
          }
        }
      }
      let backend: ForwardTarget;
      try {
        backend = await openForwarded(switchboard, address, type, { resolver });
      } catch (e) {
        if (!(e instanceof SwitchboardError)) throw e;
        debug(`proxy: ${incoming}: ${e.status}`);
        await incoming.reject(
          genericStatus(isKnownStatusCode(e.code) ? e.code : StatusCode.unavailable),
        );
        return;
      }
      const channel = backend.channel;
      debug(`proxy: ${incoming} piped to channel ${channel.id} of ${channel.connection}`);
      const header = backend.header;
      if (
        !backend.hostHint &&
        shard !== undefined &&
        slots !== undefined &&
        slots.slotTable(type) !== undefined
      ) {
        // The owner may have moved since the table was read: one retry
        // while nothing has been piped. Bounded by flow control: what the
        // client sends is held within the window granted to it, and its
        // credit goes back only once the replacement takes it.
        const carrier = slotCarrier(switchboard, {
          type,
          shard,
          channel,
          header,
          resolver: slots,
          mayLocate: () => locates.take(client),
        });
        if (carrier !== undefined) {
          await pipe(new MuxEnd(incoming.channel), new SlotEnd(carrier), {
            toA: hide,
            bulkBound: forwarding,
          });
          return;
        }
      }
      await pipe(new MuxEnd(incoming.channel), new MuxEnd(channel), {
        toA: hide,
        bulkBound: forwarding,
      });
    } finally {
      forwarding.exit(client);
    }
  };
}

/**
 * Waits for [r] to answer for [type], after which its slot tables are
 * complete: undefined once it has, else the code {@link proxyHandler}
 * rejects the channel with, the one the forwarding would fail with.
 */
async function resolved(
  r: SlotResolver,
  type: Name,
  incoming: IncomingChannel,
  debug: (message: string) => void,
): Promise<StatusCode | undefined> {
  try {
    await r.resolve(type);
    return undefined;
  } catch (e) {
    if (!(e instanceof SwitchboardError)) throw e;
    debug(`proxy: ${incoming}: ${e.status}`);
    const code = isKnownStatusCode(e.code) ? e.code : StatusCode.unavailable;
    return code === StatusCode.failedPrecondition ? StatusCode.unavailable : code;
  }
}

/**
 * Whether {@link proxyHandler} forwards a channel without a shard slot to
 * [type], which has a slot table: only when [allow] (undefined: nothing)
 * admits the type. A throwing [allow] refuses.
 */
function admitNoSlot(
  type: Name,
  allow: ((type: Name) => boolean) | undefined,
  incoming: IncomingChannel,
  switchboard: Switchboard,
): boolean {
  let allowed = false;
  if (allow !== undefined) {
    try {
      allowed = allow(type);
    } catch (e) {
      switchboard.logger.log('warn', routerLogName, `proxy: allowNoSlot threw for ${incoming}`, e);
    }
  }
  if (!allowed) {
    switchboard.logger.log(
      'debug',
      routerLogName,
      `proxy: ${incoming} has no shard slot for a table-routed type`,
    );
  }
  return allowed;
}

function notReserved(address: ChannelAddress): boolean {
  return !(address.type?.isReserved ?? false);
}

/** A MOVED or RELOCATED with its owner and epoch unknown and no reason; any other status unchanged. */
function hideOwner(status: Status): Status {
  if (!MovedStatus.carriesFields(status)) {
    return status;
  }
  return MovedStatus.unknown.toStatus({ relocated: status.known === StatusCode.relocated });
}

/**
 * The `LOCATE` budget of {@link proxyHandler}: a token bucket per client
 * connection, [burst] tokens, refilled by one every [refill] milliseconds
 * (never when 0). A [burst] of 0 is no limit.
 */
class LocateBuckets {
  readonly burst: number;
  readonly refill: number;
  readonly #clock: Clock;
  readonly #buckets = new WeakMap<object, { tokens: number; at: number }>();

  constructor(burst: number, refill: number, clock: Clock) {
    this.burst = burst;
    this.refill = refill;
    this.#clock = clock;
  }

  /** Takes a token for [connection]; false when there is none left. */
  take(connection: object): boolean {
    if (this.burst === 0) {
      return true;
    }
    const now = this.#clock.now();
    let bucket = this.#buckets.get(connection);
    if (bucket === undefined) {
      bucket = { tokens: this.burst, at: now };
      this.#buckets.set(connection, bucket);
    }
    if (this.refill > 0) {
      bucket.tokens = Math.min(this.burst, bucket.tokens + (now - bucket.at) / this.refill);
    }
    bucket.at = now;
    if (bucket.tokens < 1) {
      return false;
    }
    bucket.tokens -= 1;
    return true;
  }
}

/**
 * Runs the `authorize` hook of {@link proxyHandler} on [incoming], bounded
 * by [timeout] (0: no bound). Resolves with the address to forward to, or
 * none and the code to reject with.
 */
async function authorizeWith(
  incoming: IncomingChannel,
  authorize: NonNullable<ProxyHandlerOptions['authorize']>,
  timeout: number,
  switchboard: Switchboard,
): Promise<{ address: ChannelAddress | undefined; refusal: StatusCode }> {
  const logger = switchboard.logger;
  let answer: Promise<ChannelAddress | null | undefined>;
  try {
    answer = Promise.resolve(authorize(incoming));
  } catch (e) {
    answer = Promise.reject(e);
  }
  // A late answer, or its failure, is ignored.
  answer.catch(() => {});
  const timedOut = Symbol('timed out');
  let timer: TimerHandle | undefined;
  const bounded: Promise<ChannelAddress | null | undefined | typeof timedOut> =
    timeout > 0
      ? Promise.race([
          answer,
          new Promise<typeof timedOut>((resolve) => {
            timer = switchboard.clock.setTimeout(() => resolve(timedOut), timeout);
          }),
        ])
      : answer;
  try {
    const address = await bounded;
    if (address === timedOut) {
      logger.log('info', routerLogName, `proxy: ${incoming} not authorized within ${timeout} ms`);
      return { address: undefined, refusal: StatusCode.unauthenticated };
    }
    if (address === undefined || address === null) {
      logger.log('debug', routerLogName, `proxy: ${incoming} not authenticated`);
      return { address: undefined, refusal: StatusCode.unauthenticated };
    }
    return { address, refusal: StatusCode.unauthenticated };
  } catch (e) {
    // Clients can trigger this at will (a malformed credential, say).
    logger.log('debug', routerLogName, `proxy: authorize refused ${incoming}`, e);
    return { address: undefined, refusal: StatusCode.permissionDenied };
  } finally {
    if (timer !== undefined) {
      switchboard.clock.clearTimeout(timer);
    }
  }
}

/** Whether [promise] settles within [ms] milliseconds of [clock] (0: only if it already has, after a timer turn). */
async function within(clock: Clock, promise: Promise<unknown>, ms: number): Promise<boolean> {
  let timer: TimerHandle | undefined;
  const expired = new Promise<false>((resolve) => {
    timer = clock.setTimeout(() => resolve(false), Math.max(0, ms));
  });
  try {
    return await Promise.race([
      promise.then(
        () => true,
        () => true,
      ),
      expired,
    ]);
  } finally {
    if (timer !== undefined) clock.clearTimeout(timer);
  }
}

function log(end: PipeEnd, level: LogLevel, message: string, error?: unknown): void {
  end.mux.connection.options.logger.log(level, routerLogName, message, error);
}

function closeEnd(end: PipeEnd, status: Status): void {
  try {
    quietly(end.close(status));
  } catch {
    // Closing already.
  }
}

function quietly(promise: Promise<unknown>): void {
  promise.catch(() => {});
}
