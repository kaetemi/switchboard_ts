/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * The requester side of Talk: one outstanding request of ours, its
 * timeout, its item window, and the handle the request API returns.
 *
 * @internal
 */

import { Status, StatusCode, type SwitchboardError } from '../status.ts';
import type { TimerHandle } from '../util/clock.ts';
import { Deferred } from '../util/deferred.ts';
import type { TalkMessage, TalkRequest } from './contract.ts';
import type { BulkIn, BulkOut } from './talk-bulk-transfer.ts';
import { Lane } from './talk-bulk-transfer.ts';
import type { TalkChannel } from './talk-channel.ts';
import { Delivery, Held } from './talk-delivery.ts';
import { TalkFrame, TalkKind } from './talk-frame.ts';
import { IncomingMessage } from './talk-message.ts';
import { maxTimerDelay, TalkAbortError } from './talk-util.ts';

/** What `onExtend` gets: the deadline and the renewal as received, an absent field for 0. */
export type ExtendCallback = (extension: { deadline?: number; renew?: number }) => void;

/**
 * Receives the responses to an outgoing request synchronously, in wire
 * order, instead of through a stream or a promise. Used by forwarding.
 */
export interface ResponseSink {
  /** A stream item arrived. */
  item(item: IncomingMessage): void;
  /** The final response arrived. */
  complete(message: IncomingMessage): void;
  /** The request failed, for any reason. */
  fail(error: SwitchboardError): void;
  /** The peer sent [message], an EXTEND. */
  extended(message: IncomingMessage): void;
}

/**
 * The timeout of one request, on either side: a gap restarted by every
 * reply (the default regime) until a deadline or a renewal is declared,
 * then the later of the declared deadline and the last reply plus the
 * renewal. Instants are clock times in milliseconds.
 */
export class Expiry {
  /** The default gap; 0 for no timeout at all. */
  gap: number;

  /** A deadline or a renewal was declared. */
  declared = false;

  /** The end of the default gap; in the declared regime, set only by an empty EXTEND. */
  gapEnd: number | undefined;

  /** The declared deadline. */
  deadline: number | undefined;

  /** The declared renewal, counted from {@link lastReply}. */
  renew: number | undefined;

  /** The last reply: stream item or EXTEND. */
  lastReply = 0;

  constructor(gap: number) {
    this.gap = gap;
  }

  /** Back to the default regime, the gap counted from [now]. */
  start(now: number): void {
    this.declared = false;
    this.deadline = undefined;
    this.renew = undefined;
    this.lastReply = now;
    this.gapEnd = now + this.gap;
  }

  /** A reply at [now]: restarts the gap, or renews by the declared renewal. */
  reply(now: number): void {
    this.lastReply = now;
    if (!this.declared) {
      this.gapEnd = now + this.gap;
    }
  }

  /** An empty EXTEND at [now]: restarts the gap, keeping what was declared. */
  restartGap(now: number): void {
    this.lastReply = now;
    this.gapEnd = now + this.gap;
  }

  /**
   * An EXTEND declaring [deadline] from [now] and [renew]; an undefined
   * value leaves that one unchanged. Replaces what was declared before,
   * and the gap.
   */
  declare(now: number, deadline: number | undefined, renew: number | undefined): void {
    if (deadline === undefined && renew === undefined) {
      this.reply(now);
      return;
    }
    this.lastReply = now;
    this.declared = true;
    this.gapEnd = undefined;
    if (deadline !== undefined) {
      this.deadline = now + deadline;
    }
    if (renew !== undefined) {
      this.renew = renew;
    }
  }

  /** When the request times out; undefined for never. */
  get end(): number | undefined {
    if (this.gap <= 0) {
      return undefined;
    }
    const renew = this.renew;
    return later(
      later(this.gapEnd, this.deadline),
      renew === undefined ? undefined : this.lastReply + renew,
    );
  }
}

function later(a: number | undefined, b: number | undefined): number | undefined {
  return a === undefined ? b : b === undefined || a >= b ? a : b;
}

/** What {@link Outgoing} is created with. */
export interface OutgoingInit {
  readonly stream: boolean;
  readonly window: number | undefined;
  readonly onExtend: ExtendCallback | undefined;
  readonly sink: ResponseSink | undefined;
  readonly ordered: boolean;
}

/** One of our outstanding requests. */
export class Outgoing {
  readonly channel: TalkChannel;
  readonly id: number;
  readonly timeout: number;
  readonly isStream: boolean;
  readonly onExtend: ExtendCallback | undefined;
  readonly sink: ResponseSink | undefined;

  /** The answer is delivered in order with `TalkChannel.messages`. */
  readonly ordered: boolean;

  /** The credit of the items waiting in {@link items}. */
  readonly itemsHeld = new Held();

  /** The items of a stream request, for the application; undefined with a sink, and for a single response. */
  readonly items: Delivery | undefined;

  /** The order of what answers the request, kept while bulk payloads arrive. */
  readonly lane = new Lane();

  /** Bulk payloads of the request itself, being sent. */
  readonly bulkOuts = new Set<BulkOut>();

  /**
   * Set once the request failed locally: the status its payload is
   * stopped with, also when it starts only after (its frame waited for
   * the peer's LIMITS).
   */
  bulkStopped: Status | undefined;

  /** Reply payloads still in transfer, even after their streamed messages were delivered. */
  readonly bulkIns = new Set<BulkIn>();

  /**
   * The final response or abort arrived; it may wait in {@link lane} (or,
   * for an ordered request, in the lane of `messages`) for a bulk payload,
   * and the request stays in the channel's table until it is delivered.
   */
  finalReceived = false;

  /** The final response, once it arrived. */
  finalMessage: IncomingMessage | undefined;

  readonly completer = new Deferred<TalkMessage>();
  timer: TimerHandle | undefined;

  /** The requester timeout. */
  readonly expiry: Expiry;

  /** Cancelled locally; the id stays reserved until the final arrives. */
  abandoned = false;
  #failed = false;

  /**
   * The item window as this side sees it, for a stream request with a
   * finite one: what it granted (the window it opened with, and every
   * grant since) less the items that arrived. Undefined when unlimited,
   * and for a request that is not a stream request.
   */
  window: number | undefined;

  /**
   * Registered, its frame not sent yet: it waits for the item window of
   * the peer's request it answers (`TalkChannel._startRequest` with
   * `deferred`).
   */
  queued = false;

  /** Sends the frame of the request; set by `TalkChannel._startRequest`. */
  send: (() => void) | undefined;

  /** The status of a cancel made while {@link queued}, sent once the frame went. */
  cancelWhenSent: Status | undefined;

  /** When {@link timer} fires, if it was armed by {@link #arm}. */
  #armedEnd: number | undefined;

  constructor(channel: TalkChannel, id: number, timeout: number, init: OutgoingInit) {
    this.channel = channel;
    this.id = id;
    this.timeout = timeout;
    this.isStream = init.stream;
    this.window = init.window;
    this.onExtend = init.onExtend;
    this.sink = init.sink;
    this.ordered = init.ordered;
    this.expiry = new Expiry(timeout);
    this.items =
      init.stream && init.sink === undefined
        ? new Delivery(channel, this.itemsHeld, (dropped) => this.#onItemsCancelled(dropped))
        : undefined;
  }

  /** Answered, failed or cancelled. */
  get ended(): boolean {
    return this.#failed || this.completer.isSettled || this.abandoned;
  }

  /**
   * Whether what the peer sends for {@link id} can answer this request:
   * not once its final arrived, nor before its frame went ({@link queued}),
   * which the peer cannot know of.
   */
  get awaitsResponse(): boolean {
    return !this.finalReceived && !this.queued;
  }

  /**
   * The window is exhausted: as many items arrived as were granted, so the
   * requester timeout does not run. Not once the request was cancelled:
   * the timer then keeps its id until the final arrives.
   */
  get exhausted(): boolean {
    return this.window === 0 && !this.abandoned;
  }

  /** When the request times out, in clock time; undefined if it never does, or has ended. */
  get deadline(): number | undefined {
    return this.timer === undefined || this.ended ? undefined : this.expiry.end;
  }

  stopTimer(): void {
    if (this.timer !== undefined) {
      this.channel._clock.clearTimeout(this.timer);
      this.timer = undefined;
    }
  }

  /** Starts the requester timeout, in the default regime. */
  startTimer(): void {
    this.expiry.start(this.channel._clock.now());
    this.#arm();
  }

  /** A stream item arrived, or a chunk of the bulk payload of one. */
  replied(): void {
    this.expiry.reply(this.channel._clock.now());
    this.#arm();
  }

  /**
   * A stream item arrived: it takes one item of the window, and restarts
   * the timeout as any reply does. An item beyond the window never gets
   * here (the channel closes first).
   */
  itemArrived(): void {
    const window = this.window;
    if (window !== undefined) {
      this.window = window - 1;
    }
    this.replied();
  }

  /** `TalkStream.grant`: grants the peer [items] more items of the window. */
  grant(items: number): void {
    if (!Number.isInteger(items) || items < 1) {
      throw new RangeError(`items ${items} must be an integer of at least 1`);
    }
    const window = this.window;
    if (window === undefined) {
      throw new Error(
        this.isStream
          ? `request ${this.id} has an unlimited window`
          : `request ${this.id} is not a stream request`,
      );
    }
    if (this.ended || this.finalReceived || this.channel._outgoing.get(this.id) !== this) {
      return;
    }
    if (window + items >= TalkFrame.unlimitedWindow) {
      throw new RangeError(
        `${items} items would bring the window of request ${this.id} to ${window + items}, ` +
          `beyond ${TalkFrame.unlimitedWindow - 1}`,
      );
    }
    this.addCredit(items);
  }

  /**
   * A credit the peer granted for the request this one was forwarded for,
   * passed on: what {@link grant} does without its checks, which the
   * peer's credit passed already, and doing nothing once the request ended.
   */
  forwardCredit(items: number): void {
    const window = this.window;
    if (
      window === undefined ||
      this.ended ||
      this.finalReceived ||
      this.channel._outgoing.get(this.id) !== this
    ) {
      return;
    }
    if (window + items >= TalkFrame.unlimitedWindow) {
      // Cannot happen: this side relayed at most what it received.
      this.channel._log('warn', `credit of ${items} items overflows request ${this.id}, dropped`);
      return;
    }
    this.addCredit(items);
  }

  /**
   * Adds [items] to the window and sends the credit; one granted while the
   * request itself waits to be sent goes with it instead. The requester
   * timeout restarts, as a reply would, if the window was exhausted.
   */
  addCredit(items: number): void {
    const wasExhausted = this.exhausted;
    this.window = (this.window as number) + items;
    if (this.queued) {
      return;
    }
    this.channel._trySend(
      new TalkFrame({
        kind: TalkKind.extend,
        requestId: this.id,
        payload: TalkFrame.creditPayload(items),
      }),
    );
    if (wasExhausted && !this.ended) {
      this.replied();
    }
  }

  /** Keeps the id of a cancelled request for [duration] at most. */
  armRelease(duration: number): void {
    this.expiry.gap = duration;
    this.expiry.start(this.channel._clock.now());
    this.#arm();
  }

  #arm(): void {
    // While the window is exhausted the timeout does not run.
    const end = this.exhausted || this.queued ? undefined : this.expiry.end;
    if (end !== undefined && end === this.#armedEnd && this.timer !== undefined) {
      return;
    }
    this.stopTimer();
    this.#armedEnd = end;
    if (end === undefined) {
      return;
    }
    const clock = this.channel._clock;
    let left = end - clock.now();
    if (left < 0) {
      left = 0;
    }
    if (left > maxTimerDelay) {
      this.timer = clock.setTimeout(() => {
        this.timer = undefined;
        this.#arm();
      }, maxTimerDelay);
    } else {
      this.timer = clock.setTimeout(() => this.channel._onRequestTimeout(this), left);
    }
  }

  /** [value] within `minExtension` and `maxExtension`. */
  #bounded(value: number | undefined): number | undefined {
    if (value === undefined) {
      return undefined;
    }
    const options = this.channel.options;
    if (options.minExtension > 0 && value < options.minExtension) {
      value = options.minExtension;
    }
    if (options.maxExtension > 0 && value > options.maxExtension) {
      value = options.maxExtension;
    }
    return value;
  }

  addItem(message: IncomingMessage): void {
    const sink = this.sink;
    if (sink !== undefined) {
      // Its credit goes back once the item forwarded went out.
      this.channel._handOver(message, undefined, () =>
        this.#guard(() => sink.item(message), 'forwarding an item'),
      );
      return;
    }
    const items = this.items as Delivery;
    if (items.isClosed || items.isCancelled) {
      message._drop();
      return;
    }
    this.itemsHeld.hold(message);
    items.add(message);
  }

  complete(message: IncomingMessage): void {
    this.stopTimer();
    const sink = this.sink;
    if (sink !== undefined) {
      this.#guard(() => sink.complete(message), 'forwarding a response');
      return;
    }
    this.items?.close();
    // No more items will come: what waits for a consumer that never comes
    // must not stall the channel.
    this.channel._returnHeld(this.itemsHeld);
    this.completer.resolve(message);
  }

  fail(error: SwitchboardError): void {
    this.stopTimer();
    if (this.#failed) {
      return;
    }
    this.#failed = true;
    this.lane.drop();
    for (const bulk of [...this.bulkIns]) {
      bulk.close(cancelled('request ended'));
      bulk.abandon();
    }
    this.bulkIns.clear();
    // An ordered answer can still be waiting for dispatch in the messages
    // lane. Drop its reference now, so it cannot hold later messages up.
    this.finalMessage?._drop();
    if (!(error instanceof TalkAbortError) || error.isChannelAbort) {
      // A local failure: the payload of the request is not wanted any
      // more. After the peer's abort response, its receiver decides.
      const stopped = error.status.isOk ? cancelled('') : error.status;
      this.bulkStopped = stopped;
      for (const out of this.bulkOuts) {
        out.cancel(stopped);
      }
    }
    const sink = this.sink;
    if (sink !== undefined) {
      this.#guard(() => sink.fail(error), 'forwarding a failure');
      return;
    }
    this.completer.reject(error);
    const items = this.items;
    if (items !== undefined && !items.isClosed) {
      items.fail(error);
    }
    this.channel._returnHeld(this.itemsHeld);
  }

  /** The peer sent [frame], an EXTEND [wire] bytes long. */
  extended(frame: TalkFrame, wire: number): void {
    const { deadline, renew } = frame.extension;
    const now = this.channel._clock.now();
    if (frame.payload.length === 0) {
      this.expiry.restartGap(now);
    } else {
      this.expiry.declare(now, this.#bounded(deadline), this.#bounded(renew));
    }
    this.#arm();
    const sink = this.sink;
    if (sink !== undefined) {
      // In order with the items relayed before it, its credit returned
      // once it went out (or when the lane drops it).
      const message = new IncomingMessage(this.channel, frame);
      message._creditBytes = wire;
      this.lane.add(message, () =>
        this.channel._handOver(message, undefined, () =>
          this.#guard(() => sink.extended(message), 'forwarding EXTEND'),
        ),
      );
      return;
    }
    this.channel._credit(wire);
    const callback = this.onExtend;
    if (callback !== undefined) {
      const extension: { deadline?: number; renew?: number } = {};
      if (deadline !== undefined) {
        extension.deadline = deadline;
      }
      if (renew !== undefined) {
        extension.renew = renew;
      }
      this.#guard(() => callback(extension), `onExtend callback of request ${this.id}`);
    }
  }

  /** The items consumer stopped, or the items ended. Undelivered item requests are refused, and the request is cancelled if outstanding. */
  #onItemsCancelled(dropped: IncomingMessage[]): void {
    this.channel._drainHeld(this.itemsHeld);
    for (const message of dropped) {
      if (message.expectsReply) {
        message._abortQuietly(cancelled('stream items not listened'));
      }
    }
    this.channel._cancelOutgoing(this, cancelled('items subscription cancelled'));
  }

  #guard(action: () => void, what: string): void {
    try {
      action();
    } catch (e) {
      this.channel._log('error', `${what} failed`, e);
    }
  }
}

/** CANCELLED with [reason]. */
function cancelled(reason: string): Status {
  return Status.of(StatusCode.cancelled, reason);
}

/** The handle of a request with a single final response. */
export class TalkRequestHandle implements TalkRequest {
  readonly #pending: Outgoing;

  constructor(pending: Outgoing) {
    this.#pending = pending;
  }

  get requestId(): number {
    return this.#pending.id;
  }

  get response(): Promise<TalkMessage> {
    return this.#pending.completer.promise;
  }

  get deadline(): number | undefined {
    return this.#pending.deadline;
  }

  cancel(status?: Status): void {
    this.#pending.channel._cancelOutgoing(this.#pending, status ?? Status.of(StatusCode.cancelled));
  }
}
