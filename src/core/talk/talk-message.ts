/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * A message received on a Talk channel, with the reply API: the responder
 * side of Talk, its timeout, its credit bound and the item window of a
 * stream request it answers.
 *
 * @internal
 */

import { type PayloadLike, toPayload, utf8Decode } from '../bytes.ts';
import { Name, type NameLike, toName } from '../name.ts';
import { Status, StatusCode, SwitchboardError } from '../status.ts';
import type { TimerHandle } from '../util/clock.ts';
import { Deferred } from '../util/deferred.ts';
import type {
  ReplyOptions,
  ReplyRequestOptions,
  TalkMessage,
  TalkRequest,
  TalkStream,
} from './contract.ts';
import type { TalkBulkReference } from './talk-bulk.ts';
import type { BulkIn, BulkOut, Lane } from './talk-bulk-transfer.ts';
import { BulkPayload } from './talk-bulk-transfer.ts';
import type { TalkChannel } from './talk-channel.ts';
import { TalkFrame, TalkKind } from './talk-frame.ts';
import {
  Expiry,
  type ExtendCallback,
  type Outgoing,
  type ResponseSink,
  TalkRequestHandle,
} from './talk-request.ts';
import { TalkStreamHandle } from './talk-stream.ts';
import {
  asSwitchboardError,
  checkAbortStatus,
  maxTimerDelay,
  noop,
  once,
  quiet,
  rejected,
  windowOf,
  wireAbortStatus,
} from './talk-util.ts';

/** A reply waiting for the item window of the request it answers. */
interface Queued {
  /** A stream item: it takes one item of the window, and waits for one. */
  readonly counts: boolean;
  /** Sends the reply; throws what sending throws. */
  readonly send: () => void;
  /** The reply will not be sent: the request ended, or sending it failed with [error]. */
  readonly drop: (error: SwitchboardError) => void;
}

/** How a bulk reply ended: the transfer's own end once it started, or the failure of a frame that waited for the peer's LIMITS. */
class Sent {
  readonly #done = new Deferred<void>();

  get done(): Promise<void> {
    return this.#done.promise;
  }

  readonly started = (out: BulkOut): void => {
    this.#done.resolve(out.done);
  };

  readonly failed = (error: SwitchboardError): void => {
    this.#done.reject(error);
  };
}

/** What the internal reply senders take besides the payload and the procedure. */
export interface ReplySendOptions {
  readonly bulk?: BulkPayload | undefined;
  readonly onBulk?: ((out: BulkOut) => void) | undefined;
  readonly onFailure?: ((error: SwitchboardError) => void) | undefined;
  readonly onAccepted?: (() => void) | undefined;
}

/** What the internal reply requests take besides the payload and the procedure. */
export interface ReplyRequestInit {
  readonly stream: boolean;
  readonly timeout: number | undefined;
  readonly window?: number | undefined;
  readonly onExtend?: ExtendCallback | undefined;
  readonly sink?: ResponseSink | undefined;
  readonly bulk?: BulkPayload | undefined;
  readonly onAccepted?: (() => void) | undefined;
  readonly signal?: AbortSignal | undefined;
}

/** The procedure a reply method was given, as a name. */
function procedureOf(procedure: NameLike | undefined): Name | undefined {
  return procedure === undefined ? undefined : toName(procedure);
}

/** The longest deadline or renewal an EXTEND field can carry. */
const maxExtend = TalkFrame.maxExtendMillis;

/**
 * A message received on a Talk channel: a plain message, a request, a
 * final response, or a stream item. Any of them may itself be a request
 * ({@link expectsReply}), which the reply API answers.
 */
export class IncomingMessage implements TalkMessage {
  readonly channel: TalkChannel;
  readonly frame: TalkFrame;

  /**
   * The credit of the frame that carried this message, until it is
   * returned (`TalkChannel._creditMessage`); 0 after, and for messages of
   * a channel that is not flow controlled.
   */
  _creditBytes = 0;

  /** That credit while the message is being handed over (`TalkChannel._handOver`), returned after unless taken over. */
  _deliveryCredit = 0;

  // Bulk payload: the reference, the bulk channel once claimed, and how
  // it is delivered.
  _bulkRef: TalkBulkReference | undefined;
  _forSink = false;
  _bulk: BulkIn | undefined;

  /** Its payload is delivered as a stream (`streamBulk`); {@link _streamBulk} once it may be. */
  _streams = false;
  _streamBulk = false;
  _assembled: Uint8Array | undefined;
  _bulkFailure: Status | undefined;

  /** Bytes charged to the channel until this message is delivered. */
  _assemblyBytes = 0;
  _dropped = false;

  /** Its payload counts toward the channel's budget: it is held for a listener. */
  _budgeted = true;

  /** Waiting in the channel's line for the budget (`TalkChannel._admit`). */
  _inAdmission = false;

  /** Its bulk channel is there while it waits, holding at most {@link _waitBytes}. */
  _waiting = false;
  _waitBytes = 0;

  /** Held in a delivery with its bulk payload unread (`Held.bulk`). */
  _heldBulk = false;

  /** The lane this message waits in, while its payload is not ready. */
  _lane: Lane | undefined;

  /** Bulk payloads of the replies to this request, being sent. */
  readonly _bulkOuts = new Set<BulkOut>();

  /** Called as the bulk payload of this message is read: restarts the timeouts that its transfer counts as progress for. */
  _onBulkProgress: (() => void) | undefined;

  /** The responder timeout waits for the request to be delivered (a bulk payload being reassembled or not dispatched yet). */
  _timerDeferred = false;

  /** A final reply was sent, or the request can no longer be answered. */
  _finished: boolean;
  _cancelled = false;
  #cancel: Deferred<void> | undefined;
  #abort: AbortController | undefined;
  #cancelCause: Status | undefined;
  #timer: TimerHandle | undefined;

  /** The responder timeout; created when the request is registered. */
  _expiry: Expiry | undefined;

  /** Responder timeout override; undefined means the channel default. */
  #replyTimeout: number | undefined;

  /** The status of the peer's cancel, if the peer cancelled. */
  _cancelStatus: Status | undefined;

  /** Called synchronously once when the request is cancelled. */
  _cancelHook: (() => void) | undefined;

  /** Set while this message is the marker of an `ordered` request's answer in `messages`: delivers that answer. */
  _outcome: (() => void) | undefined;

  /** `forwardMessage` took this request over. */
  _forwarded = false;

  /** A reply to this request is being sent (see {@link #sendReply}). */
  #sending = false;

  /** The reply being sent is the final one: the request counts as answered. */
  #finalizing = false;

  /** The status of a peer cancel that arrived while a reply was being sent. */
  #heldCancel: Status | undefined;

  // Item window, for a stream request whose window is finite.

  /** What the peer granted less the items sent; undefined when unlimited, and for any other message. */
  _window: number | undefined;

  /** Opened with window 0, and no credit arrived yet: the credit bound runs from the request's arrival. */
  #awaitingGrant: boolean;

  /** Replies waiting for the window, in order: items while it is exhausted, and anything after them. */
  #queue: Queued[] | undefined;

  /** {@link #flush} is running. */
  #flushing = false;

  /** The final reply waits in {@link #queue}: the request counts as answered. */
  #finalQueued = false;

  /** {@link whenCredit} is waited for. */
  #creditWaiter: Deferred<void> | undefined;

  /** Credit bound override; undefined means the channel default. */
  #creditTimeout: number | undefined;

  /** The credit bound, while it runs: when it expires. */
  #creditEnd: number | undefined;
  #creditTimer: TimerHandle | undefined;

  /** Called with each credit the peer grants, once `forwardMessage` took this request over: passes it on. */
  _creditHook: ((items: number) => void) | undefined;

  /**
   * Items of the window that replies of the application's own, waiting
   * for it when `forwardMessage` took the request over, still take: the
   * credits that cover them are not passed to {@link _creditHook}.
   */
  _creditOwed = 0;

  /** Why replies still waiting for the window are not sent, when the request ends with an abort. */
  #dropError: SwitchboardError | undefined;

  /** When {@link #timer} fires. */
  #armedEnd: number | undefined;

  constructor(channel: TalkChannel, frame: TalkFrame) {
    this.channel = channel;
    this.frame = frame;
    this._finished = frame.requestId === 0;
    this._window =
      frame.stream && frame.window !== TalkFrame.unlimitedWindow ? frame.window : undefined;
    this.#awaitingGrant = frame.stream && frame.window === 0;
  }

  // -------------------------------------------------------------------
  // The message

  /** `message` or `streamItem`. */
  get kind(): TalkKind {
    return this.frame.kind;
  }

  get procedure(): Name {
    return this.frame.procedure ?? Name.empty;
  }

  get procedureName(): string {
    return this.procedure.toString();
  }

  get requestId(): number {
    return this.frame.requestId;
  }

  get responseId(): number {
    return this.frame.responseId;
  }

  get expectsReply(): boolean {
    return this.frame.requestId !== 0;
  }

  get expectsStream(): boolean {
    return this.frame.stream;
  }

  get isBulk(): boolean {
    return this.frame.bulk;
  }

  get bulkLength(): number | undefined {
    return this._bulkRef?.length;
  }

  /**
   * The payload: inline, or a bulk payload reassembled before delivery.
   *
   * @throws {Error} for a streamed bulk payload not yet read with
   * {@link bytes}, and the failure's {@link SwitchboardError} if it failed.
   */
  get payload(): Uint8Array {
    if (!this.frame.bulk) {
      return this.frame.payload;
    }
    const assembled = this._assembled;
    if (assembled !== undefined) {
      return assembled;
    }
    const failure = this._bulkFailure;
    if (failure !== undefined) {
      throw new SwitchboardError(failure);
    }
    throw new Error(
      'the bulk payload of this message arrives as a stream: read bulk, or await bytes()',
    );
  }

  text(): string {
    return utf8Decode(this.payload);
  }

  json<T = unknown>(): T {
    return JSON.parse(this.text()) as T;
  }

  /**
   * The payload as a byte stream; once for a streamed bulk payload, whose
   * chunks return their credit to the sender as the reader takes them.
   * Ends with the transfer's failure, if it failed. Cancelling it before
   * the end tells the sender to stop (CANCELLED).
   *
   * @throws {Error} for a streamed payload read already.
   */
  get bulk(): ReadableStream<Uint8Array> {
    const failure = this._bulkFailure;
    if (failure !== undefined) {
      const error = new SwitchboardError(failure);
      return new ReadableStream<Uint8Array>({
        start: (controller) => controller.error(error),
      });
    }
    const b = this._bulk;
    if (!this.frame.bulk || this._assembled !== undefined || b === undefined) {
      const bytes = this.frame.bulk ? this._assembled : this.frame.payload;
      if (bytes === undefined) {
        throw new Error('the bulk payload of this message is not here yet');
      }
      return new ReadableStream<Uint8Array>({
        start: (controller) => {
          if (bytes.length > 0) {
            controller.enqueue(bytes);
          }
          controller.close();
        },
      });
    }
    return b.stream();
  }

  /**
   * The whole payload, at most [maxLength] bytes (default
   * `maxInlinePayload`): a streamed bulk payload is read and reassembled;
   * beyond [maxLength] its bulk channel is closed RESOURCE_EXHAUSTED and
   * the promise rejects with it. Once resolved, {@link payload} returns
   * the same bytes.
   */
  bytes(options?: { maxLength?: number }): Promise<Uint8Array> {
    const max = options?.maxLength ?? this.channel.options.maxInlinePayload;
    const failure = this._bulkFailure;
    if (failure !== undefined) {
      return rejected(new SwitchboardError(failure));
    }
    const b = this._bulk;
    if (!this.frame.bulk || this._assembled !== undefined || b === undefined) {
      const bytes = this.frame.bulk ? this._assembled : this.frame.payload;
      if (bytes === undefined) {
        return rejected(new Error('the bulk payload of this message is not here yet'));
      }
      if (bytes.length > max) {
        return rejected(
          SwitchboardError.of(
            StatusCode.resourceExhausted,
            `payload of ${bytes.length} bytes, more than ${max}`,
          ),
        );
      }
      return Promise.resolve(bytes);
    }
    if (b._read) {
      return rejected(new Error('the bulk payload is read already'));
    }
    return quiet(
      b.collect(max).then((bytes) => {
        this._assembled = bytes;
        return bytes;
      }),
    );
  }

  /**
   * Takes over the credit of this message's frame, while it is being
   * handed over, for forwarding: the function returned gives it back,
   * once. Nothing to give back when the message was handed over before.
   */
  _takeCredit(): () => void {
    const bytes = this._deliveryCredit;
    if (bytes === 0) {
      return noop;
    }
    this._deliveryCredit = 0;
    return once(() => this.channel._credit(bytes));
  }

  /**
   * What forwarding this message sends: the payload, or the bulk channel
   * to pipe.
   *
   * @throws {Error} when the bulk payload was read already, and
   * {@link SwitchboardError} when it failed.
   */
  _forwardPayload(): [Uint8Array, BulkPayload | undefined] {
    if (!this.frame.bulk) {
      return [this.frame.payload, undefined];
    }
    const assembled = this._assembled;
    if (assembled !== undefined) {
      return [assembled, undefined];
    }
    const failure = this._bulkFailure;
    if (failure !== undefined) {
      throw new SwitchboardError(failure);
    }
    const b = this._bulk;
    if (b === undefined || b._read) {
      throw new Error('the bulk payload is read already');
    }
    return [new Uint8Array(0), BulkPayload.pipe(b)];
  }

  // -------------------------------------------------------------------
  // Reassembly budget

  /** Charges [bytes] to the channel's budget. */
  _charge(bytes: number): void {
    this._assemblyBytes += bytes;
    this.channel._assemblyBytes += bytes;
  }

  /** The reassembly completed with [length] bytes: what it is charged from now on. */
  _settleAssembly(length: number): void {
    const excess = this._assemblyBytes - length;
    if (excess > 0) {
      this._assemblyBytes = length;
      this.channel._assemblyBytes -= excess;
      this.channel._admit();
    }
  }

  _releaseAssembly(): void {
    if (this._assemblyBytes === 0) {
      return;
    }
    this.channel._assemblyBytes -= this._assemblyBytes;
    this._assemblyBytes = 0;
    // Room for what waits.
    this.channel._admit();
  }

  /** Leaves the line for the budget: admitted, failed or dropped. */
  _leaveAdmission(): void {
    if (!this._inAdmission) {
      return;
    }
    this._inAdmission = false;
    if (this._waiting) {
      this._waiting = false;
      this.channel._waitingCount -= 1;
      this.channel._waitingBytes -= this._waitBytes;
      this._waitBytes = 0;
    }
    if (this._bulk !== undefined) {
      this._bulk.waitingForBudget = false;
    }
    // It may have held up those behind it.
    this.channel._admit();
  }

  /** Starts the responder timeout deferred by `TalkChannel._register`. */
  _startDeferredTimer(): void {
    if (!this._timerDeferred) {
      return;
    }
    this._timerDeferred = false;
    if (!this._finished && this.expectsReply) {
      this._startTimer();
      this._armCredit();
    }
  }

  /** Whether the message can be delivered: inline, or its bulk payload is reassembled, failed, or delivered as a stream. */
  get _ready(): boolean {
    return (
      this._bulkRef === undefined ||
      this._bulkFailure !== undefined ||
      this._assembled !== undefined ||
      (this._bulk !== undefined && this._streamBulk)
    );
  }

  /** The message will not be delivered: its credit goes back, its bulk payload is not wanted, a request is answered CANCELLED. */
  _drop(): void {
    if (this._dropped) {
      return;
    }
    this._dropped = true;
    this._leaveAdmission();
    this._bulkFailure ??= Status.of(StatusCode.cancelled, 'message dropped');
    this.channel._creditMessage(this);
    const b = this._bulk;
    if (b !== undefined) {
      b.close(Status.of(StatusCode.cancelled, 'message dropped'));
      b.abandon();
    } else if (this._bulkRef !== undefined) {
      const number = this._bulkRef.number;
      const awaited = this.channel._bulkAwaited.get(number);
      if (awaited !== undefined && awaited.message === this) {
        this.channel._bulkAwaited.set(number, { message: undefined, timer: awaited.timer });
      }
    }
    this._abortQuietly(Status.of(StatusCode.cancelled, 'message dropped'));
    this._lane?.advance();
  }

  /** Delivers the answer this message is the marker of, once. */
  _runOutcome(): void {
    const outcome = this._outcome;
    this._outcome = undefined;
    outcome?.();
  }

  // -------------------------------------------------------------------
  // Request state

  get isCancelled(): boolean {
    return this._cancelled;
  }

  get onCancel(): Promise<void> {
    this.#cancel ??= new Deferred<void>();
    return this.#cancel.promise;
  }

  get signal(): AbortSignal {
    if (this.#abort === undefined) {
      this.#abort = new AbortController();
      if (this._cancelled) {
        this.#abort.abort(this.#cancelReason());
      }
    }
    return this.#abort.signal;
  }

  get canReply(): boolean {
    return !this._finished && !this.#finalizing && !this.#finalQueued && this.channel.isOpen;
  }

  get window(): number | undefined {
    return this._window;
  }

  get whenCredit(): Promise<void> {
    const window = this._window;
    if (window === undefined || window > 0 || this._finished || this._cancelled) {
      return Promise.resolve();
    }
    this.#creditWaiter ??= new Deferred<void>();
    const waiter = this.#creditWaiter;
    // Waiting for a grant is having something to send.
    this._armCredit();
    return waiter.promise;
  }

  setCreditTimeout(timeout: number | undefined): void {
    if (timeout !== undefined && !(timeout >= 0)) {
      throw new RangeError(`credit timeout ${timeout} is negative`);
    }
    this.#creditTimeout = timeout;
    // Restarted with the new bound, if it runs.
    this.#stopCredit();
    this._armCredit();
  }

  /** Whether a stream item can go now: the window has room, and nothing waits for it before the item. */
  get #itemCanGo(): boolean {
    return this._window !== 0 && (this.#queue === undefined || this.#queue.length === 0);
  }

  /** The items in {@link #queue}: replies that wait for the window, each taking one item of it. */
  get _queuedItems(): number {
    let count = 0;
    for (const queued of this.#queue ?? []) {
      if (queued.counts) {
        count++;
      }
    }
    return count;
  }

  /**
   * Counts one item sent against the window. Returns whether it did: not
   * for an unlimited window, nor at 0 (an item `forwardMessage` relays,
   * which keeps no window of its own).
   */
  #take(): boolean {
    const window = this._window;
    if (window === undefined || window === 0) {
      return false;
    }
    this._window = window - 1;
    return true;
  }

  /** An item counted by {@link #take} was not sent after all. */
  #untake(): void {
    this._window = (this._window as number) + 1;
  }

  /** Queues [queued] behind what waits for the window. */
  #enqueue(queued: Queued): void {
    this.#queue ??= [];
    this.#queue.push(queued);
    this._armCredit();
  }

  /** Sends what waits for the window, in order, while it has room. */
  #flush(): void {
    const queue = this.#queue;
    if (queue === undefined || this.#flushing) {
      return;
    }
    this.#flushing = true;
    try {
      while (queue.length > 0 && !this._finished) {
        const head = queue[0] as Queued;
        if (head.counts && this._window === 0) {
          break;
        }
        queue.shift();
        this.#sendQueued(head);
      }
    } finally {
      this.#flushing = false;
    }
  }

  /** Sends [queued], counted against the window if it is an item; a send that fails drops it with the failure. */
  #sendQueued(queued: Queued): void {
    const took = queued.counts && this.#take();
    try {
      queued.send();
    } catch (e) {
      if (took) {
        this.#untake();
      }
      if (!(e instanceof SwitchboardError)) {
        this.channel._log('warn', `reply to request ${this.requestId} not sent`, e);
      }
      queued.drop(asSwitchboardError(e));
    }
  }

  /** Drops what waits for the window: the request ended before it could go. */
  #dropQueue(error: SwitchboardError): void {
    const queue = this.#queue;
    if (queue === undefined || queue.length === 0) {
      return;
    }
    const dropped = queue.splice(0);
    this.#finalQueued = false;
    for (const queued of dropped) {
      try {
        queued.drop(error);
      } catch (e) {
        this.channel._log('error', `dropping a reply to request ${this.requestId} failed`, e);
      }
    }
  }

  /**
   * The peer granted [items] more items of the window. Returns the reason
   * it is a channel protocol error, or undefined: a credit for a request
   * that is not a stream request, for an unlimited window, or that would
   * bring the window to the unlimited value or beyond.
   */
  _onCredit(items: number): string | undefined {
    if (!this.expectsStream) {
      return `credit for request ${this.requestId}, not a stream request`;
    }
    const window = this._window;
    if (window === undefined) {
      return `credit for request ${this.requestId}, whose window is unlimited`;
    }
    if (window + items >= TalkFrame.unlimitedWindow) {
      return `credit of ${items} items for request ${this.requestId} overflows its window (${window} left)`;
    }
    this._window = window + items;
    this.#awaitingGrant = false;
    // The wait for a grant ended; a new one starts afresh.
    this.#stopCredit();
    if (window === 0) {
      // The responder timeout restarts, as a reply would.
      const expiry = this._expiry;
      if (expiry !== undefined && !this._finished) {
        expiry.reply(this.channel._clock.now());
      }
    }
    // What waits goes first, in its order, before anything the credit lets
    // a forwarded request's responder send.
    this.#flush();
    const hook = this._creditHook;
    if (hook !== undefined) {
      const owed = items < this._creditOwed ? items : this._creditOwed;
      this._creditOwed -= owed;
      if (items > owed) {
        try {
          hook(items - owed);
        } catch (e) {
          this.channel._log('error', `forwarding a credit of request ${this.requestId} failed`, e);
        }
      }
    }
    this.#notifyCredit();
    this.#arm();
    this._armCredit();
    return undefined;
  }

  /** Resolves {@link whenCredit} once the window has room, or the request ended. */
  #notifyCredit(): void {
    const waiter = this.#creditWaiter;
    if (waiter !== undefined && (this._window !== 0 || this._finished || this._cancelled)) {
      this.#creditWaiter = undefined;
      waiter.resolve();
    }
  }

  /**
   * Runs the credit bound while the window is exhausted and this side has
   * something to send (a reply waiting, {@link whenCredit} waited for), or
   * the request opened with window 0 has had no grant yet; stops it
   * otherwise. Once running, it keeps the time the wait began.
   */
  _armCredit(): void {
    const wanted =
      !this._finished &&
      !this._timerDeferred &&
      this._window === 0 &&
      (this.#awaitingGrant ||
        (this.#queue !== undefined && this.#queue.length > 0) ||
        this.#creditWaiter !== undefined);
    if (!wanted) {
      this.#stopCredit();
      return;
    }
    if (this.#creditEnd !== undefined) {
      return;
    }
    const bound = this.#creditTimeout ?? this.channel.options.creditTimeout;
    if (bound <= 0) {
      return;
    }
    this.#creditEnd = this.channel._clock.now() + bound;
    this.#armCreditTimer();
  }

  #armCreditTimer(): void {
    const clock = this.channel._clock;
    if (this.#creditTimer !== undefined) {
      clock.clearTimeout(this.#creditTimer);
    }
    let left = (this.#creditEnd as number) - clock.now();
    if (left < 0) {
      left = 0;
    }
    this.#creditTimer =
      left > maxTimerDelay
        ? clock.setTimeout(() => this.#armCreditTimer(), maxTimerDelay)
        : clock.setTimeout(() => this.#onCreditTimeout(), left);
  }

  #stopCredit(): void {
    if (this.#creditTimer !== undefined) {
      this.channel._clock.clearTimeout(this.#creditTimer);
      this.#creditTimer = undefined;
    }
    this.#creditEnd = undefined;
  }

  #onCreditTimeout(): void {
    this.#creditTimer = undefined;
    this.#creditEnd = undefined;
    if (this._finished) {
      return;
    }
    const bound = this.#creditTimeout ?? this.channel.options.creditTimeout;
    this.channel._log(
      'debug',
      `no credit for request ${this.requestId} within ${bound} ms, aborting`,
    );
    const status = Status.of(StatusCode.deadlineExceeded, `no credit within ${bound} ms`);
    this._abortQuietly(status);
    this.#markCancelled(undefined, status);
  }

  #check(item = false): void {
    if (!this.expectsReply) {
      throw SwitchboardError.of(StatusCode.failedPrecondition, 'message expects no reply');
    }
    if (!this.channel.isOpen) {
      throw SwitchboardError.of(StatusCode.failedPrecondition, 'channel closed');
    }
    if (this._finished || this.#finalizing || this.#finalQueued) {
      throw SwitchboardError.of(
        StatusCode.failedPrecondition,
        `request ${this.requestId} already answered`,
      );
    }
    if (item && !this.expectsStream) {
      throw SwitchboardError.of(
        StatusCode.failedPrecondition,
        `request ${this.requestId} is not a stream request`,
      );
    }
  }

  /**
   * Sends a reply to this request with [send]: the final one if
   * [isFinal], which finishes the request, else a stream item or an
   * EXTEND. Returns what [send] returns. A cancel of this request that
   * arrives while [send] runs is held until it returns, then handled as if
   * it had arrived just after: ignored if the final reply went out.
   */
  #sendReply<T>(send: () => T, isFinal = false): T {
    // Forwarding can relay a reply while another one is being sent (never
    // within a final reply: the check refuses); the outermost send handles
    // a held cancel.
    const outer = this.#sending;
    this.#sending = true;
    this.#finalizing = isFinal;
    try {
      const result = send();
      if (isFinal) {
        this.#finish();
      }
      return result;
    } finally {
      this.#sending = outer;
      this.#finalizing = false;
      const cancel = outer ? undefined : this.#heldCancel;
      if (cancel !== undefined) {
        this.#heldCancel = undefined;
        if (!this._finished) {
          this._cancelledByPeer(cancel);
        }
      }
    }
  }

  // -------------------------------------------------------------------
  // Reply API

  /**
   * Sends the final response. A payload too large for the channel's frames
   * goes as a bulk payload transparently; with `bulk`, the payload is that
   * byte source and [payload] must be empty. Made while items wait for the
   * item window, it goes after them. The promise resolves once the reply
   * was handed to the connection (for a bulk payload, once that payload
   * went whole) and is safe to drop.
   *
   * @throws {SwitchboardError} FAILED_PRECONDITION if the message expects
   * no reply, was already answered, or the channel is closed.
   */
  reply(payload?: PayloadLike, options?: ReplyOptions): Promise<void> {
    const procedure = procedureOf(options?.procedure);
    const bytes = toPayload(payload);
    const bulk = this.channel._bulkPayload(bytes, options?.bulk, options?.bulkLength);
    if (bulk === undefined) {
      return this._reply(bytes, procedure);
    }
    const sent = new Sent();
    this._reply(bytes, procedure, { bulk, onBulk: sent.started, onFailure: sent.failed });
    return sent.done;
  }

  _reply(
    payload: Uint8Array,
    procedure: Name | undefined,
    init: ReplySendOptions = {},
  ): Promise<void> {
    this.#check();
    const frame = new TalkFrame({
      kind: TalkKind.message,
      procedure,
      responseId: this.requestId,
      payload,
    });
    const send = (): Promise<void> =>
      this.#sendReply(
        () =>
          this.channel._sendFrame(frame, {
            bulk: init.bulk,
            onBulk: (out) => {
              out.track(this._bulkOuts);
              init.onBulk?.(out);
            },
            onFailure: init.onFailure,
            onAccepted: init.onAccepted,
          }),
        true,
      );
    if (this.#queue === undefined || this.#queue.length === 0) {
      return send();
    }
    // Never counted, but it goes after the items before it.
    this.#finalQueued = true;
    const sent = new Deferred<void>();
    this.#enqueue({
      counts: false,
      send: () => {
        this.#finalQueued = false;
        sent.resolve(send());
      },
      drop: (error) => {
        this.#finalQueued = false;
        // Never sent: what onAccepted gives back is not held for it.
        init.onAccepted?.();
        init.onFailure?.(error);
        sent.reject(error);
      },
    });
    return this.channel._quiet(sent.promise, frame);
  }

  replyRequest(payload?: PayloadLike, options?: ReplyRequestOptions): Promise<TalkMessage> {
    return this.startReplyRequest(payload, options).response;
  }

  startReplyRequest(payload?: PayloadLike, options?: ReplyRequestOptions): TalkRequest {
    const bytes = toPayload(payload);
    return new TalkRequestHandle(
      this._startReplyRequest(
        bytes,
        procedureOf(options?.procedure),
        this.#requestInit(false, bytes, options),
      ),
    );
  }

  replyStreamRequest(payload?: PayloadLike, options?: ReplyRequestOptions): TalkStream {
    const bytes = toPayload(payload);
    return new TalkStreamHandle(
      this._startReplyRequest(
        bytes,
        procedureOf(options?.procedure),
        this.#requestInit(true, bytes, options),
      ),
    );
  }

  /**
   * The internal options of a reply request made through the public API.
   *
   * @throws {TypeError} for a [payload] that is not empty with `bulk`, and
   * {@link RangeError} for a window or a bulk length out of range.
   */
  #requestInit(
    stream: boolean,
    payload: Uint8Array,
    options: ReplyRequestOptions | undefined,
  ): ReplyRequestInit {
    return {
      stream,
      timeout: options?.timeout,
      window: stream ? windowOf(options?.window) : undefined,
      onExtend: options?.onExtend,
      bulk: this.channel._bulkPayload(payload, options?.bulk, options?.bulkLength),
      signal: options?.signal,
    };
  }

  _startReplyRequest(
    payload: Uint8Array,
    procedure: Name | undefined,
    init: ReplyRequestInit,
  ): Outgoing {
    this.#check();
    const start = (deferred = false): Outgoing =>
      this.channel._startRequest({
        stream: init.stream,
        window: init.window,
        timeout: init.timeout,
        onExtend: init.onExtend,
        sink: init.sink,
        bulk: init.bulk,
        onAccepted: init.onAccepted,
        deferred,
        signal: init.signal,
        build: (id, window) =>
          new TalkFrame({
            kind: TalkKind.message,
            procedure,
            requestId: id,
            responseId: this.requestId,
            stream: init.stream,
            window,
            payload,
          }),
      });
    if (this.#queue === undefined || this.#queue.length === 0) {
      return this.#sendReply(() => start(), true);
    }
    // Never counted, but it goes after the items before it; its handle is
    // there at once.
    const pending = start(true);
    this.#finalQueued = true;
    this.#enqueue({
      counts: false,
      send: () => {
        this.#finalQueued = false;
        this.#sendReply(pending.send as () => void, true);
      },
      drop: (error) => {
        this.#finalQueued = false;
        init.onAccepted?.();
        this.channel._dropOutgoing(pending, error);
      },
    });
    return pending;
  }

  /**
   * Sends one stream item; restarts the responder timeout. With a finite
   * item window, an item made while the window is exhausted (or while
   * other replies wait for it) is queued, nothing on the wire, and its
   * promise resolves once a grant let it go; it rejects with the reason
   * when the request ends first.
   *
   * @throws {SwitchboardError} FAILED_PRECONDITION like {@link reply}, and
   * if the request is not a stream request.
   */
  replyItem(payload?: PayloadLike, options?: ReplyOptions): Promise<void> {
    const procedure = procedureOf(options?.procedure);
    const bytes = toPayload(payload);
    const bulk = this.channel._bulkPayload(bytes, options?.bulk, options?.bulkLength);
    if (bulk === undefined) {
      return this._replyItem(bytes, procedure);
    }
    const sent = new Sent();
    this._replyItem(bytes, procedure, { bulk, onBulk: sent.started, onFailure: sent.failed });
    return sent.done;
  }

  /**
   * Sends one stream item. With [windowed] (the reply API) it waits for
   * the item window; without (an item `forwardMessage` relays), it goes at
   * once, counted if the window has room.
   */
  _replyItem(
    payload: Uint8Array,
    procedure: Name | undefined,
    init: ReplySendOptions = {},
    windowed = true,
  ): Promise<void> {
    this.#check(true);
    const frame = new TalkFrame({
      kind: TalkKind.streamItem,
      procedure,
      responseId: this.requestId,
      payload,
    });
    const send = (): Promise<void> => {
      const sent = this.#sendReply(() =>
        this.channel._sendFrame(frame, {
          bulk: init.bulk,
          onBulk: (out) => {
            out.track(this._bulkOuts);
            // An item's payload going out is progress, as the item was.
            out.onProgress = () => this.#replied();
            init.onBulk?.(out);
          },
          onFailure: init.onFailure,
          onAccepted: init.onAccepted,
        }),
      );
      this.#replied();
      return sent;
    };
    if (!windowed || this.#itemCanGo) {
      const took = this.#take();
      try {
        return send();
      } catch (e) {
        if (took) {
          this.#untake();
        }
        throw e;
      }
    }
    const sent = new Deferred<void>();
    this.#enqueue({
      counts: true,
      send: () => {
        sent.resolve(send());
      },
      drop: (error) => {
        init.onAccepted?.();
        init.onFailure?.(error);
        sent.reject(error);
      },
    });
    return this.channel._quiet(sent.promise, frame);
  }

  replyItemRequest(payload?: PayloadLike, options?: ReplyRequestOptions): Promise<TalkMessage> {
    return this.startReplyItemRequest(payload, options).response;
  }

  startReplyItemRequest(payload?: PayloadLike, options?: ReplyRequestOptions): TalkRequest {
    const bytes = toPayload(payload);
    return new TalkRequestHandle(
      this._startItemRequest(
        bytes,
        procedureOf(options?.procedure),
        this.#requestInit(false, bytes, options),
      ),
    );
  }

  replyItemStreamRequest(payload?: PayloadLike, options?: ReplyRequestOptions): TalkStream {
    const bytes = toPayload(payload);
    return new TalkStreamHandle(
      this._startItemRequest(
        bytes,
        procedureOf(options?.procedure),
        this.#requestInit(true, bytes, options),
      ),
    );
  }

  /**
   * Sends one stream item that is itself a request, waiting for the item
   * window as {@link _replyItem} does: queued, its request registered (its
   * handle is returned at once) but not sent.
   */
  _startItemRequest(
    payload: Uint8Array,
    procedure: Name | undefined,
    init: ReplyRequestInit,
    windowed = true,
  ): Outgoing {
    this.#check(true);
    const start = (deferred = false): Outgoing =>
      this.channel._startRequest({
        stream: init.stream,
        window: init.window,
        timeout: init.timeout,
        onExtend: init.onExtend,
        sink: init.sink,
        bulk: init.bulk,
        onAccepted: init.onAccepted,
        deferred,
        signal: init.signal,
        build: (id, window) =>
          new TalkFrame({
            kind: TalkKind.streamItem,
            procedure,
            requestId: id,
            responseId: this.requestId,
            stream: init.stream,
            window,
            payload,
          }),
      });
    if (!windowed || this.#itemCanGo) {
      const took = this.#take();
      try {
        const pending = this.#sendReply(() => start());
        this.#replied();
        return pending;
      } catch (e) {
        if (took) {
          this.#untake();
        }
        throw e;
      }
    }
    const pending = start(true);
    this.#enqueue({
      counts: true,
      send: () => {
        this.#sendReply(pending.send as () => void);
        this.#replied();
      },
      drop: (error) => {
        init.onAccepted?.();
        this.channel._dropOutgoing(pending, error);
      },
    });
    return pending;
  }

  /**
   * Answers a stream request with [items]: each one is sent with
   * {@link replyItem} as it arrives, then the final reply carries the
   * trailer (empty by default). The source is not read while an item
   * waits for the channel's send window or for a grant of the item window.
   * An error from the source ends the answer with {@link replyAbort}: its
   * status if it is a {@link SwitchboardError}, otherwise INTERNAL. When
   * the request is cancelled the source is stopped and nothing more is
   * sent. Resolves once the answer has ended; never rejects.
   *
   * @throws {SwitchboardError} FAILED_PRECONDITION like {@link replyItem},
   * without reading [items].
   */
  replyStream(
    items: AsyncIterable<PayloadLike> | Iterable<PayloadLike>,
    options?: ReplyOptions & { trailer?: PayloadLike; creditTimeout?: number | undefined },
  ): Promise<void> {
    const wire = procedureOf(options?.procedure);
    this._checkStreamReply();
    if (options?.creditTimeout !== undefined) {
      this.setCreditTimeout(options.creditTimeout);
    }
    const done = new Deferred<void>();
    let iterator: AsyncIterator<PayloadLike> | Iterator<PayloadLike> | undefined;
    let stopping = false;
    const finish = (): void => {
      done.resolve();
    };
    const stop = (): void => {
      if (stopping || done.isSettled) {
        return;
      }
      stopping = true;
      const it = iterator;
      if (it?.return !== undefined) {
        try {
          const result = it.return();
          if (result instanceof Promise) {
            result.catch((e: unknown) =>
              this.channel._log('debug', 'cancelling stream reply source failed', e),
            );
          }
        } catch (e) {
          this.channel._log('debug', 'cancelling stream reply source failed', e);
        }
      }
      finish();
    };
    const abort = (error: unknown): void => {
      let status =
        error instanceof SwitchboardError
          ? error.status
          : Status.of(StatusCode.internal, 'stream failed');
      if (!(error instanceof SwitchboardError)) {
        this.channel._log('warn', `stream reply to request ${this.requestId} failed`, error);
      }
      if (status.isOk) {
        status = Status.of(StatusCode.internal, 'stream failed');
      }
      if (this.canReply) {
        try {
          void this.replyAbort(status);
        } catch (e) {
          this.channel._log(
            'debug',
            `stream reply abort not sent: ${asSwitchboardError(e).status}`,
          );
        }
      }
    };
    const run = async (): Promise<void> => {
      try {
        const asyncIterable = items as AsyncIterable<PayloadLike>;
        iterator =
          typeof asyncIterable[Symbol.asyncIterator] === 'function'
            ? asyncIterable[Symbol.asyncIterator]()
            : (items as Iterable<PayloadLike>)[Symbol.iterator]();
      } catch (e) {
        abort(e);
        finish();
        return;
      }
      for (;;) {
        let next: IteratorResult<PayloadLike>;
        try {
          next = await iterator.next();
        } catch (e) {
          if (!stopping) {
            abort(e);
          }
          stop();
          return;
        }
        if (stopping || done.isSettled) {
          return;
        }
        if (next.done === true) {
          if (this.canReply) {
            try {
              void this.reply(options?.trailer, { procedure: wire });
            } catch (e) {
              abort(e);
            }
          }
          finish();
          return;
        }
        if (!this.canReply) {
          stop();
          return;
        }
        let sent: Promise<void>;
        try {
          sent = this.replyItem(next.value, { procedure: wire });
        } catch (e) {
          abort(e);
          stop();
          return;
        }
        // The source waits while the item waits for the window, the
        // channel's or the request's item window; a failed item (the
        // channel ended) resumes it, for the end to be seen.
        await sent.then(noop, noop);
      }
    };
    void this.onCancel.then(stop);
    void run();
    return done.promise;
  }

  /** Throws like {@link replyItem} when no stream answer can start. */
  _checkStreamReply(): void {
    if (!this.expectsReply || !this.expectsStream || !this.canReply) {
      throw SwitchboardError.of(
        StatusCode.failedPrecondition,
        !this.expectsReply
          ? 'message expects no reply'
          : !this.expectsStream
            ? `request ${this.requestId} is not a stream request`
            : `request ${this.requestId} cannot be answered`,
      );
    }
  }

  /**
   * Sends the final response as an abort carrying [status]. Application
   * codes are allowed; CONNECTION_LOST goes as UNAVAILABLE.
   *
   * @throws {RangeError} if [status] is OK, otherwise like {@link reply}.
   */
  replyAbort(status: Status): Promise<void> {
    checkAbortStatus(status);
    return this._replyAbort(status);
  }

  _replyAbort(status: Status): Promise<void> {
    this.#check();
    this.#dropError ??= new SwitchboardError(status);
    return this.#sendReply(
      () =>
        this.channel._sendChecked(
          new TalkFrame({
            kind: TalkKind.abort,
            responseId: this.requestId,
            payload: wireAbortStatus(status).encode(),
          }),
        ),
      true,
    );
  }

  /**
   * Sends EXTEND: tells the requester how long the answer may still take.
   * With `deadline`, within that from now; with `renew`, within that of
   * the last reply. The requester waits `buffer` longer (default
   * `extendBuffer`); this side's own timeout does not. With neither,
   * restarts the default gap on both sides.
   *
   * @throws {RangeError} for a negative value, otherwise like {@link reply}.
   */
  extend(options?: { deadline?: number; renew?: number; buffer?: number }): Promise<void> {
    const deadline = options?.deadline;
    const renew = options?.renew;
    const buffer = options?.buffer;
    for (const [name, value] of [
      ['deadline', deadline],
      ['renew', renew],
      ['buffer', buffer],
    ] as const) {
      if (value !== undefined && !(value >= 0)) {
        throw new RangeError(`${name} ${value} must not be negative`);
      }
    }
    this.#check();
    const extra = buffer ?? this.channel.options.extendBuffer;
    // A field holds at most maxExtend, buffer included. A longer value is
    // lowered so that the local timeout stays the buffer short of what the
    // requester is told: this responder still gives up first.
    const room = extra < maxExtend ? maxExtend - extra : 0;
    const lowered = (value: number | undefined): number | undefined =>
      value === undefined || value <= room ? value : room;
    const localDeadline = lowered(deadline);
    const localRenew = lowered(renew);
    const frame = new TalkFrame({
      kind: TalkKind.extend,
      responseId: this.requestId,
      payload: TalkFrame.extendPayload({
        deadline: localDeadline === undefined ? undefined : localDeadline + extra,
        renew: localRenew === undefined ? undefined : localRenew + extra,
      }),
    });
    const sent = this.#sendReply(() => this.channel._sendChecked(frame));
    const expiry = this._expiry;
    if (expiry === undefined || this._finished) {
      return sent;
    }
    const now = this.channel._clock.now();
    if (localDeadline === undefined && localRenew === undefined) {
      expiry.restartGap(now);
    } else {
      expiry.declare(now, localDeadline, localRenew);
    }
    this.#arm();
    return sent;
  }

  /** Sends an EXTEND with [payload] as received from the far responder, for forwarding: buffers already on the wire stay as they are. */
  _extendRaw(payload: Uint8Array, onAccepted?: () => void): void {
    this.#check();
    const frame = new TalkFrame({ kind: TalkKind.extend, responseId: this.requestId, payload });
    this.#sendReply(() => this.channel._sendChecked(frame, onAccepted));
    const expiry = this._expiry;
    if (expiry === undefined || this._finished) {
      return;
    }
    const { deadline, renew } = frame.extension;
    const now = this.channel._clock.now();
    if (payload.length === 0) {
      expiry.restartGap(now);
    } else {
      expiry.declare(now, deadline, renew);
    }
    this.#arm();
  }

  setReplyTimeout(timeout: number | undefined): void {
    if (timeout !== undefined && !(timeout >= 0)) {
      throw new RangeError(`reply timeout ${timeout} is negative`);
    }
    this.#replyTimeout = timeout;
    if (!this._finished && this.expectsReply) {
      this._startTimer();
    }
  }

  /** Starts the responder timeout, in the default regime. */
  _startTimer(): void {
    this._expiry ??= new Expiry(0);
    const expiry = this._expiry;
    expiry.gap = this.#replyTimeout ?? this.channel.options.replyTimeout;
    expiry.start(this.channel._clock.now());
    this.#arm();
  }

  /** A stream item (possibly a request) was sent. Nothing to restart if the request ended meanwhile. */
  #replied(): void {
    const expiry = this._expiry;
    if (expiry === undefined || this._finished) {
      return;
    }
    expiry.reply(this.channel._clock.now());
    this.#arm();
  }

  #stopTimer(): void {
    if (this.#timer !== undefined) {
      this.channel._clock.clearTimeout(this.#timer);
      this.#timer = undefined;
    }
  }

  #arm(): void {
    // While the item window is exhausted the responder timeout does not
    // run: the credit bound does.
    const end = this._finished || this._window === 0 ? undefined : this._expiry?.end;
    if (end !== undefined && end === this.#armedEnd && this.#timer !== undefined) {
      return;
    }
    this.#stopTimer();
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
      this.#timer = clock.setTimeout(() => {
        this.#timer = undefined;
        this.#arm();
      }, maxTimerDelay);
    } else {
      this.#timer = clock.setTimeout(() => this.#onTimeout(), left);
    }
  }

  #onTimeout(): void {
    this.#timer = undefined;
    if (this._finished) {
      return;
    }
    this.channel._log('debug', `no reply to request ${this.requestId}, aborting`);
    const status = Status.of(StatusCode.deadlineExceeded, 'no reply in time');
    this._abortQuietly(status);
    this.#markCancelled(undefined, status);
  }

  /**
   * Answers with an abort on the channel's behalf, if still unanswered.
   * Never while the final reply is being sent: that is the one final.
   */
  _abortQuietly(status: Status): void {
    if (this._finished || this.#finalizing || !this.expectsReply) {
      return;
    }
    this.#dropError ??= new SwitchboardError(status);
    this.#sendReply(() => this.channel._rejectRequest(this.requestId, status), true);
  }

  /**
   * The handler this request was delivered to failed before answering it:
   * answers ABORT INTERNAL on its behalf and, as on the responder timeout,
   * marks the request cancelled, so that work started on it stops. A
   * final reply made before it failed answered it, also one that waits
   * behind items for the item window.
   */
  _handlerFailed(): void {
    if (this._finished || this.#finalizing || this.#finalQueued || !this.expectsReply) {
      return;
    }
    const status = Status.of(StatusCode.internal, 'message handler failed');
    this._abortQuietly(status);
    this.#markCancelled(undefined, status);
  }

  /**
   * The peer cancelled the request: answer it with the final the protocol
   * requires, then tell the application. Held while a reply is being sent.
   */
  _cancelledByPeer(status: Status): void {
    if (this.#sending) {
      this.#heldCancel ??= status;
      return;
    }
    this._abortQuietly(Status.of(StatusCode.cancelled, 'cancelled by requester'));
    this.#markCancelled(status, status);
  }

  #finish(): void {
    this._finished = true;
    this.#stopTimer();
    this.#stopCredit();
    this.channel._release(this);
    // Answered: a payload the application did not read is not needed.
    this._bulk?.abandon();
    // An abort went before what waited for the window.
    this.#dropQueue(
      this.#dropError ??
        SwitchboardError.of(
          StatusCode.failedPrecondition,
          `request ${this.requestId} ended before the reply was sent`,
        ),
    );
    this.#notifyCredit();
  }

  /** The reason the signal of a cancelled request carries. */
  #cancelReason(): SwitchboardError {
    return new SwitchboardError(
      this.#cancelCause ?? Status.of(StatusCode.cancelled, 'request cancelled'),
    );
  }

  /** Marks the request cancelled: [peerStatus] is the peer's cancel, if it cancelled; [cause] what the signal reports. */
  #markCancelled(peerStatus?: Status, cause?: Status): void {
    if (this._cancelled) {
      return;
    }
    this._cancelled = true;
    this._cancelStatus = peerStatus;
    this.#cancelCause = cause;
    // The request's own payload and the payloads of the replies to it
    // stop.
    const cancelled = Status.of(StatusCode.cancelled, 'request cancelled');
    this._bulk?.close(cancelled);
    this._bulk?.abandon();
    if (!this._ready) {
      this._drop();
    }
    for (const out of this._bulkOuts) {
      out.cancel(cancelled);
    }
    this.#cancel ??= new Deferred<void>();
    this.#cancel.resolve();
    this.#abort?.abort(this.#cancelReason());
    this.#notifyCredit();
    const hook = this._cancelHook;
    this._cancelHook = undefined;
    if (hook !== undefined) {
      try {
        hook();
      } catch (e) {
        this.channel._log('error', `cancel handling of request ${this.requestId} failed`, e);
      }
    }
  }

  /** The request can no longer be answered (channel closed or id reused): what waits for the window fails with [error]. */
  _abandon(error?: SwitchboardError): void {
    this._finished = true;
    this.#stopTimer();
    this.#stopCredit();
    this.#dropQueue(
      error ??
        SwitchboardError.of(
          StatusCode.failedPrecondition,
          `request ${this.requestId} can no longer be answered`,
        ),
    );
    this.#markCancelled(undefined, error?.status);
  }

  toString(): string {
    return `TalkMessage(${this.frame})`;
  }
}
