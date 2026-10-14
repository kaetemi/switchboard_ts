/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * The Talk channel (wiki page "Polyverse Switchboard Talk"): message chains
 * over one channel.
 */

import { ChannelAddress } from '../address/channel-address.ts';
import { type PayloadLike, toPayload } from '../bytes.ts';
import type { MuxChannel, MuxChannelCarrier } from '../mux/contract.ts';
import { type NameLike, toName } from '../name.ts';
import { Status, StatusCode, SwitchboardError } from '../status.ts';
import { type Clock, systemClock, type TimerHandle } from '../util/clock.ts';
import { Deferred } from '../util/deferred.ts';
import { type Logger, type LogLevel, noopLogger } from '../util/logger.ts';
import {
  type BulkSource,
  type RequestOptions,
  type SendOptions,
  type TalkChannel as TalkChannelContract,
  type TalkMessage,
  type TalkOptions,
  type TalkRequest,
  type TalkStream,
  talkOptions,
} from './contract.ts';
import { TalkBulkOpen, TalkBulkReference } from './talk-bulk.ts';
import {
  type BulkTarget,
  installBulkRouting,
  lingerBulkTarget,
  markBulkChannel,
  registerBulkTarget,
  routeBulk,
} from './talk-bulk-routes.ts';
import { type Awaited, BulkIn, BulkOut, BulkPayload, Lane } from './talk-bulk-transfer.ts';
import { Delivery, Held } from './talk-delivery.ts';
import { TalkFrame, TalkKind, talkKindName } from './talk-frame.ts';
import { IncomingMessage } from './talk-message.ts';
import {
  type ExtendCallback,
  Outgoing,
  type ResponseSink,
  TalkRequestHandle,
} from './talk-request.ts';
import { TalkStreamHandle } from './talk-stream.ts';
import {
  abortStatus,
  asSwitchboardError,
  checkAbortStatus,
  isThenable,
  maxTimerDelay,
  noop,
  once,
  rejected,
  TalkAbortError,
  talkLoggerName,
  windowOf,
  wireAbortStatus,
  wireStatus,
} from './talk-util.ts';

export { TalkAbortError } from './talk-util.ts';

/**
 * A channel of whole Talk frames that is not a mux channel: each chunk of
 * `readable` and each write to `writable` is one frame. Nothing is flow
 * controlled over it, and bulk payloads cannot travel on it. With `close`
 * and `done` (a channel that ends with a status, as a mux channel does)
 * the Talk channel closes it with its status and reads its end status from
 * `done`; without them it closes `writable` and ends OK.
 */
export interface TalkFrameChannel {
  readonly readable: ReadableStream<Uint8Array>;
  readonly writable: WritableStream<Uint8Array>;
  close?(status?: Status): Promise<void>;
  readonly done?: Promise<Status>;
}

/** What a Talk channel runs over: a mux channel, a channel carrying one (a slot channel), or any channel of whole frames. */
export type TalkRawChannel = MuxChannel | MuxChannelCarrier | TalkFrameChannel;

/** A channel that ends with a status. */
interface StatusClosable {
  close(status?: Status): Promise<void>;
  readonly done: Promise<Status>;
}

function isMuxChannel(raw: TalkRawChannel): raw is MuxChannel {
  const r = raw as Partial<MuxChannel>;
  return typeof r.openAfter === 'function' && typeof r.consumed === 'function';
}

function isCarrier(raw: TalkRawChannel): raw is MuxChannelCarrier {
  const r = raw as Partial<MuxChannelCarrier>;
  return (
    !isMuxChannel(raw) &&
    typeof r.consumed === 'function' &&
    typeof r.send === 'function' &&
    r.channel !== undefined
  );
}

function isStatusClosable(raw: TalkRawChannel): raw is TalkRawChannel & StatusClosable {
  const r = raw as Partial<StatusClosable>;
  return typeof r.close === 'function' && isThenable(r.done);
}

/** A frame waiting for the peer's LIMITS: sent in order once the limit is known, or dropped with the channel, which [drop] (if any) hears of. */
interface Deferral {
  readonly send: () => void;
  readonly drop?: ((error: SwitchboardError) => void) | undefined;
}

/** What {@link TalkChannel._sendFrame} takes besides the frame. */
export interface SendFrameOptions {
  /** The payload goes on a bulk channel, from this. */
  readonly bulk?: BulkPayload | undefined;
  /** Gets the bulk transfer, once it started. */
  readonly onBulk?: ((out: BulkOut) => void) | undefined;
  /** Called once the frame went (or its bulk transfer started), with the transfer if any. */
  readonly onSent?: ((out: BulkOut | undefined) => void) | undefined;
  /** Gets a failure of a frame that waited for the peer's LIMITS, instead of it being logged. */
  readonly onFailure?: ((error: SwitchboardError) => void) | undefined;
  /** Forwarding: called once the frame was handed to the connection, or discarded. */
  readonly onAccepted?: (() => void) | undefined;
}

/** What {@link TalkChannel._startRequest} takes. */
export interface StartRequestInit {
  readonly stream: boolean;
  readonly timeout: number | undefined;
  /** Builds the frame from the request id and the item window to put on the wire. */
  readonly build: (id: number, window: number) => TalkFrame;
  /** The item window as a wire value; undefined or {@link TalkFrame.unlimitedWindow}: unlimited. */
  readonly window?: number | undefined;
  readonly onExtend?: ExtendCallback | undefined;
  readonly sink?: ResponseSink | undefined;
  readonly ordered?: boolean | undefined;
  readonly bulk?: BulkPayload | undefined;
  readonly onAccepted?: (() => void) | undefined;
  /** Registered and returned, but not sent until `Outgoing.send` is called. */
  readonly deferred?: boolean | undefined;
  readonly signal?: AbortSignal | undefined;
}

const closedChannel = (): SwitchboardError =>
  SwitchboardError.of(StatusCode.failedPrecondition, 'channel closed');

/**
 * Talk message chains over one channel: plain messages, requests,
 * responses, stream responses, aborts, timeout extension, item windows and
 * cancellation. See the wiki page "Polyverse Switchboard Talk".
 * `forwardMessage` forwards message chains between channels.
 *
 * Works over a mux channel, a channel carrying one (a slot channel), or
 * any channel whose chunks are whole Talk frames ({@link TalkFrameChannel}).
 *
 * Delivery order: {@link messages} delivers incoming plain messages and
 * requests in wire order, and the items of a stream request theirs, but
 * the answers to our requests settle promises of their own, so an answer
 * can be seen before messages that arrived ahead of it. A request started
 * with `ordered: true` has its answer delivered in wire order with
 * {@link messages} instead (see {@link startRequest}).
 *
 * Flow control: over a mux channel, Talk returns the credit of a frame
 * when the application has taken what it carried: a plain message or a
 * request when {@link messages} hands it over, a stream item when the
 * items of its request do, the answer to an `ordered` request when it is
 * delivered in its place. Frames the channel consumes itself (a final
 * response settling a promise, aborts, EXTEND, cancels, frames for ids it
 * does not know or gave up on, requests it refuses, messages nobody will
 * read) return their credit at once. What `forwardMessage` forwards
 * returns its credit once the frame forwarded for it went out on the other
 * channel. A consumer that does not take messages therefore stalls its
 * channel once the window is used up, and only that channel. On the
 * sending side, {@link send} and the reply methods return a promise that
 * resolves once the frame was handed to the connection within the peer's
 * window (for a bulk payload, once the payload went whole): a sender that
 * awaits it is paced by the receiver, one that does not queues what the
 * window does not take.
 *
 * Bulk payloads (wiki page "Polyverse Switchboard Talk", "Bulk payloads"),
 * over a mux channel: a message too large for one frame (the channel's
 * `maxSubframeLength`, or `bulkThreshold`) has its payload sent on a bulk
 * channel of its own, transparently; the `bulk` option sends a byte
 * source that never sits in memory. A received bulk payload is reassembled
 * before its message is delivered, up to `maxInlinePayload`, so that
 * {@link TalkMessage.payload} reads it like an inline one; messages
 * `streamBulk` selects are delivered at once instead, the payload read as
 * it arrives ({@link TalkMessage.bulk}). Either way a message keeps its
 * place in the order. What the channel holds for payloads the application
 * has not taken is bounded by `reassemblyBudget`, as backpressure.
 *
 * Failure handling:
 *
 * - A malformed frame or a violation of the kind rules from the peer
 *   closes the channel with PROTOCOL_ERROR; outstanding requests fail with
 *   it.
 * - A channel abort from the peer closes the channel with the peer's
 *   status; {@link messages} throws it as a {@link TalkAbortError} after
 *   what it holds, and outstanding requests fail with the same error.
 * - When the raw channel ends, outstanding requests fail with its end
 *   status if it is not OK, else with CONNECTION_LOST.
 * - CONNECTION_LOST is local only: wherever it would be sent it goes on the
 *   wire as UNAVAILABLE.
 * - Errors thrown by the handler of {@link serve} never affect the
 *   channel: a handler that throws, or whose promise rejects, before it
 *   answered the request it was given has the request answered ABORT
 *   INTERNAL and marked cancelled, so that work started on it stops.
 * - Promises returned by the request API never report unhandled
 *   rejections.
 */
export class TalkChannel implements TalkChannelContract {
  /**
   * Routes a bulk channel the peer opened to the Talk layer of its parent
   * channel, for applications that read a connection's incoming channels
   * themselves without {@link installBulkRouting} (which a Talk channel
   * installs on its connection, and a node on every connection): call it
   * for each channel whose open payload is of the reserved type `_bulk`.
   * A malformed open payload closes it PROTOCOL_ERROR.
   *
   * @throws {TypeError} if [channel] is not a bulk channel.
   */
  static adoptBulk(channel: MuxChannel): void {
    let address: ChannelAddress;
    try {
      address = ChannelAddress.decode(channel.openPayload);
    } catch (e) {
      if (e instanceof SwitchboardError) {
        void channel.close(e.status).catch(noop);
        return;
      }
      throw e;
    }
    if (!TalkBulkOpen.isBulk(address)) {
      throw new TypeError(`channel ${channel.id} is not a _bulk channel`);
    }
    routeBulk(channel, address);
  }

  /** The policy of this channel. */
  readonly options: TalkOptions;

  /** @internal Time and timers: `options.clock`, else the connection's, else the system clock. */
  readonly _clock: Clock;

  readonly #logger: Logger;
  readonly #raw: TalkRawChannel;

  /** Writes to a channel that is not a mux channel. */
  readonly #writer: WritableStreamDefaultWriter<Uint8Array> | undefined;

  /** The last write to {@link #writer}, never rejecting: a status close of the raw channel goes after it. */
  #lastWrite: Promise<void> | undefined;
  #reader: ReadableStreamDefaultReader<Uint8Array> | undefined;

  /** Reports frames consumed to a flow-controlled raw channel; undefined when it is not one. */
  readonly #consume: ((bytes: number, frames: number) => void) | undefined;

  // Bulk payloads: the mux channel the bulk channels of the peer are routed
  // for; bulk numbers this side used; transfers in both directions.
  #registeredMux: MuxChannel | undefined;
  #bulkOutCount = 0;

  /** @internal Bulk payloads this side is sending. */
  readonly _bulkOuts = new Set<BulkOut>();
  readonly #bulkUnclaimed = new Map<number, BulkIn>();

  /** Bulk channels beyond `maxUnclaimedBulk` wait for the frames received before their OPENs to be read ({@link _adoptBulk}). */
  #unclaimedRecount = false;

  /** The reassemblies in progress, by bulk payload: a graceful end waits for those that arrived whole ({@link #peerEnded}). */
  readonly #collecting = new Map<BulkIn, Promise<void>>();

  /** @internal Messages whose bulk channel was not adopted yet, by bulk number. */
  readonly _bulkAwaited = new Map<number, Awaited>();

  /** @internal Bulk payloads being received, until read whole or abandoned. */
  readonly _bulkIns = new Set<BulkIn>();
  #bulkInHighest = 0;

  // References travel on one ordered parent stream even when bulk opens
  // travel on independent streams. This watermark retires numbers without
  // keeping a lifetime-sized set of completed transfers.
  #bulkReferenceHighest = 0;

  /**
   * @internal Set while the channel, closed gracefully by the peer, waits
   * for the bulk payloads of messages that arrived before the close
   * ({@link _linger}); settled when they all arrived or failed, or made no
   * progress for too long.
   */
  _lingering: Deferred<void> | undefined;

  /** @internal While {@link _lingering}: when a bulk payload last progressed, in clock time. */
  _lingerProgress = 0;

  /** @internal The reassembly budget in use: what the payloads admitted for a listener are charged, until taken. */
  _assemblyBytes = 0;

  /** The bulk payloads waiting for the budget to admit them, in the order their messages arrived. */
  #admission: IncomingMessage[] = [];

  /** @internal How many payloads waiting for admission have their bulk channel. */
  _waitingCount = 0;

  /** @internal What those bulk channels may hold together. */
  _waitingBytes = 0;

  #admitting = false;
  #admitAgain = false;

  /** The order of {@link messages}, kept while bulk payloads arrive. */
  readonly #messagesLane = new Lane();

  /** The credit of the frames waiting in {@link messages}. */
  readonly #messagesHeld = new Held();
  readonly #messages: Delivery;

  /** Markers carrying the outcome of an `ordered` request, added to {@link messages} and not yet handed over. */
  #orderedOutcomes: IncomingMessage[] = [];

  /** @internal Our requests that still hold a request id. */
  readonly _outgoing = new Map<number, Outgoing>();
  readonly #incoming = new Map<number, IncomingMessage>();
  #nextRequestId = 1;

  #messagesCancelled = false;
  #closing = false;
  #terminated = false;
  #rawEnded = false;
  #rawClosed = false;
  #finishing = false;
  #endStatus: Status | undefined;
  #closePromise: Promise<void> | undefined;
  #peerEndedPromise: Promise<void> | undefined;
  readonly #done = new Deferred<Status>();

  /** Frames waiting for the peer's LIMITS (see {@link _sendFrame}). */
  #deferred: Deferral[] = [];

  /** The mux channel whose LIMITS the frames in {@link #deferred} wait for. */
  #limitsAwaited: MuxChannel | undefined;

  /** The bulk target this channel registers for its mux channels. */
  readonly #bulkTarget: BulkTarget = (bulk, number) => this._adoptBulk(bulk, number);

  /**
   * Wraps [raw] and starts reading it at once, never pausing: what waits
   * for the application is held here (see {@link messages}). Over a mux
   * channel (or a channel carrying one) the channel is put in manual
   * credit, and the credit of every frame goes back to the peer when the
   * application has taken what the frame carried, so that the channel's
   * window bounds what is held here. Over any other channel nothing is flow
   * controlled. Installs bulk routing on the mux channel's connection
   * ({@link installBulkRouting}).
   */
  constructor(raw: TalkRawChannel, options?: Partial<TalkOptions>) {
    this.options = talkOptions(options);
    this.#raw = raw;
    if (isMuxChannel(raw)) {
      raw.manualCredit = true;
      this.#consume = (bytes, frames) => raw.consumed(bytes, frames);
      this.#writer = undefined;
    } else if (isCarrier(raw)) {
      raw.manualCredit = true;
      this.#consume = (bytes, frames) => raw.consumed(bytes, frames);
      this.#writer = undefined;
    } else {
      this.#consume = undefined;
      this.#writer = raw.writable.getWriter();
    }
    const mux = this.#mux;
    const connectionOptions = mux?.connection.options;
    this._clock = this.options.clock ?? connectionOptions?.clock ?? systemClock;
    this.#logger = this.options.logger ?? connectionOptions?.logger ?? noopLogger;
    this.#messages = new Delivery(this, this.#messagesHeld, (dropped) =>
      this.#onMessagesCancelled(dropped),
    );
    if (mux !== undefined) {
      installBulkRouting(mux.connection);
    }
    // Before reading: a bulk channel the peer opened for this one may be
    // waiting for its Talk layer.
    this.#routeBulk();
    void this.#read();
  }

  /** @internal Logs under `Switchboard.Talk`. */
  _log(level: LogLevel, message: string, error?: unknown): void {
    try {
      this.#logger.log(level, talkLoggerName, message, error);
    } catch {
      // A logger that throws must not break the channel.
    }
  }

  /** The mux channel underneath, when there is one (for a slot channel, the one it carries now). */
  get raw(): MuxChannel | undefined {
    return this.#mux;
  }

  /** The channel this Talk channel was created over. */
  get rawChannel(): TalkRawChannel {
    return this.#raw;
  }

  get #mux(): MuxChannel | undefined {
    const raw = this.#raw;
    if (isMuxChannel(raw)) {
      return raw;
    }
    if (isCarrier(raw)) {
      return raw.channel;
    }
    return undefined;
  }

  /**
   * Whether the bulk channels the peer opens arrive in order with this
   * channel's frames (`hasOrderedChannels`); see {@link _adoptBulk} for the
   * rules where they do not.
   */
  get #orderedBulk(): boolean {
    return this.#mux?.connection.hasOrderedChannels ?? true;
  }

  /** Takes the bulk channels the peer opens for the current mux channel (a slot channel may have replaced it before anything was sent). */
  #routeBulk(): void {
    const mux = this.#mux;
    if (mux === undefined || mux === this.#registeredMux) {
      return;
    }
    this.#registeredMux = mux;
    registerBulkTarget(mux, this.#bulkTarget);
  }

  /**
   * Incoming plain messages and requests, in arrival order; responses never
   * appear here. One consumer: a `for await` loop, or {@link serve}.
   *
   * Flow control: over a mux channel, the credit of a message goes back to
   * the peer when the message is taken, not when it arrives. So what this
   * channel holds is bounded by the channel's window: a consumer that does
   * not take messages (or is not there yet) stalls the peer on this
   * channel once the window is used up, and only this channel; everything
   * the peer sends on it then waits, responses to our own requests
   * included. Read for as long as the peer may send messages, or stop.
   *
   * A channel abort from the peer makes the iteration throw a
   * {@link TalkAbortError} carrying its status, after the messages that
   * arrived before it. The iteration also ends, without an error, when the
   * channel closes for any other reason; {@link done} has the status.
   *
   * Stopping the iteration (`break`) answers the requests not taken yet,
   * and every later request, with ABORT UNIMPLEMENTED, including requests
   * whose bulk payloads are still being reassembled; their transfers are
   * cancelled too.
   */
  get messages(): AsyncIterable<TalkMessage> {
    return this.#messages;
  }

  /**
   * Calls [handler] with every incoming plain message and request, as it
   * arrives, one per microtask: the handler is not awaited between
   * messages. Resolves once the channel ended and every message was
   * handed over (also on a channel abort, whose status is on {@link done}).
   * A handler that throws, or returns a promise that rejects, before it
   * answered the request it was given has the request answered ABORT
   * INTERNAL and marked cancelled; the error is logged.
   *
   * @throws {Error} if something consumes {@link messages} already.
   */
  serve(handler: (message: TalkMessage) => unknown): Promise<void> {
    return this.#messages.listen((message) => {
      let result: unknown;
      try {
        result = handler(message);
      } catch (e) {
        this._log('error', `message handler threw on ${message.procedureName}`, e);
        message._handlerFailed();
        return;
      }
      if (isThenable(result)) {
        Promise.resolve(result).then(noop, (e: unknown) => {
          this._log('error', `message handler failed on ${message.procedureName}`, e);
          message._handlerFailed();
        });
      }
    });
  }

  /** True until the channel starts closing, for any reason. */
  get isOpen(): boolean {
    return !this.#closing;
  }

  /**
   * Resolves once the channel is closed, with its end status: the first
   * non-OK Talk-level status sent or received (channel abort, protocol
   * error, the status given to {@link close} or {@link abort}); otherwise
   * the raw channel's end status if it has one; otherwise OK. Never
   * rejects.
   */
  get done(): Promise<Status> {
    return this.#done.promise;
  }

  /** Our requests that still hold a request id, including cancelled ones still waiting for the peer's final response. */
  get outgoingRequestCount(): number {
    return this._outgoing.size;
  }

  /** The peer's requests waiting for our final reply. */
  get incomingRequestCount(): number {
    return this.#incoming.size;
  }

  /**
   * Whether a message sent now would go to the connection at once: the
   * channel is open and its mux channel is writable (room in the peer's
   * window, nothing waiting for credit). Over a channel that is not a mux
   * channel, always true while open.
   */
  get isWritable(): boolean {
    if (this.#closing) {
      return false;
    }
    const raw = this.#raw;
    if (isMuxChannel(raw) || isCarrier(raw)) {
      return raw.isWritable;
    }
    return true;
  }

  /**
   * Resolves once the channel {@link isWritable}: at once when it is, else
   * when the peer's credit makes room. Rejects FAILED_PRECONDITION when the
   * channel can no longer send: at once if it is closing, else when its
   * mux channel stops. Never reports an unhandled rejection. The way to
   * pace what the request API sends: `await talk.whenWritable` before each.
   */
  get whenWritable(): Promise<void> {
    if (this.#closing) {
      return rejected(closedChannel());
    }
    const raw = this.#raw;
    if (isMuxChannel(raw) || isCarrier(raw)) {
      return raw.whenWritable;
    }
    return Promise.resolve();
  }

  /**
   * @internal The id the next request tries first; ids still in use are
   * skipped. A test hook.
   */
  get nextRequestId(): number {
    return this.#nextRequestId;
  }

  set nextRequestId(id: number) {
    if (!Number.isInteger(id) || id < 1 || id > TalkFrame.maxId) {
      throw new RangeError(`id ${id} is outside 1 to ${TalkFrame.maxId}`);
    }
    this.#nextRequestId = id;
  }

  // ---------------------------------------------------------------------
  // The request API

  /**
   * Sends a plain message. A message too large for the channel's frames is
   * sent as a bulk payload, transparently; with `bulk`, the payload is that
   * byte source and [payload] must be empty.
   *
   * Pacing: the promise resolves once the message was handed to the
   * connection within the channel's send window, at once when the window
   * takes it now; for a bulk payload, once that payload went whole as
   * well. It rejects FAILED_PRECONDITION when the channel can no longer
   * send the message, FRAME_TOO_LARGE when a limit the peer announced
   * after the call does not take it, and for a bulk payload as the
   * transfer fails. It never reports an unhandled rejection. Over a channel
   * that is not a mux channel it resolves at once.
   *
   * @throws {SwitchboardError} FAILED_PRECONDITION if the channel is
   * closed; {@link TypeError} for a payload with `bulk`, and
   * {@link RangeError} for an invalid name or `bulkLength`.
   */
  send(procedure: NameLike, payload?: PayloadLike, options?: SendOptions): Promise<void> {
    const name = options?.name ?? toName(procedure);
    const bytes = toPayload(payload);
    const bulk = this._bulkPayload(bytes, options?.bulk, options?.bulkLength);
    return this._sendFrame(
      new TalkFrame({ kind: TalkKind.message, procedure: name, payload: bytes }),
      { bulk },
    );
  }

  /**
   * Sends a request and returns the final response: the `response` of
   * {@link startRequest}, the same promise. Rejects with a
   * {@link TalkAbortError} carrying the peer's abort status, or a plain
   * {@link SwitchboardError}: DEADLINE_EXCEEDED if nothing arrives in time
   * (a cancel is then sent), CANCELLED when cancelled, or the channel's
   * failure. Throws synchronously like {@link startRequest}.
   */
  request(
    procedure: NameLike,
    payload?: PayloadLike,
    options?: RequestOptions,
  ): Promise<TalkMessage> {
    return this.startRequest(procedure, payload, options).response;
  }

  /**
   * Sends a request and returns its handle, through which the response
   * arrives and the request can be cancelled.
   *
   * `timeout` is the requester timeout, a gap restarted by every EXTEND
   * until the peer declares a deadline or a renewal; it defaults to
   * `requestTimeout`, and 0 disables it. `onExtend` is called synchronously
   * each time the peer sends EXTEND, with the deadline and the renewal as
   * received, before `minExtension` and `maxExtension` apply. `signal`
   * cancels the request when aborted.
   *
   * With `ordered`, the peer's answer (its final response or its abort) is
   * delivered in wire order with {@link messages}: `response` settles only
   * once every plain message and request that arrived before the answer
   * has been taken from {@link messages}, and code awaiting `response` runs
   * before the messages that arrived after it. Use it when the peer sends
   * one-way messages that the answer marks a point in. If nobody consumes
   * {@link messages} yet, or it stopped, the answer is delivered at once.
   * While the consumer does not take messages the answer waits, so the
   * body of a `for await` loop over {@link messages} must not wait for an
   * ordered request. Local failures are reported at once.
   *
   * Bulk payloads: a request too large for the channel's frames goes as a
   * bulk payload transparently; with `bulk`, [payload] must be empty and
   * the payload is that byte source, read in chunks of `bulkChunkSize` as
   * the bulk channel's window takes them. Cancelling the request, or its
   * failing locally, stops the transfer CANCELLED. Needs a mux channel:
   * over any other channel a bulk payload rejects UNIMPLEMENTED.
   *
   * Pacing: the frame is handed to the channel without waiting for the
   * peer's window; await {@link whenWritable} before each request to pace
   * a run of them.
   *
   * @throws {SwitchboardError} RESOURCE_EXHAUSTED if `maxOutgoingRequests`
   * is reached, FAILED_PRECONDITION if the channel is closed, CANCELLED if
   * `signal` is aborted already; {@link TypeError} for a payload with
   * `bulk`; {@link RangeError} for an invalid name, timeout or bulk length.
   */
  startRequest(procedure: NameLike, payload?: PayloadLike, options?: RequestOptions): TalkRequest {
    const name = options?.name ?? toName(procedure);
    const bytes = toPayload(payload);
    const bulk = this._bulkPayload(bytes, options?.bulk, options?.bulkLength);
    return new TalkRequestHandle(
      this._startRequest({
        stream: false,
        timeout: options?.timeout,
        onExtend: options?.onExtend,
        ordered: options?.ordered === true,
        bulk,
        signal: options?.signal,
        build: (id) =>
          new TalkFrame({ kind: TalkKind.message, procedure: name, requestId: id, payload: bytes }),
      }),
    );
  }

  /**
   * Sends a stream request: items and the final response arrive through
   * the returned {@link TalkStream}. Takes, throws and paces like
   * {@link startRequest} (`ordered` applies to single answers and is
   * ignored here); the requester timeout is also restarted by every item.
   *
   * `window` is the request's item window: how many items the responder
   * may send before this side grants more with {@link TalkStream.grant},
   * counted in items whatever their size; 0 for none until a grant;
   * undefined, the default, for unlimited. The final response, an abort and
   * EXTEND are never counted. While the window is exhausted the requester
   * timeout does not run: the responder bounds the wait for a grant with
   * its own `creditTimeout`.
   *
   * @throws like {@link startRequest}, and {@link RangeError} for a window
   * that is negative or not below {@link TalkFrame.unlimitedWindow}.
   */
  streamRequest(procedure: NameLike, payload?: PayloadLike, options?: RequestOptions): TalkStream {
    const name = options?.name ?? toName(procedure);
    const items = windowOf(options?.window);
    const bytes = toPayload(payload);
    const bulk = this._bulkPayload(bytes, options?.bulk, options?.bulkLength);
    return new TalkStreamHandle(
      this._startRequest({
        stream: true,
        window: items,
        timeout: options?.timeout,
        onExtend: options?.onExtend,
        bulk,
        signal: options?.signal,
        build: (id, window) =>
          new TalkFrame({
            kind: TalkKind.message,
            procedure: name,
            requestId: id,
            stream: true,
            window,
            payload: bytes,
          }),
      }),
    );
  }

  /**
   * Sends a channel abort carrying [status], then closes the channel with
   * it. Application codes are allowed in the abort; a mux channel is
   * closed UNKNOWN for them, since a mux CLOSE must not carry them.
   * CONNECTION_LOST goes as UNAVAILABLE. Does nothing if already closing.
   *
   * @throws {RangeError} if [status] is OK: use {@link close}.
   */
  abort(status: Status): void {
    checkAbortStatus(status);
    if (this.#closing) {
      return;
    }
    this._trySend(
      new TalkFrame({ kind: TalkKind.abort, payload: wireAbortStatus(status).encode() }),
    );
    void this.close(status);
  }

  /**
   * Closes the channel. Outstanding outgoing requests fail with [status],
   * or with CANCELLED if it is OK; outstanding incoming requests become
   * unanswerable and are marked cancelled. A mux channel is closed with
   * [status]; another channel with its own `close`, or by closing its
   * writable. With OK, the bulk payloads this side is sending finish first
   * (at most the connection's `closeConfirmTimeout`); otherwise they are
   * cancelled.
   *
   * If the raw channel had ended already, nothing is sent and its end
   * status applies instead of [status]. Resolves when {@link done} does;
   * calling it again returns the same promise.
   */
  close(status: Status = Status.ok): Promise<void> {
    this.#closePromise ??= this.#close(status);
    return this.#closePromise;
  }

  /** Closes the channel with OK. */
  [Symbol.asyncDispose](): Promise<void> {
    return this.close();
  }

  async #close(status: Status): Promise<void> {
    // What waited for the peer's LIMITS goes before the end, sized against
    // the limit assumed meanwhile; the mux holds it until the LIMITS.
    if (!this.#rawEnded) {
      this.#flushDeferred();
    }
    const peerEnded = this.#peerEndedPromise;
    if (peerEnded !== undefined) {
      // The raw stream ended first; its end status is what outstanding
      // requests fail with, once known.
      await peerEnded;
      await this.#finish();
      return;
    }
    if (!status.isOk) {
      this.#endStatus ??= status;
    }
    this.#terminate(
      new SwitchboardError(
        status.isOk ? Status.of(StatusCode.cancelled, 'channel closed') : status,
      ),
      { keepOutgoingBulk: status.isOk },
    );
    if (status.isOk && this._bulkOuts.size > 0) {
      await this.#awaitBulkOuts();
    }
    if (!this.#rawEnded && !this.#rawClosed) {
      this.#rawClosed = true;
      const raw = this.#raw;
      if (isStatusClosable(raw)) {
        try {
          // What is still queued in the writer goes before the close, as
          // what was added to a sink goes before its close.
          await this.#lastWrite;
          await raw.close(wireStatus(status));
        } catch (e) {
          this._log('debug', 'closing the raw channel failed', e);
        }
      } else {
        this.#writer?.close().catch(noop);
      }
    }
    // Not awaited: nothing depends on it.
    this.#reader?.cancel().catch(noop);
    await this.#finish();
  }

  /**
   * Waits for the bulk payloads still being sent to finish, as a mux CLOSE
   * waits behind the subframes before it: at most the connection's close
   * confirmation timeout, then they are cut DEADLINE_EXCEEDED.
   */
  async #awaitBulkOuts(): Promise<void> {
    const outs = [...this._bulkOuts];
    let timeout = 30_000;
    const mux = this.#mux;
    if (mux !== undefined && mux.connection.options.closeConfirmTimeout > 0) {
      timeout = mux.connection.options.closeConfirmTimeout;
    }
    let timer: TimerHandle | undefined;
    const expired = new Promise<boolean>((resolve) => {
      timer = this._clock.setTimeout(
        () => resolve(true),
        timeout > maxTimerDelay ? maxTimerDelay : timeout,
      );
    });
    const finished = Promise.all(outs.map((out) => out.done.then(noop, noop))).then(() => false);
    const timedOut = await Promise.race([finished, expired]);
    if (timer !== undefined) {
      this._clock.clearTimeout(timer);
    }
    if (timedOut) {
      for (const out of outs) {
        out.cancel(
          Status.of(
            StatusCode.deadlineExceeded,
            `bulk payload not sent within ${timeout} ms of the channel closing`,
          ),
        );
      }
    }
  }

  // ---------------------------------------------------------------------
  // Sending

  /**
   * @internal The source of an explicit bulk payload, checking the
   * arguments.
   *
   * @throws {TypeError} for a `bulkLength` without `bulk`, or a payload
   * that is not empty with it; {@link RangeError} for a length out of
   * range.
   */
  _bulkPayload(
    payload: Uint8Array,
    bulk: BulkSource | undefined,
    length: number | undefined,
  ): BulkPayload | undefined {
    if (bulk === undefined) {
      if (length !== undefined) {
        throw new TypeError('bulkLength without bulk');
      }
      return undefined;
    }
    if (payload.length !== 0) {
      throw new TypeError(
        `the payload must be empty with a bulk payload (${payload.length} bytes given)`,
      );
    }
    if (
      length !== undefined &&
      (!Number.isInteger(length) || length < 0 || length > TalkBulkReference.maxLength)
    ) {
      throw new RangeError(`bulkLength ${length} is outside 0 to ${TalkBulkReference.maxLength}`);
    }
    return BulkPayload.stream(bulk, length);
  }

  /**
   * @internal Sends [frame]: inline, or with its payload on a bulk channel
   * when `bulk` is given or the frame is too large inline (a MESSAGE or
   * STREAM_ITEM over the channel's limit).
   *
   * While the mux channel waits for the peer's LIMITS, the frame waits
   * here, in order with every frame after it, and is sized once the limit
   * the peer announces is known. Its failures are then reported to
   * `onFailure` (logged without one) instead of thrown, and so is the end
   * of the channel before the frame went out.
   *
   * With `onAccepted` (forwarding), it is called once the frame (for a bulk
   * payload: its BULK frame) was handed to the connection, or discarded:
   * exactly once whenever this returns normally, never when it throws.
   *
   * Returns a promise that resolves once the frame was handed to the
   * connection, and for a bulk payload once that payload went whole too.
   * Safe to drop: a failure is logged.
   *
   * @throws {SwitchboardError} FAILED_PRECONDITION if the channel is
   * closing, and what sending at once throws.
   */
  _sendFrame(frame: TalkFrame, options: SendFrameOptions = {}): Promise<void> {
    if (this.#closing) {
      throw closedChannel();
    }
    const { bulk, onBulk, onSent, onFailure } = options;
    if (this.#mustDefer()) {
      const accepted = options.onAccepted === undefined ? undefined : once(options.onAccepted);
      const sent = new Deferred<void>();
      const dropped = (error: SwitchboardError): void => {
        accepted?.();
        onFailure?.(error);
        sent.reject(error);
      };
      this.#deferred.push({
        send: () => {
          let result: { sent: Promise<void>; out: BulkOut | undefined };
          try {
            result = this.#sendNow(frame, bulk, accepted);
          } catch (e) {
            const error = asSwitchboardError(e);
            if (onFailure === undefined) {
              this._log('warn', `${frame} not sent: ${error.status}`);
            }
            dropped(error);
            return;
          }
          const out = result.out;
          if (out !== undefined) {
            onBulk?.(out);
          }
          onSent?.(out);
          sent.resolve(result.sent);
        },
        drop: dropped,
      });
      return this._quiet(sent.promise, frame);
    }
    const result = this.#sendNow(frame, bulk, options.onAccepted);
    const out = result.out;
    if (out !== undefined) {
      onBulk?.(out);
    }
    onSent?.(out);
    return this._quiet(result.sent, frame);
  }

  /** @internal [promise], safe to drop: a failure nobody waits for is logged rather than reported as unhandled, and still reaches whoever waits. */
  _quiet<T>(promise: Promise<T>, frame: TalkFrame): Promise<T> {
    promise.catch((e: unknown) =>
      this._log('debug', `${talkKindName(frame.kind)} frame not sent`, e),
    );
    return promise;
  }

  /** @internal {@link _sendFrame} for frames that never carry a bulk payload. */
  _sendChecked(frame: TalkFrame, onAccepted?: () => void): Promise<void> {
    return this._sendFrame(frame, { onAccepted });
  }

  /** Whether a frame sent now waits in {@link #deferred}: frames wait there already, or the mux channel waits for the peer's LIMITS. */
  #mustDefer(): boolean {
    if (this.#deferred.length > 0) {
      return true;
    }
    const mux = this.#mux;
    if (mux === undefined || !mux.awaitingLimits) {
      return false;
    }
    if (this.#limitsAwaited !== mux) {
      this.#limitsAwaited = mux;
      void mux.limitsKnown.then(() => {
        if (this.#limitsAwaited === mux) {
          this.#limitsAwaited = undefined;
        }
        // Once the channel ended, they are dropped with it (#peerEnded).
        if (this.#mux === mux && !this.#rawEnded) {
          this.#flushDeferred();
        }
      }, noop);
    }
    return true;
  }

  /** Sends the frames that waited for the peer's LIMITS, in order. */
  #flushDeferred(): void {
    while (this.#deferred.length > 0) {
      (this.#deferred.shift() as Deferral).send();
    }
  }

  /** Drops the frames that waited for the peer's LIMITS, the channel having ended, telling their senders. */
  #dropDeferred(error: SwitchboardError): void {
    const dropped = this.#deferred;
    this.#deferred = [];
    for (const deferral of dropped) {
      deferral.drop?.(error);
    }
  }

  /** {@link _sendFrame} without the wait for LIMITS; throws its failures, and returns the promise of the frame (and its payload, if bulk) going out. */
  #sendNow(
    frame: TalkFrame,
    bulk: BulkPayload | undefined,
    onAccepted?: () => void,
  ): { sent: Promise<void>; out: BulkOut | undefined } {
    let source = bulk;
    if (source === undefined) {
      const bytes = frame.encode();
      if (!this.#needsBulk(frame, bytes.length)) {
        return { sent: this.#sendRaw(bytes, onAccepted), out: undefined };
      }
      source = BulkPayload.fromBytes(frame.payload);
    }
    const mux = this.#mux;
    if (mux === undefined) {
      throw SwitchboardError.of(StatusCode.unimplemented, 'bulk payloads need a mux channel');
    }
    const out = this.#openBulk(mux, source.length);
    let sent: Promise<void>;
    try {
      sent = this.#sendRaw(
        frame.withBulk(new TalkBulkReference(out.number, source.length)).encode(),
        onAccepted,
      );
    } catch (e) {
      void out.channel.close(Status.of(StatusCode.cancelled, 'bulk message not sent')).catch(noop);
      throw e;
    }
    out.start(source);
    // The message went once its frame and its payload did.
    return { sent: sent.then(() => out.done), out };
  }

  /**
   * Hands [bytes] to the raw channel: through the awaitable send of a mux
   * channel (or of the channel carrying one), whose promise this returns,
   * else its writable (resolved at once). [onAccepted] is called once the
   * send window took them, or they were discarded: a frame for a channel
   * that can no longer send is dropped, [onAccepted] called at once and the
   * promise rejecting FAILED_PRECONDITION.
   */
  #sendRaw(bytes: Uint8Array, onAccepted?: () => void): Promise<void> {
    const raw = this.#raw;
    let sent: Promise<void>;
    try {
      if (isMuxChannel(raw) || isCarrier(raw)) {
        sent = raw.send(bytes);
      } else {
        this.#lastWrite = (this.#writer as WritableStreamDefaultWriter<Uint8Array>)
          .write(bytes)
          .catch((e: unknown) => this._log('debug', 'frame not written', e));
        onAccepted?.();
        return Promise.resolve();
      }
    } catch (e) {
      if (e instanceof SwitchboardError) {
        if (e.status.code === StatusCode.failedPrecondition) {
          onAccepted?.();
          return rejected(e);
        }
        throw e;
      }
      throw SwitchboardError.of(StatusCode.failedPrecondition, `channel closed: ${e}`, {
        cause: e,
      });
    }
    if (onAccepted !== undefined) {
      sent.then(onAccepted, onAccepted);
    }
    return sent;
  }

  /** Whether [frame], [length] bytes encoded, goes as a bulk payload. */
  #needsBulk(frame: TalkFrame, length: number): boolean {
    if (frame.kind !== TalkKind.message && frame.kind !== TalkKind.streamItem) {
      return false;
    }
    const mux = this.#mux;
    if (mux === undefined) {
      return false;
    }
    let limit = mux.maxSubframeLength;
    const threshold = this.options.bulkThreshold;
    if (threshold !== undefined && threshold < limit) {
      limit = threshold;
    }
    return length > limit;
  }

  /** Opens the next bulk channel for [mux], in order with its frames. */
  #openBulk(mux: MuxChannel, length: number | undefined): BulkOut {
    if (this.#bulkOutCount >= TalkBulkReference.maxNumber) {
      throw SwitchboardError.of(
        StatusCode.resourceExhausted,
        `bulk numbers of channel ${mux.id} used up`,
      );
    }
    const number = this.#bulkOutCount + 1;
    const channel = mux.openAfter(new TalkBulkOpen(mux.id, number).encode());
    this.#bulkOutCount = number;
    channel.priority = 'bulk';
    markBulkChannel(channel);
    return new BulkOut(this, channel, number, length);
  }

  /** @internal Sends a frame on the channel's own behalf; failures are logged only. */
  _trySend(frame: TalkFrame): void {
    if (this.#closing) {
      return;
    }
    const send = (): void => {
      try {
        this.#sendRaw(frame.encode()).catch((e: unknown) =>
          this._log('debug', `failed to send ${frame}`, e),
        );
      } catch (e) {
        this._log('debug', `failed to send ${frame}`, e);
      }
    };
    if (this.#mustDefer()) {
      this.#deferred.push({ send });
    } else {
      send();
    }
  }

  /** @internal Answers the peer's request [requestId] with an abort, without any local state. */
  _rejectRequest(requestId: number, status: Status): void {
    this._trySend(
      new TalkFrame({
        kind: TalkKind.abort,
        responseId: requestId,
        payload: wireAbortStatus(status).encode(),
      }),
    );
  }

  /**
   * @internal Registers one of our requests and sends its frame, built from
   * the request id and the item window to put on the wire. With `deferred`
   * the request is registered and returned, but nothing is sent until
   * `Outgoing.send` is called (an item request or a chained final waiting
   * for the item window of the request it answers).
   */
  _startRequest(init: StartRequestInit): Outgoing {
    if (this.#closing) {
      throw closedChannel();
    }
    const timeout = init.timeout ?? this.options.requestTimeout;
    if (!(timeout >= 0)) {
      throw new RangeError(`timeout ${init.timeout} is negative`);
    }
    const max = this.options.maxOutgoingRequests;
    if ((max > 0 && this._outgoing.size >= max) || this._outgoing.size >= TalkFrame.maxId) {
      throw SwitchboardError.of(
        StatusCode.resourceExhausted,
        `too many outstanding requests (${this._outgoing.size})`,
      );
    }
    const signal = init.signal;
    if (signal?.aborted === true) {
      throw SwitchboardError.of(StatusCode.cancelled, 'aborted', { cause: signal.reason });
    }
    let id = this.#nextRequestId;
    while (this._outgoing.has(id)) {
      id = id >= TalkFrame.maxId ? 1 : id + 1;
    }
    const window = init.window ?? TalkFrame.unlimitedWindow;
    const pending = new Outgoing(this, id, timeout, {
      stream: init.stream,
      window: init.stream && window !== TalkFrame.unlimitedWindow ? window : undefined,
      onExtend: init.onExtend,
      sink: init.sink,
      ordered: init.ordered === true,
    });
    // Registered before sending: the response can arrive while the request
    // is still being sent.
    this._outgoing.set(id, pending);
    this.#nextRequestId = id >= TalkFrame.maxId ? 1 : id + 1;
    pending.send = () => {
      pending.queued = false;
      pending.startTimer();
      try {
        // The window granted while the request waited goes with it.
        this._sendFrame(init.build(id, pending.window ?? TalkFrame.unlimitedWindow), {
          bulk: init.bulk,
          onBulk: (out) => {
            const stopped = pending.bulkStopped;
            if (stopped !== undefined) {
              // The frame waited for the peer's LIMITS, or for the window
              // of the request it answers, and the request was cancelled
              // or failed locally meanwhile.
              out.cancel(stopped);
              return;
            }
            out.track(pending.bulkOuts);
            // The request's payload going out is progress: the requester
            // timeout restarts with each chunk the receiver takes.
            out.onProgress = () => {
              if (!pending.ended && !pending.finalReceived) {
                pending.replied();
              }
            };
          },
          // Only when the frame waited for the peer's LIMITS.
          onFailure: (error) => {
            if (this._outgoing.get(id) === pending) {
              this._outgoing.delete(id);
            }
            pending.fail(error);
          },
          onAccepted: init.onAccepted,
        });
      } catch (e) {
        if (this._outgoing.get(id) === pending) {
          this._outgoing.delete(id);
        }
        pending.stopTimer();
        throw e;
      }
      const cancelled = pending.cancelWhenSent;
      if (cancelled !== undefined) {
        // Cancelled while it waited: the peer is told now that it knows of
        // the request.
        pending.cancelWhenSent = undefined;
        this.#sendCancel(pending, cancelled);
      }
    };
    if (init.deferred === true) {
      pending.queued = true;
    } else {
      pending.send();
    }
    if (signal !== undefined) {
      const onAbort = (): void =>
        this._cancelOutgoing(pending, Status.of(StatusCode.cancelled, 'aborted'));
      signal.addEventListener('abort', onAbort, { once: true });
      const remove = (): void => signal.removeEventListener('abort', onAbort);
      pending.completer.promise.then(remove, remove);
    }
    return pending;
  }

  // ---------------------------------------------------------------------
  // Requester side

  /** @internal The requester timeout of [pending] expired. */
  _onRequestTimeout(pending: Outgoing): void {
    pending.timer = undefined;
    if (this._outgoing.get(pending.id) !== pending) {
      return;
    }
    this._outgoing.delete(pending.id);
    if (pending.abandoned) {
      this._log('debug', `released cancelled request ${pending.id} on timeout`);
      return;
    }
    const reason = pending.expiry.declared
      ? 'no response by the deadline the peer declared'
      : `no response within ${pending.timeout} ms`;
    this._log('debug', `request ${pending.id} timed out: ${reason}`);
    pending.fail(SwitchboardError.of(StatusCode.deadlineExceeded, reason));
    this._trySend(
      new TalkFrame({
        kind: TalkKind.abort,
        requestId: pending.id,
        payload: Status.of(StatusCode.cancelled, 'deadline exceeded').encode(),
      }),
    );
  }

  /** @internal Cancels [pending] with [status]: it fails at once, and the peer is told. */
  _cancelOutgoing(pending: Outgoing, status: Status): void {
    if (this._outgoing.get(pending.id) !== pending || pending.abandoned) {
      return;
    }
    if (pending.finalReceived) {
      // The answer arrived and waits for its bulk payload: the peer is done
      // with the request, the payload's channel is closed.
      this._outgoing.delete(pending.id);
      pending.fail(new SwitchboardError(status));
      return;
    }
    pending.abandoned = true;
    if (pending.queued) {
      // Not sent yet: it waits for the item window of the request it
      // answers, and keeps its place there (a chained final is still the
      // answer to that request). The cancel follows it once it went.
      pending.cancelWhenSent = status;
      pending.fail(new SwitchboardError(status));
      return;
    }
    pending.fail(new SwitchboardError(status));
    this.#sendCancel(pending, status);
  }

  /** @internal [pending], registered but not sent, will not be sent: it fails with [error] and its id is released. */
  _dropOutgoing(pending: Outgoing, error: SwitchboardError): void {
    if (this._outgoing.get(pending.id) === pending) {
      this._outgoing.delete(pending.id);
    }
    pending.cancelWhenSent = undefined;
    pending.fail(error);
  }

  /** Sends the cancel of [pending], abandoned, and keeps its id until the peer's final response arrives, or on timeout. */
  #sendCancel(pending: Outgoing, status: Status): void {
    this._trySend(
      new TalkFrame({
        kind: TalkKind.abort,
        requestId: pending.id,
        payload: wireAbortStatus(status).encode(),
      }),
    );
    if (this._outgoing.get(pending.id) !== pending) {
      return;
    }
    // Keep the id until the peer's final response arrives, or on timeout. A
    // request without a timeout of its own falls back to the channel
    // default here, so a peer that never answers the cancel cannot hold the
    // id for ever.
    pending.armRelease(pending.timeout > 0 ? pending.timeout : this.options.requestTimeout);
  }

  // ---------------------------------------------------------------------
  // Responder side

  /**
   * Starts tracking the peer's request carried by [message]. Returns false
   * if it was refused (over `maxIncomingRequests`): the abort was sent and
   * [message] is marked answered.
   */
  #register(message: IncomingMessage): boolean {
    const id = message.requestId;
    const existing = this.#incoming.get(id);
    if (existing !== undefined) {
      // The peer may reuse an id after its own timeout released it.
      this._log(
        'warn',
        `peer reused request id ${id} while it was outstanding; abandoning the earlier request`,
      );
      this.#incoming.delete(id);
      existing._abandon();
    }
    const max = this.options.maxIncomingRequests;
    if (max > 0 && this.#incoming.size >= max) {
      this._log('warn', `incoming request limit ${max} reached, rejecting ${id}`);
      this._rejectRequest(
        id,
        Status.of(StatusCode.resourceExhausted, 'too many outstanding requests'),
      );
      message._finished = true;
      return false;
    }
    this.#incoming.set(id, message);
    if (message.frame.bulk) {
      // Started once the request is delivered: its application cannot
      // answer before.
      message._timerDeferred = true;
    } else {
      message._startTimer();
      message._armCredit();
    }
    return true;
  }

  /** @internal [message] was answered, or can no longer be: its id is free. */
  _release(message: IncomingMessage): void {
    if (this.#incoming.get(message.requestId) === message) {
      this.#incoming.delete(message.requestId);
    }
  }

  #deliverMessage(message: IncomingMessage): void {
    if (this.#messagesCancelled || !this.#messages.add(message)) {
      this._log('debug', `no message consumer, dropping ${message.frame}`);
      this._creditMessage(message);
      message._bulk?.abandon();
      message._abortQuietly(Status.of(StatusCode.unimplemented, 'no message listener'));
      return;
    }
    this.#messagesHeld.hold(message);
  }

  /** The consumer of {@link messages} stopped, or the end was delivered: nothing is delivered any more. */
  #onMessagesCancelled(dropped: IncomingMessage[]): void {
    this.#messagesCancelled = true;
    // What the delivery still held is dropped.
    this._drainHeld(this.#messagesHeld);
    const unimplemented = Status.of(StatusCode.unimplemented, 'no message listener');
    for (const message of dropped) {
      if (message._outcome === undefined && message.expectsReply) {
        message._abortQuietly(unimplemented);
      }
    }
    this.#messagesLane.dropIncoming(unimplemented);
    // Nothing is delivered any more: the waiting answers go at once.
    const outcomes = this.#orderedOutcomes;
    this.#orderedOutcomes = [];
    for (const marker of outcomes) {
      marker._runOutcome();
    }
  }

  /**
   * Delivers the answer to an `ordered` request: [outcome] runs once
   * [marker] reaches the consumer of {@link messages}, after the messages
   * that arrived before it; the consumer never sees [marker]. At once when
   * nobody consumes it, or nobody will.
   */
  #deliverOrdered(marker: IncomingMessage, outcome: () => void): void {
    if (this.#messagesCancelled || !this.#messages.hasListener || this.#messages.isClosed) {
      this._creditMessage(marker);
      outcome();
      return;
    }
    marker._outcome = outcome;
    this.#orderedOutcomes.push(marker);
    // The answer goes to the request, not to the consumer: its payload is
    // not the consumer's to drop.
    this.#messagesHeld.hold(marker, false);
    this.#messages.add(marker);
  }

  /** @internal [marker], the answer of an `ordered` request, reached the consumer of {@link messages}: the answer is delivered. */
  _outcomeReached(marker: IncomingMessage): void {
    const outcomes = this.#orderedOutcomes;
    const index = outcomes.indexOf(marker);
    if (index >= 0) {
      outcomes.splice(index, 1);
    }
    marker._runOutcome();
  }

  // ---------------------------------------------------------------------
  // Receiving

  async #read(): Promise<void> {
    let reader: ReadableStreamDefaultReader<Uint8Array>;
    try {
      reader = this.#raw.readable.getReader();
    } catch (e) {
      this.#onError(e);
      this.#onDone();
      return;
    }
    this.#reader = reader;
    try {
      for (;;) {
        const { value, done } = await reader.read();
        if (done) {
          break;
        }
        this.#onData(value);
        if (this.#unclaimedRecount) {
          this.#recountUnclaimed();
        }
      }
    } catch (e) {
      this.#onError(e);
    }
    this.#onDone();
  }

  #onData(data: Uint8Array): void {
    const wire = data.length;
    if (this.#closing) {
      this._credit(wire);
      return;
    }
    this.#routeBulk();
    let frame: TalkFrame;
    try {
      frame = TalkFrame.decode(data);
    } catch (e) {
      this._credit(wire);
      if (e instanceof SwitchboardError) {
        this.#protocolError(e.status.reason);
        return;
      }
      throw e;
    }
    switch (frame.kind) {
      case TalkKind.message:
        this.#onMessage(frame, wire);
        break;
      case TalkKind.streamItem:
        this.#onStreamItem(frame, wire);
        break;
      case TalkKind.abort:
        this._credit(wire);
        this.#onAbort(frame);
        break;
      case TalkKind.extend:
        this.#onExtend(frame, wire);
        break;
    }
  }

  // ---------------------------------------------------------------------
  // Flow control

  /**
   * @internal Reports [frames] frames of [bytes] bytes together consumed
   * to the raw channel, so that their credit goes back to the peer. Does
   * nothing over a raw channel that is not flow controlled.
   */
  _credit(bytes: number, frames = 1): void {
    const consume = this.#consume;
    if (consume === undefined || frames === 0) {
      return;
    }
    try {
      consume(bytes, frames);
    } catch (e) {
      // Accounting mismatch: logged, never fatal to the channel.
      this._log('warn', `credit accounting: ${e}`);
    }
  }

  /** @internal Returns the credit of [message], once: it was handed to the application, or consumed by the channel. */
  _creditMessage(message: IncomingMessage, held?: Held): void {
    const bytes = this.#uncount(message, held);
    if (bytes !== 0) {
      this._credit(bytes);
    }
  }

  /**
   * Takes [message] out of what [held] (if any) and the reassembly budget
   * count, and returns the credit of its frame still to be returned, once
   * (0 after, and when [held] returned it already).
   */
  #uncount(message: IncomingMessage, held?: Held): number {
    message._releaseAssembly();
    held?.assembled.delete(message);
    if (held !== undefined && message._heldBulk) {
      message._heldBulk = false;
      const index = held.bulk.indexOf(message);
      if (index >= 0) {
        held.bulk.splice(index, 1);
      }
    }
    const bytes = message._creditBytes;
    if (bytes === 0) {
      return 0;
    }
    message._creditBytes = 0;
    if (held !== undefined) {
      if (held.drained) {
        // Returned already with everything the delivery held.
        return 0;
      }
      held.bytes -= bytes;
      held.frames--;
    }
    return bytes;
  }

  /**
   * @internal Hands [message] over with [deliver], returning its credit
   * after, unless `forwardMessage` took it over meanwhile
   * (`IncomingMessage._takeCredit`): then the credit goes back once what it
   * forwards went out, so that a forwarded frame is paced by the window it
   * is forwarded into, not queued here.
   */
  _handOver(message: IncomingMessage, held: Held | undefined, deliver: () => void): void {
    message._deliveryCredit = this.#uncount(message, held);
    try {
      deliver();
    } finally {
      const bytes = message._deliveryCredit;
      message._deliveryCredit = 0;
      if (bytes !== 0) {
        this._credit(bytes);
      }
    }
  }

  /** @internal Returns the credit of everything [held] still holds: the delivery it counts for will hand out nothing more. */
  _drainHeld(held: Held): void {
    this._returnHeld(held);
    // Bulk payloads of the dropped messages will not be read.
    const bulk = held.bulk.splice(0);
    for (const message of bulk) {
      message._heldBulk = false;
      message._bulk?.abandon();
    }
  }

  /**
   * @internal Returns the credit of everything [held] still holds, which
   * stays deliverable: nothing more will be added to its delivery (the
   * stream request ended), so it is bounded without the window.
   */
  _returnHeld(held: Held): void {
    // So does their share of the reassembly budget: what an ended stream
    // holds is the application's, and stays so if it never reads it.
    for (const message of held.assembled) {
      message._releaseAssembly();
    }
    held.assembled.clear();
    if (held.drained) {
      return;
    }
    held.drained = true;
    const bytes = held.bytes;
    const frames = held.frames;
    held.bytes = 0;
    held.frames = 0;
    this._credit(bytes, frames);
  }

  // ---------------------------------------------------------------------
  // Bulk payloads, receiving

  /**
   * @internal Takes [bulk], a bulk channel the peer opened for this channel
   * with bulk [number].
   *
   * Where bulk channels are ordered with this channel's frames (the frame
   * mux), bulk numbers arrive in increasing order, each before the message
   * that references it: a number at or below one seen is a channel
   * protocol error. Where they are not (a stream each), they may arrive in
   * any order, after their reference or after this channel ended; only the
   * references arrive in order, and they are what "seen" means: a number a
   * reference waits for is matched; a number above every reference waits
   * for its own; a second channel with a number already waiting is a
   * channel protocol error; any other number at or below the highest
   * referenced is closed CANCELLED; once the channel ended, only a number a
   * message still waits for is admitted, any other closed
   * FAILED_PRECONDITION.
   */
  _adoptBulk(bulk: MuxChannel, number: number): void {
    // Consumed as the reader takes its bytes.
    bulk.manualCredit = true;
    bulk.priority = 'bulk';
    markBulkChannel(bulk);
    const b = new BulkIn(this, bulk, number);
    if (this.#closing) {
      if (this._lingering !== undefined && this.#claimAwaited(number, b)) {
        return;
      }
      b.close(
        this._lingering !== undefined
          ? Status.of(StatusCode.failedPrecondition, 'the channel ended')
          : Status.of(StatusCode.cancelled, 'channel closed'),
      );
      b.abandon();
      return;
    }
    if (this.#orderedBulk) {
      if (number <= this.#bulkInHighest) {
        this.#refuseReused(b, number);
        return;
      }
      this.#bulkInHighest = number;
    } else if (this.#bulkUnclaimed.has(number)) {
      this.#refuseReused(b, number);
      return;
    } else if (number <= this.#bulkReferenceHighest && !this._bulkAwaited.has(number)) {
      b.close(Status.of(StatusCode.cancelled, 'bulk number not referenced'));
      b.abandon();
      return;
    }
    if (this.#claimAwaited(number, b)) {
      return;
    }
    // The frames of this channel received before the OPEN may not be read
    // yet (this side reads them asynchronously, where the Dart mux delivers
    // them as they arrive): the references among them claim bulk channels
    // ahead of this one. Beyond the bound, the count waits for them.
    const max = this.options.maxUnclaimedBulk;
    if (this.#bulkUnclaimed.size >= max + this.#unreadReferences()) {
      this.#refuseUnclaimed(b, number);
      return;
    }
    if (this.#bulkUnclaimed.size >= max) {
      this.#unclaimedRecount = true;
    }
    this.#bulkUnclaimed.set(number, b);
    // A bulk channel no message references in time is not waited for.
    b.unclaimedTimer = this._clock.setTimeout(() => {
      b.unclaimedTimer = undefined;
      if (this.#bulkUnclaimed.get(number) === b) {
        this.#bulkUnclaimed.delete(number);
        this._log('debug', `bulk channel ${number} not referenced in time`);
        b.close(Status.of(StatusCode.cancelled, 'not referenced in time'));
        b.abandon();
      }
    }, this.options.bulkOpenTimeout);
  }

  /** A bulk channel beyond `maxUnclaimedBulk`: closed RESOURCE_EXHAUSTED. */
  #refuseUnclaimed(b: BulkIn, number: number): void {
    this._log(
      'warn',
      `peer opened more than ${this.options.maxUnclaimedBulk} bulk channels ahead of their ` +
        `messages; refusing bulk channel ${number}`,
    );
    b.close(
      Status.of(StatusCode.resourceExhausted, 'too many bulk channels before their messages'),
    );
    b.abandon();
  }

  /**
   * At most how many BULK references the frames this channel received and
   * did not read yet can carry: each costs at least what a message frame
   * of a bare reference does.
   */
  #unreadReferences(): number {
    const mux = this.#mux;
    if (mux === undefined) {
      return 0;
    }
    const buffered = mux.bufferedBytes;
    return buffered > 0
      ? Math.floor(buffered / mux.sendCost(TalkBulkReference.encodedLength + 1))
      : 0;
  }

  /**
   * Refuses the bulk channels still unclaimed beyond `maxUnclaimedBulk`
   * (the highest numbers: the last opened) once the frames received before
   * their OPENs were read ({@link _adoptBulk}).
   */
  #recountUnclaimed(): void {
    const unread = this.#unreadReferences();
    const max = this.options.maxUnclaimedBulk + unread;
    if (this.#bulkUnclaimed.size > max) {
      const numbers = [...this.#bulkUnclaimed.keys()].sort((x, y) => x - y);
      for (const number of numbers.slice(max)) {
        const b = this.#bulkUnclaimed.get(number) as BulkIn;
        this.#bulkUnclaimed.delete(number);
        if (b.unclaimedTimer !== undefined) {
          this._clock.clearTimeout(b.unclaimedTimer);
          b.unclaimedTimer = undefined;
        }
        this.#refuseUnclaimed(b, number);
      }
    }
    if (unread === 0) {
      this.#unclaimedRecount = false;
    }
  }

  /** A bulk channel with a number that cannot be used again: a channel protocol error. */
  #refuseReused(b: BulkIn, number: number): void {
    b.close(Status.of(StatusCode.protocolError, 'bulk number reused'));
    b.abandon();
    this.#protocolError(`bulk number ${number} reused`);
  }

  /** Hands [b] to the message waiting for bulk [number], if its reference arrived first: attached, or abandoned if the message was dropped. */
  #claimAwaited(number: number, b: BulkIn): boolean {
    const awaited = this._bulkAwaited.get(number);
    if (awaited === undefined) {
      return false;
    }
    this._bulkAwaited.delete(number);
    this._clock.clearTimeout(awaited.timer);
    const message = awaited.message;
    if (message === undefined) {
      b.abandon();
    } else {
      this.#attachBulk(message, b);
    }
    this._checkLinger();
    return true;
  }

  /**
   * Claims the bulk channel [message] references, if it is a bulk message:
   * attaches it, or waits for it to be adopted (its OPEN came first, but
   * dispatch may lag). False, after a channel protocol error, for a
   * reference to a bulk number not open or already referenced.
   */
  #claimBulk(message: IncomingMessage, forSink: boolean): boolean {
    const frame = message.frame;
    if (!frame.bulk) {
      return true;
    }
    const reference = frame.bulkReference;
    message._bulkRef = reference;
    message._forSink = forSink;
    if (this.#mux === undefined) {
      message._bulkFailure = Status.of(
        StatusCode.unimplemented,
        'bulk payloads need a mux channel',
      );
      return true;
    }
    const number = reference.number;
    if (!this.#acceptBulkReference(number)) {
      return false;
    }
    const b = this.#bulkUnclaimed.get(number);
    if (b !== undefined) {
      this.#bulkUnclaimed.delete(number);
      this.#queueForBudget(message);
      this.#attachBulk(message, b);
      return true;
    }
    if (number <= this.#bulkInHighest || this._bulkAwaited.has(number)) {
      this.#protocolError(
        `BULK reference to bulk channel ${number}, not open or referenced already`,
      );
      return false;
    }
    if (!this.#awaitBulk(number, message)) {
      return false;
    }
    // Its place in the order of admission is where its frame arrived, even
    // when its bulk channel is dispatched after later ones.
    this.#queueForBudget(message);
    return true;
  }

  /** Puts [message], whose bulk payload is about to be claimed, in line for the reassembly budget if it is charged to it. */
  #queueForBudget(message: IncomingMessage): void {
    if (message._budgeted && !message._forSink && this.options.reassemblyBudget > 0) {
      message._inAdmission = true;
      this.#admission.push(message);
    }
  }

  /**
   * @internal Admits the payloads waiting for the reassembly budget, in
   * order, while it is not full: each is charged what it may come to and
   * read from then on. One whose bulk channel is not dispatched yet holds
   * up those behind it.
   */
  _admit(): void {
    if (this.#admitting) {
      this.#admitAgain = true;
      return;
    }
    this.#admitting = true;
    try {
      do {
        this.#admitAgain = false;
        while (this.#admission.length > 0 && (!this.#closing || this._lingering !== undefined)) {
          const message = this.#admission[0] as IncomingMessage;
          if (!message._inAdmission) {
            this.#admission.shift();
            continue;
          }
          const b = message._bulk;
          if (b === undefined) {
            break;
          }
          if (this._assemblyBytes >= this.options.reassemblyBudget) {
            break;
          }
          this.#admission.shift();
          message._leaveAdmission();
          const length = message._bulkRef?.length;
          message._charge(
            message._streams ? streamHold(b, length) : (length ?? this.options.maxInlinePayload),
          );
          this.#startBulk(message);
          message._lane?.advance();
        }
      } while (this.#admitAgain);
    } finally {
      this.#admitting = false;
    }
  }

  /** The bulk payload of [frame], a message dropped on arrival, is not wanted: its bulk channel is closed CANCELLED, now or when it arrives. */
  #discardBulk(frame: TalkFrame): void {
    if (!frame.bulk || this.#mux === undefined) {
      return;
    }
    const number = frame.bulkReference.number;
    if (!this.#acceptBulkReference(number)) {
      return;
    }
    const b = this.#bulkUnclaimed.get(number);
    if (b !== undefined) {
      this.#bulkUnclaimed.delete(number);
      b.abandon();
      return;
    }
    if (number <= this.#bulkInHighest || this._bulkAwaited.has(number)) {
      this.#protocolError(
        `BULK reference to bulk channel ${number}, not open or referenced already`,
      );
      return;
    }
    this.#awaitBulk(number, undefined);
  }

  /**
   * On independent streams only the references, all on the parent, arrive
   * in number order. An awaited open can still arrive below this
   * watermark; an already matched number cannot be used a second time, and
   * a bulk channel below it that arrived unreferenced never will be
   * referenced: it is closed CANCELLED.
   */
  #acceptBulkReference(number: number): boolean {
    if (this.#orderedBulk) {
      return true;
    }
    if (number <= this.#bulkReferenceHighest) {
      this.#protocolError(`bulk reference ${number} reused or out of order`);
      return false;
    }
    this.#bulkReferenceHighest = number;
    for (const skipped of [...this.#bulkUnclaimed.keys()]) {
      if (skipped < number) {
        const b = this.#bulkUnclaimed.get(skipped) as BulkIn;
        this.#bulkUnclaimed.delete(skipped);
        b.close(Status.of(StatusCode.cancelled, 'bulk number not referenced'));
        b.abandon();
      }
    }
    return true;
  }

  /** Dispatch may lag, but ignored responses must not buy unbounded timers with the credit returned on arrival. */
  #awaitBulk(number: number, message: IncomingMessage | undefined): boolean {
    if (this._bulkAwaited.size >= this.options.maxUnclaimedBulk) {
      this.#protocolError('too many BULK references waiting for their channels');
      return false;
    }
    this._bulkAwaited.set(number, {
      message,
      timer: this._clock.setTimeout(
        () => this.#bulkNeverOpened(number),
        this.options.bulkOpenTimeout,
      ),
    });
    return true;
  }

  #bulkNeverOpened(number: number): void {
    if (this._bulkAwaited.delete(number)) {
      if (this._lingering !== undefined) {
        // The channel ended: the message is dropped with it.
        this._checkLinger();
        return;
      }
      this.#protocolError(
        `bulk channel ${number} not opened within ${this.options.bulkOpenTimeout} ms`,
      );
    }
  }

  /**
   * [b] carries the payload of [message]: delivered as a stream, or
   * reassembled first (see `streamBulk`), once the reassembly budget admits
   * it if it is charged to it.
   */
  #attachBulk(message: IncomingMessage, b: BulkIn): void {
    if (b.unclaimedTimer !== undefined) {
      this._clock.clearTimeout(b.unclaimedTimer);
      b.unclaimedTimer = undefined;
    }
    message._bulk = b;
    const length = message._bulkRef?.length;
    b.length = length;
    b.onProgress = message._onBulkProgress;
    // Until its bytes were all read, or it was abandoned.
    if (b._endStatus === undefined) {
      this._bulkIns.add(b);
    }
    const pending = this._outgoing.get(message.responseId);
    if (pending !== undefined) {
      pending.bulkIns.add(b);
      void b.channel.done.then(() => pending.bulkIns.delete(b));
    }
    let stream = message._forSink;
    const predicate = this.options.streamBulk;
    if (!stream && predicate !== undefined) {
      try {
        stream = predicate(message);
      } catch (e) {
        this._log('warn', 'streamBulk failed; reassembling the payload', e);
      }
    }
    message._streams = stream;
    const cap = this.options.maxInlinePayload;
    if (!stream && length !== undefined && length > cap) {
      // Over the per-message cap: fails at once, without waiting for the
      // budget.
      this.#failBulk(
        message,
        Status.of(
          StatusCode.resourceExhausted,
          `bulk payload of ${length} bytes, more than ${cap}`,
        ),
      );
    } else if (message._inAdmission) {
      // Until admitted, nothing reads it: its bulk channel holds what its
      // window lets the peer send, and gets no credit.
      b.waitingForBudget = true;
      this._admit();
      if (message._inAdmission) {
        // As many as may wait unclaimed, and more while what they hold fits
        // the budget.
        const hold = streamHold(b, length);
        const budget = this.options.reassemblyBudget;
        if (
          this._waitingCount >= this.options.maxUnclaimedBulk &&
          this._waitingBytes + hold > budget
        ) {
          this.#failBulk(
            message,
            Status.of(
              StatusCode.resourceExhausted,
              `${this._waitingCount} bulk payloads wait for the reassembly budget, ` +
                `holding ${this._waitingBytes} bytes`,
            ),
          );
        } else {
          message._waiting = true;
          message._waitBytes = hold;
          this._waitingCount++;
          this._waitingBytes += hold;
        }
      }
    } else {
      this.#startBulk(message);
    }
    message._lane?.advance();
  }

  /** Reads the bulk payload of [message], admitted (or not charged to the budget): a stream is ready for delivery, a reassembly starts. */
  #startBulk(message: IncomingMessage): void {
    const b = message._bulk as BulkIn;
    b.waitingForBudget = false;
    // The idle timeout counts from now: the wait was this side's.
    b._progressed();
    if (message._streams) {
      message._streamBulk = true;
      message._startDeferredTimer();
      return;
    }
    const collecting = b.collect(this.options.maxInlinePayload).then(
      (bytes) => {
        message._assembled = bytes;
        // Charged what it came to from now on, not what it might have.
        message._settleAssembly(bytes.length);
        message._startDeferredTimer();
        message._lane?.advance();
      },
      (error: unknown) => {
        message._releaseAssembly();
        message._bulkFailure =
          error instanceof SwitchboardError
            ? error.status
            : Status.of(StatusCode.internal, String(error));
        message._lane?.advance();
      },
    );
    this.#collecting.set(b, collecting);
    const collected = (): void => {
      this.#collecting.delete(b);
    };
    collecting.then(collected, collected);
  }

  /** The bulk payload of [message] fails with [status] before it is read: its bulk channel is closed with it, and the message fails as a failed reassembly does. */
  #failBulk(message: IncomingMessage, status: Status): void {
    message._leaveAdmission();
    message._bulkFailure = status;
    const b = message._bulk;
    if (b !== undefined) {
      b.close(status);
      b.abandon();
    }
  }

  /** Ends the bulk payloads in transfer in both directions: the channel ends. Outgoing ones are cancelled unless [keepOutgoing] (a graceful close lets them finish). */
  #endBulk(keepOutgoing: boolean): void {
    const cancelled = Status.of(StatusCode.cancelled, 'channel closed');
    for (const awaited of this._bulkAwaited.values()) {
      this._clock.clearTimeout(awaited.timer);
    }
    this._bulkAwaited.clear();
    const unclaimed = [...this.#bulkUnclaimed.values()];
    this.#bulkUnclaimed.clear();
    for (const b of unclaimed) {
      b.abandon();
    }
    const ins = [...this._bulkIns];
    this._bulkIns.clear();
    for (const b of ins) {
      b.close(cancelled);
      b.abandon();
    }
    if (!keepOutgoing) {
      for (const out of [...this._bulkOuts]) {
        out.cancel(cancelled);
      }
    }
  }

  // ---------------------------------------------------------------------
  // Messages

  #onMessage(frame: TalkFrame, wire: number): void {
    if (!frame.hasResponse) {
      if (this.#messagesCancelled) {
        this._credit(wire);
        this.#discardBulk(frame);
        if (frame.hasRequest) {
          const existing = this.#incoming.get(frame.requestId);
          if (existing !== undefined) {
            this.#incoming.delete(frame.requestId);
            existing._abandon();
          }
          this._rejectRequest(
            frame.requestId,
            Status.of(StatusCode.unimplemented, 'no message listener'),
          );
        }
        return;
      }
      const message = new IncomingMessage(this, frame);
      message._creditBytes = wire;
      if (frame.hasRequest && !this.#register(message)) {
        this._creditMessage(message);
        this.#discardBulk(frame);
        return;
      }
      if (!this.#claimBulk(message, false)) {
        return;
      }
      this.#messagesLane.add(message, () => this.#deliverIncoming(message));
      return;
    }
    const pending = this._outgoing.get(frame.responseId);
    if (pending === undefined || !pending.awaitsResponse) {
      this._credit(wire);
      this.#discardBulk(frame);
      this.#unknownResponse(frame);
      return;
    }
    pending.stopTimer();
    if (pending.abandoned) {
      this._outgoing.delete(pending.id);
      this._credit(wire);
      this.#discardBulk(frame);
      if (frame.hasRequest) {
        this._rejectRequest(
          frame.requestId,
          Status.of(StatusCode.cancelled, `request ${pending.id} cancelled`),
        );
      }
      return;
    }
    pending.finalReceived = true;
    const message = new IncomingMessage(this, frame);
    message._creditBytes = wire;
    // An answer settling a promise is taken by Talk itself, not held for a
    // consumer: no share of the reassembly budget.
    message._budgeted = pending.ordered;
    pending.finalMessage = message;
    if (frame.hasRequest) {
      this.#register(message);
    }
    if (!this.#claimBulk(message, pending.sink !== undefined)) {
      return;
    }
    if (pending.ordered) {
      // Credit when the answer is delivered, in its place.
      this.#messagesLane.add(message, () => this.#answer(pending, message, true));
    } else {
      pending.lane.add(message, () => this.#answer(pending, message, false));
    }
  }

  /** Delivers a plain message or request to {@link messages}, once its bulk payload (if any) is ready; one whose payload failed is not delivered, and a request is answered with the payload's status. */
  #deliverIncoming(message: IncomingMessage): void {
    const failure = message._bulkFailure;
    if (failure !== undefined) {
      this._log('debug', `bulk payload of ${message.frame} failed: ${failure}`);
      this._creditMessage(message);
      message._abortQuietly(failure);
      return;
    }
    this.#deliverMessage(message);
  }

  /** Delivers [message], the final response to [pending], in its turn. */
  #answer(pending: Outgoing, message: IncomingMessage, ordered: boolean): void {
    if (this._outgoing.get(pending.id) !== pending) {
      // Failed meanwhile: cancelled, or the channel ended.
      message._drop();
      return;
    }
    this._outgoing.delete(pending.id);
    const failure = message._bulkFailure;
    if (failure !== undefined) {
      this._log('debug', `bulk payload of ${message.frame} failed: ${failure}`);
      this._creditMessage(message);
      message._abortQuietly(failure);
      const error = new SwitchboardError(failure);
      if (ordered) {
        this.#deliverOrdered(new IncomingMessage(this, message.frame), () => pending.fail(error));
      } else {
        pending.fail(error);
      }
      return;
    }
    if (ordered) {
      this.#deliverOrdered(message, () => pending.complete(message));
    } else {
      this._handOver(message, undefined, () => pending.complete(message));
    }
  }

  #onStreamItem(frame: TalkFrame, wire: number): void {
    const pending = this._outgoing.get(frame.responseId);
    if (pending === undefined || !pending.awaitsResponse) {
      this._credit(wire);
      this.#discardBulk(frame);
      this.#unknownResponse(frame);
      return;
    }
    if (pending.abandoned) {
      this._credit(wire);
      this.#discardBulk(frame);
      if (frame.hasRequest) {
        this._rejectRequest(
          frame.requestId,
          Status.of(StatusCode.cancelled, `request ${pending.id} cancelled`),
        );
      }
      return;
    }
    if (!pending.isStream) {
      this._credit(wire);
      this.#protocolError(`STREAM_ITEM for non-stream request ${pending.id}`);
      return;
    }
    if (pending.window === 0) {
      // The responder may not exceed the window (wiki page "Polyverse
      // Switchboard Talk", "Item windows"): a channel protocol error on its
      // part, as for a request without STREAM.
      this._credit(wire);
      this.#protocolError(`STREAM_ITEM beyond the window of request ${pending.id}`);
      return;
    }
    pending.itemArrived();
    const message = new IncomingMessage(this, frame);
    message._creditBytes = wire;
    if (frame.hasRequest) {
      this.#register(message);
    }
    // The payload of an item arriving is progress of the request, as the
    // item was.
    message._onBulkProgress = () => {
      if (!pending.ended && !pending.finalReceived) {
        pending.replied();
      }
    };
    if (!this.#claimBulk(message, pending.sink !== undefined)) {
      return;
    }
    pending.lane.add(message, () => this.#deliverItem(pending, message));
  }

  /** Delivers [message], an item of [pending], in its turn. */
  #deliverItem(pending: Outgoing, message: IncomingMessage): void {
    const failure = message._bulkFailure;
    if (failure !== undefined) {
      this._log('debug', `bulk payload of ${message.frame} failed: ${failure}`);
      this._creditMessage(message);
      message._abortQuietly(failure);
      this._cancelOutgoing(pending, failure);
      return;
    }
    pending.addItem(message);
  }

  #onAbort(frame: TalkFrame): void {
    if (frame.hasResponse) {
      const pending = this._outgoing.get(frame.responseId);
      if (pending === undefined || !pending.awaitsResponse) {
        this._log('debug', `abort for unknown request ${frame.responseId} ignored`);
        return;
      }
      pending.stopTimer();
      if (pending.abandoned) {
        this._outgoing.delete(pending.id);
        return;
      }
      pending.finalReceived = true;
      const error = new TalkAbortError(abortStatus(frame));
      if (pending.ordered) {
        this.#messagesLane.add(undefined, () => {
          if (this._outgoing.get(pending.id) === pending) {
            this._outgoing.delete(pending.id);
            this.#deliverOrdered(new IncomingMessage(this, frame), () => pending.fail(error));
          }
        });
      } else {
        pending.lane.add(undefined, () => {
          if (this._outgoing.get(pending.id) === pending) {
            this._outgoing.delete(pending.id);
            pending.fail(error);
          }
        });
      }
    } else if (frame.hasRequest) {
      const message = this.#incoming.get(frame.requestId);
      if (message === undefined) {
        this._log('debug', `cancel for unknown request ${frame.requestId} ignored`);
        return;
      }
      message._cancelledByPeer(frame.status);
    } else {
      const status = abortStatus(frame);
      this._log('info', `peer aborted the channel: ${status}`);
      this.#endStatus ??= status;
      const error = new TalkAbortError(status, { isChannelAbort: true });
      this.#terminate(error, { reportOnMessages: true });
      void this.close(status);
    }
  }

  #onExtend(frame: TalkFrame, wire: number): void {
    if (frame.isCredit) {
      // Consumed here, its credit back at once.
      this._credit(wire);
      const message = this.#incoming.get(frame.requestId);
      if (message === undefined) {
        // Not outstanding: it may have raced the end of the request.
        this._log('debug', `credit for unknown request ${frame.requestId} ignored`);
        return;
      }
      const error = message._onCredit(frame.creditItems);
      if (error !== undefined) {
        this.#protocolError(error);
      }
      return;
    }
    const pending = this._outgoing.get(frame.responseId);
    if (pending === undefined || pending.abandoned || !pending.awaitsResponse) {
      this._credit(wire);
      return;
    }
    pending.extended(frame, wire);
  }

  #unknownResponse(frame: TalkFrame): void {
    this._log('debug', `response for unknown request ${frame.responseId} ignored`);
    if (frame.hasRequest) {
      this._rejectRequest(
        frame.requestId,
        Status.of(StatusCode.notFound, `unknown response id ${frame.responseId}`),
      );
    }
  }

  #protocolError(reason: string): void {
    this._log('warn', `talk protocol error: ${reason}`);
    void this.close(Status.of(StatusCode.protocolError, reason));
  }

  #onError(error: unknown): void {
    if (this.#closing) {
      return;
    }
    this._log('warn', 'raw channel failed', error);
    const status =
      error instanceof SwitchboardError
        ? error.status
        : Status.of(StatusCode.connectionLost, String(error));
    void this.close(status);
  }

  #onDone(): void {
    this.#rawEnded = true;
    if (this.#closing) {
      return;
    }
    this.#closing = true;
    this.#peerEndedPromise = this.#peerEnded();
  }

  async #peerEnded(): Promise<void> {
    let status = Status.of(StatusCode.connectionLost, 'channel closed');
    let graceful = false;
    const raw = this.#raw;
    if (isStatusClosable(raw)) {
      try {
        const end = await raw.done;
        if (!end.isOk) {
          status = end;
        } else {
          graceful = true;
        }
      } catch (e) {
        this._log('debug', 'raw channel done failed', e);
      }
    } else {
      this.#writer?.close().catch(noop);
    }
    const error = new SwitchboardError(status);
    if (graceful && !this.#orderedBulk) {
      await this.#linger(error);
    } else if (graceful) {
      // The bulk channels of the messages that arrived before the close
      // arrived whole before it too, but their bytes are read
      // asynchronously here (the Dart mux delivers them as they arrive):
      // the reassemblies of those that ended finish before the channel
      // ends, as there.
      const arrived: Promise<void>[] = [];
      for (const [b, collecting] of this.#collecting) {
        if (b.channel.state === 'closed') {
          arrived.push(collecting);
        }
      }
      if (arrived.length > 0) {
        await Promise.allSettled(arrived);
      }
    }
    this.#terminate(error);
    // What waited for the peer's LIMITS goes nowhere: after the requests it
    // carries failed with the channel's status, its senders hear of it.
    this.#dropDeferred(error);
    await this.#finish();
  }

  /**
   * The peer closed the channel gracefully, and its bulk channels are not
   * ordered with it (a stream each): its payloads may still be arriving.
   * Waits until every bulk channel a message that arrived before the close
   * references has arrived and ended, or failed, as long as they progress:
   * at most `bulkIdleTimeout` (30 s when that is 0) without a byte arriving
   * or being read. Meanwhile the bulk channels that name this channel go
   * on coming here (within the bound per connection): admitted for a
   * number a message waits for, closed FAILED_PRECONDITION otherwise. What
   * cannot be answered any more ends at once: requests without their final
   * response fail with [error], incoming requests become unanswerable,
   * unreferenced bulk channels are closed.
   */
  async #linger(error: SwitchboardError): Promise<void> {
    const unclaimed = [...this.#bulkUnclaimed.values()];
    this.#bulkUnclaimed.clear();
    for (const b of unclaimed) {
      b.abandon();
    }
    for (const pending of [...this._outgoing.values()]) {
      if (pending.finalReceived) {
        continue;
      }
      this._outgoing.delete(pending.id);
      if (pending.abandoned) {
        pending.stopTimer();
      } else {
        pending.fail(error);
      }
    }
    const incoming = [...this.#incoming.values()];
    this.#incoming.clear();
    for (const message of incoming) {
      message._abandon(error);
    }
    // What a dropped message referenced is not waited for.
    for (const [number, awaited] of [...this._bulkAwaited]) {
      if (awaited.message === undefined) {
        this._clock.clearTimeout(awaited.timer);
        this._bulkAwaited.delete(number);
      }
    }
    const mux = this.#mux;
    if (mux === undefined || this.#bulkSettled) {
      return;
    }
    const settled = new Deferred<void>();
    if (!lingerBulkTarget(mux, this.#bulkTarget, settled.promise)) {
      this._log('debug', 'too many channels waiting for bulk payloads; not waiting');
      settled.resolve();
      return;
    }
    this._lingering = settled;
    this._lingerProgress = this._clock.now();
    let limit = this.options.bulkIdleTimeout;
    if (limit <= 0) {
      limit = 30_000;
    }
    let timer: TimerHandle | undefined;
    const arm = (delay: number): void => {
      timer = this._clock.setTimeout(
        () => {
          if (settled.isSettled) {
            return;
          }
          const idle = this._clock.now() - this._lingerProgress;
          if (idle < limit) {
            arm(limit - idle);
            return;
          }
          this._log('debug', `bulk payloads made no progress for ${limit} ms after the close`);
          settled.resolve();
        },
        delay > maxTimerDelay ? maxTimerDelay : delay,
      );
    };
    arm(limit);
    await settled.promise;
    if (timer !== undefined) {
      this._clock.clearTimeout(timer);
    }
    this._lingering = undefined;
    // What arrived last reaches its readers before the channel ends.
    await new Promise<void>((resolve) => this._clock.setTimeout(resolve, 0));
  }

  /** @internal Ends the wait for bulk payloads after the peer's close once nothing it waits for is left. */
  _checkLinger(): void {
    const settled = this._lingering;
    if (settled !== undefined && !settled.isSettled && this.#bulkSettled) {
      settled.resolve();
    }
  }

  /** No bulk channel is awaited, and every one attached arrived whole or failed. */
  get #bulkSettled(): boolean {
    if (this._bulkAwaited.size > 0) {
      return false;
    }
    for (const b of this._bulkIns) {
      if (b.channel.state !== 'closed') {
        return false;
      }
    }
    return true;
  }

  // ---------------------------------------------------------------------
  // Shutdown

  /**
   * Stops all timers, fails outstanding outgoing requests with [error],
   * abandons outstanding incoming requests, and ends {@link messages},
   * with [error] first if `reportOnMessages`. Idempotent.
   */
  #terminate(
    error: SwitchboardError,
    options: { reportOnMessages?: boolean; keepOutgoingBulk?: boolean } = {},
  ): void {
    this.#closing = true;
    if (this.#terminated) {
      return;
    }
    this.#terminated = true;
    const outgoing = [...this._outgoing.values()];
    this._outgoing.clear();
    for (const pending of outgoing) {
      if (pending.abandoned) {
        pending.stopTimer();
      } else {
        pending.fail(error);
      }
    }
    // Answers waiting as markers arrived before the end: they keep their
    // place in the delivery, which still hands out what it holds once
    // closed.
    const incoming = [...this.#incoming.values()];
    this.#incoming.clear();
    for (const message of incoming) {
      message._abandon(error);
    }
    // What arrived whole keeps its place; what waits for a bulk payload in
    // transfer is dropped with it.
    this.#messagesLane.flush();
    // Nothing is admitted any more (the item lanes were dropped with their
    // requests).
    const admission = this.#admission;
    this.#admission = [];
    for (const message of admission) {
      message._leaveAdmission();
    }
    this.#endBulk(options.keepOutgoingBulk === true);
    if (options.reportOnMessages === true) {
      this.#messages.fail(error);
    } else {
      this.#messages.close();
    }
  }

  #finish(): Promise<void> {
    if (!this.#finishing) {
      this.#finishing = true;
      void this.#completeDone();
    }
    return this.#done.promise.then(noop);
  }

  async #completeDone(): Promise<void> {
    let status = this.#endStatus;
    if (status === undefined) {
      const raw = this.#raw;
      if (isStatusClosable(raw)) {
        try {
          status = await raw.done;
        } catch (e) {
          status = Status.of(StatusCode.connectionLost, String(e));
        }
      } else {
        status = Status.ok;
      }
    }
    this.#done.resolve(status);
  }

  toString(): string {
    const mux = this.#mux;
    return `TalkChannel(${mux === undefined ? 'frames' : `channel ${mux.id}`})`;
  }
}

/** What the bulk channel of [b], a payload of [length] bytes (undefined: unknown), may hold while nobody reads it: what arrived, and what its window still lets the peer send. */
function streamHold(b: BulkIn, length: number | undefined): number {
  // What arrived is counted on the channel, at cost: [b] reads it through
  // the channel's readable, so bytes delivered and not consumed (its
  // buffer, unread, and what is on the way to it: heldBytes) or not yet
  // delivered (bufferedBytes) are there even once the channel closed.
  const channel = b.channel;
  let hold = channel.heldBytes + channel.bufferedBytes;
  if (channel.state !== 'closed') {
    hold += channel.receiveWindow;
  }
  return length !== undefined && length < hold ? length : hold;
}
