/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * The frame mux's channel: OPEN, DATA and CLOSE frames over the
 * connection's one transport, flow controlled by LIMITS and CREDIT (wiki
 * page "Polyverse Switchboard Mux"). Ports `frame_mux_channel.dart`: the
 * channel, and the link through which its connection drives it.
 */

import { ProtocolError, Status, StatusCode, SwitchboardError } from '../status.ts';
import { Fifo } from '../transport/fifo.ts';
import type { TimerHandle } from '../util/clock.ts';
import { Deferred } from '../util/deferred.ts';
import type {
  MuxChannel,
  MuxChannelState,
  MuxConnection,
  MuxOptions,
  MuxPriority,
} from './contract.ts';
import { MuxControlMessage, MuxCredit, MuxFrame, MuxLimits } from './mux-frame.ts';
import { checkMuxStatus, fitStatus, readSubframes } from './mux-protocol.ts';

/**
 * What a channel needs of its connection; the connection implements it
 * over its private state (Dart `MuxChannelHost`). Not part of the
 * package's API.
 *
 * @internal
 */
export interface MuxChannelHost {
  /** The connection's options. */
  readonly options: MuxOptions;

  /** Largest frame the peer accepts, 0 when unknown or unlimited. */
  peerMaxFrameSize(): number;

  /** Moves queued output to [link]'s current priority tier. */
  reprioritize(link: MuxChannelLink): void;

  /**
   * Sends [frame] if the transport is still writable; never throws. Not
   * held by the output scheduler: for control messages and frames of
   * channels with nothing waiting for the transport.
   */
  sendFrame(frame: MuxFrame): void;

  /**
   * Hands [frame] of [link] to the output scheduler: DATA waits for the
   * transport to take it, in [link]'s queue; a CLOSE follows what is
   * queued before it, and goes at once when nothing is. An OPEN of the
   * channel [opens] (`openAfter`) likewise; once it is written, what that
   * channel sent meanwhile follows. Frames of a channel whose OPEN has not
   * gone out wait for it.
   */
  sendChannelFrame(link: MuxChannelLink, frame: MuxFrame, opens?: MuxChannelLink): void;

  /**
   * Creates the channel of `openAfter` on [parent]'s connection, its OPEN
   * not sent: the caller hands it to {@link sendChannelFrame} in its place.
   * Throws like `MuxConnection.open`.
   */
  openAfter(parent: MuxChannelLink, openPayload: Uint8Array): MuxChannelLink;

  /** The OPEN of [link] will never go out: ends the channel locally with [status] and frees its id. */
  openFailed(link: MuxChannelLink, status: Status): void;

  /** Drops the DATA of [link] waiting for the transport (the peer closed the channel) and sends at once what remains queued for it. */
  dropQueuedData(link: MuxChannelLink): void;

  /** Called once when [link] is mutually closed, so its id can be reused. */
  release(link: MuxChannelLink): void;

  /** Called once when the peer did not confirm [link]'s CLOSE in time. */
  abandon(link: MuxChannelLink): void;

  /** Adds [delta] (negative when bytes are delivered or dropped) to the bytes buffered in channel receive queues. */
  noteBuffered(delta: number): void;
}

/**
 * What the link calls on its channel; the channel hands it over when it
 * is made.
 *
 * @internal
 */
export interface FrameMuxChannelControl {
  closeReceived(): boolean;
  receive(payload: Uint8Array): void;
  receiveCredit(bytes: number): void;
  dataWritten(cost: number): void;
  limitsKnown(window: number): void;
  receiveClose(status: Status): void;
  closeUndelivered(status: Status): void;
  neverOpened(status: Status): void;
  complete(): void;
  connectionLost(status: Status): void;
  openWrittenAfterClose(): void;
}

/** What a link is made with. */
export interface MuxChannelLinkInit {
  readonly id: number;
  readonly isLocallyOpened: boolean;
  /** Owned by the channel: the caller copied it. */
  readonly openPayload: Uint8Array;
  readonly sendWindow: number;
  readonly receiveWindow: number;
  readonly creditWindow: number;
  readonly awaitingLimits?: boolean;
}

/** A frame handed over before its channel's OPEN was written, with the channel it opens if it is an OPEN. */
export type PreOpenFrame = readonly [MuxFrame, MuxChannelLink | undefined];

/**
 * The connection's handle on a channel: delivers received frames and
 * connection loss, and keeps the connection's bookkeeping of the channel,
 * off the public {@link MuxChannel} API (Dart `MuxChannelLink`). Made by
 * the connection.
 *
 * @internal
 */
export class MuxChannelLink {
  /** The owning connection's hooks. */
  readonly host: MuxChannelHost;

  /** The channel. */
  readonly channel: FrameMuxChannel;

  /** Whether the connection still holds this channel for delivery on `incoming`. */
  incomingPending = false;

  /**
   * False while the OPEN of a channel opened with `openAfter` waits behind
   * its parent's frames: the id is only reserved locally, and the peer's
   * frames on it are refused. Set just before the OPEN is written.
   */
  openWritten = true;

  /**
   * Whether the frames handed over for this channel wait in
   * {@link preOpen}: from `openAfter` until its OPEN is written and the
   * frames that waited have followed it.
   */
  holdsFrames = false;

  /**
   * Frames of this channel handed to the connection before its OPEN was
   * written, in order. Payloads are owned copies, since handing them over
   * completes their send promises.
   */
  preOpen: PreOpenFrame[] = [];

  readonly #control: FrameMuxChannelControl;

  /** Creates the channel. */
  constructor(host: MuxChannelHost, connection: MuxConnection, init: MuxChannelLinkInit) {
    this.host = host;
    let control: FrameMuxChannelControl | undefined;
    this.channel = new FrameMuxChannel(this, connection, init, (bound) => {
      control = bound;
    });
    this.#control = control as FrameMuxChannelControl;
  }

  /** The channel id. */
  get id(): number {
    return this.channel.id;
  }

  /** Whether the peer's CLOSE has been received. */
  get closeReceived(): boolean {
    return this.#control.closeReceived();
  }

  /** Ends a channel whose OPEN never went out, with [status]. */
  neverOpened(status: Status): void {
    this.#control.neverOpened(status);
  }

  /**
   * Delivers a DATA payload. The caller has checked that the peer's CLOSE
   * has not been received.
   *
   * @throws {ProtocolError} if the payload goes beyond the window granted to the peer.
   */
  receiveData(payload: Uint8Array): void {
    this.#control.receive(payload);
  }

  /**
   * Applies the peer's CREDIT of [bytes] for this channel.
   *
   * @throws {ProtocolError} if the window would exceed `2^32 - 1`.
   */
  receiveCredit(bytes: number): void {
    this.#control.receiveCredit(bytes);
  }

  /** DATA of [cost] bytes of credit is going to the transport now: its reservation ends. */
  dataWritten(cost: number): void {
    this.#control.dataWritten(cost);
  }

  /** The wait for the peer's LIMITS ended; [window] is the initial window of a channel opened before it. */
  limitsKnown(window: number): void {
    this.#control.limitsKnown(window);
  }

  /**
   * Handles the peer's CLOSE: frees the id, sends the confirming CLOSE if
   * this side has not sent its own (or its own if it still waited behind
   * subframes, which are dropped), and completes the channel.
   */
  receiveClose(status: Status): void {
    this.#control.receiveClose(status);
  }

  /** Closes a channel the application never received, with CLOSE carrying [status], dropping whatever it buffered. */
  closeUndelivered(status: Status): void {
    this.#control.closeUndelivered(status);
  }

  /** Completes the channel locally because the peer did not confirm its CLOSE in time. The connection keeps the id reserved. */
  abandoned(): void {
    this.#control.complete();
  }

  /** Ends the channel because the connection ended. */
  connectionLost(status: Status): void {
    this.#control.connectionLost(status);
  }

  /** Arms the close confirmation timer of a channel whose CLOSE was requested before its OPEN went out. */
  openWrittenAfterClose(): void {
    this.#control.openWrittenAfterClose();
  }
}

/** A subframe waiting for flow-control credit, the rest of a bulk payload, or an OPEN ordered behind them. */
class PendingSend {
  readonly subframe: Uint8Array;
  /** Whether {@link subframe} is a bulk payload, sent in chunks. */
  readonly split: boolean;
  /** The OPEN frame of {@link opens} (`openAfter`); undefined for DATA. */
  readonly openFrame: MuxFrame | undefined;
  /** The channel {@link openFrame} opens. */
  readonly opens: MuxChannelLink | undefined;
  /** Bytes of a bulk payload handed over so far. */
  offset = 0;
  /** Settles when handed over; a failure is never reported as unhandled. */
  readonly done = new Deferred<void>();

  constructor(subframe: Uint8Array, split: boolean, open?: [MuxFrame, MuxChannelLink]) {
    this.subframe = subframe;
    this.split = split;
    this.openFrame = open?.[0];
    this.opens = open?.[1];
  }
}

const empty = new Uint8Array(0);

/** The status a `writable.abort(reason)` closes the channel with. */
function abortStatus(reason: unknown): Status {
  let status: Status;
  if (reason instanceof SwitchboardError) {
    status = reason.status;
  } else if (reason instanceof Status) {
    status = reason;
  } else if (reason === undefined) {
    status = Status.of(StatusCode.cancelled);
  } else {
    status = Status.of(
      StatusCode.internal,
      reason instanceof Error ? reason.message : String(reason),
    );
  }
  return status.isApplicationCode ? Status.of(StatusCode.unknown, status.toString()) : status;
}

/**
 * The frame mux's channel: a {@link MuxChannel} of a `FrameMuxConnection`
 * carried as OPEN, DATA and CLOSE frames over the connection's one
 * transport. Made by the connection (`open`, `incoming`, `openAfter`),
 * never by an application.
 *
 * The frame mux's specifics, beside the contract of {@link MuxChannel}:
 *
 * - A DATA frame costs its subframe's length plus 16 ({@link sendCost}),
 *   so that a flood of empty subframes is bounded too.
 * - {@link maxSubframeLength} keeps a frame's cost to half the channel's
 *   initial window (32752 bytes with the default 64 KiB), so that a peer
 *   returning credit in batches of up to half its window never leaves a
 *   subframe waiting for ever, within the frame limit the peer announced.
 * - On a channel opened before the peer's LIMITS arrived, DATA waits for
 *   it ({@link awaitingLimits}, `MuxOptions.awaitPeerLimits`).
 * - Credit is returned once half the window it is returned against is
 *   consumed and unreturned: as `readable` delivers subframes, or as
 *   {@link consumed} reports them under {@link manualCredit}.
 * - `readable` has a high-water mark of 0: nothing is delivered (and no
 *   credit returned) until a read asks for it. Cancelling it drops what
 *   arrives from then on, which counts as consumed; it does not close the
 *   channel.
 */
export class FrameMuxChannel implements MuxChannel {
  /** The connection this channel belongs to. */
  readonly connection: MuxConnection;

  /** The channel id: even ids are allocated by the initiator, odd ids by the acceptor. */
  readonly id: number;

  /** Whether this side opened the channel. */
  readonly isLocallyOpened: boolean;

  /** The payload of the OPEN frame, passed through unchanged. */
  readonly openPayload: Uint8Array;

  readonly #link: MuxChannelLink;
  readonly #host: MuxChannelHost;
  readonly #done = new Deferred<Status>();
  #state: MuxChannelState = 'open';
  #priority: MuxPriority = 'normal';
  #endStatus: Status = Status.ok;
  // Whether this side decided to close: close() was called or the peer's
  // CLOSE was confirmed. The CLOSE may still wait behind subframes waiting
  // for credit (#pendingClose).
  #closeSent = false;
  #closeReceived = false;
  #confirmTimer: TimerHandle | undefined;

  // Receiving: the subframes not delivered yet, at cost.
  readonly #queue = new Fifo<Uint8Array>();
  #queuedBytes = 0;
  #discarding = false;
  #endRequested = false;
  // The streams are made on first use: a channel nobody reads or writes
  // through them (a Talk channel, a rejected or adopted one) never pays
  // for them.
  #readable: ReadableStream<Uint8Array> | undefined;
  #writable: WritableStream<Uint8Array> | undefined;
  #controller: ReadableStreamDefaultController<Uint8Array> | undefined;
  #readableClosed = false;
  // A read waiting for a subframe.
  #pull: Deferred<void> | undefined;

  // Flow control, sending side: the window the peer granted, the
  // subframes waiting for it (in order), and the CLOSE waiting behind
  // them. While the peer's LIMITS is awaited the window is 0 and the
  // initial window the 64 KiB assumed without LIMITS.
  #initialSendWindow: number;
  #sendWindow: number;
  // Credit reserved for DATA handed to the scheduler but not written yet.
  // The peer cannot have consumed it: it still counts toward the u32 cap.
  #reservedSendCost = 0;
  #awaitingLimits: boolean;
  #limitsKnown: Deferred<void> | undefined;
  readonly #pendingSends = new Fifo<PendingSend>();
  #pendingClose: Status | undefined;
  #closeQueued = false;
  #admitting = false;
  #writableWaiters: Deferred<void>[] = [];

  // Flow control, receiving side: what the peer may still send, the
  // window credit is returned against (half of it is the batch), what was
  // consumed and not returned yet, and, with manual credit, what was
  // delivered and not reported consumed. Every byte of cost the peer may
  // send is in exactly one of these: the receive window, #queuedBytes,
  // #held or #unreturned.
  #receiveWindow: number;
  #creditWindow: number;
  #unreturned = 0;
  #held = 0;
  #manualCredit = false;
  // Set when the channel completes: nothing is held or returned any more.
  #released = false;

  /**
   * Made by the connection through a {@link MuxChannelLink}, which receives
   * the channel's control through [bind]; not for applications.
   */
  constructor(
    link: MuxChannelLink,
    connection: MuxConnection,
    init: MuxChannelLinkInit,
    bind: (control: FrameMuxChannelControl) => void,
  ) {
    this.#link = link;
    this.#host = link.host;
    this.connection = connection;
    this.id = init.id;
    this.isLocallyOpened = init.isLocallyOpened;
    this.openPayload = init.openPayload;
    const awaitingLimits = init.awaitingLimits ?? false;
    this.#awaitingLimits = awaitingLimits;
    this.#initialSendWindow = awaitingLimits ? MuxLimits.defaultInitialWindow : init.sendWindow;
    this.#sendWindow = awaitingLimits ? 0 : this.#initialSendWindow;
    this.#receiveWindow = init.receiveWindow;
    this.#creditWindow = init.creditWindow;
    bind({
      closeReceived: () => this.#closeReceived,
      receive: (payload) => this.#receive(payload),
      receiveCredit: (bytes) => this.#receiveCredit(bytes),
      dataWritten: (cost) => {
        if (this.#state !== 'closed') {
          this.#reservedSendCost -= cost;
        }
      },
      limitsKnown: (window) => this.#onLimitsKnown(window),
      receiveClose: (status) => this.#receiveClose(status),
      closeUndelivered: (status) => {
        // Closed first, so that dropping what it buffered returns no credit.
        if (this.#state === 'open') {
          void this.close(status);
        }
        this.#discard();
      },
      neverOpened: (status) => {
        this.#link.preOpen = [];
        this.#noteStatus(status);
        this.#closeSent = true;
        this.#closeReceived = true;
        this.#complete();
      },
      complete: () => this.#complete(),
      connectionLost: (status) => {
        this.#noteStatus(status);
        this.#complete();
      },
      openWrittenAfterClose: () => {
        if (this.#closeSent && this.#state === 'halfClosedLocal') {
          this.#startConfirmTimer();
        }
      },
    });
  }

  // Streams -----------------------------------------------------------------

  /** Incoming subframes, one per chunk; see {@link MuxChannel.readable}. */
  get readable(): ReadableStream<Uint8Array> {
    this.#readable ??= new ReadableStream<Uint8Array>(
      {
        start: (controller) => {
          this.#controller = controller;
        },
        pull: () => this.#onPull(),
        cancel: () => {
          this.#readableClosed = true;
          this.#discard();
        },
      },
      { highWaterMark: 0 },
    );
    return this.#readable;
  }

  /**
   * The subframes of {@link readable}, for `for await (const subframe of
   * channel)`: each step takes delivery of one, which lets its credit go
   * back; stopping early cancels the readable, which discards what arrives
   * from then on and leaves the channel open. Locks {@link readable}.
   */
  [Symbol.asyncIterator](): AsyncIterableIterator<Uint8Array, undefined> {
    return readSubframes(this.readable);
  }

  /** Outgoing subframes; see {@link MuxChannel.writable}. */
  get writable(): WritableStream<Uint8Array> {
    this.#writable ??= new WritableStream<Uint8Array>({
      write: (chunk) => {
        if (!(chunk instanceof Uint8Array)) {
          throw new TypeError(`channel ${this.id} takes Uint8Array subframes`);
        }
        return this.send(chunk);
      },
      close: () => this.close(),
      abort: (reason: unknown) => this.close(abortStatus(reason)),
    });
    return this.#writable;
  }

  // State -------------------------------------------------------------------

  /** The channel's state. */
  get state(): MuxChannelState {
    return this.#state;
  }

  /** Whether DATA may be sent: the state is `open`. */
  get canSend(): boolean {
    return this.#state === 'open';
  }

  /**
   * The output tier of what this side sends: it applies to the frames the
   * scheduler picks from then on, and to the splitting of payloads
   * {@link send} takes from then on.
   */
  get priority(): MuxPriority {
    return this.#priority;
  }

  set priority(value: MuxPriority) {
    if (value === this.#priority) {
      return;
    }
    if (value !== 'normal' && value !== 'bulk') {
      throw new RangeError(`priority must be 'normal' or 'bulk', not ${String(value)}`);
    }
    this.#priority = value;
    this.#host.reprioritize(this.#link);
  }

  /** Bytes received and not delivered yet, each subframe at its cost (length plus 16); bounded by the window this side granted. */
  get bufferedBytes(): number {
    return this.#queuedBytes;
  }

  /** Whether something reads {@link readable}. */
  get hasListener(): boolean {
    return (this.#readable?.locked ?? false) && !this.#discarding;
  }

  /** Whether DATA waits for the peer's LIMITS: until then {@link maxSubframeLength} reflects the 64 KiB assumed without one. */
  get awaitingLimits(): boolean {
    return this.#awaitingLimits && this.#state !== 'closed';
  }

  /** Resolves once the wait for the peer's LIMITS ended, or the channel ended first. */
  get limitsKnown(): Promise<void> {
    if (!this.awaitingLimits) {
      return Promise.resolve();
    }
    this.#limitsKnown ??= new Deferred<void>();
    return this.#limitsKnown.promise;
  }

  /**
   * Bytes of cost the peer may still send before this side returns
   * credit. Starts at `MuxOptions.initialWindow` as announced (64 KiB
   * without LIMITS); for a channel the peer opened, which it may have
   * opened before this side's LIMITS reached it, at the larger of the two.
   * DATA beyond it ends the connection with GOAWAY `PROTOCOL_ERROR`.
   */
  get receiveWindow(): number {
    return this.#receiveWindow;
  }

  /**
   * Whether the layer reading the channel reports consumption itself with
   * {@link consumed}; false by default. Set it before reading. Setting it
   * back to false counts what is held as consumed. What is held does not
   * count toward the connection's `bufferedBytes` and its receive
   * high-water mark: the window bounds it.
   */
  get manualCredit(): boolean {
    return this.#manualCredit;
  }

  set manualCredit(value: boolean) {
    if (value === this.#manualCredit) {
      return;
    }
    this.#manualCredit = value;
    if (!value && this.#held > 0) {
      const cost = this.#held;
      this.#held = 0;
      this.#consume(cost);
    }
  }

  /** With {@link manualCredit}: the cost of the subframes delivered and not reported consumed. 0 once the channel is closed. */
  get heldBytes(): number {
    return this.#held;
  }

  /**
   * With {@link manualCredit}: reports that [subframes] subframes, [bytes]
   * bytes long together, have been consumed, so that their cost ([bytes]
   * plus 16 per subframe) can go back to the peer. Does nothing once the
   * channel is closed.
   *
   * @throws {RangeError} for negative values.
   * @throws {Error} when more is reported than was delivered and not yet
   *   reported (always without {@link manualCredit}).
   */
  consumed(bytes: number, subframes = 1): void {
    if (bytes < 0 || subframes < 0) {
      throw new RangeError(`negative bytes (${bytes}) or subframes (${subframes})`);
    }
    if (this.#released) {
      return;
    }
    const cost = bytes + MuxCredit.frameOverhead * subframes;
    if (cost > this.#held) {
      throw new Error(
        `channel ${this.id}: ${cost} bytes of cost reported consumed, ${this.#held} ` +
          'delivered and not consumed',
      );
    }
    this.#held -= cost;
    this.#consume(cost);
  }

  /**
   * Raises the window the peer may use by [bytes] at once: sends CREDIT
   * for them (with the credit consumed and not returned yet), and returns
   * credit against the larger window from then on. Does nothing once this
   * side can no longer receive DATA it would want more of.
   *
   * @throws {RangeError} if [bytes] is not positive, or if the window
   *   would exceed `2^32 - 1`.
   */
  grant(bytes: number): void {
    if (!(bytes > 0) || !Number.isInteger(bytes)) {
      throw new RangeError(`granted bytes must be a positive integer, not ${bytes}`);
    }
    const credit = bytes + this.#unreturned;
    // What is queued and held comes back as credit too.
    if (this.#receiveWindow + this.#queuedBytes + this.#held + credit > MuxLimits.maxWindow) {
      throw new RangeError(`the window of channel ${this.id} would exceed 2^32 - 1`);
    }
    if (!this.#mayReturnCredit) {
      return;
    }
    this.#creditWindow += bytes;
    this.#unreturned = 0;
    this.#receiveWindow += credit;
    this.#host.sendFrame(MuxControlMessage.credit(new MuxCredit(this.id, credit)).toFrame());
  }

  /** Resolves with the end status once the channel is closed; never rejects. */
  get done(): Promise<Status> {
    return this.#done.promise;
  }

  /**
   * Bytes of cost this side may still send before the peer returns credit.
   * 0 on a channel opened before the peer's LIMITS arrived, until then.
   * Credit reserved by DATA waiting in the output scheduler is left out,
   * but still counts toward the `2^32 - 1` cap on the peer's grants.
   */
  get sendWindow(): number {
    return this.#sendWindow;
  }

  /** A DATA frame costs its subframe's length plus 16 bytes. */
  sendCost(length: number): number {
    return MuxCredit.costOf(length);
  }

  /**
   * The largest subframe {@link send} takes on this channel, or a negative
   * number when not even an empty one fits: half the initial window less
   * 16, within the frame limit the peer announced (header included).
   */
  get maxSubframeLength(): number {
    let max = this.#frameCostLimit - MuxCredit.frameOverhead;
    const peerMax = this.#host.peerMaxFrameSize();
    if (peerMax > 0) {
      const fits = peerMax - MuxFrame.headerSizeFor(this.id);
      if (fits < max) {
        max = fits;
      }
    }
    return max;
  }

  /** The largest cost of one DATA frame: half the initial window, but at least an empty subframe's when the window takes one. */
  get #frameCostLimit(): number {
    const half = Math.floor(this.#initialSendWindow / 2);
    if (half >= MuxCredit.frameOverhead) {
      return half;
    }
    return this.#initialSendWindow < MuxCredit.frameOverhead
      ? this.#initialSendWindow
      : MuxCredit.frameOverhead;
  }

  // Sending -----------------------------------------------------------------

  /**
   * Sends one DATA subframe, which may be empty: see
   * {@link MuxChannel.send}. When the window takes it and nothing waits
   * before it, it is handed over at once (the returned promise is already
   * resolved); otherwise it waits in order for credit. Handing over copies
   * it; the caller may reuse [subframe] once the promise resolves.
   *
   * On a bulk channel a non-empty [subframe] of any length goes out in
   * chunks of at most `MuxOptions.bulkChunkSize` (and the peer's frame
   * limit), each sized to the window at hand.
   *
   * @throws {SwitchboardError} FAILED_PRECONDITION if `!canSend`,
   *   FRAME_TOO_LARGE above {@link maxSubframeLength} on an ordinary
   *   channel (on a bulk one: when the peer leaves no room for a byte).
   */
  send(subframe: Uint8Array): Promise<void> {
    if (!this.canSend) {
      throw this.#cannotSend();
    }
    if (this.#priority === 'bulk' && subframe.length > 0) {
      this.#checkChunkable();
      const pending = new PendingSend(subframe, true);
      if (this.#pendingSends.length === 0 && this.#admitChunks(pending)) {
        return Promise.resolve();
      }
      this.#pendingSends.push(pending);
      return pending.done.promise;
    }
    this.#checkSize(subframe.length);
    const cost = MuxCredit.costOf(subframe.length);
    if (this.#pendingSends.length === 0 && cost <= this.#sendWindow) {
      this.#sendWindow -= cost;
      this.#reservedSendCost += cost;
      this.#host.sendChannelFrame(this.#link, MuxFrame.data(this.id, subframe));
      return Promise.resolve();
    }
    const pending = new PendingSend(subframe, false);
    this.#pendingSends.push(pending);
    return pending.done.promise;
  }

  /**
   * Whether a subframe of {@link maxSubframeLength} would be handed over
   * at once: the channel can send, nothing of it waits for credit, and the
   * window has room for it. False while the peer's LIMITS is awaited.
   */
  get isWritable(): boolean {
    return (
      this.canSend && this.#pendingSends.length === 0 && this.#sendWindow >= this.#writableCost
    );
  }

  /** The cost of the largest subframe {@link send} takes. */
  get #writableCost(): number {
    const max = this.maxSubframeLength;
    return MuxCredit.costOf(max < 0 ? 0 : max);
  }

  /**
   * Resolves once the channel {@link isWritable}: at once when it is.
   * Rejects with FAILED_PRECONDITION when the channel can no longer send;
   * never reported as unhandled.
   */
  get whenWritable(): Promise<void> {
    const waiter = new Deferred<void>();
    if (!this.canSend) {
      waiter.reject(this.#cannotSend());
    } else if (this.isWritable) {
      waiter.resolve();
    } else {
      this.#writableWaiters.push(waiter);
    }
    return waiter.promise;
  }

  #cannotSend(): SwitchboardError {
    return SwitchboardError.of(
      StatusCode.failedPrecondition,
      `channel ${this.id} is ${this.#state}`,
    );
  }

  /** Resolves the {@link whenWritable} waiters once writable, or rejects them once the channel can no longer send. */
  #wakeWritable(): void {
    if (this.#writableWaiters.length === 0 || (this.canSend && !this.isWritable)) {
      return;
    }
    const waiters = this.#writableWaiters;
    this.#writableWaiters = [];
    const error = this.canSend ? undefined : this.#cannotSend();
    for (const waiter of waiters) {
      if (error === undefined) {
        waiter.resolve();
      } else {
        waiter.reject(error);
      }
    }
  }

  /**
   * Opens a channel on the same connection whose OPEN keeps its place
   * among this channel's frames: after the subframes handed to
   * {@link send} before the call (those waiting for credit, and those the
   * output scheduler holds) and before the ones after it. DATA and the
   * CLOSE of the new channel wait for its OPEN. A GOAWAY, sent or
   * received, does not refuse it. If this channel stops sending before
   * the OPEN goes out, the new channel ends at once with CANCELLED.
   *
   * @throws {SwitchboardError} like `MuxConnection.open` (except after a
   *   GOAWAY), and FAILED_PRECONDITION when `!canSend`.
   */
  openAfter(openPayload: Uint8Array): MuxChannel {
    if (!this.canSend) {
      throw this.#cannotSend();
    }
    const link = this.#host.openAfter(this.#link, openPayload);
    const frame = MuxFrame.open(link.id, link.channel.openPayload);
    if (this.#pendingSends.length === 0) {
      this.#host.sendChannelFrame(this.#link, frame, link);
    } else {
      this.#pendingSends.push(new PendingSend(empty, false, [frame, link]));
    }
    return link.channel;
  }

  /** The largest chunk of a bulk payload: `bulkChunkSize`, within what the peer's frame limit leaves after the header. */
  get #bulkChunkMax(): number {
    let max = this.#host.options.bulkChunkSize;
    const peerMax = this.#host.peerMaxFrameSize();
    if (peerMax > 0) {
      const fits = peerMax - MuxFrame.headerSizeFor(this.id);
      if (fits < max) {
        max = fits;
      }
    }
    return max;
  }

  #checkChunkable(): void {
    if (this.#bulkChunkMax < 1) {
      throw SwitchboardError.of(
        StatusCode.frameTooLarge,
        `the peer frame limit of ${this.#host.peerMaxFrameSize()} leaves no room for a chunk ` +
          `on channel ${this.id}`,
      );
    }
    if (this.#initialSendWindow <= MuxCredit.frameOverhead) {
      throw SwitchboardError.of(
        StatusCode.frameTooLarge,
        `the initial window of ${this.#initialSendWindow} of channel ${this.id} leaves no ` +
          'room for a chunk',
      );
    }
  }

  /**
   * Hands over the chunks of [pending] the window takes; true once the
   * whole payload is handed over. A chunk waits until the window takes
   * the smallest of the rest, a whole chunk and what half the initial
   * window allows, so that chunks stay large.
   */
  #admitChunks(pending: PendingSend): boolean {
    const data = pending.subframe;
    const chunkMax = this.#bulkChunkMax;
    let floor = this.#frameCostLimit - MuxCredit.frameOverhead;
    if (floor > chunkMax) {
      floor = chunkMax;
    }
    if (floor < 1) {
      floor = 1;
    }
    while (pending.offset < data.length) {
      const rest = data.length - pending.offset;
      const room = this.#sendWindow - MuxCredit.frameOverhead;
      if (room < (rest < floor ? rest : floor)) {
        return false;
      }
      let n = rest < chunkMax ? rest : chunkMax;
      if (room < n) {
        n = room;
      }
      const start = pending.offset;
      // Accounted before the write, which could come back here.
      this.#sendWindow -= MuxCredit.costOf(n);
      this.#reservedSendCost += MuxCredit.costOf(n);
      pending.offset += n;
      this.#host.sendChannelFrame(
        this.#link,
        MuxFrame.data(this.id, data.subarray(start, start + n)),
      );
    }
    return true;
  }

  #checkSize(length: number): void {
    const max = this.#host.peerMaxFrameSize();
    const size = MuxFrame.headerSizeFor(this.id) + length;
    if (max > 0 && size > max) {
      throw SwitchboardError.of(
        StatusCode.frameTooLarge,
        `frame of ${size} bytes exceeds the peer limit of ${max}`,
      );
    }
    const cost = MuxCredit.costOf(length);
    const limit = this.#frameCostLimit;
    if (cost > limit) {
      throw SwitchboardError.of(
        StatusCode.frameTooLarge,
        `subframe of ${length} bytes costs ${cost}, more than ${limit}, half the initial ` +
          `window of channel ${this.id}`,
      );
    }
  }

  /** Hands the waiting subframes the window now takes to the connection, in order, then the CLOSE if it waited behind them. */
  #admitPending(): void {
    if (this.#admitting) {
      return;
    }
    this.#admitting = true;
    try {
      for (;;) {
        const head = this.#pendingSends.peek();
        if (head === undefined) {
          break;
        }
        const opens = head.opens;
        if (opens !== undefined && head.openFrame !== undefined) {
          // An OPEN ordered behind what came before it; not flow controlled.
          this.#pendingSends.shift();
          this.#host.sendChannelFrame(this.#link, head.openFrame, opens);
          head.done.resolve();
          continue;
        }
        try {
          // The peer may have lowered its frame limit meanwhile.
          if (head.split) {
            this.#checkChunkable();
            if (!this.#admitChunks(head)) {
              break;
            }
            this.#pendingSends.shift();
          } else {
            // Checked first: a subframe the window never takes would wait for ever.
            this.#checkSize(head.subframe.length);
            const cost = MuxCredit.costOf(head.subframe.length);
            if (cost > this.#sendWindow) {
              break;
            }
            this.#sendWindow -= cost;
            this.#reservedSendCost += cost;
            this.#pendingSends.shift();
            this.#host.sendChannelFrame(this.#link, MuxFrame.data(this.id, head.subframe));
          }
        } catch (e) {
          if (!(e instanceof SwitchboardError)) {
            throw e;
          }
          this.#pendingSends.shift();
          head.done.reject(e);
          continue;
        }
        head.done.resolve();
      }
    } finally {
      this.#admitting = false;
    }
    const close = this.#pendingClose;
    if (this.#pendingSends.length === 0 && close !== undefined) {
      this.#pendingClose = undefined;
      this.#queueClose(close);
    }
    this.#wakeWritable();
  }

  /** Fails every subframe still waiting for credit; a channel whose OPEN waited behind them is never opened. */
  #failPendingSends(): void {
    if (this.#pendingSends.length === 0) {
      return;
    }
    const error = SwitchboardError.of(
      StatusCode.failedPrecondition,
      `channel ${this.id} closed before the subframe was sent`,
    );
    const pending = [...this.#pendingSends];
    this.#pendingSends.clear();
    for (const send of pending) {
      send.done.reject(error);
      if (send.opens !== undefined) {
        this.#host.openFailed(
          send.opens,
          Status.of(
            StatusCode.cancelled,
            `channel ${this.id} stopped sending before the OPEN went out`,
          ),
        );
      }
    }
  }

  /** The wait for the peer's LIMITS ended: the window starts at [window], and what waited for it goes. */
  #onLimitsKnown(window: number): void {
    if (!this.#awaitingLimits) {
      return;
    }
    this.#awaitingLimits = false;
    this.#initialSendWindow = window;
    this.#sendWindow += window;
    if (this.#state !== 'closed') {
      this.#admitPending();
    }
    const known = this.#limitsKnown;
    this.#limitsKnown = undefined;
    known?.resolve();
    this.#wakeWritable();
  }

  /** Applies the peer's CREDIT of [bytes]; ignored once the channel can send no more DATA. */
  #receiveCredit(bytes: number): void {
    if (this.#closeReceived || this.#closeQueued || this.#state === 'closed') {
      return;
    }
    const window = this.#sendWindow + bytes;
    if (window + this.#reservedSendCost > MuxLimits.maxWindow) {
      throw new ProtocolError(
        `CREDIT of ${bytes} takes the window of channel ${this.id} to ${window}, plus ` +
          `${this.#reservedSendCost} reserved for unwritten DATA, above 2^32 - 1`,
      );
    }
    this.#sendWindow = window;
    this.#admitPending();
  }

  /** Sends the CLOSE carrying [status], behind the DATA handed over before it. */
  #queueClose(status: Status): void {
    this.#closeQueued = true;
    this.#host.sendChannelFrame(this.#link, this.#closeFrame(status));
  }

  /**
   * Sends the CLOSE now if it waits behind subframes waiting for credit,
   * failing them: the close confirmation timed out. An OK close then
   * carries DEADLINE_EXCEEDED, which {@link done} reports too.
   */
  #flushPendingClose(): void {
    let close = this.#pendingClose;
    if (close === undefined) {
      return;
    }
    this.#pendingClose = undefined;
    let bytes = 0;
    for (const pending of this.#pendingSends) {
      bytes += pending.subframe.length - pending.offset;
    }
    this.#failPendingSends();
    if (close.isOk) {
      close = Status.of(
        StatusCode.deadlineExceeded,
        `${bytes} bytes not sent: no credit within the close timeout`,
      );
      this.#noteStatus(close);
    }
    this.#queueClose(close);
  }

  /**
   * Sends CLOSE carrying [status] unless sent already, behind the
   * subframes still waiting for credit; see {@link MuxChannel.close}. The
   * reason is shortened on the wire to fit the peer's frame limit and 1024
   * bytes; `CONNECTION_LOST` goes as `UNAVAILABLE`.
   *
   * Resolves when the channel is mutually closed, the connection is lost,
   * or the peer did not confirm within `MuxOptions.closeConfirmTimeout`;
   * at once if the channel is closed already. Never rejects.
   *
   * @throws {RangeError} for an application status code (256 and above).
   */
  close(status: Status = Status.ok): Promise<void> {
    checkMuxStatus(status);
    if (this.#state === 'open') {
      this.#closeSent = true;
      this.#state = 'halfClosedLocal';
      this.#noteStatus(status);
      // A channel whose OPEN waits behind its parent's frames (openAfter)
      // arms the timer when the OPEN goes out: the peer cannot confirm a
      // channel it does not know.
      if (this.#link.openWritten) {
        this.#startConfirmTimer();
      }
      if (this.#pendingSends.length === 0) {
        this.#queueClose(status);
      } else {
        this.#pendingClose = status;
      }
      // Nothing more is sent: what waits for room fails.
      this.#wakeWritable();
    }
    return this.#done.promise.then(() => {});
  }

  /** {@link close} with OK, for `await using`. */
  [Symbol.asyncDispose](): Promise<void> {
    return this.close();
  }

  #closeFrame(status: Status): MuxFrame {
    return status.isOk && status.reason.length === 0
      ? MuxFrame.close(this.id)
      : MuxFrame.close(
          this.id,
          fitStatus(status, MuxFrame.headerSizeFor(this.id), this.#host.peerMaxFrameSize()),
        );
  }

  #noteStatus(status: Status): void {
    if (this.#endStatus.isOk && !status.isOk) {
      this.#endStatus = status;
    }
  }

  #startConfirmTimer(): void {
    const timeout = this.#host.options.closeConfirmTimeout;
    if (timeout <= 0 || this.#confirmTimer !== undefined) {
      return;
    }
    this.#confirmTimer = this.#host.options.clock.setTimeout(() => {
      this.#confirmTimer = undefined;
      if (this.#state === 'halfClosedLocal') {
        this.#flushPendingClose();
        this.#host.abandon(this.#link);
      }
    }, timeout);
  }

  /** The peer's CLOSE; see {@link MuxChannelLink.receiveClose}. */
  #receiveClose(status: Status): void {
    this.#closeReceived = true;
    this.#noteStatus(status);
    const confirm = !this.#closeSent;
    const ours = this.#pendingClose;
    this.#pendingClose = undefined;
    this.#failPendingSends();
    this.#closeSent = true;
    this.#state = 'closed';
    // Free the id before sending the confirmation, so that a peer reacting
    // to it at once may reuse the id.
    this.#host.release(this.#link);
    // DATA after the peer's CLOSE must not go out, and this side's CLOSE,
    // if it was queued behind such DATA, goes now.
    this.#host.dropQueuedData(this.#link);
    if (confirm) {
      this.#closeQueued = true;
      this.#host.sendFrame(MuxFrame.close(this.id));
    } else if (ours !== undefined) {
      this.#queueClose(ours);
    }
    this.#complete();
  }

  // Receiving ---------------------------------------------------------------

  /** Takes a DATA payload; a ProtocolError if it goes beyond the window granted to the peer. */
  #receive(payload: Uint8Array): void {
    const cost = MuxCredit.costOf(payload.length);
    if (cost > this.#receiveWindow) {
      throw new ProtocolError(
        `DATA costing ${cost} bytes on channel ${this.id}, beyond the window of ` +
          `${this.#receiveWindow} granted`,
      );
    }
    this.#receiveWindow -= cost;
    if (this.#discarding || this.#endRequested) {
      // Dropped: as good as consumed.
      this.#consume(cost);
      return;
    }
    this.#queue.push(payload);
    this.#queuedBytes += cost;
    this.#host.noteBuffered(cost);
    this.#drain();
  }

  /** Drops buffered subframes and every later one; they count as consumed. */
  #discard(): void {
    this.#discarding = true;
    const cost = this.#queuedBytes;
    this.#queue.clear();
    this.#queuedBytes = 0;
    if (cost !== 0) {
      this.#host.noteBuffered(-cost);
      this.#consume(cost);
    }
    const pull = this.#pull;
    this.#pull = undefined;
    pull?.resolve();
  }

  /** Whether credit may still go to the peer: it may still send DATA, and this side's CLOSE (which tells it to stop) has not gone out. */
  get #mayReturnCredit(): boolean {
    return !this.#released && !this.#closeReceived && !this.#closeQueued;
  }

  /** Counts [cost] as consumed and returns credit once half the window is consumed and unreturned. */
  #consume(cost: number): void {
    this.#unreturned += cost;
    if (this.#unreturned === 0 || !this.#mayReturnCredit) {
      return;
    }
    const half = Math.floor(this.#creditWindow / 2);
    if (this.#unreturned < (half < 1 ? 1 : half)) {
      return;
    }
    const credit = this.#unreturned;
    this.#unreturned = 0;
    this.#receiveWindow += credit;
    this.#host.sendFrame(MuxControlMessage.credit(new MuxCredit(this.id, credit)).toFrame());
  }

  /** `readable` wants a subframe. */
  #onPull(): Promise<void> | undefined {
    if (this.#deliver()) {
      return undefined;
    }
    this.#pull = new Deferred<void>();
    return this.#pull.promise;
  }

  /** Delivers what a waiting read asks for, or ends `readable` once everything was delivered. */
  #drain(): void {
    if (this.#pull !== undefined && this.#deliver()) {
      const pull = this.#pull;
      this.#pull = undefined;
      pull.resolve();
    } else if (this.#endRequested && this.#queue.length === 0) {
      this.#closeReadable();
    }
  }

  /** One subframe into `readable`, or its end; false when there is neither. */
  #deliver(): boolean {
    if (this.#readableClosed) {
      return true;
    }
    const payload = this.#queue.shift();
    if (payload !== undefined) {
      const cost = MuxCredit.costOf(payload.length);
      this.#queuedBytes -= cost;
      this.#host.noteBuffered(-cost);
      if (this.#manualCredit && !this.#released) {
        // Held by the reader until it reports it consumed.
        this.#held += cost;
      } else {
        this.#unreturned += cost;
      }
      (this.#controller as ReadableStreamDefaultController<Uint8Array>).enqueue(payload);
      this.#consume(0);
      if (this.#endRequested && this.#queue.length === 0) {
        this.#closeReadable();
      }
      return true;
    }
    if (this.#endRequested) {
      this.#closeReadable();
      return true;
    }
    return false;
  }

  /** Ends `readable`; one not made yet ends when it is, on its first read. */
  #closeReadable(): void {
    const controller = this.#controller;
    if (this.#readableClosed || controller === undefined) {
      return;
    }
    this.#readableClosed = true;
    controller.close();
  }

  #complete(): void {
    this.#state = 'closed';
    const known = this.#limitsKnown;
    this.#limitsKnown = undefined;
    known?.resolve();
    if (this.#confirmTimer !== undefined) {
      this.#host.options.clock.clearTimeout(this.#confirmTimer);
      this.#confirmTimer = undefined;
    }
    this.#pendingClose = undefined;
    this.#failPendingSends();
    if (!this.#link.openWritten) {
      // The OPEN never goes out: neither does what waited for it.
      this.#link.preOpen = [];
    }
    this.#reservedSendCost = 0;
    this.#released = true;
    this.#held = 0;
    this.#endRequested = true;
    this.#wakeWritable();
    this.#drain();
    this.#done.resolve(this.#endStatus);
  }

  /** `MuxChannel(3, open, local)`. */
  toString(): string {
    return `MuxChannel(${this.id}, ${this.#state}, ${this.isLocallyOpened ? 'local' : 'remote'})`;
  }
}
