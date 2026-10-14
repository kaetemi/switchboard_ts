/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { concatBytes } from '../bytes.ts';
import type {
  MuxChannel,
  MuxChannelState,
  MuxConnection,
  MuxOptions,
  MuxPriority,
} from '../mux/contract.ts';
import { ProtocolError, Status, StatusCode, SwitchboardError } from '../status.ts';
import type { TimerHandle } from '../util/clock.ts';
import { Deferred } from '../util/deferred.ts';
import type { LogLevel } from '../util/logger.ts';
import { attempt, isSessionEnd, withTimeout } from './binding-util.ts';
import {
  QuicError,
  type QuicSession,
  type QuicStream,
  QuicStreamReset,
  QuicStreamStopped,
} from './contract.ts';
import type { QuicBindingOptions } from './quic-options.ts';
import { RecordReader, RecordTooLarge } from './record-reader.ts';
import {
  decodeCloseBody,
  encodeCloseRecord,
  encodeOpenRecord,
  type QuicRecord,
  QuicRecordCommand,
  quicErrorCode,
  quicRecordHeaderSize,
  statusOfQuicErrorCode,
  writeQuicRecordHeader,
} from './records.ts';
import { checkMuxStatus } from './wire-status.ts';

/** The priority class of the control stream; see `QuicStream.priority`. */
export const quicControlPriority = 2;

/** The priority class of an ordinary channel's stream. */
export const quicOrdinaryPriority = 1;

/** The priority class of a bulk channel's stream. */
export const quicBulkPriority = 0;

/**
 * What a channel needs of its connection. The connection builds it over its
 * private state; not part of the package's API.
 *
 * @internal
 */
export interface QuicChannelHost {
  readonly session: QuicSession;
  readonly options: MuxOptions;
  readonly quicOptions: QuicBindingOptions;
  /** The connection is shutting down. */
  readonly closing: boolean;
  /** The largest record body the peer accepts. */
  readonly peerRecordLimit: number;
  /** The largest record body this side accepts. */
  readonly recordLimit: number;
  /** Completes when this side may open a stream; fails with the reason it cannot. */
  openGate(): Promise<void>;
  /** A channel of this side for [openPayload], registered under a new id. */
  openLocal(openPayload: Uint8Array, continuation: boolean): QuicChannelEntry;
  /** Stops and resets [stream], which carries no channel. */
  release(stream: QuicStream, code: number): void;
  /** A direction this side finished, whose acknowledgement a graceful close waits for. */
  trackFinishing(acknowledged: Promise<void>): void;
  protocolError(reason: string): void;
  fail(status: Status): void;
  /** A record arrived from the peer. */
  noteReceive(): void;
  /** [channel] ended: its id leaves the connection. */
  releaseChannel(channel: QuicBindingChannel): void;
  log(level: LogLevel, message: string, error?: unknown): void;
}

/**
 * What the connection calls on a channel; handed to it when the channel is
 * made.
 *
 * @internal
 */
export interface QuicChannelControl {
  /** The channel's stream; [openWritten] for the peer's channels, whose OPEN record was read. */
  attach(stream: QuicStream, reader: RecordReader, openWritten: boolean): void;
  /** Opens the stream of a channel of this side. */
  beginOpen(): Promise<void>;
  /** The open never reached the wire. */
  openFailed(status: Status): void;
  /** The connection ended: the channel ends with [status]. */
  connectionLost(status: Status): void;
  /** The largest subframe may have changed: wake writers. */
  wakeWritable(): void;
}

/**
 * A channel and its control.
 *
 * @internal
 */
export interface QuicChannelEntry {
  readonly channel: QuicBindingChannel;
  readonly control: QuicChannelControl;
}

/** A subframe (or a bulk payload) waiting for the stack, or the open of a child channel in order with them. */
interface OutItem {
  readonly data: Uint8Array;
  readonly split: boolean;
  readonly opens: QuicChannelEntry | undefined;
  offset: number;
  readonly done: Deferred<void>;
}

function outItem(data: Uint8Array, split: boolean, opens?: QuicChannelEntry): OutItem {
  return { data, split, opens, offset: 0, done: new Deferred<void>() };
}

const empty = new Uint8Array(0);

/** The status a `writable.abort(reason)` closes with. */
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

function header(command: QuicRecordCommand, length: number): Uint8Array {
  const out = new Uint8Array(quicRecordHeaderSize);
  writeQuicRecordHeader(out, 0, command, length);
  return out;
}

/**
 * A channel of a `QuicBindingConnection`: one bidirectional stream, as the
 * binding page specifies (sections "Channels" and "Flow control and
 * scheduling"), keeping the contract of {@link MuxChannel}. A port of
 * `QuicChannel` of `quic_channel.dart`; made by the connection.
 *
 * - The opener's direction starts with the OPEN record, written as the
 *   stream opens, together with whatever was sent meanwhile. Each subframe
 *   is a DATA record; records waiting on the stream go to the stack
 *   together, and {@link send} resolves once the stack has taken its record
 *   (exact backpressure: the stack waits for the peer's stream and
 *   connection windows).
 * - Close: a CLOSE record carrying the status payload, then FIN, after what
 *   was sent before; with `STOP_SENDING` carrying the status as well when
 *   the status is not `OK` (a rejection, a failure), so that the peer stops
 *   sending what would be discarded. The peer's close is its CLOSE record,
 *   its reset, or its `STOP_SENDING`, after which this side resets its own
 *   direction with the same code (the stack's echo) and keeps reading until
 *   the peer's direction ends: its CLOSE record, if it has one, carries the
 *   full status, which is the one reported. A close not answered within
 *   `MuxOptions.closeConfirmTimeout` sends `STOP_SENDING`
 *   `DEADLINE_EXCEEDED`, resets this side's direction if its CLOSE record
 *   is still queued behind data (an `OK` close becoming
 *   `DEADLINE_EXCEEDED`), and ends the channel with its first status.
 * - Flow control: the stack's stream window is the channel's window. The
 *   binding reads the next record only once the layer above has taken the
 *   previous one (from {@link readable}, or by {@link consumed} under
 *   {@link manualCredit}), so that unread data stays in the stack and a
 *   reader that stops stalls its own channel; {@link grant} lets it read
 *   that much further ahead, and so does an `OK` close of this side, by a
 *   window, so that the peer's CLOSE record is found behind data nobody
 *   takes. A subframe costs its record: its length plus the 4-byte header
 *   ({@link sendCost}). The stack's send window is not visible to the
 *   binding, so {@link sendWindow} is an estimate, which nothing here counts
 *   on.
 * - {@link maxSubframeLength} is the peer's record limit; a bulk channel's
 *   payload goes in records of at most the smaller of it and
 *   `MuxOptions.bulkChunkSize`.
 */
export class QuicBindingChannel implements MuxChannel {
  readonly connection: MuxConnection;
  readonly id: number;
  readonly isLocallyOpened: boolean;
  readonly openPayload: Uint8Array;

  /** Incoming subframes, one per chunk; see {@link MuxChannel.readable}. */
  readonly readable: ReadableStream<Uint8Array>;

  /** Outgoing subframes; see {@link MuxChannel.writable}. */
  readonly writable: WritableStream<Uint8Array>;

  readonly #host: QuicChannelHost;
  #stream: QuicStream | undefined;
  #reader: RecordReader | undefined;
  readonly #done = new Deferred<Status>();
  #state: MuxChannelState = 'open';
  #priority: MuxPriority = 'normal';
  #endStatus: Status = Status.ok;
  // Whether #endStatus came from an error code only (a reset or a
  // STOP_SENDING of the peer), so that the peer's CLOSE record replaces it.
  #endFromCode = false;
  #ended = false;

  // Sending.
  readonly #out: OutItem[] = [];
  #writing = false;
  #openWritten = false;
  #pendingClose: Status | undefined;
  // This side's close, once its CLOSE record went into a write.
  #closeInFlight: Status | undefined;
  // FIN went: the stack acknowledges the direction (QuicStream.stopped).
  #finWritten = false;
  // This side has closed: its CLOSE is queued or written, or it reset.
  #closeQueued = false;
  // This side's direction ended on the wire (CLOSE and FIN, or a reset).
  #sendEnded = false;
  #stopSent = false;
  #confirmTimer: TimerHandle | undefined;
  #openTimer: TimerHandle | undefined;
  #openingResult: Deferred<QuicStream | undefined> | undefined;
  // Settles once this side's direction is acknowledged, or will never be:
  // what a graceful close of the connection waits for.
  #acknowledged: Deferred<void> | undefined;
  #writableWaiters: Deferred<void>[] = [];

  // Receiving.
  // The peer's direction ended: its CLOSE record or its reset.
  #peerEnded = false;
  readonly #queue: Uint8Array[] = [];
  #queuedCost = 0;
  #held = 0;
  #readAhead = 0;
  #manualCredit = false;
  #consumedSignal: Deferred<void> | undefined;
  #discarding = false;
  #incomingEnded = false;
  #controller!: ReadableStreamDefaultController<Uint8Array>;
  #readableClosed = false;
  #pull: Deferred<void> | undefined;

  /**
   * Made by the connection, which receives the channel's control through
   * [bind]; not for applications.
   */
  constructor(
    connection: MuxConnection,
    host: QuicChannelHost,
    init: { id: number; isLocallyOpened: boolean; openPayload: Uint8Array },
    bind: (control: QuicChannelControl) => void,
  ) {
    this.connection = connection;
    this.#host = host;
    this.id = init.id;
    this.isLocallyOpened = init.isLocallyOpened;
    this.openPayload = init.openPayload;
    this.readable = new ReadableStream<Uint8Array>(
      {
        start: (controller) => {
          this.#controller = controller;
        },
        pull: () => this.#onPull(),
        cancel: () => this.#discard(),
      },
      { highWaterMark: 0 },
    );
    this.writable = new WritableStream<Uint8Array>({
      write: (chunk) => this.send(chunk),
      close: () => this.close(),
      abort: (reason: unknown) => this.close(abortStatus(reason)),
    });
    bind({
      attach: (stream, reader, openWritten) => this.#attach(stream, reader, openWritten),
      beginOpen: () => this.#beginOpen(),
      openFailed: (status) => this.#openFailed(status),
      connectionLost: (status) => this.#connectionLost(status),
      wakeWritable: () => this.#wakeWritable(),
    });
  }

  // The contract ------------------------------------------------------------

  get state(): MuxChannelState {
    return this.#state;
  }

  get canSend(): boolean {
    return this.#state === 'open';
  }

  get priority(): MuxPriority {
    return this.#priority;
  }

  set priority(value: MuxPriority) {
    this.#priority = value;
    const stream = this.#stream;
    if (stream !== undefined && !this.#sendEnded) {
      stream.priority = this.#priorityClass;
    }
  }

  get #priorityClass(): number {
    return this.#priority === 'bulk' ? quicBulkPriority : quicOrdinaryPriority;
  }

  get bufferedBytes(): number {
    return this.#queuedCost;
  }

  get hasListener(): boolean {
    return this.readable.locked && !this.#discarding;
  }

  get #unconsumed(): number {
    return this.#queuedCost + this.#held;
  }

  /**
   * The stream window this side grants (`MuxOptions.initialWindow`, as the
   * binding configures its stack), plus what {@link grant} added, less what
   * arrived and was not consumed yet; 0 once the peer's direction ended.
   */
  get receiveWindow(): number {
    if (this.#peerEnded || this.#ended) {
      return 0;
    }
    const window = this.#host.options.initialWindow + this.#readAhead - this.#unconsumed;
    return window < 0 ? 0 : window;
  }

  get manualCredit(): boolean {
    return this.#manualCredit;
  }

  set manualCredit(value: boolean) {
    if (value === this.#manualCredit) {
      return;
    }
    this.#manualCredit = value;
    if (!value && this.#held > 0) {
      this.#held = 0;
      this.#signalConsumed();
    }
  }

  get heldBytes(): number {
    return this.#held;
  }

  consumed(bytes: number, subframes = 1): void {
    if (bytes < 0 || subframes < 0) {
      throw new RangeError(`negative bytes (${bytes}) or subframes (${subframes})`);
    }
    if (this.#ended) {
      return;
    }
    const cost = bytes + quicRecordHeaderSize * subframes;
    if (cost > this.#held) {
      throw new Error(`${cost} credit reported consumed, ${this.#held} held`);
    }
    this.#held -= cost;
    this.#signalConsumed();
  }

  /**
   * Lets the binding read [bytes] further ahead of what the layer above
   * consumed, which returns as much window to the peer through the stack.
   */
  grant(bytes: number): void {
    if (!(bytes > 0)) {
      throw new RangeError(`granted bytes must be positive, not ${bytes}`);
    }
    if (this.#peerEnded || this.#ended) {
      return;
    }
    if (this.#host.options.initialWindow + this.#readAhead + bytes > 0xffffffff) {
      throw new RangeError(`a window of more than 2^32 - 1 after granting ${bytes}`);
    }
    this.#readAhead += bytes;
    this.#signalConsumed();
  }

  get done(): Promise<Status> {
    return this.#done.promise;
  }

  /**
   * An estimate, as {@link MuxChannel.sendWindow} has it for a binding whose
   * streams the stack flow-controls: room for one largest subframe while the
   * channel {@link isWritable}, else 0.
   */
  get sendWindow(): number {
    return this.isWritable ? this.sendCost(this.maxSubframeLength) : 0;
  }

  /** A DATA record: the subframe and its 4-byte header. */
  sendCost(length: number): number {
    return length + quicRecordHeaderSize;
  }

  /** The peer's record limit: 32752 until its `HELLO` raises it. */
  get maxSubframeLength(): number {
    return this.#host.peerRecordLimit;
  }

  send(subframe: Uint8Array): Promise<void> {
    if (!this.canSend) {
      throw this.#cannotSend();
    }
    const split = this.#priority === 'bulk' && subframe.length > 0;
    if (!split && subframe.length > this.maxSubframeLength) {
      throw SwitchboardError.of(
        StatusCode.frameTooLarge,
        `subframe of ${subframe.length} bytes, at most ${this.maxSubframeLength}`,
      );
    }
    const item = outItem(subframe, split);
    this.#out.push(item);
    this.#pump();
    return item.done.promise;
  }

  /**
   * Whether a subframe would be handed to the stack at once: the stream is
   * open with this side's OPEN record written, and nothing of the channel
   * waits for the stack.
   */
  get isWritable(): boolean {
    return (
      this.canSend &&
      this.#stream !== undefined &&
      this.#openWritten &&
      !this.#writing &&
      this.#out.length === 0
    );
  }

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

  #wakeWritable(): void {
    if (this.#writableWaiters.length === 0 || (this.canSend && !this.isWritable)) {
      return;
    }
    const waiters = this.#writableWaiters;
    this.#writableWaiters = [];
    for (const waiter of waiters) {
      if (this.canSend) {
        waiter.resolve();
      } else {
        waiter.reject(this.#cannotSend());
      }
    }
  }

  get awaitingLimits(): boolean {
    return false;
  }

  get limitsKnown(): Promise<void> {
    return Promise.resolve();
  }

  /**
   * Opens a channel on the connection whose stream is opened once what this
   * channel was given before the call has been handed to the stack: the
   * peer may still receive it first, since streams are not ordered
   * (`hasOrderedChannels` is false).
   */
  openAfter(openPayload: Uint8Array): MuxChannel {
    if (!this.canSend) {
      throw this.#cannotSend();
    }
    const child = this.#host.openLocal(openPayload, true);
    if (
      this.#out.length === 0 &&
      !this.#writing &&
      this.#openWritten &&
      this.#stream !== undefined
    ) {
      void child.control.beginOpen();
    } else {
      this.#out.push(outItem(empty, false, child));
    }
    return child.channel;
  }

  close(status: Status = Status.ok): Promise<void> {
    checkMuxStatus(status);
    if (this.#state === 'open') {
      this.#state = 'halfClosedLocal';
      this.#closeQueued = true;
      this.#noteStatus(status);
      this.#pendingClose = status;
      this.#trackAcknowledgement();
      this.#startConfirmTimer();
      if (!status.isOk) {
        // A rejection or a failure: this side will not read what is still
        // coming, and the channel has ended for it, the peer's direction
        // being reset. Its CLOSE record still follows what it sent.
        this.#stopReceiving(quicErrorCode(status));
      } else if (!this.#peerEnded) {
        // What the peer sends before it sees the close is still delivered;
        // reading up to a window ahead finds its CLOSE record behind data
        // nobody takes, as the frame mux would.
        this.#readAhead += this.#host.options.initialWindow;
        this.#signalConsumed();
      }
      this.#pump();
      this.#wakeWritable();
      this.#checkEnd();
    }
    return this.#done.promise.then(() => {});
  }

  [Symbol.asyncDispose](): Promise<void> {
    return this.close();
  }

  // Opening and attaching -----------------------------------------------------

  /**
   * Opens this channel's stream (a channel of this side), within the
   * connection's gate and `QuicBindingOptions.openStreamTimeout`.
   */
  async #beginOpen(): Promise<void> {
    const host = this.#host;
    try {
      await host.openGate();
    } catch (error) {
      if (error instanceof SwitchboardError) {
        this.#openFailed(error.status);
        return;
      }
      throw error;
    }
    if (this.#ended) {
      this.#neverWritten();
      return;
    }
    const result = new Deferred<QuicStream | undefined>();
    this.#openingResult = result;
    this.#watch(
      attempt(() => host.session.openStream()).then(
        (stream) => {
          if (!result.resolve(stream)) {
            // Given up on meanwhile.
            host.release(stream, StatusCode.cancelled);
          }
        },
        (error: unknown) => {
          result.reject(error);
        },
      ),
    );
    const timeout = host.quicOptions.openStreamTimeout;
    const clock = host.options.clock;
    const timer =
      timeout > 0 ? clock.setTimeout(() => result.resolve(undefined), timeout) : undefined;
    this.#openTimer = timer;
    let stream: QuicStream | undefined;
    try {
      stream = await result.promise;
    } catch (error) {
      if (timer !== undefined) {
        clock.clearTimeout(timer);
      }
      host.log('debug', `${this}: no stream: ${error}`);
      if (isSessionEnd(error)) {
        // The session's end determines whether this was an application close.
        return;
      }
      this.#openFailed(
        Status.of(
          error instanceof QuicError && error.kind === 'streamLimit'
            ? StatusCode.resourceExhausted
            : StatusCode.internal,
          'stream open failed',
        ),
      );
      return;
    }
    if (timer !== undefined) {
      clock.clearTimeout(timer);
    }
    this.#openTimer = undefined;
    this.#openingResult = undefined;
    if (stream === undefined) {
      this.#openFailed(
        Status.of(
          StatusCode.resourceExhausted,
          `no stream credit from the peer within ${timeout} ms`,
        ),
      );
      return;
    }
    if (this.#ended) {
      host.release(stream, StatusCode.cancelled);
      this.#neverWritten();
      return;
    }
    this.#attach(stream, new RecordReader(stream), false);
  }

  /**
   * The channel ended before its stream opened (closed with a failure, or
   * the confirmation timed out): nothing of it will be written.
   */
  #neverWritten(): void {
    this.#sendEnded = true;
    this.#pendingClose = undefined;
    this.#dropOut();
    this.#acknowledge();
  }

  #attach(stream: QuicStream, reader: RecordReader, openWritten: boolean): void {
    this.#stream = stream;
    this.#reader = reader;
    this.#openWritten = openWritten;
    stream.priority = this.#priorityClass;
    this.#watch(
      stream.resetReceived.then(
        (code) => {
          if (!this.#peerEnded && !this.#ended && !this.#stopSent) {
            this.#peerReset(code);
          }
        },
        (error: unknown) => {
          if (!isSessionEnd(error) && !this.#ended) {
            this.#abort(Status.of(StatusCode.internal, 'reset notification failed'));
          }
        },
      ),
    );
    this.#watch(
      stream.stopped.then(
        (code) => {
          if (code !== undefined) {
            this.#onStopSending(code);
          }
        },
        (error: unknown) => {
          if (error instanceof QuicStreamStopped) {
            this.#onStopSending(error.code);
          } else if (!isSessionEnd(error) && !this.#ended) {
            this.#abort(Status.of(StatusCode.internal, 'stream acknowledgement failed'));
          }
        },
      ),
    );
    this.#watch(this.#readLoop());
    if (this.#stopSent) {
      // Closed with a failure before the stream existed.
      stream.stop(quicErrorCode(this.#endStatus));
    }
    this.#pump();
    this.#wakeWritable();
  }

  /** Reports a failure of an internal continuation instead of leaving it unhandled. */
  #watch(promise: Promise<unknown>): void {
    promise.catch((error: unknown) => {
      this.#host.log('error', `${this}: internal error`, error);
    });
  }

  /**
   * The open never reached the wire: the channel ends locally with
   * [status]; the peer never heard of it.
   */
  #openFailed(status: Status): void {
    if (this.#ended) {
      return;
    }
    this.#noteStatus(status);
    this.#sendEnded = true;
    this.#peerEnded = true;
    this.#closeQueued = true;
    this.#pendingClose = undefined;
    this.#dropOut();
    this.#acknowledge();
    this.#endIncoming();
    this.#end();
  }

  // Sending -------------------------------------------------------------------

  /**
   * Hands what waits to the stack, one write at a time: the OPEN record
   * first, then the queued records, then the CLOSE record and FIN.
   */
  #pump(): void {
    const stream = this.#stream;
    if (this.#writing || stream === undefined || this.#sendEnded) {
      return;
    }
    const close = this.#pendingClose;
    if (this.#openWritten && this.#out.length === 0 && close === undefined) {
      this.#wakeWritable();
      return;
    }
    const batch: Uint8Array[] = [];
    let batchLength = 0;
    const add = (part: Uint8Array): void => {
      batch.push(part);
      batchLength += part.length;
    };
    const completed: Deferred<void>[] = [];
    if (!this.#openWritten) {
      add(encodeOpenRecord(this.id, this.openPayload));
    }
    const limit = this.#host.quicOptions.writeBatchBytes;
    while (this.#out.length > 0 && batchLength < limit) {
      const head = this.#out[0] as OutItem;
      const child = head.opens;
      if (child !== undefined) {
        if (batchLength > 0) {
          // Opened once what came before it was taken.
          break;
        }
        this.#out.shift();
        void child.control.beginOpen();
        continue;
      }
      if (head.split) {
        let chunk = this.#host.options.bulkChunkSize;
        const peerLimit = this.maxSubframeLength;
        if (peerLimit < chunk) {
          chunk = peerLimit;
        }
        if (chunk < 1) {
          chunk = 1;
        }
        while (head.offset < head.data.length && batchLength < limit) {
          const rest = head.data.length - head.offset;
          const n = rest < chunk ? rest : chunk;
          add(header(QuicRecordCommand.data, n));
          add(head.data.subarray(head.offset, head.offset + n));
          head.offset += n;
        }
        if (head.offset < head.data.length) {
          break;
        }
      } else {
        add(header(QuicRecordCommand.data, head.data.length));
        add(head.data);
      }
      this.#out.shift();
      completed.push(head.done);
    }
    let finishing = false;
    if (this.#out.length === 0 && close !== undefined) {
      this.#pendingClose = undefined;
      this.#closeInFlight = close;
      add(encodeCloseRecord(close));
      finishing = true;
    }
    if (batch.length === 0) {
      // Only child opens were waiting.
      this.#wakeWritable();
      return;
    }
    this.#openWritten = true;
    this.#writing = true;
    const bytes = batch.length === 1 ? (batch[0] as Uint8Array) : concatBytes(...batch);
    attempt(() => stream.write(bytes))
      .then(
        () => {
          this.#writing = false;
          for (const done of completed) {
            done.resolve();
          }
          if (finishing) {
            this.#finishDirection(stream);
          }
          this.#pump();
          this.#wakeWritable();
        },
        (error: unknown) => {
          this.#writing = false;
          const failure = SwitchboardError.of(
            StatusCode.failedPrecondition,
            `channel ${this.id} can no longer send`,
          );
          for (const done of completed) {
            done.reject(failure);
          }
          if (this.#sendEnded) {
            // This side reset its direction meanwhile.
            this.#wakeWritable();
            return;
          }
          if (error instanceof QuicStreamStopped) {
            this.#onStopSending(error.code);
          } else if (isSessionEnd(error)) {
            // The channel ends with the connection.
            this.#sendEnded = true;
            this.#closeQueued = true;
            this.#pendingClose = undefined;
            this.#dropOut();
            this.#acknowledge();
          } else {
            this.#host.log('warn', `${this}: write failed: ${error}`);
            this.#abort(Status.of(StatusCode.internal, 'write failed'));
          }
          this.#wakeWritable();
        },
      )
      .catch((error: unknown) => {
        this.#host.log('error', `${this}: after a write`, error);
      });
  }

  /** The CLOSE record was taken: FIN ends this side's direction. */
  #finishDirection(stream: QuicStream): void {
    this.#watch(
      attempt(() => stream.finish()).then(
        () => {
          this.#sendEnded = true;
          this.#finWritten = true;
          if (this.#ended) {
            this.#clearConfirmTimer();
          }
          const acknowledge = (): void => this.#acknowledge();
          stream.stopped.then(acknowledge, acknowledge);
          this.#checkEnd();
        },
        (error: unknown) => {
          if (error instanceof QuicStreamStopped) {
            this.#onStopSending(error.code);
          } else if (!isSessionEnd(error)) {
            this.#abort(Status.of(StatusCode.internal, 'FIN failed'));
          }
        },
      ),
    );
  }

  /**
   * This side closed: a graceful close of the connection waits until its
   * direction is acknowledged (or never will be).
   */
  #trackAcknowledgement(): void {
    if (this.#acknowledged !== undefined) {
      return;
    }
    const acknowledged = new Deferred<void>();
    this.#acknowledged = acknowledged;
    this.#host.trackFinishing(acknowledged.promise);
  }

  #acknowledge(): void {
    this.#acknowledged?.resolve();
  }

  /**
   * Ends this side's direction with a reset carrying [code]: what was not
   * delivered is discarded.
   */
  #resetDirection(code: number): void {
    if (this.#sendEnded) {
      return;
    }
    this.#sendEnded = true;
    this.#closeQueued = true;
    this.#pendingClose = undefined;
    this.#dropOut();
    this.#stream?.reset(code);
    if (this.#ended) {
      this.#clearConfirmTimer();
    }
    this.#acknowledge();
  }

  /**
   * Sends `STOP_SENDING` with [code] once: this side reads no more of the
   * peer's direction, which the peer resets. What arrived and waits for the
   * reader is still delivered.
   */
  #stopReceiving(code: number): void {
    if (this.#stopSent || this.#peerEnded) {
      return;
    }
    this.#stopSent = true;
    this.#stream?.stop(code);
  }

  /**
   * Fails what waits to be sent, and never opens the channels of
   * {@link openAfter} still waiting for their turn.
   */
  #dropOut(): void {
    if (this.#out.length === 0) {
      return;
    }
    const failure = SwitchboardError.of(
      StatusCode.failedPrecondition,
      `channel ${this.id} closed before the subframe was sent`,
    );
    const items = this.#out.splice(0);
    for (const item of items) {
      const child = item.opens;
      if (child !== undefined) {
        child.control.openFailed(Status.of(StatusCode.cancelled, 'the open never went out'));
      } else {
        item.done.reject(failure);
      }
    }
  }

  // Closing -------------------------------------------------------------------

  #noteStatus(status: Status, fromCode = false): void {
    if (this.#endStatus.isOk && !status.isOk) {
      this.#endStatus = status;
      this.#endFromCode = fromCode;
    }
  }

  /**
   * The peer's close arrived before this side closed: close in turn, at
   * once, dropping what was not sent yet.
   */
  #closeInTurn(): void {
    if (this.#writing && !this.#sendEnded) {
      // An in-flight write can be stalled at the peer's window. A reset
      // confirms now and abandons that write, including a partial record.
      this.#resetDirection(quicErrorCode(this.#pendingClose ?? this.#closeInFlight ?? Status.ok));
    }
    if (this.#state === 'open') {
      this.#dropOut();
      this.#closeQueued = true;
      if (!this.#sendEnded) {
        this.#pendingClose = Status.ok;
        this.#trackAcknowledgement();
      }
    } else if (!this.#sendEnded && this.#pendingClose !== undefined) {
      // Ours waited behind data: it goes at once.
      this.#dropOut();
    }
    this.#state = 'closed';
    if (!this.#sendEnded) {
      this.#startConfirmTimer();
    }
    this.#pump();
    this.#wakeWritable();
  }

  /**
   * The peer's `STOP_SENDING`: its close, arriving early. This side stops
   * sending at once, resetting its direction with the same code, and keeps
   * reading until the peer's direction ends.
   */
  #onStopSending(code: number | undefined): void {
    if (this.#host.closing) {
      return;
    }
    const status = statusOfQuicErrorCode(code);
    if (this.#ended) {
      // Ended for this side already; what it still had to send goes no
      // further.
      this.#resetDirection(code ?? quicErrorCode(status));
      return;
    }
    if (this.#peerEnded) {
      // The peer's direction ended first (its CLOSE record, say), and its
      // STOP_SENDING now ends this side's close in turn, which the peer gave
      // up waiting for (its confirmation timeout): this side's direction
      // ends here (the stack reset it with the code), and with it the
      // channel, instead of waiting for its own timeout.
      this.#noteStatus(status);
      this.#resetDirection(code ?? quicErrorCode(status));
      this.#wakeWritable();
      this.#checkEnd();
      return;
    }
    this.#noteStatus(status, true);
    this.#resetDirection(code ?? quicErrorCode(status));
    if (this.#state === 'open') {
      this.#state = 'halfClosedLocal';
    }
    this.#startConfirmTimer();
    this.#wakeWritable();
    this.#checkEnd();
  }

  #onPeerCloseRecord(status: Status): void {
    this.#peerEnded = true;
    if (this.#endFromCode && this.#endStatus.code === status.code) {
      // The same close, known first by its code: the record carries the
      // reason and the fields.
      this.#endStatus = status;
      this.#endFromCode = false;
    } else {
      this.#noteStatus(status);
    }
    this.#endIncoming();
    this.#closeInTurn();
    this.#checkEnd();
  }

  #peerReset(code: number | undefined): void {
    if (this.#ended || this.#peerEnded) {
      return;
    }
    if (this.#host.closing) {
      // The channel ends as the connection's channels do.
      return;
    }
    this.#peerEnded = true;
    if (!this.#stopSent) {
      this.#noteStatus(statusOfQuicErrorCode(code), true);
    }
    // After this side's STOP_SENDING the reset is its confirmation, whatever
    // the code (a browser cannot choose it).
    this.#endIncoming();
    this.#closeInTurn();
    this.#checkEnd();
  }

  /**
   * Bounds the wait for the peer's close, and, once the channel ended, the
   * write of this side's CLOSE record behind what it sent.
   */
  #startConfirmTimer(): void {
    const timeout = this.#host.options.closeConfirmTimeout;
    if (!(timeout > 0) || this.#confirmTimer !== undefined) {
      return;
    }
    this.#confirmTimer = this.#host.options.clock.setTimeout(
      () => this.#onConfirmTimeout(),
      timeout,
    );
  }

  #clearConfirmTimer(): void {
    const timer = this.#confirmTimer;
    this.#confirmTimer = undefined;
    if (timer !== undefined) {
      this.#host.options.clock.clearTimeout(timer);
    }
  }

  #onConfirmTimeout(): void {
    this.#confirmTimer = undefined;
    const deadline = StatusCode.deadlineExceeded;
    if (this.#ended) {
      if (!this.#sendEnded) {
        // The CLOSE record is still queued behind data the peer does not
        // read: the direction is reset instead.
        const close = this.#pendingClose ?? this.#closeInFlight;
        this.#resetDirection(close === undefined || close.isOk ? deadline : quicErrorCode(close));
      }
      return;
    }
    this.#host.log(
      'debug',
      `${this}: no answer to the close within ${this.#host.options.closeConfirmTimeout} ms`,
    );
    if (!this.#peerEnded) {
      // Forces the peer to reset its direction.
      this.#stopReceiving(deadline);
    }
    if (!this.#sendEnded) {
      // The CLOSE record is still queued behind data the peer does not read.
      const close = this.#pendingClose ?? this.#closeInFlight;
      if (close === undefined || close.isOk) {
        this.#noteStatus(Status.of(StatusCode.deadlineExceeded));
        this.#resetDirection(deadline);
      } else {
        this.#resetDirection(quicErrorCode(close));
      }
    }
    this.#peerEnded = true;
    this.#endIncoming();
    this.#end();
  }

  /**
   * Ends the channel when both directions have ended: the peer's received
   * (or stopped by this side's `STOP_SENDING`), this side's sent (or queued
   * behind what it sent).
   */
  #checkEnd(): void {
    if (
      !this.#ended &&
      this.#sendEnded &&
      this.#closeQueued &&
      (this.#peerEnded || this.#stopSent)
    ) {
      this.#end();
    }
  }

  /**
   * A local failure of the stream: both directions are given up (reset and
   * `STOP_SENDING`), and the channel ends with [status] unless it ended
   * already.
   */
  #abort(status: Status): void {
    if (!this.#ended) {
      this.#noteStatus(status);
    }
    const code = quicErrorCode(status);
    this.#stopReceiving(code);
    this.#resetDirection(code);
    this.#peerEnded = true;
    this.#endIncoming();
    this.#end();
  }

  #connectionLost(status: Status): void {
    this.#noteStatus(status);
    this.#pendingClose = undefined;
    this.#sendEnded = true;
    this.#closeQueued = true;
    this.#peerEnded = true;
    this.#dropOut();
    if (!this.#finWritten) {
      // Nothing more goes; a FIN written is acknowledged (or fails with the
      // session) on its own, which a graceful close waits for.
      this.#acknowledge();
    }
    this.#endIncoming();
    this.#end();
  }

  #end(): void {
    if (this.#ended) {
      return;
    }
    this.#ended = true;
    const clock = this.#host.options.clock;
    if (this.#openTimer !== undefined) {
      clock.clearTimeout(this.#openTimer);
      this.#openTimer = undefined;
    }
    const opening = this.#openingResult;
    this.#openingResult = undefined;
    opening?.resolve(undefined);
    this.#state = 'closed';
    if (this.#sendEnded) {
      this.#clearConfirmTimer();
    } else {
      // What this side queued before its CLOSE record still goes, within
      // the confirmation timeout.
      this.#startConfirmTimer();
    }
    this.#held = 0;
    this.#wakeWritable();
    this.#signalConsumed();
    this.#endIncoming();
    this.#host.releaseChannel(this);
    this.#done.resolve(this.#endStatus);
  }

  // Receiving -----------------------------------------------------------------

  async #readLoop(): Promise<void> {
    const reader = this.#reader as RecordReader;
    const host = this.#host;
    for (;;) {
      while (
        !this.#ended &&
        !this.#peerEnded &&
        !this.#stopSent &&
        this.#unconsumed > 0 &&
        this.#unconsumed >= this.#readAhead
      ) {
        this.#consumedSignal ??= new Deferred<void>();
        await this.#consumedSignal.promise;
      }
      if (this.#peerEnded || this.#stopSent || host.closing) {
        return;
      }
      let record: QuicRecord | undefined;
      try {
        record = await reader.next(host.recordLimit);
      } catch (error) {
        if (this.#stopSent || this.#peerEnded || host.closing) {
          // This side stopped reading meanwhile, or the connection ends.
          return;
        }
        this.#readFailed(error);
        return;
      }
      if (this.#stopSent || this.#peerEnded || host.closing) {
        return;
      }
      if (record === undefined) {
        host.protocolError(`FIN without a CLOSE record on channel ${this.id}`);
        return;
      }
      host.noteReceive();
      switch (record.command) {
        case QuicRecordCommand.data:
          this.#onData(record.body);
          break;
        case QuicRecordCommand.open:
          host.protocolError(`a second OPEN record on channel ${this.id}`);
          return;
        case QuicRecordCommand.close: {
          let status: Status;
          try {
            status = decodeCloseBody(record.body);
          } catch (error) {
            if (error instanceof ProtocolError) {
              host.fail(error.status);
              return;
            }
            throw error;
          }
          this.#onPeerCloseRecord(status);
          const ending = reader.expectEnd();
          const timeout = host.options.closeConfirmTimeout;
          // CLOSE ends the application channel, but its stream must not keep
          // a read and stream credit forever if FIN never follows.
          const finished = withTimeout(host.options.clock, ending, timeout, () => {
            this.#stream?.stop(StatusCode.deadlineExceeded);
          });
          host.trackFinishing(finished);
          try {
            await finished;
          } catch (error) {
            if (error instanceof ProtocolError) {
              host.fail(error.status);
            }
            // Otherwise the session ended meanwhile.
          }
          return;
        }
      }
    }
  }

  /** Reading the peer's direction failed with [failure]. */
  #readFailed(failure: unknown): void {
    if (failure instanceof QuicStreamReset) {
      if (!this.#peerEnded && !this.#ended && !this.#stopSent) {
        this.#peerReset(failure.code);
      }
    } else if (failure instanceof RecordTooLarge) {
      this.#host.fail(
        Status.of(
          StatusCode.frameTooLarge,
          `record of ${failure.length} bytes on channel ${this.id}, limit ${failure.limit}`,
        ),
      );
    } else if (failure instanceof ProtocolError) {
      this.#host.fail(failure.status);
    } else if (!isSessionEnd(failure)) {
      this.#host.log('warn', `${this}: reading failed: ${failure}`);
      this.#abort(Status.of(StatusCode.internal, 'read failed'));
    }
    // A session end: the channel ends with the connection.
  }

  #onData(subframe: Uint8Array): void {
    if (this.#discarding || this.#stopSent || this.#ended) {
      return;
    }
    this.#queue.push(subframe);
    this.#queuedCost += this.sendCost(subframe.length);
    this.#drain();
  }

  #signalConsumed(): void {
    const signal = this.#consumedSignal;
    this.#consumedSignal = undefined;
    signal?.resolve();
  }

  /** The reader cancelled `readable`: what arrives is dropped. */
  #discard(): void {
    this.#discarding = true;
    this.#readableClosed = true;
    this.#queue.length = 0;
    if (this.#queuedCost > 0) {
      this.#queuedCost = 0;
      this.#signalConsumed();
    }
    const pull = this.#pull;
    this.#pull = undefined;
    pull?.resolve();
  }

  #endIncoming(): void {
    this.#incomingEnded = true;
    this.#drain();
  }

  /** `readable` wants a chunk. */
  #onPull(): Promise<void> | undefined {
    if (this.#deliver()) {
      return undefined;
    }
    this.#pull = new Deferred<void>();
    return this.#pull.promise;
  }

  /** Delivers what a pending pull waits for, or the end. */
  #drain(): void {
    if (this.#pull !== undefined && this.#deliver()) {
      const pull = this.#pull;
      this.#pull = undefined;
      pull.resolve();
    } else if (this.#incomingEnded && this.#queue.length === 0) {
      this.#closeReadable();
    }
  }

  /** One subframe into `readable`, or its end; false when there is neither. */
  #deliver(): boolean {
    if (this.#readableClosed) {
      return true;
    }
    const data = this.#queue.shift();
    if (data !== undefined) {
      const cost = this.sendCost(data.length);
      this.#queuedCost -= cost;
      if (this.#manualCredit && !this.#ended) {
        this.#held += cost;
      }
      this.#controller.enqueue(data);
      this.#signalConsumed();
      if (this.#incomingEnded && this.#queue.length === 0) {
        this.#closeReadable();
      }
      return true;
    }
    if (this.#incomingEnded) {
      this.#closeReadable();
      return true;
    }
    return false;
  }

  #closeReadable(): void {
    if (this.#readableClosed) {
      return;
    }
    this.#readableClosed = true;
    this.#controller.close();
  }

  toString(): string {
    return `QuicBindingChannel(${this.id}, ${this.#state}, ${this.isLocallyOpened ? 'local' : 'remote'})`;
  }
}
