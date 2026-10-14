/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * The stream binding of the transport layer over any ordered byte
 * connection: an 8-byte preamble, then frames prefixed with a `u32`
 * little-endian length. Ports `stream_transport.dart` and the machinery of
 * `framed_byte_transport.dart`. Used for TCP, TLS, Unix domain sockets,
 * serial lines, a QUIC stream or a Deno connection. See the wiki page
 * "Polyverse Switchboard Transport".
 */

import { concatBytes } from '../bytes.ts';
import { StatusCode, SwitchboardError } from '../status.ts';
import { type Clock, systemClock, type TimerHandle } from '../util/clock.ts';
import { Deferred } from '../util/deferred.ts';
import { type Logger, type LogLevel, noopLogger } from '../util/logger.ts';
import { type ByteInput, type ByteOutput, webByteInput, webByteOutput } from './byte-io.ts';
import { defaultMaxFrameSize, type FrameTransport } from './contract.ts';
import { Fifo } from './fifo.ts';
import { StreamFrameDecoder, StreamFraming } from './stream-framing.ts';

/** Options of {@link streamTransport} and {@link StreamTransport}. */
export interface StreamTransportOptions {
  /** Largest accepted incoming frame body; 0 means no limit. Default 1 MiB. */
  readonly maxFrameSize?: number;
  /**
   * Bytes waiting for the output above which the transport stops reading
   * its input, until they drop to half of it. 0 means never. Default 16 MiB.
   */
  readonly outputHighWaterMark?: number;
  /**
   * How long, in milliseconds, closing the output (and, with
   * {@link linger}, waiting for the peer to close its side) may take before
   * the transport aborts. Default 10 000.
   */
  readonly closeTimeout?: number;
  /**
   * Called when the transport aborts, after it has dropped its output and
   * torn down the byte streams; destroys the underlying connection when
   * that needs more (a socket handle, say).
   */
  readonly abort?: () => void;
  /**
   * Whether the input is still read, and discarded, after the transport
   * closes while the peer may be sending, until the peer closes its side or
   * {@link closeTimeout} expires (then the transport aborts). Closing a TCP
   * socket with unread input makes the kernel reset the connection, and a
   * reset can destroy the last frames still in flight to the peer, such as
   * a GOAWAY. Default false; the TCP transport sets it.
   */
  readonly linger?: boolean;
  /** The peer, for logs and `FrameTransport.remote`. */
  readonly remote?: string;
  /** Source of the close and linger timers. Default {@link systemClock}. */
  readonly clock?: Clock;
  /** Receives the transport's diagnostics, as `Switchboard.Transport`. */
  readonly logger?: Logger;
}

const loggerName = 'Switchboard.Transport';

/** Largest chunk assembled from queued writes when the output takes more. */
const coalesceLimit = 64 * 1024;

function checkNonNegativeInteger(value: number, name: string): number {
  if (!Number.isInteger(value) || value < 0) {
    throw new RangeError(`${name} must be a non-negative integer, got ${value}`);
  }
  return value;
}

/**
 * A stream binding transport over a {@link ByteInput} and a
 * {@link ByteOutput}. {@link streamTransport} makes one over Web Streams;
 * the Node TCP transport makes one over a socket.
 *
 * Behaviour, as in Dart:
 *
 * - The preamble is written at once. Incoming bytes are parsed as
 *   {@link readable} is read, never ahead of it, and the peer's preamble is
 *   checked before any frame.
 * - A wrong magic or a zero length errors {@link readable} with a
 *   `ProtocolError`; an unsupported preamble version with a
 *   `SwitchboardError` `UNSUPPORTED`; a length above {@link maxFrameSize}
 *   with `FRAME_TOO_LARGE`, without reading the body. The frames before the
 *   offending bytes are read first. The output stays open until the error
 *   has been handed to the reader, plus one turn of the clock (a zero delay
 *   timer), so that a mux can still send its GOAWAY; then it closes.
 * - Output the byte output cannot take yet waits in the transport, small
 *   writes coalesced up to 64 KiB. While more than
 *   {@link outputHighWaterMark} bytes wait, the input is not read.
 * - When the input ends, {@link readable} closes after the frames already
 *   parsed and the output is closed.
 * - Closing {@link writable} ends {@link readable} after the frames already
 *   parsed, flushes and closes the output, and resolves once it is closed.
 *   A close that takes longer than {@link closeTimeout} aborts. Aborting
 *   {@link writable} closes the same way; its `closed` promise rejects with
 *   the reason, as Dart's `addError` fails `done`.
 * - Cancelling {@link readable} leaves {@link writable} usable and keeps
 *   reading, and dropping, to notice the peer closing; input held back by
 *   the output high-water mark stays held back.
 * - Frames written after the output started closing, or after the byte
 *   output failed, are dropped.
 */
export class StreamTransport implements FrameTransport {
  /** Default {@link maxFrameSize}: 1 MiB. */
  static readonly defaultMaxFrameSize = defaultMaxFrameSize;
  /** Default {@link outputHighWaterMark}: 16 MiB. */
  static readonly defaultOutputHighWaterMark = 16 * 1024 * 1024;
  /** Default {@link closeTimeout}: 10 seconds. */
  static readonly defaultCloseTimeout = 10_000;

  /** Incoming frame bodies, one chunk each. */
  readonly readable: ReadableStream<Uint8Array>;
  /** Outgoing frame bodies, one chunk each; closing it closes the transport. */
  readonly writable: WritableStream<Uint8Array>;
  /** Largest accepted incoming frame body; 0 when there is no limit. */
  readonly maxFrameSize: number;
  /** Queued output above which the input is not read; 0 means never. */
  readonly outputHighWaterMark: number;
  /** Milliseconds a close may take before the transport aborts. */
  readonly closeTimeout: number;
  /** Whether the input is drained after the transport closes. */
  readonly linger: boolean;
  /** The peer, as given in the options. */
  declare readonly remote?: string;

  readonly #input: ByteInput;
  readonly #output: ByteOutput;
  readonly #decoder: StreamFrameDecoder;
  readonly #abortHook: (() => void) | undefined;
  readonly #clock: Clock;
  readonly #logger: Logger;

  // Input.
  readonly #frames = new Fifo<Uint8Array>();
  #failure: { readonly error: unknown } | undefined;
  #cancelled = false;
  #ended = false;
  #inputStarted = false;
  #inputDone = false;
  #inputStopped = false;
  #pumping: Promise<void> | undefined;
  #draining = false;
  #lingering = false;
  #lingerTimer: TimerHandle | undefined;
  #changed: Deferred<void> | undefined;
  #readerWake: Deferred<void> | undefined;

  // Output.
  readonly #outQueue = new Fifo<Uint8Array>();
  #outQueuedBytes = 0;
  #outAcceptedBytes = 0;
  #drainScheduled = false;
  #outEndRequested = false;
  #outputCloseStarted = false;
  #outputClosed = false;
  #throttled = false;
  #aborted = false;
  readonly #outputFinished = new Deferred<void>();
  #outputClose: Promise<void> | undefined;
  #readyWaiter: Deferred<void> | undefined;
  #watchingReady = false;
  #writableController: WritableStreamDefaultController | undefined;
  // Whether the user of `writable` closed or aborted it: the output then
  // ends as asked, and its `closed` settles that way.
  #closeAsked = false;
  // Why the output ended when the transport ended it on its own.
  #outputEndReason: unknown;

  /**
   * Starts the transport over [input] and [output] and writes the preamble.
   *
   * @throws {RangeError} for an option that is not a non-negative integer.
   */
  constructor(input: ByteInput, output: ByteOutput, options: StreamTransportOptions = {}) {
    this.maxFrameSize = checkNonNegativeInteger(
      options.maxFrameSize ?? defaultMaxFrameSize,
      'maxFrameSize',
    );
    this.outputHighWaterMark = checkNonNegativeInteger(
      options.outputHighWaterMark ?? StreamTransport.defaultOutputHighWaterMark,
      'outputHighWaterMark',
    );
    const closeTimeout = options.closeTimeout ?? StreamTransport.defaultCloseTimeout;
    if (!(closeTimeout >= 0)) {
      throw new RangeError(`closeTimeout must not be negative, got ${closeTimeout}`);
    }
    this.closeTimeout = closeTimeout;
    this.linger = options.linger ?? false;
    if (options.remote !== undefined) {
      this.remote = options.remote;
    }
    this.#input = input;
    this.#output = output;
    this.#decoder = new StreamFrameDecoder({ maxFrameSize: this.maxFrameSize });
    this.#abortHook = options.abort;
    this.#clock = options.clock ?? systemClock;
    this.#logger = options.logger ?? noopLogger;
    output.onFailure((error) => this.#onOutputError(error));
    this.readable = new ReadableStream<Uint8Array>(
      {
        pull: (controller) => this.#pull(controller),
        cancel: () => this.#onCancel(),
      },
      { highWaterMark: 0 },
    );
    this.writable = new WritableStream<Uint8Array>({
      start: (controller) => {
        this.#writableController = controller;
      },
      write: (frame) => this.#send(frame),
      close: () => {
        this.#closeAsked = true;
        return this.#closeLocal();
      },
      abort: () => {
        this.#closeAsked = true;
        void this.#closeLocal();
      },
    });
    void this.#outputFinished.promise.then(() => this.#onOutputFinished());
    this.#write(StreamFraming.encodePreamble());
  }

  /** Bytes written that the output has not taken yet, preamble and length prefixes included. */
  get bufferedOutputBytes(): number {
    return this.#outQueuedBytes;
  }

  /** Total bytes the output has taken, preamble and length prefixes included. */
  get acceptedOutputBytes(): number {
    return this.#outAcceptedBytes;
  }

  /** Whether the input is not read because more than {@link outputHighWaterMark} bytes wait. */
  get isInputThrottled(): boolean {
    return this.#throttled;
  }

  /** Whether reading the input has started. */
  get isInputStarted(): boolean {
    return this.#inputStarted;
  }

  /**
   * Whether a frame written now goes to the output without waiting behind
   * earlier output. True once the output is closing or gone, where writes
   * are dropped.
   */
  get isOutputReady(): boolean {
    return (
      this.#outputClosed ||
      this.#outEndRequested ||
      this.#aborted ||
      (this.#outQueue.length === 0 && this.#canDeliver)
    );
  }

  /** Resolves when {@link isOutputReady} becomes true, at once if it is. Never rejects. */
  get outputReady(): Promise<void> {
    if (this.isOutputReady) {
      return Promise.resolve();
    }
    if (this.#readyWaiter === undefined) {
      this.#readyWaiter = new Deferred();
      this.#watchReady();
    }
    return this.#readyWaiter.promise;
  }

  /**
   * Destroys the connection: drops buffered output, ends {@link readable}
   * after the frames already parsed, tears down the byte streams, calls the
   * `abort` hook of the options and resolves a pending close. Idempotent;
   * never throws.
   */
  abort(): void {
    if (this.#aborted) {
      return;
    }
    this.#aborted = true;
    this.#noteOutputEnd(
      SwitchboardError.of(StatusCode.connectionLost, 'the transport was aborted'),
    );
    this.#log('debug', `aborting with ${this.#outQueuedBytes} bytes unwritten`);
    this.#outputClosed = true;
    this.#dropOutput();
    this.#end();
    this.#stopLinger();
    this.#cancelInput();
    this.#output.abort();
    const hook = this.#abortHook;
    if (hook !== undefined) {
      try {
        hook();
      } catch (error) {
        this.#log('debug', `abort hook failed: ${error}`, error);
      }
    }
    this.#outputFinished.resolve();
    this.#signalReady();
    this.#poke();
  }

  // Input -------------------------------------------------------------------

  async #pull(controller: ReadableStreamDefaultController<Uint8Array>): Promise<void> {
    for (;;) {
      const frame = this.#frames.shift();
      if (frame !== undefined) {
        controller.enqueue(frame);
        return;
      }
      if (this.#ended) {
        const failure = this.#failure;
        if (failure === undefined) {
          controller.close();
          return;
        }
        controller.error(failure.error);
        // The reader has the error; let it react (send GOAWAY) before the
        // output closes.
        this.#clock.setTimeout(() => {
          void this.#closeOutput();
        }, 0);
        return;
      }
      // The transport ending wakes the reader even while a read of the input
      // is still pending. A waker of its own per wait: racing a long-lived
      // promise would pile reactions onto it.
      const wake = new Deferred<void>();
      this.#readerWake = wake;
      void this.#pumpOnce().then(() => wake.resolve());
      await wake.promise;
      this.#readerWake = undefined;
    }
  }

  #onCancel(): void {
    // Cancelling has no effect on the writable side, and the transport must
    // still notice the peer closing: keep reading and drop the frames.
    this.#cancelled = true;
    this.#frames.clear();
    if (!this.#ended) {
      void this.#drainInput();
    }
  }

  /** One read of the input at a time, shared by the reader and the drain. */
  #pumpOnce(): Promise<void> {
    if (this.#pumping === undefined) {
      this.#pumping = this.#pump().finally(() => {
        this.#pumping = undefined;
      });
    }
    return this.#pumping;
  }

  async #pump(): Promise<void> {
    while (this.#throttled && !this.#inputStopped) {
      await this.#nextChange();
    }
    if (this.#inputStopped || this.#inputDone) {
      return;
    }
    this.#inputStarted = true;
    let chunk: Uint8Array | undefined;
    try {
      chunk = await this.#input.read();
    } catch (error) {
      if (!this.#inputStopped) {
        this.#fail(error, false);
      }
      return;
    }
    if (this.#inputStopped) {
      return;
    }
    if (chunk === undefined) {
      this.#onInputDone();
      return;
    }
    if (this.#ended) {
      // Lingering: the input is drained and discarded.
      return;
    }
    try {
      this.#decoder.add(chunk, (frame) => {
        if (!this.#ended && !this.#cancelled) {
          this.#frames.push(frame);
        }
      });
    } catch (error) {
      this.#log('warn', `${error}`, error);
      this.#fail(error, true);
    }
  }

  /** Reads until the input ends, while lingering or after the reader cancelled. */
  async #drainInput(): Promise<void> {
    if (this.#draining) {
      return;
    }
    this.#draining = true;
    try {
      while (!this.#inputDone && !this.#inputStopped) {
        await this.#pumpOnce();
      }
    } finally {
      this.#draining = false;
    }
  }

  #onInputDone(): void {
    this.#inputDone = true;
    this.#stopLinger();
    if (this.#ended) {
      return;
    }
    if (this.#decoder.bufferedBytes > 0) {
      this.#log('debug', `input ended inside a frame (${this.#decoder.bufferedBytes} bytes)`);
    }
    this.#noteOutputEnd(
      SwitchboardError.of(StatusCode.connectionLost, 'the peer closed the connection'),
    );
    this.#end();
    void this.#closeOutput();
  }

  /**
   * Ends {@link readable} with [error] after the frames already parsed,
   * then closes the output once the reader has the error. [drain] (with
   * {@link linger}) keeps reading the input until the peer closes: true for
   * errors in what the peer sent, false when the input itself failed.
   */
  #fail(error: unknown, drain: boolean): void {
    if (this.#ended) {
      if (!drain) {
        // The input failed while draining: nothing more will come.
        this.#stopLinger();
        this.#cancelInput();
      }
      return;
    }
    this.#ended = true;
    this.#noteOutputEnd(error);
    if (drain) {
      this.#startLinger();
    } else {
      this.#cancelInput();
    }
    if (this.#cancelled || !this.readable.locked) {
      // Nobody to tell: close at once.
      void this.#closeOutput();
    } else {
      this.#failure = { error };
    }
    this.#poke();
  }

  #end(): void {
    if (this.#ended) {
      return;
    }
    this.#ended = true;
    this.#startLinger();
    this.#poke();
  }

  /**
   * Keeps reading (and discarding) the input until it ends, for at most
   * {@link closeTimeout}, when {@link linger} is set; otherwise stops
   * reading.
   */
  #startLinger(): void {
    if (!this.linger || this.#inputDone || this.#aborted) {
      this.#stopInput();
      return;
    }
    if (this.#lingering || this.#inputStopped) {
      return;
    }
    this.#lingering = true;
    void this.#drainInput();
    this.#lingerTimer = this.#clock.setTimeout(() => {
      this.#lingerTimer = undefined;
      if (!this.#inputDone) {
        this.#log('debug', `peer did not close within ${this.closeTimeout} ms, aborting`);
        this.abort();
      }
    }, this.closeTimeout);
  }

  #stopLinger(): void {
    if (this.#lingerTimer !== undefined) {
      this.#clock.clearTimeout(this.#lingerTimer);
      this.#lingerTimer = undefined;
    }
  }

  /**
   * Stops reading the input. The input is released (cancelled) only once
   * the output has closed: on a byte duplex where cancelling the readable
   * side destroys the connection (a Node `Duplex.toWeb()`), cancelling
   * earlier would lose the output still to be flushed, the last GOAWAY
   * among it.
   */
  #stopInput(): void {
    if (this.#inputStopped) {
      return;
    }
    this.#inputStopped = true;
    this.#poke();
    if (this.#outputFinished.isSettled) {
      this.#input.cancel();
    } else {
      void this.#outputFinished.promise.then(() => this.#input.cancel());
    }
  }

  /** Stops reading the input and releases it at once: the connection is gone or torn down. */
  #cancelInput(): void {
    this.#stopInput();
    this.#input.cancel();
  }

  /** Resolves on the next change the input pump waits for. */
  #nextChange(): Promise<void> {
    this.#changed ??= new Deferred();
    return this.#changed.promise;
  }

  #poke(): void {
    const changed = this.#changed;
    if (changed !== undefined) {
      this.#changed = undefined;
      changed.resolve();
    }
    this.#readerWake?.resolve();
  }

  // Output ------------------------------------------------------------------

  get #canDeliver(): boolean {
    return !this.#outputCloseStarted && !this.#outputClosed && this.#output.canWrite;
  }

  #send(frame: Uint8Array): void {
    if (frame.length === 0) {
      // The writable errors; close as Dart's addError does.
      void this.#closeLocal();
      throw new RangeError('stream binding frames are never empty');
    }
    this.#write(StreamFraming.encodeFrame(frame));
  }

  #write(bytes: Uint8Array): void {
    // Once the output is closing, bytes are dropped. After an input error
    // the output stays open until the reader has the error, so a last
    // frame such as GOAWAY still goes out.
    if (this.#outputClosed || this.#outEndRequested) {
      return;
    }
    if (this.#outQueue.length === 0 && this.#canDeliver) {
      this.#outAcceptedBytes += bytes.length;
      this.#output.write(bytes);
      return;
    }
    this.#outQueue.push(bytes);
    this.#outQueuedBytes += bytes.length;
    this.#updateThrottle();
    this.#scheduleDrain();
  }

  #scheduleDrain(): void {
    if (this.#drainScheduled) {
      return;
    }
    this.#drainScheduled = true;
    if (this.#canDeliver) {
      queueMicrotask(() => this.#drain());
    } else {
      void this.#output.ready().then(() => this.#drain());
    }
  }

  #drain(): void {
    this.#drainScheduled = false;
    const queue = this.#outQueue;
    while (queue.length > 0 && this.#canDeliver) {
      let chunk = queue.shift() as Uint8Array;
      if (queue.length > 0 && chunk.length < coalesceLimit) {
        const parts = [chunk];
        let size = chunk.length;
        for (let next = queue.peek(); next !== undefined; next = queue.peek()) {
          if (size + next.length > coalesceLimit) {
            break;
          }
          parts.push(next);
          size += next.length;
          queue.shift();
        }
        if (parts.length > 1) {
          chunk = concatBytes(...parts);
        }
      }
      this.#outQueuedBytes -= chunk.length;
      this.#outAcceptedBytes += chunk.length;
      this.#output.write(chunk);
    }
    this.#updateThrottle();
    if (queue.length === 0) {
      if (this.#outEndRequested) {
        this.#startOutputClose();
      }
    } else if (!this.#outputClosed && !this.#outputCloseStarted) {
      this.#scheduleDrain();
    }
    this.#signalReady();
  }

  /** Waits for the output to take more while a caller waits on {@link outputReady}. */
  #watchReady(): void {
    if (this.#watchingReady || this.#readyWaiter === undefined || this.#outQueue.length > 0) {
      // Nobody waits, or a drain is scheduled and signals.
      return;
    }
    this.#watchingReady = true;
    void this.#output.ready().then(() => {
      this.#watchingReady = false;
      this.#signalReady();
    });
  }

  #signalReady(): void {
    const waiter = this.#readyWaiter;
    if (waiter === undefined) {
      return;
    }
    if (this.isOutputReady) {
      this.#readyWaiter = undefined;
      waiter.resolve();
    } else {
      this.#watchReady();
    }
  }

  /** Stops reading the input while too much output waits, until half of it is written. */
  #updateThrottle(): void {
    const mark = this.outputHighWaterMark;
    if (!this.#throttled) {
      if (mark > 0 && this.#outQueuedBytes > mark) {
        this.#throttled = true;
        this.#log('debug', `${this.#outQueuedBytes} bytes wait for the output, pausing input`);
      }
    } else if (this.#outQueuedBytes <= Math.floor(mark / 2)) {
      this.#throttled = false;
      this.#log('debug', 'output drained, resuming input');
      this.#poke();
    }
  }

  #dropOutput(): void {
    this.#outQueue.clear();
    this.#outQueuedBytes = 0;
    this.#updateThrottle();
  }

  #startOutputClose(): void {
    if (this.#outputCloseStarted || this.#outputClosed) {
      return;
    }
    this.#outputCloseStarted = true;
    this.#output.close().then(
      () => {
        this.#outputClosed = true;
        this.#signalReady();
        this.#outputFinished.resolve();
      },
      (error: unknown) => this.#onOutputError(error),
    );
  }

  /**
   * The output stopped taking bytes: it failed, or the peer cancelled its
   * reading side, which a Web Streams pipe reports as the same error.
   * Nothing more can be written, so queued output is dropped and a pending
   * close completes. The input is left to end (or fail) on its own, as
   * Dart's transport does when its output is cancelled: a peer that stops
   * reading is not a broken stream, and a broken connection fails its
   * input too.
   */
  #onOutputError(error: unknown): void {
    if (this.#aborted || this.#outputFinished.isSettled) {
      return;
    }
    this.#log('debug', `output gone: ${error}`, error);
    this.#noteOutputEnd(error);
    this.#outputClosed = true;
    this.#dropOutput();
    this.#signalReady();
    this.#outputFinished.resolve();
  }

  /**
   * Closes the output once: flushes what is queued, closes it, and resolves
   * when it is closed. Aborts after {@link closeTimeout}. Never rejects.
   */
  #closeOutput(): Promise<void> {
    this.#outputClose ??= this.#doCloseOutput();
    return this.#outputClose;
  }

  async #doCloseOutput(): Promise<void> {
    this.#outEndRequested = true;
    this.#drain();
    this.#signalReady();
    if (!this.#outputFinished.isSettled) {
      const timer = this.#clock.setTimeout(() => {
        if (!this.#outputFinished.isSettled) {
          this.#log(
            'debug',
            `output not closed within ${this.closeTimeout} ms ` +
              `(${this.#outQueuedBytes} bytes unwritten)`,
          );
          this.abort();
        }
      }, this.closeTimeout);
      await this.#outputFinished.promise;
      this.#clock.clearTimeout(timer);
    }
    this.#outputClosed = true;
  }

  /** Records why the output ends, the first reason given. */
  #noteOutputEnd(reason: unknown): void {
    this.#outputEndReason ??= reason;
  }

  /**
   * The output has ended. When the transport ended it on its own (the
   * peer closed, the connection failed, an abort), {@link writable} is
   * errored with the reason, so that its `closed` settles and later writes
   * are refused rather than dropped for ever; a close or an abort of the
   * writable itself settles it as asked.
   */
  #onOutputFinished(): void {
    if (this.#closeAsked) {
      return;
    }
    this.#writableController?.error(
      this.#outputEndReason ??
        SwitchboardError.of(StatusCode.connectionLost, 'the transport closed'),
    );
  }

  /** Local close: the readable ends after the frames already parsed; the output closes. */
  #closeLocal(): Promise<void> {
    this.#end();
    return this.#closeOutput();
  }

  #log(level: LogLevel, message: string, error?: unknown): void {
    const text = `stream transport${this.remote === undefined ? '' : ` ${this.remote}`}: ${message}`;
    if (error === undefined) {
      this.#logger.log(level, loggerName, text);
    } else {
      this.#logger.log(level, loggerName, text, error);
    }
  }
}

/** A byte connection as Web Streams: a Node `Duplex.toWeb()`, a Deno connection, a QUIC stream. */
export interface ByteDuplex {
  /** Bytes from the peer. */
  readonly readable: ReadableStream<Uint8Array>;
  /** Bytes to the peer. */
  readonly writable: WritableStream<Uint8Array>;
}

/**
 * Runs the stream binding over [bytes], which it locks. Aborting the
 * transport aborts [bytes]' writable side and cancels its readable side,
 * then calls the `abort` hook of [options].
 *
 * @throws {RangeError} for an option that is not a non-negative integer.
 */
export function streamTransport(
  bytes: ByteDuplex,
  options: StreamTransportOptions = {},
): StreamTransport {
  return new StreamTransport(webByteInput(bytes.readable), webByteOutput(bytes.writable), options);
}
