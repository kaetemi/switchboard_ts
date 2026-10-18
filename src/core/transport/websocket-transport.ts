/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * The WebSocket binding of the transport layer: one mux frame per binary
 * WebSocket message, subprotocol `switchboard` (wiki page "Polyverse
 * Switchboard Transport", section "WebSocket binding"). Ports
 * `web_socket_transport.dart`.
 *
 * {@link WebSocketTransport} runs over a {@link WebSocketConnection}, a
 * small adapter of a WebSocket implementation: {@link webSocketConnection}
 * adapts the W3C `WebSocket` of browsers, Node 22, Bun and Deno; the Node
 * entry adapts the `ws` package, which adds what the W3C API cannot do
 * (pausing the input, observing the output, tearing the connection down,
 * bounding messages before they are assembled). {@link connectWebSocket}
 * dials over the global `WebSocket`.
 */

import { ProtocolError, StatusCode, SwitchboardError } from '../status.ts';
import { type Clock, systemClock, type TimerHandle } from '../util/clock.ts';
import { Deferred } from '../util/deferred.ts';
import { type Logger, type LogLevel, noopLogger } from '../util/logger.ts';
import { defaultMaxFrameSize, type FrameTransport, webSocketSubprotocol } from './contract.ts';
import { Fifo } from './fifo.ts';

/** The `readyState` values of the WebSocket API. */
export const WebSocketReadyState = Object.freeze({
  connecting: 0,
  open: 1,
  closing: 2,
  closed: 3,
} as const);

/** One of the {@link WebSocketReadyState} values. */
export type WebSocketReadyState = (typeof WebSocketReadyState)[keyof typeof WebSocketReadyState];

/** What a {@link WebSocketConnection} tells its transport. Called on the event loop, never re-entrantly from a call into the connection. */
export interface WebSocketHandlers {
  /** The opening handshake completed. */
  open(): void;
  /** A message arrived: bytes for a binary message, a string for a text message. */
  message(data: Uint8Array | string): void;
  /** The connection failed; a {@link close} follows. */
  error(error: unknown): void;
  /**
   * The connection closed: the code of the peer's close frame (1005 when it
   * carried none, 1006 when there was no close frame) and its reason.
   */
  close(code: number, reason: string): void;
}

/**
 * A WebSocket as {@link WebSocketTransport} drives it. The required
 * members are those of the W3C `WebSocket` API; the optional ones are what
 * an implementation such as the `ws` package adds, each enabling a
 * capability of the transport.
 */
export interface WebSocketConnection {
  /** A {@link WebSocketReadyState} value. */
  readonly readyState: number;
  /** The subprotocol the server selected; empty for none. */
  readonly protocol: string;
  /** Bytes sent and not yet handed to the network. */
  readonly bufferedAmount: number;
  /** Installs the transport's handlers. Called once, before anything else. */
  listen(handlers: WebSocketHandlers): void;
  /** Sends [frame] as one binary message. */
  send(frame: Uint8Array): void;
  /** Starts the closing handshake with [code] and [reason]. */
  close(code: number, reason?: string): void;
  /** Destroys the connection at once, without the closing handshake. */
  terminate?(): void;
  /** Stops reading from the network until {@link resume}. */
  pause?(): void;
  /** Reads from the network again after {@link pause}. */
  resume?(): void;
  /** Resolves once {@link bufferedAmount} is at most [bytes], or the connection has closed. Never rejects. */
  whenBufferedAtMost?(bytes: number): Promise<void>;
}

/** Options of {@link WebSocketTransport}. Durations in milliseconds. */
export interface WebSocketTransportOptions {
  /**
   * Largest accepted incoming message; 0 means no limit. Default 1 MiB.
   * Should agree with `MuxOptions.maxFrameSize`.
   */
  readonly maxFrameSize?: number;
  /**
   * How long the closing handshake may take before the connection is torn
   * down. Default 10 000.
   */
  readonly closeTimeout?: number;
  /**
   * Bytes waiting for the network above which the input is not read,
   * until they drop to half of it; 0 means never. Only with a connection
   * that can pause and observe its output. Default 16 MiB.
   */
  readonly outputHighWaterMark?: number;
  /**
   * The close code sent after the input failed with [error]; default 1000
   * for every failure (clients may only send 1000 and 3000 to 4999). A
   * server sends 1002, 1003 or 1009.
   */
  readonly closeCodeFor?: (error: unknown) => number;
  /** The peer, for logs and `FrameTransport.remote`. */
  readonly remote?: string;
  /** Source of the close timer. Default {@link systemClock}. */
  readonly clock?: Clock;
  /** Receives the transport's diagnostics, as `Switchboard.Transport`. */
  readonly logger?: Logger;
}

const loggerName = 'Switchboard.Transport';

/** The close code of every orderly close. */
const normalClosure = 1000;

/** Buffered output below which a frame written now is taken without waiting. */
const outputReadyMark = 64 * 1024;

const ignore = (): void => {};

/**
 * The WebSocket binding over a {@link WebSocketConnection}: binary
 * messages are frames, both ways.
 *
 * Behaviour, as in Dart:
 *
 * - A text message is a protocol error: {@link readable} fails with a
 *   `ProtocolError` after the frames before it. A binary message longer
 *   than {@link maxFrameSize} fails it with a `SwitchboardError`
 *   `FRAME_TOO_LARGE`. The WebSocket stays open until the reader has the
 *   error, plus one turn of the clock, so that a mux can still send its
 *   GOAWAY; then it is closed with `closeCodeFor(error)` (1000 by
 *   default).
 * - The peer closing ends {@link readable} after the frames already
 *   received.
 * - Closing (or aborting) {@link writable} ends {@link readable} after the
 *   frames already received and closes the WebSocket with code 1000 (with
 *   the failure's code after an input failure); it
 *   resolves once the closing handshake is over, at most
 *   {@link closeTimeout} later (then the connection is torn down).
 *   Aborting it makes its `closed` promise reject with the reason.
 * - Cancelling {@link readable} leaves {@link writable} usable and keeps
 *   reading, and dropping, so that the peer closing is still noticed.
 * - Frames written while the WebSocket is closing are dropped. Frames are
 *   not copied: a frame written belongs to the transport.
 * - Once the WebSocket ended on its own (the peer closed it, it failed,
 *   {@link abort}) without {@link writable} being closed or aborted, the
 *   writable is errored with the reason (`CONNECTION_LOST` for a close),
 *   so that its `closed` settles and later writes reject rather than being
 *   dropped for ever. Dart's sink completes instead.
 * - A connection that is still connecting is waited for: writes wait for
 *   it to open, and a close closes it once it has.
 *
 * The capabilities depend on the connection: {@link bufferedOutputBytes}
 * and {@link acceptedOutputBytes} always (from `bufferedAmount`);
 * `isOutputReady`, `outputReady` and, with `pause` and `resume`,
 * `isInputThrottled` when it can observe its output; a connection that can
 * pause is not read further while a received frame waits for its reader.
 * {@link abort} tears the connection down where it can, and closes it
 * otherwise.
 */
export class WebSocketTransport implements FrameTransport {
  /** Default {@link maxFrameSize}: 1 MiB. */
  static readonly defaultMaxFrameSize = defaultMaxFrameSize;
  /** Default {@link closeTimeout}: 10 seconds. */
  static readonly defaultCloseTimeout = 10_000;
  /** Default {@link outputHighWaterMark}: 16 MiB. */
  static readonly defaultOutputHighWaterMark = 16 * 1024 * 1024;
  /** The WebSocket subprotocol, `switchboard`. */
  static readonly subprotocol = webSocketSubprotocol;
  /** The close code of an orderly close, 1000. */
  static readonly normalClosure = normalClosure;

  /** Incoming frames, one per binary message. */
  readonly readable: ReadableStream<Uint8Array>;
  /** Outgoing frames, one per binary message; closing it closes the WebSocket. */
  readonly writable: WritableStream<Uint8Array>;
  /** Largest accepted incoming message; 0 when there is no limit. */
  readonly maxFrameSize: number;
  /** Milliseconds the closing handshake may take. */
  readonly closeTimeout: number;
  /** Buffered output above which the input is not read (connections that can pause); 0 means never. */
  readonly outputHighWaterMark: number;
  /** Resolves once the opening handshake completed; rejects if the connection closed or failed first. */
  readonly opened: Promise<void>;
  /** Resolves once the WebSocket has closed (or was torn down). Never rejects. */
  readonly closed: Promise<void>;
  /** The peer, as given in the options. */
  declare readonly remote?: string;
  /** Whether a frame written now is taken without waiting behind earlier output. */
  declare readonly isOutputReady?: boolean;
  /** Resolves when {@link isOutputReady} becomes true, at once if it is. Never rejects. */
  declare readonly outputReady?: Promise<void>;
  /** Whether the input is not read because more than {@link outputHighWaterMark} bytes wait. */
  declare readonly isInputThrottled?: boolean;

  readonly #connection: WebSocketConnection;
  readonly #clock: Clock;
  readonly #logger: Logger;
  readonly #closeCodeFor: (error: unknown) => number;
  readonly #opened = new Deferred<void>();
  readonly #closed = new Deferred<void>();

  // Input.
  readonly #frames = new Fifo<Uint8Array>();
  #failure: { readonly error: unknown } | undefined;
  #ended = false;
  #cancelled = false;
  #pulling = false;
  #inputStarted = false;
  #paused = false;
  #throttled = false;
  #wake: Deferred<void> | undefined;

  // Output.
  #sentBytes = 0;
  #acceptedBytes = 0;
  #closeRequested = false;
  #closeCode: number | undefined;
  #failureCode: number | undefined;
  #closeOnOpen: number | undefined;
  #closeTimer: TimerHandle | undefined;
  #aborted = false;
  #peerCloseCode: number | undefined;
  #peerCloseReason: string | undefined;
  #writableController: WritableStreamDefaultController | undefined;
  // Whether the user of `writable` closed or aborted it: its `closed` then
  // settles as asked.
  #closeAsked = false;
  // Why the output ended when the WebSocket ended on its own.
  #outputEndReason: unknown;

  /**
   * Runs the binding over [connection], which it owns from now on.
   *
   * @throws {RangeError} for a limit or timeout that is not a non-negative
   *   integer (a timeout may be fractional).
   */
  constructor(connection: WebSocketConnection, options: WebSocketTransportOptions = {}) {
    const max = options.maxFrameSize ?? defaultMaxFrameSize;
    if (!Number.isInteger(max) || max < 0) {
      throw new RangeError(`maxFrameSize must be a non-negative integer, got ${max}`);
    }
    const mark = options.outputHighWaterMark ?? WebSocketTransport.defaultOutputHighWaterMark;
    if (!Number.isInteger(mark) || mark < 0) {
      throw new RangeError(`outputHighWaterMark must be a non-negative integer, got ${mark}`);
    }
    const closeTimeout = options.closeTimeout ?? WebSocketTransport.defaultCloseTimeout;
    if (!(closeTimeout >= 0)) {
      throw new RangeError(`closeTimeout must not be negative, got ${closeTimeout}`);
    }
    this.maxFrameSize = max;
    this.outputHighWaterMark = mark;
    this.closeTimeout = closeTimeout;
    if (options.remote !== undefined) {
      this.remote = options.remote;
    }
    this.#connection = connection;
    this.#clock = options.clock ?? systemClock;
    this.#logger = options.logger ?? noopLogger;
    this.#closeCodeFor = options.closeCodeFor ?? (() => normalClosure);
    this.opened = this.#opened.promise;
    this.closed = this.#closed.promise;
    const observable = connection.whenBufferedAtMost !== undefined;
    if (observable) {
      Object.defineProperty(this, 'isOutputReady', {
        get: () => this.#isOutputReady(),
        enumerable: true,
      });
      Object.defineProperty(this, 'outputReady', {
        get: () => this.#outputReady(),
        enumerable: true,
      });
      if (connection.pause !== undefined && connection.resume !== undefined) {
        Object.defineProperty(this, 'isInputThrottled', {
          get: () => this.#throttled,
          enumerable: true,
        });
      }
    }
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
        return this.#opened.promise.then(ignore, ignore);
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
    connection.listen({
      open: () => this.#onOpen(),
      message: (data) => this.#onMessage(data),
      error: (error) => this.#onError(error),
      close: (code, reason) => this.#onClose(code, reason),
    });
    const state = connection.readyState;
    if (state === WebSocketReadyState.open) {
      this.#opened.resolve();
    } else if (state !== WebSocketReadyState.connecting) {
      const closed = SwitchboardError.of(StatusCode.unavailable, 'WebSocket is closed');
      this.#opened.reject(closed);
      this.#end();
      this.#closed.resolve();
      this.#outputEnded(closed);
    }
    this.#updateInput();
  }

  /** The subprotocol the server selected, or undefined for none. */
  get protocol(): string | undefined {
    const protocol = this.#connection.protocol;
    return protocol === '' ? undefined : protocol;
  }

  /** The code of the close frame the peer sent, or undefined if none arrived or it carried none. */
  get closeCode(): number | undefined {
    return this.#peerCloseCode;
  }

  /** The reason of the close frame the peer sent, or undefined. */
  get closeReason(): string | undefined {
    return this.#peerCloseReason;
  }

  /** Bytes written that the WebSocket has not handed to the network yet. */
  get bufferedOutputBytes(): number {
    return this.#connection.bufferedAmount;
  }

  /** Total bytes of frames the WebSocket handed to the network; grows while the peer reads. */
  get acceptedOutputBytes(): number {
    const accepted = Math.max(0, this.#sentBytes - this.#connection.bufferedAmount);
    if (accepted > this.#acceptedBytes) {
      this.#acceptedBytes = accepted;
    }
    return this.#acceptedBytes;
  }

  /** Whether reading has started: a reader asked for a frame. */
  get isInputStarted(): boolean {
    return this.#inputStarted;
  }

  /** Whether the connection is not read now (paused), for a connection that can pause. */
  get isInputPaused(): boolean {
    return this.#paused;
  }

  /**
   * Tears the connection down: ends {@link readable} after the frames
   * already received, drops output, terminates the connection (closes it
   * where it cannot be terminated) and resolves a pending close.
   * Idempotent; never throws.
   */
  abort(): void {
    if (this.#aborted) {
      return;
    }
    this.#aborted = true;
    this.#closeRequested = true;
    this.#log('debug', 'aborting');
    this.#end();
    this.#stopCloseTimer();
    const connection = this.#connection;
    try {
      if (connection.terminate !== undefined) {
        connection.terminate();
      } else {
        connection.close(normalClosure);
      }
    } catch (error) {
      this.#log('debug', `abort failed: ${error}`, error);
    }
    this.#opened.reject(SwitchboardError.of(StatusCode.cancelled, 'WebSocket transport aborted'));
    this.#closed.resolve();
    this.#outputEnded(SwitchboardError.of(StatusCode.connectionLost, 'the transport was aborted'));
    this.#updateInput();
  }

  // Input -------------------------------------------------------------------

  async #pull(controller: ReadableStreamDefaultController<Uint8Array>): Promise<void> {
    this.#inputStarted = true;
    for (;;) {
      const frame = this.#frames.shift();
      if (frame !== undefined) {
        controller.enqueue(frame);
        this.#updateInput();
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
        // WebSocket closes.
        this.#clock.setTimeout(() => {
          void this.#closeSocket(this.#failureCode ?? normalClosure);
        }, 0);
        return;
      }
      const wake = new Deferred<void>();
      this.#wake = wake;
      this.#pulling = true;
      this.#updateInput();
      await wake.promise;
      this.#pulling = false;
      this.#wake = undefined;
    }
  }

  #onCancel(): void {
    // Cancelling has no effect on the writable side, and the transport must
    // still notice the peer closing: keep reading and drop the frames.
    this.#cancelled = true;
    this.#frames.clear();
    this.#updateInput();
  }

  #onOpen(): void {
    this.#opened.resolve();
    const code = this.#closeOnOpen;
    if (code !== undefined) {
      this.#closeOnOpen = undefined;
      this.#sendClose(code);
    }
  }

  #onMessage(data: Uint8Array | string): void {
    if (this.#ended) {
      return;
    }
    if (typeof data === 'string') {
      this.#log('warn', 'text message received');
      this.#fail(new ProtocolError('text WebSocket message'));
      return;
    }
    const max = this.maxFrameSize;
    if (max > 0 && data.length > max) {
      this.#log('warn', `message of ${data.length} bytes exceeds ${max}`);
      this.#fail(
        SwitchboardError.of(
          StatusCode.frameTooLarge,
          `message of ${data.length} bytes exceeds limit of ${max}`,
        ),
      );
      return;
    }
    if (this.#cancelled) {
      return;
    }
    this.#frames.push(data);
    this.#poke();
    this.#updateInput();
  }

  #onError(error: unknown): void {
    this.#outputEndReason ??= error;
    if (!this.#opened.isSettled) {
      this.#opened.reject(error);
      return;
    }
    this.#log('debug', `error: ${error}`, error);
    this.#fail(error);
  }

  #onClose(code: number, reason: string): void {
    if (code !== 1005 && code !== 1006) {
      this.#peerCloseCode = code;
      this.#peerCloseReason = reason;
    }
    this.#opened.reject(
      SwitchboardError.of(
        StatusCode.unavailable,
        `WebSocket closed before it opened (${code}${reason === '' ? '' : ` ${reason}`})`,
      ),
    );
    this.#closeRequested = true;
    this.#end();
    this.#stopCloseTimer();
    this.#closed.resolve();
    this.#outputEnded(
      SwitchboardError.of(
        StatusCode.connectionLost,
        `the WebSocket closed (${code}${reason === '' ? '' : ` ${reason}`})`,
      ),
    );
    this.#updateInput();
  }

  /**
   * Ends {@link readable} with [error] after the frames already received,
   * then closes the WebSocket once the reader has the error.
   */
  #fail(error: unknown): void {
    if (this.#ended) {
      return;
    }
    this.#ended = true;
    this.#outputEndReason ??= error;
    // A close the reader asks for after this tells the peer the same.
    this.#failureCode = this.#codeFor(error);
    if (this.#cancelled || !this.readable.locked) {
      // Nobody to tell: close at once.
      void this.#closeSocket(this.#failureCode);
    } else {
      this.#failure = { error };
    }
    this.#poke();
    this.#updateInput();
  }

  #end(): void {
    if (this.#ended) {
      return;
    }
    this.#ended = true;
    this.#poke();
  }

  #poke(): void {
    this.#wake?.resolve();
  }

  /**
   * Pauses the connection while a frame waits for its reader or too much
   * output waits, and resumes it otherwise; after the input ended, it is
   * read (and dropped) to notice the peer closing.
   */
  #updateInput(): void {
    const connection = this.#connection;
    if (connection.pause === undefined || connection.resume === undefined) {
      return;
    }
    const pause =
      !this.#ended &&
      !this.#closed.isSettled &&
      (this.#throttled || (!this.#pulling && this.#frames.length > 0));
    if (pause === this.#paused) {
      return;
    }
    this.#paused = pause;
    try {
      if (pause) {
        connection.pause();
      } else {
        connection.resume();
      }
    } catch (error) {
      this.#log('debug', `${pause ? 'pause' : 'resume'} failed: ${error}`, error);
    }
  }

  #codeFor(error: unknown): number {
    try {
      return this.#closeCodeFor(error);
    } catch {
      return normalClosure;
    }
  }

  // Output ------------------------------------------------------------------

  #isOutputReady(): boolean {
    return (
      this.#closeRequested ||
      this.#closed.isSettled ||
      this.#connection.bufferedAmount < outputReadyMark
    );
  }

  #outputReady(): Promise<void> {
    if (this.#isOutputReady()) {
      return Promise.resolve();
    }
    const wait = this.#connection.whenBufferedAtMost;
    if (wait === undefined) {
      return Promise.resolve();
    }
    return Promise.race([
      wait.call(this.#connection, outputReadyMark - 1),
      this.#closed.promise,
    ]).then(ignore, ignore);
  }

  #send(frame: Uint8Array): void {
    // Dropped once the WebSocket is closing. After an input failure it
    // stays open until the reader has the error, so that a last frame such
    // as GOAWAY still goes out.
    if (this.#closeRequested || this.#connection.readyState !== WebSocketReadyState.open) {
      return;
    }
    try {
      this.#connection.send(frame);
      this.#sentBytes += frame.length;
    } catch (error) {
      this.#log('debug', `send failed: ${error}`, error);
      return;
    }
    this.#checkThrottle();
  }

  /** Stops reading while more than {@link outputHighWaterMark} bytes wait, until half of it is sent. */
  #checkThrottle(): void {
    const connection = this.#connection;
    const mark = this.outputHighWaterMark;
    const wait = connection.whenBufferedAtMost;
    if (
      this.#throttled ||
      mark === 0 ||
      wait === undefined ||
      connection.pause === undefined ||
      connection.bufferedAmount <= mark
    ) {
      return;
    }
    this.#throttled = true;
    this.#log('debug', `${connection.bufferedAmount} bytes wait for the network, pausing input`);
    this.#updateInput();
    void Promise.race([wait.call(connection, Math.floor(mark / 2)), this.#closed.promise]).then(
      () => {
        this.#throttled = false;
        this.#log('debug', 'output drained, resuming input');
        this.#updateInput();
      },
    );
  }

  /** Local close: the readable ends after the frames already received; the WebSocket closes. */
  #closeLocal(): Promise<void> {
    this.#end();
    return this.#closeSocket(this.#failureCode ?? normalClosure);
  }

  /**
   * Closes the WebSocket once with [code] (once it is open, if it is still
   * connecting) and resolves when it has closed; tears it down after
   * {@link closeTimeout}. Never rejects.
   */
  #closeSocket(code: number): Promise<void> {
    if (this.#closeCode === undefined && !this.#closed.isSettled) {
      this.#closeCode = code;
      this.#closeRequested = true;
      if (this.#connection.readyState === WebSocketReadyState.connecting) {
        // Closing a WebSocket that is still connecting fails it without a
        // close frame: close it as soon as it opens instead.
        this.#closeOnOpen = code;
      } else {
        this.#sendClose(code);
      }
      this.#closeTimer = this.#clock.setTimeout(() => {
        this.#closeTimer = undefined;
        if (!this.#closed.isSettled) {
          this.#log('debug', `not closed within ${this.closeTimeout} ms, tearing down`);
          this.abort();
        }
      }, this.closeTimeout);
      this.#updateInput();
    }
    return this.#closed.promise;
  }

  #sendClose(code: number): void {
    try {
      this.#connection.close(code);
    } catch (error) {
      this.#log('debug', `close failed: ${error}`, error);
      if (code !== normalClosure) {
        this.#sendClose(normalClosure);
      }
    }
  }

  #stopCloseTimer(): void {
    if (this.#closeTimer !== undefined) {
      this.#clock.clearTimeout(this.#closeTimer);
      this.#closeTimer = undefined;
    }
  }

  /**
   * The WebSocket has closed. Unless {@link writable} itself was closed or
   * aborted, it is errored with why the output ended (the first failure,
   * else [reason]), so that its `closed` settles and later writes reject.
   */
  #outputEnded(reason: unknown): void {
    if (this.#closeAsked) {
      return;
    }
    this.#closeAsked = true;
    this.#writableController?.error(this.#outputEndReason ?? reason);
  }

  #log(level: LogLevel, message: string, error?: unknown): void {
    const text = `WebSocket transport${this.remote === undefined ? '' : ` ${this.remote}`}: ${message}`;
    if (error === undefined) {
      this.#logger.log(level, loggerName, text);
    } else {
      this.#logger.log(level, loggerName, text, error);
    }
  }

  toString(): string {
    return `WebSocketTransport(${this.remote ?? 'unknown'})`;
  }
}

/** The W3C WebSocket API as {@link webSocketConnection} uses it. */
export interface W3CWebSocket {
  binaryType: string;
  readonly readyState: number;
  readonly protocol: string;
  readonly bufferedAmount: number;
  onopen: ((event: Event) => unknown) | null;
  onmessage: ((event: MessageEvent) => unknown) | null;
  onerror: ((event: Event) => unknown) | null;
  onclose: ((event: CloseEvent) => unknown) | null;
  send(data: ArrayBufferView<ArrayBuffer> | ArrayBuffer): void;
  close(code?: number, reason?: string): void;
}

/** The constructor of the W3C WebSocket API. */
export type W3CWebSocketConstructor = new (url: string, protocols?: string[]) => W3CWebSocket;

/** The description of a W3C WebSocket error event, which carries no detail in browsers. */
function errorOf(event: Event): unknown {
  const detail = event as { readonly error?: unknown; readonly message?: unknown };
  if (detail.error !== undefined) {
    return detail.error;
  }
  return new Error(typeof detail.message === 'string' ? detail.message : 'WebSocket error');
}

/**
 * Adapts [socket], a W3C `WebSocket` (connecting or open), for
 * {@link WebSocketTransport}: binary messages as `ArrayBuffer`s, no pausing,
 * no output observation, no teardown beyond `close()`.
 */
export function webSocketConnection(socket: W3CWebSocket): WebSocketConnection {
  socket.binaryType = 'arraybuffer';
  return {
    get readyState() {
      return socket.readyState;
    },
    get protocol() {
      return socket.protocol;
    },
    get bufferedAmount() {
      return socket.bufferedAmount;
    },
    listen(handlers) {
      socket.onopen = () => handlers.open();
      socket.onmessage = (event) => {
        const data: unknown = event.data;
        if (typeof data === 'string') {
          handlers.message(data);
        } else if (data instanceof ArrayBuffer) {
          handlers.message(new Uint8Array(data));
        } else if (ArrayBuffer.isView(data)) {
          handlers.message(new Uint8Array(data.buffer, data.byteOffset, data.byteLength));
        } else {
          handlers.error(new TypeError('WebSocket delivered a message that is not bytes'));
        }
      };
      socket.onerror = (event) => handlers.error(errorOf(event));
      socket.onclose = (event) => handlers.close(event.code, event.reason);
    },
    send(frame) {
      socket.send(frame as Uint8Array<ArrayBuffer>);
    },
    close(code, reason) {
      socket.close(code, reason);
    },
  };
}

/** Options of {@link connectWebSocket}. Durations in milliseconds. */
export interface WebSocketConnectOptions extends WebSocketTransportOptions {
  /** The subprotocols offered; default `['switchboard']`. */
  readonly protocols?: readonly string[];
  /**
   * Bound on the opening handshake; on expiry the connect fails with
   * `UNAVAILABLE` and a connection that opens later is closed. 0 or
   * undefined: none.
   */
  readonly timeout?: number | undefined;
  /** Abandons the connect (it rejects with `signal.reason`); a connection that opens later is closed. */
  readonly signal?: AbortSignal | undefined;
  /**
   * Opens the WebSocket; default the global `WebSocket` through
   * {@link webSocketConnection}. The Node entry passes the `ws` package's.
   */
  readonly webSocket?: (url: string, protocols: readonly string[]) => WebSocketConnection;
}

/** The global `WebSocket` constructor, or undefined where there is none. */
function globalWebSocket(): W3CWebSocketConstructor | undefined {
  const ctor: unknown = (globalThis as { WebSocket?: unknown }).WebSocket;
  return typeof ctor === 'function' ? (ctor as W3CWebSocketConstructor) : undefined;
}

/**
 * Connects to [url] (`ws://` or `wss://`; a fragment is ignored), offering
 * the `switchboard` subprotocol, and resolves with the transport once the
 * opening handshake completed.
 *
 * Over the global `WebSocket` (browsers, Node 22, Bun, Deno): there the
 * receive limit applies to complete messages, after the WebSocket has
 * assembled them, and compression is the platform's choice; the Node
 * entry's `connectWebSocket` uses the `ws` package when it is installed,
 * which bounds messages before assembly and never compresses.
 *
 * Rejects with a `SwitchboardError`: `UNAVAILABLE` when the connection
 * cannot be established (the cause is the implementation's error, where it
 * gives one) or is not established within `timeout`, `UNSUPPORTED` where
 * there is no WebSocket; a `TypeError` for a URL that is not `ws://` or
 * `wss://`; `signal.reason` when [options.signal] aborts. After a timeout
 * or an abort, a connection that opens later is closed with code 1000.
 */
export async function connectWebSocket(
  url: string | URL,
  options: WebSocketConnectOptions = {},
): Promise<WebSocketTransport> {
  const signal = options.signal;
  if (signal?.aborted) {
    throw signal.reason;
  }
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch (cause) {
    throw new TypeError(`not a WebSocket URL: ${String(url)}`, { cause });
  }
  if (parsed.protocol !== 'ws:' && parsed.protocol !== 'wss:') {
    throw new TypeError(`not a ws:// or wss:// URL: ${parsed.href}`);
  }
  parsed.hash = '';
  const target = parsed.href;
  const timeout = options.timeout ?? 0;
  if (!(timeout >= 0)) {
    throw new RangeError(`timeout must not be negative, got ${timeout}`);
  }
  const protocols = [...(options.protocols ?? [webSocketSubprotocol])];
  let open = options.webSocket;
  if (open === undefined) {
    const ctor = globalWebSocket();
    if (ctor === undefined) {
      throw SwitchboardError.of(StatusCode.unsupported, 'there is no WebSocket in this runtime');
    }
    open = (where, offered) => webSocketConnection(new ctor(where, [...offered]));
  }
  let connection: WebSocketConnection;
  try {
    connection = open(target, protocols);
  } catch (cause) {
    throw SwitchboardError.of(
      StatusCode.unavailable,
      `cannot connect to ${target}: ${describe(cause)}`,
      { cause },
    );
  }
  const transport = new WebSocketTransport(connection, {
    ...options,
    remote: options.remote ?? target,
  });
  const clock = options.clock ?? systemClock;
  const abandon = (): void => {
    // Closes it once it opens, or tears it down after the close timeout.
    void transport.writable.close().catch(ignore);
  };
  const outcome = new Deferred<void>();
  let timer: TimerHandle | undefined;
  const onAbort = (): void => {
    outcome.reject(signal?.reason);
  };
  if (timeout > 0) {
    timer = clock.setTimeout(() => {
      timer = undefined;
      outcome.reject(
        SwitchboardError.of(
          StatusCode.unavailable,
          `WebSocket connection to ${target} not established within ${timeout} ms`,
        ),
      );
    }, timeout);
  }
  signal?.addEventListener('abort', onAbort, { once: true });
  transport.opened.then(
    () => outcome.resolve(),
    (cause: unknown) =>
      outcome.reject(
        cause instanceof SwitchboardError
          ? cause
          : SwitchboardError.of(
              StatusCode.unavailable,
              `cannot connect to ${target}: ${describe(cause)}`,
              { cause },
            ),
      ),
  );
  try {
    await outcome.promise;
  } catch (error) {
    abandon();
    throw error;
  } finally {
    if (timer !== undefined) {
      clock.clearTimeout(timer);
    }
    signal?.removeEventListener('abort', onAbort);
  }
  return transport;
}

function describe(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
