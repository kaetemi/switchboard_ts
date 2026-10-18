/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * The stream binding over TCP on Node (and Bun and Deno through their
 * `node:net`): {@link connectTcp}, {@link listenTcp} and
 * {@link tcpTransport} over a `net.Socket`. Ports the TCP parts of
 * `stream_transport.dart` (`connectTcp`, `fromSocket`) and the TCP
 * listener of `switchboard.dart`.
 */

import { createServer, connect as netConnect, type Server, type Socket } from 'node:net';
import { StatusCode, SwitchboardError } from '../../core/status.ts';
import type { ByteInput, ByteOutput } from '../../core/transport/byte-io.ts';
import {
  StreamTransport,
  type StreamTransportOptions,
} from '../../core/transport/stream-transport.ts';
import { AsyncQueue } from '../../core/util/async-queue.ts';
import { type Clock, systemClock, type TimerHandle } from '../../core/util/clock.ts';
import { Deferred } from '../../core/util/deferred.ts';
import { noopLogger } from '../../core/util/logger.ts';

const loggerName = 'Switchboard.Transport';

/** Options of {@link tcpTransport}: those of the stream transport but its socket hooks. */
export type TcpTransportOptions = Omit<StreamTransportOptions, 'abort' | 'linger'>;

/** Options of {@link connectTcp}. Durations in milliseconds. */
export interface TcpConnectOptions extends TcpTransportOptions {
  /** Bound on establishing the connection; 0 or undefined: none (the system's). */
  readonly timeout?: number | undefined;
  /** Abandons the attempt (the connect rejects with `signal.reason`). */
  readonly signal?: AbortSignal | undefined;
}

/** Options of {@link listenTcp}. */
export interface TcpListenOptions extends TcpTransportOptions {
  /**
   * Takes each accepted connection, with the peer's `address:port`.
   * Without it, accepted transports wait in {@link TcpListener.transports}.
   */
  readonly onTransport?: ((transport: TcpTransport, remote: string) => void) | undefined;
  /** The pending-connection queue length of `listen`; default the system's. */
  readonly backlog?: number | undefined;
}

/** An accepted connection: the transport and the peer's `address:port`. */
export interface AcceptedTransport<T> {
  readonly transport: T;
  readonly remote: string;
}

/** `address:port` of [socket]'s peer, as Dart's `Switchboard` describes it; `unknown` once closed. */
export function socketRemote(socket: Socket): string {
  const address = socket.remoteAddress;
  return address === undefined ? 'unknown' : `${address}:${socket.remotePort}`;
}

/** [host] for a URI authority: an IPv6 address in brackets. */
export function uriHost(host: string): string {
  return host.includes(':') && !host.startsWith('[') ? `[${host}]` : host;
}

/**
 * What a socket holds that the operating system has not taken:
 * `writableLength` while the socket is open. A destroyed socket drops
 * that buffer, but `writableLength` keeps counting it until the handle
 * has closed, some turns of the event loop later. So the backlog notes
 * what the socket held when it was destroyed: from then on nothing is
 * buffered, and those bytes stay counted as not taken.
 */
class SocketBacklog {
  readonly #socket: Socket;
  #dropped: number | undefined;

  constructor(socket: Socket) {
    this.#socket = socket;
    // A socket the connection's failure destroys reports the error a turn
    // later, before its handle closes: what it held is still counted then.
    socket.prependListener('error', () => this.#note());
  }

  /** Bytes the socket buffers for the network: 0 once it is destroyed. */
  get buffered(): number {
    this.#note();
    return this.#dropped === undefined ? this.#socket.writableLength : 0;
  }

  /** Bytes handed to the socket that the network did not take: buffered, or dropped. */
  get unsent(): number {
    this.#note();
    return this.#dropped ?? this.#socket.writableLength;
  }

  /** Destroys the socket, noting what it held first. */
  destroy(): void {
    if (this.#dropped === undefined) {
      this.#dropped = this.#socket.writableLength;
    }
    this.#socket.destroy();
  }

  #note(): void {
    if (this.#dropped === undefined && this.#socket.destroyed) {
      this.#dropped = this.#socket.writableLength;
    }
  }
}

/**
 * The receiving half of [socket], pull-based: `socket.read()` takes what
 * the socket buffered, so the socket stops reading from the network
 * while nobody reads (Node's own back-pressure). Cancelling destroys the
 * socket: the stream transport cancels its input only once the output is
 * finished, or when it tears the connection down.
 */
function socketInput(socket: Socket, backlog: SocketBacklog): ByteInput {
  let failure: { readonly error: unknown } | undefined;
  let ended = false;
  let cancelled = false;
  let waiter: Deferred<void> | undefined;
  const wake = (): void => {
    const w = waiter;
    waiter = undefined;
    w?.resolve();
  };
  socket.on('readable', wake);
  socket.on('end', () => {
    ended = true;
    wake();
  });
  socket.on('close', () => {
    ended = true;
    wake();
  });
  socket.on('error', (error: unknown) => {
    failure ??= { error };
    wake();
  });
  return {
    async read() {
      for (;;) {
        if (cancelled) {
          return undefined;
        }
        const chunk: unknown = socket.read();
        if (chunk instanceof Uint8Array) {
          return new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.byteLength);
        }
        if (failure !== undefined) {
          throw failure.error;
        }
        if (ended || socket.readableEnded || socket.destroyed) {
          return undefined;
        }
        waiter ??= new Deferred();
        await waiter.promise;
      }
    },
    cancel() {
      if (cancelled) {
        return;
      }
      cancelled = true;
      wake();
      backlog.destroy();
    },
  };
}

/**
 * The sending half of [socket]: takes bytes while the socket does not ask
 * its writer to wait (`writableNeedDrain`), so that what does not fit
 * waits, counted, in the stream transport. Closing ends the socket (a FIN
 * after what was written) and resolves once that is done.
 */
function socketOutput(socket: Socket, backlog: SocketBacklog): ByteOutput {
  let failed = false;
  let finished = false;
  let failure: ((error: unknown) => void) | undefined;
  let readyWaiter: Deferred<void> | undefined;
  const signalReady = (): void => {
    const w = readyWaiter;
    readyWaiter = undefined;
    w?.resolve();
  };
  const fail = (error: unknown): void => {
    if (failed) {
      return;
    }
    failed = true;
    signalReady();
    failure?.(error);
  };
  socket.on('drain', signalReady);
  socket.on('finish', () => {
    finished = true;
  });
  socket.on('error', fail);
  socket.on('close', () => {
    signalReady();
    if (!finished) {
      fail(new Error('socket closed before its output was finished'));
    }
  });
  const canWrite = (): boolean =>
    !failed && !socket.destroyed && !socket.writableEnded && !socket.writableNeedDrain;
  return {
    get canWrite() {
      return canWrite();
    },
    ready() {
      if (canWrite() || failed || socket.destroyed) {
        return Promise.resolve();
      }
      readyWaiter ??= new Deferred();
      return readyWaiter.promise;
    },
    write(bytes) {
      if (failed || socket.destroyed || socket.writableEnded) {
        return;
      }
      socket.write(bytes);
    },
    close() {
      return new Promise<void>((resolve, reject) => {
        if (finished) {
          resolve();
          return;
        }
        if (failed || socket.destroyed) {
          reject(new Error('socket output failed'));
          return;
        }
        const onFinish = (): void => {
          cleanup();
          resolve();
        };
        const onError = (error: unknown): void => {
          cleanup();
          reject(error);
        };
        const onClose = (): void => {
          cleanup();
          if (finished) {
            resolve();
          } else {
            reject(new Error('socket closed before its output was finished'));
          }
        };
        const cleanup = (): void => {
          socket.off('finish', onFinish);
          socket.off('error', onError);
          socket.off('close', onClose);
        };
        socket.on('finish', onFinish);
        socket.on('error', onError);
        socket.on('close', onClose);
        socket.end();
      });
    },
    abort() {
      backlog.destroy();
    },
    onFailure(callback) {
      failure = callback;
    },
  };
}

/**
 * The stream binding over a connected `net.Socket`, as
 * {@link tcpTransport} makes it: a {@link StreamTransport} whose output
 * accounting includes what the socket itself buffers.
 */
export class TcpTransport extends StreamTransport {
  /** The socket. */
  readonly socket: Socket;
  readonly #backlog: SocketBacklog;

  /** Prefer {@link tcpTransport}, which also sets `TCP_NODELAY`. */
  constructor(socket: Socket, options: TcpTransportOptions = {}) {
    const backlog = new SocketBacklog(socket);
    super(socketInput(socket, backlog), socketOutput(socket, backlog), {
      ...options,
      remote: options.remote ?? socketRemote(socket),
      linger: true,
      abort: () => backlog.destroy(),
    });
    this.socket = socket;
    this.#backlog = backlog;
  }

  /**
   * Bytes written that the network has not taken yet: the transport's
   * queue and the socket's buffer. 0 from the moment the transport aborts
   * (or the socket is destroyed), which drops both.
   */
  override get bufferedOutputBytes(): number {
    return super.bufferedOutputBytes + this.#backlog.buffered;
  }

  /**
   * Total bytes the operating system has taken, preamble and length
   * prefixes included. What a destroyed socket dropped is never counted.
   */
  override get acceptedOutputBytes(): number {
    return Math.max(0, super.acceptedOutputBytes - this.#backlog.unsent);
  }

  override toString(): string {
    return `TcpTransport(${this.remote ?? 'unknown'})`;
  }
}

/**
 * Runs the stream binding over [socket], a connected `net.Socket`, with
 * `TCP_NODELAY` on (Dart's `StreamTransport.fromSocket`).
 *
 * As Dart's, the transport lingers when it closes: after its last bytes
 * and the end of its output (a FIN), it keeps reading and dropping input
 * until the peer closes its side, for at most `closeTimeout` (then the
 * socket is destroyed), because closing a socket with unread input makes
 * the system reset the connection, and a reset can destroy the frames
 * still on their way, such as the GOAWAY sent just before the close.
 * `abort()` destroys the socket. Output the socket does not take at once
 * waits in the transport; {@link TcpTransport.bufferedOutputBytes} counts
 * it with the socket's own buffer, and while more than
 * `outputHighWaterMark` bytes wait the socket is not read.
 *
 * The socket should be created with `allowHalfOpen: true`, so that the
 * peer's FIN does not end this side before the transport has written its
 * last frames; {@link connectTcp} and {@link listenTcp} do.
 *
 * @throws {RangeError} for an option out of range.
 */
export function tcpTransport(socket: Socket, options: TcpTransportOptions = {}): TcpTransport {
  try {
    socket.setNoDelay(true);
  } catch (error) {
    (options.logger ?? noopLogger).log(
      'debug',
      loggerName,
      `cannot set TCP_NODELAY: ${error}`,
      error,
    );
  }
  return new TcpTransport(socket, options);
}

function abortReason(signal: AbortSignal): unknown {
  return signal.reason;
}

/**
 * Connects over TCP to [host] and [port] and runs the stream binding over
 * the socket ({@link tcpTransport}).
 *
 * Rejects with a `SwitchboardError` `UNAVAILABLE` when the connection
 * cannot be established (the socket error is its `cause`) or not within
 * [options.timeout], with `signal.reason` when [options.signal] aborts
 * (the attempt is then abandoned and its socket destroyed), and with a
 * `RangeError` for a port outside 0 to 65535 or an option out of range.
 */
export function connectTcp(
  host: string,
  port: number,
  options: TcpConnectOptions = {},
): Promise<TcpTransport> {
  return new Promise<TcpTransport>((resolve, reject) => {
    if (!Number.isInteger(port) || port < 0 || port > 65535) {
      reject(new RangeError(`port ${port} is outside 0 to 65535`));
      return;
    }
    const signal = options.signal;
    if (signal?.aborted) {
      reject(abortReason(signal));
      return;
    }
    const timeout = options.timeout ?? 0;
    if (!(timeout >= 0)) {
      reject(new RangeError(`timeout must not be negative, got ${timeout}`));
      return;
    }
    const clock: Clock = options.clock ?? systemClock;
    const target = `tcp://${uriHost(host)}:${port}`;
    const bare = host.startsWith('[') && host.endsWith(']') ? host.slice(1, -1) : host;
    let socket: Socket;
    try {
      socket = netConnect({ host: bare, port, allowHalfOpen: true, noDelay: true });
    } catch (error) {
      reject(
        SwitchboardError.of(StatusCode.unavailable, `cannot connect to ${target}: ${error}`, {
          cause: error,
        }),
      );
      return;
    }
    let timer: TimerHandle | undefined;
    const settle = (): void => {
      socket.off('connect', onConnect);
      socket.off('error', onError);
      signal?.removeEventListener('abort', onAbort);
      if (timer !== undefined) {
        clock.clearTimeout(timer);
        timer = undefined;
      }
    };
    const fail = (error: unknown): void => {
      settle();
      // A late 'error' of the abandoned socket must not go unhandled.
      socket.on('error', () => {});
      socket.destroy();
      reject(error);
    };
    const onConnect = (): void => {
      settle();
      let transport: TcpTransport;
      try {
        transport = tcpTransport(socket, options);
      } catch (error) {
        socket.destroy();
        reject(error);
        return;
      }
      resolve(transport);
    };
    const onError = (error: Error): void => {
      fail(
        SwitchboardError.of(
          StatusCode.unavailable,
          `cannot connect to ${target}: ${error.message}`,
          { cause: error },
        ),
      );
    };
    const onAbort = (): void => {
      fail(abortReason(signal as AbortSignal));
    };
    socket.once('connect', onConnect);
    socket.once('error', onError);
    signal?.addEventListener('abort', onAbort, { once: true });
    if (timeout > 0) {
      timer = clock.setTimeout(() => {
        timer = undefined;
        fail(
          SwitchboardError.of(
            StatusCode.unavailable,
            `cannot connect to ${target}: not connected within ${timeout} ms`,
          ),
        );
      }, timeout);
    }
  });
}

/**
 * Accepts TCP connections and runs the stream binding over each
 * ({@link listenTcp}). The accepted transports go to the `onTransport`
 * option or, without one, wait in {@link transports}.
 */
export class TcpListener implements AsyncDisposable {
  /** The server. */
  readonly server: Server;
  /** The bound `tcp://address:port` endpoint, with the actual port. */
  readonly endpoint: string;
  /** The address bound to, as the system reports it (`127.0.0.1`, `::`, `0.0.0.0`). */
  readonly address: string;
  /** The bound port. */
  readonly port: number;

  readonly #queue: AsyncQueue<AcceptedTransport<TcpTransport>>;
  #closed: Promise<void> | undefined;

  /** Use {@link listenTcp}: a listener over [server], bound to [address] and [port], delivering to [queue]. */
  constructor(
    server: Server,
    address: string,
    port: number,
    queue: AsyncQueue<AcceptedTransport<TcpTransport>>,
  ) {
    this.server = server;
    this.address = address;
    this.port = port;
    this.endpoint = `tcp://${uriHost(address)}:${port}`;
    this.#queue = queue;
  }

  /** The accepted connections when the listener has no `onTransport`; ends once closed. */
  get transports(): AsyncIterable<AcceptedTransport<TcpTransport>> {
    return this.#queue;
  }

  /** Whether {@link close} was called. */
  get isClosed(): boolean {
    return this.#closed !== undefined;
  }

  /**
   * Stops accepting; the connections accepted stay open, and those still
   * waiting in {@link transports} are delivered. Idempotent; never rejects.
   */
  close(): Promise<void> {
    this.#closed ??= new Promise<void>((resolve) => {
      this.#queue.end();
      try {
        // Stops listening at once; the callback would wait for every
        // connection accepted, which stays the owner's.
        this.server.close();
      } catch {
        // Not listening any more.
      }
      queueMicrotask(resolve);
    });
    return this.#closed;
  }

  /** {@link close}, for `await using`. */
  [Symbol.asyncDispose](): Promise<void> {
    return this.close();
  }

  toString(): string {
    return `TcpListener(${this.endpoint})`;
  }
}

/** Listens with [server] on [host] and [port]; resolves with the bound address and port. */
export function listenOn(
  server: Server,
  host: string,
  port: number,
  backlog?: number,
): Promise<{ address: string; port: number }> {
  return new Promise((resolve, reject) => {
    if (!Number.isInteger(port) || port < 0 || port > 65535) {
      reject(new RangeError(`port ${port} is outside 0 to 65535`));
      return;
    }
    const bare = host.startsWith('[') && host.endsWith(']') ? host.slice(1, -1) : host;
    const onError = (error: Error): void => {
      server.off('listening', onListening);
      reject(error);
    };
    const onListening = (): void => {
      server.off('error', onError);
      const bound = server.address();
      if (bound === null || typeof bound === 'string') {
        reject(new Error(`server bound to ${String(bound)}`));
        return;
      }
      resolve({ address: bound.address, port: bound.port });
    };
    server.once('error', onError);
    server.once('listening', onListening);
    server.listen({ host: bare, port, ...(backlog === undefined ? {} : { backlog }) });
  });
}

/**
 * Starts accepting TCP connections on [host] (an address, or `0.0.0.0` or
 * `::` for every interface; a name is resolved) and [port] (0 picks a
 * free one), each run through {@link tcpTransport} with [options].
 *
 * Resolves with the listener once it is bound; its endpoint is
 * `tcp://address:port` with the bound address and port. Rejects with the
 * system's error when it cannot bind (`EADDRINUSE`, say), and with a
 * `RangeError` for a port outside 0 to 65535.
 */
export async function listenTcp(
  host: string,
  port: number,
  options: TcpListenOptions = {},
): Promise<TcpListener> {
  const { onTransport, backlog, ...transportOptions } = options;
  const logger = options.logger ?? noopLogger;
  const queue = new AsyncQueue<AcceptedTransport<TcpTransport>>();
  const server = createServer({ allowHalfOpen: true, noDelay: true }, (socket) => accept(socket));
  const accept = (socket: Socket): void => {
    const remote = socketRemote(socket);
    let transport: TcpTransport;
    try {
      transport = tcpTransport(socket, { ...transportOptions, remote });
    } catch (error) {
      logger.log('warn', loggerName, `cannot accept ${remote}: ${error}`, error);
      socket.destroy();
      return;
    }
    if (onTransport !== undefined) {
      try {
        onTransport(transport, remote);
      } catch (error) {
        logger.log('warn', loggerName, `the TCP listener's handler failed for ${remote}`, error);
        transport.abort();
      }
      return;
    }
    queue.push({ transport, remote });
  };
  server.on('error', (error) => {
    logger.log('warn', loggerName, `TCP listener failed: ${error}`, error);
  });
  const bound = await listenOn(server, host, port, backlog);
  return new TcpListener(server, bound.address, bound.port, queue);
}
