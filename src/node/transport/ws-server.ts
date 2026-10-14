/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * The server side of the WebSocket binding on Node, over the `ws`
 * package (an optional peer dependency, loaded on first use):
 * {@link acceptWebSocket} upgrades a request of an application's own
 * `http.Server`, {@link listenWebSocket} runs a server of its own. Ports
 * `web_socket_server.dart` (the upgrade checks, the limits, the ping
 * interval) and the WebSocket listener of `switchboard.dart`.
 *
 * Dart reads the socket with an RFC 6455 implementation of its own, to
 * bound a message before it is assembled; `ws` does the same with
 * `maxPayload` (checked against every fragment's header), and negotiates
 * no compression with `perMessageDeflate: false`.
 */

import {
  createServer,
  type IncomingMessage,
  type Server,
  type ServerResponse,
  STATUS_CODES,
} from 'node:http';
import type { Duplex } from 'node:stream';
import { StatusCode, SwitchboardError } from '../../core/status.ts';
import { webSocketSubprotocol } from '../../core/transport/contract.ts';
import {
  WebSocketTransport,
  type WebSocketTransportOptions,
} from '../../core/transport/websocket-transport.ts';
import { AsyncQueue } from '../../core/util/async-queue.ts';
import { type Clock, systemClock, type TimerHandle } from '../../core/util/clock.ts';
import { type Logger, noopLogger } from '../../core/util/logger.ts';
import { type AcceptedTransport, listenOn, uriHost } from './tcp.ts';
import { requireWs, wsConnection } from './ws-common.ts';

const loggerName = 'Switchboard.Transport';

/** Options of {@link acceptWebSocket}. Durations in milliseconds. */
export interface WebSocketAcceptOptions
  extends Omit<WebSocketTransportOptions, 'closeCodeFor' | 'remote'> {
  /**
   * Checks the client's liveness at this interval: after an interval in
   * which nothing arrived, a ping is sent, and when two further intervals
   * pass without anything arriving (pongs included) the connection is torn
   * down. Intervals in which reading had not started, or was paused while
   * a frame waited for its reader, or the client was reading output that
   * held back our reading, count as signs of life. Undefined (the
   * default): none, liveness is the mux keep-alive's.
   */
  readonly pingInterval?: number | undefined;
}

/** A WebSocket accepted by {@link acceptWebSocket}. */
export interface AcceptedWebSocket {
  /** The transport. */
  readonly transport: WebSocketTransport;
  /** The peer's `address:port`. */
  readonly remote: string;
}

/**
 * The close code the server sends after an input failure of its own
 * finding: 1003 for a text message, 1009 for a message too large. `ws`
 * closes on the violations it finds itself (1002, 1007, 1009) before the
 * transport hears of them.
 */
function serverCloseCode(error: unknown): number {
  if (error instanceof SwitchboardError) {
    if (error.code === StatusCode.frameTooLarge) {
      return 1009;
    }
    if (error.code === StatusCode.protocolError) {
      return 1003;
    }
  }
  return 1000;
}

/** Whether the comma-separated values of header [name] of [request] include [token], ignoring case. */
function hasToken(request: IncomingMessage, name: string, token: string): boolean {
  for (const value of request.headersDistinct[name] ?? []) {
    for (const part of value.split(',')) {
      if (part.trim().toLowerCase() === token) {
        return true;
      }
    }
  }
  return false;
}

/** The value of header [name] of [request] if it occurs exactly once, else undefined. */
function single(request: IncomingMessage, name: string): string | undefined {
  const values = request.headersDistinct[name];
  return values?.length === 1 ? (values[0] as string).trim() : undefined;
}

/** A key is the base64 encoding of 16 bytes. */
function validKey(key: string): boolean {
  if (!/^[A-Za-z0-9+/]{22}==$/.test(key)) {
    return false;
  }
  return Buffer.from(key, 'base64').length === 16;
}

/** The subprotocols [request] offers in `Sec-WebSocket-Protocol`. */
export function offeredProtocols(request: IncomingMessage): Set<string> {
  const offered = new Set<string>();
  for (const value of request.headersDistinct['sec-websocket-protocol'] ?? []) {
    for (const part of value.split(',')) {
      const protocol = part.trim();
      if (protocol !== '') {
        offered.add(protocol);
      }
    }
  }
  return offered;
}

/**
 * Answers the upgrade request on [socket] with [status] and closes the
 * connection. Never throws.
 */
export function refuseUpgrade(
  socket: Duplex,
  status: number,
  message = STATUS_CODES[status] ?? '',
  headers: Readonly<Record<string, string>> = {},
): void {
  try {
    if (!socket.writable) {
      socket.destroy();
      return;
    }
    const body = Buffer.from(message, 'utf8');
    const lines = [
      `HTTP/1.1 ${status} ${STATUS_CODES[status] ?? ''}`,
      'Connection: close',
      'Content-Type: text/plain; charset=utf-8',
      `Content-Length: ${body.length}`,
      ...Object.entries(headers).map(([name, value]) => `${name}: ${value}`),
      '',
      '',
    ];
    socket.end(Buffer.concat([Buffer.from(lines.join('\r\n'), 'latin1'), body]));
  } catch {
    socket.destroy();
  }
}

/**
 * Checks [request] as Dart's `WebSocketServerTransport.upgrade` does:
 * the subprotocol to select, or the refusal, answered on [socket].
 */
function checkUpgrade(
  request: IncomingMessage,
  socket: Duplex,
):
  | { readonly protocol: string | undefined }
  | { readonly refused: string; readonly status: number } {
  if (
    request.method !== 'GET' ||
    request.httpVersion !== '1.1' ||
    !hasToken(request, 'upgrade', 'websocket') ||
    !hasToken(request, 'connection', 'upgrade')
  ) {
    const reason = 'WebSocket upgrade required';
    refuseUpgrade(socket, 426, reason, { Upgrade: 'websocket' });
    return { refused: reason, status: 426 };
  }
  if (single(request, 'sec-websocket-version') !== '13') {
    const reason = 'unsupported WebSocket version';
    refuseUpgrade(socket, 426, reason, { 'Sec-WebSocket-Version': '13' });
    return { refused: reason, status: 426 };
  }
  const key = single(request, 'sec-websocket-key');
  if (key === undefined || !validKey(key)) {
    const reason = 'missing or malformed Sec-WebSocket-Key';
    refuseUpgrade(socket, 400, reason);
    return { refused: reason, status: 400 };
  }
  const offered = offeredProtocols(request);
  if (offered.size > 0 && !offered.has(webSocketSubprotocol)) {
    const reason = `the ${webSocketSubprotocol} subprotocol is required`;
    refuseUpgrade(socket, 400, reason);
    return { refused: reason, status: 400 };
  }
  return { protocol: offered.size > 0 ? webSocketSubprotocol : undefined };
}

/** `address:port` of the peer of [socket], or `unknown`. */
function remoteOf(socket: Duplex): string {
  const net = socket as { readonly remoteAddress?: string; readonly remotePort?: number };
  return net.remoteAddress === undefined ? 'unknown' : `${net.remoteAddress}:${net.remotePort}`;
}

/** Runs the ping interval of [options] on [transport]; stops when it closes. */
function startPings(
  transport: WebSocketTransport,
  ping: () => void,
  activity: { seen: boolean },
  interval: number,
  clock: Clock,
  logger: Logger,
): void {
  let silent = 0;
  let lastAccepted = -1;
  let timer: TimerHandle | undefined;
  let stopped = false;
  void transport.closed.then(() => {
    stopped = true;
    if (timer !== undefined) {
      clock.clearTimeout(timer);
    }
  });
  const tick = (): void => {
    timer = undefined;
    if (stopped) {
      return;
    }
    const accepted = transport.acceptedOutputBytes;
    const throttled = transport.isInputThrottled ?? false;
    const seen = activity.seen;
    activity.seen = false;
    const alive =
      seen ||
      // Not reading (not yet, or paused), so silence says nothing about the
      // client; or the client is reading output that holds back our reading.
      !transport.isInputStarted ||
      (transport.isInputPaused && !throttled) ||
      (throttled && accepted !== lastAccepted);
    lastAccepted = accepted;
    if (alive) {
      silent = 0;
    } else if (++silent > 2) {
      logger.log('info', loggerName, `${transport}: no answer to pings, aborting`);
      transport.abort();
      return;
    } else {
      try {
        ping();
      } catch (error) {
        logger.log('debug', loggerName, `${transport}: ping failed: ${error}`, error);
      }
    }
    timer = clock.setTimeout(tick, interval);
  };
  timer = clock.setTimeout(tick, interval);
}

/**
 * Upgrades [request], an `upgrade` event of an `http.Server` with its
 * [socket] and [head], to a WebSocket and runs the binding over it. The
 * path is not checked; routing is the caller's.
 *
 * The handshake is RFC 6455's, with Dart's checks first: a request that is
 * not a WebSocket upgrade (not a `GET` over HTTP/1.1 with
 * `Upgrade: websocket` and `Connection: Upgrade`) is answered
 * `426 Upgrade Required` with `Upgrade: websocket`; a
 * `Sec-WebSocket-Version` other than 13 gets 426 with
 * `Sec-WebSocket-Version: 13`; a missing or malformed `Sec-WebSocket-Key`,
 * or a client offering subprotocols but not `switchboard`, gets
 * `400 Bad Request`. Otherwise the response is `101 Switching Protocols`,
 * selecting `switchboard` when offered (a client offering none is
 * accepted) and no extension, compression included.
 *
 * The transport then behaves as {@link WebSocketTransport} does, over `ws`:
 * a message over `maxFrameSize` (default 1 MiB; 0 for no limit), counted
 * over its fragments before they are buffered, fails it with
 * `FRAME_TOO_LARGE` and closes with 1009; a WebSocket protocol violation
 * fails it with a `ProtocolError` (1002, 1007 for bad UTF-8 in a close
 * reason); a text message with a `ProtocolError` and close code 1003.
 * Pings are answered; a close from the client is echoed. Unlike Dart's,
 * a message over the limit is refused by `ws` at once, so the mux has no
 * chance to send its GOAWAY before the close frame.
 *
 * Rejects with a `SwitchboardError` `INVALID_ARGUMENT` for a refused
 * upgrade (after answering it), `UNSUPPORTED` when the `ws` package is
 * not installed (the socket is answered 501), and with a `RangeError` for
 * an option out of range (before touching the request).
 */
export async function acceptWebSocket(
  request: IncomingMessage,
  socket: Duplex,
  head: Uint8Array,
  options: WebSocketAcceptOptions = {},
): Promise<AcceptedWebSocket> {
  const max = options.maxFrameSize ?? WebSocketTransport.defaultMaxFrameSize;
  if (!Number.isInteger(max) || max < 0) {
    throw new RangeError(`maxFrameSize must be a non-negative integer, got ${max}`);
  }
  const interval = options.pingInterval;
  if (interval !== undefined && !(interval > 0)) {
    throw new RangeError(`pingInterval must be positive, got ${interval}`);
  }
  let ws: Awaited<ReturnType<typeof requireWs>>;
  try {
    ws = await requireWs('a WebSocket listener');
  } catch (error) {
    refuseUpgrade(socket, 501, 'WebSocket is not available');
    throw error;
  }
  const checked = checkUpgrade(request, socket);
  if ('refused' in checked) {
    throw SwitchboardError.of(
      StatusCode.invalidArgument,
      `upgrade refused (${checked.status}): ${checked.refused}`,
    );
  }
  const remote = remoteOf(socket);
  const server = new ws.WebSocketServer({
    noServer: true,
    clientTracking: false,
    perMessageDeflate: false,
    maxPayload: max,
    handleProtocols: (protocols) =>
      protocols.has(webSocketSubprotocol) ? webSocketSubprotocol : false,
  });
  const upgraded = await new Promise<import('ws').WebSocket>((resolve, reject) => {
    const onClose = (): void => {
      reject(
        SwitchboardError.of(StatusCode.invalidArgument, 'upgrade refused: the connection closed'),
      );
    };
    socket.once('close', onClose);
    server.on('wsClientError', (error, client) => {
      socket.off('close', onClose);
      refuseUpgrade(client, 400, error.message);
      reject(
        SwitchboardError.of(StatusCode.invalidArgument, `upgrade refused: ${error.message}`, {
          cause: error,
        }),
      );
    });
    try {
      server.handleUpgrade(request, socket, Buffer.from(head), (client) => {
        socket.off('close', onClose);
        resolve(client);
      });
    } catch (error) {
      socket.off('close', onClose);
      refuseUpgrade(socket, 500);
      reject(error);
    }
  });
  const activity = { seen: false };
  const connection = wsConnection(
    upgraded,
    interval === undefined ? {} : { onActivity: () => (activity.seen = true) },
  );
  const transport = new WebSocketTransport(connection, {
    ...options,
    remote,
    closeCodeFor: serverCloseCode,
  });
  if (interval !== undefined) {
    startPings(
      transport,
      () => upgraded.ping(),
      activity,
      interval,
      options.clock ?? systemClock,
      options.logger ?? noopLogger,
    );
  }
  return { transport, remote };
}

/** [path] with a leading slash and without trailing slashes (`/` stays). */
export function normalizeWebSocketPath(path: string): string {
  let p = path.startsWith('/') ? path : `/${path}`;
  while (p.length > 1 && p.endsWith('/')) {
    p = p.slice(0, -1);
  }
  return p;
}

/** Whether the target of [request] is [base] (normalised), with or without a trailing slash; the query is ignored. */
export function isRequestFor(request: IncomingMessage, base: string): boolean {
  let path: string;
  try {
    path = new URL(request.url ?? '/', 'http://localhost').pathname;
  } catch {
    return false;
  }
  return path === base || path === `${base === '/' ? '' : base}/`;
}

/**
 * Runs an application's [handler] for a request a listener does not serve.
 * The handler owns the response; if it fails before starting it, the
 * request is answered 500.
 */
export async function handOverRequest(
  request: IncomingMessage,
  response: ServerResponse,
  handler: (request: IncomingMessage, response: ServerResponse) => unknown,
  logger: Logger,
): Promise<void> {
  try {
    await handler(request, response);
  } catch (error) {
    logger.log('error', loggerName, `handler for ${request.method} ${request.url} failed`, error);
    if (!response.headersSent) {
      response.statusCode = 500;
      response.end();
    }
  }
}

/**
 * Runs an application's [handler] for an upgrade request a listener does
 * not serve. The handler owns the socket; if it fails, the request is
 * answered 500 where the socket still takes it.
 */
export async function handOverUpgrade(
  request: IncomingMessage,
  socket: Duplex,
  head: Uint8Array,
  handler: (request: IncomingMessage, socket: Duplex, head: Uint8Array) => unknown,
  logger: Logger,
): Promise<void> {
  try {
    await handler(request, socket, head);
  } catch (error) {
    logger.log('error', loggerName, `upgrade handler for ${request.url} failed`, error);
    refuseUpgrade(socket, 500);
  }
}

/** Options of {@link listenWebSocket}. */
export interface WebSocketListenOptions extends WebSocketAcceptOptions {
  /** The path upgrades are accepted at, with or without a trailing slash; default `/`. */
  readonly path?: string | undefined;
  /**
   * Takes each accepted connection, with the peer's `address:port`.
   * Without it, accepted transports wait in {@link WebSocketListener.transports}.
   */
  readonly onTransport?: ((transport: WebSocketTransport, remote: string) => void) | undefined;
  /**
   * Serves the HTTP requests for other paths (OAuth return routes,
   * `.well-known` files); it owns the response. Without it they get 403. A
   * handler that fails before answering makes it 500.
   */
  readonly onOtherRequest?:
    | ((request: IncomingMessage, response: ServerResponse) => unknown)
    | undefined;
  /** Serves the upgrade requests for other paths; it owns the socket. Without it they get 403. */
  readonly onOtherUpgrade?:
    | ((request: IncomingMessage, socket: Duplex, head: Uint8Array) => unknown)
    | undefined;
}

/**
 * A WebSocket listener of its own `http.Server` ({@link listenWebSocket}).
 * The accepted transports go to the `onTransport` option or, without one,
 * wait in {@link transports}.
 */
export class WebSocketListener implements AsyncDisposable {
  /** The server. */
  readonly server: Server;
  /** The bound `ws://address:port/path` endpoint, with the actual port. */
  readonly endpoint: string;
  /** The address bound to, as the system reports it. */
  readonly address: string;
  /** The bound port. */
  readonly port: number;
  /** The path upgrades are accepted at (normalised). */
  readonly path: string;

  readonly #queue: AsyncQueue<AcceptedTransport<WebSocketTransport>>;
  #closed: Promise<void> | undefined;

  /** Use {@link listenWebSocket}. */
  constructor(
    server: Server,
    address: string,
    port: number,
    path: string,
    queue: AsyncQueue<AcceptedTransport<WebSocketTransport>>,
  ) {
    this.server = server;
    this.address = address;
    this.port = port;
    this.path = path;
    this.endpoint = `ws://${uriHost(address)}:${port}${path}`;
    this.#queue = queue;
  }

  /** The accepted connections when the listener has no `onTransport`; ends once closed. */
  get transports(): AsyncIterable<AcceptedTransport<WebSocketTransport>> {
    return this.#queue;
  }

  /**
   * Stops accepting and closes the idle HTTP connections; the WebSockets
   * accepted stay open. Idempotent; never rejects.
   */
  close(): Promise<void> {
    this.#closed ??= new Promise<void>((resolve) => {
      this.#queue.end();
      try {
        this.server.close();
        this.server.closeIdleConnections();
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
    return `WebSocketListener(${this.endpoint})`;
  }
}

/**
 * Starts accepting WebSocket connections on [host] and [port] (0 picks a
 * free one) at `options.path`, each run through {@link acceptWebSocket}
 * with [options]. Needs the `ws` package (rejects `UNSUPPORTED` without
 * it).
 *
 * Requests for the path, with or without a trailing slash, are upgraded;
 * a plain HTTP request for it gets 426, and requests for other paths go to
 * `onOtherRequest` and `onOtherUpgrade`, or get 403. Resolves with the
 * listener once bound; its endpoint is `ws://address:port/path`.
 */
export async function listenWebSocket(
  host: string,
  port: number,
  options: WebSocketListenOptions = {},
): Promise<WebSocketListener> {
  await requireWs('a WebSocket listener');
  const { path, onTransport, onOtherRequest, onOtherUpgrade, ...acceptOptions } = options;
  const logger = options.logger ?? noopLogger;
  const base = normalizeWebSocketPath(path ?? '/');
  const queue = new AsyncQueue<AcceptedTransport<WebSocketTransport>>();
  const server = createServer();
  server.on('request', (request, response) => {
    if (isRequestFor(request, base)) {
      response.writeHead(426, { Upgrade: 'websocket', 'Content-Type': 'text/plain' });
      response.end('WebSocket upgrade required');
    } else if (onOtherRequest !== undefined) {
      void handOverRequest(request, response, onOtherRequest, logger);
    } else {
      response.writeHead(403);
      response.end();
    }
  });
  server.on('upgrade', (request: IncomingMessage, socket: Duplex, head: Buffer) => {
    if (!isRequestFor(request, base)) {
      if (onOtherUpgrade !== undefined) {
        void handOverUpgrade(request, socket, head, onOtherUpgrade, logger);
      } else {
        refuseUpgrade(socket, 403);
      }
      return;
    }
    acceptWebSocket(request, socket, head, acceptOptions).then(
      ({ transport, remote }) => {
        if (onTransport === undefined) {
          queue.push({ transport, remote });
          return;
        }
        try {
          onTransport(transport, remote);
        } catch (error) {
          logger.log('warn', loggerName, `the WebSocket listener's handler failed`, error);
          transport.abort();
        }
      },
      (error: unknown) => {
        logger.log('debug', loggerName, `WebSocket request for ${request.url} failed: ${error}`);
      },
    );
  });
  server.on('error', (error) => {
    logger.log('warn', loggerName, `HTTP listener failed: ${error}`, error);
  });
  const bound = await listenOn(server, host, port);
  return new WebSocketListener(server, bound.address, bound.port, base, queue);
}
