/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Helpers for the WebSocket transport tests: a raw RFC 6455 client over a
// socket (the port of `RawClient` and the frame builders of
// `web_socket_server_test.dart`), a plain `ws` server (the port of the
// `dart:io` `HttpServer` + `WebSocketTransformer` of `transport_test.dart`)
// and a reader that keeps reading a transport.

import { createServer, type IncomingMessage, type Server } from 'node:http';
import { connect, type Socket } from 'node:net';
import type { Duplex } from 'node:stream';
import { afterEach } from 'vitest';
import { WebSocket, WebSocketServer } from 'ws';
import type { WebSocketTransport } from '../../../src/core/transport/websocket-transport.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';
import {
  type AcceptedWebSocket,
  acceptWebSocket,
  type WebSocketAcceptOptions,
} from '../../../src/node/transport/ws-server.ts';

export const opContinuation = 0x0;
export const opText = 0x1;
export const opBinary = 0x2;
export const opClose = 0x8;
export const opPing = 0x9;
export const opPong = 0xa;

/**
 * Whether this runs on Bun, whose `ws` is its own stand-in for the
 * package: it drops a connection that breaks the protocol (close 1006, no
 * close frame, no error event), does not enforce `maxPayload`, and assembles
 * a message before anyone sees it. Its tests say where they expect that.
 */
export const isBun = typeof (globalThis as { Bun?: unknown }).Bun !== 'undefined';

/** The sample nonce of RFC 6455 section 1.3 and its accept value. */
export const sampleKey = 'dGhlIHNhbXBsZSBub25jZQ==';
export const sampleAccept = 's3pPLMBiTxaQ9kYGzzhZRbK+xOo=';

const cleanups: (() => unknown)[] = [];
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) {
    await cleanup();
  }
});

/** Runs [cleanup] after the current test. */
export function onCleanup(cleanup: () => unknown): void {
  cleanups.push(cleanup);
}

/** [length] bytes of a pattern, varied by [seed]. */
export function pattern(length: number, seed = 0): Uint8Array {
  const out = new Uint8Array(length);
  for (let i = 0; i < length; i++) {
    out[i] = (i * 31 + seed) & 0xff;
  }
  return out;
}

/** Whether [a] and [b] hold the same bytes (fast for large arrays). */
export function sameBytes(a: Uint8Array | undefined, b: Uint8Array): boolean {
  return a !== undefined && Buffer.compare(Buffer.from(a), Buffer.from(b)) === 0;
}

/** A client frame: masked unless [mask] is false. */
export function clientFrame(
  opcode: number,
  payload: ArrayLike<number>,
  options: { fin?: boolean; mask?: boolean; rsv?: number } = {},
): Uint8Array {
  const fin = options.fin ?? true;
  const mask = options.mask ?? true;
  const length = payload.length;
  const header: number[] = [(fin ? 0x80 : 0) | (options.rsv ?? 0) | opcode];
  const maskBit = mask ? 0x80 : 0;
  if (length < 126) {
    header.push(maskBit | length);
  } else if (length <= 0xffff) {
    header.push(maskBit | 126, length >> 8, length & 0xff);
  } else {
    header.push(maskBit | 127);
    const big = BigInt(length);
    for (let i = 7; i >= 0; i--) {
      header.push(Number((big >> BigInt(8 * i)) & 0xffn));
    }
  }
  const key = [0x37, 0xfa, 0x21, 0x3d];
  if (mask) {
    header.push(...key);
  }
  const out = new Uint8Array(header.length + length);
  out.set(header);
  for (let i = 0; i < length; i++) {
    out[header.length + i] = mask
      ? (payload[i] as number) ^ (key[i & 3] as number)
      : (payload[i] as number);
  }
  return out;
}

/** A frame header only, claiming [length] payload bytes (a 64-bit length, masked). */
export function clientHeader(
  opcode: number,
  length: bigint,
  options: { fin?: boolean } = {},
): Uint8Array {
  const header: number[] = [((options.fin ?? true) ? 0x80 : 0) | opcode, 0x80 | 127];
  for (let i = 7; i >= 0; i--) {
    header.push(Number((length >> BigInt(8 * i)) & 0xffn));
  }
  header.push(0, 0, 0, 0);
  return new Uint8Array(header);
}

/** A close frame payload: the code, then the UTF-8 reason. */
export function closePayload(code: number, reason = ''): Uint8Array {
  const text = Buffer.from(reason, 'utf8');
  return new Uint8Array([code >> 8, code & 0xff, ...text]);
}

/** A frame received from the server. */
export interface ServerFrame {
  readonly opcode: number;
  readonly payload: Uint8Array;
  readonly fin: boolean;
}

/** The close code of a close frame, or undefined. */
export function closeCodeOf(frame: ServerFrame): number | undefined {
  const p = frame.payload;
  return p.length >= 2 ? ((p[0] as number) << 8) | (p[1] as number) : undefined;
}

/** A queue of values, read one at a time. */
export class Queue<T> {
  readonly #items: T[] = [];
  #ended = false;
  #error: { readonly error: unknown } | undefined;
  #waiter: Deferred<void> | undefined;

  push(item: T): void {
    this.#items.push(item);
    this.#waiter?.resolve();
  }

  end(): void {
    this.#ended = true;
    this.#waiter?.resolve();
  }

  fail(error: unknown): void {
    this.#error ??= { error };
    this.#ended = true;
    this.#waiter?.resolve();
  }

  get length(): number {
    return this.#items.length;
  }

  /** The next value; throws at the end, or with the error once the values before it are taken. */
  async next(): Promise<T> {
    for (;;) {
      if (this.#items.length > 0) {
        return this.#items.shift() as T;
      }
      if (this.#error !== undefined) {
        throw this.#error.error;
      }
      if (this.#ended) {
        throw new Error('queue ended');
      }
      this.#waiter = new Deferred();
      await this.#waiter.promise;
      this.#waiter = undefined;
    }
  }

  /** Whether another value comes (Dart's `hasNext`); false at the end or on an error. */
  async hasNext(): Promise<boolean> {
    for (;;) {
      if (this.#items.length > 0) {
        return true;
      }
      if (this.#ended) {
        return false;
      }
      this.#waiter = new Deferred();
      await this.#waiter.promise;
      this.#waiter = undefined;
    }
  }
}

/** Reads [transport] from now on into a queue: Dart's `eager`. */
export function eager(transport: {
  readonly readable: ReadableStream<Uint8Array>;
}): Queue<Uint8Array> {
  const queue = new Queue<Uint8Array>();
  void (async () => {
    try {
      for await (const frame of transport.readable) {
        queue.push(frame);
      }
      queue.end();
    } catch (error) {
      queue.fail(error);
    }
  })();
  return queue;
}

/** A WebSocket handshake request. Null drops a header. */
export function handshake(
  options: {
    path?: string;
    key?: string | null;
    version?: string | null;
    protocols?: string | null;
    extensions?: string;
    upgrade?: boolean;
  } = {},
): string {
  const key = options.key === undefined ? sampleKey : options.key;
  const version = options.version === undefined ? '13' : options.version;
  const protocols = options.protocols === undefined ? 'switchboard' : options.protocols;
  return [
    `GET ${options.path ?? '/'} HTTP/1.1`,
    'Host: localhost',
    ...(options.upgrade === false ? [] : ['Upgrade: websocket', 'Connection: keep-alive, Upgrade']),
    ...(key === null ? [] : [`Sec-WebSocket-Key: ${key}`]),
    ...(version === null ? [] : [`Sec-WebSocket-Version: ${version}`]),
    ...(protocols === null ? [] : [`Sec-WebSocket-Protocol: ${protocols}`]),
    ...(options.extensions === undefined
      ? []
      : [`Sec-WebSocket-Extensions: ${options.extensions}`]),
    '',
    '',
  ].join('\r\n');
}

/**
 * A WebSocket client over a raw socket: writes whatever it is told to and
 * parses the server's frames, checking that they are not masked. Echoes
 * the server's close frame (as clients do) unless told not to.
 */
export class RawClient {
  readonly socket: Socket;
  /** Whether pings are answered with pongs as they arrive. */
  autoPong = false;
  /** Whether a close frame from the server is echoed. */
  autoClose = true;
  readonly frames = new Queue<ServerFrame>();
  readonly closed = new Deferred<void>();
  status = 0;
  headers: Record<string, string> = {};
  #buffer = Buffer.alloc(0);
  readonly #head = new Deferred<string>();
  #headDone = false;
  #maskError: string | undefined;

  private constructor(socket: Socket) {
    this.socket = socket;
  }

  static async connect(port: number, request = handshake()): Promise<RawClient> {
    const socket = connect({ host: '127.0.0.1', port });
    const client = new RawClient(socket);
    onCleanup(() => client.destroy());
    socket.on('data', (data: Buffer) => client.#onData(data));
    socket.on('error', () => client.#onDone());
    socket.on('close', () => client.#onDone());
    await new Promise<void>((resolve, reject) => {
      socket.once('connect', resolve);
      socket.once('error', reject);
    });
    socket.write(request);
    const head = await client.#head.promise;
    const lines = head.split('\r\n');
    client.status = Number((lines[0] ?? '').split(' ')[1]);
    for (const line of lines.slice(1)) {
      const colon = line.indexOf(':');
      if (colon > 0) {
        client.headers[line.slice(0, colon).trim().toLowerCase()] = line.slice(colon + 1).trim();
      }
    }
    return client;
  }

  /** Whether a frame from the server was masked (they must not be). */
  get maskError(): string | undefined {
    return this.#maskError;
  }

  #onData(data: Buffer): void {
    this.#buffer = Buffer.concat([this.#buffer, data]);
    if (!this.#headDone) {
      const end = this.#buffer.indexOf('\r\n\r\n');
      if (end < 0) {
        return;
      }
      this.#headDone = true;
      this.#head.resolve(this.#buffer.subarray(0, end).toString('latin1'));
      this.#buffer = this.#buffer.subarray(end + 4);
    }
    this.#parse();
  }

  #parse(): void {
    let bytes = this.#buffer;
    while (bytes.length >= 2) {
      const fin = ((bytes[0] as number) & 0x80) !== 0;
      const opcode = (bytes[0] as number) & 0x0f;
      if (((bytes[1] as number) & 0x80) !== 0) {
        this.#maskError = 'a server frame was masked';
      }
      let length = (bytes[1] as number) & 0x7f;
      let offset = 2;
      if (length === 126) {
        if (bytes.length < 4) break;
        length = bytes.readUInt16BE(2);
        offset = 4;
      } else if (length === 127) {
        if (bytes.length < 10) break;
        length = Number(bytes.readBigUInt64BE(2));
        offset = 10;
      }
      if (bytes.length < offset + length) {
        break;
      }
      const payload = new Uint8Array(bytes.subarray(offset, offset + length));
      if (this.autoPong && opcode === opPing) {
        this.send(clientFrame(opPong, payload));
      }
      if (this.autoClose && opcode === opClose && !this.socket.destroyed) {
        this.send(clientFrame(opClose, payload));
      }
      this.frames.push({ opcode, payload, fin });
      bytes = bytes.subarray(offset + length);
    }
    this.#buffer = bytes;
  }

  #onDone(): void {
    if (!this.#headDone) {
      this.#headDone = true;
      this.#head.resolve(this.#buffer.toString('latin1'));
    }
    if (!this.closed.isSettled) {
      this.closed.resolve();
      this.frames.end();
    }
  }

  send(bytes: Uint8Array): void {
    if (!this.socket.destroyed && this.socket.writable) {
      this.socket.write(bytes);
    }
  }

  /** Writes [bytes] a few at a time, so that the server reads them in pieces. */
  async trickle(bytes: Uint8Array, step = 3): Promise<void> {
    for (let i = 0; i < bytes.length; i += step) {
      await new Promise<void>((resolve) =>
        this.socket.write(bytes.subarray(i, i + step), () => resolve()),
      );
      await new Promise((resolve) => setTimeout(resolve, 0));
    }
  }

  /** The next frame that is not a ping or a pong. */
  async next(): Promise<ServerFrame> {
    for (;;) {
      const frame = await this.frames.next();
      if (frame.opcode !== opPing && frame.opcode !== opPong) {
        return frame;
      }
    }
  }

  destroy(): void {
    this.socket.destroy();
  }
}

/** An HTTP server upgrading every request with {@link acceptWebSocket}. */
export class Harness {
  readonly server: Server;
  readonly accepted = new Queue<AcceptedWebSocket>();
  readonly refused: unknown[] = [];
  port = 0;

  private constructor(server: Server) {
    this.server = server;
  }

  get uri(): string {
    return `ws://127.0.0.1:${this.port}/`;
  }

  static async start(options: WebSocketAcceptOptions = { maxFrameSize: 1000 }): Promise<Harness> {
    const server = createServer();
    const h = new Harness(server);
    const upgrade = (request: IncomingMessage, socket: Duplex, head: Uint8Array): void => {
      acceptWebSocket(request, socket, head, options).then(
        (accepted) => {
          h.accepted.push(accepted);
          onCleanup(() => accepted.transport.abort());
        },
        (error: unknown) => h.refused.push(error),
      );
    };
    server.on('upgrade', upgrade);
    // A request without upgrade headers: refused by the same checks.
    server.on('request', (request) => upgrade(request, request.socket, new Uint8Array(0)));
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    const address = server.address();
    h.port = typeof address === 'object' && address !== null ? address.port : 0;
    onCleanup(() => {
      server.closeAllConnections();
      server.close();
    });
    return h;
  }

  /** A raw client connected and upgraded, and the server side of it. */
  async pair(request?: string): Promise<[RawClient, WebSocketTransport]> {
    const client = await RawClient.connect(this.port, request);
    if (client.status !== 101) {
      throw new Error(`upgrade answered ${client.status}`);
    }
    return [client, (await this.accepted.next()).transport];
  }
}

/** A plain `ws` server selecting `switchboard`, whose accepted sockets the test takes. */
export async function plainWsServer(
  options: { delayUpgrade?: number } = {},
): Promise<{ url: string; port: number; accepted: Queue<WebSocket>; server: Server }> {
  const http = createServer();
  const wss = new WebSocketServer({
    noServer: true,
    handleProtocols: (protocols) => (protocols.has('switchboard') ? 'switchboard' : false),
  });
  const accepted = new Queue<WebSocket>();
  http.on('upgrade', (request, socket, head) => {
    const go = (): void =>
      wss.handleUpgrade(request, socket, head, (ws) => {
        accepted.push(ws);
        onCleanup(() => ws.terminate());
      });
    if (options.delayUpgrade === undefined) go();
    else setTimeout(go, options.delayUpgrade);
  });
  await new Promise<void>((resolve) => http.listen(0, '127.0.0.1', resolve));
  const address = http.address();
  const port = typeof address === 'object' && address !== null ? address.port : 0;
  onCleanup(() => {
    http.closeAllConnections();
    http.close();
  });
  return { url: `ws://127.0.0.1:${port}/`, port, accepted, server: http };
}

/** Resolves with the close code once [ws] has closed. */
export function closeCodeOfWs(ws: WebSocket): Promise<number> {
  if (ws.readyState === WebSocket.CLOSED) {
    return Promise.resolve(-1);
  }
  return new Promise((resolve) => ws.once('close', (code) => resolve(code)));
}

/** The messages [ws] receives from now on. */
export function wsMessages(ws: WebSocket): Queue<{ data: Uint8Array | string; binary: boolean }> {
  const queue = new Queue<{ data: Uint8Array | string; binary: boolean }>();
  ws.on('message', (data, isBinary) => {
    const bytes = Array.isArray(data)
      ? Buffer.concat(data)
      : data instanceof ArrayBuffer
        ? Buffer.from(data)
        : data;
    queue.push({ data: isBinary ? new Uint8Array(bytes) : bytes.toString(), binary: isBinary });
  });
  ws.on('close', () => queue.end());
  return queue;
}
