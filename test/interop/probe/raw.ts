/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// A scripted byte peer for the conformance probes: mux frames over a raw TCP
// socket (the stream binding) or a WebSocket, built and read by hand, without
// the library under test.

import { connect, type Socket } from 'node:net';
import { concatBytes, hexBytes, hexString } from '../../../src/core/bytes.ts';

/** The stream binding's preamble, version 1. */
export const preamble = hexBytes('53 57 42 44 01 00 00 00');

/** Little-endian bytes of [value] in [size] bytes. */
export function le(value: number, size: number): Uint8Array {
  const out = new Uint8Array(size);
  let v = value;
  for (let i = 0; i < size; i++) {
    out[i] = v % 256;
    v = Math.floor(v / 256);
  }
  return out;
}

/** An 8-byte name. */
export function name8(text: string): Uint8Array {
  const out = new Uint8Array(8);
  out.set(new TextEncoder().encode(text));
  return out;
}

/** UTF-8 bytes. */
export function utf8(text: string): Uint8Array {
  return new TextEncoder().encode(text);
}

const short = 0x02;

/** Mux frames with a short header. */
export const frame = {
  data: (channel: number, payload: Uint8Array = new Uint8Array(0)) =>
    concatBytes([short | 0x00], le(channel, 2), payload),
  open: (channel: number, payload: Uint8Array = new Uint8Array(0)) =>
    concatBytes([short | 0x10], le(channel, 2), payload),
  close: (channel: number, status: Uint8Array = new Uint8Array(0)) =>
    concatBytes([short | 0x20], le(channel, 2), status),
  control: (type: number, payload: Uint8Array = new Uint8Array(0)) =>
    concatBytes([short, 0, 0, type], payload),
};

/** A status payload: code and UTF-8 reason. */
export function status(code: number, reason = ''): Uint8Array {
  return concatBytes(le(code, 2), utf8(reason));
}

/** A frame as the probe received it, with its header read. */
export interface Received {
  readonly bytes: Uint8Array;
  readonly command: 'data' | 'open' | 'close' | 'reserved';
  readonly channel: number;
  readonly payload: Uint8Array;
  /** For DATA on channel 0: the control type; otherwise undefined. */
  readonly control: number | undefined;
  /** The payload as hex, for assertions. */
  readonly hex: string;
}

/** Reads the header of [bytes], a mux frame. */
export function parseFrame(bytes: Uint8Array): Received {
  const flags = bytes[0] ?? 0;
  const commands = ['data', 'open', 'close', 'reserved'] as const;
  const command = commands[(flags >> 4) & 3] ?? 'reserved';
  const isShort = (flags & short) !== 0;
  const headerSize = isShort ? 3 : 7;
  let channel = 0;
  for (let i = headerSize - 1; i >= 1; i--) {
    channel = channel * 256 + (bytes[i] ?? 0);
  }
  const payload = bytes.subarray(headerSize);
  const control = command === 'data' && channel === 0 ? payload[0] : undefined;
  return { bytes, command, channel, payload, control, hex: hexString(payload) };
}

/** How a raw connection ended. */
export interface Ended {
  /** The WebSocket close code, if a WebSocket. */
  readonly code?: number;
}

/** A connection the probe drives frame by frame. */
export interface RawConnection {
  /** Sends one mux frame. */
  send(frame: Uint8Array): void;
  /** The next frame received, or rejects after [timeoutMs] or at the end. */
  next(timeoutMs?: number): Promise<Received>;
  /** The next frame [match] accepts, skipping the others (keep-alive PINGs, say). */
  nextMatching(match: (f: Received) => boolean, timeoutMs?: number): Promise<Received>;
  /** Resolves when the far side ends the connection. */
  readonly ended: Promise<Ended>;
  /** Ends the connection from this side. */
  end(): void;
}

class FrameQueue {
  readonly #frames: Received[] = [];
  readonly #waiters: ((f: Received | Error) => void)[] = [];
  #end: Error | undefined;

  push(bytes: Uint8Array): void {
    const f = parseFrame(bytes);
    const w = this.#waiters.shift();
    if (w !== undefined) {
      w(f);
    } else {
      this.#frames.push(f);
    }
  }

  finish(reason: string): void {
    this.#end = new Error(reason);
    for (const w of this.#waiters.splice(0)) {
      w(this.#end);
    }
  }

  next(timeoutMs: number): Promise<Received> {
    const f = this.#frames.shift();
    if (f !== undefined) {
      return Promise.resolve(f);
    }
    if (this.#end !== undefined) {
      return Promise.reject(this.#end);
    }
    return new Promise((resolve, reject) => {
      const waiter = (r: Received | Error) => {
        clearTimeout(timer);
        if (r instanceof Error) {
          reject(r);
        } else {
          resolve(r);
        }
      };
      const timer = setTimeout(() => {
        this.#waiters.splice(this.#waiters.indexOf(waiter), 1);
        reject(new Error(`no frame within ${timeoutMs} ms`));
      }, timeoutMs);
      this.#waiters.push(waiter);
    });
  }

  async nextMatching(match: (f: Received) => boolean, timeoutMs: number): Promise<Received> {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      const f = await this.next(Math.max(1, deadline - Date.now()));
      if (match(f)) {
        return f;
      }
    }
  }
}

/** Options of {@link rawTcp}. */
export interface TcpOptions {
  /** The preamble to send first; default version 1. Null sends none. */
  preamble?: Uint8Array | null;
}

/** A raw TCP connection speaking the stream binding to `tcp://host:port`. */
export async function rawTcp(
  endpoint: string,
  options: TcpOptions = {},
): Promise<
  RawConnection & {
    /** Writes bytes to the socket as they are, outside any frame. */
    write(bytes: Uint8Array): void;
    /** The preamble the server sent. */
    readonly serverPreamble: Promise<Uint8Array>;
  }
> {
  const url = new URL(endpoint);
  const socket: Socket = connect(Number(url.port), url.hostname);
  await new Promise<void>((resolve, reject) => {
    socket.once('connect', resolve);
    socket.once('error', reject);
  });
  socket.setNoDelay(true);
  const queue = new FrameQueue();
  let buf = new Uint8Array(0);
  let gotPreamble: (b: Uint8Array) => void = () => {};
  const serverPreamble = new Promise<Uint8Array>((resolve) => {
    gotPreamble = resolve;
  });
  let preambleRead = false;
  socket.on('data', (chunk: Buffer) => {
    buf = concatBytes(buf, chunk);
    if (!preambleRead) {
      if (buf.length < 8) {
        return;
      }
      gotPreamble(buf.slice(0, 8));
      buf = buf.subarray(8);
      preambleRead = true;
    }
    while (buf.length >= 4) {
      const n =
        (buf[0] ?? 0) + (buf[1] ?? 0) * 256 + (buf[2] ?? 0) * 65536 + (buf[3] ?? 0) * 16777216;
      if (buf.length < 4 + n) {
        break;
      }
      queue.push(buf.slice(4, 4 + n));
      buf = buf.subarray(4 + n);
    }
  });
  const ended = new Promise<Ended>((resolve) => {
    socket.on('close', () => {
      queue.finish('the server closed the connection');
      resolve({});
    });
    socket.on('error', () => {});
  });
  const pre = options.preamble === undefined ? preamble : options.preamble;
  if (pre !== null) {
    socket.write(pre);
  }
  return {
    send: (f) => {
      socket.write(concatBytes(le(f.length, 4), f));
    },
    write: (bytes) => {
      socket.write(bytes);
    },
    next: (timeoutMs = 5_000) => queue.next(timeoutMs),
    nextMatching: (match, timeoutMs = 5_000) => queue.nextMatching(match, timeoutMs),
    ended,
    serverPreamble,
    end: () => {
      socket.end();
    },
  };
}

/** Options of {@link rawWebSocket}. */
export interface WsOptions {
  /** The subprotocols to offer; default `switchboard`. */
  protocols?: string[];
}

/** A raw WebSocket connection to `ws://host:port/path`: one binary message per frame. */
export async function rawWebSocket(
  endpoint: string,
  options: WsOptions = {},
): Promise<RawConnection & { readonly protocol: string }> {
  const ws = new WebSocket(endpoint, options.protocols ?? ['switchboard']);
  ws.binaryType = 'arraybuffer';
  const queue = new FrameQueue();
  ws.addEventListener('message', (e: MessageEvent) => {
    if (e.data instanceof ArrayBuffer) {
      queue.push(new Uint8Array(e.data));
    }
  });
  const ended = new Promise<Ended>((resolve) => {
    ws.addEventListener('close', (e: CloseEvent) => {
      queue.finish(`the server closed the WebSocket (${e.code})`);
      resolve({ code: e.code });
    });
  });
  await new Promise<void>((resolve, reject) => {
    ws.addEventListener('open', () => resolve());
    ws.addEventListener('error', () => reject(new Error(`cannot open ${endpoint}`)));
  });
  return {
    send: (f) => ws.send(f),
    next: (timeoutMs = 5_000) => queue.next(timeoutMs),
    nextMatching: (match, timeoutMs = 5_000) => queue.nextMatching(match, timeoutMs),
    ended,
    protocol: ws.protocol,
    end: () => ws.close(1000),
  };
}
