/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// A scripted record peer for the QUIC probe: raw QUIC connections and
// WebTransport sessions opened or accepted through the native provider
// directly, with the records of the binding (wiki page "QUIC and
// WebTransport Binding", "Records") built and read by hand, without the
// binding under test (src/core/quic). The provider only carries streams;
// every byte on them is the probe's.

import { concatBytes, hexBytes, hexString } from '../../../src/core/bytes.ts';
import {
  QuicError,
  type QuicSessionEnd,
  type QuicStream,
  QuicStreamReset,
  QuicStreamStopped,
  switchboardAlpn,
} from '../../../src/core/quic/contract.ts';
import {
  type NativeQuicListener,
  type NativeQuicSession,
  nativeQuic,
  selfSignedCertificate,
} from '../../../src/node/quic/index.ts';
import { le } from './raw.ts';

const empty = new Uint8Array(0);

/** The default record limit, and the least a `HELLO` may state. */
export const defaultRecordLimit = 32752;

/** The `EXPORTER` flag of `HELLO`. */
export const exporterFlag = 0x01;

/** The control message types (mux "Control channel", plus `HELLO`). */
export const control = {
  ping: 1,
  pong: 2,
  goAway: 3,
  limits: 4,
  nonce: 5,
  ident: 6,
  credit: 7,
  hello: 8,
} as const;

/** The commands of a record header, `header >> 30`. */
export const recordCommand = { data: 0, open: 1, close: 2, reserved: 3 } as const;

/** A record header: [length] in bits 0–29 and [command] in bits 30–31, little endian. */
export function recordHeader(command: number, length: number): Uint8Array {
  return le(command * 0x40000000 + length, 4);
}

/** Records, built by hand. */
export const record = {
  /** DATA with one subframe (or, on the control stream, one control message). */
  data: (body: Uint8Array = empty): Uint8Array =>
    concatBytes(recordHeader(recordCommand.data, body.length), body),
  /** OPEN of [channel] (a `u48`) with [openPayload]. */
  open: (channel: number, openPayload: Uint8Array = empty): Uint8Array => {
    const body = concatBytes(le(channel, 6), openPayload);
    return concatBytes(recordHeader(recordCommand.open, body.length), body);
  },
  /** CLOSE with a status payload; empty is `OK`. */
  close: (status: Uint8Array = empty): Uint8Array =>
    concatBytes(recordHeader(recordCommand.close, status.length), status),
  /** A control message: DATA whose body is the type and the payload. */
  control: (type: number, payload: Uint8Array = empty): Uint8Array =>
    record.data(concatBytes([type], payload)),
};

/** A `HELLO` payload (after the type): record limit, channels, flags, further fields. */
export function hello(
  recordLimit = defaultRecordLimit,
  channels = 0,
  flags = 0,
  further: Uint8Array = empty,
): Uint8Array {
  return concatBytes(le(recordLimit, 4), le(channels, 4), [flags], further);
}

/** A record as the probe received it. */
export interface ReceivedRecord {
  readonly command: 'data' | 'open' | 'close' | 'reserved';
  /** The whole record, header included. */
  readonly bytes: Uint8Array;
  /** The body, after the 4-byte header. */
  readonly body: Uint8Array;
  /** The whole record, header included, as hex: what the assertions compare. */
  readonly hex: string;
  /** The body as hex. */
  readonly bodyHex: string;
  /** For DATA: the first body byte, the control type on the control stream. */
  readonly type: number | undefined;
  /** For OPEN with a whole channel id: the channel id. */
  readonly channel: number | undefined;
}

const commandNames = ['data', 'open', 'close', 'reserved'] as const;

/** The first whole record of [buffer] and its size, or undefined while incomplete. */
function takeRecord(buffer: Uint8Array): { record: ReceivedRecord; size: number } | undefined {
  if (buffer.length < 4) {
    return undefined;
  }
  const header =
    (buffer[0] ?? 0) +
    (buffer[1] ?? 0) * 0x100 +
    (buffer[2] ?? 0) * 0x10000 +
    (buffer[3] ?? 0) * 0x1000000;
  const length = header % 0x40000000;
  const size = 4 + length;
  if (buffer.length < size) {
    return undefined;
  }
  const command = commandNames[Math.floor(header / 0x40000000)] ?? 'reserved';
  const bytes = buffer.slice(0, size);
  const body = bytes.subarray(4);
  let channel: number | undefined;
  if (command === 'open' && body.length >= 6) {
    channel = 0;
    for (let i = 5; i >= 0; i--) {
      channel = channel * 256 + (body[i] ?? 0);
    }
  }
  return {
    record: {
      command,
      bytes,
      body,
      hex: hexString(bytes),
      bodyHex: hexString(body),
      type: command === 'data' ? body[0] : undefined,
      channel,
    },
    size,
  };
}

/** How the peer's direction of a stream ended. */
export type DirectionEnd =
  /** FIN; [partial] is what followed the last whole record, as hex (empty normally). */
  | { readonly kind: 'fin'; readonly partial: string }
  /** RESET_STREAM with its code (undefined when the stack delivered none). */
  | { readonly kind: 'reset'; readonly code: number | undefined }
  /** The session ended first. */
  | { readonly kind: 'session' };

/** What the peer did about this side's direction. */
export type PeerStop =
  /** STOP_SENDING, with its code (undefined when the stack delivered none). */
  | { readonly kind: 'stop'; readonly code: number | undefined }
  /** Nothing: everything this side wrote was acknowledged after its FIN. */
  | { readonly kind: 'acked' }
  /** Nothing before this side reset its direction. */
  | { readonly kind: 'reset' }
  /** Nothing before the session ended. */
  | { readonly kind: 'session' };

/** Rejects with "[what] within [ms] ms" unless [promise] settles before [deadline]. */
export async function before<T>(promise: Promise<T>, deadline: number, what: string): Promise<T> {
  const ms = Math.max(1, deadline - Date.now());
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`${what} within ${ms} ms`)), ms);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

/** [promise] within [timeoutMs], or a rejection naming [what]. */
export function within<T>(promise: Promise<T>, timeoutMs: number, what: string): Promise<T> {
  return before(promise, Date.now() + timeoutMs, what);
}

type ReadOutcome = { readonly chunk: Uint8Array | undefined } | { readonly error: unknown };

/** One bidirectional stream read and written record by record. */
export class RecordStream {
  readonly stream: QuicStream;
  /** What the peer did about this side's direction; see {@link PeerStop}. */
  readonly peerStop: Promise<PeerStop>;
  #buffer: Uint8Array = empty;
  #reading: Promise<ReadOutcome> | undefined;
  #end: 'fin' | { reset: number | undefined } | 'session' | undefined;
  #resetSent = false;

  constructor(stream: QuicStream) {
    this.stream = stream;
    this.peerStop = stream.stopped.then(
      (code): PeerStop =>
        code !== undefined
          ? { kind: 'stop', code }
          : this.#resetSent
            ? { kind: 'reset' }
            : { kind: 'acked' },
      (error: unknown): PeerStop =>
        error instanceof QuicStreamStopped
          ? { kind: 'stop', code: error.code }
          : { kind: 'session' },
    );
  }

  /** Writes [parts] as they are, in one write. */
  write(...parts: Uint8Array[]): Promise<void> {
    return this.stream.write(concatBytes(...parts));
  }

  /** Writes [parts] and FIN. */
  async end(...parts: Uint8Array[]): Promise<void> {
    if (parts.length > 0) {
      await this.write(...parts);
    }
    await this.stream.finish();
  }

  /** Resets this side's direction with [code]. */
  reset(code: number): void {
    this.#resetSent = true;
    this.stream.reset(code);
  }

  /** Sends STOP_SENDING with [code]; this side reads no more. */
  stop(code: number): void {
    this.stream.stop(code);
  }

  /** The peer's STOP_SENDING code within [timeoutMs], or what happened instead. */
  async stopped(timeoutMs = 5_000): Promise<PeerStop> {
    return within(this.peerStop, timeoutMs, 'no STOP_SENDING');
  }

  /** The next record, or the end of the peer's direction; rejects after [timeoutMs]. */
  async next(timeoutMs = 5_000): Promise<ReceivedRecord | DirectionEnd> {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      const taken = takeRecord(this.#buffer);
      if (taken !== undefined) {
        this.#buffer = this.#buffer.subarray(taken.size);
        return taken.record;
      }
      const end = this.#end;
      if (end === 'fin') {
        return { kind: 'fin', partial: hexString(this.#buffer) };
      }
      if (end === 'session') {
        return { kind: 'session' };
      }
      if (end !== undefined) {
        return { kind: 'reset', code: end.reset };
      }
      await this.#fill(deadline);
    }
  }

  /** The next record; rejects when the peer's direction ends instead, or after [timeoutMs]. */
  async record(timeoutMs = 5_000): Promise<ReceivedRecord> {
    const r = await this.next(timeoutMs);
    if (!('command' in r)) {
      throw new Error(
        `the peer's direction ended (${JSON.stringify(r)}) where a record was expected`,
      );
    }
    return r;
  }

  /** The next record [match] accepts, skipping the others. */
  async recordMatching(
    match: (r: ReceivedRecord) => boolean,
    timeoutMs = 5_000,
  ): Promise<ReceivedRecord> {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      const r = await this.record(Math.max(1, deadline - Date.now()));
      if (match(r)) {
        return r;
      }
    }
  }

  /** Every record until the peer's direction ends, and how it ended. */
  async drain(timeoutMs = 5_000): Promise<{ records: ReceivedRecord[]; end: DirectionEnd }> {
    const deadline = Date.now() + timeoutMs;
    const records: ReceivedRecord[] = [];
    for (;;) {
      const r = await this.next(Math.max(1, deadline - Date.now()));
      if (!('command' in r)) {
        return { records, end: r };
      }
      records.push(r);
    }
  }

  /** Reads one chunk into the buffer, or notes the end; the read in flight survives a timeout. */
  async #fill(deadline: number): Promise<void> {
    this.#reading ??= this.stream.read(1 << 16).then(
      (chunk): ReadOutcome => ({ chunk }),
      (error: unknown): ReadOutcome => ({ error }),
    );
    const outcome = await before(this.#reading, deadline, 'no record');
    this.#reading = undefined;
    if ('error' in outcome) {
      const { error } = outcome;
      if (error instanceof QuicStreamReset) {
        this.#end = { reset: error.code };
      } else if (error instanceof QuicError && error.kind === 'sessionEnded') {
        this.#end = 'session';
      } else {
        throw error;
      }
    } else if (outcome.chunk === undefined) {
      this.#end = 'fin';
    } else {
      this.#buffer = concatBytes(this.#buffer, outcome.chunk);
    }
  }
}

/** Values delivered one at a time with a deadline, until the source ends. */
class Inbox<T> {
  readonly #values: T[] = [];
  readonly #waiters: ((v: T | Error) => void)[] = [];
  #end: Error | undefined;

  push(value: T): void {
    const w = this.#waiters.shift();
    if (w !== undefined) {
      w(value);
    } else {
      this.#values.push(value);
    }
  }

  finish(reason: string): void {
    this.#end = new Error(reason);
    for (const w of this.#waiters.splice(0)) {
      w(this.#end);
    }
  }

  next(timeoutMs: number, what: string): Promise<T> {
    if (this.#values.length > 0) {
      return Promise.resolve(this.#values.shift() as T);
    }
    if (this.#end !== undefined) {
      return Promise.reject(this.#end);
    }
    return new Promise((resolve, reject) => {
      const waiter = (v: T | Error) => {
        clearTimeout(timer);
        if (v instanceof Error) {
          reject(v);
        } else {
          resolve(v);
        }
      };
      const timer = setTimeout(() => {
        this.#waiters.splice(this.#waiters.indexOf(waiter), 1);
        reject(new Error(`no ${what} within ${timeoutMs} ms`));
      }, timeoutMs);
      this.#waiters.push(waiter);
    });
  }
}

/** Takes everything [source] yields into [inbox]. */
function pump<T, U>(
  source: AsyncIterable<T>,
  inbox: Inbox<U>,
  map: (v: T) => U,
  what: string,
): void {
  void (async () => {
    try {
      for await (const v of source) {
        inbox.push(map(v));
      }
      inbox.finish(`the ${what} ended`);
    } catch (error) {
      inbox.finish(`the ${what} failed: ${String(error)}`);
    }
  })();
}

/** A session the probe drives stream by stream: dialled by {@link dial} or accepted by a {@link ProbeListener}. */
export class ProbeSession {
  readonly session: NativeQuicSession;
  readonly #incoming = new Inbox<RecordStream>();

  constructor(session: NativeQuicSession) {
    this.session = session;
    pump(session.incomingStreams, this.#incoming, (s) => new RecordStream(s), 'incoming streams');
  }

  /** Opens a stream and writes [parts] as its first bytes (a WebTransport stream is not visible before). */
  async open(...parts: Uint8Array[]): Promise<RecordStream> {
    const stream = new RecordStream(await this.session.openStream());
    if (parts.length > 0) {
      await stream.write(...parts);
    }
    return stream;
  }

  /**
   * Opens the control stream: the OPEN record of channel 0, then [helloPayload]
   * as a `HELLO` (default: record limit 32752, no stated channel limit, no
   * flags), then [more] records, in one flight. Null sends no `HELLO`.
   */
  openControl(
    helloPayload: Uint8Array | null = hello(),
    ...more: Uint8Array[]
  ): Promise<RecordStream> {
    return this.open(
      record.open(0),
      helloPayload === null ? empty : record.control(control.hello, helloPayload),
      ...more,
    );
  }

  /** The next stream the peer opened, in the order the stack delivers them. */
  incoming(timeoutMs = 5_000): Promise<RecordStream> {
    return this.#incoming.next(timeoutMs, 'incoming stream');
  }

  /** How the session ended, within [timeoutMs]. */
  ended(timeoutMs = 5_000): Promise<QuicSessionEnd> {
    return within(this.session.done, timeoutMs, 'the session did not end');
  }

  /** Closes the session with an application [code] and [reason]. */
  close(code = 0, reason = ''): Promise<void> {
    return this.session.close(code, reason);
  }
}

/** Options of {@link dial}. */
export interface DialOptions {
  /** The receive window of each stream; default the provider's (1 MiB). */
  readonly streamReceiveWindow?: number;
}

/**
 * Dials [endpoint] (`quic://` raw QUIC with ALPN `switchboard`, `https://` a
 * WebTransport session offering `switchboard`), pinning the certificate
 * whose SHA-256 is [certificateSha256] (hex). Opens no stream.
 */
export async function dial(
  endpoint: string,
  certificateSha256: string,
  options: DialOptions = {},
): Promise<ProbeSession> {
  const session = await nativeQuic().connect(endpoint, {
    alpn: [switchboardAlpn],
    serverCertificateHashes: [hexBytes(certificateSha256)],
    streamReceiveWindow: options.streamReceiveWindow,
  });
  return new ProbeSession(session);
}

/** A raw QUIC or WebTransport listener of the probe's own, for peers to dial. */
export class ProbeListener {
  readonly listener: NativeQuicListener;
  readonly #sessions = new Inbox<ProbeSession>();

  private constructor(listener: NativeQuicListener) {
    this.listener = listener;
    pump(listener.sessions, this.#sessions, (s) => new ProbeSession(s), 'listener');
  }

  /** Listens on the loopback for raw QUIC (ALPN `switchboard`) or WebTransport (path [path]). */
  static async start(transport: 'quic' | 'webtransport', path = '/sb'): Promise<ProbeListener> {
    const certificate = selfSignedCertificate('127.0.0.1');
    const config = {
      alpn: [switchboardAlpn],
      certificateDer: certificate.certificateDer,
      privateKeyDer: certificate.privateKeyDer,
    };
    const provider = nativeQuic();
    return new ProbeListener(
      transport === 'quic'
        ? await provider.listen('127.0.0.1', 0, config)
        : await provider.listenWebTransport('127.0.0.1', 0, config, { path }),
    );
  }

  /** The endpoint peers dial. */
  get endpoint(): string {
    return this.listener.endpoint;
  }

  /** The next session accepted. */
  accept(timeoutMs = 5_000): Promise<ProbeSession> {
    return this.#sessions.next(timeoutMs, 'session');
  }

  /** Stops accepting. */
  close(): Promise<void> {
    return this.listener.close();
  }
}
