/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * The web stream provider: the browser's own WebTransport API (W3C) under the binding, so that a
 * browser frontend reaches a Switchboard node over WebTransport. A browser cannot listen:
 * {@link WebTransportProvider} is a {@link QuicConnector} of `https://` sessions only. A port of
 * `switchboard_quic`'s `web_provider.dart`.
 *
 * What the browser decides, the provider cannot: the QUIC windows, the stream limits, the idle
 * timeout and keep-alive (the configuration's are ignored), and whether a certificate is verified
 * (always: by the platform roots, or by `serverCertificateHashes`). What a browser may lack
 * (`protocol`, `draining`, `exportKeyingMaterial`, `getStats`, send orders and groups) is
 * feature-tested at run time; Chromium 141 has none of them, and sends no `WT-Available-Protocols`
 * either, so the provider checks the protocols it offers itself.
 *
 * Reads are pull-based through a default reader of each stream's readable: a read takes one chunk
 * of the browser's at most, returns at most the bytes asked for and keeps the rest of the chunk
 * for the next read; what was not read stays in the browser's stream, whose flow control is the
 * browser's. The server's streams are taken from the browser only while someone waits for one.
 * Writes go through the writable's writer, one at a time as its `ready` allows, and complete once
 * the browser took the chunk (a copy of the caller's bytes). Resets and the server's
 * `STOP_SENDING` are read from the rejections of the reader's and the writer's `closed`, so a
 * paused reader still sees them.
 *
 * Every promise of the API gets a rejection handler, and every promise the provider hands out is
 * marked as handled: a page never sees an unhandled rejection from it.
 */

import type { ConnectionConnector } from '../../core/client/reconnecting-client.ts';
import { attempt, delay, sessionGoneGrace } from '../../core/quic/binding-util.ts';
import {
  defaultQuicLimits,
  type QuicClientConfig,
  type QuicConnector,
  QuicError,
  type QuicReceiveStream,
  type QuicSession,
  type QuicSessionEnd,
  type QuicStream,
  QuicStreamReset,
  QuicStreamStopped,
} from '../../core/quic/contract.ts';
import { type QuicConnectOptions, quicConnector } from '../../core/quic/quic-dial.ts';
import { type Clock, systemClock, type TimerHandle } from '../../core/util/clock.ts';
import { Deferred } from '../../core/util/deferred.ts';
import {
  type ApiError,
  describeApiError,
  fromSession,
  fromStream,
  globalWebTransport,
  globalWebTransportError,
  isThenable,
  maxWebTransportCode,
  readApiError,
  type SendStreamExtras,
  type StreamErrors,
  streamErrorsOf,
  type WebTransportConstructor,
  type WebTransportErrorConstructor,
  type WebTransportInit,
  type WebTransportLike,
} from './webtransport-api.ts';

/** The largest close reason, in bytes of UTF-8. */
const maxReasonBytes = 1024;

/** The first and the longest wait between two attempts to open a stream without credit, in ms. */
const openBackoff = Object.freeze({ initial: 5, max: 200 });

/** The longest protocol the W3C API accepts in `protocols`, in bytes. */
const maxProtocolLength = 512;

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const noop = (): void => {};

/** [promise], marked as handled: a caller that drops it sees no unhandled rejection. */
function handled<T>(promise: Promise<T>): Promise<T> {
  promise.catch(noop);
  return promise;
}

function rejected<T>(error: unknown): Promise<T> {
  return handled(Promise.reject(error));
}

/** [text] cut to at most [max] bytes of UTF-8 at a character boundary. */
function truncateUtf8(text: string, max: number): string {
  const bytes = encoder.encode(text);
  if (bytes.length <= max) return text;
  let n = max;
  while (n > 0 && ((bytes[n] as number) & 0xc0) === 0x80) n--;
  return decoder.decode(bytes.subarray(0, n));
}

function checkCloseCode(code: number): void {
  if (!Number.isInteger(code) || code < 0 || code > maxWebTransportCode) {
    throw new RangeError(`code ${code}: must be an integer from 0 to 2^32 - 1`);
  }
}

function checkStreamCode(code: number, errors: StreamErrors): void {
  checkCloseCode(code);
  const max = errors.maxCode;
  if (code > max) {
    throw new RangeError(
      `code ${code}: this browser carries stream error codes of 0 to ${max} only`,
    );
  }
}

/** [value] as a finite number, else undefined. */
function numberOf(value: unknown): number | undefined {
  return typeof value === 'number' && Number.isFinite(value) ? value : undefined;
}

function integerOf(value: unknown): number | undefined {
  const n = numberOf(value);
  return n === undefined ? undefined : Math.trunc(n);
}

/** The message of a rejection of the API, for the errors the provider reports. */
function messageOf(error: ApiError): string {
  return error.message === '' ? describeApiError(error) : error.message;
}

/** Releases [reader]'s lock, ignoring a reader released already. */
function releaseReader(reader: ReadableStreamDefaultReader<unknown> | undefined): undefined {
  try {
    reader?.releaseLock();
  } catch {
    // Released already.
  }
  return undefined;
}

/** Whether [value] is the bidirectional stream pair of the API. */
function isPair(value: unknown): value is {
  readonly readable: ReadableStream<unknown>;
  readonly writable: WritableStream<Uint8Array>;
} {
  if (typeof value !== 'object' || value === null) return false;
  const pair = value as { readonly readable?: unknown; readonly writable?: unknown };
  return (
    typeof (pair.readable as { getReader?: unknown } | undefined)?.getReader === 'function' &&
    typeof (pair.writable as { getWriter?: unknown } | undefined)?.getWriter === 'function'
  );
}

/** Checks the protocols offered as the W3C constructor would, which Chromium 141 does not. */
function checkProtocols(protocols: readonly string[]): void {
  const seen = new Set<string>();
  for (const protocol of protocols) {
    if (seen.has(protocol)) {
      throw new TypeError(`the protocol ${JSON.stringify(protocol)} is offered twice`);
    }
    seen.add(protocol);
    if (
      protocol.length === 0 ||
      protocol.length > maxProtocolLength ||
      /[^\x20-\x7e]/.test(protocol)
    ) {
      throw new TypeError(
        `protocol ${JSON.stringify(protocol)}: must be 1 to ${maxProtocolLength} printable ASCII characters`,
      );
    }
  }
}

// --- The provider ------------------------------------------------------------------------------

/** What {@link webTransport} and {@link WebTransportProvider} take; for tests, mostly. */
export interface WebTransportProviderOptions {
  /** The `WebTransport` constructor; default the browser's (`globalThis.WebTransport`), looked up at each connect. */
  readonly WebTransport?: WebTransportConstructor | undefined;
  /** The `WebTransportError` constructor; default the browser's (`globalThis.WebTransportError`). */
  readonly WebTransportError?: WebTransportErrorConstructor | undefined;
  /** The timers of the handshake deadline, the session-gone grace and the `openStream` backoff. */
  readonly clock?: Clock | undefined;
}

/**
 * WebTransport sessions through the browser's W3C API.
 *
 * `connect` opens `new WebTransport(endpoint, { protocols, serverCertificateHashes, allowPooling:
 * false, requireUnreliable: false })`: {@link QuicClientConfig.alpn} are the protocols offered
 * (`switchboard` for Switchboard), and {@link QuicClientConfig.serverCertificateHashes} the
 * SHA-256 hashes of acceptable certificates (a self-signed ECDSA P-256 certificate valid for at
 * most 14 days; empty: the platform roots). The browser verifies the certificate whatever
 * {@link QuicClientConfig.verifyCertificate} says. The session is established once `ready`
 * resolves; the browser tells a refused request (another path, an origin the server refuses), an
 * unreachable server and a rejected certificate apart to no one ("Opening handshake failed."), so
 * all of them fail the connect with a {@link QuicError} of kind 'handshakeFailed' and the browser's
 * message; {@link QuicClientConfig.handshakeTimeout} bounds the wait (then 'unreachable'). The
 * other durations, the windows and the stream limit of the configuration are the browser's to
 * decide, but a negative duration is still a `RangeError`.
 *
 * ```ts
 * const connection = await connectQuic('https://edge.example.com:4433/sb', webTransport(), {
 *   serverCertificateHashes: [{ algorithm: 'sha-256', value: hash }],
 * });
 * ```
 */
export class WebTransportProvider implements QuicConnector {
  readonly #webTransportClass: WebTransportConstructor | undefined;
  readonly #errorClass: WebTransportErrorConstructor | undefined;
  readonly #clock: Clock;

  constructor(init: WebTransportProviderOptions = {}) {
    this.#webTransportClass = init.WebTransport;
    this.#errorClass = init.WebTransportError;
    this.#clock = init.clock ?? systemClock;
  }

  /** Whether this browser has the WebTransport API (a secure context of a browser that implements it). */
  static get isSupported(): boolean {
    return globalWebTransport() !== undefined;
  }

  /**
   * The largest stream error code ({@link QuicStream.reset}, {@link QuicStream.stop}) this browser
   * can send: 2^32 - 1 by the specification, 255 in Chromium, whose
   * `WebTransportError.streamErrorCode` is an octet. A larger code is a `RangeError` rather than
   * another code on the wire. The binding sends status codes, 0 to 255. Codes received are not
   * bounded.
   */
  static get maxStreamErrorCode(): number {
    return streamErrorsOf(globalWebTransportError()).maxCode;
  }

  /**
   * The `sendOrder` of a stream of the priority class [priorityClass] (see
   * {@link QuicStream.priority}), where the browser has send orders: higher goes sooner, strictly,
   * among the streams of one send group. A class above 1 is `class - 1`, so the control stream (2)
   * is 1; the classes 1 (ordinary) and 0 (bulk) are both 0, so that neither starves the other where
   * they share a group; a negative class is itself.
   */
  static sendOrderOf(priorityClass: number): number {
    return priorityClass > 0 ? priorityClass - 1 : priorityClass;
  }

  /** Whether this provider has a `WebTransport` constructor: the injected one, or the browser's. */
  get isSupported(): boolean {
    return this.#webTransport() !== undefined;
  }

  /** {@link WebTransportProvider.maxStreamErrorCode} of this provider's `WebTransportError`. */
  get maxStreamErrorCode(): number {
    return this.#errors().maxCode;
  }

  /**
   * Connects to the `https://host:port/path` [endpoint] (port 443 by default; its fragment
   * dropped). Rejects with a {@link QuicError} of kind 'unsupportedScheme' for any other scheme,
   * and where the browser has no WebTransport API; 'handshakeFailed' or 'unreachable' as the class
   * says; a `TypeError` for a malformed endpoint, userinfo, protocols offered twice or not
   * printable ASCII, or options the browser refuses; a `RangeError` for a negative duration or a
   * certificate hash that is not 32 bytes.
   */
  connect(endpoint: string, config: QuicClientConfig): Promise<WebTransportSession> {
    return handled(attempt(() => this.#connect(endpoint, config)));
  }

  #connect(endpoint: string, config: QuicClientConfig): Promise<WebTransportSession> {
    let url: URL;
    try {
      url = new URL(endpoint);
    } catch (cause) {
      throw new TypeError(`${endpoint}: not a valid endpoint`, { cause });
    }
    if (url.protocol !== 'https:') {
      throw new QuicError(
        'unsupportedScheme',
        `the web provider dials https:// only, not ${url.protocol}//`,
      );
    }
    if (url.hostname === '') {
      throw new QuicError('unreachable', `an https:// endpoint needs a host: ${endpoint}`);
    }
    if (url.username !== '' || url.password !== '') {
      throw new TypeError(`${endpoint}: userinfo is not allowed`);
    }
    for (const [name, value] of [
      ['idleTimeout', config.idleTimeout],
      ['keepAlive', config.keepAlive],
      ['handshakeTimeout', config.handshakeTimeout],
    ] as const) {
      if (value !== undefined && !(value >= 0)) {
        throw new RangeError(`${name} ${value}: must not be negative`);
      }
    }
    const hashes = config.serverCertificateHashes ?? [];
    for (const hash of hashes) {
      if (hash.length !== 32) throw new RangeError('a certificate hash is 32 bytes (SHA-256)');
    }
    checkProtocols(config.alpn);
    const WebTransport = this.#webTransport();
    if (WebTransport === undefined) {
      throw new QuicError(
        'unsupportedScheme',
        'this browser has no WebTransport API (or the page is not a secure context)',
      );
    }
    const init: WebTransportInit = { allowPooling: false, requireUnreliable: false };
    if (config.alpn.length > 0) init.protocols = [...config.alpn];
    if (hashes.length > 0) {
      init.serverCertificateHashes = hashes.map((hash) => ({
        algorithm: 'sha-256',
        value: new Uint8Array(hash),
      }));
    }
    url.hash = '';
    let transport: WebTransportLike;
    try {
      transport = new WebTransport(url.href, init);
    } catch (cause) {
      // A SyntaxError: the URL or the protocols.
      throw new TypeError(`the browser refused the endpoint or the options: ${String(cause)}`, {
        cause,
      });
    }
    const remote = `${url.hostname}:${url.port === '' ? 443 : url.port}`;
    return openSession(
      transport,
      remote,
      this.#errors(),
      this.#clock,
      config.handshakeTimeout ?? defaultQuicLimits.handshakeTimeout,
    );
  }

  #webTransport(): WebTransportConstructor | undefined {
    return this.#webTransportClass ?? globalWebTransport();
  }

  #errors(): StreamErrors {
    return streamErrorsOf(this.#errorClass ?? globalWebTransportError());
  }
}

/**
 * The web provider: WebTransport sessions through the browser's W3C API, for
 * {@link connectQuic} or {@link quicConnector}.
 *
 * ```ts
 * const client = new ReconnectingClient(
 *   quicConnector('https://edge.example.com:4433/sb', webTransport(), {
 *     serverCertificateHashes: [{ algorithm: 'sha-256', value: hash }],
 *   }),
 * );
 * ```
 */
export function webTransport(init: WebTransportProviderOptions = {}): WebTransportProvider {
  return new WebTransportProvider(init);
}

/** What {@link webTransportConnector} takes: the dial's options, and the provider. */
export interface WebTransportConnectorOptions
  extends Omit<QuicConnectOptions, 'options' | 'signal'> {
  /** The provider that dials; default {@link webTransport}(). */
  readonly provider?: QuicConnector | undefined;
}

/**
 * A `ReconnectingClient`'s {@link ConnectionConnector} over WebTransport: {@link quicConnector}
 * with {@link webTransport}, each attempt using the client's mux options and signal.
 *
 * ```ts
 * const client = new ReconnectingClient(
 *   webTransportConnector('https://edge.example.com:4433/sb', {
 *     serverCertificateHashes: [{ algorithm: 'sha-256', value: hash }],
 *   }),
 * );
 * ```
 *
 * @throws {TypeError} if [endpoint] is not an absolute URI.
 */
export function webTransportConnector(
  endpoint: string | URL,
  init: WebTransportConnectorOptions = {},
): ConnectionConnector {
  const { provider, ...options } = init;
  return quicConnector(endpoint, provider ?? webTransport(), options);
}

// --- Sessions ----------------------------------------------------------------------------------

/** What a session's streams use of it. */
interface SessionLink {
  /** The `WebTransportError`s of the provider. */
  readonly errors: StreamErrors;
  /** Whether the session has ended. */
  readonly ended: boolean;
  /** A stream saw the session go: see {@link WebTransportSession}'s two-second grace. */
  gone(message: string): void;
  /** The error what is pending fails with once the session ends, or is ending. */
  endedError(message?: string): QuicError;
  add(stream: StreamControl): void;
  remove(stream: StreamControl): void;
  removeReceive(stream: ReceiveControl): void;
  /** The send group of a priority class, where the browser has send groups. */
  sendGroupOf(priorityClass: number): unknown;
}

/** What a session does to its bidirectional streams. */
interface StreamControl {
  sessionEnded(error: QuicError): void;
  /** Ends a stream nobody takes: `STOP_SENDING` and `RESET_STREAM` with code 0. */
  discard(): void;
}

/** What a session does to the server's unidirectional streams it delivered. */
interface ReceiveControl {
  release(): void;
}

// Module-private access between the classes below, assigned in their static blocks, so that none
// of it is part of the public surface.
let openSession!: (
  transport: WebTransportLike,
  remote: string,
  errors: StreamErrors,
  clock: Clock,
  deadline: number,
) => Promise<WebTransportSession>;
let createStream!: (link: SessionLink, pair: unknown) => [WebTransportStream, StreamControl];
let createReceiveStream!: (
  link: SessionLink,
  readable: ReadableStream<unknown>,
) => [WebTransportReceiveStream, ReceiveControl];

/**
 * Which optional members of the W3C API a browser's session has (Chromium 141: none). For
 * diagnostics; the provider tests each where it uses it.
 */
export interface WebTransportCapabilities {
  /** `wt.protocol`, the protocol the server selected. */
  readonly protocol: boolean;
  /** `wt.draining`, the server's `WT_DRAIN_SESSION` or HTTP/3 `GOAWAY`. */
  readonly draining: boolean;
  /** `wt.exportKeyingMaterial`, for exporter-bound identity. */
  readonly exportKeyingMaterial: boolean;
  /** `wt.getStats`. */
  readonly getStats: boolean;
  /** `wt.createSendGroup`. */
  readonly createSendGroup: boolean;
}

/**
 * Statistics of a {@link WebTransportSession}, where the browser has `getStats` (W3C
 * `WebTransportConnectionStats`); a field the browser does not report is undefined. Times are
 * milliseconds.
 */
export interface WebTransportStats {
  readonly bytesSent: number | undefined;
  readonly packetsSent: number | undefined;
  readonly packetsLost: number | undefined;
  readonly bytesReceived: number | undefined;
  readonly packetsReceived: number | undefined;
  readonly smoothedRtt: number | undefined;
  readonly rttVariation: number | undefined;
  readonly minRtt: number | undefined;
  /** Bits per second. */
  readonly estimatedSendRate: number | undefined;
}

/**
 * The server's streams of one kind, taken from the browser only while the one consumer waits for
 * one: the others wait in the browser, holding the server's stream credit.
 */
class PullSource<T> implements AsyncIterable<T> {
  readonly #pull: () => void;
  #waiting: Deferred<IteratorResult<T>> | undefined;
  #ended = false;

  constructor(pull: () => void) {
    this.#pull = pull;
  }

  /** Whether the consumer waits for the next stream. */
  get isWaiting(): boolean {
    return this.#waiting !== undefined;
  }

  /** Hands [value] to the waiting consumer; false when nobody waits (the caller discards it). */
  deliver(value: T): boolean {
    const waiting = this.#waiting;
    if (waiting === undefined) return false;
    this.#waiting = undefined;
    waiting.resolve({ value, done: false });
    return true;
  }

  /** Ends the iteration. */
  end(): void {
    this.#ended = true;
    const waiting = this.#waiting;
    this.#waiting = undefined;
    waiting?.resolve({ value: undefined, done: true });
  }

  #next(): Promise<IteratorResult<T>> {
    if (this.#ended) return Promise.resolve({ value: undefined, done: true });
    if (this.#waiting !== undefined) {
      return rejected(new Error('the streams of a session have one consumer'));
    }
    const waiting = new Deferred<IteratorResult<T>>();
    this.#waiting = waiting;
    this.#pull();
    return waiting.promise;
  }

  [Symbol.asyncIterator](): AsyncIterator<T> {
    return {
      next: () => this.#next(),
      return: () => {
        // A consumer that leaves takes nothing more; a stream read meanwhile is discarded.
        this.end();
        return Promise.resolve({ value: undefined, done: true });
      },
    };
  }
}

/**
 * One WebTransport session of the browser. Always the initiator: a browser cannot listen. Its
 * {@link remote} is the endpoint's host and port (the browser does not tell the address it
 * reached), {@link peerCertificateHash} is undefined (the browser does not expose the certificate)
 * and {@link maxIncomingStreams} is undefined (the browser decides).
 */
export class WebTransportSession implements QuicSession {
  static {
    openSession = (transport, remote, errors, clock, deadline) =>
      new WebTransportSession(transport, remote, errors, clock).#establish(deadline);
  }

  readonly #transport: WebTransportLike;
  readonly #errors: StreamErrors;
  readonly #clock: Clock;
  readonly #link: SessionLink;
  #connecting: Deferred<WebTransportSession> | undefined;
  #handshakeTimer: TimerHandle | undefined;
  #protocol: string | undefined;
  readonly #done = new Deferred<QuicSessionEnd>();
  readonly #draining = new Deferred<void>();
  #drainingSeen = false;
  #end: QuicSessionEnd | undefined;
  #endError: QuicError | undefined;
  #closeSent = false;
  #closeCode = 0;
  #closeReason = '';
  #goneTimer: TimerHandle | undefined;

  // Streams.
  readonly #streams = new Set<StreamControl>();
  readonly #receiveStreams = new Set<ReceiveControl>();
  readonly #opens: Deferred<WebTransportStream>[] = [];
  #opening = false;
  readonly #incoming = new PullSource<WebTransportStream>(() => this.#pumpIncoming());
  readonly #unidirectional = new PullSource<WebTransportReceiveStream>(() =>
    this.#pumpUnidirectional(),
  );
  #incomingReader: ReadableStreamDefaultReader<unknown> | undefined;
  #unidirectionalReader: ReadableStreamDefaultReader<unknown> | undefined;
  #incomingPumping = false;
  #unidirectionalPumping = false;
  #incomingDone = false;
  #unidirectionalDone = false;

  // Send groups, where the browser has them.
  #ordinaryGroup: unknown;
  #bulkGroup: unknown;

  /** Always: a browser cannot listen. */
  readonly isInitiator: boolean = true;
  /** The endpoint's host and port: the browser does not tell the address it reached. */
  readonly remote: string;
  /** Undefined: the browser does not expose the certificate. */
  readonly peerCertificateHash: Uint8Array | undefined = undefined;
  /** Undefined: the browser decides. */
  readonly maxIncomingStreams: number | undefined = undefined;

  private constructor(
    transport: WebTransportLike,
    remote: string,
    errors: StreamErrors,
    clock: Clock,
  ) {
    this.#transport = transport;
    this.remote = remote;
    this.#errors = errors;
    this.#clock = clock;
    this.#link = this.#makeLink();
    attempt(() => transport.closed).then(
      (value) => this.#onClosed(value),
      (error: unknown) => this.#onClosedError(error),
    );
    // Handled from the start: a promise of the API is never left without a rejection handler.
    const draining = transport.draining;
    if (isThenable(draining)) {
      attempt(() => draining).then(() => {
        this.#drainingSeen = true;
        if (this.#connecting === undefined && this.#end === undefined) this.#draining.resolve();
      }, noop);
    }
  }

  /** The protocol the server selected (`wt.protocol`); undefined where the browser does not say, or the server selected none. */
  get protocol(): string | undefined {
    return this.#protocol;
  }

  get done(): Promise<QuicSessionEnd> {
    return this.#done.promise;
  }

  /**
   * Resolves on `wt.draining` where the browser has it (the server's `WT_DRAIN_SESSION` or an
   * HTTP/3 `GOAWAY`); never in Chromium 141, which has no such promise.
   */
  get draining(): Promise<void> {
    return this.#draining.promise;
  }

  /** The server's bidirectional streams, taken from the browser only while iterated. Ends with the session. */
  get incomingStreams(): AsyncIterable<WebTransportStream> {
    return this.#incoming;
  }

  /**
   * The server's unidirectional streams, from the session's `incomingUnidirectionalStreams`, for
   * the application to stop. Ends with the session.
   */
  get incomingUnidirectionalStreams(): AsyncIterable<WebTransportReceiveStream> {
    return this.#unidirectional;
  }

  /** Which optional members of the W3C API this browser's session has. */
  get capabilities(): WebTransportCapabilities {
    const t = this.#transport;
    return Object.freeze({
      protocol: typeof t.protocol === 'string',
      draining: isThenable(t.draining),
      exportKeyingMaterial: typeof t.exportKeyingMaterial === 'function',
      getStats: typeof t.getStats === 'function',
      createSendGroup: this.hasSendGroups,
    });
  }

  /**
   * Whether the browser has send groups (`createSendGroup`): the binding's classes 2 and 1 then
   * share one group (2 at a higher `sendOrder`) and class 0 has another, which the browser serves
   * side by side.
   */
  get hasSendGroups(): boolean {
    return typeof this.#transport.createSendGroup === 'function';
  }

  /**
   * The session's statistics where the browser has `getStats` (W3C); undefined where it has not
   * (Chromium 141), where it fails, or once the session has ended.
   */
  stats(): Promise<WebTransportStats | undefined> {
    return handled(this.#stats());
  }

  async #stats(): Promise<WebTransportStats | undefined> {
    const getStats = this.#transport.getStats;
    if (this.#end !== undefined || typeof getStats !== 'function') return undefined;
    let value: unknown;
    try {
      value = await attempt(() => getStats.call(this.#transport));
    } catch {
      return undefined;
    }
    if (typeof value !== 'object' || value === null) return undefined;
    const s = value as Readonly<Record<string, unknown>>;
    return Object.freeze({
      bytesSent: integerOf(s['bytesSent']),
      packetsSent: integerOf(s['packetsSent']),
      packetsLost: integerOf(s['packetsLost']),
      bytesReceived: integerOf(s['bytesReceived']),
      packetsReceived: integerOf(s['packetsReceived']),
      smoothedRtt: numberOf(s['smoothedRtt']),
      rttVariation: numberOf(s['rttVariation']),
      minRtt: numberOf(s['minRtt']),
      estimatedSendRate: integerOf(s['estimatedSendRate']),
    });
  }

  /**
   * `createBidirectionalStream`. The browser rejects it while the server grants no stream
   * (Chromium does not wait, whatever `waitUntilAvailable` says), so the provider waits itself: it
   * asks again, the first waiting call first, at growing intervals (5 to 200 ms) until the browser
   * gives a stream or the session ends.
   */
  openStream(): Promise<WebTransportStream> {
    const error = this.#endError;
    if (error !== undefined) return rejected(error);
    const d = new Deferred<WebTransportStream>();
    this.#opens.push(d);
    void this.#openNext();
    return d.promise;
  }

  async #openNext(): Promise<void> {
    if (this.#opening) return;
    this.#opening = true;
    let backoff: number = openBackoff.initial;
    while (this.#opens.length > 0 && this.#end === undefined) {
      let value: unknown;
      try {
        value = await attempt(() =>
          this.#transport.createBidirectionalStream({ waitUntilAvailable: true }),
        );
      } catch {
        if (this.#end !== undefined) break;
        // No stream credit yet, or the session is ending (its end follows).
        await delay(this.#clock, backoff);
        backoff = Math.min(backoff * 2, openBackoff.max);
        continue;
      }
      backoff = openBackoff.initial;
      let stream: WebTransportStream;
      let control: StreamControl;
      try {
        [stream, control] = createStream(this.#link, value);
      } catch (error) {
        this.#opens.shift()?.reject(new QuicError('internal', `openStream: ${String(error)}`));
        continue;
      }
      const ended = this.#endError;
      if (ended !== undefined) {
        control.sessionEnded(ended);
        break;
      }
      const first = this.#opens.shift();
      if (first === undefined) {
        // Nobody waits any more (cannot happen while the session lives).
        control.discard();
        break;
      }
      first.resolve(stream);
    }
    this.#opening = false;
  }

  /**
   * The keying material exporter of the session where the browser has `exportKeyingMaterial` (W3C;
   * [label] and [context] as given, the browser applying WebTransport's per-session derivation);
   * undefined where it has not (Chromium 141), where it fails, and where it does not export
   * [length] bytes. Rejects with 'sessionEnded' once the session has ended, and with a `RangeError`
   * for a [length] that is not positive.
   */
  exportKeyingMaterial(
    label: string,
    context: Uint8Array,
    length: number,
  ): Promise<Uint8Array | undefined> {
    return handled(this.#export(label, context, length));
  }

  async #export(
    label: string,
    context: Uint8Array,
    length: number,
  ): Promise<Uint8Array | undefined> {
    const error = this.#endError;
    if (error !== undefined) throw error;
    if (!Number.isSafeInteger(length) || length <= 0) {
      throw new RangeError(`length ${length}: must be a positive integer`);
    }
    const exporter = this.#transport.exportKeyingMaterial;
    if (typeof exporter !== 'function') return undefined;
    let value: unknown;
    try {
      value = await attempt(() =>
        exporter.call(this.#transport, encoder.encode(label), new Uint8Array(context)),
      );
    } catch {
      const ended = this.#endError;
      if (ended !== undefined) throw ended;
      return undefined;
    }
    let bytes: Uint8Array;
    if (value instanceof ArrayBuffer) {
      bytes = new Uint8Array(value);
    } else if (ArrayBuffer.isView(value)) {
      bytes = new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
    } else {
      return undefined;
    }
    return bytes.length === length ? bytes.slice() : undefined;
  }

  /**
   * `wt.close({ closeCode, reason })`: [code] 0 to 2^32 - 1, [reason] cut to 1024 bytes of UTF-8
   * at a character boundary (Chromium would send a longer one). The browser's network stack sends
   * the close and ends the QUIC connection on its own, whatever the page does next; the promise
   * resolves once the browser reports the session closed, at once. Idempotent.
   *
   * @throws {RangeError} for a code out of range.
   */
  close(code: number, reason: string): Promise<void> {
    checkCloseCode(code);
    if (this.#end === undefined && !this.#closeSent) {
      this.#closeSent = true;
      this.#closeCode = code;
      this.#closeReason = truncateUtf8(reason, maxReasonBytes);
      try {
        this.#transport.close({ closeCode: code, reason: this.#closeReason });
      } catch (error) {
        this.#ended(lostEnd(`close failed: ${String(error)}`));
      }
    }
    return this.#done.promise.then(noop);
  }

  toString(): string {
    return `WebTransportSession(${this.remote})`;
  }

  #makeLink(): SessionLink {
    const session = this;
    return {
      errors: this.#errors,
      get ended(): boolean {
        return session.#end !== undefined;
      },
      gone: (message) => this.#sessionGone(message),
      endedError: (message) => this.#sessionEndedError(message),
      add: (stream) => {
        this.#streams.add(stream);
      },
      remove: (stream) => {
        this.#streams.delete(stream);
      },
      removeReceive: (stream) => {
        this.#receiveStreams.delete(stream);
      },
      sendGroupOf: (priorityClass) => this.#sendGroupOf(priorityClass),
    };
  }

  // Establishing -------------------------------------------------------------------------------

  /** Waits for `ready` within [deadline] ms (0: no deadline). */
  #establish(deadline: number): Promise<WebTransportSession> {
    const connecting = new Deferred<WebTransportSession>();
    this.#connecting = connecting;
    if (deadline > 0) {
      this.#handshakeTimer = this.#clock.setTimeout(() => {
        this.#connectFailed(
          new QuicError(
            'unreachable',
            `the WebTransport session to ${this.remote} was not established within ${deadline} ms`,
          ),
          true,
        );
      }, deadline);
    }
    attempt(() => this.#transport.ready).then(
      () => this.#established(),
      (error: unknown) =>
        this.#connectFailed(
          new QuicError(
            'handshakeFailed',
            `the WebTransport session to ${this.remote} was not established: ` +
              `${describeApiError(readApiError(error))} (a browser tells a refusal, an ` +
              'unreachable server and a rejected certificate apart to no one)',
          ),
        ),
    );
    return connecting.promise;
  }

  #established(): void {
    const connecting = this.#connecting;
    if (connecting === undefined || this.#end !== undefined) return;
    this.#connecting = undefined;
    this.#clearHandshakeTimer();
    const protocol = this.#transport.protocol;
    if (typeof protocol === 'string') {
      this.#protocol = protocol === '' ? undefined : protocol;
    }
    if (this.#drainingSeen) this.#draining.resolve();
    connecting.resolve(this);
    // Someone waited for the server's streams already.
    if (this.#incoming.isWaiting) this.#pumpIncoming();
    if (this.#unidirectional.isWaiting) this.#pumpUnidirectional();
  }

  #connectFailed(error: QuicError, abandon = false): void {
    const connecting = this.#connecting;
    if (connecting === undefined) return;
    this.#connecting = undefined;
    this.#clearHandshakeTimer();
    if (abandon) {
      try {
        this.#transport.close();
      } catch {
        // Closed meanwhile.
      }
    }
    this.#ended(lostEnd(error.message));
    connecting.reject(error);
  }

  #clearHandshakeTimer(): void {
    if (this.#handshakeTimer !== undefined) {
      this.#clock.clearTimeout(this.#handshakeTimer);
      this.#handshakeTimer = undefined;
    }
  }

  // The end ------------------------------------------------------------------------------------

  #onClosed(value: unknown): void {
    if (this.#end !== undefined) return;
    if (this.#connecting !== undefined) {
      this.#connectFailed(
        new QuicError(
          'handshakeFailed',
          `the WebTransport session to ${this.remote} closed before it was established`,
        ),
      );
      return;
    }
    let code = 0;
    let reason = '';
    if (typeof value === 'object' && value !== null) {
      const info = value as { readonly closeCode?: unknown; readonly reason?: unknown };
      code = integerOf(info.closeCode) ?? 0;
      if (typeof info.reason === 'string') reason = info.reason;
    }
    const local = this.#closeSent && code === this.#closeCode && reason === this.#closeReason;
    this.#ended(Object.freeze({ code, reason, byPeer: !local, lost: false }));
  }

  #onClosedError(error: unknown): void {
    if (this.#end !== undefined) return;
    const e = readApiError(error);
    if (this.#connecting !== undefined) {
      this.#connectFailed(
        new QuicError(
          'handshakeFailed',
          `the WebTransport session to ${this.remote} was not established: ${describeApiError(e)}`,
        ),
      );
      return;
    }
    // A loss, also after this side's close: the browser resolves `closed` at once with the close
    // it took, so a rejection means that the session had failed before the close could be sent.
    this.#ended(lostEnd(messageOf(e)));
  }

  /**
   * A stream reported the end of the session (a `WebTransportError` of source `session`), which
   * the browser reports on the streams before `closed` settles: the session ends as lost if
   * `closed` does not settle within two seconds.
   */
  #sessionGone(message: string): void {
    if (this.#end !== undefined || this.#goneTimer !== undefined) return;
    this.#goneTimer = this.#clock.setTimeout(() => {
      this.#goneTimer = undefined;
      this.#ended(lostEnd(message));
    }, sessionGoneGrace);
  }

  /** The error what is pending fails with once the session ends, or is ending ([message] from the browser). */
  #sessionEndedError(message?: string): QuicError {
    return (
      this.#endError ??
      new QuicError(
        'sessionEnded',
        message === undefined || message === ''
          ? 'the session has ended'
          : `the session has ended: ${message}`,
      )
    );
  }

  #ended(end: QuicSessionEnd): void {
    if (this.#end !== undefined) return;
    this.#end = end;
    const error = new QuicError(
      'sessionEnded',
      end.lost
        ? `the session was lost: ${end.reason}`
        : `the session was closed with ${end.code}${end.byPeer ? ' by the peer' : ''}: ${end.reason}`,
    );
    this.#endError = error;
    if (this.#goneTimer !== undefined) {
      this.#clock.clearTimeout(this.#goneTimer);
      this.#goneTimer = undefined;
    }
    this.#clearHandshakeTimer();
    for (let d = this.#opens.shift(); d !== undefined; d = this.#opens.shift()) d.reject(error);
    for (const stream of [...this.#streams]) stream.sessionEnded(error);
    for (const stream of [...this.#receiveStreams]) stream.release();
    this.#incoming.end();
    this.#unidirectional.end();
    if (!this.#incomingPumping) this.#incomingReader = releaseReader(this.#incomingReader);
    if (!this.#unidirectionalPumping) {
      this.#unidirectionalReader = releaseReader(this.#unidirectionalReader);
    }
    this.#done.resolve(end);
  }

  // The server's streams ---------------------------------------------------------------------

  #pumpIncoming(): void {
    if (
      this.#incomingPumping ||
      this.#incomingDone ||
      this.#end !== undefined ||
      this.#connecting !== undefined
    ) {
      return;
    }
    this.#incomingPumping = true;
    void this.#runIncoming();
  }

  /** Reads the server's next bidirectional stream while someone waits for one: the others wait in the browser. */
  async #runIncoming(): Promise<void> {
    try {
      this.#incomingReader ??= this.#transport.incomingBidirectionalStreams.getReader();
      const reader = this.#incomingReader;
      while (this.#end === undefined && this.#incoming.isWaiting) {
        const result = await reader.read();
        if (result.done) {
          this.#incomingDone = true;
          break;
        }
        let stream: WebTransportStream;
        let control: StreamControl;
        try {
          [stream, control] = createStream(this.#link, result.value);
        } catch {
          // Not a stream pair: nothing to take.
          continue;
        }
        const error = this.#endError;
        if (error !== undefined) {
          control.sessionEnded(error);
          break;
        }
        if (!this.#incoming.deliver(stream)) {
          // Nobody takes it any more.
          control.discard();
          break;
        }
      }
    } catch {
      // The session is ending; `closed` tells how.
    }
    this.#incomingPumping = false;
    if (this.#end !== undefined) this.#incomingReader = releaseReader(this.#incomingReader);
  }

  #pumpUnidirectional(): void {
    if (
      this.#unidirectionalPumping ||
      this.#unidirectionalDone ||
      this.#end !== undefined ||
      this.#connecting !== undefined
    ) {
      return;
    }
    this.#unidirectionalPumping = true;
    void this.#runUnidirectional();
  }

  async #runUnidirectional(): Promise<void> {
    try {
      this.#unidirectionalReader ??= this.#transport.incomingUnidirectionalStreams.getReader();
      const reader = this.#unidirectionalReader;
      while (this.#end === undefined && this.#unidirectional.isWaiting) {
        const result = await reader.read();
        if (result.done) {
          this.#unidirectionalDone = true;
          break;
        }
        const readable = result.value as ReadableStream<unknown> | null | undefined;
        if (typeof readable?.cancel !== 'function') continue;
        const [stream, control] = createReceiveStream(this.#link, readable);
        if (this.#end !== undefined) {
          stream.stop(0);
          break;
        }
        this.#receiveStreams.add(control);
        if (!this.#unidirectional.deliver(stream)) {
          stream.stop(0);
          break;
        }
      }
    } catch {
      // The session is ending.
    }
    this.#unidirectionalPumping = false;
    if (this.#end !== undefined) {
      this.#unidirectionalReader = releaseReader(this.#unidirectionalReader);
    }
  }

  /**
   * The send group of the priority class [priorityClass], creating the session's two groups on
   * first use; undefined where the browser has none.
   */
  #sendGroupOf(priorityClass: number): unknown {
    const create = this.#transport.createSendGroup;
    if (typeof create !== 'function') return undefined;
    try {
      if (priorityClass > 0) {
        this.#ordinaryGroup ??= create.call(this.#transport);
        return this.#ordinaryGroup;
      }
      this.#bulkGroup ??= create.call(this.#transport);
      return this.#bulkGroup;
    } catch {
      return undefined;
    }
  }
}

function lostEnd(reason: string): QuicSessionEnd {
  return Object.freeze({ code: undefined, reason, byPeer: false, lost: true });
}

/** The receive direction of one of the server's unidirectional streams. */
export class WebTransportReceiveStream implements QuicReceiveStream {
  static {
    createReceiveStream = (link, readable) => {
      const stream = new WebTransportReceiveStream(link, readable);
      return [stream, stream.#control];
    };
  }

  readonly #link: SessionLink;
  readonly #readable: ReadableStream<unknown>;
  readonly #control: ReceiveControl = { release: () => this.#release() };
  #released = false;

  private constructor(link: SessionLink, readable: ReadableStream<unknown>) {
    this.#link = link;
    this.#readable = readable;
  }

  /**
   * `readable.cancel(new WebTransportError({ streamErrorCode: code }))`, the `STOP_SENDING` (a code
   * the browser carries, see {@link WebTransportProvider.maxStreamErrorCode}). Idempotent, also
   * after the session ended.
   *
   * @throws {RangeError} for a code the browser cannot carry.
   */
  stop(code: number): void {
    checkStreamCode(code, this.#link.errors);
    if (this.#released) return;
    const reason = this.#link.errors.create(code, 'STOP_SENDING');
    attempt(() => this.#readable.cancel(reason)).catch(noop);
    this.#release();
  }

  #release(): void {
    this.#released = true;
    this.#link.removeReceive(this.#control);
  }
}

// --- Streams -----------------------------------------------------------------------------------

/** One write, or the finish, queued on a stream. */
class Write {
  readonly bytes: Uint8Array;
  readonly finish: boolean;
  readonly done = new Deferred<void>();

  constructor(bytes: Uint8Array, finish: boolean) {
    this.bytes = bytes;
    this.finish = finish;
  }
}

/**
 * One bidirectional stream of a {@link WebTransportSession}: the default reader of its readable
 * and the writer of its writable, held for the stream's life and released once each direction has
 * ended.
 */
export class WebTransportStream implements QuicStream {
  static {
    createStream = (link, pair) => {
      if (!isPair(pair)) throw new TypeError('not a WebTransportBidirectionalStream');
      const stream = new WebTransportStream(link, pair.readable, pair.writable);
      return [stream, stream.#control];
    };
  }

  readonly #link: SessionLink;
  readonly #control: StreamControl;
  readonly #writable: WritableStream<Uint8Array> & SendStreamExtras;
  readonly #reader: ReadableStreamDefaultReader<unknown>;
  readonly #writer: WritableStreamDefaultWriter<Uint8Array>;
  #priority = 1;

  // Receive direction.
  #chunk: Uint8Array | undefined;
  #offset = 0;
  #read: Deferred<Uint8Array | undefined> | undefined;
  #readMax = 0;
  #pulling = false;
  #cancelling = false;
  #fin = false;
  #recvFailure: Error | undefined;
  #recvEnded = false;
  #readerReleased = false;
  readonly #reset = new Deferred<number | undefined>();

  // Send direction.
  readonly #writes: Write[] = [];
  readonly #handed = new Set<Write>();
  #pumping = false;
  #finishing: Promise<void> | undefined;
  #resetSent = false;
  #peerStop = false;
  #sendFailure: Error | undefined;
  #sendEnded = false;
  #writerReleased = false;
  readonly #stopped = new Deferred<number | undefined>();

  private constructor(
    link: SessionLink,
    readable: ReadableStream<unknown>,
    writable: WritableStream<Uint8Array>,
  ) {
    this.#link = link;
    this.#control = {
      sessionEnded: (error) => this.#sessionEnded(error),
      discard: () => {
        this.stop(0);
        this.reset(0);
      },
    };
    this.#writable = writable;
    this.#reader = readable.getReader();
    this.#writer = writable.getWriter();
    link.add(this.#control);
    attempt(() => this.#reader.closed).then(noop, (error: unknown) => this.#receiveFailed(error));
    attempt(() => this.#writer.closed).then(
      () => this.#sendClosed(),
      (error: unknown) => this.#sendFailed(error),
    );
  }

  /**
   * Resolves with the server's `RESET_STREAM` code (the readable's `closed` rejected with a
   * `WebTransportError` of source `stream`; undefined when it carried no code, or the rejection was
   * no `WebTransportError`), whether a read is pending or not; rejects with 'sessionEnded' when the
   * session ends first; stays pending after FIN or a local {@link stop}.
   */
  get resetReceived(): Promise<number | undefined> {
    return this.#reset.promise;
  }

  /**
   * Resolves with the server's `STOP_SENDING` code (the writable's `closed` rejected with it; a
   * `STOP_SENDING` without a code rejects it with a {@link QuicStreamStopped} of undefined), or
   * with undefined once the browser closed this side's direction after {@link finish} (it does not
   * tell when the server acknowledged the data) or at once after {@link reset}.
   */
  get stopped(): Promise<number | undefined> {
    return this.#stopped.promise;
  }

  /** Whether the browser has send orders on this stream (`sendOrder` on the writable, W3C `WebTransportSendStream`); Chromium 141 has none. */
  get hasSendOrder(): boolean {
    return typeof this.#writable.sendOrder === 'number';
  }

  /** The priority class last set: 2 control, 1 ordinary (the default), 0 bulk. */
  get priority(): number {
    return this.#priority;
  }

  /**
   * Where the browser has send orders: {@link WebTransportProvider.sendOrderOf} the class, in the
   * session's send group of the class where it has send groups. Chromium 141 has neither: its
   * streams share the browser's own scheduling, the control stream included.
   *
   * @throws {RangeError} for a class that is not an integer.
   */
  set priority(value: number) {
    if (!Number.isInteger(value)) throw new RangeError(`priority ${value}: must be an integer`);
    this.#priority = value;
    if (this.#sendEnded || !this.hasSendOrder) return;
    try {
      const group = this.#link.sendGroupOf(value);
      if (group !== undefined) this.#writable.sendGroup = group;
      this.#writable.sendOrder = WebTransportProvider.sendOrderOf(value);
    } catch {
      // A stream that ended meanwhile.
    }
  }

  // Reading ------------------------------------------------------------------------------------

  /**
   * Reads at least one and at most [max] bytes; undefined at the server's FIN and forever after.
   * Rejects with {@link QuicStreamReset} after the server's reset or a local {@link stop}, and
   * with 'sessionEnded' once the session ended.
   *
   * @throws {RangeError} for a [max] that is not a positive integer.
   * @throws {Error} while another read is pending.
   */
  read(max: number): Promise<Uint8Array | undefined> {
    if (!Number.isSafeInteger(max) || max <= 0) {
      throw new RangeError(`max ${max}: must be a positive integer`);
    }
    if (this.#read !== undefined) throw new Error('a read is pending on this stream');
    const failure = this.#recvFailure;
    if (failure !== undefined) return rejected(failure);
    const buffered = this.#take(max);
    if (buffered !== undefined) return Promise.resolve(buffered);
    if (this.#fin) return Promise.resolve(undefined);
    this.#readMax = max;
    const d = new Deferred<Uint8Array | undefined>();
    this.#read = d;
    this.#pull();
    return d.promise;
  }

  /** At most [max] bytes of the chunk read last; undefined when none is left. */
  #take(max: number): Uint8Array | undefined {
    const chunk = this.#chunk;
    if (chunk === undefined) return undefined;
    const n = Math.min(max, chunk.length - this.#offset);
    const bytes = chunk.subarray(this.#offset, this.#offset + n);
    this.#offset += n;
    if (this.#offset === chunk.length) {
      this.#chunk = undefined;
      this.#offset = 0;
    }
    return bytes;
  }

  #pull(): void {
    if (this.#pulling) return;
    this.#pulling = true;
    attempt(() => this.#reader.read()).then(
      (result) => this.#onChunk(result),
      (error: unknown) => this.#onReadFailed(error),
    );
  }

  #onChunk(result: ReadableStreamReadResult<unknown>): void {
    this.#pulling = false;
    if (this.#recvEnded) {
      this.#releaseReader();
      return;
    }
    if (result.done) {
      this.#fin = true;
      const d = this.#read;
      this.#read = undefined;
      d?.resolve(undefined);
      this.#endRecv();
      return;
    }
    const chunk = bytesOf(result.value);
    if (chunk.length === 0) {
      this.#pull();
      return;
    }
    this.#chunk = chunk;
    this.#offset = 0;
    const d = this.#read;
    if (d !== undefined) {
      this.#read = undefined;
      d.resolve(this.#take(this.#readMax));
    }
  }

  #onReadFailed(error: unknown): void {
    this.#pulling = false;
    if (this.#recvEnded) {
      this.#releaseReader();
      return;
    }
    this.#receiveFailed(error);
  }

  /** The readable failed (a read, or its `closed`): the server's reset, or the end of the session. */
  #receiveFailed(error: unknown): void {
    if (this.#recvEnded) return;
    const e = readApiError(error);
    if (fromSession(e) || this.#link.ended) {
      this.#link.gone(messageOf(e));
      this.#failReceive(this.#link.endedError(e.message));
      return;
    }
    const code = fromStream(e) ? e.streamErrorCode : undefined;
    this.#reset.resolve(code);
    this.#failReceive(new QuicStreamReset(code));
  }

  #failReceive(failure: Error): void {
    this.#recvFailure = failure;
    this.#chunk = undefined;
    if (failure instanceof QuicError) this.#reset.reject(failure);
    const d = this.#read;
    if (d !== undefined) {
      this.#read = undefined;
      d.reject(failure);
    }
    this.#endRecv();
  }

  /**
   * `reader.cancel(new WebTransportError({ streamErrorCode: code }))`, the `STOP_SENDING`: this
   * direction ends at once, a pending read and every later one failing with
   * {@link QuicStreamReset} of [code]. No-op once this direction ended.
   *
   * @throws {RangeError} for a code the browser cannot carry.
   */
  stop(code: number): void {
    checkStreamCode(code, this.#link.errors);
    if (this.#recvEnded) return;
    const failure = new QuicStreamReset(code);
    this.#recvFailure = failure;
    this.#chunk = undefined;
    const d = this.#read;
    if (d !== undefined) {
      this.#read = undefined;
      d.reject(failure);
    }
    this.#cancelling = true;
    const reason = this.#link.errors.create(code, 'STOP_SENDING');
    attempt(() => this.#reader.cancel(reason))
      .catch(noop)
      .then(() => {
        this.#cancelling = false;
        this.#releaseReader();
      });
    this.#endRecv();
  }

  #endRecv(): void {
    if (this.#recvEnded) return;
    this.#recvEnded = true;
    this.#chunk = undefined;
    this.#releaseReader();
    if (this.#sendEnded) this.#link.remove(this.#control);
  }

  /** Releases the reader's lock once this direction has ended and nothing of the reader is pending. */
  #releaseReader(): void {
    if (!this.#recvEnded || this.#pulling || this.#cancelling || this.#readerReleased) return;
    this.#readerReleased = true;
    try {
      this.#reader.releaseLock();
    } catch {
      // Released with the stream.
    }
  }

  // Writing ------------------------------------------------------------------------------------

  /**
   * Writes a copy of [bytes]; resolves once the browser took them. Writes queue in order, each
   * handed to the browser as its writer's `ready` allows. Rejects with {@link QuicStreamStopped}
   * after the server's `STOP_SENDING`, with an Error after {@link finish} or {@link reset}, and
   * with 'sessionEnded' once the session ended.
   *
   * @throws {TypeError} for anything but a `Uint8Array`.
   */
  write(bytes: Uint8Array): Promise<void> {
    if (!(bytes instanceof Uint8Array)) throw new TypeError('write takes a Uint8Array');
    const failure = this.#sendFailure;
    if (failure !== undefined) return rejected(failure);
    if (this.#finishing !== undefined || this.#resetSent) {
      return rejected(new Error('the stream was finished or reset'));
    }
    const w = new Write(new Uint8Array(bytes), false);
    this.#writes.push(w);
    void this.#pump();
    return w.done.promise;
  }

  /** `writer.close()` after everything written: resolves once the browser closed the direction. Idempotent. */
  finish(): Promise<void> {
    const f = this.#finishing;
    if (f !== undefined) return f;
    const failure = this.#sendFailure;
    if (failure !== undefined) return rejected(failure);
    if (this.#resetSent) return rejected(new Error('the stream was reset'));
    const w = new Write(new Uint8Array(0), true);
    this.#writes.push(w);
    void this.#pump();
    this.#finishing = w.done.promise;
    return this.#finishing;
  }

  /** Hands the queued writes to the writer in order, each once its `ready` allows (the browser's backpressure). */
  async #pump(): Promise<void> {
    if (this.#pumping) return;
    this.#pumping = true;
    try {
      while (this.#writes.length > 0 && !this.#sendEnded && !this.#resetSent) {
        let desired: number | null;
        try {
          desired = this.#writer.desiredSize;
        } catch {
          break;
        }
        if (desired === null) break;
        if (desired <= 0) {
          try {
            await this.#writer.ready;
          } catch {
            // Errored: its `closed` tells why.
            break;
          }
          continue;
        }
        const w = this.#writes.shift() as Write;
        this.#handed.add(w);
        attempt(() => (w.finish ? this.#writer.close() : this.#writer.write(w.bytes))).then(
          () => {
            this.#handed.delete(w);
            w.done.resolve();
          },
          (error: unknown) => {
            this.#handed.delete(w);
            w.done.reject(this.#sendError(error));
          },
        );
      }
    } finally {
      this.#pumping = false;
    }
  }

  /** What a failed write or finish reports. */
  #sendError(error: unknown): Error {
    if (this.#resetSent) return new Error('the stream was reset');
    const failure = this.#sendFailure;
    if (failure !== undefined) return failure;
    const e = readApiError(error);
    if (fromStream(e)) {
      this.#peerStopped(e.streamErrorCode);
      return this.#sendFailure ?? new QuicStreamStopped(e.streamErrorCode);
    }
    if (fromSession(e) || this.#link.ended) {
      this.#link.gone(messageOf(e));
      return this.#link.endedError(e.message);
    }
    return new QuicError('internal', `write failed: ${describeApiError(e)}`);
  }

  /** The writable's `closed` resolved: the browser closed the direction after FIN. */
  #sendClosed(): void {
    this.#stopped.resolve(undefined);
    this.#endSend();
  }

  /**
   * The writable's `closed` rejected: the server's `STOP_SENDING`, the end of the session, this
   * side's own reset (or released lock), or a failure of the browser's, which ends this direction
   * too.
   */
  #sendFailed(error: unknown): void {
    if (this.#resetSent || this.#sendEnded) return;
    const e = readApiError(error);
    if (fromStream(e)) {
      this.#peerStopped(e.streamErrorCode);
      return;
    }
    let failure: Error;
    if (fromSession(e) || this.#link.ended) {
      this.#link.gone(messageOf(e));
      failure = this.#link.endedError(e.message);
    } else {
      failure = new QuicError('internal', `the stream failed: ${describeApiError(e)}`);
    }
    this.#sendFailure = failure;
    this.#failWrites(failure);
    this.#stopped.reject(failure);
    this.#endSend();
  }

  #peerStopped(code: number | undefined): void {
    if (this.#peerStop || this.#sendEnded) return;
    this.#peerStop = true;
    const failure = new QuicStreamStopped(code);
    this.#sendFailure = failure;
    this.#failWrites(failure);
    if (code === undefined) {
      // Not to be read as an acknowledgement.
      this.#stopped.reject(failure);
    } else {
      this.#stopped.resolve(code);
    }
    this.#endSend();
  }

  #failWrites(error: Error): void {
    for (const w of [...this.#handed, ...this.#writes]) w.done.reject(error);
    this.#handed.clear();
    this.#writes.length = 0;
  }

  /**
   * `writer.abort(new WebTransportError({ streamErrorCode: code }))`, the `RESET_STREAM` (a code
   * the browser carries, see {@link WebTransportProvider.maxStreamErrorCode}). After
   * {@link finish}, once the browser has sent the FIN (Chromium sends it at once) the abort is too
   * late to reach the server, which reads the FIN. No-op once this direction ended.
   *
   * @throws {RangeError} for a code the browser cannot carry.
   */
  reset(code: number): void {
    checkStreamCode(code, this.#link.errors);
    if (this.#sendEnded || this.#resetSent || this.#sendFailure !== undefined) return;
    this.#resetSent = true;
    this.#failWrites(new Error('the stream was reset'));
    const reason = this.#link.errors.create(code, 'RESET_STREAM');
    attempt(() => this.#writer.abort(reason)).catch(noop);
    this.#stopped.resolve(undefined);
    this.#endSend();
  }

  #endSend(): void {
    if (this.#sendEnded) return;
    this.#sendEnded = true;
    if (!this.#writerReleased) {
      this.#writerReleased = true;
      try {
        this.#writer.releaseLock();
      } catch {
        // Released with the stream.
      }
    }
    if (this.#recvEnded) this.#link.remove(this.#control);
  }

  // The end ------------------------------------------------------------------------------------

  #sessionEnded(error: QuicError): void {
    this.#sendFailure ??= error;
    this.#recvFailure ??= error;
    const d = this.#read;
    if (d !== undefined) {
      this.#read = undefined;
      d.reject(error);
    }
    this.#failWrites(error);
    this.#stopped.reject(error);
    this.#reset.reject(error);
    this.#endRecv();
    this.#endSend();
  }
}

/** A chunk of a readable as bytes: Chromium reads `Uint8Array`s; anything else is read as bytes where it can be. */
function bytesOf(value: unknown): Uint8Array {
  if (value instanceof Uint8Array) return value;
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  return new Uint8Array(0);
}
