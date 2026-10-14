/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * The native QUIC provider: raw QUIC (`quic://host:port`, ALPN `switchboard`) and WebTransport
 * over HTTP/3 (`https://host:port/path`), as client and listener, over quinn through the sbq shim
 * (`native/sbq-vendor`) that the N-API addon in `native/` exposes function for function. A port
 * of `switchboard_quic`'s `native_provider.dart`; it runs on Node.js, Bun and Deno.
 *
 * Every call into the shim is synchronous and returns at once; an operation that cannot complete
 * yet leaves a {@link Deferred} behind, and the shim's event for the handle (READABLE, WRITABLE,
 * OPENED, ...) polls it again. Events arrive through one thread-safe function per context (the
 * doorbell), which drains the context's event queue with `poll`. The doorbell holds the event
 * loop open only while a session or listener is live; the context is shut down when the last one
 * ends, and started again on the next use.
 *
 * Reads are pull-based: data stays in quinn, holding back the peer's flow control credit, until
 * {@link QuicStream.read} takes it. Writes are copied, and complete once quinn accepted every
 * byte, which the peer's credit bounds. While no read is pending, the stream asks the shim
 * whether the peer reset its direction (`streamPollReset`, which arms the same READABLE event),
 * so that {@link QuicStream.resetReceived} resolves for a paused reader too.
 *
 * Raw QUIC has no session layer: {@link QuicSession.draining} never resolves, and
 * {@link QuicSession.incomingUnidirectionalStreams} never delivers a stream (the shim grants the
 * peer none, so a peer that opens one commits a transport error).
 *
 * WebTransport: the shim runs HTTP/3 and the session (its CONNECT, its capsules, the stream
 * headers, the mapping of codes); a session is a handle the same calls take as a connection.
 * Stream and close codes are WebTransport application codes (0 to 2^32 - 1); a reset or
 * `STOP_SENDING` outside the WebTransport range reaches the application without a code
 * (undefined). {@link QuicSession.draining} resolves on `WT_DRAIN_SESSION` or an HTTP/3 `GOAWAY`;
 * the peer's unidirectional streams are delivered for the application to refuse.
 *
 * Codes are numbers: raw QUIC carries 0 to 2^62 - 1, of which this API sends 0 to 2^53 - 1
 * (`Number.MAX_SAFE_INTEGER`); a larger code received from a peer is reported rounded to the
 * nearest number.
 *
 * The addon is loaded on first use, never at import: `$SWITCHBOARD_QUIC_ADDON` when set (the path
 * of a `.node` file, and nothing else is tried), else `native/sbq-node.node` of this package (what
 * `npm run build:native` writes), else the per-platform package
 * `@polyverse/switchboard-quic-<platform>-<arch>[-gnu|-musl|-msvc]`. Without it, connecting
 * rejects and listening fails with a {@link QuicError} of kind 'unsupportedScheme' that names the
 * package to install.
 */

import { createHash } from 'node:crypto';
import { lookup } from 'node:dns/promises';
import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { isIP } from 'node:net';
import { fileURLToPath } from 'node:url';
import {
  defaultQuicLimits,
  type QuicClientConfig,
  type QuicConnector,
  QuicError,
  type QuicListener,
  type QuicReceiveStream,
  type QuicServerConfig,
  type QuicSession,
  type QuicSessionEnd,
  type QuicStream,
  QuicStreamReset,
  QuicStreamStopped,
} from '../../core/quic/contract.ts';
import { AsyncQueue } from '../../core/util/async-queue.ts';
import { Deferred } from '../../core/util/deferred.ts';

// The C ABI of the shim (native/sbq-vendor/include/sbq.h), version 4.
const SBQ_ABI_VERSION = 4;
const SBQ_OK = 0;
const SBQ_WOULD_BLOCK = 0;
const SBQ_FIN = -1;
const SBQ_ERR_CONNECTION = -3;
const SBQ_ERR_ARG = -5;
const SBQ_ERR_IO = -7;
const SBQ_ERR_STATE = -8;
const SBQ_NO_CODE = -10;
const SBQ_RETRY = -11;
const SBQ_CODE_BASE = 16;
const SBQ_EV_INCOMING = 1;
const SBQ_EV_CONNECTED = 2;
const SBQ_EV_CLOSED = 3;
const SBQ_EV_STREAM = 4;
const SBQ_EV_OPENED = 5;
const SBQ_EV_READABLE = 6;
const SBQ_EV_WRITABLE = 7;
const SBQ_EV_STOPPED = 8;
const SBQ_EV_FINISHED_ACKED = 9;
const SBQ_EV_WT_REQUEST = 10;
const SBQ_EV_DRAINING = 11;
const SBQ_EV_UNI_STREAM = 12;
const SBQ_CLOSE_APP_REMOTE = 1;
const SBQ_CLOSE_APP_LOCAL = 2;
const SBQ_CLOSE_TIMED_OUT = 3;
const SBQ_CLOSE_CERT_REJECTED = 7;
const SBQ_CLOSE_UNREACHABLE = 8;
const SBQ_CLOSE_REFUSED = 10;
const SBQ_WT_AUTHORITY = 1;
const SBQ_WT_PATH = 2;
const SBQ_WT_ORIGIN = 3;
const SBQ_WT_PROTOCOLS = 4;
const SBQ_VERIFY_PLATFORM = 0;
const SBQ_VERIFY_PINNED = 1;
const SBQ_VERIFY_NONE = 2;

/**
 * A result of the shim: the integer its C function returns, as a number, or as a bigint when its
 * magnitude exceeds 2^53 - 1 (only the code of a reset or `STOP_SENDING` at or above 2^53 - 16).
 */
type SbqResult = number | bigint;

/** The addon's surface (native/src/lib.rs): the C ABI, one function per `sbq_*` function. */
interface SbqAddon {
  abiVersion(): number;
  init(doorbell: (ctx: number) => void, workerThreads: number): number;
  setActive(ctx: number, active: boolean): boolean;
  shutdown(ctx: number): number;
  poll(ctx: number, out: Float64Array): number;
  lastError(out: Uint8Array): SbqResult;
  generateSelfSigned(
    subject: Uint8Array,
    validityDays: number,
    certOut: Uint8Array,
    keyOut: Uint8Array,
    lengths: Float64Array,
  ): number;
  clientConfig(
    alpn: Uint8Array,
    verify: number,
    pins: Uint8Array,
    pinCount: number,
    maxBidi: number,
    streamWindow: number,
    connWindow: number,
    idleMs: number,
    keepAliveMs: number,
    handshakeMs: number,
  ): SbqResult;
  serverConfig(
    alpn: Uint8Array,
    cert: Uint8Array,
    key: Uint8Array,
    maxBidi: number,
    streamWindow: number,
    connWindow: number,
    idleMs: number,
    keepAliveMs: number,
    handshakeMs: number,
  ): SbqResult;
  configFree(config: number): number;
  listen(ctx: number, config: number, host: Uint8Array, port: number): SbqResult;
  listenerPort(listener: number): number;
  listenerClose(listener: number): number;
  connect(
    ctx: number,
    config: number,
    host: Uint8Array,
    port: number,
    serverName: Uint8Array,
  ): SbqResult;
  connectWt(
    ctx: number,
    config: number,
    host: Uint8Array,
    port: number,
    serverName: Uint8Array,
    authority: Uint8Array,
    path: Uint8Array,
    origin: Uint8Array,
    protocols: Uint8Array,
  ): SbqResult;
  wtRequest(session: number, field: number, out: Uint8Array): SbqResult;
  wtAccept(session: number, protocol: Uint8Array): number;
  wtReject(session: number, status: number): number;
  wtDrain(session: number): number;
  connClose(conn: number, code: number, reason: Uint8Array): number;
  connRelease(conn: number): number;
  connCloseReason(conn: number, out: Uint8Array): SbqResult;
  connExporter(conn: number, label: Uint8Array, context: Uint8Array, out: Uint8Array): number;
  connPeerCertSha256(conn: number, out: Uint8Array): number;
  connProtocol(conn: number, out: Uint8Array): SbqResult;
  connRemote(conn: number, out: Uint8Array): SbqResult;
  connSetMaxBidi(conn: number, count: number): number;
  connStats(conn: number, out: Float64Array): number;
  streamOpen(conn: number): SbqResult;
  streamWrite(stream: number, data: Uint8Array): SbqResult;
  streamRead(stream: number, buf: Uint8Array): SbqResult;
  streamPollReset(stream: number): SbqResult;
  streamFinish(stream: number): SbqResult;
  streamReset(stream: number, code: number): SbqResult;
  streamStop(stream: number, code: number): SbqResult;
  streamSetPriority(stream: number, priority: number): SbqResult;
  streamRelease(stream: number): number;
}

/** The ALPN protocol of HTTP/3, which the provider offers itself for WebTransport. */
const h3Alpn = 'h3';

/** The largest raw QUIC application code this API sends (QUIC carries up to 2^62 - 1). */
const maxCode = Number.MAX_SAFE_INTEGER;

/** The largest WebTransport application code (a `u32`). */
const maxWebTransportCode = 0xffffffff;

const encoder = new TextEncoder();
const decoder = new TextDecoder();

function utf8(text: string): Uint8Array {
  return encoder.encode(text);
}

function checkCode(code: number, webTransport: boolean): void {
  const max = webTransport ? maxWebTransportCode : maxCode;
  if (!Number.isInteger(code) || code < 0 || code > max) {
    throw new RangeError(
      `code ${code}: must be an integer from 0 to ${webTransport ? '2^32 - 1' : '2^53 - 1'}`,
    );
  }
}

/** Whether a result carries an application code (a reset or a `STOP_SENDING` of the peer). */
function isCode(r: SbqResult): boolean {
  return typeof r === 'bigint' || r <= -SBQ_CODE_BASE;
}

/** The application code of a result {@link isCode} accepts. */
function codeOf(r: SbqResult): number {
  return typeof r === 'bigint' ? Number(-r - BigInt(SBQ_CODE_BASE)) : -r - SBQ_CODE_BASE;
}

/** [promise], marked as handled: a caller that drops it sees no unhandled rejection. */
function handled<T>(promise: Promise<T>): Promise<T> {
  promise.catch(() => {});
  return promise;
}

function rejected<T>(error: unknown): Promise<T> {
  return handled(Promise.reject(error));
}

/** Reports an error nothing can handle, as an uncaught exception (Dart: the zone's handler). */
function reportUncaught(error: unknown): void {
  queueMicrotask(() => {
    throw error;
  });
}

// --- Loading the addon -------------------------------------------------------------------------

/** The prefix of the per-platform packages that carry a prebuilt addon. */
const packagePrefix = '@polyverse/switchboard-quic';

type Loaded =
  | { readonly addon: SbqAddon; readonly path: string }
  | { readonly message: string; readonly cause: unknown };

let loaded: Loaded | undefined;

function environment(name: string): string | undefined {
  try {
    return process.env[name];
  } catch {
    // Deno without --allow-env.
    return undefined;
  }
}

function isMusl(): boolean {
  try {
    if (readFileSync('/usr/bin/ldd', 'latin1').includes('musl')) return true;
  } catch {
    // No ldd: ask the report.
  }
  try {
    const report = process.report?.getReport() as { header?: { glibcVersionRuntime?: string } };
    return report.header !== undefined && report.header.glibcVersionRuntime === undefined;
  } catch {
    return false;
  }
}

/** The per-platform package's suffix: `linux-x64-gnu`, `darwin-arm64`, `win32-x64-msvc`, ... */
function platformTarget(): string {
  const { platform, arch } = process;
  if (platform === 'linux') return `linux-${arch}-${isMusl() ? 'musl' : 'gnu'}`;
  if (platform === 'win32') return `win32-${arch}-msvc`;
  return `${platform}-${arch}`;
}

function load(): Loaded {
  if (loaded !== undefined) return loaded;
  const require = createRequire(import.meta.url);
  const target = platformTarget();
  const packageName = `${packagePrefix}-${target}`;
  const override = environment('SWITCHBOARD_QUIC_ADDON');
  const candidates =
    override !== undefined && override !== ''
      ? [override]
      : [fileURLToPath(new URL('../../../native/sbq-node.node', import.meta.url)), packageName];
  const failures: string[] = [];
  for (const candidate of candidates) {
    let addon: SbqAddon;
    try {
      addon = require(candidate) as SbqAddon;
    } catch (error) {
      failures.push(`${candidate}: ${error instanceof Error ? error.message : String(error)}`);
      continue;
    }
    const version = typeof addon.abiVersion === 'function' ? addon.abiVersion() : undefined;
    if (version !== SBQ_ABI_VERSION) {
      failures.push(`${candidate}: ABI ${version}, this package expects ${SBQ_ABI_VERSION}`);
      continue;
    }
    loaded = { addon, path: candidate };
    return loaded;
  }
  const needs =
    `the native QUIC provider needs its addon for ${target}: install ${packageName}, ` +
    'or build it with `npm run build:native` (native/README.md)';
  const message =
    override !== undefined && override !== ''
      ? `$SWITCHBOARD_QUIC_ADDON (${override}) cannot be loaded; ${needs}`
      : needs;
  loaded = { message, cause: new Error(failures.join('; ')) };
  return loaded;
}

function requireAddon(): SbqAddon {
  const l = load();
  if ('addon' in l) return l.addon;
  throw new QuicError('unsupportedScheme', l.message, { cause: l.cause });
}

/** Whether the native QUIC provider can run here: its addon, or why it is not available. */
export type NativeQuicAvailability =
  | { readonly available: true; readonly path: string }
  | { readonly available: false; readonly reason: string };

/**
 * Whether the addon loads (it is loaded now if it was not), and from where; or, when it does not,
 * the message connecting would reject with.
 */
export function nativeQuicAvailability(): NativeQuicAvailability {
  const l = load();
  return 'addon' in l
    ? { available: true, path: l.path }
    : { available: false, reason: `${l.message} (${String((l.cause as Error).message)})` };
}

// --- Strings and buffers across the ABI --------------------------------------------------------

/** A buffer the shim copies out: [call] with a buffer returns the full length, or an error. */
function bytesOut(call: (out: Uint8Array) => SbqResult): Uint8Array | undefined {
  let buffer = new Uint8Array(256);
  let n = Number(call(buffer));
  if (n > buffer.length) {
    buffer = new Uint8Array(n);
    n = Number(call(buffer));
  }
  if (n < 0) return undefined;
  return buffer.slice(0, Math.min(n, buffer.length));
}

/** A string the shim copies out (invalid UTF-8 replaced), or undefined for an error. */
function textOut(call: (out: Uint8Array) => SbqResult): string | undefined {
  const value = bytesOut(call);
  return value === undefined ? undefined : decoder.decode(value);
}

function lastError(addon: SbqAddon): string {
  return textOut((b) => addon.lastError(b)) ?? '';
}

/** The error of a failed synchronous call. */
function failure(addon: SbqAddon, r: SbqResult, what: string): QuicError {
  const code = Number(r);
  return new QuicError(
    code === SBQ_ERR_IO ? 'unreachable' : code === SBQ_ERR_CONNECTION ? 'sessionEnded' : 'internal',
    `${what}: ${lastError(addon)} (${code})`,
  );
}

/** At most [max] bytes of [bytes], cut at a UTF-8 character boundary. */
function truncateUtf8(bytes: Uint8Array, max: number): Uint8Array {
  if (bytes.length <= max) return bytes;
  let n = max;
  while (n > 0 && ((bytes[n] ?? 0) & 0xc0) === 0x80) n--;
  return bytes.subarray(0, n);
}

/** ALPN tokens in the wire form of a list: each preceded by its length byte. */
function alpnWire(tokens: readonly string[]): Uint8Array {
  const parts = tokens.map((t) => {
    const bytes = utf8(t);
    if (bytes.length === 0 || bytes.length > 255) {
      throw new RangeError(`ALPN token ${JSON.stringify(t)}: an ALPN token is 1 to 255 bytes`);
    }
    return bytes;
  });
  const out = new Uint8Array(parts.reduce((n, p) => n + 1 + p.length, 0));
  let o = 0;
  for (const p of parts) {
    out[o++] = p.length;
    out.set(p, o);
    o += p.length;
  }
  return out;
}

/** A host as an endpoint names it: an IPv6 address in brackets. */
function uriHost(host: string): string {
  return host.includes(':') && !host.startsWith('[') ? `[${host}]` : host;
}

function bareHost(host: string): string {
  return host.startsWith('[') && host.endsWith(']') ? host.slice(1, -1) : host;
}

// --- Certificates ------------------------------------------------------------------------------

/** A certificate and its private key, both DER, with the certificate's SHA-256 (what a client pins). */
export interface NativeCertificate {
  /** The certificate, DER. */
  readonly certificateDer: Uint8Array;
  /** The private key, DER (PKCS#8). */
  readonly privateKeyDer: Uint8Array;
  /** SHA-256 of {@link certificateDer}. */
  readonly sha256: Uint8Array;
}

/**
 * SHA-256 of [der] (a certificate), the form {@link QuicClientConfig.serverCertificateHashes} and
 * {@link QuicSession.peerCertificateHash} use. Needs no addon.
 */
export function certificateHash(der: Uint8Array): Uint8Array {
  return new Uint8Array(createHash('sha256').update(der).digest());
}

/**
 * Generates a self-signed ECDSA P-256 certificate (X.509v3) for [subject] (its common name and
 * subject alternative name; an IP address when it parses as one), valid from five minutes ago for
 * [validityDays] days. With the default 14 days it also suits the W3C WebTransport API's
 * `serverCertificateHashes`, which accepts no longer validity: a browser pins
 * {@link NativeCertificate.sha256}.
 *
 * @throws {QuicError} 'unsupportedScheme' without the addon.
 */
export function selfSignedCertificate(subject: string, validityDays = 14): NativeCertificate {
  if (!Number.isInteger(validityDays) || validityDays < 0 || validityDays > 0xffffffff) {
    throw new RangeError(`validityDays ${validityDays}: must be an integer from 0 to 2^32 - 1`);
  }
  const addon = requireAddon();
  const cert = new Uint8Array(4096);
  const key = new Uint8Array(4096);
  const lengths = new Float64Array(2);
  const r = addon.generateSelfSigned(utf8(subject), validityDays, cert, key, lengths);
  if (r !== SBQ_OK) throw failure(addon, r, 'selfSignedCertificate');
  const certificateDer = cert.slice(0, lengths[0]);
  return Object.freeze({
    certificateDer,
    privateKeyDer: key.slice(0, lengths[1]),
    sha256: certificateHash(certificateDer),
  });
}

// --- The provider ------------------------------------------------------------------------------

/** Statistics of a native session. */
export interface NativeQuicStats {
  /** The smoothed round trip time, in milliseconds. */
  readonly rtt: number;
  readonly congestionWindow: number;
  readonly congestionEvents: number;
  readonly lostPackets: number;
  readonly sentPackets: number;
  readonly udpTxBytes: number;
  readonly udpRxBytes: number;
}

/** One raw QUIC connection or WebTransport session of the native provider. */
export interface NativeQuicSession extends QuicSession {
  /** Whether this is a WebTransport session rather than a raw QUIC connection. */
  readonly isWebTransport: boolean;
  /** The `:path` of an accepted WebTransport session's request (undefined on clients and raw QUIC). */
  readonly requestPath: string | undefined;
  /** The `Origin` of an accepted WebTransport session's request (undefined when it had none). */
  readonly requestOrigin: string | undefined;
  /** The `:authority` of an accepted WebTransport session's request. */
  readonly requestAuthority: string | undefined;
  /** The protocols an accepted WebTransport session's client offered. */
  readonly requestProtocols: readonly string[] | undefined;
  /**
   * The WebTransport draft an accepted session's client signalled: 2 (the
   * `sec-webtransport-http3-draft02` header or draft-02 settings alone, what Chrome sends) or 7
   * (draft-07 settings). Undefined on clients and raw QUIC.
   */
  readonly requestDraft: number | undefined;
  /** Statistics of the connection; undefined once it has ended. */
  readonly stats: NativeQuicStats | undefined;
  /**
   * Changes how many streams the peer may have open at once (`MAX_STREAMS`). Lowering it takes
   * effect as the peer's streams end.
   */
  setMaxIncomingStreams(count: number): void;
  /**
   * Asks the peer to drain a WebTransport session (`WT_DRAIN_SESSION`, at most once; its
   * `draining` resolves). Nothing on raw QUIC, which has no such signal, nor once the session has
   * ended.
   */
  drain(): void;
}

/** Accepts raw QUIC sessions on one UDP port ({@link NativeQuicProvider.listen}). */
export interface NativeQuicListener extends QuicListener {
  /** Accepted sessions, after their handshake completed. */
  readonly sessions: AsyncIterable<NativeQuicSession>;
  /** The UDP port the listener is bound to. */
  readonly port: number;
}

/** Accepts WebTransport sessions on one UDP port and one path ({@link NativeQuicProvider.listenWebTransport}). */
export interface NativeWebTransportListener extends NativeQuicListener {
  /** The path sessions are accepted on. */
  readonly path: string;
  /** The origins a browser's session may come from; undefined for any. */
  readonly allowedOrigins: ReadonlySet<string> | undefined;
  /** The protocols this listener serves, the first it shares with a client selected (in the client's order). */
  readonly protocols: readonly string[];
  /**
   * Requests answered here by status (200 for an accepted one), for logs and tests. Excludes
   * refusals in the shim, including requests arriving after this listener closed and requests
   * that exceeded native limits.
   */
  readonly answered: ReadonlyMap<number, number>;
}

/** Options of {@link NativeQuicProvider.connectWebTransport}. */
export interface NativeWebTransportConnectOptions {
  /** Sent as the `Origin` header (a browser always sends one; a node need not). */
  readonly origin?: string | undefined;
}

/** Options of {@link NativeQuicProvider.listen}. */
export interface NativeListenOptions {
  /** The host of the listener's endpoint (default: the host listened on); it names the endpoint only. */
  readonly advertisedHost?: string | undefined;
}

/** Options of {@link NativeQuicProvider.listenWebTransport}. */
export interface NativeWebTransportListenOptions extends NativeListenOptions {
  /** The path sessions are accepted on: absolute, without query or fragment. */
  readonly path: string;
  /** The origins a browser's session may come from; undefined (the default) for any. */
  readonly allowedOrigins?: Iterable<string> | undefined;
  /** The protocols served, in the listener's preference; default `['switchboard']`. */
  readonly protocols?: readonly string[] | undefined;
}

/**
 * Raw QUIC (`quic://host:port`, the ALPN tokens of {@link QuicClientConfig.alpn}) and WebTransport
 * (`https://host:port/path`, over HTTP/3) over quinn. Every instance shares the shim's runtime and
 * the context of this JavaScript environment; {@link nativeQuic} returns one.
 */
export class NativeQuicProvider implements QuicConnector {
  /** The number of tokio worker threads the shim starts with. Read once per process, by the first start. */
  static workerThreads = 2;

  /**
   * Ends every session and listener of this environment at once (sessions close with code 0) and
   * releases the shim's context. The provider starts again on the next connect or listen. A
   * WebTransport session's close is cut short with its QUIC connection (its peer reads the
   * session as lost): to end one gracefully, await its {@link QuicSession.close} first.
   */
  static shutdown(): void {
    Sbq.peek()?.shutdown();
  }

  /**
   * Connects to a `quic://host:port` endpoint (raw QUIC) or an `https://host:port/path` endpoint
   * (a WebTransport session, see {@link connectWebTransport}). Rejects with a {@link QuicError}
   * ('unsupportedScheme' for another scheme or without the addon, 'unreachable',
   * 'handshakeFailed', 'certificateRejected', 'refused'), a `RangeError` for a configuration out
   * of range (a negative duration), or a `TypeError` for a malformed endpoint.
   */
  connect(endpoint: string, config: QuicClientConfig): Promise<NativeQuicSession> {
    return handled(this.#connect(endpoint, config));
  }

  async #connect(endpoint: string, config: QuicClientConfig): Promise<NativeQuicSession> {
    const url = parseEndpoint(endpoint);
    if (url.protocol === 'https:') return this.connectWebTransport(endpoint, config);
    if (url.protocol !== 'quic:') {
      throw new QuicError(
        'unsupportedScheme',
        `the native provider dials quic:// and https:// only, not ${url.protocol}//`,
      );
    }
    const host = bareHost(url.hostname);
    if (host === '' || url.port === '') {
      throw new QuicError('unreachable', `a quic:// endpoint needs a host and a port: ${endpoint}`);
    }
    const params = clientParams(config);
    const rt = Sbq.current();
    const cfg = configHandle(rt, rt.addon.clientConfig(...params), 'client configuration');
    const name = utf8(host);
    let h: SbqResult;
    try {
      h = rt.addon.connect(rt.ctx, cfg, name, Number(url.port), name);
    } finally {
      rt.addon.configFree(cfg);
    }
    if (typeof h === 'bigint' || h <= 0) {
      const error = failure(rt.addon, h, 'connect');
      rt.releaseIfIdle();
      throw error;
    }
    const session = new Session(rt, h, {
      isInitiator: true,
      maxIncomingStreams: config.maxStreamsBidi ?? defaultQuicLimits.maxStreamsBidi,
    });
    return session.establishment();
  }

  /**
   * Opens a WebTransport session to `https://host:port/path` (port 443 by default; the path with
   * its query, never the fragment): a QUIC connection with ALPN `h3` (the provider's own:
   * {@link QuicClientConfig.alpn} lists the WebTransport protocols to offer instead, `switchboard`
   * for Switchboard), HTTP/3 over it, and the extended CONNECT. The session is established by the
   * server's 2xx response; {@link QuicSession.protocol} is the protocol it selected (undefined when
   * none). Another response rejects with 'refused' and the status in the message. Certificate
   * verification is as for raw QUIC. The handshake deadline bounds the QUIC handshake and again
   * the HTTP/3 exchange (10 seconds for HTTP/3 when the configured deadline is zero). The session
   * has its QUIC connection to itself. The endpoint, origin and protocols are printable ASCII (a
   * `TypeError` otherwise).
   */
  connectWebTransport(
    endpoint: string,
    config: QuicClientConfig,
    options: NativeWebTransportConnectOptions = {},
  ): Promise<NativeQuicSession> {
    return handled(this.#connectWebTransport(endpoint, config, options));
  }

  async #connectWebTransport(
    endpoint: string,
    config: QuicClientConfig,
    options: NativeWebTransportConnectOptions,
  ): Promise<NativeQuicSession> {
    const url = parseEndpoint(endpoint);
    if (url.protocol !== 'https:') {
      throw new QuicError(
        'unsupportedScheme',
        `a WebTransport endpoint is https://, not ${url.protocol}//`,
      );
    }
    const host = bareHost(url.hostname);
    if (host === '') {
      throw new QuicError('unreachable', `an https:// endpoint needs a host: ${endpoint}`);
    }
    if (url.username !== '' || url.password !== '') {
      throw new TypeError(`${endpoint}: userinfo is not allowed`);
    }
    const port = url.port === '' ? 443 : Number(url.port);
    if (port <= 0 || port > 65535) throw new RangeError(`port ${port}: must be 1 to 65535`);
    const hostPart = uriHost(host);
    const authority = utf8(port === 443 ? hostPart : `${hostPart}:${port}`);
    const path = utf8(`${url.pathname === '' ? '/' : url.pathname}${url.search}`);
    const protocols = alpnWire(config.alpn);
    const origin = utf8(options.origin ?? '');
    const params = clientParams(config, [h3Alpn]);
    const rt = Sbq.current();
    const cfg = configHandle(rt, rt.addon.clientConfig(...params), 'client configuration');
    const name = utf8(host);
    let h: SbqResult;
    try {
      h = rt.addon.connectWt(rt.ctx, cfg, name, port, name, authority, path, origin, protocols);
    } finally {
      rt.addon.configFree(cfg);
    }
    if (typeof h === 'bigint' || h <= 0) {
      const error =
        h === SBQ_ERR_ARG
          ? new TypeError(
              `a WebTransport endpoint, origin and protocols are printable ASCII: ${lastError(rt.addon)}`,
            )
          : failure(rt.addon, h, 'connect');
      rt.releaseIfIdle();
      throw error;
    }
    const session = new Session(rt, h, {
      isInitiator: true,
      maxIncomingStreams: config.maxStreamsBidi ?? defaultQuicLimits.maxStreamsBidi,
      isWebTransport: true,
    });
    return session.establishment();
  }

  /**
   * Listens on [host] (an address, `0.0.0.0` or `::` for every interface; a name is resolved
   * first) and [port] (0 for any free port) for raw QUIC connections. Its endpoint is
   * `quic://advertisedHost:port`. Rejects with a `RangeError` for a configuration out of range, as
   * {@link connect} does, and with a {@link QuicError} 'unsupportedScheme' without the addon.
   */
  listen(
    host: string,
    port: number,
    config: QuicServerConfig,
    options: NativeListenOptions = {},
  ): Promise<NativeQuicListener> {
    return handled(this.#listen(host, port, config, options));
  }

  async #listen(
    host: string,
    port: number,
    config: QuicServerConfig,
    options: NativeListenOptions,
  ): Promise<NativeQuicListener> {
    checkPort(port);
    const params = serverParams(config);
    const name = utf8(await listenAddress(host));
    const rt = Sbq.current();
    const h = listenOn(rt, params, name, port);
    const bound = rt.addon.listenerPort(h);
    const endpoint = `quic://${uriHost(options.advertisedHost ?? host)}:${bound}`;
    const maxIncoming = config.maxStreamsBidi ?? defaultQuicLimits.maxStreamsBidi;
    return new RawListener(rt, h, endpoint, bound, maxIncoming);
  }

  /**
   * Listens on [host] and [port] (as {@link listen}) for WebTransport sessions to
   * `https://host:port/path`: QUIC connections with ALPN `h3` (the configuration's own
   * {@link QuicServerConfig.alpn} is not used), HTTP/3 over them, and each extended CONNECT a
   * session request, answered here:
   *
   * - a request for another path than `options.path` (its query aside) is refused with 404. The
   *   request's `:authority` is not checked (it is the session's
   *   {@link NativeQuicSession.requestAuthority}): a node is reached under other names and ports
   *   than the ones it advertises, and what protects a session is its certificate, its `Origin`
   *   and identification, not the name a client used;
   * - one whose `Origin` is not among `allowedOrigins` with 403 (undefined, the default, allows
   *   any; a request without `Origin` comes from a client that is not a browser, and passes);
   * - one that offers protocols selects the first of them in its order that is among
   *   `protocols`; one that offers protocols of which none is served is refused with 400; one
   *   that offers none is accepted without a protocol ({@link QuicSession.protocol} undefined);
   * - after {@link QuicListener.close}, 503 (on the connections still open; new connections are
   *   not accepted).
   *
   * A connection may carry up to 16 sessions; the stream limit
   * ({@link QuicServerConfig.maxStreamsBidi}) is the connection's, plus one for a CONNECT stream.
   * {@link QuicServerConfig.handshakeTimeout} also bounds the HTTP/3 exchange of a request, its
   * wait for an answer (then 503), and the life of a connection without a session; when it is
   * zero, these bounds are 10 seconds. The listener's endpoint is
   * `https://advertisedHost:port/path`.
   */
  listenWebTransport(
    host: string,
    port: number,
    config: QuicServerConfig,
    options: NativeWebTransportListenOptions,
  ): Promise<NativeWebTransportListener> {
    return handled(this.#listenWebTransport(host, port, config, options));
  }

  async #listenWebTransport(
    host: string,
    port: number,
    config: QuicServerConfig,
    options: NativeWebTransportListenOptions,
  ): Promise<NativeWebTransportListener> {
    const { path } = options;
    if (!path.startsWith('/') || path.includes('?') || path.includes('#')) {
      throw new TypeError(`path ${path}: must be an absolute path without query or fragment`);
    }
    const protocols = [...(options.protocols ?? ['switchboard'])];
    // Validate before allocating the listener; every served protocol must cross the ABI.
    alpnWire(protocols);
    if (protocols.some((p) => /[^\x20-\x7e]/.test(p))) {
      throw new TypeError(`protocols ${JSON.stringify(protocols)}: must be printable ASCII`);
    }
    checkPort(port);
    const params = serverParams(config, [h3Alpn]);
    const name = utf8(await listenAddress(host));
    const rt = Sbq.current();
    const h = listenOn(rt, params, name, port);
    const bound = rt.addon.listenerPort(h);
    const endpoint = `https://${uriHost(options.advertisedHost ?? host)}:${bound}${path}`;
    const maxIncoming = config.maxStreamsBidi ?? defaultQuicLimits.maxStreamsBidi;
    return new WebTransportListener(rt, h, endpoint, bound, maxIncoming, {
      path,
      allowedOrigins:
        options.allowedOrigins === undefined ? undefined : new Set(options.allowedOrigins),
      protocols: Object.freeze(protocols),
    });
  }
}

let provider: NativeQuicProvider | undefined;

/**
 * The native QUIC provider: raw QUIC and WebTransport, client and listener, over the N-API addon.
 * Loading the addon waits for the first connect, listen or certificate.
 */
export function nativeQuic(): NativeQuicProvider {
  provider ??= new NativeQuicProvider();
  return provider;
}

function parseEndpoint(endpoint: string): URL {
  try {
    return new URL(endpoint);
  } catch (error) {
    throw new TypeError(`${endpoint}: not a valid endpoint`, { cause: error });
  }
}

function checkPort(port: number): void {
  if (!Number.isInteger(port) || port < 0 || port > 65535) {
    throw new RangeError(`port ${port}: must be 0 to 65535`);
  }
}

async function listenAddress(host: string): Promise<string> {
  const bare = bareHost(host);
  if (isIP(bare) !== 0) return bare;
  try {
    return (await lookup(bare)).address;
  } catch (error) {
    throw new QuicError('unreachable', `cannot resolve listen host: ${String(error)}`, {
      cause: error,
    });
  }
}

function listenOn(rt: Sbq, params: ServerParams, name: Uint8Array, port: number): number {
  const cfg = configHandle(rt, rt.addon.serverConfig(...params), 'server configuration');
  let h: SbqResult;
  try {
    h = rt.addon.listen(rt.ctx, cfg, name, port);
  } finally {
    rt.addon.configFree(cfg);
  }
  if (typeof h === 'bigint' || h <= 0) {
    const error = failure(rt.addon, h, 'listen');
    rt.releaseIfIdle();
    throw error;
  }
  return h;
}

function configHandle(rt: Sbq, r: SbqResult, what: string): number {
  if (typeof r === 'bigint' || r <= 0) {
    const error = failure(rt.addon, r, what);
    rt.releaseIfIdle();
    throw error;
  }
  return r;
}

/**
 * A duration of a configuration in whole milliseconds for the shim, where 0 is none: rounded up,
 * so that a positive one stays positive. A negative one is a `RangeError`.
 */
function millis(value: number | undefined, fallback: number, name: string): number {
  const ms = value ?? fallback;
  if (!Number.isFinite(ms) || ms < 0) {
    throw new RangeError(`${name} ${ms}: must be a finite duration, not negative`);
  }
  return Math.ceil(ms);
}

function quantity(value: number | undefined, fallback: number, name: string): number {
  const n = value ?? fallback;
  if (!Number.isSafeInteger(n) || n < 0) {
    throw new RangeError(`${name} ${n}: must be a non-negative integer`);
  }
  return n;
}

type ClientParams = Parameters<SbqAddon['clientConfig']>;
type ServerParams = Parameters<SbqAddon['serverConfig']>;

function clientParams(c: QuicClientConfig, alpn?: readonly string[]): ClientParams {
  const d = defaultQuicLimits;
  const idle = millis(c.idleTimeout, d.idleTimeout, 'idleTimeout');
  const keepAlive = millis(c.keepAlive, d.keepAlive, 'keepAlive');
  const handshake = millis(c.handshakeTimeout, d.handshakeTimeout, 'handshakeTimeout');
  const hashes = c.serverCertificateHashes ?? [];
  const pins = new Uint8Array(hashes.length * 32);
  hashes.forEach((h, i) => {
    if (h.length !== 32) throw new RangeError('a certificate hash is 32 bytes (SHA-256)');
    pins.set(h, i * 32);
  });
  const verify =
    hashes.length > 0
      ? SBQ_VERIFY_PINNED
      : (c.verifyCertificate ?? true)
        ? SBQ_VERIFY_PLATFORM
        : SBQ_VERIFY_NONE;
  return [
    alpnWire(alpn ?? c.alpn),
    verify,
    pins,
    hashes.length,
    quantity(c.maxStreamsBidi, d.maxStreamsBidi, 'maxStreamsBidi'),
    quantity(c.streamReceiveWindow, d.streamReceiveWindow, 'streamReceiveWindow'),
    quantity(c.connectionReceiveWindow, d.connectionReceiveWindow, 'connectionReceiveWindow'),
    idle,
    keepAlive,
    handshake,
  ];
}

function serverParams(c: QuicServerConfig, alpn?: readonly string[]): ServerParams {
  const d = defaultQuicLimits;
  const idle = millis(c.idleTimeout, d.idleTimeout, 'idleTimeout');
  const keepAlive = millis(c.keepAlive, d.keepAlive, 'keepAlive');
  const handshake = millis(c.handshakeTimeout, d.handshakeTimeout, 'handshakeTimeout');
  return [
    alpnWire(alpn ?? c.alpn),
    c.certificateDer,
    c.privateKeyDer,
    quantity(c.maxStreamsBidi, d.maxStreamsBidi, 'maxStreamsBidi'),
    quantity(c.streamReceiveWindow, d.streamReceiveWindow, 'streamReceiveWindow'),
    quantity(c.connectionReceiveWindow, d.connectionReceiveWindow, 'connectionReceiveWindow'),
    idle,
    keepAlive,
    handshake,
  ];
}

// --- Listeners ---------------------------------------------------------------------------------

/** What the shim tells a listener. */
interface ListenerEvents {
  /** A raw QUIC connection completed its handshake. */
  accepted(conn: number): void;
  /** A WebTransport request arrived (the session handle, pending, and the client's draft). */
  request(session: number, draft: number): void;
  closeListener(): void;
}

abstract class ListenerBase implements ListenerEvents {
  readonly rt: Sbq;
  readonly handle: number;
  readonly endpoint: string;
  readonly port: number;
  readonly maxIncoming: number;
  readonly #sessions = new AsyncQueue<NativeQuicSession>();
  closed = false;

  constructor(rt: Sbq, handle: number, endpoint: string, port: number, maxIncoming: number) {
    this.rt = rt;
    this.handle = handle;
    this.endpoint = endpoint;
    this.port = port;
    this.maxIncoming = maxIncoming;
    rt.listeners.set(handle, this);
    rt.retain();
  }

  abstract accepted(conn: number): void;

  abstract request(session: number, draft: number): void;

  get sessions(): AsyncIterable<NativeQuicSession> {
    return this.#sessions;
  }

  deliver(session: NativeQuicSession): void {
    this.#sessions.push(session);
  }

  close(): Promise<void> {
    this.closeListener();
    return Promise.resolve();
  }

  closeListener(): void {
    if (this.closed) return;
    this.closed = true;
    this.rt.addon.listenerClose(this.handle);
    this.rt.listeners.delete(this.handle);
    this.#sessions.end();
    this.rt.release();
  }
}

/** Accepts raw QUIC sessions on one UDP port. */
class RawListener extends ListenerBase implements NativeQuicListener {
  accepted(conn: number): void {
    this.deliver(
      new Session(this.rt, conn, { isInitiator: false, maxIncomingStreams: this.maxIncoming }),
    );
  }

  /** A raw listener serves no WebTransport (its configuration offered `h3` anyway): 404. */
  request(session: number): void {
    this.rt.addon.wtReject(session, 404);
    this.rt.addon.connRelease(session);
  }
}

/** The request of a WebTransport session a listener accepted. */
interface WebTransportRequest {
  readonly authority: string;
  readonly path: string;
  readonly origin: string | undefined;
  readonly protocols: readonly string[];
  readonly draft: number;
}

function readRequest(addon: SbqAddon, session: number, draft: number): WebTransportRequest {
  const field = (f: number) => textOut((b) => addon.wtRequest(session, f, b));
  const wire = bytesOut((b) => addon.wtRequest(session, SBQ_WT_PROTOCOLS, b)) ?? new Uint8Array(0);
  const protocols: string[] = [];
  for (let i = 0; i < wire.length; ) {
    const n = wire[i] ?? 0;
    protocols.push(decoder.decode(wire.subarray(i + 1, i + 1 + n)));
    i += 1 + n;
  }
  return Object.freeze({
    authority: field(SBQ_WT_AUTHORITY) ?? '',
    path: field(SBQ_WT_PATH) ?? '',
    origin: field(SBQ_WT_ORIGIN),
    protocols: Object.freeze(protocols),
    draft,
  });
}

/** Accepts WebTransport sessions on one UDP port and one path. */
class WebTransportListener extends ListenerBase implements NativeWebTransportListener {
  readonly path: string;
  readonly allowedOrigins: ReadonlySet<string> | undefined;
  readonly protocols: readonly string[];
  readonly #answered = new Map<number, number>();

  constructor(
    rt: Sbq,
    handle: number,
    endpoint: string,
    port: number,
    maxIncoming: number,
    options: {
      path: string;
      allowedOrigins: ReadonlySet<string> | undefined;
      protocols: readonly string[];
    },
  ) {
    super(rt, handle, endpoint, port, maxIncoming);
    this.path = options.path;
    this.allowedOrigins = options.allowedOrigins;
    this.protocols = options.protocols;
  }

  get answered(): ReadonlyMap<number, number> {
    return this.#answered;
  }

  #count(status: number): void {
    this.#answered.set(status, (this.#answered.get(status) ?? 0) + 1);
  }

  accepted(conn: number): void {
    // Not WebTransport: nothing this listener serves.
    this.rt.addon.connClose(conn, 0, new Uint8Array(0));
    this.rt.addon.connRelease(conn);
  }

  request(session: number, draft: number): void {
    const { addon } = this.rt;
    const request = readRequest(addon, session, draft);
    const offered = request.protocols;
    let selected: string | undefined;
    let status = 0;
    if (this.closed) {
      status = 503;
    } else if (request.path.split('?')[0] !== this.path) {
      status = 404;
    } else if (
      request.origin !== undefined &&
      this.allowedOrigins !== undefined &&
      !this.allowedOrigins.has(request.origin)
    ) {
      status = 403;
    } else if (offered.length > 0) {
      selected = offered.find((p) => this.protocols.includes(p));
      if (selected === undefined) status = 400;
    }
    if (status !== 0) {
      if (addon.wtReject(session, status) === SBQ_OK) this.#count(status);
      addon.connRelease(session);
      return;
    }
    if (addon.wtAccept(session, utf8(selected ?? '')) !== SBQ_OK) {
      // Refused meanwhile for lack of an answer.
      addon.connRelease(session);
      return;
    }
    this.#count(200);
    this.deliver(
      new Session(this.rt, session, {
        isInitiator: false,
        maxIncomingStreams: this.maxIncoming,
        isWebTransport: true,
        request,
      }),
    );
  }
}

// --- Sessions ----------------------------------------------------------------------------------

const neverDraining: Promise<void> = new Promise<void>(() => {});

function endedError(end: QuicSessionEnd | undefined): QuicError {
  return new QuicError(
    'sessionEnded',
    end === undefined
      ? 'the session has ended'
      : end.lost
        ? `the session was lost: ${end.reason}`
        : `the session was closed with ${end.code}${end.byPeer ? ' by the peer' : ''}: ${end.reason}`,
  );
}

function connectError(kind: number, reason: string): QuicError {
  switch (kind) {
    case SBQ_CLOSE_CERT_REJECTED:
      return new QuicError('certificateRejected', `the server certificate was rejected: ${reason}`);
    case SBQ_CLOSE_UNREACHABLE:
    case SBQ_CLOSE_TIMED_OUT:
      return new QuicError('unreachable', `unreachable: ${reason}`);
    case SBQ_CLOSE_APP_LOCAL:
      return new QuicError('sessionEnded', 'the connection attempt was abandoned');
    case SBQ_CLOSE_REFUSED:
      return new QuicError('refused', `the server refused the session: ${reason}`);
    default:
      return new QuicError('handshakeFailed', `the handshake failed: ${reason}`);
  }
}

/** One raw QUIC connection or WebTransport session. */
class Session implements NativeQuicSession {
  readonly rt: Sbq;
  readonly handle: number;
  readonly isInitiator: boolean;
  readonly isWebTransport: boolean;
  connecting: Deferred<Session> | undefined;
  #protocol: string | undefined;
  #remote = '';
  #peerHash: Uint8Array | undefined;
  #maxIncoming: number;
  readonly #incoming = new AsyncQueue<QuicStream>();
  readonly #unidirectional = new AsyncQueue<QuicReceiveStream>();
  readonly #done = new Deferred<QuicSessionEnd>();
  readonly #opens: Deferred<QuicStream>[] = [];
  readonly #spare: Stream[] = [];
  readonly streams = new Set<Stream>();
  readonly receiveStreams = new Set<ReceiveStream>();
  readonly #draining = new Deferred<void>();
  readonly #request: WebTransportRequest | undefined;
  #end: QuicSessionEnd | undefined;
  #endError: QuicError | undefined;
  #closeSent = false;

  constructor(
    rt: Sbq,
    handle: number,
    options: {
      isInitiator: boolean;
      maxIncomingStreams: number;
      isWebTransport?: boolean;
      request?: WebTransportRequest;
    },
  ) {
    this.rt = rt;
    this.handle = handle;
    this.isInitiator = options.isInitiator;
    this.isWebTransport = options.isWebTransport ?? false;
    this.#maxIncoming = options.maxIncomingStreams;
    this.#request = options.request;
    rt.sessions.set(handle, this);
    rt.retain();
    if (this.isInitiator) {
      this.connecting = new Deferred<Session>();
    } else {
      this.#established();
    }
  }

  get requestPath(): string | undefined {
    return this.#request?.path;
  }

  get requestOrigin(): string | undefined {
    return this.#request?.origin;
  }

  get requestAuthority(): string | undefined {
    return this.#request?.authority;
  }

  get requestProtocols(): readonly string[] | undefined {
    return this.#request?.protocols;
  }

  get requestDraft(): number | undefined {
    return this.#request?.draft;
  }

  get protocol(): string | undefined {
    return this.#protocol;
  }

  get remote(): string {
    return this.#remote;
  }

  get peerCertificateHash(): Uint8Array | undefined {
    return this.#peerHash;
  }

  get maxIncomingStreams(): number {
    return this.#maxIncoming;
  }

  setMaxIncomingStreams(count: number): void {
    if (!Number.isSafeInteger(count) || count < 0) {
      throw new RangeError(`count ${count}: must be a non-negative integer`);
    }
    if (this.#end !== undefined) return;
    const r = this.rt.addon.connSetMaxBidi(this.handle, count);
    if (r !== SBQ_OK) throw failure(this.rt.addon, r, 'setMaxIncomingStreams');
    this.#maxIncoming = count;
  }

  get stats(): NativeQuicStats | undefined {
    if (this.#end !== undefined) return undefined;
    const s = new Float64Array(9);
    if (this.rt.addon.connStats(this.handle, s) !== SBQ_OK) return undefined;
    return Object.freeze({
      rtt: (s[0] ?? 0) / 1000,
      congestionWindow: s[1] ?? 0,
      congestionEvents: s[2] ?? 0,
      lostPackets: s[3] ?? 0,
      sentPackets: s[4] ?? 0,
      udpTxBytes: s[5] ?? 0,
      udpRxBytes: s[6] ?? 0,
    });
  }

  get incomingStreams(): AsyncIterable<QuicStream> {
    return this.#incoming;
  }

  /**
   * Raw QUIC: never delivers a stream (the shim grants the peer no unidirectional streams, so a
   * peer that opens one anyway commits a transport error, which ends the session). WebTransport:
   * the session's unidirectional streams (their stream header stripped), for the application to
   * stop. Ends when the session ends.
   */
  get incomingUnidirectionalStreams(): AsyncIterable<QuicReceiveStream> {
    return this.#unidirectional;
  }

  get draining(): Promise<void> {
    return this.isWebTransport ? this.#draining.promise : neverDraining;
  }

  drain(): void {
    if (!this.isWebTransport || this.#end !== undefined || this.connecting !== undefined) return;
    this.rt.addon.wtDrain(this.handle);
  }

  get done(): Promise<QuicSessionEnd> {
    return this.#done.promise;
  }

  openStream(): Promise<QuicStream> {
    const error = this.#endError;
    if (error !== undefined) return rejected(error);
    const spare = this.#spare.shift();
    if (spare !== undefined) return Promise.resolve(spare);
    const r = this.rt.addon.streamOpen(this.handle);
    if (typeof r === 'number' && r > 0) {
      const stream = new Stream(this, r);
      const first = this.#opens.shift();
      if (first === undefined) return Promise.resolve(stream);
      // Earlier calls are waiting: the stream goes to the first of them, and this call waits in
      // turn (for the OPENED of the first call's request).
      first.resolve(stream);
    } else if (r !== SBQ_WOULD_BLOCK) {
      return rejected(
        r === SBQ_ERR_CONNECTION ? this.sessionEnded() : failure(this.rt.addon, r, 'openStream'),
      );
    }
    const d = new Deferred<QuicStream>();
    this.#opens.push(d);
    return d.promise;
  }

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
    const out = new Uint8Array(length);
    const r = this.rt.addon.connExporter(this.handle, utf8(label), context, out);
    if (r === SBQ_OK) return out;
    if (r === SBQ_ERR_STATE) return undefined;
    if (r === SBQ_ERR_ARG) throw new RangeError(`length ${length}: ${lastError(this.rt.addon)}`);
    throw failure(this.rt.addon, r, 'exportKeyingMaterial');
  }

  /**
   * Closes the session with [code] and [reason] (at most 1024 bytes of UTF-8). Raw QUIC:
   * `CONNECTION_CLOSE` with the code (0 to 2^53 - 1 here). WebTransport: `WT_CLOSE_SESSION` with
   * the code (0 to 2^32 - 1) and the reason, then FIN on the CONNECT stream. Resolves once the
   * peer's stack acknowledged the close, or after three round trips (within 0.2 to 2 s) without
   * that, so that shutting the provider down afterwards does not cut the close short.
   */
  close(code: number, reason: string): Promise<void> {
    checkCode(code, this.isWebTransport);
    if (this.#end === undefined && !this.#closeSent) {
      this.#closeSent = true;
      this.rt.addon.connClose(this.handle, code, truncateUtf8(utf8(reason), 1024));
    }
    return this.#done.promise.then(() => {});
  }

  sessionEnded(): QuicError {
    return this.#endError ?? endedError(undefined);
  }

  #established(): void {
    const { addon } = this.rt;
    this.#protocol = textOut((b) => addon.connProtocol(this.handle, b));
    // A WebTransport session without a selected protocol.
    if (this.#protocol === '') this.#protocol = undefined;
    this.#remote = textOut((b) => addon.connRemote(this.handle, b)) ?? '';
    if (this.isInitiator) {
      const h = new Uint8Array(32);
      if (addon.connPeerCertSha256(this.handle, h) === SBQ_OK) this.#peerHash = h;
    }
  }

  /** The connect's promise: the session once established. */
  establishment(): Promise<Session> {
    return this.connecting?.promise ?? Promise.resolve(this);
  }

  connected(): void {
    const c = this.connecting;
    if (c === undefined || this.#end !== undefined) return;
    this.connecting = undefined;
    this.#established();
    c.resolve(this);
  }

  peerStream(h: number): void {
    this.#incoming.push(new Stream(this, h));
  }

  peerUniStream(h: number): void {
    const stream = new ReceiveStream(this, h);
    this.receiveStreams.add(stream);
    this.#unidirectional.push(stream);
  }

  drainSignal(): void {
    if (this.#end === undefined) this.#draining.resolve();
  }

  opened(h: number): void {
    const stream = new Stream(this, h);
    const first = this.#opens.shift();
    if (first !== undefined) {
      first.resolve(stream);
    } else {
      this.#spare.push(stream);
    }
  }

  /** The CLOSED event (or a local shutdown): ends everything and releases the handles. */
  ended(kind: number, code: number, reason?: string): void {
    if (this.#end !== undefined) return;
    const { addon } = this.rt;
    const why = reason ?? textOut((b) => addon.connCloseReason(this.handle, b)) ?? '';
    const end: QuicSessionEnd =
      kind === SBQ_CLOSE_APP_REMOTE || kind === SBQ_CLOSE_APP_LOCAL
        ? Object.freeze({ code, reason: why, byPeer: kind === SBQ_CLOSE_APP_REMOTE, lost: false })
        : Object.freeze({
            code: undefined,
            reason: why === '' ? 'connection lost' : why,
            byPeer: false,
            lost: true,
          });
    this.#end = end;
    const error = endedError(end);
    this.#endError = error;
    const connecting = this.connecting;
    if (connecting !== undefined) {
      this.connecting = undefined;
      connecting.reject(connectError(kind, why));
    }
    for (let d = this.#opens.shift(); d !== undefined; d = this.#opens.shift()) d.reject(error);
    for (const s of [...this.streams, ...this.#spare]) s.sessionEnded(error);
    for (const s of [...this.receiveStreams]) s.release();
    this.#spare.length = 0;
    this.#incoming.end();
    this.#unidirectional.end();
    addon.connRelease(this.handle);
    this.rt.sessions.delete(this.handle);
    this.#done.resolve(end);
    this.rt.release();
  }
}

/** The receive direction of a peer's unidirectional WebTransport stream. */
class ReceiveStream implements QuicReceiveStream {
  readonly session: Session;
  readonly handle: number;
  #released = false;

  constructor(session: Session, handle: number) {
    this.session = session;
    this.handle = handle;
  }

  /**
   * Stops the direction with the WebTransport application [code] (0 to 2^32 - 1) and forgets the
   * stream. Idempotent, also after the session ended.
   */
  stop(code: number): void {
    checkCode(code, true);
    if (this.#released) return;
    this.session.rt.addon.streamStop(this.handle, code);
    this.release();
  }

  release(): void {
    if (this.#released) return;
    this.#released = true;
    this.session.rt.addon.streamRelease(this.handle);
    this.session.receiveStreams.delete(this);
  }
}

// --- Streams -----------------------------------------------------------------------------------

/** One write, or the finish, queued on a stream. */
class Write {
  bytes: Uint8Array;
  readonly finish: boolean;
  offset = 0;
  readonly done = new Deferred<void>();

  constructor(bytes: Uint8Array, finish: boolean) {
    this.bytes = bytes;
    this.finish = finish;
  }
}

/** What a read from the shim gave: bytes, undefined at FIN, {@link blocked}, or the error. */
const blocked = Symbol('blocked');
type ReadOutcome = Uint8Array | undefined | typeof blocked | Error;

/**
 * quinn's send priority for a priority class of the contract. quinn's levels are strict, and the
 * contract wants class 2 (the control stream) strictly first but classes 1 (ordinary) and 0
 * (bulk) never starving each other: 1 and 0 share quinn's level 0 (a new stream's), where quinn
 * sends their streams round robin; a class `c` above 1 is level `c - 1`, so 2 is level 1; a
 * negative class is the level of the same value. Clamped to a 32-bit level.
 */
export function quinnPriority(priorityClass: number): number {
  const level = priorityClass > 0 ? priorityClass - 1 : priorityClass;
  return Math.min(Math.max(level, -0x80000000), 0x7fffffff);
}

/** One bidirectional stream of a {@link Session}. */
class Stream implements QuicStream {
  readonly session: Session;
  readonly handle: number;
  #priority = 1;

  // Receive direction.
  #read: Deferred<Uint8Array | undefined> | undefined;
  #readMax = 0;
  #fin = false;
  #recvFailure: Error | undefined;
  #recvEnded = false;
  readonly #reset = new Deferred<number | undefined>();
  // No reset can be observed any more (FIN, a local stop, the end).
  #resetSettled = false;
  #resetCheckScheduled = false;

  // Send direction.
  readonly #writes: Write[] = [];
  #awaitingWritable = false;
  #finishing: Promise<void> | undefined;
  #resetSent = false;
  #peerStop = false;
  #sendFailure: Error | undefined;
  #sendEnded = false;
  readonly #stopped = new Deferred<number | undefined>();
  #released = false;

  constructor(session: Session, handle: number) {
    this.session = session;
    this.handle = handle;
    session.rt.streams.set(handle, this);
    session.streams.add(this);
    this.#scheduleResetCheck();
  }

  get #addon(): SbqAddon {
    return this.session.rt.addon;
  }

  get stopped(): Promise<number | undefined> {
    return this.#stopped.promise;
  }

  /**
   * Resolves with the peer's reset code, whether a read is pending or not (a raw QUIC reset always
   * carries one; a WebTransport one carries none, undefined, when its code is outside the
   * WebTransport range); rejects with 'sessionEnded' when the session ends first; stays pending
   * after FIN or a local {@link stop}.
   */
  get resetReceived(): Promise<number | undefined> {
    return this.#reset.promise;
  }

  get priority(): number {
    return this.#priority;
  }

  set priority(value: number) {
    if (!Number.isInteger(value)) throw new RangeError(`priority ${value}: must be an integer`);
    this.#priority = value;
    if (!this.#released && !this.#sendEnded) {
      this.#addon.streamSetPriority(this.handle, quinnPriority(value));
    }
  }

  read(max: number): Promise<Uint8Array | undefined> {
    if (!Number.isSafeInteger(max) || max <= 0) {
      throw new RangeError(`max ${max}: must be a positive integer`);
    }
    if (this.#read !== undefined) throw new Error('a read is pending on this stream');
    const o = this.#pollRead(max);
    if (o === blocked) {
      this.#readMax = max;
      this.#read = new Deferred();
      return this.#read.promise;
    }
    if (o instanceof Uint8Array) this.#scheduleResetCheck();
    if (o === undefined || o instanceof Uint8Array) return Promise.resolve(o);
    return rejected(o);
  }

  /** One read from the shim. */
  #pollRead(max: number): ReadOutcome {
    const recvFailure = this.#recvFailure;
    if (recvFailure !== undefined) return recvFailure;
    if (this.#fin) return undefined;
    const scratch = this.session.rt.scratch;
    const n = this.#addon.streamRead(
      this.handle,
      max >= scratch.length ? scratch : scratch.subarray(0, max),
    );
    if (typeof n === 'number' && n > 0) return scratch.slice(0, n);
    if (n === SBQ_WOULD_BLOCK) return blocked;
    if (n === SBQ_FIN) {
      this.#fin = true;
      this.#resetSettled = true;
      this.#endRecv();
      return undefined;
    }
    if (isCode(n) || n === SBQ_NO_CODE) {
      const code = n === SBQ_NO_CODE ? undefined : codeOf(n);
      const reset = new QuicStreamReset(code);
      this.#recvFailure = reset;
      this.#resetSeen(code);
      this.#endRecv();
      return reset;
    }
    if (n === SBQ_ERR_CONNECTION) return this.session.sessionEnded();
    return failure(this.#addon, n, 'read');
  }

  readable(): void {
    const d = this.#read;
    if (d === undefined) {
      // Data, FIN or a reset arrived while nobody reads.
      this.#checkReset();
      return;
    }
    const o = this.#pollRead(this.#readMax);
    if (o === blocked) return;
    this.#read = undefined;
    if (o instanceof Uint8Array) this.#scheduleResetCheck();
    if (o === undefined || o instanceof Uint8Array) {
      d.resolve(o);
    } else {
      d.reject(o);
    }
  }

  /** Checks for a reset once the reader has had its turn: a reader that reads on at once observes the reset with its read instead. */
  #scheduleResetCheck(): void {
    if (this.#resetCheckScheduled || this.#resetSettled || this.#reset.isSettled) return;
    this.#resetCheckScheduled = true;
    queueMicrotask(() => {
      this.#resetCheckScheduled = false;
      this.#checkReset();
    });
  }

  /** While no read is pending: asks the shim whether the peer reset its direction, which arms READABLE for the next change when it did not. */
  #checkReset(): void {
    if (
      this.#read !== undefined ||
      this.#released ||
      this.#recvEnded ||
      this.#resetSettled ||
      this.#reset.isSettled
    ) {
      return;
    }
    const r = this.#addon.streamPollReset(this.handle);
    if (r === SBQ_WOULD_BLOCK) return;
    if (isCode(r) || r === SBQ_NO_CODE) {
      const code = r === SBQ_NO_CODE ? undefined : codeOf(r);
      this.#recvFailure = new QuicStreamReset(code);
      this.#resetSeen(code);
      this.#endRecv();
      return;
    }
    // FIN behind the unread data, stopped here, or the session is ending (its CLOSED event fails
    // what waits).
    this.#resetSettled = true;
  }

  #resetSeen(code: number | undefined): void {
    this.#resetSettled = true;
    this.#reset.resolve(code);
  }

  write(bytes: Uint8Array): Promise<void> {
    if (!(bytes instanceof Uint8Array)) throw new TypeError('write takes a Uint8Array');
    const sendFailure = this.#sendFailure;
    if (sendFailure !== undefined) return rejected(sendFailure);
    if (this.#finishing !== undefined || this.#resetSent) {
      return rejected(new Error('the stream was finished or reset'));
    }
    const w = new Write(bytes, false);
    this.#writes.push(w);
    this.#pump();
    // What the stack did not take yet is copied: the caller may reuse its buffer at once.
    if (!w.done.isSettled) {
      w.bytes = w.bytes.slice(w.offset);
      w.offset = 0;
    }
    return w.done.promise;
  }

  finish(): Promise<void> {
    const f = this.#finishing;
    if (f !== undefined) return f;
    const sendFailure = this.#sendFailure;
    if (sendFailure !== undefined) return rejected(sendFailure);
    if (this.#resetSent) return rejected(new Error('the stream was reset'));
    const w = new Write(new Uint8Array(0), true);
    this.#writes.push(w);
    this.#pump();
    this.#finishing = w.done.promise;
    return this.#finishing;
  }

  #pump(): void {
    if (this.#awaitingWritable) return;
    for (let w = this.#writes[0]; w !== undefined; w = this.#writes[0]) {
      if (w.finish) {
        const r = this.#addon.streamFinish(this.handle);
        if (r === SBQ_RETRY) {
          // The stream header of a WebTransport stream waits for credit.
          this.#awaitingWritable = true;
          return;
        }
        if (r !== SBQ_OK) {
          this.#sendError(r);
          return;
        }
        this.#writes.shift();
        w.done.resolve();
        continue;
      }
      const remaining = w.bytes.length - w.offset;
      if (remaining === 0) {
        this.#writes.shift();
        w.done.resolve();
        continue;
      }
      const n = this.#addon.streamWrite(
        this.handle,
        w.offset === 0 ? w.bytes : w.bytes.subarray(w.offset),
      );
      if (typeof n === 'number' && n > 0) {
        w.offset += n;
      } else if (n === SBQ_WOULD_BLOCK) {
        this.#awaitingWritable = true;
        return;
      } else {
        this.#sendError(n);
        return;
      }
    }
  }

  writable(): void {
    if (!this.#awaitingWritable) return;
    this.#awaitingWritable = false;
    this.#pump();
  }

  #sendError(r: SbqResult): void {
    if (isCode(r)) {
      this.peerStopped(codeOf(r));
    } else if (r === SBQ_NO_CODE) {
      this.peerStopped(undefined);
    } else {
      this.#failWrites(
        r === SBQ_ERR_CONNECTION ? this.session.sessionEnded() : failure(this.#addon, r, 'write'),
      );
    }
  }

  #failWrites(error: Error): void {
    this.#awaitingWritable = false;
    for (let w = this.#writes.shift(); w !== undefined; w = this.#writes.shift()) {
      w.done.reject(error);
    }
  }

  reset(code: number): void {
    checkCode(code, this.session.isWebTransport);
    if (this.#released || this.#resetSent || this.#sendFailure !== undefined) return;
    this.#resetSent = true;
    this.#addon.streamReset(this.handle, code);
    this.#failWrites(new Error('the stream was reset'));
    this.#stopped.resolve(undefined);
    this.#endSend();
  }

  stop(code: number): void {
    checkCode(code, this.session.isWebTransport);
    if (this.#released || this.#recvEnded) return;
    this.#resetSettled = true;
    this.#addon.streamStop(this.handle, code);
    const reset = new QuicStreamReset(code);
    this.#recvFailure = reset;
    const d = this.#read;
    if (d !== undefined) {
      this.#read = undefined;
      d.reject(reset);
    }
    this.#endRecv();
  }

  peerStopped(code: number | undefined): void {
    if (this.#peerStop || this.#sendEnded) return;
    this.#peerStop = true;
    const stopped = new QuicStreamStopped(code);
    this.#sendFailure = stopped;
    this.#failWrites(stopped);
    if (code === undefined) {
      // Not to be read as an acknowledgement.
      this.#stopped.reject(stopped);
    } else {
      this.#stopped.resolve(code);
    }
    this.#endSend();
  }

  acked(): void {
    this.#stopped.resolve(undefined);
    this.#endSend();
  }

  sessionEnded(error: QuicError): void {
    this.#sendFailure ??= error;
    this.#recvFailure ??= error;
    const d = this.#read;
    if (d !== undefined) {
      this.#read = undefined;
      d.reject(error);
    }
    this.#failWrites(error);
    this.#stopped.reject(error);
    this.#resetSettled = true;
    this.#reset.reject(error);
    this.#recvEnded = true;
    this.#sendEnded = true;
    this.#release();
  }

  #endRecv(): void {
    this.#recvEnded = true;
    if (this.#sendEnded) this.#release();
  }

  #endSend(): void {
    this.#sendEnded = true;
    if (this.#recvEnded) this.#release();
  }

  #release(): void {
    if (this.#released) return;
    this.#released = true;
    this.#addon.streamRelease(this.handle);
    this.session.rt.streams.delete(this.handle);
    this.session.streams.delete(this);
  }
}

// --- The context -------------------------------------------------------------------------------

/** Events taken per `poll`. */
const batch = 256;
/** The largest read from the shim at once. */
const readChunk = 256 * 1024;

/**
 * The shim's context of this JavaScript environment: the doorbell, the event buffer, and the
 * handles of the live objects. Started on first use, shut down once no listener or session is
 * left (so that the process can exit and no doorbell rings for it afterwards).
 */
class Sbq {
  static #current: Sbq | undefined;

  readonly addon: SbqAddon;
  readonly ctx: number;
  readonly scratch = new Uint8Array(readChunk);
  readonly sessions = new Map<number, Session>();
  readonly streams = new Map<number, Stream>();
  readonly listeners = new Map<number, ListenerEvents>();
  readonly #events = new Float64Array(batch * 4);
  #live = 0;
  #down = false;

  private constructor(addon: SbqAddon, ctx: number) {
    this.addon = addon;
    this.ctx = ctx;
  }

  /** The running context, started now if there is none. */
  static current(): Sbq {
    Sbq.#current ??= Sbq.#start();
    return Sbq.#current;
  }

  /** The running context, if there is one. */
  static peek(): Sbq | undefined {
    return Sbq.#current;
  }

  static #start(): Sbq {
    const addon = requireAddon();
    let rt: Sbq | undefined;
    const ctx = addon.init((id) => rt?.drain(id), NativeQuicProvider.workerThreads);
    if (ctx === 0) {
      throw new QuicError('internal', `the native runtime did not start: ${lastError(addon)}`);
    }
    rt = new Sbq(addon, ctx);
    return rt;
  }

  retain(): void {
    if (this.#live++ === 0) this.addon.setActive(this.ctx, true);
  }

  release(): void {
    if (--this.#live > 0 || this.#down) return;
    this.addon.setActive(this.ctx, false);
    // Shut the context down before anything that waits on the last session runs: a process with
    // nothing else to do may exit then.
    this.shutdown();
  }

  /** After a failed connect or listen: a context that holds nothing is shut down at once. */
  releaseIfIdle(): void {
    if (this.#live === 0) this.shutdown();
  }

  shutdown(): void {
    if (this.#down) return;
    this.#down = true;
    if (Sbq.#current === this) Sbq.#current = undefined;
    for (const s of [...this.sessions.values()]) s.ended(SBQ_CLOSE_APP_LOCAL, 0, 'shutdown');
    for (const l of [...this.listeners.values()]) l.closeListener();
    // The doorbell never waits for this thread, so a synchronous shutdown cannot deadlock.
    this.addon.shutdown(this.ctx);
  }

  drain(id: number): void {
    if (this.#down || id !== this.ctx) return;
    const events = this.#events;
    for (;;) {
      const n = this.addon.poll(this.ctx, events);
      for (let i = 0; i < n && !this.#down; i++) {
        const kind = events[i * 4] ?? 0;
        const handle = events[i * 4 + 1] ?? 0;
        const a = events[i * 4 + 2] ?? 0;
        const b = events[i * 4 + 3] ?? 0;
        try {
          this.#dispatch(kind, handle, a, b);
        } catch (error) {
          reportUncaught(error);
        }
      }
      if (n < batch || this.#down) return;
    }
  }

  #dispatch(kind: number, handle: number, a: number, b: number): void {
    const { addon } = this;
    switch (kind) {
      case SBQ_EV_READABLE:
        this.streams.get(handle)?.readable();
        break;
      case SBQ_EV_WRITABLE:
        this.streams.get(handle)?.writable();
        break;
      case SBQ_EV_STREAM: {
        const s = this.sessions.get(a);
        if (s === undefined) {
          addon.streamRelease(handle);
        } else {
          s.peerStream(handle);
        }
        break;
      }
      case SBQ_EV_OPENED: {
        const s = this.sessions.get(a);
        if (s === undefined) {
          addon.streamRelease(handle);
        } else {
          s.opened(handle);
        }
        break;
      }
      case SBQ_EV_STOPPED:
        this.streams.get(handle)?.peerStopped(a < 0 ? undefined : a);
        break;
      case SBQ_EV_FINISHED_ACKED:
        this.streams.get(handle)?.acked();
        break;
      case SBQ_EV_CONNECTED:
        this.sessions.get(handle)?.connected();
        break;
      case SBQ_EV_INCOMING: {
        const l = this.listeners.get(a);
        if (l === undefined) {
          addon.connRelease(handle);
        } else {
          l.accepted(handle);
        }
        break;
      }
      case SBQ_EV_CLOSED: {
        const s = this.sessions.get(handle);
        if (s === undefined) {
          addon.connRelease(handle);
        } else {
          s.ended(a, b);
        }
        break;
      }
      case SBQ_EV_WT_REQUEST: {
        const l = this.listeners.get(a);
        if (l === undefined) {
          addon.wtReject(handle, 503);
          addon.connRelease(handle);
        } else {
          l.request(handle, b);
        }
        break;
      }
      case SBQ_EV_DRAINING:
        this.sessions.get(handle)?.drainSignal();
        break;
      case SBQ_EV_UNI_STREAM: {
        const s = this.sessions.get(a);
        if (s === undefined) {
          addon.streamStop(handle, 0);
          addon.streamRelease(handle);
        } else {
          s.peerUniStream(handle);
        }
        break;
      }
    }
  }
}
