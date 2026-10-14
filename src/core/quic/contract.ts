/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * The stream provider contract of the QUIC and WebTransport binding (wiki
 * page "Polyverse Switchboard QUIC and WebTransport Binding"), a port of
 * `switchboard_quic`'s `QuicConnector`, `QuicListener`, `QuicSession` and
 * `QuicStream`. The binding (records, channels, the control stream, HELLO,
 * identity rules) is written once against these interfaces; the providers
 * implement them: the native provider (`@kaetemi/switchboard/node`, an
 * N-API addon over quinn: `quic://` raw QUIC and `https://` WebTransport,
 * client and listener) and the web provider (`@kaetemi/switchboard/web`,
 * the browser's WebTransport API: `https://` only, no listener).
 *
 * Reads are pull-based so that unread data stays in the stack and holds
 * back the peer's credit: the binding reads the next record only once the
 * previous one has been taken by the layer above.
 *
 * When a session ends, everything pending on it and on its streams rejects
 * with a {@link QuicError} of kind 'sessionEnded'; {@link QuicSession.done}
 * tells how it ended.
 *
 * Durations are milliseconds; 0 means none.
 */

/** Opens sessions to endpoints. */
export interface QuicConnector {
  /**
   * Connects to [endpoint] (`quic://host:port` or `https://host:port/path`)
   * with [config]. Rejects with a {@link QuicError}: 'unreachable',
   * 'handshakeFailed', 'unsupportedScheme', 'refused', ... A handshake past
   * `handshakeTimeout` fails with 'unreachable'. A WebTransport session is
   * established by the server's 2xx; another response fails with 'refused'.
   *
   * The attempt cannot be cancelled: a caller that stops waiting closes the
   * session the promise still yields.
   */
  connect(endpoint: string, config: QuicClientConfig): Promise<QuicSession>;
}

/** Accepts sessions on one local endpoint (native provider only). */
export interface QuicListener {
  /** The endpoint peers dial, as the node advertises it. */
  readonly endpoint: string;
  /** Accepted sessions, after their handshake completed. */
  readonly sessions: AsyncIterable<QuicSession>;
  /** Stops accepting; open sessions are left to their owners. */
  close(): Promise<void>;
}

/** One QUIC connection or WebTransport session. */
export interface QuicSession {
  readonly isInitiator: boolean;
  /** The negotiated ALPN (`switchboard`) or the WebTransport protocol the server selected; undefined when unknown or none. */
  readonly protocol: string | undefined;
  /** The peer's address for logs. */
  readonly remote: string;
  /**
   * Opens a bidirectional stream. Waits for stream credit; rejects with a
   * QuicError once the session has ended. Nothing is visible to the peer
   * until the first write. A caller that stops waiting resets the stream
   * the call still yields.
   */
  openStream(): Promise<QuicStream>;
  /** Streams the peer opened, in the order the stack delivers them. Taken only while iterated. */
  readonly incomingStreams: AsyncIterable<QuicStream>;
  /** Peer-created unidirectional streams, for the binding to refuse by stopping them. */
  readonly incomingUnidirectionalStreams: AsyncIterable<QuicReceiveStream>;
  /** Resolves when the peer starts draining this session. Never rejects; stays pending on raw QUIC. */
  readonly draining: Promise<void>;
  /** How many streams opened by the peer this side accepts at once; undefined where the stack decides (browsers). */
  readonly maxIncomingStreams: number | undefined;
  /**
   * The keying material exporter; undefined when the stack has none (a
   * browser without `exportKeyingMaterial`) or cannot export [length]
   * bytes. Raw QUIC: the TLS exporter with [label] and [context] as given
   * (Switchboard: RFC 9266 `EXPORTER-Channel-Binding`, empty context, 32
   * bytes). WebTransport: the draft's per-session derivation applied to
   * the application's label (Switchboard: `switchboard-ident`).
   */
  exportKeyingMaterial(
    label: string,
    context: Uint8Array,
    length: number,
  ): Promise<Uint8Array | undefined>;
  /** The server certificate's SHA-256 over its DER, for pinning; undefined when not exposed. */
  readonly peerCertificateHash: Uint8Array | undefined;
  /**
   * Closes with an application [code] (0 to 255) and [reason] (at most
   * 1024 bytes of UTF-8). Idempotent; resolves once the session has ended.
   */
  close(code: number, reason: string): Promise<void>;
  /** Resolves when the session has ended, with how. Never rejects. */
  readonly done: Promise<QuicSessionEnd>;
}

/** The receive direction of a peer-created unidirectional stream. */
export interface QuicReceiveStream {
  /** Stops this direction with an error code, discarding unread bytes. Idempotent. */
  stop(code: number): void;
}

/** One bidirectional stream. */
export interface QuicStream {
  /**
   * Reads the next chunk, at least one and at most [max] bytes; undefined
   * at the peer's FIN. Rejects with {@link QuicStreamReset} when the peer
   * reset its direction. One read at a time.
   */
  read(max: number): Promise<Uint8Array | undefined>;
  /**
   * Resolves on the peer's RESET_STREAM, even while no read is pending,
   * with its code (undefined when none was delivered). FIN does not resolve
   * it. Rejects with 'sessionEnded' when the session ends first.
   */
  readonly resetReceived: Promise<number | undefined>;
  /**
   * Writes [bytes]; resolves once the stack accepted them (copied). Writes
   * queue in order and wait for credit. Rejects with {@link QuicStreamStopped}
   * once the peer sent STOP_SENDING, and with an Error after finish or reset.
   */
  write(bytes: Uint8Array): Promise<void>;
  /** Ends this side's direction (FIN) after everything written. Idempotent. */
  finish(): Promise<void>;
  /** Aborts this side's direction with [code] (RESET_STREAM), also after finish. No-op once ended. @throws {RangeError} for a code the stack cannot carry. */
  reset(code: number): void;
  /** Asks the peer to stop sending with [code] (STOP_SENDING); this side's reads end at once. */
  stop(code: number): void;
  /** Sender-local priority class: 2 control, 1 ordinary, 0 bulk. */
  priority: number;
  /**
   * Resolves when the peer's STOP_SENDING arrived (its code), or when every
   * byte was acknowledged after finish (undefined). A STOP_SENDING without
   * a code rejects with a QuicStreamStopped whose code is undefined.
   */
  readonly stopped: Promise<number | undefined>;
}

/** Client configuration. A provider whose stack decides some of these (a browser) ignores them. */
export interface QuicClientConfig {
  /** ALPN tokens to offer (`switchboard` for raw QUIC); for WebTransport, the protocols offered in `WT-Available-Protocols`. */
  readonly alpn: readonly string[];
  /** SHA-256 hashes of acceptable server certificates (DER). Empty with `verifyCertificate`: the platform roots. */
  readonly serverCertificateHashes?: readonly Uint8Array[] | undefined;
  /** Default true; false: any certificate, where the stack allows it (identity then comes from IDENT). */
  readonly verifyCertificate?: boolean | undefined;
  /** Streams the peer may have open at once; default 128. */
  readonly maxStreamsBidi?: number | undefined;
  /** Receive window of each stream; default 1 MiB. */
  readonly streamReceiveWindow?: number | undefined;
  /** Receive window of the connection; default 16 MiB. */
  readonly connectionReceiveWindow?: number | undefined;
  /** Default 30 000. */
  readonly idleTimeout?: number | undefined;
  /** The stack's own keep-alive interval, shorter than the idle timeout; default 10 000. */
  readonly keepAlive?: number | undefined;
  /** Bound on the handshake; default 10 000. */
  readonly handshakeTimeout?: number | undefined;
}

/** Server configuration (native provider). */
export interface QuicServerConfig {
  readonly alpn: readonly string[];
  /** The certificate, DER, optionally followed by the DER certificates of its chain. */
  readonly certificateDer: Uint8Array;
  /** The private key, DER (PKCS#8, SEC1 or PKCS#1). */
  readonly privateKeyDer: Uint8Array;
  readonly maxStreamsBidi?: number | undefined;
  readonly streamReceiveWindow?: number | undefined;
  readonly connectionReceiveWindow?: number | undefined;
  readonly idleTimeout?: number | undefined;
  readonly keepAlive?: number | undefined;
  readonly handshakeTimeout?: number | undefined;
}

export const defaultQuicLimits = Object.freeze({
  maxStreamsBidi: 128,
  streamReceiveWindow: 1 << 20,
  connectionReceiveWindow: 16 << 20,
  idleTimeout: 30_000,
  keepAlive: 10_000,
  handshakeTimeout: 10_000,
});

/** How a session ended. */
export interface QuicSessionEnd {
  /** The application code of the close; undefined when the transport was lost. */
  readonly code: number | undefined;
  readonly reason: string;
  readonly byPeer: boolean;
  readonly lost: boolean;
}

export type QuicErrorKind =
  | 'unsupportedScheme'
  | 'unreachable'
  | 'handshakeFailed'
  | 'certificateRejected'
  /** The server answered a WebTransport request with a status other than 2xx. */
  | 'refused'
  | 'sessionEnded'
  | 'streamLimit'
  | 'internal';

/** A provider failure. */
export class QuicError extends Error {
  readonly kind: QuicErrorKind;
  constructor(kind: QuicErrorKind, message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = 'QuicError';
    this.kind = kind;
  }
}

/** The peer reset its direction of a stream. */
export class QuicStreamReset extends Error {
  /** The application error code; undefined when the stack delivered none. */
  readonly code: number | undefined;
  constructor(code: number | undefined) {
    super(`stream reset (${code ?? 'no code'})`);
    this.name = 'QuicStreamReset';
    this.code = code;
  }
}

/** The peer asked this side to stop sending. */
export class QuicStreamStopped extends Error {
  readonly code: number | undefined;
  constructor(code: number | undefined) {
    super(`stream stopped (${code ?? 'no code'})`);
    this.name = 'QuicStreamStopped';
    this.code = code;
  }
}

/** The ALPN token and WebTransport protocol of the binding. */
export const switchboardAlpn = 'switchboard';

/** The label of the exporter over raw QUIC (RFC 9266). */
export const rawQuicExporterLabel = 'EXPORTER-Channel-Binding';

/** The application label of the exporter over WebTransport. */
export const webTransportExporterLabel = 'switchboard-ident';
