/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// The peer protocol's messages as TypeScript types (README, "Process
// protocol"). The Dart peer and the TS peer speak it; the launcher and the
// tests program against these types only.

/** Version of the peer protocol this harness speaks. */
export const peerProtocolVersion = 1;

/** The transports a peer can listen on and dial. */
export const transports = ['tcp', 'ws', 'quic', 'webtransport'] as const;

/** A transport. */
export type Transport = (typeof transports)[number];

/** The implementations behind a peer. */
export type Implementation = 'dart' | 'ts';

/** The line a peer prints once it listens: its endpoints and what it can do. */
export interface ReadyInfo {
  /** The peer protocol version, {@link peerProtocolVersion}. */
  readonly protocol: number;
  /** Which implementation answered. */
  readonly implementation: Implementation;
  /** `tcp://127.0.0.1:port`, when listening on TCP. */
  readonly tcp?: string;
  /** `ws://127.0.0.1:port/sb`, when listening on WebSocket. */
  readonly ws?: string;
  /** `quic://127.0.0.1:port`, when listening on raw QUIC. */
  readonly quic?: string;
  /** `https://127.0.0.1:port/sb`, when listening on WebTransport. */
  readonly webtransport?: string;
  /** Lower-case hex SHA-256 of the QUIC listeners' certificate. */
  readonly certificateSha256?: string;
  /** The process id. */
  readonly pid: number;
  /** The oracle kinds `decode` and `encode` accept. */
  readonly oracle: readonly string[];
}

/** The configuration a peer starts with (`--config`). Absent keys keep defaults. */
export interface PeerConfig {
  /** Transports to listen on; default all four. */
  listen?: readonly Transport[];
  /** WebSocket and WebTransport path; default `/sb`. */
  path?: string;
  /** Mux options of every connection. */
  mux?: MuxConfig;
  /** Talk options of the services and the client's Talk channels. */
  talk?: TalkConfig;
  /** Services to register; default `echo`, `raw` and `sink` at instance 1. */
  services?: readonly ServiceConfig[];
}

/** Mux options by the Dart names; durations in milliseconds. */
export interface MuxConfig {
  maxFrameSize?: number;
  maxChannels?: number;
  shortIdsOnly?: boolean;
  /** 0 disables keep-alive. */
  keepAliveIntervalMs?: number;
  keepAliveTimeoutMs?: number;
  goAwayGraceMs?: number;
  announceLimits?: boolean;
  closeConfirmTimeoutMs?: number;
  identityTimeoutMs?: number;
  requireNamedIdent?: boolean;
  initialWindow?: number;
  awaitPeerLimits?: boolean;
}

/** Talk options by the Dart names; durations in milliseconds. */
export interface TalkConfig {
  requestTimeoutMs?: number;
  replyTimeoutMs?: number;
  bulkThreshold?: number;
  maxInlinePayload?: number;
  reassemblyBudget?: number;
  bulkIdleTimeoutMs?: number;
  creditTimeoutMs?: number;
}

/** A service: `echo` serves the interop procedures, `raw` hands out channels, `sink` never reads. */
export interface ServiceConfig {
  type: string;
  behaviour?: 'echo' | 'raw' | 'sink';
  instance?: number;
}

/** An error as a peer reports it. */
export interface PeerErrorInfo {
  /** `status` (with code and reason), `format`, `argument` or `other`. */
  readonly kind: 'status' | 'format' | 'argument' | 'other';
  /** The implementation's error class name. */
  readonly type: string;
  readonly message: string;
  readonly code?: number;
  readonly reason?: string;
}

/** A failed command, carrying what the peer reported. */
export class PeerError extends Error {
  /** The peer's error object. */
  readonly info: PeerErrorInfo;

  constructor(op: string, info: PeerErrorInfo) {
    super(`${op}: ${info.type}: ${info.message}`);
    this.name = 'PeerError';
    this.info = info;
  }

  /** The status code, for a `status` error. */
  get code(): number | undefined {
    return this.info.code;
  }
}

/** One response line. */
export type Response =
  | { readonly id: number; readonly ok: true; readonly value: unknown }
  | { readonly id: number; readonly ok: false; readonly error: PeerErrorInfo };

/** One event line: `event` names it, the other fields depend on it. */
export interface PeerEvent {
  readonly event: string;
  readonly [field: string]: unknown;
}

/** A Talk message as the client commands report it. */
export interface MessageInfo {
  readonly procedure: string;
  readonly bulk: boolean;
  /** Hex, unless the command asked for a digest. */
  readonly payload?: string;
  /** With `digest`: the payload's length. */
  readonly size?: number;
  /** With `digest`: the payload's SHA-256, lower-case hex. */
  readonly sha256?: string;
}

/** An address for `open` and `talk.open`; `payload` is hex. */
export interface AddressArg {
  type?: string;
  typeHex?: string;
  instance?: number;
  shard?: number;
  host?: string;
  payload?: string;
}

/** One result of `decodeMany`. */
export type DecodeResult =
  | { readonly ok: true; readonly value: Record<string, unknown> }
  | { readonly ok: false; readonly error: PeerErrorInfo };
