/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import type { CredentialVerifier, HolderKey, PeerIdentity } from '../identity/contract.ts';
import type { Status } from '../status.ts';
import { type Clock, systemClock } from '../util/clock.ts';
import { type Logger, noopLogger } from '../util/logger.ts';

/**
 * The connection and channel contract (wiki page "Polyverse Switchboard
 * QUIC and WebTransport Binding", section "Channel contract"): what the
 * layers above the mux program against, whatever carries the channels.
 * Two bindings implement it: the frame mux (`FrameMuxConnection`, over a
 * {@link FrameTransport}) and the QUIC binding (`QuicBindingConnection`,
 * over a `QuicSession`).
 */

/** The state of a channel from this side's point of view. */
export type MuxChannelState = 'open' | 'halfClosedLocal' | 'closed';

/** The output tier of a channel, sender-local. */
export type MuxPriority = 'normal' | 'bulk';

/** Options of a connection, both bindings. Durations in milliseconds. */
export interface MuxOptions {
  /** The largest frame this side accepts, announced with LIMITS; default 1 MiB. A smaller transport limit is announced instead. */
  readonly maxFrameSize: number;
  /** Open channels and, separately, the incoming backlog; default 65536; 0 = unlimited. */
  readonly maxChannels: number;
  /** Embedded style peer: refuse long ids; default false. */
  readonly shortIdsOnly: boolean;
  /** PING after this much silence; default 10 000; undefined disables. */
  readonly keepAliveInterval: number | undefined;
  /** Close if nothing arrives after PING; default 10 000; also bounds `close()`, then the transport is aborted. */
  readonly keepAliveTimeout: number;
  /** How long `goAway()` waits for channels; default 10 000. */
  readonly goAwayGrace: number;
  /** Send LIMITS on start; default true. */
  readonly announceLimits: boolean;
  /** Ids awaiting the peer's CLOSE without a channel; default 1024 (0 = unlimited); beyond: GOAWAY RESOURCE_EXHAUSTED. */
  readonly maxPendingRejections: number;
  /** What all channels hold unconsumed (at flow-control cost) plus held OPEN payloads; default 16 MiB (0 = never); above: stop reading until half. */
  readonly receiveHighWaterMarkBytes: number;
  /** `MuxChannel.close()` wait for the peer's CLOSE; default 30 000 (0 = forever). */
  readonly closeConfirmTimeout: number;
  /** OPEN payloads held for the peer's open or undelivered channels; default 16 MiB, at most half of `receiveHighWaterMarkBytes`; 0 = no budget. */
  readonly maxOpenPayloadBytes: number;
  /** Checks the peer's IDENT; undefined: IDENT ignored (NONCE still answered). */
  readonly identityVerifier: CredentialVerifier | undefined;
  /** `identify()`'s default bound; default 10 000; 0 = none. */
  readonly identityTimeout: number;
  /** Refuse an IDENT that names no receiver (GOAWAY UNAUTHENTICATED); default false. */
  readonly requireNamedIdent: boolean;
  /** The window this side grants every channel, announced in LIMITS; default 64 KiB; 1 to 2^32-1. */
  readonly initialWindow: number;
  /** Output scheduler: one bulk frame after this many ordinary frames while bulk frames wait; default 4; at least 1. */
  readonly bulkZipper: number;
  /** Largest chunk `send()` cuts a payload into on a bulk channel; default 64 KiB. */
  readonly bulkChunkSize: number;
  /** DATA on a channel opened before the peer's first frame waits for it (its LIMITS); default true. */
  readonly awaitPeerLimits: boolean;
  /** Time and timers; default the system clock. Tests inject a manual clock. */
  readonly clock: Clock;
  /** Diagnostics; default discards. */
  readonly logger: Logger;
}

/** The defaults of {@link MuxOptions}. */
export const defaultMuxOptions: MuxOptions = Object.freeze({
  maxFrameSize: 1024 * 1024,
  maxChannels: 65536,
  shortIdsOnly: false,
  keepAliveInterval: 10_000,
  keepAliveTimeout: 10_000,
  goAwayGrace: 10_000,
  announceLimits: true,
  maxPendingRejections: 1024,
  receiveHighWaterMarkBytes: 16 * 1024 * 1024,
  closeConfirmTimeout: 30_000,
  maxOpenPayloadBytes: 16 * 1024 * 1024,
  identityVerifier: undefined,
  identityTimeout: 10_000,
  requireNamedIdent: false,
  initialWindow: 64 * 1024,
  bulkZipper: 4,
  bulkChunkSize: 64 * 1024,
  awaitPeerLimits: true,
  clock: systemClock,
  logger: noopLogger,
});

/** [options] completed with the defaults. */
export function muxOptions(options?: Partial<MuxOptions>): MuxOptions {
  return options === undefined ? defaultMuxOptions : { ...defaultMuxOptions, ...options };
}

/** What `MuxConnection.identify` takes. */
export interface IdentifyOptions {
  /** The holder key of a holder-key credential; required for one, refused for a bearer credential. */
  holderKey?: HolderKey | undefined;
  /** Opaque intent, at most 64 bytes; empty unless answering a CONNECT. */
  intent?: Uint8Array | undefined;
  /** The identity the peer is expected to have; undefined names nobody. */
  receiver?: string | undefined;
  /** Bound on the exchange; default `MuxOptions.identityTimeout`. */
  timeout?: number | undefined;
  signal?: AbortSignal | undefined;
}

/**
 * A connection: channels opened by either side, the control messages, an
 * end status. The frame mux constructs one over a transport; a binding
 * yields one. Everything above programs against this interface only.
 */
export interface MuxConnection extends AsyncDisposable {
  readonly isInitiator: boolean;
  readonly options: MuxOptions;

  /** A description of the peer for logs. */
  readonly remote: string;

  /**
   * Channels opened by the peer, in order, buffered until taken. One
   * consumer. Stopping the iteration (`break`) closes the channels not
   * taken yet with UNAVAILABLE, and the ones arriving after.
   */
  readonly incoming: AsyncIterable<MuxChannel>;

  /**
   * Installs a filter that sees every peer-opened channel before
   * `incoming` does. A filter that returns true has adopted the channel:
   * it does not reach `incoming`. Filters run in installation order, on the
   * open payload only; the channel is already usable. The Talk layer
   * installs one that routes `_bulk` channels to their parent, so raw
   * consumers of `incoming` never see them. Returns the uninstaller.
   */
  adoptIncoming(filter: (channel: MuxChannel) => boolean): () => void;

  /**
   * Installs the rule for OPENs that arrive after this side sent GOAWAY:
   * one accepted by a predicate continues (the Talk layer admits `_bulk`
   * continuations of its channels); otherwise it is refused with
   * FAILED_PRECONDITION. Returns the uninstaller.
   */
  admitAfterGoAway(predicate: (openPayload: Uint8Array) => boolean): () => void;

  /**
   * Opens a channel with [openPayload]. Usable at once.
   *
   * @throws {SwitchboardError} RESOURCE_EXHAUSTED if no id is free or the
   * peer's channel limit is reached; FAILED_PRECONDITION once closed or
   * after the peer's GOAWAY.
   */
  open(openPayload: Uint8Array): MuxChannel;

  /** The channel with that id not mutually closed yet (for bulk routing). */
  channelWithId(id: number): MuxChannel | undefined;
  readonly channels: Iterable<MuxChannel>;
  readonly openChannelCount: number;

  /** How many channels the peer accepts at once; 0 for no stated limit (frame mux: LIMITS; QUIC: HELLO). */
  readonly peerMaxChannels: number;

  /** Frame mux: true (one ordered stream); independent streams: false. */
  readonly hasOrderedChannels: boolean;

  /** Round trip of a PING in milliseconds. At most 1024 await their PONG; one more fails the oldest with DEADLINE_EXCEEDED. */
  ping(payload?: Uint8Array, options?: { signal?: AbortSignal }): Promise<number>;

  /** Sends GOAWAY, waits for the open channels up to the grace, then closes. */
  goAway(status?: Status): Promise<void>;

  /** Closes the transport now; every channel ends with CONNECTION_LOST. */
  close(): Promise<void>;

  /**
   * The end status: GOING_AWAY if a GOAWAY was received, the GOAWAY status
   * this side sent if it ended the connection (PROTOCOL_ERROR, ...),
   * CONNECTION_LOST otherwise. Never rejects.
   */
  readonly done: Promise<Status>;
  readonly isOpen: boolean;
  readonly peerGoingAway: boolean;

  /** The peer's GOAWAY status as sent; never settles without one, never rejects. */
  readonly peerGoAwayStatus: Promise<Status>;

  /** Identifies to the peer: NONCE exchange and IDENT. See the wiki page "Identity and Credentials". */
  identify(
    credential: import('../identity/contract.ts').Credential,
    options?: IdentifyOptions,
  ): Promise<void>;

  /** This side's identity, which a peer's IDENT must name if it names anyone. */
  localIdentity: string | undefined;

  /** The last valid IDENT; undefined before, without a verifier, and once its credential expired. */
  readonly peerIdentity: PeerIdentity | undefined;

  /** The first valid IDENT; never settles without one, never rejects. */
  readonly peerIdentified: Promise<PeerIdentity>;

  /** The peer's NONCE arrived before ours; never rejects. */
  readonly identityRequested: Promise<void>;

  /** The next valid IDENT or the end. */
  readonly identityChanged: Promise<void>;

  /** What all channels received and did not deliver yet, at flow-control cost, plus held OPEN payloads. */
  readonly bufferedBytes: number;

  // Diagnostics of the frame mux; a binding answers neutrally.
  readonly sentBytes: number;
  readonly receivedBytes: number;
  readonly openPayloadBytes: number;
  readonly isReceivePaused: boolean;
  readonly unconfirmedCloseCount: number;
  readonly heldOutputBytes: number;
}

/**
 * A channel: subframes in both directions, a close with a status, flow
 * control. `readable` and `writable` are Web Streams so that a channel
 * pipes like any other stream; the methods beside them are the contract
 * Talk and the proxies use.
 */
export interface MuxChannel extends AsyncDisposable {
  readonly connection: MuxConnection;
  readonly id: number;
  readonly isLocallyOpened: boolean;
  readonly openPayload: Uint8Array;
  readonly state: MuxChannelState;

  /** state === 'open'. */
  readonly canSend: boolean;

  /** 'normal' by default; 'bulk': `send()` cuts payloads into chunks, scheduled after ordinary frames. */
  priority: MuxPriority;

  /**
   * Incoming subframes, one per chunk. Ends when the peer's CLOSE arrives
   * or the connection drops (the channel's `done` tells how). Taking a
   * chunk consumes it (credit goes back) unless {@link manualCredit}.
   */
  readonly readable: ReadableStream<Uint8Array>;

  /** Outgoing subframes: a `write` is a {@link send}; `close()` is `close(OK)`; `abort(status)` closes with that status. */
  readonly writable: WritableStream<Uint8Array>;

  /**
   * Sends one subframe. Waits for credit; resolves once handed to the
   * connection (the bytes are copied). Rejects with FAILED_PRECONDITION if
   * the channel stops sending first (never unhandled).
   *
   * @throws {SwitchboardError} FAILED_PRECONDITION if `!canSend`,
   * FRAME_TOO_LARGE above {@link maxSubframeLength} on an ordinary channel.
   */
  send(subframe: Uint8Array): Promise<void>;

  /** The flow-control cost of a subframe of [length] bytes. */
  sendCost(length: number): number;

  /** Largest subframe `send()` takes on an ordinary channel. */
  readonly maxSubframeLength: number;

  /** Cost this side may still send before credit. */
  readonly sendWindow: number;

  /** `canSend`, nothing waiting for credit, and room for the largest subframe. */
  readonly isWritable: boolean;

  /** Resolves when {@link isWritable} (at once if so); rejects FAILED_PRECONDITION when the channel can no longer send (never unhandled). */
  readonly whenWritable: Promise<void>;

  /** Cost the peer may still send; DATA beyond it is a protocol error. */
  readonly receiveWindow: number;

  /** Received, not yet delivered, at cost. */
  readonly bufferedBytes: number;

  /** false: taking a subframe from `readable` consumes it; true: the layer reports consumption with {@link consumed}. */
  manualCredit: boolean;

  /** Manual credit: delivered, not reported consumed. */
  readonly heldBytes: number;

  /** Manual credit: reports consumption; cost = bytes + overhead per subframe. @throws {Error} beyond what was delivered. */
  consumed(bytes: number, subframes?: number): void;

  /** CREDIT at once, raising the window; @throws {RangeError} if the window would pass 2^32 - 1. */
  grant(bytes: number): void;

  /** A channel on the same connection whose OPEN keeps its place among this channel's frames (bulk channels). */
  openAfter(openPayload: Uint8Array): MuxChannel;

  /** Sends CLOSE (behind subframes waiting for credit); resolves when mutually closed, lost, or the confirm timeout passed. */
  close(status?: Status): Promise<void>;

  /** The end status: the first non-OK status sent or received, else OK; CONNECTION_LOST or GOING_AWAY if the connection dropped. Never rejects. */
  readonly done: Promise<Status>;

  /** Something reads `readable` (bulk routing: a parent read raw takes no bulk channels). */
  readonly hasListener: boolean;

  /** The frame mux: DATA waits for the peer's LIMITS; a binding: false. */
  readonly awaitingLimits: boolean;

  /** Resolves once the peer's limits are known (at once for a binding). */
  readonly limitsKnown: Promise<void>;
}
