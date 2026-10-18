/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * The frame mux: a {@link MuxConnection} over a {@link FrameTransport}
 * carrying one mux frame per chunk (wiki page "Polyverse Switchboard
 * Mux"). This is how WebSocket, TCP and in-process connections carry
 * channels. Ports the frame mux of `mux_connection.dart`.
 */

import { bytesEqual, utf8Encode } from '../bytes.ts';
import type { Credential, CredentialVerifier, PeerIdentity } from '../identity/contract.ts';
import { secureRandomBytes } from '../identity/secure-random.ts';
import { ProtocolError, Status, StatusCode, SwitchboardError } from '../status.ts';
import type { FrameTransport } from '../transport/contract.ts';
import { Fifo } from '../transport/fifo.ts';
import type { Clock, TimerHandle } from '../util/clock.ts';
import { Deferred } from '../util/deferred.ts';
import type { LogLevel } from '../util/logger.ts';
import {
  type IdentifyOptions,
  type MuxChannel,
  type MuxConnection,
  type MuxOptions,
  muxOptions,
} from './contract.ts';
import { type MuxChannelHost, MuxChannelLink } from './frame-mux-channel.ts';
import {
  MuxCommand,
  MuxControlMessage,
  MuxControlType,
  MuxCredit,
  MuxFrame,
  MuxLimits,
} from './mux-frame.ts';
import {
  checkMuxStatus,
  decodeStatusPayload,
  fitStatus,
  identReceiverProblem,
  MuxIdent,
  statusFromWire,
  verifyIdent,
} from './mux-protocol.ts';
import { OutputScheduler } from './output-scheduler.ts';

const loggerName = 'Switchboard.Mux';

/** What {@link FrameMuxConnection} takes: the mux options, and the side. */
export interface FrameMuxConnectionOptions extends Partial<MuxOptions> {
  /**
   * True on the side that established the connection: it allocates even
   * channel ids, the acceptor odd ones.
   */
  readonly isInitiator: boolean;
}

/** A PING of {@link FrameMuxConnection.ping} awaiting its PONG. */
interface PendingPing {
  readonly payload: Uint8Array;
  readonly sent: number;
  readonly result: Deferred<number>;
}

/**
 * Most frames handed to the transport's writable and not taken by it yet.
 * Node's WritableStream takes its queued writes with `Array#shift`, which
 * turns quadratic once some 16K writes are queued; a burst beyond this
 * waits in the connection's own queue instead, in order.
 */
const maxWritesInFlight = 1024;

/** The status of the GOAWAY sent for an IDENT that fails verification. */
const unauthenticated = Status.of(StatusCode.unauthenticated, 'identification failed');

const empty = new Uint8Array(0);

function clampU32(value: number): number {
  return value < 0 ? 0 : value > 0xffffffff ? 0xffffffff : value;
}

/**
 * The frame mux: channels opened by either side over one transport, plus
 * what concerns the connection as a whole: round trips and keep-alive
 * ({@link ping}), graceful shutdown ({@link goAway}) and the peer's
 * identity ({@link identify}, {@link peerIdentity}). Implements
 * {@link MuxConnection}; the frame mux's own diagnostics
 * ({@link peerLimits}, {@link nextChannelIdForTesting}) come with it.
 *
 * ```ts
 * const connection = new FrameMuxConnection(transport, { isInitiator: true });
 * const channel = connection.open(utf8Encode('hello'));
 * for await (const incoming of connection.incoming) { … }
 * ```
 *
 * Frames go to the transport as the application produces them. When the
 * transport tells whether it is ready ({@link FrameTransport.isOutputReady})
 * and is not, DATA waits in per-channel queues and the output scheduler
 * picks the order in which they go once it is: control messages first
 * (they are never held), then ordinary channels, one frame each in turn,
 * then bulk channels, with one bulk frame after `MuxOptions.bulkZipper`
 * ordinary ones while bulk frames wait. The order within a channel never
 * changes.
 *
 * Reading the transport stops while more than
 * `MuxOptions.receiveHighWaterMarkBytes` are buffered over all channels,
 * and while an IDENT is verified, so that the frames after it see its
 * outcome.
 */
export class FrameMuxConnection implements MuxConnection {
  /**
   * Most PINGs of {@link ping} awaiting their PONG. Beyond it, the oldest
   * fails with DEADLINE_EXCEEDED, so that a peer that withholds PONGs
   * cannot make them accumulate.
   */
  static readonly maxPendingPings = 1024;

  /** Whether this side initiated the connection. */
  readonly isInitiator: boolean;

  /** Local configuration, completed with the defaults. */
  readonly options: MuxOptions;

  /**
   * This side's identity (its credential's identity), which the receiver
   * field of the peer's IDENT must name when it names anyone: an IDENT
   * naming another receiver is refused (GOAWAY UNAUTHENTICATED), so that
   * a peer cannot relay another node's identification to this one.
   * Undefined, the default, refuses every IDENT that names a receiver.
   */
  localIdentity: string | undefined;

  readonly #transport: FrameTransport;
  readonly #reader: ReadableStreamDefaultReader<Uint8Array>;
  readonly #writer: WritableStreamDefaultWriter<Uint8Array>;
  readonly #clock: Clock;
  readonly #host: MuxChannelHost;
  readonly #scheduler: OutputScheduler;
  readonly #undelivered = new Fifo<MuxChannelLink>();
  readonly #links = new Map<number, MuxChannelLink>();
  // Ids this side sent CLOSE for, without a channel, awaiting the peer's
  // CLOSE, with the receive credit the peer may still use on them.
  readonly #awaitingClose = new Map<number, number>();
  readonly #done = new Deferred<Status>();
  readonly #pings: PendingPing[] = [];
  readonly #firstId: number;
  readonly #firstLongId: number;
  readonly #shortIdCapacity: number;
  #nextShortId: number;
  #nextLongId: number;
  // Ids of this side's parity below 0x10000 that are open or await a CLOSE.
  #shortIdsInUse = 0;
  #openCount = 0;
  #pingCounter = 0;
  #bufferedBytes = 0;
  // OPEN payload bytes of the peer's channels still open or undelivered.
  #openPayloadBytes = 0;
  #incomingTaken = false;
  #incomingCancelled = false;
  #incomingEndRequested = false;
  #incomingWaiter: Deferred<IteratorResult<MuxChannel>> | undefined;
  readonly #adopters: ((channel: MuxChannel) => boolean)[] = [];
  readonly #admitters: ((openPayload: Uint8Array) => boolean)[] = [];
  #receivePaused = false;
  #closing = false;
  #writable = true;
  #goAwaySent = false;
  #peerGoAway: Status | undefined;
  readonly #peerGoAwayStatus = new Deferred<Status>();
  #sentBytes = 0;
  // Frames written past maxWritesInFlight, in order, and the writes the
  // transport has not taken yet. The output scheduler holds DATA while
  // frames wait here, as when the transport is not ready.
  readonly #backlog = new Fifo<Uint8Array>();
  #writesInFlight = 0;
  #backlogDrained: Deferred<void> | undefined;
  // The transport's output failed: nothing more is handed to it.
  #outputFailed = false;
  #receivedBytes = 0;
  #peerLimits: MuxLimits | undefined;
  // False until the peer's first frame (its LIMITS, normally) arrived, or
  // the wait for it ended (MuxOptions.awaitPeerLimits).
  #limitsKnown = true;
  #limitsTimer: TimerHandle | undefined;
  // Channels opened before then; their DATA waits.
  readonly #awaitingLimits: MuxChannelLink[] = [];
  #idle: Deferred<void> | undefined;
  #closeFuture: Promise<void> | undefined;
  // The read loop waits here while reading is paused.
  #resume: Deferred<void> | undefined;

  // Identity (wiki "Polyverse Switchboard Identity and Credentials").
  #localNonce: Uint8Array | undefined;
  #peerNonce: Uint8Array | undefined;
  #peerNonceWaiter: Deferred<Uint8Array> | undefined;
  readonly #identityRequested = new Deferred<void>();
  #peerIdentity: PeerIdentity | undefined;
  readonly #peerIdentified = new Deferred<PeerIdentity>();
  #identityEvent: Deferred<void> | undefined;
  // Frames that arrived while an IDENT was verified; handled after it, in
  // order, so that they see its outcome. The read loop does not read while
  // verifying, so this holds only what a verification leaves behind.
  readonly #heldFrames = new Fifo<Uint8Array>();
  #verifyingIdent = false;
  // The peer's GOAWAY status as sent, for identify().
  #peerGoAwayReceived: Status | undefined;

  #keepAliveTimer: TimerHandle | undefined;
  #lastReceive: number;
  #keepAliveProbing = false;
  #lastAcceptedOutput = -1;

  // The status sent in this side's own graceful GOAWAY, if any.
  #ownGoAway: Status | undefined;
  // The status the connection ended (or is ending) with.
  #closeStatus: Status | undefined;

  /**
   * Starts the frame mux over [transport], taking its readable and
   * writable for good. Sends LIMITS at once if `announceLimits`; if the
   * transport accepts smaller frames than `maxFrameSize`
   * ({@link FrameTransport.maxFrameSize}), logs a warning and announces
   * the transport's limit.
   *
   * @throws {RangeError} if `initialWindow` is not 1 to `2^32 - 1`, or if
   *   `bulkZipper` or `bulkChunkSize` is below 1.
   * @throws {TypeError} if the transport's streams are locked already.
   */
  constructor(transport: FrameTransport, options: FrameMuxConnectionOptions) {
    const { isInitiator, ...rest } = options;
    const resolved = muxOptions(rest);
    if (
      !Number.isInteger(resolved.initialWindow) ||
      resolved.initialWindow < 1 ||
      resolved.initialWindow > MuxLimits.maxWindow
    ) {
      throw new RangeError(
        `options.initialWindow ${resolved.initialWindow} must be 1 to ${MuxLimits.maxWindow}`,
      );
    }
    if (!(resolved.bulkZipper >= 1)) {
      throw new RangeError(`options.bulkZipper ${resolved.bulkZipper} must be at least 1`);
    }
    if (!(resolved.bulkChunkSize >= 1)) {
      throw new RangeError(`options.bulkChunkSize ${resolved.bulkChunkSize} must be at least 1`);
    }
    this.isInitiator = isInitiator;
    this.options = resolved;
    this.#transport = transport;
    this.#clock = resolved.clock;
    this.#lastReceive = this.#clock.now();
    this.#firstId = isInitiator ? 2 : 3;
    this.#firstLongId = MuxFrame.maxShortId + (isInitiator ? 1 : 2);
    this.#nextShortId = this.#firstId;
    this.#nextLongId = this.#firstLongId;
    let shortIds = Math.floor((MuxFrame.maxShortId - this.#firstId) / 2) + 1;
    for (let id = this.#firstId; id <= MuxFrame.maxShortId; id += 2) {
      if (MuxFrame.isReservedId(id)) {
        shortIds--;
      }
    }
    this.#shortIdCapacity = shortIds;
    this.#host = {
      options: resolved,
      peerMaxFrameSize: () => this.#peerMaxFrameSize,
      reprioritize: (link) => this.#scheduler.reprioritize(link),
      sendFrame: (frame) => this.#sendFrame(frame),
      sendChannelFrame: (link, frame, opens) => this.#sendChannelFrame(link, frame, opens),
      openAfter: (_parent, openPayload) => this.#openLink(openPayload, false),
      openFailed: (link, status) => this.#openFailed(link, status),
      dropQueuedData: (link) => this.#scheduler.dropQueuedData(link.id),
      release: (link) => this.#release(link),
      abandon: (link) => this.#abandon(link),
      noteBuffered: (delta) => this.#noteBuffered(delta),
    };
    this.#scheduler = new OutputScheduler(
      {
        writable: () => this.#writable,
        outputReady: () => this.#backlog.length === 0 && (this.#transport.isOutputReady ?? true),
        whenOutputReady: () => {
          if (this.#backlog.length > 0) {
            this.#backlogDrained ??= new Deferred<void>();
            return this.#backlogDrained.promise;
          }
          return this.#transport.outputReady;
        },
        link: (id) => this.#links.get(id),
        writeOpening: (bytes, opens) => this.#writeOpening(bytes, opens),
      },
      resolved.bulkZipper,
    );
    this.#reader = transport.readable.getReader();
    this.#writer = transport.writable.getWriter();
    this.#writer.closed.catch(() => {
      this.#writable = false;
    });
    let receiveLimit = resolved.maxFrameSize;
    const transportLimit = transport.maxFrameSize ?? 0;
    if (transportLimit > 0 && (receiveLimit <= 0 || transportLimit < receiveLimit)) {
      this.#log(
        'warn',
        `${this}: the transport accepts frames of at most ${transportLimit} bytes, less than ` +
          `MuxOptions.maxFrameSize ${receiveLimit}; announcing ${transportLimit}`,
      );
      receiveLimit = transportLimit;
    }
    this.#limitsKnown = !resolved.awaitPeerLimits || resolved.keepAliveTimeout <= 0;
    if (resolved.announceLimits) {
      this.#sendFrame(
        MuxControlMessage.limits(
          new MuxLimits({
            maxFrameSize: clampU32(receiveLimit),
            maxChannels: clampU32(resolved.maxChannels),
            initialWindow: resolved.initialWindow,
          }),
        ).toFrame(),
      );
    }
    if (!this.#limitsKnown && !this.#closing) {
      this.#limitsTimer = this.#clock.setTimeout(() => {
        this.#limitsTimer = undefined;
        if (!this.#limitsKnown) {
          this.#log(
            'debug',
            `${this}: no frame from the peer within ${resolved.keepAliveTimeout} ms; sending ` +
              'against the 64 KiB window assumed without LIMITS',
          );
        }
        this.#endLimitsWait();
      }, resolved.keepAliveTimeout);
    }
    this.#startKeepAlive();
    void this.#readLoop();
  }

  // The connection ------------------------------------------------------------

  /** The transport's description of the peer, or `unknown`. */
  get remote(): string {
    return this.#transport.remote ?? 'unknown';
  }

  /**
   * Channels opened by the peer, in order, buffered until taken; one
   * consumer. The backlog, closed channels included, is bounded by
   * `MuxOptions.maxChannels`; OPENs beyond it get CLOSE
   * RESOURCE_EXHAUSTED. Stopping the iteration closes the channels not
   * taken yet with UNAVAILABLE, and refuses the ones arriving after the
   * same way. Ends once the connection ended and every channel was taken.
   */
  get incoming(): AsyncIterable<MuxChannel> {
    return { [Symbol.asyncIterator]: () => this.#iterateIncoming() };
  }

  /**
   * Installs a filter that sees every peer-opened channel before
   * {@link incoming}: one it returns true for is adopted and does not
   * reach {@link incoming}. Filters run in installation order, on the
   * OPEN; a filter that throws is logged and passed over. Returns the
   * uninstaller.
   */
  adoptIncoming(filter: (channel: MuxChannel) => boolean): () => void {
    this.#adopters.push(filter);
    return () => {
      const i = this.#adopters.indexOf(filter);
      if (i >= 0) {
        this.#adopters.splice(i, 1);
      }
    };
  }

  /**
   * Installs a rule for the OPENs that arrive after this side sent GOAWAY:
   * one a predicate accepts continues work the connection admitted (Talk
   * admits the `_bulk` channels of its open channels); the others are
   * refused with CLOSE GOING_AWAY. A predicate that throws refuses.
   * Returns the uninstaller.
   */
  admitAfterGoAway(predicate: (openPayload: Uint8Array) => boolean): () => void {
    this.#admitters.push(predicate);
    return () => {
      const i = this.#admitters.indexOf(predicate);
      if (i >= 0) {
        this.#admitters.splice(i, 1);
      }
    };
  }

  /**
   * Resolves with the end status, never rejects: GOING_AWAY if the peer
   * sent GOAWAY (its status in the reason if it was another); the status
   * of the GOAWAY this side sent when it ended the connection because the
   * peer broke the protocol (PROTOCOL_ERROR, FRAME_TOO_LARGE,
   * UNSUPPORTED), left too many CLOSEs unconfirmed (RESOURCE_EXHAUSTED) or
   * sent an IDENT that failed verification (UNAUTHENTICATED); else the
   * status of this side's {@link goAway}; otherwise CONNECTION_LOST.
   */
  get done(): Promise<Status> {
    return this.#done.promise;
  }

  /** Whether the transport is open and no local close (including {@link goAway}) has started. */
  get isOpen(): boolean {
    return !this.#closing && !this.#goAwaySent;
  }

  /** Whether the peer sent GOAWAY. */
  get peerGoingAway(): boolean {
    return this.#peerGoAway !== undefined;
  }

  /**
   * Resolves with the status of the peer's GOAWAY as sent (an application
   * code reads as UNKNOWN); never settles without one, never rejects.
   */
  get peerGoAwayStatus(): Promise<Status> {
    return this.#peerGoAwayStatus.promise;
  }

  /** Bytes of the mux frames handed to the transport so far, headers included. */
  get sentBytes(): number {
    return this.#sentBytes;
  }

  /** Bytes of the mux frames received from the transport so far, headers included. */
  get receivedBytes(): number {
    return this.#receivedBytes;
  }

  /** The limits the peer announced with LIMITS, or undefined if none arrived. A frame mux diagnostic. */
  get peerLimits(): MuxLimits | undefined {
    return this.#peerLimits;
  }

  /** The channel limit of the peer's LIMITS; 0 for none (or none stated yet). */
  get peerMaxChannels(): number {
    return this.#peerLimits?.maxChannels ?? 0;
  }

  /** True: the frame mux carries every channel on one ordered transport. */
  get hasOrderedChannels(): boolean {
    return true;
  }

  /**
   * Channels not mutually closed yet, in both directions. Peer channels
   * the connection rejected itself, and channels whose close confirmation
   * timed out, are not counted.
   */
  get openChannelCount(): number {
    return this.#openCount;
  }

  /** The channel with [id] that is not mutually closed yet, if any. */
  channelWithId(id: number): MuxChannel | undefined {
    return this.#links.get(id)?.channel;
  }

  /** The channels counted by {@link openChannelCount}, as a snapshot. */
  get channels(): Iterable<MuxChannel> {
    return [...this.#links.values()].map((link) => link.channel);
  }

  /**
   * Receive backlog at flow-control cost over all channels (closed,
   * undelivered ones included) plus the OPEN payloads held, which
   * `MuxOptions.receiveHighWaterMarkBytes` bounds. What a reader with
   * `manualCredit` holds is not counted; each channel's window bounds it.
   */
  get bufferedBytes(): number {
    return this.#bufferedBytes;
  }

  /** Bytes of OPEN payloads held for the peer's channels still open or undelivered. */
  get openPayloadBytes(): number {
    return this.#openPayloadBytes;
  }

  /** Whether reading the transport is paused because more than the receive high-water mark is buffered. */
  get isReceivePaused(): boolean {
    return this.#receivePaused && !this.#closing;
  }

  /** Channel ids this side sent CLOSE for without keeping a channel, whose confirmation has not arrived. */
  get unconfirmedCloseCount(): number {
    return this.#awaitingClose.size;
  }

  /** Bytes of the frames the output scheduler holds because the transport was not ready. */
  get heldOutputBytes(): number {
    return this.#scheduler.heldBytes;
  }

  /**
   * Who the peer proved to be with its last valid IDENT, or undefined if
   * it has not identified (or `MuxOptions.identityVerifier` is undefined,
   * so its IDENT was ignored); undefined again once the credential has
   * expired, by `MuxOptions.wallTime`, until a valid IDENT with a renewed
   * one arrives.
   * Frames after an IDENT wait for its verification, so a channel opened
   * right after it sees that identity.
   */
  get peerIdentity(): PeerIdentity | undefined {
    const identity = this.#peerIdentity;
    return identity === undefined || identity.isExpired(this.options.wallTime?.())
      ? undefined
      : identity;
  }

  /** Resolves with the first valid IDENT of the peer; never settles without one, never rejects. */
  get peerIdentified(): Promise<PeerIdentity> {
    return this.#peerIdentified.promise;
  }

  /**
   * Resolves when the peer's NONCE arrives before this side sent one: the
   * peer intends to identify, or asks this side to. The connection has
   * answered with its own NONCE already. Never settles otherwise, never
   * rejects.
   */
  get identityRequested(): Promise<void> {
    return this.#identityRequested.promise;
  }

  /**
   * Resolves at the next valid IDENT of the peer, or when the connection
   * ends; never rejects. The new identity is visible in
   * {@link peerIdentity} by then. Each read waits for the next change.
   */
  get identityChanged(): Promise<void> {
    this.#identityEvent ??= new Deferred<void>();
    return this.#identityEvent.promise;
  }

  /**
   * Sets the next id the allocator tries: in the short range for ids below
   * 0x10000, else in the long range (used only once every short id of this
   * side's parity is in use). For tests of id wrapping.
   */
  set nextChannelIdForTesting(id: number) {
    if (id <= MuxFrame.maxShortId) {
      this.#nextShortId = id;
    } else {
      this.#nextLongId = id;
    }
  }

  /**
   * Identifies this side to the peer with [credential] (wiki page
   * "Polyverse Switchboard Identity and Credentials", section "Connection
   * identity"): sends NONCE unless this side has sent one, waits for the
   * peer's NONCE, and sends IDENT with `intent` (at most 64 bytes) and
   * `receiver` (the identity the peer is expected to have; undefined or
   * empty names nobody), with the proof signed by `holderKey` for a
   * credential with a holder key. Resolves once the peer has handled the
   * IDENT: a PING sent right after it was answered, which the peer does
   * only after verifying the IDENT. Calling again sends another IDENT,
   * which replaces the first at the peer.
   *
   * `timeout` (default `MuxOptions.identityTimeout`; 0: no bound) bounds
   * the whole exchange; `signal` ends the wait early with its reason.
   *
   * Rejects with a {@link SwitchboardError}: UNAUTHENTICATED when the peer
   * refused the credential, DEADLINE_EXCEEDED when the peer's NONCE or the
   * confirmation did not arrive in time, FAILED_PRECONDITION when the
   * connection is closed, the connection's end status when it ends
   * meanwhile; with a {@link RangeError} when the credential names a holder
   * key and `holderKey` is missing or another, and when the intent, the
   * receiver or the IDENT is too long. The promise is safe to drop: a
   * rejection nobody awaits is not reported as unhandled.
   */
  identify(credential: Credential, options: IdentifyOptions = {}): Promise<void> {
    const identifying = this.#identify(credential, options);
    identifying.catch(() => {});
    return identifying;
  }

  async #identify(credential: Credential, options: IdentifyOptions): Promise<void> {
    const holder = credential.holderKey;
    const holderKey = options.holderKey;
    if (holder !== undefined) {
      if (holderKey === undefined) {
        throw new RangeError(
          'the credential names a holder key, the proof needs its key pair (holderKey)',
        );
      }
      if (!bytesEqual(holderKey.publicKey, holder)) {
        throw new RangeError('holderKey is not the holder key the credential names');
      }
    }
    const intent = options.intent === undefined ? empty : options.intent.slice();
    const receiver = options.receiver;
    const receiverBytes = receiver === undefined ? empty : utf8Encode(receiver);
    const credentialBytes = credential.encode();
    // Checks the lengths before anything is sent.
    new MuxIdent({
      credential: credentialBytes,
      intent,
      receiver: receiverBytes,
      proof: holder === undefined ? undefined : new Uint8Array(MuxIdent.proofLength),
    }).encode();
    if (this.#closing) {
      throw SwitchboardError.of(StatusCode.failedPrecondition, 'connection is closed');
    }
    const limit = options.timeout ?? this.options.identityTimeout;
    const started = this.#clock.now();
    const signal = options.signal;
    signal?.throwIfAborted();
    const bounded = <T>(promise: Promise<T>, what: string): Promise<T> =>
      this.#bounded(promise, limit, started, signal, `${what} within ${limit} ms`);

    const localNonce = this.#localNonce ?? this.#sendNonce();
    let peerNonce = this.#peerNonce;
    if (peerNonce === undefined) {
      this.#peerNonceWaiter ??= new Deferred<Uint8Array>();
      try {
        peerNonce = await bounded(this.#peerNonceWaiter.promise, 'no NONCE from the peer');
      } catch (e) {
        throw this.#identifyError(e);
      }
    }
    let proof: Uint8Array | undefined;
    if (holderKey !== undefined && holder !== undefined) {
      proof = await bounded(
        holderKey.sign(MuxIdent.proofMessage(localNonce, peerNonce, intent, receiverBytes)),
        'proof not signed',
      );
    }
    if (this.#closing) {
      throw this.#identifyFailure();
    }
    this.#log(
      'debug',
      `${this}: identifying as "${credential.identity}"` +
        (receiver === undefined || receiver.length === 0 ? '' : ` to "${receiver}"`),
    );
    this.#sendFrame(
      MuxControlMessage.ident(
        new MuxIdent({ credential: credentialBytes, intent, receiver: receiverBytes, proof }),
      ).toFrame(),
    );
    try {
      await bounded(this.ping(), 'IDENT not confirmed');
    } catch (e) {
      throw this.#identifyError(e);
    }
  }

  /** What {@link identify} rejects with for [error] of one of its waits. */
  #identifyError(error: unknown): unknown {
    if (!(error instanceof SwitchboardError) || error.code === StatusCode.deadlineExceeded) {
      // A deadline, or the caller's signal.
      return error;
    }
    return this.#identifyFailure();
  }

  /** Why {@link identify} failed once the connection ended. */
  #identifyFailure(): SwitchboardError {
    const peer = this.#peerGoAwayReceived;
    if (peer?.known === StatusCode.unauthenticated) {
      return SwitchboardError.of(StatusCode.unauthenticated, 'the peer refused the credential');
    }
    if (this.#done.isSettled || this.#closing) {
      return new SwitchboardError(this.#closeStatus ?? this.#lostStatus());
    }
    return SwitchboardError.of(StatusCode.failedPrecondition, 'connection is closed');
  }

  /**
   * [promise], rejected with DEADLINE_EXCEEDED [message] once [limit] ms
   * passed since [started] (0: no bound), or with [signal]'s reason.
   */
  #bounded<T>(
    promise: Promise<T>,
    limit: number,
    started: number,
    signal: AbortSignal | undefined,
    message: string,
  ): Promise<T> {
    if (limit <= 0 && signal === undefined) {
      return promise;
    }
    return new Promise<T>((resolve, reject) => {
      let timer: TimerHandle | undefined;
      const onAbort = (): void => {
        settle();
        reject(signal?.reason);
      };
      const settle = (): void => {
        if (timer !== undefined) {
          this.#clock.clearTimeout(timer);
          timer = undefined;
        }
        signal?.removeEventListener('abort', onAbort);
      };
      if (signal !== undefined) {
        if (signal.aborted) {
          reject(signal.reason);
          return;
        }
        signal.addEventListener('abort', onAbort, { once: true });
      }
      if (limit > 0) {
        const left = limit - (this.#clock.now() - started);
        timer = this.#clock.setTimeout(
          () => {
            timer = undefined;
            settle();
            reject(SwitchboardError.of(StatusCode.deadlineExceeded, message));
          },
          left > 0 ? left : 0,
        );
      }
      promise.then(
        (value) => {
          settle();
          resolve(value);
        },
        (error: unknown) => {
          settle();
          reject(error);
        },
      );
    });
  }

  /**
   * Opens a channel carrying [openPayload] (copied) in its OPEN frame. The
   * channel is usable at once.
   *
   * @throws {SwitchboardError} FAILED_PRECONDITION if the connection is
   *   closing, this side sent GOAWAY or the peer did; RESOURCE_EXHAUSTED
   *   if no channel id is free or the peer's channel limit is reached;
   *   FRAME_TOO_LARGE if the OPEN frame would exceed the peer's frame
   *   limit.
   */
  open(openPayload: Uint8Array): MuxChannel {
    return this.#openLink(openPayload, true).channel;
  }

  /**
   * Opens a channel: sends its OPEN unless [openWritten] is false, when the
   * caller hands it to the output scheduler in its place (`openAfter`).
   */
  #openLink(openPayload: Uint8Array, openWritten: boolean): MuxChannelLink {
    // A channel opened after another one (openAfter: a Talk bulk channel)
    // continues work the connection admitted already: a GOAWAY, this
    // side's or the peer's, does not stop it.
    const continuation = !openWritten;
    if (this.#closing || (this.#goAwaySent && !continuation)) {
      throw SwitchboardError.of(StatusCode.failedPrecondition, 'connection is closing');
    }
    if (this.#peerGoAway !== undefined && !continuation) {
      throw SwitchboardError.of(StatusCode.failedPrecondition, 'peer is going away');
    }
    const peerMaxChannels = this.#peerLimits?.maxChannels ?? 0;
    if (peerMaxChannels > 0 && this.#openCount >= peerMaxChannels) {
      throw SwitchboardError.of(
        StatusCode.resourceExhausted,
        `peer accepts at most ${peerMaxChannels} channels`,
      );
    }
    const id = this.#allocateId();
    const peerMaxFrame = this.#peerMaxFrameSize;
    const size = MuxFrame.headerSizeFor(id) + openPayload.length;
    if (peerMaxFrame > 0 && size > peerMaxFrame) {
      throw SwitchboardError.of(
        StatusCode.frameTooLarge,
        `OPEN of ${size} bytes exceeds the peer limit of ${peerMaxFrame}`,
      );
    }
    const payload = openPayload.slice();
    const link = new MuxChannelLink(this.#host, this, {
      id,
      isLocallyOpened: true,
      openPayload: payload,
      sendWindow: this.#peerInitialWindow,
      awaitingLimits: !this.#limitsKnown,
      // The peer learns of the channel after this side's LIMITS.
      receiveWindow: this.#localInitialWindow,
      creditWindow: this.#localInitialWindow,
    });
    if (!this.#limitsKnown) {
      this.#awaitingLimits.push(link);
    }
    this.#links.set(id, link);
    this.#openCount++;
    if (this.#isOwnShortId(id)) {
      this.#shortIdsInUse++;
    }
    if (openWritten) {
      this.#sendFrame(MuxFrame.open(id, payload));
    } else {
      link.openWritten = false;
      link.holdsFrames = true;
    }
    return link;
  }

  /** The OPEN of [link] will never be written: the channel ends locally and its id is free again. */
  #openFailed(link: MuxChannelLink, status: Status): void {
    if (this.#links.get(link.id) !== link || link.openWritten) {
      return;
    }
    this.#links.delete(link.id);
    this.#openCount--;
    if (this.#isOwnShortId(link.id)) {
      this.#shortIdsInUse--;
    }
    const waiting = this.#awaitingLimits.indexOf(link);
    if (waiting >= 0) {
      this.#awaitingLimits.splice(waiting, 1);
    }
    const nested: MuxChannelLink[] = [];
    for (const [, opens] of link.preOpen) {
      if (opens !== undefined) {
        nested.push(opens);
      }
    }
    link.neverOpened(status);
    for (const opens of nested) {
      this.#openFailed(opens, status);
    }
    queueMicrotask(() => this.#checkIdle());
  }

  /**
   * Ends the wait for the peer's LIMITS: the channels opened meanwhile get
   * the window it announced (64 KiB without one) and send the DATA that
   * waited.
   */
  #endLimitsWait(): void {
    if (this.#limitsKnown) {
      return;
    }
    this.#limitsKnown = true;
    if (this.#limitsTimer !== undefined) {
      this.#clock.clearTimeout(this.#limitsTimer);
      this.#limitsTimer = undefined;
    }
    const window = this.#peerInitialWindow;
    const links = this.#awaitingLimits.splice(0);
    for (const link of links) {
      link.limitsKnown(window);
    }
  }

  /**
   * Sends PING and resolves with the round trip in milliseconds when the
   * matching PONG arrives. Without [payload] a unique one is generated.
   * Rejects with a {@link SwitchboardError} if the connection is closed or
   * ends before the PONG, with DEADLINE_EXCEEDED if
   * {@link FrameMuxConnection.maxPendingPings} later PINGs are sent before
   * it arrives, and with the signal's reason when it aborts.
   *
   * @throws {RangeError} if [payload] is longer than 125 bytes.
   */
  ping(payload?: Uint8Array, options: { signal?: AbortSignal } = {}): Promise<number> {
    const message = MuxControlMessage.ping(payload ?? this.#nextPingPayload());
    if (this.#closing) {
      return Promise.reject(
        SwitchboardError.of(StatusCode.failedPrecondition, 'connection is closed'),
      );
    }
    const signal = options.signal;
    if (signal?.aborted) {
      return Promise.reject(signal.reason);
    }
    if (this.#pings.length >= FrameMuxConnection.maxPendingPings) {
      this.#pings
        .shift()
        ?.result.reject(
          SwitchboardError.of(
            StatusCode.deadlineExceeded,
            `no PONG before ${FrameMuxConnection.maxPendingPings} later PINGs`,
          ),
        );
    }
    const pending: PendingPing = {
      payload: message.payload.slice(),
      sent: this.#clock.now(),
      result: new Deferred<number>(),
    };
    this.#pings.push(pending);
    if (signal !== undefined) {
      const onAbort = (): void => {
        const i = this.#pings.indexOf(pending);
        if (i >= 0) {
          this.#pings.splice(i, 1);
        }
        pending.result.reject(signal.reason);
      };
      signal.addEventListener('abort', onAbort, { once: true });
      const remove = (): void => signal.removeEventListener('abort', onAbort);
      pending.result.promise.then(remove, remove);
    }
    this.#sendFrame(message.toFrame());
    return pending.result.promise;
  }

  /**
   * Graceful shutdown: sends GOAWAY carrying [status] (default
   * GOING_AWAY), stops accepting channels (peer OPENs get CLOSE
   * GOING_AWAY, but for those {@link admitAfterGoAway} admits), waits up to
   * `MuxOptions.goAwayGrace` for the open channels to finish, then
   * {@link close}s. The reason is shortened on the wire to fit; the
   * local-only CONNECTION_LOST goes as UNAVAILABLE. Never rejects.
   *
   * @throws {RangeError} for an application status code (256 and above).
   */
  goAway(status: Status = Status.of(StatusCode.goingAway)): Promise<void> {
    checkMuxStatus(status);
    if (this.#closing) {
      return this.close();
    }
    if (!this.#goAwaySent) {
      this.#goAwaySent = true;
      this.#ownGoAway = status;
      this.#log('debug', `${this}: sending GOAWAY ${status}`);
      this.#sendGoAway(status);
    }
    return this.#goAwayWait();
  }

  async #goAwayWait(): Promise<void> {
    if (this.#openCount > 0 && !this.#closing) {
      this.#idle ??= new Deferred<void>();
      const idle = this.#idle;
      const grace = this.#clock.setTimeout(() => {
        if (!idle.isSettled) {
          this.#log('debug', `${this}: GOAWAY grace expired with ${this.#openCount} channels`);
          idle.resolve();
        }
      }, this.options.goAwayGrace);
      await idle.promise;
      this.#clock.clearTimeout(grace);
    }
    await this.close();
  }

  /**
   * Closes the transport now. Every channel ends with CONNECTION_LOST (or
   * the GOAWAY status if either side sent GOAWAY) and {@link done}
   * resolves. Resolves once the transport has closed, waiting at most
   * `MuxOptions.keepAliveTimeout` for it; a transport still open then is
   * aborted ({@link FrameTransport.abort}). Never rejects.
   */
  close(): Promise<void> {
    return this.#shutdown(this.#lostStatus(), undefined);
  }

  /** {@link close}, for `await using`. */
  [Symbol.asyncDispose](): Promise<void> {
    return this.close();
  }

  /** `MuxConnection(initiator, 2 channels)`. */
  toString(): string {
    return (
      `MuxConnection(${this.isInitiator ? 'initiator' : 'acceptor'}, ` +
      `${this.#openCount} channels${this.#closing ? ', closed' : ''})`
    );
  }

  // Allocation ----------------------------------------------------------------

  #isOwnShortId(id: number): boolean {
    return id <= MuxFrame.maxShortId && id % 2 === this.#firstId % 2;
  }

  /**
   * Allocates by incrementing within the short range (ids below 0x10000
   * of this side's parity), wrapping there, so that a long-lived
   * connection keeps using short ids however many channels come and go.
   * The long range is used only while every short id is in use, and never
   * with `shortIdsOnly`. Ids reserved, open or awaiting the peer's CLOSE
   * are skipped.
   */
  #allocateId(): number {
    if (this.#shortIdsInUse < this.#shortIdCapacity) {
      const id = this.#scan(this.#nextShortId, this.#firstId, MuxFrame.maxShortId);
      if (id !== undefined) {
        this.#nextShortId = id + 2;
        return id;
      }
      this.#log('error', `${this}: short id accounting is off, ${this.#shortIdsInUse} used`);
    }
    if (!this.options.shortIdsOnly) {
      const id = this.#scan(this.#nextLongId, this.#firstLongId, MuxFrame.maxId);
      if (id !== undefined) {
        this.#nextLongId = id + 2;
        return id;
      }
    }
    throw SwitchboardError.of(StatusCode.resourceExhausted, 'no free channel id');
  }

  /** The first free id of this side's parity from [start], wrapping from [last] to [first]. */
  #scan(start: number, first: number, last: number): number | undefined {
    const candidates = Math.floor((last - first) / 2) + 1;
    let id = start < first ? first : start;
    for (let i = 0; i < candidates; i++) {
      if (id > last) {
        id = first;
      }
      const candidate = id;
      id += 2;
      if (
        MuxFrame.isReservedId(candidate) ||
        this.#links.has(candidate) ||
        this.#awaitingClose.has(candidate)
      ) {
        continue;
      }
      return candidate;
    }
    return undefined;
  }

  // Sending -------------------------------------------------------------------

  /** Writes [frame] to the transport now, whatever is held: control messages, OPEN, and CLOSE with nothing queued before it. */
  #sendFrame(frame: MuxFrame): void {
    if (!this.#writable) {
      return;
    }
    this.#write(frame.encode());
  }

  #write(bytes: Uint8Array): void {
    if (!this.#writable) {
      return;
    }
    if (this.#writer.desiredSize === null) {
      this.#log('debug', `${this}: transport write failed: the transport failed`);
      this.#writable = false;
      return;
    }
    this.#sentBytes += bytes.length;
    if (this.#backlog.length > 0 || this.#writesInFlight >= maxWritesInFlight) {
      this.#backlog.push(bytes);
      return;
    }
    this.#hand(bytes);
  }

  /** Hands [bytes] to the transport's writable. */
  #hand(bytes: Uint8Array): void {
    this.#writesInFlight++;
    this.#writer.write(bytes).then(
      () => {
        this.#writesInFlight--;
        this.#drainBacklog();
      },
      (error: unknown) => {
        this.#writesInFlight--;
        if (!this.#outputFailed) {
          this.#outputFailed = true;
          if (this.#writable) {
            this.#log('debug', `${this}: transport write failed: ${error}`);
            this.#writable = false;
          }
          this.#backlog.clear();
          this.#drainBacklog();
        }
      },
    );
  }

  /** Hands the frames of the backlog over as the transport takes the writes before them. */
  #drainBacklog(): void {
    while (
      this.#backlog.length > 0 &&
      this.#writesInFlight < maxWritesInFlight &&
      !this.#outputFailed
    ) {
      this.#hand(this.#backlog.shift() as Uint8Array);
    }
    if (this.#backlog.length === 0) {
      const drained = this.#backlogDrained;
      this.#backlogDrained = undefined;
      drained?.resolve();
    }
  }

  /** Resolves once every frame of the backlog was handed to the transport (or the output failed). */
  #backlogHandedOver(): Promise<void> {
    if (this.#backlog.length === 0) {
      return Promise.resolve();
    }
    this.#backlogDrained ??= new Deferred<void>();
    return this.#backlogDrained.promise;
  }

  /** Hands [frame] of [link] to the output: held with the channel while it holds frames, else scheduled. */
  #sendChannelFrame(link: MuxChannelLink, frame: MuxFrame, opens?: MuxChannelLink): void {
    if (!this.#writable) {
      return;
    }
    if (link.holdsFrames) {
      // The channel's own OPEN waits behind its parent's frames, or the
      // frames that waited for it are going out: this one follows them.
      // send has handed the payload over: the caller may reuse it now.
      link.preOpen.push([
        new MuxFrame(frame.command, frame.channelId, frame.payload.slice()),
        opens,
      ]);
      return;
    }
    this.#scheduler.schedule(link, frame, opens);
  }

  /**
   * Makes an OPEN visible before writing it, then writes what its channel
   * handed over meanwhile behind it. Until then its id is only reserved
   * locally, and DATA or CLOSE from the peer on it is refused.
   */
  #writeOpening(bytes: Uint8Array, opens: MuxChannelLink | undefined): void {
    if (opens !== undefined) {
      opens.openWritten = true;
    }
    this.#write(bytes);
    if (opens !== undefined) {
      this.#openWritten(opens);
    }
  }

  /** The OPEN of [link] (`openAfter`) was written: what the channel sent meanwhile follows it. */
  #openWritten(link: MuxChannelLink): void {
    link.openWritten = true;
    link.openWrittenAfterClose();
    try {
      // Read as it grows: what the channel hands over meanwhile is added behind.
      for (let i = 0; i < link.preOpen.length; i++) {
        const [frame, opens] = link.preOpen[i] as [MuxFrame, MuxChannelLink | undefined];
        if (link.closeReceived && frame.command === MuxCommand.data) {
          // The peer closed the channel meanwhile: DATA would land on an id
          // it considers closed.
          continue;
        }
        this.#scheduler.schedule(link, frame, opens);
      }
    } finally {
      link.preOpen = [];
      link.holdsFrames = false;
    }
  }

  /** Sends GOAWAY with [status] shortened to fit the peer's frame limit. */
  #sendGoAway(status: Status): void {
    // Header and control type byte.
    const overhead = MuxFrame.shortHeaderSize + 1;
    const fitted = fitStatus(status, overhead, this.#peerMaxFrameSize);
    this.#sendFrame(MuxControlMessage.goAway(fitted).toFrame());
  }

  get #peerMaxFrameSize(): number {
    return this.#peerLimits?.maxFrameSize ?? 0;
  }

  /** The send window of a channel opened now: the initial window of the peer's last LIMITS, 64 KiB before any. */
  get #peerInitialWindow(): number {
    return this.#peerLimits?.initialWindow ?? MuxLimits.defaultInitialWindow;
  }

  /** The window this side grants: the one announced in LIMITS, or the 64 KiB the peer assumes without it. */
  get #localInitialWindow(): number {
    return this.options.announceLimits
      ? this.options.initialWindow
      : MuxLimits.defaultInitialWindow;
  }

  #nextPingPayload(): Uint8Array {
    const n = this.#pingCounter;
    this.#pingCounter = (n + 1) % 0x80000000;
    return Uint8Array.of(n & 0xff, (n >>> 8) & 0xff, (n >>> 16) & 0xff, (n >>> 24) & 0xff);
  }

  // Receiving -----------------------------------------------------------------

  /** Whether reading the transport waits: too much buffered, or an IDENT being verified. */
  get #readPaused(): boolean {
    return this.#receivePaused || this.#verifyingIdent;
  }

  /** Lets the read loop go on if nothing holds it any more. */
  #resumeReading(): void {
    if (this.#readPaused && !this.#closing) {
      return;
    }
    const resume = this.#resume;
    this.#resume = undefined;
    resume?.resolve();
  }

  async #readLoop(): Promise<void> {
    const reader = this.#reader;
    for (;;) {
      while (this.#readPaused && !this.#closing) {
        this.#resume ??= new Deferred<void>();
        await this.#resume.promise;
      }
      if (this.#closing) {
        return;
      }
      let result: ReadableStreamReadResult<Uint8Array>;
      try {
        result = await reader.read();
      } catch (error) {
        this.#onTransportError(error);
        return;
      }
      if (result.done) {
        this.#onTransportDone();
        return;
      }
      this.#onFrame(result.value);
    }
  }

  #onFrame(bytes: Uint8Array): void {
    if (this.#closing) {
      return;
    }
    this.#receivedBytes += bytes.length;
    this.#noteReceive();
    if (this.#verifyingIdent) {
      // Waits for the IDENT like the rest.
      this.#heldFrames.push(bytes);
      return;
    }
    this.#processFrame(bytes);
  }

  #processFrame(bytes: Uint8Array): void {
    try {
      this.#handleFrame(bytes);
    } catch (e) {
      if (e instanceof SwitchboardError) {
        this.#fail(e.status);
      } else {
        this.#log('error', `${this}: internal error while handling a frame`, e);
        this.#fail(Status.of(StatusCode.internal, 'internal error'));
      }
    }
  }

  #handleFrame(bytes: Uint8Array): void {
    const maxFrame = this.options.maxFrameSize;
    if (maxFrame > 0 && bytes.length > maxFrame) {
      throw SwitchboardError.of(
        StatusCode.frameTooLarge,
        `frame of ${bytes.length} bytes exceeds limit of ${maxFrame}`,
      );
    }
    const frame = MuxFrame.decode(bytes);
    if (frame.longForm && this.options.shortIdsOnly) {
      throw SwitchboardError.of(StatusCode.unsupported, 'long channel ids are not supported');
    }
    const id = frame.channelId;
    if (
      !this.#limitsKnown &&
      (id !== MuxFrame.controlChannelId ||
        frame.payload.length === 0 ||
        frame.payload[0] !== MuxControlType.limits)
    ) {
      // A peer that announces sends LIMITS first: this one does not.
      this.#endLimitsWait();
    }
    if (id === MuxFrame.controlChannelId) {
      this.#handleControl(frame.payload);
      return;
    }
    switch (frame.command) {
      case MuxCommand.open:
        this.#handleOpen(id, frame.payload);
        return;
      case MuxCommand.data: {
        const link = this.#links.get(id);
        if (link?.openWritten && !link.closeReceived) {
          link.receiveData(frame.payload);
          return;
        }
        const window = link === undefined ? this.#awaitingClose.get(id) : undefined;
        if (window === undefined) {
          throw new ProtocolError(`DATA on channel ${id} which is not open`);
        }
        // In flight before the peer saw this side's CLOSE: dropped, but
        // still charged against the credit it had when the channel ended.
        const cost = MuxCredit.costOf(frame.payload.length);
        if (cost > window) {
          throw new ProtocolError(
            `DATA costing ${cost} bytes on closing channel ${id}, beyond the window of ` +
              `${window} granted`,
          );
        }
        this.#awaitingClose.set(id, window - cost);
        return;
      }
      case MuxCommand.close: {
        const status = statusFromWire(decodeStatusPayload(frame.payload, 'CLOSE'));
        const link = this.#links.get(id);
        if (link?.openWritten && !link.closeReceived) {
          link.receiveClose(status);
        } else if (link === undefined && this.#awaitingClose.delete(id)) {
          // The confirmation of a CLOSE sent without a channel: the id is
          // mutually closed and free again. A channel whose close
          // confirmation timed out may still have DATA queued, and the
          // CLOSE behind it, if the peer's CLOSE crossed this side's.
          this.#scheduler.dropQueuedData(id);
          if (this.#isOwnShortId(id)) {
            this.#shortIdsInUse--;
          }
        } else {
          throw new ProtocolError(`CLOSE on channel ${id} which is not open`);
        }
        return;
      }
    }
  }

  #handleOpen(id: number, payload: Uint8Array): void {
    // The peer allocates odd ids if this side is the initiator, even otherwise.
    if ((id % 2 === 1) !== this.isInitiator) {
      throw new ProtocolError(`OPEN on channel ${id} with the wrong parity`);
    }
    if (this.#links.has(id) || this.#awaitingClose.has(id)) {
      throw new ProtocolError(`OPEN on channel ${id} which is in use`);
    }
    let rejection: Status | undefined;
    const payloadBudget = this.#openPayloadBudget;
    const maxChannels = this.options.maxChannels;
    if (this.#goAwaySent && !this.#continuesOpenChannel(payload)) {
      rejection = Status.of(StatusCode.goingAway);
    } else if (this.#incomingCancelled) {
      rejection = Status.of(StatusCode.unavailable, 'not accepting channels');
    } else if (
      maxChannels > 0 &&
      (this.#openCount >= maxChannels || this.#undelivered.length >= maxChannels)
    ) {
      rejection = Status.of(StatusCode.resourceExhausted, `at most ${maxChannels} channels`);
    } else if (
      payloadBudget !== undefined &&
      this.#openPayloadBytes + payload.length > payloadBudget
    ) {
      rejection = Status.of(
        StatusCode.resourceExhausted,
        `open payloads exceed ${payloadBudget} bytes`,
      );
    }
    if (rejection !== undefined) {
      this.#reject(id, rejection);
      return;
    }
    const local = this.#localInitialWindow;
    const assumed = MuxLimits.defaultInitialWindow;
    const link = new MuxChannelLink(this.#host, this, {
      id,
      isLocallyOpened: false,
      openPayload: payload.slice(),
      sendWindow: this.#peerInitialWindow,
      // The peer may have opened it before this side's LIMITS reached it,
      // with the 64 KiB it assumes until then: accept the larger of the
      // two, and return credit against the smaller, so that a sender
      // holding either window never waits for credit this side would not
      // send.
      receiveWindow: local > assumed ? local : assumed,
      creditWindow: local < assumed ? local : assumed,
    });
    this.#links.set(id, link);
    this.#openCount++;
    this.#openPayloadBytes += payload.length;
    this.#noteBuffered(payload.length);
    if (this.#adopt(link.channel)) {
      return;
    }
    link.incomingPending = true;
    this.#undelivered.push(link);
    this.#deliverIncoming();
  }

  /** Whether a filter of {@link adoptIncoming} took [channel]. */
  #adopt(channel: MuxChannel): boolean {
    for (const filter of [...this.#adopters]) {
      try {
        if (filter(channel)) {
          return true;
        }
      } catch (error) {
        this.#log('error', `${this}: an incoming channel filter failed`, error);
      }
    }
    return false;
  }

  /** Whether a rule of {@link admitAfterGoAway} admits [openPayload]. */
  #continuesOpenChannel(openPayload: Uint8Array): boolean {
    for (const predicate of [...this.#admitters]) {
      try {
        if (predicate(openPayload)) {
          return true;
        }
      } catch {
        // A payload a rule cannot read is not a continuation.
      }
    }
    return false;
  }

  /**
   * `MuxOptions.maxOpenPayloadBytes`, or half of
   * `MuxOptions.receiveHighWaterMarkBytes` if that is smaller; undefined
   * for none.
   */
  get #openPayloadBudget(): number | undefined {
    const budget = this.options.maxOpenPayloadBytes;
    const mark = this.options.receiveHighWaterMarkBytes;
    const half = mark > 0 ? Math.floor(mark / 2) : undefined;
    if (budget <= 0) {
      return half;
    }
    return half !== undefined && half < budget ? half : budget;
  }

  /** Releases the accounting of the OPEN payload of a peer's channel, once it is closed and no longer awaiting delivery. */
  #releaseOpenPayload(link: MuxChannelLink): void {
    if (link.channel.isLocallyOpened || this.#closing || link.incomingPending) {
      return;
    }
    const size = link.channel.openPayload.length;
    this.#openPayloadBytes -= size;
    this.#noteBuffered(-size);
  }

  /** Rejects a peer OPEN with CLOSE carrying [status], remembering its id and receive window until the peer confirms. */
  #reject(id: number, status: Status): void {
    const cap = this.options.maxPendingRejections;
    if (cap > 0 && this.#awaitingClose.size >= cap) {
      throw SwitchboardError.of(
        StatusCode.resourceExhausted,
        `peer left ${cap} rejected channels unconfirmed`,
      );
    }
    this.#log('debug', `${this}: rejecting channel ${id}: ${status}`);
    const local = this.#localInitialWindow;
    const assumed = MuxLimits.defaultInitialWindow;
    this.#awaitingClose.set(id, local > assumed ? local : assumed);
    this.#sendFrame(
      MuxFrame.close(id, fitStatus(status, MuxFrame.headerSizeFor(id), this.#peerMaxFrameSize)),
    );
  }

  #handleControl(payload: Uint8Array): void {
    const message = MuxControlMessage.decode(payload);
    switch (message.knownType) {
      case MuxControlType.ping:
        this.#sendFrame(MuxControlMessage.pong(message.payload).toFrame());
        return;
      case MuxControlType.pong:
        this.#completePing(message.payload);
        return;
      case MuxControlType.goAway: {
        const status = statusFromWire(message.goAwayStatus);
        if (this.#peerGoAway !== undefined) {
          return;
        }
        this.#log('debug', `${this}: peer sent GOAWAY ${status}`);
        this.#peerGoAwayReceived = status;
        this.#peerGoAway =
          status.known === StatusCode.goingAway
            ? status
            : Status.of(StatusCode.goingAway, status.toString());
        this.#peerGoAwayStatus.resolve(status);
        queueMicrotask(() => this.#checkIdle());
        return;
      }
      case MuxControlType.limits:
        this.#peerLimits = MuxLimits.decode(message.payload);
        this.#log('debug', `${this}: peer limits ${this.#peerLimits}`);
        this.#endLimitsWait();
        return;
      case MuxControlType.nonce:
        this.#onPeerNonce(message.payload);
        return;
      case MuxControlType.ident:
        this.#onIdent(message.payload);
        return;
      case MuxControlType.credit:
        this.#onCredit(MuxCredit.decode(message.payload));
        return;
      case undefined:
        this.#log('debug', `${this}: ignoring control type ${message.type}`);
        return;
    }
  }

  /**
   * Credit from the peer: ignored for a channel that is not open here (it
   * raced a CLOSE), and for an id whose OPEN this side has not written yet
   * (`openAfter`), which the peer cannot know.
   */
  #onCredit(credit: MuxCredit): void {
    const link = this.#links.get(credit.channelId);
    if (link?.openWritten) {
      link.receiveCredit(credit.bytes);
    }
  }

  // Identity ------------------------------------------------------------------

  /** Sends this side's NONCE (once per connection) and returns it. */
  #sendNonce(): Uint8Array {
    const nonce = secureRandomBytes(MuxIdent.nonceLength);
    this.#localNonce = nonce;
    this.#sendFrame(MuxControlMessage.nonce(nonce).toFrame());
    return nonce;
  }

  #onPeerNonce(payload: Uint8Array): void {
    if (payload.length !== MuxIdent.nonceLength) {
      throw new ProtocolError(`NONCE of ${payload.length} bytes`);
    }
    if (this.#peerNonce !== undefined) {
      throw new ProtocolError('second NONCE');
    }
    const local = this.#localNonce;
    if (local !== undefined && bytesEqual(local, payload)) {
      // Only a peer that echoes this side's sends it: with both nonces
      // equal, the proof of this side's IDENT verifies as one of the
      // peer's, which could then send it back and be identified as this
      // node.
      throw new ProtocolError('NONCE equal to ours');
    }
    const nonce = payload.slice();
    this.#peerNonce = nonce;
    const requested = this.#localNonce === undefined;
    if (requested) {
      this.#sendNonce();
    }
    const waiter = this.#peerNonceWaiter;
    this.#peerNonceWaiter = undefined;
    waiter?.resolve(nonce);
    if (requested) {
      this.#identityRequested.resolve();
    }
  }

  #onIdent(payload: Uint8Array): void {
    const localNonce = this.#localNonce;
    const peerNonce = this.#peerNonce;
    if (localNonce === undefined || peerNonce === undefined) {
      throw new ProtocolError('IDENT before both nonces were exchanged');
    }
    // The envelope rules hold with or without a verifier, so that a
    // malformed IDENT does not tell whether this side checks identity.
    const ident = MuxIdent.decode(payload);
    const verifier = this.options.identityVerifier;
    if (verifier === undefined) {
      this.#log('debug', `${this}: ignoring IDENT, no identity verifier`);
      return;
    }
    // Frames after the IDENT wait for its verification, so that they see
    // the identity it establishes (or never run if it fails).
    this.#verifyingIdent = true;
    void this.#verifyIdent(verifier, ident, peerNonce, localNonce);
  }

  /**
   * Verifies [ident], sent by the peer with [senderNonce] (the peer's) and
   * [receiverNonce] (this side's); then sets the identity or ends the
   * connection with GOAWAY UNAUTHENTICATED, and handles the frames held
   * meanwhile. Never rejects. An IDENT naming another receiver is refused
   * before anything is awaited.
   */
  async #verifyIdent(
    verifier: CredentialVerifier,
    ident: MuxIdent,
    senderNonce: Uint8Array,
    receiverNonce: Uint8Array,
  ): Promise<void> {
    let identity: PeerIdentity | undefined;
    // Refused for its receiver at once: nothing else is checked.
    let failure =
      identReceiverProblem(ident.receiver, {
        localIdentity: this.localIdentity,
        requireNamedIdent: this.options.requireNamedIdent,
      }) ?? '';
    if (failure.length === 0) {
      try {
        identity = await verifyIdent(ident, {
          verifier,
          senderNonce,
          receiverNonce,
          localIdentity: this.localIdentity,
          now: this.options.wallTime?.(),
          requireNamedIdent: this.options.requireNamedIdent,
        });
      } catch (e) {
        if (e instanceof SwitchboardError) {
          failure = e.status.reason;
        } else {
          this.#log('warn', `${this}: IDENT verification failed`, e);
          failure = 'verification failed';
        }
      }
    }
    this.#verifyingIdent = false;
    if (this.#closing) {
      this.#heldFrames.clear();
      return;
    }
    if (identity === undefined) {
      this.#log('info', `${this}: IDENT refused: ${failure}`);
      this.#heldFrames.clear();
      this.#fail(unauthenticated);
      return;
    }
    this.#log('debug', `${this}: peer identified as ${identity}`);
    this.#peerIdentity = identity;
    this.#peerIdentified.resolve(identity);
    this.#signalIdentity();
    while (this.#heldFrames.length > 0 && !this.#verifyingIdent && !this.#closing) {
      this.#processFrame(this.#heldFrames.shift() as Uint8Array);
    }
    this.#resumeReading();
  }

  #signalIdentity(): void {
    const event = this.#identityEvent;
    this.#identityEvent = undefined;
    event?.resolve();
  }

  #completePing(payload: Uint8Array): void {
    for (let i = 0; i < this.#pings.length; i++) {
      const pending = this.#pings[i] as PendingPing;
      if (bytesEqual(pending.payload, payload)) {
        this.#pings.splice(i, 1);
        pending.result.resolve(this.#clock.now() - pending.sent);
        return;
      }
    }
    // Unsolicited PONGs (answers to keep-alive PINGs among them) are ignored.
  }

  #onTransportError(error: unknown): void {
    if (this.#closing) {
      return;
    }
    if (
      error instanceof SwitchboardError &&
      (error.code === StatusCode.protocolError ||
        error.code === StatusCode.frameTooLarge ||
        error.code === StatusCode.unsupported)
    ) {
      this.#fail(error.status);
      return;
    }
    this.#log('debug', `${this}: transport failed: ${error}`);
    void this.#shutdown(this.#lostStatus(), undefined);
  }

  #onTransportDone(): void {
    if (this.#closing) {
      return;
    }
    this.#log('debug', `${this}: transport closed by peer`);
    this.#writable = false;
    void this.#shutdown(this.#lostStatus(), undefined);
  }

  // Incoming channel delivery -------------------------------------------------

  #iterateIncoming(): AsyncIterator<MuxChannel> {
    if (this.#incomingTaken) {
      throw new Error('incoming channels have one consumer');
    }
    this.#incomingTaken = true;
    return {
      next: () => this.#nextIncoming(),
      return: () => {
        this.#onIncomingCancel();
        return Promise.resolve({ value: undefined, done: true });
      },
    };
  }

  #nextIncoming(): Promise<IteratorResult<MuxChannel>> {
    if (this.#incomingCancelled) {
      return Promise.resolve({ value: undefined, done: true });
    }
    const link = this.#takeUndelivered();
    if (link !== undefined) {
      return Promise.resolve({ value: link.channel, done: false });
    }
    if (this.#incomingEndRequested) {
      return Promise.resolve({ value: undefined, done: true });
    }
    if (this.#incomingWaiter !== undefined) {
      return Promise.reject(new Error('incoming channels have one consumer'));
    }
    this.#incomingWaiter = new Deferred<IteratorResult<MuxChannel>>();
    return this.#incomingWaiter.promise;
  }

  /** The next undelivered channel, handed over. */
  #takeUndelivered(): MuxChannelLink | undefined {
    const link = this.#undelivered.shift();
    if (link !== undefined) {
      link.incomingPending = false;
      if (link.channel.state === 'closed') {
        this.#releaseOpenPayload(link);
      }
    }
    return link;
  }

  /** Hands the next undelivered channel, or the end, to a waiting consumer. */
  #deliverIncoming(): void {
    const waiter = this.#incomingWaiter;
    if (waiter === undefined) {
      return;
    }
    const link = this.#takeUndelivered();
    if (link !== undefined) {
      this.#incomingWaiter = undefined;
      waiter.resolve({ value: link.channel, done: false });
    } else if (this.#incomingEndRequested || this.#incomingCancelled) {
      this.#incomingWaiter = undefined;
      waiter.resolve({ value: undefined, done: true });
    }
  }

  #onIncomingCancel(): void {
    if (this.#incomingCancelled) {
      return;
    }
    this.#incomingCancelled = true;
    const links = [...this.#undelivered];
    this.#undelivered.clear();
    if (links.length > 0) {
      this.#log(
        'debug',
        `${this}: incoming cancelled, closing ${links.length} undelivered channels`,
      );
    }
    const status = Status.of(StatusCode.unavailable, 'not accepting channels');
    for (const link of links) {
      link.incomingPending = false;
      if (link.channel.state === 'closed') {
        this.#releaseOpenPayload(link);
      }
      link.closeUndelivered(status);
    }
    this.#deliverIncoming();
  }

  // Channel bookkeeping -------------------------------------------------------

  #release(link: MuxChannelLink): void {
    if (this.#links.get(link.id) !== link) {
      return;
    }
    this.#links.delete(link.id);
    this.#openCount--;
    this.#releaseOpenPayload(link);
    if (this.#isOwnShortId(link.id)) {
      this.#shortIdsInUse--;
    }
    // Deferred so that the channel finishes sending its confirming CLOSE
    // before an idle connection is closed.
    queueMicrotask(() => this.#checkIdle());
  }

  #abandon(link: MuxChannelLink): void {
    if (this.#closing || this.#links.get(link.id) !== link) {
      return;
    }
    this.#log(
      'debug',
      `${this}: channel ${link.id}: no CLOSE confirmation within ` +
        `${this.options.closeConfirmTimeout} ms`,
    );
    this.#links.delete(link.id);
    this.#openCount--;
    this.#releaseOpenPayload(link);
    this.#awaitingClose.set(link.id, link.channel.receiveWindow);
    link.abandoned();
    queueMicrotask(() => this.#checkIdle());
    const cap = this.options.maxPendingRejections;
    if (cap > 0 && this.#awaitingClose.size > cap) {
      this.#fail(
        Status.of(StatusCode.resourceExhausted, `peer left more than ${cap} CLOSEs unconfirmed`),
      );
    }
  }

  #checkIdle(): void {
    if (this.#closing || this.#openCount > 0) {
      return;
    }
    const idle = this.#idle;
    this.#idle = undefined;
    idle?.resolve();
    if (this.#peerGoAway !== undefined) {
      this.#log('debug', `${this}: idle after peer GOAWAY, closing`);
      void this.close();
    }
  }

  /** Tracks bytes buffered in channel queues; pauses reading above the high-water mark and resumes at half of it. */
  #noteBuffered(delta: number): void {
    this.#bufferedBytes += delta;
    const mark = this.options.receiveHighWaterMarkBytes;
    if (mark <= 0 || this.#closing) {
      return;
    }
    if (!this.#receivePaused) {
      if (this.#bufferedBytes > mark) {
        this.#receivePaused = true;
        this.#log('debug', `${this}: ${this.#bufferedBytes} bytes buffered, pausing input`);
      }
    } else if (this.#bufferedBytes <= Math.floor(mark / 2)) {
      this.#receivePaused = false;
      this.#log('debug', `${this}: buffers drained, resuming input`);
      // The silence while paused was this side's own doing.
      this.#lastReceive = this.#clock.now();
      this.#resumeReading();
    }
  }

  // Keep-alive ----------------------------------------------------------------

  #startKeepAlive(): void {
    const interval = this.options.keepAliveInterval;
    if (interval === undefined || this.#closing) {
      return;
    }
    this.#keepAliveTimer = this.#clock.setTimeout(() => this.#onKeepAliveTimer(), interval);
  }

  #noteReceive(): void {
    this.#lastReceive = this.#clock.now();
    if (this.#keepAliveProbing) {
      this.#keepAliveProbing = false;
      if (this.#keepAliveTimer !== undefined) {
        this.#clock.clearTimeout(this.#keepAliveTimer);
        this.#keepAliveTimer = undefined;
      }
      this.#startKeepAlive();
    }
  }

  #onKeepAliveTimer(): void {
    this.#keepAliveTimer = undefined;
    const interval = this.options.keepAliveInterval;
    if (this.#closing || interval === undefined) {
      return;
    }
    if (this.#receivePaused || this.#verifyingIdent || this.#outputProgressed()) {
      // This side is not reading, so silence says nothing about the peer;
      // or the transport stopped reading because of this side's output,
      // which the peer is still consuming.
      this.#lastReceive = this.#clock.now();
      this.#keepAliveProbing = false;
      this.#keepAliveTimer = this.#clock.setTimeout(() => this.#onKeepAliveTimer(), interval);
      return;
    }
    if (this.#keepAliveProbing) {
      this.#log('info', `${this}: no answer to keep-alive PING, closing`);
      void this.#shutdown(this.#lostStatus('keep-alive timeout'), undefined);
      return;
    }
    const silence = this.#clock.now() - this.#lastReceive;
    if (silence < interval) {
      this.#keepAliveTimer = this.#clock.setTimeout(
        () => this.#onKeepAliveTimer(),
        interval - silence,
      );
      return;
    }
    this.#keepAliveProbing = true;
    this.#keepAliveTimer = this.#clock.setTimeout(
      () => this.#onKeepAliveTimer(),
      this.options.keepAliveTimeout,
    );
    this.#sendFrame(MuxControlMessage.ping(this.#nextPingPayload()).toFrame());
  }

  /**
   * Whether this side's output is backed up (the transport throttles its
   * input because of it, or the scheduler holds frames the transport has
   * not taken) and the peer has read some of it since the last check.
   */
  #outputProgressed(): boolean {
    const accepted = this.#transport.acceptedOutputBytes;
    if (accepted === undefined) {
      return false;
    }
    const progressed =
      ((this.#transport.isInputThrottled ?? false) || this.#scheduler.heldFrames > 0) &&
      accepted !== this.#lastAcceptedOutput;
    this.#lastAcceptedOutput = accepted;
    return progressed;
  }

  // Shutdown ------------------------------------------------------------------

  /**
   * The status of the channels (and the connection) when the transport
   * ends without this side ending it for a protocol error: the peer's
   * GOAWAY status, else this side's graceful GOAWAY status, else
   * CONNECTION_LOST.
   */
  #lostStatus(reason = ''): Status {
    return this.#peerGoAway ?? this.#ownGoAway ?? Status.of(StatusCode.connectionLost, reason);
  }

  /** Ends the connection because the peer broke the protocol: GOAWAY with [status], then close. */
  #fail(status: Status): void {
    if (this.#closing) {
      return;
    }
    this.#log('warn', `${this}: closing with ${status}`);
    this.#sendGoAway(status);
    void this.#shutdown(status, this.#lostStatus());
  }

  /** Ends the connection with [status]; channels end with [channelStatus] (default [status]). Idempotent; never rejects. */
  #shutdown(status: Status, channelStatus: Status | undefined): Promise<void> {
    this.#closeStatus ??= status;
    this.#closeFuture ??= this.#doShutdown(status, channelStatus ?? status);
    return this.#closeFuture;
  }

  async #doShutdown(status: Status, channelStatus: Status): Promise<void> {
    this.#closing = true;
    this.#scheduler.flushHeld();
    this.#finish(status, channelStatus);
    this.#writable = false;
    let timer: TimerHandle | undefined;
    const timedOut = new Promise<boolean>((resolve) => {
      timer = this.#clock.setTimeout(() => resolve(true), this.options.keepAliveTimeout);
    });
    const closed = Promise.all([
      this.#reader.cancel().catch((e: unknown) => {
        this.#log('debug', `${this}: transport close: ${e}`);
      }),
      // What was written before goes first, the backlog included.
      this.#backlogHandedOver()
        .then(() => this.#writer.close())
        .catch((e: unknown) => {
          this.#log('debug', `${this}: transport close: ${e}`);
        }),
    ]).then(() => false);
    const late = await Promise.race([closed, timedOut]);
    if (timer !== undefined) {
      this.#clock.clearTimeout(timer);
    }
    if (late) {
      const abort = this.#transport.abort;
      if (abort !== undefined) {
        this.#log('debug', `${this}: transport not closed in time, aborting it`);
        try {
          abort.call(this.#transport);
        } catch (e) {
          this.#log('debug', `${this}: transport abort: ${e}`);
        }
      } else {
        this.#log('debug', `${this}: transport not closed in time`);
      }
    }
  }

  #finish(status: Status, channelStatus: Status): void {
    this.#scheduler.clear();
    if (this.#keepAliveTimer !== undefined) {
      this.#clock.clearTimeout(this.#keepAliveTimer);
      this.#keepAliveTimer = undefined;
    }
    if (this.#limitsTimer !== undefined) {
      this.#clock.clearTimeout(this.#limitsTimer);
      this.#limitsTimer = undefined;
    }
    this.#awaitingLimits.length = 0;
    const links = [...this.#links.values()];
    this.#links.clear();
    this.#awaitingClose.clear();
    this.#shortIdsInUse = 0;
    this.#bufferedBytes -= this.#openPayloadBytes;
    this.#openPayloadBytes = 0;
    this.#openCount = 0;
    for (const link of links) {
      try {
        link.connectionLost(channelStatus);
      } catch (e) {
        this.#log('warn', `${this}: error ending channel ${link.id}`, e);
      }
    }
    const pings = this.#pings.splice(0);
    for (const pending of pings) {
      pending.result.reject(new SwitchboardError(status));
    }
    this.#heldFrames.clear();
    const nonceWaiter = this.#peerNonceWaiter;
    this.#peerNonceWaiter = undefined;
    nonceWaiter?.reject(new SwitchboardError(status));
    this.#signalIdentity();
    const idle = this.#idle;
    this.#idle = undefined;
    idle?.resolve();
    this.#incomingEndRequested = true;
    this.#deliverIncoming();
    this.#resumeReading();
    this.#done.resolve(status);
  }

  #log(level: LogLevel, message: string, error?: unknown): void {
    this.options.logger.log(level, loggerName, message, error);
  }
}
