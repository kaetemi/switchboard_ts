/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { bytesEqual, concatBytes, utf8Encode } from '../bytes.ts';
import type { Credential, PeerIdentity } from '../identity/contract.ts';
import { secureRandomBytes } from '../identity/secure-random.ts';
import { BindingConnection } from '../mux/binding-connection.ts';
import {
  type IdentifyOptions,
  type MuxChannel,
  type MuxConnection,
  type MuxOptions,
  muxOptions,
} from '../mux/contract.ts';
import { MuxControlMessage, MuxControlType, MuxFrame } from '../mux/mux-frame.ts';
import {
  checkMuxStatus,
  MuxIdent,
  maxControlPayloadLength,
  statusForWire,
  statusFromWire,
  truncateStatus,
  verifyIdent,
} from '../mux/mux-protocol.ts';
import { ProtocolError, Status, StatusCode, SwitchboardError } from '../status.ts';
import type { TimerHandle } from '../util/clock.ts';
import { Deferred } from '../util/deferred.ts';
import type { LogLevel } from '../util/logger.ts';
import { attempt, delay, isSessionEnd, quicLoggerName, withTimeout } from './binding-util.ts';
import {
  QuicError,
  type QuicReceiveStream,
  type QuicSession,
  type QuicSessionEnd,
  type QuicStream,
  QuicStreamReset,
  QuicStreamStopped,
  rawQuicExporterLabel,
  switchboardAlpn,
  webTransportExporterLabel,
} from './contract.ts';
import { quicExporterLength } from './exporter.ts';
import { QuicHello, quicHelloType } from './hello.ts';
import {
  QuicBindingChannel,
  type QuicChannelControl,
  type QuicChannelEntry,
  type QuicChannelHost,
  quicControlPriority,
  quicOrdinaryPriority,
} from './quic-binding-channel.ts';
import { type QuicBindingOptions, quicBindingOptions } from './quic-options.ts';
import { RecordReader, RecordTooLarge } from './record-reader.ts';
import {
  decodeOpenBody,
  encodeCloseRecord,
  encodeDataRecord,
  encodeOpenRecord,
  type QuicRecord,
  QuicRecordCommand,
  quicControlChannelId,
  quicDefaultRecordLimit,
  quicErrorCode,
  quicMaxChannelId,
  quicMaxRecordLength,
  quicRecordCommandName,
  quicRecordHeaderSize,
  statusOfQuicErrorCode,
} from './records.ts';

/** How a connection of the binding is started; see {@link QuicBindingConnection.start}. */
export interface QuicBindingStartOptions {
  /** The connection's options, completed with the defaults. */
  readonly options?: Partial<MuxOptions> | undefined;
  /** The binding's own options, completed with the defaults. */
  readonly quicOptions?: Partial<QuicBindingOptions> | undefined;
  /** The session is a WebTransport session rather than a raw QUIC connection. */
  readonly isWebTransport?: boolean | undefined;
  /** This side's identity, set before anything from the peer is handled. */
  readonly localIdentity?: string | undefined;
}

interface PendingPing {
  readonly payload: Uint8Array;
  readonly done: Deferred<number>;
  readonly start: number;
  cleanup: (() => void) | undefined;
}

const unauthenticated = Status.of(StatusCode.unauthenticated, 'identification failed');

/** Marks a timed-out wait. */
const timedOut = Symbol('timed out');

/** The reason of a connection close for [status], within 1024 bytes. */
function closeReason(status: Status): string {
  return truncateStatus(status, 2 + maxControlPayloadLength).reason;
}

/**
 * A Switchboard connection over one QUIC connection or WebTransport session
 * ({@link QuicSession}): the connection contract of {@link MuxConnection}
 * carried by native streams, one per channel, as the wiki page "Polyverse
 * Switchboard QUIC and WebTransport Binding" specifies. A port of
 * `QuicBindingConnection` of `quic_binding_connection.dart`.
 *
 * - Channels ({@link QuicBindingChannel}): a bidirectional stream each, the
 *   opener's direction starting with an OPEN record (`u48` channel id, then
 *   the open payload); subframes travel as DATA records; each side ends its
 *   direction with a CLOSE record and FIN, or a reset carrying the status.
 *   Channel ids follow the mux's parity and reserved ids, are allocated in
 *   increasing order and never reused.
 * - The control stream: the initiator's stream whose OPEN record names
 *   channel 0, opened before any channel, on which both sides write their
 *   control messages, each one a DATA record: `HELLO` first (the record
 *   limit, the channels this side accepts, the `EXPORTER` flag), then
 *   `PING`, `PONG`, `GOAWAY`, `NONCE` and `IDENT`; `LIMITS` and `CREDIT`
 *   are protocol errors, QUIC's flow control and stream limits taking their
 *   place.
 * - Identity: the `NONCE`/`IDENT` exchange, the proof bound to the TLS
 *   exporter when both `HELLO`s carry `EXPORTER`, and the rules of
 *   `QuicIdentityRules`.
 * - The connection ends with an application close carrying the status (0
 *   after a graceful drain); a received one counts as a `GOAWAY` with its
 *   status, any other end of the session is a lost connection.
 *
 * `MuxOptions` apply as their documentation says for a stream binding;
 * {@link QuicBindingOptions} holds this binding's own. Start one with
 * {@link start}; `connectQuic` and `listenQuic` do.
 */
export class QuicBindingConnection extends BindingConnection implements MuxConnection {
  /**
   * How long a reset or `STOP_SENDING` without an application code on the
   * control stream of a WebTransport session waits for the session's end
   * before it counts as a protocol error, in milliseconds.
   */
  static readonly webTransportSessionGoneGrace = 2000;

  /** PINGs awaiting their PONG; one more fails the oldest. */
  static readonly maxPendingPings = 1024;

  /** The QUIC connection or WebTransport session carrying this connection. */
  readonly session: QuicSession;

  /** Whether {@link session} is a WebTransport session: it decides how the exporter is asked for. */
  readonly isWebTransport: boolean;

  readonly options: MuxOptions;

  /** This binding's own options. */
  readonly quicOptions: QuicBindingOptions;

  readonly #exporter: Uint8Array | undefined;
  readonly #host: QuicChannelHost;

  // Channels.
  readonly #channels = new Map<number, QuicChannelEntry>();
  #undelivered: QuicBindingChannel[] = [];
  readonly #payloads = new Set<QuicBindingChannel>();
  #openPayloadBytes = 0;
  #nextId: number;
  #incomingTaken = false;
  #incomingCancelled = false;
  #incomingWaiter: Deferred<IteratorResult<MuxChannel>> | undefined;
  readonly #adopters: ((channel: MuxChannel) => boolean)[] = [];
  readonly #admitters: ((openPayload: Uint8Array) => boolean)[] = [];
  #streams: AsyncIterator<QuicStream> | undefined;
  readonly #pendingStreams = new Set<QuicStream>();
  #unidirectional: AsyncIterator<QuicReceiveStream> | undefined;

  // The control stream.
  #control: QuicStream | undefined;
  #controlReader: RecordReader | undefined;
  readonly #controlReady = new Deferred<void>();
  readonly #ready = new Deferred<void>();
  #controlQueue: Uint8Array[] = [];
  #controlQueued = 0;
  #controlWriting = false;
  #controlDrained: Deferred<void> | undefined;
  #controlTimer: TimerHandle | undefined;
  #sessionGoneTimer: TimerHandle | undefined;
  #peerHello: QuicHello | undefined;

  // The connection.
  readonly #done = new Deferred<Status>();
  readonly #peerGoAwayStatus = new Deferred<Status>();
  #pings: PendingPing[] = [];
  #pingCounter = 0;
  #closing = false;
  #goAwaySent = false;
  #ownGoAway: Status | undefined;
  #peerGoAway: Status | undefined;
  #peerGoAwayReceived: Status | undefined;
  #closeStatus: Status | undefined;
  #closeFuture: Promise<void> | undefined;
  #idle: Deferred<void> | undefined;
  readonly #finishing = new Set<Promise<void>>();

  // Keep-alive.
  #keepAliveTimer: TimerHandle | undefined;
  #lastReceive: number;
  #probing = false;

  // Identity.
  #localNonce: Uint8Array | undefined;
  #peerNonce: Uint8Array | undefined;
  #nonceWaiter: Deferred<Uint8Array> | undefined;
  readonly #identityRequested = new Deferred<void>();
  #peerIdentity: PeerIdentity | undefined;
  #peerIdentityBound = false;
  readonly #peerIdentified = new Deferred<PeerIdentity>();
  #boundIdentityWaiter: Deferred<PeerIdentity> | undefined;
  readonly #mutualIdentification = new Deferred<void>();
  #identityEvent: Deferred<void> | undefined;
  #verifying = false;
  #heldControl: MuxControlMessage[] = [];

  private constructor(
    session: QuicSession,
    options: MuxOptions,
    quicOptions: QuicBindingOptions,
    exporter: Uint8Array | undefined,
    isWebTransport: boolean,
  ) {
    super();
    this.session = session;
    this.options = options;
    this.quicOptions = quicOptions;
    this.#exporter = exporter;
    this.isWebTransport = isWebTransport;
    this.#nextId = session.isInitiator ? 2 : 3;
    this.#lastReceive = options.clock.now();
    const self = this;
    this.#host = {
      session,
      options,
      quicOptions,
      get closing() {
        return self.#closing;
      },
      get peerRecordLimit() {
        return self.peerRecordLimit;
      },
      get recordLimit() {
        return self.recordLimit;
      },
      openGate: () => this.#openGate(),
      openLocal: (openPayload, continuation) => this.#openLocal(openPayload, continuation),
      release: (stream, code) => this.#release(stream, code),
      trackFinishing: (acknowledged) => this.#trackFinishing(acknowledged),
      protocolError: (reason) => this.#protocolError(reason),
      fail: (status) => this.#fail(status),
      noteReceive: () => this.#noteReceive(),
      releaseChannel: (channel) => this.#releaseChannel(channel),
      log: (level, message, error) => this.#log(level, message, error),
    };
  }

  /**
   * Starts the binding over [session], an established QUIC connection or
   * WebTransport session (`isWebTransport`), and resolves with the
   * connection once it can open channels: on the initiator, once the
   * control stream is open, its OPEN record and `HELLO` written first.
   *
   * Asks the session for its exporter first (RFC 9266's
   * `EXPORTER-Channel-Binding` on raw QUIC, the application label
   * `switchboard-ident` over WebTransport), which decides the `EXPORTER`
   * flag of this side's `HELLO`. `localIdentity` is set before anything from
   * the peer is handled.
   *
   * A connection that ends while it starts (the session ended, or the
   * identification rules refused it) is returned ended: its {@link done}
   * has resolved.
   *
   * @throws {RangeError} for a `recordLimit` outside 32752 to 2^30 - 1, or
   * binding limits and windows that are not positive.
   */
  static start(
    session: QuicSession,
    init: QuicBindingStartOptions = {},
  ): Promise<QuicBindingConnection> {
    const options = muxOptions(init.options);
    const quicOptions = quicBindingOptions(init.quicOptions);
    const limit = quicOptions.recordLimit;
    if (!Number.isInteger(limit) || limit < quicDefaultRecordLimit || limit > quicMaxRecordLength) {
      throw new RangeError(
        `quicOptions.recordLimit must be ${quicDefaultRecordLimit} to ${quicMaxRecordLength}, not ${limit}`,
      );
    }
    if (
      !(quicOptions.maxPendingStreams >= 1) ||
      !(quicOptions.controlOutputMark >= 1) ||
      !(quicOptions.writeBatchBytes >= 1) ||
      !(quicOptions.maxHeldControlMessages >= 1) ||
      !(options.initialWindow >= 1) ||
      options.initialWindow > 0xffffffff ||
      !(options.bulkChunkSize >= 1)
    ) {
      throw new RangeError('binding limits and windows must be positive');
    }
    return QuicBindingConnection.#start(
      session,
      options,
      quicOptions,
      init.isWebTransport ?? false,
      init.localIdentity,
    );
  }

  static async #start(
    session: QuicSession,
    options: MuxOptions,
    quicOptions: QuicBindingOptions,
    isWebTransport: boolean,
    localIdentity: string | undefined,
  ): Promise<QuicBindingConnection> {
    let exporter: Uint8Array | undefined;
    let exporterFailure: unknown;
    let exporterFailed = false;
    try {
      const ended = session.done.then((): never => {
        throw new QuicError('sessionEnded', 'session ended during export');
      });
      ended.catch(() => {});
      const exporting = Promise.race([
        attempt(() =>
          session.exportKeyingMaterial(
            isWebTransport ? webTransportExporterLabel : rawQuicExporterLabel,
            new Uint8Array(0),
            quicExporterLength,
          ),
        ),
        ended,
      ]);
      const timeout = quicOptions.controlStreamTimeout ?? options.keepAliveInterval ?? 10_000;
      exporter = await withTimeout(options.clock, exporting, timeout, () => {
        throw new Error(`no exporter within ${timeout} ms`);
      });
    } catch (error) {
      exporterFailure = error;
      exporterFailed = true;
      options.logger.log('debug', quicLoggerName, `session exporter failed: ${error}`);
    }
    if (exporter !== undefined && exporter.length !== quicExporterLength) {
      options.logger.log(
        'warn',
        quicLoggerName,
        `the session exported ${exporter.length} bytes instead of ${quicExporterLength}`,
      );
      exporterFailure = new Error('invalid exporter length');
      exporterFailed = true;
      exporter = undefined;
    }
    const connection = new QuicBindingConnection(
      session,
      options,
      quicOptions,
      exporter?.slice(),
      isWebTransport,
    );
    connection.localIdentity = localIdentity;
    connection.#begin();
    if (exporterFailed && !isSessionEnd(exporterFailure)) {
      connection.#fail(Status.of(StatusCode.internal, 'session exporter failed'));
    }
    await connection.#ready.promise;
    return connection;
  }

  get isInitiator(): boolean {
    return this.session.isInitiator;
  }

  get remote(): string {
    return this.session.remote;
  }

  /**
   * Whether this side can compute the exporter of the connection: the
   * `EXPORTER` flag of its `HELLO`.
   */
  get hasExporter(): boolean {
    return this.#exporter !== undefined;
  }

  /**
   * A copy of the exporter this side derived for the connection (32 bytes),
   * undefined when its stack has none. Keying material: for tests and
   * diagnostics that check both ends of a session derive the same value and
   * two sessions different ones, never for logs.
   */
  get exporterForTesting(): Uint8Array | undefined {
    return this.#exporter?.slice();
  }

  // The contract ------------------------------------------------------------

  /**
   * Channels opened by the peer, in order, buffered until taken. One
   * consumer. Stopping the iteration closes the channels not taken yet
   * with `UNAVAILABLE`, and the ones arriving after.
   */
  get incoming(): AsyncIterable<MuxChannel> {
    return { [Symbol.asyncIterator]: () => this.#iterateIncoming() };
  }

  get done(): Promise<Status> {
    return this.#done.promise;
  }

  get isOpen(): boolean {
    return !this.#closing && !this.#goAwaySent;
  }

  get peerGoingAway(): boolean {
    return this.#peerGoAway !== undefined;
  }

  get peerGoAwayStatus(): Promise<Status> {
    return this.#peerGoAwayStatus.promise;
  }

  /**
   * The channels the peer accepts at once, as its `HELLO` states them; 0
   * before it arrives and when it states no limit. Advisory: the pool of a
   * node opens a further connection once it is reached, and {@link open}
   * beyond it waits for stream credit in the stack, which enforces the
   * limit, instead of being refused.
   */
  override get peerMaxChannels(): number {
    return this.#peerHello?.channels ?? 0;
  }

  /** The peer's `HELLO`, undefined until it arrives. */
  get peerHello(): QuicHello | undefined {
    return this.#peerHello;
  }

  /**
   * The largest record body the peer accepts on a channel's stream: its
   * `HELLO`'s record limit, 32752 before it.
   */
  get peerRecordLimit(): number {
    return this.#peerHello?.recordLimit ?? quicDefaultRecordLimit;
  }

  /** The record limit this side announced. */
  get recordLimit(): number {
    return this.quicOptions.recordLimit;
  }

  /**
   * Whether `IDENT` proofs on this connection are exporter-bound: this side
   * can compute the exporter and the peer's `HELLO` says it can.
   */
  get exporterBound(): boolean {
    return this.#exporter !== undefined && (this.#peerHello?.exporter ?? false);
  }

  /** Whether the peer's current identity was proven with an exporter-bound `IDENT`. */
  get peerIdentityBound(): boolean {
    return this.peerIdentity !== undefined && this.#peerIdentityBound;
  }

  /**
   * Whether the other end of this connection is authenticated: by the TLS
   * handshake (`QuicIdentityRules.tlsAuthenticatesPeer`), or by its
   * exporter-bound `IDENT` ({@link peerIdentityBound}). A credential in a
   * channel's application payload, which carries no proof bound to the
   * connection, goes only to an authenticated end.
   */
  get peerAuthenticated(): boolean {
    return this.quicOptions.identity.tlsAuthenticatesPeer || this.peerIdentityBound;
  }

  get openChannelCount(): number {
    return this.#channels.size;
  }

  override get openPayloadBytes(): number {
    return this.#openPayloadBytes;
  }

  override get heldOutputBytes(): number {
    return this.#closing ? 0 : this.#controlQueued;
  }

  /** What the channels hold for their readers, plus the open payloads held for the peer's channels. */
  override get bufferedBytes(): number {
    return super.bufferedBytes + this.#openPayloadBytes;
  }

  channelWithId(id: number): MuxChannel | undefined {
    return this.#channels.get(id)?.channel;
  }

  get channels(): MuxChannel[] {
    return [...this.#channels.values()].map((entry) => entry.channel);
  }

  get peerIdentity(): PeerIdentity | undefined {
    const identity = this.#peerIdentity;
    return identity === undefined || identity.isExpired() ? undefined : identity;
  }

  get peerIdentified(): Promise<PeerIdentity> {
    return this.#peerIdentified.promise;
  }

  get identityRequested(): Promise<void> {
    return this.#identityRequested.promise;
  }

  get identityChanged(): Promise<void> {
    this.#identityEvent ??= new Deferred<void>();
    return this.#identityEvent.promise;
  }

  adoptIncoming(filter: (channel: MuxChannel) => boolean): () => void {
    this.#adopters.push(filter);
    return () => {
      const i = this.#adopters.indexOf(filter);
      if (i >= 0) {
        this.#adopters.splice(i, 1);
      }
    };
  }

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
   * Sets the next channel id this side allocates, which must be of its
   * parity and above the ids it used: ids are never reused here. For tests
   * of the reserved ids and of the end of the id space.
   *
   * @throws {RangeError} for an id this side cannot use.
   */
  setNextChannelIdForTesting(id: number): void {
    if (
      !Number.isInteger(id) ||
      (id % 2 === 1) === this.isInitiator ||
      id < this.#nextId ||
      id > quicMaxChannelId
    ) {
      throw new RangeError(`${id} is not an unused id of this side`);
    }
    this.#nextId = id;
  }

  #log(level: LogLevel, message: string, error?: unknown): void {
    this.options.logger.log(level, quicLoggerName, message, error);
  }

  /** Runs [action] on the next macrotask of the clock. */
  #defer(action: () => void): void {
    this.options.clock.setTimeout(action, 0);
  }

  // Starting ----------------------------------------------------------------

  #begin(): void {
    const session = this.session;
    session.done.then(
      (end) => this.#onSessionEnd(end),
      (error: unknown) =>
        this.#onSessionEnd({ code: undefined, reason: `${error}`, byPeer: false, lost: true }),
    );
    void this.#acceptStreams();
    session.draining.then(() => {
      if (!this.#closing) {
        this.#onPeerGoAway(Status.of(StatusCode.goingAway));
      }
    });
    void this.#refuseUnidirectional();
    const timeout =
      this.quicOptions.controlStreamTimeout ?? this.options.keepAliveInterval ?? 10_000;
    if (timeout > 0) {
      this.#controlTimer = this.options.clock.setTimeout(() => {
        if (this.#peerHello === undefined && !this.#closing) {
          this.#protocolError(`no control HELLO within ${timeout} ms`);
        }
      }, timeout);
    }
    if (session.protocol !== undefined && session.protocol !== switchboardAlpn) {
      this.#fail(Status.of(StatusCode.unsupported, 'unsupported session protocol'));
      return;
    }
    const rules = this.quicOptions.identity;
    if (this.isInitiator) {
      if (rules.requireExporterBinding && this.#exporter === undefined) {
        this.#log(
          'info',
          `${this}: the stack cannot compute the exporter, which this side requires`,
        );
        this.#fail(unauthenticated);
        return;
      }
      if (rules.awaitPeerIdentity && this.options.identityVerifier === undefined) {
        // The acceptor's IDENT could never verify: nothing would open.
        this.#log(
          'warn',
          `${this}: awaiting the peer's identity needs MuxOptions.identityVerifier`,
        );
        this.#fail(unauthenticated);
        return;
      }
      this.#openControl().catch((error: unknown) => {
        if (!isSessionEnd(error)) {
          this.#lost('control stream setup failed');
        }
      });
    } else {
      this.#ready.resolve();
    }
    this.#startKeepAlive();
  }

  async #acceptStreams(): Promise<void> {
    const streams = this.session.incomingStreams[Symbol.asyncIterator]();
    this.#streams = streams;
    try {
      for (;;) {
        const next = await streams.next();
        if (next.done) {
          return;
        }
        void this.#onIncomingStream(next.value);
      }
    } catch (error) {
      if (!isSessionEnd(error)) {
        this.#lost('incoming streams failed');
      }
    }
  }

  async #refuseUnidirectional(): Promise<void> {
    const streams = this.session.incomingUnidirectionalStreams[Symbol.asyncIterator]();
    this.#unidirectional = streams;
    try {
      for (;;) {
        const next = await streams.next();
        if (next.done) {
          return;
        }
        this.#guarded(() => next.value.stop(StatusCode.unsupported));
      }
    } catch (error) {
      if (!isSessionEnd(error)) {
        this.#lost('incoming unidirectional streams failed');
      }
    }
  }

  /** This side's `HELLO`. */
  #hello(): QuicHello {
    let channels = this.options.maxChannels > 0 ? this.options.maxChannels : 0;
    const streams = this.session.maxIncomingStreams;
    if (streams !== undefined) {
      // The acceptor's stream limit counts the control stream.
      const usable = this.isInitiator ? streams : streams - 1;
      if (usable > 0 && (channels === 0 || usable < channels)) {
        channels = usable;
      }
    }
    return new QuicHello({
      recordLimit: this.quicOptions.recordLimit,
      channels: channels > 0xffffffff ? 0xffffffff : channels,
      exporter: this.#exporter !== undefined,
    });
  }

  /** Initiator: opens the control stream, its OPEN record and `HELLO` in the first flight. */
  async #openControl(): Promise<void> {
    let stream: QuicStream;
    try {
      stream = await this.session.openStream();
    } catch (error) {
      this.#log('debug', `${this}: cannot open the control stream: ${error}`);
      if (!isSessionEnd(error)) {
        this.#lost('cannot open the control stream');
      }
      this.#ready.resolve();
      return;
    }
    if (this.#closing) {
      stream.stop(StatusCode.cancelled);
      stream.reset(StatusCode.cancelled);
      this.#ready.resolve();
      return;
    }
    stream.priority = quicControlPriority;
    const hello = encodeDataRecord(this.#hello().encodeMessage());
    const open = encodeOpenRecord(quicControlChannelId, new Uint8Array(0));
    this.#controlQueue.unshift(open, hello);
    this.#controlQueued += hello.length + open.length;
    this.#attachControl(stream, new RecordReader(stream));
    this.#ready.resolve();
  }

  /** Acceptor: the stream whose OPEN record names channel 0 arrived. */
  #onControlStream(stream: QuicStream, reader: RecordReader): void {
    stream.priority = quicControlPriority;
    const hello = encodeDataRecord(this.#hello().encodeMessage());
    this.#controlQueue.unshift(hello);
    this.#controlQueued += hello.length;
    this.#attachControl(stream, reader);
  }

  #attachControl(stream: QuicStream, reader: RecordReader): void {
    this.#control = stream;
    this.#controlReader = reader;
    stream.resetReceived.then(
      (code) => this.#controlAborted('the control stream was reset', code !== undefined),
      (error: unknown) => {
        if (!isSessionEnd(error) && !this.#closing) {
          this.#lost('control reset notification failed');
        }
      },
    );
    stream.stopped.then(
      (code) => {
        // Never finished, so only a STOP_SENDING resolves it with a code.
        if (code !== undefined && !this.#closing) {
          this.#protocolError('STOP_SENDING on the control stream');
        }
      },
      (error: unknown) => {
        if (error instanceof QuicStreamStopped) {
          this.#controlAborted('STOP_SENDING on the control stream', error.code !== undefined);
        } else if (!isSessionEnd(error) && !this.#closing) {
          this.#lost('control stream acknowledgement failed');
        }
      },
    );
    this.#pumpControl();
    this.#controlReady.resolve();
    void this.#readControl();
  }

  // The control stream -------------------------------------------------------

  /** Queues [message] on the control stream; written once it exists. */
  #sendControl(message: MuxControlMessage): void {
    if (this.#closing) {
      return;
    }
    const record = encodeDataRecord(message.encode());
    // Reading-driven replies may overshoot the soft mark by one record.
    // Local callers (PING/IDENT) must not bypass the bound indefinitely.
    if (
      this.#controlQueued + record.length >
      this.quicOptions.controlOutputMark + 1 + maxControlPayloadLength + quicRecordHeaderSize
    ) {
      this.#fail(Status.of(StatusCode.resourceExhausted, 'control output full'));
      return;
    }
    this.#controlQueue.push(record);
    this.#controlQueued += record.length;
    this.#pumpControl();
  }

  #pumpControl(): void {
    const stream = this.#control;
    if (
      stream === undefined ||
      this.#controlWriting ||
      this.#controlQueue.length === 0 ||
      this.#closing
    ) {
      return;
    }
    this.#controlWriting = true;
    const queue = this.#controlQueue;
    this.#controlQueue = [];
    const bytes = queue.length === 1 ? (queue[0] as Uint8Array) : concatBytes(...queue);
    attempt(() => stream.write(bytes))
      .then(
        () => {
          this.#controlWriting = false;
          this.#controlQueued -= bytes.length;
          if (this.#controlQueued <= this.quicOptions.controlOutputMark) {
            const drained = this.#controlDrained;
            this.#controlDrained = undefined;
            drained?.resolve();
          }
          this.#pumpControl();
        },
        (error: unknown) => {
          this.#controlWriting = false;
          if (error instanceof QuicStreamStopped) {
            this.#controlAborted('STOP_SENDING on the control stream', error.code !== undefined);
          } else if (!isSessionEnd(error)) {
            this.#log('warn', `${this}: control stream write failed: ${error}`);
            this.#lost('control stream write failed');
          }
        },
      )
      .catch((error: unknown) => {
        this.#log('error', `${this}: after a control stream write`, error);
      });
  }

  async #readControl(): Promise<void> {
    const reader = this.#controlReader as RecordReader;
    while (!this.#closing) {
      if (this.#controlQueued > this.quicOptions.controlOutputMark) {
        // Our own control output backs up: a peer that does not read cannot
        // make the answers to what it sends pile up.
        this.#controlDrained ??= new Deferred<void>();
        await this.#controlDrained.promise;
        continue;
      }
      let record: QuicRecord | undefined;
      try {
        record = await reader.next(1 + maxControlPayloadLength);
      } catch (error) {
        if (error instanceof RecordTooLarge) {
          this.#protocolError(`control message of ${error.length} bytes`);
        } else if (error instanceof ProtocolError) {
          this.#fail(error.status);
        } else if (error instanceof QuicStreamReset) {
          this.#controlAborted('the control stream was reset', error.code !== undefined);
        } else if (error instanceof QuicError) {
          if (!isSessionEnd(error)) {
            this.#log('warn', `${this}: reading the control stream: ${error}`);
            this.#lost('the control stream failed');
          }
        } else {
          this.#log('error', `${this}: reading the control stream`, error);
          this.#fail(Status.of(StatusCode.internal, 'internal error'));
        }
        return;
      }
      if (this.#closing) {
        return;
      }
      if (record === undefined) {
        this.#protocolError('FIN on the control stream');
        return;
      }
      this.#noteReceive();
      switch (record.command) {
        case QuicRecordCommand.data: {
          const body = record.body;
          this.#guarded(() => this.#onControlRecord(body));
          break;
        }
        case QuicRecordCommand.open:
          this.#protocolError('a second OPEN record on the control stream');
          return;
        case QuicRecordCommand.close:
          this.#protocolError('a CLOSE record on the control stream');
          return;
      }
    }
  }

  /** Runs [action], ending the connection on a protocol error. */
  #guarded(action: () => void): void {
    try {
      action();
    } catch (error) {
      if (error instanceof SwitchboardError) {
        this.#fail(error.status);
      } else {
        this.#log('error', `${this}: internal error`, error);
        this.#fail(Status.of(StatusCode.internal, 'internal error'));
      }
    }
  }

  #onControlRecord(body: Uint8Array): void {
    if (body.length === 0) {
      throw new ProtocolError('empty control record');
    }
    const message = MuxControlMessage.decode(body);
    if (this.#verifying) {
      // Handled once the IDENT being verified has taken effect.
      if (this.#heldControl.length >= this.quicOptions.maxHeldControlMessages) {
        this.#fail(
          Status.of(
            StatusCode.resourceExhausted,
            'too many control messages during an identification',
          ),
        );
        return;
      }
      this.#heldControl.push(message);
      return;
    }
    this.#handleControl(message);
  }

  #handleControl(message: MuxControlMessage): void {
    if (this.#peerHello === undefined) {
      if (message.type !== quicHelloType) {
        throw new ProtocolError(`control message ${message.type} before HELLO`);
      }
      this.#onHello(QuicHello.decode(message.payload));
      return;
    }
    switch (message.type) {
      case quicHelloType:
        throw new ProtocolError('second HELLO');
      case MuxControlType.ping:
        this.#sendControl(MuxControlMessage.pong(message.payload));
        return;
      case MuxControlType.pong:
        this.#completePing(message.payload);
        return;
      case MuxControlType.goAway:
        this.#onPeerGoAway(statusFromWire(message.goAwayStatus));
        return;
      case MuxControlType.limits:
        throw new ProtocolError('LIMITS over the QUIC binding');
      case MuxControlType.nonce:
        this.#onPeerNonce(message.payload);
        return;
      case MuxControlType.ident:
        this.#onIdent(message.payload);
        return;
      case MuxControlType.credit:
        throw new ProtocolError('CREDIT over the QUIC binding');
      default:
        this.#log('debug', `${this}: ignoring control type ${message.type}`);
    }
  }

  #onHello(hello: QuicHello): void {
    this.#clearControlTimer();
    this.#peerHello = hello;
    this.#log('debug', `${this}: peer ${hello}`);
    const rules = this.quicOptions.identity;
    if (this.isInitiator && rules.requireExporterBinding && !this.exporterBound) {
      this.#log('info', `${this}: the peer's HELLO lacks EXPORTER, which this side requires`);
      this.#fail(unauthenticated);
      return;
    }
    // The largest subframe may have grown.
    for (const entry of [...this.#channels.values()]) {
      entry.control.wakeWritable();
    }
  }

  #clearControlTimer(): void {
    if (this.#controlTimer !== undefined) {
      this.options.clock.clearTimeout(this.#controlTimer);
      this.#controlTimer = undefined;
    }
  }

  #onPeerGoAway(status: Status): void {
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
    this.#defer(() => this.#checkIdle());
  }

  // Incoming streams ----------------------------------------------------------

  async #onIncomingStream(stream: QuicStream): Promise<void> {
    if (this.#pendingStreams.size >= this.quicOptions.maxPendingStreams) {
      this.#release(stream, StatusCode.resourceExhausted);
      return;
    }
    this.#pendingStreams.add(stream);
    try {
      await this.#readOpen(stream);
    } catch (error) {
      this.#log('error', `${this}: reading an OPEN record`, error);
    } finally {
      this.#pendingStreams.delete(stream);
      this.#defer(() => this.#checkIdle());
    }
  }

  async #readOpen(stream: QuicStream): Promise<void> {
    if (this.#closing) {
      this.#release(stream, StatusCode.goingAway);
      return;
    }
    const reader = new RecordReader(stream);
    let first: QuicRecord | undefined;
    try {
      const reading: Promise<QuicRecord | undefined | typeof timedOut> = reader.next(
        this.quicOptions.recordLimit,
      );
      const result = await withTimeout(
        this.options.clock,
        reading,
        this.quicOptions.openStreamTimeout,
        (): typeof timedOut => timedOut,
      );
      if (result === timedOut) {
        this.#release(stream, StatusCode.deadlineExceeded);
        return;
      }
      first = result;
    } catch (error) {
      if (error instanceof QuicStreamReset) {
        // Its opener gave it up before the OPEN record was complete.
        this.#release(stream, StatusCode.cancelled);
      } else if (error instanceof RecordTooLarge) {
        this.#fail(
          Status.of(
            StatusCode.frameTooLarge,
            `OPEN record of ${error.length} bytes, limit ${error.limit}`,
          ),
        );
      } else if (error instanceof ProtocolError) {
        this.#fail(error.status);
      } else if (error instanceof QuicError) {
        if (!isSessionEnd(error)) {
          this.#log('debug', `${this}: a stream failed before its OPEN record: ${error}`);
          this.#release(stream, StatusCode.internal);
        }
      } else {
        this.#log('error', `${this}: reading an OPEN record`, error);
        this.#fail(Status.of(StatusCode.internal, 'internal error'));
      }
      return;
    }
    if (this.#closing) {
      this.#release(stream, StatusCode.goingAway);
      return;
    }
    if (first === undefined) {
      this.#protocolError('FIN before the OPEN record');
      return;
    }
    if (first.command !== QuicRecordCommand.open) {
      this.#protocolError(`a stream whose first record is ${quicRecordCommandName(first.command)}`);
      return;
    }
    this.#noteReceive();
    const body = first.body;
    this.#guarded(() => this.#onOpen(stream, reader, body));
  }

  /** Stops and resets [stream], which carries no channel. */
  #release(stream: QuicStream, code: number): void {
    try {
      stream.stop(code);
      stream.reset(code);
    } catch (error) {
      this.#log('debug', `${this}: releasing a stream: ${error}`);
    }
  }

  #onOpen(stream: QuicStream, reader: RecordReader, body: Uint8Array): void {
    const { id, openPayload } = decodeOpenBody(body);
    if (id === quicControlChannelId) {
      if (this.isInitiator) {
        throw new ProtocolError('channel 0 on a stream the acceptor opened');
      }
      if (this.#control !== undefined) {
        throw new ProtocolError('a second control stream');
      }
      if (openPayload.length > 0) {
        throw new ProtocolError('control OPEN has a payload');
      }
      this.#onControlStream(stream, reader);
      return;
    }
    if (MuxFrame.isReservedId(id)) {
      throw new ProtocolError(`OPEN of the reserved channel ${id}`);
    }
    // The peer allocates odd ids if this side is the initiator.
    if ((id % 2 === 1) !== this.isInitiator) {
      throw new ProtocolError(`OPEN of channel ${id}, of the wrong parity`);
    }
    if (this.#channels.has(id)) {
      throw new ProtocolError(`OPEN of channel ${id}, which is open`);
    }
    const rejection = this.#admission(openPayload);
    if (rejection !== undefined) {
      this.#reject(stream, id, rejection);
      return;
    }
    const entry = this.#makeChannel(id, false, openPayload.slice());
    const channel = entry.channel;
    this.#channels.set(id, entry);
    this.#payloads.add(channel);
    this.#openPayloadBytes += channel.openPayload.length;
    entry.control.attach(stream, reader, true);
    if (!this.#adopt(channel)) {
      this.#undelivered.push(channel);
      this.#deliverIncoming();
    }
  }

  #makeChannel(id: number, isLocallyOpened: boolean, openPayload: Uint8Array): QuicChannelEntry {
    let control: QuicChannelControl | undefined;
    const channel = new QuicBindingChannel(
      this,
      this.#host,
      { id, isLocallyOpened, openPayload },
      (bound) => {
        control = bound;
      },
    );
    return { channel, control: control as QuicChannelControl };
  }

  /** Whether a filter of {@link adoptIncoming} took [channel]. */
  #adopt(channel: QuicBindingChannel): boolean {
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

  /** Why the peer's channel with [openPayload] is refused, or undefined. */
  #admission(openPayload: Uint8Array): Status | undefined {
    if (this.#goAwaySent && !this.#continuesOpenChannel(openPayload)) {
      return Status.of(StatusCode.goingAway);
    }
    if (this.#incomingCancelled) {
      return Status.of(StatusCode.unavailable, 'not accepting channels');
    }
    const max = this.options.maxChannels;
    if (max > 0 && (this.#channels.size >= max || this.#undelivered.length >= max)) {
      return Status.of(StatusCode.resourceExhausted, `at most ${max} channels`);
    }
    const budget = this.#openPayloadBudget;
    if (budget !== undefined && this.#openPayloadBytes + openPayload.length > budget) {
      return Status.of(StatusCode.resourceExhausted, `open payloads exceed ${budget} bytes`);
    }
    return undefined;
  }

  /**
   * `MuxOptions.maxOpenPayloadBytes`, within half of
   * `MuxOptions.receiveHighWaterMarkBytes` as on the frame mux; undefined
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
   * Refuses the peer's channel [id] on [stream] with [status]: a CLOSE
   * record and FIN, and `STOP_SENDING` with the same status.
   */
  #reject(stream: QuicStream, id: number, status: Status): void {
    this.#log('debug', `${this}: rejecting channel ${id}: ${status}`);
    const code = quicErrorCode(status);
    const sending = (async () => {
      stream.priority = quicOrdinaryPriority;
      stream.stop(code);
      await stream.write(encodeCloseRecord(status));
      await stream.finish();
      await stream.stopped;
    })();
    const bounded = withTimeout(
      this.options.clock,
      sending,
      this.options.closeConfirmTimeout,
      () => {
        throw new Error('rejection not acknowledged');
      },
    );
    this.#trackFinishing(bounded.catch(() => this.#release(stream, code)));
  }

  // Delivery of incoming channels ---------------------------------------------

  #iterateIncoming(): AsyncIterator<MuxChannel> {
    if (this.#incomingTaken) {
      throw new Error('incoming channels have one consumer');
    }
    this.#incomingTaken = true;
    return {
      next: () => this.#nextIncoming(),
      return: () => {
        this.#cancelIncoming();
        return Promise.resolve({ value: undefined, done: true });
      },
    };
  }

  #nextIncoming(): Promise<IteratorResult<MuxChannel>> {
    if (this.#incomingCancelled) {
      return Promise.resolve({ value: undefined, done: true });
    }
    const channel = this.#undelivered.shift();
    if (channel !== undefined) {
      if (channel.state === 'closed') {
        this.#releasePayload(channel);
      }
      return Promise.resolve({ value: channel, done: false });
    }
    if (this.#closing) {
      return Promise.resolve({ value: undefined, done: true });
    }
    if (this.#incomingWaiter !== undefined) {
      return Promise.reject(new Error('incoming channels have one consumer'));
    }
    this.#incomingWaiter = new Deferred<IteratorResult<MuxChannel>>();
    return this.#incomingWaiter.promise;
  }

  /** Hands the next undelivered channel, or the end, to a waiting consumer. */
  #deliverIncoming(): void {
    const waiter = this.#incomingWaiter;
    if (waiter === undefined) {
      return;
    }
    const channel = this.#undelivered.shift();
    if (channel !== undefined) {
      this.#incomingWaiter = undefined;
      if (channel.state === 'closed') {
        this.#releasePayload(channel);
      }
      waiter.resolve({ value: channel, done: false });
    } else if (this.#closing || this.#incomingCancelled) {
      this.#incomingWaiter = undefined;
      waiter.resolve({ value: undefined, done: true });
    }
  }

  #cancelIncoming(): void {
    if (this.#incomingCancelled) {
      return;
    }
    this.#incomingCancelled = true;
    const pending = this.#undelivered;
    this.#undelivered = [];
    for (const channel of pending) {
      if (channel.state === 'closed') {
        this.#releasePayload(channel);
      }
      void channel.close(Status.of(StatusCode.unavailable, 'not accepting channels'));
    }
    this.#deliverIncoming();
  }

  #releasePayload(channel: QuicBindingChannel): void {
    if (this.#payloads.delete(channel)) {
      this.#openPayloadBytes -= channel.openPayload.length;
    }
  }

  /** [channel] ended: its id leaves the connection (never to be reused). */
  #releaseChannel(channel: QuicBindingChannel): void {
    if (this.#channels.get(channel.id)?.channel !== channel) {
      return;
    }
    this.#channels.delete(channel.id);
    if (!this.#undelivered.includes(channel)) {
      this.#releasePayload(channel);
    }
    this.#defer(() => this.#checkIdle());
  }

  override drainStateChanged(): void {
    this.#defer(() => this.#checkIdle());
  }

  #checkIdle(): void {
    if (
      this.#closing ||
      this.#channels.size > 0 ||
      this.hasDrainHolds ||
      this.#pendingStreams.size > 0
    ) {
      return;
    }
    const idle = this.#idle;
    this.#idle = undefined;
    idle?.resolve();
    if (this.#peerGoAway !== undefined) {
      this.#log('debug', `${this}: idle after the peer's GOAWAY, closing`);
      void this.close();
    }
  }

  // Opening -------------------------------------------------------------------

  open(openPayload: Uint8Array): MuxChannel {
    const entry = this.#openLocal(openPayload, false);
    entry.control.beginOpen().catch((error: unknown) => {
      this.#log('error', `${this}: opening channel ${entry.channel.id}`, error);
    });
    return entry.channel;
  }

  /**
   * A channel of this side, registered under a new id; its stream is opened
   * by its `beginOpen`, now ({@link open}) or once its parent sent what came
   * before (`openAfter`).
   */
  #openLocal(openPayload: Uint8Array, continuation: boolean): QuicChannelEntry {
    if (this.#closing || (this.#goAwaySent && !continuation)) {
      throw SwitchboardError.of(StatusCode.failedPrecondition, 'connection is closing');
    }
    if (this.#peerGoAway !== undefined && !continuation) {
      throw SwitchboardError.of(StatusCode.failedPrecondition, 'peer is going away');
    }
    // The channels of the peer's HELLO are advisory (the pool reads them,
    // peerMaxChannels): beyond them an open waits for stream credit in the
    // stack, bounded by QuicBindingOptions.openStreamTimeout.
    const size = 6 + openPayload.length;
    if (size > this.peerRecordLimit) {
      throw SwitchboardError.of(
        StatusCode.frameTooLarge,
        `OPEN record of ${size} bytes exceeds the peer's record limit of ${this.peerRecordLimit}`,
      );
    }
    const id = this.#allocateId();
    const entry = this.#makeChannel(id, true, openPayload.slice());
    this.#channels.set(id, entry);
    return entry;
  }

  /**
   * The next id of this side's parity, in increasing order, skipping the
   * reserved ids; never one used before on this connection.
   */
  #allocateId(): number {
    let id = this.#nextId;
    while (MuxFrame.isReservedId(id)) {
      id += 2;
    }
    if (id > quicMaxChannelId) {
      throw SwitchboardError.of(
        StatusCode.resourceExhausted,
        'every channel id of this side was used; open on a new connection',
      );
    }
    this.#nextId = id + 2;
    return id;
  }

  /**
   * Resolves when this side may open a channel's stream: on the initiator,
   * once the control stream is open (it goes first) and, with
   * `QuicIdentityRules.awaitPeerIdentity`, once the acceptor has identified.
   * Rejects with the reason the channel cannot be opened.
   */
  async #openGate(): Promise<void> {
    if (this.isInitiator) {
      await this.#controlReady.promise;
      if (this.quicOptions.identity.awaitPeerIdentity && !this.#mutualIdentification.isSettled) {
        const limit = this.options.identityTimeout;
        await withTimeout(this.options.clock, this.#mutualIdentification.promise, limit, () => {
          throw SwitchboardError.of(
            StatusCode.unauthenticated,
            `the peer did not identify within ${limit} ms`,
          );
        });
      }
    }
    if (this.#closing) {
      throw new SwitchboardError(this.#closeStatus ?? this.#lostStatus());
    }
  }

  // Ping, GOAWAY, close -------------------------------------------------------

  ping(payload?: Uint8Array, options?: { signal?: AbortSignal }): Promise<number> {
    const message = MuxControlMessage.ping(payload ?? this.#nextPingPayload());
    const done = new Deferred<number>();
    const signal = options?.signal;
    if (this.#closing) {
      done.reject(SwitchboardError.of(StatusCode.failedPrecondition, 'connection is closed'));
      return done.promise;
    }
    if (signal?.aborted) {
      done.reject(signal.reason);
      return done.promise;
    }
    if (this.#pings.length >= QuicBindingConnection.maxPendingPings) {
      const oldest = this.#pings.shift() as PendingPing;
      oldest.cleanup?.();
      oldest.done.reject(
        SwitchboardError.of(
          StatusCode.deadlineExceeded,
          `no PONG before ${QuicBindingConnection.maxPendingPings} later PINGs`,
        ),
      );
    }
    const pending: PendingPing = {
      payload: message.payload.slice(),
      done,
      start: this.options.clock.now(),
      cleanup: undefined,
    };
    if (signal !== undefined) {
      const onAbort = (): void => {
        const i = this.#pings.indexOf(pending);
        if (i >= 0) {
          this.#pings.splice(i, 1);
        }
        done.reject(signal.reason);
      };
      signal.addEventListener('abort', onAbort, { once: true });
      pending.cleanup = () => signal.removeEventListener('abort', onAbort);
    }
    this.#pings.push(pending);
    this.#sendControl(message);
    return done.promise;
  }

  #nextPingPayload(): Uint8Array {
    const n = this.#pingCounter;
    this.#pingCounter = (n + 1) & 0x7fffffff;
    return Uint8Array.of(n & 0xff, (n >> 8) & 0xff, (n >> 16) & 0xff, (n >> 24) & 0xff);
  }

  #completePing(payload: Uint8Array): void {
    const i = this.#pings.findIndex((pending) => bytesEqual(pending.payload, payload));
    if (i < 0) {
      return;
    }
    const [pending] = this.#pings.splice(i, 1) as [PendingPing];
    pending.cleanup?.();
    pending.done.resolve(this.options.clock.now() - pending.start);
  }

  /**
   * Sends GOAWAY with [status], waits for the open channels (and the drain
   * holds) up to `MuxOptions.goAwayGrace`, then closes.
   *
   * @throws {RangeError} for an application code.
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
      this.#sendControl(MuxControlMessage.goAway(statusForWire(status)));
    }
    return this.#goAwayWait();
  }

  async #goAwayWait(): Promise<void> {
    // Let channel readers register their pending bulk work.
    await delay(this.options.clock, 0);
    if (
      (this.#channels.size > 0 || this.hasDrainHolds || this.#pendingStreams.size > 0) &&
      !this.#closing
    ) {
      this.#idle ??= new Deferred<void>();
      await withTimeout(this.options.clock, this.#idle.promise, this.options.goAwayGrace, () => {
        this.#log('debug', `${this}: GOAWAY grace expired with ${this.#channels.size} channels`);
      });
    }
    await this.close();
  }

  /**
   * Closes the session: an application close with code 0 (or the status of
   * this side's GOAWAY when it was not `GOING_AWAY`), once what this side
   * finished writing on its streams has been acknowledged, within
   * `MuxOptions.keepAliveTimeout`. The channels still open end with
   * `CONNECTION_LOST`, or the GOAWAY status if either side sent one. Never
   * rejects.
   */
  close(): Promise<void> {
    const own = this.#ownGoAway;
    const graceful = own === undefined || own.known === StatusCode.goingAway;
    return this.#shutdown(this.#lostStatus(), undefined, {
      code: graceful ? 0 : quicErrorCode(own),
      reason: graceful ? '' : closeReason(own),
      drain: true,
    });
  }

  [Symbol.asyncDispose](): Promise<void> {
    return this.close();
  }

  /** The local `CONNECTION_LOST` status, unless a GOAWAY was received or sent first. */
  #lostStatus(reason = ''): Status {
    return this.#peerGoAway ?? this.#ownGoAway ?? Status.of(StatusCode.connectionLost, reason);
  }

  #protocolError(reason: string): void {
    this.#fail(Status.of(StatusCode.protocolError, reason));
  }

  /**
   * The peer reset or stopped the control stream, which is a protocol
   * error, except where it is the end of a WebTransport session: the side
   * that terminates a session resets and stops its streams with
   * `WT_SESSION_GONE`, which carries no application code and can overtake
   * the close itself. Without a code over WebTransport, the session's end is
   * waited for (it ends this connection as the session ends) for
   * {@link webTransportSessionGoneGrace}, and only then is it a protocol
   * error.
   */
  #controlAborted(reason: string, hasCode: boolean): void {
    if (this.#closing) {
      return;
    }
    if (!this.isWebTransport || hasCode) {
      this.#protocolError(reason);
      return;
    }
    this.#sessionGoneTimer ??= this.options.clock.setTimeout(() => {
      if (!this.#closing) {
        this.#protocolError(reason);
      }
    }, QuicBindingConnection.webTransportSessionGoneGrace);
  }

  /**
   * Ends the connection because the peer broke the protocol (or the
   * identification rules): an application close carrying [status] at once,
   * without a GOAWAY first, which could be lost with its stream.
   */
  #fail(status: Status): void {
    if (this.#closing) {
      return;
    }
    this.#log('warn', `${this}: closing with ${status}`);
    void this.#shutdown(status, this.#lostStatus(), {
      code: quicErrorCode(status),
      reason: closeReason(status),
      drain: false,
    });
  }

  /** The session is lost to this side: closed without a status of its own. */
  #lost(reason: string): void {
    if (this.#closing) {
      return;
    }
    void this.#shutdown(this.#lostStatus(reason), undefined, {
      code: StatusCode.unavailable,
      reason,
      drain: false,
    });
  }

  #shutdown(
    status: Status,
    channelStatus: Status | undefined,
    close: { code: number; reason: string; drain: boolean },
  ): Promise<void> {
    this.#closeStatus ??= status;
    this.#closeFuture ??= this.#doShutdown(status, channelStatus ?? status, close);
    return this.#closeFuture;
  }

  async #doShutdown(
    status: Status,
    channelStatus: Status,
    close: { code: number; reason: string; drain: boolean },
  ): Promise<void> {
    this.#closing = true;
    const finishing = close.drain ? [...this.#finishing] : [];
    this.#finish(status, channelStatus);
    const clock = this.options.clock;
    const limit = this.options.keepAliveTimeout;
    if (finishing.length > 0) {
      const all = Promise.allSettled(finishing).then((results) => {
        if (results.some((result) => result.status === 'rejected')) {
          throw new Error('a stream failed');
        }
      });
      try {
        await withTimeout(clock, all, limit, () => {
          throw new Error('timed out');
        });
      } catch {
        this.#log('debug', `${this}: closing before every stream was acknowledged`);
      }
    }
    try {
      await withTimeout(
        clock,
        attempt(() => this.session.close(close.code, close.reason)),
        limit,
        () => {
          throw new Error('session close timed out');
        },
      );
    } catch (error) {
      this.#log('debug', `${this}: session close: ${error}`);
    }
    for (const iterator of [this.#streams, this.#unidirectional]) {
      // Not awaited: an iterator may finish only with the session.
      attempt(() => iterator?.return?.()).catch((error: unknown) => {
        this.#log('debug', `${this}: cancelling incoming streams: ${error}`);
      });
    }
  }

  #finish(status: Status, channelStatus: Status): void {
    const clock = this.options.clock;
    if (this.#keepAliveTimer !== undefined) {
      clock.clearTimeout(this.#keepAliveTimer);
      this.#keepAliveTimer = undefined;
    }
    this.#clearControlTimer();
    if (this.#sessionGoneTimer !== undefined) {
      clock.clearTimeout(this.#sessionGoneTimer);
      this.#sessionGoneTimer = undefined;
    }
    this.#controlQueue = [];
    for (const stream of this.#pendingStreams) {
      this.#release(stream, StatusCode.goingAway);
    }
    this.#pendingStreams.clear();
    const drained = this.#controlDrained;
    this.#controlDrained = undefined;
    drained?.resolve();
    const entries = [...this.#channels.values()];
    this.#channels.clear();
    for (const { channel, control } of entries) {
      try {
        control.connectionLost(channelStatus);
      } catch (error) {
        this.#log('warn', `${this}: ending channel ${channel.id}`, error);
      }
      if (!this.#undelivered.includes(channel)) {
        this.#releasePayload(channel);
      }
    }
    const pings = this.#pings;
    this.#pings = [];
    for (const pending of pings) {
      pending.cleanup?.();
      pending.done.reject(new SwitchboardError(status));
    }
    this.#heldControl = [];
    const nonceWaiter = this.#nonceWaiter;
    this.#nonceWaiter = undefined;
    nonceWaiter?.reject(new SwitchboardError(status));
    const bound = this.#boundIdentityWaiter;
    this.#boundIdentityWaiter = undefined;
    bound?.reject(new SwitchboardError(status));
    this.#controlReady.reject(new SwitchboardError(status));
    this.#mutualIdentification.reject(new SwitchboardError(status));
    this.#ready.resolve();
    this.#signalIdentity();
    const idle = this.#idle;
    this.#idle = undefined;
    idle?.resolve();
    this.#deliverIncoming();
    this.#done.resolve(status);
  }

  #onSessionEnd(end: QuicSessionEnd): void {
    if (this.#closing) {
      return;
    }
    if (!end.lost && end.byPeer) {
      // An application close counts as a GOAWAY with its status (code 0:
      // GOING_AWAY), then the end of the transport.
      const code = end.code;
      this.#onPeerGoAway(
        code === 0
          ? Status.of(StatusCode.goingAway, end.reason)
          : statusFromWire(statusOfQuicErrorCode(code, end.reason)),
      );
      this.#log('debug', `${this}: the peer closed the session (${code} ${end.reason})`);
    } else {
      this.#log('debug', `${this}: the session ended: ${end.reason}`);
    }
    void this.#shutdown(this.#lostStatus(end.reason), undefined, {
      code: 0,
      reason: '',
      drain: false,
    });
  }

  /**
   * Tracks the acknowledgement of a stream direction this side finished,
   * which a graceful {@link close} waits for.
   */
  #trackFinishing(acknowledged: Promise<void>): void {
    if (this.#closing) {
      acknowledged.catch(() => {});
      return;
    }
    this.#finishing.add(acknowledged);
    const remove = (): void => {
      this.#finishing.delete(acknowledged);
    };
    acknowledged.then(remove, remove);
  }

  // Keep-alive ----------------------------------------------------------------

  #startKeepAlive(): void {
    const interval = this.options.keepAliveInterval;
    if (interval === undefined || this.#closing) {
      return;
    }
    this.#armKeepAlive(interval);
  }

  #armKeepAlive(ms: number): void {
    this.#keepAliveTimer = this.options.clock.setTimeout(() => this.#onKeepAliveTimer(), ms);
  }

  /** A record arrived from the peer: it is alive. */
  #noteReceive(): void {
    this.#lastReceive = this.options.clock.now();
    if (this.#probing) {
      this.#probing = false;
      if (this.#keepAliveTimer !== undefined) {
        this.options.clock.clearTimeout(this.#keepAliveTimer);
        this.#keepAliveTimer = undefined;
      }
      this.#startKeepAlive();
    }
  }

  #onKeepAliveTimer(): void {
    const interval = this.options.keepAliveInterval;
    this.#keepAliveTimer = undefined;
    if (this.#closing || interval === undefined) {
      return;
    }
    if (this.#controlQueued > this.quicOptions.controlOutputMark) {
      // Reading is paused locally; that silence is not a dead peer.
      this.#probing = false;
      this.#armKeepAlive(interval);
      return;
    }
    if (this.#probing) {
      this.#log('info', `${this}: no answer to keep-alive PING, closing`);
      void this.#shutdown(this.#lostStatus('keep-alive timeout'), undefined, {
        code: StatusCode.deadlineExceeded,
        reason: 'keep-alive timeout',
        drain: false,
      });
      return;
    }
    const silence = this.options.clock.now() - this.#lastReceive;
    if (silence < interval || this.#verifying) {
      this.#armKeepAlive(silence < interval ? interval - silence : interval);
      return;
    }
    this.#probing = true;
    this.#armKeepAlive(this.options.keepAliveTimeout);
    this.#sendControl(MuxControlMessage.ping(this.#nextPingPayload()));
  }

  // Identity ------------------------------------------------------------------

  /**
   * Identifies this side to the peer, as `MuxConnection.identify` does, with
   * the proof bound to the TLS exporter when both `HELLO`s carry `EXPORTER`
   * ({@link exporterBound}), and under the `QuicIdentityRules` of
   * {@link quicOptions}:
   *
   * - With `requireExporterBinding`, rejects with `UNAUTHENTICATED` without
   *   identifying when the proof could not be exporter-bound.
   * - With `bearerOnlyToAuthenticatedPeer`, a bearer credential rejects
   *   with `FAILED_PRECONDITION` unless the peer is an authenticated end
   *   ({@link peerAuthenticated}); with `identifyOnlyToIdentifiedPeer`,
   *   every credential does.
   * - With `awaitPeerIdentity` on the initiator, resolves only once the
   *   acceptor's exporter-bound `IDENT` naming this side (and, when
   *   `receiver` is given, proving that identity) has verified; the channels
   *   opened meanwhile wait for it. The wait counts toward `timeout`.
   *
   * A holder key given with a bearer credential is ignored: a bearer
   * credential carries no proof (as in Dart, and as the frame mux does).
   *
   * @throws {TypeError} for a holder-key credential whose holder key is
   * missing or not the one it names.
   * @throws {RangeError} for an intent, receiver or credential too long for
   * an `IDENT`.
   */
  identify(credential: Credential, options: IdentifyOptions = {}): Promise<void> {
    const holder = credential.holderKey;
    const holderKey = options.holderKey;
    if (holder !== undefined) {
      if (holderKey === undefined) {
        throw new TypeError('the credential names a holder key, the proof needs its key pair');
      }
      if (!bytesEqual(holderKey.publicKey, holder)) {
        throw new TypeError('not the holder key the credential names');
      }
    }
    const intent = options.intent === undefined ? new Uint8Array(0) : options.intent.slice();
    const receiver = options.receiver;
    const receiverBytes = receiver === undefined ? new Uint8Array(0) : utf8Encode(receiver);
    const credentialBytes = credential.encode();
    // Checks the lengths before anything is sent.
    new MuxIdent({
      credential: credentialBytes,
      intent,
      receiver: receiverBytes,
      proof: holder === undefined ? undefined : new Uint8Array(MuxIdent.proofLength),
    }).encode();
    const identifying = this.#identify(
      credential,
      credentialBytes,
      intent,
      receiver,
      receiverBytes,
      options,
    );
    identifying.catch(() => {});
    return identifying;
  }

  async #identify(
    credential: Credential,
    credentialBytes: Uint8Array,
    intent: Uint8Array,
    receiver: string | undefined,
    receiverBytes: Uint8Array,
    options: IdentifyOptions,
  ): Promise<void> {
    if (this.#closing) {
      throw SwitchboardError.of(StatusCode.failedPrecondition, 'connection is closed');
    }
    const rules = this.quicOptions.identity;
    if (rules.identifyOnlyToIdentifiedPeer && !this.peerAuthenticated) {
      throw SwitchboardError.of(
        StatusCode.failedPrecondition,
        'the peer has not identified with an exporter-bound proof',
      );
    }
    const holder = credential.holderKey;
    if (holder === undefined && rules.bearerOnlyToAuthenticatedPeer && !this.peerAuthenticated) {
      throw SwitchboardError.of(
        StatusCode.failedPrecondition,
        'a bearer credential goes only to an authenticated peer',
      );
    }
    const clock = this.options.clock;
    const limit = options.timeout ?? this.options.identityTimeout;
    const start = clock.now();
    const signal = options.signal;
    signal?.throwIfAborted();
    const bounded = <T>(promise: Promise<T>, what: string): Promise<T> => {
      let raced = promise;
      if (signal !== undefined) {
        raced = new Promise<T>((resolve, reject) => {
          const onAbort = (): void => reject(signal.reason);
          signal.addEventListener('abort', onAbort, { once: true });
          promise.then(
            (value) => {
              signal.removeEventListener('abort', onAbort);
              resolve(value);
            },
            (error: unknown) => {
              signal.removeEventListener('abort', onAbort);
              reject(error);
            },
          );
        });
      }
      if (!(limit > 0)) {
        return raced;
      }
      const left = limit - (clock.now() - start);
      return withTimeout(clock, raced, left > 0 ? left : 1, () => {
        throw SwitchboardError.of(StatusCode.deadlineExceeded, `${what} within ${limit} ms`);
      });
    };
    const failure = (error: unknown): unknown =>
      !(error instanceof SwitchboardError) || error.is(StatusCode.deadlineExceeded)
        ? error
        : this.#identifyFailure();

    const localNonce = this.#localNonce ?? this.#sendNonce();
    let peerNonce = this.#peerNonce;
    if (peerNonce === undefined) {
      this.#nonceWaiter ??= new Deferred<Uint8Array>();
      try {
        peerNonce = await bounded(this.#nonceWaiter.promise, 'no NONCE from the peer');
      } catch (error) {
        throw failure(error);
      }
    }
    // The peer's HELLO came before its NONCE, so both are known.
    const bound = this.exporterBound;
    if (!bound && rules.requireExporterBinding) {
      throw SwitchboardError.of(
        StatusCode.unauthenticated,
        'the identification cannot be bound to the TLS session',
      );
    }
    let proof: Uint8Array | undefined;
    if (holder !== undefined) {
      const key = options.holderKey as NonNullable<IdentifyOptions['holderKey']>;
      const message = MuxIdent.proofMessage(
        localNonce,
        peerNonce,
        intent,
        receiverBytes,
        bound ? this.#exporter : undefined,
      );
      proof = await bounded(
        attempt(() => key.sign(message)),
        'proof not signed',
      );
    }
    if (this.#closing) {
      throw this.#identifyFailure();
    }
    const awaitPeer = this.isInitiator && rules.awaitPeerIdentity;
    if (awaitPeer) {
      this.#boundIdentityWaiter ??= new Deferred<PeerIdentity>();
    }
    const peerWaiter = awaitPeer ? this.#boundIdentityWaiter : undefined;
    this.#log(
      'debug',
      `${this}: identifying as "${credential.identity}"` +
        `${receiver === undefined || receiver.length === 0 ? '' : ` to "${receiver}"`}` +
        `${bound ? ', exporter-bound' : ''}`,
    );
    this.#sendControl(
      MuxControlMessage.ident(
        new MuxIdent({ credential: credentialBytes, intent, receiver: receiverBytes, proof }),
      ),
    );
    try {
      await bounded(this.ping(), 'IDENT not confirmed');
    } catch (error) {
      throw failure(error);
    }
    if (peerWaiter === undefined) {
      return;
    }
    let identity: PeerIdentity;
    try {
      identity =
        this.peerIdentityBound && this.peerIdentity !== undefined
          ? this.peerIdentity
          : await bounded(peerWaiter.promise, 'no exporter-bound IDENT from the peer');
    } catch (error) {
      throw failure(error);
    }
    if (receiver !== undefined && receiver.length > 0 && identity.identity !== receiver) {
      this.#log(
        'info',
        `${this}: the peer identified as "${identity.identity}", not "${receiver}"`,
      );
      this.#fail(unauthenticated);
      throw SwitchboardError.of(StatusCode.unauthenticated, `the peer is not "${receiver}"`);
    }
    this.#mutualIdentification.resolve();
  }

  #identifyFailure(): SwitchboardError {
    const peer = this.#peerGoAwayReceived;
    if (peer !== undefined && peer.known === StatusCode.unauthenticated) {
      return SwitchboardError.of(StatusCode.unauthenticated, 'the peer refused the credential');
    }
    if (this.#done.isSettled || this.#closing) {
      return new SwitchboardError(this.#closeStatus ?? this.#lostStatus());
    }
    return SwitchboardError.of(StatusCode.failedPrecondition, 'connection is closed');
  }

  #sendNonce(): Uint8Array {
    const nonce = secureRandomBytes(MuxIdent.nonceLength);
    this.#localNonce = nonce;
    this.#sendControl(MuxControlMessage.nonce(nonce));
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
      throw new ProtocolError('NONCE equal to ours');
    }
    const nonce = payload.slice();
    this.#peerNonce = nonce;
    const requested = this.#localNonce === undefined;
    if (requested) {
      this.#sendNonce();
    }
    const waiter = this.#nonceWaiter;
    this.#nonceWaiter = undefined;
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
    const ident = MuxIdent.decode(payload);
    const verifier = this.options.identityVerifier;
    const bound = this.exporterBound;
    if (!bound && this.quicOptions.identity.requireExporterBinding) {
      this.#log('info', `${this}: IDENT refused: not bound to the TLS session`);
      this.#fail(unauthenticated);
      return;
    }
    if (verifier === undefined) {
      this.#log('debug', `${this}: ignoring IDENT, no identity verifier`);
      return;
    }
    // Control messages after the IDENT wait for its verification; the
    // channels do not, being streams of their own.
    this.#verifying = true;
    void this.#verifyIdent(verifier, ident, peerNonce, localNonce, bound);
  }

  async #verifyIdent(
    verifier: NonNullable<MuxOptions['identityVerifier']>,
    ident: MuxIdent,
    senderNonce: Uint8Array,
    receiverNonce: Uint8Array,
    bound: boolean,
  ): Promise<void> {
    let identity: PeerIdentity | undefined;
    let failureReason = '';
    try {
      const ended = this.done.then((status): never => {
        throw new SwitchboardError(status);
      });
      ended.catch(() => {});
      const verification = Promise.race([
        verifyIdent(ident, {
          verifier,
          senderNonce,
          receiverNonce,
          localIdentity: this.localIdentity,
          requireNamedIdent:
            this.options.requireNamedIdent ||
            (this.isInitiator && this.quicOptions.identity.awaitPeerIdentity),
          exporter: bound ? this.#exporter : undefined,
        }),
        ended,
      ]);
      identity = await withTimeout(
        this.options.clock,
        verification,
        this.options.identityTimeout,
        () => {
          throw SwitchboardError.of(StatusCode.deadlineExceeded, 'verification timed out');
        },
      );
    } catch (error) {
      if (error instanceof SwitchboardError) {
        failureReason = error.status.reason;
      } else {
        this.#log('warn', `${this}: IDENT verification failed`, error);
        failureReason = 'verification failed';
      }
    }
    this.#verifying = false;
    if (this.#closing) {
      this.#heldControl = [];
      return;
    }
    if (identity === undefined) {
      this.#log('info', `${this}: IDENT refused: ${failureReason}`);
      this.#heldControl = [];
      this.#fail(unauthenticated);
      return;
    }
    // A bearer credential carries no proof: nothing bound it to the session.
    const proven = bound && ident.proof.length > 0;
    this.#log(
      'debug',
      `${this}: peer identified as "${identity.identity}"${proven ? ', exporter-bound' : ''}`,
    );
    this.#peerIdentity = identity;
    this.#peerIdentityBound = proven;
    if (proven) {
      const waiter = this.#boundIdentityWaiter;
      this.#boundIdentityWaiter = undefined;
      waiter?.resolve(identity);
    }
    this.#peerIdentified.resolve(identity);
    this.#signalIdentity();
    while (this.#heldControl.length > 0 && !this.#verifying && !this.#closing) {
      const message = this.#heldControl.shift() as MuxControlMessage;
      this.#guarded(() => this.#handleControl(message));
    }
  }

  #signalIdentity(): void {
    const event = this.#identityEvent;
    this.#identityEvent = undefined;
    event?.resolve();
  }

  override toString(): string {
    return (
      `QuicBindingConnection(${this.isInitiator ? 'initiator' : 'acceptor'}, ` +
      `${this.session.remote}, ${this.#channels.size} channels${this.#closing ? ', closed' : ''})`
    );
  }
}
