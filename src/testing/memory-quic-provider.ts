/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * An in-memory QUIC stream provider for the tests of the binding and of
 * the node's QUIC paths, a port of `switchboard_quic`'s
 * `test/memory_provider.dart`: sessions with bidirectional streams whose
 * directions are delivered independently, each in order, with a
 * configurable delay per stream (so streams overtake one another and the
 * control stream), stream and connection flow control (data stays with the
 * sender until the receiver reads), a stream limit, resets and
 * STOP_SENDING with codes (the sender's stack resets on STOP_SENDING, as
 * RFC 9000 section 3.5 has it), FIN, an exporter equal at both ends of a
 * session and different per session, and ends without one (a browser
 * without `exportKeyingMaterial`).
 *
 * Time comes from the network's {@link Clock} (the system clock by
 * default), so a test may drive delays by hand.
 */

import { bytesEqual } from '../core/bytes.ts';
import {
  defaultQuicLimits,
  type QuicClientConfig,
  type QuicConnector,
  QuicError,
  type QuicListener,
  type QuicReceiveStream,
  type QuicSession,
  type QuicSessionEnd,
  type QuicStream,
  QuicStreamReset,
  QuicStreamStopped,
} from '../core/quic/contract.ts';
import { AsyncQueue } from '../core/util/async-queue.ts';
import { type Clock, systemClock, type TimerHandle } from '../core/util/clock.ts';
import { Deferred } from '../core/util/deferred.ts';

/** What the delay of a stream direction is chosen by. */
export interface MemoryQuicStreamInfo {
  /** The stream's place among the session's streams, in the order they were opened (by either side), from 0. */
  readonly ordinal: number;
  /** Whether the initiator opened the stream. */
  readonly openedByInitiator: boolean;
}

/** The delivery delay of a stream's bytes in milliseconds, per direction: [fromInitiator] is the direction the initiator writes. */
export type MemoryQuicDelay = (stream: MemoryQuicStreamInfo, fromInitiator: boolean) => number;

/** No delay: everything is delivered on the next macrotask. */
export const noDelay: MemoryQuicDelay = () => 0;

/** One end's configuration: what it grants its peer, and what it can do. */
export interface MemoryQuicEndConfig {
  /** Whether this end can compute the exporter; default true. */
  readonly exporter: boolean;
  /** The receive window of each stream this end grants; default 64 KiB. */
  readonly streamWindow: number;
  /** The connection receive window this end grants; default 16 MiB. */
  readonly connectionWindow: number;
  /** How many streams the peer may have open toward this end at once; default 128. */
  readonly maxStreams: number;
  /** The hash of this end's certificate (servers). */
  readonly certificateHash: Uint8Array | undefined;
}

/** The defaults of {@link MemoryQuicEndConfig}. */
export const defaultMemoryQuicEndConfig: MemoryQuicEndConfig = Object.freeze({
  exporter: true,
  streamWindow: 64 * 1024,
  connectionWindow: 16 << 20,
  maxStreams: 128,
  certificateHash: undefined,
});

/** [config] completed with the defaults. */
export function memoryQuicEndConfig(config?: Partial<MemoryQuicEndConfig>): MemoryQuicEndConfig {
  return config === undefined
    ? defaultMemoryQuicEndConfig
    : { ...defaultMemoryQuicEndConfig, ...config };
}

const sessionEnded = (): QuicError => new QuicError('sessionEnded', 'session ended');

/** Listeners by endpoint, and the connectors that reach them. */
export class MemoryQuicNetwork {
  /** The delay of the stream directions of every session. */
  delay: MemoryQuicDelay;
  /** How long an application close takes to reach the peer, in milliseconds. */
  closeDelay: number;
  /** Time and timers. */
  readonly clock: Clock;
  /** Every session pair created, initiator first. */
  readonly pairs: [MemoryQuicSession, MemoryQuicSession][] = [];
  readonly #listeners = new Map<string, MemoryQuicListener>();

  constructor(options: { delay?: MemoryQuicDelay; closeDelay?: number; clock?: Clock } = {}) {
    this.delay = options.delay ?? noDelay;
    this.closeDelay = options.closeDelay ?? 0;
    this.clock = options.clock ?? systemClock;
  }

  /** The key of an endpoint: scheme, host and port. */
  static key(endpoint: string): string {
    const url = new URL(endpoint);
    return `${url.protocol.toLowerCase()}//${url.hostname.toLowerCase()}:${url.port}`;
  }

  /** A listener at [endpoint] whose sessions grant what [config] says. */
  listen(endpoint: string, config?: Partial<MemoryQuicEndConfig>): MemoryQuicListener {
    const listener = new MemoryQuicListener(this, endpoint, memoryQuicEndConfig(config));
    this.#listeners.set(MemoryQuicNetwork.key(endpoint), listener);
    return listener;
  }

  /**
   * A connector whose sessions are configured by [config] beyond what the
   * client configuration says. It dials `quic` endpoints, and `https` ones
   * with [webTransport].
   */
  connector(
    options: { config?: Partial<MemoryQuicEndConfig>; webTransport?: boolean } = {},
  ): MemoryQuicConnector {
    return new MemoryQuicConnector(
      this,
      memoryQuicEndConfig(options.config),
      options.webTransport ?? false,
    );
  }

  /** The listener at [endpoint], if any. */
  listenerAt(endpoint: string): MemoryQuicListener | undefined {
    return this.#listeners.get(MemoryQuicNetwork.key(endpoint));
  }

  /** @internal */
  removeListener(listener: MemoryQuicListener): void {
    const key = MemoryQuicNetwork.key(listener.endpoint);
    if (this.#listeners.get(key) === listener) {
      this.#listeners.delete(key);
    }
  }

  /** A promise that resolves after [ms] on the network's clock. */
  sleep(ms: number): Promise<void> {
    return new Promise((resolve) => {
      this.clock.setTimeout(resolve, ms);
    });
  }
}

/** Dials the listeners of a {@link MemoryQuicNetwork}. */
export class MemoryQuicConnector implements QuicConnector {
  readonly network: MemoryQuicNetwork;
  readonly config: MemoryQuicEndConfig;
  readonly webTransport: boolean;
  /** The configurations the binding asked for, in order. */
  readonly configs: QuicClientConfig[] = [];
  /** While set, a connect waits for it before it reaches the listener (a handshake that takes its time). */
  connectGate: Promise<void> | undefined;

  constructor(network: MemoryQuicNetwork, config: MemoryQuicEndConfig, webTransport: boolean) {
    this.network = network;
    this.config = config;
    this.webTransport = webTransport;
  }

  async connect(endpoint: string, config: QuicClientConfig): Promise<QuicSession> {
    for (const [name, duration] of [
      ['idleTimeout', config.idleTimeout],
      ['keepAlive', config.keepAlive],
      ['handshakeTimeout', config.handshakeTimeout],
    ] as const) {
      if (duration !== undefined && duration < 0) {
        throw new RangeError(`${name} must not be negative: ${duration}`);
      }
    }
    this.configs.push(config);
    await this.connectGate;
    await this.network.sleep(0);
    const scheme = endpoint.slice(0, Math.max(0, endpoint.indexOf(':'))).toLowerCase();
    if (scheme !== 'quic' && !(this.webTransport && scheme === 'https')) {
      throw new QuicError('unsupportedScheme', `the in-memory provider does not dial ${scheme}`);
    }
    const listener = this.network.listenerAt(endpoint);
    if (listener === undefined || listener.isClosed) {
      throw new QuicError('unreachable', `no listener at ${endpoint}`);
    }
    const hash = listener.config.certificateHash;
    const pins = config.serverCertificateHashes ?? [];
    if (pins.length > 0 && (hash === undefined || !pins.some((h) => bytesEqual(h, hash)))) {
      throw new QuicError('certificateRejected', 'the certificate is not pinned');
    }
    const [client, server] = MemoryQuicSession.pair({
      network: this.network,
      initiator: {
        ...this.config,
        streamWindow: config.streamReceiveWindow ?? defaultQuicLimits.streamReceiveWindow,
        connectionWindow:
          config.connectionReceiveWindow ?? defaultQuicLimits.connectionReceiveWindow,
        maxStreams: config.maxStreamsBidi ?? defaultQuicLimits.maxStreamsBidi,
      },
      acceptor: listener.config,
      certificateHash: hash,
    });
    listener.acceptSession(server);
    return client;
  }
}

/** Accepts the sessions dialled to its endpoint. */
export class MemoryQuicListener implements QuicListener {
  readonly network: MemoryQuicNetwork;
  readonly config: MemoryQuicEndConfig;
  readonly endpoint: string;
  readonly #sessions = new AsyncQueue<QuicSession>();
  #closed = false;

  constructor(network: MemoryQuicNetwork, endpoint: string, config: MemoryQuicEndConfig) {
    this.network = network;
    this.endpoint = endpoint;
    this.config = config;
  }

  get sessions(): AsyncIterable<QuicSession> {
    return this.#sessions;
  }

  get isClosed(): boolean {
    return this.#closed;
  }

  /** Hands [session] to the listener's owner, as if it had been accepted. */
  acceptSession(session: QuicSession): void {
    this.#sessions.push(session);
  }

  async close(): Promise<void> {
    this.#closed = true;
    this.network.removeListener(this);
    this.#sessions.end();
  }
}

// A deterministic generator for the session secrets (mulberry32), so that
// runs are reproducible as the Dart provider's seeded Random makes them.
let seed = 1234;
function nextRandomByte(): number {
  seed = (seed + 0x6d2b79f5) | 0;
  let t = seed;
  t = Math.imul(t ^ (t >>> 15), t | 1);
  t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
  return ((t ^ (t >>> 14)) >>> 0) & 0xff;
}

/** One end of an in-memory session. */
export class MemoryQuicSession implements QuicSession {
  readonly network: MemoryQuicNetwork;
  readonly isInitiator: boolean;
  /** What this end grants its peer. */
  readonly config: MemoryQuicEndConfig;
  readonly #secret: Uint8Array;
  readonly #certificateHash: Uint8Array | undefined;
  #peer: MemoryQuicSession | undefined;

  readonly #incoming = new AsyncQueue<QuicStream>();
  readonly #unidirectional = new AsyncQueue<QuicReceiveStream>();
  readonly #draining = new Deferred<void>();
  readonly #done = new Deferred<QuicSessionEnd>();
  readonly #streamWaiters: Deferred<void>[] = [];
  /** @internal Directions blocked on connection credit. */
  readonly blocked = new Set<MemoryQuicDirection>();

  /** Streams of this end, opened by either side, in order. */
  readonly streams: MemoryQuicStream[] = [];
  /** The ordinals of the streams the peer opened, in the order they became visible here. */
  readonly announced: number[] = [];
  /** Application closes this end made: [code, reason]. */
  readonly closes: [number, string][] = [];
  /** The labels the exporter was asked for. */
  readonly exporterLabels: string[] = [];
  /** Mixed into the exporter, to make the two ends disagree. */
  exporterXor = 0;
  /** While set, the exporter fails with it. */
  exporterFailure: unknown;
  /** While set, the exporter waits for it. */
  exporterGate: Promise<void> | undefined;
  /** While set, `openStream` fails with it. */
  openFailure: unknown;

  #openStreams = 0;
  /** @internal */
  connectionSent = 0;
  /** @internal */
  connectionConsumed = 0;
  #ordinal = 0;

  private constructor(
    network: MemoryQuicNetwork,
    isInitiator: boolean,
    config: MemoryQuicEndConfig,
    secret: Uint8Array,
    certificateHash: Uint8Array | undefined,
  ) {
    this.network = network;
    this.isInitiator = isInitiator;
    this.config = config;
    this.#secret = secret;
    this.#certificateHash = certificateHash;
  }

  /** A connected pair: the initiator's end and the acceptor's. */
  static pair(
    options: {
      network?: MemoryQuicNetwork;
      initiator?: Partial<MemoryQuicEndConfig>;
      acceptor?: Partial<MemoryQuicEndConfig>;
      certificateHash?: Uint8Array | undefined;
    } = {},
  ): [MemoryQuicSession, MemoryQuicSession] {
    const network = options.network ?? new MemoryQuicNetwork();
    const secret = new Uint8Array(32);
    for (let i = 0; i < secret.length; i++) {
      secret[i] = nextRandomByte();
    }
    const a = new MemoryQuicSession(
      network,
      true,
      memoryQuicEndConfig(options.initiator),
      secret,
      options.certificateHash,
    );
    const b = new MemoryQuicSession(
      network,
      false,
      memoryQuicEndConfig(options.acceptor),
      secret,
      undefined,
    );
    a.#peer = b;
    b.#peer = a;
    network.pairs.push([a, b]);
    return [a, b];
  }

  /** The other end. */
  get peer(): MemoryQuicSession {
    return this.#peer as MemoryQuicSession;
  }

  get ended(): boolean {
    return this.#done.isSettled;
  }

  get protocol(): string | undefined {
    return 'switchboard';
  }

  get remote(): string {
    return this.isInitiator ? 'memory acceptor' : 'memory initiator';
  }

  get maxIncomingStreams(): number | undefined {
    return this.config.maxStreams;
  }

  get peerCertificateHash(): Uint8Array | undefined {
    return this.#certificateHash;
  }

  get incomingStreams(): AsyncIterable<QuicStream> {
    return this.#incoming;
  }

  get incomingUnidirectionalStreams(): AsyncIterable<QuicReceiveStream> {
    return this.#unidirectional;
  }

  get draining(): Promise<void> {
    return this.#draining.promise;
  }

  get done(): Promise<QuicSessionEnd> {
    return this.#done.promise;
  }

  /** The peer starts draining (a WebTransport drain). */
  drainPeer(): void {
    this.peer.#draining.resolve();
  }

  /** Opens a unidirectional stream toward the peer; returns the peer's receive side. */
  openUnidirectional(): MemoryQuicReceiveStream {
    const stream = new MemoryQuicReceiveStream();
    this.peer.#unidirectional.push(stream);
    return stream;
  }

  /** Fails the iteration of incoming streams with [error]. */
  failIncoming(error: unknown): void {
    this.#incoming.fail(error);
  }

  async exportKeyingMaterial(
    label: string,
    context: Uint8Array,
    length: number,
  ): Promise<Uint8Array | undefined> {
    this.exporterLabels.push(label);
    await this.exporterGate;
    if (this.exporterFailure !== undefined) {
      throw this.exporterFailure;
    }
    if (!this.config.exporter) {
      return undefined;
    }
    let h = 0x811c9dc5;
    const mix = [...new TextEncoder().encode(label), ...context];
    for (const b of mix) {
      h = Math.imul(h ^ b, 0x01000193) >>> 0;
    }
    const out = new Uint8Array(length);
    for (let i = 0; i < length; i++) {
      out[i] =
        ((this.#secret[i % this.#secret.length] as number) ^
          ((h >>> ((i % 4) * 8)) & 0xff) ^
          i ^
          this.exporterXor) &
        0xff;
    }
    return out;
  }

  async openStream(): Promise<QuicStream> {
    if (this.openFailure !== undefined) {
      throw this.openFailure;
    }
    if (this.ended) {
      throw sessionEnded();
    }
    while (this.#openStreams >= this.peer.config.maxStreams) {
      const waiter = new Deferred<void>();
      this.#streamWaiters.push(waiter);
      await waiter.promise;
      if (this.ended) {
        throw sessionEnded();
      }
    }
    this.#openStreams++;
    const info: MemoryQuicStreamInfo = {
      ordinal: this.#nextOrdinal(),
      openedByInitiator: this.isInitiator,
    };
    const shared = new Shared(this);
    const local = new MemoryQuicStream(this, info, true);
    const remote = new MemoryQuicStream(this.peer, info, false);
    const out = new MemoryQuicDirection(this, this.peer, info, local, remote, shared);
    const back = new MemoryQuicDirection(this.peer, this, info, remote, local, shared);
    local.attach(out, back);
    remote.attach(back, out);
    this.streams.push(local);
    return local;
  }

  #nextOrdinal(): number {
    const ordinal = this.#ordinal;
    this.#ordinal = ordinal + 1;
    this.peer.#ordinal = ordinal + 1;
    return ordinal;
  }

  /** @internal The peer's stream became visible here (its first bytes, FIN or reset arrived). */
  announce(stream: MemoryQuicStream): void {
    if (stream.isAnnounced || this.ended) {
      return;
    }
    stream.isAnnounced = true;
    this.streams.push(stream);
    this.announced.push(stream.info.ordinal);
    this.#incoming.push(stream);
  }

  /** @internal A stream this end opened is released by the peer: credit for another. */
  streamReleased(): void {
    this.#openStreams--;
    this.#streamWaiters.shift()?.resolve();
  }

  /** @internal */
  get connectionCredit(): number {
    return this.peer.config.connectionWindow - (this.connectionSent - this.connectionConsumed);
  }

  /** @internal */
  retryBlocked(): void {
    const blocked = [...this.blocked];
    this.blocked.clear();
    for (const direction of blocked) {
      direction.pump();
    }
  }

  async close(code: number, reason: string): Promise<void> {
    if (this.ended) {
      return;
    }
    const cut = truncateUtf8(reason, 1024);
    this.closes.push([code, cut]);
    this.#end({ code, reason: cut, byPeer: false, lost: false });
    const other = this.peer;
    this.network.clock.setTimeout(() => {
      other.#end({ code, reason: cut, byPeer: true, lost: false });
    }, this.network.closeDelay);
  }

  /** Both ends lose the session (a QUIC error, the idle timeout). */
  lose(reason = 'lost'): void {
    const end: QuicSessionEnd = { code: undefined, reason, byPeer: false, lost: true };
    this.#end(end);
    this.peer.#end(end);
  }

  #end(end: QuicSessionEnd): void {
    if (this.ended) {
      return;
    }
    this.#done.resolve(end);
    for (const stream of this.streams) {
      stream.sessionEnded();
    }
    for (const waiter of this.#streamWaiters.splice(0)) {
      waiter.resolve();
    }
    this.#incoming.end();
    this.#unidirectional.end();
  }

  toString(): string {
    return `MemoryQuicSession(${this.isInitiator ? 'initiator' : 'acceptor'})`;
  }
}

/** What the two directions of a stream share: the opener's stream credit returns once both closed at their receivers. */
class Shared {
  readonly opener: MemoryQuicSession;
  closed = 0;

  constructor(opener: MemoryQuicSession) {
    this.opener = opener;
  }

  directionClosed(): void {
    if (++this.closed === 2) {
      this.opener.streamReleased();
    }
  }
}

interface QueuedWrite {
  readonly data: Uint8Array;
  readonly done: Deferred<void>;
}

/** @internal One direction of a stream: what [from] writes and [to] reads. */
export class MemoryQuicDirection {
  readonly from: MemoryQuicSession;
  readonly to: MemoryQuicSession;
  readonly info: MemoryQuicStreamInfo;
  readonly sender: MemoryQuicStream;
  readonly receiver: MemoryQuicStream;
  readonly shared: Shared;
  readonly window: number;

  // Sender side: writes queued in order, the head sent as the windows allow.
  sent = 0;
  consumed = 0;
  readonly #writes: QueuedWrite[] = [];
  #writeOffset = 0;
  finished = false;
  #finish: Deferred<void> | undefined;
  #finished: Promise<void> | undefined;
  resetSent = false;
  stopCode: number | undefined;
  stopReceived = false;
  readonly stopped = new Deferred<number | undefined>();
  readonly resetReceived = new Deferred<number | undefined>();

  // Receiver side.
  readonly buffer: Uint8Array[] = [];
  fin = false;
  finRead = false;
  reset = false;
  resetCode: number | undefined;
  /** This side stopped its read direction (STOP_SENDING) with {@link stopSentCode}. */
  stopSent = false;
  stopSentCode: number | undefined;
  read_: Deferred<Uint8Array | undefined> | undefined;
  #readMax = 0;
  closedAtReceiver = false;

  // Delivery, in order.
  readonly #events: [number, () => void][] = [];
  #timer: TimerHandle | undefined;

  constructor(
    from: MemoryQuicSession,
    to: MemoryQuicSession,
    info: MemoryQuicStreamInfo,
    sender: MemoryQuicStream,
    receiver: MemoryQuicStream,
    shared: Shared,
  ) {
    this.from = from;
    this.to = to;
    this.info = info;
    this.sender = sender;
    this.receiver = receiver;
    this.shared = shared;
    this.window = to.config.streamWindow;
  }

  get #clock(): Clock {
    return this.from.network.clock;
  }

  /** Runs [event] on the receiver's side after the delay, in order. */
  #post(event: () => void): void {
    const now = this.#clock.now();
    let due = now + this.from.network.delay(this.info, this.from.isInitiator);
    const last = this.#events[this.#events.length - 1];
    if (last !== undefined && last[0] > due) {
      due = last[0];
    }
    this.#events.push([due, event]);
    this.#arm();
  }

  #arm(): void {
    const first = this.#events[0];
    if (this.#timer !== undefined || first === undefined) {
      return;
    }
    const wait = first[0] - this.#clock.now();
    this.#timer = this.#clock.setTimeout(
      () => {
        this.#timer = undefined;
        const now = this.#clock.now();
        for (;;) {
          const head = this.#events[0];
          if (head === undefined || head[0] > now) {
            break;
          }
          if (this.to.ended || this.from.ended) {
            this.#events.length = 0;
            return;
          }
          this.#events.shift();
          head[1]();
        }
        this.#arm();
      },
      wait < 0 ? 0 : wait,
    );
  }

  // Writing ---------------------------------------------------------------

  write(bytes: Uint8Array): Promise<void> {
    if (this.from.ended) {
      return Promise.reject(sessionEnded());
    }
    if (this.stopReceived) {
      return Promise.reject(new QuicStreamStopped(this.stopCode));
    }
    if (this.finished || this.resetSent) {
      return Promise.reject(new Error('the direction has ended'));
    }
    const done = new Deferred<void>();
    // A copy, as the contract has it (a Buffer's own `slice` is a view).
    this.#writes.push({ data: Uint8Array.prototype.slice.call(bytes), done });
    this.pump();
    return done.promise;
  }

  /** Sends what the windows allow of the queued writes. */
  pump(): void {
    for (;;) {
      const head = this.#writes[0];
      if (head === undefined) {
        break;
      }
      if (this.from.ended || this.resetSent || this.stopReceived) {
        return;
      }
      const data = head.data;
      while (this.#writeOffset < data.length) {
        const streamCredit = this.window - (this.sent - this.consumed);
        const connectionCredit = this.from.connectionCredit;
        let n = data.length - this.#writeOffset;
        if (streamCredit < n) {
          n = streamCredit;
        }
        if (connectionCredit < n) {
          n = connectionCredit;
        }
        if (n <= 0) {
          if (connectionCredit <= 0) {
            this.from.blocked.add(this);
          }
          return;
        }
        const chunk = data.subarray(this.#writeOffset, this.#writeOffset + n);
        this.#writeOffset += n;
        this.sent += n;
        this.from.connectionSent += n;
        this.#post(() => this.#receive(chunk));
      }
      this.#writes.shift();
      this.#writeOffset = 0;
      head.done.resolve();
    }
    this.#postFin();
  }

  /** Fails the writes still queued with [error]. */
  #failWrites(error: unknown): void {
    const writes = this.#writes.splice(0);
    this.#writeOffset = 0;
    for (const w of writes) {
      w.done.reject(error);
    }
  }

  finish(): Promise<void> {
    if (this.from.ended) {
      return Promise.reject(sessionEnded());
    }
    if (this.stopReceived) {
      return Promise.reject(new QuicStreamStopped(this.stopCode));
    }
    if (this.resetSent) {
      return Promise.reject(new Error('the direction was reset'));
    }
    if (!this.finished) {
      this.finished = true;
      this.#finish = new Deferred<void>();
      this.#finished = this.#finish.promise;
      this.#postFin();
    }
    return this.#finished as Promise<void>;
  }

  /** FIN goes once every queued write went. */
  #postFin(): void {
    const finish = this.#finish;
    if (!this.finished || this.#writes.length > 0 || finish === undefined) {
      return;
    }
    this.#finish = undefined;
    this.#post(() => this.#receiveFin());
    finish.resolve();
  }

  resetDirection(code: number | undefined): void {
    if (this.resetSent || this.from.ended) {
      return;
    }
    if (this.finished && this.fin) {
      // Everything arrived already: the direction has ended.
      return;
    }
    this.resetSent = true;
    this.#failWrites(new Error('reset'));
    const finish = this.#finish;
    this.#finish = undefined;
    finish?.reject(new Error('reset'));
    this.stopped.resolve(undefined);
    this.#post(() => this.#receiveReset(code));
  }

  // Receiving -------------------------------------------------------------

  #receive(chunk: Uint8Array): void {
    this.to.announce(this.receiver);
    if (this.reset) {
      // Discarded: credit back at once.
      this.#credit(chunk.length);
      return;
    }
    if (this.stopSent) {
      // Discarded; a stopped stream gets no more stream credit (the sender
      // is to stop), only the connection's.
      this.#creditConnection(chunk.length);
      return;
    }
    this.buffer.push(chunk);
    this.#serveRead();
  }

  #receiveFin(): void {
    this.to.announce(this.receiver);
    this.fin = true;
    // Acknowledged.
    this.stopped.resolve(undefined);
    this.#serveRead();
    this.#checkClosed();
  }

  #receiveReset(code: number | undefined): void {
    this.to.announce(this.receiver);
    if (this.reset) {
      return;
    }
    this.reset = true;
    this.resetCode = code;
    this.resetReceived.resolve(code);
    let dropped = 0;
    for (const chunk of this.buffer) {
      dropped += chunk.length;
    }
    this.buffer.length = 0;
    if (dropped > 0) {
      this.#credit(dropped);
    }
    if (!this.stopSent) {
      const read = this.read_;
      this.read_ = undefined;
      read?.reject(new QuicStreamReset(code));
    }
    this.#checkClosed();
  }

  read(max: number): Promise<Uint8Array | undefined> {
    if (this.to.ended) {
      return Promise.reject(sessionEnded());
    }
    if (this.read_ !== undefined) {
      return Promise.reject(new Error('a read is pending'));
    }
    if (!Number.isInteger(max) || max <= 0) {
      return Promise.reject(new RangeError(`read of ${max} bytes`));
    }
    if (this.stopSent) {
      return Promise.reject(new QuicStreamReset(this.stopSentCode));
    }
    const read = new Deferred<Uint8Array | undefined>();
    this.read_ = read;
    this.#readMax = max;
    this.#serveRead();
    return read.promise;
  }

  #serveRead(): void {
    const read = this.read_;
    if (read === undefined) {
      return;
    }
    const head = this.buffer[0];
    if (head !== undefined) {
      let out: Uint8Array;
      if (head.length <= this.#readMax) {
        out = head;
        this.buffer.shift();
      } else {
        out = head.slice(0, this.#readMax);
        this.buffer[0] = head.subarray(this.#readMax);
      }
      this.read_ = undefined;
      this.#credit(out.length);
      read.resolve(out);
      return;
    }
    if (this.reset) {
      this.read_ = undefined;
      read.reject(new QuicStreamReset(this.resetCode));
      return;
    }
    if (this.fin) {
      this.read_ = undefined;
      this.finRead = true;
      read.resolve(undefined);
      this.#checkClosed();
    }
  }

  /** The receiver consumed [n] bytes: window back to the sender. */
  #credit(n: number): void {
    queueMicrotask(() => {
      this.consumed += n;
      this.from.connectionConsumed += n;
      this.pump();
      this.from.retryBlocked();
    });
  }

  /** The receiver discarded [n] bytes of a direction it stopped: window back to the connection only. */
  #creditConnection(n: number): void {
    queueMicrotask(() => {
      this.from.connectionConsumed += n;
      this.from.retryBlocked();
    });
  }

  /**
   * This side's read direction ends at once: a pending read and every later
   * one fail with a reset of [code]; STOP_SENDING goes to the sender, whose
   * confirming reset is not observable here.
   */
  stop(code: number | undefined): void {
    if (this.stopSent || this.reset || this.finRead || this.to.ended) {
      return;
    }
    this.stopSent = true;
    this.stopSentCode = code;
    let dropped = 0;
    for (const chunk of this.buffer) {
      dropped += chunk.length;
    }
    this.buffer.length = 0;
    if (dropped > 0) {
      // As a QUIC stack: no stream credit for a direction it stopped, so a
      // write stalled at the window fails with the STOP_SENDING rather than
      // completing first.
      this.#creditConnection(dropped);
    }
    const read = this.read_;
    this.read_ = undefined;
    read?.reject(new QuicStreamReset(code));
    // STOP_SENDING travels back to the sender.
    this.receiver.out.#post(() => this.#receiveStop(code));
    this.#checkClosed();
  }

  /** On the sender's side: the peer asked it to stop. */
  #receiveStop(code: number | undefined): void {
    if (this.stopReceived) {
      return;
    }
    this.stopReceived = true;
    this.stopCode = code;
    this.#failWrites(new QuicStreamStopped(code));
    const finish = this.#finish;
    this.#finish = undefined;
    finish?.reject(new QuicStreamStopped(code));
    if (code === undefined) {
      this.stopped.reject(new QuicStreamStopped(undefined));
    } else {
      this.stopped.resolve(code);
    }
    // The stack answers with a reset of the same code, unless everything
    // arrived already.
    if (!this.resetSent && !(this.finished && this.fin)) {
      this.resetSent = true;
      this.#post(() => this.#receiveReset(code));
    }
  }

  #checkClosed(): void {
    if (this.closedAtReceiver) {
      return;
    }
    if (
      this.reset ||
      (this.fin && (this.finRead || this.stopSent)) ||
      (this.stopSent && this.resetSent)
    ) {
      this.closedAtReceiver = true;
      this.shared.directionClosed();
    }
  }

  /** The sender's session ended: what it has pending fails. */
  senderEnded(): void {
    if (this.#timer !== undefined) {
      this.#clock.clearTimeout(this.#timer);
      this.#timer = undefined;
    }
    this.#events.length = 0;
    this.#failWrites(sessionEnded());
    const finish = this.#finish;
    this.#finish = undefined;
    finish?.reject(sessionEnded());
    this.stopped.reject(sessionEnded());
  }

  /** The receiver's session ended: its pending read fails. */
  receiverEnded(): void {
    this.resetReceived.reject(sessionEnded());
    const read = this.read_;
    this.read_ = undefined;
    read?.reject(sessionEnded());
  }
}

/** One end's view of an in-memory stream. */
export class MemoryQuicStream implements QuicStream {
  readonly session: MemoryQuicSession;
  readonly info: MemoryQuicStreamInfo;
  readonly locallyOpened: boolean;
  /** @internal */
  isAnnounced: boolean;
  #out: MemoryQuicDirection | undefined;
  #in: MemoryQuicDirection | undefined;

  /** The last priority set. */
  lastPriority = 0;
  /** Codes of the resets this end sent. */
  readonly resets: number[] = [];
  /** Codes of the stops this end sent. */
  readonly stops: number[] = [];
  /** While set, writes fail with it. */
  writeFailure: unknown;
  /** While set, finish fails with it. */
  finishFailure: unknown;

  constructor(session: MemoryQuicSession, info: MemoryQuicStreamInfo, locallyOpened: boolean) {
    this.session = session;
    this.info = info;
    this.locallyOpened = locallyOpened;
    this.isAnnounced = locallyOpened;
  }

  /** @internal */
  attach(out: MemoryQuicDirection, inbound: MemoryQuicDirection): void {
    this.#out = out;
    this.#in = inbound;
  }

  /** @internal This end's sending direction. */
  get out(): MemoryQuicDirection {
    return this.#out as MemoryQuicDirection;
  }

  /** @internal This end's receiving direction. */
  get inbound(): MemoryQuicDirection {
    return this.#in as MemoryQuicDirection;
  }

  /** Whether this end's direction ended with FIN. */
  get finished(): boolean {
    return this.out.finished;
  }

  read(max: number): Promise<Uint8Array | undefined> {
    return this.inbound.read(max);
  }

  /** Resets this end's direction without an application code (a WebTransport session going away). */
  resetWithoutCode(): void {
    this.out.resetDirection(undefined);
  }

  /** Stops the peer's direction without an application code. */
  stopWithoutCode(): void {
    this.inbound.stop(undefined);
  }

  /** Fails the pending read with [error]. */
  failRead(error: unknown): void {
    const reading = this.inbound.read_;
    this.inbound.read_ = undefined;
    reading?.reject(error);
  }

  write(bytes: Uint8Array): Promise<void> {
    return this.writeFailure === undefined
      ? this.out.write(bytes)
      : Promise.reject(this.writeFailure);
  }

  finish(): Promise<void> {
    return this.finishFailure === undefined
      ? this.out.finish()
      : Promise.reject(this.finishFailure);
  }

  reset(code: number): void {
    this.resets.push(code);
    this.out.resetDirection(code);
  }

  stop(code: number): void {
    this.stops.push(code);
    this.inbound.stop(code);
  }

  get priority(): number {
    return this.lastPriority;
  }

  set priority(value: number) {
    this.lastPriority = value;
  }

  get stopped(): Promise<number | undefined> {
    return this.out.stopped.promise;
  }

  get resetReceived(): Promise<number | undefined> {
    return this.inbound.resetReceived.promise;
  }

  /** @internal */
  sessionEnded(): void {
    this.#out?.senderEnded();
    this.#in?.receiverEnded();
  }

  toString(): string {
    return `MemoryQuicStream(#${this.info.ordinal}, ${this.locallyOpened ? 'local' : 'remote'})`;
  }
}

/** A peer-created unidirectional receive direction. */
export class MemoryQuicReceiveStream implements QuicReceiveStream {
  /** Resolves with the code of the first stop. */
  readonly stopped = new Deferred<number>();

  stop(code: number): void {
    this.stopped.resolve(code);
  }
}

/** [text] cut to at most [max] bytes of UTF-8, at a character boundary. */
function truncateUtf8(text: string, max: number): string {
  const bytes = new TextEncoder().encode(text);
  if (bytes.length <= max) {
    return text;
  }
  let cut = max;
  while (cut > 0 && ((bytes[cut] as number) & 0xc0) === 0x80) {
    cut--;
  }
  return new TextDecoder().decode(bytes.subarray(0, cut));
}
