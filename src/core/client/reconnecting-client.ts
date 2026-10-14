/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { ChannelAddress } from '../address/index.ts';
import { type PayloadLike, toPayload } from '../bytes.ts';
import {
  type MuxChannel,
  type MuxConnection,
  type MuxOptions,
  muxOptions,
} from '../mux/contract.ts';
import { ProtocolError, Status, StatusCode, SwitchboardError } from '../status.ts';
import { Broadcast } from '../switchboard/broadcast.ts';
import { coreRuntime } from '../switchboard/core-runtime.ts';
import { IncomingChannel } from '../switchboard/incoming-channel.ts';
import type { MuxFactory } from '../switchboard/runtime.ts';
import { type ServiceAddressLike, toServiceAddress } from '../switchboard/service-address-like.ts';
import {
  installBulkRouting,
  isBulkAddress,
  refuseBulk,
  talkOver,
} from '../switchboard/talk-layer.ts';
import { discardTransport } from '../switchboard/waits.ts';
import { type TalkChannel, type TalkOptions, talkOptions } from '../talk/contract.ts';
import type { FrameTransport, TransportConnector } from '../transport/contract.ts';
import { AsyncQueue } from '../util/async-queue.ts';
import { type Clock, systemClock, type TimerHandle } from '../util/clock.ts';
import { Deferred } from '../util/deferred.ts';
import { Emitter, type Unsubscribe } from '../util/emitter.ts';
import { type Logger, type LogLevel, noopLogger } from '../util/logger.ts';
import { Backoff } from './backoff.ts';
import {
  PersistentChannel,
  PersistentCore,
  type PersistentEntry,
  type PersistentHost,
  type PersistentKind,
  PersistentTalk,
} from './persistent-channel.ts';

const logName = 'Switchboard.Client';

/**
 * Establishes new connections to the endpoint through a binding of the
 * connection contract other than the frame mux (QUIC, or WebTransport in a
 * browser), for a {@link ReconnectingClient}. `connect` is called once per
 * attempt with the client's mux options and resolves with a connection
 * established as its initiator, whose `incoming` nobody reads: the client
 * owns dispatch and the connection's lifetime from then on. [init.signal]
 * aborts when the client gives the attempt up (its connect timeout, `stop`,
 * `close`); a connection that still arrives is closed.
 */
export interface ConnectionConnector {
  connect(options: MuxOptions, init: { readonly signal: AbortSignal }): Promise<MuxConnection>;
}

/** Phase of a {@link ReconnectingClient}'s connection. */
export const ClientPhase = Object.freeze({
  /** No connection: not started, stopped, closed, or waiting for the next attempt. */
  disconnected: 'disconnected',
  /** A connection attempt is in progress. */
  connecting: 'connecting',
  /** Connected: `connection` is the live connection. */
  connected: 'connected',
} as const);

/** One of the {@link ClientPhase} values. */
export type ClientPhase = (typeof ClientPhase)[keyof typeof ClientPhase];

/** What {@link ClientState} takes beside the phase. */
export interface ClientStateInit {
  readonly attempt?: number | undefined;
  readonly lastStatus?: Status | undefined;
  readonly since?: number | undefined;
  readonly nextAttemptAt?: number | undefined;
  /** The clock {@link ClientState.since} and {@link ClientState.nextAttemptAt} are read on; default the system clock. */
  readonly clock?: Clock | undefined;
}

/**
 * A snapshot of a {@link ReconnectingClient}'s connection, as reported by
 * `state` and `states`. Times are on the client's clock (monotonic
 * milliseconds, `performance.now()` by default), not wall-clock dates.
 */
export class ClientState {
  readonly phase: ClientPhase;

  /**
   * Connection attempts since the last connection was established (or
   * since `start`). While connecting, the attempt in progress, counting
   * from 1; while connected, the attempt that succeeded; while
   * disconnected, the attempts that failed so far: 0 right after a
   * connection ended or when stopped.
   */
  readonly attempt: number;

  /**
   * Why the last connection or attempt ended: the status the connection
   * ended with, the endpoint's GOAWAY status when the client left a
   * connection after it, GOING_AWAY when `reconnectNow({ force: true })`
   * left it, the failure of the last attempt, or CANCELLED when stopped or
   * closed. Undefined until something has ended; kept unchanged while
   * connecting and connected.
   */
  readonly lastStatus: Status | undefined;

  /** When this phase began, on the client's clock. */
  readonly since: number | undefined;

  /**
   * When the next attempt is due, on the client's clock: {@link since} plus
   * the backoff delay. Set only while disconnected with an attempt
   * scheduled; undefined while connecting or connected, and when stopped
   * or closed. The attempt is driven by a timer: it can come later than
   * this, for example when the application was suspended (call
   * `reconnectNow` on resume).
   */
  readonly nextAttemptAt: number | undefined;

  readonly #clock: Clock;

  constructor(phase: ClientPhase, init: ClientStateInit = {}) {
    this.phase = phase;
    this.attempt = init.attempt ?? 0;
    this.lastStatus = init.lastStatus;
    this.since = init.since;
    this.nextAttemptAt = init.nextAttemptAt;
    this.#clock = init.clock ?? systemClock;
    Object.freeze(this);
  }

  /**
   * Milliseconds left until {@link nextAttemptAt}, measured at each read
   * and never negative (0 once due), for a countdown; undefined when no
   * attempt is scheduled.
   */
  get retryIn(): number | undefined {
    const at = this.nextAttemptAt;
    return at === undefined ? undefined : Math.max(0, at - this.#clock.now());
  }

  /** Whether {@link phase} is `connected`. */
  get isConnected(): boolean {
    return this.phase === ClientPhase.connected;
  }

  toString(): string {
    const last = this.lastStatus !== undefined ? `, last: ${this.lastStatus}` : '';
    const next =
      this.nextAttemptAt !== undefined ? `, next attempt at ${this.nextAttemptAt} ms` : '';
    return `ClientState(${this.phase}, attempt ${this.attempt}${last}${next})`;
  }
}

/** The events of a {@link ReconnectingClient}. */
export type ClientEvents = {
  /** Every change of `state`. */
  state: ClientState;
};

/** A channel address, or a service address it is made from (`'feed'`, `'npc/1a2b'`). */
export type ChannelAddressLike = ChannelAddress | ServiceAddressLike;

function toChannelAddress(address: ChannelAddressLike): ChannelAddress {
  return address instanceof ChannelAddress ? address : ChannelAddress.to(toServiceAddress(address));
}

/** The options of a {@link ReconnectingClient}. Durations are milliseconds. */
export interface ReconnectingClientOptions {
  /** Mux configuration of every connection; the client's clock and logger unless it gives its own. */
  readonly muxOptions?: Partial<MuxOptions> | undefined;
  /** Default Talk configuration of the channels opened or accepted here. */
  readonly talkOptions?: Partial<TalkOptions> | undefined;
  /** Delay before the first retry after a failure. Default 500. */
  readonly initialBackoff?: number | undefined;
  /** Upper bound of the backoff before jitter, and the lifetime beyond which a connection (or persistent channel) resets it. Default 30 000. */
  readonly maxBackoff?: number | undefined;
  /** Growth of the backoff per consecutive failure. Default 2. */
  readonly backoffFactor?: number | undefined;
  /** Random spread of each delay, as a fraction of it. Default 0.2 (±20 %). */
  readonly jitter?: number | undefined;
  /** Longest time a connection attempt may take; 0 waits for ever. Default 10 000. */
  readonly connectTimeout?: number | undefined;
  /** Application payload of every channel opened with an empty one (typically the credential). */
  readonly defaultPayload?: PayloadLike;
  /** Whether the constructor calls `start`. Default true. */
  readonly autoStart?: boolean | undefined;
  /** Source of the jitter, numbers in [0, 1). Default `Math.random`. */
  readonly random?: (() => number) | undefined;
  /** Time and timers of the client and, unless `muxOptions` gives one, of its connections. */
  readonly clock?: Clock | undefined;
  /** Diagnostics, as `Switchboard.Client`; default discards. */
  readonly logger?: Logger | undefined;
  /** The frame mux run over the transports of a transport connector. Default: the library's. */
  readonly mux?: MuxFactory | undefined;
}

/** What {@link ReconnectingClient.openChannel} and {@link ReconnectingClient.openTalk} take. */
export interface ClientOpenOptions {
  /**
   * How long to wait for a usable connection, in milliseconds; then the
   * open fails with UNAVAILABLE. 0 fails at once instead of waiting.
   * Default {@link ReconnectingClient.defaultOpenTimeout}.
   */
  readonly openTimeout?: number | undefined;
  /** Abandons the wait for a connection; the open rejects with `signal.reason`. */
  readonly signal?: AbortSignal | undefined;
}

/** An {@link ReconnectingClient.openChannel} call waiting for a connection. */
class PendingOpen {
  readonly bytes: Uint8Array;
  readonly deferred = new Deferred<MuxChannel>();
  timer: TimerHandle | undefined;
  release: (() => void) | undefined;

  constructor(bytes: Uint8Array) {
    this.bytes = bytes;
  }
}

/**
 * One connection to one endpoint, kept up for a frontend application:
 * reconnects with exponential backoff after every loss, re-opens the
 * application's long-lived channels on every new connection, and delivers
 * the channels the endpoint pushes. Runtime neutral: browsers, Node, Bun
 * and Deno, over any transport connector.
 *
 * ```ts
 * const client = new ReconnectingClient(
 *   ({ signal }) => connectWebSocket('wss://example.com/sb', { signal }),
 *   { defaultPayload: credential },
 * );
 * client.on('state', showConnectionBanner);
 *
 * // One-off request: waits for the connection if needed.
 * const talk = await client.openTalk('profile');
 * const reply = await talk.request('GET');
 * await talk.close();
 *
 * // Subscription, set up again after every reconnect.
 * const feed = client.openPersistentTalk('feed', async (talk) => {
 *   void talk.serve(onFeedMessage);
 *   await talk.request('SUB', topic);
 * }, { onClosed: (status) => log(`feed closed: ${status}`) });
 *
 * // Channels the endpoint opens towards the client.
 * for await (const incoming of client.incoming) handlePush(incoming);
 *
 * await client.close();
 * ```
 *
 * Connecting: the connector is called (bounded by {@link connectTimeout})
 * and the frame mux runs over the transport as the initiator, or, given a
 * {@link ConnectionConnector}, the connector establishes the connection
 * through its own binding. Once connected, every persistent channel is
 * opened (in the order they were created) and pending {@link openChannel}
 * calls proceed.
 *
 * Reconnecting: when the connection ends, for any reason, the state becomes
 * disconnected with the end status, every persistent channel reports its
 * channel's end through its `onClosed`, and a new attempt is made after the
 * backoff.
 *
 * GOAWAY: when the endpoint sends GOAWAY with GOING_AWAY (it is restarting
 * or draining), the client moves to a new connection without waiting for
 * the old one to end: the state becomes connecting with the GOAWAY status
 * as `lastStatus`, the persistent channels on the old connection are closed
 * with GOING_AWAY (so that it goes idle and the endpoint can finish
 * promptly) and opened again on the new one, and waiting opens go to the
 * new one too. A planned restart is not a failure: the attempt is made at
 * once, and only if it fails does the backoff apply. Channels the
 * application opened on the old connection, and channels the endpoint
 * pushed there, finish there. A connection made after a GOAWAY that
 * receives GOAWAY in turn before lasting {@link maxBackoff} is followed by
 * the backoff instead. A GOAWAY carrying an error is not a restart: the
 * endpoint ends the connection, which is handled as a loss.
 *
 * Backoff: the first attempt after {@link start} is immediate. The delay
 * before attempt `n + 1` after `n` consecutive failures is
 * `min(initialBackoff * backoffFactor^n, maxBackoff)`, then multiplied by a
 * random factor in `[1 - jitter, 1 + jitter]`. A connection that ends
 * counts as a failure unless it lasted longer than {@link maxBackoff},
 * which resets the sequence. While waiting, `state.nextAttemptAt` says when
 * the next attempt is due, and {@link reconnectNow} makes it at once.
 */
export class ReconnectingClient implements AsyncDisposable {
  /** Default `openTimeout` of {@link openChannel} and {@link openTalk}: 30 s. */
  static readonly defaultOpenTimeout = 30_000;

  /** Mux configuration of every connection. */
  readonly muxOptions: MuxOptions;
  /** Default Talk configuration of the channels opened or accepted here. */
  readonly talkOptions: TalkOptions;
  /** Delay before the first retry after a failure. */
  readonly initialBackoff: number;
  /** Upper bound of the backoff before jitter, and the lifetime beyond which a connection resets it. */
  readonly maxBackoff: number;
  /** Growth of the backoff per consecutive failure. */
  readonly backoffFactor: number;
  /** Random spread of each delay, as a fraction of it. */
  readonly jitter: number;
  /** Longest time a connection attempt may take; 0: no bound. */
  readonly connectTimeout: number;
  /** Time and timers of the client. */
  readonly clock: Clock;
  /** Diagnostics of the client. */
  readonly logger: Logger;

  /**
   * Application payload attached to every channel opened with an empty
   * one, like `Switchboard.defaultPayload`. May be replaced at any time
   * (after a credential refresh, say); channels opened afterwards,
   * re-opened persistent channels included, carry the new value.
   */
  defaultPayload: Uint8Array;

  readonly #connectTransport: TransportConnector | undefined;
  readonly #connectConnection: ConnectionConnector | undefined;
  readonly #schedule: Backoff;
  readonly #mux: MuxFactory;
  readonly #events = new Emitter<ClientEvents>();
  readonly #states: Broadcast<ClientState>;
  readonly #incoming = new AsyncQueue<IncomingChannel>();
  readonly #pendingOpens = new Set<PendingOpen>();
  readonly #persistent: PersistentEntry[] = [];
  readonly #retiring = new Set<Promise<void>>();
  /** Connections left after the endpoint's GOAWAY that have not closed. */
  readonly #draining = new Set<MuxConnection>();
  readonly #host: PersistentHost;
  #state: ClientState;
  #connected = new Deferred<MuxConnection>();
  #connection: MuxConnection | undefined;
  #running = false;
  #closed = false;
  #incomingCancelled = false;
  #attempt = 0;
  #failures = 0;
  #attemptAbort: AbortController | undefined;
  #connectTimer: TimerHandle | undefined;
  #retryTimer: TimerHandle | undefined;
  #lifetimeTimer: TimerHandle | undefined;
  #livedLong = false;
  /** The last connection was made at once after a GOAWAY, and none has lasted maxBackoff since. */
  #restartedOnGoAway = false;
  #closing: Promise<void> | undefined;

  /**
   * Creates the client and, unless `autoStart` is false, calls
   * {@link start}, which calls the connector at once. [connector] is a
   * function giving a transport the frame mux runs over (such as
   * `({ signal }) => connectWebSocket(url, { signal })`), or a
   * {@link ConnectionConnector} giving connections of another binding.
   *
   * @throws {RangeError} if `initialBackoff` is negative, `maxBackoff` is
   * below it, `backoffFactor` is below 1 or not finite, `jitter` is
   * outside 0 to 1, or `connectTimeout` is negative.
   * @throws {TypeError} if [connector] is neither.
   */
  constructor(
    connector: TransportConnector | ConnectionConnector,
    options: ReconnectingClientOptions = {},
  ) {
    if (typeof connector === 'function') {
      this.#connectTransport = connector;
    } else if (typeof connector?.connect === 'function') {
      this.#connectConnection = connector;
    } else {
      throw new TypeError('connector must be a function or have a connect method');
    }
    this.initialBackoff = options.initialBackoff ?? 500;
    this.maxBackoff = options.maxBackoff ?? 30_000;
    this.backoffFactor = options.backoffFactor ?? 2;
    this.jitter = options.jitter ?? 0.2;
    this.#schedule = new Backoff(
      {
        initial: this.initialBackoff,
        max: this.maxBackoff,
        factor: this.backoffFactor,
        jitter: this.jitter,
      },
      options.random,
    );
    const connectTimeout = options.connectTimeout ?? 10_000;
    if (!(connectTimeout >= 0)) {
      throw new RangeError(`connectTimeout ${connectTimeout} must not be negative`);
    }
    this.connectTimeout = connectTimeout;
    const given = options.muxOptions ?? {};
    this.clock = options.clock ?? given.clock ?? systemClock;
    this.logger = options.logger ?? given.logger ?? noopLogger;
    this.muxOptions = muxOptions({
      ...given,
      clock: given.clock ?? this.clock,
      logger: given.logger ?? this.logger,
    });
    this.talkOptions = talkOptions(options.talkOptions);
    this.defaultPayload = toPayload(options.defaultPayload);
    this.#mux = options.mux ?? coreRuntime().mux;
    this.#state = new ClientState(ClientPhase.disconnected, {
      since: this.clock.now(),
      clock: this.clock,
    });
    this.#states = new Broadcast(() => this.#state);
    this.#incoming.onCancel = () => this.#onIncomingCancelled();
    this.#host = this.#persistentHost();
    if (options.autoStart ?? true) {
      this.start();
    }
  }

  // State ---------------------------------------------------------------

  /**
   * Every change of {@link state}: each `for await` first receives the
   * state current when it starts, then every change. Ends after
   * {@link close}.
   */
  get states(): AsyncIterable<ClientState> {
    return this.#states;
  }

  /** Calls [listener] with every change of {@link state} (not the current one). */
  on<K extends keyof ClientEvents & string>(
    event: K,
    listener: (value: ClientEvents[K]) => void,
  ): Unsubscribe {
    return this.#events.on(event, listener);
  }

  /** Like {@link on}, for the next change only. */
  once<K extends keyof ClientEvents & string>(
    event: K,
    listener: (value: ClientEvents[K]) => void,
  ): Unsubscribe {
    return this.#events.once(event, listener);
  }

  /** Removes a listener added with {@link on}. */
  off<K extends keyof ClientEvents & string>(
    event: K,
    listener: (value: ClientEvents[K]) => void,
  ): void {
    this.#events.off(event, listener);
  }

  /** The current state. */
  get state(): ClientState {
    return this.#state;
  }

  /** When the next attempt is due while waiting after a failure or a disconnect: `state.nextAttemptAt`. */
  get nextAttemptAt(): number | undefined {
    return this.#state.nextAttemptAt;
  }

  /**
   * The current connection while connected, else undefined. One that
   * receives GOAWAY with GOING_AWAY stops being current as soon as the
   * client handles it; one that receives a GOAWAY carrying an error stays
   * current until it ends.
   */
  get connection(): MuxConnection | undefined {
    return this.#connection;
  }

  /**
   * Resolves with the connection once connected: at once while connected,
   * else with the next connection. A new promise replaces it after every
   * disconnect and every GOAWAY. Rejects with CANCELLED when the client is
   * closed first.
   */
  get connected(): Promise<MuxConnection> {
    return this.#connected.promise;
  }

  /** Whether the client is started: connected, or connecting or waiting to. */
  get isRunning(): boolean {
    return this.#running;
  }

  /** Whether {@link close} was called. */
  get isClosed(): boolean {
    return this.#closed;
  }

  /**
   * Channels the endpoint opens towards the client, on whichever connection
   * is current, across reconnects. One consumer; buffered until taken.
   *
   * The consumer owns each channel, as with `Switchboard` dispatch, and
   * closes or rejects it. A channel whose OPEN payload is not a valid
   * address is closed with PROTOCOL_ERROR without being delivered. Once the
   * consumer stops iterating, or after {@link close}, further channels are
   * closed with UNAVAILABLE.
   */
  get incoming(): AsyncIterable<IncomingChannel> {
    return this.#incoming;
  }

  // Lifecycle -------------------------------------------------------------

  /**
   * Starts connecting, and reconnecting after every loss. Called by the
   * constructor unless `autoStart` is false. Calls the connector at once,
   * and resets the backoff. Does nothing if already started or closed.
   */
  start(): void {
    if (this.#closed || this.#running) {
      return;
    }
    this.#running = true;
    this.#failures = 0;
    this.#restartedOnGoAway = false;
    this.#attempt = 0;
    this.#log('debug', 'starting');
    this.#attemptConnect();
  }

  /**
   * Stops reconnecting and disconnects: abandons a connection attempt in
   * progress, closes the persistent channels with GOING_AWAY (they are
   * opened again after {@link start}), and sends GOAWAY on the connection,
   * and on an earlier one still held open by the application's channels
   * after the endpoint's GOAWAY; each then closes once the channels the
   * application opened are done, or after `muxOptions.goAwayGrace`.
   *
   * The state becomes disconnected with CANCELLED. Pending opens keep
   * waiting. Does nothing if not started. Never throws.
   */
  stop(): void {
    if (this.#running) {
      this.#halt('stopped');
    }
  }

  /**
   * Makes the next connection attempt now, for when waiting is pointless:
   * the application resumed, or the network came back.
   *
   * While waiting for the next attempt after a failure or a disconnect,
   * cancels the wait and attempts at once. The backoff sequence carries
   * on: if the attempt fails, the next delay is the one that would have
   * followed the cancelled attempt ({@link stop} then {@link start} resets
   * it).
   *
   * While an attempt is in progress, does nothing. While connected, does
   * nothing unless [options.force]; then the client leaves the connection
   * as it does after the endpoint's GOAWAY, for a connection that may be
   * stale: the state becomes connecting with GOING_AWAY as `lastStatus`,
   * the persistent channels move to the new connection, and the old one
   * gets GOAWAY. If the new attempt fails, the backoff applies.
   *
   * Does nothing if not started, or after {@link close}. Never throws.
   */
  reconnectNow(options: { force?: boolean } = {}): void {
    if (this.#closed || !this.#running) {
      return;
    }
    const retry = this.#retryTimer;
    if (retry !== undefined) {
      this.clock.clearTimeout(retry);
      this.#log('debug', 'connecting now');
      this.#attemptConnect();
      return;
    }
    const connection = this.#connection;
    if (connection === undefined || !options.force) {
      // An attempt is in progress, or connected.
      return;
    }
    this.#log('info', 'leaving the connection, reconnecting');
    const status = Status.of(StatusCode.goingAway, 'client reconnecting');
    this.#leave(connection);
    this.#retire(connection, status);
    this.#attemptConnect(status);
  }

  /**
   * Stops (as {@link stop}), closes every persistent channel for good,
   * rejects pending opens and {@link connected} with CANCELLED, and ends
   * {@link states} and {@link incoming}.
   *
   * Resolves once every connection of this client has closed (after
   * GOAWAY: when the channels the application opened are done, at most
   * `muxOptions.goAwayGrace`); no timer of the client is left then.
   * Calling it again returns the same promise. Never rejects.
   */
  close(): Promise<void> {
    this.#closing ??= this.#close();
    return this.#closing;
  }

  /** {@link close}. */
  [Symbol.asyncDispose](): Promise<void> {
    return this.close();
  }

  async #close(): Promise<void> {
    this.#closed = true;
    if (this.#running) {
      this.#halt('client closed');
    }
    const cancelled = SwitchboardError.of(StatusCode.cancelled, 'client closed');
    const pending = [...this.#pendingOpens];
    this.#pendingOpens.clear();
    for (const open of pending) {
      this.#failPending(open, cancelled);
    }
    this.#connected.reject(cancelled);
    // Untaken pushed channels would keep a retiring connection busy until
    // its GOAWAY grace ran out; refuse them before waiting on it.
    this.#refuseBuffered();
    this.#incoming.end();
    const persistent = [...this.#persistent].map((p) => p.close());
    try {
      await Promise.all(persistent);
      await Promise.all([...this.#retiring]);
    } catch (e) {
      this.#log('warn', 'error while closing', e);
    }
    this.#states.end();
    this.#log('debug', 'closed');
  }

  #halt(reason: string): void {
    this.#running = false;
    this.#clearTimer(this.#retryTimer);
    this.#retryTimer = undefined;
    this.#abandonAttempt();
    this.#clearTimer(this.#lifetimeTimer);
    this.#lifetimeTimer = undefined;
    const connection = this.#connection;
    this.#connection = undefined;
    this.#attempt = 0;
    if (this.#connected.isSettled) {
      this.#connected = new Deferred();
    }
    this.#log('debug', reason);
    this.#setState(ClientPhase.disconnected, Status.of(StatusCode.cancelled, reason));
    if (connection !== undefined) {
      this.#retire(connection, Status.of(StatusCode.goingAway, 'client stopping'));
    }
    for (const old of [...this.#draining]) {
      this.#goAway(old);
    }
    this.#draining.clear();
  }

  /** Closes the persistent channels on [connection] with [status] and sends GOAWAY on it. */
  #retire(connection: MuxConnection, status: Status): void {
    for (const p of [...this.#persistent]) {
      p.retire(connection, status);
    }
    this.#goAway(connection);
  }

  /** Sends GOAWAY on [connection]; {@link close} waits for it to close. */
  #goAway(connection: MuxConnection): void {
    let gone: Promise<void>;
    try {
      gone = connection.goAway();
    } catch (e) {
      this.#log('warn', 'GOAWAY failed', e);
      gone = connection.close();
    }
    const tracked = gone.then(
      () => {},
      () => {},
    );
    this.#retiring.add(tracked);
    void tracked.then(() => this.#retiring.delete(tracked));
  }

  /** [retryDelay] is the delay of the attempt scheduled, if any. */
  #setState(
    phase: ClientPhase,
    lastStatus: Status | undefined,
    init: { attempt?: number; retryDelay?: number } = {},
  ): void {
    const now = this.clock.now();
    this.#state = new ClientState(phase, {
      attempt: init.attempt ?? this.#attempt,
      lastStatus,
      since: now,
      nextAttemptAt: init.retryDelay === undefined ? undefined : now + init.retryDelay,
      clock: this.clock,
    });
    if (!this.#states.isEnded) {
      this.#states.push(this.#state);
      this.#events.emit('state', this.#state);
    }
  }

  // Connecting ------------------------------------------------------------

  /** Starts an attempt; [lastStatus] replaces `state.lastStatus`. */
  #attemptConnect(lastStatus?: Status): void {
    this.#retryTimer = undefined;
    if (!this.#running) {
      return;
    }
    this.#attempt++;
    const abort = new AbortController();
    this.#attemptAbort = abort;
    this.#setState(ClientPhase.connecting, lastStatus ?? this.#state.lastStatus);
    this.#log('debug', `connecting, attempt ${this.#attempt}`);
    const init = { signal: abort.signal };
    const connectConnection = this.#connectConnection;
    if (connectConnection !== undefined) {
      let attempt: Promise<MuxConnection>;
      try {
        attempt = Promise.resolve(connectConnection.connect(this.muxOptions, init));
      } catch (e) {
        attempt = Promise.reject(e);
      }
      this.#awaitAttempt(
        abort,
        attempt,
        (connection) => this.#onConnection(connection),
        (connection) => {
          this.#log('debug', 'closing a connection that arrived too late');
          void connection.close().catch(() => {});
        },
      );
      return;
    }
    let attempt: Promise<FrameTransport>;
    try {
      attempt = Promise.resolve((this.#connectTransport as TransportConnector)(init));
    } catch (e) {
      attempt = Promise.reject(e);
    }
    this.#awaitAttempt(
      abort,
      attempt,
      (transport) => this.#onTransport(transport),
      (transport) => {
        this.#log('debug', 'closing a transport that arrived too late');
        discardTransport(transport);
      },
    );
  }

  /**
   * Bounds the attempt of [abort] by {@link connectTimeout}, and hands what
   * [attempt] gives to [arrived], or to [late] once the attempt was given up.
   */
  #awaitAttempt<T>(
    abort: AbortController,
    attempt: Promise<T>,
    arrived: (value: T) => void,
    late: (value: T) => void,
  ): void {
    if (this.#attemptAbort === abort && this.connectTimeout > 0) {
      this.#connectTimer = this.clock.setTimeout(() => {
        this.#connectTimer = undefined;
        if (this.#attemptAbort === abort) {
          this.#attemptAbort = undefined;
          const error = SwitchboardError.of(
            StatusCode.unavailable,
            `cannot connect: timed out after ${this.connectTimeout} ms`,
          );
          abort.abort(error);
          this.#connectFailed(error);
        }
      }, this.connectTimeout);
    }
    attempt.then(
      (value) => {
        if (this.#attemptAbort !== abort) {
          late(value);
          return;
        }
        this.#attemptAbort = undefined;
        this.#clearTimer(this.#connectTimer);
        this.#connectTimer = undefined;
        arrived(value);
      },
      (error: unknown) => {
        if (this.#attemptAbort !== abort) {
          this.#log('debug', `abandoned connection attempt failed: ${error}`);
          return;
        }
        this.#attemptAbort = undefined;
        this.#clearTimer(this.#connectTimer);
        this.#connectTimer = undefined;
        this.#connectFailed(error);
      },
    );
  }

  /** Gives up the attempt in progress, if any: its signal aborts and what it still yields is discarded. */
  #abandonAttempt(): void {
    this.#clearTimer(this.#connectTimer);
    this.#connectTimer = undefined;
    const abort = this.#attemptAbort;
    this.#attemptAbort = undefined;
    abort?.abort(SwitchboardError.of(StatusCode.cancelled, 'connection attempt abandoned'));
  }

  #connectFailed(error: unknown): void {
    const status =
      error instanceof SwitchboardError
        ? error.status
        : Status.of(StatusCode.unavailable, `cannot connect: ${describe(error)}`);
    if (this.#attempt === 1) {
      this.#log('warn', `connection attempt failed: ${status}`);
    } else {
      this.#log('debug', `connection attempt ${this.#attempt} failed: ${status}`);
    }
    this.#retryLater(status);
  }

  /** Reports the state disconnected with [status], and the next attempt after the backoff. */
  #retryLater(status: Status): void {
    if (!this.#running) {
      this.#setState(ClientPhase.disconnected, status);
      return;
    }
    const delay = this.#backoff(this.#failures++);
    this.#log('debug', `next connection attempt in ${delay} ms`);
    this.#retryTimer = this.clock.setTimeout(() => this.#attemptConnect(), delay);
    this.#setState(ClientPhase.disconnected, status, { retryDelay: delay });
  }

  /** The delay after [failures] consecutive failures, with jitter. */
  #backoff(failures: number): number {
    return this.#schedule.delay(failures);
  }

  #onTransport(transport: FrameTransport): void {
    let connection: MuxConnection;
    try {
      connection = this.#mux(transport, this.muxOptions, true);
    } catch (e) {
      discardTransport(transport);
      this.#connectFailed(e);
      return;
    }
    this.#onConnection(connection);
  }

  /** [connection], established by the attempt, is the current one. */
  #onConnection(connection: MuxConnection): void {
    try {
      installBulkRouting(connection);
      void this.#readIncoming(connection);
    } catch (e) {
      void connection.close().catch(() => {});
      this.#connectFailed(e);
      return;
    }
    const attempt = this.#attempt;
    this.#attempt = 0;
    this.#connection = connection;
    this.#livedLong = false;
    this.#lifetimeTimer = this.clock.setTimeout(() => {
      this.#lifetimeTimer = undefined;
      this.#livedLong = true;
    }, this.maxBackoff);
    void connection.done.then((status) => this.#onConnectionDone(connection, status));
    void connection.peerGoAwayStatus.then((status) => this.#onPeerGoAway(connection, status));
    this.#log('info', 'connected');
    this.#setState(ClientPhase.connected, this.#state.lastStatus, { attempt });
    this.#connected.resolve(connection);
    // Callbacks may stop or close the client: check before each step.
    for (const p of [...this.#persistent]) {
      if (this.#usableConnection !== connection) {
        return;
      }
      p.connectionUp(connection);
    }
    this.#servePendingOpens(connection);
  }

  async #readIncoming(connection: MuxConnection): Promise<void> {
    try {
      for await (const channel of connection.incoming) {
        this.#onPeerChannel(channel);
      }
    } catch (e) {
      this.#log('debug', `reading channels from the endpoint failed: ${describe(e)}`);
    }
  }

  #onConnectionDone(connection: MuxConnection, status: Status): void {
    if (this.#connection !== connection) {
      // Retired by stop() or close(), or left after a GOAWAY.
      return;
    }
    this.#leave(connection);
    this.#log('info', `disconnected: ${status}`);
    this.#retryLater(status);
  }

  /** [connection], the current one, stops being current. */
  #leave(connection: MuxConnection): void {
    if (this.#connection !== connection) {
      return;
    }
    this.#connection = undefined;
    this.#clearTimer(this.#lifetimeTimer);
    this.#lifetimeTimer = undefined;
    if (this.#livedLong) {
      this.#failures = 0;
      this.#restartedOnGoAway = false;
    }
    if (this.#connected.isSettled) {
      this.#connected = new Deferred();
    }
  }

  /**
   * The endpoint sent GOAWAY on [connection]: unless it is an error, leaves
   * the connection to the application's channels on it and connects again
   * at once.
   */
  #onPeerGoAway(connection: MuxConnection, status: Status): void {
    if (this.#connection !== connection) {
      // Retired by stop() or close().
      return;
    }
    if (status.code !== StatusCode.goingAway) {
      // The endpoint is ending the connection: a loss once it has ended.
      this.#log('debug', `endpoint sent GOAWAY ${status}`);
      return;
    }
    this.#leave(connection);
    // Not closed here: the mux closes it once idle after the peer's GOAWAY.
    this.#draining.add(connection);
    void connection.done.then(() => this.#draining.delete(connection));
    const goingAway = Status.of(StatusCode.goingAway, 'endpoint going away');
    for (const p of [...this.#persistent]) {
      p.retire(connection, goingAway);
    }
    if (this.#restartedOnGoAway) {
      this.#log('info', `endpoint going away again: ${status}`);
      this.#retryLater(status);
      return;
    }
    this.#restartedOnGoAway = true;
    this.#log('info', `endpoint going away: ${status}, reconnecting`);
    this.#attemptConnect(status);
  }

  /** The current connection if new channels may be opened on it. */
  get #usableConnection(): MuxConnection | undefined {
    const connection = this.#connection;
    return connection?.isOpen && !connection.peerGoingAway ? connection : undefined;
  }

  #onPeerChannel(channel: MuxChannel): void {
    let address: ChannelAddress;
    try {
      address = ChannelAddress.decode(channel.openPayload);
    } catch (e) {
      const status =
        e instanceof ProtocolError ? e.status : Status.of(StatusCode.protocolError, describe(e));
      this.#log('warn', `channel ${channel.id} from the endpoint: ${status}`);
      closeQuietly(channel, status);
      return;
    }
    if (isBulkAddress(address)) {
      // The payload of a Talk message of a channel without a Talk layer.
      refuseBulk(channel);
      return;
    }
    if (this.#incomingCancelled || this.#incoming.isEnded) {
      this.#log('debug', `channel ${channel.id} from the endpoint: nobody listens`);
      closeQuietly(channel, Status.of(StatusCode.unavailable, 'not accepting channels'));
      return;
    }
    this.#incoming.push(new IncomingChannel(channel, { address, talkOptions: this.talkOptions }));
  }

  #onIncomingCancelled(): void {
    this.#incomingCancelled = true;
    this.#refuseBuffered();
  }

  /** Closes the pushed channels nobody took with UNAVAILABLE. */
  #refuseBuffered(): void {
    for (const incoming of this.#incoming.drain()) {
      closeQuietly(incoming.channel, Status.of(StatusCode.unavailable, 'not accepting channels'));
    }
  }

  // Opening channels ------------------------------------------------------

  #encode(address: ChannelAddress): Uint8Array {
    return (
      address.payload.length === 0 && this.defaultPayload.length > 0
        ? address.with({ payload: this.defaultPayload })
        : address
    ).encode();
  }

  /**
   * Opens a channel to [address] on the current connection, with
   * {@link defaultPayload} as the application payload when the address has
   * none. A service address (`'events'`, `'npc/1a2b'`) stands for the
   * channel address of that type and instance.
   *
   * While no connection is usable (disconnected, stopped, or the current
   * connection received GOAWAY), waits for the next connection, at most
   * [options.openTimeout]; then rejects with UNAVAILABLE. 0 rejects at once
   * instead of waiting.
   *
   * The channel belongs to the caller and is not re-opened: it ends with
   * its connection. A GOAWAY does not move it: it carries on over the old
   * connection until it ends there or the endpoint's grace period expires.
   * A rejection by the endpoint arrives as its close status (`done`).
   * Rejects like `MuxConnection.open` (RESOURCE_EXHAUSTED, say), with
   * CANCELLED if the client is closed while waiting, with FAILED_PRECONDITION
   * after {@link close}, and with `signal.reason` when [options.signal]
   * aborts the wait.
   */
  openChannel(address: ChannelAddressLike, options: ClientOpenOptions = {}): Promise<MuxChannel> {
    if (this.#closed) {
      return Promise.reject(closedError());
    }
    let bytes: Uint8Array;
    try {
      bytes = this.#encode(toChannelAddress(address));
    } catch (e) {
      return Promise.reject(e);
    }
    const signal = options.signal;
    if (signal?.aborted) {
      return Promise.reject(signal.reason);
    }
    const connection = this.#usableConnection;
    if (connection !== undefined) {
      try {
        return Promise.resolve(connection.open(bytes));
      } catch (e) {
        if (!(e instanceof SwitchboardError) || e.code !== StatusCode.failedPrecondition) {
          return Promise.reject(e);
        }
      }
    }
    const openTimeout = options.openTimeout ?? ReconnectingClient.defaultOpenTimeout;
    if (!(openTimeout > 0)) {
      return Promise.reject(SwitchboardError.of(StatusCode.unavailable, 'not connected'));
    }
    const pending = new PendingOpen(bytes);
    this.#pendingOpens.add(pending);
    pending.timer = this.clock.setTimeout(() => {
      pending.timer = undefined;
      if (this.#pendingOpens.delete(pending)) {
        this.#failPending(
          pending,
          SwitchboardError.of(StatusCode.unavailable, `not connected within ${openTimeout} ms`),
        );
      }
    }, openTimeout);
    if (signal !== undefined) {
      const onAbort = (): void => {
        if (this.#pendingOpens.delete(pending)) {
          this.#failPending(pending, signal.reason);
        }
      };
      signal.addEventListener('abort', onAbort, { once: true });
      pending.release = () => signal.removeEventListener('abort', onAbort);
    }
    return pending.deferred.promise;
  }

  #servePendingOpens(connection: MuxConnection): void {
    for (const pending of [...this.#pendingOpens]) {
      if (this.#usableConnection !== connection) {
        return;
      }
      if (!this.#pendingOpens.has(pending)) {
        continue;
      }
      let channel: MuxChannel;
      try {
        channel = connection.open(pending.bytes);
      } catch (e) {
        if (e instanceof SwitchboardError && e.code === StatusCode.failedPrecondition) {
          // The connection is going away; wait for the next one.
          return;
        }
        this.#pendingOpens.delete(pending);
        this.#failPending(pending, e);
        continue;
      }
      this.#pendingOpens.delete(pending);
      this.#settlePending(pending);
      pending.deferred.resolve(channel);
    }
  }

  #failPending(pending: PendingOpen, error: unknown): void {
    this.#settlePending(pending);
    pending.deferred.reject(error);
  }

  #settlePending(pending: PendingOpen): void {
    this.#clearTimer(pending.timer);
    pending.timer = undefined;
    pending.release?.();
    pending.release = undefined;
  }

  /**
   * {@link openChannel} wrapped in a Talk channel with [options.talkOptions]
   * (default: {@link talkOptions}). Waits and rejects like
   * {@link openChannel}.
   */
  async openTalk(
    address: ChannelAddressLike,
    options: ClientOpenOptions & { readonly talkOptions?: Partial<TalkOptions> | undefined } = {},
  ): Promise<TalkChannel> {
    const channel = await this.openChannel(address, options);
    try {
      return talkOver(channel, talkOptions({ ...this.talkOptions, ...options.talkOptions }));
    } catch (e) {
      closeQuietly(
        channel,
        e instanceof SwitchboardError ? e.status : Status.of(StatusCode.internal),
      );
      throw e;
    }
  }

  /**
   * A channel to [address] that is opened on every connection, for
   * subscriptions and other long-lived exchanges. See
   * {@link PersistentChannel}.
   *
   * [onOpen] is called with each new channel, starting with the first
   * connection (never synchronously from this call): read the channel and
   * send what sets it up there. [options.onClosed] is called with the end
   * status of each channel when it ends. Exceptions from either, and a
   * rejected promise from [onOpen], are logged and change nothing.
   *
   * @throws {SwitchboardError} FAILED_PRECONDITION after {@link close}.
   */
  openPersistent(
    address: ChannelAddressLike,
    onOpen: (channel: MuxChannel) => unknown,
    options: { readonly onClosed?: ((status: Status) => void) | undefined } = {},
  ): PersistentChannel {
    this.#checkNotClosed();
    const kind: PersistentKind<MuxChannel> = {
      name: 'PersistentChannel',
      wrap: (channel) => channel,
      close: (channel, status) => channel.close(status),
      doneOf: (channel) => channel.done,
    };
    const core = new PersistentCore(
      this.#host,
      toChannelAddress(address),
      kind,
      onOpen,
      options.onClosed,
    );
    this.#addPersistent(core);
    return new PersistentChannel(core);
  }

  /**
   * {@link openPersistent} with each channel wrapped in a Talk channel with
   * [options.talkOptions] (default: {@link talkOptions}). See
   * {@link PersistentTalk}. [onOpen] typically serves the channel's
   * messages and sends the subscription request again; [options.onClosed]
   * receives the end status of each Talk channel (`TalkChannel.done`).
   *
   * @throws {SwitchboardError} FAILED_PRECONDITION after {@link close}.
   */
  openPersistentTalk(
    address: ChannelAddressLike,
    onOpen: (talk: TalkChannel) => unknown,
    options: {
      readonly talkOptions?: Partial<TalkOptions> | undefined;
      readonly onClosed?: ((status: Status) => void) | undefined;
    } = {},
  ): PersistentTalk {
    this.#checkNotClosed();
    const resolved = talkOptions({ ...this.talkOptions, ...options.talkOptions });
    const kind: PersistentKind<TalkChannel> = {
      name: 'PersistentTalk',
      wrap: (channel) => talkOver(channel, resolved),
      close: (talk, status) => talk.close(status),
      doneOf: (talk) => talk.done,
    };
    const core = new PersistentCore(
      this.#host,
      toChannelAddress(address),
      kind,
      onOpen,
      options.onClosed,
    );
    this.#addPersistent(core);
    return new PersistentTalk(core);
  }

  #addPersistent(core: PersistentEntry): void {
    this.#persistent.push(core);
    // Never call onOpen before the caller has the handle.
    queueMicrotask(() => {
      const connection = this.#usableConnection;
      if (connection !== undefined) {
        core.connectionUp(connection);
      }
    });
  }

  #checkNotClosed(): void {
    if (this.#closed) {
      throw closedError();
    }
  }

  /** The view of this client its persistent channels work through. */
  #persistentHost(): PersistentHost {
    const client = this;
    return {
      clock: this.clock,
      maxBackoff: this.maxBackoff,
      encode: (address) => this.#encode(address),
      backoff: (failures) => this.#backoff(failures),
      get connection() {
        return client.#connection;
      },
      get usableConnection() {
        return client.#usableConnection;
      },
      remove: (core) => {
        const index = this.#persistent.indexOf(core);
        if (index >= 0) {
          this.#persistent.splice(index, 1);
        }
      },
      log: (level, message, error) => this.#log(level, message, error),
    };
  }

  #clearTimer(timer: TimerHandle | undefined): void {
    if (timer !== undefined) {
      this.clock.clearTimeout(timer);
    }
  }

  #log(level: LogLevel, message: string, error?: unknown): void {
    this.logger.log(level, logName, message, error);
  }

  toString(): string {
    return `ReconnectingClient(${this.#state.phase})`;
  }
}

function closedError(): SwitchboardError {
  return SwitchboardError.of(StatusCode.failedPrecondition, 'client closed');
}

function closeQuietly(channel: MuxChannel, status: Status): void {
  try {
    void channel.close(status).catch(() => {});
  } catch {
    // Closing already.
  }
}

function describe(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
