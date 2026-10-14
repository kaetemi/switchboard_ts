/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { ChannelAddress, ServiceAddress } from '../address/index.ts';
import { type PayloadLike, toPayload } from '../bytes.ts';
import { Backoff } from '../client/backoff.ts';
import type { MuxChannel, MuxConnection } from '../mux/contract.ts';
import { type Name, type NameLike, toName } from '../name.ts';
import { serviceRecordsEqual, serviceRecordToString } from '../naming/naming-protocol.ts';
import { Status, StatusCode, SwitchboardError } from '../status.ts';
import { type TalkOptions, talkOptions } from '../talk/contract.ts';
import type { TalkChannel } from '../talk/talk-channel.ts';
import type { Clock, TimerHandle } from '../util/clock.ts';
import { Deferred } from '../util/deferred.ts';
import { Emitter, type Unsubscribe } from '../util/emitter.ts';
import type { Logger, LogLevel } from '../util/logger.ts';
import { Broadcast } from './broadcast.ts';
import type { ChannelPolicy, Resolver, ServiceEvent, ServiceRecord } from './contract.ts';
import type { Switchboard } from './switchboard.ts';
import { talkOver } from './talk-layer.ts';
import { bounded } from './waits.ts';

const logName = 'Switchboard.Client';

/** Where a {@link Peer} of a {@link PeerSet} stands. */
export const PeerState = Object.freeze({
  /** Just known, no connection attempt made yet. */
  added: 'added',
  /**
   * A connection attempt is in progress: dialling the record's endpoints,
   * or connected and running the `onConnect` hook and setting up the
   * per-peer channel (`Peer.connection` is set then); or, on a live
   * connection, re-opening the per-peer channel.
   */
  connecting: 'connecting',
  /**
   * Connected and set up: `Peer.connection` is live, its hooks succeeded
   * and the per-peer channel (if any) is open.
   */
  online: 'online',
  /**
   * Not usable: not connected, or connected with the per-peer channel
   * closed. The next attempt (a connection, or the channel on the live
   * connection) is due at `Peer.nextAttemptAt`.
   */
  offline: 'offline',
  /**
   * The record left the resolver less than the set's removal hold-down
   * ago: the connection is kept, in case the record comes back (a naming
   * service or registrar restart). Not counted in `PeerSet.online`.
   */
  held: 'held',
  /** Gone from the resolver, or the set was closed. Final. */
  removed: 'removed',
} as const);

/** One of the {@link PeerState} values. */
export type PeerState = (typeof PeerState)[keyof typeof PeerState];

/** What a {@link PeerEvent} reports. */
export const PeerEventType = Object.freeze({
  /** A record of the type appeared in the resolver: a new {@link Peer}. */
  added: 'added',
  /** The peer became {@link PeerState.online}. */
  online: 'online',
  /**
   * The peer stopped being online (its connection was lost, the peer sent
   * GOAWAY, its per-peer channel ended or was refused, a hook failed), or
   * its first connection attempt failed. Reported once until the peer is
   * online again.
   */
  offline: 'offline',
  /**
   * The resolver's record of the peer changed (`Peer.record`): other
   * endpoints, or other metadata.
   */
  updated: 'updated',
  /**
   * The record left the resolver: the peer is {@link PeerState.held} for
   * the set's removal hold-down, then removed, unless its record comes back
   * first (then `online` or `offline` follows, as the peer stands).
   */
  held: 'held',
  /**
   * The record left the resolver (after the hold-down), or the set was
   * closed. The last event of the peer.
   */
  removed: 'removed',
} as const);

/** One of the {@link PeerEventType} values. */
export type PeerEventType = (typeof PeerEventType)[keyof typeof PeerEventType];

/** A change of a {@link PeerSet}, on `PeerSet.events` and its `event` listeners. */
export class PeerEvent {
  /** What happened. */
  readonly type: PeerEventType;

  /**
   * The peer it happened to: a live view, which may have changed again by
   * the time the event is read from `PeerSet.events`.
   */
  readonly peer: Peer;

  /**
   * For {@link PeerEventType.offline}, why the peer went offline (as
   * `Peer.lastStatus` was then); for {@link PeerEventType.removed},
   * CANCELLED when the set was closed. Undefined otherwise.
   */
  readonly status: Status | undefined;

  /** Creates an event. */
  constructor(type: PeerEventType, peer: Peer, status?: Status) {
    this.type = type;
    this.peer = peer;
    this.status = status;
    Object.freeze(this);
  }

  toString(): string {
    const status = this.status !== undefined ? `, ${this.status}` : '';
    return `PeerEvent(${this.type} ${this.peer.address}${status})`;
  }
}

/** The events of a {@link PeerSet}. */
export type PeerSetEvents = {
  /** Every addition, state change and removal of a peer, in order. */
  event: PeerEvent;
};

/** The options of {@link PeerSet.watch}. Durations are milliseconds. */
export interface PeerSetOptions {
  /** Where the instances are watched; default the node's `resolver`. */
  readonly resolver?: Resolver | undefined;
  /**
   * Runs after every (re)connect of a peer, before the peer is reported
   * online; `Peer.connection` is set then, and `PeerSet.openChannel` and
   * `PeerSet.openTalk` work for the peer, for a handshake.
   */
  readonly onConnect?: ((peer: Peer) => unknown) | undefined;
  /**
   * The address of a channel opened to every peer on every (re)connect,
   * after `onConnect`: its instance, when 0, is set to the peer's instance
   * id, and an empty application payload is replaced by the node's payload
   * for the peer (`Switchboard.payloadFor`, so `credentialFor` with the
   * peer's record).
   */
  readonly channel?: ChannelAddress | undefined;
  /** Runs with each per-peer channel opened; `Peer.channel` holds it while it is open. Needs [channel]. */
  readonly onOpen?: ((peer: Peer, channel: MuxChannel) => unknown) | undefined;
  /** Delay before the first retry after a failure. Default 500. */
  readonly initialBackoff?: number | undefined;
  /** Upper bound of the backoff before jitter, and the lifetime beyond which a connection (or per-peer channel) resets it. Default 30 000. */
  readonly maxBackoff?: number | undefined;
  /** Growth of the backoff per consecutive failure. Default 2. */
  readonly backoffFactor?: number | undefined;
  /** Random spread of each delay, as a fraction of it. Default 0.2 (±20 %). */
  readonly jitter?: number | undefined;
  /**
   * Bound on each connection attempt (dialling the endpoints one after the
   * other) and each step of the set-up (each hook, credential selection,
   * the PING round trip); 0 waits for ever. Default 10 000.
   */
  readonly connectTimeout?: number | undefined;
  /**
   * How long a peer whose record left the resolver is kept
   * ({@link PeerState.held}) before it is removed; 0 removes it at once.
   * Default 15 000.
   */
  readonly removalHoldDown?: number | undefined;
  /**
   * The policy of the set's connections, overriding the node's
   * `endpointPolicy` and `outgoingPolicy`.
   */
  readonly policy?: ChannelPolicy | undefined;
  /** Source of the jitter, numbers in [0, 1). Default `Math.random`. */
  readonly random?: (() => number) | undefined;
  /** Time and timers of the set; default the node's clock. */
  readonly clock?: Clock | undefined;
  /** Diagnostics, as `Switchboard.Client`; default the node's logger. */
  readonly logger?: Logger | undefined;
}

/** What {@link PeerSet.openChannel} takes. */
export interface PeerOpenOptions {
  /**
   * The application payload; default the node's payload for the peer
   * (`Switchboard.payloadFor` with its record).
   */
  readonly payload?: PayloadLike;
}

/**
 * The answer a {@link Switchboard} gives about the reachable instances of
 * one type, kept connected: one connection per instance the resolver
 * knows, re-established with backoff after every loss.
 *
 * For a consumer that keeps a connection to every worker of a fleet (the
 * wiki page "Polyverse Switchboard Use Cases", entry 5) and load-balances
 * over the ones that are online with its own policy.
 *
 * ```ts
 * const fleet = PeerSet.watch(node, 'gpu', {
 *   channel: new ChannelAddress({ type: 'gpu' }),
 *   onOpen: async (peer, channel) => {
 *     // The capability exchange, again after every reconnect.
 *     const talk = await fleet.openTalk(peer.address.instance);
 *     capabilities.set(peer.address.instance, await talk.request('CAPS'));
 *   },
 * });
 * fleet.on('event', (event) => log(`${event}`));
 * await fleet.ready;
 * const worker = pick(fleet.online);
 * const talk = await fleet.openTalk(worker.address.instance);
 * ```
 *
 * Membership is the resolver's records of the type: the records
 * `Resolver.resolve` returns once the resolver is `ready`, with the events
 * that arrived while it answered folded in, then its `events`. A record
 * that appears is added and connected. A record that changes is updated;
 * if its endpoints changed, the peer moves to the new ones. A record that
 * leaves is held for {@link removalHoldDown} ({@link PeerState.held}): the
 * peer's connection stays up, and if a record with the same address comes
 * back in time at the endpoint the peer is connected to (or the peer is not
 * connected), the peer is adopted again as it stands, connection and
 * channel included. Otherwise the peer is removed: its connection is sent
 * GOAWAY (the channels the application opened on it get the grace period)
 * and the peer ends in {@link PeerState.removed}. The hold-down exists
 * because a naming service or a registrar that restarts makes every record
 * of the fleet disappear for a moment; without it every job in flight would
 * be cut after the GOAWAY grace period.
 *
 * The resolver's events are read as an async iterable. A `next()` that
 * rejects is logged and the set asks for the next event, as a Dart stream
 * goes on after an error event; one that rejects again right after (an
 * iterator that ended with an error) ends the following of events, and so
 * does the end of the iteration. The peers stay as they are then.
 *
 * Connections: the set owns one connection per peer, established with
 * `Switchboard.dial` and never pooled, so that a worker that reboots
 * disturbs nothing else, and a backoff is kept per peer. The node's
 * outgoing policy rules apply to it ({@link policy}, else the node's
 * `endpointPolicy`, else its `outgoingPolicy`), and the node's
 * `credentialFor` chooses the payload of the channels opened on it. Each
 * attempt dials the record's endpoints in order and keeps the first that
 * answers, within {@link connectTimeout}: each endpoint gets the time left
 * divided by the endpoints left, so that one that does not answer at all
 * leaves time for the next.
 *
 * A record without endpoints is reached through the naming service's
 * `CONNECT` instead (`Switchboard.broker`; the resolver must be a
 * `BrokeringResolver`, as a naming resolver is): the peer dials this node
 * and the connection that arrives, accepted on the node's listener and
 * under that listener's policy (not {@link policy}), is the peer's, with no
 * `Peer.endpoint`. After every loss the set brokers again, with the same
 * backoff, hooks and online rules.
 *
 * On a node that cannot broker (`Switchboard.canBroker` false: it listens
 * nowhere) but has a `Switchboard.relay`, a record without endpoints is
 * reached through a relay instead (wiki page "Polyverse Switchboard
 * Identity and Credentials", section "Relay", "Staying connected"): each
 * attempt dials the relays `Switchboard.resolveRelays` gives, in that order
 * and within {@link connectTimeout} as for a record's endpoints, naming
 * each relay's identity from its record when it identifies, and keeps the
 * first that answers as the peer's own connection (`Peer.viaRelay`,
 * `Peer.endpoint` the relay's endpoint). The per-peer {@link channel} and
 * the channels of {@link openChannel} are opened on it as relay channels
 * carrying the peer's open payload. A connection to a relay that is lost,
 * or that the relay sends GOAWAY on, is replaced as any other, through
 * whichever relay resolves next; a per-peer channel that ends while the
 * peer is online is opened again on the same relay connection after the
 * backoff, and when that fails (the relay cannot reach the peer) the
 * connection is left and the next attempt goes through the relays again.
 * The PING round trip of the set-up is answered by the relay, before the
 * peer has seen the channel: for a relayed peer, only an {@link onOpen}
 * that exchanges something with the peer confirms that the channel reached
 * it, so give relayed peers a per-peer channel and an {@link onOpen} that
 * does. A node that can neither broker nor relay fails each attempt as
 * brokering does.
 *
 * A peer is online only once it is usable: once connected, `onConnect`
 * runs, then the per-peer {@link channel} (if any) is opened, a PING round
 * trip on the connection makes sure the peer did not refuse it on arrival,
 * and `onOpen` runs with it; only then is the peer
 * {@link PeerState.online}. A hook that throws, rejects or does not
 * complete within {@link connectTimeout}, and a per-peer channel the peer
 * refuses or closes before the peer is online (with any status), fail the
 * attempt: the set leaves the connection with GOAWAY and the peer is
 * offline (with INTERNAL for a hook, the channel's close status for the
 * channel) until the next attempt after the backoff. The hooks run again
 * after every reconnect. A hook may close the set, at once or later; its
 * timeout is cancelled with the connection.
 *
 * The per-peer channel behaves like a `PersistentChannel` of a
 * `ReconnectingClient`: when it ends while the connection stays up, the
 * peer goes offline with its close status and the channel is set up again
 * on that connection after a backoff kept per peer (reset by a channel that
 * lasted longer than {@link maxBackoff}), with the same PING round trip and
 * `onOpen`, bounded the same way; a re-open that fails is tried again after
 * the backoff, and the peer is online again once one succeeds. A status
 * that will not change on its own (PERMISSION_DENIED, UNAUTHENTICATED,
 * UNIMPLEMENTED, NOT_FOUND) fails the connection instead: GOAWAY, offline
 * with that status until the next connection attempt after the backoff.
 *
 * Backoff, as for `ReconnectingClient`: the first attempt is immediate;
 * after `n` consecutive failures the delay is
 * `min(initialBackoff * backoffFactor^n, maxBackoff)` times a random factor
 * in `[1 - jitter, 1 + jitter]`. A connection that ends counts as a failure
 * unless the peer went online on it and it lasted longer than
 * {@link maxBackoff}. A GOAWAY GOING_AWAY from the peer (a worker shutting
 * down or rebooting) is not waited out: the peer goes offline, its channel
 * on the old connection is closed with GOING_AWAY, the old connection is
 * left to the application's channels on it, and a new attempt is made at
 * once (with the backoff only if it fails, or if the connection was itself
 * made after a GOAWAY and did not last {@link maxBackoff}).
 *
 * Events are delivered to the `event` listeners synchronously, as they
 * happen, and to each consumer of {@link events} in order.
 *
 * Close the set before the {@link Switchboard}: once the node is closed
 * every attempt fails and the set keeps retrying.
 */
export class PeerSet implements AsyncDisposable {
  /** The node the connections are made through. */
  readonly switchboard: Switchboard;
  /** The service type watched; also the type of {@link openChannel}. */
  readonly type: Name;
  /** The resolver whose records of {@link type} are the members. */
  readonly resolver: Resolver;
  /** Runs after every (re)connect of a peer, before it is online. */
  readonly onConnect: ((peer: Peer) => unknown) | undefined;
  /** The per-peer channel's address, or undefined for none. */
  readonly channel: ChannelAddress | undefined;
  /** Runs with every per-peer channel opened. */
  readonly onOpen: ((peer: Peer, channel: MuxChannel) => unknown) | undefined;
  /** Delay before the first retry after a failure. */
  readonly initialBackoff: number;
  /** Upper bound of the backoff before jitter, and the lifetime beyond which a connection (or per-peer channel) resets it. */
  readonly maxBackoff: number;
  /** Growth of the backoff per consecutive failure. */
  readonly backoffFactor: number;
  /** Random spread of each delay, as a fraction of it. */
  readonly jitter: number;
  /** Bound on each connection attempt and each step of the set-up; 0 for none. */
  readonly connectTimeout: number;
  /** How long a peer whose record left the resolver is held before it is removed; 0 for not at all. */
  readonly removalHoldDown: number;
  /** The policy of the set's connections, or undefined for the node's. */
  readonly policy: ChannelPolicy | undefined;
  /** Time and timers of the set. */
  readonly clock: Clock;
  /** Diagnostics of the set. */
  readonly logger: Logger;

  readonly #host: PeerSetHost;
  /** The peers by instance id, in the order they were added; {@link peers} is the view of it. */
  readonly #cores = new Map<number, PeerCore>();
  readonly #view = new Map<number, Peer>();
  readonly #emitter = new Emitter<PeerSetEvents>();
  readonly #events = new Broadcast<PeerEvent>();
  readonly #ready = new Deferred<void>();
  #subscription: AsyncIterator<ServiceEvent> | undefined;

  /**
   * Connection attempts in progress, GOAWAYs sent, connections left after
   * the peer's GOAWAY: what {@link close} waits for.
   */
  readonly #dials = new Set<Promise<void>>();
  readonly #retiring = new Set<Promise<void>>();
  readonly #draining = new Set<MuxConnection>();
  #closed = false;
  #closing: Promise<void> | undefined;

  private constructor(
    switchboard: Switchboard,
    type: Name,
    resolver: Resolver,
    options: PeerSetOptions,
  ) {
    this.switchboard = switchboard;
    this.type = type;
    this.resolver = resolver;
    this.onConnect = options.onConnect;
    this.channel = options.channel;
    this.onOpen = options.onOpen;
    this.initialBackoff = options.initialBackoff ?? 500;
    this.maxBackoff = options.maxBackoff ?? 30_000;
    this.backoffFactor = options.backoffFactor ?? 2;
    this.jitter = options.jitter ?? 0.2;
    this.connectTimeout = options.connectTimeout ?? 10_000;
    this.removalHoldDown = options.removalHoldDown ?? 15_000;
    this.policy = options.policy;
    this.clock = options.clock ?? switchboard.clock;
    this.logger = options.logger ?? switchboard.logger;
    const schedule = new Backoff(
      {
        initial: this.initialBackoff,
        max: this.maxBackoff,
        factor: this.backoffFactor,
        jitter: this.jitter,
      },
      options.random,
    );
    this.#host = {
      set: this,
      schedule,
      emit: (event) => this.#emit(event),
      retire: (connection) => this.#retire(connection),
      drain: (connection) => this.#drain(connection),
      track: (attempt) => this.#track(attempt),
      expire: (core) => this.#expire(core),
      log: (level, message, error) => this.#log(level, message, error),
    };
  }

  /**
   * Watches the instances of [type] in [options.resolver] (default: the
   * node's `resolver`) and keeps a connection to each, through
   * [switchboard]. Starts at once; {@link ready} resolves once the initial
   * records are added. See {@link PeerSetOptions} for the hooks, the
   * per-peer channel and the schedule.
   *
   * Logs a warning when no policy applies to the set's connections (no
   * `policy`, and the node has neither an outgoing nor an endpoint policy:
   * every peer may open channels to the node's services), when the node's
   * `defaultPayload` is not empty and it has no `credentialFor` (every peer
   * receives the node's credential), and when the node identifies with a
   * bearer `credential` on every connection it initiates (every peer
   * receives that credential in `IDENT`).
   *
   * @throws {TypeError} when there is no resolver, and when `onOpen` is
   * given without `channel`.
   * @throws {RangeError} for a negative `removalHoldDown`, and for a backoff
   * schedule `ReconnectingClient` would refuse (`initialBackoff` negative,
   * `maxBackoff` below it, `backoffFactor` below 1 or not finite, `jitter`
   * outside 0 to 1).
   */
  static watch(switchboard: Switchboard, type: NameLike, options: PeerSetOptions = {}): PeerSet {
    const resolver = options.resolver ?? switchboard.resolver;
    if (resolver === undefined) {
      throw new TypeError('no resolver given and the switchboard has none');
    }
    if (options.onOpen !== undefined && options.channel === undefined) {
      throw new TypeError('onOpen needs a channel');
    }
    const removalHoldDown = options.removalHoldDown ?? 15_000;
    if (!(removalHoldDown >= 0)) {
      throw new RangeError(`removalHoldDown ${removalHoldDown} must not be negative`);
    }
    const set = new PeerSet(switchboard, toName(type), resolver, options);
    if (
      set.policy === undefined &&
      switchboard.outgoingPolicy === undefined &&
      switchboard.endpointPolicy === undefined
    ) {
      set.#log(
        'warn',
        `peer set ${set.type}: no policy applies to its connections, so every peer may open ` +
          'channels to the services of this node; give the set a policy, or the node an ' +
          'outgoingPolicy or endpointPolicy',
      );
    }
    if (switchboard.credentialFor === undefined && switchboard.defaultPayload.length > 0) {
      set.#log(
        'warn',
        `peer set ${set.type}: the node has a defaultPayload and no credentialFor, so every ` +
          "peer receives the node's credential",
      );
    }
    const identity = switchboard.credential;
    if (
      identity?.isBearer === true &&
      switchboard.identifyFor === undefined &&
      switchboard.identifyOutgoing
    ) {
      set.#log(
        'warn',
        `peer set ${set.type}: the node identifies with a bearer credential on every ` +
          'connection it initiates, so every peer receives it; restrict identification with ' +
          'identifyFor',
      );
    }
    set.#start();
    return set;
  }

  /**
   * The peers by instance id, in the order they were added, held ones
   * included. A live, read-only view; empty after {@link close}.
   */
  get peers(): ReadonlyMap<number, Peer> {
    return this.#view;
  }

  /** The peers that are {@link PeerState.online}, in the order they were added. */
  get online(): Peer[] {
    return [...this.#view.values()].filter((peer) => peer.isOnline);
  }

  /**
   * Additions, state changes and removals of peers, in order: each
   * `for await` sees the events from the moment it starts. Ends after
   * {@link close}, which reports every peer removed first.
   */
  get events(): AsyncIterable<PeerEvent> {
    return this.#events;
  }

  /** Calls [listener] with every event, synchronously, as it happens. */
  on<K extends keyof PeerSetEvents & string>(
    event: K,
    listener: (value: PeerSetEvents[K]) => void,
  ): Unsubscribe {
    return this.#emitter.on(event, listener);
  }

  /** Like {@link on}, for the next event only. */
  once<K extends keyof PeerSetEvents & string>(
    event: K,
    listener: (value: PeerSetEvents[K]) => void,
  ): Unsubscribe {
    return this.#emitter.once(event, listener);
  }

  /** Removes a listener added with {@link on}. */
  off<K extends keyof PeerSetEvents & string>(
    event: K,
    listener: (value: PeerSetEvents[K]) => void,
  ): void {
    this.#emitter.off(event, listener);
  }

  /**
   * Resolves once the records the resolver had when it became ready are
   * added (their connection attempts started). Rejects with the resolver's
   * error if it cannot be read (the set still follows its events), and
   * with CANCELLED if the set is closed first.
   */
  get ready(): Promise<void> {
    return this.#ready.promise;
  }

  /** Whether {@link close} was called. */
  get isClosed(): boolean {
    return this.#closed;
  }

  #start(): void {
    // Until the initial records are read: the events of the type. Those
    // that came before resolve() was called are in its answer and are
    // dropped; those that come while it answers are folded into it.
    const buffered: ServiceEvent[] = [];
    let initial = true;
    const onEvent = (event: ServiceEvent): void => {
      if (!event.record.address.type.equals(this.type) || this.#closed) {
        return;
      }
      if (initial) {
        buffered.push(event);
      } else {
        this.#apply(event);
      }
    };
    try {
      const iterator = this.resolver.events[Symbol.asyncIterator]();
      this.#subscription = iterator;
      void this.#follow(iterator, onEvent);
    } catch (e) {
      this.#log('warn', `peer set ${this.type}: cannot read the resolver's events`, e);
    }
    const load = async (): Promise<void> => {
      let failed = false;
      let error: unknown;
      let records: readonly ServiceRecord[] = [];
      try {
        await this.resolver.ready;
        if (this.#closed) {
          return;
        }
        buffered.length = 0;
        records = await this.resolver.resolve(this.type);
        if (this.#closed) {
          return;
        }
      } catch (e) {
        if (this.#closed) {
          return;
        }
        this.#log('warn', `peer set ${this.type}: cannot read the resolver`, e);
        failed = true;
        error = e;
      }
      // The answer may or may not include the events that came meanwhile;
      // UP replaces a record and DOWN removes it, so applying all of them
      // to the answer, in order, gives the resolver's current table. Each
      // record of it is added once; one that came and went never is.
      const table = new Map<number, ServiceRecord>();
      for (const record of records) {
        if (record.address.type.equals(this.type)) {
          table.set(record.address.instance, record);
        }
      }
      for (const event of buffered) {
        const instance = event.record.address.instance;
        if (event.up) {
          table.set(instance, event.record);
        } else {
          table.delete(instance);
        }
      }
      buffered.length = 0;
      initial = false;
      for (const record of table.values()) {
        this.#upsert(record);
      }
      if (failed) {
        this.#ready.reject(error);
      } else {
        this.#ready.resolve();
      }
    };
    void load().catch((e: unknown) => {
      this.#log('warn', `peer set ${this.type}: adding the initial records failed`, e);
      this.#ready.reject(e);
    });
  }

  /**
   * Hands the events of [iterator] to [onEvent] until it ends or the set
   * closes. A rejected `next()` is logged and the next event asked for; a
   * second rejection in a row ends the following.
   */
  async #follow(
    iterator: AsyncIterator<ServiceEvent>,
    onEvent: (event: ServiceEvent) => void,
  ): Promise<void> {
    let failed = false;
    for (;;) {
      let result: IteratorResult<ServiceEvent>;
      try {
        result = await iterator.next();
      } catch (e) {
        if (this.#closed) {
          return;
        }
        if (failed) {
          this.#log(
            'warn',
            `peer set ${this.type}: resolver events failed again; no longer following them`,
            e,
          );
          return;
        }
        // An error event; the set goes on, as it does on a Dart stream.
        failed = true;
        this.#log('warn', `peer set ${this.type}: resolver event error`, e);
        continue;
      }
      if (result.done === true || this.#closed) {
        return;
      }
      failed = false;
      try {
        onEvent(result.value);
      } catch (e) {
        this.#log('warn', `peer set ${this.type}: applying a resolver event failed`, e);
      }
    }
  }

  #apply(event: ServiceEvent): void {
    if (this.#closed) {
      return;
    }
    if (event.up) {
      this.#upsert(event.record);
      return;
    }
    const instance = event.record.address.instance;
    const core = this.#cores.get(instance);
    if (core === undefined || core.isHeld) {
      return;
    }
    if (this.removalHoldDown > 0) {
      core.hold();
    } else {
      this.#delete(instance);
      core.remove(undefined);
    }
  }

  #upsert(record: ServiceRecord): void {
    if (this.#closed) {
      // An event listener closed the set while the initial records were added.
      return;
    }
    const instance = record.address.instance;
    const existing = this.#cores.get(instance);
    if (existing?.isHeld) {
      if (existing.readopt(record)) {
        return;
      }
      // Elsewhere now: the held peer goes, a new one comes.
      this.#delete(instance);
      existing.remove(undefined);
    } else if (existing !== undefined) {
      if (!serviceRecordsEqual(existing.record, record)) {
        existing.update(record);
      }
      return;
    }
    const core = new PeerCore(this.#host, record);
    this.#cores.set(instance, core);
    this.#view.set(instance, core.peer);
    this.#log('debug', `peer set ${this.type}: ${record.address} added`);
    this.#emit(new PeerEvent(PeerEventType.added, core.peer));
    core.attemptConnect();
  }

  #delete(instance: number): void {
    this.#cores.delete(instance);
    this.#view.delete(instance);
  }

  /** The hold-down of [core] ran out: it goes. */
  #expire(core: PeerCore): void {
    const instance = core.address.instance;
    if (this.#cores.get(instance) === core) {
      this.#delete(instance);
    }
    core.remove(undefined);
  }

  #emit(event: PeerEvent): void {
    if (!this.#events.isEnded) {
      this.#events.push(event);
      this.#emitter.emit('event', event);
    }
  }

  // Connections -------------------------------------------------------------

  /** Runs [attempt], a connection attempt that never rejects, as work {@link close} waits for. */
  #track(attempt: Promise<void>): void {
    const tracked = attempt.then(
      () => {},
      (e: unknown) => this.#log('warn', `peer set ${this.type}: connection attempt failed`, e),
    );
    this.#dials.add(tracked);
    void tracked.then(() => this.#dials.delete(tracked));
  }

  /** Sends GOAWAY on [connection], which this set owns; {@link close} waits for it to close. */
  #retire(connection: MuxConnection): void {
    this.#draining.delete(connection);
    let gone: Promise<void>;
    try {
      gone = connection.goAway();
    } catch (e) {
      this.#log('warn', 'GOAWAY failed', e);
      try {
        gone = connection.close();
      } catch {
        gone = Promise.resolve();
      }
    }
    const tracked = gone.then(
      () => {},
      () => {},
    );
    this.#retiring.add(tracked);
    void tracked.then(() => this.#retiring.delete(tracked));
  }

  /** Keeps [connection], left after the peer's GOAWAY, until it closes on its own; {@link close} sends it GOAWAY. */
  #drain(connection: MuxConnection): void {
    this.#draining.add(connection);
    void connection.done.then(() => this.#draining.delete(connection));
  }

  // Opening channels --------------------------------------------------------

  /**
   * Opens a channel to `(type, instance)` on the connection of the peer
   * [instance], with [options.payload] as the application payload
   * (default: the node's payload for the peer, `Switchboard.payloadFor`
   * with its record).
   *
   * The channel belongs to the caller and ends with that connection; it is
   * not moved when the peer reconnects. A rejection by the peer arrives as
   * its close status (`done`).
   *
   * Rejects with `SwitchboardError`: NOT_FOUND if the set has no such
   * peer; FAILED_PRECONDITION if the peer has no usable connection (not
   * connected, or its connection received GOAWAY; it works while the peer
   * is connecting with its hooks running, while its per-peer channel is
   * being re-opened, and while it is held) or the set is closed; and like
   * `MuxConnection.open` and the node's `credentialFor`. Without a payload,
   * the credential is chosen for the peer's connection and record as they
   * are at the call; when either changes before it is chosen (the peer
   * reconnected, moved or was removed, or the set closed), the open fails
   * with FAILED_PRECONDITION and nothing is sent, so a credential chosen
   * for one destination never reaches another. Opening again once the peer
   * is usable chooses one for its new connection.
   */
  async openChannel(instance: number, options: PeerOpenOptions = {}): Promise<MuxChannel> {
    if (this.#closed) {
      throw SwitchboardError.of(StatusCode.failedPrecondition, 'peer set closed');
    }
    const core = this.#cores.get(instance);
    if (core === undefined) {
      throw SwitchboardError.of(
        StatusCode.notFound,
        `no peer ${new ServiceAddress(this.type, instance)}`,
      );
    }
    const connection = core.usable;
    if (connection === undefined) {
      throw SwitchboardError.of(StatusCode.failedPrecondition, `${core.address} is ${core.state}`);
    }
    const record = core.record;
    const application =
      options.payload !== undefined
        ? toPayload(options.payload)
        : await this.switchboard.payloadFor(connection, { record });
    if (connection !== core.usable || record !== core.record) {
      throw SwitchboardError.of(
        StatusCode.failedPrecondition,
        `${core.address} changed while choosing its credential`,
      );
    }
    return connection.open(
      PeerCore.wrap(
        new ChannelAddress({ type: this.type, instance, payload: application }),
        core.relayType,
      ).encode(),
    );
  }

  /**
   * {@link openChannel} wrapped in a Talk channel with [options.talkOptions]
   * over the node's `talkOptions`. Rejects like {@link openChannel}; when
   * the Talk layer cannot take the channel, closes it and rejects with that
   * error.
   */
  async openTalk(
    instance: number,
    options: PeerOpenOptions & { readonly talkOptions?: Partial<TalkOptions> | undefined } = {},
  ): Promise<TalkChannel> {
    const channel = await this.openChannel(instance, { payload: options.payload });
    try {
      return talkOver(
        channel,
        talkOptions({ ...this.switchboard.talkOptions, ...options.talkOptions }),
      );
    } catch (e) {
      closeQuietly(
        channel,
        e instanceof SwitchboardError ? e.status : Status.of(StatusCode.internal),
        this.#host,
      );
      throw e;
    }
  }

  // Shutdown ----------------------------------------------------------------

  /**
   * Stops watching, removes every peer (each reported
   * {@link PeerEventType.removed} with CANCELLED, its per-peer channel
   * closed and its connection sent GOAWAY), rejects {@link ready} if it has
   * not settled, and ends {@link events}.
   *
   * Resolves once every connection of the set has closed (the channels the
   * application opened get the GOAWAY grace period) and the dials in
   * progress have settled; no timer of the set is left then. Does not close
   * the resolver or the node. Calling it again returns the same promise.
   * Never rejects.
   */
  close(): Promise<void> {
    this.#closing ??= this.#close();
    return this.#closing;
  }

  /** {@link close}, for `await using`. */
  [Symbol.asyncDispose](): Promise<void> {
    return this.close();
  }

  async #close(): Promise<void> {
    this.#closed = true;
    const subscription = this.#subscription;
    this.#subscription = undefined;
    try {
      // Not awaited: an async generator answers return() only once the
      // next() it is waiting on settles.
      void Promise.resolve(subscription?.return?.()).catch(() => {});
    } catch {
      // The iterator could not stop; nothing reads it any more.
    }
    const cancelled = Status.of(StatusCode.cancelled, 'peer set closed');
    for (const core of [...this.#cores.values()]) {
      core.remove(cancelled);
    }
    this.#cores.clear();
    this.#view.clear();
    for (const connection of [...this.#draining]) {
      this.#retire(connection);
    }
    this.#ready.reject(new SwitchboardError(cancelled));
    try {
      // A connection that lands now is sent GOAWAY at once. A dial given
      // up on is tracked when it is given up on, so look again.
      while (this.#dials.size > 0) {
        await Promise.all([...this.#dials]);
      }
      await Promise.all([...this.#retiring]);
    } catch (e) {
      this.#log('warn', 'error while closing the peer set', e);
    }
    this.#events.end();
    this.#log('debug', `peer set ${this.type} closed`);
  }

  #log(level: LogLevel, message: string, error?: unknown): void {
    this.logger.log(level, logName, message, error);
  }

  toString(): string {
    return `PeerSet(${this.type}, ${this.#cores.size} peers)`;
  }
}

/**
 * What the state machine of a peer needs from its {@link PeerSet}.
 * Internal.
 */
export interface PeerSetHost {
  readonly set: PeerSet;
  /** The backoff schedule of the set's peers. */
  readonly schedule: Backoff;
  emit(event: PeerEvent): void;
  /** Sends GOAWAY on a connection of the set; `close` waits for it. */
  retire(connection: MuxConnection): void;
  /** Keeps a connection left after the peer's GOAWAY until it closes. */
  drain(connection: MuxConnection): void;
  /** Runs a connection attempt that never rejects as work `close` waits for. */
  track(attempt: Promise<void>): void;
  /** The hold-down of a peer ran out. */
  expire(core: PeerCore): void;
  log(level: LogLevel, message: string, error?: unknown): void;
}

/** Close statuses of the per-peer channel that a new attempt on the same connection would get again. */
function isTerminal(status: Status): boolean {
  switch (status.code) {
    case StatusCode.permissionDenied:
    case StatusCode.unauthenticated:
    case StatusCode.unimplemented:
    case StatusCode.notFound:
      return true;
    default:
      return false;
  }
}

function isPromiseLike(value: unknown): value is PromiseLike<unknown> {
  return (
    typeof value === 'object' &&
    value !== null &&
    typeof (value as { then?: unknown }).then === 'function'
  );
}

function describe(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function sameEndpoints(a: readonly string[], b: readonly string[]): boolean {
  if (a.length !== b.length) {
    return false;
  }
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) {
      return false;
    }
  }
  return true;
}

/** Closes [channel] with [status]; never throws or rejects (a failure is logged). */
function closeQuietly(channel: MuxChannel, status: Status, host: PeerSetHost): void {
  try {
    void channel
      .close(status)
      .catch((e: unknown) => host.log('debug', `closing a channel failed: ${describe(e)}`));
  } catch (e) {
    host.log('debug', `closing a channel failed: ${describe(e)}`);
  }
}

/**
 * The state machine of one peer of a {@link PeerSet}: its record, its
 * connection, its per-peer channel, its backoff and its hold-down.
 * {@link Peer} is its public view. Internal.
 */
export class PeerCore {
  /** The public view of this peer. */
  readonly peer: Peer;
  readonly #host: PeerSetHost;
  readonly #set: PeerSet;
  readonly #clock: Clock;
  #record: ServiceRecord;
  #state: PeerState = PeerState.added;
  #lastStatus: Status | undefined;
  #since: number;
  #nextAttemptAt: number | undefined;
  #attempt = 0;
  #endpoint: string | undefined;

  /** The type of the relay channels while {@link #connection} is a connection to a relay. */
  #relayType: Name | undefined;
  #connection: MuxConnection | undefined;
  #channel: MuxChannel | undefined;

  /** Whether the per-peer channel was set up (its PING round trip and `onOpen` done); false while it is being set up. */
  #channelUp = false;

  /** The last event of the online, offline and held kind reported (undefined: none yet). */
  #reported: PeerEventType | undefined;
  #removed = false;

  /** The connection attempt in progress (dialling), else undefined. */
  #token: object | undefined;
  #failures = 0;

  /** Whether the peer went online on the current connection (set-up done once): only then does the connection count as a success. */
  #established = false;
  #livedLong = false;
  #restartedOnGoAway = false;
  #retryTimer: TimerHandle | undefined;
  #lifetimeTimer: TimerHandle | undefined;
  #hookTimer: TimerHandle | undefined;
  #hookWait: Deferred<Status | undefined> | undefined;
  #channelFailures = 0;
  #channelLivedLong = false;
  #channelRetry: TimerHandle | undefined;
  #channelLifetime: TimerHandle | undefined;
  #holdTimer: TimerHandle | undefined;
  #heldSince: number | undefined;

  constructor(host: PeerSetHost, record: ServiceRecord) {
    this.#host = host;
    this.#set = host.set;
    this.#clock = host.set.clock;
    this.#record = record;
    this.#since = this.#clock.now();
    this.peer = new Peer(this);
  }

  // The view ----------------------------------------------------------------

  get address(): ServiceAddress {
    return this.#record.address;
  }

  get record(): ServiceRecord {
    return this.#record;
  }

  get state(): PeerState {
    return this.#holdTimer !== undefined ? PeerState.held : this.#state;
  }

  get lastStatus(): Status | undefined {
    return this.#lastStatus;
  }

  get since(): number {
    return this.#heldSince ?? this.#since;
  }

  get nextAttemptAt(): number | undefined {
    return this.#holdTimer !== undefined || this.#state !== PeerState.offline
      ? undefined
      : this.#nextAttemptAt;
  }

  get attempt(): number {
    return this.#attempt;
  }

  get endpoint(): string | undefined {
    return this.#endpoint;
  }

  get relayType(): Name | undefined {
    return this.#relayType;
  }

  get connection(): MuxConnection | undefined {
    return this.#connection;
  }

  get channel(): MuxChannel | undefined {
    return this.#channel;
  }

  get isHeld(): boolean {
    return this.#holdTimer !== undefined;
  }

  /** The connection, if channels may be opened on it. */
  get usable(): MuxConnection | undefined {
    const connection = this.#connection;
    return connection?.isOpen && !connection.peerGoingAway ? connection : undefined;
  }

  // State -------------------------------------------------------------------

  #setState(state: PeerState, lastStatus: Status | undefined, retryDelay?: number): void {
    const now = this.#clock.now();
    this.#state = state;
    this.#lastStatus = lastStatus;
    this.#since = now;
    this.#nextAttemptAt = retryDelay === undefined ? undefined : now + retryDelay;
    switch (state) {
      case PeerState.online:
        this.#report(PeerEventType.online, undefined);
        break;
      case PeerState.offline:
        this.#report(PeerEventType.offline, lastStatus);
        break;
      case PeerState.connecting:
        if (this.#reported === PeerEventType.online) {
          this.#report(PeerEventType.offline, lastStatus);
        }
        break;
      default:
    }
  }

  #report(type: PeerEventType, status: Status | undefined): void {
    if (this.#reported === type || this.#holdTimer !== undefined) {
      // While held, what happens to the connection is reported when the
      // peer is adopted again.
      return;
    }
    this.#reported = type;
    this.#host.emit(new PeerEvent(type, this.peer, status));
  }

  /** The resolver's record of the peer changed to [record]. */
  update(record: ServiceRecord): void {
    const previous = this.#record;
    this.#record = record;
    this.#log('debug', `${this}: record updated: ${serviceRecordToString(record)}`);
    this.#host.emit(new PeerEvent(PeerEventType.updated, this.peer));
    if (sameEndpoints(previous.endpoints, record.endpoints)) {
      return;
    }
    // Other endpoints: the failures so far say nothing about them.
    this.#failures = 0;
    const connection = this.#connection;
    if (connection !== undefined) {
      const endpoint = this.#endpoint;
      if (endpoint === undefined || !record.endpoints.includes(endpoint)) {
        this.#log('info', `${this}: endpoint ${endpoint ?? ''} withdrawn, moving`);
        const status = Status.of(StatusCode.goingAway, 'endpoints changed');
        this.#leave(connection, status);
        this.#host.retire(connection);
        this.attemptConnect(status);
      }
    } else if (this.#token !== undefined) {
      // Dialling the old endpoints: start over with the new ones.
      this.#token = undefined;
      this.attemptConnect();
    } else if (this.#retryTimer !== undefined) {
      this.#clock.clearTimeout(this.#retryTimer);
      this.#retryTimer = undefined;
      this.attemptConnect();
    }
  }

  /** The record left the resolver: held for the set's hold-down, its connection kept. */
  hold(): void {
    const holdDown = this.#set.removalHoldDown;
    this.#log('debug', `${this}: record gone, held for ${holdDown} ms`);
    this.#heldSince = this.#clock.now();
    this.#reported = PeerEventType.held;
    // Armed before the event, so that a listener sees the peer held.
    this.#holdTimer = this.#clock.setTimeout(() => {
      this.#holdTimer = undefined;
      this.#heldSince = undefined;
      this.#log('debug', `${this}: not back within ${holdDown} ms`);
      this.#host.expire(this);
    }, holdDown);
    this.#host.emit(new PeerEvent(PeerEventType.held, this.peer));
  }

  /**
   * A record of the held peer came back: adopted again if it can still
   * reach the peer where it is connected (or it is not connected). Returns
   * false, leaving the peer held, when the record is elsewhere.
   */
  readopt(record: ServiceRecord): boolean {
    const endpoint = this.#endpoint;
    const elsewhere =
      this.#relayType !== undefined
        ? record.endpoints.length > 0
        : endpoint !== undefined && !record.endpoints.includes(endpoint);
    if (elsewhere) {
      return false;
    }
    if (this.#holdTimer !== undefined) {
      this.#clock.clearTimeout(this.#holdTimer);
    }
    this.#holdTimer = undefined;
    this.#heldSince = undefined;
    this.#since = this.#clock.now();
    this.#log('debug', `${this}: record back, adopted again`);
    switch (this.#state) {
      case PeerState.online:
        this.#report(PeerEventType.online, undefined);
        break;
      case PeerState.offline:
        this.#report(PeerEventType.offline, this.#lastStatus);
        break;
      default:
      // Connecting: the outcome is reported.
    }
    if (!serviceRecordsEqual(record, this.#record)) {
      this.update(record);
    }
    return true;
  }

  /** The record is gone (undefined) or the set closes ([status]). */
  remove(status: Status | undefined): void {
    if (this.#removed) {
      return;
    }
    this.#removed = true;
    if (this.#holdTimer !== undefined) {
      this.#clock.clearTimeout(this.#holdTimer);
    }
    this.#holdTimer = undefined;
    this.#heldSince = undefined;
    this.#token = undefined;
    if (this.#retryTimer !== undefined) {
      this.#clock.clearTimeout(this.#retryTimer);
    }
    this.#retryTimer = undefined;
    const connection = this.#connection;
    if (connection !== undefined) {
      this.#leave(connection, Status.of(StatusCode.goingAway, 'peer removed'));
      this.#host.retire(connection);
    }
    this.#log('debug', `${this}: removed`);
    this.#state = PeerState.removed;
    this.#since = this.#clock.now();
    this.#nextAttemptAt = undefined;
    this.#host.emit(new PeerEvent(PeerEventType.removed, this.peer, status));
  }

  // Connecting --------------------------------------------------------------

  /** Starts an attempt; [lastStatus] replaces `lastStatus`. */
  attemptConnect(lastStatus?: Status): void {
    this.#retryTimer = undefined;
    if (this.#removed) {
      return;
    }
    this.#attempt++;
    const token = {};
    this.#token = token;
    this.#setState(PeerState.connecting, lastStatus ?? this.#lastStatus);
    this.#host.track(this.#dialFirst(token));
  }

  /**
   * Dials the endpoints of the record in order; the first that answers is
   * the connection. With `connectTimeout`, each endpoint gets the time left
   * divided by the endpoints left. A record without endpoints is brokered
   * instead, or reached through a relay by a node that cannot broker and
   * has one.
   */
  async #dialFirst(token: object): Promise<void> {
    if (this.#token !== token) {
      // An event listener removed the peer as the attempt began.
      return;
    }
    const record = this.#record;
    if (record.endpoints.length === 0) {
      const switchboard = this.#set.switchboard;
      if (switchboard.relay !== undefined && !switchboard.canBroker) {
        await this.#relayConnection(token);
      } else {
        await this.#brokerConnection(token);
      }
      return;
    }
    await this.#dialEach(
      token,
      record.endpoints.map((endpoint) => [endpoint, record] as const),
      false,
    );
  }

  /**
   * Dials the relays of the node in the order it gives them
   * (`Switchboard.resolveRelays`), as {@link #dialFirst} dials a record's
   * endpoints.
   */
  async #relayConnection(token: object): Promise<void> {
    let relays: ServiceRecord[];
    try {
      relays = await this.#set.switchboard.resolveRelays({ resolver: this.#set.resolver });
    } catch (e) {
      if (this.#token !== token) {
        return;
      }
      this.#token = undefined;
      this.#connectFailed(
        e instanceof SwitchboardError
          ? e.status
          : Status.of(StatusCode.unavailable, `no relay: ${describe(e)}`),
      );
      return;
    }
    await this.#dialEach(
      token,
      relays.flatMap((relay) => relay.endpoints.map((endpoint) => [endpoint, relay] as const)),
      true,
    );
  }

  /**
   * Dials [targets], endpoints with the record each is dialled for, in
   * order; the first that answers is the connection, to a relay when
   * [relayed]. With `connectTimeout`, each endpoint gets the time left
   * divided by the endpoints left.
   */
  async #dialEach(
    token: object,
    targets: readonly (readonly [string, ServiceRecord])[],
    relayed: boolean,
  ): Promise<void> {
    const budget = this.#set.connectTimeout;
    const start = this.#clock.now();
    let failure: Status | undefined;
    for (let i = 0; i < targets.length; i++) {
      if (this.#token !== token) {
        return;
      }
      const [endpoint, record] = targets[i] as readonly [string, ServiceRecord];
      let bound: number | undefined;
      if (budget > 0) {
        const left = budget - (this.#clock.now() - start);
        if (left <= 0) {
          failure = Status.of(
            StatusCode.unavailable,
            `cannot connect: timed out after ${budget} ms`,
          );
          break;
        }
        bound = left / (targets.length - i);
      }
      let connection: MuxConnection;
      try {
        connection = await this.#dial(endpoint, bound, record);
      } catch (e) {
        failure =
          e instanceof SwitchboardError
            ? e.status
            : Status.of(StatusCode.unavailable, `cannot connect: ${describe(e)}`);
        this.#log('debug', `${this}: ${endpoint}: ${failure}`);
        continue;
      }
      if (this.#token !== token) {
        this.#log('debug', `${this}: closing a connection that arrived too late`);
        this.#host.retire(connection);
        return;
      }
      this.#token = undefined;
      this.#onConnected(connection, endpoint, relayed ? record.address.type : undefined);
      return;
    }
    if (this.#token !== token) {
      return;
    }
    this.#token = undefined;
    this.#connectFailed(
      failure ??
        Status.of(
          StatusCode.unavailable,
          relayed ? 'no relay endpoints' : 'record has no endpoints',
        ),
    );
  }

  /**
   * Has the peer, registered without endpoints, dial this node through the
   * naming service (`Switchboard.broker`), within `connectTimeout`; a
   * connection that lands after that is sent GOAWAY at once.
   */
  async #brokerConnection(token: object): Promise<void> {
    let brokering: Promise<MuxConnection>;
    try {
      brokering = this.#set.switchboard.broker(this.#record.address, {
        resolver: this.#set.resolver,
      });
    } catch (e) {
      brokering = Promise.reject(e);
    }
    const budget = this.#set.connectTimeout;
    let connection: MuxConnection;
    try {
      connection = await (budget > 0
        ? bounded(brokering, budget, this.#clock, () => {
            this.#host.track(
              brokering.then(
                (late) => this.#host.retire(late),
                () => {},
              ),
            );
            return SwitchboardError.of(StatusCode.unavailable, `not brokered within ${budget} ms`);
          })
        : brokering);
    } catch (e) {
      if (this.#token !== token) {
        return;
      }
      this.#token = undefined;
      this.#connectFailed(
        e instanceof SwitchboardError
          ? e.status
          : Status.of(StatusCode.unavailable, `cannot broker: ${describe(e)}`),
      );
      return;
    }
    if (this.#token !== token) {
      this.#log('debug', `${this}: closing a connection that arrived too late`);
      this.#host.retire(connection);
      return;
    }
    this.#token = undefined;
    this.#onConnected(connection, undefined, undefined);
  }

  /**
   * Dials [endpoint] for [record] (the peer's, or a relay's), giving up
   * after [bound] milliseconds if given; a connection that lands after
   * that is sent GOAWAY at once (`PeerSet.close` waits for it).
   */
  #dial(
    endpoint: string,
    bound: number | undefined,
    record: ServiceRecord,
  ): Promise<MuxConnection> {
    let dialing: Promise<MuxConnection>;
    try {
      dialing = this.#set.switchboard.dial(endpoint, { policy: this.#set.policy, record });
    } catch (e) {
      dialing = Promise.reject(e);
    }
    if (bound === undefined) {
      return dialing;
    }
    return bounded(dialing, bound, this.#clock, () => {
      this.#host.track(
        dialing.then(
          (late) => this.#host.retire(late),
          () => {},
        ),
      );
      return SwitchboardError.of(
        StatusCode.unavailable,
        `cannot connect to ${endpoint}: timed out after ${bound} ms`,
      );
    });
  }

  #connectFailed(status: Status): void {
    if (this.#attempt === 1) {
      this.#log('info', `${this}: connection attempt failed: ${status}`);
    } else {
      this.#log('debug', `${this}: connection attempt ${this.#attempt} failed: ${status}`);
    }
    this.#retryLater(status);
  }

  /** Offline with [status]; the next attempt after the backoff. */
  #retryLater(status: Status): void {
    const delay = this.#host.schedule.delay(this.#failures++);
    this.#log('debug', `${this}: next connection attempt in ${delay} ms`);
    this.#retryTimer = this.#clock.setTimeout(() => this.attemptConnect(), delay);
    this.#setState(PeerState.offline, status, delay);
  }

  #onConnected(
    connection: MuxConnection,
    endpoint: string | undefined,
    relayType: Name | undefined,
  ): void {
    this.#connection = connection;
    this.#endpoint = endpoint;
    this.#relayType = relayType;
    this.#established = false;
    this.#livedLong = false;
    this.#lifetimeTimer = this.#clock.setTimeout(() => {
      this.#lifetimeTimer = undefined;
      this.#livedLong = true;
    }, this.#set.maxBackoff);
    void connection.done.then((status) => this.#onConnectionDone(connection, status));
    void connection.peerGoAwayStatus.then(
      (status) => this.#onPeerGoAway(connection, status),
      () => {},
    );
    this.#log(
      'debug',
      `${this}: ${
        endpoint === undefined
          ? 'brokered'
          : relayType === undefined
            ? `connected to ${endpoint}`
            : `connected to the relay at ${endpoint}`
      }`,
    );
    void this.#setUp(connection).catch((e: unknown) =>
      this.#log('warn', `${this}: setting up the connection failed`, e),
    );
  }

  /**
   * Runs `onConnect` and sets up the per-peer channel on [connection],
   * then reports the peer online; a step that fails fails the attempt.
   * Stops when the connection is left meanwhile.
   */
  async #setUp(connection: MuxConnection): Promise<void> {
    const onConnect = this.#set.onConnect;
    if (onConnect !== undefined) {
      const failure = await this.#bounded('onConnect', () => onConnect(this.peer));
      if (this.#connection !== connection) {
        return;
      }
      if (failure !== undefined) {
        this.#failConnection(connection, failure);
        return;
      }
    }
    if (this.#set.channel !== undefined && !(await this.#openPerPeerChannel(connection))) {
      return;
    }
    this.#established = true;
    this.#log(
      'info',
      `${this}: online at ${
        this.#endpoint === undefined
          ? 'a brokered connection'
          : this.#relayType === undefined
            ? this.#endpoint
            : `the relay at ${this.#endpoint}`
      }`,
    );
    this.#setState(PeerState.online, this.#lastStatus);
  }

  /**
   * Runs one step of the set-up, [hook], for at most `connectTimeout` or
   * until the connection or the per-peer channel is left
   * ({@link #endHook}). Resolves with undefined when it completed (or was
   * left: the caller checks), else with a status of [code] saying it failed
   * or timed out (logged, as a warning unless [quiet]).
   */
  async #bounded(
    name: string,
    hook: () => unknown,
    code: StatusCode = StatusCode.internal,
    quiet = false,
  ): Promise<Status | undefined> {
    const failed = (e: unknown): Status => {
      if (quiet || this.#failures > 0 || this.#channelFailures > 0) {
        // Once per series of failures is enough.
        this.#log('debug', `${this}: ${name} failed: ${describe(e)}`);
      } else {
        this.#log('warn', `${this}: ${name} failed`, e);
      }
      return Status.of(code, `${name} failed`);
    };
    const wait = new Deferred<Status | undefined>();
    this.#hookWait = wait;
    const timeout = this.#set.connectTimeout;
    if (timeout > 0) {
      this.#hookTimer = this.#clock.setTimeout(() => {
        this.#hookTimer = undefined;
        this.#log(
          quiet ? 'debug' : 'warn',
          `${this}: ${name} did not complete within ${timeout} ms`,
        );
        wait.resolve(Status.of(code, `${name} timed out`));
      }, timeout);
    }
    let result: unknown;
    try {
      result = hook();
    } catch (e) {
      if (!wait.isSettled) {
        wait.resolve(failed(e));
      }
      if (this.#hookWait === wait) {
        this.#endHook();
      }
      return wait.promise;
    }
    if (!isPromiseLike(result)) {
      wait.resolve(undefined);
    } else {
      Promise.resolve(result).then(
        () => {
          wait.resolve(undefined);
        },
        (e: unknown) => {
          if (wait.isSettled) {
            // Left, or timed out, already.
            this.#log('debug', `${this}: ${name} failed late: ${describe(e)}`);
            return;
          }
          wait.resolve(failed(e));
        },
      );
    }
    const outcome = await wait.promise;
    if (this.#hookWait === wait) {
      this.#endHook();
    }
    return outcome;
  }

  /** Stops waiting for the step in progress, if any. */
  #endHook(): void {
    if (this.#hookTimer !== undefined) {
      this.#clock.clearTimeout(this.#hookTimer);
    }
    this.#hookTimer = undefined;
    const wait = this.#hookWait;
    this.#hookWait = undefined;
    wait?.resolve(undefined);
  }

  #onConnectionDone(connection: MuxConnection, status: Status): void {
    if (this.#connection !== connection) {
      // Left after a GOAWAY, a refusal or a removal.
      return;
    }
    this.#leave(connection);
    this.#log('info', `${this}: disconnected: ${status}`);
    this.#retryLater(status);
  }

  /**
   * The peer sent GOAWAY on [connection]: unless it is an error, leave the
   * connection to the application's channels on it and connect again at
   * once.
   */
  #onPeerGoAway(connection: MuxConnection, status: Status): void {
    if (this.#connection !== connection) {
      return;
    }
    if (status.code !== StatusCode.goingAway) {
      // The peer is ending the connection: a loss once it has ended.
      this.#log('debug', `${this}: peer sent GOAWAY ${status}`);
      return;
    }
    this.#leave(connection, Status.of(StatusCode.goingAway, 'peer going away'));
    this.#host.drain(connection);
    if (this.#restartedOnGoAway) {
      this.#log('info', `${this}: going away again: ${status}`);
      this.#retryLater(status);
      return;
    }
    this.#restartedOnGoAway = true;
    this.#log('info', `${this}: going away: ${status}, reconnecting`);
    this.attemptConnect(status);
  }

  /**
   * [connection] failed: its set-up failed, or the per-peer channel was
   * refused for good. Leave it with GOAWAY and retry after the backoff.
   */
  #failConnection(connection: MuxConnection, status: Status): void {
    if (this.#connection !== connection) {
      return;
    }
    this.#log(
      this.#failures === 0 ? 'info' : 'debug',
      `${this}: leaving the connection: ${status}`,
    );
    this.#leave(connection, status);
    this.#host.retire(connection);
    this.#retryLater(status);
  }

  /**
   * [connection], the current one, stops being current. The per-peer
   * channel on it is closed with [channelStatus] if given (it ends with the
   * connection anyway).
   */
  #leave(connection: MuxConnection, channelStatus?: Status): void {
    // Every caller checked that [connection] is the current one.
    void connection;
    this.#connection = undefined;
    this.#endpoint = undefined;
    this.#relayType = undefined;
    if (this.#lifetimeTimer !== undefined) {
      this.#clock.clearTimeout(this.#lifetimeTimer);
    }
    this.#lifetimeTimer = undefined;
    if (this.#established) {
      // The attempts count again from the next one.
      this.#attempt = 0;
      if (this.#livedLong) {
        this.#failures = 0;
        this.#restartedOnGoAway = false;
      }
    }
    this.#established = false;
    this.#endHook();
    if (this.#channelRetry !== undefined) {
      this.#clock.clearTimeout(this.#channelRetry);
    }
    this.#channelRetry = undefined;
    const channel = this.#channel;
    if (channel !== undefined) {
      this.#channelEnded();
      if (channelStatus !== undefined) {
        closeQuietly(channel, channelStatus, this.#host);
      }
    }
  }

  // The per-peer channel ----------------------------------------------------

  /**
   * Opens the per-peer channel on [connection], makes sure with a PING
   * round trip that the peer did not refuse it on arrival, and runs
   * `onOpen` with it. Resolves with true when all of that succeeded;
   * otherwise the failure is handled ({@link #channelFailed}), or the
   * connection or the channel was left meanwhile, and resolves with false.
   */
  async #openPerPeerChannel(connection: MuxConnection): Promise<boolean> {
    this.#channelUp = false;
    const template = this.#set.channel as ChannelAddress;
    let payload = template.payload;
    if (payload.length === 0) {
      const record = this.#record;
      const failure = await this.#bounded('credential', async () => {
        payload = await this.#set.switchboard.payloadFor(connection, { record });
      });
      if (this.#connection !== connection || this.#channel !== undefined) {
        return false;
      }
      if (failure !== undefined) {
        this.#failConnection(connection, failure);
        return false;
      }
    }
    const address = template.with({
      instance: template.instance === 0 ? this.#record.address.instance : template.instance,
      payload,
    });
    let channel: MuxChannel;
    try {
      channel = connection.open(PeerCore.wrap(address, this.#relayType).encode());
    } catch (e) {
      if (!(e instanceof SwitchboardError)) {
        throw e;
      }
      this.#log('info', `${this}: cannot open the channel: ${e.status}`);
      this.#channelFailed(connection, e.status);
      return false;
    }
    this.#channel = channel;
    this.#channelLivedLong = false;
    this.#channelLifetime = this.#clock.setTimeout(() => {
      this.#channelLifetime = undefined;
      this.#channelLivedLong = true;
    }, this.#set.maxBackoff);
    void channel.done.then((status) => this.#onChannelEnded(channel, status));
    this.#log('debug', `${this}: opened channel ${channel.id}`);
    // A refusal the peer sends on arrival (its policy, no such service, a
    // credential checked at once, a handler that throws) comes before the
    // answer to a PING sent after the OPEN, and is handled by
    // #onChannelEnded before this goes on.
    const noPong = await this.#bounded(
      'PING',
      () => connection.ping(),
      StatusCode.unavailable,
      true,
    );
    if (this.#connection !== connection || this.#channel !== channel) {
      return false;
    }
    if (noPong !== undefined) {
      this.#channelFailed(connection, noPong);
      return false;
    }
    const onOpen = this.#set.onOpen;
    if (onOpen !== undefined) {
      const failure = await this.#bounded('onOpen', () => onOpen(this.peer, channel));
      if (this.#connection !== connection || this.#channel !== channel) {
        return false;
      }
      if (failure !== undefined) {
        this.#channelFailed(connection, failure);
        return false;
      }
    }
    this.#channelUp = true;
    return true;
  }

  /** [header], a channel's to the peer: as is, or inside the payload of a relay channel of [relayType]. */
  static wrap(header: ChannelAddress, relayType: Name | undefined): ChannelAddress {
    return relayType === undefined
      ? header
      : new ChannelAddress({ type: relayType, payload: header.encode() });
  }

  /**
   * The per-peer channel on [connection], the current connection, could
   * not be set up, or ended, with [status]. Before the peer went online on
   * [connection], for a status that would repeat, and for a channel through
   * a relay that could not be set up again, the connection fails;
   * otherwise the peer is offline and the channel is set up again after
   * its backoff.
   */
  #channelFailed(connection: MuxConnection, status: Status): void {
    if (
      !this.#established ||
      isTerminal(status) ||
      (this.#relayType !== undefined && !this.#channelUp)
    ) {
      this.#failConnection(connection, status);
      return;
    }
    const channel = this.#channel;
    if (channel !== undefined) {
      this.#channelEnded();
      closeQuietly(channel, status, this.#host);
    }
    // A re-open waiting for its PING or onOpen gives up.
    this.#endHook();
    this.#scheduleChannelRetry(status);
  }

  /** The per-peer channel ended or was dropped: its lifetime counts. */
  #channelEnded(): void {
    this.#channel = undefined;
    if (this.#channelLifetime !== undefined) {
      this.#clock.clearTimeout(this.#channelLifetime);
    }
    this.#channelLifetime = undefined;
    if (this.#channelLivedLong) {
      this.#channelFailures = 0;
    }
  }

  #onChannelEnded(channel: MuxChannel, status: Status): void {
    if (this.#channel !== channel) {
      return;
    }
    this.#channelEnded();
    const connection = channel.connection;
    if (this.#connection !== connection || !connection.isOpen || connection.peerGoingAway) {
      // The connection is ending or going away: the set reconnects.
      this.#log('debug', `${this}: channel ${channel.id} ended with ${status}`);
      return;
    }
    this.#log('debug', `${this}: channel ${channel.id} ended with ${status}`);
    this.#channelFailed(connection, status);
  }

  /** Offline with [status] on the live connection; the per-peer channel is set up again after its backoff. */
  #scheduleChannelRetry(status: Status): void {
    if (this.#channelRetry !== undefined) {
      this.#clock.clearTimeout(this.#channelRetry);
    }
    const delay = this.#host.schedule.delay(this.#channelFailures++);
    this.#log('debug', `${this}: channel set up again in ${delay} ms`);
    this.#channelRetry = this.#clock.setTimeout(() => {
      this.#channelRetry = undefined;
      const connection = this.usable;
      if (this.#removed || this.#channel !== undefined || connection === undefined) {
        return;
      }
      void this.#reopen(connection).catch((e: unknown) =>
        this.#log('warn', `${this}: setting up the channel again failed`, e),
      );
    }, delay);
    this.#setState(PeerState.offline, status, delay);
  }

  /** Sets the per-peer channel up again on [connection], on which the peer was online, as on a new connection. */
  async #reopen(connection: MuxConnection): Promise<void> {
    this.#setState(PeerState.connecting, this.#lastStatus);
    if (!(await this.#openPerPeerChannel(connection))) {
      return;
    }
    this.#log('debug', `${this}: channel set up again, online`);
    this.#setState(PeerState.online, this.#lastStatus);
  }

  #log(level: LogLevel, message: string, error?: unknown): void {
    this.#host.log(level, message, error);
  }

  toString(): string {
    return `Peer(${this.address}, ${this.state})`;
  }
}

/**
 * One instance of a {@link PeerSet}: its record, its connection and its
 * state.
 *
 * A live view, updated by the set; read it, do not keep copies of its
 * fields. Times are on the set's clock (monotonic milliseconds,
 * `performance.now()` by default), not wall-clock dates.
 */
export class Peer {
  readonly #core: PeerCore;

  /** @internal Created by the {@link PeerSet}. */
  constructor(core: PeerCore) {
    this.#core = core;
  }

  /** The type and instance id. */
  get address(): ServiceAddress {
    return this.#core.address;
  }

  /** The resolver's current record of the peer (while held, the last one). */
  get record(): ServiceRecord {
    return this.#core.record;
  }

  /** Where the peer stands. */
  get state(): PeerState {
    return this.#core.state;
  }

  /**
   * Why the last connection, connection attempt or per-peer channel ended:
   * the status the connection ended with, the peer's GOAWAY status, the
   * close status of the per-peer channel, a hook's failure (INTERNAL), or
   * the failure of the last attempt (with several endpoints, of the last
   * one tried). Undefined until something has ended. Kept through
   * {@link PeerState.connecting} and {@link PeerState.online}.
   */
  get lastStatus(): Status | undefined {
    return this.#core.lastStatus;
  }

  /** When {@link state} began, on the set's clock. */
  get since(): number {
    return this.#core.since;
  }

  /**
   * When the next attempt (a connection, or the per-peer channel on the
   * live connection) is due while {@link PeerState.offline}, on the set's
   * clock; undefined in every other state. Driven by a timer: the attempt
   * may come later.
   */
  get nextAttemptAt(): number | undefined {
    return this.#core.nextAttemptAt;
  }

  /**
   * Connection attempts since the peer was last online: while
   * {@link PeerState.connecting} the attempt in progress (from 1), while
   * online the attempt that succeeded, while offline the attempts that
   * failed (0 right after a connection it was online on ended, and while
   * the per-peer channel is re-opened on such a connection).
   */
  get attempt(): number {
    return this.#core.attempt;
  }

  /**
   * The endpoint of {@link connection} while it is set: the record's
   * endpoint that answered, or the relay's endpoint for a peer reached
   * through a relay ({@link viaRelay}); undefined for a brokered connection
   * (the peer registered without endpoints and dialled this node).
   */
  get endpoint(): string | undefined {
    return this.#core.endpoint;
  }

  /**
   * Whether {@link connection} is a connection to a relay, which forwards
   * the peer's channels (see {@link PeerSet}, about records without
   * endpoints); false while not connected.
   */
  get viaRelay(): boolean {
    return this.#core.relayType !== undefined;
  }

  /**
   * The peer's connection: set from the moment it is established (during
   * {@link PeerState.connecting}, while the hooks run) until it is lost or
   * left.
   */
  get connection(): MuxConnection | undefined {
    return this.#core.connection;
  }

  /** The per-peer channel while it is open, else undefined. */
  get channel(): MuxChannel | undefined {
    return this.#core.channel;
  }

  /** Whether {@link state} is {@link PeerState.online}. */
  get isOnline(): boolean {
    return this.#core.state === PeerState.online;
  }

  toString(): string {
    return this.#core.toString();
  }
}
