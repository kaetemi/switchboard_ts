/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import {
  ChannelAddress,
  maxInstance,
  ServiceAddress,
  type ServiceAddressLike,
  toServiceAddress,
} from '../address/index.ts';
import { bytesEqual, hexString, type PayloadLike, toPayload, utf8Encode } from '../bytes.ts';
import type {
  Credential,
  CredentialVerifier,
  HolderKey,
  PeerIdentity,
} from '../identity/contract.ts';
import { CredentialVerifier as WallTimeVerifier } from '../identity/credential-verifier.ts';
import {
  type MuxChannel,
  type MuxConnection,
  type MuxOptions,
  muxOptions,
} from '../mux/contract.ts';
import { type Name, type NameLike, toName } from '../name.ts';
import { SlotMode, serviceRecord } from '../naming/naming-protocol.ts';
import type { SlotChannel } from '../naming/slot-channel.ts';
import type { SlotTable } from '../naming/slot-table.ts';
import { isKnownStatusCode, Status, StatusCode, SwitchboardError } from '../status.ts';
import { type TalkOptions, talkOptions } from '../talk/contract.ts';
import type { TalkChannel } from '../talk/talk-channel.ts';
import { defaultMaxFrameSize, type FrameTransport } from '../transport/contract.ts';
import { parseEndpoint } from '../transport/endpoint.ts';
import { type Clock, systemClock } from '../util/clock.ts';
import { Deferred } from '../util/deferred.ts';
import { type Logger, type LogLevel, noopLogger } from '../util/logger.ts';
import { Broadcast } from './broadcast.ts';
import { ChannelPolicies } from './channel-policy.ts';
import type {
  BrokeringResolver,
  ChannelHandler,
  ChannelPolicy,
  EndpointCredential,
  EndpointPolicy,
  ExpectedIdentity,
  Resolver,
  SelectionPolicy,
  ServiceRecord,
  SlotResolver,
} from './contract.ts';
import { coreRuntime } from './core-runtime.ts';
import {
  canonicalHost,
  memoryScheme,
  type NodeEndpoint,
  normaliseBindingEndpoint,
  normalizePath,
  parseNodeEndpoint,
  portOf,
} from './endpoints.ts';
import { genericStatus } from './generic-status.ts';
import { waitForIdentityChange } from './identity-wait.ts';
import { IncomingChannel } from './incoming-channel.ts';
import { defaultRelayType, type RelayConfig } from './relay-config.ts';
import { isBrokeringResolver, isSlotResolver } from './resolver.ts';
import type { SwitchboardBinding, SwitchboardRuntime, TransportDialer } from './runtime.ts';
import { slotChannel } from './slot-layer.ts';
import { installBulkRouting, isBulkAddress, routeDispatchedBulk, talkOver } from './talk-layer.ts';
import {
  abortable,
  bounded,
  checkAborted,
  closeQuietly,
  discardTransport,
  goAwayQuietly,
  quietly,
} from './waits.ts';

/** The logger name of the node. */
const logName = 'Switchboard.Node';

/** The largest intent an IDENT carries, in bytes. */
const maxIntentLength = 64;

/** The largest receiver an IDENT names, in bytes of UTF-8. */
const maxReceiverLength = 255;

/** Length of the random intent of a brokered connection. */
const brokerIntentLength = 16;

/** How many given-up broker intents are remembered. */
const maxAbandoned = 256;

/** The remote description of the accepted side of a connection the node made to one of its own listeners. */
const localRemote = 'local';

/** The schemes the node dials itself (with the dialers of its runtime). */
const builtInSchemes: ReadonlySet<string> = new Set(['ws', 'wss', 'tcp', memoryScheme]);

const schemePattern = /^[a-z][a-z0-9+.-]*$/;

const emptyBytes = new Uint8Array(0);

const strictUtf8 = new TextDecoder('utf-8', { fatal: true });

/** An instance {@link Switchboard.selectAndConnect} selected and the connection to it. */
export interface SelectedInstance {
  /** The record of the instance; its instance is the one to put in the address header. */
  readonly record: ServiceRecord;
  readonly connection: MuxConnection;
}

/** Options of a {@link Switchboard}. Durations in milliseconds. */
export interface SwitchboardOptions {
  /** Resolves service types for the open methods; the caller owns it and closes it after the node. */
  resolver?: Resolver | undefined;
  /** The application payload of every channel the node opens without one; empty by default. Never used implicitly with {@link credentialFor}. */
  defaultPayload?: PayloadLike;
  /**
   * The mux options of every connection, accepted or initiated. Without a
   * `wallTime`, a {@link verifier} that is a `CredentialVerifier` gives its
   * own, so that credential expiry is read on one wall clock.
   */
  muxOptions?: Partial<MuxOptions> | undefined;
  /** The Talk defaults of `openTalk`, `openTalkAt` and `IncomingChannel.talk`. */
  talkOptions?: Partial<TalkOptions> | undefined;
  /** Bound on establishing an outgoing connection; default 10 000; 0: none. */
  connectTimeout?: number | undefined;
  /**
   * Whether the proxy handler honours the host hint of incoming channels by
   * connecting to that host directly. **This turns the node into an open
   * relay** for anyone who can reach it. Default false.
   */
  allowHostHint?: boolean | undefined;
  /** Pooled connections to one endpoint; default 4; 1 to 65536. */
  maxConnectionsPerEndpoint?: number | undefined;
  /** Bound on a `LOCATE` of slot routing; default 5000. */
  slotRefreshTimeout?: number | undefined;
  /** How an instance is picked when no shard slot is given; default `'roundRobin'`. */
  selection?: SelectionPolicy | undefined;
  /** The randomness of `'random'` selection, a number in [0, 1); default `Math.random`. */
  random?: (() => number) | undefined;
  /** The policy of the connections the node initiates; undefined allows everything. */
  outgoingPolicy?: ChannelPolicy | undefined;
  /** Chooses the policy of an initiated connection by its endpoint; overrides {@link outgoingPolicy}. */
  endpointPolicy?: EndpointPolicy | undefined;
  /** Chooses the application payload of a channel opened without one, per destination. */
  credentialFor?: EndpointCredential | undefined;
  /** The credential the node identifies with; undefined for none. */
  credential?: Credential | undefined;
  /** The key pair of the holder key the credential names. */
  holderKey?: HolderKey | undefined;
  /** Checks the credentials peers present; becomes `muxOptions.identityVerifier`. */
  verifier?: CredentialVerifier | undefined;
  /** Whether the node identifies on the connections it initiates; default true. */
  identifyOutgoing?: boolean | undefined;
  /** Whether the node identifies on a connection it initiates to an endpoint (normalised); overrides {@link identifyOutgoing}. */
  identifyFor?: ((endpoint: string) => boolean) | undefined;
  /** Bound on each identification and on holding a channel for the peer to identify; default 10 000; 0: none, and no holding. */
  identityTimeout?: number | undefined;
  /** The receiver an IDENT on an initiated connection names. */
  expectedIdentityFor?: ExpectedIdentity | undefined;
  /** How a node that cannot broker reaches instances registered without endpoints. */
  relay?: RelayConfig | undefined;
  /** Where brokered instances dial this node; default the first listening endpoint. */
  brokerEndpoint?: string | undefined;
  /** Whether connections to the node's own listeners are made in-process; default true. */
  localConnections?: boolean | undefined;
  /** Bindings registered with {@link Switchboard.registerBinding}, in order. */
  bindings?: Iterable<SwitchboardBinding> | undefined;
  /**
   * The platform services: the frame mux, in-process transport pairs, the
   * `mem://` listeners and the socket dialers. The entry points supply
   * theirs; tests may supply their own.
   */
  runtime?: SwitchboardRuntime | undefined;
  /** Time and timers of the node and, unless `muxOptions` gives one, of its connections; default the system clock. */
  clock?: Clock | undefined;
  /** Diagnostics of the node and, unless `muxOptions` gives one, of its connections; default discards. */
  logger?: Logger | undefined;
}

/** What {@link Switchboard.registerService} takes beside the handler. */
export interface RegisterServiceOptions {
  /** The instance id (`u48`); default 0. */
  instance?: number | undefined;
  /** Also serve channels to any instance of the type no registration matches exactly. */
  acceptAnyInstance?: boolean | undefined;
}

/** What the resolving opens take. */
export interface OpenChannelOptions {
  /** The shard slot (`u32`): routes to `candidates[slot mod n]`, or to the slot's owner. */
  shard?: number | undefined;
  /** The application payload; default the node's for the destination (see `credentialFor`). */
  payload?: PayloadLike;
  /** Only records it accepts are candidates. */
  where?: ((record: ServiceRecord) => boolean) | undefined;
  /** Abandons the wait (the open rejects with `signal.reason`). */
  signal?: AbortSignal | undefined;
}

/** What {@link Switchboard.selectAndConnect} takes. */
export interface SelectOptions {
  shard?: number | undefined;
  /** The resolver; default the node's. */
  resolver?: Resolver | undefined;
  /** Skip endpoints of the node's own listeners, so that a proxy never forwards to itself. */
  excludeOwnEndpoints?: boolean | undefined;
  where?: ((record: ServiceRecord) => boolean) | undefined;
  signal?: AbortSignal | undefined;
}

/** What {@link Switchboard.openSelected} takes. */
export interface OpenSelectedOptions extends SelectOptions {
  /** The application payload is the node's for the connection and the record selected. */
  implicitPayload?: boolean | undefined;
  /** An instance without endpoints may be reached through the relay. */
  mayRelay?: boolean | undefined;
}

/** A listener of the node: what {@link Switchboard.isOwnEndpoint} matches. */
export interface OwnListener {
  /** The endpoint as listed in `listeningEndpoints`. */
  readonly uri: string;
  readonly endpoint: NodeEndpoint;
  /** The host names (canonical) that designate the listener. */
  readonly hosts: Set<string>;
  /** The policy of the connections it accepts. */
  readonly policy: ChannelPolicy | undefined;
  /** Declared with `addListeningEndpoint`: compared as its binding normalises endpoints. */
  readonly viaBinding: boolean;
}

/** A listener a subclass started, for the node to stop on `close()`. */
export interface ListenerHandle {
  /** The bound endpoint. */
  readonly uri: string;
  /** Host names (canonical) that designate it. */
  readonly hosts: Iterable<string>;
  readonly policy: ChannelPolicy | undefined;
  /** Stops accepting; connections accepted already stay. */
  stop(): Promise<void>;
  /** Closes what the listener still holds, once the node's connections are gone. */
  forceStop?(): Promise<void>;
}

interface Identification {
  readonly intent: Uint8Array | undefined;
  readonly receiver: string | undefined;
}

/**
 * Where an open goes: the destination's [record] (its instance the one to
 * put in the header) and the [connection] to send the OPEN on, to the
 * instance itself, or to a relay of [relayType].
 */
class Route {
  readonly record: ServiceRecord;
  readonly connection: MuxConnection;
  readonly relayType: Name | undefined;

  constructor(record: ServiceRecord, connection: MuxConnection, relayType?: Name) {
    this.record = record;
    this.connection = connection;
    this.relayType = relayType;
  }

  /** Opens the channel with [header], the destination's: inside a relay channel's payload when relayed. */
  open(header: ChannelAddress): MuxChannel {
    const relay = this.relayType;
    return this.connection.open(
      relay === undefined
        ? header.encode()
        : new ChannelAddress({ type: relay, payload: header.encode() }).encode(),
    );
  }
}

function isPromiseLike(value: unknown): value is PromiseLike<unknown> {
  return (
    typeof value === 'object' &&
    value !== null &&
    typeof (value as { then?: unknown }).then === 'function'
  );
}

/** Throws `RangeError` unless [slot] is a `u32`. */
function checkSlot(slot: number, what: string): void {
  if (!Number.isInteger(slot) || slot < 0 || slot > 0xffffffff) {
    throw new RangeError(`${what} ${slot} is outside 0 to 0xFFFFFFFF`);
  }
}

/** The records of [address] (an exact instance) among [records]. */
function recordsOf(records: readonly ServiceRecord[], address: ServiceAddress): ServiceRecord[] {
  return records.filter(
    (record) =>
      record.address.type.equals(address.type) && record.address.instance === address.instance,
  );
}

function describe(error: unknown): string {
  return error instanceof SwitchboardError ? error.status.toString() : String(error);
}

/** Whether [host] (canonical) is an IP address literal. */
function isIpAddress(host: string): boolean {
  return /^\d{1,3}(\.\d{1,3}){3}$/.test(host) || host.includes(':');
}

/**
 * A mesh node: listens for connections, pools outgoing connections,
 * dispatches incoming channels to local services and opens channels to
 * services anywhere in the mesh through a {@link Resolver}.
 *
 * Dispatch and resolution follow the wiki page "Polyverse Switchboard
 * Addressing and Dispatch". Dispatch is symmetric: channels the peer opens
 * on a connection this node initiated are dispatched exactly like channels
 * on accepted connections, under that connection's policy
 * ({@link outgoingPolicy}). Bulk channels (the reserved type `_bulk`) are
 * not dispatched to services: each goes to the Talk layer of the channel it
 * names on the same connection, without any policy, and is refused
 * `FAILED_PRECONDITION` when no such channel is open.
 *
 * This is the runtime-neutral node: it listens on in-process `mem://`
 * endpoints ({@link listenMemory}) and dials them, adopts connections
 * accepted elsewhere ({@link accept}, {@link adopt}), and dials further
 * schemes through the dialers of its runtime and the bindings registered
 * with {@link registerBinding}. `@kaetemi/switchboard/node` adds TCP,
 * WebSocket and QUIC listeners and dialers; `@kaetemi/switchboard/web`
 * the browser's WebSocket and WebTransport. A URI fragment (the text form
 * of a service address, as in `ws://host/path#npc/1a2b`) is ignored for
 * connecting. An endpoint of one of the node's own listeners is reached
 * in-process, without a socket (see {@link localConnections}).
 */
export class Switchboard implements AsyncDisposable {
  /** Resolves service types for the open methods. May be replaced at any time. */
  resolver: Resolver | undefined;

  /** The application payload attached to every channel the node opens without one; not used implicitly once {@link credentialFor} is set. */
  defaultPayload: Uint8Array;

  /** The mux options of every connection. */
  readonly muxOptions: MuxOptions;

  /** The Talk defaults of channels opened or accepted here. */
  readonly talkOptions: TalkOptions;

  /** Bound on establishing an outgoing connection, in milliseconds; 0: none. */
  readonly connectTimeout: number;

  /** Whether the proxy handler honours host hints. See {@link SwitchboardOptions.allowHostHint}. */
  readonly allowHostHint: boolean;

  /**
   * Largest number of pooled connections to one endpoint. A further
   * connection is only opened when every pooled one has as many channels
   * open as its peer announced it accepts.
   */
  readonly maxConnectionsPerEndpoint: number;

  /** Bound on a `LOCATE` of slot routing, in milliseconds. */
  readonly slotRefreshTimeout: number;

  /** How {@link selectAndConnect} picks among the candidates when no shard slot is given. */
  readonly selection: SelectionPolicy;

  /**
   * The policy of the connections this node initiates ({@link connect},
   * {@link dial}, and through them the open methods), applied to every
   * channel the peer opens on such a connection exactly like a listener
   * policy. Undefined allows everything, as on an internal listener.
   *
   * The policy of a connection is chosen once, when it is established: the
   * `policy` passed to {@link connect} or {@link dial} if any, else what
   * {@link endpointPolicy} returns for the endpoint if it returns one, else
   * this.
   */
  readonly outgoingPolicy: ChannelPolicy | undefined;

  /**
   * Chooses the policy of each connection this node initiates by its
   * endpoint, as the pool tells endpoints apart (scheme and host
   * lower-cased, the default port filled in, the path and query kept for
   * WebSocket URIs only, no fragment, a `mem` URI by its id alone; a
   * binding's scheme as the binding normalises it). A hook that throws
   * gives the connection {@link ChannelPolicies.denyAll}.
   */
  readonly endpointPolicy: EndpointPolicy | undefined;

  /**
   * Chooses the application payload of every channel this node opens
   * without one from the caller, per destination: the endpoint the channel
   * goes to and, for the opens that resolve, the record of the selected
   * instance. A channel opened on a connection this node accepted carries
   * an empty payload. A payload the caller passes always wins. A hook that
   * throws, or whose promise rejects, fails the open.
   */
  readonly credentialFor: EndpointCredential | undefined;

  /** The key pair of the holder key {@link credential} names; undefined for a bearer credential. */
  readonly holderKey: HolderKey | undefined;

  /** Whether the node identifies on the connections it initiates, for endpoints {@link identifyFor} does not decide. */
  readonly identifyOutgoing: boolean;

  /** Whether the node identifies on a connection it initiates to an endpoint (normalised); a hook that throws means no. */
  readonly identifyFor: ((endpoint: string) => boolean) | undefined;

  /** Bound on each identification of the node and on holding a channel for the peer to identify, in milliseconds. */
  readonly identityTimeout: number;

  /**
   * The identity the node expects the peer of a connection it initiates to
   * have, named in the receiver field of its IDENT there. A hook that
   * throws fails the connection (GOAWAY, INTERNAL) rather than naming
   * nobody.
   */
  readonly expectedIdentityFor: ExpectedIdentity | undefined;

  /**
   * The relays through which this node reaches the instances registered
   * without endpoints when it cannot broker them itself ({@link canBroker}
   * is false). Undefined: no relay. May be replaced at any time.
   */
  relay: RelayConfig | undefined;

  /**
   * Where an instance registered without endpoints dials this node when the
   * node brokers a connection to it ({@link broker}). Undefined: the first
   * of {@link listeningEndpoints}.
   */
  brokerEndpoint: string | undefined;

  /**
   * Whether a connection this node initiates to one of its own listeners
   * (an endpoint {@link isOwnEndpoint} recognises) is made in-process, over
   * a transport pair whose frames go through the event queue, instead of
   * through a socket or a binding. Nothing differs on the wire. Default
   * true.
   */
  readonly localConnections: boolean;

  /** Receives channels that carry no service type. Undefined for none. */
  defaultService: ChannelHandler | undefined;

  /** Receives channels no local service claims, typically a proxy handler. Undefined: they are refused NOT_FOUND. */
  catchAll: ChannelHandler | undefined;

  /** The node's time and timers. */
  readonly clock: Clock;

  /** The node's diagnostics. */
  readonly logger: Logger;

  readonly #runtime: SwitchboardRuntime;
  readonly #random: () => number;
  #credential: Credential | undefined;

  readonly #services = new Map<string, Map<number, ChannelHandler>>();
  readonly #anyInstance = new Map<string, Set<number>>();

  readonly #listeners: ListenerHandle[] = [];
  readonly #memoryIds: string[] = [];
  readonly #endpoints: string[] = [];
  readonly #own: OwnListener[] = [];

  readonly #live = new Set<MuxConnection>();
  readonly #lateShutdowns = new Set<Promise<void>>();
  readonly #inFlight = new Set<Promise<void>>();
  readonly #remotes = new WeakMap<MuxConnection, string>();
  readonly #policies = new WeakMap<MuxConnection, ChannelPolicy>();
  readonly #dialled = new WeakMap<MuxConnection, string>();
  readonly #identified = new WeakMap<MuxConnection, Identification>();
  readonly #adopted = new WeakSet<MuxConnection>();
  readonly #bindings = new Map<string, SwitchboardBinding>();

  readonly #pool = new Map<string, MuxConnection[]>();
  readonly #dialing = new Map<string, Promise<MuxConnection>>();
  readonly #policyIds = new WeakMap<ChannelPolicy, number>();
  #nextPolicyId = 0;

  readonly #brokeredPool = new Map<string, MuxConnection>();
  readonly #brokeredAs = new WeakMap<MuxConnection, string>();
  readonly #brokering = new Map<string, Promise<MuxConnection>>();
  readonly #brokerWaits = new Map<string, Deferred<MuxConnection>>();
  readonly #abandoned = new Set<string>();
  readonly #roundRobin = new Map<string, number>();
  readonly #connections = new Broadcast<MuxConnection>();

  #closing = false;
  #closed: Promise<void> | undefined;

  /**
   * Creates a node. See {@link SwitchboardOptions}.
   *
   * @throws {RangeError} for a [maxConnectionsPerEndpoint] outside 1 to
   * 65536, a negative timeout, a credential that names a holder key when
   * [holderKey] is missing or another, and bindings that
   * {@link registerBinding} refuses.
   */
  constructor(options: SwitchboardOptions = {}) {
    const maxConnections = options.maxConnectionsPerEndpoint ?? 4;
    if (!Number.isInteger(maxConnections) || maxConnections < 1 || maxConnections > 65536) {
      throw new RangeError(`maxConnectionsPerEndpoint ${maxConnections} is outside 1 to 65536`);
    }
    const connectTimeout = options.connectTimeout ?? 10_000;
    const identityTimeout = options.identityTimeout ?? 10_000;
    const slotRefreshTimeout = options.slotRefreshTimeout ?? 5000;
    for (const [name, value] of [
      ['connectTimeout', connectTimeout],
      ['identityTimeout', identityTimeout],
      ['slotRefreshTimeout', slotRefreshTimeout],
    ] as const) {
      if (!(value >= 0)) {
        throw new RangeError(`${name} ${value} must not be negative`);
      }
    }
    const given = options.muxOptions ?? {};
    this.clock = options.clock ?? given.clock ?? systemClock;
    this.logger = options.logger ?? given.logger ?? noopLogger;
    const verifier = options.verifier ?? given.identityVerifier;
    const wallTime =
      given.wallTime ?? (verifier instanceof WallTimeVerifier ? verifier.wallTime : undefined);
    this.muxOptions = muxOptions({
      ...given,
      clock: given.clock ?? this.clock,
      logger: given.logger ?? this.logger,
      ...(options.verifier === undefined ? {} : { identityVerifier: options.verifier }),
      ...(wallTime === undefined ? {} : { wallTime }),
    });
    this.talkOptions = talkOptions(options.talkOptions);
    this.resolver = options.resolver;
    this.defaultPayload = toPayload(options.defaultPayload);
    this.connectTimeout = connectTimeout;
    this.allowHostHint = options.allowHostHint ?? false;
    this.maxConnectionsPerEndpoint = maxConnections;
    this.slotRefreshTimeout = slotRefreshTimeout;
    this.selection = options.selection ?? 'roundRobin';
    this.#random = options.random ?? Math.random;
    this.outgoingPolicy = options.outgoingPolicy;
    this.endpointPolicy = options.endpointPolicy;
    this.credentialFor = options.credentialFor;
    this.holderKey = options.holderKey;
    this.identifyOutgoing = options.identifyOutgoing ?? true;
    this.identifyFor = options.identifyFor;
    this.identityTimeout = identityTimeout;
    this.expectedIdentityFor = options.expectedIdentityFor;
    this.relay = options.relay;
    this.brokerEndpoint = options.brokerEndpoint;
    this.localConnections = options.localConnections ?? true;
    this.#runtime = options.runtime ?? coreRuntime();
    if (options.credential !== undefined) {
      this.#checkHolderKey(options.credential);
    }
    this.#credential = options.credential;
    for (const binding of options.bindings ?? []) {
      this.registerBinding(binding);
    }
  }

  // Identity ----------------------------------------------------------------

  /**
   * The credential this node identifies with, or undefined for none. May be
   * replaced, for example by a renewed credential: connections established
   * afterwards use the new one; {@link identifyOn} presents it on a live
   * connection, and {@link updateCredential} on every connection the node
   * identified on. Its identity becomes the `localIdentity` of every
   * connection.
   *
   * @throws {RangeError} when the new credential names a holder key other
   * than {@link holderKey}'s.
   */
  get credential(): Credential | undefined {
    return this.#credential;
  }

  set credential(value: Credential | undefined) {
    if (value !== undefined) {
      this.#checkHolderKey(value);
    }
    this.#credential = value;
    for (const connection of this.#live) {
      connection.localIdentity = value?.identity;
    }
  }

  /** Checks the credentials peers present with IDENT; undefined when the node does not check identity. */
  get verifier(): CredentialVerifier | undefined {
    return this.muxOptions.identityVerifier;
  }

  /**
   * Replaces {@link credential} with [credential] (a renewed one,
   * typically) and presents it again, with IDENT, on every open connection
   * this node has identified on, with the intent and receiver of the last
   * identification there. A connection the node is identifying on
   * meanwhile gets it once that identification completes. Resolves once
   * every such identification has completed or failed (failures are
   * logged: a peer that refuses the credential ends its connection).
   *
   * @throws {RangeError} like the {@link credential} setter.
   */
  async updateCredential(credential: Credential): Promise<void> {
    this.credential = credential;
    await Promise.all(
      [...this.#live]
        .filter((connection) => connection.isOpen && this.#identified.has(connection))
        .map((connection) => this.#reidentify(connection, credential)),
    );
  }

  async #reidentify(connection: MuxConnection, credential: Credential): Promise<void> {
    const last = this.#identified.get(connection);
    try {
      await connection.identify(credential, {
        holderKey: this.holderKey,
        intent: last?.intent,
        receiver: last?.receiver,
        timeout: this.identityTimeout,
      });
    } catch (e) {
      this.log(
        'info',
        `identifying again on ${this.#remotes.get(connection)} failed: ${describe(e)}`,
      );
    }
  }

  /**
   * After an identification on [connection] with [used]: when the
   * credential was replaced meanwhile, {@link updateCredential} skipped the
   * connection (not identified yet), so the current one is presented there
   * now.
   */
  async #presentCurrent(connection: MuxConnection, used: Credential): Promise<void> {
    const current = this.#credential;
    if (current !== undefined && current !== used && connection.isOpen) {
      await this.#reidentify(connection, current);
    }
  }

  #checkHolderKey(credential: Credential): void {
    const holder = credential.holderKey;
    if (holder === undefined) {
      return;
    }
    const key = this.holderKey?.publicKey;
    if (key === undefined || !bytesEqual(key, holder)) {
      throw new RangeError('the credential names a holder key; pass its key pair as holderKey');
    }
  }

  /**
   * Identifies the node with {@link credential} on [connection], with
   * [intent]: for a connection this node accepted whose peer wants it to
   * identify (`identityRequested`), or to present a renewed credential on
   * a live connection. Bounded by {@link identityTimeout}.
   *
   * The IDENT names [receiver] as the peer's identity; by default the
   * receiver of the node's last identification on [connection], else, for
   * a connection the node initiated, what {@link expectedIdentityFor} gives
   * for its endpoint, else nobody.
   *
   * Rejects like `MuxConnection.identify`, and with INTERNAL when
   * {@link expectedIdentityFor} throws (no IDENT is sent).
   *
   * @throws {Error} when the node has no credential.
   */
  identifyOn(
    connection: MuxConnection,
    options: { intent?: Uint8Array | undefined; receiver?: string | undefined } = {},
  ): Promise<void> {
    const credential = this.#credential;
    if (credential === undefined) {
      throw new Error('the node has no credential');
    }
    const dialled = this.#dialled.get(connection);
    let named: string | undefined;
    try {
      named =
        options.receiver ??
        this.#identified.get(connection)?.receiver ??
        (dialled === undefined
          ? undefined
          : this.#expectedIdentity(this.#normalised(dialled), undefined));
    } catch (e) {
      return Promise.reject(e);
    }
    const intent = options.intent;
    return connection
      .identify(credential, {
        holderKey: this.holderKey,
        intent,
        receiver: named,
        timeout: this.identityTimeout,
      })
      .then(() => {
        this.#identified.set(connection, { intent, receiver: named });
        return this.#presentCurrent(connection, credential);
      });
  }

  // Listening ---------------------------------------------------------------

  /**
   * Starts accepting in-process connections at a `mem://` endpoint, for
   * running every node of a mesh in one process (tests, a single-process
   * mode of a deployment).
   *
   * [options.name] is the endpoint id: ASCII letters, digits, `.`, `_`, `~`
   * and `-`, compared in lower case. Undefined generates an id unique in
   * the process. The id is registered with the runtime's memory endpoints
   * until {@link close}: {@link connect} on any node sharing them reaches
   * this node at the returned URI over an in-process transport pair, and
   * the connection is accepted exactly like a TCP connection (listener
   * policy, dispatch, {@link connections}, GOAWAY on {@link close}).
   *
   * [options.policy] is the listener policy applied to every channel
   * arriving on a connection accepted here; undefined allows everything.
   * **A listener reachable by untrusted peers must have a policy** that
   * refuses the reserved types ({@link ChannelPolicies.denyReserved}).
   *
   * Resolves with the `mem://id` URI. Throws `RangeError` for an invalid
   * name, and rejects with `SwitchboardError` ALREADY_EXISTS for an id in
   * use and FAILED_PRECONDITION after {@link close}.
   */
  async listenMemory(
    options: { policy?: ChannelPolicy | undefined; name?: string | undefined } = {},
  ): Promise<string> {
    this.checkOpen();
    const policy = options.policy;
    const uri = this.#runtime.memoryEndpoints.register(
      (transport, remote) => this.adoptTransport(transport, { isInitiator: false, remote, policy }),
      options.name === undefined ? {} : { name: options.name },
    );
    const endpoint = parseNodeEndpoint(uri);
    this.#memoryIds.push(endpoint.host);
    this.#endpoints.push(uri);
    this.#own.push({ uri, endpoint, hosts: new Set([endpoint.host]), policy, viaBinding: false });
    this.log('info', `listening on ${uri}`);
    return uri;
  }

  /**
   * Declares [endpoint], where the listener of a binding accepts the
   * connections it hands to {@link adopt}, as a listener of this node: it
   * is in {@link listeningEndpoints} and {@link isOwnEndpoint} recognises
   * it, so that the proxy, the relay and slot gate forwarding never connect
   * to it, and the node reaches it in-process. [options.policy] is the
   * listener policy the binding's listener adopts its connections with.
   *
   * [endpoint] is compared with others as the binding of its scheme
   * normalises them, apart from the host, which matches [endpoint]'s own
   * and, given the [options.address] the listener is bound to (by default
   * [endpoint]'s host when that is an IP address), what reaches a listener
   * bound there.
   *
   * @throws {RangeError} for a scheme the node listens on itself (`ws`,
   * `wss`, `tcp`, `mem`) and for an endpoint declared already. Rejects with
   * FAILED_PRECONDITION once {@link close} has started.
   */
  async addListeningEndpoint(
    endpoint: string,
    options: { policy?: ChannelPolicy | undefined; address?: string | undefined } = {},
  ): Promise<void> {
    this.checkOpen();
    const parsed = parseNodeEndpoint(endpoint);
    if (builtInSchemes.has(parsed.scheme)) {
      throw new RangeError(`the node listens on ${parsed.scheme} endpoints itself: ${endpoint}`);
    }
    if (this.#declaredEndpoint(parsed) !== undefined) {
      throw new RangeError(`${endpoint} is declared already`);
    }
    const bound =
      options.address === undefined
        ? isIpAddress(parsed.host)
          ? parsed.host
          : undefined
        : canonicalHost(options.address);
    const hosts = new Set([parsed.host]);
    if (bound !== undefined) {
      for (const host of await this.hostsOf(bound)) {
        hosts.add(host);
      }
    }
    this.#endpoints.push(endpoint);
    this.#own.push({
      uri: endpoint,
      endpoint: parsed,
      hosts,
      policy: options.policy,
      viaBinding: true,
    });
    this.log('info', `listening on ${endpoint} (a binding's listener)`);
  }

  /**
   * Withdraws [endpoint], declared with {@link addListeningEndpoint} (in
   * any spelling that designates the same endpoint and host). False when it
   * was not declared.
   */
  removeListeningEndpoint(endpoint: string): boolean {
    let parsed: NodeEndpoint;
    try {
      parsed = parseNodeEndpoint(endpoint);
    } catch {
      return false;
    }
    const own = this.#declaredEndpoint(parsed);
    if (own === undefined) {
      return false;
    }
    this.#own.splice(this.#own.indexOf(own), 1);
    const index = this.#endpoints.indexOf(own.uri);
    if (index >= 0) {
      this.#endpoints.splice(index, 1);
    }
    return true;
  }

  #declaredEndpoint(endpoint: NodeEndpoint): OwnListener | undefined {
    for (const own of this.#own) {
      if (
        own.viaBinding &&
        own.endpoint.host === endpoint.host &&
        this.#sameBindingEndpoint(own.endpoint, endpoint)
      ) {
        return own;
      }
    }
    return undefined;
  }

  /** Whether [a] and [b] designate the same endpoint of a binding, the host apart. */
  #sameBindingEndpoint(a: NodeEndpoint, b: NodeEndpoint): boolean {
    if (a.scheme !== b.scheme) {
      return false;
    }
    let x: NodeEndpoint;
    let y: NodeEndpoint;
    try {
      x = parseNodeEndpoint(this.#bindingNormalise(a));
      y = parseNodeEndpoint(this.#bindingNormalise(b));
    } catch {
      return false;
    }
    return x.scheme === y.scheme && x.port === y.port && x.path === y.path && x.query === y.query;
  }

  #bindingNormalise(endpoint: NodeEndpoint): string {
    const binding = this.#bindings.get(endpoint.scheme);
    return binding?.normalise !== undefined
      ? binding.normalise(endpoint.uri)
      : normaliseBindingEndpoint(endpoint);
  }

  /**
   * The host names (canonical) that designate a listener bound to
   * [address] (canonical): the address itself and, for loopback and
   * wildcard binds, the loopback names. The Node entry adds the host name
   * and the addresses of the network interfaces for wildcard binds.
   */
  protected async hostsOf(address: string): Promise<Set<string>> {
    const hosts = new Set([address]);
    const any4 = address === '0.0.0.0';
    const any6 = address === '::';
    const loopback = address === '::1' || /^127\./.test(address);
    if (loopback || any4 || any6) {
      hosts.add('localhost');
    }
    if (any4 || any6) {
      hosts.add('127.0.0.1');
    }
    if (any6) {
      hosts.add('::1');
      hosts.add('0.0.0.0');
    }
    return hosts;
  }

  /**
   * Whether [endpoint] designates one of this node's own listeners, so that
   * connecting to it would reach this node.
   *
   * Compares scheme, port (defaults filled in) and, for WebSocket URIs, the
   * path (with or without a trailing slash; the query is ignored, as the
   * listener ignores it). The host matches the bound address, its loopback
   * names (`localhost`) and, for a wildcard bind, the loopback addresses
   * (and on Node the host name and interface addresses). A `mem` URI
   * matches by its id alone. An endpoint of a binding's listener matches
   * once declared with {@link addListeningEndpoint}. Nothing matches once
   * {@link close} has started.
   */
  isOwnEndpoint(endpoint: string): boolean {
    return this.#ownListener(endpoint) !== undefined;
  }

  #ownListener(endpoint: string): OwnListener | undefined {
    let parsed: NodeEndpoint;
    try {
      parsed = parseNodeEndpoint(endpoint);
    } catch {
      return undefined;
    }
    const { scheme, host } = parsed;
    const port = portOf(parsed);
    const isWs = scheme === 'ws' || scheme === 'wss';
    const path = isWs ? normalizePath(parsed.path) : '';
    for (const own of this.#own) {
      if (own.viaBinding) {
        if (own.hosts.has(host) && this.#sameBindingEndpoint(own.endpoint, parsed)) {
          return own;
        }
        continue;
      }
      if (
        own.endpoint.scheme === scheme &&
        portOf(own.endpoint) === port &&
        (!isWs || normalizePath(own.endpoint.path) === path) &&
        own.hosts.has(host)
      ) {
        return own;
      }
    }
    return undefined;
  }

  /** The endpoints the node listens on, in the order of the calls, until {@link close}. */
  get listeningEndpoints(): readonly string[] {
    return Object.freeze([...this.#endpoints]);
  }

  /**
   * Registers a listener a subclass started (the Node entry's TCP and
   * WebSocket listeners): it is listed, recognised as the node's own, and
   * stopped by {@link close}. Once {@link close} has started, the listener
   * is stopped at once and this rejects with FAILED_PRECONDITION.
   */
  protected async addListener(listener: ListenerHandle): Promise<OwnListener> {
    if (this.#closing) {
      await quietly(() => listener.stop());
      throw this.closedError();
    }
    const endpoint = parseNodeEndpoint(listener.uri);
    const own: OwnListener = {
      uri: listener.uri,
      endpoint,
      hosts: new Set(listener.hosts),
      policy: listener.policy,
      viaBinding: false,
    };
    this.#listeners.push(listener);
    this.#endpoints.push(listener.uri);
    this.#own.push(own);
    this.log('info', `listening on ${listener.uri}`);
    return own;
  }

  // Connections -------------------------------------------------------------

  /** Every connection this node accepts or initiates, as it is established. Each iteration sees those after it starts; ends after {@link close}. */
  get connections(): AsyncIterable<MuxConnection> {
    return this.#connections;
  }

  /** The schemes the node dials through the bindings registered with {@link registerBinding}. */
  get bindingSchemes(): ReadonlySet<string> {
    return new Set(this.#bindings.keys());
  }

  /**
   * Has the node dial the endpoints of [binding]'s schemes through it:
   * {@link connect}, {@link dial} and through them the open methods, the
   * peer set and the proxy. Connections already established are unaffected.
   *
   * @throws {RangeError} for a scheme the node has built in (`ws`, `wss`,
   * `tcp`, `mem`), one another binding registered, or one that is not a
   * lower-case URI scheme; nothing is registered then.
   * @throws {SwitchboardError} FAILED_PRECONDITION after {@link close}.
   */
  registerBinding(binding: SwitchboardBinding): void {
    this.checkOpen();
    const schemes = [...binding.schemes];
    for (const scheme of schemes) {
      if (!schemePattern.test(scheme)) {
        throw new RangeError(`binding scheme ${scheme} is not a lower-case URI scheme`);
      }
      if (builtInSchemes.has(scheme)) {
        throw new RangeError(`the node dials ${scheme} endpoints itself`);
      }
      if (this.#bindings.has(scheme)) {
        throw new RangeError(`another binding dials ${scheme} endpoints`);
      }
    }
    for (const scheme of schemes) {
      this.#bindings.set(scheme, binding);
    }
  }

  /**
   * Adopts [transport], a connection accepted outside this node, exactly
   * like a connection a listener accepts: the node is the acceptor side of
   * the mux, [options.policy] is the listener policy of every channel
   * arriving on it, those channels are dispatched to the local services,
   * the connection appears on {@link connections}, and {@link close} sends
   * it GOAWAY and waits for it. [options.remote] describes the peer in logs
   * and in `IncomingChannel.remote`; default `'external'`.
   *
   * Rejects with FAILED_PRECONDITION once {@link close} has started; the
   * transport is then adopted all the same and sent GOAWAY at once.
   */
  async accept(
    transport: FrameTransport,
    options: { policy?: ChannelPolicy | undefined; remote?: string | undefined } = {},
  ): Promise<MuxConnection> {
    const connection = this.adoptTransport(transport, {
      isInitiator: false,
      remote: options.remote ?? 'external',
      policy: options.policy,
    });
    if (this.#closing) {
      throw this.closedError();
    }
    return connection;
  }

  /**
   * Adopts [connection], established by any binding, exactly like a
   * connection a listener of this node accepts: what {@link accept} does
   * for a transport the frame mux is to run over. This is how the
   * connections a binding's listener accepts join the node; declare the
   * listener's endpoint with {@link addListeningEndpoint}. Hand it over as
   * soon as it is established: the node reads `incoming` itself.
   *
   * @throws {TypeError} if this node owns [connection] already. Rejects
   * with FAILED_PRECONDITION once {@link close} has started; the connection
   * is then adopted all the same and sent GOAWAY at once.
   */
  async adopt(
    connection: MuxConnection,
    options: { policy?: ChannelPolicy | undefined; remote?: string | undefined } = {},
  ): Promise<MuxConnection> {
    this.#adoptConnection(
      connection,
      connection.isInitiator,
      options.remote ?? 'external',
      options.policy,
    );
    if (this.#closing) {
      throw this.closedError();
    }
    return connection;
  }

  /** Runs the frame mux of the runtime over [transport] and adopts the connection. */
  protected adoptTransport(
    transport: FrameTransport,
    options: {
      isInitiator: boolean;
      remote?: string | undefined;
      policy?: ChannelPolicy | undefined;
    },
  ): MuxConnection {
    const connection = this.#runtime.mux(transport, this.muxOptions, options.isInitiator);
    return this.#adoptConnection(connection, options.isInitiator, options.remote, options.policy);
  }

  #adoptConnection(
    connection: MuxConnection,
    isInitiator: boolean,
    remote: string | undefined,
    policy: ChannelPolicy | undefined,
  ): MuxConnection {
    if (this.#adopted.has(connection)) {
      throw new TypeError('the connection is adopted by this node already');
    }
    this.#adopted.add(connection);
    installBulkRouting(connection);
    if (!this.#closing) {
      void this.#dispatchAll(connection);
    }
    connection.localIdentity = this.#credential?.identity;
    if (remote !== undefined) {
      this.#remotes.set(connection, remote);
    }
    if (policy !== undefined) {
      this.#policies.set(connection, policy);
    }
    if (this.#closing) {
      this.log('debug', `closing, refusing connection from ${remote}`);
      const shutdown = goAwayQuietly(connection);
      this.#lateShutdowns.add(shutdown);
      void shutdown.then(() => this.#lateShutdowns.delete(shutdown));
      return connection;
    }
    this.#live.add(connection);
    this.log('debug', `${isInitiator ? 'connected to' : 'accepted connection from'} ${remote}`);
    void connection.done.then((status) => {
      this.#live.delete(connection);
      this.log('debug', `connection with ${remote} ended: ${status}`);
    });
    if (!isInitiator) {
      void connection.peerIdentified.then((identity) =>
        this.#onAcceptedIdentified(connection, identity),
      );
    }
    this.#connections.push(connection);
    return connection;
  }

  async #dispatchAll(connection: MuxConnection): Promise<void> {
    try {
      for await (const channel of connection.incoming) {
        this.#dispatch(connection, channel);
      }
    } catch (e) {
      this.log(
        'debug',
        `incoming channels of ${this.#remotes.get(connection)} failed: ${describe(e)}`,
      );
    }
  }

  /**
   * A pooled connection to [endpoint], established if needed.
   *
   * Connections are pooled per endpoint (scheme, host and port compared
   * case-insensitively with default ports filled in, plus the path and
   * query for WebSocket URIs; the fragment is ignored; a `mem` URI by its
   * id alone). A pooled connection that is closing or whose peer sent
   * GOAWAY is dropped from the pool and replaced (it keeps serving its open
   * channels). Concurrent calls for the same endpoint share one attempt.
   * Failures are not cached.
   *
   * The first pooled connection with room for another channel is returned:
   * one has room unless its peer states a channel limit
   * (`peerMaxChannels`) and `openChannelCount` has reached it. When none
   * has room, another one is established, up to
   * {@link maxConnectionsPerEndpoint}; beyond that the least loaded one is
   * returned (opening on it then fails with RESOURCE_EXHAUSTED).
   *
   * An endpoint of one of this node's own listeners is reached in-process
   * unless {@link localConnections} is false.
   *
   * Channels the peer opens on the connection are dispatched to the local
   * services under the connection's policy: [options.policy] if given, else
   * {@link endpointPolicy} for [endpoint], else {@link outgoingPolicy}. A
   * call with a policy shares pooled connections only with calls passing
   * the same policy object, never with calls without one.
   *
   * [options.record] is the resolver's record the connection is
   * established for, if the caller has one: it reaches
   * {@link expectedIdentityFor} (and names a relay from its metadata) when
   * a new connection is dialled. [options.signal] abandons this caller's
   * wait; the attempt goes on for the others.
   *
   * Rejects with `SwitchboardError`: UNAVAILABLE if the connection cannot
   * be established within {@link connectTimeout} (for a `mem` URI: no
   * listener has that id), UNIMPLEMENTED for an unsupported scheme,
   * INVALID_ARGUMENT for a URI without a scheme, a `tcp` URI without host
   * or port, or a `mem` URI without id, FAILED_PRECONDITION after
   * {@link close}; when the node identifies on a new connection,
   * UNAUTHENTICATED when the identification fails and INTERNAL when
   * {@link expectedIdentityFor} throws (the connection is sent GOAWAY).
   */
  async connect(
    endpoint: string,
    options: {
      policy?: ChannelPolicy | undefined;
      record?: ServiceRecord | undefined;
      signal?: AbortSignal | undefined;
    } = {},
  ): Promise<MuxConnection> {
    checkAborted(options.signal);
    return abortable(this.#connect(endpoint, options.policy, options.record), options.signal);
  }

  #connect(
    endpoint: string,
    policy: ChannelPolicy | undefined,
    record: ServiceRecord | undefined,
  ): Promise<MuxConnection> {
    if (this.#closing) {
      return Promise.reject(this.closedError());
    }
    let key: string;
    try {
      key = this.#poolKey(endpoint, policy);
    } catch (e) {
      return Promise.reject(e);
    }
    const pooled = this.#pool.get(key);
    if (pooled !== undefined) {
      for (let i = pooled.length - 1; i >= 0; i--) {
        const connection = pooled[i] as MuxConnection;
        if (!connection.isOpen || connection.peerGoingAway) {
          pooled.splice(i, 1);
        }
      }
      let leastLoaded: MuxConnection | undefined;
      for (const connection of pooled) {
        if (Switchboard.#hasRoom(connection)) {
          return Promise.resolve(connection);
        }
        if (
          leastLoaded === undefined ||
          connection.openChannelCount < leastLoaded.openChannelCount
        ) {
          leastLoaded = connection;
        }
      }
      if (pooled.length === 0) {
        this.#pool.delete(key);
      } else if (pooled.length >= this.maxConnectionsPerEndpoint && leastLoaded !== undefined) {
        return Promise.resolve(leastLoaded);
      }
    }
    const existing = this.#dialing.get(key);
    if (existing !== undefined) {
      return existing;
    }
    const attempt = this.#dialPooled(endpoint, key, policy, record);
    this.#dialing.set(key, attempt);
    this.track(attempt);
    return attempt;
  }

  /** The pool key of [endpoint] (normalised), with the id of [policy] when given. */
  #poolKey(endpoint: string, policy: ChannelPolicy | undefined): string {
    const normalised = this.#normalised(endpoint);
    if (policy === undefined) {
      return normalised;
    }
    let id = this.#policyIds.get(policy);
    if (id === undefined) {
      id = ++this.#nextPolicyId;
      this.#policyIds.set(policy, id);
    }
    return `${normalised}\n${id}`;
  }

  /**
   * A new connection to [endpoint] that is not pooled: established through
   * the same path as {@link connect}, adopted for dispatch, reported on
   * {@link connections} and sent GOAWAY by {@link close}, but never handed
   * out by {@link connect} or used by the open methods. The caller owns it
   * and ends it with `goAway()` when done.
   *
   * The node identifies on it as on any connection it initiates, naming the
   * receiver {@link expectedIdentityFor} gives for [endpoint] and
   * [options.record], or, when the hook names nobody and the record is a
   * relay's, the identity in the record's metadata. With an
   * [options.intent] or an [options.receiver] it identifies whatever
   * {@link identifyFor} and {@link identifyOutgoing} say, sending the
   * intent (at most 64 bytes) and naming the receiver (at most 255 bytes of
   * UTF-8): this is how an instance dials back the consumer a naming
   * service's CONNECT names, with one IDENT.
   *
   * Rejects like {@link connect}; with an intent or a receiver and no
   * {@link credential}, with FAILED_PRECONDITION before dialling. A
   * connection that arrives after [options.signal] aborted is sent GOAWAY.
   *
   * @throws {RangeError} for an intent or receiver too long for an IDENT.
   */
  dial(
    endpoint: string,
    options: {
      policy?: ChannelPolicy | undefined;
      intent?: Uint8Array | undefined;
      receiver?: string | undefined;
      record?: ServiceRecord | undefined;
      signal?: AbortSignal | undefined;
    } = {},
  ): Promise<MuxConnection> {
    const { intent, receiver } = options;
    if (intent !== undefined && intent.length > maxIntentLength) {
      throw new RangeError(`intent of ${intent.length} bytes; at most ${maxIntentLength}`);
    }
    if (receiver !== undefined && utf8Encode(receiver).length > maxReceiverLength) {
      throw new RangeError(`receiver longer than ${maxReceiverLength} bytes of UTF-8`);
    }
    if (this.#closing) {
      return Promise.reject(this.closedError());
    }
    if ((intent !== undefined || receiver !== undefined) && this.#credential === undefined) {
      return Promise.reject(
        SwitchboardError.of(
          StatusCode.failedPrecondition,
          'the node has no credential to identify with',
        ),
      );
    }
    if (options.signal?.aborted) {
      return Promise.reject(options.signal.reason);
    }
    const attempt = this.#dial(endpoint, options.policy, intent, receiver, options.record);
    this.track(attempt);
    return abortable(attempt, options.signal, (connection) => void goAwayQuietly(connection));
  }

  /** Whether [connection] can take another channel within the limit its peer announced. */
  static #hasRoom(connection: MuxConnection): boolean {
    const max = connection.peerMaxChannels;
    return max === 0 || connection.openChannelCount < max;
  }

  /**
   * [endpoint] as the pool tells endpoints apart (see {@link connect}); for
   * the scheme of a registered binding, what the binding's `normalise`
   * gives. What {@link endpointPolicy} is asked about.
   *
   * @throws {SwitchboardError} INVALID_ARGUMENT for a URI without a scheme.
   */
  #normalised(endpoint: string): string {
    const parsed = parseNodeEndpoint(endpoint);
    return this.#bindings.has(parsed.scheme)
      ? this.#bindingNormalise(parsed)
      : parseEndpoint(endpoint).key;
  }

  async #dialPooled(
    endpoint: string,
    key: string,
    policy: ChannelPolicy | undefined,
    record: ServiceRecord | undefined,
  ): Promise<MuxConnection> {
    try {
      const connection = await this.#dial(endpoint, policy, undefined, undefined, record);
      let pooled = this.#pool.get(key);
      if (pooled === undefined) {
        pooled = [];
        this.#pool.set(key, pooled);
      }
      pooled.push(connection);
      void connection.done.then(() => {
        const list = this.#pool.get(key);
        const index = list?.indexOf(connection) ?? -1;
        if (list !== undefined && index >= 0) {
          list.splice(index, 1);
          if (list.length === 0) {
            this.#pool.delete(key);
          }
        }
      });
      return connection;
    } finally {
      // The attempt is over (its callers hold the promise); the next
      // connect() starts a new one if needed.
      this.#dialing.delete(key);
    }
  }

  /** Establishes and adopts a connection to [endpoint] and identifies on it (see {@link dial}). */
  async #dial(
    endpoint: string,
    policy: ChannelPolicy | undefined,
    intent: Uint8Array | undefined,
    receiver: string | undefined,
    record: ServiceRecord | undefined,
  ): Promise<MuxConnection> {
    const parsed = parseNodeEndpoint(endpoint);
    const own = this.localConnections ? this.#ownListener(endpoint) : undefined;
    const binding = this.#bindings.get(parsed.scheme);
    let transport: FrameTransport | undefined;
    let established: MuxConnection | undefined;
    if (own !== undefined) {
      transport = this.#connectLocal(own);
    } else if (binding !== undefined) {
      established = await this.#connectBinding(binding, endpoint);
    } else if (parsed.scheme === memoryScheme) {
      transport = this.#connectMemory(parsed);
    } else {
      const dialer = this.#runtime.dialers[parsed.scheme];
      if (dialer === undefined) {
        throw SwitchboardError.of(
          StatusCode.unimplemented,
          `unsupported endpoint scheme: ${endpoint}`,
        );
      }
      transport = await this.#connectTransport(dialer, parsed);
    }
    const normalised = this.#normalised(endpoint);
    const connectionPolicy = policy ?? this.#endpointPolicyOf(normalised) ?? this.outgoingPolicy;
    const connection =
      established === undefined
        ? this.adoptTransport(transport as FrameTransport, {
            isInitiator: true,
            remote: endpoint,
            policy: connectionPolicy,
          })
        : this.#adoptConnection(established, true, endpoint, connectionPolicy);
    this.#dialled.set(connection, endpoint);
    if (this.#closing) {
      // Adopted while closing: it goes away at once and close() waits for it.
      throw this.closedError();
    }
    const credential = this.#credential;
    if (
      credential !== undefined &&
      (intent !== undefined || receiver !== undefined || this.#identifiesTo(normalised))
    ) {
      let named: string | undefined;
      try {
        named =
          receiver ?? this.#expectedIdentity(normalised, record) ?? this.#relayIdentity(record);
      } catch (e) {
        // The hook failed: no IDENT that names nobody instead.
        void goAwayQuietly(connection);
        throw e;
      }
      try {
        await connection.identify(credential, {
          holderKey: this.holderKey,
          intent,
          receiver: named,
          timeout: this.identityTimeout,
        });
        this.#identified.set(connection, { intent, receiver: named });
        await this.#presentCurrent(connection, credential);
      } catch (e) {
        this.log('info', `identification to ${endpoint} failed: ${describe(e)}`);
        void goAwayQuietly(connection);
        if (this.#closing) {
          throw this.closedError();
        }
        throw SwitchboardError.of(
          StatusCode.unauthenticated,
          `identification to ${endpoint} failed: ${describe(e)}`,
        );
      }
    }
    return connection;
  }

  /** Whether the node identifies on a connection to [endpoint] (normalised). */
  #identifiesTo(endpoint: string): boolean {
    const hook = this.identifyFor;
    if (hook === undefined) {
      return this.identifyOutgoing;
    }
    try {
      return hook(endpoint);
    } catch (e) {
      this.log('warn', `identifyFor failed for ${endpoint}`, e);
      return false;
    }
  }

  /**
   * The identity of the relay [record] describes, from its metadata (a
   * record of `_relay`, or of the type of {@link relay}): undefined for any
   * other record, and for metadata that is empty, not UTF-8, or too long
   * for the receiver field of an IDENT.
   */
  #relayIdentity(record: ServiceRecord | undefined): string | undefined {
    if (
      record === undefined ||
      record.metadata.length === 0 ||
      record.metadata.length > maxReceiverLength
    ) {
      return undefined;
    }
    const type = record.address.type;
    if (!type.equals(defaultRelayType) && !type.equals(this.relay?.type)) {
      return undefined;
    }
    try {
      return strictUtf8.decode(record.metadata);
    } catch {
      this.log('debug', `relay ${record.address}: metadata is not an identity`);
      return undefined;
    }
  }

  /**
   * What {@link expectedIdentityFor} says for [endpoint] (normalised) and
   * [record]. A hook that throws fails closed: the error is logged and this
   * throws INTERNAL naming the hook (not the error, which a proxy or relay
   * could pass on).
   */
  #expectedIdentity(endpoint: string, record: ServiceRecord | undefined): string | undefined {
    const hook = this.expectedIdentityFor;
    if (hook === undefined) {
      return undefined;
    }
    try {
      return hook(endpoint, record);
    } catch (e) {
      this.log('warn', `expectedIdentityFor failed for ${endpoint}`, e);
      throw SwitchboardError.of(StatusCode.internal, `expectedIdentityFor failed for ${endpoint}`);
    }
  }

  /** What {@link endpointPolicy} says for [endpoint]; a hook that throws refuses everything. */
  #endpointPolicyOf(endpoint: string): ChannelPolicy | undefined {
    const hook = this.endpointPolicy;
    if (hook === undefined) {
      return undefined;
    }
    try {
      return hook(endpoint);
    } catch (e) {
      this.log('warn', `endpoint policy failed for ${endpoint}`, e);
      return ChannelPolicies.denyAll;
    }
  }

  /** [connectTimeout] as a bound: undefined for none. */
  get #connectBound(): number | undefined {
    return this.connectTimeout > 0 ? this.connectTimeout : undefined;
  }

  /**
   * Dials [endpoint] through [binding], bounded by {@link connectTimeout};
   * a connection that arrives later is closed. Errors other than
   * `SwitchboardError`s become UNAVAILABLE.
   */
  async #connectBinding(binding: SwitchboardBinding, endpoint: string): Promise<MuxConnection> {
    let attempt: Promise<MuxConnection>;
    try {
      attempt = binding.dial(endpoint, {
        options: this.muxOptions,
        timeout: this.connectTimeout,
        localIdentity: this.#credential?.identity,
        signal: undefined,
      });
    } catch (e) {
      attempt = Promise.reject(e);
    }
    try {
      return await bounded(
        attempt,
        this.#connectBound,
        this.clock,
        () =>
          SwitchboardError.of(
            StatusCode.unavailable,
            `cannot connect to ${endpoint}: timed out after ${this.connectTimeout} ms`,
          ),
        (late) => void quietly(() => late.close()),
      );
    } catch (e) {
      if (e instanceof SwitchboardError) {
        throw e;
      }
      throw SwitchboardError.of(StatusCode.unavailable, `cannot connect to ${endpoint}: ${e}`);
    }
  }

  /**
   * Dials [endpoint] with the runtime's [dialer] for its scheme, bounded by
   * {@link connectTimeout}; a transport that arrives later is destroyed.
   * Errors other than `SwitchboardError`s become UNAVAILABLE.
   */
  async #connectTransport(
    dialer: TransportDialer,
    endpoint: NodeEndpoint,
  ): Promise<FrameTransport> {
    const uri = endpoint.uri;
    if (endpoint.scheme === 'tcp' && (endpoint.host.length === 0 || endpoint.port === undefined)) {
      throw SwitchboardError.of(
        StatusCode.invalidArgument,
        `tcp endpoint needs a host and a port: ${uri}`,
      );
    }
    const hash = uri.indexOf('#');
    const target = hash < 0 ? uri : uri.slice(0, hash);
    let attempt: Promise<FrameTransport>;
    try {
      attempt = dialer(target, {
        maxFrameSize:
          endpoint.scheme === 'tcp' ? this.transportFrameLimit : this.webSocketFrameLimit,
        timeout: this.connectTimeout,
        signal: undefined,
      });
    } catch (e) {
      attempt = Promise.reject(e);
    }
    try {
      return await bounded(
        attempt,
        this.#connectBound,
        this.clock,
        () =>
          SwitchboardError.of(
            StatusCode.unavailable,
            `cannot connect to ${uri}: timed out after ${this.connectTimeout} ms`,
          ),
        discardTransport,
      );
    } catch (e) {
      if (e instanceof SwitchboardError) {
        throw e;
      }
      throw SwitchboardError.of(StatusCode.unavailable, `cannot connect to ${uri}: ${e}`);
    }
  }

  /**
   * Connects to [own], one of this node's listeners, in-process: one side
   * of a new transport pair is adopted as that listener adopts a connection
   * it accepts, and the other is returned, to be adopted as the initiated
   * connection.
   */
  #connectLocal(own: OwnListener): FrameTransport {
    const [local, accepted] = this.#runtime.transportPair();
    this.adoptTransport(accepted, { isInitiator: false, remote: localRemote, policy: own.policy });
    return local;
  }

  /** Hands one side of a new transport pair to the in-process listener at [endpoint] and returns the other. */
  #connectMemory(endpoint: NodeEndpoint): FrameTransport {
    const id = endpoint.host;
    if (id.length === 0) {
      throw SwitchboardError.of(
        StatusCode.invalidArgument,
        `mem endpoint needs an id: ${endpoint.uri}`,
      );
    }
    const acceptor = this.#runtime.memoryEndpoints.lookup(id);
    if (acceptor === undefined) {
      throw SwitchboardError.of(
        StatusCode.unavailable,
        `cannot connect to ${endpoint.uri}: no memory listener with that id`,
      );
    }
    // Every frame goes through the event queue, as from a socket: a request
    // loop between nodes of the process would otherwise never let a timer
    // run.
    const [local, remote] = this.#runtime.transportPair();
    const first = this.#memoryIds[0];
    acceptor(remote, first === undefined ? 'memory peer' : `${memoryScheme}://${first}`);
    return local;
  }

  /** The stream binding enforces the mux frame limit; 0 (no limit) maps to the largest `u32` length. */
  protected get transportFrameLimit(): number {
    const max = this.muxOptions.maxFrameSize;
    return max > 0 ? max : 0xffffffff;
  }

  /** The WebSocket binding enforces the mux frame limit on whole messages; 0 keeps the transport's default. */
  protected get webSocketFrameLimit(): number {
    const max = this.muxOptions.maxFrameSize;
    return max > 0 ? max : defaultMaxFrameSize;
  }

  /** Runs [operation] as work {@link close} waits for (at most {@link connectTimeout}); its failure is not reported here. */
  protected track(operation: Promise<unknown>): void {
    const settled = operation.then(
      () => {},
      () => {},
    );
    this.#inFlight.add(settled);
    void settled.then(() => this.#inFlight.delete(settled));
  }

  // Brokered connections ----------------------------------------------------

  /**
   * Whether instances registered without endpoints can dial this node
   * ({@link broker}): it has a {@link brokerEndpoint} or listens somewhere.
   */
  get canBroker(): boolean {
    return this.brokerEndpoint !== undefined || this.#endpoints.length > 0;
  }

  /**
   * Has the instance at [address], whose record has no endpoints, dial
   * this node, and returns that connection: the naming service's CONNECT
   * (wiki page "Polyverse Switchboard Identity and Credentials", section
   * "Reverse connections"), through [options.resolver] (default: the
   * node's), which must be a {@link BrokeringResolver}.
   *
   * The node asks the instance to dial {@link brokerEndpoint} (by default
   * its first listening endpoint) with a random intent, and waits, at most
   * {@link connectTimeout} in all, for the answer (the identity the
   * instance presents) and for an accepted connection whose peer
   * identified with that identity and that intent. With a
   * {@link credential}, the node then identifies on it in turn, naming the
   * instance as the receiver. The connection is accepted: the policy of
   * the listener it arrived on applies. If brokering fails, a connection
   * that already arrived is sent GOAWAY.
   *
   * The connection is not pooled: the caller owns it and ends it with
   * `goAway()`. The resolving opens broker their own pooled connection,
   * one per instance.
   *
   * Rejects with `SwitchboardError`: FAILED_PRECONDITION when the resolver
   * cannot broker, the node has no {@link verifier}, listens nowhere, or is
   * closed; INVALID_ARGUMENT for an address without an instance; what the
   * naming service answers; UNAVAILABLE when the instance did not dial
   * back in time; UNAUTHENTICATED when the connection that arrived is not
   * the instance's, or identifying on it failed.
   */
  broker(
    address: ServiceAddressLike,
    options: { resolver?: Resolver | undefined; signal?: AbortSignal | undefined } = {},
  ): Promise<MuxConnection> {
    if (this.#closing) {
      return Promise.reject(this.closedError());
    }
    let target: ServiceAddress;
    try {
      target = toServiceAddress(address);
    } catch (e) {
      return Promise.reject(e);
    }
    const attempt = this.#broker(target, options.resolver ?? this.resolver);
    this.track(attempt);
    return abortable(attempt, options.signal, (connection) => void goAwayQuietly(connection));
  }

  /**
   * A pooled brokered connection to [address], brokered through [resolver]
   * if there is none; concurrent calls share one attempt. A pooled
   * connection whose peer no longer carries the identity it was brokered
   * as is sent GOAWAY and the instance brokered again.
   */
  #brokeredPooled(address: ServiceAddress, resolver: Resolver): Promise<MuxConnection> {
    const key = address.key;
    const pooled = this.#brokeredPool.get(key);
    if (pooled?.isOpen && !pooled.peerGoingAway) {
      if (pooled.peerIdentity?.identity === this.#brokeredAs.get(pooled)) {
        return Promise.resolve(pooled);
      }
      // Its peer identity expired or changed: the connection no longer
      // shows that the peer is the instance.
      this.log('debug', `brokered connection to ${address} no longer identified`);
      void goAwayQuietly(pooled);
    }
    this.#brokeredPool.delete(key);
    const existing = this.#brokering.get(key);
    if (existing !== undefined) {
      return existing;
    }
    const attempt = (async () => {
      try {
        const connection = await this.#broker(address, resolver);
        this.#brokeredPool.set(key, connection);
        void connection.done.then(() => {
          if (this.#brokeredPool.get(key) === connection) {
            this.#brokeredPool.delete(key);
          }
        });
        return connection;
      } finally {
        this.#brokering.delete(key);
      }
    })();
    this.#brokering.set(key, attempt);
    this.track(attempt);
    return attempt;
  }

  async #broker(address: ServiceAddress, resolver: Resolver | undefined): Promise<MuxConnection> {
    if (resolver === undefined || !isBrokeringResolver(resolver)) {
      throw SwitchboardError.of(
        StatusCode.failedPrecondition,
        `the resolver cannot broker connections to ${address}`,
      );
    }
    if (address.isAny) {
      throw SwitchboardError.of(
        StatusCode.invalidArgument,
        `brokering needs an instance: ${address}`,
      );
    }
    if (this.verifier === undefined) {
      throw SwitchboardError.of(
        StatusCode.failedPrecondition,
        `brokering needs a verifier to recognise ${address}`,
      );
    }
    const endpoint = this.brokerEndpoint ?? this.#endpoints[0];
    if (endpoint === undefined) {
      throw SwitchboardError.of(
        StatusCode.failedPrecondition,
        `no listener for ${address} to dial back`,
      );
    }
    const intent = crypto.getRandomValues(new Uint8Array(brokerIntentLength));
    const key = hexString(intent);
    const arrived = new Deferred<MuxConnection>();
    this.#brokerWaits.set(key, arrived);
    const start = this.clock.now();
    const left = (): number | undefined =>
      this.connectTimeout > 0
        ? Math.max(0, this.connectTimeout - (this.clock.now() - start))
        : undefined;
    try {
      const identity = await (resolver as BrokeringResolver).connectTo(address, endpoint, intent, {
        timeout: left(),
      });
      this.checkOpen();
      const connection = await bounded(arrived.promise, left(), this.clock, () =>
        SwitchboardError.of(
          StatusCode.unavailable,
          `${address} did not dial back within ${this.connectTimeout} ms`,
        ),
      );
      this.checkOpen();
      const peer = connection.peerIdentity;
      if (peer === undefined || peer.identity !== identity) {
        throw SwitchboardError.of(
          StatusCode.unauthenticated,
          `the connection for ${address} is not "${identity}"'s`,
        );
      }
      const credential = this.#credential;
      if (credential !== undefined) {
        try {
          await connection.identify(credential, {
            holderKey: this.holderKey,
            receiver: identity,
            timeout: this.identityTimeout,
          });
        } catch (e) {
          throw SwitchboardError.of(
            StatusCode.unauthenticated,
            `identification to ${address} failed: ${describe(e)}`,
          );
        }
        this.#identified.set(connection, { intent: undefined, receiver: identity });
        await this.#presentCurrent(connection, credential);
      }
      this.#brokeredAs.set(connection, identity);
      this.log('debug', `${address} dialled back from ${this.#remotes.get(connection)}`);
      return connection;
    } catch (e) {
      if (this.#brokerWaits.delete(key)) {
        // A connection that lands later is sent GOAWAY.
        this.#abandoned.add(key);
        if (this.#abandoned.size > maxAbandoned) {
          const oldest = this.#abandoned.values().next();
          if (oldest.done !== true) {
            this.#abandoned.delete(oldest.value);
          }
        }
      }
      // The dial-back may have arrived while CONNECT was still pending; the
      // failure closes it even if the wait above was never reached.
      void arrived.promise.then((connection) => goAwayQuietly(connection));
      this.log('debug', `brokering ${address} failed: ${describe(e)}`);
      if (this.#closing) {
        throw this.closedError();
      }
      throw e;
    }
  }

  /**
   * An accepted connection identified: if its intent is one this node is
   * waiting for ({@link broker}), it is that connection; if it is one the
   * node gave up on, it is sent GOAWAY.
   */
  #onAcceptedIdentified(connection: MuxConnection, identity: PeerIdentity): void {
    if (identity.intent.length === 0) {
      return;
    }
    const key = hexString(identity.intent);
    const waiting = this.#brokerWaits.get(key);
    if (waiting !== undefined) {
      this.#brokerWaits.delete(key);
      waiting.resolve(connection);
    } else if (this.#abandoned.delete(key)) {
      this.log(
        'debug',
        `a brokered connection from "${identity.identity}" arrived too late; leaving it`,
      );
      void goAwayQuietly(connection);
    }
  }

  // Dispatch ----------------------------------------------------------------

  /**
   * Registers [handler] for channels addressed to `(type, instance)`.
   *
   * A channel for `(type, i)` goes to the handler registered for exactly
   * that pair. A channel for `(type, 0)` (any instance) goes to the handler
   * registered for `(type, 0)` if there is one, else to the first
   * registered instance of [type]. A registration with instance 0 does
   * *not* receive channels addressed to a specific non-zero instance.
   *
   * With [options.acceptAnyInstance], the registration also serves
   * channels addressed to any instance of [type] that no registration
   * matches exactly (among several such registrations, the first
   * registered): for a service that does not know the instance id it is
   * known by.
   *
   * Registering an existing pair replaces its handler and its
   * `acceptAnyInstance` flag and keeps its position.
   *
   * @throws {RangeError} if the instance is outside `u48`, or [type] is not a name.
   */
  registerService(
    type: NameLike,
    handler: ChannelHandler,
    options: RegisterServiceOptions = {},
  ): void {
    const key = toName(type).key;
    const instance = options.instance ?? 0;
    if (!Number.isInteger(instance) || instance < 0 || instance > maxInstance) {
      throw new RangeError(`instance ${instance} is outside 0 to ${maxInstance}`);
    }
    let handlers = this.#services.get(key);
    if (handlers === undefined) {
      handlers = new Map();
      this.#services.set(key, handlers);
    }
    handlers.set(instance, handler);
    if (options.acceptAnyInstance ?? false) {
      let any = this.#anyInstance.get(key);
      if (any === undefined) {
        any = new Set();
        this.#anyInstance.set(key, any);
      }
      any.add(instance);
    } else {
      this.#removeAnyInstance(key, instance);
    }
  }

  /** Removes the handler registered for `(type, instance)`, if any. Channels already dispatched to it are unaffected. */
  unregisterService(type: NameLike, instance = 0): void {
    const key = toName(type).key;
    this.#removeAnyInstance(key, instance);
    const handlers = this.#services.get(key);
    if (handlers === undefined) {
      return;
    }
    handlers.delete(instance);
    if (handlers.size === 0) {
      this.#services.delete(key);
    }
  }

  #removeAnyInstance(key: string, instance: number): void {
    const any = this.#anyInstance.get(key);
    if (any?.delete(instance) && any.size === 0) {
      this.#anyInstance.delete(key);
    }
  }

  #dispatch(connection: MuxConnection, channel: MuxChannel): void {
    const remote = this.#remotes.get(connection);
    let address: ChannelAddress;
    try {
      address = ChannelAddress.decode(channel.openPayload);
    } catch (e) {
      const status =
        e instanceof SwitchboardError ? e.status : Status.of(StatusCode.protocolError, String(e));
      this.log('warn', `channel ${channel.id} from ${remote}: ${status}`);
      void closeQuietly(channel, status);
      return;
    }
    if (isBulkAddress(address)) {
      // A bulk channel belongs to a channel of this connection that was
      // admitted already; the Talk layer routes those before `incoming`.
      routeDispatchedBulk(channel, address);
      return;
    }
    const policy = this.#policies.get(connection);
    if (policy !== undefined) {
      const refusal = this.#refusal(policy, address, connection);
      if (
        refusal === StatusCode.unauthenticated &&
        connection.peerIdentity === undefined &&
        this.identityTimeout > 0
      ) {
        void this.#holdForIdentity(connection, channel, address, policy);
        return;
      }
      if (refusal !== undefined) {
        this.#refuse(channel, address, remote, refusal);
        return;
      }
    }
    this.#deliver(channel, address, remote);
  }

  #refuse(
    channel: MuxChannel,
    address: ChannelAddress,
    remote: string | undefined,
    code: StatusCode,
  ): void {
    this.log(
      'info',
      `channel ${channel.id} from ${remote}: ${address} refused by the policy (${genericStatus(code).reason})`,
    );
    void closeQuietly(channel, genericStatus(code));
  }

  /**
   * Holds [channel], which [policy] refused because the peer has not
   * identified, until the peer identifies (then evaluates the policy
   * again), the channel or the connection ends, or
   * {@link identityTimeout} passes (then refuses it with UNAUTHENTICATED).
   * Never rejects.
   */
  async #holdForIdentity(
    connection: MuxConnection,
    channel: MuxChannel,
    address: ChannelAddress,
    policy: ChannelPolicy,
  ): Promise<void> {
    const remote = this.#remotes.get(connection);
    this.log(
      'debug',
      `channel ${channel.id} from ${remote}: ${address} held until the peer identifies`,
    );
    const start = this.clock.now();
    for (;;) {
      const left = this.identityTimeout - (this.clock.now() - start);
      if (left <= 0) {
        break;
      }
      await waitForIdentityChange(channel, left, this.clock);
      if (channel.state !== 'open') {
        // Closed by the peer, or the connection ended.
        return;
      }
      const refusal = this.#refusal(policy, address, connection);
      if (refusal === undefined) {
        this.#deliver(channel, address, remote);
        return;
      }
      if (refusal !== StatusCode.unauthenticated || connection.peerIdentity !== undefined) {
        this.#refuse(channel, address, remote, refusal);
        return;
      }
    }
    this.log(
      'info',
      `channel ${channel.id} from ${remote}: ${address} refused, the peer did not identify within ${this.identityTimeout} ms`,
    );
    void closeQuietly(channel, genericStatus(StatusCode.unauthenticated));
  }

  /** Hands [channel] to the handler [address] selects, or refuses it with NOT_FOUND. */
  #deliver(channel: MuxChannel, address: ChannelAddress, remote: string | undefined): void {
    const handler = this.#select(address);
    if (handler === undefined) {
      this.log('debug', `channel ${channel.id} from ${remote}: no handler for ${address}`);
      void closeQuietly(channel, genericStatus(StatusCode.notFound));
      return;
    }
    const incoming = new IncomingChannel(channel, {
      address,
      talkOptions: this.talkOptions,
      remote,
    });
    try {
      const result = handler(incoming);
      if (isPromiseLike(result)) {
        Promise.resolve(result).then(undefined, (e: unknown) => this.#handlerFailed(incoming, e));
      }
    } catch (e) {
      this.#handlerFailed(incoming, e);
    }
  }

  /**
   * Undefined when [policy] admits [address], else the code to refuse it
   * with: that of a `SwitchboardError` the policy throws (a code a CLOSE may
   * carry), else PERMISSION_DENIED.
   */
  #refusal(
    policy: ChannelPolicy,
    address: ChannelAddress,
    connection: MuxConnection,
  ): StatusCode | undefined {
    try {
      return policy(address, connection) ? undefined : StatusCode.permissionDenied;
    } catch (e) {
      if (e instanceof SwitchboardError) {
        const code = e.code;
        if (
          !isKnownStatusCode(code) ||
          code === StatusCode.ok ||
          code === StatusCode.connectionLost
        ) {
          this.log('warn', `policy failed for ${address}: ${e.status}`);
          return StatusCode.permissionDenied;
        }
        return code;
      }
      this.log('warn', `policy failed for ${address}`, e);
      return StatusCode.permissionDenied;
    }
  }

  #select(address: ChannelAddress): ChannelHandler | undefined {
    const type = address.type;
    if (type === undefined) {
      return this.defaultService ?? this.catchAll;
    }
    const handlers = this.#services.get(type.key);
    if (handlers !== undefined) {
      const exact = handlers.get(address.instance);
      if (exact !== undefined) {
        return exact;
      }
      const any = this.#anyInstance.get(type.key);
      if (any !== undefined) {
        for (const [instance, handler] of handlers) {
          if (any.has(instance)) {
            return handler;
          }
        }
      }
      if (address.instance === 0) {
        for (const handler of handlers.values()) {
          return handler;
        }
      }
    }
    return this.catchAll;
  }

  #handlerFailed(incoming: IncomingChannel, error: unknown): void {
    this.log('error', `handler for ${incoming} failed`, error);
    void incoming.reject(Status.of(StatusCode.internal, 'handler failed'));
  }

  // Opening channels --------------------------------------------------------

  /**
   * Resolves [address] through [options.resolver] (default: the node's
   * resolver), selects an instance and returns it with a connection to it.
   *
   * Records whose instance differs from a non-zero requested instance are
   * skipped; records with instance 0 (as an {@link EndpointResolver}
   * returns) match any requested instance. So are records [options.where]
   * refuses (an exception it throws reaches the caller). The candidates are
   * sorted by instance; with a shard slot `s` the first choice is
   * `candidates[s mod n]`, otherwise the node's {@link selection} picks it.
   * Endpoints of the chosen instance are tried in order; if none can be
   * connected, the following instances are tried in turn.
   *
   * Slot routing (wiki page "Polyverse Switchboard Sharding", section
   * "Routing"): when [address] names any instance, a shard is given and
   * the resolver is a {@link SlotResolver} with a slot table for the type,
   * the channel goes to the slot's owner and nowhere else: the owner from
   * the table (`SlotResolver.slotOwner`, the old owner while the slot
   * migrates); if the table has none, in a managed space the owner the
   * resolver locates (`SlotResolver.locateSlot`, which assigns a free slot;
   * at most {@link slotRefreshTimeout}, then UNAVAILABLE), and in a static
   * space the open fails with UNAVAILABLE. A slot outside the space fails
   * with OUT_OF_RANGE; an owner that cannot be connected, or is missing
   * from the service table, with UNAVAILABLE (the resolver is asked once
   * more before, since an owner just located may have registered after the
   * records were read). An explicit instance bypasses slot routing (the
   * shard is still carried in the header). An owner that [options.where]
   * refuses is not found (NOT_FOUND); no other instance is tried.
   *
   * The returned record's instance is the one to put in the address
   * header: the selected record's instance, or the requested instance when
   * the record's is 0.
   *
   * With [options.excludeOwnEndpoints], endpoints for which
   * {@link isOwnEndpoint} holds are skipped as unreachable.
   *
   * An instance whose record has no endpoints is reached through the
   * naming service's CONNECT when the resolver is a
   * {@link BrokeringResolver} (see {@link broker}); the connection is
   * pooled for that instance until it ends. This never goes through a
   * {@link relay}.
   *
   * Rejects with `SwitchboardError`: NOT_FOUND if no instance is known, or
   * none that [options.where] accepts; UNAVAILABLE if none is reachable;
   * FAILED_PRECONDITION if there is no resolver or the node is closed; and
   * like `Resolver.resolve`. Rejects with `RangeError` for a shard outside
   * `u32`.
   */
  async selectAndConnect(
    address: ServiceAddressLike,
    options: SelectOptions = {},
  ): Promise<SelectedInstance> {
    const route = await this.#selectRoute(toServiceAddress(address), {
      ...options,
      mayRelay: false,
    });
    return { record: route.record, connection: route.connection };
  }

  async #selectRoute(address: ServiceAddress, options: OpenSelectedOptions): Promise<Route> {
    this.checkOpen();
    checkAborted(options.signal);
    const resolver = options.resolver ?? this.resolver;
    if (resolver === undefined) {
      throw SwitchboardError.of(StatusCode.failedPrecondition, `no resolver to find ${address}`);
    }
    const shard = options.shard;
    if (shard !== undefined) {
      checkSlot(shard, 'shard');
    }
    // Not `await resolver.ready`: resolve() waits for the table itself,
    // bounded, whereas `ready` has no bound.
    const known = await abortable(
      resolver.resolve(
        address.type,
        options.signal === undefined ? {} : { signal: options.signal },
      ),
      options.signal,
    );
    this.checkOpen();
    if (shard !== undefined && address.isAny && isSlotResolver(resolver)) {
      const table = resolver.slotTable(address.type);
      if (table !== undefined) {
        return this.#slotRoute(address, shard, table, resolver, known, options);
      }
    }
    const candidates = known.filter(
      (record) =>
        record.address.type.equals(address.type) &&
        (address.isAny || record.address.isAny || record.address.instance === address.instance),
    );
    if (candidates.length === 0) {
      throw SwitchboardError.of(StatusCode.notFound, `no known instance of ${address}`);
    }
    const where = options.where;
    const accepted =
      where === undefined ? candidates : candidates.filter((record) => where(record));
    if (accepted.length === 0) {
      throw SwitchboardError.of(
        StatusCode.notFound,
        `none of the ${candidates.length} known instances of ${address} matches the filter`,
      );
    }
    accepted.sort((a, b) => a.address.instance - b.address.instance);
    const start =
      shard !== undefined
        ? shard % accepted.length
        : this.#selectStart(address.type, accepted.length);
    return this.#connectFirst(address, accepted, start, options, resolver);
  }

  /** The route of a shard [slot] of [address]'s type to the owner [table] names (see {@link selectAndConnect}). */
  async #slotRoute(
    address: ServiceAddress,
    slot: number,
    table: SlotTable,
    resolver: SlotResolver,
    known: ServiceRecord[],
    options: OpenSelectedOptions,
  ): Promise<Route> {
    const owner = await this.#slotOwner(resolver, table, slot, false, options.signal);
    this.checkOpen();
    const target = new ServiceAddress(address.type, owner);
    let records = recordsOf(known, target);
    if (records.length === 0) {
      // An owner that registered while the slot was located is not in the
      // records read before.
      records = recordsOf(
        await abortable(
          resolver.resolve(
            address.type,
            options.signal === undefined ? {} : { signal: options.signal },
          ),
          options.signal,
        ),
        target,
      );
      this.checkOpen();
    }
    if (records.length === 0) {
      throw SwitchboardError.of(
        StatusCode.unavailable,
        `owner ${target} of slot ${slot} is not in the service table`,
      );
    }
    const where = options.where;
    const accepted = where === undefined ? records : records.filter((record) => where(record));
    if (accepted.length === 0) {
      throw SwitchboardError.of(
        StatusCode.notFound,
        `owner ${target} of slot ${slot} does not match the filter`,
      );
    }
    return this.#connectFirst(target, accepted, 0, options, resolver);
  }

  /**
   * The owner of [slot] in [table], as routing finds it: from the table,
   * else (managed space, or [refresh]) through `SlotResolver.locateSlot`,
   * bounded by {@link slotRefreshTimeout}.
   */
  async #slotOwner(
    resolver: SlotResolver,
    table: SlotTable,
    slot: number,
    refresh: boolean,
    signal: AbortSignal | undefined,
  ): Promise<number> {
    const type = table.type;
    if (slot >= table.count) {
      throw SwitchboardError.of(
        StatusCode.outOfRange,
        `slot ${slot} outside the ${table.count} slots of ${type}`,
      );
    }
    let entry = refresh ? undefined : resolver.slotOwner(type, slot);
    if (entry === undefined && (refresh || table.space.mode === SlotMode.managed)) {
      const timeout = this.slotRefreshTimeout;
      entry = await abortable(
        bounded(resolver.locateSlot(type, slot), timeout, this.clock, () =>
          SwitchboardError.of(
            StatusCode.unavailable,
            `slot ${type}/${slot} not located within ${timeout} ms`,
          ),
        ),
        signal,
      );
    }
    if (entry === undefined || entry.owner === 0) {
      throw SwitchboardError.of(StatusCode.unavailable, `slot ${type}/${slot} has no owner`);
    }
    return entry.owner;
  }

  /**
   * The owner of [slot] of [type] by slot routing (see
   * {@link selectAndConnect}): from the resolver's table, or located when
   * the table has none in a managed space. With [options.refresh] the
   * resolver always asks (`SlotResolver.locateSlot`), in either mode; this
   * is how a caller handling `MOVED` itself refreshes the slot.
   *
   * Rejects with `SwitchboardError`: FAILED_PRECONDITION if the resolver is
   * not a {@link SlotResolver} (or there is none, or the node is closed),
   * NOT_FOUND if it has no slot table for [type], OUT_OF_RANGE for a slot
   * outside the space and UNAVAILABLE when the slot has no owner; also
   * like `Resolver.resolve` and `SlotResolver.locateSlot`. Rejects with
   * `RangeError` for a slot outside `u32`.
   */
  async resolveSlotOwner(
    type: NameLike,
    slot: number,
    options: { refresh?: boolean | undefined; signal?: AbortSignal | undefined } = {},
  ): Promise<ServiceAddress> {
    this.checkOpen();
    checkSlot(slot, 'slot');
    checkAborted(options.signal);
    const name = toName(type);
    const resolver = this.resolver;
    if (!isSlotResolver(resolver)) {
      throw SwitchboardError.of(StatusCode.failedPrecondition, 'the resolver has no slot tables');
    }
    // The table is complete once the resolver is synced.
    await abortable(
      resolver.resolve(name, options.signal === undefined ? {} : { signal: options.signal }),
      options.signal,
    );
    this.checkOpen();
    const table = resolver.slotTable(name);
    if (table === undefined) {
      throw SwitchboardError.of(StatusCode.notFound, `no slot space for ${name}`);
    }
    return new ServiceAddress(
      name,
      await this.#slotOwner(resolver, table, slot, options.refresh ?? false, options.signal),
    );
  }

  /** The index among [count] candidates of [type] that the {@link selection} policy picks first. */
  #selectStart(type: Name, count: number): number {
    if (this.selection === 'random') {
      return Math.min(count - 1, Math.floor(this.#random() * count));
    }
    return this.#nextRoundRobin(type) % count;
  }

  #nextRoundRobin(type: Name): number {
    const next = this.#roundRobin.get(type.key) ?? 0;
    this.#roundRobin.set(type.key, (next + 1) % 0x40000000);
    return next;
  }

  /**
   * Connects to the first reachable of [candidates], starting at [start]
   * and going round; an instance without endpoints is brokered through
   * [resolver] when it can, or, with `mayRelay` on a node that cannot
   * broker, reached through the {@link relay}.
   */
  async #connectFirst(
    address: ServiceAddress,
    candidates: ServiceRecord[],
    start: number,
    options: OpenSelectedOptions,
    resolver: Resolver,
  ): Promise<Route> {
    const n = candidates.length;
    const failures: string[] = [];
    // One relay connection for every candidate that needs it.
    let relayed: Promise<SelectedInstance> | undefined;
    for (let i = 0; i < n; i++) {
      const record = candidates[(start + i) % n] as ServiceRecord;
      for (const endpoint of record.endpoints) {
        if ((options.excludeOwnEndpoints ?? false) && this.isOwnEndpoint(endpoint)) {
          this.log(
            'warn',
            `${address}: ${record.address} at ${endpoint} is this node itself; not connecting`,
          );
          failures.push(`${endpoint}: this node`);
          continue;
        }
        try {
          const connection = await this.connect(endpoint, { record, signal: options.signal });
          // A record for any instance (an endpoint resolver's) stands for
          // the instance asked for, metadata included.
          const selected =
            record.address.isAny && !address.isAny
              ? serviceRecord(address, { endpoints: record.endpoints, metadata: record.metadata })
              : record;
          return new Route(selected, connection);
        } catch (e) {
          if (this.#closing || !(e instanceof SwitchboardError)) {
            throw e;
          }
          this.log('debug', `${address}: ${record.address} at ${endpoint}: ${e.status}`);
          failures.push(`${endpoint}: ${e.status}`);
        }
      }
      if (record.endpoints.length === 0) {
        const config = (options.mayRelay ?? false) && !this.canBroker ? this.relay : undefined;
        if (config !== undefined && !record.address.isAny) {
          try {
            relayed ??= this.#connectRelay(config, resolver, options.signal);
            const via = await relayed;
            this.log(
              'debug',
              `${address}: ${record.address} through the relay ${via.record.address} at ${this.#dialled.get(via.connection) ?? this.#remotes.get(via.connection)}`,
            );
            return new Route(record, via.connection, config.type);
          } catch (e) {
            if (this.#closing || !(e instanceof SwitchboardError)) {
              throw e;
            }
            this.log('debug', `${address}: relaying to ${record.address}: ${e.status}`);
            failures.push(`${record.address} (relayed): ${e.status}`);
            continue;
          }
        }
        if (!isBrokeringResolver(resolver) || record.address.isAny) {
          failures.push(`${record.address}: no endpoints`);
          continue;
        }
        try {
          const connection = await abortable(
            this.#brokeredPooled(record.address, resolver),
            options.signal,
          );
          return new Route(record, connection);
        } catch (e) {
          if (this.#closing || !(e instanceof SwitchboardError)) {
            throw e;
          }
          this.log('debug', `${address}: brokering ${record.address}: ${e.status}`);
          failures.push(`${record.address} (brokered): ${e.status}`);
        }
      }
    }
    throw SwitchboardError.of(
      StatusCode.unavailable,
      `no reachable instance of ${address} (${failures.join('; ')})`,
    );
  }

  // Relays ------------------------------------------------------------------

  /**
   * The relays this node would open a relayed channel through now (see
   * {@link relay}), in the order it would try them: one record per explicit
   * `RelayConfig.endpoints` entry (instance 0, the configured identity as
   * metadata), or the records of `RelayConfig.type` that the resolver
   * knows with endpoints, sorted by instance; the first is picked by the
   * node's {@link selection} policy (each call moves the round robin on)
   * and the others follow in turn, for failover.
   *
   * Rejects with `SwitchboardError`: FAILED_PRECONDITION without a
   * {@link relay}, without a resolver to find the relays in, or after
   * {@link close}; NOT_FOUND when no relay is known; and like
   * `Resolver.resolve`.
   */
  async resolveRelays(options: { resolver?: Resolver | undefined } = {}): Promise<ServiceRecord[]> {
    this.checkOpen();
    const config = this.relay;
    if (config === undefined) {
      throw SwitchboardError.of(StatusCode.failedPrecondition, 'no relay configured');
    }
    return this.#relayCandidates(config, options.resolver ?? this.resolver, undefined);
  }

  async #relayCandidates(
    config: RelayConfig,
    resolver: Resolver | undefined,
    signal: AbortSignal | undefined,
  ): Promise<ServiceRecord[]> {
    const explicit = config.endpoints;
    let records: ServiceRecord[];
    if (explicit !== undefined) {
      const identity = config.identity;
      const metadata = identity === undefined ? emptyBytes : utf8Encode(identity);
      records = explicit.map((endpoint) =>
        serviceRecord(new ServiceAddress(config.type), { endpoints: [endpoint], metadata }),
      );
    } else {
      if (resolver === undefined) {
        throw SwitchboardError.of(
          StatusCode.failedPrecondition,
          'no resolver to find the relays in',
        );
      }
      const known = await abortable(
        resolver.resolve(config.type, signal === undefined ? {} : { signal }),
        signal,
      );
      this.checkOpen();
      records = known
        .filter((record) => record.address.type.equals(config.type) && record.endpoints.length > 0)
        .sort((a, b) => a.address.instance - b.address.instance);
    }
    if (records.length === 0) {
      throw SwitchboardError.of(StatusCode.notFound, `no relay of ${config.type} known`);
    }
    const n = records.length;
    const start = this.#selectStart(config.type, n);
    return Array.from({ length: n }, (_, i) => records[(start + i) % n] as ServiceRecord);
  }

  /** A pooled connection to the first relay that answers, with its record. */
  async #connectRelay(
    config: RelayConfig,
    resolver: Resolver,
    signal: AbortSignal | undefined,
  ): Promise<SelectedInstance> {
    const failures: string[] = [];
    for (const record of await this.#relayCandidates(config, resolver, signal)) {
      for (const endpoint of record.endpoints) {
        try {
          return { record, connection: await this.connect(endpoint, { record, signal }) };
        } catch (e) {
          if (this.#closing || !(e instanceof SwitchboardError)) {
            throw e;
          }
          this.log('debug', `relay ${record.address} at ${endpoint}: ${e.status}`);
          failures.push(`${endpoint}: ${e.status}`);
        }
      }
    }
    throw SwitchboardError.of(
      StatusCode.unavailable,
      `no reachable relay (${failures.join('; ')})`,
    );
  }

  /**
   * Opens a channel to a service anywhere in the mesh: resolves and
   * connects with {@link selectAndConnect}, then sends OPEN with the header
   * `{type, selected instance, shard}` and [options.payload] as the
   * application payload (default: what {@link credentialFor} gives for the
   * endpoint and the selected record, or {@link defaultPayload} without
   * that hook).
   *
   * The channel is returned at once; a rejection by the peer arrives as its
   * close status (`done`). Rejects like {@link selectAndConnect}, like
   * `MuxConnection.open` (RESOURCE_EXHAUSTED, say), and like
   * {@link credentialFor}.
   *
   * An instance without endpoints is brokered, or, on a node that cannot
   * broker, reached through its {@link relay}: the channel returned is then
   * the relay channel, whose open payload carries the destination's.
   */
  async openChannel(
    address: ServiceAddressLike,
    options: OpenChannelOptions = {},
  ): Promise<MuxChannel> {
    const target = toServiceAddress(address);
    const { channel } = await this.openSelected(
      target,
      new ChannelAddress({ type: target.type, shard: options.shard, payload: options.payload }),
      {
        where: options.where,
        signal: options.signal,
        implicitPayload: options.payload === undefined,
        mayRelay: true,
      },
    );
    return channel;
  }

  /**
   * Selects and connects like {@link selectAndConnect} (through the
   * {@link relay} when [options.mayRelay] and the node cannot broker the
   * instance selected) and opens a channel with [header] there, its
   * instance set to the selected one and its host hint removed; with
   * [options.implicitPayload], the application payload is the node's for
   * the connection and the record selected (see {@link payloadFor}). A
   * pooled connection that went away or filled up in the meantime is
   * replaced once. Resolves with the channel and the header of the
   * destination (inside the relay channel's payload when relayed).
   *
   * @internal For the proxy and slot routing. Rejects like {@link openChannel}.
   */
  async openSelected(
    address: ServiceAddress,
    header: ChannelAddress,
    options: OpenSelectedOptions = {},
  ): Promise<{ channel: MuxChannel; header: ChannelAddress }> {
    for (let attempt = 0; ; attempt++) {
      const route = await this.#selectRoute(address, { ...options, shard: header.shard });
      let selected = header.with({ instance: route.record.address.instance, clearHost: true });
      if (options.implicitPayload ?? false) {
        const implicit = this.#implicitPayload(route.connection, route.record);
        selected = selected.with({
          payload: implicit instanceof Promise ? await implicit : implicit,
        });
      }
      checkAborted(options.signal);
      try {
        return { channel: route.open(selected), header: selected };
      } catch (e) {
        if (!this.#retryOpen(e, attempt)) {
          throw e;
        }
      }
    }
  }

  /**
   * Opens a channel to the owner of [slot] of [type], with the `MOVED`
   * retry of the wiki page "Polyverse Switchboard Sharding", section
   * "Routing".
   *
   * The first channel is opened as {@link openChannel} with `shard: slot`
   * does (slot routing through a {@link SlotResolver}; through any other
   * resolver, such as the {@link EndpointResolver} of a frontend client,
   * the endpoint routes). If the owner rejects it with CLOSE `MOVED` before
   * anything was sent or received, it is retried once, with the same
   * [options.payload] (default: the payload {@link openChannel} gives the
   * new destination): to the owner the rejection names (`MovedStatus`),
   * unless the resolver's table has a more recent one (even the instance
   * that rejected: the slot came back to it); when it names none, to the
   * owner the resolver finds by asking (`SlotResolver.locateSlot`, bounded
   * by {@link slotRefreshTimeout}); through a resolver without slot tables,
   * the same address again. When the refresh finds no owner other than the
   * instance that rejected, or the retry fails, the returned channel ends
   * with the `MOVED` status. See `SlotChannel` for why a `MOVED` after the
   * first subframe is not retried (the caller may open again and resend:
   * nothing was processed) and why a `RELOCATED` never is. Subframes sent
   * while the replacement is being opened are held, at most one window,
   * `MuxOptions.initialWindow` of {@link muxOptions}
   * (`SlotChannel.maxHeldBytes`); beyond, the channel is closed with
   * `RESOURCE_EXHAUSTED`.
   *
   * [options.where] applies to the first channel and to the retry: an
   * owner it refuses is not found (see {@link selectAndConnect}); on the
   * retry, the returned channel then ends with that NOT_FOUND status
   * instead of `MOVED`.
   *
   * Without a payload, each channel carries the payload {@link openChannel}
   * would give its destination: the retry asks {@link credentialFor} again
   * for the new owner, so a per-destination credential never reaches the
   * other instance. [options.signal] abandons the first open only.
   *
   * Rejects like {@link openChannel}.
   */
  async openChannelToSlot(
    type: NameLike,
    slot: number,
    options: Omit<OpenChannelOptions, 'shard'> = {},
  ): Promise<SlotChannel> {
    const name = toName(type);
    const header = new ChannelAddress({ type: name, shard: slot, payload: options.payload });
    const implicitPayload = options.payload === undefined;
    const { channel: first, header: selected } = await this.openSelected(
      new ServiceAddress(name),
      header,
      { where: options.where, signal: options.signal, implicitPayload, mayRelay: true },
    );
    return slotChannel(this, name, slot, first, {
      header,
      rejectedBy: selected.instance,
      where: options.where,
      implicitPayload,
    });
  }

  /**
   * {@link openChannelToSlot} wrapped in a Talk channel with the node's Talk
   * options and [options.talkOptions]. The retry applies until the first
   * request or message is sent; see `SlotChannel`.
   */
  async openTalkToSlot(
    type: NameLike,
    slot: number,
    options: Omit<OpenChannelOptions, 'shard'> & {
      talkOptions?: Partial<TalkOptions> | undefined;
    } = {},
  ): Promise<TalkChannel> {
    const channel = await this.openChannelToSlot(type, slot, options);
    return this.#talk(channel, options.talkOptions);
  }

  /** {@link openChannel} wrapped in a Talk channel with the node's Talk options and [options.talkOptions]. */
  async openTalk(
    address: ServiceAddressLike,
    options: OpenChannelOptions & { talkOptions?: Partial<TalkOptions> | undefined } = {},
  ): Promise<TalkChannel> {
    const channel = await this.openChannel(address, options);
    return this.#talk(channel, options.talkOptions);
  }

  #talk(channel: MuxChannel | SlotChannel, options: Partial<TalkOptions> | undefined): TalkChannel {
    try {
      return talkOver(
        channel,
        options === undefined ? this.talkOptions : talkOptions({ ...this.talkOptions, ...options }),
      );
    } catch (e) {
      void closeQuietly(
        channel,
        e instanceof SwitchboardError
          ? e.status
          : Status.of(StatusCode.internal, 'cannot wrap the channel in Talk'),
      );
      throw e;
    }
  }

  /**
   * Opens a channel to an explicit [endpoint] with [address] as the OPEN
   * payload, bypassing resolution. This is how a frontend client reaches
   * its endpoint and how a service reaches the naming service.
   *
   * The header (including a host hint, for relays) is sent as given. If
   * its payload is empty, the payload {@link credentialFor} gives for
   * [endpoint] (with no record) is sent instead, or {@link defaultPayload}
   * without that hook; to send an empty application payload from a node
   * that has a default payload, use {@link connect} and `open`. Rejects
   * like {@link connect}, `MuxConnection.open` and {@link credentialFor}.
   */
  async openChannelAt(
    endpoint: string,
    address: ChannelAddress,
    options: { signal?: AbortSignal | undefined } = {},
  ): Promise<MuxChannel> {
    let bytes = address.payload.length === 0 ? undefined : address.encode();
    for (let attempt = 0; ; attempt++) {
      const connection = await this.connect(endpoint, { signal: options.signal });
      if (bytes === undefined) {
        const implicit = this.#credentialPayload(endpoint, undefined);
        bytes = address
          .with({ payload: implicit instanceof Promise ? await implicit : implicit })
          .encode();
      }
      checkAborted(options.signal);
      try {
        return connection.open(bytes);
      } catch (e) {
        if (!this.#retryOpen(e, attempt)) {
          throw e;
        }
      }
    }
  }

  /** {@link openChannelAt} wrapped in a Talk channel with the node's Talk options and [options.talkOptions]. */
  async openTalkAt(
    endpoint: string,
    address: ChannelAddress,
    options: {
      signal?: AbortSignal | undefined;
      talkOptions?: Partial<TalkOptions> | undefined;
    } = {},
  ): Promise<TalkChannel> {
    const channel = await this.openChannelAt(endpoint, address, options);
    return this.#talk(channel, options.talkOptions);
  }

  /**
   * Opens a channel on an existing [connection], for example one an
   * incoming channel arrived on, to reach a service the peer registered
   * locally (symmetric dispatch). An empty payload in [address] is
   * replaced as for {@link openChannelAt}: by {@link defaultPayload}, or
   * with {@link credentialFor} set, by the hook's payload for the endpoint
   * a connection this node initiated goes to, and by an empty payload on a
   * connection this node accepted.
   *
   * @throws like `MuxConnection.open`.
   * @throws {TypeError} when the payload would come from
   * {@link credentialFor} and the hook answers with a promise: this call
   * cannot wait, so pass the payload in [address] (see {@link payloadFor}).
   */
  openChannelOn(connection: MuxConnection, address: ChannelAddress): MuxChannel {
    if (address.payload.length > 0) {
      return connection.open(address.encode());
    }
    const implicit = this.#implicitPayload(connection, undefined);
    if (implicit instanceof Promise) {
      implicit.catch(() => {});
      throw new TypeError(
        'credentialFor answered asynchronously; openChannelOn needs the payload in the address',
      );
    }
    return connection.open(address.with({ payload: implicit }).encode());
  }

  /**
   * The application payload this node attaches to a channel it opens on
   * [connection] when the caller supplies none: with {@link credentialFor},
   * the hook's result for the endpoint [connection] was established to and
   * [options.record] (undefined: an empty payload), or an empty payload for
   * a connection this node accepted; without the hook,
   * {@link defaultPayload}. Rejects like {@link credentialFor}.
   */
  async payloadFor(
    connection: MuxConnection,
    options: { record?: ServiceRecord | undefined } = {},
  ): Promise<Uint8Array> {
    return this.#implicitPayload(connection, options.record);
  }

  #implicitPayload(
    connection: MuxConnection,
    record: ServiceRecord | undefined,
  ): Uint8Array | Promise<Uint8Array> {
    if (this.credentialFor === undefined) {
      return this.defaultPayload;
    }
    const endpoint = this.#dialled.get(connection);
    if (endpoint === undefined) {
      // Accepted: there is no destination endpoint to choose for.
      return emptyBytes;
    }
    return this.#credentialPayload(endpoint, record);
  }

  /** The payload for [endpoint] by {@link credentialFor}, or {@link defaultPayload} without the hook. */
  #credentialPayload(
    endpoint: string,
    record: ServiceRecord | undefined,
  ): Uint8Array | Promise<Uint8Array> {
    const hook = this.credentialFor;
    if (hook === undefined) {
      return this.defaultPayload;
    }
    const result = hook(endpoint, record);
    if (isPromiseLike(result)) {
      return Promise.resolve(result).then((payload) => payload ?? emptyBytes);
    }
    return result ?? emptyBytes;
  }

  /** A pooled connection may receive GOAWAY, or fill up to its peer's channel limit, between `connect` and the OPEN; one retry replaces it. */
  #retryOpen(error: unknown, attempt: number): boolean {
    return (
      attempt === 0 &&
      error instanceof SwitchboardError &&
      (error.is(StatusCode.failedPrecondition) || error.is(StatusCode.resourceExhausted)) &&
      !this.#closing
    );
  }

  // Shutdown ----------------------------------------------------------------

  /**
   * Shuts the node down: stops listening (the ids of {@link listenMemory}
   * are released at once), sends GOAWAY on every connection and waits for
   * each to finish its channels (up to `goAwayGrace`) and close. Further
   * calls to {@link connect} and the open methods fail with
   * FAILED_PRECONDITION.
   *
   * Connections that are established while closing (a dial in progress, a
   * transport given to {@link accept}) receive GOAWAY at once, and the
   * returned promise waits for them too (for dials in progress, at most
   * {@link connectTimeout}).
   *
   * The {@link resolver} is owned by the caller and is not closed. Calling
   * again returns the same promise. Never rejects.
   */
  close(): Promise<void> {
    this.#closed ??= this.#close();
    return this.#closed;
  }

  /** {@link close}, for `await using`. */
  [Symbol.asyncDispose](): Promise<void> {
    return this.close();
  }

  async #close(): Promise<void> {
    this.#closing = true;
    this.log('info', 'closing');
    const waits = [...this.#brokerWaits.values()];
    this.#brokerWaits.clear();
    for (const wait of waits) {
      wait.reject(this.closedError());
    }
    this.#brokeredPool.clear();
    const listeners = this.#listeners.splice(0);
    for (const id of this.#memoryIds) {
      this.#runtime.memoryEndpoints.unregister(id);
    }
    this.#memoryIds.length = 0;
    this.#endpoints.length = 0;
    this.#own.length = 0;
    await Promise.all(listeners.map((listener) => quietly(() => listener.stop())));
    this.#pool.clear();
    await Promise.all([...this.#live].map((connection) => goAwayQuietly(connection)));
    // Dials in progress may still produce connections, which are adopted
    // and sent GOAWAY at once: wait until none is left, for the dials at
    // most connectTimeout.
    const start = this.clock.now();
    for (;;) {
      const late = [...this.#lateShutdowns];
      this.#lateShutdowns.clear();
      const left = this.connectTimeout - (this.clock.now() - start);
      const waiting = left > 0 ? [...this.#inFlight] : [];
      if (late.length === 0 && waiting.length === 0) {
        if (this.#inFlight.size > 0) {
          this.log('debug', `closed with ${this.#inFlight.size} dials pending`);
        }
        break;
      }
      const pending: Promise<unknown>[] = [...late];
      if (waiting.length > 0) {
        pending.push(
          bounded(Promise.all(waiting), left, this.clock, () => undefined).then(
            () => {},
            () => {},
          ),
        );
      }
      await Promise.all(pending);
    }
    await Promise.all(
      listeners.map((listener) =>
        quietly(() => (listener.forceStop === undefined ? undefined : listener.forceStop())),
      ),
    );
    this.#connections.end();
    this.log('info', 'closed');
  }

  /** Whether {@link close} has started. */
  protected get isClosing(): boolean {
    return this.#closing;
  }

  /** @throws {SwitchboardError} FAILED_PRECONDITION once {@link close} has started. */
  protected checkOpen(): void {
    if (this.#closing) {
      throw this.closedError();
    }
  }

  /** The error of a call after {@link close}. */
  protected closedError(): SwitchboardError {
    return SwitchboardError.of(StatusCode.failedPrecondition, 'switchboard is closed');
  }

  /** Logs under `Switchboard.Node`. */
  protected log(level: LogLevel, message: string, error?: unknown): void {
    this.logger.log(level, logName, message, error);
  }
}
