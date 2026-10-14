/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { ChannelAddress, ServiceAddress } from '../address/index.ts';
import { utf8Encode } from '../bytes.ts';
import type { Credential } from '../identity/contract.ts';
import type { MuxConnection } from '../mux/contract.ts';
import { Name, type NameLike, toName } from '../name.ts';
import { NamingClient } from '../naming/naming-client.ts';
import {
  type DialBackRequest,
  type PhaseItem,
  Services,
  SlotMode,
  SlotSpace,
} from '../naming/naming-protocol.ts';
import { NamingResolver } from '../naming/naming-resolver.ts';
import { SlotGate, SlotGates, type SlotLifecycle } from '../naming/slot-gate.ts';
import { StatusCode, SwitchboardError } from '../status.ts';
import type { TalkOptions } from '../talk/contract.ts';
import type { Clock, TimerHandle } from '../util/clock.ts';
import { Deferred } from '../util/deferred.ts';
import type { Logger, LogLevel } from '../util/logger.ts';
import { ChannelPolicies } from './channel-policy.ts';
import type { ChannelHandler, ChannelPolicy } from './contract.ts';
import type { RelayService } from './relay.ts';
import type { Switchboard } from './switchboard.ts';
import { bounded, goAwayQuietly } from './waits.ts';

const logName = 'Switchboard.Naming';

/**
 * At most this many `RELEASE`s, and as many hand-over migrations,
 * {@link MeshNode.leave} keeps in flight: well under the request limit of
 * the naming channel (`TalkOptions.maxOutgoingRequests`, 1024 by default),
 * refilled as the answers arrive.
 */
const leaveWindow = 64;

/** What {@link namingClientFor} takes beside the node and the endpoint. */
export interface NamingClientForOptions {
  /** Delay before each reconnect attempt, in ms; default 1000. */
  readonly reconnectDelay?: number | undefined;
  /**
   * The Talk options of the naming channel, over the node's
   * (`Switchboard.talkOptions`); their `requestTimeout` bounds the `WATCH`
   * with its snapshot. Default the node's.
   */
  readonly talkOptions?: Partial<TalkOptions> | undefined;
  /** Whether the client mirrors the table (`NamingClient.watch`); default true. */
  readonly watch?: boolean | undefined;
  /** Longest a slot handler call may run, in ms (`NamingClient.slotHandlerMaxDuration`); default 600 000. */
  readonly slotHandlerMaxDuration?: number | undefined;
  /** The client's timers; default the node's clock. */
  readonly clock?: Clock | undefined;
  /** The client's diagnostics; default the node's logger. */
  readonly logger?: Logger | undefined;
}

/**
 * A {@link NamingClient} that reaches the naming service at [namingEndpoint]
 * through [switchboard].
 *
 * Every (re)connect opens a fresh channel with `Switchboard.openTalkAt` to
 * `_ns` (any instance) at [namingEndpoint], on the node's pooled connection
 * to that endpoint, carrying the payload the node gives that endpoint:
 * `credentialFor(namingEndpoint, undefined)` when the node has a
 * `credentialFor` hook (a hook that answers undefined there sends an empty
 * credential to the naming service), else the node's `defaultPayload`. A
 * naming service that stops answering is detected by the node's connection
 * to it (the mux keep-alive of `Switchboard.muxOptions`), which ends the
 * channel.
 *
 * Nothing happens until `NamingClient.start`. Close the client before
 * closing [switchboard]: once the node is closed every reconnect attempt
 * fails, and the client keeps retrying until it is closed.
 *
 * @throws {RangeError} like the `NamingClient` constructor.
 */
export function namingClientFor(
  switchboard: Switchboard,
  namingEndpoint: string,
  options: NamingClientForOptions = {},
): NamingClient {
  const talkOptions = options.talkOptions;
  const address = new ChannelAddress({ type: Services.naming });
  return new NamingClient(
    () =>
      switchboard.openTalkAt(
        namingEndpoint,
        address,
        talkOptions === undefined ? {} : { talkOptions },
      ),
    {
      clock: options.clock ?? switchboard.clock,
      logger: options.logger ?? switchboard.logger,
      ...(options.reconnectDelay !== undefined && { reconnectDelay: options.reconnectDelay }),
      ...(options.watch !== undefined && { watch: options.watch }),
      ...(options.slotHandlerMaxDuration !== undefined && {
        slotHandlerMaxDuration: options.slotHandlerMaxDuration,
      }),
    },
  );
}

/** What {@link MeshNode.join} takes beside the node and the naming endpoint. */
export interface MeshNodeOptions {
  /** Delay before each reconnect to the naming service, in ms; default 1000. */
  readonly reconnectDelay?: number | undefined;
  /** How long resolution waits for the first sync (`NamingResolver.resolveTimeout`), in ms; default 5000. */
  readonly resolveTimeout?: number | undefined;
  /** The Talk options of the naming channel, over the node's; default the node's. */
  readonly talkOptions?: Partial<TalkOptions> | undefined;
  /**
   * Longest {@link MeshNode.leave} spends handing this node's slots over,
   * and then again releasing those it could not hand over, in ms; default
   * 5000.
   */
  readonly leaveTimeout?: number | undefined;
  /**
   * The policy of the connections the node dials when the naming service
   * relays a `CONNECT`; default `ChannelPolicies.scoped()` (the consumer,
   * which identifies in turn, may open the types its `open` scopes grant;
   * the node needs a verifier for that).
   */
  readonly brokeredPolicy?: ChannelPolicy | undefined;
  /**
   * Whether the node renews its credential through the naming service
   * (`RENEW`); default true. See {@link MeshNode.renewCredential}.
   */
  readonly renewCredential?: boolean | undefined;
  /**
   * Whether the client mirrors the table (`NamingClient.watch`); default
   * true. False joins without mirroring, for a node whose credential has no
   * `watch` scope, such as a worker.
   */
  readonly watch?: boolean | undefined;
  /**
   * Wall time in ms since the epoch, which the renewal is scheduled by
   * (credentials expire by it); default `Date.now`.
   */
  readonly wallTime?: (() => number) | undefined;
  /** The timers of the node and its naming client; default the node's clock. */
  readonly clock?: Clock | undefined;
  /** Diagnostics, under the name `Switchboard.Naming`; default the node's logger. */
  readonly logger?: Logger | undefined;
}

/** What {@link MeshNode.publish} takes beside the type and the handler. */
export interface MeshPublishOptions {
  /** The instance id to ask for; 0 (the default) has the naming service assign one. */
  readonly instance?: number | undefined;
  /**
   * What other nodes connect to; default the node's
   * `Switchboard.listeningEndpoints` at the time of the call (so listen
   * first, on an address other nodes can reach). Empty for an instance
   * that listens nowhere and is reached through `CONNECT` or a relay.
   */
  readonly endpoints?: readonly string[] | undefined;
}

/**
 * What {@link MeshNode.publishSharded} takes beside the type and the
 * lifecycle: the slot space (`count`, `mode`, `lazy`, `shared`), this
 * instance's share of it (`capacity`, `holding`), the registration
 * (`instance`, `endpoints`, as {@link MeshPublishOptions}) and the gate's
 * settings (see `SlotGateOptions`).
 */
export interface MeshPublishShardedOptions extends MeshPublishOptions {
  /** Number of slots, 1 to 0xFFFFFFFF. */
  readonly count: number;
  /** Default `SlotMode.managed`. */
  readonly mode?: SlotMode | undefined;
  /** Slots are assigned when first located rather than up front (managed mode); default false. */
  readonly lazy?: boolean | undefined;
  /** Shared recovery: the state is in shared storage; default false (holder-only). */
  readonly shared?: boolean | undefined;
  /** This instance's share weight in a managed space; 0 takes no slots from the allocator; default 1. */
  readonly capacity?: number | undefined;
  /** Slots whose storage this instance has, declared with `HOLDING`; default none. */
  readonly holding?: Iterable<number> | undefined;
  /** How long channels and requests keep being forwarded to the new owner after `FORWARD`, in ms; default 30 000. */
  readonly forwardGrace?: number | undefined;
  /** Longest wait of `DRAIN` for a locked slot's work in flight, in ms; default 30 000. */
  readonly drainTimeout?: number | undefined;
  /** Most channels the gate queues at a time, over all slots; default 1024. */
  readonly maxQueuedChannels?: number | undefined;
  /** Most requests the gate queues at a time, over all slots; default 1024. */
  readonly maxQueuedRequests?: number | undefined;
  /** Whether served channels count as work in flight until they end; default true. */
  readonly trackChannels?: boolean | undefined;
  /** Receives the channels that carry no shard slot (state transfer between instances); default none (`INVALID_ARGUMENT`). */
  readonly noSlotHandler?: ChannelHandler | undefined;
}

/**
 * What {@link MeshNode.leave} did with the slots the node served, by type
 * (keyed by `Name.key`, sorted slot numbers).
 */
export class LeaveReport {
  /**
   * Slots handed over to another instance through a migration while this
   * node was still up: their state was drained here and taken over.
   */
  readonly handedOver: ReadonlyMap<string, readonly number[]>;
  /**
   * Slots released keeping their storage (`RELEASE`): not handed over in
   * time, or in a space where no other instance could take them, a static
   * space or a `shared` one. In a holder-only managed space their new
   * owner fetches the state from this node only while it is still
   * reachable.
   */
  readonly released: ReadonlyMap<string, readonly number[]>;
  /**
   * Slots stopped here without the naming service taking them: the node
   * was not connected to it, or the release failed or timed out. The
   * naming service frees them when this node's registration goes away.
   */
  readonly dropped: ReadonlyMap<string, readonly number[]>;

  /** A report of the given slots; types without slots are left out. */
  constructor(
    init: {
      handedOver?: ReadonlyMap<string, Iterable<number>>;
      released?: ReadonlyMap<string, Iterable<number>>;
      dropped?: ReadonlyMap<string, Iterable<number>>;
    } = {},
  ) {
    this.handedOver = sorted(init.handedOver);
    this.released = sorted(init.released);
    this.dropped = sorted(init.dropped);
  }

  /** Whether every slot was handed over (nothing released or dropped). */
  get isComplete(): boolean {
    return this.released.size === 0 && this.dropped.size === 0;
  }

  /** The three maps, with the type names. */
  toString(): string {
    return (
      `LeaveReport(handed over ${showSlots(this.handedOver)}, ` +
      `released ${showSlots(this.released)}, dropped ${showSlots(this.dropped)})`
    );
  }
}

/** `{type: [slots], ...}` with the type names. */
function showSlots(map: ReadonlyMap<string, readonly number[]>): string {
  return `{${[...map].map(([key, slots]) => `${Name.fromKey(key)}: [${slots.join(', ')}]`).join(', ')}}`;
}

function sorted(
  slots: ReadonlyMap<string, Iterable<number>> | undefined,
): ReadonlyMap<string, readonly number[]> {
  const result = new Map<string, readonly number[]>();
  for (const [key, set] of slots ?? []) {
    const list = [...new Set(set)].sort((a, b) => a - b);
    if (list.length > 0) {
      result.set(key, Object.freeze(list));
    }
  }
  return result;
}

/**
 * Wires a {@link Switchboard} into a mesh run by a naming service: the usual
 * setup of a backend service node.
 *
 * {@link MeshNode.join} creates a {@link NamingClient} that reaches the
 * naming service through the node ({@link namingClientFor}), installs a
 * {@link NamingResolver} over it as the node's `Switchboard.resolver`, and
 * starts the client. {@link publish} registers a local service with the
 * naming service and dispatches channels for the instance id it was
 * assigned to the handler. {@link leave} undoes all of it.
 *
 * ```ts
 * const node = new Switchboard();
 * await node.listenTcp('10.0.0.5', 9101);
 * const mesh = MeshNode.join(node, 'tcp://10.0.0.2:9100');
 * const id = await mesh.publish('npc', npcHandler);
 * // ... node.openTalk('chat') reaches the mesh.
 * await mesh.leave();
 * await node.close();
 * ```
 *
 * A sharded type is published with {@link publishSharded}, which also
 * defines its slot space and installs a {@link SlotGate}; {@link claimSlot},
 * {@link releaseSlot}, {@link migrateSlot} and {@link slotOwners} work on its
 * slots. See the wiki page "Polyverse Switchboard Sharding".
 *
 * Identity (wiki page "Polyverse Switchboard Identity and Credentials"):
 * the node serves the `CONNECT` requests the naming service relays to it
 * (a consumer wants an instance this node registered without endpoints to
 * dial it): it dials the consumer with `Switchboard.dial`, identifying
 * with the consumer's intent and naming the consumer as the receiver,
 * under {@link brokeredPolicy}; it keeps those connections until they end
 * or it leaves. And it renews the node's `Switchboard.credential` through
 * the naming service (`RENEW`) at two thirds of its lifetime, presenting
 * the renewed credential on every connection the node identified on
 * (`Switchboard.updateCredential`).
 */
export class MeshNode implements AsyncDisposable {
  /** The node this joined to the mesh. */
  readonly switchboard: Switchboard;
  /** The naming client: registrations and the mirrored table. */
  readonly client: NamingClient;
  /** The resolver installed on {@link switchboard}. */
  readonly resolver: NamingResolver;
  /** Longest {@link leave} waits for the naming service, in ms, once for the hand-overs and once for the releases. */
  readonly leaveTimeout: number;
  /** The policy of the connections the node dials for `CONNECT`. */
  readonly brokeredPolicy: ChannelPolicy;
  /**
   * Whether the node renews its credential through the naming service: a
   * node that has a credential with an expiry when it joins renews it at
   * two thirds of its lifetime, again after every renewal, and retries a
   * failed renewal (after a quarter of the time left, at least a second)
   * until the credential expires.
   */
  readonly renewCredential: boolean;
  /** Wall time in ms since the epoch, which the renewal is scheduled by. */
  readonly wallTime: () => number;
  /** The clock of the renewal timer. */
  readonly clock: Clock;
  /** Where the node's mesh diagnostics go. */
  readonly logger: Logger;

  /**
   * The slot gates of the types published with {@link publishSharded}; the
   * {@link client}'s slot handler once one is published.
   */
  readonly gates = new SlotGates();

  readonly #brokered = new Set<MuxConnection>();
  readonly #published = new Map<string, ServiceAddress>();
  /** The slot spaces of the types published sharded here, by `Name.key`, once defined. */
  readonly #spaces = new Map<string, SlotSpace>();
  #renewTimer: TimerHandle | undefined;
  #nextRenewalAt: number | undefined;
  #leaving = false;
  #leavePromise: Promise<LeaveReport> | undefined;

  private constructor(
    switchboard: Switchboard,
    client: NamingClient,
    resolver: NamingResolver,
    options: MeshNodeOptions,
    clock: Clock,
    logger: Logger,
  ) {
    this.switchboard = switchboard;
    this.client = client;
    this.resolver = resolver;
    this.leaveTimeout = options.leaveTimeout ?? 5000;
    this.brokeredPolicy = options.brokeredPolicy ?? ChannelPolicies.scoped();
    this.renewCredential = options.renewCredential ?? true;
    this.wallTime = options.wallTime ?? Date.now;
    this.clock = clock;
    this.logger = logger;
  }

  /**
   * Joins the mesh whose naming service listens at [namingEndpoint].
   *
   * Creates the client with {@link namingClientFor} (passing
   * `reconnectDelay`, `talkOptions` and `watch`), sets
   * `switchboard.resolver` to a {@link NamingResolver} with
   * `resolveTimeout`, replacing any resolver the node had, and starts the
   * client. Returns at once; the client keeps connecting in the background
   * until {@link leave}, and {@link synced} resolves once the table is
   * mirrored. With `renewCredential` (the default) the renewal of the
   * node's credential is scheduled (see {@link renewCredential}).
   *
   * @throws {RangeError} for a negative `reconnectDelay` or `leaveTimeout`.
   */
  static join(
    switchboard: Switchboard,
    namingEndpoint: string,
    options: MeshNodeOptions = {},
  ): MeshNode {
    if (options.leaveTimeout !== undefined && !(options.leaveTimeout >= 0)) {
      throw new RangeError(`leaveTimeout must not be negative: ${options.leaveTimeout}`);
    }
    const clock = options.clock ?? switchboard.clock;
    const logger = options.logger ?? switchboard.logger;
    const client = namingClientFor(switchboard, namingEndpoint, {
      reconnectDelay: options.reconnectDelay,
      talkOptions: options.talkOptions,
      watch: options.watch,
      clock,
      logger,
    });
    const resolver = new NamingResolver(
      client,
      options.resolveTimeout === undefined ? {} : { resolveTimeout: options.resolveTimeout },
    );
    switchboard.resolver = resolver;
    const node = new MeshNode(switchboard, client, resolver, options, clock, logger);
    client.connectHandler = node.#dialBack;
    void client.start().catch(() => {});
    node.#scheduleRenewal();
    return node;
  }

  /** The connections the node dialled for `CONNECT` requests that are still open. */
  get brokeredConnections(): MuxConnection[] {
    return [...this.#brokered];
  }

  /**
   * When the node renews its credential next, in ms since the epoch (by
   * {@link wallTime}), while a renewal is scheduled; undefined otherwise.
   */
  get nextRenewalAt(): number | undefined {
    return this.#nextRenewalAt;
  }

  /** The client's `NamingClient.synced`: resolves once the current session has mirrored the table. */
  get synced(): Promise<void> {
    return this.client.synced;
  }

  /** The addresses of the services published through this object and dispatched locally, with their current instance ids. */
  get published(): ServiceAddress[] {
    return [...this.#published.values()];
  }

  /**
   * Registers a local service of [type] with the naming service and
   * resolves with its instance id.
   *
   * `instance` 0 lets the naming service assign the id; a non-zero id is
   * requested as is. `endpoints` (default: the node's listening endpoints
   * at the time of the call) are what other nodes connect to. Once the id
   * is known, [handler] is registered with `Switchboard.registerService`
   * for exactly `(type, id)`, so channels addressed to this instance, and
   * channels to any instance of [type] that reach this node, are
   * dispatched to it. If a reconnect to the naming service has to assign a
   * different id (the old one was taken), the local registration moves to
   * the new id.
   *
   * Rejects like `NamingClient.register`, and with `FAILED_PRECONDITION`
   * after {@link leave}.
   */
  publish(
    type: NameLike,
    handler: ChannelHandler,
    options: MeshPublishOptions = {},
  ): Promise<number> {
    if (this.#leaving) {
      return Promise.reject(SwitchboardError.of(StatusCode.failedPrecondition, 'left mesh'));
    }
    let name: Name;
    try {
      name = toName(type);
    } catch (error) {
      return Promise.reject(error);
    }
    return this.#publish(name, handler, options, undefined, undefined);
  }

  /**
   * Publishes [relay] as this node's `_relay` service (`Services.relay`):
   * registers the type like {@link publish} (with `instance` and
   * `endpoints`, by default the node's listening endpoints, which consumers
   * and instances must be able to reach) and dispatches `_relay` channels
   * to `RelayService.handler`. The record's metadata is the identity
   * (UTF-8) of the node's `Switchboard.credential`, which consumers name as
   * the receiver of their `IDENT` (wiki page "Polyverse Switchboard
   * Identity and Credentials", section "Finding a relay"); a node without a
   * credential publishes empty metadata, and a warning is logged, since
   * consumers then cannot name it (and it cannot broker). The credential
   * needs `register` for `_relay`.
   *
   * @throws {TypeError} when [relay] forwards through another node.
   * Rejects like {@link publish}.
   */
  publishRelay(relay: RelayService, options: MeshPublishOptions = {}): Promise<number> {
    if (relay.switchboard !== this.switchboard) {
      throw new TypeError('the relay forwards through another node');
    }
    if (this.#leaving) {
      return Promise.reject(SwitchboardError.of(StatusCode.failedPrecondition, 'left mesh'));
    }
    const identity = this.switchboard.credential?.identity;
    if (identity === undefined) {
      this.#log(
        'warn',
        'publishing a relay without a credential: consumers cannot name it when they ' +
          'identify, and it cannot broker connections',
      );
    }
    return this.#publish(
      Services.relay,
      relay.handler,
      options,
      identity === undefined ? undefined : utf8Encode(identity),
      undefined,
    );
  }

  #publish(
    type: Name,
    handler: ChannelHandler,
    options: MeshPublishOptions,
    metadata: Uint8Array | undefined,
    onId: ((id: number) => void) | undefined,
  ): Promise<number> {
    let current: ServiceAddress | undefined;
    return this.client.register(type, options.endpoints ?? this.switchboard.listeningEndpoints, {
      instance: options.instance ?? 0,
      ...(metadata !== undefined && { metadata }),
      onAssigned: (id) => {
        if (this.#leaving) {
          return;
        }
        const previous = current;
        if (previous !== undefined && this.#published.delete(previous.key)) {
          this.switchboard.unregisterService(type, previous.instance);
        }
        const address = new ServiceAddress(type, id);
        current = address;
        this.#published.set(address.key, address);
        this.switchboard.registerService(type, handler, { instance: id });
        onId?.(id);
      },
    });
  }

  /**
   * Publishes a sharded service of [type]: creates its {@link SlotGate}
   * with [lifecycle], registers the type like {@link publish} (with the
   * gate's handler, under `instance`, which stateful services set to a
   * stable configured id so that holders survive a reboot), defines or
   * confirms the slot space (`SLOTS` with `count`, `mode`, `lazy`, `shared`
   * and this instance's `capacity`; 0 takes no slots from the allocator)
   * and, when `holding` lists slots whose storage this instance has,
   * declares them (`HOLDING`); the slots it should discard reach
   * `SlotLifecycle.discard` (through `SlotGate.onDiscard`, as do those of
   * the `HOLDING` the naming client sends again after a reconnect).
   * Resolves with the gate once all of that is done; in a managed space
   * slots may already be assigned by then.
   *
   * The node's {@link gates} become the naming client's slot handler. The
   * remaining options configure the gate (see `SlotGate`), whose timers and
   * diagnostics are this node's {@link clock} and {@link logger}.
   *
   * Rejects with `FAILED_PRECONDITION` if [type] is already published
   * sharded here, if the client has a slot handler other than
   * {@link gates}, or after {@link leave}; otherwise like
   * `NamingClient.register`, `NamingClient.defineSlots` (for example
   * `FAILED_PRECONDITION` when the space is defined differently) and
   * `NamingClient.declareHolding`, and like the `SlotGate` constructor
   * (an {@link Error} for a lifecycle attached to another gate that is not
   * closed, a {@link RangeError} for negative bounds). On failure the type
   * is unregistered again and the gate closed. Both requests' errors are
   * handled even if one fails before the other finishes.
   */
  async publishSharded(
    type: NameLike,
    lifecycle: SlotLifecycle,
    options: MeshPublishShardedOptions,
  ): Promise<SlotGate> {
    if (this.#leaving) {
      throw SwitchboardError.of(StatusCode.failedPrecondition, 'left mesh');
    }
    const name = toName(type);
    if (this.gates.get(name) !== undefined) {
      throw SwitchboardError.of(
        StatusCode.failedPrecondition,
        `${name} is already published sharded`,
      );
    }
    const current = this.client.slotHandler;
    if (current !== undefined && current !== this.gates) {
      throw SwitchboardError.of(
        StatusCode.failedPrecondition,
        'the naming client has another slot handler',
      );
    }
    this.client.slotHandler = this.gates;
    const gate = new SlotGate(this.switchboard, this.client, name, {
      lifecycle,
      forwardGrace: options.forwardGrace,
      drainTimeout: options.drainTimeout,
      maxQueuedChannels: options.maxQueuedChannels,
      maxQueuedRequests: options.maxQueuedRequests,
      trackChannels: options.trackChannels,
      noSlotHandler: options.noSlotHandler,
      instance: options.instance,
      clock: this.clock,
      logger: this.logger,
    });
    this.gates.add(gate);
    let id = 0;
    try {
      id = await this.#publish(name, gate.handler, options, undefined, (assigned) => {
        gate.instance = assigned;
      });
      const space = new SlotSpace(name, {
        count: options.count,
        mode: options.mode ?? SlotMode.managed,
        lazy: options.lazy ?? false,
        shared: options.shared ?? false,
      });
      // Start SLOTS and HOLDING back to back, so that the naming service
      // sees them in one burst and no round trip separates them.
      const held = [...(options.holding ?? [])];
      const defined = this.client.defineSlots(name, {
        count: space.count,
        mode: space.mode,
        lazy: space.lazy,
        shared: space.shared,
        capacity: options.capacity ?? 1,
      });
      const declared = held.length > 0 ? this.client.declareHolding(name, held) : undefined;
      // Observed at once, so that a HOLDING failing while SLOTS is pending
      // (its slots are checked before it is sent) is not an unhandled
      // rejection; awaited below.
      declared?.catch(() => {});
      await defined;
      // Known from here on, so that a leave() meanwhile withdraws this
      // node's capacity for the space.
      this.#spaces.set(name.key, space);
      // The slots to discard reach lifecycle.discard through the gate.
      await declared;
      return gate;
    } catch (error) {
      this.gates.remove(name);
      this.#spaces.delete(name.key);
      if (id !== 0 && !this.#leaving) {
        await this.unpublish(name, id).catch(() => {});
      }
      await gate.close();
      throw error;
    }
  }

  /** The gate of [type], published here with {@link publishSharded}. */
  #gateOf(type: NameLike): SlotGate {
    const name = toName(type);
    const gate = this.gates.get(name);
    if (gate === undefined) {
      throw SwitchboardError.of(
        StatusCode.failedPrecondition,
        `${name} is not published sharded here`,
      );
    }
    return gate;
  }

  /**
   * Claims the free [slot] of [type], published with {@link publishSharded}
   * (`SlotGate.claim`): the slot is loaded before this resolves. Resolves
   * with its epoch. Rejects with `ALREADY_EXISTS` if another instance owns
   * it, `FAILED_PRECONDITION` if [type] is not published sharded here, and
   * like `NamingClient.claim`.
   */
  async claimSlot(
    type: NameLike,
    slot: number,
    options: { holding?: boolean } = {},
  ): Promise<number> {
    return this.#gateOf(type).claim(slot, options);
  }

  /**
   * Releases [slot] of [type] (`SlotGate.release`) and stops serving it
   * here; `keepStorage` keeps this instance its holder. Rejects like
   * `NamingClient.release`, and with `FAILED_PRECONDITION` if [type] is not
   * published sharded here.
   */
  async releaseSlot(
    type: NameLike,
    slot: number,
    options: { keepStorage?: boolean } = {},
  ): Promise<void> {
    return this.#gateOf(type).release(slot, options);
  }

  /**
   * Unregisters `type/instance`, published through this object, from the
   * naming service, then stops dispatching it locally.
   *
   * Rejects like `NamingClient.unregister` (`NOT_FOUND` if it was not
   * published here) and with `FAILED_PRECONDITION` after {@link leave}. The
   * local registration is removed even if the naming service refuses.
   */
  async unpublish(type: NameLike, instance: number): Promise<void> {
    if (this.#leaving) {
      throw SwitchboardError.of(StatusCode.failedPrecondition, 'left mesh');
    }
    const name = toName(type);
    try {
      await this.client.unregister(name, instance);
    } finally {
      const address = new ServiceAddress(name, instance);
      if (this.#published.delete(address.key)) {
        this.switchboard.unregisterService(name, instance);
      }
    }
  }

  /**
   * Moves [slot] of [type] to instance `to` (0, the default, lets the
   * allocator choose) through the hand-over (`NamingClient.migrate`); any
   * node may ask. The iterable yields the phases and ends when the
   * migration is done.
   */
  migrateSlot(
    type: NameLike,
    slot: number,
    options: { to?: number; signal?: AbortSignal } = {},
  ): AsyncIterable<PhaseItem> {
    return this.client.migrate(type, slot, options);
  }

  /**
   * The distinct instances the slots of [type] are routed to, from the
   * mirror (`NamingClient.slotOwners`): for example to reach every node that
   * serves a slot of [type].
   */
  slotOwners(type: NameLike): Set<number> {
    return this.client.slotOwners(type);
  }

  // Identity ----------------------------------------------------------------

  /** Serves a `CONNECT` relayed by the naming service: dials the consumer with its intent, naming it as the receiver. */
  readonly #dialBack = async (request: DialBackRequest): Promise<void> => {
    if (this.#leaving) {
      throw SwitchboardError.of(StatusCode.failedPrecondition, 'left mesh');
    }
    if (this.switchboard.credential === undefined) {
      throw SwitchboardError.of(StatusCode.failedPrecondition, 'no credential to identify with');
    }
    const connection = await this.switchboard.dial(request.endpoint, {
      policy: this.brokeredPolicy,
      intent: request.intent,
      receiver: request.requester,
    });
    if (this.#leaving) {
      void goAwayQuietly(connection);
      throw SwitchboardError.of(StatusCode.failedPrecondition, 'left mesh');
    }
    this.#brokered.add(connection);
    void connection.done.then(
      () => this.#brokered.delete(connection),
      () => this.#brokered.delete(connection),
    );
    this.#log(
      'info',
      `dialled back ${request.endpoint} for "${
        request.requester === '' ? 'an unidentified consumer' : request.requester
      }"`,
    );
  };

  /**
   * Schedules the renewal of the node's credential at two thirds of its
   * lifetime, or at once when that is past; after [retryIn] ms instead when
   * given.
   */
  #scheduleRenewal(retryIn?: number): void {
    if (this.#renewTimer !== undefined) {
      this.clock.clearTimeout(this.#renewTimer);
      this.#renewTimer = undefined;
    }
    this.#nextRenewalAt = undefined;
    const credential = this.switchboard.credential;
    if (!this.renewCredential || this.#leaving || credential === undefined) {
      return;
    }
    if (credential.expiresAt === 0) {
      return;
    }
    const now = this.wallTime();
    let delay: number;
    if (retryIn !== undefined) {
      delay = retryIn;
    } else {
      const lifetime = (credential.expiresAt - credential.issuedAt) * 1000;
      const at = credential.issuedAt * 1000 + Math.trunc((lifetime * 2) / 3);
      delay = Math.max(0, at - now);
    }
    this.#nextRenewalAt = now + delay;
    this.#renewTimer = this.clock.setTimeout(() => {
      this.#renewTimer = undefined;
      this.#nextRenewalAt = undefined;
      void this.#renew();
    }, delay);
  }

  async #renew(): Promise<void> {
    const current = this.switchboard.credential;
    if (this.#leaving || current === undefined) {
      return;
    }
    try {
      const renewed = await this.client.renew({ current });
      if (this.#leaving) {
        return;
      }
      await this.switchboard.updateCredential(renewed);
      this.#log(
        'info',
        `credential of "${renewed.identity}" renewed until ${expiryOf(renewed) ?? 'never'}`,
      );
      this.#scheduleRenewal();
    } catch (error) {
      if (this.#leaving) {
        return;
      }
      const left = current.expiresAt === 0 ? 0 : current.expiresAt * 1000 - this.wallTime();
      if (left <= 0) {
        this.#log(
          'error',
          `credential of "${current.identity}" expired without renewal: ${error}`,
          error,
        );
        return;
      }
      // Again in a quarter of the time left, at least a second later.
      let retry = Math.trunc(left / 4);
      if (retry < 1000) {
        retry = left < 1000 ? left : 1000;
      }
      this.#log('warn', `renewing the credential failed, again in ${retry} ms: ${error}`, error);
      this.#scheduleRenewal(retry);
    }
  }

  // Leaving -----------------------------------------------------------------

  /**
   * Leaves the mesh: hands over or gives up the slots of the types
   * published with {@link publishSharded}, closes the resolver and with it
   * the client (the naming service then drops every registration of this
   * node and publishes `DOWN` for each), stops dispatching the published
   * services locally, and removes the resolver from {@link switchboard} if
   * it is still installed there. Channels already open are unaffected.
   *
   * The slots are handled while connected to the naming service, and while
   * the gates (and their `noSlotHandler`, which serves state transfers) are
   * still up:
   *
   * 1. For each managed type, `SLOTS` with capacity 0, so that the
   *    allocator gives this node no slot back.
   * 2. In a holder-only managed space, every served slot is handed over,
   *    one after the other (types in parallel, at most 64 migrations at a
   *    time), with a migration to an instance the allocator chooses
   *    ({@link migrateSlot} with `to` 0): the new owner takes the drained
   *    state over (fetching it from this node) while this node is still
   *    up, so no data stays behind on a node that is about to stop. Steps
   *    1 and 2 take at most {@link leaveTimeout} in all.
   * 3. Every slot still served (not handed over in time, a space with no
   *    other instance to take it, a static space, or a `shared` space,
   *    whose state is in shared storage and is reassigned at once) is
   *    released keeping its storage (`RELEASE`, `SlotGate.release`), at
   *    most {@link leaveTimeout} more, 64 at a time. In a holder-only
   *    managed space the allocator then assigns such a slot elsewhere, the
   *    new owner fetching the state from this node only while it is
   *    reachable.
   *
   * Then every gate is closed (`SlotGate.close`). The returned
   * {@link LeaveReport} tells which slots were handed over, released, or
   * dropped (stopped here without the naming service taking them: not
   * connected, or the release failed). A node that is going to come back
   * with its storage (a reboot) should rather not leave, so that its
   * holder-only slots wait for it.
   *
   * Leaving first stops renewing the credential and serving `CONNECT`, and
   * sends GOAWAY on the {@link brokeredConnections}. Never rejects.
   *
   * Call this before closing {@link switchboard} (it also cancels the
   * renewal timer, which a node that is closed without leaving keeps until
   * it fires). Calling it again returns the same promise.
   */
  leave(): Promise<LeaveReport> {
    this.#leavePromise ??= this.#leave();
    return this.#leavePromise;
  }

  /** {@link leave}, for `await using`. */
  async [Symbol.asyncDispose](): Promise<void> {
    await this.leave();
  }

  async #leave(): Promise<LeaveReport> {
    this.#leaving = true;
    if (this.#renewTimer !== undefined) {
      this.clock.clearTimeout(this.#renewTimer);
      this.#renewTimer = undefined;
    }
    this.#nextRenewalAt = undefined;
    this.client.connectHandler = undefined;
    for (const connection of [...this.#brokered]) {
      void goAwayQuietly(connection);
    }
    const gates = [...this.gates.gates.values()];
    const served = new Map<string, Set<number>>();
    for (const gate of gates) {
      served.set(gate.type.key, new Set(gate.servedSlots.keys()));
    }
    const handedOver = new Map<string, Set<number>>();
    const released = new Map<string, Set<number>>();
    if (this.client.isConnected && gates.length > 0) {
      const start = this.clock.now();
      let over = false;
      const left = (): number => this.leaveTimeout - (this.clock.now() - start);
      const window = new Window(leaveWindow);
      const handOvers = Promise.all(
        gates.map((gate) => {
          const done = new Set<number>();
          handedOver.set(gate.type.key, done);
          return this.#handOver(gate, window, left, () => over, done);
        }),
      );
      if (!(await this.#within(handOvers, this.leaveTimeout))) {
        this.#log('warn', `leaving: slots not handed over within ${this.leaveTimeout} ms`);
      }
      over = true;
      if (this.client.isConnected) {
        let stop = false;
        const releases = this.#releaseAll(released, () => stop);
        if (!(await this.#within(releases, this.leaveTimeout))) {
          this.#log('warn', `leaving: slots not released within ${this.leaveTimeout} ms`);
        }
        stop = true;
      }
    }
    const dropped = new Map<string, Set<number>>();
    for (const [key, slots] of served) {
      const kept = new Set<number>();
      for (const slot of slots) {
        if (!handedOver.get(key)?.has(slot) && !released.get(key)?.has(slot)) {
          kept.add(slot);
        }
      }
      dropped.set(key, kept);
    }
    const report = new LeaveReport({ handedOver, released, dropped });
    if (report.dropped.size > 0) {
      this.#log('warn', `leaving: slots dropped: ${showSlots(report.dropped)}`);
    }
    await Promise.allSettled([...this.gates.gates.values()].map((gate) => gate.close()));
    if (this.switchboard.resolver === this.resolver) {
      this.switchboard.resolver = undefined;
    }
    try {
      await this.resolver.close();
    } catch (error) {
      this.#log('debug', `leaving: the naming client did not close cleanly: ${error}`, error);
    }
    for (const address of this.#published.values()) {
      this.switchboard.unregisterService(address.type, address.instance);
    }
    this.#published.clear();
    return report;
  }

  /** Whether [work] settled within [ms] on the node's clock (it goes on either way); never rejects. */
  async #within(work: Promise<unknown>, ms: number): Promise<boolean> {
    try {
      await bounded(work, ms, this.clock, () => timedOut);
      return true;
    } catch (error) {
      if (error === timedOut) {
        return false;
      }
      this.#log('debug', `leaving: ${error}`, error);
      return true;
    }
  }

  /**
   * Takes this instance out of the allocator for [gate]'s type, then, in a
   * holder-only managed space, hands every served slot over to another
   * instance, one at a time and each in its turn of [window], while [left]
   * has time and [over] is false. Adds the slots handed over to [done].
   */
  async #handOver(
    gate: SlotGate,
    window: Window,
    left: () => number,
    over: () => boolean,
    done: Set<number>,
  ): Promise<void> {
    const type = gate.type;
    const space = this.#spaces.get(type.key);
    if (space === undefined || space.mode !== SlotMode.managed) {
      return;
    }
    try {
      await this.client.defineSlots(type, {
        count: space.count,
        mode: space.mode,
        lazy: space.lazy,
        shared: space.shared,
        capacity: 0,
      });
    } catch (error) {
      this.#log('info', `leaving: capacity of ${type} not withdrawn: ${error}`, error);
    }
    if (space.shared) {
      // The state is in shared storage: a RELEASE is enough.
      return;
    }
    for (const slot of [...gate.servedSlots.keys()].sort((a, b) => a - b)) {
      await window.enter();
      try {
        const time = left();
        if (over() || time <= 0 || !this.client.isConnected) {
          return;
        }
        if (!gate.serves(slot)) {
          continue;
        }
        await bounded(drain(this.client.migrate(type, slot)), time, this.clock, () => timedOut);
        done.add(slot);
      } catch (error) {
        if (error === timedOut) {
          this.#log('info', `leaving: ${type}/${slot} not handed over in time`);
          return;
        }
        this.#log('info', `leaving: ${type}/${slot} not handed over: ${error}`, error);
      } finally {
        window.exit();
      }
    }
  }

  /**
   * Releases the slots the gates still serve, keeping the storage, at most
   * {@link leaveWindow} at a time and until [stop]; adds those released to
   * [released].
   */
  async #releaseAll(released: Map<string, Set<number>>, stop: () => boolean): Promise<void> {
    const work: [SlotGate, number][] = [];
    for (const gate of this.gates.gates.values()) {
      released.set(gate.type.key, new Set());
      for (const slot of [...gate.servedSlots.keys()].sort((a, b) => a - b)) {
        work.push([gate, slot]);
      }
    }
    let next = 0;
    const releasing = async (): Promise<void> => {
      while (next < work.length && !stop()) {
        const [gate, slot] = work[next++] as [SlotGate, number];
        try {
          await gate.release(slot, { keepStorage: true });
          released.get(gate.type.key)?.add(slot);
        } catch (error) {
          this.#log('info', `leaving: ${gate.type}/${slot} not released: ${error}`, error);
        }
      }
    };
    await Promise.all(
      Array.from({ length: Math.min(leaveWindow, work.length) }, () => releasing()),
    );
  }

  #log(level: LogLevel, message: string, error?: unknown): void {
    try {
      this.logger.log(level, logName, message, error);
    } catch {
      // A logger that throws must not break the mesh node.
    }
  }
}

/** What a bound of {@link MeshNode.leave} rejects with when its time is up. */
const timedOut: unique symbol = Symbol('timed out');

/** Takes every item of [items] until they end; rejects as the iteration does. */
async function drain(items: AsyncIterable<unknown>): Promise<void> {
  for await (const _ of items) {
    // Only the end matters.
  }
}

/**
 * Lets at most [size] operations in at a time; the others wait for their
 * turn, in order.
 */
class Window {
  readonly size: number;
  #inside = 0;
  readonly #waiting: Deferred<void>[] = [];

  constructor(size: number) {
    this.size = size;
  }

  /** Resolves when it is the caller's turn; {@link exit} ends it. */
  enter(): Promise<void> {
    if (this.#inside < this.size) {
      this.#inside++;
      return Promise.resolve();
    }
    const turn = new Deferred<void>();
    this.#waiting.push(turn);
    return turn.promise;
  }

  exit(): void {
    const turn = this.#waiting.shift();
    if (turn !== undefined) {
      // The turn passes on.
      turn.resolve();
    } else {
      this.#inside--;
    }
  }
}

/** The expiry of [credential] as an ISO date, or undefined when it never expires. */
function expiryOf(credential: Credential): string | undefined {
  return credential.expiresAt === 0
    ? undefined
    : new Date(credential.expiresAt * 1000).toISOString();
}
