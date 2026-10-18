/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * Where the `_bulk` channels of a connection go (wiki page "Polyverse
 * Switchboard Addressing and Dispatch", reserved type `_bulk`): per parent
 * channel, the target that takes them. Talk registers its channels,
 * `pipeChannels` its piped ones, a slot channel moves its target to the
 * channel that replaced it, and {@link installBulkRouting} (which every
 * `TalkChannel` installs on its connection) routes what the peer opens.
 *
 * Internal to the package: exported for the node and the proxies, not part
 * of the public API.
 *
 * @internal
 */

import { ChannelAddress } from '../address/channel-address.ts';
import { readU48 } from '../bytes.ts';
import type { MuxChannel, MuxConnection } from '../mux/contract.ts';
import { ProtocolError, Status, StatusCode } from '../status.ts';
import type { TimerHandle } from '../util/clock.ts';
import { TalkBulkOpen } from './talk-bulk.ts';
import { noop, talkLoggerName } from './talk-util.ts';

/**
 * Takes a bulk channel the peer opened for a parent channel, with its bulk
 * number: the parent's Talk layer, or a pipe forwarding it.
 *
 * @internal
 */
export type BulkTarget = (bulk: MuxChannel, number: number) => void;

/**
 * Bulk channels held for a parent that no target took yet, beyond which a
 * `_bulk` OPEN is closed RESOURCE_EXHAUSTED; also the bound of the other
 * per-connection backlogs of bulk routing.
 *
 * @internal
 */
export const maxHeldBulk = 16;

/**
 * How long, in milliseconds, a bulk channel is held for a parent that no
 * target took yet; then it is closed FAILED_PRECONDITION. Held channels
 * are not read, so what they buffer counts toward the connection's receive
 * high-water mark meanwhile.
 *
 * @internal
 */
export const heldBulkTimeout = 10_000;

/** A bulk channel waiting for its parent's target. */
interface HeldBulk {
  readonly bulk: MuxChannel;
  readonly number: number;
  readonly timer: TimerHandle;
}

/** The routing state of one parent channel. */
interface Route {
  readonly parent: MuxChannel;
  target: BulkTarget | undefined;
  held: HeldBulk[];
  /** The target will not linger ({@link releaseBulkTarget}): a graceful end of the parent keeps nothing for it. */
  released: boolean;
}

const routes = new WeakMap<MuxConnection, Map<number, Route>>();
const bulkChannels = new WeakSet<MuxChannel>();
const pendingParents = new WeakMap<MuxConnection, PendingParents>();
const endedParents = new WeakMap<MuxConnection, EndedParents>();

/** The target registered for a parent, kept after the parent closed for {@link followBulkTarget}. */
const targets = new WeakMap<MuxChannel, BulkTarget>();

/**
 * Marks [channel] as a bulk channel, which no bulk channel may name as its
 * parent.
 *
 * @internal
 */
export function markBulkChannel(channel: MuxChannel): void {
  bulkChannels.add(channel);
}

/**
 * Whether [channel] is a bulk channel.
 *
 * @internal
 */
export function isBulkChannel(channel: MuxChannel): boolean {
  return bulkChannels.has(channel);
}

/**
 * [target] takes the bulk channels the peer opens for [parent] from now
 * on, and first those held for it until now, in order. Does nothing once
 * [parent] is closed.
 *
 * @internal
 */
export function registerBulkTarget(parent: MuxChannel, target: BulkTarget): void {
  targets.set(parent, target);
  if (parent.state === 'closed') {
    return;
  }
  const route = routeOf(parent);
  route.target = target;
  pendingParents.get(parent.connection)?.dispatch(parent);
  const held = route.held;
  route.held = [];
  const clock = parent.connection.options.clock;
  for (const entry of held) {
    clock.clearTimeout(entry.timer);
    target(entry.bulk, entry.number);
  }
}

/**
 * The target of [from], if any, takes the bulk channels of [to] too: [to]
 * replaced [from] (a slot channel retried after MOVED).
 *
 * @internal
 */
export function followBulkTarget(from: MuxChannel, to: MuxChannel): void {
  const target = targets.get(from);
  if (target !== undefined) {
    registerBulkTarget(to, target);
  }
}

/**
 * [target], which took the bulk channels the peer opened for [parent],
 * takes those that name [parent] after it ended, until [until] settles: on
 * a connection whose channels are not ordered (`hasOrderedChannels`
 * false), a bulk channel the peer opened before its graceful close may
 * arrive after it, and is admitted if a message referencing it arrived
 * before the close (wiki page "Polyverse Switchboard QUIC and WebTransport
 * Binding", section "Bulk payloads"). It takes at once those routed for
 * [parent] since it ended, while its owner had not decided yet (held for
 * it, or as for a parent that has not arrived). At most
 * {@link maxHeldBulk} parents of a connection linger at once. False, and
 * nothing is kept, beyond that, on an ordered connection, and for a parent
 * lingering already; then those held for it are refused, as
 * {@link releaseBulkTarget} refuses them.
 *
 * @internal
 */
export function lingerBulkTarget(
  parent: MuxChannel,
  target: BulkTarget,
  until: Promise<void>,
): boolean {
  const connection = parent.connection;
  if (connection.hasOrderedChannels) {
    return false;
  }
  const ended = endedParentsOf(connection);
  if (!ended.add(parent.id, target, until)) {
    ended.release(parent.id);
    return false;
  }
  // A binding connection keeps draining while the parent waits.
  const retain = (connection as { retainForDrain?: unknown }).retainForDrain;
  if (typeof retain === 'function') {
    (retain as (id: number, until: Promise<void>) => void).call(connection, parent.id, until);
  }
  // What arrived between the parent's end and now.
  const clock = connection.options.clock;
  for (const entry of ended.decide(parent.id)) {
    clock.clearTimeout(entry.timer);
    target(entry.bulk, entry.number);
  }
  pendingParents.get(connection)?.dispatch(parent);
  return true;
}

/**
 * [target], which took the bulk channels the peer opened for [parent],
 * will not take any after [parent] ends: on a connection whose channels
 * are not ordered, a graceful end of the parent keeps the target until
 * its owner lingers ({@link lingerBulkTarget}) or calls this, holding
 * what is routed for the parent meanwhile; this refuses what was held
 * (FAILED_PRECONDITION), or, before the end, keeps nothing at the end.
 * Does nothing on an ordered connection.
 *
 * @internal
 */
export function releaseBulkTarget(parent: MuxChannel, target: BulkTarget): void {
  const connection = parent.connection;
  if (connection.hasOrderedChannels) {
    return;
  }
  const route = routes.get(connection)?.get(parent.id);
  if (route !== undefined && route.parent === parent && route.target === target) {
    route.released = true;
  }
  endedParents.get(connection)?.release(parent.id);
}

/**
 * Routes [bulk], a channel the peer opened with the open payload [open],
 * to the target of its parent: closed FAILED_PRECONDITION when no such
 * channel is open (or it is itself a bulk channel), or when something that
 * is not a target reads it; held for a target to come while nothing reads
 * it yet, at most {@link maxHeldBulk}, then RESOURCE_EXHAUSTED. Held
 * channels are closed CANCELLED when the parent closes.
 *
 * On a connection whose channels are not ordered, a bulk channel can
 * arrive before its parent or after it ended: one naming a parent that
 * lingers goes to its target; one naming a parent that ended gracefully
 * while a target took its bulk channels, whose owner has not decided yet
 * whether to linger, is held for it, at most {@link heldBulkTimeout} and
 * {@link maxHeldBulk} per parent (then as below); one naming an id of the
 * peer's parity that is not open is held until the parent arrives, at
 * most {@link heldBulkTimeout}, in a backlog of {@link maxHeldBulk} per
 * connection (beyond it RESOURCE_EXHAUSTED, after it FAILED_PRECONDITION);
 * one naming an id of this side's parity that is not open is refused at
 * once.
 *
 * @internal
 */
export function routeBulkChannel(bulk: MuxChannel, open: TalkBulkOpen): void {
  markBulkChannel(bulk);
  bulk.priority = 'bulk';
  const connection = bulk.connection;
  const parent = connection.channelWithId(open.parentId);
  if (!connection.hasOrderedChannels && (parent === undefined || parent.state === 'closed')) {
    const ended = endedParents.get(connection);
    const lingering = ended?.targetOf(open.parentId);
    if (lingering !== undefined) {
      lingering(bulk, open.number);
      return;
    }
    if (ended?.hold(open.parentId, bulk, open.number) === true) {
      return;
    }
    if (parent === undefined && ended?.failed(open.parentId) === true) {
      // Its payloads failed with it.
      refuse(bulk, Status.of(StatusCode.cancelled, 'the parent channel failed'));
      return;
    }
    // This side's own channels it knows; the peer's may not have arrived
    // yet. Ids are not reused on such a connection.
    const ours = open.parentId % 2 === (connection.isInitiator ? 0 : 1);
    if (parent === undefined && !ours) {
      let pending = pendingParents.get(connection);
      if (pending === undefined) {
        pending = new PendingParents(connection);
        pendingParents.set(connection, pending);
      }
      pending.hold(bulk, open);
      return;
    }
  }
  if (
    parent === undefined ||
    parent === bulk ||
    isBulkChannel(parent) ||
    parent.state === 'closed'
  ) {
    refuse(
      bulk,
      Status.of(StatusCode.failedPrecondition, 'no open parent channel for the bulk channel'),
    );
    return;
  }
  const route = routeOf(parent);
  const target = route.target;
  if (target !== undefined) {
    target(bulk, open.number);
    return;
  }
  if (parent.hasListener) {
    refuse(
      bulk,
      Status.of(StatusCode.failedPrecondition, 'the parent channel takes no bulk payloads'),
    );
    return;
  }
  if (route.held.length >= maxHeldBulk) {
    refuse(
      bulk,
      Status.of(StatusCode.resourceExhausted, 'too many bulk channels before their messages'),
    );
    return;
  }
  const entry: HeldBulk = {
    bulk,
    number: open.number,
    timer: connection.options.clock.setTimeout(() => {
      const before = route.held.length;
      route.held = route.held.filter((held) => held !== entry);
      if (route.held.length !== before) {
        refuse(
          bulk,
          Status.of(
            StatusCode.failedPrecondition,
            'the parent channel took no bulk payloads in time',
          ),
        );
      }
    }, heldBulkTimeout),
  };
  route.held.push(entry);
}

/**
 * Routes [channel], a channel the peer opened whose open payload is of the
 * reserved type `_bulk` ([address]), to the Talk layer of its parent
 * channel; a malformed `_bulk` payload closes it PROTOCOL_ERROR.
 *
 * @internal
 */
export function routeBulk(channel: MuxChannel, address: ChannelAddress): void {
  let open: TalkBulkOpen;
  try {
    open = TalkBulkOpen.decode(address);
  } catch (e) {
    if (e instanceof ProtocolError) {
      void channel.close(e.status).catch(noop);
      return;
    }
    throw e;
  }
  routeBulkChannel(channel, open);
}

const routed = new WeakSet<MuxConnection>();

/**
 * Installs bulk routing on [connection], once: a filter adopting every
 * channel the peer opens with an open payload of the reserved type `_bulk`
 * (routed to its parent's Talk layer, never reaching `incoming` nor a
 * listener policy), and the rule admitting such a channel for an open
 * parent, or for one a binding connection retains for the drain, after
 * this side's GOAWAY (it continues work the connection admitted already).
 * A `TalkChannel` installs it on its connection; a node installs it on
 * every connection.
 *
 * @internal
 */
export function installBulkRouting(connection: MuxConnection): void {
  if (routed.has(connection)) {
    return;
  }
  routed.add(connection);
  connection.adoptIncoming((channel) => {
    let address: ChannelAddress;
    try {
      address = ChannelAddress.decode(channel.openPayload);
    } catch {
      // Not ours to judge: the consumer of `incoming` refuses it.
      return false;
    }
    if (!TalkBulkOpen.isBulk(address)) {
      return false;
    }
    routeBulk(channel, address);
    return true;
  });
  connection.admitAfterGoAway((openPayload) => continuesOpenChannel(connection, openPayload));
}

/**
 * Whether [openPayload] opens a bulk channel for a channel open on
 * [connection], or for one that ended while its reader still waits for its
 * bulk channels (a binding connection retains it for the drain:
 * {@link lingerBulkTarget}): it carries the payload of a message of that
 * channel, work admitted before the GOAWAY.
 */
function continuesOpenChannel(connection: MuxConnection, openPayload: Uint8Array): boolean {
  let address: ChannelAddress;
  try {
    address = ChannelAddress.decode(openPayload);
  } catch {
    return false;
  }
  if (!TalkBulkOpen.isBulk(address) || address.payload.length < TalkBulkOpen.payloadLength) {
    return false;
  }
  const parentId = readU48(address.payload, 0);
  const parent = connection.channelWithId(parentId);
  if (parent !== undefined && parent.state !== 'closed') {
    return true;
  }
  if (endedParents.get(connection)?.isDeciding(parentId) === true) {
    return true;
  }
  const retained = (connection as { retainedForDrain?: unknown }).retainedForDrain;
  return (
    typeof retained === 'function' &&
    (retained as (id: number) => boolean).call(connection, parentId) === true
  );
}

function refuse(bulk: MuxChannel, status: Status): void {
  bulk.connection.options.logger.log(
    'debug',
    talkLoggerName,
    `bulk channel ${bulk.id} refused: ${status}`,
  );
  void bulk.close(status).catch(noop);
}

function routeOf(parent: MuxChannel): Route {
  const connection = parent.connection;
  let byId = routes.get(connection);
  if (byId === undefined) {
    byId = new Map();
    routes.set(connection, byId);
  }
  const existing = byId.get(parent.id);
  if (existing !== undefined && existing.parent === parent) {
    return existing;
  }
  const route: Route = { parent, target: undefined, held: [], released: false };
  byId.set(parent.id, route);
  const map = byId;
  void parent.done.then((status) => {
    if (map.get(parent.id) === route) {
      map.delete(parent.id);
    }
    if (!connection.hasOrderedChannels) {
      if (!status.isOk) {
        endedParentsOf(connection).fail(parent.id);
      } else if (route.target !== undefined && !route.released) {
        // Its bulk channels may still come; its target's owner sees the
        // end later (a reader behind the mux) and decides then.
        endedParentsOf(connection).end(parent.id);
      }
    }
    const held = route.held;
    route.held = [];
    route.target = undefined;
    const clock = connection.options.clock;
    for (const entry of held) {
      clock.clearTimeout(entry.timer);
      void entry.bulk.close(Status.of(StatusCode.cancelled, 'parent channel closed')).catch(noop);
    }
  });
  return route;
}

/**
 * Independent streams can deliver a bulk OPEN before its parent's OPEN.
 * One bounded backlog per connection, not one per unknown id.
 */
class PendingParents {
  readonly #connection: MuxConnection;
  readonly #held = new Map<MuxChannel, { open: TalkBulkOpen; timer: TimerHandle }>();

  constructor(connection: MuxConnection) {
    this.#connection = connection;
    void connection.done.then(() => this.#clear());
  }

  hold(bulk: MuxChannel, open: TalkBulkOpen): void {
    if (this.#held.size >= maxHeldBulk) {
      refuse(bulk, Status.of(StatusCode.resourceExhausted, 'too many unknown bulk parents'));
      return;
    }
    const timer = this.#connection.options.clock.setTimeout(() => {
      this.#held.delete(bulk);
      refuse(bulk, Status.of(StatusCode.failedPrecondition, 'bulk parent did not arrive'));
    }, heldBulkTimeout);
    this.#held.set(bulk, { open, timer });
  }

  dispatch(parent: MuxChannel): void {
    for (const [bulk, { open, timer }] of [...this.#held]) {
      if (open.parentId === parent.id) {
        this.#held.delete(bulk);
        this.#connection.options.clock.clearTimeout(timer);
        routeBulkChannel(bulk, open);
      }
    }
  }

  #clear(): void {
    for (const { timer } of this.#held.values()) {
      this.#connection.options.clock.clearTimeout(timer);
    }
    this.#held.clear();
  }
}

function endedParentsOf(connection: MuxConnection): EndedParents {
  let ended = endedParents.get(connection);
  if (ended === undefined) {
    ended = new EndedParents(connection);
    endedParents.set(connection, ended);
  }
  return ended;
}

/**
 * What a connection whose channels are not ordered remembers of the
 * parents that ended: the targets of those that still wait for bulk
 * channels ({@link lingerBulkTarget}), at most {@link maxHeldBulk}, each
 * until its target is done waiting; those that ended gracefully while a
 * target took their bulk channels, until its owner lingers or declines
 * ({@link releaseBulkTarget}), with the bulk channels held for them
 * meanwhile, at most {@link maxHeldBulk} parents (the oldest goes, its
 * bulk channels refused); and the ids of the last {@link maxHeldBulk}
 * that ended with a failure status, whose late bulk channels are closed
 * CANCELLED (ids are not reused on such a connection). Older ones are
 * forgotten, their late bulk channels taken for ones whose parent has not
 * arrived.
 */
class EndedParents {
  readonly #connection: MuxConnection;
  readonly #targets = new Map<number, BulkTarget>();
  // Both insertion ordered: the oldest goes first.
  readonly #deciding = new Map<number, HeldBulk[]>();
  readonly #failed = new Set<number>();
  #ended = false;

  constructor(connection: MuxConnection) {
    this.#connection = connection;
    void connection.done.then(() => {
      this.#ended = true;
      this.#targets.clear();
      this.#failed.clear();
      const clock = connection.options.clock;
      for (const held of this.#deciding.values()) {
        for (const entry of held) {
          clock.clearTimeout(entry.timer);
        }
      }
      this.#deciding.clear();
    });
  }

  /** [parentId] ended gracefully with a target: what names it is held until its owner decides. */
  end(parentId: number): void {
    if (this.#ended || this.#deciding.has(parentId) || this.#targets.has(parentId)) {
      return;
    }
    if (this.#deciding.size >= maxHeldBulk) {
      const oldest = this.#deciding.keys().next();
      if (oldest.done !== true) {
        this.release(oldest.value);
      }
    }
    this.#deciding.set(parentId, []);
  }

  /** Whether bulk channels naming [parentId] are held for its owner's decision. */
  isDeciding(parentId: number): boolean {
    return this.#deciding.has(parentId);
  }

  /** Holds [bulk] for [parentId], whose owner has not decided yet; false when it is not such a parent. */
  hold(parentId: number, bulk: MuxChannel, number: number): boolean {
    const held = this.#deciding.get(parentId);
    if (held === undefined) {
      return false;
    }
    if (held.length >= maxHeldBulk) {
      refuse(
        bulk,
        Status.of(StatusCode.resourceExhausted, 'too many bulk channels before their messages'),
      );
      return true;
    }
    const entry: HeldBulk = {
      bulk,
      number,
      timer: this.#connection.options.clock.setTimeout(() => {
        const index = held.indexOf(entry);
        if (index >= 0) {
          held.splice(index, 1);
          refuse(
            bulk,
            Status.of(
              StatusCode.failedPrecondition,
              'the parent channel took no bulk payloads in time',
            ),
          );
        }
      }, heldBulkTimeout),
    };
    held.push(entry);
    return true;
  }

  /** The owner of [parentId] lingers: what was held for it, which it takes now. */
  decide(parentId: number): HeldBulk[] {
    const held = this.#deciding.get(parentId) ?? [];
    this.#deciding.delete(parentId);
    return held;
  }

  /** The owner of [parentId] does not linger: what was held for it is refused. */
  release(parentId: number): void {
    const held = this.#deciding.get(parentId);
    if (held === undefined) {
      return;
    }
    this.#deciding.delete(parentId);
    const clock = this.#connection.options.clock;
    for (const entry of held) {
      clock.clearTimeout(entry.timer);
      refuse(
        entry.bulk,
        Status.of(StatusCode.failedPrecondition, 'no open parent channel for the bulk channel'),
      );
    }
  }

  targetOf(parentId: number): BulkTarget | undefined {
    return this.#targets.get(parentId);
  }

  failed(parentId: number): boolean {
    return this.#failed.has(parentId);
  }

  fail(parentId: number): void {
    if (this.#ended) {
      return;
    }
    if (this.#failed.size >= maxHeldBulk) {
      const oldest = this.#failed.values().next();
      if (oldest.done !== true) {
        this.#failed.delete(oldest.value);
      }
    }
    this.#failed.add(parentId);
  }

  add(parentId: number, target: BulkTarget, until: Promise<void>): boolean {
    if (this.#ended || this.#targets.size >= maxHeldBulk || this.#targets.has(parentId)) {
      return false;
    }
    this.#targets.set(parentId, target);
    const done = (): void => {
      if (this.#targets.get(parentId) === target) {
        this.#targets.delete(parentId);
      }
    };
    until.then(done, done);
    return true;
  }
}
