/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import type { ChannelAddress } from '../address/index.ts';
import type { MuxChannel, MuxConnection } from '../mux/contract.ts';
import { Name } from '../name.ts';
import { Status, StatusCode, SwitchboardError } from '../status.ts';
import type { TalkChannel, TalkOptions } from '../talk/contract.ts';

/**
 * Where the node meets the Talk layer: wrapping a channel, routing the
 * `_bulk` channels of a connection, and the `_bulk` channels that reach
 * dispatch all the same. One module, so that the node depends on the Talk
 * implementation in one place. Internal.
 */

/** The reserved type of the channels that carry Talk bulk payloads. */
const bulkType = Name.of('_bulk');

/** Whether [address] is a Talk bulk channel's. */
export function isBulkAddress(address: ChannelAddress): boolean {
  return address.type?.equals(bulkType) ?? false;
}

/** [channel] wrapped in the Talk layer with [options]. */
export function talkOver(channel: MuxChannel, options: TalkOptions): TalkChannel {
  void channel;
  void options;
  throw SwitchboardError.of(StatusCode.unimplemented, 'the Talk layer is not available yet');
}

/**
 * Installs the Talk layer's routing of `_bulk` channels on [connection]
 * (idempotent): bulk channels then go to the Talk channel they name before
 * `incoming`, and are admitted after this side's GOAWAY while their parent
 * is open. The node calls it on every connection it adopts, before reading
 * `incoming`.
 */
export function installBulkRouting(connection: MuxConnection): void {
  void connection;
}

/**
 * A `_bulk` channel that reached dispatch: no Talk layer on its connection
 * claimed it (`MuxConnection.adoptIncoming`), so its parent has none.
 * Refused `FAILED_PRECONDITION`, as the Dart `BulkRoutes` refuses a bulk
 * channel without a Talk parent.
 */
export function refuseBulk(channel: MuxChannel): void {
  try {
    void channel
      .close(Status.of(StatusCode.failedPrecondition, 'no channel for this bulk payload'))
      .catch(() => {});
  } catch {
    // Closing already.
  }
}

/** Takes a bulk channel the peer opened for a parent channel, with its bulk number. */
export type BulkTarget = (bulk: MuxChannel, number: number) => void;

const bulkChannels = new WeakSet<MuxChannel>();

/**
 * The Talk layer's routing of `_bulk` channels to their parent, as a
 * forwarder (the proxy) uses it: the port of Dart's `BulkRoutes`. Until
 * the Talk layer lands, nothing routes bulk channels ({@link
 * installBulkRouting} does nothing and dispatch refuses them), so a target
 * registered here is never called; only the marks are kept.
 */
export const bulkRoutes = Object.freeze({
  /** Bulk channels held for a parent no target took yet; beyond, RESOURCE_EXHAUSTED. */
  maxHeld: 16,
  /** How long a bulk channel is held for a target, in milliseconds. */
  heldTimeout: 10_000,
  /** Marks [channel] as a bulk channel, which no bulk channel may name as its parent. */
  markBulk(channel: MuxChannel): void {
    bulkChannels.add(channel);
  },
  /** Whether [channel] is a bulk channel. */
  isBulk(channel: MuxChannel): boolean {
    return bulkChannels.has(channel);
  },
  /**
   * [target] takes the bulk channels the peer opens for [parent] from now
   * on, and first those held for it until now, in order.
   */
  register(parent: MuxChannel, target: BulkTarget): void {
    void parent;
    void target;
  },
  /**
   * [target] goes on taking the bulk channels that name [parent] after it
   * ended, until [until] settles (a connection whose channels are not
   * ordered); false, keeping nothing, when it cannot.
   */
  linger(parent: MuxChannel, target: BulkTarget, until: Promise<void>): boolean {
    void parent;
    void target;
    void until;
    return false;
  },
});
