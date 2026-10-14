/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import type { ChannelAddress } from '../address/index.ts';
import type { MuxChannel, MuxChannelCarrier, MuxConnection } from '../mux/contract.ts';
import {
  routeBulk,
  TalkBulkOpen,
  TalkChannel,
  type TalkOptions,
  installBulkRouting as talkBulkRouting,
} from '../talk/index.ts';

/**
 * Where the node meets the Talk layer: wrapping a channel, routing the
 * `_bulk` channels of a connection, and the `_bulk` channels that reach
 * dispatch all the same. One module, so that the node depends on the Talk
 * implementation in one place. Internal.
 */

/** Whether [address] is a Talk bulk channel's (the reserved type `_bulk`). */
export function isBulkAddress(address: ChannelAddress): boolean {
  return TalkBulkOpen.isBulk(address);
}

/** [channel] (a mux channel, or a slot channel carrying one) wrapped in the Talk layer with [options]. */
export function talkOver(
  channel: MuxChannel | MuxChannelCarrier,
  options: Partial<TalkOptions>,
): TalkChannel {
  return new TalkChannel(channel, options);
}

/**
 * Installs the Talk layer's routing of `_bulk` channels on [connection]
 * (idempotent): bulk channels then go to the Talk channel (or pipe) of the
 * parent they name before `incoming`, and are admitted after this side's
 * GOAWAY while their parent is open. The node and the reconnecting client
 * call it on every connection they adopt, before reading `incoming`.
 */
export function installBulkRouting(connection: MuxConnection): void {
  talkBulkRouting(connection);
}

/**
 * A `_bulk` channel that reached dispatch all the same (one the peer
 * opened before bulk routing was installed on its connection): routed to
 * the Talk layer of its parent channel as the installed routing would
 * have, as Dart's dispatch hands it to `TalkChannel.routeBulk`. Without
 * one it is closed FAILED_PRECONDITION; a malformed `_bulk` payload closes
 * it PROTOCOL_ERROR.
 */
export function routeDispatchedBulk(channel: MuxChannel, address: ChannelAddress): void {
  routeBulk(channel, address);
}
