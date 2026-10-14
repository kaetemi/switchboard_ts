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
