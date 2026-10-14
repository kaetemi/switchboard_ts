/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import type { ChannelAddress } from '../address/index.ts';
import type { MuxChannel, MuxChannelCarrier } from '../mux/contract.ts';
import type { Name } from '../name.ts';
import type { SlotResolver } from './contract.ts';
import type { Switchboard } from './switchboard.ts';

/**
 * Where the node meets the sharding layer's slot channel (Dart
 * `SlotChannel` and `reopenAtSlotOwner`): a channel to a slot's owner that
 * is opened once more at the new owner when the owner rejects it MOVED
 * before anything was sent or received on it. One module, so that the
 * proxy depends on the sharding implementation in one place. Until it
 * lands, {@link slotCarrier} gives no carrier and the caller uses the
 * channel as it is: a MOVED reaches the client. Internal.
 */

/** A carrier that may replace its mux channel once, until {@link commit}. */
export interface SlotCarrier extends MuxChannelCarrier {
  /** Whether the carrier can still send: true while a replacement is awaited. */
  readonly canSend: boolean;
  /** No retry any more: something was forwarded on it. */
  commit(): void;
}

/** What {@link slotCarrier} needs to reopen a channel at a slot's new owner. */
export interface SlotRetry {
  /** The sharded type. */
  readonly type: Name;
  /** The slot. */
  readonly shard: number;
  /** The channel opened at the owner the table named. */
  readonly channel: MuxChannel;
  /** The header [channel] was opened with. */
  readonly header: ChannelAddress;
  /** The resolver whose table routed it. */
  readonly resolver: SlotResolver;
  /** Whether a LOCATE the retry needs may be made (the proxy's per-client budget). */
  readonly mayLocate: () => boolean;
}

/**
 * A carrier of [retry]'s channel that reopens it at the slot's new owner
 * after MOVED, holding what is sent meanwhile within the window granted to
 * the sender (no bytes beyond flow control); undefined while the sharding
 * layer is not available.
 */
export function slotCarrier(switchboard: Switchboard, retry: SlotRetry): SlotCarrier | undefined {
  void switchboard;
  void retry;
  return undefined;
}
