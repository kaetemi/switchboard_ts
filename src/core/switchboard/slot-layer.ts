/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import type { ChannelAddress } from '../address/index.ts';
import type { MuxChannel, MuxChannelCarrier } from '../mux/contract.ts';
import type { Name } from '../name.ts';
import { SlotChannel } from '../naming/slot-channel.ts';
import { reopenAtSlotOwner } from '../naming/slot-reopen.ts';
import type { ServiceRecord, SlotResolver } from './contract.ts';
import type { Switchboard } from './switchboard.ts';

/**
 * Where the node meets the sharding layer's slot channel (Dart
 * `SlotChannel` and `reopenAtSlotOwner`): a channel to a slot's owner that
 * is opened once more at the new owner when the owner rejects it MOVED
 * before anything was sent or received on it. One module, so that the
 * node and the proxy depend on the sharding implementation in one place.
 * Internal.
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
 * The proxy's carrier of [retry]'s channel: it reopens the channel at the
 * slot's new owner after MOVED (never at one of the node's own listeners),
 * holding what is sent meanwhile without a byte bound of its own: the
 * proxy bounds it by flow control, returning the client's credit only once
 * the replacement took a subframe, so it holds at most the window it grants
 * the client.
 */
export function slotCarrier(switchboard: Switchboard, retry: SlotRetry): SlotCarrier {
  const { header, resolver, mayLocate } = retry;
  return new SlotChannel(
    retry.type,
    retry.shard,
    retry.channel,
    (moved) =>
      reopenAtSlotOwner(switchboard, header, header.instance, moved, {
        resolver,
        excludeOwnEndpoints: true,
        mayLocate,
      }),
    { maxHeldBytes: 0, logger: switchboard.logger },
  );
}

/** What {@link slotChannel} takes beside the first channel. */
export interface SlotOpen {
  /** The header the first channel was asked for: no instance, the caller's payload if any. */
  readonly header: ChannelAddress;
  /** The instance the first channel went to. */
  readonly rejectedBy: number;
  /** The caller's filter of the records, for the retry too. */
  readonly where: ((record: ServiceRecord) => boolean) | undefined;
  /** The node chooses the payload of the replacement for its destination. */
  readonly implicitPayload: boolean;
}

/**
 * `Switchboard.openChannelToSlot`'s slot channel over [first], the channel
 * opened to the owner of [slot] of [type]: the retry goes through the
 * node's resolver, may go through its relay, and the node's initial window
 * bounds what is held meanwhile.
 */
export function slotChannel(
  switchboard: Switchboard,
  type: Name,
  slot: number,
  first: MuxChannel,
  open: SlotOpen,
): SlotChannel {
  const { header, rejectedBy, where, implicitPayload } = open;
  return new SlotChannel(
    type,
    slot,
    first,
    (moved) =>
      reopenAtSlotOwner(switchboard, header, rejectedBy, moved, {
        where,
        implicitPayload,
        mayRelay: true,
      }),
    { maxHeldBytes: switchboard.muxOptions.initialWindow, logger: switchboard.logger },
  );
}
