/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { type ChannelAddress, ServiceAddress } from '../address/index.ts';
import type { MuxChannel } from '../mux/contract.ts';
import { type Status, StatusCode, SwitchboardError } from '../status.ts';
import type { Resolver, ServiceRecord, SlotResolver } from '../switchboard/contract.ts';
import type { Switchboard } from '../switchboard/switchboard.ts';
import { MovedStatus } from './naming-protocol.ts';

const logName = 'Switchboard.Router';

/** Whether [resolver] knows slot tables: a {@link SlotResolver}. */
export function isSlotResolver(resolver: Resolver | undefined): resolver is SlotResolver {
  const candidate = resolver as Partial<SlotResolver> | undefined;
  return (
    typeof candidate?.slotTable === 'function' &&
    typeof candidate.slotOwner === 'function' &&
    typeof candidate.locateSlot === 'function'
  );
}

/** What {@link reopenAtSlotOwner} takes beside the rejected channel's header and status. */
export interface SlotReopenOptions {
  /** The resolver to route through; default the node's. */
  readonly resolver?: Resolver | undefined;
  /** As `SelectOptions.excludeOwnEndpoints`. */
  readonly excludeOwnEndpoints?: boolean | undefined;
  /** Asked before a `LOCATE`; false gives up (undefined). */
  readonly mayLocate?: (() => boolean) | undefined;
  /**
   * Filters the records as `SelectOptions.where`: a target it refuses fails
   * with `NOT_FOUND`, whereas a target the resolver does not know gives up.
   */
  readonly where?: ((record: ServiceRecord) => boolean) | undefined;
  /** The node chooses the application payload for the connection and record selected. */
  readonly implicitPayload?: boolean | undefined;
  /** An owner without endpoints is reached through the node's relay when the node cannot broker. */
  readonly mayRelay?: boolean | undefined;
}

/** Rejects with `UNAVAILABLE` unless [promise] settles within [ms]. */
function bounded<T>(
  switchboard: Switchboard,
  promise: Promise<T>,
  ms: number,
  what: string,
): Promise<T> {
  const clock = switchboard.clock;
  return new Promise<T>((resolve, reject) => {
    const timer = clock.setTimeout(() => {
      reject(SwitchboardError.of(StatusCode.unavailable, `${what} not located within ${ms} ms`));
    }, ms);
    promise.then(
      (value) => {
        clock.clearTimeout(timer);
        resolve(value);
      },
      (error: unknown) => {
        clock.clearTimeout(timer);
        reject(error);
      },
    );
  });
}

/**
 * @internal Opens the replacement of a channel to the shard slot of [header]
 * that instance [rejectedBy] closed with [moved], or resolves with undefined
 * when there is nowhere else to go. The `MOVED` retry shared by
 * `Switchboard.openChannelToSlot` and the proxy.
 *
 * The target is the owner the status names (`MovedStatus`), unless the
 * resolver's table has an entry with a higher epoch; when the status names
 * none, the owner `SlotResolver.locateSlot` finds within
 * `Switchboard.slotRefreshTimeout`. An owner equal to [rejectedBy] does not
 * count, except in a table entry with a higher epoch than the status: the
 * slot came back to it after the rejection, so the retry goes to it.
 * Through a resolver without a table for the type, the target is the named
 * owner, or the same address again (instance 0).
 *
 * The replacement carries [header] with the instance set to the selected
 * one and no host hint; the application payload is unchanged unless
 * `implicitPayload` is set (the per-destination credential of
 * `openChannelToSlot`). Rejects like `Switchboard.selectAndConnect` and
 * `SlotResolver.locateSlot`.
 */
export async function reopenAtSlotOwner(
  switchboard: Switchboard,
  header: ChannelAddress,
  rejectedBy: number,
  moved: Status,
  options: SlotReopenOptions = {},
): Promise<MuxChannel | undefined> {
  const type = header.type;
  const slot = header.shard;
  if (type === undefined || slot === undefined) {
    throw new TypeError('a slot channel header has a type and a shard');
  }
  const log = (message: string): void => {
    try {
      switchboard.logger.log('debug', logName, message);
    } catch {
      // A logger that throws must not break routing.
    }
  };
  const named = MovedStatus.fromStatus(moved);
  let target = 0;
  if (named.hasOwner && named.owner !== rejectedBy) {
    target = named.owner;
  }
  const r = options.resolver ?? switchboard.resolver;
  if (isSlotResolver(r) && r.slotTable(type) !== undefined) {
    const mirrored = r.slotOwner(type, slot);
    if (
      mirrored !== undefined &&
      ((target === 0 && mirrored.owner !== rejectedBy) ||
        (target !== 0 && mirrored.epoch > named.epoch))
    ) {
      target = mirrored.owner;
    }
    if (target === 0) {
      if (options.mayLocate !== undefined && !options.mayLocate()) {
        log(`${type}/${slot}: MOVED by ${rejectedBy}, no LOCATE allowed`);
        return undefined;
      }
      const located = await bounded(
        switchboard,
        r.locateSlot(type, slot),
        switchboard.slotRefreshTimeout,
        `slot ${type}/${slot}`,
      );
      if (located !== undefined && located.owner !== rejectedBy) {
        target = located.owner;
      }
    }
    if (target === 0) {
      log(`${type}/${slot}: MOVED by ${rejectedBy}, no other owner known`);
      return undefined;
    }
  }
  let refused = false;
  const where = options.where;
  const filter =
    where === undefined
      ? undefined
      : (record: ServiceRecord): boolean => {
          const accepted = where(record);
          refused ||= !accepted;
          return accepted;
        };
  try {
    // A pooled connection that went away or filled up in the meantime is
    // replaced once.
    const { channel } = await switchboard.openSelected(new ServiceAddress(type, target), header, {
      ...(r === undefined ? {} : { resolver: r }),
      excludeOwnEndpoints: options.excludeOwnEndpoints ?? false,
      where: filter,
      implicitPayload: options.implicitPayload ?? false,
      mayRelay: options.mayRelay ?? false,
    });
    return channel;
  } catch (error) {
    if (!(error instanceof SwitchboardError) || !error.is(StatusCode.notFound) || refused) {
      throw error;
    }
    // Not in the resolver's table (yet): nowhere to go.
    log(`${type}/${slot}: MOVED by ${rejectedBy}, ${error.status}`);
    return undefined;
  }
}
