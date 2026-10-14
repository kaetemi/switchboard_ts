/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * Channel forwarding shared by `proxyHandler` and `RelayService`: the
 * bound on the channels forwarded per client connection and the opening of
 * the outgoing channel (never to the node itself). The piping is
 * `pipeChannels`. Internal to the package; not exported.
 */

import { type ChannelAddress, ServiceAddress } from '../address/index.ts';
import type { MuxChannel, MuxConnection } from '../mux/contract.ts';
import type { Name } from '../name.ts';
import { StatusCode, SwitchboardError } from '../status.ts';
import { parseEndpoint } from '../transport/endpoint.ts';
import type { Resolver, ServiceRecord } from './contract.ts';
import type { Switchboard } from './switchboard.ts';

/** The logger name of the proxy and the relay's forwarding. */
export const routerLogName = 'Switchboard.Router';

/**
 * Counts the channels being forwarded for each client connection, bounded
 * by {@link max} (0: no bound), wiki page "Polyverse Switchboard
 * Proxying": all clients share the forwarding node's connections to a
 * destination, so without a bound one client could use up the channels a
 * destination accepts for everyone.
 */
export class ForwardingBound {
  /** The bound; 0 for none. */
  readonly max: number;

  readonly #counts = new WeakMap<MuxConnection, number>();

  /** A bound of [max] channels per connection; 0 for none. */
  constructor(max: number) {
    this.max = max;
  }

  /** The channels of [connection] being forwarded. */
  countOf(connection: MuxConnection): number {
    return this.#counts.get(connection) ?? 0;
  }

  /**
   * Counts one more channel of [connection]; false, counting nothing, when
   * [connection] has {@link max} already. Each true is matched by an
   * {@link exit} once the channel is done.
   */
  enter(connection: MuxConnection): boolean {
    const count = this.countOf(connection);
    if (this.max > 0 && count >= this.max) {
      return false;
    }
    this.#counts.set(connection, count + 1);
    return true;
  }

  /** A channel of [connection] counted by {@link enter} is done. */
  exit(connection: MuxConnection): void {
    const left = this.countOf(connection) - 1;
    if (left > 0) {
      this.#counts.set(connection, left);
    } else {
      this.#counts.delete(connection);
    }
  }
}

/**
 * The outgoing channel of a forwarded channel, the header it was opened
 * with, and whether it went to a host hint rather than through the
 * resolver.
 */
export interface ForwardTarget {
  readonly channel: MuxChannel;
  readonly header: ChannelAddress;
  readonly hostHint: boolean;
}

/** What {@link openForwarded} takes beside the address. */
export interface OpenForwardedOptions {
  /** The resolver of the destination; undefined: the node's. */
  readonly resolver?: Resolver | undefined;
  /** Which records may be connected to; undefined: all. */
  readonly where?: ((record: ServiceRecord) => boolean) | undefined;
}

/**
 * Opens the outgoing channel for a forwarded channel addressed to
 * [address] (of service [type]): to its host hint when it has one and
 * `Switchboard.allowHostHint` is set, otherwise to the instance the node
 * selects through [options.resolver] (default: the node's) as
 * `Switchboard.selectAndConnect` does, the selected instance filled in and
 * the host hint removed, and only to records [options.where] accepts.
 * Never to the node's own listeners (`Switchboard.isOwnEndpoint`): a
 * record or a hint pointing back at the node would make it forward to
 * itself without end. The application payload is [address]'s, unchanged.
 *
 * Rejects with a `SwitchboardError` with the status of the failure
 * (NOT_FOUND, UNAVAILABLE, ...); a missing resolver or a closing node
 * (FAILED_PRECONDITION locally) is reported as UNAVAILABLE: to the client,
 * the node is simply unable to forward.
 */
export async function openForwarded(
  switchboard: Switchboard,
  address: ChannelAddress,
  type: Name,
  options: OpenForwardedOptions = {},
): Promise<ForwardTarget> {
  try {
    return await open(switchboard, address, type, options);
  } catch (e) {
    if (e instanceof SwitchboardError && e.code === StatusCode.failedPrecondition) {
      throw SwitchboardError.of(StatusCode.unavailable, e.status.reason, { cause: e });
    }
    throw e;
  }
}

async function open(
  switchboard: Switchboard,
  address: ChannelAddress,
  type: Name,
  options: OpenForwardedOptions,
): Promise<ForwardTarget> {
  const host = address.host;
  if (host === undefined || !switchboard.allowHostHint) {
    const { channel, header } = await switchboard.openSelected(
      new ServiceAddress(type, address.instance),
      address,
      { resolver: options.resolver, excludeOwnEndpoints: true, where: options.where },
    );
    return { channel, header, hostHint: false };
  }
  const endpoint = hostHintEndpoint(host);
  if (switchboard.isOwnEndpoint(endpoint)) {
    switchboard.logger.log(
      'warn',
      routerLogName,
      `forwarding: host hint ${host} points at this node itself`,
    );
    throw SwitchboardError.of(StatusCode.unavailable, `host hint ${host} is this node`);
  }
  const header = address.with({ clearHost: true });
  for (let attempt = 0; ; attempt++) {
    const connection = await switchboard.connect(endpoint);
    try {
      return { channel: connection.open(header.encode()), header, hostHint: true };
    } catch (e) {
      // A pooled connection may have received GOAWAY, or filled up to the
      // peer's channel limit, in the meantime; one retry replaces it.
      if (
        attempt > 0 ||
        !(e instanceof SwitchboardError) ||
        (e.code !== StatusCode.failedPrecondition && e.code !== StatusCode.resourceExhausted)
      ) {
        throw e;
      }
    }
  }
}

/** The endpoint of a host hint: a URI, or `host:port` meaning `tcp://host:port`. */
function hostHintEndpoint(host: string): string {
  const uri = host.includes('://') ? host : `tcp://${host}`;
  let problem: string;
  try {
    if (parseEndpoint(uri).host !== '') {
      return uri;
    }
    problem = 'no host';
  } catch (e) {
    problem = e instanceof Error ? e.message : String(e);
  }
  throw SwitchboardError.of(StatusCode.invalidArgument, `bad host hint "${host}": ${problem}`);
}
