/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import type { ChannelAddress, ServiceAddress } from '../address/index.ts';
import type { PeerIdentity } from '../identity/contract.ts';
import type { MuxChannel, MuxConnection } from '../mux/contract.ts';
import type { Name } from '../name.ts';
import type { Status } from '../status.ts';
import type { TalkChannel, TalkOptions } from '../talk/contract.ts';

/**
 * What the Switchboard layer (wiki page "Polyverse Switchboard Addressing
 * and Dispatch") shares between the node (`@kaetemi/switchboard/node`),
 * the reconnecting client, the naming client and the proxies. The
 * `Switchboard` node itself is the "node, naming and sharding" thread's.
 */

/** A handler of channels dispatched to a service; a throw or a rejection closes the channel with INTERNAL. */
export type ChannelHandler = (channel: IncomingChannel) => unknown;

/** A channel the peer opened, with its address parsed. */
export interface IncomingChannel {
  readonly channel: MuxChannel;
  readonly address: ChannelAddress;
  readonly connection: MuxConnection;
  readonly peerIdentity: PeerIdentity | undefined;
  /**
   * A description of the remote side, for logs: the remote address of an
   * accepted connection, the endpoint of an initiated one, `local` on the
   * accepted side of a connection a node made to its own listener;
   * undefined when unknown.
   */
  readonly remote: string | undefined;
  /** The channel's Talk layer; wraps once, cached. */
  talk(options?: Partial<TalkOptions>): TalkChannel;
  reject(status: Status): Promise<void>;
}

/**
 * A connection policy: evaluated for every channel the peer opens on a
 * connection, after the header is parsed and before any handler. Refused:
 * CLOSE PERMISSION_DENIED; a thrown SwitchboardError: its code; another
 * throw: PERMISSION_DENIED. UNAUTHENTICATED while the peer has no identity
 * holds the channel for the identity timeout.
 */
export type ChannelPolicy = (address: ChannelAddress, connection: MuxConnection) => boolean;

/** A service record: an address, the endpoints it listens on and opaque metadata. */
export interface ServiceRecord {
  readonly address: ServiceAddress;
  /** Endpoint URIs in order of preference; empty: reached through CONNECT (brokering) or a relay. */
  readonly endpoints: readonly string[];
  readonly metadata: Uint8Array;
}

/** An UP (full record) or DOWN (address only) event of a service table. */
export interface ServiceEvent {
  readonly up: boolean;
  readonly record: ServiceRecord;
}

/** How an instance is picked when no shard is given. */
export type SelectionPolicy = 'roundRobin' | 'random';

/** Resolves service types to live instances. */
export interface Resolver {
  /** All known live instances of [type]. */
  resolve(type: Name, options?: { signal?: AbortSignal }): Promise<ServiceRecord[]>;
  readonly events: AsyncIterable<ServiceEvent>;
  /** Resolves once the table has been usable at least once (at once for a static resolver). */
  readonly ready: Promise<void>;
  close(): Promise<void>;
}

/**
 * A resolver that can have an instance which registered without endpoints
 * dial the caller: the naming service's CONNECT (wiki page "Polyverse
 * Switchboard Identity and Credentials", section "Reverse connections").
 * The node brokers a connection through it to an instance whose record
 * has no endpoints (`Switchboard.broker`).
 */
export interface BrokeringResolver extends Resolver {
  /**
   * Has the instance at [address] dial [endpoint] and identify there with
   * [intent] (at most 64 bytes). Resolves once it has, with the identity it
   * presents. [options.timeout] (milliseconds) bounds the whole request.
   */
  connectTo(
    address: ServiceAddress,
    endpoint: string,
    intent: Uint8Array,
    options?: { timeout?: number | undefined; signal?: AbortSignal | undefined },
  ): Promise<string>;
}

/** A hook choosing the policy of a connection the node initiates; undefined: the node's outgoing policy. */
export type EndpointPolicy = (endpoint: string) => ChannelPolicy | undefined;

/** A hook choosing the application payload of an open without one; undefined: empty. */
export type EndpointCredential = (
  endpoint: string,
  record: ServiceRecord | undefined,
) => Uint8Array | undefined | Promise<Uint8Array | undefined>;

/** The receiver an IDENT on an initiated connection names; undefined: nobody. */
export type ExpectedIdentity = (
  endpoint: string,
  record: ServiceRecord | undefined,
) => string | undefined;
