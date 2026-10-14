/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import type { ChannelAddress } from '../address/index.ts';
import { Right } from '../identity/contract.ts';
import type { MuxConnection } from '../mux/contract.ts';
import { Name, type NameLike, toName } from '../name.ts';
import { StatusCode, SwitchboardError } from '../status.ts';
import type { ChannelPolicy } from './contract.ts';

/** The naming service, admitted by {@link ChannelPolicies.scoped} for any identified peer. */
const namingType = Name.of('_ns');

/** The relay, admitted by {@link ChannelPolicies.scoped} for any identified peer. */
const relayType = Name.of('_relay');

function unidentified(): SwitchboardError {
  return SwitchboardError.of(StatusCode.unauthenticated, 'the peer has not identified');
}

function scoped(
  address: ChannelAddress,
  connection: MuxConnection,
  requireIdentity: boolean,
): boolean {
  const identity = connection.peerIdentity;
  if (identity === undefined) {
    if (requireIdentity) {
      throw unidentified();
    }
    return true;
  }
  const type = address.type ?? Name.empty;
  return type.equals(namingType) || type.equals(relayType) || identity.allows(Right.open, type);
}

const scopedStrict: ChannelPolicy = (address, connection) => scoped(address, connection, true);
const scopedLenient: ChannelPolicy = (address, connection) => scoped(address, connection, false);

const requireIdentityPolicy: ChannelPolicy = (_address, connection) => {
  if (connection.peerIdentity === undefined) {
    throw unidentified();
  }
  return true;
};

/**
 * Ready-made {@link ChannelPolicy} values (wiki pages "Polyverse
 * Switchboard Addressing and Dispatch", section "Dispatch of incoming
 * channels", and "Identity and Credentials").
 *
 * A listener facing untrusted peers must refuse the reserved types (`_ns`
 * and the rest of the `_` namespace) unless deliberately opened; otherwise
 * a peer could register services in the naming service and receive other
 * peers' channels and credentials.
 *
 * A policy never sees bulk channels (the reserved type `_bulk`): they carry
 * the payload of a message of a channel the policy admitted already, and
 * are routed to that channel's Talk layer before any policy.
 */
export const ChannelPolicies = Object.freeze({
  /** Allows everything: the behaviour of a listener without a policy. For internal listeners only. */
  allowAll: ((_address, _connection) => true) as ChannelPolicy,

  /**
   * Refuses every channel. For connections to untrusted peers that have no
   * business opening channels towards this node, such as the connections a
   * consumer initiates to workers it does not trust (see the node's
   * `outgoingPolicy`).
   */
  denyAll: ((_address, _connection) => false) as ChannelPolicy,

  /**
   * Refuses channels to reserved types (names starting with `_`, such as
   * the naming service `_ns`); allows everything else, including channels
   * without a service type.
   */
  denyReserved: ((address, _connection) => !(address.type?.isReserved ?? false)) as ChannelPolicy,

  /**
   * Allows only channels to the service types in [types] (any instance).
   * Channels without a service type (for the default service) are allowed
   * only with [untyped]. A reserved type is allowed only if it is listed.
   *
   * Returns a new function on every call, and functions are equal only to
   * themselves: create one and reuse it when passing it to the node's
   * `connect({ policy })`, which pools connections per policy object, or
   * every call gets a connection of its own.
   */
  allowTypes(types: Iterable<NameLike>, options: { untyped?: boolean } = {}): ChannelPolicy {
    const allowed = new Set<string>();
    for (const type of types) {
      allowed.add(toName(type).key);
    }
    const untyped = options.untyped ?? false;
    return (address) => {
      const type = address.type;
      return type === undefined ? untyped : allowed.has(type.key);
    };
  },

  /**
   * Admits a channel only if the peer's credential has an `open` scope for
   * its type; refuses the others with `PERMISSION_DENIED`. A channel
   * without a service type is checked as the empty name, which only a `*`
   * pattern matches. The naming service `_ns` is admitted for every
   * identified peer (it checks the naming rights itself, and a worker that
   * may open nothing must still register), and so is the relay `_relay`
   * (it checks the consumer's `open` scope for every channel it relays).
   *
   * A peer that has not identified (or whose credential expired) is
   * refused with `UNAUTHENTICATED` when [requireIdentity] (the default),
   * which makes the node hold its channels until it identifies; without
   * it, the peer passes unchecked, for a mesh moving to identities
   * (combine with another policy through {@link ChannelPolicies.all}).
   *
   * Returns one of two fixed functions, so that calls with the same
   * argument give equal policies (pooling compares policies).
   */
  scoped(options: { requireIdentity?: boolean } = {}): ChannelPolicy {
    return (options.requireIdentity ?? true) ? scopedStrict : scopedLenient;
  },

  /**
   * Admits every channel of an identified peer, and refuses a peer that has
   * not identified (or whose credential expired) with `UNAUTHENTICATED`,
   * holding its channels until it identifies. Always the same function.
   */
  requireIdentity(): ChannelPolicy {
    return requireIdentityPolicy;
  },

  /**
   * Admits a channel only if every policy of [policies] admits it, in
   * order; the first refusal (false or a throw) decides. An empty list
   * admits everything. Returns a new function on every call.
   */
  all(policies: Iterable<ChannelPolicy>): ChannelPolicy {
    const list = [...policies];
    return (address, connection) => {
      for (const policy of list) {
        if (!policy(address, connection)) {
          return false;
        }
      }
      return true;
    };
  },
});
