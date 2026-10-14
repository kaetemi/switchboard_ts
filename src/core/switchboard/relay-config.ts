/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { utf8Encode } from '../bytes.ts';
import { Name, type NameLike, toName } from '../name.ts';
import { parseNodeEndpoint } from './endpoints.ts';

/** The largest receiver an IDENT names, in bytes of UTF-8. */
const maxReceiverLength = 255;

/** The default type of the relays. */
export const defaultRelayType: Name = Name.of('_relay');

/**
 * How a node that listens nowhere reaches the instances registered without
 * endpoints: through a relay (`RelayService`), the `relay` of a
 * `Switchboard` (wiki page "Polyverse Switchboard Identity and
 * Credentials", section "Relay").
 *
 * The relays are the records of {@link type} in the node's resolver (round
 * robin with failover; each record's metadata is the relay's identity,
 * which the node names as the receiver of its IDENT there), or the
 * explicit {@link endpoints}, in which case {@link identity} is what the
 * node names. The node keeps its connection to a relay pooled like any
 * endpoint connection.
 */
export class RelayConfig {
  /** The relays' service type: `_relay` unless the mesh runs them under another reserved type. */
  readonly type: Name;
  /** Explicit relay endpoints, used instead of resolving {@link type}; undefined to resolve. */
  readonly endpoints: readonly string[] | undefined;
  /** The identity the relays at {@link endpoints} present; undefined names nobody. Ignored when resolving. */
  readonly identity: string | undefined;

  /**
   * @throws {RangeError} for a [type] that is not reserved (its name does
   * not start with `_`: a relay refuses inner channels of reserved types,
   * which keeps relays from chaining only if they run under one), an empty
   * [endpoints] list, an endpoint without a scheme, and an [identity] that
   * is empty or longer than 255 bytes of UTF-8.
   */
  constructor(
    options: {
      type?: NameLike | undefined;
      endpoints?: readonly string[] | undefined;
      identity?: string | undefined;
    } = {},
  ) {
    const type = options.type === undefined ? defaultRelayType : toName(options.type);
    if (!type.isReserved) {
      throw new RangeError(`relay type ${type} must be a reserved name (starting with "_")`);
    }
    const endpoints = options.endpoints;
    if (endpoints !== undefined) {
      if (endpoints.length === 0) {
        throw new RangeError('relay endpoints must not be empty; omit them to resolve the relays');
      }
      for (const endpoint of endpoints) {
        try {
          parseNodeEndpoint(endpoint);
        } catch {
          throw new RangeError(`relay endpoint ${endpoint} needs a scheme`);
        }
      }
    }
    const identity = options.identity;
    if (
      identity !== undefined &&
      (identity.length === 0 || utf8Encode(identity).length > maxReceiverLength)
    ) {
      throw new RangeError(`relay identity must be 1 to ${maxReceiverLength} bytes of UTF-8`);
    }
    this.type = type;
    this.endpoints = endpoints === undefined ? undefined : Object.freeze([...endpoints]);
    this.identity = identity;
  }

  toString(): string {
    return `RelayConfig(${this.type}${this.endpoints === undefined ? '' : ` at ${this.endpoints.join(', ')}`})`;
  }
}
