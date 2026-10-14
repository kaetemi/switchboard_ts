/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { ServiceAddress } from '../address/index.ts';
import type { NameLike } from '../name.ts';

/** A service address, or the text or name of one: `'echo'`, `'npc/1a2b'`, `Name.of('echo')`. */
export type ServiceAddressLike = ServiceAddress | NameLike;

/**
 * [address] as a {@link ServiceAddress}: itself, the text form parsed
 * (`type` or `type/<hex instance>`), or a name with instance 0 (any).
 *
 * @throws {RangeError} for text that is not a service address.
 */
export function toServiceAddress(address: ServiceAddressLike): ServiceAddress {
  if (address instanceof ServiceAddress) {
    return address;
  }
  return typeof address === 'string' ? ServiceAddress.parse(address) : new ServiceAddress(address);
}
