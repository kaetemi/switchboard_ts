/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { maxU48 } from '../bytes.ts';
import { Name, type NameLike, toName } from '../name.ts';

/** Largest valid instance id (`u48`). */
export const maxInstance = maxU48;

/**
 * A service address: a `(type, instance)` pair.
 *
 * Instance 0 means "any instance". Non-zero instances are unique across
 * the mesh. The text form is `type/instance` with the instance in lowercase
 * hexadecimal without leading zeros (`npc/1a2b`), or just `type` when the
 * instance is 0. The text form is never sent on the wire.
 */
export class ServiceAddress {
  /** Service type. */
  readonly type: Name;

  /** Instance id (`u48`), 0 for any instance. */
  readonly instance: number;

  /** The address as a map key: equal strings mean equal addresses. */
  readonly key: string;

  /** @throws {RangeError} if [instance] is outside 0 to 0xFFFFFFFFFFFF. */
  constructor(type: NameLike, instance = 0) {
    if (!Number.isInteger(instance) || instance < 0 || instance > maxInstance) {
      throw new RangeError(`instance ${instance} is outside 0 to ${maxInstance}`);
    }
    this.type = toName(type);
    this.instance = instance;
    this.key = `${this.type.key}/${instance}`;
  }

  /** True when the instance is 0, meaning any instance. */
  get isAny(): boolean {
    return this.instance === 0;
  }

  /**
   * Parses `type`, `type/0` or `type/<hex>`.
   *
   * @throws {RangeError} for an empty or invalid type, a non-hex or
   * over-long instance, or an instance out of range.
   */
  static parse(text: string): ServiceAddress {
    const slash = text.indexOf('/');
    const typeText = slash < 0 ? text : text.substring(0, slash);
    if (typeText.length === 0) {
      throw new RangeError(`empty service type in "${text}"`);
    }
    const type = Name.of(typeText);
    if (slash < 0) {
      return new ServiceAddress(type);
    }
    const hex = text.substring(slash + 1);
    if (!/^[0-9a-fA-F]{1,12}$/.test(hex)) {
      throw new RangeError(`invalid instance (1 to 12 hex digits) in "${text}"`);
    }
    return new ServiceAddress(type, Number.parseInt(hex, 16));
  }

  equals(other: unknown): boolean {
    return other instanceof ServiceAddress && other.key === this.key;
  }

  toString(): string {
    return this.isAny ? this.type.toString() : `${this.type}/${this.instance.toString(16)}`;
  }

  toJSON(): string {
    return this.toString();
  }
}

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
