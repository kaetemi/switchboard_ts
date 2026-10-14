/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import {
  ByteFormatError,
  ByteReader,
  ByteWriter,
  type PayloadLike,
  toPayload,
  utf8Encode,
} from '../bytes.ts';
import { type Name, type NameLike, toName } from '../name.ts';
import { ProtocolError } from '../status.ts';
import { maxInstance, ServiceAddress } from './service-address.ts';

const hasService = 0x01;
const hasInstance = 0x02;
const hasShard = 0x04;
const hasHost = 0x08;
const reserved = 0xf0;

/** The fields of a {@link ChannelAddress}. */
export interface ChannelAddressInit {
  /** Service type, or undefined for the receiver's default service. */
  type?: NameLike | undefined;
  /** Instance id (`u48`), 0 for any. */
  instance?: number | undefined;
  /** Shard slot (`u32`), a routing hint. */
  shard?: number | undefined;
  /** Host hint for relaying proxies. */
  host?: string | undefined;
  /** Opaque application payload, passed through unchanged. */
  payload?: PayloadLike;
}

/**
 * The address header and application payload of a mux OPEN payload.
 *
 * Wire layout: a flags byte, then service type, instance, shard slot and
 * host hint when their flag is set, then the opaque application payload.
 */
export class ChannelAddress {
  /** Service type, or undefined for the receiver's default service. */
  readonly type: Name | undefined;

  /** Instance id (`u48`), 0 for any. */
  readonly instance: number;

  /** Shard slot (`u32`), a routing hint, or undefined. */
  readonly shard: number | undefined;

  /** Host hint for relaying proxies, or undefined. */
  readonly host: string | undefined;

  /** Opaque application payload, passed through unchanged. */
  readonly payload: Uint8Array;

  /**
   * @throws {RangeError} if [instance] is outside u48 or [shard] outside
   * u32, or if [host] exceeds 255 bytes of UTF-8.
   */
  constructor(init: ChannelAddressInit = {}) {
    const instance = init.instance ?? 0;
    if (!Number.isInteger(instance) || instance < 0 || instance > maxInstance) {
      throw new RangeError(`instance ${instance} is outside 0 to ${maxInstance}`);
    }
    const shard = init.shard;
    if (shard !== undefined && (!Number.isInteger(shard) || shard < 0 || shard > 0xffffffff)) {
      throw new RangeError(`shard ${shard} is outside 0 to 0xFFFFFFFF`);
    }
    const host = init.host;
    if (host !== undefined && utf8Encode(host).length > 0xff) {
      throw new RangeError('host hint longer than 255 bytes');
    }
    this.type = init.type === undefined ? undefined : toName(init.type);
    this.instance = instance;
    this.shard = shard;
    this.host = host;
    this.payload = toPayload(init.payload);
  }

  /** A channel address for [address], with the given [payload]. */
  static to(address: ServiceAddress, payload?: PayloadLike): ChannelAddress {
    return new ChannelAddress({ type: address.type, instance: address.instance, payload });
  }

  /** The service address, or undefined when {@link type} is undefined. */
  get address(): ServiceAddress | undefined {
    return this.type === undefined ? undefined : new ServiceAddress(this.type, this.instance);
  }

  /**
   * Encodes the address header followed by {@link payload}.
   *
   * HAS_INSTANCE is set only when {@link instance} is not 0.
   */
  encode(): Uint8Array {
    const w = new ByteWriter(16 + this.payload.length);
    w.u8(
      (this.type !== undefined ? hasService : 0) |
        (this.instance !== 0 ? hasInstance : 0) |
        (this.shard !== undefined ? hasShard : 0) |
        (this.host !== undefined ? hasHost : 0),
    );
    if (this.type !== undefined) {
      w.name(this.type);
    }
    if (this.instance !== 0) {
      w.u48(this.instance);
    }
    if (this.shard !== undefined) {
      w.u32(this.shard);
    }
    if (this.host !== undefined) {
      w.string8(this.host);
    }
    w.bytes(this.payload);
    return w.toBytes();
  }

  /**
   * Decodes an OPEN payload. An empty payload is the default address.
   *
   * @throws {ProtocolError} on reserved flag bits, truncation or an
   * invalid name. The application payload is a copy of the remaining bytes.
   */
  static decode(openPayload: Uint8Array): ChannelAddress {
    if (openPayload.length === 0) {
      return new ChannelAddress();
    }
    try {
      const r = new ByteReader(openPayload);
      const flags = r.u8('flags');
      if ((flags & reserved) !== 0) {
        throw new ProtocolError(`reserved address flags 0x${(flags & reserved).toString(16)}`);
      }
      const type = (flags & hasService) !== 0 ? r.name('service type') : undefined;
      const instance = (flags & hasInstance) !== 0 ? r.u48('instance') : 0;
      const shard = (flags & hasShard) !== 0 ? r.u32('shard') : undefined;
      const host = (flags & hasHost) !== 0 ? r.string8('host') : undefined;
      return new ChannelAddress({ type, instance, shard, host, payload: r.rest().slice() });
    } catch (e) {
      if (e instanceof ByteFormatError || e instanceof RangeError) {
        // A malformed host hint can decode (with replacement characters) to
        // more than 255 bytes and be refused by the constructor.
        throw new ProtocolError(`bad open payload: ${e.message}`, { cause: e });
      }
      throw e;
    }
  }

  /**
   * A copy with the given fields replaced. `clearHost` and `clearShard`
   * drop the host hint and shard slot; a proxy uses `clearHost` once it has
   * acted on the hint.
   */
  with(
    changes: ChannelAddressInit & { clearHost?: boolean; clearShard?: boolean },
  ): ChannelAddress {
    return new ChannelAddress({
      type: changes.type ?? this.type,
      instance: changes.instance ?? this.instance,
      shard: changes.clearShard ? undefined : (changes.shard ?? this.shard),
      host: changes.clearHost ? undefined : (changes.host ?? this.host),
      payload: changes.payload ?? this.payload,
    });
  }

  toString(): string {
    const shard = this.shard !== undefined ? ` shard ${this.shard}` : '';
    const host = this.host !== undefined ? ` host ${this.host}` : '';
    return `ChannelAddress(${this.address?.toString() ?? 'default'}${shard}${host}, ${this.payload.length} payload bytes)`;
  }
}
