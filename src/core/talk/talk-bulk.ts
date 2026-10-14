/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * The wire structures of Talk bulk payloads (wiki page "Polyverse
 * Switchboard Talk", section "Bulk payloads"): the reference a `BULK`
 * message carries, and the open payload of a `_bulk` channel.
 */

import { ChannelAddress } from '../address/channel-address.ts';
import { ByteFormatError, ByteReader, ByteWriter, maxU48 } from '../bytes.ts';
import { Name } from '../name.ts';
import { ProtocolError } from '../status.ts';

/**
 * The payload field of a Talk message with the `BULK` flag
 * ({@link TalkFrame.bulk}): which bulk channel carries the payload, and how
 * long it is.
 *
 * ```
 * u32   bulk number   among the bulk channels the sender opened for the
 *                     message's channel, from 1
 * u48   length        payload bytes to expect; 0xFFFFFFFFFFFF unknown
 * ```
 */
export class TalkBulkReference {
  /** Bytes of an encoded reference: the whole payload field of a `BULK` message. */
  static readonly encodedLength = 10;

  /** The length field meaning "not known in advance". */
  static readonly unknownLength = maxU48;

  /** The largest declared length. */
  static readonly maxLength = maxU48 - 1;

  /** The largest bulk number (`u32`). */
  static readonly maxNumber = 0xffffffff;

  /** The number of the bulk channel among those the sender opened for the channel the message travels on, from 1. */
  readonly number: number;

  /** The payload length in bytes, or undefined when the sender did not know it. */
  readonly length: number | undefined;

  /**
   * A reference to bulk channel [number] carrying [length] bytes
   * (undefined: not known in advance).
   *
   * @throws {RangeError} for a [number] outside 1 to {@link maxNumber} or a
   * [length] outside 0 to {@link maxLength}.
   */
  constructor(number: number, length?: number) {
    checkInterval(number, 1, TalkBulkReference.maxNumber, 'number');
    if (length !== undefined) {
      checkInterval(length, 0, TalkBulkReference.maxLength, 'length');
    }
    this.number = number;
    this.length = length;
  }

  /** The 10 bytes of the payload field. */
  encode(): Uint8Array {
    return new ByteWriter(TalkBulkReference.encodedLength)
      .u32(this.number)
      .u48(this.length ?? TalkBulkReference.unknownLength)
      .toBytes();
  }

  /**
   * Decodes the payload field of a `BULK` message.
   *
   * @throws {ProtocolError} unless it is exactly {@link encodedLength}
   * bytes with a bulk number of at least 1.
   */
  static decode(payload: Uint8Array): TalkBulkReference {
    if (payload.length !== TalkBulkReference.encodedLength) {
      throw new ProtocolError(`bulk reference of ${payload.length} bytes`);
    }
    const r = new ByteReader(payload);
    const number = r.u32('bulk number');
    const length = r.u48('bulk length');
    if (number === 0) {
      throw new ProtocolError('bulk number 0');
    }
    return new TalkBulkReference(
      number,
      length === TalkBulkReference.unknownLength ? undefined : length,
    );
  }

  /** Whether [other] is a reference with the same number and length. */
  equals(other: unknown): boolean {
    return (
      other instanceof TalkBulkReference &&
      other.number === this.number &&
      other.length === this.length
    );
  }

  toString(): string {
    return `TalkBulkReference(${this.number}, ${this.length ?? 'unknown'} bytes)`;
  }
}

/**
 * The open payload of a bulk channel: an address header of the reserved
 * type `_bulk` (instance 0, no shard), whose application payload names
 * the parent channel and the bulk number.
 *
 * ```
 * u48   parent channel id   the channel the message travels on, as the
 *                           receiver of the OPEN sees it (a channel has
 *                           the same id at both ends of a connection)
 * u32   bulk number         the number the message's reference carries
 * ```
 *
 * A receiver admits a `_bulk` channel only while its parent is open on the
 * connection the OPEN arrived on, without consulting any listener policy,
 * and hands it to the parent's Talk layer.
 */
export class TalkBulkOpen {
  /** The reserved service type of bulk channels. */
  static readonly type: Name = Name.of('_bulk');

  /** Bytes of the application payload. */
  static readonly payloadLength = 10;

  /** The id of the channel whose message the bulk channel carries. */
  readonly parentId: number;

  /** The bulk number, as the message's {@link TalkBulkReference} carries it. */
  readonly number: number;

  /**
   * The open payload of bulk channel [number] for the channel [parentId].
   *
   * @throws {RangeError} for a [parentId] outside `u48` or a [number]
   * outside 1 to {@link TalkBulkReference.maxNumber}.
   */
  constructor(parentId: number, number: number) {
    checkInterval(parentId, 0, maxU48, 'parentId');
    checkInterval(number, 1, TalkBulkReference.maxNumber, 'number');
    this.parentId = parentId;
    this.number = number;
  }

  /** The address header and application payload of the OPEN. */
  get address(): ChannelAddress {
    const payload = new ByteWriter(TalkBulkOpen.payloadLength)
      .u48(this.parentId)
      .u32(this.number)
      .toBytes();
    return new ChannelAddress({ type: TalkBulkOpen.type, payload });
  }

  /** The complete OPEN payload. */
  encode(): Uint8Array {
    return this.address.encode();
  }

  /** Whether [address] is that of a bulk channel. */
  static isBulk(address: ChannelAddress): boolean {
    return address.type?.equals(TalkBulkOpen.type) === true;
  }

  /**
   * Decodes the application payload of a `_bulk` OPEN ([address] must be
   * of {@link type}); bytes after the two fields are ignored.
   *
   * @throws {ProtocolError} when it is shorter, or names bulk number 0.
   */
  static decode(address: ChannelAddress): TalkBulkOpen {
    const payload = address.payload;
    if (payload.length < TalkBulkOpen.payloadLength) {
      throw new ProtocolError(`_bulk open payload of ${payload.length} bytes`);
    }
    const r = new ByteReader(payload);
    let parent: number;
    let number: number;
    try {
      parent = r.u48('parent channel id');
      number = r.u32('bulk number');
    } catch (e) {
      if (e instanceof ByteFormatError) {
        throw new ProtocolError(e.message);
      }
      throw e;
    }
    if (number === 0) {
      throw new ProtocolError('bulk number 0');
    }
    return new TalkBulkOpen(parent, number);
  }

  toString(): string {
    return `TalkBulkOpen(parent ${this.parentId}, number ${this.number})`;
  }
}

function checkInterval(value: number, min: number, max: number, name: string): void {
  if (!Number.isInteger(value) || value < min || value > max) {
    throw new RangeError(`${name} ${value} is outside ${min} to ${max}`);
  }
}
