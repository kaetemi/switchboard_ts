/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { readU32, writeU32 } from '../bytes.ts';
import { ProtocolError } from '../status.ts';
import { quicDefaultRecordLimit, quicMaxRecordLength } from './records.ts';

/** The control type of `HELLO`, which belongs to the QUIC binding only. */
export const quicHelloType = 0x08;

/** The fields of a {@link QuicHello}. */
export interface QuicHelloInit {
  /** Default {@link quicDefaultRecordLimit}; at least that and a `u32`. */
  recordLimit?: number | undefined;
  /** Default 0 (no stated limit); a `u32`. */
  channels?: number | undefined;
  /** Default false. */
  exporter?: boolean | undefined;
}

/**
 * The `HELLO` control message: the first one in each direction of the
 * control stream.
 *
 * ```
 * offset  size  field
 * 0       u32   record limit, at least 32752
 * 4       u32   channels: how many channels opened by the peer this side
 *               accepts at once; 0 = no stated limit
 * 8       u8    flags: 0x01 EXPORTER
 * 9       ...   further fields, ignored
 * ```
 */
export class QuicHello {
  /** The `EXPORTER` flag. */
  static readonly flagExporter = 0x01;

  /** Bytes of the fields this version knows. */
  static readonly encodedLength = 9;

  /**
   * The largest record body this side accepts on a channel's stream. A
   * decoded value above {@link quicMaxRecordLength} reads as it.
   */
  readonly recordLimit: number;

  /** How many channels opened by the peer this side accepts at once; 0 for no stated limit. Advisory. */
  readonly channels: number;

  /** Whether this side can compute the exporter of the connection. */
  readonly exporter: boolean;

  /**
   * @throws {RangeError} for a record limit below
   * {@link quicDefaultRecordLimit} or above `u32`, or channels outside `u32`.
   */
  constructor(init: QuicHelloInit = {}) {
    const recordLimit = init.recordLimit ?? quicDefaultRecordLimit;
    const channels = init.channels ?? 0;
    if (
      !Number.isInteger(recordLimit) ||
      recordLimit < quicDefaultRecordLimit ||
      recordLimit > 0xffffffff
    ) {
      throw new RangeError(
        `record limit ${recordLimit} is not at least ${quicDefaultRecordLimit} and a u32`,
      );
    }
    if (!Number.isInteger(channels) || channels < 0 || channels > 0xffffffff) {
      throw new RangeError(`channels ${channels} is not a u32`);
    }
    this.recordLimit = recordLimit;
    this.channels = channels;
    this.exporter = init.exporter ?? false;
  }

  /** The payload, after the control type. */
  encode(): Uint8Array {
    const out = new Uint8Array(QuicHello.encodedLength);
    writeU32(out, 0, this.recordLimit);
    writeU32(out, 4, this.channels);
    out[8] = this.exporter ? QuicHello.flagExporter : 0;
    return out;
  }

  /** The control message on the wire: the type byte {@link quicHelloType}, then {@link encode}. */
  encodeMessage(): Uint8Array {
    const payload = this.encode();
    const out = new Uint8Array(1 + payload.length);
    out[0] = quicHelloType;
    out.set(payload, 1);
    return out;
  }

  /**
   * Decodes a `HELLO` payload (after the type byte). Unknown flags and
   * further fields are ignored; a record limit above
   * {@link quicMaxRecordLength} reads as it.
   *
   * @throws {ProtocolError} for a payload shorter than 9 bytes or a record
   * limit below {@link quicDefaultRecordLimit}.
   */
  static decode(payload: Uint8Array): QuicHello {
    if (payload.length < QuicHello.encodedLength) {
      throw new ProtocolError(`HELLO of ${payload.length} bytes`);
    }
    const limit = readU32(payload, 0);
    if (limit < quicDefaultRecordLimit) {
      throw new ProtocolError(`HELLO with a record limit of ${limit}`);
    }
    return new QuicHello({
      recordLimit: limit > quicMaxRecordLength ? quicMaxRecordLength : limit,
      channels: readU32(payload, 4),
      exporter: ((payload[8] as number) & QuicHello.flagExporter) !== 0,
    });
  }

  /** Field-wise equality. */
  equals(other: unknown): boolean {
    return (
      other instanceof QuicHello &&
      other.recordLimit === this.recordLimit &&
      other.channels === this.channels &&
      other.exporter === this.exporter
    );
  }

  toString(): string {
    return (
      `QuicHello(record limit ${this.recordLimit}, channels ${this.channels}` +
      `${this.exporter ? ', EXPORTER' : ''})`
    );
  }
}
