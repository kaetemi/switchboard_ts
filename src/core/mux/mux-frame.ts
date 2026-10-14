/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * The frame mux's wire format: the frame header, the control messages on
 * channel 0, LIMITS and CREDIT. Ports `mux_frame.dart`. What every binding
 * shares (IDENT, the status rules) is in `mux-protocol.ts`.
 *
 * See the wiki page "Polyverse Switchboard Mux".
 */

import { ByteFormatError, ByteReader, ByteWriter, hexString, maxU48 } from '../bytes.ts';
import { ProtocolError, type Status } from '../status.ts';
import {
  decodeStatusPayload,
  MuxIdent,
  maxControlPayloadLength,
  truncateStatus,
} from './mux-protocol.ts';

/**
 * Mux command, carried in bits 4 and 5 of the frame flags. The wire value 3
 * is reserved and decoding it is a protocol error.
 */
export const MuxCommand = {
  /** Carries one subframe on an open channel, or a control message on channel 0. */
  data: 0,
  /** Opens a channel; the payload is the open payload. */
  open: 1,
  /** Closes a channel; the payload is a status payload, possibly empty. */
  close: 2,
} as const;

/** A mux command: one of the values of {@link MuxCommand}. */
export type MuxCommand = (typeof MuxCommand)[keyof typeof MuxCommand];

const commandNames = ['data', 'open', 'close'] as const;
const empty = new Uint8Array(0);

/**
 * One mux frame: a flags byte, a 2 or 6 byte channel id and a payload.
 *
 * See the wiki page "Polyverse Switchboard Mux", section "Frame header".
 */
export class MuxFrame {
  /** Flag bit 0x02: the channel id is a `u16` rather than a `u48`. */
  static readonly flagShort = 0x02;

  /** Flag bits 0x01, 0x04 and 0x08: reserved for incompatible extensions, must be 0. */
  static readonly flagsReservedIncompatible = 0x0d;

  /** Mask of the command bits. */
  static readonly commandMask = 0x30;

  /** Shift of the command bits. */
  static readonly commandShift = 4;

  /** Flag bits 0x40 and 0x80: reserved for compatible extensions, ignored. */
  static readonly flagsReservedCompatible = 0xc0;

  /** Size of the header with a short channel id. */
  static readonly shortHeaderSize = 3;

  /** Size of the header with a long channel id. */
  static readonly longHeaderSize = 7;

  /** Largest id that fits the short form. */
  static readonly maxShortId = 0xffff;

  /** Largest channel id (`u48`). */
  static readonly maxId = maxU48;

  /** The control channel. */
  static readonly controlChannelId = 0;

  /** Reserved for future protocol use; any frame on it is a protocol error. */
  static readonly reservedChannelId = 1;

  /** The command. */
  readonly command: MuxCommand;

  /** The channel id, 0 to 0xFFFFFFFFFFFF. */
  readonly channelId: number;

  /** The payload: subframe, open payload or status payload. May be empty. */
  readonly payload: Uint8Array;

  /**
   * Whether the frame was decoded from the long (6 byte) id form.
   * Informational only: {@link encode} always uses the short form when the
   * id fits, as the specification recommends.
   */
  readonly longForm: boolean;

  /** A frame. [payload] is used as is, not copied. */
  constructor(
    command: MuxCommand,
    channelId: number,
    payload: Uint8Array = empty,
    options: { readonly longForm?: boolean } = {},
  ) {
    this.command = command;
    this.channelId = channelId;
    this.payload = payload;
    this.longForm = options.longForm ?? false;
  }

  /** A DATA frame carrying [payload] on [channelId]. */
  static data(channelId: number, payload: Uint8Array): MuxFrame {
    return new MuxFrame(MuxCommand.data, channelId, payload);
  }

  /** An OPEN frame for [channelId] with the given open payload. */
  static open(channelId: number, openPayload: Uint8Array): MuxFrame {
    return new MuxFrame(MuxCommand.open, channelId, openPayload);
  }

  /**
   * A CLOSE frame for [channelId] carrying [status]. Without a status the
   * payload is empty, which the receiver reads as `OK`.
   */
  static close(channelId: number, status?: Status): MuxFrame {
    return new MuxFrame(MuxCommand.close, channelId, status?.encode());
  }

  /**
   * Whether [id] is reserved: 1, 0xFFFF, 0xFFFF00000000 or 0xFFFFFFFFFFFF.
   * Reserved ids can never be opened and any frame on them is a protocol
   * error. Channel 0 is not reserved in this sense; it is the control
   * channel.
   */
  static isReservedId(id: number): boolean {
    return (
      id === MuxFrame.reservedChannelId ||
      id === 0xffff ||
      id === 0xffff00000000 ||
      id === 0xffffffffffff
    );
  }

  /** Header size {@link encode} uses for [channelId]. */
  static headerSizeFor(channelId: number): number {
    return channelId <= MuxFrame.maxShortId ? MuxFrame.shortHeaderSize : MuxFrame.longHeaderSize;
  }

  /** Size of the encoded frame in bytes, header included. */
  get encodedLength(): number {
    return MuxFrame.headerSizeFor(this.channelId) + this.payload.length;
  }

  /**
   * Encodes the frame, in the short id form whenever the id is below
   * 0x10000. The encoder does not refuse reserved ids, so that tests can
   * produce invalid frames; the connection never sends them.
   *
   * @throws {RangeError} if the id is not an integer that fits a `u48`.
   */
  encode(): Uint8Array {
    const id = this.channelId;
    if (!Number.isInteger(id) || id < 0 || id > MuxFrame.maxId) {
      throw new RangeError(`channel id ${id} does not fit in u48`);
    }
    const short = id <= MuxFrame.maxShortId;
    const headerSize = short ? MuxFrame.shortHeaderSize : MuxFrame.longHeaderSize;
    const out = new Uint8Array(headerSize + this.payload.length);
    out[0] = (this.command << MuxFrame.commandShift) | (short ? MuxFrame.flagShort : 0);
    if (short) {
      out[1] = id & 0xff;
      out[2] = (id >> 8) & 0xff;
    } else {
      // Split without shifting past 32 bits, which bitwise operators cannot.
      const hi = Math.floor(id / 0x100000000);
      const lo = id - hi * 0x100000000;
      out[1] = lo & 0xff;
      out[2] = (lo >>> 8) & 0xff;
      out[3] = (lo >>> 16) & 0xff;
      out[4] = (lo >>> 24) & 0xff;
      out[5] = hi & 0xff;
      out[6] = (hi >> 8) & 0xff;
    }
    out.set(this.payload, headerSize);
    return out;
  }

  /**
   * Decodes one mux frame. The payload is a view into [bytes]. Compatible
   * reserved flags (0x40, 0x80) are ignored.
   *
   * @throws {ProtocolError} for an empty frame, an incompatible reserved
   *   flag, command 3, a truncated header, any frame for a reserved id (1,
   *   0xFFFF, 0xFFFF00000000, 0xFFFFFFFFFFFF), and OPEN or CLOSE for the
   *   control channel 0, checked in that order.
   */
  static decode(bytes: Uint8Array): MuxFrame {
    if (bytes.length === 0) {
      throw new ProtocolError('empty mux frame');
    }
    const flags = bytes[0] as number;
    if ((flags & MuxFrame.flagsReservedIncompatible) !== 0) {
      throw new ProtocolError(`reserved mux flag set: 0x${flags.toString(16)}`);
    }
    const command = ((flags & MuxFrame.commandMask) >> MuxFrame.commandShift) as MuxCommand | 3;
    if (command === 3) {
      throw new ProtocolError('reserved mux command 3');
    }
    const short = (flags & MuxFrame.flagShort) !== 0;
    const reader = new ByteReader(bytes, 1);
    let channelId: number;
    try {
      channelId = short ? reader.u16('channel id') : reader.u48('channel id');
    } catch (error) {
      if (error instanceof ByteFormatError) {
        throw new ProtocolError(`truncated mux header: ${error.message}`);
      }
      throw error;
    }
    if (MuxFrame.isReservedId(channelId)) {
      throw new ProtocolError(`frame on reserved channel ${channelId}`);
    }
    if (channelId === MuxFrame.controlChannelId && command !== MuxCommand.data) {
      throw new ProtocolError(`${commandNames[command]} on control channel 0`);
    }
    return new MuxFrame(command, channelId, reader.rest(), { longForm: !short });
  }

  /** The command, the id and the payload in hex, for logs and tests. */
  toString(): string {
    return `MuxFrame(${commandNames[this.command]}, ${this.channelId}, ${hexString(this.payload)})`;
  }
}

/** Control message types carried on channel 0. */
export const MuxControlType = {
  /** Keep-alive or latency probe; the receiver answers with PONG. */
  ping: 1,
  /** Answer to PING with the same payload. */
  pong: 2,
  /** The sender is shutting down; the payload is a status payload. */
  goAway: 3,
  /** Advisory limits of the sender; the payload is {@link MuxLimits}. */
  limits: 4,
  /**
   * 32 random bytes: the sender intends to identify, or wants the peer to.
   * Answered with the receiver's own NONCE if it has not sent one.
   */
  nonce: 5,
  /** The sender's credential and proof of possession; the payload is {@link MuxIdent}. */
  ident: 6,
  /** Flow-control credit for one channel; the payload is {@link MuxCredit}. */
  credit: 7,
} as const;

/** A known control type: one of the values of {@link MuxControlType}. */
export type MuxControlType = (typeof MuxControlType)[keyof typeof MuxControlType];

/** The name of a known control type: one of the keys of {@link MuxControlType}. */
export type MuxControlTypeName = keyof typeof MuxControlType;

const controlTypeNames = new Map<number, MuxControlTypeName>(
  (Object.entries(MuxControlType) as [MuxControlTypeName, MuxControlType][]).map(([name, code]) => [
    code,
    name,
  ]),
);

/** The name of the control type [code], or undefined for unknown types (which receivers ignore). */
export function muxControlTypeName(code: number): MuxControlTypeName | undefined {
  return controlTypeNames.get(code);
}

/**
 * A control message: a type byte followed by a control payload, carried as
 * the payload of a DATA frame on channel 0.
 */
export class MuxControlMessage {
  /** Largest PING payload a sender may use. */
  static readonly maxPingPayload = 125;

  /** Largest control payload (after the type byte) a receiver accepts. */
  static readonly maxControlPayload = maxControlPayloadLength;

  /** Control type code; may be a type this implementation does not know. */
  readonly type: number;

  /** Control payload, after the type byte. */
  readonly payload: Uint8Array;

  /** A control message. [payload] is used as is, not copied. */
  constructor(type: number, payload: Uint8Array = empty) {
    this.type = type;
    this.payload = payload;
  }

  /**
   * PING with an opaque payload of at most
   * {@link MuxControlMessage.maxPingPayload} bytes.
   *
   * @throws {RangeError} if the payload is too long.
   */
  static ping(payload: Uint8Array = empty): MuxControlMessage {
    if (payload.length > MuxControlMessage.maxPingPayload) {
      throw new RangeError(
        `a PING payload is at most ${MuxControlMessage.maxPingPayload} bytes, got ${payload.length}`,
      );
    }
    return new MuxControlMessage(MuxControlType.ping, payload);
  }

  /** PONG echoing a PING payload. */
  static pong(payload: Uint8Array): MuxControlMessage {
    return new MuxControlMessage(MuxControlType.pong, payload);
  }

  /**
   * GOAWAY carrying [status]. The reason is shortened, on a UTF-8 character
   * boundary, so that the payload stays within
   * {@link MuxControlMessage.maxControlPayload} bytes.
   */
  static goAway(status: Status): MuxControlMessage {
    return new MuxControlMessage(
      MuxControlType.goAway,
      truncateStatus(status, MuxControlMessage.maxControlPayload).encode(),
    );
  }

  /** LIMITS carrying [limits]. @throws {RangeError} as {@link MuxLimits.encode}. */
  static limits(limits: MuxLimits): MuxControlMessage {
    return new MuxControlMessage(MuxControlType.limits, limits.encode());
  }

  /**
   * NONCE carrying [nonce], which must be {@link MuxIdent.nonceLength} bytes.
   *
   * @throws {RangeError} for another length.
   */
  static nonce(nonce: Uint8Array): MuxControlMessage {
    if (nonce.length !== MuxIdent.nonceLength) {
      throw new RangeError(`a NONCE is ${MuxIdent.nonceLength} bytes, got ${nonce.length}`);
    }
    return new MuxControlMessage(MuxControlType.nonce, nonce);
  }

  /** IDENT carrying [ident]. @throws {RangeError} as {@link MuxIdent.encode}. */
  static ident(ident: MuxIdent): MuxControlMessage {
    return new MuxControlMessage(MuxControlType.ident, ident.encode());
  }

  /** CREDIT carrying [credit]. @throws {RangeError} as {@link MuxCredit.encode}. */
  static credit(credit: MuxCredit): MuxControlMessage {
    return new MuxControlMessage(MuxControlType.credit, credit.encode());
  }

  /** The known type, or undefined if {@link type} is not one of {@link MuxControlType}. */
  get knownType(): MuxControlType | undefined {
    return controlTypeNames.has(this.type) ? (this.type as MuxControlType) : undefined;
  }

  /** Encodes as the payload of a DATA frame on channel 0. */
  encode(): Uint8Array {
    const out = new Uint8Array(1 + this.payload.length);
    out[0] = this.type;
    out.set(this.payload, 1);
    return out;
  }

  /** This message as a DATA frame on channel 0. */
  toFrame(): MuxFrame {
    return MuxFrame.data(MuxFrame.controlChannelId, this.encode());
  }

  /**
   * Decodes the payload of a DATA frame on channel 0. The payload is a view
   * into [bytes]. Unknown types decode fine.
   *
   * @throws {ProtocolError} if [bytes] is empty or the control payload
   *   exceeds {@link MuxControlMessage.maxControlPayload} bytes.
   */
  static decode(bytes: Uint8Array): MuxControlMessage {
    if (bytes.length === 0) {
      throw new ProtocolError('control message without type');
    }
    if (bytes.length - 1 > MuxControlMessage.maxControlPayload) {
      throw new ProtocolError(
        `control payload of ${bytes.length - 1} bytes exceeds ${MuxControlMessage.maxControlPayload}`,
      );
    }
    return new MuxControlMessage(bytes[0] as number, bytes.subarray(1));
  }

  /**
   * The payload of a GOAWAY message as a status.
   *
   * @throws {ProtocolError} if the status payload is malformed.
   */
  get goAwayStatus(): Status {
    return decodeStatusPayload(this.payload, 'GOAWAY');
  }

  /** The type and the payload in hex, for logs and tests. */
  toString(): string {
    return `MuxControlMessage(${muxControlTypeName(this.type) ?? this.type}, ${hexString(this.payload)})`;
  }
}

/** What {@link MuxLimits} is built from. */
export interface MuxLimitsInit {
  /** Largest mux frame, header included, the sender accepts; 0: no stated limit. */
  readonly maxFrameSize: number;
  /** Largest number of simultaneously open channels the sender accepts; 0: no stated limit. */
  readonly maxChannels: number;
  /** The window the sender grants every channel; default {@link MuxLimits.defaultInitialWindow}. */
  readonly initialWindow?: number | undefined;
}

/**
 * What a peer announces with the LIMITS control message: the largest frame
 * and the number of channels it accepts (advisory, 0 meaning "no stated
 * limit"), and the initial flow-control window of the channels opened
 * after the message is received.
 *
 * See the wiki page "Polyverse Switchboard Mux", sections "Control channel"
 * and "Flow control".
 */
export class MuxLimits {
  /** The initial window of every channel until the peer's LIMITS has been received: 64 KiB. */
  static readonly defaultInitialWindow = 64 * 1024;

  /** Largest window: a window above `2^32 - 1` is a protocol error. */
  static readonly maxWindow = 0xffffffff;

  /** Encoded size of the fields this implementation understands, and the smallest LIMITS payload it accepts. */
  static readonly encodedLength = 12;

  /** Largest mux frame, header included, the sender accepts. */
  readonly maxFrameSize: number;

  /** Largest number of simultaneously open channels the sender accepts. */
  readonly maxChannels: number;

  /**
   * The flow-control window, in bytes of cost (see {@link MuxCredit.costOf}),
   * that the sender of the LIMITS grants every channel opened after the
   * message was received, in each channel's direction towards it. Never 0.
   */
  readonly initialWindow: number;

  /** Limits; the initial window defaults to the window a sender assumes before any LIMITS. */
  constructor(init: MuxLimitsInit) {
    this.maxFrameSize = init.maxFrameSize;
    this.maxChannels = init.maxChannels;
    this.initialWindow = init.initialWindow ?? MuxLimits.defaultInitialWindow;
  }

  /**
   * Encodes as `u32 maxFrameSize`, `u32 maxChannels`, `u32 initialWindow`.
   *
   * @throws {RangeError} if a value is not an integer that fits a `u32`, or
   *   if the initial window is 0.
   */
  encode(): Uint8Array {
    for (const [name, value] of [
      ['maxFrameSize', this.maxFrameSize],
      ['maxChannels', this.maxChannels],
      ['initialWindow', this.initialWindow],
    ] as const) {
      if (!Number.isInteger(value) || value < 0 || value > 0xffffffff) {
        throw new RangeError(`${name} ${value} does not fit in u32`);
      }
    }
    if (this.initialWindow === 0) {
      throw new RangeError('initialWindow must not be 0');
    }
    return new ByteWriter(MuxLimits.encodedLength)
      .u32(this.maxFrameSize)
      .u32(this.maxChannels)
      .u32(this.initialWindow)
      .toBytes();
  }

  /**
   * Decodes a LIMITS payload. Fields beyond the first three are ignored.
   *
   * @throws {ProtocolError} if the payload is shorter than
   *   {@link MuxLimits.encodedLength} bytes or the initial window is 0.
   */
  static decode(bytes: Uint8Array): MuxLimits {
    if (bytes.length < MuxLimits.encodedLength) {
      throw new ProtocolError(
        `LIMITS of ${bytes.length} bytes, at least ${MuxLimits.encodedLength} expected`,
      );
    }
    const reader = new ByteReader(bytes);
    const limits = new MuxLimits({
      maxFrameSize: reader.u32('max frame size'),
      maxChannels: reader.u32('max channels'),
      initialWindow: reader.u32('initial window'),
    });
    if (limits.initialWindow === 0) {
      throw new ProtocolError('LIMITS with an initial window of 0');
    }
    return limits;
  }

  /** Whether [other] is a {@link MuxLimits} with the same values. */
  equals(other: unknown): boolean {
    return (
      other instanceof MuxLimits &&
      other.maxFrameSize === this.maxFrameSize &&
      other.maxChannels === this.maxChannels &&
      other.initialWindow === this.initialWindow
    );
  }

  /** The three values, for logs and tests. */
  toString(): string {
    return (
      `MuxLimits(maxFrameSize: ${this.maxFrameSize}, maxChannels: ${this.maxChannels}, ` +
      `initialWindow: ${this.initialWindow})`
    );
  }
}

/**
 * The payload of a CREDIT control message: `u48` channel id, `u32` bytes.
 *
 * The sender has consumed {@link bytes} of cost on {@link channelId} (or
 * grants that much more), and the receiver may send as many more bytes of
 * cost on it. See the wiki page "Polyverse Switchboard Mux", section "Flow
 * control".
 */
export class MuxCredit {
  /**
   * What a DATA frame costs beyond its payload, so that a flood of empty
   * subframes is bounded too: 16 bytes.
   */
  static readonly frameOverhead = 16;

  /** Encoded size: exactly 10 bytes. */
  static readonly encodedLength = 10;

  /** The channel the credit is for: the same id as on the wire, from either side. */
  readonly channelId: number;

  /** Bytes of cost returned or granted, 0 to `2^32 - 1`. */
  readonly bytes: number;

  /** Credit of [bytes] for [channelId]. */
  constructor(channelId: number, bytes: number) {
    this.channelId = channelId;
    this.bytes = bytes;
  }

  /**
   * The flow-control cost of a DATA frame carrying a subframe of
   * [subframeLength] bytes: the length plus {@link MuxCredit.frameOverhead}.
   */
  static costOf(subframeLength: number): number {
    return subframeLength + MuxCredit.frameOverhead;
  }

  /**
   * Encodes as `u48 channelId`, `u32 bytes`.
   *
   * @throws {RangeError} if the channel id is not an integer that fits a
   *   `u48` or the bytes one that fits a `u32`.
   */
  encode(): Uint8Array {
    if (
      !Number.isInteger(this.channelId) ||
      this.channelId < 0 ||
      this.channelId > MuxFrame.maxId
    ) {
      throw new RangeError(`channel id ${this.channelId} does not fit in u48`);
    }
    if (!Number.isInteger(this.bytes) || this.bytes < 0 || this.bytes > 0xffffffff) {
      throw new RangeError(`credit of ${this.bytes} bytes does not fit in u32`);
    }
    return new ByteWriter(MuxCredit.encodedLength).u48(this.channelId).u32(this.bytes).toBytes();
  }

  /**
   * Decodes a CREDIT payload.
   *
   * @throws {ProtocolError} unless the payload is exactly
   *   {@link MuxCredit.encodedLength} bytes.
   */
  static decode(bytes: Uint8Array): MuxCredit {
    if (bytes.length !== MuxCredit.encodedLength) {
      throw new ProtocolError(
        `CREDIT of ${bytes.length} bytes, ${MuxCredit.encodedLength} expected`,
      );
    }
    const reader = new ByteReader(bytes);
    return new MuxCredit(reader.u48('channel id'), reader.u32('bytes'));
  }

  /** Whether [other] is a {@link MuxCredit} with the same values. */
  equals(other: unknown): boolean {
    return (
      other instanceof MuxCredit && other.channelId === this.channelId && other.bytes === this.bytes
    );
  }

  /** The channel and the bytes, for logs and tests. */
  toString(): string {
    return `MuxCredit(${this.channelId}, ${this.bytes})`;
  }
}
