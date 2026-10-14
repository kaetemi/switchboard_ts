/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * The wire format of the QUIC and WebTransport binding (wiki page
 * "Polyverse Switchboard QUIC and WebTransport Binding", sections
 * "Records", "Channels", "Control stream" and "Status codes on streams and
 * connections"): records, the OPEN and CLOSE bodies, and the status of a
 * stream or connection error code. A port of `switchboard_quic`'s
 * `records.dart`.
 *
 * Every integer is little endian. The header is read unsigned (`>>> 0`) and
 * split with arithmetic, never with a signed shift.
 */

import { maxU48, readU32, readU48, writeU32, writeU48 } from '../bytes.ts';
import {
  checkMuxStatus,
  decodeStatusPayload,
  statusForWire,
  statusFromWire,
} from '../mux/mux-protocol.ts';
import { ProtocolError, Status, StatusCode } from '../status.ts';

/** The command of a record: the top two bits of its header. Command 3 is reserved. */
export const QuicRecordCommand = {
  /** One subframe, possibly empty; on the control stream, one control message. */
  data: 0,
  /** The first record of the opener's direction of a stream: `u48` channel id, then the open payload. */
  open: 1,
  /** The last record of a direction: a status payload, possibly absent (`OK`); FIN follows. */
  close: 2,
} as const;

/** A record command: one of the values of {@link QuicRecordCommand}. */
export type QuicRecordCommand = (typeof QuicRecordCommand)[keyof typeof QuicRecordCommand];

const commandNames = ['DATA', 'OPEN', 'CLOSE'] as const;

/** The name of a record command for messages: `DATA`, `OPEN`, `CLOSE`. */
export function quicRecordCommandName(command: QuicRecordCommand): string {
  return commandNames[command];
}

/** Bytes of a record header: a `u32`. */
export const quicRecordHeaderSize = 4;

/** The largest body length a header holds: `0x3FFFFFFF`. */
export const quicMaxRecordLength = 0x3fffffff;

/**
 * The smallest record limit a side may announce, and the default: the
 * largest subframe a frame mux peer with the default 64 KiB window takes in
 * one DATA frame (half the window less the 16 bytes a frame costs).
 */
export const quicDefaultRecordLimit = 32752;

/** The channel id of the control stream's OPEN record. */
export const quicControlChannelId = 0;

/** The largest channel id, `2^48 - 1`. */
export const quicMaxChannelId = maxU48;

/** Bytes of the channel id at the start of an OPEN record's body. */
export const quicChannelIdSize = 6;

/** A record: a command and its body (exactly the length the header gave). */
export interface QuicRecord {
  readonly command: QuicRecordCommand;
  readonly body: Uint8Array;
}

/**
 * The header of a record of [command] whose body is [length] bytes.
 *
 * @throws {RangeError} for a length outside 0 to {@link quicMaxRecordLength}.
 */
export function quicRecordHeader(command: QuicRecordCommand, length: number): number {
  if (!Number.isInteger(length) || length < 0 || length > quicMaxRecordLength) {
    throw new RangeError(`a record body is 0 to ${quicMaxRecordLength} bytes, not ${length}`);
  }
  return command * 0x40000000 + length;
}

/** Writes the header of a record of [command] with a [length]-byte body at [offset] of [out]. */
export function writeQuicRecordHeader(
  out: Uint8Array,
  offset: number,
  command: QuicRecordCommand,
  length: number,
): void {
  writeU32(out, offset, quicRecordHeader(command, length));
}

/** The command code (0 to 3) of a header read as a `u32`. */
export function quicHeaderCommand(header: number): number {
  return Math.floor(header / 0x40000000);
}

/** The body length of a header read as a `u32`. */
export function quicHeaderLength(header: number): number {
  return header % 0x40000000;
}

/** Reads the `u32` header at [offset] of [bytes]. */
export function readQuicRecordHeader(bytes: Uint8Array, offset = 0): number {
  return readU32(bytes, offset);
}

/** A record of [command] carrying [body], header included. */
export function encodeQuicRecord(command: QuicRecordCommand, body: ArrayLike<number>): Uint8Array {
  const out = new Uint8Array(quicRecordHeaderSize + body.length);
  writeQuicRecordHeader(out, 0, command, body.length);
  out.set(body, quicRecordHeaderSize);
  return out;
}

/** A DATA record carrying [subframe]. */
export function encodeDataRecord(subframe: ArrayLike<number>): Uint8Array {
  return encodeQuicRecord(QuicRecordCommand.data, subframe);
}

/**
 * An OPEN record for channel [id] carrying [openPayload].
 *
 * @throws {RangeError} for an id outside `u48`.
 */
export function encodeOpenRecord(id: number, openPayload: ArrayLike<number>): Uint8Array {
  if (!Number.isInteger(id) || id < 0 || id > quicMaxChannelId) {
    throw new RangeError(`channel id ${id} is outside 0 to ${quicMaxChannelId}`);
  }
  const out = new Uint8Array(quicRecordHeaderSize + quicChannelIdSize + openPayload.length);
  writeQuicRecordHeader(out, 0, QuicRecordCommand.open, quicChannelIdSize + openPayload.length);
  writeU48(out, quicRecordHeaderSize, id);
  out.set(openPayload, quicRecordHeaderSize + quicChannelIdSize);
  return out;
}

const emptyBody = new Uint8Array(0);

/**
 * A CLOSE record carrying [status], prepared for the wire (application
 * codes refused, `CONNECTION_LOST` sent as `UNAVAILABLE`, the payload kept
 * within 1024 bytes). An `OK` without a reason has no body: `00 00 00 80`.
 *
 * @throws {RangeError} for an application code.
 */
export function encodeCloseRecord(status: Status): Uint8Array {
  const wire = statusForWire(status);
  if (wire.code === 0 && wire.reason.length === 0) {
    return encodeQuicRecord(QuicRecordCommand.close, emptyBody);
  }
  return encodeQuicRecord(QuicRecordCommand.close, wire.encode());
}

/** The channel id and open payload of an OPEN record. */
export interface QuicOpenBody {
  readonly id: number;
  /** A view into the record's body. */
  readonly openPayload: Uint8Array;
}

/**
 * The channel id and open payload of an OPEN record's [body]; the payload
 * is a view into it.
 *
 * @throws {ProtocolError} for a body shorter than the channel id.
 */
export function decodeOpenBody(body: Uint8Array): QuicOpenBody {
  if (body.length < quicChannelIdSize) {
    throw new ProtocolError(`OPEN record of ${body.length} bytes`);
  }
  return { id: readU48(body, 0), openPayload: body.subarray(quicChannelIdSize) };
}

/**
 * The status of a CLOSE record's [body]: `OK` when empty. An application
 * code (which the peer must not send) reads as `UNKNOWN`.
 *
 * @throws {ProtocolError} for a body of one byte.
 */
export function decodeCloseBody(body: Uint8Array): Status {
  return statusFromWire(decodeStatusPayload(body, 'CLOSE record'));
}

/**
 * The error code a stream reset, a `STOP_SENDING` or a connection close
 * carries for [status]: the status code itself. Only codes 0 to 255 are
 * sent: `CONNECTION_LOST` goes as `UNAVAILABLE`, as on the frame mux.
 *
 * @throws {RangeError} for an application code (256 and above).
 */
export function quicErrorCode(status: Status): number {
  checkMuxStatus(status);
  if (status.known === StatusCode.connectionLost) {
    return StatusCode.unavailable;
  }
  if (status.code > 0xff) {
    // Reserved codes above 255 (none are defined) go as UNKNOWN.
    return StatusCode.unknown;
  }
  return status.code;
}

/**
 * The status of an error code a stream reset, a `STOP_SENDING` or a
 * connection close carried: a code from 0 to 255 is that status; a larger
 * code, or none (undefined: a WebTransport reset without an application
 * code), reads as `UNKNOWN`, the code kept in the reason.
 */
export function statusOfQuicErrorCode(code: number | undefined, reason = ''): Status {
  if (code === undefined) {
    return Status.of(
      StatusCode.unknown,
      reason.length === 0 ? 'no application error code' : reason,
    );
  }
  if (!Number.isInteger(code) || code < 0 || code > 0xff) {
    return Status.of(
      StatusCode.unknown,
      `error code ${code}${reason.length === 0 ? '' : `: ${reason}`}`,
    );
  }
  return new Status(code, reason);
}
