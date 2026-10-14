/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * The status rules every binding shares for CLOSE and GOAWAY (Dart
 * `mux_protocol.dart`: `checkMuxStatus`, `truncateStatus`, `fitStatus`,
 * `statusForWire`, `statusFromWire`, `decodeStatusPayload`).
 *
 * The frame mux's `mux-protocol.ts` (the protocol core thread) exports the
 * same rules; until it lands, the QUIC binding keeps its own copy here, not
 * exported from the package. Swap the imports for the core's once it does.
 */

import { ProtocolError, Status, StatusCode, StatusFormatError } from '../status.ts';

/**
 * Largest control payload (after the type byte) of `PING`, `PONG`,
 * `GOAWAY`, `NONCE` and `IDENT`: 1024 bytes, and the bound every CLOSE and
 * GOAWAY status payload is kept within.
 */
export const maxControlPayloadLength = 1024;

/** Fixed fields of `MOVED` and `RELOCATED` before the reason: `u48 owner`, `u32 epoch`. */
const slotMoveFields = 10;

/**
 * @throws {RangeError} if [status] carries an application code (256 and
 * above), which a channel CLOSE or a GOAWAY never carries.
 */
export function checkMuxStatus(status: Status): void {
  if (status.isApplicationCode) {
    throw new RangeError(
      `application status codes are not allowed in mux CLOSE or GOAWAY: ${status}`,
    );
  }
}

/**
 * [status] with its reason shortened so that its encoded payload (the `u16`
 * code and the bytes after it) is at most [maxPayload] bytes, cut on a UTF-8
 * character boundary; the fixed fields of `MOVED` and `RELOCATED` are kept
 * whole or dropped with the reason.
 */
export function truncateStatus(status: Status, maxPayload: number): Status {
  if (status.reason.length === 0) {
    return status;
  }
  const room = maxPayload - 2;
  if (room <= 0) {
    return new Status(status.code);
  }
  // At most 3 bytes per UTF-16 code unit (a replaced malformed sequence too).
  if (status.reason.length * 3 <= room) {
    return status;
  }
  const encoded = status.encode();
  if (encoded.length - 2 <= room) {
    return status;
  }
  let cut = 2 + room;
  for (let i = 0; i < 3 && cut > 2 && ((encoded[cut] as number) & 0xc0) === 0x80; i++) {
    cut--;
  }
  const code = status.known;
  if ((code === StatusCode.moved || code === StatusCode.relocated) && cut < 2 + slotMoveFields) {
    return new Status(status.code);
  }
  return Status.decode(encoded.subarray(0, cut));
}

/**
 * A CLOSE or GOAWAY status for a binding's wire: application codes are
 * refused, `CONNECTION_LOST` becomes `UNAVAILABLE` (the bytes after the code
 * kept as they are), and the payload is shortened to
 * {@link maxControlPayloadLength} bytes.
 *
 * @throws {RangeError} for an application code.
 */
export function statusForWire(status: Status): Status {
  checkMuxStatus(status);
  let wire = status;
  if (status.known === StatusCode.connectionLost) {
    const bytes = status.encode();
    bytes[0] = StatusCode.unavailable & 0xff;
    bytes[1] = StatusCode.unavailable >> 8;
    wire = Status.decode(bytes);
  }
  return truncateStatus(wire, maxControlPayloadLength);
}

/**
 * A status received in CLOSE or GOAWAY, for local use: an application code,
 * which the peer must not send there, becomes `UNKNOWN` with the original
 * kept in the reason.
 */
export function statusFromWire(status: Status): Status {
  return status.isApplicationCode ? Status.of(StatusCode.unknown, status.toString()) : status;
}

/**
 * Decodes a status payload carried by CLOSE or GOAWAY; [what] names the
 * carrier for the error.
 *
 * @throws {ProtocolError} for a malformed payload (one byte).
 */
export function decodeStatusPayload(payload: Uint8Array, what: string): Status {
  try {
    return Status.decode(payload);
  } catch (error) {
    if (error instanceof StatusFormatError) {
      throw new ProtocolError(`malformed ${what} status: ${error.message}`);
    }
    throw error;
  }
}

/** [text] cut to at most [max] bytes of UTF-8, at a character boundary. */
export function truncateUtf8(text: string, max: number): string {
  const bytes = new TextEncoder().encode(text);
  if (bytes.length <= max) {
    return text;
  }
  let cut = max;
  while (cut > 0 && ((bytes[cut] as number) & 0xc0) === 0x80) {
    cut--;
  }
  return new TextDecoder().decode(bytes.subarray(0, cut));
}
