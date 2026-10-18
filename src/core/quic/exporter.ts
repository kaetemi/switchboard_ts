/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * The exporter of a connection and the WebTransport error code mapping
 * (wiki page "Polyverse Switchboard QUIC and WebTransport Binding",
 * sections "Exporter-bound proof", "Status codes on streams and
 * connections" and "Test vectors"): what the binding asks of a provider, and
 * the arithmetic a provider that builds the WebTransport layer itself (on a
 * raw TLS exporter and raw HTTP/3 codes) needs. A port of `exporter.dart`.
 *
 * The labels the binding passes to `QuicSession.exportKeyingMaterial`
 * are `rawQuicExporterLabel` and `webTransportIdentLabel` of the
 * contract.
 */

import { webTransportIdentLabel } from './contract.ts';

/** Bytes of the exporter an exporter-bound `IDENT` proof signs. */
export const quicExporterLength = 32;

/**
 * The TLS exporter label of WebTransport's keying material exporter
 * (draft-ietf-webtrans-http3, "Use of Keying Material Exporters"); the
 * application's label (`switchboard-ident`) goes into the context.
 */
export const webTransportExporterLabel = 'EXPORTER-WebTransport';

/**
 * The TLS exporter context of WebTransport's keying material exporter for
 * the session [sessionId] and the application's [label] and [context]:
 * `u64 session ID` (big endian), `len8 label`, label, `len8 context`,
 * context.
 *
 * @throws {RangeError} for a session ID outside 0 to 2^53 - 1, or a label or
 * context over 255 bytes.
 */
export function webTransportExporterContext(
  sessionId: number,
  label: string = webTransportIdentLabel,
  context: ArrayLike<number> = new Uint8Array(0),
): Uint8Array {
  if (!Number.isSafeInteger(sessionId) || sessionId < 0) {
    throw new RangeError(`session ID ${sessionId} is not a u64 within 2^53`);
  }
  const labelBytes = new TextEncoder().encode(label);
  if (labelBytes.length > 0xff || context.length > 0xff) {
    throw new RangeError('label or context longer than 255 bytes');
  }
  const out = new Uint8Array(8 + 1 + labelBytes.length + 1 + context.length);
  const view = new DataView(out.buffer);
  const high = Math.floor(sessionId / 0x100000000);
  view.setUint32(0, high);
  view.setUint32(4, sessionId - high * 0x100000000);
  let offset = 8;
  out[offset++] = labelBytes.length;
  out.set(labelBytes, offset);
  offset += labelBytes.length;
  out[offset++] = context.length;
  out.set(context, offset);
  return out;
}

/** The first HTTP/3 error code of the WebTransport application error codes. */
export const webTransportHttp3CodeBase = 0x52e4a40fa8db;

/**
 * The HTTP/3 error code a stack puts on the wire for the WebTransport
 * application error code [code] (a `u32`): `0x52e4a40fa8db + n + floor(n /
 * 0x1e)`, which skips HTTP/3's reserved codepoints.
 *
 * @throws {RangeError} for a code outside `u32`.
 */
export function webTransportCodeToHttp3(code: number): number {
  if (!Number.isInteger(code) || code < 0 || code > 0xffffffff) {
    throw new RangeError(`WebTransport code ${code} is not a u32`);
  }
  return webTransportHttp3CodeBase + code + Math.floor(code / 0x1e);
}

const lastWebTransportHttp3Code = webTransportCodeToHttp3(0xffffffff);

/**
 * The WebTransport application error code of the HTTP/3 error code
 * [http3], or undefined when it is none (outside the range, or a reserved
 * codepoint).
 */
export function webTransportCodeFromHttp3(http3: number): number | undefined {
  if (http3 < webTransportHttp3CodeBase || http3 > lastWebTransportHttp3Code) {
    return undefined;
  }
  const shifted = http3 - webTransportHttp3CodeBase;
  // One value in every 0x1f is a reserved codepoint.
  if (shifted % 0x1f === 0x1e) {
    return undefined;
  }
  return shifted - Math.floor(shifted / 0x1f);
}

/**
 * [value] as a QUIC variable-length integer (RFC 9000, section 16): 1, 2, 4
 * or 8 bytes, the two top bits of the first byte giving the length.
 *
 * @throws {RangeError} for a value outside 0 to 2^53 - 1 (beyond that a
 * JavaScript number is not exact).
 */
export function encodeQuicVarint(value: number): Uint8Array {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new RangeError(`${value} is not a variable-length integer within 2^53`);
  }
  if (value < 0x40) {
    return Uint8Array.of(value);
  }
  if (value < 0x4000) {
    return Uint8Array.of(0x40 | (value >> 8), value & 0xff);
  }
  if (value < 0x40000000) {
    const out = new Uint8Array(4);
    new DataView(out.buffer).setUint32(0, value);
    out[0] = (out[0] as number) | 0x80;
    return out;
  }
  const out = new Uint8Array(8);
  const view = new DataView(out.buffer);
  const high = Math.floor(value / 0x100000000);
  view.setUint32(0, high);
  view.setUint32(4, value - high * 0x100000000);
  out[0] = (out[0] as number) | 0xc0;
  return out;
}
