/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * What every binding of a connection carries identically, whatever its
 * framing (the frame mux, a stream binding such as QUIC): the `IDENT`
 * payload and its verification (the proof bound to the nonces, or also to
 * the TLS exporter a stream binding supplies), the bound on control
 * payloads, and the rules for the status of a channel CLOSE or a GOAWAY.
 * Ports `mux_protocol.dart`.
 *
 * The frame mux's own wire format (frame header, LIMITS, CREDIT) is in
 * `mux-frame.ts`.
 */

import { ByteFormatError, ByteReader, ByteWriter, bytesEqual, hexString } from '../bytes.ts';
import type { CredentialVerifier, PeerIdentity } from '../identity/contract.ts';
import { ed25519Verify } from '../identity/ed25519.ts';
import { createPeerIdentity } from '../identity/peer-identity.ts';
import {
  ProtocolError,
  Status,
  StatusCode,
  StatusFormatError,
  SwitchboardError,
} from '../status.ts';

const utf8Encoder = new TextEncoder();
const utf8Decoder = new TextDecoder('utf-8', { fatal: false });

/**
 * Largest control payload (after the type byte) of `PING`, `PONG`,
 * `GOAWAY`, `NONCE` and `IDENT`: 1024 bytes, and the bound this
 * implementation keeps every CLOSE and GOAWAY status payload within. The
 * frame mux's `MuxControlMessage.maxControlPayload`.
 */
export const maxControlPayloadLength = 1024;

/**
 * Fixed fields of `MOVED` and `RELOCATED` before the reason: `u48 owner`,
 * `u32 epoch` (`MovedStatus.fieldsLength`).
 */
const slotMoveFields = 10;

/**
 * Refuses a status that must not be used in a channel CLOSE or a GOAWAY,
 * whichever binding carries them.
 *
 * @throws {RangeError} if [status] carries an application code (256 and
 *   above).
 */
export function checkMuxStatus(status: Status): void {
  if (status.isApplicationCode) {
    throw new RangeError(
      `application status codes are not allowed in mux CLOSE or GOAWAY: ${status}`,
    );
  }
}

/**
 * [status] with its reason shortened so that its encoded payload (the
 * `u16` code and the bytes after it) is at most [maxPayload] bytes.
 *
 * Cuts the encoded bytes on a UTF-8 character boundary, so that the fixed
 * fields a code places before its reason (`MOVED` and `RELOCATED`: 10
 * bytes) are kept whole; when they do not fit, everything after the code is
 * dropped. Returns [status] itself when it fits; drops the reason entirely
 * when [maxPayload] leaves no room for it.
 */
export function truncateStatus(status: Status, maxPayload: number): Status {
  if (status.reason.length === 0) {
    return status;
  }
  const room = maxPayload - 2;
  if (room <= 0) {
    return new Status(status.code);
  }
  // The bytes after the code are at most 3 per UTF-16 code unit of the
  // reason (a replaced malformed sequence is at most 3 bytes as well).
  if (status.reason.length * 3 <= room) {
    return status;
  }
  const encoded = status.encode();
  if (encoded.length - 2 <= room) {
    return status;
  }
  let cut = 2 + room;
  // Back off continuation bytes (10xxxxxx), at most the 3 of the longest
  // character, so the cut lands on the first byte of a character.
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
 * [status] shortened (see {@link truncateStatus}) so that a frame carrying
 * it after [overhead] bytes of headers fits both the 1024 byte bound this
 * implementation keeps every CLOSE and GOAWAY status payload within and the
 * peer's announced frame limit [peerMaxFrameSize] (0 when unknown). The
 * local-only `CONNECTION_LOST` code becomes `UNAVAILABLE` on the wire; the
 * bytes after the code are kept as they are, as for any relayed status.
 *
 * @internal The frame mux's; a binding uses {@link statusForWire}.
 */
export function fitStatus(status: Status, overhead: number, peerMaxFrameSize: number): Status {
  let wire = status;
  if (status.known === StatusCode.connectionLost) {
    const bytes = status.encode();
    bytes[0] = StatusCode.unavailable & 0xff;
    bytes[1] = StatusCode.unavailable >> 8;
    wire = Status.decode(bytes);
  }
  let max = maxControlPayloadLength;
  if (peerMaxFrameSize > 0 && peerMaxFrameSize - overhead < max) {
    max = peerMaxFrameSize - overhead;
  }
  return truncateStatus(wire, max);
}

/**
 * A CLOSE or GOAWAY status for a binding's wire: application codes are
 * refused, `CONNECTION_LOST` becomes `UNAVAILABLE`, and the payload is
 * shortened to {@link maxControlPayloadLength} bytes without changing fixed
 * fields or splitting a UTF-8 character. [status] itself is not changed.
 *
 * @throws {RangeError} for an application code.
 */
export function statusForWire(status: Status): Status {
  checkMuxStatus(status);
  return fitStatus(status, 0, 0);
}

/**
 * A status received in CLOSE or GOAWAY, for local use: an application code
 * (256 and above), which the peer must not send there, becomes `UNKNOWN`
 * with the original code kept in the reason, so that the status can be
 * relayed in a CLOSE of this side's own.
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

/** What {@link MuxIdent} is built from. */
export interface MuxIdentInit {
  /** The encoded credential. */
  readonly credential: Uint8Array;
  /** Opaque intent; default empty. */
  readonly intent?: Uint8Array | undefined;
  /** The identity (UTF-8) the sender expects the receiver to have; default empty: none named. */
  readonly receiver?: Uint8Array | undefined;
  /** The holder's signature; default empty: a bearer credential. */
  readonly proof?: Uint8Array | undefined;
}

const empty = new Uint8Array(0);

/**
 * The payload of an IDENT control message: `u16` credential length, the
 * credential, `len8` intent, `len8` receiver, then the proof (the rest).
 *
 * See the wiki page "Polyverse Switchboard Identity and Credentials",
 * section "Connection identity". The credential is carried as bytes; the
 * connection decodes and verifies it with its verifier.
 */
export class MuxIdent {
  /** Length of a NONCE. */
  static readonly nonceLength = 32;

  /** Longest intent. */
  static readonly maxIntentLength = 64;

  /** Longest receiver: an identity string of a credential. */
  static readonly maxReceiverLength = 255;

  /** Length of a proof: an Ed25519 signature. */
  static readonly proofLength = 64;

  /** The label the proof starts with: `SWBIDENT` in ASCII. Must not be modified. */
  static readonly proofLabel: Uint8Array = utf8Encoder.encode('SWBIDENT');

  /**
   * The label an exporter-bound proof starts with instead: `SWBIDTLS` in
   * ASCII. The two labels keep a signature of one form from verifying as
   * the other. Must not be modified.
   */
  static readonly exporterProofLabel: Uint8Array = utf8Encoder.encode('SWBIDTLS');

  /** Length of the TLS exporter an exporter-bound proof signs. */
  static readonly exporterLength = 32;

  /** The encoded credential. */
  readonly credential: Uint8Array;

  /**
   * Opaque intent, at most {@link MuxIdent.maxIntentLength} bytes; empty
   * unless the connection answers a `CONNECT`.
   */
  readonly intent: Uint8Array;

  /**
   * The identity (UTF-8) the sender expects the receiver to have, at most
   * {@link MuxIdent.maxReceiverLength} bytes; empty when the sender names
   * none.
   */
  readonly receiver: Uint8Array;

  /** The holder's Ed25519 signature of {@link MuxIdent.proofMessage}, or empty for a bearer credential. */
  readonly proof: Uint8Array;

  /** An IDENT payload. The arrays are used as given, not copied. */
  constructor(init: MuxIdentInit) {
    this.credential = init.credential;
    this.intent = init.intent ?? empty;
    this.receiver = init.receiver ?? empty;
    this.proof = init.proof ?? empty;
  }

  /**
   * What the holder signs: {@link MuxIdent.proofLabel}, the nonce the
   * IDENT's sender sent ([senderNonce]), the nonce its receiver sent
   * ([receiverNonce]), then the intent and the receiver as the IDENT
   * carries them, each with its `len8` length, so that no byte can move from
   * one field to the other.
   *
   * With an [exporter] (a binding that binds the proof to its TLS session,
   * such as the QUIC and WebTransport binding when both sides can compute
   * the exporter), the message is the exporter-bound one instead:
   * {@link MuxIdent.exporterProofLabel}, the two nonces, the
   * {@link MuxIdent.exporterLength} bytes of the exporter, then the intent
   * and the receiver as above. The frame mux never passes one.
   *
   * @throws {RangeError} for an [intent] or [receiver] longer than a `len8`
   *   field can carry, and for an [exporter] that is not
   *   {@link MuxIdent.exporterLength} bytes long.
   */
  static proofMessage(
    senderNonce: Uint8Array,
    receiverNonce: Uint8Array,
    intent: Uint8Array,
    receiver: Uint8Array,
    exporter?: Uint8Array,
  ): Uint8Array {
    if (intent.length > 0xff || receiver.length > 0xff) {
      throw new RangeError('intent or receiver longer than 255 bytes');
    }
    if (exporter !== undefined && exporter.length !== MuxIdent.exporterLength) {
      throw new RangeError(
        `an exporter is ${MuxIdent.exporterLength} bytes, got ${exporter.length}`,
      );
    }
    const writer = new ByteWriter(
      8 + senderNonce.length + receiverNonce.length + 34 + intent.length + receiver.length,
    );
    writer.bytes(exporter === undefined ? MuxIdent.proofLabel : MuxIdent.exporterProofLabel);
    writer.bytes(senderNonce).bytes(receiverNonce);
    if (exporter !== undefined) {
      writer.bytes(exporter);
    }
    return writer.u8(intent.length).bytes(intent).u8(receiver.length).bytes(receiver).toBytes();
  }

  /**
   * Encodes the payload (after the control type byte).
   *
   * @throws {RangeError} for an intent over {@link MuxIdent.maxIntentLength}
   *   bytes, a receiver over {@link MuxIdent.maxReceiverLength} bytes, and a
   *   payload over {@link maxControlPayloadLength} bytes.
   */
  encode(): Uint8Array {
    if (this.intent.length > MuxIdent.maxIntentLength) {
      throw new RangeError(
        `an intent is at most ${MuxIdent.maxIntentLength} bytes, got ${this.intent.length}`,
      );
    }
    if (this.receiver.length > MuxIdent.maxReceiverLength) {
      throw new RangeError(
        `a receiver is at most ${MuxIdent.maxReceiverLength} bytes, got ${this.receiver.length}`,
      );
    }
    const length =
      2 +
      this.credential.length +
      1 +
      this.intent.length +
      1 +
      this.receiver.length +
      this.proof.length;
    if (length > maxControlPayloadLength) {
      throw new RangeError(
        `IDENT of ${length} bytes exceeds the control payload limit of ${maxControlPayloadLength}`,
      );
    }
    return new ByteWriter(length)
      .u16(this.credential.length)
      .bytes(this.credential)
      .u8(this.intent.length)
      .bytes(this.intent)
      .u8(this.receiver.length)
      .bytes(this.receiver)
      .bytes(this.proof)
      .toBytes();
  }

  /**
   * Decodes an IDENT payload. The fields are views into [bytes].
   *
   * The proof is whatever follows the receiver; its length is checked when
   * the credential is verified, and the receiver (any bytes) when it is
   * compared with the receiver's identity.
   *
   * @throws {ProtocolError} on truncation and for an intent over
   *   {@link MuxIdent.maxIntentLength} bytes.
   */
  static decode(bytes: Uint8Array): MuxIdent {
    const reader = new ByteReader(bytes);
    try {
      const credential = reader.take(reader.u16('credential length'), 'credential');
      const intentLength = reader.u8('intent length');
      if (intentLength > MuxIdent.maxIntentLength) {
        throw new ProtocolError(`IDENT intent of ${intentLength} bytes`);
      }
      const intent = reader.take(intentLength, 'intent');
      const receiver = reader.take(reader.u8('receiver length'), 'receiver');
      return new MuxIdent({ credential, intent, receiver, proof: reader.rest() });
    } catch (error) {
      if (error instanceof ByteFormatError) {
        throw new ProtocolError(`malformed IDENT: ${error.message}`);
      }
      throw error;
    }
  }

  /** The field lengths and the intent, for logs. */
  toString(): string {
    return (
      `MuxIdent(credential of ${this.credential.length} bytes, ` +
      `intent ${hexString(this.intent)}, receiver of ${this.receiver.length} bytes, ` +
      `proof of ${this.proof.length} bytes)`
    );
  }
}

/** What {@link verifyIdent} checks an IDENT against. */
export interface VerifyIdentOptions {
  /** Checks the credential. */
  readonly verifier: CredentialVerifier;
  /** The NONCE the IDENT's sender (the peer) sent. */
  readonly senderNonce: Uint8Array;
  /** The NONCE the IDENT's receiver (this side) sent. */
  readonly receiverNonce: Uint8Array;
  /** This side's identity; undefined for a side without a credential, which refuses every IDENT that names a receiver. */
  readonly localIdentity?: string | undefined;
  /** Refuse an IDENT that names no receiver; default false. */
  readonly requireNamedIdent?: boolean | undefined;
  /** The TLS exporter the proof must be bound to; the frame mux never passes one. */
  readonly exporter?: Uint8Array | undefined;
  /**
   * Wall time, milliseconds since the epoch: the identity's `verifiedAt`,
   * and the time the credential's expiry is checked at. Default
   * `Date.now()`.
   */
  readonly now?: number | undefined;
}

/**
 * Verifies [ident], an `IDENT` the peer sent, as every binding must (wiki
 * page "Polyverse Switchboard Identity and Credentials", section
 * "Connection identity"), and returns the identity it establishes.
 *
 * The checks: a receiver named in the `IDENT` must be `localIdentity`, and
 * with `requireNamedIdent` it must name one (see
 * {@link identReceiverProblem}); the credential must pass the verifier;
 * for a credential with a holder key, the proof must be the holder's
 * Ed25519 signature of {@link MuxIdent.proofMessage} over `senderNonce`
 * (the peer's `NONCE`), `receiverNonce` (this side's `NONCE`), the intent
 * and the receiver, while a bearer credential must carry no proof. The
 * envelope rules (both nonces exchanged, lengths) are the caller's:
 * {@link MuxIdent.decode} checks the lengths.
 *
 * With an `exporter` the proof must be the exporter-bound one: a binding
 * that binds identification to its TLS session passes the session's
 * exporter when both sides announced they can compute it, and a
 * nonce-bound proof then fails, as an exporter-bound one fails without it.
 *
 * @throws {RangeError} for an exporter that is not
 *   {@link MuxIdent.exporterLength} bytes long.
 * @throws {SwitchboardError} `UNAUTHENTICATED` with the reason when the
 *   `IDENT` is refused, or the status the verifier refuses the credential
 *   with. The reason is for logs: a binding ends the connection with a
 *   generic status (the frame mux: GOAWAY `UNAUTHENTICATED`,
 *   "identification failed"). Other errors of the verifier propagate.
 */
export async function verifyIdent(
  ident: MuxIdent,
  options: VerifyIdentOptions,
): Promise<PeerIdentity> {
  const { exporter } = options;
  if (exporter !== undefined && exporter.length !== MuxIdent.exporterLength) {
    throw new RangeError(`an exporter is ${MuxIdent.exporterLength} bytes, got ${exporter.length}`);
  }
  const problem = identReceiverProblem(ident.receiver, options);
  if (problem !== undefined) {
    throw SwitchboardError.of(StatusCode.unauthenticated, problem);
  }
  const now = options.now;
  const credential = await options.verifier.verify(
    ident.credential.slice(),
    now === undefined ? undefined : { now },
  );
  const holder = credential.holderKey;
  if (holder === undefined) {
    if (ident.proof.length > 0) {
      throw SwitchboardError.of(StatusCode.unauthenticated, 'a proof with a bearer credential');
    }
  } else if (
    ident.proof.length !== MuxIdent.proofLength ||
    !(await ed25519Verify(
      holder,
      MuxIdent.proofMessage(
        options.senderNonce,
        options.receiverNonce,
        ident.intent,
        ident.receiver,
        exporter,
      ),
      ident.proof,
    ))
  ) {
    throw SwitchboardError.of(StatusCode.unauthenticated, 'proof of possession does not verify');
  }
  return createPeerIdentity({
    credential,
    intent: ident.intent.slice(),
    verifiedAt: now ?? Date.now(),
    // Checked against this side's identity: valid UTF-8 when not empty.
    receiver: utf8Decoder.decode(ident.receiver),
  });
}

/**
 * Why an `IDENT` naming [receiver] is refused by a side whose identity is
 * `localIdentity`, or undefined when it is not: a receiver other than
 * `localIdentity`, or none while `requireNamedIdent`. The first check of
 * {@link verifyIdent}, which a binding may make before anything
 * asynchronous.
 */
export function identReceiverProblem(
  receiver: Uint8Array,
  options: {
    readonly localIdentity?: string | undefined;
    readonly requireNamedIdent?: boolean | undefined;
  } = {},
): string | undefined {
  if (receiver.length === 0) {
    return options.requireNamedIdent === true ? 'the IDENT names no receiver' : undefined;
  }
  const local = options.localIdentity;
  if (local === undefined || !bytesEqual(utf8Encoder.encode(local), receiver)) {
    return `the IDENT names another receiver ("${utf8Decoder.decode(receiver)}")`;
  }
  return undefined;
}
