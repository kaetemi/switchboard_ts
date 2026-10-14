/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * Stand-ins for what the QUIC binding takes from the protocol core (the
 * "protocol core" thread's mux codecs and identity, ports of `switchboard`'s
 * `mux_frame.dart` and `mux_protocol.dart`): the control messages the
 * binding reuses (`PING`, `PONG`, `GOAWAY`, `NONCE`, `IDENT`), the `IDENT`
 * payload, its proof and its verification, and the reserved channel ids.
 * Not exported from the package: once the core's codecs and identity land,
 * the binding imports them instead and this file goes. Byte for byte the
 * Dart formats (wiki pages "Polyverse Switchboard Mux" and "Polyverse
 * Switchboard Identity and Credentials").
 */

import { ByteFormatError, ByteReader, ByteWriter, bytesEqual, utf8Encode } from '../bytes.ts';
import type { CredentialVerifier, PeerIdentity } from '../identity/contract.ts';
import { ProtocolError, type Status, StatusCode, SwitchboardError } from '../status.ts';
import { decodeStatusPayload, maxControlPayloadLength, truncateStatus } from './wire-status.ts';

/** Control message types, the first byte of a control message. */
export const ControlType = {
  ping: 1,
  pong: 2,
  goAway: 3,
  limits: 4,
  nonce: 5,
  ident: 6,
  credit: 7,
} as const;

/** Largest PING payload a sender uses. */
export const maxPingPayload = 125;

/** A control message: a type byte, then its payload. */
export class ControlMessage {
  /** The type; unknown types decode and are ignored by receivers. */
  readonly type: number;
  /** The payload after the type byte. */
  readonly payload: Uint8Array;

  constructor(type: number, payload: Uint8Array = new Uint8Array(0)) {
    this.type = type;
    this.payload = payload;
  }

  /** @throws {RangeError} for a payload over {@link maxPingPayload} bytes. */
  static ping(payload: Uint8Array = new Uint8Array(0)): ControlMessage {
    if (payload.length > maxPingPayload) {
      throw new RangeError(`a PING payload is at most ${maxPingPayload} bytes`);
    }
    return new ControlMessage(ControlType.ping, payload);
  }

  /** A PONG echoing a PING's payload. */
  static pong(payload: Uint8Array): ControlMessage {
    return new ControlMessage(ControlType.pong, payload);
  }

  /** A GOAWAY carrying [status], shortened to the control payload limit. */
  static goAway(status: Status): ControlMessage {
    return new ControlMessage(
      ControlType.goAway,
      truncateStatus(status, maxControlPayloadLength).encode(),
    );
  }

  /** @throws {RangeError} for a nonce that is not {@link Ident.nonceLength} bytes. */
  static nonce(nonce: Uint8Array): ControlMessage {
    if (nonce.length !== Ident.nonceLength) {
      throw new RangeError(`a NONCE is ${Ident.nonceLength} bytes`);
    }
    return new ControlMessage(ControlType.nonce, nonce);
  }

  /** An IDENT. @throws {RangeError} as {@link Ident.encode}. */
  static ident(ident: Ident): ControlMessage {
    return new ControlMessage(ControlType.ident, ident.encode());
  }

  /** The type byte and the payload. */
  encode(): Uint8Array {
    const out = new Uint8Array(1 + this.payload.length);
    out[0] = this.type;
    out.set(this.payload, 1);
    return out;
  }

  /**
   * A control message; the payload is a view into [bytes].
   *
   * @throws {ProtocolError} when empty or over the control payload limit.
   */
  static decode(bytes: Uint8Array): ControlMessage {
    if (bytes.length === 0) {
      throw new ProtocolError('control message without type');
    }
    if (bytes.length - 1 > maxControlPayloadLength) {
      throw new ProtocolError(
        `control payload of ${bytes.length - 1} bytes exceeds ${maxControlPayloadLength}`,
      );
    }
    return new ControlMessage(bytes[0] as number, bytes.subarray(1));
  }

  /** The status of a GOAWAY. @throws {ProtocolError} for a malformed one. */
  get goAwayStatus(): Status {
    return decodeStatusPayload(this.payload, 'GOAWAY');
  }
}

const proofLabel = utf8Encode('SWBIDENT');
const exporterProofLabel = utf8Encode('SWBIDTLS');

/** An IDENT payload: a credential, the intent, the receiver it names, the proof. */
export class Ident {
  /** Bytes of a NONCE. */
  static readonly nonceLength = 32;
  /** Longest intent. */
  static readonly maxIntentLength = 64;
  /** Longest receiver. */
  static readonly maxReceiverLength = 255;
  /** Bytes of a proof: an Ed25519 signature. */
  static readonly proofLength = 64;
  /** Bytes of the exporter an exporter-bound proof signs. */
  static readonly exporterLength = 32;

  readonly credential: Uint8Array;
  readonly intent: Uint8Array;
  readonly receiver: Uint8Array;
  /** Empty for a bearer credential. */
  readonly proof: Uint8Array;

  constructor(init: {
    credential: Uint8Array;
    intent?: Uint8Array | undefined;
    receiver?: Uint8Array | undefined;
    proof?: Uint8Array | undefined;
  }) {
    this.credential = init.credential;
    this.intent = init.intent ?? new Uint8Array(0);
    this.receiver = init.receiver ?? new Uint8Array(0);
    this.proof = init.proof ?? new Uint8Array(0);
  }

  /**
   * What the holder signs: `SWBIDENT`, the sender's nonce, the receiver's
   * nonce, then `len8` intent and `len8` receiver; with an [exporter],
   * `SWBIDTLS`, the nonces, the exporter, then the same.
   *
   * @throws {RangeError} for an intent or receiver over 255 bytes, or an
   * exporter that is not {@link Ident.exporterLength} bytes.
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
    if (exporter !== undefined && exporter.length !== Ident.exporterLength) {
      throw new RangeError(`an exporter is ${Ident.exporterLength} bytes`);
    }
    const writer = new ByteWriter(128);
    writer.bytes(exporter === undefined ? proofLabel : exporterProofLabel);
    writer.bytes(senderNonce).bytes(receiverNonce);
    if (exporter !== undefined) {
      writer.bytes(exporter);
    }
    return writer.u8(intent.length).bytes(intent).u8(receiver.length).bytes(receiver).toBytes();
  }

  /** @throws {RangeError} for an intent, receiver or payload over its limit. */
  encode(): Uint8Array {
    if (this.intent.length > Ident.maxIntentLength) {
      throw new RangeError(`an intent is at most ${Ident.maxIntentLength} bytes`);
    }
    if (this.receiver.length > Ident.maxReceiverLength) {
      throw new RangeError(`a receiver is at most ${Ident.maxReceiverLength} bytes`);
    }
    const length = 2 + this.credential.length + 1 + this.intent.length + 1 + this.receiver.length;
    if (length + this.proof.length > maxControlPayloadLength) {
      throw new RangeError(
        `IDENT of ${length + this.proof.length} bytes exceeds the control payload limit of ${maxControlPayloadLength}`,
      );
    }
    return new ByteWriter(length + this.proof.length)
      .u16(this.credential.length)
      .bytes(this.credential)
      .u8(this.intent.length)
      .bytes(this.intent)
      .u8(this.receiver.length)
      .bytes(this.receiver)
      .bytes(this.proof)
      .toBytes();
  }

  /** The fields are views into [bytes]. @throws {ProtocolError} when malformed. */
  static decode(bytes: Uint8Array): Ident {
    const reader = new ByteReader(bytes);
    try {
      const credential = reader.take(reader.u16('credential length'), 'credential');
      const intentLength = reader.u8('intent length');
      if (intentLength > Ident.maxIntentLength) {
        throw new ProtocolError(`IDENT intent of ${intentLength} bytes`);
      }
      const intent = reader.take(intentLength, 'intent');
      const receiver = reader.take(reader.u8('receiver length'), 'receiver');
      return new Ident({ credential, intent, receiver, proof: reader.rest() });
    } catch (error) {
      if (error instanceof ByteFormatError) {
        throw new ProtocolError(`malformed IDENT: ${error.message}`);
      }
      throw error;
    }
  }
}

/**
 * Why an IDENT naming [receiver] is refused by a side whose identity is
 * [localIdentity], or undefined when it is not.
 */
export function identReceiverProblem(
  receiver: Uint8Array,
  localIdentity: string | undefined,
  requireNamedIdent: boolean,
): string | undefined {
  if (receiver.length === 0) {
    return requireNamedIdent ? 'the IDENT names no receiver' : undefined;
  }
  if (localIdentity === undefined || !bytesEqual(utf8Encode(localIdentity), receiver)) {
    return `the IDENT names another receiver ("${new TextDecoder().decode(receiver)}")`;
  }
  return undefined;
}

/** Whether an Ed25519 [signature] of [message] verifies under [publicKey]. */
export async function ed25519Verify(
  publicKey: Uint8Array,
  message: Uint8Array,
  signature: Uint8Array,
): Promise<boolean> {
  try {
    const key = await crypto.subtle.importKey(
      'raw',
      publicKey as Uint8Array<ArrayBuffer>,
      { name: 'Ed25519' },
      false,
      ['verify'],
    );
    return await crypto.subtle.verify(
      { name: 'Ed25519' },
      key,
      signature as Uint8Array<ArrayBuffer>,
      message as Uint8Array<ArrayBuffer>,
    );
  } catch {
    return false;
  }
}

/**
 * Verifies [ident], an IDENT the peer sent, and returns the identity it
 * establishes: the receiver it names, the credential, and the proof over
 * the nonces (and the [exporter] when given).
 *
 * @throws {SwitchboardError} UNAUTHENTICATED with the reason (for logs),
 * or the verifier's refusal.
 */
export async function verifyIdent(
  ident: Ident,
  options: {
    verifier: CredentialVerifier;
    senderNonce: Uint8Array;
    receiverNonce: Uint8Array;
    localIdentity?: string | undefined;
    requireNamedIdent?: boolean | undefined;
    exporter?: Uint8Array | undefined;
  },
): Promise<PeerIdentity> {
  const { exporter } = options;
  if (exporter !== undefined && exporter.length !== Ident.exporterLength) {
    throw new RangeError(`an exporter is ${Ident.exporterLength} bytes`);
  }
  const problem = identReceiverProblem(
    ident.receiver,
    options.localIdentity,
    options.requireNamedIdent ?? false,
  );
  if (problem !== undefined) {
    throw SwitchboardError.of(StatusCode.unauthenticated, problem);
  }
  const credential = await options.verifier.verify(ident.credential.slice());
  const holder = credential.holderKey;
  if (holder === undefined) {
    if (ident.proof.length > 0) {
      throw SwitchboardError.of(StatusCode.unauthenticated, 'a proof with a bearer credential');
    }
  } else if (
    ident.proof.length !== Ident.proofLength ||
    !(await ed25519Verify(
      holder,
      Ident.proofMessage(
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
  const intent = ident.intent.slice();
  const receiver = new TextDecoder().decode(ident.receiver);
  const verifiedAt = Date.now();
  return {
    credential,
    intent,
    verifiedAt,
    receiver,
    identity: credential.identity,
    kind: credential.kind,
    scopes: credential.scopes,
    provedPossession: holder !== undefined,
    allows: (right, name) => credential.allows(right, name),
    isExpired: (now) => credential.isExpired(now),
  };
}

/** Whether [id] is reserved: 1, 0xFFFF, 0xFFFF00000000 or 0xFFFFFFFFFFFF. */
export function isReservedChannelId(id: number): boolean {
  return id === 1 || id === 0xffff || id === 0xffff00000000 || id === 0xffffffffffff;
}

/** [length] cryptographically random bytes. */
export function secureRandomBytes(length: number): Uint8Array {
  return crypto.getRandomValues(new Uint8Array(length));
}
