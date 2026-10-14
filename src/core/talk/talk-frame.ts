/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { ByteFormatError, ByteReader, readU32, writeU32 } from '../bytes.ts';
import { Name } from '../name.ts';
import { ProtocolError, Status, StatusCode } from '../status.ts';
import { TalkBulkReference } from './talk-bulk.ts';

/**
 * The kind of a Talk message, bits `0x30` of the flags byte. The value is
 * the wire value: `(flags & 0x30) >> 4`.
 */
export const TalkKind = {
  /** Kind 0: plain message, request, final response or chained response. */
  message: 0,
  /** Kind 1: a non-final response to a `STREAM` request. */
  streamItem: 1,
  /** Kind 2: abort response, cancel, or channel abort, by the ids present. */
  abort: 2,
  /**
   * Kind 3: control of an outstanding request, by either side. From the
   * responder (a response id): a response timeout extension, a deadline
   * and a renewal it declares, or, empty, a restart of the requester's
   * default timeout. From the requester (a request id of its own): a
   * credit, more items of the item window of its stream request
   * ({@link TalkFrame.isCredit}).
   */
  extend: 3,
} as const;

/** A Talk message kind: one of the values of {@link TalkKind}. */
export type TalkKind = (typeof TalkKind)[keyof typeof TalkKind];

const kindNames = ['message', 'streamItem', 'abort', 'extend'] as const;

/** The name of [kind] (`message`, `streamItem`, `abort`, `extend`). */
export function talkKindName(kind: TalkKind): (typeof kindNames)[number] {
  return kindNames[kind];
}

/** The fields of a {@link TalkFrame}; absent fields take their defaults. */
export interface TalkFrameInit {
  readonly kind: TalkKind;
  /** The procedure name; undefined: the field is absent. */
  readonly procedure?: Name | undefined;
  /** The sender's request id; 0 (the default): absent. */
  readonly requestId?: number | undefined;
  /** The peer's request id this responds to; 0 (the default): absent. */
  readonly responseId?: number | undefined;
  /** The `STREAM` flag; default false. */
  readonly stream?: boolean | undefined;
  /** The item window of a stream request; default {@link TalkFrame.unlimitedWindow}. */
  readonly window?: number | undefined;
  /** The `BULK` flag; default false. */
  readonly bulk?: boolean | undefined;
  /** The payload; default empty. */
  readonly payload?: Uint8Array | undefined;
}

/** What an `EXTEND` from the responder declares, in milliseconds; undefined: unchanged. */
export interface TalkExtension {
  readonly deadline: number | undefined;
  readonly renew: number | undefined;
}

const emptyPayload = new Uint8Array(0);

/**
 * One Talk message header plus payload, as carried in one mux subframe.
 *
 * A {@link requestId} or {@link responseId} of 0 means the field is absent
 * (0 is never a valid id on the wire). An undefined {@link procedure} means
 * the procedure field is absent, which receivers treat as the all-zero
 * name. With {@link bulk} the payload field is a {@link TalkBulkReference}
 * to a bulk channel carrying the payload. A stream request
 * ({@link stream}) carries its item {@link window} after the ids.
 *
 * See the wiki page "Polyverse Switchboard Talk" for the layout and the
 * flag rules.
 */
export class TalkFrame {
  /** Flag bit: an 8-byte procedure name follows. */
  static readonly flagHasProcedure = 0x01;

  /** Flag bit: a request id follows; the sender expects a response. */
  static readonly flagHasRequest = 0x02;

  /** Flag bit: a response id follows; this responds to the peer's request. */
  static readonly flagHasResponse = 0x04;

  /** Flag bit: with HAS_REQUEST, the sender accepts stream items, and a `u32` item window follows the ids. */
  static readonly flagStream = 0x08;

  /** Mask of the kind field. */
  static readonly kindMask = 0x30;

  /** Flag bit: the payload is on a bulk channel; the payload field is a {@link TalkBulkReference}. */
  static readonly flagBulk = 0x40;

  /** Reserved bit; receiving it set is a channel protocol error. */
  static readonly reservedMask = 0x80;

  /** Largest valid request or response id (`u24`). */
  static readonly maxId = 0xffffff;

  /** Length of a non-empty `EXTEND` payload: `u32 deadline ms`, `u32 renew ms`. */
  static readonly extendPayloadLength = 8;

  /** Largest value of an `EXTEND` field, in milliseconds (`u32`). */
  static readonly maxExtendMillis = 0xffffffff;

  /**
   * The item window of a stream request that takes any number of items:
   * no accounting, the channel's flow control alone bounds the stream.
   * Finite windows are 0 to `unlimitedWindow - 1`.
   */
  static readonly unlimitedWindow = 0xffffffff;

  /** Length of a credit's payload ({@link isCredit}): `u32 items`. */
  static readonly creditPayloadLength = 4;

  /** Message kind. */
  readonly kind: TalkKind;

  /** Procedure name, or undefined when the field is absent. */
  readonly procedure: Name | undefined;

  /** The sender's request id, or 0 if the sender expects no response. */
  readonly requestId: number;

  /** The peer's request id this frame responds to, or 0 if not a response. */
  readonly responseId: number;

  /** The `STREAM` flag: the sender accepts `STREAM_ITEM` responses. */
  readonly stream: boolean;

  /**
   * The item window of a stream request ({@link stream}): how many
   * `STREAM_ITEM`s the requester is ready to take before it grants more
   * with a credit ({@link isCredit}), 0 for none until a grant, or
   * {@link unlimitedWindow}. On the wire only with {@link stream}; any
   * other frame keeps the default, {@link unlimitedWindow}.
   */
  readonly window: number;

  /** The `BULK` flag: {@link payload} is a {@link TalkBulkReference}. */
  readonly bulk: boolean;

  /** The payload. After {@link decode} this is a view into the decoded buffer. */
  readonly payload: Uint8Array;

  /** Creates a frame. Nothing is validated until {@link encode}. */
  constructor(init: TalkFrameInit) {
    this.kind = init.kind;
    this.procedure = init.procedure;
    this.requestId = init.requestId ?? 0;
    this.responseId = init.responseId ?? 0;
    this.stream = init.stream ?? false;
    this.window = init.window ?? TalkFrame.unlimitedWindow;
    this.bulk = init.bulk ?? false;
    this.payload = init.payload ?? emptyPayload;
  }

  /**
   * The bulk reference a {@link bulk} frame carries as its payload.
   *
   * @throws {Error} for a frame without {@link bulk};
   * {@link ProtocolError} for a malformed reference (which {@link decode}
   * never returns).
   */
  get bulkReference(): TalkBulkReference {
    if (!this.bulk) {
      throw new Error('only BULK frames carry a bulk reference');
    }
    return TalkBulkReference.decode(this.payload);
  }

  /** A copy of this frame with {@link bulk} set and [reference] as its payload. */
  withBulk(reference: TalkBulkReference): TalkFrame {
    return new TalkFrame({
      kind: this.kind,
      procedure: this.procedure,
      requestId: this.requestId,
      responseId: this.responseId,
      stream: this.stream,
      window: this.window,
      bulk: true,
      payload: reference.encode(),
    });
  }

  /** True when {@link requestId} is present. */
  get hasRequest(): boolean {
    return this.requestId !== 0;
  }

  /** True when {@link responseId} is present. */
  get hasResponse(): boolean {
    return this.responseId !== 0;
  }

  /**
   * True for a credit: an `EXTEND` from the requester ({@link requestId},
   * its own stream request), granting the responder {@link creditItems}
   * more items of that request's item window.
   */
  get isCredit(): boolean {
    return this.kind === TalkKind.extend && this.hasRequest;
  }

  /**
   * The number of items a credit ({@link isCredit}) grants, at least 1.
   *
   * @throws {Error} for any other frame.
   */
  get creditItems(): number {
    if (!this.isCredit) {
      throw new Error('only credits carry an item count');
    }
    return readU32(this.payload, 0);
  }

  /**
   * The payload of a credit granting [items] more items (`u32`, little
   * endian).
   *
   * @throws {RangeError} unless [items] is 1 to 0xFFFFFFFF.
   */
  static creditPayload(items: number): Uint8Array {
    if (!Number.isInteger(items) || items < 1 || items > 0xffffffff) {
      throw new RangeError(`items ${items} must be 1 to 0xFFFFFFFF`);
    }
    const out = new Uint8Array(TalkFrame.creditPayloadLength);
    writeU32(out, 0, items);
    return out;
  }

  /** The flags byte this frame encodes to. */
  get flags(): number {
    return (
      (this.procedure !== undefined ? TalkFrame.flagHasProcedure : 0) |
      (this.hasRequest ? TalkFrame.flagHasRequest : 0) |
      (this.hasResponse ? TalkFrame.flagHasResponse : 0) |
      (this.stream ? TalkFrame.flagStream : 0) |
      (this.kind << 4) |
      (this.bulk ? TalkFrame.flagBulk : 0)
    );
  }

  /**
   * The status carried by an {@link TalkKind.abort} frame.
   *
   * An empty payload means {@link StatusCode.unknown} (unlike a mux CLOSE,
   * where it means OK).
   *
   * @throws {Error} for other kinds.
   */
  get status(): Status {
    if (this.kind !== TalkKind.abort) {
      throw new Error('only abort frames carry a status');
    }
    if (this.payload.length === 0) {
      return Status.of(StatusCode.unknown);
    }
    return Status.decode(this.payload);
  }

  /**
   * The values carried by an {@link TalkKind.extend} frame from the
   * responder: the deadline and the renewal it declares, in milliseconds,
   * each undefined when the field is 0 ("unchanged"). Both are undefined
   * for an empty payload, which restarts the requester's default timeout;
   * {@link payload} tells the two apart.
   *
   * @throws {Error} for other kinds, and for a credit ({@link isCredit}).
   */
  get extension(): TalkExtension {
    if (this.kind !== TalkKind.extend) {
      throw new Error('only EXTEND frames carry an extension');
    }
    if (this.hasRequest) {
      throw new Error('a credit carries no extension');
    }
    if (this.payload.length === 0) {
      return { deadline: undefined, renew: undefined };
    }
    const deadline = readU32(this.payload, 0);
    const renew = readU32(this.payload, 4);
    return {
      deadline: deadline === 0 ? undefined : deadline,
      renew: renew === 0 ? undefined : renew,
    };
  }

  /**
   * The payload of an `EXTEND` declaring [deadline] and [renew], given in
   * milliseconds: each in whole milliseconds, rounded up (a value under
   * 1 ms is sent as 1 ms, since 0 means "unchanged") and capped at
   * {@link maxExtendMillis}; undefined is sent as 0. Empty when both are
   * undefined.
   *
   * @throws {RangeError} for a negative value or NaN.
   */
  static extendPayload(
    values: { readonly deadline?: number | undefined; readonly renew?: number | undefined } = {},
  ): Uint8Array {
    const { deadline, renew } = values;
    if (deadline === undefined && renew === undefined) {
      return emptyPayload;
    }
    const out = new Uint8Array(TalkFrame.extendPayloadLength);
    writeU32(out, 0, extendMillis(deadline, 'deadline'));
    writeU32(out, 4, extendMillis(renew, 'renew'));
    return out;
  }

  /**
   * Encodes to the short canonical form: optional fields are present only
   * when set, the item {@link window} whenever {@link stream} is (unlimited
   * included).
   *
   * @throws {RangeError} if the frame breaks a rule a receiver would reject
   * as a protocol error, so that this implementation never sends such a
   * frame.
   */
  encode(): Uint8Array {
    checkU(this.requestId, TalkFrame.maxId, 'requestId', 'u24');
    checkU(this.responseId, TalkFrame.maxId, 'responseId', 'u24');
    checkU(this.window, TalkFrame.unlimitedWindow, 'window', 'u32');
    if (!this.stream && this.window !== TalkFrame.unlimitedWindow) {
      throw new RangeError(`window ${this.window} without STREAM`);
    }
    const error = ruleViolation(
      this.kind,
      this.procedure !== undefined,
      this.hasRequest,
      this.hasResponse,
      this.stream,
      this.bulk,
      this.payload.length,
    );
    if (error !== undefined) {
      throw new RangeError(error);
    }
    if (this.isCredit && this.creditItems === 0) {
      throw new RangeError('credit of 0 items');
    }
    if (this.bulk) {
      try {
        TalkBulkReference.decode(this.payload);
      } catch (e) {
        if (e instanceof ProtocolError) {
          throw new RangeError(e.status.reason);
        }
        throw e;
      }
    }
    const procedure = this.procedure;
    const length =
      1 +
      (procedure !== undefined ? Name.byteLength : 0) +
      (this.hasRequest ? 3 : 0) +
      (this.hasResponse ? 3 : 0) +
      (this.stream ? 4 : 0) +
      this.payload.length;
    const out = new Uint8Array(length);
    out[0] = this.flags;
    let offset = 1;
    if (procedure !== undefined) {
      procedure.writeTo(out, offset);
      offset += Name.byteLength;
    }
    if (this.hasRequest) {
      offset = writeU24(out, offset, this.requestId);
    }
    if (this.hasResponse) {
      offset = writeU24(out, offset, this.responseId);
    }
    if (this.stream) {
      writeU32(out, offset, this.window);
      offset += 4;
    }
    out.set(this.payload, offset);
    return out;
  }

  /**
   * Decodes one Talk message. The payload of the result is a view of
   * [bytes], not a copy.
   *
   * @throws {ProtocolError} for a truncated header (a stream request
   * without its item window among them), the reserved bit, an invalid flag
   * combination, an invalid procedure name, or a violation of the kind
   * rules (an `EXTEND` with both ids or neither, an extend payload that is
   * neither empty nor {@link extendPayloadLength} bytes long, a credit
   * payload that is not {@link creditPayloadLength} bytes long or grants 0
   * items, `BULK` on an `ABORT` or an `EXTEND`, and a bulk reference that
   * is not a valid {@link TalkBulkReference} among them).
   */
  static decode(bytes: Uint8Array): TalkFrame {
    const reader = new ByteReader(bytes);
    try {
      const flags = reader.u8('flags');
      if ((flags & TalkFrame.reservedMask) !== 0) {
        throw new ProtocolError(`reserved talk flag bits set: 0x${flags.toString(16)}`);
      }
      const kind = ((flags & TalkFrame.kindMask) >> 4) as TalkKind;
      const hasProcedure = (flags & TalkFrame.flagHasProcedure) !== 0;
      const hasRequest = (flags & TalkFrame.flagHasRequest) !== 0;
      const hasResponse = (flags & TalkFrame.flagHasResponse) !== 0;
      const stream = (flags & TalkFrame.flagStream) !== 0;
      const bulk = (flags & TalkFrame.flagBulk) !== 0;
      const procedure = hasProcedure ? reader.name('procedure') : undefined;
      const requestId = hasRequest ? reader.u24('request id') : 0;
      if (hasRequest && requestId === 0) {
        throw new ProtocolError('request id 0');
      }
      const responseId = hasResponse ? reader.u24('response id') : 0;
      if (hasResponse && responseId === 0) {
        throw new ProtocolError('response id 0');
      }
      const streamError = streamViolation(kind, hasRequest);
      if (stream && streamError !== undefined) {
        throw new ProtocolError(streamError);
      }
      const window = stream ? reader.u32('item window') : TalkFrame.unlimitedWindow;
      const payload = reader.rest();
      const error = ruleViolation(
        kind,
        hasProcedure,
        hasRequest,
        hasResponse,
        stream,
        bulk,
        payload.length,
      );
      if (error !== undefined) {
        throw new ProtocolError(error);
      }
      if (bulk) {
        TalkBulkReference.decode(payload);
      }
      const frame = new TalkFrame({
        kind,
        procedure,
        requestId,
        responseId,
        stream,
        window,
        bulk,
        payload,
      });
      if (frame.isCredit && frame.creditItems === 0) {
        throw new ProtocolError('credit of 0 items');
      }
      return frame;
    } catch (e) {
      // A truncated field, or an invalid name.
      if (e instanceof ByteFormatError || e instanceof RangeError) {
        throw new ProtocolError(e.message);
      }
      throw e;
    }
  }

  /** Whether [other] is a frame with the same fields and payload bytes. */
  equals(other: unknown): boolean {
    if (
      !(other instanceof TalkFrame) ||
      other.kind !== this.kind ||
      !sameName(other.procedure, this.procedure) ||
      other.requestId !== this.requestId ||
      other.responseId !== this.responseId ||
      other.stream !== this.stream ||
      other.window !== this.window ||
      other.bulk !== this.bulk ||
      other.payload.length !== this.payload.length
    ) {
      return false;
    }
    for (let i = 0; i < this.payload.length; i++) {
      if (other.payload[i] !== this.payload[i]) {
        return false;
      }
    }
    return true;
  }

  toString(): string {
    const parts: string[] = [talkKindName(this.kind)];
    if (this.procedure !== undefined) {
      parts.push(`"${this.procedure}"`);
    }
    if (this.hasRequest) {
      parts.push(`req=${this.requestId}`);
    }
    if (this.hasResponse) {
      parts.push(`resp=${this.responseId}`);
    }
    if (this.stream) {
      parts.push(
        this.window === TalkFrame.unlimitedWindow ? 'stream' : `stream, window=${this.window}`,
      );
    }
    if (this.bulk) {
      parts.push('bulk');
    }
    parts.push(`${this.payload.length} bytes`);
    return `TalkFrame(${parts.join(', ')})`;
  }
}

/**
 * The flag and kind rules shared by encode and decode. Returns a
 * description of the first violation, or undefined if the combination is
 * valid.
 */
function ruleViolation(
  kind: TalkKind,
  hasProcedure: boolean,
  hasRequest: boolean,
  hasResponse: boolean,
  stream: boolean,
  bulk: boolean,
  payloadLength: number,
): string | undefined {
  if (stream) {
    const error = streamViolation(kind, hasRequest);
    if (error !== undefined) {
      return error;
    }
  }
  if (bulk) {
    if (kind === TalkKind.abort || kind === TalkKind.extend) {
      return `BULK on ${kind === TalkKind.abort ? 'ABORT' : 'EXTEND'}`;
    }
    if (payloadLength !== TalkBulkReference.encodedLength) {
      return `BULK reference of ${payloadLength} bytes`;
    }
  }
  switch (kind) {
    case TalkKind.message:
      if (!hasResponse && !hasProcedure) {
        return hasRequest ? 'request without procedure' : 'plain message without procedure';
      }
      break;
    case TalkKind.streamItem:
      if (!hasResponse) {
        return 'STREAM_ITEM without HAS_RESPONSE';
      }
      break;
    case TalkKind.abort:
      if (hasRequest && hasResponse) {
        return 'ABORT with both request and response id';
      }
      if (payloadLength === 1) {
        return 'ABORT payload is not a status';
      }
      break;
    case TalkKind.extend:
      if (hasRequest && hasResponse) {
        return 'EXTEND with both request and response id';
      }
      if (!hasRequest && !hasResponse) {
        return 'EXTEND without an id';
      }
      if (hasRequest) {
        if (payloadLength !== TalkFrame.creditPayloadLength) {
          return `credit payload of ${payloadLength} bytes`;
        }
      } else if (payloadLength !== 0 && payloadLength !== TalkFrame.extendPayloadLength) {
        return `EXTEND payload of ${payloadLength} bytes`;
      }
      break;
  }
  return undefined;
}

/**
 * Where the `STREAM` flag may not be: without a request id, and on an
 * `ABORT` or an `EXTEND`, whose request id names an outstanding request (a
 * cancel, a credit) instead of opening one.
 */
function streamViolation(kind: TalkKind, hasRequest: boolean): string | undefined {
  if (!hasRequest) {
    return 'STREAM without HAS_REQUEST';
  }
  if (kind === TalkKind.abort || kind === TalkKind.extend) {
    return `STREAM on ${kind === TalkKind.abort ? 'ABORT' : 'EXTEND'}`;
  }
  return undefined;
}

function extendMillis(value: number | undefined, name: string): number {
  if (value === undefined) {
    return 0;
  }
  if (Number.isNaN(value) || value < 0) {
    throw new RangeError(`${name} ${value} must not be negative`);
  }
  const ms = Math.ceil(value);
  return ms < 1 ? 1 : ms > TalkFrame.maxExtendMillis ? TalkFrame.maxExtendMillis : ms;
}

function checkU(value: number, max: number, name: string, width: string): void {
  if (!Number.isInteger(value) || value < 0 || value > max) {
    throw new RangeError(`${name} ${value} must fit in ${width}`);
  }
}

function writeU24(out: Uint8Array, offset: number, value: number): number {
  out[offset] = value & 0xff;
  out[offset + 1] = (value >> 8) & 0xff;
  out[offset + 2] = (value >> 16) & 0xff;
  return offset + 3;
}

function sameName(a: Name | undefined, b: Name | undefined): boolean {
  return a === undefined ? b === undefined : a.equals(b);
}
