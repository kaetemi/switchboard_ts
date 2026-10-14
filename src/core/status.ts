/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * Status codes shared by every layer of the protocol.
 *
 * Codes 0 to 16 are numerically identical to the gRPC status codes. Codes
 * from 32 upward are Switchboard specific. Codes 256 and above are
 * application codes, valid in Talk aborts only. See the wiki page
 * "Polyverse Switchboard Status Codes".
 */
export const StatusCode = {
  ok: 0,
  cancelled: 1,
  unknown: 2,
  invalidArgument: 3,
  deadlineExceeded: 4,
  notFound: 5,
  alreadyExists: 6,
  permissionDenied: 7,
  resourceExhausted: 8,
  failedPrecondition: 9,
  aborted: 10,
  outOfRange: 11,
  unimplemented: 12,
  internal: 13,
  unavailable: 14,
  dataLoss: 15,
  unauthenticated: 16,
  protocolError: 32,
  goingAway: 33,
  /** Local only: the transport closed. Never sent on the wire. */
  connectionLost: 34,
  frameTooLarge: 35,
  unsupported: 36,
  moved: 37,
  relocated: 38,
} as const;

/** A well-known status code: one of the values of {@link StatusCode}. */
export type StatusCode = (typeof StatusCode)[keyof typeof StatusCode];

/** The name of a well-known status code: one of the keys of {@link StatusCode}. */
export type StatusCodeName = keyof typeof StatusCode;

const codeNames = new Map<number, StatusCodeName>(
  (Object.entries(StatusCode) as [StatusCodeName, number][]).map(([name, code]) => [code, name]),
);

/** The name of a well-known [code], or undefined for other codes. */
export function statusCodeName(code: number): StatusCodeName | undefined {
  return codeNames.get(code);
}

/** Whether [code] is one of the well-known codes. */
export function isKnownStatusCode(code: number): code is StatusCode {
  return codeNames.has(code);
}

const utf8Decoder = new TextDecoder('utf-8', { fatal: false });
const utf8Encoder = new TextEncoder();

/**
 * A status: a `u16` code and an optional human readable reason.
 *
 * The reason is for logs only; programs must not branch on it. A code's
 * definition may place fixed fields before the reason (`MOVED` and
 * `RELOCATED`); this class does not interpret them: a decoded status keeps
 * the bytes after the code exactly as received, {@link encode} writes them
 * back unchanged (so relaying a status is lossless), and {@link reason} is
 * their UTF-8 decoding.
 */
export class Status {
  /** Numeric code, 0 to 0xFFFF. */
  readonly code: number;

  /**
   * Human readable reason, may be empty. For a decoded status, the bytes
   * after the code decoded as UTF-8 (malformed sequences replaced), fixed
   * fields included.
   */
  readonly reason: string;

  /** The bytes after the code as received; undefined when built from a reason. */
  readonly #body: Uint8Array | undefined;

  /** A status with [code] and the human readable [reason]. */
  constructor(code: number, reason = '', body?: Uint8Array) {
    if (!Number.isInteger(code) || code < 0 || code > 0xffff) {
      throw new RangeError(`status code ${code} does not fit in u16`);
    }
    this.code = code;
    this.reason = reason;
    this.#body = body;
  }

  /** A status with a well-known [code] and the human readable [reason]. */
  static of(code: StatusCode, reason = ''): Status {
    return new Status(code, reason);
  }

  static readonly ok: Status = new Status(0);

  /** The well-known code, or undefined for unknown or application codes. */
  get known(): StatusCode | undefined {
    return isKnownStatusCode(this.code) ? this.code : undefined;
  }

  /** The name of the well-known code, or undefined. */
  get name(): StatusCodeName | undefined {
    return statusCodeName(this.code);
  }

  get isOk(): boolean {
    return this.code === 0;
  }

  /** Application codes are 256 and above and are only valid in Talk aborts. */
  get isApplicationCode(): boolean {
    return this.code >= 256;
  }

  /** Whether this status [is] the well-known [code]. */
  is(code: StatusCode): boolean {
    return this.code === code;
  }

  /** The bytes after the code: as received for a decoded status, else the UTF-8 reason. */
  get body(): Uint8Array {
    return this.#body ?? utf8Encoder.encode(this.reason);
  }

  /**
   * Encodes as `u16 code` followed by the UTF-8 reason, or, for a decoded
   * status, by the bytes after the code exactly as they were received.
   */
  encode(): Uint8Array {
    const body = this.body;
    const out = new Uint8Array(2 + body.length);
    out[0] = this.code & 0xff;
    out[1] = (this.code >> 8) & 0xff;
    out.set(body, 2);
    return out;
  }

  /**
   * Decodes a status payload. An empty payload decodes as {@link Status.ok}.
   *
   * The bytes after the code become the reason and are also kept as they
   * are, so that {@link encode} reproduces [bytes] exactly.
   *
   * @throws {StatusFormatError} if the payload is exactly one byte, which
   * cannot be a status.
   */
  static decode(bytes: Uint8Array): Status {
    if (bytes.length === 0) {
      return Status.ok;
    }
    if (bytes.length < 2) {
      throw new StatusFormatError('status payload shorter than 2 bytes');
    }
    const code = (bytes[0] as number) | ((bytes[1] as number) << 8);
    if (bytes.length === 2) {
      return new Status(code);
    }
    const body = bytes.slice(2);
    return new Status(code, utf8Decoder.decode(body), body);
  }

  equals(other: unknown): boolean {
    if (!(other instanceof Status) || other.code !== this.code || other.reason !== this.reason) {
      return false;
    }
    const a = this.body;
    const b = other.body;
    if (a.length !== b.length) {
      return false;
    }
    for (let i = 0; i < a.length; i++) {
      if (a[i] !== b[i]) {
        return false;
      }
    }
    return true;
  }

  /**
   * The code's name and the reason; a reason with control characters or
   * malformed UTF-8 (fixed fields before it, say) is shown as hex bytes.
   */
  toString(): string {
    const name = this.name ?? `code ${this.code}`;
    if (this.reason.length === 0) {
      return name;
    }
    if (isPrintable(this.reason)) {
      return `${name}: ${this.reason}`;
    }
    const hex = Array.from(this.body, (b) => b.toString(16).padStart(2, '0')).join(' ');
    return `${name}: [${hex}]`;
  }
}

/** Whether [text] has no control characters and no replacement character (malformed UTF-8). */
function isPrintable(text: string): boolean {
  for (let i = 0; i < text.length; i++) {
    const c = text.charCodeAt(i);
    if (c < 0x20 || c === 0x7f || c === 0xfffd) {
      return false;
    }
  }
  return true;
}

/** A status payload that cannot be decoded (one byte). */
export class StatusFormatError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'StatusFormatError';
  }
}

/**
 * Base error for every failure reported by the library. Carries a
 * {@link Status}; `code` is its numeric code and `statusName` the
 * well-known name when there is one.
 */
export class SwitchboardError extends Error {
  readonly status: Status;

  constructor(status: Status, options?: ErrorOptions) {
    super(status.toString(), options);
    this.name = 'SwitchboardError';
    this.status = status;
  }

  /** A SwitchboardError with a well-known [code] and [reason]. */
  static of(code: StatusCode, reason = '', options?: ErrorOptions): SwitchboardError {
    return new SwitchboardError(Status.of(code, reason), options);
  }

  get code(): number {
    return this.status.code;
  }

  get statusName(): StatusCodeName | undefined {
    return this.status.name;
  }

  /** Whether the error's status is the well-known [code]. */
  is(code: StatusCode): boolean {
    return this.status.code === code;
  }
}

/** A peer violated the protocol. */
export class ProtocolError extends SwitchboardError {
  constructor(reason: string, options?: ErrorOptions) {
    super(Status.of(StatusCode.protocolError, reason), options);
    this.name = 'ProtocolError';
  }
}

/** Whether [error] is a {@link SwitchboardError} with the well-known [code]. */
export function isStatus(error: unknown, code: StatusCode): error is SwitchboardError {
  return error instanceof SwitchboardError && error.status.code === code;
}
