/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * What the web provider needs of the W3C WebTransport API, as structural types of its own (the
 * DOM library of TypeScript lacks several members, and a test injects fakes), the reading of the
 * errors the API rejects with, and the building of the `WebTransportError`s that carry a stream
 * error code to the peer. A port of `switchboard_quic`'s `web_interop.dart`. Every member a
 * browser may lack is optional here and feature-tested where it is used, never assumed.
 */

/** A pinned certificate hash of the `WebTransport` options (W3C `WebTransportHash`). */
export interface WebTransportHashInit {
  readonly algorithm: string;
  readonly value: Uint8Array<ArrayBuffer>;
}

/** The options the provider gives the `WebTransport` constructor (W3C `WebTransportOptions`). */
export interface WebTransportInit {
  allowPooling: boolean;
  requireUnreliable: boolean;
  /** The protocols offered (`WT-Available-Protocols`); absent when none are. */
  protocols?: string[];
  /** The SHA-256 hashes of acceptable certificates; absent for the platform roots. */
  serverCertificateHashes?: WebTransportHashInit[];
}

/** The options of `createBidirectionalStream` (W3C `WebTransportSendStreamOptions`). */
export interface WebTransportStreamInit {
  sendOrder?: number;
  waitUntilAvailable?: boolean;
}

/** What `close` takes (W3C `WebTransportCloseInfo`). */
export interface WebTransportCloseInit {
  closeCode?: number;
  reason?: string;
}

/**
 * The members of a W3C `WebTransport` session that the provider uses. Those after `close` are
 * optional: a browser may lack them (Chromium 141 lacks all of them), and the provider tests
 * each before use.
 */
export interface WebTransportLike {
  readonly ready: Promise<unknown>;
  /** Resolves with a `WebTransportCloseInfo` once closed; rejects when the session was lost. */
  readonly closed: Promise<unknown>;
  /** The server's bidirectional streams, `WebTransportBidirectionalStream`s. */
  readonly incomingBidirectionalStreams: ReadableStream<unknown>;
  /** The server's unidirectional streams, readables. */
  readonly incomingUnidirectionalStreams: ReadableStream<unknown>;
  createBidirectionalStream(options?: WebTransportStreamInit): Promise<unknown>;
  close(closeInfo?: WebTransportCloseInit): void;
  /** The protocol the server selected (`WT-Protocol`), empty for none. */
  readonly protocol?: unknown;
  /** A promise that resolves when the server starts draining the session. */
  readonly draining?: unknown;
  /** The session's keying material exporter: an `ArrayBuffer`. */
  readonly exportKeyingMaterial?:
    | ((label: Uint8Array<ArrayBuffer>, context?: Uint8Array<ArrayBuffer>) => Promise<unknown>)
    | undefined;
  /** The session's statistics (W3C `WebTransportConnectionStats`). */
  readonly getStats?: (() => Promise<unknown>) | undefined;
  /** A send group (W3C `WebTransportSendGroup`). */
  readonly createSendGroup?: (() => unknown) | undefined;
}

/** The `WebTransport` constructor, or a fake of it. */
export type WebTransportConstructor = new (
  url: string,
  options: WebTransportInit,
) => WebTransportLike;

/**
 * The `WebTransportError` constructor, or a fake of it: the specification's form
 * `new WebTransportError(message, { streamErrorCode })` or Chromium's
 * `new WebTransportError({ message, streamErrorCode })`, whichever this one has.
 */
export type WebTransportErrorConstructor = new (...args: never[]) => unknown;

/** The bidirectional stream pair `createBidirectionalStream` and the server's streams give. */
export interface BidirectionalStreamLike {
  readonly readable: ReadableStream<unknown>;
  readonly writable: WritableStream<Uint8Array>;
}

/** The send stream attributes of the W3C API (`WebTransportSendStream`), absent in Chromium. */
export interface SendStreamExtras {
  sendOrder?: unknown;
  sendGroup?: unknown;
}

/**
 * A rejection of the WebTransport API, read: the `name` and `message` of an error, and the
 * `source` and `streamErrorCode` of a `WebTransportError`.
 */
export interface ApiError {
  readonly name: string;
  readonly message: string;
  /** `stream` or `session` for a `WebTransportError`. */
  readonly source: string | undefined;
  /** The application code of a `WebTransportError` from a stream; undefined when it carried none. */
  readonly streamErrorCode: number | undefined;
}

/** Reads [error], a rejection of the API (a `WebTransportError`, a `DOMException`, a `TypeError`). */
export function readApiError(error: unknown): ApiError {
  if (typeof error !== 'object' || error === null) {
    return { name: '', message: String(error), source: undefined, streamErrorCode: undefined };
  }
  const fields = error as {
    readonly name?: unknown;
    readonly message?: unknown;
    readonly source?: unknown;
    readonly streamErrorCode?: unknown;
  };
  const text = (value: unknown): string | undefined =>
    typeof value === 'string' ? value : undefined;
  const code = fields.streamErrorCode;
  return {
    name: text(fields.name) ?? '',
    message: text(fields.message) ?? '',
    source: text(fields.source),
    streamErrorCode:
      typeof code === 'number' && Number.isFinite(code) ? Math.trunc(code) : undefined,
  };
}

/** Whether [error] is a `WebTransportError` about one stream: a reset of a readable, a `STOP_SENDING` of a writable. */
export function fromStream(error: ApiError): boolean {
  return error.name === 'WebTransportError' && error.source === 'stream';
}

/** Whether [error] is a `WebTransportError` about the session: it closed or was lost. */
export function fromSession(error: ApiError): boolean {
  return error.name === 'WebTransportError' && error.source === 'session';
}

/** [error] for a message: `name: message (source s, code c)`. */
export function describeApiError(error: ApiError): string {
  const head = error.name === '' ? error.message : `${error.name}: ${error.message}`;
  if (error.source === undefined) return head;
  const code = error.streamErrorCode === undefined ? '' : `, code ${error.streamErrorCode}`;
  return `${head} (source ${error.source}${code})`;
}

/** The largest WebTransport application error code (a `u32`). */
export const maxWebTransportCode = 0xffffffff;

type SpecForm = new (message: string, options: { streamErrorCode: number }) => unknown;
type InitForm = new (init: { message: string; streamErrorCode: number }) => unknown;

/**
 * Builds the `WebTransportError`s that carry a code to the peer (the reason of `abort` on a
 * writable, `RESET_STREAM`, and of `cancel` on a readable, `STOP_SENDING`) in whichever
 * constructor form the browser has, and knows the largest code it carries. The forms are probed
 * once, on first use.
 */
export class StreamErrors {
  readonly #errorClass: WebTransportErrorConstructor | undefined;
  #specForm: boolean | undefined;
  #maxCode = 0;

  /** Over [errorClass], the `WebTransportError` constructor; undefined where there is none. */
  constructor(errorClass: WebTransportErrorConstructor | undefined) {
    this.#errorClass = errorClass;
  }

  /**
   * The largest stream error code this browser carries: 2^32 - 1 by the specification, 255 in
   * Chromium (an octet); 0 where neither form of the constructor works.
   */
  get maxCode(): number {
    this.#probe();
    return this.#maxCode;
  }

  /**
   * A `WebTransportError` with [code] and [message], the reason of an abort or a cancel. Where
   * neither form of the constructor works, undefined for code 0, which the browser sends for a
   * reason that is no `WebTransportError`.
   *
   * @throws {RangeError} for a code above {@link maxCode}.
   */
  create(code: number, message: string): unknown {
    this.#probe();
    if (code > this.#maxCode) {
      throw new RangeError(
        `code ${code}: this browser carries stream error codes of 0 to ${this.#maxCode}`,
      );
    }
    const errorClass = this.#errorClass;
    if (errorClass === undefined || (this.#specForm === false && this.#maxCode === 0)) {
      return undefined;
    }
    return this.#specForm === true
      ? new (errorClass as unknown as SpecForm)(message, { streamErrorCode: code })
      : new (errorClass as unknown as InitForm)({ message, streamErrorCode: code });
  }

  #probe(): void {
    if (this.#specForm !== undefined) return;
    this.#specForm = false;
    const errorClass = this.#errorClass;
    if (errorClass === undefined) return;
    const probe = maxWebTransportCode;
    const readBack = (build: () => unknown): number | undefined => {
      try {
        const code = (build() as { readonly streamErrorCode?: unknown } | null)?.streamErrorCode;
        return typeof code === 'number' && Number.isFinite(code) ? Math.trunc(code) : undefined;
      } catch {
        return undefined;
      }
    };
    const spec = readBack(
      () => new (errorClass as unknown as SpecForm)('probe', { streamErrorCode: probe }),
    );
    if (spec !== undefined) {
      this.#specForm = true;
      this.#maxCode = Math.min(Math.max(spec, 0), probe);
      return;
    }
    const init = readBack(
      () => new (errorClass as unknown as InitForm)({ message: 'probe', streamErrorCode: probe }),
    );
    this.#maxCode = init === undefined ? 0 : Math.min(Math.max(init, 0), probe);
  }
}

const probed = new WeakMap<WebTransportErrorConstructor, StreamErrors>();
const none = new StreamErrors(undefined);

/** The {@link StreamErrors} of [errorClass], probed once per constructor. */
export function streamErrorsOf(errorClass: WebTransportErrorConstructor | undefined): StreamErrors {
  if (errorClass === undefined) return none;
  let errors = probed.get(errorClass);
  if (errors === undefined) {
    errors = new StreamErrors(errorClass);
    probed.set(errorClass, errors);
  }
  return errors;
}

/** The global `WebTransport` constructor; undefined where the browser has none (or outside a secure context). */
export function globalWebTransport(): WebTransportConstructor | undefined {
  const value = (globalThis as { readonly WebTransport?: unknown }).WebTransport;
  return typeof value === 'function' ? (value as WebTransportConstructor) : undefined;
}

/** The global `WebTransportError` constructor; undefined where the browser has none. */
export function globalWebTransportError(): WebTransportErrorConstructor | undefined {
  const value = (globalThis as { readonly WebTransportError?: unknown }).WebTransportError;
  return typeof value === 'function' ? (value as WebTransportErrorConstructor) : undefined;
}

/** Whether [value] is a promise or another thenable. */
export function isThenable(value: unknown): value is PromiseLike<unknown> {
  return (
    (typeof value === 'object' || typeof value === 'function') &&
    value !== null &&
    typeof (value as { readonly then?: unknown }).then === 'function'
  );
}
