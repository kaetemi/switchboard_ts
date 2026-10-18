/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import type { PayloadLike } from '../bytes.ts';
import type { MuxChannel } from '../mux/contract.ts';
import type { Name, NameLike } from '../name.ts';
import type { Status, SwitchboardError } from '../status.ts';
import type { Clock } from '../util/clock.ts';
import type { Logger } from '../util/logger.ts';
import type { TalkFrame } from './talk-frame.ts';

/**
 * The Talk layer (wiki page "Polyverse Switchboard Talk") as the layers
 * above see it: message chains on one channel. The implementation
 * (`talk-frame.ts`, `talk-channel.ts`, `talk-message.ts`, bulk transfers,
 * `forwardMessage`) is the "Talk" thread's; the node, the naming service
 * and the proxies program against these interfaces.
 *
 * Durations are milliseconds. Payloads are bytes or text (UTF-8).
 */

/** Options of a Talk channel. */
export interface TalkOptions {
  /** Requester side; default 15 000. */
  readonly requestTimeout: number;
  /** Responder side; default 10 000. */
  readonly replyTimeout: number;
  /** Default 1024; beyond: ABORT RESOURCE_EXHAUSTED. */
  readonly maxIncomingRequests: number;
  /** Default 1024; beyond: throws RESOURCE_EXHAUSTED locally. */
  readonly maxOutgoingRequests: number;
  /** Requester: declared values raised to it; default 1000 (0: no floor). */
  readonly minExtension: number;
  /** Requester: declared values lowered to it; default 3 600 000 (0: no clamp). */
  readonly maxExtension: number;
  /** Responder: added on the wire by `extend()`, not to its own timeout; default 5000. */
  readonly extendBuffer: number;
  /** Sending: largest encoded message sent inline; undefined = the channel's `maxSubframeLength`. */
  readonly bulkThreshold: number | undefined;
  /** Sending: a bulk source is coalesced into chunks of this; default 64 KiB; a positive integer. */
  readonly bulkChunkSize: number;
  /** Receiving: largest bulk payload reassembled per message; default 16 MiB. */
  readonly maxInlinePayload: number;
  /** Receiving: what the channel holds for payloads the application has not taken; default 16 MiB; 0 = none. */
  readonly reassemblyBudget: number;
  /** Receiving: the window of a bulk channel is raised to min(declared length, this) when read; default 1 MiB. */
  readonly bulkWindow: number;
  /** Receiving: bulk channels opened ahead of their messages; default 16. */
  readonly maxUnclaimedBulk: number;
  /** Receiving: how long a BULK message waits for its bulk channel; default 10 000. */
  readonly bulkOpenTimeout: number;
  /** Receiving: a bulk channel without a byte for this long while awaited is closed DEADLINE_EXCEEDED; default 30 000; 0: none. */
  readonly bulkIdleTimeout: number;
  /** Receiving: which bulk messages are delivered at once with their payload as a stream; undefined: all reassembled first. */
  readonly streamBulk: ((message: TalkMessage) => boolean) | undefined;
  /**
   * Responder: the credit bound, how long a stream request with a finite
   * item window may stay exhausted while this side has something to send
   * (and a request opened with window 0 from its arrival until the first
   * grant); then ABORT DEADLINE_EXCEEDED. Default 60 000; 0: no bound.
   */
  readonly creditTimeout: number;
  /** Time and timers; undefined: the connection's clock. */
  readonly clock: Clock | undefined;
  /** Diagnostics; undefined: the connection's logger. */
  readonly logger: Logger | undefined;
}

export const defaultTalkOptions: TalkOptions = Object.freeze({
  requestTimeout: 15_000,
  replyTimeout: 10_000,
  maxIncomingRequests: 1024,
  maxOutgoingRequests: 1024,
  minExtension: 1000,
  maxExtension: 3_600_000,
  extendBuffer: 5000,
  bulkThreshold: undefined,
  bulkChunkSize: 64 * 1024,
  maxInlinePayload: 16 * 1024 * 1024,
  reassemblyBudget: 16 * 1024 * 1024,
  bulkWindow: 1024 * 1024,
  maxUnclaimedBulk: 16,
  bulkOpenTimeout: 10_000,
  bulkIdleTimeout: 30_000,
  streamBulk: undefined,
  creditTimeout: 60_000,
  clock: undefined,
  logger: undefined,
});

/**
 * [options] over {@link defaultTalkOptions}.
 *
 * @throws {RangeError} for a `bulkChunkSize` that is not a positive
 * integer (a bulk sender would loop on empty chunks).
 */
export function talkOptions(options?: Partial<TalkOptions>): TalkOptions {
  if (options === undefined) {
    return defaultTalkOptions;
  }
  const merged = { ...defaultTalkOptions, ...options };
  const chunk = merged.bulkChunkSize;
  if (!Number.isInteger(chunk) || chunk < 1) {
    throw new RangeError(`bulkChunkSize ${chunk} is not a positive integer`);
  }
  return merged;
}

/** A byte source sent as a bulk payload. */
export type BulkSource =
  | ReadableStream<Uint8Array>
  | AsyncIterable<Uint8Array>
  | Iterable<Uint8Array>;

/** What every send takes beside the payload. */
export interface SendOptions {
  /** Sent instead of the procedure string when given (lossless, for generated stubs). */
  name?: Name | undefined;
  /** A byte stream sent as the message's bulk payload; the payload must then be empty. */
  bulk?: BulkSource | undefined;
  /** The length of `bulk`, when known. */
  bulkLength?: number | undefined;
}

/** What a request takes. */
export interface RequestOptions extends SendOptions {
  /** Requester timeout; default `TalkOptions.requestTimeout`. */
  timeout?: number | undefined;
  /** Cancels the request (sends a cancel) when aborted. */
  signal?: AbortSignal | undefined;
  /** Called on every EXTEND from the responder. */
  onExtend?: ((extension: { deadline?: number; renew?: number }) => void) | undefined;
  /** The answer is delivered in wire order with the channel's messages. */
  ordered?: boolean | undefined;
  /**
   * Stream requests: the item window, how many items the responder may
   * send before this side grants more (`TalkStream.grant`); 0 for none
   * until a grant. Undefined: unlimited.
   */
  window?: number | undefined;
}

/** What a reply takes. */
export interface ReplyOptions {
  procedure?: NameLike | undefined;
  bulk?: BulkSource | undefined;
  bulkLength?: number | undefined;
}

export interface ReplyRequestOptions extends ReplyOptions {
  timeout?: number | undefined;
  signal?: AbortSignal | undefined;
  onExtend?: ((extension: { deadline?: number; renew?: number }) => void) | undefined;
  /** Stream requests: the item window, as for `RequestOptions.window`. */
  window?: number | undefined;
}

/** The peer sent ABORT. The class is exported from `talk-channel.ts` (and the barrel). */
export interface TalkAbortError extends SwitchboardError {
  /** A channel abort rather than an abort response. */
  readonly isChannelAbort: boolean;
}

/** A handle for a request with a single final response. */
export interface TalkRequest {
  readonly requestId: number;
  /** The final response; never an unhandled rejection. */
  readonly response: Promise<TalkMessage>;
  /**
   * When the requester timeout expires, in the time of the channel's clock
   * (`clock.now()`, monotonic); undefined: none, or ended.
   */
  readonly deadline: number | undefined;
  cancel(status?: Status): void;
}

/** A handle for a stream request: the items, then the final response. Iterating it iterates `items`. */
export interface TalkStream extends AsyncIterable<TalkMessage> {
  readonly requestId: number;
  /** As `TalkRequest.deadline`; also undefined while the item window is exhausted. */
  readonly deadline: number | undefined;
  /** The item window as this side sees it: granted less arrived; undefined when unlimited. */
  readonly window: number | undefined;
  /**
   * Grants the responder [items] more items (sends a credit). Nothing is
   * granted on the application's behalf.
   *
   * @throws {RangeError} below 1 or beyond the largest window; {@link Error} for an unlimited window.
   */
  grant(items: number): void;
  /** STREAM_ITEMs; errors on abort, timeout or cancel; stopping the iteration cancels the request. */
  readonly items: AsyncIterable<TalkMessage>;
  /** The final response (may carry a payload and may itself be a request). */
  readonly done: Promise<TalkMessage>;
  cancel(status?: Status): void;
}

/** An incoming message: plain message, request, or response, with the reply API. */
export interface TalkMessage {
  readonly channel: TalkChannel;
  /** The decoded wire frame. */
  readonly frame: TalkFrame;
  readonly procedure: Name;
  /** The procedure as text (lossy for non-UTF-8 names). */
  readonly procedureName: string;
  /** The payload; a reassembled bulk payload like an inline one. @throws {Error} for a streamed bulk payload not yet taken with `bytes()`. */
  readonly payload: Uint8Array;
  /** The payload as UTF-8 text. */
  text(): string;
  /** The payload parsed as JSON. */
  json<T = unknown>(): T;
  /** The payload, reassembled when bulk, at most `maxLength` (default `maxInlinePayload`). */
  bytes(options?: { maxLength?: number }): Promise<Uint8Array>;
  /** The BULK flag. */
  readonly isBulk: boolean;
  /** The declared bulk length; undefined when unknown or inline. */
  readonly bulkLength: number | undefined;
  /** The payload as a stream (once for a streamed one); credit goes back as the reader takes chunks. */
  readonly bulk: ReadableStream<Uint8Array>;
  /** 0 if the peer expects no reply. */
  readonly requestId: number;
  /** 0 if not a response. */
  readonly responseId: number;
  readonly expectsReply: boolean;
  readonly expectsStream: boolean;
  /** The peer cancelled, the responder timeout expired, the channel closed, or the id was reused. */
  readonly isCancelled: boolean;
  /** Resolves when cancelled (never for non-requests). */
  readonly onCancel: Promise<void>;
  /** Aborted when cancelled: pass it to what the handler awaits. */
  readonly signal: AbortSignal;
  readonly canReply: boolean;
  /**
   * Stream requests with a finite item window: what the peer granted less
   * the items sent; undefined when unlimited or not a stream request.
   */
  readonly window: number | undefined;
  /** Resolves once {@link window} is above 0 or the request ended; at once when unlimited. Never rejects. */
  readonly whenCredit: Promise<void>;
  /** Per-request credit bound: undefined = `TalkOptions.creditTimeout`, 0 = none. */
  setCreditTimeout(timeout: number | undefined): void;

  // Reply API: each throws FAILED_PRECONDITION if !expectsReply or already finally replied.
  reply(payload?: PayloadLike, options?: ReplyOptions): Promise<void>;
  replyRequest(payload?: PayloadLike, options?: ReplyRequestOptions): Promise<TalkMessage>;
  startReplyRequest(payload?: PayloadLike, options?: ReplyRequestOptions): TalkRequest;
  replyStreamRequest(payload?: PayloadLike, options?: ReplyRequestOptions): TalkStream;
  /** @throws {SwitchboardError} FAILED_PRECONDITION if !expectsStream. */
  replyItem(payload?: PayloadLike, options?: ReplyOptions): Promise<void>;
  replyItemRequest(payload?: PayloadLike, options?: ReplyRequestOptions): Promise<TalkMessage>;
  startReplyItemRequest(payload?: PayloadLike, options?: ReplyRequestOptions): TalkRequest;
  replyItemStreamRequest(payload?: PayloadLike, options?: ReplyRequestOptions): TalkStream;
  /** replyItem per item, then reply(trailer); an error from `items` becomes replyAbort. Never rejects. */
  replyStream(
    items: AsyncIterable<PayloadLike> | Iterable<PayloadLike>,
    options?: ReplyOptions & { trailer?: PayloadLike; creditTimeout?: number | undefined },
  ): Promise<void>;
  /** @throws {RangeError} for OK. */
  replyAbort(status: Status): Promise<void>;
  extend(options?: { deadline?: number; renew?: number; buffer?: number }): Promise<void>;
  /** Per-request responder timeout: undefined = channel default, 0 = none. */
  setReplyTimeout(timeout: number | undefined): void;
}

/** A Talk channel over a mux channel (or any frame channel without flow control). */
export interface TalkChannel extends AsyncDisposable {
  readonly options: TalkOptions;
  /** The mux channel underneath, when there is one. */
  readonly raw: MuxChannel | undefined;
  /** Incoming plain messages and requests (not responses). A channel abort from the peer ends it with a TalkAbortError. */
  readonly messages: AsyncIterable<TalkMessage>;
  /** Iterates `messages`, calling [handler] for each (not awaited between messages); resolves when the channel ends. */
  serve(handler: (message: TalkMessage) => unknown): Promise<void>;
  send(procedure: NameLike, payload?: PayloadLike, options?: SendOptions): Promise<void>;
  request(
    procedure: NameLike,
    payload?: PayloadLike,
    options?: RequestOptions,
  ): Promise<TalkMessage>;
  startRequest(procedure: NameLike, payload?: PayloadLike, options?: RequestOptions): TalkRequest;
  streamRequest(procedure: NameLike, payload?: PayloadLike, options?: RequestOptions): TalkStream;
  /** Channel abort, then close. @throws {RangeError} for OK. */
  abort(status: Status): void;
  readonly isWritable: boolean;
  readonly whenWritable: Promise<void>;
  close(status?: Status): Promise<void>;
  readonly done: Promise<Status>;
  readonly isOpen: boolean;
  readonly outgoingRequestCount: number;
  readonly incomingRequestCount: number;
}
