/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// A pair of in-memory Talk channels for the naming tests, standing in for
// the Talk thread's `TalkChannel` until it lands (the port of the Dart
// harness's `TalkChannel` over a `StreamChannelController`). It implements
// what the naming client and service use of `talk/contract.ts`, with the
// Dart semantics: requests and replies, stream items, aborts and cancels,
// requester and responder timeouts, EXTEND, `ordered` answers, the request
// limits, close and channel abort. Frames are objects, not bytes; bulk
// payloads and replies that are requests are not supported.

import { type PayloadLike, toPayload, utf8Decode } from '../../../src/core/bytes.ts';
import type { MuxChannel } from '../../../src/core/mux/contract.ts';
import { type Name, type NameLike, toName } from '../../../src/core/name.ts';
import { Status, StatusCode, SwitchboardError } from '../../../src/core/status.ts';
import {
  type ReplyOptions,
  type RequestOptions,
  type SendOptions,
  type TalkChannel,
  type TalkMessage,
  type TalkOptions,
  type TalkRequest,
  type TalkStream,
  talkOptions,
} from '../../../src/core/talk/contract.ts';
import { type Clock, systemClock, type TimerHandle } from '../../../src/core/util/clock.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';

/** The peer sent ABORT (the Talk thread's `TalkAbortError`). */
export class FakeTalkAbortError extends SwitchboardError {
  readonly isChannelAbort: boolean;

  constructor(status: Status, isChannelAbort: boolean) {
    super(status);
    this.name = 'TalkAbortError';
    this.isChannelAbort = isChannelAbort;
  }
}

/** A frame on the in-memory link. */
export type FakeFrame =
  | {
      readonly kind: 'message';
      readonly procedure: Name;
      readonly payload: Uint8Array;
      readonly requestId: number;
      readonly responseId: number;
      readonly expectsStream: boolean;
      readonly item: boolean;
    }
  | {
      /** responseId set: an abort response; requestId only: a cancel; neither: a channel abort. */
      readonly kind: 'abort';
      readonly requestId: number;
      readonly responseId: number;
      readonly status: Status;
    }
  | {
      readonly kind: 'extend';
      readonly responseId: number;
      readonly deadline: number | undefined;
      readonly renew: number | undefined;
    }
  | { readonly kind: 'close'; readonly status: Status };

/** What a link does with the frames of one direction. */
export type FrameFilter = (frame: FakeFrame) => boolean;

/** Options of {@link fakeTalkPair}. */
export interface FakeTalkPairOptions {
  /** Options of the first channel. */
  readonly a?: Partial<TalkOptions>;
  /** Options of the second channel. */
  readonly b?: Partial<TalkOptions>;
  /** Deliver frames synchronously (one burst per step, as the mux delivers a batch). */
  readonly sync?: boolean;
  /** Drops a frame from a to b when it returns true. */
  readonly dropAtoB?: FrameFilter;
  /** Drops a frame from b to a when it returns true. */
  readonly dropBtoA?: FrameFilter;
}

/** Two connected channels. */
export function fakeTalkPair(
  options: FakeTalkPairOptions = {},
): [FakeTalkChannel, FakeTalkChannel] {
  const a = new FakeTalkChannel(talkOptions(options.a), options.sync ?? false);
  const b = new FakeTalkChannel(talkOptions(options.b), options.sync ?? false);
  a.connect(b, options.dropAtoB);
  b.connect(a, options.dropBtoA);
  return [a, b];
}

interface Outgoing {
  readonly id: number;
  readonly response: Deferred<TalkMessage>;
  readonly ordered: boolean;
  readonly gap: number;
  readonly onExtend: RequestOptions['onExtend'];
  readonly items: ItemQueue | undefined;
  timer: TimerHandle | undefined;
  deadline: number | undefined;
  unlisten: (() => void) | undefined;
}

type LaneEntry = { message: FakeMessage } | { outcome: () => void };

/** The incoming messages: one consumer, answers to `ordered` requests in their place. */
class MessageLane implements AsyncIterable<TalkMessage> {
  readonly #entries: LaneEntry[] = [];
  #waiting: Deferred<IteratorResult<TalkMessage>> | undefined;
  #listening = false;
  #cancelled = false;
  #ended = false;
  #error: unknown;
  readonly #onCancel: (messages: FakeMessage[]) => void;

  constructor(onCancel: (messages: FakeMessage[]) => void) {
    this.#onCancel = onCancel;
  }

  /** Whether nobody listens, or nobody will. */
  get deaf(): boolean {
    return !this.#listening || this.#cancelled;
  }

  get isEnded(): boolean {
    return this.#ended;
  }

  push(message: FakeMessage): void {
    if (this.#ended) {
      return;
    }
    const waiting = this.#waiting;
    if (waiting !== undefined && this.#entries.length === 0) {
      this.#waiting = undefined;
      waiting.resolve({ value: message, done: false });
      return;
    }
    this.#entries.push({ message });
  }

  /** Runs [outcome] once everything ahead of it has been handed out. */
  pushOutcome(outcome: () => void): void {
    if (this.deaf || this.#entries.length === 0) {
      outcome();
      return;
    }
    this.#entries.push({ outcome });
  }

  end(error?: unknown): void {
    if (this.#ended) {
      return;
    }
    this.#ended = true;
    this.#error = error;
    this.#settleWaiting();
  }

  #settleWaiting(): void {
    const waiting = this.#waiting;
    if (waiting === undefined || this.#entries.length > 0) {
      return;
    }
    this.#waiting = undefined;
    if (this.#error !== undefined) {
      waiting.reject(this.#error);
    } else {
      waiting.resolve({ value: undefined, done: true });
    }
  }

  #runOutcomes(): void {
    while (this.#entries.length > 0) {
      const head = this.#entries[0] as LaneEntry;
      if (!('outcome' in head)) {
        return;
      }
      this.#entries.shift();
      head.outcome();
    }
  }

  next(): Promise<IteratorResult<TalkMessage>> {
    this.#listening = true;
    this.#runOutcomes();
    const head = this.#entries.shift();
    if (head !== undefined && 'message' in head) {
      // The answers right behind it are delivered after it, before the next message.
      this.#runOutcomes();
      return Promise.resolve({ value: head.message, done: false });
    }
    if (this.#ended) {
      return this.#error !== undefined
        ? Promise.reject(this.#error)
        : Promise.resolve({ value: undefined, done: true });
    }
    if (this.#waiting !== undefined) {
      return Promise.reject(new Error('messages has one consumer'));
    }
    this.#waiting = new Deferred();
    return this.#waiting.promise;
  }

  cancel(): void {
    if (this.#cancelled) {
      return;
    }
    this.#cancelled = true;
    const entries = this.#entries.splice(0);
    const dropped: FakeMessage[] = [];
    for (const entry of entries) {
      if ('outcome' in entry) {
        entry.outcome();
      } else {
        dropped.push(entry.message);
      }
    }
    this.#ended = true;
    this.#settleWaiting();
    this.#onCancel(dropped);
  }

  [Symbol.asyncIterator](): AsyncIterator<TalkMessage> {
    if (this.#listening) {
      throw new Error('messages already listened to');
    }
    this.#listening = true;
    return {
      next: () => this.next(),
      return: () => {
        this.cancel();
        return Promise.resolve({ value: undefined, done: true });
      },
    };
  }
}

/** Stream items of an outgoing stream request. */
class ItemQueue implements AsyncIterable<TalkMessage> {
  readonly #buffer: TalkMessage[] = [];
  #waiting: Deferred<IteratorResult<TalkMessage>> | undefined;
  #ended = false;
  #error: unknown;
  readonly #onStop: () => void;

  constructor(onStop: () => void) {
    this.#onStop = onStop;
  }

  push(item: TalkMessage): void {
    if (this.#ended) {
      return;
    }
    const waiting = this.#waiting;
    if (waiting !== undefined) {
      this.#waiting = undefined;
      waiting.resolve({ value: item, done: false });
    } else {
      this.#buffer.push(item);
    }
  }

  end(error?: unknown): void {
    if (this.#ended) {
      return;
    }
    this.#ended = true;
    this.#error = error;
    const waiting = this.#waiting;
    if (waiting !== undefined) {
      this.#waiting = undefined;
      if (error !== undefined) waiting.reject(error);
      else waiting.resolve({ value: undefined, done: true });
    }
  }

  [Symbol.asyncIterator](): AsyncIterator<TalkMessage> {
    return {
      next: () => {
        const value = this.#buffer.shift();
        if (value !== undefined) {
          return Promise.resolve({ value, done: false });
        }
        if (this.#ended) {
          return this.#error !== undefined
            ? Promise.reject(this.#error)
            : Promise.resolve({ value: undefined, done: true });
        }
        this.#waiting = new Deferred();
        return this.#waiting.promise;
      },
      return: () => {
        if (!this.#ended) {
          this.#onStop();
        }
        return Promise.resolve({ value: undefined, done: true });
      },
    };
  }
}

const textDecoder = utf8Decode;

/** An incoming message of a {@link FakeTalkChannel}. */
export class FakeMessage implements TalkMessage {
  readonly channel: FakeTalkChannel;
  readonly procedure: Name;
  readonly payload: Uint8Array;
  readonly requestId: number;
  readonly responseId: number;
  readonly expectsStream: boolean;
  readonly isBulk = false;
  readonly bulkLength = undefined;
  readonly #cancelled = new Deferred<void>();
  readonly #abort = new AbortController();
  #finished = false;
  #timer: TimerHandle | undefined;

  constructor(
    channel: FakeTalkChannel,
    frame: Extract<FakeFrame, { kind: 'message' }>,
    responseTo?: number,
  ) {
    this.channel = channel;
    this.procedure = frame.procedure;
    this.payload = frame.payload;
    this.requestId = frame.requestId;
    this.responseId = responseTo ?? frame.responseId;
    this.expectsStream = frame.expectsStream;
  }

  get procedureName(): string {
    return this.procedure.toString();
  }

  text(): string {
    return textDecoder(this.payload);
  }

  json<T = unknown>(): T {
    return JSON.parse(this.text()) as T;
  }

  bytes(): Promise<Uint8Array> {
    return Promise.resolve(this.payload);
  }

  get bulk(): ReadableStream<Uint8Array> {
    const payload = this.payload;
    return new ReadableStream({
      start(controller) {
        controller.enqueue(payload);
        controller.close();
      },
    });
  }

  get expectsReply(): boolean {
    return this.requestId !== 0;
  }

  get isCancelled(): boolean {
    return this.#cancelled.isSettled;
  }

  get onCancel(): Promise<void> {
    return this.#cancelled.promise;
  }

  get signal(): AbortSignal {
    return this.#abort.signal;
  }

  get canReply(): boolean {
    return this.expectsReply && !this.#finished && !this.isCancelled && this.channel.isOpen;
  }

  /** Arms the responder timeout (channel default) on arrival. */
  start(): void {
    this.#arm(this.channel.options.replyTimeout);
  }

  #arm(ms: number): void {
    const clock = this.channel.clock;
    if (this.#timer !== undefined) {
      clock.clearTimeout(this.#timer);
      this.#timer = undefined;
    }
    if (ms <= 0 || !this.canReply) {
      return;
    }
    this.#timer = clock.setTimeout(() => {
      this.#timer = undefined;
      if (this.canReply) {
        this.#finish();
        this.channel.write({
          kind: 'abort',
          requestId: 0,
          responseId: this.requestId,
          status: Status.of(StatusCode.deadlineExceeded, 'responder timeout'),
        });
        this.cancelled();
      }
    }, ms);
  }

  #finish(): void {
    this.#finished = true;
    if (this.#timer !== undefined) {
      this.channel.clock.clearTimeout(this.#timer);
      this.#timer = undefined;
    }
    this.channel.forgetIncoming(this);
  }

  /** The request is cancelled (by the peer, a timeout or the end of the channel). */
  cancelled(): void {
    if (this.#timer !== undefined) {
      this.channel.clock.clearTimeout(this.#timer);
      this.#timer = undefined;
    }
    if (this.#cancelled.resolve()) {
      this.#abort.abort(SwitchboardError.of(StatusCode.cancelled, 'request cancelled'));
    }
  }

  #check(stream = false): void {
    if (!this.canReply) {
      throw SwitchboardError.of(StatusCode.failedPrecondition, 'cannot reply');
    }
    if (stream && !this.expectsStream) {
      throw SwitchboardError.of(StatusCode.failedPrecondition, 'not a stream request');
    }
  }

  reply(payload?: PayloadLike, options?: ReplyOptions): Promise<void> {
    this.#check();
    this.#finish();
    this.channel.write({
      kind: 'message',
      procedure: toName(options?.procedure ?? ''),
      payload: toPayload(payload),
      requestId: 0,
      responseId: this.requestId,
      expectsStream: false,
      item: false,
    });
    return Promise.resolve();
  }

  replyItem(payload?: PayloadLike, options?: ReplyOptions): Promise<void> {
    this.#check(true);
    this.channel.write({
      kind: 'message',
      procedure: toName(options?.procedure ?? ''),
      payload: toPayload(payload),
      requestId: 0,
      responseId: this.requestId,
      expectsStream: false,
      item: true,
    });
    return Promise.resolve();
  }

  async replyStream(
    items: AsyncIterable<PayloadLike> | Iterable<PayloadLike>,
    options?: ReplyOptions & { trailer?: PayloadLike },
  ): Promise<void> {
    try {
      for await (const item of items) {
        await this.replyItem(item, options);
      }
      await this.reply(options?.trailer, options);
    } catch (error) {
      if (this.canReply) {
        await this.replyAbort(
          error instanceof SwitchboardError && !error.status.isOk
            ? error.status
            : Status.of(StatusCode.unknown, String(error)),
        );
      }
    }
  }

  replyAbort(status: Status): Promise<void> {
    if (status.isOk) {
      throw new RangeError('an abort needs a status other than OK');
    }
    this.#check();
    this.#finish();
    this.channel.write({ kind: 'abort', requestId: 0, responseId: this.requestId, status });
    return Promise.resolve();
  }

  extend(options: { deadline?: number; renew?: number; buffer?: number } = {}): Promise<void> {
    this.#check();
    const { deadline, renew } = options;
    const buffer = options.buffer ?? this.channel.options.extendBuffer;
    this.channel.write({
      kind: 'extend',
      responseId: this.requestId,
      deadline: deadline === undefined ? undefined : deadline + buffer,
      renew: renew === undefined ? undefined : renew + buffer,
    });
    if (deadline === undefined && renew === undefined) {
      this.#arm(this.channel.options.replyTimeout);
    } else {
      this.#arm(Math.max(1, Math.min(deadline ?? Infinity, renew ?? Infinity)));
    }
    return Promise.resolve();
  }

  setReplyTimeout(timeout: number | undefined): void {
    this.#arm(timeout ?? this.channel.options.replyTimeout);
  }

  replyRequest(): Promise<TalkMessage> {
    throw new Error('not supported by the fake Talk channel');
  }

  startReplyRequest(): TalkRequest {
    throw new Error('not supported by the fake Talk channel');
  }

  replyStreamRequest(): TalkStream {
    throw new Error('not supported by the fake Talk channel');
  }

  replyItemRequest(): Promise<TalkMessage> {
    throw new Error('not supported by the fake Talk channel');
  }

  startReplyItemRequest(): TalkRequest {
    throw new Error('not supported by the fake Talk channel');
  }

  replyItemStreamRequest(): TalkStream {
    throw new Error('not supported by the fake Talk channel');
  }
}

/** One end of a {@link fakeTalkPair}. */
export class FakeTalkChannel implements TalkChannel {
  readonly options: TalkOptions;
  readonly raw: MuxChannel | undefined = undefined;
  readonly clock: Clock;
  readonly #sync: boolean;
  #peer: FakeTalkChannel | undefined;
  #drop: FrameFilter | undefined;
  #nextRequestId = 1;
  readonly #outgoing = new Map<number, Outgoing>();
  readonly #incoming = new Map<number, FakeMessage>();
  readonly #lane: MessageLane;
  readonly #done = new Deferred<Status>();
  #open = true;

  /** Every frame this side sent, in order (for tests that check the wire). */
  readonly sent: FakeFrame[] = [];

  constructor(options: TalkOptions, sync: boolean) {
    this.options = options;
    this.clock = options.clock ?? systemClock;
    this.#sync = sync;
    this.#lane = new MessageLane((dropped) => {
      for (const message of dropped) {
        if (message.canReply) {
          void message.replyAbort(Status.of(StatusCode.unimplemented, 'no message listener'));
        }
      }
    });
  }

  /** Links this side to [peer]. */
  connect(peer: FakeTalkChannel, drop?: FrameFilter): void {
    this.#peer = peer;
    this.#drop = drop;
  }

  /** Replaces the filter of the frames this side sends. */
  set drop(filter: FrameFilter | undefined) {
    this.#drop = filter;
  }

  get messages(): AsyncIterable<TalkMessage> {
    return this.#lane;
  }

  async serve(handler: (message: TalkMessage) => unknown): Promise<void> {
    try {
      for await (const message of this.#lane) {
        try {
          const result = handler(message);
          if (result instanceof Promise) {
            result.catch(() => {});
          }
        } catch {
          // A handler that throws does not stop the others.
        }
      }
    } catch {
      // A channel abort ends the loop.
    }
  }

  get isOpen(): boolean {
    return this.#open;
  }

  get isWritable(): boolean {
    return this.#open;
  }

  get whenWritable(): Promise<void> {
    return Promise.resolve();
  }

  get done(): Promise<Status> {
    return this.#done.promise;
  }

  get outgoingRequestCount(): number {
    return this.#outgoing.size;
  }

  get incomingRequestCount(): number {
    return this.#incoming.size;
  }

  /** Sends [frame] to the peer (unless the filter drops it). */
  write(frame: FakeFrame): void {
    this.sent.push(frame);
    const peer = this.#peer;
    if (peer === undefined || this.#drop?.(frame) === true) {
      return;
    }
    if (this.#sync) {
      peer.receive(frame);
    } else {
      queueMicrotask(() => peer.receive(frame));
    }
  }

  forgetIncoming(message: FakeMessage): void {
    if (this.#incoming.get(message.requestId) === message) {
      this.#incoming.delete(message.requestId);
    }
  }

  #checkOpen(): void {
    if (!this.#open) {
      throw SwitchboardError.of(StatusCode.unavailable, 'Talk channel closed');
    }
  }

  send(procedure: NameLike, payload?: PayloadLike, options?: SendOptions): Promise<void> {
    this.#checkOpen();
    this.write({
      kind: 'message',
      procedure: options?.name ?? toName(procedure),
      payload: toPayload(payload),
      requestId: 0,
      responseId: 0,
      expectsStream: false,
      item: false,
    });
    return Promise.resolve();
  }

  request(
    procedure: NameLike,
    payload?: PayloadLike,
    options?: RequestOptions,
  ): Promise<TalkMessage> {
    return this.startRequest(procedure, payload, options).response;
  }

  startRequest(procedure: NameLike, payload?: PayloadLike, options?: RequestOptions): TalkRequest {
    const out = this.#start(procedure, payload, options, false);
    const self = this;
    return {
      requestId: out.id,
      response: out.response.promise,
      get deadline() {
        return out.deadline;
      },
      cancel(status?: Status) {
        self.#cancel(out, status);
      },
    };
  }

  streamRequest(procedure: NameLike, payload?: PayloadLike, options?: RequestOptions): TalkStream {
    const out = this.#start(procedure, payload, options, true);
    const self = this;
    const items = out.items as ItemQueue;
    return {
      requestId: out.id,
      get deadline() {
        return out.deadline;
      },
      items,
      done: out.response.promise,
      cancel(status?: Status) {
        self.#cancel(out, status);
      },
      [Symbol.asyncIterator]() {
        return items[Symbol.asyncIterator]();
      },
    };
  }

  #start(
    procedure: NameLike,
    payload: PayloadLike,
    options: RequestOptions | undefined,
    stream: boolean,
  ): Outgoing {
    this.#checkOpen();
    if (this.#outgoing.size >= this.options.maxOutgoingRequests) {
      throw SwitchboardError.of(StatusCode.resourceExhausted, 'too many outgoing requests');
    }
    const id = this.#nextRequestId++;
    const out: Outgoing = {
      id,
      response: new Deferred(),
      ordered: options?.ordered ?? false,
      gap: options?.timeout ?? this.options.requestTimeout,
      onExtend: options?.onExtend,
      items: undefined,
      timer: undefined,
      deadline: undefined,
      unlisten: undefined,
    };
    if (stream) {
      (out as { items: ItemQueue | undefined }).items = new ItemQueue(() => this.#cancel(out));
    }
    this.#outgoing.set(id, out);
    this.#arm(out, out.gap);
    const signal = options?.signal;
    if (signal !== undefined) {
      if (signal.aborted) {
        queueMicrotask(() => this.#cancel(out));
      } else {
        const onAbort = (): void => this.#cancel(out);
        signal.addEventListener('abort', onAbort, { once: true });
        out.unlisten = () => signal.removeEventListener('abort', onAbort);
      }
    }
    this.write({
      kind: 'message',
      procedure: options?.name ?? toName(procedure),
      payload: toPayload(payload),
      requestId: id,
      responseId: 0,
      expectsStream: stream,
      item: false,
    });
    return out;
  }

  #arm(out: Outgoing, ms: number): void {
    if (out.timer !== undefined) {
      this.clock.clearTimeout(out.timer);
      out.timer = undefined;
    }
    out.deadline = undefined;
    if (ms <= 0) {
      return;
    }
    out.deadline = this.clock.now() + ms;
    out.timer = this.clock.setTimeout(() => {
      out.timer = undefined;
      if (this.#outgoing.get(out.id) !== out) {
        return;
      }
      const status = Status.of(StatusCode.deadlineExceeded, 'request timed out');
      this.#end(out);
      this.write({ kind: 'abort', requestId: out.id, responseId: 0, status });
      this.#fail(out, new SwitchboardError(status));
    }, ms);
  }

  #end(out: Outgoing): void {
    this.#outgoing.delete(out.id);
    if (out.timer !== undefined) {
      this.clock.clearTimeout(out.timer);
      out.timer = undefined;
    }
    out.deadline = undefined;
    out.unlisten?.();
  }

  #fail(out: Outgoing, error: unknown): void {
    out.items?.end(error);
    out.response.reject(error);
  }

  #cancel(out: Outgoing, status?: Status): void {
    if (this.#outgoing.get(out.id) !== out) {
      return;
    }
    const s = status ?? Status.of(StatusCode.cancelled, 'request cancelled');
    this.#end(out);
    if (this.#open) {
      this.write({ kind: 'abort', requestId: out.id, responseId: 0, status: s });
    }
    this.#fail(out, new SwitchboardError(s));
  }

  /** A frame from the peer. */
  receive(frame: FakeFrame): void {
    if (!this.#open) {
      return;
    }
    switch (frame.kind) {
      case 'message':
        if (frame.responseId !== 0) {
          this.#onResponse(frame);
        } else {
          this.#onMessage(frame);
        }
        return;
      case 'abort':
        if (frame.responseId !== 0) {
          this.#onAbortResponse(frame.responseId, frame.status);
        } else if (frame.requestId !== 0) {
          const message = this.#incoming.get(frame.requestId);
          if (message !== undefined) {
            this.#incoming.delete(frame.requestId);
            message.cancelled();
          }
        } else {
          this.#lost(frame.status, new FakeTalkAbortError(frame.status, true));
        }
        return;
      case 'extend':
        this.#onExtend(frame);
        return;
      case 'close':
        this.#lost(frame.status, undefined);
        return;
    }
  }

  #onMessage(frame: Extract<FakeFrame, { kind: 'message' }>): void {
    const message = new FakeMessage(this, frame);
    if (frame.requestId !== 0) {
      if (this.#incoming.size >= this.options.maxIncomingRequests) {
        this.write({
          kind: 'abort',
          requestId: 0,
          responseId: frame.requestId,
          status: Status.of(StatusCode.resourceExhausted, 'too many incoming requests'),
        });
        return;
      }
      this.#incoming.get(frame.requestId)?.cancelled();
      this.#incoming.set(frame.requestId, message);
      message.start();
      if (this.#lane.deaf && this.#lane.isEnded) {
        void message.replyAbort(Status.of(StatusCode.unimplemented, 'no message listener'));
        return;
      }
    }
    this.#lane.push(message);
  }

  #onResponse(frame: Extract<FakeFrame, { kind: 'message' }>): void {
    const out = this.#outgoing.get(frame.responseId);
    if (out === undefined) {
      return;
    }
    const message = new FakeMessage(this, frame);
    if (frame.item) {
      if (out.items !== undefined) {
        this.#arm(out, out.gap);
        out.items.push(message);
      }
      return;
    }
    this.#end(out);
    const deliver = (): void => {
      out.items?.end();
      out.response.resolve(message);
    };
    if (out.ordered) {
      this.#lane.pushOutcome(deliver);
    } else {
      deliver();
    }
  }

  #onAbortResponse(id: number, status: Status): void {
    const out = this.#outgoing.get(id);
    if (out === undefined) {
      return;
    }
    this.#end(out);
    const error = new FakeTalkAbortError(status, false);
    if (out.ordered) {
      this.#lane.pushOutcome(() => this.#fail(out, error));
    } else {
      this.#fail(out, error);
    }
  }

  #onExtend(frame: Extract<FakeFrame, { kind: 'extend' }>): void {
    const out = this.#outgoing.get(frame.responseId);
    if (out === undefined) {
      return;
    }
    const extension: { deadline?: number; renew?: number } = {};
    if (frame.deadline !== undefined && frame.deadline > 0) extension.deadline = frame.deadline;
    if (frame.renew !== undefined && frame.renew > 0) extension.renew = frame.renew;
    try {
      out.onExtend?.(extension);
    } catch {
      // Logged by the real channel.
    }
    if (extension.deadline === undefined && extension.renew === undefined) {
      this.#arm(out, out.gap);
      return;
    }
    const clamp = (ms: number): number => {
      let value = ms;
      if (this.options.minExtension > 0) value = Math.max(value, this.options.minExtension);
      if (this.options.maxExtension > 0) value = Math.min(value, this.options.maxExtension);
      return value;
    };
    this.#arm(
      out,
      Math.min(
        extension.deadline === undefined ? Infinity : clamp(extension.deadline),
        extension.renew === undefined ? Infinity : clamp(extension.renew),
      ),
    );
  }

  /** The channel ended: from the peer (close or channel abort) or locally. */
  #lost(status: Status, error: unknown): void {
    if (!this.#open) {
      return;
    }
    this.#open = false;
    const failure = SwitchboardError.of(
      StatusCode.unavailable,
      `Talk channel closed${status.isOk ? '' : `: ${status}`}`,
    );
    for (const out of [...this.#outgoing.values()]) {
      this.#end(out);
      this.#fail(out, failure);
    }
    for (const message of [...this.#incoming.values()]) {
      message.cancelled();
    }
    this.#incoming.clear();
    this.#lane.end(error);
    this.#done.resolve(status);
  }

  abort(status: Status): void {
    if (status.isOk) {
      throw new RangeError('an abort needs a status other than OK');
    }
    if (!this.#open) {
      return;
    }
    this.write({ kind: 'abort', requestId: 0, responseId: 0, status });
    this.write({ kind: 'close', status });
    this.#lost(status, undefined);
  }

  close(status: Status = Status.ok): Promise<void> {
    if (this.#open) {
      this.write({ kind: 'close', status });
      this.#lost(status, undefined);
    }
    return Promise.resolve();
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }
}
