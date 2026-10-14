/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * Bulk payloads in transfer (wiki page "Polyverse Switchboard Talk",
 * section "Bulk payloads"): the payloads this side sends on bulk channels
 * of its own, the payloads the peer sends, and the lanes that keep
 * deliveries in order while payloads arrive.
 *
 * @internal
 */

import type { MuxChannel } from '../mux/contract.ts';
import { Status, StatusCode, SwitchboardError } from '../status.ts';
import type { TimerHandle } from '../util/clock.ts';
import { Deferred } from '../util/deferred.ts';
import type { BulkSource } from './contract.ts';
import type { TalkChannel } from './talk-channel.ts';
import type { IncomingMessage } from './talk-message.ts';
import { maxTimerDelay, noop } from './talk-util.ts';

/** What the payload of a bulk message is sent from: a byte source, bytes in memory, or the bulk channel of a message being forwarded. */
export class BulkPayload {
  readonly source: BulkSource | undefined;
  readonly bytes: Uint8Array | undefined;
  readonly from: BulkIn | undefined;

  /** The declared length; undefined when not known in advance. */
  readonly length: number | undefined;

  private constructor(
    source: BulkSource | undefined,
    bytes: Uint8Array | undefined,
    from: BulkIn | undefined,
    length: number | undefined,
  ) {
    this.source = source;
    this.bytes = bytes;
    this.from = from;
    this.length = length;
  }

  static stream(source: BulkSource, length: number | undefined): BulkPayload {
    return new BulkPayload(source, undefined, undefined, length);
  }

  static fromBytes(bytes: Uint8Array): BulkPayload {
    return new BulkPayload(undefined, bytes, undefined, bytes.length);
  }

  static pipe(from: BulkIn): BulkPayload {
    return new BulkPayload(undefined, undefined, from, from.length);
  }
}

/** The status a bulk channel is closed with for [status]: a mux CLOSE carries neither application codes nor OK for a failure. */
export function bulkCloseStatus(status: Status): Status {
  if (status.isOk) {
    return Status.of(StatusCode.internal, 'bulk payload failed');
  }
  if (status.isApplicationCode) {
    const reason = status.reason.length === 0 ? '' : `: ${status.reason}`;
    return Status.of(StatusCode.unknown, `application status ${status.code}${reason}`);
  }
  return status;
}

/** The status of an error a bulk source raised. */
function sourceStatus(talk: TalkChannel, error: unknown): Status {
  if (error instanceof SwitchboardError) {
    return error.status.isOk ? Status.of(StatusCode.internal, 'bulk source failed') : error.status;
  }
  talk._log('warn', 'bulk source failed', error);
  return Status.of(StatusCode.internal, 'bulk source failed');
}

/** A chunk of a bulk source as bytes: a `Uint8Array` as is, another view or buffer wrapped, an array of numbers copied. */
function toChunk(value: unknown): Uint8Array {
  if (value instanceof Uint8Array) {
    return value;
  }
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }
  if (value instanceof ArrayBuffer) {
    return new Uint8Array(value);
  }
  if (Array.isArray(value)) {
    return Uint8Array.from(value as number[]);
  }
  throw new TypeError(`bulk source chunk is not bytes: ${typeof value}`);
}

/** An async iterator over [source], whose `return()` cancels it. */
function sourceIterator(source: BulkSource): AsyncIterator<unknown> {
  const stream = source as ReadableStream<Uint8Array>;
  if (typeof stream.getReader === 'function') {
    const reader = stream.getReader();
    return {
      next: () => reader.read() as Promise<IteratorResult<unknown>>,
      return: async () => {
        await reader.cancel();
        return { value: undefined, done: true };
      },
    };
  }
  const asyncIterable = source as AsyncIterable<unknown>;
  if (typeof asyncIterable[Symbol.asyncIterator] === 'function') {
    return asyncIterable[Symbol.asyncIterator]();
  }
  const iterator = (source as Iterable<unknown>)[Symbol.iterator]();
  return {
    next: async () => iterator.next(),
    return: async () => {
      iterator.return?.();
      return { value: undefined, done: true };
    },
  };
}

/** Stops [iterator], logging a failure. */
function stopIterator(talk: TalkChannel, iterator: AsyncIterator<unknown>): void {
  const stop = iterator.return;
  if (stop === undefined) {
    return;
  }
  try {
    stop
      .call(iterator)
      .then(noop, (e: unknown) => talk._log('debug', 'cancelling a bulk source failed', e));
  } catch (e) {
    talk._log('debug', 'cancelling a bulk source failed', e);
  }
}

/** One bulk payload this side sends: the bulk channel, and what feeds it. */
export class BulkOut {
  readonly talk: TalkChannel;

  /** The bulk channel, opened in order with the parent's frames. */
  readonly channel: MuxChannel;

  /** The bulk number the reference carries. */
  readonly number: number;

  /** The declared length, undefined when unknown. */
  readonly length: number | undefined;

  /** Called each time a chunk was handed to the bulk channel (its window took it): the transfer progresses. */
  onProgress: (() => void) | undefined;

  readonly #done = new Deferred<void>();
  #stopped = false;
  #iterator: AsyncIterator<unknown> | undefined;
  #owner: Set<BulkOut> | undefined;
  #from: BulkIn | undefined;

  constructor(talk: TalkChannel, channel: MuxChannel, number: number, length: number | undefined) {
    this.talk = talk;
    this.channel = channel;
    this.number = number;
    this.length = length;
  }

  /**
   * Resolves once the payload was sent whole and the bulk channel closed;
   * rejects with the status the transfer ended with otherwise. Never
   * reported as unhandled.
   */
  get done(): Promise<void> {
    return this.#done.promise;
  }

  /** The request keeps its active sends for cancellation, not its history. */
  track(owner: Set<BulkOut>): void {
    if (!this.#done.isSettled) {
      this.#owner = owner;
      owner.add(this);
    }
  }

  /** Starts sending from [source]. */
  start(source: BulkPayload): void {
    this.talk._bulkOuts.add(this);
    void this.channel.done.then((status) => this.#channelDone(status));
    if (source.bytes !== undefined) {
      void this.#pump(this.#pieces(source.bytes));
    } else if (source.source !== undefined) {
      let iterator: AsyncIterator<unknown>;
      try {
        iterator = sourceIterator(source.source);
      } catch (e) {
        this.#finish(sourceStatus(this.talk, e));
        return;
      }
      void this.#pump(iterator);
    } else if (source.from !== undefined) {
      this.#pipeFrom(source.from);
    }
  }

  /**
   * Stops the transfer: closes the bulk channel with [status] (the source,
   * if any, is cancelled; a forwarded payload's own bulk channel is closed
   * with it too).
   */
  cancel(status: Status): void {
    if (this.#stopped) {
      return;
    }
    this.#stopped = true;
    this.#stopSource();
    this.#from?.close(status);
    void this.channel.close(bulkCloseStatus(status));
  }

  #progress(): void {
    const callback = this.onProgress;
    if (callback !== undefined) {
      try {
        callback();
      } catch (e) {
        this.talk._log('error', 'bulk progress callback failed', e);
      }
    }
  }

  #stopSource(): void {
    const iterator = this.#iterator;
    this.#iterator = undefined;
    if (iterator !== undefined) {
      stopIterator(this.talk, iterator);
    }
    this.#from?._detach();
  }

  /** The payload went whole ([failure] undefined) or failed: closes the bulk channel accordingly. */
  #finish(failure: Status | undefined): void {
    if (this.#stopped) {
      return;
    }
    this.#stopped = true;
    this.#stopSource();
    if (failure !== undefined) {
      this.talk._log('debug', `bulk payload ${this.number} failed: ${failure}`);
      this.#from?.close(failure);
    }
    void this.channel.close(failure === undefined ? Status.ok : bulkCloseStatus(failure));
  }

  #channelDone(status: Status): void {
    this.talk._bulkOuts.delete(this);
    this.#owner?.delete(this);
    this.#owner = undefined;
    if (!this.#stopped) {
      // The receiver closed it (it does not want the rest), or the
      // connection ended.
      this.#stopped = true;
      this.#stopSource();
      this.#from?.close(status);
    }
    if (status.isOk) {
      this.#done.resolve();
    } else {
      this.#done.reject(new SwitchboardError(status));
    }
  }

  /** [bytes] as views of `bulkChunkSize` bytes, without a copy. */
  #pieces(bytes: Uint8Array): AsyncIterator<unknown> {
    const chunk = this.talk.options.bulkChunkSize;
    let offset = 0;
    return {
      next: async () => {
        if (offset >= bytes.length) {
          return { value: undefined, done: true };
        }
        const end = Math.min(offset + chunk, bytes.length);
        const piece = bytes.subarray(offset, end);
        offset = end;
        return { value: piece, done: false };
      },
      return: async () => {
        offset = bytes.length;
        return { value: undefined, done: true };
      },
    };
  }

  async #pump(iterator: AsyncIterator<unknown>): Promise<void> {
    this.#iterator = iterator;
    const chunk = this.talk.options.bulkChunkSize;
    const parts: Uint8Array[] = [];
    let held = 0;
    let total = 0;
    let failure: Status | undefined;
    try {
      while (!this.#stopped) {
        const next = await iterator.next();
        if (next.done === true || this.#stopped) {
          break;
        }
        const bytes = toChunk(next.value);
        total += bytes.length;
        const declared = this.length;
        if (declared !== undefined && total > declared) {
          failure = Status.of(
            StatusCode.invalidArgument,
            `the bulk source produced more than the ${declared} bytes declared`,
          );
          break;
        }
        if (bytes.length > 0) {
          parts.push(bytes);
          held += bytes.length;
        }
        while (held >= chunk && !this.#stopped) {
          const piece = takeBytes(parts, chunk);
          held -= piece.length;
          await this.channel.send(piece);
          this.#progress();
        }
      }
      if (!this.#stopped && failure === undefined) {
        const declared = this.length;
        if (declared !== undefined && total !== declared) {
          failure = Status.of(
            StatusCode.invalidArgument,
            `the bulk source produced ${total} bytes, ${declared} declared`,
          );
        } else {
          while (held > 0 && !this.#stopped) {
            const piece = takeBytes(parts, held < chunk ? held : chunk);
            held -= piece.length;
            await this.channel.send(piece);
            this.#progress();
          }
        }
      }
    } catch (e) {
      // The bulk channel stopped (the receiver closed it, the connection
      // ended) or refused a chunk, or the source failed.
      if (!this.#stopped) {
        failure = e instanceof SwitchboardError ? e.status : sourceStatus(this.talk, e);
      }
    }
    if (this.#iterator === iterator) {
      this.#iterator = undefined;
      stopIterator(this.talk, iterator);
    }
    this.#finish(failure);
  }

  /**
   * Forwards the bytes of [from], a bulk channel of a message being
   * forwarded, with credit passed through: what [from] received counts as
   * consumed once this channel's send took it.
   */
  #pipeFrom(from: BulkIn): void {
    this.#from = from;
    from._grant();
    let received = 0;
    // The last send: they complete in order.
    let sending: Promise<void> = Promise.resolve();
    from._attach(
      (data) => {
        const length = data.length;
        received += length;
        const declared = from.length;
        if (declared !== undefined && received > declared) {
          from._consume(length);
          this.#finish(
            Status.of(
              StatusCode.protocolError,
              `bulk payload longer than the ${declared} bytes declared`,
            ),
          );
          return;
        }
        if (this.#stopped || !this.channel.canSend) {
          from._consume(length);
          return;
        }
        try {
          sending = this.channel.send(data).then(
            () => from._consume(length),
            () => from._consume(length),
          );
        } catch (e) {
          from._consume(length);
          this.#finish(
            e instanceof SwitchboardError ? e.status : Status.of(StatusCode.internal, String(e)),
          );
        }
      },
      (status) => {
        const declared = from.length;
        if (!status.isOk) {
          this.#finish(status);
        } else if (declared !== undefined && received !== declared) {
          this.#finish(
            Status.of(
              StatusCode.protocolError,
              `bulk payload of ${received} bytes, ${declared} declared`,
            ),
          );
        } else {
          // The CLOSE would wait behind what still waits for credit, but
          // its confirmation is timed from now: a reader slower than that
          // would see the payload cut. Closed once the window took it all,
          // as a payload sent from a source is.
          void sending.then(() => this.#finish(undefined));
        }
      },
    );
  }
}

/** The first [n] bytes of [parts], removed from them. */
function takeBytes(parts: Uint8Array[], n: number): Uint8Array {
  const first = parts[0] as Uint8Array;
  if (first.length === n) {
    parts.shift();
    return first;
  }
  if (first.length > n) {
    parts[0] = first.subarray(n);
    return first.subarray(0, n);
  }
  const out = new Uint8Array(n);
  let offset = 0;
  while (offset < n) {
    const part = parts.shift() as Uint8Array;
    const take = part.length <= n - offset ? part.length : n - offset;
    out.set(part.subarray(0, take), offset);
    offset += take;
    if (take < part.length) {
      parts.unshift(part.subarray(take));
    }
  }
  return out;
}

/**
 * One bulk payload the peer sends this side: the bulk channel it arrived
 * on, adopted for a parent channel and claimed by the message that
 * references it.
 *
 * Read from adoption on, in manual credit: what arrives before a reader
 * takes it waits here, bounded by the bulk channel's window.
 */
export class BulkIn {
  readonly talk: TalkChannel;

  /** The bulk channel, in manual credit: its subframes are consumed as the reader takes them. */
  readonly channel: MuxChannel;

  /** The bulk number of its OPEN. */
  readonly number: number;

  /** The declared length, once claimed; undefined when unknown. */
  length: number | undefined;

  /** What arrived before a reader was attached. */
  readonly _buffer: Uint8Array[] = [];

  /** Whether something reads it (the application's stream, the reassembly, a forwarding pipe), or it was abandoned. */
  _read = false;

  /** The bulk channel's end status, once known. */
  _endStatus: Status | undefined;

  /** Bounds the wait of an unclaimed bulk channel for its message. */
  unclaimedTimer: TimerHandle | undefined;

  /** Called each time the reader took a chunk: the transfer progresses. */
  onProgress: (() => void) | undefined;

  /** Its payload waits for the reassembly budget: nothing reads it, and the silence is this side's. */
  waitingForBudget = false;

  #onData: ((data: Uint8Array) => void) | undefined;
  #onDone: ((status: Status) => void) | undefined;

  /** Abandoned, or its reader gone: what arrives is dropped. */
  #dropping = false;

  // The idle timeout (bulkIdleTimeout): one timer while the channel is
  // open and read, checking the last progress when it fires.
  #idleTimer: TimerHandle | undefined;
  #lastProgress = 0;

  /** Why the transfer failed on this side (it went idle), for a reader that attaches later. */
  #failed: Status | undefined;

  constructor(talk: TalkChannel, channel: MuxChannel, number: number) {
    this.talk = talk;
    this.channel = channel;
    this.number = number;
    // Never paused nor cancelled: it ends with the bulk channel, and what
    // arrives for nobody is dropped (and consumed) here.
    void this.#readAll();
    const timeout = talk.options.bulkIdleTimeout;
    if (timeout > 0) {
      this.#lastProgress = talk._clock.now();
      this.#armIdle(timeout);
    }
  }

  async #readAll(): Promise<void> {
    let reader: ReadableStreamDefaultReader<Uint8Array>;
    try {
      reader = this.channel.readable.getReader();
    } catch (e) {
      this.talk._log('warn', `bulk channel ${this.channel.id} cannot be read`, e);
      this.#onEnd();
      return;
    }
    try {
      for (;;) {
        const { value, done } = await reader.read();
        if (done) {
          break;
        }
        this.#onChunk(value);
      }
    } catch (e) {
      this.talk._log('debug', `bulk channel ${this.channel.id} failed`, e);
    }
    this.#onEnd();
  }

  #armIdle(delay: number): void {
    this.#idleTimer = this.talk._clock.setTimeout(
      () => this.#checkIdle(),
      delay > maxTimerDelay ? maxTimerDelay : delay,
    );
  }

  #stopIdle(): void {
    if (this.#idleTimer !== undefined) {
      this.talk._clock.clearTimeout(this.#idleTimer);
      this.#idleTimer = undefined;
    }
  }

  /** A chunk arrived or was consumed: the idle timeout restarts, and that of a channel waiting for its payloads after its end. */
  _progressed(): void {
    const now = this.talk._clock.now();
    if (this.#idleTimer !== undefined) {
      this.#lastProgress = now;
    }
    if (this.talk._lingering !== undefined) {
      this.talk._lingerProgress = now;
    }
  }

  #checkIdle(): void {
    this.#idleTimer = undefined;
    if (this.#dropping || this.channel.state !== 'open') {
      return;
    }
    const timeout = this.talk.options.bulkIdleTimeout;
    const now = this.talk._clock.now();
    if (this.waitingForBudget || this.channel.heldBytes > 0 || this.channel.bufferedBytes > 0) {
      // What arrived waits to be consumed here, or the payload waits for
      // room to be read: the peer may be waiting for this side's credit.
      this.#lastProgress = now;
    }
    const idle = now - this.#lastProgress;
    if (idle < timeout) {
      this.#armIdle(timeout - idle);
      return;
    }
    const status = Status.of(
      StatusCode.deadlineExceeded,
      `no byte of bulk payload ${this.number} within ${timeout} ms`,
    );
    this.talk._log('debug', `bulk channel ${this.channel.id} went idle: ${status}`);
    this.#failed = status;
    this.close(status);
    const onDone = this.#onDone;
    if (onDone !== undefined) {
      // The reader learns of it now, not once the peer confirmed the
      // CLOSE; what still arrives is dropped.
      this._detach();
      onDone(status);
    }
  }

  #onChunk(data: Uint8Array): void {
    this._progressed();
    if (this.#dropping || this.#failed !== undefined) {
      this._consume(data.length);
      return;
    }
    const onData = this.#onData;
    if (onData === undefined) {
      this._buffer.push(data);
      return;
    }
    onData(data);
  }

  #onEnd(): void {
    this.#stopIdle();
    void this.channel.done.then((status) => {
      this._endStatus = status;
      this.talk._bulkIns.delete(this);
      if (!this.#dropping) {
        this.#onDone?.(status);
      }
      this.talk._checkLinger();
    });
  }

  /** Attaches the reader: [onData] gets what arrived so far and every later chunk, [onDone] the bulk channel's end status after the last. */
  _attach(onData: (data: Uint8Array) => void, onDone: (status: Status) => void): void {
    this._read = true;
    const failed = this.#failed;
    if (failed !== undefined) {
      // Went idle before it was read.
      this._detach();
      onDone(failed);
      return;
    }
    this.#onData = onData;
    this.#onDone = onDone;
    while (this._buffer.length > 0 && this.#onData === onData) {
      onData(this._buffer.shift() as Uint8Array);
    }
    const status = this._endStatus;
    if (status !== undefined && this.#onDone === onDone && !this.#dropping) {
      onDone(status);
    }
  }

  /** The reader went away: what arrives from now on is dropped. */
  _detach(): void {
    this.#stopIdle();
    this.#dropping = true;
    this.#onData = undefined;
    this.#onDone = undefined;
    this.#dropBuffer();
  }

  #dropBuffer(): void {
    let bytes = 0;
    const frames = this._buffer.length;
    for (const chunk of this._buffer) {
      bytes += chunk.length;
    }
    this._buffer.length = 0;
    if (frames > 0) {
      try {
        this.channel.consumed(bytes, frames);
      } catch (e) {
        this.talk._log('warn', `bulk credit accounting: ${e}`);
      }
    }
  }

  /**
   * Closes the bulk channel with [status] if it is still open: the
   * receiver does not want the rest, or the message or channel it belongs
   * to ended.
   */
  close(status: Status): void {
    if (this.channel.state === 'open') {
      this.#stopIdle();
      void this.channel.close(bulkCloseStatus(status));
    }
  }

  /** Nobody will read it: closes it CANCELLED if it is still in transfer and drops what it holds. */
  abandon(): void {
    if (this.unclaimedTimer !== undefined) {
      this.talk._clock.clearTimeout(this.unclaimedTimer);
      this.unclaimedTimer = undefined;
    }
    if (this._read) {
      return;
    }
    this._read = true;
    this.talk._bulkIns.delete(this);
    this.close(Status.of(StatusCode.cancelled, 'bulk payload not read'));
    this._detach();
  }

  /** Raises the window to the declared length, up to `bulkWindow`: the reader is ready for more than the initial window. */
  _grant(): void {
    const declared = this.length;
    if (declared === undefined) {
      return;
    }
    const cap = this.talk.options.bulkWindow;
    const want = declared < cap ? declared : cap;
    const extra = want - this.channel.connection.options.initialWindow;
    if (extra > 0) {
      try {
        this.channel.grant(extra);
      } catch (e) {
        this.talk._log('debug', `bulk window not raised: ${e}`);
      }
    }
  }

  /**
   * The payload as a byte stream, once: ends with an error if the
   * transfer failed (the sender's status, PROTOCOL_ERROR for another
   * length than declared, CONNECTION_LOST). Credit for each chunk goes
   * back when the reader takes it; cancelling the stream closes the bulk
   * channel CANCELLED.
   *
   * @throws {Error} the second time.
   */
  stream(): ReadableStream<Uint8Array> {
    if (this._read) {
      throw new Error('the bulk payload is read already');
    }
    this._read = true;
    let received = 0;
    let finished = false;
    let attached = false;
    let closed = false;
    const queue: Uint8Array[] = [];
    let end: { error: SwitchboardError | undefined } | undefined;
    let wake: Deferred<void> | undefined;
    let controller!: ReadableStreamDefaultController<Uint8Array>;

    const dropQueue = (): void => {
      for (const chunk of queue.splice(0)) {
        this._consume(chunk.length);
      }
    };
    const awaken = (): void => {
      const w = wake;
      wake = undefined;
      w?.resolve();
    };
    const fail = (status: Status): void => {
      finished = true;
      this.close(status);
      this._detach();
      dropQueue();
      end = { error: new SwitchboardError(status) };
      awaken();
    };
    const deliver = (): Promise<void> | undefined => {
      if (closed) {
        return undefined;
      }
      const chunk = queue.shift();
      if (chunk !== undefined) {
        controller.enqueue(chunk);
        this._taken(chunk.length);
        return undefined;
      }
      if (end !== undefined) {
        closed = true;
        if (end.error !== undefined) {
          controller.error(end.error);
        } else {
          controller.close();
        }
        return undefined;
      }
      const w = new Deferred<void>();
      wake = w;
      return w.promise.then(() => deliver());
    };
    const onData = (data: Uint8Array): void => {
      if (closed) {
        this._consume(data.length);
        return;
      }
      received += data.length;
      const declared = this.length;
      if (declared !== undefined && received > declared) {
        this._consume(data.length);
        fail(
          Status.of(
            StatusCode.protocolError,
            `bulk payload longer than the ${declared} bytes declared`,
          ),
        );
        return;
      }
      queue.push(data);
      awaken();
    };
    const onDone = (status: Status): void => {
      finished = true;
      if (closed || end !== undefined) {
        return;
      }
      const declared = this.length;
      let error: SwitchboardError | undefined;
      if (!status.isOk) {
        error = new SwitchboardError(status);
      } else if (declared !== undefined && received !== declared) {
        error = SwitchboardError.of(
          StatusCode.protocolError,
          `bulk payload of ${received} bytes, ${declared} declared`,
        );
      }
      end = { error };
      awaken();
    };
    return new ReadableStream<Uint8Array>(
      {
        start: (c) => {
          controller = c;
        },
        pull: () => {
          if (!attached) {
            attached = true;
            this._grant();
            this._attach(onData, onDone);
          }
          return deliver();
        },
        cancel: () => {
          closed = true;
          if (!finished) {
            this.close(Status.of(StatusCode.cancelled, 'bulk payload not wanted'));
            this._detach();
          }
          dropQueue();
          awaken();
        },
      },
      { highWaterMark: 0 },
    );
  }

  _consume(length: number): void {
    this._progressed();
    try {
      this.channel.consumed(length);
    } catch (e) {
      this.talk._log('warn', `bulk credit accounting: ${e}`);
    }
  }

  /** The reader took a chunk. */
  _taken(length: number): void {
    this._consume(length);
    const callback = this.onProgress;
    if (callback !== undefined) {
      try {
        callback();
      } catch (e) {
        this.talk._log('error', 'bulk progress callback failed', e);
      }
    }
  }

  /**
   * Reads the whole payload, at most [max] bytes: beyond, the bulk channel
   * is closed RESOURCE_EXHAUSTED and the promise rejects with it.
   */
  collect(max: number): Promise<Uint8Array> {
    const result = new Deferred<Uint8Array>();
    const declared = this.length;
    if (declared !== undefined && declared > max) {
      const status = Status.of(
        StatusCode.resourceExhausted,
        `bulk payload of ${declared} bytes, more than ${max}`,
      );
      this._read = true;
      this.talk._bulkIns.delete(this);
      this.close(status);
      this._detach();
      result.reject(new SwitchboardError(status));
      return result.promise;
    }
    const reader = this.stream().getReader();
    void (async () => {
      const parts: Uint8Array[] = [];
      let total = 0;
      try {
        for (;;) {
          const { value, done } = await reader.read();
          if (done) {
            break;
          }
          if (total + value.length > max) {
            const status = Status.of(
              StatusCode.resourceExhausted,
              `bulk payload of more than ${max} bytes`,
            );
            this.close(status);
            reader.cancel().catch(noop);
            result.reject(new SwitchboardError(status));
            return;
          }
          parts.push(value);
          total += value.length;
        }
        const out = parts.length === 1 ? (parts[0] as Uint8Array) : new Uint8Array(total);
        if (parts.length !== 1) {
          let offset = 0;
          for (const part of parts) {
            out.set(part, offset);
            offset += part.length;
          }
        }
        result.resolve(out);
      } catch (e) {
        result.reject(
          e instanceof SwitchboardError ? e : SwitchboardError.of(StatusCode.internal, String(e)),
        );
      }
    })();
    return result.promise;
  }
}

/**
 * A message whose bulk channel has not been adopted yet (its OPEN came
 * first on the wire, but dispatch may lag): delivered, or dropped, when it
 * arrives. Bounded by `bulkOpenTimeout`.
 */
export interface Awaited {
  /** Undefined when the message was dropped: the bulk channel is closed when it arrives. */
  readonly message: IncomingMessage | undefined;
  readonly timer: TimerHandle;
}

/**
 * Deliveries that must keep their order while some wait for a bulk
 * payload: the messages of `TalkChannel.messages`, or what answers one
 * request. Each entry is delivered once it and everything before it is
 * ready.
 */
export class Lane {
  readonly #queue: [IncomingMessage | undefined, () => void][] = [];
  #advancing = false;

  /** Whether deliveries wait here. */
  get isWaiting(): boolean {
    return this.#queue.length > 0;
  }

  /** Delivers [deliver] now if nothing waits and [message] (undefined for a delivery without a payload) is ready, else in its turn. */
  add(message: IncomingMessage | undefined, deliver: () => void): void {
    if (this.#queue.length === 0 && (message === undefined || message._ready)) {
      deliver();
      return;
    }
    if (message !== undefined) {
      message._lane = this;
    }
    this.#queue.push([message, deliver]);
    this.advance();
  }

  /** Delivers what is ready at the head. */
  advance(): void {
    if (this.#advancing) {
      return;
    }
    this.#advancing = true;
    try {
      while (this.#queue.length > 0) {
        const [message, deliver] = this.#queue[0] as [IncomingMessage | undefined, () => void];
        if (message !== undefined && !message._ready) {
          return;
        }
        this.#queue.shift();
        deliver();
      }
    } finally {
      this.#advancing = false;
    }
  }

  /** Delivers what is ready, in order, dropping (with their bulk payloads and credit) the entries that wait. */
  flush(): void {
    const entries = this.#queue.splice(0);
    for (const [message, deliver] of entries) {
      if (message === undefined || message._ready) {
        deliver();
      } else {
        message._drop();
      }
    }
  }

  /** Drops everything waiting, with bulk payloads and credit. */
  drop(): void {
    const entries = this.#queue.splice(0);
    for (const [message] of entries) {
      message?._drop();
    }
  }

  /** Nobody will read incoming messages: keeps ordered answers, which still belong to their requests, but refuses and drops everything else. */
  dropIncoming(status: Status): void {
    const entries = this.#queue.splice(0);
    for (const entry of entries) {
      const message = entry[0];
      if (message !== undefined && !message.frame.hasResponse) {
        message._abortQuietly(status);
        message._drop();
      } else {
        this.#queue.push(entry);
      }
    }
    this.advance();
  }
}
