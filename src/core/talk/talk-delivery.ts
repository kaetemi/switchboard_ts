/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * Delivery of received messages to the application: what the Dart port
 * builds from a stream controller, its guarded subscription and the credit
 * it holds, behind `TalkChannel.messages` and `TalkStream.items`.
 *
 * Delivery is pull based. A consumer iterating with `for await` takes a
 * message when it asks for the next one; `TalkChannel.serve` installs a
 * handler that gets every message as it arrives, one per microtask, as a
 * Dart listener does. Either way a message is handed over through the
 * channel (`TalkChannel._handOver`), which returns the credit of its frame
 * to the peer: taking delivery is what returns flow-control credit.
 *
 * @internal
 */

import { Deferred } from '../util/deferred.ts';
import type { TalkMessage } from './contract.ts';
import type { TalkChannel } from './talk-channel.ts';
import type { IncomingMessage } from './talk-message.ts';

/**
 * The credit of the frames a delivery holds for its consumer: their wire
 * bytes, returned one by one as they are delivered, all at once when the
 * consumer stops ({@link drained}).
 *
 * @internal
 */
export class Held {
  bytes = 0;
  frames = 0;

  /** The messages held whose bulk payload is a stream nobody read yet. */
  readonly bulk: IncomingMessage[] = [];

  /**
   * The held messages whose reassembled payload has a share of the
   * channel's budget, released as each is delivered, or all of them with
   * the credit (`TalkChannel._returnHeld`).
   */
  readonly assembled = new Set<IncomingMessage>();

  /** The delivery hands out nothing more; its credit went back. */
  drained = false;

  /** Counts the credit of [message], added to the delivery, and, with [trackBulk], its streamed bulk payload. */
  hold(message: IncomingMessage, trackBulk = true): void {
    if (message._assemblyBytes > 0) {
      this.assembled.add(message);
    }
    if (message._creditBytes !== 0) {
      this.bytes += message._creditBytes;
      this.frames++;
    }
    if (trackBulk && message._bulk !== undefined && message._streamBulk) {
      message._heldBulk = true;
      this.bulk.push(message);
    }
  }
}

/**
 * Messages waiting for a consumer, in order, delivered as an async
 * iterable or to a handler. One consumer, attached once: a second
 * iteration, or a handler after an iteration, throws.
 *
 * Ends after what it holds once {@link close}d, with an error first after
 * {@link fail}. When the consumer stops before the end (`break` in a
 * `for await` loop), what it holds is dropped and [onCancel] gets it;
 * [onCancel] is also called, with nothing, once the end was delivered, as
 * a Dart controller calls its `onCancel` after the done event.
 *
 * @internal
 */
export class Delivery implements AsyncIterable<TalkMessage> {
  readonly held: Held;
  readonly #channel: TalkChannel;
  readonly #onCancel: (dropped: IncomingMessage[]) => void;
  readonly #buffer: IncomingMessage[] = [];
  #closed = false;
  #failed = false;
  #error: unknown;
  #listened = false;
  #cancelled = false;
  #ended = false;
  #errorDelivered = false;
  #scheduled = false;
  #waiting: Deferred<IteratorResult<TalkMessage>> | undefined;
  #handler: ((message: IncomingMessage) => void) | undefined;
  #served: Deferred<void> | undefined;

  constructor(channel: TalkChannel, held: Held, onCancel: (dropped: IncomingMessage[]) => void) {
    this.#channel = channel;
    this.held = held;
    this.#onCancel = onCancel;
  }

  /** A consumer is attached (iterating, or serving) and has not stopped. */
  get hasListener(): boolean {
    return this.#listened && !this.#cancelled;
  }

  /** {@link close} or {@link fail} was called. */
  get isClosed(): boolean {
    return this.#closed;
  }

  /** The consumer stopped before the end. */
  get isCancelled(): boolean {
    return this.#cancelled;
  }

  /** How many messages wait for the consumer. */
  get length(): number {
    return this.#buffer.length;
  }

  /** Adds [message] for the consumer. False, and nothing added, once closed or cancelled. */
  add(message: IncomingMessage): boolean {
    if (this.#closed || this.#cancelled) {
      return false;
    }
    this.#buffer.push(message);
    this.#schedule();
    return true;
  }

  /** Ends the delivery with [error], after what it holds. */
  fail(error: unknown): void {
    if (this.#closed) {
      return;
    }
    this.#failed = true;
    this.#error = error;
    this.close();
  }

  /** Ends the delivery after what it holds. */
  close(): void {
    if (this.#closed) {
      return;
    }
    this.#closed = true;
    this.#schedule();
  }

  /**
   * Delivers every message to [handler], one per microtask, as it arrives;
   * resolves once the end was delivered. Never rejects.
   *
   * @throws {Error} if a consumer is attached already.
   */
  listen(handler: (message: IncomingMessage) => void): Promise<void> {
    if (this.#listened) {
      throw new Error('a Talk message delivery has one consumer');
    }
    this.#listened = true;
    this.#handler = handler;
    const served = new Deferred<void>();
    this.#served = served;
    if (this.#cancelled || this.#ended) {
      served.resolve();
    } else {
      this.#schedule();
    }
    return served.promise;
  }

  /**
   * The one iteration of this delivery, as a Dart stream is listened to
   * once: stopping it (`break`) cancels the delivery, so there is nothing
   * left for a second one.
   *
   * @throws {Error} if a consumer was attached already.
   */
  [Symbol.asyncIterator](): AsyncIterator<TalkMessage> {
    if (this.#listened) {
      throw new Error('a Talk message delivery has one consumer');
    }
    this.#listened = true;
    return {
      next: () => this.#next(),
      return: () => {
        this.#cancel();
        return Promise.resolve({ value: undefined, done: true });
      },
    };
  }

  #next(): Promise<IteratorResult<TalkMessage>> {
    if (this.#cancelled || (this.#ended && this.#buffer.length === 0)) {
      return Promise.resolve({ value: undefined, done: true });
    }
    if (this.#waiting !== undefined) {
      return Promise.reject(new Error('a Talk message delivery has one consumer'));
    }
    const waiting = new Deferred<IteratorResult<TalkMessage>>();
    this.#waiting = waiting;
    this.#pump();
    return waiting.promise;
  }

  #schedule(): void {
    if (this.#scheduled) {
      return;
    }
    this.#scheduled = true;
    queueMicrotask(() => {
      this.#scheduled = false;
      this.#pump();
    });
  }

  /**
   * Delivers the head to the consumer, if it takes one now: one message
   * per microtask to a handler, one per `next()` to an iterator. The
   * answer of an `ordered` request is never shown to the consumer: it is
   * run in its place, and the next delivery waits a microtask so that the
   * code awaiting that answer runs before the messages after it.
   */
  #pump(): void {
    if (this.#cancelled || this.#ended) {
      return;
    }
    const push = this.#handler !== undefined;
    if (!push && this.#waiting === undefined) {
      return;
    }
    const message = this.#buffer.shift();
    if (message === undefined) {
      if (this.#closed) {
        this.#end();
      }
      return;
    }
    const marker = message._outcome !== undefined;
    this.#channel._handOver(message, this.held, () => {
      if (marker) {
        this.#channel._outcomeReached(message);
      } else if (push) {
        this.#handler?.(message);
      } else {
        const waiting = this.#waiting;
        this.#waiting = undefined;
        waiting?.resolve({ value: message, done: false });
      }
    });
    if (push || marker) {
      this.#schedule();
    }
  }

  /** Delivers the end: the error first, if any, then done. */
  #end(): void {
    const waiting = this.#waiting;
    if (this.#failed && !this.#errorDelivered) {
      if (this.#handler === undefined) {
        if (waiting === undefined) {
          return;
        }
        this.#errorDelivered = true;
        this.#waiting = undefined;
        this.#ended = true;
        waiting.reject(this.#error);
        this.#onCancel([]);
        return;
      }
      this.#errorDelivered = true;
    }
    this.#ended = true;
    this.#waiting = undefined;
    waiting?.resolve({ value: undefined, done: true });
    this.#served?.resolve();
    this.#onCancel([]);
  }

  /** The consumer stopped: what is held is dropped. */
  #cancel(): void {
    if (this.#cancelled || this.#ended) {
      return;
    }
    this.#cancelled = true;
    const dropped = this.#buffer.splice(0);
    const waiting = this.#waiting;
    this.#waiting = undefined;
    waiting?.resolve({ value: undefined, done: true });
    this.#served?.resolve();
    this.#onCancel(dropped);
  }
}
