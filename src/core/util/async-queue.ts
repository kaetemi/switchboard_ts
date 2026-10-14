/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { Deferred } from './deferred.ts';

/**
 * A single-consumer queue of values delivered as an async iterable: the
 * library's substitute for a Dart single-subscription stream. Values are
 * buffered until taken; {@link end} ends the iteration after the buffered
 * values, {@link fail} ends it with an error. A `for await` loop that
 * `break`s calls {@link onCancel}, so that the producer can stop (a mux
 * connection closes the channels nobody took, say).
 */
export class AsyncQueue<T> implements AsyncIterable<T> {
  readonly #buffer: T[] = [];
  #waiting: Deferred<IteratorResult<T>> | undefined;
  #ended = false;
  #error: unknown;
  #consumed = false;

  /** Called once when the consumer stops iterating before the end. */
  onCancel: (() => void) | undefined;

  /** How many values wait to be taken. */
  get length(): number {
    return this.#buffer.length;
  }

  get isEnded(): boolean {
    return this.#ended;
  }

  /** Whether something is iterating (or has iterated) the queue. */
  get hasConsumer(): boolean {
    return this.#consumed;
  }

  /** Whether the consumer is waiting for a value right now. */
  get isWaiting(): boolean {
    return this.#waiting !== undefined;
  }

  push(value: T): void {
    if (this.#ended) {
      return;
    }
    const w = this.#waiting;
    if (w !== undefined) {
      this.#waiting = undefined;
      w.resolve({ value, done: false });
    } else {
      this.#buffer.push(value);
    }
  }

  end(): void {
    if (this.#ended) {
      return;
    }
    this.#ended = true;
    const w = this.#waiting;
    if (w !== undefined) {
      this.#waiting = undefined;
      w.resolve({ value: undefined, done: true });
    }
  }

  fail(error: unknown): void {
    if (this.#ended) {
      return;
    }
    this.#ended = true;
    this.#error = error;
    const w = this.#waiting;
    if (w !== undefined) {
      this.#waiting = undefined;
      w.reject(error);
    }
  }

  /** Takes the next value: the result, or `done` at the end. */
  next(): Promise<IteratorResult<T>> {
    this.#consumed = true;
    if (this.#buffer.length > 0) {
      return Promise.resolve({ value: this.#buffer.shift() as T, done: false });
    }
    if (this.#ended) {
      return this.#error !== undefined
        ? Promise.reject(this.#error)
        : Promise.resolve({ value: undefined, done: true });
    }
    if (this.#waiting !== undefined) {
      return Promise.reject(new Error('AsyncQueue has one consumer'));
    }
    this.#waiting = new Deferred();
    return this.#waiting.promise;
  }

  /** The buffered values, removed from the queue. */
  drain(): T[] {
    return this.#buffer.splice(0);
  }

  [Symbol.asyncIterator](): AsyncIterator<T> {
    this.#consumed = true;
    return {
      next: () => this.next(),
      return: () => {
        if (!this.#ended) {
          this.onCancel?.();
        }
        return Promise.resolve({ value: undefined, done: true });
      },
    };
  }
}
