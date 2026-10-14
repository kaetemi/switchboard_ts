/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { AsyncQueue } from '../util/async-queue.ts';

/**
 * Values delivered to every consumer iterating at the time, the library's
 * form of a Dart broadcast stream: each `for await` sees the values pushed
 * from the moment it starts (after [replay], when given), buffered until
 * taken; {@link end} ends every iteration. Internal.
 */
export class Broadcast<T> implements AsyncIterable<T> {
  readonly #queues = new Set<AsyncQueue<T>>();
  readonly #replay: (() => T | undefined) | undefined;
  #ended = false;

  /** [replay] gives the value each new consumer receives first, if any. */
  constructor(replay?: () => T | undefined) {
    this.#replay = replay;
  }

  get isEnded(): boolean {
    return this.#ended;
  }

  /** How many consumers are iterating. */
  get listenerCount(): number {
    return this.#queues.size;
  }

  push(value: T): void {
    for (const queue of this.#queues) {
      queue.push(value);
    }
  }

  end(): void {
    if (this.#ended) {
      return;
    }
    this.#ended = true;
    for (const queue of this.#queues) {
      queue.end();
    }
    this.#queues.clear();
  }

  [Symbol.asyncIterator](): AsyncIterator<T> {
    const queue = new AsyncQueue<T>();
    const first = this.#replay?.();
    if (first !== undefined) {
      queue.push(first);
    }
    if (this.#ended) {
      queue.end();
    } else {
      this.#queues.add(queue);
      queue.onCancel = () => {
        this.#queues.delete(queue);
      };
    }
    return queue[Symbol.asyncIterator]();
  }
}
