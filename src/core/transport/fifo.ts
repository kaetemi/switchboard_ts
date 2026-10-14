/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * A first-in first-out queue with constant time `shift`, for the frame and
 * output queues of the transports and the mux. Internal; not exported from
 * the package.
 */
export class Fifo<T> {
  #items: (T | undefined)[] = [];
  #head = 0;

  /** Number of queued items. */
  get length(): number {
    return this.#items.length - this.#head;
  }

  /** The item {@link shift} would return, or `undefined`. */
  peek(): T | undefined {
    return this.#items[this.#head];
  }

  /** Appends [item]. */
  push(item: T): void {
    this.#items.push(item);
  }

  /** Removes and returns the first item, or `undefined` when empty. */
  shift(): T | undefined {
    if (this.#head >= this.#items.length) {
      return undefined;
    }
    const item = this.#items[this.#head];
    this.#items[this.#head] = undefined;
    this.#head++;
    if (this.#head === this.#items.length) {
      this.#items = [];
      this.#head = 0;
    } else if (this.#head >= 1024 && this.#head * 2 >= this.#items.length) {
      this.#items = this.#items.slice(this.#head);
      this.#head = 0;
    }
    return item;
  }

  /** Removes every item. */
  clear(): void {
    this.#items = [];
    this.#head = 0;
  }

  /** The queued items, first to last. */
  *[Symbol.iterator](): IterableIterator<T> {
    for (let i = this.#head; i < this.#items.length; i++) {
      yield this.#items[i] as T;
    }
  }
}
