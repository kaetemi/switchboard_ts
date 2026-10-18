/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { type Clock, systemClock } from './clock.ts';

/**
 * A promise with its resolvers, settled at most once. `promise` never
 * reports an unhandled rejection: a rejection nobody awaits is swallowed,
 * which is what the library's `done` futures need.
 */
export class Deferred<T> {
  readonly promise: Promise<T>;
  #resolve!: (value: T | PromiseLike<T>) => void;
  #reject!: (reason: unknown) => void;
  #settled = false;

  constructor() {
    this.promise = new Promise<T>((resolve, reject) => {
      this.#resolve = resolve;
      this.#reject = reject;
    });
    this.promise.catch(() => {});
  }

  get isSettled(): boolean {
    return this.#settled;
  }

  resolve(value: T | PromiseLike<T>): boolean {
    if (this.#settled) {
      return false;
    }
    this.#settled = true;
    this.#resolve(value);
    return true;
  }

  reject(reason: unknown): boolean {
    if (this.#settled) {
      return false;
    }
    this.#settled = true;
    this.#reject(reason);
    return true;
  }
}

/**
 * A promise that resolves after [ms] milliseconds of [clock] (default
 * {@link systemClock}), or rejects when [signal] aborts. A rejection
 * nobody awaits is not reported as unhandled.
 */
export function sleep(ms: number, signal?: AbortSignal, clock: Clock = systemClock): Promise<void> {
  const slept = new Promise<void>((resolve, reject) => {
    if (signal?.aborted) {
      reject(signal.reason);
      return;
    }
    const timer = clock.setTimeout(() => {
      signal?.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    function onAbort(): void {
      clock.clearTimeout(timer);
      reject(signal?.reason);
    }
    signal?.addEventListener('abort', onAbort, { once: true });
  });
  slept.catch(() => {});
  return slept;
}
