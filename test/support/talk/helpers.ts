/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { expect } from 'vitest';
import { bytesEqual, toPayload } from '../../../src/core/bytes.ts';
import type { MuxChannel } from '../../../src/core/mux/contract.ts';
import { type Status, type StatusCode, SwitchboardError } from '../../../src/core/status.ts';
import type { TalkMessage } from '../../../src/core/talk/contract.ts';
import type { TalkChannel } from '../../../src/core/talk/talk-channel.ts';
import { TalkAbortError } from '../../../src/core/talk/talk-util.ts';
import type { Clock } from '../../../src/core/util/clock.ts';
import { ManualClock } from '../../../src/testing/manual-clock.ts';

/**
 * Lets everything that is not waiting for a timer run: promise jobs, Web
 * Streams deliveries, and the microtasks they queue, over [turns] event
 * loop turns (default 3), so that chains hopping through a macrotask
 * settle too ({@link ManualClock.settle}).
 */
export function flush(turns = 3): Promise<void> {
  return ManualClock.settle(turns);
}

/** Bytes from numbers. */
export function bytes(values: number[]): Uint8Array {
  return Uint8Array.from(values);
}

/**
 * Pulls from an async iterable on demand, as Dart's `StreamQueue` does: a
 * message is taken (and its credit returned) only when asked for.
 */
export class Queue<T> {
  readonly #iterator: AsyncIterator<T>;
  #peeked: Promise<IteratorResult<T>> | undefined;

  constructor(iterable: AsyncIterable<T>) {
    this.#iterator = iterable[Symbol.asyncIterator]();
  }

  #pull(): Promise<IteratorResult<T>> {
    const peeked = this.#peeked;
    if (peeked !== undefined) {
      this.#peeked = undefined;
      return peeked;
    }
    return this.#iterator.next();
  }

  /** The next value; rejects at the end, or with the iteration's error. */
  async next(): Promise<T> {
    const result = await this.#pull();
    if (result.done === true) {
      throw new Error('no next value');
    }
    return result.value;
  }

  /** Whether there is a next value, without taking it; rejects with the iteration's error. */
  async hasNext(): Promise<boolean> {
    this.#peeked ??= this.#iterator.next();
    const result = await this.#peeked;
    return result.done !== true;
  }

  /** The next [count] values. */
  async take(count: number): Promise<T[]> {
    const values: T[] = [];
    for (let i = 0; i < count; i++) {
      values.push(await this.next());
    }
    return values;
  }

  /** Every value until the end. */
  async rest(): Promise<T[]> {
    const values: T[] = [];
    for (;;) {
      const result = await this.#pull();
      if (result.done === true) {
        return values;
      }
      values.push(result.value);
    }
  }

  /** Stops the iteration. */
  async cancel(): Promise<void> {
    await this.#iterator.return?.();
  }
}

/** Replies to every message on [channel] with [respond]; errors are the channel's. */
export function serve(channel: TalkChannel, respond: (message: TalkMessage) => unknown): void {
  void channel.serve(respond);
}

/** The outcome of a promise, observable synchronously once settled. */
export class Outcome<T> {
  value: T | undefined;
  error: unknown;
  isDone = false;

  constructor(promise: Promise<T>) {
    promise.then(
      (value) => {
        this.value = value;
        this.isDone = true;
      },
      (error: unknown) => {
        this.error = error;
        this.isDone = true;
      },
    );
  }

  /** The status code of the error, if it is a {@link SwitchboardError}. */
  get code(): number | undefined {
    return this.error instanceof SwitchboardError ? this.error.status.code : undefined;
  }
}

/** Collects the rejections left unhandled while [body] runs and a little after. */
export async function uncaughtErrors(body: () => Promise<void>): Promise<unknown[]> {
  const errors: unknown[] = [];
  const onRejection = (reason: unknown): void => {
    errors.push(reason);
  };
  process.on('unhandledRejection', onRejection);
  try {
    await body();
    for (let i = 0; i < 5; i++) {
      await new Promise<void>((resolve) => setTimeout(resolve, 0));
    }
  } finally {
    process.off('unhandledRejection', onRejection);
  }
  return errors;
}

/** Expects [error] to be a {@link SwitchboardError} with [code], and returns it. */
export function expectStatus(error: unknown, code: StatusCode): SwitchboardError {
  expect(error).toBeInstanceOf(SwitchboardError);
  expect((error as SwitchboardError).status.code).toBe(code);
  return error as SwitchboardError;
}

/** Expects [action] to throw a {@link SwitchboardError} with [code], and returns it. */
export function throwsStatus(action: () => unknown, code: StatusCode): SwitchboardError {
  let thrown: unknown;
  try {
    action();
  } catch (e) {
    thrown = e;
  }
  expect(thrown, 'expected a throw').toBeDefined();
  return expectStatus(thrown, code);
}

/** Expects [promise] to reject with a {@link SwitchboardError} with [code], and returns it. */
export async function rejectsStatus(
  promise: Promise<unknown>,
  code: StatusCode,
): Promise<SwitchboardError> {
  let rejection: unknown;
  let rejected = false;
  try {
    await promise;
  } catch (e) {
    rejection = e;
    rejected = true;
  }
  expect(rejected, 'expected a rejection').toBe(true);
  return expectStatus(rejection, code);
}

/** Text as bytes, for payloads. */
export function text(value: string): Uint8Array {
  return toPayload(value);
}

/**
 * Consumes an async iterable in the background, collecting its values,
 * and can be paused like a Dart stream subscription: while paused it asks
 * for nothing, so nothing is taken (and no credit returned).
 */
export class Collector<T> {
  readonly values: T[] = [];
  error: unknown;
  isDone = false;
  readonly #iterator: AsyncIterator<T>;
  readonly #onValue: ((value: T) => unknown) | undefined;
  #paused: boolean;
  #resume: (() => void) | undefined;
  #stopped = false;
  readonly finished: Promise<void>;

  constructor(
    iterable: AsyncIterable<T>,
    options: { paused?: boolean; onValue?: (value: T) => unknown } = {},
  ) {
    this.#iterator = iterable[Symbol.asyncIterator]();
    this.#paused = options.paused === true;
    this.#onValue = options.onValue;
    this.finished = this.#run();
  }

  async #run(): Promise<void> {
    try {
      for (;;) {
        while (this.#paused && !this.#stopped) {
          await new Promise<void>((resolve) => {
            this.#resume = resolve;
          });
        }
        if (this.#stopped) {
          return;
        }
        const result = await this.#iterator.next();
        if (result.done === true) {
          return;
        }
        this.values.push(result.value);
        this.#onValue?.(result.value);
      }
    } catch (e) {
      this.error = e;
    } finally {
      this.isDone = true;
    }
  }

  pause(): void {
    this.#paused = true;
  }

  resume(): void {
    this.#paused = false;
    const resume = this.#resume;
    this.#resume = undefined;
    resume?.();
  }

  /** Stops the iteration, as cancelling a subscription does. */
  async cancel(): Promise<void> {
    this.#stopped = true;
    const resume = this.#resume;
    this.#resume = undefined;
    resume?.();
    await this.#iterator.return?.();
  }
}

/** Every value of [iterable], as Dart's `Stream.toList`; marked handled until awaited. */
export function toList<T>(iterable: AsyncIterable<T>): Promise<T[]> {
  const list = (async () => {
    const values: T[] = [];
    for await (const value of iterable) {
      values.push(value);
    }
    return values;
  })();
  list.catch(() => {});
  return list;
}

/** The first value of [iterable], which then stops, as Dart's `Stream.first`. */
export async function first<T>(iterable: AsyncIterable<T>): Promise<T> {
  for await (const value of iterable) {
    return value;
  }
  throw new Error('no element');
}

/** The error [promise] rejects with; fails the test if it resolves. */
export async function rejection(promise: Promise<unknown>): Promise<unknown> {
  try {
    await promise;
  } catch (e) {
    return e;
  }
  throw new Error('expected a rejection');
}

/** [promise], failing after [ms] of real time (Dart's `Future.timeout`). */
export function within<T>(promise: Promise<T>, ms = 1000): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`no outcome within ${ms} ms`)), ms);
    promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (error: unknown) => {
        clearTimeout(timer);
        reject(error);
      },
    );
  });
}

/** Resolves after [ms] of [clock]'s time. */
export function sleep(clock: Clock, ms: number): Promise<void> {
  return new Promise<void>((resolve) => {
    clock.setTimeout(resolve, ms);
  });
}

/** Expects [actual] to hold the bytes of [expected], compared byte by byte (a deep equality of large arrays is slow). */
export function expectBytes(actual: Uint8Array, expected: Uint8Array): void {
  expect(actual.length).toBe(expected.length);
  expect(bytesEqual(actual, expected), 'bytes differ').toBe(true);
}

/** Expects [actual] to be the same status as [expected]: code and bytes. */
export function expectSame(actual: Status | undefined, expected: Status): void {
  expect(actual?.equals(expected), `${actual} is ${expected}`).toBe(true);
}

/** Expects [error] to be an abort that came from the peer (a {@link TalkAbortError}) with [code], and returns it. */
export function expectRemote(error: unknown, code: StatusCode): TalkAbortError {
  expect(error).toBeInstanceOf(TalkAbortError);
  expect((error as TalkAbortError).status.code).toBe(code);
  return error as TalkAbortError;
}

/** Expects [promise] to reject with an abort that came from the peer, with [code], and returns it. */
export async function rejectsRemote(
  promise: Promise<unknown>,
  code: StatusCode,
): Promise<TalkAbortError> {
  return expectRemote(await rejection(promise), code);
}

/** The mux channel under [talk]. */
export function rawOf(talk: TalkChannel): MuxChannel {
  const raw = talk.raw;
  if (raw === undefined) {
    throw new Error('not over a mux channel');
  }
  return raw;
}

/** The only element of [list]. */
export function only<T>(list: readonly T[]): T {
  expect(list).toHaveLength(1);
  return list[0] as T;
}

/** The last element of [list]. */
export function last<T>(list: readonly T[]): T {
  expect(list.length).toBeGreaterThan(0);
  return list[list.length - 1] as T;
}
