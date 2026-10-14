/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { expect } from 'vitest';
import { toPayload } from '../../../src/core/bytes.ts';
import { type StatusCode, SwitchboardError } from '../../../src/core/status.ts';
import type { TalkMessage } from '../../../src/core/talk/contract.ts';
import type { TalkChannel } from '../../../src/core/talk/talk-channel.ts';

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
