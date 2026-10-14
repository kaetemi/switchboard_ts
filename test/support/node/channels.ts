/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { utf8Decode } from '../../../src/core/bytes.ts';

/** What these helpers read: a mux channel, or a channel carrying one (a slot channel). */
export interface Readable {
  readonly readable: ReadableStream<Uint8Array>;
}

/**
 * Reading raw channels and async iterables in tests, one value at a time,
 * without cancelling them in between (a `for await` that breaks would).
 */

/** Pulls values from [iterable] one by one through one iterator. */
export class Pull<T> {
  readonly #iterator: AsyncIterator<T>;

  constructor(iterable: AsyncIterable<T>) {
    this.#iterator = iterable[Symbol.asyncIterator]();
  }

  /** The next value; throws if the iterable ended. */
  async next(): Promise<T> {
    const result = await this.#iterator.next();
    if (result.done === true) {
      throw new Error('ended without a value');
    }
    return result.value;
  }

  /** Stops the iteration. */
  async cancel(): Promise<void> {
    await this.#iterator.return?.();
  }
}

/** Reads the subframes of a channel through one reader, which stays locked. */
export class Frames {
  readonly #reader: ReadableStreamDefaultReader<Uint8Array>;

  constructor(channel: Readable) {
    this.#reader = channel.readable.getReader();
  }

  /** The next subframe; undefined at the end. */
  async next(): Promise<Uint8Array | undefined> {
    const { value, done } = await this.#reader.read();
    return done ? undefined : value;
  }

  /** The next subframe as text; throws at the end. */
  async text(): Promise<string> {
    const value = await this.next();
    if (value === undefined) {
      throw new Error('no frame');
    }
    return utf8Decode(value);
  }

  /** The subframes left, until the end. */
  async rest(): Promise<Uint8Array[]> {
    const frames: Uint8Array[] = [];
    for (;;) {
      const value = await this.next();
      if (value === undefined) return frames;
      frames.push(value);
    }
  }
}

/** The first subframe of [channel] as text, the reader released after. */
export async function nextText(channel: Readable): Promise<string> {
  const reader = channel.readable.getReader();
  try {
    const { value } = await reader.read();
    if (value === undefined) {
      throw new Error('no frame');
    }
    return utf8Decode(value);
  } finally {
    reader.releaseLock();
  }
}

/** Every subframe of [channel] until its end. */
export function framesOf(channel: Readable): Promise<Uint8Array[]> {
  return new Frames(channel).rest();
}

/** [length] bytes of a recognisable pattern starting at [seed]. */
export function pattern(length: number, seed = 0): Uint8Array {
  const bytes = new Uint8Array(length);
  for (let i = 0; i < length; i++) {
    bytes[i] = (i * 31 + seed) & 0xff;
  }
  return bytes;
}
