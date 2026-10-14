/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// What the node integration tests share: Node nodes (the entry point's
// Switchboard, real sockets) closed after each test, an echo service over
// raw channels, and readers.

import { afterEach } from 'vitest';
import { utf8Decode, utf8Encode } from '../../src/core/bytes.ts';
import type { MuxChannel, MuxOptions } from '../../src/core/mux/contract.ts';
import type { ChannelHandler } from '../../src/core/switchboard/contract.ts';
import { Switchboard, type SwitchboardOptions } from '../../src/node/switchboard.ts';

/** No keep-alive and a short GOAWAY grace, so that closing is quick. */
export const fast: Partial<MuxOptions> = Object.freeze({
  goAwayGrace: 200,
  keepAliveInterval: undefined,
});

const cleanups: (() => unknown)[] = [];
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) {
    await cleanup();
  }
});

/** Runs [cleanup] after the current test. */
export function onCleanup(cleanup: () => unknown): void {
  cleanups.push(cleanup);
}

/** A Node node with [options] and fast mux options, closed after the test. */
export function node(options: SwitchboardOptions = {}): Switchboard {
  const s = new Switchboard({ ...options, muxOptions: { ...fast, ...options.muxOptions } });
  onCleanup(() => s.close());
  return s;
}

/** A handler that sends every subframe back, then closes when the opener does. */
export const echo: ChannelHandler = (incoming) => {
  void (async () => {
    try {
      for await (const subframe of incoming.channel.readable) {
        await incoming.channel.send(subframe);
      }
    } catch {
      // The channel ended.
    }
    await incoming.channel.close();
  })();
};

/** Sends [text] on [channel] and reads one subframe back as text. */
export async function roundTrip(channel: MuxChannel, text: string): Promise<string> {
  const reader = channel.readable.getReader();
  try {
    await channel.send(utf8Encode(text));
    const result = await reader.read();
    return result.done ? 'done' : utf8Decode(result.value);
  } finally {
    reader.releaseLock();
  }
}

/** Resolves [promise], or rejects after [ms]. */
export function within<T>(promise: Promise<T>, ms = 10_000): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`not settled within ${ms} ms`)), ms);
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

export { Switchboard };
