/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// What the frame mux tests over real transports share: the `exercise` of
// the "real transports" group of `test/mux_test.dart`, and small readers.

import { expect } from 'vitest';
import { hexBytes, hexString } from '../../src/core/bytes.ts';
import type { MuxChannel, MuxConnection, MuxOptions } from '../../src/core/mux/contract.ts';
import { Status, StatusCode } from '../../src/core/status.ts';

/** No keep-alive, a short grace; windows of 256 KiB, so that a subframe of 100000 bytes fits. */
export const wide: Partial<MuxOptions> = Object.freeze({
  keepAliveInterval: undefined,
  goAwayGrace: 2000,
  initialWindow: 256 * 1024,
});

const cleanups: (() => unknown)[] = [];

/** Runs [cleanup] after the current test (call {@link runCleanups} from `afterEach`). */
export function onCleanup(cleanup: () => unknown): void {
  cleanups.push(cleanup);
}

/** Runs the cleanups registered so far, last first. */
export async function runCleanups(): Promise<void> {
  for (const cleanup of cleanups.splice(0).reverse()) {
    await cleanup();
  }
}

/** A reader of [iterable] that takes one value at a time. */
export function queue<T>(iterable: AsyncIterable<T>): { next(): Promise<T> } {
  const iterator = iterable[Symbol.asyncIterator]();
  return {
    async next() {
      const result = await iterator.next();
      if (result.done === true) {
        throw new Error('ended');
      }
      return result.value;
    },
  };
}

/** Whether [a] and [b] hold the same bytes (fast for large arrays). */
export function sameBytes(a: Uint8Array | undefined, b: Uint8Array): boolean {
  return a !== undefined && Buffer.compare(Buffer.from(a), Buffer.from(b)) === 0;
}

/** Reads [count] bytes of [channel], however they are split into subframes. */
async function readBytes(channel: MuxChannel, count: number): Promise<Uint8Array> {
  const out = new Uint8Array(count);
  let filled = 0;
  for await (const chunk of channel.readable) {
    out.set(chunk, filled);
    filled += chunk.length;
    if (filled >= count) {
      break;
    }
  }
  return out.subarray(0, filled);
}

/**
 * The exercise of Dart's "real transports" group: an echo channel with a
 * subframe of 100000 bytes, a bulk channel the other way carrying 1 MB
 * paced by the window, a channel close status, a ping, and GOAWAY ending
 * both sides with GOING_AWAY.
 */
export async function exercise(client: MuxConnection, server: MuxConnection): Promise<void> {
  // Channels opened before the peer's LIMITS arrived keep 64 KiB.
  await client.ping();
  await server.ping();
  const serverIncoming = queue(server.incoming);
  const clientIncoming = queue(client.incoming);
  const channel = client.open(hexBytes('AA'));
  const accepted = await serverIncoming.next();
  expect(accepted.id).toBe(2);
  expect(hexString(accepted.openPayload)).toBe('AA');
  const echo = (async () => {
    for await (const subframe of accepted.readable) {
      await accepted.send(subframe);
    }
  })();
  const replies = channel.readable.getReader();
  const big = new Uint8Array(100000).fill(7);
  void channel.send(hexBytes('01 02'));
  void channel.send(big);
  expect(hexString((await replies.read()).value as Uint8Array)).toBe('01 02');
  expect(sameBytes((await replies.read()).value, big)).toBe(true);
  replies.releaseLock();
  // A bulk payload of 1 MB, in chunks, paced by the window.
  const back = server.open(hexBytes('BB'));
  back.priority = 'bulk';
  expect(back.id).toBe(3);
  const backAtClient = await clientIncoming.next();
  const bulk = new Uint8Array(1000000);
  for (let i = 0; i < bulk.length; i++) {
    bulk[i] = (i * 13) & 0xff;
  }
  const received = readBytes(backAtClient, bulk.length);
  await back.send(bulk);
  expect(sameBytes(await received, bulk)).toBe(true);
  await channel.close(Status.of(StatusCode.cancelled));
  expect((await accepted.done).known).toBe(StatusCode.cancelled);
  await echo.catch(() => {});
  await back.close();
  expect(typeof (await client.ping())).toBe('number');
  await server.goAway();
  expect((await client.done).known).toBe(StatusCode.goingAway);
  expect((await server.done).known).toBe(StatusCode.goingAway);
}
