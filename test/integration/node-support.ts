/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// What the node integration tests share: Node nodes (the entry point's
// Switchboard, real sockets) closed after each test, an echo service over
// raw channels, a Talk service, and readers.

import { afterEach, expect } from 'vitest';
import { utf8Decode, utf8Encode } from '../../src/core/bytes.ts';
import type { MuxChannel, MuxOptions } from '../../src/core/mux/contract.ts';
import { Status, StatusCode, SwitchboardError, statusCodeName } from '../../src/core/status.ts';
import type { ChannelHandler } from '../../src/core/switchboard/contract.ts';
import type { TalkChannel } from '../../src/core/talk/contract.ts';
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

/**
 * A Talk service (the design's example): `UPPER` answers its payload upper
 * cased, `COUNT n` answers the items `1` to `n` and then `done`, `ECHO`
 * answers its payload (inline or bulk, as it came), anything else
 * UNIMPLEMENTED.
 */
export const talkService: ChannelHandler = async (incoming) => {
  const talk = incoming.talk();
  for await (const message of talk.messages) {
    switch (message.procedureName) {
      case 'UPPER':
        await message.reply(message.text().toUpperCase());
        break;
      case 'COUNT':
        for (let i = 1; i <= Number(message.text()); i++) await message.replyItem(String(i));
        await message.reply('done');
        break;
      case 'ECHO':
        await message.reply(message.payload);
        break;
      default:
        await message.replyAbort(Status.of(StatusCode.unimplemented));
    }
  }
};

/**
 * Requests to a {@link talkService} on [talk]: a reply, a stream of items
 * and its final response, a refusal, and an echo of [echoBytes] bytes
 * (bulk both ways when the channel's `bulkThreshold`, if set, is below
 * that).
 */
export async function exerciseTalk(talk: TalkChannel, echoBytes = 0): Promise<void> {
  expect((await talk.request('UPPER', 'hello')).text()).toBe('HELLO');
  const stream = talk.streamRequest('COUNT', '3');
  const items: string[] = [];
  for await (const item of stream) items.push(item.text());
  expect(items).toEqual(['1', '2', '3']);
  expect((await stream.done).text()).toBe('done');
  const refused = await talk.request('OTHER').then(
    () => undefined,
    (e: unknown) => e,
  );
  expect(refused).toBeInstanceOf(SwitchboardError);
  expect(statusCodeName((refused as SwitchboardError).code)).toBe('unimplemented');
  if (echoBytes > 0) {
    const payload = new Uint8Array(echoBytes);
    for (let i = 0; i < payload.length; i++) {
      payload[i] = (i * 31 + Math.floor(i / 2048)) & 0xff;
    }
    const reply = await talk.request('ECHO', payload);
    const threshold = talk.options.bulkThreshold;
    if (threshold !== undefined) {
      expect(reply.isBulk).toBe(echoBytes > threshold);
    }
    expect(Buffer.compare(Buffer.from(reply.payload), Buffer.from(payload))).toBe(0);
  }
}

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
