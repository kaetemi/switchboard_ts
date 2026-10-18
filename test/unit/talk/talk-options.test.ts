/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// The options merge refuses the Talk options whose value would make the
// channel loop: a `bulkChunkSize` that is not a positive integer has a
// bulk sender send empty chunks for ever (`BulkOut`'s pump takes chunks
// while it holds at least that many bytes). TS only: Dart's const
// constructor takes any value.

import { expect, test } from 'vitest';
import { defaultTalkOptions, talkOptions } from '../../../src/core/talk/contract.ts';
import { TalkChannel } from '../../../src/core/talk/talk-channel.ts';
import { collect, pattern } from '../../support/talk/bulk-peers.ts';
import { expectBytes, Queue } from '../../support/talk/helpers.ts';
import { memoryMuxPair } from '../../support/talk/memory-mux.ts';

const empty = new Uint8Array(0);

test('a bulkChunkSize that is not a positive integer is a RangeError', () => {
  for (const bulkChunkSize of [0, -1, 0.5, 1.5, Number.NaN, Number.POSITIVE_INFINITY]) {
    expect(() => talkOptions({ bulkChunkSize })).toThrow(RangeError);
  }
  const [client] = memoryMuxPair();
  expect(() => new TalkChannel(client.open(empty), { bulkChunkSize: 0 })).toThrow(RangeError);
});

test('the defaults, and a bulkChunkSize of 1, are taken', () => {
  expect(talkOptions()).toBe(defaultTalkOptions);
  expect(talkOptions({}).bulkChunkSize).toBe(defaultTalkOptions.bulkChunkSize);
  expect(talkOptions({ bulkChunkSize: 1 }).bulkChunkSize).toBe(1);
});

test('a bulk payload goes in chunks of a small bulkChunkSize', async () => {
  const [client, server] = memoryMuxPair();
  const talk = new TalkChannel(client.open(empty), { bulkChunkSize: 7 });
  const responder = new TalkChannel(await server.accept(), { streamBulk: () => true });
  const payload = pattern(100);
  void talk.send('PUT', empty, { bulk: [payload] });
  const message = await new Queue(responder.messages).next();
  expectBytes(await collect(message.bulk), payload);
  // The bulk channel's subframes: 14 of 7 bytes and the 2 left.
  const sizes = server.received.flatMap((frame) =>
    frame.type === 'data' && frame.id !== 2 ? [frame.bytes.length] : [],
  );
  expect(sizes).toEqual([...Array.from({ length: 14 }, () => 7), 2]);
  await talk.close();
  await client.close();
  await server.close();
});
