/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Bulk payloads across the pairings (talk_bulk_test.dart, "over TCP"): the
// inline/bulk boundary of requests and responses at the default subframe
// limit (32752 bytes: half the default 64 KiB window, and the QUIC record
// limit before a HELLO raises it), bulk payloads of declared and unknown
// length both ways, bulk stream items and plain messages, and messages
// keeping their order while a bulk payload is reassembled.

import { expect, test } from 'vitest';
import { hexString } from '../../../src/core/bytes.ts';
import { describeCells } from '../matrix.ts';
import type { MessageInfo, TalkItemEvent } from '../protocol.ts';
import { hex, openTalk, pattern, sha256, text } from './support.ts';

/** The largest subframe at the defaults. */
const maxSubframe = 32752;
/** A request's header: flags, procedure, request id. */
const requestHeader = 12;
/** A response's header: flags, response id. */
const responseHeader = 4;

describeCells('bulk', {}, (peers) => {
  async function how(size: number): Promise<string> {
    const { talk } = await openTalk(peers);
    const reply = await peers.client.rpc<MessageInfo>('talk.request', {
      talk,
      procedure: 'HOW',
      patternLength: size,
    });
    return text(reply.payload);
  }

  test.each([0, 1, maxSubframe - requestHeader])('a request of %i bytes goes inline', async (n) => {
    expect(await how(n)).toBe(`inline ${n}`);
  });

  test.each([maxSubframe - requestHeader + 1, maxSubframe, maxSubframe + 1, 100_000, 1_000_000])(
    'a request of %i bytes goes bulk',
    async (n) => {
      expect(await how(n)).toBe(`bulk ${n}`);
    },
  );

  test.each([
    [maxSubframe - responseHeader, false],
    [maxSubframe - responseHeader + 1, true],
    [1_000_000, true],
  ])('a response of %i bytes: bulk %s', async (n, bulk) => {
    const { talk } = await openTalk(peers);
    const reply = await peers.client.rpc<MessageInfo>('talk.request', {
      talk,
      procedure: 'BIG',
      text: String(n),
      digest: true,
    });
    expect(reply).toEqual({ procedure: '', bulk, size: n, sha256: sha256(pattern(n)) });
  });

  test('a request with a bulk payload of declared length', async () => {
    const { talk } = await openTalk(peers);
    const n = 300_000;
    const reply = await peers.client.rpc<MessageInfo>('talk.request', {
      talk,
      procedure: 'SIZE',
      bulkPattern: { length: n, chunk: 7000 },
    });
    expect(reply.payload).toBe(hex(`${n} ${sha256(pattern(n))}`));
  });

  test('a request with a bulk payload of unknown length, in small pieces', async () => {
    const { talk } = await openTalk(peers);
    const n = 123_457;
    const reply = await peers.client.rpc<MessageInfo>('talk.request', {
      talk,
      procedure: 'SIZE',
      bulkPattern: { length: n, chunk: 777, declared: false },
    });
    expect(reply.payload).toBe(hex(`${n} ${sha256(pattern(n))}`));
  });

  test('a short bulk payload of unknown length', async () => {
    const { talk } = await openTalk(peers);
    const reply = await peers.client.rpc<MessageInfo>('talk.request', {
      talk,
      procedure: 'HOW',
      bulkPattern: { length: 10, declared: false },
    });
    expect(text(reply.payload)).toBe('bulk 10');
  });

  test('a response with a bulk payload of unknown length', async () => {
    const { talk } = await openTalk(peers);
    const n = 200_001;
    const reply = await peers.client.rpc<MessageInfo>('talk.request', {
      talk,
      procedure: 'BIGSTRM',
      text: String(n),
      digest: true,
    });
    expect(reply).toEqual({ procedure: '', bulk: true, size: n, sha256: sha256(pattern(n)) });
  });

  test('stream items with bulk payloads', async () => {
    const { talk } = await openTalk(peers);
    const n = 50_000;
    const { stream } = await peers.client.rpc<{ stream: string }>('talk.stream', {
      talk,
      procedure: 'BIGITEMS',
      text: `3 ${n}`,
      digest: true,
    });
    const done = await peers.client.events.waitFor({ event: 'talk.done', stream });
    const items = peers.client.events.filter<TalkItemEvent>({ event: 'talk.item', stream });
    expect(items.map((i) => [i.bulk, i.size, i.sha256])).toEqual(
      Array.from({ length: 3 }, () => [true, n, sha256(pattern(n))]),
    );
    expect(done).toMatchObject({ items: 3, bulk: false, size: 4 });
  });

  test('a plain message with a bulk payload, from the requester', async () => {
    const { talk } = await openTalk(peers);
    const n = 40_000;
    await peers.client.rpc('talk.send', { talk, procedure: 'NOTE', patternLength: n });
    const message = await peers.server.events.waitFor('service.message');
    expect(message).toMatchObject({
      procedure: 'NOTE',
      bulk: true,
      payload: hexString(pattern(n)),
    });
  });

  test('a plain message with a bulk payload, from the responder', async () => {
    const { talk } = await openTalk(peers);
    const n = 40_000;
    const reply = await peers.client.rpc<MessageInfo>('talk.request', {
      talk,
      procedure: 'NOTIFY',
      patternLength: n,
    });
    expect(reply.payload).toBe(hex('sent'));
    const note = await peers.client.events.waitFor({ event: 'talk.message', talk });
    expect(note).toMatchObject({ procedure: 'NOTE', bulk: true, payload: hexString(pattern(n)) });
  });

  test('messages keep their order while a bulk payload is reassembled', async () => {
    const { talk } = await openTalk(peers);
    const sizes = [1, 500_000, 2, 100_000, 3];
    await Promise.all(
      sizes.map((n) =>
        peers.client.rpc('talk.send', { talk, procedure: 'NOTE', patternLength: n }),
      ),
    );
    const messages = await peers.server.events.waitForCount('service.message', sizes.length);
    expect(messages.map((m) => String(m['payload']).replaceAll(' ', '').length / 2)).toEqual(sizes);
  });

  test('bulk payloads on several channels at once', async () => {
    const channels = await Promise.all([openTalk(peers), openTalk(peers), openTalk(peers)]);
    const n = 400_000;
    const replies = await Promise.all(
      channels.flatMap(({ talk }) => [
        peers.client.rpc<MessageInfo>('talk.request', {
          talk,
          procedure: 'SIZE',
          patternLength: n,
        }),
        peers.client.rpc<MessageInfo>('talk.request', {
          talk,
          procedure: 'BIG',
          text: String(n),
          digest: true,
        }),
      ]),
    );
    const digest = sha256(pattern(n));
    for (let i = 0; i < replies.length; i += 2) {
      expect(replies[i]?.payload).toBe(hex(`${n} ${digest}`));
      expect(replies[i + 1]).toMatchObject({ bulk: true, size: n, sha256: digest });
    }
  });
});
