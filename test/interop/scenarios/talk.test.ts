/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Talk across the pairings (talk_test.dart, talk_window_test.dart): requests
// in flight, stream requests and their item windows, aborts with their exact
// status, timeouts on either side, EXTEND, cancels and plain messages. The
// timeouts cannot be faked across processes, so they are short and real.

import { expect, test } from 'vitest';
import { hexString } from '../../../src/core/bytes.ts';
import { StatusCode } from '../../../src/core/status.ts';
import { wikiBytes } from '../../support/vectors.ts';
import { type CellPeers, describeCells } from '../matrix.ts';
import type { MessageInfo, PeerErrorInfo, PeerEvent, TalkItemEvent } from '../protocol.ts';
import { connect, hex, openTalk } from './support.ts';

/** The items and the end of a stream request. */
async function streamOutcome(
  peers: CellPeers,
  stream: string,
): Promise<{ items: string[]; done: PeerEvent }> {
  const done = await peers.client.events.waitFor({ event: 'talk.done', stream });
  const items = peers.client.events
    .filter<TalkItemEvent>({ event: 'talk.item', stream })
    .map((i) => i.payload ?? '');
  return { items, done };
}

/**
 * A round trip on [talk]: everything the responder sent on the channel
 * before answering it has arrived once it is answered.
 */
async function barrier(peers: CellPeers, talk: string): Promise<void> {
  await peers.client.rpc('talk.request', { talk, procedure: 'ECHO', text: 'barrier' });
}

describeCells('talk', {}, (peers) => {
  test('an empty request is answered with an empty payload', async () => {
    const { talk } = await openTalk(peers);
    const reply = await peers.client.rpc<MessageInfo>('talk.request', { talk, procedure: 'ECHO' });
    expect(reply).toEqual({ procedure: '', bulk: false, payload: '' });
  });

  test('200 requests in flight on one channel are all answered', async () => {
    const { talk } = await openTalk(peers);
    const texts = Array.from({ length: 200 }, (_, i) => `request ${i}`);
    const replies = await Promise.all(
      texts.map((text) =>
        peers.client.rpc<MessageInfo>('talk.request', { talk, procedure: 'UPPER', text }),
      ),
    );
    expect(replies.map((r) => r.payload)).toEqual(texts.map((t) => hex(t.toUpperCase())));
  });

  test('channels of one connection are independent', async () => {
    const connection = await connect(peers);
    const [a, b] = await Promise.all([
      openTalk(peers, 'echo', connection),
      openTalk(peers, 'echo', connection),
    ]);
    const stalled = peers.client.rpcError('talk.request', {
      talk: a.talk,
      procedure: 'STALL',
      timeoutMs: 500,
    });
    const reply = await peers.client.rpc<MessageInfo>('talk.request', {
      talk: b.talk,
      procedure: 'ECHO',
      text: 'b',
    });
    expect(reply.payload).toBe(hex('b'));
    expect(await stalled).toMatchObject({ kind: 'status', code: StatusCode.deadlineExceeded });
  });

  test('a stream request without items has only its final response', async () => {
    const { talk } = await openTalk(peers);
    const { stream } = await peers.client.rpc<{ stream: string }>('talk.stream', {
      talk,
      procedure: 'COUNT',
      text: '0',
    });
    const { items, done } = await streamOutcome(peers, stream);
    expect(items).toEqual([]);
    expect(done).toMatchObject({ items: 0, procedure: '', bulk: false, payload: hex('done') });
  });

  test('a finite item window delivers that many items, then waits for a grant', async () => {
    const { talk } = await openTalk(peers);
    const { stream } = await peers.client.rpc<{ stream: string }>('talk.stream', {
      talk,
      procedure: 'COUNT',
      text: '5',
      window: 2,
    });
    await peers.client.events.waitForCount({ event: 'talk.item', stream }, 2);
    await barrier(peers, talk);
    expect(peers.client.events.filter({ event: 'talk.item', stream })).toHaveLength(2);
    await peers.client.rpc('stream.grant', { stream, items: 1 });
    await peers.client.events.waitForCount({ event: 'talk.item', stream }, 3);
    await barrier(peers, talk);
    expect(peers.client.events.filter({ event: 'talk.item', stream })).toHaveLength(3);
    await peers.client.rpc('stream.grant', { stream, items: 2 });
    const { items, done } = await streamOutcome(peers, stream);
    expect(items).toEqual(['1', '2', '3', '4', '5'].map(hex));
    expect(done).toMatchObject({ items: 5, payload: hex('done') });
  });

  test('a window of 0 starts exhausted; the first grant starts delivery', async () => {
    const { talk } = await openTalk(peers);
    const { stream } = await peers.client.rpc<{ stream: string }>('talk.stream', {
      talk,
      procedure: 'COUNT',
      text: '2',
      window: 0,
    });
    await barrier(peers, talk);
    expect(peers.client.events.filter({ event: 'talk.item', stream })).toEqual([]);
    await peers.client.rpc('stream.grant', { stream, items: 2 });
    const { items, done } = await streamOutcome(peers, stream);
    expect(items).toEqual([hex('1'), hex('2')]);
    expect(done).toMatchObject({ items: 2, payload: hex('done') });
  });

  test('the final response passes at a window of 0', async () => {
    const { talk } = await openTalk(peers);
    const { stream } = await peers.client.rpc<{ stream: string }>('talk.stream', {
      talk,
      procedure: 'COUNT',
      text: '1',
      window: 1,
    });
    const { items, done } = await streamOutcome(peers, stream);
    expect(items).toEqual([hex('1')]);
    expect(done).toMatchObject({ items: 1, payload: hex('done') });
  });

  test('cancelling a stream request ends it with CANCELLED', async () => {
    const { talk } = await openTalk(peers);
    const { stream } = await peers.client.rpc<{ stream: string }>('talk.stream', {
      talk,
      procedure: 'COUNT',
      text: '3',
      window: 0,
    });
    await peers.client.rpc('stream.cancel', { stream });
    const { items, done } = await streamOutcome(peers, stream);
    expect(items).toEqual([]);
    expect(done['error']).toMatchObject({ kind: 'status', code: StatusCode.cancelled });
    // The channel carries on.
    await barrier(peers, talk);
  });

  test('an abort response carries the exact code and reason', async () => {
    const { talk } = await openTalk(peers);
    const abort = (text: string): Promise<PeerErrorInfo> =>
      peers.client.rpcError('talk.request', { talk, procedure: 'ABORT', text });
    expect(await abort('5 not here')).toMatchObject({
      kind: 'status',
      code: StatusCode.notFound,
      reason: 'not here',
      body: hex('not here'),
    });
    expect(await abort('13')).toMatchObject({ code: StatusCode.internal, reason: '' });
    expect(await abort('65535 the largest code')).toMatchObject({
      code: 65535,
      reason: 'the largest code',
    });
    expect(await abort('256 an application code')).toMatchObject({
      code: 256,
      reason: 'an application code',
    });
  });

  test('a MOVED and a RELOCATED abort keep their fields byte for byte', async () => {
    const { talk } = await openTalk(peers);
    const moved = wikiBytes('test-vectors/sharding-payloads/moved-owner-0x1a2b-epoch-7');
    const relocated = wikiBytes('test-vectors/sharding-payloads/relocated-owner-0x1a2b-epoch-7');
    for (const status of [moved, relocated]) {
      const error = await peers.client.rpcError('talk.request', {
        talk,
        procedure: 'STATUS',
        payload: hexString(status),
      });
      expect(error).toMatchObject({
        kind: 'status',
        code: (status[0] ?? 0) + (status[1] ?? 0) * 256,
        body: hexString(status.subarray(2)),
      });
    }
  });

  test('a request past its timeout fails with DEADLINE_EXCEEDED, the channel carries on', async () => {
    const { talk } = await openTalk(peers);
    const error = await peers.client.rpcError('talk.request', {
      talk,
      procedure: 'STALL',
      timeoutMs: 200,
    });
    expect(error).toMatchObject({ kind: 'status', code: StatusCode.deadlineExceeded });
    await barrier(peers, talk);
  });

  test('a stream request past its timeout fails with DEADLINE_EXCEEDED', async () => {
    const { talk } = await openTalk(peers);
    const { stream } = await peers.client.rpc<{ stream: string }>('talk.stream', {
      talk,
      procedure: 'STALL',
      timeoutMs: 200,
    });
    const { done } = await streamOutcome(peers, stream);
    expect(done['error']).toMatchObject({ kind: 'status', code: StatusCode.deadlineExceeded });
  });

  test('without EXTEND, a slow reply misses the requester timeout', async () => {
    const { talk } = await openTalk(peers);
    const error = await peers.client.rpcError('talk.request', {
      talk,
      procedure: 'SLOW',
      text: '700',
      timeoutMs: 250,
    });
    expect(error).toMatchObject({ kind: 'status', code: StatusCode.deadlineExceeded });
  });

  test('an EXTEND lets the responder outlive the requester timeout', async () => {
    const { talk } = await openTalk(peers);
    const reply = await peers.client.rpc<MessageInfo>('talk.request', {
      talk,
      procedure: 'EXTEND',
      text: '700',
      timeoutMs: 250,
    });
    expect(reply.payload).toBe(hex('extended'));
    // The deadline on the wire carries the responder's default buffer.
    const extended = await peers.client.events.waitFor({ event: 'talk.extend', talk });
    expect(extended).toMatchObject({ deadlineMs: 2 * 700 + 5000, renewMs: null });
  });

  test('an EXTEND reaches a stream request too', async () => {
    const { talk } = await openTalk(peers);
    const { stream } = await peers.client.rpc<{ stream: string }>('talk.stream', {
      talk,
      procedure: 'EXTEND',
      text: '600',
      timeoutMs: 250,
    });
    const { done } = await streamOutcome(peers, stream);
    expect(done).toMatchObject({ items: 0, payload: hex('extended') });
    expect(await peers.client.events.waitFor({ event: 'talk.extend', stream })).toMatchObject({
      deadlineMs: 2 * 600 + 5000,
    });
  });

  test('plain messages reach the service in order', async () => {
    const { talk } = await openTalk(peers);
    for (const text of ['one', 'two', 'three']) {
      await peers.client.rpc('talk.send', { talk, procedure: 'NOTE', text });
    }
    const messages = await peers.server.events.waitForCount('service.message', 3);
    expect(messages.map((m) => [m['procedure'], m['payload'], m['bulk']])).toEqual([
      ['NOTE', hex('one'), false],
      ['NOTE', hex('two'), false],
      ['NOTE', hex('three'), false],
    ]);
  });
});

describeCells(
  'talk with a short reply timeout on the server',
  { server: { talk: { replyTimeoutMs: 300 } } },
  (peers) => {
    test('a responder past its reply timeout answers ABORT DEADLINE_EXCEEDED', async () => {
      const { talk } = await openTalk(peers);
      const started = Date.now();
      const error = await peers.client.rpcError('talk.request', {
        talk,
        procedure: 'SLOW',
        text: '1500',
      });
      expect(error).toMatchObject({ kind: 'status', code: StatusCode.deadlineExceeded });
      // The responder's abort, well before the requester's own 15 s.
      expect(Date.now() - started).toBeLessThan(1400);
    });

    test("the responder's EXTEND lifts its own reply timeout", async () => {
      const { talk } = await openTalk(peers);
      const reply = await peers.client.rpc<MessageInfo>('talk.request', {
        talk,
        procedure: 'EXTEND',
        text: '700',
      });
      expect(reply.payload).toBe(hex('extended'));
    });
  },
);
