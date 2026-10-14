/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Talk byte for byte on a live channel: the client's Talk implementation
// against a raw channel at the server (what the requester puts on the wire,
// and how it reads hand-built responses), and the server's echo service
// against raw frames from the client. The bytes are the wiki's ("Talk
// messages") where it has the case.

import { expect, test } from 'vitest';
import { concatBytes, hexString } from '../../../src/core/bytes.ts';
import { StatusCode } from '../../../src/core/status.ts';
import { wikiBytes } from '../../support/vectors.ts';
import { type CellPeers, describeCells } from '../matrix.ts';
import { le, name8, utf8 } from '../probe/raw.ts';
import type { MessageInfo, TalkItemEvent } from '../protocol.ts';
import { connect, nextSubframe, rawTalk } from './support.ts';

/** A request frame: flags 0x03, the procedure, the request id, the payload. */
function request(procedure: string, id: number, payload = new Uint8Array(0)): string {
  return hexString(concatBytes([0x03], name8(procedure), le(id, 3), payload));
}

/** A raw channel from the client to the server's echo service. */
async function rawEcho(peers: CellPeers): Promise<string> {
  const connection = await connect(peers);
  const { channel } = await peers.client.rpc<{ channel: string }>('open', {
    connection,
    address: { type: 'echo' },
  });
  return channel;
}

/** The [n]th subframe (from 0) the client's raw channel [channel] received. */
async function received(peers: CellPeers, channel: string, n: number): Promise<string> {
  const events = await peers.client.events.waitForCount({ event: 'channel.data', channel }, n + 1);
  return String(events[n]?.['hex']);
}

describeCells('talk on the wire', {}, (peers) => {
  test('the requester: a request and its final response', async () => {
    const { talk, far } = await rawTalk(peers);
    const reply = peers.client.rpc<MessageInfo>('talk.request', {
      talk,
      procedure: 'ECHO',
      text: 'hi',
    });
    expect(await nextSubframe(peers, far, 0)).toBe(request('ECHO', 1, utf8('hi')));
    await peers.server.rpc('channel.send', {
      channel: far,
      hex: hexString(
        wikiBytes('test-vectors/talk-messages/final-response-to-1-no-procedure-payload-ff'),
      ),
    });
    expect(await reply).toEqual({ procedure: '', bulk: false, payload: 'FF' });
  });

  test('the requester: request ids count from 1', async () => {
    const { talk, far } = await rawTalk(peers);
    for (const text of ['a', 'b', 'c']) {
      void peers.client.rpc('talk.request', { talk, procedure: 'ECHO', text }).catch(() => {});
    }
    const frames = await peers.server.events.waitForCount(
      { event: 'channel.data', channel: far },
      3,
    );
    expect(frames.map((f) => f['hex'])).toEqual([
      request('ECHO', 1, utf8('a')),
      request('ECHO', 2, utf8('b')),
      request('ECHO', 3, utf8('c')),
    ]);
  });

  test('the requester: a plain message', async () => {
    const { talk, far } = await rawTalk(peers);
    await peers.client.rpc('talk.send', { talk, procedure: 'HELLO', payload: '01' });
    expect(await nextSubframe(peers, far, 0)).toBe(
      hexString(wikiBytes('test-vectors/talk-messages/plain-message-hello-payload-01')),
    );
  });

  test('the requester: a stream request with a window, its credit, items and the end', async () => {
    const { talk, far } = await rawTalk(peers);
    const { stream } = await peers.client.rpc<{ stream: string }>('talk.stream', {
      talk,
      procedure: 'LIST',
      window: 16,
    });
    expect(await nextSubframe(peers, far, 0)).toBe(
      hexString(wikiBytes('test-vectors/talk-messages/stream-request-list-id-2-window-16')).replace(
        '02 00 00',
        '01 00 00',
      ),
    );
    await peers.server.rpc('channel.send', {
      channel: far,
      hex: '15 49 54 45 4D 00 00 00 00 01 00 00 07',
    });
    const item = await peers.client.events.waitFor<TalkItemEvent>({ event: 'talk.item', stream });
    expect(item).toMatchObject({ procedure: 'ITEM', payload: '07' });
    await peers.client.rpc('stream.grant', { stream, items: 8 });
    expect(await nextSubframe(peers, far, 1)).toBe(
      hexString(
        wikiBytes('test-vectors/talk-messages/credit-for-own-stream-request-2-8-more-items'),
      ).replace('02 00 00', '01 00 00'),
    );
    await peers.server.rpc('channel.send', { channel: far, hex: '04 01 00 00' });
    expect(await peers.client.events.waitFor({ event: 'talk.done', stream })).toMatchObject({
      items: 1,
      payload: '',
    });
  });

  test('the requester: an unlimited stream request', async () => {
    const { talk, far } = await rawTalk(peers);
    await peers.client.rpc('talk.stream', { talk, procedure: 'LIST' });
    expect(await nextSubframe(peers, far, 0)).toBe(
      hexString(
        wikiBytes('test-vectors/talk-messages/stream-request-list-id-2-unlimited-window'),
      ).replace('02 00 00', '01 00 00'),
    );
  });

  test('the requester: a cancel', async () => {
    const { talk, far } = await rawTalk(peers);
    const { stream } = await peers.client.rpc<{ stream: string }>('talk.stream', {
      talk,
      procedure: 'LIST',
    });
    await nextSubframe(peers, far, 0);
    await peers.client.rpc('stream.cancel', { stream });
    expect(await nextSubframe(peers, far, 1)).toBe(
      hexString(wikiBytes('test-vectors/talk-messages/cancel-own-request-2')).replace(
        '02 00 00',
        '01 00 00',
      ),
    );
  });

  test('the requester: a timeout sends a cancel', async () => {
    const { talk, far } = await rawTalk(peers);
    const error = await peers.client.rpcError('talk.request', {
      talk,
      procedure: 'X',
      timeoutMs: 200,
    });
    expect(error).toMatchObject({ code: StatusCode.deadlineExceeded });
    const cancel = await nextSubframe(peers, far, 1);
    // A cancel (kind 2 with the request id) of request 1.
    expect(cancel.slice(0, 11)).toBe('22 01 00 00');
  });

  test('the requester: an abort response, the EXTENDs it reports', async () => {
    const { talk, far } = await rawTalk(peers);
    const pending = peers.client.rpcError('talk.request', { talk, procedure: 'GET' });
    await nextSubframe(peers, far, 0);
    for (const id of [
      'test-vectors/talk-messages/extend-response-to-1-deadline-900000-ms-renew-300000-ms',
      'test-vectors/talk-messages/extend-response-to-1-deadline-900000-ms-renew-unchanged',
      'test-vectors/talk-messages/extend-response-to-1-restart-the-default-timeout',
    ]) {
      await peers.server.rpc('channel.send', { channel: far, hex: hexString(wikiBytes(id)) });
    }
    await peers.server.rpc('channel.send', {
      channel: far,
      hex: hexString(wikiBytes('test-vectors/talk-messages/abort-response-to-1-not-found-nope')),
    });
    expect(await pending).toMatchObject({
      kind: 'status',
      code: StatusCode.notFound,
      reason: 'nope',
    });
    const extends_ = peers.client.events.filter({ event: 'talk.extend', talk });
    expect(extends_.map((e) => [e['deadlineMs'], e['renewMs']])).toEqual([
      [900_000, 300_000],
      [900_000, null],
      [null, null],
    ]);
  });

  test('the requester: a chained response is a request back to it', async () => {
    const { talk, far } = await rawTalk(peers);
    const pending = peers.client.rpc<MessageInfo>('talk.request', { talk, procedure: 'GET' });
    await nextSubframe(peers, far, 0);
    await peers.server.rpc('channel.send', {
      channel: far,
      hex: hexString(
        wikiBytes('test-vectors/talk-messages/chained-response-to-1-that-is-request-7-more'),
      ),
    });
    expect(await pending).toEqual({ procedure: 'MORE', bulk: false, payload: '' });
  });

  test('the responder: ECHO, COUNT with a window and its credit, ABORT, EXTEND', async () => {
    const channel = await rawEcho(peers);
    await peers.client.rpc('channel.send', { channel, hex: request('ECHO', 1, utf8('hi')) });
    expect(await received(peers, channel, 0)).toBe('04 01 00 00 68 69');
    await peers.client.rpc('channel.send', {
      channel,
      hex: hexString(concatBytes([0x0b], name8('COUNT'), le(2, 3), le(1, 4), utf8('2'))),
    });
    expect(await received(peers, channel, 1)).toBe('14 02 00 00 31');
    await peers.client.rpc('channel.send', { channel, hex: '32 02 00 00 01 00 00 00' });
    expect(await received(peers, channel, 2)).toBe('14 02 00 00 32');
    expect(await received(peers, channel, 3)).toBe('04 02 00 00 64 6F 6E 65');
    await peers.client.rpc('channel.send', { channel, hex: request('ABORT', 3, utf8('5 nope')) });
    expect(await received(peers, channel, 4)).toBe(
      hexString(wikiBytes('test-vectors/talk-messages/abort-response-to-1-not-found-nope')).replace(
        '24 01',
        '24 03',
      ),
    );
    await peers.client.rpc('channel.send', { channel, hex: request('EXTEND', 4, utf8('100')) });
    // A declared deadline of 200 ms, plus the default buffer of 5000 ms.
    expect(await received(peers, channel, 5)).toBe(
      hexString(concatBytes([0x34], le(4, 3), le(5200, 4), le(0, 4))),
    );
    expect(await received(peers, channel, 6)).toBe(
      hexString(concatBytes([0x04], le(4, 3), utf8('extended'))),
    );
  });

  test('the responder: an unknown procedure is ABORT UNIMPLEMENTED without a reason', async () => {
    const channel = await rawEcho(peers);
    await peers.client.rpc('channel.send', { channel, hex: request('NOPE', 1) });
    expect(await received(peers, channel, 0)).toBe('24 01 00 00 0C 00');
  });

  test('the responder: a cancel is answered ABORT CANCELLED at once', async () => {
    const channel = await rawEcho(peers);
    await peers.client.rpc('channel.send', { channel, hex: request('STALL', 1) });
    await peers.client.rpc('channel.send', { channel, hex: '22 01 00 00 01 00' });
    await peers.client.rpc('channel.send', { channel, hex: request('ECHO', 2, utf8('ok')) });
    expect(await received(peers, channel, 0)).toBe(
      hexString(
        concatBytes([0x24, 1, 0, 0], le(StatusCode.cancelled, 2), utf8('cancelled by requester')),
      ),
    );
    expect(await received(peers, channel, 1)).toBe('04 02 00 00 6F 6B');
  });

  test('the responder: a plain message is no request', async () => {
    const channel = await rawEcho(peers);
    await peers.client.rpc('channel.send', {
      channel,
      hex: hexString(wikiBytes('test-vectors/talk-messages/plain-message-hello-payload-01')),
    });
    expect(await peers.server.events.waitFor('service.message')).toMatchObject({
      procedure: 'HELLO',
      bulk: false,
      payload: '01',
    });
  });

  test('the responder: GOAWAY after its reply', async () => {
    const channel = await rawEcho(peers);
    await peers.client.rpc('channel.send', { channel, hex: request('GOAWAY', 1) });
    expect(await received(peers, channel, 0)).toBe(
      hexString(concatBytes([0x04, 1, 0, 0], utf8('going'))),
    );
    expect(await peers.client.events.waitFor('connection.goAway')).toMatchObject({
      code: StatusCode.goingAway,
    });
  });

  test('the responder: a bulk response announces its bulk channel', async () => {
    const channel = await rawEcho(peers);
    const n = 40_000;
    await peers.client.rpc('channel.send', { channel, hex: request('BIG', 1, utf8(String(n))) });
    // A final response with the BULK flag and a reference: bulk number 1, length n.
    expect(await received(peers, channel, 0)).toBe(
      hexString(concatBytes([0x44], le(1, 3), le(1, 4), le(n, 6))),
    );
  });
});
