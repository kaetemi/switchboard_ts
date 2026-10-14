/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// The basic exchanges in every cell: Talk requests, streams and aborts on the
// echo service, a raw channel both ways and its close, PING and GOAWAY. The
// Dart to Dart cells pin the expected behaviour on the reference.

import { createHash } from 'node:crypto';
import { expect, test } from 'vitest';
import { hexString, utf8Encode } from '../../../src/core/bytes.ts';
import { StatusCode } from '../../../src/core/status.ts';
import { describeCells } from '../matrix.ts';
import type { MessageInfo } from '../protocol.ts';

const hex = (text: string): string => hexString(utf8Encode(text));

function pattern(length: number): Uint8Array {
  const out = new Uint8Array(length);
  for (let i = 0; i < length; i++) {
    out[i] = (i * 31) & 0xff;
  }
  return out;
}

describeCells('basics', {}, (peers) => {
  async function talk(type = 'echo'): Promise<string> {
    const r = await peers.client.rpc<{ talk: string }>('talk.open', {
      endpoint: peers.endpoint,
      address: { type },
    });
    return r.talk;
  }

  test('a request is answered with its payload', async () => {
    const t = await talk();
    const reply = await peers.client.rpc<MessageInfo>('talk.request', {
      talk: t,
      procedure: 'ECHO',
      text: 'hello',
    });
    expect(reply).toEqual({ procedure: '', bulk: false, payload: hex('hello') });
  });

  test('requests on one channel are answered in any order they complete', async () => {
    const t = await talk();
    const replies = await Promise.all(
      ['a', 'b', 'c'].map((text) =>
        peers.client.rpc<MessageInfo>('talk.request', { talk: t, procedure: 'UPPER', text }),
      ),
    );
    expect(replies.map((r) => r.payload)).toEqual([hex('A'), hex('B'), hex('C')]);
  });

  test('a stream request delivers its items, then the final response', async () => {
    const t = await talk();
    const { stream } = await peers.client.rpc<{ stream: string }>('talk.stream', {
      talk: t,
      procedure: 'COUNT',
      text: '3',
    });
    const done = await peers.client.events.waitFor({ event: 'talk.done', stream });
    const items = peers.client.events.filter({ event: 'talk.item', stream });
    expect(items.map((i) => i.payload)).toEqual([hex('1'), hex('2'), hex('3')]);
    expect(done).toMatchObject({ items: 3, payload: hex('done') });
  });

  test('an abort reaches the requester with its code and reason', async () => {
    const t = await talk();
    const error = await peers.client.rpcError('talk.request', {
      talk: t,
      procedure: 'ABORT',
      text: '300 too busy',
    });
    expect(error).toMatchObject({ kind: 'status', code: 300, reason: 'too busy' });
  });

  test('an unknown procedure is UNIMPLEMENTED', async () => {
    const t = await talk();
    const error = await peers.client.rpcError('talk.request', { talk: t, procedure: 'NOPE' });
    expect(error).toMatchObject({ kind: 'status', code: StatusCode.unimplemented });
  });

  test('a channel to a type nobody serves is refused with NOT_FOUND', async () => {
    const t = await talk('nobody');
    const error = await peers.client.rpcError('talk.request', { talk: t, procedure: 'ECHO' });
    expect(error).toMatchObject({ kind: 'status', code: StatusCode.notFound });
  });

  test('a large response arrives whole', async () => {
    const t = await talk();
    const size = 3 * 1024 * 1024 + 7;
    const reply = await peers.client.rpc<MessageInfo>('talk.request', {
      talk: t,
      procedure: 'BIG',
      text: String(size),
      digest: true,
    });
    expect(reply.size).toBe(size);
    expect(reply.sha256).toBe(createHash('sha256').update(pattern(size)).digest('hex'));
  });

  test('a large request arrives whole', async () => {
    const t = await talk();
    const size = 2 * 1024 * 1024 + 3;
    const reply = await peers.client.rpc<MessageInfo>('talk.request', {
      talk: t,
      procedure: 'SIZE',
      patternLength: size,
    });
    const digest = createHash('sha256').update(pattern(size)).digest('hex');
    expect(reply.payload).toBe(hex(`${size} ${digest}`));
  });

  test('a plain message from the responder reaches the requester first', async () => {
    const t = await talk();
    const reply = await peers.client.rpc<MessageInfo>('talk.request', {
      talk: t,
      procedure: 'NOTIFY',
      text: 'note',
    });
    expect(reply.payload).toBe(hex('sent'));
    expect(peers.client.events.filter({ event: 'talk.message', talk: t })).toMatchObject([
      { procedure: 'NOTE', payload: hex('note') },
    ]);
  });

  test('the responder closing the channel fails later requests with its status', async () => {
    const t = await talk();
    const reply = await peers.client.rpc<MessageInfo>('talk.request', {
      talk: t,
      procedure: 'CLOSE',
      text: `${StatusCode.aborted} done here`,
    });
    expect(reply.payload).toBe(hex('closing'));
    const closed = await peers.client.events.waitFor('channel.closed');
    expect(closed).toMatchObject({ code: StatusCode.aborted, reason: 'done here' });
  });

  test('a raw channel carries subframes both ways and its close status', async () => {
    const { connection } = await peers.client.rpc<{ connection: string }>('connect', {
      endpoint: peers.endpoint,
    });
    const { channel } = await peers.client.rpc<{ channel: string }>('open', {
      connection,
      address: { type: 'raw', payload: '0A 0B' },
    });
    const incoming = await peers.server.events.waitFor('channel.incoming');
    expect(incoming).toMatchObject({ service: 'raw', type: 'raw', payload: '0A 0B' });
    const far = incoming.channel as string;

    await peers.client.rpc('channel.send', { channel, hex: '01 02 03' });
    await peers.server.events.waitFor({ event: 'channel.data', channel: far, hex: '01 02 03' });
    await peers.server.rpc('channel.send', { channel: far, hex: 'FF' });
    await peers.client.events.waitFor({ event: 'channel.data', channel, hex: 'FF' });

    await peers.client.rpc('channel.close', {
      channel,
      code: StatusCode.cancelled,
      reason: 'bye',
    });
    const serverSide = await peers.server.events.waitFor({ event: 'channel.closed', channel: far });
    expect(serverSide).toMatchObject({ code: StatusCode.cancelled, reason: 'bye' });
    const clientSide = await peers.client.events.waitFor({ event: 'channel.closed', channel });
    expect(clientSide).toMatchObject({ code: StatusCode.cancelled });
  });

  test('PING is answered', async () => {
    const { connection } = await peers.client.rpc<{ connection: string }>('connect', {
      endpoint: peers.endpoint,
    });
    const { rttMs } = await peers.client.rpc<{ rttMs: number }>('connection.ping', {
      connection,
      payload: '01 02',
    });
    expect(rttMs).toBeGreaterThanOrEqual(0);
  });

  test('GOAWAY from the server reaches the client with GOING_AWAY', async () => {
    const t = await talk();
    const reply = await peers.client.rpc<MessageInfo>('talk.request', {
      talk: t,
      procedure: 'GOAWAY',
    });
    expect(reply.payload).toBe(hex('going'));
    const goAway = await peers.client.events.waitFor('connection.goAway');
    expect(goAway).toMatchObject({ code: StatusCode.goingAway });
  });
});
