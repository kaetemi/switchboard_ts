/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Protocol errors injected into a live connection, both ways
// (talk_test.dart, talk_window_test.dart, mux_talk_test.dart,
// switchboard_test.dart): a raw channel stands in for one end of a Talk
// channel and sends what the wiki says a receiver must refuse. A Talk
// protocol error closes only its channel with PROTOCOL_ERROR; the
// connection carries on.

import { expect, test } from 'vitest';
import { concatBytes, hexString } from '../../../src/core/bytes.ts';
import { StatusCode } from '../../../src/core/status.ts';
import { wikiVectorsIn } from '../../support/vectors.ts';
import { type CellPeers, describeCells } from '../matrix.ts';
import { name8, utf8 } from '../probe/raw.ts';
import type { MessageInfo } from '../protocol.ts';
import { connect, hex, openTalk, rawTalk } from './support.ts';

/** The Talk frames a receiver must refuse (wiki, "Talk messages"). */
const malformed = wikiVectorsIn('test-vectors/talk-messages/').filter(
  (v) => v.negative && v.hex !== null,
);

/** A request frame: flags 0x03, the procedure, the request id. */
function request(procedure: string, id: number, payload = new Uint8Array(0)): string {
  return hexString(concatBytes([0x03], name8(procedure), [id, 0, 0], payload));
}

/** A raw channel from the client to the server's echo service: the client writes Talk by hand. */
async function rawEcho(peers: CellPeers): Promise<{ connection: string; channel: string }> {
  const connection = await connect(peers);
  const { channel } = await peers.client.rpc<{ channel: string }>('open', {
    connection,
    address: { type: 'echo' },
  });
  return { connection, channel };
}

describeCells('protocol errors', {}, (peers) => {
  test.each(malformed)(
    'the responder refuses "$case": PROTOCOL_ERROR on that channel only',
    async (v) => {
      const { connection, channel } = await rawEcho(peers);
      await peers.client.rpc('channel.send', { channel, hex: v.hex });
      expect(await peers.client.events.waitFor({ event: 'channel.closed', channel })).toMatchObject(
        { code: StatusCode.protocolError },
      );
      // The connection and its other channels carry on.
      await peers.client.rpc('connection.ping', { connection });
      const { talk } = await openTalk(peers, 'echo', connection);
      const reply = await peers.client.rpc<MessageInfo>('talk.request', {
        talk,
        procedure: 'ECHO',
        text: 'after',
      });
      expect(reply.payload).toBe(hex('after'));
    },
  );

  test.each(malformed)(
    'the requester refuses "$case": PROTOCOL_ERROR on that channel only',
    async (v) => {
      const { channel, far, farConnection } = await rawTalk(peers);
      await peers.server.rpc('channel.send', { channel: far, hex: v.hex });
      expect(await peers.client.events.waitFor({ event: 'channel.closed', channel })).toMatchObject(
        {
          code: StatusCode.protocolError,
        },
      );
      expect(
        await peers.server.events.waitFor({ event: 'channel.closed', channel: far }),
      ).toMatchObject({ code: StatusCode.protocolError });
      await peers.server.rpc('connection.ping', { connection: farConnection });
    },
  );

  test('a cancel of a request never made is ignored', async () => {
    const { channel } = await rawEcho(peers);
    await peers.client.rpc('channel.send', { channel, hex: '22 09 00 00 01 00' });
    await peers.client.rpc('channel.send', { channel, hex: request('ECHO', 1, utf8('ok')) });
    expect(await peers.client.events.waitFor({ event: 'channel.data', channel })).toMatchObject({
      hex: '04 01 00 00 6F 6B',
    });
  });

  test('a request id still outstanding, reused: the first request is cancelled', async () => {
    const { channel } = await rawEcho(peers);
    await peers.client.rpc('channel.send', { channel, hex: request('STALL', 1) });
    await peers.client.rpc('channel.send', { channel, hex: request('ECHO', 1, utf8('two')) });
    const data = await peers.client.events.waitFor({ event: 'channel.data', channel });
    expect(data).toMatchObject({ hex: '04 01 00 00 74 77 6F' });
  });

  test('a credit for a request that is not a stream request: PROTOCOL_ERROR', async () => {
    const { channel } = await rawEcho(peers);
    await peers.client.rpc('channel.send', { channel, hex: request('STALL', 1) });
    await peers.client.rpc('channel.send', { channel, hex: '32 01 00 00 01 00 00 00' });
    expect(await peers.client.events.waitFor({ event: 'channel.closed', channel })).toMatchObject({
      code: StatusCode.protocolError,
    });
  });

  test('a credit beyond the largest window: PROTOCOL_ERROR', async () => {
    const { channel } = await rawEcho(peers);
    // COUNT 3 with a window of 0xFFFFFFFE, then a credit of 2 more.
    await peers.client.rpc('channel.send', {
      channel,
      hex: hexString(concatBytes([0x0b], name8('STALL'), [1, 0, 0], [0xfe, 0xff, 0xff, 0xff])),
    });
    await peers.client.rpc('channel.send', { channel, hex: '32 01 00 00 02 00 00 00' });
    expect(await peers.client.events.waitFor({ event: 'channel.closed', channel })).toMatchObject({
      code: StatusCode.protocolError,
    });
  });

  test('a malformed OPEN payload is refused with PROTOCOL_ERROR', async () => {
    const connection = await connect(peers);
    // Reserved address flags.
    const { channel } = await peers.client.rpc<{ channel: string }>('open', {
      connection,
      openHex: '80',
    });
    expect(await peers.client.events.waitFor({ event: 'channel.closed', channel })).toMatchObject({
      code: StatusCode.protocolError,
    });
    await peers.client.rpc('connection.ping', { connection });
  });

  test('an OPEN for a reserved type nobody serves is NOT_FOUND', async () => {
    const connection = await connect(peers);
    const { channel } = await peers.client.rpc<{ channel: string }>('open', {
      connection,
      address: { type: '_nope' },
    });
    expect(await peers.client.events.waitFor({ event: 'channel.closed', channel })).toMatchObject({
      code: StatusCode.notFound,
    });
  });

  test('a response to a request never made is ignored by the requester', async () => {
    const { talk, far } = await rawTalk(peers);
    await peers.server.rpc('channel.send', { channel: far, hex: '04 07 00 00 41' });
    // The channel still works: a request is answered by hand.
    const reply = peers.client.rpc<MessageInfo>('talk.request', {
      talk,
      procedure: 'ECHO',
      text: 'x',
    });
    await peers.server.events.waitFor({ event: 'channel.data', channel: far });
    await peers.server.rpc('channel.send', { channel: far, hex: '04 01 00 00 79' });
    expect((await reply).payload).toBe('79');
  });

  test('an item beyond the window: the requester closes the channel PROTOCOL_ERROR', async () => {
    const { talk, channel, far } = await rawTalk(peers);
    const { stream } = await peers.client.rpc<{ stream: string }>('talk.stream', {
      talk,
      procedure: 'COUNT',
      window: 1,
    });
    await peers.server.events.waitFor({ event: 'channel.data', channel: far });
    await peers.server.rpc('channel.send', { channel: far, hex: '14 01 00 00 31' });
    await peers.server.rpc('channel.send', { channel: far, hex: '14 01 00 00 32' });
    expect(await peers.client.events.waitFor({ event: 'channel.closed', channel })).toMatchObject({
      code: StatusCode.protocolError,
    });
    const done = await peers.client.events.waitFor({ event: 'talk.done', stream });
    expect(done['items']).toBe(1);
  });

  test('a channel abort from the responder fails its requests with the status', async () => {
    const { talk, channel, far } = await rawTalk(peers);
    const pending = peers.client.rpcError('talk.request', { talk, procedure: 'ECHO' });
    await peers.server.events.waitFor({ event: 'channel.data', channel: far });
    await peers.server.rpc('channel.send', { channel: far, hex: '20 10 00' });
    expect(await pending).toMatchObject({ kind: 'status', code: StatusCode.unauthenticated });
    expect(await peers.client.events.waitFor({ event: 'channel.closed', channel })).toMatchObject({
      code: StatusCode.unauthenticated,
    });
  });
});
