/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Port of the Dart `test/mux_talk_test.dart`: Talk over mux, the
// integration point between the two layers. The Dart tests run over the
// frame mux on the in-memory transport; these run over the memory mux pair,
// which carries CLOSE statuses and their confirmation as the frame mux
// does, and is what the assertions on the mux side check.

import { expect, test } from 'vitest';
import { hexBytes } from '../../../src/core/bytes.ts';
import { Status, StatusCode } from '../../../src/core/status.ts';
import { TalkChannel } from '../../../src/core/talk/talk-channel.ts';
import { bytes, serve } from '../../support/talk/helpers.ts';
import type { MemoryMuxChannel, MemoryMuxConnection } from '../../support/talk/memory-mux.ts';
import { memoryMuxPair } from '../../support/talk/memory-mux.ts';

const empty = new Uint8Array(0);

/** Hands every channel the peer of [connection] opens to [onChannel], until the connection closes. */
async function acceptAll(
  connection: MemoryMuxConnection,
  onChannel: (channel: MemoryMuxChannel) => void,
): Promise<void> {
  for await (const channel of connection.incoming) {
    onChannel(channel as MemoryMuxChannel);
  }
}

test('talk request over a mux channel, close status propagates', async () => {
  const [client, server] = memoryMuxPair();
  const serverChannels = acceptAll(server, (channel) => {
    const talk = new TalkChannel(channel);
    serve(talk, (message) => message.reply(bytes([...message.payload, 1]), { procedure: 'PONG' }));
  });
  const channel = client.open(bytes([7]));
  const talk = new TalkChannel(channel);
  const reply = await talk.request('PING', bytes([2]));
  expect(reply.procedureName).toBe('PONG');
  expect([...reply.payload]).toEqual([2, 1]);
  await talk.close(Status.of(StatusCode.goingAway, 'bye'));
  expect((await talk.done).known).toBe(StatusCode.goingAway);
  expect((await channel.done).known).toBe(StatusCode.goingAway);
  await client.close();
  await server.close();
  await serverChannels;
});

test('talk abort maps application code before mux close', async () => {
  const [client, server] = memoryMuxPair();
  const serverChannel = server.accept();
  const serverTalkDone = serverChannel.then((c) => new TalkChannel(c).done);
  const talk = new TalkChannel(client.open(empty));
  talk.abort(new Status(300, 'app'));
  const status = await serverTalkDone;
  expect(status.code).toBe(300);
  // Added: the mux CLOSE carries no application code; the memory mux shows
  // the status it carried (the frame mux's bytes are not modelled).
  expect((await (await serverChannel).done).known).toBe(StatusCode.unknown);
  await client.close();
  await server.close();
});

test('a talk protocol error closes only that channel', async () => {
  const [client, server] = memoryMuxPair();
  const serverTalks: TalkChannel[] = [];
  const serverChannels = acceptAll(server, (channel) => {
    const talk = new TalkChannel(channel);
    serverTalks.push(talk);
    void talk.serve((m) => m.reply(bytes([1]))).catch(() => {});
  });
  const bad = client.open(empty);
  const good = new TalkChannel(client.open(empty));
  const sibling = new TalkChannel(client.open(empty));
  // BULK with an empty bulk reference: a channel protocol error.
  void bad.send(hexBytes('41 48 45 4C 4C 4F 00 00 00'));
  expect((await bad.done).known).toBe(StatusCode.protocolError);
  expect((await (serverTalks[0] as TalkChannel).done).known).toBe(StatusCode.protocolError);
  expect([...(await good.request('X', empty)).payload]).toEqual([1]);
  expect([...(await sibling.request('Y', empty)).payload]).toEqual([1]);
  expect(client.isOpen).toBe(true);
  expect(server.isOpen).toBe(true);
  await client.close();
  await server.close();
  await serverChannels;
});

test('CONNECTION_LOST goes on the wire as UNAVAILABLE', async () => {
  const [client, server] = memoryMuxPair();
  const serverDone = server.accept().then((c) => c.done);
  const talk = new TalkChannel(client.open(empty));
  await talk.close(Status.of(StatusCode.connectionLost, 'upstream gone'));
  expect((await talk.done).known).toBe(StatusCode.connectionLost);
  const remote = await serverDone;
  expect(remote.known).toBe(StatusCode.unavailable);
  expect(remote.reason).toContain('upstream gone');
  await client.close();
  await server.close();
});
