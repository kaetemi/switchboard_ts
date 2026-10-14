// Talk over the frame mux over the memory pair: the integration point
// between the two layers. Ports `test/mux_talk_test.dart`.
import { expect, test } from 'vitest';
import { hexBytes } from '../../../src/core/bytes.ts';
import { Status, StatusCode } from '../../../src/core/status.ts';
import { TalkChannel } from '../../../src/core/talk/talk-channel.ts';
import { empty, expectBytes, expectCode, firstIncoming, muxPair, pattern } from './support.ts';

/** Serves every channel of [connection] with Talk, answering each message with [answer]. */
function serve(
  connection: ReturnType<typeof muxPair>[number],
  answer: (payload: Uint8Array) => [Uint8Array, string?],
  talks: TalkChannel[] = [],
): TalkChannel[] {
  void (async () => {
    for await (const channel of connection.incoming) {
      const talk = new TalkChannel(channel);
      talks.push(talk);
      void (async () => {
        try {
          for await (const message of talk.messages) {
            const [payload, procedure] = answer(message.payload);
            void message.reply(payload, procedure === undefined ? {} : { procedure });
          }
        } catch {
          // The channel failed: its done says how.
        }
      })();
    }
  })();
  return talks;
}

test('talk request over a mux channel, close status propagates', async () => {
  const [client, server] = muxPair({ initiator: {}, acceptor: {} });
  serve(server, (payload) => [Uint8Array.of(...payload, 1), 'PONG']);
  const channel = client.open(Uint8Array.of(7));
  const talk = new TalkChannel(channel);
  const reply = await talk.request('PING', Uint8Array.of(2));
  expect(reply.procedureName).toBe('PONG');
  expect([...reply.payload]).toEqual([2, 1]);
  await talk.close(Status.of(StatusCode.goingAway, 'bye'));
  expectCode(await talk.done, StatusCode.goingAway);
  expectCode(await channel.done, StatusCode.goingAway);
  await client.close();
  await server.close();
});

test('talk abort maps application code before mux close', async () => {
  const [client, server] = muxPair({ initiator: {}, acceptor: {} });
  const serverTalkDone = firstIncoming(server).then((c) => new TalkChannel(c).done);
  const talk = new TalkChannel(client.open(empty));
  talk.abort(new Status(300, 'app'));
  const status = await serverTalkDone;
  expect(status.code).toBe(300);
  await client.close();
  await server.close();
});

test('a talk protocol error closes only that channel', async () => {
  const [client, server] = muxPair({ initiator: {}, acceptor: {} });
  const serverTalks = serve(server, () => [Uint8Array.of(1)]);
  const bad = client.open(empty);
  const good = new TalkChannel(client.open(empty));
  const sibling = new TalkChannel(client.open(empty));
  // BULK with an empty bulk reference: a channel protocol error.
  void bad.send(hexBytes('41 48 45 4C 4C 4F 00 00 00'));
  expectCode(await bad.done, StatusCode.protocolError);
  expectCode(await (serverTalks[0] as TalkChannel).done, StatusCode.protocolError);
  expect([...(await good.request('X', empty)).payload]).toEqual([1]);
  expect([...(await sibling.request('Y', empty)).payload]).toEqual([1]);
  expect(client.isOpen).toBe(true);
  expect(server.isOpen).toBe(true);
  await client.close();
  await server.close();
});

test('CONNECTION_LOST goes on the wire as UNAVAILABLE', async () => {
  const [client, server] = muxPair({ initiator: {}, acceptor: {} });
  const serverDone = firstIncoming(server).then((c) => c.done);
  const talk = new TalkChannel(client.open(empty));
  await talk.close(Status.of(StatusCode.connectionLost, 'upstream gone'));
  expectCode(await talk.done, StatusCode.connectionLost);
  const remote = await serverDone;
  expect(remote.known).toBe(StatusCode.unavailable);
  expect(remote.reason).toContain('upstream gone');
  await client.close();
  await server.close();
});

// Not Dart tests: Talk's bulk payloads over the frame mux, which the Talk
// suites run over a fake channel pair. They go through `openAfter`, the
// `adoptIncoming` filter and the `admitAfterGoAway` rule that Talk installs.

test('a payload larger than a frame goes as a bulk payload, both ways', async () => {
  const [client, server] = muxPair();
  serve(server, (payload) => [pattern(payload.length * 2, 31, 7)]);
  const talk = new TalkChannel(client.open(empty));
  const sent = pattern(300000, 31, 3);
  const reply = await talk.request('BIG', sent);
  expectBytes(reply.payload, pattern(600000, 31, 7));
  expect(client.openChannelCount).toBe(1);
  // The bulk channels never reached `incoming`.
  expect(server.openChannelCount).toBe(1);
  await talk.close();
  await client.close();
  await server.close();
});

test('bulk continuations are admitted after GOAWAY, other channels refused', async () => {
  const [client, server] = muxPair();
  serve(server, (payload) => [pattern(payload.length, 31, 9)]);
  const talk = new TalkChannel(client.open(empty));
  expect([...(await talk.request('SMALL', Uint8Array.of(1))).payload]).toEqual([9]);
  const goingAway = server.goAway();
  await client.ping();
  expect(client.peerGoingAway).toBe(true);
  // A new channel is refused, a bulk payload of the open one goes.
  expect(() => client.open(empty)).toThrow();
  const reply = await talk.request('BIG', pattern(200000, 31, 5));
  expectBytes(reply.payload, pattern(200000, 31, 9));
  await talk.close();
  await goingAway;
  expectCode(await client.done, StatusCode.goingAway);
});
