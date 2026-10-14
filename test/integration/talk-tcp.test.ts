/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Talk over the frame mux over TCP on the loopback (connectTcp and
// listenTcp): the group 'over TCP' of the Dart `test/talk_bulk_test.dart`,
// the four tests of 'over memory' (in talk-frame-mux-bulk.test.ts) over a
// real socket, where frames cross in the pieces the socket cuts them into
// and the stream decoder queues what one read brings; then the
// `mux_talk_test` cases and a stalled reader over TCP. The Dart `Peers`
// connects the same way: `StreamTransport.connectTcp` on one side, the
// listener's socket on the other.

import { describe, expect, onTestFinished, test } from 'vitest';
import { bytesEqual, hexBytes } from '../../src/core/bytes.ts';
import { Status, StatusCode } from '../../src/core/status.ts';
import type { TalkMessage } from '../../src/core/talk/contract.ts';
import { TalkChannel } from '../../src/core/talk/talk-channel.ts';
import { TalkKind } from '../../src/core/talk/talk-frame.ts';
import type { FrameTransport } from '../../src/core/transport/contract.ts';
import { Deferred } from '../../src/core/util/deferred.ts';
import { connectTcp, listenTcp } from '../../src/node/transport/tcp.ts';
import { collect, generated, pattern, until } from '../support/talk/bulk-peers.ts';
import { FrameMuxPeers, nextIncoming } from '../support/talk/frame-mux-peers.ts';
import { bytes, Collector, expectBytes, rawOf, serve } from '../support/talk/helpers.ts';

const empty = new Uint8Array(0);

/** Two transports connected over TCP on the loopback, the connecting side first; aborted after the test. */
async function tcpTransports(): Promise<[FrameTransport, FrameTransport]> {
  const listener = await listenTcp('127.0.0.1', 0);
  const client = await connectTcp('127.0.0.1', listener.port);
  const iterator = listener.transports[Symbol.asyncIterator]();
  const next = await iterator.next();
  await listener.close();
  if (next.done === true) {
    throw new Error('no connection accepted');
  }
  const server = next.value.transport;
  onTestFinished(() => {
    client.abort();
    server.abort();
  });
  return [client, server];
}

/** Peers over TCP, as the Dart `Peers.connect(tcp: true)`. */
async function overTcp(): Promise<FrameMuxPeers> {
  return FrameMuxPeers.connect({ transports: await tcpTransports() });
}

describe('over TCP', () => {
  test('a request with a bulk payload of known length', async () => {
    const peers = await overTcp();
    const [talk, server] = await peers.open();
    const checks: unknown[] = [];
    serve(server, (m) => {
      checks.push(m.isBulk, m.bulkLength);
      void m.reply(bytes([Math.floor(m.payload.length / 10_000)]));
      checks.push(bytesEqual(m.payload, pattern(300_000)));
    });
    const answer = await talk.request('PUT', empty, {
      bulk: generated(300_000),
      bulkLength: 300_000,
    });
    expect([...answer.payload]).toEqual([30]);
    expect(answer.isBulk).toBe(false);
    expect(checks).toEqual([true, 300_000, true]);
  });

  test('a request with a bulk payload of unknown length', async () => {
    const peers = await overTcp();
    const [talk, server] = await peers.open();
    const checks: unknown[] = [];
    serve(server, (m) => {
      checks.push(m.bulkLength);
      void m.reply(m.payload.subarray(0, 3));
      checks.push(bytesEqual(m.payload, pattern(123_457)));
    });
    const answer = await talk.request('PUT', empty, {
      bulk: generated(123_457, { piece: 777 }),
    });
    expect([...answer.payload]).toEqual([0, 1, 2]);
    expect(checks).toEqual([undefined, true]);
  });

  test('a response, stream items and a plain message with bulk payloads', async () => {
    const peers = await overTcp();
    const [talk, server] = await peers.open({
      options: { streamBulk: (m) => m.frame.kind === TalkKind.streamItem },
    });
    const plain = new Deferred<TalkMessage>();
    serve(server, async (m) => {
      switch (m.procedureName) {
        case 'GET':
          await m.reply(empty, { bulk: generated(200_000), bulkLength: 200_000 });
          break;
        case 'LIST':
          for (let i = 0; i < 3; i++) {
            void m.replyItem(empty, { bulk: generated(70_000 + i) });
          }
          void m.reply(bytes([9]));
          break;
        case 'NOTE':
          plain.resolve(m);
          break;
      }
    });
    const got = await talk.request('GET', empty);
    expect(got.isBulk).toBe(true);
    expect(got.bulkLength).toBe(200_000);
    expectBytes(got.payload, pattern(200_000));
    const stream = talk.streamRequest('LIST', empty);
    const lengths: number[] = [];
    for await (const item of stream.items) {
      expect(item.isBulk).toBe(true);
      // Streamed: payload is not there before it is read.
      expect(() => item.payload).toThrow(Error);
      const received = await collect(item.bulk);
      expectBytes(received, pattern(received.length));
      lengths.push(received.length);
    }
    expect(lengths).toEqual([70_000, 70_001, 70_002]);
    expect([...(await stream.done).payload]).toEqual([9]);
    void talk.send('NOTE', empty, { bulk: [pattern(50_000)] });
    const note = await plain.promise;
    expect(note.isBulk).toBe(true);
    expectBytes(note.payload, pattern(50_000));
  });

  test('a payload too large for a frame goes bulk on its own', async () => {
    const peers = await overTcp();
    const [talk, server] = await peers.open();
    const seen: boolean[] = [];
    serve(server, (m) => {
      seen.push(m.isBulk);
      void m.reply(Uint8Array.from(m.payload).reverse());
    });
    const small = bytes([1, 2, 3]);
    const big = pattern(100_000);
    const limit = rawOf(talk).maxSubframeLength;
    // Just inline: the frame header (1 + 8 + 3 bytes) and the payload.
    const edge = pattern(limit - 12);
    const answers: TalkMessage[] = [];
    for (const payload of [small, big, edge, pattern(limit - 11)]) {
      answers.push(await talk.request('ECHO', payload));
    }
    const [smallAnswer, bigAnswer, edgeAnswer, overAnswer] = answers as [
      TalkMessage,
      TalkMessage,
      TalkMessage,
      TalkMessage,
    ];
    expect(seen).toEqual([false, true, false, true]);
    expect([...smallAnswer.payload]).toEqual([3, 2, 1]);
    expect(bigAnswer.isBulk).toBe(true);
    expectBytes(bigAnswer.payload, Uint8Array.from(big).reverse());
    expect(edgeAnswer.isBulk).toBe(false);
    // The answer's header is 8 bytes shorter (no procedure, a response id
    // instead of a request id): inline.
    expect(overAnswer.isBulk).toBe(false);
    expect(overAnswer.payload).toHaveLength(limit - 11);
  });
});

describe('mux_talk_test over TCP', () => {
  test('talk request over a mux channel, close status propagates', async () => {
    const peers = await overTcp();
    const channel = peers.client.open(bytes([7]));
    const talk = new TalkChannel(channel);
    const server = await peers.acceptTalk();
    serve(server, (message) =>
      message.reply(bytes([...message.payload, 1]), { procedure: 'PONG' }),
    );
    const reply = await talk.request('PING', bytes([2]));
    expect(reply.procedureName).toBe('PONG');
    expect([...reply.payload]).toEqual([2, 1]);
    await talk.close(Status.of(StatusCode.goingAway, 'bye'));
    expect((await talk.done).known).toBe(StatusCode.goingAway);
    expect((await channel.done).known).toBe(StatusCode.goingAway);
  });

  test('talk abort maps application code before mux close', async () => {
    const peers = await overTcp();
    const talk = new TalkChannel(peers.client.open(empty));
    const serverChannel = await nextIncoming(peers.server);
    const serverTalk = new TalkChannel(serverChannel);
    talk.abort(new Status(300, 'app'));
    expect((await serverTalk.done).code).toBe(300);
    expect((await serverChannel.done).known).toBe(StatusCode.unknown);
  });

  test('a talk protocol error closes only that channel', async () => {
    const peers = await overTcp();
    const bad = peers.client.open(empty);
    const badFar = new TalkChannel(await nextIncoming(peers.server));
    void badFar.serve(() => {}).catch(() => {});
    const good = new TalkChannel(peers.client.open(empty));
    const goodFar = new TalkChannel(await nextIncoming(peers.server));
    serve(goodFar, (m) => m.reply(bytes([1])));
    // BULK with an empty bulk reference: a channel protocol error.
    void bad.send(hexBytes('41 48 45 4C 4C 4F 00 00 00'));
    expect((await bad.done).known).toBe(StatusCode.protocolError);
    expect((await badFar.done).known).toBe(StatusCode.protocolError);
    expect([...(await good.request('X', empty)).payload]).toEqual([1]);
    expect(peers.client.isOpen).toBe(true);
    expect(peers.server.isOpen).toBe(true);
  });

  test('CONNECTION_LOST goes on the wire as UNAVAILABLE', async () => {
    const peers = await overTcp();
    const talk = new TalkChannel(peers.client.open(empty));
    const serverChannel = await nextIncoming(peers.server);
    await talk.close(Status.of(StatusCode.connectionLost, 'upstream gone'));
    expect((await talk.done).known).toBe(StatusCode.connectionLost);
    const remote = await serverChannel.done;
    expect(remote.known).toBe(StatusCode.unavailable);
    expect(remote.reason).toContain('upstream gone');
  });
});

describe('flow control over TCP', () => {
  test('a paused messages stream stalls its channel at the window, and only it; resuming drains', async () => {
    const peers = await overTcp();
    const [talk, server] = await peers.open();
    const [other, otherServer] = await peers.open();
    const received = new Collector(server.messages, { paused: true });
    serve(otherServer, (m) => m.reply(m.payload));
    let sent = 0;
    for (let i = 0; i < 200; i++) {
      void talk.send('MSG', new Uint8Array(1000)).then(() => {
        sent++;
      });
    }
    await until(() => sent > 0);
    await new Promise((resolve) => setTimeout(resolve, 50));
    // The window is spent: what the server holds is bounded by it.
    expect(received.values).toHaveLength(0);
    expect(sent).toBeLessThan(70);
    expect(rawOf(server).heldBytes).toBeLessThanOrEqual(64 * 1024);
    const echo = await other.request('ECHO', bytes([7]));
    expect([...echo.payload]).toEqual([7]);
    received.resume();
    await until(() => received.values.length === 200);
    expect(sent).toBe(200);
    await received.cancel();
  });
});
