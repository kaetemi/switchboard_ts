/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// The server side of the WebSocket binding, acceptWebSocket over `ws`,
// driven by a raw RFC 6455 client on the loopback. Ports
// `test/web_socket_server_test.dart`. Dart's server reads frames itself;
// here `ws` does, so the cases about Dart's own buffering are not ported,
// and an oversized message is closed by `ws` at once (no last frame before
// the close frame). The "Switchboard listener" group is in
// test/integration/node-websocket.test.ts.

import { describe, expect, test } from 'vitest';
import { WebSocket } from 'ws';
import { utf8Decode, utf8Encode } from '../../../src/core/bytes.ts';
import type { MuxOptions } from '../../../src/core/mux/contract.ts';
import { FrameMuxConnection } from '../../../src/core/mux/frame-mux-connection.ts';
import { ProtocolError, StatusCode, SwitchboardError } from '../../../src/core/status.ts';
import { connectWebSocket } from '../../../src/core/transport/websocket-transport.ts';
import { connectWsWebSocket } from '../../../src/node/transport/ws-client.ts';
import { maxMessageFragments } from '../../../src/node/transport/ws-server.ts';
import {
  clientFrame,
  clientHeader,
  closeCodeOf,
  closeCodeOfWs,
  closePayload,
  eager,
  Harness,
  handshake,
  isBun,
  opBinary,
  opClose,
  opContinuation,
  opPing,
  opPong,
  opText,
  pattern,
  RawClient,
  sameBytes,
  sampleAccept,
  wsMessages,
} from './ws-support.ts';

async function rejection(promise: Promise<unknown>): Promise<unknown> {
  try {
    await promise;
  } catch (error) {
    return error;
  }
  throw new Error('expected a rejection');
}

function isStatusError(error: unknown, code: StatusCode): boolean {
  return error instanceof SwitchboardError && error.code === code;
}

describe('handshake', () => {
  test('a good request is switched with the switchboard protocol', async () => {
    const h = await Harness.start();
    const [client, server] = await h.pair(
      handshake({
        protocols: 'other, switchboard',
        extensions: 'permessage-deflate; client_max_window_bits',
      }),
    );
    expect(client.headers['sec-websocket-accept']).toBe(sampleAccept);
    expect(client.headers['upgrade']?.toLowerCase()).toBe('websocket');
    expect(client.headers['connection']?.toLowerCase()).toBe('upgrade');
    expect(client.headers['sec-websocket-protocol']).toBe('switchboard');
    // No extension is negotiated, compression included.
    expect(client.headers['sec-websocket-extensions']).toBeUndefined();
    expect(server.protocol).toBe('switchboard');
    expect(server.maxFrameSize).toBe(1000);
    expect(server.abort).toBeTypeOf('function');
    expect(server.bufferedOutputBytes).toBe(0);
    expect(server.isOutputReady).toBe(true);
  });

  test('a client offering no subprotocol is accepted', async () => {
    const h = await Harness.start();
    const [client, server] = await h.pair(handshake({ protocols: null }));
    expect(client.headers['sec-websocket-protocol']).toBeUndefined();
    expect(server.protocol).toBeUndefined();
  });

  for (const [name, request, status] of [
    ['missing key', handshake({ key: null }), 400],
    ['malformed key', handshake({ key: 'c2hvcnQ=' }), 400],
    ['wrong version', handshake({ version: '8' }), 426],
    ['missing version', handshake({ version: null }), 426],
    ['other subprotocol', handshake({ protocols: 'chat' }), 400],
    ['not an upgrade', handshake({ upgrade: false }), 426],
  ] as const) {
    test(`${name} is refused with ${status}`, async () => {
      const h = await Harness.start();
      const client = await RawClient.connect(h.port, request);
      expect(client.status).toBe(status);
      if (name === 'wrong version' || name === 'missing version') {
        expect(client.headers['sec-websocket-version']).toBe('13');
      }
      if (name === 'not an upgrade') {
        expect(client.headers['upgrade']).toBe('websocket');
      }
      await new Promise((resolve) => setTimeout(resolve, 10));
      expect(h.refused).toHaveLength(1);
      expect(isStatusError(h.refused[0], StatusCode.invalidArgument)).toBe(true);
    });
  }

  test('an option out of range is a RangeError before the request is touched', async () => {
    const { acceptWebSocket } = await import('../../../src/node/transport/ws-server.ts');
    const untouched = {} as never;
    await expect(
      acceptWebSocket(untouched, untouched, new Uint8Array(0), { maxFrameSize: -1 }),
    ).rejects.toBeInstanceOf(RangeError);
    await expect(
      acceptWebSocket(untouched, untouched, new Uint8Array(0), { pingInterval: 0 }),
    ).rejects.toBeInstanceOf(RangeError);
  });
});

describe('frames', () => {
  async function setUp(): Promise<{
    client: RawClient;
    server: Awaited<ReturnType<Harness['pair']>>[1];
    received: ReturnType<typeof eager>;
  }> {
    const h = await Harness.start({ maxFrameSize: 1024 * 1024 });
    const [client, server] = await h.pair();
    return { client, server, received: eager(server) };
  }

  test('binary messages both ways, every length encoding', async () => {
    const { client, server, received } = await setUp();
    const writer = server.writable.getWriter();
    for (const length of [3, 125, 126, 0xffff, 0x10000, 300000]) {
      client.send(clientFrame(opBinary, pattern(length, length)));
      expect(sameBytes(await received.next(), pattern(length, length))).toBe(true);
      void writer.write(pattern(length, 7));
      const frame = await client.next();
      expect(frame.opcode).toBe(opBinary);
      expect(frame.fin).toBe(true);
      expect(sameBytes(frame.payload, pattern(length, 7))).toBe(true);
    }
    expect(client.maskError).toBeUndefined();
  });

  test('a fragmented message is reassembled, with a ping between fragments', async () => {
    const { client, received } = await setUp();
    client.send(clientFrame(opBinary, [1, 2], { fin: false }));
    client.send(clientFrame(opPing, utf8Encode('mid')));
    client.send(clientFrame(opContinuation, [3, 4], { fin: false }));
    client.send(clientFrame(opContinuation, [5, 6]));
    expect(await received.next()).toEqual(new Uint8Array([1, 2, 3, 4, 5, 6]));
    const pong = await client.frames.next();
    expect(pong.opcode).toBe(opPong);
    expect(utf8Decode(pong.payload)).toBe('mid');
    // Large fragments, and a message after it.
    const big = pattern(900000, 3);
    client.send(clientFrame(opBinary, big.subarray(0, 300000), { fin: false }));
    client.send(clientFrame(opContinuation, big.subarray(300000, 600000), { fin: false }));
    client.send(clientFrame(opContinuation, big.subarray(600000)));
    client.send(clientFrame(opBinary, [9]));
    expect(sameBytes(await received.next(), big)).toBe(true);
    expect(await received.next()).toEqual(new Uint8Array([9]));
  });

  test('frames split across reads', async () => {
    const { client, received } = await setUp();
    const bytes = new Uint8Array([
      ...clientFrame(opBinary, pattern(300, 1), { fin: false }),
      ...clientFrame(opContinuation, pattern(20, 2)),
      ...clientFrame(opPing, [7]),
    ]);
    await client.trickle(bytes);
    expect(await received.next()).toEqual(new Uint8Array([...pattern(300, 1), ...pattern(20, 2)]));
    expect((await client.frames.next()).payload).toEqual(new Uint8Array([7]));
  });

  test('ping is answered, an unsolicited pong ignored', async () => {
    const { client, received } = await setUp();
    client.send(clientFrame(opPing, utf8Encode('hello')));
    const pong = await client.frames.next();
    expect(pong.opcode).toBe(opPong);
    expect(utf8Decode(pong.payload)).toBe('hello');
    client.send(clientFrame(opPing, []));
    expect((await client.frames.next()).payload).toHaveLength(0);
    client.send(clientFrame(opPong, [1, 2]));
    client.send(clientFrame(opBinary, [3]));
    expect(await received.next()).toEqual(new Uint8Array([3]));
  });

  test('a close from the client is echoed and ends the stream', async () => {
    const { client, server, received } = await setUp();
    client.autoClose = false;
    client.send(clientFrame(opBinary, [1]));
    client.send(clientFrame(opClose, closePayload(1000, 'bye')));
    expect(await received.next()).toEqual(new Uint8Array([1]));
    expect(await received.hasNext()).toBe(false);
    const close = await client.next();
    expect(close.opcode).toBe(opClose);
    expect(closeCodeOf(close)).toBe(1000);
    // Nothing is sent after the close frame, and the server closes its side.
    // Once the WebSocket closed, the writable is errored (design section 12,
    // item 11): a write is refused.
    const refused = server.writable.getWriter().write(new Uint8Array([2]));
    await client.closed.promise;
    expect(await client.frames.hasNext()).toBe(false);
    await server.closed;
    expect(isStatusError(await rejection(refused), StatusCode.connectionLost)).toBe(true);
    expect(server.closeCode).toBe(1000);
    expect(server.closeReason).toBe('bye');
  });

  test('a close without a code is echoed without one', async () => {
    const { client, server, received } = await setUp();
    client.autoClose = false;
    client.send(clientFrame(opClose, []));
    const close = await client.next();
    expect(close.opcode).toBe(opClose);
    expect(close.payload).toHaveLength(0);
    expect(await received.hasNext()).toBe(false);
    await server.closed;
    expect(server.closeCode).toBeUndefined();
  });

  test('closing the sink sends close 1000 and waits for the client', async () => {
    const { client, server, received } = await setUp();
    const writer = server.writable.getWriter();
    void writer.write(new Uint8Array([5]));
    const closing = writer.close();
    expect((await client.next()).payload).toEqual(new Uint8Array([5]));
    const close = await client.next();
    expect(close.opcode).toBe(opClose);
    expect(closeCodeOf(close)).toBe(1000);
    // The client echoed it: the closing handshake is over.
    await closing;
    expect(await received.hasNext()).toBe(false);
    // The server closed its side: the client sees the end.
    await client.closed.promise;
  });
});

describe('limits and protocol errors', () => {
  /**
   * Sends [frames] and expects the stream to fail as [check] says and the
   * connection to close with [code]. [byWs]: a violation `ws` finds, which
   * Bun's stand-in for `ws` answers by dropping the connection (the stream
   * ends, no close frame).
   */
  async function expectFailure(
    frames: readonly Uint8Array[],
    check: (error: unknown) => boolean,
    code: number,
    byWs = true,
  ): Promise<void> {
    const h = await Harness.start({ maxFrameSize: 1000 });
    const [client, server] = await h.pair();
    const received = eager(server);
    for (const frame of frames) client.send(frame);
    if (isBun && byWs) {
      expect(await received.hasNext()).toBe(false);
      await client.closed.promise;
      return;
    }
    expect(check(await rejection(received.next()))).toBe(true);
    expect(await received.hasNext()).toBe(false);
    const close = await client.next();
    expect(close.opcode).toBe(opClose);
    expect(closeCodeOf(close)).toBe(code);
    await client.closed.promise;
  }

  const tooLarge = (error: unknown): boolean => isStatusError(error, StatusCode.frameTooLarge);
  const protocol = (error: unknown): boolean => error instanceof ProtocolError;

  test('a single message over the limit: 1009 and FRAME_TOO_LARGE', () =>
    // On Bun the transport's own check of the assembled message finds it.
    expectFailure([clientFrame(opBinary, pattern(1001))], tooLarge, 1009, false));

  // Bun's stand-in for `ws` reads a frame whole before anyone sees it, so
  // the cases about refusing on a header do not apply there.
  test.skipIf(isBun)('a continuation length cannot overflow the message limit', async () => {
    const h = await Harness.start({ maxFrameSize: 1000 });
    const [client, server] = await h.pair();
    const received = eager(server);
    client.send(clientFrame(opBinary, [1], { fin: false }));
    // Only the header: the size is refused before any of the claimed payload.
    client.send(clientHeader(opContinuation, 0x7fffffffffffffffn));
    expect(tooLarge(await rejection(received.next()))).toBe(true);
    expect(closeCodeOf(await client.next())).toBe(1009);
  });

  test.skipIf(isBun)('the limit counts every fragment of a message', async () => {
    const h = await Harness.start({ maxFrameSize: 1000 });
    const [client, server] = await h.pair();
    const received = eager(server);
    // Exactly at the limit in three fragments: fine.
    client.send(clientFrame(opBinary, pattern(400), { fin: false }));
    client.send(clientFrame(opContinuation, pattern(400), { fin: false }));
    client.send(clientFrame(opContinuation, pattern(200)));
    expect(await received.next()).toHaveLength(1000);
    // One byte more over three fragments: refused at the last header.
    client.send(clientFrame(opBinary, pattern(400), { fin: false }));
    client.send(clientFrame(opContinuation, pattern(400), { fin: false }));
    client.send(clientFrame(opContinuation, pattern(201)));
    expect(tooLarge(await rejection(received.next()))).toBe(true);
    expect(closeCodeOf(await client.next())).toBe(1009);
  });

  // Dart's "a message in many small fragments costs about its size", in
  // part: the deliveries are checked, the cost is not. Dart counts its own
  // message buffers (`messageBuffersForTesting`), which hold the fragments
  // together in 64 KiB pieces; `ws` assembles a message itself, keeping a
  // view of the socket's chunk per fragment until the last one (at most
  // `maxMessageFragments` of them), and shows nothing to count.
  test('a message in many small fragments is delivered whole', async () => {
    const h = await Harness.start({ maxFrameSize: 1024 * 1024 });
    const [client, server] = await h.pair();
    const received = eager(server);
    // 3000 fragments of 1 to 600 bytes, 900 kB in all.
    const parts: Uint8Array[] = [];
    for (let i = 0; i < 3000; i++) {
      const fragment = pattern(((i * 97) % 600) + 1, i);
      parts.push(fragment);
      client.send(clientFrame(i === 0 ? opBinary : opContinuation, fragment, { fin: false }));
    }
    // The pong comes once the server has read every fragment before it.
    client.send(clientFrame(opPing, [1]));
    expect((await client.frames.next()).opcode).toBe(opPong);
    client.send(clientFrame(opContinuation, [7]));
    parts.push(new Uint8Array([7]));
    expect(sameBytes(await received.next(), Buffer.concat(parts))).toBe(true);
    // Small fragments then a large one.
    const large = pattern(300000, 9);
    client.send(clientFrame(opBinary, [1], { fin: false }));
    client.send(clientFrame(opContinuation, [2], { fin: false }));
    client.send(clientFrame(opContinuation, large));
    expect(sameBytes(await received.next(), new Uint8Array([1, 2, ...large]))).toBe(true);
    // A message in one frame.
    client.send(clientFrame(opBinary, pattern(3, 5)));
    expect(await received.next()).toEqual(pattern(3, 5));
  });

  // Dart's "a message in more fragments than the cap: 1009 and
  // FRAME_TOO_LARGE". The cap is `ws`'s `maxFragments` (from `ws` 8.21),
  // which closes with 1008 where Dart closes with 1009, and counts a
  // fragment once its payload is read where Dart counts it on its header
  // (the same for the one-byte and empty fragments here). Dart also checks
  // that a message at the cap is held in one buffer, which `ws` does not
  // show. Bun's stand-in for `ws` has no fragment cap.
  test.skipIf(isBun)(
    'a message in more fragments than the cap: 1008 and FRAME_TOO_LARGE',
    async () => {
      const cap = maxMessageFragments;
      const h = await Harness.start({ maxFrameSize: 1024 * 1024 });
      const [client, server] = await h.pair();
      const received = eager(server);
      const fragments = (count: number, fin: boolean): Uint8Array =>
        Buffer.concat(
          Array.from({ length: count }, (_, i) =>
            clientFrame(i === 0 ? opBinary : opContinuation, [i & 0xff], {
              fin: fin && i === count - 1,
            }),
          ),
        );
      // Exactly at the cap, twice: counted per message.
      for (let round = 0; round < 2; round++) {
        client.send(fragments(cap - 1, false));
        client.send(clientFrame(opPing, [round]));
        expect((await client.frames.next()).opcode).toBe(opPong);
        client.send(clientFrame(opContinuation, [(cap - 1) & 0xff]));
        expect(await received.next()).toEqual(Uint8Array.from({ length: cap }, (_, i) => i & 0xff));
      }
      // One more, even empty: refused.
      client.send(fragments(cap, false));
      client.send(clientFrame(opContinuation, []));
      expect(tooLarge(await rejection(received.next()))).toBe(true);
      expect(closeCodeOf(await client.next())).toBe(1008);
    },
  );

  test.skipIf(isBun)('a fragment is refused on its header, before its payload', async () => {
    const h = await Harness.start({ maxFrameSize: 1024 * 1024 });
    const [client, server] = await h.pair();
    const received = eager(server);
    client.send(clientFrame(opBinary, new Uint8Array(1024 * 1024), { fin: false }));
    client.send(clientHeader(opContinuation, 1024n * 1024n, { fin: false }));
    expect(tooLarge(await rejection(received.next()))).toBe(true);
    expect(closeCodeOf(await client.next())).toBe(1009);
    // A header claiming an absurd length is refused the same way.
    const [other, otherServer] = await h.pair();
    const otherReceived = eager(otherServer);
    other.send(clientHeader(opBinary, 1n << 40n));
    expect(tooLarge(await rejection(otherReceived.next()))).toBe(true);
    expect(closeCodeOf(await other.next())).toBe(1009);
  });

  test('after a failure the stream listener can still send a last frame before the close frame', async () => {
    // Dart's case is an oversized message; `ws` closes on that itself
    // before telling anyone, so the failure here is a text message, which
    // the transport finds.
    const h = await Harness.start({ maxFrameSize: 1000 });
    const [client, server] = await h.pair();
    const writer = server.writable.getWriter();
    void (async () => {
      try {
        for await (const _ of server.readable) {
          // Nothing arrives before the failure.
        }
      } catch {
        void writer.write(new Uint8Array([0xee]));
      }
    })();
    client.send(clientFrame(opText, utf8Encode('hello')));
    const last = await client.next();
    expect(last.opcode).toBe(opBinary);
    expect(last.payload).toEqual(new Uint8Array([0xee]));
    expect(closeCodeOf(await client.next())).toBe(1003);
    await writer.close();
  });

  test('an unmasked client frame: 1002', () =>
    expectFailure([clientFrame(opBinary, [1, 2, 3], { mask: false })], protocol, 1002));

  test('a text message: 1003', () =>
    expectFailure([clientFrame(opText, utf8Encode('hello'))], protocol, 1003, false));

  for (const [name, frames] of [
    ['reserved bits (no extension)', [clientFrame(opBinary, [1], { rsv: 0x40 })]],
    ['reserved opcode', [clientFrame(0x3, [1])]],
    ['continuation outside a message', [clientFrame(opContinuation, [1])]],
    [
      'new message inside a fragmented one',
      [clientFrame(opBinary, [1], { fin: false }), clientFrame(opBinary, [2])],
    ],
    ['control frame over 125 bytes', [clientFrame(opPing, new Uint8Array(126))]],
    ['fragmented control frame', [clientFrame(opPing, [1], { fin: false })]],
    ['close with a one byte payload', [clientFrame(opClose, [3])]],
    ['close with a reserved code', [clientFrame(opClose, closePayload(1005))]],
  ] as const) {
    // Bun's stand-in for `ws` takes a new message inside a fragmented one.
    test.skipIf(isBun && name === 'new message inside a fragmented one')(`${name}: 1002`, () =>
      expectFailure(frames, protocol, 1002),
    );
  }

  test('a close reason that is not UTF-8: 1007', () =>
    expectFailure([clientFrame(opClose, [0x03, 0xe8, 0xc3, 0x28])], protocol, 1007));
});

describe('ping interval', () => {
  test('a client that does not answer is dropped', async () => {
    const h = await Harness.start({ pingInterval: 30 });
    const client = await RawClient.connect(h.port);
    const { transport: server } = await h.accepted.next();
    const drained = eager(server);
    const ping = await client.frames.next();
    expect(ping.opcode).toBe(opPing);
    // Never answered: the server aborts after two more intervals.
    const start = performance.now();
    expect(await drained.hasNext()).toBe(false);
    await client.closed.promise;
    expect(performance.now() - start).toBeLessThan(2000);
  });

  test('a stream not listened to yet is neither pinged nor dropped', async () => {
    const h = await Harness.start({ pingInterval: 30 });
    const client = await RawClient.connect(h.port);
    const { transport: server } = await h.accepted.next();
    // Ten intervals without reading: silence says nothing about the client.
    await new Promise((resolve) => setTimeout(resolve, 300));
    expect(client.closed.isSettled).toBe(false);
    expect(client.frames.length).toBe(0);
    client.send(clientFrame(opBinary, [1, 2, 3]));
    const received = eager(server);
    expect(await received.next()).toEqual(new Uint8Array([1, 2, 3]));
    // Pings start once the stream is read.
    const ping = await client.frames.next();
    expect(ping.opcode).toBe(opPing);
    await server.writable.close();
  });

  test('a client that answers stays', async () => {
    const h = await Harness.start({ pingInterval: 30 });
    const client = await RawClient.connect(h.port);
    client.autoPong = true;
    const { transport: server } = await h.accepted.next();
    const received = eager(server);
    // Several pings, each answered: the connection stays.
    for (let i = 0; i < 4; i++) {
      expect((await client.frames.next()).opcode).toBe(opPing);
    }
    client.send(clientFrame(opBinary, [1, 2, 3]));
    expect(await received.next()).toEqual(new Uint8Array([1, 2, 3]));
    void server.writable.getWriter().write(new Uint8Array([4]));
    expect((await client.next()).payload).toEqual(new Uint8Array([4]));
  });
});

describe('interoperability', () => {
  test('ws client: messages, compression refused, client close', async () => {
    const h = await Harness.start({ maxFrameSize: 1024 * 1024 });
    const ws = new WebSocket(h.uri, ['switchboard']);
    await new Promise((resolve) => ws.once('open', resolve));
    expect(ws.protocol).toBe('switchboard');
    // ws offers permessage-deflate by default; it is not accepted.
    expect(ws.extensions).toBe('');
    const { transport: server } = await h.accepted.next();
    const received = eager(server);
    const atClient = wsMessages(ws);
    const big = pattern(900 * 1024, 5);
    ws.send(new Uint8Array([1]));
    ws.send(big);
    expect(await received.next()).toEqual(new Uint8Array([1]));
    expect(sameBytes(await received.next(), big)).toBe(true);
    const writer = server.writable.getWriter();
    void writer.write(new Uint8Array([2]));
    void writer.write(big);
    expect((await atClient.next()).data).toEqual(new Uint8Array([2]));
    expect(sameBytes((await atClient.next()).data as Uint8Array, big)).toBe(true);
    const code = closeCodeOfWs(ws);
    ws.close(1000, 'done');
    expect(await received.hasNext()).toBe(false);
    await server.closed;
    expect(server.closeCode).toBe(1000);
    expect(server.closeReason).toBe('done');
    expect(await code).toBe(1000);
  });

  test('ws client: server close and message limit', async () => {
    const h = await Harness.start({ maxFrameSize: 1024 * 1024 });
    const ws = new WebSocket(h.uri);
    await new Promise((resolve) => ws.once('open', resolve));
    const { transport: server } = await h.accepted.next();
    const code = closeCodeOfWs(ws);
    await server.writable.close();
    expect(await code).toBe(1000);

    const over = new WebSocket(h.uri);
    over.on('error', () => {});
    await new Promise((resolve) => over.once('open', resolve));
    const { transport: overServer } = await h.accepted.next();
    const overCode = closeCodeOfWs(over);
    const received = eager(overServer);
    over.send(new Uint8Array(1024 * 1024 + 1));
    expect(isStatusError(await rejection(received.next()), StatusCode.frameTooLarge)).toBe(true);
    expect(await overCode).toBe(1009);
  });

  test('the global WebSocket through connectWebSocket', async () => {
    const h = await Harness.start({ maxFrameSize: 1024 * 1024 });
    const client = await connectWebSocket(h.uri);
    const { transport: server } = await h.accepted.next();
    expect(server.protocol).toBe('switchboard');
    const received = eager(server);
    const atClient = eager(client);
    const writer = client.writable.getWriter();
    void writer.write(new Uint8Array([2, 2, 0, 1]));
    expect(await received.next()).toEqual(new Uint8Array([2, 2, 0, 1]));
    void server.writable.getWriter().write(new Uint8Array([2, 3, 0, 2]));
    expect(await atClient.next()).toEqual(new Uint8Array([2, 3, 0, 2]));
    await writer.close();
    expect(await received.hasNext()).toBe(false);
    expect(await atClient.hasNext()).toBe(false);
    await server.closed;
    expect(server.closeCode).toBe(1000);
  });

  test('a mux connection over it, both closes give goingAway', async () => {
    const h = await Harness.start({ maxFrameSize: 1024 * 1024 });
    const options: Partial<MuxOptions> = { keepAliveInterval: undefined };
    for (const serverCloses of [false, true]) {
      const client = new FrameMuxConnection(await connectWsWebSocket(h.uri), {
        ...options,
        isInitiator: true,
      });
      const server = new FrameMuxConnection((await h.accepted.next()).transport, {
        ...options,
        isInitiator: false,
      });
      await client.ping();
      await server.ping();
      if (serverCloses) {
        await server.goAway();
        expect((await client.done).code).toBe(StatusCode.goingAway);
      } else {
        await client.goAway();
        expect((await server.done).code).toBe(StatusCode.goingAway);
      }
    }
  });
});
