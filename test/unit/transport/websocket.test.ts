/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// The WebSocket binding's client, connectWebSocket, against a plain `ws`
// server on the loopback, and acceptWebSocket against the `ws` client.
// Ports the `WebSocketTransport` and `IOWebSocketTransport` groups of
// `test/transport_test.dart`. The client runs twice: over the global
// WebSocket (the core's connectWebSocket, as in browsers) and over the `ws`
// package (the Node entry's choice when it is installed, Dart's
// IOWebSocketTransport.connect). Dart's server side there is dart:io's
// WebSocket wrapped with WebSocketTransport.wrap; here a `ws` socket
// wrapped with wsConnection.

import { request as httpRequest } from 'node:http';
import { createServer, type Socket } from 'node:net';
import { describe, expect, test } from 'vitest';
import { WebSocket } from 'ws';
import { hexBytes, hexString } from '../../../src/core/bytes.ts';
import { ProtocolError, StatusCode, SwitchboardError } from '../../../src/core/status.ts';
import {
  connectWebSocket as connectGlobal,
  type WebSocketConnectOptions,
  WebSocketTransport,
} from '../../../src/core/transport/websocket-transport.ts';
import { connectWebSocket, connectWsWebSocket } from '../../../src/node/transport/ws-client.ts';
import { wsConnection } from '../../../src/node/transport/ws-common.ts';
import {
  closeCodeOfWs,
  eager,
  Harness,
  isBun,
  onCleanup,
  pattern,
  plainWsServer,
  sameBytes,
  wsMessages,
} from './ws-support.ts';

type Connect = (url: string, options?: WebSocketConnectOptions) => Promise<WebSocketTransport>;

const clients: readonly (readonly [string, Connect])[] = [
  ['the global WebSocket', connectGlobal],
  ['ws', connectWsWebSocket],
];

function wrap(ws: WebSocket, options: { maxFrameSize?: number } = {}): WebSocketTransport {
  return new WebSocketTransport(wsConnection(ws), options);
}

async function rejection(promise: Promise<unknown>): Promise<unknown> {
  try {
    await promise;
  } catch (error) {
    return error;
  }
  throw new Error('expected a rejection');
}

function expectCode(error: unknown, code: StatusCode): void {
  expect(error).toBeInstanceOf(SwitchboardError);
  expect((error as SwitchboardError).code).toBe(code);
}

for (const [name, connect] of clients) {
  describe(`WebSocketTransport over ${name}`, () => {
    test('binary frames both ways and close', async () => {
      const server = await plainWsServer();
      const client = await connect(server.url);
      onCleanup(() => client.abort());
      const ws = await server.accepted.next();
      expect(ws.protocol).toBe('switchboard');
      expect(client.protocol).toBe('switchboard');
      const serverSide = wrap(ws);
      const qc = eager(client);
      const qs = eager(serverSide);
      const writer = client.writable.getWriter();
      void writer.write(hexBytes('02 02 00 01'));
      void writer.write(pattern(200000));
      void serverSide.writable.getWriter().write(hexBytes('02 03 00 02'));
      expect(hexString(await qs.next())).toBe('02 02 00 01');
      expect(sameBytes(await qs.next(), pattern(200000))).toBe(true);
      expect(hexString(await qc.next())).toBe('02 03 00 02');
      const code = closeCodeOfWs(ws);
      await writer.close();
      expect(await qs.hasNext()).toBe(false);
      expect(await qc.hasNext()).toBe(false);
      expect(await code).toBe(WebSocketTransport.normalClosure);
      expect(serverSide.closeCode).toBe(1000);
    });

    test('cancelling a paused stream still observes peer close', async () => {
      const server = await plainWsServer();
      const client = await connect(server.url);
      const ws = await server.accepted.next();
      const reader = client.readable.getReader();
      await new Promise((resolve) => setTimeout(resolve, 5));
      await reader.cancel();
      ws.close();
      let timer: ReturnType<typeof setTimeout> | undefined;
      await Promise.race([
        client.closed,
        new Promise((_, reject) => {
          timer = setTimeout(() => reject(new Error('peer close not observed')), 1000);
        }),
      ]);
      clearTimeout(timer);
    });

    test('a text message is a protocol error', async () => {
      const server = await plainWsServer();
      const client = await connect(server.url);
      const ws = await server.accepted.next();
      const code = closeCodeOfWs(ws);
      const queue = eager(client);
      ws.send(new Uint8Array([2, 2, 0]));
      ws.send('hello');
      expect(hexString(await queue.next())).toBe('02 02 00');
      expect(await rejection(queue.next())).toBeInstanceOf(ProtocolError);
      expect(await queue.hasNext()).toBe(false);
      expect(await code).toBe(WebSocketTransport.normalClosure);
    });

    test('a message over maxFrameSize fails with FRAME_TOO_LARGE', async () => {
      const server = await plainWsServer();
      const client = await connect(server.url, { maxFrameSize: 16 });
      expect(client.maxFrameSize).toBe(16);
      const ws = await server.accepted.next();
      const code = closeCodeOfWs(ws);
      const queue = eager(client);
      ws.send(new Uint8Array(16));
      ws.send(new Uint8Array(17));
      expect(await queue.next()).toHaveLength(16);
      expectCode(await rejection(queue.next()), StatusCode.frameTooLarge);
      expect(await queue.hasNext()).toBe(false);
      // The global WebSocket checks the assembled message and closes with
      // 1000, as Dart's client does; `ws` refuses it before assembly and
      // closes with 1009 itself (Bun's stand-in for `ws` does not, and the
      // transport's own check closes with 1000).
      expect(await code).toBe(name === 'ws' && !isBun ? 1009 : WebSocketTransport.normalClosure);
    });

    test('the default limit is 1 MiB', async () => {
      const server = await plainWsServer();
      const client = await connect(server.url);
      expect(client.maxFrameSize).toBe(1024 * 1024);
      const ws = await server.accepted.next();
      const code = closeCodeOfWs(ws);
      await client.writable.close();
      expect(await code).toBe(1000);
    });

    test('connect timeout: UNAVAILABLE when the handshake stalls', async () => {
      const held: Socket[] = [];
      const silent = createServer((socket) => held.push(socket));
      await new Promise<void>((resolve) => silent.listen(0, '127.0.0.1', resolve));
      onCleanup(() => {
        for (const socket of held) socket.destroy();
        silent.close();
      });
      const address = silent.address();
      const port = typeof address === 'object' && address !== null ? address.port : 0;
      const error = await rejection(connect(`ws://127.0.0.1:${port}/`, { timeout: 50 }));
      expectCode(error, StatusCode.unavailable);
    });

    test('connect timeout: a connection completed late is closed', async () => {
      const slow = await plainWsServer({ delayUpgrade: 100 });
      const error = await rejection(connect(slow.url, { timeout: 20 }));
      expectCode(error, StatusCode.unavailable);
      const ws = await slow.accepted.next();
      expect(await closeCodeOfWs(ws)).toBe(WebSocketTransport.normalClosure);
    });

    test('connect with a timeout in time works as without', async () => {
      const server = await plainWsServer();
      const client = await connect(server.url, { timeout: 5000 });
      const ws = await server.accepted.next();
      const code = closeCodeOfWs(ws);
      await client.writable.close();
      expect(await code).toBe(1000);
    });

    test('connect failure throws', async () => {
      const server = await plainWsServer();
      const port = server.port;
      await new Promise<void>((resolve) => server.server.close(() => resolve()));
      expectCode(await rejection(connect(`ws://127.0.0.1:${port}/`)), StatusCode.unavailable);
    });

    test('an aborted signal rejects with its reason; a late connection is closed', async () => {
      const slow = await plainWsServer({ delayUpgrade: 50 });
      const controller = new AbortController();
      const reason = new Error('stop');
      const attempt = connect(slow.url, { signal: controller.signal });
      // Once the handshake is under way (the server answers it 50 ms late).
      setTimeout(() => controller.abort(reason), 10);
      expect(await rejection(attempt)).toBe(reason);
      const ws = await slow.accepted.next();
      expect(await closeCodeOfWs(ws)).toBe(1000);
      const already = new AbortController();
      already.abort(reason);
      expect(await rejection(connect(slow.url, { signal: already.signal }))).toBe(reason);
    });

    test('a fragment is ignored; a URL of another scheme is a TypeError', async () => {
      const server = await plainWsServer();
      const client = await connect(`${server.url}#npc/1a2b`);
      const ws = await server.accepted.next();
      const code = closeCodeOfWs(ws);
      await client.writable.close();
      expect(await code).toBe(1000);
      expect(await rejection(connect('http://127.0.0.1:1/'))).toBeInstanceOf(TypeError);
    });

    test('output accounting: buffered and accepted bytes', async () => {
      const server = await plainWsServer();
      const client = await connect(server.url);
      onCleanup(() => client.abort());
      const ws = await server.accepted.next();
      const received = wsMessages(ws);
      expect(client.bufferedOutputBytes).toBe(0);
      expect(client.acceptedOutputBytes).toBe(0);
      void client.writable.getWriter().write(pattern(1000));
      await received.next();
      expect(client.acceptedOutputBytes).toBe(1000);
      expect(client.bufferedOutputBytes).toBe(0);
    });
  });
}

describe('the WebSocket capabilities', () => {
  test('over the global WebSocket: no output readiness, no throttling', async () => {
    const server = await plainWsServer();
    const client = await connectGlobal(server.url);
    onCleanup(() => client.abort());
    expect('isOutputReady' in client).toBe(false);
    expect(client.isOutputReady).toBeUndefined();
    expect(client.outputReady).toBeUndefined();
    expect(client.isInputThrottled).toBeUndefined();
  });

  // Bun's stand-in for `ws` cannot pause a server's socket, which is how
  // this case holds the output back.
  test.skipIf(isBun)('over ws: output readiness and throttling', async () => {
    const server = await plainWsServer();
    const client = await connectWsWebSocket(server.url, { outputHighWaterMark: 256 * 1024 });
    onCleanup(() => client.abort());
    const ws = await server.accepted.next();
    expect(client.isOutputReady).toBe(true);
    expect(client.isInputThrottled).toBe(false);
    await client.outputReady;
    // The server stops reading: the output piles up.
    ws.pause();
    const writer = client.writable.getWriter();
    for (let i = 0; i < 64; i++) void writer.write(pattern(256 * 1024, i));
    await writer.ready;
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(client.isOutputReady).toBe(false);
    expect(client.isInputThrottled).toBe(true);
    expect(client.bufferedOutputBytes).toBeGreaterThan(256 * 1024);
    const ready = client.outputReady;
    const received = wsMessages(ws);
    ws.resume();
    await ready;
    for (let i = 0; i < 64; i++) await received.next();
    expect(client.isInputThrottled).toBe(false);
    expect(client.acceptedOutputBytes).toBe(64 * 256 * 1024);
  });

  test('over ws: a frame waiting for its reader pauses the socket', async () => {
    const server = await plainWsServer();
    const client = await connectWsWebSocket(server.url);
    onCleanup(() => client.abort());
    const ws = await server.accepted.next();
    ws.send(new Uint8Array([1]));
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(client.isInputPaused).toBe(true);
    expect(client.isInputStarted).toBe(false);
    const reader = client.readable.getReader();
    expect((await reader.read()).value).toEqual(new Uint8Array([1]));
    expect(client.isInputPaused).toBe(false);
    expect(client.isInputStarted).toBe(true);
  });

  test('abort tears the connection down', async () => {
    const server = await plainWsServer();
    for (const connect of [connectGlobal, connectWsWebSocket]) {
      const client = await connect(server.url);
      const ws = await server.accepted.next();
      const code = closeCodeOfWs(ws);
      client.abort();
      client.abort();
      await client.closed;
      // ws terminates (no close frame: 1006); the global WebSocket closes with 1000.
      expect([1000, 1005, 1006]).toContain(await code);
    }
  });

  test('the Node connectWebSocket prefers ws', async () => {
    const server = await plainWsServer();
    const client = await connectWebSocket(server.url);
    onCleanup(() => client.abort());
    expect(client.isOutputReady).toBe(true);
    const viaGlobal = await connectWebSocket(server.url, {
      webSocket: (url, protocols) =>
        wsConnection(new WebSocket(url, [...protocols], { perMessageDeflate: false })),
    });
    onCleanup(() => viaGlobal.abort());
    expect(viaGlobal.protocol).toBe('switchboard');
  });

  test('options are validated', () => {
    const fake = {
      readyState: 1,
      protocol: '',
      bufferedAmount: 0,
      listen() {},
      send() {},
      close() {},
    };
    expect(() => new WebSocketTransport(fake, { maxFrameSize: -1 })).toThrow(RangeError);
    expect(() => new WebSocketTransport(fake, { closeTimeout: -1 })).toThrow(RangeError);
    expect(() => new WebSocketTransport(fake, { outputHighWaterMark: 1.5 })).toThrow(RangeError);
  });
});

describe('IOWebSocketTransport (acceptWebSocket and the ws client)', () => {
  test('upgrade and connect: frames, subprotocol, limit', async () => {
    const h = await Harness.start({ maxFrameSize: 64 });
    const client = await connectWsWebSocket(h.uri);
    expect(client.maxFrameSize).toBe(1024 * 1024);
    const { transport: serverSide } = await h.accepted.next();
    expect(serverSide.maxFrameSize).toBe(64);
    const qc = eager(client);
    const qs = eager(serverSide);
    const writer = client.writable.getWriter();
    void writer.write(hexBytes('02 02 00 01'));
    void serverSide.writable.getWriter().write(hexBytes('02 03 00 02'));
    expect(hexString(await qs.next())).toBe('02 02 00 01');
    expect(hexString(await qc.next())).toBe('02 03 00 02');
    // Over the server's limit.
    void writer.write(new Uint8Array(65));
    expectCode(await rejection(qs.next()), StatusCode.frameTooLarge);
    expect(await qs.hasNext()).toBe(false);
    expect(await qc.hasNext()).toBe(false);
    await client.closed;
    expect(client.closeCode).toBe(1009);
  });

  test('the switchboard subprotocol is selected', async () => {
    const h = await Harness.start();
    const ws = new WebSocket(h.uri, ['other', 'switchboard']);
    await new Promise((resolve) => ws.once('open', resolve));
    expect(ws.protocol).toBe('switchboard');
    const { transport } = await h.accepted.next();
    const received = eager(transport);
    ws.send(new Uint8Array([2, 2, 0]));
    expect(hexString(await received.next())).toBe('02 02 00');
    ws.close();
    await transport.writable.close();
  });

  test('clients offering no subprotocol are accepted', async () => {
    const h = await Harness.start();
    const ws = new WebSocket(h.uri);
    await new Promise((resolve) => ws.once('open', resolve));
    expect(ws.protocol).toBe('');
    const { transport } = await h.accepted.next();
    expect(transport.protocol).toBeUndefined();
    const received = eager(transport);
    ws.send(new Uint8Array([2, 2, 0]));
    expect(hexString(await received.next())).toBe('02 02 00');
    ws.close();
    await transport.writable.close();
  });

  test('clients offering only other subprotocols are refused', async () => {
    const h = await Harness.start();
    const ws = new WebSocket(h.uri, ['chat']);
    const failed = await new Promise<boolean>((resolve) => {
      ws.once('open', () => resolve(false));
      ws.on('error', () => resolve(true));
    });
    expect(failed).toBe(true);
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(h.refused).toHaveLength(1);
  });

  test('compression is not negotiated by default', async () => {
    const h = await Harness.start();
    const response = await new Promise<{ status: number; headers: Record<string, unknown> }>(
      (resolve, reject) => {
        const request = httpRequest({
          host: '127.0.0.1',
          port: h.port,
          path: '/',
          headers: {
            Connection: 'Upgrade',
            Upgrade: 'websocket',
            'Sec-WebSocket-Version': '13',
            'Sec-WebSocket-Key': 'dGhlIHNhbXBsZSBub25jZQ==',
            'Sec-WebSocket-Protocol': 'switchboard',
            'Sec-WebSocket-Extensions': 'permessage-deflate',
          },
        });
        request.on('upgrade', (res, socket) => {
          socket.destroy();
          resolve({ status: res.statusCode ?? 0, headers: res.headers });
        });
        request.on('error', reject);
        request.end();
      },
    );
    expect(response.status).toBe(101);
    expect(response.headers['sec-websocket-protocol']).toBe('switchboard');
    expect(response.headers['sec-websocket-extensions']).toBeUndefined();
    const { transport } = await h.accepted.next();
    const received = eager(transport);
    expect(await received.hasNext()).toBe(false);
  });
});
