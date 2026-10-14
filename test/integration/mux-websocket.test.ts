/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Two frame mux connections over WebSocket on the loopback: the "real
// transports" WebSocket case of `test/mux_test.dart` with every pairing of
// this package's clients (the global WebSocket, ws) and servers (a plain
// ws socket wrapped, acceptWebSocket), and the WebSocket cases of its
// "transport failures end to end" group.

import { createServer, type IncomingMessage } from 'node:http';
import type { Duplex } from 'node:stream';
import { afterEach, describe, expect, test } from 'vitest';
import { WebSocket, WebSocketServer } from 'ws';
import type { MuxOptions } from '../../src/core/mux/contract.ts';
import { FrameMuxConnection } from '../../src/core/mux/frame-mux-connection.ts';
import { MuxControlMessage, MuxControlType, MuxFrame } from '../../src/core/mux/mux-frame.ts';
import { StatusCode } from '../../src/core/status.ts';
import type { FrameTransport } from '../../src/core/transport/contract.ts';
import {
  connectWebSocket as connectGlobal,
  WebSocketTransport,
} from '../../src/core/transport/websocket-transport.ts';
import { connectWsWebSocket } from '../../src/node/transport/ws-client.ts';
import { wsConnection } from '../../src/node/transport/ws-common.ts';
import { acceptWebSocket, listenWebSocket } from '../../src/node/transport/ws-server.ts';
import { exercise, onCleanup, queue, runCleanups, wide } from './mux-support.ts';

afterEach(runCleanups);

const isBun = typeof (globalThis as { Bun?: unknown }).Bun !== 'undefined';

/** A plain ws server whose sockets are wrapped as transports: Dart's dart:io upgrade and WebSocketTransport.wrap. */
async function plainServer(): Promise<{ url: string; next(): Promise<FrameTransport> }> {
  const http = createServer();
  const wss = new WebSocketServer({
    noServer: true,
    handleProtocols: () => 'switchboard',
  });
  const accepted: FrameTransport[] = [];
  const waiting: ((transport: FrameTransport) => void)[] = [];
  http.on('upgrade', (request: IncomingMessage, socket: Duplex, head: Buffer) => {
    wss.handleUpgrade(request, socket, head, (ws) => {
      const transport = new WebSocketTransport(wsConnection(ws));
      onCleanup(() => transport.abort());
      const waiter = waiting.shift();
      if (waiter === undefined) accepted.push(transport);
      else waiter(transport);
    });
  });
  await new Promise<void>((resolve) => http.listen(0, '127.0.0.1', resolve));
  onCleanup(() => {
    http.closeAllConnections();
    http.close();
  });
  const address = http.address();
  const port = typeof address === 'object' && address !== null ? address.port : 0;
  return {
    url: `ws://127.0.0.1:${port}/`,
    next: () => {
      const transport = accepted.shift();
      return transport !== undefined
        ? Promise.resolve(transport)
        : new Promise((resolve) => waiting.push(resolve));
    },
  };
}

/** A listener of this package (acceptWebSocket over ws). */
async function ownServer(): Promise<{ url: string; next(): Promise<FrameTransport> }> {
  const listener = await listenWebSocket('127.0.0.1', 0);
  onCleanup(() => listener.close());
  const accepted = queue(listener.transports);
  return {
    url: listener.endpoint,
    next: async () => {
      const { transport } = await accepted.next();
      onCleanup(() => transport.abort());
      return transport;
    },
  };
}

const clients = [
  ['the global WebSocket', connectGlobal],
  ['ws', connectWsWebSocket],
] as const;
const servers = [
  ['a wrapped ws socket', plainServer],
  ['acceptWebSocket', ownServer],
] as const;

describe('frame mux over WebSocket', () => {
  for (const [clientName, connect] of clients) {
    for (const [serverName, start] of servers) {
      test(`${clientName} to ${serverName}`, async () => {
        const server = await start();
        const clientTransport = await connect(server.url);
        onCleanup(() => clientTransport.abort());
        const client = new FrameMuxConnection(clientTransport, { ...wide, isInitiator: true });
        const accepted = new FrameMuxConnection(await server.next(), {
          ...wide,
          isInitiator: false,
        });
        await exercise(client, accepted);
      });
    }
  }
});

describe('transport failures end to end over WebSocket', () => {
  const rawOptions: Partial<MuxOptions> = {
    keepAliveInterval: undefined,
    announceLimits: false,
    awaitPeerLimits: false,
  };

  /** A mux behind acceptWebSocket with a 64-byte limit, and a raw ws client of it. */
  async function setUp(): Promise<{ ws: WebSocket; server: Promise<FrameMuxConnection> }> {
    const http = createServer();
    let resolveServer: (mux: FrameMuxConnection) => void = () => {};
    const server = new Promise<FrameMuxConnection>((resolve) => {
      resolveServer = resolve;
    });
    http.on('upgrade', (request: IncomingMessage, socket: Duplex, head: Buffer) => {
      void acceptWebSocket(request, socket, head, { maxFrameSize: 64 }).then(({ transport }) => {
        onCleanup(() => transport.abort());
        resolveServer(new FrameMuxConnection(transport, { ...rawOptions, isInitiator: false }));
      });
    });
    await new Promise<void>((resolve) => http.listen(0, '127.0.0.1', resolve));
    onCleanup(() => {
      http.closeAllConnections();
      http.close();
    });
    const address = http.address();
    const port = typeof address === 'object' && address !== null ? address.port : 0;
    const ws = new WebSocket(`ws://127.0.0.1:${port}/`, ['switchboard']);
    ws.on('error', () => {});
    onCleanup(() => ws.terminate());
    await new Promise((resolve) => ws.once('open', resolve));
    return { ws, server };
  }

  /** The messages [ws] receives until it closes, and its close code. */
  function received(ws: WebSocket): Promise<{ messages: Uint8Array[]; code: number }> {
    const messages: Uint8Array[] = [];
    ws.on('message', (data) => messages.push(new Uint8Array(data as Buffer)));
    return new Promise((resolve) => ws.once('close', (code) => resolve({ messages, code })));
  }

  function goAways(messages: readonly Uint8Array[]): StatusCode[] {
    return messages
      .map((m) => MuxControlMessage.decode(MuxFrame.decode(m).payload))
      .filter((c) => c.knownType === MuxControlType.goAway)
      .map((c) => c.goAwayStatus.code as StatusCode);
  }

  test('a text message: GOAWAY PROTOCOL_ERROR', async () => {
    const { ws, server } = await setUp();
    const outcome = received(ws);
    ws.send('hello');
    expect((await (await server).done).known).toBe(StatusCode.protocolError);
    const { messages, code } = await outcome;
    expect(goAways(messages)).toEqual([StatusCode.protocolError]);
    // Dart's IOWebSocketTransport closes with 1000; this server, like
    // Dart's WebSocketServerTransport, with 1003.
    expect(code).toBe(1003);
  });

  test('a message over the limit: FRAME_TOO_LARGE', async () => {
    const { ws, server } = await setUp();
    const outcome = received(ws);
    ws.send(new Uint8Array(65));
    expect((await (await server).done).known).toBe(StatusCode.frameTooLarge);
    const { messages, code } = await outcome;
    if (isBun) {
      // Bun's stand-in for ws lets the message through to the transport's
      // own check, which leaves room for the GOAWAY.
      expect(goAways(messages)).toEqual([StatusCode.frameTooLarge]);
      return;
    }
    // ws refuses the message as it arrives and closes with 1009 at once, so
    // the GOAWAY FRAME_TOO_LARGE Dart's server sends first never goes out.
    expect(goAways(messages)).toEqual([]);
    expect(code).toBe(1009);
  });
});
