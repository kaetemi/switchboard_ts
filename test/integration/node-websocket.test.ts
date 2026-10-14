/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Nodes end to end over WebSocket on the loopback: listenWebSocket and
// acceptWebSocket of the Node node, its ws and wss dialer, and the web
// entry's connect-only node. Ports the "Switchboard listener" group of
// `test/web_socket_server_test.dart`. Raw channels; the Talk cases wait
// for the Talk layer of the node.

import { createServer, request as httpRequest } from 'node:http';
import { describe, expect, test } from 'vitest';
import { ChannelAddress } from '../../src/core/address/index.ts';
import type { MuxConnection } from '../../src/core/mux/contract.ts';
import { MuxControlMessage, MuxControlType, MuxFrame } from '../../src/core/mux/mux-frame.ts';
import { Name } from '../../src/core/name.ts';
import { StatusCode } from '../../src/core/status.ts';
import { ChannelPolicies } from '../../src/core/switchboard/channel-policy.ts';
import { EndpointResolver } from '../../src/core/switchboard/resolver.ts';
import { Switchboard as WebSwitchboard } from '../../src/web/switchboard.ts';
import { rejectsWith, tagged, tagOf } from '../support/node/harness.ts';
import {
  clientFrame,
  clientHeader,
  closeCodeOf,
  handshake,
  isBun,
  opBinary,
  opClose,
  opContinuation,
  RawClient,
  sampleAccept,
} from '../unit/transport/ws-support.ts';
import { echo, node, onCleanup, roundTrip } from './node-support.ts';

const svc = Name.of('svc');

function portOf(uri: string): number {
  return Number(new URL(uri).port);
}

describe('two nodes over WebSocket', () => {
  test('a client opens a channel to a service of a server', async () => {
    const server = node();
    server.registerService('echo', echo);
    const endpoint = await server.listenWebSocket('127.0.0.1', 0, { path: '/mesh' });
    expect(endpoint).toMatch(/^ws:\/\/127\.0\.0\.1:\d+\/mesh$/);
    expect(server.isOwnEndpoint(endpoint)).toBe(true);
    expect(server.isOwnEndpoint(`${endpoint}/`)).toBe(true);
    expect(server.isOwnEndpoint(endpoint.replace('/mesh', '/other'))).toBe(false);
    const client = node({ resolver: new EndpointResolver(endpoint) });
    const channel = await client.openChannel('echo');
    expect(await roundTrip(channel, 'hello')).toBe('hello');
    await channel.close();
  });

  test.todo('Talk requests over WebSocket: the node Talk layer is not wired yet');

  test('a fragment names the service, not the endpoint; a query is kept', async () => {
    const server = node();
    server.registerService(svc, tagged('svc'));
    const endpoint = await server.listenWebSocket('127.0.0.1', 0);
    const client = node();
    const channel = await client.openChannelAt(
      `${endpoint}?token=1#svc`,
      new ChannelAddress({ type: svc }),
    );
    expect(await tagOf(channel)).toBe('svc');
  });

  test('the accepted side sees the peer address', async () => {
    const server = node();
    const remotes: (string | undefined)[] = [];
    server.registerService(svc, (incoming) => {
      remotes.push(incoming.remote);
      tagged('svc')(incoming);
    });
    const endpoint = await server.listenWebSocket('127.0.0.1', 0);
    const client = node();
    expect(
      await tagOf(await client.openChannelAt(endpoint, new ChannelAddress({ type: svc }))),
    ).toBe('svc');
    expect(remotes[0]).toMatch(/^127\.0\.0\.1:\d+$/);
  });

  test('the listener policy applies', async () => {
    const server = node();
    server.registerService(svc, tagged('svc'));
    const endpoint = await server.listenWebSocket('127.0.0.1', 0, {
      policy: ChannelPolicies.denyAll,
    });
    const client = node();
    expect(
      await tagOf(await client.openChannelAt(endpoint, new ChannelAddress({ type: svc }))),
    ).toBe('closed permissionDenied');
  });

  test('closing the server: GOAWAY, then the listener refuses', async () => {
    const server = node();
    server.registerService(svc, () => {});
    const endpoint = await server.listenWebSocket('127.0.0.1', 0);
    const client = node();
    const connection = await client.connect(endpoint);
    await server.close();
    expect((await connection.done).known).toBe(StatusCode.goingAway);
    await rejectsWith(client.connect(endpoint), StatusCode.unavailable);
  });

  test('a wss endpoint that answers no TLS is UNAVAILABLE', async () => {
    const server = node();
    const endpoint = await server.listenWebSocket('127.0.0.1', 0);
    const client = node({ connectTimeout: 2000 });
    await rejectsWith(client.connect(endpoint.replace('ws:', 'wss:')), StatusCode.unavailable);
  });
});

describe('Switchboard listener', () => {
  test('upgrade requests for another path get 403', async () => {
    const n = node();
    const uri = await n.listenWebSocket('127.0.0.1', 0, { path: '/mesh' });
    const other = await RawClient.connect(portOf(uri), handshake({ path: '/other' }));
    expect(other.status).toBe(403);
    const good = await RawClient.connect(portOf(uri), handshake({ path: '/mesh' }));
    expect(good.status).toBe(101);
    expect(good.headers['sec-websocket-accept']).toBe(sampleAccept);
    // Only other subprotocols, no upgrade headers, a bad version.
    for (const [request, status] of [
      [handshake({ path: '/mesh', protocols: 'chat' }), 400],
      [handshake({ path: '/mesh', upgrade: false }), 426],
      [handshake({ path: '/mesh', version: '12' }), 426],
    ] as const) {
      const refused = await RawClient.connect(portOf(uri), request);
      expect(refused.status).toBe(status);
    }
  });

  test('the listener bounds fragmented messages by the mux frame limit', async () => {
    const n = node({ muxOptions: { maxFrameSize: 4096 } });
    const uri = await n.listenWebSocket('127.0.0.1', 0);
    const accepted = (async () => {
      for await (const connection of n.connections) return connection;
      throw new Error('no connection');
    })();
    const client = await RawClient.connect(portOf(uri));
    const connection = await accepted;
    for (let i = 0; i < 4; i++) {
      client.send(
        clientFrame(i === 0 ? opBinary : opContinuation, new Uint8Array(1024), { fin: false }),
      );
    }
    client.send(clientHeader(opContinuation, 1024n));
    if (isBun) {
      // Bun's stand-in for ws waits for the claimed payload: nothing to refuse yet.
      client.destroy();
      await connection.done;
      return;
    }
    expect((await connection.done).known).toBe(StatusCode.frameTooLarge);
    // The client gets the node's LIMITS, then the close frame 1009. Dart's
    // server sends GOAWAY FRAME_TOO_LARGE between them; ws refuses the
    // message itself and closes at once, so the mux's GOAWAY is dropped.
    const goAways: number[] = [];
    let limits = 0;
    for (;;) {
      const frame = await client.next();
      if (frame.opcode !== opBinary) {
        expect(frame.opcode).toBe(opClose);
        expect(closeCodeOf(frame)).toBe(1009);
        break;
      }
      const control = MuxControlMessage.decode(MuxFrame.decode(frame.payload).payload);
      if (control.knownType === MuxControlType.goAway) goAways.push(control.goAwayStatus.code);
      if (control.knownType === MuxControlType.limits) limits++;
    }
    expect(limits).toBe(1);
    expect(goAways).toEqual([]);
  });

  test('acceptWebSocket bounds messages by its own maxFrameSize', async () => {
    const n = node();
    const http = createServer();
    const accepted = new Promise<MuxConnection>((resolve, reject) => {
      http.on('upgrade', (request, socket, head) => {
        n.acceptWebSocket(request, socket, head, { maxFrameSize: 4096 }).then(resolve, reject);
      });
    });
    await new Promise<void>((resolve) => http.listen(0, '127.0.0.1', resolve));
    onCleanup(() => {
      http.closeAllConnections();
      http.close();
    });
    const address = http.address();
    const port = typeof address === 'object' && address !== null ? address.port : 0;
    const client = await RawClient.connect(port);
    expect(client.status).toBe(101);
    const connection = await accepted;
    // 5 KiB: over the limit given, far below the node's 1 MiB.
    for (let i = 0; i < 4; i++) {
      client.send(
        clientFrame(i === 0 ? opBinary : opContinuation, new Uint8Array(1024), { fin: false }),
      );
    }
    client.send(clientFrame(opContinuation, new Uint8Array(1024)));
    expect((await connection.done).known).toBe(StatusCode.frameTooLarge);
  });

  test('acceptWebSocket: a refusal is INVALID_ARGUMENT; after close, 503 and FAILED_PRECONDITION', async () => {
    const n = node();
    const http = createServer();
    const outcomes: Promise<MuxConnection>[] = [];
    http.on('upgrade', (request, socket, head) => {
      const outcome = n.acceptWebSocket(request, socket, head);
      outcome.catch(() => {});
      outcomes.push(outcome);
    });
    await new Promise<void>((resolve) => http.listen(0, '127.0.0.1', resolve));
    onCleanup(() => {
      http.closeAllConnections();
      http.close();
    });
    const address = http.address();
    const port = typeof address === 'object' && address !== null ? address.port : 0;
    const refused = await RawClient.connect(port, handshake({ protocols: 'chat' }));
    expect(refused.status).toBe(400);
    await rejectsWith(outcomes[0] as Promise<MuxConnection>, StatusCode.invalidArgument);
    await n.close();
    const late = await RawClient.connect(port);
    expect(late.status).toBe(503);
    await rejectsWith(outcomes[1] as Promise<MuxConnection>, StatusCode.failedPrecondition);
    expect(() =>
      n.acceptWebSocket({} as never, {} as never, new Uint8Array(0), { maxFrameSize: -1 }),
    ).toThrow(RangeError);
  });

  test('a plain request for the path gets 426; others go to onOtherRequest', async () => {
    const n = node();
    const uri = await n.listenWebSocket('127.0.0.1', 0, {
      path: '/mesh',
      onOtherRequest: (request, response) => {
        if (request.url === '/fail') throw new Error('handler failed');
        response.writeHead(200, { 'Content-Type': 'text/plain' });
        response.end(`served ${request.url}`);
      },
    });
    const get = (path: string): Promise<{ status: number; body: string }> =>
      new Promise((resolve, reject) => {
        const request = httpRequest({ host: '127.0.0.1', port: portOf(uri), path }, (response) => {
          let body = '';
          response.on('data', (chunk: Buffer) => {
            body += chunk.toString();
          });
          response.on('end', () => resolve({ status: response.statusCode ?? 0, body }));
        });
        request.on('error', reject);
        request.end();
      });
    expect((await get('/mesh')).status).toBe(426);
    expect(await get('/.well-known/x')).toEqual({ status: 200, body: 'served /.well-known/x' });
    expect((await get('/fail')).status).toBe(500);
  });

  test('upgrades for other paths go to onOtherUpgrade', async () => {
    const n = node();
    const uri = await n.listenWebSocket('127.0.0.1', 0, {
      path: '/mesh',
      onOtherUpgrade: (_request, socket) => {
        socket.end('HTTP/1.1 418 I am a teapot\r\nConnection: close\r\n\r\n');
      },
    });
    const other = await RawClient.connect(portOf(uri), handshake({ path: '/tea' }));
    expect(other.status).toBe(418);
  });
});

describe('the web entry node', () => {
  test('connects over ws to a Node listener, with the global WebSocket', async () => {
    const server = node();
    server.registerService('echo', echo);
    const endpoint = await server.listenWebSocket('127.0.0.1', 0);
    const browser = new WebSwitchboard({
      resolver: new EndpointResolver(endpoint),
      muxOptions: { keepAliveInterval: undefined, goAwayGrace: 200 },
    });
    onCleanup(() => browser.close());
    const channel = await browser.openChannel('echo');
    expect(await roundTrip(channel, 'from a browser')).toBe('from a browser');
    await channel.close();
  });

  test('refuses tcp as UNSUPPORTED, and https without a quic provider as UNIMPLEMENTED', async () => {
    const browser = new WebSwitchboard();
    onCleanup(() => browser.close());
    await rejectsWith(browser.connect('tcp://127.0.0.1:1'), StatusCode.unsupported);
    await rejectsWith(browser.connect('https://127.0.0.1:1/sb'), StatusCode.unimplemented);
    expect(browser.quic).toBeUndefined();
  });
});
