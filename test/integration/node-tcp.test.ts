/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Two Node nodes end to end over TCP on the loopback: listenTcp, the tcp
// dialer, the pool, dial, listener policies, close, and what the node
// refuses. Raw channels; the Talk cases wait for the Talk layer of the
// node (talk-layer.ts still refuses).

import { connect } from 'node:net';
import { describe, expect, test } from 'vitest';
import { ChannelAddress } from '../../src/core/address/index.ts';
import type { MuxConnection } from '../../src/core/mux/contract.ts';
import { MuxControlMessage, MuxControlType, MuxFrame } from '../../src/core/mux/mux-frame.ts';
import { Name } from '../../src/core/name.ts';
import { StatusCode } from '../../src/core/status.ts';
import { ChannelPolicies } from '../../src/core/switchboard/channel-policy.ts';
import { EndpointResolver } from '../../src/core/switchboard/resolver.ts';
import { StreamFrameDecoder, StreamFraming } from '../../src/core/transport/stream-framing.ts';
import { rejectsWith, tagged, tagOf } from '../support/node/harness.ts';
import { echo, node, onCleanup, roundTrip } from './node-support.ts';

const svc = Name.of('svc');

describe('two nodes over TCP', () => {
  test('a client opens a channel to a service of a server', async () => {
    const server = node();
    server.registerService('echo', echo);
    const endpoint = await server.listenTcp('127.0.0.1', 0);
    expect(endpoint).toMatch(/^tcp:\/\/127\.0\.0\.1:\d+$/);
    expect(server.listeningEndpoints).toEqual([endpoint]);
    expect(server.isOwnEndpoint(endpoint)).toBe(true);
    expect(server.isOwnEndpoint(endpoint.replace('127.0.0.1', 'localhost'))).toBe(true);

    const client = node({ resolver: new EndpointResolver(endpoint) });
    const channel = await client.openChannel('echo');
    expect(await roundTrip(channel, 'hello')).toBe('hello');
    expect(await roundTrip(channel, 'again')).toBe('again');
    await channel.close();
    expect((await channel.done).isOk).toBe(true);
  });

  test('the README example over raw channels', async () => {
    const server = node();
    server.registerService('echo', echo);
    const endpoint = await server.listenTcp('127.0.0.1', 0);
    const client = node({ resolver: new EndpointResolver(endpoint) });
    const channel = await client.openChannel('echo', { shard: 7 });
    expect(await roundTrip(channel, 'blob')).toBe('blob');
    await channel.close();
    await client.close();
    await server.close();
  });

  test.todo('the README example with Talk (UPPER, COUNT): the node Talk layer is not wired yet');

  test('each side sees the address of the other', async () => {
    const server = node();
    const remotes: (string | undefined)[] = [];
    server.registerService(svc, (incoming) => {
      remotes.push(incoming.remote);
      tagged('svc')(incoming);
    });
    const endpoint = await server.listenTcp('127.0.0.1', 0);
    const client = node();
    const channel = await client.openChannelAt(endpoint, new ChannelAddress({ type: svc }));
    expect(await tagOf(channel)).toBe('svc');
    expect(remotes[0]).toMatch(/^127\.0\.0\.1:\d+$/);
    expect(channel.connection.remote).toBe(endpoint.slice('tcp://'.length));
  });

  test('the pool reuses a connection; dial gives a new one', async () => {
    const server = node();
    server.registerService(svc, tagged('svc'));
    const endpoint = await server.listenTcp('127.0.0.1', 0);
    const client = node();
    const connection = await client.connect(endpoint);
    expect(await client.connect(endpoint)).toBe(connection);
    expect(await client.connect(endpoint.replace('127.0.0.1', '127.0.0.1'))).toBe(connection);
    const dialled = await client.dial(endpoint);
    expect(dialled).not.toBe(connection);
    expect(await tagOf(client.openChannelOn(dialled, new ChannelAddress({ type: svc })))).toBe(
      'svc',
    );
    await dialled.goAway();
  });

  test('the listener policy applies', async () => {
    const server = node();
    server.registerService(svc, tagged('svc'));
    server.registerService('other', tagged('other'));
    const endpoint = await server.listenTcp('127.0.0.1', 0, {
      policy: ChannelPolicies.allowTypes([svc]),
    });
    const client = node();
    expect(
      await tagOf(await client.openChannelAt(endpoint, new ChannelAddress({ type: svc }))),
    ).toBe('svc');
    expect(
      await tagOf(
        await client.openChannelAt(endpoint, new ChannelAddress({ type: Name.of('other') })),
      ),
    ).toBe('closed permissionDenied');
  });

  test('symmetric dispatch: the server opens a channel back on the accepted connection', async () => {
    const server = node();
    const client = node();
    client.registerService('back', tagged('back'));
    const answered = new Promise<string>((resolve) => {
      server.registerService(svc, (incoming) => {
        const back = incoming.connection.open(
          new ChannelAddress({ type: Name.of('back') }).encode(),
        );
        resolve(tagOf(back));
        void incoming.channel.close();
      });
    });
    const endpoint = await server.listenTcp('127.0.0.1', 0);
    await client.openChannelAt(endpoint, new ChannelAddress({ type: svc }));
    expect(await answered).toBe('back');
  });

  test('closing the server: GOAWAY on its connections, the listener stopped', async () => {
    const server = node();
    server.registerService(svc, () => {});
    const endpoint = await server.listenTcp('127.0.0.1', 0);
    const client = node();
    const connection = await client.connect(endpoint);
    await server.close();
    expect((await connection.done).known).toBe(StatusCode.goingAway);
    expect(server.isOwnEndpoint(endpoint)).toBe(false);
    expect(server.listeningEndpoints).toEqual([]);
    await rejectsWith(client.connect(endpoint), StatusCode.unavailable);
    await rejectsWith(server.listenTcp('127.0.0.1', 0), StatusCode.failedPrecondition);
  });

  test('connections reports both ends', async () => {
    const server = node();
    const seen: MuxConnection[] = [];
    void (async () => {
      for await (const connection of server.connections) seen.push(connection);
    })();
    const endpoint = await server.listenTcp('127.0.0.1', 0);
    const client = node();
    const connection = await client.connect(endpoint);
    await connection.ping();
    expect(seen).toHaveLength(1);
    expect(seen[0]?.isInitiator).toBe(false);
  });
});

describe('what the TCP node refuses', () => {
  test('a port nobody listens on is UNAVAILABLE', async () => {
    const free = node();
    const endpoint = await free.listenTcp('127.0.0.1', 0);
    await free.close();
    await rejectsWith(node().connect(endpoint), StatusCode.unavailable);
  });

  test('a tcp endpoint without a port is INVALID_ARGUMENT', async () => {
    await rejectsWith(node().connect('tcp://127.0.0.1'), StatusCode.invalidArgument);
  });

  test('quic and https without a quic provider are UNIMPLEMENTED; an unknown scheme too', async () => {
    const n = node();
    await rejectsWith(n.connect('quic://127.0.0.1:4433'), StatusCode.unimplemented);
    await rejectsWith(n.connect('https://127.0.0.1:4433/sb'), StatusCode.unimplemented);
    await rejectsWith(n.connect('gopher://127.0.0.1:70'), StatusCode.unimplemented);
  });

  test('a connect timeout is UNAVAILABLE', async () => {
    const n = node({ connectTimeout: 50 });
    // A non-routable address: the SYN goes unanswered.
    await rejectsWith(n.connect('tcp://10.255.255.1:9'), StatusCode.unavailable);
  });

  test('a frame over the node limit from a raw peer: GOAWAY FRAME_TOO_LARGE', async () => {
    const server = node({ muxOptions: { maxFrameSize: 4096 } });
    const endpoint = await server.listenTcp('127.0.0.1', 0);
    const port = Number(new URL(endpoint).port);
    const socket = connect({ host: '127.0.0.1', port });
    onCleanup(() => socket.destroy());
    const output: Buffer[] = [];
    socket.on('data', (data: Buffer) => output.push(data));
    const closed = new Promise<void>((resolve) => socket.on('close', () => resolve()));
    await new Promise((resolve) => socket.once('connect', resolve));
    const head = new Uint8Array(12);
    head.set(StreamFraming.encodePreamble());
    new DataView(head.buffer).setUint32(8, 4097, true);
    socket.write(head);
    // Read until the server closes; then end this side too.
    const ended = new Promise<void>((resolve) => socket.once('end', resolve));
    await ended;
    socket.end();
    await closed;
    const frames: Uint8Array[] = [];
    new StreamFrameDecoder().add(new Uint8Array(Buffer.concat(output)), (frame) =>
      frames.push(frame),
    );
    const controls = frames.map((f) => MuxControlMessage.decode(MuxFrame.decode(f).payload));
    const limits = controls.find((c) => c.knownType === MuxControlType.limits);
    expect(limits).toBeDefined();
    const goAway = controls.filter((c) => c.knownType === MuxControlType.goAway);
    expect(goAway.map((c) => c.goAwayStatus.known)).toEqual([StatusCode.frameTooLarge]);
  });
});

describe('a wildcard listener', () => {
  test('is the node own endpoint under the loopback and interface addresses', async () => {
    const server = node();
    const endpoint = await server.listenTcp('0.0.0.0', 0);
    const port = new URL(endpoint).port;
    expect(endpoint).toBe(`tcp://0.0.0.0:${port}`);
    expect(server.isOwnEndpoint(`tcp://127.0.0.1:${port}`)).toBe(true);
    expect(server.isOwnEndpoint(`tcp://localhost:${port}`)).toBe(true);
    const { networkInterfaces } = await import('node:os');
    for (const addresses of Object.values(networkInterfaces())) {
      for (const a of addresses ?? []) {
        if (a.family === 'IPv4') {
          expect(server.isOwnEndpoint(`tcp://${a.address}:${port}`)).toBe(true);
        }
      }
    }
    expect(server.isOwnEndpoint(`tcp://192.0.2.1:${port}`)).toBe(false);
  });
});
