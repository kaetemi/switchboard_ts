/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Two frame mux connections over TCP on the loopback (connectTcp and
// listenTcp): the "real transports" TCP cases of `test/mux_test.dart`,
// and the preamble errors of its "transport failures end to end" group
// over a real socket.

import { connect } from 'node:net';
import { afterEach, describe, expect, test } from 'vitest';
import { hexBytes } from '../../src/core/bytes.ts';
import { FrameMuxConnection } from '../../src/core/mux/frame-mux-connection.ts';
import { MuxControlMessage, MuxControlType, MuxFrame } from '../../src/core/mux/mux-frame.ts';
import { StatusCode } from '../../src/core/status.ts';
import { StreamFrameDecoder, StreamFraming } from '../../src/core/transport/stream-framing.ts';
import { connectTcp, listenTcp, type TcpTransport } from '../../src/node/transport/tcp.ts';
import { exercise, onCleanup, queue, runCleanups, wide } from './mux-support.ts';

afterEach(runCleanups);

async function tcpPair(): Promise<[TcpTransport, TcpTransport]> {
  const listener = await listenTcp('127.0.0.1', 0);
  onCleanup(() => listener.close());
  const client = await connectTcp('127.0.0.1', listener.port);
  const accepted = await queue(listener.transports).next();
  await listener.close();
  onCleanup(() => {
    client.abort();
    accepted.transport.abort();
  });
  return [client, accepted.transport];
}

describe('frame mux over TCP', () => {
  test('TCP', async () => {
    const [clientTransport, serverTransport] = await tcpPair();
    const client = new FrameMuxConnection(clientTransport, { ...wide, isInitiator: true });
    const server = new FrameMuxConnection(serverTransport, { ...wide, isInitiator: false });
    await exercise(client, server);
  });

  test('TCP: GOAWAY reaches a peer that is still sending', async () => {
    // The server sends GOAWAY behind 4 MB of data and closes at once, while
    // the client keeps its socket full. Closing a socket with unread input
    // resets the connection, and the reset destroys the GOAWAY in flight;
    // the TCP transport lingers (reads and drops until the client closes) so
    // the client ends with GOING_AWAY, not CONNECTION_LOST.
    const [clientTransport, serverTransport] = await tcpPair();
    const client = new FrameMuxConnection(clientTransport, { ...wide, isInitiator: true });
    const server = new FrameMuxConnection(serverTransport, { ...wide, isInitiator: false });
    const channel = client.open(new Uint8Array(0));
    const chunk = new Uint8Array(64 * 1024);
    let flooding = true;
    onCleanup(() => {
      flooding = false;
    });
    void (async () => {
      while (flooding && channel.canSend) {
        try {
          await channel.send(chunk);
        } catch {
          break;
        }
      }
    })();
    const drain = async (readable: ReadableStream<Uint8Array>): Promise<void> => {
      try {
        for await (const _ of readable) {
          // Dropped.
        }
      } catch {
        // Ended with the connection.
      }
    };
    const fromServer = drain(channel.readable);
    const serverSide = await queue(server.incoming).next();
    const toServer = drain(serverSide.readable);
    await new Promise((resolve) => setTimeout(resolve, 50));
    for (let i = 0; i < 64; i++) {
      void serverSide.send(chunk).catch(() => {});
    }
    const goingAway = server.goAway();
    await server.close();
    expect((await client.done).known).toBe(StatusCode.goingAway);
    expect(client.peerGoingAway).toBe(true);
    expect((await channel.done).known).toBe(StatusCode.goingAway);
    await goingAway;
    await Promise.all([fromServer, toServer]);
  });

  test('the transport limit is announced in LIMITS', async () => {
    const listener = await listenTcp('127.0.0.1', 0, { maxFrameSize: 4096 });
    onCleanup(() => listener.close());
    const clientTransport = await connectTcp('127.0.0.1', listener.port);
    const accepted = await queue(listener.transports).next();
    onCleanup(() => {
      clientTransport.abort();
      accepted.transport.abort();
    });
    const client = new FrameMuxConnection(clientTransport, { ...wide, isInitiator: true });
    const server = new FrameMuxConnection(accepted.transport, { ...wide, isInitiator: false });
    await client.ping();
    expect(client.peerLimits?.maxFrameSize).toBe(4096);
    await client.goAway();
    expect((await server.done).known).toBe(StatusCode.goingAway);
  });
});

describe('transport failures end to end over a socket', () => {
  /** Runs a mux over an accepted socket fed with [input]; its end status and the control messages it sent. */
  async function overSocket(input: Uint8Array) {
    const listener = await listenTcp('127.0.0.1', 0);
    onCleanup(() => listener.close());
    const socket = connect({ host: '127.0.0.1', port: listener.port });
    onCleanup(() => socket.destroy());
    const output: Buffer[] = [];
    socket.on('data', (data: Buffer) => output.push(data));
    const ended = new Promise<void>((resolve) => socket.on('close', () => resolve()));
    await new Promise((resolve) => socket.once('connect', resolve));
    const accepted = await queue(listener.transports).next();
    const mux = new FrameMuxConnection(accepted.transport, { ...wide, isInitiator: false });
    socket.write(input);
    const status = await mux.done;
    socket.end();
    await ended;
    const frames: Uint8Array[] = [];
    new StreamFrameDecoder().add(new Uint8Array(Buffer.concat(output)), (frame) =>
      frames.push(frame),
    );
    // The control messages it sent: its LIMITS, then the GOAWAY.
    const sent = frames.map((frame) => MuxControlMessage.decode(MuxFrame.decode(frame).payload));
    return { status, goAway: sent.filter((m) => m.knownType === MuxControlType.goAway) };
  }

  test('wrong preamble magic: GOAWAY PROTOCOL_ERROR', async () => {
    const { status, goAway } = await overSocket(hexBytes('53 57 42 58 01 00 00 00'));
    expect(status.known).toBe(StatusCode.protocolError);
    expect(goAway).toHaveLength(1);
    expect(goAway[0]?.goAwayStatus.known).toBe(StatusCode.protocolError);
  });

  test('unsupported preamble version: GOAWAY UNSUPPORTED', async () => {
    const { status, goAway } = await overSocket(StreamFraming.encodePreamble({ version: 2 }));
    expect(status.known).toBe(StatusCode.unsupported);
    expect(goAway).toHaveLength(1);
    expect(goAway[0]?.goAwayStatus.known).toBe(StatusCode.unsupported);
  });

  test('a frame over the limit: GOAWAY FRAME_TOO_LARGE', async () => {
    // The length prefix alone: refused before the body is read.
    const head = new Uint8Array(12);
    head.set(StreamFraming.encodePreamble());
    new DataView(head.buffer).setUint32(8, 2 * 1024 * 1024, true);
    const { status, goAway } = await overSocket(head);
    expect(status.known).toBe(StatusCode.frameTooLarge);
    expect(goAway).toHaveLength(1);
    expect(goAway[0]?.goAwayStatus.known).toBe(StatusCode.frameTooLarge);
  });
});
