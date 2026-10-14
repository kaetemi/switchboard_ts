/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// The stream binding over TCP on the loopback: connectTcp, listenTcp and
// tcpTransport over node:net. Ports the `StreamTransport over TCP` group
// of `test/transport_test.dart`, plus the TS-only surface (the listener,
// the connect bounds, the socket accounting, the close sequence).

import { createServer, type Server, type Socket } from 'node:net';
import { afterEach, describe, expect, test } from 'vitest';
import { hexBytes, hexString } from '../../../src/core/bytes.ts';
import { StatusCode, SwitchboardError } from '../../../src/core/status.ts';
import { StreamTransport } from '../../../src/core/transport/stream-transport.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';
import {
  connectTcp,
  listenTcp,
  type TcpListener,
  type TcpTransport,
  tcpTransport,
} from '../../../src/node/transport/tcp.ts';
import { bytesOf, collect } from './support.ts';

const preambleHex = '53 57 42 44 01 00 00 00';

const cleanups: (() => unknown)[] = [];
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) {
    await cleanup();
  }
});

/** A plain server socket on the loopback whose accepted sockets the test takes. */
async function rawServer(): Promise<{ server: Server; port: number; next(): Promise<Socket> }> {
  const sockets: Socket[] = [];
  let waiter: Deferred<void> | undefined;
  const server = createServer({ allowHalfOpen: true }, (socket) => {
    sockets.push(socket);
    waiter?.resolve();
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  cleanups.push(() => {
    for (const socket of sockets) socket.destroy();
    server.close();
  });
  const address = server.address();
  const port = typeof address === 'object' && address !== null ? address.port : 0;
  return {
    server,
    port,
    async next() {
      while (sockets.length === 0) {
        waiter = new Deferred();
        await waiter.promise;
      }
      return sockets.shift() as Socket;
    },
  };
}

/** A listener of the stream binding on the loopback, closed after the test. */
async function listener(options: Parameters<typeof listenTcp>[2] = {}): Promise<TcpListener> {
  const l = await listenTcp('127.0.0.1', 0, options);
  cleanups.push(() => l.close());
  return l;
}

async function accepted(l: TcpListener): Promise<TcpTransport> {
  for await (const { transport } of l.transports) {
    return transport;
  }
  throw new Error('listener closed');
}

async function next(reader: ReadableStreamDefaultReader<Uint8Array>): Promise<string> {
  const result = await reader.read();
  return result.done ? 'done' : hexString(result.value);
}

function same(a: Uint8Array | undefined, b: Uint8Array): boolean {
  if (a === undefined || a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
  return true;
}

describe('StreamTransport over TCP', () => {
  test('frames both ways, close propagates', async () => {
    const l = await listener();
    const client = await connectTcp('127.0.0.1', l.port);
    const server = await accepted(l);
    const qc = client.readable.getReader();
    const qs = server.readable.getReader();
    const big = bytesOf(300000);
    const wc = client.writable.getWriter();
    void wc.write(hexBytes('02 02 00 01'));
    void wc.write(big);
    const ws = server.writable.getWriter();
    for (let i = 0; i < 100; i++) {
      void ws.write(bytesOf(1 + i, i));
    }
    expect(await next(qs)).toBe('02 02 00 01');
    expect(same((await qs.read()).value, big)).toBe(true);
    for (let i = 0; i < 100; i++) {
      expect(same((await qc.read()).value, bytesOf(1 + i, i))).toBe(true);
    }
    await wc.close();
    expect(await next(qs)).toBe('done');
    expect(await next(qc)).toBe('done');
    // The server's output closed once its input ended: closing it is done at once.
    await ws.close();
  });

  test('frames split into one byte chunks', async () => {
    const raw = await rawServer();
    const client = await connectTcp('127.0.0.1', raw.port);
    const socket = await raw.next();
    // The server side reads one byte at a time.
    const readable = new ReadableStream<Uint8Array>({
      start(controller) {
        socket.on('data', (data: Buffer) => {
          for (const b of data) controller.enqueue(new Uint8Array([b]));
        });
        socket.on('end', () => {
          try {
            controller.close();
          } catch {
            // Cancelled by the transport.
          }
        });
      },
    });
    const writable = new WritableStream<Uint8Array>({
      write: (chunk) => new Promise<void>((resolve) => socket.write(chunk, () => resolve())),
      close: () => new Promise<void>((resolve) => socket.end(resolve)),
    });
    const { streamTransport } = await import('../../../src/core/transport/stream-transport.ts');
    const server = streamTransport({ readable, writable });
    const frames = Array.from({ length: 59 }, (_, i) => bytesOf((i + 1) * 7, i + 1));
    const reader = server.readable.getReader();
    const writer = client.writable.getWriter();
    for (const frame of frames) void writer.write(frame);
    for (const frame of frames) {
      expect(same((await reader.read()).value, frame)).toBe(true);
    }
    reader.releaseLock();
    await writer.close();
    await server.writable.close();
  });

  test('abort destroys the socket', async () => {
    const l = await listener();
    const client = await connectTcp('127.0.0.1', l.port);
    const server = await accepted(l);
    expect(server.maxFrameSize).toBe(StreamTransport.defaultMaxFrameSize);
    const serverEnd = collect(server.readable);
    const clientEnd = collect(client.readable).catch(() => []);
    server.abort();
    expect(await serverEnd).toEqual([]);
    await server.writable.close();
    expect(server.socket.destroyed).toBe(true);
    // The peer sees the connection go away.
    await clientEnd;
    await client.writable.close().catch(() => {});
  });

  test('frame over the limit fails the stream', async () => {
    const l = await listener({ maxFrameSize: 100 });
    const client = await connectTcp('127.0.0.1', l.port);
    const server = await accepted(l);
    const queue = server.readable.getReader();
    const writer = client.writable.getWriter();
    void writer.write(bytesOf(101));
    const error = await queue.read().then(
      () => undefined,
      (e: unknown) => e,
    );
    expect(error).toBeInstanceOf(SwitchboardError);
    expect((error as SwitchboardError).code).toBe(StatusCode.frameTooLarge);
    // The server closed its side, which ends the client stream.
    expect(await collect(client.readable)).toEqual([]);
    await writer.close();
  });
});

describe('connectTcp and listenTcp', () => {
  test('the preamble goes out at once; remote, endpoint and NODELAY', async () => {
    const raw = await rawServer();
    const client = await connectTcp('127.0.0.1', raw.port);
    const socket = await raw.next();
    const data = await new Promise<Buffer>((resolve) => socket.once('data', resolve));
    expect(hexString(data.subarray(0, 8))).toBe(preambleHex);
    expect(client.remote).toBe(`127.0.0.1:${raw.port}`);
    client.abort();
  });

  test('the listener endpoint and remote', async () => {
    const l = await listener();
    expect(l.endpoint).toBe(`tcp://127.0.0.1:${l.port}`);
    expect(l.address).toBe('127.0.0.1');
    const client = await connectTcp('localhost', l.port);
    let remote = '';
    for await (const accepted of l.transports) {
      remote = accepted.remote;
      expect(accepted.transport.remote).toBe(remote);
      accepted.transport.abort();
      break;
    }
    expect(remote).toMatch(/^127\.0\.0\.1:\d+$|^::1:\d+$|^::ffff:127\.0\.0\.1:\d+$/);
    client.abort();
  });

  test('onTransport takes the accepted connections', async () => {
    const got = new Deferred<TcpTransport>();
    const l = await listener({ onTransport: (transport) => got.resolve(transport) });
    const client = await connectTcp('127.0.0.1', l.port);
    const server = await got.promise;
    void client.writable.getWriter().write(hexBytes('02 02 00 07'));
    expect(await next(server.readable.getReader())).toBe('02 02 00 07');
    client.abort();
    server.abort();
  });

  test('an IPv6 listener on the loopback', async () => {
    let l: TcpListener;
    try {
      l = await listenTcp('::1', 0);
    } catch {
      return; // No IPv6 loopback here.
    }
    cleanups.push(() => l.close());
    expect(l.endpoint).toBe(`tcp://[::1]:${l.port}`);
    const client = await connectTcp('[::1]', l.port);
    const server = await accepted(l);
    void client.writable.getWriter().write(hexBytes('02 02 00 08'));
    expect(await next(server.readable.getReader())).toBe('02 02 00 08');
    client.abort();
    server.abort();
  });

  test('a refused connection is UNAVAILABLE with the socket error as cause', async () => {
    const raw = await rawServer();
    const port = raw.port;
    raw.server.close();
    await new Promise((resolve) => setTimeout(resolve, 10));
    const error = await connectTcp('127.0.0.1', port).then(
      () => undefined,
      (e: unknown) => e,
    );
    expect(error).toBeInstanceOf(SwitchboardError);
    expect((error as SwitchboardError).code).toBe(StatusCode.unavailable);
    expect((error as Error).cause).toBeInstanceOf(Error);
  });

  test('an aborted signal rejects with its reason', async () => {
    const raw = await rawServer();
    const controller = new AbortController();
    const reason = new Error('stop');
    controller.abort(reason);
    await expect(connectTcp('127.0.0.1', raw.port, { signal: controller.signal })).rejects.toBe(
      reason,
    );
    const late = new AbortController();
    const attempt = connectTcp('10.255.255.1', 9, { signal: late.signal });
    late.abort(reason);
    await expect(attempt).rejects.toBe(reason);
  });

  test('a connect that takes too long is UNAVAILABLE', async () => {
    // A non-routable address: the SYN goes unanswered.
    const error = await connectTcp('10.255.255.1', 9, { timeout: 30 }).then(
      () => undefined,
      (e: unknown) => e,
    );
    expect(error).toBeInstanceOf(SwitchboardError);
    expect((error as SwitchboardError).code).toBe(StatusCode.unavailable);
  });

  test('ports are checked', async () => {
    await expect(connectTcp('127.0.0.1', 70000)).rejects.toBeInstanceOf(RangeError);
    await expect(listenTcp('127.0.0.1', -1)).rejects.toBeInstanceOf(RangeError);
  });

  test('binding a port in use fails', async () => {
    const l = await listener();
    await expect(listenTcp('127.0.0.1', l.port)).rejects.toThrow();
  });

  test('closing the listener ends transports and keeps the connections', async () => {
    const l = await listenTcp('127.0.0.1', 0);
    const client = await connectTcp('127.0.0.1', l.port);
    const iterator = l.transports[Symbol.asyncIterator]();
    const first = await iterator.next();
    await l.close();
    await l.close();
    expect(l.isClosed).toBe(true);
    expect((await iterator.next()).done).toBe(true);
    const server = (first.value as { transport: TcpTransport }).transport;
    void client.writable.getWriter().write(hexBytes('02 02 00 09'));
    expect(await next(server.readable.getReader())).toBe('02 02 00 09');
    await expect(connectTcp('127.0.0.1', l.port)).rejects.toBeInstanceOf(SwitchboardError);
    client.abort();
    server.abort();
  });
});

describe('the socket accounting', () => {
  test('buffered output counts the socket buffer; accepted output grows as the peer reads', async () => {
    const raw = await rawServer();
    const client = await connectTcp('127.0.0.1', raw.port, { outputHighWaterMark: 0 });
    const socket = await raw.next();
    socket.pause();
    const writer = client.writable.getWriter();
    const frame = bytesOf(256 * 1024);
    for (let i = 0; i < 64; i++) void writer.write(frame);
    await new Promise((resolve) => setTimeout(resolve, 50));
    // 16 MiB cannot all have gone: some waits in the socket or the transport.
    expect(client.bufferedOutputBytes).toBeGreaterThan(0);
    expect(client.isOutputReady).toBe(false);
    const before = client.acceptedOutputBytes;
    socket.resume();
    socket.on('data', () => {});
    await client.outputReady;
    expect(client.acceptedOutputBytes).toBeGreaterThan(before);
    await new Promise<void>((resolve) => {
      const check = (): void => {
        if (client.bufferedOutputBytes === 0) resolve();
        else setTimeout(check, 5);
      };
      check();
    });
    expect(client.acceptedOutputBytes).toBe(8 + 64 * (4 + frame.length));
    client.abort();
  });

  test('a peer that does not read throttles the input past the high-water mark', async () => {
    const raw = await rawServer();
    const client = await connectTcp('127.0.0.1', raw.port, { outputHighWaterMark: 64 * 1024 });
    const socket = await raw.next();
    socket.pause();
    const writer = client.writable.getWriter();
    for (let i = 0; i < 64; i++) void writer.write(bytesOf(256 * 1024, i));
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(client.isInputThrottled).toBe(true);
    socket.resume();
    socket.on('data', () => {});
    await new Promise<void>((resolve) => {
      const check = (): void => {
        if (!client.isInputThrottled) resolve();
        else setTimeout(check, 5);
      };
      check();
    });
    client.abort();
  });
});

describe('the close sequence', () => {
  test('close ends the output with a FIN and lingers until the peer closes', async () => {
    const raw = await rawServer();
    const client = await connectTcp('127.0.0.1', raw.port);
    const socket = await raw.next();
    const received: Buffer[] = [];
    socket.on('data', (data: Buffer) => received.push(data));
    const ended = new Promise<void>((resolve) => socket.once('end', resolve));
    const writer = client.writable.getWriter();
    void writer.write(hexBytes('02 02 00 0A'));
    const closing = writer.close();
    await ended;
    expect(hexString(Buffer.concat(received))).toBe(`${preambleHex} 04 00 00 00 02 02 00 0A`);
    await closing;
    // Still lingering: the input is read until the peer closes.
    expect(client.socket.destroyed).toBe(false);
    socket.write(hexBytes('ff ff'));
    socket.end();
    await new Promise<void>((resolve) => client.socket.once('close', () => resolve()));
  });

  test('a peer that never closes is cut off after the close timeout', async () => {
    const raw = await rawServer();
    const client = await connectTcp('127.0.0.1', raw.port, { closeTimeout: 50 });
    const socket = await raw.next();
    socket.on('data', () => {});
    const start = performance.now();
    await client.writable.close().catch(() => {});
    await new Promise<void>((resolve) => client.socket.once('close', () => resolve()));
    expect(performance.now() - start).toBeLessThan(5000);
    expect(client.socket.destroyed).toBe(true);
  });

  test('the peer closing ends the input and closes the output', async () => {
    const raw = await rawServer();
    const client = await connectTcp('127.0.0.1', raw.port);
    const socket = await raw.next();
    socket.on('data', () => {});
    const ended = new Promise<void>((resolve) => socket.once('end', resolve));
    socket.end();
    expect(await collect(client.readable)).toEqual([]);
    await ended;
  });

  test('a connection reset fails the input', async () => {
    const raw = await rawServer();
    const client = await connectTcp('127.0.0.1', raw.port);
    const socket = await raw.next();
    const reader = client.readable.getReader();
    const read = reader.read();
    socket.resetAndDestroy();
    const outcome = await read.then(
      (r) => (r.done ? 'done' : 'frame'),
      () => 'error',
    );
    expect(['error', 'done']).toContain(outcome);
  });

  test('tcpTransport over an accepted socket, with its remote', async () => {
    const raw = await rawServer();
    const client = await connectTcp('127.0.0.1', raw.port);
    const socket = await raw.next();
    const server = tcpTransport(socket);
    expect(server.remote).toMatch(/:\d+$/);
    void client.writable.getWriter().write(hexBytes('02 02 00 0B'));
    expect(await next(server.readable.getReader())).toBe('02 02 00 0B');
    client.abort();
    server.abort();
  });
});
