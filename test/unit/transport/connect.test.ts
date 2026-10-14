/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// connectTransport: a frame transport to a tcp, ws, wss or mem endpoint
// on Node, and what it refuses. Dart has no counterpart of its own (the
// node's dial covers it); an unknown scheme is UNSUPPORTED here where
// Dart's node answers UNIMPLEMENTED.

import { describe, expect, test } from 'vitest';
import { hexBytes, hexString } from '../../../src/core/bytes.ts';
import { StatusCode, SwitchboardError } from '../../../src/core/status.ts';
import type { FrameTransport } from '../../../src/core/transport/contract.ts';
import { MemoryEndpoints } from '../../../src/core/transport/memory-endpoints.ts';
import { connectTransport } from '../../../src/node/transport/connect.ts';
import { listenTcp } from '../../../src/node/transport/tcp.ts';
import { listenWebSocket } from '../../../src/node/transport/ws-server.ts';
import { onCleanup } from './ws-support.ts';

async function codeOf(promise: Promise<unknown>): Promise<number | undefined> {
  try {
    await promise;
  } catch (error) {
    return error instanceof SwitchboardError ? error.code : undefined;
  }
  throw new Error('expected a rejection');
}

async function firstFrame(transport: FrameTransport): Promise<string> {
  const reader = transport.readable.getReader();
  const result = await reader.read();
  reader.releaseLock();
  return result.done ? 'done' : hexString(result.value);
}

describe('connectTransport', () => {
  test('tcp: the stream binding', async () => {
    const listener = await listenTcp('127.0.0.1', 0);
    onCleanup(() => listener.close());
    const client = await connectTransport(listener.endpoint, { timeout: 2000 });
    onCleanup(() => client.abort?.());
    for await (const { transport } of listener.transports) {
      onCleanup(() => transport.abort());
      void client.writable.getWriter().write(hexBytes('02 02 00 01'));
      expect(await firstFrame(transport)).toBe('02 02 00 01');
      break;
    }
  });

  test('ws: the WebSocket binding; a fragment is ignored', async () => {
    const listener = await listenWebSocket('127.0.0.1', 0, { path: '/sb' });
    onCleanup(() => listener.close());
    const client = await connectTransport(`${listener.endpoint}#npc/1a2b`, { maxFrameSize: 4096 });
    onCleanup(() => client.abort?.());
    expect(client.maxFrameSize).toBe(4096);
    for await (const { transport } of listener.transports) {
      onCleanup(() => transport.abort());
      void client.writable.getWriter().write(hexBytes('02 02 00 02'));
      expect(await firstFrame(transport)).toBe('02 02 00 02');
      break;
    }
  });

  test('mem: an in-process pair with the listener', async () => {
    const accepted: { transport: FrameTransport; remote: string }[] = [];
    const uri = MemoryEndpoints.register((transport, remote) =>
      accepted.push({ transport, remote }),
    );
    onCleanup(() => MemoryEndpoints.unregister(new URL(uri).hostname));
    const client = await connectTransport(uri.toUpperCase().replace('MEM:', 'mem:'), {
      memoryRemote: 'mem://me',
    });
    expect(accepted).toHaveLength(1);
    expect(accepted[0]?.remote).toBe('mem://me');
    void client.writable.getWriter().write(hexBytes('02 02 00 03'));
    expect(await firstFrame(accepted[0]?.transport as FrameTransport)).toBe('02 02 00 03');
  });

  test('refusals', async () => {
    expect(await codeOf(connectTransport('not a uri'))).toBe(StatusCode.invalidArgument);
    expect(await codeOf(connectTransport('tcp://127.0.0.1'))).toBe(StatusCode.invalidArgument);
    expect(await codeOf(connectTransport('mem://'))).toBe(StatusCode.invalidArgument);
    expect(await codeOf(connectTransport('mem://nobody-listens'))).toBe(StatusCode.unavailable);
    expect(await codeOf(connectTransport('quic://127.0.0.1:4433'))).toBe(StatusCode.unsupported);
    expect(await codeOf(connectTransport('https://127.0.0.1:4433/sb'))).toBe(
      StatusCode.unsupported,
    );
    expect(await codeOf(connectTransport('gopher://127.0.0.1:70'))).toBe(StatusCode.unsupported);
  });

  test('an unreachable tcp endpoint is UNAVAILABLE; an aborted signal rejects with its reason', async () => {
    const listener = await listenTcp('127.0.0.1', 0);
    const endpoint = listener.endpoint;
    await listener.close();
    expect(await codeOf(connectTransport(endpoint))).toBe(StatusCode.unavailable);
    const controller = new AbortController();
    const reason = new Error('stop');
    controller.abort(reason);
    await expect(connectTransport('mem://x', { signal: controller.signal })).rejects.toBe(reason);
    await expect(connectTransport(endpoint, { signal: controller.signal })).rejects.toBe(reason);
  });
});
