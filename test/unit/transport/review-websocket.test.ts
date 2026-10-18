/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Review findings 4 and 5 (docs/notes/review-mux.md), the WebSocket
// binding.
//
// Finding 4: `WebSocketTransport`'s writable was never settled when the
// WebSocket ended on its own (the peer's close, a failure, `abort()`):
// `writer.closed` stayed pending for ever and later writes resolved while
// the frames were dropped. Design section 12 item 11 says a transport's
// writable errors once its output has ended on its own, as the stream
// transport's and the memory pair's do; Dart's sink completes `done` when
// the WebSocket closes (`_onDone`, `closeWebSocket`, `_sink.complete()`).
// The writable is now errored, over a scripted connection and over `ws`
// (the client, the global WebSocket client and the `ws` server).
//
// Finding 5: over the `ws` package, waiting for the output to drain
// (`outputReady`, and the input throttle) polled `bufferedAmount` with the
// global `setTimeout` (`wsConnection` in ws-common.ts), so it ran on real
// time whatever `clock` the transport was given. Design section 5: every
// timer comes from the `Clock` in the options.
//
// A test still marked `test.fails` fails against the current sources; once
// its defect is fixed it passes, `test.fails` reports that, and the
// `.fails` goes.

import { EventEmitter } from 'node:events';
import { describe, expect, test, vi } from 'vitest';
import { StatusCode, SwitchboardError } from '../../../src/core/status.ts';
import {
  connectWebSocket,
  type WebSocketConnection,
  type WebSocketHandlers,
  WebSocketTransport,
} from '../../../src/core/transport/websocket-transport.ts';
import { connectWsWebSocket } from '../../../src/node/transport/ws-client.ts';
import { wsConnection } from '../../../src/node/transport/ws-common.ts';
import { ManualClock } from '../../../src/testing/manual-clock.ts';
import { Harness } from './ws-support.ts';

/** A WebSocket the test drives: open, recording what is sent. */
function scriptedConnection(): {
  readonly connection: WebSocketConnection;
  readonly handlers: () => WebSocketHandlers;
  readonly sent: Uint8Array[];
} {
  let installed: WebSocketHandlers | undefined;
  const sent: Uint8Array[] = [];
  const connection: WebSocketConnection = {
    readyState: 1,
    protocol: 'switchboard',
    bufferedAmount: 0,
    listen(handlers) {
      installed = handlers;
    },
    send(frame) {
      sent.push(frame);
    },
    close() {},
  };
  return { connection, handlers: () => installed as WebSocketHandlers, sent };
}

/** Lets promise work run. */
function settle(): Promise<void> {
  return ManualClock.settle();
}

/** Whether `writer.closed` settled, and whether a write after the end was refused. */
async function writableAfterEnd(
  transport: WebSocketTransport,
  end: () => void,
): Promise<{ closedSettled: boolean; laterWriteRejected: boolean }> {
  const writer = transport.writable.getWriter();
  let closedSettled = false;
  const settled = (): void => {
    closedSettled = true;
  };
  writer.closed.then(settled, settled);
  await settle();
  end();
  await settle();
  let laterWriteRejected = false;
  await writer.write(new Uint8Array([2, 2, 0])).catch(() => {
    laterWriteRejected = true;
  });
  await settle();
  return { closedSettled, laterWriteRejected };
}

describe('review: the WebSocket transport writable once the WebSocket ended', () => {
  test('the peer closing errors the writable', async () => {
    const scripted = scriptedConnection();
    const transport = new WebSocketTransport(scripted.connection, { clock: new ManualClock() });
    const outcome = await writableAfterEnd(transport, () => scripted.handlers().close(1000, ''));
    expect(outcome).toEqual({ closedSettled: true, laterWriteRejected: true });
    expect(scripted.sent).toEqual([]);
  });

  test('abort errors the writable', async () => {
    const scripted = scriptedConnection();
    const transport = new WebSocketTransport(scripted.connection, { clock: new ManualClock() });
    const outcome = await writableAfterEnd(transport, () => transport.abort());
    expect(outcome).toEqual({ closedSettled: true, laterWriteRejected: true });
    expect(scripted.sent).toEqual([]);
  });
});

/** What [promise] rejects with; fails the test if it resolves. */
async function rejection(promise: Promise<unknown>): Promise<unknown> {
  try {
    await promise;
  } catch (error) {
    return error;
  }
  throw new Error('expected a rejection');
}

/**
 * Takes a writer of [transport], runs [end] (which ends the WebSocket from
 * the other side), and expects `writer.closed` and a later write to reject
 * with CONNECTION_LOST.
 */
async function expectErroredOnceEnded(
  transport: WebSocketTransport,
  end: () => Promise<unknown>,
): Promise<void> {
  const writer = transport.writable.getWriter();
  await end();
  const closed = await rejection(writer.closed);
  expect(closed).toBeInstanceOf(SwitchboardError);
  expect((closed as SwitchboardError).code).toBe(StatusCode.connectionLost);
  const write = await rejection(writer.write(new Uint8Array([2, 2, 0])));
  expect((write as SwitchboardError).code).toBe(StatusCode.connectionLost);
  await transport.closed;
}

describe('review: over ws, the writable once the other side closed', () => {
  test('the ws client: the server closing errors it', async () => {
    const h = await Harness.start({ maxFrameSize: 1024 * 1024 });
    const client = await connectWsWebSocket(h.uri);
    const { transport: server } = await h.accepted.next();
    await expectErroredOnceEnded(client, () => server.writable.close());
  });

  test('the global WebSocket client: the server closing errors it', async () => {
    const h = await Harness.start({ maxFrameSize: 1024 * 1024 });
    const client = await connectWebSocket(h.uri);
    const { transport: server } = await h.accepted.next();
    await expectErroredOnceEnded(client, () => server.writable.close());
  });

  test('the ws server: the client closing errors it', async () => {
    const h = await Harness.start({ maxFrameSize: 1024 * 1024 });
    const client = await connectWsWebSocket(h.uri);
    const { transport: server } = await h.accepted.next();
    await expectErroredOnceEnded(server, () => client.writable.close());
  });

  test('the ws server: the client vanishing errors it', async () => {
    const h = await Harness.start({ maxFrameSize: 1024 * 1024 });
    const client = await connectWsWebSocket(h.uri);
    const { transport: server } = await h.accepted.next();
    await expectErroredOnceEnded(server, async () => client.abort());
  });
});

/** The parts of a `ws` WebSocket that `wsConnection` uses, driven by the test. */
class ScriptedWs extends EventEmitter {
  readyState = 1;
  protocol = 'switchboard';
  bufferedAmount = 0;
  send(_frame: Uint8Array, _options: unknown, _callback?: () => void): void {}
  close(): void {}
  terminate(): void {}
  pause(): void {}
  resume(): void {}
}

describe('review: output readiness over ws and the injected clock', () => {
  test.fails('the wait for bufferedAmount to drop runs on the transport clock', async () => {
    const clock = new ManualClock();
    const ws = new ScriptedWs();
    // The clock goes to the adapter too, as acceptWebSocket and
    // connectWsWebSocket could pass the one of their options; today the
    // adapter takes none and ignores it.
    const transport = new WebSocketTransport(wsConnection(ws as never, { clock } as never), {
      clock,
    });
    ws.bufferedAmount = 200_000;
    expect(transport.isOutputReady).toBe(false);
    // The global timers are stopped: only the clock's time passes.
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
    try {
      let ready = false;
      void transport.outputReady?.then(() => {
        ready = true;
      });
      // The network took the bytes; no send callback says so.
      ws.bufferedAmount = 0;
      await clock.elapseAsync(1000);
      expect({ ready, globalTimers: vi.getTimerCount() }).toEqual({
        ready: true,
        globalTimers: 0,
      });
    } finally {
      vi.useRealTimers();
      transport.abort();
    }
  });
});
