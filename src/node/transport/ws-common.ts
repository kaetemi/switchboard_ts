/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * The `ws` package, loaded lazily and optionally, and its adapter to the
 * core's `WebSocketConnection`. Nothing here imports `ws` at module load:
 * a consumer without it can use TCP, and the WebSocket client falls back
 * to the global `WebSocket`.
 */

import type { WebSocket as WsWebSocket } from 'ws';
import { ProtocolError, StatusCode, SwitchboardError } from '../../core/status.ts';
import {
  type WebSocketConnection,
  WebSocketReadyState,
} from '../../core/transport/websocket-transport.ts';

/** The module of the `ws` package. */
export type WsModule = typeof import('ws');

let loading: Promise<WsModule | undefined> | undefined;

/**
 * The `ws` package, or undefined when it is not installed. Loaded once, on
 * first use.
 */
export function loadWs(): Promise<WsModule | undefined> {
  loading ??= import('ws').then(
    (module) => {
      const loaded = module as WsModule & { readonly default?: unknown };
      if (typeof loaded.WebSocketServer === 'function' && typeof loaded.WebSocket === 'function') {
        return loaded;
      }
      // An environment that offers only the CommonJS shape.
      const fallback = loaded.default as Partial<WsModule> | undefined;
      return typeof fallback?.WebSocketServer === 'function' ? (fallback as WsModule) : undefined;
    },
    () => undefined,
  );
  return loading;
}

/** The `ws` package, or a `SwitchboardError` `UNSUPPORTED` naming what to install. */
export async function requireWs(what: string): Promise<WsModule> {
  const ws = await loadWs();
  if (ws === undefined) {
    throw SwitchboardError.of(
      StatusCode.unsupported,
      `${what} needs the ws package: npm install ws`,
    );
  }
  return ws;
}

/** The `ws` error codes of messages over `maxPayload`. */
const tooLargeCodes: ReadonlySet<string> = new Set([
  'WS_ERR_UNSUPPORTED_MESSAGE_LENGTH',
  'WS_ERR_UNSUPPORTED_DATA_PAYLOAD_LENGTH',
]);

/**
 * The error a `ws` error means for the transport: a message over the limit
 * is `FRAME_TOO_LARGE`, a WebSocket protocol violation (bad frame, close
 * code or UTF-8) a `ProtocolError`; anything else (a socket error) as it
 * is.
 */
export function wsFailure(error: unknown): unknown {
  const code = (error as { readonly code?: unknown } | undefined)?.code;
  if (typeof code !== 'string' || !code.startsWith('WS_ERR_')) {
    return error;
  }
  const message = error instanceof Error ? error.message : String(error);
  if (tooLargeCodes.has(code)) {
    return SwitchboardError.of(StatusCode.frameTooLarge, message, { cause: error });
  }
  return new ProtocolError(message, { cause: error });
}

/** Options of {@link wsConnection}. */
export interface WsConnectionOptions {
  /** Told of every sign of life from the peer: a message, a ping or a pong. */
  readonly onActivity?: (() => void) | undefined;
}

/**
 * Adapts [socket], a `ws` WebSocket (connecting or open), for the core's
 * `WebSocketTransport`: binary messages as bytes, pausing, output
 * observation through the send callbacks, and teardown with `terminate()`.
 */
export function wsConnection(
  socket: WsWebSocket,
  options: WsConnectionOptions = {},
): WebSocketConnection {
  const onActivity = options.onActivity;
  const waiters: { readonly bytes: number; readonly resolve: () => void }[] = [];
  const check = (): void => {
    const buffered = socket.bufferedAmount;
    const gone = socket.readyState === WebSocketReadyState.closed;
    for (let i = waiters.length - 1; i >= 0; i--) {
      const waiter = waiters[i] as (typeof waiters)[number];
      if (gone || buffered <= waiter.bytes) {
        waiters.splice(i, 1);
        waiter.resolve();
      }
    }
  };
  return {
    get readyState() {
      return socket.readyState;
    },
    get protocol() {
      return socket.protocol;
    },
    get bufferedAmount() {
      return socket.bufferedAmount;
    },
    listen(handlers) {
      socket.on('open', () => handlers.open());
      socket.on('message', (data, isBinary) => {
        onActivity?.();
        if (!isBinary) {
          handlers.message(String(data));
          return;
        }
        if (Array.isArray(data)) {
          handlers.message(new Uint8Array(Buffer.concat(data)));
        } else if (data instanceof ArrayBuffer) {
          handlers.message(new Uint8Array(data));
        } else {
          handlers.message(new Uint8Array(data.buffer, data.byteOffset, data.byteLength));
        }
      });
      if (onActivity !== undefined) {
        socket.on('ping', onActivity);
        socket.on('pong', onActivity);
      }
      socket.on('error', (error) => handlers.error(wsFailure(error)));
      socket.on('close', (code, reason) => {
        check();
        handlers.close(code, reason.toString());
      });
    },
    send(frame) {
      socket.send(frame, { binary: true }, check);
    },
    close(code, reason) {
      socket.close(code, reason);
    },
    terminate() {
      socket.terminate();
    },
    pause() {
      socket.pause();
    },
    resume() {
      socket.resume();
    },
    whenBufferedAtMost(bytes) {
      if (socket.bufferedAmount <= bytes || socket.readyState === WebSocketReadyState.closed) {
        return Promise.resolve();
      }
      return new Promise<void>((resolve) => waiters.push({ bytes, resolve }));
    },
  };
}
