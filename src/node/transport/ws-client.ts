/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * The client side of the WebSocket binding on Node: the `ws` package's
 * client when it is installed (`maxPayload` bounds a message before it is
 * assembled, and compression is never negotiated), the global `WebSocket`
 * otherwise. Ports `IOWebSocketTransport.connect` of
 * `web_socket_transport_io.dart`.
 */

import {
  connectWebSocket as connectGlobalWebSocket,
  type WebSocketConnectOptions,
  type WebSocketTransport,
} from '../../core/transport/websocket-transport.ts';
import { loadWs, requireWs, wsConnection } from './ws-common.ts';

/**
 * Connects to [url] (`ws://` or `wss://`) with the `ws` package's client:
 * subprotocol `switchboard`, `perMessageDeflate: false`, and
 * `maxPayload = maxFrameSize` (default 1 MiB; 0 for no limit), so that a
 * message over the limit is refused as its fragments arrive, before it is
 * buffered. Otherwise as the core's `connectWebSocket`, whose options it
 * takes. Rejects with `UNSUPPORTED` when `ws` is not installed.
 */
export async function connectWsWebSocket(
  url: string | URL,
  options: Omit<WebSocketConnectOptions, 'webSocket'> = {},
): Promise<WebSocketTransport> {
  const ws = await requireWs('the ws WebSocket client');
  const maxPayload = options.maxFrameSize ?? 1024 * 1024;
  return connectGlobalWebSocket(url, {
    ...options,
    webSocket: (target, protocols) =>
      wsConnection(
        new ws.WebSocket(target, [...protocols], {
          perMessageDeflate: false,
          maxPayload,
          followRedirects: false,
        }),
        { clock: options.clock },
      ),
  });
}

/**
 * Connects to [url] (`ws://` or `wss://`), offering the `switchboard`
 * subprotocol: with the `ws` package's client when it is installed
 * ({@link connectWsWebSocket}), with the global `WebSocket` otherwise (the
 * core's `connectWebSocket`), or with the `webSocket` of [options] when
 * given. Resolves with the transport once the opening handshake completed;
 * rejects as the core's `connectWebSocket` does.
 */
export async function connectWebSocket(
  url: string | URL,
  options: WebSocketConnectOptions = {},
): Promise<WebSocketTransport> {
  if (options.webSocket === undefined && (await loadWs()) !== undefined) {
    return connectWsWebSocket(url, options);
  }
  return connectGlobalWebSocket(url, options);
}
