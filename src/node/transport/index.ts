/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * The Node transports: TCP (`node:net`), the WebSocket server and client
 * over the optional `ws` package (loaded on first use), and
 * `connectTransport` for `tcp`, `ws`, `wss` and `mem` endpoints.
 *
 * Owned by the "protocol core" thread.
 */

export {
  connectTransport,
  type MemoryListenerLookup,
  type TransportConnectOptions,
} from './connect.ts';
export {
  type AcceptedTransport,
  connectTcp,
  listenTcp,
  type TcpConnectOptions,
  TcpListener,
  type TcpListenOptions,
  TcpTransport,
  type TcpTransportOptions,
  tcpTransport,
} from './tcp.ts';
export { connectWebSocket, connectWsWebSocket } from './ws-client.ts';
export {
  type AcceptedWebSocket,
  acceptWebSocket,
  listenWebSocket,
  type WebSocketAcceptOptions,
  WebSocketListener,
  type WebSocketListenOptions,
} from './ws-server.ts';
