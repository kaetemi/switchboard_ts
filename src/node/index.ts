/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * `@kaetemi/switchboard/node`: the server-runtime parts, for Node.js,
 * Bun and Deno (through their `node:` compatibility): the TCP stream
 * transport, the WebSocket server transport, the `Switchboard` node and
 * its listeners, `MeshNode`, `PeerSet`, the proxy, the relay, the
 * instance side of sharding, and the native QUIC provider with the QUIC
 * listener and binding for nodes. Everything of the core is re-exported,
 * so one import serves a service.
 *
 * Owned by the "node and client" thread (`src/node/*.ts`), the
 * "protocol core" thread (`src/node/transport/`) and the "QUIC transport"
 * thread (`src/node/quic/`).
 */

export * from '../core/index.ts';
export * from './quic/index.ts';
// The Node node and its options replace the core's of the same names.
export {
  type AcceptWebSocketOptions,
  type ListenerCertificate,
  type ListenQuicOptions,
  type ListenTcpOptions,
  type ListenWebSocketOptions,
  type ListenWebTransportOptions,
  type QuicListenProvider,
  Switchboard,
  type SwitchboardOptions,
} from './switchboard.ts';
// connectWebSocket replaces the core's: it prefers the ws package.
export {
  type AcceptedTransport,
  type AcceptedWebSocket,
  acceptWebSocket,
  connectTcp,
  connectTransport,
  connectWebSocket,
  connectWsWebSocket,
  listenTcp,
  listenWebSocket,
  type MemoryListenerLookup,
  type TcpConnectOptions,
  TcpListener,
  type TcpListenOptions,
  TcpTransport,
  type TcpTransportOptions,
  type TransportConnectOptions,
  tcpTransport,
  type WebSocketAcceptOptions,
  WebSocketListener,
  type WebSocketListenOptions,
} from './transport/index.ts';
