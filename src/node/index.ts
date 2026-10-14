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
 * Owned by the "node, naming and sharding" thread (`src/node/*.ts`), the
 * "protocol core" thread (`stream-transport.ts`, `web-socket-server.ts`)
 * and the "QUIC transport" thread (`src/node/quic/`).
 */

export * from '../core/index.ts';
export * from './quic/index.ts';
