# Polyverse Switchboard for TypeScript

`@kaetemi/switchboard` is the TypeScript implementation of the Switchboard protocol, wire-compatible with the Dart reference implementation. It gives JavaScript and TypeScript programs, on servers and in browsers, the same microservice mesh: multiplexed channels with flow control, Talk message chains, service addressing, connection identity, a naming service with sharding, proxying and relaying, over WebSocket, TCP, QUIC and WebTransport. It runs on Node.js 22 and later, Bun, Deno and browsers, as ES modules.

These pages document the TypeScript package. The protocol itself is specified in the Switchboard wiki (`transport`, `mux`, `talk`, `addressing`, `identity`, `naming`, `sharding`, `proxying`, `relay`, `quic-binding`, `test-vectors`); nothing here changes the wire.

## Pages

- [Getting Started](Getting-Started): install, a service and a client, WebSocket, raw channels, a browser client, Bun and Deno.
- [API Overview](API-Overview): the entry points, the idioms the whole API follows, the layers, the node.
- [Channels and Talk](Channels-and-Talk): connections and channels, flow control, Talk requests, streams, bulk payloads, forwarding, timeouts and cancellation.
- [Identity](Identity): credentials, issuers, holder keys, verifiers, scopes, policies, exporter-bound identity over QUIC.
- [Naming and Sharding](Naming-and-Sharding): the naming service, `MeshNode`, slots, slot routing and the slot gate.
- [Proxy and Relay](Proxy-and-Relay): the proxy, `pipeChannels`, the relay, peer sets.
- [Clients](Clients): the reconnecting client, persistent channels, WebTransport and QUIC connectors.
- [Runtimes and QUIC](Runtimes-and-QUIC): Node, Bun, Deno and browsers; how QUIC and WebTransport are provided on each.
- [Testing and Interop](Testing-and-Interop): the test layers, the wiki vectors, the Dart reference peer, the manual clock.
- [Contributing](Contributing): tooling, conventions, the branch and review process.

## Status

Version 3.0.0-dev.1, not published yet. Every layer of the protocol is ported and tested against the Dart test suites, on Node 22, 24 and 26 and Bun in CI, with browser tests in Chromium and the Dart reference as an interop peer. Landing now: the TypeScript interop peer's cells of the interop matrix.

## Where things are

| | |
| --- | --- |
| Repository | `kaetemi/switchboard_ts` |
| Design document | `docs/design.md` in the repository |
| State of the port | `docs/handoff.md` |
| Reference implementation | `kaetemi/switchboard` (core), `kaetemi/switchboard_quic` (QUIC and WebTransport) |
| Protocol specification | the Switchboard wiki |
