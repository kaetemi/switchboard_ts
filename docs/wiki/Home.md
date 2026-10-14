# Polyverse Switchboard for TypeScript

`@polyverse/switchboard` is the TypeScript implementation of the Switchboard protocol, wire-compatible with the Dart reference implementation. It gives JavaScript and TypeScript programs, on servers and in browsers, the same microservice mesh: multiplexed channels with flow control, Talk message chains, service addressing, connection identity, a naming service with sharding, proxying and relaying, over WebSocket, TCP, QUIC and WebTransport.

These pages document the TypeScript package. The protocol itself is specified in the Switchboard wiki (`transport`, `mux`, `talk`, `addressing`, `identity`, `naming`, `sharding`, `quic-binding`, `test-vectors`); nothing here changes the wire.

## Pages

- [Getting Started](Getting-Started): install, a service and a client, raw channels, a browser client.
- [Runtimes and QUIC](Runtimes-and-QUIC): Node, Bun, Deno and browsers; how QUIC and WebTransport are provided on each.
- [API Overview](API-Overview): the entry points and the layers, with the idioms the whole API follows.
- [Testing and Interop](Testing-and-Interop): the test layers, the wiki vectors and the Dart reference peer.
- [Contributing](Contributing): tooling, conventions, the branch and review process.

## Where things are

| | |
| --- | --- |
| Repository | `kaetemi/switchboard_ts` |
| Design document | `docs/design.md` in the repository |
| Work split | `docs/work-split.md` |
| Reference implementation | `kaetemi/switchboard` (core), `kaetemi/switchboard_quic` (QUIC and WebTransport) |
| Protocol specification | the Switchboard wiki |
