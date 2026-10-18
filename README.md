# Polyverse Switchboard for TypeScript

`@kaetemi/switchboard` is the TypeScript implementation of the Switchboard protocol: multiplexed channels with credit-based flow control, Talk message chains (requests, streams, bulk transfers), service addressing, connection identity, a naming service with sharding, proxying and relaying, over WebSocket, TCP, QUIC and WebTransport. It is a port of the Dart reference implementation ([`kaetemi/switchboard`](https://github.com/kaetemi/switchboard), [`kaetemi/switchboard_quic`](https://github.com/kaetemi/switchboard_quic)) and is wire-compatible with it.

- ES modules only, Node.js 22 or newer, Bun, Deno and browsers.
- `for await` over channels and messages, Web Streams for bytes, `AbortSignal` for cancellation, `await using` for lifetimes.
- QUIC and WebTransport are built in: a native provider for Node, Bun and Deno (prebuilt N-API addons as optional dependencies), the WebTransport API in browsers.

**Status: 3.0.0-dev.1, not published yet.** Every layer of the protocol is ported and tested against the Dart test suites: transports (stream framing, in-memory pairs, WebSocket, TCP), the frame mux, identity (credentials, issuers, verifiers, holder keys, exporter-bound identity over QUIC), Talk, addressing, the node with its listeners, policies, pool, brokering, proxy and relay, the reconnecting client and peer sets, the naming service and client, `MeshNode` with sharded services, slot routing and the slot gate, and the QUIC binding with the native and WebTransport providers. About 3200 unit and integration tests run on Node 22, 24 and 26 (the unit project on Bun as well), the browser tests in Chromium and Firefox, and the Dart reference and this port run as interop peers of each other in every pairing over TCP, WebSocket, QUIC and WebTransport. The design is in [`docs/design.md`](docs/design.md), the state of the port in [`docs/handoff.md`](docs/handoff.md), the user guide in [`docs/wiki/`](docs/wiki/Home.md). The protocol specification is the Switchboard wiki.

## Install

```sh
npm install @kaetemi/switchboard
npm install ws          # Node, Bun, Deno: for WebSocket servers (and a client that bounds message size)
```

## A service and a client

```ts
import { EndpointResolver, Switchboard } from '@kaetemi/switchboard/node';

const server = new Switchboard();
server.registerService('echo', async (incoming) => {
  for await (const message of incoming.talk().messages) {
    if (message.expectsReply) await message.reply(message.text().toUpperCase());
  }
});
const endpoint = await server.listenTcp('127.0.0.1', 0);

const client = new Switchboard({ resolver: new EndpointResolver(endpoint) });
{
  await using talk = await client.openTalk('echo');
  console.log((await talk.request('UPPER', 'hello')).text()); // HELLO
} // the Talk channel closes here

await client.close();
await server.close(); // the listener keeps the process alive until then
```

`await using` needs a runtime that parses it: Node 24 or later, Bun or Deno run the TypeScript directly; on Node 22, whose type stripping cannot parse it, compile with `tsc` first. Close channels before their node: a node's `close()` gives open channels its GOAWAY grace (10 s by default).

## Entry points

| Import | Runtime | Contents |
| --- | --- | --- |
| `@kaetemi/switchboard` | any | the runtime-neutral core: codecs, transports over Web Streams (memory pair, stream framing, WebSocket client), the frame mux, identity, Talk, addressing, the node core with policies, resolvers, proxy, relay, `PeerSet` and `MeshNode`, naming and sharding, the reconnecting client, the QUIC binding over the provider contract |
| `@kaetemi/switchboard/node` | Node, Bun, Deno | the core plus the Node `Switchboard` (TCP, WebSocket, QUIC and WebTransport listeners and dialers), `connectTcp`, `listenTcp`, `listenWebSocket`, `acceptWebSocket`, `connectTransport`, and the native QUIC provider (`nativeQuic()`, `selfSignedCertificate`) |
| `@kaetemi/switchboard/web` | browsers | the core plus a connect-only browser `Switchboard` (`ws`, `wss`, and `https` over WebTransport) and the WebTransport provider (`webTransport()`, `webTransportConnector`) |
| `@kaetemi/switchboard/testing` | tests | `ManualClock` and the fake QUIC provider |

## Developing

```sh
npm ci
npm run check          # lint, typecheck, unit and integration tests
npx tsc -p tsconfig.test.json --noEmit    # type-checks the tests too
npm run build          # dist/
npm run build:native   # the QUIC addon, with Rust (see native/README.md)
npm run test:browser   # WebTransport in Playwright's Chromium and Firefox (needs the addon)
npm run test:interop   # against the Dart reference (needs a Dart SDK and Rust; see test/interop/README.md)
```

## License

BSD-3-Clause. Copyright (C) 2018-2026 Jan BOON (Kaetemi).
