# Polyverse Switchboard for TypeScript

`@kaetemi/switchboard` is the TypeScript implementation of the Switchboard protocol: multiplexed channels with credit-based flow control, Talk message chains (requests, streams, bulk transfers), service addressing, connection identity, a naming service with sharding, proxying and relaying, over WebSocket, TCP, QUIC and WebTransport. It is a port of the Dart reference implementation ([`kaetemi/switchboard`](https://github.com/kaetemi/switchboard), [`kaetemi/switchboard_quic`](https://github.com/kaetemi/switchboard_quic)) and is wire-compatible with it.

- ES modules only, Node.js 22 or newer, Bun, Deno and browsers.
- `for await` over channels and messages, Web Streams for bytes, `AbortSignal` for cancellation, `await using` for lifetimes.
- QUIC and WebTransport are built in: a native provider for Node, Bun and Deno, the WebTransport API in browsers.

**Status: under construction.** The design is in [`docs/design.md`](docs/design.md); the work split in [`docs/work-split.md`](docs/work-split.md); wiki pages in [`docs/wiki/`](docs/wiki/). The protocol specification is the Switchboard wiki.

## Install

```sh
npm install @kaetemi/switchboard
npm install ws          # Node only, for WebSocket servers
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
await using talk = await client.openTalk('echo');
console.log((await talk.request('UPPER', 'hello')).text()); // HELLO
```

## Entry points

| Import | Runtime | Contents |
| --- | --- | --- |
| `@kaetemi/switchboard` | any | the runtime-neutral core: codecs, mux, Talk, identity, naming, the node, the QUIC binding |
| `@kaetemi/switchboard/node` | Node, Bun, Deno | the core plus TCP and WebSocket listeners and the native QUIC provider |
| `@kaetemi/switchboard/web` | browsers | the core plus the WebTransport provider |
| `@kaetemi/switchboard/testing` | tests | a manual clock, the fake QUIC provider, harness helpers |

## Developing

```sh
npm ci
npm run check          # lint, typecheck, unit and integration tests
npm run test:interop   # against the Dart reference (needs a Dart SDK and Rust; see test/interop/README.md)
npm run build          # dist/
```

## License

BSD-3-Clause. Copyright (C) 2018-2026 Jan BOON (Kaetemi).
