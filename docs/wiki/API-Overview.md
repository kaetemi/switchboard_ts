# API Overview

## Entry points

| Import | Contents |
| --- | --- |
| `@polyverse/switchboard` | the runtime-neutral core |
| `@polyverse/switchboard/node` | the core, TCP and WebSocket listeners and connectors, the native QUIC provider, the Node `Switchboard` |
| `@polyverse/switchboard/web` | the core, the WebTransport provider, a connect-only `Switchboard` |
| `@polyverse/switchboard/testing` | `ManualClock`, the fake QUIC provider, harness helpers |

## Idioms

- **Bytes** are `Uint8Array`. Inputs accept `Uint8Array | string | undefined` (a string is UTF-8, `undefined` is empty); received payloads have `text()`, `json()` and `bytes()`.
- **Numbers** are plain `number`s; channel ids are 48-bit.
- **Names** (`Name`) are 8 bytes; every API taking a name also takes a string.
- **Durations** are milliseconds. `undefined` disables; `0` means none or forever where the protocol says so.
- **Receiving** is pull based: channels, incoming channels, messages, stream items and events are `AsyncIterable`. Taking delivery returns flow-control credit; `break` cancels. Channel bytes are also a `ReadableStream`.
- **Sending**: `send()` resolves on hand-over; channel bytes are also a `WritableStream` with backpressure.
- **Cancellation**: `AbortSignal` on everything that waits; timeouts are options.
- **Lifetimes**: `await using` (`Symbol.asyncDispose`) on connections, channels, listeners and nodes; `done` resolves with the end `Status`.
- **Errors**: `SwitchboardError` carries a `Status`; `ProtocolError` for the peer's violations; `RangeError` and `TypeError` for misuse, thrown synchronously.
- **Time and logging** are injectable (`clock`, `logger` options); the library never touches `console` or the global timers directly.
- **Enumerations** are `const` objects (`StatusCode.ok`), not TS enums.

## Layers

| Layer | Main exports | Protocol page |
| --- | --- | --- |
| Primitives | `Name`, `Status`, `StatusCode`, `SwitchboardError`, `ProtocolError`, `ByteReader`, `ByteWriter` | status-codes |
| Transport | `FrameTransport`, `connectWebSocket`, `connectTcp`, `listenTcp`, `listenWebSocket`, `memoryTransportPair`, `streamTransport` | transport |
| Mux | `MuxConnection`, `MuxChannel`, `MuxOptions`, `FrameMuxConnection`, the control message codecs | mux |
| Identity | `Credential`, `Scope`, `HolderKey`, `CredentialIssuer`, `CredentialVerifier`, `PeerIdentity` | identity |
| Talk | `TalkChannel`, `TalkMessage`, `TalkOptions`, `forwardMessage` | talk |
| Addressing | `ServiceAddress`, `ChannelAddress` | addressing |
| Node | `Switchboard`, `ChannelPolicies`, `IncomingChannel`, `Resolver`, `StaticResolver`, `EndpointResolver`, `pipeChannels`, `proxyHandler` | home, proxying, relay |
| Naming and sharding | `NamingClient`, `NamingService`, `NamingResolver`, `SlotTable`, `SlotGate`, `slotForKey` | naming, sharding |
| Clients | `ReconnectingClient`, `PeerSet`, `MeshNode` | home |
| QUIC | `QuicConnector`, `QuicListener`, `connectQuic`, `listenQuic`, `nativeQuic` (node), `webTransport` (web) | quic-binding |

### The channel contract

Everything above the mux programs against `MuxConnection` and `MuxChannel`, whether the channels ride on mux frames over a `FrameTransport` or on QUIC streams. A connection has `incoming` (peer-opened channels), `open(payload)`, `identify(credential)`, `ping()`, `goAway()`, `close()`, `done` and the identity state. A channel has `send()`, `writable`, `readable`, `[Symbol.asyncIterator]`, `sendWindow`, `receiveWindow`, `close(status)` and `done`. The Talk layer installs the routing of `_bulk` channels, so raw consumers of `incoming` never see them.

### Talk

```ts
const reply = await talk.request('PROC', payload, { timeout, signal });
const stream = talk.streamRequest('PROC', payload);  // AsyncIterable of items; stream.done is the final reply
for await (const message of talk.messages) { await message.reply(...); }
```

Replies: `reply`, `replyItem`, `replyStream`, `replyAbort`, `replyRequest` (a request chained on the reply), `extend` (more time), with `message.signal` aborting when the requester cancels.

### The node

```ts
const node = new Switchboard({ resolver, credential, holderKey, verifier, quic: nativeQuic() });
node.registerService('type', handler, { instance });
await node.listenTcp(host, port, { policy });
const talk = await node.openTalk('type/instance', { shard, payload, signal });
const connection = await node.connect('tcp://host:port');   // pooled
await node.close();
```

The full signatures are in the repository's `src/core/*/contract.ts` files and in `docs/design.md`.
