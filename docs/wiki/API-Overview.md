# API Overview

## Entry points

| Import | Contents |
| --- | --- |
| `@kaetemi/switchboard` | the runtime-neutral core: everything below except the Node and browser specifics |
| `@kaetemi/switchboard/node` | the core, plus the Node `Switchboard` (TCP, WebSocket, QUIC and WebTransport listeners and dialers), `connectTcp`, `listenTcp`, `listenWebSocket`, `acceptWebSocket`, `connectWebSocket` (over `ws` when installed), `connectTransport`, and the native QUIC provider (`nativeQuic`, `selfSignedCertificate`, `certificateHash`, `nativeQuicAvailability`) |
| `@kaetemi/switchboard/web` | the core, plus a connect-only browser `Switchboard` and the WebTransport provider (`webTransport`, `webTransportConnector`, `WebTransportProvider`, its sessions and streams) |
| `@kaetemi/switchboard/testing` | `ManualClock` and the fake QUIC provider (`FakeQuicNetwork`, `FakeQuicConnector`, ...) |

`Switchboard` and `SwitchboardOptions` of the node and web entries replace the core's of the same names; import the node from the entry of your runtime.

## Idioms

- **Bytes** are `Uint8Array`. Inputs accept `Uint8Array | string | undefined` (a string is UTF-8, `undefined` is empty); received payloads have `text()`, `json()` and `bytes()`.
- **Numbers** are plain `number`s; channel ids are 48-bit, so the library never uses bitwise operators on them.
- **Names** (`Name`) are 8 bytes; every API taking a name also takes a string (`Name.of` refuses more than 8 bytes of UTF-8).
- **Durations** are milliseconds. `undefined` disables; `0` means none or forever where the option says so.
- **Receiving** is pull based: channels, incoming channels, messages, stream items and events are `AsyncIterable`, each with one consumer. Taking delivery returns flow-control credit; `break` stops delivery. Channel bytes are also a `ReadableStream`.
- **Sending**: `send()` resolves on hand-over; channel bytes are also a `WritableStream` with backpressure.
- **Cancellation**: `AbortSignal` on everything that waits; timeouts are options.
- **Lifetimes**: `await using` (`Symbol.asyncDispose`) on connections, channels, Talk channels, listeners, clients and nodes; `done` resolves with the end `Status` and never rejects; `close()` is idempotent.
- **Errors**: `SwitchboardError` carries a `Status` (`error.status`, `error.code`, `error.is(StatusCode.notFound)`, `isStatus(error, code)`); `ProtocolError` for the peer's violations; `TalkAbortError` for an ABORT; `RangeError` and `TypeError` for misuse, thrown synchronously. Promises the library hands out never become unhandled rejections.
- **Time and logging** are injectable (`clock`, `logger` options); the library never touches `console` or the global timers directly. `consoleLogger` logs to the console.
- **Enumerations** are `const` objects (`StatusCode.ok`, `Right.open`, `CredentialKind.node`), not TS enums.

## Layers

| Layer | Main exports | Page | Protocol page |
| --- | --- | --- | --- |
| Primitives | `Name`, `Status`, `StatusCode`, `SwitchboardError`, `ProtocolError`, `ByteReader`, `ByteWriter`, `utf8Encode`, `utf8Decode` | | status-codes |
| Addressing | `ServiceAddress`, `ChannelAddress` | | addressing |
| Transport | `FrameTransport`, `memoryTransportPair`, `MemoryEndpoints`, `streamTransport`, `StreamFraming`, `connectWebSocket`; node: `connectTcp`, `listenTcp`, `listenWebSocket`, `acceptWebSocket`, `connectTransport` | [Runtimes and QUIC](Runtimes-and-QUIC) | transport |
| Mux | `MuxConnection`, `MuxChannel`, `MuxOptions`, `FrameMuxConnection`, the frame and control codecs | [Channels and Talk](Channels-and-Talk) | mux |
| Identity | `Credential`, `Scope`, `Right`, `CredentialKind`, `CredentialIssuer`, `CredentialVerifier`, `Ed25519HolderKey`, `PeerIdentity` | [Identity](Identity) | identity |
| Talk | `TalkChannel`, `TalkMessage`, `TalkStream`, `TalkOptions`, `TalkAbortError`, `forwardMessage` | [Channels and Talk](Channels-and-Talk) | talk |
| Node | `Switchboard`, `ChannelPolicies`, `IncomingChannel`, `Resolver`, `StaticResolver`, `EndpointResolver` | this page | addressing, home |
| Proxy and relay | `proxyHandler`, `pipeChannels`, `RelayService`, `RelayConfig`, `PeerSet` | [Proxy and Relay](Proxy-and-Relay) | proxying, relay |
| Naming and sharding | `NamingService`, `NamingClient`, `NamingResolver`, `MeshNode` (`join`, `publish`, `publishSharded`, `leave`), `slotForKey`, `slotForText`, `SlotSpace`, `SlotTable`, `SlotGate`, `SlotLifecycle`, `SlotGates`, `SlotChannel`; the node's `openChannelToSlot`, `openTalkToSlot`, `resolveSlotOwner` | [Naming and Sharding](Naming-and-Sharding) | naming, sharding |
| Clients | `ReconnectingClient`, `PersistentChannel`, `PersistentTalk`, `ClientState` | [Clients](Clients) | home |
| QUIC | `QuicConnector`, `QuicListener`, `connectQuic`, `listenQuic`, `quicConnector`, `QuicBindingConnection`, `QuicIdentityRules`; node: `nativeQuic`; web: `webTransport`, `webTransportConnector` | [Runtimes and QUIC](Runtimes-and-QUIC) | quic-binding |

### The channel contract

Everything above the mux programs against `MuxConnection` and `MuxChannel`, whether the channels ride on mux frames over a `FrameTransport` or on QUIC streams. A connection has `incoming` (peer-opened channels), `open(payload)`, `identify(credential, options)`, `ping()`, `goAway()`, `close()`, `done` and the identity state (`peerIdentity`, `peerIdentified`). A channel has `send()`, `writable`, `readable`, `[Symbol.asyncIterator]`, `sendWindow`, `receiveWindow`, `close(status)` and `done`. The Talk layer installs the routing of `_bulk` channels, so raw consumers of `incoming` never see them. See [Channels and Talk](Channels-and-Talk).

### The node

A `Switchboard` registers services, listens, dispatches the channels peers open to the services (under the listener's policy), pools its outgoing connections and opens channels to services anywhere through its `resolver`.

```ts
import { ChannelPolicies, EndpointResolver, Switchboard } from '@kaetemi/switchboard/node';

const node = new Switchboard({
  resolver: new EndpointResolver('tcp://10.0.0.2:4000'), // or a NamingResolver, a StaticResolver
  // credential, holderKey, verifier: see Identity; quic: nativeQuic(): see Runtimes and QUIC
});
node.registerService('npc', (incoming) => incoming.talk().serve((m) => m.reply('hi')), { instance: 7 });
const endpoint = await node.listenTcp('0.0.0.0', 4000, { policy: ChannelPolicies.denyReserved });

{
  await using talk = await node.openTalk('npc/7', { shard: 3, payload: 'credential' });
  await using raw = await node.openChannel('blob');
  const connection = await node.connect('tcp://10.0.0.3:4000'); // pooled; the node owns it
  console.log(endpoint, node.listeningEndpoints, connection.isOpen);
}
await node.close();
```

| Member | |
| --- | --- |
| `registerService(type, handler, { instance, acceptAnyInstance })`, `unregisterService(type, instance)` | a handler per `(type, instance)`; instance 0 does not receive channels addressed to another instance unless `acceptAnyInstance` |
| `defaultService`, `catchAll` | the handlers of channels without a type, and of everything nothing else serves (a proxy) |
| `openChannel(address, { shard, payload, where, signal })`, `openTalk(address, { ..., talkOptions })` | resolve the type, select an instance (round robin, or by slot), connect, open |
| `openChannelAt(endpoint, channelAddress)`, `openTalkAt(...)` | open at an explicit endpoint, bypassing resolution |
| `openChannelOn(connection, channelAddress)` | open on an existing connection (for example back to a peer: dispatch is symmetric) |
| `openChannelToSlot(type, slot)`, `openTalkToSlot(type, slot)`, `resolveSlotOwner(type, slot)` | slot routing with the `MOVED` retry; a sharded type is served by a `SlotGate` over your `SlotLifecycle`, published with `MeshNode.publishSharded` ([Naming and Sharding](Naming-and-Sharding)) |
| `connect(endpoint, { policy, signal })`, `dial(endpoint, { intent, receiver, ... })` | a pooled connection, and one of your own |
| `listenTcp`, `listenWebSocket`, `acceptWebSocket`, `listenQuic`, `listenWebTransport` (node entry), `listenMemory` (every entry) | listeners; each resolves with its bound endpoint as a string and takes a `policy` |
| `accept(transport, { policy })`, `adopt(connection, { policy })` | connections accepted elsewhere |
| `identifyOn(connection)`, `updateCredential(credential)`, `broker(address)` | identity and brokering ([Identity](Identity)) |
| `connections`, `listeningEndpoints`, `isOwnEndpoint(endpoint)` | state |
| `close()` | stops listening, sends GOAWAY on every connection and waits for their channels (up to `goAwayGrace`); never rejects |

Options (durations in ms): `resolver`, `defaultPayload`, `muxOptions`, `talkOptions`, `connectTimeout` (10 000), `maxConnectionsPerEndpoint` (4), `selection` (`'roundRobin'` or `'random'`), `slotRefreshTimeout`, `outgoingPolicy` and `endpointPolicy` (the policy of the connections the node initiates), `credentialFor`, `credential`, `holderKey`, `verifier`, `identifyOutgoing`, `identifyFor`, `identityTimeout`, `expectedIdentityFor`, `relay`, `brokerEndpoint`, `allowHostHint`, `localConnections` (connections to its own listeners are made in-process; default true), `bindings`, `clock`, `logger`; the node and web entries add `quic` (a QUIC provider) and `quicBinding`.

A handler receives an `IncomingChannel`: `channel` (the `MuxChannel`), `address` (the parsed header and application payload), `connection`, `peerIdentity`, `remote`, `talk(options?)` (wraps once; returns the `TalkChannel`) and `reject(status)`. A handler that throws or rejects closes the channel with `INTERNAL`. The policies are `ChannelPolicies.allowAll`, `denyAll`, `denyReserved`, `allowTypes(types)`, `scoped()`, `requireIdentity()` and `all(policies)`; a policy is a function `(address, connection) => boolean`.

The full signatures are in the doc comments of the package (and the repository's `src/core/*/contract.ts` files and `docs/design.md`).
