# Polyverse Switchboard for TypeScript: design

_Status: v2, 2026-10-14, brought in step with the code on `main`. The lead owns this document; it is refined as the port proceeds. Decisions are stated as decisions; open questions are collected at the end._

`@kaetemi/switchboard` is the TypeScript port of the Switchboard protocol: multiplexed channels, Talk message chains, service addressing, identity, the naming service and sharding, over WebSocket, TCP, QUIC and WebTransport. It targets modern JavaScript and TypeScript consumers with ES modules, and makes the protocol feel native to the platform: `for await` over channels and messages, Web Streams for bytes, `AbortSignal` for cancellation, promises everywhere a Dart future was, and `await using` for lifetimes.

QUIC is an integral part of the package, not an experimental add-on: the binding is in the core, a WebTransport provider ships for browsers, and a native QUIC provider ships for Node, Bun and Deno.

## 1. Sources of truth and precedence

1. **The wiki** in `kaetemi/sway` under `wiki/switchboard/` is the normative protocol specification: `home`, `transport`, `mux`, `status-codes`, `talk`, `addressing`, `identity`, `naming`, `sharding`, `proxying`, `relay`, `quic-binding`, `embedded-profile`, `test-vectors`. When the wiki and a Dart implementation disagree, the wiki wins unless `test-vectors.md` is amended.
2. **The Dart reference implementation**, `kaetemi/switchboard` and `kaetemi/switchboard_quic`, at the commits pinned in `test/interop/dart_peer/dart_commits.txt`. `doc/dart-design.md` in `switchboard` records every layer's API contract and the behaviours the Dart tests assert; the TS port keeps the same behaviours and the same names where the platform allows.
3. **The wire is byte-identical.** Nothing in this port changes the protocol. Every change in observable behaviour relative to Dart is listed in section 12.
4. The porting notes the threads wrote are inputs, not sources: `docs/notes/*.md`, with the Dart test inventory in `docs/notes/interop-dart-suites.md`.

The Dart repositories are read-only for this project.

## 2. What it looks like

A service and a client on one Node process:

```ts
import { EndpointResolver, Status, StatusCode, Switchboard } from '@kaetemi/switchboard/node';

const server = new Switchboard();
server.registerService('echo', async (incoming) => {
  const talk = incoming.talk();
  for await (const message of talk.messages) {
    switch (message.procedureName) {
      case 'UPPER':
        await message.reply(message.text().toUpperCase());
        break;
      case 'COUNT':
        for (let i = 1; i <= Number(message.text()); i++) await message.replyItem(String(i));
        await message.reply('done');
        break;
      default:
        await message.replyAbort(Status.of(StatusCode.unimplemented));
    }
  }
});
const endpoint = await server.listenTcp('127.0.0.1', 0); // 'tcp://127.0.0.1:41234'

const client = new Switchboard({ resolver: new EndpointResolver(endpoint) });
{
  await using talk = await client.openTalk('echo');

  const reply = await talk.request('UPPER', 'hello');
  console.log(reply.text()); // HELLO

  const stream = talk.streamRequest('COUNT', '3');
  for await (const item of stream) console.log(item.text()); // 1 2 3
  console.log((await stream.done).text()); // done
} // the Talk channel closes here

await client.close();
await server.close();
```

The block matters: `close()` sends GOAWAY and waits up to `goAwayGrace` (10 s) for the channels still open, so a Talk channel left open until the end of the module holds the client's close for that long.

Raw channels, bytes in and out, with backpressure:

```ts
const channel = await client.openChannel('blob', { shard: 7 });
await channel.send(new Uint8Array(1024));
for await (const chunk of channel) handle(chunk); // credit returns as chunks are taken
await channel.close();
```

A browser client over WebTransport, reconnecting, with a persistent channel:

```ts
import { ReconnectingClient, webTransportConnector } from '@kaetemi/switchboard/web';

const client = new ReconnectingClient(
  webTransportConnector('https://edge.example.com:4433/sb', {
    serverCertificateHashes: [{ algorithm: 'sha-256', value: hash }],
  }),
);
client.openPersistentTalk('chat', async (talk) => {
  for await (const message of talk.messages) render(message.json());
});
```

Identity, scoped policy, QUIC on Node:

```ts
import {
  ChannelPolicies,
  CredentialIssuer,
  CredentialKind,
  CredentialVerifier,
  Ed25519HolderKey,
  Right,
  Scope,
  Switchboard,
  nativeQuic,
  selfSignedCertificate,
} from '@kaetemi/switchboard/node';

const issuer = await CredentialIssuer.ed25519FromSeed('auth1', authoritySeed); // the mesh authority
const holderKey = await Ed25519HolderKey.generate(); // this node's key pair
const verifier = new CredentialVerifier({ ed25519Keys: { auth1: await issuer.publicKey() } });

const node = new Switchboard({
  credential: await issuer.issue({
    kind: CredentialKind.node,
    identity: 'edge-1',
    scopes: [Scope.of(Right.open, '*')],
    holderKey: holderKey.publicKey,
  }),
  holderKey,
  verifier,
  quic: nativeQuic(),
});
const quic = await node.listenQuic('0.0.0.0', 4433, {
  certificate: selfSignedCertificate('edge.example.com'), // synchronous; also the default
  policy: ChannelPolicies.scoped(),
});
```

These examples are checked against the built package (the first and the last run on Node 22, Bun and Deno; the browser one type-checks).

## 3. Runtime targets

| Runtime | Entry | Transports | QUIC and WebTransport | Status |
| --- | --- | --- | --- | --- |
| Node.js 22, 24 and 26 | `@kaetemi/switchboard/node` | TCP (`node:net`), WebSocket client (`ws` when installed, else the global `WebSocket`), WebSocket server (`ws`), in-memory | native provider (N-API addon over quinn) | primary target; unit and integration projects in CI |
| Bun 1.x | `@kaetemi/switchboard/node` | same, through Bun's Node compatibility | native provider (Bun loads N-API addons) | unit project in CI; TCP, WebSocket and QUIC round trips checked by hand |
| Deno 2.x | `@kaetemi/switchboard/node` | same, through `node:` compatibility and npm specifiers | native provider (`--allow-ffi`) | checked by hand on Deno 2.9 (TCP, WebSocket, QUIC); not in CI |
| Browsers (evergreen) | `@kaetemi/switchboard/web` | WebSocket (the global `WebSocket`) | WebTransport provider; `protocols` from Chrome 143, Firefox 155, Safari 26.4; the exporter on Firefox 155+ | Chromium 141 in CI (Playwright 1.56.1); Firefox and Safari manual |
| Workers and edge runtimes | `@kaetemi/switchboard` (core) with a caller-supplied transport | WebSocket where offered | none | not a target; nothing in the core prevents it |

The **core entry** `@kaetemi/switchboard` has no runtime dependency beyond the Web platform APIs that all of these share: `Uint8Array`, `DataView`, `TextEncoder`, `ReadableStream`, `WritableStream`, `AbortSignal`, `crypto.subtle`, `crypto.getRandomValues`, `performance.now`, `queueMicrotask`, timers. It never imports a `node:` module, `ws`, or a DOM-only API. The node and web entries re-export the core and add the runtime specific transports and providers.

Supported language level: ES2023 output, ES modules only (no CommonJS build; Node 22 `require()` of ESM works for synchronous graphs if a consumer needs it). TypeScript consumers get `.d.ts` with `exactOptionalPropertyTypes` friendly signatures. Node 22's built-in type stripping cannot parse `await using`; a consumer running TypeScript sources directly needs Node 24 or later, or compiles with `tsc` (which lowers it for ES2023).

### 3.1 How QUIC is provided

The protocol needs a QUIC implementation that exposes bidirectional streams, per-stream resets with an application code, connection close with a code and reason, and keying-material export for exporter-bound identity. No JavaScript runtime offers that today: Node 22 has no `node:quic`, the `node:quic` of later releases is experimental and has no WebTransport, Bun and Deno have no QUIC API, and browsers offer only WebTransport (no raw QUIC, and in Chromium no `exportKeyingMaterial` yet).

**Decision.** Two providers behind one contract, `QuicConnector` and `QuicListener` in `src/core/quic/contract.ts`:

1. **Native provider** (`@kaetemi/switchboard/node`, `nativeQuic()`): an N-API addon written in Rust with `napi-rs`, in `native/`, over `quinn` 0.11 and `rustls`, serving raw QUIC with ALPN `switchboard` and WebTransport over HTTP/3, both as client and as server. The addon is built per platform and published as optional dependencies: `@kaetemi/switchboard-quic-linux-x64-gnu`, `-linux-arm64-gnu`, `-darwin-arm64`, `-darwin-x64` and `-win32-x64-msvc` (`native/README.md`; manifests under `native/npm/<target>/`; `.github/workflows/release.yml` builds and packs them on manual dispatch). The node entry loads it lazily on first use (`$SWITCHBOARD_QUIC_ADDON`, then `native/sbq-node.node`, then the per-platform package), so a consumer that never touches QUIC never loads native code. Without a binary the provider fails with `QuicError` `'unsupportedScheme'`, whose message names the package to install (`nativeQuicAvailability()` says why without connecting); a node dialling through it reads that as `UNIMPLEMENTED` and moves on to a record's next endpoint. One addon serves Node, Bun and Deno, since all three load N-API modules.
2. **WebTransport provider** (`@kaetemi/switchboard/web`, `webTransport(init?)`, a `QuicConnector` for `connectQuic`, `quicConnector` and the browser node's `quic` option; `webTransportConnector(url, init)` gives a `ReconnectingClient` its connector): the W3C WebTransport API as the browser offers it. Raw QUIC is not available in browsers. The Chromium quirks the Dart reference documents apply (Chromium 141, as tested here, has no `protocol`, `draining`, `exportKeyingMaterial`, `getStats`, `createSendGroup` or `sendOrder`, so identity is nonce-bound; stream error codes are 8-bit), and every optional member is feature-tested.

The addon reuses the Dart package's shim: `switchboard_quic/native/sbq` already implements the connection and stream model, HTTP/3 and WebTransport over quinn, with 11 000 lines including tests, and it is the reference the Dart tests exercise. The user approved reusing it (2026-10-14). The addon mirrors the C ABI function for function (handles as numbers, `sbq_poll` filling an event array, the doorbell as a non-blocking thread-safe function with a weak reference so an idle pump does not hold the process open); all protocol logic stays in TypeScript, and the TS native provider is a direct port of the Dart one. How the addon depends on the shim:

- **Decided (2026-10-14, the user's call):** duplicate the shim now. `native/sbq-vendor/` holds `switchboard_quic/native/sbq` at the commit recorded in `native/sbq-vendor.commit` (outside the copy, so the copy stays byte-identical to upstream apart from the crate-type line), with one change, `"rlib"` added to the crate types so the addon can depend on it by path; `tool/sync-sbq.mjs` refreshes it from a `switchboard_quic` checkout, and the `native` and `browser` CI jobs verify (`--check`) that the vendored tree equals upstream at that commit except for that line. No change is made to `switchboard_quic`. The copy stays cleanly separable: its own directory, no TS-specific edits to the Rust beyond the crate-type line, the N-API crate as a separate crate next to it. **Intended direction:** once the port is ready, the shim may move to a dedicated repository shared by the Dart and TypeScript packages; the separation above makes that extraction a move, not a rewrite.

Alternatives considered and rejected: a pure FFI layer over the unchanged cdylib (`koffi`, `bun:ffi`, `Deno.dlopen`: two native artifacts per platform, three loaders, blocking-callback rules; kept as a possible later fallback since the addon mirrors the C ABI), `@fails-components/webtransport` (a second QUIC stack with its own dialect, no raw QUIC, fails in Deno), a fresh quinn binding (would re-derive what `sbq` settled), Deno's built-in QUIC (no exporter, no pinning, `--unstable-net`), and WASM (a possible no-prebuild fallback later).

`node:quic` stays a possibility as a third provider behind the same contract once it stabilises and grows keying-material export; nothing above the provider contract would change.

### 3.2 Bun and Deno

Bun runs the Node entry unchanged; its WebSocket and `node:net` compatibility is sufficient, and it loads N-API addons. Deno 2 runs it through its `node:` compatibility and npm specifiers with `--allow-net --allow-read --allow-env --allow-ffi` (`--allow-ffi` for the addon; `--allow-net` does not constrain the addon's sockets); the WebSocket server needs `ws` in a `node_modules` directory (for a lone script, for example `--node-modules-dir=manual` next to an installed `ws`). Neither gets a separate entry point. CI runs the unit project on Bun; Deno is run by hand (TCP, WebSocket and native QUIC round trips with the built package on Deno 2.9) until the suite is known to pass there, then added.

## 4. Package layout and module boundaries

```
switchboard_ts/
  package.json               @kaetemi/switchboard, ESM, exports: ., ./node, ./web, ./testing, ./package.json
  src/
    core/                    runtime neutral; never imports node:, ws, or DOM-only APIs
      index.ts               gathers the barrels below
      bytes.ts               ByteReader, ByteWriter, u48 helpers, hex, utf8, PayloadLike
      name.ts                Name (8-byte), NameLike
      status.ts              StatusCode, Status, SwitchboardError, ProtocolError
      address/               ServiceAddress, ChannelAddress
      transport/             contract.ts (FrameTransport); stream framing and transport, memory pair,
                             MemoryEndpoints, parseEndpoint, the WebSocket client over the global WebSocket
      mux/                   contract.ts (MuxConnection, MuxChannel, MuxOptions); frame and control codecs,
                             IDENT verification, FrameMuxConnection, FrameMuxChannel, output scheduler,
                             BindingConnection base
      identity/              contract.ts; Credential and Scope codecs, CredentialIssuer, CredentialVerifier,
                             Ed25519HolderKey, Ed25519 over WebCrypto, PeerIdentity, secureRandomBytes
      talk/                  contract.ts (TalkOptions, TalkMessage, TalkStream, ...); frame codec,
                             TalkChannel, messages, requests, bulk transfers and routing, forwardMessage
      naming/                _ns and sharding codecs, NamingClient, NamingService, NamingResolver,
                             slot keys and tables, SlotGate, SlotLifecycle, SlotGates, SlotChannel,
                             reopenAtSlotOwner
      switchboard/           contract.ts (handlers, policies, resolvers); the Switchboard node core,
                             ChannelPolicies, IncomingChannel, StaticResolver, EndpointResolver,
                             isSlotResolver, proxy, relay, PeerSet, MeshNode, the runtime seam
      client/                ReconnectingClient, PersistentChannel, PersistentTalk, Backoff (internal)
      quic/                  contract.ts (QuicConnector, QuicListener, QuicSession, QuicStream);
                             records, HELLO, exporter, QuicBindingConnection, QuicBindingChannel,
                             connectQuic, quicConnector, listenQuic, QuicSwitchboardBinding,
                             QuicSwitchboardListener, QuicIdentityRules
      util/                  Clock, Logger, Deferred, Emitter, AsyncQueue
    node/                    Node, Bun, Deno: re-exports the core
      switchboard.ts         the Node Switchboard (listenTcp, listenWebSocket, acceptWebSocket,
                             listenQuic, listenWebTransport; tcp, ws, wss, quic, https dialers)
      transport/             connectTcp, listenTcp, the ws server and client, connectTransport
      quic/                  nativeQuic(), the lazy addon loader, selfSignedCertificate, certificateHash
    web/                     browsers: re-exports the core
      switchboard.ts         the connect-only browser Switchboard (ws, wss, https with a quic provider)
      quic/                  the WebTransport provider: webTransport(), webTransportConnector, sessions,
                             streams, the structural W3C types
    testing/                 ManualClock, the fake QUIC provider
  native/                    the N-API addon crate (Rust), sbq-vendor/, build.mjs, npm/<target>/ manifests
  test/
    unit/                    fast, in process, one module at a time (node suites also over-frame-mux/)
    vectors/                 the wiki test vectors (by id from wiki-vectors.json) and the Dart-only vectors
    integration/             nodes end to end over memory, TCP, WebSocket and QUIC on the loopback
    browser/                 the web provider in Playwright's Chromium (its own vitest config)
    interop/                 the Dart peer, the TS peer, the launcher, the pairing matrix, oracle, probes
    support/                 fixtures the suites share
  docs/
    design.md                this document
    work-split.md            the original per-thread assignments, with what is done
    handoff.md               the state of the port for a session starting fresh
    wiki/                    wiki-ready pages (see Home.md)
  tool/                      dart-peer.mjs, extract-wiki-vectors.mjs, sync-sbq.mjs
  .github/workflows/         ci.yml, release.yml (manual dispatch draft)
```

**Boundaries that hold the parallel work together:**

- Every layer's public interface lives in that directory's `contract.ts`, owned by the lead. A layer programs against the contracts of the layers below and exports its surface from its directory's `index.ts`. A change to a contract is its own commit with a rationale, never a silent edit.
- `src/core` is runtime neutral, enforced by review (`lib` without Node types for the core is a follow-up) and by the unit project running under Bun in CI.
- The QUIC binding depends only on `quic/contract.ts` and implements `mux/contract.ts`. Talk and everything above depend only on `mux/contract.ts` and `hasOrderedChannels`; they never see frames, CREDIT or the scheduler.
- The `_bulk` filter and the "admit after GOAWAY" rule are installed on a connection by the Talk layer (`installBulkRouting`) through two hooks on `MuxConnection` (`adoptIncoming(filter)` and `admitAfterGoAway(predicate)`), so the mux does not decode address headers and raw `incoming` consumers never see `_bulk` channels. This replaces Dart's `adoptBulk` call that raw consumers had to make.
- `MeshNode` and `PeerSet` live in `core/switchboard/` beside the node they drive, not in `core/client/`; `SlotGate` lives in `core/naming/`; `isSlotResolver` and `isBrokeringResolver` in `core/switchboard/resolver.ts`.

## 5. API idioms

These are the rules every layer follows. They are what "natively convenient" means here.

| Concern | Rule |
| --- | --- |
| Bytes | `Uint8Array` in and out. Node `Buffer` is accepted (it is a `Uint8Array`) and normalised at the transport boundary. Decoders return views, framing copies out of its buffer, OPEN payloads are copied, `send()` copies on hand-over, as in Dart. |
| Payloads | Everywhere Dart takes `Uint8List`, TS takes `PayloadLike = Uint8Array \| string \| undefined`: a string is UTF-8 encoded, `undefined` is empty. Received payloads come with `text()`, `json()` and `bytes()` helpers. |
| Integers | `number` everywhere. Channel ids are u48 and exceed 2^32, so no bitwise operators on ids, windows or byte counts; `ByteReader.u48` and `ByteWriter.u48` split as `lo + hi * 2**32`. No `bigint`. |
| Names | `Name` with a `key` string (the 8 code units) for `Map` keys and `equals()`. Every API that names a service, procedure or scope accepts `NameLike = Name \| string`. |
| Durations | milliseconds as `number`. `undefined` disables where Dart used `null`; `0` means "none" or "forever" exactly where Dart's `Duration.zero` did, documented per option. |
| Enumerations | `const` objects `as const` with a union type (`StatusCode.ok`, `type StatusCode`), never TS `enum`, for erasable syntax. |
| Options | plain objects with `readonly` fields and a frozen `defaultXOptions`; `xOptions(partial)` merges; constructors validate like Dart and throw `RangeError` or `TypeError` for misuse. |
| Errors | `SwitchboardError` carries a `Status`; `ProtocolError` extends it; argument misuse throws `RangeError`/`TypeError`. Methods that validate synchronously in Dart (`send`, `open`, `goAway`, `close(status)`, `ping`) stay non-`async` and throw synchronously, returning a promise for the rest. Internal promises the caller may drop are pre-caught so nothing is ever an unhandled rejection; the returned promise still rejects. |
| Receiving | pull based. Channels (`[Symbol.asyncIterator]` over their subframes), incoming channels, messages, stream items and resolver events are `AsyncIterable`: `for await` takes delivery, and taking delivery is what returns flow-control credit. Each has one consumer. `break` stops delivery: on a channel it cancels the readable (what arrives is discarded) and leaves the channel open; on `incoming` it closes the channels not taken and every later one with UNAVAILABLE; on `TalkChannel.messages` it answers the requests not taken, and every later one, ABORT UNIMPLEMENTED; on a stream's items it cancels the request. Channel bytes are also a `ReadableStream<Uint8Array>` (`channel.readable`, made on first use) with `highWaterMark: 0` for `pipeTo` and `pipeThrough`. |
| Sending | `send()` returns a promise for hand-over (resolves once accepted into the window), plus `isWritable` and `whenWritable`. Channel bytes are also a `WritableStream<Uint8Array>` (`channel.writable`, made on first use) whose `write` is a `send`. |
| Events | typed `Emitter` with `on/once/off` returning an unsubscribe function, and `events(name, signal)` as an `AsyncIterable`. Used for state changes (`ReconnectingClient.on('state')`, `PeerSet.on('event')`, `NamingClient.on('event')`, `NamingService.on('event')`); never for data delivery. Iterable views sit beside them (`ReconnectingClient.states` replays the current state to each new iteration; `NamingClient.events`, `PeerSet.events`). One-shot changes are promises (`MuxConnection.peerIdentified`, `identityChanged`). |
| Cancellation | `AbortSignal` on every operation that waits: connect, open, `identify`, `ping`, `request`, resolve. Timeouts are options with defaults; a signal shortens them. `TalkMessage.signal` aborts when the request is cancelled or the channel closes. |
| Lifetimes | `Symbol.asyncDispose` on connections, channels, Talk channels, listeners, clients, persistent channels, peer sets, mesh nodes (`leave()`) and nodes, so `await using` closes them. `done` is a promise of the end `Status`. `close()` is idempotent and never throws. |
| Time | every timer comes from the `Clock` in the options (default `systemClock`); tests inject `ManualClock`. Monotonic time only. |
| Logging | an injectable `Logger` (default silent) with the names of section 7. Never `console`. |
| Crypto | WebCrypto: Ed25519, HMAC-SHA256, SHA-256 and `getRandomValues`. All verification is async; the credential codec stays synchronous. |
| Tree shaking | `sideEffects: false`; nothing runs at import. Process-wide state: the `mem://` endpoint registry (`MemoryEndpoints`), the lazily loaded `ws` module and native addon (with the one provider `nativeQuic()` returns), and weak maps keyed by the library's own objects. |
| Naming | camelCase members, the Dart names otherwise. Dart `Stream` getters become `AsyncIterable` getters of the same name (`incoming`, `messages`, `items`); Dart `Future` getters become promise getters (`done`, `connected`); Dart `sink` becomes `writable`. |

## 6. The layers

Each subsection names the contract file, the Dart source it ports, the TS surface and what differs. Signatures are summaries; the contract files and the doc comments are the definition. Every layer below is complete on `main`; section 12 lists what behaves differently from Dart.

### 6.1 Primitives: `bytes.ts`, `name.ts`, `status.ts`

- `ByteReader` and `ByteWriter` port `bytes.dart`: u8, u16, u24, u32, u48 little endian, 8-byte names, length-8 strings, `take`, `rest`; `ByteFormatError` on truncation. `hexBytes`, `hexString`, `bytesEqual`, `concatBytes`, `utf8Encode`, `utf8Decode`, `toPayload`.
- `Name`: 8 bytes, zero padded, no interior zero; `Name.of('echo')` (a `RangeError` beyond 8 bytes of UTF-8), `Name.fromBytes`, `.key` for maps, `.isReserved` for `_` types, `toString()`.
- `StatusCode` is the wiki table (gRPC 0 to 16, Switchboard 32 to 38). `Status` keeps the raw body after the code byte-exact, so a relayed CLOSE, GOAWAY or ABORT is forwarded unchanged (including MOVED and RELOCATED fields and malformed UTF-8); `reason` is decoded on demand. `Status.decode(bytes)` with an empty body is OK; one byte is a `StatusFormatError`. `SwitchboardError.of(code, reason)`, `ProtocolError`, `isStatus(error, code)`, `statusCodeName(code)`.

### 6.2 Transports: `transport/contract.ts`

Ports `transport/*.dart`. A transport carries whole frames, each a `Uint8Array`, in order, in both directions:

```ts
interface FrameTransport {
  readonly readable: ReadableStream<Uint8Array>;   // one chunk per frame; ends when the peer closed
  readonly writable: WritableStream<Uint8Array>;   // one chunk per frame; close() = orderly close
  readonly maxFrameSize?: number;                   // FrameLimited: announced in LIMITS when smaller than ours
  readonly remote?: string;                         // for logs: 'ip:port', a URL, 'memory'
  abort?(): void;                                   // tear down without the orderly close
  readonly bufferedOutputBytes?: number;            // OutputBuffered
  readonly acceptedOutputBytes?: number;            // OutputAccounting
  readonly isInputThrottled?: boolean;              // input paused while too much output waits
  readonly isOutputReady?: boolean; readonly outputReady?: Promise<void>;   // OutputReady
}
type TransportConnector = (options: { signal?: AbortSignal }) => Promise<FrameTransport>;
```

Capabilities are optional members (checked with `!== undefined`), replacing Dart's `is FrameLimited` style interface checks. Implementations:

| Transport | Module | Notes |
| --- | --- | --- |
| Stream framing | `core/transport/stream-framing.ts`, `stream-transport.ts` | `StreamFraming` and `StreamFrameDecoder` (the Dart names) are the codec of the 8-byte preamble (`SWBD`, version 1, flags 0) and the u32 LE length-prefixed frames; `StreamFrameEncoderStream` and `StreamFrameDecoderStream` are the same as transforms for `pipeThrough`. Default limit 1 MiB; a frame over the limit errors the readable with `SwitchboardError` FRAME_TOO_LARGE (as in Dart; `ProtocolError` is reserved for PROTOCOL_ERROR), a wrong magic or a zero length with `ProtocolError`, an unknown preamble version with UNSUPPORTED; the output stays open one clock turn after the reader has the error so a GOAWAY still goes out. `streamTransport(byteDuplex, options)` wraps a `{ readable, writable }` byte pair (a Node socket as Web Streams, a QUIC stream, a Deno conn) over the `ByteInput`/`ByteOutput` pair of `byte-io.ts`; output is coalesced up to 64 KiB and throttles input above a 16 MiB high-water mark. Once its output has ended on its own (the peer's FIN, a failure, an abort) the writable errors, so `writer.closed` settles and later writes reject (section 12, item 11). |
| Memory pair | `core/transport/memory-transport.ts`, `memory-endpoints.ts` | `memoryTransportPair({ delay, maxFrameSize, remotes, clock, logger })` for tests and in-process connections; `delay` 0 goes through a turn of the clock, as Dart's does, `undefined` hands over at once (read in a later microtask). `MemoryEndpoints` (its own module, the `mem://` registry) is what `listenMemory` registers with. `parseEndpoint` in `endpoint.ts` classifies endpoint URIs and gives the pool key. |
| WebSocket client | `core/transport/websocket-transport.ts` | `connectWebSocket(url, { protocols, timeout, signal, maxFrameSize, webSocket, clock, logger })` over the global `WebSocket` (browsers, Node 22, Bun, Deno): `binaryType = 'arraybuffer'`, subprotocol `switchboard`, receive limit enforced on assembled messages, `bufferedAmount` polled for `bufferedOutputBytes`; `WebSocketTransport`, `webSocketConnection` for an injected socket. |
| Node TCP | `node/transport/tcp.ts` | `net.Socket` with `setNoDelay`, stream framing, `writableLength` and `bytesWritten` accounting, pause and resume for throttling, the Dart close sequence (end, linger, destroy). `connectTcp(host, port, options)`, `listenTcp(host, port, options)` resolving with a `TcpListener` (accepted transports go to `onTransport`, or wait in `listener.transports`), `tcpTransport(socket)`. |
| Node WebSocket | `node/transport/ws-server.ts`, `ws-client.ts` | the `ws` package (optional peer dependency, loaded on first use) with `perMessageDeflate: false`, `maxPayload = maxFrameSize` (fragments counted before buffering), the subprotocol check, 403 for other paths, 426 for a plain request, 400 for a bad upgrade: `listenWebSocket(host, port, options)` resolving with a `WebSocketListener` (`onTransport` or `transports` likewise), `acceptWebSocket(request, socket, head, options)` for an application's own HTTP server. The Node entry's `connectWebSocket` uses the `ws` client when `ws` is installed (`connectWsWebSocket`) and the global `WebSocket` otherwise. |
| Any of the above by URI | `node/transport/connect.ts` | `connectTransport(endpoint, options)` for `tcp`, `ws`, `wss` and `mem`; another scheme (`quic`, `https`, which are the QUIC binding's) is refused with UNSUPPORTED. |

Endpoint schemes: `tcp://host:port`, `ws://` and `wss://host:port/path`, `mem://name`, `quic://host:port`, `https://host:port/path` (WebTransport). `parseEndpoint(uri)` in `core/transport/endpoint.ts` classifies them. A node's `connect` refuses a scheme it has no dialer or binding for with UNIMPLEMENTED (as in Dart), which makes it move on to a record's next endpoint; the browser node refuses `tcp` with UNSUPPORTED; `connectTransport` refuses the QUIC schemes with UNSUPPORTED.

### 6.3 Mux and the channel contract: `mux/contract.ts`

Ports `mux/*.dart`, the frame mux and the abstract connection seam. `MuxConnection` and `MuxChannel` are what everything above programs against; two bindings implement them, the frame mux over a `FrameTransport` and the QUIC binding over a `QuicSession`.

```ts
const connection = new FrameMuxConnection(transport, { isInitiator: true, ...muxOptions });
const channel = connection.open(openPayload);                // usable at once; throws for misuse
for await (const incoming of connection.incoming) { … }     // peer-opened channels, one consumer
await connection.identify(credential, { holderKey, intent, receiver, timeout, signal });
const rtt = await connection.ping(undefined, { signal });
await connection.goAway(status); await connection.close();
await using connection;                                      // asyncDispose = close()
```

`MuxConnection` also has `adoptIncoming`, `admitAfterGoAway`, `channelWithId`, `channels`, `openChannelCount`, `peerMaxChannels`, `hasOrderedChannels`, `done`, `isOpen`, `peerGoingAway`, `peerGoAwayStatus`, `localIdentity`, `peerIdentity`, `peerIdentified`, `identityRequested`, `identityChanged`, `bufferedBytes` and the diagnostics.

`MuxChannel`: `connection`, `id`, `isLocallyOpened`, `openPayload`, `state`, `canSend`, `priority` (settable, `'normal'` or `'bulk'`), `readable` and `[Symbol.asyncIterator]` for subframes, `writable` and `send(subframe)` (promise for hand-over, throws synchronously for misuse), `sendCost(length)`, `maxSubframeLength`, `sendWindow`, `isWritable`, `whenWritable`, `receiveWindow`, `bufferedBytes`, `manualCredit`, `heldBytes`, `consumed(bytes, subframes?)`, `grant(bytes)`, `openAfter(openPayload)`, `close(status?)`, `done`, `hasListener`, `awaitingLimits`, `limitsKnown`.

What is kept from Dart, exactly: option validation and defaults (`MuxOptions`, `defaultMuxOptions`, `muxOptions(partial)`), the LIMITS exchange and `awaitPeerLimits`, window bookkeeping, credit returned at delivery, the output scheduler with the bulk zipper, keep-alive, GOAWAY and the close sequence, the pending-rejection table, the receive high-water mark, held OPEN payloads and their budget, the identity state machine with the transport paused during verification, the connection versus channel error scopes. Every Dart mux suite is ported with `ManualClock` (`test/unit/mux/`, section 9).

What changes:

- Delivery is pull based (section 5). A channel not being read holds its credit. `break` out of `for await` over a channel cancels its readable, which discards what arrives from then on, and does not close the channel. `incoming` has one consumer; stopping its iteration closes the channels not taken, and the ones arriving after, with UNAVAILABLE.
- A channel's `readable` and `writable` are created on first use, and a connection keeps at most 1024 writes in flight to its transport's writable, queueing the rest itself in order (Node's `WritableStream` takes its queue in quadratic time beyond some 16K queued writes).
- Two hooks replace Dart's address decoding inside the mux: `connection.adoptIncoming(filter)` lets a layer claim peer-opened channels before they reach `incoming` (Talk claims `_bulk` channels for their parent), and `connection.admitAfterGoAway(predicate)` decides which OPENs are still accepted after GOAWAY (Talk admits `_bulk` continuations of a parent that is open or retained for its drain).
- `BindingConnection` is the abstract base for the QUIC binding: neutral diagnostics, `hasOrderedChannels = false`, drain holds (`retainForDrain`, `hasDrainHolds`, `retainedForDrain`, `drainStateChanged`). As in Dart, it carries no identity exchange and neither hook: the QUIC binding has its own (exporter binding, held control messages) and the frame mux its own (held frames). Decided 2026-10-14 with PR #9.
- The frame and control codecs are exported for the vectors and for the QUIC binding, which reuses NONCE, IDENT, GOAWAY and PING: `MuxFrame` (`data`, `open`, `close`, `decode`), `MuxControlMessage` (`ping`, `pong`, `goAway`, `limits`, `nonce`, `ident`, `credit`, `decode`), `MuxLimits`, `MuxCredit`, `MuxIdent` with `verifyIdent`, and the status rules (`checkMuxStatus`, `truncateStatus`, `statusForWire`, `statusFromWire`, `decodeStatusPayload`). HELLO is the QUIC binding's `QuicHello`.

### 6.4 Identity: `identity/contract.ts`

Ports `identity/*.dart`. The contract has the shapes every layer programs against (`Credential`, `Scope`, `HolderKey`, `CredentialVerifier`, `PeerIdentity`, and the `CredentialKind`, `Right` and `SignatureKind` constants); the implementations are exported under the same names where they are classes:

- `Credential` (kind, key id, identity, issued and expiry in Unix seconds, holder key or none for a bearer, scopes, signature kind, signature): `encode()`, `Credential.decode(bytes)` (synchronous, byte-exact, re-encodes to its exact bytes, `ProtocolError` for malformed input; scopes with unknown rights are dropped), `allows(right, name)`, `isExpired(now?)` with `now` in milliseconds since the epoch, `signedBytes`. At most `Credential.maxLength` (637) bytes.
- `Scope` (`Scope.of(Right.register, 'worker-*')`): a trailing `*` matches any suffix, the empty one included; `*` alone matches every name; a `*` elsewhere is an ordinary byte; `admin` allows everything.
- `CredentialIssuer.hmac(keyId, key)` (bearer only, key at least 16 bytes), `CredentialIssuer.ed25519(keyId, keyPair)` (a WebCrypto `CryptoKeyPair`) and `await CredentialIssuer.ed25519FromSeed(keyId, seed)`; `await issuer.issue({ kind, identity, scopes, holderKey, issuedAt, lifetime })` (`lifetime` in ms, default 8 h, `null` never expires; a holder key with an HMAC issuer rejects with `RangeError`); `await issuer.publicKey()` for Ed25519.
- `Ed25519HolderKey` implements the contract's `HolderKey` (a public key and an async `sign`), so a key kept elsewhere can stand in: `await Ed25519HolderKey.generate()`, `fromSeed(seed)`, `fromKeyPair(pair)` (non-extractable private keys work).
- `CredentialVerifier({ hmacKeys, ed25519Keys, cacheSize, wallTime, logger })`: `addHmacKey`, `addEd25519Key`, `await addIssuer(issuer)`, `removeKey(keyId)` (rotation: keep the old and the new key until the old credentials expired), `await verify(bytes, { now })` (rejects `UNAUTHENTICATED` 'invalid credential' whatever the cause, which goes to the logger at `debug`), `cached(bytes, { now })` (synchronous), an LRU cache by credential bytes that shares concurrent checks.
- The IDENT proof over `SWBIDENT` ‖ nonces ‖ intent ‖ receiver, and its exporter-bound `SWBIDTLS` variant (`MuxIdent.proofMessage`, `verifyIdent` with wall time in ms), `createPeerIdentity`, `secureRandomBytes`.

On a connection: `await connection.identify(credential, { holderKey, receiver, intent })` (a holder key given with a bearer credential is ignored, as in Dart; a holder-key credential without its key, or with another key, is refused before anything is sent); the receiving side verifies with `identityVerifier` in its mux options, and `requireNamedIdent` refuses an IDENT that names no receiver. All signing and verification is `async` over `crypto.subtle`; Ed25519 private keys import as PKCS#8. Times are wall time in milliseconds since the epoch (the injected `Clock` is monotonic).

### 6.5 Talk: `talk/contract.ts`

Ports `talk/*.dart`. `TalkChannel` wraps a `MuxChannel` (or a `MuxChannelCarrier` such as a `SlotChannel`, or any channel of whole Talk frames):

```ts
const talk = new TalkChannel(channel, options);        // or incoming.talk(), node.openTalk()
for await (const message of talk.messages) { … }       // or talk.serve(handler)
const reply = await talk.request('PROC', payload, { timeout, signal, onExtend, ordered });
const request = talk.startRequest('PROC', payload);    // { requestId, response, deadline, cancel() }
const stream = talk.streamRequest('PROC', payload, { window: 8 });   // AsyncIterable of items, `.done`
await talk.send('EVENT', payload);                      // fire and forget
talk.abort(status); await talk.close();
```

`TalkMessage`: `channel`, `frame`, `procedure`, `procedureName`, `payload`, `text()`, `json()`, `bytes()`, `isBulk`, `bulkLength`, `bulk` (a `ReadableStream`, for payloads `streamBulk` selects), `requestId`, `responseId`, `expectsReply`, `expectsStream`, `isCancelled`, `onCancel`, `signal`, `canReply`, `window`, `whenCredit`, `setCreditTimeout`, and the reply family: `reply`, `replyRequest`, `startReplyRequest`, `replyStreamRequest`, `replyItem`, `replyItemRequest`, `startReplyItemRequest`, `replyItemStreamRequest`, `replyStream`, `replyAbort`, `extend`, `setReplyTimeout`. `TalkStream` has `items` (and is itself the async iterable of them), `done`, `window`, `grant(items)`, `deadline`, `cancel()`. `forwardMessage(message, target)` forwards a message chain to another Talk channel with flow control passed through hop by hop. `TalkAbortError` is the error of an ABORT.

Kept exactly: the timeout regime (request, reply, EXTEND clamping and buffer), item windows and credits with the credit bound, bulk transfers (threshold, chunking, `_bulk` channel routing, reassembly budget, unclaimed bulk limits, idle timeouts), abort semantics, `forwardMessage`. `ordered` requests depend on per-microtask delivery; the TS implementation documents its guarantee in terms of `queueMicrotask` ordering and tests it.

Changed: payloads are `PayloadLike`; a bulk source is a `ReadableStream<Uint8Array>`, an `AsyncIterable<Uint8Array>` or an `Iterable<Uint8Array>` with an optional known length; `TalkStream` is itself the async iterable of items; `serve(handler)` is the guarded listener; `messages` and items are iterated once. Section 12 lists the rest.

### 6.6 Addressing: `address/`

`ServiceAddress` (`type/instance`, `ServiceAddress.parse('npc/1a2b')`, `new ServiceAddress('npc', 7)`) and `ChannelAddress` (type, instance, shard, host, payload) with `encode()` and `ChannelAddress.decode(openPayload)` throwing `ProtocolError` on reserved flag bits, truncation or an invalid name. `ChannelAddress.to(address, payload)` and `with({ ..., clearHost, clearShard })` for rewrites. `ServiceAddressLike = ServiceAddress | string | Name`.

### 6.7 The node: `switchboard/contract.ts`, `switchboard/`, `node/`, `web/`

Ports `switchboard.dart` and its helpers. The runtime-neutral core node (`core/switchboard/switchboard.ts`) holds the registrations, dispatch, policies, the connection pool, resolvers, identity on connections, brokering, slot routing, the proxy and the relay; it takes the frame mux, the in-process pairs, the `mem://` registry and the socket dialers from a `SwitchboardRuntime` the entry point supplies, and dials further schemes through `SwitchboardBinding`s (`registerBinding`). `@kaetemi/switchboard/node` exports a `Switchboard` that adds `listenTcp`, `listenWebSocket`, `acceptWebSocket`, `listenQuic` and `listenWebTransport` and dials `tcp`, `ws`, `wss`, `mem`, and `quic` and `https` through the `quic` option's provider (or, once it listens on QUIC without one, the listener's); `@kaetemi/switchboard/web` exports a `Switchboard` that dials `ws` and `wss` over the browser's `WebSocket` and `https` through its `quic` option (`webTransport()`), refuses `tcp` with UNSUPPORTED, and listens only in process (`listenMemory`).

```ts
class Switchboard {
  constructor(options?: SwitchboardOptions);       // resolver, defaultPayload, muxOptions, talkOptions, connectTimeout,
                                                   // allowHostHint, maxConnectionsPerEndpoint, slotRefreshTimeout, selection,
                                                   // random, outgoingPolicy, endpointPolicy, credentialFor, credential,
                                                   // holderKey, verifier, identifyOutgoing, identifyFor, identityTimeout,
                                                   // expectedIdentityFor, relay, brokerEndpoint, localConnections, bindings,
                                                   // runtime, clock, logger; the entries add quic and quicBinding
  registerService(type: NameLike, handler: ChannelHandler, options?: { instance?, acceptAnyInstance? }): void;
  unregisterService(type: NameLike, instance?: number): void;
  defaultService: ChannelHandler | undefined; catchAll: ChannelHandler | undefined;
  openChannel(address: ServiceAddressLike, options?: { shard?, payload?, where?, signal? }): Promise<MuxChannel>;
  openTalk(address, options?: { …, talkOptions? }): Promise<TalkChannel>;
  openChannelAt(endpoint: string, address: ChannelAddress, options?: { signal? }): Promise<MuxChannel>; openTalkAt(…);
  openChannelOn(connection: MuxConnection, address: ChannelAddress): MuxChannel;
  openChannelToSlot(type, slot, options?): Promise<SlotChannel>; openTalkToSlot(type, slot, options?): Promise<TalkChannel>;
  resolveSlotOwner(type, slot, options?: { refresh?, signal? }): Promise<ServiceAddress>;
  selectAndConnect(address, options?): Promise<SelectedInstance>; payloadFor(connection, options?): Promise<Uint8Array>;
  connect(endpoint: string, options?: { policy?, record?, signal? }): Promise<MuxConnection>;   // pooled
  dial(endpoint, options?: { policy?, intent?, receiver?, record?, signal? }): Promise<MuxConnection>;   // not pooled
  broker(address, options?: { resolver?, signal? }): Promise<MuxConnection>; readonly canBroker: boolean;
  resolveRelays(options?): Promise<ServiceRecord[]>;
  identifyOn(connection, options?: { intent?, receiver? }): Promise<void>; updateCredential(credential): Promise<void>;
  listenMemory(options?: { policy?, name? }): Promise<string>;
  accept(transport, options?: { policy?, remote? }): Promise<MuxConnection>; adopt(connection, options?): Promise<MuxConnection>;
  addListeningEndpoint(endpoint, options?): Promise<void>; removeListeningEndpoint(endpoint): boolean;
  registerBinding(binding: SwitchboardBinding): void;
  readonly connections: AsyncIterable<MuxConnection>; readonly listeningEndpoints: readonly string[];
  isOwnEndpoint(endpoint: string): boolean;
  close(): Promise<void>; [Symbol.asyncDispose]();
}
// @kaetemi/switchboard/node adds:
//   listenTcp(address, port, { policy? }): Promise<string>
//   listenWebSocket(address, port, { path?, policy?, onOtherRequest?, onOtherUpgrade? }): Promise<string>
//   acceptWebSocket(request, socket, head, { policy?, maxFrameSize? }): Promise<MuxConnection>
//   listenQuic(address, port, { certificate?, advertisedHost?, provider?, policy?, identityRules?, … }): Promise<string>
//   listenWebTransport(address, port, { path?, allowedOrigins?, …the listenQuic options }): Promise<string>
//   readonly quic: QuicConnector | undefined
```

`ChannelPolicies` (`allowAll`, `denyAll`, `denyReserved`, `allowTypes`, `scoped`, `requireIdentity`, `all`), `IncomingChannel`, `Resolver`, `SlotResolver`, `BrokeringResolver`, `StaticResolver` (a `SlotResolver` over configured slot tables: `defineSlots`, `setSlot`, `removeSlots`, `locateSlot`), `EndpointResolver`, `NamingResolver`, `pipeChannels`, `proxyHandler`, `RelayConfig`, `RelayService`, `PeerSet` and `MeshNode` keep their Dart semantics (the `dart-design.md` behaviour notes are the contract). Listener options are an object, bound endpoints are returned as strings. `listenQuic` and `listenWebTransport` default to a self-signed certificate for the advertised host and fill in the `switchboard` ALPN and protocol themselves. `ServiceAddressLike = ServiceAddress | string | Name` so `openTalk('echo')` and `openTalk('npc/1a2b')` both work. The node logs as `Switchboard.Node`, its proxy and forwarding as `Switchboard.Router`, the relay as `Switchboard.Relay`.

`MeshNode` (`core/switchboard/mesh-node.ts`, Dart `mesh.dart`) wires a node into a mesh: `MeshNode.join(node, namingEndpoint, options)` creates a `NamingClient` through the node (`namingClientFor`), installs a `NamingResolver` as the node's resolver and starts the client; `publish(type, handler, { instance, endpoints })`, `publishRelay(relay, options)`, `unpublish`, `publishSharded(type, lifecycle, options)` (the gate, the registration, `SLOTS` and `HOLDING`; resolves with the type's `SlotGate`), `claimSlot`, `releaseSlot`, `gates` (a `SlotGates`, the client's slot handler), `migrateSlot`, `slotOwners`, `synced`, the `CONNECT` dial-back and the credential renewal at two thirds of its lifetime, and `leave()`, which hands the node's slots over, releases those it could not (each step bounded by `leaveTimeout`), and resolves with a `LeaveReport` (`handedOver`, `released`, `dropped`: maps keyed by `Name.key` of sorted, frozen slot arrays). It keeps Dart's behaviour with the TS shapes: options objects (`MeshNodeOptions`, `MeshPublishOptions`, `MeshPublishShardedOptions`) instead of named parameters, `published` and `brokeredConnections` as arrays, `nextRenewalAt` in ms since the epoch, `wallTime`, `clock` and `logger` options, and `Symbol.asyncDispose` (`leave()`).

### 6.8 Naming and sharding: `naming/`

Ports `naming/*.dart` and the slot machinery: the `_ns` protocol codecs (REGISTER, UNREGSTR, WATCH, UNWATCH, LOOKUP, RENEW, CONNECT, UP, DOWN, and the slot messages SLOTS, HOLDING, CLAIM, RELEASE, LOCATE, MIGRATE, ASSIGN, DRAIN, FORWARD, RESUME), `NamingClient` (register, unregister, watch and the mirrored table, `renew()`, `connectTo`, `defineSlots`, `declareHolding`, `claim`, `release`, `locate`, `migrate`, the slot handler), `NamingService` (leases tied to registration channels, watches, CONNECT brokering, identity scopes, expiry and RENEW, slot spaces, the allocator and migration), `NamingResolver` (a `SlotResolver` and `BrokeringResolver`), `SlotTable`, `SlotGate`, `SlotLifecycle` (the application side: `load(slot, SlotLoadOptions)`, `drain(slot, SlotDrainOptions)`, `unload`, `serve`, `discard`), `SlotGateState` and `SlotGates` (`naming/slot-gate.ts`, Dart `switchboard/slot_gate.dart`), `SlotChannel`, `reopenAtSlotOwner` (the MOVED retry shared by `openChannelToSlot` and the proxy), `fnv1a32`, `slotForKey`, `slotForText`. Events are `AsyncIterable` and `Emitter` based; everything else keeps the Dart shape.

### 6.9 Clients: `client/`

`ReconnectingClient(connector, options)` over a `TransportConnector` (the frame mux runs over the transport) or a `ConnectionConnector` object `{ connect(options, { signal }) }` (Dart's `ReconnectingClient.connections`), which is how QUIC and WebTransport plug in: `quicConnector(endpoint, provider, init)` in the core and `webTransportConnector(url, init)` in the web entry return one. The client has `states` (an `AsyncIterable` replaying the current state) and `on('state')`, `state`, `nextAttemptAt`, `connection`, `connected`, `isRunning`, `isClosed`, `incoming`, `start`, `stop`, `reconnectNow({ force })`, `close`, `openChannel`, `openTalk`, `openPersistent`, `openPersistentTalk` (returning `PersistentChannel` and `PersistentTalk`), and a settable `defaultPayload`. `Backoff` (internal) is shared with `PeerSet`; backoff randomness is injectable (`random: () => number`).

### 6.10 QUIC and WebTransport binding: `quic/contract.ts`, `quic/`

Ports `switchboard_quic/lib/src/binding/*`. The provider contract (`QuicConnector`, `QuicListener`, `QuicSession`, `QuicStream`) is the TS form of Dart's `QuicProvider`: sessions, bidirectional streams with `read(max)`, `write`, `finish`, `reset(code)`, `stop(code)`, `priority`, keying-material export, peer certificate hash, close with code and reason. `QuicBindingConnection extends BindingConnection` implements `MuxConnection` over a session: one stream per channel, the record codec (u32 LE header, length in bits 0 to 29, command in bits 30 and 31: DATA, OPEN with u48 id and payload, CLOSE with status then FIN), the control stream (the initiator's stream with OPEN channel 0), HELLO first (record limit, channels, flags with EXPORTER), ids never reused, resets and STOP_SENDING carrying status codes 0 to 255, GOAWAY and drain holds for bulk, exporter-bound identity with `EXPORTER-Channel-Binding` on raw QUIC and `switchboard-ident` on WebTransport, `QuicIdentityRules` for self-signed certificates. `connectQuic(endpoint, connector, options)` dials, `quicConnector(endpoint, connector, init)` gives a `ReconnectingClient` its connector, `listenQuic(providerListener, options)` accepts sessions as connections (`QuicBindingListener`); `QuicSwitchboardBinding` and `QuicSwitchboardListener` put them into a node.

The native provider (`node/quic/native-provider.ts` plus `native/`: `nativeQuic()`, `NativeQuicProvider`, `selfSignedCertificate(subject, validityDays?)` (synchronous, ECDSA P-256, 14 days by default, which a browser's `serverCertificateHashes` accepts), `certificateHash`, `nativeQuicAvailability()`) and the WebTransport provider (`web/quic/webtransport-provider.ts`: `webTransport(init?)`, `WebTransportProvider` with `isSupported` and `maxStreamErrorCode`, `WebTransportSession`, `WebTransportStream`, `webTransportConnector`) implement the contract; `testing/fake-quic-provider.ts` implements it in memory with independent stream delivery, configurable delays, flow control, a stream limit, resets and an exporter, so the binding's tests and the node's QUIC paths run without native code.

## 7. Cross-cutting behaviour

**Errors.** Protocol errors on a connection end it (GOAWAY PROTOCOL_ERROR, then close); protocol errors in a channel's OPEN payload or Talk header close that channel with PROTOCOL_ERROR. The library never throws across an `await` for a peer's misbehaviour; it reports through statuses, `done` promises and rejected request promises. It throws `RangeError`/`TypeError` synchronously for the caller's misuse. The IDENT failure reason never reaches the wire or an error `cause`.

**Time.** `Clock` (section 5). `systemClock` re-arms timers longer than 2^31-1 ms. Timers hold a Node process open; a consumer that wants an idle connection not to keep the process alive closes it. The native QUIC addon holds the event loop only while a session or listener is live.

**Logging.** `Logger.log(level, name, message, error?)` with the names `Switchboard.Transport`, `Switchboard.Mux`, `Switchboard.Identity`, `Switchboard.Talk`, `Switchboard.Node`, `Switchboard.Router` (proxy, forwarding, slot gates), `Switchboard.Relay`, `Switchboard.Naming`, `Switchboard.Quic`, `Switchboard.Client` (the reconnecting client and `PeerSet`). `consoleLogger` and `noopLogger` (the default) are provided.

**Unhandled rejections.** Every promise the library creates and hands out (`send`, `whenWritable`, `done`, `connected`, `identify`, request responses) is pre-caught; a consumer that drops it sees no warning, one that awaits it sees the rejection.

**Resource management.** `Symbol.asyncDispose` everywhere a Dart `close()` exists (`MeshNode`'s is `leave()`). `done` resolves, never rejects, with the end `Status`. A node's `close()` sends GOAWAY on every connection and waits up to `goAwayGrace` for their channels: close channels (or leave their `await using` blocks) before closing the node.

**Concurrency model.** Single threaded, like Dart. Delivery order within a channel is the arrival order; across channels it is the mux's scheduling, as in Dart. Where Dart relies on microtask ordering (Talk `ordered`), the TS code relies on `queueMicrotask` ordering and the tests assert it.

## 8. Build and tooling

| Tool | Choice | Why |
| --- | --- | --- |
| Language | TypeScript 7 (`tsc`), `module: NodeNext`, target ES2023, `.ts` extensions in imports with `rewriteRelativeImportExtensions`, `verbatimModuleSyntax`, `erasableSyntaxOnly`, strict plus `exactOptionalPropertyTypes` and `noUncheckedIndexedAccess` | native TS runs the sources directly in Node 22 (`--experimental-strip-types`), Bun and Deno; `tsc` emits `dist/` with declarations for publishing |
| Tests | Vitest 5: the projects `unit`, `integration` and `interop` of `vitest.config.ts`, and the browser tests under their own configuration, `test/browser/vitest.config.ts` (`npm run test:browser`), with Playwright 1.56.1 driving Chromium and Vite serving the page | fast, ESM native; the browser tests need the native addon for their listeners and stay out of `npm test` |
| Lint and format | Biome 2 (`biome check`), single quotes, 100 columns, `noExplicitAny` error, `useImportType` error, `useLiteralKeys` off (the TS config requires bracket access to index-signature keys); Markdown is not linted | one tool, fast, no plugin graph |
| Package | `@kaetemi/switchboard` 3.0.0-dev.1 (the Dart package is 3.x; the ports share the protocol version line), ESM only, `exports` for `.`, `./node`, `./web`, `./testing`, `./package.json`; `ws` optional peer dependency; the five prebuilt QUIC addons as optional dependencies | consumers import one package; `ws` only where a server (or a bounded client) is needed |
| Native | Rust 1.99.0 in CI (the crate needs 1.88+), `napi-rs` 3, `cargo`; `npm run build:native` (`native/build.mjs`); prebuilt per target by `.github/workflows/release.yml` (manual dispatch; builds, tests and packs, publishes only when asked) | section 3.1 |
| Scripts | `npm run check` (lint, typecheck of `src`, unit and integration tests), `npx tsc -p tsconfig.test.json --noEmit` (type-checks `test/**` too; not in CI), `npm run test:interop`, `npm run test:browser`, `npm run build`, `npm run build:native` | what a contributor runs before pushing |
| CI | `.github/workflows/ci.yml`: lint and typecheck; unit and integration on Node 22, 24 and 26; the unit project on Bun; build (and an import of `dist/`); `interop` with Dart 3.13.5, Rust 1.99.0 and the pinned Dart checkouts in required mode; `native` builds the addon (after `sync-sbq --check`) and runs the native QUIC integration files; `browser` builds the addon, installs Playwright's Chromium and runs `npm run test:browser` in required mode | section 9 |

Node 22 is the floor because of global `WebSocket`, stable `crypto.subtle` Ed25519, `Promise.withResolvers`, `Symbol.asyncDispose` and `ReadableStream.from`.

## 9. Test strategy

Five layers, lowest first. Every layer runs in CI; the interop layer needs Dart and the browser layer the native addon and Chromium, and each skips locally with one visible line naming what is missing (`SWITCHBOARD_INTEROP=required`, `SWITCHBOARD_NATIVE=required` and `SWITCHBOARD_BROWSER=required` turn the skip into a failure, as in CI).

1. **Unit and vectors** (`test/unit`, `test/vectors`, `src/**/*.test.ts`): every codec against the wiki vectors, named by id from `test/vectors/wiki-vectors.json` (extracted from the sway wiki by `tool/extract-wiki-vectors.mjs` with the sway commit recorded inside; sway is private, so CI reads the committed JSON and `test/vectors/coverage.test.ts` checks that every vector id is asserted or skipped by some test), plus the Dart-only vectors with the codec they belong to. Behavioural tests port the Dart unit tests one to one with `ManualClock` in place of `fake_async`, named after the Dart files; the node, naming and client suites also run over the real frame mux (`over-frame-mux/`) besides their in-memory fixtures. The Dart test files are the checklist (`docs/notes/interop-dart-suites.md`).
2. **Integration** (`test/integration`): nodes in one process over the memory transport, TCP, WebSocket and QUIC on the loopback (the fake provider and the native provider): Talk over the frame mux, TCP and the QUIC binding, dispatch, policies, identity with real credentials, naming, the mesh suites of Dart's `test/integration`, proxying, relay, the reconnecting client, bulk transfers, GOAWAY and drain. The native files skip without the addon.
3. **Interop** (`test/interop`): the Dart reference as a peer. One JSON-lines peer protocol spoken by a Dart peer (an AOT executable built from `test/interop/dart_peer/`, depending on the two Dart repositories by path at the commits in `dart_commits.txt`) and a TS peer (`test/interop/ts_peer/`, using only the public API). Each scenario runs across four pairings, Dart to Dart (checks the scenario against the reference), TS to Dart, Dart to TS, TS to TS, over TCP, WebSocket, raw QUIC and WebTransport as the pairing allows; a cell that cannot run is skipped with a reason. A codec oracle in the Dart peer decodes protocol messages, so the vector suite also runs differentially, and a fuzz pass compares random inputs. Raw byte conformance probes check framing against a server without the library.
4. **Browser** (`test/browser`, its own vitest configuration): the web provider and the QUIC binding in Playwright's headless Chromium, against native WebTransport listeners in the test process, the page served by Vite (ports of `web_provider_test.dart` and `web_binding_test.dart`).
5. **Runtimes by hand**: the built package on Bun and Deno (TCP, WebSocket and QUIC round trips) until Deno joins CI.

A change to the pinned Dart commits is a deliberate commit; a protocol change upstream shows up as one failing interop run.

## 10. Work split and integration

The original assignments are in `docs/work-split.md` (six threads, each owning directories and their tests, programming against the contracts in this document), with what is done marked there. The port's first phase went through pull requests into the integration branch `claude/switchboard-ts-design-s5r9na`; at the first hand-off `main` was initialised from it, and `main` is the integration branch since. The continuation works on topic branches `claude/<topic>`, one git worktree each under `/home/user/wt/<topic>`, which the lead merges into `main` with merge commits (never squash or rebase) after `npm run check` and the test typecheck pass on the merge; the lead keeps this document, the work split, the hand-off and the wiki pages in step (`docs/handoff.md` has the procedure and the state).

## 11. Decisions taken

| Question | Decision |
| --- | --- |
| Package name | `@kaetemi/switchboard` (the user's npm scope, decided 2026-10-14; the bare `switchboard` is taken by an unrelated package and `polyverse` is not the user's npm account), version line 3.x like the Dart package. Prebuilt QUIC addons as `@kaetemi/switchboard-quic-<target>`. |
| Module format | ESM only, Node 22 floor |
| QUIC on Node, Bun, Deno | one N-API addon mirroring the `sbq` C ABI over a vendored copy of the shim; five prebuilt optional dependencies (Linux x64 and arm64 glibc, macOS arm64 and x64, Windows x64); lazy load; a missing binary is `QuicError` `'unsupportedScheme'` (UNIMPLEMENTED through the node), never a crash |
| Provider contract | the Dart shape typed for TS: pull-based `read(max)`, `write`, `finish`, `reset`, `stop`, streams taken only while asked for; WHATWG streams are adapted at the provider, not required of it |
| QUIC in browsers | WebTransport provider; raw QUIC impossible there. `webTransport(init?)` returns the provider (a `QuicConnector`) for `connectQuic`, `quicConnector` and the browser node's `quic` option; `webTransportConnector(url, init)` returns a `ReconnectingClient`'s `ConnectionConnector` (the web entry-point helper the first hand-off left open) |
| `node:quic` | not now; possible later behind the same contract |
| Bytes API | `Uint8Array`, `PayloadLike` for inputs, `text()/json()/bytes()` on outputs |
| Delivery | pull based async iterables plus Web Streams; credit on delivery; channels are async iterable over their subframes |
| Events | typed `Emitter`; no `EventEmitter`, no `EventTarget` |
| Time and logging | injectable `Clock` and `Logger` in every options object |
| `_bulk` routing | the Talk layer installs it through `adoptIncoming` and `admitAfterGoAway` hooks; raw consumers never see `_bulk` |
| Binding base | `BindingConnection` keeps Dart's scope (drain holds, neutral diagnostics); the identity exchange and the two hooks are each binding's own |
| Placement | `MeshNode` and `PeerSet` in `core/switchboard/`; `SlotGate`, `SlotLifecycle` and `SlotGates` in `core/naming/slot-gate.ts`; `isSlotResolver` beside `isBrokeringResolver` in `core/switchboard/resolver.ts`; `StaticResolver` is a `SlotResolver` |
| Reconnecting client | takes a `TransportConnector` function or a `ConnectionConnector` object `{ connect(options, { signal }) }` |
| Wall time | `verifyIdent`, `Credential.isExpired`, `CredentialVerifier.verify` and `cached` take wall time in ms since the epoch; the injected `Clock` is monotonic |
| Test runner | Vitest: projects `unit`, `integration`, `interop`; the browser tests under their own config (`npm run test:browser`) |
| Interop layout | `test/interop/{dart_peer,ts_peer,…}`, `test/vectors/wiki-vectors.json`; the harness thread's proposal as written |
| Branches | `main` is the integration branch (since the first hand-off); topic branches `claude/<topic>` in worktrees, merged by the lead with merge commits |

## 12. Deviations from the Dart API

Behavioural differences a Dart user would notice; the wire is unchanged.

1. Pull delivery: a channel or Talk channel nobody iterates holds its credit (Dart buffered until listened, then pushed).
2. `sink.add` fire-and-forget is gone; `writable` applies backpressure and `send()` resolves on hand-over.
3. Raw `incoming` consumers never see `_bulk` channels and do not call `adoptBulk`; Talk installs the filter.
4. All identity verification is asynchronous; the transport is paused meanwhile exactly as the Dart IDENT path already does.
5. Timers do not use `unref`; close what should not keep the process alive.
6. Durations are milliseconds; `undefined` where Dart used `null`.
7. Every waiting call takes an `AbortSignal`; Dart used `Future.timeout` at call sites.
8. A write to a channel that can no longer send rejects and errors the `writable` (frame mux and QUIC binding alike); Dart's `sink.add` dropped the data silently.
9. An OPEN after this side's GOAWAY is refused with GOING_AWAY unless an `admitAfterGoAway` predicate accepts it; with none installed nothing is admitted, not even `_bulk` continuations (Talk installs the predicate).
10. A channel's `readable` has a high-water mark of 0: a subframe is delivered, and its credit returned, only on a read; cancelling the readable discards what arrives and does not close the channel.
11. A transport's writable errors once its output has ended on its own (the peer's FIN, a failure, an abort), so `writer.closed` settles and later writes reject; Dart dropped them silently.
12. `MuxChannel` is async iterable (`[Symbol.asyncIterator]`) over its subframes; `break` out of the loop cancels the readable as item 10 says and leaves the channel open.
13. `IncomingChannel.talk()` returns the `TalkChannel` class (not only the contract's interface), so `forwardMessage` and the class's members apply to it.
14. Talk, as `docs/handoff-notes/talk.md` details: `serve(handler)` is the guarded listener (a handler that throws or rejects answers its request ABORT INTERNAL and marks it cancelled); a throw in a `for await` body does not answer the request it was given (it waits for the responder timeout, as with Dart's `await for`); `messages` and a stream's items are iterated once; `forwardMessage` takes over a message's frame credit only when called synchronously inside a `serve` handler (a message taken by `for await` had its credit returned before the loop body ran); `onExtend` gets one `{ deadline?, renew? }` object; bulk receive allows for Talk reading behind the mux (bulk admission counts the frames on their way to Talk); over unordered connections (QUIC) Talk reports the frames it holds consumed ahead of their delivery while bulk channels wait for their references, up to one window, and counts the frames it may still read on before refusing a bulk channel (section 13, item 6). Dart's receiver refuses such a burst past 16.
15. Errors: Dart's `ArgumentError` is `RangeError` or `TypeError`, its `StateError` a plain `Error`; FRAME_TOO_LARGE is a `SwitchboardError`, only PROTOCOL_ERROR a `ProtocolError`.
16. `PeerSet` events fire synchronously (Dart's broadcast is asynchronous), with guards against re-entrancy; a signal passed to a pooled `connect` abandons only that caller's wait, and a late connection of an abandoned `dial` is sent GOAWAY.

Not a deviation, but easy to misread: `identify` ignores a holder key given with a bearer credential, in the frame mux and the QUIC binding alike, as Dart does.

## 13. Open questions

1. ~~`rlib` in `switchboard_quic/native/sbq/Cargo.toml`.~~ Decided: no upstream change now; the addon carries a separable copy of the shim, and a dedicated shim repository shared by both packages is the intended direction once the port is ready (section 3.1).
2. ~~**`main`.**~~ Closed: `main` was initialised from the integration branch at the first hand-off and is the integration branch since; topic branches `claude/<topic>` merge into it with merge commits.
3. **Prebuilt platforms.** The five targets of `switchboard_quic`'s release workflow have manifests and a release draft; musl Linux has none yet (the loader looks for `-linux-<arch>-musl`; build locally). A platform without a prebuild has no QUIC: the connector reports `'unsupportedScheme'` and the node moves on to the record's next endpoint. Publishing is the user's call (`release.yml` publishes only when asked).
4. **Deno in CI.** The built package works on Deno 2.9 by hand; added to CI once the suite is known to pass there.
5. **GitHub wiki.** The wiki repository could not be written from this environment; the pages live under `docs/wiki/` in wiki-ready Markdown until they are pushed.
6. ~~**QUIC bulk bursts.**~~ Closed: Talk reads on for itself over unordered connections; the wire, the binding and the contracts are unchanged.
   * **The gap.** The QUIC binding reads a channel's next record only once the previous one was consumed (as Dart's does), and Talk holds a message's frame until its payload is there. The bulk streams of a burst arrive side by side, ahead of the references the parent stream carries a frame at a time, and behind any message Talk holds for its payload; past `maxUnclaimedBulk` (16) waiting, Talk refused the rest RESOURCE_EXHAUSTED and those messages failed at the sender. The frame mux reads the references as they arrive. Measured before the change: over the native provider, raw QUIC and WebTransport alike and without any lag, 11 to 12 of 30 messages of 50 KB were refused, 14 of 30 behind a 4 MiB message, 67 to 80 of 100; over the fake provider with the first bulk stream lagging, 13 of 30 and 83 of 100.
   * **The binding reads ahead for Talk: not taken.** It changes the binding's contract: with `manualCredit` the next record is read only once the previous one was consumed, or within what `grant` allowed. That is the binding page's flow control (unread data stays in the QUIC stack, so the stack's stream window is the channel's window) and a ported Dart test (`binding.test.ts`, "with manualCredit the next record is read once the previous one is consumed; grant reads further ahead"). It would also hold more for every manual-credit reader (Talk, a proxy's pipe), not only while bulk channels wait.
   * **Talk grants its parent a window ahead: not taken as stated.** A fixed read-ahead (`grant(initialWindow)` on unordered connections) keeps the contracts but turns every burst over 16 into a channel PROTOCOL_ERROR ("too many BULK references waiting for their channels"), with or without a lag: over independent streams the references usually overtake their bulk streams (`openAfter` starts the child's stream and the reference is written right after it), and reading them all ahead leaves more than 16 references waiting for their channels, which the Dart design makes a protocol error. Two existing cases of `talk-quic.test.ts` failed with it too, among them the one pinning that a message Talk holds for its payload holds the CLOSE behind it.
   * **Taken: Talk reads on, on demand.** While bulk channels of the peer wait unclaimed, and fewer than `maxUnclaimedBulk` references wait for their channels, Talk reports the frames it holds consumed ahead of their delivery (`consumed`, no `grant`), up to one `initialWindow` of them, so the binding reads on to the references; a delivery then owes nothing for what was already reported (`#readOn` in `talk-channel.ts`). And it refuses a bulk channel beyond 16 only once the frames that could carry its reference were read, the frames it may still read on included: the bulk streams of a burst are adopted side by side while the parent is read a frame at a time, so their references are usually there, unread (the allowance of section 12, item 14, extended). Without that allowance about 9 of 30 were still refused over the native provider. Ordered connections (the frame mux) do not read on.
   * **Bounds.** Talk holds at most one window of frames read on plus the record the binding read next, besides the stack's stream window, as one window bounds them over the frame mux. A peer that opens bulk streams without sending their references is refused once the parent's unread frames can no longer carry them, else when `bulkOpenTimeout` (10 s) ends them `CANCELLED`; the stack's stream limit bounds them meanwhile, where the frame mux refuses beyond 16 at once. More than 16 references waiting while bulk channels wait unclaimed stops the reading on, and the refusal past 16 applies as before.
   * **Dart.** The reference keeps the gap; it is reported, not patched. An interop scenario with such a burst asserts per implementation: the TS receiver takes it, the Dart receiver refuses the excess.
   * **Tests.** `talk-quic.test.ts` over the fake provider: the burst behind a lagging bulk stream (no longer `test.fails`), 100 behind three lagging streams, and 100 without a lag, which a fixed read-ahead fails. `quic-node-native.test.ts` over the native provider: bursts of 30, 30 behind 4 MiB, and 100 between nodes, raw QUIC and WebTransport.
7. **Firefox and newer Chromium.** The exporter path (Firefox 155+) and the `protocols` negotiation (Chromium 143+, which needs a Playwright newer than 1.56.1) are untested in CI.
