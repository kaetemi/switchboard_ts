# Polyverse Switchboard for TypeScript: design

_Status: v1, 2026-10-14. The lead thread owns this document; it is refined as the port proceeds. Decisions are stated as decisions; open questions are collected at the end._

`@polyverse/switchboard` is the TypeScript port of the Switchboard protocol: multiplexed channels, Talk message chains, service addressing, identity, the naming service and sharding, over WebSocket, TCP, QUIC and WebTransport. It targets modern JavaScript and TypeScript consumers with ES modules, and makes the protocol feel native to the platform: `for await` over channels and messages, Web Streams for bytes, `AbortSignal` for cancellation, promises everywhere a Dart future was, and `await using` for lifetimes.

QUIC is an integral part of the package, not an experimental add-on: the binding is in the core, a WebTransport provider ships for browsers, and a native QUIC provider ships for Node, Bun and Deno.

## 1. Sources of truth and precedence

1. **The wiki** in `kaetemi/sway` under `wiki/switchboard/` is the normative protocol specification: `home`, `transport`, `mux`, `status-codes`, `talk`, `addressing`, `identity`, `naming`, `sharding`, `proxying`, `relay`, `quic-binding`, `embedded-profile`, `test-vectors`. When the wiki and a Dart implementation disagree, the wiki wins unless `test-vectors.md` is amended.
2. **The Dart reference implementation**, `kaetemi/switchboard` and `kaetemi/switchboard_quic`, at the commits pinned in `test/interop/dart_peer/dart_commits.txt`. `doc/dart-design.md` in `switchboard` records every layer's API contract and the behaviours the Dart tests assert; the TS port keeps the same behaviours and the same names where the platform allows.
3. **The wire is byte-identical.** Nothing in this port changes the protocol. Every change in observable behaviour relative to Dart is listed in section 12.
4. The porting notes the threads wrote are inputs, not sources: `/mnt/project-files/switchboard-ts/notes/core.md`, `interop.md`, `interop-dart-suites.md`.

The Dart repositories are read-only for this project.

## 2. What it looks like

A service and a client on one Node process:

```ts
import { EndpointResolver, Switchboard } from '@polyverse/switchboard/node';

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
await using talk = await client.openTalk('echo');

const reply = await talk.request('UPPER', 'hello');
console.log(reply.text()); // HELLO

const stream = talk.streamRequest('COUNT', '3');
for await (const item of stream) console.log(item.text()); // 1 2 3
console.log((await stream.done).text()); // done

await client.close();
await server.close();
```

Raw channels, bytes in and out, with backpressure:

```ts
const channel = await client.openChannel('blob', { shard: 7 });
await channel.send(new Uint8Array(1024));
for await (const chunk of channel) handle(chunk); // credit returns as chunks are taken
await channel.close();
```

A browser client over WebTransport, reconnecting, with a persistent channel:

```ts
import { ReconnectingClient, webTransport } from '@polyverse/switchboard/web';

const client = new ReconnectingClient(webTransport('https://edge.example.com:4433/sb', {
  serverCertificateHashes: [{ algorithm: 'sha-256', value: hash }],
}));
client.openPersistentTalk('chat', async (talk) => {
  for await (const message of talk.messages) render(message.json());
});
```

Identity, scoped policy, QUIC on Node:

```ts
import { ChannelPolicies, Switchboard, nativeQuic, selfSignedCertificate } from '@polyverse/switchboard/node';

const node = new Switchboard({
  credential: await issuer.issueNode('edge-1', scopes),
  holderKey,
  verifier,
  quic: nativeQuic(),
});
const quic = await node.listenQuic('0.0.0.0', 4433, {
  certificate: await selfSignedCertificate('edge.example.com'),
  policy: ChannelPolicies.scoped(),
});
```

## 3. Runtime targets

| Runtime | Entry | Transports | QUIC and WebTransport | Status |
| --- | --- | --- | --- | --- |
| Node.js 22, 24 and 26 | `@polyverse/switchboard/node` | TCP (`node:net`), WebSocket client (global `WebSocket`, or `ws`), WebSocket server (`ws`), in-memory | native provider (N-API addon over quinn) | primary target, CI |
| Bun 1.x | `@polyverse/switchboard/node` | same, through Bun's Node compatibility | native provider (Bun loads N-API addons) | CI, best effort |
| Deno 2.x | `@polyverse/switchboard/node` | same, through `node:` compatibility | native provider (Deno loads N-API addons with `--allow-ffi`) | tested manually until CI has Deno |
| Browsers (evergreen) | `@polyverse/switchboard/web` | WebSocket | WebTransport API provider (`protocols` from Chrome 143, Firefox 155, Safari 26.4; the exporter on Firefox 155+) | Chrome for Testing 143+ in CI via Playwright; Firefox and Safari manual |
| Workers and edge runtimes | `@polyverse/switchboard` (core) with a caller-supplied transport | WebSocket where offered | none | not a target; nothing in the core prevents it |

The **core entry** `@polyverse/switchboard` has no runtime dependency beyond the Web platform APIs that all of these share: `Uint8Array`, `DataView`, `TextEncoder`, `ReadableStream`, `WritableStream`, `AbortSignal`, `crypto.subtle`, `crypto.getRandomValues`, `performance.now`, `queueMicrotask`, timers. It never imports a `node:` module, `ws`, or a DOM-only API. The node and web entries re-export the core and add the runtime specific transports and providers.

Supported language level: ES2023 output, ES modules only (no CommonJS build; Node 22 `require()` of ESM works for synchronous graphs if a consumer needs it). TypeScript consumers get `.d.ts` with `exactOptionalPropertyTypes` friendly signatures.

### 3.1 How QUIC is provided

The protocol needs a QUIC implementation that exposes bidirectional streams, per-stream resets with an application code, connection close with a code and reason, and keying-material export for exporter-bound identity. No JavaScript runtime offers that today: Node 22 has no `node:quic`, the `node:quic` of later releases is experimental and has no WebTransport, Bun and Deno have no QUIC API, and browsers offer only WebTransport (no raw QUIC, and in Chromium no `exportKeyingMaterial` yet).

**Decision.** Two providers behind one contract, `QuicConnector` and `QuicListener` in `src/core/quic/contract.ts`:

1. **Native provider** (`@polyverse/switchboard/node`, `nativeQuic()`): an N-API addon written in Rust with `napi-rs`, in `native/`, over `quinn` 0.11 and `rustls`, serving raw QUIC with ALPN `switchboard` and WebTransport over HTTP/3, both as client and as server. The addon is built per platform and published as optional dependencies (`@polyverse/switchboard-quic-linux-x64-gnu`, `-darwin-arm64`, `-win32-x64-msvc`, and so on; the exact list is the QUIC thread's call). The node entry loads it lazily on first use, so a consumer that never touches QUIC never loads native code, and a missing binary fails with `UNSUPPORTED` and a message that says which package to install. One addon serves Node, Bun and Deno, since all three load N-API modules.
2. **WebTransport provider** (`@polyverse/switchboard/web`, `webTransport()`): the W3C WebTransport API as the browser offers it. Raw QUIC is not available in browsers. Chromium 149 quirks from the wiki apply (no `exportKeyingMaterial`, so identity is nonce-bound; no `sendOrder`; 8-bit stream error codes).

The addon reuses the Dart package's shim: `switchboard_quic/native/sbq` already implements the connection and stream model, HTTP/3 and WebTransport over quinn, with 11 000 lines including tests, and it is the reference the Dart tests exercise. The user approved reusing it (2026-10-14). The QUIC thread built such an addon with `napi-rs` 3 and completed a QUIC handshake with matching TLS key export on both ends in Node 22, 24 and 26, Bun and Deno (`notes/quic.md`, section 9). The addon mirrors the C ABI function for function (handles as numbers, `sbq_poll` filling an event array, the doorbell as a non-blocking thread-safe function with a weak reference so an idle pump does not hold the process open); all protocol logic stays in TypeScript, and the TS native provider is a direct port of the Dart one. How the addon depends on the shim:

- **Decided (2026-10-14, the user's call):** the shim is vendored, permanently. `native/sbq-vendor/` holds `switchboard_quic/native/sbq` at the commit recorded in `native/sbq-vendor/COMMIT`, with one change, `"rlib"` added to the crate types so the addon can depend on it by path; `tool/sync-sbq.mjs` refreshes it from a `switchboard_quic` checkout, and a CI check verifies that the vendored tree equals upstream at that commit except for that line. No change is made to `switchboard_quic`.

Alternatives considered and rejected: a pure FFI layer over the unchanged cdylib (`koffi`, `bun:ffi`, `Deno.dlopen`: two native artifacts per platform, three loaders, blocking-callback rules; kept as a possible later fallback since the addon mirrors the C ABI), `@fails-components/webtransport` (a second QUIC stack with its own dialect, no raw QUIC, fails in Deno), a fresh quinn binding (would re-derive what `sbq` settled), Deno's built-in QUIC (no exporter, no pinning, `--unstable-net`), and WASM (a possible no-prebuild fallback later).

`node:quic` stays a possibility as a third provider behind the same contract once it stabilises and grows keying-material export; nothing above the provider contract would change.

### 3.2 Bun and Deno

Bun runs the Node entry unchanged; its WebSocket and `node:net` compatibility is sufficient, and it loads N-API addons. Deno 2 runs it with `--allow-net --allow-ffi` (the addon) through its `node:` compatibility and npm specifiers. Neither gets a separate entry point. The CI runs the unit and integration projects on Bun; Deno is run by hand until the suite is known to pass there, then added.

## 4. Package layout and module boundaries

```
switchboard_ts/
  package.json               @polyverse/switchboard, ESM, exports: ., ./node, ./web, ./testing
  src/
    core/                    runtime neutral; never imports node:, ws, or DOM-only APIs
      index.ts               gathers the barrels below
      bytes.ts               ByteReader, ByteWriter, u48 helpers, hex, utf8, PayloadLike
      name.ts                Name (8-byte), NameLike
      status.ts              StatusCode, Status, SwitchboardError, ProtocolError
      address/               ServiceAddress, ChannelAddress (done)
      transport/             contract.ts (FrameTransport); stream framing, memory pair, WebSocket client
      mux/                   contract.ts (MuxConnection, MuxChannel, MuxOptions); frame codecs, control
                             messages, FrameMuxConnection, output scheduler, BindingConnection base
      identity/              contract.ts; credential and scope codecs, HMAC and Ed25519 verifiers,
                             issuer, IDENT proof (nonce and exporter bound)
      talk/                  contract.ts (TalkChannel, TalkMessage, TalkOptions); frame codec, channel,
                             message, bulk transfers, forwardMessage
      naming/                naming protocol codecs, NamingClient, NamingService, NamingResolver,
                             slot tables, SlotGate, slot channels, migration
      switchboard/           contract.ts (handlers, policies, resolvers); the Switchboard node core,
                             dispatch, ChannelPolicies, StaticResolver, EndpointResolver, proxying,
                             relay
      client/                ReconnectingClient, Backoff, PeerSet, MeshNode
      quic/                  contract.ts (QuicConnector, QuicListener, QuicSession, QuicStream);
                             records, QuicBindingConnection, QuicBindingChannel, HELLO, identity rules
      util/                  Clock, Logger, Deferred, Emitter, AsyncQueue
    node/                    Node, Bun, Deno: TCP and ws transports, listenTcp/listenWebSocket,
                             the native QUIC provider loader; re-exports the core
    web/                     browsers: the WebTransport provider; re-exports the core
    testing/                 ManualClock, fake QUIC provider, shared harness helpers
  native/                    the N-API addon crate (Rust), its build and packaging scripts
  test/
    unit/                    fast, in process, one module at a time
    vectors/                 the wiki test vectors (hand-written per codec and wiki-vectors.json driven)
    integration/             two nodes end to end over memory, TCP, WebSocket, QUIC on the loopback
    interop/                 the Dart peer, the TS peer, the launcher and the pairing matrix
  docs/
    design.md                this document
    work-split.md            per-thread assignments and the integration process
    wiki/                    wiki-ready pages (Home, Getting Started, Runtimes, API, Testing, Contributing)
  tool/                      maintenance scripts (vector extraction, sbq sync, prebuilt packaging)
```

**Boundaries that hold the parallel work together:**

- Every layer's public interface lives in that directory's `contract.ts`, owned by the lead. A thread implementing a layer programs against the contracts of the layers below and exports its surface from its directory's `index.ts`. A change to a contract is a small PR to the lead first (or a note in the thread's PR that the lead merges first), never a silent edit.
- `src/core` is runtime neutral, enforced by `tsconfig` (`lib` without Node types for the core is a follow-up; until then, review) and by the unit project running under Bun in CI.
- The QUIC binding depends only on `quic/contract.ts` and implements `mux/contract.ts`. Talk and everything above depend only on `mux/contract.ts` and `hasOrderedChannels`; they never see frames, CREDIT or the scheduler.
- The `_bulk` filter and the "admit after GOAWAY" rule are installed on a connection by the Talk layer through two hooks on `MuxConnection` (`adoptIncoming(filter)` and `admitAfterGoAway(predicate)`), so the mux does not decode address headers and raw `incoming` consumers never see `_bulk` channels. This replaces Dart's `adoptBulk` call that raw consumers had to make.

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
| Receiving | pull based. Channels, incoming channels, messages, stream items and resolver events are `AsyncIterable`: `for await` takes delivery, and taking delivery is what returns flow-control credit. `break` cancels. Channel bytes are also a `ReadableStream<Uint8Array>` (`channel.readable`) with `highWaterMark: 0` for `pipeTo` and `pipeThrough`. |
| Sending | `send()` returns a promise for hand-over (resolves once accepted into the window), plus `isWritable` and `whenWritable`. Channel bytes are also a `WritableStream<Uint8Array>` (`channel.writable`) whose `write` awaits `send`. |
| Events | typed `Emitter` with `on/once/off` returning an unsubscribe function, and `events(name, signal)` as an `AsyncIterable`. Used for state changes (`ReconnectingClient.states`, resolver events, `identityChanged`); never for data delivery. |
| Cancellation | `AbortSignal` on every operation that waits: connect, open, `identify`, `ping`, `request`, resolve. Timeouts are options with defaults; a signal shortens them. `TalkMessage.signal` aborts when the request is cancelled or the channel closes. |
| Lifetimes | `Symbol.asyncDispose` on connections, channels, Talk channels, listeners and nodes, so `await using` closes them. `done` is a promise of the end `Status`. `close()` is idempotent and never throws. |
| Time | every timer comes from the `Clock` in the options (default `systemClock`); tests inject `ManualClock`. Monotonic time only. |
| Logging | an injectable `Logger` (default silent) with the Dart logger names. Never `console`. |
| Crypto | WebCrypto: Ed25519, HMAC-SHA256, SHA-256 and `getRandomValues`. All verification is async; the credential codec stays synchronous. |
| Tree shaking | `sideEffects: false`, no module-level state except the memory endpoint registry (its module is only imported by what uses it). |
| Naming | camelCase members, the Dart names otherwise. Dart `Stream` getters become `AsyncIterable` getters of the same name (`incoming`, `messages`, `items`); Dart `Future` getters become promise getters (`done`, `connected`); Dart `sink` becomes `writable`. |

## 6. The layers

Each subsection names the contract file, the Dart source it ports, the TS surface and what differs. Signatures are summaries; the contract files are the definition.

### 6.1 Primitives: `bytes.ts`, `name.ts`, `status.ts` (done)

- `ByteReader` and `ByteWriter` port `bytes.dart`: u8, u16, u24, u32, u48 little endian, 8-byte names, length-8 strings, `take`, `rest`; `ByteFormatError` on truncation. `hexBytes`, `hexString`, `bytesEqual`, `concatBytes`, `utf8Encode`, `utf8Decode`, `toPayload`.
- `Name`: 8 bytes, zero padded, no interior zero; `Name.of('echo')`, `Name.fromBytes`, `.key` for maps, `.isReserved` for `_` types, `toString()`.
- `StatusCode` is the wiki table (gRPC 0 to 16, Switchboard 32 to 38). `Status` keeps the raw body after the code byte-exact, so a relayed CLOSE, GOAWAY or ABORT is forwarded unchanged (including MOVED and RELOCATED fields and malformed UTF-8); `reason` is decoded on demand. `Status.decode(bytes)` with an empty body is OK; one byte is a `StatusFormatError`. `SwitchboardError.of(code, reason)`, `ProtocolError`, `isStatus(error, code)`.

### 6.2 Transports: `transport/contract.ts`

Ports `transport/*.dart`. A transport carries whole frames, each a `Uint8Array`, in order, in both directions:

```ts
interface FrameTransport {
  readonly readable: ReadableStream<Uint8Array>;   // one chunk per frame; closing = peer closed
  readonly writable: WritableStream<Uint8Array>;   // one chunk per frame; close() = orderly close
  readonly maxFrameSize?: number;                   // FrameLimited: announced in LIMITS when smaller than ours
  readonly remote?: string;                         // 'tcp://10.0.0.5:4000', 'ws://…', 'mem://name'
  abort?(reason?: unknown): void;                   // tear down without the orderly close
  readonly bufferedOutputBytes?: number;            // OutputBuffered
  readonly acceptedOutputBytes?: number;            // OutputAccounting
  readonly isInputThrottled?: boolean;              // the mux has paused reading
  readonly isOutputReady?: boolean; readonly outputReady?: Promise<void>;   // OutputReady
}
type TransportConnector = (signal?: AbortSignal) => Promise<FrameTransport>;
```

Capabilities are optional members (checked with `!== undefined`), replacing Dart's `is FrameLimited` style interface checks. Implementations:

| Transport | Module | Notes |
| --- | --- | --- |
| Stream framing | `core/transport/stream-framing.ts` | `StreamFramer`: the 8-byte preamble (`SWBD`, version 1, flags 0) and u32 LE length-prefixed frames as two `TransformStream`s over any byte duplex; default limit 1 MiB; a frame over the limit is a `ProtocolError` FRAME_TOO_LARGE and the transport is aborted. `streamTransport(byteDuplex, options)` wraps a `{ readable, writable }` byte pair (a Node socket, a QUIC stream, a Deno conn). |
| Memory pair | `core/transport/memory-transport.ts` | `memoryTransportPair({ delay, maxFrameSize })` for tests and in-process connections; `delay` 0 still goes through a macrotask, as Dart's does. `MemoryEndpoints` is the `mem://` registry used by `listenMemory`. |
| WebSocket client | `core/transport/websocket-transport.ts` | over the global `WebSocket` (browsers, Node 22, Bun, Deno): `binaryType = 'arraybuffer'`, subprotocol `switchboard`, receive limit enforced in `onmessage`, `bufferedAmount` polled for `bufferedOutputBytes`. `connectWebSocket(url, options)`. |
| Node TCP | `node/transport/tcp.ts` | `net.Socket` with `setNoDelay`, stream framing, `writableLength` and `bytesWritten` accounting, pause and resume for throttling, the Dart close sequence (end, linger, destroy). `connectTcp(host, port)`, `listenTcp`. |
| Node WebSocket server | `node/transport/ws-server.ts` | the `ws` package (optional peer dependency) with `perMessageDeflate: false`, `maxPayload = maxFrameSize`, subprotocol check, 403 for other paths or the `onOtherRequest` hook; `acceptWebSocket(request, socket, head)` for an application's own HTTP server. `ws` is also used as the client when a receive limit or no compression must be enforced (`connectWebSocket` picks it when present on Node). |

Endpoint schemes: `tcp://host:port`, `ws://` and `wss://host:port/path`, `mem://name`, `quic://host:port`, `https://host:port/path` (WebTransport). `parseEndpoint(uri)` in `core/transport/endpoint.ts` classifies them; a runtime's `connect` refuses the schemes it cannot serve with `UNSUPPORTED`.

### 6.3 Mux and the channel contract: `mux/contract.ts`

Ports `mux/*.dart`, the frame mux and the abstract connection seam. `MuxConnection` and `MuxChannel` are what everything above programs against; two bindings implement them, the frame mux over a `FrameTransport` and the QUIC binding over a `QuicSession`.

```ts
const connection = new FrameMuxConnection(transport, { isInitiator: true, ...options });
const channel = connection.open(openPayload, { after, priority, manualCredit });
for await (const incoming of connection.incoming) { … }     // peer-opened channels, backlog bounded by maxChannels
await connection.identify(credential, { intent, receiver, signal });
await connection.goAway(status); await connection.close();
await using connection;                                      // asyncDispose = close()
```

`MuxChannel`: `id`, `isLocallyOpened`, `openPayload`, `state`, `priority` (settable), `readable` and `[Symbol.asyncIterator]` for subframes, `writable` and `send(subframe)` (promise for hand-over, throws synchronously for misuse), `sendWindow`, `sendCost(length)`, `maxSubframeLength`, `isWritable`, `whenWritable`, `receiveWindow`, `bufferedBytes`, `manualCredit`, `heldBytes`, `consumed(bytes)`, `grant(bytes)`, `openAfter(other)`, `close(status?)`, `done`, `awaitingLimits`, `limitsKnown`.

What is kept from Dart, exactly: option validation and defaults (`MuxOptions`, section 14 of the core notes), the LIMITS exchange and `awaitPeerLimits`, window bookkeeping, credit returned at delivery, the output scheduler with the bulk zipper, keep-alive, GOAWAY and the close sequence, the pending-rejection table, the receive high-water mark, held OPEN payloads, the identity state machine with the transport paused during verification, the connection versus channel error scopes. The Dart tests for all of it port with `ManualClock`.

What changes:

- Delivery is pull based (section 5). A channel not being read holds its credit; `break` out of `for await` discards and closes.
- Two hooks replace Dart's address decoding inside the mux: `connection.adoptIncoming(filter)` lets a layer claim peer-opened channels before they reach `incoming` (Talk claims `_bulk` channels for their parent), and `connection.admitAfterGoAway(predicate)` decides which OPENs are still accepted after GOAWAY (Talk admits `_bulk` continuations).
- `BindingConnection` is the abstract base for the QUIC binding: neutral diagnostics, `hasOrderedChannels = false`, drain holds (`retainForDrain`, `hasDrainHolds`, `retainedForDrain`, `drainStateChanged`).
- Control message codecs (`MuxPing`, `MuxGoAway`, `MuxLimits`, `MuxNonce`, `MuxIdent`, `MuxCredit`, `MuxHello`) are exported for the vectors and for the QUIC binding, which reuses NONCE, IDENT, GOAWAY and PING.

### 6.4 Identity: `identity/contract.ts`

Ports `identity/*.dart`: `Credential` (kind, holder, scopes, expiry, signature kind, signature) and its codec, `Scope` patterns and matching, `HolderKey` (Ed25519 key pair, or a bearer), `CredentialVerifier` (HMAC-SHA256 with a shared secret, Ed25519 with issuer public keys), `CredentialIssuer`, the IDENT proof over `SWBIDENT` ‖ nonces ‖ intent ‖ receiver and its exporter-bound `SWBIDTLS` variant, and `PeerIdentity`. All signing and verification is `async` over `crypto.subtle`; Ed25519 private keys import as PKCS#8 and export the same way. The codec is synchronous and byte-exact against the wiki vectors.

### 6.5 Talk: `talk/contract.ts`

Ports `talk/*.dart`. `TalkChannel` wraps a `MuxChannel`:

```ts
const talk = new TalkChannel(channel, options);        // or incoming.talk(), node.openTalk()
for await (const message of talk.messages) { … }       // or talk.serve(handler)
const reply = await talk.request('PROC', payload, { timeout, signal, onExtend, ordered });
const request = talk.startRequest('PROC', payload);    // { requestId, response, deadline, cancel() }
const stream = talk.streamRequest('PROC', payload);    // AsyncIterable of items, `.done` = the final reply
await talk.send('EVENT', payload);                      // fire and forget
await talk.abort(status); await talk.close();
```

`TalkMessage`: `procedure`, `procedureName`, `payload`, `text()`, `json()`, `bytes()`, `isBulk`, `bulkLength`, `bulk` (a `ReadableStream` when `streamBulk` selects it), `requestId`, `responseId`, `expectsReply`, `expectsStream`, `isCancelled`, `signal`, `canReply`, and the reply family: `reply`, `replyRequest`, `startReplyRequest`, `replyStreamRequest`, `replyItem`, `replyItemRequest`, `startReplyItemRequest`, `replyItemStreamRequest`, `replyStream`, `replyAbort`, `extend`, `setReplyTimeout`.

Kept exactly: the timeout regime (request, reply, EXTEND clamping and buffer), item windows and credits, bulk transfers (threshold, chunking, `_bulk` channel routing, reassembly budget, unclaimed bulk limits, idle timeouts), abort semantics, `forwardMessage`. `ordered` requests depend on per-microtask delivery; the TS implementation documents its guarantee in terms of `queueMicrotask` ordering and tests it.

Changed: payloads are `PayloadLike`; a bulk source is a `ReadableStream<Uint8Array>` or an `AsyncIterable<Uint8Array>` with an optional known length; `TalkStream` is itself the async iterable of items.

### 6.6 Addressing: `address/` (done)

`ServiceAddress` (`type/instance`, `ServiceAddress.parse('npc/1a2b')`) and `ChannelAddress` (type, instance, shard, host, payload) with `encode()` and `ChannelAddress.decode(openPayload)` throwing `ProtocolError` on reserved flag bits, truncation or an invalid name. `ChannelAddress.to(address, payload)` and `with()` for rewrites.

### 6.7 The node: `switchboard/contract.ts`, `switchboard/`, `node/`

Ports `switchboard.dart` and its helpers. The runtime-neutral core node (`core/switchboard/switchboard.ts`) holds the registrations, dispatch, policies, the connection pool, resolvers, identity on connections, the proxy and the relay; it takes transports and listeners from a `Runtime` object the entry point supplies (`connect(endpoint)` and the listener factories). `@polyverse/switchboard/node` exports `Switchboard` with `listenTcp`, `listenWebSocket`, `listenMemory`, `listenQuic`, `listenWebTransport`, `accept`, `acceptWebSocket` and `connect` over `tcp`, `ws`, `wss`, `mem`, `quic` and `https`; `@polyverse/switchboard/web` exports a `Switchboard` that can `connect` over `wss` and `https` and listen on nothing.

```ts
class Switchboard {
  constructor(options?: SwitchboardOptions);       // resolver, defaultPayload, muxOptions, talkOptions, connectTimeout,
                                                   // allowHostHint, maxConnectionsPerEndpoint, selection, outgoingPolicy,
                                                   // endpointPolicy, credentialFor, credential, holderKey, verifier,
                                                   // identifyOutgoing, identifyFor, identityTimeout, expectedIdentityFor,
                                                   // relay, localConnections, quic, clock, logger
  registerService(type: NameLike, handler: ChannelHandler, options?: { instance?, acceptAnyInstance? }): void;
  unregisterService(type: NameLike, instance?: number): void;
  defaultService: ChannelHandler | undefined; catchAll: ChannelHandler | undefined;
  openChannel(address: ServiceAddressLike, options?: { shard?, payload?, where?, signal? }): Promise<MuxChannel>;
  openTalk(address, options?): Promise<TalkChannel>;
  openChannelAt(endpoint: string, address: ChannelAddress, options?): Promise<MuxChannel>;
  openChannelOn(connection: MuxConnection, address: ChannelAddress): MuxChannel;
  openChannelToSlot(type, slot, options?): Promise<SlotChannel>; openTalkToSlot(…): Promise<TalkChannel>;
  connect(endpoint: string, options?: { policy?, record?, signal? }): Promise<MuxConnection>;   // pooled
  dial(endpoint, options?: { policy?, intent?, receiver?, record?, signal? }): Promise<MuxConnection>;   // not pooled
  broker(address: ServiceAddress, options?): Promise<MuxConnection>;
  identifyOn(connection, options?): Promise<void>; updateCredential(credential): Promise<void>;
  readonly connections: AsyncIterable<MuxConnection>; readonly listeningEndpoints: readonly string[];
  isOwnEndpoint(endpoint: string): boolean;
  close(): Promise<void>; [Symbol.asyncDispose]();
}
```

`ChannelPolicies` (`allowAll`, `denyAll`, `denyReserved`, `allowTypes`, `scoped`, `requireIdentity`, `all`), `IncomingChannel`, `Resolver`, `SlotResolver`, `BrokeringResolver`, `StaticResolver`, `EndpointResolver`, `NamingResolver`, `pipeChannels`, `proxyHandler`, `RelayConfig` and the relay service keep their Dart semantics (the `dart-design.md` behaviour notes are the contract). Listener options are an object (`{ path, policy, onOtherRequest }`), bound endpoints are returned as strings. `ServiceAddressLike = ServiceAddress | string | Name` so `openTalk('echo')` and `openTalk('npc/1a2b')` both work.

### 6.8 Naming and sharding: `naming/`

Ports `naming/*.dart` and the slot machinery: the `_ns` protocol codecs (REGISTER, UNREGSTR, WATCH, UNWATCH, LOOKUP, RENEW, CONNECT, UP, DOWN, and the slot messages SLOTS, HOLDING, CLAIM, RELEASE, LOCATE, MIGRATE, ASSIGN, DRAIN, FORWARD, RESUME), `NamingClient` (register, watch, lookup, renew, connectTo, locate), `NamingService` (the server with leases, slot spaces and migration), `NamingResolver`, `SlotTable`, `SlotGate`, `SlotChannel`, slot reopen on MOVED, `fnv1a32`, `slotForKey`, `slotForText`. Events are `AsyncIterable` and `Emitter` based; everything else keeps the Dart shape.

### 6.9 Clients: `client/`

`ReconnectingClient(connector, options)` over a `TransportConnector` or a `QuicConnector` endpoint: `states` (an `Emitter` event and an `AsyncIterable`, replaying the current state), `state`, `connection`, `connected`, `start`, `stop`, `reconnectNow`, `close`, `openChannel`, `openTalk`, `openPersistent`, `openPersistentTalk`, `incoming`. `Backoff` is shared with `PeerSet`; `MeshNode` as in Dart. Backoff randomness is injectable (`random: () => number`).

### 6.10 QUIC and WebTransport binding: `quic/contract.ts`, `quic/`

Ports `switchboard_quic/lib/src/binding/*`. The provider contract (`QuicConnector`, `QuicListener`, `QuicSession`, `QuicStream`) is the TS form of Dart's `QuicProvider`: sessions, bidirectional streams with `read(max)`, `write`, `finish`, `reset(code)`, `stop(code)`, `priority`, keying-material export, peer certificate hash, close with code and reason. `QuicBindingConnection extends BindingConnection` implements `MuxConnection` over a session: one stream per channel, the record codec (u32 LE header, length in bits 0 to 29, command in bits 30 and 31: DATA, OPEN with u48 id and payload, CLOSE with status then FIN), the control stream (the initiator's stream with OPEN channel 0), HELLO first (record limit, channels, flags with EXPORTER), ids never reused, resets and STOP_SENDING carrying status codes 0 to 255, GOAWAY and drain holds for bulk, exporter-bound identity with `EXPORTER-Channel-Binding` on raw QUIC and `switchboard-ident` on WebTransport, `QuicIdentityRules` for self-signed certificates. `connectQuic(endpoint, provider, options)` and `listenQuic(provider, options)` produce connections and listeners the node uses.

The native provider (`node/quic/native-provider.ts` plus `native/`) and the WebTransport provider (`web/quic/webtransport-provider.ts`) implement the contract; `testing/fake-quic-provider.ts` implements it in memory with independent stream delivery, configurable delays, flow control, a stream limit, resets and an exporter, so the binding's tests and the node's QUIC paths run without native code.

## 7. Cross-cutting behaviour

**Errors.** Protocol errors on a connection end it (GOAWAY PROTOCOL_ERROR, then close); protocol errors in a channel's OPEN payload or Talk header close that channel with PROTOCOL_ERROR. The library never throws across an `await` for a peer's misbehaviour; it reports through statuses, `done` promises and rejected request promises. It throws `RangeError`/`TypeError` synchronously for the caller's misuse. The IDENT failure reason never reaches the wire or an error `cause`.

**Time.** `Clock` (section 5). `systemClock` re-arms timers longer than 2^31-1 ms. Timers hold a Node process open; a consumer that wants an idle connection not to keep the process alive closes it.

**Logging.** `Logger.log(level, name, message, error?)` with names `Switchboard.Mux`, `Switchboard.Talk`, `Switchboard.Node`, `Switchboard.Naming`, `Switchboard.Quic`, `Switchboard.Client`.

**Unhandled rejections.** Every promise the library creates and hands out (`send`, `whenWritable`, `done`, `connected`, request responses) is pre-caught; a consumer that drops it sees no warning, one that awaits it sees the rejection.

**Resource management.** `Symbol.asyncDispose` everywhere a Dart `close()` exists. `done` resolves, never rejects, with the end `Status`.

**Concurrency model.** Single threaded, like Dart. Delivery order within a channel is the arrival order; across channels it is the mux's scheduling, as in Dart. Where Dart relies on microtask ordering (Talk `ordered`), the TS code relies on `queueMicrotask` ordering and the tests assert it.

## 8. Build and tooling

| Tool | Choice | Why |
| --- | --- | --- |
| Language | TypeScript 7 (`tsc`), `module: NodeNext`, target ES2023, `.ts` extensions in imports with `rewriteRelativeImportExtensions`, `verbatimModuleSyntax`, `erasableSyntaxOnly`, strict plus `exactOptionalPropertyTypes` and `noUncheckedIndexedAccess` | native TS runs the sources directly in Node 22 (`--experimental-strip-types`), Bun and Deno; `tsc` emits `dist/` with declarations for publishing |
| Tests | Vitest 5, projects `unit`, `integration`, `interop` (and `browser` when the QUIC thread adds Playwright) | fast, ESM native, fake timers available, browser mode available |
| Lint and format | Biome 2 (`biome check`), single quotes, 100 columns, `noExplicitAny` error, `useImportType` error | one tool, fast, no plugin graph |
| Package | `@polyverse/switchboard` 3.0.0 (the Dart package is 3.x; the ports share the protocol version line), ESM only, `exports` for `.`, `./node`, `./web`, `./testing`, `./package.json`; `ws` optional peer dependency; prebuilt QUIC addons as optional dependencies | consumers import one package; `ws` only where a server is needed |
| Native | Rust, `napi-rs`, `cargo`, prebuilt by the CI release workflow for the platforms listed in `native/README.md` | section 3.1 |
| Scripts | `npm run check` (lint, typecheck, unit and integration tests), `npm run test:interop`, `npm run build` | what a contributor runs before pushing |
| CI | `.github/workflows/ci.yml`: lint and typecheck; unit and integration on Node 22, 24 and 26 and on Bun; `interop` with Dart 3.13.5, Rust 1.99.0 and the pinned Dart checkouts; `native` builds the addon; `browser` runs Chrome for Testing | section 9 |

Node 22 is the floor because of global `WebSocket`, stable `crypto.subtle` Ed25519, `Promise.withResolvers`, `Symbol.asyncDispose` and `ReadableStream.from`.

## 9. Test strategy

Four layers, lowest first. Every layer runs in CI; the interop layer needs Dart and skips locally with one visible line when it is absent.

1. **Unit and vectors** (`test/unit`, `test/vectors`, `src/**/*.test.ts`): every codec against the wiki vectors, by hand per codec (the thread porting it writes them from `test-vectors.md` and from the Dart vector tests) and data driven from `test/vectors/wiki-vectors.json`, extracted from the sway wiki by `tool/extract-wiki-vectors.mjs` with the sway commit recorded inside (sway is private, so CI reads the committed JSON and checks that every vector id is asserted by some test). The roughly 200 Dart test vectors not on the wiki page (core notes, appendix A) are ported with the codec they belong to. Behavioural tests port the Dart unit tests one to one over the memory transport with `ManualClock` in place of `fake_async`; the Dart test files are the checklist (`interop-dart-suites.md` catalogues them).
2. **Integration** (`test/integration`): two or more nodes in one process over the memory transport, TCP, WebSocket and QUIC on the loopback (the fake provider first, the native provider when it exists): dispatch, policies, identity, naming, sharding, proxying, relay, the reconnecting client, bulk transfers, GOAWAY and drain. The Dart `test/integration` files are the checklist.
3. **Interop** (`test/interop`): the Dart reference as a peer. One JSON-lines peer protocol spoken by a Dart peer (an AOT executable built from `test/interop/dart_peer/`, depending on the two Dart repositories by path at the commits in `dart_commits.txt`) and a TS peer (`test/interop/ts_peer/`, using only the public API). Each scenario runs across four pairings, Dart to Dart (checks the scenario against the reference), TS to Dart, Dart to TS, TS to TS, over TCP, WebSocket, raw QUIC and WebTransport as the pairing allows; a cell the TS port cannot serve yet is skipped with a reason. A codec oracle in the Dart peer decodes mux, Talk, address, status, naming and credential messages, so the vector suite also runs differentially, and a fuzz pass compares random inputs. Raw byte conformance scripts (the hand-rolled probe) check framing against the Dart server without the library.
4. **Browser** (`test/browser`, Vitest browser mode or Playwright): the web entry in headless Chromium against a Node or Dart WebTransport listener; the Chromium version matters for the protocol check, so CI uses a current one.

A change to the pinned Dart commits is a deliberate commit; a protocol change upstream shows up as one failing interop run.

## 10. Work split and integration

The detailed assignments are in `docs/work-split.md`. In short: six threads, each owning directories and their tests, programming against the contracts in this document; PRs into the integration branch `claude/switchboard-ts-design-s5r9na`; the lead reviews, merges in dependency order, runs the whole suite including interop, and keeps this document and the wiki pages in step. `main` receives the integration branch once the suite is green end to end.

## 11. Decisions taken

| Question | Decision |
| --- | --- |
| Package name | `@polyverse/switchboard`, version line 3.x like the Dart package |
| Module format | ESM only, Node 22 floor |
| QUIC on Node, Bun, Deno | one N-API addon mirroring the `sbq` C ABI; prebuilt optional dependencies; lazy load; a missing binary is `UNSUPPORTED`, never a crash |
| Provider contract | the Dart shape typed for TS: pull-based `read(max)`, `write`, `finish`, `reset`, `stop`, streams taken only while asked for; WHATWG streams are adapted at the provider, not required of it |
| QUIC in browsers | WebTransport provider; raw QUIC impossible there |
| `node:quic` | not now; possible later behind the same contract |
| Bytes API | `Uint8Array`, `PayloadLike` for inputs, `text()/json()/bytes()` on outputs |
| Delivery | pull based async iterables plus Web Streams; credit on delivery |
| Events | typed `Emitter`; no `EventEmitter`, no `EventTarget` |
| Time and logging | injectable `Clock` and `Logger` in every options object |
| `_bulk` routing | the Talk layer installs it through `adoptIncoming` and `admitAfterGoAway` hooks; raw consumers never see `_bulk` |
| Test runner | Vitest, projects `unit`, `integration`, `interop`, `browser` |
| Interop layout | `test/interop/{dart_peer,ts_peer,…}`, `test/vectors/wiki-vectors.json`; the harness thread's proposal as written |
| Integration branch | `claude/switchboard-ts-design-s5r9na`; `main` is empty until the first green end-to-end state |

## 12. Deviations from the Dart API

Behavioural differences a Dart user would notice; the wire is unchanged.

1. Pull delivery: a channel or Talk channel nobody iterates holds its credit (Dart buffered until listened, then pushed); `break` cancels and closes.
2. `sink.add` fire-and-forget is gone; `writable` applies backpressure and `send()` resolves on hand-over.
3. Raw `incoming` consumers never see `_bulk` channels and do not call `adoptBulk`; Talk installs the filter.
4. All identity verification is asynchronous; the transport is paused meanwhile exactly as the Dart IDENT path already does.
5. Timers do not use `unref`; close what should not keep the process alive.
6. Durations are milliseconds; `undefined` where Dart used `null`.
7. Every waiting call takes an `AbortSignal`; Dart used `Future.timeout` at call sites.

## 13. Open questions

1. ~~`rlib` in `switchboard_quic/native/sbq/Cargo.toml`.~~ Decided: no upstream change; the addon vendors the shim (section 3.1).
2. **`main`.** The repository is empty, so the integration branch has no base for a pull request. Default: the integration branch is the base for thread PRs; `main` is initialised from it at the first green end-to-end state, with the user's go-ahead.
3. **Prebuilt platforms.** The targets `switchboard_quic`'s release workflow builds: Linux x64 and arm64 (glibc), macOS arm64 and x64, Windows x64 (`+crt-static`); musl later. A platform without a prebuild has no QUIC: the connector reports `UNSUPPORTED` and the node moves on to the record's next endpoint.
4. **Deno in CI.** Added once the suite passes there by hand.
5. **GitHub wiki.** The wiki repository could not be written from this environment; the pages live under `docs/wiki/` in wiki-ready Markdown until they can be pushed.
