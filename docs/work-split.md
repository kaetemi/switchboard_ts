# Work split and integration process

_Owned by the lead thread. v1, 2026-10-14; status marks added on 2026-10-14 after the continuation's merges. Line counts are the Dart sources being ported, as a size guide._

Six threads port the package in parallel. Each owns directories and their tests, programs against the contracts in `src/core/*/contract.ts`, and opens pull requests into the integration branch. The lead reviews, merges in dependency order, runs the full suite including interop, and keeps `docs/design.md`, `docs/work-split.md` and `docs/wiki/` in step.

This is the original assignment. The first phase went through pull requests #1 to #18 into `claude/switchboard-ts-design-s5r9na`; `main` was initialised from it at the first hand-off and is the integration branch since. The continuation worked on topic branches `claude/<topic>` (one git worktree each), merged into `main` by the lead with merge commits. Each deliverable below is marked **Done** (with where it landed), **In flight** (the topic branch working on it) or **Open**; `docs/handoff.md` has the current state.

## Status at a glance

| Thread | Deliverables | State |
| --- | --- | --- |
| 1. Protocol core | 1 to 5 | Done: transports (#5, `claude/transports`), codecs and binding base (#9), frame mux (#14) with every Dart mux suite (`claude/mux-tests`), identity (`claude/identity`). Acceptance: the raw probe against a TS server waits on the TS peer. |
| 2. Talk | 1 to 4 | Done (#1, #4, #11), with the integration tests over the frame mux, TCP and the QUIC binding (`claude/talk-integration`). Acceptance: the Talk scenarios wait on the TS peer. |
| 3. Node and client | 1 to 5 | Done (#6, #10, #16, #17, `claude/node-talk`, `claude/mesh`, `claude/transports`, `claude/slot-routing`, `claude/sharding`). Acceptance: the interop cells wait on the TS peer. |
| 4. Naming and sharding | 1 to 4 | Done (#3, #13, `claude/slot-routing`, and the `test/integration/sharding/` port on `claude/sharding`). Acceptance: interop with the Dart naming service waits on the TS peer. |
| 5. QUIC | 1 to 5 | Done (#2, #8, #15, `claude/webtransport`, `claude/quic-identity`); publishing is the user's call. Acceptance: Chromium reaches a TS WebTransport listener (done); interop over raw QUIC and WebTransport waits on the TS peer. |
| 6. Interop | 1 to 4 | 1 done (#7, #12); 2 to 4 done (`claude/ts-peer`, merged 2026-10-18); the remainder is `work-split-2.md`'s interop thread. |

## Ground rules for every thread

- **Read first:** `docs/design.md` (sections 4, 5 and the subsection for your layer), the contract files you implement and depend on, the wiki pages for your layer in `kaetemi/sway` `wiki/switchboard/`, the matching section of `switchboard/doc/dart-design.md`, and the Dart sources named below. The notes under `docs/notes/` are inputs: `core.md` (sections 0 and 20 for everyone), `interop.md`, `interop-dart-suites.md` (the Dart test inventory, your checklist).
- **Branch and PRs:** work on your thread's own branch, PRs into `claude/switchboard-ts-design-s5r9na` in `kaetemi/switchboard_ts`. Small PRs in the order listed under "deliverables" beat one large one; the first one can land within a day. Each PR: `npm run check` green (lint, typecheck, unit and integration tests), `tsc` clean, no new `biome-ignore` without a reason.
- **Contracts:** `src/core/*/contract.ts` and the `index.ts` barrels outside your directories are the lead's. When your port needs a contract change, put it in its own small PR (or the first commit of your PR) with a one-paragraph rationale; the lead merges contract changes first so the other threads see them. Never widen a contract silently.
- **Files:** you own what is listed under your thread and nothing else. Shared helpers you need from another thread's directory: ask the lead in your thread, or stub them under `test/support/<thread>/` until they land.
- **Behaviour:** the Dart behaviour is the contract; the Dart tests are the checklist. Port each Dart test file you cover and name the TS file after it. `ManualClock` from `@kaetemi/switchboard/testing` replaces `fake_async`. Byte-exact against the wiki vectors.
- **Style:** `biome check`, 100 columns, single quotes, `type` imports, no `any`, no TS `enum`, `const` objects `as const`, `.ts` import extensions, file header comment as in the existing sources, JSDoc on every exported symbol (one line is fine).
- **No model identifiers** in commits, PR text or code comments. Commit on every milestone; `Co-Authored-By: Claude <noreply@anthropic.com>` on commits.
- **Dart repositories are read-only.** Build the Dart peer from them; change nothing in them.
- **Notes:** keep your running notes under `docs/notes/<thread>.md` (status, decisions, questions for the lead), and write questions for the lead in your thread. Do not wait on an answer for anything reversible: pick the Dart behaviour, say so, keep going.

## Thread 1: Port protocol core (existing thread)

Branch suggestion: `claude/sb-ts-core`. Dart: `lib/src/transport/*` (2 700 lines), `lib/src/mux/*` (5 200), `lib/src/identity/*` (1 300), `monotonic.dart`.

**Owns**

- `src/core/transport/` except `contract.ts`: `stream-framing.ts`, `stream-transport.ts`, `memory-transport.ts` (pair and `MemoryEndpoints` registry), `websocket-transport.ts` (global `WebSocket` client), `endpoint.ts` (`parseEndpoint`), `index.ts`.
- `src/core/mux/` except `contract.ts`: `mux-frame.ts`, `mux-protocol.ts` (PING, PONG, GOAWAY, LIMITS, NONCE, IDENT, CREDIT, HELLO codecs), `frame-mux-connection.ts`, `frame-mux-channel.ts`, `output-scheduler.ts`, `binding-connection.ts` (the abstract base for the QUIC binding, with drain holds), `index.ts`.
- `src/core/identity/` except `contract.ts`: `credential.ts` (codec), `scope.ts`, `holder-key.ts`, `credential-verifier.ts` (HMAC-SHA256 and Ed25519 over WebCrypto), `credential-issuer.ts`, `ident-proof.ts` (nonce-bound and exporter-bound), `peer-identity.ts`, `secure-random.ts`, `index.ts`.
- `src/node/transport/`: `tcp.ts` (`connectTcp`, `listenTcp` returning a listener of `FrameTransport`s), `ws-server.ts` (`listenWebSocket`, `acceptWebSocket` over `ws`), `ws-client.ts` (the `ws` client with `maxPayload` and no compression, chosen by `connectWebSocket` on Node when `ws` is installed), `index.ts`, and `connectTransport(endpoint, options)` in `src/node/transport/connect.ts` covering `tcp`, `ws`, `wss`, `mem`.
- Tests: `test/vectors/{transport,mux,control,credential,identity}.test.ts` (the wiki vectors by hand plus the Dart-only vectors of core notes appendix A), `test/unit/{transport,mux,identity}/**`, `test/integration/mux-{tcp,websocket}.test.ts`.

**Deliverables, in order (one PR each or grouped as marked)**

1. `endpoint.ts`, `stream-framing.ts`, `memory-transport.ts`, `stream-transport.ts`, vectors for the stream binding. Lands first: the Talk and node threads test over the memory pair. **Done** (#5).
2. `mux-frame.ts`, `mux-protocol.ts` with every wiki and Dart vector; `binding-connection.ts` as an abstract class the QUIC thread extends, with Dart's scope: drain holds and neutral diagnostics; the identity exchange, `adoptIncoming` and `admitAfterGoAway` are each binding's own (landed as PR #9). Land this before step 3 so the QUIC thread is unblocked. **Done** (#9).
3. `frame-mux-connection.ts`, `frame-mux-channel.ts`, `output-scheduler.ts`: the full `MuxConnection` and `MuxChannel` over a `FrameTransport`, porting `mux_connection_test.dart`, `mux_channel_test.dart`, `frame_mux_*` tests and the limits, credit, scheduler, keep-alive, GOAWAY, close, held-frame and pending-rejection tests with `ManualClock`. **Done**: the implementation (#14) and the Dart suites `mux_test`, `mux_flow_test`, `mux_limits_test`, `mux_schedule_test`, `mux_vectors_test` and the IDENT gating of `binding_protocol_test` (`claude/mux-tests`, `test/unit/mux/`).
4. Identity: codecs with vectors, verifiers, issuer, the IDENT exchange on the connection (transport paused while verifying, held frames), `identity_test.dart` ports. **Done** (`claude/identity`): `Credential`, `Scope`, `CredentialIssuer`, `CredentialVerifier`, `Ed25519HolderKey`, the credential vectors, `identity_test.dart`; the node and QUIC fixtures use real credentials (`claude/quic-identity`), and `test/support/identity/stub-verifier.ts` remains only for the mux IDENT tests, which check IDENT verification on its own.
5. `websocket-transport.ts`, `src/node/transport/*`, integration tests over the loopback, the Dart `web_socket_*` and `stream_transport_*` test ports. **Done** (`claude/transports`): `connectWebSocket`, `connectTcp`, `listenTcp`, the `ws` server and client, `connectTransport`, `transport_test.dart` and `web_socket_server_test.dart`, `test/integration/mux-{tcp,websocket}.test.ts`.

**Acceptance:** every Dart test under `test/mux`, `test/transport`, `test/identity` and `test/vectors` for these codecs has a TS counterpart or a documented reason; the interop thread's raw probe exchanges frames with a `FrameMuxConnection` over TCP and WebSocket; `npm run check` green.

## Thread 2: Port Talk (new thread)

Branch suggestion: `claude/sb-ts-talk`. Dart: `lib/src/talk/*` (8 100 lines).

**Owns**

- `src/core/talk/` except `contract.ts`: `talk-frame.ts` (header codec, item windows), `talk-channel.ts`, `talk-message.ts`, `talk-request.ts`, `talk-stream.ts`, `talk-bulk.ts` and `talk-bulk-transfer.ts` (`BulkRoutes`, the `_bulk` channel filter installed with `adoptIncoming` and `admitAfterGoAway`, send-side chunking, receive-side reassembly and streaming), `talk-forward.ts` (`forwardMessage`), `index.ts`.
- Tests: `test/vectors/talk.test.ts`, `test/unit/talk/**` (ports of `talk_channel_test.dart`, `talk_frame_test.dart`, `talk_bulk_*`, `talk_forward_test.dart`, `talk_timeout_*`, item window tests), `test/integration/talk-*.test.ts` over the memory pair once thread 1's step 3 lands.

**Deliverables, in order**

1. `talk-frame.ts` with every wiki and Dart vector (the frame codec is independent of the mux). **Done** (#1).
2. `talk-channel.ts`, `talk-message.ts`, `talk-request.ts`, `talk-stream.ts`: requests, replies, streams, aborts, timeouts and EXTEND, item windows and credits, `ordered` requests, over a `MuxChannel`. Until thread 1's step 3 lands, test over a memory `MuxChannel` pair under `test/support/talk/memory-mux.ts` that implements `mux/contract.ts` in memory (send window, credit on delivery, close with status); keep it, it is a useful unit fixture after too. **Done** (#4, #11).
3. Bulk transfers: `talk-bulk.ts`, `talk-bulk-transfer.ts`, the `_bulk` filter, `streamBulk`, budgets and timeouts. **Done** (#11).
4. `talk-forward.ts`, the forwarding tests. **Done** (#11), and the integration tests over the frame mux, TCP and the QUIC binding (`claude/talk-integration`, `test/integration/talk-*.test.ts`).

**Acceptance:** every Dart `test/talk/*` has a TS counterpart or a documented reason; the interop thread's scenarios (request, stream, abort, bulk over 32752 bytes, forward) pass TS to Dart and Dart to TS over TCP and WebSocket once the node lands.

## Thread 3: Port node and client (new thread)

Branch suggestion: `claude/sb-ts-node`. Dart: `lib/src/switchboard/{switchboard,channel_policy,forwarding,generic_status,identity_wait,incoming_channel,memory_endpoints,outgoing_policy,proxy,relay,relay_config,resolver,switchboard_binding,mesh,peer_set}.dart` (8 800 lines), `lib/src/client/*` (1 600), `lib/switchboard_io.dart`-style entry points.

**Owns**

- `src/core/switchboard/` except `contract.ts` (as landed, dispatch and the outgoing policy live in `switchboard.ts`): `switchboard.ts` (the runtime-neutral node over a `Runtime` of connectors and listener factories), `dispatch.ts`, `channel-policy.ts` (`ChannelPolicies`), `incoming-channel.ts`, `outgoing-policy.ts`, `resolver.ts` (`StaticResolver`, `EndpointResolver`, selection), `proxy.ts` (`pipeChannels`, `proxyHandler`), `relay.ts`, `relay-config.ts`, `identity-wait.ts`, `forwarding.ts`, `peer-set.ts`, `mesh-node.ts`, `index.ts`.
- `src/core/client/`: `backoff.ts`, `reconnecting-client.ts`, `persistent-channel.ts`, `index.ts`.
- `src/node/switchboard.ts` (the Node `Switchboard` with `listenTcp`, `listenWebSocket`, `listenMemory`, `accept`, `acceptWebSocket`, `connect` and `dial` for `tcp`, `ws`, `wss`, `mem`, and the `quic` and `https` schemes delegated to the QUIC thread's `connectQuic`/`listenQuic` through the `quic` option), `src/node/index.ts` exports for it, `src/web/switchboard.ts` (connect-only node for browsers), `src/web/index.ts` exports for it.
- Tests: `test/unit/switchboard/**`, `test/unit/client/**` (ports of `switchboard_test.dart`, `policy_test.dart`, `proxy_test.dart`, `relay_test.dart`, `reconnecting_client_test.dart`, `peer_set_test.dart`, `mesh_test.dart`), `test/integration/node-*.test.ts` over memory, TCP and WebSocket.

**Deliverables, in order**

1. `ChannelPolicies`, `IncomingChannel`, dispatch and registration over the memory transport (thread 1 step 1) with a stub Talk until thread 2 step 2 lands: `registerService`, `openChannelOn`, `accept`, `connections`, `close`. **Done** (#6; the Talk seam wired on `claude/node-talk`).
2. Resolvers, the connection pool, `connect`/`dial`, `openChannel`/`openTalk`/`openChannelAt`, identity on connections (`credential`, `identifyOn`, `expectedIdentityFor`, `credentialFor`), `localConnections` and `isOwnEndpoint`. **Done** (#6, #17); the node suites run over the toy mux and over the frame mux (`test/unit/switchboard/over-frame-mux/`).
3. `ReconnectingClient`, `Backoff`, persistent channels with `ManualClock`. **Done** (#10).
4. Proxy, relay, forwarding; `PeerSet`, `MeshNode`. **Done** (#16, `claude/node-talk`, `claude/mesh`; `MeshNode` lives in `src/core/switchboard/mesh-node.ts`), and `MeshNode`'s sharded half (`publishSharded`, `claimSlot`, `releaseSlot`, `gates`, the hand-over in `leave`) on `claude/sharding`.
5. The Node and web entry points with the listeners and schemes; slot-routed opens (`openChannelToSlot`, `selectAndConnect` with a `SlotResolver`) wired to thread 4's types. **Done**: the entry points on `claude/transports` (`src/node/switchboard.ts`, `src/web/switchboard.ts`); slot routing on `claude/slot-routing` (`openChannelToSlot`, `openTalkToSlot`, `resolveSlotOwner`, `StaticResolver` as a `SlotResolver`, the proxy's slot groups).

**Acceptance:** the Dart `test/switchboard/*`, `test/client/*` and the node-level `test/integration/*` have TS counterparts or a documented reason; the interop thread's Dart peer completes requests against a TS node and a TS node completes requests against the Dart peer over TCP and WebSocket.

## Thread 4: Port naming and sharding (new thread)

Branch suggestion: `claude/sb-ts-naming`. Dart: `lib/src/naming/*` (9 100 lines), `lib/src/switchboard/{slot_gate,slot_channel,slot_reopen,slot_key}.dart` (2 200).

**Owns**

- `src/core/naming/`: `naming-protocol.ts` (every `_ns` message codec incl. the slot messages), `naming-client.ts`, `naming-client-slots.ts`, `naming-service.ts`, `naming-service-slots.ts`, `naming-resolver.ts`, `slot-table.ts`, `slot-gate.ts`, `slot-channel.ts`, `slot-reopen.ts`, `slot-key.ts` (`fnv1a32`, `slotForKey`, `slotForText`), `index.ts`.
- Tests: `test/vectors/naming.test.ts`, `test/unit/naming/**` (ports of `naming_protocol_test.dart`, `naming_client_test.dart`, `naming_service_test.dart`, the slot tests, `slot_gate_test.dart`, `slot_channel_test.dart`), `test/integration/naming-*.test.ts` and `sharding-*.test.ts` over the memory transport with thread 3's node.

**Deliverables, in order**

1. `naming-protocol.ts` with vectors; `slot-key.ts`, `slot-table.ts`. **Done** (#3).
2. `NamingClient` and `NamingResolver` over a `TalkChannel` (thread 2 step 2; until then the memory channel fixture). **Done** (#13; `renew()` on `claude/identity`; the suites over real Talk on `claude/slot-routing`).
3. `NamingService` with leases, watches, CONNECT brokering. **Done** (#13).
4. Slot spaces: client and service halves, `SlotGate`, `SlotChannel`, reopen on MOVED, migration (ASSIGN, DRAIN, FORWARD, RESUME). **Done** (#13, `claude/slot-routing`: `SlotGate`, `SlotLifecycle`, `SlotGates` in `src/core/naming/slot-gate.ts`, `slot_gate_test.dart`, `slot_gate_credential_test.dart`, `slot_routing_test.dart`). The integration port of `test/integration/sharding/*` (the whole Dart sharding suite) is done on `claude/sharding`.

**Acceptance:** the Dart `test/naming/*` and slot tests have TS counterparts or a documented reason; a TS naming service serves the Dart peer's registrations and watches and the reverse, in the interop matrix.

## Thread 5: Port QUIC transport (existing thread)

Branch suggestion: `claude/sb-ts-quic`. Dart: `switchboard_quic/lib/src/binding/*`, `lib/src/provider/*`, `lib/src/native/*`, `lib/src/web/*`; Rust: `native/sbq`. Your recommendation in `notes/quic.md` section 12 is adopted: the provider contract keeps the Dart shape (pull-based reads, streams taken only while asked for), the addon mirrors the C ABI, `sbq` is vendored at a pinned commit with the `rlib` line (the user decided to duplicate the shim now; a dedicated shim repository shared by both packages may follow once the port is ready, so keep the copy cleanly separable: its own directory, no TS-specific edits to the Rust beyond the crate-type line, the N-API crate as a separate crate next to it), prebuilds follow `switchboard_quic`'s release targets, and the phases are the three you listed. You may reshape `src/core/quic/contract.ts` in your first PR (for instance `acceptStream()` instead of the async iterable) with a short rationale; the lead merges it first.

**Owns**

- `src/core/quic/` except `contract.ts`: `records.ts` (record header codec and reader), `hello.ts`, `quic-binding-connection.ts` (extends thread 1's `BindingConnection`; until it lands, extend a local stub that implements the identity parts you need and swap when it does), `quic-binding-channel.ts`, `quic-dial.ts` (`connectQuic`), `quic-listen.ts` (`listenQuic` producing `MuxConnection`s for the node), `quic-options.ts`, `quic-identity-rules.ts`, `exporter.ts`, `index.ts`.
- `src/web/quic/webtransport-provider.ts` (`webTransport()` returning a `QuicConnector` provider; `quicConnector(url, provider, init)` in `quic-dial.ts` gives a client its `ConnectionConnector`, and `webTransportConnector(url, init)` the same over `webTransport()`), `src/web/quic/index.ts`.
- `src/node/quic/native-provider.ts` (`nativeQuic()`, lazy `createRequire` load of the addon, `selfSignedCertificate`), `src/node/quic/index.ts`.
- `native/`: the N-API crate (`napi-rs`), `Cargo.toml`, `build.rs` as needed, `package.json` scripts for `napi build`, the per-platform package manifests under `native/npm/<target>/`, `native/README.md` (how to build locally, supported targets, how `sbq` is reused). Reuse `switchboard_quic/native/sbq` as a vendored copy under `native/sbq-vendor/` (the user's decision: duplicate now, possibly a shared shim repository later, so the copy must stay separable): `tool/sync-sbq.mjs` copies it from a `switchboard_quic` checkout at the commit recorded in `native/sbq-vendor.commit` (outside the copy, so the copy stays byte-identical to upstream apart from the crate-type line), adding `"rlib"` to the crate types, and a CI check verifies the copy equals upstream except for that line. The exported JS surface of the addon is yours; keep it minimal (sessions, streams, events through a thread-safe function) and keep all protocol logic in TypeScript.
- `src/testing/memory-quic-provider.ts` (the in-memory provider: independent stream delivery with configurable delays, flow control, a stream limit, resets, an exporter) and its export from `src/testing/index.ts`.
- Tests: `test/vectors/quic-records.test.ts`, `test/unit/quic/**` (ports of the `switchboard_quic` binding tests over the in-memory provider), `test/integration/quic-native.test.ts` (loopback over the addon, skipped with a message when the addon is not built), `test/browser/webtransport.test.ts` (Chromium via Playwright against a native listener; `playwright` as a dev dependency, Chromium from `PLAYWRIGHT_BROWSERS_PATH`), the `native` and `browser` CI jobs in `.github/workflows/ci.yml` (add jobs; do not edit the existing ones).

**Deliverables, in order**

1. `records.ts`, `hello.ts`, vectors; the in-memory provider. **Done** (#2).
2. The binding over the in-memory provider: `QuicBindingConnection`, `QuicBindingChannel`, control stream, HELLO, GOAWAY and drain holds, exporter-bound identity, the Dart binding test ports. **Done** (#8).
3. The native provider and addon for Linux x64 (CI), then macOS and Windows targets; `connectQuic` and `listenQuic`; the loopback integration test; the `native` CI job building the addon with cargo and running the integration test. **Done** (#8, #15; node-level native tests on `claude/quic-identity`). CI builds Linux x64; the other targets are built by the release workflow.
4. The WebTransport provider and the browser test; the `browser` CI job. **Done** (`claude/webtransport`: 34 browser tests in Chromium 141, the `browser` job, the man-in-the-middle group).
5. Prebuilt packaging: the per-target optional dependencies and a `release` workflow building them (a draft is enough; publishing is the user's call). **Done** (`claude/quic-identity`: `native/npm/<target>/`, `optionalDependencies`, `.github/workflows/release.yml`). Musl prebuilds are open.

**Acceptance:** every `switchboard_quic` test has a TS counterpart over the in-memory provider or a documented reason; a TS node over the native provider interoperates with the Dart peer over raw QUIC and WebTransport in both directions; Chromium reaches a TS WebTransport listener.

## Thread 6: Dart interop test harness (existing thread)

Branch suggestion: `claude/sb-ts-interop`. Your proposal in `notes/interop.md` is adopted as written, with these answers: Vitest; `test/interop/` and `test/vectors/` as proposed; QUIC on Node is the native N-API provider (thread 5), Bun runs the Node entry, Deno is manual for now; order as you proposed (codec differential first, scenarios as the node lands, meshes last).

**Owns**

- `test/interop/**`: `dart_peer/` (the AOT peer, `pubspec.yaml`, `bin/dart_peer.dart`, `dart_commits.txt`, build script), `ts_peer/` (the TS peer over the public API only), `launcher.ts` (spawns peers, `READY` handshake, JSON lines), `matrix.ts` (the four pairings, transport cells, skip reasons), `scenarios/*.test.ts`, `oracle.test.ts` (codec differential over `wiki-vectors.json` and a fuzz pass), `probe/` (raw byte conformance scripts), `README.md`.
- `test/vectors/wiki-vectors.json` and `tool/extract-wiki-vectors.mjs`, plus `test/vectors/coverage.test.ts` that fails on a vector id no test asserts or asserts that is unknown (threads 1, 2, 4 and 5 register the ids they assert through a small helper you provide under `test/support/vectors.ts`).
- `tool/dart-peer.mjs` (build, locate or download the Dart SDK locally; used by `npm run test:interop` and CI) and the `interop` job in `.github/workflows/ci.yml` (add the job; do not edit the existing ones).
- Documentation: `docs/wiki/Testing-and-Interop.md` (the lead drafted it; you own it from your first PR).

**Deliverables, in order**

1. The Dart peer moved from the prototype into `test/interop/dart_peer/` with `dart_commits.txt`, the launcher, `tool/dart-peer.mjs`, the vectors JSON and extractor, the coverage test and the `test/support/vectors.ts` helper, the `interop` CI job (skips cleanly when Dart cannot be installed). The oracle test over the existing codecs (status, name, address) and the raw probe scripts as tests. **Done** (#7, #12).
2. The TS peer and the scenario matrix for the mux and Talk scenarios (request, stream, abort, extend, bulk, close, GOAWAY, protocol-error injection), running as cells become available. **Done** (`claude/ts-peer`: the TS peer, every cell of the matrix over TCP, WebSocket, raw QUIC and WebTransport), as are 3 and 4 (the identity, naming, sharding, proxy, relay and forwarding scenarios, the chains, the oracle for every codec, the Dart driver).
3. Identity scenarios (NONCE/IDENT both directions, holder key and bearer, exporter-bound over QUIC), naming and sharding scenarios, relay and proxy, mixed-language meshes.
4. A Dart driver that spawns the TS peer, so the Dart repositories' own interop scenarios can run against TS later (a tool under `test/interop/dart_driver/`, not a change to the Dart repositories).

**Acceptance:** the `interop` job is green with the Dart to Dart cells on day one and grows a cell as each thread lands; every skipped cell names the missing feature.

## Lead: integration

- Owns `docs/`, every `contract.ts`, the barrels outside thread directories, `package.json`, the configs, the `lint`, `typecheck` and `test` CI jobs, `src/core/{bytes,name,status}.ts`, `src/core/address/`, `src/core/util/`, `src/testing/manual-clock.ts`.
- Reviews and merges thread PRs into `claude/switchboard-ts-design-s5r9na` in dependency order; runs `npm run check` and `npm run test:interop` on the merged result; resolves conflicts; keeps the design, the work split and the wiki pages current; raises contract changes to the threads they affect.
- Merge order expected: 1.1, 1.2, 2.1, 5.1, 6.1 in the first days; then 1.3, 2.2, 3.1, 4.1, 5.2; then the rest as they come.
- `main`: initialised from the integration branch at the first hand-off; the integration branch since.

## Dependency graph

```
thread 1 step 1 (framing, memory pair) ──► thread 3 step 1, thread 2 integration tests
thread 1 step 2 (codecs, BindingConnection) ──► thread 5 step 2
thread 1 step 3 (frame mux) ──► thread 2 step 2 on the real mux, thread 3 step 2, thread 6 scenarios
thread 2 step 2 (Talk) ──► thread 3 openTalk, thread 4 step 2, thread 6 Talk scenarios
thread 3 step 2 (node) ──► thread 4 integration, thread 5 node wiring, thread 6 node scenarios
thread 5 step 3 (native) ──► thread 6 QUIC cells
```

Nothing above waits on anything to start: each thread begins with its codecs and vectors, which depend only on what is already in the repository.
