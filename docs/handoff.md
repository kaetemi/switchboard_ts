# Hand-off: the Switchboard TypeScript port

_Written by the lead session on 2026-10-14 for the session that continues the port. Everything below is the state at hand-off; the per-thread hand-off notes under `/mnt/project-files/switchboard-ts/handoff-notes/` and the running notes under `/mnt/project-files/switchboard-ts/notes/` carry the detail. The design is `docs/design.md`; the original split of work is `docs/work-split.md`; the user-facing pages are `docs/wiki/`._

## 1. Where things are

- **Repository:** `kaetemi/switchboard_ts`, package `@kaetemi/switchboard` 3.0.0-dev.1, ESM only, TypeScript 7 strict, Vitest 5, Biome 2, Node 22 or later, Bun and Deno for the Node entry.
- **Branches:** `main` is initialised from the integration branch `claude/switchboard-ts-design-s5r9na` at hand-off; both point at the same commit (see section 7 for the hashes). The six thread branches hold nothing the integration branch lacks.
- **Reference implementation:** `kaetemi/switchboard` (Dart core) and `kaetemi/switchboard_quic` (Dart QUIC binding and the Rust `sbq` shim), read only. The wiki is `kaetemi/sway` under `wiki/switchboard/`. The TS test peer pins the Dart repositories in `test/interop/dart_peer/dart_commits.txt` (core `3816e6f`, QUIC `c5b3265`); the vendored shim pin is `native/sbq-vendor.commit` (`c5b3265`).
- **Shared folder:** `/mnt/project-files/switchboard-ts/` holds copies of `design.md`, `work-split.md`, the wiki pages, this document, `notes/` (one file per thread plus `lead.md`) and `handoff-notes/`.

## 2. What is merged

Every pull request went in with a merge commit, in dependency order, after the lead ran `biome check`, `tsc -p tsconfig.json`, `tsc -p tsconfig.test.json` and the unit and integration projects on a test-merge, and CI was green on the head.

| PR | Thread | Content | Merge |
|---|---|---|---|
| #1 | Talk | Talk frame codec with the wiki and Dart vectors | 04362ea |
| #2 | QUIC | QUIC records, HELLO, exporter helpers, the fake QUIC provider (`@kaetemi/switchboard/testing`) | e7c04da |
| #3 | Naming | Naming and sharding codecs, slot keys, slot tables | 47fb444 |
| #4 | Talk | Talk contract: item windows, the credit bound, slot carriers | f378d19 |
| #5 | Core | Stream framing, stream transport, memory pair, endpoints | f0e7263 |
| #6 | Node | The core Switchboard node: dispatch, pool, identity, brokering | 0c88ec6 |
| #7 | Interop | Dart interop harness: Dart test peer, wiki vectors JSON, oracle, raw probes, `interop` CI job | ecf4f54 |
| #9 | Core | Mux frame and control codecs, IDENT verification, Ed25519, the `BindingConnection` base | 1ba6818 |
| #10 | Node | `ReconnectingClient`, backoff, persistent channels | 0a96930 |
| #8 | QUIC | QUIC binding (`QuicBindingConnection`, `QuicBindingChannel`, `connectQuic`, `listenQuic`), the N-API addon over the vendored `sbq`, `nativeQuic()`, the `native` CI job | 63aeabb |
| #12 | Interop | Oracle: TS mux frame and control codecs against the Dart reference | 034ab8b |
| #11 | Talk | `TalkChannel` runtime: requests, streams, item windows, bulk transfers, `forwardMessage` (328 tests over a fake mux pair) | cd08e02 |
| #13 | Naming | `NamingClient`, `NamingResolver`, `NamingService` with slot spaces, `SlotChannel`, `reopenAtSlotOwner`, the `SlotResolver` contract | dd8c205 |
| #14 | Core | `FrameMuxConnection`, `FrameMuxChannel`, the output scheduler (4 smoke tests; the Dart test ports are the first remaining item) | 1ba6e94 |
| #15 | QUIC | `QuicSwitchboardBinding`, `QuicSwitchboardListener`, `quicConnector`, the WebTransport API surface (not exported) | 6cf6beb |
| #16 | Node | `pipeChannels`, `proxyHandler`, `RelayService`, forwarding, `PeerSet` | 1a608ed |
| #17 | Node | The core runtime's nodes run over `FrameMuxConnection` | 7975285 |
| #18 | Node | Branch sync after #17, no file changes | 0c0f92e |

Lead commits between them: the contract placements (`BrokeringResolver` in the switchboard contract, `ServiceAddressLike` in the address layer, injectable `Clock` and `Logger` in every options object), the package rename, the design and work-split updates, and the Biome rule change (`useLiteralKeys` off, because the TS config requires bracket access to index-signature keys).

What this gives you, by layer (file paths under `src/`):

- `core/bytes.ts`, `core/name.ts`, `core/status.ts`, `core/address/`, `core/util/` (clock, logger, emitter, deferred, async queue): the lead's foundations.
- `core/transport/`: `StreamFraming`, `StreamFrameDecoder`, the Web Streams encoder and decoder, `streamTransport` over byte I/O, the memory pair and `MemoryEndpoints`, `parseEndpoint`.
- `core/mux/`: the contract, `MuxFrame`, `MuxControlMessage`, `MuxLimits`, `MuxCredit`, `MuxIdent`, `verifyIdent`, the CLOSE and GOAWAY status rules, `BindingConnection`. **No `FrameMuxConnection` yet** (section 4).
- `core/identity/`: the contract, Ed25519 over WebCrypto, `secureRandomBytes`, `PeerIdentity`. **No credential codec, verifier or issuer yet.**
- `core/talk/`: the contract, `TalkFrame`, `TalkBulkOpen`, `TalkBulkReference`, and the Talk runtime from PR #11 (section 3 says how far its tests got).
- `core/naming/`: the `_ns` codecs, slot keys and tables, and the naming client, resolver and service from the naming PR (section 3).
- `core/switchboard/`: the runtime-neutral `Switchboard` node (dispatch, pool, policies, identity waits, brokering resolvers, proxy, relay, `PeerSet`), whose core runtime opens `FrameMuxConnection`s since #17; its Talk seam (`talk-layer.ts`) still throws `UNIMPLEMENTED` and is the first thing to wire (section 4).
- `core/client/`: `ReconnectingClient`, `PersistentChannel`, `PersistentTalk`, `Backoff` (internal).
- `core/quic/`: the provider contract, records, HELLO, exporter helpers, the binding, `QuicIdentityRules`, `connectQuic`, `listenQuic`.
- `node/quic/`: `nativeQuic()`, the lazily loaded N-API addon (`$SWITCHBOARD_QUIC_ADDON`, then the local build, then `@kaetemi/switchboard-quic-<target>`), `selfSignedCertificate`.
- `testing/`: `ManualClock`, the fake QUIC provider.
- `native/`: the N-API crate (`src/lib.rs`, `Cargo.toml`, `build.mjs`), `sbq-vendor/` (the shim at the pin with `"rlib"` added), `sbq-vendor.commit`, `tool/sync-sbq.mjs` (`--check` verifies the copy against a `switchboard_quic` checkout).
- `test/`: `unit/`, `integration/`, `vectors/` (wiki vectors JSON, coverage test, per-layer vector tests), `interop/` (Dart peer, launcher, matrix, scenarios, oracle, probes), `support/` (fixtures the threads share: toy mux, fake Talk pair, fake mux pair, stepping and fake clocks, identity stubs).

## 3. What landed at the end, with gaps marked

The user asked for everything to be merged and pushed at hand-off, so the last four PRs went in with their gaps marked rather than staying on thread branches. What they leave open, from their PR bodies and the threads' notes:

- **Frame mux (#14).** The implementation is complete and exported; only four smoke tests cover it. Keep-alive, the LIMITS wait, the output scheduler and bulk zipper, the open-payload budget and pending-rejection cap, the receive high-water pause and the IDENT exchange were compared with Dart by reading only. Porting `mux_test`, `mux_flow_test`, `mux_schedule_test`, the connection groups of `mux_limits_test`, the connection group of `mux_vectors_test` and the IDENT gating tests of `binding_protocol_test` (with `mux_harness.dart` as `test/unit/mux/support.ts`) is the first job, and it will find bugs.
- **Talk (#11).** Complete against the Dart tests except the groups its tests mark `// Not ported` (the bulk "over TCP" variant needs the frame mux over TCP; the forwarding "synchronous transport" variants have no TS counterpart; `onError(null)`). Its tests run over `test/support/talk/fake-channel.ts`; the integration tests over the real frame mux and over `QuicBindingConnection` are not written. A known gap in bulk admission: a frame delivered to Talk's reader but not yet processed is not counted, so a peer exactly at `maxUnclaimedBulk` can get a spurious RESOURCE_EXHAUSTED.
- **Naming (#13).** `SlotGate` and `SlotLifecycle` are not ported (they need the real Talk channel and the node's proxy forwarding); `NamingClient.renew()` is unimplemented (needs `Credential.decode`); the node-side identity and brokering tests are marked `// Not ported` for the node; the routing group of `slot-routing.test.ts` is `test.todo` until the node routes by slot; the tests run over `test/support/naming/fake-talk.ts`, which has no item windows; nothing is checked against the Dart naming service yet.
- **Node (#6, #10, #16, #17).** The core runtime opens `FrameMuxConnection`s (#17, one smoke test); the node test suites still run over the toy mux (`test/support/node/toy-mux.ts`) and should run over the frame mux too, which enables the todos needing real LIMITS and flow control; `talk-layer.ts` still throws `UNIMPLEMENTED` for `talkOver` and must import `TalkChannel` and `installBulkRouting` from `src/core/talk`; `slot-layer.ts` returns no carrier, so the proxy's slot-routing groups (about 20 Dart tests) are unported; `MeshNode` is not started; the Talk and bulk cases of the proxy and relay tests are `it.todo` (16 and 2). `src/node/switchboard.ts` and `src/web/switchboard.ts` (listeners and schemes) do not exist yet.
- **QUIC (#8, #15).** Node integration is in (`QuicSwitchboardBinding`, `QuicSwitchboardListener`, `quicConnector`); its node-level tests still have the Talk bulk case, the two proxy cases, `SlotChannel` at record cost and the man-in-the-middle group as todo. The browser WebTransport provider exists only as the API surface (`src/web/quic/webtransport-api.ts`, unexported); the provider, its Playwright test and the `browser` CI job remain. Prebuilt packages and the release workflow are not started; only Linux x64 has been built. A raw QUIC listener needs `alpn: [switchboardAlpn]` in its config and nothing fills it in for the caller yet.
- **Interop (#7, #12).** No TS peer yet, so every TS cell of the matrix skips with the reason in its name; the oracle covers names, status, addresses, mux frames and control messages.
- **Identity.** No credential codec, verifier or issuer; every layer that needs one uses a stub (`test/support/identity/stub-verifier.ts`, `test/support/node/fake-identity.ts`, `test/unit/quic/identity-fixtures.ts` with real Ed25519 holder keys).

## 4. Remaining work, by area

The work split (`docs/work-split.md`) lists each thread's deliverables; this is what is left of them. A sensible order for the first days: (1) wire the node to what has landed, that is `talk-layer.ts` to `TalkChannel` and `installBulkRouting` (the mux factory already opens `FrameMuxConnection`s since #17), which flips dozens of `todo` tests across the node, proxy, relay, PeerSet and QUIC suites; (2) port the Dart frame mux tests and fix what they find; (3) the TS interop peer, so the Dart matrix starts exercising the port over TCP and WebSocket; (4) identity codecs; (5) the Node and web entry points with the TCP and WebSocket transports; then the rest below.

**Core (thread 1)**
1. `frame-mux-connection.ts`, `frame-mux-channel.ts`, `output-scheduler.ts`: the full `MuxConnection` and `MuxChannel` over a `FrameTransport`, with the identity exchange (held frames while verifying), keep-alive, GOAWAY, credit, limits, the output scheduler. Port `mux_connection_test.dart`, `mux_channel_test.dart`, the `frame_mux_*` tests. This unblocks the node's mux factory (`core-runtime.ts`), the Talk integration tests over the memory pair, the TS interop peer and the naming integration tests.
2. Identity: `credential.ts` (codec with vectors), `scope.ts`, `holder-key.ts`, `credential-verifier.ts` (HMAC-SHA256 and Ed25519), `credential-issuer.ts`; replace `test/support/identity/stub-verifier.ts` and the node's `fake-identity.ts` where they stand in; `NamingClient.renew()` needs `Credential.decode`.
3. `websocket-transport.ts` (global `WebSocket`), `src/node/transport/` (`connectTcp`, `listenTcp`, `ws` server and client, `connectTransport`), the loopback integration tests, the Dart `web_socket_*` and `stream_transport_*` test ports.

**Talk (thread 2)**
- Whatever PR #11's description and `handoff-notes/talk.md` mark as not yet ported (ordered answers, bulk transfers, forwarding tests were the last groups); the integration tests over the real frame mux once it lands; `ManualClock.elapseAsync` so `FakeClock`, `SteppingClock` and `TestClock` can merge.

**Node and client (thread 3)**
- Deliverable 4: proxy (`pipeChannels`, `proxyHandler`), relay, forwarding, `PeerSet`, `MeshNode`, with `switchboard_test.dart`'s proxy and relay groups, `peer_set_test.dart`, `mesh_test.dart`.
- Deliverable 5: `src/node/switchboard.ts` (listeners for `tcp`, `ws`, `wss`, `mem`; `quic` and `https` delegated to `connectQuic` and `listenQuic` through the `quic` option), `src/web/switchboard.ts`, the entry points' exports, slot-routed opens (`openChannelToSlot`, `selectAndConnect` with a `SlotResolver`) using the naming layer's `reopenAtSlotOwner`.
- Run the node suites over `FrameMuxConnection` as well as the toy mux (`core-runtime.ts` already opens the frame mux since #17).

**Naming and sharding (thread 4)**
- `SlotGate` and the proxy half of slot routing (need the real Talk channel and the node's proxy forwarding); the integration tests over the memory transport; the `test.todo` routing group in `slot-routing.test.ts` once the node routes by slot; `NamingClient.renew()`.

**QUIC (thread 5)**
- Node integration: Dart's `QuicSwitchboardBinding` and `QuicSwitchboardListener` over the node's `registerBinding`, `adopt` and identifying in return (touches the node thread's files; coordinate on `src/node/switchboard.ts`).
- The WebTransport provider for browsers (`src/web/quic/webtransport-provider.ts`, `webTransport()`), the Playwright Chromium test and the `browser` CI job.
- Dart interop over raw QUIC and WebTransport once the TS peer exists.
- Prebuilt packaging: `@kaetemi/switchboard-quic-<target>` optional dependencies for the `switchboard_quic` release targets (Linux x64 and arm64 glibc, macOS arm64 and x64, Windows x64) and a `release` workflow; publishing is the user's call.

**Interop (thread 6)**
- The TS peer (`test/interop/ts_peer/main.ts`, public API only, same JSON-lines protocol as `test/interop/README.md`), then the mux and Talk scenario matrix; clear `tsTransportGaps` in `matrix.ts` as transports become servable.
- More oracle kinds (talk frame, stream framing now; naming, sharding, credential and QUIC records after the Dart peer's oracle grows).
- Identity, naming, relay and proxy scenarios; mixed-language meshes; the Dart driver under `test/interop/dart_driver/`.
- Register the inline-hex vector tests through `test/support/vectors.ts` so the coverage test's `todo` tables go live (`SWITCHBOARD_VECTORS_STRICT=1` makes every table required).

**Lead**
- Keep `docs/design.md`, `docs/work-split.md` and `docs/wiki/` in step; the GitHub wiki repository could not be pushed from the cloud environment, so the pages stay under `docs/wiki/` until someone pushes them.
- Deno in CI once the suite passes there by hand; `CHANGELOG.md` before any publish.

## 5. Decisions already taken

- **Package name** `@kaetemi/switchboard` (the user's npm scope); prebuilt addons as `@kaetemi/switchboard-quic-<target>`.
- **QUIC provision:** an N-API addon over a vendored copy of `switchboard_quic/native/sbq` with one change (`"rlib"` in the crate types). No upstream change. The copy stays byte-identical to upstream otherwise and separable; a dedicated shim repository shared by the Dart and TS packages is the intended later direction. The pin lives outside the copy in `native/sbq-vendor.commit`.
- **Binding base keeps Dart's scope:** `BindingConnection` has drain holds and neutral diagnostics only; the identity exchange, `adoptIncoming` and `admitAfterGoAway` are each binding's own.
- **Idioms** (design section 5): `Uint8Array` bytes, numbers for u48 ids, ms durations, const objects not enums, pull-based `AsyncIterable` delivery (credit returned on delivery) plus Web Streams, typed `Emitter`, `AbortSignal`, `Symbol.asyncDispose`, injectable `Clock` and `Logger` in every options object, WebCrypto, `SwitchboardError` and `ProtocolError` (only PROTOCOL_ERROR is a `ProtocolError`; FRAME_TOO_LARGE is a `SwitchboardError`), `RangeError`/`TypeError` for Dart's `ArgumentError`, plain `Error` for `StateError`.
- **`_bulk` routing** is installed by the Talk layer through the mux contract's `adoptIncoming(filter)` and `admitAfterGoAway(predicate)`; raw `incoming` consumers never see `_bulk` channels.
- **`ReconnectingClient`** takes a `TransportConnector` function or a `ConnectionConnector` object `{ connect(options, { signal }) }`; QUIC and WebTransport plug in through the latter.
- **`verifyIdent` takes wall time** (`now`, ms since the epoch) because the injected `Clock` is monotonic.
- **Contract files** (`src/core/*/contract.ts`) and the barrels outside a thread's directories are the lead's; contract changes go in their own commit with a rationale.

Open: the GitHub wiki push; Deno in CI; whether a web entry-point helper should return a `ConnectionConnector` for WebTransport; `isSlotResolver` may move from `naming/slot-reopen.ts` next to the switchboard contract when the node wires slot routing.

## 6. Conventions

- **Merges:** real merge commits only (`merge_method: merge`), never squash or rebase; keep branch history granular; after a PR merges, continue the branch from the integration branch with a merge, not a rebase.
- **Commits:** one per milestone, descriptive subject, `Co-Authored-By: Claude <noreply@anthropic.com>` and a `Claude-Session:` line; no model identifiers in commits, PR text or code comments; operational security in commit messages.
- **PR bodies:** Before, After, How, notes for the lead; the project attribution footer.
- **Before every push:** `npm run check` (lint, typecheck, unit and integration) and `npx tsc -p tsconfig.test.json --noEmit`; CI does not run the second one, the lead did.
- **Style:** Biome (single quotes, 100 columns), `.ts` import extensions, `type` imports, no `any`, no TS `enum`, file header comment as in the sources, JSDoc on every export, `process.env['X']` (index signatures need bracket access).
- **Ownership:** as in `docs/work-split.md`; shared helpers under `test/support/<thread>/`.
- **Dart repositories are read-only.** Findings about them go in PR descriptions and notes.
- **Wiki pages** live in `docs/wiki/` in wiki-ready Markdown.

## 7. How to run everything

```sh
npm ci
npm run check                 # biome, tsc (src), unit + integration projects
npx tsc -p tsconfig.test.json --noEmit    # type-checks test/** too
npm run build                 # dist/ for the four entry points
```

**Dart interop** (`npm run test:interop`): needs a Dart SDK 3.13.5 (`dart` on PATH, or `SWITCHBOARD_DART_SDK`, or `node tool/dart-peer.mjs build --download-sdk`), a Rust toolchain (the Dart QUIC package's build hook runs cargo), and the two Dart repositories as sibling checkouts of this one, or `SWITCHBOARD_DART_CORE` and `SWITCHBOARD_DART_QUIC`, or `node tool/dart-peer.mjs fetch` (clones the pins into `test/interop/dart_peer/.deps/`). The global setup builds the AOT peer once and caches it on a stamp; `node tool/dart-peer.mjs status` says what it would use. Without Dart the interop files skip with one line saying why; `SWITCHBOARD_INTEROP=required` turns that into a failure (CI), `SWITCHBOARD_INTEROP=skip` skips without looking, `SWITCHBOARD_DART_PEER=<exe>` uses a prebuilt peer, `SWITCHBOARD_FUZZ_SEED` and `SWITCHBOARD_FUZZ_COUNT` replay or widen the oracle. In the cloud container used for this work Dart was at `/opt/dart-sdk/bin` and the Dart repositories were sibling checkouts in `/home/user`.

**Native QUIC:** `npm run build:native` builds the addon with cargo (Rust 1.99.0 in CI) into `native/`; `npx vitest run --project integration test/integration/quic-native.test.ts test/integration/quic-binding-native.test.ts` runs the provider and the binding end to end over the loopback. Without the addon those files skip with a reason; `SWITCHBOARD_NATIVE=required` fails instead (CI). `node tool/sync-sbq.mjs --check <switchboard_quic checkout>` verifies the vendored shim against the pin.

**CI** (`.github/workflows/ci.yml`): lint and typecheck, Node 22, 24 and 26, Bun, build, `interop` (Dart peer, required mode), `native` (addon build and loopback tests). The `browser` job is still to be added by the QUIC thread.

**State at hand-off:** the last content merge is 7975285 (PR #17; PR #18, 0c0f92e, is a branch sync with no file changes); `main` and `claude/switchboard-ts-design-s5r9na` point at the same head, the commit that last touched this document. On it, `npm run check` and the test typecheck are clean and `npm test` reports 1517 passed, 125 skipped (the native QUIC files without the addon, 9 naming cases with their reasons) and 50 todo; the interop project passes 241 Dart cells in CI and skips every TS cell. No pull request is open and no thread branch holds a commit the integration branch lacks.
