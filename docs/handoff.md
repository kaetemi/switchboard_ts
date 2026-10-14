# Hand-off: the Switchboard TypeScript port

_The state of the port on 2026-10-14, after the second lead session's merges, for a session starting fresh. The design is `docs/design.md` (with the decisions in section 11, the deviations from Dart in section 12 and the open questions in section 13); the original assignment is `docs/work-split.md`, marked with what is done; the user guide is `docs/wiki/`; the per-thread detail is in `docs/handoff-notes/` and the running notes in `docs/notes/`._

## 1. Where things are

- **Repository:** `kaetemi/switchboard_ts`, package `@kaetemi/switchboard` 3.0.0-dev.1 (not published), ESM only, TypeScript 7 strict, Vitest 5, Biome 2; Node 22 or later, Bun and Deno for the Node entry, evergreen browsers for the web entry. Entries `.` (`src/core`), `./node` (`src/node`), `./web` (`src/web`), `./testing` (`src/testing`).
- **Branches:** `main` is the integration branch. Work happens on topic branches `claude/<topic>`, each in its own git worktree under `/home/user/wt/<topic>` (with `node_modules` a symlink to the main checkout's), merged into `main` by the lead with merge commits. The main checkout is `/home/user/switchboard_ts`. The first phase's integration branch `claude/switchboard-ts-design-s5r9na` and its thread branches hold nothing `main` lacks.
- **Reference implementation:** `kaetemi/switchboard` (Dart core) and `kaetemi/switchboard_quic` (Dart QUIC binding and the Rust `sbq` shim), read only, checked out as siblings in `/home/user/switchboard` and `/home/user/switchboard_quic`; `switchboard/doc/dart-design.md` records every layer's contract. The protocol wiki is `kaetemi/sway` under `wiki/switchboard/` (private; its vectors are committed as `test/vectors/wiki-vectors.json`). Pins: `test/interop/dart_peer/dart_commits.txt` (core `3816e6f`, QUIC `c5b3265`) and `native/sbq-vendor.commit` (`c5b3265`).
- **The container used for this work:** Node 22, Bun (`/root/.bun/bin/bun`), Rust and cargo (`/root/.cargo/bin`), the Dart SDK 3.13.5 at `/root/.cache/switchboard_ts/dart-sdk-3.13.5-linux-x64/dart-sdk/bin`, Playwright 1.56.1 with Chromium 141 in `/opt/pw-browsers` (`PLAYWRIGHT_BROWSERS_PATH=/opt/pw-browsers`, `PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1`; never `playwright install` there). The native addon is built in the main checkout (`native/sbq-node.node`; point a worktree at it with `SWITCHBOARD_QUIC_ADDON`) and the Dart peer too (`test/interop/dart_peer/build/peer/bundle/bin/dart_peer`; `SWITCHBOARD_DART_PEER`). Deno 2.9 was run from an npm-installed copy (`npm install deno` in a scratch directory). No GitHub CLI; pushing works through the configured remote.
- **Docs:** `docs/design.md`, `docs/work-split.md`, this document, `docs/wiki/*.md` (wiki-ready pages: Home, Getting-Started, API-Overview, Channels-and-Talk, Identity, Naming-and-Sharding, Proxy-and-Relay, Clients, Runtimes-and-QUIC, Testing-and-Interop, Contributing, `_Sidebar`), `docs/handoff-notes/<thread>.md`, `docs/notes/<thread>.md` (with `interop-dart-suites.md`, the Dart test inventory), `native/README.md`, `test/interop/README.md` (the peer protocol), `CHANGELOG.md`.

## 2. What is complete, by layer

Every layer is ported and exported. The Dart behaviour is the contract and the Dart tests the checklist; each TS test file is named after the Dart file it ports, and a Dart test that is not ported carries `// Not ported: <reason>` or a `test.todo` with the reason.

| Layer | Source | Dart suites covered (TS files) |
| --- | --- | --- |
| Primitives, addressing | `core/bytes.ts`, `name.ts`, `status.ts`, `address/` | the vectors (`test/vectors/{names,status,address}.test.ts`) |
| Transports | `core/transport/` (stream framing and transport, memory pair, `MemoryEndpoints`, `parseEndpoint`, WebSocket client), `node/transport/` (TCP, `ws` server and client, `connectTransport`) | `transport_test`, `transport_vectors_test`, `memory_endpoint_test`, `web_socket_server_test` (`test/unit/transport/`, `test/vectors/transport.test.ts`), `mux_test`'s TCP and WebSocket groups (`test/integration/mux-{tcp,websocket}.test.ts`) |
| Frame mux | `core/mux/` (`FrameMuxConnection`, `FrameMuxChannel`, the output scheduler, the codecs, `verifyIdent`, `BindingConnection`) | `mux_test`, `mux_flow_test`, `mux_limits_test`, `mux_schedule_test`, `binding_drain_test`, `binding_protocol_test`, `mux_vectors_test` (`test/unit/mux/`, `test/vectors/{mux,binding-protocol}.test.ts`; harness `test/unit/mux/support.ts`, the port of `mux_harness.dart`) |
| Identity | `core/identity/` (`Credential`, `Scope`, `CredentialIssuer`, `CredentialVerifier`, `Ed25519HolderKey`, `PeerIdentity`) | `identity_test`, `identity_vectors_test`, `exporter_identity_vectors_test` (`test/unit/identity/`, `test/vectors/{credential,identity,exporter-identity}.test.ts`), `identity_node_test` (`test/unit/switchboard/identity-node.test.ts`, `test/integration/identity.test.ts` over TCP and QUIC) |
| Talk | `core/talk/` | `talk_test`, `talk_window_test`, `talk_credit_test`, `talk_bulk_test`, `talk_forward_test`, `mux_talk_test`, `talk_vectors_test` (`test/unit/talk/` over the fake mux pair; `test/integration/talk-*.test.ts` over the frame mux, TCP and the QUIC binding) |
| Node | `core/switchboard/` (the core `Switchboard`, policies, resolvers, proxy, relay, `PeerSet`, `MeshNode`), `node/switchboard.ts`, `web/switchboard.ts` | `switchboard_test`, `selection_test`, `resolver_test`, `local_connections_test`, `memory_endpoint_test`, `proxy_test`, `relay_test`, `peer_set_test`, the node half of `naming_identity_test` (`test/unit/switchboard/`, each also under `over-frame-mux/`), and the node-level `test/integration` suites: `mesh_test`, `embedded_test`, `brokered_workers_test`, `cohosted_mesh_test`, `fleet_consumer_test`, `relayed_workers_test`, `naming_mesh_test` (`test/integration/node-*.test.ts` over TCP, WebSocket, QUIC and WebTransport) |
| Clients | `core/client/` (`ReconnectingClient`, `PersistentChannel`, `PersistentTalk`, `Backoff`) | `reconnecting_client_test` (`test/unit/client/`, also over the frame mux) |
| Naming and sharding | `core/naming/` (codecs, `NamingClient`, `NamingService`, `NamingResolver`, slot keys and tables, `SlotChannel`, `reopenAtSlotOwner`, `SlotGate`, `SlotLifecycle`, `SlotGates`) and `MeshNode`'s sharded half | `naming_test`, `naming_protocol_test`, `naming_resolver_test`, `naming_identity_test`, `naming_bulk_test`, `sharding_test`, `sharding_client_test`, `slot_gate_test`, `slot_gate_credential_test`, `slot_routing_test`, the naming, sharding and slot-key vectors (`test/unit/naming/`, also over the frame mux), and the whole Dart sharding integration suite (`test/integration/sharding/`, with `cluster.ts`) |
| QUIC and WebTransport | `core/quic/` (the binding, `connectQuic`, `listenQuic`, `quicConnector`, `QuicSwitchboardBinding`, `QuicSwitchboardListener`), `node/quic/` (the native provider), `web/quic/` (the WebTransport provider), `testing/fake-quic-provider.ts`, `native/` (the N-API addon over the vendored `sbq`) | `binding_test`, `review_cases`, `review_switchboard_test`, `switchboard_test` (including the man-in-the-middle group), `vectors_test`, `native_provider_test`, `native_integration_test` (`test/unit/quic/`, `test/integration/quic-*.test.ts`, `node-quic`, `node-webtransport`), `web_provider_test` and `web_binding_test` (`test/browser/webtransport.test.ts`, 34 tests in Chromium 141; also `test/unit/quic/webtransport-provider.test.ts` over an injected API); `build_hook_test` has a written reason (`test/unit/quic/not-ported.test.ts`: Dart's native-assets hook, which `native/build.mjs` stands in for) |
| Interop | `test/interop/` (the Dart peer, launcher, matrix, scenarios, oracle, probes) | the Dart cells of every scenario and the codec oracle; the TS peer and its cells are being added (section 4) |

Test counts on `main` (after the sharding merge) with the native addon built: the unit and integration projects report 3163 passed, 1 expected fail (the QUIC bulk-burst gap, section 7), 2 skipped (environment-dependent cases, such as the IPv6 loopback case in a container without one) and 3 todo (two `ws` fragment cases that need Dart's fragment cap, and `build_hook_test`); without the addon the native QUIC files skip with their reason. Every wiki vector table is named by a test (`SWITCHBOARD_VECTORS_STRICT=1` passes). On Bun, a few WebSocket fragment cases that Bun's `ws` cannot express are skipped. The browser test passes 34. The prebuilt addon packages (`native/npm/<target>/`, five targets) and `.github/workflows/release.yml` (manual dispatch: build, test and pack per target; publish only when asked) exist; only Linux x64 is built in CI.

## 3. How to run everything

```sh
npm ci                                    # in a worktree: node_modules is a symlink to the main checkout's; do not install there
npm run check                             # biome, tsc (src), unit + integration projects
npx tsc -p tsconfig.test.json --noEmit    # type-checks test/** too; CI does not run it
npm run build                             # dist/ for the four entry points
npx vitest run --project unit test/unit/mux   # one directory, fast
```

**Native QUIC.** `npm run build:native` builds the addon with cargo into `native/sbq-node.node` (about three minutes cold; Rust 1.99.0 in CI, 1.88 or later needed); or set `SWITCHBOARD_QUIC_ADDON=<file>` to one already built. The native files then run with the others; on their own: `npx vitest run --project integration test/integration/quic-native.test.ts test/integration/quic-binding-native.test.ts test/integration/quic-node-native.test.ts test/integration/node-quic.test.ts test/integration/node-webtransport.test.ts test/integration/identity.test.ts`. Without the addon they skip with a reason; `SWITCHBOARD_NATIVE=required` fails instead (CI). `node tool/sync-sbq.mjs --check <switchboard_quic checkout>` verifies the vendored shim against the pin.

**Browser.** `SWITCHBOARD_BROWSER=required npm run test:browser` (its own config, `test/browser/vitest.config.ts`): the addon for the native WebTransport listeners, the `playwright` dev dependency (1.56.1) and its Chromium (`PLAYWRIGHT_BROWSERS_PATH`); Vite serves the page from the sources. `SWITCHBOARD_BROWSER_LOG=1` prints the page's log.

**Interop.** `npm run test:interop` needs a Dart SDK 3.13.5 (`dart` on PATH, `SWITCHBOARD_DART_SDK`, or `node tool/dart-peer.mjs build --download-sdk`), a Rust toolchain (the Dart QUIC package's build hook runs cargo) and the two Dart repositories (sibling checkouts, `SWITCHBOARD_DART_CORE` and `SWITCHBOARD_DART_QUIC`, or `node tool/dart-peer.mjs fetch`). The global setup builds the AOT peer once and caches it on a stamp (`node tool/dart-peer.mjs status`); `SWITCHBOARD_DART_PEER=<exe>` uses a prebuilt peer (from a worktree, the main checkout's), `SWITCHBOARD_INTEROP=required` fails without a peer (CI), `SWITCHBOARD_INTEROP=skip` skips without looking, `SWITCHBOARD_FUZZ_SEED` and `SWITCHBOARD_FUZZ_COUNT` replay or widen the oracle.

**Bun and Deno**, with the built package (a consumer project whose `node_modules/@kaetemi/switchboard` points at the repository, `ws` installed beside it):

```sh
bun run test:unit                         # what CI runs on Bun
bun app.ts                                # a script importing @kaetemi/switchboard/node
deno run --allow-net --allow-read --allow-env --allow-ffi app.js
```

Deno needs `--allow-ffi` for the addon (and `--allow-read --allow-env` for its loader), and `ws` in a `node_modules` directory for the WebSocket server (`--node-modules-dir=manual` for a lone script). Node 22 runs compiled JavaScript; its type stripping cannot parse `await using`, so TypeScript using it runs on Node 24+ or after `tsc`.

**CI** (`.github/workflows/ci.yml`): lint and typecheck; unit and integration on Node 22, 24 and 26; the unit project on Bun; build (and an import of `dist/`); `interop` (Dart peer, required mode); `native` (addon build after `sync-sbq --check`, the native QUIC files); `browser` (addon build, Playwright's Chromium, `npm run test:browser` in required mode). Deno is not in CI.

## 4. Interop status

_To be filled in by the lead once the `claude/ts-peer` branch is merged: which cells of the matrix pass in each pairing (Dart and Dart, Dart and TS, TS and Dart, TS and TS) over each transport (tcp, ws, quic, webtransport), which scenarios exist, the oracle kinds checked, and what is still skipped and why._

## 5. Decisions

The design's section 11 is the list; the ones a session meets first:

- **Package** `@kaetemi/switchboard`; prebuilt addons `@kaetemi/switchboard-quic-<target>` as optional dependencies (Linux x64 and arm64 glibc, macOS arm64 and x64, Windows x64).
- **QUIC provision:** an N-API addon over a vendored copy of `switchboard_quic/native/sbq` with one change (`"rlib"` in the crate types), byte-identical otherwise and separable; the pin outside the copy (`native/sbq-vendor.commit`); a shared shim repository is the intended later direction. No upstream change.
- **Browsers:** `webTransport(init?)` returns the provider (a `QuicConnector`); `webTransportConnector(url, init)` returns a `ReconnectingClient`'s connector.
- **Placement:** `MeshNode` and `PeerSet` in `core/switchboard/`; `SlotGate`, `SlotLifecycle` and `SlotGates` in `core/naming/slot-gate.ts`; `isSlotResolver` in `core/switchboard/resolver.ts`; `StaticResolver` is a `SlotResolver`.
- **Binding base** keeps Dart's scope (drain holds, neutral diagnostics); the identity exchange and the two hooks are each binding's own. `_bulk` routing is installed by Talk through `adoptIncoming` and `admitAfterGoAway`.
- **Idioms** (design section 5): `Uint8Array` bytes, numbers for u48 ids, ms durations, const objects, pull-based `AsyncIterable` delivery with credit on delivery, Web Streams, typed `Emitter`, `AbortSignal`, `Symbol.asyncDispose`, injectable `Clock` and `Logger`, WebCrypto, `SwitchboardError` and `ProtocolError`, `RangeError`/`TypeError` for misuse, plain `Error` for `StateError`; wall time in ms for credentials and `verifyIdent`.
- **Branches:** `main` is the integration branch (the first hand-off's open question is closed).

## 6. Conventions

- **Worktrees and branches:** one topic per branch `claude/<topic>`, in a worktree `/home/user/wt/<topic>` branched from `main`; never edit the main checkout or commit on `main` from a worktree. Push only your own branch (`git push -u origin claude/<topic>`, retrying with backoff on network errors); the lead merges into `main` and pushes it.
- **Merges:** real merge commits only (`git merge --no-ff claude/<topic>`), never squash or rebase; keep branch history granular; continue a merged branch from `main` with a merge. The lead's procedure: merge, resolve conflicts, `npm run check`, `npx tsc -p tsconfig.test.json --noEmit`, push.
- **Commits:** one per milestone, a descriptive subject, a body saying what and why, and the two trailer lines `Co-Authored-By: Claude <noreply@anthropic.com>` and `Claude-Session: https://claude.ai/code/session_01B2KoRQTAdtpnxcxjhmD2ey`; no model names or identifiers anywhere (commits, code, comments, docs); nothing about hosts, credentials or internal paths in commit messages.
- **Before handing a branch over:** `npm run check` and `npx tsc -p tsconfig.test.json --noEmit` clean; the projects relevant to the change run often while working.
- **Style:** Biome (single quotes, 100 columns), `.ts` import extensions, `type` imports, no `any`, no TS `enum`, no `biome-ignore` without a reason, the file header comment as in the sources, JSDoc on every export, `process.env['X']`.
- **Ownership:** contract files (`src/core/*/contract.ts`) and the barrels outside a topic's directories are the lead's; a contract change goes in its own commit with a rationale. Stay inside the files a task names; shared test helpers go under `test/support/<area>/`.
- **Tests:** never skip, disable or weaken a test to get green; name TS test files after the Dart files; register wiki vector ids through `test/support/vectors.ts`.
- **Dart repositories are read-only.** Findings about them are reported (in notes and to the lead), never patched here.
- **Docs:** keep `docs/design.md`, `docs/work-split.md`, this document and `docs/wiki/` in step; wiki pages are GitHub wiki Markdown with relative page links (`[Identity](Identity)`), and every code block in them type-checks against the built package.

## 7. What remains

- **Interop:** the TS peer and its cells (in flight on `claude/ts-peer`, section 4).
- **Packaging and publishing:** musl Linux prebuilds (the loader looks for `-linux-<arch>-musl`); the first publish (the user's call; `release.yml` publishes only when asked, and the lock file needs regenerating after the first publish so that `npm ci` resolves the optional dependencies); only Linux x64 is built in CI.
- **The GitHub wiki push:** the wiki repository could not be written from this environment; `docs/wiki/` is ready to push as it is.
- **Deno in CI:** checked by hand on Deno 2.9 (TCP, WebSocket and native QUIC round trips with the built package); add a job once the suite is known to pass there.
- **Browsers:** Firefox's exporter path (Firefox 155+ has `exportKeyingMaterial`) is untested; the `protocols` negotiation needs Chromium 143+, which needs a Playwright newer than 1.56.1.
- **The QUIC bulk-burst gap**, shared with Dart: over the QUIC binding, which reads a channel's next record only after the previous one was consumed, a burst of more than 16 (`maxUnclaimedBulk`) bulk messages fails when an early bulk stream lags; recorded as `test.fails` in `test/integration/talk-quic.test.ts`. Options: the binding reads ahead for Talk, or Talk grants the parent a window ahead on unordered connections (design section 13, item 6).
- **Transport details:** `TcpTransport.bufferedOutputBytes` reports a turn late after an abort; the memory pair's writable `closed` does not settle when the other side closes.
- **Test fixtures:** `test/support/identity/mux-harness.ts` duplicates `test/unit/mux/support.ts`; the node's `SteppingClock` (`test/support/node/stepping-clock.ts`) and the naming tests' `TestClock` (`test/support/naming/test-clock.ts`) could merge into `ManualClock.elapseAsync`; the unit case "a forged IDENT gets GOAWAY UNAUTHENTICATED" (`test/unit/switchboard/identity-node.test.ts`) uses the rogue authority's credential where Dart flips a byte of a valid one (the integration port in `test/integration/identity.test.ts` flips the byte); `test/support/identity/stub-verifier.ts` stays for the mux IDENT tests.
- **Small doc and config leftovers** found by the docs pass (none changes behaviour): the root `vitest.config.ts` comment says "Four projects" but defines three (the browser tests have their own config); the `RelayService` doc example in `src/core/switchboard/relay.ts` calls `node.listenTcp({ host, port, policy })`, where the signature is `listenTcp(address, port, { policy })`; `test/integration/identity.test.ts` copies design section 2's identity example with the old `issuer.issueNode` and an awaited `selfSignedCertificate`, which the design now writes as `issuer.issue({...})` and a synchronous call.

## 8. History

**First phase** (pull requests into `claude/switchboard-ts-design-s5r9na`, merged with merge commits; `main` was initialised from it at the first hand-off): #1 Talk frame codec (04362ea), #2 QUIC records and the fake provider (e7c04da), #3 naming codecs and slot tables (47fb444), #4 Talk contract (f378d19), #5 stream framing, memory pair, endpoints (f0e7263), #6 the core node (0c88ec6), #7 the interop harness (ecf4f54), #9 mux codecs, IDENT, the binding base (1ba6818), #10 the reconnecting client (0a96930), #8 the QUIC binding and the native addon (63aeabb), #12 the oracle for mux codecs (034ab8b), #11 the Talk runtime (cd08e02), #13 the naming client, service and resolver (dd8c205), #14 the frame mux (1ba6e94), #15 the QUIC node binding (6cf6beb), #16 proxy, relay, `PeerSet` (1a608ed), #17 the frame mux in the core runtime (7975285), #18 a branch sync (0c0f92e).

**Continuation** (topic branches merged into `main`):

- `claude/mux-tests`: every Dart mux suite (`test/unit/mux/`), two performance fixes (channel streams made on first use; at most 1024 writes in flight to a transport), `MuxChannel[Symbol.asyncIterator]`, `identify()` pre-caught, a transport's writable that errors once its output ended.
- `claude/identity`: `Credential`, `Scope`, `CredentialIssuer`, `CredentialVerifier`, `Ed25519HolderKey`, the credential vectors, `identity_test.dart`, `NamingClient.renew()`.
- `claude/transports`: `websocket-transport.ts`, `src/node/transport/` (TCP, the `ws` server and client, `connectTransport`), `src/node/switchboard.ts` (`listenTcp`, `listenWebSocket`, `acceptWebSocket`, `listenQuic`, `listenWebTransport`), `src/web/switchboard.ts`, the loopback integration tests.
- `claude/talk-integration`: Talk over the frame mux, TCP and the QUIC binding (the integration tests), the bulk admission fix, vectors by id, `ManualClock.elapseAsync`.
- `claude/node-talk`: the node's Talk seam wired to `TalkChannel`; every node suite over the toy mux and the frame mux; the proxy, relay, `PeerSet`, client and QUIC node todos ported.
- `claude/webtransport`: the WebTransport provider (`webTransport()`, `webTransportConnector`), `test/browser/` with its own config, the `browser` CI job, the QUIC man-in-the-middle tests.
- `claude/mesh`: `MeshNode` (`src/core/switchboard/mesh-node.ts`) and the Dart node-level integration suites.
- `claude/quic-identity`: the node-level native QUIC tests, real credentials in the fixtures, the prebuilt addon manifests, `optionalDependencies`, `release.yml`, the QUIC vectors by id.
- `claude/slot-routing`: `openChannelToSlot`, `openTalkToSlot`, `resolveSlotOwner`, `StaticResolver` as a `SlotResolver`, the proxy's slot groups, `SlotGate`, `SlotLifecycle` and `SlotGates`, the naming suites over real Talk.
- `claude/sharding`: `MeshNode.publishSharded`, `claimSlot`, `releaseSlot`, `gates` and the slot hand-over of `leave`, the Dart sharding integration suite (`test/integration/sharding/`).
- `claude/docs`: this document, the design, the work split, the wiki pages (five new), the README and the CHANGELOG brought in step with the code.
