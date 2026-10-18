# Hand-off: QUIC transport (thread 5)

State on 2026-10-14, for the session that picks this up, after the continuation's merges.
Repository `kaetemi/switchboard_ts`, integration branch `main` (the first phase's PRs went into
`claude/switchboard-ts-design-s5r9na`, from which `main` was initialised).
The assignment is thread 5 of `docs/work-split.md`; the design is `docs/design.md` sections 3.1
and 6.10. The Dart reference is `kaetemi/switchboard_quic` (read-only).

## Merged

* **#2**: the record codecs (`records.ts`, `record-reader.ts`), HELLO, the exporter helpers, and
  the in-memory provider (`src/testing/memory-quic-provider.ts`), with the record vectors.
* **#8**: the binding and the native provider.
  * `QuicBindingConnection` and `QuicBindingChannel` extend the core's `BindingConnection` and
    reuse its control codecs and `verifyIdent`.
  * Also `connectQuic`, `listenQuic` and `QuicIdentityRules`.
  * The N-API addon in `native/`, and `nativeQuic()` and `selfSignedCertificate()` from
    `@kaetemi/switchboard/node`.
  * The `native` CI job.
  * Tests: `test/unit/quic/binding.test.ts` (57), `review.test.ts` (31),
    `test/vectors/quic-connection.test.ts` (22), `test/integration/quic-native.test.ts` (94),
    and `test/integration/quic-binding-native.test.ts` (21).

## Merged: PR #15, the hand-off state

[PR #15](https://github.com/kaetemi/switchboard_ts/pull/15) merged into the integration branch
as `6cf6beb`, so nothing of this thread is left unmerged. Its head, `909c212` on
`claude/switchboard-ts-quic-v2rwql`, had the integration branch merged in at `dd8c205` (Talk #11
and naming #13 included) and was green in CI. `npm test` passed locally: 63 files, 1533 tests.
Bun's unit run passed too. The PR lists the gaps below. Start follow-up work from the integration
branch; #8 and #15 must not be reused.

* **`75658d3`**: QUIC inside the Switchboard node, a port of `quic_switchboard_binding.dart`.
  It is in `src/core/quic/quic-switchboard.ts` and exported from `src/core/quic/index.ts`.
  * `QuicSwitchboardBinding` is a `SwitchboardBinding` for `quic` and `https`, used through
    `registerBinding` or `bindings: [...]`. Its `normalise` is `parseEndpoint(e).key`.
  * `QuicSwitchboardListener.start(node, providerListener, options)` declares the endpoint,
    adopts each session's connection, and identifies in return.
  * `quicConnector(endpoint, connector, init)` is in `quic-dial.ts`. It gives a
    `ReconnectingClient` its `ConnectionConnector`.
  * Nothing in the node's files changed. The node and client thread owns them, and the
    coordinator relays any hook request.
* **`786a59b`**: `test/unit/quic/switchboard.test.ts`, with 20 tests and 5 todo. It ports
  `switchboard_test.dart` and `review_switchboard_test.dart` as far as the core reaches.
  * Payloads go over raw channels in pieces of 16 KiB or less. Without Talk, a `send` is one
    subframe and the record limit is 32752.
  * Todo, each with its reason in the file:
    * the Talk bulk case;
    * the two proxy cases (they need the frame mux, Talk and the proxy);
    * `SlotChannel`, which doesn't exist in TS yet;
    * the man-in-the-middle group. This one is portable now: relay the streams through
      `network.listen` plus a connector, and clear `QuicHello.flagExporter` at offset 23 from
      the initiator and 13 from the acceptor.
* **`6359410`**, merged as `8d1e400`: `src/web/quic/webtransport-api.ts`. It holds the
  structural W3C WebTransport types, how the API's rejections are read, and `StreamErrors`,
  which probes the spec and the Chromium `WebTransportError` constructors. It is a port of
  `web_interop.dart` and is **not exported yet**. The provider that uses it isn't written.

## Done since: deliverables 4 and 5, and the test parity

All on `main` (the integration branch since the first hand-off), from topic branches:

1. **The WebTransport provider** (`claude/webtransport`): `src/web/quic/webtransport-provider.ts`, a port of `web_provider.dart`, exported from `src/web/quic/index.ts` and `src/web/index.ts`: `webTransport(init?)` returns a `WebTransportProvider` (a `QuicConnector`; `init` injects the `WebTransport` and `WebTransportError` constructors and a clock), `webTransportConnector(url, init)` gives a `ReconnectingClient` its connector (`quicConnector` over `webTransport()`), with `WebTransportSession`, `WebTransportStream`, `WebTransportReceiveStream`, `isSupported`, `maxStreamErrorCode`, `sendOrderOf`, the 2 s session-gone grace, the `openStream` credit wait, feature tests of every optional member, and the duplicate-protocol check Chromium does not make. The docs' disagreement on `webTransport` is settled: it returns the provider (design section 11). Tests: `test/unit/quic/webtransport-provider.test.ts` over an injected API, and `test/browser/webtransport.test.ts` (34 tests: `web_provider_test.dart` and `web_binding_test.dart` in Playwright's Chromium 141 against native listeners started in the test process, the page served by Vite; its own `test/browser/vitest.config.ts`; `npm run test:browser`), the `browser` CI job (`SWITCHBOARD_BROWSER=required`). The man-in-the-middle group of `switchboard_test.dart` is `test/unit/quic/mitm.test.ts`.
2. **Prebuilt packaging** (`claude/quic-identity`): the five manifests under `native/npm/<target>/` (`linux-x64-gnu`, `linux-arm64-gnu`, `darwin-arm64`, `darwin-x64`, `win32-x64-msvc`), the `optionalDependencies` the pack step adds with `tool/prebuilt-deps.mjs` (not in the repository's `package.json` until the packages are published), `test/unit/quic/packages.test.ts`, and `.github/workflows/release.yml` (manual dispatch: builds and tests each target on a runner of its platform, packs the six tarballs, publishes only when asked). CI builds Linux x64 only; the other targets are built by the release workflow.
3. **Node-level tests** (`claude/quic-identity`, `claude/node-talk`, `claude/talk-integration`): `test/integration/quic-node-native.test.ts` (the node half of `native_integration_test.dart`: Talk, proxy, throughput, a paused reader), `node-quic.test.ts` and `node-webtransport.test.ts` (the Node entry's `listenQuic` and `listenWebTransport`, which fill in the ALPN and protocol), `identity.test.ts` (exporter-bound identity between nodes over raw QUIC and WebTransport), `test/integration/talk-quic.test.ts` (Talk over the binding: unordered channels, bulk streams overtaking or lagging, drain holds, `_bulk` continuations after GOAWAY); the Talk, proxy and `SlotChannel` todos of `test/unit/quic/switchboard.test.ts` are ported. The tests use real credentials (`identity-fixtures.ts`, also in `review.test.ts`).
4. **Bulk bursts** (`claude/quic-bulk-burst-g3lirh`, design section 13 item 6): a burst of more than 16 bulk messages over the binding no longer fails. The binding is unchanged (it still reads a channel's next record only once the previous one was consumed, as the binding page and the ported `manualCredit` test say); Talk reads on for itself on unordered connections, reporting the frames it holds consumed ahead of their delivery while bulk channels wait for their references, up to one window, and refuses a bulk channel beyond 16 only once the frames that could carry its reference were read. A fixed `grant` ahead is the wrong fix: the references usually overtake their bulk streams, so it leaves more than 16 waiting for their channels, a channel PROTOCOL_ERROR. Before it, the native provider refused 11 to 12 of 30 without any lag. Tests: `talk-quic.test.ts` (the former `test.fails`, and two bursts of 100) and `quic-node-native.test.ts` (bursts between nodes, raw QUIC and WebTransport).

Test parity, by Dart file:

| Dart test file | TS |
|---|---|
| `binding_test`, `review_cases`, `vectors_test`, `native_provider_test` | `binding.test.ts`, `review.test.ts`, `test/vectors/quic-*.test.ts`, `test/integration/quic-native.test.ts` |
| `native_integration_test` | `test/integration/quic-binding-native.test.ts` (connection level), `quic-node-native.test.ts` (through nodes) |
| `switchboard_test`, `review_switchboard_test` | `switchboard.test.ts`, `mitm.test.ts`, `review.test.ts` |
| `web_provider_test`, `web_binding_test` | `test/browser/webtransport.test.ts` (and `webtransport-provider.test.ts`) |
| `build_hook_test` | the reason in `not-ported.test.ts`: Dart's native-assets hook, which `native/build.mjs` stands in for |

**Left:** interop with the Dart peer over raw QUIC and WebTransport in both directions comes with the TS peer (`claude/ts-peer`; the Dart peer already listens and dials on `quic` and `webtransport`, its certificate hash in READY); the first publish (the user's call); Firefox's departures, above all its exporter, which makes a node refuse a Firefox page's `IDENT` (design section 13, item 7).

**Since (`claude/packaging-runtimes`):** musl prebuilds for Linux x64 and arm64 (`build-musl` in `release.yml`, an Alpine container; seven targets in all), a `deno` CI job (the unit project, and the built package from its tarballs through `test/runtime/smoke.mjs`), and Playwright 1.64.1 with a project per browser: Chromium 156 (the `protocols` negotiation) and Firefox 157 (the exporter path), the departures of Firefox checked as such (`departures` in `test/browser/webtransport.test.ts`).

## Known gaps and decisions

* `contract.ts` says a close code is 0 to 255, but WebTransport carries 0 to 2^32 - 1 in both
  the native provider and the Dart web provider. A browser can't tell `refused` from
  `certificateRejected`; Dart reports `handshakeFailed` for both.
* The Dart reference keeps the bulk-burst gap (reported, not patched): its receiver refuses a
  burst past 16 bulk channels waiting for their references where the TS receiver takes it, so
  an interop scenario with such a burst asserts per implementation.
* `connectQuic` takes `serverCertificateHashes`, as in the design's example. Each hash is 32
  bytes or WebTransport's `{ algorithm: 'sha-256', value }`. Another algorithm is a
  synchronous `TypeError`.
* The sbq pin is in `native/sbq-vendor.commit`, outside the vendored copy. The design doc was
  updated to say so in `783e31f`.
* A raw QUIC listener needs `alpn: [switchboardAlpn]` in its `QuicServerConfig`; the Node
  entry's `listenQuic` and `listenWebTransport` fill it in (and the WebTransport protocol).
* The test identity: `test/unit/quic/identity-fixtures.ts` has real credentials of an Ed25519
  test authority and an HMAC issuer, real verifiers and `Ed25519HolderKey`s, as
  `binding_test.dart` has them; the node-level tests use `test/support/node/seeded-identity.ts`,
  which is real credentials too.

## The native addon and the vendored shim

* **Build.** `npm run build:native` runs `native/build.mjs`: `cargo build --release --locked`,
  then a copy to `native/sbq-node.node`, which git ignores.
  * You need Rust; CI and `switchboard_quic` use 1.99.0, and the crate needs 1.88 or later.
  * `node native/build.mjs --debug` builds the dev profile.
  * `--target <triple> --out <file>` cross-builds.
* **Loading.** The addon loads on first use, never at import. The order is
  `$SWITCHBOARD_QUIC_ADDON` (and only that, when set), then `native/sbq-node.node`, then
  `@kaetemi/switchboard-quic-<target>`.
  * Without it, QUIC fails `unsupportedScheme`, and `nativeQuicAvailability()` says why.
  * The native tests skip with a reason. `SWITCHBOARD_NATIVE=required`, as in CI, makes them
    fail instead.
  * Bun loads it as Node does. Deno needs `--allow-ffi --allow-read --allow-env`.
* **The shim.** `native/sbq-vendor/` is `switchboard_quic/native/sbq` at
  `c5b32652cdcfe91e496fe853b7dcb3a354325bbe`. It is byte-identical apart from one change:
  `"rlib"` added to `crate-type`.
  * `node tool/sync-sbq.mjs [<checkout>]` copies it again.
  * `--commit <rev>` moves the pin.
  * `--check` exits 1 when the copy differs.
  * CI checks out `kaetemi/switchboard_quic` at the pin, and the commit is hard-coded in the
    `native` job's `ref`. Change both together. Fixes go upstream first.
  * The user's plan is a dedicated shim repository shared with Dart once the port is ready.
    Keep the copy separable.
* **The addon's JS surface** is `sbq.h` ABI v4, function for function (`SbqAddon` in
  `src/node/quic/native-provider.ts`). Events arrive through a weak, non-blocking thread-safe
  doorbell. `setActive` holds the event loop only while a session or listener is live, so an
  idle process exits.

## Conventions the user asked for

* Use real merges, never squash, with granular commits at each milestone.
* End each commit with `Co-Authored-By: Claude <noreply@anthropic.com>` and the
  `Claude-Session` trailer.
* No model names in commits, PRs or code.
