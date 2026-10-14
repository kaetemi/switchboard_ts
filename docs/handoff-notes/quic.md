# Hand-off: QUIC transport (thread 5)

State on 2026-10-14, for the session that picks this up. Repository
`kaetemi/switchboard_ts`, integration branch `claude/switchboard-ts-design-s5r9na`.
The assignment is thread 5 of `docs/work-split.md`; the design is `docs/design.md` sections 3.1
and 6.10. The Dart reference is `kaetemi/switchboard_quic` (read-only).

## Merged

* **#2**: the record codecs (`records.ts`, `record-reader.ts`), HELLO, the exporter helpers, and
  the fake provider (`src/testing/fake-quic-provider.ts`), with the record vectors.
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

## What remains of the assignment

1. **The WebTransport provider (deliverable 4).**
   * Port `switchboard_quic/lib/src/web/web_provider.dart` (1291 lines) to
     `src/web/quic/webtransport-provider.ts`.
     * Expose a `webTransport()` factory returning a `QuicConnector`. Inject the
       `WebTransport` constructor for tests.
     * Reads are chunked to `read(max)`. Writes use `writer.ready` for backpressure.
     * `reset` is `writer.abort(WebTransportError)`. `stop` is `reader.cancel`.
     * Keep the 2 s session-gone grace and the `openStream` credit wait with 5 to 200 ms
       backoff. Feature-test `protocol`, `draining`, `exportKeyingMaterial`, `getStats`,
       `createSendGroup` and `sendOrder`.
     * Pre-catch every promise.
   * Add `src/web/quic/index.ts`, and export it from `src/web/index.ts`. That file's doc comment
     says the provider is not there yet; rewrite it to name what is exported.
   * The browser test goes in `test/browser/webtransport.test.ts`, with its own
     `test/browser/vitest.config.ts`; don't edit the root config.
     * Node starts native WebTransport listeners with handlers ported from
       `tool/web_server.dart`. Vite (in `node_modules`) serves a page module importing
       `/src/web/index.ts`. Playwright drives Chromium through `page.evaluate`.
     * Port `web_provider_test.dart` (568 lines) and `web_binding_test.dart` (220), using
       `web_harness.dart` as the guide.
     * Skip with a reason when the addon or Chromium is missing; `SWITCHBOARD_BROWSER=required`
       fails instead.
   * Add a `browser` CI job: the `native` job's Rust and sbq steps, then
     `npx playwright install --with-deps chromium`, then the browser test. The work split
     allows `playwright` as a dev dependency. Pin `1.56.1` to match Chromium 1194.
   * In this container, Playwright 1.56.1 is installed globally at
     `/opt/node22/lib/node_modules/playwright`, Chromium is in `/opt/pw-browsers`, and
     `PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1` is set.
   * A hand check on Chromium 141 (build 1194) against the native listener from an
     `http://127.0.0.1` page:
     * Echo, reset 42 and close 7 'bye' worked, with `serverCertificateHashes` taking
       `selfSignedCertificate('127.0.0.1')` as it is.
     * Chromium has none of `protocol`, `draining`, `exportKeyingMaterial`, `getStats`,
       `createSendGroup` or `sendOrder`, and sends no `WT-Available-Protocols`. So the
       provider must refuse duplicate protocols itself, to keep Dart's "schemes and
       arguments" case.
     * The spec form of the `WebTransportError` constructor throws; the dictionary form
       clamps the code to 255.
     * A writer's `desiredSize` starts at 1.
   * The rest of the provider was designed but not written:
     * `WebTransportProvider` with `isSupported`, `maxStreamErrorCode`, `sendOrderOf` and a
       `webTransport(init?)` factory;
     * an injectable constructor and clock;
     * exported `WebTransportSession` and `WebTransportStream`;
     * incoming streams read only while someone waits;
     * `connectWebTransport` and `webTransportConnector`.
   * The docs disagree on `webTransport`. `docs/design.md` returns a provider for
     `connectQuic`; the wiki page and `docs/work-split.md` return a connector from
     `webTransport(url, options)`. Settle it with the lead.
2. **Prebuilt packaging (deliverable 5).**
   * Per-target manifests go under `native/npm/<target>/`. The targets and package names are
     in the table in `native/README.md`.
   * Draft a `release` workflow that builds the five targets. Publishing is the user's call.
   * The loader already looks for `@kaetemi/switchboard-quic-<target>`. The
     `optionalDependencies` in `package.json` belong to the lead, so ask.
   * Only Linux x64 is built in CI today. macOS and Windows have never been built.
3. **Interop with the Dart peer.**
   * Acceptance: a TS node over the native provider interoperates with the Dart peer over raw
     QUIC and WebTransport, in both directions.
   * The Dart peer in `test/interop/dart_peer` already listens and dials on `quic` and
     `webtransport`. Its listeners share one ECDSA certificate whose hash it reports as
     `certificateSha256`.
   * `tsTransportGaps` in `test/interop/matrix.ts` lists `quic` and `webtransport` as waiting
     on thread 5. That file is thread 6's.
   * Once a TS peer exists, wire it up:
     * dial with `QuicSwitchboardBinding(nativeQuic())`, with
       `webTransportCertificateVerified: false` or the hash as a pin;
     * listen with `QuicSwitchboardListener.start(node, await nativeQuic().listen(host, 0, { alpn: [switchboardAlpn], certificateDer, privateKeyDer }))`,
       or with `listenWebTransport(..., { path: '/sb' })`.
4. **Test parity.** Every `switchboard_quic` test should have a TS counterpart or a written
   reason. Status by Dart file:

   | Dart test file | TS status |
   |---|---|
   | `binding_test`, `review_cases`, `vectors_test`, `native_provider_test` | done |
   | `native_integration_test` | ported at connection level, not through nodes; its Talk, proxy, throughput and paused-reader cases can now move onto `QuicSwitchboardBinding` |
   | `switchboard_test`, `review_switchboard_test` | partly done, as above |
   | `web_provider_test`, `web_binding_test` | not started |
   | `build_hook_test` | needs a reason in a test file: it tests Dart's native-assets hook, which has no TS counterpart (`native/build.mjs` stands in) |

5. **Talk (#11) has merged:** port the todo Talk cases in `switchboard.test.ts`. Also check
   that Talk's `installBulkRouting` installs `admitAfterGoAway` on a QUIC connection, which is
   how `_bulk` continuations are admitted after GOAWAY. Dart decodes `TalkBulkOpen` inside the
   binding; TS leaves that to the hook.
6. Optionally, `test/unit/quic/review.test.ts` could use `identity-fixtures.ts` instead of its
   local identity stand-ins.

## Known gaps and decisions

* `contract.ts` says a close code is 0 to 255, but WebTransport carries 0 to 2^32 - 1 in both
  the native provider and the Dart web provider. A browser can't tell `refused` from
  `certificateRejected`; Dart reports `handshakeFailed` for both.
* `connectQuic` takes `serverCertificateHashes`, as in the design's example. Each hash is 32
  bytes or WebTransport's `{ algorithm: 'sha-256', value }`. Another algorithm is a
  synchronous `TypeError`.
* The sbq pin is in `native/sbq-vendor.commit`, outside the vendored copy. The design doc was
  updated to say so in `783e31f`.
* A raw QUIC listener needs `alpn: [switchboardAlpn]` in its `QuicServerConfig`. Nothing on the
  node side fills it in for the caller.
* `src/core/switchboard/switchboard.ts` and `runtime.ts` still say `@polyverse/switchboard` in
  their docs. These are the node thread's files.
* The test identity: `test/unit/quic/identity-fixtures.ts` has real Ed25519 holder keys over
  WebCrypto (`generateHolderKey`), because the binding verifies proofs for real. The node-level
  tests pair them with `test/support/node/fake-identity.ts` credentials and verifier. The core
  has no credential codec or issuer yet.

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
