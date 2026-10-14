# Hand-off: Dart interop test harness (thread 6)

State on 2026-10-14, for the session that picks this up. Repository
`kaetemi/switchboard_ts`, integration branch `main` (the first phase's PRs
went into `claude/switchboard-ts-design-s5r9na`, from which `main` was
initialised). Updated by the docs pass for what changed around this thread;
the TS peer itself is in flight on `claude/ts-peer`, whose own notes are
the current word on it.
The longer background is `../notes/interop.md` (sections 1 to 9 are the
phase 1 design, section 10 the phase 2 status); the peer protocol is
specified in `test/interop/README.md` in the repository.

## Merged

Both PRs were merged into the integration branch with merge commits:

* **#7**, the harness:
  * the Dart peer (`test/interop/dart_peer/`, pinned by `dart_commits.txt`)
  * `tool/dart-peer.mjs` and its `tool/dart-peer.d.mts`
  * the launcher, matrix and global setup (`test/interop/*.ts`)
  * `scenarios/basics.test.ts`
  * the raw probes (`probe/`)
  * the oracle
  * the wiki vectors (`test/vectors/wiki-vectors.json`, 214 vectors) with
    `tool/extract-wiki-vectors.mjs`, `test/support/vectors.ts` and
    `test/vectors/coverage.test.ts`
  * the `interop` CI job
  * `docs/wiki/Testing-and-Interop.md`
* **#12**, the oracle extended: the TS mux frame and control codecs are now
  checked against the Dart reference.

The interop project currently passes 241 tests and skips 190. The skipped
tests are every TS cell, and each skipped suite names what it waits on.

## Unmerged

The first thread branch `claude/switchboard-ts-interop-x2iz7c` holds no
commit `main` lacks. The TS peer, the scenario matrix, more oracle kinds and
the identity, naming and relay scenarios are being written on
`claude/ts-peer` (worktree `/home/user/wt/ts-peer`).

## Remaining from the work split

1. **Deliverable 2: the TS peer and the mux and Talk matrix.**
   * It needed the frame mux connection (thread 1) and the Talk channel
     layer (thread 2); both have landed since, with the node's entry points
     and every transport (TCP, WebSocket, native QUIC and WebTransport).
   * The TS peer goes in `test/interop/ts_peer/main.ts`, using the public
     API only. It implements the commands, events and `echo`, `raw` and
     `sink` services in `test/interop/README.md`, and prints the same READY
     line. `peers.ts` finds it by that path.
   * Clear the entries in `tsTransportGaps` (`test/interop/matrix.ts`) as
     transports become servable. `src/core/quic/` and `src/node/quic/` have
     landed (thread 5), so check whether QUIC can be served on Node.
   * Then add the scenarios: request, stream, abort, extend, bulk, close,
     GOAWAY and protocol-error injection. Also point `probe/conformance.test.ts`
     at the TS server, which it already loops over.
2. **More oracle kinds.**
   * The TS talk frame (`src/core/talk/talk-frame.ts`) and stream framing
     codecs exist, and the Dart peer already answers the `talk` and `stream`
     kinds. Agreement checks for these two are not written yet.
   * Naming, sharding, credential and QUIC record kinds need new cases in
     `dart_peer/lib/src/oracle.dart` first.
3. **Deliverable 3:** identity (NONCE and IDENT both ways, holder key and
   bearer, exporter-bound over QUIC), naming and sharding, relay and proxy,
   and mixed-language meshes.
4. **Deliverable 4:** a Dart driver under `test/interop/dart_driver/` that
   spawns the TS peer.
5. **Vector coverage:** done. Every vector test names its ids through
   `test/support/vectors.ts`, and `SWITCHBOARD_VECTORS_STRICT=1` passes.

Conventions:
* The lead merges with real merge commits. Keep branch history granular,
  with no squash, amend or rebase.
* Work on a topic branch `claude/<topic>` in a worktree under
  `/home/user/wt/<topic>`; the lead merges it into `main`.
* `npx tsc -p tsconfig.test.json --noEmit` is the lead's check over `test/**`.
  CI does not run it, so run it before every push.

## Running the Dart interop suite

**Locally:**

```sh
npm ci
npm run test:interop        # builds the Dart peer once if needed, then runs the interop project
node tool/dart-peer.mjs status   # what the build uses and whether the peer is fresh
```

* **Needs:**
  * a Dart SDK, 3.13.5 in CI: `dart` on PATH, or `SWITCHBOARD_DART_SDK` or
    `DART_SDK` (`build --download-sdk` fetches one)
  * a Rust toolchain: switchboard_quic's build hook runs cargo, about two
    minutes cold
* **The two Dart repositories**, found in this order:
  * `SWITCHBOARD_DART_CORE` and `SWITCHBOARD_DART_QUIC`
  * sibling checkouts `../switchboard` and `../switchboard_quic`
  * `node tool/dart-peer.mjs fetch`, which clones the pinned commits into
    `test/interop/dart_peer/.deps/`
* **Pins:** in `test/interop/dart_peer/dart_commits.txt` (core `3816e6f`,
  QUIC `c5b3265`). A checkout at another commit only warns locally.
* **Without Dart:** the interop files skip, with one line saying why.
* **Environment variables:**
  * `SWITCHBOARD_INTEROP=required` fails the run instead of skipping.
  * `SWITCHBOARD_INTEROP=skip` skips without looking for a peer.
  * `SWITCHBOARD_INTEROP_BUILD=0` never builds the peer.
  * `SWITCHBOARD_DART_PEER=<exe>` uses a prebuilt peer.
  * `SWITCHBOARD_FUZZ_SEED` and `SWITCHBOARD_FUZZ_COUNT` replay or widen
    the oracle's fuzzing.
* **In the cloud container used here:** Dart 3.13.5 is at
  `/root/.cache/switchboard_ts/dart-sdk-3.13.5-linux-x64/dart-sdk/bin`,
  the Dart repositories are sibling checkouts in `/home/user`, and the
  main checkout's built peer can be reused from a worktree with
  `SWITCHBOARD_DART_PEER`.

**In CI:** the `interop` job in `.github/workflows/ci.yml`:
* checks out both Dart repositories at the pins
* installs Dart 3.13.5 and Rust 1.99.0
* restores the cargo and peer caches; the peer cache key hashes the peer's
  sources and the pins
* runs `node tool/dart-peer.mjs build --expect-pins`, which fails on a pin
  mismatch
* runs `npm run test:interop` with `SWITCHBOARD_INTEROP=required`

If Dart or Rust cannot be installed, the job warns and runs with the Dart
cells skipped. A cold run takes about 80 s and a warm one about 45 s.
Moving the pins forward is a deliberate commit to `dart_commits.txt`.
