# Hand-off: Dart interop test harness (thread 6)

State on 2026-10-14, for the session that picks this up. Repository
`kaetemi/switchboard_ts`, integration branch `claude/switchboard-ts-design-s5r9na`.
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

Nothing. The branch `claude/switchboard-ts-interop-x2iz7c` holds no commit
the integration branch lacks, and no work in progress exists. The TS peer has
not been started.

## Done since (branch `claude/ts-peer`)

The TS peer, the scenario matrix in all four pairings over TCP, WebSocket,
raw QUIC and WebTransport, chains of three peers in every assignment of
the implementations, the identity, naming, sharding, proxy and relay
scenarios, the oracle for every codec, the Dart driver and the CI steps:
see `../notes/interop.md`, section 11. 3686 interop tests pass, none
skipped, with the native addon.

## Remaining

1. Sharding beyond a static space and one hand-over: managed and lazy
   spaces, refused hand-overs (RESUME, roll-back), and the MOVED retry of
   slot-routed opens once the node routes by slot.
2. The reconnecting client against a server's GOAWAY and close (scenario
   23 of the notes) and the QUIC record-level cases (24): they need a raw
   QUIC probe or more peer commands.
3. A browser peer (the web entry in Chromium) as a third implementation of
   the client half.
4. More of the Dart repositories' two-peer tests in the Dart driver.

Conventions:
* The lead merges with real merge commits. Keep branch history granular,
  with no squash, amend or rebase.
* After a PR merges, restart the branch from the integration branch.
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
* **In the cloud container used here:** Dart is at `/opt/dart-sdk/bin`
  (`export PATH=/opt/dart-sdk/bin:$PATH`), and the Dart repositories are
  sibling checkouts in `/home/user`.

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
