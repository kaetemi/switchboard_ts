# Hand-off: Dart interop test harness (thread 6)

State on 2026-10-14, for the session that picks this up. Repository
`kaetemi/switchboard_ts`, integration branch `main` (the first phase's PRs
went into `claude/switchboard-ts-design-s5r9na`, from which `main` was
initialised). Updated by the docs pass for what changed around this thread,
by the lead for the merge of `claude/ts-peer` (the TS peer), and on
2026-10-18 for branch `claude/interop-2-c9hpc0` (phase 4).
The longer background is `../notes/interop.md` (sections 1 to 9 are the
phase 1 design, sections 10 to 12 the status of phases 2 to 4); the peer
protocol is specified in `test/interop/README.md` in the repository.

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

`claude/interop-2-c9hpc0` until the lead merges its PR. The first thread
branch `claude/switchboard-ts-interop-x2iz7c` and `claude/ts-peer` hold no
commit `main` lacks.

## Done since (branch `claude/ts-peer`, merged into `main`)

The TS peer, the scenario matrix in all four pairings over TCP, WebSocket,
raw QUIC and WebTransport, chains of three peers in every assignment of
the implementations, the identity, naming, sharding, proxy and relay
scenarios, the oracle for every codec, the Dart driver and the CI steps:
see `../notes/interop.md`, section 11. 3686 interop tests pass, none
skipped, with the native addon.

## Done since (branch `claude/interop-2-c9hpc0`)

Section 12 of `../notes/interop.md` has the detail. 4540 interop tests
pass in required mode and 4 are skipped with their reason; the Dart driver
passes 59 tests. Nothing in `src/**` changed.

* **Sharding** (`scenarios/sharding.test.ts`): managed and lazy spaces,
  refused hand-overs (RESUME, roll-back), an owner going down mid-hand-over,
  the queue, and the MOVED retry of `openChannelToSlot`, `openTalkToSlot`
  and `resolveSlotOwner`, with the slot handler and `sc` slot channel
  commands in both peers.
* **The reconnecting client** (`scenarios/reconnect.test.ts`) against a
  server's GOAWAY and close, on all four transports.
* **QUIC records** (`probe/quic.test.ts`): a raw QUIC and WebTransport
  peer on the native provider, against both peers in both directions.
* **The Dart driver**: 24 more tests, for what the TS cells do not see
  from the Dart side.
* **The close races of CI run 157**: the first was the peers reporting a
  channel's end before its last subframes (fixed in both peers); the
  second is a WebTransport session-gone race both implementations share,
  which the scenarios accept as the binding page allows.

Reported to the lead, not patched: both QUIC bindings can close the
session before their GOAWAY record is acknowledged, which loses it. The
probe's case for it is skipped in its four cells with that reason.

## Remaining

1. Out of scope: a browser peer (the web entry in Chromium) as a third
   implementation of the client half, listed under "Not started" in the
   work split.
2. Each needing a piece the harness does not have (a transport the test
   can silence, a peer restarted on the same port, a raw WebSocket client,
   window introspection on the TS side), listed in section D, "Coverage",
   of `../notes/interop-dart-suites.md`: keep-alive against a silent peer
   over TCP and WebSocket, LIMITS sent mid-connection, a naming-service
   restart with re-claims by epoch, slot-request EXTEND deadlines,
   `reassemblyBudget` back-pressure, the WebSocket handshake refusals, and
   what the native QUIC provider cannot produce.

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
