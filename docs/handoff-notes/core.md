# Hand-off: protocol core (transports, mux, identity)

State on 2026-10-14, after PR #14 merged. Repo `kaetemi/switchboard_ts`. Base branch `claude/switchboard-ts-design-s5r9na` (the lead merges, real merges, never squash). Porting decisions so far are in `notes/core.md` §22.

## Merged

- **PR #5**, transports: endpoint, stream framing, memory pair, stream transport, stream-binding vectors.
- **PR #9**, mux codecs (`mux-frame.ts`), `mux-protocol.ts` (IDENT, `verifyIdent`, the status rules), Ed25519 over WebCrypto, `secureRandomBytes`, `createPeerIdentity`, and the `BindingConnection` base (Dart's scope only).

## Also merged: PR #14, the frame mux without its tests

https://github.com/kaetemi/switchboard_ts/pull/14 was merged by kaetemi into the integration branch at `1ba6e94` (a real merge), with all 16 CI checks green, the Dart interop job among them. The branch `claude/switchboard-ts-core-tsaqug` has nothing left unmerged. The PR body lists the gaps below.

- `src/core/mux/frame-mux-channel.ts`: `FrameMuxChannel`, plus the internal `MuxChannelLink` and `MuxChannelHost`.
- `src/core/mux/frame-mux-connection.ts`: `FrameMuxConnection(transport, { isInitiator, ...MuxOptions })`.
- `src/core/mux/output-scheduler.ts`: the per-channel queues and the bulk zipper.
- Both classes are exported from `src/core/mux/index.ts`.
- `Fifo` (`src/core/transport/fifo.ts`) is now iterable.
- `test/unit/mux/frame-mux-smoke.test.ts`: 4 end-to-end checks over the memory pair (subframes, credit wait, close status, ping, GOAWAY, writable). This is the only test coverage the frame mux has so far.

## Remaining from the work split

**Step 3: frame mux tests (PR #14 has the implementation only)**

- Port the Dart tests:
  - `mux_test.dart` (1610 lines)
  - `mux_flow_test.dart` (1019)
  - the connection groups of `mux_limits_test.dart` (1068)
  - `mux_schedule_test.dart` (872)
  - the "connection" group of `vectors/mux_vectors_test.dart`
  - the IDENT gating tests of `binding_protocol_test.dart`
- Port `mux_harness.dart` to `test/unit/mux/support.ts` first: `quiet`, `rawOptions`, `muxPair`, `RawPeer`, `rawPair`, and `GatedTransport` (a `FrameTransport` with `isOutputReady`/`outputReady`).
- Time: `ManualClock`, or `SteppingClock` plus `settle()` from `test/support/node/stepping-clock.ts`. That is the `fake_async` `elapse`/`flushMicrotasks` equivalent.
- Expect bugs: keep-alive, the LIMITS wait, the scheduler, budgets, rejections and the IDENT exchange on the connection have only been read against Dart, not run.
- Open a follow-up PR with the ports and fixes.
- Ask the node thread, or the lead, to wire `coreRuntime().mux` in `src/core/switchboard/core-runtime.ts` (their file; it still says "unavailable") to `(t, o, i) => new FrameMuxConnection(t, { ...o, isInitiator: i })`. The node tests' `test/support/node/toy-mux.ts` can then give way to the real mux.

**Step 4: identity**

- Credential and scope codecs, with vectors C1 and C2.
- HMAC and Ed25519 verifiers, and the issuer.
- A `HolderKey` class. `ed25519KeyPairFromSeed` already exists. The contract already has an interface named `HolderKey`, so the class needs an explicit export.
- Port `identity_test.dart` (953 lines).
- Replace `test/support/identity/stub-verifier.ts` with the real verifier.

**Step 5: transports and integration**

- `websocket-transport.ts`, Node `tcp.ts` and `ws-server.ts`, `connect.ts`.
- Integration tests over the loopback.
- Port the Dart `web_socket_*` and `stream_transport_*` tests.
- The interop raw probe against `FrameMuxConnection`.

## Deviations from Dart

The lead accepted the first three choices below as written and recorded them, with the no-predicate rule of `admitAfterGoAway`, as items 8 to 10 of `docs/design.md` §12 (commit `5a991bb`).

- **`readable`.** It has a high-water mark of 0, and a subframe is delivered (and its credit returned) only on a read. Cancelling discards what arrives and does not close the channel. That is Dart's and the QUIC binding's behaviour; it reads design §6.3 "discards and closes" as closing the readable.
- **`writable.write` is `send`.** A write after the channel stops sending rejects and errors the writable, as in the QUIC binding. Dart's `sink.add` drops silently.
- **OPEN after this side's GOAWAY.** It is refused with CLOSE GOING_AWAY, as in Dart and QUIC. The lead fixed the `admitAfterGoAway` comment in the contract to match (`5a991bb`).
- **Address decoding in the mux is gone.**
  - `admitAfterGoAway` predicates replace Dart's `_continuesOpenChannel`. With no predicate installed, nothing is admitted after GOAWAY, not even `_bulk` continuations.
  - `adoptIncoming` filters run on the OPEN. Adopted channels never count toward the undelivered backlog.
- **The transport is read by a loop** that checks the pause flags (receive high-water mark, IDENT verification) before each `read()`. `#heldFrames` is kept for parity but stays empty in practice.
- **Writes are not awaited**, matching Dart's `sink.add`.
  - A failed write only marks the output gone; the end of the input drives shutdown.
  - Shutdown calls `reader.cancel()` and `writer.close()`, bounded by `keepAliveTimeout` on the clock, then `transport.abort?.()`.
- **No synchronous peers.** At best the memory pair hands frames over in a microtask. Dart tests that rely on a transport answering during a write need adapting.
- **API differences.**
  - `ping` resolves in ms and takes an `AbortSignal`, and so does `identify`.
  - `identify` is async, so holder-key misuse rejects with `RangeError`, as in Dart.
  - `peerIdentity` expiry uses wall time.
  - `consumed` throws `Error` (Dart `StateError`).
  - `grant` requires an integer, and the `priority` setter validates its value.
  - `toString` keeps Dart's `MuxConnection(...)`.
- **For step 5.** Dart's unknown scheme is UNIMPLEMENTED; the design says UNSUPPORTED.

## For a fresh session

- Push only to the designated branch.
- If you open a PR against the design branch:
  - follow the attribution lines the thread's instructions give;
  - assign kaetemi;
  - subscribe to the PR's activity.
- Never rewrite merged history.
- The Dart repos are read-only.
- Contracts (`src/core/*/contract.ts`) are the lead's.
- Name tests after the Dart files.
- Biome rules: 100 columns, no new `biome-ignore`.
