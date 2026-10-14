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
- `test/unit/mux/frame-mux-smoke.test.ts`: 4 end-to-end checks over the memory pair (subframes, credit wait, close status, ping, GOAWAY, writable). The Dart suites are ported since (step 3 below).

## Remaining from the work split

**Step 3: frame mux tests (branch `claude/mux-tests`)**

- Ported, one TS file per Dart file under `test/unit/mux/`, over `support.ts` (the port of `mux_harness.dart`, every connection on a `SteppingClock`): `mux.test.ts` (`mux_test.dart`), `mux-flow.test.ts`, `mux-limits.test.ts` (connection groups added to the codec half), `mux-schedule.test.ts` and `mux-talk.test.ts`; the "connection identity" group of `identity_test.dart` is the identity thread's `test/unit/identity/identity.test.ts`. `test/vectors/mux.test.ts` has the "connection" group of `mux_vectors_test.dart`; `test/vectors/binding-protocol.test.ts` the IDENT gating tests. `frame-mux-api.test.ts` covers the TS-only API (streams, signals, `await using`, the hooks, the write backlog).
- The TCP cases of the flow and limits suites run on the loopback in the unit files; those of `mux_test.dart` (TCP, WebSocket) are the transports thread's `test/integration/mux-tcp.test.ts` and `mux-websocket.test.ts`. Not ported: the Dart mirrors that count the scheduler's private turn lists (the observable half is checked).
- Also fixed on this branch: `identify()` hands out a pre-caught promise; `MuxChannel` is async iterable over its subframes (contract, all channel classes, `readSubframes` in `mux-protocol.ts`); a `StreamTransport` errors its writable once its output ended on its own (peer FIN, failure, abort), so `writer.closed` settles, at the cost of Dart's silent drop of later writes; the `IdentifyOptions.holderKey` comment (ignored with a bearer credential, as in Dart; the QUIC binding still refuses it).
- The suites found no wire or state bug in the frame mux. They found two performance problems, both fixed: every channel built a `ReadableStream` and a `WritableStream` at both ends (now made on first use), and Node's `WritableStream` drains its queue in quadratic time beyond some 16K queued writes (the mux now keeps at most 1024 writes in flight and queues the rest itself).
- Adaptations, each commented where it applies: credit is returned per read (design §12 item 10), so batch sizes differ from Dart's; Dart's synchronous peers become `AnsweringTransport`, whose answer the mux reads after the write.
- `coreRuntime().mux` already opens `FrameMuxConnection`s since PR #17; the node tests' `toy-mux.ts` can give way to the real mux.

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
