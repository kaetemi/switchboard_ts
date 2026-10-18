# Hand-off: protocol core (transports, mux, identity)

State on 2026-10-18, with the third round's transports and fixtures branch (below); before it, on 2026-10-14, after the continuation's merges. Repo `kaetemi/switchboard_ts`, integration branch `main` (the first phase's PRs went into `claude/switchboard-ts-design-s5r9na`, from which `main` was initialised; the lead merges with real merges, never squash). Porting decisions are in `notes/core.md` §22.

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
- `test/unit/mux/frame-mux-smoke.test.ts`: 4 end-to-end checks over the memory pair (subframes, credit wait, close status, ping, GOAWAY, writable). The Dart suites are ported since (below).

## Done since: steps 3 to 5 of the work split

All merged into `main` (the integration branch since the first hand-off) from topic branches:

**Step 3, the frame mux tests (`claude/mux-tests`).**

- Ported, one TS file per Dart file under `test/unit/mux/`, over `support.ts` (the port of `mux_harness.dart`, every connection on a `ManualClock` stepped with `elapseAsync`): `mux.test.ts` (`mux_test.dart`), `mux-flow.test.ts`, `mux-limits.test.ts` (connection groups added to the codec half), `mux-schedule.test.ts`; the "connection identity" group of `identity_test.dart` is in `test/unit/identity/identity.test.ts`, and `mux_talk_test.dart` in `test/unit/talk/mux-talk.test.ts` and `test/integration/talk-frame-mux.test.ts`. `test/vectors/mux.test.ts` has the "connection" group of `mux_vectors_test.dart`; `test/vectors/binding-protocol.test.ts` the IDENT gating tests. `frame-mux-api.test.ts` covers the TS-only API (streams, signals, `await using`, the hooks, the write backlog).
- The TCP cases of the flow and limits suites run on the loopback in the unit files; those of `mux_test.dart` (TCP, WebSocket) are `test/integration/mux-tcp.test.ts` and `mux-websocket.test.ts`. Not ported: the Dart mirrors that count the scheduler's private turn lists (the observable half is checked).
- Also fixed there: `identify()` hands out a pre-caught promise; `MuxChannel` is async iterable over its subframes (contract, all channel classes, `readSubframes` in `mux-protocol.ts`); a `StreamTransport` errors its writable once its output ended on its own (peer FIN, failure, abort), so `writer.closed` settles, at the cost of Dart's silent drop of later writes (design section 12, item 11); the `IdentifyOptions.holderKey` comment (ignored with a bearer credential, as in Dart and in the QUIC binding).
- The suites found no wire or state bug in the frame mux. They found two performance problems, both fixed: every channel built a `ReadableStream` and a `WritableStream` at both ends (now made on first use), and Node's `WritableStream` drains its queue in quadratic time beyond some 16K queued writes (the mux now keeps at most 1024 writes in flight and queues the rest itself).
- Adaptations, each commented where it applies: credit is returned per read (design section 12, item 10), so batch sizes differ from Dart's; Dart's synchronous peers become `AnsweringTransport`, whose answer the mux reads after the write.

**Step 4, identity (`claude/identity`).** `credential.ts`, `scope.ts`, `credential-issuer.ts` (`hmac`, `ed25519`, `ed25519FromSeed`, `issue`), `credential-verifier.ts` (keys by id, `addIssuer`, `removeKey`, `verify`, `cached`, the LRU cache), `holder-key.ts` (`Ed25519HolderKey`, exported under its own name beside the contract's `HolderKey` interface), the credential vectors C1 and C2 and the negative ones, `identity_test.dart` ported, `NamingClient.renew()` on top. The node and QUIC fixtures use real credentials (`claude/quic-identity`); `test/support/identity/stub-verifier.ts` remains for the mux IDENT tests, which check IDENT verification on its own.

**Step 5, transports (`claude/transports`).** `websocket-transport.ts` (the global `WebSocket`), `src/node/transport/tcp.ts`, `ws-server.ts`, `ws-client.ts`, `connect.ts` (`connectTransport` for `tcp`, `ws`, `wss`, `mem`; other schemes are UNSUPPORTED), the loopback integration tests, and the ports of `transport_test.dart` and `web_socket_server_test.dart`. The interop raw probe against a TS server waits on the TS peer (`claude/ts-peer`).

**Third round, transports and fixtures (`claude/transports-fixtures-medczm`).** The items the hand-off's section 7 listed for this area, each fixed with a test (detail in `notes/core.md` §22, last entry):

- `TcpTransport.bufferedOutputBytes` is 0 from the abort on (a destroyed socket's `writableLength` lingers until its handle closes), and `acceptedOutputBytes` no longer counts the dropped bytes once it does.
- The memory pair errors the other side's writable when its readable ends (CONNECTION_LOST, or the FRAME_TOO_LARGE that closed the pair on its behalf), so its `closed` settles; later writes reject, as with the stream transport.
- The WebSocket server passes Dart's fragment cap (`maxMessageFragments`, 4096) to `ws`'s `maxFragments` (`ws` 8.21 or later) and reads its error as FRAME_TOO_LARGE; `ws` closes with 1008 where Dart closes with 1009. Both `ws` fragment todos are ported, "costs about its size" for its deliveries only.
- One mux harness (`test/unit/mux/support.ts`; the identity copy is gone), no `SteppingClock` or `TestClock` (`ManualClock.elapseAsync` everywhere), and the unit "forged IDENT" case flips a byte of a valid credential (`forged()` in `test/support/node/seeded-identity.ts`).

**Left in this area:** nothing from the hand-off's list. For the lead: the `ws` peer range is `^8.18.0`, where an older `ws` has no fragment cap; `^8.21.0` would make it certain.

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
- **Unknown schemes.** `connectTransport` refuses a scheme that is not a frame transport's with UNSUPPORTED; the node's `connect` refuses a scheme it has no dialer or binding for with UNIMPLEMENTED, as Dart does.

## For a fresh session

- Work on a topic branch `claude/<topic>` in a worktree under `/home/user/wt/<topic>`; push only that branch; the lead merges into `main`.
- Never rewrite merged history.
- The Dart repos are read-only.
- Contracts (`src/core/*/contract.ts`) are the lead's.
- Name tests after the Dart files.
- Biome rules: 100 columns, no new `biome-ignore`.
