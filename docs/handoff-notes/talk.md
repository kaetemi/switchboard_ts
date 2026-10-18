# Hand-off: Talk port (thread 2)

State on 2026-10-14, for the session that picks this up (updated after the integration tests over the frame mux, TCP and the QUIC binding merged from `claude/talk-integration`). Repository
`kaetemi/switchboard_ts`, integration branch `main` (the first phase's PRs went into `claude/switchboard-ts-design-s5r9na`, from which `main` was initialised).
Running notes with the earlier design decisions are in `../notes/talk.md`.

## Merged

* **#1**: the Talk frame codec, the bulk reference and the `_bulk` OPEN payload, with 101 vector tests.
* **#4**: the contract changes. These are item windows, `creditTimeout`, `MuxChannelCarrier` and `TalkMessage.frame`.

## Merged: PR #11

PR #11 was merged into the integration branch with a merge commit, at head 6c4ddfb. It carried work-split deliverables 2, 3 and 4. Its branch, `claude/switchboard-ts-talk-u0a6uj`, holds no commit the integration branch lacks. Nothing is unmerged and no work is in progress.

The source files are all in `src/core/talk/`:

* `talk-channel.ts` (the runtime)
* `talk-message.ts` (the received side)
* `talk-request.ts` (outgoing requests, `Expiry`)
* `talk-stream.ts`
* `talk-delivery.ts` (pull-based delivery, held credit)
* `talk-bulk-transfer.ts`
* `talk-bulk-routes.ts` (the `_bulk` filter: `installBulkRouting` and friends)
* `talk-forward.ts` (`forwardMessage`)
* `talk-util.ts`
* `index.ts`

The tests are 15 files and 328 tests in `test/unit/talk/`. Every test name in the Dart files `talk_test`, `talk_window_test`, `talk_credit_test`, `talk_bulk_test`, `talk_forward_test` and `mux_talk_test` appears verbatim in a TS test or in a `// Not ported` comment. I checked this with a script that greps the Dart names out of the Dart files.

The test fixtures are in `test/support/talk/`:

* `memory-mux.ts`: `memoryMuxPair()`, an in-memory `MuxConnection` pair with real credit semantics. Keep it.
* `frame-mux-peers.ts`: Talk over the real frame mux: `frameMuxPair()` (two `FrameMuxConnection`s on the memory pair, bulk routing installed, closed after the test; a `WireLog` of what both sides write; a transport that queues frames ahead), `FrameMuxPeers` (the frame-mux `Peers`), `RawMuxPeer`/`rawMuxPair()` (a mux whose peer hands it raw frames all at once).
* `frame-link.ts`: `Pair`, `RawPeer`, `FrameEnd`.
* `helpers.ts`: `Queue`, `Collector`, `serve`, `flush`, the status assertions, and the helpers the files used to copy (`toList`, `first`, `rejection`, `within`, `sleep`, `expectBytes`, `expectSame`, `expectRemote`, `rejectsRemote`, `rawOf`, `only`, `last`).
* `frames.ts`: frames written by hand (credits, stream requests, items, BULK references, `_bulk` open payloads).
* `carrier.ts`: `Carrier`, the `MuxChannelCarrier` stand-in (limit, interceptor).
* `timer-clock.ts`: `TimerClock`, a `ManualClock` that tells its pending delays.
* `bulk-peers.ts`: `Peers`, sources, `until`, `PulledSource`.

The Talk tests' own clock is gone: they use `ManualClock.elapseAsync` (and `flushAsync`, `settle`, `nextAt`) from `src/testing/manual-clock.ts`. The node's `SteppingClock` and the naming tests' `TestClock` can merge into it the same way (their owners' call).

## Integration tests (`test/integration/talk-*.test.ts`)

* `talk-frame-mux.test.ts` (45): `mux_talk_test` over the frame mux, requests, streams, aborts, cancels, timeouts and EXTEND, item windows, ordered answers, GOAWAY and its grace, a transport frame limit, keep-alive. Time on a `ManualClock`.
* `talk-frame-mux-credit.test.ts` (16): `talk_credit_test` over the frame mux.
* `talk-frame-mux-bulk.test.ts` (41) and `talk-frame-mux-bulk-rules.test.ts` (30): all of `talk_bulk_test` over the frame mux, the adapted tests as in Dart (LIMITS, the GOAWAY drain, the receive mark), 'over memory' run again with transport latency (the 'over TCP' adaptation), and the bulk admission at the bound.
* `talk-frame-mux-forward.test.ts` (53): forwarding chains over frame mux pairs, 'bulk payloads' and 'forwarding credit' (with the real `SlotChannel`).
* `talk-quic.test.ts` (17, one `test.fails`): Talk over `QuicBindingConnection` on the in-memory provider: unordered channels, bulk streams overtaking or lagging their references and their parent, lingering with drain holds, `_bulk` continuations after GOAWAY.

* `talk-tcp.test.ts` (9): the 'over TCP' group of `talk_bulk_test` over the frame mux on loopback TCP (`connectTcp`, `listenTcp`), `mux_talk_test` over TCP, and a stalled reader holding its channel at the window.

## Not ported, with the reason in the test file

* **`talk_forward_test` '1 proxy, synchronous transport' and '2 proxies, synchronous transport' (22 tests), plus 3 loop cases in 'item windows':** a Talk channel reads through a ReadableStream, which never delivers inside a send. The non-sync variants are fully ported.
* **`talk_test` 'onError(null) on the subscription logs instead':** this is about Dart's `StreamSubscription.onError`, which TS has no equivalent for.
* **`talk_test` 'synchronous transport' (11 tests):** ported, but adapted. The peer's cancel crosses the reply on the wire instead of arriving inside the send. They use a carrier that can intercept or refuse each frame. The held-cancel path (`#sending`/`#heldCancel` in `IncomingMessage`) is reachable only through re-entrant forwarding, and no test reaches it.

## Deviations from Dart

All of these are documented in the code.

* **Delivery is pull based.** A paused subscription is a consumer that is not pulling. A pull already pending when the consumer pauses still takes one more message.
* **`serve(handler)` is the guarded listener.** If the handler throws or rejects, the request is answered ABORT INTERNAL and marked cancelled.
* **A throw in a `for await` body does not answer the request it was given.** The request waits for the responder timeout, as with Dart's `await for`.
* **`messages` and a stream's items are iterated once.** A second iteration, or `serve` after an iteration, throws.
* **`forwardMessage` takes over a message's frame credit only when it is called synchronously inside a `serve` handler.** A message taken by `for await` was handed over, and its credit returned, before the loop body runs.
* **Error types differ.** `ArgumentError` becomes `RangeError` or `TypeError`, and `StateError` becomes `Error`. `onExtend` gets one `{deadline?, renew?}` object.
* **Bulk receive allows for Talk running behind the mux.** Talk reads its channels through ReadableStreams, while the Dart mux delivers synchronously in wire order. These are the fixes in commit 9c2919d, `talk-channel.ts`:
  * `streamHold` counts the bulk channel's `heldBytes + bufferedBytes`, plus its window while it is open.
  * `_adoptBulk` refuses only beyond `maxUnclaimedBulk` plus the references that the parent's unread frames could carry: those the mux channel buffers (`bufferedBytes`) and those delivered toward Talk's reader and not read yet (the channel's `heldBytes` less what Talk took from its reader and holds). It then recounts after each frame read. (The second half closed the gap where a peer exactly at the bound got a spurious RESOURCE_EXHAUSTED, reachable through a slot channel over a transport that queues frames ahead.)
  * On an ordered connection, a graceful close waits for the reassemblies of bulk channels that had already closed.
* **On a frame channel that is not a mux channel, a status close waits for the last write.** This is so a channel abort sent just before the close is not lost. A sink that never accepts the write would hold that close.

## Fixes the integration tests found

* **Bulk admission at the bound** (the `_adoptBulk` allowance above, commit "Talk: count frames on their way to the reader in bulk admission"; test 'a reference on its way to Talk frees its place for the next OPEN, over a slot channel').
* **After this side's GOAWAY, `installBulkRouting` admits a `_bulk` continuation for a parent that is open or that the connection retains for its drain** (Dart's QUIC binding: open or retained for its drain). Before, a late bulk stream of a closed parent Talk waited for was refused GOING_AWAY (test "after this side's GOAWAY, the late bulk channel Talk waits for is admitted, and the GOAWAY waits for it" in `talk-quic.test.ts`). The node thread fixed the same on main at the same time (by `retainedForDrain`, which also covers a proxy's pipe); the merge kept that version.

## Closed: the QUIC bulk burst gap

Over the QUIC binding, which reads a channel's stream only as far as what it delivered was consumed or granted ahead (as Dart's does), a message Talk holds for its payload held every frame behind it in the QUIC stack, and the bulk streams of a burst arrive beside their references: past `maxUnclaimedBulk` Talk refused them RESOURCE_EXHAUSTED, even without a lag over the native provider. Talk now reads on for itself on unordered connections (`#readOn` in `talk-channel.ts`): while bulk channels wait unclaimed and fewer than `maxUnclaimedBulk` references wait for their channels, it reports the frames it holds consumed ahead of their delivery, up to one `initialWindow`, and `_adoptBulk`'s allowance counts the frames it may still read on. The binding and the contracts are unchanged; design section 13, item 6, has the options and why a fixed grant ahead is wrong. The Dart reference keeps the gap.

## Remaining

1. **The interop thread's Talk scenarios** (request, stream, abort, bulk over 32752 bytes, forward) come with the TS peer (`claude/ts-peer`, in flight). Talk itself is ready for them; the node's Talk seam (`src/core/switchboard/talk-layer.ts`) is wired to `TalkChannel` since `claude/node-talk`.
2. **Fixture follow-ups (optional):** the memory mux does not model a GOAWAY drain, a per-side LIMITS or `maxSubframeLength`, or the receive high-water mark; the frame-mux integration tests cover those.

## Conventions this thread followed

* Commits are granular, and integration merges are merge commits, never squashes (the user's instruction).
* Commits end with `Co-Authored-By: Claude <noreply@anthropic.com>` and the session link. No model identifier appears in commits, PR text or code.
* The Dart repositories are read-only references: `lib/src/talk/*.dart` is the line-by-line source for the port.
