# Hand-off: Talk port (thread 2)

State on 2026-10-14, for the session that picks this up. Repository
`kaetemi/switchboard_ts`, integration branch `claude/switchboard-ts-design-s5r9na`.
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

* `fake-channel.ts`: `fakeMuxPair()`, an in-memory `MuxConnection` pair with real credit semantics. Keep it.
* `fake-clock.ts`: `FakeClock`, with `elapse` flushing promise jobs between timers.
* `frame-link.ts`: `Pair`, `RawPeer`, `FrameEnd`.
* `helpers.ts`: `Queue`, `Collector`, `serve`, and the status assertions.
* `bulk-peers.ts`

## Not ported, with the reason in the test file

* **`talk_bulk_test` 'over TCP':** there is no mux over TCP yet. The same tests run 'over memory' on the fake mux.
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
  * `_adoptBulk` refuses only beyond `maxUnclaimedBulk` plus the references that the parent's unread (`bufferedBytes`) frames could carry. It then recounts after each frame read.
    * Known gap: a frame already delivered to Talk's reader but not yet processed is not counted. A peer sitting exactly at the bound can therefore get a spurious RESOURCE_EXHAUSTED.
  * On an ordered connection, a graceful close waits for the reassemblies of bulk channels that had already closed.
* **On a frame channel that is not a mux channel, a status close waits for the last write.** This is so a channel abort sent just before the close is not lost. A sink that never accepts the write would hold that close.

## Remaining from the work split

1. **Register the Talk vector ids through `test/support/vectors.ts`.** Use `wikiVector`/`wikiBytes` in `test/vectors/talk.test.ts`, which still reads hex inline, so that `test/vectors/coverage.test.ts` checks the Talk tables. The support file has landed, so this is unblocked.
2. **Integration tests (`test/integration/talk-*.test.ts`):**
   * Talk over the real frame mux on the memory pair, once thread 1's frame mux connection lands. It has not landed yet: `src/core/mux/` has the codecs and `BindingConnection` only.
   * Then the bulk 'over TCP' group.
   * Talk over `QuicBindingConnection` can be done now with `test/unit/quic/harness.ts`. That gives independent channels, the `ordered: false` paths and bulk lingering.
3. **The interop thread's Talk scenarios** (request, stream, abort, bulk over 32752 bytes, forward) need the TS peer and the node. Talk itself is ready for them.
4. **Fixture follow-ups (optional):**
   * Move the helpers the workers wrote locally into `test/support/talk/`: `toList`, `first`, `rejection`, `until`, a `MuxChannelCarrier` stand-in, `TimerClock`, frame builders, and a hand-fed source.
   * Give `FakeClock` the pending-timer delays.
   * The fake mux does not model a GOAWAY drain, a per-side LIMITS or `maxSubframeLength`, or the receive high-water mark.
   * A core `ManualClock.elapseAsync` would let `FakeClock` go.

## Conventions this thread followed

* Commits are granular, and integration merges are merge commits, never squashes (the user's instruction).
* Commits end with `Co-Authored-By: Claude <noreply@anthropic.com>` and the session link. No model identifier appears in commits, PR text or code.
* The Dart repositories are read-only references: `lib/src/talk/*.dart` is the line-by-line source for the port.
