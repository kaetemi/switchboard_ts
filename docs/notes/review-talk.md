# Review: the Talk layer

An independent correctness review of `src/core/talk/**` against the Dart reference (`lib/src/talk/*.dart`), `docs/design.md`
sections 5, 6.5, 7 and 12, and the Talk section of `dart-design.md`, on the head f61c3e1. Every finding has a test in
`test/unit/talk/review-*.test.ts` written as `test.fails`, so `npm run check` stays green; flipping one to `test` shows its
failure, and is what its fix does. Each was checked flipped and run 10 times. No source was changed.

The port renders Dart closely: the request and responder state machines, the timeout regime, item windows and credits, the
lanes, bulk admission and `forwardMessage` match function by function. The defects are where TS reads asynchronously what Dart's
mux delivers in wire order, and in the read-on allowance added for QUIC bursts. Ranked by realistic impact: finding 1 reproduces
over the QUIC binding; findings 2 and 3 lose data but reproduced only over the memory mux, whose channels interleave at
microtask granularity, as the native QUIC provider's batched event dispatch can (`native-provider.ts`, `drain`).

## Findings

### 1. Over unordered channels, `maxUnclaimedBulk` admits about 4400 unreferenced bulk channels per Talk channel

- **Severity:** hang or leak (resource bound).
- **Where:** `talk-channel.ts:1873`: `#unreadReferences` adds `room - #readHeld`, and `room` (`#readOnRoom`, 1615) is a whole
  `initialWindow` while nothing was read on.
- **Path:** a QUIC peer opens a Talk channel, sends nothing on it, then opens unreferenced bulk streams naming it. Talk adopts
  each one unclaimed, since `16 + 65536 / sendCost(11)` references "may still be read on": 2443 over the memory mux, 4385 over
  the binding (4-byte records). Each holds its window until `bulkOpenTimeout`, and the peer can open more. Measured over the
  memory mux: 2443 held, 146 MB buffered, for one idle parent. Design 13.6 names the stack's stream limit as the backstop; for a
  dialler that is 65536 (`quic-dial.ts:157`), against 128 at a listener.
- **Tests:** `review-bulk-admission.test.ts`: "bulk channels beyond maxUnclaimedBulk for an idle parent are refused, unordered
  channels" (40 opened, 0 refused, 24 expected), and "over the QUIC binding, bulk streams beyond maxUnclaimedBulk for an idle
  parent are refused". The ordered case is the passing control.
- **Fix:** beyond the bound, admit an unclaimed channel only while those beyond it hold no more than `reassemblyBudget`
  together, as `#attachBulk` does for payloads waiting for the budget, or cap the read-on share of the allowance; either must
  keep the bursts of `talk-quic.test.ts` passing.
- **Dart:** refuses beyond 16. Design 12 item 14 lists "the frames it may still read on", but not that with nothing on the
  parent this lifts the bound about 270 times; 13.6 says such a peer "is refused once the parent's unread frames can no longer
  carry them", which an idle parent never reaches. The tests assert Dart's bound; if the lead picks another, the count changes.

### 2. A graceful close of the parent loses payloads that arrived whole before it

- **Severity:** wire or data loss.
- **Where:** `talk-channel.ts:2232`: `#endBulk` abandons every BulkIn still in `_bulkIns`. `#peerEnded` (2541) waits only for
  reassemblies (2571). Over unordered channels `#linger` returns at once (2628) when `#bulkSettled` (2680) sees every bulk
  channel `closed`, though `closed` does not mean its reader drained it.
- **Path:** a peer sends a bulk channel whole (OPEN, DATA, CLOSE), the BULK message, then the parent's CLOSE without waiting
  for the bulk channel's confirmation. That is allowed ("a peer need only have sent them", dart-design.md): a raw peer or a relay
  does it, a Talk sender waits. If the mux takes it all in one go, Talk sees the parent end before the bulk channel's reader
  reaches its end. (a) A streamed payload whose consumer has not opened `bulk` yet is abandoned, ordered or not: its message is
  still delivered, but `message.bulk` throws "the bulk payload is read already" and the bytes are gone. (b) Over unordered
  channels, a reassembled payload still being read is dropped with its message. The frame mux kept the readers in step in every
  probe (it reads one frame per await), and the in-memory QUIC provider did not reproduce it either.
- **Tests:** `review-graceful-close.test.ts`: "a streamed payload sent whole before the parent closed is still readable after
  the end, unordered channels" (and "..., ordered channels"), and "a reassembled payload sent whole before the parent closed is
  delivered (unordered channels)". The frame-mux reassembled case is the passing control.
- **Fix:** before `#terminate` on a graceful end (both orderings and `#linger`), wait until every BulkIn on a closed channel
  has run `#onEnd` and its reassembly has finished; make `#bulkSettled` test that instead of `channel.state`.
- **Dart:** not affected: its mux delivers in wire order, so a bulk channel that ended before the parent's CLOSE has already
  left `_bulkIns` when Talk sees that CLOSE.

### 3. A bulk channel routed between the parent's end and `#linger` is lost

- **Severity:** wire or data loss (a race window of a few microtasks).
- **Where:** `talk-channel.ts:2547` awaits `raw.done` before `#linger` registers the target (`lingerBulkTarget`,
  `talk-bulk-routes.ts:147`). In that window `routeBulkChannel` finds no open parent and no lingering target, and holds the
  channel as one whose parent has not arrived (`talk-bulk-routes.ts:217`). Nothing re-routes it once the target is registered.
- **Path:** over QUIC, the bulk stream of a message that arrived before the parent's graceful close is processed just after
  the binding ended the parent, while Talk's reader still lags and then awaits `done`. The bulk channel is refused
  FAILED_PRECONDITION after `heldBulkTimeout`, or confirmed OK if the sender closed it first (the sender sees success), and the
  message is dropped when `bulkOpenTimeout` ends the linger 10 s later.
- **Test:** `review-graceful-close.test.ts`: "a bulk channel routed just after its parent ended, before Talk lingers, is still
  handed to the message waiting for it (unordered channels)".
- **Fix:** on an unordered connection, keep the parent's route target after a graceful end until `#peerEnded` lingers or
  declines, and have `lingerBulkTarget` dispatch what `PendingParents` holds for that id.
- **Dart:** the same structure (`_peerEnded` awaits `done` before `_linger`, `talk_channel.dart:2562-2582`); whether Dart's QUIC
  delivery can hit the window is unverified.

### 4. A plain message's `signal` never aborts, though the wiki says closing the channel aborts it

- **Severity:** ergonomics or docs.
- **Where:** `talk-message.ts:1597`: `#markCancelled` runs for requests only. The wiki claim: `docs/wiki/Channels-and-Talk.md:123`.
- **Path:** a handler passes a plain message's `signal` (a streamed upload, say) to its work; the channel closes; the work does
  not stop.
- **Test:** `review-signal.test.ts`: "the signal of a plain message aborts when the channel closes, as the wiki says". The
  request case is the passing control.
- **Fix:** say "of a request" in the wiki, or abort every delivered message's signal when the channel ends. contract.ts and
  `onCancel` ("never for non-requests") side with the code.
- **Dart:** has no signal, and `onCancel` never completes for non-requests. The wiki does not rule on it.

## Unverified suspicions

- `#admission` keeps entries that have left it (failed RESOURCE_EXHAUSTED, or dropped) until they reach its head. Behind a head
  that a full budget stalls (the items of a stream request nobody reads, kept alive by EXTEND), bulk messages on `messages`
  fail and return their credit at once, so the array grows without bound. Dart's `_admission` has the same shape. Observing it
  needs a heap measurement.
- `#mustDefer` (`talk-channel.ts:983`): if a carrier's mux channel were replaced while frames wait for the old one's LIMITS,
  the flush is skipped (`this.#mux !== mux`) and later frames defer without listening on the new one. Unreachable over the frame
  mux, where MOVED follows LIMITS.
- `bulkChunkSize` 0 makes `BulkOut.#pump` send empty subframes forever. `TalkOptions` is not validated, as in Dart.

## Fine, but surprising

- The hand-off's admission fix holds over ordered channels: 20 plain frames, then 20 unreferenced OPENs, leave exactly 16 open
  once the frames are read.
- `forwardMessage` relays a remote abort byte for byte, including non-UTF-8 reasons, and EXTEND payloads raw; its
  local-failure reasons match Dart's.
- No unhandled rejections when a bulk source throws mid-stream (async iterables and ReadableStreams, through `send`, `request`,
  `reply`, `replyItem` and `replyStream`, also via a proxy).
- No timers left after close, abort, the peer's close or abort, or loss of the connection, with requests, cancels, windows and
  transfers in flight, under both orderings. The exception: an unordered channel lingering after its connection died waits out
  the bulk timeouts, as Dart does.
- Credit is conserved (`heldBytes` 0 at rest) across echoes, bulk replies, streams left with `break`, unread streamed
  payloads, aborts and ordered answers.
- The `ordered` guarantee lasts one microtask hop: code awaiting `response` directly runs before the next message, a wrapping
  async function does not (Dart has the same caveat).
- `#unreadReferences` divides by `sendCost(11)`, but the smallest BULK frame is 14 bytes: the allowance is about 25% generous.
- `startRequest`'s JSDoc says a bulk payload over a non-mux channel "rejects UNIMPLEMENTED"; it throws synchronously, as in Dart
  and as `send` documents.
