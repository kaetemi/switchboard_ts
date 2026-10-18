# Talk thread notes

Running notes of the Talk port (`src/core/talk/`). Information for the
lead and the other threads, not instructions.

## Status

- PR #1 (frame codec, vectors): merged.
- PR #4 (contract changes: item windows, creditTimeout, MuxChannelCarrier,
  TalkMessage.frame): merged.
- Deliverable 2 (channel, message, request, stream, with bulk plumbing):
  implemented on `claude/switchboard-ts-talk-u0a6uj`; Dart test ports in
  progress over a fake mux pair, PR to follow.
- Deliverable 3 (bulk tests) and 4 (`forwardMessage` in
  `talk-forward.ts`, ported; tests in progress): same branch.

## Public surface so far (`src/core/talk/index.ts`)

- `TalkKind` (const object: `message` 0, `streamItem` 1, `abort` 2,
  `extend` 3), `talkKindName(kind)`.
- `TalkFrame` (init object `TalkFrameInit`), `TalkExtension`
  (`{deadline, renew}` in ms, undefined = unchanged).
- `TalkBulkReference` (u32 number from 1, u48 length or undefined),
  `TalkBulkOpen` (`_bulk` OPEN payload: u48 parent id, u32 number;
  `TalkBulkOpen.isBulk(address)`, `TalkBulkOpen.decode(address)`).

## Codec decisions

- Decode errors are `ProtocolError` (status 32); encode refusals are
  `RangeError` (Dart `ArgumentError`). Reader `ByteFormatError` and
  `Name.fromBytes` `RangeError` are both mapped to `ProtocolError` in
  `TalkFrame.decode`.
- EXTEND payload values: ms, `Math.ceil`, below 1 ms becomes 1 ms, clamp to
  `u32` (about 49.7 days). Negative or NaN throws `RangeError`. Both
  undefined gives the empty payload (plain keep-alive).
- Decoded payload is a view on the input bytes (as Dart's `sublistView`).

## Planned contract changes (first commit of PR 2, for the lead to review)

The Dart Talk surface has members the TS contract lacks. To be proposed in
PR 2 with rationale:

- `TalkOptions.creditTimeout` (default 60000 ms): bound on waiting for item
  credit (Dart `creditTimeout`).
- Item windows: `RequestOptions.window`, `TalkStream.window`/`grant(n)`,
  `TalkMessage.window`/`whenCredit()`/`setCreditTimeout(ms)`,
  `ReplyRequestOptions.window`, `replyStream` credit timeout.
- `onExtend` callback argument `{deadline, renew}` in ms.
- `TalkAbortError` as a concrete class in `contract.ts` (an interface plus
  a class of the same name in another module clashes in the barrel).
- A `MuxChannelCarrier` interface in the mux contract for slot channels
  (Dart `MuxChannelCarrier`), so Talk can run over a binding's slot.

## Bulk routing hook (PR 3)

`installBulkRouting(connection)` (idempotent, WeakSet) installs the
`adoptIncoming` filter (decode `ChannelAddress`, `_bulk` goes to the
parent's Talk layer) and the `admitAfterGoAway` predicate (`_bulk`, payload
of at least 10 bytes, parent `channelWithId` open). `TalkChannel` installs
it on construction; the node/switchboard threads may call it per
connection. `BulkRoutes` is not exported from the barrel; import from
`../talk/talk-bulk.ts` inside the package.

## Channel port decisions (deliverable 2)

- Raw channel: `TalkRawChannel = MuxChannel | MuxChannelCarrier |
  TalkFrameChannel` (duck typed). A `TalkFrameChannel` is `{readable,
  writable}` plus optional `close(status)` and `done` (a status-closable
  channel); frames written to it go out before its status close.
- Clock and logger: `TalkOptions.clock`/`logger`, else the mux
  connection's, else the system clock and the `Switchboard.Talk` logger.
- Delivery is pull based (`talk-delivery.ts`): `for await` over
  `messages`/`items` takes delivery and returns credit; `serve(handler)`
  is the push form (handler per message, a throw or rejection answers
  ABORT INTERNAL and marks the request cancelled). Leaving the loop
  cancels the delivery; what it held is dropped and its credit returned.
- `forwardMessage` takes over a message's credit only when called inside
  a `serve` handler (synchronously); a message taken by `for await` was
  handed over before the loop body runs.
- Bulk routing: a `TalkChannel` installs `installBulkRouting` on its mux
  connection itself (WeakSet idempotent).

## Test fixtures other threads may reuse

- `test/support/talk/memory-mux.ts`: `memoryMuxPair()`, an in-memory
  `MuxConnection` pair implementing `mux/contract.ts` (cost = length + 16,
  credit batched at half the window, manual credit, grant, CLOSE confirmed
  by the peer, `openAfter` ordering, bulk chunking, adoptIncoming and
  admitAfterGoAway, `ordered: false` with held channels for reordering).
- `ManualClock.elapseAsync(ms)` (with `flushAsync`, `settle`, `nextAt`)
  in `src/testing/manual-clock.ts` flushes promise jobs between timers
  (the `fake_async` model); it replaced the Talk tests' `FakeClock`. The
  node's `SteppingClock` and the naming tests' `TestClock` can merge into
  it.
- `test/support/talk/frame-mux-peers.ts`: Talk over the real frame mux
  (`frameMuxPair`, `FrameMuxPeers`, `WireLog`, `RawMuxPeer`).
