# Review: naming and sharding

Independent review of `src/core/naming/**` and the slot-routed half of the node (`openChannelToSlot`,
`openTalkToSlot`, `resolveSlotOwner`, `slot-layer.ts`, `MeshNode`'s sharded half), against
`docs/design.md` 5, 6.8, 7, 12, `dart-design.md` (naming, sharding) and the Dart sources at 6ed87b4.
Each finding has an expected-fail case (`test.fails`) under `test/unit/naming/review-*.test.ts`; a fix
flips it to `test`. No source was changed. `npm run check`: 3007 passed, 7 expected fail.

## Findings, most severe first

1. **`SlotChannel.writable` drops writes after the end instead of rejecting** (data loss).
   `src/core/naming/slot-channel.ts:194-212`: `write` returns without sending once `canSend` is false,
   and swallows the `FAILED_PRECONDITION` of a held subframe dropped at the end. Design 12.8 says a
   write to a channel that can no longer send rejects and errors the writable; `MuxChannel.writable`
   does, the slot channel keeps Dart's `sink.add`. Path: `source.pipeTo(slot.writable)` on a channel
   from `openChannelToSlot`; the owner closes it `RELOCATED` (never retried) or anything else ends it;
   the pipe drains the rest of the source into nothing and resolves as if delivered.
   Test: `review-slot-channel.test.ts`, "a write to a SlotChannel writable after the channel ended
   rejects too" (control beside it for `MuxChannel`). Fix: let `write` be `send` (throw or reject as
   `send` does), drop the early return and the swallow, and fix the doc comment. Dart: drops silently
   by design (its sink); the TS deviation item 8 should cover the carrier too.

2. **Cancelling `SlotChannel.readable` before the first read does not reach the mux channel** (hang).
   `slot-channel.ts:170-182`: the readable's `cancel` only cancels `#reader`, which exists after the
   first pull; before it (and for a replacement opened after the cancel) the mux channel's readable
   stays live, so what the owner sends is held, with its credit, instead of discarded (design 12.10).
   Path: a client that only sends on a slot channel and cancels the readable at once; the owner's
   acknowledgements fill the window and its `send()`s never resolve. Test: "cancelling a SlotChannel
   readable before reading discards what arrives too" (1160 bytes buffered; the `MuxChannel` control
   has 0). Fix: remember the cancel; cancel `#current.readable` when there is no reader, and the
   replacement's in `#retry`. Dart: a cancel needs a listener, whose subscription is on the current
   channel, so the first case is TS-only; a replacement opened after a cancel is buffered in both.

3. **`MeshNode.leave()` can stay pending for ever** (hang). `src/core/switchboard/mesh-node.ts:950`
   awaits every `gate.close()`, which awaits `SlotLifecycle.unload` of each slot still served, with no
   bound, although design 6.7 says each step is bounded by `leaveTimeout` and `leave` never rejects.
   Path: the naming service is unreachable at leave (nothing released, so every slot is still in the
   gate) and an `unload` blocks (a flush to a store that is down too); `await using mesh` never exits.
   Test: `review-mesh-node.test.ts`, "leave settles within its bounds when an unload hangs and the
   naming service is gone". Fix: bound the close with `#within(…, leaveTimeout)` (the unload goes on;
   the lifecycle keeps those slots busy as it already does). Dart: the same (`Future.wait` over
   `gate.close()`); dart-design.md does not say leave waits for unloads.

4. **`NamingService.close()` publishes no `SLOT` changes on `slotEvents`** (wrong behaviour).
   `src/core/naming/naming-service.ts:310`: `emitSlotItem` checks `#closed`, set first in `close()`,
   where `#publish` (line 1554) checks the closed signal, aborted at the end, so the DOWNs of the
   dropped owners reach `events` but the slots they free never reach `slotEvents`. Dart's
   `setEntry` adds to `events` until close closes the controller at its end. Test:
   `review-naming-service.test.ts`, "close publishes the slot its owner frees on slotEvents, as it
   does on events" (the `events` control passes). Fix (one line): guard on
   `this.#closedSignal.signal.aborted`. Dart: emits them; TS-only.

5. **`SlotChannel` is not async iterable** (ergonomics). `slot-channel.ts:80`: no
   `[Symbol.asyncIterator]`, so `for await (const subframe of await node.openChannelToSlot(...))`
   throws a TypeError, while the `MuxChannel` it "behaves like" is (design 5, 12.12). Test: "a
   SlotChannel is async iterable over its subframes too". Fix: `[Symbol.asyncIterator]()` returning
   `readSubframes(this.readable)`, as `FrameMuxChannel` does. Dart: n/a (a `StreamChannel`).

6. **The waiting slot and registration calls take no `AbortSignal`** (ergonomics). Design 5 puts a
   signal on every operation that waits; `NamingClient` has one on `renew`, `connectTo`, `migrate`,
   not on `locate`, `claim`, `release` (`naming-client.ts:510-542`, queued in
   `naming-client-slots.ts` `call`), `register` (832), `defineSlots`, `declareHolding`, nor do
   `MeshNode.claimSlot`/`releaseSlot`/`publishSharded` or `SlotGate.claim`/`release`. A `LOCATE` can
   wait an `ASSIGN`, the hold and the backoffs (up to `handoverTimeout`). Test:
   `review-naming-client.test.ts`, "locate takes an AbortSignal, as the waiting calls of design
   section 5 do" (passes one through a cast; ignored). Fix: an options `signal` passed to
   `startRequest` in `#pumpCalls` (a queued call leaving the queue on abort). Dart: `Future.timeout`.

7. **Naming event listeners run inside the client's step; not listed in design 12** (docs).
   `naming-client.ts:1476-1511`: `on('event')` listeners are called synchronously from `#onSynced`,
   before the table is marked synced, so a listener of a resync DOWN sees `isSynced` false and a table
   mid-update; Dart's broadcast stream delivers after the step (`isSynced` true). The same holds for
   `slotEvent` and `NamingService.on`. Design 12.16 lists this only for `PeerSet`. Test: "an event
   listener sees the resync it is told about as done, as a Dart listener does" (the `events` iterable
   control sees it done). Fix: list it in 12.16 (or deliver `on()` after the step).

## Unverified suspicions

- Shared with Dart, documented in dart-design.md: when the naming channel is lost after a `DRAIN`
  succeeded and the new owner was assigned, the old owner's client resumes the slot locally
  (`ClientSlots.onLost`), its gate serves the queued channels on the drained state until its re-claim
  is refused after the reconnect: a split-brain window whose writes are lost. A design question for
  the wiki, not a port defect.
- Allocator arithmetic (`naming-service-slots.ts:1797`, `#rebalance`): `weight * assigned -
  load * total` passes 2^53 once the summed capacities pass about 2^37 at 65 536 slots (u32 weights);
  Dart's int64 does not. Only affects extreme capacities; no test.
- `SlotGate` validates neither `forwardGrace` nor `drainTimeout` (Dart neither, but a `Duration` is
  never NaN): NaN makes a `ManualClock` timer never fire; the doc's "RangeError for negative bounds"
  covers the queue bounds only.
- `NamingService.table`, `slotTables` and `SlotTable.entries` hand out the live `Map` typed
  `ReadonlyMap` (Dart wraps them in `UnmodifiableMapView`); a cast mutates the service.
- A malformed `PHASE` item ends `NamingClient.migrate`'s iteration and cancels the `MIGRATE` (the
  generator's `finally`); Dart adds an error event and the stream goes on.

## Fine, but worth knowing

- Endpoint normalisation (interop note: Dart normalises, TS carries as written): nothing in the
  naming layer is keyed on the endpoint text. The only effects are record equality (a re-registration
  that only respells an endpoint publishes an UP where a Dart service would not) and, in the node
  layer, `PeerSet`'s `record.endpoints.includes(endpoint)` (`peer-set.ts:1171,1215`), which treats a
  respelling as a move. The node's pool and `isOwnEndpoint` normalise. `endpointProblem` refuses
  every character Dart would escape, so a TS-accepted endpoint never grows past 255 bytes in Dart.
- The port is function-for-function with Dart in the service's slot manager (claim, release, locate,
  migrate, allocator, backoffs, settle, resume, abandon), the client's slot half (serve, restore,
  reclaims, call queue) and the gate; the CONNECT relay, RENEW (lifetime, payload adoption), expiry
  timers, `MeshNode` renewal (2/3, quarter-of-the-rest retries) and `NamingResolver` match too.
- A seeded state fuzz (80 seeds of 150 random claims, releases, locates and migrations with refused
  `ASSIGN`/`DRAIN`, dropped links and naming service restarts with a hold and a settle window, over
  three real `NamingClient`s in virtual time) always quiesced consistent: the owner's served epoch
  equals the table, no stale server, no lock left, no timer after close. Not committed (probe only).
- Every timer in the layer is on the injected clock and cleared on close (read for connectTo,
  resolve, the gate's grace, drain and idle timers, the service's backoffs, settle, resume and LOCATE
  waits; probed for a slot handler that never answers when the client closes; the fuzz too).
- Outside the layer: `sleep()` in `src/core/util/deferred.ts:50`, exported from the core barrel,
  uses the global `setTimeout`, not a `Clock`; nothing in `src/` calls it.
