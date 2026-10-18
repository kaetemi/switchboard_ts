# Naming and sharding thread notes

Running notes of the naming and sharding port (`src/core/naming/`).
Information for the lead and the other threads, not instructions.

## Status

- PR 3 (codecs, slot keys, slot tables, vectors) merged.
- On `claude/switchboard-ts-naming-hi15aj` now, PR to follow: `NamingClient`
  (with the client half of slot spaces), `NamingResolver` (implements the
  new `SlotResolver`), `NamingService` (leases, watches, CONNECT, relay,
  dial-back, the service half of slot spaces: allocator, migrations,
  assignment hold), `SlotChannel`, `reopenAtSlotOwner`, and the ports of the
  Dart naming, sharding, client-slot, identity, bulk, resolver and MOVED
  retry tests.
- Integration branch merged in after PRs 7, 9 and 10 (no conflicts).
- Blocked: `SlotGate` and the proxy half of slot routing need the real
  `TalkChannel` and the node's proxy forwarding; integration tests over the
  memory transport and the Dart interop both ways need the node's Talk layer
  (it throws UNIMPLEMENTED until Talk lands).

## Slot routing and SlotGate (branch claude/slot-routing)

- `SlotGate`, `SlotLifecycle`, `SlotGateState`, `SlotGates` in
  `src/core/naming/slot-gate.ts` (the design's layout puts the gate in the
  naming layer; Dart has it in `switchboard/`). Dart's named parameters
  of `load`/`drain` are options objects (`SlotLoadOptions`,
  `SlotDrainOptions`); the gate takes an injectable `clock` and `logger`
  (default the node's). Ports of `slot_gate_test.dart` (53) and
  `slot_gate_credential_test.dart` (6), and the two sharding-client cases
  that waited for the gate.
- The node routes by slot (`openChannelToSlot`, `openTalkToSlot`,
  `resolveSlotOwner`, slot-routed `selectAndConnect`) and the proxy
  retries MOVED through `slot-layer.ts`; `isSlotResolver` moved to
  `src/core/switchboard/resolver.ts`; `StaticResolver` is a
  `SlotResolver`. `slot-routing.test.ts` runs the node's routing in full.
- `SlotChannel` follows Talk's bulk route to its replacement itself
  (`followBulkTarget`, Dart `BulkRoutes.follow`); the `onReplaced` option
  is gone.
- The naming tests run over real `TalkChannel`s
  (`test/support/naming/talk-link.ts`); the memory pair stays for the four
  `naming.test.ts` cases that need synchronous delivery.
  `naming-bulk.test.ts` runs over nodes and checks `isBulk` again.
- The node-side identity cases run over real nodes in
  `naming-identity-nodes.test.ts`, with a test-local stand-in for
  `MeshNode.join`/`publish` until `MeshNode` lands.

## Contract change for the lead (own commit 4d82321)

- `SlotResolver extends Resolver` in `src/core/switchboard/contract.ts`:
  `slotTable(type)`, `slotOwner(type, slot)`, `slotOwners(type)`,
  `slotEvents`, `locateSlot(type, slot)`. `isSlotResolver(resolver)` now
  lives beside `isBrokeringResolver` in `src/core/switchboard/resolver.ts`.

## Seams other threads will meet

- `reopenAtSlotOwner(switchboard, header, rejectedBy, moved, options)` is the
  MOVED retry Dart shares between `openChannelToSlot` and the proxy. It uses
  `switchboard.openSelected`, `resolver`, `clock`, `logger`,
  `slotRefreshTimeout`.
- `SlotChannel` (a `MuxChannelCarrier`) calls Talk's `followBulkTarget`
  when a replacement takes over, as Dart calls `BulkRoutes.follow`. Its
  held bytes default to `defaultMuxOptions.initialWindow` (64 KiB) as in
  Dart.
- `TestClock` (`test/support/naming/test-clock.ts`) ported fake_async
  (`await clock.elapse(ms)`); with the node thread's `SteppingClock` it was
  folded into `ManualClock.elapseAsync` in the third round.
- `NamingClient.renew()` is not implemented yet: it needs a core
  `Credential.decode`.
- `NamingCredentialIssuer` is the service's hook for CONNECT credentials.

## Public surface so far (`src/core/naming/index.ts`)

- `Procedures`, `Services` (`_ns`, `_relay`, `_bulk`), `maxMetadataLength`,
  `maxConnectIntentLength`, `maxU32`.
- Records and events as functions over the contract's `ServiceRecord` and
  `ServiceEvent` interfaces (`serviceRecord`, `encodeServiceRecord`,
  `decodeServiceRecord`, `serviceRecordsEqual`, `upEvent`, `downEvent`,
  `decodeUpEvent`, `decodeDownEvent`, `encodeServiceEvent`, ...). Classes of
  the same name would clash with the contract interfaces in the barrel.
- Request classes: `RegisterRequest`, `RegisterResponse`,
  `UnregisterRequest`, `WatchRequest`, `ConnectRequest`, `ConnectResponse`,
  `DialBackRequest`, and the sharding set (`SlotSpace`, `SlotsRequest`,
  `HoldingRequest/Response`, `ClaimRequest/Response`, `ReleaseRequest`,
  `LocateRequest/Response`, `MigrateRequest`, `PhaseItem`, `AssignRequest/
  Response`, `DrainRequest`, `ForwardRequest`, `ResumeRequest`, `SlotEntry`,
  `SlotItem`, `MovedStatus`).
- `SlotMode`, `SlotState`, `MigrationPhase` const objects with name helpers.
- `fnv1a32`, `slotForKey`, `slotForText`; `SlotTable`, `SlotEvent`.

## Decisions

- Decode errors are `ProtocolError`; encode refusals are `RangeError`
  (Dart `ArgumentError`), matching the Talk thread.
- Endpoints are carried as strings, verbatim. `checkEndpoint` /
  `endpointProblem` is a predicate checked differentially against Dart's
  `Uri.parse` (oracle fixture `test/unit/naming/fixtures/endpoint-oracle.json`,
  1000 rows; script beside it). Over about 39k generated candidates TS never
  accepts what Dart refuses. Known cases where Dart accepts and TS refuses:
  `data:` URIs, ports beyond int64 in simple URIs, dot segments in
  authority-less paths that Dart's simple path keeps.
- Dart SDK finding: `Uri.parse` throws `RangeError` (not `FormatException`)
  for some IPv6 literals such as `[1:2:3:4:5:6:7::9]`, so the Dart record
  decoder can let a non-protocol exception escape on hostile input. Dart
  repo is read-only; reported in the PR description only.
- `MovedStatus.parse` never throws (missing or short fields mean unknown
  owner), as in Dart.

## Review follow-ups not taken (docs/notes/review-naming.md)

The review's unverified suspicions stay open, unreproduced: the
split-brain window when the naming channel is lost after a `DRAIN`
succeeded and the new owner was assigned (the old owner resumes the slot
locally until its re-claim is refused after the reconnect; Dart has it
too, so it is a design question for the wiki, not a port defect); the
allocator's `weight * assigned - load * total`, which passes 2^53 only
once the summed capacities pass about 2^37 at 65 536 slots; `SlotGate`
not validating `forwardGrace` and `drainTimeout` (NaN never fires a
`ManualClock` timer); `NamingService.table`, `slotTables` and
`SlotTable.entries` handing out the live maps typed `ReadonlyMap`, where
Dart wraps them; and a malformed `PHASE` item ending `migrate`'s
iteration (and cancelling the `MIGRATE`) where Dart adds an error and
goes on.

## Questions for the lead

- Review the `SlotResolver` contract commit (above) with the next PR.
