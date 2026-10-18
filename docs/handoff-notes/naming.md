# Hand-off: naming and sharding (thread 4)

State on 2026-10-14, for the session that picks this up, after the
continuation's merges. Repository `kaetemi/switchboard_ts`, integration
branch `main` (the first phase's PRs went into
`claude/switchboard-ts-design-s5r9na`, from which `main` was initialised;
the first thread branch was `claude/switchboard-ts-naming-hi15aj`). The running notes, with
the earlier decisions and the seams other threads meet, are in
`../notes/naming.md`.

## Merged

* **#3**: the naming and sharding wire codecs (`naming-protocol.ts`), slot
  keys (`slot-key.ts`), `SlotTable` (`slot-table.ts`), and their vectors and
  unit tests. Endpoints are checked against a Dart `Uri.parse` oracle fixture
  (`test/unit/naming/fixtures/endpoint-oracle.json`).

## Merged: PR #13

PR #13 (head `claude/switchboard-ts-naming-hi15aj`) was merged into the
integration branch on 2026-10-14. Before merging, the branch had the
integration branch merged in up to PRs 7, 9 and 10. Locally, lint, typecheck, `npm test`,
`bun run test:unit` and the build all pass, with 959 passing, 9 skipped and
25 todo. Nothing on the thread branch is unmerged, and no work is in progress. Restart
the branch from the integration branch before new work. The test ports
were done in parallel worktrees and merged into the branch with real merge
commits.

The source files, all in `src/core/naming/`:

* `naming-client.ts`: `NamingClient` (register, watch, the mirrored table,
  reconnect, `connectTo`)
* `naming-client-slots.ts`: the client half of slot spaces (`SlotHandler`,
  `AssignResult`)
* `naming-resolver.ts`: `NamingResolver`, which implements `SlotResolver` and
  `BrokeringResolver`
* `naming-service.ts`: `NamingService` (leases, watches, CONNECT, relay,
  dial-back, identity scopes, expiry and RENEW)
* `naming-service-slots.ts`: the service half of slot spaces (`SlotManager`:
  the allocator, backoffs, the settle window, holder grace, migrations, the
  assignment hold)
* `slot-channel.ts`: `SlotChannel`, which holds what is sent until the first
  channel is accepted and replaces a channel once on MOVED
* `slot-reopen.ts`: `reopenAtSlotOwner` (the MOVED retry that Dart shares
  between `openChannelToSlot` and the proxy) and `isSlotResolver`

The contract change is its own commit, 4d82321, for the lead to review:
`SlotResolver extends Resolver` in `src/core/switchboard/contract.ts`.

The tests are in `test/unit/naming/`, with groups and titles kept in Dart's
order:

* `naming.test.ts`: 71 of 71 tests of `naming_test.dart`
* `sharding.test.ts`: 63 of 63 of `sharding_test.dart`
* `sharding-client.test.ts`: 28 of 30 of `sharding_client_test.dart`
* `naming-identity.test.ts`: 27 of 34 of `naming_identity_test.dart`
* `naming-bulk.test.ts`: 2 of 2 of `naming_bulk_test.dart`
* `naming-resolver.test.ts`: 12 of 12 of `naming_resolver_test.dart`
* `slot-routing.test.ts`: the MOVED group of `slot_routing_test.dart`, 14 tests

The test support is in `test/support/naming/`:

* `memory-talk.ts`: an in-memory Talk pair built to the landed Talk contract
* time: `ManualClock` with `await clock.elapseAsync(ms)`, the fake_async
  port (the thread's own `TestClock` was folded into it in the third round)
* `harness.ts`, `sharding-harness.ts`: ports of the Dart harnesses
* `fake-identity.ts`: fake credentials, issuer, verifier and naming
  connection

## Done since: everything the work split left open

Merged into `main` (the integration branch since the first hand-off) from topic branches:

1. **`SlotGate`, `SlotLifecycle`, `SlotGates`** (`src/core/naming/slot-gate.ts`, Dart `switchboard/slot_gate.dart`) on `claude/slot-routing`, with `slot-gate.test.ts` (53 of `slot_gate_test.dart`) and `slot-gate-credential.test.ts` (6 of `slot_gate_credential_test.dart`); the two `sharding-client.test.ts` tests once marked `// Not ported: SlotGate` run.
2. **Slot routing in the node** (`claude/slot-routing`): `Switchboard.openChannelToSlot`, `openTalkToSlot`, `resolveSlotOwner` and slot-routed `selectAndConnect`, `src/core/switchboard/slot-layer.ts` wired to `SlotChannel` and `reopenAtSlotOwner`, `StaticResolver` as a `SlotResolver`, the proxy's slot groups; `slot-routing.test.ts` has the routing group and the Talk and proxy MOVED cases, over the node's `openChannelToSlot`. `isSlotResolver` moved to `src/core/switchboard/resolver.ts`.
3. **Real Talk:** the suites run over real `TalkChannel`s (`test/support/naming/talk-link.ts`, an in-memory frame link), the fake pair (`memory-talk.ts`) staying only where a test needs synchronous delivery; `SlotChannel`'s replacement is followed by Talk's bulk routes (`followBulkTarget`). The suites that need nodes also run over the frame mux (`test/unit/naming/over-frame-mux/`).
4. **`NamingClient.renew()`** on `claude/identity` (with `Credential.decode`); MeshNode's renewal schedule is in `test/unit/switchboard/mesh.test.ts`.
5. **Integration tests:** `test/integration/naming-mesh.test.ts` (`naming_mesh_test.dart`, over TCP, WebSocket and `mem`), and the whole Dart sharding suite in `test/integration/sharding/` with `cluster.ts` (`claude/sharding`, together with `MeshNode.publishSharded`, `claimSlot`, `releaseSlot`, `gates` and the slot hand-over of `leave`).
6. **The node-side identity tests** of `naming_identity_test.dart` that need real nodes are in `naming-identity-nodes.test.ts` (brokered connections, dial-back GOAWAY, the `broker` preconditions) and `test/unit/switchboard/mesh.test.ts`; each place left in `naming-identity.test.ts` says where.
7. **Vector coverage:** the naming, sharding and slot-key vectors name their ids through `test/support/vectors.ts`; `SWITCHBOARD_VECTORS_STRICT=1` passes.

**Left:** interop with the Dart naming service in both directions (the acceptance criterion) comes with the TS peer and the naming and sharding scenarios on `claude/ts-peer`. (`TestClock` is merged into `ManualClock.elapseAsync` since the third round.)

## Known gaps and deviations from Dart

* The fake Talk pair follows Dart's delivery: one frame per microtask,
  `ordered` answers released after the consumer is done with what precedes
  them, a zero responder timeout that EXTEND does not re-arm, and a local
  close that fails requests with the close status (CANCELLED for OK). A close
  by the peer fails requests with UNAVAILABLE, where Dart uses
  CONNECTION_LOST.
* When a fake request declares both a deadline and a renewal, it times out at
  the earlier of the two; Dart uses the later. No test exercises this.
* `isSlotResolver` is a duck-typed guard, now in
  `src/core/switchboard/resolver.ts` beside `isBrokeringResolver`.
* `SlotChannel.close` throws `RangeError` for an application status code, as
  an inlined `checkMuxStatus`. Its held bytes default to
  `defaultMuxOptions.initialWindow` (64 KiB), as in Dart.
* Error types: `ArgumentError` becomes `RangeError` or `TypeError`, and
  `StateError` becomes `Error`. Decode errors are `ProtocolError`.
* `TestClock` and the node thread's `SteppingClock` are merged into
  `ManualClock.elapseAsync` (third round), as Talk's `FakeClock` was.
* Dart SDK finding: `Uri.parse` throws `RangeError` for some IPv6 literals,
  so the Dart record decoder can let a non-protocol exception escape. It was
  reported in PR #3 only, since the Dart repositories are read-only.

## Conventions

* The lead merges with real merge commits. Keep branch history granular,
  with no squash, amend or rebase.
* Work on a topic branch `claude/<topic>` in a worktree under
  `/home/user/wt/<topic>`; the lead merges it into `main`.
* Run `npx tsc -p tsconfig.test.json --noEmit` before every push; CI does not
  run it.
* `npx biome check .` fails while a git worktree sits inside the checkout
  (a nested `biome.json`); the worktrees live under `/home/user/wt/` for
  that reason.
* The Dart repositories `kaetemi/switchboard` and `kaetemi/switchboard_quic`
  are read-only references. Dart behaviour is the contract and the Dart tests
  are the checklist.
