# Hand-off: node and client (work-split thread 3)

Repo `kaetemi/switchboard_ts`, integration branch `main` (the first
phase's PRs went into `claude/switchboard-ts-design-s5r9na`, from which
`main` was initialised; the first thread branch was
`claude/switchboard-ts-node-client-wum9sv`). History is granular, merges
are merge commits, and nothing is squashed. Running notes from the thread
are in `../notes/node.md` (layout, fixtures, decisions). This file is the
state on 2026-10-14 after the continuation's merges.

## Merged

- #6: deliverables 1 and 2, the core `Switchboard` node. Covers dispatch,
  registration, accept, the pool, `connect`/`dial`, open methods, identity,
  brokering, `localConnections` and `isOwnEndpoint`.
- #10: deliverable 3, `ReconnectingClient`, `Backoff` and persistent
  channels.
- #16: deliverable 4 except `MeshNode`:
  - `pipeChannels`, `proxyHandler`, `RelayService`, and `forwarding.ts`
    (shared by proxy and relay)
  - `PeerSet`, with all 35 Dart tests
  - the merges of Talk #11, naming #13 and oracle #12
- #17 (opened by the lead from this branch): the core runtime's mux
  factory builds a `FrameMuxConnection`, and `core-runtime.test.ts` checks
  one channel end to end. #18 held the same commit and was merged with no
  content change, so the thread branch head is an ancestor of main.

The first thread branch adds nothing to `main`.

## Merged since: the rest of deliverables 4 and 5

All on `main` (the integration branch since the first hand-off), from topic branches:

- `claude/node-talk`: `src/core/switchboard/talk-layer.ts` wired to `TalkChannel` and Talk's bulk routes (`installBulkRouting`, `markBulkChannel`/`isBulkChannel`, `registerBulkTarget`, `lingerBulkTarget`, `followBulkTarget`, `routeBulk`); the interim `bulkRoutes` is gone and `proxy.ts` uses Talk's functions. The Talk todos of the proxy, relay, PeerSet, client and QUIC node suites are ported, and every node suite runs over the toy mux and over the frame mux (`test/unit/switchboard/over-frame-mux/`, `test/unit/client/over-frame-mux/`), including the todos that needed real LIMITS and flow control.
- `claude/transports`: the entry points, `src/node/switchboard.ts` (`listenTcp`, `listenWebSocket`, `acceptWebSocket`, `listenQuic`, `listenWebTransport`; the `tcp`, `ws`, `wss` dialers; `quic` and `https` through the `quic` option or a QUIC listener's provider; `listenQuic` fills in the `switchboard` ALPN) and `src/web/switchboard.ts` (connect-only: `ws`, `wss`, `https` with a `quic` provider; `tcp` is UNSUPPORTED), with `test/integration/node-{tcp,websocket,quic,webtransport,local-connections}.test.ts`.
- `claude/mesh`: `MeshNode` (`src/core/switchboard/mesh-node.ts`, Dart `mesh.dart`: `join`, `publish`, `publishRelay`, `unpublish`, `migrateSlot`, `slotOwners`, the `CONNECT` dial-back, the credential renewal, `leave`) and the Dart node-level integration suites: `mesh_test`, `embedded_test`, `brokered_workers_test`, `cohosted_mesh_test`, `fleet_consumer_test`, `relayed_workers_test` (`test/integration/*.test.ts`), plus `test/unit/switchboard/mesh.test.ts`.
- `claude/slot-routing`: `openChannelToSlot`, `openTalkToSlot`, `resolveSlotOwner`, slot-routed `selectAndConnect` (`slot-layer.ts` over `SlotChannel` and `reopenAtSlotOwner`), `StaticResolver` as a `SlotResolver` (`defineSlots`, `setSlot`, `removeSlots`, `locateSlot`), `isSlotResolver` beside `isBrokeringResolver` in `resolver.ts`, the proxy groups "proxyHandler with slot tables" and "proxyHandler: channels without a shard slot", the slot cases of `selection.test.ts` and `resolver.test.ts`, the `openTalkToSlot` relay case.
- `claude/sharding`: `MeshNode.publishSharded`, `claimSlot`, `releaseSlot`, `gates` and the slot hand-over of `leave` (`LeaveReport`), with the Dart sharding integration suite (`test/integration/sharding/`).

## Test status

Every file of this thread ports its Dart file, over the toy mux and over the frame mux; each TS file's header says what lives elsewhere and why:

| TS file (`test/unit/switchboard/` unless noted) | Dart file |
|---|---|
| `switchboard.test.ts`, `identity-node.test.ts`, `outgoing.test.ts` | `switchboard_test.dart`, `identity_node_test.dart` (the TCP and QUIC halves in `test/integration/identity.test.ts`), the outgoing-policy cases |
| `selection.test.ts`, `resolver.test.ts` | `selection_test.dart`, `resolver_test.dart`, with their slot cases |
| `local-connections.test.ts`, `test/integration/node-local-connections.test.ts` | `local_connections_test.dart` (over `mem://`, and over TCP and WebSocket) |
| `memory-endpoints.test.ts` | `memory_endpoint_test.dart` (two cases live in `switchboard.test.ts` and `proxy.test.ts`) |
| `proxy.test.ts`, `relay.test.ts`, `peer-set.test.ts` | `proxy_test.dart`, `relay_test.dart`, `peer_set_test.dart` |
| `mesh.test.ts`, `test/integration/mesh.test.ts` and the other node-level integration files | `mesh_test.dart` and Dart's `test/integration/*` |
| `test/unit/client/reconnecting-client.test.ts` | `reconnecting_client_test.dart` |

**Left:** the acceptance interop (the Dart peer against a TS node and back, over TCP, WebSocket, QUIC and WebTransport) comes with the TS peer (`claude/ts-peer`); `SteppingClock` is merged into `ManualClock.elapseAsync` since the third round; the relay tests keep `test/support/node/fake-mesh.ts` beside the real naming service of the integration suites.

## Deviations from Dart and known gaps

- **Errors.** Dart `StateError` becomes `Error`; `ArgumentError` becomes
  `RangeError` or `TypeError`. Durations are milliseconds.
- **Abort signals.** A signal only abandons the caller's wait. A shared
  pooled attempt goes on for the other callers, and a late connection of an
  abandoned `dial` is sent GOAWAY.
- **Logger names.** The node logs as `Switchboard.Node` (the design doc's
  name; Dart uses `Switchboard.Router` for the node). The proxy and
  forwarding log as `Switchboard.Router`, the relay as `Switchboard.Relay`,
  and the client as `Switchboard.Client`.
- **File layout.** Dispatch, outgoing policy and the pool live in
  `switchboard.ts` (shared private state). The work split's `dispatch.ts` and
  `outgoing-policy.ts` don't exist.
- **Client connectors.** `ReconnectingClient` takes either a
  `TransportConnector` function or a `ConnectionConnector` object
  `{ connect(options, { signal }) }`, the port of Dart
  `ReconnectingClient.connections` and the QUIC/WebTransport entry. On close,
  the client refuses buffered pushed channels before waiting for retiring
  connections, so GOAWAY grace doesn't hold the close.
- **PeerSet behaviour.**
  - Events fire synchronously (Dart's broadcast is async). Three guards
    cover re-entrancy: hold-down is armed before `held` fires, a stale first
    dial is ignored, and nothing is added after close.
  - A rejected resolver `next()` is logged once and following continues; a
    second rejection in a row stops following.
  - `close()` does not await the resolver iterator's `return()`.
  - Events that arrive before `resolve` are folded into the first answer.
  - `peers` is a live `ReadonlyMap` and `online` is an array. Options take
    `clock` and `logger`.
  - The Dart assert in `leave` is dropped.
- **Relay.**
  - Warns when the relay node lacks `muxOptions.requireNamedIdent`.
  - With `allowEndpoints`, records pointing at the relay's own listeners are
    skipped with a warning (`... is this node itself`) and fail as
    UNAVAILABLE.
- **MeshNode.** Options objects (`MeshNodeOptions`, `MeshPublishOptions`,
  `MeshPublishShardedOptions`) instead of named parameters; `published` and
  `brokeredConnections` are arrays; `nextRenewalAt` is ms since the epoch
  (by `wallTime`); `wallTime`, `clock` and `logger` options; it is
  `AsyncDisposable` (`leave()`); `LeaveReport`'s maps are keyed by
  `Name.key` with sorted, frozen slot arrays.
- **Proxy.** A host hint parses to `tcp://host:port`; a bad hint is
  INVALID_ARGUMENT. The TCP dialer is not in the core runtime, so in unit
  tests a hint either relays to `mem` endpoints or fails as configured.

## What a fresh session must know

- **Ownership.** This thread owns `src/core/switchboard/` (except
  `contract.ts`, the lead's), `src/core/client/`, `src/node/switchboard.ts`,
  `src/web/switchboard.ts`, the switchboard and client unit tests, and
  `test/integration/node-*.test.ts`. Contract changes go as their own commit
  with a rationale. `SlotResolver` and `BrokeringResolver` are in the
  contract already.
- **Test fixtures** in `test/support/node/`:
  - `harness.ts`: `node()`, `onCleanup`, `until`, `pump`, `rejectsWith`
  - `toy-mux.ts`: an in-memory `MuxConnection` without flow control
  - `fake-identity.ts`
  - `memory-table.ts`: per-test `mem://` registry; a leaked listener fails
    the test
  - time: `ManualClock` with `elapseAsync` (the `stepping-clock.ts` of
    this thread was folded into it in the third round)
  - `channels.ts`: `Pull`, `Frames`, `nextText`, `framesOf`, `pattern`
  - `fake-mesh.ts`: `FakeMesh`/`MeshResolver`, a naming stand-in with
    `CONNECT` brokering that needs `broker` rights

  The naming thread has its own fake Talk in `test/support/naming/`,
  which overlaps with these. `gated-transport.ts`
  is the port of Dart's `GatedTransport` (a transport whose output
  readiness the test gates).
- **Test pitfalls.**
  - `toEqual` on large `Uint8Array`s is very slow under vitest; use a byte
    comparison.
  - A JS default parameter swallows an explicit `undefined`. Use `null` for
    "any" in helpers.
- **Lint.** `npm run check` runs `biome check .`, which fails on a nested
  `biome.json` if a git worktree sits inside the checkout; worktrees live
  under `/home/user/wt/<topic>` for that reason.
- **Conventions.**
  - Biome, 100 columns, single quotes, `type` imports, no `any` or TS enums,
    `.ts` import extensions, the file header, and JSDoc on every export.
  - Commits carry the `Co-Authored-By` and `Claude-Session` trailers and no
    model names.
  - Real merges, not squashes.
