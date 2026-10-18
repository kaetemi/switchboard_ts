# Node and client thread notes

Running notes of the node and client port (`src/core/switchboard/`,
`src/core/client/`, `src/node/switchboard.ts`, `src/web/switchboard.ts`).
Information for the lead and the other threads, not instructions.

## Status

- Branch `claude/switchboard-ts-node-client-wum9sv` (merges the integration
  branch in with merge commits; history kept granular).
- Deliverables 1 and 2 (the core node): merged (#6).
- Deliverable 3 (Backoff, ReconnectingClient, persistent channels): #10.
- Next: 4 (proxy, relay, forwarding, PeerSet, MeshNode), 5 (Node and web
  entry points, slot-routed opens).

## Layout

- `switchboard.ts`: the runtime-neutral `Switchboard`. Dispatch, outgoing
  policy and the pool live in the class (they share its private state), so
  the separate `dispatch.ts` and `outgoing-policy.ts` of the work split are
  not separate files.
- `runtime.ts`: `SwitchboardRuntime` `{ mux, transportPair,
  memoryEndpoints, dialers }` and `SwitchboardBinding` (Dart
  `SwitchboardBinding`: `schemes`, `dial`, `normalise`). `core-runtime.ts`
  gives the core default: `memoryTransportPair({ delay: 0 })`,
  `MemoryEndpoints`, no dialers, and a mux factory that throws
  UNIMPLEMENTED until the frame mux lands (thread 1 step 3), when it
  becomes `new FrameMuxConnection(...)`.
- `talk-layer.ts`: the seam to Talk. `talkOver` throws UNIMPLEMENTED and
  `installBulkRouting` is a no-op until the Talk thread's channel and
  `installBulkRouting(connection)` land; then this file imports them.
- `endpoints.ts` (own-listener matching, binding default normalisation),
  `waits.ts` (bounded and abortable waits), `broadcast.ts` (the
  `connections` and resolver event fan-out), `generic-status.ts`,
  `identity-wait.ts`, `incoming-channel.ts`, `channel-policy.ts`,
  `relay-config.ts`, `resolver.ts`.
- Pool keys come from the transport layer's `parseEndpoint(...).key`
  (Dart-equal for every frame-mux scheme: `tcp://h:0` without a port, as
  Dart's `Uri(port: 0)` prints).

## Test fixtures (`test/support/node/`)

- `toy-mux.ts`: a small in-memory `MuxConnection` over a `FrameTransport`
  (OPEN, data, CLOSE, GOAWAY, LIMITS, NONCE/IDENT with `TestVerifier`).
  Not a frame-mux port: once `FrameMuxConnection` lands the harness can run
  the suites over it too.
- `seeded-identity.ts`: credentials from fixed seeds, holder keys and a verifier.
- `memory-table.ts`: a per-test `mem://` registry (same rules and id form
  as `MemoryEndpoints`); the harness fails a test that leaks a listener.
- `harness.ts`: `node()` closed after the test, `tagged`/`tagOf`,
  `rejectsWith`, `until`, `pump`.
- `stepping-clock.ts`: `SteppingClock`, a `ManualClock` whose `elapse(ms)`
  moves timer to timer and settles microtasks between (Dart `fake_async`
  `elapse`), and `settle()`. Used by the client tests; `PeerSet` and mesh
  tests will use it too.

## Client (`src/core/client/`)

- `ReconnectingClient(connector, options)`: `connector` is a
  `TransportConnector` function (frame mux over it) or a
  `ConnectionConnector` object `{ connect(options, { signal }) }` (Dart
  `ReconnectingClient.connections`), the QUIC/WebTransport entry:
  `{ connect: (options, { signal }) => connectQuic(url, webTransport(), { options, signal }) }`.
- `backoff.ts` is internal (not in the barrel), shared with `PeerSet`.
- `persistent-channel.ts`: `PersistentCore` behind `PersistentChannel` and
  `PersistentTalk`.
- Logs as `Switchboard.Client`.

## Contract changes

- `IncomingChannel.remote: string | undefined` (own commit 34e2406): the
  Dart `IncomingChannel.remote` the handlers and tests read.

## Questions for the lead

- Settled by the lead: `BrokeringResolver` is in `contract.ts`,
  `ServiceAddressLike` and `toServiceAddress` in the address layer.
- The node logs as `Switchboard.Node`.
- `serviceRecord` comes from the naming layer (`naming-protocol.ts`); the
  node's own copy was removed after #3 landed.

## Dart behaviour decisions

- `StateError` cases become plain `Error`; `ArgumentError` becomes
  `RangeError` or `TypeError`.
- A signal only abandons the caller's wait: a shared pooled attempt goes on
  for the other callers; a late connection of an abandoned `dial` is sent
  GOAWAY.

## Slot routing (branch claude/slot-routing)

- `Switchboard.openChannelToSlot`, `openTalkToSlot`, `resolveSlotOwner`
  and slot routing in `selectAndConnect` (Dart `_slotOwner`,
  `_recordsOf`); `slotRefreshTimeout` bounds every LOCATE on the node's
  clock.
- `slot-layer.ts` builds the slot channels: the node's (retry through the
  node's resolver, may relay, holds one initial window) and the proxy's
  carrier (no byte bound, own endpoints excluded, LOCATEs counted).
- `StaticResolver` is a `SlotResolver` (`defineSlots`, `setSlot`,
  `removeSlots`, `locateSlot` from the configured table).
- Ported: the slot cases of `selection_test`, the slot half of
  `resolver_test`, the proxy's slot-table groups (22), the relay's
  `openTalkToSlot`. The flaky authorize-timeout case runs the proxy on a
  manual clock.


## Review of the node layer: unverified suspicions

The review (`docs/notes/review-node.md`) left three suspicions untested,
kept here for a later look: `PeerCore.readopt` reports
`online`/`offline` synchronously and then calls `update`, so a listener
that closes the set on that event would get `updated` after `removed`
(contrived); a `Logger` that throws inside the node's catch paths
(`#dispatchAll`, `#holdForIdentity`, the proxy's `forward`) would turn a
`void` promise into an unhandled rejection, where `MeshNode` guards its
logger; and the Node entry's `close()` closes the QUIC listeners while
the core's GOAWAY waits run, which interop suggests is harmless (it
reads GOING_AWAY over QUIC, so the shim seems to keep accepted
sessions), unchecked.
