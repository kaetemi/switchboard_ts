# Hand-off: node and client (work-split thread 3)

Repo `kaetemi/switchboard_ts`, integration branch
`claude/switchboard-ts-design-s5r9na`. Thread branch
`claude/switchboard-ts-node-client-wum9sv`. History is granular, the
integration branch is merged in with merge commits, and nothing is squashed.
Running notes from the thread are in `../notes/node.md` (layout, fixtures,
decisions). This file is the state at hand-off.

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

Nothing of this thread is unmerged: main and the integration branch are at
59ee1e3, and the thread branch adds nothing to them.

## Test status

`npm run check` and `npx tsc -p tsconfig.test.json` are clean: 1517 tests
pass, with 50 todo across the repo. Per file of this thread, against Dart (counts are Dart `test(` calls; each TS file's header says what is left out):

| TS file | Dart file | Ported |
|---|---|---|
| `switchboard.test.ts`, `identity-node`, `outgoing` | the matching `*_test.dart` | ported in #6; Talk requests go over plain channels, and `talk() fails until the Talk layer lands` flips when the seam is swapped |
| `selection.test.ts` | `selection_test.dart` (16) | 12; the slot routing cases wait for slot routing |
| `resolver.test.ts` | `resolver_test.dart` | the `StaticResolver` slot-table half waits for slot routing |
| `local-connections.test.ts` | `local_connections_test.dart` (16) | 12; the Talk, PeerSet and proxy cases are not ported yet (PeerSet and proxy now exist), and the TCP/WS variants belong in the Node integration tests |
| `memory-endpoints.test.ts` | `memory_endpoint_test.dart` (13) | 11; requests go over plain channels until Talk is wired, and the proxy case moved to `proxy.test.ts` |
| `client/reconnecting-client.test.ts` | `reconnecting_client_test.dart` | all |
| `peer-set.test.ts` | `peer_set_test.dart` | 35 of 35 |
| `proxy.test.ts` | `proxy_test.dart` (66) | 29 pass, 16 todo, slot groups missing (below) |
| `relay.test.ts` | `relay_test.dart` (32) | 29 pass, 4 todo |
| `mesh.test.ts` | `mesh_test.dart`, `test/integration/*` | not started |

## Remaining from the work split

Talk (#11), naming (#13), the frame mux (#14) and QUIC (#15) have landed
since this work was written. Items 1 to 4 are unblocked now.

1. **Swap the Talk seam.** `src/core/switchboard/talk-layer.ts` is interim:
   - `talkOver` throws UNIMPLEMENTED
   - `installBulkRouting` is a no-op
   - `refuseBulk` and `bulkRoutes` stand in for Talk's

   Wire it to `TalkChannel` and to `installBulkRouting`,
   `markBulkChannel`/`isBulkChannel`, `registerBulkTarget`,
   `lingerBulkTarget`, `followBulkTarget` and `routeBulk` from
   `src/core/talk/index.ts`. Delete the interim `bulkRoutes` and point
   `proxy.ts` at Talk's functions; the interim names map one to one. Then:
   - enable the Talk todos in `proxy.test.ts` (16) and `relay.test.ts`
     (bulk payloads)
   - add the client's Talk variants (`openTalk`, `openPersistentTalk`)
   - turn PeerSet's three `openTalk` UNIMPLEMENTED assertions into Talk
     round trips
   - restore the Talk echo in `peer-set.test.ts`'s worker
   - flip `talk() fails until the Talk layer lands` in `switchboard.test.ts`
     and port the Talk, PeerSet and proxy cases of `local-connections` and
     the request cases of `memory-endpoints`
2. **Slot routing** (deliverable 5, second half). The naming hand-off leaves
   it to this thread or theirs; it is still open. Pieces:
   - `src/core/switchboard/slot-layer.ts`: `slotCarrier` returns undefined.
     Wire it to `SlotChannel` and `reopenAtSlotOwner` (naming
     `slot-channel.ts`, `slot-reopen.ts`).
   - Add `openChannelToSlot` and a slot-routed `selectAndConnect` /
     `resolveSlotOwner` to `Switchboard`.
   - Port the slot cases of `selection.test.ts` and `resolver.test.ts`.
   - Make `StaticResolver` a `SlotResolver` (Dart: `defineSlots`, `setSlot`,
     `removeSlots`, `locateSlot`).
   - Port the proxy groups "proxyHandler with slot tables" and "proxyHandler:
     channels without a shard slot" (about 20 tests; Dart `proxy_test.dart`
     from line 1338). They use subclasses of the Dart `_Locating` and
     `_Gated` resolvers.
   - Port the `openTalkToSlot` relay todo and the naming thread's
     `slot-routing.test.ts` routing todos (9 + 3). Replace that file's
     test-local `openChannelToSlot` with the node's.
   - `proxy.ts` already checks slot tables through `isSlotResolver`, which
     now comes from `naming/slot-reopen.ts`.
3. **`MeshNode`** (`mesh-node.ts`, Dart `mesh.dart`, `mesh_test.dart`) is not
   started. It needs `NamingClient` and `NamingResolver` (#13 landed). Once
   it exists:
   - The relay tests can move from `test/support/node/fake-mesh.ts` to a
     real naming service, or keep the fake.
   - Port the 2 PeerSet todos in `relay.test.ts`: a relayed peer, and "a
     node that can neither broker nor relay".
   - MeshNode owns the credential renewal schedule; `NamingClient.renew()`
     waits on a core `Credential.decode`, per the naming hand-off.
4. **Run the node suites over the frame mux.** Since #17, `core-runtime.ts`
   builds a `FrameMuxConnection` (#14 landed), and
   `test/unit/switchboard/core-runtime.test.ts` checks one channel end to
   end over it. The node suites still run over `test/support/node/toy-mux.ts`
   through the harness's `testRuntime`. Next:
   - Run them over the frame mux as well, which may surface frame-mux
     bugs, since the core hand-off says much of it is read-only verified.
   - Enable the todos that need real LIMITS or flow control: the proxy's
     pipe tests, "a send rejected by delayed LIMITS" and "a subframe over
     the receiving peer limit".
5. **The Node and web entry points** (deliverable 5, first half) are not
   started. `src/node/switchboard.ts` and `src/web/switchboard.ts` don't
   exist yet:
   - Node: `listenTcp`, `listenWebSocket`, `acceptWebSocket`, and `tcp`,
     `ws`, `wss` dialers, plus `quic`/`https` through the QUIC thread's
     `connectQuic`/`listenQuic` via a `quic` option.
   - Web: a connect-only node.
   - Both build on `SwitchboardRuntime`/`SwitchboardBinding` in `runtime.ts`
     (Dart `SwitchboardBinding`). They are blocked on `src/node/transport/`
     (TCP, `ws`), which the core thread owns (step 5) and has not landed. The
     QUIC hand-off notes that a raw QUIC listener needs
     `alpn: [switchboardAlpn]` and that the node side doesn't fill it in.
6. **Integration tests** `test/integration/node-*.test.ts` over memory, TCP
   and WebSocket, and the Dart `test/integration/*` node-level ports:
   `mesh_test`, `embedded_test`, `brokered_workers_test`,
   `cohosted_mesh_test`, `fleet_consumer_test`, `relayed_workers_test`.
   The acceptance interop (Dart peer against a TS node and back, over TCP and
   WebSocket) needs item 5 and the interop thread's TS peer.

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
  - `stepping-clock.ts`: `SteppingClock`, `fake_async`-style `elapse`
  - `channels.ts`: `Pull`, `Frames`, `nextText`, `framesOf`, `pattern`
  - `fake-mesh.ts`: `FakeMesh`/`MeshResolver`, a naming stand-in with
    `CONNECT` brokering that needs `broker` rights

  The naming thread has its own `TestClock` and fake Talk in
  `test/support/naming/`, which overlap with these.
- **Test pitfalls.**
  - `toEqual` on large `Uint8Array`s is very slow under vitest; use a byte
    comparison.
  - A JS default parameter swallows an explicit `undefined`. Use `null` for
    "any" in helpers.
- **Lint.** `npm run check` runs `biome check .`, which fails on a nested
  `biome.json` if an agent worktree is left under `.claude/worktrees/`.
  Remove merged worktrees before checking.
- **Conventions.**
  - Biome, 100 columns, single quotes, `type` imports, no `any` or TS enums,
    `.ts` import extensions, the file header, and JSDoc on every export.
  - Commits carry the `Co-Authored-By` and `Claude-Session` trailers and no
    model names.
  - Real merges, not squashes.
