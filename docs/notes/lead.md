# Lead thread: integration log and follow-ups

_Kept by the lead (design and integration) thread. Integration branch: `claude/switchboard-ts-design-s5r9na` in `kaetemi/switchboard_ts`._

## Merged

| PR | Thread | What | Merged head |
| --- | --- | --- | --- |
| #1 | Talk | Talk frame codec, bulk reference and `_bulk` open payload, 101 vector tests | 04362ea |
| #2 | QUIC | records, record reader, HELLO, exporter helpers, fake QUIC provider, 77 tests | e7c04da |
| #3 | Naming | `_ns` and sharding codecs, slot keys, slot tables, endpoint check with a Dart oracle fixture | 47fb444 |
| #4 | Talk | contract: item windows, `creditTimeout`, `MuxChannelCarrier`, `TalkMessage.frame` | f378d19 |
| #5 | Core | stream framing codec and transforms, stream transport over `ByteInput`/`ByteOutput`, memory pair, endpoints, `MemoryEndpoints` | f0e7263 |

| #6 | Node | the core `Switchboard` node: dispatch, policies, pool, resolvers, identity on connections, brokering, `mem://` listeners; `IncomingChannel.remote` in the contract | 0c88ec6 |

After #6: 25 test files, 580 tests, lint and typecheck clean. The design's transport section now matches the port (FRAME_TOO_LARGE is a `SwitchboardError`, the Dart codec names).

Lead follow-up commit after #6: `BrokeringResolver` moved into `src/core/switchboard/contract.ts` (the naming thread's `NamingResolver` implements it), `isBrokeringResolver` stays in `resolver.ts`; `ServiceAddressLike` and `toServiceAddress` moved into `src/core/address/service-address.ts` (exported from the address barrel and the core). Node thread: merge the integration branch before the next PR and import them from there. Merges are merge commits (never squash), by the user's instruction.

- PR #7 interop harness: merged ecf4f54 (merge commit, head 5bdf8f3). Integration branch: 592 tests + 13 todo, both tsc configs and biome clean; CI interop job ran the Dart cells in required mode (204 passed, 190 TS cells skipped until the TS peer exists). The interop thread fixed the test-config type errors before merge (`tool/dart-peer.d.mts`, typed peer protocol shapes).

- PR #9 core codecs, IDENT, binding base: merged 1ba6818 (merge commit, head 75cf303). 695 tests + 13 todo on the merged tree, both tsc configs clean. Decision taken: `BindingConnection` keeps Dart's scope (drain holds, neutral diagnostics); the identity exchange, `adoptIncoming` and `admitAfterGoAway` stay per binding (QUIC: exporter binding and held control messages; frame mux: held frames). The work split's placement is superseded.

- PR #10 reconnecting client: merged 0a96930 (merge commit, head 665ed42). 742 tests + 13 todo, both tsc configs clean; CI incl. interop green. `ReconnectingClient` takes a `TransportConnector` or a `ConnectionConnector` `{ connect(options, { signal }) }`; design example updated (lead commit on the integration branch).

- PR #8 QUIC binding + native provider: merged 63aeabb (merge commit, head 56ef733). Stand-ins swapped for the core's modules; `native/` holds the N-API crate and `sbq-vendor/` (pin in `native/sbq-vendor.commit`, checked by `tool/sync-sbq.mjs --check` in the new `native` CI job). 853 tests + 116 skipped (native files skip without the addon) + 13 todo locally; CI green incl. native and interop. Design updated for the pin file location.

- PR #12 oracle for mux frame/control codecs: merged 034ab8b (merge commit, head 91d8986). Interop project 241 passed in CI; no TS/Dart codec disagreement.

- Wrap-up 2026-10-14: PRs #11 (cd08e02), #13 (dd8c205), #14 (1ba6e94), #15 (6cf6beb), #16 (1a608ed) merged with merge commits; lead commits 5a991bb (GOAWAY contract comment, design deviations) and f6119fc (webTransport/quicConnector docs, u32 close codes). docs/handoff.md written; main initialised from the integration branch (same head). Then PR #17 (7975285, frame mux in the core runtime, pushed by the node thread after #16) and PR #18 (0c0f92e, branch sync, no content) merged and main moved with them. 1517 tests, 125 skipped, 50 todo; every check green.

## Contract changes pre-approved

The Talk thread's planned contract changes (talk.md, "Planned contract changes") are fine as described; put them in the first commit of the PR and the lead merges them with it: `TalkOptions.creditTimeout` (60 000), item-window members on `RequestOptions`, `TalkStream`, `TalkMessage` and `ReplyRequestOptions`, `onExtend({deadline, renew})` in ms, `TalkAbortError` as a class in `contract.ts`, and `MuxChannelCarrier` in `mux/contract.ts` as the TS form of Dart's `@internal` interface (`channel`, `manualCredit` setter, `consumed`, `send`, `isWritable`, `whenWritable`). The naming thread builds slot channels on it.

## In review (2026-10-14)

- (done, see Merged) PR #8 (QUIC binding, head e076d4e, draft): biome, both tsc configs and 637 unit+integration tests green on the test-merged tree with 06a505f; no conflicts. Holds until the QUIC thread marks it ready (regression cases still coming). Stand-ins to swap once the core thread lands: `binding-connection-stub.ts` (BindingConnection base), `control-stub.ts` (control/IDENT codecs, verifyIdent), `wire-status.ts`; all unexported. `listenQuic` takes a provider `QuicListener`; adopting into a node is the node thread's.

- PR #11 (Talk channel, head f330e8c, draft): pre-review green on the test-merged tree with d8b477d (biome, both tsc configs, 890 tests); no conflicts; all files within the Talk thread's ownership (`src/core/talk`, `test/unit/talk`, `test/support/talk`). Holds until the ordered-answer, bulk transfer and forwarding tests land and the thread marks it ready.

## Follow-ups the lead tracks

- `src/core/quic/wire-status.ts` duplicates the CLOSE/GOAWAY status rules (`checkMuxStatus`, `truncateStatus`, `statusForWire`, `statusFromWire`, `decodeStatusPayload`). When the core thread's `mux-protocol.ts` lands, the QUIC thread swaps its imports and deletes the copy. Not exported from the package meanwhile.
- Core's vector tests (`test/vectors/{mux,identity,exporter-identity,binding-protocol}.test.ts`) read hex inline; register through `wikiBytes` ids as a follow-up.
- Vector id registration: the Talk and QUIC vector tests name vectors by their `wiki-vectors.json` ids; once the interop thread's `test/support/vectors.ts` lands, both register through it and the coverage test goes live.
- Open: `main` is still empty; initialised from the integration branch at the first green end-to-end state, with the user's go-ahead.

## Decisions since the design was published

- 2026-10-14: the package is `@kaetemi/switchboard` (the user's npm scope); prebuilt QUIC addons as `@kaetemi/switchboard-quic-<target>`. Renamed on the integration branch in package.json, the docs, the wiki pages and the doc comments. Threads: use the new name in doc comments and examples; imports inside the package are relative and unaffected.

- 2026-10-14: the QUIC shim is duplicated (vendored copy with the `rlib` line), no upstream change; a dedicated shim repository shared by the Dart and TS packages is the intended later direction, so the copy stays separable.
