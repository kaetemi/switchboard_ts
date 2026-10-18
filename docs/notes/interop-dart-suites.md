# Catalogue of the Dart test suites: switchboard and switchboard_quic

Everything here was measured from the files themselves. Nothing was run or changed.

**Test counts** come from `grep -cE "^\s*test\("`, so they count `test(` call sites. Where a test sits in a loop or a vector helper, the number of tests that actually run is given as "≈N runs".

**Totals:**
- **Core** (`/home/user/switchboard/test`): 51,896 lines, 1,468 call sites, 110 of them in `test/vectors`.
- **QUIC** (`/home/user/switchboard_quic/test`): 8,850 lines, 267 call sites.
- **QUIC `tool/`**: 1,366 lines.

**Platform setup:**
- **Core, `/home/user/switchboard/dart_test.yaml`:** no file has `@TestOn`. `dart test` runs on the VM. The `node` preset (`-P node`) runs `test/vectors/`, `bytes_test`, `identity_test`, `name_test`, `status_test`, `reconnecting_client_test` and `talk_window_test`. There is no web preset.
- **QUIC, `/home/user/switchboard_quic/dart_test.yaml`:**
  - The `node` preset runs `vectors_test` and `binding_test` (which pulls in `review_cases.dart`).
  - The `web` preset runs `web_provider_test` and `web_binding_test` in Chrome with `--no-sandbox`.
  - Everything else is `@TestOn('vm')`, apart from the two browser files.

**Column meanings:**
- **Peers:**
  - `codec`: no connection.
  - `mem-pair`: `MemoryTransport.pair`, `StreamChannelController` or a raw peer.
  - `fake-binding`: `FakeConnection`/`FakeBinding`.
  - `fake-provider`: `FakeQuicSession`/`FakeQuicNetwork`.
  - `mem://`: in-process Switchboard nodes through `listenMemory`.
  - `TCP`, `WS`, `QUIC`: real loopback sockets.
  - `FA`: fake_async; the number is how many `fakeAsync` uses the file has.
- **Interop:** `codec` means byte-exact and reusable as data vectors. `2peer` means one side could be Dart and the other TS. `internal` means it depends on Dart internals, the Dart API or fake time.

---

## 1. Core package `/home/user/switchboard/test`

| Path | Lines | Area | Tests | What it covers | Peers | Interop | Platform |
|---|---|---|---|---|---|---|---|
| /home/user/switchboard/test/address_test.dart | 206 | addressing | 17 | `ServiceAddress`: string forms, parsing, maximum u48 instance, equality, range checks. `ChannelAddress`: default instance left out, host-hint length limit, shard 0 and empty host kept, 100 KiB payload, reserved flags rejected, a bad name is a `ProtocolException`, hostile input only throws `ProtocolException`. | codec | codec | VM (no dart:io) |
| /home/user/switchboard/test/binding_bulk_test.dart | 580 | binding seam / bulk | 19 | Bulk channels on independent streams. Covers a bulk channel arriving before or after its parent, the parent already ended, the bulk-number order, and a duplicate number or wrong parity being a protocol error. A graceful close waits for an awaited bulk channel, within limits: the idle timeout cuts a stall and at most 16 ended parents wait. A failed parent cancels its bulk channels. The proxy forwards bulk toward the frame mux, and binding listener endpoints count as the node's own. | fake-binding, FA(5), mem:// | internal (these are QUIC receiver rules that could be retested over real QUIC) | VM |
| /home/user/switchboard/test/binding_drain_test.dart | 90 | binding seam | 3 | `retainForDrain`/`hasDrainHolds`: a gracefully closed parent that still waits for a referenced bulk channel is held for the idle check and GOAWAY admission, within a bound. | fake-binding, FA | internal | VM |
| /home/user/switchboard/test/binding_review_test.dart | 710 | binding seam | 22 | Review regressions: bulk opens for unknown parents expire and share one bounded backlog; the proxy restores bulk-open order; Talk switches to bulk at the binding's subframe boundary; reordered bulk opens; empty subframes stall a stopped reader; one blocked channel leaves another writable; GOAWAY still allows a bulk continuation. Node lifecycle: binding errors become UNAVAILABLE, close waits for a late dial and sends GOAWAY, the relay waits for identity on the separate control stream, `ReconnectingClient` closes a late connection. | fake-binding, FA(1), mem:// | internal | VM |
| /home/user/switchboard/test/binding_seam_test.dart | 665 | binding contract / node | 15 | The frame mux charges a subframe its length plus 16; stream-binding channel ids are the same at both ends, with mux parity. A node over a binding: dials, pools by normalised endpoint, dispatches; `registerBinding` refusals; `adopt` once. Talk bulk and `whenWritable` over a binding. Identity: IDENT on dial, a channel that overtakes the IDENT is held. Also the proxy between frame mux and binding, slot-channel reopen, and `ReconnectingClient`. | fake-binding, mem:// | internal | VM |
| /home/user/switchboard/test/bytes_test.dart | 73 | codec utilities | 4 | Little-endian writer/reader, truncation throws, hex helpers, u32/u48 values that survive JavaScript's 32-bit arithmetic. | codec | codec | node preset |
| /home/user/switchboard/test/fake_binding.dart | 1435 | helper | 0 | See section B. | – | – | – |
| /home/user/switchboard/test/identity_node_test.dart | 955 | identity on nodes | 28 (6 run twice, mem and tcp) | Mutual identity; `ChannelPolicies.scoped`; a worker that can open nothing; bearer credential; a credential from another authority fails the connect with UNAUTHENTICATED; a forged IDENT gets GOAWAY UNAUTHENTICATED. Holding: a channel that arrives before the IDENT is held then admitted, or refused UNAUTHENTICATED after `identityTimeout`. `requireIdentity` with `denyReserved`. `identifyFor`, `identifyOutgoing`, `expectedIdentityFor`, `updateCredential`. PeerSet identifies. A dial-back that arrives before CONNECT fails gets GOAWAY. | mem:// and TCP | 2peer (tcp group); hooks are internal | VM (dart:io) |
| /home/user/switchboard/test/identity_test.dart | 953 | identity: credentials, NONCE and IDENT | 36 (protocol-error group runs twice) | Issuer: lifetime, layout limits, the largest credential fits an IDENT. Verifier: both kinds, LRU cache, key rotation, generic "invalid credential". Connection identity: bearer without proof; holder key with proof and intent; both directions at once; a second IDENT replaces the first; frames after an IDENT wait for its verification; a replay from another connection is rejected; an echoed NONCE cannot bounce our IDENT back; a replayed bearer IDENT is accepted by design; a failed IDENT ends the connection. Receiver binding: a relayed handshake is refused, a relay cannot rename the receiver, `requireNamedIdent`, a bearer IDENT with a proof is rejected; failures give GOAWAY UNAUTHENTICATED. Timeouts for a missing NONCE and missing confirmation. Protocol errors: NONCE of 31 bytes, second NONCE, IDENT before nonces, truncated IDENT, intent over 64 bytes. | mem-pair (`muxPair`/`rawPair`) | 2peer (handshake) + codec | node preset |
| /home/user/switchboard/test/integration/brokered_workers_test.dart | 300 | naming / identity / PeerSet (use case 5b) | 1 | A consumer reaches an outbound-only worker through CONNECT: request and reply; the worker cannot open on the consumer; re-brokered after a loss; consumer restart; worker leaving takes the peer down then out. | TCP multi-node | 2peer (multi-node) | VM |
| /home/user/switchboard/test/integration/cohosted_mesh_test.dart | 274 | mesh, relay, naming | 1 (runs twice: tcp, ws) | One node hosts naming, `_relay` and a service, reaching itself over an in-process pair. An outbound-only worker registers through it; a consumer reaches the service directly and the worker through the relay. Strict credentials and named IDENTs. | TCP / WS multi-node | 2peer | VM |
| /home/user/switchboard/test/integration/embedded_test.dart | 159 | transport + mux + Talk (embedded profile) | 3 | Byte-exact embedded round trip from a raw socket. A device that announces a 256-byte window is held to it from the first channel. A device that never sends LIMITS, with bytes split one per write. | real TCP, raw bytes | codec + 2peer (best interop candidate) | VM |
| /home/user/switchboard/test/integration/fleet_consumer_test.dart | 342 | naming, PeerSet, outgoing policy (use case 5) | 1 | A scaler registers 8 workers as registrar, with ids, endpoints and metadata. The consumer's PeerSet uses per-worker channels and load-balances. Covers a reboot, unregister (held, then removed), a refused push, credential isolation. | mem:// multi-node | 2peer (mem only) | VM |
| /home/user/switchboard/test/integration/mesh_test.dart | 218 | node / mesh | 3 | Static-table mesh: A (alpha/1, beta/2 on TCP and WS), B (gamma/3 on WS), client C. Cross calls over both transports, a request chained across nodes, graceful node shutdown during a stream. | TCP + WS multi-node | 2peer | VM |
| /home/user/switchboard/test/integration/naming_mesh_test.dart | 963 | naming, MeshNode, frontend proxy | 14 (11 run ×3: tcp, ws, mem; ≈36 runs) | `publishSharded` failures; naming service registered as `_ns/1` with global ids; resolution (round robin, exact instance, shards, leave, NOT_FOUND); unpublish; naming restart (stale table, same ids, resync); id taken during an outage; unreachable naming service, bounded. Frontend endpoint: Talk and stream through the proxy, allowlist, backend shutdown ends a proxied stream; "a raw mux peer without a Switchboard" (hand-built OPEN). Liveness through mux keep-alive. | multi-node tcp/ws/mem + raw socket | 2peer | VM |
| /home/user/switchboard/test/integration/relayed_workers_test.dart | 391 | relay (use case 5c) | 1 | Two relays; worker and consumers listen nowhere; one brokered connection serves both consumers; a consumer without `open` is refused; relay failover; worker restart. | TCP multi-node | 2peer | VM |
| /home/user/switchboard/test/integration/sharding/chat_rooms_test.dart | 327 | sharding (room, eager, shared recovery) | 1 | A crashed server's rooms are reassigned at once; members get RELOCATED naming the new owner and rejoin through `openTalkToSlot`. | `Cluster('tcp')` | 2peer multi-node | VM |
| /home/user/switchboard/test/integration/sharding/cluster.dart | 211 | helper | 0 | See section B. | – | – | – |
| /home/user/switchboard/test/integration/sharding/discord_frontend_test.dart | 322 | sharding (static) | 5 (runs twice: tcp, ws) | Guild routed by `(guild_id>>22) mod N`; a second frontend for a taken shard refuses to start; more shards than the naming channel takes; republish after failure; restart with another shard count. | Cluster tcp/ws | 2peer | VM |
| /home/user/switchboard/test/integration/sharding/job_partitions_test.dart | 239 | sharding (static, shared recovery) | 1 | A standby worker takes over the partitions of a worker that went down. | Cluster tcp | 2peer | VM |
| /home/user/switchboard/test/integration/sharding/kv_store_test.dart | 421 | sharding (eager, holder-only) | 2 (runs twice: tcp, mem) | Slots move one at a time while clients write per-key sequence numbers: nothing lost, duplicated or reordered; reboot; a leaving node hands its slots over. | Cluster tcp/mem | 2peer | VM |
| /home/user/switchboard/test/integration/sharding/per_user_workers_test.dart | 357 | sharding (lazy, userq 65536) | 1 | LOCATE assigns on first use; users move one at a time with job order kept; web and Discord frontends land on the same worker. | Cluster tcp | 2peer | VM |
| /home/user/switchboard/test/integration/sharding/zone_servers_test.dart | 310 | sharding (static, MIGRATE) | 1 (runs twice: tcp, ws) | A hot zone moves to a fresh server during play: state handed over in DRAIN, requests of attached players forwarded as message chains, channels queued while locked are piped to the new server. | Cluster tcp/ws | 2peer | VM |
| /home/user/switchboard/test/local_connections_test.dart | 502 | node (`localConnections`) | 16 (10 run ×3: tcp, ws, mem) | Connections a node makes to its own listener use an in-process pair. Policy, pool, GOAWAY; the node identifies to itself; a large bulk payload to itself under flow control; PeerSet over its own instance; the proxy refuses its own listener; the socket path when `localConnections` is off. | multi-node tcp/ws/mem | internal | VM |
| /home/user/switchboard/test/memory_endpoint_test.dart | 362 | `mem://` transport / node | 13 | `listenMemory` URIs and names, Talk through dispatch, timers not starved, the acceptor sees the initiator's mem endpoint, policy, pooling, unknown id is UNAVAILABLE and missing id INVALID_ARGUMENT, GOAWAY on close, proxy over mem. | mem:// | internal | VM |
| /home/user/switchboard/test/mux_flow_test.dart | 1019 | mux flow control | 35 | Send window and CREDIT in order; a subframe over half the window is refused; CREDIT beyond 2^32−1 is a protocol error; CREDIT after CLOSE or for an unknown channel is ignored; close waits behind credit-blocked data; the peer's CLOSE drops waiting data; `whenWritable`. DATA waits for the peer's LIMITS (OPEN goes at once; a first frame that is not LIMITS, or the keep-alive timeout, ends the wait). Receive window: automatic credit at half the window, manual credit, grant; a frame beyond the window is a protocol error. Between two connections over memory and TCP: window accounting, a slow consumer stalls only its own channel, the connection backstop. | mem-pair / raw peer; TCP | 2peer (flow control) | VM |
| /home/user/switchboard/test/mux_harness.dart | 209 | helper | 0 | See section B. | – | – | – |
| /home/user/switchboard/test/mux_limits_test.dart | 1068 | mux limits and status mapping | 34 | A rejected OPEN keeps its id until confirmed (re-OPEN before that is a protocol error; many unconfirmed rejections give GOAWAY RESOURCE_EXHAUSTED); backlog caps on undelivered channels; receive buffers within the window; the high-water mark pauses transport input; output backpressure over TCP; reason truncation is UTF-8 safe and keeps MOVED fields; RELOCATED crosses a CLOSE byte for byte; an 1100-byte GOAWAY reason; a CLOSE reason fits the peer's frame limit; CONNECTION_LOST goes out as UNAVAILABLE; application codes from the peer (CLOSE reported UNKNOWN, GOAWAY reported GOING_AWAY); close confirmation timeout; OPEN payload budget. | mem-pair / raw peer; TCP | 2peer + internal bounds | VM |
| /home/user/switchboard/test/mux_schedule_test.dart | 872 | mux output scheduling | 28 | Scheduler through `GatedTransport`: CREDIT reservations, priority moves, control never held, round-robin, bulk interleaving (`bulkZipper`), CLOSE follows its DATA, GOAWAY first on close, bulk chunking within the window and frame limit. `openAfter`: child OPEN ordering; DATA or CLOSE before the child's OPEN is a protocol error; CREDIT for an unsent child is ignored. | mem-pair (gated), raw peer | internal (scheduler); a few wire-order rules are 2peer | VM |
| /home/user/switchboard/test/mux_talk_test.dart | 104 | mux + Talk | 4 | Talk request over a mux channel and close status; a Talk abort maps an application code before the mux close; a Talk protocol error closes only that channel; CONNECTION_LOST goes out as UNAVAILABLE. | MemoryTransport.pair | 2peer | VM |
| /home/user/switchboard/test/mux_test.dart | 1610 | mux | 74 | Channels: open from both sides, close handshakes, simultaneous close, in-flight data, sink semantics, wire bytes of a channel's life. Limits: maxChannels gives CLOSE RESOURCE_EXHAUSTED; LIMITS is the first frame, byte exact; mid-connection LIMITS; peer frame and channel limits. Channel ids: parity, reuse, wrap, reserved ids skipped, long form, `shortIdsOnly` gives UNSUPPORTED. Protocol errors. Control: PING/PONG, 126–1024-byte PINGs answered, ping bounds, second GOAWAY ignored. GOAWAY: no opens after, racing OPEN gets GOING_AWAY, grace expiry, `peerGoAwayStatus`. Keep-alive: a dead peer gives CONNECTION_LOST. Transport failures: wrong magic gives GOAWAY PROTOCOL_ERROR, bad version gives GOAWAY UNSUPPORTED, WS text gives PROTOCOL_ERROR, an oversize message gives FRAME_TOO_LARGE. Real transports: TCP and WebSocket. | mem-pair / raw peer (most), FA(2), TCP + WS loopback | 2peer (core) | VM |
| /home/user/switchboard/test/name_test.dart | 54 | names | 5 | Name vectors (the wiki Names table), round trips, too long or embedded zero rejected, bad wire bytes, `isReserved`. | codec | codec | node preset |
| /home/user/switchboard/test/naming_bulk_test.dart | 120 | naming + bulk | 2 | A 40 KiB REGISTER goes bulk and reaches watchers; a HOLDING of 20,000 slots goes bulk. | mem:// | 2peer | VM |
| /home/user/switchboard/test/naming_harness.dart | 412 | helper | 0 | See section B. | – | – | – |
| /home/user/switchboard/test/naming_identity_test.dart | 1761 | naming + identity | 33 | Scope enforcement per `_ns` channel (register, slot space, watch, broker, admin); the relay record names the registering identity; connection identity wins over a payload credential. Replacement credentials end the channel or narrow subscriptions. Expiry and RENEW: GOAWAY at expiry; MeshNode renews at two thirds of the lifetime. CONNECT brokering: refusals, timeout UNAVAILABLE, a late dial-back gets GOAWAY, PeerSet brokers. | mem://, FA(8) | 2peer (naming procedures) + internal (fake time) | VM |
| /home/user/switchboard/test/naming_protocol_test.dart | 493 | naming codecs | 31 | ServiceRecord: 0, 255 and 256 (refused) endpoints, non-ASCII URI, instance 0 refused, truncation at every boundary. Metadata: empty field is 2 zero bytes, maximum length. Register, Unregister, Watch and ServiceEvent round trips; `checkEndpoint`; fuzz. | codec | codec | VM (portable) |
| /home/user/switchboard/test/naming_resolver_test.dart | 210 | naming resolver | 12 | Resolves after sync, UNAVAILABLE if never synced, serves the stale table while resyncing, `ready`/`close`. | Harness, FA(1) | internal | VM |
| /home/user/switchboard/test/naming_test.dart | 2065 | NamingService + NamingClient | 71 | Service: ids increase from 1, requested ids honoured, u48 wrap, ALREADY_EXISTS, re-register sends UP, 4096-byte metadata cap, registrar, UNREGSTR (NOT_FOUND, PERMISSION_DENIED, DOWN), WATCH snapshot then reply then live events, UP before the reply on the registering channel, filters, per-channel watch cap, LOOKUP stream, channel close sends DOWN, REGISTER racing a snapshot gives exactly one UP, close gives GOING_AWAY. Idle subscriptions; assignment hold (EXTEND with time left). Client: sync and mirror; reconnect, re-register the same ids and resync with DOWN; a taken id is replaced; many REGISTER/UNREGSTR races. | Harness (StreamChannelController, sync or async), LossyConnector, FA(8) | 2peer (Talk-level protocol) + internal races | VM |
| /home/user/switchboard/test/outgoing_test.dart | 690 | node: outgoing policy, `credentialFor` | 24 | Precedence of `outgoingPolicy`, `endpointPolicy` and `connect(policy:)`; dial not pooled; `denyAll`. `credentialFor` per endpoint, record and metadata; an explicit payload wins; the proxy forwards the client's payload; the MOVED retry uses each owner's key. | mem://, ws://127 keys | internal | VM |
| /home/user/switchboard/test/peer_set_test.dart | 1391 | PeerSet | 34 | Membership, online/offline, reboot, removal hold-down; per-peer channel and `onOpen`; endpoint fallback; backoff; GOAWAY reconnects at once; outgoing policy; per-worker credentials; resolver errors; close. | mem://, StaticResolver, FA(7), a ServerSocket that never answers | internal | VM |
| /home/user/switchboard/test/proxy_test.dart | 1986 | proxy | 66 | `pipeChannels`: order, credit hop by hop, close statuses both ways, a lost side gives UNAVAILABLE, an application code gives UNKNOWN, Talk through the pipe, bulk twins. `proxyHandler`: request/reply, bulk, finite-window streams, rejections without mesh detail, `denyReserved`, host hint relay, self-loop refused, per-client fairness, GOAWAY retry. `authorize`: rewrite; null gives UNAUTHENTICATED, a throw gives PERMISSION_DENIED; timeout. Slot tables: owner routing, MOVED retried once unseen by the client, MOVED after data forwarded with the owner hidden, RELOCATED without owner, `revealOwners` byte for byte, LOCATE budget. Channels without a slot. | mem-pair chains + nodes on TCP / WS | 2peer (proxy chains, mixed hops) | VM |
| /home/user/switchboard/test/reconnecting_client_test.dart | 1710 | reconnecting client | 40 | States; backoff (doubling, jitter, reset); `openChannel` timeout gives UNAVAILABLE, close gives CANCELLED; `openTalk` and bulk; persistent channels (reopen, stay closed for some codes, PersistentTalk re-sends its subscription); stop/close; GOAWAY from the endpoint (persistent channels move at once, idle connection reconnects at once, application channels finish on the old connection, a repeated GOAWAY backs off); `nextAttemptAt`, `reconnectNow`, pushed channels. | MemoryTransport endpoint, FA(38) | internal (timing); the GOAWAY reaction is 2peer | node preset |
| /home/user/switchboard/test/relay_test.dart | 1540 | relay | 32 | `_relay` end to end (the instance sees the relay's identity and the consumer's payload); bulk both ways; one brokered connection shared; losses propagate; MOVED relayed byte for byte; `openTalkToSlot` through the relay. Every refusal: UNAUTHENTICATED, an OPEN that overtakes its IDENT, expiry, unnamed IDENT, malformed or reserved or chained addresses, PERMISSION_DENIED, per-connection bound RESOURCE_EXHAUSTED, NOT_FOUND, no `broker` gives UNAVAILABLE. PeerSet through the relay. | mem:// + NamingService | 2peer (multi-hop) | VM |
| /home/user/switchboard/test/resolver_test.dart | 104 | resolver | 4 | StaticResolver `close` and events; EndpointResolver. | API only | internal | VM |
| /home/user/switchboard/test/selection_test.dart | 428 | selection / dispatch | 16 | Round robin; `where` filter on metadata; shard slot picks `sorted[slot mod n]`; NOT_FOUND; random selection; `where` on the MOVED retry; `acceptAnyInstance` dispatch. | mem:// | internal | VM |
| /home/user/switchboard/test/sharding_client_test.dart | 1322 | sharding (instance side of NamingClient) | 30 | CLAIM through ASSIGN with the mirror and resolver following; migration seen in protocol order; handler answers; a slow ASSIGN that declares its estimate; naming restart (resume, re-send, re-claim by epoch); beyond the Talk request limit; settle window; slot handler maximum duration; declared deadlines. | Harness / ScriptedNaming, FA(12) | 2peer (instance ↔ naming procedures) + internal | VM |
| /home/user/switchboard/test/sharding_harness.dart | 410 | helper | 0 | See section B. | – | – | – |
| /home/user/switchboard/test/sharding_test.dart | 2047 | sharding (naming service side) | 63 | SLOTS define, confirm, refuse, redefine; CLAIM and RELEASE; LOCATE (lazy, holder preferred, static free slot, capacity 0, a refused ASSIGN gives UNAVAILABLE); allocator (eager, holder-only grace, shared recovery); HOLDING; WATCH/LOOKUP snapshot with SLOTSPC before its slots; MIGRATE hand-over (DRAIN, ASSIGN, table, FORWARD), queueing, refusals then RESUME; hand-over bound; restart recovery; ASSIGN backoff; RESUME retries; settle window; declared deadlines. | Harness + scripted instances, FA(27) | 2peer (procedure sequences) + internal (fake time) | VM |
| /home/user/switchboard/test/slot_gate_credential_test.dart | 301 | slot gate / credentials | 6 | Requests a gate forwards present the client's OPEN payload credential, never the node's default; one forwarding channel per token. | 3 mem:// nodes | internal | VM |
| /home/user/switchboard/test/slot_gate_test.dart | 1609 | slot gate state machine | 53 | Dispatch (MOVED without owner, load then serve, queues, revocation, OUT_OF_RANGE, `maxQueuedChannels` gives UNAVAILABLE, a throwing serve gives INTERNAL). Hand-over (DRAIN waits up to `drainTimeout`, FORWARD pipes the queue in order, after the grace unload then MOVED, RELOCATED names the new owner, FAILED_PRECONDITION cases). Requests (ABORT MOVED, forwarded with bulk). SlotGates; HOLDING discards. | mem-pair mux links + Harness + loopback TCP node | internal; the MOVED/RELOCATED payloads are 2peer | VM |
| /home/user/switchboard/test/slot_routing_test.dart | 591 | slot routing (consumer side) | 26 | Owner and old owner while migrating; LOCATE on a miss (managed) vs UNAVAILABLE (static); OUT_OF_RANGE. MOVED retried once at the named owner; a newer table entry wins; never on RELOCATED; not after the first send or receive. Subframes buffered during the retry, RESOURCE_EXHAUSTED beyond the buffer. Talk retry with bulk. | nodes on TCP + StaticResolver / LocatingResolver | 2peer (MOVED retry) | VM |
| /home/user/switchboard/test/status_test.dart | 92 | status codes | 6 | `05 00 78`, `00 00`, `34 12`; empty payload is OK; bytes after the code kept as received (`25 00 FE DC BA 98 76 54 01 00 00 80 78`); `toString` shows binary reasons as hex; u16 range; gRPC numbering. | codec | codec | node preset |
| /home/user/switchboard/test/switchboard_test.dart | 1609 | Switchboard node | 58 | Listening (URIs, WS path and HTTP answers, subprotocol); WS binding (frame limit reaches both ends, no compression). Dispatch: exact instance, any instance, NOT_FOUND, default service, catch-all, a malformed OPEN payload is a channel protocol error, a throwing handler gives INTERNAL, reject status. Listener policy (`denyReserved`, `allowTypes`). Bulk channels: routed to the parent before policy; no open parent gives FAILED_PRECONDITION. Symmetric dispatch. Pool (shared, per path, concurrent connects, replaced, a GOAWAY peer is not reused, refused not cached, a GOAWAY between connect and OPEN is retried once). `openChannel` round robin, shards, failover. Close sends GOAWAY. `accept`/`adopt`; app HttpServer. | TCP + WS loopback, HttpServer | 2peer | VM |
| /home/user/switchboard/test/talk_bulk_test.dart | 1960 | Talk bulk | 60 (4 run ×2: memory, TCP) | Reassembly budget, `maxUnclaimedBulk`, per-message cap; no deadlock; idle timeout; over memory and TCP: known and unknown length, bulk response/items/plain message, automatic bulk above `maxSubframeLength`−12; EXTEND on uploads; slow readers; cancellation; wrong length. Bulk channel rules: unreferenced closed; a never-opened number or a number referenced twice is a protocol error; no parent gives FAILED_PRECONDITION; a parent closing cancels; a graceful close waits. | mem-pair + TCP, FA(12) | 2peer (bulk) | VM |
| /home/user/switchboard/test/talk_credit_test.dart | 561 | Talk ↔ mux credit | 18 | Talk uses manual mux credit; a paused `messages` stream stalls at the window; frames Talk consumes return credit at once; `items` flow control; awaitable sends; `whenWritable`; a raw stream channel is not flow controlled. | mem-pair | internal (window effects observable) | VM |
| /home/user/switchboard/test/talk_forward_test.dart | 1893 | Talk forwarding (message-chain proxy) | 49 (main group runs ×4: 1/2 proxies × sync/async; window group ×3) | Forwarding of plain messages, requests, streams, chained replies, item requests, aborts and cancels; losses give UNAVAILABLE. Timeouts: declared deadlines and EXTEND pass through, the proxy adds no timeout of its own. Byte-for-byte passthrough: non-UTF-8 procedure names, EXTEND payloads, MOVED/RELOCATED abort fields. Id rewriting; bulk piped; forwarding credit; item windows with rewritten ids. | StreamChannelController chains (+MemoryTransport), FA(7) | 2peer (a TS proxy between Dart peers) | VM |
| /home/user/switchboard/test/talk_test.dart | 3678 | Talk | 123 | Messages and requests (chained, 200 interleaved, large payloads); streams; aborts (exact status, channel abort, a handler exception gives ABORT INTERNAL); cancel; timeouts (JS-safe timer steps, responder ABORT DEADLINE_EXCEEDED, requester cancel, EXTEND restarts timers). Limits and ids: `maxIncomingRequests` gives ABORT RESOURCE_EXHAUSTED; ids wrap and skip outstanding ones; an unknown response id with HAS_REQUEST gets ABORT NOT_FOUND; a reused id supersedes. Protocol errors; abort statuses (empty means UNKNOWN; CONNECTION_LOST never goes on the wire). Declared deadlines (min/max clamps, empty EXTEND). Synchronous-transport races; ordered answers; subscriptions; `replyStream` stubs. | raw Talk-frame peer over StreamChannelController (no mux), FA(42) | 2peer for wire semantics; internal for timing and API | VM (portable) |
| /home/user/switchboard/test/talk_window_test.dart | 1575 | Talk item windows | 41 | A finite window delivers exactly N items; grants add up; window 0; final, abort and extend pass at zero; an item request counts as one item. Credits received: unknown request ignored; bad credits give PROTOCOL_ERROR; largest credit accepted; a credit for a non-stream request gives PROTOCOL_ERROR; an item beyond the window is a protocol error. Timeouts stop while the window is exhausted; the credit bound gives DEADLINE_EXCEEDED. Responder side: `whenCredit`, `replyStream` pacing. | raw peer + mem-pair, FA(12) | 2peer (stream request credit) | node preset |
| /home/user/switchboard/test/transport_test.dart | 894 | transport | 41 | MemoryTransport (order, copy, close, delay, `maxFrameSize`, `addError`). StreamTransport over byte pipes (preamble at once, any chunk size, error codes, FRAME_TOO_LARGE, output throttling, abort) and over TCP. WebSocketTransport (binary only, text is a protocol error, 1 MiB default limit, connect timeout gives UNAVAILABLE). IOWebSocketTransport (subprotocol selection, refusals, no compression). | mem-pair, pipes, TCP + WS | 2peer (framing) + codec | VM |
| /home/user/switchboard/test/web_socket_server_test.dart | 1033 | transport (Dart WebSocket server) | 31 | Handshake: `switchboard` subprotocol; no subprotocol accepted; 400/426 refusals. Frames: all length encodings, fragmentation with a ping between, split reads, ping answered, close echoed with or without a code, sink close sends 1000. Limits and errors: 1009 with FRAME_TOO_LARGE, fragment caps, unmasked frame 1002, text 1003, non-UTF-8 close reason 1007. Ping interval drops a silent client. Interoperability: dart:io client, `web_socket_channel`, a mux over it where either close gives GOING_AWAY. Listener: 403 for other paths. | raw TCP sockets + dart:io WS clients to a loopback server | 2peer | VM |
| /home/user/switchboard/test/vectors/address_vectors_test.dart | 197 | addressing | 5 (≈13 runs) | Address headers, `_relay`, `_bulk`, negatives (see section A). | codec | codec | node preset |
| /home/user/switchboard/test/vectors/binding_protocol_test.dart | 141 | mux identity / binding helpers | 3 (4 runs) | IDENT verification gates OPEN, PING and a queued IDENT (success and failure); a wrong receiver closes synchronously without calling the verifier; `statusForWire`/`statusFromWire` (MOVED with a 900×'é' reason fits 1024; CONNECTION_LOST becomes UNAVAILABLE; 256 refused on send and read as UNKNOWN). | raw peer (`rawPair`) | 2peer (IDENT gating) + internal | node preset |
| /home/user/switchboard/test/vectors/exporter_identity_vectors_test.dart | 150 | identity (proof X for QUIC) | 8 | X message and signature; verify and refuse cases; `secureRandomBytes` lengths. | codec | codec | node preset |
| /home/user/switchboard/test/vectors/identity_vectors_test.dart | 437 | identity | 14 (≈47 runs) | C1, C2, P, malformed credentials, scopes, NONCE and IDENT frames. | codec | codec | node preset |
| /home/user/switchboard/test/vectors/mux_vectors_test.dart | 482 | mux | 20 (≈57 runs) | Frames, control messages, decoder negatives; connection negatives through a raw MemoryTransport peer. | codec (+ raw mem connection) | codec (+2peer for the connection group) | node preset |
| /home/user/switchboard/test/vectors/naming_vectors_test.dart | 203 | naming | 18 (≈22 runs) | REGISTER, UNREGSTR, WATCH, UP, DOWN, CONNECT. | codec | codec | node preset |
| /home/user/switchboard/test/vectors/sharding_vectors_test.dart | 599 | sharding | 13 (≈47 runs) | Slot procedures, MOVED and RELOCATED. | codec | codec | node preset |
| /home/user/switchboard/test/vectors/slot_key_vectors_test.dart | 35 | key-to-slot | 2 | FNV-1a 32 and slot of a key. | codec | codec | node preset |
| /home/user/switchboard/test/vectors/talk_vectors_test.dart | 705 | Talk | 16 (≈93 runs) | Positive and negative vectors, extra rules, item windows, encoder refusals. | codec | codec | node preset |
| /home/user/switchboard/test/vectors/transport_vectors_test.dart | 161 | transport | 11 | Preamble and framing rules. | codec | codec | node preset |

## 2. QUIC package `/home/user/switchboard_quic/test`

| Path | Lines | Area | Tests | What it covers | Peers | Interop | Platform |
|---|---|---|---|---|---|---|---|
| /home/user/switchboard_quic/test/binding_test.dart | 1436 | QUIC binding | 57 own + 27 from `reviewCases()` = 84 | Channel ids: even from 2 for the initiator, odd from 3 for the acceptor, never reused; reserved ids skipped; the OPEN record comes first; streams before the control stream; a reset before OPEN is complete. Subframes at record cost; the largest subframe is the peer's HELLO record limit; an oversize record gives FRAME_TOO_LARGE with the body unread; bulk chunks. Close: CLOSE record plus FIN; MOVED fields in CLOSE; rejection is CLOSE plus STOP_SENDING with the code; a reset is a close by its code; codes over 255 read UNKNOWN; STOP_SENDING is an early close; confirmation timeout gives STOP_SENDING DEADLINE_EXCEEDED. Flow control; priorities 2/1/0; `openAfter`. GOAWAY (no new channels, a racing channel gets GOING_AWAY, bulk admitted for an open parent); an application close counts as GOAWAY; a lost session gives CONNECTION_LOST. Keep-alive and control stream (WebTransport reset and STOP_SENDING without a code). Resource limits (maxChannels gives RESOURCE_EXHAUSTED with STOP_SENDING, open payload budget, HELLO channel count advisory, stream credit). Identity (exporter labels, EXPORTER flag, nonce-bound vs exporter-bound, bearer rules, IDENT gating, NONCE/IDENT protocol errors). | fake-provider (`connectionPair`, `bindingAndRaw`) + RawPeer | 2peer (QUIC wire rules) | node preset (and VM) |
| /home/user/switchboard_quic/test/review_cases.dart | 548 | QUIC binding regressions | 27 (run inside binding_test) | CLOSE without FIN released after timeout; control reset while throttled; peer reset ends an idle channel; STOP_SENDING without a code is not an acknowledgement, and on the control stream is a protocol error; a failed exporter is not taken as "unavailable"; session loss interrupts a pending exporter; identity defaults; Talk waits for late bulk after parent CLOSE and GOAWAY; a failure close before opening still writes OPEN and CLOSE; concurrent error closes; peer CLOSE interrupts a stalled write; second HELLO; control OPEN must be empty; a record at the limit over a small window; missing HELLO bounded; a listener refuses a nonce-bound IDENT even without a verifier; mismatched exporters; pending OPEN streams capped; bounded PING output; reset without a code is UNKNOWN; provider draining is GOAWAY; unidirectional streams refused; provider failures end only their channel; stream open failure status; session loss resolves writers; incoming stream source failure. | fake-provider + RawPeer | 2peer | node (through binding_test) |
| /home/user/switchboard_quic/test/build_hook_test.dart | 158 | native library build hook | 6 | ELF, Mach-O (thin and fat) and PE header checks. | none | internal | @TestOn('vm') |
| /home/user/switchboard_quic/test/native_integration_test.dart | 848 | binding over the native provider + Switchboard | 17 (`cases()` runs ×2: raw QUIC and WebTransport; plus WebTransport-only cases) | Exporter-bound identity both ways (label `switchboard-ident`, 32 bytes, equal at both ends); a dialler without a credential refused UNAUTHENTICATED; certificate pin right and wrong. Talk; 8 MiB bulk each way; `whenWritable`; a paused reader sees the peer's reset at once; 64 parallel channels three times over; throughput. GOAWAY with code and reason from either side (session closes with code 14 and reason "upgrade"); idle timeout gives CONNECTION_LOST; stack keep-alive; proxy with a frame mux backend behind a QUIC front; WebTransport path refused (UNAVAILABLE); WT_DRAIN_SESSION read as GOAWAY GOING_AWAY. | real QUIC and WebTransport on loopback (Rust shim) | 2peer (the QUIC interop reference) | @TestOn('vm') |
| /home/user/switchboard_quic/test/native_provider_test.dart | 1517 | native QUIC provider | 54 (contract runs ×2 modes) | Certificates and configurations; handshake (unverified, pinned, wrong hash, platform verification, IPv6); exporter. WebTransport: request path, origin, protocols, draft; 404/403 refusals; drain. Streams: FIN, reset and stop codes, 4 MiB backpressure, pending operations fail at session end. Priorities; stream limit; session end (close code and reason, idle timeout, keep-alive, 256 streams, 64 MiB on one stream, isolates, shutdown). | real QUIC loopback (shim) | internal (provider API); the code semantics are stack-level | @TestOn('vm') |
| /home/user/switchboard_quic/test/review_switchboard_test.dart | 156 | QUIC + node | 4 | `adopt` takes a QUIC connection once; closing the listener during exporter setup; SlotChannel counts held sends at record cost; ReconnectingClient reopens over a new QUIC session. | fake-provider | internal | @TestOn('vm') |
| /home/user/switchboard_quic/test/switchboard_test.dart | 1205 | QUIC + Switchboard node | 24 | Two nodes: one stream per channel, pooled by endpoint; 3 MiB bulk each way with streams arriving out of order; many channels (pool overflow, waiting for stream credit); `whenWritable`; https endpoints; port required. Dialling timeouts; late sessions closed CANCELLED; stack keep-alive is a third of the idle timeout. Identity: exporter-bound both ways with a scoped policy; an overtaking channel is held; without the exporter a node cannot identify to a self-signed listener; a bearer credential goes only to an authenticated end. Man in the middle: relayed HELLOs give different exporters; stripping EXPORTER fails. Proxy between frame mux and QUIC in both directions with bulk twins. End: GOAWAY gives GOING_AWAY and session code 0; GOAWAY status and reason reach the peer; a lost session gives CONNECTION_LOST. | FakeQuicNetwork (+ mem:// for the frame mux side) | 2peer | @TestOn('vm') |
| /home/user/switchboard_quic/test/vectors_test.dart | 692 | QUIC records, control, status, exporter | 44 (≈70 runs) | See section A. Negative vectors are fed through `bindingAndRaw`. | codec + fake-provider raw | codec (+2peer for connection negatives) | node preset |
| /home/user/switchboard_quic/test/web_binding_test.dart | 220 | binding over the web provider (browser) | 8 | HELLO both ways over WebTransport with the certificate authenticating the node; Talk ECHO and a 200,000-byte BIG; a nonce-bound IDENT (credential from the `issue` service, WHOAMI answers `browser-1`); the node's GOAWAY (browser stops opening, `done` is GOING_AWAY, the server records "connection done"); a browser close read by the node as GOING_AWAY. ReconnectingClient: connects, reconnects after the node closes, moves on GOAWAY. | Chrome to `tool/web_server.dart` `/sb` over real WebTransport | 2peer (a template for a TS client against a Dart node) | @TestOn('browser'), web preset |
| /home/user/switchboard_quic/test/web_provider_test.dart | 568 | web provider (W3C WebTransport) | 26 | Protocol selection; echo with FIN both ways; `read(max)` behaviour; quiet session outlives the server's handshake deadline; one read at a time; `reset(42)` arrives as 42; the server's reset arrives as 14; `stop(5)` arrives as STOP_SENDING 5; the server's STOP_SENDING 9; `close(7, reason)`; reason cut to 1024 bytes of UTF-8; the server's close; drain; refused path, wrong hash, closed port; handshake deadline; exporter null without `exportKeyingMaterial`; server-opened streams; unidirectional; stream credit; writes stall; two sessions; code range; send orders; stats. | Chrome to web_server `/provider` and `/limited` | 2peer at stack level (provider contract) | @TestOn('browser'), web preset |
| /home/user/switchboard_quic/test/fake_provider.dart | 1029 | helper | 0 | See section B. | | | |
| /home/user/switchboard_quic/test/harness.dart | 259 | helper | 0 | See section B. | | | |
| /home/user/switchboard_quic/test/web_harness.dart | 214 | helper | 0 | See section B. | | | |

---

## A. Test vector groups compared with the wiki

**Wiki tables used:** `/home/user/sway/wiki/switchboard/test-vectors.md` and the "Test vectors" section of `/home/user/sway/wiki/switchboard/quic-binding.md`.

**Overall result:** every vector in the wiki tables is asserted by the Dart suites. The Dart tests also contain many extra vectors. Most of them come from rules stated in the prose of `transport.md`, `mux.md`, `talk.md` and `embedded-profile.md`, not from the vector tables.

**Stale wiki line:** `test-vectors.md` (line 54) says "The Dart reference does not implement the binding yet, so its suite does not assert them." This is out of date. `switchboard_quic/test/vectors_test.dart` asserts the QUIC tables, and the core `exporter_identity_vectors_test.dart` asserts X.

### Core package `test/vectors/*.dart`

**transport_vectors_test, group "transport, stream binding" (11 tests)**
- Wiki: preamble `53 57 42 44 01 00 00 00`; frame `04 00 00 00 02 02 00 AA`. Both present.
- Extras:
  - Embedded-profile OPEN (from embedded-profile.md) `12 02 00 01 64 65 76 00 00 00 00 00` frames as `0C 00 00 00 12 02 00 01 64 65 76 00 00 00 00 00`.
  - Preamble flags are ignored: `53 57 42 44 01 00 FF FF 01 00 00 00 02` yields frame `02`.
  - Wrong magic `53 57 42 45 01 00 00 00` is a protocol error.
  - Version 2 `53 57 42 44 02 00 00 00` is UNSUPPORTED, not a protocol error.
  - Length `00 00 00 00` is a protocol error, and encoding an empty frame is refused.
  - `05 00 00 00` with `maxFrameSize` 4 is FRAME_TOO_LARGE before the body arrives.
  - `maxFrameSize` 0 means no limit, but length 0 is still an error.
  - `00 00 00 80` and `FF FF FF FF` are FRAME_TOO_LARGE, not negative.
  - Frames before an error are delivered first: `01 00 00 00 02` then `00 00 00 00` delivers `02`, then throws.

**mux_vectors_test, group "mux frames"**
- Wiki: all 14 frame rows are present. NONCE and IDENT are in identity_vectors_test.
- Extras:
  - LIMITS ignores trailing fields: `00 04 00 00 04 00 00 00 00 00 01 00 99 99`.
  - Full u32 values: LIMITS `FF FF FF FF 00 00 00 80 FF FF FF FF`; CREDIT `FE FF FF FF FF FF FF FF FF FF`; CREDIT(0x100000002, 0x80000000).
  - Encoding refuses out-of-range LIMITS and CREDIT. `MuxCredit.costOf(0)` is 16 and `costOf(100)` is 116.
  - Unknown control type `7F 01` decodes.
  - Short form: id 0xFFFE encodes `02 FE FF`. The long form of a short id, `00 02 00 00 00 00 00 AA`, decodes as id 2 with longForm set.
  - Ids above 32 bits: `00 02 00 00 00 01 00` (0x100000002), `00 AB 90 78 56 34 12`, `00 02 00 00 00 FF FF`, `00 FE FF FF FF FF FF`, `00 00 00 00 80 00 00`.
  - Compatible reserved bits are ignored, beyond the wiki's `42 02 00 01`: `82 02 00` and `D2 03 00`.

**mux_vectors_test, group "mux negative vectors, decoder"**
- Wiki: all 6 rows present, plus the three bad control payloads (LIMITS shorter than 12 bytes, LIMITS with a zero window, CREDIT that is not 10 bytes).
- Extras (from the rules in mux.md):

  | Case | Bytes |
  |---|---|
  | Reserved incompatible flag 0x04 | `06 02 00` |
  | Reserved incompatible flag 0x08 | `0A 02 00` |
  | CLOSE on channel 0 | `22 00 00` |
  | OPEN on channel 1 | `12 01 00` |
  | Reserved id 0xFFFF | `02 FF FF` |
  | Reserved id 0xFFFF00000000 | `00 00 00 00 00 FF FF` |
  | Reserved id 0xFFFFFFFFFFFF | `00 FF FF FF FF FF FF` |
  | Reserved id with payload | `00 00 00 00 00 FF FF 01 DE AD` |
  | Empty frame | (no bytes) |
  | Control payload of 1025 bytes (1024 is accepted) | type `7F` + 1025 bytes |
  | Control message without a type | (empty payload) |
  | Truncated LIMITS | `00 04 00 00 04 00 00` |
  | CREDIT of 11 bytes | `02 00 00 00 00 00 00 80 00 00 00` |

**mux_vectors_test, group "mux negative vectors, connection"**
- The wiki negatives through a mux connection: each gives GOAWAY PROTOCOL_ERROR then close.
- Extras:
  - CREDIT for a channel that is not open is ignored: `02 00 00 07 02 …` and `… FE FF FF FF FF FF FF FF FF FF`, then a PING `AA` is answered `02 00 00 02 AA`.
  - A long id addresses its own channel: OPEN `10 03 00 00 00 01 00` and OPEN `12 03 00`. DATA on each reaches its own channel, and our CLOSE uses the long form `20 03 00 00 00 01 00`.
  - Compatible reserved bits ignored by a connection: `52 03 00`, `82 03 00 01`, `C2 00 00 01 DE AD`, answered `02 00 00 02 DE AD`.

**talk_vectors_test**
- **"Talk vectors, positive":** all 20 wiki rows.
  - Extras: largest bulk reference `FF FF FF FF FE FF FF FF FF FF`; empty abort payload `24 01 00 00` and channel abort `20` both read as UNKNOWN.
- **"Talk vectors, negative":** all 17 wiki rows.
- **"Talk decoding rules beyond the wiki vectors":** 26 extra negatives (each a protocol error):

  | Case | Bytes |
  |---|---|
  | Reserved bit 0x80 | `81 48 45 4C 4C 4F 00 00 00` |
  | BULK with an empty reference | `41 48 45 4C 4C 4F 00 00 00` |
  | BULK with an 11-byte reference | `41 48 45 4C 4C 4F 00 00 00` + 11 bytes |
  | BULK reference to bulk number 0 | `41 … 00 00 00 00 05 00 00 00 00 00` |
  | BULK on a cancel | `62 02 00 00 01 00` |
  | BULK on a channel abort | `60 10 00` |
  | Empty input | (no bytes) |
  | Response id 0 | `04 00 00 00` |
  | Chained with response id 0 | `06 01 00 00 00 00 00` |
  | EXTEND with STREAM and no request | `3C 01 00 00` |
  | Credit with STREAM and a window | `3A 02 00 00 FF FF FF FF 08 00 00 00` |
  | Cancel with STREAM and a window | `2A 02 00 00 FF FF FF FF 01 00` |
  | Credit with an empty payload | `32 02 00 00` |
  | Credit with a 3-byte payload | `32 02 00 00 08 00 00` |
  | Credit with a 5-byte payload | `32 02 00 00 08 00 00 00 00` |
  | Credit with a procedure and 0 items | `33 41 00 00 00 00 00 00 00 02 00 00 00 00 00 00` |
  | Chained stream request without its window | `0E 04 00 00 03 00 00` |
  | Item stream request with a truncated window | `1E 05 00 00 03 00 00 04 00 00` |
  | EXTEND with a 2-byte payload | `34 01 00 00 AA BB` |
  | EXTEND with a 9-byte payload | `34 01 00 00 01 00 00 00 01 00 00 00 00` |
  | STREAM_ITEM with only a request id | `13 48 45 4C 4C 4F 00 00 00 01 00 00` |
  | Request without a procedure | `02 01 00 00` |
  | ABORT with a 1-byte payload | `24 01 00 00 05` |
  | Truncated request id | `03 48 45 4C 4C 4F 00 00 00 01 00` |
  | Truncated response id | `04 01 00` |
  | Procedure with a byte after the padding | `01 61 00 62 00 00 00 00 00` |

  The same group adds positive decode checks:
  - EXTEND `34 01 00 00 00 00 00 00 E0 93 04 00`: renew only.
  - EXTEND `34 01 00 00 00 00 00 00 00 00 00 00`: zeros, 8-byte payload.
  - EXTEND `34 01 00 00 FF FF FF FF 01 00 00 00`.
  - Encoding rounds up to whole milliseconds: 1.5 ms gives `02 00 00 00 00 00 00 00`; renew 0 gives `00 00 00 00 01 00 00 00`; 60 days clamps to `FF FF FF FF 00 00 00 00`.
  - Chained stream request `0E 04 00 00 03 00 00 FF FF FF FF`; item that is a request `16 05 00 00 03 00 00 AA`.
  - The payload is a view, not a copy.
- **"Item windows on the wire":** extras:
  - `0B 4C 49 53 54 00 00 00 00 02 00 00 00 00 00 00` (window 0).
  - `0B 4C 49 53 54 00 00 00 00 02 00 00 FE FF FF FF AA BB`.
  - Built frame `0B 4C 49 53 54 00 00 00 00 02 00 00 03 00 00 00 01`.
  - `0E 04 00 00 03 00 00 02 00 00 00 CC`.
  - `1F 53 55 42 00 00 00 00 00 05 00 00 03 00 00 04 00 00 00 AA`.
  - `5E 05 00 00 03 00 00 00 00 00 00 02 00 00 00 00 00 00 00 00 00`.
  - `33 41 00 00 00 00 00 00 00 02 00 00 01 00 00 00`.
  - `32 FF FF FF FF FF FF FF`.
  - `creditPayload(1)` is `01 00 00 00`.
- **"TalkFrame.encode refuses invalid frames":** 18 encoder refusal cases. These are behaviours, not bytes.

**address_vectors_test**
- "address header positive vectors" (6), "relay open payloads" (2), `_bulk`, the empty OPEN payload and "address header negative vectors" (3): all in the wiki.
- Extras:
  - An empty payload re-encodes as `00`.
  - `TalkBulkOpen(0xFFFFFFFFFFFE, 0xFFFFFFFF)` round trip.
  - Truncated `_bulk` `01 5F 62 75 6C 6B 00 00 00 02 00 00 00 00 00 01 00 00` is a protocol error.
  - Bulk number 0 `01 5F 62 75 6C 6B 00 00 00 02 00 00 00 00 00 00 00 00 00` is a protocol error.

**naming_vectors_test** (13 top-level, "CONNECT" 4, 5 CONNECT negatives)
- All wiki rows and all prose negatives are present.
- Extras:
  - WATCH with an all-zero 8-byte name means "all".
  - UNWATCH procedure bytes `55 4E 57 41 54 43 48 00`.

**sharding_vectors_test**
- "vectors": all 29 wiki rows. "negative vectors": all 7. "MOVED and RELOCATED status": all 3 wiki rows plus the ABORTED note.
- Extras in "codecs":
  - Every strict prefix of every vector is a protocol error (ASSIGN response excepted); appending `EE 01` is ignored.
  - Unknown flags: AssignResponse `FE` reads as holding; CLAIM `6B 76 00 00 00 00 00 00 07 00 00 00 FE 00 00 00 00` reads holding false.
  - HoldingResponse `FF FF FF FF 01 00 00 00` is refused before reading.
  - u32/u48 range errors on encode.
  - Procedure names; SLOTSPC bytes `53 4C 4F 54 53 50 43 00`; status codes 37 and 38; 39 is unknown.
- Extras in "MOVED and RELOCATED status":
  - Largest values with reason "é": `25 00 FF FF FF FF FF FF FF FF FF FF C3 A9` (and `26 00 …`).
  - Lossless through Status: owner 0xFEDCBA987654, epoch 0x80000001.
  - Hostile inputs that parse as unknown: (empty), `25`, `25 00`, the 11-byte cut, `0E 00 …`, `0A 00 …`, `25 01 …`, `27 00 …`.
  - A trailing `FF` decodes as reason U+FFFD.

**slot_key_vectors_test — none of it is in the wiki** (sharding.md only mentions FNV-1a 32 in prose):
- fnv1a32 of empty is 0x811c9dc5; "a" is 0xe40c292c; "foobar" is 0xbf9cf968; "user:42" is 0x2f6b7b82; bytes 0..255 give 0x90a458c5; elements are taken mod 256.
- `slotForText('user:42', 65536)` is 31618; `slotForKey('foobar', 1)` is 0; "é" hashes as UTF-8 `C3 A9`; count 0 throws RangeError.

**identity_vectors_test**
- Groups: "credential layout" (C1, C2, unknown right), "negative (malformed)" (12), "negative (signature)" (3), "scope patterns" (17 + admin), "connection identity control messages" (NONCE, both IDENTs, proof length binding), "negative (protocol error)" (7 IDENT payloads).
- All wiki rows are present.
- Extras:
  - Malformed case "empty" (0 bytes).
  - Scope `abcdefgh` matches `abcdefgh`.
  - Admin allows everything.
  - Asserts the RFC 8032 public keys `D7 5A 98…` and `3D 40 17…`, and that the HMAC signature covers the first 54 bytes.

**exporter_identity_vectors_test**
- Group "exporter-bound proof (QUIC and WebTransport binding, X)": the 118-byte message and the signature X, as in quic-binding.md.
- Extras:
  - X with another exporter (32 bytes of `34`) does not verify.
  - An exporter of 31 or 33 bytes is refused.
  - `secureRandomBytes` returns lengths 0, 32, 65536 and 65537.

**binding_protocol_test:** behaviour checks, no vectors.

### QUIC package `/home/user/switchboard_quic/test/vectors_test.dart`

**Group "records"**
- Wiki: all 11 positive rows; `FF FF FF 3F` and `FF FF FF 7F` read as 1073741823 bytes with only 4 bytes read; all 7 negatives (through a connection).
- Extras:
  - A record arriving in 2-byte chunks (`05 00 00 00 01 02 03 04 05 00 00 00 80`) is reassembled, reading only its own bytes.
  - FIN inside a record: `03 00 00 00 01` and `03 00`.
  - A second OPEN record: `06 00 00 40 02 00 00 00 00 00 06 00 00 40 04 00 00 00 00 00`.
  - State-dependent cases the wiki only describes in prose: wrong parity `06 00 00 40 03 00 00 00 00 00`; second channel-0 OPEN `06 00 00 40 00 00 00 00 00 00`; FIN without CLOSE `06 00 00 40 02 00 00 00 00 00 01 00 00 00 07` + FIN.
  - Channel 0 on a stream the acceptor opened ends the connection.

**Group "control stream"**
- Wiki: 3 HELLOs, PING, PONG, GOAWAY, NONCE, IDENT C1; all 6 negatives.
- Extras:
  - HELLO `FF FF FF FF 00 00 00 00 FE`: a record limit above 0x3FFFFFFF reads as the maximum; other flag bits are ignored.
  - After HELLO, each a protocol error: a second HELLO; a CLOSE record `00 00 00 80` on the control stream; a control payload over 1024 bytes (`02 04 00 00 01` + 1025 zeros).
  - FIN, reset or STOP_SENDING on the control stream ends the connection.
  - Unknown control type 0x7E is ignored.

**Group "bulk channels":** both wiki rows.

**Group "status codes"**
- All 12 wiki rows: status, raw QUIC varint, HTTP/3 code, wire bytes.
- Extras:
  - Codes 0..999 round-trip; 0x52E4A40FA8DA maps to nothing.
  - 256, base+0 and "no code" read as UNKNOWN (in wiki prose).
  - CONNECTION_LOST is sent as UNAVAILABLE; 256 is refused on send.

**Group "exporter":** all 4 wiki rows. Extra: both ends of a fake session derive the same exporter, and two sessions differ.

**Group "example":** the records of the quic-binding.md "Example" section, not its "Test vectors" section:
- Control stream: `06 00 00 40 00 00 00 00 00 00 0A 00 00 00 08 F0 7F 00 00 00 00 00 00 00`.
- Hub HELLO: `0A 00 00 00 08 00 00 10 00 64 00 00 00 01`.
- `chat/1` OPEN: `15 00 00 40 02 00 00 00 00 00 03 63 68 61 74 00 00 00 00 01 00 00 00 00 00`.
- HISTORY request: `0E 00 00 00 03 48 49 53 54 4F 52 59 00 01 00 00 14 00`.
- Bulk OPEN on channel 3, then the response with bulk reference: `0E 00 00 00 44 01 00 00 01 00 00 00 A0 86 01 00 00 00`.
- Payload split into records `F0 7F 00 00` ×3 and `D0 06 00 00`, then close `00 00 00 80`.

### `/home/user/switchboard/doc/sharding-vectors.md` (64 lines)

This is an old staging copy of the sharding section, written "for the integrator: copy this section into the wiki page … after 'Naming service payloads'". It is the header comment of sharding_vectors_test that still points to it.

It is a strict subset of the current wiki section:
- It lacks the `RELOCATED, owner 0x1A2B, epoch 7` row (`26 00 2B 1A 00 00 00 00 07 00 00 00`).
- It lacks the ABORTED-has-no-fields note.
- It describes MOVED only (code 37).

The positive and negative tables are otherwise identical. Use the wiki, not this file.

---

## B. Test helpers

**`/home/user/switchboard/test/mux_harness.dart`** (209 lines)
- Option sets:
  - `quiet`: keep-alive off, `goAwayGrace` 2 s.
  - `rawOptions`: no LIMITS sent, peer LIMITS not awaited.
- Matchers `throwsStatus` and `hasCode`.
- `muxPair()`: two MuxConnections over `MemoryTransport.pair()`.
- `RawPeer`: the far side of a transport, driven with raw frames. Methods: `send(hex)`, `nextHex()`, `next()` → MuxFrame, `nextControl(type)`, `rest()`.
- `rawPair()`: a mux plus a RawPeer.
- `GatedTransport` (`OutputReadyTransport`): the test lets frames out with `allow(n)`; `written` records every frame; `on(channelId)` filters them.
- `gatedPair()`.

**`/home/user/switchboard/test/naming_harness.dart`** (412 lines)
- Timing constants; `serverOptions` and `clientOptions` (TalkOptions); URIs `tcp://10.0.0.5:9101` and `ws://10.0.0.6:9100/ws`.
- `until()` polling.
- `Harness`: a NamingService (assignment hold, settle and so on configurable) plus `link({sync})`, which builds a pair of TalkChannels over a StreamChannelController and serves the server side.
- `Connector`: a TalkConnector that can be switched off with `down`.
- `LossyConnector`: drops Talk frames in either direction through predicates.
- `isFinalResponse` and `isRequestFor`.
- Raw procedure helpers `register()` and `unregister()`.
- `Watcher`: WATCH/UNWATCH on a raw channel, with events as strings such as `UP npc/1 …`, `DOWN …`, `WATCHED`.
- `describe` / `describeEvent`; `fuzzInput` for naming-shaped fuzz.

**`/home/user/switchboard/test/sharding_harness.dart`** (410 lines)
- Names `kv`, `zone`, `userq`, `room`; describers.
- `ItemWatch`; `Lookup` (raw LOOKUP).
- `Instance`: a scripted sharded instance on a raw Talk channel. It registers with a fixed id and answers ASSIGN, DRAIN, FORWARD and RESUME through replaceable behaviours. It can send heartbeat EXTENDs and declare deadlines. Helpers: `slots`, `claim`, `release`, `holding`, `locate`. Every request goes to a shared log.
- `Migration` (MIGRATE with PHASE items).
- `SwitchableConnector`.
- `RecordingHandler` (a SlotHandler that logs).
- `ScriptedNaming`: a fake naming service on one link that answers REGISTER, SLOTS, HOLDING, CLAIM and WATCH as scripted and logs requests.

**`/home/user/switchboard/test/fake_binding.dart`** (1435 lines)
- `FakeConnection.pair({initiator, acceptor, controlDelay, channelDelay})`: a BindingConnection with no mux frames. Each channel is an ordered in-process "stream" carrying OPEN, subframes, credit and close. Control messages (PING, PONG, GOAWAY, NONCE, IDENT, closed) go on a separate control "stream" that can be delayed.
- Flow control counts payload plus one byte per record. Windows are each side's `initialWindow`, known in advance. Channel ids: even from 2 for the initiator, odd from 3 for the acceptor, used once.
- IDENT is checked with the core `verifyIdent`. `sent` counts records by kind.
- `FakeChannel` supports priority, manual credit, `openAfter` and `close`.
- `FakeBinding`: a `fake://name` SwitchboardBinding. `listen(node, name, policy)` and `stop(name)`; `dial` makes a pair and adopts the remote end into the listening node; `dialled` lists initiated connections.

**`/home/user/switchboard/test/integration/sharding/cluster.dart`** (211 lines)
- Options: `mux` (grace 100 ms, keep-alive off) and `talkOptions` (2 s / 1 s).
- `until()`, `retryable()` (MOVED, UNAVAILABLE, CONNECTION_LOST, GOING_AWAY, CANCELLED).
- `Cluster(scheme)` with scheme `tcp`, `ws` or `mem`:
  - `start()`: a naming node.
  - `node()`: a node on 127.0.0.1, or in-isolate for mem.
  - `join()` and `member()`: MeshNode.
  - `crash()`: cuts connections without GOAWAY.
  - `close()`: naming first, then meshes, then nodes.
- A mirror-watcher class that records the most slots migrating at once and the migrations done.

**`/home/user/switchboard_quic/test/harness.dart`** (259 lines)
- `fast` options (grace 200 ms, keep-alive off, close confirm 5 s); `bytes`, `text`, `pattern`; `throwsCode`, `hasCode`, `until`, `settle`.
- `connectionPair(...)`: two QuicBindingConnections over `FakeQuicSession.pair`, with per-end FakeEndConfig, network and identities.
- `RawPeer` (a hand-written binding peer on a fake session):
  - `nextStream()`.
  - `openControl({hello, sendHello})`: writes the OPEN record of channel 0 and a HELLO.
  - `acceptControl()`: checks the binding's OPEN(0) and HELLO, answers with its own.
  - `sendControl`, `nextControl`, `nextControlOf(type)`, `openRaw(bytes)`.
  - `drain(stream)` → (records, `'fin'` or reset code); `end`.
- `bindingAndRaw({bindingIsInitiator, …, rawControl, rawHello, isWebTransport})`.

**`/home/user/switchboard_quic/test/fake_provider.dart`** (1029 lines)
- An in-memory QUIC provider. Each stream direction is delivered independently and in order, with a delay per stream (`FakeDelay` by ordinal and opener). Stream and connection flow control: data stays with the sender until read.
- Stream limit (`maxStreams`); RESET and STOP_SENDING with codes (the sender resets on STOP_SENDING); FIN.
- Exporter equal at both ends and different per session; ends without an exporter can be configured.
- `FakeEndConfig`: exporter, `streamWindow` 64 KiB, `connectionWindow` 16 MiB, `maxStreams` 128, `certificateHash`.
- `FakeQuicNetwork(delay, closeDelay)`: `listen(uri)` → FakeQuicListener; `connector(webTransport:)` → FakeQuicConnector (`connectGate`); `pairs`.
- `FakeQuicSession.pair()`: unidirectional streams, `drainPeer`, `close`, `lose`.
- Failure injection: `exporterFailure`, `exporterGate`, `openFailure`, `failIncoming`, `writeFailure`, `finishFailure`, `resetWithoutCode`, `stopWithoutCode`, `failRead`.

**`/home/user/switchboard_quic/test/web_harness.dart`** (214 lines)
- `loadServer()` fetches `web_server.json` next to the test page. It is written by `tool/web_smoke.sh` from the server's READY line; without it the tests are marked skipped.
- `WebServer` fields: `provider`, `limited`, `node`, `silent`, `hash`, `handshakeTimeout`. Methods:
  - `config()`: ALPN `switchboard`, `serverCertificateHashes`.
  - `open(kind, id)`: a session to `?case=kind&id=id`.
  - `report(id)` / `reported(id, test)`: polls `?case=report` for the server's JSON event records.
- `watchUnhandledRejections()`: fails the suite on unhandled promise rejections.
- `newId`, `readAll`; `pattern(n) = (i*31 + (i>>8)) & 0xFF`; `until`, `pendingAfter`.

**`/home/user/switchboard_quic/test/review_cases.dart`** (548 lines)
- `reviewCases()`: one group, "review regressions", of 27 tests using `bindingAndRaw` and `connectionPair`. It is called from binding_test.dart, so it also runs under `-P node`.

---

## C. Server processes in `tool/` and `example/`

Only `switchboard_quic` has servers that report to a test over stdout.

**`/home/user/switchboard_quic/tool/web_server.dart`** (360 lines)
- Needs the native shim.
- **Readiness:** prints one line `READY {"provider":"https://127.0.0.1:P/provider","limited":"https://127.0.0.1:P/limited","node":"https://127.0.0.1:P/sb","silent":"https://127.0.0.1:P/sb","certificateSha256":"<lowercase hex>","handshakeTimeoutMs":2000}`.
- **Lifetime:** serves until SIGTERM or SIGINT, or a 10-minute watchdog, then exits 0. `WEB_SERVER_LOG=1` sends logs to stderr.
- **Certificate:** fresh self-signed ECDSA P-256 for 127.0.0.1, ALPN `switchboard`, any origin.
- **`/provider` and `/limited`** (`/limited` allows only 2 streams): native WebTransport listeners. The query `?case=K&id=I` chooses the behaviour per session:
  - `echo`: read to FIN, write it back, FIN.
  - `sink`: read to FIN.
  - `send`: read a u32 big-endian size, write that many pattern bytes, FIN.
  - `reset`: read, write "partial", reset with 14 after 100 ms.
  - `stop`: STOP_SENDING 9.
  - `close`: `session.close(9, 'server bye')`.
  - `drain`: `session.drain()`.
  - `hold`: do nothing.
  - `open`: the server opens a stream, writes "from server", FIN.
  - `report`: answers, on the first stream, the JSON array of events recorded under that id.
- **Recorded events:** `session`, `exporter` (hex of the `switchboard-ident` exporter, or null), `stream`, `reset`, `stopped` / `acknowledged`, `fin`, `sent`, `end` (code, reason, byPeer, lost), and others.
- **`/sb`:** a Switchboard node behind `QuicSwitchboardListener` with `QuicIdentityRules.none`, so nonce-bound IDENTs are accepted. Verifier authority: Ed25519 `webauth` from seed `11*i+1`. Services:
  - `echo`: replies with the payload.
  - `issue`: the payload is a holder public key; replies with a credential for identity `browser-1`.
  - `control`:
    - TAG: records `tagged`, `connection done` (code, reason) and `peer goaway` under the tag text.
    - WHOAMI: replies with the peer identity.
    - GOAWAY: `connection.goAway()` after 50 ms.
    - CLOSE: `connection.close()` after 50 ms.
- **`silent`:** a UDP port that never answers, for handshake timeouts.

**`/home/user/switchboard_quic/tool/web_smoke.sh`** (123 lines)
- Runs `dart run tool/web_server.dart` and polls stdout for `^READY ` for up to 60 s.
- Writes the JSON to `test/web_server.json` and runs `CHROME_EXECUTABLE=<wrapper> dart test -j 2 -P web`, under a systemd-run memory cap if available.
- Fails on: no "All tests passed!", a "no server" line, an `[E]` line, the server dying, or a non-zero server exit.
- Kills the server and removes the JSON at the end. Prints `SKIPPED` if Chromium is missing.

**`/home/user/switchboard_quic/tool/browser_server.dart`** (148 lines)
- Serves a WebTransport listener at `/sb` with `allowedOrigins` set to the page origin and `advertisedHost: node.invalid`, plus an HTTP page server.
- Prints `READY {"webtransport":"https://127.0.0.1:P/sb?probe=1","page":"http://127.0.0.1:Q/","origin":…,"certificateSha256":hex,"certificateBytes":n}`.
- Serves one session:
  1. Echoes the first stream.
  2. Expects the second stream reset with 42.
  3. Expects the session close with code 7 and reason "browser done".
  4. Expects one 404 for another path.
- Then prints `RESULT {json}` with request origin, path, authority, protocols, draft, protocol, remote, exporter, echoed text, abortedStream, sessionEnd, answered and failures.
- Exits 0 if there were no failures, 1 otherwise, 2 on the 60 s watchdog.

**`/home/user/switchboard_quic/tool/browser_probe.mjs`** (147 lines)
- Usage: `node browser_probe.mjs <devtools port> <page url> <cert sha256 hex> <webtransport url>`.
- Drives headless Chromium over CDP to run the W3C WebTransport API against the server.
- Prints `PROBE {json}`; exits 1 on mismatch, 2 if there is no DevTools endpoint.
- It is the only existing JavaScript-side client of a Dart server, and a starting point for a TS WebTransport harness.

**`/home/user/switchboard_quic/tool/browser_smoke.sh`** (111 lines)
- Starts browser_server, waits for `READY`, extracts fields with node, starts Chromium with `--remote-debugging-port=0`, reads `DevToolsActivePort`, and runs the probe under `timeout 60`.
- Passes only if both the probe and the server exit 0.

**`/home/user/switchboard_quic/tool/native_smoke.dart`** (297 lines)
- Self-contained: no server for others.
- Checks the native provider (pinned client, echo with FIN, exporter on both ends, close code, 64 MiB on one stream), then the binding (exporter-bound HELLOs, echo of a subframe, close seen as GOAWAY 33), then the same over WebTransport.
- Prints `native smoke: binding OK`, `native smoke: WebTransport OK`, `native smoke: OK`; exits 1 on failure, 2 on timeout.

**Other tools:**
- `/home/user/switchboard_quic/tool/aot_smoke.sh` builds and runs native_smoke with `dart build cli` and `dart compile exe`.
- `/home/user/switchboard_quic/tool/fetch_prebuilt.sh` downloads prebuilt shim binaries (network; not used here).
- `/home/user/switchboard_quic/tool/core_commit.txt` pins core commit `3816e6f1397f7719e3e897167332015e6d56c6ba`.

**Core package:**
- `/home/user/switchboard/tool/aot_smoke.dart` runs naming and a mesh node in one process over `mem://`, prints `ok`, exits 0 or 1. `tool/aot_smoke.sh` compiles and runs it.
- `/home/user/switchboard/example/{echo_tcp,endpoint_proxy,mesh,raw_layers,relay}.dart` are single-process demos printing human-readable lines (for example `server: listening on $uri`). They have no machine-readable protocol and do not start a server for another process.
- **There is no existing Dart TCP/WS/mem interop server for a TS test to drive.** One would need to be written, for example modelled on web_server.dart's `READY {json}` handshake and the `control` service.

---

## D. The 25 most valuable two-peer scenarios for Dart ↔ TS interop

Each entry names the Dart file and test, then what both sides must observe. Where the Dart test uses an in-memory pair or fake time, the same scenario can run over real TCP, WS or QUIC with real (short) timers.

1. **Embedded byte-exact round trip.** `integration/embedded_test.dart`, "exact bytes, endpoint announcing no limits", over TCP.
   - The device sends preamble `53 57 42 44 01 00 00 00`, OPEN `0C 00 00 00 12 02 00 01 64 65 76 00 00 00 00 00` and request `0F 00 00 00 02 02 00 03 50 49 4E 47 00 00 00 00 01 00 00`.
   - The endpoint (`announceLimits:false`) sends the preamble, then exactly `07 00 00 00 02 02 00 04 01 00 00`.
   - The device's CLOSE `03 00 00 00 22 02 00` is echoed byte for byte; when the device hangs up, the endpoint's socket closes.
   - Also "a device skipping the LIMITS control frame": input arrives one byte per write; the endpoint sends LIMITS (`10 00 00 00 02 00 00 04 …`) and then the same reply.
   - Either side can be TS.

2. **Window held from the first channel.** `integration/embedded_test.dart`, "a device announcing a 256 byte window is held to it from the first channel on", over TCP.
   - The node sends LIMITS then OPEN `12 02 00`, and no DATA until the peer's LIMITS arrives.
   - The device sends LIMITS `10 00 00 00 02 00 00 04 00 00 00 00 04 00 00 00 00 01 00 00` (frame limit 0, 4 channels, window 256).
   - Only two 109-byte-payload DATA frames go (cost is payload + 16 each, at most 256 in total). The third waits until CREDIT `0E 00 00 00 02 00 00 07 02 00 00 00 00 00 <used u32>` arrives.

3. **Real-transport mux exercise.** `mux_test.dart`, group "real transports", tests "TCP" and "WebSocket" (wide 256 KiB windows).
   - PING both ways.
   - The client opens a channel with payload `AA`; the server sees id 2 and echoes it. A 2-byte subframe and a 100,000-byte subframe come back intact.
   - The server opens bulk-priority channel id 3 with payload `BB` and sends 1,000,000 bytes (`i*13 & 0xFF`); the client receives them intact.
   - The client closes CANCELLED; the server's channel `done` is CANCELLED.
   - The server's GOAWAY makes both connections' `done` GOING_AWAY.
   - Also "TCP: GOAWAY reaches a peer that is still sending".

4. **Flow-control accounting and isolation.** `mux_flow_test.dart`: "window accounting both ways over TCP", "a slow consumer stalls its own channel, another one flows", "the connection backstop pauses reading for all channels together", "OPEN goes at once, DATA once the LIMITS arrived, within the window it announced".
   - 200 subframes each way, sizes `(i*997) % 30000`, filled with `size & 0xFF`; all arrive intact and in order.
   - When idle, each side's sendWindow equals the other's receiveWindow, and both are over 32768. This needs window introspection on the TS side.
   - A paused reader stalls only its own channel.

5. **GOAWAY semantics.** `mux_test.dart`: "peer cannot open after GOAWAY; channels finish, then close", "OPEN racing our GOAWAY is rejected with GOING_AWAY", "grace expiry closes with channels still open", "goAway with no channels closes at once" (exactly `02 00 00 03 21 00`), "peerGoAwayStatus: the status the peer sent".
   - After B's GOAWAY, `a.peerGoingAway` is set and opening on either side fails FAILED_PRECONDITION locally. The existing channel still carries data both ways, and both sides report GOING_AWAY once it closes.
   - A racing OPEN gets CLOSE GOING_AWAY.
   - When the grace expires, the sender's channels end UNAVAILABLE and the peer's GOING_AWAY carries the reason ("maintenance").
   - An error GOAWAY keeps its code in `peerGoAwayStatus`, but `done` is GOING_AWAY.

6. **LIMITS and the channel cap.** `mux_test.dart`: "LIMITS is the first frame, byte exact" (`02 00 00 04 00 10 00 00 07 00 00 00 00 00 01 00` for 4096 / 7 / 64 KiB), "peer OPEN beyond maxChannels gets CLOSE RESOURCE_EXHAUSTED", "LIMITS mid-connection applies to later frames", "frame over maxFrameSize ends the connection". Also `mux_limits_test.dart` "many unconfirmed rejections end the connection with GOAWAY RESOURCE_EXHAUSTED".
   - An acceptor with `maxChannels` 1 rejects the second OPEN with RESOURCE_EXHAUSTED. DATA racing the rejection is not an error, and the connection stays open.

7. **PING/PONG and keep-alive.** `mux_test.dart`: "PING is answered with the same payload", "PINGs with 126 to 1024 byte payloads are answered", "a dead peer leads to CONNECTION_LOST", "two live peers keep the connection open", "traffic postpones keep-alive PINGs".
   - PONG echoes the payload exactly; a 1024-byte payload is accepted on receive.
   - A silent peer reaches CONNECTION_LOST after interval plus timeout; two live peers stay connected.

8. **Status mapping across CLOSE and GOAWAY.** `mux_limits_test.dart`: "CLOSE: reported as UNKNOWN and relayable", "GOAWAY: reported as GOING_AWAY with the code", "CLOSE sends UNAVAILABLE and retains the local status", "only the code changes: the bytes after it are relayed", "a RELOCATED status crosses a CLOSE byte for byte", "an 1100 byte GOAWAY reason does not break a conforming peer", "a CLOSE reason fits the peer frame limit". Also `mux_talk_test.dart` "CONNECTION_LOST goes on the wire as UNAVAILABLE".
   - A peer's CLOSE `22 02 00 2C 01 61` is confirmed with `22 02 00` and reported as UNKNOWN "code 300: a".
   - A GOAWAY `02 00 00 03 2C 01` gives `done` GOING_AWAY "unknown: code 300".
   - CONNECTION_LOST goes out as `0E 00 …`, keeping the reason bytes.
   - A RELOCATED status (owner 0x80FF80FF80FF, epoch 0x80808080) arrives byte-identical.
   - With a 64-byte frame limit, the reason is truncated to 59 bytes.

9. **Channel ids, short and long form.** `mux_test.dart`: "parity and increment", "long ids use the long form on the wire", "a long form id addresses a channel opened in short form", "ids stay short over many channels to a short-id-only peer", "shortIdsOnly refuses long form frames with UNSUPPORTED". Also `vectors/mux_vectors_test.dart` "a long id above 32 bits addresses its own channel".
   - The initiator uses even ids and the acceptor odd ones; reserved ids are skipped.
   - 0x100000003 and 3 are distinct channels, and the CLOSE for the long id uses the long form.

10. **NONCE/IDENT handshake.** `identity_test.dart`: "bearer credential: identified without a proof", "holder-key credential with a proof and an intent", "both directions, independently and at once", "a second valid IDENT replaces the first", "frames after an IDENT wait for its verification". Over real sockets: `identity_node_test.dart` group "over tcp" — "mutual identity", "a forged IDENT gets GOAWAY UNAUTHENTICATED", "a credential of another authority fails the connect with UNAUTHENTICATED".
    - The verifier sees identity `npc-7` (bearer, `provedPossession` false) or `worker-a1` (proved, intent `01 02 03`).
    - An IDENT followed by OPEN `12 02 00 00` in one burst: the OPEN is delivered only after verification, with the identity already set.
    - Use the C1, C2 and P vectors (RFC 8032 keys) so both implementations share credentials.

11. **Receiver binding and identity protocol errors.** `identity_test.dart`: "the named receiver accepts; another refuses", "a relay cannot rename the receiver", "a receiver without an identity refuses a named IDENT", "an unnamed IDENT is accepted unless requireNamedIdent", "a bearer IDENT with a proof is rejected", "a replayed IDENT from another connection is rejected", "a peer that echoes our NONCE cannot send our IDENT back to be identified as us"; protocol errors "NONCE of 31 bytes", "a second NONCE", "IDENT before the nonces", "an IDENT with an intent over 64 bytes".
    - Refusals end in GOAWAY UNAUTHENTICATED; malformed sequences end in GOAWAY PROTOCOL_ERROR.

12. **Channels held until IDENT.** `identity_node_test.dart`: "a channel arriving before IDENT is held, then admitted", "a channel held past identityTimeout is refused UNAUTHENTICATED", "a held channel the peer closes is dropped". Also `binding_protocol_test` "IDENT verification gates OPEN, PING and a queued IDENT".
    - The PONG and the OPEN delivery happen only after verification; on failure, no PONG is sent and the connection ends UNAUTHENTICATED.

13. **Talk core semantics.** `talk_test.dart`: "items then a final response with trailing payload", "abort response carries the exact status and reason", "handler exceptions are answered with ABORT INTERNAL", "requester cancels: the channel answers CANCELLED at once and the responder sees onCancel", "an unknown response id with HAS_REQUEST gets ABORT NOT_FOUND", "maxIncomingRequests exceeded: ABORT RESOURCE_EXHAUSTED", "interleaving: 200 requests in flight, replied in reverse", "chained replies, several hops deep", "request ids wrap and skip ids still outstanding", "an empty abort response payload means UNKNOWN". Also `mux_talk_test.dart` "a talk protocol error closes only that channel".

14. **Talk item windows and credit.** `talk_window_test.dart`: "a finite window delivers exactly that many items, then nothing until a grant", "window 0 starts exhausted, and the first grant starts delivery", "the final response, an abort and an extend pass at zero", "item requests count as one item each", "$label closes the channel PROTOCOL_ERROR" (bad credits), "a credit for a request that is not a stream request closes the channel PROTOCOL_ERROR", "an item beyond the window is a channel protocol error on the responder".
    - The wire form is a credit `32 <id u24> <items u32>`. The responder never sends more than the granted items.

15. **Talk EXTEND and declared deadlines.** `talk_test.dart`: "extend restarts both timers", "responder side: ABORT DEADLINE_EXCEEDED, later replies throw", "requester side: fails locally and sends a cancel", "a deadline replaces the default gap; items do not move it", "an empty EXTEND restarts the default gap, keeping what was declared; zero fields count as a reply", "values below minExtension are raised to it". Also `talk_forward_test.dart` "EXTEND payloads pass through byte for byte".
    - The Dart test uses fake time; port it with 100–500 ms timeouts.

16. **Talk bulk payloads over TCP.** `talk_bulk_test.dart`, group "over TCP": "a request with a bulk payload of known length" (300,000 bytes, `bulkLength` 300,000, answer `[30]`), "a request with a bulk payload of unknown length" (123,457 bytes in 777-byte pieces), "a response, stream items and a plain message with bulk payloads", "a payload too large for a frame goes bulk on its own".
    - Inline up to `maxSubframeLength`−12 for a request; one byte more goes bulk. A response header is 8 bytes shorter.
    - On the wire: a `_bulk` OPEN (parent id, bulk number) plus the BULK flag and a 10-byte reference.

17. **Bulk channel rules and ordering.** `talk_bulk_test.dart`: "a reference to a bulk number never opened is a channel protocol error", "a bulk number referenced twice is a channel protocol error", "a bulk channel for no open parent is refused FAILED_PRECONDITION", "a bulk channel carrying another length than its reference is a …", "a message keeps its place while its payload is reassembled" (A 1, B 500000, C 2, D 100000 arrive in order), "a parent closing cancels its bulk channels in transfer". Also `switchboard_test.dart` "naming no open channel are refused FAILED_PRECONDITION".

18. **Node dispatch, policy and close.** `switchboard_test.dart`:
    - "exact instance", "unknown instance or type is not found" (CLOSE NOT_FOUND), "malformed open payload is a channel protocol error", "default service for channels without a type", "denyReserved on a WebSocket listener".
    - "the server opens a channel back to a client service" (symmetric dispatch).
    - "sends GOAWAY, stops listening and refuses new work", "a connection whose peer sent GOAWAY is not reused", "a GOAWAY between connect and OPEN is retried once".
    - "the mux frame limit reaches both ends".

19. **WebSocket handshake and framing.** `web_socket_server_test.dart`: "a good request is switched with the switchboard protocol", "a client offering no subprotocol is accepted", "$name is refused with $status" (400/426), "a text message: 1003", "a single message over the limit: 1009 and FRAME_TOO_LARGE", "a close from the client is echoed and ends the stream", "dart:io WebSocket client: messages, compression refused, client close", "a mux connection over it, both closes give goingAway". Also `transport_test.dart` "the default limit is 1 MiB"; `mux_test.dart` "a text message: GOAWAY PROTOCOL_ERROR" and "a message over the limit: GOAWAY FRAME_TOO_LARGE".
    - Run a TS browser or `ws` client against the Dart server, and a dart:io client against a TS server.

20. **Naming service protocol.** `naming_test.dart`: "assigns increasing ids across types, starting at 1", "a requested id in use is ALREADY_EXISTS", "watch: snapshot events, the reply, then live UP and DOWN", "channel close removes its records; watchers see DOWN", "LOOKUP streams the records of a type, then ends", "REGISTER racing a WATCH snapshot: exactly one UP", "reconnects, re-registers the same ids, resyncs with DOWN". Also `integration/naming_mesh_test.dart` "over tcp/ws": "naming service is registered as _ns/1 and ids are global", "resolution: round robin, exact instance, shards, leave, notFound", "naming service restart: stale table, same ids, resync".
    - Run a TS NamingClient against a Dart NamingService and the reverse. The watch order is snapshot UPs, then the reply, then live events.

21. **Sharding hand-over.** `sharding_test.dart`: "hand-over: DRAIN, ASSIGN, table, FORWARD", "ASSIGN refused: RESUME, epoch unchanged", "lazy: assigns on first use, by spare capacity", "snapshot: SLOTSPC before its slots, empty slots left out". `sharding_client_test.dart`: "claim through ASSIGN; the mirror and the resolver follow", "migration seen from the instances, in protocol order", "naming service restart: resume, re-send, re-claim by epoch". `slot_routing_test.dart`: "retried once at the owner the reason names", "never on RELOCATED: the slot moved after work started". End to end: `integration/sharding/kv_store_test.dart` "tcp kv: slots move one at a time while clients write…".

22. **Proxy and relay chains with mixed implementations.** `proxy_test.dart`: "request and reply through the endpoint", "bulk payloads through the endpoint, both ways", "backend shutdown is GOING_AWAY at the client", "MOVED before any data: retried once, unseen by the client" (the retry re-sends the OPEN payload with instance set to the new owner), "revealOwners: MOVED and RELOCATED byte for byte". `integration/naming_mesh_test.dart` "a raw mux peer without a Switchboard". `relay_test.dart` "a channel through the relay: request and reply end to end; …", "MOVED is relayed with its fields byte for byte". `talk_forward_test.dart` "a client cancel reaches the backend", "${code.name} abort fields and reason pass through byte for byte".
    - Example chain: a TS client, a Dart proxy or relay, then a TS backend.

23. **ReconnectingClient against a server's GOAWAY or close.** `reconnecting_client_test.dart`: "persistent channels move to a new connection at once", "on an idle connection: reconnects at once", "an application channel carries on over the old connection", "closed by the endpoint with ${code.name}: stays closed", "PersistentTalk re-sends its subscription on every connection". Over QUIC: `web_binding_test.dart` "reconnects after the node closes the connection, and serves again" and "moves to a new connection on the node's GOAWAY".

24. **QUIC records, HELLO and close.** Against the Dart native node (`native_integration_test.dart`, or `tool/web_server.dart` `/sb`):
    - `web_binding_test.dart` "the connection: HELLO both ways, the session WebTransport, the certificate authenticating the node".
    - `vectors_test.dart` group "example": control stream `06 00 00 40 00 …` + HELLO.
    - `binding_test.dart`: "even from 2 for the initiator, odd from 3 for the acceptor, …", "the largest subframe is the peer's record limit, which its HELLO raises", "a record over the record limit ends the connection with FRAME_TOO_LARGE, its body unread", "a CLOSE record and FIN, confirmed by the peer's", "a rejection: a CLOSE record and STOP_SENDING with the status; …", "a reset is the peer's close by its code: …", "STOP_SENDING is the peer's close arriving early: …", "GOAWAY: no new channels either way, the open ones finish, then the session closes", "a channel the peer opened before it saw the GOAWAY is rejected GOING_AWAY, a bulk channel for an open one admitted", "a received application close counts as a GOAWAY with its status".
    - `native_integration_test.dart`: "GOAWAY with a code and a reason, from the dialler" (session end is `(14, 'upgrade', byPeer: true, lost: false)`) and "… from the listener"; "a WT_DRAIN_SESSION reads as a GOAWAY with GOING_AWAY, …".
    - `web_binding_test.dart` "a close from the browser: the node reads it as GOING_AWAY".

25. **QUIC identity and bulk at scale.**
    - `binding_test.dart`: "both HELLOs with EXPORTER: the proof signs the exporter", "a HELLO without EXPORTER: the proof is nonce-bound", "a side that requires the binding neither sends nor accepts a nonce-bound IDENT", "an initiator that requires the binding closes UNAUTHENTICATED on a HELLO without EXPORTER".
    - `native_integration_test.dart`: "nodes identify both ways with exporter-bound proofs over the TLS exporter both ends derived" (exporter label `switchboard-ident`, 32 bytes, identical at both ends), "an 8 MiB bulk payload each way", "64 channels in parallel on one connection, three times over: the stream credit of ended channels comes back", "whenWritable paces a publisher at the stream window", "the proxy: a frame mux backend behind a QUIC front, bulk twins forwarded in order".
    - QUIC `switchboard_test.dart`: "a 3 MiB bulk payload each way, its streams overtaking the parent and arriving out of order".
    - `web_binding_test.dart`: "a nonce-bound IDENT from the browser, accepted by a listener with QuicIdentityRules.none" (`issue` then WHOAMI gives `browser-1`).
    - The X vector in `vectors/exporter_identity_vectors_test.dart` gives a fixed cross-check of the exporter-bound proof.

### Coverage

Where the scenarios above are checked now (`interop.md`, sections 11 and 12, has the detail).

**In the TS-driven cells** (`test/interop/scenarios/`, the four pairings on every transport) **and the probes** (`test/interop/probe/`):
- **3, 5, 8, 13 to 18, 20, 22:** the scenario files of the same names (`close`, `mux`, `talk`, `talk-wire`, `bulk`, `basics`, `naming`, `forward`, and the chains in `mesh`).
- **10 to 12:** `identity.test.ts`, exporter-bound over QUIC.
- **21:** `sharding.test.ts`: lazy and eager allocation, refused ASSIGN and DRAIN with RESUME, an owner going down mid-hand-over, the queue, the MOVED retry of `openChannelToSlot` and `openTalkToSlot`, never on RELOCATED.
- **23:** `reconnect.test.ts`, on all four transports.
- **24:** `probe/quic.test.ts`, a raw QUIC and WebTransport peer built on the native provider: HELLOs, records, the negative vectors, FRAME_TOO_LARGE, rejections with STOP_SENDING, resets both ways, the confirmation timeout, GOAWAY, application closes, WT_DRAIN_SESSION, keep-alive. One case is skipped in its four cells, with the reason: both implementations can close the session before their GOAWAY record is acknowledged, which loses it.
- **1, 2, 6, 9 in part:** `probe/conformance.test.ts` over TCP and WebSocket: the preamble, LIMITS first, the channel window, the channel limit, the negative mux vectors.
- **7 and 19 in part:** PING payloads and live peers keeping an idle connection open (`mux.test.ts`); the `switchboard` subprotocol and 1009 on an oversized message (`probe/conformance.test.ts`).

**In the Dart driver** (`test/interop/dart_driver/`), for what the Dart node itself sees of a TS peer:
- **4 (in part):** a paused Dart reader's held bytes and windows; a Dart send window stalled at the window the TS peer announced.
- **6 (in part):** the LIMITS the Dart connection read; an OPEN past the TS channel limit; the pool opening a second connection at the limit.
- **13 and 14 (handler side):** the item window and credit as a Dart handler reads them; the TS requester's timeout and cancel reaching `onCancel`.
- **16 and 17:** `maxInlinePayload` refusing before the handler; `streamBulk` not held to the limit.
- **21:** ASSIGN and DRAIN refusals by Dart handlers and the roll-back, against the TS naming service; the slot mirror; the Dart router's MOVED retry and RELOCATED with no retry.
- **23:** the Dart ReconnectingClient on a TS server's GOAWAY, CLOSE statuses and a dropped connection, over all four transports.
- **24 and 25:** the TS listener's HELLO equal to a Dart listener's; its GOAWAY code and reason in the QUIC session end; WT_DRAIN_SESSION; 64 parallel channels three times over, with bulk payloads.

**Still open:**
- **4:** idle window equality needs window introspection on the TS side; the connection-level backstop is not covered.
- **6:** LIMITS sent mid-connection has no peer command, and unconfirmed rejections need a raw peer.
- **7:** a silent peer ending CONNECTION_LOST over TCP and WebSocket needs a transport the test can silence (the QUIC probe's silent peer gets DEADLINE_EXCEEDED).
- **17:** `reassemblyBudget` back-pressure.
- **21:** a naming-service restart with re-claims by epoch (it needs a peer restarted on the same port); slot-request EXTEND deadlines.
- **24:** what the native provider cannot produce: a unidirectional stream, datagrams, codeless resets on the control stream, opens beyond the peer's stream credit.
- **19:** the handshake refusals (400, 426) and the text message (1003), which need a raw WebSocket client; the browser client comes with the browser peer, out of scope here.
