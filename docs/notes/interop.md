# Switchboard TS: interop testing against the Dart implementation

Phase 1 notes of the thread "Dart interop test harness". Everything marked
*measured* was run in this cloud container on 2026-10-14; everything marked
*proposal* waits for the lead's design (package layout, test runner, module
boundaries) and may move.

Inputs: `kaetemi/switchboard` at `3816e6f` (core), `kaetemi/switchboard_quic`
at `c5b3265` (QUIC, pins core `3816e6f` in `tool/core_commit.txt`), the wiki
in `kaetemi/sway` at `e8c2206` (`wiki/switchboard/*.md`). The Dart
repositories were not changed: `dart pub get` and the cargo build only wrote
ignored files (`.dart_tool/`, `pubspec.lock`, `native/sbq/target/`).

## 1. Summary

* **The Dart implementation can serve as a test peer without changing it.**
  A small Dart package that depends on both Dart packages by path builds to
  an AOT executable that serves TCP, WebSocket, raw QUIC and WebTransport,
  acts as a client over the same four, and answers codec queries. A
  prototype exists and works (section 3).
* **Measured end to end**: a hand-rolled JavaScript client (no Switchboard
  code, raw bytes) completed request, stream request, abort and the close
  handshake against the Dart peer over TCP and over WebSocket; the Dart peer
  as a client completed requests to TCP, WebSocket, QUIC and WebTransport
  listeners; the Dart codecs agreed with all 69 non-composite wiki vectors of
  the Talk, mux and address tables; headless Chromium 141 opened a
  WebTransport session to the Dart native listener (echo, reset code, close
  code and reason all correct).
* **Approach** (proposal): one language-neutral *peer protocol* (JSON lines
  over stdin/stdout) spoken by a Dart peer and a TS peer. TS tests drive any
  two peers through it, so every scenario runs as Dart↔Dart (which checks
  the test's expectations against the reference), TS→Dart, Dart→TS and
  TS↔TS. A Dart driver can spawn the TS peer later to run Dart-written
  scenarios against TS. Four test layers: codec differential (wiki vectors
  plus fuzzing against the Dart oracle), two-peer scenarios over real
  transports, mixed-language meshes, and raw byte conformance scripts
  checked against the Dart server first.
* **CI is feasible on ubuntu-latest**: setup-dart 3.13.5, the Rust toolchain
  for the QUIC shim (no prebuilt shim is released yet), sibling checkouts of
  the Dart repositories at pinned commits, `dart build cli`, cached.

## 2. Environment facts

| Fact | Detail |
| --- | --- |
| Dart SDK here | Not preinstalled. The stable zip installs cleanly: `https://storage.googleapis.com/dart-archive/channels/stable/release/3.13.5/sdk/dartsdk-linux-x64-release.zip` → `/opt/dart-sdk` (*measured*: a 239 MB download, under a minute). pub.dev is reachable through the proxy. |
| Dart SDK in GitHub Actions | `dart-lang/setup-dart@v1` with `sdk: 3.13.5`, as `switchboard_quic/.github/workflows/ci.yml` already does. Both packages require `sdk: ^3.13.0`. |
| Core suite | `dart pub get && dart test` in `switchboard`: **1840 tests pass in 51 s** (*measured*). `dart test test/vectors`: 304 tests in 11 s. `dart test -P node` runs the codec, vector, identity, reconnecting client and item window tests compiled to JS on Node.js. |
| QUIC suite | `dart test -j 2` in `switchboard_quic`: **312 pass, 2 skipped, 1 min 56 s including the cold cargo build of the shim** (*measured*, cargo 1.97 here; CI pins Rust 1.99.0). Needs `../switchboard` as a sibling. |
| QUIC native shim | `native/sbq` (Rust, quinn 0.11 + rustls, HTTP/3 and WebTransport implemented in the shim). `hook/build.dart` uses a prebuilt `native/sbq/prebuilt/<os>_<arch>/libsbq.so` if present, else runs `cargo build --release --locked`. **No GitHub releases exist yet** (list_releases is empty), so CI must build it with cargo; `Swatinem/rust-cache` keeps that to a cache restore after the first run. |
| Visibility | `kaetemi/switchboard`, `kaetemi/switchboard_quic`, `kaetemi/switchboard_ts` are public: Actions can check them out without a token. `kaetemi/sway` (the wiki) is **private**: CI cannot read the wiki, so the vectors are committed to switchboard_ts as extracted JSON (section 6). |
| Pinned git dependencies | **Do not work**: `switchboard_quic`'s pubspec has `switchboard: path: ../switchboard`, and pub refuses a path outside a git dependency's repository even with a `dependency_overrides` entry (*measured*: "the path ../switchboard cannot refer outside the git repository"). The peer therefore needs sibling checkouts, the layout `switchboard_quic`'s own CI uses. |
| AOT build | `dart build cli -t bin/dart_peer.dart -o build/peer`: **7 s** once the shim is built; bundle = 7.9 MB executable + 2.7 MB `libsbq.so` (*measured*). Startup to `READY` and shutdown: **10 ms**. `dart run` instead: about 5 s per start. `dart compile exe` runs no build hooks; the bundle from `dart build cli` is the one to use. |
| `dart run` noise | `dart run` writes `Running build hooks...` to **stdout without a newline**, so the first line the peer prints is glued to it. The launcher must find `READY ` anywhere in a line (or use the AOT bundle, which prints nothing extra). |
| Node.js | 22.22 here. Global `WebSocket` client works and negotiates the `switchboard` subprotocol. **No `node:quic`** in 22 (neither the module nor `--experimental-quic`). Which QUIC stack the TS port uses on Node is the QUIC thread's call; the peer offers raw QUIC and WebTransport endpoints either way. |
| Browser | Chromium 141 (`/opt/pw-browsers/chromium-1194/chrome-linux/chrome`, Playwright 1.56.1 installed globally) ran `switchboard_quic/tool/browser_smoke.sh` against the Dart native WebTransport listener: secure context, session ready, stream echo, stream abort code 42 seen by the server, another path refused, `close({closeCode: 7, reason})` seen by the server. It failed only the protocol check: Chromium 141 does not negotiate `protocols: ['switchboard']` (`wt.protocol` undefined; the wiki says Chrome 149 does), and `exportKeyingMaterial` is undefined, so IDENT from a browser is nonce-bound. The Dart server accepts sessions without a protocol, so Switchboard works; CI should use a current Chromium for the protocol check. |
| Dart TLS | The Dart node listens on `ws://` and `tcp://` only (no TLS listener; it dials `wss://`). TLS interop exists only through QUIC/WebTransport. |

## 3. The Dart peer

### 3.1 What it is (proposal)

A Dart package inside switchboard_ts, for example
`test/interop/dart_peer/` (the lead decides the path), with:

```
test/interop/dart_peer/
  pubspec.yaml            path deps ../../../../switchboard and ../../../../switchboard_quic
  analysis_options.yaml   the Dart repos' lint set
  dart_commits.txt        the two pinned commits (switchboard = switchboard_quic's core pin)
  bin/dart_peer.dart      entry point: argument parsing, the stdin/stdout loop
  lib/src/protocol.dart   command dispatch, handles, events
  lib/src/node.dart       the node's configuration, listeners, services
  lib/src/services.dart   the interop services (3.4)
  lib/src/oracle.dart     codec decode/encode (3.6)
  lib/src/client.dart     the client operations (3.5)
```

`pubspec_overrides.yaml` (gitignored) lets a developer point at other
checkouts. A small script (`scripts/dart-peer.mjs`, proposal) does `dart pub
get` and `dart build cli`, writes a stamp with the commits and a hash of the
peer's sources, and skips the build when the stamp matches. Tests find the
executable through `SWITCHBOARD_DART_PEER`, else the bundle path, else fall
back to `dart run` (slow but works).

The prototype is in
[interop-prototype/dart_peer](docs/interop-prototype/dart_peer/bin/dart_peer.dart)
(its pubspec uses this container's absolute paths, `/home/user/switchboard`
and `/home/user/switchboard_quic`; `build.sh` installs Dart if needed and
builds the bundle). Other threads can use it now, for example to point a
Node QUIC or WebTransport client at a reference listener.

### 3.2 Process protocol (proposal)

* **Start**: `dart_peer serve [--config <json>]`. The config chooses
  listeners, options, identity and services (3.3).
* **stdout** carries exactly one `READY {json}` line, then JSON lines only:
  responses and events. **stderr** carries logs (`--log fine` turns on
  `package:logging`), which the launcher collects and prints when a test
  fails.
* **`READY`** names every endpoint and what a client needs to reach it:
  `{"tcp": "tcp://127.0.0.1:p", "ws": "ws://127.0.0.1:p/sb", "quic":
  "quic://127.0.0.1:p", "webtransport": "https://127.0.0.1:p/sb",
  "certificateSha256": "<hex>", "pid": n, "versions": {"switchboard": "<sha>",
  "switchboard_quic": "<sha>"}}`.
* **Commands** on stdin, one JSON object per line: `{"id": 7, "op":
  "talk.request", ...}`. Each gets exactly one response line `{"id": 7,
  "ok": true, "value": ...}` or `{"id": 7, "ok": false, "error": {"code": 5,
  "reason": "...", "type": "SwitchboardException"}}`. Commands run
  concurrently; responses can come out of order.
* **Events** are unsolicited lines `{"event": "channel.closed", "handle":
  "ch3", ...}` (3.7). The launcher keeps them in a log the test can await
  with a predicate and a timeout.
* **Lifetime**: the peer exits on stdin EOF (so a crashed test runner leaves
  no orphan), on `shutdown`, on SIGTERM/SIGINT, or on a watchdog (default
  10 minutes, configurable), as `switchboard_quic/tool/web_server.dart` does.
* **Handles**: every object a command creates (connection, channel, Talk
  channel, request, stream, listener) gets a string handle (`c1`, `ch4`,
  `rq9`). `reset` closes everything the previous test created and keeps the
  listeners, so one peer process serves a whole test file.

The prototype implements `READY`, the JSON-lines loop, `ping`, `shutdown`,
`decode` (talk, mux, address) and `request` (client mode, any scheme).

### 3.3 Node configuration

Everything a scenario needs to vary, passed in `--config` or a
`configure` command before the first listener starts:

* `listen`: any of `tcp`, `ws` (path), `quic`, `webtransport` (path), on
  127.0.0.1 port 0. The certificate is generated
  (`NativeQuicProvider.generateSelfSigned`, ECDSA P-256, 14 days, browser
  acceptable for `serverCertificateHashes`) unless one is given.
* `mux`: `MuxOptions` fields that matter across processes:
  `keepAliveInterval`, `keepAliveTimeout`, `goAwayGrace`,
  `closeConfirmTimeout`, `maxFrameSize`, `maxChannels`, `initialWindow`,
  `identityTimeout`, `requireNamedIdent`, `awaitPeerLimits`.
* `talk`: `TalkOptions` (request timeout, responder timeout,
  `maxInlinePayload`, `reassemblyBudget`, `bulkIdleTimeout`, which
  procedures stream their bulk).
* `quic`: `recordLimit`, `QuicIdentityRules` (`none`, `listener`,
  `initiator`), stream limits, idle timeout.
* `identity`: deterministic keys so both languages share an authority: the
  wiki's RFC 8032 test 1 seed as the Ed25519 authority, `00 01 … 1F` as the
  HMAC key, holder keys from fixed seeds. The peer can issue, present and
  verify credentials (`credential.issue`, `identify`, a verifier with the
  authority).
* `policy`: none, `denyReserved`, `allowTypes`, `scoped`.
* `services`: which interop services to register and with which instance
  ids; optionally a naming service (`_ns/1`) the other peer can join, a
  relay, a sharded store.

Timeouts cannot use virtual time across a process boundary, so every timeout a
scenario exercises is configured short (100 to 500 ms) on the side that
enforces it, and tests wait for events instead of sleeping.

### 3.4 Interop services

Fixed, documented behaviour that both peers implement identically (the
gRPC interop test service is the model). Procedures on type `echo`
(proposal; names are 8-byte Talk procedure names):

| Procedure | Behaviour | Exercises |
| --- | --- | --- |
| `ECHO` | Final reply with the request's payload. | Requests, payload sizes (inline and bulk both ways). |
| `UPPER` | Reply with the UTF-8 payload upper-cased. | Trivial check. |
| `SIZE` | Reply with the request payload's length and SHA-256. | Large and streamed uploads (bulk) without echoing them. |
| `BIG` | Reply with n patterned bytes (n in the payload), inline or bulk as the size decides. | Downloads, bulk channels opened by the peer. |
| `BIGSTRM` | Reply with a streamed bulk of n bytes, length declared or unknown. | `replyBulk` from a stream, unknown length. |
| `COUNT` | n stream items, then a final reply. | Stream requests. |
| `ITEMS` | n stream items honouring the item window and credits; reports when it waited. | Item windows and `CREDIT`. |
| `ABORT` | Abort response with the given code and reason. | Status payloads, `MOVED`/`RELOCATED` fields. |
| `SLOW` | `EXTEND` with a deadline, then the reply after the given delay. | Responder deadlines, requester timeouts. |
| `STALL` | Never answers. | Requester timeout, cancel. |
| `CHAIN` | Final response that is itself a request back to the requester; replies with what it got. | Chained responses. |
| `NOTIFY` | Sends a plain message back on the channel, then replies. | Plain messages, ordering. |
| `CLOSE` | Closes the channel with the given status after replying. | Channel close from the far side. |
| `GOAWAY` | Replies, then sends `GOAWAY` on its connection. | Graceful shutdown, `GOING_AWAY` for later opens. |
| `WHOAMI` | Replies with the peer's verified identity (empty without one). | IDENT, credentials. |
| `SINK` | Reads a streamed bulk upload at a configured pace (or pauses), replies with the size. | Flow control and backpressure. |

Types `sink` (a reader that never reads, to stall one channel) and an
unregistered type (`NOT_FOUND`) and a policy-refused type
(`PERMISSION_DENIED`) complete the set.

### 3.5 Client operations

The peer as the initiator, against the other peer's listener:

* `connect {endpoint, certificateSha256?, identity?}` → connection handle.
* `open {connection | endpoint, address, payload}` → channel handle; the
  open payload is either structured (`type`, `instance`, `shard`, `host`,
  `payload`) or raw hex, so a scenario can send a malformed one.
* `talk.request {channel, procedure, payload, bulk?, timeoutMs?}` → reply or
  status; `talk.stream {channel, procedure, window}` → stream handle whose
  items arrive as events; `talk.credit`, `talk.cancel`, `talk.message`.
* `channel.send {channel, hex}` and `channel.close {channel, status}` for
  subframe-level scenarios below Talk.
* `connection.ping`, `connection.goAway {status}`, `connection.close`,
  `connection.identify`.
* `mesh.join {namingEndpoint}`, `mesh.publish {type}`, `naming.watch`, for
  mixed meshes.

### 3.6 Codec oracle

`decode {kind, hex}` answers the reference decoder's fields as canonical
JSON plus the re-encoding, or the error (`ProtocolException`,
`FormatException`, ... with message). `encode {kind, fields}` is the
reverse. `decodeMany {kind, hexes: [...]}` takes thousands per call for
fuzzing. Kinds, all public in `package:switchboard/core.dart` or
`package:switchboard_quic/core.dart`: `name`, `status`, `streamPreamble`,
`streamFrame`, `mux` (`MuxFrame`), `control` (`MuxControlMessage`, LIMITS,
CREDIT, GOAWAY, NONCE, IDENT), `talk` (`TalkFrame`, bulk reference),
`address` (`ChannelAddress`, `_relay`, `_bulk`), the naming payloads
(REGISTER, UP, DOWN, WATCH, CONNECT, ...), the sharding payloads (SLOTS,
SLOTSPC, HOLDING, CLAIM, LOCATE, MIGRATE, ASSIGN, DRAIN, SLOT, ...),
`credential` (decode, verify with the test authority), `identProof`,
`quicRecord`, `quicHello`. The prototype has `talk`, `mux` and `address`;
JSON numbers carry 48-bit ids and u32 windows exactly (both below 2^53).

### 3.7 Events

Each event names the handle and what the Dart side observed, for assertions
the TS side cannot make about its peer:

* `connection.opened {connection, transport, remote, peerLimits | peerHello}`
* `connection.goAway {connection, status}` (the peer's GOAWAY), `connection.done {connection, status}`
* `ident.verified {connection, identity, exporterBound}`, `ident.failed`
* `channel.incoming {channel, connection, address}`, `channel.closed {channel, status, sentStatus}`
* `talk.message {channel, procedure, requestId, payloadSize}` (services in record mode), `talk.item {stream, payload}`, `talk.done {request, status | payload}`
* `bulk.opened`, `bulk.done {size, sha256}`
* `flow.stalled`, `flow.resumed` for the pacing scenarios

A test tags its work so events are attributable when several run on one
peer: the channel's application payload or the connection's first request
carries a tag the events repeat (as `web_server.dart`'s `TAG` does).

## 4. Integration test approach

### 4.1 Peers and the matrix (proposal)

* A **TS peer** (`test/interop/ts_peer/main.ts`, run with Node) speaks the
  same protocol over stdin/stdout, built on the TS port's public API.
* The TS test files program against one `Peer` interface (`rpc`, `events`,
  `ready`) and get their two peers from a matrix helper, so one scenario
  runs as:
  * **Dart server, Dart client**: proves the scenario's expectations against
    the reference. A scenario that fails here is a broken test, not a TS bug.
  * **TS client → Dart server** and **Dart client → TS server**: the interop
    cells.
  * **TS ↔ TS**: the port against itself over real sockets (in-process
    variants belong to the core thread's unit tests).
* Transports are a second axis: `tcp`, `ws`, `quic`, `webtransport`, each
  cell enabled when both sides support it (the TS side's QUIC on Node
  depends on the QUIC thread's stack choice). The browser is a third peer
  kind: a Playwright page running the TS browser build, against a Dart
  WebTransport and WebSocket listener, for the client half of the matrix.
* Peers live per test file (start in `beforeAll`, `reset` between tests,
  stop in `afterAll`); startup is 10 ms for the AOT Dart peer, so isolation
  per file costs nothing.

### 4.2 Test layers

1. **Codec differential** (fast, runs on every push):
   * every wiki vector (section 6) through the TS codec, asserting the bytes
     and the decoded fields; the same vector through the Dart oracle as a
     check that the expectations transcribed in TS match the reference;
   * property tests (fast-check or similar, the lead's choice): random valid
     structures encoded by TS must decode in Dart to the same fields and
     re-encode to the same bytes; random and mutated byte strings decoded
     by both must agree on accept or reject and on the fields. These are the
     cases fixed vectors miss in JavaScript: u32 fields above 2^31 (sign
     bugs with `|` and `<<`), 48-bit ids, u24 request ids, UTF-8 names at 8
     bytes, truncation at every offset, reserved and compatible bits.
2. **Two-peer scenarios** over real transports (section 5 lists them).
3. **Mixed meshes**: a Dart naming service with TS services and a Dart
   client; a TS naming service with Dart services; a proxy endpoint in one
   language before a backend in the other; a relay; slot migration with
   owners in both languages.
4. **Raw conformance scripts**: a scripted byte peer (TS, raw socket) sends
   frames and checks the reaction (`OPEN` on channel 1 → GOAWAY
   `PROTOCOL_ERROR`, a frame over the limit → `FRAME_TOO_LARGE`, DATA
   beyond the window, a second NONCE, ...). Each script also runs against
   the Dart server, which validates the expected reaction; then against the
   TS server. The prototype's hand-rolled client (`probe.mjs`) is the seed
   of this.

### 4.3 Dart tests against a TS peer

Two ways, in order of cost:

* The matrix above already runs the **Dart client against the TS server**:
  every scenario's client steps run in Dart with Dart's own checks on what
  it received.
* Later, a **Dart driver** (`test/interop/dart_driver/`, `dart test`) spawns
  the TS peer through the same protocol and runs Dart-written scenarios,
  porting the two-peer tests of the Dart suites listed in section 5 where
  the Dart side is in-process and fully observable. The existing Dart tests
  themselves cannot point at a TS process: they build both ends in memory
  (`MemoryTransport.pair`, the memory binding and the in-memory provider) and many
  use `fake_async`.

## 5. The Dart test suites

The full per-file catalogue (every test file and helper of both packages,
with lines, test counts, what each covers, how it builds its peers and its
interop relevance, plus the helpers, the stdout-protocol tools and the
extra vectors) is in
[interop-dart-suites.md](docs/notes/interop-dart-suites.md).
It was compiled by reading the files (counts are `test(` call sites from
grep, so loops and helpers run more). The essentials:

### 5.1 Size and shape

| Package | Test lines | `test(` sites | Runs (*measured*) | Platforms |
| --- | --- | --- | --- | --- |
| switchboard (core) | 51,896 | 1,468 (110 in `test/vectors`) | 1840 pass, 51 s | VM; `-P node` for vectors, bytes, identity, name, status, reconnecting client, item windows |
| switchboard_quic | 8,850 | 267 | 312 pass, 2 skipped, 1:56 with the cold shim build | VM; `-P node` for vectors and binding; `-P web` in Chrome for the web provider |

By how the tests build their peers:

* **Codec only** (no connection): `test/vectors/*`, `address`, `bytes`,
  `name`, `status`, `naming_protocol`, QUIC `vectors_test`. Directly
  reusable as data (section 6).
* **In-memory pairs** (`MemoryTransport.pair`, `RawPeer`, the memory binding,
  the in-memory QUIC provider): most of `mux_*`, `talk_*`, `identity_test`, the
  QUIC `binding_test` and its 27 review cases. These are the wire rules,
  and the scenarios worth porting run over real sockets instead.
* **Virtual time** (`fake_async`): heavy in `talk_test` (42 uses),
  `reconnecting_client_test` (38), `sharding_test` (27),
  `sharding_client_test`, `talk_window_test`, `talk_bulk_test` (12 each).
  These cannot cross a process boundary; their timeout scenarios are ported
  with short real timeouts.
* **Real loopback sockets**: `mux_test` "real transports", `mux_flow_test`
  over TCP, `transport_test`, `web_socket_server_test`,
  `integration/embedded_test` (raw bytes over TCP), `switchboard_test`, the
  integration meshes and sharding clusters (tcp/ws/mem), QUIC
  `native_integration_test` and `native_provider_test` (real QUIC and
  WebTransport through the shim), and the browser suites.
* **Internal** (Dart API, `localConnections`, `mem://`, PeerSet internals,
  slot gate state machine, the build hook): no interop value.

There is **no existing Dart TCP/WS interop server** for another process to
drive. The only stdout-protocol servers are in `switchboard_quic/tool/`:
`web_server.dart` (`READY {json}`, WebTransport listeners with per-session
behaviours chosen by query, a Switchboard node at `/sb` with `echo`,
`issue`, `control` services, event records fetched by a `report` session)
and `browser_server.dart` (`READY` then `RESULT {json}`). The Dart peer
of section 3 generalises that pattern.

### 5.2 Scenarios to port as two-peer interop tests

From the catalogue's list, in the order to build them. Each names its Dart
source; the expectations there are the reference.

| # | Scenario | Dart source | Transports |
| --- | --- | --- | --- |
| 1 | Byte-exact embedded round trip: preamble, OPEN, request, reply, CLOSE echoed; a device that sends no LIMITS, one byte per write | `integration/embedded_test.dart` | tcp (raw) |
| 2 | Window held from the first channel: a 256-byte window lets two 109-byte DATA frames through, the third waits for CREDIT | `integration/embedded_test.dart` | tcp (raw) |
| 3 | Mux over a real transport: PING both ways, 2 B and 100 kB subframes, a 1 MB bulk-priority channel from the acceptor, CANCELLED close, GOAWAY | `mux_test.dart` "real transports" | tcp, ws |
| 4 | Flow control: 200 subframes of varied size both ways, windows match when idle, a paused reader stalls only its channel, the connection backstop | `mux_flow_test.dart` | tcp, ws |
| 5 | GOAWAY: no opens after it, a racing OPEN gets GOING_AWAY, grace expiry, `peerGoAwayStatus` keeps the code | `mux_test.dart` | all |
| 6 | LIMITS byte exact as the first frame, `maxChannels` rejection with RESOURCE_EXHAUSTED, mid-connection LIMITS, frame over the limit | `mux_test.dart`, `mux_limits_test.dart` | tcp, ws |
| 7 | PING/PONG payloads up to 1024 bytes, keep-alive: a silent peer ends CONNECTION_LOST, live peers stay up | `mux_test.dart` | all |
| 8 | Status mapping: application codes in CLOSE read UNKNOWN, in GOAWAY GOING_AWAY; CONNECTION_LOST sent as UNAVAILABLE; RELOCATED byte for byte; reason truncation at the peer's frame limit | `mux_limits_test.dart`, `mux_talk_test.dart` | tcp, ws |
| 9 | Channel ids: parity, reserved ids skipped, long form above 0xFFFF and above 32 bits, `shortIdsOnly` | `mux_test.dart`, `vectors/mux_vectors_test.dart` | tcp (raw) |
| 10 | NONCE/IDENT: bearer, holder key with proof and intent, both directions at once, replacement, frames held until verified; over sockets: mutual identity, forged IDENT, foreign authority | `identity_test.dart`, `identity_node_test.dart` | all |
| 11 | Receiver binding and identity errors: named receiver, relay cannot rename, replay, echoed NONCE, the four protocol errors | `identity_test.dart` | tcp, ws |
| 12 | Channels held until IDENT, refused UNAUTHENTICATED after `identityTimeout` | `identity_node_test.dart`, `vectors/binding_protocol_test.dart` | all |
| 13 | Talk: items then final with payload, exact abort status, handler exception as ABORT INTERNAL, cancel, unknown response id, `maxIncomingRequests`, 200 interleaved requests, chained replies, id wrap, empty abort payload | `talk_test.dart`, `mux_talk_test.dart` | all |
| 14 | Item windows and credits: exactly N items, window 0, final/abort/extend at zero, bad credits close PROTOCOL_ERROR, an item beyond the window | `talk_window_test.dart` | all |
| 15 | EXTEND and declared deadlines (ported from virtual time to short real timeouts) | `talk_test.dart`, `talk_forward_test.dart` | all |
| 16 | Bulk payloads: known and unknown length uploads, bulk responses, items and plain messages, the inline/bulk boundary at `maxSubframeLength − 12` | `talk_bulk_test.dart` "over TCP" | all |
| 17 | Bulk rules: unknown or reused bulk number, no parent, wrong length, order kept while reassembling, parent close cancels | `talk_bulk_test.dart`, `switchboard_test.dart` | all |
| 18 | Node dispatch: exact instance, NOT_FOUND, malformed OPEN payload, default service, `denyReserved`, symmetric dispatch, GOAWAY on close, retry after GOAWAY | `switchboard_test.dart` | all |
| 19 | WebSocket handshake and framing: subprotocol, none offered, 400/426, text 1003, 1009 FRAME_TOO_LARGE, close echo, no compression | `web_socket_server_test.dart`, `transport_test.dart` | ws |
| 20 | Naming service: ids from 1, ALREADY_EXISTS, WATCH order (snapshot, reply, live), DOWN on close, LOOKUP, reconnect and resync | `naming_test.dart`, `integration/naming_mesh_test.dart` | tcp, ws |
| 21 | Sharding hand-over: DRAIN, ASSIGN, FORWARD, refusals and RESUME, lazy LOCATE, MOVED retry, never on RELOCATED, kv store under migration | `sharding_test.dart`, `sharding_client_test.dart`, `slot_routing_test.dart`, `integration/sharding/kv_store_test.dart` | tcp |
| 22 | Proxy and relay chains with mixed implementations (TS client, Dart proxy, TS backend, and the reverse), MOVED and abort fields byte for byte | `proxy_test.dart`, `relay_test.dart`, `talk_forward_test.dart` | tcp, ws |
| 23 | ReconnectingClient against a server's GOAWAY and close | `reconnecting_client_test.dart`, QUIC `web_binding_test.dart` | ws, webtransport |
| 24 | QUIC records, HELLO, close, rejection with STOP_SENDING, resets, GOAWAY, application close, WT_DRAIN_SESSION | QUIC `binding_test.dart`, `native_integration_test.dart`, `web_binding_test.dart` | quic, webtransport |
| 25 | QUIC identity (exporter-bound both ways, nonce-bound fallback, requirement rules) and scale (8 MiB bulk each way, 64 parallel channels three times, `whenWritable`, a frame mux backend behind a QUIC front) | QUIC `binding_test.dart`, `native_integration_test.dart`, `switchboard_test.dart` | quic, webtransport |

Scenarios 1, 2 and 9 need a raw byte peer on one side (layer 4 of
section 4.2); 4 needs window introspection on the TS side (a debug
accessor on the channel, which the core thread would have to expose); 20
to 22 are the mesh layer.


## 6. Test vectors

### 6.1 Extracted from the wiki

`extract-wiki-vectors.mjs` parses the two wiki pages and writes
[wiki-vectors.json](docs/interop-prototype/js/wiki-vectors.json)
(source commit recorded inside): **185 table vectors** and **9 named blobs**.
Each vector has an id (`test-vectors/talk-messages/request-id-0`), page,
section, case text, hex, `negative`, `composite` and the raw cell.

| Page, section | Vectors | Negative | Composite |
| --- | --- | --- | --- |
| Test vectors: Transport, stream binding | 2 | 0 | 0 |
| Mux frames | 23 | 6 | 3 |
| Talk messages | 37 | 17 | 0 |
| Address headers (incl. `_relay`, `_bulk`) | 12 | 3 | 0 |
| Naming service payloads (incl. identity procedures) | 15 | 0 | 3 |
| Sharding payloads (incl. MOVED/RELOCATED) | 39 | 7 | 0 |
| Credentials (modifications of C1) | 8 | 8 | 8 |
| Names | 5 | 0 | 0 |
| QUIC binding: Records | 18 | 7 | 1 |
| QUIC binding: Control stream | 8 | 0 | 2 |
| QUIC binding: Bulk channels | 2 | 0 | 0 |
| QUIC binding: Status codes (with wire columns) | 12 | 0 | 0 |
| QUIC binding: Exporter labels and contexts | 4 | 0 | 0 |

Blobs: `C1` (86 bytes), `C2` (136), `C2.authority-seed`, `C2.authority-key`,
`C2.holder-seed`, `C2.holder-key`, `P` (64), `quic.X` (the 118-byte signed
message) and `quic.X.signature` (64).

*Composite* rows mix hex with prose ("`02 00 00 06 56 00`, the 86 bytes of
C1, `00 00`", "byte 53 = `03`"); the tests assemble those by hand from the
blobs. A few vectors live in prose, not tables, and are transcribed by hand:
compatible reserved bits (`42 02 00 01`), the LIMITS and CREDIT payload
negatives, the seven IDENT payload negatives, the naming negatives (CONNECT
with a 65-byte intent, no scheme, non-UTF-8 requester, short response), the
record or REGISTER without metadata, SLOTSPC unknown flag bits, the ABORTED
payload rule, the QUIC `FF FF FF 3F` and `FF FF FF 7F` headers, the QUIC
control-stream negatives, and the scope pattern table (name matching, no
bytes).

Measured: the Dart oracle accepts every positive and rejects every negative
of the 69 non-composite Talk, mux and address vectors, and re-encodes each
positive to the same bytes (`probe.mjs`).

### 6.2 Where they go (proposal)

* `test/vectors/wiki-vectors.json` in switchboard_ts, generated, never
  edited by hand, with the sway commit inside; `tools/extract-wiki-vectors.mjs`
  regenerates it from a local sway checkout. CI cannot read sway (private),
  so CI only checks that the TS tests cover every vector id (a test that
  lists the ids it asserts, and fails on an id it does not know or a vector
  nobody asserts).
* The core and QUIC threads read the JSON in their codec unit tests; the
  interop layer runs the same ids through the Dart oracle.

### 6.3 Vectors beyond the wiki

Every wiki table vector is asserted by the Dart suites. The Dart suites
also carry many more byte cases derived from the prose of the transport,
mux, Talk and embedded profile pages; the catalogue lists them with their
hex (section A of
[interop-dart-suites.md](docs/notes/interop-dart-suites.md#a-test-vector-groups-compared-with-the-wiki)).
The largest groups:

* transport: preamble flags ignored, wrong magic, version 2 is
  UNSUPPORTED, length 0, FRAME_TOO_LARGE before the body, frames before an
  error delivered first;
* mux: 13 more decoder negatives (reserved flags 0x04 and 0x08, CLOSE on
  channel 0, OPEN on channel 1, the three reserved ids, empty frame,
  oversize control payload, short LIMITS, 11-byte CREDIT), full-range u32
  LIMITS and CREDIT, ids above 32 bits, more compatible reserved bits;
* Talk: 26 more negatives and about 15 more positives (EXTEND rounding and
  clamping, chained stream requests, item windows on the wire, largest
  bulk reference);
* addressing: `_bulk` truncated and bulk number 0;
* sharding: every strict prefix of every vector is an error, trailing
  bytes ignored, unknown flags, MOVED/RELOCATED at their largest values;
* key-to-slot: FNV-1a 32 and `slotForText`, which the wiki has only in
  prose (`slot_key_vectors_test.dart`);
* QUIC: records in 2-byte chunks, FIN inside a record, the state-dependent
  negatives, the HELLO above 0x3FFFFFFF, the "Example" section's records.

The core thread has already transcribed these, with the hex written out
and each case cited by Dart file, group and test: Appendix A of
[core.md](docs/notes/core.md#appendix-a-dart-test-vectors-not-on-the-wiki-page).
Proposal: turn that appendix into `test/vectors/dart-extra-vectors.json`
(same shape as the wiki file, each entry citing its Dart test), and check
every entry against the Dart oracle in CI, so that a transcription error
cannot survive; the entries it marks *derived* are checked the same way.

Two stale documents found on the way: `test-vectors.md` still says "The
Dart reference does not implement the binding yet, so its suite does not
assert them", but `switchboard_quic/test/vectors_test.dart` asserts the
QUIC tables and core `exporter_identity_vectors_test.dart` asserts X; and
`switchboard/doc/sharding-vectors.md` is an old staging copy of the
sharding section (missing the RELOCATED row and the ABORTED note): use
the wiki.


## 7. CI (proposal)

One job, `interop`, on `ubuntu-latest`, next to the lead's unit test jobs:

```yaml
- uses: actions/checkout@v5
  with: { path: switchboard_ts }
- name: Dart commits
  id: dart
  run: |   # read test/interop/dart_peer/dart_commits.txt
    ...
- uses: actions/checkout@v5
  with: { repository: kaetemi/switchboard, ref: ${{ steps.dart.outputs.core }}, path: switchboard }
- uses: actions/checkout@v5
  with: { repository: kaetemi/switchboard_quic, ref: ${{ steps.dart.outputs.quic }}, path: switchboard_quic }
- uses: dtolnay/rust-toolchain@stable
  with: { toolchain: 1.99.0 }            # the QUIC repo's pin
- uses: Swatinem/rust-cache@v2
  with: { workspaces: switchboard_quic/native/sbq -> target }
- uses: dart-lang/setup-dart@v1
  with: { sdk: 3.13.5 }
- uses: actions/cache@v4                  # the built peer
  with:
    path: switchboard_ts/test/interop/dart_peer/build
    key: dart-peer-${{ hashFiles('switchboard_ts/test/interop/dart_peer/**') }}-${{ steps.dart.outputs.core }}-${{ steps.dart.outputs.quic }}
- uses: actions/setup-node@v4
  with: { node-version: 24 }              # or the lead's matrix
- run: npm ci && node scripts/dart-peer.mjs build && npm run test:interop
  working-directory: switchboard_ts
  env: { SWITCHBOARD_INTEROP: required }
- run: npx playwright install --with-deps chromium && npm run test:interop:browser
```

* `SWITCHBOARD_INTEROP=required` turns a missing peer into a failure; a
  local run without Dart skips the interop files with one visible line.
* Cold: about 2 minutes of cargo plus 7 s of `dart build cli`; warm: cache
  restores. The core suite of the Dart repos does not need to run here; the
  Dart↔Dart cells of the matrix check the peer itself.
* A check that `dart_commits.txt`'s core commit equals the QUIC repo's
  `tool/core_commit.txt` at the pinned QUIC commit keeps the pair coherent.
* Moving the pins forward is a deliberate commit, so a protocol change in
  the Dart repos shows up as one failing interop run.

## 8. Ownership (proposal, pending the lead's split)

This thread owns `test/interop/**` (the Dart peer, the TS peer, the
launcher, the scenario files), `test/vectors/wiki-vectors.json` and its
extractor, and the `interop` CI job. It does not touch `src/**` or the
unit tests of the core and QUIC threads; the TS peer uses only the port's
public API, so a missing feature there shows up as a skipped matrix cell
with a reason, not as an edit in someone else's files.

## 9. Questions for the lead

1. Test runner (vitest or `node:test`): decides the launcher's shape
   (global setup, per-file fixtures) and how the matrix is expressed.
2. The QUIC stack on Node (and whether Deno or Bun are targets): decides
   which QUIC cells of the matrix run.
3. Paths: `test/interop/` and `test/vectors/` as above, or elsewhere.
4. The order of work: the codec differential layer can start as soon as
   the codecs exist; scenarios need the node API; meshes come last.

## 10. Phase 2 status (deliverable 1, branch `claude/switchboard-ts-interop-x2iz7c`)

Landed on the branch (draft PR against `claude/switchboard-ts-design-s5r9na`):

* `test/interop/dart_peer/` (the Dart peer, pinned by `dart_commits.txt`),
  `tool/dart-peer.mjs` (build, locate, status, fetch; stamp in
  `build/peer/stamp.json`), `test/interop/{protocol,launcher,peers,matrix,global-setup}.ts`.
  `test/interop/README.md` is the peer protocol's specification; the TS
  peer (deliverable 2) implements the same commands, events and services.
* `test/vectors/wiki-vectors.json` (214 vectors, wiki `e8c2206`),
  `tool/extract-wiki-vectors.mjs`, `test/support/vectors.ts`,
  `test/vectors/coverage.test.ts`.
* `test/interop/oracle.test.ts` (118 tests; 155 once the mux and control
  codecs joined in PR #12), `test/interop/probe/` (34),
  `test/interop/scenarios/basics.test.ts` (13 per cell). Interop project:
  204 passed, 190 skipped, every skipped suite naming why.
* The `interop` job in `ci.yml` (existing jobs untouched).
* One lead-owned line, in its own commit: `globalSetup` on the interop
  project in `vitest.config.ts`, so the peer is built once per run and
  handed to the files with `inject('dartPeer')`.

For the other threads:

* Name wiki vectors through `test/support/vectors.ts` with string literals
  (`wikiBytes('test-vectors/names/api')`, `wikiVectorsIn(prefix)`,
  `skippedWikiVector(id, reason)`); the coverage test finds them by a scan.
  The vector tests under `test/vectors/` (names, status, address, talk,
  naming, sharding, transport, slot key, QUIC records) read hex inline and
  carry ids only in test names, which the scan does not count, so their
  tables show as `todo` until they call the helper. Once one vector of a wiki table is named, every
  vector of that table must be named or skipped; `SWITCHBOARD_VECTORS_STRICT=1`
  checks every table.
* Clearing a matrix cell: remove the transport's entry from
  `tsTransportGaps` in `test/interop/matrix.ts` when the TS peer can serve
  it (thread 6 does this as each port lands).
* Measured behaviour of the reference worth knowing: after receiving
  GOAWAY the Dart peer closes the connection once no channel is left,
  without sending GOAWAY back (wiki mux.md, "SHOULD close"); an OPEN after
  its own GOAWAY is refused with GOING_AWAY; window cost is payload + 16.

## 11. Phase 3 status (the TS peer and the matrix, branch `claude/ts-peer`)

Landed, every cell green in required mode (`SWITCHBOARD_INTEROP=required`,
`SWITCHBOARD_NATIVE=required`): 3686 interop tests, none skipped, in about
4 minutes here.

* **The TS peer** (`test/interop/ts_peer/`, public Node entry only): every
  command, event and service of the README, run as `node .../main.ts`
  (type stripping, Node 22.18 or later), READY with `certificateSha256`
  from `selfSignedCertificate`. `tsTransportGaps` is empty when the native
  addon loads; without it the TS cells over raw QUIC and WebTransport skip
  with the reason.
* **Protocol additions, in both peers**: the echo procedures `BIGSTRM`,
  `BIGITEMS`, `HOW`, `EXTEND`, `STATUS`; the `forward` behaviour and
  `anyInstance`; `bulkPattern` sources; `talk.extend`; status bodies in
  errors and events and `statusHex`; `identity` (shared test authorities,
  `connection.identify`, `connection.identified`); `naming`, `proxy`,
  `relay` and the `naming.*` commands; a QUIC binding on the listening
  node so that intermediaries dial QUIC.
* **Scenarios**: basics, talk, talk-wire, bulk, close and GOAWAY, mux,
  protocol-errors, forward, identity, naming (with a slot hand-over:
  DRAIN, ASSIGN, FORWARD in protocol order) in the 16 pairing cells;
  proxy, relay, forward and naming chains of three peers (`chain.ts`) in
  8 implementation cells per transport; the probe against both servers.
* **Oracle**: the TS peer's oracle answers every kind of the Dart peer's;
  the Dart oracle gained the naming, sharding, MOVED, credential, slot key,
  QUIC record, HELLO, QUIC code and exporter kinds. No vector table is
  `todo` any more.
* **Dart driver** (`test/interop/dart_driver/`, `node tool/dart-peer.mjs
  driver`): 35 Dart-written tests against the TS peer.
* **CI**: the interop job builds the native addon too and runs the TS
  cells and the driver.

Differences found, none a failing cell:

* The TS WebSocket listener refuses a message over the limit through the
  `ws` package, which closes with 1009 before the mux can send GOAWAY
  FRAME_TOO_LARGE; the Dart listener sends the GOAWAY, then 1009. Already
  documented in `acceptWebSocket`; the probe asserts each server's reaction.
* Endpoints in naming payloads: Dart parses them as `Uri` and sends the
  normalised text (scheme and host lower-cased, characters escaped), TS
  carries them as written and refuses what a URI cannot hold. Documented in
  `endpointProblem`; the oracle checks that what Dart sends instead decodes
  alike in TS, which held for every input found (three seeds × 10 000).
* A fresh TS server sends its LIMITS a little later after accepting than
  the Dart one (19 ms against 5 ms for the first connection of a new
  process, measured once), so a Dart client opening channels at once can
  run past the TS server's channel limit before it knows it; the server then
  refuses the extra channel with RESOURCE_EXHAUSTED, a valid outcome. The
  mux scenario waits for a PONG first.
* Names: TS `webTransportExporterLabel` is Dart's `webTransportIdentLabel`
  (`switchboard-ident`), and TS `webTransportTlsExporterLabel` is Dart's
  `webTransportExporterLabel` (`EXPORTER-WebTransport`).
* Reference behaviour worth knowing, pinned by the scenarios: an
  application status (256 and above) in a mux CLOSE or GOAWAY is refused
  before sending (`ArgumentError`, `RangeError` in TS); a cancel is
  answered `ABORT CANCELLED "cancelled by requester"`; over QUIC a bearer
  credential goes only to an authenticated end, so a bearer IDENT to a
  listener with a self-signed certificate fails with UNAUTHENTICATED; a
  connection closed over QUIC reads as GOING_AWAY at the far side
  (CONNECTION_LOST over streams); instance ids are global across types, so
  a service registered with a naming service rarely keeps instance 1.
