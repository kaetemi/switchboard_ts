# Interop tests against the Dart reference

The Dart implementation (`kaetemi/switchboard` and `kaetemi/switchboard_quic`) runs as a separate process, a **peer**, that the tests drive over a JSON-lines protocol on stdin and stdout. The TS peer speaks the same protocol over this package's public API, so that one scenario runs in four pairings: Dart to Dart (which checks the scenario against the reference: a failure there is a broken test, not a TS bug), TS to Dart, Dart to TS and TS to TS, each over TCP, WebSocket, raw QUIC and WebTransport. Chains of three peers (a proxy, a relay, a forwarding service, a naming service between a client and a backend) run in every assignment of the two implementations to the roles. The Dart driver runs Dart-written scenarios against the TS peer, and the codec oracle compares the two implementations' codecs.

```
test/interop/
  dart_peer/          the Dart peer: a package depending on the Dart repositories by path,
                      built with `dart build cli` to an AOT executable; dart_commits.txt pins them
  ts_peer/            the TS peer over the public Node entry (src/node/index.ts only),
                      run as `node ts_peer/main.ts`
  dart_driver/        Dart-written scenarios (`dart test`) against the TS peer it spawns
  protocol.ts         the protocol's messages as types
  launcher.ts         spawns a peer, waits for READY, speaks JSON lines, logs events
  peers.ts            which peers this run has (the Dart peer from the global setup, the TS peer)
  global-setup.ts     builds or locates the Dart peer once per run
  matrix.ts           pairings × transports, skip reasons, describeCells()
  chain.ts            chains of three or more peers, every role in either language, describeChains()
  scenarios/          the scenarios: basics, talk, talk-wire, bulk, close and GOAWAY, mux limits,
                      protocol errors, forward, identity, naming and sharding, mesh (proxy, relay,
                      forwarding and naming chains)
  oracle.test.ts      the wiki vectors against the Dart codecs; the TS codecs against Dart, fuzzed
  probe/              raw byte conformance: hand-built frames against each server's listeners
```

## Running

```sh
npm run test:interop                    # builds the Dart peer if needed, then runs the interop project
node tool/dart-peer.mjs status          # what the build would use
node tool/dart-peer.mjs build           # build only
node tool/dart-peer.mjs driver          # the Dart driver's scenarios against the TS peer
```

The TS peer runs on the Node that runs the tests (22.18 or later: type stripping runs `ts_peer/main.ts` as it is). Its raw QUIC and WebTransport listeners and dialers need the native QUIC addon (`npm run build:native`, or `SWITCHBOARD_QUIC_ADDON=<path>`); without it the TS cells over those two transports skip with the reason, and `SWITCHBOARD_NATIVE=required` (CI) fails instead.

The Dart peer needs a Dart SDK (3.13 or later; CI uses 3.13.5), a Rust toolchain (switchboard_quic's build hook compiles its QUIC library with cargo, a minute or two the first time), and checkouts of the two Dart repositories. They are found as follows:

| What | Where |
| --- | --- |
| Dart SDK | `dart` on PATH, `SWITCHBOARD_DART_SDK`, `DART_SDK`, or the one `node tool/dart-peer.mjs build --download-sdk` downloads into the user cache |
| Dart repositories | sibling checkouts (`../switchboard`, `../switchboard_quic`), `SWITCHBOARD_DART_CORE` and `SWITCHBOARD_DART_QUIC`, or the pinned clones `node tool/dart-peer.mjs fetch` makes in `dart_peer/.deps/` |
| A prebuilt peer | `SWITCHBOARD_DART_PEER=<executable>` skips all of the above |

The build writes a stamp next to the executable and is skipped while the peer's sources, the SDK version and the checkouts (commit and uncommitted changes) match it. A checkout at another commit than `dart_commits.txt` gives a warning locally and fails CI (`--expect-pins`).

Without a peer the interop files skip, and the run prints one line saying why. Environment:

| Variable | Effect |
| --- | --- |
| `SWITCHBOARD_INTEROP=required` | a missing Dart peer fails the run (CI) |
| `SWITCHBOARD_INTEROP=skip` | skip the Dart cells without looking for a peer |
| `SWITCHBOARD_INTEROP_BUILD=0` | use a peer that is already built, never build |
| `SWITCHBOARD_FUZZ_SEED`, `SWITCHBOARD_FUZZ_COUNT` | the oracle's random inputs (the seed is in the test names) |
| `SWITCHBOARD_QUIC_ADDON=<path>` | the native QUIC addon the TS peer and the matrix load |
| `SWITCHBOARD_NATIVE=required` | a missing native QUIC addon fails the run instead of skipping the TS QUIC cells (CI) |

## Writing a scenario

```ts
import { expect, test } from 'vitest';
import { describeCells } from '../matrix.ts';

describeCells('my scenario', { transports: ['tcp', 'ws'] }, (peers) => {
  test('ECHO', async () => {
    const { talk } = await peers.client.rpc<{ talk: string }>('talk.open', {
      endpoint: peers.endpoint,
      address: { type: 'echo' },
    });
    const reply = await peers.client.rpc('talk.request', { talk, procedure: 'ECHO', text: 'hi' });
    expect(reply).toMatchObject({ payload: '68 69' });
  });
});
```

Each cell starts its server and client peers once, resets both after every test (and fails the test if either reported a `peer.error`), and prints both peers' stderr when a test fails. A cell that cannot run is a skipped suite whose name says why: a peer is missing, or the TS side lacks the transport (`tsTransportGaps` in `matrix.ts`: raw QUIC and WebTransport without the native addon). `describeChains` (`chain.ts`) does the same for three or more peers, each role in either language, started from the backend to the client and each configured from the peers started before it.

A value the reference decides is found by running the Dart to Dart cell and asserting what it does in every cell, never by recording a snapshot per cell (each cell would record its own).

Wait for events rather than sleeping: `peers.server.events.waitFor({ event: 'channel.closed', channel })`. Timeouts cannot be faked across a process boundary, so a scenario that exercises one configures it short (100 to 500 ms) on the side that enforces it.

## Process protocol

Version 1. The peer is started as `<peer> [--config <json>] [--log <level>] [--watchdog <seconds>]`.

- **stdout** carries exactly one `READY {json}` line, then JSON lines only: responses and events. A launcher looks for `READY ` anywhere in a line (`dart run` prints its build progress before it without a newline). **stderr** carries logs; `--log FINE` (a `package:logging` level name) turns on the library's logging.
- **READY** names the endpoints and what the peer can do:

  ```json
  {"protocol": 1, "implementation": "dart", "tcp": "tcp://127.0.0.1:41234",
   "ws": "ws://127.0.0.1:41235/sb", "quic": "quic://127.0.0.1:41236",
   "webtransport": "https://127.0.0.1:41237/sb", "certificateSha256": "<64 hex digits>",
   "pid": 1234, "oracle": ["name", "status", "address", "mux", "control", "talk", "stream", ...]}
  ```

  Only the transports the peer listens on appear; the QUIC listeners share one self-signed ECDSA P-256 certificate whose SHA-256 is `certificateSha256` (for WebTransport's `serverCertificateHashes`).
- **Commands**, one JSON object per line on stdin: `{"id": 7, "op": "talk.request", ...}`. Each gets exactly one response line, `{"id": 7, "ok": true, "value": ...}` or `{"id": 7, "ok": false, "error": {...}}`. Commands run concurrently, so responses can come out of order.
- **Errors** are `{"kind", "type", "message"}` plus `code`, `reason` and `body` (the status payload's bytes after the code, hex, as received) for kind `status`: `status` (a Switchboard status: an abort, a refused channel, a protocol error), `format` (malformed input to a codec), `argument` (a bad command), `other`. `type` is the implementation's error class and differs between implementations; tests compare `kind` and `code`.
- **Events** are unsolicited lines `{"event": "channel.closed", "channel": "ch3", "code": 5, "reason": ""}`.
- **Handles**: every object a command or a service creates gets a string handle that is never reused within the process: `c` connections, `ch` channels, `t` Talk channels, `s` streams.
- **Lifetime**: the peer exits on `shutdown`, at the end of stdin (so a test runner that dies leaves no orphan), on SIGTERM or SIGINT, and after the watchdog (default 600 s, exit code 3).

Byte strings are hex, upper case, space separated (`"01 02 FF"`; spaces are optional on input). JSON numbers carry 48-bit ids and `u32` windows exactly.

### Configuration

`--config` takes a JSON object; absent keys keep the defaults.

| Key | Default | Meaning |
| --- | --- | --- |
| `listen` | all four | any of `tcp`, `ws`, `quic`, `webtransport`, on 127.0.0.1 port 0 |
| `path` | `/sb` | the WebSocket and WebTransport path |
| `mux` | Dart defaults | `maxFrameSize`, `maxChannels`, `shortIdsOnly`, `keepAliveIntervalMs` (0 disables), `keepAliveTimeoutMs`, `goAwayGraceMs`, `announceLimits`, `closeConfirmTimeoutMs`, `identityTimeoutMs`, `requireNamedIdent`, `initialWindow`, `awaitPeerLimits` |
| `talk` | Dart defaults | `requestTimeoutMs`, `replyTimeoutMs`, `bulkThreshold`, `maxInlinePayload`, `reassemblyBudget`, `bulkIdleTimeoutMs`, `creditTimeoutMs` |
| `services` | `echo`, `raw`, `sink` at instance 1 | `[{"type": "echo", "behaviour": "echo", "instance": 1}]`; `forward` takes a `target` |
| `identity` | none | what the peer presents and verifies (below) |
| `naming` | none | `{assignmentHoldMs}` (default 0): serve a naming service as `_ns` instance 1, listed in its own table at the peer's endpoints |
| `proxy` | none | `{endpoint, types, revealOwners}`: `proxyHandler` for each of `types` (default `echo`), forwarding to their instance 1 at `endpoint` |
| `relay` | none | `{endpoint, types}`: a `RelayService` at `_relay` (`allowEndpoints`, no identity required) relaying `types` (default `echo`) to their instance 1 at `endpoint` |

The options apply to the listeners' connections and to the connections the client commands make.

### Mesh

The listening node and the client node both dial `quic://` and `https://` endpoints, so a proxy, a relay, a forwarding service or a naming client reaches a peer over any transport. A service spec with `anyInstance: true` also serves the instances of its type no other registration serves (an id the naming service assigned). The naming commands open naming clients on nodes of their own, whose resolver is a `NamingResolver` over the client:

| Op | Arguments | Value |
| --- | --- | --- |
| `naming.open` | `endpoint`, `watch` (default true) | `{naming}`: a started naming client of the naming service at `endpoint`; its table's changes are `naming.event {naming, up, type, instance, endpoints, metadata}`, and the slot requests it gets (all accepted at once, `ASSIGN` answered holding) `naming.slot {naming, request, type, slot, epoch, to}` |
| `naming.synced` | `naming` | `null` once the table first synced |
| `naming.register` | `naming`, `type`, `instance`, `endpoints` (default the peer's own), `metadata` | `{instance}` |
| `naming.unregister` | `naming`, `type`, `instance` | `null` |
| `naming.defineSlots` | `naming`, `type`, `count`, `mode` (`managed`, `static`), `lazy`, `shared`, `capacity` | `null` |
| `naming.claim` | `naming`, `type`, `slot`, `holding` | `{epoch}` |
| `naming.release` | `naming`, `type`, `slot`, `keepStorage` | `null` |
| `naming.locate` | `naming`, `type`, `slot` | `{state, owner, epoch}`, the state by name |
| `naming.migrate` | `naming`, `type`, `slot`, `to` (0: the allocator chooses) | `{phases}`: the hand-over's phases by name, once it is done |
| `naming.channel` | `naming`, `address` (`type`, `instance`, `shard`, `payload`) | `{talk, channel, connection}`: a Talk channel opened through the resolver, as `talk.open` reports it |
| `naming.close` | `naming` | `null` |

`chain.ts` runs scenarios of three or more peers (`describeChains`): every role played by either implementation, eight cells per transport for three roles, started from the backend to the client.

### Identity

Both peers derive the same test authorities, so a credential one issues verifies at the other: an Ed25519 authority with key id `auth1` and seed bytes `3 × i`, an HMAC key `mesh1` of bytes `0 … 31`, and a rogue Ed25519 authority claiming `auth1` with seed bytes `5 × i + 1` that nobody trusts. A holder key's seed is the SHA-256 of `holder:<identity>`. The `identity` object:

| Key | Default | Meaning |
| --- | --- | --- |
| `verify` | false | verify IDENTs with `auth1` and `mesh1` (the node's `verifier`) |
| `credential` | none | `{identity, authority, holder, kind, scopes, forge}`: issued at start by `authority` (`ed25519`, `hmac` or `rogue`; default `ed25519`), naming a holder key unless `holder` is false (always false for `hmac`), of `kind` (`node`, `client`, `device`; default `node`), with `scopes` (`[{right, pattern}]`, rights by name; default open, register, watch and claim on `*`); `forge` flips the signature's last byte. Both the listening node and the client node present it |
| `identifyOutgoing` | true | identify on every connection the client dials |
| `identifyInReturn` | false | identify in return on an accepted connection whose peer sent NONCE first (the QUIC listener's `identifyInReturn`) |
| `identityTimeoutMs` | 10000 | how long a channel waits for its connection's IDENT |
| `policy` | none | the listeners' policy: `scoped`, `requireIdentity` or `denyReserved` |
| `quicRules` | `none` | the QUIC listeners' identification rules: `none` or `listener` (`QuicIdentityRules.listener`: exporter-bound IDENTs only) |

`connect` takes `intent` (hex) and `receiver`, and `connection.identify {connection, intent, receiver}` identifies again on a live connection (the server's own, for a connection a service reported). Every valid IDENT of a connection, the first and those replacing it, is reported as `connection.identified {connection, identity, kind, provedPossession, intent, receiver}`; the listening side reports a connection once its peer identified. `channel.incoming` carries the peer's `identity` (null without one), and `WHOAMI` answers it.

### Services

Behaviours both peers implement identically, so a scenario can run against either.

**`echo`**: the interop procedures over Talk. Payloads are UTF-8 text where a number or a word is expected; `code reason` means a decimal status code, a space, and the rest as the reason.

| Procedure | Request payload | Behaviour |
| --- | --- | --- |
| `ECHO` | anything | final response with the request's payload |
| `UPPER` | text | final response with ASCII `a`–`z` upper-cased, other bytes as they are |
| `SIZE` | anything | final response `"<length> <sha256 lower-case hex>"` of the request payload |
| `BIG` | `n` | final response of n bytes, byte i = `(i * 31) & 0xFF` |
| `BIGSTRM` | `n` | final response with the `BIG` pattern of n bytes as a bulk payload of undeclared length, streamed in 1000-byte pieces |
| `COUNT` | `n` | n stream items `"1"` … `"n"`, then the final response `"done"` |
| `BIGITEMS` | `n size` | n stream items of the `BIG` pattern of `size` bytes, then the final response `"done"` |
| `HOW` | anything | final response `"inline <length>"` or `"bulk <length>"`: how the request's payload arrived |
| `EXTEND` | `ms` | `EXTEND` declaring a deadline of 2 × ms (the wire value adds the responder's buffer, 5000 ms by default), then the final response `"extended"` after ms milliseconds |
| `STATUS` | a status payload | abort response with that status, decoded as is: its bytes after the code pass byte for byte (`MOVED`, `RELOCATED`) |
| `ABORT` | `code reason` | abort response with that status |
| `SLOW` | `ms` | final response `"slow"` after ms milliseconds |
| `STALL` | | never answers |
| `NOTIFY` | anything | a plain message `NOTE` with the payload on the channel, then the final response `"sent"` |
| `CLOSE` | `code reason` | final response `"closing"`, then closes the channel with that status |
| `GOAWAY` | | final response `"going"`, then GOAWAY `GOING_AWAY` on the connection |
| `WHOAMI` | | final response with the peer's verified identity, empty without one |
| anything else | | abort response `UNIMPLEMENTED`, no reason |

A plain message (no reply expected) to `echo` is reported as `service.message {service, procedure, bulk, payload}`.

**`raw`**: each incoming channel becomes a handle: `channel.incoming {channel, connection, service, id, type, instance, shard, host, payload, identity}`, its subframes `channel.data {channel, hex}`, its end `channel.closed {channel, code, reason}`. `channel.send` and `channel.close` drive it.

**`sink`**: accepts the channel and never reads it, so its window fills up.

**`forward`**: a message chain intermediary. Every message of the channel is forwarded (`forwardMessage`) on a Talk channel of its own to the service the spec's `target` names, `{"endpoint", "type", "instance"}` (the endpoint defaults to the peer's first listener, the type to `echo`), dialled through the peer's client node on a pooled connection; every message from there comes back the same way, and a close on either side closes the other with its status. `{"type": "fwd", "behaviour": "forward", "target": {"type": "echo", "instance": 1}}`.

### Commands

| Op | Arguments | Value |
| --- | --- | --- |
| `ping` | | `"pong"` |
| `info` | | the READY object and `handles`, the number of live handles |
| `reset` | | closes every handle's object (newest first) and forgets them: `{closed}`; the listeners stay |
| `shutdown` | | `null`, then the peer exits |
| `decode` | `kind`, `hex` | the reference decoder's fields and `reencoded` (see the oracle) |
| `decodeMany` | `kind`, `hexes` | `[{ok: true, value} \| {ok: false, error}]`, one per input |
| `encode` | `kind`, `fields` | hex |
| `connect` | `endpoint`, `intent`, `receiver` | `{connection}`: a new connection, not pooled, identified with `intent` and naming `receiver` when given |
| `open` | `connection`, `address` or `openHex` | `{channel, id}`: a raw channel, its data and end reported as events |
| `talk.open` | `connection` or `endpoint`, `address` or `openHex` | `{talk, channel, connection}`; messages the far side sends on it are `talk.message {talk, requestId, procedure, bulk, payload}` |
| `talk.request` | `talk`, `procedure`, payload, `timeoutMs`, `digest` | the final response: `{procedure, bulk, payload}`, or with `digest` `{procedure, bulk, size, sha256}`; a status error for an abort. Each `EXTEND` of the responder is reported as `talk.extend {talk, requestId, deadlineMs, renewMs}` (null for an absent field) |
| `talk.stream` | `talk`, `procedure`, payload, `window`, `timeoutMs`, `digest` | `{stream, requestId}`; then `talk.item {stream, index, ...message}` per item and `talk.done {stream, items, ...message}` or `talk.done {stream, items, error}`; `talk.extend {stream, deadlineMs, renewMs}` |
| `talk.send` | `talk`, `procedure`, payload | `null`: a plain message |
| `talk.close` | `talk`, `code`, `reason` | `null` |
| `stream.grant` | `stream`, `items` | `null`: item credit |
| `stream.cancel` | `stream`, `code`, `reason` | `null` |
| `channel.send` | `channel`, `hex` | `null`: one subframe |
| `channel.close` | `channel`, `code`, `reason` or `statusHex` | `null`; `statusHex` is a status payload sent as is (`MOVED` with its fields) |
| `connection.ping` | `connection`, `payload` | `{rttMs}` |
| `connection.goAway` | `connection`, `code` (default 33), `reason` | `null` |
| `connection.close` | `connection` | `null` |
| `connection.identify` | `connection`, `intent`, `receiver` | `null`: identifies with the peer's credential |
| `request` | `endpoint`, `service`, `instance`, `procedure`, payload, `timeoutMs`, `digest` | one request on a connection of its own, closed afterwards |

An `address` is `{type, typeHex, instance, shard, host, payload}`, every field optional (`type` is text, `typeHex` the 8 name bytes, `payload` hex). A payload argument is one of `payload` (hex), `text` (UTF-8) or `patternLength` (the `BIG` pattern of that many bytes); `talk.request`, `talk.stream` and `talk.send` also take `bulkPattern: {length, chunk, declared}`, an empty payload with the `BIG` pattern of `length` bytes as a bulk source in pieces of `chunk` bytes (default 1000), its length declared unless `declared` is false. Endpoints are any the READY line names: `tcp://`, `ws://`, `quic://`, `https://` (WebTransport). The client side does not verify the QUIC certificate.

Connections report `connection.goAway {connection, code, reason, body}` when the far side sends GOAWAY and `connection.closed {connection, code, reason, body}` when they end; channels report `channel.closed {channel, code, reason, body}` (`body`: the status payload's bytes after the code, hex). An error no command answers (in a service, say) is `peer.error {error}`.

### Oracle kinds

| Kind | Decoded fields |
| --- | --- |
| `name` | `text`, `length` |
| `status` | `code`, `reason`, `body` (the bytes after the code, kept as received) |
| `address` | `type` (the 8 name bytes as hex, or null), `instance`, `shard`, `host`, `payload` |
| `mux` | `command` (`data`, `open`, `close`), `channelId`, `longForm`, `payload` |
| `control` | `type`, `typeName`, `payload`, `fields` (LIMITS, CREDIT, GOAWAY, IDENT decoded; `{error}` when the typed payload does not decode) |
| `talk` | `kind`, `procedure` (hex or null), `requestId`, `responseId`, `stream`, `window`, `bulk`, `payload` |
| `stream` | `preamble`, `frames` (hex each), `buffered`: a byte stream of the stream binding |
| `record`, `register`, `registerResponse`, `unregister`, `watch`, `up`, `down`, `connect`, `connectResponse`, `dialBack` | the naming payload's fields by their Dart names: names as the 8 bytes in hex, endpoints as text, bytes as hex (`up` and `down`: `{up, record}`) |
| `slotSpace`, `slots`, `holding`, `holdingResponse`, `claim`, `claimResponse`, `release`, `locate`, `locateResponse`, `migrate`, `phase`, `assign`, `assignResponse`, `drain`, `forward`, `resume`, `slot` | the sharding payload's fields by their Dart names, enums as their codes |
| `moved` | `owner`, `epoch`, `reason` of a `MOVED` or `RELOCATED` status payload (code included); re-encoded with the code it had |
| `credential` | `version`, `kind`, `keyId`, `identity`, `issuedAt`, `expiresAt`, `holderKey` (or null), `scopes` (`[{right, pattern}]`), `signatureKind`, `signature`, `signed` (the signed bytes) |
| `slotKey` | `fnv1a32` and `slots`, the key's slot among 1, 7, 16, 1000, 65536 and 2^32 - 1 slots (no `reencoded`) |
| `quicRecords` | `records` (`[{command, body}]`, an `OPEN` with `channelId` and `openPayload`, a `CLOSE` with `code` and `reason`), `buffered`: a channel stream's bytes from its first one (no `reencoded`) |
| `quicHello` | `recordLimit`, `channels`, `exporter` of a `HELLO` payload |
| `quicCode` | for a status code (2 bytes, little endian): `raw` and `rawVarint`, its error code on raw QUIC (null for an application code), `http3` and `http3Varint`, its WebTransport code as HTTP/3 carries it, and `back`, that code read again (no `reencoded`) |
| `exporter` | for a WebTransport session id (8 bytes, big endian): `context`, the TLS exporter context of its exporter-bound `IDENT`, and the labels `webTransportLabel`, `rawQuicLabel`, `identLabel` (no `reencoded`) |

The kinds without a note also answer `reencoded`. The oracle test compares the Dart peer's answers with the TS peer's oracle (`ts_peer/oracle.ts`, the same JSON from the TS codecs) on the wiki vectors and on random and mutated inputs. One difference is documented rather than compared: Dart parses an endpoint as a `Uri` and sends its normalised text (scheme and host lower-cased, characters escaped), where TS carries endpoints as written and refuses what a URI cannot hold (`endpointProblem` in `src/core/naming/naming-protocol.ts`); for such an input the test checks that what Dart sends instead decodes alike in TS.

## Test vectors

`test/vectors/wiki-vectors.json` holds the wiki's vectors with the wiki commit; `tool/extract-wiki-vectors.mjs` regenerates it from a checkout of `kaetemi/sway` (`wiki/switchboard/`). Tests name vectors through `test/support/vectors.ts` (`wikiBytes('test-vectors/names/api')`, `wikiVectorsIn('test-vectors/talk-messages/')`, `skippedWikiVector(id, reason)`), always with string literals: `test/vectors/coverage.test.ts` reads the test sources for them, fails on an id the file does not have, and, once a test names any vector of a wiki table, on every vector of that table no test names. `SWITCHBOARD_VECTORS_STRICT=1` checks every table.

## Dart driver

`dart_driver/` is a Dart package whose tests run the Dart Switchboard in process against the TS peer, which `lib/ts_peer.dart` spawns with Node (`SWITCHBOARD_NODE`, else `node` on PATH) and drives over this protocol: the Dart side is then observable as in the Dart repositories' own two-peer tests, the TS side is the port behind its peer. The driver is for what the TS-driven cells cannot see: what the Dart node itself observes of a TS peer (its own statuses and windows, the LIMITS and HELLO it read, what its handlers, clients and slot handlers are asked). `node tool/dart-peer.mjs driver [-- <dart test arguments>]` runs it with the SDK and Dart checkouts the peer build uses (it writes the driver's `pubspec_overrides.yaml` too). 59 tests in six files, run one file at a time (`dart_test.yaml`), with the fixture they share in `test/support.dart`:

| File | Tests | What the Dart side observes |
| --- | --- | --- |
| `ts_peer_test.dart` | 35 | a Dart node against the TS peer over each transport (requests, item windows, aborts, bulk both ways, plain messages, GOAWAY, raw channels), a Dart credential identifying to the TS peer and the TS peer in return, the TS peer as the requester of a service implemented in the Dart test, and Dart-encoded Talk frames decoded alike by the TS peer's oracle |
| `flow_test.dart` | 4 | a paused Dart reader holding what the TS peer sent within the window it announced, and the receive window the Dart side keeps on a channel the peer opened, while the TS peer keeps sending on another channel (TCP, WebSocket); the LIMITS of a TS peer taking two channels as the Dart connection reads them, an OPEN sent before them beyond the limit refused `RESOURCE_EXHAUSTED`, the Dart pool opening a second connection at the limit; a channel the TS peer never reads stalling at the window it announced while another flows |
| `reconnect_test.dart` | 8 | the Dart `ReconnectingClient` against the TS peer over each transport: its states and its persistent channels when the TS peer sends GOAWAY (moved at once, an application channel finishing on the old connection), closes a persistent channel `PERMISSION_DENIED` (stays closed) or `UNAVAILABLE` (re-opened), or drops the connection |
| `quic_test.dart` | 5 | the Dart QUIC binding against the TS raw QUIC and WebTransport listeners: their `HELLO` against the one a Dart listener configured as the Dart peer's sends, the QUIC session end after the TS listener's GOAWAY with a code and a reason, 64 channels in parallel three times over with bulk payloads on one connection, `WT_DRAIN_SESSION` (`connection.drain`) |
| `talk_handler_test.dart` | 4 | a Dart Talk service under a TS requester (TCP, WebSocket): the item window and credit of a TS stream request as the handler reads them, the TS requester's timeout and cancel reaching `onCancel`, TS bulk payloads against the Dart `maxInlinePayload` (refused before the handler beyond it, streamed reads not held to it) |
| `sharding_test.dart` | 3 | Dart sharded instances, naming clients and a slot router against the TS peer's naming service (`naming: {}`) and its `raw` service: claims confirmed through `ASSIGN` (the holder named, `notHolding`, refusals failing the claim), hand-overs rolled back with `RESUME` when the new owner refuses the `ASSIGN` or the old one the `DRAIN`, the `MOVED` retry of `openChannelToSlot` on a TS backend's CLOSE, never on `RELOCATED` |

A group whose transport the TS peer cannot serve is skipped with the reason. The Dart peer's library `switchboard_dart_peer.dart` exports what the driver shares with it: the test authorities and the byte helpers.
