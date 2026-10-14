# Interop tests against the Dart reference

The Dart implementation (`kaetemi/switchboard` and `kaetemi/switchboard_quic`) runs as a separate process, a **peer**, that the tests drive over a JSON-lines protocol on stdin and stdout. A TS peer will speak the same protocol over this package's public API, so that one scenario runs in four pairings: Dart to Dart (which checks the scenario against the reference: a failure there is a broken test, not a TS bug), TS to Dart, Dart to TS and TS to TS.

```
test/interop/
  dart_peer/          the Dart peer: a package depending on the Dart repositories by path,
                      built with `dart build cli` to an AOT executable; dart_commits.txt pins them
  ts_peer/            the TS peer (not yet: it needs the mux and Talk ports)
  protocol.ts         the protocol's messages as types
  launcher.ts         spawns a peer, waits for READY, speaks JSON lines, logs events
  peers.ts            which peers this run has (the Dart peer from the global setup)
  global-setup.ts     builds or locates the Dart peer once per run
  matrix.ts           pairings × transports, skip reasons, describeCells()
  scenarios/          two-peer scenarios over real transports
  oracle.test.ts      the wiki vectors against the Dart codecs; TS codecs against Dart, fuzzed
  probe/              raw byte conformance: hand-built frames against a server's listeners
```

## Running

```sh
npm run test:interop                    # builds the Dart peer if needed, then runs the interop project
node tool/dart-peer.mjs status          # what the build would use
node tool/dart-peer.mjs build           # build only
```

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

Each cell starts its server and client peers once, resets both after every test (and fails the test if either reported a `peer.error`), and prints both peers' stderr when a test fails. A cell that cannot run is a skipped suite whose name says why: the TS peer is missing, or the TS side lacks the transport (`tsTransportGaps` in `matrix.ts`, which each porting thread clears as it lands).

Wait for events rather than sleeping: `peers.server.events.waitFor({ event: 'channel.closed', channel })`. Timeouts cannot be faked across a process boundary, so a scenario that exercises one configures it short (100 to 500 ms) on the side that enforces it.

## Process protocol

Version 1. The peer is started as `<peer> [--config <json>] [--log <level>] [--watchdog <seconds>]`.

- **stdout** carries exactly one `READY {json}` line, then JSON lines only: responses and events. A launcher looks for `READY ` anywhere in a line (`dart run` prints its build progress before it without a newline). **stderr** carries logs; `--log FINE` (a `package:logging` level name) turns on the library's logging.
- **READY** names the endpoints and what the peer can do:

  ```json
  {"protocol": 1, "implementation": "dart", "tcp": "tcp://127.0.0.1:41234",
   "ws": "ws://127.0.0.1:41235/sb", "quic": "quic://127.0.0.1:41236",
   "webtransport": "https://127.0.0.1:41237/sb", "certificateSha256": "<64 hex digits>",
   "pid": 1234, "oracle": ["name", "status", "address", "mux", "control", "talk", "stream"]}
  ```

  Only the transports the peer listens on appear; the QUIC listeners share one self-signed ECDSA P-256 certificate whose SHA-256 is `certificateSha256` (for WebTransport's `serverCertificateHashes`).
- **Commands**, one JSON object per line on stdin: `{"id": 7, "op": "talk.request", ...}`. Each gets exactly one response line, `{"id": 7, "ok": true, "value": ...}` or `{"id": 7, "ok": false, "error": {...}}`. Commands run concurrently, so responses can come out of order.
- **Errors** are `{"kind", "type", "message"}` plus `code` and `reason` for kind `status`: `status` (a Switchboard status: an abort, a refused channel, a protocol error), `format` (malformed input to a codec), `argument` (a bad command), `other`. `type` is the implementation's error class and differs between implementations; tests compare `kind` and `code`.
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
| `services` | `echo`, `raw`, `sink` at instance 1 | `[{"type": "echo", "behaviour": "echo", "instance": 1}]` |

The options apply to the listeners' connections and to the connections the client commands make.

### Services

Behaviours both peers implement identically, so a scenario can run against either.

**`echo`**: the interop procedures over Talk. Payloads are UTF-8 text where a number or a word is expected; `code reason` means a decimal status code, a space, and the rest as the reason.

| Procedure | Request payload | Behaviour |
| --- | --- | --- |
| `ECHO` | anything | final response with the request's payload |
| `UPPER` | text | final response with ASCII `a`–`z` upper-cased, other bytes as they are |
| `SIZE` | anything | final response `"<length> <sha256 lower-case hex>"` of the request payload |
| `BIG` | `n` | final response of n bytes, byte i = `(i * 31) & 0xFF` |
| `COUNT` | `n` | n stream items `"1"` … `"n"`, then the final response `"done"` |
| `ABORT` | `code reason` | abort response with that status |
| `SLOW` | `ms` | final response `"slow"` after ms milliseconds |
| `STALL` | | never answers |
| `NOTIFY` | anything | a plain message `NOTE` with the payload on the channel, then the final response `"sent"` |
| `CLOSE` | `code reason` | final response `"closing"`, then closes the channel with that status |
| `GOAWAY` | | final response `"going"`, then GOAWAY `GOING_AWAY` on the connection |
| `WHOAMI` | | final response with the peer's verified identity, empty without one |
| anything else | | abort response `UNIMPLEMENTED`, no reason |

A plain message (no reply expected) to `echo` is reported as `service.message {service, procedure, payload}`.

**`raw`**: each incoming channel becomes a handle: `channel.incoming {channel, connection, service, id, type, instance, shard, host, payload}`, its subframes `channel.data {channel, hex}`, its end `channel.closed {channel, code, reason}`. `channel.send` and `channel.close` drive it.

**`sink`**: accepts the channel and never reads it, so its window fills up.

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
| `connect` | `endpoint` | `{connection}`: a new connection, not pooled |
| `open` | `connection`, `address` or `openHex` | `{channel, id}`: a raw channel, its data and end reported as events |
| `talk.open` | `connection` or `endpoint`, `address` or `openHex` | `{talk, channel, connection}`; messages the far side sends on it are `talk.message {talk, requestId, procedure, bulk, payload}` |
| `talk.request` | `talk`, `procedure`, payload, `timeoutMs`, `digest` | the final response: `{procedure, bulk, payload}`, or with `digest` `{procedure, bulk, size, sha256}`; a status error for an abort |
| `talk.stream` | `talk`, `procedure`, payload, `window`, `timeoutMs`, `digest` | `{stream, requestId}`; then `talk.item {stream, index, ...message}` per item and `talk.done {stream, items, ...message}` or `talk.done {stream, items, error}` |
| `talk.send` | `talk`, `procedure`, payload | `null`: a plain message |
| `talk.close` | `talk`, `code`, `reason` | `null` |
| `stream.grant` | `stream`, `items` | `null`: item credit |
| `stream.cancel` | `stream`, `code`, `reason` | `null` |
| `channel.send` | `channel`, `hex` | `null`: one subframe |
| `channel.close` | `channel`, `code`, `reason` | `null` |
| `connection.ping` | `connection`, `payload` | `{rttMs}` |
| `connection.goAway` | `connection`, `code` (default 33), `reason` | `null` |
| `connection.close` | `connection` | `null` |
| `request` | `endpoint`, `service`, `instance`, `procedure`, payload, `timeoutMs`, `digest` | one request on a connection of its own, closed afterwards |

An `address` is `{type, typeHex, instance, shard, host, payload}`, every field optional (`type` is text, `typeHex` the 8 name bytes, `payload` hex). A payload argument is one of `payload` (hex), `text` (UTF-8) or `patternLength` (the `BIG` pattern of that many bytes). Endpoints are any the READY line names: `tcp://`, `ws://`, `quic://`, `https://` (WebTransport). The client side does not verify the QUIC certificate.

Connections report `connection.goAway {connection, code, reason}` when the far side sends GOAWAY and `connection.closed {connection, code, reason}` when they end; channels report `channel.closed {channel, code, reason}`. An error no command answers (in a service, say) is `peer.error {error}`.

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

Every kind but `stream` also answers `reencoded`. The naming, sharding, credential, identity and QUIC record kinds come with the scenarios that need them.

## Test vectors

`test/vectors/wiki-vectors.json` holds the wiki's vectors with the wiki commit; `tool/extract-wiki-vectors.mjs` regenerates it from a checkout of `kaetemi/sway` (`wiki/switchboard/`). Tests name vectors through `test/support/vectors.ts` (`wikiBytes('test-vectors/names/api')`, `wikiVectorsIn('test-vectors/talk-messages/')`, `skippedWikiVector(id, reason)`), always with string literals: `test/vectors/coverage.test.ts` reads the test sources for them, fails on an id the file does not have, and, once a test names any vector of a wiki table, on every vector of that table no test names. `SWITCHBOARD_VECTORS_STRICT=1` checks every table.
