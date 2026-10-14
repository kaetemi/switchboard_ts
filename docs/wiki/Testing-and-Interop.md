# Testing and Interop

## Layers

| Project | Where | What | Command |
| --- | --- | --- | --- |
| `unit` | `src/**/*.test.ts`, `test/unit`, `test/vectors` | codecs against the wiki vectors; behaviour ports of the Dart unit tests over the memory transport with `ManualClock` | `npm run test:unit` |
| `integration` | `test/integration` | two or more nodes in one process over memory, TCP, WebSocket and QUIC on the loopback | `npm run test:integration` |
| `interop` | `test/interop` | the Dart reference implementation and the TS port as peers of each other | `npm run test:interop` |
| `browser` | `test/browser` | the web entry in headless Chromium against a WebTransport listener | `npm run test:browser` |

`npm test` runs `unit` and `integration`; `npm run check` adds lint and typecheck.

## Test vectors

The Switchboard wiki's vectors (the `test-vectors` page and the vectors of `quic-binding`) are extracted to `test/vectors/wiki-vectors.json`, with the wiki commit recorded inside:

```sh
node tool/extract-wiki-vectors.mjs ../sway/wiki/switchboard
```

Every table row becomes a vector with an id `page/section/case` (`test-vectors/names/api`), its bytes as hex, and whether it is negative (must be rejected) or composite (built from other vectors). Bytes the wiki gives in words ("`X` followed by 65 bytes `00`", "byte 3 = `FF`", "the 86 bytes of C1") are resolved; a row whose bytes are not described has `hex: null`. Vectors in prose paragraphs get `prose-` ids and belong to the table above them.

Tests name vectors through `test/support/vectors.ts`, always with string literals:

```ts
import { wikiBytes, wikiVectorsIn, skippedWikiVector } from '../support/vectors.ts';

const api = wikiBytes('test-vectors/names/api'); // the bytes of one vector
test.each(wikiVectorsIn('test-vectors/talk-messages/'))('$case', (v) => { /* ... */ });
skippedWikiVector('test-vectors/credentials/truncated', 'the credential codec is not ported yet');
```

`test/vectors/coverage.test.ts` reads the test sources for those calls. It fails on an id the file does not have, and, once a test names any vector of a wiki table, on every vector of that table no test names or skips; a table nobody has started is a `todo`. `SWITCHBOARD_VECTORS_STRICT=1` checks every table, for the end of the port.

The Dart test suites carry about two hundred more vectors; they are ported with the codec they belong to.

## The Dart peer

`test/interop/dart_peer/` is a small Dart package depending on `kaetemi/switchboard` and `kaetemi/switchboard_quic` by path, at the commits pinned in `dart_commits.txt`. Built with `dart build cli`, it is an executable that serves the interop services over TCP, WebSocket, raw QUIC and WebTransport, acts as a client over the same, and decodes and encodes protocol messages on request. It speaks a JSON-lines protocol on stdin and stdout; the TS peer (`test/interop/ts_peer/`, run as `node test/interop/ts_peer/main.ts`) speaks the same protocol over this package's public Node entry only. `test/interop/README.md` is the protocol's specification: the READY line, the commands and events, the configuration (identity with test authorities both peers derive, a naming service, a proxy, a relay), and the `echo`, `raw`, `sink` and `forward` services. The TS peer's raw QUIC and WebTransport need the native QUIC addon (`npm run build:native` or `SWITCHBOARD_QUIC_ADDON`); without it those TS cells skip, and `SWITCHBOARD_NATIVE=required` fails instead.

`tool/dart-peer.mjs` builds and locates it:

```sh
node tool/dart-peer.mjs status     # what a build would use, and whether the peer is fresh
node tool/dart-peer.mjs build      # build if the sources, SDK or checkouts changed
node tool/dart-peer.mjs fetch      # clone the pinned commits into dart_peer/.deps/
node tool/dart-peer.mjs driver     # the Dart driver's scenarios against the TS peer
```

It needs a Dart SDK (3.13.5 in CI) and a Rust toolchain (`switchboard_quic` compiles its QUIC library with cargo). The Dart repositories are taken from `SWITCHBOARD_DART_CORE` and `SWITCHBOARD_DART_QUIC`, sibling checkouts, or the pinned clones; a checkout at another commit than the pin gives a warning locally and fails CI. `npm run test:interop` builds the peer once per run in Vitest's global setup.

| Variable | Effect |
| --- | --- |
| `SWITCHBOARD_INTEROP=required` | a missing Dart peer fails the run (CI) |
| `SWITCHBOARD_INTEROP=skip` | skip the Dart cells without looking for a peer |
| `SWITCHBOARD_INTEROP_BUILD=0` | use a peer that is already built, never build |
| `SWITCHBOARD_DART_PEER` | a prebuilt peer executable |
| `SWITCHBOARD_DART_SDK`, `DART_SDK` | a Dart SDK not on PATH |

Without a peer the interop files skip, and the run prints one line saying why.

## What the interop project checks

**Scenarios** (`test/interop/scenarios/`) run across four pairings of server and client (Dart and Dart, Dart and TS, TS and Dart, TS and TS) on every transport (TCP, WebSocket, raw QUIC, WebTransport): Talk requests, streams with item windows, aborts, timeouts and EXTEND, Talk byte for byte against raw channels, bulk payloads at the inline boundary and beyond, closes and GOAWAY, mux limits and keep-alive, protocol errors injected both ways, forwarding, identity (holder keys and bearer credentials, intents, receivers, refused credentials, held channels; exporter-bound over QUIC), and naming and sharding (a naming service of either language serving clients of the other). Chains (`chain.ts`) put a proxy, a relay, a forwarding service or a naming service of either language between a client and a backend of either, eight cells per transport. The all-Dart cell checks a scenario against the reference, so a failure there is a broken test, not a TS bug. A cell that cannot run is a skipped suite whose name says why.

**The oracle** (`oracle.test.ts`) decodes every wiki vector with the Dart codecs and checks the fields and the re-encoding against the wiki, then checks the TS codecs (through the TS peer's oracle, `ts_peer/oracle.ts`) against the Dart ones on the vectors and on seeded random and mutated inputs: names, statuses, addresses, mux frames and control messages, Talk frames, the stream binding, every naming and sharding payload, credentials, slot keys, QUIC records, HELLO, QUIC and HTTP/3 codes and the WebTransport exporter context. The seed is in the test names; `SWITCHBOARD_FUZZ_SEED` and `SWITCHBOARD_FUZZ_COUNT` replay or widen a run.

**Raw probes** (`probe/`) send hand-built frames to a server's TCP and WebSocket listeners and check the reaction byte for byte: the preamble and LIMITS, a Talk exchange, PING, refused and oversized frames, the channel window and channel limit, GOAWAY in both directions, and every negative mux vector of the wiki. Each probe runs against the Dart server and the TS server.

**The Dart driver** (`test/interop/dart_driver/`, `node tool/dart-peer.mjs driver`) runs Dart-written scenarios with the Dart Switchboard in process against the TS peer it spawns.

In CI the `interop` job checks out the two Dart repositories at the pinned commits, installs Dart and Rust, builds the peer (cached on the peer's sources and the pins) and the native QUIC addon, runs `npm run test:interop` with `SWITCHBOARD_INTEROP=required` and `SWITCHBOARD_NATIVE=required`, then the Dart driver. When Dart or Rust cannot be installed it warns and runs with the cells needing them skipped.

## Clocks

Every timer in the library comes from its `clock` option. Tests use `ManualClock` from `@kaetemi/switchboard/testing` and drive time with `advance(ms)` and `flush()`, the port of Dart's `fake_async`. Timeouts cannot be faked across a process boundary, so an interop scenario that exercises one configures it short (100 to 500 ms) on the side that enforces it.
