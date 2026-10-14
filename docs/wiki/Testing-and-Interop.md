# Testing and Interop

## Layers

| Project | Where | What | Command |
| --- | --- | --- | --- |
| `unit` | `src/**/*.test.ts`, `test/unit`, `test/vectors` | codecs against the wiki vectors; behaviour ports of the Dart unit tests over the memory transport with `ManualClock` | `npm run test:unit` |
| `integration` | `test/integration` | two or more nodes in one process over memory, TCP, WebSocket and QUIC on the loopback | `npm run test:integration` |
| `interop` | `test/interop` | the Dart reference implementation as a peer | `npm run test:interop` |
| `browser` | `test/browser` | the web entry in headless Chromium against a WebTransport listener | (added by the QUIC thread) |

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

`test/interop/dart_peer/` is a small Dart package depending on `kaetemi/switchboard` and `kaetemi/switchboard_quic` by path, at the commits pinned in `dart_commits.txt`. Built with `dart build cli`, it is an executable that serves the interop services over TCP, WebSocket, raw QUIC and WebTransport, acts as a client over the same, and decodes and encodes protocol messages on request. It speaks a JSON-lines protocol on stdin and stdout; a TS peer speaks the same protocol over this package's public API. `test/interop/README.md` is the protocol's specification: the READY line, the commands and events, the configuration, and the `echo`, `raw` and `sink` services.

`tool/dart-peer.mjs` builds and locates it:

```sh
node tool/dart-peer.mjs status     # what a build would use, and whether the peer is fresh
node tool/dart-peer.mjs build      # build if the sources, SDK or checkouts changed
node tool/dart-peer.mjs fetch      # clone the pinned commits into dart_peer/.deps/
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

**Scenarios** (`test/interop/scenarios/`) run across four pairings of server and client (Dart and Dart, Dart and TS, TS and Dart, TS and TS) on every transport (TCP, WebSocket, raw QUIC, WebTransport). The Dart to Dart cell checks the scenario against the reference, so a failure there is a broken test, not a TS bug. A cell that cannot run is a skipped suite whose name says why: the TS peer is missing, or the TS side lacks the transport (`tsTransportGaps` in `matrix.ts`, cleared as each port lands).

**The oracle** (`oracle.test.ts`) decodes every wiki vector with the Dart codecs and checks the fields and the re-encoding against the wiki, then checks the TS codecs against the Dart ones on the vectors and on seeded random and mutated inputs. The seed is in the test names; `SWITCHBOARD_FUZZ_SEED` and `SWITCHBOARD_FUZZ_COUNT` replay or widen a run.

**Raw probes** (`probe/`) send hand-built frames to a server's TCP and WebSocket listeners and check the reaction byte for byte: the preamble and LIMITS, a Talk exchange, PING, refused and oversized frames, the channel window, GOAWAY in both directions, and every negative mux vector of the wiki. Each probe runs against the Dart server and, once it exists, the TS server.

In CI the `interop` job checks out the two Dart repositories at the pinned commits, installs Dart and Rust, builds the peer (cached on the peer's sources and the pins) and runs `npm run test:interop` with `SWITCHBOARD_INTEROP=required`. When Dart or Rust cannot be installed it warns and runs with the Dart cells skipped.

## Clocks

Every timer in the library comes from its `clock` option. Tests use `ManualClock` from `@polyverse/switchboard/testing` and drive time with `advance(ms)` and `flush()`, the port of Dart's `fake_async`. Timeouts cannot be faked across a process boundary, so an interop scenario that exercises one configures it short (100 to 500 ms) on the side that enforces it.
