# Testing and Interop

## Layers

| Layer | Where | What | Command |
| --- | --- | --- | --- |
| `unit` | `src/**/*.test.ts`, `test/unit`, `test/vectors` | codecs against the wiki vectors; ports of the Dart unit tests, one TS file per Dart file, with `ManualClock` | `npm run test:unit` |
| `integration` | `test/integration` | nodes end to end over memory, TCP, WebSocket and QUIC on the loopback (the in-memory QUIC provider, and the native one when the addon is built) | `npm run test:integration` |
| `interop` | `test/interop` | the Dart reference implementation and the TS port as peers of each other, in every pairing | `npm run test:interop` |
| browser | `test/browser` (its own `vitest.config.ts`) | the WebTransport provider and the QUIC binding in Playwright's headless Chromium and Firefox (projects `chromium` and `firefox`) against native listeners in the test process | `npm run test:browser` |

`npm test` runs `unit` and `integration`; `npm run check` adds lint (Biome) and the typecheck of `src`; `npx tsc -p tsconfig.test.json --noEmit` type-checks the tests too. `unit`, `integration` and `interop` are the projects of the root `vitest.config.ts`; the browser tests are a configuration of their own because they need the native addon for their listeners and a browser. The node, naming and client suites run twice, over their in-memory fixtures and over the real frame mux (the `over-frame-mux/` directories).

| Variable | Effect |
| --- | --- |
| `SWITCHBOARD_QUIC_ADDON=<file>` | the native addon to load (else `native/sbq-node.node`, else the platform package) |
| `SWITCHBOARD_NATIVE=required` | a missing addon fails the native QUIC files instead of skipping them (CI) |
| `SWITCHBOARD_BROWSER=required` | a missing addon, Playwright or browser fails the browser tests instead of skipping them (CI); `SWITCHBOARD_BROWSER_LOG=1` prints the page's log |
| `PLAYWRIGHT_BROWSERS_PATH` | where Playwright's browsers are (`npx playwright install chromium firefox` puts them in the default place) |
| `SWITCHBOARD_VECTORS_STRICT=1` | every wiki vector table must be covered, not only those a test started on |

The native QUIC files skip with a reason without the addon: build it with `npm run build:native` (Rust 1.88 or later; CI uses 1.99.0) or point `SWITCHBOARD_QUIC_ADDON` at one. The browser tests use the `playwright` dev dependency (1.64.1: Chromium 156 and Firefox 157); `npm run test:browser -- --project firefox` runs one browser. Where Firefox departs from the drafts, the tests check the departure ([Runtimes and QUIC](Runtimes-and-QUIC)). CI also runs the unit project on Bun and on Deno, and the built package on Deno (`test/runtime/smoke.mjs`).

## Test vectors

The Switchboard wiki's vectors (the `test-vectors` page and the vectors of `quic-binding`) are extracted to `test/vectors/wiki-vectors.json`, with the wiki commit recorded inside:

```sh
node tool/extract-wiki-vectors.mjs ../sway/wiki/switchboard
```

Every table row becomes a vector with an id `page/section/case` (`test-vectors/names/api`), its bytes as hex, and whether it is negative (must be rejected) or composite (built from other vectors). Bytes the wiki gives in words ("`X` followed by 65 bytes `00`", "byte 3 = `FF`", "the 86 bytes of C1") are resolved; a row whose bytes are not described has `hex: null`. Vectors in prose paragraphs get `prose-` ids and belong to the table above them.

Tests name vectors through `test/support/vectors.ts`, always with string literals:

```ts
import { skippedWikiVector, wikiBytes, wikiVectorsIn } from '../support/vectors.ts';

const api = wikiBytes('test-vectors/names/api'); // the bytes of one vector
test.each(wikiVectorsIn('test-vectors/talk-messages/'))('$case', (v) => {
  /* ... */
});
skippedWikiVector('test-vectors/credentials/truncated', 'why this one is not asserted');
```

`test/vectors/coverage.test.ts` reads the test sources for those calls. It fails on an id the file does not have, and, once a test names any vector of a wiki table, on every vector of that table no test names or skips; a table nobody has started is a `todo`. The Dart test suites carry about two hundred more vectors; they are ported with the codec they belong to.

## The Dart peer

`test/interop/dart_peer/` is a small Dart package depending on `kaetemi/switchboard` and `kaetemi/switchboard_quic` by path, at the commits pinned in `dart_commits.txt`. Built with `dart build cli`, it is an executable that serves the interop services over TCP, WebSocket, raw QUIC and WebTransport, acts as a client over the same, and decodes and encodes protocol messages on request. It speaks a JSON-lines protocol on stdin and stdout; the TS peer (`test/interop/ts_peer/`, run as `node test/interop/ts_peer/main.ts`) speaks the same protocol over this package's public Node entry only. `test/interop/README.md` is the protocol's specification: the READY line, the commands and events, the configuration (identity with test authorities both peers derive, a naming service, a proxy, a relay), and the `echo`, `raw`, `sink` and `forward` services. The TS peer's raw QUIC and WebTransport need the native QUIC addon (`npm run build:native` or `SWITCHBOARD_QUIC_ADDON`); without it those TS cells skip, and `SWITCHBOARD_NATIVE=required` fails instead.

`tool/dart-peer.mjs` builds and locates it:

```sh
node tool/dart-peer.mjs status     # what a build would use, and whether the peer is fresh
node tool/dart-peer.mjs build      # build if the sources, SDK or checkouts changed
node tool/dart-peer.mjs fetch      # clone the pinned commits into dart_peer/.deps/
node tool/dart-peer.mjs driver     # the Dart driver's scenarios against the TS peer
```

It needs a Dart SDK (3.13.5 in CI; `dart` on PATH, `SWITCHBOARD_DART_SDK`, `DART_SDK`, or `build --download-sdk`) and a Rust toolchain (`switchboard_quic` compiles its QUIC library with cargo). The Dart repositories are taken from `SWITCHBOARD_DART_CORE` and `SWITCHBOARD_DART_QUIC`, sibling checkouts, or the pinned clones; a checkout at another commit than the pin gives a warning locally and fails CI. `npm run test:interop` builds the peer once per run in Vitest's global setup.

| Variable | Effect |
| --- | --- |
| `SWITCHBOARD_INTEROP=required` | a missing Dart peer fails the run (CI) |
| `SWITCHBOARD_INTEROP=skip` | skip the Dart cells without looking for a peer |
| `SWITCHBOARD_INTEROP_BUILD=0` | use a peer that is already built, never build |
| `SWITCHBOARD_DART_PEER` | a prebuilt peer executable |
| `SWITCHBOARD_DART_SDK`, `DART_SDK` | a Dart SDK not on PATH |
| `SWITCHBOARD_FUZZ_SEED`, `SWITCHBOARD_FUZZ_COUNT` | replay or widen the oracle's random inputs (the seed is in the test names) |

Without a peer the interop files skip, and the run prints one line saying why.

## What the interop project checks

**Scenarios** (`test/interop/scenarios/`) run across four pairings of server and client (Dart and Dart, Dart and TS, TS and Dart, TS and TS) on every transport (TCP, WebSocket, raw QUIC, WebTransport): Talk requests, streams with item windows, aborts, timeouts and EXTEND, Talk byte for byte against raw channels, bulk payloads at the inline boundary and beyond, closes and GOAWAY, mux limits and keep-alive, protocol errors injected both ways, forwarding, identity (holder keys and bearer credentials, intents, receivers, refused credentials, held channels; exporter-bound over QUIC), and naming and sharding (a naming service of either language serving clients of the other). Chains (`chain.ts`) put a proxy, a relay, a forwarding service or a naming service of either language between a client and a backend of either, eight cells per transport. The all-Dart cell checks a scenario against the reference, so a failure there is a broken test, not a TS bug. A cell that cannot run is a skipped suite whose name says why.

**The oracle** (`oracle.test.ts`) decodes every wiki vector with the Dart codecs and checks the fields and the re-encoding against the wiki, then checks the TS codecs (through the TS peer's oracle, `ts_peer/oracle.ts`) against the Dart ones on the vectors and on seeded random and mutated inputs: names, statuses, addresses, mux frames and control messages, Talk frames, the stream binding, every naming and sharding payload, credentials, slot keys, QUIC records, HELLO, QUIC and HTTP/3 codes and the WebTransport exporter context. The seed is in the test names; `SWITCHBOARD_FUZZ_SEED` and `SWITCHBOARD_FUZZ_COUNT` replay or widen a run.

**Raw probes** (`probe/`) send hand-built frames to a server's TCP and WebSocket listeners and check the reaction byte for byte: the preamble and LIMITS, a Talk exchange, PING, refused and oversized frames, the channel window and channel limit, GOAWAY in both directions, and every negative mux vector of the wiki. Each probe runs against the Dart server and the TS server.

**The Dart driver** (`test/interop/dart_driver/`, `node tool/dart-peer.mjs driver`) runs Dart-written scenarios with the Dart Switchboard in process against the TS peer it spawns.

In CI the `interop` job checks out the two Dart repositories at the pinned commits, installs Dart and Rust, builds the peer (cached on the peer's sources and the pins) and the native QUIC addon, runs `npm run test:interop` with `SWITCHBOARD_INTEROP=required` and `SWITCHBOARD_NATIVE=required`, then the Dart driver. When Dart or Rust cannot be installed it warns and runs with the cells needing them skipped.

## Clocks

Every timer in the library comes from its `clock` option. Tests use `ManualClock` from `@kaetemi/switchboard/testing`, the port of Dart's `fake_async`: `advance(ms)` and `flush()` fire due timers synchronously; code driven by promises (a Talk channel, a mux reading its transport, a Web Streams pipe) uses `await clock.elapseAsync(ms)` and `await clock.flushAsync()`, which let promise jobs run before, between and after the timers, and `await ManualClock.settle()` lets everything not waiting for a timer run. `pendingCount` and `nextAt` tell what is armed. Timeouts cannot run on virtual time across a process boundary, so an interop scenario that exercises one configures it short (100 to 500 ms) on the side that enforces it.

```ts
import { ManualClock } from '@kaetemi/switchboard/testing';
import { FrameMuxConnection, memoryTransportPair } from '@kaetemi/switchboard';

const clock = new ManualClock();
const [a, b] = memoryTransportPair({ delay: 2, clock }); // every frame takes 2 ms of the manual clock
const client = new FrameMuxConnection(a, { isInitiator: true, clock });
const server = new FrameMuxConnection(b, { isInitiator: false, clock });
const rtt = client.ping();
await clock.elapseAsync(10);
console.log(await rtt); // 4

const closed = Promise.all([client.close(), server.close()]);
await clock.elapseAsync(100);
await closed;
console.log('closed');
```
