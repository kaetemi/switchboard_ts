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

The Switchboard wiki's `test-vectors` page is extracted to `test/vectors/wiki-vectors.json` with the wiki commit recorded inside; codec tests assert the vectors by id, and a coverage test fails on a vector nobody asserts. The Dart test suites carry about two hundred more vectors; they are ported with the codec they belong to.

## The Dart peer

`test/interop/dart_peer/` is a small Dart package depending on `kaetemi/switchboard` and `kaetemi/switchboard_quic` by path at the commits pinned in `dart_commits.txt`. Built with `dart build cli`, it is an executable that serves the interop services over TCP, WebSocket, raw QUIC and WebTransport, acts as a client over the same, and decodes protocol messages on request. It speaks a JSON-lines protocol on stdin and stdout; a TS peer speaks the same protocol over this package's public API.

Each scenario runs across four pairings: Dart to Dart (checks the scenario against the reference), TS to Dart, Dart to TS, TS to TS, on every transport the pairing allows. A cell the port cannot serve yet is skipped with a reason.

Locally: a Dart SDK (3.13.5) and a Rust toolchain are needed for the peer; `tool/dart-peer.mjs` builds it. Without them the interop files skip with one visible line. In CI, `SWITCHBOARD_INTEROP=required` turns a missing peer into a failure.

## Clocks

Every timer in the library comes from its `clock` option. Tests use `ManualClock` from `@kaetemi/switchboard/testing` and drive time with `advance(ms)` and `flush()`, the port of Dart's `fake_async`.
