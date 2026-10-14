# Interop tests against the Dart reference

Owned by the "Dart interop test harness" thread; see `docs/work-split.md` thread 6 and the proposal in the project notes.

Layout (planned):

```
test/interop/
  dart_peer/        a Dart package depending on kaetemi/switchboard and kaetemi/switchboard_quic by path,
                    built with `dart build cli` to an AOT executable; dart_commits.txt pins the commits
  ts_peer/          the same peer protocol over this package's public API
  launcher.ts       spawns a peer, waits for READY, speaks JSON lines
  matrix.ts         the pairings (Dart↔Dart, TS→Dart, Dart→TS, TS↔TS) and transport cells
  scenarios/        *.test.ts
  oracle.test.ts    codec differential against the Dart decoders
  probe/            raw byte conformance scripts
```

`npm run test:interop` builds or locates the Dart peer through `tool/dart-peer.mjs`; without a Dart SDK the files skip with one visible line, and `SWITCHBOARD_INTEROP=required` turns that into a failure (CI).
