/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// The TS Switchboard as a test peer for the interop tests, the counterpart of
// dart_peer/bin/dart_peer.dart. See test/interop/README.md for the protocol.
// Runs on Node 22.18 or later as `node test/interop/ts_peer/main.ts` (type
// stripping), and on Bun.

import { runPeer } from './peer.ts';

try {
  await runPeer(process.argv.slice(2));
} catch (e) {
  // Before READY: the launcher reports the exit with stderr.
  process.stderr.write(`ts_peer: ${e instanceof Error ? e.stack : e}\n`);
  process.exit(1);
}
