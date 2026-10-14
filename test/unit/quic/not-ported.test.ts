// The `switchboard_quic` tests that have no TypeScript counterpart, each
// with its reason, so that the inventory of the QUIC thread's ports is
// complete.
//
// Not ported: `test/build_hook_test.dart` tests Dart's native-assets build
// hook (`hook/binary_target.dart`: the ELF, Mach-O and PE header checks by
// which the hook tells the target of a prebuilt library). The TypeScript
// package has no build hook: `native/build.mjs` builds the N-API addon with
// cargo for the target it is given, and the loader
// (`src/node/quic/native-provider.ts`) picks the per-platform package by
// `process.platform` and `process.arch` and checks the addon's ABI version.
import { test } from 'vitest';

test.todo(
  "build_hook_test: Dart's native-assets build hook has no TypeScript counterpart (native/build.mjs stands in)",
);
