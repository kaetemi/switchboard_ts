/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Global setup of the `interop` project: builds the Dart peer when it is
// missing or stale (tool/dart-peer.mjs) once per run, before the test files
// start, and hands its path, or the reason there is none, to them as
// `inject('dartPeer')`. Without a Dart SDK or the Dart checkouts the Dart
// cells skip with that reason; SWITCHBOARD_INTEROP=required (CI) makes it a
// failure, SWITCHBOARD_INTEROP=skip skips without looking, and
// SWITCHBOARD_INTEROP_BUILD=0 only uses a peer that is already built.

import type { TestProject } from 'vitest/node';
import { ensurePeer } from '../../tool/dart-peer.mjs';
import type { Availability } from './peers.ts';

/** Builds or locates the Dart peer and provides it to the test files. */
export default function setup(project: TestProject): void {
  const { SWITCHBOARD_INTEROP: mode = 'auto', SWITCHBOARD_INTEROP_BUILD } = process.env;
  let dart: Availability;
  if (mode === 'skip') {
    dart = { reason: 'SWITCHBOARD_INTEROP=skip' };
  } else {
    const r = ensurePeer({
      build: SWITCHBOARD_INTEROP_BUILD !== '0',
      log: (line) => console.log(line),
    });
    for (const w of 'path' in r ? r.warnings : []) {
      console.warn(`interop: warning: ${w}`);
    }
    dart = 'path' in r ? { path: r.path } : { reason: r.reason };
  }
  if ('reason' in dart) {
    if (mode === 'required') {
      throw new Error(`SWITCHBOARD_INTEROP=required but the Dart peer is missing: ${dart.reason}`);
    }
    console.warn(`interop: the Dart cells skip: ${dart.reason}`);
  }
  project.provide('dartPeer', dart);
}
