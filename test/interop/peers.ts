/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Which peers this run can start: the Dart peer the global setup built or
// located, and the TS peer once it exists. A missing peer makes its cells
// skip with the reason, unless SWITCHBOARD_INTEROP=required.

import { existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { inject } from 'vitest';
import { ensurePeer } from '../../tool/dart-peer.mjs';
import { Peer, type PeerCommand, type StartOptions } from './launcher.ts';
import type { Implementation } from './protocol.ts';

/** A peer this run can start, or why it cannot. */
export type Availability = { readonly path: string } | { readonly reason: string };

declare module 'vitest' {
  export interface ProvidedContext {
    /** The Dart peer executable, or why there is none (test/interop/global-setup.ts). */
    dartPeer: Availability;
  }
}

const { SWITCHBOARD_INTEROP } = process.env;

/** Whether interop peers are required (CI): `SWITCHBOARD_INTEROP=required`. */
export const interopRequired = SWITCHBOARD_INTEROP === 'required';

const tsPeerMain = fileURLToPath(new URL('./ts_peer/main.ts', import.meta.url));

function locateDart(): Availability {
  if (SWITCHBOARD_INTEROP === 'skip') {
    return { reason: 'SWITCHBOARD_INTEROP=skip' };
  }
  const provided = inject('dartPeer');
  if (provided !== undefined) {
    return provided;
  }
  // Without the global setup: use a peer that is already built.
  const r = ensurePeer({ build: false });
  return 'path' in r
    ? { path: r.path }
    : { reason: `${r.reason}; build it with \`node tool/dart-peer.mjs build\`` };
}

function locateTs(): Availability {
  return existsSync(tsPeerMain)
    ? { path: tsPeerMain }
    : {
        reason: `no TS peer at ${tsPeerMain}`,
      };
}

/** The peers of this run, by implementation. */
export const availability: Readonly<Record<Implementation, Availability>> = {
  dart: locateDart(),
  ts: locateTs(),
};

if (interopRequired && 'reason' in availability.dart) {
  throw new Error(
    `SWITCHBOARD_INTEROP=required but the Dart peer is missing: ${availability.dart.reason}`,
  );
}

/** The reason [implementation] cannot run, or undefined when it can. */
export function unavailable(implementation: Implementation): string | undefined {
  const a = availability[implementation];
  return 'reason' in a ? a.reason : undefined;
}

/** The command line of [implementation]'s peer. Throws when it is unavailable. */
export function peerCommand(implementation: Implementation): PeerCommand {
  const a = availability[implementation];
  if ('reason' in a) {
    throw new Error(`${implementation} peer unavailable: ${a.reason}`);
  }
  return implementation === 'dart'
    ? { label: 'dart', command: a.path }
    : { label: 'ts', command: process.execPath, args: [a.path] };
}

/** Starts a peer of [implementation]. */
export function startPeer(implementation: Implementation, options?: StartOptions): Promise<Peer> {
  return Peer.start(peerCommand(implementation), options);
}
