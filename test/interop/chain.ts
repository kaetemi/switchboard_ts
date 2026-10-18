/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Chains and meshes of more than two peers: every role played by either
// implementation, so that a proxy, a relay or a naming service of one
// language sits between peers of the other. A chain of three roles runs in
// eight implementation cells per transport; the all-Dart cell checks the
// scenario against the reference. Peers start from the last role (the
// backend) to the first (the client), each configured from those started
// before it, and live for the cell.

import { afterAll, afterEach, beforeAll, beforeEach, describe, expect } from 'vitest';
import type { Peer } from './launcher.ts';
import { tsTransportGaps } from './matrix.ts';
import { startPeer, unavailable } from './peers.ts';
import { type Implementation, type PeerConfig, type Transport, transports } from './protocol.ts';

/** One role of a chain. */
export interface ChainRole {
  /** Its name in cell titles and for {@link ChainPeers.peer}. */
  readonly name: string;
  /** Its configuration, from the peers started before it and the cell's transport. */
  config(started: ChainPeers, transport: Transport): PeerConfig;
}

/** Which cells a chain runs. */
export interface ChainOptions {
  /** The roles, from the one that starts first (the backend) to the client. */
  readonly roles: readonly ChainRole[];
  /** Default all four. */
  readonly transports?: readonly Transport[];
}

/** One cell: an implementation per role and a transport. */
export interface ChainCell {
  readonly implementations: readonly Implementation[];
  readonly transport: Transport;
  readonly name: string;
  readonly skip: string | undefined;
}

/** The running peers of a cell, by role. */
export interface ChainPeers {
  readonly cell: ChainCell;
  /** The peer playing [role]. */
  peer(role: string): Peer;
  /** The endpoint [role]'s peer listens on for the cell's transport. */
  endpoint(role: string): string;
}

function combinations(n: number): Implementation[][] {
  if (n === 0) {
    return [[]];
  }
  return combinations(n - 1).flatMap((rest) => [
    [...rest, 'dart'] as Implementation[],
    [...rest, 'ts'] as Implementation[],
  ]);
}

function skipReason(implementations: readonly Implementation[], t: Transport): string | undefined {
  for (const side of new Set(implementations)) {
    const missing = unavailable(side);
    if (missing !== undefined) {
      return `${side} peer: ${missing}`;
    }
    const gap = side === 'ts' ? tsTransportGaps[t] : undefined;
    if (gap !== undefined) {
      return `TS needs ${gap}`;
    }
  }
  return undefined;
}

/**
 * Defines [body]'s tests once per cell of [options]. Each cell starts its
 * peers once, resets every peer after each test (failing it on a
 * `peer.error`), prints their stderr when a test fails and stops them at the
 * end.
 */
export function describeChains(
  title: string,
  options: ChainOptions,
  body: (peers: ChainPeers) => void,
): void {
  const { roles } = options;
  describe(title, () => {
    for (const transport of options.transports ?? transports) {
      for (const implementations of combinations(roles.length)) {
        const name = [...roles]
          .map((r, i) => `${r.name} ${implementations[i]}`)
          .reverse()
          .join(' → ');
        const cell: ChainCell = {
          implementations,
          transport,
          name: `${name} over ${transport}`,
          skip: skipReason(implementations, transport),
        };
        const running = new Map<string, Peer>();
        const peers: ChainPeers = {
          cell,
          peer(role) {
            const p = running.get(role);
            if (p === undefined) {
              throw new Error(`no peer for role ${role} (yet)`);
            }
            return p;
          },
          endpoint(role) {
            return this.peer(role).endpoint(transport);
          },
        };
        if (cell.skip !== undefined) {
          describe.skip(`${cell.name} (skipped: ${cell.skip})`, () => body(peers));
          continue;
        }
        describe(cell.name, () => {
          beforeAll(async () => {
            for (const [i, role] of roles.entries()) {
              const config = role.config(peers, transport);
              running.set(
                role.name,
                await startPeer(implementations[i] as Implementation, {
                  config: { listen: [transport], ...config },
                }),
              );
            }
          });
          beforeEach(({ onTestFailed }) => {
            onTestFailed(() => {
              for (const p of running.values()) {
                console.error(p.diagnostics());
              }
            });
          });
          afterEach(async () => {
            const errors = [...running.values()].flatMap((p) => p.errors);
            await Promise.all([...running.values()].map((p) => p.reset()));
            expect(errors, 'errors the peers reported outside any command').toEqual([]);
          });
          afterAll(async () => {
            await Promise.all([...running.values()].map((p) => p.stop()));
            running.clear();
          });
          body(peers);
        });
      }
    }
  });
}
