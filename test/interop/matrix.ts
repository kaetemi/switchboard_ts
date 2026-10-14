/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// The pairing matrix: one scenario body runs once per cell, a server peer and
// a client peer of either implementation over one transport. Dart to Dart
// checks the scenario against the reference (a failure there is a broken
// test, not a TS bug); the other three pairings test the port. A cell that
// cannot run is a skipped suite whose name carries the reason.

import { afterAll, afterEach, beforeAll, beforeEach, describe, expect } from 'vitest';
import { nativeQuicAvailability } from '../../src/node/quic/index.ts';
import type { Peer, StartOptions } from './launcher.ts';
import { startPeer, unavailable } from './peers.ts';
import { type Implementation, type PeerConfig, type Transport, transports } from './protocol.ts';

/** A server and a client implementation. */
export type Pairing = readonly [server: Implementation, client: Implementation];

/** The four pairings, the reference first. */
export const pairings: readonly Pairing[] = [
  ['dart', 'dart'],
  ['dart', 'ts'],
  ['ts', 'dart'],
  ['ts', 'ts'],
];

const quic = nativeQuicAvailability();
const noQuic = quic.available
  ? undefined
  : `the native QUIC addon (${quic.reason}; \`npm run build:native\` or SWITCHBOARD_QUIC_ADDON)`;

/**
 * What the TS side cannot do here, by transport: the reason its cells skip
 * even though the TS peer exists. TCP and WebSocket always run; raw QUIC and
 * WebTransport need the native addon (`nativeQuic()`).
 */
export const tsTransportGaps: Readonly<Partial<Record<Transport, string>>> =
  noQuic === undefined ? {} : { quic: noQuic, webtransport: noQuic };

/** One cell of the matrix. */
export interface Cell {
  readonly server: Implementation;
  readonly client: Implementation;
  readonly transport: Transport;
  /** `dart → dart over tcp` style, client first. */
  readonly name: string;
  /** Why the cell cannot run here, or undefined. */
  readonly skip: string | undefined;
}

/** Which cells a scenario runs. */
export interface MatrixOptions {
  /** Default all four. */
  transports?: readonly Transport[];
  /** Default all four. */
  pairings?: readonly Pairing[];
  /** The server peer's configuration; `listen` defaults to the cell's transport. */
  server?: PeerConfig;
  /** The client peer's configuration; it listens nowhere by default. */
  client?: PeerConfig;
  /** Peer start options shared by both, such as a log level. */
  start?: Omit<StartOptions, 'config'>;
}

function skipReason(
  server: Implementation,
  client: Implementation,
  t: Transport,
): string | undefined {
  for (const side of new Set([server, client])) {
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

/** The cells [options] select, with their skip reasons. */
export function matrix(options: MatrixOptions = {}): Cell[] {
  const cells: Cell[] = [];
  for (const [server, client] of options.pairings ?? pairings) {
    for (const transport of options.transports ?? transports) {
      cells.push({
        server,
        client,
        transport,
        name: `${client} → ${server} over ${transport}`,
        skip: skipReason(server, client, transport),
      });
    }
  }
  return cells;
}

/** The two running peers of a cell, valid inside its tests. */
export interface CellPeers {
  readonly cell: Cell;
  /** The peer that listens. */
  readonly server: Peer;
  /** The peer that dials. */
  readonly client: Peer;
  /** The server's endpoint for the cell's transport. */
  readonly endpoint: string;
}

/**
 * Defines [body]'s tests once per cell. Each cell starts its two peers once,
 * resets both after every test (and fails the test on a `peer.error` event),
 * prints both peers' stderr when a test fails, and stops them at the end.
 */
export function describeCells(
  title: string,
  options: MatrixOptions,
  body: (peers: CellPeers) => void,
): void {
  describe(title, () => {
    for (const cell of matrix(options)) {
      if (cell.skip !== undefined) {
        describe.skip(`${cell.name} (skipped: ${cell.skip})`, () => body(unstarted(cell)));
        continue;
      }
      describe(cell.name, () => {
        let server: Peer | undefined;
        let client: Peer | undefined;
        const peers: CellPeers = {
          cell,
          get server() {
            return server ?? notStarted();
          },
          get client() {
            return client ?? notStarted();
          },
          get endpoint() {
            return (server ?? notStarted()).endpoint(cell.transport);
          },
        };
        beforeAll(async () => {
          [server, client] = await Promise.all([
            startPeer(cell.server, {
              ...options.start,
              config: { listen: [cell.transport], ...options.server },
            }),
            startPeer(cell.client, {
              ...options.start,
              config: { listen: [], ...options.client },
            }),
          ]);
        });
        beforeEach(({ onTestFailed }) => {
          onTestFailed(() => {
            for (const p of [server, client]) {
              if (p !== undefined) {
                console.error(p.diagnostics());
              }
            }
          });
        });
        afterEach(async () => {
          const errors = [...(server?.errors ?? []), ...(client?.errors ?? [])];
          await Promise.all([server?.reset(), client?.reset()]);
          expect(errors, 'errors the peers reported outside any command').toEqual([]);
        });
        afterAll(async () => {
          await Promise.all([server?.stop(), client?.stop()]);
        });
        body(peers);
      });
    }
  });
}

function notStarted(): never {
  throw new Error('the cell peers start in beforeAll; use them inside tests');
}

function unstarted(cell: Cell): CellPeers {
  return {
    cell,
    get server(): Peer {
      return notStarted();
    },
    get client(): Peer {
      return notStarted();
    },
    get endpoint(): string {
      return notStarted();
    },
  };
}
