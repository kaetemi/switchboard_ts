/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// The types of tool/dart-peer.mjs, for the TypeScript files that import it
// (test/interop/global-setup.ts and peers.ts). Kept in step with the JSDoc
// there, which `// @ts-check` checks the script against.

/** The Dart SDK release the peer is built and tested with. */
export declare const dartVersion: string;

/** The repository root. */
export declare const repoRoot: string;

/** The Dart peer package. */
export declare const peerDir: string;

/** The Dart repositories the peer depends on, by package name. */
export declare const repositories: {
  readonly switchboard: {
    readonly github: 'kaetemi/switchboard';
    readonly env: 'SWITCHBOARD_DART_CORE';
  };
  readonly switchboard_quic: {
    readonly github: 'kaetemi/switchboard_quic';
    readonly env: 'SWITCHBOARD_DART_QUIC';
  };
};

/** A Dart repository's package name. */
export type RepoName = keyof typeof repositories;

/** A Dart SDK: its executable, version, and where it was found. */
export interface DartSdk {
  dart: string;
  version: string;
  source: string;
}

/** Why something the build needs is missing. */
export interface Missing {
  reason: string;
}

/** A checkout's path, commit, and a fingerprint of its uncommitted edits. */
export interface Checkout {
  path: string;
  head: string | undefined;
  fingerprint: string;
}

/** Where the two Dart repositories are, and whether an overrides file points there. */
export interface Deps {
  repos: Record<RepoName, Checkout>;
  overrides: boolean;
}

/** A located or built peer, and any differences from the pins. */
export interface Peer {
  path: string;
  built: boolean;
  warnings: string[];
}

/** The pinned commits of dart_commits.txt. */
export declare function readPins(): Record<RepoName, string>;

/** The Dart executable to build with. */
export declare function findDart(options?: {
  download?: boolean;
  log?: (line: string) => void;
}): DartSdk | Missing;

/**
 * Where the two Dart repositories are: environment, sibling checkouts, or
 * the pinned clones of `fetch`. Writes or removes pubspec_overrides.yaml to
 * match.
 */
export declare function resolveDeps(): Deps | Missing;

/** The peer executable inside the bundle `dart build cli` writes. */
export declare function peerExecutable(): string;

/** Differences between the checkouts and the pins, as warnings. */
export declare function pinWarnings(deps: Deps): string[];

/**
 * The peer executable: SWITCHBOARD_DART_PEER, else the built one, building
 * it first when [build] is set and it is missing or stale.
 */
export declare function ensurePeer(options?: {
  build?: boolean;
  force?: boolean;
  download?: boolean;
  expectPins?: boolean;
  log?: (line: string) => void;
}): Peer | Missing;
