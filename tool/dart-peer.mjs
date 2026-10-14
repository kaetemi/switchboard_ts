#!/usr/bin/env node
/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// @ts-check

// Builds, locates and describes the Dart test peer of test/interop/dart_peer.
//
//   node tool/dart-peer.mjs build [--download-sdk] [--force] [--expect-pins]
//   node tool/dart-peer.mjs locate [--json]
//   node tool/dart-peer.mjs status
//   node tool/dart-peer.mjs fetch
//
// `build` finds a Dart SDK (PATH, then SWITCHBOARD_DART_SDK or DART_SDK, then
// one this tool downloaded; `--download-sdk` downloads Dart 3.13.5 into the
// user cache when none is found), finds the two Dart repositories (sibling
// checkouts of this repository, or SWITCHBOARD_DART_CORE and
// SWITCHBOARD_DART_QUIC, or the pinned clones `fetch` made), and runs
// `dart pub get` and `dart build cli`. It skips the build when the stamp
// written next to the executable matches the peer's sources and the
// checkouts. switchboard_quic's build hook compiles its native QUIC library
// with cargo, so a Rust toolchain is needed too. `--expect-pins` fails when a
// checkout is not at the commit in dart_commits.txt (CI).
//
// `locate` prints the executable when it is built and current (exit 1 with
// the reason otherwise). `status` prints what `build` would use. `fetch`
// clones the pinned commits into test/interop/dart_peer/.deps/.
//
// SWITCHBOARD_DART_PEER names a prebuilt executable and short-circuits all of
// this. The test suite imports ensurePeer() from its global setup.

import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import {
  existsSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  rmSync,
  statSync,
  writeFileSync,
} from 'node:fs';
import { homedir, tmpdir } from 'node:os';
import { dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

/** The Dart SDK release the peer is built and tested with. */
export const dartVersion = '3.13.5';

/** The repository root. */
export const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');

/** The Dart peer package. */
export const peerDir = join(repoRoot, 'test', 'interop', 'dart_peer');

const buildDir = join(peerDir, 'build', 'peer');
const stampPath = join(buildDir, 'stamp.json');
const overridesPath = join(peerDir, 'pubspec_overrides.yaml');
const overridesMarker = '# Written by tool/dart-peer.mjs';
const depsDir = join(peerDir, '.deps');
const isWindows = process.platform === 'win32';

/** The Dart repositories the peer depends on, by package name. */
export const repositories = /** @type {const} */ ({
  switchboard: { github: 'kaetemi/switchboard', env: 'SWITCHBOARD_DART_CORE' },
  switchboard_quic: { github: 'kaetemi/switchboard_quic', env: 'SWITCHBOARD_DART_QUIC' },
});

/** @typedef {keyof typeof repositories} RepoName */

/**
 * The pinned commits of dart_commits.txt.
 * @returns {Record<RepoName, string>}
 */
export function readPins() {
  /** @type {Record<string, string>} */
  const pins = {};
  for (const raw of readFileSync(join(peerDir, 'dart_commits.txt'), 'utf8').split('\n')) {
    const line = raw.trim();
    if (line === '' || line.startsWith('#')) {
      continue;
    }
    const [name, commit] = line.split(/\s+/);
    if (name !== undefined && commit !== undefined) {
      pins[name] = commit;
    }
  }
  for (const name of Object.keys(repositories)) {
    if (!/^[0-9a-f]{40}$/.test(pins[name] ?? '')) {
      throw new Error(`dart_commits.txt: no full commit for ${name}`);
    }
  }
  return /** @type {Record<RepoName, string>} */ (pins);
}

/**
 * Runs [command] and answers its output, or undefined when it cannot run or
 * fails.
 * @param {string} command
 * @param {string[]} args
 * @param {{ cwd?: string }} [options]
 * @returns {string | undefined}
 */
function output(command, args, options = {}) {
  const r = spawnSync(command, args, {
    cwd: options.cwd,
    encoding: 'utf8',
    shell: isWindows,
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  if (r.error !== undefined || r.status !== 0) {
    return undefined;
  }
  return `${r.stdout}${r.stderr}`.trim();
}

/** @returns {string} */
function cacheDir() {
  const base =
    process.env.XDG_CACHE_HOME ??
    (isWindows ? (process.env.LOCALAPPDATA ?? tmpdir()) : join(homedir(), '.cache'));
  return join(base, 'switchboard_ts');
}

function downloadedSdk() {
  const arch = process.arch === 'arm64' ? 'arm64' : 'x64';
  const os = isWindows ? 'windows' : process.platform === 'darwin' ? 'macos' : 'linux';
  return {
    url: `https://storage.googleapis.com/dart-archive/channels/stable/release/${dartVersion}/sdk/dartsdk-${os}-${arch}-release.zip`,
    dir: join(cacheDir(), `dart-sdk-${dartVersion}-${os}-${arch}`),
  };
}

/**
 * @typedef {{ dart: string, version: string, source: string }} DartSdk
 * @typedef {{ reason: string }} Missing
 */

/**
 * The Dart executable to build with.
 * @param {{ download?: boolean, log?: (line: string) => void }} [options]
 * @returns {DartSdk | Missing}
 */
export function findDart(options = {}) {
  const exe = isWindows ? 'dart.exe' : 'dart';
  /** @type {[string, string][]} */
  const candidates = [['PATH', 'dart']];
  for (const env of ['SWITCHBOARD_DART_SDK', 'DART_SDK']) {
    const dir = process.env[env];
    if (dir !== undefined && dir !== '') {
      candidates.push([env, join(dir, 'bin', exe)]);
    }
  }
  const cached = downloadedSdk();
  candidates.push(['download cache', join(cached.dir, 'dart-sdk', 'bin', exe)]);
  for (const [source, dart] of candidates) {
    const version = output(dart, ['--version']);
    const match = version?.match(/Dart SDK version: (\d+)\.(\d+)\.(\d+)/);
    if (match === undefined || match === null) {
      continue;
    }
    const [major, minor] = [Number(match[1]), Number(match[2])];
    if (major < 3 || (major === 3 && minor < 13)) {
      options.log?.(`dart-peer: ignoring Dart ${match[0]} from ${source}, 3.13 or later needed`);
      continue;
    }
    return { dart, version: `${match[1]}.${match[2]}.${match[3]}`, source };
  }
  if (options.download) {
    return downloadDart(options.log);
  }
  return {
    reason:
      `no Dart SDK 3.13 or later (PATH, SWITCHBOARD_DART_SDK, DART_SDK); ` +
      `install Dart ${dartVersion} or run \`node tool/dart-peer.mjs build --download-sdk\``,
  };
}

/**
 * Downloads and unpacks the Dart SDK into the user cache.
 * @param {((line: string) => void) | undefined} log
 * @returns {DartSdk | Missing}
 */
function downloadDart(log) {
  const { url, dir } = downloadedSdk();
  const zip = `${dir}.zip`;
  mkdirSync(dirname(zip), { recursive: true });
  log?.(`dart-peer: downloading ${url}`);
  const curl = spawnSync('curl', ['-fsSL', '-o', zip, url], { stdio: 'inherit' });
  if (curl.error !== undefined || curl.status !== 0) {
    return { reason: `could not download the Dart SDK from ${url} (curl failed)` };
  }
  rmSync(dir, { recursive: true, force: true });
  mkdirSync(dir, { recursive: true });
  const unpack =
    process.platform === 'linux'
      ? spawnSync('unzip', ['-q', zip, '-d', dir], { stdio: 'inherit' })
      : spawnSync('tar', ['-xf', zip, '-C', dir], { stdio: 'inherit' });
  rmSync(zip, { force: true });
  if (unpack.error !== undefined || unpack.status !== 0) {
    return { reason: `could not unpack the Dart SDK into ${dir}` };
  }
  return findDart({ log });
}

/**
 * @typedef {{ path: string, head: string | undefined, fingerprint: string }} Checkout
 * @typedef {{ repos: Record<RepoName, Checkout>, overrides: boolean }} Deps
 */

/**
 * A checkout's commit and a fingerprint that changes with uncommitted edits.
 * @param {string} path
 * @returns {Checkout}
 */
function checkout(path) {
  const head = output('git', ['-C', path, 'rev-parse', 'HEAD']);
  const status = output('git', ['-C', path, 'status', '--porcelain']) ?? '';
  const diff = status === '' ? '' : (output('git', ['-C', path, 'diff', 'HEAD']) ?? '');
  const fingerprint = createHash('sha256').update(`${head}\n${status}\n${diff}`).digest('hex');
  return { path, head, fingerprint };
}

/**
 * Where the two Dart repositories are: environment, sibling checkouts, or
 * the pinned clones of `fetch`. Writes or removes pubspec_overrides.yaml to
 * match.
 * @returns {Deps | Missing}
 */
export function resolveDeps() {
  /** @type {Partial<Record<RepoName, string>>} */
  const paths = {};
  let overrides = false;
  for (const [name, repo] of Object.entries(repositories)) {
    const fromEnv = process.env[repo.env];
    const sibling = resolve(repoRoot, '..', name);
    const fetched = join(depsDir, name);
    const isPackage = (/** @type {string} */ p) => existsSync(join(p, 'pubspec.yaml'));
    let path;
    if (fromEnv !== undefined && fromEnv !== '') {
      path = resolve(fromEnv);
      overrides = true;
    } else if (isPackage(sibling)) {
      path = sibling;
    } else if (isPackage(fetched)) {
      path = fetched;
      overrides = true;
    } else {
      return {
        reason:
          `no checkout of ${repo.github}: clone it next to this repository, set ${repo.env}, ` +
          'or run `node tool/dart-peer.mjs fetch`',
      };
    }
    if (!isPackage(path)) {
      return { reason: `${path} (${repo.env}) is not a Dart package` };
    }
    paths[/** @type {RepoName} */ (name)] = path;
  }
  const resolved = /** @type {Record<RepoName, string>} */ (paths);
  writeOverrides(overrides ? resolved : undefined);
  return {
    repos: {
      switchboard: checkout(resolved.switchboard),
      switchboard_quic: checkout(resolved.switchboard_quic),
    },
    overrides,
  };
}

/** @param {Record<RepoName, string> | undefined} paths */
function writeOverrides(paths) {
  const existing = existsSync(overridesPath) ? readFileSync(overridesPath, 'utf8') : undefined;
  if (paths === undefined) {
    if (existing?.startsWith(overridesMarker)) {
      rmSync(overridesPath);
    }
    return;
  }
  if (existing !== undefined && !existing.startsWith(overridesMarker)) {
    // A developer's own overrides win.
    return;
  }
  const lines = [overridesMarker, 'dependency_overrides:'];
  for (const [name, path] of Object.entries(paths)) {
    lines.push(`  ${name}:`, `    path: ${JSON.stringify(path)}`);
  }
  const text = `${lines.join('\n')}\n`;
  if (text !== existing) {
    writeFileSync(overridesPath, text);
  }
}

/** The peer executable inside the bundle `dart build cli` writes. */
export function peerExecutable() {
  return join(buildDir, 'bundle', 'bin', isWindows ? 'dart_peer.exe' : 'dart_peer');
}

/** A hash of every file the peer is built from. */
function sourceHash() {
  const hash = createHash('sha256');
  /** @param {string} dir */
  const walk = (dir) => {
    for (const entry of readdirSync(dir).sort()) {
      const path = join(dir, entry);
      if (statSync(path).isDirectory()) {
        walk(path);
      } else if (entry.endsWith('.dart')) {
        hash.update(`${relative(peerDir, path)}\n`).update(readFileSync(path));
      }
    }
  };
  for (const file of ['pubspec.yaml', 'pubspec_overrides.yaml', 'dart_commits.txt']) {
    const path = join(peerDir, file);
    if (existsSync(path)) {
      hash.update(`${file}\n`).update(readFileSync(path));
    }
  }
  walk(join(peerDir, 'bin'));
  walk(join(peerDir, 'lib'));
  return hash.digest('hex');
}

/**
 * The stamp the current sources and checkouts call for.
 * @param {Deps} deps
 * @param {DartSdk | undefined} sdk
 */
function wantedStamp(deps, sdk) {
  return {
    sources: sourceHash(),
    switchboard: deps.repos.switchboard.fingerprint,
    switchboard_quic: deps.repos.switchboard_quic.fingerprint,
    ...(sdk === undefined ? {} : { dart: sdk.version }),
  };
}

/**
 * Whether the built peer matches the stamp the current state calls for.
 * @param {Deps} deps
 * @param {DartSdk | undefined} sdk
 * @returns {{ fresh: true } | { fresh: false, reason: string }}
 */
function freshness(deps, sdk) {
  if (!existsSync(peerExecutable()) || !existsSync(stampPath)) {
    return { fresh: false, reason: 'the Dart peer is not built' };
  }
  /** @type {Record<string, string>} */
  const stamp = JSON.parse(readFileSync(stampPath, 'utf8'));
  const wanted = wantedStamp(deps, sdk);
  for (const [key, value] of Object.entries(wanted)) {
    if (stamp[key] !== value) {
      return { fresh: false, reason: `the Dart peer is out of date (${key} changed)` };
    }
  }
  return { fresh: true };
}

/**
 * Differences between the checkouts and the pins, as warnings.
 * @param {Deps} deps
 * @returns {string[]}
 */
export function pinWarnings(deps) {
  const pins = readPins();
  const warnings = [];
  for (const name of /** @type {RepoName[]} */ (Object.keys(repositories))) {
    const c = deps.repos[name];
    if (c.head !== pins[name]) {
      warnings.push(`${name} at ${c.path} is at ${c.head ?? 'no commit'}, pinned ${pins[name]}`);
    }
  }
  const corePin = join(deps.repos.switchboard_quic.path, 'tool', 'core_commit.txt');
  if (existsSync(corePin)) {
    const core = readFileSync(corePin, 'utf8')
      .split('\n')
      .map((l) => l.trim())
      .find((l) => l !== '' && !l.startsWith('#'));
    if (core !== pins.switchboard) {
      warnings.push(`switchboard_quic is tested against core ${core}, pinned ${pins.switchboard}`);
    }
  }
  return warnings;
}

/**
 * @typedef {{ path: string, built: boolean, warnings: string[] }} Peer
 */

/**
 * The peer executable: SWITCHBOARD_DART_PEER, else the built one, building
 * it first when [build] is set and it is missing or stale.
 * @param {{ build?: boolean, force?: boolean, download?: boolean,
 *   expectPins?: boolean, log?: (line: string) => void }} [options]
 * @returns {Peer | Missing}
 */
export function ensurePeer(options = {}) {
  const log = options.log ?? ((/** @type {string} */ line) => console.error(line));
  const prebuilt = process.env.SWITCHBOARD_DART_PEER;
  if (prebuilt !== undefined && prebuilt !== '') {
    return existsSync(prebuilt)
      ? { path: resolve(prebuilt), built: false, warnings: [] }
      : { reason: `SWITCHBOARD_DART_PEER names ${prebuilt}, which does not exist` };
  }
  const deps = resolveDeps();
  if ('reason' in deps) {
    return deps;
  }
  const warnings = pinWarnings(deps);
  if (options.expectPins && warnings.length > 0) {
    return { reason: `checkouts not at the pinned commits: ${warnings.join('; ')}` };
  }
  if (!options.build) {
    const state = freshness(deps, undefined);
    return state.fresh ? { path: peerExecutable(), built: false, warnings } : state;
  }
  const sdk = findDart({ download: options.download, log });
  if ('reason' in sdk) {
    return sdk;
  }
  if (!options.force && freshness(deps, sdk).fresh) {
    return { path: peerExecutable(), built: false, warnings };
  }
  if (output('cargo', ['--version']) === undefined) {
    return {
      reason: "no cargo on PATH: switchboard_quic's build hook compiles its QUIC library with Rust",
    };
  }
  for (const w of warnings) {
    log(`dart-peer: warning: ${w}`);
  }
  log(`dart-peer: building the Dart peer with Dart ${sdk.version} (${sdk.source})`);
  /** @param {string[]} args */
  const dart = (args) =>
    spawnSync(sdk.dart, args, {
      cwd: peerDir,
      stdio: ['ignore', 'inherit', 'inherit'],
      shell: isWindows,
    });
  for (const args of [
    ['pub', 'get'],
    ['build', 'cli', '-t', 'bin/dart_peer.dart', '-o', join('build', 'peer')],
  ]) {
    const r = dart(args);
    if (r.error !== undefined || r.status !== 0) {
      return { reason: `\`dart ${args.join(' ')}\` failed in ${peerDir}` };
    }
  }
  if (!existsSync(peerExecutable())) {
    return { reason: `dart build cli did not produce ${peerExecutable()}` };
  }
  writeFileSync(stampPath, `${JSON.stringify(wantedStamp(deps, sdk), null, 2)}\n`);
  return { path: peerExecutable(), built: true, warnings };
}

/** Clones the pinned commits into .deps/ (shallow). */
function fetchPins() {
  const pins = readPins();
  for (const [name, repo] of Object.entries(repositories)) {
    const dir = join(depsDir, name);
    const commit = pins[/** @type {RepoName} */ (name)];
    mkdirSync(dir, { recursive: true });
    const git = (/** @type {string[]} */ args) =>
      spawnSync('git', ['-C', dir, ...args], { stdio: 'inherit' }).status === 0;
    const ok =
      (existsSync(join(dir, '.git')) || git(['init', '-q'])) &&
      git(['fetch', '-q', '--depth', '1', `https://github.com/${repo.github}.git`, commit]) &&
      git(['checkout', '-q', '--detach', 'FETCH_HEAD']);
    if (!ok) {
      throw new Error(`could not fetch ${repo.github} at ${commit}`);
    }
    console.error(`dart-peer: ${name} at ${commit} in ${dir}`);
  }
}

function main() {
  const [command = 'build', ...rest] = process.argv.slice(2);
  const flags = new Set(rest);
  if (command === 'fetch') {
    fetchPins();
    return 0;
  }
  if (command === 'status') {
    const sdk = findDart();
    const deps = resolveDeps();
    console.log(`dart: ${'reason' in sdk ? sdk.reason : `${sdk.version} (${sdk.dart})`}`);
    if ('reason' in deps) {
      console.log(`deps: ${deps.reason}`);
    } else {
      for (const [name, c] of Object.entries(deps.repos)) {
        console.log(`${name}: ${c.path} at ${c.head ?? 'no commit'}`);
      }
      for (const w of pinWarnings(deps)) {
        console.log(`warning: ${w}`);
      }
      const state = freshness(deps, 'reason' in sdk ? undefined : sdk);
      console.log(`peer: ${state.fresh ? peerExecutable() : state.reason}`);
    }
    return 0;
  }
  if (command !== 'build' && command !== 'locate') {
    console.error(`usage: node tool/dart-peer.mjs build|locate|status|fetch`);
    return 64;
  }
  const result = ensurePeer({
    build: command === 'build',
    force: flags.has('--force'),
    download: flags.has('--download-sdk'),
    expectPins: flags.has('--expect-pins'),
  });
  if (flags.has('--json')) {
    console.log(JSON.stringify(result));
  } else if ('path' in result) {
    console.log(result.path);
  } else {
    console.error(`dart-peer: ${result.reason}`);
  }
  return 'path' in result ? 0 : 1;
}

if (process.argv[1] !== undefined && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  process.exitCode = main();
}
