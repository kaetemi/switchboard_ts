/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Copies `native/sbq` of a kaetemi/switchboard_quic checkout, at the commit
// pinned in `native/sbq-vendor.commit`, into `native/sbq-vendor/`, changing
// one line: "rlib" is added to the crate-type, so that the N-API addon can
// call the shim Rust to Rust. Nothing else in the copy differs from upstream,
// so that it can move to a repository of its own (or give way to a git
// dependency once the line lands upstream).
//
//   node tool/sync-sbq.mjs [--check] [--commit <rev>] [<switchboard_quic checkout>]
//
// The checkout defaults to $SWITCHBOARD_QUIC, else ../switchboard_quic next to
// this repository. It only has to contain the pinned commit; its working tree
// is not read. --commit <rev> (sync only) moves the pin to <rev> first.
// --check copies nothing: it exits with 1 when the vendored tree differs from
// upstream at the pin by anything but that line, and lists the differences.
// Exit code 2: a usage or environment error.

import { execFileSync } from 'node:child_process';
import {
  chmodSync,
  existsSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  rmSync,
  statSync,
  writeFileSync,
} from 'node:fs';
import { dirname, join, relative, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const vendorDir = join(root, 'native', 'sbq-vendor');
const pinFile = join(root, 'native', 'sbq-vendor.commit');
const upstreamDir = 'native/sbq';
// Never part of the copy: build output and release artifacts (untracked upstream anyway).
const excluded = ['target', 'prebuilt'];
const upstreamLine = 'crate-type = ["cdylib", "staticlib"]';
const vendoredLine = 'crate-type = ["cdylib", "staticlib", "rlib"]';

function fail(message) {
  console.error(`sync-sbq: ${message}`);
  process.exit(2);
}

function parseArgs(argv) {
  const options = { check: false, commit: undefined, checkout: undefined };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--check') {
      options.check = true;
    } else if (a === '--commit') {
      options.commit = argv[++i];
      if (options.commit === undefined) fail('--commit needs a revision');
    } else if (a === '--help' || a === '-h') {
      console.log(
        'usage: node tool/sync-sbq.mjs [--check] [--commit <rev>] [<switchboard_quic checkout>]',
      );
      process.exit(0);
    } else if (a.startsWith('-')) {
      fail(`unknown option ${a}`);
    } else if (options.checkout === undefined) {
      options.checkout = a;
    } else {
      fail(`unexpected argument ${a}`);
    }
  }
  if (options.check && options.commit !== undefined) fail('--check takes no --commit');
  return options;
}

function git(checkout, args, encoding = 'utf8') {
  return execFileSync('git', ['-C', checkout, ...args], {
    encoding,
    maxBuffer: 256 << 20,
    stdio: ['ignore', 'pipe', 'pipe'],
  });
}

function readPin() {
  if (!existsSync(pinFile)) fail(`${relative(root, pinFile)} is missing`);
  const line = readFileSync(pinFile, 'utf8')
    .split(/\r?\n/)
    .map((l) => l.trim())
    .find((l) => l !== '' && !l.startsWith('#'));
  if (line === undefined || !/^[0-9a-f]{40}$/.test(line)) {
    fail(`${relative(root, pinFile)} holds no full commit hash`);
  }
  return line;
}

function writePin(commit) {
  const text = readFileSync(pinFile, 'utf8');
  const lines = text.split(/\r?\n/);
  const i = lines.findIndex((l) => /^[0-9a-f]{40}$/.test(l.trim()));
  lines[i] = commit;
  writeFileSync(pinFile, lines.join('\n'));
}

function resolveCommit(checkout, rev) {
  try {
    return git(checkout, ['rev-parse', '--verify', '--quiet', `${rev}^{commit}`]).trim();
  } catch {
    fail(
      `${checkout} has no commit ${rev}; fetch it first (git -C ${checkout} fetch origin ${rev})`,
    );
  }
}

/** The files of native/sbq at [commit], as they belong in the copy: path (posix) to bytes. */
function upstreamFiles(checkout, commit) {
  const listing = git(checkout, ['ls-tree', '-r', '-z', commit, '--', upstreamDir]);
  const files = new Map();
  const modes = new Map();
  for (const entry of listing.split('\0')) {
    if (entry === '') continue;
    const tab = entry.indexOf('\t');
    const [mode, type, object] = entry.slice(0, tab).split(' ');
    const path = entry.slice(tab + 1).slice(upstreamDir.length + 1);
    if (excluded.includes(path.split('/')[0])) continue;
    if (type !== 'blob' || mode === '120000') {
      fail(`${upstreamDir}/${path} is a ${type} (${mode}); only plain files are copied`);
    }
    files.set(path, git(checkout, ['cat-file', 'blob', object], 'buffer'));
    modes.set(path, mode);
  }
  if (!files.has('Cargo.toml')) fail(`${upstreamDir}/Cargo.toml is missing at ${commit}`);
  const toml = files.get('Cargo.toml').toString('utf8');
  const lines = toml.split('\n');
  const at = lines.findIndex((l) => l.replace(/\r$/, '') === upstreamLine);
  if (at >= 0) {
    lines[at] = lines[at].replace(upstreamLine, vendoredLine);
    files.set('Cargo.toml', Buffer.from(lines.join('\n'), 'utf8'));
  } else if (!lines.some((l) => l.replace(/\r$/, '') === vendoredLine)) {
    fail(
      `${upstreamDir}/Cargo.toml at ${commit} has no line \`${upstreamLine}\`; update this script`,
    );
  } else {
    console.log(
      'sync-sbq: upstream already builds an rlib; the copy could become a git dependency',
    );
  }
  return { files, modes };
}

/** The files of the vendored tree: path (posix) to bytes. */
function vendoredFiles() {
  const files = new Map();
  if (!existsSync(vendorDir)) return files;
  const walk = (dir) => {
    for (const name of readdirSync(dir)) {
      const full = join(dir, name);
      const path = relative(vendorDir, full).split(sep).join('/');
      if (dir === vendorDir && excluded.includes(name)) continue;
      if (statSync(full).isDirectory()) {
        walk(full);
      } else {
        files.set(path, readFileSync(full));
      }
    }
  };
  walk(vendorDir);
  return files;
}

// A checkout with CRLF line endings (core.autocrlf on Windows) is the same tree.
const normalize = (bytes) => bytes.toString('latin1').replaceAll('\r\n', '\n');

function check(expected) {
  const actual = vendoredFiles();
  const differences = [];
  for (const [path, bytes] of expected) {
    const have = actual.get(path);
    if (have === undefined) {
      differences.push(`missing  ${path}`);
    } else if (normalize(have) !== normalize(bytes)) {
      differences.push(`changed  ${path}`);
    }
  }
  for (const path of actual.keys()) {
    if (!expected.has(path)) differences.push(`extra    ${path}`);
  }
  return differences;
}

function sync(expected, modes) {
  mkdirSync(vendorDir, { recursive: true });
  for (const name of readdirSync(vendorDir)) {
    if (!excluded.includes(name)) rmSync(join(vendorDir, name), { recursive: true, force: true });
  }
  for (const [path, bytes] of expected) {
    const full = join(vendorDir, ...path.split('/'));
    mkdirSync(dirname(full), { recursive: true });
    writeFileSync(full, bytes);
    if (modes.get(path) === '100755') chmodSync(full, 0o755);
  }
}

const options = parseArgs(process.argv.slice(2));
const checkout = resolve(
  options.checkout ?? process.env.SWITCHBOARD_QUIC ?? join(root, '..', 'switchboard_quic'),
);
if (!existsSync(checkout)) fail(`no switchboard_quic checkout at ${checkout}`);
const commit = resolveCommit(checkout, options.commit ?? readPin());
const { files, modes } = upstreamFiles(checkout, commit);
const where = `${relative(root, vendorDir)} against ${upstreamDir} at ${commit}`;

if (options.check) {
  const differences = check(files);
  if (differences.length > 0) {
    console.error(`sync-sbq: ${where} differs (beyond the crate-type line):`);
    for (const d of differences) console.error(`  ${d}`);
    console.error('Run `node tool/sync-sbq.mjs` to copy it again.');
    process.exit(1);
  }
  console.log(`sync-sbq: ${where}: ${files.size} files, identical but for the crate-type line`);
} else {
  if (options.commit !== undefined) writePin(commit);
  sync(files, modes);
  console.log(`sync-sbq: copied ${files.size} files to ${where}`);
}
