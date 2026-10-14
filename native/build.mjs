/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Builds the N-API addon (`npm run build:native`): `cargo build --release --locked` in native/,
// then copies the library cargo wrote to native/sbq-node.node, the file the native provider
// loads first (src/node/quic/native-provider.ts).
//
//   node native/build.mjs [--debug] [--target <triple>] [--out <file>]
//
// --debug builds the dev profile (faster to build, much slower to run); --target cross-compiles
// (the toolchain for it must be installed); --out writes the addon elsewhere.

import { spawnSync } from 'node:child_process';
import { copyFileSync, existsSync, mkdirSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const native = dirname(fileURLToPath(import.meta.url));
const args = process.argv.slice(2);
let profile = 'release';
let target;
let out = join(native, 'sbq-node.node');
for (let i = 0; i < args.length; i++) {
  if (args[i] === '--debug') {
    profile = 'debug';
  } else if (args[i] === '--target' && args[i + 1] !== undefined) {
    target = args[++i];
  } else if (args[i] === '--out' && args[i + 1] !== undefined) {
    out = resolve(args[++i]);
  } else {
    console.error('usage: node native/build.mjs [--debug] [--target <triple>] [--out <file>]');
    process.exit(2);
  }
}

const cargoArgs = ['build', '--locked'];
if (profile === 'release') cargoArgs.push('--release');
if (target !== undefined) cargoArgs.push('--target', target);
const cargo = spawnSync(process.env.CARGO ?? 'cargo', cargoArgs, { cwd: native, stdio: 'inherit' });
if (cargo.error !== undefined) {
  console.error(`build-native: cannot run cargo (${cargo.error.message}); install Rust: https://rustup.rs`);
  process.exit(1);
}
if (cargo.status !== 0) process.exit(cargo.status ?? 1);

const os = target ?? `${process.platform}`;
const file = /windows|win32/.test(os)
  ? 'sbq_node.dll'
  : /apple|darwin/.test(os)
    ? 'libsbq_node.dylib'
    : 'libsbq_node.so';
const targetDir = process.env.CARGO_TARGET_DIR ?? join(native, 'target');
const built = join(resolve(native, targetDir), ...(target === undefined ? [] : [target]), profile, file);
if (!existsSync(built)) {
  console.error(`build-native: cargo succeeded but ${built} is missing`);
  process.exit(1);
}
mkdirSync(dirname(out), { recursive: true });
copyFileSync(built, out);
console.log(`build-native: ${out}`);
