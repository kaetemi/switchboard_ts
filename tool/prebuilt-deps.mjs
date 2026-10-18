/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// The prebuilt QUIC addon packages (@kaetemi/switchboard-quic-<target>, one
// manifest per directory under native/npm/) as optionalDependencies of the
// package, pinned to its version.
//
// They are not written in the repository's package.json: npm 11 refuses
// `npm ci` with a lock file that cannot resolve them, and they resolve only
// once published. The release workflow's pack step adds them to the
// package.json it packs (`--apply`), so the published package pulls the
// addon for its platform while the repository installs cleanly on every npm.
//
//   node tool/prebuilt-deps.mjs            print the map as JSON
//   node tool/prebuilt-deps.mjs --apply    write it into package.json

import { readdirSync, readFileSync, writeFileSync } from 'node:fs';

const root = new URL('../', import.meta.url);
const manifest = JSON.parse(readFileSync(new URL('package.json', root), 'utf8'));

/** @returns {Record<string, string>} package name to version, sorted by name */
export function prebuiltDependencies() {
  const entries = readdirSync(new URL('native/npm/', root), { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => {
      const target = JSON.parse(
        readFileSync(new URL(`native/npm/${entry.name}/package.json`, root), 'utf8'),
      );
      return [target.name, target.version];
    })
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  return Object.fromEntries(entries);
}

const mode = process.argv[2];
if (mode === '--apply') {
  if (manifest.optionalDependencies !== undefined) {
    throw new Error('package.json already has optionalDependencies');
  }
  const dependencies = prebuiltDependencies();
  for (const [name, version] of Object.entries(dependencies)) {
    if (version !== manifest.version) {
      throw new Error(`${name} is ${version}, the package is ${manifest.version}`);
    }
  }
  // Placed after peerDependencies, where package.json keeps dependency maps.
  const out = {};
  for (const [key, value] of Object.entries(manifest)) {
    out[key] = value;
    if (key === 'peerDependenciesMeta') out.optionalDependencies = dependencies;
  }
  if (out.optionalDependencies === undefined) out.optionalDependencies = dependencies;
  writeFileSync(new URL('package.json', root), `${JSON.stringify(out, null, 2)}\n`);
  console.log(`package.json: ${Object.keys(dependencies).length} optional dependencies added`);
} else if (mode === undefined) {
  console.log(JSON.stringify(prebuiltDependencies(), null, 2));
} else {
  console.error(`usage: node tool/prebuilt-deps.mjs [--apply]`);
  process.exit(2);
}
