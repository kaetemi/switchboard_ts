/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Every wiki vector is asserted by some test, and every id a test names is a
// wiki vector. Tests name vectors through test/support/vectors.ts with string
// literals; this file finds them by reading the test sources, so a vector
// counts once a test file names it, whichever project runs that file.
//
// A wiki table (with the paragraphs below it) is checked once any test names
// one of its vectors: from then on, each of its vectors must be asserted or
// skipped with a reason. Tables no test has started yet are listed as todo.
// SWITCHBOARD_VECTORS_STRICT=1 checks every table (the goal once every codec
// is ported).

import { readdirSync, readFileSync, statSync } from 'node:fs';
import { join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, test } from 'vitest';
import { type WikiVector, wikiVectorFile } from '../support/vectors.ts';

const root = fileURLToPath(new URL('../..', import.meta.url));
const { SWITCHBOARD_VECTORS_STRICT } = process.env;
const strict = SWITCHBOARD_VECTORS_STRICT === '1';
const skipFiles = new Set(['test/support/vectors.ts', 'test/vectors/coverage.test.ts']);

function sources(dir: string, out: string[] = []): string[] {
  for (const entry of readdirSync(dir)) {
    if (entry === 'node_modules' || entry.startsWith('.')) {
      continue;
    }
    const path = join(dir, entry);
    if (statSync(path).isDirectory()) {
      sources(path, out);
    } else if (entry.endsWith('.ts')) {
      out.push(path);
    }
  }
  return out;
}

/** Ids and prefixes the test sources name, with the files naming them. */
function references(): { ids: Map<string, string[]>; prefixes: Map<string, string[]> } {
  const ids = new Map<string, string[]>();
  const prefixes = new Map<string, string[]>();
  const call =
    /\b(wikiVector|wikiVectors|wikiBytes|wikiVariants|skippedWikiVector|wikiVectorsIn)\(([^)]*)\)/g;
  const literal = /(['"`])((?:(?!\1)[^\\]|\\.)*)\1/g;
  const add = (map: Map<string, string[]>, key: string, file: string) => {
    const files = map.get(key) ?? [];
    if (!files.includes(file)) {
      files.push(file);
    }
    map.set(key, files);
  };
  for (const path of [...sources(join(root, 'src')), ...sources(join(root, 'test'))]) {
    const file = relative(root, path).replaceAll('\\', '/');
    if (skipFiles.has(file)) {
      continue;
    }
    for (const m of readFileSync(path, 'utf8').matchAll(call)) {
      const fn = m[1];
      const args = [...(m[2] ?? '').matchAll(literal)]
        .map((l) => l[2] ?? '')
        .filter((s) => !s.includes('${'));
      for (const value of fn === 'wikiVectors' ? args : args.slice(0, 1)) {
        add(fn === 'wikiVectorsIn' ? prefixes : ids, value, file);
      }
    }
  }
  return { ids, prefixes };
}

const { ids, prefixes } = references();
const known = new Set(wikiVectorFile.vectors.map((v) => v.id));

function covered(v: WikiVector): boolean {
  if (ids.has(v.id)) {
    return true;
  }
  for (const p of prefixes.keys()) {
    if (v.id.startsWith(p)) {
      return true;
    }
  }
  return false;
}

describe('wiki vectors', () => {
  test('the file records the wiki commit; ids are unique, hex well formed', () => {
    expect(wikiVectorFile.source.commit).toMatch(/^[0-9a-f]{40}$/);
    expect(known.size).toBe(wikiVectorFile.vectors.length);
    const hex = /^([0-9A-F]{2}( [0-9A-F]{2})*)?$/;
    for (const v of wikiVectorFile.vectors) {
      for (const h of [v.hex ?? '', ...(v.variants ?? [])]) {
        expect(h, v.id).toMatch(hex);
      }
    }
  });

  test('every id a test names is a wiki vector', () => {
    const unknown = [...ids].filter(([id]) => !known.has(id)).map(([id, f]) => `${id} (${f})`);
    expect(unknown).toEqual([]);
  });

  test('every prefix a test names matches a wiki vector', () => {
    const unmatched = [...prefixes]
      .filter(([p]) => ![...known].some((id) => id.startsWith(p)))
      .map(([p, f]) => `${p} (${f})`);
    expect(unmatched).toEqual([]);
  });

  const tables = new Map<string, WikiVector[]>();
  for (const v of wikiVectorFile.vectors) {
    const key = `${v.page}: ${v.table ?? v.section}`;
    tables.set(key, [...(tables.get(key) ?? []), v]);
  }
  for (const [table, vectors] of tables) {
    const started = vectors.some(covered);
    if (!started && !strict) {
      test.todo(`${table}: ${vectors.length} vectors, no test names one yet`);
      continue;
    }
    test(`${table}: every vector is asserted or skipped with a reason`, () => {
      expect(vectors.filter((v) => !covered(v)).map((v) => v.id)).toEqual([]);
    });
  }
});
