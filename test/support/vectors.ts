/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// The wiki's test vectors (test/vectors/wiki-vectors.json, extracted by
// tool/extract-wiki-vectors.mjs) by id, for the tests that assert them.
//
// Every id a test asserts goes through one of the functions below as a
// string literal, `wikiBytes('test-vectors/names/api')`, so that
// test/vectors/coverage.test.ts can find it without running the test. A
// vector a test deliberately leaves out is named with skippedWikiVector and
// a reason. A loop over vectors uses wikiVectorsIn with a literal prefix, or
// wikiVectors with literal ids.

import { readFileSync } from 'node:fs';
import { test } from 'vitest';
import { hexBytes } from '../../src/core/bytes.ts';

/** One vector of the wiki, as extracted. */
export interface WikiVector {
  /** `page/section/case`, stable while the wiki's case name is. */
  readonly id: string;
  /** `test-vectors` or `quic-binding`. */
  readonly page: string;
  /** The heading it is under, `Mux frames` or `Test vectors / Records`. */
  readonly section: string;
  /**
   * The table it is a row of (`mux-frames#2`), or for a vector in a
   * paragraph the table above it; null when the section has none above.
   */
  readonly table: string | null;
  /** The case name, the row's first cell or the paragraph's clause. */
  readonly case: string;
  /** The bytes as hex, or null when the wiki gives several or describes them. */
  readonly hex: string | null;
  /** Whether a decoder must refuse it. */
  readonly negative: boolean;
  /** Whether the wiki gives more than one hex literal (see {@link text}). */
  readonly composite: boolean;
  /** A vector from a paragraph: often a fragment of a message, see {@link text}. */
  readonly prose?: boolean;
  /** Several values for one case: `Kind 0, kind 4`. */
  readonly variants?: readonly string[];
  /** Every cell of a table with more than two columns, by header. */
  readonly columns?: Readonly<Record<string, string>>;
  /** The cell or paragraph the vector came from. */
  readonly text: string;
}

/** The extracted file. */
export interface WikiVectorFile {
  readonly source: { readonly repository: string; readonly path: string; readonly commit: string };
  readonly vectors: readonly WikiVector[];
  readonly blobs: Readonly<Record<string, string>>;
}

/** Path of the extracted vectors. */
export const wikiVectorsPath = new URL('../vectors/wiki-vectors.json', import.meta.url);

/** The extracted vectors. */
export const wikiVectorFile: WikiVectorFile = JSON.parse(
  readFileSync(wikiVectorsPath, 'utf8'),
) as WikiVectorFile;

const byId = new Map(wikiVectorFile.vectors.map((v) => [v.id, v]));

/** The vector [id]. @throws {Error} for an id the wiki does not have. */
export function wikiVector(id: string): WikiVector {
  const v = byId.get(id);
  if (v === undefined) {
    throw new Error(`no wiki vector ${id} (test/vectors/wiki-vectors.json)`);
  }
  return v;
}

/** The vectors [ids], in order. @throws {Error} for an unknown id. */
export function wikiVectors(...ids: string[]): WikiVector[] {
  return ids.map(wikiVector);
}

/** Every vector whose id starts with [prefix]. @throws {Error} when there is none. */
export function wikiVectorsIn(prefix: string): WikiVector[] {
  const found = wikiVectorFile.vectors.filter((v) => v.id.startsWith(prefix));
  if (found.length === 0) {
    throw new Error(`no wiki vector id starts with ${prefix}`);
  }
  return found;
}

/**
 * The bytes of vector [id].
 * @throws {Error} for an unknown id, or one without a single value (see
 * {@link wikiVariants}).
 */
export function wikiBytes(id: string): Uint8Array {
  const v = wikiVector(id);
  if (v.hex === null) {
    throw new Error(`wiki vector ${id} has no single value: ${v.text}`);
  }
  return hexBytes(v.hex);
}

/** Every value of vector [id]: its variants, or its one value. */
export function wikiVariants(id: string): Uint8Array[] {
  const v = wikiVector(id);
  if (v.variants !== undefined) {
    return v.variants.map(hexBytes);
  }
  return v.hex === null ? [] : [hexBytes(v.hex)];
}

/** The bytes of blob [name] (`C1`, `C2.holder-key`, `quic.X`). */
export function wikiBlob(name: string): Uint8Array {
  const hex = wikiVectorFile.blobs[name];
  if (hex === undefined) {
    throw new Error(`no wiki blob ${name}`);
  }
  return hexBytes(hex);
}

/**
 * Marks vector [id] as deliberately not asserted, for [reason]: a skipped
 * test in the report, and covered for the coverage test. Call it where tests
 * are defined (in a file or a describe block).
 */
export function skippedWikiVector(id: string, reason: string): void {
  wikiVector(id);
  test.skip(`wiki vector ${id}: ${reason}`, () => {});
}
