#!/usr/bin/env node
/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// @ts-check

// Extracts the byte vectors of the Switchboard wiki into
// test/vectors/wiki-vectors.json: the page "Test vectors" and the section
// "Test vectors" of the page "QUIC and WebTransport binding".
//
//   node tool/extract-wiki-vectors.mjs <sway checkout>/wiki/switchboard [commit]
//
// The commit defaults to the last one that touched the directory. The JSON
// keeps the wiki's words: every vector carries its case name and the text it
// came from, and nothing is decoded; what a vector means stays with the test
// that asserts it by id (test/support/vectors.ts).
//
// What becomes a vector:
//
// - Every row of a table whose bytes column (`Bytes`, `Change`, `On the
//   wire`, `8-byte name`) holds backticked hex or `(empty)`, or describes a
//   change to the table's base blob ("offsets into C1"). A cell that is not
//   one hex literal is `composite`; its `hex` is resolved when the cell is
//   one of the forms the wiki uses (`X` followed by 32 bytes `11`; `X`, the 86
//   bytes of C1, `Y`; the endpoint defined above the table; byte N = `XX`;
//   `00` appended; last byte removed) and null otherwise. A cell of several
//   hex literals (`52 45 …`, `43 4F …`) or several byte values has
//   `variants` instead, and `hex` null.
// - Every backticked hex group in a paragraph that states a decoding rule
//   (protocol error, MUST, decodes as, is invalid, ignored, reads as): a
//   `prose` vector, often a fragment, whose `text` is the paragraph and whose
//   `table` is the table above it in the same section.
// - Every fenced block of hex rows: a named blob (C1, C2 and its keys, P,
//   quic.X, quic.X.signature).
//
// `negative` comes from the prose before a table ("Negative vectors", "each
// MUST be a protocol error") or the paragraph of a prose vector.

import { execFileSync } from 'node:child_process';
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const hexPattern = /^(?:[0-9A-Fa-f]{2})(?: [0-9A-Fa-f]{2})*$/;
const bytesColumn = /^(Bytes|Change|On the wire|8-byte name)$/;
const negativeProse = /negative|protocol error|must be|refused/i;
const ruleProse = /protocol error|MUST|decodes as|is invalid|ignored|reads as/;

/**
 * @typedef {{
 *   id: string, page: string, section: string, table: string | null, case: string,
 *   hex: string | null, negative: boolean, composite: boolean, prose?: true,
 *   variants?: string[], columns?: Record<string, string>, text: string
 * }} Vector
 */

/** @param {string} s */
function slug(s) {
  return s
    .toLowerCase()
    .replace(/`/g, '')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '')
    .slice(0, 80)
    .replace(/-$/, '');
}

/** @param {string} line */
function cells(line) {
  return line
    .trim()
    .replace(/^\|/, '')
    .replace(/\|$/, '')
    .split('|')
    .map((c) => c.trim());
}

/** @param {string} hex */
const normal = (hex) => hex.trim().toUpperCase();

/** @param {string} hex */
const byteCount = (hex) => (hex === '' ? 0 : hex.split(' ').length);

/**
 * @param {string} hex
 * @param {number} n
 */
const repeat = (hex, n) => Array.from({ length: n }, () => hex).join(' ');

/** @param {string[]} parts */
const join_ = (parts) => parts.filter((p) => p !== '').join(' ');

/**
 * Resolves a composite bytes cell into its full hex, or variants, or nothing.
 * @param {string} cell
 * @param {{ blobs: Record<string, string>, aliases: Record<string, string>, base: string | null }} ctx
 * @returns {{ hex: string | null, variants?: string[] }}
 */
function resolveCell(cell, ctx) {
  const literal = (/** @type {string} */ s) => {
    const m = s.match(/^`([^`]*)`$/);
    return m !== null && hexPattern.test(m[1] ?? '') ? normal(m[1] ?? '') : undefined;
  };
  const baseHex = ctx.base === null ? undefined : ctx.blobs[ctx.base];
  // `X` followed by N bytes `YY`
  let m = cell.match(/^`([0-9A-Fa-f ]+)` followed by (\d+) bytes `([0-9A-Fa-f]{2})`$/);
  if (m !== null) {
    return { hex: join_([normal(m[1] ?? ''), repeat(normal(m[3] ?? ''), Number(m[2]))]) };
  }
  // byte N = `XX`, `YY`: changes to the base blob
  m = cell.match(/^byte (\d+) = ((?:`[0-9A-Fa-f]{2}`(?:, )?)+)$/);
  if (m !== null && baseHex !== undefined) {
    const offset = Number(m[1]);
    const bytes = baseHex.split(' ');
    const values = [...(m[2] ?? '').matchAll(/`([0-9A-Fa-f]{2})`/g)].map((v) => normal(v[1] ?? ''));
    if (offset >= bytes.length) {
      throw new Error(`byte ${offset} is outside ${ctx.base}: ${cell}`);
    }
    const variants = values.map((v) => bytes.map((b, i) => (i === offset ? v : b)).join(' '));
    return variants.length === 1 ? { hex: variants[0] ?? null } : { hex: null, variants };
  }
  m = cell.match(/^`([0-9A-Fa-f ]+)` appended$/);
  if (m !== null && baseHex !== undefined) {
    return { hex: join_([baseHex, normal(m[1] ?? '')]) };
  }
  if (cell === 'last byte removed' && baseHex !== undefined) {
    return { hex: baseHex.split(' ').slice(0, -1).join(' ') };
  }
  // A sequence: literals, `the N bytes of BLOB`, `the endpoint`.
  const parts = cell.split(/, /);
  const literals = parts.map(literal);
  if (parts.length > 1 && literals.every((l) => l !== undefined)) {
    return { hex: null, variants: /** @type {string[]} */ (literals) };
  }
  /** @type {string[]} */
  const out = [];
  for (const [i, part] of parts.entries()) {
    const lit = literals[i];
    if (lit !== undefined) {
      out.push(lit);
      continue;
    }
    const blob = part.match(/^the (\d+) bytes of (\w+)$/);
    if (blob !== null) {
      const hex = ctx.blobs[blob[2] ?? ''];
      if (hex === undefined || byteCount(hex) !== Number(blob[1])) {
        throw new Error(`"${part}" does not match a blob of that length`);
      }
      out.push(hex);
      continue;
    }
    const alias = part.match(/^the (\w+)$/);
    if (alias !== null && ctx.aliases[alias[1] ?? ''] !== undefined) {
      out.push(ctx.aliases[alias[1] ?? ''] ?? '');
      continue;
    }
    return { hex: null };
  }
  return { hex: join_(out) };
}

/**
 * The prose vectors of one paragraph. In a paragraph of protocol errors
 * every hex group is one; in a paragraph of decoding rules only the groups
 * the sentence decodes (`X` decodes as, `X` is an, `X` as).
 * @param {string} paragraph
 * @param {boolean} negative
 * @returns {{ hex: string, case: string }[]}
 */
function proseVectors(paragraph, negative) {
  /** @type {{ hex: string, case: string }[]} */
  const found = [];
  const re = /`([^`]*)`/g;
  for (const m of paragraph.matchAll(re)) {
    const hex = m[1] ?? '';
    if (!hexPattern.test(hex) || m.index === undefined) {
      continue;
    }
    const before = paragraph.slice(0, m.index);
    const after = paragraph.slice(m.index + m[0].length);
    if (!negative && !/^\s+(decodes as|is an?|reads as|as)\s/.test(after)) {
      continue;
    }
    // "description (`hex`)" or "`hex` (description)" or the clause around it.
    let description = '';
    const inParens = before.match(/\(\s*(?:for example\s*)?$/);
    if (inParens !== null) {
      const lead = before.slice(0, inParens.index);
      description = (lead.split(/[,;:.]\s/).pop() ?? '').replace(/^(and|or)\s+/, '');
    } else {
      const trailing = after.match(/^(?:\s+followed by [^(,;]*)?\s*\(([^)]*)\)/);
      description =
        trailing?.[1] ?? (after.split(/[,;.](\s|$)/)[0] ?? '').replace(/^\s+/, '').slice(0, 60);
    }
    found.push({ hex: normal(hex), case: description.trim() || normal(hex) });
  }
  return found;
}

/**
 * Parses one page (or, with [fromHeading], one top-level section of it).
 * @param {string} page
 * @param {string} text
 * @param {string | null} fromHeading
 * @param {Record<string, string>} blobs
 * @param {(() => void)[]} pending resolutions to run once every blob is known
 */
function parse(page, text, fromHeading, blobs, pending) {
  const lines = text.split('\n');
  /** @type {Vector[]} */
  const vectors = [];
  /** @type {Record<string, string>} */
  const aliases = {};
  let h1 = '';
  let h2 = '';
  let active = fromHeading === null;
  /** @type {string[] | null} */
  let header = null;
  let tableIndex = 0;
  let lastProse = '';
  /** @type {string | null} */
  let lastBlob = null;
  /** @type {string | null} */
  let base = null;
  /** @type {string | null} the table a paragraph's vectors belong with */
  let lastTable = null;
  const section = () => (h2 !== '' ? `${h1} / ${h2}` : h1);
  const sectionSlug = () => `${slug(h1)}${h2 !== '' ? `/${slug(h2)}` : ''}`;
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i] ?? '';
    if (line.startsWith('# ')) {
      h1 = line.slice(2).trim();
      h2 = '';
      lastProse = '';
      header = null;
      lastTable = null;
      if (fromHeading !== null) {
        active = h1 === fromHeading;
      }
      continue;
    }
    if (line.startsWith('## ')) {
      h2 = line.slice(3).trim();
      lastProse = '';
      header = null;
      lastTable = null;
      continue;
    }
    if (!active) {
      continue;
    }
    if (line.startsWith('```')) {
      /** @type {string[]} */
      const body = [];
      for (i++; i < lines.length && !(lines[i] ?? '').startsWith('```'); i++) {
        body.push(lines[i] ?? '');
      }
      /** @type {string | null} */
      let label = null;
      /** @type {Record<string, string>} */
      const groups = {};
      for (const row of body) {
        const m = row.match(/^([A-Za-z][A-Za-z ]*?)\s{2,}([0-9A-Fa-f ]+)$/);
        if (m !== null) {
          label = (m[1] ?? '').trim();
          groups[label] = `${groups[label] ?? ''} ${m[2]}`;
        } else if (hexPattern.test(row.trim())) {
          const key = label ?? '';
          groups[key] = `${groups[key] ?? ''} ${row.trim()}`;
        } else if (row.trim() === '') {
          label = null;
        }
      }
      const bold = lastProse.match(/\*\*([A-Z][A-Za-z0-9]*)\*\*/)?.[1];
      const name =
        bold ??
        (/signature/i.test(lastProse) && lastBlob !== null
          ? `${lastBlob}.signature`
          : slug(lastProse));
      lastBlob = bold ?? lastBlob;
      for (const [key, hex] of Object.entries(groups)) {
        blobs[key === '' ? name : `${name}.${slug(key)}`] = normal(hex.replace(/\s+/g, ' '));
      }
      continue;
    }
    if (line.startsWith('|')) {
      const row = cells(line);
      if (row.every((c) => /^-+$/.test(c))) {
        continue;
      }
      if (header === null) {
        header = row;
        tableIndex++;
        lastTable = `${sectionSlug()}#${tableIndex}`;
        base = lastProse.match(/offsets into (\w+)/)?.[1] ?? null;
        continue;
      }
      const column = header.findIndex((h) => bytesColumn.test(h));
      if (column < 0) {
        continue;
      }
      const cell = row[column] ?? '';
      const empty = cell === '(empty)';
      const literal = cell.match(/^`([^`]*)`$/)?.[1];
      const plain = literal !== undefined && hexPattern.test(literal);
      const hasHex = [...cell.matchAll(/`([^`]*)`/g)].some((g) => hexPattern.test(g[1] ?? ''));
      if (!plain && !empty && !hasHex && base === null) {
        continue;
      }
      const caseName = (row[0] ?? '').replace(/`/g, '');
      const vector = {
        id: `${page}/${sectionSlug()}/${slug(caseName)}`,
        page,
        section: section(),
        table: lastTable,
        case: caseName,
        hex: plain || empty ? (empty ? '' : normal(literal ?? '')) : null,
        negative: negativeProse.test(lastProse),
        composite: !plain && !empty,
        ...(header.length > 2
          ? { columns: Object.fromEntries(header.map((h, j) => [h, row[j] ?? ''])) }
          : {}),
        text: cell,
      };
      vectors.push(vector);
      if (vector.composite) {
        // Blobs can be defined further down (C1 is), so resolve at the end.
        const ctx = { blobs, aliases: { ...aliases }, base };
        pending.push(() => {
          const r = resolveCell(cell, ctx);
          /** @type {Vector} */ (vector).hex = r.hex;
          if (r.variants !== undefined) {
            /** @type {Vector} */ (vector).variants = r.variants;
          }
        });
      }
      continue;
    }
    header = null;
    if (line.trim() === '') {
      continue;
    }
    // A paragraph: up to the next blank line, table, fence or heading.
    let paragraph = line.trim();
    while (i + 1 < lines.length && /^[^|#`\s]/.test(lines[i + 1] ?? '')) {
      paragraph += ` ${(lines[++i] ?? '').trim()}`;
    }
    lastProse = paragraph;
    const alias = paragraph.match(/the (\w+) `[^`]+` is `([0-9A-Fa-f ]+)` with its length/);
    if (alias !== null) {
      aliases[alias[1] ?? ''] = normal(alias[2] ?? '');
    }
    if (ruleProse.test(paragraph)) {
      const negative = /protocol error|MUST be (rejected|refused)|is invalid/.test(paragraph);
      for (const p of proseVectors(paragraph, negative)) {
        vectors.push({
          id: `${page}/${sectionSlug()}/prose-${slug(p.hex)}`,
          page,
          section: section(),
          table: lastTable,
          case: p.case,
          hex: p.hex,
          negative,
          composite: true,
          prose: true,
          text: paragraph,
        });
      }
    }
  }
  return vectors;
}

/**
 * Extracts the vectors of the wiki directory [dir] at [commit].
 * @param {string} dir
 * @param {string | null} commit
 */
export function extract(dir, commit) {
  /** @type {Record<string, string>} */
  const blobs = {};
  /** @type {(() => void)[]} */
  const pending = [];
  const a = parse(
    'test-vectors',
    readFileSync(join(dir, 'test-vectors.md'), 'utf8'),
    null,
    blobs,
    pending,
  );
  /** @type {Record<string, string>} */
  const quicBlobs = { ...blobs };
  const b = parse(
    'quic-binding',
    readFileSync(join(dir, 'quic-binding.md'), 'utf8'),
    'Test vectors',
    quicBlobs,
    pending,
  );
  for (const resolveLater of pending) {
    resolveLater();
  }
  const ids = new Set();
  const vectors = [...a, ...b];
  for (const v of vectors) {
    let id = v.id;
    for (let n = 2; ids.has(id); n++) {
      id = `${v.id}-${n}`;
    }
    v.id = id;
    ids.add(id);
  }
  for (const [k, v] of Object.entries(quicBlobs)) {
    if (!(k in blobs)) {
      blobs[`quic.${k}`] = v;
    }
  }
  return {
    source: { repository: 'kaetemi/sway', path: 'wiki/switchboard', commit },
    vectors,
    blobs,
  };
}

function main() {
  const [dirArg, commitArg] = process.argv.slice(2);
  if (dirArg === undefined) {
    console.error('usage: node tool/extract-wiki-vectors.mjs <sway>/wiki/switchboard [commit]');
    return 64;
  }
  const dir = resolve(dirArg);
  const commit =
    commitArg ??
    execFileSync('git', ['-C', dir, 'log', '-1', '--format=%H', '--', '.'], {
      encoding: 'utf8',
    }).trim();
  const out = join(
    dirname(fileURLToPath(import.meta.url)),
    '..',
    'test',
    'vectors',
    'wiki-vectors.json',
  );
  const result = extract(dir, commit === '' ? null : commit);
  writeFileSync(out, `${JSON.stringify(result, null, 2)}\n`);
  // The repository's formatter has the last word on the layout.
  const biome = join(dirname(out), '..', '..', 'node_modules', '.bin', 'biome');
  if (existsSync(biome)) {
    execFileSync(biome, ['format', '--write', out], { stdio: 'ignore' });
  }
  console.error(
    `${result.vectors.length} vectors, ${Object.keys(result.blobs).length} blobs, ` +
      `wiki commit ${commit} -> ${out}`,
  );
  return 0;
}

if (process.argv[1] !== undefined && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  process.exitCode = main();
}
