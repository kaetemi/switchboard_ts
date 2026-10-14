// Seeded randomness and the fuzz inputs of the Dart naming harness
// (`fuzzInput` in test/naming_harness.dart): noise, and record-shaped
// payloads with the pieces that trip up endpoint parsing.
import { concatBytes, utf8Encode } from '../../../src/core/bytes.ts';
import { Name } from '../../../src/core/name.ts';

/** A small seeded generator (mulberry32): `next()` in [0, 1), `int(n)` in [0, n). */
export class Random {
  #state: number;

  constructor(seed: number) {
    this.#state = seed >>> 0;
  }

  next(): number {
    this.#state = (this.#state + 0x6d2b79f5) >>> 0;
    let t = this.#state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 0x100000000;
  }

  int(n: number): number {
    return Math.floor(this.next() * n);
  }
}

function noise(random: Random, n: number): Uint8Array {
  const out = new Uint8Array(n);
  for (let i = 0; i < n; i++) {
    out[i] = random.int(256);
  }
  return out;
}

const pieces = [
  '/.',
  '/..',
  './',
  '../',
  '/',
  '//',
  '.',
  '..',
  '\\',
  ':',
  '{',
  '}',
  '#',
  '?',
  '%',
  '%41',
  '%zz',
  '@',
  '[',
  ']',
  'h',
  '1',
  ' ',
  '\u00e9',
  '\u00ff',
  '"',
  '<',
  '^',
  '|',
  '`',
];
const prefixes = ['', 'tcp:', 'ws:', 'tcp:/', 'tcp://', 'ws://h/', 'tcp://h:', 'a:/'];

/** Input [i] of a fuzz run: noise for even [i], a record-shaped payload for odd. */
export function fuzzInput(random: Random, i: number): Uint8Array {
  if (i % 2 === 0) {
    return noise(random, random.int(64));
  }
  const parts: Uint8Array[] = [Name.of('npc').bytes];
  parts.push(random.int(4) === 0 ? new Uint8Array(6) : noise(random, 6));
  const count = random.int(4);
  parts.push(Uint8Array.of(count));
  for (let k = 0; k < count; k++) {
    if (random.int(4) === 0) {
      const length = random.int(256);
      parts.push(Uint8Array.of(length), noise(random, random.int(length + 2)));
      continue;
    }
    let text = prefixes[random.int(prefixes.length)] as string;
    for (let n = random.int(12); n > 0; n--) {
      text += pieces[random.int(pieces.length)];
    }
    const encoded = utf8Encode(text);
    const length = Math.min(encoded.length, 255);
    parts.push(Uint8Array.of(length), encoded.subarray(0, length));
  }
  switch (random.int(4)) {
    case 0:
      break;
    case 1:
      parts.push(
        Uint8Array.of(random.int(256), random.int(3) === 0 ? random.int(256) : 0),
        noise(random, random.int(9)),
      );
      break;
    default: {
      const length = random.int(8);
      parts.push(Uint8Array.of(length, 0), noise(random, length));
    }
  }
  return concatBytes(...parts);
}
