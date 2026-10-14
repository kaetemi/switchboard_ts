// The reference key-to-slot function, FNV-1a 32 of the key bytes modulo the
// slot count (Dart `test/vectors/slot_key_vectors_test.dart`). The 32-bit
// multiplication must stay exact. The wiki's test-vector page has no
// key-to-slot table (test/vectors/wiki-vectors.json has no such vector), so
// there is nothing to register through test/support/vectors.ts here: these
// are the Dart file's values.
import { expect, test } from 'vitest';
import { utf8Encode } from '../../src/core/bytes.ts';
import { fnv1a32, slotForKey, slotForText } from '../../src/core/naming/index.ts';

test('FNV-1a 32 vectors', () => {
  expect(fnv1a32([])).toBe(0x811c9dc5);
  expect(fnv1a32(utf8Encode('a'))).toBe(0xe40c292c);
  expect(fnv1a32(utf8Encode('foobar'))).toBe(0xbf9cf968);
  expect(fnv1a32(utf8Encode('user:42'))).toBe(0x2f6b7b82);
  expect(fnv1a32(Array.from({ length: 256 }, (_, i) => i))).toBe(0x90a458c5);
  // Elements are taken modulo 256.
  expect(fnv1a32([0x161])).toBe(fnv1a32(utf8Encode('a')));
});

test('slot of a key', () => {
  expect(slotForText('user:42', 65536)).toBe(0x2f6b7b82 % 65536);
  expect(slotForText('user:42', 65536)).toBe(31618);
  expect(slotForKey(utf8Encode('foobar'), 1)).toBe(0);
  expect(slotForText('é', 7)).toBe(slotForKey([0xc3, 0xa9], 7));
  expect(() => slotForKey([1], 0)).toThrow(RangeError);
  expect(() => slotForKey([1], 0x100000000)).toThrow(RangeError);
  expect(slotForKey([1], 0xffffffff)).toBe(fnv1a32([1]) % 0xffffffff);
});
