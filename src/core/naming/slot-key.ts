/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { utf8Encode } from '../bytes.ts';

/**
 * FNV-1a 32 of [bytes] (offset basis `0x811c9dc5`, prime `16777619`), as an
 * unsigned 32-bit value. Each element is taken modulo 256. `Math.imul`
 * keeps the multiplication exact.
 */
export function fnv1a32(bytes: ArrayLike<number>): number {
  let hash = 0x811c9dc5;
  for (let i = 0; i < bytes.length; i++) {
    hash ^= (bytes[i] as number) & 0xff;
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

/**
 * The reference key-to-slot function of the wiki page "Polyverse
 * Switchboard Sharding": {@link fnv1a32} of the key bytes modulo [count].
 * A peer in another language that hashes the same bytes the same way gets
 * the same slot.
 *
 * @throws {RangeError} unless [count] is an integer from 1 to 0xFFFFFFFF.
 */
export function slotForKey(key: ArrayLike<number>, count: number): number {
  if (!Number.isInteger(count) || count < 1 || count > 0xffffffff) {
    throw new RangeError(`slot count ${count} is outside 1 to 0xFFFFFFFF`);
  }
  return fnv1a32(key) % count;
}

/** {@link slotForKey} of the UTF-8 encoding of [key]. */
export function slotForText(key: string, count: number): number {
  return slotForKey(utf8Encode(key), count);
}
