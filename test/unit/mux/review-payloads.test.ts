/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// REVIEW FINDING (docs/notes/review-mux.md, finding 2). `FrameMuxChannel.send()`,
// `openAfter()` and `FrameMuxConnection.open()` take any value with a
// `length` at run time: a string passed by an untyped caller (JavaScript,
// or a cast) is copied into the frame with `Uint8Array#set`, which turns
// every character into 0, so the peer receives as many zero bytes as the
// string has characters, and nothing fails. Design section 5 says strings
// are UTF-8 payloads wherever Dart takes a `Uint8List`; the channel's
// `writable` refuses a chunk that is not bytes with a TypeError. Either is
// acceptable here: a TypeError before anything is sent, or the UTF-8 bytes
// on the wire. Zeros are not.
//
// The tests are `test.fails`: their bodies fail against the current sources.
// Once the defect is fixed they pass, `test.fails` reports that, and the
// `.fails` goes.

import { describe, expect, test } from 'vitest';
import { hexString } from '../../../src/core/bytes.ts';
import type { MuxChannel } from '../../../src/core/mux/contract.ts';
import { empty, rawPair } from './support.ts';

/** What [call] does with a string: `TypeError`, or the hex of the frame it sends. */
async function outcomeOf(call: () => unknown, next: () => Promise<string>): Promise<string> {
  try {
    call();
  } catch (error) {
    return error instanceof TypeError ? 'TypeError' : `threw ${String(error)}`;
  }
  return next();
}

describe('review: a string given as bytes', () => {
  test.fails('send() refuses it or sends its UTF-8, never zeros', async () => {
    const [mux, raw] = rawPair();
    const channel = mux.open(empty);
    expect(await raw.nextHex()).toBe('12 02 00');
    const sent = await outcomeOf(
      () => channel.send('hi' as unknown as Uint8Array),
      () => raw.nextHex(),
    );
    expect(['TypeError', '02 02 00 68 69']).toContain(sent);
    await mux.close();
  });

  test.fails('open() refuses it or opens with its UTF-8, never zeros', async () => {
    const [mux, raw] = rawPair();
    const opened = await outcomeOf(
      () => mux.open('hi' as unknown as Uint8Array),
      () => raw.nextHex(),
    );
    expect(['TypeError', '12 02 00 68 69']).toContain(opened);
    await mux.close();
  });

  test.fails('openAfter() refuses it or opens with its UTF-8, never zeros', async () => {
    const [mux, raw] = rawPair();
    const parent: MuxChannel = mux.open(empty);
    expect(await raw.nextHex()).toBe('12 02 00');
    let child: MuxChannel | undefined;
    const opened = await outcomeOf(
      () => {
        child = parent.openAfter('hi' as unknown as Uint8Array);
      },
      () => raw.nextHex(),
    );
    expect(['TypeError', '12 04 00 68 69']).toContain(opened);
    expect(child === undefined || hexString(child.openPayload) === '68 69').toBe(true);
    await mux.close();
  });
});
