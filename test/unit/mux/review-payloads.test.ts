/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Review finding 2 (docs/notes/review-mux.md). `FrameMuxChannel.send()`,
// `openAfter()` and `FrameMuxConnection.open()` took any value with a
// `length` at run time: a string passed by an untyped caller (JavaScript,
// or a cast) was copied into the frame with `Uint8Array#set`, which turns
// every character into 0, so the peer received as many zero bytes as the
// string has characters, and nothing failed. The contract types them
// `Uint8Array`, so a string is the caller's misuse: they throw a TypeError
// before anything is sent, as the channel's `writable` already did, and so
// do the QUIC binding's `open()`, `openAfter()` and `send()`. A Node
// `Buffer` is a `Uint8Array` and is taken.

import { describe, expect, test } from 'vitest';
import type { MuxChannel } from '../../../src/core/mux/contract.ts';
import { bytes, connectionPair, firstChunk, nextChannel, text } from '../quic/harness.ts';
import { empty, rawPair } from './support.ts';

const hi = 'hi' as unknown as Uint8Array;

describe('review: a string given as bytes', () => {
  test('send() refuses it with a TypeError and sends nothing', async () => {
    const [mux, raw] = rawPair();
    const channel = mux.open(empty);
    expect(await raw.nextHex()).toBe('12 02 00');
    expect(() => channel.send(hi)).toThrow(TypeError);
    // Nothing went out for it: the next frame is the next send's.
    void channel.send(Buffer.from('hi'));
    expect(await raw.nextHex()).toBe('02 02 00 68 69');
    await mux.close();
  });

  test('open() refuses it with a TypeError and opens nothing', async () => {
    const [mux, raw] = rawPair();
    expect(() => mux.open(hi)).toThrow(TypeError);
    expect(mux.openChannelCount).toBe(0);
    // The id was not taken either.
    mux.open(Buffer.from('hi'));
    expect(await raw.nextHex()).toBe('12 02 00 68 69');
    await mux.close();
  });

  test('openAfter() refuses it with a TypeError and opens nothing', async () => {
    const [mux, raw] = rawPair();
    const parent: MuxChannel = mux.open(empty);
    expect(await raw.nextHex()).toBe('12 02 00');
    expect(() => parent.openAfter(hi)).toThrow(TypeError);
    expect(mux.openChannelCount).toBe(1);
    parent.openAfter(Buffer.from('hi'));
    expect(await raw.nextHex()).toBe('12 04 00 68 69');
    await mux.close();
  });

  test('the QUIC binding refuses it in open(), openAfter() and send() alike', async () => {
    const [client, server] = await connectionPair();
    expect(() => client.open(hi)).toThrow(TypeError);
    const channel = client.open(bytes('x'));
    expect(() => channel.send(hi)).toThrow(TypeError);
    expect(() => channel.openAfter(hi)).toThrow(TypeError);
    void channel.send(bytes('hi'));
    const accepted = await nextChannel(server);
    expect(text(accepted.openPayload)).toBe('x');
    expect(text(await firstChunk(accepted))).toBe('hi');
  });
});
