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
//
// A Buffer the caller reuses (found by the QUIC review in the frame mux):
// the frame mux copied the caller's bytes it keeps (the OPEN payload, the
// `identify` intent, a PING payload awaiting its PONG, DATA held behind an
// `openAfter` OPEN) with `slice()`, which on a Node `Buffer` is a view
// sharing its memory, so changing the Buffer after the call changed the
// library's copy. They are copied with the `Uint8Array` constructor now.

import { describe, expect, test } from 'vitest';
import { hexBytes, hexString } from '../../../src/core/bytes.ts';
import type { MuxChannel } from '../../../src/core/mux/contract.ts';
import { MuxCommand, MuxControlMessage, MuxControlType } from '../../../src/core/mux/mux-frame.ts';
import { MuxIdent } from '../../../src/core/mux/mux-protocol.ts';
import { stubCredential } from '../../support/identity/stub-verifier.ts';
import { bytes, connectionPair, firstChunk, nextChannel, text } from '../quic/harness.ts';
import { creditHex, empty, rawPair, rejection, settleRaw, tracked } from './support.ts';

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

describe('review: a Buffer the caller changes after the call', () => {
  test('changes none of the bytes the frame mux keeps', async () => {
    const [mux, raw] = rawPair();
    // The OPEN payload.
    const openPayload = Buffer.from('ab');
    const parent = mux.open(openPayload);
    openPayload.fill(0);
    expect(hexString(parent.openPayload)).toBe('61 62');
    expect(await raw.nextHex()).toBe('12 02 00 61 62');
    // A PING payload, which its PONG must match.
    const pingPayload = Buffer.from([1]);
    const pong = tracked(mux.ping(pingPayload));
    pingPayload[0] = 9;
    expect(await raw.nextHex()).toBe('02 00 00 01 01');
    raw.send('02 00 00 02 01');
    await settleRaw(raw);
    expect(pong.settled).toBe(true);
    // DATA held behind an OPEN that waits behind its parent's frames: the
    // parent's third subframe waits for credit, the child's OPEN behind it.
    for (let i = 0; i < 3; i++) {
      void parent.send(new Uint8Array(parent.maxSubframeLength));
    }
    const child = parent.openAfter(empty);
    const subframe = Buffer.from('cd');
    await child.send(subframe);
    subframe.fill(0);
    raw.send(creditHex(parent.id, 65536));
    for (;;) {
      const frame = await raw.next();
      if (frame.channelId === child.id && frame.command === MuxCommand.data) {
        expect(hexString(frame.payload)).toBe('63 64');
        break;
      }
    }
    // The intent of identify, sent once the peer's NONCE arrived.
    const intent = Buffer.from('ef');
    const credential = stubCredential({ identity: 'n', bytes: hexBytes('01') });
    const identifying = mux.identify(credential, { intent });
    intent.fill(0);
    await raw.nextControl(MuxControlType.nonce);
    raw.sendBytes(MuxControlMessage.nonce(new Uint8Array(32).fill(7)).toFrame().encode());
    const ident = MuxIdent.decode((await raw.nextControl(MuxControlType.ident)).payload);
    expect(hexString(ident.intent)).toBe('65 66');
    await mux.close();
    await rejection(identifying);
  });
});
