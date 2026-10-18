/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Review finding 3 (docs/notes/review-mux.md). The connection's receive
// high-water mark counts the subframes buffered on channels the peer has
// closed, which no pause can release (nothing more arrives on them) and
// which only the consumer takes out. With pull delivery a channel the
// application takes and never reads is ordinary (a channel it only writes
// to, say); once such closed channels hold half the mark, a pause that a
// live channel starts lasts until the consumer deals with them.
//
// Decided: no change to the accounting. The wiki's resource-limits rule
// counts what all channels buffer, Dart counts the same, and the consumer
// owns a channel it was given: it reads it or closes it (design section
// 13 keeps the reviewer's concern open). What these tests pin is that the
// consumer can end such a pause: closing a channel that has ended, or
// cancelling its readable, drops what it buffered from the count, and the
// connection reads again.

import { describe, expect, test } from 'vitest';
import type { MuxChannel } from '../../../src/core/mux/contract.ts';
import type { FrameMuxConnection } from '../../../src/core/mux/frame-mux-connection.ts';
import { ManualClock } from '../../../src/testing/manual-clock.ts';
import { empty, Incoming, muxPair, quiet, Subframes, settle, tracked } from './support.ts';

/**
 * A pair whose acceptor holds two channels the peer closed after sending
 * 400 bytes on each, taken and never read, and is paused by a live channel
 * of which it read one subframe: the next one waits behind the pause.
 */
async function pausedByUnreadClosedChannels(): Promise<{
  readonly a: FrameMuxConnection;
  readonly b: FrameMuxConnection;
  readonly taken: readonly MuxChannel[];
  readonly second: { readonly settled: boolean };
}> {
  const clock = new ManualClock();
  const [a, b] = muxPair({
    clock,
    acceptor: { ...quiet, receiveHighWaterMarkBytes: 1000 },
  });
  const incoming = new Incoming(b);
  const closed = [a.open(empty), a.open(empty)];
  for (const channel of closed) {
    void channel.send(new Uint8Array(400));
    void channel.close();
  }
  const taken = [await incoming.next(), await incoming.next()];
  for (const channel of [...closed, ...taken]) {
    await channel.done;
  }
  expect(taken.every((channel) => channel.state === 'closed')).toBe(true);
  expect(b.bufferedBytes).toBe(2 * 416);
  // A live channel crosses the mark with its first subframe (832 + 416 >
  // 1000); reading it leaves 832 on the closed channels, above half the
  // mark, so the input stays paused.
  const live = a.open(empty);
  for (let i = 0; i < 3; i++) {
    void live.send(new Uint8Array(400));
  }
  const reader = new Subframes(await incoming.next());
  const first = tracked(reader.next());
  await settle();
  expect(first.settled).toBe(true);
  const second = tracked(reader.next());
  await settle();
  expect(second.settled).toBe(false);
  expect(b.isReceivePaused).toBe(true);
  expect(b.bufferedBytes).toBe(2 * 416);
  return { a, b, taken, second };
}

describe('review: the receive high-water mark and closed channels', () => {
  test('closing the unread channels that ended ends the pause', async () => {
    const { a, b, taken, second } = await pausedByUnreadClosedChannels();
    // One is enough: 416 is half the mark or less.
    await (taken[0] as MuxChannel).close();
    expect((taken[0] as MuxChannel).bufferedBytes).toBe(0);
    await settle();
    expect(b.isReceivePaused).toBe(false);
    expect(second.settled).toBe(true);
    await (taken[1] as MuxChannel).close();
    await settle();
    expect(b.bufferedBytes).toBe(416);
    const pong = tracked(a.ping());
    await settle();
    expect(pong.settled).toBe(true);
    await a.close();
    await b.done;
  });

  test('cancelling the readable of an unread channel that ended ends the pause', async () => {
    const { a, b, taken, second } = await pausedByUnreadClosedChannels();
    await (taken[0] as MuxChannel).readable.cancel();
    expect((taken[0] as MuxChannel).bufferedBytes).toBe(0);
    await settle();
    expect(b.isReceivePaused).toBe(false);
    expect(second.settled).toBe(true);
    const pong = tracked(a.ping());
    await settle();
    expect(pong.settled).toBe(true);
    await a.close();
    await b.done;
  });

  test('closing an ended channel that is being read keeps what it buffered for the reader', async () => {
    const [a, b] = muxPair();
    const incoming = new Incoming(b);
    const sent = a.open(empty);
    void sent.send(Uint8Array.of(1));
    void sent.send(Uint8Array.of(2));
    void sent.close();
    const channel = await incoming.next();
    await channel.done;
    const reader = new Subframes(channel);
    expect(await reader.nextHex()).toBe('01');
    await channel.close();
    expect(await reader.rest()).toEqual([Uint8Array.of(2)]);
    await a.close();
    await b.done;
  });
});
