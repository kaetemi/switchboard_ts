/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// REVIEW FINDING (docs/notes/review-mux.md, finding 3). The connection's receive
// high-water mark counts the subframes buffered on channels the peer has
// closed, which no pause can release (nothing more arrives on them) and
// which only a read of those channels takes out. With pull delivery a
// channel the application takes and never reads is ordinary (a channel it
// only writes to, say); once such closed channels hold half the mark, a
// pause that a live channel starts never ends: the connection stops reading
// the transport for good (and keep-alive no longer probes, the silence
// being local). Dart counts the same, and the resource-limits text it
// quotes from the wiki counts "what all channels buffer"; the exception it
// makes for manual credit ("pausing the connection cannot release it but
// would stall every other channel") applies to a closed channel as well.
//
// The test is `test.fails`: its body fails against the current sources.
// Once the defect is fixed it passes, `test.fails` reports that, and the
// `.fails` goes.

import { describe, expect, test } from 'vitest';
import { ManualClock } from '../../../src/testing/manual-clock.ts';
import { empty, Incoming, muxPair, quiet, Subframes, settle, tracked } from './support.ts';

describe('review: the receive high-water mark and closed channels', () => {
  test.fails('closed channels nobody reads do not keep the connection paused', async () => {
    const clock = new ManualClock();
    const [a, b] = muxPair({
      clock,
      acceptor: { ...quiet, receiveHighWaterMarkBytes: 1000 },
    });
    const incoming = new Incoming(b);
    // Two channels that send 400 bytes and close; b takes them, never reads.
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
    // A live channel crosses the mark on its own (3 * 416 > 1000) and b
    // reads all of it: what is left buffered is on closed channels.
    const live = a.open(empty);
    for (let i = 0; i < 3; i++) {
      void live.send(new Uint8Array(400));
    }
    const reader = new Subframes(await incoming.next());
    for (let i = 0; i < 3; i++) {
      const read = tracked(reader.next());
      await settle();
      expect(read.settled, `subframe ${i} of the live channel`).toBe(true);
    }
    expect(b.isReceivePaused).toBe(false);
    const pong = tracked(a.ping());
    await settle();
    expect(pong.settled).toBe(true);
    await a.close();
    await b.done;
  });
});
