// Output scheduling (wiki page "Polyverse Switchboard Mux", section "Output
// scheduling"): what waits for a transport that is not ready, and in which
// order it goes; bulk channels and their chunks; `openAfter`. Ports
// `test/mux_schedule_test.dart`.
//
// Dart's "synchronous peers" answer a frame during the mux's write of it.
// Here `AnsweringTransport` queues the answer as the frame is written and
// the mux reads it in a later microtask, so the answer is handled after
// the write and what the write set off, never inside them; what the tests
// check (the frames that go out and their order) is the same.
import { describe, expect, test } from 'vitest';
import type { MuxChannel, MuxPriority } from '../../../src/core/mux/contract.ts';
import { FrameMuxConnection } from '../../../src/core/mux/frame-mux-connection.ts';
import {
  MuxCommand,
  MuxControlMessage,
  MuxControlType,
  MuxCredit,
  MuxFrame,
  MuxLimits,
} from '../../../src/core/mux/mux-frame.ts';
import { Status, StatusCode } from '../../../src/core/status.ts';
import { StreamTransport } from '../../../src/core/transport/stream-transport.ts';
import {
  AnsweringTransport,
  ByteDrain,
  ByteFeed,
  clockOf,
  collect,
  creditHex,
  elapseUntil,
  empty,
  expectCode,
  expectRejectsStatus,
  expectStatus,
  expectThrowsStatus,
  firstIncoming,
  GatedTransport,
  gatedPair,
  Incoming,
  isControl,
  limitsHex,
  memoryPair,
  muxPair,
  quiet,
  RawPeer,
  rawOptions,
  rawPair,
  SteppingClock,
  Subframes,
  settle,
  settleRaw,
  takeIncoming,
} from './support.ts';

function isData(frame: MuxFrame): boolean {
  return frame.command === MuxCommand.data && frame.channelId !== 0;
}

/** A subframe naming its channel and a sequence number. */
function tagged(channel: MuxChannel, n: number): Uint8Array {
  return Uint8Array.of(channel.id, n);
}

/** The [channel, sequence number] of the DATA frames written so far. */
function order(gate: GatedTransport): [number, number][] {
  return gate.written.filter(isData).map((frame) => [frame.channelId, frame.payload[1] ?? -1]);
}

/** [command, channel id] of every frame written but control messages. */
function commands(gate: GatedTransport): [MuxCommand, number][] {
  return gate.on().map((frame) => [frame.command, frame.channelId]);
}

describe('output scheduler', () => {
  test('CREDIT cannot replace credit reserved by unwritten DATA', async () => {
    const clock = new SteppingClock();
    const [local, remote] = memoryPair(clock);
    const gate = new GatedTransport(local);
    const a = new FrameMuxConnection(gate, { ...rawOptions, clock, isInitiator: true });
    const raw = new RawPeer(remote);
    raw.send(limitsHex(MuxLimits.maxWindow));
    await settleRaw(raw);
    const channel = a.open(empty);
    await channel.send(new Uint8Array(1));
    expect(a.heldOutputBytes).toBeGreaterThan(0);
    // Nothing was written, so none of the initial window was consumed.
    raw.send(creditHex(channel.id, 1));
    await settle();
    expect(a.isOpen).toBe(false);
    expectCode(await a.done, StatusCode.protocolError);
  });

  test('a priority change moves already queued frames to the new tier', async () => {
    const [a, , gate] = gatedPair();
    await a.ping();
    const first = a.open(empty);
    const second = a.open(empty);
    await first.send(tagged(first, 1));
    await second.send(tagged(second, 2));
    first.priority = 'bulk';
    gate.allow(2);
    await settle();
    expect(order(gate)).toEqual([
      [second.id, 2],
      [first.id, 1],
    ]);
    await a.close();
  });

  test('closed channels leave no scheduler turns while output is blocked', async () => {
    // Dart counts the scheduler's turn lists through mirrors; they are
    // private here. What can be seen: nothing is held, and nothing stale
    // goes out once the transport takes frames again.
    const [a, b, gate] = gatedPair();
    const incoming = new Incoming(b);
    for (let i = 0; i < 40; i++) {
      const channel = a.open(empty);
      const peer = await incoming.next();
      await channel.send(new Uint8Array(1));
      await peer.close();
    }
    expect(a.heldOutputBytes).toBe(0);
    expect(gate.isOutputReady).toBe(false);
    gate.allow(1000);
    await a.ping();
    expect(order(gate)).toEqual([]);
    await incoming.cancel();
    await a.close();
  });

  test('connection loss releases scheduled and pre-OPEN output', async () => {
    // As above, the pre-OPEN list and the turn lists are private: what is
    // held is released, and the channel that waited for its OPEN ends.
    const [a, b, gate] = gatedPair();
    await a.ping();
    const parent = a.open(empty);
    await parent.send(new Uint8Array(1));
    const child = parent.openAfter(empty);
    await child.send(new Uint8Array(1));
    expect(a.heldOutputBytes).toBeGreaterThan(0);
    await b.close();
    await a.done;
    expect(a.heldOutputBytes).toBe(0);
    expect(child.state).toBe('closed');
    expectCode(await child.done, StatusCode.connectionLost);
    gate.allow(1000);
    await settle();
    expect(gate.on(child.id)).toEqual([]);
  });

  test('frames go straight out while the transport is ready', async () => {
    const [a, b, gate] = gatedPair();
    gate.allow(1000);
    await a.ping();
    const channel = a.open(empty);
    await channel.send(tagged(channel, 0));
    expect(a.heldOutputBytes).toBe(0);
    expect(order(gate)).toEqual([[2, 0]]);
    expect(b.isOpen).toBe(true);
    await a.close();
  });

  test('control messages are never held', async () => {
    const [a, b, gate] = gatedPair();
    const channel = a.open(empty);
    await channel.send(tagged(channel, 0));
    expect(a.heldOutputBytes).toBeGreaterThan(0);
    // The PING overtakes the DATA held for the transport, and so does the
    // OPEN of another channel.
    const pinged = a.ping();
    const other = a.open(empty);
    await settle();
    const written = gate.written.map((f) => `${f.command} ${f.channelId}`);
    const expected = [
      `${MuxCommand.open} 2`,
      `${MuxCommand.data} 0`,
      `${MuxCommand.open} ${other.id}`,
    ];
    let at = 0;
    for (const frame of written) {
      if (frame === expected[at]) {
        at++;
      }
    }
    expect(at, written.join(', ')).toBe(expected.length);
    expect(order(gate)).toEqual([]);
    await pinged;
    gate.allow(1);
    await settle();
    expect(order(gate)).toEqual([[2, 0]]);
    expect(a.heldOutputBytes).toBe(0);
    expect(b.isOpen).toBe(true);
    await a.close();
  });

  test('ordinary channels take turns, one frame each', async () => {
    const [a, , gate] = gatedPair();
    const channels = [0, 1, 2].map(() => a.open(empty));
    for (let n = 0; n < 3; n++) {
      for (const channel of channels) {
        await channel.send(tagged(channel, n));
      }
    }
    // Some more on the first channel only.
    const first = channels[0] as MuxChannel;
    for (let n = 3; n < 5; n++) {
      await first.send(tagged(first, n));
    }
    gate.allow(11);
    await settle();
    expect(order(gate)).toEqual([
      [2, 0],
      [4, 0],
      [6, 0],
      [2, 1],
      [4, 1],
      [6, 1],
      [2, 2],
      [4, 2],
      [6, 2],
      [2, 3],
      [2, 4],
    ]);
    await a.close();
  });

  test('a bulk frame goes after bulkZipper ordinary ones while bulk work waits', async () => {
    const [a, , gate] = gatedPair({ initiator: { ...quiet, bulkChunkSize: 10 } });
    const bulk = a.open(empty);
    bulk.priority = 'bulk';
    const first = a.open(empty);
    const second = a.open(empty);
    // Five chunks of 10 bytes.
    await bulk.send(new Uint8Array(50).fill(bulk.id));
    for (let n = 0; n < 6; n++) {
      await first.send(tagged(first, n));
      await second.send(tagged(second, n));
    }
    gate.allow(17);
    await settle();
    const ids = gate.written.filter(isData).map((frame) => frame.channelId);
    const [b, o1, o2] = [2, 4, 6];
    expect(ids).toEqual([o1, o2, o1, o2, b, o1, o2, o1, o2, b, o1, o2, o1, o2, b, b, b]);
    // The order within each channel is kept.
    expect(
      gate
        .on(o1)
        .filter(isData)
        .map((frame) => frame.payload[1]),
    ).toEqual([0, 1, 2, 3, 4, 5]);
    await a.close();
  });

  test('a CLOSE follows the DATA queued before it, an OPEN does not wait', async () => {
    const [a, b, gate] = gatedPair();
    const atB = new Incoming(b);
    const channel = a.open(empty);
    await channel.send(tagged(channel, 0));
    await channel.send(tagged(channel, 1));
    const closed = channel.close();
    const later = a.open(empty);
    await settle();
    expect(gate.on(2).map((f) => f.command)).toEqual([MuxCommand.open]);
    expect(gate.written.at(-1)?.channelId).toBe(later.id);
    gate.allow(3);
    const atBChannel = await atB.next();
    expect(await collect(atBChannel)).toHaveLength(2);
    await closed;
    expect(gate.on(2).map((f) => f.command)).toEqual([
      MuxCommand.open,
      MuxCommand.data,
      MuxCommand.data,
      MuxCommand.close,
    ]);
    await atB.cancel();
    await a.close();
  });

  test("the peer's CLOSE drops what waits for the transport, and the confirmation goes at once", async () => {
    const [a, b, gate] = gatedPair();
    const channel = a.open(empty);
    const atB = await firstIncoming(b);
    for (let n = 0; n < 3; n++) {
      await channel.send(tagged(channel, n));
    }
    expect(a.heldOutputBytes).toBeGreaterThan(0);
    await atB.close(Status.of(StatusCode.cancelled));
    expectCode(await channel.done, StatusCode.cancelled);
    expect(a.heldOutputBytes).toBe(0);
    expect(order(gate)).toEqual([]);
    expect(gate.written.at(-1)?.command).toBe(MuxCommand.close);
    gate.allow(10);
    await settle();
    expect(order(gate)).toEqual([]);
    await a.close();
  });

  test(
    "a channel whose close was not confirmed keeps its queue until the peer's CLOSE, which " +
      'drops the DATA and lets the CLOSE go',
    async () => {
      const [a, b, gate] = gatedPair({ initiator: { ...quiet, closeConfirmTimeout: 20 } });
      const channel = a.open(empty);
      const atB = await firstIncoming(b);
      for (let n = 0; n < 3; n++) {
        await channel.send(tagged(channel, n));
      }
      // Nothing written: the CLOSE waits behind the DATA, and the
      // confirmation times out.
      await elapseUntil(clockOf(a), channel.close());
      expect(a.unconfirmedCloseCount).toBe(1);
      expect(a.heldOutputBytes).toBeGreaterThan(0);
      expect(gate.on(2).map((f) => f.command)).toEqual([MuxCommand.open]);
      // The peer closes too: its CLOSE crosses ours.
      void atB.close();
      await settle();
      expect(a.unconfirmedCloseCount).toBe(0);
      expect(a.heldOutputBytes).toBe(0);
      expect(gate.on(2).map((f) => f.command)).toEqual([MuxCommand.open, MuxCommand.close]);
      expectStatus(await atB.done, Status.ok);
      await a.close();
    },
  );

  test('closing the connection writes the GOAWAY first, then what was held', async () => {
    const [a, b, gate] = gatedPair();
    const channel = a.open(empty);
    const received = collect(await firstIncoming(b));
    await channel.send(tagged(channel, 0));
    await channel.send(tagged(channel, 1));
    void a.goAway();
    await settle();
    const goAway = gate.written.findIndex((f) => isControl(f, MuxControlType.goAway));
    expect(goAway).not.toBe(-1);
    expect(order(gate)).toEqual([]);
    await a.close();
    expect(order(gate)).toEqual([
      [2, 0],
      [2, 1],
    ]);
    expect(gate.written.findIndex(isData)).toBeGreaterThan(goAway);
    expect(await received).toHaveLength(2);
    expect(a.heldOutputBytes).toBe(0);
  });
});

describe('bulk channels', () => {
  test('a payload is cut into chunks of bulkChunkSize', async () => {
    const [a, b] = muxPair({ acceptor: { ...quiet, initialWindow: 1 << 20 } });
    await a.ping();
    const channel = a.open(empty);
    channel.priority = 'bulk';
    const atB = new Subframes(await firstIncoming(b));
    const payload = Uint8Array.from({ length: 200000 }, (_, i) => (i * 31) & 0xff);
    // The window takes it all: handed over at once.
    await channel.send(payload);
    const received = await atB.take(4);
    expect(received.map((d) => d.length)).toEqual([65536, 65536, 65536, 3392]);
    const joined = new Uint8Array(200000);
    let at = 0;
    for (const chunk of received) {
      joined.set(chunk, at);
      at += chunk.length;
    }
    expect(joined).toEqual(payload);
    // An empty one is one empty subframe.
    await channel.send(empty);
    expect(await atB.next()).toHaveLength(0);
    await atB.cancel();
    await a.close();
  });

  test('chunks fit the window and the peer frame limit', async () => {
    const [a, b] = muxPair({ acceptor: { ...quiet, maxFrameSize: 1000 } });
    await a.ping();
    const limited = a.open(empty);
    limited.priority = 'bulk';
    const atB = new Incoming(b);
    const limitedAtB = await atB.next();
    await limited.send(new Uint8Array(2500));
    expect((await new Subframes(limitedAtB).take(3)).map((d) => d.length)).toEqual([997, 997, 506]);
    // The default window, 64 KiB: each chunk takes what the window allows,
    // the transfer goes on as credit comes back.
    const wide = a.open(empty);
    wide.priority = 'bulk';
    const wideAtB = await atB.next();
    const total = 300000;
    const payload = Uint8Array.from({ length: total }, (_, i) => (i * 7) & 0xff);
    const sent = wide.send(payload);
    const received = new Uint8Array(total);
    let length = 0;
    const sizes: number[] = [];
    for await (const chunk of wideAtB.readable) {
      received.set(chunk, length);
      length += chunk.length;
      sizes.push(chunk.length);
      if (length === total) {
        break;
      }
    }
    await sent;
    expect(received).toEqual(payload);
    for (const size of sizes) {
      expect(size).toBeLessThanOrEqual(997);
    }
    await atB.cancel();
    await a.close();
  });

  test('a bulk send waits in order behind earlier ones and before a CLOSE', async () => {
    const [a, b] = muxPair();
    await a.ping();
    const channel = a.open(empty);
    channel.priority = 'bulk';
    const atB = await firstIncoming(b);
    const all = (async () => {
      let n = 0;
      for await (const d of atB.readable) {
        n += d.length;
      }
      return n;
    })();
    const first = channel.send(new Uint8Array(100000));
    const second = channel.send(new Uint8Array(100000));
    void channel.close();
    await Promise.all([first, second]);
    expect(await all).toBe(200000);
    expectStatus(await channel.done, Status.ok);
    await a.close();
  });

  test('an ordinary frame overtakes a transfer on a saturated transport', async () => {
    // The initiator writes through a stream transport whose output takes a
    // chunk every 2 ms: the scheduler holds what the transport has not
    // taken.
    const clock = new SteppingClock();
    const aToB = new ByteFeed();
    const bToA = new ByteFeed();
    const a = new FrameMuxConnection(
      new StreamTransport(bToA, new ByteDrain({ rate: 2, clock, target: aToB }), { clock }),
      { ...quiet, clock, isInitiator: true },
    );
    const b = new FrameMuxConnection(
      new StreamTransport(aToB, new ByteDrain({ target: bToA }), { clock }),
      { ...quiet, initialWindow: 8 << 20, clock, isInitiator: false },
    );
    await elapseUntil(clock, a.ping());
    await elapseUntil(clock, b.ping());
    const bulk = a.open(empty);
    bulk.priority = 'bulk';
    const normal = a.open(empty);
    const atB = new Incoming(b);
    const bulkNext = atB.next();
    await elapseUntil(clock, bulkNext);
    const bulkAtB = await bulkNext;
    const normalNext = atB.next();
    await elapseUntil(clock, normalNext);
    const normalAtB = new Subframes(await normalNext);
    let bulkBytes = 0;
    const transferred = (async () => {
      for await (const d of bulkAtB.readable) {
        bulkBytes += d.length;
      }
    })();
    const total = 4 << 20;
    // The window takes it all at once: 64 chunks wait for the transport.
    await bulk.send(new Uint8Array(total));
    expect(a.heldOutputBytes).toBeGreaterThan(total - 4 * 65536);
    const waits: number[] = [];
    for (let i = 0; i < 5; i++) {
      await clock.elapse(5);
      const before = bulkBytes;
      await normal.send(new Uint8Array(10));
      await elapseUntil(clock, normalAtB.next());
      waits.push(bulkBytes - before);
    }
    // Still transferring, and each ordinary frame waited for at most the
    // chunk the transport was writing and the one being read.
    expect(bulkBytes).toBeLessThan(total);
    for (const wait of waits) {
      expect(wait).toBeLessThanOrEqual(2 * 65536);
    }
    while (bulkBytes < total) {
      await clock.elapse(5);
    }
    await normalAtB.cancel();
    await atB.cancel();
    await elapseUntil(clock, a.close());
    await elapseUntil(clock, b.close());
    await transferred;
  });
});

describe('openAfter', () => {
  test('a synchronous peer may refuse a child during its OPEN write', async () => {
    const clock = new SteppingClock();
    const transport = new AnsweringTransport((bytes) => {
      const frame = MuxFrame.decode(bytes);
      return frame.command === MuxCommand.open && frame.payload.length > 0
        ? [MuxFrame.close(frame.channelId).encode()]
        : [];
    });
    const mux = new FrameMuxConnection(transport, { ...rawOptions, clock, isInitiator: true });
    const parent = mux.open(empty);
    const child = parent.openAfter(Uint8Array.of(9));
    expectStatus(await child.done, Status.ok);
    expect(mux.isOpen).toBe(true);
    expect(mux.openChannelCount).toBe(1);
    await mux.close();
  });

  /**
   * A mux over a gated transport whose raw peer answers as the mux writes:
   * [reply] sees every frame the mux writes, and what it returns is queued
   * for the mux before the write returns.
   */
  function syncPeer(
    reply: (frame: MuxFrame) => readonly MuxFrame[],
  ): [FrameMuxConnection, GatedTransport] {
    const clock = new SteppingClock();
    const transport = new AnsweringTransport((bytes) =>
      reply(MuxFrame.decode(bytes)).map((frame) => frame.encode()),
    );
    const gate = new GatedTransport(transport);
    const mux = new FrameMuxConnection(gate, { ...rawOptions, clock, isInitiator: true });
    return [mux, gate];
  }

  test(
    'a child refused during its OPEN write from the scheduler still sends its CLOSE and the ' +
      'OPENs behind it',
    async () => {
      const [mux, gate] = syncPeer((frame) =>
        frame.command === MuxCommand.open && frame.payload.length > 0
          ? [MuxFrame.close(frame.channelId)]
          : [],
      );
      const parent = mux.open(empty);
      await parent.send(new Uint8Array(1));
      const child = parent.openAfter(Uint8Array.of(9));
      const grandchild = child.openAfter(Uint8Array.of(8));
      const closed = child.close();
      // The OPEN goes from the scheduler, not from a frame the mux reads.
      gate.allow(10);
      await closed;
      await settle();
      const on = (channel: MuxChannel): MuxCommand[] =>
        gate.on(channel.id).map((frame) => frame.command);
      // Our CLOSE confirms the peer's: without it the peer would keep the
      // id half closed, and refuse its reuse.
      expect(on(child)).toEqual([MuxCommand.open, MuxCommand.close]);
      expect(on(grandchild)).toEqual([MuxCommand.open, MuxCommand.close]);
      expect(grandchild.state).toBe('closed');
      expect(mux.openChannelCount).toBe(1);
      expect(mux.isOpen).toBe(true);
      await mux.close();
    },
  );

  test('what a synchronous answer to the OPEN releases follows the frames held for it', async () => {
    const [mux, gate] = syncPeer((frame) =>
      frame.command === MuxCommand.open && frame.payload.length > 0
        ? [MuxControlMessage.credit(new MuxCredit(frame.channelId, 100)).toFrame()]
        : [],
    );
    const parent = mux.open(empty);
    await parent.send(new Uint8Array(1));
    const child = parent.openAfter(Uint8Array.of(9));
    // Held for the OPEN, using the whole window.
    await child.send(new Uint8Array(32752));
    await child.send(new Uint8Array(32752));
    // Waits for the CREDIT the peer sends as the OPEN is written.
    const last = child.send(new Uint8Array(3));
    gate.allow(10);
    await last;
    await settle();
    expect(
      gate
        .on(child.id)
        .filter(isData)
        .map((frame) => frame.payload.length),
    ).toEqual([32752, 32752, 3]);
    await mux.close();
  });

  for (const [name, command] of [
    ['data', MuxCommand.data],
    ['close', MuxCommand.close],
  ] as const) {
    test(`${name} before the child OPEN is a protocol error`, async () => {
      const [mux, raw] = rawPair();
      const parent = mux.open(empty);
      await parent.send(new Uint8Array(32752));
      await parent.send(new Uint8Array(32752));
      void parent.send(empty).catch(() => {});
      const child = parent.openAfter(empty);
      raw.sendBytes(new MuxFrame(command, child.id, empty).encode());
      await settle();
      expect(mux.isOpen).toBe(false);
      expectCode(await mux.done, StatusCode.protocolError);
    });
  }

  test('CREDIT for a child whose OPEN has not gone out is ignored', async () => {
    const [mux, raw] = rawPair();
    const parent = mux.open(empty);
    await parent.send(new Uint8Array(32752));
    await parent.send(new Uint8Array(32752));
    void parent.send(empty).catch(() => {});
    const child = parent.openAfter(empty);
    raw.send(creditHex(child.id, 100));
    await settle();
    expect(child.sendWindow).toBe(MuxLimits.defaultInitialWindow);
    expect(mux.isOpen).toBe(true);
    await mux.close();
  });

  test('the OPEN goes at once when nothing waits on the parent', async () => {
    const [a, b, gate] = gatedPair();
    gate.allow(1000);
    await a.ping();
    const parent = a.open(empty);
    const child = parent.openAfter(Uint8Array.of(9));
    await child.send(tagged(child, 0));
    await settle();
    expect(commands(gate)).toEqual([
      [MuxCommand.open, parent.id],
      [MuxCommand.open, child.id],
      [MuxCommand.data, child.id],
    ]);
    expect(b.isOpen).toBe(true);
    await a.close();
  });

  test(
    "the OPEN keeps its place behind DATA the scheduler holds, and the new channel's frames " +
      'wait for it',
    async () => {
      const [a, b, gate] = gatedPair();
      const opened = takeIncoming(b);
      const parent = a.open(empty);
      await settle();
      await parent.send(tagged(parent, 0));
      await parent.send(tagged(parent, 1));
      const child = parent.openAfter(empty);
      await parent.send(tagged(parent, 2));
      // Taken by the child's window, held until its OPEN is written.
      await child.send(tagged(child, 0));
      void child.close();
      expect(child.canSend).toBe(false);
      await settle();
      expect(commands(gate)).toEqual([[MuxCommand.open, parent.id]]);
      gate.allow(10);
      await settle();
      expect(commands(gate)).toEqual([
        [MuxCommand.open, parent.id],
        [MuxCommand.data, parent.id],
        [MuxCommand.data, parent.id],
        [MuxCommand.open, child.id],
        [MuxCommand.data, parent.id],
        [MuxCommand.data, child.id],
        [MuxCommand.close, child.id],
      ]);
      expect(opened.map((c) => c.id)).toEqual([parent.id, child.id]);
      expect(b.isOpen).toBe(true);
      await a.close();
    },
  );

  test('the OPEN waits behind DATA waiting for credit', async () => {
    const [mux, raw] = rawPair();
    raw.send(limitsHex(64));
    await settleRaw(raw);
    const parent = mux.open(empty);
    expect((await raw.next()).command).toBe(MuxCommand.open);
    // 16 + 16 each: the window takes two.
    await parent.send(new Uint8Array(16));
    await parent.send(new Uint8Array(16));
    const third = parent.send(new Uint8Array(16));
    const child = parent.openAfter(empty);
    void child.send(new Uint8Array(5));
    expect((await raw.next()).command).toBe(MuxCommand.data);
    expect((await raw.next()).command).toBe(MuxCommand.data);
    await settle();
    // CREDIT for one: the third goes, then the OPEN, then the child's DATA.
    raw.send(creditHex(parent.id, 32));
    await third;
    const frames = [await raw.next(), await raw.next(), await raw.next()];
    expect(frames.map((f) => [f.command, f.channelId])).toEqual([
      [MuxCommand.data, parent.id],
      [MuxCommand.open, child.id],
      [MuxCommand.data, child.id],
    ]);
    await mux.close();
  });

  for (const priority of ['normal', 'bulk'] as const satisfies readonly MuxPriority[]) {
    test(`a ${priority} send before OPEN copies the caller buffer`, async () => {
      const [a, b, gate] = gatedPair();
      await a.ping();
      const parent = a.open(empty);
      await parent.send(new Uint8Array(1));
      const child = parent.openAfter(empty);
      child.priority = priority;
      const payload = Uint8Array.of(1, 2, 3);
      await child.send(payload);
      payload.fill(9);
      gate.allow(10);
      await settle();
      const data = gate.on(child.id).filter(isData);
      expect(data).toHaveLength(1);
      expect([...(data[0] as MuxFrame).payload]).toEqual([1, 2, 3]);
      expect(b.isOpen).toBe(true);
      await a.close();
    });
  }

  test('a parent closed before the OPEN went out ends the new channel unopened, its id free', async () => {
    const [mux, raw] = rawPair();
    raw.send(limitsHex(64));
    await settleRaw(raw);
    const parent = mux.open(empty);
    await raw.next();
    await parent.send(new Uint8Array(16));
    await parent.send(new Uint8Array(16));
    const waiting = parent.send(new Uint8Array(16));
    const child = parent.openAfter(empty);
    const childSend = child.send(new Uint8Array(5));
    expect(mux.openChannelCount).toBe(2);
    await raw.next();
    await raw.next();
    // The peer closes the parent.
    raw.send('22 02 00');
    await expectRejectsStatus(waiting, StatusCode.failedPrecondition);
    // Handed to the connection (the window took it), then dropped with
    // the channel.
    await childSend;
    expect((await child.done).known).toBe(StatusCode.cancelled);
    expect(child.state).toBe('closed');
    expect(mux.openChannelCount).toBe(0);
    // Only the parent's confirming CLOSE went out.
    expect(await raw.nextHex()).toBe('22 02 00');
    expect([...mux.channels]).toEqual([]);
    const next = mux.open(empty);
    expect((await raw.next()).channelId).toBe(next.id);
    await mux.close();
  });

  test(
    'a close of a channel whose OPEN still waits is not timed until the OPEN goes out; ' +
      'nothing leaks when it never does',
    async () => {
      const [mux, raw] = rawPair({ options: { ...rawOptions, closeConfirmTimeout: 30 } });
      raw.send(limitsHex(64));
      await settleRaw(raw);
      const parent = mux.open(empty);
      await raw.next();
      await parent.send(new Uint8Array(16));
      await parent.send(new Uint8Array(16));
      void parent.send(new Uint8Array(16)).catch(() => {});
      const child = parent.openAfter(empty);
      await child.send(new Uint8Array(5));
      void child.close();
      await raw.next();
      await raw.next();
      // Longer than the confirmation timeout: the child is not abandoned.
      await clockOf(mux).elapse(80);
      expect(child.state).toBe('halfClosedLocal');
      expect(mux.unconfirmedCloseCount).toBe(0);
      raw.send('22 02 00');
      expect((await child.done).known).toBe(StatusCode.cancelled);
      expect(mux.unconfirmedCloseCount).toBe(0);
      expect(mux.openChannelCount).toBe(0);
      await mux.close();
    },
  );

  test('refused once the parent can no longer send', async () => {
    const [a, b] = muxPair();
    const parent = a.open(empty);
    void parent.close();
    expectThrowsStatus(() => parent.openAfter(empty), StatusCode.failedPrecondition);
    await a.close();
    await b.close();
  });
});
