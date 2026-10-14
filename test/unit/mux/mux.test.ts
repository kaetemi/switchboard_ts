// The frame mux: channels from both sides, the close handshake, LIMITS,
// channel ids, protocol errors, the control channel, GOAWAY, byte
// counters, keep-alive, the end of a connection and transport failures.
// Ports `test/mux_test.dart`; the harness is `support.ts`
// (`mux_harness.dart`). Time is a SteppingClock: the Dart tests that wait
// on real time (keep-alive, the GOAWAY grace) elapse it instead.
import { describe, expect, test } from 'vitest';
import { hexBytes, hexString } from '../../../src/core/bytes.ts';
import type { MuxChannel } from '../../../src/core/mux/contract.ts';
import { FrameMuxConnection } from '../../../src/core/mux/frame-mux-connection.ts';
import {
  MuxCommand,
  MuxControlMessage,
  MuxControlType,
  MuxFrame,
  MuxLimits,
} from '../../../src/core/mux/mux-frame.ts';
import { Status, StatusCode, SwitchboardError } from '../../../src/core/status.ts';
import { StreamFraming } from '../../../src/core/transport/stream-framing.ts';
import { StreamTransport } from '../../../src/core/transport/stream-transport.ts';
import type { Logger, LogLevel } from '../../../src/core/util/logger.ts';
import {
  AnsweringTransport,
  ByteDrain,
  ByteFeed,
  clockOf,
  collect,
  elapseUntil,
  empty,
  expectBytes,
  expectCode,
  expectRejectsStatus,
  expectStatus,
  expectThrowsStatus,
  firstIncoming,
  firstSubframe,
  framesIn,
  Incoming,
  isControl,
  memoryPair,
  muxPair,
  outcome,
  pattern,
  quiet,
  RawPeer,
  rawOptions,
  rawPair,
  rejection,
  SteppingClock,
  Subframes,
  settle,
  streamOf,
  Tap,
  takeIncoming,
  tracked,
} from './support.ts';

const ping = MuxControlType.ping;

test('a synchronous keep-alive reply and close leave no timer', async () => {
  // Dart's peer answers the keep-alive PING during its write and the test
  // closes the connection from there. Here the answer is queued as the
  // PING is written and the close happens inside the write too; the mux
  // reads the PONG only after it closed.
  const clock = new SteppingClock();
  let mux: FrameMuxConnection | undefined;
  let closed: Promise<void> | undefined;
  const transport = new AnsweringTransport((bytes) => {
    const frame = MuxFrame.decode(bytes);
    if (frame.channelId !== 0) {
      return [];
    }
    const message = MuxControlMessage.decode(frame.payload);
    if (message.knownType !== ping) {
      return [];
    }
    closed = mux?.close();
    return [MuxControlMessage.pong(message.payload).toFrame().encode()];
  });
  mux = new FrameMuxConnection(transport, {
    ...rawOptions,
    keepAliveInterval: 5,
    clock,
    isInitiator: true,
  });
  await clock.elapse(5);
  expect(closed).toBeDefined();
  await closed;
  await settle();
  expect(clock.pending).toBe(0);
});

describe('channels', () => {
  test('a synchronous CLOSE confirmation leaves no timer', async () => {
    // The confirmation is queued as the CLOSE is written; the mux handles
    // it once it reads it, not during the write, so the channel is closed
    // after a turn rather than at once.
    const clock = new SteppingClock();
    const transport = new AnsweringTransport((bytes) => {
      const frame = MuxFrame.decode(bytes);
      return frame.command === MuxCommand.close ? [MuxFrame.close(frame.channelId).encode()] : [];
    });
    const mux = new FrameMuxConnection(transport, { ...rawOptions, clock, isInitiator: true });
    const channel = mux.open(empty);
    void channel.close();
    await settle();
    expect(channel.state).toBe('closed');
    expect(clock.pending).toBe(0);
    await mux.close();
    expect(clock.pending).toBe(0);
  });

  test('open from both sides, payloads and data both ways', async () => {
    const [a, b] = muxPair();
    const aIncoming = new Incoming(a);
    const bIncoming = new Incoming(b);
    expect(a.isInitiator).toBe(true);
    expect(b.isInitiator).toBe(false);

    const ca = a.open(hexBytes('AA'));
    // A writable hands a chunk to the channel once it has started, a turn
    // after it is made: taken early, so that its write below goes between
    // the two sends around it, as Dart's synchronous sink.add does.
    const sink = ca.writable.getWriter();
    expect(ca.id).toBe(2);
    expect(ca.isLocallyOpened).toBe(true);
    expect(ca.connection).toBe(a);
    expect(ca.state).toBe('open');
    expect(ca.canSend).toBe(true);
    const cb = b.open(hexBytes('BB CC'));
    expect(cb.id).toBe(3);

    const ra = await bIncoming.next();
    expect(ra.id).toBe(2);
    expect(ra.isLocallyOpened).toBe(false);
    expect(hexString(ra.openPayload)).toBe('AA');
    const rb = await aIncoming.next();
    expect(rb.id).toBe(3);
    expect(hexString(rb.openPayload)).toBe('BB CC');

    const raData = new Subframes(ra);
    const caData = new Subframes(ca);
    void ca.send(hexBytes('01'));
    void sink.write(hexBytes('02 03'));
    void ca.send(empty);
    void ra.send(hexBytes('04'));
    expect(await raData.nextHex()).toBe('01');
    expect(await raData.nextHex()).toBe('02 03');
    expect(await raData.next()).toHaveLength(0);
    expect(await caData.nextHex()).toBe('04');

    expect(a.openChannelCount).toBe(2);
    expect(b.openChannelCount).toBe(2);
    expect(new Set(a.channels)).toEqual(new Set([ca, rb]));
    expect(new Set(b.channels)).toEqual(new Set([cb, ra]));
    await a.close();
    await b.done;
  });

  test('close handshake started by the opener', async () => {
    const [a, b] = muxPair();
    const bIncoming = new Incoming(b);
    const ca = a.open(empty);
    const ra = await bIncoming.next();
    const raData = collect(ra);

    const closing = ca.close();
    expect(ca.state).toBe('halfClosedLocal');
    expect(ca.canSend).toBe(false);
    expectThrowsStatus(() => ca.send(empty), StatusCode.failedPrecondition);
    await closing;
    expect(ca.state).toBe('closed');
    expectStatus(await ca.done, Status.ok);
    expect(await raData).toEqual([]);
    expectStatus(await ra.done, Status.ok);
    expect(ra.state).toBe('closed');
    expect(a.openChannelCount).toBe(0);
    expect(b.openChannelCount).toBe(0);
    expect(a.isOpen).toBe(true);
    await a.close();
  });

  test('close handshake started by the accepting side', async () => {
    const [a, b] = muxPair();
    const bIncoming = new Incoming(b);
    const ca = a.open(empty);
    const ra = await bIncoming.next();
    const caData = collect(ca);

    await ra.close(Status.of(StatusCode.notFound, 'x'));
    expectStatus(await ra.done, Status.of(StatusCode.notFound, 'x'));
    expect(await caData).toEqual([]);
    expectStatus(await ca.done, Status.of(StatusCode.notFound, 'x'));
    expect(ca.state).toBe('closed');
    expectThrowsStatus(() => ca.send(empty), StatusCode.failedPrecondition);
    // Dart's sink drops silently once the peer closed; a write to the
    // writable rejects and errors it (design section 12, item 8).
    const writer = ca.writable.getWriter();
    await expectRejectsStatus(writer.write(hexBytes('01')), StatusCode.failedPrecondition);
    await rejection(writer.close());
    expect(a.isOpen).toBe(true);
    await a.close();
  });

  test('simultaneous close from both sides', async () => {
    const [a, b] = muxPair();
    const bIncoming = new Incoming(b);
    const ca = a.open(empty);
    const ra = await bIncoming.next();
    const c1 = ca.close(Status.of(StatusCode.aborted));
    const c2 = ra.close();
    await Promise.all([c1, c2]);
    expectCode(await ca.done, StatusCode.aborted);
    expectCode(await ra.done, StatusCode.aborted);
    await a.ping();
    expect(a.isOpen).toBe(true);
    expect(b.isOpen).toBe(true);
    expect(a.openChannelCount + b.openChannelCount).toBe(0);
    await b.close();
  });

  test('data in flight is delivered after a local close', async () => {
    const [a, b] = muxPair();
    const bIncoming = new Incoming(b);
    const ca = a.open(empty);
    const ra = await bIncoming.next();
    const caData = collect(ca);
    void ra.send(hexBytes('05'));
    void ra.send(hexBytes('06'));
    await ca.close();
    expect((await caData).map(hexString)).toEqual(['05', '06']);
    await a.close();
  });

  test('sink close, sink done and addError', async () => {
    // The sink is the writable: `close` is `close()`, `done` the writer's
    // `closed`, `addError` an `abort` with the error.
    const [a, b] = muxPair();
    const bIncoming = new Incoming(b);
    const c1 = a.open(empty);
    const c2 = a.open(empty);
    const r1 = await bIncoming.next();
    const r2 = await bIncoming.next();

    const w1 = c1.writable.getWriter();
    await w1.close();
    await w1.closed;
    expectStatus(await r1.done, Status.ok);
    expect(await rejection(w1.write(empty))).toBeInstanceOf(TypeError);

    void c2.writable.abort(SwitchboardError.of(StatusCode.permissionDenied, 'no'));
    expectStatus(await r2.done, Status.of(StatusCode.permissionDenied, 'no'));
    expectStatus(await c2.done, Status.of(StatusCode.permissionDenied, 'no'));

    const c3 = a.open(empty);
    const r3 = await bIncoming.next();
    void c3.writable.abort(new Error('bug'));
    expectCode(await r3.done, StatusCode.internal);
    await a.close();
  });

  test('sink addStream sends every subframe', async () => {
    const [a, b] = muxPair();
    const bIncoming = new Incoming(b);
    const ca = a.open(empty);
    const ra = await bIncoming.next();
    const received = collect(ra);
    await streamOf([hexBytes('01'), hexBytes('02')]).pipeTo(ca.writable, {
      preventClose: true,
    });
    await ca.writable.close();
    expect((await received).map(hexString)).toEqual(['01', '02']);
    await a.close();
  });

  test('close is idempotent and completes after the fact', async () => {
    const [a, b] = muxPair();
    const bIncoming = new Incoming(b);
    const ca = a.open(empty);
    await bIncoming.next();
    const first = ca.close(Status.of(StatusCode.cancelled));
    const second = ca.close();
    await Promise.all([first, second]);
    await ca.close();
    expectCode(await ca.done, StatusCode.cancelled);
    expect(() => ca.close(new Status(300))).toThrow(RangeError);
    await a.close();
  });

  test('application status codes are refused for CLOSE', async () => {
    const [a] = muxPair();
    const ca = a.open(empty);
    expect(() => ca.close(new Status(256))).toThrow(RangeError);
    expect(ca.state).toBe('open');
    await a.close();
  });

  test('open payload is copied', async () => {
    const [a, b] = muxPair();
    const bIncoming = new Incoming(b);
    const payload = hexBytes('01 02');
    const ca = a.open(payload);
    payload[0] = 0xff;
    expect(hexString(ca.openPayload)).toBe('01 02');
    expect(hexString((await bIncoming.next()).openPayload)).toBe('01 02');
    await a.close();
  });

  test('wire bytes of a channel lifetime', async () => {
    const [mux, raw] = rawPair({ muxIsInitiator: false });
    const incoming = new Incoming(mux);

    const channel = mux.open(hexBytes('AA'));
    expect(await raw.nextHex()).toBe('12 03 00 AA');
    void channel.send(hexBytes('01 02 03'));
    expect(await raw.nextHex()).toBe('02 03 00 01 02 03');
    const closed = channel.close(Status.of(StatusCode.notFound, 'x'));
    expect(await raw.nextHex()).toBe('22 03 00 05 00 78');
    raw.send('22 03 00');
    await closed;
    expectStatus(await channel.done, Status.of(StatusCode.notFound, 'x'));

    raw.send('12 02 00 BB');
    const remote = await incoming.next();
    expect(remote.id).toBe(2);
    raw.send('02 02 00 07');
    expect(hexString(await firstSubframe(remote))).toBe('07');
    raw.send('22 02 00 05 00 78');
    expect(await raw.nextHex()).toBe('22 02 00');
    expectStatus(await remote.done, Status.of(StatusCode.notFound, 'x'));

    const plain = mux.open(empty);
    expect(await raw.nextHex()).toBe('12 05 00');
    void plain.close();
    expect(await raw.nextHex()).toBe('22 05 00');
    raw.send('22 05 00 00 00');
    expectStatus(await plain.done, Status.ok);
    await mux.close();
  });
});

describe('rejection and limits', () => {
  test('peer OPEN beyond maxChannels gets CLOSE RESOURCE_EXHAUSTED', async () => {
    const [a, b] = muxPair({ acceptor: { ...quiet, maxChannels: 1, announceLimits: false } });
    const bIncoming = new Incoming(b);
    const c1 = a.open(empty);
    const c2 = a.open(empty);
    // DATA racing the rejection is not an error.
    void c2.send(hexBytes('01'));
    const c2Data = collect(c2);
    expectCode(await c2.done, StatusCode.resourceExhausted);
    expect(await c2Data).toEqual([]);
    const r1 = await bIncoming.next();
    expect(r1.id).toBe(c1.id);
    await a.ping();
    expect(b.openChannelCount).toBe(1);
    expect(a.openChannelCount).toBe(1);
    expect(a.isOpen).toBe(true);

    await c1.close();
    const c3 = a.open(empty);
    expect((await bIncoming.next()).id).toBe(c3.id);
    await a.close();
  });

  test('LIMITS announcement and peerLimits', async () => {
    const [a, b] = muxPair({ initiator: { ...quiet, maxFrameSize: 4096, maxChannels: 7 } });
    expect(a.peerLimits).toBeUndefined();
    await b.ping();
    expect(b.peerLimits?.equals(new MuxLimits({ maxFrameSize: 4096, maxChannels: 7 }))).toBe(true);
    expect(
      a.peerLimits?.equals(new MuxLimits({ maxFrameSize: 1024 * 1024, maxChannels: 65536 })),
    ).toBe(true);
    await a.close();
  });

  test('LIMITS is the first frame, byte exact', async () => {
    const [mux, raw] = rawPair({
      options: { ...rawOptions, announceLimits: true, maxFrameSize: 4096, maxChannels: 7 },
    });
    expect(await raw.nextHex()).toBe('02 00 00 04 00 10 00 00 07 00 00 00 00 00 01 00');
    await mux.close();
  });

  test('no LIMITS when disabled', async () => {
    const [a, b] = muxPair({ initiator: { ...quiet, announceLimits: false } });
    await b.ping();
    expect(b.peerLimits).toBeUndefined();
    expect(a.peerLimits).toBeDefined();
    await a.close();
  });

  test('LIMITS mid-connection applies to later frames', async () => {
    const [mux, raw] = rawPair();
    const announce = (maxFrameSize: number, maxChannels: number): void =>
      raw.sendBytes(
        MuxControlMessage.limits(new MuxLimits({ maxFrameSize, maxChannels })).toFrame().encode(),
      );
    // Frames are handled in order: once the PONG is back, so is LIMITS.
    const settled = async (): Promise<void> => {
      raw.send('02 00 00 01 5E');
      expect(await raw.nextHex()).toBe('02 00 00 02 5E');
    };

    const channel = mux.open(empty);
    expect(await raw.nextHex()).toBe('12 02 00');
    void channel.send(new Uint8Array(200));
    expect((await raw.next()).payload).toHaveLength(200);

    announce(64, 1);
    await settled();
    expect(mux.peerLimits?.equals(new MuxLimits({ maxFrameSize: 64, maxChannels: 1 }))).toBe(true);
    expectThrowsStatus(() => channel.send(new Uint8Array(62)), StatusCode.frameTooLarge);
    void channel.send(new Uint8Array(61));
    expect((await raw.next()).encode()).toHaveLength(64);
    expectThrowsStatus(() => mux.open(empty), StatusCode.resourceExhausted);
    // The CLOSE reason is cut to the new limit.
    const closing = channel.close(Status.of(StatusCode.cancelled, 'z'.repeat(100)));
    const close = await raw.next();
    expect(close.command).toBe(MuxCommand.close);
    expect(close.encode()).toHaveLength(64);
    expectStatus(Status.decode(close.payload), Status.of(StatusCode.cancelled, 'z'.repeat(59)));
    raw.send('22 02 00');
    await closing;

    // Raised again: larger frames and more channels are back.
    announce(4096, 8);
    await settled();
    const second = mux.open(new Uint8Array(1000));
    expect((await raw.next()).payload).toHaveLength(1000);
    void second.send(new Uint8Array(4000));
    expect((await raw.next()).payload).toHaveLength(4000);
    void second.close();
    expect(await raw.nextHex()).toBe('22 04 00');
    raw.send('22 04 00');
    await second.done;

    // And GOAWAY fits the latest limit too.
    announce(32, 8);
    await settled();
    const goingAway = mux.goAway(Status.of(StatusCode.unavailable, 'w'.repeat(100)));
    const goAway = await raw.next();
    expect(goAway.encode()).toHaveLength(32);
    expectStatus(
      MuxControlMessage.decode(goAway.payload).goAwayStatus,
      Status.of(StatusCode.unavailable, 'w'.repeat(26)),
    );
    await goingAway;
    expect(await raw.rest()).toEqual([]);
  });

  test('peer channel limit caps open() locally', async () => {
    const [a, b] = muxPair({ acceptor: { ...quiet, maxChannels: 1 } });
    await a.ping();
    a.open(empty);
    expectThrowsStatus(() => a.open(empty), StatusCode.resourceExhausted);
    await b.close();
  });

  test('peer frame limit is enforced on send and open', async () => {
    const [a, b] = muxPair({ acceptor: { ...quiet, maxFrameSize: 16 } });
    await a.ping();
    const channel = a.open(new Uint8Array(13));
    void channel.send(new Uint8Array(13));
    expectThrowsStatus(() => channel.send(new Uint8Array(14)), StatusCode.frameTooLarge);
    expectThrowsStatus(() => a.open(new Uint8Array(14)), StatusCode.frameTooLarge);
    await a.ping();
    expect(b.isOpen).toBe(true);
    await b.close();
  });

  test('frame over maxFrameSize ends the connection', async () => {
    const [mux, raw] = rawPair({ options: { ...rawOptions, maxFrameSize: 16 } });
    raw.send('12 03 00');
    raw.send(`02 03 00 ${'00 '.repeat(14)}`);
    const goAway = await raw.nextControl(MuxControlType.goAway);
    expect(goAway.goAwayStatus.known).toBe(StatusCode.frameTooLarge);
    expectCode(await mux.done, StatusCode.frameTooLarge);
  });

  test('frame over the stream transport limit sends GOAWAY', async () => {
    const clock = new SteppingClock();
    const toMux = new ByteFeed();
    const fromMux = new ByteDrain();
    const mux = new FrameMuxConnection(
      new StreamTransport(toMux, fromMux, { maxFrameSize: 64, clock }),
      { ...rawOptions, clock, isInitiator: true },
    );
    toMux.add(StreamFraming.encodePreamble());
    // A length of 4096 and no body.
    toMux.add(hexBytes('00 10 00 00'));
    expectCode(await mux.done, StatusCode.frameTooLarge);
    await clock.elapse(0);
    const frames = framesIn(fromMux.chunks);
    expect(frames).toHaveLength(1);
    const goAway = MuxControlMessage.decode(MuxFrame.decode(frames[0] as Uint8Array).payload);
    expect(goAway.knownType).toBe(MuxControlType.goAway);
    expect(goAway.goAwayStatus.known).toBe(StatusCode.frameTooLarge);
    toMux.end();
  });

  test('cancelling incoming rejects further OPENs', async () => {
    const [a, b] = muxPair();
    a.open(empty);
    const first = await firstIncoming(b);
    expect(first.id).toBe(2);
    const late = a.open(empty);
    expectCode(await late.done, StatusCode.unavailable);
    await a.close();
  });
});

describe('channel ids', () => {
  /** Opens [count] channels on [mux] in one go. */
  function openMany(mux: FrameMuxConnection, count: number): MuxChannel[] {
    return Array.from({ length: count }, () => mux.open(empty));
  }

  test('parity and increment', async () => {
    const [a, b] = muxPair();
    expect([0, 1, 2].map(() => a.open(empty).id)).toEqual([2, 4, 6]);
    expect([0, 1, 2].map(() => b.open(empty).id)).toEqual([3, 5, 7]);
    await a.close();
  });

  test('reuse after mutual close, wrapping a short id space', async () => {
    const [a, b] = muxPair({
      initiator: { ...quiet, shortIdsOnly: true, announceLimits: false },
      acceptor: { ...quiet, maxChannels: 0, shortIdsOnly: true },
    });
    const remote = takeIncoming(b);
    // Even ids 2 to 0xFFFE.
    const count = Math.floor(0xfffe / 2);
    const channels = openMany(a, count);
    expect(channels[0]?.id).toBe(2);
    expect(channels.at(-1)?.id).toBe(0xfffe);
    expectThrowsStatus(() => a.open(empty), StatusCode.resourceExhausted);
    await a.ping();
    expect(remote).toHaveLength(count);

    const victim = channels[99] as MuxChannel;
    expect(victim.id).toBe(200);
    const closing = victim.close();
    // Half closed: the id is not free yet.
    expectThrowsStatus(() => a.open(empty), StatusCode.resourceExhausted);
    await closing;
    const reused = a.open(hexBytes('01'));
    expect(reused.id).toBe(200);
    await a.ping();
    expect(remote.at(-1)?.id).toBe(200);
    expect(hexString(remote.at(-1)?.openPayload ?? empty)).toBe('01');
    expect(a.isOpen && b.isOpen).toBe(true);
    await a.close();
  });

  test('allocation skips reserved short id 0xFFFF and wraps', async () => {
    const [a, b] = muxPair({ acceptor: { ...quiet, shortIdsOnly: true } });
    b.nextChannelIdForTesting = 0xfffd;
    expect(b.open(empty).id).toBe(0xfffd);
    expect(b.open(empty).id).toBe(3);
    await a.close();
  });

  test('allocation skips reserved long ids and wraps', async () => {
    const unlimited = { ...quiet, maxChannels: 0 };
    const [a, b] = muxPair({ initiator: unlimited, acceptor: unlimited });
    const atB = takeIncoming(b);
    const atA = takeIncoming(a);
    // The long range is only used once every short id is in use.
    openMany(a, 0x7fff);
    a.nextChannelIdForTesting = 0xfffefffffffe;
    expect(a.open(empty).id).toBe(0xfffefffffffe);
    expect(a.open(empty).id).toBe(0xffff00000002);
    a.nextChannelIdForTesting = 0xfffffffffffe;
    expect(a.open(empty).id).toBe(0xfffffffffffe);
    // Wraps within the long range.
    expect(a.open(empty).id).toBe(0x10000);
    await a.ping();
    expect(atB.slice(0x7fff).map((c) => c.id)).toEqual([
      0xfffefffffffe, 0xffff00000002, 0xfffffffffffe, 0x10000,
    ]);

    // Odd ids 3 to 0xFFFD, skipping the reserved 0xFFFF.
    openMany(b, 0x7ffe);
    b.nextChannelIdForTesting = 0xfffffffffffd;
    expect(b.open(empty).id).toBe(0xfffffffffffd);
    expect(b.open(empty).id).toBe(0x10001);
    await b.ping();
    expect(atA.slice(0x7ffe).map((c) => c.id)).toEqual([0xfffffffffffd, 0x10001]);
    expect(a.isOpen).toBe(true);
    await a.close();
  });

  test('long ids use the long form on the wire', async () => {
    const [mux, raw] = rawPair();
    openMany(mux, 0x7fff);
    for (let i = 0; i < 0x7fff; i++) {
      await raw.frames.nextBytes();
    }
    mux.open(hexBytes('AA'));
    expect(await raw.nextHex()).toBe('10 00 00 01 00 00 00 AA');
    await mux.close();
  });

  test('ids wrap within the short range first', async () => {
    const [a, b] = muxPair({ acceptor: { ...quiet, maxChannels: 0 } });
    takeIncoming(b);
    const channels = openMany(a, 0x7fff);
    expect(channels[0]?.id).toBe(2);
    expect(channels.at(-1)?.id).toBe(0xfffe);
    // Every short id of our parity is in use: the next one is long.
    const long = a.open(empty);
    expect(long.id).toBe(0x10000);
    // A freed short id is preferred over the long range again.
    const freed = channels[10] as MuxChannel;
    await freed.close();
    expect(a.open(empty).id).toBe(freed.id);
    expect(a.open(empty).id).toBe(0x10002);
    await a.ping();
    expect(a.isOpen && b.isOpen).toBe(true);
    await a.close();
  });

  test('ids stay short over many channels to a short-id-only peer', async () => {
    const [a, b] = muxPair({ acceptor: { ...quiet, shortIdsOnly: true } });
    takeIncoming(b);
    let largest = 0;
    // 40000 channels opened and closed, more than there are short ids.
    for (let round = 0; round < 40; round++) {
      const channels = Array.from({ length: 1000 }, () => a.open(empty));
      for (const channel of channels) {
        largest = channel.id > largest ? channel.id : largest;
      }
      await Promise.all(channels.map((channel) => channel.close()));
      for (const channel of channels) {
        expectStatus(await channel.done, Status.ok);
      }
    }
    expect(largest).toBe(0xfffe);
    await a.ping();
    expect(a.isOpen && b.isOpen).toBe(true);
    expect(a.open(empty).id).toBeLessThan(0x10000);
    await a.close();
  });

  test('a long form id addresses a channel opened in short form', async () => {
    const [mux, raw] = rawPair();
    raw.send('12 03 00');
    const channel = await firstIncoming(mux);
    const data = new Subframes(channel);
    raw.send('00 03 00 00 00 00 00 77');
    expect(await data.nextHex()).toBe('77');
    // CLOSE in the long form too.
    raw.send('20 03 00 00 00 00 00');
    expect(await raw.nextHex()).toBe('22 03 00');
    expectStatus(await channel.done, Status.ok);
    expect(await data.hasNext()).toBe(false);
    await mux.close();
  });

  test('shortIdsOnly refuses long form frames with UNSUPPORTED', async () => {
    const [mux, raw] = rawPair({ options: { ...rawOptions, shortIdsOnly: true } });
    raw.send('10 03 00 00 00 00 00');
    const goAway = await raw.nextControl(MuxControlType.goAway);
    expect(goAway.goAwayStatus.known).toBe(StatusCode.unsupported);
    expectCode(await mux.done, StatusCode.unsupported);
  });
});

describe('protocol errors', () => {
  const cases: readonly (readonly [string, readonly string[]])[] = [
    ['DATA on an unknown channel', ['02 03 00 01']],
    ['OPEN on an id in use', ['12 03 00', '12 03 00']],
    ['OPEN with the wrong parity', ['12 02 00']],
    ['command 3', ['32 02 00']],
    ['OPEN on channel 0', ['12 00 00']],
    ['CLOSE on channel 0', ['22 00 00']],
    ['DATA on channel 1', ['02 01 00']],
    ['DATA on reserved id 0xFFFF', ['02 FF FF']],
    ['reserved flag', ['03 02 00']],
    ['truncated header', ['02 03']],
    ['CLOSE on an unknown channel', ['22 03 00']],
    ['DATA after the channel was closed', ['12 03 00', '22 03 00', '02 03 00']],
    ['CLOSE twice', ['12 03 00', '22 03 00', '22 03 00']],
    ['malformed CLOSE status', ['12 03 00', '22 03 00 05']],
    ['control message without type', ['02 00 00']],
    ['malformed GOAWAY', ['02 00 00 03 21']],
    ['malformed LIMITS', ['02 00 00 04 00 01']],
  ];

  for (const [name, frames] of cases) {
    test(`${name}: GOAWAY PROTOCOL_ERROR, close, stop processing`, async () => {
      const [mux, raw] = rawPair();
      const local = mux.open(empty);
      expect(await raw.nextHex()).toBe('12 02 00');
      for (const frame of frames) {
        raw.send(frame);
      }
      // Must not be answered: frames after the error are not processed.
      raw.send('02 00 00 01 AA');
      const rest = await raw.rest();
      const goAways = rest.filter((f) => isControl(f, MuxControlType.goAway));
      expect(goAways).toHaveLength(1);
      expect(MuxControlMessage.decode((goAways[0] as MuxFrame).payload).goAwayStatus.known).toBe(
        StatusCode.protocolError,
      );
      expect(rest.filter((f) => isControl(f, MuxControlType.pong))).toEqual([]);
      expect(isControl(rest.at(-1) as MuxFrame, MuxControlType.goAway)).toBe(true);
      expectCode(await mux.done, StatusCode.protocolError);
      expectCode(await local.done, StatusCode.connectionLost);
      expect(mux.isOpen).toBe(false);
    });
  }

  test('control payload over 1024 bytes', async () => {
    const [mux, raw] = rawPair();
    raw.send(`02 00 00 01 ${'00 '.repeat(1025)}`);
    const goAway = await raw.nextControl(MuxControlType.goAway);
    expect(goAway.goAwayStatus.known).toBe(StatusCode.protocolError);
    expectCode(await mux.done, StatusCode.protocolError);
  });

  test('OPEN on a locally half-closed id', async () => {
    const [mux, raw] = rawPair();
    raw.send('12 03 00');
    const channel = await firstIncoming(mux);
    void channel.close();
    expect(await raw.nextHex()).toBe('22 03 00');
    // DATA in flight after our CLOSE is fine.
    raw.send('02 03 00 01');
    raw.send('12 03 00');
    const goAway = await raw.nextControl(MuxControlType.goAway);
    expect(goAway.goAwayStatus.known).toBe(StatusCode.protocolError);
    expectCode(await mux.done, StatusCode.protocolError);
    expectCode(await channel.done, StatusCode.connectionLost);
  });

  test('a broken channel handler does not affect the connection', async () => {
    // Dart's listener throws inside its zone; here the reader of the
    // channel throws, which stops its own reading and nothing else.
    const [a, b] = muxPair();
    const bIncoming = new Incoming(b);
    const c1 = a.open(empty);
    const c2 = a.open(empty);
    const r1 = await bIncoming.next();
    const r2 = await bIncoming.next();
    const broken = (async () => {
      for await (const _ of r1.readable) {
        throw new Error('application bug');
      }
    })();
    const r2Data = new Subframes(r2);
    void c1.send(hexBytes('01'));
    void c2.send(hexBytes('02'));
    expect(await r2Data.nextHex()).toBe('02');
    await a.ping();
    expect(((await rejection(broken)) as Error).message).toBe('application bug');
    expect(a.isOpen && b.isOpen).toBe(true);
    await c1.close();
    expectStatus(await r1.done, Status.ok);
    await a.close();
  });
});

describe('control channel', () => {
  test('PING/PONG round trip and ping() duration', async () => {
    const [a, b] = muxPair();
    const rtt = await a.ping();
    expect(rtt).toBeGreaterThanOrEqual(0);
    expect(rtt).toBeLessThan(1000);
    const both = await Promise.all([b.ping(hexBytes('01')), b.ping(hexBytes('02')), b.ping()]);
    expect(both).toHaveLength(3);
    await a.close();
  });

  test('PING is answered with the same payload', async () => {
    const [mux, raw] = rawPair();
    raw.send('02 00 00 01 DE AD');
    expect(await raw.nextHex()).toBe('02 00 00 02 DE AD');
    // Unsolicited PONG and unknown types are ignored.
    raw.send('02 00 00 02 01');
    raw.send('02 00 00 7F 01 02');
    raw.send('02 00 00 01');
    expect(await raw.nextHex()).toBe('02 00 00 02');
    expect(mux.isOpen).toBe(true);
    await mux.close();
  });

  test('ping() sends PING and matches the PONG payload', async () => {
    const [mux, raw] = rawPair();
    const pong = mux.ping(hexBytes('BE EF'));
    expect(await raw.nextHex()).toBe('02 00 00 01 BE EF');
    raw.send('02 00 00 02 00');
    raw.send('02 00 00 02 BE EF');
    expect(typeof (await pong)).toBe('number');
    await mux.close();
  });

  test('PINGs with 126 to 1024 byte payloads are answered', async () => {
    const [mux, raw] = rawPair();
    for (const size of [126, 1000, 1024]) {
      const payload = Uint8Array.from({ length: size }, (_, i) => i & 0xff);
      raw.sendBytes(new MuxControlMessage(MuxControlType.ping, payload).toFrame().encode());
      const pong = MuxControlMessage.decode((await raw.next()).payload);
      expect(pong.knownType).toBe(MuxControlType.pong);
      expect(hexString(pong.payload)).toBe(hexString(payload));
    }
    expect(mux.isOpen).toBe(true);
    await mux.close();
  });

  test('keep-alive PONGs over 125 bytes are accepted', async () => {
    // The 125 byte PING limit binds senders; a receiver accepts control
    // payloads up to 1024 bytes, and any frame is a sign of life.
    const [mux, raw] = rawPair({
      options: { ...rawOptions, keepAliveInterval: 20, keepAliveTimeout: 200 },
    });
    const clock = clockOf(mux);
    for (const size of [126, 1024]) {
      const next = raw.next();
      await elapseUntil(clock, next);
      expect(isControl(await next, ping)).toBe(true);
      raw.sendBytes(
        MuxControlMessage.pong(Uint8Array.from({ length: size }, (_, i) => i & 0xff))
          .toFrame()
          .encode(),
      );
    }
    // Neither a protocol error nor a missed answer: the next frame is the
    // next probe, not GOAWAY or the end of the connection.
    const next = raw.next();
    await elapseUntil(clock, next);
    expect(isControl(await next, ping)).toBe(true);
    expect(mux.isOpen).toBe(true);
    await mux.close();
  });

  test('pending pings are bounded: the oldest fails with DEADLINE_EXCEEDED', async () => {
    // A peer that withholds PONGs cannot make ping() accumulate.
    const [mux, raw] = rawPair();
    const max = FrameMuxConnection.maxPendingPings;
    const outcomes = Array.from({ length: max + 1 }, () => outcome(mux.ping()));
    const first = (await outcomes[0]) as SwitchboardError;
    expect(first).toBeInstanceOf(SwitchboardError);
    expect(first.code).toBe(StatusCode.deadlineExceeded);
    const sent: Uint8Array[] = [];
    for (let i = 0; i <= max; i++) {
      sent.push((await raw.nextControl(ping)).payload);
    }
    // The others still wait for their own PONGs.
    raw.sendBytes(
      MuxControlMessage.pong(sent[1] as Uint8Array)
        .toFrame()
        .encode(),
    );
    expect(typeof (await outcomes[1])).toBe('number');
    await mux.close();
    for (const later of outcomes.slice(2)) {
      expect(await later).toBeInstanceOf(SwitchboardError);
    }
  });

  test('ping payload over 125 bytes is refused', async () => {
    const [a] = muxPair();
    expect(() => a.ping(new Uint8Array(126))).toThrow(RangeError);
    await a.ping(new Uint8Array(125));
    await a.close();
  });

  test('pending ping fails when the connection ends', async () => {
    const [mux, raw] = rawPair();
    const pong = mux.ping();
    await raw.next();
    await raw.close();
    await expectRejectsStatus(pong, StatusCode.connectionLost);
    await expectRejectsStatus(mux.ping(), StatusCode.failedPrecondition);
  });

  test('second GOAWAY is ignored', async () => {
    const [mux, raw] = rawPair();
    const incoming = new Incoming(mux);
    raw.send('12 03 00');
    raw.send('02 00 00 03 21 00');
    raw.send('02 00 00 03 05 00');
    raw.send('02 00 00 01 01');
    expect(await raw.nextHex()).toBe('02 00 00 02 01');
    expect(mux.peerGoingAway).toBe(true);
    const channel = await incoming.next();
    // Idle after GOAWAY: the receiver confirms the CLOSE, then closes.
    raw.send('22 03 00');
    expect((await raw.rest()).map((f) => hexString(f.encode()))).toEqual(['22 03 00']);
    expectStatus(await channel.done, Status.ok);
    expectStatus(await mux.done, Status.of(StatusCode.goingAway));
  });

  test('GOAWAY with another status is reported as GOING_AWAY', async () => {
    const [mux, raw] = rawPair();
    raw.send('02 00 00 03 20 00 78');
    const status = await mux.done;
    expect(status.known).toBe(StatusCode.goingAway);
    expect(status.reason).toContain('x');
    await raw.rest();
  });
});

describe('GOAWAY', () => {
  test('peer cannot open after GOAWAY; channels finish, then close', async () => {
    const [a, b] = muxPair();
    const bIncoming = new Incoming(b);
    const ca = a.open(empty);
    const rb = await bIncoming.next();
    const caData = new Subframes(ca);
    const rbData = new Subframes(rb);

    const goingAway = b.goAway();
    expect(b.isOpen).toBe(false);
    expectThrowsStatus(() => b.open(empty), StatusCode.failedPrecondition);
    await a.ping();
    expect(a.peerGoingAway).toBe(true);
    expect(b.peerGoingAway).toBe(false);
    expectThrowsStatus(() => a.open(empty), StatusCode.failedPrecondition);

    void ca.send(hexBytes('01'));
    expect(await rbData.nextHex()).toBe('01');
    void rb.send(hexBytes('02'));
    expect(await caData.nextHex()).toBe('02');

    await ca.close();
    await goingAway;
    expectCode(await a.done, StatusCode.goingAway);
    // The side that sent GOAWAY reports the status it sent.
    expectCode(await b.done, StatusCode.goingAway);
    expectStatus(await ca.done, Status.ok);
    expectStatus(await rb.done, Status.ok);
  });

  test('OPEN racing our GOAWAY is rejected with GOING_AWAY', async () => {
    const [a, b] = muxPair();
    const bIncoming = new Incoming(b);
    const keep = a.open(empty);
    const kept = await bIncoming.next();
    const goingAway = b.goAway();
    const racing = a.open(empty);
    expectCode(await racing.done, StatusCode.goingAway);
    expect(b.openChannelCount).toBe(1);
    await keep.close();
    await goingAway;
    expectStatus(await kept.done, Status.ok);
  });

  test('grace expiry closes with channels still open', async () => {
    const [a, b] = muxPair({ acceptor: { ...quiet, goAwayGrace: 30 } });
    const clock = clockOf(b);
    const bIncoming = new Incoming(b);
    const ca = a.open(empty);
    const rb = await bIncoming.next();
    const goingAway = tracked(b.goAway(Status.of(StatusCode.unavailable, 'maintenance')));
    await clock.elapse(29);
    expect(goingAway.settled).toBe(false);
    await clock.elapse(1);
    await goingAway.promise;
    expectCode(await rb.done, StatusCode.unavailable);
    expectCode(await ca.done, StatusCode.goingAway);
    const status = await a.done;
    expect(status.known).toBe(StatusCode.goingAway);
    expect(status.reason).toContain('maintenance');
  });

  test('goAway with no channels closes at once', async () => {
    const [mux, raw] = rawPair();
    await mux.goAway();
    const rest = await raw.rest();
    expect(rest.map((f) => hexString(f.encode()))).toEqual(['02 00 00 03 21 00']);
    expectCode(await mux.done, StatusCode.goingAway);
    await mux.goAway();
  });

  test('peerGoAwayStatus: the status the peer sent', async () => {
    const [a, b] = muxPair();
    let seen: Status | undefined;
    void a.peerGoAwayStatus.then((status) => {
      seen = status;
    });
    await a.ping();
    expect(seen).toBeUndefined();
    const keep = a.open(empty);
    const goingAway = b.goAway(Status.of(StatusCode.goingAway, 'deploy'));
    expectStatus(await a.peerGoAwayStatus, Status.of(StatusCode.goingAway, 'deploy'));
    expect(a.peerGoingAway).toBe(true);
    expect(a.isOpen).toBe(true);
    await keep.close();
    await goingAway;
    // An error GOAWAY keeps its code here; done reports GOING_AWAY.
    const [c, d] = muxPair();
    const failing = d.goAway(Status.of(StatusCode.resourceExhausted, 'x'));
    expectStatus(await c.peerGoAwayStatus, Status.of(StatusCode.resourceExhausted, 'x'));
    expectCode(await c.done, StatusCode.goingAway);
    await failing;
  });

  test('peerGoAwayStatus never completes without a GOAWAY', async () => {
    const [a, b] = muxPair();
    let completed = false;
    void a.peerGoAwayStatus.then(() => {
      completed = true;
    });
    await b.close();
    expectCode(await a.done, StatusCode.connectionLost);
    await settle();
    expect(completed).toBe(false);
    // The side that sent GOAWAY never sees one.
    const [c, d] = muxPair();
    let dCompleted = false;
    void d.peerGoAwayStatus.then(() => {
      dCompleted = true;
    });
    await d.goAway();
    await c.done;
    await settle();
    expect(dCompleted).toBe(false);
  });

  test('application codes are refused for GOAWAY', async () => {
    const [a] = muxPair();
    expect(() => a.goAway(new Status(1000))).toThrow(RangeError);
    expect(a.isOpen).toBe(true);
    await a.close();
  });
});

describe('byte counters', () => {
  test('sentBytes and receivedBytes count whole mux frames', async () => {
    const [a, b] = muxPair({ initiator: rawOptions, acceptor: rawOptions });
    expect(a.sentBytes).toBe(0);
    expect(a.receivedBytes).toBe(0);
    const bIncoming = new Incoming(b);
    const ca = a.open(hexBytes('AA BB'));
    void ca.send(new Uint8Array(100));
    const rb = await bIncoming.next();
    const data = new Subframes(rb);
    expect(await data.next()).toHaveLength(100);
    const sent =
      MuxFrame.open(2, hexBytes('AA BB')).encode().length +
      MuxFrame.data(2, new Uint8Array(100)).encode().length;
    expect(a.sentBytes).toBe(sent);
    expect(b.receivedBytes).toBe(sent);
    void rb.send(new Uint8Array(10));
    await firstSubframe(ca);
    expect(a.receivedBytes).toBe(MuxFrame.data(2, new Uint8Array(10)).encode().length);
    expect(b.sentBytes).toBe(a.receivedBytes);
    // Control frames count too.
    const before = a.sentBytes;
    await a.ping();
    expect(a.sentBytes).toBeGreaterThan(before);
    expect(b.receivedBytes).toBe(a.sentBytes);
    await a.close();
    await b.done;
  });
});

describe('keep-alive', () => {
  test('PING appears on an idle connection', async () => {
    const [mux, raw] = rawPair({
      options: { ...rawOptions, keepAliveInterval: 20, keepAliveTimeout: 5000 },
    });
    const clock = clockOf(mux);
    for (let i = 0; i < 2; i++) {
      const probe = raw.nextControl(ping);
      expect(await elapseUntil(clock, probe)).toBe(20);
      raw.sendBytes(
        MuxControlMessage.pong((await probe).payload)
          .toFrame()
          .encode(),
      );
    }
    expect(mux.isOpen).toBe(true);
    await mux.close();
  });

  test('traffic postpones keep-alive PINGs', async () => {
    const [mux, raw] = rawPair({ options: { ...rawOptions, keepAliveInterval: 80 } });
    const clock = clockOf(mux);
    // A frame every 5 ms for 100 ms.
    for (let i = 0; i < 20; i++) {
      raw.send('02 00 00 7F');
      await clock.elapse(5);
    }
    await mux.close();
    const rest = await raw.rest();
    expect(rest.filter((f) => isControl(f, ping))).toEqual([]);
  });

  test('any frame during the probe window cancels the timeout', async () => {
    const [mux, raw] = rawPair({
      options: { ...rawOptions, keepAliveInterval: 30, keepAliveTimeout: 100 },
    });
    const clock = clockOf(mux);
    let elapsed = 0;
    // Longer than the probe window: without the rule the first probe would
    // end the connection.
    while (elapsed < 250) {
      elapsed += await elapseUntil(clock, raw.nextControl(ping));
      await clock.elapse(20);
      elapsed += 20;
      // An unknown control type, not a PONG.
      raw.send('02 00 00 7F');
    }
    expect(mux.isOpen).toBe(true);
    await mux.close();
  });

  test('a dead peer leads to CONNECTION_LOST', async () => {
    const [mux, raw] = rawPair({
      options: { ...rawOptions, keepAliveInterval: 20, keepAliveTimeout: 20 },
    });
    const channel = mux.open(empty);
    const done = mux.done;
    expect(await elapseUntil(clockOf(mux), done)).toBe(40);
    const status = await done;
    expect(status.known).toBe(StatusCode.connectionLost);
    expectCode(await channel.done, StatusCode.connectionLost);
    const rest = await raw.rest();
    expect(rest.filter((f) => isControl(f, ping)).length).toBeGreaterThan(0);
  });

  test('two live peers keep the connection open', async () => {
    const options = { keepAliveInterval: 10, keepAliveTimeout: 20 };
    const [a, b] = muxPair({ initiator: options, acceptor: options });
    await clockOf(a).elapse(100);
    expect(a.isOpen && b.isOpen).toBe(true);
    await a.close();
    await b.done;
  });

  test('a connection that ends while it starts arms no keep-alive', async () => {
    // Dart's peer hangs up while the constructor writes LIMITS, before the
    // first timer is armed. Here the hang-up reaches the mux once it reads,
    // after the constructor: what is checked is that no timer is left.
    const clock = new SteppingClock();
    const transport = new AnsweringTransport((_frame, self) => {
      self.end();
      return [];
    });
    const mux = new FrameMuxConnection(transport, {
      keepAliveInterval: 5000,
      keepAliveTimeout: 1000,
      clock,
      isInitiator: true,
    });
    await settle();
    expect(mux.isOpen).toBe(false);
    // Past the bound of the close on the transport.
    await clock.elapse(2000);
    expect(clock.pending).toBe(0);
  });
});

describe('connection end', () => {
  test('transport drop ends every channel with CONNECTION_LOST', async () => {
    const clock = new SteppingClock();
    const [ta, tb] = memoryPair(clock);
    const tap = new Tap(tb);
    const a = new FrameMuxConnection(ta, { ...quiet, clock, isInitiator: true });
    const b = new FrameMuxConnection(tap, { ...quiet, clock, isInitiator: false });
    const aIncoming = new Incoming(a);
    const bIncoming = new Incoming(b);
    const local = a.open(empty);
    b.open(empty);
    const remote = await aIncoming.next();
    await bIncoming.next();
    const localData = collect(local);
    const closing = remote.close();

    await tap.close();
    expectStatus(await a.done, Status.of(StatusCode.connectionLost));
    expectStatus(await b.done, Status.of(StatusCode.connectionLost));
    await closing;
    for (const channel of [local, remote]) {
      expectCode(await channel.done, StatusCode.connectionLost);
      expect(channel.state).toBe('closed');
      await channel.close();
    }
    expect(await localData).toEqual([]);
    expect(a.openChannelCount).toBe(0);
    expect([...a.channels]).toEqual([]);
    expect(a.isOpen).toBe(false);
    expect(await aIncoming.hasNext()).toBe(false);
    expectThrowsStatus(() => a.open(empty), StatusCode.failedPrecondition);
    await a.close();
  });

  test('close() ends channels, is idempotent and never throws', async () => {
    const [a, b] = muxPair();
    const channel = a.open(empty);
    const c1 = a.close();
    const c2 = a.close();
    await Promise.all([c1, c2]);
    expectCode(await channel.done, StatusCode.connectionLost);
    expectCode(await b.done, StatusCode.connectionLost);
    await b.close();
    await a.goAway();
  });

  test('close() over a failed transport', async () => {
    const clock = new SteppingClock();
    const [ta, tb] = memoryPair(clock);
    const mux = new FrameMuxConnection(ta, { ...quiet, clock, isInitiator: true });
    const writer = tb.writable.getWriter();
    const gone = new Error('gone');
    void writer.abort(gone).catch(() => {});
    expect(await rejection(writer.closed)).toBe(gone);
    await mux.close();
    expectCode(await mux.done, StatusCode.connectionLost);
  });

  test('incoming channels are buffered until listened to', async () => {
    const [a, b] = muxPair();
    for (let i = 0; i < 3; i++) {
      void a.open(Uint8Array.of(i)).send(Uint8Array.of(i + 10));
    }
    await a.ping();
    const incoming = new Incoming(b);
    const channels = [await incoming.next(), await incoming.next(), await incoming.next()];
    await incoming.cancel();
    expect(channels.map((c) => c.openPayload[0])).toEqual([0, 1, 2]);
    for (const [i, channel] of channels.entries()) {
      expect((await firstSubframe(channel))[0]).toBe(i + 10);
    }
    await a.close();
  });

  test('re-entrant close from handlers', async () => {
    const [a, b] = muxPair();
    const channel = a.open(empty);
    const done = (async () => {
      for await (const remote of b.incoming) {
        for await (const _ of remote.readable) {
          void remote.close();
          await b.close();
          return;
        }
      }
    })();
    void channel.send(hexBytes('01'));
    await done;
    expect(await channel.done).toBeInstanceOf(Status);
    expectCode(await a.done, StatusCode.connectionLost);
  });
});

describe('transport failures end to end', () => {
  /** Runs a mux over a byte pipe fed with [input]; its end status and the control messages it sent. */
  async function overBytes(input: Uint8Array): Promise<[Status, MuxControlMessage[]]> {
    const clock = new SteppingClock();
    const toMux = new ByteFeed();
    const fromMux = new ByteDrain();
    const mux = new FrameMuxConnection(new StreamTransport(toMux, fromMux, { clock }), {
      ...rawOptions,
      clock,
      isInitiator: false,
    });
    toMux.add(input);
    const status = await mux.done;
    await clock.elapse(0);
    toMux.end();
    return [
      status,
      framesIn(fromMux.chunks).map((frame) =>
        MuxControlMessage.decode(MuxFrame.decode(frame).payload),
      ),
    ];
  }

  test('wrong preamble magic: GOAWAY PROTOCOL_ERROR', async () => {
    const [status, sent] = await overBytes(hexBytes('53 57 42 58 01 00 00 00'));
    expectCode(status, StatusCode.protocolError);
    expect(sent).toHaveLength(1);
    expect(sent[0]?.knownType).toBe(MuxControlType.goAway);
    expect(sent[0]?.goAwayStatus.known).toBe(StatusCode.protocolError);
  });

  test('unsupported preamble version: GOAWAY UNSUPPORTED', async () => {
    const [status, sent] = await overBytes(StreamFraming.encodePreamble({ version: 2 }));
    expectCode(status, StatusCode.unsupported);
    expect(sent).toHaveLength(1);
    expect(sent[0]?.knownType).toBe(MuxControlType.goAway);
    expect(sent[0]?.goAwayStatus.known).toBe(StatusCode.unsupported);
  });

  // Ported with the WebSocket transports, in
  // test/integration/mux-websocket.test.ts: WebSocket › a text message:
  // GOAWAY PROTOCOL_ERROR, and WebSocket › a message over the limit: GOAWAY
  // FRAME_TOO_LARGE.

  test('memory transport limit: the sender sees GOAWAY FRAME_TOO_LARGE', async () => {
    const clock = new SteppingClock();
    const [ta, tb] = memoryPair(clock, { maxFrameSize: 64 });
    const mux = new FrameMuxConnection(tb, { ...rawOptions, clock, isInitiator: false });
    const raw = new RawPeer(ta);
    raw.send(`02 02 00 ${'00 '.repeat(67)}`);
    expectCode(await mux.done, StatusCode.frameTooLarge);
    const rest = raw.rest();
    await clock.elapse(0);
    const frames = await rest;
    expect(frames).toHaveLength(1);
    const goAway = MuxControlMessage.decode((frames[0] as MuxFrame).payload);
    expect(goAway.knownType).toBe(MuxControlType.goAway);
    expect(goAway.goAwayStatus.known).toBe(StatusCode.frameTooLarge);
  });

  test('a transport limit below maxFrameSize: warned about and announced', async () => {
    const records: { level: LogLevel; name: string; message: string }[] = [];
    const logger: Logger = {
      log: (level, name, message) => {
        records.push({ level, name, message });
      },
    };
    const clock = new SteppingClock();
    const [ta, tb] = memoryPair(clock, { maxFrameSize: 512 });
    const a = new FrameMuxConnection(ta, { ...quiet, clock, logger, isInitiator: true });
    const b = new FrameMuxConnection(tb, { ...quiet, clock, logger, isInitiator: false });
    await a.ping();
    expect(a.peerLimits?.maxFrameSize).toBe(512);
    expect(b.peerLimits?.maxFrameSize).toBe(512);
    expect(
      records.filter(
        (r) => r.level === 'warn' && r.name === 'Switchboard.Mux' && r.message.includes('512'),
      ),
    ).toHaveLength(2);
    await a.close();
    await b.done;
  });
});

describe('real transports', () => {
  // Windows of 256 KiB, so that a subframe of 100000 bytes (at most half
  // the window) can be sent.
  const wide = { ...quiet, initialWindow: 256 * 1024 };

  async function exercise(client: FrameMuxConnection, server: FrameMuxConnection): Promise<void> {
    // Channels opened before the peer's LIMITS arrived keep 64 KiB.
    await client.ping();
    await server.ping();
    const serverIncoming = new Incoming(server);
    const channel = client.open(hexBytes('AA'));
    const accepted = await serverIncoming.next();
    expect(accepted.id).toBe(2);
    expect(hexString(accepted.openPayload)).toBe('AA');
    const echo = (async () => {
      for await (const subframe of accepted.readable) {
        await accepted.send(subframe);
      }
    })().catch(() => {});
    const replies = new Subframes(channel);
    const big = new Uint8Array(100000).fill(7);
    void channel.send(hexBytes('01 02'));
    void channel.send(big);
    expect(await replies.nextHex()).toBe('01 02');
    expectBytes(await replies.next(), big);
    // A bulk payload of 1 MB, in chunks, paced by the window.
    const back = server.open(hexBytes('BB'));
    back.priority = 'bulk';
    expect(back.id).toBe(3);
    const backAtClient = await firstIncoming(client);
    const bulk = pattern(1000000, 13);
    const received = (async () => {
      const out = new Uint8Array(bulk.length);
      let at = 0;
      for await (const chunk of backAtClient.readable) {
        out.set(chunk, at);
        at += chunk.length;
        if (at === bulk.length) {
          break;
        }
      }
      return out;
    })();
    await back.send(bulk);
    expectBytes(await received, bulk);
    await channel.close(Status.of(StatusCode.cancelled));
    expectCode(await accepted.done, StatusCode.cancelled);
    await echo;
    await back.close();
    expect(typeof (await client.ping())).toBe('number');
    await server.goAway();
    expectCode(await client.done, StatusCode.goingAway);
    expectCode(await server.done, StatusCode.goingAway);
  }

  // Ported with the TCP and WebSocket transports, in
  // test/integration/mux-tcp.test.ts and mux-websocket.test.ts: TCP; TCP:
  // GOAWAY reaches a peer that is still sending; WebSocket.

  test('a stream transport over a byte pipe (the TCP and WebSocket exercise)', async () => {
    // Not a Dart test: the exercise of the three above over the stream
    // binding without a socket, so that the mux runs over a transport that
    // reports output readiness and throttles its input.
    const clock = new SteppingClock();
    const toServer = new ByteFeed();
    const toClient = new ByteFeed();
    const client = new FrameMuxConnection(
      new StreamTransport(toClient, new ByteDrain({ target: toServer }), { clock }),
      { ...wide, clock, isInitiator: true },
    );
    const server = new FrameMuxConnection(
      new StreamTransport(toServer, new ByteDrain({ target: toClient }), { clock }),
      { ...wide, clock, isInitiator: false },
    );
    await exercise(client, server);
  });
});
