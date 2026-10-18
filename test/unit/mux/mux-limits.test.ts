// Resource limits of the mux: rejected OPENs, undelivered channels, receive
// buffers and the connection high-water mark, reason truncation,
// application codes from the peer, the close confirmation timeout and the
// budget of held OPEN payloads. Ports `test/mux_limits_test.dart`: its
// connection groups over the frame mux, and the codec half of its
// "reasons" and "CONNECTION_LOST stays local" groups (core notes, appendix
// A.12) on the status helpers alone.
import { once } from 'node:events';
import { connect } from 'node:net';
import { describe, expect, test } from 'vitest';
import { hexBytes, hexString } from '../../../src/core/bytes.ts';
import type { MuxChannel } from '../../../src/core/mux/contract.ts';
import { FrameMuxConnection } from '../../../src/core/mux/frame-mux-connection.ts';
import {
  MuxCommand,
  MuxControlMessage,
  MuxControlType,
  MuxCredit,
  type MuxFrame,
  MuxLimits,
} from '../../../src/core/mux/mux-frame.ts';
import {
  checkMuxStatus,
  decodeStatusPayload,
  fitStatus,
  statusForWire,
  statusFromWire,
  truncateStatus,
} from '../../../src/core/mux/mux-protocol.ts';
import { MovedStatus } from '../../../src/core/naming/naming-protocol.ts';
import { ProtocolError, Status, StatusCode } from '../../../src/core/status.ts';
import { StreamFraming } from '../../../src/core/transport/stream-framing.ts';
import { StreamTransport } from '../../../src/core/transport/stream-transport.ts';
import { listenTcp } from '../../../src/node/transport/tcp.ts';
import { ManualClock } from '../../../src/testing/manual-clock.ts';
import {
  ByteDrain,
  ByteFeed,
  clockOf,
  collect,
  dataHex,
  elapseUntil,
  empty,
  expectCode,
  expectStatus,
  firstIncoming,
  firstSubframe,
  Incoming,
  muxPair,
  openHex,
  quiet,
  rawOptions,
  rawPair,
  Subscription,
  settle,
  settleRaw,
} from './support.ts';

const strictUtf8 = new TextDecoder('utf-8', { fatal: true });

function closeStatus(frame: MuxFrame): Status {
  expect(frame.command).toBe(MuxCommand.close);
  return Status.decode(frame.payload);
}

describe('rejected OPENs', () => {
  test('keep only the id; re-OPEN before the confirmation is a protocol error', async () => {
    const [mux, raw] = rawPair({ options: { ...rawOptions, maxChannels: 1 } });
    raw.send(openHex(3));
    raw.send(openHex(5, 1000));
    const rejection = await raw.next();
    expect(rejection.channelId).toBe(5);
    expect(closeStatus(rejection).known).toBe(StatusCode.resourceExhausted);
    expect(mux.unconfirmedCloseCount).toBe(1);
    expect(mux.openChannelCount).toBe(1);
    // DATA racing the rejection is dropped, not a protocol error.
    raw.send('02 05 00 01');
    raw.send('02 00 00 01 AA');
    expect(await raw.nextHex()).toBe('02 00 00 02 AA');
    expect(mux.isOpen).toBe(true);
    raw.send(openHex(5));
    const goAway = await raw.nextControl(MuxControlType.goAway);
    expect(goAway.goAwayStatus.known).toBe(StatusCode.protocolError);
    expectCode(await mux.done, StatusCode.protocolError);
  });

  test('the confirmation frees the id', async () => {
    const [mux, raw] = rawPair({ options: { ...rawOptions, maxChannels: 1 } });
    const channels = new Incoming(mux);
    raw.send(openHex(3));
    raw.send(openHex(5));
    expect((await raw.next()).channelId).toBe(5);
    raw.send('22 05 00');
    raw.send('02 00 00 01 AA');
    expect(await raw.nextHex()).toBe('02 00 00 02 AA');
    expect(mux.unconfirmedCloseCount).toBe(0);
    // Channel 3 ends, then 5 can be opened again.
    const first = await channels.next();
    raw.send('22 03 00');
    expect(await raw.nextHex()).toBe('22 03 00');
    expectStatus(await first.done, Status.ok);
    raw.send(openHex(5));
    expect((await channels.next()).id).toBe(5);
    await mux.close();
    await channels.cancel();
  });

  test('many unconfirmed rejections end the connection with GOAWAY RESOURCE_EXHAUSTED', async () => {
    const [mux, raw] = rawPair({ options: { ...rawOptions, maxChannels: 1 } });
    const cap = 1024;
    expect(mux.options.maxPendingRejections).toBe(cap);
    raw.send(openHex(3));
    for (let i = 0; i < cap; i++) {
      raw.send(openHex(5 + 2 * i, 1000));
    }
    raw.send('02 00 00 01 AA');
    let closes = 0;
    for (;;) {
      const frame = await raw.next();
      if (frame.channelId === 0) {
        expect(hexString(frame.payload)).toBe('02 AA');
        break;
      }
      expect(closeStatus(frame).known).toBe(StatusCode.resourceExhausted);
      closes++;
    }
    expect(closes).toBe(cap);
    expect(mux.unconfirmedCloseCount).toBe(cap);
    expect(mux.isOpen).toBe(true);
    // One more is beyond the cap.
    raw.send(openHex(5 + 2 * cap, 1000));
    const rest = await raw.rest();
    expect(rest).toHaveLength(1);
    const goAway = MuxControlMessage.decode((rest[0] as MuxFrame).payload);
    expect(goAway.knownType).toBe(MuxControlType.goAway);
    expect(goAway.goAwayStatus.known).toBe(StatusCode.resourceExhausted);
    expectCode(await mux.done, StatusCode.resourceExhausted);
    expect(mux.unconfirmedCloseCount).toBe(0);
  });
});

describe('undelivered incoming channels', () => {
  test('closed channels still count toward the incoming backlog cap', async () => {
    const [mux, raw] = rawPair({ options: { ...rawOptions, maxChannels: 2 } });
    for (let i = 0; i < 2; i++) {
      raw.send(openHex(3));
      raw.send('22 03 00');
      expectStatus(closeStatus(await raw.next()), Status.ok);
    }
    expect(mux.openChannelCount).toBe(0);
    raw.send(openHex(3));
    raw.send('22 03 00');
    expect(closeStatus(await raw.next()).known).toBe(StatusCode.resourceExhausted);
    const incoming = new Incoming(mux);
    expect((await incoming.next()).state).toBe('closed');
    expect((await incoming.next()).state).toBe('closed');
    // Delivering the backlog frees capacity, including a reused id.
    raw.send(openHex(3));
    expect((await incoming.next()).state).toBe('open');
    await incoming.cancel();
    await mux.close();
  });

  test('closed undelivered OPEN payloads retain their budget', async () => {
    const [mux, raw] = rawPair({ options: { ...rawOptions, maxOpenPayloadBytes: 100 } });
    raw.send(openHex(3, 100));
    raw.send('02 03 00 AA');
    raw.send('22 03 00');
    expectStatus(closeStatus(await raw.next()), Status.ok);
    expect(mux.openPayloadBytes).toBe(100);
    // The payload and one subframe at its cost, 1 + 16.
    expect(mux.bufferedBytes).toBe(117);
    raw.send(openHex(3, 1));
    raw.send('22 03 00');
    expect(closeStatus(await raw.next()).known).toBe(StatusCode.resourceExhausted);
    const incoming = new Incoming(mux);
    const channel = await incoming.next();
    expect(channel.openPayload).toHaveLength(100);
    expect((await collect(channel)).map(hexString)).toEqual(['AA']);
    expect(mux.openPayloadBytes).toBe(0);
    expect(mux.bufferedBytes).toBe(0);
    raw.send(openHex(3, 100));
    expect((await incoming.next()).openPayload).toHaveLength(100);
    await incoming.cancel();
    await mux.close();
  });

  test('cancelling or ending a closed backlog releases accounting once', async () => {
    for (const closeConnection of [false, true]) {
      const [mux, raw] = rawPair();
      raw.send(openHex(3, 100));
      raw.send('02 03 00 AA');
      raw.send('22 03 00');
      expectStatus(closeStatus(await raw.next()), Status.ok);
      expect(mux.openPayloadBytes).toBe(100);
      if (closeConnection) {
        await mux.close();
      }
      // A consumer that takes nothing, then stops.
      await new Incoming(mux).cancel();
      expect(mux.openPayloadBytes, `closeConnection: ${closeConnection}`).toBe(0);
      expect(mux.bufferedBytes, `closeConnection: ${closeConnection}`).toBe(0);
      await mux.close();
    }
  });

  test('a paused listener holds at most maxChannels of them', async () => {
    // Paused: a consumer that does not ask for the next channel.
    const [mux, raw] = rawPair({ options: { ...rawOptions, maxChannels: 2 } });
    const incoming = new Incoming(mux);
    for (let i = 0; i < 2; i++) {
      raw.send(openHex(3, 10));
      raw.send('02 03 00 AA');
      raw.send('22 03 00');
      expectStatus(closeStatus(await raw.next()), Status.ok);
    }
    // Its CLOSE confirms the rejection.
    raw.send(openHex(3, 10));
    raw.send('22 03 00');
    expect(closeStatus(await raw.next()).known).toBe(StatusCode.resourceExhausted);
    expect(mux.openPayloadBytes).toBe(20);
    const received = [await incoming.next(), await incoming.next()];
    expect(mux.openPayloadBytes).toBe(0);
    for (const channel of received) {
      expect(channel.state).toBe('closed');
      expect((await collect(channel)).map(hexString)).toEqual(['AA']);
    }
    expect(mux.bufferedBytes).toBe(0);
    await incoming.cancel();
    await mux.close();
  });

  test('cancelling incoming closes them with UNAVAILABLE', async () => {
    const [a, b] = muxPair({ acceptor: { ...quiet, goAwayGrace: 5000 } });
    const c1 = a.open(empty);
    const c2 = a.open(empty);
    const c3 = a.open(empty);
    void c2.send(hexBytes('01 02 03'));
    await a.ping();
    // The DATA waited for b's LIMITS, so it left behind the PING; in Dart
    // it reaches b before the test goes on, here after a turn.
    await settle();
    expect(b.openChannelCount).toBe(3);
    expect(b.bufferedBytes).toBeGreaterThan(0);

    const first = await firstIncoming(b);
    expect(first.id).toBe(c1.id);
    expectCode(await c2.done, StatusCode.unavailable);
    expectCode(await c3.done, StatusCode.unavailable);
    await a.ping();
    expect(b.openChannelCount).toBe(1);
    expect([...b.channels]).toEqual([first]);
    expect(b.bufferedBytes).toBe(0);
    // Later OPENs are refused the same way.
    expectCode(await a.open(empty).done, StatusCode.unavailable);

    await first.close();
    expect(b.openChannelCount).toBe(0);
    // Without waiting for the grace: no clock moves here.
    await b.goAway();
    expectStatus(await c1.done, Status.ok);
    expectCode(await a.done, StatusCode.goingAway);
  });

  test('a paused incoming subscription keeps them buffered', async () => {
    const [a, b] = muxPair();
    const incoming = new Incoming(b);
    const received: MuxChannel[] = [];
    a.open(empty);
    await a.ping();
    received.push(await incoming.next());
    expect(received).toHaveLength(1);
    // Paused: nothing asks for the next one.
    a.open(empty);
    a.open(empty);
    await a.ping();
    expect(received).toHaveLength(1);
    expect(b.openChannelCount).toBe(3);
    received.push(await incoming.next(), await incoming.next());
    expect(received.map((c) => c.id)).toEqual([2, 4, 6]);
    await incoming.cancel();
    await a.close();
  });
});

describe('receive buffers', () => {
  test(
    'a channel nobody reads holds at most its window, the others flow; DATA beyond the ' +
      'window ends the connection',
    async () => {
      const [mux, raw] = rawPair({
        options: { ...rawOptions, announceLimits: true, initialWindow: 4096 },
      });
      expect(
        MuxLimits.decode((await raw.nextControl(MuxControlType.limits)).payload).initialWindow,
      ).toBe(4096);
      // Channels this side opens: the peer saw the LIMITS first, so their
      // window is the one announced.
      const unread = mux.open(empty);
      const read = mux.open(empty);
      expect(await raw.nextHex()).toBe('12 02 00');
      expect(await raw.nextHex()).toBe('12 04 00');
      const readData = collect(read);
      for (let i = 0; i < 4; i++) {
        raw.send(dataHex(2, 1000));
        raw.send(dataHex(4, 1000));
      }
      raw.send('02 00 00 01 AA');
      // The channel that is read returns credit at half its window.
      const credits: MuxCredit[] = [];
      for (;;) {
        const frame = await raw.next();
        const control = MuxControlMessage.decode(frame.payload);
        if (control.knownType === MuxControlType.pong) {
          break;
        }
        credits.push(MuxCredit.decode(control.payload));
      }
      // In batches of at least half the window, as the reader takes them;
      // what is consumed and not returned yet is less than half.
      expect(credits.length).toBeGreaterThan(0);
      let returned = 0;
      for (const credit of credits) {
        expect(credit.channelId).toBe(4);
        expect(credit.bytes).toBeGreaterThanOrEqual(2048);
        returned += credit.bytes;
      }
      expect(4064 - returned).toBeGreaterThanOrEqual(0);
      expect(4064 - returned).toBeLessThanOrEqual(2047);
      expect(read.receiveWindow).toBe(4096 - 4064 + returned);
      // 4 * (1000 + 16) bytes buffered for the unread channel: its window
      // is used up.
      expect(unread.bufferedBytes).toBe(4064);
      expect(unread.receiveWindow).toBe(32);
      expect(mux.bufferedBytes).toBe(4064);
      expect(unread.state).toBe('open');

      // Beyond it: GOAWAY PROTOCOL_ERROR.
      raw.send(dataHex(2, 17));
      const goAway = await raw.nextControl(MuxControlType.goAway);
      expect(goAway.goAwayStatus.known).toBe(StatusCode.protocolError);
      expect(goAway.goAwayStatus.reason).toContain('beyond the window');
      expect((await mux.done).known).toBe(StatusCode.protocolError);
      expect((await readData).map((d) => d.length)).toEqual([1000, 1000, 1000, 1000]);
    },
  );

  test('a channel the peer opened accepts 64 KiB however small the announced window', async () => {
    const [mux, raw] = rawPair({
      options: { ...rawOptions, announceLimits: true, initialWindow: 4096 },
    });
    // Opened before the LIMITS reached the peer, as far as this side can
    // tell: the peer's window is 64 KiB.
    raw.send(openHex(3));
    const channel = await firstIncoming(mux);
    expect(channel.receiveWindow).toBe(65536);
    for (let i = 0; i < 64; i++) {
      raw.send(dataHex(3, 1008));
    }
    await settleRaw(raw);
    expect(channel.receiveWindow).toBe(0);
    expect(mux.isOpen).toBe(true);
    raw.send(dataHex(3, 0));
    const goAway = await raw.nextControl(MuxControlType.goAway);
    expect(goAway.goAwayStatus.known).toBe(StatusCode.protocolError);
  });

  test('a cancelled channel subscription drops later DATA, returning its credit', async () => {
    const [mux, raw] = rawPair();
    raw.send(openHex(3));
    const channel = await firstIncoming(mux);
    const sub = new Subscription(channel);
    raw.send(dataHex(3, 3000));
    await settle();
    await sub.cancel();
    for (let i = 0; i < 10; i++) {
      raw.send(dataHex(3, 3000));
    }
    raw.send('02 00 00 01 AA');
    // 11 * 3016 bytes consumed: past half the window of 64 KiB.
    const credit = await raw.nextControl(MuxControlType.credit);
    expect(`${MuxCredit.decode(credit.payload)}`).toBe(`${new MuxCredit(3, 33176)}`);
    expect(await raw.nextHex()).toBe('02 00 00 02 AA');
    expect(channel.state).toBe('open');
    expect(channel.receiveWindow).toBe(65536);
    expect(mux.bufferedBytes).toBe(0);
    await mux.close();
  });

  describe('connection high-water mark', () => {
    /** A mux over a stream transport the test feeds, and the channel its first frames open. */
    async function start(
      options: Partial<FrameMuxConnection['options']>,
    ): Promise<[FrameMuxConnection, ByteFeed, MuxChannel, ManualClock]> {
      const clock = new ManualClock();
      const toMux = new ByteFeed();
      const mux = new FrameMuxConnection(new StreamTransport(toMux, new ByteDrain(), { clock }), {
        ...options,
        clock,
        isInitiator: false,
      });
      toMux.add(StreamFraming.encodePreamble());
      toMux.add(StreamFraming.encodeFrame(hexBytes(openHex(2))));
      return [mux, toMux, await firstIncoming(mux), clock];
    }

    // At most the channel's window of 64 KiB: 64 * 1016 bytes.
    function sendData(toMux: ByteFeed, count: number): void {
      for (let i = 0; i < count; i++) {
        toMux.add(StreamFraming.encodeFrame(hexBytes(dataHex(2, 1000))));
      }
    }

    test('a paused channel pauses the transport input', async () => {
      const [mux, toMux, channel] = await start({
        ...rawOptions,
        receiveHighWaterMarkBytes: 16 * 1024,
      });
      const sizes: number[] = [];
      const sub = new Subscription(channel, (d) => sizes.push(d.length)).pause();
      sendData(toMux, 60);
      await settle();
      expect(mux.isReceivePaused).toBe(true);
      expect(toMux.isPaused).toBe(true);
      expect(mux.bufferedBytes).toBeGreaterThan(16 * 1024);
      // Bounded by the mark and what the transport had already decoded,
      // not by what the peer sent.
      expect(mux.bufferedBytes).toBeLessThan(20 * 1024);
      expect(channel.bufferedBytes).toBe(mux.bufferedBytes);

      sub.resume();
      await settle();
      expect(sizes).toHaveLength(60);
      expect(mux.isReceivePaused).toBe(false);
      expect(toMux.isPaused).toBe(false);
      expect(mux.bufferedBytes).toBe(0);
      await sub.cancel();
      await mux.close();
      toMux.end();
    });

    test('keep-alive does not probe while reading is paused', async () => {
      const [mux, toMux, channel, clock] = await start({
        ...rawOptions,
        receiveHighWaterMarkBytes: 4096,
        keepAliveInterval: 20,
        keepAliveTimeout: 20,
      });
      const sub = new Subscription(channel).pause();
      sendData(toMux, 10);
      await settle();
      expect(mux.isReceivePaused).toBe(true);
      // The byte peer never answers PING: without the pause rule the
      // connection would be gone after 40 ms.
      await clock.elapseAsync(150);
      expect(mux.isOpen).toBe(true);
      await sub.cancel();
      await elapseUntil(clock, mux.close());
      toMux.end();
    });
  });
});

describe('output backpressure over TCP', () => {
  test('a peer that never reads: output stays bounded, input pauses, close destroys the socket', async () => {
    // On the loopback with the system clock. The client never reads: its
    // kernel buffers and the server's fill with PONGs, the server's
    // transport stops reading once its own output passes the mark, and
    // the client's PINGs back up in turn.
    const mark = 64 * 1024;
    const listener = await listenTcp('127.0.0.1', 0, { outputHighWaterMark: mark });
    const accepted = listener.transports[Symbol.asyncIterator]().next();
    const client = connect(listener.port, '127.0.0.1');
    client.on('error', () => {});
    await once(client, 'connect');
    client.pause();
    const clientClosed = new Promise<void>((resolve) => {
      client.once('close', () => resolve());
    });
    const next = await accepted;
    if (next.done === true) {
      throw new Error('no connection accepted');
    }
    await listener.close();
    const transport = next.value.transport;
    const mux = new FrameMuxConnection(transport, {
      ...rawOptions,
      keepAliveTimeout: 200,
      isInitiator: false,
    });

    // PINGs with 1024 byte payloads, each answered by an equal PONG.
    const ping = StreamFraming.encodeFrame(
      new MuxControlMessage(MuxControlType.ping, new Uint8Array(1024)).toFrame().encode(),
    );
    const chunk = new Uint8Array(64 * ping.length);
    for (let i = 0; i < 64; i++) {
      chunk.set(ping, i * ping.length);
    }
    /** Writes [bytes] if the socket has taken everything before; whether it took them. */
    const write = (bytes: Uint8Array): boolean => {
      if (client.writableLength > 0) {
        return false;
      }
      client.write(bytes);
      return true;
    };
    const tick = (): Promise<void> => new Promise((resolve) => setImmediate(resolve));

    write(StreamFraming.encodePreamble());
    let sent = 0;
    const started = Date.now();
    while (!transport.isInputThrottled) {
      if (Date.now() - started > 20_000 || sent > 512 * 1024 * 1024) {
        throw new Error(`output never backed up after ${sent} bytes`);
      }
      if (write(chunk)) {
        sent += chunk.length;
      }
      await tick();
    }
    await settle();
    expect(transport.bufferedOutputBytes).toBeGreaterThan(mark);
    // The mark plus the PONGs for what had already been read.
    const bound = mark + 8 * 1024 * 1024;
    expect(transport.bufferedOutputBytes).toBeLessThan(bound);

    // The client keeps sending until its own kernel buffers are full: the
    // server reads only while its output drains, so the backpressure
    // reaches the client and the server's output stays bounded meanwhile.
    let extra = 0;
    let stuck = 0;
    while (stuck < 20) {
      if (write(chunk)) {
        extra += chunk.length;
        stuck = 0;
      } else {
        stuck++;
      }
      if (extra > 256 * 1024 * 1024) {
        throw new Error('the server kept reading');
      }
      await tick();
      expect(transport.bufferedOutputBytes).toBeLessThan(bound);
    }
    await settle();
    expect(transport.bufferedOutputBytes).toBeLessThan(bound);
    expect(transport.isInputThrottled).toBe(true);

    // Closing cannot flush to a peer that does not read: after
    // keepAliveTimeout the socket is destroyed.
    const closing = Date.now();
    await mux.close();
    expect(Date.now() - closing).toBeLessThan(5000);
    expect(transport.socket.destroyed).toBe(true);
    // The destroyed socket's buffer is dropped: nothing counts as buffered.
    expect(transport.bufferedOutputBytes).toBe(0);
    // The client learns of it once it reads.
    client.resume();
    await clientClosed;
  }, 30_000);
});

test('keep-alive counts a throttled peer that reads as alive', async () => {
  const clock = new ManualClock();
  const toMux = new ByteFeed();
  const output = new ByteDrain({ slow: true });
  const transport = new StreamTransport(toMux, output, { outputHighWaterMark: 4096, clock });
  const mux = new FrameMuxConnection(transport, {
    ...rawOptions,
    keepAliveInterval: 20,
    keepAliveTimeout: 20,
    clock,
    isInitiator: true,
  });
  // OPEN frames are not flow controlled, so they fill the transport's
  // output where DATA would wait for credit.
  for (let i = 0; i < 2000; i++) {
    mux.open(new Uint8Array(1000));
  }
  await settle();
  expect(transport.isInputThrottled).toBe(true);
  // Nothing arrives, but the peer reads a chunk every 10 ms: longer than
  // interval and timeout together, the connection stays.
  for (let i = 0; i < 15; i++) {
    output.take();
    await clock.elapseAsync(10);
  }
  expect(transport.isInputThrottled).toBe(true);
  expect(mux.isOpen).toBe(true);
  // The peer stops reading: keep-alive gives up, the transport is aborted.
  await elapseUntil(clock, mux.done);
  expectCode(await mux.done, StatusCode.connectionLost);
  await elapseUntil(clock, mux.close());
  expect(transport.bufferedOutputBytes).toBe(0);
  toMux.end();
});

test('keep-alive counts a peer that reads output the scheduler holds as alive', async () => {
  const clock = new ManualClock();
  const toMux = new ByteFeed();
  const output = new ByteDrain({ slow: true });
  const transport = new StreamTransport(toMux, output, { clock });
  const mux = new FrameMuxConnection(transport, {
    ...rawOptions,
    keepAliveInterval: 20,
    keepAliveTimeout: 20,
    clock,
    isInitiator: true,
  });
  // A window of 64 KiB: the transport takes the first frames, the
  // scheduler holds the rest.
  const channel = mux.open(empty);
  for (let i = 0; i < 64; i++) {
    void channel.send(new Uint8Array(1000));
  }
  await settle();
  expect(transport.isInputThrottled).toBe(false);
  expect(mux.heldOutputBytes).toBeGreaterThan(32 * 1000);
  // Nothing arrives, but the peer reads a chunk every 10 ms: longer than
  // interval and timeout together, the connection stays.
  for (let i = 0; i < 15; i++) {
    output.take();
    await clock.elapseAsync(10);
  }
  expect(mux.heldOutputBytes).toBeGreaterThan(0);
  expect(mux.isOpen).toBe(true);
  // The peer stops reading: keep-alive gives up.
  await elapseUntil(clock, mux.done);
  expectCode(await mux.done, StatusCode.connectionLost);
  await elapseUntil(clock, mux.close());
  toMux.end();
});

describe('reasons', () => {
  test('truncation is UTF-8 safe', () => {
    for (const [char, bytes, length] of [
      ['x', 1, 1024],
      ['é', 2, 1024],
      ['€', 3, 1022],
      ['😀', 4, 1022],
    ] as const) {
      const status = Status.of(StatusCode.unavailable, char.repeat(2000));
      const cut = truncateStatus(status, 1024);
      const encoded = cut.encode();
      expect(encoded.length, char).toBeLessThanOrEqual(1024);
      expect(encoded.length, char).toBeGreaterThan(1024 - bytes);
      expect(encoded.length, char).toBe(length);
      expect(strictUtf8.decode(encoded.subarray(2))).toBe(cut.reason);
      expect(cut.reason.replaceAll(char, ''), char).toBe('');
    }
    const short = Status.of(StatusCode.unavailable, 'ok');
    expect(truncateStatus(short, 1024)).toBe(short);
    expect(truncateStatus(short, 2).equals(new Status(14))).toBe(true);
    expect(
      MuxControlMessage.goAway(Status.of(StatusCode.unavailable, 'x'.repeat(2000))).payload,
    ).toHaveLength(MuxControlMessage.maxControlPayload);
  });

  test('truncation keeps fixed fields before the reason', () => {
    const moved = new MovedStatus({
      owner: 0xfedcba987654,
      epoch: 7,
      reason: '€'.repeat(400),
    }).toStatus();
    const cut = truncateStatus(moved, 1024);
    const encoded = cut.encode();
    expect(encoded.length).toBeGreaterThanOrEqual(1022);
    expect(encoded.length).toBeLessThanOrEqual(1024);
    expect(encoded.length).toBe(1023);
    expect(hexString(encoded.subarray(0, 12))).toBe('25 00 54 76 98 BA DC FE 07 00 00 00');
    expect(hexString(encoded.subarray(0, 12))).toBe(hexString(moved.encode().subarray(0, 12)));
    const fields = MovedStatus.fromStatus(cut);
    expect(fields.owner).toBe(0xfedcba987654);
    expect(fields.epoch).toBe(7);
    expect(fields.reason.replaceAll('€', '')).toBe('');
    // No room for the reason: the fields alone; no room for the fields:
    // nothing, never part of them.
    expect(MovedStatus.fromStatus(truncateStatus(moved, 12)).owner).toBe(0xfedcba987654);
    expect(truncateStatus(moved, 11).equals(new Status(37))).toBe(true);
    expect(truncateStatus(moved, 2).equals(new Status(37))).toBe(true);
    // RELOCATED has the same fields.
    const relocated = Status.decode(Uint8Array.of(38, 0, ...moved.encode().subarray(2)));
    expect(MovedStatus.fromStatus(truncateStatus(relocated, 12)).owner).toBe(0xfedcba987654);
    expect(truncateStatus(relocated, 11).equals(new Status(38))).toBe(true);
  });

  test('a status fits the peer frame limit after the header', () => {
    // A CLOSE on a short id to a peer that takes frames of 64 bytes: 64,
    // less the 3 byte header, less the 2 byte code.
    const fitted = fitStatus(Status.of(StatusCode.cancelled, 'y'.repeat(100)), 3, 64);
    expect(fitted.equals(Status.of(StatusCode.cancelled, 'y'.repeat(59)))).toBe(true);
    // No stated limit: the 1024 byte bound alone.
    expect(
      fitStatus(Status.of(StatusCode.cancelled, 'y'.repeat(2000)), 3, 0).encode(),
    ).toHaveLength(1024);
    // A large limit leaves the 1024 byte bound.
    expect(
      fitStatus(Status.of(StatusCode.cancelled, 'y'.repeat(2000)), 3, 1 << 20).encode(),
    ).toHaveLength(1024);
  });
  test('a RELOCATED status crosses a CLOSE byte for byte', async () => {
    const [a, b] = muxPair();
    const ca = a.open(empty);
    const cb = await firstIncoming(b);
    const moved = new MovedStatus({ owner: 0x80ff80ff80ff, epoch: 0x80808080 }).toStatus({
      relocated: true,
    });
    await cb.close(moved);
    const received = await ca.done;
    expect(hexString(received.encode())).toBe(hexString(moved.encode()));
    const fields = MovedStatus.fromStatus(received);
    expect(fields.owner).toBe(0x80ff80ff80ff);
    expect(fields.epoch).toBe(0x80808080);
    expect(fields.reason).toBe('');
    await a.close();
  });

  test('an 1100 byte GOAWAY reason does not break a conforming peer', async () => {
    const [a, b] = muxPair();
    const ca = a.open(empty);
    await firstIncoming(b);
    const goingAway = b.goAway(Status.of(StatusCode.unavailable, 'x'.repeat(1100)));
    await a.ping();
    expect(a.peerGoingAway).toBe(true);
    expect(a.isOpen).toBe(true);
    await ca.close();
    const status = await a.done;
    expect(status.known).toBe(StatusCode.goingAway);
    expect(status.reason.startsWith('unavailable: xxx')).toBe(true);
    expect(new TextEncoder().encode(status.reason).length).toBeLessThan(1100);
    await goingAway;
  });

  test('a CLOSE reason fits the peer frame limit', async () => {
    const [a, b] = muxPair({ acceptor: { ...quiet, maxFrameSize: 64 } });
    await a.ping();
    const ca = a.open(empty);
    const rb = await firstIncoming(b);
    const full = Status.of(StatusCode.cancelled, 'y'.repeat(100));
    void ca.close(full);
    // 64 minus a 3 byte header and the 2 byte code.
    expectStatus(await rb.done, Status.of(StatusCode.cancelled, 'y'.repeat(59)));
    expectStatus(await ca.done, full);
    await a.ping();
    expect(b.isOpen).toBe(true);
    await a.close();
  });
});

describe('CONNECTION_LOST stays local (the status helpers)', () => {
  test('on the wire it is UNAVAILABLE, the reason kept', () => {
    const local = Status.of(StatusCode.connectionLost, 'upstream gone');
    const wire = statusForWire(local);
    expect(wire.equals(Status.of(StatusCode.unavailable, 'upstream gone'))).toBe(true);
    expect(local.known).toBe(StatusCode.connectionLost);
  });

  test('only the code changes: the bytes after it are relayed', () => {
    const relayed = Status.decode(hexBytes('22 00 FF 41'));
    expect(hexString(statusForWire(relayed).encode())).toBe('0E 00 FF 41');
  });
});

describe('application codes (the status helpers)', () => {
  test('are refused in a CLOSE or GOAWAY of this side', () => {
    expect(() => checkMuxStatus(new Status(256))).toThrow(RangeError);
    expect(() => checkMuxStatus(new Status(0xffff, 'app'))).toThrow(RangeError);
    expect(() => checkMuxStatus(new Status(255))).not.toThrow();
  });

  test('from the peer are reported as UNKNOWN and relayable', () => {
    const received = statusFromWire(Status.decode(hexBytes('2C 01 61')));
    expect(received.equals(Status.of(StatusCode.unknown, 'code 300: a'))).toBe(true);
    expect(() => statusForWire(received)).not.toThrow();
    const known = Status.of(StatusCode.notFound, 'x');
    expect(statusFromWire(known)).toBe(known);
  });
});

test('a one byte status payload is a protocol error', () => {
  expect(() => decodeStatusPayload(hexBytes('05'), 'CLOSE')).toThrow(ProtocolError);
  expect(() => decodeStatusPayload(hexBytes('05'), 'CLOSE')).toThrow('malformed CLOSE status');
  expect(decodeStatusPayload(new Uint8Array(0), 'CLOSE').isOk).toBe(true);
});

describe('CONNECTION_LOST stays local', () => {
  test('CLOSE sends UNAVAILABLE and retains the local status', async () => {
    const [mux, raw] = rawPair();
    const channel = mux.open(empty);
    await raw.next();
    const status = Status.of(StatusCode.connectionLost, 'upstream gone');
    void channel.close(status);
    expectStatus(closeStatus(await raw.next()), Status.of(StatusCode.unavailable, 'upstream gone'));
    raw.send('22 02 00');
    expectStatus(await channel.done, status);
    await mux.close();
  });

  test('GOAWAY sends UNAVAILABLE and retains the local status', async () => {
    const [mux, raw] = rawPair();
    const status = Status.of(StatusCode.connectionLost, 'upstream gone');
    await mux.goAway(status);
    const frames = await raw.rest();
    expect(frames).toHaveLength(1);
    expectStatus(
      MuxControlMessage.decode((frames[0] as MuxFrame).payload).goAwayStatus,
      Status.of(StatusCode.unavailable, 'upstream gone'),
    );
    expectStatus(await mux.done, status);
  });

  test('only the code changes: the bytes after it are relayed', async () => {
    const [mux, raw] = rawPair();
    const channel = mux.open(empty);
    await raw.next();
    // As a peer that broke the rule sent it, the reason not even UTF-8.
    const status = Status.decode(hexBytes('22 00 FF 41'));
    void channel.close(status);
    expect(hexString((await raw.next()).payload)).toBe('0E 00 FF 41');
    raw.send('22 02 00');
    expectStatus(await channel.done, status);
    await mux.close();
  });
});

describe('application codes from the peer', () => {
  test('CLOSE: reported as UNKNOWN and relayable', async () => {
    const [mux, raw] = rawPair();
    const channel = mux.open(empty);
    expect(await raw.nextHex()).toBe('12 02 00');
    raw.send('22 02 00 2C 01 61');
    expect(await raw.nextHex()).toBe('22 02 00');
    const status = await channel.done;
    expectStatus(status, Status.of(StatusCode.unknown, 'code 300: a'));
    const relay = mux.open(empty);
    expect(await raw.nextHex()).toBe('12 04 00');
    // Would throw RangeError for an application code.
    void relay.close(status);
    const close = await raw.next();
    expect(close.channelId).toBe(4);
    expectStatus(closeStatus(close), status);
    await mux.close();
  });

  test('GOAWAY: reported as GOING_AWAY with the code', async () => {
    const [mux, raw] = rawPair();
    raw.send('02 00 00 03 2C 01');
    expectStatus(await mux.done, Status.of(StatusCode.goingAway, 'unknown: code 300'));
    await raw.rest();
  });
});

describe('close confirmation timeout', () => {
  const options = { keepAliveInterval: undefined, announceLimits: false, closeConfirmTimeout: 50 };

  test('close() completes; the id stays reserved until the late CLOSE', async () => {
    const [mux, raw] = rawPair({ options });
    const channel = mux.open(empty);
    expect(await raw.nextHex()).toBe('12 02 00');
    const closing = channel.close(Status.of(StatusCode.cancelled));
    expect(await elapseUntil(clockOf(mux), closing)).toBeGreaterThanOrEqual(40);
    expect(await raw.nextHex()).toBe('22 02 00 01 00');
    expectStatus(await channel.done, Status.of(StatusCode.cancelled));
    expect(channel.state).toBe('closed');
    expect(mux.openChannelCount).toBe(0);
    expect([...mux.channels]).toEqual([]);
    expect(mux.unconfirmedCloseCount).toBe(1);

    // Not reused while the peer may still consider it open.
    mux.nextChannelIdForTesting = 2;
    const other = mux.open(empty);
    expect(other.id).toBe(4);
    expect(await raw.nextHex()).toBe('12 04 00');

    // Late DATA is dropped, the late CLOSE frees the id.
    raw.send('02 02 00 01');
    raw.send('22 02 00');
    raw.send('02 00 00 01 AA');
    expect(await raw.nextHex()).toBe('02 00 00 02 AA');
    expect(mux.unconfirmedCloseCount).toBe(0);
    mux.nextChannelIdForTesting = 2;
    expect(mux.open(empty).id).toBe(2);
    await mux.close();
  });

  test('goAway does not wait for unconfirmed channels', async () => {
    const [mux, raw] = rawPair({ options: { ...options, goAwayGrace: 5000 } });
    const clock = clockOf(mux);
    const channel = mux.open(empty);
    void channel.close();
    await elapseUntil(clock, channel.done);
    const goingAway = mux.goAway();
    // Well within the grace.
    expect(await elapseUntil(clock, goingAway)).toBeLessThan(2000);
    await raw.rest();
  });

  test('too many unconfirmed closes end the connection', async () => {
    const [mux, raw] = rawPair({ options: { ...options, maxPendingRejections: 2 } });
    const channels = [0, 1, 2].map(() => mux.open(empty));
    for (const channel of channels) {
      void channel.close();
    }
    await elapseUntil(clockOf(mux), mux.done);
    expectCode(await mux.done, StatusCode.resourceExhausted);
    const goAway = await raw.nextControl(MuxControlType.goAway);
    expect(goAway.goAwayStatus.known).toBe(StatusCode.resourceExhausted);
  });

  test('Duration.zero waits forever', async () => {
    const [mux, raw] = rawPair({ options: { ...options, closeConfirmTimeout: 0 } });
    const channel = mux.open(empty);
    const closed = channel.close();
    await clockOf(mux).elapseAsync(100);
    expect(channel.state).toBe('halfClosedLocal');
    await mux.close();
    await closed;
    expectCode(await channel.done, StatusCode.connectionLost);
    await raw.rest();
  });
});

describe('open payload budget', () => {
  test('peer OPENs beyond maxOpenPayloadBytes are refused until channels close', async () => {
    const [a, b] = muxPair({ acceptor: { ...quiet, maxOpenPayloadBytes: 100 } });
    const atB = new Incoming(b);
    const first = a.open(new Uint8Array(40));
    const second = a.open(new Uint8Array(40));
    const third = a.open(new Uint8Array(40));
    const none = a.open(new Uint8Array(0));
    const firstAtB = await atB.next();
    await atB.next();
    expectCode(await third.done, StatusCode.resourceExhausted);
    // An empty payload still fits.
    expect((await atB.next()).id).toBe(none.id);
    expect(b.openPayloadBytes).toBe(80);
    // Held payloads count as buffered bytes.
    expect(b.bufferedBytes).toBe(80);
    // Channels the acceptor opens itself do not count.
    b.open(new Uint8Array(90));
    await b.ping();
    expect(b.openPayloadBytes).toBe(80);
    // A mutually closed channel releases its payload.
    await firstAtB.close();
    expectStatus(await first.done, Status.ok);
    expect(b.openPayloadBytes).toBe(40);
    expect(b.bufferedBytes).toBe(40);
    const fourth = a.open(new Uint8Array(60));
    expect((await atB.next()).openPayload).toHaveLength(60);
    expect(b.openPayloadBytes).toBe(100);
    expect(fourth.state).toBe('open');
    expect(second.state).toBe('open');
    await a.close();
    await b.done;
    expect(b.openPayloadBytes).toBe(0);
    expect(b.bufferedBytes).toBe(0);
    await atB.cancel();
  });

  test(
    'half a smaller receive high-water mark is the budget, so held payloads never keep the ' +
      'reading paused',
    async () => {
      const [a, b] = muxPair({ acceptor: { ...quiet, receiveHighWaterMarkBytes: 200 } });
      const atB = new Incoming(b);
      a.open(new Uint8Array(60));
      const refused = a.open(new Uint8Array(60));
      const second = a.open(new Uint8Array(40));
      await atB.next();
      const held = await atB.next();
      expect(held.openPayload).toHaveLength(40);
      expectCode(await refused.done, StatusCode.resourceExhausted);
      expect(b.bufferedBytes).toBe(100);
      expect(b.isReceivePaused).toBe(false);
      // Unread data on top of them pauses it, and reading that data
      // resumes it although the payloads are still held.
      void second.send(new Uint8Array(120));
      await settle();
      expect(b.isReceivePaused).toBe(true);
      await firstSubframe(held);
      expect(b.bufferedBytes).toBe(100);
      expect(b.isReceivePaused).toBe(false);
      await a.close();
      await atB.cancel();
    },
  );

  test('a channel whose close is not confirmed releases its payload', async () => {
    const [mux, raw] = rawPair({ options: { ...rawOptions, closeConfirmTimeout: 30 } });
    raw.send(openHex(3, 50));
    const channel = await firstIncoming(mux);
    expect(mux.openPayloadBytes).toBe(50);
    await elapseUntil(clockOf(mux), channel.close());
    expect(mux.openPayloadBytes).toBe(0);
    expect(mux.bufferedBytes).toBe(0);
    await mux.close();
    await raw.rest();
  });
});
