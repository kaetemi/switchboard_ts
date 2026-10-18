// Flow control (wiki page "Polyverse Switchboard Mux", section "Flow
// control"): the send window, CREDIT, the receive window and the ways
// credit is returned. Ports `test/mux_flow_test.dart`.
//
// Delivery is pull based (design section 12, item 10): a subframe is
// delivered, and counts as consumed, when a read takes it, one per read.
// Dart's listener takes every buffered subframe at once and returns their
// credit in one batch afterwards; here credit goes as soon as half the
// window is consumed. The tests that count the batches say so.
import { describe, expect, test } from 'vitest';
import { hexBytes, hexString } from '../../../src/core/bytes.ts';
import type { MuxChannel } from '../../../src/core/mux/contract.ts';
import { FrameMuxConnection } from '../../../src/core/mux/frame-mux-connection.ts';
import {
  MuxCommand,
  MuxControlType,
  MuxCredit,
  MuxFrame,
  MuxLimits,
} from '../../../src/core/mux/mux-frame.ts';
import { Name } from '../../../src/core/name.ts';
import { SlotChannel } from '../../../src/core/naming/slot-channel.ts';
import { Status, StatusCode } from '../../../src/core/status.ts';
import { connectTcp, listenTcp } from '../../../src/node/transport/tcp.ts';
import { ManualClock } from '../../../src/testing/manual-clock.ts';
import {
  AnsweringTransport,
  clockOf,
  collect,
  creditHex,
  dataHex,
  elapseUntil,
  empty,
  expectCode,
  expectRejectsStatus,
  expectThrowsStatus,
  Incoming,
  isControl,
  limitsHex,
  muxPair,
  quiet,
  type RawPeer,
  rawOptions,
  rawPair,
  Subframes,
  Subscription,
  settle,
  settleRaw,
  streamOf,
  tracked,
} from './support.ts';

const credit = MuxControlType.credit;

function decodeCredit(payload: Uint8Array): MuxCredit {
  return MuxCredit.decode(payload);
}

function expectCredit(actual: MuxCredit, id: number, bytes: number): void {
  expect(`${actual}`).toBe(`${new MuxCredit(id, bytes)}`);
}

describe('send window', () => {
  test('a sender waits for credit, which admits subframes in order', async () => {
    const [mux, raw] = rawPair();
    raw.send(limitsHex(100));
    await settleRaw(raw);
    const channel = mux.open(empty);
    expect(await raw.nextHex()).toBe('12 02 00');
    expect(channel.sendWindow).toBe(100);
    expect(channel.maxSubframeLength).toBe(34);
    // Each costs its length plus 16: 50, the most a frame may cost.
    const sent: number[] = [];
    for (let i = 0; i < 4; i++) {
      const subframe = new Uint8Array(34);
      subframe[0] = i;
      void channel.send(subframe).then(() => sent.push(i));
    }
    await settle();
    expect(sent).toEqual([0, 1]);
    expect(channel.sendWindow).toBe(0);
    for (let i = 0; i < 2; i++) {
      const frame = await raw.next();
      expect(frame.command).toBe(MuxCommand.data);
      expect(frame.payload[0]).toBe(i);
    }
    // Not enough for the next: nothing moves.
    raw.send(creditHex(2, 49));
    await settleRaw(raw);
    expect(sent).toEqual([0, 1]);
    expect(channel.sendWindow).toBe(49);
    raw.send(creditHex(2, 51));
    for (let i = 2; i < 4; i++) {
      expect((await raw.next()).payload[0]).toBe(i);
    }
    await settle();
    expect(sent).toEqual([0, 1, 2, 3]);
    expect(channel.sendWindow).toBe(0);
    await mux.close();
  });

  test('a subframe costing more than half the initial window is refused', async () => {
    const [mux, raw] = rawPair();
    const early = mux.open(empty);
    raw.send(limitsHex(1000));
    await settleRaw(raw);
    const late = mux.open(empty);
    // Opened before the LIMITS arrived: 64 KiB.
    expect(early.sendWindow).toBe(65536);
    expect(early.maxSubframeLength).toBe(32752);
    expect(late.sendWindow).toBe(1000);
    expect(late.maxSubframeLength).toBe(484);
    expectThrowsStatus(() => late.send(new Uint8Array(485)), StatusCode.frameTooLarge);
    expectThrowsStatus(() => early.send(new Uint8Array(32753)), StatusCode.frameTooLarge);
    await late.send(new Uint8Array(484));
    await early.send(new Uint8Array(32752));
    expect(late.sendWindow).toBe(500);
    expect(early.sendWindow).toBe(32768);
    await mux.close();
  });

  test('CREDIT beyond 2^32 - 1 is a protocol error', async () => {
    const [mux, raw] = rawPair();
    raw.send(limitsHex(MuxLimits.maxWindow - 10));
    await settleRaw(raw);
    const channel = mux.open(empty);
    raw.send(creditHex(channel.id, 10));
    await settleRaw(raw);
    expect(channel.sendWindow).toBe(MuxLimits.maxWindow);
    raw.send(creditHex(channel.id, 1));
    const goAway = await raw.nextControl(MuxControlType.goAway);
    expect(goAway.goAwayStatus.known).toBe(StatusCode.protocolError);
    expect((await mux.done).known).toBe(StatusCode.protocolError);
  });

  test('CREDIT after our CLOSE or for an unknown channel is ignored', async () => {
    const [mux, raw] = rawPair();
    const channel = mux.open(empty);
    await raw.next();
    void channel.close();
    expect(await raw.nextHex()).toBe('22 02 00');
    raw.send(creditHex(2, 0xffffffff));
    raw.send(creditHex(4, 100));
    await settleRaw(raw);
    expect(channel.sendWindow).toBe(65536);
    expect(mux.isOpen).toBe(true);
    await mux.close();
  });

  test('close waits behind subframes waiting for credit', async () => {
    const [mux, raw] = rawPair();
    raw.send(limitsHex(64));
    await settleRaw(raw);
    const channel = mux.open(empty);
    await raw.next();
    // Each costs 32: two fit the window, the third waits.
    const sends = [0, 1, 2].map(() => channel.send(new Uint8Array(16)));
    const closed = channel.close(Status.of(StatusCode.cancelled));
    expect(channel.canSend).toBe(false);
    expect(channel.state).toBe('halfClosedLocal');
    await sends[1];
    for (let i = 0; i < 2; i++) {
      expect((await raw.next()).payload).toHaveLength(16);
    }
    raw.send(creditHex(2, 32));
    await sends[2];
    expect((await raw.next()).payload).toHaveLength(16);
    const close = await raw.next();
    expect(close.command).toBe(MuxCommand.close);
    expect(Status.decode(close.payload).known).toBe(StatusCode.cancelled);
    raw.send('22 02 00');
    await closed;
    expectCode(await channel.done, StatusCode.cancelled);
    await mux.close();
  });

  test("the peer's CLOSE drops waiting subframes and sends ours", async () => {
    const [mux, raw] = rawPair();
    raw.send(limitsHex(64));
    await settleRaw(raw);
    const channel = mux.open(empty);
    await raw.next();
    await channel.send(new Uint8Array(16));
    await channel.send(new Uint8Array(16));
    await raw.next();
    await raw.next();
    const waiting = channel.send(new Uint8Array(16));
    const closed = channel.close(Status.of(StatusCode.cancelled));
    raw.send('22 02 00');
    await expectRejectsStatus(waiting, StatusCode.failedPrecondition);
    const close = await raw.next();
    expect(close.command).toBe(MuxCommand.close);
    expect(Status.decode(close.payload).known).toBe(StatusCode.cancelled);
    await closed;
    expect(channel.state).toBe('closed');
    await mux.close();
  });

  test('an unconfirmed close sends the CLOSE waiting behind subframes', async () => {
    const [mux, raw] = rawPair({ options: { ...rawOptions, closeConfirmTimeout: 30 } });
    raw.send(limitsHex(64));
    await settleRaw(raw);
    const channel = mux.open(empty);
    await raw.next();
    await channel.send(new Uint8Array(16));
    await channel.send(new Uint8Array(16));
    await raw.next();
    await raw.next();
    const waiting = channel.send(new Uint8Array(16));
    const closing = channel.close();
    expect(await elapseUntil(clockOf(mux), closing)).toBe(30);
    await expectRejectsStatus(waiting, StatusCode.failedPrecondition);
    // Not OK: what was sent did not all go out.
    const close = await raw.next();
    expect(close.command).toBe(MuxCommand.close);
    const status = Status.decode(close.payload);
    expect(status.known).toBe(StatusCode.deadlineExceeded);
    expect(status.reason).toContain('16 bytes not sent');
    expectCode(await channel.done, StatusCode.deadlineExceeded);
    expect(mux.unconfirmedCloseCount).toBe(1);
    await mux.close();
  });

  test('a bulk payload needs a window that takes a byte', async () => {
    const [mux, raw] = rawPair();
    raw.send(limitsHex(16));
    await settleRaw(raw);
    const channel = mux.open(empty);
    channel.priority = 'bulk';
    expectThrowsStatus(() => channel.send(new Uint8Array(10)), StatusCode.frameTooLarge);
    // An empty subframe is not split, and fits.
    await channel.send(empty);
    await mux.close();
  });

  test('sink.addStream fails on a subframe the channel refuses', async () => {
    const [mux, raw] = rawPair();
    raw.send(limitsHex(1000));
    await settleRaw(raw);
    const channel = mux.open(empty);
    await expectRejectsStatus(
      streamOf([new Uint8Array(10), new Uint8Array(1000)]).pipeTo(channel.writable),
      StatusCode.frameTooLarge,
    );
    await mux.close();
  });

  test('a lost connection fails waiting subframes', async () => {
    const [mux, raw] = rawPair();
    raw.send(limitsHex(64));
    await settleRaw(raw);
    const channel = mux.open(empty);
    await channel.send(new Uint8Array(16));
    await channel.send(new Uint8Array(16));
    const waiting = channel.send(new Uint8Array(16));
    await raw.close();
    await expectRejectsStatus(waiting, StatusCode.failedPrecondition);
    expectCode(await mux.done, StatusCode.connectionLost);
  });

  test('a failure no one awaits is not reported', async () => {
    // Dart's `sink.add` drops the subframe silently; a write to the
    // writable is a send whose rejection the stream hands to the caller
    // (design section 12, item 8), so only `send` is left unawaited here.
    const [mux, raw] = rawPair();
    raw.send(limitsHex(64));
    await settleRaw(raw);
    const channel = mux.open(empty);
    for (let i = 0; i < 4; i++) {
      void channel.send(new Uint8Array(16));
    }
    void channel.whenWritable;
    await mux.close();
    await settle();
  });

  test('sink.addStream waits for each subframe', async () => {
    const [mux, raw] = rawPair();
    raw.send(limitsHex(64));
    await settleRaw(raw);
    const channel = mux.open(empty);
    await raw.next();
    let pulled = 0;
    const source = new ReadableStream<Uint8Array>(
      {
        pull: (controller) => {
          const subframe = new Uint8Array(16);
          subframe[0] = pulled++;
          controller.enqueue(subframe);
          if (pulled === 4) {
            controller.close();
          }
        },
      },
      { highWaterMark: 0 },
    );
    const added = source.pipeTo(channel.writable, { preventClose: true });
    await settle();
    // Two frames sent, one waiting for credit, the last still in the
    // source, which is not read.
    expect(channel.sendWindow).toBe(0);
    expect(pulled).toBe(3);
    for (let i = 0; i < 2; i++) {
      expect((await raw.next()).payload[0]).toBe(i);
    }
    raw.send(creditHex(2, 64));
    await added;
    for (let i = 2; i < 4; i++) {
      expect((await raw.next()).payload[0]).toBe(i);
    }
    await mux.close();
  });
});

describe('readiness', () => {
  test(
    'whenWritable waits while the window has no room for the largest subframe, completes on ' +
      'CREDIT, and at once with room',
    async () => {
      const [mux, raw] = rawPair();
      raw.send(limitsHex(100));
      await settleRaw(raw);
      const channel = mux.open(empty);
      expect(await raw.nextHex()).toBe('12 02 00');
      // The largest subframe is 34 bytes, costing 50.
      expect(channel.maxSubframeLength).toBe(34);
      expect(channel.isWritable).toBe(true);
      await channel.whenWritable;
      await channel.send(new Uint8Array(34));
      expect(channel.isWritable).toBe(true);
      await channel.send(new Uint8Array(10));
      expect(channel.sendWindow).toBe(24);
      expect(channel.isWritable).toBe(false);
      const writable = tracked(channel.whenWritable);
      // Not enough yet.
      raw.send(creditHex(channel.id, 20));
      await settleRaw(raw);
      await settle();
      expect(writable.settled).toBe(false);
      expect(channel.isWritable).toBe(false);
      raw.send(creditHex(channel.id, 6));
      await settleRaw(raw);
      await writable.promise;
      expect(channel.sendWindow).toBe(50);
      expect(channel.isWritable).toBe(true);
      await channel.whenWritable;
      await mux.close();
    },
  );

  test('a subframe waiting for credit keeps the channel not writable until it went', async () => {
    const [mux, raw] = rawPair();
    raw.send(limitsHex(100));
    await settleRaw(raw);
    const channel = mux.open(empty);
    await channel.send(new Uint8Array(34));
    await channel.send(new Uint8Array(34));
    const queued = channel.send(new Uint8Array(34));
    const writable = tracked(channel.whenWritable);
    // Room for the waiting one only: it goes, and nothing is left.
    raw.send(creditHex(channel.id, 50));
    await settleRaw(raw);
    await queued;
    await settle();
    expect(writable.settled).toBe(false);
    raw.send(creditHex(channel.id, 50));
    await settleRaw(raw);
    await writable.promise;
    expect(channel.isWritable).toBe(true);
    await mux.close();
  });

  test('whenWritable fails when the channel stops sending, by either side', async () => {
    const [mux, raw] = rawPair();
    raw.send(limitsHex(100));
    await settleRaw(raw);
    const ours = mux.open(empty);
    const theirs = mux.open(empty);
    for (let i = 0; i < 2; i++) {
      await ours.send(new Uint8Array(34));
      await theirs.send(new Uint8Array(34));
    }
    expect(ours.isWritable).toBe(false);
    const oursWaiting = ours.whenWritable;
    const theirsWaiting = theirs.whenWritable;
    void ours.close();
    await expectRejectsStatus(oursWaiting, StatusCode.failedPrecondition);
    raw.send(hexString(MuxFrame.close(theirs.id).encode()));
    await expectRejectsStatus(theirsWaiting, StatusCode.failedPrecondition);
    expect(ours.isWritable).toBe(false);
    expect(theirs.isWritable).toBe(false);
    await expectRejectsStatus(ours.whenWritable, StatusCode.failedPrecondition);
    // Never unhandled when nobody waits.
    void ours.whenWritable;
    const dropped = mux.open(empty);
    await dropped.send(new Uint8Array(34));
    await dropped.send(new Uint8Array(34));
    void dropped.whenWritable;
    await mux.close();
    await settle();
  });

  test('a channel opened before the peer LIMITS becomes writable when it arrives', async () => {
    const [mux, raw] = rawPair({ options: { ...rawOptions, awaitPeerLimits: true } });
    const channel = mux.open(empty);
    expect(channel.isWritable).toBe(false);
    const writable = tracked(channel.whenWritable);
    await settle();
    expect(writable.settled).toBe(false);
    raw.send(limitsHex(1000));
    await writable.promise;
    expect(channel.isWritable).toBe(true);
    expect(channel.sendWindow).toBe(1000);
    await mux.close();
  });
});

describe('DATA waits for the peer LIMITS', () => {
  const waiting = { ...rawOptions, awaitPeerLimits: true };

  test('LIMITS delivered synchronously during construction ends the wait', async () => {
    // Dart's peer answers the LIMITS during the constructor's write, and
    // the wait is over when the constructor returns. Here the answer is
    // queued as the LIMITS is written and read in a later microtask.
    const clock = new ManualClock();
    const transport = new AnsweringTransport((frame) =>
      isControl(MuxFrame.decode(frame), MuxControlType.limits) ? [hexBytes(limitsHex(256))] : [],
    );
    const mux = new FrameMuxConnection(transport, { ...quiet, clock, isInitiator: true });
    await settle();
    expect(mux.peerLimits?.initialWindow).toBe(256);
    const channel = mux.open(empty);
    expect(channel.sendWindow).toBe(256);
    await channel.send(new Uint8Array(10));
    await mux.close();
  });

  for (const slot of [false, true]) {
    test(`addStream reports a size refusal after LIMITS arrives (slot: ${slot})`, async () => {
      const [mux, raw] = rawPair({ options: waiting });
      const channel = mux.open(empty);
      const writable = slot
        ? new SlotChannel(Name.of('test'), 0, channel, async () => undefined).writable
        : channel.writable;
      const added = streamOf([new Uint8Array(1000)]).pipeTo(writable);
      await settle();
      raw.send(limitsHex(256));
      await expectRejectsStatus(added, StatusCode.frameTooLarge);
      await mux.close();
    });
  }

  test('OPEN goes at once, DATA once the LIMITS arrived, within the window it announced', async () => {
    const [mux, raw] = rawPair({ options: waiting });
    const channel = mux.open(empty);
    expect(await raw.nextHex()).toBe('12 02 00');
    expect(channel.sendWindow).toBe(0);
    const sent: number[] = [];
    for (let i = 0; i < 3; i++) {
      const subframe = new Uint8Array(100);
      subframe[0] = i;
      void channel.send(subframe).then(() => sent.push(i));
    }
    await settle();
    expect(sent).toEqual([]);
    // An embedded device: a window of 256, so frames of at most 128.
    raw.send(limitsHex(256));
    // Two frames of 116 fit; the third waits for credit.
    let used = 0;
    for (let i = 0; i < 2; i++) {
      const frame = await raw.next();
      expect(frame.command).toBe(MuxCommand.data);
      expect(frame.payload[0]).toBe(i);
      used += MuxCredit.costOf(frame.payload.length);
    }
    expect(used).toBeLessThanOrEqual(256);
    await settleRaw(raw);
    expect(sent).toEqual([0, 1]);
    expect(channel.sendWindow).toBe(256 - used);
    expect(channel.maxSubframeLength).toBe(112);
    raw.send(creditHex(2, used));
    expect((await raw.next()).payload[0]).toBe(2);
    await settle();
    expect(sent).toEqual([0, 1, 2]);
    await mux.close();
  });

  test('a subframe the announced window does not take fails its send', async () => {
    const [mux, raw] = rawPair({ options: waiting });
    const channel = mux.open(empty);
    await raw.next();
    // Taken against the 64 KiB assumption.
    expect(channel.maxSubframeLength).toBe(32752);
    const big = channel.send(new Uint8Array(1000));
    const small = channel.send(new Uint8Array(10));
    raw.send(limitsHex(256));
    await expectRejectsStatus(big, StatusCode.frameTooLarge);
    await small;
    const frame = await raw.next();
    expect(frame.payload).toHaveLength(10);
    await mux.close();
  });

  test('a peer whose first frame is not LIMITS ends the wait', async () => {
    const [mux, raw] = rawPair({ options: waiting });
    const channel = mux.open(empty);
    await raw.next();
    const sent = channel.send(new Uint8Array(5));
    raw.send('02 00 00 01 5E');
    await sent;
    expect(channel.sendWindow).toBe(65536 - 21);
    // The wait ends before the PING is handled: the DATA, then the PONG.
    expect((await raw.next()).payload).toHaveLength(5);
    await raw.nextControl(MuxControlType.pong);
    // A LIMITS arriving later applies to channels opened after it.
    raw.send(limitsHex(1000));
    await settleRaw(raw);
    expect(mux.open(empty).sendWindow).toBe(1000);
    expect(channel.sendWindow).toBe(65536 - 21);
    await mux.close();
  });

  test('the wait ends after the keep-alive timeout', async () => {
    const [mux, raw] = rawPair({ options: { ...waiting, keepAliveTimeout: 50 } });
    const channel = mux.open(empty);
    await raw.next();
    expect(await elapseUntil(clockOf(mux), channel.send(new Uint8Array(5)))).toBe(50);
    expect(channel.sendWindow).toBe(65536 - 21);
    expect((await raw.next()).payload).toHaveLength(5);
    await mux.close();
  });

  test('a close waits behind the DATA that waits', async () => {
    const [mux, raw] = rawPair({ options: waiting });
    const channel = mux.open(empty);
    await raw.next();
    void channel.send(new Uint8Array(5));
    void channel.close();
    raw.send(limitsHex(1000));
    expect((await raw.next()).command).toBe(MuxCommand.data);
    expect(await raw.nextHex()).toBe('22 02 00');
    await mux.close();
  });

  test('between two connections nothing waits longer than a round trip', async () => {
    const [a, b] = muxPair({
      initiator: { ...quiet, initialWindow: 256 },
      acceptor: { ...quiet, initialWindow: 256 },
    });
    // Opened before either LIMITS arrived: the window is still the 256
    // each side announces.
    const ca = a.open(empty);
    const received: number[] = [];
    void (async () => {
      for await (const c of b.incoming) {
        void (async () => {
          for await (const d of c.readable) {
            received.push(d.length);
          }
        })();
      }
    })();
    for (let i = 0; i < 10; i++) {
      void ca.send(new Uint8Array(100));
    }
    await settle();
    expect(ca.maxSubframeLength).toBe(112);
    expect(received).toHaveLength(10);
    await a.close();
    await b.close();
  });
});

describe('receive window', () => {
  for (const rejected of [false, true]) {
    test(`DATA beyond the remaining window after CLOSE is refused (rejected: ${rejected})`, async () => {
      const [mux, raw] = rawPair({
        options: { ...rawOptions, maxChannels: 1, closeConfirmTimeout: 20 },
      });
      const channel = mux.open(empty);
      const id = rejected ? 3 : channel.id;
      if (rejected) {
        raw.send(hexString(MuxFrame.open(id, empty).encode()));
      } else {
        await elapseUntil(clockOf(mux), channel.close());
      }
      raw.send(hexString(MuxFrame.data(id, new Uint8Array(32752)).encode()));
      raw.send(hexString(MuxFrame.data(id, new Uint8Array(32752)).encode()));
      await settle();
      expect(mux.isOpen).toBe(true);
      raw.send(hexString(MuxFrame.data(id, empty).encode()));
      await settle();
      expect(mux.isOpen).toBe(false);
      expectCode(await mux.done, StatusCode.protocolError);
    });
  }

  test('grant inside an automatic listener counts the delivered frame', async () => {
    const [mux, raw] = rawPair({
      options: { ...rawOptions, announceLimits: true, initialWindow: MuxLimits.maxWindow },
    });
    const channel = mux.open(empty);
    const checked = (async () => {
      for await (const _ of channel.readable) {
        try {
          channel.grant(1);
          return undefined;
        } catch (error) {
          return error;
        }
      }
      return 'ended';
    })();
    raw.send(hexString(MuxFrame.data(channel.id, empty).encode()));
    expect(await checked).toBeInstanceOf(RangeError);
    expect(channel.receiveWindow).toBe(MuxLimits.maxWindow - MuxCredit.frameOverhead);
    await mux.close();
  });

  /** A channel the mux opened over [raw], after the mux's LIMITS of [window]. */
  async function opened(
    window: number,
    manual = false,
  ): Promise<[FrameMuxConnection, RawPeer, MuxChannel]> {
    const [mux, raw] = rawPair({
      options: { ...rawOptions, announceLimits: true, initialWindow: window },
    });
    await raw.nextControl(MuxControlType.limits);
    const channel = mux.open(empty);
    channel.manualCredit = manual;
    expect(await raw.nextHex()).toBe('12 02 00');
    return [mux, raw, channel];
  }

  const data = (length: number): string => dataHex(2, length);

  test(
    'automatic credit: returned once half the window is taken; a paused subscription takes ' +
      'nothing',
    async () => {
      const [mux, raw, channel] = await opened(1000);
      const taken: number[] = [];
      const sub = new Subscription(channel, (d) => taken.push(d.length)).pause();
      // 6 subframes of cost 100: nothing taken while paused.
      for (let i = 0; i < 6; i++) {
        raw.send(data(84));
      }
      await settleRaw(raw);
      expect(channel.receiveWindow).toBe(400);
      expect(channel.bufferedBytes).toBe(600);
      expect(mux.bufferedBytes).toBe(600);
      sub.resume();
      // Taken one by one: the credit goes with the fifth, half the window
      // (Dart's listener takes all six, then returns 600).
      expectCredit(decodeCredit((await raw.nextControl(credit)).payload), 2, 500);
      await settle();
      expect(taken).toHaveLength(6);
      expect(channel.receiveWindow).toBe(900);
      expect(mux.bufferedBytes).toBe(0);
      // Under half: kept until more is taken.
      raw.send(data(84));
      raw.send(data(84));
      await settleRaw(raw);
      expect(channel.receiveWindow).toBe(700);
      for (let i = 0; i < 3; i++) {
        raw.send(data(84));
      }
      expectCredit(decodeCredit((await raw.nextControl(credit)).payload), 2, 500);
      await sub.cancel();
      await mux.close();
    },
  );

  test('manual credit: returned as the layer reports consumption', async () => {
    const [mux, raw, channel] = await opened(1000, true);
    const taken: Uint8Array[] = [];
    new Subscription(channel, (d) => taken.push(d));
    for (let i = 0; i < 6; i++) {
      raw.send(data(84));
    }
    await settleRaw(raw);
    await settle();
    // Taken, not consumed: held, not counted by the connection.
    expect(taken).toHaveLength(6);
    expect(channel.bufferedBytes).toBe(0);
    expect(channel.heldBytes).toBe(600);
    expect(mux.bufferedBytes).toBe(0);
    expect(channel.receiveWindow).toBe(400);
    channel.consumed(84);
    channel.consumed(168, 2);
    await settleRaw(raw);
    expect(channel.receiveWindow).toBe(400);
    expect(channel.heldBytes).toBe(300);
    // Half the window: returned.
    channel.consumed((taken[3] as Uint8Array).length);
    channel.consumed((taken[4] as Uint8Array).length);
    expectCredit(decodeCredit((await raw.nextControl(credit)).payload), 2, 500);
    expect(channel.receiveWindow).toBe(900);
    // More than delivered and not consumed: a bug of the layer.
    expect(() => channel.consumed(200)).toThrow(Error);
    expect(() => channel.consumed(-1)).toThrow(RangeError);
    // Back to automatic: what is held counts as consumed (100 bytes), and
    // delivery is consumption again.
    channel.manualCredit = false;
    expect(channel.heldBytes).toBe(0);
    raw.send(data(384));
    expectCredit(decodeCredit((await raw.nextControl(credit)).payload), 2, 500);
    expect(channel.receiveWindow).toBe(1000);
    expect(() => channel.consumed(0)).toThrow(Error);
    await mux.close();
  });

  test('grant raises the window at once, with the credit not returned yet', async () => {
    // Read subframe by subframe: a paused Dart subscription is a reader
    // that does not read.
    const [mux, raw, channel] = await opened(1000);
    const reader = new Subframes(channel);
    raw.send(data(84));
    await reader.next();
    expect(channel.receiveWindow).toBe(900);
    // With the 100 bytes consumed and not returned yet.
    channel.grant(10000);
    expectCredit(decodeCredit((await raw.nextControl(credit)).payload), 2, 10100);
    expect(channel.receiveWindow).toBe(11000);
    // The peer may use all of it.
    for (let i = 0; i < 10; i++) {
      raw.send(data(984));
    }
    await settleRaw(raw);
    expect(channel.receiveWindow).toBe(1000);
    // Credit now comes in batches of half the raised window, 5500: with
    // the sixth subframe taken (Dart's listener takes all ten, then
    // returns 10000).
    await reader.take(10);
    expectCredit(decodeCredit((await raw.nextControl(credit)).payload), 2, 6000);
    expect(channel.receiveWindow).toBe(7000);
    expect(() => channel.grant(0)).toThrow(RangeError);
    // What is buffered or held comes back as credit too, and so does what
    // was consumed and not returned (4000 here): the window could go over
    // 2^32 - 1.
    raw.send(data(584));
    await settleRaw(raw);
    expect(channel.bufferedBytes).toBe(600);
    expect(channel.receiveWindow).toBe(6400);
    expect(() => channel.grant(MuxLimits.maxWindow - 11000 + 1)).toThrow(RangeError);
    channel.grant(MuxLimits.maxWindow - 11000);
    await reader.cancel();
    expect(() => channel.grant(MuxLimits.maxWindow)).toThrow(RangeError);
    expect(mux.isOpen).toBe(true);
    await mux.close();
  });

  test("no credit once our CLOSE is out or the peer's came", async () => {
    const [mux, raw, channel] = await opened(1000);
    const sub = new Subscription(channel).pause();
    raw.send(data(484));
    await settleRaw(raw);
    void channel.close();
    expect(await raw.nextHex()).toBe('22 02 00');
    sub.resume();
    channel.grant(100);
    // In flight before the peer saw the CLOSE: still within the window.
    raw.send(data(484));
    raw.send('22 02 00');
    await channel.done;
    await settleRaw(raw);
    expect(channel.receiveWindow).toBe(0);
    await sub.cancel();
    await mux.close();
  });

  test('a frame beyond the window is a protocol error, logged with its channel', async () => {
    const [mux, raw, channel] = await opened(100);
    raw.send(data(84));
    raw.send(data(0));
    const goAway = await raw.nextControl(MuxControlType.goAway);
    expect(goAway.goAwayStatus.known).toBe(StatusCode.protocolError);
    expect(goAway.goAwayStatus.reason).toBe(
      'DATA costing 16 bytes on channel 2, beyond the window of 0 granted',
    );
    expect((await mux.done).known).toBe(StatusCode.protocolError);
    expectCode(await channel.done, StatusCode.connectionLost);
  });
});

describe('between two connections', () => {
  async function exchange(a: FrameMuxConnection, b: FrameMuxConnection): Promise<void> {
    await a.ping();
    await b.ping();
    const atB = new Incoming(b);
    const ca = a.open(empty);
    const cb = await atB.next();
    const sizes = Array.from({ length: 200 }, (_, i) => (i * 997) % 30000);
    const sendAll = async (channel: MuxChannel): Promise<void> => {
      for (const size of sizes) {
        await channel.send(new Uint8Array(size).fill(size & 0xff));
      }
    };
    const receiveAll = async (channel: MuxChannel): Promise<void> => {
      const received = await new Subframes(channel).take(sizes.length);
      expect(received.map((d) => d.length)).toEqual(sizes);
      for (const d of received) {
        expect(d.every((byte) => byte === (d.length & 0xff))).toBe(true);
      }
    };
    await Promise.all([sendAll(ca), sendAll(cb), receiveAll(ca), receiveAll(cb)]);
    await a.ping();
    await b.ping();
    // Nothing in flight: each side's view of each window agrees.
    expect(ca.sendWindow).toBe(cb.receiveWindow);
    expect(cb.sendWindow).toBe(ca.receiveWindow);
    expect(ca.sendWindow).toBeGreaterThan(32768);
    expect(cb.sendWindow).toBeGreaterThan(32768);
    await ca.close();
    await atB.cancel();
  }

  test('window accounting both ways over memory', async () => {
    const [a, b] = muxPair();
    await exchange(a, b);
    await a.close();
  });

  test('window accounting both ways over TCP', async () => {
    // On the loopback, with the system clock: the TCP transport's close
    // timers are real ones, and nothing here waits for them.
    const listener = await listenTcp('127.0.0.1', 0);
    const accepted = listener.transports[Symbol.asyncIterator]().next();
    const a = new FrameMuxConnection(await connectTcp('127.0.0.1', listener.port), {
      ...quiet,
      isInitiator: true,
    });
    const next = await accepted;
    if (next.done === true) {
      throw new Error('no connection accepted');
    }
    const b = new FrameMuxConnection(next.value.transport, { ...quiet, isInitiator: false });
    await listener.close();
    await exchange(a, b);
    await a.close();
    await b.done;
  });

  test('a slow consumer stalls its own channel, another one flows', async () => {
    const [a, b] = muxPair();
    await a.ping();
    const atB = new Incoming(b);
    const slow = a.open(empty);
    const fast = a.open(empty);
    const slowAtB = await atB.next();
    const fastAtB = await atB.next();
    const fastReceived = (async () => {
      let n = 0;
      for await (const d of fastAtB.readable) {
        n += d.length;
      }
      return n;
    })();
    let slowSent = 0;
    const slowDone = (async () => {
      for (let i = 0; i < 100; i++) {
        await slow.send(new Uint8Array(16000));
        slowSent++;
      }
    })();
    for (let i = 0; i < 100; i++) {
      await fast.send(new Uint8Array(16000));
    }
    await a.ping();
    // The channel not read holds its window, 4 subframes of 16016; the
    // fifth waits for credit.
    expect(slowSent).toBe(4);
    expect(slowAtB.bufferedBytes).toBe(4 * 16016);
    expect(b.bufferedBytes).toBe(4 * 16016);
    await fast.close();
    expect(await fastReceived).toBe(100 * 16000);
    const slowRead = collect(slowAtB);
    await slowDone;
    await slow.close();
    expect((await slowRead).length).toBe(100);
    await a.close();
    await atB.cancel();
  });

  test('the connection backstop pauses reading for all channels together', async () => {
    const [a, b] = muxPair({ acceptor: { ...quiet, receiveHighWaterMarkBytes: 100000 } });
    await a.ping();
    const atB = new Incoming(b);
    const channels = [0, 1, 2, 3].map(() => a.open(empty));
    const subs: Subscription[] = [];
    for (let i = 0; i < 4; i++) {
      const channel = await atB.next();
      // The first held by a layer with manual credit, which does not
      // count; three not read at all.
      channel.manualCredit = i === 0;
      const sub = new Subscription(channel);
      if (i > 0) {
        sub.pause();
      }
      subs.push(sub);
    }
    for (const channel of channels) {
      for (let i = 0; i < 4; i++) {
        void channel.send(new Uint8Array(16000));
      }
    }
    await settle();
    const layer = [...b.channels].find((c) => c.manualCredit) as MuxChannel;
    expect(layer.heldBytes).toBe(4 * 16016);
    // Each channel not read holds up to its window, 64064 bytes, and the
    // second pushes the total over the mark.
    expect(b.bufferedBytes).toBeGreaterThan(100000);
    expect(b.isReceivePaused).toBe(true);
    // A ping is not answered while reading is paused.
    const answered = tracked(a.ping());
    await settle();
    expect(answered.settled).toBe(false);
    // Two of them are read: reading resumes below half the mark, and the
    // third holds its window.
    (subs[1] as Subscription).resume();
    (subs[2] as Subscription).resume();
    await settle();
    expect(b.isReceivePaused).toBe(false);
    expect(answered.settled).toBe(true);
    expect(b.bufferedBytes).toBe(4 * 16016);
    for (const sub of subs) {
      await sub.cancel();
    }
    await a.close();
    await atB.cancel();
  });
});
