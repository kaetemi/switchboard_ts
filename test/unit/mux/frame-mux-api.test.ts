// What the TypeScript frame mux has beside the Dart API, which the Dart
// suites cannot check: the Web Streams of a channel (design section 12,
// items 8 and 10), the abort signals of `ping` and `identify`, `await
// using`, the `adoptIncoming` and `admitAfterGoAway` hooks (items 3 and 9)
// and the argument checks of the constructor and the channel.
import { describe, expect, test } from 'vitest';
import { hexBytes, hexString } from '../../../src/core/bytes.ts';
import type { MuxChannel } from '../../../src/core/mux/contract.ts';
import { FrameMuxConnection } from '../../../src/core/mux/frame-mux-connection.ts';
import {
  MuxCommand,
  MuxControlType,
  MuxCredit,
  type MuxFrame,
} from '../../../src/core/mux/mux-frame.ts';
import { Status, StatusCode, SwitchboardError } from '../../../src/core/status.ts';
import type { Logger, LogLevel } from '../../../src/core/util/logger.ts';
import { stubCredential } from '../../support/identity/stub-verifier.ts';
import {
  collect,
  creditHex,
  dataHex,
  empty,
  expectCode,
  expectRejectsStatus,
  expectStatus,
  firstIncoming,
  Incoming,
  limitsHex,
  memoryPair,
  muxPair,
  quiet,
  rawOptions,
  rawPair,
  rejection,
  SteppingClock,
  Subframes,
  settle,
  settleRaw,
  takeIncoming,
  tracked,
} from './support.ts';

describe('the readable of a channel', () => {
  test('delivers a subframe, and returns its credit, only on a read', async () => {
    const [mux, raw] = rawPair({ options: { ...rawOptions, announceLimits: true } });
    await raw.nextControl(MuxControlType.limits);
    const channel = mux.open(empty);
    await raw.next();
    for (let i = 0; i < 3; i++) {
      raw.send(`02 02 00 0${i}`);
    }
    await settleRaw(raw);
    // Nothing read: everything buffered, nothing returned.
    expect(channel.bufferedBytes).toBe(3 * 17);
    expect(channel.receiveWindow).toBe(65536 - 3 * 17);
    const reader = channel.readable.getReader();
    expect(hexString((await reader.read()).value as Uint8Array)).toBe('00');
    expect(channel.bufferedBytes).toBe(2 * 17);
    reader.releaseLock();
    await mux.close();
  });

  test('breaking out of for await discards what arrives; the channel stays open', async () => {
    const [a, b] = muxPair();
    const atB = new Incoming(b);
    const ca = a.open(empty);
    const cb = await atB.next();
    void ca.send(hexBytes('01'));
    for await (const subframe of cb.readable) {
      expect(hexString(subframe)).toBe('01');
      break;
    }
    expect(cb.state).toBe('open');
    expect(cb.hasListener).toBe(false);
    // Dropped as it arrives, its credit returned: the sender is never held.
    for (let i = 0; i < 10; i++) {
      await ca.send(new Uint8Array(30000));
    }
    await a.ping();
    expect(cb.bufferedBytes).toBe(0);
    // The other direction still works.
    void cb.send(hexBytes('02'));
    expect(await new Subframes(ca).nextHex()).toBe('02');
    await ca.close();
    expectStatus(await cb.done, Status.ok);
    await a.close();
  });

  test('ends after the subframes buffered when the channel closed', async () => {
    const [mux, raw] = rawPair();
    raw.send('12 03 00');
    const channel = await firstIncoming(mux);
    raw.send('02 03 00 AA');
    raw.send('02 03 00 BB');
    raw.send('22 03 00 05 00 78');
    expect(await raw.nextHex()).toBe('22 03 00');
    expectStatus(await channel.done, Status.of(StatusCode.notFound, 'x'));
    expect((await collect(channel)).map(hexString)).toEqual(['AA', 'BB']);
    await mux.close();
  });
});

describe('for await over a channel', () => {
  test('takes its subframes in order, returning their credit, until it ends', async () => {
    const [mux, raw] = rawPair();
    raw.send('12 03 00');
    const channel = await firstIncoming(mux);
    for (let i = 0; i < 3; i++) {
      raw.send(dataHex(3, 30000));
    }
    raw.send('22 03 00');
    const sizes: number[] = [];
    for await (const subframe of channel) {
      sizes.push(subframe.length);
    }
    expect(sizes).toEqual([30000, 30000, 30000]);
    // Delivery returned the credit of the first two, half the window.
    const credit = await raw.nextControl(MuxControlType.credit);
    expect(`${MuxCredit.decode(credit.payload)}`).toBe(`${new MuxCredit(3, 2 * 30016)}`);
    expect(await raw.nextHex()).toBe('22 03 00');
    expectStatus(await channel.done, Status.ok);
    await mux.close();
  });

  test('break discards what arrives, and the channel stays open', async () => {
    const [a, b] = muxPair();
    const ca = a.open(empty);
    const cb = await firstIncoming(b);
    void ca.send(hexBytes('01'));
    for await (const subframe of cb) {
      expect(hexString(subframe)).toBe('01');
      break;
    }
    expect(cb.state).toBe('open');
    expect(cb.hasListener).toBe(false);
    for (let i = 0; i < 10; i++) {
      await ca.send(new Uint8Array(30000));
    }
    await a.ping();
    expect(cb.bufferedBytes).toBe(0);
    await ca.close();
    expectStatus(await cb.done, Status.ok);
    await a.close();
  });

  test('an exception out of the loop stops it the same way', async () => {
    const [a, b] = muxPair();
    const ca = a.open(empty);
    const cb = await firstIncoming(b);
    void ca.send(hexBytes('01'));
    const loop = (async () => {
      for await (const _ of cb) {
        throw new Error('handler bug');
      }
    })();
    expect(((await rejection(loop)) as Error).message).toBe('handler bug');
    expect(cb.state).toBe('open');
    // The readable was cancelled: iterating again ends at once.
    expect((await cb[Symbol.asyncIterator]().next()).done).toBe(true);
    await a.close();
  });
});

describe('the writable of a channel', () => {
  test('a write waits for credit: the writer is not ready meanwhile', async () => {
    const [mux, raw] = rawPair();
    raw.send(limitsHex(64));
    await settleRaw(raw);
    const channel = mux.open(empty);
    await raw.next();
    const writer = channel.writable.getWriter();
    await writer.write(new Uint8Array(16));
    await writer.write(new Uint8Array(16));
    const third = tracked(writer.write(new Uint8Array(16)));
    await settle();
    expect(third.settled).toBe(false);
    expect(writer.desiredSize).toBe(0);
    raw.send(creditHex(channel.id, 32));
    await third.promise;
    expect(writer.desiredSize).toBe(1);
    await mux.close();
  });

  test('close closes the channel with OK and resolves once it is mutually closed', async () => {
    const [mux, raw] = rawPair();
    const channel = mux.open(empty);
    await raw.next();
    const closed = tracked(channel.writable.close());
    expect(await raw.nextHex()).toBe('22 02 00');
    await settle();
    expect(closed.settled).toBe(false);
    raw.send('22 02 00');
    await closed.promise;
    expectStatus(await channel.done, Status.ok);
    await mux.close();
  });

  test('abort closes with the reason as a status', async () => {
    const [mux, raw] = rawPair();
    const reasons: [unknown, Status][] = [
      [undefined, Status.of(StatusCode.cancelled)],
      [Status.of(StatusCode.notFound, 'gone'), Status.of(StatusCode.notFound, 'gone')],
      [
        SwitchboardError.of(StatusCode.permissionDenied, 'no'),
        Status.of(StatusCode.permissionDenied, 'no'),
      ],
      // An application code, which CLOSE must not carry, goes as UNKNOWN.
      [new Status(300, 'app'), Status.of(StatusCode.unknown, 'code 300: app')],
      [new Error('bug'), Status.of(StatusCode.internal, 'bug')],
    ];
    for (const [reason, status] of reasons) {
      const channel = mux.open(empty);
      await raw.next();
      void channel.writable.abort(reason);
      const close = await raw.next();
      expectStatus(Status.decode(close.payload), status);
      raw.send(hexString(close.encode()).slice(0, 8));
      expectStatus(await channel.done, status);
    }
    await mux.close();
  });

  test('a chunk that is not bytes is refused', async () => {
    const [a] = muxPair();
    const channel = a.open(empty);
    const writer = channel.writable.getWriter() as WritableStreamDefaultWriter<unknown>;
    expect(await rejection(writer.write('text'))).toBeInstanceOf(TypeError);
    await a.close();
  });

  test('pipeTo carries a channel into another with flow control', async () => {
    // A relay in one process: what b receives on one channel it sends on
    // another, to c; nothing waits beyond the windows.
    const [a, b] = muxPair();
    const [b2, c] = muxPair();
    const source = a.open(empty);
    const inbound = await firstIncoming(b);
    const outbound = b2.open(empty);
    const atC = await firstIncoming(c);
    const piped = inbound.readable.pipeTo(outbound.writable);
    const received = collect(atC);
    let sent = 0;
    for (let i = 0; i < 20; i++) {
      await source.send(new Uint8Array(30000));
      sent += 30000;
    }
    await source.close();
    await piped;
    const total = (await received).reduce((n, d) => n + d.length, 0);
    expect(total).toBe(sent);
    expectStatus(await atC.done, Status.ok);
    await a.close();
    await b2.close();
  });
});

describe('signals and disposal', () => {
  test('ping: an aborted signal rejects with its reason, the PONG is then ignored', async () => {
    const [mux, raw] = rawPair();
    const controller = new AbortController();
    const pong = mux.ping(hexBytes('01'), { signal: controller.signal });
    expect(await raw.nextHex()).toBe('02 00 00 01 01');
    const reason = new Error('stop');
    controller.abort(reason);
    expect(await rejection(pong)).toBe(reason);
    raw.send('02 00 00 02 01');
    await settleRaw(raw);
    // An aborted signal refuses at once.
    expect(await rejection(mux.ping(undefined, { signal: controller.signal }))).toBe(reason);
    expect(mux.isOpen).toBe(true);
    await mux.close();
  });

  test('identify: an aborted signal ends the wait with its reason', async () => {
    const [mux, raw] = rawPair();
    const controller = new AbortController();
    const credential = stubCredential({ identity: 'n', bytes: hexBytes('01') });
    const identifying = mux.identify(credential, { signal: controller.signal });
    await raw.nextControl(MuxControlType.nonce);
    const reason = new Error('stop');
    controller.abort(reason);
    expect(await rejection(identifying)).toBe(reason);
    expect(await rejection(mux.identify(credential, { signal: controller.signal }))).toBe(reason);
    await mux.close();
  });

  test('a dropped identify reports nothing when the connection ends', async () => {
    // Every promise the library hands out is safe to drop (design section
    // 7); one awaited still rejects.
    const [client, server] = muxPair();
    const credential = stubCredential({ identity: 'n', bytes: hexBytes('01') });
    void client.identify(credential);
    const awaited = client.identify(credential);
    await server.close();
    await expectRejectsStatus(awaited, StatusCode.connectionLost);
    await settle();
  });

  test('await using closes the connection and the channel', async () => {
    const [a, b] = muxPair();
    let channel: MuxChannel | undefined;
    {
      await using connection = a;
      await using opened = connection.open(empty);
      channel = opened;
      await firstIncoming(b);
    }
    expect(channel?.state).toBe('closed');
    expect(a.isOpen).toBe(false);
    expectCode(await b.done, StatusCode.connectionLost);
  });
});

describe('the adoptIncoming and admitAfterGoAway hooks', () => {
  test('an adopted channel does not reach incoming; a failing filter is passed over', async () => {
    const logs: LogLevel[] = [];
    const logger: Logger = { log: (level) => logs.push(level) };
    const [a, b] = muxPair({ acceptor: { ...quiet, logger } });
    const adopted: MuxChannel[] = [];
    b.adoptIncoming(() => {
      throw new Error('filter bug');
    });
    const uninstall = b.adoptIncoming((channel) => {
      if (channel.openPayload[0] === 1) {
        adopted.push(channel);
        return true;
      }
      return false;
    });
    const incoming = takeIncoming(b);
    a.open(Uint8Array.of(1));
    a.open(Uint8Array.of(2));
    await a.ping();
    expect(adopted.map((c) => c.openPayload[0])).toEqual([1]);
    expect(incoming.map((c) => c.openPayload[0])).toEqual([2]);
    expect(logs.filter((level) => level === 'error')).toHaveLength(2);
    uninstall();
    a.open(Uint8Array.of(1));
    await a.ping();
    expect(adopted).toHaveLength(1);
    expect(incoming.map((c) => c.openPayload[0])).toEqual([2, 1]);
    // The adopted channel is usable and counted.
    expect(b.openChannelCount).toBe(3);
    await a.close();
  });

  test('after GOAWAY, only what a predicate admits is accepted', async () => {
    const [mux, raw] = rawPair({ options: { ...rawOptions, goAwayGrace: 1000 } });
    const keep = new Incoming(mux);
    raw.send('12 03 00');
    const kept = await keep.next();
    const going = mux.goAway();
    await raw.nextControl(MuxControlType.goAway);
    // No predicate: refused.
    raw.send('12 05 00 01');
    const refused = await raw.next();
    expect(refused.channelId).toBe(5);
    expectCode(Status.decode(refused.payload), StatusCode.goingAway);
    const uninstall = mux.admitAfterGoAway((payload) => payload[0] === 1);
    mux.admitAfterGoAway(() => {
      throw new Error('rule bug');
    });
    raw.send('12 07 00 01');
    raw.send('12 09 00 02');
    expect((await keep.next()).id).toBe(7);
    const second = await raw.next();
    expect(second.channelId).toBe(9);
    expectCode(Status.decode(second.payload), StatusCode.goingAway);
    uninstall();
    raw.send('12 0B 00 01');
    expect((await raw.next()).channelId).toBe(0x0b);
    // The refused ones are confirmed; the open ones finish, then the
    // connection closes.
    raw.send('22 05 00');
    raw.send('22 09 00');
    raw.send('22 0B 00');
    raw.send('22 03 00');
    raw.send('22 07 00');
    await going;
    expectStatus(await kept.done, Status.ok);
    expectCode(await mux.done, StatusCode.goingAway);
  });
});

describe('argument checks', () => {
  test('the options of the constructor', () => {
    for (const options of [
      { initialWindow: 0 },
      { initialWindow: 0x100000000 },
      { initialWindow: 1.5 },
      { bulkZipper: 0 },
      { bulkChunkSize: 0 },
    ]) {
      const [t] = memoryPair(new SteppingClock());
      expect(
        () => new FrameMuxConnection(t, { ...quiet, ...options, isInitiator: true }),
        JSON.stringify(options),
      ).toThrow(RangeError);
    }
  });

  test('a transport whose streams are taken already', () => {
    const [t] = memoryPair(new SteppingClock());
    t.readable.getReader();
    expect(() => new FrameMuxConnection(t, { ...quiet, isInitiator: true })).toThrow(TypeError);
  });

  test('the channel: priority, grant and consumed', async () => {
    const [a] = muxPair();
    const channel = a.open(empty);
    expect(() => {
      (channel as { priority: string }).priority = 'urgent';
    }).toThrow(RangeError);
    expect(channel.priority).toBe('normal');
    expect(() => channel.grant(1.5)).toThrow(RangeError);
    expect(() => channel.grant(-1)).toThrow(RangeError);
    expect(() => channel.consumed(1)).toThrow(Error);
    expect(() => channel.consumed(-1)).toThrow(RangeError);
    expect(`${channel}`).toBe('MuxChannel(2, open, local)');
    expect(`${a}`).toBe('MuxConnection(initiator, 1 channels)');
    await a.close();
    expect(`${a}`).toBe('MuxConnection(initiator, 0 channels, closed)');
  });

  test('incoming has one consumer', async () => {
    const [a] = muxPair();
    a.incoming[Symbol.asyncIterator]();
    expect(() => a.incoming[Symbol.asyncIterator]()).toThrow(Error);
    await a.close();
  });

  test('channelWithId finds the channels not mutually closed', async () => {
    const [mux, raw] = rawPair();
    const channel = mux.open(empty);
    expect(mux.channelWithId(2)).toBe(channel);
    expect(mux.channelWithId(4)).toBeUndefined();
    void channel.close();
    await raw.next();
    expect(await raw.nextHex()).toBe('22 02 00');
    expect(mux.channelWithId(2)).toBe(channel);
    raw.send('22 02 00');
    await channel.done;
    expect(mux.channelWithId(2)).toBeUndefined();
    await mux.close();
  });
});

describe('a burst of frames', () => {
  // Beyond 1024 writes the transport has not taken, frames wait in the
  // connection, so that Node's WritableStream queue stays short.
  test('goes out whole and in order, and closing waits for it', async () => {
    const [mux, raw] = rawPair();
    const count = 20000;
    for (let i = 0; i < count; i++) {
      mux.open(empty);
    }
    void mux.ping(hexBytes('AB'));
    const closing = mux.close();
    const frames = await raw.rest();
    await closing;
    expect(frames).toHaveLength(count + 1);
    for (let i = 0; i < count; i++) {
      const frame = frames[i] as MuxFrame;
      expect(frame.command).toBe(MuxCommand.open);
      // Even ids from 2, short then long.
      expect(frame.channelId).toBe(i < 0x7fff ? 2 + 2 * i : 0x10000 + 2 * (i - 0x7fff));
    }
    expect(hexString((frames[count] as MuxFrame).payload)).toBe('01 AB');
    expect(mux.sentBytes).toBe(frames.reduce((n, f) => n + f.encode().length, 0));
  });

  test('holds DATA in the scheduler while it waits, then sends it behind', async () => {
    const [mux, raw] = rawPair();
    const channels = Array.from({ length: 3000 }, () => mux.open(empty));
    const first = channels[0] as MuxChannel;
    await first.send(hexBytes('01'));
    expect(mux.heldOutputBytes).toBe(4);
    const frames: MuxFrame[] = [];
    while (frames.length < 3001) {
      frames.push(await raw.next());
    }
    expect(frames.slice(0, 3000).every((f) => f.command === MuxCommand.open)).toBe(true);
    expect(hexString((frames[3000] as MuxFrame).encode())).toBe('02 02 00 01');
    expect(mux.heldOutputBytes).toBe(0);
    await mux.close();
  });

  test('a transport that fails drops what waits', async () => {
    const clock = new SteppingClock();
    let fail: ((error: unknown) => void) | undefined;
    const written: number[] = [];
    const writable = new WritableStream<Uint8Array>(
      {
        write: (frame) => {
          written.push(frame.length);
          // The first write never completes until the transport fails.
          if (written.length === 1) {
            return new Promise<void>((_, reject) => {
              fail = reject;
            });
          }
          return undefined;
        },
      },
      { highWaterMark: 4096 },
    );
    const readable = new ReadableStream<Uint8Array>({}, { highWaterMark: 0 });
    const mux = new FrameMuxConnection(
      { readable, writable },
      { ...rawOptions, clock, isInitiator: true },
    );
    for (let i = 0; i < 2000; i++) {
      mux.open(empty);
    }
    await settle();
    // One write waits at the sink: the rest of the first 1024 are queued
    // in the stream, the others in the connection.
    expect(written).toHaveLength(1);
    fail?.(new Error('gone'));
    await settle();
    expect(written).toHaveLength(1);
    const closing = mux.close();
    await clock.elapse(10_000);
    await closing;
    expectCode(await mux.done, StatusCode.connectionLost);
  });
});
