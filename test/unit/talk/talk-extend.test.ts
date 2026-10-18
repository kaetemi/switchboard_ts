/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Port of the Dart `test/talk_test.dart`, groups 'request handles and
// EXTEND', 'declared deadlines' and 'responder timeout override': request
// handles, EXTEND on the requester and the responder, declared deadlines
// and renewals, and the per-request responder timeout.

import { describe, expect, test } from 'vitest';
import { hexString } from '../../../src/core/bytes.ts';
import { Status, StatusCode } from '../../../src/core/status.ts';
import type { TalkMessage, TalkOptions } from '../../../src/core/talk/contract.ts';
import { TalkAbortError } from '../../../src/core/talk/talk-channel.ts';
import { TalkFrame, TalkKind } from '../../../src/core/talk/talk-frame.ts';
import type { Logger, LogLevel } from '../../../src/core/util/logger.ts';
import { ManualClock } from '../../../src/testing/manual-clock.ts';
import { Pair, RawPeer } from '../../support/talk/frame-link.ts';
import {
  bytes,
  Collector,
  expectRemote,
  expectStatus,
  flush,
  Outcome,
  rejectsStatus,
  serve,
} from '../../support/talk/helpers.ts';

const empty = new Uint8Array(0);
const ms30 = 30;
const sec = 1000;
const ms1 = 1;
const minute = 60 * sec;
const hour = 60 * minute;
const day = 24 * hour;

/**
 * The manual clock starts away from 0, as `fake_async` starts at the time
 * of day: an instant of 0 must not read as unset.
 */
const clockStart = 1_000_000;

/** The error [promise] rejects with; fails if it resolves. */
async function failure(promise: Promise<unknown>): Promise<unknown> {
  let error: unknown;
  let rejected = false;
  try {
    await promise;
  } catch (e) {
    error = e;
    rejected = true;
  }
  expect(rejected, 'expected a rejection').toBe(true);
  return error;
}

/** Expects [error] to be a local failure with [code], not the peer's abort. */
function expectLocal(error: unknown, code: StatusCode): void {
  expectStatus(error, code);
  expect(error).not.toBeInstanceOf(TalkAbortError);
}

/** Closes [close]'s channels without waiting, then expects no timer left on [clock]. */
async function closeAndCheck(clock: ManualClock, close: () => Promise<void>): Promise<void> {
  void close();
  await flush();
  expect(clock.pendingCount).toBe(0);
}

describe('request handles and EXTEND', () => {
  test('startRequest returns a handle with the id and the response', async () => {
    const p = new Pair();
    const held: TalkMessage[] = [];
    serve(p.b, (m) => {
      held.push(m);
    });
    const r = p.a.startRequest('A', bytes([1]));
    const c = p.a.startRequest('B', bytes([2]));
    await flush();
    expect(r.requestId).toBe(p.aToB[0]?.requestId);
    expect(c.requestId).toBe(p.aToB.at(-1)?.requestId);
    expect(held).toHaveLength(2);
    void held[0]?.reply(bytes([3]));
    expect([...(await r.response).payload]).toEqual([3]);
    c.cancel();
    expectLocal(await failure(c.response), StatusCode.cancelled);
    await flush();
    expect(held.at(-1)?.isCancelled).toBe(true);
    expect(p.a.outgoingRequestCount).toBe(0);
    await p.close();
  });

  test('onExtend is called for every EXTEND, on every kind of request', async () => {
    const p = new Pair();
    const counts: Record<string, number> = {};
    const counter = (key: string) => (): void => {
      counts[key] = (counts[key] ?? 0) + 1;
    };
    serve(p.b, async (m) => {
      void m.extend();
      void m.extend();
      if (m.procedureName === 'CHAIN') {
        const r = m.startReplyRequest(empty, { onExtend: counter('chained') });
        await r.response;
      } else if (m.procedureName === 'CHAINS') {
        const s = m.replyStreamRequest(empty, { onExtend: counter('chainedStream') });
        await s.done;
      } else if (m.procedureName === 'ITEMS') {
        const r = m.startReplyItemRequest(empty, { onExtend: counter('item') });
        const s = m.replyItemStreamRequest(empty, { onExtend: counter('itemStream') });
        await r.response;
        await s.done;
        void m.reply(empty);
      } else {
        void m.reply(empty);
      }
    });
    const plain = p.a.startRequest('PLAIN', empty, { onExtend: counter('request') });
    await plain.response;
    const stream = p.a.streamRequest('STREAM', empty, { onExtend: counter('stream') });
    await stream.done;
    for (const procedure of ['CHAIN', 'CHAINS']) {
      const r = await p.a.request(procedure, empty);
      void r.extend();
      void r.reply(empty);
    }
    const items = p.a.streamRequest('ITEMS', empty);
    new Collector(items.items, {
      onValue: (item) => {
        void item.extend();
        void item.reply(empty);
      },
    });
    await items.done;
    await flush();
    expect(counts).toEqual({
      request: 2,
      stream: 2,
      chained: 1,
      chainedStream: 1,
      item: 1,
      itemStream: 1,
    });
    await p.close();
  });

  test('an onExtend callback that throws is logged only', async () => {
    const logged: [LogLevel, string][] = [];
    const logger: Logger = {
      log(level, _name, message) {
        logged.push([level, message]);
      },
    };
    const peer = new RawPeer({ options: { logger } });
    const r = peer.talk.startRequest('X', empty, {
      onExtend: () => {
        throw new Error('callback bug');
      },
    });
    await flush();
    peer.send(new TalkFrame({ kind: TalkKind.extend, responseId: r.requestId }));
    peer.send(new TalkFrame({ kind: TalkKind.message, responseId: r.requestId }));
    await r.response;
    expect(peer.talk.isOpen).toBe(true);
    expect(logged.some(([level, message]) => level === 'error' && /onExtend/.test(message))).toBe(
      true,
    );
    await peer.talk.close();
  });

  test('EXTEND restarts the timer of a stream request', async () => {
    const clock = new ManualClock(clockStart);
    const peer = new RawPeer({ options: { requestTimeout: ms30, clock } });
    let extends_ = 0;
    const s = peer.talk.streamRequest('S', empty, {
      onExtend: () => {
        extends_++;
      },
    });
    const done = new Outcome(s.done);
    await flush();
    for (let i = 0; i < 5; i++) {
      await clock.elapseAsync(20);
      peer.send(new TalkFrame({ kind: TalkKind.extend, responseId: s.requestId }));
      await flush();
    }
    await clock.elapseAsync(20);
    expect(done.isDone).toBe(false);
    expect(extends_).toBe(5);
    peer.send(new TalkFrame({ kind: TalkKind.message, responseId: s.requestId }));
    await flush();
    expect(done.value).toBeDefined();
    await closeAndCheck(clock, () => peer.talk.close());
  });

  test('EXTEND after a cancel is ignored', async () => {
    const clock = new ManualClock(clockStart);
    const peer = new RawPeer({ options: { requestTimeout: ms30, clock } });
    let extends_ = 0;
    const s = peer.talk.streamRequest('S', empty, {
      onExtend: () => {
        extends_++;
      },
    });
    await flush();
    s.cancel();
    await clock.elapseAsync(20);
    peer.send(new TalkFrame({ kind: TalkKind.extend, responseId: s.requestId }));
    await flush();
    expect(extends_).toBe(0);
    expect(peer.talk.outgoingRequestCount).toBe(1);
    // The release timer was not restarted by the EXTEND.
    await clock.elapseAsync(11);
    expect(peer.talk.outgoingRequestCount).toBe(0);
    await closeAndCheck(clock, () => peer.talk.close());
  });

  test('a cancelled request without a timeout is released after the channel default', async () => {
    const clock = new ManualClock(clockStart);
    const peer = new RawPeer({ options: { requestTimeout: ms30, clock } });
    const s = peer.talk.streamRequest('S', empty, { timeout: 0 });
    await clock.elapseAsync(hour);
    expect(peer.talk.outgoingRequestCount).toBe(1);
    s.cancel();
    await clock.elapseAsync(31);
    expect(peer.talk.outgoingRequestCount).toBe(0);
    void peer.talk.close();
    await flush();
  });

  test('an item that is itself a stream request', async () => {
    const p = new Pair();
    serve(p.b, async (m) => {
      const sub = m.replyItemStreamRequest(bytes([1]), { procedure: 'SUB' });
      const got: number[] = [];
      for await (const i of sub.items) {
        expect(i.payload).toHaveLength(1);
        got.push(i.payload[0] as number);
      }
      const end = await sub.done;
      expect(end.payload).toHaveLength(1);
      void m.reply(bytes([...got, end.payload[0] as number]));
    });
    const s = p.a.streamRequest('S', empty);
    const seen: unknown[] = [];
    new Collector(s.items, {
      onValue: (item) => {
        seen.push([item.frame.kind, item.expectsReply, item.expectsStream, item.procedureName]);
        void item.replyItem(bytes([7]));
        void item.replyItem(bytes([8]));
        void item.reply(bytes([9]));
      },
    });
    expect([...(await s.done).payload]).toEqual([7, 8, 9]);
    expect(seen).toEqual([[TalkKind.streamItem, true, true, 'SUB']]);
    const item = p.bToA[0] as TalkFrame;
    expect(item.kind).toBe(TalkKind.streamItem);
    expect(item.flags).toBe(0x1f);
    expect(item.responseId).toBe(s.requestId);
    expect(p.a.incomingRequestCount).toBe(0);
    expect(p.b.outgoingRequestCount).toBe(0);
    await p.close();
  });

  test('remote aborts are TalkAbortException, local failures are not', async () => {
    const peer = new RawPeer();
    const remote = peer.talk.request('R', empty);
    const expired = peer.talk.request('T', empty, { timeout: 1 });
    const cancelled = peer.talk.startRequest('C', empty);
    cancelled.cancel();
    await flush();
    peer.send(
      new TalkFrame({
        kind: TalkKind.abort,
        responseId: (peer.received[0] as TalkFrame).requestId,
        payload: Status.of(StatusCode.deadlineExceeded).encode(),
      }),
    );
    expectRemote(await failure(remote), StatusCode.deadlineExceeded);
    expectLocal(await failure(expired), StatusCode.deadlineExceeded);
    expectLocal(await failure(cancelled.response), StatusCode.cancelled);
    const lost = peer.talk.request('L', empty);
    const broken = new RawPeer();
    const protocol = broken.talk.request('P', empty);
    await flush();
    peer.raw.end();
    broken.sendHex('41');
    expectLocal(await failure(lost), StatusCode.connectionLost);
    expectLocal(await failure(protocol), StatusCode.protocolError);
  });
});

describe('declared deadlines', () => {
  function declare(id: number, values: { deadline?: number; renew?: number }): TalkFrame {
    return new TalkFrame({
      kind: TalkKind.extend,
      responseId: id,
      payload: TalkFrame.extendPayload(values),
    });
  }

  function item(id: number): TalkFrame {
    return new TalkFrame({ kind: TalkKind.streamItem, responseId: id });
  }

  /**
   * A stream request from a raw peer's channel (default options unless
   * given) on a manual clock, its outcome, and the time it was sent.
   */
  async function start(options: Partial<TalkOptions> = {}) {
    const clock = new ManualClock(clockStart);
    const peer = new RawPeer({ options: { ...options, clock } });
    const s = peer.talk.streamRequest('S', empty);
    const done = new Outcome(s.done);
    await flush();
    return { clock, peer, s, done, t0: clock.now() };
  }

  async function finish(clock: ManualClock, peer: RawPeer): Promise<void> {
    await closeAndCheck(clock, () => peer.talk.close());
  }

  /** Expects [done] to fail with the requester's own DEADLINE_EXCEEDED exactly [left] from now. */
  async function expectTimeoutIn(
    clock: ManualClock,
    done: Outcome<TalkMessage>,
    left: number,
  ): Promise<void> {
    await clock.elapseAsync(left - ms1);
    expect(done.isDone).toBe(false);
    await clock.elapseAsync(ms1);
    expectLocal(done.error, StatusCode.deadlineExceeded);
  }

  test('without EXTEND the default gap applies, restarted by items', async () => {
    const { clock, peer, s, done, t0 } = await start();
    expect(s.deadline).toBe(t0 + sec * 15);
    await clock.elapseAsync(sec * 10);
    peer.send(item(s.requestId));
    await flush();
    expect(s.deadline).toBe(t0 + sec * 25);
    await expectTimeoutIn(clock, done, sec * 15);
    expect(s.deadline).toBeUndefined();
    await finish(clock, peer);
  });

  test('a deadline replaces the default gap; items do not move it', async () => {
    const { clock, peer, s, done, t0 } = await start();
    peer.send(declare(s.requestId, { deadline: sec * 60 }));
    await flush();
    expect(s.deadline).toBe(t0 + sec * 60);
    await clock.elapseAsync(sec * 50);
    peer.send(item(s.requestId));
    await flush();
    expect(s.deadline, 'no renewal declared').toBe(t0 + sec * 60);
    await expectTimeoutIn(clock, done, sec * 10);
    // The requester cancels what it gave up on.
    expect(peer.received.at(-1)?.kind).toBe(TalkKind.abort);
    expect(peer.received.at(-1)?.requestId).toBe(s.requestId);
    await finish(clock, peer);
  });

  test('a renewal alone counts from the last reply', async () => {
    const { clock, peer, s, done, t0 } = await start();
    peer.send(declare(s.requestId, { renew: sec * 30 }));
    await flush();
    expect(s.deadline).toBe(t0 + sec * 30);
    await clock.elapseAsync(sec * 20);
    peer.send(item(s.requestId));
    await flush();
    expect(s.deadline).toBe(t0 + sec * 50);
    await expectTimeoutIn(clock, done, sec * 30);
    await finish(clock, peer);
  });

  test('with both, the later one applies: a reply never brings the deadline closer', async () => {
    const { clock, peer, s, done, t0 } = await start();
    peer.send(declare(s.requestId, { deadline: sec * 60, renew: sec * 20 }));
    await flush();
    expect(s.deadline).toBe(t0 + sec * 60);
    await clock.elapseAsync(sec * 10);
    peer.send(item(s.requestId));
    await flush();
    expect(s.deadline, 'not t0 + 30 s').toBe(t0 + sec * 60);
    await clock.elapseAsync(sec * 45);
    peer.send(item(s.requestId));
    await flush();
    expect(s.deadline).toBe(t0 + sec * 75);
    await expectTimeoutIn(clock, done, sec * 20);
    await finish(clock, peer);
  });

  test('a later EXTEND replaces what it carries, and may shorten it; a 0 field is unchanged', async () => {
    const { clock, peer, s, done, t0 } = await start();
    peer.send(declare(s.requestId, { deadline: 10 * minute, renew: minute }));
    await flush();
    expect(s.deadline).toBe(t0 + 10 * minute);
    await clock.elapseAsync(sec * 10);
    // Deadline shortened to t0 + 30 s; the renewal of 1 min stands,
    // counted from this EXTEND.
    peer.send(declare(s.requestId, { deadline: sec * 20 }));
    await flush();
    expect(s.deadline).toBe(t0 + sec * 70);
    await clock.elapseAsync(sec * 10);
    // Renewal shortened; the deadline (t0 + 30 s) stands.
    peer.send(declare(s.requestId, { renew: sec * 5 }));
    await flush();
    expect(s.deadline).toBe(t0 + sec * 30);
    await expectTimeoutIn(clock, done, sec * 10);
    await finish(clock, peer);
  });

  test('values below minExtension are raised to it', async () => {
    {
      const { clock, peer, s, done, t0 } = await start();
      peer.send(declare(s.requestId, { deadline: 100, renew: ms1 }));
      await flush();
      expect(s.deadline).toBe(t0 + sec);
      await expectTimeoutIn(clock, done, sec);
      await finish(clock, peer);
    }
    {
      const { clock, peer, s, done } = await start({ minExtension: 0 });
      peer.send(declare(s.requestId, { deadline: 100 }));
      await flush();
      await expectTimeoutIn(clock, done, 100);
      await finish(clock, peer);
    }
  });

  test('values above maxExtension are lowered to it', async () => {
    {
      const { clock, peer, s, done, t0 } = await start();
      peer.send(declare(s.requestId, { deadline: 2 * hour }));
      await flush();
      expect(s.deadline).toBe(t0 + hour);
      await expectTimeoutIn(clock, done, hour);
      await finish(clock, peer);
    }
    {
      const { clock, peer, s, t0 } = await start({ maxExtension: 10 * minute });
      peer.send(declare(s.requestId, { renew: hour }));
      await flush();
      expect(s.deadline).toBe(t0 + 10 * minute);
      await finish(clock, peer);
    }
    {
      const { clock, peer, s, t0 } = await start({ maxExtension: 0 });
      peer.send(declare(s.requestId, { deadline: 3 * day }));
      await flush();
      expect(s.deadline).toBe(t0 + 3 * day);
      await finish(clock, peer);
    }
  });

  test('an empty EXTEND restarts the default gap, keeping what was declared; zero fields count as a reply', async () => {
    const { clock, peer, s, done, t0 } = await start();
    // Default regime: an empty EXTEND and two zero fields restart the
    // gap alike.
    await clock.elapseAsync(sec * 10);
    peer.send(new TalkFrame({ kind: TalkKind.extend, responseId: s.requestId }));
    await flush();
    expect(s.deadline).toBe(t0 + sec * 25);
    await clock.elapseAsync(sec * 10);
    peer.send(
      new TalkFrame({ kind: TalkKind.extend, responseId: s.requestId, payload: new Uint8Array(8) }),
    );
    await flush();
    expect(s.deadline).toBe(t0 + sec * 35);
    // Declared: deadline t0 + 80 s.
    peer.send(declare(s.requestId, { deadline: sec * 60 }));
    await flush();
    await clock.elapseAsync(sec * 55);
    // t0 + 75 s: the gap ends after the deadline.
    peer.send(new TalkFrame({ kind: TalkKind.extend, responseId: s.requestId }));
    await flush();
    expect(s.deadline).toBe(t0 + sec * 90);
    await clock.elapseAsync(sec * 10);
    // t0 + 85 s: a declaration replaces the gap; the deadline stands.
    peer.send(declare(s.requestId, { renew: sec * 2 }));
    await flush();
    expect(s.deadline).toBe(t0 + sec * 87);
    await expectTimeoutIn(clock, done, sec * 2);
    await finish(clock, peer);
  });

  test('an EXTEND payload that is neither empty nor 8 bytes is a channel protocol error', async () => {
    const peer = new RawPeer({ status: true });
    const pending = peer.talk.request('X', empty);
    await flush();
    peer.sendHex('34 01 00 00 A0 BB 0D 00');
    await rejectsStatus(pending, StatusCode.protocolError);
    expect((await peer.talk.done).known).toBe(StatusCode.protocolError);
    expect(peer.status?.closedWith.map((s) => s.known)).toEqual([StatusCode.protocolError]);
  });

  test('onExtend receives the values as received', async () => {
    const clock = new ManualClock(clockStart);
    const peer = new RawPeer({ options: { clock } });
    const seen: { deadline?: number; renew?: number }[] = [];
    const r = peer.talk.startRequest('X', empty, {
      onExtend: (extension) => {
        seen.push(extension);
      },
    });
    await flush();
    const id = r.requestId;
    peer.send(declare(id, { deadline: 100 }));
    peer.send(declare(id, { renew: 2 * hour }));
    peer.send(declare(id, { deadline: sec * 90, renew: sec * 30 }));
    peer.send(new TalkFrame({ kind: TalkKind.extend, responseId: id }));
    await flush();
    expect(seen).toStrictEqual([
      { deadline: 100 },
      { renew: 2 * hour },
      { deadline: sec * 90, renew: sec * 30 },
      {},
    ]);
    await finish(clock, peer);
  });

  test('the deadline of a request without a timeout is null', async () => {
    const clock = new ManualClock(clockStart);
    const peer = new RawPeer({ options: { clock } });
    const r = peer.talk.startRequest('X', empty, { timeout: 0 });
    const result = new Outcome(r.response);
    await flush();
    expect(r.deadline).toBeUndefined();
    peer.send(declare(r.requestId, { deadline: sec * 10 }));
    await clock.elapseAsync(2 * hour);
    expect(r.deadline).toBeUndefined();
    expect(result.isDone).toBe(false);
    peer.send(new TalkFrame({ kind: TalkKind.message, responseId: r.requestId }));
    await flush();
    expect(result.value).toBeDefined();
    await finish(clock, peer);
  });

  test('extend puts the buffer on the wire, not on the responder timeout, so the responder gives up first', async () => {
    const clock = new ManualClock(clockStart);
    const p = new Pair({ a: { clock }, b: { clock } });
    const held: TalkMessage[] = [];
    serve(p.b, (m) => {
      held.push(m);
    });
    const r = p.a.startRequest('SLOW', empty);
    const result = new Outcome(r.response);
    await flush();
    const t0 = clock.now();
    expect(held).toHaveLength(1);
    const m = held[0] as TalkMessage;
    void m.extend({ deadline: sec * 60 });
    await flush();
    expect(p.bToA).toHaveLength(1);
    const wire = p.bToA[0] as TalkFrame;
    expect(wire.extension).toEqual({ deadline: sec * 65, renew: undefined });
    expect(hexString(p.bToARaw[0] as Uint8Array)).toBe('34 01 00 00 E8 FD 00 00 00 00 00 00');
    expect(r.deadline).toBe(t0 + sec * 65);
    await clock.elapseAsync(sec * 60 - ms1);
    expect(result.isDone).toBe(false);
    await clock.elapseAsync(ms1);
    expectRemote(result.error, StatusCode.deadlineExceeded);
    expect(m.isCancelled).toBe(true);
    await closeAndCheck(clock, () => p.close());
  });

  test('the buffer comes from extendBuffer or the call, on each value', async () => {
    const clock = new ManualClock(clockStart);
    const p = new Pair({ a: { clock }, b: { extendBuffer: 10 * sec, clock } });
    const held: TalkMessage[] = [];
    serve(p.b, (m) => {
      held.push(m);
    });
    void p.a.request('A', empty);
    await flush();
    expect(held).toHaveLength(1);
    const m = held[0] as TalkMessage;
    void m.extend({ deadline: sec * 60, renew: sec * 20 });
    void m.extend({ renew: sec * 20, buffer: 0 });
    void m.extend({ deadline: 0, buffer: 0 });
    void m.extend();
    await flush();
    expect(p.bToA.map((f) => f.extension)).toEqual([
      { deadline: sec * 70, renew: sec * 30 },
      { deadline: undefined, renew: sec * 20 },
      { deadline: ms1, renew: undefined },
      { deadline: undefined, renew: undefined },
    ]);
    expect(p.bToA.at(-1)?.payload).toHaveLength(0);
    void p.close();
    await flush();
  });

  test('a value beyond the EXTEND field is lowered on both sides, the buffer kept', async () => {
    const clock = new ManualClock(clockStart);
    const max = TalkFrame.maxExtendMillis;
    const p = new Pair({
      a: { requestTimeout: 100 * day, maxExtension: 0, clock },
      b: { replyTimeout: 100 * day, extendBuffer: 20 * day, clock },
    });
    const held = new Map<string, TalkMessage>();
    serve(p.b, (m) => {
      held.set(m.procedureName, m);
    });
    const deadline = new Outcome(p.a.request('DEADLINE', empty));
    const renew = new Outcome(p.a.request('RENEW', empty));
    await flush();
    void held.get('DEADLINE')?.extend({ deadline: 60 * day });
    void held.get('RENEW')?.extend({ renew: 60 * day });
    await flush();
    expect(p.bToA.map((f) => f.extension)).toEqual([
      { deadline: max, renew: undefined },
      { deadline: undefined, renew: max },
    ]);
    // The responder still gives up the buffer before the requester.
    await clock.elapseAsync(max - 20 * day - ms1);
    expect([...held.values()].map((m) => m.canReply)).toEqual([true, true]);
    await clock.elapseAsync(ms1);
    expect([...held.values()].map((m) => m.canReply)).toEqual([false, false]);
    await flush();
    for (const outcome of [deadline, renew]) {
      expectRemote(outcome.error, StatusCode.deadlineExceeded);
    }
    await closeAndCheck(clock, () => p.close());
  });

  test('the responder timeout follows its own declaration: items renew, the default gap is gone', async () => {
    const clock = new ManualClock(clockStart);
    const p = new Pair({ a: { clock }, b: { clock } });
    const held: TalkMessage[] = [];
    serve(p.b, (m) => {
      held.push(m);
    });
    const s = p.a.streamRequest('S', empty);
    const done = new Outcome(s.done);
    new Collector(s.items);
    await flush();
    expect(held).toHaveLength(1);
    const m = held[0] as TalkMessage;
    void m.extend({ deadline: sec * 30, renew: sec * 20 });
    await clock.elapseAsync(sec * 25);
    void m.replyItem(bytes([1]));
    // Responder: max(30, 25 + 20) = 45 s; the requester waits until
    // max(35, 25 + 25) = 50 s.
    await clock.elapseAsync(sec * 20 - ms1);
    expect(done.isDone).toBe(false);
    expect(m.canReply).toBe(true);
    await clock.elapseAsync(ms1);
    expect(m.canReply).toBe(false);
    await flush();
    expect(done.error).toBeInstanceOf(TalkAbortError);
    await closeAndCheck(clock, () => p.close());
  });

  test('on the responder, an empty extend restarts the gap and setReplyTimeout goes back to the default regime', async () => {
    const clock = new ManualClock(clockStart);
    const p = new Pair({ a: { requestTimeout: 0, clock }, b: { clock } });
    const held = new Map<string, TalkMessage>();
    serve(p.b, (m) => {
      held.set(m.procedureName, m);
    });
    void p.a.request('GAP', empty);
    void p.a.request('RESET', empty);
    void p.a.request('NONE', empty);
    await flush();
    const gap = held.get('GAP') as TalkMessage;
    const reset = held.get('RESET') as TalkMessage;
    const none = held.get('NONE') as TalkMessage;
    none.setReplyTimeout(0);
    for (const m of [gap, reset, none]) {
      void m.extend({ deadline: sec * 30 });
    }
    await clock.elapseAsync(sec * 25);
    void gap.extend();
    reset.setReplyTimeout(sec * 2);
    // GAP: max(30, 25 + 10) = 35 s; RESET: 27 s; NONE: never.
    await clock.elapseAsync(sec * 2 - ms1);
    expect(reset.canReply).toBe(true);
    await clock.elapseAsync(ms1);
    expect(reset.canReply).toBe(false);
    await clock.elapseAsync(sec * 8 - ms1);
    expect(gap.canReply).toBe(true);
    await clock.elapseAsync(ms1);
    expect(gap.canReply).toBe(false);
    await clock.elapseAsync(hour);
    expect(none.canReply).toBe(true);
    void none.reply(empty);
    await closeAndCheck(clock, () => p.close());
  });

  test('extend refuses negative values and sends nothing', async () => {
    const p = new Pair();
    const held: TalkMessage[] = [];
    serve(p.b, (m) => {
      held.push(m);
    });
    void p.a.request('A', empty);
    await flush();
    expect(held).toHaveLength(1);
    const m = held[0] as TalkMessage;
    expect(() => m.extend({ deadline: -sec })).toThrow(RangeError);
    expect(() => m.extend({ renew: -sec })).toThrow(RangeError);
    expect(() => m.extend({ buffer: -sec })).toThrow(RangeError);
    await flush();
    expect(p.bToA).toHaveLength(0);
    await p.close();
  });
});

describe('responder timeout override', () => {
  test('setReplyTimeout applies at once and to later restarts', async () => {
    const clock = new ManualClock(clockStart);
    const p = new Pair({
      a: { requestTimeout: 0, clock },
      b: { replyTimeout: ms30, clock },
    });
    const held = new Map<string, TalkMessage>();
    serve(p.b, (m) => {
      held.set(m.procedureName, m);
      switch (m.procedureName) {
        case 'LONGER':
          m.setReplyTimeout(100);
          break;
        case 'NONE':
          m.setReplyTimeout(0);
          break;
        case 'BACK':
          m.setReplyTimeout(0);
          break;
      }
    });
    const longer = new Outcome(p.a.request('LONGER', empty));
    const none = new Outcome(p.a.request('NONE', empty));
    const back = new Outcome(p.a.request('BACK', empty));
    const normal = new Outcome(p.a.request('NORMAL', empty));
    await clock.elapseAsync(20);
    held.get('BACK')?.setReplyTimeout(undefined);
    await clock.elapseAsync(11);
    expectStatus(normal.error, StatusCode.deadlineExceeded);
    expect(longer.isDone).toBe(false);
    expect(back.isDone, 'restarted at 20 ms').toBe(false);
    await clock.elapseAsync(20);
    expectStatus(back.error, StatusCode.deadlineExceeded);
    void held.get('LONGER')?.extend();
    await clock.elapseAsync(99);
    expect(longer.isDone, 'extend restarts 100 ms').toBe(false);
    await clock.elapseAsync(2);
    expectStatus(longer.error, StatusCode.deadlineExceeded);
    await clock.elapseAsync(hour);
    expect(none.isDone).toBe(false);
    void held.get('NONE')?.reply(bytes([1]));
    await flush();
    expect([...(none.value as TalkMessage).payload]).toEqual([1]);
    held.get('NONE')?.setReplyTimeout(ms30);
    await closeAndCheck(clock, () => p.close());
  });
});
