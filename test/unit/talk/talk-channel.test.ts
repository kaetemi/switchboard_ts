/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Port of the Dart `test/talk_test.dart`: Talk over a channel of whole
// frames (no flow control), the requester and responder sides, timeouts,
// EXTEND, aborts, cancels, limits, protocol errors and ordered answers.

import { describe, expect, test } from 'vitest';
import { bytesEqual, hexString } from '../../../src/core/bytes.ts';
import { Name } from '../../../src/core/name.ts';
import { Status, StatusCode, SwitchboardError } from '../../../src/core/status.ts';
import type { TalkMessage } from '../../../src/core/talk/contract.ts';
import { TalkKind } from '../../../src/core/talk/talk-frame.ts';
import { Pair } from '../../support/talk/frame-link.ts';
import { bytes, flush, Queue, serve, throwsStatus } from '../../support/talk/helpers.ts';

const empty = new Uint8Array(0);

describe('messages and requests', () => {
  test('plain messages both ways', async () => {
    const p = new Pair();
    const atA = new Queue(p.a.messages);
    const atB = new Queue(p.b.messages);
    void p.a.send('HELLO', bytes([1, 2]));
    void p.b.send('WORLD', bytes([3]));
    const onB = await atB.next();
    expect(onB.procedureName).toBe('HELLO');
    expect(onB.procedure.equals(Name.of('HELLO'))).toBe(true);
    expect([...onB.payload]).toEqual([1, 2]);
    expect(onB.frame.kind).toBe(TalkKind.message);
    expect(onB.expectsReply).toBe(false);
    expect(onB.requestId).toBe(0);
    expect(onB.responseId).toBe(0);
    expect(onB.canReply).toBe(false);
    const onA = await atA.next();
    expect(onA.procedureName).toBe('WORLD');
    expect([...onA.payload]).toEqual([3]);
    expect(hexString(p.aToBRaw[0] as Uint8Array)).toBe('01 48 45 4C 4C 4F 00 00 00 01 02');
    expect(p.aToBRaw).toHaveLength(1);
    await p.close();
    expect(await atA.hasNext()).toBe(false);
    expect(await atB.hasNext()).toBe(false);
  });

  test('request and reply both ways', async () => {
    const p = new Pair();
    serve(p.b, (m) => m.reply(Uint8Array.from(m.payload).reverse()));
    serve(p.a, (m) => m.reply(bytes([m.payload.length])));
    const r1 = await p.a.request('ECHO', bytes([1, 2, 3]));
    expect([...r1.payload]).toEqual([3, 2, 1]);
    expect(r1.expectsReply).toBe(false);
    expect(r1.responseId).toBe(p.aToB[0]?.requestId);
    expect(p.aToB).toHaveLength(1);
    const r2 = await p.b.request('LEN', bytes([9, 9]));
    expect([...r2.payload]).toEqual([2]);
    expect(p.a.outgoingRequestCount).toBe(0);
    expect(p.b.incomingRequestCount).toBe(0);
    await p.close();
  });

  test('text and JSON payloads', async () => {
    const p = new Pair();
    serve(p.b, (m) => m.reply(JSON.stringify({ echo: m.text() })));
    const r = await p.a.request('ECHO', 'héllo');
    expect(r.json()).toEqual({ echo: 'héllo' });
    await p.close();
  });

  test('reply with and without procedure', async () => {
    const p = new Pair();
    serve(p.b, (m) => {
      if (m.procedureName === 'NAMED') {
        void m.reply(bytes([1]), { procedure: 'RESULT' });
      } else {
        void m.reply(bytes([2]));
      }
    });
    const named = await p.a.request('NAMED', empty);
    expect(named.procedureName).toBe('RESULT');
    expect(p.bToA.at(-1)?.procedure?.equals(Name.of('RESULT'))).toBe(true);
    const plain = await p.a.request('PLAIN', empty);
    expect(plain.procedure.equals(Name.empty)).toBe(true);
    expect(plain.procedureName).toBe('');
    expect(p.bToA.at(-1)?.procedure).toBeUndefined();
    expect(hexString(p.bToARaw.at(-1) as Uint8Array)).toBe('04 02 00 00 02');
    await p.close();
  });

  test('chained replies, several hops deep', async () => {
    const p = new Pair();
    const checks: unknown[] = [];
    serve(p.b, async (m1) => {
      checks.push(m1.procedureName);
      const r2 = await m1.replyRequest(bytes([2]), { procedure: 'Q2' });
      checks.push(r2.procedureName, r2.expectsReply);
      const r4 = await r2.replyRequest(bytes([4]));
      checks.push([...r4.payload], r4.expectsReply);
      void r4.reply(bytes([6]));
    });
    const r1 = await p.a.request('Q1', bytes([1]));
    expect([...r1.payload]).toEqual([2]);
    expect(r1.procedureName).toBe('Q2');
    expect(r1.expectsReply).toBe(true);
    const r3 = await r1.replyRequest(bytes([3]), { procedure: 'Q3' });
    expect([...r3.payload]).toEqual([4]);
    expect(r3.procedure.equals(Name.empty)).toBe(true);
    const r5 = await r3.replyRequest(bytes([5]));
    expect([...r5.payload]).toEqual([6]);
    expect(r5.expectsReply).toBe(false);
    expect(checks).toEqual(['Q1', 'Q3', true, [5], true]);
    // Every hop is a response to the previous request.
    const aToB = p.aToB;
    const bToA = p.bToA;
    expect(bToA[0]?.responseId).toBe(aToB[0]?.requestId);
    expect(aToB[1]?.responseId).toBe(bToA[0]?.requestId);
    expect(bToA[1]?.responseId).toBe(aToB[1]?.requestId);
    expect(aToB[2]?.responseId).toBe(bToA[1]?.requestId);
    expect(bToA[2]?.responseId).toBe(aToB[2]?.requestId);
    expect(bToA[2]?.hasRequest).toBe(false);
    for (const c of [p.a, p.b]) {
      expect(c.outgoingRequestCount).toBe(0);
      expect(c.incomingRequestCount).toBe(0);
    }
    await p.close();
  });

  test('reply on a message that expects no reply throws', async () => {
    const p = new Pair();
    const atB = new Queue(p.b.messages);
    void p.a.send('NOTE', empty);
    const m = await atB.next();
    const failed = StatusCode.failedPrecondition;
    throwsStatus(() => m.reply(empty), failed);
    throwsStatus(() => m.replyAbort(Status.of(StatusCode.internal)), failed);
    throwsStatus(() => m.extend(), failed);
    throwsStatus(() => m.replyRequest(empty), failed);
    throwsStatus(() => m.replyItem(empty), failed);
    expect(p.bToARaw).toHaveLength(0);
    await p.close();
  });

  test('double final reply throws failedPrecondition', async () => {
    const p = new Pair();
    const errors: unknown[] = [];
    serve(p.b, (m) => {
      void m.reply(bytes([1]));
      for (const again of [
        () => m.reply(bytes([2])),
        () => m.replyAbort(Status.of(StatusCode.internal)),
        () => m.extend(),
        () => m.replyRequest(empty),
      ]) {
        try {
          again();
        } catch (e) {
          errors.push(e);
        }
      }
    });
    const r = await p.a.request('ONCE', empty);
    expect([...r.payload]).toEqual([1]);
    expect(errors).toHaveLength(4);
    for (const error of errors) {
      expect((error as SwitchboardError).status.code).toBe(StatusCode.failedPrecondition);
    }
    expect(p.bToA).toHaveLength(1);
    await p.close();
  });

  test('replyItem on a non-stream request throws', async () => {
    const p = new Pair();
    const atB = new Queue(p.b.messages);
    const f = p.a.request('ONE', empty);
    const m = await atB.next();
    expect(m.expectsStream).toBe(false);
    throwsStatus(() => m.replyItem(empty), StatusCode.failedPrecondition);
    throwsStatus(() => m.replyItemRequest(empty), StatusCode.failedPrecondition);
    expect(m.canReply).toBe(true);
    void m.reply(bytes([7]));
    expect([...(await f).payload]).toEqual([7]);
    await p.close();
  });

  test('interleaving: 200 requests in flight, replied in reverse', async () => {
    const p = new Pair();
    const held: TalkMessage[] = [];
    let all!: () => void;
    const allArrived = new Promise<void>((resolve) => {
      all = resolve;
    });
    serve(p.b, (m) => {
      held.push(m);
      if (held.length === 200) {
        all();
      }
    });
    const promises: Promise<TalkMessage>[] = [];
    for (let i = 0; i < 200; i++) {
      promises.push(p.a.request('N', bytes([i & 0xff, i >> 8])));
    }
    await allArrived;
    expect(p.a.outgoingRequestCount).toBe(200);
    expect(p.b.incomingRequestCount).toBe(200);
    for (const m of [...held].reverse()) {
      void m.reply(bytes([...m.payload, 0xee]));
    }
    const results = await Promise.all(promises);
    for (let i = 0; i < 200; i++) {
      expect([...(results[i] as TalkMessage).payload]).toEqual([i & 0xff, i >> 8, 0xee]);
    }
    expect(p.a.outgoingRequestCount).toBe(0);
    expect(p.b.incomingRequestCount).toBe(0);
    await p.close();
  });

  test('large payloads pass through unchanged', async () => {
    const p = new Pair();
    const big = new Uint8Array(1 << 20);
    let seed = 42;
    for (let i = 0; i < big.length; i++) {
      seed = (seed * 1103515245 + 12345) >>> 0;
      big[i] = seed >>> 24;
    }
    serve(p.b, (m) => m.reply(m.payload));
    const r = await p.a.request('BIG', big);
    expect(r.payload.length).toBe(big.length);
    expect(bytesEqual(r.payload, big)).toBe(true);
    await p.close();
  });
});

describe('streams', () => {
  test('items then a final response with trailing payload', async () => {
    const p = new Pair();
    const seen: boolean[] = [];
    serve(p.b, (m) => {
      seen.push(m.expectsStream);
      void m.replyItem(bytes([1]), { procedure: 'ITEM' });
      void m.replyItem(bytes([2]));
      void m.replyItem(bytes([3]));
      void m.reply(bytes([0xff]), { procedure: 'END' });
    });
    const s = p.a.streamRequest('LIST', bytes([0]));
    const items: TalkMessage[] = [];
    for await (const item of s.items) {
      items.push(item);
    }
    expect(seen).toEqual([true]);
    expect(items.map((m) => m.payload[0])).toEqual([1, 2, 3]);
    expect(items[0]?.frame.kind).toBe(TalkKind.streamItem);
    expect(items[0]?.procedureName).toBe('ITEM');
    expect(items.every((m) => m.responseId === s.requestId)).toBe(true);
    const last = await s.done;
    expect(last.frame.kind).toBe(TalkKind.message);
    expect([...last.payload]).toEqual([0xff]);
    expect(last.procedureName).toBe('END');
    expect(p.aToB).toHaveLength(1);
    expect(p.aToB[0]?.stream).toBe(true);
    expect(p.a.outgoingRequestCount).toBe(0);
    await p.close();
  });

  test('iterating the stream handle iterates its items', async () => {
    const p = new Pair();
    serve(p.b, (m) => m.replyStream([bytes([1]), bytes([2])], { trailer: bytes([3]) }));
    const s = p.a.streamRequest('LIST', empty);
    const got: number[] = [];
    for await (const item of s) {
      got.push(item.payload[0] as number);
    }
    expect(got).toEqual([1, 2]);
    expect([...(await s.done).payload]).toEqual([3]);
    await p.close();
  });

  test('items that are requests are answered by the requester', async () => {
    const p = new Pair();
    serve(p.b, async (m) => {
      const answers = await Promise.all([
        m.replyItemRequest(bytes([1]), { procedure: 'MORE' }),
        m.replyItemRequest(bytes([2])),
      ]);
      void m.reply(bytes([(answers[0].payload[0] as number) + (answers[1].payload[0] as number)]));
    });
    const s = p.a.streamRequest('ASK', empty);
    const kinds: [boolean, TalkKind][] = [];
    void (async () => {
      for await (const item of s.items) {
        kinds.push([item.expectsReply, item.frame.kind]);
        void item.reply(bytes([(item.payload[0] as number) * 10]));
      }
    })();
    const last = await s.done;
    expect([...last.payload]).toEqual([30]);
    expect(kinds).toEqual([
      [true, TalkKind.streamItem],
      [true, TalkKind.streamItem],
    ]);
    const items = p.bToA.filter((f) => f.kind === TalkKind.streamItem);
    expect(items.every((f) => f.hasRequest && f.hasResponse)).toBe(true);
    expect(p.a.incomingRequestCount).toBe(0);
    expect(p.b.outgoingRequestCount).toBe(0);
    await p.close();
  });

  test('a final response that is itself a stream request', async () => {
    const p = new Pair();
    const checks: unknown[] = [];
    serve(p.b, async (m) => {
      const back = m.replyStreamRequest(bytes([1]), { procedure: 'BACK' });
      const items: number[] = [];
      for await (const item of back.items) {
        items.push(item.payload[0] as number);
      }
      const end = await back.done;
      checks.push(items, [...end.payload]);
    });
    const r = await p.a.request('START', empty);
    expect(r.expectsReply).toBe(true);
    expect(r.expectsStream).toBe(true);
    void r.replyItem(bytes([7]));
    void r.replyItem(bytes([8]));
    void r.reply(bytes([9]));
    await flush();
    expect(checks).toEqual([[7, 8], [9]]);
    expect(p.b.outgoingRequestCount).toBe(0);
    await p.close();
  });

  test('abort response fails items and done', async () => {
    const p = new Pair();
    serve(p.b, (m) => {
      void m.replyItem(bytes([1]));
      void m.replyAbort(new Status(300, 'gone'));
    });
    const s = p.a.streamRequest('LIST', empty);
    const got: number[] = [];
    let itemsError: unknown;
    const ended = (async () => {
      try {
        for await (const m of s.items) {
          got.push(m.payload[0] as number);
        }
      } catch (e) {
        itemsError = e;
      }
    })();
    const error = await s.done.then(
      () => undefined,
      (e: unknown) => e,
    );
    expect(error).toBeInstanceOf(SwitchboardError);
    expect((error as SwitchboardError).status.equals(new Status(300, 'gone'))).toBe(true);
    await ended;
    expect(got).toEqual([1]);
    expect(itemsError).toBeInstanceOf(SwitchboardError);
    await p.close();
  });

  test('messages and items are iterated once', async () => {
    const p = new Pair();
    serve(p.b, (m) => {
      void m.replyItem(bytes([1]));
      void m.reply(empty);
    });
    const s = p.a.streamRequest('LIST', empty);
    for await (const _ of s) {
      break;
    }
    // Stopping the iteration dropped what was left: a second consumer is
    // a mistake, not an empty stream.
    expect(() => s.items[Symbol.asyncIterator]()).toThrow(Error);
    await s.done.catch(() => {});
    const messages = new Queue(p.a.messages);
    expect(() => p.a.messages[Symbol.asyncIterator]()).toThrow(Error);
    expect(() => p.a.serve(() => {})).toThrow(Error);
    await messages.cancel();
    await p.close();
  });
});
