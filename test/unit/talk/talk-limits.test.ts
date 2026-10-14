/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Port of the Dart `test/talk_test.dart`, group 'limits and ids': the
// incoming and outgoing request limits, request id allocation and
// wrapping, responses to unknown ids, and request ids reused by the peer.

import { describe, expect, test } from 'vitest';
import { hexString } from '../../../src/core/bytes.ts';
import { Name } from '../../../src/core/name.ts';
import { StatusCode } from '../../../src/core/status.ts';
import type { TalkMessage } from '../../../src/core/talk/contract.ts';
import { TalkFrame, TalkKind } from '../../../src/core/talk/talk-frame.ts';
import { Pair, RawPeer } from '../../support/talk/frame-link.ts';
import {
  bytes,
  expectStatus,
  flush,
  rejectsStatus,
  serve,
  throwsStatus,
} from '../../support/talk/helpers.ts';

const empty = new Uint8Array(0);

describe('limits and ids', () => {
  test('maxIncomingRequests exceeded: ABORT RESOURCE_EXHAUSTED', async () => {
    const p = new Pair({ b: { maxIncomingRequests: 2 } });
    const held: TalkMessage[] = [];
    serve(p.b, (m) => {
      held.push(m);
    });
    const f1 = p.a.request('A', empty);
    const f2 = p.a.request('B', empty);
    const f3 = p.a.request('C', empty);
    await rejectsStatus(f3, StatusCode.resourceExhausted);
    expect(held.map((m) => m.procedureName)).toEqual(['A', 'B']);
    void held[0]?.reply(empty);
    await f1;
    const f4 = p.a.request('D', empty);
    await flush();
    expect(held.map((m) => m.procedureName)).toEqual(['A', 'B', 'D']);
    void held[1]?.reply(empty);
    void held[2]?.reply(empty);
    await Promise.all([f2, f4]);
    await p.close();
  });

  test('maxOutgoingRequests exceeded: throws locally, sends nothing', async () => {
    const p = new Pair({ a: { maxOutgoingRequests: 2 } });
    const held: TalkMessage[] = [];
    serve(p.b, (m) => {
      held.push(m);
    });
    const f1 = p.a.request('A', empty);
    const s2 = p.a.streamRequest('B', empty);
    const exhausted = StatusCode.resourceExhausted;
    throwsStatus(() => p.a.request('C', empty), exhausted);
    throwsStatus(() => p.a.streamRequest('C', empty), exhausted);
    await flush();
    expect(p.aToB).toHaveLength(2);
    // A chained reply is an outgoing request too.
    const thrown: unknown[] = [];
    const canReply: boolean[] = [];
    serve(p.a, (m) => {
      try {
        void m.replyRequest(empty);
      } catch (e) {
        thrown.push(e);
      }
      canReply.push(m.canReply);
      void m.reply(empty);
    });
    await p.b.request('Z', empty);
    expect(thrown).toHaveLength(1);
    expectStatus(thrown[0], exhausted);
    expect(canReply, 'nothing was sent').toEqual([true]);
    void held[0]?.reply(empty);
    void held[1]?.reply(empty);
    await f1;
    await s2.done;
    const f4 = p.a.request('D', empty);
    await flush();
    expect(held.at(-1)?.procedureName).toBe('D');
    void held.at(-1)?.reply(empty);
    await f4;
    await p.close();
  });

  test('request ids wrap and skip ids still outstanding', async () => {
    const peer = new RawPeer();
    peer.talk.nextRequestId = 0xfffffe;
    const a = peer.talk.request('A', empty);
    void peer.talk.request('B', empty);
    void peer.talk.request('C', empty);
    peer.talk.nextRequestId = 0xfffffe;
    void peer.talk.request('D', empty);
    expect(peer.talk.nextRequestId).toBe(3);
    await flush();
    expect(peer.received.map((f) => f.requestId)).toEqual([0xfffffe, 0xffffff, 1, 2]);
    peer.send(new TalkFrame({ kind: TalkKind.message, responseId: 0xfffffe }));
    await a;
    peer.talk.nextRequestId = 0xfffffe;
    void peer.talk.request('E', empty);
    await flush();
    const last = peer.received.at(-1) as TalkFrame;
    expect(last.requestId).toBe(0xfffffe);
    expect(hexString(last.encode())).toBe('03 45 00 00 00 00 00 00 00 FE FF FF');
    await peer.talk.close();
  });

  test('responses for unknown ids are ignored', async () => {
    const peer = new RawPeer();
    const messages: TalkMessage[] = [];
    serve(peer.talk, (m) => {
      messages.push(m);
    });
    peer.send(new TalkFrame({ kind: TalkKind.message, responseId: 99 }));
    peer.send(new TalkFrame({ kind: TalkKind.streamItem, responseId: 99 }));
    peer.send(new TalkFrame({ kind: TalkKind.extend, responseId: 99 }));
    peer.sendHex('24 63 00 00 05 00');
    await flush();
    expect(peer.received).toHaveLength(0);
    expect(messages).toHaveLength(0);
    expect(peer.talk.isOpen).toBe(true);
    await peer.talk.close();
  });

  test('an unknown response id with HAS_REQUEST gets ABORT NOT_FOUND', async () => {
    const peer = new RawPeer();
    peer.send(new TalkFrame({ kind: TalkKind.message, requestId: 5, responseId: 99 }));
    peer.send(new TalkFrame({ kind: TalkKind.streamItem, requestId: 6, responseId: 98 }));
    await flush();
    expect(peer.received).toHaveLength(2);
    const [first, second] = peer.received as [TalkFrame, TalkFrame];
    expect(first.kind).toBe(TalkKind.abort);
    expect(first.responseId).toBe(5);
    expect(first.status.code).toBe(StatusCode.notFound);
    expect(second.responseId).toBe(6);
    expect(second.status.code).toBe(StatusCode.notFound);
    expect(peer.talk.isOpen).toBe(true);
    await peer.talk.close();
  });

  test('a request id reused while outstanding supersedes the old one', async () => {
    const peer = new RawPeer();
    const held: TalkMessage[] = [];
    serve(peer.talk, (m) => {
      held.push(m);
    });
    peer.send(new TalkFrame({ kind: TalkKind.message, procedure: Name.of('OLD'), requestId: 1 }));
    await flush();
    peer.send(new TalkFrame({ kind: TalkKind.message, procedure: Name.of('NEW'), requestId: 1 }));
    await flush();
    expect(held).toHaveLength(2);
    const [old, current] = held as [TalkMessage, TalkMessage];
    expect(old.isCancelled).toBe(true);
    expect(old.canReply).toBe(false);
    throwsStatus(() => old.reply(empty), StatusCode.failedPrecondition);
    void current.reply(bytes([1]));
    await flush();
    expect(peer.received).toHaveLength(1);
    expect(peer.received[0]?.responseId).toBe(1);
    expect([...(peer.received[0] as TalkFrame).payload]).toEqual([1]);
    await peer.talk.close();
  });
});
