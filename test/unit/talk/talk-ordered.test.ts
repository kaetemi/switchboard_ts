/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Port of the Dart `test/talk_test.dart`, groups 'synchronous transport'
// and 'ordered answers': frames the peer sends while this side is sending,
// sends the transport refuses, and the answers of `ordered` requests
// delivered in wire order with the channel's messages.
//
// The Dart groups run over a synchronous link, where a frame the peer sends
// is decoded at once, even inside this side's own send. A TS channel reads
// its frames through a stream reader, one per microtask, never inside a
// send: here the peer's frame crosses the one being sent on the wire,
// arriving once the send returned, and a burst of frames is read before the
// consumer takes any of it.

import { describe, expect, test } from 'vitest';
import { Name } from '../../../src/core/name.ts';
import { Status, StatusCode, SwitchboardError } from '../../../src/core/status.ts';
import type { TalkMessage, TalkOptions } from '../../../src/core/talk/contract.ts';
import { TalkAbortError, TalkChannel } from '../../../src/core/talk/talk-channel.ts';
import { TalkFrame, TalkKind } from '../../../src/core/talk/talk-frame.ts';
import { ManualClock } from '../../../src/testing/manual-clock.ts';
import { Carrier } from '../../support/talk/carrier.ts';
import { type FrameEnd, frameLink, Pair, RawPeer } from '../../support/talk/frame-link.ts';
import {
  bytes,
  Collector,
  expectStatus,
  flush,
  Outcome,
  Queue,
  rejectsStatus,
  serve,
  throwsStatus,
} from '../../support/talk/helpers.ts';
import {
  channelPair,
  type MemoryMuxChannel,
  memoryMuxPair,
} from '../../support/talk/memory-mux.ts';

const empty = new Uint8Array(0);

/** A Talk channel on an {@link Carrier}, and the far end of its mux channel. */
async function carriedChannel(
  options?: Partial<TalkOptions>,
): Promise<[TalkChannel, Carrier, MemoryMuxChannel]> {
  const clock = options?.clock;
  const [client, server] = memoryMuxPair(clock === undefined ? {} : { mux: { clock } });
  const [local, remote] = await channelPair(client, server);
  const carrier = new Carrier(local);
  return [new TalkChannel(carrier, options), carrier, remote];
}

/**
 * A responder holding request 1 of a peer driven by hand, on an
 * {@link Carrier}. [cancelDuring] has the peer cancel the
 * request the moment the responder sends a reply: the Dart responder
 * handled the cancel inside its own send, over a synchronous link; here
 * the cancel crosses the reply on the wire and is read once the send
 * returned.
 */
class SyncResponder {
  static async create(
    clock: ManualClock,
    options: { stream?: boolean } = {},
  ): Promise<SyncResponder> {
    const [talk, carrier, peer] = await carriedChannel({ clock });
    return new SyncResponder(talk, carrier, peer, options.stream === true);
  }

  readonly talk: TalkChannel;
  readonly #carrier: Carrier;
  readonly #peer: MemoryMuxChannel;
  readonly #held: TalkMessage[] = [];

  /** Every frame the responder sent, in wire order. */
  readonly sent: TalkFrame[] = [];

  private constructor(
    talk: TalkChannel,
    carrier: Carrier,
    peer: MemoryMuxChannel,
    stream: boolean,
  ) {
    this.talk = talk;
    this.#carrier = carrier;
    this.#peer = peer;
    void this.#read();
    serve(talk, (m) => {
      this.#held.push(m);
    });
    this.send(
      new TalkFrame({ kind: TalkKind.message, procedure: Name.of('Q'), requestId: 1, stream }),
    );
  }

  async #read(): Promise<void> {
    const reader = this.#peer.readable.getReader();
    for (;;) {
      const { value, done } = await reader.read();
      if (done) {
        return;
      }
      this.sent.push(TalkFrame.decode(value));
    }
  }

  /** The peer's request 1, once delivered. */
  get request(): TalkMessage {
    expect(this.#held).toHaveLength(1);
    return this.#held[0] as TalkMessage;
  }

  send(frame: TalkFrame): void {
    void this.#peer.send(frame.encode());
  }

  /**
   * The peer cancels request 1 the moment the responder sends the first
   * frame [when] accepts: once the frame is on the wire, or, if [refuse],
   * in place of it, the send then failing with FRAME_TOO_LARGE.
   */
  cancelDuring(when: (frame: TalkFrame) => boolean, options: { refuse?: boolean } = {}): void {
    const refuse = options.refuse === true;
    this.#carrier.intercept = (frame, pass) => {
      if (!when(frame)) {
        return pass();
      }
      this.#carrier.intercept = undefined;
      const sent = refuse ? undefined : pass();
      this.send(
        new TalkFrame({
          kind: TalkKind.abort,
          requestId: 1,
          payload: Status.of(StatusCode.cancelled).encode(),
        }),
      );
      if (sent === undefined) {
        throw SwitchboardError.of(StatusCode.frameTooLarge);
      }
      return sent;
    };
  }

  /** The final responses sent for request 1. */
  get finals(): TalkFrame[] {
    return this.sent.filter(isFinal);
  }
}

function isFinal(f: TalkFrame): boolean {
  return f.responseId === 1 && (f.kind === TalkKind.message || f.kind === TalkKind.abort);
}

describe('synchronous transport', () => {
  test('a response sent while the request is being sent is not lost', async () => {
    // The peer answers the moment the request frame is written; the answer
    // is read once the send returned.
    const [local, foreign] = frameLink();
    const talk = new TalkChannel(local);
    local.tap = (data) => {
      const frame = TalkFrame.decode(data);
      if (frame.hasRequest && frame.kind === TalkKind.message) {
        foreign.add(
          new TalkFrame({
            kind: TalkKind.message,
            responseId: frame.requestId,
            payload: bytes([42]),
          }).encode(),
        );
      }
      return true;
    };
    const r = await talk.request('X', empty, { timeout: 0 });
    expect([...r.payload]).toEqual([42]);
    expect(talk.outgoingRequestCount).toBe(0);
    await talk.close();
  });

  test('a synchronous rejection reaches the request', async () => {
    // The rejection is read once the send of the request returned.
    const p = new Pair({ b: { maxIncomingRequests: 1 } });
    const held: TalkMessage[] = [];
    serve(p.b, (m) => {
      held.push(m);
    });
    const first = p.a.request('A', empty, { timeout: 0 });
    const second = p.a.request('B', empty, { timeout: 0 });
    await rejectsStatus(second, StatusCode.resourceExhausted);
    await flush();
    expect(held).toHaveLength(1);
    void held[0]?.reply(empty);
    await first;
    expect(p.a.outgoingRequestCount).toBe(0);
    await p.close();
  });

  test('a failed send leaves no request behind', async () => {
    // The mux channel refuses frames over 64 bytes as it is given them.
    const [talk, carrier, peer] = await carriedChannel();
    carrier.limit = 64;
    const held: TalkMessage[] = [];
    serve(talk, (m) => {
      held.push(m);
    });
    throwsStatus(() => talk.request('BIG', new Uint8Array(100)), StatusCode.frameTooLarge);
    expect(talk.outgoingRequestCount).toBe(0);
    // A chained reply that cannot be sent leaves the message answerable.
    void peer.send(
      new TalkFrame({ kind: TalkKind.message, procedure: Name.of('Q'), requestId: 1 }).encode(),
    );
    await flush();
    expect(held).toHaveLength(1);
    const m = held[0] as TalkMessage;
    throwsStatus(() => m.replyRequest(new Uint8Array(100)), StatusCode.frameTooLarge);
    expect(talk.outgoingRequestCount).toBe(0);
    expect(m.canReply).toBe(true);
    void m.reply(empty);
    await talk.close();
  });

  // A cancel that crosses a reply on the wire is handled once the send
  // returned: after a final reply it is ignored (the request is answered),
  // after anything else it is answered ABORT CANCELLED as usual. Either way
  // the request gets exactly one final response.

  /** Ends the test: no timer may be left, before or after the close. */
  async function finish(clock: ManualClock, r: SyncResponder): Promise<void> {
    expect(clock.pendingCount).toBe(0);
    void r.talk.close();
    await flush();
    expect(clock.pendingCount).toBe(0);
  }

  function cancelled(id: number): TalkFrame {
    return new TalkFrame({
      kind: TalkKind.abort,
      responseId: id,
      payload: Status.of(StatusCode.cancelled).encode(),
    });
  }

  test('a cancel during the final reply gets no second final', async () => {
    const clock = new ManualClock();
    const r = await SyncResponder.create(clock);
    await flush();
    r.cancelDuring(isFinal);
    void r.request.reply(bytes([1]));
    await flush();
    expect(r.finals).toHaveLength(1);
    expect(
      r.finals[0]?.equals(
        new TalkFrame({ kind: TalkKind.message, responseId: 1, payload: bytes([1]) }),
      ),
    ).toBe(true);
    expect(r.request.canReply).toBe(false);
    expect(r.request.isCancelled, 'answered first').toBe(false);
    expect(r.talk.incomingRequestCount).toBe(0);
    await finish(clock, r);
  });

  test('a cancel during replyAbort keeps the application status', async () => {
    const clock = new ManualClock();
    const r = await SyncResponder.create(clock);
    await flush();
    r.cancelDuring(isFinal);
    void r.request.replyAbort(new Status(300, 'mine'));
    await flush();
    expect(r.finals).toHaveLength(1);
    const final = r.finals[0] as TalkFrame;
    expect(final.kind).toBe(TalkKind.abort);
    expect(final.status.equals(new Status(300, 'mine'))).toBe(true);
    expect(r.talk.incomingRequestCount).toBe(0);
    await finish(clock, r);
  });

  test('a cancel during an automatic timeout abort gets no second final', async () => {
    const clock = new ManualClock();
    const r = await SyncResponder.create(clock);
    await flush();
    r.cancelDuring(isFinal);
    await clock.elapseAsync(10_000);
    await flush();
    expect(r.finals).toHaveLength(1);
    expect(r.finals[0]?.status.known).toBe(StatusCode.deadlineExceeded);
    expect(r.request.canReply).toBe(false);
    expect(r.request.isCancelled).toBe(true);
    expect(r.talk.incomingRequestCount).toBe(0);
    await finish(clock, r);
  });

  test('a cancel during a chained reply leaves the chained request as the final', async () => {
    const clock = new ManualClock();
    const r = await SyncResponder.create(clock);
    await flush();
    r.cancelDuring(isFinal);
    const chained = r.request.startReplyRequest(bytes([2]), { timeout: 0 });
    const answer = new Outcome(chained.response);
    await flush();
    expect(r.finals).toHaveLength(1);
    expect(r.finals[0]?.requestId).toBe(chained.requestId);
    expect(r.talk.incomingRequestCount).toBe(0);
    expect(r.talk.outgoingRequestCount).toBe(1);
    // The peer answers the chained final of what it cancelled.
    r.send(cancelled(chained.requestId));
    await flush();
    expect(answer.error).toBeInstanceOf(TalkAbortError);
    expect(r.talk.outgoingRequestCount).toBe(0);
    await finish(clock, r);
  });

  test('a cancel during a final that fails to send is answered', async () => {
    const clock = new ManualClock();
    const r = await SyncResponder.create(clock);
    await flush();
    r.cancelDuring(isFinal, { refuse: true });
    throwsStatus(() => r.request.reply(bytes([1])), StatusCode.frameTooLarge);
    await flush();
    expect(r.finals).toHaveLength(1);
    expect(r.finals[0]?.status.known).toBe(StatusCode.cancelled);
    expect(r.request.isCancelled).toBe(true);
    expect(r.request.canReply).toBe(false);
    expect(r.talk.incomingRequestCount).toBe(0);
    await finish(clock, r);
  });

  test('a cancel during a stream item is answered after it', async () => {
    const clock = new ManualClock();
    const r = await SyncResponder.create(clock, { stream: true });
    await flush();
    r.cancelDuring((f) => f.kind === TalkKind.streamItem);
    void r.request.replyItem(bytes([1]));
    await flush();
    expect(r.sent.map((f) => f.kind)).toEqual([TalkKind.streamItem, TalkKind.abort]);
    expect(r.finals).toHaveLength(1);
    expect(r.finals[0]?.status.known).toBe(StatusCode.cancelled);
    expect(r.request.isCancelled).toBe(true);
    expect(r.talk.incomingRequestCount).toBe(0);
    await finish(clock, r);
  });

  test('a cancel during an item request is answered after it', async () => {
    const clock = new ManualClock();
    const r = await SyncResponder.create(clock, { stream: true });
    await flush();
    r.cancelDuring((f) => f.kind === TalkKind.streamItem);
    const item = r.request.startReplyItemRequest(bytes([1]), { timeout: 0 });
    const answer = new Outcome(item.response);
    await flush();
    expect(r.sent.map((f) => [f.kind, f.requestId])).toEqual([
      [TalkKind.streamItem, item.requestId],
      [TalkKind.abort, 0],
    ]);
    expect(r.finals).toHaveLength(1);
    expect(r.finals[0]?.status.known).toBe(StatusCode.cancelled);
    expect(r.talk.incomingRequestCount).toBe(0);
    r.send(cancelled(item.requestId));
    await flush();
    expect(answer.error).toBeInstanceOf(TalkAbortError);
    await finish(clock, r);
  });

  test('a cancel during EXTEND is answered after it', async () => {
    const clock = new ManualClock();
    const r = await SyncResponder.create(clock);
    await flush();
    r.cancelDuring((f) => f.kind === TalkKind.extend);
    void r.request.extend({ deadline: 60_000 });
    await flush();
    expect(r.sent.map((f) => f.kind)).toEqual([TalkKind.extend, TalkKind.abort]);
    expect(r.finals).toHaveLength(1);
    expect(r.finals[0]?.status.known).toBe(StatusCode.cancelled);
    expect(r.talk.incomingRequestCount).toBe(0);
    await finish(clock, r);
  });
});

describe('ordered answers', () => {
  /**
   * A channel whose peer is driven by hand at the frame level. The Dart
   * link decoded a burst of frames in one go, as the mux delivers them;
   * here the channel reads them one per microtask, so the tests hold the
   * consumer back until a burst was read ({@link flush}).
   */
  function syncLink(options: Partial<TalkOptions> = {}): [TalkChannel, FrameEnd] {
    const peer = new RawPeer({ options });
    return [peer.talk, peer.raw];
  }

  function plain(procedure: string): Uint8Array {
    return new TalkFrame({ kind: TalkKind.message, procedure: Name.of(procedure) }).encode();
  }

  function answer(id: number): Uint8Array {
    return new TalkFrame({ kind: TalkKind.message, responseId: id }).encode();
  }

  function abortAnswer(id: number, code: StatusCode): Uint8Array {
    return new TalkFrame({
      kind: TalkKind.abort,
      responseId: id,
      payload: Status.of(code).encode(),
    }).encode();
  }

  /** A consumer of [talk]'s messages recording their procedures in [seen], paused until resumed. */
  function held(talk: TalkChannel, seen: string[]): Collector<TalkMessage> {
    return new Collector(talk.messages, {
      paused: true,
      onValue: (m) => seen.push(m.procedureName),
    });
  }

  test('the answer comes after the messages sent before it, and code waiting on it runs before the messages sent after it', async () => {
    const [talk, peer] = syncLink();
    const seen: string[] = [];
    const sub = held(talk, seen);
    const awaiting = async (): Promise<void> => {
      await talk.request('Q', empty, { ordered: true });
      seen.push('awaited');
    };
    const ordered = awaiting();
    const unordered = talk.request('P', empty).then(() => seen.push('unordered'));
    for (const frame of [
      plain('M1'),
      plain('M2'),
      plain('M3'),
      answer(1),
      plain('M4'),
      plain('M5'),
      answer(2),
    ]) {
      peer.add(frame);
    }
    await flush();
    sub.resume();
    await Promise.all([ordered, unordered]);
    await flush();
    expect(seen.filter((s) => s !== 'unordered')).toEqual([
      'M1',
      'M2',
      'M3',
      'awaited',
      'M4',
      'M5',
    ]);
    // Without the option the answer overtakes messages sent before it.
    expect(seen.indexOf('unordered')).toBeLessThan(seen.indexOf('M5'));
    expect(talk.outgoingRequestCount).toBe(0);
    await talk.close();
  });

  test('an abort answer is ordered too', async () => {
    const [talk, peer] = syncLink();
    const seen: string[] = [];
    const sub = held(talk, seen);
    const failed = talk.request('Q', empty, { ordered: true }).then(
      () => {
        throw new Error('no abort');
      },
      (e: unknown) => {
        seen.push(`aborted ${(e as SwitchboardError).status.code}`);
      },
    );
    peer.add(plain('M1'));
    peer.add(plain('M2'));
    peer.add(abortAnswer(1, StatusCode.notFound));
    peer.add(plain('M3'));
    await flush();
    sub.resume();
    await failed;
    await flush();
    expect(seen).toEqual(['M1', 'M2', `aborted ${StatusCode.notFound}`, 'M3']);
    await talk.close();
  });

  test('without a listener the answer is delivered at once', async () => {
    const [talk, peer] = syncLink();
    const response = talk.request('Q', empty, { ordered: true });
    peer.add(plain('M1'));
    peer.add(answer(1));
    await response;
    // The message stays buffered for a later listener.
    expect((await new Queue(talk.messages).next()).procedureName).toBe('M1');
    await talk.close();
  });

  test('a paused listener holds the answer; resuming or cancelling delivers it', async () => {
    const clock = new ManualClock();
    const [talk, peer] = syncLink({ clock });
    const seen: string[] = [];
    const sub = held(talk, seen);
    let done = false;
    const response = talk.request('Q', empty, { ordered: true }).then(() => {
      done = true;
    });
    peer.add(plain('M1'));
    peer.add(answer(1));
    await flush();
    expect(done).toBe(false);
    // Local failures do not wait.
    const late = talk.request('T', empty, { timeout: 30, ordered: true });
    await clock.elapseAsync(30);
    await rejectsStatus(late, StatusCode.deadlineExceeded);
    sub.resume();
    await response;
    expect(seen).toEqual(['M1']);

    // A cancelled subscription releases the answers it held.
    sub.pause();
    done = false;
    const second = talk.request('Q', empty, { ordered: true }).then(() => {
      done = true;
    });
    peer.add(plain('M2'));
    peer.add(answer(3));
    await flush();
    expect(done).toBe(false);
    await sub.cancel();
    await second;
    // Delivery is pull based: the pull still pending when the consumer
    // paused takes M2, and the answer behind it waits all the same.
    expect(seen).toEqual(['M1', 'M2']);
    // Answers arriving afterwards are delivered at once.
    const third = talk.request('Q', empty, { ordered: true });
    peer.add(answer(4));
    await third;
    await talk.close();
  });

  for (const end of ['a channel abort', 'a protocol error', 'close', 'the peer closing'] as const) {
    test(`an answer that arrived before ${end} keeps its place`, async () => {
      const [talk, peer] = syncLink();
      const seen: string[] = [];
      // Held back until the end was read; records the iteration's error in
      // its place, as the Dart listener's onError does.
      const iterator = talk.messages[Symbol.asyncIterator]();
      let start!: () => void;
      const started = new Promise<void>((resolve) => {
        start = resolve;
      });
      const listened = (async () => {
        await started;
        try {
          for (;;) {
            const result = await iterator.next();
            if (result.done === true) {
              return;
            }
            seen.push(result.value.procedureName);
          }
        } catch {
          seen.push('error');
        }
      })();
      const response = talk.request('Q', empty, { ordered: true }).then(() => {
        seen.push('answer');
      });
      peer.add(plain('M1'));
      peer.add(plain('M2'));
      peer.add(answer(1));
      // Read before the end: a closing channel drops what it has not read.
      await flush();
      switch (end) {
        case 'a channel abort':
          peer.add(
            new TalkFrame({
              kind: TalkKind.abort,
              payload: Status.of(StatusCode.unavailable).encode(),
            }).encode(),
          );
          break;
        case 'a protocol error':
          peer.add(bytes([0xc0]));
          break;
        case 'close':
          void talk.close();
          break;
        default:
          peer.end();
      }
      await flush();
      start();
      await response;
      await talk.done;
      await listened;
      await flush();
      expect(seen).toEqual(['M1', 'M2', 'answer', ...(end === 'a channel abort' ? ['error'] : [])]);
    });
  }

  for (const abort of [false, true]) {
    test(`a paused listener holds an ${abort ? 'abort' : 'answer'} that arrived before the channel ended; resuming or cancelling delivers it`, async () => {
      const clock = new ManualClock();
      for (const resume of [true, false]) {
        const [talk, peer] = syncLink({ clock });
        const seen: string[] = [];
        const sub = held(talk, seen);
        const response = new Outcome(
          talk.request('Q', empty, { ordered: true }).finally(() => seen.push('answer')),
        );
        peer.add(plain('M1'));
        peer.add(abort ? abortAnswer(1, StatusCode.notFound) : answer(1));
        await flush();
        expect(talk.outgoingRequestCount).toBe(0);
        const closed = new Outcome(talk.close(Status.of(StatusCode.unavailable)));
        await flush();
        expect(closed.isDone).toBe(true);
        expect(response.isDone).toBe(false);
        if (resume) {
          sub.resume();
          await flush();
          expect(seen).toEqual(['M1', 'answer']);
        } else {
          void sub.cancel();
          await flush();
          expect(seen).toEqual(['answer']);
        }
        expect(response.isDone).toBe(true);
        if (abort) {
          expect(response.error).toBeInstanceOf(TalkAbortError);
          expectStatus(response.error, StatusCode.notFound);
        } else {
          expect(response.error).toBeUndefined();
          expect(response.value?.responseId).toBe(1);
        }
        void sub.cancel();
        await flush();
        expect(clock.pendingCount).toBe(0);
      }
    });
  }
});
