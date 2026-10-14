/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Port of the Dart `test/talk_bulk_test.dart`, from the group 'timeouts'
// to the end (the groups 'timeouts', 'cancellation', 'failed payloads'
// and 'bulk channel rules', and the tests between them): Talk bulk
// payloads (wiki page "Polyverse Switchboard Talk", section "Bulk
// payloads") over the fake mux pair. A Talk channel installs bulk routing
// on its connection itself; the peers here install it on both
// connections at once, as the Dart `Peers` routes bulk channels with
// `TalkChannel.adoptBulk` from the start. Timings the Dart tests take in
// real time run on a ManualClock.

import { describe, expect, onTestFinished, test } from 'vitest';
import { bytesEqual, hexString } from '../../../src/core/bytes.ts';
import type { MuxChannel } from '../../../src/core/mux/contract.ts';
import { Name } from '../../../src/core/name.ts';
import { Status, StatusCode, SwitchboardError } from '../../../src/core/status.ts';
import type { TalkMessage, TalkOptions } from '../../../src/core/talk/contract.ts';
import { TalkBulkOpen, TalkBulkReference } from '../../../src/core/talk/talk-bulk.ts';
import { installBulkRouting } from '../../../src/core/talk/talk-bulk-routes.ts';
import { TalkAbortError, TalkChannel } from '../../../src/core/talk/talk-channel.ts';
import { TalkFrame, TalkKind } from '../../../src/core/talk/talk-frame.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';
import { ManualClock } from '../../../src/testing/manual-clock.ts';
import {
  collect,
  generated,
  Peers,
  pattern,
  SourceController,
  until,
} from '../../support/talk/bulk-peers.ts';
import { fakeMuxPair } from '../../support/talk/fake-channel.ts';
import {
  bytes,
  Collector,
  expectStatus,
  flush,
  Outcome,
  rejectsStatus,
  serve,
} from '../../support/talk/helpers.ts';

const empty = new Uint8Array(0);

/** Expects [actual] to hold the bytes of [expected]. */
function expectBytes(actual: Uint8Array, expected: Uint8Array): void {
  expect(actual.length).toBe(expected.length);
  expect(bytesEqual(actual, expected), 'bytes differ').toBe(true);
}

/** [ms] of [clock]. */
function sleep(clock: ManualClock, ms: number): Promise<void> {
  return new Promise<void>((resolve) => {
    clock.setTimeout(resolve, ms);
  });
}

/** Moves [clock] on in steps of [step] until [promise] settled (at most [limit] ms), and returns it. */
async function elapseUntil<T>(
  clock: ManualClock,
  promise: Promise<T>,
  limit = 60_000,
  step = 5,
): Promise<T> {
  const outcome = new Outcome(promise);
  for (let t = 0; t < limit && !outcome.isDone; t += step) {
    await clock.elapseAsync(step);
  }
  expect(outcome.isDone, `not settled within ${limit} ms`).toBe(true);
  return promise;
}

/**
 * A byte source that makes a chunk each time its reader asks for one, as
 * the Dart tests' source fed by a periodic timer while not paused; whether
 * its reader cancelled it shows.
 */
class PulledSource {
  readonly cancelled = new Deferred<void>();
  readonly stream: ReadableStream<Uint8Array>;

  constructor(make: () => Uint8Array) {
    this.stream = new ReadableStream<Uint8Array>(
      {
        pull: (controller) => controller.enqueue(make()),
        cancel: () => {
          this.cancelled.resolve();
        },
      },
      { highWaterMark: 0 },
    );
  }
}

/** The next Talk channel the client of [peers] opened, at the server (Dart's `peers.accepted.next`). */
async function accept(peers: Peers): Promise<TalkChannel> {
  return new TalkChannel(await peers.server.accept(), peers.serverOptions);
}

/** The encoded frame of a BULK message referencing bulk [number]. */
function bulkMessage(
  procedure: string,
  number: number,
  options: { requestId?: number; length?: number } = {},
): Uint8Array {
  return new TalkFrame({
    kind: TalkKind.message,
    procedure: Name.of(procedure),
    requestId: options.requestId,
    bulk: true,
    payload: new TalkBulkReference(number, options.length).encode(),
  }).encode();
}

/** The open payload of bulk channel [number] for [parent]. */
function bulkOpen(parent: MuxChannel, number: number): Uint8Array {
  return new TalkBulkOpen(parent.id, number).encode();
}

// The tests of the Dart file before the group 'timeouts' are in
// talk-bulk.test.ts.

describe('timeouts', () => {
  const short: Partial<TalkOptions> = { requestTimeout: 200, replyTimeout: 150 };

  /** [count] pieces of [size] bytes, one every [every] ms of [clock]. */
  async function* slow(
    clock: ManualClock,
    count: number,
    size: number,
    every: number,
  ): AsyncGenerator<Uint8Array> {
    for (let i = 0; i < count; i++) {
      await sleep(clock, every);
      yield pattern(size, i * size);
    }
  }

  test('an upload longer than both timeouts succeeds while it progresses (reassembled)', async () => {
    const clock = new ManualClock();
    const peers = await Peers.connect({ options: { clock }, serverOptions: short });
    const [talk, server] = await peers.open({ options: short });
    serve(server, (m) => m.reply(bytes([1])));
    // 12 pieces, 50 ms apart: 600 ms, three times the requester timeout.
    const answer = await elapseUntil(
      clock,
      talk.request('PUT', empty, { bulk: slow(clock, 12, 70000, 50) }),
    );
    expect([...answer.payload]).toEqual([1]);
  });

  test('a streamed upload read for longer than both timeouts, declared with EXTEND', async () => {
    const clock = new ManualClock();
    const peers = await Peers.connect({
      options: { clock },
      serverOptions: { ...short, streamBulk: () => true },
    });
    const [talk, server] = await peers.open({ options: short });
    serve(server, async (m) => {
      // A long read is declared, as any long work on a request is.
      void m.extend({ deadline: 5000 });
      let n = 0;
      for await (const chunk of m.bulk) {
        n += chunk.length;
        await sleep(clock, 30);
      }
      void m.reply(bytes([Math.floor(n / 100000)]));
    });
    const answer = await elapseUntil(
      clock,
      talk.request('PUT', empty, {
        bulk: generated(1000000, { piece: 65536 }),
        bulkLength: 1000000,
      }),
    );
    expect([...answer.payload]).toEqual([10]);
  });

  test('a stalled upload times out and is cancelled', async () => {
    const clock = new ManualClock();
    const peers = await Peers.connect({ options: { clock }, serverOptions: short });
    const [talk, server] = await peers.open({ options: short });
    const served: TalkMessage[] = [];
    serve(server, (m) => served.push(m));
    const source = new SourceController();
    source.add(pattern(100000));
    const request = talk.request('PUT', empty, { bulk: source.stream });
    await elapseUntil(
      clock,
      request.catch(() => undefined),
    );
    await rejectsStatus(request, StatusCode.deadlineExceeded);
    await flush();
    expect(source.hasListener).toBe(false);
    expect(served).toHaveLength(0);
    source.close();
  });
});

test("messages sent before the peer's LIMITS are sized against the window it announces", async () => {
  // The server grants 256 bytes: frames of at most 112 bytes. The client
  // talks at once, before that LIMITS arrived.
  // Adapted: the fake mux has no LIMITS frame; its channels wait for
  // `announceLimits`, and the subframe length the client's channel
  // assumes before it (the default window's, as the frame mux) is
  // modelled here.
  const [client, server] = fakeMuxPair({ mux: { initialWindow: 256 }, awaitLimits: true });
  onTestFinished(async () => {
    await client.close();
    await server.close();
  });
  installBulkRouting(client);
  installBulkRouting(server);
  const received: string[] = [];
  const all = new Deferred<void>();
  void server.accept().then((channel) => {
    serve(new TalkChannel(channel), (m) => {
      received.push(`${m.procedureName} ${m.payload.length} ${m.isBulk}`);
      if (received.length === 3) {
        all.resolve();
      }
    });
  });
  const raw = client.open(empty);
  Object.defineProperty(raw, 'maxSubframeLength', {
    get: () => (raw.awaitingLimits ? 32752 : Math.floor(256 / 2) - 16),
  });
  expect(raw.maxSubframeLength).toBe(32752);
  const talk = new TalkChannel(raw);
  void talk.send('SMALL', pattern(50));
  void talk.send('BIG', pattern(200));
  void talk.send('AFTER', pattern(10));
  await flush();
  client.announceLimits();
  server.announceLimits();
  await all.promise;
  expect(raw.maxSubframeLength).toBe(112);
  expect(received).toEqual(['SMALL 50 false', 'BIG 200 true', 'AFTER 10 false']);
});

test('a large answer to a request completes while its requester drains', async () => {
  const peers = await Peers.connect();
  const [talk, server] = await peers.open();
  const answering = new Deferred<TalkMessage>();
  serve(server, (m) => answering.resolve(m));
  const big = talk.request('GET', empty);
  const request = await answering.promise;
  // The client sends GOAWAY: no new channels toward it, but the bulk
  // channel of an answer on a channel it opened is not a new one.
  // Adapted: the fake mux closes at once on GOAWAY; the drain is modelled
  // by holding its close until the client's channel is done.
  const close = peers.client.close.bind(peers.client);
  peers.client.close = async () => {
    await talk.done;
    await close();
  };
  const drained = peers.client.goAway();
  await flush();
  expect(peers.server.peerGoingAway).toBe(true);
  void request.reply(pattern(150000));
  const answer = await big;
  expect(answer.isBulk).toBe(true);
  expectBytes(answer.payload, pattern(150000));
  await talk.close();
  await drained;
});

test('a lower bulkThreshold sends bulk earlier', async () => {
  const peers = await Peers.connect();
  const [talk, server] = await peers.open({ options: { bulkThreshold: 100 } });
  const seen: boolean[] = [];
  serve(server, (m) => {
    seen.push(m.isBulk);
    void m.reply(empty);
  });
  await talk.request('X', new Uint8Array(80));
  await talk.request('X', new Uint8Array(200));
  expect(seen).toEqual([false, true]);
});

test('4 MB streamed to a slow reader is never held whole', async () => {
  const peers = await Peers.connect({ serverOptions: { streamBulk: () => true } });
  const [talk, server] = await peers.open();
  const total = 4 * 1024 * 1024;
  let produced = 0;
  let consumed = 0;
  let maxAhead = 0;
  const done = new Deferred<void>();
  let mismatch: string | undefined;
  serve(server, async (m) => {
    expect(m.bulkLength).toBe(total);
    for await (const chunk of m.bulk) {
      if (mismatch === undefined && !bytesEqual(chunk, pattern(chunk.length, consumed))) {
        mismatch = `chunk at ${consumed}: ${hexString(chunk.subarray(0, 8))}`;
      }
      consumed += chunk.length;
      const ahead = produced - consumed;
      if (ahead > maxAhead) {
        maxAhead = ahead;
      }
      if (consumed % (256 * 1024) < chunk.length) {
        // Slow: a pause every 256 KiB.
        await new Promise<void>((resolve) => setTimeout(resolve, 5));
      }
    }
    void m.reply(empty);
    done.resolve();
  });
  await talk.request('PUT', empty, {
    bulk: generated(total, { piece: 32768, produced: (n) => (produced += n) }),
    bulkLength: total,
  });
  await done.promise;
  expect(mismatch).toBeUndefined();
  expect(consumed).toBe(total);
  // The sender ran ahead of the reader by at most the window it was
  // granted (1 MiB) and a few chunks.
  expect(maxAhead).toBeLessThan(1024 * 1024 + 4 * 65536);
});

test('a slow bulk reader stalls only its bulk channel', async () => {
  const peers = await Peers.connect({ serverOptions: { streamBulk: () => true } });
  const [talk, server] = await peers.open();
  const [other, otherServer] = await peers.open();
  serve(otherServer, (m) => m.reply(m.payload));
  let reading: Collector<Uint8Array> | undefined;
  let received = 0;
  serve(server, (m) => {
    if (m.procedureName === 'PUT') {
      reading = new Collector(m.bulk, {
        paused: true,
        onValue: (chunk) => {
          received += chunk.length;
        },
      });
    } else {
      void m.reply(bytes([1]));
    }
  });
  let produced = 0;
  const upload = talk.request('PUT', empty, {
    bulk: generated(2 * 1024 * 1024, { produced: (n) => (produced += n) }),
    bulkLength: 2 * 1024 * 1024,
  });
  // Adapted: what Dart lets run for 100 ms runs until nothing moves.
  await flush(50);
  expect(received).toBe(0);
  // Stalled at the window: the source is paused.
  expect(produced).toBeLessThan(1024 * 1024 + 4 * 65536);
  // The same channel, and another one, keep flowing.
  expect([...(await talk.request('PING', empty)).payload]).toEqual([1]);
  expect([...(await other.request('ECHO', bytes([7]))).payload]).toEqual([7]);
  reading?.resume();
  upload.catch(() => {});
  await until(() => received > 1024 * 1024);
  await reading?.cancel();
});

describe('cancellation', () => {
  test('the receiver cancelling the stream stops the sender', async () => {
    const peers = await Peers.connect({ serverOptions: { streamBulk: () => true } });
    const [talk, server] = await peers.open();
    // Adapted: Dart adds 1000 bytes every millisecond while the source is
    // not paused; this source makes them when they are read.
    const source = new PulledSource(() => pattern(1000));
    serve(server, async (m) => {
      const reader = m.bulk.getReader();
      const first = await reader.read();
      await reader.cancel();
      expect(first.value?.length ?? 0).toBeGreaterThan(0);
      void m.reply(bytes([1]));
    });
    const answer = await talk.request('PUT', empty, { bulk: source.stream });
    expect([...answer.payload]).toEqual([1]);
    await source.cancelled.promise;
  });

  test('a reply payload the receiver stops fails replyBulk', async () => {
    const peers = await Peers.connect();
    const [talk, server] = await peers.open({ options: { streamBulk: () => true } });
    const sent = new Deferred<unknown>();
    serve(server, (m) => {
      m.reply(empty, { bulk: generated(4 * 1024 * 1024) }).then(
        () => sent.resolve(undefined),
        (e: unknown) => sent.resolve(e),
      );
    });
    const answer = await talk.request('GET', empty);
    // The reader takes one chunk and pauses.
    const reading = answer.bulk.getReader();
    await reading.read();
    await flush(20);
    await reading.cancel();
    expectStatus(await sent.promise, StatusCode.cancelled);
  });

  test('the sender cancelling the request stops the payload', async () => {
    const peers = await Peers.connect({ serverOptions: { streamBulk: () => true } });
    const [talk, server] = await peers.open();
    const request = new Deferred<TalkMessage>();
    let reading: Collector<Uint8Array> | undefined;
    serve(server, (m) => {
      request.resolve(m);
      reading = new Collector(m.bulk);
    });
    const source = new SourceController();
    source.add(pattern(1000));
    const pending = talk.startRequest('PUT', empty, { bulk: source.stream });
    const m = await request.promise;
    pending.cancel();
    await reading?.finished;
    expectStatus(reading?.error, StatusCode.cancelled);
    await m.onCancel;
    await flush();
    expect(source.hasListener).toBe(false);
    source.close();
  });

  test('a source error fails the payload with its status', async () => {
    const peers = await Peers.connect({ serverOptions: { streamBulk: () => true } });
    const [talk, server] = await peers.open();
    const failed = new Deferred<unknown>();
    serve(server, (m) => {
      const reading = new Collector(m.bulk);
      void reading.finished.then(() => failed.resolve(reading.error));
    });
    void talk
      .send('PUT', empty, {
        bulk: (async function* () {
          yield pattern(10);
          throw SwitchboardError.of(StatusCode.dataLoss, 'disk');
        })(),
      })
      .catch(() => {});
    const error = expectStatus(await failed.promise, StatusCode.dataLoss);
    expect(error.status.reason).toBe('disk');
  });
});

describe('failed payloads', () => {
  test('a source producing another length than declared', async () => {
    const peers = await Peers.connect();
    const [talk, server] = await peers.open();
    const delivered: TalkMessage[] = [];
    serve(server, (m) => delivered.push(m));
    const error = await rejectsStatus(
      talk.request('PUT', empty, { bulk: [pattern(50)], bulkLength: 100 }),
      StatusCode.invalidArgument,
    );
    expect(error).toBeInstanceOf(TalkAbortError);
    expect(delivered, 'not delivered').toHaveLength(0);
  });

  test('a bulk channel carrying another length than its reference is a PROTOCOL_ERROR for the payload only', async () => {
    const peers = await Peers.connect({ serverOptions: { streamBulk: () => true } });
    const raw = peers.client.open(empty);
    const server = await accept(peers);
    const failed = new Deferred<unknown>();
    const requests: TalkMessage[] = [];
    serve(server, (m) => {
      requests.push(m);
      if (m.isBulk) {
        const reading = new Collector(m.bulk);
        void reading.finished.then(() => failed.resolve(reading.error));
      }
    });
    const bulk = raw.openAfter(bulkOpen(raw, 1));
    await raw.send(bulkMessage('PUT', 1, { requestId: 1, length: 100 }));
    await bulk.send(new Uint8Array(50));
    await bulk.close();
    expectStatus(await failed.promise, StatusCode.protocolError);
    // The message stands, the channel too.
    expect(requests).toHaveLength(1);
    expect(requests[0]?.canReply).toBe(true);
    void requests[0]?.reply(empty);
    expect(raw.canSend).toBe(true);
  });

  test('a payload over maxInlinePayload fails toward both sides', async () => {
    const peers = await Peers.connect({ serverOptions: { maxInlinePayload: 1000 } });
    const [talk, server] = await peers.open();
    const seen: TalkMessage[] = [];
    serve(server, (m) => {
      seen.push(m);
      void m.reply(bytes([Math.floor(m.payload.length / 100)]));
    });
    let error = await rejectsStatus(
      talk.request('PUT', empty, { bulk: [pattern(5000)], bulkLength: 5000 }),
      StatusCode.resourceExhausted,
    );
    expect(error).toBeInstanceOf(TalkAbortError);
    // Unknown length: the cap is found while reading.
    error = await rejectsStatus(
      talk.request('PUT', empty, { bulk: generated(5000, { piece: 100 }) }),
      StatusCode.resourceExhausted,
    );
    expect(error).toBeInstanceOf(TalkAbortError);
    expect(seen).toHaveLength(0);
    // Within the cap.
    const ok = await talk.request('PUT', empty, { bulk: [pattern(1000)] });
    expect([...ok.payload]).toEqual([10]);
    expect(seen).toHaveLength(1);
  });

  test('payloadBytes reassembles a streamed payload within a cap', async () => {
    const peers = await Peers.connect({ serverOptions: { streamBulk: () => true } });
    const [talk, server] = await peers.open();
    serve(server, async (m) => {
      try {
        const payload = await m.bytes({ maxLength: 60000 });
        expectBytes(m.payload, payload);
        void m.reply(bytes([Math.floor(payload.length / 1000)]));
      } catch (e) {
        if (!(e instanceof SwitchboardError)) {
          throw e;
        }
        void m.replyAbort(e.status);
      }
    });
    const ok = await talk.request('PUT', pattern(50000));
    expect([...ok.payload]).toEqual([50]);
    const error = await rejectsStatus(
      talk.request('PUT', pattern(70000)),
      StatusCode.resourceExhausted,
    );
    expect(error).toBeInstanceOf(TalkAbortError);
  });
});

describe('bulk channel rules', () => {
  test('more unclaimed bulk channels than allowed are refused', async () => {
    const peers = await Peers.connect();
    const raw = peers.client.open(empty);
    const server = await accept(peers);
    serve(server, () => {});
    const bulks: MuxChannel[] = [];
    for (let n = 1; n <= 17; n++) {
      bulks.push(raw.openAfter(bulkOpen(raw, n)));
    }
    const last = await (bulks[16] as MuxChannel).done;
    expect(last.known).toBe(StatusCode.resourceExhausted);
    await flush();
    const first = bulks[0] as MuxChannel;
    expect(first.state).toBe('open');
    // The parent closing cancels the rest.
    await raw.close();
    expect((await first.done).known).toBe(StatusCode.cancelled);
  });

  test('a bulk channel no message references in time is closed', async () => {
    const clock = new ManualClock();
    const peers = await Peers.connect({
      options: { clock },
      serverOptions: { bulkOpenTimeout: 50 },
    });
    const raw = peers.client.open(empty);
    const server = await accept(peers);
    serve(server, () => {});
    const bulk = raw.openAfter(bulkOpen(raw, 1));
    await bulk.send(new Uint8Array(1000));
    await clock.elapseAsync(50);
    expect((await bulk.done).known).toBe(StatusCode.cancelled);
    // A late reference to it is a protocol error.
    await raw.send(bulkMessage('PUT', 1));
    expect((await elapseUntil(clock, raw.done)).known).toBe(StatusCode.protocolError);
  });

  test('bulk payloads nobody reads yet do not pause the connection', async () => {
    // A small receive mark: 20 unread payloads, a window each, are well
    // beyond it.
    // Adapted: the fake mux has no receive high-water mark (nor
    // `isReceivePaused`); what it models is what the connection's channels
    // buffer unread, which a paused connection would be paused for.
    const peers = await Peers.connect({ serverOptions: { streamBulk: () => true } });
    const [talk, server] = await peers.open();
    const [other, otherServer] = await peers.open();
    serve(otherServer, (m) => m.reply(m.payload));
    const unread: TalkMessage[] = [];
    serve(server, (m) => unread.push(m));
    for (let i = 0; i < 20; i++) {
      void talk.send('PUT', pattern(200000)).catch(() => {});
    }
    await until(() => unread.length === 20);
    await flush(20);
    expect(peers.server.bufferedBytes).toBeLessThan(64 * 1024);
    const echo = await other.request('ECHO', bytes([5]), { timeout: 2000 });
    expect([...echo.payload]).toEqual([5]);
    // Read one of them whole: what waited is there.
    expectBytes(await collect((unread[0] as TalkMessage).bulk), pattern(200000));
  });

  test('a reference to a bulk number never opened is a channel protocol error', async () => {
    const clock = new ManualClock();
    const peers = await Peers.connect({
      options: { clock },
      serverOptions: { bulkOpenTimeout: 50 },
    });
    const raw = peers.client.open(empty);
    const server = await accept(peers);
    serve(server, () => {});
    await raw.send(bulkMessage('PUT', 5));
    await clock.elapseAsync(50);
    expect((await elapseUntil(clock, raw.done)).known).toBe(StatusCode.protocolError);
    expect((await server.done).known).toBe(StatusCode.protocolError);
  });

  test('a bulk number referenced twice is a channel protocol error', async () => {
    const peers = await Peers.connect({ serverOptions: { streamBulk: () => true } });
    const raw = peers.client.open(empty);
    const server = await accept(peers);
    const seen: TalkMessage[] = [];
    void server
      .serve((m) => {
        seen.push(m);
      })
      .catch(() => {});
    const bulk = raw.openAfter(bulkOpen(raw, 1));
    const frame = bulkMessage('PUT', 1);
    await raw.send(frame);
    await raw.send(frame);
    expect((await raw.done).known).toBe(StatusCode.protocolError);
    expect(seen).toHaveLength(1);
    expect((await bulk.done).known).toBe(StatusCode.cancelled);
  });

  test('a bulk channel for no open parent is refused FAILED_PRECONDITION', async () => {
    const peers = await Peers.connect();
    const stray = peers.client.open(new TalkBulkOpen(1000, 1).encode());
    expect((await stray.done).known).toBe(StatusCode.failedPrecondition);
  });

  test('a parent closing cancels its bulk channels in transfer', async () => {
    const peers = await Peers.connect({ serverOptions: { streamBulk: () => true } });
    const [talk, server] = await peers.open();
    serve(server, (m) => {
      // Read paused: nothing is taken.
      new Collector(m.bulk, { paused: true });
      void server.close(Status.of(StatusCode.aborted, 'gone'));
    });
    // Adapted: Dart adds 1000 bytes every millisecond while the source is
    // not paused; this source makes them when they are read.
    const source = new PulledSource(() => pattern(1000));
    void talk.send('PUT', empty, { bulk: source.stream }).catch(() => {});
    await source.cancelled.promise;
    expect((await talk.done).known).toBe(StatusCode.aborted);
  });

  test('a graceful close waits for the payloads being sent', async () => {
    const peers = await Peers.connect();
    const [talk, server] = await peers.open();
    const got = new Deferred<TalkMessage>();
    serve(server, (m) => got.resolve(m));
    void talk.send('PUT', pattern(300000));
    await talk.close();
    const m = await got.promise;
    expectBytes(m.payload, pattern(300000));
  });
});
