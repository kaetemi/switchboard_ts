/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Port of the Dart `test/talk_bulk_test.dart`, from the start of `main` to
// the end of the 'over memory' / 'over TCP' groups: the retention and
// reassembly budget tests, 'no deadlock', the cancellation tests,
// 'idle timeout', 'over memory', and the ordering tests after it. Talk bulk
// payloads (wiki page "Polyverse Switchboard Talk", section "Bulk
// payloads") over the fake mux pair; the rest of the Dart file is in
// `talk-bulk-rules.test.ts`.

import { describe, expect, test } from 'vitest';
import { bytesEqual } from '../../../src/core/bytes.ts';
import { defaultMuxOptions, type MuxChannel } from '../../../src/core/mux/contract.ts';
import { Name } from '../../../src/core/name.ts';
import { Status, StatusCode } from '../../../src/core/status.ts';
import {
  defaultTalkOptions,
  type TalkMessage,
  type TalkOptions,
} from '../../../src/core/talk/contract.ts';
import { TalkBulkOpen } from '../../../src/core/talk/talk-bulk.ts';
import { installBulkRouting } from '../../../src/core/talk/talk-bulk-routes.ts';
import { TalkAbortError, TalkChannel } from '../../../src/core/talk/talk-channel.ts';
import { forwardMessage } from '../../../src/core/talk/talk-forward.ts';
import { TalkFrame, TalkKind } from '../../../src/core/talk/talk-frame.ts';
import { IncomingMessage } from '../../../src/core/talk/talk-message.ts';
import { noop } from '../../../src/core/talk/talk-util.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';
import { ManualClock } from '../../../src/testing/manual-clock.ts';
import {
  collect,
  concat,
  generated,
  Peers,
  pattern,
  SourceController,
  until,
} from '../../support/talk/bulk-peers.ts';
import { fakeMuxPair } from '../../support/talk/fake-channel.ts';
import { bulkFrame } from '../../support/talk/frames.ts';
import {
  bytes,
  Collector,
  expectBytes,
  expectStatus,
  flush,
  Outcome,
  Queue,
  rawOf,
  rejectsStatus,
  serve,
  within,
} from '../../support/talk/helpers.ts';

const empty = new Uint8Array(0);

/** Lets what the fake mux and Talk have in flight settle: the Dart tests' short real-time waits. */
const settle = (): Promise<void> => flush(20);

test('bulk channels finished before their references are not retained', async () => {
  const peers = await Peers.connect();
  const [talk, server] = await peers.open();
  const messages = new Queue(server.messages);
  const raw = rawOf(talk);
  for (let number = 1; number <= 10; number++) {
    const bulk = raw.openAfter(new TalkBulkOpen(raw.id, number).encode());
    await bulk.send(pattern(100));
    await bulk.close();
    await flush();
    void raw.send(bulkFrame({ procedure: 'NOTE' }, number, 100));
    expectBytes((await messages.next()).payload, pattern(100));
  }
  expect(server._bulkIns.size).toBe(0);
  await messages.cancel();
});

test('a stream request does not retain completed bulk sends', async () => {
  const peers = await Peers.connect();
  const [talk, server] = await peers.open();
  const received = new Deferred<TalkMessage>();
  serve(server, (m) => received.resolve(m));
  const request = talk.streamRequest('LIST', empty);
  const items = new Queue(request.items);
  const message = await received.promise;
  for (let i = 0; i < 10; i++) {
    await message.replyItem(empty, { bulk: [pattern(100)] });
    expectBytes((await items.next()).payload, pattern(100));
  }
  expect((message as IncomingMessage)._bulkOuts.size).toBe(0);
  void message.reply(empty);
  await request.done;
  await items.cancel();
});

test('a peer cancel abandons a bulk request still waiting for dispatch', async () => {
  const peers = await Peers.connect();
  const [talk, server] = await peers.open();
  const seen: TalkMessage[] = [];
  serve(server, (m) => seen.push(m));
  const raw = rawOf(talk);
  void raw.send(bulkFrame({ procedure: 'PUT', requestId: 1 }, 1));
  void raw.send(
    new TalkFrame({
      kind: TalkKind.abort,
      requestId: 1,
      payload: Status.of(StatusCode.cancelled).encode(),
    }).encode(),
  );
  void talk.send('AFTER', bytes([7]));
  await flush();
  expect(seen).toHaveLength(1);
  expect([...(seen[0] as TalkMessage).payload]).toEqual([7]);
  const bulk = raw.openAfter(new TalkBulkOpen(raw.id, 1).encode());
  expect((await bulk.done).known).toBe(StatusCode.cancelled);
  expect(rawOf(server).heldBytes).toBe(0);
  expect(server.incomingRequestCount).toBe(0);
});

test('cancelling an ordered bulk answer before dispatch releases its lane', async () => {
  const peers = await Peers.connect();
  const [talk, server] = await peers.open();
  const seen: TalkMessage[] = [];
  serve(talk, (m) => seen.push(m));
  serve(server, (message) => {
    const raw = rawOf(server);
    void raw.send(bulkFrame({ responseId: message.requestId }, 1));
    void server.send('AFTER', bytes([7]));
  });
  const request = talk.startRequest('GET', empty, { ordered: true });
  await flush();
  expect(seen).toHaveLength(0);
  request.cancel();
  await flush();
  expect(seen).toHaveLength(1);
  expect([...(seen[0] as TalkMessage).payload]).toEqual([7]);
  expect(rawOf(talk).heldBytes).toBe(0);
  // Dispatch catches up after cancellation: no payload may be attached
  // to the abandoned answer, and the parent remains usable.
  const raw = rawOf(server);
  const bulk = raw.openAfter(new TalkBulkOpen(raw.id, 1).encode());
  expect((await bulk.done).known).toBe(StatusCode.cancelled);
  expect(talk.isOpen).toBe(true);
});

test('failed reassembly releases its partial memory budget', async () => {
  const peers = await Peers.connect({
    serverOptions: { maxInlinePayload: 1000, reassemblyBudget: 1000 },
  });
  const [talk, server] = await peers.open({ options: { bulkChunkSize: 100 } });
  serve(server, (message) => message.reply(empty));
  await rejectsStatus(
    talk.request('PUT', empty, { bulk: [pattern(1100)] }),
    StatusCode.resourceExhausted,
  );
  await talk.request('PUT', empty, { bulk: [pattern(1000)] });
});

test('a full reassembly budget holds the next payload back, its sender stalled at its window, until the listener takes a message', async () => {
  const peers = await Peers.connect({ serverOptions: { bulkThreshold: 100 } });
  const [talk, server] = await peers.open({ options: { reassemblyBudget: 100_000 } });
  serve(server, (message) => {
    void message.replyItem(pattern(100_000));
    void message.replyItem(pattern(100_000, 1));
    void message.reply(empty);
  });
  const stream = talk.streamRequest('LIST', empty);
  let ended = false;
  stream.done.then(
    () => {
      ended = true;
    },
    () => {
      ended = true;
    },
  );
  // The first item fills the budget and waits for a listener; the second
  // is not read, and its sender spends the window granted at its OPEN.
  await until(() => talk._waitingBytes > 0);
  await settle();
  const stalled = [...peers.server.channels].filter(
    (c) => c.isLocallyOpened && c.priority === 'bulk' && c.canSend && c.sendWindow === 0,
  );
  expect(stalled).toHaveLength(1);
  expect(talk._assemblyBytes).toBe(100_000);
  await settle();
  // No credit came meanwhile, and nothing failed.
  expect((stalled[0] as MuxChannel).sendWindow).toBe(0);
  expect(ended).toBe(false);
  const items = new Queue(stream.items);
  expectBytes((await items.next()).payload, pattern(100_000));
  expectBytes((await items.next()).payload, pattern(100_000, 1));
  await stream.done;
  expect(await items.hasNext()).toBe(false);
  expect(talk._assemblyBytes).toBe(0);
  expect(talk._waitingBytes).toBe(0);
});

test('the end of a stream releases the reassembly budget of its unread items', async () => {
  const peers = await Peers.connect({ serverOptions: { bulkThreshold: 100 } });
  const [talk, server] = await peers.open({ options: { reassemblyBudget: 1000 } });
  serve(server, (message) => {
    void message.replyItem(pattern(1000));
    void message.reply(empty);
  });
  // Nobody ever listens to the items of the first: they are the
  // application's once the stream ended, as their credit is.
  const first = talk.streamRequest('LIST', empty);
  await first.done;
  const second = talk.streamRequest('LIST', empty);
  await second.done;
  const secondItems = await new Queue(second.items).rest();
  expect(secondItems).toHaveLength(1);
  expectBytes((secondItems[0] as TalkMessage).payload, pattern(1000));
  const firstItems = await new Queue(first.items).rest();
  expect(firstItems).toHaveLength(1);
  expectBytes((firstItems[0] as TalkMessage).payload, pattern(1000));
});

test('an answer Talk takes itself has no share of the reassembly budget', async () => {
  const peers = await Peers.connect({ serverOptions: { bulkThreshold: 100 } });
  const [talk, server] = await peers.open({ options: { reassemblyBudget: 1000 } });
  // A paused listener holds a reassembled message: the budget is full.
  const held = new Collector(talk.messages, { paused: true });
  serve(server, (message) => message.reply(pattern(1000, 2)));
  void server.send('NOTE', pattern(1000));
  await until(() => talk._assemblyBytes === 1000);
  const answer = await talk.request('GET', empty);
  expectBytes(answer.payload, pattern(1000, 2));
  held.resume();
  await flush();
  expect(held.values).toHaveLength(1);
  expectBytes((held.values[0] as TalkMessage).payload, pattern(1000));
  await held.cancel();
});

test('streamed payloads waiting for a paused listener are admitted at what their window lets the peer send; the others wait', async () => {
  const peers = await Peers.connect({
    serverOptions: { reassemblyBudget: 200_000, streamBulk: () => true },
  });
  const [talk, server] = await peers.open();
  const seen = new Collector(server.messages, { paused: true });
  let answered = 0;
  const sent = Array.from({ length: 6 }, (_, i) =>
    talk.request('PUT', empty, { bulk: [pattern(100_000, i)], bulkLength: 100_000 }).finally(() => {
      answered++;
    }),
  );
  // A window (64 KiB) each: four are admitted (the budget overshoots by
  // the last one), two wait, and nothing fails.
  await until(() => server._waitingBytes > 0);
  await settle();
  const charged = server._assemblyBytes;
  expect(charged).toBeGreaterThanOrEqual(200_000);
  expect(charged).toBeLessThanOrEqual(200_000 + 65_536);
  expect(server._waitingBytes).toBeLessThanOrEqual(2 * 65_536);
  expect(answered).toBe(0);
  seen.resume();
  for (let i = 0; i < 6; i++) {
    await until(() => seen.values.length > i);
    const message = seen.values[i] as TalkMessage;
    expectBytes(await collect(message.bulk), pattern(100_000, i));
    void message.reply(empty);
  }
  for (const answer of sent) {
    expect(await answer).toBeInstanceOf(IncomingMessage);
  }
  expect(server._assemblyBytes).toBe(0);
  await seen.cancel();
});

test('a request over a full budget waits for it instead of failing, and is read once the listener takes a message', async () => {
  const peers = await Peers.connect({ serverOptions: { reassemblyBudget: 1000 } });
  const [talk, server] = await peers.open({ options: { bulkThreshold: 100 } });
  const subscription = new Collector(server.messages, {
    paused: true,
    onValue: (message) => message.reply(bytes([message.payload[1] as number])),
  });
  let answered = 0;
  const first = talk.request('PUT', pattern(1000)).finally(() => {
    answered++;
  });
  const second = talk.request('PUT', pattern(1000, 1)).finally(() => {
    answered++;
  });
  await until(() => server._waitingBytes === 1000);
  await settle();
  expect(answered).toBe(0);
  expect(server._assemblyBytes).toBe(1000);
  subscription.resume();
  expect([...(await first).payload]).toEqual([1]);
  expect([...(await second).payload]).toEqual([2]);
  expect(server._assemblyBytes).toBe(0);
  expect(server._waitingBytes).toBe(0);
  await subscription.cancel();
});

test('what waits for a full budget is bounded: beyond maxUnclaimedBulk payloads holding more than the budget, the payload fails RESOURCE_EXHAUSTED', async () => {
  const peers = await Peers.connect({
    serverOptions: { reassemblyBudget: 1000, maxUnclaimedBulk: 1 },
  });
  const [talk, server] = await peers.open({ options: { bulkThreshold: 100 } });
  const subscription = new Collector(server.messages, {
    paused: true,
    onValue: (message) => message.reply(empty),
  });
  const outcomes = Array.from({ length: 3 }, (_, i) =>
    talk.request('PUT', pattern(1000, i)).then(
      (m): unknown => m,
      (e: unknown) => e,
    ),
  );
  await until(() => server._waitingBytes === 1000);
  await settle();
  subscription.resume();
  expect(await outcomes[0]).toBeInstanceOf(IncomingMessage);
  expect(await outcomes[1]).toBeInstanceOf(IncomingMessage);
  const third = await outcomes[2];
  expect(third).toBeInstanceOf(TalkAbortError);
  expectStatus(third, StatusCode.resourceExhausted);
  await subscription.cancel();
});

test('a payload over the per-message cap fails at once, without waiting for a full budget', async () => {
  const peers = await Peers.connect({
    serverOptions: { maxInlinePayload: 2000, reassemblyBudget: 1000 },
  });
  const [talk, server] = await peers.open({ options: { bulkThreshold: 100 } });
  const subscription = new Collector(server.messages, {
    paused: true,
    onValue: (message) => message.reply(empty),
  });
  const first = talk.request('PUT', pattern(1000));
  await until(() => server._assemblyBytes === 1000);
  const error = await rejectsStatus(
    talk.request('PUT', pattern(5000)),
    StatusCode.resourceExhausted,
  );
  expect(error).toBeInstanceOf(TalkAbortError);
  subscription.resume();
  await first;
  await subscription.cancel();
});

describe('no deadlock', () => {
  test('payloads are admitted in the order their messages arrived, not in the order their bulk channels were dispatched', async () => {
    const peers = await Peers.connect({ serverOptions: { reassemblyBudget: 1000 } });
    const [talk, server] = await peers.open();
    const seen: string[] = [];
    serve(server, (m) => seen.push(`${m.procedureName} ${m.payload[0]}`));
    const raw = rawOf(talk);
    // ONE references the bulk channel opened second: when the first one
    // arrives, TWO must not take the budget ONE needs, since TWO is
    // delivered after ONE.
    for (const [name, number] of [
      ['ONE', 2],
      ['TWO', 1],
    ] as const) {
      void raw.send(bulkFrame({ procedure: name }, number, 1000));
    }
    for (const number of [1, 2]) {
      const bulk = raw.openAfter(new TalkBulkOpen(raw.id, number).encode());
      await bulk.send(pattern(1000, number));
      await bulk.close();
    }
    await until(() => seen.length === 2);
    expect(seen).toEqual(['ONE 2', 'TWO 1']);
    expect(server._assemblyBytes).toBe(0);
  });

  test('a payload larger than the whole budget is admitted alone and completes', async () => {
    const peers = await Peers.connect({ serverOptions: { reassemblyBudget: 1000 } });
    const [talk, server] = await peers.open();
    const subscription = new Collector(server.messages, {
      paused: true,
      onValue: (message) => message.reply(bytes([Math.floor(message.payload.length / 1000)])),
    });
    const first = talk.request('PUT', pattern(200_000));
    const second = talk.request('PUT', pattern(100_000, 1));
    await until(() => server._assemblyBytes === 200_000 && server._waitingBytes > 0);
    subscription.resume();
    expect([...(await first).payload]).toEqual([200]);
    expect([...(await second).payload]).toEqual([100]);
    await subscription.cancel();
  });
});

test('cancelling a stream request closes a delivered item bulk channel without relying on the peer', async () => {
  const peers = await Peers.connect();
  const [talk, server] = await peers.open({ options: { streamBulk: () => true } });
  const opened = new Deferred<MuxChannel>();
  serve(server, (message) => {
    const raw = rawOf(server);
    // A peer that keeps sending the item after the request was cancelled.
    opened.resolve(raw.openAfter(new TalkBulkOpen(raw.id, 1).encode()));
    void raw.send(bulkFrame({ kind: TalkKind.streamItem, responseId: message.requestId }, 1));
  });
  const request = talk.streamRequest('LIST', empty);
  const items = new Queue(request.items);
  expect((await items.next()).isBulk).toBe(true);
  request.cancel();
  const bulk = await opened.promise;
  expect((await within(bulk.done)).known).toBe(StatusCode.cancelled);
  await items.cancel();
});

test('cancelling messages releases an ordered answer behind a bulk message being reassembled', async () => {
  const peers = await Peers.connect();
  const [talk, server] = await peers.open();
  const source = new SourceController();
  serve(server, (message) => {
    void server.send('NOTE', empty, { bulk: source.stream });
    void message.reply(bytes([7]));
  });
  const delivered: TalkMessage[] = [];
  const subscription = new Collector(talk.messages, { onValue: (m) => delivered.push(m) });
  const response = talk.request('GET', empty, { ordered: true });
  await flush();
  await subscription.cancel();
  expect([...(await within(response)).payload]).toEqual([7]);
  await flush();
  expect(source.hasListener).toBe(false);
  expect(rawOf(talk).heldBytes).toBe(0);
  expect(delivered).toHaveLength(0);
  source.close();
});

for (const cancelBefore of [false, true]) {
  test(`cancelling messages ${cancelBefore ? 'before' : 'during'} bulk reassembly refuses the unread request`, async () => {
    const peers = await Peers.connect();
    const [talk, server] = await peers.open();
    const delivered: TalkMessage[] = [];
    const subscription = new Collector(server.messages, { onValue: (m) => delivered.push(m) });
    if (cancelBefore) {
      await subscription.cancel();
    }
    const source = new SourceController();
    const response = talk.request('PUT', empty, { bulk: source.stream });
    await flush();
    if (!cancelBefore) {
      await subscription.cancel();
    }
    await rejectsStatus(within(response), StatusCode.unimplemented);
    await flush();
    expect(source.hasListener).toBe(false);
    expect(rawOf(server).heldBytes).toBe(0);
    expect(delivered).toHaveLength(0);
    source.close();
  });
}

test('references without bulk OPENs have a bounded dispatch wait', async () => {
  const clock = new ManualClock();
  const [client, server] = fakeMuxPair({ mux: { clock } });
  const talks: TalkChannel[] = [];
  void server.accept().then((raw) => talks.push(new TalkChannel(raw)));
  const raw = client.open(empty);
  await clock.elapseAsync(0);
  const baseline = clock.pendingCount;
  for (let number = 1; number <= 100; number++) {
    // Unknown: consumed by Talk immediately.
    void raw.send(bulkFrame({ responseId: 1 }, number));
  }
  await clock.elapseAsync(0);
  expect(clock.pendingCount - baseline).toBeLessThanOrEqual(defaultTalkOptions.maxUnclaimedBulk);
  expect(talks).toHaveLength(1);
  let end: Status | undefined;
  void (talks[0] as TalkChannel).done.then((status) => {
    end = status;
  });
  await clock.elapseAsync(0);
  expect(end?.known).toBe(StatusCode.protocolError);
  expect(clock.pendingCount).toBe(0);
  await client.close();
  await server.close();
  await clock.elapseAsync(defaultMuxOptions.keepAliveTimeout);
});

test("a request cancelled while it waits for the peer's LIMITS does not start its upload", async () => {
  const clock = new ManualClock();
  const [client, server] = fakeMuxPair({ mux: { clock }, awaitLimits: true });
  // A peer that takes the channels but never reads nor closes them.
  const taken: MuxChannel[] = [];
  void (async () => {
    for await (const channel of server.incoming) {
      taken.push(channel);
    }
  })();
  const talk = new TalkChannel(client.open(empty));
  const source = new SourceController();
  const request = talk.startRequest('PUT', empty, { bulk: source.stream });
  request.cancel();
  // The LIMITS arrive: the request, its bulk channel and the cancel go
  // out, in that order.
  client.announceLimits();
  await clock.elapseAsync(0);
  expect(taken).toHaveLength(2);
  expect(source.hasListener).toBe(false);
  source.close();
  await client.close();
  await server.close();
  await clock.elapseAsync(defaultMuxOptions.keepAliveTimeout);
});

describe('idle timeout', () => {
  /**
   * A Talk channel and its far end over a fake mux pair, in fake time, the
   * bulk channels of both sides routed; then a function closing both.
   */
  async function fakeTalk(
    clock: ManualClock,
    init: { near?: Partial<TalkOptions>; far?: Partial<TalkOptions> } = {},
  ): Promise<[TalkChannel, TalkChannel, () => Promise<void>]> {
    const [client, server] = fakeMuxPair({ mux: { clock } });
    installBulkRouting(client);
    installBulkRouting(server);
    const talk = new TalkChannel(client.open(empty), init.near);
    const accepted = new TalkChannel(await server.accept(), init.far);
    await clock.elapseAsync(0);
    return [
      talk,
      accepted,
      async () => {
        await client.close();
        await server.close();
        await clock.elapseAsync(defaultMuxOptions.keepAliveTimeout);
      },
    ];
  }

  /** Expects [error] to be this side's DEADLINE_EXCEEDED. */
  const deadlineExceeded = (error: unknown): void => {
    expectStatus(error, StatusCode.deadlineExceeded);
  };

  test('a request whose bulk payload stalls is answered DEADLINE_EXCEEDED once no byte came for the timeout', async () => {
    const clock = new ManualClock();
    const [talk, server, close] = await fakeTalk(clock, { near: { bulkChunkSize: 500 } });
    const delivered: TalkMessage[] = [];
    serve(server, (m) => delivered.push(m));
    const source = new SourceController();
    const outcome = new Outcome(talk.request('PUT', empty, { bulk: source.stream, timeout: 0 }));
    source.add(pattern(500));
    await clock.elapseAsync(20_000);
    // A chunk restarts the timeout.
    source.add(pattern(500));
    await clock.elapseAsync(29_000);
    expect(outcome.isDone).toBe(false);
    await clock.elapseAsync(2000);
    expect(outcome.error).toBeInstanceOf(TalkAbortError);
    deadlineExceeded(outcome.error);
    // The sender's transfer ended with its bulk channel.
    expect(source.hasListener).toBe(false);
    expect(server.incomingRequestCount).toBe(0);
    expect(delivered).toHaveLength(0);
    source.close();
    await close();
  });

  test('a final response whose payload stalls fails its request', async () => {
    const clock = new ManualClock();
    const [talk, server, close] = await fakeTalk(clock);
    const source = new SourceController();
    const sent: Promise<void>[] = [];
    serve(server, (m) => {
      sent.push(m.reply(empty, { bulk: source.stream }));
    });
    const outcome = new Outcome(talk.request('GET', empty));
    await clock.elapseAsync(29_000);
    expect(outcome.isDone).toBe(false);
    await clock.elapseAsync(2000);
    // This side's own failure, not an abort from the peer.
    deadlineExceeded(outcome.error);
    expect(outcome.error).not.toBeInstanceOf(TalkAbortError);
    expect(sent).toHaveLength(1);
    const sendOutcome = new Outcome(sent[0] as Promise<void>);
    await clock.elapseAsync(0);
    deadlineExceeded(sendOutcome.error);
    expect(source.hasListener).toBe(false);
    source.close();
    await close();
  });

  test('an item whose payload stalls ends its stream', async () => {
    const clock = new ManualClock();
    // The responder's own timeout would end the request first.
    const [talk, server, close] = await fakeTalk(clock, { far: { replyTimeout: 0 } });
    const source = new SourceController();
    serve(server, (m) => {
      m.replyItem(empty, { bulk: source.stream }).catch(noop);
    });
    const stream = talk.streamRequest('LIST', empty, { timeout: 0 });
    const delivered: TalkMessage[] = [];
    const items = new Collector(stream.items, { onValue: (m) => delivered.push(m) });
    const outcome = new Outcome(stream.done);
    await clock.elapseAsync(29_000);
    expect(outcome.isDone).toBe(false);
    await clock.elapseAsync(2000);
    deadlineExceeded(outcome.error);
    expect(items.isDone).toBe(true);
    deadlineExceeded(items.error);
    expect(delivered).toHaveLength(0);
    expect(source.hasListener).toBe(false);
    source.close();
    await close();
  });

  test('a plain message whose payload stalls is dropped', async () => {
    const clock = new ManualClock();
    const [talk, server, close] = await fakeTalk(clock);
    const seen = new Collector(talk.messages);
    const source = new SourceController();
    void server.send('NOTE', empty, { bulk: source.stream });
    void server.send('AFTER', bytes([7]));
    await clock.elapseAsync(29_000);
    expect(seen.values).toHaveLength(0);
    await clock.elapseAsync(2000);
    expect(seen.values.map((m) => m.procedureName)).toEqual(['AFTER']);
    expect(source.hasListener).toBe(false);
    source.close();
    await close();
  });

  test('a streamed payload the application reads errors', async () => {
    const clock = new ManualClock();
    const [talk, server, close] = await fakeTalk(clock, { near: { streamBulk: () => true } });
    const readers: Collector<Uint8Array>[] = [];
    serve(talk, (m) => {
      readers.push(new Collector(m.bulk));
    });
    const source = new SourceController();
    void server.send('NOTE', empty, { bulk: source.stream });
    source.add(pattern(100));
    await clock.elapseAsync(29_000);
    expect(readers).toHaveLength(1);
    const reader = readers[0] as Collector<Uint8Array>;
    expect(reader.isDone).toBe(false);
    await clock.elapseAsync(2000);
    expect(reader.isDone).toBe(true);
    deadlineExceeded(reader.error);
    source.close();
    await close();
  });

  test('a payload waiting for its reader here does not time out', async () => {
    const clock = new ManualClock();
    const [talk, server, close] = await fakeTalk(clock, { near: { streamBulk: () => true } });
    const readers: Collector<Uint8Array>[] = [];
    serve(talk, (m) => {
      // Not pulled while paused: the reader does not take the payload.
      readers.push(new Collector(m.bulk, { paused: true }));
    });
    void server.send('NOTE', empty, { bulk: [pattern(200_000)], bulkLength: 200_000 });
    // The window is used up: the sender waits for this side.
    await clock.elapseAsync(120_000);
    expect(readers).toHaveLength(1);
    const reading = readers[0] as Collector<Uint8Array>;
    expect(reading.isDone).toBe(false);
    reading.resume();
    await clock.elapseAsync(0);
    expect(reading.isDone).toBe(true);
    expect(reading.error).toBeUndefined();
    expectBytes(concat(reading.values), pattern(200_000));
    // Nothing is in transfer: no timer is left.
    expect(clock.pendingCount).toBe(0);
    await reading.cancel();
    await close();
  });

  test('a payload waiting for the reassembly budget does not time out; once admitted, the timeout counts from then', async () => {
    const clock = new ManualClock();
    const [talk, server, close] = await fakeTalk(clock, {
      near: { reassemblyBudget: 1000 },
      far: { bulkThreshold: 100 },
    });
    const seen = new Collector(talk.messages, { paused: true });
    // Fills the budget, held by the paused listener.
    void server.send('FULL', pattern(1000));
    await clock.elapseAsync(0);
    // Waits for the budget, its sender silent meanwhile.
    const source = new SourceController();
    void server.send('NEXT', empty, { bulk: source.stream });
    await clock.elapseAsync(120_000);
    expect(seen.values).toHaveLength(0);
    seen.resume();
    await clock.elapseAsync(0);
    expect(seen.values.map((m) => m.procedureName)).toEqual(['FULL']);
    await clock.elapseAsync(29_000);
    source.add(pattern(500));
    source.close();
    await clock.elapseAsync(0);
    expect(seen.values.map((m) => m.procedureName)).toEqual(['FULL', 'NEXT']);
    expectBytes((seen.values[1] as TalkMessage).payload, pattern(500));
    await seen.cancel();
    await close();
  });

  test('a forwarded payload waiting for a paused reader is neither cut nor timed out', async () => {
    const clock = new ManualClock();
    const piping: Partial<TalkOptions> = { streamBulk: () => true };
    const [client, front, closeFront] = await fakeTalk(clock, { near: piping, far: piping });
    const [back, backend, closeBack] = await fakeTalk(clock, { near: piping });
    serve(front, (m) => {
      forwardMessage(m, back).catch(noop);
    });
    serve(backend, (m) => {
      m.reply(empty, { bulk: [pattern(300_000)], bulkLength: 300_000 }).catch(noop);
    });
    const outcome = new Outcome(client.request('GET', empty));
    await clock.elapseAsync(0);
    const response = outcome.value as TalkMessage;
    expect(response).toBeDefined();
    const reading = new Collector(response.bulk, { paused: true });
    // Every hop waits for the client, which does not read: longer than the
    // idle timeout, and than the close confirmation timeout of the
    // forwarded bulk channel, whose source ended meanwhile.
    await clock.elapseAsync(120_000);
    expect(reading.isDone).toBe(false);
    reading.resume();
    await clock.elapseAsync(0);
    expect(reading.isDone).toBe(true);
    expect(reading.error).toBeUndefined();
    expectBytes(concat(reading.values), pattern(300_000));
    await reading.cancel();
    await closeFront();
    await closeBack();
  });

  test('an unclaimed bulk channel goes idle too', async () => {
    const clock = new ManualClock();
    const [talk, server, close] = await fakeTalk(clock, { near: { bulkOpenTimeout: 60_000 } });
    const seen = new Collector(talk.messages);
    const raw = rawOf(server);
    const bulk = raw.openAfter(new TalkBulkOpen(raw.id, 1).encode());
    let end: Status | undefined;
    void bulk.done.then((status) => {
      end = status;
    });
    await clock.elapseAsync(29_000);
    expect(end).toBeUndefined();
    await clock.elapseAsync(2000);
    expect(end?.known).toBe(StatusCode.deadlineExceeded);
    // Its message fails when it comes: refused, the next one delivered.
    void raw.send(bulkFrame({ procedure: 'PUT', requestId: 1 }, 1));
    void raw.send(
      new TalkFrame({
        kind: TalkKind.message,
        procedure: Name.of('AFTER'),
        payload: empty,
      }).encode(),
    );
    await clock.elapseAsync(0);
    expect(seen.values.map((m) => m.procedureName)).toEqual(['AFTER']);
    expect(talk.incomingRequestCount).toBe(0);
    await close();
  });

  test('Duration.zero sets no idle timeout', async () => {
    const clock = new ManualClock();
    const [talk, server, close] = await fakeTalk(clock, {
      near: { bulkChunkSize: 500 },
      far: { bulkIdleTimeout: 0 },
    });
    serve(server, (m) => m.reply(m.payload.subarray(0, 1)));
    const source = new SourceController();
    const outcome = new Outcome(talk.request('PUT', empty, { bulk: source.stream, timeout: 0 }));
    source.add(pattern(500));
    await clock.elapseAsync(600_000);
    expect(outcome.isDone).toBe(false);
    source.add(pattern(500, 500));
    source.close();
    await clock.elapseAsync(0);
    expect([...((outcome.value as TalkMessage | undefined)?.payload ?? [])]).toEqual([0]);
    expect(clock.pendingCount).toBe(0);
    await close();
  });
});

describe('over memory', () => {
  test('a request with a bulk payload of known length', async () => {
    const peers = await Peers.connect();
    const [talk, server] = await peers.open();
    const checks: unknown[] = [];
    serve(server, (m) => {
      checks.push(m.isBulk, m.bulkLength);
      void m.reply(bytes([Math.floor(m.payload.length / 10_000)]));
      checks.push(bytesEqual(m.payload, pattern(300_000)));
    });
    const answer = await talk.request('PUT', empty, {
      bulk: generated(300_000),
      bulkLength: 300_000,
    });
    expect([...answer.payload]).toEqual([30]);
    expect(answer.isBulk).toBe(false);
    expect(checks).toEqual([true, 300_000, true]);
  });

  test('a request with a bulk payload of unknown length', async () => {
    const peers = await Peers.connect();
    const [talk, server] = await peers.open();
    const checks: unknown[] = [];
    serve(server, (m) => {
      checks.push(m.bulkLength);
      void m.reply(m.payload.subarray(0, 3));
      checks.push(bytesEqual(m.payload, pattern(123_457)));
    });
    const answer = await talk.request('PUT', empty, {
      bulk: generated(123_457, { piece: 777 }),
    });
    expect([...answer.payload]).toEqual([0, 1, 2]);
    expect(checks).toEqual([undefined, true]);
  });

  test('a response, stream items and a plain message with bulk payloads', async () => {
    const peers = await Peers.connect();
    const [talk, server] = await peers.open({
      options: { streamBulk: (m) => m.frame.kind === TalkKind.streamItem },
    });
    const plain = new Deferred<TalkMessage>();
    serve(server, async (m) => {
      switch (m.procedureName) {
        case 'GET':
          await m.reply(empty, { bulk: generated(200_000), bulkLength: 200_000 });
          break;
        case 'LIST':
          for (let i = 0; i < 3; i++) {
            void m.replyItem(empty, { bulk: generated(70_000 + i) });
          }
          void m.reply(bytes([9]));
          break;
        case 'NOTE':
          plain.resolve(m);
          break;
      }
    });
    const got = await talk.request('GET', empty);
    expect(got.isBulk).toBe(true);
    expect(got.bulkLength).toBe(200_000);
    expectBytes(got.payload, pattern(200_000));
    const stream = talk.streamRequest('LIST', empty);
    const lengths: number[] = [];
    for await (const item of stream.items) {
      expect(item.isBulk).toBe(true);
      // Streamed: payload is not there before it is read.
      expect(() => item.payload).toThrow(Error);
      const received = await collect(item.bulk);
      expectBytes(received, pattern(received.length));
      lengths.push(received.length);
    }
    expect(lengths).toEqual([70_000, 70_001, 70_002]);
    expect([...(await stream.done).payload]).toEqual([9]);
    void talk.send('NOTE', empty, { bulk: [pattern(50_000)] });
    const note = await plain.promise;
    expect(note.isBulk).toBe(true);
    expectBytes(note.payload, pattern(50_000));
  });

  test('a payload too large for a frame goes bulk on its own', async () => {
    const peers = await Peers.connect();
    const [talk, server] = await peers.open();
    const seen: boolean[] = [];
    serve(server, (m) => {
      seen.push(m.isBulk);
      void m.reply(Uint8Array.from(m.payload).reverse());
    });
    const small = bytes([1, 2, 3]);
    const big = pattern(100_000);
    const limit = rawOf(talk).maxSubframeLength;
    // Just inline: the frame header (1 + 8 + 3 bytes) and the payload.
    const edge = pattern(limit - 12);
    const answers: TalkMessage[] = [];
    for (const payload of [small, big, edge, pattern(limit - 11)]) {
      answers.push(await talk.request('ECHO', payload));
    }
    const [smallAnswer, bigAnswer, edgeAnswer, overAnswer] = answers as [
      TalkMessage,
      TalkMessage,
      TalkMessage,
      TalkMessage,
    ];
    expect(seen).toEqual([false, true, false, true]);
    expect([...smallAnswer.payload]).toEqual([3, 2, 1]);
    expect(bigAnswer.isBulk).toBe(true);
    expectBytes(bigAnswer.payload, Uint8Array.from(big).reverse());
    expect(edgeAnswer.isBulk).toBe(false);
    // The answer's header is 8 bytes shorter (no procedure, a response id
    // instead of a request id): inline.
    expect(overAnswer.isBulk).toBe(false);
    expect(overAnswer.payload).toHaveLength(limit - 11);
  });
});

// Not ported: 'over TCP': this thread has no mux over TCP yet; the
// integration tests over the real mux and transports cover it once the mux
// lands. (The group runs the four tests of 'over memory' over loopback TCP.)

test('a send with a bulk payload completes once the payload went whole, and fails with how its transfer ended', async () => {
  const peers = await Peers.connect({
    serverOptions: { streamBulk: (m) => m.procedureName === 'DROP' },
  });
  const [talk, server] = await peers.open();
  const seen: TalkMessage[] = [];
  serve(server, (m) => {
    seen.push(m);
    if (m.procedureName === 'DROP') {
      // The receiver does not want it: the transfer is cancelled.
      m.bulk.cancel().catch(noop);
    }
  });
  const source = new SourceController();
  let sent = false;
  const put = talk.send('PUT', empty, { bulk: source.stream }).then(() => {
    sent = true;
  });
  source.add(pattern(1000));
  await flush();
  // Its frame went, a chunk too, but the payload is not whole yet.
  expect(sent).toBe(false);
  source.close();
  await put;
  await until(() => seen.length === 1);
  expectBytes((seen[0] as TalkMessage).payload, pattern(1000));
  // A payload too large for a frame goes bulk: the same.
  await talk.send('BIG', pattern(100_000));
  await until(() => seen.length === 2);
  expectBytes((seen[1] as TalkMessage).payload, pattern(100_000));
  const dropped = new SourceController();
  const drop = talk.send('DROP', empty, { bulk: dropped.stream });
  dropped.add(pattern(1000));
  await rejectsStatus(drop, StatusCode.cancelled);
  dropped.close();
});

test('a message keeps its place while its payload is reassembled', async () => {
  const peers = await Peers.connect();
  const [talk, server] = await peers.open();
  const seen: string[] = [];
  const all = new Deferred<void>();
  serve(server, (m) => {
    seen.push(`${m.procedureName} ${m.payload.length}`);
    if (seen.length === 4) {
      all.resolve();
    }
  });
  void talk.send('A', new Uint8Array(1));
  void talk.send('B', empty, { bulk: generated(500_000, { piece: 1000 }) });
  void talk.send('C', new Uint8Array(2));
  void talk.send('D', pattern(100_000));
  await all.promise;
  expect(seen).toEqual(['A 1', 'B 500000', 'C 2', 'D 100000']);
});

test('an ordered answer with a bulk payload keeps its place among the messages', async () => {
  const peers = await Peers.connect();
  const [talk, server] = await peers.open();
  serve(server, (m) => {
    void server.send('EVENT', bytes([1]));
    void m.reply(pattern(200_000));
    void server.send('EVENT', bytes([2]));
  });
  const log: string[] = [];
  serve(talk, (m) => log.push(`event ${m.payload[0]}`));
  const answer = await talk.request('SUB', empty, { ordered: true });
  log.push(`answer ${answer.payload.length}`);
  await settle();
  expect(log).toEqual(['event 1', 'answer 200000', 'event 2']);
});
