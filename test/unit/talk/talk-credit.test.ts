/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Port of the Dart `test/talk_credit_test.dart`: Talk returns the credit
// of a frame when the application has taken what it carried (wiki "Mux",
// "Flow control"; "Talk", the delivery point). Runs over the fake mux pair,
// which charges and returns credit as the frame mux does.

import { afterEach, beforeEach, describe, expect, test } from 'vitest';
import { defaultMuxOptions } from '../../../src/core/mux/contract.ts';
import { Name } from '../../../src/core/name.ts';
import { Status, StatusCode } from '../../../src/core/status.ts';
import { TalkAbortError, TalkChannel } from '../../../src/core/talk/talk-channel.ts';
import { TalkFrame, TalkKind } from '../../../src/core/talk/talk-frame.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';
import {
  type FakeMuxChannel,
  type FakeMuxConnection,
  fakeMuxPair,
  costOf as muxCost,
} from '../../support/talk/fake-channel.ts';
import { FakeClock, flush } from '../../support/talk/fake-clock.ts';
import { frameLink } from '../../support/talk/frame-link.ts';
import { Collector, rejectsStatus, serve, uncaughtErrors } from '../../support/talk/helpers.ts';

const empty = new Uint8Array(0);
const defaultInitialWindow = defaultMuxOptions.initialWindow;

/** Lets the mux and Talk deliver what is in flight. */
const settle = (): Promise<void> => flush(20);

let client: FakeMuxConnection;
let server: FakeMuxConnection;

/**
 * A client and a server Talk channel over one mux connection, with both
 * raw mux channels for inspection.
 */
async function talkPair(): Promise<[TalkChannel, TalkChannel, FakeMuxChannel, FakeMuxChannel]> {
  const raw = client.open(empty);
  const talk = new TalkChannel(raw);
  const serverRaw = await server.accept();
  return [talk, new TalkChannel(serverRaw), raw, serverRaw];
}

beforeEach(() => {
  [client, server] = fakeMuxPair();
});

afterEach(async () => {
  await client.close();
  await server.close();
});

test('a Talk channel puts its mux channel in manual credit', async () => {
  const [talk, serverTalk, raw, serverRaw] = await talkPair();
  expect(raw.manualCredit).toBe(true);
  expect(serverRaw.manualCredit).toBe(true);
  await talk.close();
  await serverTalk.done;
});

test('a paused messages stream stalls its channel at the window, and only it; resuming drains', async () => {
  const [talk, serverTalk, raw, serverRaw] = await talkPair();
  const other = await talkPair();
  const received = new Collector(serverTalk.messages, { paused: true });
  serve(other[1], (m) => m.reply(m.payload));
  const count = 200;
  const payload = new Uint8Array(1000);
  for (let i = 0; i < count; i++) {
    void talk.send('MSG', payload);
  }
  await settle();
  // The window is spent and nothing more arrives: what the server holds
  // is bounded by it.
  expect(received.values).toHaveLength(0);
  expect(raw.sendWindow).toBeLessThan(1100);
  expect(serverRaw.heldBytes + serverRaw.bufferedBytes).toBeLessThanOrEqual(defaultInitialWindow);
  expect(serverRaw.heldBytes).toBeGreaterThan(60000);
  // Another channel of the connection flows meanwhile.
  const echo = await other[0].request('ECHO', Uint8Array.of(7));
  expect([...echo.payload]).toEqual([7]);
  received.resume();
  await settle();
  expect(received.values).toHaveLength(count);
  expect(serverRaw.heldBytes).toBe(0);
  await received.cancel();
  await talk.close();
});

test("a paused messages stream holds responses to its own channel's requests behind the window too", async () => {
  const [talk, serverTalk, raw] = await talkPair();
  // The server answers requests; the client never listens to messages,
  // and the server sends it plain messages until the window is spent.
  serve(serverTalk, (m) => m.reply(empty));
  for (let i = 0; i < 100; i++) {
    void serverTalk.send('EVENT', new Uint8Array(1000));
  }
  await settle();
  const answer = talk.request('REQ', empty);
  let answered = false;
  void answer.then(() => {
    answered = true;
  });
  await settle();
  expect(answered).toBe(false);
  const events = new Collector(talk.messages);
  await answer;
  await settle();
  expect(events.values).toHaveLength(100);
  expect(raw.heldBytes).toBe(0);
  await events.cancel();
  await talk.close();
});

test('frames Talk consumes itself return their credit at once', async () => {
  const [talk, serverTalk, raw] = await talkPair();
  serve(serverTalk, (m) => {
    switch (m.procedureName) {
      case 'BIG':
        void m.reply(new Uint8Array(20000));
        break;
      case 'EXT':
        void m.extend();
        void m.extend();
        void m.reply(empty);
        break;
      case 'FAIL':
        void m.replyAbort(Status.of(StatusCode.notFound, 'x'.repeat(1000)));
        break;
    }
  });
  // Nobody listens to the client's messages: responses, EXTENDs and
  // aborts never wait for a listener.
  for (let i = 0; i < 20; i++) {
    expect((await talk.request('BIG', empty)).payload).toHaveLength(20000);
    await talk.request('EXT', empty);
    await expect(talk.request('FAIL', empty)).rejects.toBeInstanceOf(TalkAbortError);
  }
  await settle();
  expect(raw.heldBytes).toBe(0);
  await talk.close();
});

test('a response for an unknown or cancelled request returns its credit', async () => {
  const [talk, serverTalk, raw, serverRaw] = await talkPair();
  const requests = new Collector(serverTalk.messages);
  const pending = talk.startRequest('SLOW', empty);
  await settle();
  pending.cancel();
  await settle();
  // The server's late reply is dropped by the client's Talk, and the
  // server answered the cancel already; a stray response to an id never
  // used is dropped too.
  expect(requests.values).toHaveLength(1);
  expect(requests.values[0]?.isCancelled).toBe(true);
  void serverRaw.send(
    new TalkFrame({
      kind: TalkKind.message,
      responseId: 77,
      payload: new Uint8Array(5000),
    }).encode(),
  );
  await settle();
  expect(raw.heldBytes).toBe(0);
  await requests.cancel();
  await talk.close();
});

test('cancelling the messages subscription returns what it held', async () => {
  const [talk, serverTalk, , serverRaw] = await talkPair();
  const subscription = new Collector(serverTalk.messages, { paused: true });
  for (let i = 0; i < 30; i++) {
    void talk.send('MSG', new Uint8Array(1000));
  }
  await settle();
  expect(serverRaw.heldBytes).toBeGreaterThan(30000);
  await subscription.cancel();
  expect(serverRaw.heldBytes).toBe(0);
  // Later messages are dropped, their credit returned at once.
  for (let i = 0; i < 100; i++) {
    void talk.send('MSG', new Uint8Array(1000));
  }
  await settle();
  expect(serverRaw.heldBytes).toBe(0);
  await talk.close();
});

test('stream items are flow controlled by the items subscription', async () => {
  const [talk, serverTalk, raw] = await talkPair();
  serve(serverTalk, (m) => {
    for (let i = 0; i < 100; i++) {
      void m.replyItem(new Uint8Array(1000));
    }
    void m.reply(empty);
  });
  const stream = talk.streamRequest('LIST', empty);
  const items = new Collector(stream.items, { paused: true });
  await settle();
  expect(items.values).toHaveLength(0);
  expect(raw.heldBytes).toBeGreaterThan(60000);
  expect(raw.heldBytes).toBeLessThanOrEqual(defaultInitialWindow);
  items.resume();
  await stream.done;
  await settle();
  expect(items.values).toHaveLength(100);
  expect(raw.heldBytes).toBe(0);
  await items.cancel();
  await talk.close();
});

test('items nobody listens to stall the channel until their request ends, not longer', async () => {
  const clock = new FakeClock();
  [client, server] = fakeMuxPair({ mux: { clock } });
  const raw = client.open(empty);
  const talk = new TalkChannel(raw, { requestTimeout: 200 });
  const serverTalk = new TalkChannel(await server.accept());
  serve(serverTalk, (m) => {
    if (m.procedureName === 'LIST') {
      for (let i = 0; i < 100; i++) {
        void m.replyItem(new Uint8Array(1000));
      }
      void m.reply(empty);
    } else {
      void m.reply(Uint8Array.of(1));
    }
  });
  // Only `done` is awaited: the items fill the window, the final cannot
  // follow, and the request times out.
  const stream = talk.streamRequest('LIST', empty);
  const done = rejectsStatus(stream.done, StatusCode.deadlineExceeded);
  await clock.elapse(200);
  await done;
  // Its end returned the items' credit: the channel flows again.
  expect(raw.heldBytes).toBe(0);
  const answer = talk.request('PING', empty);
  await clock.elapse(100);
  expect([...(await answer).payload]).toEqual([1]);
  await talk.close();
});

test("cancelling a stream's items returns what they held", async () => {
  const [talk, serverTalk, raw] = await talkPair();
  serve(serverTalk, (m) => {
    for (let i = 0; i < 20; i++) {
      void m.replyItem(new Uint8Array(1000));
    }
  });
  const stream = talk.streamRequest('LIST', empty);
  void stream.done.catch(() => {});
  const items = new Collector(stream.items, { paused: true });
  await settle();
  expect(raw.heldBytes).toBeGreaterThan(20000);
  await items.cancel();
  expect(raw.heldBytes).toBe(0);
  await talk.close();
});

test('an ordered answer returns its credit when it is delivered', async () => {
  const [talk, serverTalk, raw] = await talkPair();
  serve(serverTalk, (m) => {
    void serverTalk.send('EVENT', new Uint8Array(100));
    void m.reply(new Uint8Array(3000));
  });
  const events = new Collector(talk.messages, { paused: true });
  const answer = talk.request('SUB', empty, { ordered: true });
  let answered = false;
  void answer.then(() => {
    answered = true;
  });
  await settle();
  expect(answered).toBe(false);
  expect(raw.heldBytes).toBeGreaterThan(3000);
  events.resume();
  await answer;
  await settle();
  expect(events.values).toHaveLength(1);
  expect(raw.heldBytes).toBe(0);
  await events.cancel();
  await talk.close();
});

test('over a raw stream channel nothing is flow controlled', async () => {
  const [a, b] = frameLink();
  const left = new TalkChannel(a);
  const right = new TalkChannel(b);
  serve(right, (m) => m.reply(m.payload));
  const answer = await left.request('ECHO', Uint8Array.of(1, 2));
  expect([...answer.payload]).toEqual([1, 2]);
  await left.close();
  await right.close();
});

describe('awaitable sends', () => {
  /** The flow-control cost of a plain message [procedure] carrying [length] bytes. */
  const costOf = (procedure: string, length: number): number =>
    muxCost(
      new TalkFrame({
        kind: TalkKind.message,
        procedure: Name.of(procedure),
        payload: new Uint8Array(length),
      }).encode().length,
    );

  /** The flow-control cost of a stream item carrying [length] bytes. */
  const itemCost = (length: number): number =>
    muxCost(
      new TalkFrame({
        kind: TalkKind.streamItem,
        responseId: 1,
        payload: new Uint8Array(length),
      }).encode().length,
    );

  test('send completes once the window took the message, waits beyond it, and fails when the channel can no longer send', async () => {
    const [talk, serverTalk, raw] = await talkPair();
    const subscription = new Collector(serverTalk.messages, { paused: true });
    // The window takes it now: completed at once.
    await talk.send('MSG', new Uint8Array(1000));
    const pending: Promise<void>[] = [];
    for (let i = 0; i < 100; i++) {
      pending.push(talk.send('MSG', new Uint8Array(1000)));
    }
    let done = 0;
    for (const sent of pending) {
      void sent.then(() => {
        done++;
      });
    }
    await settle();
    const window = Math.floor(defaultInitialWindow / costOf('MSG', 1000));
    expect(done).toBe(window - 1);
    // The listener takes messages: credit comes back, the rest goes.
    subscription.resume();
    await Promise.all(pending);
    subscription.pause();
    const stuck: Promise<void>[] = [];
    for (let i = 0; i < 100; i++) {
      stuck.push(talk.send('MSG', new Uint8Array(1000)));
    }
    for (const sent of stuck) {
      void sent.catch(() => {});
    }
    await settle();
    // The peer closes the channel: what still waits for credit fails.
    await serverTalk.close();
    await rejectsStatus(stuck[stuck.length - 1] as Promise<void>, StatusCode.failedPrecondition);
    expect(raw.canSend).toBe(false);
    await subscription.cancel();
  });

  test('a failure nobody waits for is not reported as unhandled', async () => {
    const errors = await uncaughtErrors(async () => {
      const [talk, serverTalk] = await talkPair();
      // Nobody waits for these either.
      new Collector(serverTalk.messages, {
        paused: true,
        onValue: (m) => {
          void m.reply(empty);
        },
      });
      for (let i = 0; i < 100; i++) {
        void talk.send('MSG', new Uint8Array(1000));
      }
      await settle();
      await serverTalk.close();
      await settle();
    });
    expect(errors).toEqual([]);
  });

  test('a publisher awaiting each send holds at most one message beyond the window of a stalled subscriber', async () => {
    const [talk, serverTalk, raw, serverRaw] = await talkPair();
    const received = new Collector(serverTalk.messages, { paused: true });
    const count = 300;
    let sent = 0;
    const publisher = (async () => {
      for (let i = 0; i < count; i++) {
        await talk.send('EVENT', new Uint8Array(1000));
        sent++;
      }
    })();
    await settle();
    const window = Math.floor(defaultInitialWindow / costOf('EVENT', 1000));
    // What the window took went out; the publisher waits on the next one
    // instead of queueing the rest.
    expect(sent).toBe(window);
    await settle();
    expect(sent).toBe(window);
    expect(raw.sendWindow).toBeLessThan(costOf('EVENT', 1000));
    expect(client.heldOutputBytes).toBe(0);
    expect(serverRaw.heldBytes).toBeLessThanOrEqual(defaultInitialWindow);
    received.resume();
    await publisher;
    await settle();
    expect(received.values).toHaveLength(count);
    await received.cancel();
    await talk.close();
  });

  test('a responder awaiting each replyItem is paced by a stalled items subscription', async () => {
    const [talk, serverTalk, raw] = await talkPair();
    const count = 300;
    let sent = 0;
    const responded = new Deferred<void>();
    serve(serverTalk, async (m) => {
      for (let i = 0; i < count; i++) {
        await m.replyItem(new Uint8Array(1000));
        sent++;
      }
      await m.reply(empty);
      responded.resolve();
    });
    const stream = talk.streamRequest('LIST', empty);
    const items = new Collector(stream.items, { paused: true });
    await settle();
    const window = Math.floor(defaultInitialWindow / itemCost(1000));
    // The request took its share of the window too.
    expect(sent).toBeLessThanOrEqual(window);
    expect(sent).toBeGreaterThan(window - 3);
    expect(raw.heldBytes).toBeLessThanOrEqual(defaultInitialWindow);
    items.resume();
    await stream.done;
    await responded.promise;
    await items.finished;
    expect(items.values).toHaveLength(count);
    await items.cancel();
    await talk.close();
  });

  test('replyStream pauses its source while an item waits for the window', async () => {
    const [talk, serverTalk] = await talkPair();
    const count = 300;
    let produced = 0;
    async function* source(): AsyncGenerator<Uint8Array> {
      for (let i = 0; i < count; i++) {
        produced++;
        yield new Uint8Array(1000);
      }
    }
    serve(serverTalk, (m) => {
      void m.replyStream(source());
    });
    const stream = talk.streamRequest('LIST', empty);
    const items = new Collector(stream.items, { paused: true });
    await settle();
    const window = Math.floor(defaultInitialWindow / itemCost(1000));
    expect(produced).toBeLessThanOrEqual(window + 3);
    items.resume();
    await stream.done;
    await items.finished;
    expect(items.values).toHaveLength(count);
    expect(produced).toBe(count);
    await items.cancel();
    await talk.close();
  });
});

describe('readiness', () => {
  test('a publisher gating on whenWritable never queues in the channel, and a stalled subscriber stops it', async () => {
    const [talk, serverTalk, raw, serverRaw] = await talkPair();
    const received = new Collector(serverTalk.messages, { paused: true });
    const count = 300;
    let sent = 0;
    let waiting = 0;
    const publisher = (async () => {
      for (let i = 0; i < count; i++) {
        await talk.whenWritable;
        const handed = talk.send('EVENT', new Uint8Array(1000));
        // The window had room: it went at once.
        let done = false;
        void handed.then(() => {
          done = true;
        });
        await Promise.resolve();
        if (!done) {
          waiting++;
        }
        sent++;
      }
    })();
    await settle();
    // Writable while the window has room for the largest frame (half of
    // it): the publisher stopped with the window half spent.
    expect(talk.isWritable).toBe(false);
    expect(waiting).toBe(0);
    expect(raw.sendWindow).toBeGreaterThan(0);
    expect(raw.sendWindow).toBeLessThan(raw.maxSubframeLength + 16);
    expect(sent).toBeLessThan(40);
    expect(serverRaw.heldBytes).toBeLessThan(defaultInitialWindow);
    await settle();
    expect(sent).toBeLessThan(40);
    received.resume();
    await publisher;
    await settle();
    expect(received.values).toHaveLength(count);
    expect(waiting).toBe(0);
    await received.cancel();
    // A closed channel is not writable.
    await talk.close();
    expect(talk.isWritable).toBe(false);
    await rejectsStatus(talk.whenWritable, StatusCode.failedPrecondition);
  });

  test('whenWritable over a raw stream channel completes at once', async () => {
    const [a] = frameLink();
    const talk = new TalkChannel(a);
    expect(talk.isWritable).toBe(true);
    await talk.whenWritable;
    await talk.close();
    expect(talk.isWritable).toBe(false);
    await rejectsStatus(talk.whenWritable, StatusCode.failedPrecondition);
  });
});
