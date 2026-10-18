/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// The reconnecting client against an in-memory endpoint, on a stepping
// manual clock so that the backoff schedule is exact and the tests are
// fast. A port of the Dart reconnecting_client_test.dart, over the toy mux
// and, from `over-frame-mux/`, over the frame mux. The bulk payload case
// needs the frame mux's window (over the toy mux the payload goes inline)
// and runs over it only.

import { afterEach, describe, expect, test } from 'vitest';
import { ChannelAddress } from '../../../src/core/address/index.ts';
import { bytesEqual } from '../../../src/core/bytes.ts';
import {
  ClientPhase,
  ClientState,
  type PersistentChannel,
  ReconnectingClient,
  type ReconnectingClientOptions,
} from '../../../src/core/client/index.ts';
import {
  type MuxChannel,
  type MuxConnection,
  type MuxOptions,
  muxOptions,
} from '../../../src/core/mux/contract.ts';
import { Name } from '../../../src/core/name.ts';
import { Status, StatusCode, SwitchboardError, statusCodeName } from '../../../src/core/status.ts';
import { IncomingChannel } from '../../../src/core/switchboard/incoming-channel.ts';
import { TalkBulkOpen, TalkChannel } from '../../../src/core/talk/index.ts';
import type { FrameTransport, TransportConnector } from '../../../src/core/transport/contract.ts';
import { memoryTransportPair } from '../../../src/core/transport/memory-transport.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';
import type { Logger, LogLevel } from '../../../src/core/util/logger.ts';
import { ManualClock } from '../../../src/testing/manual-clock.ts';
import { overFrameMux, testMux } from '../../support/node/harness.ts';

/**
 * Keep-alive off so that the clock only sees the timers under test, and a
 * short keep-alive timeout, which bounds how long a close waits for the
 * transport.
 */
const quiet: Partial<MuxOptions> = Object.freeze({
  keepAliveInterval: undefined,
  keepAliveTimeout: 100,
  goAwayGrace: 2000,
});

const events = new ChannelAddress({ type: 'events' });
const feed = new ChannelAddress({ type: 'feed' });

function bytes(...values: number[]): Uint8Array {
  return Uint8Array.from(values);
}

function codeOf(status: Status | undefined): string | undefined {
  return status === undefined ? undefined : statusCodeName(status.code);
}

function errorCode(error: unknown): string | undefined {
  return error instanceof SwitchboardError ? statusCodeName(error.code) : undefined;
}

/**
 * The endpoint side: every transport the connector creates is accepted by
 * a toy mux connection here, unless the endpoint is told to refuse.
 */
class ScriptedEndpoint {
  readonly clock: ManualClock;
  readonly options: MuxOptions;
  readonly connections: MuxConnection[] = [];
  readonly channels: IncomingChannel[] = [];
  /** Clock time of every connector call. */
  readonly attempts: number[] = [];
  /** The next this many connector calls fail. */
  failures = 0;
  /** Every connector call fails. */
  refuseAll = false;
  /** Called with each channel the client opens. */
  onChannel: ((channel: IncomingChannel) => void) | undefined;

  constructor(clock: ManualClock, options: Partial<MuxOptions> = quiet) {
    this.clock = clock;
    this.options = muxOptions({ ...options, clock });
  }

  readonly connect: TransportConnector = async () => {
    this.attempts.push(this.clock.now());
    if (this.refuseAll || this.failures > 0) {
      if (this.failures > 0) {
        this.failures--;
      }
      throw SwitchboardError.of(StatusCode.unavailable, 'refused');
    }
    return this.accept();
  };

  accept(): FrameTransport {
    const [client, server] = memoryTransportPair();
    const connection = testMux()(server, this.options, false);
    this.connections.push(connection);
    void (async () => {
      for await (const channel of connection.incoming) {
        const incoming = new IncomingChannel(channel);
        if (TalkBulkOpen.isBulk(incoming.address)) {
          // As a Switchboard routes them (before a Talk channel installed
          // bulk routing on the connection).
          TalkChannel.adoptBulk(channel);
          continue;
        }
        this.channels.push(incoming);
        this.onChannel?.(incoming);
      }
    })();
    return client;
  }

  get last(): MuxConnection {
    return this.connections.at(-1) as MuxConnection;
  }

  /** The channels the client opened on [connection]. */
  on(connection: MuxConnection): IncomingChannel[] {
    return this.channels.filter((channel) => channel.connection === connection);
  }
}

/** Serves the `feed` channels the client opens on [endpoint] with Talk, counting the `SUB` requests. */
function subscriptionsAt(endpoint: ScriptedEndpoint): { count: number } {
  const subscriptions = { count: 0 };
  endpoint.onChannel = (incoming) => {
    if (!incoming.address.type?.equals(Name.of('feed'))) {
      return;
    }
    void incoming.talk().serve((message) => {
      subscriptions.count++;
      void message.reply();
    });
  };
  return subscriptions;
}

const clients: ReconnectingClient[] = [];

afterEach(async () => {
  // A safety net for a failed test; each test closes its own client.
  for (const client of clients.splice(0)) {
    void client.close();
  }
});

function clientFor(
  endpoint: ScriptedEndpoint,
  options: ReconnectingClientOptions & { connect?: TransportConnector } = {},
): ReconnectingClient {
  const client = new ReconnectingClient(options.connect ?? endpoint.connect, {
    muxOptions: quiet,
    jitter: 0,
    clock: endpoint.clock,
    mux: testMux(),
    ...options,
  });
  clients.push(client);
  return client;
}

/** The outcome of a promise, observable synchronously. */
class Outcome<T> {
  value: T | undefined;
  error: unknown;
  isDone = false;

  constructor(promise: Promise<T>) {
    promise.then(
      (value) => {
        this.value = value;
        this.isDone = true;
      },
      (error: unknown) => {
        this.error = error;
        this.isDone = true;
      },
    );
  }
}

/** The current state of [client] and every change from now on. */
function record(client: ReconnectingClient): ClientState[] {
  const states = [client.state];
  client.on('state', (state) => states.push(state));
  return states;
}

/** Every change of [client]'s state from now on (Dart `states.skip(1)`). */
function changes(client: ReconnectingClient): ClientState[] {
  const states: ClientState[] = [];
  client.on('state', (state) => states.push(state));
  return states;
}

/** The frames [channel] receives from now on. */
function frames(channel: MuxChannel): number[][] {
  const received: number[][] = [];
  void (async () => {
    for await (const frame of channel.readable) received.push([...frame]);
  })();
  return received;
}

/**
 * Closes [client] and checks that it leaves nothing behind. The channels
 * the application opened delay it by up to the GOAWAY grace.
 */
async function closeAndCheck(
  clock: ManualClock,
  client: ReconnectingClient,
  appChannels = false,
): Promise<void> {
  const closed = new Outcome(client.close());
  await clock.elapseAsync(appChannels ? 2000 + 100 : 100);
  expect(closed.isDone, 'close() resolved').toBe(true);
  expect(clock.pendingCount, 'no timers after close()').toBe(0);
}

/** A seeded source of numbers in [0, 1) (mulberry32). */
function seeded(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 0x100000000;
  };
}

describe('connecting', () => {
  test('connects and reports its states', async () => {
    const clock = new ManualClock();
    const endpoint = new ScriptedEndpoint(clock);
    const client = clientFor(endpoint);
    expect(client.state.phase).toBe(ClientPhase.connecting);
    expect(client.state.attempt).toBe(1);
    expect(client.isRunning).toBe(true);
    const states = record(client);
    const connected = new Outcome(client.connected);
    await ManualClock.settle();
    expect(states.map((s) => s.phase)).toEqual(['connecting', 'connected']);
    const last = states.at(-1) as ClientState;
    expect(last.attempt).toBe(1);
    expect(last.lastStatus).toBeUndefined();
    expect(last.since).toBe(0);
    expect(client.state.isConnected).toBe(true);
    expect(client.connection).toBeDefined();
    expect(connected.value).toBe(client.connection);
    expect(endpoint.connections).toHaveLength(1);
    expect(client.connection?.isInitiator).toBe(true);

    // A later consumer first receives the current state.
    const late: string[] = [];
    const lateDone = new Outcome(
      (async () => {
        for await (const state of client.states) late.push(state.phase);
      })(),
    );
    await ManualClock.settle();
    expect(late).toEqual(['connected']);

    void endpoint.last.close();
    await ManualClock.settle();
    expect(client.state.phase).toBe(ClientPhase.disconnected);
    expect(codeOf(client.state.lastStatus)).toBe('connectionLost');
    expect(client.state.attempt).toBe(0);
    expect(client.connection).toBeUndefined();
    // A new promise for the next connection.
    const next = new Outcome(client.connected);
    await ManualClock.settle();
    expect(next.isDone).toBe(false);
    await clock.elapseAsync(500);
    expect(next.value).toBe(client.connection);
    expect(next.value).not.toBe(connected.value);
    expect(states.map((s) => s.phase)).toEqual([
      'connecting',
      'connected',
      'disconnected',
      'connecting',
      'connected',
    ]);
    // The status of the previous loss is kept while connected.
    expect(codeOf(states.at(-1)?.lastStatus)).toBe('connectionLost');

    await closeAndCheck(clock, client);
    expect(lateDone.isDone, 'states ends after close()').toBe(true);
    expect(late.at(-1)).toBe('disconnected');
    expect(client.state.phase).toBe(ClientPhase.disconnected);
    expect(codeOf(client.state.lastStatus)).toBe('cancelled');
    // A consumer after close gets the last state, then the end.
    const after: string[] = [];
    for await (const state of client.states) after.push(state.phase);
    expect(after).toEqual(['disconnected']);
  });

  test('autoStart false waits for start()', async () => {
    const clock = new ManualClock();
    const endpoint = new ScriptedEndpoint(clock);
    const client = clientFor(endpoint, { autoStart: false });
    await clock.elapseAsync(5000);
    expect(endpoint.attempts).toEqual([]);
    expect(client.state.phase).toBe(ClientPhase.disconnected);
    expect(client.isRunning).toBe(false);
    client.start();
    await ManualClock.settle();
    expect(endpoint.connections).toHaveLength(1);
    expect(client.state.phase).toBe(ClientPhase.connected);
    await closeAndCheck(clock, client);
  });

  test('rejects invalid settings', () => {
    let calls = 0;
    const connect: TransportConnector = () => {
      calls++;
      return Promise.reject(new Error('not called'));
    };
    expect(() => new ReconnectingClient(connect, { initialBackoff: -1 })).toThrow(RangeError);
    expect(() => new ReconnectingClient(connect, { initialBackoff: 10, maxBackoff: 5 })).toThrow(
      RangeError,
    );
    expect(() => new ReconnectingClient(connect, { backoffFactor: 0.5 })).toThrow(RangeError);
    expect(
      () => new ReconnectingClient(connect, { backoffFactor: Number.POSITIVE_INFINITY }),
    ).toThrow(RangeError);
    expect(() => new ReconnectingClient(connect, { jitter: 1.5 })).toThrow(RangeError);
    expect(() => new ReconnectingClient(connect, { connectTimeout: -1 })).toThrow(RangeError);
    expect(
      () => new ReconnectingClient({} as unknown as TransportConnector, { autoStart: false }),
    ).toThrow(TypeError);
    expect(calls).toBe(0);
  });

  test('a connection connector: another binding, its signal aborted when given up', async () => {
    const clock = new ManualClock();
    const endpoint = new ScriptedEndpoint(clock);
    const signals: AbortSignal[] = [];
    const hanging = new Deferred<MuxConnection>();
    let calls = 0;
    const seenOptions: MuxOptions[] = [];
    const client = new ReconnectingClient(
      {
        connect: (options, init) => {
          calls++;
          seenOptions.push(options);
          signals.push(init.signal);
          if (calls === 1) {
            return hanging.promise;
          }
          return Promise.resolve(testMux()(endpoint.accept(), muxOptions(options), true));
        },
      },
      { muxOptions: quiet, jitter: 0, clock, connectTimeout: 1000 },
    );
    clients.push(client);
    expect(seenOptions[0]?.goAwayGrace).toBe(2000);
    expect(seenOptions[0]?.clock).toBe(clock);
    await clock.elapseAsync(1000);
    expect(signals[0]?.aborted).toBe(true);
    expect(codeOf(client.state.lastStatus)).toBe('unavailable');
    await clock.elapseAsync(500);
    expect(calls).toBe(2);
    expect(client.state.phase).toBe(ClientPhase.connected);
    expect(signals[1]?.aborted).toBe(false);
    // The connection of the abandoned attempt arrives late: closed.
    const stray = testMux()(endpoint.accept(), muxOptions({ ...quiet, clock }), true);
    hanging.resolve(stray);
    await ManualClock.settle();
    expect(stray.isOpen).toBe(false);
    await closeAndCheck(clock, client);
  });
});

describe('backoff', () => {
  test('doubles from the initial delay up to the cap', async () => {
    const clock = new ManualClock();
    const endpoint = new ScriptedEndpoint(clock);
    endpoint.refuseAll = true;
    const client = clientFor(endpoint, { maxBackoff: 4000 });
    const states = record(client);
    await clock.elapseAsync(20_000);
    expect(endpoint.attempts).toEqual([0, 500, 1500, 3500, 7500, 11_500, 15_500, 19_500]);
    const failed = states.filter((s) => s.phase === ClientPhase.disconnected);
    expect(failed.map((s) => s.attempt)).toEqual([1, 2, 3, 4, 5, 6, 7, 8]);
    for (const s of failed) {
      expect(codeOf(s.lastStatus)).toBe('unavailable');
    }
    expect(states.filter((s) => s.phase === ClientPhase.connecting).map((s) => s.attempt)).toEqual([
      1, 2, 3, 4, 5, 6, 7, 8,
    ]);
    await closeAndCheck(clock, client);
  });

  test('jitter stays within its bounds', async () => {
    const clock = new ManualClock();
    const endpoint = new ScriptedEndpoint(clock);
    endpoint.refuseAll = true;
    const client = clientFor(endpoint, { maxBackoff: 4000, jitter: 0.2, random: seeded(7) });
    await clock.elapseAsync(120_000);
    const times = endpoint.attempts;
    expect(times.length).toBeGreaterThan(20);
    let exact = 0;
    for (let i = 1; i < times.length; i++) {
      const gap = (times[i] as number) - (times[i - 1] as number);
      const base = Math.min(500 * 2 ** (i - 1), 4000);
      expect(gap).toBeGreaterThanOrEqual(base * 0.8 - 0.001);
      expect(gap).toBeLessThanOrEqual(base * 1.2 + 0.001);
      if (gap === base) {
        exact++;
      }
    }
    expect(exact, 'jitter applied').toBeLessThan(Math.floor(times.length / 2));
    await closeAndCheck(clock, client);
  });

  test('resets after a connection that outlived the cap', async () => {
    const clock = new ManualClock();
    const endpoint = new ScriptedEndpoint(clock);
    endpoint.failures = 3;
    const client = clientFor(endpoint, { maxBackoff: 8000 });
    // Fails at 0, 0.5 and 1.5 s; connects at 3.5 s.
    await clock.elapseAsync(3500);
    expect(endpoint.connections).toHaveLength(1);
    // A short-lived connection keeps the sequence going: 4 s.
    await clock.elapseAsync(1000);
    void endpoint.last.close();
    await clock.elapseAsync(3999);
    expect(endpoint.connections).toHaveLength(1);
    await clock.elapseAsync(1);
    expect(endpoint.connections).toHaveLength(2);
    // A connection older than maxBackoff resets it: 0.5 s.
    await clock.elapseAsync(10_000);
    void endpoint.last.close();
    await clock.elapseAsync(500);
    expect(endpoint.attempts).toEqual([0, 500, 1500, 3500, 8500, 19_000]);
    expect(endpoint.connections).toHaveLength(3);
    await closeAndCheck(clock, client);
  });

  test('a throwing, failing or hanging connector is retried', async () => {
    const clock = new ManualClock();
    const endpoint = new ScriptedEndpoint(clock);
    const late = new Deferred<FrameTransport>();
    let calls = 0;
    let hangingSignal: AbortSignal | undefined;
    const connect: TransportConnector = (init) => {
      calls++;
      switch (calls) {
        case 1:
          throw new Error('synchronous failure');
        case 2:
          return Promise.reject(new SyntaxError('asynchronous'));
        case 3:
          hangingSignal = init.signal;
          return late.promise;
        default:
          return Promise.resolve(endpoint.accept());
      }
    };
    const client = clientFor(endpoint, { connect, connectTimeout: 2000 });
    const statuses: (Status | undefined)[] = [];
    client.on('state', (s) => {
      if (s.phase === ClientPhase.disconnected) statuses.push(s.lastStatus);
    });
    await ManualClock.settle();
    expect(calls).toBe(1);
    expect(client.state.phase).toBe(ClientPhase.disconnected);
    await clock.elapseAsync(500);
    expect(calls).toBe(2);
    await clock.elapseAsync(1000);
    expect(calls).toBe(3);
    expect(client.state.phase).toBe(ClientPhase.connecting);
    // The third attempt times out after 2 s; the fourth comes 2 s later.
    await clock.elapseAsync(2000);
    expect(client.state.phase).toBe(ClientPhase.disconnected);
    expect(client.state.attempt).toBe(3);
    expect(statuses.map(codeOf)).toEqual(['unavailable', 'unavailable', 'unavailable']);
    expect(hangingSignal?.aborted).toBe(true);
    expect(errorCode(hangingSignal?.reason)).toBe('unavailable');
    // The transport of the timed out attempt arrives late: closed.
    late.resolve(endpoint.accept());
    await ManualClock.settle();
    const strayDone = new Outcome(endpoint.last.done);
    await ManualClock.settle();
    expect(strayDone.isDone).toBe(true);
    await clock.elapseAsync(2000);
    expect(calls).toBe(4);
    expect(client.state.phase).toBe(ClientPhase.connected);
    expect(client.state.attempt).toBe(4);
    await closeAndCheck(clock, client);
  });
});

describe('openChannel', () => {
  test('waits for the connection, with defaultPayload attached', async () => {
    const clock = new ManualClock();
    const endpoint = new ScriptedEndpoint(clock);
    const client = clientFor(endpoint, { autoStart: false, defaultPayload: bytes(0xc0, 0xde) });
    const firstOpen = new Outcome(client.openChannel(events));
    const own = new Outcome(client.openChannel(feed.with({ payload: bytes(1) })));
    await clock.elapseAsync(5000);
    expect(firstOpen.isDone).toBe(false);
    client.start();
    await ManualClock.settle();
    expect(firstOpen.value?.state).toBe('open');
    expect(own.value?.state).toBe('open');
    expect(endpoint.channels).toHaveLength(2);
    expect(endpoint.channels[0]?.address.type?.equals(Name.of('events'))).toBe(true);
    expect([...(endpoint.channels[0]?.address.payload ?? [])]).toEqual([0xc0, 0xde]);
    expect(endpoint.channels[1]?.address.type?.equals(Name.of('feed'))).toBe(true);
    expect([...(endpoint.channels[1]?.address.payload ?? [])]).toEqual([1]);
    // Connected: opens at once, with the current default payload; a
    // service address stands for its channel address.
    client.defaultPayload = bytes(9);
    const now = new Outcome(client.openChannel('events'));
    await ManualClock.settle();
    expect(now.value?.state).toBe('open');
    expect([...(endpoint.channels.at(-1)?.address.payload ?? [])]).toEqual([9]);
    expect(endpoint.channels.at(-1)?.address.type?.equals(Name.of('events'))).toBe(true);
    await closeAndCheck(clock, client, true);
  });

  test('fails with UNAVAILABLE after openTimeout, CANCELLED on close', async () => {
    const clock = new ManualClock();
    const endpoint = new ScriptedEndpoint(clock);
    endpoint.refuseAll = true;
    const client = clientFor(endpoint);
    const timed = new Outcome(client.openChannel(events, { openTimeout: 3000 }));
    const immediate = new Outcome(client.openChannel(events, { openTimeout: 0 }));
    const waiting = new Outcome(client.openChannel(events));
    const connected = new Outcome(client.connected);
    await ManualClock.settle();
    expect(errorCode(immediate.error)).toBe('unavailable');
    await clock.elapseAsync(2999);
    expect(timed.isDone).toBe(false);
    await clock.elapseAsync(1);
    expect(errorCode(timed.error)).toBe('unavailable');
    expect(waiting.isDone).toBe(false);
    await closeAndCheck(clock, client);
    expect(errorCode(waiting.error)).toBe('cancelled');
    expect(errorCode(connected.error)).toBe('cancelled');
    const after = new Outcome(client.openChannel(events));
    await ManualClock.settle();
    expect(errorCode(after.error)).toBe('failedPrecondition');
    let thrown: unknown;
    try {
      client.openPersistent(events, () => {});
    } catch (e) {
      thrown = e;
    }
    expect(errorCode(thrown)).toBe('failedPrecondition');
  });

  test('a signal abandons the wait', async () => {
    const clock = new ManualClock();
    const endpoint = new ScriptedEndpoint(clock);
    const client = clientFor(endpoint, { autoStart: false });
    const controller = new AbortController();
    const waiting = new Outcome(client.openChannel(events, { signal: controller.signal }));
    controller.abort(new Error('not any more'));
    await ManualClock.settle();
    expect((waiting.error as Error).message).toBe('not any more');
    const aborted = new Outcome(client.openChannel(events, { signal: controller.signal }));
    await ManualClock.settle();
    expect(aborted.error).toBe(controller.signal.reason);
    client.start();
    await ManualClock.settle();
    // The abandoned open was not sent.
    expect(endpoint.channels).toHaveLength(0);
    expect(clock.pendingCount).toBe(1); // the connection lifetime timer only
    await closeAndCheck(clock, client);
  });

  test('a Talk request through openTalk', async () => {
    const clock = new ManualClock();
    const endpoint = new ScriptedEndpoint(clock);
    endpoint.onChannel = (incoming) => {
      void incoming.talk().serve((message) => {
        void message.reply(bytes(...message.payload, 2), { procedure: 'PONG' });
      });
    };
    const client = clientFor(endpoint);
    const reply = new Outcome(
      client.openTalk(events).then((talk) => talk.request('PING', bytes(1))),
    );
    await ManualClock.settle();
    expect(reply.value?.procedureName).toBe('PONG');
    expect([...(reply.value?.payload ?? [])]).toEqual([1, 2]);
    await closeAndCheck(clock, client, true);
  });

  test("openTalk merges its Talk options over the client's", async () => {
    const clock = new ManualClock();
    const endpoint = new ScriptedEndpoint(clock);
    const client = clientFor(endpoint, { talkOptions: { requestTimeout: 1234, replyTimeout: 99 } });
    const talk = new Outcome(client.openTalk(events, { talkOptions: { replyTimeout: 55 } }));
    await ManualClock.settle();
    expect(talk.value).toBeInstanceOf(TalkChannel);
    expect(talk.value?.options.requestTimeout).toBe(1234);
    expect(talk.value?.options.replyTimeout).toBe(55);
    expect(talk.value?.raw?.connection).toBe(client.connection);
    await closeAndCheck(clock, client, true);
  });
});

if (overFrameMux()) {
  describe('bulk payloads', () => {
    test('both ways through openTalk', async () => {
      const clock = new ManualClock();
      const endpoint = new ScriptedEndpoint(clock);
      endpoint.onChannel = (incoming) => {
        void incoming.talk().serve((message) => {
          void message.reply(Uint8Array.from([...message.payload, ...message.payload]));
        });
      };
      const client = clientFor(endpoint);
      const big = Uint8Array.from({ length: 70_000 }, (_, i) => i % 251);
      const reply = new Outcome(client.openTalk(events).then((talk) => talk.request('PUT', big)));
      await ManualClock.settle();
      await clock.elapseAsync(10);
      expect(reply.error).toBeUndefined();
      expect(reply.value?.isBulk).toBe(true);
      const doubled = Uint8Array.from([...big, ...big]);
      expect(bytesEqual(reply.value?.payload ?? new Uint8Array(0), doubled)).toBe(true);
      // The endpoint's bulk channel never reached the application.
      expect(endpoint.channels).toHaveLength(1);
      await closeAndCheck(clock, client, true);
    });
  });
}

describe('persistent channels', () => {
  test('re-opened after the endpoint drops the connection', async () => {
    const clock = new ManualClock();
    const endpoint = new ScriptedEndpoint(clock);
    const client = clientFor(endpoint);
    const opened: MuxChannel[] = [];
    const closed: Status[] = [];
    const phaseAtClose: ClientPhase[] = [];
    const persistent = client.openPersistent(events, (channel) => opened.push(channel), {
      onClosed: (status) => {
        closed.push(status);
        phaseAtClose.push(client.state.phase);
      },
    });
    // Never opened synchronously.
    expect(opened).toEqual([]);
    const announced: MuxChannel[] = [];
    void (async () => {
      for await (const channel of persistent.opened) announced.push(channel);
    })();
    await ManualClock.settle();
    expect(opened).toHaveLength(1);
    expect(persistent.current).toBe(opened[0]);
    expect(announced).toEqual(opened);
    expect(endpoint.channels[0]?.address.type?.equals(Name.of('events'))).toBe(true);

    void endpoint.last.close();
    await ManualClock.settle();
    expect(closed.map(codeOf)).toEqual(['connectionLost']);
    // The disconnect is reported before the channels end.
    expect(phaseAtClose).toEqual([ClientPhase.disconnected]);
    expect(persistent.current).toBeUndefined();
    expect(persistent.isClosed).toBe(false);

    await clock.elapseAsync(500);
    expect(endpoint.connections).toHaveLength(2);
    expect(opened).toHaveLength(2);
    expect(persistent.current).toBe(opened[1]);
    expect(opened[1]?.connection).toBe(client.connection);
    expect(endpoint.on(endpoint.last)).toHaveLength(1);
    expect(announced).toEqual(opened);

    // Data flows on the new channel.
    const received = frames(opened[1] as MuxChannel);
    void endpoint.on(endpoint.last)[0]?.channel.send(bytes(5));
    await ManualClock.settle();
    expect(received).toEqual([[5]]);

    const done = new Outcome(persistent.close());
    await ManualClock.settle();
    expect(done.isDone).toBe(true);
    expect(closed).toHaveLength(2);
    expect(codeOf(closed[1])).toBe('ok');
    expect(persistent.isClosed).toBe(true);
    expect(endpoint.on(endpoint.last)[0]?.channel.state).toBe('closed');
    await closeAndCheck(clock, client);
    expect(opened).toHaveLength(2);
  });

  test('closed by the endpoint: re-opened after the backoff', async () => {
    const clock = new ManualClock();
    const endpoint = new ScriptedEndpoint(clock);
    const client = clientFor(endpoint, { maxBackoff: 4000 });
    const opened: MuxChannel[] = [];
    const closed: Status[] = [];
    const persistent = client.openPersistent(events, (channel) => opened.push(channel), {
      onClosed: (status) => closed.push(status),
    });
    await ManualClock.settle();
    expect(opened).toHaveLength(1);

    const endpointCloses = (code: StatusCode): Promise<void> =>
      (endpoint.channels.at(-1) as IncomingChannel).reject(Status.of(code, 'test'));

    void endpointCloses(StatusCode.unavailable);
    await ManualClock.settle();
    expect(closed.map(codeOf)).toEqual(['unavailable']);
    expect(persistent.current).toBeUndefined();
    await clock.elapseAsync(499);
    expect(opened).toHaveLength(1);
    await clock.elapseAsync(1);
    expect(opened).toHaveLength(2);
    // Same connection; the next delay doubles.
    expect(endpoint.connections).toHaveLength(1);
    void endpointCloses(StatusCode.internal);
    await clock.elapseAsync(999);
    expect(opened).toHaveLength(2);
    await clock.elapseAsync(1);
    expect(opened).toHaveLength(3);
    // A channel that outlived maxBackoff resets the sequence.
    await clock.elapseAsync(5000);
    void endpointCloses(StatusCode.resourceExhausted);
    await clock.elapseAsync(500);
    expect(opened).toHaveLength(4);
    // The application closing the channel itself also re-opens it.
    void opened[3]?.close();
    await clock.elapseAsync(1000);
    expect(opened).toHaveLength(5);
    expect(closed).toHaveLength(4);
    expect(codeOf(closed[3])).toBe('ok');
    expect(endpoint.connections).toHaveLength(1);
    await closeAndCheck(clock, client);
  });

  for (const code of [
    StatusCode.permissionDenied,
    StatusCode.unauthenticated,
    StatusCode.unimplemented,
    StatusCode.notFound,
  ]) {
    test(`closed by the endpoint with ${statusCodeName(code)}: stays closed`, async () => {
      const clock = new ManualClock();
      const endpoint = new ScriptedEndpoint(clock);
      const client = clientFor(endpoint);
      const opened: MuxChannel[] = [];
      const closed: Status[] = [];
      const closedFlag: boolean[] = [];
      const persistent: PersistentChannel = client.openPersistent(
        events,
        (channel) => opened.push(channel),
        {
          onClosed: (status) => {
            closed.push(status);
            closedFlag.push(persistent.isClosed);
          },
        },
      );
      const done = new Outcome(persistent.done);
      const announced: MuxChannel[] = [];
      const announcedEnd = new Outcome(
        (async () => {
          for await (const channel of persistent.opened) announced.push(channel);
        })(),
      );
      await ManualClock.settle();
      void endpoint.channels[0]?.reject(Status.of(code));
      await clock.elapseAsync(60_000);
      expect(opened).toHaveLength(1);
      expect(closed.map((s) => s.code)).toEqual([code]);
      expect(closedFlag).toEqual([true]);
      expect(persistent.isClosed).toBe(true);
      expect(done.value?.code).toBe(code);
      expect(announcedEnd.isDone).toBe(true);
      expect(announced).toHaveLength(1);
      // Not re-opened on a new connection either.
      void endpoint.last.close();
      await clock.elapseAsync(2000);
      expect(endpoint.connections).toHaveLength(2);
      expect(opened).toHaveLength(1);
      expect(endpoint.channels).toHaveLength(1);
      await closeAndCheck(clock, client);
    });
  }

  test('onOpen exceptions are logged and do not affect others', async () => {
    const clock = new ManualClock();
    const endpoint = new ScriptedEndpoint(clock);
    const logged: [LogLevel, string, string][] = [];
    const logger: Logger = {
      log: (level, name, message) => logged.push([level, name, message]),
    };
    const client = clientFor(endpoint, { logger });
    const opened: string[] = [];
    client.openPersistent(events, () => {
      opened.push('throws');
      throw new Error('boom');
    });
    client.openPersistent(events, async () => {
      opened.push('fails');
      throw new Error('boom');
    });
    client.openPersistent(feed, () => opened.push('fine'));
    await ManualClock.settle();
    expect(opened).toEqual(['throws', 'fails', 'fine']);
    void endpoint.last.close();
    await clock.elapseAsync(500);
    expect(opened).toEqual(['throws', 'fails', 'fine', 'throws', 'fails', 'fine']);
    expect(endpoint.on(endpoint.last)).toHaveLength(3);
    const failures = logged.filter(([, , message]) => message.endsWith('onOpen failed'));
    expect(failures).toHaveLength(4);
    expect(
      failures.every(([level, name]) => level === 'warn' && name === 'Switchboard.Client'),
    ).toBe(true);
    await closeAndCheck(clock, client);
  });

  test('PersistentTalk re-sends its subscription on every connection', async () => {
    const clock = new ManualClock();
    const endpoint = new ScriptedEndpoint(clock);
    let subscriptions = 0;
    endpoint.onChannel = (incoming) => {
      const talk = incoming.talk();
      void talk.serve((message) => {
        if (message.procedureName === 'SUB') {
          subscriptions++;
          void message.reply();
          void talk.send('EVENT', bytes(subscriptions));
        }
      });
    };
    const client = clientFor(endpoint);
    const received: number[] = [];
    const replies: string[] = [];
    const closed: Status[] = [];
    const persistent = client.openPersistentTalk(
      feed,
      async (talk) => {
        void talk.serve((m) => {
          received.push(m.payload[0] as number);
        });
        const reply = await talk.request('SUB', bytes(1));
        replies.push(`subscribed ${reply.payload.length}`);
      },
      { onClosed: (status) => closed.push(status) },
    );
    await ManualClock.settle();
    expect(subscriptions).toBe(1);
    expect(replies).toEqual(['subscribed 0']);
    expect(received).toEqual([1]);
    expect(persistent.current).toBeInstanceOf(TalkChannel);

    void endpoint.last.close();
    await ManualClock.settle();
    expect(closed.map(codeOf)).toEqual(['connectionLost']);
    await clock.elapseAsync(500);
    expect(subscriptions).toBe(2);
    expect(replies).toEqual(['subscribed 0', 'subscribed 0']);
    expect(received).toEqual([1, 2]);

    // A channel abort with a terminal status ends it.
    (endpoint.channels.at(-1) as IncomingChannel)
      .talk()
      .abort(Status.of(StatusCode.permissionDenied));
    await clock.elapseAsync(10_000);
    expect(persistent.isClosed).toBe(true);
    expect(codeOf(closed.at(-1))).toBe('permissionDenied');
    expect(subscriptions).toBe(2);
    await closeAndCheck(clock, client);
  });

  test('opened while connected, and on the next connection', async () => {
    const clock = new ManualClock();
    const endpoint = new ScriptedEndpoint(clock);
    endpoint.failures = 1;
    const client = clientFor(endpoint, { defaultPayload: bytes(7) });
    const opened: MuxChannel[] = [];
    client.openPersistent(events, (channel) => opened.push(channel));
    await ManualClock.settle();
    expect(opened).toEqual([]);
    await clock.elapseAsync(500);
    expect(opened).toHaveLength(1);
    expect([...(endpoint.channels[0]?.address.payload ?? [])]).toEqual([7]);
    // Created while connected: opened in a microtask.
    client.defaultPayload = bytes(8);
    client.openPersistent(feed, (channel) => opened.push(channel));
    expect(opened).toHaveLength(1);
    await ManualClock.settle();
    expect(opened).toHaveLength(2);
    expect([...(endpoint.channels.at(-1)?.address.payload ?? [])]).toEqual([8]);
    // Re-opened with the payload current at the time.
    void endpoint.last.close();
    await clock.elapseAsync(1000);
    expect(opened).toHaveLength(4);
    expect(endpoint.on(endpoint.last).map((c) => c.address.payload[0])).toEqual([8, 8]);
    await closeAndCheck(clock, client);
  });
});

describe('stop and close', () => {
  test('a connector that closes the client leaves no timeout behind', async () => {
    const clock = new ManualClock();
    const endpoint = new ScriptedEndpoint(clock);
    let client: ReconnectingClient | undefined;
    client = clientFor(endpoint, {
      autoStart: false,
      connect: () => {
        void client?.close();
        return new Deferred<FrameTransport>().promise;
      },
    });
    client.start();
    await ManualClock.settle();
    expect(client.isClosed).toBe(true);
    expect(clock.pendingCount).toBe(0);
  });

  test('stop() sends GOAWAY and stops reconnecting; start() resumes', async () => {
    const clock = new ManualClock();
    const endpoint = new ScriptedEndpoint(clock);
    const client = clientFor(endpoint);
    const opened: MuxChannel[] = [];
    const closed: Status[] = [];
    const persistent = client.openPersistent(events, (channel) => opened.push(channel), {
      onClosed: (status) => closed.push(status),
    });
    await ManualClock.settle();
    const server = endpoint.last;
    const serverDone = new Outcome(server.done);
    client.stop();
    client.stop();
    expect(client.isRunning).toBe(false);
    expect(client.state.phase).toBe(ClientPhase.disconnected);
    expect(codeOf(client.state.lastStatus)).toBe('cancelled');
    expect(client.connection).toBeUndefined();
    await ManualClock.settle();
    expect(server.peerGoingAway).toBe(true);
    // The persistent channel closed with GOING_AWAY, so the connection went
    // idle and closed without waiting for the grace period.
    expect(closed.map(codeOf)).toEqual(['goingAway']);
    expect(endpoint.channels[0]?.channel.state).toBe('closed');
    expect(codeOf(serverDone.value)).toBe('goingAway');
    expect(persistent.isClosed).toBe(false);
    await clock.elapseAsync(5 * 60_000);
    expect(endpoint.attempts).toHaveLength(1);
    expect(clock.pendingCount).toBe(0);

    client.start();
    await ManualClock.settle();
    expect(endpoint.connections).toHaveLength(2);
    expect(opened).toHaveLength(2);
    await closeAndCheck(clock, client);
    expect(closed).toHaveLength(2);
    expect(persistent.isClosed).toBe(true);
  });

  test('stop() during a connection attempt discards its transport', async () => {
    const clock = new ManualClock();
    const endpoint = new ScriptedEndpoint(clock);
    const pending = new Deferred<FrameTransport>();
    let signal: AbortSignal | undefined;
    const client = clientFor(endpoint, {
      connect: (init) => {
        signal = init.signal;
        return pending.promise;
      },
    });
    client.stop();
    expect(clock.pendingCount).toBe(0);
    expect(signal?.aborted).toBe(true);
    pending.resolve(endpoint.accept());
    const serverDone = new Outcome(endpoint.last.done);
    await ManualClock.settle();
    expect(serverDone.isDone).toBe(true);
    expect(client.connection).toBeUndefined();
    await closeAndCheck(clock, client);
  });

  test('close() waits for the application channels within the grace', async () => {
    const clock = new ManualClock();
    const endpoint = new ScriptedEndpoint(clock);
    const client = clientFor(endpoint);
    const own = new Outcome(client.openChannel(events));
    await ManualClock.settle();
    expect(own.value?.state).toBe('open');
    const closed = new Outcome(client.close());
    await clock.elapseAsync(1000);
    expect(closed.isDone).toBe(false);
    // The grace (2 s here) bounds it.
    await clock.elapseAsync(1200);
    expect(closed.isDone).toBe(true);
    expect(codeOf(await (own.value as MuxChannel).done)).toBe('goingAway');
    expect(clock.pendingCount).toBe(0);
  });

  test('close() while waiting for the next attempt', async () => {
    const clock = new ManualClock();
    const endpoint = new ScriptedEndpoint(clock);
    endpoint.refuseAll = true;
    const client = clientFor(endpoint);
    const persistent = client.openPersistent(events, () => {});
    await clock.elapseAsync(700);
    expect(endpoint.attempts).toHaveLength(2);
    await closeAndCheck(clock, client);
    expect(persistent.isClosed).toBe(true);
    expect(client.isClosed).toBe(true);
    client.start();
    await clock.elapseAsync(5000);
    expect(endpoint.attempts).toHaveLength(2);
  });

  test('await using closes the client', async () => {
    const clock = new ManualClock();
    const endpoint = new ScriptedEndpoint(clock);
    let kept: ReconnectingClient | undefined;
    {
      await using client = clientFor(endpoint);
      kept = client;
      await ManualClock.settle();
      expect(client.state.isConnected).toBe(true);
    }
    expect(kept?.isClosed).toBe(true);
    expect(clock.pendingCount).toBe(0);
  });
});

describe('GOAWAY from the endpoint', () => {
  test('persistent channels move to a new connection at once', async () => {
    const clock = new ManualClock();
    const endpoint = new ScriptedEndpoint(clock);
    const subscriptions = subscriptionsAt(endpoint);
    const client = clientFor(endpoint);
    const opened: MuxChannel[] = [];
    const closed: Status[] = [];
    const persistent = client.openPersistent(events, (channel) => opened.push(channel), {
      onClosed: (status) => closed.push(status),
    });
    const talkClosed: Status[] = [];
    const talk = client.openPersistentTalk(feed, (talk) => talk.request('SUB'), {
      onClosed: (status) => talkClosed.push(status),
    });
    await clock.elapseAsync(1000);
    expect(subscriptions.count).toBe(1);
    const firstConnection = client.connection as MuxConnection;
    const server = endpoint.last;
    const states = changes(client);
    // What the client is doing when the old connection ends.
    let stateAtOldEnd: ClientState | undefined;
    let connectionAtOldEnd: MuxConnection | undefined;
    const firstDone = new Outcome(
      firstConnection.done.then((status) => {
        stateAtOldEnd = client.state;
        connectionAtOldEnd = client.connection;
        return status;
      }),
    );
    const serverDone = new Outcome(server.done);
    void server.goAway();
    await ManualClock.settle();

    // A new attempt at once, reported with the GOAWAY status.
    expect(endpoint.attempts).toEqual([0, 1000]);
    expect(states.map((s) => s.phase)).toEqual(['connecting', 'connected']);
    expect(states.map((s) => codeOf(s.lastStatus))).toEqual(['goingAway', 'goingAway']);
    expect(states.map((s) => s.attempt)).toEqual([1, 1]);
    const second = client.connection as MuxConnection;
    expect(second).not.toBe(firstConnection);
    // The persistent channels were closed with GOING_AWAY, so the old
    // connection went idle and ended, after the new one was up.
    expect(closed.map(codeOf)).toEqual(['goingAway']);
    expect(talkClosed.map(codeOf)).toEqual(['goingAway']);
    expect(endpoint.on(server).every((c) => c.channel.state === 'closed')).toBe(true);
    expect(codeOf(firstDone.value)).toBe('goingAway');
    expect(stateAtOldEnd?.phase).toBe(ClientPhase.connected);
    expect(connectionAtOldEnd).toBe(second);
    expect(codeOf(serverDone.value), 'the endpoint did not wait for its grace').toBe('goingAway');
    // Opened again on the new connection.
    expect(opened).toHaveLength(2);
    expect(opened[1]?.connection).toBe(second);
    expect(persistent.current).toBe(opened[1]);
    expect(talk.current?.isOpen).toBe(true);
    expect(subscriptions.count).toBe(2);
    expect(
      endpoint
        .on(endpoint.last)
        .map((c) => c.address.type?.toString())
        .sort(),
    ).toEqual(['events', 'feed']);

    // The end of the old connection causes no second reconnect.
    await clock.elapseAsync(5 * 60_000);
    expect(endpoint.attempts).toHaveLength(2);
    expect(client.connection).toBe(second);
    expect(states).toHaveLength(2);
    expect(opened).toHaveLength(2);
    await closeAndCheck(clock, client);
  });

  test('on an idle connection: reconnects at once', async () => {
    const clock = new ManualClock();
    const endpoint = new ScriptedEndpoint(clock);
    const client = clientFor(endpoint);
    await ManualClock.settle();
    const firstConnection = client.connection as MuxConnection;
    const firstConnected = client.connected;
    const states = changes(client);
    const firstDone = new Outcome(firstConnection.done);
    void endpoint.last.goAway();
    await ManualClock.settle();
    expect(endpoint.attempts).toEqual([0, 0]);
    expect(states.map((s) => s.phase)).toEqual(['connecting', 'connected']);
    expect(codeOf(states[0]?.lastStatus)).toBe('goingAway');
    expect(client.connection).not.toBe(firstConnection);
    expect(client.connected).not.toBe(firstConnected);
    expect(codeOf(firstDone.value)).toBe('goingAway');
    await clock.elapseAsync(5 * 60_000);
    expect(endpoint.attempts).toHaveLength(2);
    expect(client.state.phase).toBe(ClientPhase.connected);
    await closeAndCheck(clock, client);
  });

  test('an application channel carries on over the old connection', async () => {
    const clock = new ManualClock();
    const endpoint = new ScriptedEndpoint(clock);
    const client = clientFor(endpoint);
    const own = new Outcome(client.openChannel(feed));
    await ManualClock.settle();
    const firstConnection = client.connection as MuxConnection;
    const server = endpoint.last;
    const mine = own.value as MuxChannel;
    const theirs = (endpoint.on(server)[0] as IncomingChannel).channel;
    const firstDone = new Outcome(firstConnection.done);
    const serverDone = new Outcome(server.done);
    void server.goAway();
    await ManualClock.settle();
    // Connected again at once; the old connection stays up for it.
    expect(endpoint.attempts).toHaveLength(2);
    expect(client.state.phase).toBe(ClientPhase.connected);
    expect(client.connection).not.toBe(firstConnection);
    expect(firstDone.isDone).toBe(false);
    expect(mine.state).toBe('open');

    const received = frames(mine);
    const sent = frames(theirs);
    await clock.elapseAsync(1000);
    void theirs.send(bytes(1));
    void mine.send(bytes(2));
    await ManualClock.settle();
    expect(received).toEqual([[1]]);
    expect(sent).toEqual([[2]]);
    // New channels go to the new connection.
    const next = new Outcome(client.openChannel(events));
    await ManualClock.settle();
    expect(next.value?.connection).toBe(client.connection);

    // Once the application is done, the old connection closes, before the
    // endpoint's grace expires, and nothing reconnects.
    void mine.close();
    await ManualClock.settle();
    expect(codeOf(firstDone.value)).toBe('goingAway');
    expect(codeOf(serverDone.value)).toBe('goingAway');
    await clock.elapseAsync(5 * 60_000);
    expect(endpoint.attempts).toHaveLength(2);
    expect(client.state.phase).toBe(ClientPhase.connected);
    expect(next.value?.state).toBe('open');
    await closeAndCheck(clock, client, true);
  });

  test('close() bounds an old connection held by the application', async () => {
    // The endpoint would wait long; close() waits the client's grace.
    const clock = new ManualClock();
    const endpoint = new ScriptedEndpoint(clock, { ...quiet, goAwayGrace: 10 * 60_000 });
    const client = clientFor(endpoint);
    const own = new Outcome(client.openChannel(feed));
    await ManualClock.settle();
    const firstConnection = client.connection as MuxConnection;
    void endpoint.last.goAway();
    await ManualClock.settle();
    expect(client.connection).not.toBe(firstConnection);
    const firstDone = new Outcome(firstConnection.done);
    await closeAndCheck(clock, client, true);
    expect(codeOf(firstDone.value)).toBe('goingAway');
    expect(own.value?.state).toBe('closed');
  });

  for (const how of ['stop', 'close'] as const) {
    test(`arriving during ${how}(): no reconnect`, async () => {
      const clock = new ManualClock();
      const endpoint = new ScriptedEndpoint(clock);
      const client = clientFor(endpoint);
      const closed: Status[] = [];
      client.openPersistent(events, () => {}, { onClosed: (status) => closed.push(status) });
      await ManualClock.settle();
      const server = endpoint.last;
      const serverDone = new Outcome(server.done);
      // Both sides send GOAWAY in the same turn.
      void server.goAway();
      let closing: Outcome<void> | undefined;
      if (how === 'stop') {
        client.stop();
      } else {
        closing = new Outcome(client.close());
      }
      await ManualClock.settle();
      expect(client.connection).toBeUndefined();
      expect(client.state.phase).toBe(ClientPhase.disconnected);
      expect(codeOf(client.state.lastStatus)).toBe('cancelled');
      expect(closed.map(codeOf)).toEqual(['goingAway']);
      expect(codeOf(serverDone.value)).toBe('goingAway');
      await clock.elapseAsync(5 * 60_000);
      expect(endpoint.attempts).toHaveLength(1);
      expect(clock.pendingCount).toBe(0);
      if (closing !== undefined) {
        expect(closing.isDone).toBe(true);
        return;
      }
      client.start();
      await ManualClock.settle();
      expect(endpoint.attempts).toHaveLength(2);
      expect(client.state.phase).toBe(ClientPhase.connected);
      await closeAndCheck(clock, client);
    });
  }

  test('stop() during the attempt that follows it', async () => {
    const clock = new ManualClock();
    const endpoint = new ScriptedEndpoint(clock);
    const pending = new Deferred<FrameTransport>();
    let calls = 0;
    const client = clientFor(endpoint, {
      connect: (init) => {
        calls++;
        return calls === 1 ? endpoint.connect(init) : pending.promise;
      },
    });
    await ManualClock.settle();
    void endpoint.last.goAway();
    await ManualClock.settle();
    expect(calls).toBe(2);
    expect(client.state.phase).toBe(ClientPhase.connecting);
    client.stop();
    expect(client.state.phase).toBe(ClientPhase.disconnected);
    pending.resolve(endpoint.accept());
    const strayDone = new Outcome(endpoint.last.done);
    await clock.elapseAsync(5 * 60_000);
    expect(strayDone.isDone, 'late transport closed').toBe(true);
    expect(calls).toBe(2);
    expect(client.connection).toBeUndefined();
    expect(clock.pendingCount).toBe(0);
    await closeAndCheck(clock, client);
  });

  test('a failed attempt after it backs off as usual', async () => {
    const clock = new ManualClock();
    const endpoint = new ScriptedEndpoint(clock);
    const client = clientFor(endpoint);
    const opened: MuxChannel[] = [];
    client.openPersistent(events, (channel) => opened.push(channel));
    await clock.elapseAsync(1000);
    endpoint.failures = 2;
    void endpoint.last.goAway();
    await ManualClock.settle();
    expect(endpoint.attempts).toEqual([0, 1000]);
    expect(client.state.phase).toBe(ClientPhase.disconnected);
    expect(codeOf(client.state.lastStatus)).toBe('unavailable');
    expect(client.state.attempt).toBe(1);
    // Waits for the next connection.
    const waiting = new Outcome(client.openChannel(feed));
    await clock.elapseAsync(499);
    expect(endpoint.attempts).toHaveLength(2);
    await clock.elapseAsync(1);
    expect(endpoint.attempts).toEqual([0, 1000, 1500]);
    expect(waiting.isDone).toBe(false);
    await clock.elapseAsync(1000);
    expect(endpoint.attempts).toEqual([0, 1000, 1500, 2500]);
    expect(client.state.phase).toBe(ClientPhase.connected);
    expect(client.state.attempt).toBe(3);
    expect(waiting.value?.connection).toBe(client.connection);
    expect(opened).toHaveLength(2);
    expect(opened[1]?.connection).toBe(client.connection);
    await closeAndCheck(clock, client, true);
  });

  test('again soon after reconnecting at once: backs off', async () => {
    const clock = new ManualClock();
    const endpoint = new ScriptedEndpoint(clock);
    const client = clientFor(endpoint, { maxBackoff: 8000 });
    await clock.elapseAsync(1000);
    void endpoint.last.goAway();
    await ManualClock.settle();
    expect(endpoint.attempts).toEqual([0, 1000]);
    // The new connection goes away too before lasting maxBackoff.
    await clock.elapseAsync(1000);
    void endpoint.last.goAway();
    await ManualClock.settle();
    expect(client.state.phase).toBe(ClientPhase.disconnected);
    expect(codeOf(client.state.lastStatus)).toBe('goingAway');
    await clock.elapseAsync(499);
    expect(endpoint.attempts).toHaveLength(2);
    await clock.elapseAsync(1);
    expect(endpoint.attempts).toEqual([0, 1000, 2500]);
    expect(client.state.phase).toBe(ClientPhase.connected);
    // A connection that lasted maxBackoff makes the next GOAWAY a planned
    // restart again.
    await clock.elapseAsync(10_000);
    void endpoint.last.goAway();
    await ManualClock.settle();
    expect(endpoint.attempts).toEqual([0, 1000, 2500, 12_500]);
    expect(client.state.phase).toBe(ClientPhase.connected);
    await closeAndCheck(clock, client);
  });
});

describe('next attempt', () => {
  test('nextAttemptAt matches the backoff schedule', async () => {
    const clock = new ManualClock();
    const endpoint = new ScriptedEndpoint(clock);
    endpoint.refuseAll = true;
    const client = clientFor(endpoint, { maxBackoff: 4000, jitter: 0.2, random: seeded(3) });
    const states = record(client);
    await clock.elapseAsync(20_000);
    const waits = states.filter((s) => s.phase === ClientPhase.disconnected);
    const times = endpoint.attempts;
    expect(times.length).toBeGreaterThan(5);
    expect(waits).toHaveLength(times.length);
    for (let i = 0; i + 1 < times.length; i++) {
      const wait = waits[i] as ClientState;
      expect(wait.nextAttemptAt).toBeDefined();
      expect(wait.nextAttemptAt).toBeCloseTo(times[i + 1] as number, 6);
      expect((wait.nextAttemptAt as number) - (wait.since as number)).toBeCloseTo(
        (times[i + 1] as number) - (times[i] as number),
        6,
      );
    }
    expect(client.nextAttemptAt).toBe(client.state.nextAttemptAt);
    expect(client.nextAttemptAt).toBeDefined();
    for (const s of states.filter((s) => s.phase !== ClientPhase.disconnected)) {
      expect(s.nextAttemptAt).toBeUndefined();
    }
    await closeAndCheck(clock, client);
    expect(client.nextAttemptAt).toBeUndefined();
  });

  test('undefined while connecting, connected or stopped', async () => {
    const clock = new ManualClock();
    const endpoint = new ScriptedEndpoint(clock);
    const client = clientFor(endpoint);
    expect(client.state.phase).toBe(ClientPhase.connecting);
    expect(client.nextAttemptAt).toBeUndefined();
    expect(client.state.retryIn).toBeUndefined();
    await ManualClock.settle();
    expect(client.state.phase).toBe(ClientPhase.connected);
    expect(client.nextAttemptAt).toBeUndefined();

    // After a loss: the next attempt after the backoff.
    const states = changes(client);
    void endpoint.last.close();
    await ManualClock.settle();
    const waiting = client.state;
    expect(waiting.phase).toBe(ClientPhase.disconnected);
    expect((waiting.nextAttemptAt as number) - (waiting.since as number)).toBe(500);
    expect(waiting.retryIn).toBe(500);
    await clock.elapseAsync(200);
    expect(waiting.retryIn).toBe(300);
    await clock.elapseAsync(300);
    expect(client.state.phase).toBe(ClientPhase.connected);
    expect(waiting.retryIn).toBe(0);
    expect(states.map((s) => s.phase)).toEqual(['disconnected', 'connecting', 'connected']);
    expect(states.map((s) => s.nextAttemptAt !== undefined)).toEqual([true, false, false]);

    client.stop();
    expect(client.state.phase).toBe(ClientPhase.disconnected);
    expect(client.nextAttemptAt).toBeUndefined();
    expect(client.state.retryIn).toBeUndefined();
    await closeAndCheck(clock, client);
  });

  test('retryIn counts down to zero, never below', () => {
    const clock = new ManualClock(1000);
    expect(new ClientState(ClientPhase.disconnected, { clock }).retryIn).toBeUndefined();
    const due = new ClientState(ClientPhase.disconnected, {
      since: 1000,
      nextAttemptAt: 61_000,
      clock,
    });
    expect(due.retryIn).toBe(60_000);
    clock.advance(10_000);
    expect(due.retryIn).toBe(50_000);
    const past = new ClientState(ClientPhase.disconnected, {
      since: 0,
      nextAttemptAt: 1000,
      clock,
    });
    expect(past.retryIn).toBe(0);
    expect(past.toString()).toContain('next attempt at');
    expect(Object.isFrozen(past)).toBe(true);
  });
});

describe('reconnectNow', () => {
  test('during the backoff: attempts at once', async () => {
    const clock = new ManualClock();
    const endpoint = new ScriptedEndpoint(clock);
    endpoint.refuseAll = true;
    const client = clientFor(endpoint);
    const states = record(client);
    await ManualClock.settle();
    expect(client.state.phase).toBe(ClientPhase.disconnected);
    expect(client.nextAttemptAt).toBeDefined();

    await clock.elapseAsync(100);
    client.reconnectNow();
    expect(endpoint.attempts).toEqual([0, 100]);
    expect(client.state.phase).toBe(ClientPhase.connecting);
    expect(client.state.attempt).toBe(2);
    expect(client.nextAttemptAt).toBeUndefined();
    await ManualClock.settle();
    // Failed again: the backoff carries on, 1 s rather than 500 ms.
    expect(client.state.phase).toBe(ClientPhase.disconnected);
    expect((client.nextAttemptAt as number) - (client.state.since as number)).toBe(1000);
    await clock.elapseAsync(999);
    expect(endpoint.attempts).toHaveLength(2);
    await clock.elapseAsync(1);
    expect(endpoint.attempts).toEqual([0, 100, 1100]);

    // Waiting 2 s now; the endpoint is back.
    endpoint.refuseAll = false;
    await clock.elapseAsync(100);
    expect(client.state.phase).toBe(ClientPhase.disconnected);
    client.reconnectNow();
    await ManualClock.settle();
    expect(endpoint.attempts).toEqual([0, 100, 1100, 1200]);
    expect(client.state.phase).toBe(ClientPhase.connected);
    expect(client.state.attempt).toBe(4);
    expect(states.map((s) => s.phase)).toEqual([
      'connecting',
      'disconnected',
      'connecting',
      'disconnected',
      'connecting',
      'disconnected',
      'connecting',
      'connected',
    ]);
    expect(states.map((s) => s.nextAttemptAt !== undefined)).toEqual(
      states.map((s) => s.phase === ClientPhase.disconnected),
    );
    // The cancelled wait does not fire.
    await clock.elapseAsync(5 * 60_000);
    expect(endpoint.attempts).toHaveLength(4);
    expect(client.state.phase).toBe(ClientPhase.connected);
    expect(endpoint.connections).toHaveLength(1);
    await closeAndCheck(clock, client);
  });

  test('while connecting or connected: does nothing', async () => {
    const clock = new ManualClock();
    const endpoint = new ScriptedEndpoint(clock);
    const pending = new Deferred<FrameTransport>();
    let calls = 0;
    const client = clientFor(endpoint, {
      connect: () => {
        calls++;
        return pending.promise;
      },
    });
    const states = changes(client);
    await clock.elapseAsync(100);
    client.reconnectNow();
    client.reconnectNow({ force: true });
    await ManualClock.settle();
    expect(calls).toBe(1);
    expect(client.state.phase).toBe(ClientPhase.connecting);
    expect(client.state.attempt).toBe(1);
    expect(states).toEqual([]);

    pending.resolve(endpoint.accept());
    await ManualClock.settle();
    expect(client.state.phase).toBe(ClientPhase.connected);
    const connection = client.connection;
    client.reconnectNow();
    await clock.elapseAsync(60_000);
    expect(calls).toBe(1);
    expect(client.connection).toBe(connection);
    expect(endpoint.last.peerGoingAway).toBe(false);
    expect(states.map((s) => s.phase)).toEqual(['connected']);
    await closeAndCheck(clock, client);
  });

  test('force while connected: moves to a new connection', async () => {
    const clock = new ManualClock();
    const endpoint = new ScriptedEndpoint(clock);
    const subscriptions = subscriptionsAt(endpoint);
    const client = clientFor(endpoint);
    const opened: MuxChannel[] = [];
    const closed: Status[] = [];
    const persistent = client.openPersistent(events, (channel) => opened.push(channel), {
      onClosed: (status) => closed.push(status),
    });
    const talkClosed: Status[] = [];
    const talk = client.openPersistentTalk(feed, (talk) => talk.request('SUB'), {
      onClosed: (status) => talkClosed.push(status),
    });
    const own = new Outcome(client.openChannel(events));
    await clock.elapseAsync(1000);
    expect(subscriptions.count).toBe(1);
    const firstConnection = client.connection as MuxConnection;
    const firstConnected = client.connected;
    const server = endpoint.last;
    const mine = own.value as MuxChannel;
    const theirs = (endpoint.on(server).at(-1) as IncomingChannel).channel;
    const states = changes(client);
    const firstDone = new Outcome(firstConnection.done);
    const serverDone = new Outcome(server.done);

    client.reconnectNow({ force: true });
    expect(client.state.phase).toBe(ClientPhase.connecting);
    expect(client.connection).toBeUndefined();
    await ManualClock.settle();

    expect(endpoint.attempts).toEqual([0, 1000]);
    expect(states.map((s) => s.phase)).toEqual(['connecting', 'connected']);
    expect(states.map((s) => codeOf(s.lastStatus))).toEqual(['goingAway', 'goingAway']);
    expect(states.map((s) => s.attempt)).toEqual([1, 1]);
    const second = client.connection as MuxConnection;
    expect(second).not.toBe(firstConnection);
    expect(client.connected).not.toBe(firstConnected);
    // The old connection got GOAWAY; the persistent channels on it were
    // closed with GOING_AWAY and opened again on the new one.
    expect(server.peerGoingAway).toBe(true);
    expect(closed.map(codeOf)).toEqual(['goingAway']);
    expect(talkClosed.map(codeOf)).toEqual(['goingAway']);
    expect(opened).toHaveLength(2);
    expect(opened[1]?.connection).toBe(second);
    expect(persistent.current).toBe(opened[1]);
    expect(talk.current?.isOpen).toBe(true);
    expect(subscriptions.count).toBe(2);
    expect(
      endpoint
        .on(endpoint.last)
        .map((c) => c.address.type?.toString())
        .sort(),
    ).toEqual(['events', 'feed']);

    // The application channel carries on over the old connection.
    expect(firstDone.isDone).toBe(false);
    expect(mine.state).toBe('open');
    const received = frames(mine);
    void theirs.send(bytes(1));
    await ManualClock.settle();
    expect(received).toEqual([[1]]);
    // Once it is done, the old connection closes, and nothing reconnects.
    void mine.close();
    await ManualClock.settle();
    expect(codeOf(firstDone.value)).toBe('goingAway');
    expect(codeOf(serverDone.value)).toBe('goingAway');
    await clock.elapseAsync(5 * 60_000);
    expect(endpoint.attempts).toHaveLength(2);
    expect(client.connection).toBe(second);
    expect(states).toHaveLength(2);
    expect(opened).toHaveLength(2);
    await closeAndCheck(clock, client);
  });

  test('force: a failed attempt backs off as usual', async () => {
    const clock = new ManualClock();
    const endpoint = new ScriptedEndpoint(clock);
    const client = clientFor(endpoint);
    const opened: MuxChannel[] = [];
    client.openPersistent(events, (channel) => opened.push(channel));
    await clock.elapseAsync(1000);
    endpoint.failures = 1;
    client.reconnectNow({ force: true });
    await ManualClock.settle();
    expect(endpoint.attempts).toEqual([0, 1000]);
    expect(client.state.phase).toBe(ClientPhase.disconnected);
    expect(codeOf(client.state.lastStatus)).toBe('unavailable');
    expect((client.nextAttemptAt as number) - (client.state.since as number)).toBe(500);
    await clock.elapseAsync(500);
    expect(endpoint.attempts).toEqual([0, 1000, 1500]);
    expect(client.state.phase).toBe(ClientPhase.connected);
    expect(opened).toHaveLength(2);
    expect(opened[1]?.connection).toBe(client.connection);
    await closeAndCheck(clock, client);
  });

  test('not started, stopped or closed: does nothing', async () => {
    const clock = new ManualClock();
    const endpoint = new ScriptedEndpoint(clock);
    const client = clientFor(endpoint, { autoStart: false });
    client.reconnectNow();
    client.reconnectNow({ force: true });
    await clock.elapseAsync(5000);
    expect(endpoint.attempts).toEqual([]);
    expect(client.isRunning).toBe(false);

    client.start();
    await ManualClock.settle();
    expect(client.state.phase).toBe(ClientPhase.connected);
    client.stop();
    client.reconnectNow();
    client.reconnectNow({ force: true });
    await clock.elapseAsync(5000);
    expect(endpoint.attempts).toHaveLength(1);
    expect(client.state.phase).toBe(ClientPhase.disconnected);
    expect(clock.pendingCount).toBe(0);

    // Closed while waiting for the next attempt.
    endpoint.refuseAll = true;
    client.start();
    await ManualClock.settle();
    expect(client.nextAttemptAt).toBeDefined();
    await closeAndCheck(clock, client);
    const states = record(client);
    client.reconnectNow();
    client.reconnectNow({ force: true });
    await clock.elapseAsync(5000);
    expect(endpoint.attempts).toHaveLength(2);
    expect(client.state.phase).toBe(ClientPhase.disconnected);
    expect(codeOf(client.state.lastStatus)).toBe('cancelled');
    expect(client.nextAttemptAt).toBeUndefined();
    expect(states.map((s) => s.phase)).toEqual(['disconnected']);
    expect(clock.pendingCount).toBe(0);
  });
});

describe('the endpoint', () => {
  test('pushed channels arrive on incoming across reconnects', async () => {
    const clock = new ManualClock();
    const endpoint = new ScriptedEndpoint(clock);
    const client = clientFor(endpoint);
    const pushed: IncomingChannel[] = [];
    void (async () => {
      for await (const incoming of client.incoming) {
        pushed.push(incoming);
        if (incoming.address.type === undefined) {
          void incoming.talk().serve((m) => void m.reply(bytes(3)));
        }
      }
    })();
    await ManualClock.settle();
    const push = new ChannelAddress({ type: 'notify', payload: bytes(1) });
    void endpoint.last.open(push.encode()).send(bytes(42));
    await ManualClock.settle();
    expect(pushed).toHaveLength(1);
    expect(pushed[0]?.address.type?.equals(Name.of('notify'))).toBe(true);
    expect([...(pushed[0]?.address.payload ?? [])]).toEqual([1]);
    expect(pushed[0]?.connection).toBe(client.connection);
    const data = frames((pushed[0] as IncomingChannel).channel);
    await ManualClock.settle();
    expect(data).toEqual([[42]]);

    // A malformed OPEN payload is refused, not delivered.
    const bad = endpoint.last.open(bytes(0xf0));
    const badDone = new Outcome(bad.done);
    await ManualClock.settle();
    expect(codeOf(badDone.value)).toBe('protocolError');
    expect(pushed).toHaveLength(1);
    // So is a bulk payload with no Talk channel to take it.
    const bulk = endpoint.last.open(new TalkBulkOpen(4242, 1).encode());
    const bulkDone = new Outcome(bulk.done);
    await ManualClock.settle();
    expect(codeOf(bulkDone.value)).toBe('failedPrecondition');
    expect(pushed).toHaveLength(1);

    void endpoint.last.close();
    await clock.elapseAsync(500);
    endpoint.last.open(push.encode());
    await ManualClock.settle();
    expect(pushed).toHaveLength(2);
    expect(pushed[1]?.connection).toBe(client.connection);

    // A Talk exchange the endpoint starts, on the default address.
    const talk = new TalkChannel(endpoint.last.open(new Uint8Array(0)));
    const reply = new Outcome(talk.request('ASK'));
    await ManualClock.settle();
    expect(pushed).toHaveLength(3);
    expect(pushed[2]?.address.type).toBeUndefined();
    expect(pushed[2]?.remote).toBeUndefined();
    expect([...(reply.value?.payload ?? [])]).toEqual([3]);
    await closeAndCheck(clock, client, true);
  });

  test('pushed channels are refused once the consumer stops', async () => {
    const clock = new ManualClock();
    const endpoint = new ScriptedEndpoint(clock);
    const client = clientFor(endpoint);
    const pushed: IncomingChannel[] = [];
    const stop = new Deferred<void>();
    void (async () => {
      for await (const incoming of client.incoming) {
        pushed.push(incoming);
        await stop.promise;
        break;
      }
    })();
    await ManualClock.settle();
    endpoint.last.open(new Uint8Array(0));
    await ManualClock.settle();
    expect(pushed).toHaveLength(1);
    // Buffered while the consumer is busy, refused once it stops.
    const buffered = new Outcome(endpoint.last.open(new Uint8Array(0)).done);
    await ManualClock.settle();
    expect(buffered.isDone).toBe(false);
    stop.resolve();
    await ManualClock.settle();
    expect(codeOf(buffered.value)).toBe('unavailable');
    const refused = new Outcome(endpoint.last.open(new Uint8Array(0)).done);
    await ManualClock.settle();
    expect(codeOf(refused.value)).toBe('unavailable');
    expect(pushed).toHaveLength(1);
    void pushed[0]?.reject(Status.ok);
    await closeAndCheck(clock, client);
  });

  test('after close, incoming ends and pushed channels are refused', async () => {
    const clock = new ManualClock();
    const endpoint = new ScriptedEndpoint(clock);
    const client = clientFor(endpoint);
    await ManualClock.settle();
    const server = endpoint.last;
    const unread = new Outcome(server.open(new Uint8Array(0)).done);
    await ManualClock.settle();
    await closeAndCheck(clock, client);
    expect(codeOf(unread.value)).toBe('unavailable');
    const seen: IncomingChannel[] = [];
    for await (const incoming of client.incoming) seen.push(incoming);
    expect(seen).toEqual([]);
  });
});
