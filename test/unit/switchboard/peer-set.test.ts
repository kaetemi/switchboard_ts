/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// PeerSet: a consumer kept connected to every instance of a type, over
// mem:// workers and a StaticResolver (no naming service): membership (the
// initial records, the events around them, the removal hold-down), online
// and offline events, a worker's reboot, the per-peer channel and its hooks
// (a peer is online only once both succeeded, on every connection and on
// every re-open), endpoint fallback within the connect timeout, the backoff
// schedule (a stepping clock), the outgoing policy and the per-worker
// credential, resolver errors, and close. The consumer side of the
// untrusted worker fleet (wiki page "Polyverse Switchboard Use Cases",
// entry 5). A port of the Dart peer_set_test.dart, over the toy mux and,
// from `over-frame-mux/`, over the frame mux.

import { beforeEach, describe, expect, test } from 'vitest';
import { ChannelAddress, ServiceAddress } from '../../../src/core/address/index.ts';
import { bytesEqual, utf8Decode } from '../../../src/core/bytes.ts';
import type { MuxChannel, MuxConnection, MuxOptions } from '../../../src/core/mux/contract.ts';
import { Name } from '../../../src/core/name.ts';
import { downEvent, serviceRecord, upEvent } from '../../../src/core/naming/naming-protocol.ts';
import { Status, StatusCode, statusCodeName } from '../../../src/core/status.ts';
import { ChannelPolicies } from '../../../src/core/switchboard/channel-policy.ts';
import type {
  ChannelPolicy,
  IncomingChannel,
  Resolver,
  ServiceEvent,
  ServiceRecord,
} from '../../../src/core/switchboard/contract.ts';
import {
  type Peer,
  type PeerEvent,
  PeerEventType,
  PeerSet,
  type PeerSetOptions,
  PeerState,
} from '../../../src/core/switchboard/peer-set.ts';
import { StaticResolver } from '../../../src/core/switchboard/resolver.ts';
import type { SwitchboardRuntime } from '../../../src/core/switchboard/runtime.ts';
import { Switchboard, type SwitchboardOptions } from '../../../src/core/switchboard/switchboard.ts';
import { TalkChannel } from '../../../src/core/talk/index.ts';
import type { FrameTransport } from '../../../src/core/transport/contract.ts';
import { memoryTransportPair } from '../../../src/core/transport/memory-transport.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';
import type { Logger, LogLevel } from '../../../src/core/util/logger.ts';
import { ManualClock } from '../../../src/testing/manual-clock.ts';
import {
  bytes,
  currentRuntime,
  fast,
  node,
  onCleanup,
  rejectsWith,
} from '../../support/node/harness.ts';

/** Waits up to 10 s for the channels on a connection it leaves. */
const patient: Partial<MuxOptions> = Object.freeze({
  goAwayGrace: 10_000,
  keepAliveInterval: undefined,
});

/** How long the real-time waits of these tests may take. */
const limit = 5000;

const gpu = Name.of('gpu');
const back = Name.of('back');
const meshSecret = bytes('mesh-secret');
const gpuChannel = new ChannelAddress({ type: gpu });

/** The well-known name of [status]'s code. */
function code(status: Status | undefined): string | undefined {
  return status === undefined ? undefined : statusCodeName(status.code);
}

/** Whether [needle] occurs anywhere in [haystack]. */
function containsBytes(haystack: Uint8Array, needle: Uint8Array): boolean {
  for (let i = 0; i + needle.length <= haystack.length; i++) {
    if (bytesEqual(haystack.subarray(i, i + needle.length), needle)) {
      return true;
    }
  }
  return false;
}

/** Polls [condition] every few milliseconds; fails after {@link limit}. */
async function eventually(condition: () => boolean, what = 'condition'): Promise<void> {
  const deadline = Date.now() + limit;
  while (!condition()) {
    if (Date.now() > deadline) {
      throw new Error(`${what} not met within ${limit} ms`);
    }
    await delay(2);
  }
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** [promise], or a failure after {@link limit} (Dart `timeout(limit)`). */
async function within<T>(promise: Promise<T>): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`not settled within ${limit} ms`)), limit);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

/** Sends the request [procedure] with [question] on [talk] and returns the answer as text. */
async function ask(talk: TalkChannel, question: string, procedure = 'ECHO'): Promise<string> {
  return (await talk.request(procedure, question)).text();
}

/** The single element of [items]. */
function only<T>(items: Iterable<T>): T {
  const all = [...items];
  expect(all).toHaveLength(1);
  return all[0] as T;
}

/** The peer [instance] of [set], which must exist. */
function peerOf(set: PeerSet, instance: number): Peer {
  const peer = set.peers.get(instance);
  expect(peer, `peer ${instance}`).toBeDefined();
  return peer as Peer;
}

/** Whether the peer [instance] of [set] exists and is online. */
function isOnline(set: PeerSet, instance = 1): boolean {
  return set.peers.get(instance)?.isOnline ?? false;
}

/** A runtime of the current test's `mem://` registry whose transport pairs run on [clock]. */
function steppingRuntime(clock: ManualClock): SwitchboardRuntime {
  return { ...currentRuntime(), transportPair: () => memoryTransportPair({ delay: 0, clock }) };
}

/**
 * A node whose timers, connections and transports run on [clock], the
 * replacement for a node inside Dart's `fakeAsync`. Not closed by the
 * harness: the test closes it and checks that no timer is left.
 */
function steppingNode(clock: ManualClock, options: SwitchboardOptions = {}): Switchboard {
  return new Switchboard({
    runtime: steppingRuntime(clock),
    clock,
    ...options,
    muxOptions: { ...fast, ...options.muxOptions },
  });
}

/**
 * Lets [closing] finish on [clock] (one second of it) and checks that
 * nothing is left: Dart's `async.elapse(1 s)` then `pendingTimers` empty.
 */
async function closeAll(clock: ManualClock, closing: Promise<unknown>[]): Promise<void> {
  let closed = false;
  void Promise.all(closing).then(() => {
    closed = true;
  });
  await clock.elapseAsync(1000);
  expect(closed, 'everything closed').toBe(true);
  expect(clock.pendingCount, 'no timer left').toBe(0);
}

let names = 0;

/**
 * A worker: a node listening at a fixed mem:// name, serving `gpu` under
 * whatever instance it is addressed as (a registrar registered it), with a
 * Talk echo `<id>:<payload>`. It records every OPEN payload as received.
 */
class Worker {
  readonly id: number;
  readonly name: string;
  readonly #mux: Partial<MuxOptions>;
  readonly #clock: ManualClock | undefined;
  sb: Switchboard | undefined;
  readonly raw: Uint8Array[] = [];
  readonly accepted: MuxConnection[] = [];

  /** Rejects the channels it receives with this status instead of serving them. */
  reject: Status | undefined;

  /** Handles the channels it receives instead of serving them, if set. */
  handler: ((incoming: IncomingChannel) => void) | undefined;

  /** A worker of the current test; with [init.clock], its node runs on that clock. */
  constructor(id: number, init: { mux?: Partial<MuxOptions>; clock?: ManualClock } = {}) {
    this.id = id;
    this.name = `worker-${id}-${names++}-${Math.floor(Math.random() * 2 ** 30)}`;
    this.#mux = init.mux ?? fast;
    this.#clock = init.clock;
  }

  get uri(): string {
    return `mem://${this.name}`;
  }

  /** The application payloads received, as text. */
  get payloads(): string[] {
    return this.raw.map((open) => utf8Decode(ChannelAddress.decode(open).payload));
  }

  record(init: { metadata?: Uint8Array; endpoints?: string[] } = {}): ServiceRecord {
    return serviceRecord(new ServiceAddress(gpu, this.id), {
      endpoints: init.endpoints ?? [this.uri],
      metadata: init.metadata,
    });
  }

  async start(): Promise<void> {
    const clock = this.#clock;
    const sb =
      clock === undefined
        ? node({ muxOptions: this.#mux })
        : steppingNode(clock, { muxOptions: this.#mux });
    this.sb = sb;
    sb.registerService(gpu, (incoming) => this.#serve(incoming), { acceptAnyInstance: true });
    void (async () => {
      for await (const connection of sb.connections) {
        this.accepted.push(connection);
      }
    })();
    await sb.listenMemory({ name: this.name, policy: ChannelPolicies.allowTypes([gpu]) });
  }

  async stop(): Promise<void> {
    await this.sb?.close();
    this.sb = undefined;
  }

  #serve(incoming: IncomingChannel): void {
    this.raw.push(incoming.channel.openPayload);
    const handle = this.handler;
    if (handle !== undefined) {
      handle(incoming);
      return;
    }
    const status = this.reject;
    if (status !== undefined) {
      void incoming.reject(status);
      return;
    }
    void incoming.talk().serve((message) => {
      if (message.expectsReply) {
        void message.reply(`${this.id}:${message.text()}`);
      }
    });
  }

  /** The connection accepted last. */
  get last(): MuxConnection {
    return this.accepted.at(-1) as MuxConnection;
  }

  /**
   * Sends GOAWAY GOING_AWAY on the last connection accepted, which stays up
   * for the channels on it (at most the grace of its mux options); resolves
   * once it has closed.
   */
  goAway(): Promise<void> {
    return this.last.goAway(Status.of(StatusCode.goingAway, 'worker rebooting'));
  }

  /** Pushes a channel to `back` into the consumer on the last connection accepted, and returns how it ended. */
  push(): Promise<Status> {
    return (this.sb as Switchboard).openChannelOn(this.last, new ChannelAddress({ type: back }))
      .done;
  }
}

/** A resolver the test drives: ready when [readyNow] resolves, answering `resolve` with [answer] ([called] resolves when it is asked), and its events from [feed]. */
class ControlledResolver implements Resolver {
  readonly readyNow = new Deferred<void>();
  readonly answer = new Deferred<ServiceRecord[]>();
  readonly called = new Deferred<void>();
  readonly feed = new EventFeed();

  get ready(): Promise<void> {
    return this.readyNow.promise;
  }

  get events(): AsyncIterable<ServiceEvent> {
    return this.feed;
  }

  resolve(): Promise<ServiceRecord[]> {
    this.called.resolve();
    return this.answer.promise;
  }

  async close(): Promise<void> {
    this.feed.end();
  }
}

type FeedItem = { readonly event: ServiceEvent } | { readonly error: unknown };

/**
 * Resolver events for one consumer, with error events as a Dart stream
 * has them: a `next()` that rejects with the error, after which the
 * iteration goes on.
 */
class EventFeed implements AsyncIterable<ServiceEvent> {
  readonly #items: FeedItem[] = [];
  #waiting: Deferred<IteratorResult<ServiceEvent>> | undefined;
  #ended = false;

  add(event: ServiceEvent): void {
    this.#deliver({ event });
  }

  addError(error: unknown): void {
    this.#deliver({ error });
  }

  end(): void {
    this.#ended = true;
    const waiting = this.#waiting;
    this.#waiting = undefined;
    waiting?.resolve({ value: undefined, done: true });
  }

  #deliver(item: FeedItem): void {
    if (this.#ended) {
      return;
    }
    const waiting = this.#waiting;
    if (waiting === undefined) {
      this.#items.push(item);
      return;
    }
    this.#waiting = undefined;
    EventFeed.#settle(waiting, item);
  }

  static #settle(deferred: Deferred<IteratorResult<ServiceEvent>>, item: FeedItem): void {
    if ('event' in item) {
      deferred.resolve({ value: item.event, done: false });
    } else {
      deferred.reject(item.error);
    }
  }

  [Symbol.asyncIterator](): AsyncIterator<ServiceEvent> {
    return {
      next: () => {
        const item = this.#items.shift();
        const deferred = new Deferred<IteratorResult<ServiceEvent>>();
        if (item !== undefined) {
          EventFeed.#settle(deferred, item);
        } else if (this.#ended) {
          deferred.resolve({ value: undefined, done: true });
        } else {
          this.#waiting = deferred;
        }
        return deferred.promise;
      },
      return: () => {
        this.end();
        return Promise.resolve({ value: undefined, done: true });
      },
    };
  }
}

/** A resolver that never becomes ready. */
class NeverReady implements Resolver {
  readonly ready = new Promise<void>(() => {});
  readonly events: AsyncIterable<ServiceEvent> = {
    [Symbol.asyncIterator]: () => ({
      next: () => Promise.resolve({ value: undefined, done: true }),
    }),
  };

  async resolve(): Promise<ServiceRecord[]> {
    return [];
  }

  async close(): Promise<void> {}
}

/** A logger that keeps what it receives. */
class Recorder implements Logger {
  readonly records: {
    level: LogLevel;
    name: string;
    message: string;
    error: unknown;
  }[] = [];

  log(level: LogLevel, name: string, message: string, error?: unknown): void {
    this.records.push({ level, name, message, error });
  }
}

let resolver: StaticResolver;
let consumer: Switchboard;
let log: string[];

/** A worker of the current test, started, stopped after it. */
async function worker(id: number): Promise<Worker> {
  const w = new Worker(id);
  await w.start();
  onCleanup(() => w.stop());
  return w;
}

/**
 * The consumer: its default payload is the mesh credential, and each
 * worker's key is in its record's metadata.
 */
function consumerNode(
  init: {
    outgoingPolicy?: ChannelPolicy;
    connectTimeout?: number;
    runtime?: SwitchboardRuntime;
  } = {},
): Switchboard {
  const sb = node({
    defaultPayload: meshSecret,
    connectTimeout: init.connectTimeout ?? 10_000,
    outgoingPolicy: init.outgoingPolicy,
    credentialFor: (_endpoint, record) =>
      record !== undefined && record.metadata.length > 0 ? record.metadata : undefined,
    ...(init.runtime === undefined ? {} : { runtime: init.runtime }),
  });
  sb.registerService(back, (incoming) => {
    void incoming.channel.send(bytes('consumer'));
    void incoming.channel.close();
  });
  return sb;
}

function keyed(w: Worker): ServiceRecord {
  return w.record({ metadata: bytes(`key-${w.id}`) });
}

/** The set the tests watch with, over the current consumer, closed after the test; its events go to `log`. */
function watch(options: PeerSetOptions & { from?: Resolver } = {}): PeerSet {
  const set = PeerSet.watch(consumer, gpu, {
    initialBackoff: 10,
    maxBackoff: 80,
    jitter: 0,
    connectTimeout: 1000,
    removalHoldDown: 0,
    ...options,
    resolver: options.from ?? resolver,
  });
  onCleanup(() => set.close());
  set.on('event', (event) => log.push(`${event.type} ${event.peer.address.instance}`));
  return set;
}

beforeEach(() => {
  resolver = new StaticResolver();
  onCleanup(() => resolver.close());
  consumer = consumerNode();
  log = [];
});

test('added, online, offline, online after a reboot, removed', async () => {
  const w = await worker(1);
  resolver.add(keyed(w));
  const set = watch();
  await set.ready;
  const peer = peerOf(set, 1);
  expect(peer.address.equals(new ServiceAddress(gpu, 1))).toBe(true);
  await eventually(() => peer.isOnline, 'online');
  expect(peer.endpoint).toBe(w.uri);
  expect(peer.connection).toBeDefined();
  expect(set.online).toHaveLength(1);
  expect(set.online[0]).toBe(peer);
  // The worker reboots: it goes away, and is back at the same address.
  await w.stop();
  await eventually(() => peer.state === PeerState.offline, 'offline');
  expect(peer.connection).toBeUndefined();
  expect(peer.lastStatus).toBeDefined();
  expect(peer.nextAttemptAt).toBeDefined();
  expect(set.online).toEqual([]);
  await w.start();
  await eventually(() => peer.isOnline, 'online again');
  resolver.remove(new ServiceAddress(gpu, 1));
  await eventually(() => peer.state === PeerState.removed, 'removed');
  expect(set.peers.size).toBe(0);
  await eventually(() => log.length === 5);
  expect(log).toEqual(['added 1', 'online 1', 'offline 1', 'online 1', 'removed 1']);
  // Removed: the worker's side of the connection ends (GOAWAY).
  await within(w.last.done);
});

test('the initial records and later ones; one connection each, its own', async () => {
  const a = await worker(1);
  const b = await worker(2);
  resolver.add(keyed(a));
  const set = watch();
  await set.ready;
  expect([...set.peers.keys()]).toEqual([1]);
  resolver.add(keyed(b));
  await eventually(() => set.online.length === 2, 'both online');
  expect([...set.peers.keys()]).toEqual([1, 2]);
  expect(a.accepted).toHaveLength(1);
  expect(b.accepted).toHaveLength(1);
  // Not the node's pooled connections: a reboot disturbs nobody else.
  const pooled = await consumer.connect(a.uri);
  expect(pooled).not.toBe(peerOf(set, 1).connection);
  expect(log.indexOf('added 1')).toBeGreaterThanOrEqual(0);
  expect(log.indexOf('added 2')).toBeGreaterThan(log.indexOf('added 1'));
});

test('onConnect, the per-peer channel and onOpen, on every connection', async () => {
  const w = await worker(1);
  resolver.add(keyed(w));
  const hooks: string[] = [];
  const ref: { set?: PeerSet } = {};
  const set = watch({
    onConnect: async (peer) => {
      const self = ref.set as PeerSet;
      hooks.push(`connect ${peer.state} ${peer.connection !== undefined}`);
      // The connection is usable for a handshake already.
      const talk = await self.openTalk(peer.address.instance);
      hooks.push(await ask(talk, 'hi', 'HELLO'));
      await talk.close();
    },
    channel: gpuChannel,
    onOpen: async (_peer, channel) => {
      const talk = new TalkChannel(channel);
      hooks.push(`open ${await ask(talk, 'caps', 'CAPS')}`);
    },
  });
  ref.set = set;
  await eventually(() => isOnline(set), 'online');
  expect(hooks).toEqual(['connect connecting true', '1:hi', 'open 1:caps']);
  // Online only once the hooks are done.
  await eventually(() => log.includes('online 1'));
  const channel = peerOf(set, 1).channel as MuxChannel;
  expect(ChannelAddress.decode(channel.openPayload).instance).toBe(1);
  // A reboot: everything again, on the new connection.
  hooks.length = 0;
  await w.stop();
  await eventually(() => !isOnline(set));
  await w.start();
  await eventually(() => isOnline(set), 'online again');
  expect(hooks).toEqual(['connect connecting true', '1:hi', 'open 1:caps']);
  expect(peerOf(set, 1).channel).not.toBe(channel);
  expect(log).toEqual(['added 1', 'online 1', 'offline 1', 'online 1']);
});

test('the per-peer channel is opened again when it ends on a live connection; onOpen runs again', async () => {
  const w = await worker(1);
  resolver.add(keyed(w));
  const opened: MuxChannel[] = [];
  const set = watch({
    channel: gpuChannel,
    onOpen: (_peer, channel) => {
      opened.push(channel);
    },
  });
  await eventually(() => isOnline(set), 'online');
  const connection = peerOf(set, 1).connection;
  // The worker closes it (not a refusal): offline while it is closed,
  // opened again on the same connection, online again.
  await eventually(() => w.accepted.length > 0);
  const served = only(only(w.accepted).channels);
  await served.close(Status.of(StatusCode.unavailable, 'restarting'));
  await eventually(() => opened.length === 2, 'opened again');
  await eventually(() => isOnline(set), 'online again');
  expect(peerOf(set, 1).connection).toBe(connection);
  expect(peerOf(set, 1).channel).toBe(opened[1]);
  expect(w.accepted).toHaveLength(1);
  expect(log).toEqual(['added 1', 'online 1', 'offline 1', 'online 1']);
});

test('a re-open the worker refuses: offline on the same connection, no onOpen, opened again after the backoff', async () => {
  const w = await worker(1);
  resolver.add(keyed(w));
  const opened: MuxChannel[] = [];
  const set = watch({
    channel: gpuChannel,
    onOpen: (_peer, channel) => {
      opened.push(channel);
    },
  });
  await eventually(() => isOnline(set), 'online');
  const peer = peerOf(set, 1);
  const connection = peer.connection;
  // The worker closes the channel and refuses the next ones on arrival.
  w.reject = Status.of(StatusCode.unavailable, 'busy');
  await only(only(w.accepted).channels).close(Status.of(StatusCode.unavailable, 'restarting'));
  await eventually(() => w.raw.length >= 4, 'opened again and refused');
  expect(peer.isOnline).toBe(false);
  expect(set.online).toEqual([]);
  expect(code(peer.lastStatus)).toBe('unavailable');
  expect(peer.connection).toBe(connection);
  // onOpen never ran on a refused channel.
  expect(opened).toHaveLength(1);
  w.reject = undefined;
  await eventually(() => peer.isOnline, 'online once accepted');
  expect(opened).toHaveLength(2);
  expect(peer.channel).toBe(opened[1]);
  expect(peer.connection).toBe(connection);
  expect(w.accepted).toHaveLength(1);
  expect(log).toEqual(['added 1', 'online 1', 'offline 1', 'online 1']);
});

test('onOpen on a re-open is bounded by connectTimeout', async () => {
  const w = await worker(1);
  resolver.add(keyed(w));
  let calls = 0;
  const set = watch({
    connectTimeout: 100,
    channel: gpuChannel,
    // The second never completes.
    onOpen: () => (++calls === 2 ? new Promise<void>(() => {}) : undefined),
  });
  await eventually(() => isOnline(set), 'online');
  const peer = peerOf(set, 1);
  const connection = peer.connection;
  await only(only(w.accepted).channels).close(Status.of(StatusCode.unavailable, 'restarting'));
  await eventually(() => calls === 3 && peer.isOnline, 'online after the third');
  expect(w.raw).toHaveLength(3);
  expect(peer.connection).toBe(connection);
  expect(log).toEqual(['added 1', 'online 1', 'offline 1', 'online 1']);
});

test('a refused per-peer channel takes the peer offline and is retried after the backoff', async () => {
  const w = await worker(1);
  w.reject = Status.of(StatusCode.permissionDenied, 'bad key');
  resolver.add(keyed(w));
  const set = watch({ channel: gpuChannel });
  await eventually(() => w.raw.length >= 3, 'retried');
  const peer = peerOf(set, 1);
  expect(peer.isOnline).toBe(false);
  expect(code(peer.lastStatus)).toBe('permissionDenied');
  expect(log).toEqual(['added 1', 'offline 1']);
  // Each attempt was a new connection, left with GOAWAY.
  expect(w.accepted.length).toBeGreaterThanOrEqual(3);
  w.reject = undefined;
  await eventually(() => peer.isOnline, 'online once accepted');
  expect(log).toEqual(['added 1', 'offline 1', 'online 1']);
});

test('openChannel and openTalk on a peer', async () => {
  const w = await worker(1);
  resolver.add(keyed(w));
  const set = watch();
  await eventually(() => isOnline(set), 'online');
  const talk = await set.openTalk(1);
  expect(await ask(talk, 'x')).toBe('1:x');
  await talk.close();
  const explicit = await set.openTalk(1, { payload: bytes('given') });
  await explicit.request('ECHO');
  await explicit.close();
  // openChannel, raw.
  const channel = await set.openChannel(1);
  expect(await ask(new TalkChannel(channel), 'y')).toBe('1:y');
  await channel.close();
  expect(w.payloads).toEqual(['key-1', 'given', 'key-1']);
  const header = ChannelAddress.decode(w.raw[0] as Uint8Array);
  expect(header.address?.equals(new ServiceAddress(gpu, 1))).toBe(true);
  await rejectsWith(set.openChannel(9), StatusCode.notFound);
  await rejectsWith(set.openTalk(9), StatusCode.notFound);
  await w.stop();
  await eventually(() => !isOnline(set));
  await rejectsWith(set.openChannel(1), StatusCode.failedPrecondition);
  await rejectsWith(set.openTalk(1), StatusCode.failedPrecondition);
});

test('credentials pending during an endpoint change stay at their destination', async () => {
  const a = await worker(1);
  const b = await worker(1);
  const credential = new Deferred<Uint8Array | undefined>();
  const requested = new Deferred<void>();
  const endpoints: string[] = [];
  consumer = node({
    credentialFor: (endpoint) => {
      requested.resolve();
      endpoints.push(endpoint);
      return credential.promise;
    },
  });
  resolver.add(a.record());
  const set = watch();
  await eventually(() => set.online.length === 1);
  const opening = set.openChannel(1);
  const refused = rejectsWith(opening, StatusCode.failedPrecondition);
  await requested.promise;
  resolver.add(b.record());
  await eventually(() => peerOf(set, 1).endpoint === b.uri);
  credential.resolve(bytes('only-for-a'));
  await refused;
  expect(b.payloads).toEqual([]);
  expect(endpoints).toEqual([a.uri]);
});

test('credentials: each worker sees its own key, never the mesh credential', async () => {
  const ws: Worker[] = [];
  for (let id = 1; id <= 4; id++) {
    ws.push(await worker(id));
  }
  for (const w of ws) {
    resolver.add(keyed(w));
  }
  const set = watch({ channel: gpuChannel });
  await eventually(() => set.online.length === 4, 'all online');
  for (const w of ws) {
    const talk = await set.openTalk(w.id);
    expect(await ask(talk, 'echo')).toBe(`${w.id}:echo`);
    await talk.close();
  }
  for (const w of ws) {
    expect(w.payloads, w.name).toEqual([`key-${w.id}`, `key-${w.id}`]);
    for (const open of w.raw) {
      expect(containsBytes(open, meshSecret)).toBe(false);
    }
  }
});

test('outgoing policy: a worker may not push channels into the consumer', async () => {
  const w = await worker(1);
  resolver.add(keyed(w));
  // No policy anywhere: the push is dispatched.
  const open = watch();
  await eventually(() => isOnline(open), 'online');
  expect((await w.push()).isOk).toBe(true);
  await open.close();
  // The set's policy.
  const strict = watch({ policy: ChannelPolicies.denyAll });
  await eventually(() => isOnline(strict), 'online');
  const refused = await w.push();
  expect(code(refused)).toBe('permissionDenied');
  expect(refused.reason).toBe('permission denied');
  await strict.close();
  // The node's outgoing policy.
  consumer = consumerNode({ outgoingPolicy: ChannelPolicies.denyAll });
  const byNode = watch();
  await eventually(() => isOnline(byNode), 'online');
  expect(code(await w.push())).toBe('permissionDenied');
});

test('a record that changes: updated; other endpoints move the peer', async () => {
  const w = await worker(1);
  resolver.add(keyed(w));
  const set = watch();
  await eventually(() => isOnline(set), 'online');
  const peer = peerOf(set, 1);
  const first = peer.connection as MuxConnection;
  // Other metadata, same endpoints: the connection stays.
  resolver.add(w.record({ metadata: bytes('key-1b') }));
  await eventually(() => log.includes('updated 1'));
  expect(peer.connection).toBe(first);
  expect(utf8Decode(peer.record.metadata)).toBe('key-1b');
  // The worker moves to another address.
  const moved = await worker(1);
  resolver.add(moved.record({ metadata: bytes('key-1b') }));
  await eventually(() => peer.isOnline && peer.endpoint === moved.uri, 'moved and online');
  expect(peer.connection).not.toBe(first);
  await within(first.done);
  expect(log).toEqual(['added 1', 'online 1', 'updated 1', 'updated 1', 'offline 1', 'online 1']);
});

test('endpoints are tried in order; the first that answers', async () => {
  const w = await worker(1);
  resolver.add(w.record({ endpoints: [`mem://nobody-${names++}`, w.uri] }));
  const set = watch();
  await eventually(() => isOnline(set), 'online');
  expect(peerOf(set, 1).endpoint).toBe(w.uri);
  expect(peerOf(set, 1).attempt).toBe(1);
});

test('an endpoint that never answers leaves time for the next', async () => {
  // A WebSocket dialer that never answers: a dial to it hangs until the
  // node's connect timeout, like a dial to an unroutable address.
  const runtime: SwitchboardRuntime = {
    ...currentRuntime(),
    dialers: { ws: () => new Promise<FrameTransport>(() => {}) },
  };
  const w = await worker(1);
  resolver.add(w.record({ metadata: bytes('key-1'), endpoints: ['ws://127.0.0.1:9/', w.uri] }));
  // Equal bounds for the node and the set: the first endpoint must not
  // take all of it.
  consumer = consumerNode({ connectTimeout: 400, runtime });
  const set = watch({ connectTimeout: 400 });
  await eventually(() => isOnline(set), 'online');
  const peer = peerOf(set, 1);
  expect(peer.endpoint).toBe(w.uri);
  expect(peer.attempt).toBe(1);
  expect(log).toEqual(['added 1', 'online 1']);
});

test('a hook that throws, fails or hangs fails the attempt: GOAWAY, offline, retried after the backoff', async () => {
  const w = await worker(1);
  resolver.add(keyed(w));
  const outcomes: (() => unknown)[] = [
    () => {
      throw new Error('broken hook');
    },
    () => Promise.reject(new Error('broken hook')),
    () => new Promise<void>(() => {}),
  ];
  let calls = 0;
  const statuses: string[] = [];
  const set = watch({
    connectTimeout: 100,
    onConnect: (peer) => {
      const status = peer.lastStatus;
      if (status !== undefined) {
        statuses.push(status.reason);
      }
      return calls < outcomes.length ? outcomes[calls++]?.() : undefined;
    },
  });
  await eventually(() => isOnline(set), 'online');
  expect(calls).toBe(3);
  expect(statuses).toEqual(['onConnect failed', 'onConnect failed', 'onConnect timed out']);
  expect(log).toEqual(['added 1', 'offline 1', 'online 1']);
  // Each failed attempt was a connection of its own, left with GOAWAY.
  expect(w.accepted).toHaveLength(4);
  for (const connection of w.accepted.slice(0, 3)) {
    await within(connection.done);
  }
});

describe('not online until the per-peer channel and onOpen succeeded', () => {
  /**
   * A worker (instance 7) whose gpu channels [init.handler] handles,
   * watched with [init.onOpen]: the peer must stay offline with [expected],
   * retried, and onOpen must never run on a channel the worker refused.
   */
  async function staysOffline(
    expected: StatusCode,
    init: {
      handler?: (incoming: IncomingChannel) => void;
      reject?: Status;
      onOpen?: (peer: Peer, channel: MuxChannel) => unknown;
    },
  ): Promise<void> {
    const w = await worker(7);
    w.handler = init.handler;
    w.reject = init.reject;
    resolver.add(keyed(w));
    let onOpenCalls = 0;
    const set = watch({
      channel: gpuChannel,
      onOpen: (peer, channel) => {
        onOpenCalls++;
        return init.onOpen?.(peer, channel);
      },
    });
    await eventually(() => w.accepted.length >= 3, 'retried');
    const peer = peerOf(set, 7);
    expect(peer.isOnline).toBe(false);
    expect(set.online).toEqual([]);
    expect(code(peer.lastStatus)).toBe(statusCodeName(expected));
    expect(log).toEqual(['added 7', 'offline 7']);
    // Each attempt was a connection of its own, left with GOAWAY.
    for (const connection of w.accepted.slice(0, 2)) {
      await within(connection.done);
    }
    if (init.onOpen === undefined) {
      expect(onOpenCalls).toBe(0);
    } else {
      expect(onOpenCalls).toBeGreaterThanOrEqual(2);
    }
  }

  test("the worker's handler throws", async () => {
    await staysOffline(StatusCode.internal, {
      handler: () => {
        throw new Error('boom');
      },
    });
  });

  test('the worker rejects the channel with UNAVAILABLE', async () => {
    await staysOffline(StatusCode.unavailable, {
      reject: Status.of(StatusCode.unavailable, 'not yet'),
    });
  });

  test('onOpen throws', async () => {
    await staysOffline(StatusCode.internal, {
      onOpen: () => {
        throw new Error('caps failed');
      },
    });
  });

  test("onOpen's promise rejects", async () => {
    await staysOffline(StatusCode.internal, {
      onOpen: async () => {
        throw new Error('caps failed');
      },
    });
  });
});

test('events before resolve are dropped; those while it answers are folded into its answer', async () => {
  const late = new ControlledResolver();
  onCleanup(() => late.close());
  const set = watch({ from: late });
  const seen: string[] = [];
  set.on('event', (event) => {
    if (event.type !== PeerEventType.online && event.type !== PeerEventType.offline) {
      seen.push(
        `${event.type} ${event.peer.address.instance} ${event.peer.record.endpoints.join(',')}`,
      );
    }
  });
  const a = 'mem://nobody-a';
  const b = 'mem://nobody-b';
  const c = 'mem://nobody-c';
  const d = 'mem://nobody-d';
  const at = (id: number, endpoint: string): ServiceRecord =>
    serviceRecord(new ServiceAddress(gpu, id), { endpoints: [endpoint] });
  const up = (id: number, endpoint: string): ServiceEvent => upEvent(at(id, endpoint));
  const down = (id: number): ServiceEvent => downEvent(new ServiceAddress(gpu, id));
  // Before resolve is called: its answer has them, they are dropped. The
  // events of an async iterable arrive over a few turns, where the Dart
  // test's synchronous stream delivers them at once: let them arrive.
  late.feed.add(up(5, a));
  late.feed.add(down(5));
  late.feed.add(up(6, a));
  late.feed.add(up(6, b));
  await delay(5);
  late.readyNow.resolve();
  await late.called.promise;
  // While it answers: folded into the answer.
  late.feed.add(up(7, c));
  late.feed.add(up(6, b));
  late.feed.add(up(9, c));
  late.feed.add(down(9));
  late.feed.add(down(8));
  await delay(5);
  late.answer.resolve([at(6, b), at(8, d)]);
  await set.ready;
  expect([...set.peers.keys()]).toEqual([6, 7]);
  // Let anything still on its way arrive.
  await delay(20);
  expect(seen).toEqual([`added 6 ${b}`, `added 7 ${c}`]);
  expect([...set.peers.keys()]).toEqual([6, 7]);
  // From then on, as they come.
  late.feed.add(down(7));
  await eventually(() => !set.peers.has(7), 'removed');
  expect(seen.at(-1)).toBe(`removed 7 ${c}`);
});

test("an error on the resolver's events is logged; the set goes on", async () => {
  const logger = new Recorder();
  const source = new ControlledResolver();
  onCleanup(() => source.close());
  source.readyNow.resolve();
  source.answer.resolve([]);
  const set = watch({ from: source, logger });
  await set.ready;
  source.feed.addError(new Error('naming hiccup'));
  const w = await worker(1);
  source.feed.add(upEvent(keyed(w)));
  await eventually(() => isOnline(set), 'online');
  expect(
    logger.records.filter(
      (r) =>
        r.level === 'warn' &&
        r.name === 'Switchboard.Client' &&
        r.message.includes('resolver event error') &&
        r.error instanceof Error &&
        r.error.message === 'naming hiccup',
    ),
  ).toHaveLength(1);
  expect(set.isClosed).toBe(false);
});

test('warnings: no policy for the connections; the default payload to every peer', async () => {
  const logger = new Recorder();
  const warnings = (): string[] =>
    logger.records
      .filter((r) => r.level === 'warn' && r.message.startsWith('peer set'))
      .map((r) => r.message);
  const plain = node({ defaultPayload: meshSecret });
  const careless = PeerSet.watch(plain, gpu, { resolver, logger });
  onCleanup(() => careless.close());
  expect(warnings()).toHaveLength(2);
  expect(warnings()[0]).toContain('no policy');
  expect(warnings()[1]).toContain('credentialFor');
  logger.records.length = 0;
  // A policy, and a credential per peer: nothing to say.
  watch({ policy: ChannelPolicies.denyAll, logger });
  expect(warnings()).toEqual([]);
});

describe('removal hold-down', () => {
  test('a record back within the hold-down: adopted again, connection and channel kept', async () => {
    const w = await worker(1);
    resolver.add(keyed(w));
    const opened: MuxChannel[] = [];
    const set = watch({
      channel: gpuChannel,
      onOpen: (_peer, channel) => {
        opened.push(channel);
      },
      removalHoldDown: 5000,
    });
    await eventually(() => isOnline(set), 'online');
    const peer = peerOf(set, 1);
    const connection = peer.connection;
    const channel = peer.channel;
    resolver.remove(new ServiceAddress(gpu, 1));
    await eventually(() => log.includes('held 1'), 'held');
    expect(peer.state).toBe(PeerState.held);
    expect(peer.isOnline).toBe(false);
    expect(set.online).toEqual([]);
    expect(set.peers.get(1)).toBe(peer);
    // Still usable for the jobs in flight, and for new ones.
    const talk = await set.openTalk(1);
    expect(await ask(talk, 'x')).toBe('1:x');
    await talk.close();
    resolver.add(keyed(w));
    await eventually(() => peer.isOnline, 'online again');
    expect(peer.connection).toBe(connection);
    expect(peer.channel).toBe(channel);
    expect(opened).toHaveLength(1);
    // No GOAWAY, no reconnect.
    expect(w.accepted).toHaveLength(1);
    expect(only(w.accepted).peerGoingAway).toBe(false);
    expect(log).toEqual(['added 1', 'online 1', 'held 1', 'online 1']);
  });

  test('a record back after the hold-down: removed, then added', async () => {
    const w = await worker(1);
    resolver.add(keyed(w));
    const set = watch({ removalHoldDown: 50 });
    await eventually(() => isOnline(set), 'online');
    const peer = peerOf(set, 1);
    resolver.remove(new ServiceAddress(gpu, 1));
    await eventually(() => peer.state === PeerState.removed, 'removed');
    expect(set.peers.size).toBe(0);
    await within(only(w.accepted).done);
    resolver.add(keyed(w));
    await eventually(() => isOnline(set), 'online again');
    expect(set.peers.get(1)).not.toBe(peer);
    expect(w.accepted).toHaveLength(2);
    expect(log).toEqual(['added 1', 'online 1', 'held 1', 'removed 1', 'added 1', 'online 1']);
  });

  test('a record back elsewhere: the held peer is removed, a new one added', async () => {
    const w = await worker(1);
    resolver.add(keyed(w));
    const set = watch({ removalHoldDown: 5000 });
    await eventually(() => isOnline(set), 'online');
    const peer = peerOf(set, 1);
    resolver.remove(new ServiceAddress(gpu, 1));
    await eventually(() => log.includes('held 1'), 'held');
    const moved = await worker(1);
    resolver.add(moved.record({ metadata: bytes('key-1') }));
    await eventually(() => isOnline(set), 'online');
    expect(peer.state).toBe(PeerState.removed);
    expect(peerOf(set, 1).endpoint).toBe(moved.uri);
    await within(only(w.accepted).done);
    expect(log).toEqual(['added 1', 'online 1', 'held 1', 'removed 1', 'added 1', 'online 1']);
  });
});

test('close: every peer removed, its connection closed, events end', async () => {
  const a = await worker(1);
  const b = await worker(2);
  resolver.add(keyed(a));
  resolver.add(keyed(b));
  resolver.add(
    serviceRecord(new ServiceAddress(gpu, 3), { endpoints: [`mem://nobody-${names++}`] }),
  );
  const set = watch({ channel: gpuChannel });
  const iterated: PeerEvent[] = [];
  const ended = new Deferred<void>();
  void (async () => {
    for await (const event of set.events) {
      iterated.push(event);
    }
    ended.resolve();
  })();
  await eventually(() => set.online.length === 2, 'online');
  await eventually(() => set.peers.get(3)?.state === PeerState.offline);
  const peers = [...set.peers.values()];
  await set.close();
  await within(ended.promise);
  expect(set.isClosed).toBe(true);
  expect(set.peers.size).toBe(0);
  for (const peer of peers) {
    expect(peer.state).toBe(PeerState.removed);
  }
  expect(log.filter((e) => e.startsWith('removed'))).toHaveLength(3);
  const removed = iterated.filter((event) => event.type === PeerEventType.removed);
  expect(removed.map((event) => code(event.status))).toEqual([
    'cancelled',
    'cancelled',
    'cancelled',
  ]);
  for (const w of [a, b]) {
    await within(only(w.accepted).done);
  }
  await rejectsWith(set.openChannel(1), StatusCode.failedPrecondition);
  // Membership no longer followed.
  resolver.add(serviceRecord(new ServiceAddress(gpu, 4)));
  await delay(10);
  expect(set.peers.size).toBe(0);
});

test('ready fails with CANCELLED if closed first; argument checks', async () => {
  const slow = new NeverReady();
  const set = PeerSet.watch(consumer, gpu, { resolver: slow });
  await set.close();
  await rejectsWith(set.ready, StatusCode.cancelled);
  const bare = node();
  expect(() => PeerSet.watch(bare, gpu)).toThrow(TypeError);
  expect(() => PeerSet.watch(consumer, gpu, { resolver, onOpen: () => {} })).toThrow(TypeError);
  expect(() => PeerSet.watch(consumer, gpu, { resolver, backoffFactor: 0.5 })).toThrow(RangeError);
  expect(() => PeerSet.watch(consumer, gpu, { resolver, removalHoldDown: -1 })).toThrow(RangeError);
});

describe('timing (stepping clock)', () => {
  test('a hanging credential fails setup within connectTimeout', async () => {
    const clock = new ManualClock();
    const resolver = new StaticResolver();
    const credential = new Deferred<Uint8Array | undefined>();
    const consumer = steppingNode(clock, { credentialFor: () => credential.promise });
    const w = new Worker(1, { clock });
    void w.start();
    await ManualClock.settle();
    resolver.add(w.record());
    const set = PeerSet.watch(consumer, gpu, {
      resolver,
      channel: gpuChannel,
      connectTimeout: 300,
      initialBackoff: 500,
      jitter: 0,
      clock,
    });
    await clock.elapseAsync(299);
    const peer = peerOf(set, 1);
    expect(peer.state).toBe(PeerState.connecting);
    await clock.elapseAsync(2);
    expect(peer.state).toBe(PeerState.offline);
    expect(code(peer.lastStatus)).toBe('internal');
    expect(peer.connection).toBeUndefined();
    // A credential delivered after its bound cannot open a channel.
    credential.resolve(bytes('late'));
    await ManualClock.settle();
    expect(w.raw).toEqual([]);
    await closeAll(clock, [set.close(), w.stop(), consumer.close(), resolver.close()]);
  });

  for (const hook of ['onConnect', 'onOpen'] as const) {
    test(`${hook} closing the set leaves no hook timeout behind`, async () => {
      const clock = new ManualClock();
      const resolver = new StaticResolver();
      const consumer = steppingNode(clock);
      const w = new Worker(1, { clock });
      void w.start();
      await ManualClock.settle();
      resolver.add(w.record());
      const ref: { set?: PeerSet } = {};
      const closeInHook = (): Promise<void> => {
        void ref.set?.close();
        return new Promise<void>(() => {});
      };
      const set = PeerSet.watch(consumer, gpu, {
        resolver,
        connectTimeout: 10_000,
        clock,
        onConnect: hook === 'onConnect' ? () => closeInHook() : undefined,
        channel: hook === 'onOpen' ? gpuChannel : undefined,
        onOpen: hook === 'onOpen' ? () => closeInHook() : undefined,
      });
      ref.set = set;
      await clock.elapseAsync(1000);
      expect(set.isClosed).toBe(true);
      await closeAll(clock, [set.close(), w.stop(), consumer.close(), resolver.close()]);
    });
  }

  test('the backoff schedule, a lasting connection resets it, close leaves no timer', async () => {
    const clock = new ManualClock();
    const resolver = new StaticResolver();
    const consumer = steppingNode(clock);
    const w = new Worker(1, { clock });
    resolver.add(w.record());
    const set = PeerSet.watch(consumer, gpu, {
      resolver,
      initialBackoff: 500,
      maxBackoff: 4000,
      jitter: 0,
      clock,
    });
    const events: string[] = [];
    set.on('event', (e) => events.push(`${e.type} ${clock.now()}`));
    expect(set.peers.size).toBe(0);
    await ManualClock.settle();
    // Attempt times: the attempt counter, sampled every 10 ms.
    const attempts: number[] = [];
    let seen = 0;
    for (let t = 0; t <= 12_000; t += 10) {
      const current = peerOf(set, 1).attempt;
      if (current > seen) {
        attempts.push(t);
        seen = current;
      }
      await clock.elapseAsync(10);
    }
    expect(attempts).toEqual([0, 500, 1500, 3500, 7500, 11_500]);
    expect(events).toEqual(['added 0', 'offline 0']);
    // The worker comes up: the next attempt (4 s later) connects.
    void w.start();
    await clock.elapseAsync(4000);
    expect(peerOf(set, 1).isOnline).toBe(true);
    expect(events.at(-1)).toBe('online 15500');
    // A connection that lasts beyond maxBackoff resets the schedule: after
    // the worker goes away, an attempt at once (it fails, the worker is
    // down), then the first delay again, 500 ms.
    await clock.elapseAsync(5000);
    void w.stop();
    await clock.elapseAsync(200);
    expect(peerOf(set, 1).state).toBe(PeerState.offline);
    expect(peerOf(set, 1).attempt).toBe(1);
    expect(events.at(-1)).toBe('offline 21010');
    void w.start();
    await clock.elapseAsync(299);
    expect(peerOf(set, 1).isOnline).toBe(false);
    await clock.elapseAsync(2);
    expect(peerOf(set, 1).isOnline).toBe(true);
    expect(events.at(-1)).toBe('online 21510');
    // Close: nothing left running.
    await closeAll(clock, [set.close(), w.stop(), consumer.close(), resolver.close()]);
    expect(events.at(-1)).toMatch(/^removed/);
  });

  test('a GOAWAY: reconnected at once, not after the backoff', async () => {
    const clock = new ManualClock();
    const resolver = new StaticResolver();
    const consumer = steppingNode(clock);
    const a = new Worker(1, { clock });
    const b = new Worker(1, { clock });
    void a.start();
    void b.start();
    await ManualClock.settle();
    // Two endpoints: the first goes away, the second answers at once.
    resolver.add(a.record({ endpoints: [a.uri, b.uri] }));
    const set = PeerSet.watch(consumer, gpu, {
      resolver,
      initialBackoff: 500,
      jitter: 0,
      clock,
    });
    await clock.elapseAsync(10);
    expect(peerOf(set, 1).endpoint).toBe(a.uri);
    void a.stop();
    await clock.elapseAsync(10);
    expect(peerOf(set, 1).isOnline).toBe(true);
    expect(peerOf(set, 1).endpoint).toBe(b.uri);
    await closeAll(clock, [set.close(), b.stop(), consumer.close(), resolver.close()]);
  });

  test('a hanging hook fails the attempt after connectTimeout', async () => {
    const clock = new ManualClock();
    const resolver = new StaticResolver();
    const consumer = steppingNode(clock);
    const w = new Worker(1, { clock });
    void w.start();
    await ManualClock.settle();
    resolver.add(w.record());
    let calls = 0;
    const set = PeerSet.watch(consumer, gpu, {
      resolver,
      connectTimeout: 300,
      initialBackoff: 500,
      jitter: 0,
      clock,
      onConnect: () => {
        calls++;
        return new Promise<void>(() => {});
      },
    });
    await clock.elapseAsync(299);
    const peer = peerOf(set, 1);
    expect(peer.state).toBe(PeerState.connecting);
    expect(peer.connection).toBeDefined();
    await clock.elapseAsync(2);
    expect(peer.state).toBe(PeerState.offline);
    expect(code(peer.lastStatus)).toBe('internal');
    expect(peer.lastStatus?.reason).toBe('onConnect timed out');
    expect(peer.connection).toBeUndefined();
    expect(peer.attempt).toBe(1);
    // Left with GOAWAY; the next attempt after the backoff.
    await clock.elapseAsync(200);
    expect(only(w.accepted).isOpen).toBe(false);
    expect(calls).toBe(1);
    await clock.elapseAsync(300);
    expect(peer.state).toBe(PeerState.connecting);
    expect(peer.attempt).toBe(2);
    expect(calls).toBe(2);
    await closeAll(clock, [set.close(), w.stop(), consumer.close(), resolver.close()]);
  });
});

describe("a worker's GOAWAY on a live connection (stepping clock)", () => {
  function describeEvent(e: PeerEvent): string {
    return `${e.type}${e.status === undefined ? '' : ` ${code(e.status)}`}`;
  }

  test('offline with its status, online at once on a new connection with the per-peer channel opened there; the old one drains', async () => {
    const clock = new ManualClock();
    const resolver = new StaticResolver();
    const consumer = steppingNode(clock);
    // It waits for the channels on the connection it leaves.
    const w = new Worker(1, { clock, mux: patient });
    void w.start();
    await ManualClock.settle();
    resolver.add(w.record());
    const opened: MuxChannel[] = [];
    const set = PeerSet.watch(consumer, gpu, {
      resolver,
      channel: gpuChannel,
      onOpen: (_peer, channel) => {
        opened.push(channel);
      },
      initialBackoff: 500,
      maxBackoff: 4000,
      jitter: 0,
      clock,
    });
    const events: string[] = [];
    set.on('event', (e) => events.push(describeEvent(e)));
    await clock.elapseAsync(10);
    const peer = peerOf(set, 1);
    expect(peer.isOnline).toBe(true);
    const first = peer.connection as MuxConnection;
    let firstClosed = false;
    void first.done.then(() => {
      firstClosed = true;
    });
    let firstChannelEnd: Status | undefined;
    void (peer.channel as MuxChannel).done.then((s) => {
      firstChannelEnd = s;
    });
    // An application channel on the first connection.
    let app: TalkChannel | undefined;
    void set.openTalk(1).then((talk) => {
      app = talk;
    });
    await clock.elapseAsync(10);
    const answers: string[] = [];
    const question = (text: string): void => {
      void ask(app as TalkChannel, text, 'Q').then((answer) => answers.push(answer));
    };
    question('before');
    await clock.elapseAsync(10);
    expect(answers).toEqual(['1:before']);
    // The worker goes away, its connection staying up meanwhile.
    let workerLeft = false;
    void w.last.done.then(() => {
      workerLeft = true;
    });
    void w.goAway();
    await clock.elapseAsync(10);
    // At once (the backoff is 500 ms), on a new connection.
    expect(peer.isOnline).toBe(true);
    expect(peer.connection).not.toBe(first);
    expect(code(peer.lastStatus)).toBe('goingAway');
    expect(peer.lastStatus?.reason).toBe('worker rebooting');
    expect(w.accepted).toHaveLength(2);
    expect(events).toEqual(['added', 'online', 'offline goingAway', 'online']);
    // The per-peer channel was closed GOING_AWAY and opened again on the
    // new connection, onOpen with it.
    expect(code(firstChannelEnd)).toBe('goingAway');
    expect(opened).toHaveLength(2);
    expect(peer.channel).toBe(opened[1]);
    expect([...w.last.channels]).toHaveLength(1);
    // The old connection is left to the application's channel.
    expect([...(w.accepted[0] as MuxConnection).channels]).toHaveLength(1);
    question('during');
    await clock.elapseAsync(10);
    expect(answers).toEqual(['1:before', '1:during']);
    expect(firstClosed).toBe(false);
    expect(workerLeft).toBe(false);
    // Once that channel is closed, it closes.
    void app?.close();
    await clock.elapseAsync(10);
    expect(firstClosed).toBe(true);
    expect(workerLeft).toBe(true);
    expect(peer.isOnline).toBe(true);
    expect(events).toHaveLength(4);
    await closeAll(clock, [set.close(), w.stop(), consumer.close(), resolver.close()]);
  });

  test('a second GOAWAY before the connection lasted maxBackoff: offline, the next attempt after the backoff; after a connection that lasted, at once again', async () => {
    const clock = new ManualClock();
    const resolver = new StaticResolver();
    const consumer = steppingNode(clock);
    const w = new Worker(1, { clock });
    void w.start();
    await ManualClock.settle();
    resolver.add(w.record());
    const set = PeerSet.watch(consumer, gpu, {
      resolver,
      channel: gpuChannel,
      initialBackoff: 500,
      maxBackoff: 4000,
      jitter: 0,
      clock,
    });
    const events: string[] = [];
    set.on('event', (e) => events.push(describeEvent(e)));
    await clock.elapseAsync(10);
    const peer = peerOf(set, 1);
    expect(peer.isOnline).toBe(true);
    // The first GOAWAY: online again at once.
    void w.goAway();
    await clock.elapseAsync(10);
    expect(peer.isOnline).toBe(true);
    expect(w.accepted).toHaveLength(2);
    // The second, 100 ms into that connection: the backoff applies.
    await clock.elapseAsync(100);
    void w.goAway();
    await clock.elapseAsync(10);
    expect(peer.state).toBe(PeerState.offline);
    expect(code(peer.lastStatus)).toBe('goingAway');
    expect(peer.connection).toBeUndefined();
    expect(peer.nextAttemptAt).toBeDefined();
    await clock.elapseAsync(480);
    expect(peer.state).toBe(PeerState.offline);
    expect(w.accepted).toHaveLength(2);
    await clock.elapseAsync(20);
    expect(peer.isOnline).toBe(true);
    expect(w.accepted).toHaveLength(3);
    // That connection lasts beyond maxBackoff: a GOAWAY on it reconnects
    // at once again.
    await clock.elapseAsync(5000);
    void w.goAway();
    await clock.elapseAsync(10);
    expect(peer.isOnline).toBe(true);
    expect(w.accepted).toHaveLength(4);
    expect(events).toEqual([
      'added',
      'online',
      'offline goingAway',
      'online',
      'offline goingAway',
      'online',
      'offline goingAway',
      'online',
    ]);
    await closeAll(clock, [set.close(), w.stop(), consumer.close(), resolver.close()]);
  });
});
