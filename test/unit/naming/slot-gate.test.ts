/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Port of the Dart `slot_gate_test.dart`: the slot gate state machine,
// queueing, forwarding, resuming and revoking slots, MOVED and RELOCATED
// fields, bounds, request gating. Incoming channels arrive over in-memory
// mux links; forwarding goes to a node on a `mem` listener (Dart: loopback
// TCP). Over the toy mux and, from `over-frame-mux/`, over the frame mux;
// the bulk case runs over the frame mux only (over the toy mux the payload
// goes inline). An error the gate let escape would be an unhandled
// rejection, which fails the run (Dart guards zones for it). Where Dart
// waits for the forwarding grace period or the idle forwarding channel in
// real time and a slow run could lose the race, the gate runs on a manual
// clock (its connections keep the system clock).

import { describe, expect, test } from 'vitest';
import { ChannelAddress, ServiceAddress } from '../../../src/core/address/index.ts';
import { bytesEqual, concatBytes, utf8Decode } from '../../../src/core/bytes.ts';
import { type MuxChannel, type MuxConnection, muxOptions } from '../../../src/core/mux/contract.ts';
import { Name } from '../../../src/core/name.ts';
import { NamingClient } from '../../../src/core/naming/naming-client.ts';
import {
  AssignResult,
  type SlotRequestContext,
} from '../../../src/core/naming/naming-client-slots.ts';
import {
  AssignRequest,
  DrainRequest,
  ForwardRequest,
  MovedStatus,
  ResumeRequest,
  SlotMode,
  serviceRecord,
} from '../../../src/core/naming/naming-protocol.ts';
import {
  type SlotDrainOptions,
  SlotGate,
  type SlotGateOptions,
  SlotGateState,
  SlotGates,
  SlotLifecycle,
  type SlotLoadOptions,
} from '../../../src/core/naming/slot-gate.ts';
import { Status, StatusCode, SwitchboardError, statusCodeName } from '../../../src/core/status.ts';
import type {
  ChannelHandler,
  IncomingChannel,
  ServiceRecord,
} from '../../../src/core/switchboard/contract.ts';
import { IncomingChannel as Incoming } from '../../../src/core/switchboard/incoming-channel.ts';
import { StaticResolver } from '../../../src/core/switchboard/resolver.ts';
import type { Switchboard } from '../../../src/core/switchboard/switchboard.ts';
import type { TalkMessage } from '../../../src/core/talk/contract.ts';
import { installBulkRouting, TalkChannel } from '../../../src/core/talk/index.ts';
import { memoryTransportPair } from '../../../src/core/transport/memory-transport.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';
import type { Logger, LogLevel } from '../../../src/core/util/logger.ts';
import { ManualClock } from '../../../src/testing/manual-clock.ts';
import { Connector, Harness } from '../../support/naming/harness.ts';
import { Frames, nextText } from '../../support/node/channels.ts';
import {
  bytes,
  node,
  onCleanup,
  overFrameMux,
  rejectsWith,
  testMux,
} from '../../support/node/harness.ts';

const kv = Name.of('kv');
const limit = 5000;

function text(data: Uint8Array): string {
  return utf8Decode(data);
}

/** Waits until [condition] holds; fails after [limit] milliseconds. */
async function until(condition: () => boolean): Promise<void> {
  const deadline = performance.now() + limit;
  while (!condition()) {
    if (performance.now() > deadline) {
      throw new Error(`condition not met within ${limit} ms`);
    }
    await new Promise((resolve) => setTimeout(resolve, 2));
  }
}

/** Lets pending microtasks and short timers run. */
function settle(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 5));
}

function expectCode(status: Status, code: StatusCode): void {
  expect(statusCodeName(status.code)).toBe(statusCodeName(code));
}

/** A MOVED status naming [owner] at [epoch] (0: unknown). */
function expectMoved(status: Status, owner = 0, epoch = 0): void {
  expectCode(status, StatusCode.moved);
  const fields = MovedStatus.fromStatus(status);
  expect([fields.owner, fields.epoch]).toEqual([owner, epoch]);
}

/** A RELOCATED status naming [owner] at [epoch]. */
function expectRelocated(status: Status, owner = 0, epoch = 0): void {
  expectCode(status, StatusCode.relocated);
  const fields = MovedStatus.fromStatus(status);
  expect([fields.owner, fields.epoch]).toEqual([owner, epoch]);
}

const levels: Record<LogLevel, number> = { debug: 0, info: 1, warn: 2, error: 3 };
let sinks: { readonly floor: number; readonly records: string[] }[] = [];

/** The router's logger: what the gates log, for {@link gateLogs}. */
const routerLog: Logger = {
  log(level, name, message) {
    if (name !== 'Switchboard.Router') return;
    for (const sink of sinks) {
      if (levels[level] >= sink.floor) sink.records.push(message);
    }
  },
};

/** The messages the gates log at [level] or above from now on, until the test ends. */
function gateLogs(level: LogLevel): string[] {
  const sink = { floor: levels[level], records: [] as string[] };
  sinks.push(sink);
  onCleanup(() => {
    sinks = sinks.filter((s) => s !== sink);
  });
  return sink.records;
}

/** Answers every subframe of [channel] with what [answer] makes of it. */
function answerAll(channel: MuxChannel, answer: (data: Uint8Array) => string): void {
  void (async () => {
    try {
      for await (const data of channel.readable) {
        void channel.send(bytes(answer(data))).catch(() => {});
      }
    } catch {
      // The channel ended.
    }
  })();
}

/** A scriptable lifecycle that records every call. */
class TestLifecycle extends SlotLifecycle {
  readonly log: string[] = [];
  loadGate: Deferred<void> | undefined;
  drainGate: Deferred<void> | undefined;
  unloadGate: Deferred<void> | undefined;
  loadError: unknown;
  result: AssignResult = AssignResult.holding;
  /** Served channels, by slot, in arrival order. */
  readonly served = new Map<number, IncomingChannel[]>();
  /** Called for every served channel, its result returned by `serve`; the default echoes `slot:data`. */
  onServe: ((channel: IncomingChannel, slot: number) => unknown) | undefined;
  /** The contexts of the latest load and drain. */
  loadContext: SlotRequestContext | undefined;
  drainContext: SlotRequestContext | undefined;

  override async load(slot: number, options: SlotLoadOptions): Promise<AssignResult> {
    this.log.push(
      `load ${slot} e${options.epoch} h${options.holder}${options.shared ? ' shared' : ''}`,
    );
    this.loadContext = options.context;
    await this.loadGate?.promise;
    if (this.loadError !== undefined) {
      throw this.loadError;
    }
    return this.result;
  }

  override async drain(slot: number, options: SlotDrainOptions): Promise<void> {
    this.log.push(`drain ${slot} e${options.epoch} to${options.to}`);
    this.drainContext = options.context;
    await this.drainGate?.promise;
  }

  override async unload(slot: number): Promise<void> {
    this.log.push(`unload ${slot}`);
    await this.unloadGate?.promise;
  }

  override serve(channel: IncomingChannel, slot: number): unknown {
    this.log.push(`serve ${slot} ${text(channel.address.payload)}`);
    const list = this.served.get(slot) ?? [];
    list.push(channel);
    this.served.set(slot, list);
    const hook = this.onServe;
    if (hook !== undefined) {
      return hook(channel, slot);
    }
    answerAll(channel.channel, (data) => `${slot}:${text(data)}`);
    return undefined;
  }
}

/** A request context that records what is declared through it. */
class RecordingContext implements SlotRequestContext {
  readonly type: Name;
  readonly slot: number;
  readonly epoch: number;
  readonly declared: [number | undefined, number | undefined][] = [];
  readonly cancelled = new Deferred<void>();
  readonly #abort = new AbortController();

  constructor(type: Name, slot: number, epoch: number) {
    this.type = type;
    this.slot = slot;
    this.epoch = epoch;
    void this.cancelled.promise.then(() => this.#abort.abort());
  }

  get isCancelled(): boolean {
    return this.cancelled.isSettled;
  }

  get onCancel(): Promise<void> {
    return this.cancelled.promise;
  }

  get signal(): AbortSignal {
    return this.#abort.signal;
  }

  extend(options: { deadline?: number; renew?: number } = {}): void {
    this.declared.push([options.deadline, options.renew]);
  }
}

/** Holds forwarding opens so their queue and cancellation can be inspected, or fails them before returning a promise. */
class DelayedResolver extends StaticResolver {
  readonly proceed = new Deferred<void>();
  /** `resolve` throws at once instead of returning a promise. */
  throwAtOnce = false;

  override resolve(type: Name): Promise<ServiceRecord[]> {
    if (this.throwAtOnce) {
      throw SwitchboardError.of(StatusCode.unavailable, 'resolver down');
    }
    return this.proceed.promise.then(() => super.resolve(type));
  }
}

function isPromiseLike(value: unknown): value is PromiseLike<unknown> {
  return typeof (value as { then?: unknown } | undefined)?.then === 'function';
}

/**
 * A peer linked to the gate's dispatch over memory, on the mux of the run;
 * bulk channels go to their parent's Talk layer, as a node routes them.
 */
class Peer {
  readonly client: MuxConnection;
  readonly server: MuxConnection;
  /** How many channels reached the gate's dispatch. */
  dispatched = 0;

  constructor(gate: SlotGate) {
    const [a, b] = memoryTransportPair({ delay: 0 });
    const quiet = muxOptions({ keepAliveInterval: undefined });
    const client = testMux()(a, quiet, true);
    const server = testMux()(b, quiet, false);
    this.client = client;
    this.server = server;
    installBulkRouting(client);
    installBulkRouting(server);
    void (async () => {
      for await (const channel of server.incoming) {
        const result = gate.handler(new Incoming(channel));
        this.dispatched++;
        if (isPromiseLike(result)) {
          result.then(
            () => {},
            () => {},
          );
        }
      }
    })();
    void (async () => {
      for await (const channel of client.incoming) {
        void channel.close(Status.of(StatusCode.notFound));
      }
    })();
  }

  open(init: { shard?: number; payload?: string; instance?: number } = {}): MuxChannel {
    return this.client.open(
      new ChannelAddress({
        type: kv,
        instance: init.instance ?? 1,
        shard: init.shard,
        payload: bytes(init.payload ?? 'cred'),
      }).encode(),
    );
  }

  /** Resolves once [count] channels in all reached the gate's dispatch (Dart waits a few ms). */
  arrived(count: number): Promise<void> {
    return until(() => this.dispatched >= count);
  }

  async close(): Promise<void> {
    await this.client.close();
    await this.server.close();
  }
}

/** Sends [message] and resolves with the first answer. */
async function ask(channel: MuxChannel, message: string): Promise<string> {
  const reader = channel.readable.getReader();
  try {
    void channel.send(bytes(message)).catch(() => {});
    const { value } = await reader.read();
    if (value === undefined) {
      throw new Error('the channel ended');
    }
    return text(value);
  } finally {
    reader.releaseLock();
  }
}

function assign(slot: number, init: { epoch?: number; holder?: number } = {}): AssignRequest {
  return new AssignRequest(kv, slot, {
    epoch: init.epoch ?? 1,
    holder: init.holder ?? 0,
    shared: false,
  });
}

function drain(slot: number, epoch: number, to: number): DrainRequest {
  return new DrainRequest(kv, slot, { epoch, to });
}

function forward(slot: number, epoch: number, to: number): ForwardRequest {
  return new ForwardRequest(kv, slot, { epoch, to });
}

function resume(slot: number, epoch: number): ResumeRequest {
  return new ResumeRequest(kv, slot, { epoch });
}

/** A naming client that never connects (the gate reads only its empty mirror). */
function offlineClient(): NamingClient {
  const c = new NamingClient(async () => {
    throw new Error('not connected');
  });
  onCleanup(() => c.close());
  return c;
}

/** A naming client of [h], started and synced, with a static space of 4 slots for `kv` and no capacity. */
async function mirroredClient(h: Harness): Promise<NamingClient> {
  const c = new NamingClient(new Connector(h).call);
  onCleanup(() => c.close());
  await c.start();
  await c.synced;
  await c.defineSlots(kv, { count: 4, mode: SlotMode.static, capacity: 0 });
  await until(() => c.slotTable(kv) !== undefined);
  return c;
}

let client: NamingClient;
let sb: Switchboard;
let target: Switchboard;
let targetUri: string;
let lifecycle: TestLifecycle;
let gate: SlotGate;
let peer: Peer;
/** Channels the target node (`kv/2`) received. */
let arrived: IncomingChannel[];

function newGate(
  options: Omit<Partial<SlotGateOptions>, 'lifecycle' | 'instance' | 'logger'> = {},
): SlotGate {
  lifecycle = new TestLifecycle();
  gate = new SlotGate(sb, client, kv, {
    lifecycle,
    forwardGrace: 5000,
    drainTimeout: limit,
    ...options,
    instance: 1,
    logger: routerLog,
  });
  peer = new Peer(gate);
  const created = { gate, peer };
  onCleanup(() => created.peer.close());
  // Closing cancels the timers of forwarding slots (forwardGrace).
  onCleanup(() => created.gate.close());
  return gate;
}

async function setUp(): Promise<void> {
  client = offlineClient();
  target = node();
  arrived = [];
  target.registerService(
    kv,
    (incoming) => {
      arrived.push(incoming);
      answerAll(incoming.channel, (data) => `to:${text(data)}`);
    },
    { instance: 2 },
  );
  targetUri = await target.listenMemory();
  const resolver = new StaticResolver([
    serviceRecord(new ServiceAddress(kv, 2), { endpoints: [targetUri] }),
  ]);
  onCleanup(() => resolver.close());
  sb = node({ resolver });
}

/** A target handler that serves Talk and answers `to:<payload>`. */
function talkTarget(): ChannelHandler {
  return (incoming) => {
    arrived.push(incoming);
    void incoming.talk().serve((m) => void m.reply(`to:${m.text()}`));
  };
}

describe('dispatch', () => {
  test('no shard: noSlotHandler, else INVALID_ARGUMENT', async () => {
    await setUp();
    newGate();
    const refused = peer.open();
    expectCode(await refused.done, StatusCode.invalidArgument);
    const seen: string[] = [];
    gate.noSlotHandler = (incoming) => {
      seen.push(text(incoming.address.payload));
      void incoming.channel.close();
    };
    expect((await peer.open({ payload: 'xfer' }).done).isOk).toBe(true);
    expect(seen).toEqual(['xfer']);
  });

  test('a slot not served here: MOVED, no owner without a mirror', async () => {
    await setUp();
    newGate();
    const done = peer.open({ shard: 3 }).done;
    expect(gate.stateOf(3)).toBeUndefined();
    expectMoved(await done);
  });

  test('ASSIGN loads, then channels are served', async () => {
    await setUp();
    newGate();
    expect(await gate.onAssign(assign(3, { epoch: 4, holder: 7 }))).toBe(AssignResult.holding);
    expect(gate.serves(3)).toBe(true);
    expect(gate.servedSlots).toEqual(new Map([[3, 4]]));
    expect(await ask(peer.open({ shard: 3 }), 'hi')).toBe('3:hi');
    expect(lifecycle.log).toEqual(['load 3 e4 h7', 'serve 3 cred']);
  });

  test('channels wait while loading, in order', async () => {
    await setUp();
    newGate();
    lifecycle.loadGate = new Deferred<void>();
    const assigned = gate.onAssign(assign(1));
    await settle();
    expect(gate.stateOf(1)).toBe(SlotGateState.loading);
    const a = peer.open({ shard: 1, payload: 'a' });
    const b = peer.open({ shard: 1, payload: 'b' });
    void a.send(bytes('x'));
    await settle();
    expect(lifecycle.served.size).toBe(0);
    lifecycle.loadGate.resolve();
    await assigned;
    expect(await nextText(a)).toBe('1:x');
    expect(await ask(b, 'y')).toBe('1:y');
    expect(lifecycle.log).toEqual(['load 1 e1 h0', 'serve 1 a', 'serve 1 b']);
  });

  test('revocation from serve refuses the rest of the released queue', async () => {
    await setUp();
    newGate();
    lifecycle.loadGate = new Deferred<void>();
    const assigned = gate.onAssign(assign(1));
    const a = peer.open({ shard: 1, payload: 'a' });
    const b = peer.open({ shard: 1, payload: 'b' });
    await peer.arrived(2);
    lifecycle.onServe = (_, slot) => gate.onRevoke(kv, slot);
    lifecycle.loadGate.resolve();
    await assigned;
    expect(lifecycle.served.get(1)).toHaveLength(1);
    expectRelocated(await a.done);
    expectMoved(await b.done);
  });

  test('a revoked load must finish cleanup before another ASSIGN', async () => {
    await setUp();
    newGate();
    const hold = new Deferred<void>();
    lifecycle.loadGate = hold;
    onCleanup(() => hold.resolve());
    const loading = gate.onAssign(assign(1));
    loading.catch(() => {});
    await gate.onRevoke(kv, 1);
    lifecycle.loadGate = undefined;
    await rejectsWith(gate.onAssign(assign(1, { epoch: 2 })), StatusCode.unavailable);
    hold.resolve();
    await rejectsWith(loading, StatusCode.unavailable);
    await gate.onAssign(assign(1, { epoch: 2 }));
    expect(lifecycle.log).toEqual(['load 1 e1 h0', 'unload 1', 'load 1 e2 h0']);
    expect(await ask(peer.open({ shard: 1 }), 'x')).toBe('1:x');
  });

  test('a new gate on the lifecycle refuses the slots its closed gate is still loading, and only those', async () => {
    await setUp();
    newGate();
    const hold = new Deferred<void>();
    lifecycle.loadGate = hold;
    onCleanup(() => hold.resolve());
    const loading = gate.onAssign(assign(1));
    loading.catch(() => {});
    await gate.close();
    // A failed publishSharded can be repeated with the same lifecycle.
    const next = new SlotGate(sb, client, kv, { lifecycle });
    onCleanup(() => next.close());
    expect(lifecycle.gate).toBe(next);
    lifecycle.loadGate = undefined;
    await rejectsWith(next.onAssign(assign(1, { epoch: 2 })), StatusCode.unavailable);
    await next.onAssign(assign(2));
    expect(next.serves(2)).toBe(true);
    hold.resolve();
    await rejectsWith(loading, StatusCode.unavailable);
    await next.onAssign(assign(1, { epoch: 2 }));
    expect(next.serves(1)).toBe(true);
    expect(lifecycle.log).toEqual(['load 1 e1 h0', 'load 2 e1 h0', 'unload 1', 'load 1 e2 h0']);
  });

  test('an unload must finish before another ASSIGN', async () => {
    await setUp();
    newGate();
    await gate.onAssign(assign(1));
    const hold = new Deferred<void>();
    lifecycle.unloadGate = hold;
    onCleanup(() => hold.resolve());
    const revoking = gate.onRevoke(kv, 1);
    await rejectsWith(gate.onAssign(assign(1, { epoch: 2 })), StatusCode.unavailable);
    hold.resolve();
    await revoking;
    await gate.onAssign(assign(1, { epoch: 2 }));
    expect(gate.serves(1)).toBe(true);
  });

  test('closing during reassignment unload prevents the new load', async () => {
    await setUp();
    newGate();
    await gate.onAssign(assign(1));
    await gate.onForward(forward(1, 2, 2));
    const hold = new Deferred<void>();
    lifecycle.unloadGate = hold;
    onCleanup(() => hold.resolve());
    const loading = gate.onAssign(assign(1, { epoch: 3 }));
    loading.catch(() => {});
    await gate.close();
    hold.resolve();
    await rejectsWith(loading, StatusCode.unavailable);
    expect(lifecycle.log).toEqual(['load 1 e1 h0', 'unload 1']);
  });

  test('a cancelled ASSIGN cannot serve its queue when load completes', async () => {
    await setUp();
    newGate();
    const context = new RecordingContext(kv, 1, 1);
    const hold = new Deferred<void>();
    lifecycle.loadGate = hold;
    const loading = gate.onAssign(assign(1), context);
    loading.catch(() => {});
    const queued = peer.open({ shard: 1 });
    await settle();
    context.cancelled.resolve();
    hold.resolve();
    await rejectsWith(loading, StatusCode.cancelled);
    expectMoved(await queued.done);
    expect(lifecycle.served.size).toBe(0);
    expect(gate.serves(1)).toBe(false);
    expect(lifecycle.log).toEqual(['load 1 e1 h0', 'unload 1']);
  });

  test('a failed load refuses the queue and fails ASSIGN', async () => {
    await setUp();
    newGate();
    lifecycle.loadGate = new Deferred<void>();
    lifecycle.loadError = SwitchboardError.of(StatusCode.unavailable, 'no');
    const assigned = gate.onAssign(assign(1));
    assigned.catch(() => {});
    await settle();
    const queued = peer.open({ shard: 1 });
    await settle();
    lifecycle.loadGate.resolve();
    await rejectsWith(assigned, StatusCode.unavailable);
    expectMoved(await queued.done);
    expect(gate.stateOf(1)).toBeUndefined();
  });

  test('a slot outside the mirrored space: OUT_OF_RANGE', async () => {
    await setUp();
    const h = new Harness();
    onCleanup(() => h.close());
    client = await mirroredClient(h);
    newGate();
    expectCode(await peer.open({ shard: 4 }).done, StatusCode.outOfRange);
    expectMoved(await peer.open({ shard: 3 }).done);
  });

  test('channels beyond maxQueuedChannels: UNAVAILABLE', async () => {
    await setUp();
    newGate({ maxQueuedChannels: 2 });
    lifecycle.loadGate = new Deferred<void>();
    const assigned = gate.onAssign(assign(1));
    await settle();
    const a = peer.open({ shard: 1, payload: 'a' });
    const b = peer.open({ shard: 1, payload: 'b' });
    const c = peer.open({ shard: 1, payload: 'c' });
    expectCode(await c.done, StatusCode.unavailable);
    // A queued channel its peer closes leaves the queue.
    await a.close();
    await settle();
    const d = peer.open({ shard: 1, payload: 'd' });
    lifecycle.loadGate.resolve();
    await assigned;
    expect(await ask(b, 'x')).toBe('1:x');
    expect(await ask(d, 'y')).toBe('1:y');
    expect(lifecycle.log).toEqual(['load 1 e1 h0', 'serve 1 b', 'serve 1 d']);
  });

  test('a throwing serve closes the channel with INTERNAL', async () => {
    await setUp();
    newGate();
    lifecycle.onServe = () => {
      throw new Error('boom');
    };
    await gate.onAssign(assign(1));
    expectCode(await peer.open({ shard: 1 }).done, StatusCode.internal);
  });

  test('an async serve that fails after an await closes the channel with INTERNAL; the error does not escape', async () => {
    await setUp();
    const severe = gateLogs('error');
    newGate();
    let calls = 0;
    lifecycle.onServe = async () => {
      calls++;
      await settle();
      throw new Error('async boom');
    };
    await gate.onAssign(assign(1));
    expectCode(await peer.open({ shard: 1 }).done, StatusCode.internal);
    // A queued channel, served by RESUME rather than by the dispatch.
    await gate.onDrain(drain(1, 2, 2));
    const queued = peer.open({ shard: 1 });
    await settle();
    await gate.onResume(resume(1, 1));
    expectCode(await queued.done, StatusCode.internal);
    expect(calls).toBe(2);
    await settle();
    expect(severe).toHaveLength(2);
    expect(severe.every((message) => message.includes('failed'))).toBe(true);
    // The gate goes on serving.
    lifecycle.onServe = undefined;
    expect(await ask(peer.open({ shard: 1 }), 'x')).toBe('1:x');
    expect(gate.serves(1)).toBe(true);
  });
});

describe('hand-over', () => {
  test('DRAIN waits for tracked channels, then drains', async () => {
    await setUp();
    newGate();
    await gate.onAssign(assign(1));
    const busy = peer.open({ shard: 1, payload: 'busy' });
    expect(await ask(busy, 'x')).toBe('1:x');
    const drained = gate.onDrain(drain(1, 2, 2));
    await settle();
    expect(gate.stateOf(1)).toBe(SlotGateState.locked);
    expect(lifecycle.log).not.toContain('drain 1 e2 to2');
    // New channels are queued meanwhile, unread.
    const late = peer.open({ shard: 1, payload: 'late' });
    void late.send(bytes('queued'));
    await settle();
    expect(lifecycle.served.get(1)).toHaveLength(1);
    await busy.close();
    await drained;
    expect(lifecycle.log.at(-1)).toBe('drain 1 e2 to2');
    // RESUME serves the queue here, with what it sent meanwhile.
    await gate.onResume(resume(1, 1));
    expect(gate.serves(1)).toBe(true);
    expect(await nextText(late)).toBe('1:queued');
    expect(lifecycle.log.at(-1)).toBe('serve 1 late');
  });

  test('DRAIN declares its wait for work in flight, a second longer, then restarts the default timeout; load and drain get the context', async () => {
    await setUp();
    newGate();
    const loading = new RecordingContext(kv, 1, 1);
    await gate.onAssign(assign(1), loading);
    expect(lifecycle.loadContext).toBe(loading);
    expect(loading.declared).toEqual([]);
    const busy = peer.open({ shard: 1, payload: 'busy' });
    expect(await ask(busy, 'x')).toBe('1:x');
    const draining = new RecordingContext(kv, 1, 2);
    const drained = gate.onDrain(drain(1, 2, 2), draining);
    await settle();
    const declared = limit + 1000;
    expect(draining.declared).toEqual([[declared, undefined]]);
    await busy.close();
    await drained;
    expect(lifecycle.drainContext).toBe(draining);
    // The drain gets the naming service's default timeout again.
    expect(draining.declared).toEqual([
      [declared, undefined],
      [undefined, undefined],
    ]);
    // Nothing in flight: nothing to declare.
    await gate.onResume(resume(1, 1));
    const idle = new RecordingContext(kv, 1, 3);
    await gate.onDrain(drain(1, 3, 2), idle);
    expect(idle.declared).toEqual([]);
    expect(lifecycle.drainContext).toBe(idle);
  });

  test('a DRAIN cancelled while it waits for work in flight stops waiting, closes nothing and does not drain', async () => {
    await setUp();
    newGate();
    await gate.onAssign(assign(1));
    const busy = peer.open({ shard: 1, payload: 'busy' });
    const answers = new Frames(busy);
    void busy.send(bytes('x'));
    expect(await answers.text()).toBe('1:x');
    const draining = new RecordingContext(kv, 1, 2);
    const drained = rejectsWith(gate.onDrain(drain(1, 2, 2), draining), StatusCode.cancelled);
    await settle();
    const start = performance.now();
    draining.cancelled.resolve();
    await drained;
    // Well before drainTimeout (5 s).
    expect(performance.now() - start).toBeLessThan(2000);
    expect(lifecycle.log).not.toContain('drain 1 e2 to2');
    expect(busy.canSend).toBe(true);
    expect(gate.stateOf(1)).toBe(SlotGateState.locked);
    await gate.onResume(resume(1, 1));
    void busy.send(bytes('y'));
    expect(await answers.text()).toBe('1:y');
  });

  test('detached channels do not hold DRAIN', async () => {
    await setUp();
    newGate();
    lifecycle.onServe = (channel) => gate.detach(channel);
    await gate.onAssign(assign(1));
    const long = peer.open({ shard: 1, payload: 'long' });
    await until(() => lifecycle.served.get(1)?.length === 1);
    // Held, it would wait for drainTimeout (5 s).
    const start = performance.now();
    await gate.onDrain(drain(1, 2, 2));
    expect(performance.now() - start).toBeLessThan(2000);
    expect(lifecycle.log.at(-1)).toBe('drain 1 e2 to2');
    expect(long.canSend).toBe(true);
  });

  test('a tracked channel holds DRAIN at most drainTimeout, then is closed with RELOCATED before the drain', async () => {
    await setUp();
    newGate({ drainTimeout: 30 });
    lifecycle.onServe = () => {};
    await gate.onAssign(assign(1));
    const stuck = peer.open({ shard: 1, payload: 'stuck' });
    await until(() => lifecycle.served.get(1)?.length === 1);
    const start = performance.now();
    lifecycle.drainGate = new Deferred<void>();
    const drained = gate.onDrain(drain(1, 2, 2));
    await until(() => lifecycle.log.at(-1) === 'drain 1 e2 to2');
    expect(performance.now() - start).toBeGreaterThanOrEqual(25);
    // Closed while drain still runs, naming the new owner and epoch.
    expectRelocated(await stuck.done, 2, 2);
    lifecycle.drainGate.resolve();
    await drained;
    expect(gate.stateOf(1)).toBe(SlotGateState.locked);
  });

  test('after DRAIN gave up waiting, the old owner serves nothing more on the channel', async () => {
    await setUp();
    // The channel outlives drainTimeout; it must not be served on state
    // that is handed over (wiki: "The hand-over, step by step").
    newGate({ drainTimeout: 30, forwardGrace: 2000 });
    await gate.onAssign(assign(1));
    const stuck = peer.open({ shard: 1, payload: 'stuck' });
    const answers = new Frames(stuck);
    void stuck.send(bytes('before'));
    expect(await answers.text()).toBe('1:before');
    await gate.onDrain(drain(1, 2, 2));
    await gate.onForward(forward(1, 2, 2));
    expect(gate.stateOf(1)).toBe(SlotGateState.forwarding);
    expectRelocated(await stuck.done, 2, 2);
    if (stuck.canSend) {
      void stuck.send(bytes('after-forward')).catch(() => {});
    }
    expect(await answers.next()).toBeUndefined();
    await settle();
    expect(lifecycle.served.get(1)).toHaveLength(1);
    expect(arrived).toEqual([]);
  });

  test('FORWARD pipes the queue to the new owner in order, and late arrivals too', async () => {
    await setUp();
    newGate();
    await gate.onAssign(assign(1));
    await gate.onDrain(drain(1, 2, 2));
    const first = peer.open({ shard: 1, payload: 'first' });
    const second = peer.open({ shard: 1, payload: 'second' });
    void first.send(bytes('a'));
    void second.send(bytes('b'));
    await settle();
    await gate.onForward(forward(1, 2, 2));
    expect(gate.stateOf(1)).toBe(SlotGateState.forwarding);
    expect(await nextText(first)).toBe('to:a');
    const answers = new Frames(second);
    void second.send(bytes('c'));
    expect([await answers.text(), await answers.text()]).toEqual(['to:b', 'to:c']);
    // In arrival order, with the open payload except the instance.
    expect(arrived.map((c) => text(c.address.payload))).toEqual(['first', 'second']);
    expect(arrived[0]?.address.instance).toBe(2);
    expect(arrived[0]?.address.shard).toBe(1);
    // Late arrivals are forwarded during the grace period.
    expect(await ask(peer.open({ shard: 1, payload: 'late' }), 'd')).toBe('to:d');
    expectMoved(gate.movedStatus(1), 2, 2);
    expect(gate.servedSlots.size).toBe(0);
    expect(lifecycle.log).not.toContain('unload 1');
  });

  test('forwarding opens remain bounded while resolution is stalled', async () => {
    await setUp();
    const resolver = new DelayedResolver([
      serviceRecord(new ServiceAddress(kv, 2), { endpoints: [targetUri] }),
    ]);
    sb.resolver = resolver;
    onCleanup(() => resolver.close());
    onCleanup(() => resolver.proceed.resolve());
    newGate({ maxQueuedChannels: 1 });
    await gate.onAssign(assign(1));
    await gate.onForward(forward(1, 2, 2));
    const first = peer.open({ shard: 1 });
    await peer.arrived(1);
    await first.close();
    // Closing and opening again cannot accumulate unbounded pending
    // resolver calls or continuations outside the mux channel limit.
    const excess = peer.open({ shard: 1 });
    expectCode(await excess.done, StatusCode.unavailable);
    resolver.proceed.resolve();
    await sb.connect(targetUri);
    await settle();
    expect(await ask(peer.open({ shard: 1 }), 'new')).toBe('to:new');
  });

  test('a forwarding open that fails at once gives its place back', async () => {
    await setUp();
    const resolver = new DelayedResolver([
      serviceRecord(new ServiceAddress(kv, 2), { endpoints: [targetUri] }),
    ]);
    resolver.throwAtOnce = true;
    resolver.proceed.resolve();
    sb.resolver = resolver;
    onCleanup(() => resolver.close());
    newGate({ maxQueuedChannels: 1 });
    await gate.onAssign(assign(1));
    await gate.onForward(forward(1, 2, 2));
    for (let i = 0; i < 3; i++) {
      expectCode(await peer.open({ shard: 1 }).done, StatusCode.unavailable);
    }
    // Refused by the bound if a failed open had kept its place.
    resolver.throwAtOnce = false;
    expect(await ask(peer.open({ shard: 1 }), 'new')).toBe('to:new');
  });

  test('after the grace period: unload, then MOVED', async () => {
    await setUp();
    newGate({ forwardGrace: 30 });
    await gate.onAssign(assign(1));
    await gate.onDrain(drain(1, 2, 2));
    const queued = peer.open({ shard: 1 });
    const answers = new Frames(queued);
    void queued.send(bytes('a'));
    await peer.arrived(1);
    await gate.onForward(forward(1, 2, 2));
    await until(() => gate.stateOf(1) === undefined);
    expect(lifecycle.log.at(-1)).toBe('unload 1');
    // The mirror knows nothing here: MOVED with an empty reason.
    expectMoved(await peer.open({ shard: 1 }).done);
    // A forwarded channel keeps working after the grace period.
    expect(await answers.text()).toBe('to:a');
    void queued.send(bytes('b'));
    expect(await answers.text()).toBe('to:b');
  });

  test('the RELOCATED status names the new owner while forwarding', async () => {
    await setUp();
    newGate();
    await gate.onAssign(assign(1));
    expectRelocated(gate.relocatedStatus(1));
    await gate.onDrain(drain(1, 2, 2));
    await gate.onForward(forward(1, 2, 2));
    expect(gate.movedTo(1).equals(new MovedStatus({ owner: 2, epoch: 2 }))).toBe(true);
    expectRelocated(gate.relocatedStatus(1), 2, 2);
    expectMoved(gate.movedStatus(1), 2, 2);
  });

  test('a slot coming back during the grace period: forwarding stops, it is unloaded, then loaded and served here', async () => {
    await setUp();
    // The grace period on the gate's own clock (Dart: 50 ms of real time,
    // which a slow forwarded open could outlast).
    const clock = new ManualClock();
    newGate({ forwardGrace: 50, clock });
    await gate.onAssign(assign(1));
    await gate.onDrain(drain(1, 2, 2));
    await gate.onForward(forward(1, 2, 2));
    expect(await ask(peer.open({ shard: 1, payload: 'late' }), 'a')).toBe('to:a');
    await gate.onAssign(assign(1, { epoch: 3, holder: 2 }));
    expect(gate.serves(1)).toBe(true);
    expect(gate.servedSlots).toEqual(new Map([[1, 3]]));
    expect(lifecycle.log.slice(2)).toEqual(['unload 1', 'load 1 e3 h2']);
    expect(await ask(peer.open({ shard: 1 }), 'x')).toBe('1:x');
    // Past the grace period it was handed over with: still served here,
    // nothing more forwarded or unloaded.
    clock.advance(100);
    expect(await ask(peer.open({ shard: 1, payload: 'after' }), 'y')).toBe('1:y');
    expect(gate.servedSlots).toEqual(new Map([[1, 3]]));
    expect(arrived).toHaveLength(1);
    expect(lifecycle.log.filter((line) => line === 'unload 1')).toHaveLength(1);
  });

  test('ASSIGN of a slot served already only updates the epoch', async () => {
    await setUp();
    newGate();
    lifecycle.result = AssignResult.notHolding;
    await gate.onAssign(assign(1));
    expect(await gate.onAssign(assign(1, { epoch: 9 }))).toBe(AssignResult.notHolding);
    expect(gate.servedSlots).toEqual(new Map([[1, 9]]));
    expect(lifecycle.log).toEqual(['load 1 e1 h0']);
  });

  test('ASSIGN of a slot still locked after a lost RESUME unlocks it with the new epoch and serves its queue here', async () => {
    await setUp();
    const infos = gateLogs('info');
    newGate();
    await gate.onAssign(assign(1, { holder: 1 }));
    await gate.onDrain(drain(1, 2, 2));
    const queued = peer.open({ shard: 1, payload: 'queued' });
    void queued.send(bytes('a'));
    await settle();
    expect(lifecycle.served.size).toBe(0);
    // The migration to 2 failed, its RESUME (epoch 1) never arrived, and
    // the slot, free at epoch 1, is assigned back here at epoch 2.
    expect(await gate.onAssign(assign(1, { epoch: 2, holder: 1 }))).toBe(AssignResult.holding);
    expect(gate.stateOf(1)).toBe(SlotGateState.serving);
    expect(gate.servedSlots).toEqual(new Map([[1, 2]]));
    expect(await nextText(queued)).toBe('1:a');
    expect(await ask(peer.open({ shard: 1, payload: 'new' }), 'b')).toBe('1:b');
    // Served on the state it has: not unloaded nor loaded again, nothing
    // forwarded.
    expect(lifecycle.log).toEqual([
      'load 1 e1 h1',
      'drain 1 e2 to2',
      'serve 1 queued',
      'serve 1 new',
    ]);
    expect(arrived).toEqual([]);
    expect(infos).toHaveLength(1);
    expect(infos[0]).toContain('RESUME lost');
    // A RESUME arriving late changes nothing.
    await gate.onResume(resume(1, 1));
    expect(gate.servedSlots).toEqual(new Map([[1, 2]]));
  });

  test('ASSIGN of a slot locked while another instance owned it since refuses its queue with MOVED, unloads, then loads', async () => {
    await setUp();
    newGate();
    await gate.onAssign(assign(1, { holder: 1 }));
    await gate.onDrain(drain(1, 2, 2));
    const queued = peer.open({ shard: 1, payload: 'queued' });
    await peer.arrived(1);
    // Owned by 2 at epoch 2, handed back at epoch 3: the state is there.
    await gate.onAssign(assign(1, { epoch: 3, holder: 2 }));
    expectMoved(await queued.done);
    expect(gate.servedSlots).toEqual(new Map([[1, 3]]));
    expect(lifecycle.log.slice(2)).toEqual(['unload 1', 'load 1 e3 h2']);
    expect(await ask(peer.open({ shard: 1 }), 'x')).toBe('1:x');
  });

  test('revocation refuses the queue with MOVED and closes served channels with RELOCATED', async () => {
    await setUp();
    newGate();
    await gate.onAssign(assign(1));
    const served = peer.open({ shard: 1 });
    expect(await ask(served, 'x')).toBe('1:x');
    // DRAIN waits for the served channel; the revocation ends it.
    const drained = rejectsWith(gate.onDrain(drain(1, 2, 2)), StatusCode.unavailable);
    const queued = peer.open({ shard: 1 });
    await settle();
    await gate.onRevoke(kv, 1);
    expectMoved(await queued.done);
    expectRelocated(await served.done);
    expect(lifecycle.log.at(-1)).toBe('unload 1');
    expect(gate.stateOf(1)).toBeUndefined();
    await drained;
    expect(lifecycle.log).not.toContain('drain 1 e2 to2');
  });

  test('DRAIN of a slot not served: FAILED_PRECONDITION', async () => {
    await setUp();
    newGate();
    await rejectsWith(gate.onDrain(drain(1, 2, 2)), StatusCode.failedPrecondition);
  });

  test('DRAIN naming no instance or this one: FAILED_PRECONDITION, the slot goes on being served', async () => {
    await setUp();
    const warnings = gateLogs('warn');
    newGate();
    await gate.onAssign(assign(1));
    const served = peer.open({ shard: 1 });
    const answers = new Frames(served);
    void served.send(bytes('x'));
    expect(await answers.text()).toBe('1:x');
    for (const to of [0, 1]) {
      await rejectsWith(gate.onDrain(drain(1, 2, to)), StatusCode.failedPrecondition);
    }
    expect(gate.serves(1)).toBe(true);
    void served.send(bytes('y'));
    expect(await answers.text()).toBe('1:y');
    expect(lifecycle.log.some((line) => line.startsWith('drain'))).toBe(false);
    expect(warnings).toHaveLength(2);
    expect(warnings.every((message) => message.includes('DRAIN'))).toBe(true);
  });

  test('FORWARD naming this instance: FAILED_PRECONDITION, nothing piped back into this node, the queue refused with MOVED', async () => {
    await setUp();
    const warnings = gateLogs('warn');
    newGate();
    // The new owner named is this instance, whose record is this node's own
    // listener, where the gate is registered: forwarding there would
    // dispatch the queued channel into this gate again, a loop.
    const self = await sb.listenMemory();
    const resolver = new StaticResolver([
      serviceRecord(new ServiceAddress(kv, 1), { endpoints: [self] }),
    ]);
    onCleanup(() => resolver.close());
    sb.resolver = resolver;
    let arrivals = 0;
    sb.registerService(
      kv,
      (incoming) => {
        arrivals++;
        return gate.handler(incoming);
      },
      { instance: 1 },
    );
    await gate.onAssign(assign(1));
    await gate.onDrain(drain(1, 2, 2));
    const queued = peer.open({ shard: 1 });
    await settle();
    await rejectsWith(gate.onForward(forward(1, 2, 1)), StatusCode.failedPrecondition);
    // Never read here: MOVED. The naming client dropped the slot on
    // FORWARD, so the gate stops serving it.
    expectMoved(await queued.done);
    expect(gate.stateOf(1)).toBeUndefined();
    expect(lifecycle.log.at(-1)).toBe('unload 1');
    expectMoved(await peer.open({ shard: 1 }).done);
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(arrivals).toBe(0);
    expect(lifecycle.served.size).toBe(0);
    // No instance: refused as well, even for a slot not here.
    await rejectsWith(gate.onForward(forward(2, 2, 0)), StatusCode.failedPrecondition);
    expect(warnings).toHaveLength(2);
    expect(warnings.every((message) => message.includes('FORWARD'))).toBe(true);
  });

  test("a new owner whose record is this node's own listener: nothing is forwarded there, the queue and late arrivals get UNAVAILABLE", async () => {
    await setUp();
    newGate();
    const self = await sb.listenMemory();
    const resolver = new StaticResolver([
      serviceRecord(new ServiceAddress(kv, 2), { endpoints: [self] }),
    ]);
    onCleanup(() => resolver.close());
    sb.resolver = resolver;
    // This node takes every instance of the type: a channel piped back to
    // it would reach the forwarding gate again.
    let arrivals = 0;
    sb.registerService(
      kv,
      (incoming) => {
        arrivals++;
        return gate.handler(incoming);
      },
      { instance: 1, acceptAnyInstance: true },
    );
    await gate.onAssign(assign(1));
    await gate.onDrain(drain(1, 2, 2));
    const queued = peer.open({ shard: 1 });
    await settle();
    await gate.onForward(forward(1, 2, 2));
    expectCode(await queued.done, StatusCode.unavailable);
    expectCode(await peer.open({ shard: 1 }).done, StatusCode.unavailable);
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(arrivals).toBe(0);
    expect(gate.stateOf(1)).toBe(SlotGateState.forwarding);
  });

  test('RESUME and FORWARD of unknown slots do nothing', async () => {
    await setUp();
    newGate();
    await gate.onResume(resume(1, 1));
    await gate.onForward(forward(1, 2, 2));
    expect(gate.stateOf(1)).toBeUndefined();
    expect(lifecycle.log).toEqual([]);
  });

  test('close stops every slot and refuses later ASSIGNs', async () => {
    await setUp();
    newGate();
    await gate.onAssign(assign(1));
    await gate.onAssign(assign(2));
    await gate.close();
    expect(lifecycle.log.slice(2).sort()).toEqual(['unload 1', 'unload 2']);
    await expect(gate.onAssign(assign(3))).rejects.toBeInstanceOf(SwitchboardError);
    expectMoved(await peer.open({ shard: 1 }).done);
  });
});

describe('requests', () => {
  let answered: string[];
  /** The requests whose handler started, in order. */
  let started: string[];

  test('revocation from a request refuses the rest of the released queue', async () => {
    await setUp();
    newGate({ trackChannels: false });
    const handled: string[] = [];
    lifecycle.onServe = (channel, slot) => {
      void channel.talk().serve((message) => {
        void gate.serveRequest(message, slot, (m) => {
          handled.push(m.text());
          void m.reply(m.payload);
          void gate.onRevoke(kv, slot);
        });
      });
    };
    await gate.onAssign(assign(1));
    const talk = new TalkChannel(peer.open({ shard: 1 }));
    await settle();
    await gate.onDrain(drain(1, 2, 2));
    const first = talk.request('GET', 'a');
    const second = rejectsWith(talk.request('GET', 'b'), StatusCode.moved);
    await settle();
    await gate.onResume(resume(1, 1));
    expect((await first).text()).toBe('a');
    await second;
    expect(handled).toEqual(['a']);
    await talk.close();
  });

  /**
   * Serves Talk on every channel; requests go through `serveRequest` and are
   * answered `slot:payload` after [hold] resolves.
   */
  function talkLifecycle(hold?: Deferred<void>): void {
    answered = [];
    started = [];
    lifecycle.onServe = (channel, slot) => {
      gate.detach(channel);
      void channel.talk().serve((m) =>
        gate.serveRequest(m, slot, async (request) => {
          started.push(request.text());
          await hold?.promise;
          answered.push(request.text());
          void request.reply(`${slot}:${request.text()}`);
        }),
      );
    };
  }

  test('forwarded requests remain bounded while resolution is stalled', async () => {
    await setUp();
    const resolver = new DelayedResolver([
      serviceRecord(new ServiceAddress(kv, 2), { endpoints: [targetUri] }),
    ]);
    sb.resolver = resolver;
    onCleanup(() => resolver.close());
    onCleanup(() => resolver.proceed.resolve());
    newGate({ maxQueuedRequests: 1, trackChannels: false });
    talkLifecycle();
    await gate.onAssign(assign(1));
    const talk = new TalkChannel(peer.open({ shard: 1 }));
    await settle();
    await gate.onForward(forward(1, 2, 2));
    target.registerService(
      kv,
      (incoming) => {
        void incoming.talk().serve((m) => void m.reply(m.payload));
      },
      { instance: 2 },
    );
    const first = talk.startRequest('GET', 'first');
    const cancelled = rejectsWith(first.response, StatusCode.cancelled);
    await settle();
    first.cancel();
    await cancelled;
    await rejectsWith(talk.request('GET', 'excess', { timeout: limit }), StatusCode.unavailable);
    resolver.proceed.resolve();
    await sb.connect(targetUri);
    await settle();
    expect((await talk.request('GET', 'new')).text()).toBe('new');
    await talk.close();
  });

  test("a forwarding channel that fails at once gives the request's place back", async () => {
    await setUp();
    const resolver = new DelayedResolver([
      serviceRecord(new ServiceAddress(kv, 2), { endpoints: [targetUri] }),
    ]);
    resolver.throwAtOnce = true;
    resolver.proceed.resolve();
    sb.resolver = resolver;
    onCleanup(() => resolver.close());
    newGate({ maxQueuedRequests: 1, trackChannels: false });
    talkLifecycle();
    await gate.onAssign(assign(1));
    const talk = new TalkChannel(peer.open({ shard: 1 }));
    await settle();
    await gate.onForward(forward(1, 2, 2));
    target.registerService(
      kv,
      (incoming) => {
        void incoming.talk().serve((m) => void m.reply(m.payload));
      },
      { instance: 2 },
    );
    for (let i = 0; i < 3; i++) {
      await rejectsWith(talk.request('GET', `x${i}`, { timeout: limit }), StatusCode.unavailable);
    }
    // Refused by the bound if a failed open had kept its place.
    resolver.throwAtOnce = false;
    expect((await talk.request('GET', 'new')).text()).toBe('new');
    await talk.close();
  });

  test('served, queued while locked, forwarded after FORWARD', async () => {
    await setUp();
    newGate({ trackChannels: false });
    talkLifecycle();
    await gate.onAssign(assign(1));
    const talk = new TalkChannel(peer.open({ shard: 1 }));
    expect((await talk.request('GET', 'a')).text()).toBe('1:a');
    await gate.onDrain(drain(1, 2, 2));
    const queued = talk.request('GET', 'b');
    await settle();
    expect(answered).toEqual(['a']);
    // The target node echoes raw subframes; serve Talk there instead.
    target.registerService(kv, talkTarget(), { instance: 2 });
    await gate.onForward(forward(1, 2, 2));
    expect((await queued).text()).toBe('to:b');
    expect((await talk.request('GET', 'c')).text()).toBe('to:c');
    // One forwarding channel, addressed to the slot, for both.
    expect(arrived).toHaveLength(1);
    expect(arrived[0]?.address.shard).toBe(1);
    expect(arrived[0]?.address.instance).toBe(2);
    await talk.close();
  });

  if (overFrameMux()) {
    test('a forwarded request and its answer carry bulk payloads', async () => {
      await setUp();
      newGate({ trackChannels: false });
      talkLifecycle();
      await gate.onAssign(assign(1));
      const talk = new TalkChannel(peer.open({ shard: 1 }));
      await gate.onDrain(drain(1, 2, 2));
      const bulkSeen: boolean[] = [];
      target.registerService(
        kv,
        (incoming) => {
          void incoming.talk().serve((m) => {
            bulkSeen.push(m.isBulk);
            void m.reply(concatBytes(m.payload, m.payload));
          });
        },
        { instance: 2 },
      );
      await gate.onForward(forward(1, 2, 2));
      const big = Uint8Array.from({ length: 100_000 }, (_, i) => i % 251);
      const answer = await talk.request('PUT', big);
      expect(answer.isBulk).toBe(true);
      expect(bytesEqual(answer.payload, concatBytes(big, big))).toBe(true);
      expect(bulkSeen).toEqual([true]);
      await talk.close();
    });
  }

  test('in-flight requests hold DRAIN; RESUME runs the queue here', async () => {
    await setUp();
    newGate({ trackChannels: false });
    const hold = new Deferred<void>();
    talkLifecycle(hold);
    await gate.onAssign(assign(1));
    const talk = new TalkChannel(peer.open({ shard: 1 }));
    const first = talk.request('GET', 'a');
    await until(() => started.length === 1);
    const drained = gate.onDrain(drain(1, 2, 2));
    const second = talk.request('GET', 'b');
    await settle();
    expect(lifecycle.log.at(-1)).toBe('serve 1 cred');
    hold.resolve();
    await drained;
    expect(lifecycle.log.at(-1)).toBe('drain 1 e2 to2');
    expect(answered).toEqual(['a']);
    await gate.onResume(resume(1, 1));
    expect((await first).text()).toBe('1:a');
    expect((await second).text()).toBe('1:b');
    expect(answered).toEqual(['a', 'b']);
    await talk.close();
  });

  test('an ASSIGN of the locked slot after a lost RESUME runs the queue here and answers as its load did', async () => {
    await setUp();
    newGate({ trackChannels: false });
    talkLifecycle();
    lifecycle.result = AssignResult.notHolding;
    await gate.onAssign(assign(1));
    const talk = new TalkChannel(peer.open({ shard: 1 }));
    expect((await talk.request('GET', 'a')).text()).toBe('1:a');
    await gate.onDrain(drain(1, 2, 2));
    const queued = talk.request('GET', 'b');
    await settle();
    expect(answered).toEqual(['a']);
    expect(await gate.onAssign(assign(1, { epoch: 2 }))).toBe(AssignResult.notHolding);
    expect((await queued).text()).toBe('1:b');
    expect(answered).toEqual(['a', 'b']);
    expect(arrived).toEqual([]);
    await talk.close();
  });

  /** Serves Talk on every channel; each request names its slot in its payload and is answered with it. */
  function slotNamingLifecycle(): void {
    lifecycle.onServe = (channel) => {
      gate.detach(channel);
      void channel
        .talk()
        .serve((m) =>
          gate.serveRequest(m, Number(m.text()), (request) => void request.reply(request.payload)),
        );
    };
  }

  /** The answer's text, or the name of the abort code. */
  function outcome(request: Promise<TalkMessage>): Promise<string | undefined> {
    return request.then(
      (m) => m.text(),
      (e: unknown) => statusCodeName((e as SwitchboardError).code),
    );
  }

  test('a slot not served: ABORT MOVED; bounded queue', async () => {
    await setUp();
    newGate({ trackChannels: false, maxQueuedRequests: 1 });
    // Requests name their slot, which need not be the channel's.
    slotNamingLifecycle();
    await gate.onAssign(assign(1));
    const talk = new TalkChannel(peer.open({ shard: 1 }));
    expect(await outcome(talk.request('GET', '1'))).toBe('1');
    expect(await outcome(talk.request('GET', '5'))).toBe('moved');
    await gate.onDrain(drain(1, 2, 2));
    const parked = outcome(talk.request('GET', '1'));
    expect(await outcome(talk.request('GET', '1'))).toBe('unavailable');
    await gate.onResume(resume(1, 1));
    expect(await parked).toBe('1');
    await talk.close();
  });

  test('a slot outside the mirrored space: ABORT OUT_OF_RANGE', async () => {
    await setUp();
    const h = new Harness();
    onCleanup(() => h.close());
    client = await mirroredClient(h);
    newGate({ trackChannels: false });
    slotNamingLifecycle();
    await gate.onAssign(assign(1));
    const talk = new TalkChannel(peer.open({ shard: 1 }));
    expect(await outcome(talk.request('GET', '1'))).toBe('1');
    expect(await outcome(talk.request('GET', '3'))).toBe('moved');
    expect(await outcome(talk.request('GET', '4'))).toBe('outOfRange');
    await talk.close();
  });

  test('a queued request whose channel closed leaves the queue', async () => {
    await setUp();
    newGate({ trackChannels: false, maxQueuedRequests: 1 });
    talkLifecycle();
    await gate.onAssign(assign(1));
    const first = new TalkChannel(peer.open({ shard: 1 }));
    const second = new TalkChannel(peer.open({ shard: 1 }));
    expect((await first.request('GET', 'a')).text()).toBe('1:a');
    await until(() => lifecycle.served.get(1)?.length === 2);
    await gate.onDrain(drain(1, 2, 2));
    const gone = first.request('GET', 'gone');
    gone.catch(() => {});
    // Queued at the gate before the next request arrives on the other
    // channel.
    const servedFirst = (lifecycle.served.get(1) ?? [])[0] as IncomingChannel;
    const atGate = servedFirst.talk();
    await until(() => atGate.incomingRequestCount === 1);
    const refused = second.request('GET', 'refused').then(
      () => undefined,
      (e: unknown) => e,
    );
    await settle();
    await first.close();
    await settle();
    // Its place in the queue is free again.
    const queued = second.request('GET', 'b');
    await settle();
    await gate.onResume(resume(1, 1));
    expect((await queued).text()).toBe('1:b');
    expect(answered).toEqual(['a', 'b']);
    expect(await refused).toBeInstanceOf(SwitchboardError);
    await second.close();
  });

  test('the forwarding channel is closed once idle, and opened again for a late request', async () => {
    await setUp();
    // The idle second on the gate's own clock (Dart waits for it in real
    // time), so that no gap between the first two requests closes it.
    const clock = new ManualClock();
    newGate({ trackChannels: false, clock });
    talkLifecycle();
    target.registerService(kv, talkTarget(), { instance: 2 });
    await gate.onAssign(assign(1));
    const talk = new TalkChannel(peer.open({ shard: 1 }));
    await settle();
    await gate.onDrain(drain(1, 2, 2));
    await gate.onForward(forward(1, 2, 2));
    expect((await talk.request('GET', 'a')).text()).toBe('to:a');
    expect((await talk.request('GET', 'b')).text()).toBe('to:b');
    expect(arrived).toHaveLength(1);
    // The grace period and the idle timer of the forwarding channel.
    await until(() => clock.pendingCount === 2);
    // About a second without a request: closed.
    clock.advance(1000);
    expect((await (arrived[0] as IncomingChannel).channel.done).isOk).toBe(true);
    expect((await talk.request('GET', 'c')).text()).toBe('to:c');
    expect(arrived).toHaveLength(2);
    await talk.close();
  });
});

test('MOVED fields follow the naming client mirror', async () => {
  await setUp();
  const h = new Harness();
  onCleanup(() => h.close());
  // Instance 5 owns slot 2 of a static space.
  const owner = new NamingClient(new Connector(h).call);
  onCleanup(() => owner.close());
  const ownerLifecycle = new TestLifecycle();
  const ownerGate = new SlotGate(sb, owner, kv, { lifecycle: ownerLifecycle, instance: 5 });
  onCleanup(() => ownerGate.close());
  owner.slotHandler = ownerGate;
  await owner.start();
  await owner.register(kv, [targetUri], { instance: 5 });
  await owner.defineSlots(kv, { count: 4, mode: SlotMode.static });
  expect(await ownerGate.claim(2)).toBe(1);
  expect(ownerGate.serves(2)).toBe(true);
  expect(ownerLifecycle.log).toEqual(['load 2 e1 h0']);
  client = new NamingClient(new Connector(h).call);
  const mirror = client;
  onCleanup(() => mirror.close());
  await client.start();
  await client.synced;
  newGate();
  await until(() => client.slotOwner(kv, 2) !== undefined);
  expectMoved(await peer.open({ shard: 2 }).done, 5, 1);
  expectMoved(gate.movedStatus(1));
  // Release stops serving locally after the naming service.
  await ownerGate.release(2);
  expect(ownerGate.stateOf(2)).toBeUndefined();
  expect(ownerLifecycle.log.at(-1)).toBe('unload 2');
  await until(() => client.slotOwner(kv, 2) === undefined);
  expectMoved(gate.movedStatus(2));
});

test('SlotGates dispatches by type', async () => {
  await setUp();
  const gates = new SlotGates();
  newGate();
  gates.add(gate);
  expect(() => gates.add(gate)).toThrow(Error);
  expect(gates.get(kv)).toBe(gate);
  await gates.onAssign(assign(1));
  expect(gate.serves(1)).toBe(true);
  const zz = Name.of('zz');
  const other = new AssignRequest(zz, 1, { epoch: 1, holder: 0, shared: false });
  await rejectsWith(gates.onAssign(other), StatusCode.unavailable);
  await expect(gates.onDrain(new DrainRequest(zz, 1, { epoch: 2, to: 2 }))).rejects.toBeInstanceOf(
    SwitchboardError,
  );
  await gates.onForward(new ForwardRequest(zz, 1, { epoch: 2, to: 2 }));
  await gates.onResume(new ResumeRequest(zz, 1, { epoch: 1 }));
  await gates.onRevoke(zz, 1);
  await gates.onRevoke(kv, 1);
  expect(gate.stateOf(1)).toBeUndefined();
  expect(gates.remove(kv)).toBe(gate);
  expect(gates.gates.size).toBe(0);
});

test('a lifecycle serves one gate, and another once that is closed', async () => {
  await setUp();
  newGate();
  expect(() => new SlotGate(sb, client, kv, { lifecycle })).toThrow(/another slot gate/);
  expect(() => new TestLifecycle().gate).toThrow(/not attached/);
  expect(lifecycle.gate).toBe(gate);
  await gate.close();
  // Still answers for the closed gate until attached again.
  expect(lifecycle.gate).toBe(gate);
  const again = new SlotGate(sb, client, kv, { lifecycle });
  expect(lifecycle.gate).toBe(again);
  await again.onAssign(assign(1));
  expect(again.serves(1)).toBe(true);
  await again.close();
});

/** Records `SlotLifecycle.discard` calls. */
class DiscardLog extends TestLifecycle {
  readonly discarded: (readonly number[])[];

  constructor(discarded: (readonly number[])[]) {
    super();
    this.discarded = discarded;
  }

  override discard(slots: readonly number[]): void {
    this.discarded.push(slots);
  }
}

/** Records `SlotLifecycle.discard` calls, then fails: after an await, or at once when [atOnce] is set. */
class FailingDiscard extends TestLifecycle {
  readonly discarded: (readonly number[])[];
  atOnce = false;

  constructor(discarded: (readonly number[])[]) {
    super();
    this.discarded = discarded;
  }

  override discard(slots: readonly number[]): unknown {
    this.discarded.push(slots);
    if (this.atOnce) {
      throw new Error('discard failed at once');
    }
    return (async () => {
      await settle();
      throw new Error('discard failed after an await');
    })();
  }
}

test('HOLDING discards reach the lifecycle through SlotGates', async () => {
  await setUp();
  const discarded: (readonly number[])[] = [];
  const gates = new SlotGates();
  gates.add(new SlotGate(sb, client, kv, { lifecycle: new DiscardLog(discarded) }));
  gates.onDiscard(kv, [3, 5]);
  gates.onDiscard(Name.of('zz'), [1]);
  expect(discarded).toEqual([[3, 5]]);
});

test('a discard that fails, at once or after an await, is logged; the error does not escape', async () => {
  await setUp();
  const warnings = gateLogs('warn');
  const discarded: (readonly number[])[] = [];
  const failing = new FailingDiscard(discarded);
  const gates = new SlotGates();
  gates.add(new SlotGate(sb, client, kv, { lifecycle: failing, logger: routerLog }));
  gates.onDiscard(kv, [3, 5]);
  failing.atOnce = true;
  gates.onDiscard(kv, [7]);
  await until(() => warnings.length === 2);
  await settle();
  expect(discarded).toEqual([[3, 5], [7]]);
  expect(warnings.every((message) => message.includes('discard'))).toBe(true);
});
