/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Shared helpers for the sharding tests: scripted instances speaking the
// slot procedures over raw Talk channels, and watches of slot events. The
// port of the Dart `sharding_harness.dart`.

import { Name } from '../../../src/core/name.ts';
import type {
  AssignResult,
  SlotHandler,
  SlotRequestContext,
} from '../../../src/core/naming/naming-client-slots.ts';
import {
  AssignRequest,
  AssignResponse,
  ClaimRequest,
  ClaimResponse,
  DrainRequest,
  ForwardRequest,
  HoldingRequest,
  HoldingResponse,
  LocateRequest,
  LocateResponse,
  MigrateRequest,
  migrationPhaseName,
  PhaseItem,
  RegisterRequest,
  RegisterResponse,
  ReleaseRequest,
  ResumeRequest,
  SlotItem,
  SlotSpace,
  SlotsRequest,
  slotModeName,
} from '../../../src/core/naming/naming-protocol.ts';
import { Status, StatusCode, SwitchboardError } from '../../../src/core/status.ts';
import type {
  TalkChannel,
  TalkMessage,
  TalkOptions,
  TalkStream,
} from '../../../src/core/talk/contract.ts';
import { type Clock, systemClock, type TimerHandle } from '../../../src/core/util/clock.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';
import { type FakeTalkChannel, type FrameFilter, fakeTalkPair } from './fake-talk.ts';
import {
  describe,
  type Harness,
  LineQueue,
  ms50,
  register,
  serverOptions,
  Watcher,
} from './harness.ts';

export const kv = Name.of('kv');
export const zone = Name.of('zone');
export const userq = Name.of('userq');
export const room = Name.of('room');

/** `SLOTSPC kv 8 managed lazy shared`, `SLOT kv/3 owned 1 holder 1 epoch 1`, and the forms of {@link describe} for the rest. */
export function describeItem(m: TalkMessage): string {
  switch (m.procedureName) {
    case 'SLOTSPC':
      return describeSpace(SlotSpace.decode(m.payload));
    case 'SLOT':
      return describeSlot(SlotItem.decode(m.payload));
    case 'PHASE':
      return `PHASE ${migrationPhaseName(PhaseItem.decode(m.payload).phase)}`;
    default:
      return describe(m);
  }
}

export function describeSpace(s: SlotSpace): string {
  return (
    `SLOTSPC ${s.type} ${s.count} ${slotModeName(s.mode)}` +
    `${s.lazy ? ' lazy' : ''}${s.shared ? ' shared' : ''}`
  );
}

export function describeSlot(item: SlotItem): string {
  return `SLOT ${item.type}/${item.slot} ${item.entry}`;
}

/** A {@link Watcher} that also reads the slot events (`SLOTSPC`, `SLOT`). */
export class ItemWatch extends Watcher {
  constructor(channel: TalkChannel, options: { payload?: Uint8Array; start?: boolean } = {}) {
    super(channel, { ...options, describer: describeItem });
  }
}

/** A raw `LOOKUP` whose items are read one by one as strings. */
export class Lookup {
  readonly stream: TalkStream;
  readonly #lines = new LineQueue();

  constructor(channel: TalkChannel, payload: Uint8Array) {
    const stream = channel.streamRequest('LOOKUP', payload);
    this.stream = stream;
    void (async () => {
      try {
        for await (const item of stream.items) {
          this.#lines.push(describeItem(item));
        }
      } catch {
        // Ended by an abort; the test reads `stream.done`.
      }
    })();
  }

  take(count: number): Promise<string[]> {
    return this.#lines.take(count);
  }
}

/** Arms a timer that fires every [ms] until [tick] returns false. */
function periodic(clock: Clock, ms: number, tick: () => boolean): () => void {
  let handle: TimerHandle | undefined;
  const arm = (): void => {
    handle = clock.setTimeout(() => {
      handle = undefined;
      if (tick()) arm();
    }, ms);
  };
  arm();
  return () => {
    if (handle !== undefined) clock.clearTimeout(handle);
    handle = undefined;
  };
}

/**
 * A sharded instance scripted over a raw Talk channel: registers with a
 * fixed id, answers the naming service's slot requests through the
 * replaceable behaviours, and records every request in {@link log} (shared
 * between instances to observe the global order).
 */
export class Instance {
  readonly harness: Harness;
  readonly type: Name;
  readonly id: number;
  readonly log: string[];
  channel!: FakeTalkChannel;
  server!: FakeTalkChannel;

  /** Behaviours; the defaults answer at once (holding for ASSIGN). */
  onAssign: (request: AssignRequest) => Promise<AssignResponse> = async () => new AssignResponse();
  onDrain: (request: DrainRequest) => Promise<void> = async () => {};
  onForward: (request: ForwardRequest) => Promise<void> = async () => {};
  onResume: (request: ResumeRequest) => Promise<void> = async () => {};

  /** Slots this instance believes it serves, with their epochs. */
  readonly serving = new Map<number, number>();

  /**
   * Sends an empty `EXTEND` every 50 ms while it handles a request, as an
   * instance written before declared deadlines did (they restart the naming
   * service's default timeout). Off: it sends none.
   */
  heartbeat = true;

  /** Receives the one-way messages (subscription events) of {@link channel}. */
  onEvent: ((event: TalkMessage) => void) | undefined;

  /** The deadline to declare with one `EXTEND` when a request arrives (undefined: none), by its procedure. */
  declare: ((procedure: string) => number | undefined) | undefined;

  constructor(harness: Harness, type: Name, id: number, log: string[]) {
    this.harness = harness;
    this.type = type;
    this.id = id;
    this.log = log;
  }

  get #clock(): Clock {
    return this.harness.clock ?? systemClock;
  }

  async start(options: { space?: SlotSpace; capacity?: number } = {}): Promise<void> {
    const [client, server] = this.harness.link();
    this.channel = client;
    this.server = server;
    void (async () => {
      try {
        for await (const message of client.messages) {
          this.#serve(message);
        }
      } catch {
        // The channel ended.
      }
    })();
    await register(client, this.type.toString(), { instance: this.id });
    if (options.space !== undefined) {
      await this.slots(options.space, { capacity: options.capacity ?? 1 });
    }
  }

  #serve(m: TalkMessage): void {
    if (!m.expectsReply) {
      this.onEvent?.(m);
      return;
    }
    if (!m.canReply) {
      return;
    }
    m.setReplyTimeout(0);
    const deadline = this.declare?.(m.procedureName);
    if (deadline !== undefined) {
      m.extend({ deadline }).catch(() => {});
    }
    const stop = periodic(this.#clock, ms50, () => {
      if (m.canReply && this.heartbeat) {
        m.extend().catch(() => {});
        return true;
      }
      return false;
    });
    this.#handle(m)
      .then(
        (payload) => {
          if (m.canReply) m.reply(payload).catch(() => {});
        },
        (error: unknown) => {
          if (m.canReply) {
            m.replyAbort(
              error instanceof SwitchboardError
                ? error.status
                : Status.of(StatusCode.unavailable, String(error)),
            ).catch(() => {});
          }
        },
      )
      .finally(stop);
  }

  async #handle(m: TalkMessage): Promise<Uint8Array> {
    const name = this.id.toString(16);
    switch (m.procedureName) {
      case 'ASSIGN': {
        const r = AssignRequest.decode(m.payload);
        this.log.push(
          `${name} ASSIGN ${r.slot} e${r.epoch} h${r.holder}${r.shared ? ' shared' : ''}`,
        );
        const response = await this.onAssign(r);
        this.serving.set(r.slot, r.epoch);
        return response.encode();
      }
      case 'DRAIN': {
        const r = DrainRequest.decode(m.payload);
        this.log.push(`${name} DRAIN ${r.slot} e${r.epoch} to${r.to}`);
        await this.onDrain(r);
        return new Uint8Array(0);
      }
      case 'FORWARD': {
        const r = ForwardRequest.decode(m.payload);
        this.log.push(`${name} FORWARD ${r.slot} e${r.epoch} to${r.to}`);
        this.serving.delete(r.slot);
        await this.onForward(r);
        return new Uint8Array(0);
      }
      case 'RESUME': {
        const r = ResumeRequest.decode(m.payload);
        this.log.push(`${name} RESUME ${r.slot} e${r.epoch}`);
        await this.onResume(r);
        return new Uint8Array(0);
      }
    }
    throw SwitchboardError.of(StatusCode.unimplemented);
  }

  async slots(space: SlotSpace, options: { capacity?: number } = {}): Promise<void> {
    await this.channel.request(
      'SLOTS',
      new SlotsRequest(space, { capacity: options.capacity ?? 1 }).encode(),
    );
  }

  async claim(slot: number, options: { holding?: boolean; epoch?: number } = {}): Promise<number> {
    const r = await this.channel.request(
      'CLAIM',
      new ClaimRequest(this.type, slot, options).encode(),
    );
    return ClaimResponse.decode(r.payload).epoch;
  }

  async release(slot: number, options: { keepStorage?: boolean } = {}): Promise<void> {
    await this.channel.request('RELEASE', new ReleaseRequest(this.type, slot, options).encode());
  }

  async holding(slots: readonly number[]): Promise<readonly number[]> {
    const r = await this.channel.request('HOLDING', new HoldingRequest(this.type, slots).encode());
    return HoldingResponse.decode(r.payload).discard;
  }

  async locate(slot: number): Promise<LocateResponse> {
    const r = await this.channel.request('LOCATE', new LocateRequest(this.type, slot).encode());
    return LocateResponse.decode(r.payload);
  }

  close(): Promise<void> {
    return this.channel.close();
  }
}

/** Starts a `MIGRATE` over [channel]; collects its `PHASE` items. */
export class Migration {
  readonly stream: TalkStream;
  readonly phases: string[] = [];
  readonly done: Promise<Status>;

  constructor(channel: TalkChannel, type: Name, slot: number, options: { to?: number } = {}) {
    const stream = channel.streamRequest(
      'MIGRATE',
      new MigrateRequest(type, slot, options).encode(),
    );
    this.stream = stream;
    void (async () => {
      try {
        for await (const m of stream.items) {
          this.phases.push(migrationPhaseName(PhaseItem.decode(m.payload).phase));
        }
      } catch {
        // Read through `done`.
      }
    })();
    this.done = stream.done.then(
      () => Status.ok,
      (error: unknown) => (error as SwitchboardError).status,
    );
  }
}

/**
 * A `TalkConnector` whose naming service can be swapped (a restart) and
 * whose links can drop frames to the client.
 */
export class SwitchableConnector {
  harness: Harness;
  /** Drops frames from the naming service to the client. */
  dropToClient: FrameFilter = () => false;
  /** Server sides of the links handed out, in order. */
  readonly servers: FakeTalkChannel[] = [];

  constructor(harness: Harness) {
    this.harness = harness;
  }

  readonly call = async (): Promise<TalkChannel> => {
    const [client, server] = this.harness.link({
      dropToClient: (frame) => this.dropToClient(frame),
    });
    this.servers.push(server);
    return client;
  };
}

/** A {@link SlotHandler} that records calls and can be scripted. */
export class RecordingHandler implements SlotHandler {
  readonly name: string;
  readonly log: string[];
  assign: (request: AssignRequest) => Promise<AssignResult> = async () => 'holding';
  drain: (request: DrainRequest) => Promise<void> = async () => {};
  forward: (request: ForwardRequest) => Promise<void> = async () => {};
  resume: (request: ResumeRequest) => Promise<void> = async () => {};
  readonly revoked: string[] = [];
  /** Every `onDiscard`, as `type: slots`. */
  readonly discarded: string[] = [];
  /** The context of the latest request of each kind and slot, by `ASSIGN 3`, `DRAIN 3` and so on; set before the script runs. */
  readonly contexts = new Map<string, SlotRequestContext>();

  constructor(name: string, log: string[]) {
    this.name = name;
    this.log = log;
  }

  onAssign(r: AssignRequest, context: SlotRequestContext): Promise<AssignResult> {
    this.log.push(`${this.name} ASSIGN ${r.slot} e${r.epoch} h${r.holder}`);
    this.contexts.set(`ASSIGN ${r.slot}`, context);
    return this.assign(r);
  }

  onDrain(r: DrainRequest, context: SlotRequestContext): Promise<void> {
    this.log.push(`${this.name} DRAIN ${r.slot} e${r.epoch} to${r.to}`);
    this.contexts.set(`DRAIN ${r.slot}`, context);
    return this.drain(r);
  }

  onForward(r: ForwardRequest, context: SlotRequestContext): Promise<void> {
    this.log.push(`${this.name} FORWARD ${r.slot} e${r.epoch} to${r.to}`);
    this.contexts.set(`FORWARD ${r.slot}`, context);
    return this.forward(r);
  }

  onResume(r: ResumeRequest, context: SlotRequestContext): Promise<void> {
    this.log.push(`${this.name} RESUME ${r.slot} e${r.epoch}`);
    this.contexts.set(`RESUME ${r.slot}`, context);
    return this.resume(r);
  }

  async onRevoke(type: Name, slot: number): Promise<void> {
    this.log.push(`${this.name} REVOKE ${slot}`);
    this.revoked.push(`${type}/${slot}`);
  }

  onDiscard(type: Name, slots: readonly number[]): void {
    this.discarded.push(`${type}: ${slots.join(',')}`);
  }
}

/**
 * A scripted naming service over one link at a time, for timing what the
 * client sends: answers at once `REGISTER` (the id asked for), `SLOTS`,
 * `HOLDING` (nothing to discard) and `CLAIM` (the epoch claimed, unless
 * {@link answerClaims} is off), and `WATCH` once {@link watch} resolves.
 * Records the requests in {@link log}.
 */
export class ScriptedNaming {
  /** The client's options. */
  readonly options: Partial<TalkOptions>;
  readonly clock: Clock | undefined;
  readonly log: string[] = [];
  readonly watch = new Deferred<void>();
  answerClaims = true;
  connects = 0;
  /** The service side of the latest link. */
  server!: FakeTalkChannel;

  constructor(options: { maxOutgoingRequests?: number; clock?: Clock } = {}) {
    this.clock = options.clock;
    this.options = {
      requestTimeout: 2000,
      replyTimeout: 1000,
      maxOutgoingRequests: options.maxOutgoingRequests ?? 1024,
      ...(options.clock === undefined ? {} : { clock: options.clock }),
    };
  }

  readonly connect = async (): Promise<TalkChannel> => {
    this.connects++;
    const clock = this.clock === undefined ? {} : { clock: this.clock };
    const [client, server] = fakeTalkPair({
      a: this.options,
      b: { ...serverOptions, ...clock },
    });
    this.server = server;
    void (async () => {
      try {
        for await (const message of server.messages) {
          this.#serve(message);
        }
      } catch {
        // The link ended.
      }
    })();
    return client;
  };

  #serve(m: TalkMessage): void {
    if (!m.canReply) {
      return;
    }
    const reply = (payload: Uint8Array): void => {
      m.reply(payload).catch(() => {});
    };
    switch (m.procedureName) {
      case 'WATCH':
        this.log.push('WATCH');
        m.setReplyTimeout(0);
        void this.watch.promise.then(() => {
          if (m.canReply) reply(new Uint8Array(0));
        });
        break;
      case 'REGISTER': {
        const r = RegisterRequest.decode(m.payload);
        this.log.push(`REGISTER ${r.type}/${r.requestedInstance}`);
        reply(new RegisterResponse(r.requestedInstance).encode());
        break;
      }
      case 'SLOTS':
        this.log.push(`SLOTS ${SlotsRequest.decode(m.payload).space.type}`);
        reply(new Uint8Array(0));
        break;
      case 'HOLDING': {
        const r = HoldingRequest.decode(m.payload);
        this.log.push(`HOLDING ${r.type} ${r.slots.join(',')}`);
        reply(new HoldingResponse([]).encode());
        break;
      }
      case 'CLAIM': {
        const r = ClaimRequest.decode(m.payload);
        this.log.push(`CLAIM ${r.type}/${r.slot} e${r.epoch}`);
        if (this.answerClaims) {
          reply(new ClaimResponse(r.epoch).encode());
        } else {
          m.setReplyTimeout(0);
        }
        break;
      }
      default:
        this.log.push(m.procedureName);
        reply(new Uint8Array(0));
    }
  }
}
