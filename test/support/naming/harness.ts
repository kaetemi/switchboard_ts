/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Shared helpers for the naming service, client and resolver tests: the
// port of the Dart `naming_harness.dart`, over the in-memory Talk pair.

import { expect } from 'vitest';
import { utf8Decode } from '../../../src/core/bytes.ts';
import { Name } from '../../../src/core/name.ts';
import {
  decodeDownEvent,
  decodeUpEvent,
  RegisterRequest,
  RegisterResponse,
  UnregisterRequest,
  WatchRequest,
} from '../../../src/core/naming/naming-protocol.ts';
import {
  type NamingConnection,
  NamingService,
  type NamingServiceOptions,
} from '../../../src/core/naming/naming-service.ts';
import { StatusCode, SwitchboardError, statusCodeName } from '../../../src/core/status.ts';
import type { ServiceEvent } from '../../../src/core/switchboard/contract.ts';
import type { TalkChannel, TalkMessage, TalkOptions } from '../../../src/core/talk/contract.ts';
import type { Clock } from '../../../src/core/util/clock.ts';
import {
  type FakeFrame,
  type FakeTalkChannel,
  type FrameFilter,
  fakeTalkPair,
} from './fake-talk.ts';
import { flushMicrotasks } from './test-clock.ts';

export const ms10 = 10;
export const ms50 = 50;
export const reconnectDelay = 20;

/** Upper bound for anything a test waits on; only reached on failure. */
export const timeout = 2000;

/** Short timeouts, so that a request left unanswered shows quickly. */
export const serverOptions: Partial<TalkOptions> = { replyTimeout: 200, requestTimeout: 300 };
export const clientOptions: Partial<TalkOptions> = { replyTimeout: 200, requestTimeout: 300 };

export const uriA = 'tcp://10.0.0.5:9101';
export const uriB = 'ws://10.0.0.6:9100/ws';

/** The bytes of [values]. */
export function bytes(values: readonly number[]): Uint8Array {
  return Uint8Array.from(values);
}

/** Awaits [promise] and checks that it rejects with a `SwitchboardError` of [code]; returns it. */
export async function rejectsWith(
  promise: Promise<unknown>,
  code: StatusCode,
): Promise<SwitchboardError> {
  let error: unknown;
  try {
    await promise;
  } catch (e) {
    error = e;
  }
  expect(error, `expected a ${statusCodeName(code)} rejection`).toBeInstanceOf(SwitchboardError);
  expect(statusCodeName((error as SwitchboardError).code)).toBe(statusCodeName(code));
  return error as SwitchboardError;
}

/** Checks that [action] throws a `SwitchboardError` of [code] at once. */
export function throwsWith(action: () => unknown, code: StatusCode): SwitchboardError {
  let error: unknown;
  try {
    action();
  } catch (e) {
    error = e;
  }
  expect(error, `expected a ${statusCodeName(code)} error`).toBeInstanceOf(SwitchboardError);
  expect(statusCodeName((error as SwitchboardError).code)).toBe(statusCodeName(code));
  return error as SwitchboardError;
}

/** Lets pending microtasks and deliveries run (the Dart tests' `pump()`). */
export function pump(): Promise<void> {
  return flushMicrotasks();
}

/** Waits [ms] of real time. */
export function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Polls [condition] every few milliseconds of real time; fails after {@link timeout}. */
export async function until(condition: () => boolean, within = timeout): Promise<void> {
  const deadline = Date.now() + within;
  while (!condition()) {
    if (Date.now() > deadline) {
      throw new Error(`condition not met within ${within} ms`);
    }
    await delay(2);
  }
}

/** Rejects with a timeout error unless [promise] settles within [ms] of real time. */
export function within<T>(promise: Promise<T>, ms = timeout): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  return Promise.race([
    promise,
    new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`not settled within ${ms} ms`)), ms);
    }),
  ]).finally(() => clearTimeout(timer));
}

/** Whether [promise] has settled after the pending microtasks ran. */
export async function isSettled(promise: Promise<unknown>): Promise<boolean> {
  let settled = false;
  promise.then(
    () => {
      settled = true;
    },
    () => {
      settled = true;
    },
  );
  await flushMicrotasks();
  return settled;
}

/** What a link takes. */
export interface LinkOptions {
  /**
   * Deliver synchronously, so that everything the service sends in one step
   * reaches the client's channel in one burst, as the mux delivers a batch
   * of frames.
   */
  readonly sync?: boolean;
  /** The client's Talk options. */
  readonly options?: Partial<TalkOptions>;
  /** Drops frames from the client to the naming service. */
  readonly dropToService?: FrameFilter;
  /** Drops frames from the naming service to the client. */
  readonly dropToClient?: FrameFilter;
  /** The naming service's Talk options; default {@link serverOptions}. */
  readonly serverOptions?: Partial<TalkOptions>;
  /** The connection the service reads the channel's identity from (`serve`). */
  readonly connection?: NamingConnection;
  /** The application payload of the channel's OPEN, a credential (`serve`). */
  readonly credential?: Uint8Array;
}

/**
 * A naming service and in-memory links to it. No assignment hold and no
 * settle window after `SLOTS` unless asked for (most tests assign fresh
 * slots and check the timing of what follows; {@link productionSettle} opts
 * in to the service's default window); the `ASSIGN` backoff and the
 * hand-over bound of the service by default. With a `clock`, the service
 * and every link run on it.
 */
export class Harness {
  /** The service's default `holdingSettle`, for tests that run with it. */
  static readonly productionSettle = 1000;

  readonly service: NamingService;
  readonly clock: Clock | undefined;
  /** Server sides of every link, in creation order. */
  readonly servers: FakeTalkChannel[] = [];
  /** Client sides of every link, in creation order. */
  readonly clients: FakeTalkChannel[] = [];

  constructor(options: NamingServiceOptions = {}) {
    this.clock = options.clock;
    this.service = new NamingService({
      assignmentHold: 0,
      holderGrace: 300_000,
      handoverTimeout: 300,
      assignBackoff: 200,
      assignBackoffMax: 10_000,
      holdingSettle: 0,
      handoverMaxDuration: 600_000,
      resumeAttempts: 5,
      ...options,
    });
  }

  /** Opens a link: the server side is served, both sides are returned. */
  link(options: LinkOptions = {}): [FakeTalkChannel, FakeTalkChannel] {
    const clock = this.clock === undefined ? {} : { clock: this.clock };
    const [client, server] = fakeTalkPair({
      a: { ...clientOptions, ...clock, ...options.options },
      b: { ...serverOptions, ...clock, ...options.serverOptions },
      sync: options.sync ?? false,
      ...(options.dropToService === undefined ? {} : { dropAtoB: options.dropToService }),
      ...(options.dropToClient === undefined ? {} : { dropBtoA: options.dropToClient }),
    });
    this.servers.push(server);
    this.clients.push(client);
    this.service.serve(server, {
      ...(options.connection === undefined ? {} : { connection: options.connection }),
      ...(options.credential === undefined ? {} : { credential: options.credential }),
    });
    return [client, server];
  }

  async close(): Promise<void> {
    await this.service.close();
    await Promise.all(this.clients.map((c) => c.close()));
  }
}

/**
 * A `TalkConnector` over a {@link Harness} that can be switched off. With
 * `sync`, its links deliver synchronously (see {@link LinkOptions.sync});
 * `options` are the client's.
 */
export class Connector {
  harness: Harness;
  readonly sync: boolean;
  readonly options: Partial<TalkOptions>;
  /** While true, connecting fails. */
  down = false;
  calls = 0;
  /** Server sides of the links handed out. */
  readonly servers: FakeTalkChannel[] = [];

  constructor(harness: Harness, options: { sync?: boolean; options?: Partial<TalkOptions> } = {}) {
    this.harness = harness;
    this.sync = options.sync ?? false;
    this.options = options.options ?? clientOptions;
  }

  readonly call = async (): Promise<TalkChannel> => {
    this.calls++;
    if (this.down) {
      throw SwitchboardError.of(StatusCode.unavailable, 'service down');
    }
    const [client, server] = this.harness.link({ sync: this.sync, options: this.options });
    this.servers.push(server);
    return client;
  };
}

/**
 * A `TalkConnector` over a {@link Harness} whose links pass every frame
 * through filters: a frame for which {@link dropToService} or
 * {@link dropToClient} returns true is lost on the way.
 */
export class LossyConnector {
  readonly harness: Harness;
  /** Drops frames from the client to the naming service. */
  dropToService: FrameFilter = () => false;
  /** Drops frames from the naming service to the client. */
  dropToClient: FrameFilter = () => false;
  calls = 0;

  constructor(harness: Harness) {
    this.harness = harness;
  }

  readonly call = async (): Promise<TalkChannel> => {
    this.calls++;
    const [client] = this.harness.link({
      dropToService: (frame) => this.dropToService(frame),
      dropToClient: (frame) => this.dropToClient(frame),
    });
    return client;
  };
}

/** True for the final response to a request (not a stream item, extend or abort). */
export function isFinalResponse(frame: FakeFrame): boolean {
  return frame.kind === 'message' && frame.responseId !== 0 && !frame.item;
}

/** True for a request for [procedure]. */
export function isRequestFor(procedure: string): FrameFilter {
  return (frame) =>
    frame.kind === 'message' && frame.requestId !== 0 && frame.procedure.toString() === procedure;
}

/**
 * Registers over a raw Talk channel and returns the instance id. The answer
 * is ordered with the channel's messages: a {@link Watcher} on the same
 * channel has seen the `UP` when this resolves.
 */
export async function register(
  channel: TalkChannel,
  type: string,
  options: { instance?: number; endpoints?: readonly string[]; metadata?: Uint8Array } = {},
): Promise<number> {
  const response = await channel.request(
    'REGISTER',
    new RegisterRequest(Name.of(type), {
      requestedInstance: options.instance ?? 0,
      endpoints: options.endpoints ?? [],
      ...(options.metadata === undefined ? {} : { metadata: options.metadata }),
    }).encode(),
    { ordered: true },
  );
  return RegisterResponse.decode(response.payload).instance;
}

/** Unregisters over a raw Talk channel; ordered like {@link register}. */
export async function unregister(
  channel: TalkChannel,
  type: string,
  instance: number,
): Promise<void> {
  await channel.request('UNREGSTR', new UnregisterRequest(Name.of(type), instance).encode(), {
    ordered: true,
  });
}

/** A queue of lines read one by one, with a timeout. */
export class LineQueue {
  readonly #lines: string[] = [];
  readonly #waiters: ((line: string) => void)[] = [];

  push(line: string): void {
    const waiter = this.#waiters.shift();
    if (waiter !== undefined) {
      waiter(line);
    } else {
      this.#lines.push(line);
    }
  }

  /** The next line, waiting at most {@link timeout} of real time. */
  next(ms = timeout): Promise<string> {
    const line = this.#lines.shift();
    if (line !== undefined) {
      return Promise.resolve(line);
    }
    return new Promise<string>((resolve, reject) => {
      const waiter = (value: string): void => {
        clearTimeout(timer);
        resolve(value);
      };
      const timer = setTimeout(() => {
        const index = this.#waiters.indexOf(waiter);
        if (index >= 0) this.#waiters.splice(index, 1);
        reject(new Error(`no line within ${ms} ms`));
      }, ms);
      this.#waiters.push(waiter);
    });
  }

  /** The next [count] lines. */
  async take(count: number): Promise<string[]> {
    const out: string[] = [];
    for (let i = 0; i < count; i++) {
      out.push(await this.next());
    }
    return out;
  }

  /** The lines queued and not read yet. */
  get pending(): readonly string[] {
    return this.#lines;
  }
}

/**
 * The subscriptions of one raw Talk channel to the naming table, read one by
 * one as strings in channel order: the events (`UP npc/1`, `DOWN npc/1`, ...
 * through the describer) and the answers to {@link watch} and
 * {@link unwatch} (`WATCHED`, `UNWATCHED`, or for instance
 * `UNWATCH notFound`). Everything read is also in {@link log} at once.
 */
export class Watcher {
  readonly channel: TalkChannel;
  readonly describer: (message: TalkMessage) => string;
  /** Everything read so far, in order. */
  readonly log: string[] = [];
  readonly #lines = new LineQueue();

  /**
   * Takes [channel]'s messages unless `fed`, then sends `WATCH` with `type`
   * or `payload` unless `start` is false. A `fed` watcher's channel has its
   * messages taken elsewhere: that listener passes the one-way messages to
   * {@link add}.
   */
  constructor(
    channel: TalkChannel,
    options: {
      type?: string;
      payload?: Uint8Array;
      start?: boolean;
      fed?: boolean;
      describer?: (message: TalkMessage) => string;
    } = {},
  ) {
    this.channel = channel;
    this.describer = options.describer ?? describe;
    if (options.fed === true) {
      return;
    }
    void (async () => {
      try {
        for await (const message of channel.messages) {
          this.add(message);
        }
      } catch {
        // The channel ended.
      }
    })();
    if (options.start ?? true) {
      this.watch({
        ...(options.type === undefined ? {} : { type: options.type }),
        ...(options.payload === undefined ? {} : { payload: options.payload }),
      }).catch(() => {});
    }
  }

  /** Takes one message of the channel. */
  add(message: TalkMessage): void {
    this.line(this.describer(message));
  }

  /** Adds a line of the test's own. */
  line(line: string): void {
    this.log.push(line);
    this.#lines.push(line);
  }

  /** Sends `WATCH` (all types unless `type` or `payload`); its answer is read in its place among the events. */
  watch(options: { type?: string; payload?: Uint8Array } = {}): Promise<void> {
    return this.#subscription('WATCH', options);
  }

  /** Sends `UNWATCH`, like {@link watch}. */
  unwatch(options: { type?: string; payload?: Uint8Array } = {}): Promise<void> {
    return this.#subscription('UNWATCH', options);
  }

  async #subscription(
    procedure: string,
    options: { type?: string; payload?: Uint8Array },
  ): Promise<void> {
    const payload =
      options.payload ??
      new WatchRequest(options.type === undefined ? undefined : Name.of(options.type)).encode();
    try {
      await this.channel.request(procedure, payload, { ordered: true });
    } catch (error) {
      this.line(
        `${procedure} ${
          error instanceof SwitchboardError
            ? (statusCodeName(error.code) ?? error.status.toString())
            : String(error)
        }`,
      );
      throw error;
    }
    this.line(`${procedure}ED`);
  }

  get next(): Promise<string> {
    return this.#lines.next();
  }

  take(count: number): Promise<string[]> {
    return this.#lines.take(count);
  }

  /** Lines up to and including the first that satisfies [test]. */
  async until(test: (line: string) => boolean): Promise<string[]> {
    const out: string[] = [];
    for (;;) {
      const line = await this.next;
      out.push(line);
      if (test(line)) {
        return out;
      }
    }
  }
}

/** `UP npc/1 tcp://...`, `DOWN npc/1`, or the procedure name. */
export function describe(m: TalkMessage): string {
  switch (m.procedureName) {
    case 'UP':
      return describeEvent(decodeUpEvent(m.payload));
    case 'DOWN':
      return describeEvent(decodeDownEvent(m.payload));
    default:
      return m.procedureName;
  }
}

export function describeEvent(e: ServiceEvent): string {
  if (!e.up) {
    return `DOWN ${e.record.address}`;
  }
  const parts = [`UP ${e.record.address}`];
  const endpoints = e.record.endpoints.join(',');
  if (endpoints.length > 0) parts.push(endpoints);
  if (e.record.metadata.length > 0) parts.push(`meta ${utf8Decode(e.record.metadata)}`);
  return parts.join(' ');
}

/** A seeded pseudo-random source (mulberry32) for the fuzz tests. */
export class SeededRandom {
  #state: number;

  constructor(seed: number) {
    this.#state = seed >>> 0;
  }

  /** An integer in `0 .. max - 1`. */
  nextInt(max: number): number {
    this.#state = (this.#state + 0x6d2b79f5) >>> 0;
    let t = this.#state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    const value = ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    return Math.floor(value * max);
  }
}

/**
 * Random bytes on even [i]; on odd [i] something shaped like a naming
 * payload (name, u48, endpoint list, then mostly a metadata field, of any
 * size, possibly cut short) with noisy, URI-ish endpoints.
 */
export function fuzzInput(random: SeededRandom, i: number): Uint8Array {
  const noise = (n: number): number[] => Array.from({ length: n }, () => random.nextInt(256));
  if (i % 2 === 0) {
    return Uint8Array.from(noise(random.nextInt(64)));
  }
  const b: number[] = [...Name.of('npc').bytes];
  b.push(...(random.nextInt(4) === 0 ? [0, 0, 0, 0, 0, 0] : noise(6)));
  const count = random.nextInt(4);
  b.push(count);
  for (let k = 0; k < count; k++) {
    if (random.nextInt(4) === 0) {
      // Random bytes, possibly cut short.
      const length = random.nextInt(256);
      b.push(length, ...noise(random.nextInt(length + 2)));
      continue;
    }
    // Short runs of the pieces that trip up URI normalisation: dot
    // segments, backslashes, empty authorities and ports, stray escapes.
    const pieces = [
      '/.',
      '/..',
      './',
      '../',
      '/',
      '//',
      '.',
      '..',
      '\\',
      ':',
      '{',
      '}',
      '#',
      '?',
      '%',
      '%41',
      '%zz',
      '@',
      '[',
      ']',
      'h',
      '1',
      ' ',
      '\u00e9',
      '\u00ff',
      '"',
      '<',
      '^',
      '|',
      '`',
    ];
    const prefixes = ['', 'tcp:', 'ws:', 'tcp:/', 'tcp://', 'ws://h/', 'tcp://h:', 'a:/'];
    let text = prefixes[random.nextInt(prefixes.length)] as string;
    for (let n = random.nextInt(12); n > 0; n--) {
      text += pieces[random.nextInt(pieces.length)] as string;
    }
    const encoded = new TextEncoder().encode(text);
    const length = Math.min(encoded.length, 255);
    b.push(length, ...encoded.subarray(0, length));
  }
  switch (random.nextInt(4)) {
    case 0:
      // No metadata field: the layout before it was added.
      break;
    case 1:
      // A length, possibly over the limit, and up to 8 bytes.
      b.push(random.nextInt(256), random.nextInt(3) === 0 ? random.nextInt(256) : 0);
      b.push(...noise(random.nextInt(9)));
      break;
    default: {
      const length = random.nextInt(8);
      b.push(length, 0, ...noise(length));
    }
  }
  return Uint8Array.from(b);
}
