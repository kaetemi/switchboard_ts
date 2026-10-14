/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Shared helpers for the frame mux tests: the port of `test/mux_harness.dart`
// (`quiet`, `rawOptions`, `muxPair`, `RawPeer`, `rawPair`, `GatedTransport`,
// `gatedPair`), plus what Dart's streams give for free (subscriptions that
// pause, `StreamQueue`) and byte pipes for the stream transport.
//
// Time: every connection the helpers make runs on a `SteppingClock` (one
// per pair, shared by both ends and by the memory pair), never on the
// system clock. `clock.elapse(ms)` moves from timer to timer and lets the
// microtasks each one starts run before the next, which is what
// `fake_async`'s `elapse` does and what the mux needs: it re-arms its
// keep-alive, grace and close timers in promise continuations (the read
// loop, the close sequence), which a plain `ManualClock.advance` would not
// let run between two timers. `settle()` is Dart's `pumpEventQueue`. A
// test that never elapses never fires a timer, so nothing waits on real
// time.
//
// The memory pair hands a frame over when the writable takes it and the
// mux reads it in a later microtask: no peer answers synchronously during
// a write, as Dart's `StreamChannelController(sync: true)` does. The tests
// that rely on that say how they are adapted where they are.

import { expect } from 'vitest';
import { bytesEqual, hexBytes, hexString } from '../../../src/core/bytes.ts';
import type { MuxChannel, MuxConnection, MuxOptions } from '../../../src/core/mux/contract.ts';
import {
  FrameMuxConnection,
  type FrameMuxConnectionOptions,
} from '../../../src/core/mux/frame-mux-connection.ts';
import {
  MuxControlMessage,
  MuxControlType,
  MuxCredit,
  MuxFrame,
  MuxLimits,
} from '../../../src/core/mux/mux-frame.ts';
import { type Status, type StatusCode, SwitchboardError } from '../../../src/core/status.ts';
import type { ByteInput, ByteOutput } from '../../../src/core/transport/byte-io.ts';
import type { FrameTransport } from '../../../src/core/transport/contract.ts';
import {
  type MemoryTransport,
  type MemoryTransportOptions,
  memoryTransportPair,
} from '../../../src/core/transport/memory-transport.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';
import { SteppingClock, settle } from '../../support/node/stepping-clock.ts';

export { SteppingClock, settle };

/** Options of a test connection; `clock` defaults to the pair's {@link SteppingClock}. */
export type TestMuxOptions = Partial<MuxOptions>;

/** Keep-alive off so that tests only see the frames they cause. */
export const quiet: TestMuxOptions = Object.freeze({
  keepAliveInterval: undefined,
  goAwayGrace: 2000,
});

/**
 * Additionally no LIMITS, for byte exact raw peer tests, and DATA sent
 * without waiting for the raw peer's LIMITS (which it may never send).
 */
export const rawOptions: TestMuxOptions = Object.freeze({
  keepAliveInterval: undefined,
  announceLimits: false,
  awaitPeerLimits: false,
});

/** An empty payload. */
export const empty = new Uint8Array(0);

/** The {@link SteppingClock} [connection] runs on. */
export function clockOf(connection: MuxConnection): SteppingClock {
  const clock = connection.options.clock;
  if (!(clock instanceof SteppingClock)) {
    throw new Error(`${connection} does not run on a SteppingClock`);
  }
  return clock;
}

function connect(
  transport: FrameTransport,
  isInitiator: boolean,
  options: TestMuxOptions,
  clock: SteppingClock,
): FrameMuxConnection {
  const init: FrameMuxConnectionOptions = { clock, ...options, isInitiator };
  return new FrameMuxConnection(transport, init);
}

/** What the pair helpers take. */
export interface PairOptions {
  readonly initiator?: TestMuxOptions;
  readonly acceptor?: TestMuxOptions;
  /** Shared by both connections and the memory pair; a new one by default. */
  readonly clock?: SteppingClock;
  /** For the memory pair (its `clock` is the pair's). */
  readonly transport?: MemoryTransportOptions;
}

/** Two connections over a memory pair, the first the initiator. */
export function muxPair(options: PairOptions = {}): [FrameMuxConnection, FrameMuxConnection] {
  const clock = options.clock ?? new SteppingClock();
  const [a, b] = memoryTransportPair({ clock, ...options.transport });
  return [
    connect(a, true, options.initiator ?? quiet, clock),
    connect(b, false, options.acceptor ?? quiet, clock),
  ];
}

/** Reads every frame of [transport] as it comes; one reader per transport. */
export class FrameReader {
  readonly #reader: ReadableStreamDefaultReader<Uint8Array>;

  constructor(readable: ReadableStream<Uint8Array>) {
    this.#reader = readable.getReader();
  }

  /** The next frame's bytes. @throws {Error} once the transport ended. */
  async nextBytes(): Promise<Uint8Array> {
    const result = await this.#reader.read();
    if (result.done) {
      throw new Error('the transport ended');
    }
    return result.value;
  }

  /** Whether another frame comes before the end: Dart's `StreamQueue.hasNext`, consuming it. */
  async hasNext(): Promise<boolean> {
    const result = await this.#reader.read();
    return !result.done;
  }

  /** Every frame until the transport ends. */
  async rest(): Promise<Uint8Array[]> {
    const out: Uint8Array[] = [];
    for (;;) {
      const result = await this.#reader.read();
      if (result.done) {
        return out;
      }
      out.push(result.value);
    }
  }

  /** Stops reading. */
  cancel(): Promise<void> {
    return this.#reader.cancel().catch(() => {});
  }
}

/** The far side of a transport, driven with raw frames. */
export class RawPeer {
  readonly transport: FrameTransport;
  readonly frames: FrameReader;
  readonly #writer: WritableStreamDefaultWriter<Uint8Array>;

  constructor(transport: FrameTransport) {
    this.transport = transport;
    this.frames = new FrameReader(transport.readable);
    this.#writer = transport.writable.getWriter();
    this.#writer.closed.catch(() => {});
  }

  /** Writes the frame [hex]. */
  send(hex: string): void {
    this.sendBytes(hexBytes(hex));
  }

  /** Writes [bytes] as one frame (Dart's `raw.transport.sink.add`). */
  sendBytes(bytes: Uint8Array): void {
    this.#writer.write(bytes).catch(() => {});
  }

  /** Closes this side of the transport (Dart's `raw.transport.sink.close()`). */
  close(): Promise<void> {
    return this.#writer.close().catch(() => {});
  }

  /** Fails the transport (Dart's `sink.addError`): aborts the writable with [reason]. */
  abort(reason?: unknown): Promise<void> {
    return this.#writer.abort(reason).catch(() => {});
  }

  /** The next frame, as hex. */
  async nextHex(): Promise<string> {
    return hexString(await this.frames.nextBytes());
  }

  /** The next frame, decoded. */
  async next(): Promise<MuxFrame> {
    return MuxFrame.decode(await this.frames.nextBytes());
  }

  /** Skips frames until a control message of [type] and returns it. */
  async nextControl(type: MuxControlType): Promise<MuxControlMessage> {
    for (;;) {
      const frame = await this.next();
      if (frame.channelId === 0) {
        const message = MuxControlMessage.decode(frame.payload);
        if (message.knownType === type) {
          return message;
        }
      }
    }
  }

  /** Every frame until the transport closes. */
  async rest(): Promise<MuxFrame[]> {
    return (await this.frames.rest()).map((bytes) => MuxFrame.decode(bytes));
  }
}

/** What {@link rawPair} takes. */
export interface RawPairOptions {
  readonly muxIsInitiator?: boolean;
  readonly options?: TestMuxOptions;
  readonly clock?: SteppingClock;
  readonly transport?: MemoryTransportOptions;
}

/** A connection over a memory pair, and a {@link RawPeer} on the other side. */
export function rawPair(options: RawPairOptions = {}): [FrameMuxConnection, RawPeer] {
  const clock = options.clock ?? new SteppingClock();
  const [a, b] = memoryTransportPair({ clock, ...options.transport });
  return [
    connect(a, options.muxIsInitiator ?? true, options.options ?? rawOptions, clock),
    new RawPeer(b),
  ];
}

/** Whether [frame] is a control message of [type]. */
export function isControl(frame: MuxFrame, type: MuxControlType): boolean {
  return frame.channelId === 0 && MuxControlMessage.decode(frame.payload).knownType === type;
}

/** A LIMITS frame announcing [window] (and [maxFrameSize], [maxChannels]), as hex. */
export function limitsHex(window: number, maxFrameSize = 0, maxChannels = 0): string {
  return hexString(
    MuxControlMessage.limits(new MuxLimits({ maxFrameSize, maxChannels, initialWindow: window }))
      .toFrame()
      .encode(),
  );
}

/** A CREDIT frame for [id] of [bytes], as hex. */
export function creditHex(id: number, bytes: number): string {
  return hexString(MuxControlMessage.credit(new MuxCredit(id, bytes)).toFrame().encode());
}

/** A DATA frame on [id] with [length] payload bytes, as hex. */
export function dataHex(id: number, length: number): string {
  return hexString(MuxFrame.data(id, new Uint8Array(length)).encode());
}

/** An OPEN frame on [id] with [length] payload bytes, as hex. */
export function openHex(id: number, length = 0): string {
  return hexString(MuxFrame.open(id, new Uint8Array(length)).encode());
}

/** Sends PING and waits for its PONG: the frames sent before it were handled. */
export async function settleRaw(raw: RawPeer): Promise<void> {
  raw.send('02 00 00 01 5E');
  await raw.nextControl(MuxControlType.pong);
}

// Writes the mux makes, seen as it makes them. A `WritableStream` calls its
// sink for the first write at once but for the next ones only after a
// microtask each; a transport that must count frames as the mux writes
// them (Dart's synchronous `sink.add`) hooks the writer the mux takes.
function hookWrites(
  writable: WritableStream<Uint8Array>,
  onWrite: (frame: Uint8Array) => void,
): WritableStream<Uint8Array> {
  const getWriter = writable.getWriter.bind(writable);
  writable.getWriter = () => {
    const writer = getWriter();
    const write = writer.write.bind(writer);
    writer.write = (chunk?: Uint8Array) => {
      const written = write(chunk);
      if (chunk !== undefined) {
        onWrite(chunk);
      }
      return written;
    };
    return writer;
  };
  return writable;
}

/** A transport that hands over what [inner] carries and sees each frame written as it is written. */
function forward(
  inner: FrameTransport,
  onWrite: (frame: Uint8Array) => void,
): WritableStream<Uint8Array> {
  const writer = inner.writable.getWriter();
  writer.closed.catch(() => {});
  return hookWrites(
    new WritableStream<Uint8Array>({
      write: (frame) => writer.write(frame),
      close: () => writer.close(),
      abort: (reason: unknown) => writer.abort(reason),
    }),
    onWrite,
  );
}

/**
 * A transport the mux writes through to [inner], which the test can still
 * close or fail from outside: Dart's `transport.sink.close()` on a sink
 * the mux also writes to.
 */
export class Tap implements FrameTransport {
  readonly readable: ReadableStream<Uint8Array>;
  readonly writable: WritableStream<Uint8Array>;
  readonly maxFrameSize: number;
  readonly #writer: WritableStreamDefaultWriter<Uint8Array>;

  constructor(inner: FrameTransport) {
    this.readable = inner.readable;
    this.maxFrameSize = inner.maxFrameSize ?? 0;
    const writer = inner.writable.getWriter();
    writer.closed.catch(() => {});
    this.#writer = writer;
    this.writable = new WritableStream<Uint8Array>({
      write: (frame) => writer.write(frame),
      close: () => writer.close(),
      abort: (reason: unknown) => writer.abort(reason),
    });
  }

  /** Closes the inner transport's output, as its own side closing. */
  close(): Promise<void> {
    return this.#writer.close().catch(() => {});
  }
}

/**
 * A transport that takes output only as the test allows: an output-ready
 * transport ({@link FrameTransport.isOutputReady}) over [inner], for tests
 * of the output scheduler.
 *
 * It is ready while {@link allow} has granted frames; each frame written
 * then uses one. Frames written while it is not ready (control messages,
 * which the mux never holds) go through all the same. {@link written}
 * records every frame in order, as the mux writes it.
 */
export class GatedTransport implements FrameTransport {
  readonly inner: FrameTransport;
  readonly readable: ReadableStream<Uint8Array>;
  readonly writable: WritableStream<Uint8Array>;
  readonly written: MuxFrame[] = [];
  #allowance = 0;
  #waiter: Deferred<void> | undefined;

  constructor(inner: FrameTransport) {
    this.inner = inner;
    this.readable = inner.readable;
    this.writable = forward(inner, (frame) => {
      if (this.#allowance > 0) {
        this.#allowance--;
      }
      this.written.push(MuxFrame.decode(frame));
    });
  }

  get isOutputReady(): boolean {
    return this.#allowance > 0;
  }

  get outputReady(): Promise<void> {
    if (this.isOutputReady) {
      return Promise.resolve();
    }
    this.#waiter ??= new Deferred<void>();
    return this.#waiter.promise;
  }

  /** Takes [frames] more frames; the mux writes them on its next turn. */
  allow(frames: number): void {
    this.#allowance += frames;
    const waiter = this.#waiter;
    if (waiter !== undefined && this.isOutputReady) {
      this.#waiter = undefined;
      waiter.resolve();
    }
  }

  /** Written frames on [channelId], or every frame but control messages. */
  on(channelId?: number): MuxFrame[] {
    return this.written.filter((frame) =>
      channelId === undefined ? frame.channelId !== 0 : frame.channelId === channelId,
    );
  }
}

/**
 * Two connections over a memory pair whose initiator side is gated
 * ({@link GatedTransport}, not ready until the test allows frames).
 */
export function gatedPair(
  options: PairOptions = {},
): [FrameMuxConnection, FrameMuxConnection, GatedTransport] {
  const clock = options.clock ?? new SteppingClock();
  const [a, b] = memoryTransportPair({ clock, ...options.transport });
  const gate = new GatedTransport(a);
  return [
    connect(gate, true, options.initiator ?? quiet, clock),
    connect(b, false, options.acceptor ?? quiet, clock),
    gate,
  ];
}

/**
 * A transport whose peer answers each frame the mux writes as it writes
 * it: the port of Dart's `StreamChannelController(sync: true)` peers.
 * [reply] sees every frame as it is written; the frames it returns are
 * queued for the mux at once, before the write returns. The mux reads
 * them in a later microtask all the same, since it reads the transport
 * in a loop: the answer is never handled during the write.
 */
export class AnsweringTransport implements FrameTransport {
  readonly readable: ReadableStream<Uint8Array>;
  readonly writable: WritableStream<Uint8Array>;
  readonly written: Uint8Array[] = [];
  #controller!: ReadableStreamDefaultController<Uint8Array>;
  #ended = false;

  constructor(reply: (frame: Uint8Array, transport: AnsweringTransport) => readonly Uint8Array[]) {
    this.readable = new ReadableStream<Uint8Array>(
      {
        start: (controller) => {
          this.#controller = controller;
        },
        cancel: () => {
          this.#ended = true;
        },
      },
      { highWaterMark: 0 },
    );
    this.writable = hookWrites(
      new WritableStream<Uint8Array>({
        close: () => this.end(),
        abort: () => this.end(),
      }),
      (frame) => {
        this.written.push(frame.slice());
        for (const answer of reply(frame, this)) {
          this.deliver(answer);
        }
      },
    );
  }

  /** Queues [frame] for the mux. */
  deliver(frame: Uint8Array): void {
    if (!this.#ended) {
      this.#controller.enqueue(frame);
    }
  }

  /** Ends what the mux reads: the peer closed. */
  end(): void {
    if (!this.#ended) {
      this.#ended = true;
      this.#controller.close();
    }
  }
}

// Expectations -----------------------------------------------------------------

/** Dart's `hasCode`: [status] is a status whose known code is [code]. */
export function expectCode(status: Status, code: StatusCode): void {
  expect(status.known, `${status}`).toBe(code);
}

/** [actual] equals [expected]: the same code and the same bytes after it. */
export function expectStatus(actual: Status, expected: Status): void {
  expect(`${actual}`).toBe(`${expected}`);
  expect(actual.equals(expected), `${actual} equals ${expected}`).toBe(true);
}

/**
 * [actual] holds the bytes of [expected]. For large payloads, where a deep
 * equality takes seconds; reports the first difference.
 */
export function expectBytes(actual: Uint8Array, expected: Uint8Array): void {
  if (bytesEqual(actual, expected)) {
    return;
  }
  let at = 0;
  while (at < actual.length && at < expected.length && actual[at] === expected[at]) {
    at++;
  }
  expect.fail(
    `${actual.length} bytes, ${expected.length} expected; first difference at ${at}: ` +
      `${actual[at]} for ${expected[at]}`,
  );
}

/** [length] bytes of a pattern varied by [seed]: `(i * step + seed) & 0xFF`. */
export function pattern(length: number, step = 31, seed = 0): Uint8Array {
  const out = new Uint8Array(length);
  for (let i = 0; i < length; i++) {
    out[i] = (i * step + seed) & 0xff;
  }
  return out;
}

/** Dart's `throwsStatus`: [fn] throws a {@link SwitchboardError} with [code]. */
export function expectThrowsStatus(fn: () => unknown, code: StatusCode): void {
  let error: unknown;
  try {
    fn();
  } catch (e) {
    error = e;
  }
  expect(error).toBeInstanceOf(SwitchboardError);
  expect((error as SwitchboardError).code, `${error}`).toBe(code);
}

/** [promise] rejects with a {@link SwitchboardError} with [code]. */
export async function expectRejectsStatus(
  promise: Promise<unknown>,
  code: StatusCode,
): Promise<void> {
  const error = await rejection(promise);
  expect(error).toBeInstanceOf(SwitchboardError);
  expect((error as SwitchboardError).code, `${error}`).toBe(code);
}

/** The error [promise] rejects with. @throws {Error} if it resolves. */
export async function rejection(promise: Promise<unknown>): Promise<unknown> {
  try {
    await promise;
  } catch (error) {
    return error;
  }
  throw new Error('expected a rejection');
}

/** What [promise] settles with, the error for a rejection. */
export function outcome<T>(promise: Promise<T>): Promise<T | unknown> {
  return promise.then(
    (value) => value,
    (error: unknown) => error,
  );
}

/** Whether [promise] has settled, after the microtasks that are due ran. */
export function tracked<T>(promise: Promise<T>): {
  readonly settled: boolean;
  promise: Promise<T>;
} {
  const state = { settled: false, promise };
  promise.then(
    () => {
      state.settled = true;
    },
    () => {
      state.settled = true;
    },
  );
  return state;
}

/**
 * Elapses [clock] [step] ms at a time until [promise] settles; resolves
 * with the time elapsed. The port of a real-time wait for something a
 * timer causes. @throws {Error} after [max] ms.
 */
export async function elapseUntil(
  clock: SteppingClock,
  promise: Promise<unknown>,
  step = 1,
  max = 60_000,
): Promise<number> {
  const state = tracked(promise);
  let elapsed = 0;
  await settle();
  while (!state.settled) {
    if (elapsed >= max) {
      throw new Error(`still waiting after ${max} ms`);
    }
    await clock.elapse(step);
    elapsed += step;
  }
  return elapsed;
}

// Streams as Dart has them -----------------------------------------------------

/**
 * The peer-opened channels of [connection], taken one by one: Dart's
 * `StreamQueue(connection.incoming)`. {@link cancel} stops the iteration,
 * which refuses further OPENs.
 */
export class Incoming {
  readonly #iterator: AsyncIterator<MuxChannel>;

  constructor(connection: MuxConnection) {
    this.#iterator = connection.incoming[Symbol.asyncIterator]();
  }

  /** The next channel. @throws {Error} once the connection ended. */
  async next(): Promise<MuxChannel> {
    const result = await this.#iterator.next();
    if (result.done === true) {
      throw new Error('no more incoming channels');
    }
    return result.value;
  }

  /** Whether another channel comes before the end, taking it. */
  async hasNext(): Promise<boolean> {
    return (await this.#iterator.next()).done !== true;
  }

  /** Stops taking channels. */
  async cancel(): Promise<void> {
    await this.#iterator.return?.();
  }
}

/** Dart's `connection.incoming.first`: takes one channel and stops taking more. */
export async function firstIncoming(connection: MuxConnection): Promise<MuxChannel> {
  const incoming = new Incoming(connection);
  const channel = await incoming.next();
  await incoming.cancel();
  return channel;
}

/** Dart's `connection.incoming.listen(list.add)`: takes every channel into [into]. */
export function takeIncoming(connection: MuxConnection, into: MuxChannel[] = []): MuxChannel[] {
  void (async () => {
    for await (const channel of connection.incoming) {
      into.push(channel);
    }
  })();
  return into;
}

/** The subframes of a channel, read one by one: Dart's `StreamQueue(channel.stream)`. */
export class Subframes {
  readonly #reader: ReadableStreamDefaultReader<Uint8Array>;

  constructor(channel: MuxChannel) {
    this.#reader = channel.readable.getReader();
  }

  /** The next subframe. @throws {Error} once the channel ended. */
  async next(): Promise<Uint8Array> {
    const result = await this.#reader.read();
    if (result.done) {
      throw new Error('the channel ended');
    }
    return result.value;
  }

  /** The next subframe, as hex. */
  async nextHex(): Promise<string> {
    return hexString(await this.next());
  }

  /** Whether another subframe comes before the end, taking it. */
  async hasNext(): Promise<boolean> {
    return !(await this.#reader.read()).done;
  }

  /** The next [count] subframes. */
  async take(count: number): Promise<Uint8Array[]> {
    const out: Uint8Array[] = [];
    for (let i = 0; i < count; i++) {
      out.push(await this.next());
    }
    return out;
  }

  /** Every subframe until the end. */
  async rest(): Promise<Uint8Array[]> {
    const out: Uint8Array[] = [];
    for (;;) {
      const result = await this.#reader.read();
      if (result.done) {
        return out;
      }
      out.push(result.value);
    }
  }

  /** Stops reading: what arrives from then on is dropped (Dart's `cancel`). */
  cancel(): Promise<void> {
    return this.#reader.cancel().catch(() => {});
  }
}

/** A stream of [chunks]: Dart's `Stream.fromIterable`, for `pipeTo` (`sink.addStream`). */
export function streamOf(chunks: readonly Uint8Array[]): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    start: (controller) => {
      for (const chunk of chunks) {
        controller.enqueue(chunk);
      }
      controller.close();
    },
  });
}

/** Dart's `channel.stream.toList()`: every subframe until the channel ends. */
export function collect(channel: MuxChannel): Promise<Uint8Array[]> {
  return new Subframes(channel).rest();
}

/** Dart's `channel.stream.first`: one subframe, then the reading stops. */
export async function firstSubframe(channel: MuxChannel): Promise<Uint8Array> {
  const subframes = new Subframes(channel);
  const first = await subframes.next();
  await subframes.cancel();
  return first;
}

/**
 * Dart's `channel.stream.listen(onData)`: reads in a loop and hands each
 * subframe to [onData]. A paused subscription asks for nothing, so it
 * takes nothing and returns no credit: pausing it right after listening
 * (in the same turn) takes nothing at all. A read already waiting when it
 * is paused still takes the next subframe; that one is handed over on
 * resume.
 */
export class Subscription {
  readonly #reader: ReadableStreamDefaultReader<Uint8Array>;
  readonly #onData: (subframe: Uint8Array) => void;
  #paused = false;
  #reading = false;
  #cancelled = false;
  readonly #held: Uint8Array[] = [];
  readonly #done = new Deferred<void>();

  constructor(channel: MuxChannel, onData: (subframe: Uint8Array) => void = () => {}) {
    this.#reader = channel.readable.getReader();
    this.#onData = onData;
    queueMicrotask(() => this.#run());
  }

  /** Resolves when the channel's subframes ended or the subscription was cancelled. */
  get done(): Promise<void> {
    return this.#done.promise;
  }

  get isPaused(): boolean {
    return this.#paused;
  }

  pause(): this {
    this.#paused = true;
    return this;
  }

  resume(): this {
    this.#paused = false;
    for (;;) {
      const subframe = this.#held.shift();
      if (subframe === undefined) {
        break;
      }
      this.#onData(subframe);
    }
    queueMicrotask(() => this.#run());
    return this;
  }

  /** Stops reading: what arrives from then on is dropped. */
  async cancel(): Promise<void> {
    this.#cancelled = true;
    await this.#reader.cancel().catch(() => {});
    this.#done.resolve();
  }

  async #run(): Promise<void> {
    if (this.#reading) {
      return;
    }
    this.#reading = true;
    try {
      while (!this.#paused && !this.#cancelled) {
        let result: ReadableStreamReadResult<Uint8Array>;
        try {
          result = await this.#reader.read();
        } catch {
          this.#done.resolve();
          return;
        }
        if (result.done) {
          this.#done.resolve();
          return;
        }
        if (this.#paused) {
          this.#held.push(result.value);
        } else {
          this.#onData(result.value);
        }
      }
    } finally {
      this.#reading = false;
    }
  }
}

// Byte pipes for the stream transport -------------------------------------------

/**
 * Bytes fed to a stream transport by the test: Dart's `toMux`
 * `StreamController`. {@link isPaused} tells whether the transport has
 * stopped asking for more while chunks wait.
 */
export class ByteFeed implements ByteInput {
  readonly #chunks: Uint8Array[] = [];
  #ended = false;
  #waiter: Deferred<void> | undefined;
  #reading = false;

  /** Queues [bytes]. */
  add(bytes: Uint8Array): void {
    this.#chunks.push(bytes);
    this.#wake();
  }

  /** Ends the input: the peer closed. */
  end(): void {
    this.#ended = true;
    this.#wake();
  }

  /** Whether chunks wait and nobody is reading them. */
  get isPaused(): boolean {
    return this.#chunks.length > 0 && !this.#reading;
  }

  /** Chunks not read yet. */
  get unread(): number {
    return this.#chunks.length;
  }

  async read(): Promise<Uint8Array | undefined> {
    this.#reading = true;
    try {
      for (;;) {
        const chunk = this.#chunks.shift();
        if (chunk !== undefined) {
          return chunk;
        }
        if (this.#ended) {
          return undefined;
        }
        this.#waiter ??= new Deferred<void>();
        await this.#waiter.promise;
      }
    } finally {
      this.#reading = false;
    }
  }

  cancel(): void {
    this.#ended = true;
    this.#chunks.length = 0;
    this.#wake();
  }

  #wake(): void {
    const waiter = this.#waiter;
    this.#waiter = undefined;
    waiter?.resolve();
  }
}

/**
 * An output that takes what the transport writes: at once (the default),
 * one chunk each time {@link take} is called (Dart's `SlowSink`, a socket
 * whose peer reads slowly), or one chunk each [rate] ms of [clock]
 * (Dart's `RateSink`). What it took goes to {@link chunks} and to [target],
 * which it ends when it closes: a byte pipe to another stream transport.
 */
export class ByteDrain implements ByteOutput {
  readonly chunks: Uint8Array[] = [];
  accepted = 0;
  #credit: number;
  readonly #mode: 'open' | 'slow' | 'rate';
  readonly #rate: number;
  readonly #clock: SteppingClock | undefined;
  readonly #target: ByteFeed | undefined;
  #ready: Deferred<void> | undefined;
  #failure: ((error: unknown) => void) | undefined;
  #closed = false;
  #aborted = false;

  constructor(
    options: {
      readonly slow?: boolean;
      readonly rate?: number;
      readonly clock?: SteppingClock;
      readonly target?: ByteFeed;
    } = {},
  ) {
    this.#mode = options.rate !== undefined ? 'rate' : options.slow === true ? 'slow' : 'open';
    this.#rate = options.rate ?? 0;
    this.#clock = options.clock;
    this.#target = options.target;
    this.#credit = this.#mode === 'open' ? Number.POSITIVE_INFINITY : this.#mode === 'rate' ? 1 : 0;
  }

  /** Whether the transport closed the output. */
  get isClosed(): boolean {
    return this.#closed;
  }

  /** Whether the transport aborted the output. */
  get isAborted(): boolean {
    return this.#aborted;
  }

  /** Accepts one more chunk (a slow output). */
  take(): void {
    this.#credit++;
    this.#signal();
  }

  get canWrite(): boolean {
    return !this.#closed && this.#credit > 0;
  }

  ready(): Promise<void> {
    if (this.canWrite || this.#closed) {
      return Promise.resolve();
    }
    this.#ready ??= new Deferred<void>();
    return this.#ready.promise;
  }

  write(bytes: Uint8Array): void {
    if (this.#closed) {
      return;
    }
    this.#credit--;
    this.chunks.push(bytes);
    this.accepted += bytes.length;
    this.#target?.add(bytes);
    if (this.#mode === 'rate') {
      this.#clock?.setTimeout(() => this.take(), this.#rate);
    }
  }

  close(): Promise<void> {
    this.#closed = true;
    this.#target?.end();
    this.#signal();
    return Promise.resolve();
  }

  abort(): void {
    this.#aborted = true;
    this.#closed = true;
    this.#target?.end();
    this.#signal();
  }

  onFailure(callback: (error: unknown) => void): void {
    this.#failure = callback;
  }

  /** Fails the output: nothing written is delivered any more. */
  fail(error: unknown): void {
    this.#closed = true;
    this.#failure?.(error);
    this.#signal();
  }

  #signal(): void {
    const ready = this.#ready;
    this.#ready = undefined;
    ready?.resolve();
  }
}

/** The mux frames in [chunks], the bytes a stream transport wrote (preamble first). */
export function framesIn(chunks: readonly Uint8Array[]): Uint8Array[] {
  const bytes = new Uint8Array(chunks.reduce((n, c) => n + c.length, 0));
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.length;
  }
  const frames: Uint8Array[] = [];
  let at = 8;
  while (at + 4 <= bytes.length) {
    const length =
      (bytes[at] as number) +
      (bytes[at + 1] as number) * 0x100 +
      (bytes[at + 2] as number) * 0x10000 +
      (bytes[at + 3] as number) * 0x1000000;
    if (at + 4 + length > bytes.length) {
      break;
    }
    frames.push(bytes.slice(at + 4, at + 4 + length));
    at += 4 + length;
  }
  return frames;
}

/** A memory pair whose transports are typed as such, for tests that hold both. */
export function memoryPair(
  clock: SteppingClock,
  options: MemoryTransportOptions = {},
): [MemoryTransport, MemoryTransport] {
  return memoryTransportPair({ clock, ...options });
}
