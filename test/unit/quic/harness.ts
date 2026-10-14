// Helpers of the binding's tests, a port of `switchboard_quic`'s
// `test/harness.dart`: connection pairs over the fake provider, and a raw
// peer that writes and reads records by hand.
import { expect, onTestFinished } from 'vitest';
import { bytesEqual } from '../../../src/core/bytes.ts';
import type { MuxChannel, MuxConnection, MuxOptions } from '../../../src/core/mux/contract.ts';
import { MuxControlMessage } from '../../../src/core/mux/mux-frame.ts';
import type { QuicSessionEnd, QuicStream } from '../../../src/core/quic/contract.ts';
import { QuicStreamReset } from '../../../src/core/quic/contract.ts';
import { QuicHello, quicHelloType } from '../../../src/core/quic/hello.ts';
import { QuicBindingConnection } from '../../../src/core/quic/quic-binding-connection.ts';
import type { QuicBindingOptions } from '../../../src/core/quic/quic-options.ts';
import { RecordReader } from '../../../src/core/quic/record-reader.ts';
import {
  decodeOpenBody,
  encodeDataRecord,
  encodeOpenRecord,
  type QuicRecord,
  QuicRecordCommand,
} from '../../../src/core/quic/records.ts';
import { type Status, type StatusCode, SwitchboardError } from '../../../src/core/status.ts';
import { AsyncQueue } from '../../../src/core/util/async-queue.ts';
import {
  type FakeEndConfig,
  type FakeQuicNetwork,
  FakeQuicSession,
} from '../../../src/testing/fake-quic-provider.ts';

/** How long a step of a test may take. */
export const limit = 10_000;

/** Options without keep-alive and with short waits. */
export const fast: Partial<MuxOptions> = Object.freeze({
  goAwayGrace: 200,
  keepAliveInterval: undefined,
  closeConfirmTimeout: 5000,
});

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export const bytes = (value: string): Uint8Array => encoder.encode(value);
export const text = (data: Uint8Array): string => decoder.decode(data);

/** [length] bytes of a pattern a corrupted transfer would not keep. */
export function pattern(length: number, seed = 0): Uint8Array {
  const out = new Uint8Array(length);
  for (let i = 0; i < length; i++) {
    out[i] = (i * 7 + Math.floor(i / 251) + seed) & 0xff;
  }
  return out;
}

/** Resolves after [ms] milliseconds. */
export function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Waits until [condition] holds, polling every 2 ms; fails after [within]. */
export async function until(condition: () => boolean, within = limit): Promise<void> {
  const deadline = Date.now() + within;
  while (!condition()) {
    if (Date.now() > deadline) {
      throw new Error(`condition not met within ${within} ms`);
    }
    await delay(2);
  }
}

/** Lets things settle for [ms] milliseconds. */
export const settle = (ms = 20): Promise<void> => delay(ms);

/** [promise], failing the test when it takes longer than [ms]. */
export function within<T>(promise: Promise<T>, ms = limit): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`not settled within ${ms} ms`)), ms);
    promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (error: unknown) => {
        clearTimeout(timer);
        reject(error);
      },
    );
  });
}

/** The error [promise] rejects with; fails when it resolves. */
export async function rejection(promise: Promise<unknown>): Promise<unknown> {
  try {
    await within(promise);
  } catch (error) {
    return error;
  }
  throw new Error('expected a rejection');
}

/** Expects [promise] to reject with a SwitchboardError of [code]. */
export async function expectCode(promise: Promise<unknown>, code: StatusCode): Promise<void> {
  const error = await rejection(promise);
  expect(error).toBeInstanceOf(SwitchboardError);
  expect((error as SwitchboardError).code).toBe(code);
}

/** Expects [fn] to throw a SwitchboardError of [code] synchronously. */
export function expectThrowsCode(fn: () => unknown, code: StatusCode): void {
  let thrown: unknown;
  try {
    fn();
  } catch (error) {
    thrown = error;
  }
  expect(thrown).toBeInstanceOf(SwitchboardError);
  expect((thrown as SwitchboardError).code).toBe(code);
}

/**
 * Expects [actual] to be the chunks [expected], compared byte by byte: a
 * deep equality of large arrays is slow.
 */
export function expectChunks(actual: readonly Uint8Array[], expected: readonly Uint8Array[]): void {
  expect(actual.map((c) => c.length)).toEqual(expected.map((c) => c.length));
  for (let i = 0; i < expected.length; i++) {
    expect(bytesEqual(actual[i] as Uint8Array, expected[i] as Uint8Array), `chunk ${i}`).toBe(true);
  }
}

/** Expects [status] to be the well-known [code]. */
export function expectStatus(status: Status, code: StatusCode): void {
  expect(status.code, `${status}`).toBe(code);
}

/** Closes [connection] when the test ends. */
export function closeAfter(connection: QuicBindingConnection): void {
  onTestFinished(() => connection.close());
}

const incomingIterators = new WeakMap<MuxConnection, AsyncIterator<MuxChannel>>();

/** The one iterator of [connection]'s incoming channels, shared by the helpers below. */
export function incomingOf(connection: MuxConnection): AsyncIterator<MuxChannel> {
  let iterator = incomingIterators.get(connection);
  if (iterator === undefined) {
    iterator = connection.incoming[Symbol.asyncIterator]();
    incomingIterators.set(connection, iterator);
  }
  return iterator;
}

/** The channel [connection] delivers next. */
export async function nextChannel(connection: MuxConnection): Promise<MuxChannel> {
  const next = await within(incomingOf(connection).next());
  if (next.done === true) {
    throw new Error('no more channels');
  }
  return next.value;
}

/** Every channel [connection] delivers, added to [into]; [each] runs on each. */
export function collect(
  connection: MuxConnection,
  into: MuxChannel[] = [],
  each?: (channel: MuxChannel) => void,
): MuxChannel[] {
  void (async () => {
    for (;;) {
      const next = await incomingOf(connection).next();
      if (next.done === true) {
        return;
      }
      into.push(next.value);
      each?.(next.value);
    }
  })().catch(() => {});
  return into;
}

/** Reads [channel] to its end in the background, adding the chunks to [into]: Dart's `listen`. */
export function listen(channel: MuxChannel, into: Uint8Array[] = []): Uint8Array[] {
  void (async () => {
    for await (const chunk of channel.readable) {
      into.push(chunk);
    }
  })().catch(() => {});
  return into;
}

/** Every chunk of [channel] until its end: Dart's `toList`. */
export async function readAll(channel: MuxChannel): Promise<Uint8Array[]> {
  const out: Uint8Array[] = [];
  await within(
    (async () => {
      for await (const chunk of channel.readable) {
        out.push(chunk);
      }
    })(),
  );
  return out;
}

/** The first chunk of [channel]: Dart's `first`. The rest stays unread. */
export async function firstChunk(channel: MuxChannel): Promise<Uint8Array> {
  const reader = channel.readable.getReader();
  try {
    const result = await within(reader.read());
    if (result.done) {
      throw new Error('no chunk');
    }
    return result.value;
  } finally {
    reader.releaseLock();
  }
}

/** What {@link connectionPair} takes. */
export interface PairOptions {
  initiator?: Partial<MuxOptions>;
  acceptor?: Partial<MuxOptions>;
  initiatorQuic?: Partial<QuicBindingOptions>;
  acceptorQuic?: Partial<QuicBindingOptions>;
  initiatorEnd?: Partial<FakeEndConfig>;
  acceptorEnd?: Partial<FakeEndConfig>;
  network?: FakeQuicNetwork;
  initiatorIdentity?: string;
  acceptorIdentity?: string;
}

/** Two connections of the binding over a fake session pair, closed after the test. */
export async function connectionPair(
  init: PairOptions = {},
): Promise<[QuicBindingConnection, QuicBindingConnection]> {
  const [a, b] = FakeQuicSession.pair({
    ...(init.network === undefined ? {} : { network: init.network }),
    initiator: init.initiatorEnd ?? {},
    acceptor: init.acceptorEnd ?? {},
  });
  const server = QuicBindingConnection.start(b, {
    options: init.acceptor ?? fast,
    quicOptions: init.acceptorQuic,
    localIdentity: init.acceptorIdentity,
  });
  const client = await QuicBindingConnection.start(a, {
    options: init.initiator ?? fast,
    quicOptions: init.initiatorQuic,
    localIdentity: init.initiatorIdentity,
  });
  const accepted = await server;
  closeAfter(client);
  closeAfter(accepted);
  return [client, accepted];
}

/** A peer that speaks the binding by hand, over the other end of a fake session. */
export class RawPeer {
  readonly session: FakeQuicSession;
  readonly #streams: AsyncIterator<QuicStream>;
  control: QuicStream | undefined;
  controlReader: RecordReader | undefined;

  constructor(session: FakeQuicSession) {
    this.session = session;
    // Taken for the session's life, so that streams queue in order.
    const queue = new AsyncQueue<QuicStream>();
    void (async () => {
      try {
        for await (const stream of session.incomingStreams) {
          queue.push(stream);
        }
      } catch {
        // The session ended.
      }
      queue.end();
    })();
    this.#streams = queue[Symbol.asyncIterator]();
  }

  /** The next stream the binding opened. */
  async nextStream(): Promise<QuicStream> {
    const next = await within(this.#streams.next());
    if (next.done === true) {
      throw new Error('no more streams');
    }
    return next.value;
  }

  /**
   * Initiator: opens the control stream with the OPEN record of channel 0
   * and [hello] (a default `HELLO` when undefined, none with [sendHello]
   * false).
   */
  async openControl(options: { hello?: QuicHello; sendHello?: boolean } = {}): Promise<QuicStream> {
    const stream = await this.session.openStream();
    const parts = [encodeOpenRecord(0, [])];
    if (options.sendHello ?? true) {
      parts.push(encodeDataRecord((options.hello ?? new QuicHello()).encodeMessage()));
    }
    await stream.write(concat(parts));
    this.control = stream;
    this.controlReader = new RecordReader(stream);
    return stream;
  }

  /**
   * Acceptor: takes the binding's control stream, checks its OPEN record
   * and `HELLO`, and answers with [hello] unless [sendHello] is false.
   */
  async acceptControl(
    options: { hello?: QuicHello; sendHello?: boolean } = {},
  ): Promise<QuicHello> {
    const stream = await this.nextStream();
    const reader = new RecordReader(stream);
    const open = await within(reader.next(1 << 20));
    expect(open?.command).toBe(QuicRecordCommand.open);
    expect(decodeOpenBody((open as QuicRecord).body).id).toBe(0);
    const first = await within(reader.next(1 << 20));
    expect(first?.command).toBe(QuicRecordCommand.data);
    const message = MuxControlMessage.decode((first as QuicRecord).body);
    expect(message.type).toBe(quicHelloType);
    if (options.sendHello ?? true) {
      await stream.write(encodeDataRecord((options.hello ?? new QuicHello()).encodeMessage()));
    }
    this.control = stream;
    this.controlReader = reader;
    return QuicHello.decode(message.payload);
  }

  /** Writes a control message on the control stream. */
  sendControl(message: MuxControlMessage): Promise<void> {
    return (this.control as QuicStream).write(encodeDataRecord(message.encode()));
  }

  /** The next control message the binding wrote. */
  async nextControl(): Promise<MuxControlMessage> {
    const record = await within((this.controlReader as RecordReader).next(1 << 20));
    expect(record?.command).toBe(QuicRecordCommand.data);
    return MuxControlMessage.decode((record as QuicRecord).body);
  }

  /** The next control message of [type], skipping others. */
  async nextControlOf(type: number): Promise<MuxControlMessage> {
    for (;;) {
      const message = await this.nextControl();
      if (message.type === type) {
        return message;
      }
    }
  }

  /** Opens a stream and writes [data] on it. */
  async openRaw(data: ArrayLike<number>): Promise<QuicStream> {
    const stream = await this.session.openStream();
    if (data.length > 0) {
      await stream.write(Uint8Array.from(data));
    }
    return stream;
  }

  /**
   * Everything the binding writes on [stream] until it ends: the records,
   * and how the direction ended (`'fin'`, or the reset code).
   */
  static async drain(stream: QuicStream): Promise<[QuicRecord[], number | 'fin' | 'reset']> {
    const reader = new RecordReader(stream);
    const records: QuicRecord[] = [];
    try {
      for (;;) {
        const record = await within(reader.next(1 << 30));
        if (record === undefined) {
          return [records, 'fin'];
        }
        records.push(record);
      }
    } catch (error) {
      if (error instanceof QuicStreamReset) {
        return [records, error.code ?? 'reset'];
      }
      throw error;
    }
  }

  /** How the session ended for this peer. */
  get end(): Promise<QuicSessionEnd> {
    return within(this.session.done);
  }
}

/** What {@link bindingAndRaw} takes. */
export interface RawOptions {
  bindingIsInitiator?: boolean;
  options?: Partial<MuxOptions>;
  quicOptions?: Partial<QuicBindingOptions>;
  bindingEnd?: Partial<FakeEndConfig>;
  rawEnd?: Partial<FakeEndConfig>;
  rawControl?: boolean;
  rawHello?: QuicHello;
  isWebTransport?: boolean;
}

/**
 * A binding connection against a raw peer: the binding is the initiator
 * when [bindingIsInitiator] is true (default false). With the binding as
 * acceptor, the raw peer opens the control stream first unless
 * [rawControl] is false.
 */
export async function bindingAndRaw(
  init: RawOptions = {},
): Promise<[QuicBindingConnection, RawPeer]> {
  const bindingIsInitiator = init.bindingIsInitiator ?? false;
  const bindingEnd = init.bindingEnd ?? {};
  const rawEnd = init.rawEnd ?? {};
  const [a, b] = FakeQuicSession.pair({
    initiator: bindingIsInitiator ? bindingEnd : rawEnd,
    acceptor: bindingIsInitiator ? rawEnd : bindingEnd,
  });
  const raw = new RawPeer(bindingIsInitiator ? b : a);
  const hello = init.rawHello === undefined ? {} : { hello: init.rawHello };
  const connection = await QuicBindingConnection.start(bindingIsInitiator ? a : b, {
    options: init.options ?? fast,
    quicOptions: init.quicOptions,
    isWebTransport: init.isWebTransport ?? false,
  });
  closeAfter(connection);
  if (bindingIsInitiator) {
    await raw.acceptControl(hello);
  } else if (init.rawControl ?? true) {
    await raw.openControl(hello);
  }
  return [connection, raw];
}

/** [parts] joined. */
export function concat(parts: readonly Uint8Array[]): Uint8Array {
  let length = 0;
  for (const part of parts) {
    length += part.length;
  }
  const out = new Uint8Array(length);
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.length;
  }
  return out;
}
