// Helpers for the transport tests: byte pipes the tests drive by hand, the
// port of the `RawPeer` and `streamPair` helpers of `transport_test.dart`.
import { hexBytes, hexString } from '../../../src/core/bytes.ts';
import {
  type StreamTransport,
  type StreamTransportOptions,
  streamTransport,
} from '../../../src/core/transport/stream-transport.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';

/** Lets every pending microtask and a few macrotasks run: Dart's `pumpEventQueue`. */
export async function pump(turns = 5): Promise<void> {
  for (let i = 0; i < turns; i++) {
    await new Promise<void>((resolve) => setImmediate(resolve));
  }
}

/** Reads [readable] to its end. */
export async function collect<T>(readable: ReadableStream<T>): Promise<T[]> {
  const out: T[] = [];
  for await (const chunk of readable) {
    out.push(chunk);
  }
  return out;
}

/** The error [promise] rejects with. */
export async function rejection(promise: Promise<unknown>): Promise<unknown> {
  try {
    await promise;
  } catch (error) {
    return error;
  }
  throw new Error('expected a rejection');
}

/** [length] bytes of a pattern, varied by [seed]. */
export function bytesOf(length: number, seed = 0): Uint8Array {
  const out = new Uint8Array(length);
  for (let i = 0; i < length; i++) {
    out[i] = (i * 31 + seed) & 0xff;
  }
  return out;
}

/** A small deterministic generator, so that "random" boundaries repeat. */
export function seeded(seed: number): (bound: number) => number {
  let state = seed >>> 0 || 1;
  return (bound) => {
    state ^= state << 13;
    state >>>= 0;
    state ^= state >>> 17;
    state ^= state << 5;
    state >>>= 0;
    return state % bound;
  };
}

/** A byte source the test feeds. */
export class ByteSource {
  readonly readable: ReadableStream<Uint8Array>;
  #controller!: ReadableStreamDefaultController<Uint8Array>;

  constructor() {
    this.readable = new ReadableStream<Uint8Array>(
      {
        start: (controller) => {
          this.#controller = controller;
        },
      },
      { highWaterMark: 0 },
    );
  }

  /** Chunks queued and not read yet. */
  get unread(): number {
    return Math.max(0, -(this.#controller.desiredSize ?? 0));
  }

  /** Queues [bytes] (hex or bytes); dropped once the reader has gone. */
  add(bytes: string | Uint8Array): void {
    try {
      this.#controller.enqueue(typeof bytes === 'string' ? hexBytes(bytes) : bytes);
    } catch {
      // Cancelled by the transport: as a socket, the bytes go nowhere.
    }
  }

  /** Ends the source: the peer closed. */
  end(): void {
    try {
      this.#controller.close();
    } catch {
      // Already cancelled.
    }
  }

  /** Fails the source: the connection broke. */
  fail(error: unknown): void {
    try {
      this.#controller.error(error);
    } catch {
      // Already cancelled.
    }
  }
}

/** A byte sink that records what it is given and can be held back. */
export class ByteSink {
  readonly writable: WritableStream<Uint8Array>;
  readonly bytes: number[] = [];
  readonly chunks: Uint8Array[] = [];
  #offset = 0;
  #closed = false;
  #aborted = false;
  #changed = new Deferred<void>();
  #gate: Deferred<void> | undefined;

  constructor(options: { readonly paused?: boolean } = {}) {
    if (options.paused === true) {
      this.#gate = new Deferred();
    }
    this.writable = new WritableStream<Uint8Array>({
      write: async (chunk) => {
        while (this.#gate !== undefined) {
          await this.#gate.promise;
        }
        this.chunks.push(chunk.slice());
        for (const b of chunk) {
          this.bytes.push(b);
        }
        this.#notify();
      },
      close: () => {
        this.#closed = true;
        this.#notify();
      },
      abort: () => {
        this.#aborted = true;
        this.#closed = true;
        this.#notify();
      },
    });
  }

  /** Whether the transport closed (or aborted) the sink. */
  get isClosed(): boolean {
    return this.#closed;
  }

  /** Whether the transport aborted the sink. */
  get isAborted(): boolean {
    return this.#aborted;
  }

  /** Holds every write until {@link resume}. */
  pause(): void {
    this.#gate ??= new Deferred();
  }

  /** Lets held writes through. */
  resume(): void {
    const gate = this.#gate;
    this.#gate = undefined;
    gate?.resolve();
  }

  /** The next [count] bytes as hex, once they have arrived. */
  async read(count: number): Promise<string> {
    while (this.bytes.length - this.#offset < count) {
      if (this.#closed) {
        throw new Error(`sink closed with ${this.bytes.length - this.#offset} of ${count} bytes`);
      }
      await this.#changed.promise;
    }
    const out = this.bytes.slice(this.#offset, this.#offset + count);
    this.#offset += count;
    return hexString(out);
  }

  /** Whether more bytes come before the sink closes: Dart's `StreamQueue.hasNext`. */
  async hasNext(): Promise<boolean> {
    for (;;) {
      if (this.bytes.length > this.#offset) {
        return true;
      }
      if (this.#closed) {
        return false;
      }
      await this.#changed.promise;
    }
  }

  #notify(): void {
    const changed = this.#changed;
    this.#changed = new Deferred();
    changed.resolve();
  }
}

/** A stream transport whose peer is driven with raw bytes. */
export class RawPeer {
  readonly toTransport = new ByteSource();
  readonly fromTransport: ByteSink;
  readonly transport: StreamTransport;

  constructor(options: StreamTransportOptions & { readonly paused?: boolean } = {}) {
    const { paused, ...transportOptions } = options;
    this.fromTransport = new ByteSink(paused === undefined ? {} : { paused });
    this.transport = streamTransport(
      { readable: this.toTransport.readable, writable: this.fromTransport.writable },
      transportOptions,
    );
  }

  /** Bytes written by the transport, as hex. */
  read(count: number): Promise<string> {
    return this.fromTransport.read(count);
  }
}

/** A transform that splits every chunk into pieces of [size] bytes. */
export function rechunk(size: number): TransformStream<Uint8Array, Uint8Array> {
  return new TransformStream<Uint8Array, Uint8Array>({
    transform(chunk, controller) {
      for (let i = 0; i < chunk.length; i += size) {
        controller.enqueue(chunk.slice(i, Math.min(i + size, chunk.length)));
      }
    },
  });
}

/** Two stream transports connected by byte pipes, optionally re-chunked. */
export function streamPair(
  options: StreamTransportOptions & { readonly chunkSize?: number } = {},
): [StreamTransport, StreamTransport] {
  const { chunkSize, ...transportOptions } = options;
  const aToB = new TransformStream<Uint8Array, Uint8Array>();
  const bToA = new TransformStream<Uint8Array, Uint8Array>();
  const input = (readable: ReadableStream<Uint8Array>): ReadableStream<Uint8Array> =>
    chunkSize === undefined ? readable : readable.pipeThrough(rechunk(chunkSize));
  const a = streamTransport(
    { readable: input(bToA.readable), writable: aToB.writable },
    transportOptions,
  );
  const b = streamTransport(
    { readable: input(aToB.readable), writable: bToA.writable },
    transportOptions,
  );
  return [a, b];
}
