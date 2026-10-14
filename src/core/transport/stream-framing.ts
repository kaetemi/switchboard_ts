/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * The wire format of the stream binding, without any I/O: the 8-byte
 * preamble and the frames prefixed with a `u32` little-endian length. Ports
 * `stream_framing.dart`. See the wiki page "Polyverse Switchboard
 * Transport", section "Stream binding".
 *
 * {@link StreamFraming} and {@link StreamFrameDecoder} are the codec;
 * {@link StreamFrameEncoderStream} and {@link StreamFrameDecoderStream} are
 * the same as Web Streams transforms for code that runs its own byte duplex;
 * `streamTransport()` in `stream-transport.ts` is the whole transport.
 */

import { readU32, writeU32 } from '../bytes.ts';
import { ProtocolError, StatusCode, SwitchboardError } from '../status.ts';
import { Deferred } from '../util/deferred.ts';
import { defaultMaxFrameSize } from './contract.ts';

const magic = Object.freeze([0x53, 0x57, 0x42, 0x44] as const);

/** Constants and encoders of the stream binding. */
export const StreamFraming = Object.freeze({
  /** Default upper limit on an incoming frame body: 1 MiB. */
  defaultMaxFrameSize,
  /** Protocol version sent in the preamble and the only one accepted. */
  version: 1,
  /** Length of the preamble in bytes. */
  preambleLength: 8,
  /** Length of the frame length prefix in bytes. */
  lengthPrefixSize: 4,
  /** The preamble magic, `SWBD`. */
  magic,

  /** Encodes a preamble: the magic, `u16` [version] (default 1), `u16` [flags] (default 0). */
  encodePreamble(options: { readonly version?: number; readonly flags?: number } = {}): Uint8Array {
    const version = options.version ?? 1;
    const flags = options.flags ?? 0;
    return Uint8Array.of(
      ...magic,
      version & 0xff,
      (version >> 8) & 0xff,
      flags & 0xff,
      (flags >> 8) & 0xff,
    );
  },

  /**
   * Encodes one frame: its `u32` length followed by [body], copied.
   *
   * @throws {RangeError} if [body] is empty, since a length of 0 is a
   *   protocol error, or longer than a `u32` can say.
   */
  encodeFrame(body: Uint8Array): Uint8Array {
    const length = body.length;
    if (length === 0) {
      throw new RangeError('stream binding frames are never empty');
    }
    if (length > 0xffffffff) {
      throw new RangeError(`frame of ${length} bytes does not fit a u32 length`);
    }
    const out = new Uint8Array(4 + length);
    writeU32(out, 0, length);
    out.set(body, 4);
    return out;
  },
});

/** Options of {@link StreamFrameDecoder} and {@link StreamFrameDecoderStream}. */
export interface StreamFrameDecoderOptions {
  /** Largest accepted frame body; 0 means no limit. Default 1 MiB. */
  readonly maxFrameSize?: number;
}

/** A drained buffer larger than this is released rather than kept. */
const keepLimit = 64 * 1024;

/**
 * Incremental parser for the receiving side of the stream binding.
 *
 * Feed it chunks of any size with {@link add}; it emits complete frame
 * bodies in order. Frames may be split across chunks at any byte and a
 * chunk may hold any number of frames.
 */
export class StreamFrameDecoder {
  /** Largest accepted frame body; 0 means no limit. */
  readonly maxFrameSize: number;
  #buf = new Uint8Array(0);
  #len = 0;
  #preambleReceived = false;

  /**
   * @throws {RangeError} for a negative or fractional [maxFrameSize].
   */
  constructor(options: StreamFrameDecoderOptions = {}) {
    const max = options.maxFrameSize ?? defaultMaxFrameSize;
    if (!Number.isInteger(max) || max < 0) {
      throw new RangeError(`maxFrameSize must be a non-negative integer, got ${max}`);
    }
    this.maxFrameSize = max;
  }

  /** Whether the peer preamble has been read and accepted. */
  get preambleReceived(): boolean {
    return this.#preambleReceived;
  }

  /** Number of bytes held while waiting for the rest of a frame. */
  get bufferedBytes(): number {
    return this.#len;
  }

  /**
   * Parses [chunk], calling [onFrame] with a fresh copy of every frame body
   * completed by it.
   *
   * Frames completed before offending bytes have already been passed to
   * [onFrame] when it throws. After a throw the decoder must not be used
   * again.
   *
   * @throws {ProtocolError} on a wrong magic or a zero length.
   * @throws {SwitchboardError} `UNSUPPORTED` on a preamble version other
   *   than {@link StreamFraming.version}, `FRAME_TOO_LARGE` on a length
   *   above {@link maxFrameSize}, decided from the length alone.
   */
  add(chunk: Uint8Array, onFrame: (frame: Uint8Array) => void): void {
    if (chunk.length === 0) {
      return;
    }
    let data: Uint8Array;
    let end: number;
    if (this.#len === 0) {
      data = chunk;
      end = chunk.length;
    } else {
      this.#ensure(this.#len + chunk.length);
      this.#buf.set(chunk, this.#len);
      this.#len += chunk.length;
      data = this.#buf;
      end = this.#len;
    }
    let pos = 0;
    try {
      for (;;) {
        if (!this.#preambleReceived) {
          if (end - pos < StreamFraming.preambleLength) {
            break;
          }
          checkPreamble(data, pos);
          pos += StreamFraming.preambleLength;
          this.#preambleReceived = true;
          continue;
        }
        if (end - pos < StreamFraming.lengthPrefixSize) {
          break;
        }
        const length = readU32(data, pos);
        if (length === 0) {
          throw new ProtocolError('zero length frame');
        }
        if (this.maxFrameSize > 0 && length > this.maxFrameSize) {
          throw SwitchboardError.of(
            StatusCode.frameTooLarge,
            `frame of ${length} bytes exceeds limit of ${this.maxFrameSize}`,
          );
        }
        const start = pos + StreamFraming.lengthPrefixSize;
        if (end - start < length) {
          break;
        }
        pos = start + length;
        onFrame(data.slice(start, pos));
      }
    } finally {
      this.#keep(data, pos, end);
    }
  }

  /** Keeps `data[pos, end)` as the buffered remainder. */
  #keep(data: Uint8Array, pos: number, end: number): void {
    const rest = end - pos;
    if (data === this.#buf) {
      if (pos > 0 && rest > 0) {
        this.#buf.copyWithin(0, pos, end);
      }
      this.#len = rest;
    } else {
      this.#len = 0;
      if (rest > 0) {
        this.#ensure(rest);
        this.#buf.set(data.subarray(pos, end), 0);
        this.#len = rest;
      }
    }
    // Do not hold on to a large buffer once it is drained.
    if (this.#len === 0 && this.#buf.length > keepLimit) {
      this.#buf = new Uint8Array(0);
    }
  }

  #ensure(capacity: number): void {
    if (capacity <= this.#buf.length) {
      return;
    }
    let size = this.#buf.length === 0 ? 256 : this.#buf.length;
    while (size < capacity) {
      size *= 2;
    }
    const grown = new Uint8Array(size);
    grown.set(this.#buf.subarray(0, this.#len));
    this.#buf = grown;
  }
}

function checkPreamble(data: Uint8Array, pos: number): void {
  for (let i = 0; i < magic.length; i++) {
    if (data[pos + i] !== magic[i]) {
      throw new ProtocolError('wrong preamble magic');
    }
  }
  const version = (data[pos + 4] as number) | ((data[pos + 5] as number) << 8);
  if (version !== StreamFraming.version) {
    throw SwitchboardError.of(
      StatusCode.unsupported,
      `unsupported stream binding version ${version}`,
    );
  }
  // The flags at pos + 6 are ignored, as the specification requires.
}

/**
 * Frames to stream binding bytes: the preamble first, then every chunk
 * written as one length-prefixed frame. The writing half of the binding for
 * a byte duplex the caller runs itself.
 *
 * An empty chunk is a `RangeError` and errors the stream.
 */
export class StreamFrameEncoderStream extends TransformStream<Uint8Array, Uint8Array> {
  constructor() {
    super({
      start(controller) {
        controller.enqueue(StreamFraming.encodePreamble());
      },
      transform(frame, controller) {
        controller.enqueue(StreamFraming.encodeFrame(frame));
      },
    });
  }
}

/**
 * Stream binding bytes to frames: checks the peer preamble, then yields one
 * chunk per frame body. The reading half of the binding for a byte duplex
 * the caller runs itself; use it with `pipeThrough`.
 *
 * Bytes are decoded as the readable side is read, never ahead of it. A
 * violation (see {@link StreamFrameDecoder.add}) errors the readable side
 * once the frames before it have been read, and errors the writable side at
 * once. Input that ends inside a frame ends the readable side normally, as
 * the stream transport does.
 */
export class StreamFrameDecoderStream implements ReadableWritablePair<Uint8Array, Uint8Array> {
  /** Frame bodies, one chunk each. */
  readonly readable: ReadableStream<Uint8Array>;
  /** Stream binding bytes, in chunks of any size. */
  readonly writable: WritableStream<Uint8Array>;

  constructor(options: StreamFrameDecoderOptions = {}) {
    const decoder = new StreamFrameDecoder(options);
    const frames: Uint8Array[] = [];
    let failure: { readonly error: unknown } | undefined;
    let ended = false;
    // The writable side waits while decoded frames are not taken; the
    // readable side waits for input.
    let taken: Deferred<void> | undefined;
    let input: Deferred<void> | undefined;
    const wakeInput = (): void => {
      input?.resolve();
      input = undefined;
    };
    const wakeTaken = (): void => {
      taken?.resolve();
      taken = undefined;
    };
    this.readable = new ReadableStream<Uint8Array>(
      {
        async pull(controller) {
          for (;;) {
            const frame = frames.shift();
            if (frame !== undefined) {
              controller.enqueue(frame);
              if (frames.length === 0) {
                wakeTaken();
              }
              return;
            }
            if (failure !== undefined) {
              controller.error(failure.error);
              return;
            }
            if (ended) {
              controller.close();
              return;
            }
            wakeTaken();
            input ??= new Deferred();
            await input.promise;
          }
        },
        cancel() {
          ended = true;
          frames.length = 0;
          wakeTaken();
        },
      },
      { highWaterMark: 0 },
    );
    this.writable = new WritableStream<Uint8Array>({
      async write(chunk) {
        if (ended) {
          return;
        }
        while (frames.length > 0) {
          taken ??= new Deferred();
          await taken.promise;
          if (ended) {
            return;
          }
        }
        try {
          decoder.add(chunk, (frame) => frames.push(frame));
        } catch (error) {
          failure = { error };
          ended = true;
          wakeInput();
          throw error;
        }
        wakeInput();
      },
      close() {
        ended = true;
        wakeInput();
      },
      abort(reason) {
        failure ??= { error: reason };
        ended = true;
        wakeInput();
      },
    });
  }
}
