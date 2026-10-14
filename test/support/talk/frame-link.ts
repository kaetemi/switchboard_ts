/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { hexBytes } from '../../../src/core/bytes.ts';
import { Status } from '../../../src/core/status.ts';
import type { TalkOptions } from '../../../src/core/talk/contract.ts';
import { TalkChannel, type TalkFrameChannel } from '../../../src/core/talk/talk-channel.ts';
import { TalkFrame } from '../../../src/core/talk/talk-frame.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';

/** One direction of a link: what is written at one end is read at the other. */
class Direction {
  readonly readable: ReadableStream<Uint8Array>;
  #controller!: ReadableStreamDefaultController<Uint8Array>;
  #ended = false;

  /** Sees every chunk written, before it is passed on; returns false to drop it. */
  tap: ((chunk: Uint8Array) => boolean | undefined) | undefined;

  constructor() {
    this.readable = new ReadableStream<Uint8Array>({
      start: (controller) => {
        this.#controller = controller;
      },
      cancel: () => {
        this.#ended = true;
      },
    });
  }

  get ended(): boolean {
    return this.#ended;
  }

  write(chunk: Uint8Array): void {
    if (this.tap?.(chunk) === false || this.#ended) {
      return;
    }
    this.#controller.enqueue(chunk);
  }

  end(): void {
    if (this.#ended) {
      return;
    }
    this.#ended = true;
    this.#controller.close();
  }

  fail(error: unknown): void {
    if (this.#ended) {
      return;
    }
    this.#ended = true;
    this.#controller.error(error);
  }
}

/** One end of an in-memory frame link: a {@link TalkFrameChannel}, plus a synchronous sink for tests. */
export class FrameEnd implements TalkFrameChannel {
  readonly readable: ReadableStream<Uint8Array>;
  readonly writable: WritableStream<Uint8Array>;
  readonly #out: Direction;

  constructor(incoming: Direction, outgoing: Direction) {
    this.readable = incoming.readable;
    this.#out = outgoing;
    this.writable = new WritableStream<Uint8Array>({
      write: (chunk) => outgoing.write(chunk),
      close: () => outgoing.end(),
      abort: () => outgoing.end(),
    });
  }

  /** Writes [chunk] at once, as a Dart sink's `add` does. */
  add(chunk: Uint8Array): void {
    this.#out.write(chunk);
  }

  /** Ends what this end sends: the other end's readable ends. */
  end(): void {
    this.#out.end();
  }

  /** Fails what this end sends: the other end's readable errors. */
  fail(error: unknown): void {
    this.#out.fail(error);
  }

  /** Whether what this end sends has ended. */
  get outEnded(): boolean {
    return this.#out.ended;
  }

  /** Sees every chunk this end writes, before it goes; returning false drops it. */
  set tap(tap: ((chunk: Uint8Array) => boolean | undefined) | undefined) {
    this.#out.tap = tap;
  }
}

/** Two joined frame ends: what one writes, the other reads. */
export function frameLink(): [FrameEnd, FrameEnd] {
  const ab = new Direction();
  const ba = new Direction();
  return [new FrameEnd(ba, ab), new FrameEnd(ab, ba)];
}

/**
 * A frame end that ends with a status, standing in for a mux channel that
 * is not flow controlled: closable with a status, with an end status.
 */
export class StatusFrameEnd implements TalkFrameChannel {
  readonly #inner: FrameEnd;
  readonly closedWith: Status[] = [];
  readonly #done = new Deferred<Status>();

  constructor(inner: FrameEnd) {
    this.#inner = inner;
  }

  get readable(): ReadableStream<Uint8Array> {
    return this.#inner.readable;
  }

  get writable(): WritableStream<Uint8Array> {
    return this.#inner.writable;
  }

  async close(status: Status = Status.ok): Promise<void> {
    this.closedWith.push(status);
    this.#done.resolve(status);
    this.#inner.end();
  }

  get done(): Promise<Status> {
    return this.#done.promise;
  }

  /** The peer closed the channel with [status]: what it sends ends. */
  endFromPeer(foreign: FrameEnd, status: Status): void {
    this.#done.resolve(status);
    foreign.end();
  }

  /** Resolves {@link done} alone, as the mux does once the close completes. */
  completeDone(status: Status): void {
    this.#done.resolve(status);
  }
}

/** Two Talk channels joined through a tap that records every frame. */
export class Pair {
  readonly a: TalkChannel;
  readonly b: TalkChannel;
  readonly aToBRaw: Uint8Array[] = [];
  readonly bToARaw: Uint8Array[] = [];
  readonly endA: FrameEnd;
  readonly endB: FrameEnd;

  constructor(options: { a?: Partial<TalkOptions>; b?: Partial<TalkOptions> } = {}) {
    const [endA, endB] = frameLink();
    this.endA = endA;
    this.endB = endB;
    endA.tap = (chunk) => {
      this.aToBRaw.push(chunk.slice());
      return true;
    };
    endB.tap = (chunk) => {
      this.bToARaw.push(chunk.slice());
      return true;
    };
    this.a = new TalkChannel(endA, options.a);
    this.b = new TalkChannel(endB, options.b);
  }

  get aToB(): TalkFrame[] {
    return this.aToBRaw.map((raw) => TalkFrame.decode(raw));
  }

  get bToA(): TalkFrame[] {
    return this.bToARaw.map((raw) => TalkFrame.decode(raw));
  }

  async close(): Promise<void> {
    await Promise.all([this.a.close(), this.b.close()]);
  }
}

/** A Talk channel whose peer is driven by hand at the frame level. */
export class RawPeer {
  readonly talk: TalkChannel;
  readonly raw: FrameEnd;
  /** With `status`: the channel under {@link talk}, closable with a status. */
  readonly status: StatusFrameEnd | undefined;
  readonly received: TalkFrame[] = [];
  ended = false;

  constructor(options: { options?: Partial<TalkOptions>; status?: boolean } = {}) {
    const [local, foreign] = frameLink();
    this.raw = foreign;
    this.status = options.status === true ? new StatusFrameEnd(local) : undefined;
    this.talk = new TalkChannel(this.status ?? local, options.options);
    void this.#read();
  }

  async #read(): Promise<void> {
    const reader = this.raw.readable.getReader();
    try {
      for (;;) {
        const { value, done } = await reader.read();
        if (done) {
          break;
        }
        this.received.push(TalkFrame.decode(value));
      }
    } catch {
      // The link failed.
    }
    this.ended = true;
  }

  send(frame: TalkFrame): void {
    this.raw.add(frame.encode());
  }

  sendHex(hex: string): void {
    this.raw.add(hexBytes(hex));
  }
}
