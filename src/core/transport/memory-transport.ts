/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * In-memory transport pair, for tests and for connecting two endpoints in
 * the same process. Ports `memory_transport.dart`.
 */

import { StatusCode, SwitchboardError } from '../status.ts';
import { type Clock, systemClock, type TimerHandle } from '../util/clock.ts';
import { Deferred } from '../util/deferred.ts';
import { type Logger, noopLogger } from '../util/logger.ts';
import type { FrameTransport } from './contract.ts';
import { Fifo } from './fifo.ts';

/** Options of {@link memoryTransportPair}. */
export interface MemoryTransportOptions {
  /**
   * Milliseconds every frame, and the close, takes to arrive, in order.
   * `undefined` (the default) hands frames over at once, to be read in a
   * later microtask; `0` delivers them on the next turn of the clock, as a
   * socket does, which lets the clock's timers run between frames.
   */
  readonly delay?: number;
  /**
   * Largest frame the receiving side accepts; a longer one errors that
   * side's readable with `FRAME_TOO_LARGE`, as the stream binding does.
   * 0 or `undefined` (the default) means no limit.
   */
  readonly maxFrameSize?: number;
  /** The {@link FrameTransport.remote} of the first and the second transport. */
  readonly remotes?: readonly [string | undefined, string | undefined];
  /** Source of the delivery timers. Default {@link systemClock}. */
  readonly clock?: Clock;
  /** Receives the pair's diagnostics, as `Switchboard.Transport`. */
  readonly logger?: Logger;
}

/** One side of a {@link memoryTransportPair}. */
export interface MemoryTransport extends FrameTransport {
  /** The pair's frame size limit; 0 when there is none. */
  readonly maxFrameSize: number;
}

/**
 * Two connected transports: frames written to one are read from the other.
 *
 * Frames are copied as the writable takes them, so the caller may reuse
 * its buffer once the write has resolved. Closing one side's writable
 * ends both readables: its own at once (after the frames already
 * delivered to it), the other after the frames in flight towards it.
 * Aborting a writable closes the pair at once, dropping frames in flight;
 * that writable's `closed` promise rejects with the reason.
 *
 * The other side's writable is errored when its readable ends, with a
 * `SwitchboardError` `CONNECTION_LOST`, so that its `closed` settles and
 * later writes are refused, as the stream transport's writable is when
 * the peer closes (Dart's sink completes `done` and drops later frames).
 * Frames written to it between the close and that moment, while the
 * frames in flight towards it are still on their way, are dropped. A
 * close or an abort of a writable settles it as asked.
 *
 * With a positive `maxFrameSize`, a frame longer than that errors the
 * receiving side's readable with `FRAME_TOO_LARGE` after the frames before
 * it, and ends it. The failed side stays writable until its reader has the
 * error, plus one turn of the clock, so that a mux can still send GOAWAY
 * to the sender; then the pair closes on its behalf, and that side's
 * writable is errored with the same error. With nobody reading it, the
 * pair closes at once. Frames sent to the failed side are dropped.
 *
 * @throws {RangeError} for a negative delay or a negative or fractional
 *   frame size limit.
 */
export function memoryTransportPair(
  options: MemoryTransportOptions = {},
): [MemoryTransport, MemoryTransport] {
  const delay = options.delay;
  if (delay !== undefined && !(delay >= 0)) {
    throw new RangeError(`delay must not be negative, got ${delay}`);
  }
  const max = options.maxFrameSize ?? 0;
  if (!Number.isInteger(max) || max < 0) {
    throw new RangeError(`maxFrameSize must be a non-negative integer, got ${max}`);
  }
  const link = new MemoryLink(delay, max, options.clock ?? systemClock, options.logger);
  const a = new MemoryEnd(link, 'a', options.remotes?.[0]);
  const b = new MemoryEnd(link, 'b', options.remotes?.[1]);
  a.peer = b;
  b.peer = a;
  return [a, b];
}

class MemoryLink {
  readonly delay: number | undefined;
  readonly maxFrameSize: number;
  readonly clock: Clock;
  readonly logger: Logger;
  closed = false;
  readonly #timers = new Set<TimerHandle>();

  constructor(delay: number | undefined, maxFrameSize: number, clock: Clock, logger?: Logger) {
    this.delay = delay;
    this.maxFrameSize = maxFrameSize;
    this.clock = clock;
    this.logger = logger ?? noopLogger;
  }

  schedule(action: () => void): void {
    const delay = this.delay;
    if (delay === undefined) {
      action();
      return;
    }
    const timer = this.clock.setTimeout(() => {
      this.#timers.delete(timer);
      action();
    }, delay);
    this.#timers.add(timer);
  }

  /**
   * Closes the pair on behalf of [from]: its own readable ends at once, the
   * other side's after the frames in flight towards it (or at once when
   * [immediate]).
   */
  close(from: MemoryEnd, immediate = false): void {
    if (this.closed) {
      return;
    }
    this.closed = true;
    from.finish();
    const peer = from.peer;
    if (immediate) {
      for (const timer of this.#timers) {
        this.clock.clearTimeout(timer);
      }
      this.#timers.clear();
      peer.finish();
    } else {
      this.schedule(() => peer.finish());
    }
  }
}

class MemoryEnd implements MemoryTransport {
  readonly readable: ReadableStream<Uint8Array>;
  readonly writable: WritableStream<Uint8Array>;
  declare readonly remote?: string;
  peer!: MemoryEnd;
  readonly #link: MemoryLink;
  readonly #label: string;
  readonly #frames = new Fifo<Uint8Array>();
  readonly #done = new Deferred<void>();
  #ended = false;
  #failure: { readonly error: unknown; readonly closePair: boolean } | undefined;
  #cancelled = false;
  #arrived: Deferred<void> | undefined;
  #writableController: WritableStreamDefaultController | undefined;
  // Whether the user of `writable` closed or aborted it: its `closed` then
  // settles as asked.
  #closeAsked = false;

  constructor(link: MemoryLink, label: string, remote: string | undefined) {
    this.#link = link;
    this.#label = label;
    if (remote !== undefined) {
      this.remote = remote;
    }
    this.readable = new ReadableStream<Uint8Array>(
      {
        pull: (controller) => this.#pull(controller),
        cancel: () => {
          this.#cancelled = true;
          this.#frames.clear();
        },
      },
      { highWaterMark: 0 },
    );
    this.writable = new WritableStream<Uint8Array>({
      start: (controller) => {
        this.#writableController = controller;
      },
      write: (frame) => this.#send(frame),
      close: () => {
        this.#closeAsked = true;
        this.#link.close(this);
        return this.#done.promise;
      },
      abort: () => {
        this.#closeAsked = true;
        this.#link.close(this, true);
      },
    });
  }

  get maxFrameSize(): number {
    return this.#link.maxFrameSize;
  }

  /**
   * Ends this side's readable after the frames already delivered to it.
   * Unless this side's own writable was closed or aborted, the pair
   * closed under it: the writable is errored, with the failure that closed
   * the pair on this side's behalf, or `CONNECTION_LOST`.
   */
  finish(): void {
    if (!this.#ended) {
      this.#ended = true;
      this.#wake();
    }
    this.#done.resolve();
    if (!this.#closeAsked) {
      this.#closeAsked = true;
      this.#writableController?.error(
        this.#failure?.error ??
          SwitchboardError.of(StatusCode.connectionLost, 'the peer closed the connection'),
      );
    }
  }

  #send(frame: Uint8Array): void {
    const link = this.#link;
    if (link.closed) {
      return;
    }
    const copy = frame.slice();
    const max = link.maxFrameSize;
    link.schedule(() => {
      const peer = this.peer;
      if (peer.#ended) {
        return;
      }
      if (max > 0 && copy.length > max) {
        link.logger.log(
          'warn',
          'Switchboard.Transport',
          `memory transport ${this.#label}: frame of ${copy.length} bytes exceeds ${max}`,
        );
        peer.#failIncoming(
          SwitchboardError.of(
            StatusCode.frameTooLarge,
            `frame of ${copy.length} bytes exceeds ${max}`,
          ),
        );
        return;
      }
      if (!peer.#cancelled) {
        peer.#frames.push(copy);
        peer.#wake();
      }
    });
  }

  /**
   * Ends this side's readable with [error]. The pair stays open so that
   * this side can still answer; it closes on this side's behalf once the
   * reader has the error.
   */
  #failIncoming(error: unknown): void {
    if (this.#ended) {
      return;
    }
    this.#ended = true;
    const listened = this.readable.locked && !this.#cancelled;
    this.#failure = { error, closePair: listened };
    this.#wake();
    if (!listened) {
      // Nobody to react: close at once, as the stream binding does.
      this.#link.close(this);
    }
  }

  async #pull(controller: ReadableStreamDefaultController<Uint8Array>): Promise<void> {
    for (;;) {
      const frame = this.#frames.shift();
      if (frame !== undefined) {
        controller.enqueue(frame);
        return;
      }
      if (this.#ended) {
        const failure = this.#failure;
        if (failure === undefined) {
          controller.close();
          return;
        }
        controller.error(failure.error);
        if (failure.closePair) {
          this.#link.clock.setTimeout(() => this.#link.close(this), 0);
        }
        return;
      }
      this.#arrived ??= new Deferred();
      await this.#arrived.promise;
    }
  }

  #wake(): void {
    const arrived = this.#arrived;
    if (arrived !== undefined) {
      this.#arrived = undefined;
      arrived.resolve();
    }
  }
}
