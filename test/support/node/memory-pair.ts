/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import type { FrameTransport } from '../../../src/core/transport/contract.ts';

/**
 * A connected pair of in-memory frame transports for the node thread's
 * tests, standing in for the protocol core's `memoryTransportPair` until it
 * lands. Every frame is copied and delivered through a macrotask, as from a
 * socket. Closing one side's `writable` ends the other's `readable`;
 * `abort()` errors both.
 */
export function testTransportPair(
  options: { remoteA?: string; remoteB?: string } = {},
): [FrameTransport, FrameTransport] {
  const ab = new Pipe();
  const ba = new Pipe();
  return [
    ab.endpoint(ba, options.remoteA ?? 'memory'),
    ba.endpoint(ab, options.remoteB ?? 'memory'),
  ];
}

/** One direction: frames written on one side, read on the other. */
class Pipe {
  #controller: ReadableStreamDefaultController<Uint8Array> | undefined;
  readonly readable: ReadableStream<Uint8Array>;
  #ended = false;

  constructor() {
    this.readable = new ReadableStream<Uint8Array>(
      {
        start: (controller) => {
          this.#controller = controller;
        },
      },
      { highWaterMark: Number.POSITIVE_INFINITY },
    );
  }

  deliver(frame: Uint8Array): void {
    const copy = frame.slice();
    setTimeout(() => {
      if (!this.#ended) this.#controller?.enqueue(copy);
    }, 0);
  }

  end(): void {
    setTimeout(() => {
      if (this.#ended) return;
      this.#ended = true;
      try {
        this.#controller?.close();
      } catch {
        // Already errored.
      }
    }, 0);
  }

  fail(reason: unknown): void {
    if (this.#ended) return;
    this.#ended = true;
    try {
      this.#controller?.error(reason);
    } catch {
      // Already closed.
    }
  }

  /** The transport that reads [incoming] and writes into this pipe. */
  endpoint(incoming: Pipe, remote: string): FrameTransport {
    const outgoing = this;
    const writable = new WritableStream<Uint8Array>({
      write(frame) {
        outgoing.deliver(frame);
      },
      close() {
        outgoing.end();
      },
      abort(reason) {
        outgoing.fail(reason);
        incoming.fail(reason);
      },
    });
    return {
      readable: incoming.readable,
      writable,
      remote,
      abort() {
        const error = new Error('transport aborted');
        outgoing.fail(error);
        incoming.fail(error);
      },
    };
  }
}
