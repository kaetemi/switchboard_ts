/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { MuxFrame } from '../../../src/core/mux/mux-frame.ts';
import type { FrameTransport } from '../../../src/core/transport/contract.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';

/**
 * A transport whose output the test gates, the port of the Dart harness's
 * `GatedTransport` (`mux_harness.dart`): it tells the frame mux it is
 * ready (`isOutputReady`) only while it has an allowance of frames left,
 * so that the mux's output scheduler holds what it would send meanwhile.
 * Every frame written goes through at once all the same, and is recorded.
 */
export class GatedTransport implements FrameTransport {
  readonly readable: ReadableStream<Uint8Array>;
  readonly writable: WritableStream<Uint8Array>;

  /** Every frame written, decoded. */
  readonly written: MuxFrame[] = [];

  #allowance = 0;
  #waiter: Deferred<void> | undefined;

  constructor(inner: FrameTransport) {
    this.readable = inner.readable;
    const writer = inner.writable.getWriter();
    this.writable = new WritableStream<Uint8Array>({
      write: (frame) => {
        if (this.#allowance > 0) {
          this.#allowance--;
        }
        this.written.push(MuxFrame.decode(frame));
        return writer.write(frame);
      },
      close: () => writer.close(),
      abort: (reason) => writer.abort(reason),
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

  /** Takes [frames] more frames (fewer when negative); the mux writes them on its next turn. */
  allow(frames: number): void {
    this.#allowance += frames;
    const waiter = this.#waiter;
    if (waiter !== undefined && this.isOutputReady) {
      this.#waiter = undefined;
      waiter.resolve();
    }
  }

  /** The frames written on [channelId], or every frame but control messages. */
  on(channelId?: number): MuxFrame[] {
    return this.written.filter((frame) =>
      channelId === undefined ? frame.channelId !== 0 : frame.channelId === channelId,
    );
  }
}
