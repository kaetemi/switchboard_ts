/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import type { MuxChannel, MuxChannelCarrier } from '../../../src/core/mux/contract.ts';
import { type Status, StatusCode, SwitchboardError } from '../../../src/core/status.ts';
import { TalkFrame } from '../../../src/core/talk/talk-frame.ts';

/**
 * A channel carrying a mux channel, as a slot channel does (Dart's
 * `SlotChannel` with a reopen that finds nowhere else to go), passing
 * everything through to it: the stand-in for a `MuxChannelCarrier`. It
 * can also refuse subframes longer than [limit] with FRAME_TOO_LARGE, as
 * the Dart `_LimitedSink` does, and hand each frame Talk sends to an
 * [intercept]or first, as the Dart `_InterceptingSink` does. Talk hands
 * every frame to the carrier's `send` synchronously, and a refusal thrown
 * there reaches whoever sent the frame, as `MuxChannel.send` throws
 * FRAME_TOO_LARGE.
 */
export class Carrier<C extends MuxChannel = MuxChannel> implements MuxChannelCarrier {
  readonly channel: C;

  /**
   * Subframes over this many bytes are refused FRAME_TOO_LARGE. Talk sizes
   * its frames against the channel's `maxSubframeLength` (beyond it a
   * message goes as a bulk payload), so a refusal needs a lower limit here.
   */
  limit: number;

  /**
   * When set, gets each frame instead of passing it on: the interceptor
   * passes it on with `pass`, and may do more before or after, or throw
   * instead.
   */
  intercept: ((frame: TalkFrame, pass: () => Promise<void>) => Promise<void>) | undefined;

  constructor(channel: C, limit = Number.POSITIVE_INFINITY) {
    this.channel = channel;
    this.limit = limit;
  }

  get readable(): ReadableStream<Uint8Array> {
    return this.channel.readable;
  }

  get writable(): WritableStream<Uint8Array> {
    return this.channel.writable;
  }

  get manualCredit(): boolean {
    return this.channel.manualCredit;
  }

  set manualCredit(value: boolean) {
    this.channel.manualCredit = value;
  }

  consumed(bytes: number, subframes?: number): void {
    this.channel.consumed(bytes, subframes);
  }

  send(subframe: Uint8Array): Promise<void> {
    if (subframe.length > this.limit) {
      throw SwitchboardError.of(
        StatusCode.frameTooLarge,
        `subframe of ${subframe.length} bytes, more than ${this.limit}`,
      );
    }
    const intercept = this.intercept;
    if (intercept === undefined) {
      return this.channel.send(subframe);
    }
    return intercept(TalkFrame.decode(subframe), () => this.channel.send(subframe));
  }

  get isWritable(): boolean {
    return this.channel.isWritable;
  }

  get whenWritable(): Promise<void> {
    return this.channel.whenWritable;
  }

  close(status?: Status): Promise<void> {
    return this.channel.close(status);
  }

  get done(): Promise<Status> {
    return this.channel.done;
  }
}
