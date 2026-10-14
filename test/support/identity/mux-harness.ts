// The part of Dart's `test/mux_harness.dart` the identity tests use: the
// quiet and raw options, a connected pair of frame muxes over the memory
// transport, and a raw peer that drives the far side of a transport with
// hex frames. The mux test ports keep their own copy of the whole harness
// (`test/unit/mux/support.ts`); this one can give way to it once both have
// landed.
import { hexBytes, hexString } from '../../../src/core/bytes.ts';
import type { MuxOptions } from '../../../src/core/mux/contract.ts';
import { FrameMuxConnection } from '../../../src/core/mux/frame-mux-connection.ts';
import {
  MuxControlMessage,
  type MuxControlType,
  MuxFrame,
} from '../../../src/core/mux/mux-frame.ts';
import type { FrameTransport } from '../../../src/core/transport/contract.ts';
import { memoryTransportPair } from '../../../src/core/transport/memory-transport.ts';

/** Keep-alive off so that tests only see the frames they cause. */
export const quiet: Partial<MuxOptions> = Object.freeze({
  keepAliveInterval: undefined,
  goAwayGrace: 2000,
});

/**
 * Additionally no LIMITS, for byte exact raw peer tests, and DATA sent
 * without waiting for the raw peer's LIMITS (which it may never send).
 */
export const rawOptions: Partial<MuxOptions> = Object.freeze({
  keepAliveInterval: undefined,
  announceLimits: false,
  awaitPeerLimits: false,
});

/** Two connected muxes over a memory pair: the initiator, then the acceptor. */
export function muxPair(
  options: { initiator?: Partial<MuxOptions>; acceptor?: Partial<MuxOptions> } = {},
): [FrameMuxConnection, FrameMuxConnection] {
  const [a, b] = memoryTransportPair();
  return [
    new FrameMuxConnection(a, { ...(options.initiator ?? quiet), isInitiator: true }),
    new FrameMuxConnection(b, { ...(options.acceptor ?? quiet), isInitiator: false }),
  ];
}

/** The far side of a transport, driven with raw frames. */
export class RawPeer {
  readonly transport: FrameTransport;
  readonly #reader: ReadableStreamDefaultReader<Uint8Array>;
  readonly #writer: WritableStreamDefaultWriter<Uint8Array>;

  constructor(transport: FrameTransport) {
    this.transport = transport;
    this.#reader = transport.readable.getReader();
    this.#writer = transport.writable.getWriter();
  }

  /** Sends the frame [hex] (Dart `sink.add`: not awaited, in order). */
  send(hex: string): void {
    this.#writer.write(hexBytes(hex)).catch(() => {});
  }

  /** Closes this side's output (Dart `transport.sink.close()`). */
  async closeOutput(): Promise<void> {
    await this.#writer.close().catch(() => {});
  }

  /** The next frame's bytes. @throws {Error} once the transport closed. */
  async nextBytes(): Promise<Uint8Array> {
    const { done, value } = await this.#reader.read();
    if (done === true) {
      throw new Error('the transport closed');
    }
    return value;
  }

  async nextHex(): Promise<string> {
    return hexString(await this.nextBytes());
  }

  async next(): Promise<MuxFrame> {
    return MuxFrame.decode(await this.nextBytes());
  }

  /** Skips frames until a control message of [type] and returns it. */
  async nextControl(type: MuxControlType): Promise<MuxControlMessage> {
    while (true) {
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
    const frames: MuxFrame[] = [];
    while (true) {
      const { done, value } = await this.#reader.read();
      if (done === true) {
        return frames;
      }
      frames.push(MuxFrame.decode(value));
    }
  }
}

/** A mux over one side of a memory pair, and a raw peer on the other. */
export function rawPair(
  options: { muxIsInitiator?: boolean; options?: Partial<MuxOptions> } = {},
): [FrameMuxConnection, RawPeer] {
  const [a, b] = memoryTransportPair();
  return [
    new FrameMuxConnection(a, {
      ...(options.options ?? rawOptions),
      isInitiator: options.muxIsInitiator ?? true,
    }),
    new RawPeer(b),
  ];
}

/** Whether [frame] is a control message of [type]. */
export function isControl(frame: MuxFrame, type: MuxControlType): boolean {
  return frame.channelId === 0 && MuxControlMessage.decode(frame.payload).knownType === type;
}

/** The frame of a control message of [type] carrying [payload], in hex. */
export function control(type: MuxControlType, payload: Uint8Array): string {
  return hexString(new MuxControlMessage(type, payload).toFrame().encode());
}
