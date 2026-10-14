/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * Talk over the real frame mux: two `FrameMuxConnection`s joined by the
 * memory transport pair (Dart's `muxPair` in `test/mux_harness.dart`), the
 * shared iterator of a connection's incoming channels, and
 * {@link FrameMuxPeers}, the frame-mux counterpart of `Peers` in
 * `bulk-peers.ts` (the Dart `talk_bulk_test.dart` harness, which runs over
 * the real mux in Dart). Every connection made here closes when the test
 * finishes.
 */

import { onTestFinished } from 'vitest';
import type { MuxChannel, MuxConnection, MuxOptions } from '../../../src/core/mux/contract.ts';
import { FrameMuxConnection } from '../../../src/core/mux/frame-mux-connection.ts';
import { MuxCommand, MuxFrame } from '../../../src/core/mux/mux-frame.ts';
import { decodeStatusPayload } from '../../../src/core/mux/mux-protocol.ts';
import type { Status } from '../../../src/core/status.ts';
import type { TalkOptions } from '../../../src/core/talk/contract.ts';
import { installBulkRouting } from '../../../src/core/talk/talk-bulk-routes.ts';
import { TalkChannel } from '../../../src/core/talk/talk-channel.ts';
import { TalkFrame } from '../../../src/core/talk/talk-frame.ts';
import type { FrameTransport } from '../../../src/core/transport/contract.ts';
import {
  type MemoryTransportOptions,
  memoryTransportPair,
} from '../../../src/core/transport/memory-transport.ts';

/** Which side wrote a frame. */
export type WireSide = 'initiator' | 'acceptor';

/** A frame one side of a {@link frameMuxPair} wrote, decoded. */
export interface WireFrame {
  readonly from: WireSide;
  readonly frame: MuxFrame;
}

/**
 * Every frame both sides of a {@link frameMuxPair} wrote to the transport,
 * in the order each side wrote them: what crosses the wire, for the
 * assertions the Dart tests make on bytes.
 */
export class WireLog {
  readonly frames: WireFrame[] = [];

  /** @internal */
  record(from: WireSide, bytes: Uint8Array): void {
    this.frames.push({ from, frame: MuxFrame.decode(bytes.slice()) });
  }

  /** The frames [from] wrote on channel [channelId] with [command]. */
  of(from: WireSide, channelId: number, command?: MuxCommand): MuxFrame[] {
    return this.frames
      .filter(
        (f) =>
          f.from === from &&
          f.frame.channelId === channelId &&
          (command === undefined || f.frame.command === command),
      )
      .map((f) => f.frame);
  }

  /** The Talk frames [from] sent on channel [channelId], decoded from its DATA. */
  talk(from: WireSide, channelId: number): TalkFrame[] {
    return this.of(from, channelId, MuxCommand.data).map((f) => TalkFrame.decode(f.payload));
  }

  /** The statuses of the CLOSEs [from] sent on channel [channelId], as they went on the wire. */
  closes(from: WireSide, channelId: number): Status[] {
    return this.of(from, channelId, MuxCommand.close).map((f) =>
      decodeStatusPayload(f.payload, 'CLOSE'),
    );
  }
}

/** [transport] with up to [frames] incoming frames read ahead into a queue. */
function queuedAhead(transport: FrameTransport, frames: number): FrameTransport {
  const ahead: FrameTransport = {
    readable: transport.readable.pipeThrough(
      new TransformStream<Uint8Array, Uint8Array>(undefined, undefined, { highWaterMark: frames }),
    ),
    writable: transport.writable,
  };
  return Object.assign(
    ahead,
    transport.maxFrameSize === undefined ? {} : { maxFrameSize: transport.maxFrameSize },
    transport.remote === undefined ? {} : { remote: transport.remote },
  );
}

/** [transport] with every frame written to it recorded in [log] as [side]'s. */
function tapped(transport: FrameTransport, log: WireLog, side: WireSide): FrameTransport {
  const writer = transport.writable.getWriter();
  const tap: FrameTransport = {
    readable: transport.readable,
    writable: new WritableStream<Uint8Array>({
      write: (chunk) => {
        log.record(side, chunk);
        return writer.write(chunk);
      },
      close: () => writer.close(),
      abort: (reason: unknown) => writer.abort(reason),
    }),
  };
  return Object.assign(
    tap,
    transport.maxFrameSize === undefined ? {} : { maxFrameSize: transport.maxFrameSize },
    transport.remote === undefined ? {} : { remote: transport.remote },
  );
}

/** Keep-alive off so that tests only see the frames they cause (Dart `quiet`). */
export const quiet: Partial<MuxOptions> = Object.freeze({
  keepAliveInterval: undefined,
  goAwayGrace: 2000,
});

/** What {@link frameMuxPair} takes. */
export interface FrameMuxPairInit {
  /** Mux options of both sides, over {@link quiet}. */
  readonly options?: Partial<MuxOptions>;
  /** Mux options of the initiator only, over [options]. */
  readonly initiator?: Partial<MuxOptions>;
  /** Mux options of the acceptor only, over [options]. */
  readonly acceptor?: Partial<MuxOptions>;
  /** Options of the memory transport pair (a delivery delay, a frame size limit). */
  readonly transport?: MemoryTransportOptions;
  /** The two connected transports to run over (the initiator's first) instead of a memory pair: TCP on the loopback, say. */
  readonly transports?: readonly [FrameTransport, FrameTransport];
  /** Install bulk routing on both connections at once (default true): Dart routes with `TalkChannel.adoptBulk` from the start. */
  readonly routeBulk?: boolean;
  /** Records what both sides write. */
  readonly wire?: WireLog;
  /**
   * Frames queue ahead of each mux, up to this many, as a stream
   * transport's decoder queues the frames of one read: the mux then takes
   * them one after the other without waiting for the transport between
   * them. Undefined (the default): the memory transport hands each frame
   * over when the mux reads.
   */
  readonly queued?: number;
}

/**
 * An initiator and an acceptor frame mux over a memory transport pair (or
 * over the `transports` given), closed when the test finishes.
 */
export function frameMuxPair(
  init: FrameMuxPairInit = {},
): [FrameMuxConnection, FrameMuxConnection] {
  const [memoryA, memoryB] = init.transports ?? memoryTransportPair(init.transport);
  const wire = init.wire;
  let a: FrameTransport = wire === undefined ? memoryA : tapped(memoryA, wire, 'initiator');
  let b: FrameTransport = wire === undefined ? memoryB : tapped(memoryB, wire, 'acceptor');
  if (init.queued !== undefined) {
    a = queuedAhead(a, init.queued);
    b = queuedAhead(b, init.queued);
  }
  const client = new FrameMuxConnection(a, {
    isInitiator: true,
    ...quiet,
    ...init.options,
    ...init.initiator,
  });
  const server = new FrameMuxConnection(b, {
    isInitiator: false,
    ...quiet,
    ...init.options,
    ...init.acceptor,
  });
  if (init.routeBulk ?? true) {
    installBulkRouting(client);
    installBulkRouting(server);
  }
  onTestFinished(async () => {
    await client.close();
    await server.close();
  });
  return [client, server];
}

const incomingIterators = new WeakMap<MuxConnection, AsyncIterator<MuxChannel>>();

/** The one iterator of [connection]'s incoming channels, shared by the helpers here. */
export function incomingOf(connection: MuxConnection): AsyncIterator<MuxChannel> {
  let iterator = incomingIterators.get(connection);
  if (iterator === undefined) {
    iterator = connection.incoming[Symbol.asyncIterator]();
    incomingIterators.set(connection, iterator);
  }
  return iterator;
}

/** The next channel [connection]'s peer opened that no filter adopted. */
export async function nextIncoming(connection: MuxConnection): Promise<MuxChannel> {
  const next = await incomingOf(connection).next();
  if (next.done === true) {
    throw new Error('no more incoming channels');
  }
  return next.value;
}

/**
 * Hands every channel [connection]'s peer opens to [onChannel], until the
 * connection ends; resolves then.
 */
export async function acceptAll(
  connection: MuxConnection,
  onChannel: (channel: MuxChannel) => void,
): Promise<void> {
  for (;;) {
    const next = await incomingOf(connection).next();
    if (next.done === true) {
      return;
    }
    onChannel(next.value);
  }
}

/**
 * A client and a server frame mux over one memory transport pair, each
 * routing the bulk channels its peer opens from the start; the server's
 * channels become Talk channels with `serverOptions`. The frame-mux
 * counterpart of `Peers` in `bulk-peers.ts`, with the same members.
 */
export class FrameMuxPeers {
  readonly client: FrameMuxConnection;
  readonly server: FrameMuxConnection;
  readonly serverOptions: Partial<TalkOptions> | undefined;

  private constructor(
    client: FrameMuxConnection,
    server: FrameMuxConnection,
    serverOptions: Partial<TalkOptions> | undefined,
  ) {
    this.client = client;
    this.server = server;
    this.serverOptions = serverOptions;
  }

  /** Connects a pair with the mux `options` of both sides (a `clock`, an `initialWindow`, ...). */
  static connect(
    init: {
      options?: Partial<MuxOptions>;
      serverOptions?: Partial<TalkOptions>;
      transport?: MemoryTransportOptions;
      transports?: readonly [FrameTransport, FrameTransport];
    } = {},
  ): FrameMuxPeers {
    const [client, server] = frameMuxPair({
      ...(init.options === undefined ? {} : { options: init.options }),
      ...(init.transport === undefined ? {} : { transport: init.transport }),
      ...(init.transports === undefined ? {} : { transports: init.transports }),
    });
    return new FrameMuxPeers(client, server, init.serverOptions);
  }

  /** The next channel the client opened, at the server. */
  accept(): Promise<MuxChannel> {
    return nextIncoming(this.server);
  }

  /** The Talk channel over the next channel the client opened, with `serverOptions`. */
  async acceptTalk(): Promise<TalkChannel> {
    return new TalkChannel(await this.accept(), this.serverOptions);
  }

  /** A Talk channel from the client with `options`, and its server end. */
  async open(init: { options?: Partial<TalkOptions> } = {}): Promise<[TalkChannel, TalkChannel]> {
    const talk = new TalkChannel(this.client.open(new Uint8Array(0)), init.options);
    return [talk, await this.acceptTalk()];
  }

  /** Closes both connections. */
  async close(): Promise<void> {
    await this.client.close();
    await this.server.close();
  }
}

/**
 * The far side of a frame mux's transport, driven with raw frames (Dart's
 * `RawPeer` in `test/mux_harness.dart`). What {@link send} is given is
 * queued for the mux all at once, as a stream transport's decoder queues
 * the frames of one read: the mux takes them one after the other without
 * waiting for the transport between them. Every frame the mux writes is in
 * {@link written}, decoded.
 */
export class RawMuxPeer {
  /** The transport the mux runs over. */
  readonly transport: FrameTransport;
  /** Every frame the mux wrote, in order. */
  readonly written: MuxFrame[] = [];
  #controller!: ReadableStreamDefaultController<Uint8Array>;
  #closed = false;

  constructor() {
    this.transport = {
      readable: new ReadableStream<Uint8Array>(
        {
          start: (controller) => {
            this.#controller = controller;
          },
        },
        { highWaterMark: Number.POSITIVE_INFINITY },
      ),
      writable: new WritableStream<Uint8Array>({
        write: (chunk) => {
          this.written.push(MuxFrame.decode(chunk.slice()));
        },
        close: () => this.close(),
      }),
      remote: 'raw peer',
    };
  }

  /** Queues [frames] for the mux, in order, at once. */
  send(...frames: MuxFrame[]): void {
    if (this.#closed) {
      throw new Error('raw peer closed');
    }
    for (const frame of frames) {
      this.#controller.enqueue(frame.encode());
    }
  }

  /** The CLOSEs the mux sent, as `id status` (the status's code). */
  get closes(): string[] {
    return this.written
      .filter((f) => f.command === MuxCommand.close)
      .map((f) => `${f.channelId} ${decodeStatusPayload(f.payload, 'CLOSE').code}`);
  }

  /** Ends the mux's readable. */
  close(): void {
    if (!this.#closed) {
      this.#closed = true;
      this.#controller.close();
    }
  }
}

/**
 * A frame mux (the acceptor unless [isInitiator]) whose peer is a
 * {@link RawMuxPeer}, with bulk routing installed; closed when the test
 * finishes.
 */
export function rawMuxPair(
  init: { options?: Partial<MuxOptions>; isInitiator?: boolean } = {},
): [FrameMuxConnection, RawMuxPeer] {
  const peer = new RawMuxPeer();
  const connection = new FrameMuxConnection(peer.transport, {
    isInitiator: init.isInitiator ?? false,
    ...quiet,
    ...init.options,
  });
  installBulkRouting(connection);
  onTestFinished(() => connection.close());
  return [connection, peer];
}
