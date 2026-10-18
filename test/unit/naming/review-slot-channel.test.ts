/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Review findings (naming layer), fixed: `SlotChannel`, which
// `openChannelToSlot` returns and which "behaves like" the `MuxChannel` it
// carries, keeps the channel idioms of design sections 5 and 12 as
// `MuxChannel` does: it is async iterable (12.12), a write to its
// `writable` once it can no longer send rejects and errors the writable
// (12.8), and cancelling its `readable` before the first read reaches the
// mux channel, and a replacement's, so what arrives is discarded and its
// credit returned (12.10). The controls pin the `MuxChannel` behaviour the
// slot channel is held to. Over the frame mux, on the in-memory transport.

import { afterEach, beforeEach, describe, expect, test } from 'vitest';
import { ServiceAddress } from '../../../src/core/address/index.ts';
import { utf8Decode } from '../../../src/core/bytes.ts';
import type { MuxChannel } from '../../../src/core/mux/contract.ts';
import {
  MovedStatus,
  SlotEntry,
  SlotMode,
  SlotSpace,
  serviceRecord,
} from '../../../src/core/naming/naming-protocol.ts';
import { SlotChannel } from '../../../src/core/naming/slot-channel.ts';
import { StatusCode } from '../../../src/core/status.ts';
import type { IncomingChannel } from '../../../src/core/switchboard/contract.ts';
import { StaticResolver } from '../../../src/core/switchboard/resolver.ts';
import type { Switchboard } from '../../../src/core/switchboard/switchboard.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';
import { isSettled } from '../../support/naming/harness.ts';
import { bytes, node, pump, rejectsWith, svc, until, useMux } from '../../support/node/harness.ts';

useMux('frame');

/** How the backend treats a channel. */
type Mode = 'greet' | 'relocated' | 'moved' | 'flood';

class Backend {
  sb!: Switchboard;
  uri!: string;
  mode: Mode = 'greet';
  /** The channels it received, in order. */
  readonly channels: MuxChannel[] = [];

  async start(): Promise<void> {
    this.sb = node();
    this.sb.registerService(svc, (incoming) => this.#serve(incoming), { instance: 1 });
    this.uri = await this.sb.listenMemory();
  }

  #serve(incoming: IncomingChannel): void {
    const channel = incoming.channel;
    this.channels.push(channel);
    switch (this.mode) {
      case 'greet':
        void channel.send(bytes('hello')).catch(() => {});
        break;
      case 'relocated':
        void incoming.reject(new MovedStatus({ owner: 2, epoch: 2 }).toStatus({ relocated: true }));
        break;
      case 'moved':
        void incoming.reject(new MovedStatus({ owner: 2, epoch: 2 }).toStatus());
        break;
      case 'flood':
        // Ten subframes of 100 bytes, sent as the window lets them.
        void (async () => {
          for (let i = 0; i < 10; i++) {
            await channel.send(bytes(`${i}`.repeat(100)));
          }
        })().catch(() => {});
        break;
    }
  }
}

describe('review: SlotChannel against the MuxChannel idioms', () => {
  let backend: Backend;
  let resolver: StaticResolver;
  let router: Switchboard;

  beforeEach(async () => {
    backend = new Backend();
    await backend.start();
    resolver = new StaticResolver([
      serviceRecord(new ServiceAddress(svc, 1), { endpoints: [backend.uri] }),
    ]);
    resolver.defineSlots(new SlotSpace(svc, { count: 4, mode: SlotMode.static }));
    resolver.setSlot(svc, 1, SlotEntry.owned(1, { epoch: 1 }));
    router = node({ resolver });
  });

  afterEach(async () => {
    await resolver.close();
  });

  test('control: a MuxChannel is async iterable over its subframes', async () => {
    const channel = await router.openChannel(new ServiceAddress(svc, 1), { shard: 1 });
    for await (const subframe of channel) {
      expect(utf8Decode(subframe)).toBe('hello');
      break;
    }
  });

  // Design 5 ("Channels ([Symbol.asyncIterator] over their subframes)") and
  // 12.12: `for await` over what openChannelToSlot returns.
  test('a SlotChannel is async iterable over its subframes too', async () => {
    const channel: SlotChannel = await router.openChannelToSlot(svc, 1);
    expect(typeof channel[Symbol.asyncIterator]).toBe('function');
    for await (const subframe of channel) {
      expect(utf8Decode(subframe)).toBe('hello');
      break;
    }
  });

  // 12.12: `break` cancels the readable, so what arrives is discarded, and
  // leaves the channel open.
  test('a break out of a SlotChannel loop discards what arrives and leaves it open', async () => {
    backend.mode = 'flood';
    const channel = await router.openChannelToSlot(svc, 1);
    for await (const subframe of channel) {
      expect(utf8Decode(subframe)).toBe('0'.repeat(100));
      break;
    }
    await pump();
    expect(channel.channel.bufferedBytes).toBe(0);
    expect(channel.canSend).toBe(true);
    await channel.send(bytes('still open'));
  });

  test('control: a write to a MuxChannel writable after the channel ended rejects', async () => {
    backend.mode = 'relocated';
    const channel = await router.openChannel(new ServiceAddress(svc, 1), { shard: 1 });
    await channel.done;
    const writer = channel.writable.getWriter();
    await expect(writer.write(bytes('lost'))).rejects.toBeDefined();
  });

  // Design 12.8: "A write to a channel that can no longer send rejects and
  // errors the writable"; the slot channel's `write` is its `send`, so a
  // `pipeTo` into it fails once the owner closed the channel (RELOCATED
  // here, which is never retried) instead of draining its source.
  test('a write to a SlotChannel writable after the channel ended rejects too', async () => {
    backend.mode = 'relocated';
    const channel = await router.openChannelToSlot(svc, 1);
    await channel.done;
    expect(channel.canSend).toBe(false);
    const writer = channel.writable.getWriter();
    await rejectsWith(writer.write(bytes('lost')), StatusCode.failedPrecondition);
    await rejectsWith(writer.closed, StatusCode.failedPrecondition);
  });

  // A subframe held while the replacement is being opened, and dropped
  // because the channel ended first, fails its write and the writable.
  test('a held subframe dropped at the end fails the SlotChannel writable', async () => {
    backend.mode = 'moved';
    const first = await router.openChannel(new ServiceAddress(svc, 1), { shard: 1 });
    expect((await first.done).is(StatusCode.moved)).toBe(true);
    const reopened = new Deferred<MuxChannel | undefined>();
    const channel = new SlotChannel(svc, 1, first, () => reopened.promise);
    await pump(1);
    const writer = channel.writable.getWriter();
    const written = writer.write(bytes('held'));
    written.catch(() => {});
    await pump();
    // Held: the channel can still send, and the write waits.
    expect(channel.canSend).toBe(true);
    expect(await isSettled(written)).toBe(false);
    // Nowhere else to go: the channel ends with the MOVED itself.
    reopened.resolve(undefined);
    await rejectsWith(written, StatusCode.failedPrecondition);
    await rejectsWith(writer.closed, StatusCode.failedPrecondition);
    expect((await channel.done).is(StatusCode.moved)).toBe(true);
  });

  test('control: cancelling a MuxChannel readable before reading discards what arrives', async () => {
    backend.mode = 'flood';
    const channel = await router.openChannel(new ServiceAddress(svc, 1), { shard: 1 });
    await channel.readable.cancel();
    await until(() => backend.channels.length > 0);
    await pump();
    expect(channel.bufferedBytes).toBe(0);
  });

  // Design 12.10: cancelling a channel's readable discards what arrives and
  // returns its credit. SlotChannel's readable has no reader of the mux
  // channel until its first pull; a cancel before it reaches the mux
  // channel's readable all the same, so what the owner sends does not pile
  // up there, holding its credit.
  test('cancelling a SlotChannel readable before reading discards what arrives too', async () => {
    backend.mode = 'flood';
    const channel = await router.openChannelToSlot(svc, 1);
    await channel.readable.cancel();
    await until(() => backend.channels.length > 0);
    await pump();
    expect(channel.channel.bufferedBytes).toBe(0);
  });

  test('cancelling a SlotChannel readable discards what its replacement receives too', async () => {
    backend.mode = 'moved';
    const first = await router.openChannel(new ServiceAddress(svc, 1), { shard: 1 });
    await first.done;
    const reopened = new Deferred<MuxChannel | undefined>();
    const channel = new SlotChannel(svc, 1, first, () => reopened.promise);
    await pump(1);
    // Cancelled while the replacement is being opened.
    await channel.readable.cancel();
    backend.mode = 'flood';
    reopened.resolve(await router.openChannel(new ServiceAddress(svc, 1), { shard: 1 }));
    await until(() => channel.retried && backend.channels.length > 1);
    await pump();
    expect(channel.channel).not.toBe(first);
    expect(channel.channel.bufferedBytes).toBe(0);
    expect(channel.canSend).toBe(true);
  });
});
