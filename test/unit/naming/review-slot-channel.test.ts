/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Review finding (naming layer): `SlotChannel`, which `openChannelToSlot`
// returns and which "behaves like" the `MuxChannel` it carries, departs from
// the channel idioms of design sections 5 and 12 where `MuxChannel` keeps
// them: it is not async iterable (12.12), a write to its `writable` once it
// can no longer send is dropped and resolves instead of rejecting (12.8),
// and cancelling its `readable` before the first read does not reach the
// mux channel, so what arrives is held (and holds its credit) instead of
// being discarded (12.10). Each `test.fails` is a finding; the plain tests
// beside them pin the `MuxChannel` behaviour the slot channel is held to.
// Over the frame mux, on the in-memory transport.

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
import type { SlotChannel } from '../../../src/core/naming/slot-channel.ts';
import type { IncomingChannel } from '../../../src/core/switchboard/contract.ts';
import { StaticResolver } from '../../../src/core/switchboard/resolver.ts';
import type { Switchboard } from '../../../src/core/switchboard/switchboard.ts';
import { bytes, node, pump, svc, until, useMux } from '../../support/node/harness.ts';

useMux('frame');

/** How the backend treats a channel. */
type Mode = 'greet' | 'relocated' | 'flood';

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
  // 12.12; SlotChannel has no [Symbol.asyncIterator], so `for await` over
  // what openChannelToSlot returns throws a TypeError.
  test.fails('a SlotChannel is async iterable over its subframes too', async () => {
    const channel: SlotChannel = await router.openChannelToSlot(svc, 1);
    const iterable = channel as unknown as AsyncIterable<Uint8Array>;
    expect(typeof iterable[Symbol.asyncIterator]).toBe('function');
    for await (const subframe of iterable) {
      expect(utf8Decode(subframe)).toBe('hello');
      break;
    }
  });

  test('control: a write to a MuxChannel writable after the channel ended rejects', async () => {
    backend.mode = 'relocated';
    const channel = await router.openChannel(new ServiceAddress(svc, 1), { shard: 1 });
    await channel.done;
    const writer = channel.writable.getWriter();
    await expect(writer.write(bytes('lost'))).rejects.toBeDefined();
  });

  // Design 12.8: "A write to a channel that can no longer send rejects and
  // errors the writable (frame mux and QUIC binding alike); Dart's sink.add
  // dropped the data silently." SlotChannel.writable keeps Dart's sink.add:
  // `write` returns without sending once `canSend` is false, so a `pipeTo`
  // into it drains its source into nothing after the owner closed the
  // channel (RELOCATED here, which is never retried).
  test.fails('a write to a SlotChannel writable after the channel ended rejects too', async () => {
    backend.mode = 'relocated';
    const channel = await router.openChannelToSlot(svc, 1);
    await channel.done;
    expect(channel.canSend).toBe(false);
    const writer = channel.writable.getWriter();
    await expect(writer.write(bytes('lost'))).rejects.toBeDefined();
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
  // channel until its first pull, and its `cancel` only cancels that reader,
  // so a cancel before the first read leaves the mux channel's readable
  // live: what the owner sends piles up there, holding its credit.
  test.fails('cancelling a SlotChannel readable before reading discards what arrives too', async () => {
    backend.mode = 'flood';
    const channel = await router.openChannelToSlot(svc, 1);
    await channel.readable.cancel();
    await until(() => backend.channels.length > 0);
    await pump();
    expect(channel.channel.bufferedBytes).toBe(0);
  });
});
