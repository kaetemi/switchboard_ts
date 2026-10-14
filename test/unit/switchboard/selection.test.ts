/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Instance selection on the consumer side (a `where` filter over the
// records, the node's selection policy) and the dispatch of a registration
// that accepts any instance: the naming side of the untrusted worker fleet
// (wiki page "Polyverse Switchboard Use Cases", entry 5). A port of the
// Dart selection_test.dart; the slot routing cases come with slot routing.

import { beforeEach, describe, expect, test } from 'vitest';
import { ChannelAddress, maxInstance, ServiceAddress } from '../../../src/core/address/index.ts';
import { utf8Decode } from '../../../src/core/bytes.ts';
import type { MuxChannel } from '../../../src/core/mux/contract.ts';
import { Name } from '../../../src/core/name.ts';
import { serviceRecord } from '../../../src/core/naming/naming-protocol.ts';
import { StatusCode } from '../../../src/core/status.ts';
import type {
  ChannelHandler,
  Resolver,
  SelectionPolicy,
  ServiceRecord,
} from '../../../src/core/switchboard/contract.ts';
import { StaticResolver } from '../../../src/core/switchboard/resolver.ts';
import type { Switchboard } from '../../../src/core/switchboard/switchboard.ts';
import { bytes, codeOf, node as harnessNode, rejectsWith } from '../../support/node/harness.ts';

const gpu = Name.of('gpu');

/** A seeded source of numbers in [0, 1) (mulberry32). */
function seeded(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 0x100000000;
  };
}

function node(
  options: { resolver?: Resolver; selection?: SelectionPolicy; random?: () => number } = {},
): Switchboard {
  return harnessNode(options);
}

/** A handler that sends `<tag> <instance in the header>` and closes. */
function answering(tag: string): ChannelHandler {
  return (incoming) => {
    void incoming.channel.send(bytes(`${tag} ${incoming.address.instance}`));
    void incoming.channel.close();
  };
}

/** What an {@link answering} handler sent, or the close status. */
async function answerOf(channel: MuxChannel): Promise<string> {
  const frames: Uint8Array[] = [];
  for await (const frame of channel.readable) frames.push(frame);
  const status = await channel.done;
  return frames.length === 0 ? `closed ${codeOf(status)}` : utf8Decode(frames[0] as Uint8Array);
}

/** Worker metadata: the models it offers, as JSON. */
function offering(models: string[]): Uint8Array {
  return bytes(JSON.stringify({ models }));
}

/** A filter for the workers whose metadata offers [model]. */
function offers(model: string): (record: ServiceRecord) => boolean {
  return (record) => {
    if (record.metadata.length === 0) {
      return false;
    }
    const decoded = JSON.parse(utf8Decode(record.metadata)) as { models: string[] };
    return decoded.models.includes(model);
  };
}

describe('selection', () => {
  let resolver: StaticResolver;

  beforeEach(async () => {
    // Four workers behind one node, each under its own id.
    const workers = node();
    for (let id = 1; id <= 4; id++) {
      workers.registerService(gpu, answering('w'), { instance: id });
    }
    const uri = await workers.listenMemory();
    const worker = (id: number, metadata?: Uint8Array): ServiceRecord =>
      serviceRecord(new ServiceAddress(gpu, id), { endpoints: [uri], metadata });
    resolver = new StaticResolver([
      // Out of order: selection sorts by instance.
      worker(3, offering(['b'])),
      worker(1, offering(['a'])),
      worker(4),
      worker(2, offering(['a', 'b'])),
    ]);
  });

  async function instancesOf(
    client: Switchboard,
    count: number,
    options: { where?: (record: ServiceRecord) => boolean; shard?: number } = {},
  ): Promise<number[]> {
    const instances: number[] = [];
    for (let i = 0; i < count; i++) {
      const answer = await answerOf(await client.openChannel(new ServiceAddress(gpu), options));
      instances.push(Number(answer.split(' ').at(-1)));
    }
    return instances;
  }

  test('round robin is the default', async () => {
    const client = node({ resolver });
    expect(client.selection).toBe('roundRobin');
    expect(await instancesOf(client, 8)).toEqual([1, 2, 3, 4, 1, 2, 3, 4]);
  });

  test('where filters on metadata', async () => {
    const client = node({ resolver });
    // One round-robin counter per type, over the matching records.
    expect(await instancesOf(client, 4, { where: offers('b') })).toEqual([2, 3, 2, 3]);
    expect(await instancesOf(client, 2, { where: offers('a') })).toEqual([1, 2]);
    const { record } = await client.selectAndConnect(new ServiceAddress(gpu), {
      where: (r) => r.metadata.length === 0,
    });
    expect(record.address.instance).toBe(4);
  });

  test('with a shard slot: sorted[slot mod n] of the matching', async () => {
    const client = node({ resolver });
    expect(await instancesOf(client, 2, { where: offers('b'), shard: 0 })).toEqual([2, 2]);
    expect(await instancesOf(client, 1, { where: offers('b'), shard: 5 })).toEqual([3]);
    expect(await instancesOf(client, 1, { shard: 5 })).toEqual([2]);
  });

  test('none left is NOT_FOUND', async () => {
    const client = node({ resolver });
    await rejectsWith(
      client.openChannel(new ServiceAddress(gpu), { where: () => false }),
      StatusCode.notFound,
    );
    await rejectsWith(
      client.openTalk(new ServiceAddress(gpu), { where: offers('z') }),
      StatusCode.notFound,
    );
    await rejectsWith(
      client.openChannel(new ServiceAddress(gpu), { shard: 3, where: offers('z') }),
      StatusCode.notFound,
    );
    // An explicit instance the filter refuses.
    await rejectsWith(
      client.openChannel(new ServiceAddress(gpu, 1), { where: offers('b') }),
      StatusCode.notFound,
    );
    expect(
      await answerOf(await client.openChannel(new ServiceAddress(gpu, 2), { where: offers('b') })),
    ).toBe('w 2');
  });

  test('a throwing filter fails the open', async () => {
    const client = node({ resolver });
    await expect(
      client.openChannel(new ServiceAddress(gpu), {
        where: () => {
          throw new Error('bad filter');
        },
      }),
    ).rejects.toThrow('bad filter');
  });

  test('random selection reaches every instance', async () => {
    const client = node({ resolver, selection: 'random', random: seeded(7) });
    const counts = new Map<number, number>();
    const sequence: number[] = [];
    for (let i = 0; i < 400; i++) {
      const { record } = await client.selectAndConnect(new ServiceAddress(gpu));
      sequence.push(record.address.instance);
      counts.set(record.address.instance, (counts.get(record.address.instance) ?? 0) + 1);
    }
    expect([...counts.keys()].sort()).toEqual([1, 2, 3, 4]);
    for (const n of counts.values()) {
      expect(n).toBeGreaterThanOrEqual(60);
      expect(n).toBeLessThanOrEqual(140);
    }
    // Not a rotation.
    expect(sequence.slice(0, 8)).not.toEqual([1, 2, 3, 4, 1, 2, 3, 4]);
    // The injected source decides: the same seed, the same choices.
    const again = node({ resolver, selection: 'random', random: seeded(7) });
    const replay: number[] = [];
    for (let i = 0; i < 40; i++) {
      replay.push((await again.selectAndConnect(new ServiceAddress(gpu))).record.address.instance);
    }
    expect(replay).toEqual(sequence.slice(0, 40));
    // Over the matching records only.
    const filtered = new Set<number>();
    for (let i = 0; i < 100; i++) {
      const { record } = await client.selectAndConnect(new ServiceAddress(gpu), {
        where: offers('a'),
      });
      filtered.add(record.address.instance);
    }
    expect(filtered).toEqual(new Set([1, 2]));
  });

  test('random selection keeps shard selection', async () => {
    const client = node({ resolver, selection: 'random', random: seeded(1) });
    expect(await instancesOf(client, 4, { shard: 6 })).toEqual([3, 3, 3, 3]);
  });
});

describe('acceptAnyInstance', () => {
  let server: Switchboard;
  let client: Switchboard;
  let uri: string;

  beforeEach(async () => {
    server = node();
    uri = await server.listenMemory();
    client = node();
  });

  async function open(instance: number): Promise<string> {
    return answerOf(await client.openChannelAt(uri, new ChannelAddress({ type: gpu, instance })));
  }

  test('serves a channel addressed to any instance', async () => {
    server.registerService(gpu, answering('any'), { acceptAnyInstance: true });
    expect(await open(0x1234)).toBe('any 4660');
    expect(await open(maxInstance)).toBe(`any ${maxInstance}`);
    expect(await open(0)).toBe('any 0');
    // Other types are not affected.
    expect(
      await answerOf(await client.openChannelAt(uri, new ChannelAddress({ type: 'cpu' }))),
    ).toBe('closed notFound');
  });

  test('an exact registration still wins', async () => {
    server.registerService(gpu, answering('any'), { acceptAnyInstance: true });
    server.registerService(gpu, answering('exact'), { instance: 0x1234 });
    expect(await open(0x1234)).toBe('exact 4660');
    expect(await open(0x1235)).toBe('any 4661');
  });

  test('comes before the first registered for instance 0', async () => {
    server.registerService(gpu, answering('five'), { instance: 5 });
    server.registerService(gpu, answering('seven'), { instance: 7, acceptAnyInstance: true });
    expect(await open(0)).toBe('seven 0');
    expect(await open(5)).toBe('five 5');
    expect(await open(9)).toBe('seven 9');
  });

  test('registering again without it, or unregistering, ends it', async () => {
    server.registerService(gpu, answering('any'), { acceptAnyInstance: true });
    expect(await open(0x1234)).toBe('any 4660');
    server.registerService(gpu, answering('zero'));
    expect(await open(0x1234)).toBe('closed notFound');
    expect(await open(0)).toBe('zero 0');
    server.registerService(gpu, answering('any'), { acceptAnyInstance: true });
    server.unregisterService(gpu);
    server.registerService(gpu, answering('seven'), { instance: 7 });
    expect(await open(0x1234)).toBe('closed notFound');
  });

  test('a worker registered by a registrar under an id it was never told is reached through that id', async () => {
    server.registerService(gpu, answering('worker'), { acceptAnyInstance: true });
    const consumer = node({
      resolver: new StaticResolver([
        serviceRecord(new ServiceAddress(gpu, 0x1234), {
          endpoints: [uri],
          metadata: offering(['a']),
        }),
      ]),
    });
    expect(
      await answerOf(await consumer.openChannel(new ServiceAddress(gpu), { where: offers('a') })),
    ).toBe('worker 4660');
  });
});
