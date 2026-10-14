/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Port of the Dart `test/integration/fleet_consumer_test.dart`: the
// untrusted worker fleet (wiki page "Polyverse Switchboard Use Cases",
// entry 5) in one process over `mem://` endpoints, as in Dart. A naming
// service; a scaler that registers eight workers on its own `_ns` channel
// with requested ids, their endpoints and metadata (models and key);
// workers that only accept connections, behind a policy, checking their
// key; and a consumer joined to the mesh that keeps a PeerSet over them,
// exchanges capabilities on a per-worker channel, and load-balances over
// the online ones. Covered: all online, a worker reboot, an unregistered
// worker (held, then removed), a refused push from a worker, and
// credential isolation (each worker sees its own key, never the mesh
// credential; the naming service sees the mesh credential, and refuses
// channels without it).

import { expect, test } from 'vitest';
import { ChannelAddress } from '../../src/core/address/index.ts';
import { utf8Decode, utf8Encode } from '../../src/core/bytes.ts';
import type { MuxConnection } from '../../src/core/mux/contract.ts';
import { Name } from '../../src/core/name.ts';
import { Services } from '../../src/core/naming/naming-protocol.ts';
import { NamingService } from '../../src/core/naming/naming-service.ts';
import { Status, StatusCode } from '../../src/core/status.ts';
import { ChannelPolicies } from '../../src/core/switchboard/channel-policy.ts';
import type { IncomingChannel } from '../../src/core/switchboard/contract.ts';
import { MeshNode, namingClientFor } from '../../src/core/switchboard/mesh-node.ts';
import { type Peer, PeerSet, PeerState } from '../../src/core/switchboard/peer-set.ts';
import type { TalkOptions } from '../../src/core/talk/contract.ts';
import { TalkChannel } from '../../src/core/talk/index.ts';
import { codeOf, limit, until } from './mesh-support.ts';
import { node, onCleanup, type Switchboard, within } from './node-support.ts';

const talkOptions: Partial<TalkOptions> = { requestTimeout: 2000, replyTimeout: 1000 };

const gpu = Name.of('gpu');
const back = Name.of('back');
const meshSecret = utf8Encode('mesh-secret');

/** Whether [needle] occurs anywhere in [haystack]. */
function containsBytes(haystack: Uint8Array, needle: Uint8Array): boolean {
  return Buffer.from(haystack).includes(Buffer.from(needle));
}

let run = 0;

/**
 * A rented GPU machine: listens only, behind a policy, at a fixed name
 * (kept across reboots); serves `gpu` under whatever id it was registered
 * with, for callers presenting its key.
 */
class Worker {
  readonly index: number;
  readonly models: readonly string[];
  readonly name: string;
  readonly key: string;
  node: Switchboard | undefined;
  readonly raw: Uint8Array[] = [];
  readonly accepted: MuxConnection[] = [];

  constructor(index: number, models: readonly string[]) {
    this.index = index;
    this.models = models;
    this.name = `fleet-${run}-worker-${index}`;
    this.key = `worker-key-${index}`;
  }

  get uri(): string {
    return `mem://${this.name}`;
  }

  get id(): number {
    return 0x100 + this.index;
  }

  get metadata(): Uint8Array {
    return utf8Encode(JSON.stringify({ models: this.models, key: this.key }));
  }

  async start(): Promise<void> {
    const n = node({ talkOptions });
    this.node = n;
    n.registerService(gpu, (incoming) => this.#serve(incoming), { acceptAnyInstance: true });
    void (async () => {
      for await (const connection of n.connections) {
        this.accepted.push(connection);
      }
    })();
    await n.listenMemory({ name: this.name, policy: ChannelPolicies.allowTypes([gpu]) });
  }

  async stop(): Promise<void> {
    await this.node?.close();
    this.node = undefined;
  }

  #serve(incoming: IncomingChannel): void {
    this.raw.push(incoming.channel.openPayload);
    if (utf8Decode(incoming.address.payload) !== this.key) {
      void incoming.reject(Status.of(StatusCode.unauthenticated));
      return;
    }
    void incoming.talk().serve((message) => {
      if (!message.expectsReply) {
        return;
      }
      switch (message.procedureName) {
        case 'CAPS':
          void message.reply(JSON.stringify(this.models));
          break;
        case 'RUN':
          void message.reply(`${this.index} ran ${message.text()}`);
          break;
        default:
          void message.replyAbort(Status.of(StatusCode.unimplemented));
      }
    });
  }

  /** Tries to push a channel into the consumer on its last connection. */
  push(): Promise<Status> {
    const n = this.node as Switchboard;
    return n.openChannelOn(
      this.accepted.at(-1) as MuxConnection,
      new ChannelAddress({ type: back }),
    ).done;
  }
}

test('scaler, eight workers, consumer: online, reboot, unregister, refused push, credential isolation', async () => {
  run++;
  // Naming service: only for holders of the mesh credential.
  const namingNode = node({ talkOptions });
  const naming = new NamingService({ assignmentHold: 0 });
  onCleanup(() => naming.close());
  const namingPayloads: string[] = [];
  namingNode.registerService(
    Services.naming,
    (incoming) => {
      const payload = utf8Decode(incoming.address.payload);
      namingPayloads.push(payload);
      if (payload !== utf8Decode(meshSecret)) {
        return incoming.reject(Status.of(StatusCode.unauthenticated));
      }
      return naming.handler(incoming);
    },
    { instance: 1 },
  );
  const namingUri = await namingNode.listenMemory();
  naming.registerLocal(Services.naming, [namingUri], { instance: 1 });

  // Workers.
  const workers = Array.from(
    { length: 8 },
    (_, i) => new Worker(i, i % 2 === 0 ? ['llm'] : ['llm', 'tts']),
  );
  for (const w of workers) {
    await w.start();
    onCleanup(() => w.stop());
  }

  // The scaler registers them on its own channel.
  const scalerNode = node({ talkOptions, defaultPayload: meshSecret });
  const scaler = namingClientFor(scalerNode, namingUri, { reconnectDelay: 20, talkOptions });
  onCleanup(() => scaler.close());
  void scaler.start();
  for (const w of workers) {
    expect(await scaler.register(gpu, [w.uri], { instance: w.id, metadata: w.metadata })).toBe(
      w.id,
    );
  }

  // The consumer: trusts the naming service, nothing else; presents each
  // worker its own key from the record, the mesh credential only to the
  // naming service.
  const consumer = node({
    talkOptions,
    defaultPayload: meshSecret,
    outgoingPolicy: ChannelPolicies.denyAll,
    endpointPolicy: (endpoint) => (endpoint === namingUri ? ChannelPolicies.allowAll : undefined),
    credentialFor: (endpoint, record) => {
      if (endpoint === namingUri) {
        return meshSecret;
      }
      if (record === undefined || record.metadata.length === 0) {
        return undefined;
      }
      const meta = JSON.parse(utf8Decode(record.metadata)) as { key: string };
      return utf8Encode(meta.key);
    },
  });
  consumer.registerService(back, (incoming) => incoming.reject(Status.ok));
  const mesh = MeshNode.join(consumer, namingUri, {
    reconnectDelay: 20,
    resolveTimeout: limit,
    talkOptions,
  });
  onCleanup(() => mesh.leave());
  const capabilities = new Map<number, string[]>();
  const fleet = PeerSet.watch(consumer, gpu, {
    channel: new ChannelAddress({ type: gpu }),
    onOpen: async (peer, channel) => {
      const talk = new TalkChannel(channel, talkOptions);
      const reply = await talk.request('CAPS');
      capabilities.set(peer.address.instance, JSON.parse(reply.text()) as string[]);
    },
    initialBackoff: 20,
    maxBackoff: 200,
    removalHoldDown: 200,
  });
  onCleanup(() => fleet.close());
  const events: string[] = [];
  fleet.on('event', (e) => events.push(`${e.type} ${e.peer.address.instance - 0x100}`));
  await within(fleet.ready, limit);
  await until(() => fleet.online.length === 8, 'all eight online');
  expect([...capabilities.keys()].sort((a, b) => a - b)).toEqual(workers.map((w) => w.id));

  // Online means the capability exchange is done: every online worker has
  // its capabilities.
  const capabilitiesOf = (peer: Peer): string[] => {
    const known = capabilities.get(peer.address.instance);
    if (known === undefined) {
      throw new Error(`${peer.address} is online without its capabilities`);
    }
    return known;
  };

  // Load balancing by the application: the online workers with `tts`.
  const tts = fleet.online.filter((peer) => capabilitiesOf(peer).includes('tts'));
  expect(tts).toHaveLength(4);
  for (const peer of tts) {
    const talk = await fleet.openTalk(peer.address.instance);
    const reply = await talk.request('RUN', 'speech');
    expect(reply.text()).toBe(`${peer.address.instance - 0x100} ran speech`);
    await talk.close();
  }

  // A worker reboots: offline, and back online at the same address; it
  // stays registered meanwhile.
  const rebooted = workers[3] as Worker;
  await rebooted.stop();
  await until(() => fleet.peers.get(rebooted.id)?.isOnline === false, 'offline');
  expect(fleet.online).toHaveLength(7);
  expect(fleet.peers.size).toBe(8);
  await rebooted.start();
  await until(() => fleet.peers.get(rebooted.id)?.isOnline === true, 'back online');
  expect(events.filter((e) => e.endsWith(' 3'))).toEqual([
    'added 3',
    'online 3',
    'offline 3',
    'online 3',
  ]);

  // The scaler retires a worker: held for the hold-down (no longer
  // online), then removed, and its connection goes.
  const retired = workers[5] as Worker;
  await scaler.unregister(gpu, retired.id);
  await until(() => events.includes('held 5'), 'held');
  expect(fleet.online).toHaveLength(7);
  expect(fleet.peers.get(retired.id)?.state).toBe(PeerState.held);
  await until(() => !fleet.peers.has(retired.id), 'removed');
  await within((retired.accepted.at(-1) as MuxConnection).done, limit);
  expect(events.filter((e) => e.endsWith(' 5')).slice(2)).toEqual(['held 5', 'removed 5']);
  expect(fleet.online).toHaveLength(7);

  // A worker may not push channels into the consumer.
  const pushed = await (workers[0] as Worker).push();
  expect(codeOf(pushed)).toBe('permissionDenied');
  expect(pushed.reason).toBe('permission denied');

  // Every worker saw its own key and nothing else.
  for (const w of workers) {
    expect(w.raw.length).toBeGreaterThan(0);
    for (const open of w.raw) {
      expect(utf8Decode(ChannelAddress.decode(open).payload)).toBe(w.key);
      expect(containsBytes(open, meshSecret)).toBe(false);
    }
  }
  // The naming service saw the mesh credential, from the scaler and the
  // consumer alike, and nothing else.
  expect(namingPayloads.length).toBeGreaterThanOrEqual(2);
  expect(namingPayloads.every((p) => p === utf8Decode(meshSecret))).toBe(true);
  // Without it, the naming service is out of reach.
  const stranger = node();
  const refused = await stranger.openChannelAt(
    namingUri,
    new ChannelAddress({ type: Services.naming }),
  );
  expect(codeOf(await within(refused.done, limit))).toBe('unauthenticated');

  await fleet.close();
  expect(fleet.peers.size).toBe(0);
});
