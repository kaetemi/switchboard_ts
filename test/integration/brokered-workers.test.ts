/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Port of the Dart `test/integration/brokered_workers_test.dart`:
// outbound-only trusted workers (wiki page "Polyverse Switchboard Use
// Cases", entry 5b). A naming service that requires credentials, and named
// IDENTs on its listener; a worker that listens nowhere, dials the naming
// service, and holds a credential with `register` only; a consumer with
// `watch`, `open` and `broker` that listens behind a scoped policy and
// keeps a PeerSet over the workers. The consumer reaches the worker through
// CONNECT: request and reply work, the worker can open nothing on the
// consumer, a lost connection is brokered again, a consumer restart (a new
// listener) is brokered anew, and the worker going away takes the peer down
// and then out of the set. Over TCP as in Dart, and over `mem://`.
//
// The relay variant, for a consumer that listens nowhere either, is in
// relayed-workers.test.ts.

import { beforeAll, describe, expect, test } from 'vitest';
import { ChannelAddress, ServiceAddress } from '../../src/core/address/index.ts';
import { Right } from '../../src/core/identity/contract.ts';
import { Ed25519HolderKey } from '../../src/core/identity/holder-key.ts';
import { Scope } from '../../src/core/identity/scope.ts';
import type { MuxConnection } from '../../src/core/mux/contract.ts';
import { Name } from '../../src/core/name.ts';
import { Services } from '../../src/core/naming/naming-protocol.ts';
import { NamingService } from '../../src/core/naming/naming-service.ts';
import { ChannelPolicies } from '../../src/core/switchboard/channel-policy.ts';
import { MeshNode } from '../../src/core/switchboard/mesh-node.ts';
import { PeerSet } from '../../src/core/switchboard/peer-set.ts';
import type { TalkOptions } from '../../src/core/talk/contract.ts';
import { Authority, codeOf, limit, listen, type Scheme, seed, until } from './mesh-support.ts';
import { node, onCleanup, type Switchboard, within } from './node-support.ts';

const talkOptions: Partial<TalkOptions> = { requestTimeout: 3000, replyTimeout: 2000 };

const naming = Services.naming;
const workerType = Name.of('worker-g');
const consumerType = Name.of('consumer');

let authority: Authority;
let nsKey: Ed25519HolderKey;
let workerKey: Ed25519HolderKey;
let consumerKey: Ed25519HolderKey;

beforeAll(async () => {
  authority = await Authority.fromSeed(seed((i) => 13 * i + 5));
  nsKey = await Ed25519HolderKey.generate();
  workerKey = await Ed25519HolderKey.generate();
  consumerKey = await Ed25519HolderKey.generate();
});

/** The consumer: a node listening, joined to the mesh, with a PeerSet over the workers. */
class Consumer {
  readonly node: Switchboard;
  readonly mesh: MeshNode;
  readonly set: PeerSet;
  readonly events: string[];

  private constructor(node: Switchboard, mesh: MeshNode, set: PeerSet, events: string[]) {
    this.node = node;
    this.mesh = mesh;
    this.set = set;
    this.events = events;
  }

  static async start(namingEndpoint: string, scheme: Scheme): Promise<Consumer> {
    const n = node({
      talkOptions,
      credential: await authority.issue('consumer-1', consumerKey, [
        Scope.of(Right.watch, '*'),
        Scope.of(Right.open, 'worker-*'),
        Scope.of(Right.broker, 'worker-*'),
      ]),
      holderKey: consumerKey,
      verifier: await authority.verifier(),
      outgoingPolicy: ChannelPolicies.denyAll,
      expectedIdentityFor: (endpoint) => (endpoint === namingEndpoint ? 'ns' : undefined),
    });
    // Brokered workers dial this listener; its policy is theirs.
    await listen(n, scheme, { policy: ChannelPolicies.scoped() });
    const mesh = MeshNode.join(n, namingEndpoint, { talkOptions });
    onCleanup(() => mesh.leave());
    await within(mesh.synced, limit);
    const set = PeerSet.watch(n, workerType, {
      channel: new ChannelAddress({ type: workerType }),
      initialBackoff: 50,
      maxBackoff: 500,
      removalHoldDown: 300,
    });
    onCleanup(() => set.close());
    const events: string[] = [];
    set.on('event', (e) => events.push(`${e.type} ${e.peer.address}`));
    return new Consumer(n, mesh, set, events);
  }

  /** Runs a job on the worker [id] over a fresh channel of the set. */
  async run(id: number, job: string): Promise<string> {
    const channel = await this.set.openTalk(id);
    try {
      return (await channel.request('RUN', job)).text();
    } finally {
      await channel.close();
    }
  }

  async stop(): Promise<void> {
    await this.set.close();
    await this.mesh.leave();
    await this.node.close();
  }
}

for (const scheme of ['tcp', 'mem'] as const) {
  describe(`over ${scheme}`, () => {
    test('a consumer reaches an outbound-only worker through CONNECT', async () => {
      // The naming service: credentials required, and IDENTs must name it.
      const host = node({
        muxOptions: { requireNamedIdent: true },
        talkOptions,
        credential: await authority.issue('ns', nsKey, []),
        holderKey: nsKey,
        verifier: await authority.verifier(),
      });
      const service = new NamingService({
        assignmentHold: 0,
        verifier: await authority.verifier(),
        requireCredential: true,
        issuer: authority.issuer,
      });
      onCleanup(() => service.close());
      host.registerService(naming, service.handler, { instance: 1 });
      const namingEndpoint = await listen(host, scheme, { policy: ChannelPolicies.scoped() });
      service.registerLocal(naming, [namingEndpoint], { instance: 1 });

      // The worker: no listener, register only.
      const worker = node({
        talkOptions,
        credential: await authority.issue('worker-a1', workerKey, [
          Scope.of(Right.register, 'worker-*'),
        ]),
        holderKey: workerKey,
        verifier: await authority.verifier(),
        expectedIdentityFor: (endpoint) => (endpoint === namingEndpoint ? 'ns' : undefined),
      });
      const workerMesh = MeshNode.join(worker, namingEndpoint, { talkOptions, watch: false });
      onCleanup(() => workerMesh.leave());
      const id = await within(
        workerMesh.publish(
          workerType,
          (incoming) => {
            const caller = incoming.peerIdentity?.identity;
            void incoming.talk().serve((message) => {
              if (message.expectsReply) {
                void message.reply(`${message.text()} done for ${caller}`);
              }
            });
          },
          { endpoints: [] },
        ),
        limit,
      );
      expect(worker.listeningEndpoints).toEqual([]);
      expect(service.table.get(new ServiceAddress(workerType, id).key)?.endpoints).toEqual([]);

      // The consumer brokers a connection and talks to the worker.
      let consumer = await Consumer.start(namingEndpoint, scheme);
      await until(() => consumer.set.online.length > 0, 'worker online');
      const peer = consumer.set.peers.get(id);
      if (peer === undefined) throw new Error('no peer');
      expect(peer.endpoint).toBeUndefined();
      expect(peer.connection?.peerIdentity?.identity).toBe('worker-a1');
      expect(peer.connection?.peerIdentity?.receiver).toBe('consumer-1');
      expect(await within(consumer.run(id, 'job 1'), limit)).toBe('job 1 done for consumer-1');
      expect(workerMesh.brokeredConnections).toHaveLength(1);
      const brokered = workerMesh.brokeredConnections[0] as MuxConnection;
      expect(brokered.peerIdentity?.identity).toBe('consumer-1');

      // The worker can open nothing on the consumer.
      const push = worker.openChannelOn(brokered, new ChannelAddress({ type: consumerType }));
      expect(codeOf(await within(push.done, limit))).toBe('permissionDenied');
      const pushWorker = worker.openChannelOn(brokered, new ChannelAddress({ type: workerType }));
      expect(codeOf(await within(pushWorker.done, limit))).toBe('permissionDenied');

      // A lost connection is brokered again.
      const first = peer.connection as MuxConnection;
      await first.close();
      await until(() => peer.isOnline && peer.connection !== first, 'brokered again');
      expect(await within(consumer.run(id, 'job 2'), limit)).toBe('job 2 done for consumer-1');
      await until(
        () => workerMesh.brokeredConnections.length === 1,
        'one brokered connection at the worker',
      );

      // The consumer restarts with a new listener: brokered anew.
      const oldListener = consumer.node.listeningEndpoints[0];
      await consumer.stop();
      await until(() => workerMesh.brokeredConnections.length === 0, 'the worker left alone');
      consumer = await Consumer.start(namingEndpoint, scheme);
      expect(consumer.node.listeningEndpoints[0]).not.toBe(oldListener);
      await until(() => consumer.set.online.length > 0, 'online again');
      expect(await within(consumer.run(id, 'job 3'), limit)).toBe('job 3 done for consumer-1');

      // The worker goes away: offline, held, then removed.
      const address = `${new ServiceAddress(workerType, id)}`;
      await workerMesh.leave();
      await worker.close();
      const events = consumer.events;
      await until(() => events.includes(`removed ${address}`), 'worker removed');
      expect(events.slice(0, 2)).toEqual([`added ${address}`, `online ${address}`]);
      expect(events.at(-1)).toBe(`removed ${address}`);
      expect(events).toContain(`held ${address}`);
      expect(
        events.slice(2, -1).every((e) => e.startsWith('offline') || e.startsWith('held')),
      ).toBe(true);
      expect(consumer.set.peers.size).toBe(0);
    });
  });
}
