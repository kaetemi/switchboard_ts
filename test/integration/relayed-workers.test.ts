/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Port of the Dart `test/integration/relayed_workers_test.dart`:
// outbound-only on both ends (wiki page "Polyverse Switchboard Use Cases",
// entry 5c). A naming service that requires credentials and named IDENTs;
// two relay nodes that listen, require named IDENTs and publish `_relay`
// with their identities; a worker that listens nowhere and holds
// `register worker-*` only; two consumers that listen nowhere either, keep
// a PeerSet over the workers through the relays and run jobs with
// openTalk. Strict credentials for everyone. The consumers come online
// through one relay, which brokers one connection to the worker for both;
// a consumer without the `open` scope is refused by the relay; the relay in
// use stops and the consumers come back through the other one; the worker
// restarts and is brokered again. Over TCP as in Dart, and over `mem://`.

import { beforeAll, describe, expect, test } from 'vitest';
import { ChannelAddress, ServiceAddress } from '../../src/core/address/index.ts';
import { utf8Decode } from '../../src/core/bytes.ts';
import { Right } from '../../src/core/identity/contract.ts';
import { Ed25519HolderKey } from '../../src/core/identity/holder-key.ts';
import { Scope } from '../../src/core/identity/scope.ts';
import { Name } from '../../src/core/name.ts';
import { Services } from '../../src/core/naming/naming-protocol.ts';
import { NamingService } from '../../src/core/naming/naming-service.ts';
import { ChannelPolicies } from '../../src/core/switchboard/channel-policy.ts';
import type { ExpectedIdentity } from '../../src/core/switchboard/contract.ts';
import { MeshNode } from '../../src/core/switchboard/mesh-node.ts';
import { PeerSet } from '../../src/core/switchboard/peer-set.ts';
import { RelayService } from '../../src/core/switchboard/relay.ts';
import { RelayConfig } from '../../src/core/switchboard/relay-config.ts';
import type { TalkOptions } from '../../src/core/talk/contract.ts';
import { TalkChannel } from '../../src/core/talk/index.ts';
import { Authority, codeOf, limit, listen, type Scheme, seed, until } from './mesh-support.ts';
import { node, onCleanup, type Switchboard, within } from './node-support.ts';

const talkOptions: Partial<TalkOptions> = { requestTimeout: 3000, replyTimeout: 2000 };

const naming = Services.naming;
const workerType = Name.of('worker-g');

let authority: Authority;
let nsKey: Ed25519HolderKey;
let workerKey: Ed25519HolderKey;
let relayKey: Ed25519HolderKey;
let consumerKey: Ed25519HolderKey;

beforeAll(async () => {
  authority = await Authority.fromSeed(seed((i) => 17 * i + 1));
  nsKey = await Ed25519HolderKey.generate();
  workerKey = await Ed25519HolderKey.generate();
  relayKey = await Ed25519HolderKey.generate();
  consumerKey = await Ed25519HolderKey.generate();
});

/** Names the naming service when identifying to it; the relays are named from their records. */
function namesTheNamingService(namingEndpoint: string): ExpectedIdentity {
  return (endpoint) => (endpoint === namingEndpoint ? 'ns' : undefined);
}

/** A relay node. */
class Relay {
  readonly node: Switchboard;
  readonly mesh: MeshNode;
  readonly uri: string;

  private constructor(node: Switchboard, mesh: MeshNode, uri: string) {
    this.node = node;
    this.mesh = mesh;
    this.uri = uri;
  }

  static async start(namingEndpoint: string, scheme: Scheme, identity: string): Promise<Relay> {
    const n = node({
      muxOptions: { requireNamedIdent: true },
      talkOptions,
      credential: await authority.issue(identity, relayKey, [
        Scope.of(Right.register, '_relay'),
        Scope.of(Right.watch, '*'),
        Scope.of(Right.open, 'worker-*'),
        Scope.of(Right.broker, 'worker-*'),
      ]),
      holderKey: relayKey,
      verifier: await authority.verifier(),
      expectedIdentityFor: namesTheNamingService(namingEndpoint),
    });
    const uri = await listen(n, scheme, { policy: ChannelPolicies.scoped() });
    const mesh = MeshNode.join(n, namingEndpoint, { talkOptions });
    onCleanup(() => mesh.leave());
    await within(mesh.synced, limit);
    await within(mesh.publishRelay(new RelayService(n)), limit);
    return new Relay(n, mesh, uri);
  }

  async stop(): Promise<void> {
    await this.mesh.leave();
    await this.node.close();
  }
}

/** The worker: no listener, `register worker-*` only, answering jobs with the identity it sees (the relay's). */
class Worker {
  readonly node: Switchboard;
  readonly mesh: MeshNode;
  readonly id: number;

  private constructor(node: Switchboard, mesh: MeshNode, id: number) {
    this.node = node;
    this.mesh = mesh;
    this.id = id;
  }

  static async start(namingEndpoint: string, instance = 0): Promise<Worker> {
    const n = node({
      talkOptions,
      credential: await authority.issue('worker-a1', workerKey, [
        Scope.of(Right.register, 'worker-*'),
      ]),
      holderKey: workerKey,
      verifier: await authority.verifier(),
      expectedIdentityFor: namesTheNamingService(namingEndpoint),
    });
    const mesh = MeshNode.join(n, namingEndpoint, { talkOptions, watch: false });
    onCleanup(() => mesh.leave());
    const id = await within(
      mesh.publish(
        workerType,
        (incoming) => {
          const via = incoming.peerIdentity?.identity;
          void incoming.talk().serve((message) => {
            if (message.expectsReply) {
              void message.reply(`${message.text()} done via ${via}`);
            }
          });
        },
        { endpoints: [], instance },
      ),
      limit,
    );
    expect(n.listeningEndpoints).toEqual([]);
    return new Worker(n, mesh, id);
  }

  async stop(): Promise<void> {
    await this.mesh.leave();
    await this.node.close();
  }
}

/** A consumer: no listener, `open` and `watch`, relays found through the naming service, a PeerSet over the workers. */
class Consumer {
  readonly node: Switchboard;
  readonly set: PeerSet;
  readonly events: string[];

  private constructor(node: Switchboard, set: PeerSet, events: string[]) {
    this.node = node;
    this.set = set;
    this.events = events;
  }

  static async start(namingEndpoint: string, identity: string): Promise<Consumer> {
    const n = node({
      talkOptions,
      credential: await authority.issue(identity, consumerKey, [
        Scope.of(Right.watch, '*'),
        Scope.of(Right.open, 'worker-*'),
      ]),
      holderKey: consumerKey,
      verifier: await authority.verifier(),
      outgoingPolicy: ChannelPolicies.denyAll,
      expectedIdentityFor: namesTheNamingService(namingEndpoint),
      relay: new RelayConfig(),
    });
    const mesh = MeshNode.join(n, namingEndpoint, { talkOptions });
    onCleanup(() => mesh.leave());
    await within(mesh.synced, limit);
    expect(n.canBroker).toBe(false);
    const set = PeerSet.watch(n, workerType, {
      channel: new ChannelAddress({ type: workerType }),
      // A round trip on the per-peer channel: through a relay, only that
      // says the channel reached the worker.
      onOpen: async (_peer, channel) => {
        await new TalkChannel(channel, talkOptions).request('HELLO');
      },
      initialBackoff: 50,
      maxBackoff: 500,
      removalHoldDown: 5000,
    });
    onCleanup(() => set.close());
    const events: string[] = [];
    set.on('event', (e) => events.push(`${e.type} ${e.peer.address}`));
    return new Consumer(n, set, events);
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
}

for (const scheme of ['tcp', 'mem'] as const) {
  describe(`over ${scheme}`, () => {
    test('consumers reach an outbound-only worker through relays', async () => {
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
      });
      onCleanup(() => service.close());
      host.registerService(naming, service.handler, { instance: 1 });
      const namingEndpoint = await listen(host, scheme, { policy: ChannelPolicies.scoped() });
      service.registerLocal(naming, [namingEndpoint], { instance: 1 });

      const relayA = await Relay.start(namingEndpoint, scheme, 'relay-a');
      const relayB = await Relay.start(namingEndpoint, scheme, 'relay-b');
      const relays = [...service.table.values()]
        .filter((record) => record.address.type.equals(Services.relay))
        .sort((x, y) => x.address.instance - y.address.instance);
      expect(relays.map((r) => utf8Decode(r.metadata))).toEqual(['relay-a', 'relay-b']);
      expect(relays[0]?.endpoints).toEqual([relayA.uri]);

      let worker = await Worker.start(namingEndpoint);
      const id = worker.id;
      expect(service.table.get(new ServiceAddress(workerType, id).key)?.endpoints).toEqual([]);

      // Both consumers come online through the first relay, which brokers
      // one connection to the worker for both.
      const one = await Consumer.start(namingEndpoint, 'consumer-1');
      const two = await Consumer.start(namingEndpoint, 'consumer-2');
      await until(
        () => one.set.online.length > 0 && two.set.online.length > 0,
        'the worker online for both consumers',
      );
      const peerOne = one.set.peers.get(id);
      const peerTwo = two.set.peers.get(id);
      if (peerOne === undefined || peerTwo === undefined) throw new Error('no peer');
      for (const peer of [peerOne, peerTwo]) {
        expect(peer.viaRelay).toBe(true);
        expect(peer.endpoint).toBe(relayA.uri);
      }
      expect(await within(one.run(id, 'job 1'), limit)).toBe('job 1 done via relay-a');
      expect(await within(two.run(id, 'job 2'), limit)).toBe('job 2 done via relay-a');
      expect(worker.mesh.brokeredConnections).toHaveLength(1);
      const brokered = worker.mesh.brokeredConnections[0];
      expect(brokered?.peerIdentity?.identity).toBe('relay-a');
      expect(brokered?.peerIdentity?.receiver).toBe('worker-a1');

      // A consumer without `open` for the worker's type is refused by the
      // relay.
      const stranger = node({
        talkOptions,
        credential: await authority.issue('consumer-3', consumerKey, [Scope.of(Right.watch, '*')]),
        holderKey: consumerKey,
        verifier: await authority.verifier(),
        expectedIdentityFor: namesTheNamingService(namingEndpoint),
        relay: new RelayConfig(),
      });
      const strangerMesh = MeshNode.join(stranger, namingEndpoint, { talkOptions });
      onCleanup(() => strangerMesh.leave());
      await within(strangerMesh.synced, limit);
      const refused = await within(stranger.openChannel(new ServiceAddress(workerType, id)), limit);
      expect(codeOf(await within(refused.done, limit))).toBe('permissionDenied');

      // The relay in use stops: both consumers come back through the other.
      await relayA.stop();
      await until(
        () =>
          peerOne.isOnline &&
          peerOne.endpoint === relayB.uri &&
          peerTwo.isOnline &&
          peerTwo.endpoint === relayB.uri,
        'back through relay-b',
      );
      expect(await within(one.run(id, 'job 3'), limit)).toBe('job 3 done via relay-b');
      expect(await within(two.run(id, 'job 4'), limit)).toBe('job 4 done via relay-b');
      await until(
        () => worker.mesh.brokeredConnections.length === 1,
        'one brokered connection, to relay-b',
      );
      expect(worker.mesh.brokeredConnections[0]?.peerIdentity?.identity).toBe('relay-b');

      // The worker restarts under its id: brokered again, online again.
      await worker.stop();
      await until(() => !peerOne.isOnline && !peerTwo.isOnline, 'the worker offline');
      worker = await Worker.start(namingEndpoint, id);
      expect(worker.id).toBe(id);
      await until(() => peerOne.isOnline && peerTwo.isOnline, 'the worker online again');
      expect(one.set.peers.get(id)).toBe(peerOne);
      expect(await within(one.run(id, 'job 5'), limit)).toBe('job 5 done via relay-b');
      expect(await within(two.run(id, 'job 6'), limit)).toBe('job 6 done via relay-b');
      expect(worker.mesh.brokeredConnections).toHaveLength(1);
      for (const consumer of [one, two]) {
        expect(consumer.events[0]).toBe(`added ${new ServiceAddress(workerType, id)}`);
        expect(consumer.events.filter((e) => e.startsWith('removed'))).toEqual([]);
      }
    });
  });
}
