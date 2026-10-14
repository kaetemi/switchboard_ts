/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Port of the Dart `test/integration/cohosted_mesh_test.dart`: one node
// hosting a whole small mesh: the naming service, a `_relay` and an
// application service, joined to the naming service it hosts through its
// own listener. Its own naming channel and its own channels to the
// application service go over an in-process pair, not a socket to itself.
// A worker that listens nowhere registers through it; a consumer that
// listens nowhere reaches the application service directly and the worker
// through the relay, which brokers the worker's connection through the
// naming service on the same node. Strict credentials and named IDENTs, as
// in relayed-workers.test.ts. Over TCP and WebSocket as in Dart, and over
// `mem://` (where the other nodes' channels arrive from a memory peer
// instead of a loopback socket).

import { beforeAll, describe, expect, test } from 'vitest';
import { ServiceAddress } from '../../src/core/address/index.ts';
import { Right } from '../../src/core/identity/contract.ts';
import { Ed25519HolderKey } from '../../src/core/identity/holder-key.ts';
import { Scope } from '../../src/core/identity/scope.ts';
import { Name } from '../../src/core/name.ts';
import { Services } from '../../src/core/naming/naming-protocol.ts';
import { NamingService } from '../../src/core/naming/naming-service.ts';
import { ChannelPolicies } from '../../src/core/switchboard/channel-policy.ts';
import type { IncomingChannel } from '../../src/core/switchboard/contract.ts';
import { MeshNode } from '../../src/core/switchboard/mesh-node.ts';
import { RelayService } from '../../src/core/switchboard/relay.ts';
import { RelayConfig } from '../../src/core/switchboard/relay-config.ts';
import type { TalkOptions } from '../../src/core/talk/contract.ts';
import { Authority, limit, listen, seed } from './mesh-support.ts';
import { node, onCleanup, type Switchboard, within } from './node-support.ts';

const talkOptions: Partial<TalkOptions> = { requestTimeout: 3000, replyTimeout: 2000 };

const naming = Services.naming;
const app = Name.of('app');
const workerType = Name.of('worker-g');

let authority: Authority;
let hubKey: Ed25519HolderKey;
let workerKey: Ed25519HolderKey;
let consumerKey: Ed25519HolderKey;

beforeAll(async () => {
  authority = await Authority.fromSeed(seed((i) => 13 * i + 5));
  hubKey = await Ed25519HolderKey.generate();
  workerKey = await Ed25519HolderKey.generate();
  consumerKey = await Ed25519HolderKey.generate();
});

/** One request on a fresh Talk channel from [from] to [to]. */
async function call(from: Switchboard, to: ServiceAddress, job: string): Promise<string> {
  const talk = await from.openTalk(to);
  try {
    return (await talk.request('RUN', job)).text();
  } finally {
    await talk.close();
  }
}

/** A channel the hub received: who opened it, and over what. */
interface Arrival {
  readonly type: string;
  readonly identity: string | undefined;
  readonly remote: string | undefined;
}

for (const scheme of ['tcp', 'ws', 'mem'] as const) {
  describe(`over ${scheme}`, () => {
    test(`one node hosting naming, a relay and a service, joined to itself in-process (${scheme})`, async () => {
      // The hub. It names itself when it identifies to its own listener,
      // and requires named IDENTs, as a relay should.
      let hubUri = '';
      const hub = node({
        muxOptions: { requireNamedIdent: true },
        talkOptions,
        credential: await authority.issue('hub', hubKey, [
          Scope.of(Right.register, '_relay'),
          Scope.of(Right.register, 'app'),
          Scope.of(Right.watch, '*'),
          Scope.of(Right.open, 'app'),
          Scope.of(Right.open, 'worker-*'),
          Scope.of(Right.broker, 'worker-*'),
        ]),
        holderKey: hubKey,
        verifier: await authority.verifier(),
        expectedIdentityFor: (endpoint) => (endpoint === hubUri ? 'hub' : undefined),
      });
      const arrivals: Arrival[] = [];
      const arrived = (incoming: IncomingChannel): void => {
        arrivals.push({
          type: `${incoming.address.type}`,
          identity: incoming.peerIdentity?.identity,
          remote: incoming.remote,
        });
      };

      const service = new NamingService({
        assignmentHold: 0,
        verifier: await authority.verifier(),
        requireCredential: true,
      });
      onCleanup(() => service.close());
      hub.registerService(
        naming,
        (incoming) => {
          arrived(incoming);
          return service.handler(incoming);
        },
        { instance: 1 },
      );
      hubUri = await listen(hub, scheme, { policy: ChannelPolicies.scoped() });
      service.registerLocal(naming, [hubUri], { instance: 1 });
      expect(hub.isOwnEndpoint(hubUri)).toBe(true);

      // The hub joins the naming service it hosts, through its own
      // listener, and publishes the relay and the application service.
      const hubMesh = MeshNode.join(hub, hubUri, { talkOptions });
      onCleanup(() => hubMesh.leave());
      await within(hubMesh.synced, limit);
      await within(hubMesh.publishRelay(new RelayService(hub)), limit);
      const appId = await within(
        hubMesh.publish(app, (incoming) => {
          arrived(incoming);
          void incoming.talk().serve((message) => {
            void message.reply(`${message.text()} for ${incoming.peerIdentity?.identity}`);
          });
        }),
        limit,
      );
      expect(service.table.get(new ServiceAddress(app, appId).key)?.endpoints).toEqual([hubUri]);

      // A worker that listens nowhere registers through the hub.
      const worker = node({
        talkOptions,
        credential: await authority.issue('worker-a1', workerKey, [
          Scope.of(Right.register, 'worker-*'),
        ]),
        holderKey: workerKey,
        verifier: await authority.verifier(),
        expectedIdentityFor: (endpoint) => (endpoint === hubUri ? 'hub' : undefined),
      });
      const workerMesh = MeshNode.join(worker, hubUri, { talkOptions, watch: false });
      onCleanup(() => workerMesh.leave());
      const workerId = await within(
        workerMesh.publish(
          workerType,
          (incoming) => {
            const via = incoming.peerIdentity?.identity;
            void incoming.talk().serve((message) => {
              void message.reply(`${message.text()} done via ${via}`);
            });
          },
          { endpoints: [] },
        ),
        limit,
      );
      expect(service.table.get(new ServiceAddress(workerType, workerId).key)?.endpoints).toEqual(
        [],
      );

      // A consumer that listens nowhere either: the service directly, the
      // worker through the hub's relay.
      const consumer = node({
        talkOptions,
        credential: await authority.issue('consumer-1', consumerKey, [
          Scope.of(Right.watch, '*'),
          Scope.of(Right.open, 'app'),
          Scope.of(Right.open, 'worker-*'),
        ]),
        holderKey: consumerKey,
        verifier: await authority.verifier(),
        outgoingPolicy: ChannelPolicies.denyAll,
        expectedIdentityFor: (endpoint) => (endpoint === hubUri ? 'hub' : undefined),
        relay: new RelayConfig(),
      });
      const consumerMesh = MeshNode.join(consumer, hubUri, { talkOptions });
      onCleanup(() => consumerMesh.leave());
      await within(consumerMesh.synced, limit);
      expect(consumer.canBroker).toBe(false);

      expect(await within(call(consumer, new ServiceAddress(app), 'ping'), limit)).toBe(
        'ping for consumer-1',
      );
      expect(
        await within(call(consumer, new ServiceAddress(workerType, workerId), 'job 1'), limit),
      ).toBe('job 1 done via hub');
      expect(workerMesh.brokeredConnections).toHaveLength(1);
      expect(workerMesh.brokeredConnections[0]?.peerIdentity?.identity).toBe('hub');
      // The hub reaches its own service too, on the same in-process
      // connection as its naming channel.
      expect(await within(call(hub, new ServiceAddress(app), 'self'), limit)).toBe('self for hub');

      // Every channel the hub opened to itself came over the in-process
      // pair: no socket from the hub to its own listener. The others came
      // over sockets (from memory peers over mem).
      const own = arrivals.filter((a) => a.identity === 'hub');
      expect(own.map((a) => a.type).sort()).toEqual([`${naming}`, `${app}`].sort());
      expect(own.every((a) => a.remote === 'local')).toBe(true);
      const others = arrivals.filter((a) => a.identity !== 'hub');
      expect(others.map((a) => `${a.type} ${a.identity}`).sort()).toEqual(
        [`${naming} worker-a1`, `${naming} consumer-1`, `${app} consumer-1`].sort(),
      );
      expect(others.every((a) => a.remote !== 'local')).toBe(true);
      if (scheme === 'mem') {
        expect(others.every((a) => a.remote === 'memory peer')).toBe(true);
      } else {
        expect(others.every((a) => a.remote?.startsWith('127.0.0.1:'))).toBe(true);
      }
    });
  });
}
