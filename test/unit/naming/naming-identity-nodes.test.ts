/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// The node-side cases of the Dart `naming_identity_test.dart`: in group
// "scopes", a host that requires a named IDENT; in group "CONNECT",
// brokered connections to an outbound-only worker, the GOAWAY of a
// dial-back nobody waits for, `Switchboard.broker`'s preconditions and a
// PeerSet that brokers. `naming-identity.test.ts` ports the rest of the file
// over in-memory Talk channels with manual connections; these need what only
// the node does (the IDENT exchange on real connections, dialling back), so
// they run as Dart's do: over nodes on `mem` listeners with real
// credentials (Ed25519 and HMAC issuers, holder keys, verifiers), over the
// toy mux and, from `over-frame-mux/`, over the frame mux (the late
// dial-back cases there only). The nodes join the naming service with
// `MeshNode.join`, as in Dart.

import { beforeAll, describe, expect, test } from 'vitest';
import { ChannelAddress, ServiceAddress } from '../../../src/core/address/index.ts';
import {
  type Credential,
  CredentialKind,
  Right,
  type Scope,
} from '../../../src/core/identity/contract.ts';
import { CredentialIssuer } from '../../../src/core/identity/credential-issuer.ts';
import { CredentialVerifier } from '../../../src/core/identity/credential-verifier.ts';
import { Ed25519HolderKey } from '../../../src/core/identity/holder-key.ts';
import { Scope as Scopes } from '../../../src/core/identity/scope.ts';
import type { MuxConnection } from '../../../src/core/mux/contract.ts';
import { Name } from '../../../src/core/name.ts';
import type { ConnectHandler } from '../../../src/core/naming/naming-client.ts';
import { Services } from '../../../src/core/naming/naming-protocol.ts';
import { NamingService } from '../../../src/core/naming/naming-service.ts';
import {
  type Status,
  StatusCode,
  SwitchboardError,
  statusCodeName,
} from '../../../src/core/status.ts';
import { ChannelPolicies } from '../../../src/core/switchboard/channel-policy.ts';
import { MeshNode, type MeshNodeOptions } from '../../../src/core/switchboard/mesh-node.ts';
import { type PeerEvent, PeerSet } from '../../../src/core/switchboard/peer-set.ts';
import type { Switchboard } from '../../../src/core/switchboard/switchboard.ts';
import type { TalkOptions } from '../../../src/core/talk/contract.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';
import {
  node as harnessNode,
  onCleanup,
  overFrameMux,
  rejectsWith,
} from '../../support/node/harness.ts';

const talk: Partial<TalkOptions> = { requestTimeout: 2000, replyTimeout: 1000 };
const limit = 5000;
const hour = 3_600_000;

const naming = Services.naming;
const workerType = Name.of('worker-g');

const authoritySeed = Uint8Array.from({ length: 32 }, (_, i) => (11 * i) & 0xff);
const meshKey = Uint8Array.from({ length: 32 }, (_, i) => 200 - i);

let authority: CredentialIssuer;
let hmac: CredentialIssuer;
let key: Ed25519HolderKey;

const workerScopes: Scope[] = [Scopes.of(Right.register, 'worker-*')];
const consumerScopes: Scope[] = [
  Scopes.of(Right.watch, '*'),
  Scopes.of(Right.open, 'worker-*'),
  Scopes.of(Right.broker, 'worker-*'),
];

beforeAll(async () => {
  authority = await CredentialIssuer.ed25519FromSeed('auth1', authoritySeed);
  hmac = CredentialIssuer.hmac('mesh1', meshKey);
  key = await Ed25519HolderKey.generate();
});

function issue(identity: string, scopes: readonly Scope[]): Promise<Credential> {
  return authority.issue({
    kind: CredentialKind.node,
    identity,
    scopes,
    holderKey: key.publicKey,
    lifetime: 8 * hour,
  });
}

async function verifier(): Promise<CredentialVerifier> {
  const v = new CredentialVerifier();
  await v.addIssuer(authority);
  await v.addIssuer(hmac);
  return v;
}

function expectCode(status: Status, code: StatusCode): void {
  expect(statusCodeName(status.code)).toBe(statusCodeName(code));
}

/** Waits until [condition] holds; fails after [limit] milliseconds. */
async function until(condition: () => boolean): Promise<void> {
  const deadline = performance.now() + limit;
  while (!condition()) {
    if (performance.now() > deadline) {
      throw new Error(`condition not met within ${limit} ms`);
    }
    await new Promise((resolve) => setTimeout(resolve, 2));
  }
}

/** A node with [credential] (identifying with the holder key) and a verifier, closed after the test. */
async function node(
  options: { credential?: Credential; verify?: boolean; connectTimeout?: number } = {},
): Promise<Switchboard> {
  const credential = options.credential;
  return harnessNode({
    talkOptions: talk,
    credential,
    holderKey: credential === undefined || credential.isBearer ? undefined : key,
    verifier: (options.verify ?? true) ? await verifier() : undefined,
    connectTimeout: options.connectTimeout ?? 10_000,
  });
}

/** A naming service hosted on a node listening on a `mem` endpoint. */
class Mesh {
  readonly host: Switchboard;
  readonly service: NamingService;
  readonly uri: string;

  private constructor(host: Switchboard, service: NamingService, uri: string) {
    this.host = host;
    this.service = service;
    this.uri = uri;
  }

  static async start(options: { brokerTimeout?: number } = {}): Promise<Mesh> {
    const host = await node();
    const service = new NamingService({
      assignmentHold: 0,
      holdingSettle: 0,
      verifier: await verifier(),
      requireCredential: true,
      issuer: authority,
      brokerTimeout: options.brokerTimeout ?? 5000,
    });
    onCleanup(() => service.close());
    host.registerService(naming, service.handler, { instance: 1 });
    // The naming service checks identities itself, the payload's too.
    const uri = await host.listenMemory({ policy: ChannelPolicies.allowTypes([naming]) });
    return new Mesh(host, service, uri);
  }
}

/** [sb] joined to the naming service at [uri] with `MeshNode.join`, left after the test. */
function join(sb: Switchboard, uri: string, options: MeshNodeOptions = {}): MeshNode {
  const mesh = MeshNode.join(sb, uri, options);
  onCleanup(() => mesh.leave());
  return mesh;
}

/** No endpoints: the instances of these tests are outbound only. */
const outbound = { endpoints: [] };

describe('scopes', () => {
  test('a node whose IDENT names the naming service is accepted by a host that requires it; MeshNode works under the scopes', async () => {
    const host = harnessNode({
      muxOptions: { requireNamedIdent: true },
      talkOptions: talk,
      credential: await issue('ns', []),
      holderKey: key,
      verifier: await verifier(),
    });
    const service = new NamingService({
      assignmentHold: 0,
      verifier: await verifier(),
      requireCredential: true,
    });
    onCleanup(() => service.close());
    host.registerService(naming, service.handler, { instance: 1 });
    const uri = await host.listenMemory({ policy: ChannelPolicies.scoped() });
    const worker = harnessNode({
      talkOptions: talk,
      credential: await issue('worker-a1', workerScopes),
      holderKey: key,
      expectedIdentityFor: (endpoint) => (endpoint === uri ? 'ns' : undefined),
    });
    const mesh = join(worker, uri, { watch: false, renewCredential: false });
    const id = await mesh.publish(workerType, () => {}, outbound);
    expect([...service.table.values()].map((r) => r.address.toString())).toEqual([
      new ServiceAddress(workerType, id).toString(),
    ]);
    expect(mesh.client.isSynced).toBe(false);
    // A client that watches anyway is refused once, and keeps its channel
    // and its registrations.
    const watching = join(worker, uri, { renewCredential: false });
    await watching.publish(Name.of('worker-h'), () => {}, outbound);
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(watching.client.isConnected).toBe(true);
    expect(service.table.size).toBe(2);
    // Another node that names nobody is refused by the host.
    const unnamed = await node({ credential: await issue('consumer-1', consumerScopes) });
    await rejectsWith(unnamed.connect(uri), StatusCode.unauthenticated);
  });
});

describe('CONNECT', () => {
  /** A mesh, an outbound-only worker registered without endpoints, and a consumer listening with a scoped policy. */
  async function setUp(): Promise<{
    worker: Switchboard;
    workerMesh: MeshNode;
    id: number;
    consumer: Switchboard;
  }> {
    const mesh = await Mesh.start();
    const worker = await node({ credential: await issue('worker-a1', workerScopes) });
    const workerMesh = join(worker, mesh.uri, { watch: false });
    const id = await workerMesh.publish(
      workerType,
      (incoming) => {
        void incoming.talk().serve((message) => {
          if (message.expectsReply) {
            void message.reply(`${message.text()} by ${incoming.peerIdentity?.identity}`);
          }
        });
      },
      outbound,
    );
    const consumer = await node({ credential: await issue('consumer-1', consumerScopes) });
    await consumer.listenMemory({ policy: ChannelPolicies.scoped() });
    const consumerMesh = join(consumer, mesh.uri);
    await consumerMesh.client.synced;
    return { worker, workerMesh, id, consumer };
  }

  test('the consumer opens a channel to an outbound-only worker', async () => {
    const { worker, workerMesh, id, consumer } = await setUp();
    const address = new ServiceAddress(workerType, id);
    const channel = await consumer.openTalk(address);
    const reply = await channel.request('RUN', 'job');
    expect(reply.text()).toBe('job by consumer-1');
    // The connection is pooled per instance.
    const again = await consumer.openTalk(address);
    expect((await again.request('RUN', 'two')).text()).toBe('two by consumer-1');
    expect(workerMesh.brokeredConnections).toHaveLength(1);
    const brokered = workerMesh.brokeredConnections[0] as MuxConnection;
    expect(brokered.peerIdentity?.identity).toBe('consumer-1');
    expect(brokered.peerIdentity?.receiver).toBe('worker-a1');
    // The worker may open nothing on the consumer.
    const push = worker.openChannelOn(brokered, new ChannelAddress({ type: workerType }));
    expectCode(await push.done, StatusCode.permissionDenied);
  });

  test('a dial-back already received is closed when CONNECT fails', async () => {
    const { workerMesh, id, consumer } = await setUp();
    const dialBack = workerMesh.client.connectHandler as ConnectHandler;
    let dialled: MuxConnection | undefined;
    workerMesh.client.connectHandler = async (request) => {
      await dialBack(request);
      dialled = workerMesh.brokeredConnections[0];
      throw SwitchboardError.of(StatusCode.unavailable, 'dial failed');
    };
    await rejectsWith(consumer.broker(new ServiceAddress(workerType, id)), StatusCode.unavailable);
    expectCode(await (dialled as MuxConnection).done, StatusCode.goingAway);
    expect(workerMesh.brokeredConnections).toEqual([]);
  });

  // Over the frame mux only: the toy mux confirms an IDENT as soon as it has
  // verified it, before the node can send GOAWAY for an intent it gave up
  // on, so the instance's dial does not fail there.
  for (const consumerFirst of overFrameMux() ? [true, false] : []) {
    // Dart runs both in one test; one case each here.
    test(`a dial-back that arrives after the consumer gave up is sent GOAWAY, and the instance's dial fails (${consumerFirst ? 'the consumer' : 'the naming service'} gives up first)`, async () => {
      // Either side gives up first: the consumer (its connectTimeout), or
      // the naming service (its brokerTimeout, then UNAVAILABLE).
      const short = 300;
      const mesh = await Mesh.start({ brokerTimeout: consumerFirst ? limit : short });
      const worker = await node({ credential: await issue('worker-a1', workerScopes) });
      const workerMesh = join(worker, mesh.uri, { watch: false });
      const id = await workerMesh.publish(workerType, () => {}, outbound);
      // The instance answers only once the consumer has given up.
      const dialBack = workerMesh.client.connectHandler as ConnectHandler;
      const asked = new Deferred<void>();
      const release = new Deferred<void>();
      const dialled = new Deferred<unknown>();
      workerMesh.client.connectHandler = async (request) => {
        asked.resolve();
        await release.promise;
        try {
          await dialBack(request);
          dialled.resolve(undefined);
        } catch (e) {
          dialled.resolve(e);
          throw e;
        }
      };
      const peerGoAways: Status[] = [];
      const ended: Promise<Status>[] = [];
      void (async () => {
        for await (const connection of worker.connections) {
          void connection.peerGoAwayStatus.then((status) => peerGoAways.push(status));
          ended.push(connection.done);
        }
      })();
      const consumer = await node({
        credential: await issue('consumer-1', consumerScopes),
        connectTimeout: consumerFirst ? short : limit,
      });
      await consumer.listenMemory({ policy: ChannelPolicies.scoped() });
      const consumerMesh = join(consumer, mesh.uri);
      await consumerMesh.client.synced;
      await rejectsWith(
        consumer.broker(new ServiceAddress(workerType, id)),
        consumerFirst ? StatusCode.deadlineExceeded : StatusCode.unavailable,
      );
      await asked.promise;
      release.resolve();
      const outcome = await dialled.promise;
      expect(outcome).toBeInstanceOf(SwitchboardError);
      expect(statusCodeName((outcome as SwitchboardError).code)).toBe('unauthenticated');
      // The consumer recognised the intent it gave up on and sent GOAWAY
      // once the IDENT verified.
      expect(ended).toHaveLength(1);
      await (ended[0] as Promise<Status>);
      expect(peerGoAways).toHaveLength(1);
      expectCode(peerGoAways[0] as Status, StatusCode.goingAway);
      expect(workerMesh.brokeredConnections).toEqual([]);
    });
  }

  test('broker refuses without a brokering resolver, a listener, an instance or a verifier', async () => {
    const consumer = await node({ credential: await issue('consumer-1', consumerScopes) });
    const address = new ServiceAddress(workerType, 5);
    await rejectsWith(consumer.broker(address), StatusCode.failedPrecondition);
    const mesh = await Mesh.start();
    join(consumer, mesh.uri);
    await rejectsWith(consumer.broker(address), StatusCode.failedPrecondition);
    await consumer.listenMemory();
    await rejectsWith(consumer.broker(new ServiceAddress(workerType)), StatusCode.invalidArgument);
    // Without a verifier it could not tell who dialled back.
    const blind = await node({
      credential: await issue('consumer-1', consumerScopes),
      verify: false,
    });
    await blind.listenMemory();
    await rejectsWith(
      blind.broker(address, { resolver: consumer.resolver }),
      StatusCode.failedPrecondition,
    );
  });

  test('PeerSet brokers, and brokers again after a loss', async () => {
    const { workerMesh, id, consumer } = await setUp();
    const set = PeerSet.watch(consumer, workerType, {
      channel: new ChannelAddress({ type: workerType }),
      initialBackoff: 20,
      maxBackoff: 100,
    });
    onCleanup(() => set.close());
    const events: string[] = [];
    set.on('event', (e: PeerEvent) => {
      events.push(e.type);
    });
    await until(() => set.online.length === 1);
    const peer = set.peers.get(id);
    expect(peer).toBeDefined();
    expect(peer?.endpoint).toBeUndefined();
    expect(peer?.connection?.peerIdentity?.identity).toBe('worker-a1');
    const first = peer?.connection as MuxConnection;
    await first.close();
    await until(
      () => peer?.isOnline === true && peer.connection !== undefined && peer.connection !== first,
    );
    await until(() => workerMesh.brokeredConnections.length === 1);
    expect(events).toEqual(['added', 'online', 'offline', 'online']);
    const channel = await set.openTalk(id);
    expect((await channel.request('RUN', 'x')).text()).toBe('x by consumer-1');
  });
});

// Not here: "MeshNode renews at two thirds of the lifetime; a credential
// that is not renewed ends the session at its expiry" is ported in
// `test/unit/switchboard/mesh.test.ts`, with MeshNode's other cases.
