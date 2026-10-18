/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// MeshNode: a node joined to a naming service over mem:// nodes, with real
// credentials (an Ed25519 authority, an HMAC one, holder keys) and the real
// NamingService, over the toy mux and, from `over-frame-mux/`, over the
// frame mux. The Dart reference has no unit file of its own for MeshNode:
// its node-level cases are in `naming_identity_test.dart` (the ones the
// naming thread's `test/unit/naming/naming-identity.test.ts` leaves to the
// node: MeshNode under a host that requires named IDENTs, a MeshNode
// registering again under a replacement credential, the renewal of its
// credential with fake time, and the CONNECT cases that need brokered
// connections and PeerSet), ported here with Dart's titles. The first
// group checks what `mesh.dart` documents of `join`, `publish`,
// `publishRelay`, `unpublish` and `leave`; the end-to-end meshes are in
// `test/integration/naming-mesh.test.ts` and its siblings, the sharded
// ones in `test/integration/sharding/`.

import { afterEach, beforeAll, describe, expect, test, vi } from 'vitest';
import { ChannelAddress, ServiceAddress } from '../../../src/core/address/index.ts';
import { utf8Decode } from '../../../src/core/bytes.ts';
import { type Credential, CredentialKind, Right } from '../../../src/core/identity/contract.ts';
import { CredentialIssuer } from '../../../src/core/identity/credential-issuer.ts';
import { CredentialVerifier } from '../../../src/core/identity/credential-verifier.ts';
import { Ed25519HolderKey } from '../../../src/core/identity/holder-key.ts';
import { Scope } from '../../../src/core/identity/scope.ts';
import type { MuxConnection } from '../../../src/core/mux/contract.ts';
import { Name } from '../../../src/core/name.ts';
import { AssignResult } from '../../../src/core/naming/naming-client-slots.ts';
import { Services, SlotMode, SlotSpace } from '../../../src/core/naming/naming-protocol.ts';
import { NamingResolver } from '../../../src/core/naming/naming-resolver.ts';
import { NamingService } from '../../../src/core/naming/naming-service.ts';
import { SlotLifecycle } from '../../../src/core/naming/slot-gate.ts';
import { type Status, StatusCode, SwitchboardError } from '../../../src/core/status.ts';
import { ChannelPolicies } from '../../../src/core/switchboard/channel-policy.ts';
import type { IncomingChannel, ServiceEvent } from '../../../src/core/switchboard/contract.ts';
import {
  LeaveReport,
  MeshNode,
  type MeshNodeOptions,
  namingClientFor,
} from '../../../src/core/switchboard/mesh-node.ts';
import { PeerSet } from '../../../src/core/switchboard/peer-set.ts';
import { RelayService } from '../../../src/core/switchboard/relay.ts';
import { StaticResolver } from '../../../src/core/switchboard/resolver.ts';
import type { Switchboard } from '../../../src/core/switchboard/switchboard.ts';
import type { TalkOptions } from '../../../src/core/talk/contract.ts';
import type { Clock } from '../../../src/core/util/clock.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';
import type { Logger, LogLevel } from '../../../src/core/util/logger.ts';
import { ManualClock } from '../../../src/testing/manual-clock.ts';
import { codeOf, node, onCleanup, overFrameMux, rejectsWith } from '../../support/node/harness.ts';

/** The Talk options of every node of the Dart test. */
const talk: Partial<TalkOptions> = { requestTimeout: 2000, replyTimeout: 1000 };
/** Upper bound for anything a test waits on in real time; only reached on failure. */
const limit = 5000;
const second = 1000;
const minute = 60 * second;
const hour = 60 * minute;

const naming = Services.naming;
const workerType = Name.of('worker-g');
const npc = Name.of('npc');

const authoritySeed = Uint8Array.from({ length: 32 }, (_, i) => (11 * i) & 0xff);
const meshKey = Uint8Array.from({ length: 32 }, (_, i) => 200 - i);

/** Wall time, read when called: the fake Date of the tests on fake time. */
const wall = (): number => Date.now();

let authority: CredentialIssuer;
let hmac: CredentialIssuer;
let key: Ed25519HolderKey;

const workerScopes = [Scope.of(Right.register, 'worker-*')];
const consumerScopes = [
  Scope.of(Right.watch, '*'),
  Scope.of(Right.open, 'worker-*'),
  Scope.of(Right.broker, 'worker-*'),
];

beforeAll(async () => {
  authority = await CredentialIssuer.ed25519FromSeed('auth1', authoritySeed, { wallTime: wall });
  hmac = CredentialIssuer.hmac('mesh1', meshKey, { wallTime: wall });
  key = await Ed25519HolderKey.generate();
});

afterEach(() => {
  vi.restoreAllMocks();
});

function issue(
  identity: string,
  scopes: readonly Scope[],
  options: { lifetime?: number; bearer?: boolean } = {},
): Promise<Credential> {
  const bearer = options.bearer ?? false;
  return (bearer ? hmac : authority).issue({
    kind: CredentialKind.node,
    identity,
    scopes,
    holderKey: bearer ? undefined : key.publicKey,
    lifetime: options.lifetime ?? 8 * hour,
  });
}

async function verifier(): Promise<CredentialVerifier> {
  const v = new CredentialVerifier({ wallTime: wall });
  await v.addIssuer(authority);
  await v.addIssuer(hmac);
  return v;
}

/** Resolves [promise], or rejects after [ms] of real time. */
function within<T>(promise: Promise<T>, ms = limit): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`not settled within ${ms} ms`)), ms);
    promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (error: unknown) => {
        clearTimeout(timer);
        reject(error);
      },
    );
  });
}

/** Polls [condition] every 2 ms of real time; fails after [limit]. */
async function until(condition: () => boolean, what = 'condition'): Promise<void> {
  const deadline = performance.now() + limit;
  while (!condition()) {
    if (performance.now() > deadline) {
      throw new Error(`${what} not met within ${limit} ms`);
    }
    await new Promise((resolve) => setTimeout(resolve, 2));
  }
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

interface NodeOptions {
  credential?: Credential | undefined;
  verify?: boolean;
  connectTimeout?: number;
  clock?: Clock | undefined;
  logger?: Logger | undefined;
}

/** A node with the Dart test's options, closed after the test. */
async function newNode(options: NodeOptions = {}): Promise<Switchboard> {
  const credential = options.credential;
  return node({
    talkOptions: talk,
    credential,
    holderKey: credential === undefined || credential.isBearer ? undefined : key,
    verifier: (options.verify ?? true) ? await verifier() : undefined,
    connectTimeout: options.connectTimeout ?? 10_000,
    clock: options.clock,
    logger: options.logger,
  });
}

/** [switchboard] joined to the naming service at [uri], left after the test. */
function join(switchboard: Switchboard, uri: string, options: MeshNodeOptions = {}): MeshNode {
  const mesh = MeshNode.join(switchboard, uri, options);
  onCleanup(() => mesh.leave());
  return mesh;
}

/** A naming service hosted as `_ns/1` on a node listening at [uri]. */
class Mesh {
  readonly host: Switchboard;
  readonly service: NamingService;
  readonly uri: string;

  private constructor(host: Switchboard, service: NamingService, uri: string) {
    this.host = host;
    this.service = service;
    this.uri = uri;
  }

  /** A naming service; with [options.name], at that `mem` name (where a stopped one was). */
  static async start(
    options: {
      requireCredential?: boolean;
      renewable?: (credential: Credential) => boolean;
      brokerTimeout?: number;
      clock?: Clock;
      name?: string;
    } = {},
  ): Promise<Mesh> {
    const host = await newNode({ clock: options.clock });
    const service = new NamingService({
      assignmentHold: 0,
      holdingSettle: 0,
      verifier: await verifier(),
      requireCredential: options.requireCredential ?? true,
      issuer: authority,
      renewable: options.renewable,
      brokerTimeout: options.brokerTimeout ?? 5000,
      wallTime: wall,
      ...(options.clock !== undefined && { clock: options.clock }),
    });
    onCleanup(() => service.close());
    host.registerService(naming, service.handler, { instance: 1 });
    // The naming service checks identities itself, the payload's too.
    const uri = await host.listenMemory({
      policy: ChannelPolicies.allowTypes([naming]),
      name: options.name,
    });
    return new Mesh(host, service, uri);
  }

  async close(): Promise<void> {
    await this.service.close();
    await this.host.close();
  }
}

function describeEvent(e: ServiceEvent): string {
  return `${e.up ? 'UP' : 'DOWN'} ${e.record.address}`;
}

/** The addresses of [service]'s table. */
function addresses(service: NamingService): string[] {
  return [...service.table.values()].map((record) => `${record.address}`);
}

/** A logger keeping the messages logged under [name] at [level] or above. */
function logged(level: LogLevel, name: string): { logger: Logger; messages: string[] } {
  const order: Record<LogLevel, number> = { debug: 0, info: 1, warn: 2, error: 3 };
  const messages: string[] = [];
  return {
    messages,
    logger: {
      log(l, n, message) {
        if (order[l] >= order[level] && n === name) {
          messages.push(message);
        }
      },
    },
  };
}

/** Answers every Talk request on [incoming] with `<payload> by <the caller's identity>`. */
function answering(incoming: IncomingChannel): void {
  void incoming.talk().serve((message) => {
    if (message.expectsReply) {
      void message.reply(`${message.text()} by ${incoming.peerIdentity?.identity}`);
    }
  });
}

/** A sharded service that keeps nothing and serves nothing. */
class Idle extends SlotLifecycle {
  load(): Promise<AssignResult> {
    return Promise.resolve(AssignResult.notHolding);
  }

  serve(): void {}
}

/** A sharded service that keeps its slots' storage and records what it unloads. */
class Recorded extends SlotLifecycle {
  readonly unloaded: number[] = [];

  load(): Promise<AssignResult> {
    return Promise.resolve(AssignResult.holding);
  }

  override unload(slot: number): void {
    this.unloaded.push(slot);
  }

  serve(): void {}
}

/**
 * A manual clock whose time is also the wall time, `Date.now()` reading it
 * at [origin] plus the clock's time: Dart's fake_async with
 * `package:clock`. Undone after each test.
 */
class FakeWall extends ManualClock {
  readonly origin: number;

  constructor(origin: number) {
    super();
    this.origin = origin;
    vi.spyOn(Date, 'now').mockImplementation(() => this.origin + this.now());
  }
}

describe('MeshNode', () => {
  test('join installs a naming resolver and starts the client; publish dispatches the assigned id; leave undoes both', async () => {
    const mesh = await Mesh.start({ requireCredential: false });
    const backend = await newNode();
    const uri = await backend.listenMemory();
    const previous = new StaticResolver();
    onCleanup(() => previous.close());
    backend.resolver = previous;
    const joined = join(backend, mesh.uri);
    expect(backend.resolver).toBe(joined.resolver);
    expect(joined.resolver).toBeInstanceOf(NamingResolver);
    expect(joined.resolver.client).toBe(joined.client);
    await within(joined.synced);
    const seen: string[] = [];
    const id = await within(
      joined.publish(npc, (incoming) => {
        seen.push(`${incoming.address.address}`);
        answering(incoming);
      }),
    );
    const address = new ServiceAddress(npc, id);
    expect(joined.published.map((a) => `${a}`)).toEqual([`${address}`]);
    expect(mesh.service.table.get(address.key)?.endpoints).toEqual([uri]);
    // Another node reaches it through the mesh, and the backend dispatches it.
    const consumer = await newNode();
    const consumerMesh = join(consumer, mesh.uri);
    await within(consumerMesh.synced);
    await until(() => consumerMesh.client.table.has(address.key), 'npc mirrored');
    const channel = await within(consumer.openTalk(npc));
    expect((await within(channel.request('RUN', 'job'))).text()).toBe('job by undefined');
    await channel.close();
    expect(seen).toEqual([`${address}`]);
    // Leaving: the record goes, the resolver is removed, the handler too.
    const report = await within(joined.leave());
    expect(joined.leave()).toBe(joined.leave());
    expect(report).toBeInstanceOf(LeaveReport);
    expect(report.isComplete).toBe(true);
    expect(report.handedOver.size + report.released.size + report.dropped.size).toBe(0);
    expect(backend.resolver).toBeUndefined();
    expect(joined.client.isClosed).toBe(true);
    expect(joined.published).toEqual([]);
    await until(() => !mesh.service.table.has(address.key), 'record dropped');
    const stale = await within(
      consumer.openTalkAt(uri, new ChannelAddress({ type: npc, instance: id })),
    );
    await rejectsWith(stale.request('RUN', 'late'), StatusCode.notFound);
    await stale.close();
    // Nothing works after leaving.
    await rejectsWith(
      joined.publish(npc, () => {}),
      StatusCode.failedPrecondition,
    );
    await rejectsWith(joined.unpublish(npc, id), StatusCode.failedPrecondition);
    await rejectsWith(
      joined.publishRelay(new RelayService(backend)),
      StatusCode.failedPrecondition,
    );
  });

  test('leave leaves a resolver installed meanwhile alone', async () => {
    const mesh = await Mesh.start({ requireCredential: false });
    const n = await newNode();
    const joined = join(n, mesh.uri);
    const other = new StaticResolver();
    onCleanup(() => other.close());
    n.resolver = other;
    await within(joined.leave());
    expect(n.resolver).toBe(other);
  });

  test('unpublish unregisters with the naming service, then locally; a second time is NOT_FOUND', async () => {
    const mesh = await Mesh.start({ requireCredential: false });
    const n = await newNode();
    await n.listenMemory();
    const joined = join(n, mesh.uri, { watch: false });
    const id = await within(joined.publish(npc, answering));
    const other = await within(joined.publish('chat', answering, { endpoints: [] }));
    expect(mesh.service.table.get(new ServiceAddress('chat', other).key)?.endpoints).toEqual([]);
    await within(joined.unpublish(npc, id));
    expect(joined.published.map((a) => `${a}`)).toEqual([`${new ServiceAddress('chat', other)}`]);
    expect(mesh.service.table.has(new ServiceAddress(npc, id).key)).toBe(false);
    await rejectsWith(joined.unpublish(npc, id), StatusCode.notFound);
  });

  test("publishRelay: the credential's identity as metadata; a relay of another node is a TypeError; without a credential, empty metadata and a warning", async () => {
    const mesh = await Mesh.start({ requireCredential: false });
    const relayNode = await newNode({
      credential: await issue('relay-1', [Scope.of(Right.register, '_relay')]),
    });
    await relayNode.listenMemory();
    const joined = join(relayNode, mesh.uri, { renewCredential: false });
    const elsewhere = await newNode();
    expect(() => joined.publishRelay(new RelayService(elsewhere))).toThrow(TypeError);
    const id = await within(joined.publishRelay(new RelayService(relayNode)));
    const record = mesh.service.table.get(new ServiceAddress(Services.relay, id).key);
    expect(utf8Decode(record?.metadata ?? new Uint8Array(0))).toBe('relay-1');
    expect(record?.endpoints).toEqual(relayNode.listeningEndpoints);
    // Without a credential.
    const { logger, messages } = logged('warn', 'Switchboard.Naming');
    const anonymous = await newNode({ logger });
    await anonymous.listenMemory();
    const anonymousMesh = join(anonymous, mesh.uri);
    const anonymousId = await within(anonymousMesh.publishRelay(new RelayService(anonymous)));
    const bare = mesh.service.table.get(new ServiceAddress(Services.relay, anonymousId).key);
    expect(bare?.metadata).toHaveLength(0);
    expect(messages.some((m) => m.includes('publishing a relay without a credential'))).toBe(true);
  });

  // Beside the Dart cases (the integration suites cover the hand-over):
  // what leave reports when no other instance can take a slot over, and
  // when the naming service is gone.
  test('leave releases, keeping the storage, the slots no other instance can take over', async () => {
    const mesh = await Mesh.start({ requireCredential: false });
    const n = await newNode();
    const joined = join(n, mesh.uri, { watch: false });
    const lifecycle = new Recorded();
    const gate = await within(joined.publishSharded(npc, lifecycle, { count: 4, endpoints: [] }));
    await until(() => gate.servedSlots.size === 4, 'every slot assigned');
    const id = gate.instance;
    const report = await within(joined.leave());
    expect([...report.released]).toEqual([[npc.key, [0, 1, 2, 3]]]);
    expect(report.handedOver.size).toBe(0);
    expect(report.dropped.size).toBe(0);
    expect(report.isComplete).toBe(false);
    expect(lifecycle.unloaded.sort()).toEqual([0, 1, 2, 3]);
    const table = mesh.service.slotTable(npc);
    for (let slot = 0; slot < 4; slot++) {
      expect(table?.get(slot).holder, `slot ${slot}`).toBe(id);
    }
  });

  test('leave stops handing over at leaveTimeout, on its clock; a slot still migrating then is dropped', async () => {
    const mesh = await Mesh.start({ requireCredential: false });
    const clock = new ManualClock();
    const a = await newNode();
    await a.listenMemory();
    const { logger, messages } = logged('warn', 'Switchboard.Naming');
    const leaving = join(a, mesh.uri, { watch: false, clock, logger, leaveTimeout: 1000 });
    const draining = new Deferred<void>();
    const hold = new Deferred<void>();
    onCleanup(() => hold.resolve());
    const lifecycle = new (class extends Recorded {
      override drain(): Promise<void> {
        draining.resolve();
        return hold.promise;
      }
    })();
    const gate = await within(leaving.publishSharded(npc, lifecycle, { count: 1 }));
    await until(() => gate.serves(0), 'the slot assigned');
    expect(gate.clock).toBe(clock);
    // A second instance the slot can go to.
    const b = await newNode();
    await b.listenMemory();
    const staying = join(b, mesh.uri, { watch: false });
    await within(staying.publishSharded(npc, new Recorded(), { count: 1 }));
    const report = leaving.leave();
    await within(draining.promise);
    clock.advance(999);
    await ManualClock.settle();
    expect(messages).toEqual([]);
    clock.advance(1);
    const done = await within(report);
    expect([...done.dropped]).toEqual([[npc.key, [0]]]);
    expect(done.handedOver.size + done.released.size).toBe(0);
    expect(messages).toEqual([
      'leaving: slots not handed over within 1000 ms',
      'leaving: slots dropped: {npc: [0]}',
    ]);
  });

  test('leave drops the slots when the naming service is gone', async () => {
    const mesh = await Mesh.start({ requireCredential: false });
    const n = await newNode();
    const joined = join(n, mesh.uri, { watch: false, reconnectDelay: 20 });
    const lifecycle = new Recorded();
    await within(
      joined.publishSharded(npc, lifecycle, { count: 4, mode: SlotMode.static, endpoints: [] }),
    );
    await within(joined.claimSlot(npc, 3));
    await within(joined.claimSlot(npc, 1));
    await rejectsWith(joined.claimSlot('chat', 0), StatusCode.failedPrecondition);
    await rejectsWith(joined.releaseSlot('chat', 0), StatusCode.failedPrecondition);
    await rejectsWith(
      joined.publishSharded(npc, new Idle(), { count: 4 }),
      StatusCode.failedPrecondition,
    );
    await mesh.close();
    await until(() => !joined.client.isConnected, 'the naming channel lost');
    const report = await within(joined.leave());
    expect([...report.dropped]).toEqual([[npc.key, [1, 3]]]);
    expect(report.released.size + report.handedOver.size).toBe(0);
    expect(`${report}`).toBe('LeaveReport(handed over {}, released {}, dropped {npc: [1, 3]})');
    expect(lifecycle.unloaded.sort()).toEqual([1, 3]);
    await rejectsWith(
      joined.publishSharded('chat', new Idle(), { count: 1 }),
      StatusCode.failedPrecondition,
    );
  });

  test('no renewal is scheduled without a credential, for one that never expires, or without renewCredential', async () => {
    const mesh = await Mesh.start({ requireCredential: false });
    const bare = await newNode();
    expect(join(bare, mesh.uri).nextRenewalAt).toBeUndefined();
    const forever = await newNode({
      credential: await authority.issue({
        kind: CredentialKind.node,
        identity: 'worker-a1',
        scopes: workerScopes,
        holderKey: key.publicKey,
        lifetime: null,
      }),
    });
    expect(join(forever, mesh.uri).nextRenewalAt).toBeUndefined();
    const credential = await issue('worker-a1', workerScopes, { lifetime: 3 * hour });
    const off = await newNode({ credential });
    const offMesh = join(off, mesh.uri, { renewCredential: false });
    expect(offMesh.renewCredential).toBe(false);
    expect(offMesh.nextRenewalAt).toBeUndefined();
    const on = await newNode({ credential });
    const onMesh = join(on, mesh.uri);
    expect(onMesh.nextRenewalAt).toBe((credential.issuedAt + 2 * 3600) * 1000);
    await within(onMesh.leave());
    expect(onMesh.nextRenewalAt).toBeUndefined();
  });

  test('namingClientFor sends the payload credentialFor gives the naming endpoint, else the default payload', async () => {
    const mesh = await Mesh.start({ requireCredential: false });
    const payloads: string[] = [];
    mesh.host.registerService(
      naming,
      (incoming) => {
        payloads.push(utf8Decode(incoming.address.payload));
        return mesh.service.handler(incoming);
      },
      { instance: 1 },
    );
    const plain = await newNode();
    plain.defaultPayload = new TextEncoder().encode('default');
    const a = namingClientFor(plain, mesh.uri);
    onCleanup(() => a.close());
    void a.start();
    await within(a.synced);
    const asked: (string | undefined)[] = [];
    const hooked = node({
      talkOptions: talk,
      defaultPayload: 'ignored',
      credentialFor: (endpoint, record) => {
        asked.push(`${endpoint} ${record === undefined ? 'no record' : 'record'}`);
        return new TextEncoder().encode('hooked');
      },
    });
    const b = namingClientFor(hooked, mesh.uri, { reconnectDelay: 20, watch: false });
    onCleanup(() => b.close());
    expect(b.reconnectDelay).toBe(20);
    expect(b.watch).toBe(false);
    expect(b.clock).toBe(hooked.clock);
    void b.start();
    await until(() => payloads.length === 2, 'the second naming channel');
    expect(payloads).toEqual(['default', 'hooked']);
    expect(asked).toEqual([`${mesh.uri} no record`]);
  });
});

describe('scopes', () => {
  test('after a naming service restart, an instance defines its slot space again with its own credential', async () => {
    const name = 'ns-restarting';
    const mesh = await Mesh.start({ name });
    const worker = await newNode({
      credential: await issue('worker-a1', [...workerScopes, Scope.of(Right.claim, 'worker-*')]),
    });
    const joined = join(worker, mesh.uri, {
      watch: false,
      renewCredential: false,
      reconnectDelay: 20,
    });
    const space = new SlotSpace(workerType, { count: 4, mode: SlotMode.static });
    await within(
      joined.publishSharded(workerType, new Idle(), {
        count: space.count,
        mode: space.mode,
        capacity: 0,
        endpoints: [],
      }),
    );
    await within(joined.claimSlot(workerType, 2));
    const ids = [...mesh.service.table.values()].map((r) => r.address.instance);
    expect(ids).toHaveLength(1);
    const id = ids[0] as number;
    expect(mesh.service.slotTable(workerType)?.get(2).owner).toBe(id);
    await mesh.close();
    // Nobody with admin around: the instance's own SLOTS defines the space
    // on the new naming service, and its slot is claimed back.
    const restarted = await Mesh.start({ name });
    expect(restarted.uri).toBe(mesh.uri);
    await until(
      () => restarted.service.slotTable(workerType)?.get(2).owner === id,
      'the slot claimed back',
    );
    expect(restarted.service.slotTable(workerType)?.space.equals(space)).toBe(true);
    expect(addresses(restarted.service)).toEqual([`${new ServiceAddress(workerType, id)}`]);
    await within(joined.leave());
  });

  test('a node whose IDENT names the naming service is accepted by a host that requires it; MeshNode works under the scopes', async () => {
    const host = node({
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
      wallTime: wall,
    });
    onCleanup(() => service.close());
    host.registerService(naming, service.handler, { instance: 1 });
    const uri = await host.listenMemory({ policy: ChannelPolicies.scoped() });
    const worker = node({
      talkOptions: talk,
      credential: await issue('worker-a1', workerScopes),
      holderKey: key,
      expectedIdentityFor: (endpoint) => (endpoint === uri ? 'ns' : undefined),
    });
    const mesh = join(worker, uri, { renewCredential: false, watch: false });
    const id = await within(mesh.publish(workerType, () => {}, { endpoints: [] }));
    expect(addresses(service)).toEqual([`${new ServiceAddress(workerType, id)}`]);
    expect(mesh.client.isSynced).toBe(false);
    // A client that watches anyway is refused once, and keeps its channel
    // and its registrations.
    const watching = join(worker, uri, { renewCredential: false });
    await within(watching.publish('worker-h', () => {}, { endpoints: [] }));
    await delay(100);
    expect(watching.client.isConnected).toBe(true);
    expect(service.table.size).toBe(2);
    // Another node that names nobody is refused by the host.
    const unnamed = await newNode({ credential: await issue('consumer-1', consumerScopes) });
    await rejectsWith(unnamed.connect(uri), StatusCode.unauthenticated);
  });
});

describe('a replacement credential', () => {
  test('of another identity: a MeshNode registers again under it', async () => {
    const mesh = await Mesh.start();
    const changes: string[] = [];
    mesh.service.on('event', (e) => changes.push(describeEvent(e)));
    const worker = await newNode({ credential: await issue('worker-a1', workerScopes) });
    const joined = join(worker, mesh.uri, {
      watch: false,
      renewCredential: false,
      reconnectDelay: 20,
    });
    const id = await within(joined.publish(workerType, () => {}, { endpoints: [] }));
    await worker.updateCredential(await issue('worker-b2', workerScopes));
    await until(() => changes.length === 3, 'registered again');
    const address = `${new ServiceAddress(workerType, id)}`;
    expect(changes).toEqual([`UP ${address}`, `DOWN ${address}`, `UP ${address}`]);
    expect(mesh.service.channelCount).toBe(1);
  });
});

describe('expiry and RENEW', () => {
  test('MeshNode renews at two thirds of the lifetime; a credential that is not renewed ends the session at its expiry', async () => {
    // A whole second, so that the issue times are exact.
    const clock = new FakeWall(Date.UTC(2026, 9, 14, 12));
    let revoked = false;
    const mesh = await Mesh.start({ clock, renewable: () => !revoked });
    const original = await issue('worker-a1', workerScopes, { lifetime: 3 * hour });
    const worker = await newNode({ credential: original, clock });
    const ended: Status[] = [];
    void (async () => {
      for await (const connection of worker.connections) {
        void connection.peerGoAwayStatus.then((status) => ended.push(status));
      }
    })();
    const joined = MeshNode.join(worker, mesh.uri, { watch: false, wallTime: wall });
    await within(joined.publish(workerType, () => {}, { endpoints: [] }));
    expect(mesh.service.table.size).toBe(1);
    expect(joined.nextRenewalAt).toBe((original.issuedAt + 7200) * 1000);
    // Two hours in: renewed, and presented on the naming connection.
    await clock.elapseAsync(2 * hour);
    await until(() => worker.credential !== original, 'renewed');
    const renewed = worker.credential as Credential;
    expect(renewed.expiresAt).toBeGreaterThan(original.expiresAt);
    expect(renewed.identity).toBe(original.identity);
    await until(() => joined.nextRenewalAt !== undefined, 'next renewal scheduled');
    expect(joined.nextRenewalAt).toBe((renewed.issuedAt + 7200) * 1000);
    // Past the first expiry: still registered.
    await clock.elapseAsync(hour + 30 * minute);
    expect(mesh.service.table.size).toBe(1);
    expect(ended).toEqual([]);
    // Revoked: the next renewals fail (from four hours in), each retried
    // after a quarter of the time left, and the session ends at the
    // expiry, five hours in.
    revoked = true;
    await clock.elapseAsync(30 * minute);
    await until(() => joined.nextRenewalAt !== undefined, 'retry scheduled');
    expect(joined.nextRenewalAt).toBe(clock.origin + 4 * hour + 15 * minute);
    await clock.elapseAsync(30 * minute);
    expect(worker.credential).toBe(renewed);
    expect(mesh.service.table.size).toBe(1);
    expect(ended).toEqual([]);
    await clock.elapseAsync(31 * minute);
    await until(() => mesh.service.table.size === 0, 'record dropped');
    // The naming connection is sent GOAWAY; so is every new one, since the
    // credential has expired.
    await until(() => ended.length > 0, 'GOAWAY');
    expect(ended.map(codeOf).every((code) => code === 'unauthenticated')).toBe(true);
    const closing = Promise.all([joined.leave(), worker.close(), mesh.close()]);
    let closed = false;
    void closing.then(() => {
      closed = true;
    });
    for (let i = 0; i < 100 && !closed; i++) {
      await clock.elapseAsync(second);
    }
    expect(closed).toBe(true);
    await clock.elapseAsync(30 * second);
    expect(clock.pendingCount).toBe(0);
  });
});

describe('CONNECT', () => {
  /** A mesh, an outbound-only worker registered without endpoints, and a consumer listening with a scoped policy. */
  async function setUp() {
    const mesh = await Mesh.start();
    const worker = await newNode({ credential: await issue('worker-a1', workerScopes) });
    const workerMesh = join(worker, mesh.uri, { watch: false });
    const id = await within(workerMesh.publish(workerType, answering, { endpoints: [] }));
    const consumer = await newNode({ credential: await issue('consumer-1', consumerScopes) });
    await consumer.listenMemory({ policy: ChannelPolicies.scoped() });
    const consumerMesh = join(consumer, mesh.uri);
    await within(consumerMesh.synced);
    return { mesh, worker, workerMesh, id, consumer, consumerMesh };
  }

  test('the consumer opens a channel to an outbound-only worker', async () => {
    const { worker, workerMesh, id, consumer } = await setUp();
    const address = new ServiceAddress(workerType, id);
    const channel = await within(consumer.openTalk(address));
    expect((await within(channel.request('RUN', 'job'))).text()).toBe('job by consumer-1');
    // The connection is pooled per instance.
    const again = await within(consumer.openTalk(address));
    expect((await within(again.request('RUN', 'two'))).text()).toBe('two by consumer-1');
    expect(workerMesh.brokeredConnections).toHaveLength(1);
    const brokered = workerMesh.brokeredConnections[0] as MuxConnection;
    expect(brokered.peerIdentity?.identity).toBe('consumer-1');
    expect(brokered.peerIdentity?.receiver).toBe('worker-a1');
    // The worker may open nothing on the consumer.
    const push = worker.openChannelOn(brokered, new ChannelAddress({ type: workerType }));
    expect(codeOf(await within(push.done))).toBe('permissionDenied');
  });

  test('a dial-back already received is closed when CONNECT fails', async () => {
    const { workerMesh, id, consumer } = await setUp();
    const dialBack = workerMesh.client.connectHandler;
    expect(dialBack).toBeDefined();
    let dialled: MuxConnection | undefined;
    workerMesh.client.connectHandler = async (request) => {
      await dialBack?.(request);
      dialled = workerMesh.brokeredConnections[0];
      throw SwitchboardError.of(StatusCode.unavailable, 'dial failed');
    };
    await rejectsWith(consumer.broker(new ServiceAddress(workerType, id)), StatusCode.unavailable);
    expect(dialled).toBeDefined();
    expect(codeOf(await within((dialled as MuxConnection).done))).toBe('goingAway');
    expect(workerMesh.brokeredConnections).toEqual([]);
  });

  test("a dial-back that arrives after the consumer gave up is sent GOAWAY, and the instance's dial fails", async () => {
    // Either side gives up first: the consumer (its connectTimeout), or the
    // naming service (its brokerTimeout, then UNAVAILABLE).
    for (const consumerFirst of [true, false]) {
      const short = 300;
      const mesh = await Mesh.start({ brokerTimeout: consumerFirst ? limit : short });
      const worker = await newNode({ credential: await issue('worker-a1', workerScopes) });
      const workerMesh = join(worker, mesh.uri, { watch: false });
      const id = await within(workerMesh.publish(workerType, () => {}, { endpoints: [] }));
      // The instance answers only once the consumer has given up.
      const dialBack = workerMesh.client.connectHandler;
      const asked = new Deferred<void>();
      const release = new Deferred<void>();
      const dialled = new Deferred<unknown>();
      workerMesh.client.connectHandler = async (request) => {
        asked.resolve();
        await release.promise;
        try {
          await dialBack?.(request);
          dialled.resolve(undefined);
        } catch (error) {
          dialled.resolve(error);
          throw error;
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
      const consumer = await newNode({
        credential: await issue('consumer-1', consumerScopes),
        connectTimeout: consumerFirst ? short : limit,
      });
      await consumer.listenMemory({ policy: ChannelPolicies.scoped() });
      const consumerMesh = join(consumer, mesh.uri);
      await within(consumerMesh.synced);
      await rejectsWith(
        consumer.broker(new ServiceAddress(workerType, id)),
        consumerFirst ? StatusCode.deadlineExceeded : StatusCode.unavailable,
      );
      await within(asked.promise);
      release.resolve();
      const outcome = await within(dialled.promise);
      if (overFrameMux()) {
        // The GOAWAY overtakes the IDENT's confirmation, so the dial fails.
        expect(outcome).toBeInstanceOf(SwitchboardError);
        expect(codeOf((outcome as SwitchboardError).status)).toBe('unauthenticated');
      } else {
        // The toy mux confirms an IDENT at once.
        expect(outcome).toBeUndefined();
      }
      // The consumer recognised the intent it gave up on and sent GOAWAY
      // once the IDENT verified.
      expect(ended).toHaveLength(1);
      await within(ended[0] as Promise<Status>);
      expect(peerGoAways.map(codeOf)).toEqual(['goingAway']);
      expect(workerMesh.brokeredConnections).toEqual([]);
    }
  });

  test('broker refuses without a brokering resolver, a listener, an instance or a verifier', async () => {
    const consumer = await newNode({ credential: await issue('consumer-1', consumerScopes) });
    const address = new ServiceAddress(workerType, 5);
    await rejectsWith(consumer.broker(address), StatusCode.failedPrecondition);
    const mesh = await Mesh.start();
    join(consumer, mesh.uri);
    await rejectsWith(consumer.broker(address), StatusCode.failedPrecondition);
    await consumer.listenMemory();
    await rejectsWith(consumer.broker(new ServiceAddress(workerType)), StatusCode.invalidArgument);
    // Without a verifier it could not tell who dialled back.
    const blind = await newNode({
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
    set.on('event', (e) => events.push(e.type));
    await until(() => set.online.length === 1, 'online');
    const peer = set.peers.get(id);
    expect(peer).toBeDefined();
    if (peer === undefined) return;
    expect(peer.endpoint).toBeUndefined();
    expect(peer.connection?.peerIdentity?.identity).toBe('worker-a1');
    const first = peer.connection as MuxConnection;
    await first.close();
    await until(
      () => peer.isOnline && peer.connection !== undefined && peer.connection !== first,
      'brokered again',
    );
    await until(() => workerMesh.brokeredConnections.length === 1, 'one brokered connection');
    expect(events).toEqual(['added', 'online', 'offline', 'online']);
    const channel = await set.openTalk(id);
    expect((await within(channel.request('RUN', 'x'))).text()).toBe('x by consumer-1');
  });
});
