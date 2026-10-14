/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Ports of the Dart relay tests (`relay_test.dart`): the `_relay` service
// over mem:// nodes, outbound-only workers registered without endpoints, a
// relay node publishing `_relay` with its identity, and consumers that
// listen nowhere and reach the workers through it, over the toy mux and,
// from `over-frame-mux/`, over the frame mux. The Dart tests run a naming
// service and `MeshNode`; until those land here, a fake mesh stands in (a
// shared table and the naming service's CONNECT). The workers answer Talk
// requests, as in Dart; the bulk payload case needs the frame mux's frame
// limit and window (over the toy mux the payloads go inline) and runs over
// it only. The cases that need slot-routed opens or `MeshNode` are listed
// as todo.

import { afterEach, describe, expect, it, vi } from 'vitest';
import { ChannelAddress, ServiceAddress } from '../../../src/core/address/index.ts';
import { utf8Decode, utf8Encode } from '../../../src/core/bytes.ts';
import { type Credential, type PeerIdentity, Right } from '../../../src/core/identity/contract.ts';
import { type MuxChannel, type MuxConnection, muxOptions } from '../../../src/core/mux/contract.ts';
import { Name, type NameLike } from '../../../src/core/name.ts';
import { MovedStatus, Services, serviceRecord } from '../../../src/core/naming/naming-protocol.ts';
import { Status, StatusCode, statusCodeName } from '../../../src/core/status.ts';
import { ChannelPolicies } from '../../../src/core/switchboard/channel-policy.ts';
import type {
  ChannelPolicy,
  ExpectedIdentity,
  IncomingChannel,
  Resolver,
  ServiceEvent,
  ServiceRecord,
} from '../../../src/core/switchboard/contract.ts';
import { IncomingChannel as Incoming } from '../../../src/core/switchboard/incoming-channel.ts';
import { type Peer, PeerSet } from '../../../src/core/switchboard/peer-set.ts';
import { RelayService } from '../../../src/core/switchboard/relay.ts';
import { RelayConfig } from '../../../src/core/switchboard/relay-config.ts';
import type { Switchboard } from '../../../src/core/switchboard/switchboard.ts';
import { TalkChannel } from '../../../src/core/talk/index.ts';
import { memoryTransportPair } from '../../../src/core/transport/memory-transport.ts';
import type { Logger, LogLevel } from '../../../src/core/util/logger.ts';
import { Frames, nextText } from '../../support/node/channels.ts';
import { FakeVerifier, fakeCredential, fakeHolderKey } from '../../support/node/fake-identity.ts';
import { FakeMesh, type MeshResolver } from '../../support/node/fake-mesh.ts';
import {
  fast,
  node,
  onCleanup,
  overFrameMux,
  rejectsWith,
  testMux,
  until,
} from '../../support/node/harness.ts';

const limit = 5000;

const relayType = Services.relay;
const naming = Services.naming;
const workerType = Name.of('worker-g');
const otherType = Name.of('worker-d');

const key = fakeHolderKey(new Uint8Array(32).fill(9));

type Scopes = readonly (readonly [Right, string])[];

const workerScopes: Scopes = [[Right.register, 'worker-*']];
const consumerScopes: Scopes = [
  [Right.watch, '*'],
  [Right.open, 'worker-*'],
];
const relayScopes: Scopes = [
  [Right.register, '_relay'],
  [Right.watch, '*'],
  [Right.open, 'worker-*'],
  [Right.broker, 'worker-*'],
];

function issue(identity: string, scopes: Scopes, expiresAt = 0): Credential {
  return fakeCredential({ identity, holderKey: key.publicKey, scopes, expiresAt });
}

function bytes(text: string): Uint8Array {
  return utf8Encode(text);
}

function expectCode(status: Status, code: StatusCode): void {
  expect(statusCodeName(status.code)).toBe(statusCodeName(code));
}

afterEach(() => {
  vi.useRealTimers();
});

/** A logger keeping the messages logged under [name] (any name when null) at [level] or above. */
function logged(
  level: LogLevel,
  name: string | null = 'Switchboard.Relay',
): { logger: Logger; messages: string[] } {
  const order: Record<LogLevel, number> = { debug: 0, info: 1, warn: 2, error: 3 };
  const messages: string[] = [];
  return {
    messages,
    logger: {
      log(l, n, message) {
        if (order[l] >= order[level] && (name === null || n === name)) {
          messages.push(message);
        }
      },
    },
  };
}

interface NodeOptions {
  credential?: Credential | undefined;
  requireNamedIdent?: boolean;
  identifyOutgoing?: boolean;
  identityTimeout?: number;
  relay?: RelayConfig | undefined;
  expectedIdentityFor?: ExpectedIdentity | undefined;
  resolver?: Resolver | undefined;
  logger?: Logger | undefined;
}

/** A node with a verifier, closed after the test. */
function newNode(options: NodeOptions = {}): Switchboard {
  return node({
    muxOptions: { requireNamedIdent: options.requireNamedIdent ?? false },
    credential: options.credential,
    holderKey: options.credential === undefined ? undefined : key,
    verifier: new FakeVerifier(),
    identifyOutgoing: options.identifyOutgoing ?? true,
    identityTimeout: options.identityTimeout ?? limit,
    relay: options.relay,
    expectedIdentityFor: options.expectedIdentityFor,
    resolver: options.resolver,
    logger: options.logger,
  });
}

/** A node of [mesh]: its resolver is the mesh's, asking CONNECT as the node. */
function meshNode(mesh: FakeMesh, options: NodeOptions = {}): Switchboard {
  const resolver: MeshResolver = mesh.resolver();
  const n = newNode({ ...options, resolver });
  resolver.requester = n;
  return n;
}

/** What a worker channel did: the identity it saw and the application payload it received. */
interface Seen {
  readonly identity: string | undefined;
  readonly payload: string;
  readonly channel: MuxChannel;
}

/** An outbound-only worker: registered without endpoints, `register` only. */
class Worker {
  readonly node: Switchboard;
  readonly mesh: FakeMesh;
  readonly id: number;
  readonly type: Name;
  readonly seen: Seen[] = [];

  private constructor(node: Switchboard, mesh: FakeMesh, type: Name) {
    this.node = node;
    this.mesh = mesh;
    this.type = type;
    this.id = mesh.publish(node, type, (incoming) => this.#serve(incoming), { endpoints: [] });
  }

  static start(mesh: FakeMesh, options: { identity?: string; type?: Name } = {}): Worker {
    const n = newNode({ credential: issue(options.identity ?? 'worker-a1', workerScopes) });
    return new Worker(n, mesh, options.type ?? workerType);
  }

  /**
   * Answers every Talk request with what it saw; an application payload
   * `moved` is refused with a MOVED naming owner 7 at epoch 3, `hold`
   * keeps the channel open without reading it.
   */
  #serve(incoming: IncomingChannel): void {
    const payload = utf8Decode(incoming.address.payload);
    this.seen.push({
      identity: incoming.peerIdentity?.identity,
      payload,
      channel: incoming.channel,
    });
    if (payload === 'moved') {
      void incoming.reject(new MovedStatus({ owner: 7, epoch: 3, reason: 'elsewhere' }).toStatus());
      return;
    }
    if (payload === 'hold') {
      return;
    }
    void incoming.talk().serve((m) => {
      if (m.expectsReply) {
        void m.reply(
          `${m.text()} by ${this.id} for ${incoming.peerIdentity?.identity} with "${payload}"`,
        );
      }
    });
  }

  get address(): ServiceAddress {
    return new ServiceAddress(this.type, this.id);
  }

  get brokeredConnections(): MuxConnection[] {
    return this.mesh.brokeredConnections(this.node);
  }
}

interface RelayOptions {
  identity?: string;
  scopes?: Scopes;
  requireIdentity?: boolean;
  allowEndpoints?: boolean;
  maxChannelsPerConnection?: number;
  policy?: ChannelPolicy;
  identityTimeout?: number;
  resolver?: Resolver;
  logger?: Logger;
}

/** A relay node: listens on mem:// behind [policy], requires named IDENTs, publishes `_relay` with its identity. */
class Relay {
  readonly node: Switchboard;
  readonly service: RelayService;
  readonly uri: string;
  /** The address its `_relay` record is published under. */
  readonly address: ServiceAddress;

  private constructor(
    node: Switchboard,
    service: RelayService,
    uri: string,
    address: ServiceAddress,
  ) {
    this.node = node;
    this.service = service;
    this.uri = uri;
    this.address = address;
  }

  static async start(mesh: FakeMesh, options: RelayOptions = {}): Promise<Relay> {
    const identity = options.identity ?? 'relay-1';
    const n = meshNode(mesh, {
      credential: issue(identity, options.scopes ?? relayScopes),
      requireNamedIdent: true,
      identityTimeout: options.identityTimeout ?? limit,
      logger: options.logger,
    });
    const uri = await n.listenMemory({ policy: options.policy });
    const service = new RelayService(n, {
      requireIdentity: options.requireIdentity,
      allowEndpoints: options.allowEndpoints,
      maxChannelsPerConnection: options.maxChannelsPerConnection,
      resolver: options.resolver,
    });
    const id = mesh.publish(n, relayType, service.handler, { metadata: utf8Encode(identity) });
    return new Relay(n, service, uri, new ServiceAddress(relayType, id));
  }
}

/** A resolver of fixed [records] that answers only once released. */
class GatedResolver implements Resolver {
  readonly records: ServiceRecord[];
  #release: () => void = () => {};
  readonly #gate = new Promise<void>((resolve) => {
    this.#release = resolve;
  });
  #onAsked: () => void = () => {};
  /** Resolves at the first {@link resolve}. */
  readonly asked = new Promise<void>((resolve) => {
    this.#onAsked = resolve;
  });

  constructor(records: ServiceRecord[]) {
    this.records = records;
  }

  release(): void {
    this.#release();
  }

  async resolve(type: Name): Promise<ServiceRecord[]> {
    this.#onAsked();
    await this.#gate;
    return this.records.filter((record) => record.address.type.equals(type));
  }

  get events(): AsyncIterable<ServiceEvent> {
    return {
      [Symbol.asyncIterator]: () => ({
        next: () => Promise.resolve({ done: true, value: undefined }),
      }),
    };
  }

  get ready(): Promise<void> {
    return Promise.resolve();
  }

  async close(): Promise<void> {}
}

/** A consumer that listens nowhere (unless [listen]), joined to the mesh, with [relay]. */
async function consumer(
  mesh: FakeMesh,
  options: {
    identity?: string;
    scopes?: Scopes;
    relay?: RelayConfig;
    useRelay?: boolean;
    listen?: boolean;
    expectedIdentityFor?: ExpectedIdentity;
  } = {},
): Promise<Switchboard> {
  const c = meshNode(mesh, {
    credential: issue(options.identity ?? 'consumer-1', options.scopes ?? consumerScopes),
    relay: (options.useRelay ?? true) ? (options.relay ?? new RelayConfig()) : undefined,
    expectedIdentityFor: options.expectedIdentityFor,
  });
  if (options.listen ?? false) {
    await c.listenMemory({ policy: ChannelPolicies.scoped() });
  }
  return c;
}

const talks = new WeakMap<MuxChannel, TalkChannel>();

/** The answer of a worker to the job [job] on [channel] (wrapped in Talk once). */
async function ask(channel: MuxChannel, job: string): Promise<string> {
  let talk = talks.get(channel);
  if (talk === undefined) {
    talk = new TalkChannel(channel);
    talks.set(channel, talk);
  }
  return (await talk.request('RUN', job)).text();
}

/** A job on a fresh channel to [address] from [c]; its answer. */
async function run(c: Switchboard, address: ServiceAddress, job: string): Promise<string> {
  const channel = await c.openTalk(address);
  try {
    return (await channel.request('RUN', job)).text();
  } finally {
    await channel.close();
  }
}

/** A connection from [c] to the relay at [uri], identified naming it. */
function relayConnection(
  c: Switchboard,
  uri: string,
  receiver = 'relay-1',
): Promise<MuxConnection> {
  return c.dial(uri, { receiver });
}

/** Opens a `_relay` channel with [inner] as its application payload. */
function openRelayed(connection: MuxConnection, inner: Uint8Array): MuxChannel {
  return connection.open(new ChannelAddress({ type: relayType, payload: inner }).encode());
}

/** The close status of a `_relay` channel carrying [inner]. */
function refusal(connection: MuxConnection, inner: Uint8Array): Promise<Status> {
  return openRelayed(connection, inner).done;
}

/** The inner open payload of a channel to [type]/[instance] carrying [payload]. */
function innerOf(type: NameLike, instance = 0, payload = ''): Uint8Array {
  return new ChannelAddress({ type, instance, payload: bytes(payload) }).encode();
}

/** Collects the connections [s] reports from now on. */
function collect(s: Switchboard): MuxConnection[] {
  const seen: MuxConnection[] = [];
  void (async () => {
    for await (const connection of s.connections) seen.push(connection);
  })();
  return seen;
}

/** A connection of the run's mux whose reads of the next identity event are counted. */
function observedIdentity(connection: MuxConnection): { readonly waits: number } {
  let proto: object | null = Object.getPrototypeOf(connection);
  let getter: (() => Promise<void>) | undefined;
  while (proto !== null && getter === undefined) {
    getter = Object.getOwnPropertyDescriptor(proto, 'identityChanged')?.get;
    proto = Object.getPrototypeOf(proto);
  }
  const read = getter as () => Promise<void>;
  const counter = { waits: 0 };
  Object.defineProperty(connection, 'identityChanged', {
    get: () => {
      counter.waits++;
      return read.call(connection);
    },
  });
  return counter;
}

describe('identity holds', () => {
  for (const timedOut of [false, true]) {
    it(`identity holds release ${timedOut ? 'timed-out' : 'closed'} channels without accumulating listeners`, async () => {
      const host = newNode({ requireNamedIdent: true, identityTimeout: timedOut ? 5 : limit });
      const service = new RelayService(host);
      const [local, remote] = memoryTransportPair({ delay: 0 });
      const incoming = testMux()(local, { ...host.muxOptions, ...fast }, false);
      const observed = observedIdentity(incoming);
      const peer = testMux()(remote, muxOptions(fast), true);
      onCleanup(() => incoming.close());
      onCleanup(() => peer.close());
      const work: Promise<unknown>[] = [];
      void (async () => {
        for await (const channel of incoming.incoming) {
          work.push(Promise.resolve().then(() => service.handler(new Incoming(channel))));
        }
      })();
      for (let i = 0; i < 100; i++) {
        const channel = openRelayed(peer, innerOf(workerType));
        await peer.ping();
        if (timedOut) {
          expectCode(await channel.done, StatusCode.unauthenticated);
        } else {
          await channel.close();
        }
      }
      await Promise.all(work);
      expect(observed.waits).toBe(1);
      expect(incoming.openChannelCount).toBe(0);
    });
  }
});

it('scoped() admits _relay and _ns for every identified peer', async () => {
  const mesh = new FakeMesh();
  // The relay listener's policy lets a consumer with no `open` scope for
  // `_relay` through; the relay checks the inner type.
  const relay = await Relay.start(mesh, { policy: ChannelPolicies.scoped() });
  const worker = Worker.start(mesh);
  const c = await consumer(mesh);
  expect(await run(c, worker.address, 'job')).toContain(`by ${worker.id}`);
  expect(relay.node.listeningEndpoints).not.toHaveLength(0);
  const dummy = newNode({ credential: issue('x', []) });
  const connection = await dummy.dial(relay.uri, { receiver: 'relay-1' });
  // No `open` scope at all: the policy admits the channel, the relay
  // refuses the inner type.
  expectCode(
    await refusal(connection, innerOf(workerType, worker.id)),
    StatusCode.permissionDenied,
  );
  // Anything else is refused by the policy.
  const other = connection.open(new ChannelAddress({ type: workerType }).encode());
  expectCode(await other.done, StatusCode.permissionDenied);
});

describe('relaying', () => {
  it("a channel through the relay: request and reply end to end; the instance sees the relay's identity and the consumer's payload, the relay the consumer's", async () => {
    const mesh = new FakeMesh();
    const relay = await Relay.start(mesh);
    const worker = Worker.start(mesh);
    const c = await consumer(mesh);
    expect(c.canBroker).toBe(false);
    const mux = await c.openChannel(worker.address, { payload: bytes('token') });
    expect(await ask(mux, 'job')).toBe(`job by ${worker.id} for relay-1 with "token"`);
    // The channel went through the relay: a `_relay` channel whose payload
    // is the destination's open payload, instance filled in.
    const outer = ChannelAddress.decode(mux.openPayload);
    expect(outer.type?.equals(relayType)).toBe(true);
    expect(outer.instance).toBe(0);
    const inner = ChannelAddress.decode(outer.payload);
    expect(inner.type?.equals(workerType)).toBe(true);
    expect(inner.instance).toBe(worker.id);
    expect(utf8Decode(inner.payload)).toBe('token');
    expect(mux.connection.isOpen).toBe(true);
    expect(relay.uri).toBeDefined();
    // The worker dialled the relay, which identified naming it.
    const brokered = worker.brokeredConnections;
    expect(brokered).toHaveLength(1);
    expect(brokered[0]?.peerIdentity?.identity).toBe('relay-1');
    expect(brokered[0]?.peerIdentity?.receiver).toBe('worker-a1');
    expect(worker.seen[0]?.identity).toBe('relay-1');
    await mux.close();
  });

  if (overFrameMux()) {
    it('bulk payloads through the relay, both ways', async () => {
      const mesh = new FakeMesh();
      await Relay.start(mesh);
      const worker = Worker.start(mesh);
      const c = await consumer(mesh);
      const mux = await c.openChannel(worker.address, { payload: bytes('token') });
      const channel = new TalkChannel(mux);
      // 100 kB each way: too large for a frame, so a bulk payload through
      // the relay's pipes, toward the worker and back.
      const job = 'x'.repeat(100_000);
      const reply = await channel.request('RUN', job);
      expect(reply.isBulk).toBe(true);
      expect(reply.text()).toBe(`${job} by ${worker.id} for relay-1 with "token"`);
      const streamed = await channel.request('RUN', undefined, {
        bulk: [bytes('a'.repeat(40_000)), bytes('b'.repeat(40_000))],
      });
      expect(streamed.text().startsWith(`${'a'.repeat(40_000)}${'b'.repeat(40_000)} by`)).toBe(
        true,
      );
      await channel.close();
    });
  }

  it("the relay record carries the relay's identity; the consumer names it", async () => {
    const mesh = new FakeMesh();
    const relay = await Relay.start(mesh, { identity: 'relay-7' });
    const c = await consumer(mesh);
    const relays = await c.resolveRelays();
    expect(relays).toHaveLength(1);
    expect(relays[0]?.address.type.equals(relayType)).toBe(true);
    expect(utf8Decode(relays[0]?.metadata ?? new Uint8Array(0))).toBe('relay-7');
    expect(relays[0]?.endpoints).toEqual([relay.uri]);
    const worker = Worker.start(mesh);
    // A connection the relay accepted from the consumer: identified, naming relay-7.
    const identities: PeerIdentity[] = [];
    void (async () => {
      for await (const connection of relay.node.connections) {
        connection.peerIdentified.then(
          (identity) => identities.push(identity),
          () => {},
        );
      }
    })();
    expect(await run(c, worker.address, 'x')).toContain('for relay-7');
    await until(() => identities.length > 0);
    expect(identities[0]?.identity).toBe('consumer-1');
    expect(identities[0]?.receiver).toBe('relay-7');
  });

  it('explicit relay endpoints with an identity', async () => {
    const mesh = new FakeMesh();
    const relay = await Relay.start(mesh);
    const worker = Worker.start(mesh);
    const c = await consumer(mesh, {
      relay: new RelayConfig({ endpoints: [relay.uri], identity: 'relay-1' }),
    });
    expect(await run(c, worker.address, 'x')).toContain('for relay-1');
    const relays = await c.resolveRelays();
    expect(relays[0]?.address.instance).toBe(0);
    expect(relays[0]?.endpoints).toEqual([relay.uri]);
    // Without the identity the relay's host refuses the IDENT that names nobody.
    const anonymous = await consumer(mesh, {
      identity: 'consumer-2',
      relay: new RelayConfig({ endpoints: [relay.uri] }),
    });
    await rejectsWith(anonymous.openChannel(worker.address), StatusCode.unavailable);
    await rejectsWith(anonymous.connect(relay.uri), StatusCode.unauthenticated);
    // expectedIdentityFor wins over the record: a wrong name is refused.
    const wrong = await consumer(mesh, {
      identity: 'consumer-3',
      expectedIdentityFor: (_endpoint, record) =>
        record?.address.type.equals(relayType) ? 'relay-9' : undefined,
    });
    await rejectsWith(wrong.openChannel(worker.address), StatusCode.unavailable);
    expect(() => new RelayConfig({ endpoints: [] })).toThrow(RangeError);
    expect(() => new RelayConfig({ endpoints: [relay.uri], identity: '' })).toThrow(RangeError);
  });

  it('an expectedIdentityFor that throws fails that relay: the next is tried', async () => {
    const mesh = new FakeMesh();
    const relays = [await Relay.start(mesh), await Relay.start(mesh, { identity: 'relay-2' })];
    const worker = Worker.start(mesh);
    // The first relay asked about fails, the other one carries the open.
    const asked: string[] = [];
    const c = await consumer(mesh, {
      expectedIdentityFor: (endpoint, record) => {
        if (!record?.address.type.equals(relayType)) {
          return undefined;
        }
        asked.push(endpoint);
        if (asked.length === 1) {
          throw new Error('vault sealed');
        }
        return undefined;
      },
    });
    const answer = await run(c, worker.address, 'x');
    expect(asked).toHaveLength(2);
    expect(new Set(asked).size).toBe(2);
    const used = relays.find((r) => r.uri === asked.at(-1));
    expect(answer).toContain(`for ${used === relays[0] ? 'relay-1' : 'relay-2'}`);
    // Every relay failing: the destination is unavailable, never reached
    // through an IDENT naming nobody.
    const failing = await consumer(mesh, {
      expectedIdentityFor: (_endpoint, record) => {
        if (record?.address.type.equals(relayType)) {
          throw new Error('vault sealed');
        }
        return undefined;
      },
    });
    const error = await rejectsWith(failing.openChannel(worker.address), StatusCode.unavailable);
    expect(error.status.reason).toContain('expectedIdentityFor');
    expect(error.status.reason).not.toContain('vault sealed');
    expect(worker.seen).toHaveLength(1);
  });

  it('RelayConfig: the relay type must be reserved', () => {
    expect(new RelayConfig().type.equals(relayType)).toBe(true);
    expect(new RelayConfig({ type: '_relay2' }).type.equals(Name.of('_relay2'))).toBe(true);
    // Relays refuse to relay to reserved types only: a relay type outside
    // them would let relays chain.
    expect(() => new RelayConfig({ type: 'meshrelay' })).toThrow(RangeError);
    expect(() => new RelayConfig({ type: Name.empty })).toThrow(RangeError);
    expect(
      () => new RelayConfig({ type: 'meshrelay', endpoints: ['mem://relay'], identity: 'relay-1' }),
    ).toThrow(RangeError);
  });

  it('a relay on a node that accepts unnamed IDENTs is warned about', () => {
    const { logger, messages } = logged('warn');
    const lax = newNode({ credential: issue('relay-1', relayScopes), logger });
    new RelayService(lax);
    expect(messages).toHaveLength(1);
    expect(messages[0]).toContain('requireNamedIdent');
    messages.length = 0;
    const strict = newNode({
      credential: issue('relay-1', relayScopes),
      requireNamedIdent: true,
      logger,
    });
    new RelayService(strict);
    expect(messages).toHaveLength(0);
  });

  it('one brokered connection shared by two consumers', async () => {
    const mesh = new FakeMesh();
    await Relay.start(mesh);
    const worker = Worker.start(mesh);
    const a = await consumer(mesh);
    const b = await consumer(mesh, { identity: 'consumer-2' });
    const results = await Promise.all([
      run(a, worker.address, 'a1'),
      run(b, worker.address, 'b1'),
      run(a, worker.address, 'a2'),
      run(b, worker.address, 'b2'),
    ]);
    expect(results).toEqual(
      ['a1', 'b1', 'a2', 'b2'].map((job) => `${job} by ${worker.id} for relay-1 with ""`),
    );
    expect(worker.brokeredConnections).toHaveLength(1);
  });

  it('a node that can broker never relays', async () => {
    const mesh = new FakeMesh();
    const relay = await Relay.start(mesh);
    const worker = Worker.start(mesh);
    const c = await consumer(mesh, {
      scopes: [...consumerScopes, [Right.broker, 'worker-*']],
      listen: true,
    });
    expect(c.canBroker).toBe(true);
    const relayed = collect(relay.node);
    const mux = await c.openChannel(worker.address);
    expect(await ask(mux, 'x')).toBe(`x by ${worker.id} for consumer-1 with ""`);
    expect(ChannelAddress.decode(mux.openPayload).type?.equals(workerType)).toBe(true);
    // Only the worker's own dial-back for the consumer, no relay.
    expect(relayed).toHaveLength(0);
    expect(worker.brokeredConnections[0]?.peerIdentity?.identity).toBe('consumer-1');
    await mux.close();
  });

  it('without a relay, a node that cannot broker fails as before; a record with endpoints is dialled directly', async () => {
    const mesh = new FakeMesh();
    await Relay.start(mesh);
    const worker = Worker.start(mesh);
    const c = await consumer(mesh, { useRelay: false });
    const error = await rejectsWith(c.openChannel(worker.address), StatusCode.unavailable);
    expect(error.status.reason).toContain('no listener');
    await rejectsWith(c.resolveRelays(), StatusCode.failedPrecondition);
    // A worker that listens is reached directly by a consumer with a relay.
    const lister = newNode({ credential: issue('worker-b2', workerScopes) });
    const listerUri = await lister.listenMemory({ policy: ChannelPolicies.scoped() });
    const listed = mesh.publish(
      lister,
      otherType,
      (incoming) => {
        void (async () => {
          const frames = new Frames(incoming.channel);
          while ((await frames.next()) !== undefined) {
            void incoming.channel.send(bytes(`direct for ${incoming.peerIdentity?.identity}`));
          }
        })();
      },
      { endpoints: [listerUri] },
    );
    const r = await consumer(mesh, { identity: 'consumer-2' });
    const mux = await r.openChannel(new ServiceAddress(otherType, listed));
    await mux.send(bytes('X'));
    expect(await nextText(mux)).toBe('direct for consumer-2');
    expect(ChannelAddress.decode(mux.openPayload).type?.equals(otherType)).toBe(true);
    await mux.close();
  });

  it('a lost connection on either side reaches the other as UNAVAILABLE; the relay brokers again', async () => {
    const mesh = new FakeMesh();
    await Relay.start(mesh);
    const worker = Worker.start(mesh);
    const c = await consumer(mesh);
    // The worker side goes: the consumer's channel ends UNAVAILABLE.
    const first = await c.openChannel(worker.address, { payload: bytes('hold') });
    await until(() => worker.seen.length === 1);
    await (worker.brokeredConnections[0] as MuxConnection).close();
    expectCode(await first.done, StatusCode.unavailable);
    // Brokered again for the next channel.
    expect(await run(c, worker.address, 'again')).toContain('again by');
    await until(() => worker.brokeredConnections.length === 1);
    // The consumer side goes: the worker's channel ends UNAVAILABLE.
    const second = await c.openChannel(worker.address, { payload: bytes('hold') });
    await until(() => worker.seen.length === 3);
    const atWorker = (worker.seen.at(-1) as Seen).channel;
    await second.connection.close();
    expectCode(await atWorker.done, StatusCode.unavailable);
  });

  it('MOVED is relayed with its fields byte for byte', async () => {
    const mesh = new FakeMesh();
    await Relay.start(mesh);
    const worker = Worker.start(mesh);
    const c = await consumer(mesh);
    const channel = await c.openChannel(worker.address, { payload: bytes('moved') });
    const status = await channel.done;
    expectCode(status, StatusCode.moved);
    expect(status.encode()).toEqual(
      new MovedStatus({ owner: 7, epoch: 3, reason: 'elsewhere' }).encode(),
    );
  });

  it.todo(
    'openTalkToSlot reaches a slot owner through the relay (needs slot-routed opens and Talk)',
  );
});

describe('refusals', () => {
  it('an unidentified consumer: UNAUTHENTICATED, after the hold; one whose OPEN overtakes its IDENT is held, then admitted', async () => {
    const mesh = new FakeMesh();
    const worker = Worker.start(mesh);
    const relay = await Relay.start(mesh, { identityTimeout: 200 });
    const anonymous = newNode();
    const connection = await anonymous.connect(relay.uri);
    expectCode(
      await refusal(connection, innerOf(workerType, worker.id)),
      StatusCode.unauthenticated,
    );
    // A consumer whose OPEN overtakes its IDENT is held, then admitted.
    const overtaken = await anonymous.dial(relay.uri);
    const held = openRelayed(overtaken, innerOf(workerType, worker.id, 'p'));
    await overtaken.identify(issue('consumer-1', consumerScopes), {
      holderKey: key,
      receiver: 'relay-1',
    });
    expect(await ask(held, 'h')).toBe(`h by ${worker.id} for relay-1 with "p"`);
    await held.close();
  });

  it('a channel that ends while held for identity is dropped without a refusal and does not count toward the bound', async () => {
    const mesh = new FakeMesh();
    const worker = Worker.start(mesh);
    const { logger, messages } = logged('debug');
    const relay = await Relay.start(mesh, { maxChannelsPerConnection: 1, logger });
    const c = newNode({ credential: issue('consumer-1', consumerScopes), identifyOutgoing: false });
    const connection = await c.dial(relay.uri);
    const dropped = openRelayed(connection, innerOf(workerType, worker.id, 'dropped'));
    await until(() => messages.some((m) => m.includes('held until')));
    await dropped.close(Status.of(StatusCode.cancelled, 'gone'));
    await until(() => messages.some((m) => m.includes('ended while held')));
    expect(messages.filter((m) => m.includes('refused'))).toHaveLength(0);
    // The bound of one is free: the next channel, held until the consumer
    // identifies, is relayed, and the one after it refused.
    const next = openRelayed(connection, innerOf(workerType, worker.id, 'hold'));
    await c.identifyOn(connection, { receiver: 'relay-1' });
    await until(() => worker.seen.length === 1);
    expect(worker.seen[0]?.payload).toBe('hold');
    expectCode(
      await refusal(connection, innerOf(workerType, worker.id, 'hold')),
      StatusCode.resourceExhausted,
    );
    expect(next.state).toBe('open');
  });

  it('without requireIdentity: an unidentified consumer is admitted unchecked after the hold, an identified one held to its scopes', async () => {
    const mesh = new FakeMesh();
    const worker = Worker.start(mesh);
    const hold = 300;
    const lenient = await Relay.start(mesh, {
      identity: 'relay-2',
      requireIdentity: false,
      identityTimeout: hold,
    });
    const anonymous = newNode();
    const open = await anonymous.connect(lenient.uri);
    const started = Date.now();
    const channel = openRelayed(open, innerOf(workerType, worker.id, 'p'));
    expect(await ask(channel, 'r')).toBe(`r by ${worker.id} for relay-2 with "p"`);
    // Held for the node's identity timeout first, in case an IDENT was on its way.
    expect(Date.now() - started).toBeGreaterThanOrEqual(hold - 5);
    await channel.close();
    // An identified consumer is held to its scopes all the same.
    const scoped = newNode({ credential: issue('consumer-4', [[Right.open, 'x']]) });
    const identified = await scoped.dial(lenient.uri, { receiver: 'relay-2' });
    expectCode(
      await refusal(identified, innerOf(workerType, worker.id)),
      StatusCode.permissionDenied,
    );
    expect(worker.seen).toHaveLength(1);
  });

  it('without requireIdentity: an OPEN that overtakes the IDENT is checked under the identity the IDENT establishes', async () => {
    const mesh = new FakeMesh();
    const worker = Worker.start(mesh);
    const lenient = await Relay.start(mesh, { identity: 'relay-2', requireIdentity: false });
    // A credential without `open` for the type, presented only after the OPEN.
    const sneaky = newNode({
      credential: issue('consumer-4', [[Right.open, 'x']]),
      identifyOutgoing: false,
    });
    const connection = await sneaky.dial(lenient.uri);
    const channel = openRelayed(connection, innerOf(workerType, worker.id, 'sneaky'));
    await sneaky.identifyOn(connection, { receiver: 'relay-2' });
    expectCode(await channel.done, StatusCode.permissionDenied);
    expect(worker.seen).toHaveLength(0);
    // The same order with `open` for the type: relayed.
    const honest = newNode({
      credential: issue('consumer-1', consumerScopes),
      identifyOutgoing: false,
    });
    const second = await honest.dial(lenient.uri);
    const relayed = openRelayed(second, innerOf(workerType, worker.id, 'p'));
    await honest.identifyOn(second, { receiver: 'relay-2' });
    expect(await ask(relayed, 'h')).toBe(`h by ${worker.id} for relay-2 with "p"`);
    await relayed.close();
  });

  it('a consumer that identifies again while its channel is being opened is checked under the new identity', async () => {
    const mesh = new FakeMesh();
    // A worker that listens, which the relay finds through a resolver that
    // answers only once the consumer has identified again.
    const lister = newNode({ credential: issue('worker-b2', workerScopes) });
    const listerUri = await lister.listenMemory({ policy: ChannelPolicies.scoped() });
    const listedType = Name.of('worker-l');
    const opened: MuxChannel[] = [];
    lister.registerService(listedType, (incoming) => void opened.push(incoming.channel), {
      instance: 1,
    });
    const gate = new GatedResolver([
      serviceRecord(new ServiceAddress(listedType, 1), { endpoints: [listerUri] }),
    ]);
    const relay = await Relay.start(mesh, { allowEndpoints: true, resolver: gate });
    const accepted = collect(relay.node);
    const c = newNode({ credential: issue('consumer-1', consumerScopes) });
    const connection = await relayConnection(c, relay.uri);
    const channel = openRelayed(connection, innerOf(listedType, 1));
    // Past the scope check, resolving: the consumer identifies again, with
    // a credential that may not open the type.
    await gate.asked;
    await connection.identify(issue('consumer-5', [[Right.open, 'other-*']]), {
      holderKey: key,
      receiver: 'relay-1',
    });
    await until(() => accepted.some((x) => x.peerIdentity?.identity === 'consumer-5'));
    gate.release();
    expectCode(await channel.done, StatusCode.permissionDenied);
    // The channel to the instance, opened meanwhile, is closed.
    await until(() => opened.length > 0);
    expectCode(await (opened[0] as MuxChannel).done, StatusCode.cancelled);
  });

  it('a consumer expiring while its channel is opened is refused; the channel to the instance is cancelled, the bound released', async () => {
    vi.useFakeTimers({ toFake: ['Date'] });
    const mesh = new FakeMesh();
    const worker = newNode();
    const workerUri = await worker.listenMemory();
    const opened: MuxChannel[] = [];
    worker.registerService(
      workerType,
      (incoming) => {
        opened.push(incoming.channel);
        void incoming.channel.send(bytes('private greeting'));
      },
      { instance: 1 },
    );
    const gate = new GatedResolver([
      serviceRecord(new ServiceAddress(workerType, 1), { endpoints: [workerUri] }),
    ]);
    const relay = await Relay.start(mesh, {
      allowEndpoints: true,
      resolver: gate,
      maxChannelsPerConnection: 1,
    });
    const credential = (): Credential =>
      issue('consumer-1', consumerScopes, Math.floor(Date.now() / 1000) + 60);
    const c = newNode({ credential: credential() });
    const connection = await relayConnection(c, relay.uri);
    const inner = innerOf(workerType, 1);
    const channel = openRelayed(connection, inner);
    await gate.asked;
    vi.setSystemTime(Date.now() + 120_000);
    gate.release();
    expectCode(await channel.done, StatusCode.unauthenticated);
    expect(await new Frames(channel).rest()).toHaveLength(0);
    await until(() => opened.length > 0);
    expectCode(await (opened[0] as MuxChannel).done, StatusCode.cancelled);
    // Renewed, the consumer has the one channel its bound allows.
    await c.updateCredential(credential());
    const again = openRelayed(connection, inner);
    expect(await nextText(again)).toBe('private greeting');
    await again.close();
  });

  it('a consumer that identifies without naming the relay is refused by requireNamedIdent', async () => {
    const mesh = new FakeMesh();
    const relay = await Relay.start(mesh);
    const c = newNode({ credential: issue('consumer-1', consumerScopes) });
    await rejectsWith(c.connect(relay.uri), StatusCode.unauthenticated);
    await rejectsWith(c.dial(relay.uri, { receiver: 'someone-else' }), StatusCode.unauthenticated);
    const named = await c.dial(relay.uri, { receiver: 'relay-1' });
    expect(named.isOpen).toBe(true);
  });

  it('malformed, untyped, host hints, reserved types and chains: INVALID_ARGUMENT', async () => {
    const mesh = new FakeMesh();
    const relay = await Relay.start(mesh);
    const worker = Worker.start(mesh);
    const c = newNode({ credential: issue('consumer-1', [[Right.open, '*']]) });
    const connection = await relayConnection(c, relay.uri);
    const cases: Record<string, Uint8Array> = {
      'reserved flag bit': Uint8Array.of(0x10),
      truncated: Uint8Array.of(0x01, 0x61, 0x70),
      'no type': new Uint8Array(0),
      'default service': Uint8Array.of(0x00, 0x01),
      'host hint': new ChannelAddress({
        type: workerType,
        instance: worker.id,
        host: 'b.example:1',
      }).encode(),
      'the naming service': new ChannelAddress({ type: naming }).encode(),
      'a chained relay': new ChannelAddress({
        type: relayType,
        payload: innerOf(workerType, worker.id),
      }).encode(),
      'another reserved type': new ChannelAddress({ type: Name.of('_echo') }).encode(),
    };
    for (const [what, inner] of Object.entries(cases)) {
      expect(statusCodeName((await refusal(connection, inner)).code), what).toBe(
        statusCodeName(StatusCode.invalidArgument),
      );
    }
    expect(worker.seen).toHaveLength(0);
  });

  it('a consumer without `open` for the inner type: PERMISSION_DENIED', async () => {
    const mesh = new FakeMesh();
    const relay = await Relay.start(mesh);
    const worker = Worker.start(mesh);
    const c = await consumer(mesh, {
      scopes: [
        [Right.watch, '*'],
        [Right.open, 'other-*'],
      ],
    });
    const channel = await c.openChannel(worker.address);
    expectCode(await channel.done, StatusCode.permissionDenied);
    const raw = await relayConnection(c, relay.uri);
    expectCode(await refusal(raw, innerOf(workerType, worker.id)), StatusCode.permissionDenied);
    expect(worker.seen).toHaveLength(0);
  });

  it('the per-connection bound: RESOURCE_EXHAUSTED', async () => {
    const mesh = new FakeMesh();
    const relay = await Relay.start(mesh, { maxChannelsPerConnection: 2 });
    const worker = Worker.start(mesh);
    const c = newNode({ credential: issue('consumer-1', consumerScopes) });
    const connection = await relayConnection(c, relay.uri);
    const hold = innerOf(workerType, worker.id, 'hold');
    const first = openRelayed(connection, hold);
    const second = openRelayed(connection, hold);
    await until(() => worker.seen.length === 2);
    expectCode(await refusal(connection, hold), StatusCode.resourceExhausted);
    // Another connection has its own bound.
    const other = await c.dial(relay.uri, { receiver: 'relay-1' });
    const third = openRelayed(other, hold);
    await until(() => worker.seen.length === 3);
    // One ends: room again.
    await first.close();
    await until(() => worker.seen[0]?.channel.state !== 'open');
    const fourth = openRelayed(connection, hold);
    await until(() => worker.seen.length === 4);
    for (const channel of [second, third, fourth]) {
      expect(channel.state).toBe('open');
    }
    expect(() => new RelayService(relay.node, { maxChannelsPerConnection: -1 })).toThrow(
      RangeError,
    );
  });

  it('maxChannelsPerConnection 0: no bound', async () => {
    const mesh = new FakeMesh();
    const relay = await Relay.start(mesh, { maxChannelsPerConnection: 0 });
    const worker = Worker.start(mesh);
    const c = newNode({ credential: issue('consumer-1', consumerScopes) });
    const connection = await relayConnection(c, relay.uri);
    const hold = innerOf(workerType, worker.id, 'hold');
    const channels = Array.from({ length: 5 }, () => openRelayed(connection, hold));
    await until(() => worker.seen.length === 5);
    for (const channel of channels) {
      expect(channel.state).toBe('open');
    }
  });

  it('no record: NOT_FOUND; a record with endpoints: FAILED_PRECONDITION, relayed with allowEndpoints', async () => {
    const mesh = new FakeMesh();
    const relay = await Relay.start(mesh);
    const worker = Worker.start(mesh);
    const c = newNode({ credential: issue('consumer-1', consumerScopes) });
    const connection = await relayConnection(c, relay.uri);
    expectCode(
      await refusal(connection, innerOf(workerType, worker.id + 100)),
      StatusCode.notFound,
    );
    expectCode(await refusal(connection, innerOf('worker-z')), StatusCode.notFound);
    // A worker that listens.
    const lister = newNode({ credential: issue('worker-b2', workerScopes) });
    const listerUri = await lister.listenMemory({ policy: ChannelPolicies.scoped() });
    const listedType = Name.of('worker-l');
    const listed = mesh.publish(
      lister,
      listedType,
      (incoming) => {
        void (async () => {
          const frames = new Frames(incoming.channel);
          while ((await frames.next()) !== undefined) {
            void incoming.channel.send(bytes(`listed for ${incoming.peerIdentity?.identity}`));
          }
        })();
      },
      { endpoints: [listerUri] },
    );
    const listedAddress = innerOf(listedType, listed);
    expectCode(await refusal(connection, listedAddress), StatusCode.failedPrecondition);
    // Any instance of the type: only records with endpoints, also FAILED_PRECONDITION.
    expectCode(await refusal(connection, innerOf(listedType)), StatusCode.failedPrecondition);
    const open = await Relay.start(mesh, { identity: 'relay-2', allowEndpoints: true });
    const openConnection = await c.dial(open.uri, { receiver: 'relay-2' });
    const channel = openRelayed(openConnection, listedAddress);
    await channel.send(bytes('L'));
    expect(await nextText(channel)).toBe('listed for relay-2');
    await channel.close();
  });

  it("with allowEndpoints, a record pointing back at the relay's own listener: UNAVAILABLE, never connected to", async () => {
    const mesh = new FakeMesh();
    const { logger, messages } = logged('warn', null);
    const relay = await Relay.start(mesh, { allowEndpoints: true, logger });
    // A record of a worker claiming to listen where the relay does.
    const misdirected = newNode({ credential: issue('worker-c3', workerScopes) });
    const selfType = Name.of('worker-s');
    const id = mesh.publish(misdirected, selfType, () => {}, { endpoints: [relay.uri] });
    const c = newNode({ credential: issue('consumer-1', consumerScopes) });
    const connection = await relayConnection(c, relay.uri);
    const connections = collect(relay.node);
    expectCode(await refusal(connection, innerOf(selfType, id)), StatusCode.unavailable);
    expect(connections).toHaveLength(0);
    expect(messages.filter((m) => m.includes('is this node itself'))).toHaveLength(1);
  });

  it('a relay without `broker`: UNAVAILABLE', async () => {
    const mesh = new FakeMesh();
    const relay = await Relay.start(mesh, {
      scopes: [
        [Right.register, '_relay'],
        [Right.watch, '*'],
        [Right.open, 'worker-*'],
      ],
    });
    const worker = Worker.start(mesh);
    const c = await consumer(mesh);
    const channel = await c.openChannel(worker.address);
    expectCode(await channel.done, StatusCode.unavailable);
    expect(worker.brokeredConnections).toHaveLength(0);
    expect(relay.uri).toBeDefined();
  });

  it("a relay without `open` for the type: the worker's brokered policy refuses, and the refusal is relayed", async () => {
    const mesh = new FakeMesh();
    await Relay.start(mesh, {
      scopes: [
        [Right.register, '_relay'],
        [Right.watch, '*'],
        [Right.broker, 'worker-*'],
      ],
    });
    const worker = Worker.start(mesh);
    const c = await consumer(mesh);
    const channel = await c.openChannel(worker.address);
    expectCode(await channel.done, StatusCode.permissionDenied);
    expect(worker.seen).toHaveLength(0);
    expect(worker.brokeredConnections).toHaveLength(1);
  });
});

describe('PeerSet', () => {
  it('a peer without endpoints is connected through the relay, and comes back through the other relay when its relay goes', async () => {
    const mesh = new FakeMesh();
    const first = await Relay.start(mesh, { identity: 'relay-1' });
    const worker = Worker.start(mesh);
    const c = await consumer(mesh);
    const set = PeerSet.watch(c, workerType, {
      channel: new ChannelAddress({ type: workerType }),
      onOpen: async (_peer, channel) => {
        // A round trip confirms the channel reached the worker.
        await new TalkChannel(channel).request('HELLO');
      },
      policy: ChannelPolicies.denyAll,
      initialBackoff: 20,
      maxBackoff: 100,
    });
    onCleanup(() => set.close());
    const events: string[] = [];
    set.on('event', (e) => events.push(e.type));
    await until(() => set.online.length === 1);
    const peer = set.peers.get(worker.id) as Peer;
    expect(peer.viaRelay).toBe(true);
    expect(peer.endpoint).toBe(first.uri);
    const talk = await set.openTalk(worker.id);
    expect((await talk.request('J', 'j')).text()).toBe(`j by ${worker.id} for relay-1 with ""`);
    await talk.close();
    // A second relay; the first goes away.
    const second = await Relay.start(mesh, { identity: 'relay-2' });
    expect((await c.resolveRelays()).map((r) => r.endpoints[0])).toContain(second.uri);
    mesh.unpublish(first.address);
    await first.node.close();
    await until(() => peer.isOnline && peer.endpoint === second.uri);
    expect(events[0]).toBe('added');
    const order = ['online', 'offline', 'online'];
    let at = 0;
    for (const type of events) {
      if (type === order[at]) at++;
    }
    expect(at).toBe(order.length);
    const again = await set.openTalk(worker.id);
    expect((await again.request('J', 'k')).text()).toBe(`k by ${worker.id} for relay-2 with ""`);
    await again.close();
    // The worker's connection to the relay is lost: the channel ends, and
    // the peer is set up again through the relay.
    const brokered = worker.brokeredConnections;
    expect(brokered).toHaveLength(1);
    await (brokered[0] as MuxConnection).close();
    await until(() => !peer.isOnline);
    await until(() => peer.isOnline);
    expect(peer.viaRelay).toBe(true);
  });

  it('a node that can neither broker nor relay reports the brokering failure', async () => {
    const mesh = new FakeMesh();
    const worker = Worker.start(mesh);
    const c = await consumer(mesh, { useRelay: false });
    const set = PeerSet.watch(c, workerType, {
      channel: new ChannelAddress({ type: workerType }),
      policy: ChannelPolicies.denyAll,
      initialBackoff: 20,
      maxBackoff: 100,
    });
    onCleanup(() => set.close());
    await until(() => set.peers.get(worker.id)?.lastStatus !== undefined);
    const peer = set.peers.get(worker.id) as Peer;
    expect(peer.isOnline).toBe(false);
    expect(peer.viaRelay).toBe(false);
    expectCode(peer.lastStatus as Status, StatusCode.failedPrecondition);
  });
});
