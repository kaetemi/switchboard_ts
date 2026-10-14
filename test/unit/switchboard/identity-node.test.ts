/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { afterEach, describe, expect, test, vi } from 'vitest';
import { ChannelAddress, ServiceAddress } from '../../../src/core/address/index.ts';
import { utf8Decode, utf8Encode } from '../../../src/core/bytes.ts';
import { type Credential, Right } from '../../../src/core/identity/contract.ts';
import type { MuxChannel, MuxConnection } from '../../../src/core/mux/contract.ts';
import { Name, type NameLike } from '../../../src/core/name.ts';
import { serviceRecord } from '../../../src/core/naming/naming-protocol.ts';
import { StatusCode, SwitchboardError } from '../../../src/core/status.ts';
import { ChannelPolicies } from '../../../src/core/switchboard/channel-policy.ts';
import type {
  ChannelPolicy,
  ExpectedIdentity,
  IncomingChannel,
  Resolver,
  ServiceRecord,
} from '../../../src/core/switchboard/contract.ts';
import { type BrokeringResolver, StaticResolver } from '../../../src/core/switchboard/resolver.ts';
import { Switchboard } from '../../../src/core/switchboard/switchboard.ts';
import { memoryTransportPair } from '../../../src/core/transport/memory-transport.ts';
import type { Logger } from '../../../src/core/util/logger.ts';
import { FakeVerifier, fakeCredential, fakeHolderKey } from '../../support/node/fake-identity.ts';
import {
  codeOf,
  currentRuntime,
  node as harnessNode,
  pump,
  rejectsWith,
  until,
} from '../../support/node/harness.ts';

const npc = Name.of('npc');
const chat = Name.of('chat');
const naming = Name.of('_ns');

const npcKey = fakeHolderKey(new Uint8Array(32).fill(1));
const gateKey = fakeHolderKey(new Uint8Array(32).fill(2));
const workerKey = fakeHolderKey(new Uint8Array(32).fill(3));

/** `npc-7`, which may open `npc` and nothing else. */
function npcCredential(): Credential {
  return fakeCredential({
    identity: 'npc-7',
    holderKey: npcKey.publicKey,
    scopes: [[Right.open, 'npc']],
  });
}

/** A worker: may register its type, open nothing. */
function workerCredential(expiresAt = 0): Credential {
  return fakeCredential({
    identity: 'worker-a1',
    holderKey: workerKey.publicKey,
    scopes: [[Right.register, 'worker-*']],
    expiresAt,
  });
}

/** A credential no verifier knows (another authority's, or forged). */
function unknownCredential(): Credential {
  const genuine = npcCredential();
  return {
    ...genuine,
    isBearer: genuine.isBearer,
    holderKey: genuine.holderKey,
    allows: (right, name) => genuine.allows(right, name),
    isExpired: (now) => genuine.isExpired(now),
    encode: () => utf8Encode('forged'),
  };
}

interface NodeOptions {
  credential?: Credential;
  holderKey?: typeof npcKey;
  verify?: boolean;
  identifyOutgoing?: boolean;
  identifyFor?: (endpoint: string) => boolean;
  identityTimeout?: number;
  outgoingPolicy?: ChannelPolicy;
  resolver?: Resolver;
  requireNamedIdent?: boolean;
  expectedIdentityFor?: ExpectedIdentity;
  logger?: Logger;
}

/** A node with a verifier (unless [verify] is false), closed after the test. */
function node(options: NodeOptions = {}): Switchboard {
  return harnessNode({
    credential: options.credential,
    holderKey: options.holderKey,
    verifier: (options.verify ?? true) ? new FakeVerifier() : undefined,
    identifyOutgoing: options.identifyOutgoing,
    identifyFor: options.identifyFor,
    identityTimeout: options.identityTimeout ?? 5000,
    outgoingPolicy: options.outgoingPolicy,
    resolver: options.resolver,
    expectedIdentityFor: options.expectedIdentityFor,
    logger: options.logger,
    muxOptions: { requireNamedIdent: options.requireNamedIdent ?? false },
  });
}

/** A handler answering with the peer's identity and closing. */
function answer(incoming: IncomingChannel): void {
  void incoming.channel.send(
    utf8Encode(`${incoming.address.type} for ${incoming.peerIdentity?.identity ?? 'nobody'}`),
  );
  void incoming.channel.close();
}

/** What an {@link answer} handler said, or the close status. */
async function answerOf(channel: MuxChannel): Promise<string> {
  const frames: Uint8Array[] = [];
  for await (const frame of channel.readable) frames.push(frame);
  const status = await channel.done;
  return frames.length === 0 ? `closed ${codeOf(status)}` : utf8Decode(frames[0] as Uint8Array);
}

/** A server answering `npc`, `chat` and `_ns` on a memory listener with [policy]. */
async function server(
  options: NodeOptions & { policy?: ChannelPolicy } = {},
): Promise<{ s: Switchboard; uri: string }> {
  const s = node(options);
  for (const type of [npc, chat, naming]) {
    s.registerService(type, answer);
  }
  const uri = await s.listenMemory({ policy: options.policy });
  return { s, uri };
}

async function open(client: Switchboard, endpoint: string, type: NameLike): Promise<string> {
  return answerOf(await client.openChannelAt(endpoint, new ChannelAddress({ type })));
}

/** Collects the connections [s] reports from now on. */
function collect(s: Switchboard): MuxConnection[] {
  const seen: MuxConnection[] = [];
  void (async () => {
    for await (const connection of s.connections) seen.push(connection);
  })();
  return seen;
}

class BrokerResolver extends StaticResolver implements BrokeringResolver {
  readonly #onConnect: (endpoint: string, intent: Uint8Array) => Promise<string>;

  constructor(onConnect: (endpoint: string, intent: Uint8Array) => Promise<string>) {
    super();
    this.#onConnect = onConnect;
  }

  connectTo(_address: ServiceAddress, endpoint: string, intent: Uint8Array): Promise<string> {
    return this.#onConnect(endpoint, intent);
  }
}

afterEach(() => {
  vi.useRealTimers();
});

describe('brokering', () => {
  test('a pooled brokered connection is reused only while its peer carries the identity it was brokered as', async () => {
    vi.useFakeTimers({ toFake: ['Date'] });
    const credential = () => workerCredential(Math.floor(Date.now() / 1000) + 60);
    const worker = node({ credential: credential(), holderKey: workerKey });
    worker.registerService(npc, answer, { instance: 1 });
    const dialled: MuxConnection[] = [];
    let asked = 0;
    const resolver = new BrokerResolver(async (endpoint, intent) => {
      asked++;
      dialled.push(await worker.dial(endpoint, { intent }));
      return 'worker-a1';
    });
    resolver.add(serviceRecord(new ServiceAddress(npc, 1)));
    const consumer = node({ resolver });
    await consumer.listenMemory();
    const openNpc = async () => answerOf(await consumer.openChannel(new ServiceAddress(npc, 1)));
    expect(await openNpc()).toBe('npc for nobody');
    expect(dialled).toHaveLength(1);
    // Renewed in time: the connection is reused past the first expiry.
    vi.setSystemTime(Date.now() + 40_000);
    await worker.updateCredential(credential());
    vi.setSystemTime(Date.now() + 40_000);
    expect(await openNpc()).toBe('npc for nobody');
    expect(dialled).toHaveLength(1);
    // Not renewed (revoked): not reused. The instance is brokered again
    // (its dial-back fails, its credential having expired), and the
    // connection it had is sent GOAWAY.
    vi.setSystemTime(Date.now() + 120_000);
    await expect(consumer.openChannel(new ServiceAddress(npc, 1))).rejects.toBeInstanceOf(
      SwitchboardError,
    );
    expect(dialled).toHaveLength(1);
    expect(asked).toBe(2);
    expect(codeOf(await (dialled[0] as MuxConnection).done)).toBe('goingAway');
  });

  test('a dial-back arriving before CONNECT fails is sent GOAWAY', async () => {
    const worker = node({ credential: workerCredential(), holderKey: workerKey });
    let dialled: MuxConnection | undefined;
    const resolver = new BrokerResolver(async (endpoint, intent) => {
      dialled = await worker.dial(endpoint, { intent });
      // The worker identified, but its CONNECT reply is lost or rejected
      // before the naming service confirms the request.
      throw SwitchboardError.of(StatusCode.unavailable);
    });
    const consumer = node({ resolver });
    await consumer.listenMemory();
    await rejectsWith(consumer.broker(new ServiceAddress(npc, 1)), StatusCode.unavailable);
    expect(codeOf(await (dialled as MuxConnection).done)).toBe('goingAway');
  });

  test('broker identifies back, names the instance and is not pooled', async () => {
    const worker = node({ credential: workerCredential(), holderKey: workerKey });
    worker.registerService(npc, answer, { instance: 1 });
    const resolver = new BrokerResolver(async (endpoint, intent) => {
      await worker.dial(endpoint, { intent });
      return 'worker-a1';
    });
    const consumer = node({ resolver, credential: npcCredential(), holderKey: npcKey });
    await consumer.listenMemory();
    const connection = await consumer.broker('npc/1');
    expect(connection.isInitiator).toBe(false);
    expect(connection.peerIdentity?.identity).toBe('worker-a1');
    const back = consumer.openChannelOn(connection, new ChannelAddress({ type: npc, instance: 1 }));
    expect(await answerOf(back)).toBe('npc for npc-7');
    // Not pooled: another broker() asks again.
    const again = await consumer.broker(new ServiceAddress(npc, 1));
    expect(again).not.toBe(connection);
  });

  test('broker preconditions', async () => {
    const plain = node();
    await rejectsWith(plain.broker('npc/1'), StatusCode.failedPrecondition);
    const resolver = new BrokerResolver(async () => 'x');
    await rejectsWith(plain.broker('npc/1', { resolver }), StatusCode.failedPrecondition);
    const unverified = node({ verify: false, resolver });
    await unverified.listenMemory();
    await rejectsWith(unverified.broker('npc/1'), StatusCode.failedPrecondition);
    const consumer = node({ resolver });
    await rejectsWith(consumer.broker('npc'), StatusCode.invalidArgument);
    // Listens nowhere.
    await rejectsWith(consumer.broker('npc/1'), StatusCode.failedPrecondition);
  });

  test('an instance that never dials back is unavailable; its late connection is sent GOAWAY', async () => {
    const worker = node({ credential: workerCredential(), holderKey: workerKey });
    let request: { endpoint: string; intent: Uint8Array } | undefined;
    const resolver = new BrokerResolver(async (endpoint, intent) => {
      request = { endpoint, intent };
      return 'worker-a1';
    });
    const consumer = harnessNode({
      resolver,
      verifier: new FakeVerifier(),
      connectTimeout: 50,
    });
    await consumer.listenMemory();
    await rejectsWith(consumer.broker('npc/1'), StatusCode.unavailable);
    const { endpoint, intent } = request as { endpoint: string; intent: Uint8Array };
    const late = await worker.dial(endpoint, { intent });
    expect(codeOf(await late.done)).toBe('goingAway');
  });

  test('a connection that is not the instance is refused UNAUTHENTICATED', async () => {
    const worker = node({ credential: workerCredential(), holderKey: workerKey });
    const resolver = new BrokerResolver(async (endpoint, intent) => {
      await worker.dial(endpoint, { intent });
      return 'someone-else';
    });
    const consumer = node({ resolver });
    await consumer.listenMemory();
    await rejectsWith(consumer.broker('npc/1'), StatusCode.unauthenticated);
  });
});

describe('over mem', () => {
  test('mutual identity', async () => {
    const { s: b, uri } = await server({
      credential: fakeCredential({
        identity: 'gate',
        holderKey: gateKey.publicKey,
        scopes: [[Right.open, '*']],
      }),
      holderKey: gateKey,
    });
    // The server identifies on the connections whose peers ask.
    const accepted: MuxConnection[] = [];
    void (async () => {
      for await (const connection of b.connections) {
        accepted.push(connection);
        void connection.identityRequested.then(() => b.identifyOn(connection));
      }
    })();
    const a = node({ credential: npcCredential(), holderKey: npcKey });
    const connection = await a.connect(uri);
    // connect() returned after the server confirmed the IDENT.
    expect(accepted[0]?.peerIdentity?.identity).toBe('npc-7');
    expect(accepted[0]?.peerIdentity?.provedPossession).toBe(true);
    const gate = await connection.peerIdentified;
    expect(gate.identity).toBe('gate');
    expect(connection.localIdentity).toBe('npc-7');
    expect(await open(a, uri, npc)).toBe('npc for npc-7');
  });

  test('a scoped policy admits open-scoped types only', async () => {
    const { uri } = await server({ policy: ChannelPolicies.scoped() });
    const a = node({ credential: npcCredential(), holderKey: npcKey });
    expect(await open(a, uri, npc)).toBe('npc for npc-7');
    expect(await open(a, uri, chat)).toBe('closed permissionDenied');
    // The naming service checks the credential itself.
    expect(await open(a, uri, naming)).toBe('_ns for npc-7');
  });

  test('a worker, identified, can open nothing', async () => {
    const { uri } = await server({ policy: ChannelPolicies.scoped() });
    const w = node({ credential: workerCredential(), holderKey: workerKey });
    expect(await open(w, uri, npc)).toBe('closed permissionDenied');
    expect(await open(w, uri, chat)).toBe('closed permissionDenied');
    expect(await open(w, uri, naming)).toBe('_ns for worker-a1');
  });

  test('a bearer credential works', async () => {
    const { uri } = await server({ policy: ChannelPolicies.scoped() });
    const a = node({
      credential: fakeCredential({ identity: 'npc-8', scopes: [[Right.open, 'npc']] }),
    });
    expect(await open(a, uri, npc)).toBe('npc for npc-8');
    expect(await open(a, uri, chat)).toBe('closed permissionDenied');
  });

  test('a credential of another authority fails the connect with UNAUTHENTICATED', async () => {
    const { s: b, uri } = await server({ policy: ChannelPolicies.scoped() });
    const ended: Promise<unknown>[] = [];
    const accepted = collect(b);
    const a = node({ credential: unknownCredential(), holderKey: npcKey });
    await rejectsWith(a.connect(uri), StatusCode.unauthenticated);
    await rejectsWith(
      a.openChannelAt(uri, new ChannelAddress({ type: npc })),
      StatusCode.unauthenticated,
    );
    await until(() => accepted.length === 2);
    for (const connection of accepted) ended.push(connection.done);
    for (const done of ended) {
      expect(codeOf((await done) as never)).toBe('unauthenticated');
    }
  });

  test('a forged IDENT gets GOAWAY UNAUTHENTICATED', async () => {
    const { uri } = await server();
    const a = node({ credential: unknownCredential(), holderKey: npcKey });
    const dialled = collect(a);
    await rejectsWith(a.connect(uri), StatusCode.unauthenticated);
    const connection = dialled[0] as MuxConnection;
    expect(codeOf(await connection.peerGoAwayStatus)).toBe('unauthenticated');
    expect(codeOf(await connection.done)).toBe('goingAway');
  });
});

describe('holding', () => {
  test('a channel arriving before IDENT is held, then admitted', async () => {
    const { uri } = await server({ policy: ChannelPolicies.scoped() });
    const a = node({ credential: npcCredential(), holderKey: npcKey, identifyOutgoing: false });
    const connection = await a.connect(uri);
    const channel = a.openChannelOn(connection, new ChannelAddress({ type: npc }));
    const answered = answerOf(channel);
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(channel.state).toBe('open');
    await a.identifyOn(connection);
    expect(await answered).toBe('npc for npc-7');
    // Held channels are evaluated again: a type out of scope is refused
    // once the peer has identified.
    expect(await answerOf(a.openChannelOn(connection, new ChannelAddress({ type: chat })))).toBe(
      'closed permissionDenied',
    );
  });

  test('a channel held past identityTimeout is refused UNAUTHENTICATED', async () => {
    const { uri } = await server({ policy: ChannelPolicies.scoped(), identityTimeout: 100 });
    const a = node();
    const start = Date.now();
    expect(await open(a, uri, npc)).toBe('closed unauthenticated');
    expect(Date.now() - start).toBeGreaterThanOrEqual(90);
  });

  test('without an identityTimeout nothing is held', async () => {
    const { uri } = await server({
      policy: ChannelPolicies.requireIdentity(),
      identityTimeout: 0,
    });
    const a = node();
    expect(await open(a, uri, npc)).toBe('closed unauthenticated');
  });

  test('a held channel the peer closes is dropped', async () => {
    const { uri } = await server({ policy: ChannelPolicies.scoped() });
    const a = node();
    const connection = await a.connect(uri);
    const channel = a.openChannelOn(connection, new ChannelAddress({ type: npc }));
    await new Promise((resolve) => setTimeout(resolve, 20));
    await channel.close();
    expect(codeOf(await channel.done)).toBe('ok');
  });

  test('several held channels are admitted by one identification', async () => {
    const { uri } = await server({ policy: ChannelPolicies.scoped() });
    const a = node({ credential: npcCredential(), holderKey: npcKey, identifyOutgoing: false });
    const connection = await a.connect(uri);
    const answers = [npc, npc, chat].map((type) =>
      answerOf(a.openChannelOn(connection, new ChannelAddress({ type }))),
    );
    await pump();
    await a.identifyOn(connection);
    expect(await Promise.all(answers)).toEqual([
      'npc for npc-7',
      'npc for npc-7',
      'closed permissionDenied',
    ]);
  });
});

describe('policies', () => {
  test('requireIdentity, combined with denyReserved', async () => {
    const { uri } = await server({
      policy: ChannelPolicies.all([
        ChannelPolicies.denyReserved,
        ChannelPolicies.requireIdentity(),
      ]),
    });
    const a = node({ credential: npcCredential(), holderKey: npcKey });
    expect(await open(a, uri, chat)).toBe('chat for npc-7');
    expect(await open(a, uri, naming)).toBe('closed permissionDenied');
  });

  test('scoped without requireIdentity lets unidentified peers pass', async () => {
    const { uri } = await server({ policy: ChannelPolicies.scoped({ requireIdentity: false }) });
    const anonymous = node();
    expect(await open(anonymous, uri, chat)).toBe('chat for nobody');
    const a = node({ credential: npcCredential(), holderKey: npcKey });
    expect(await open(a, uri, chat)).toBe('closed permissionDenied');
  });

  test('a worker is refused in the other direction too', async () => {
    // The consumer dials the worker and identifies; the worker identifies
    // when asked; its channels back are checked by the consumer's
    // outgoing policy.
    const w = node({ credential: workerCredential(), holderKey: workerKey });
    const pushed: Promise<string>[] = [];
    void (async () => {
      for await (const connection of w.connections) {
        void connection.identityRequested.then(async () => {
          await w.identifyOn(connection);
          pushed.push(answerOf(w.openChannelOn(connection, new ChannelAddress({ type: npc }))));
        });
      }
    })();
    const workerUri = await w.listenMemory();
    const consumer = node({
      credential: npcCredential(),
      holderKey: npcKey,
      outgoingPolicy: ChannelPolicies.scoped(),
    });
    consumer.registerService(npc, answer);
    const connection = await consumer.connect(workerUri);
    expect((await connection.peerIdentified).identity).toBe('worker-a1');
    await until(() => pushed.length > 0);
    expect(await pushed[0]).toBe('closed permissionDenied');
  });
});

describe('identifying', () => {
  test('identifyFor chooses the endpoints the node identifies to', async () => {
    const { uri: mesh } = await server();
    const { uri: outside } = await server();
    const asked: string[] = [];
    const a = node({
      credential: npcCredential(),
      holderKey: npcKey,
      identifyFor: (endpoint) => {
        asked.push(endpoint);
        return endpoint === mesh;
      },
    });
    expect(await open(a, mesh, npc)).toBe('npc for npc-7');
    expect(await open(a, outside, npc)).toBe('npc for nobody');
    expect(asked).toEqual([mesh, outside]);
  });

  test('identifyOutgoing false, and a hook that throws', async () => {
    const { uri } = await server();
    const quiet = node({ credential: npcCredential(), holderKey: npcKey, identifyOutgoing: false });
    expect(await open(quiet, uri, npc)).toBe('npc for nobody');
    const failing = node({
      credential: npcCredential(),
      holderKey: npcKey,
      identifyFor: () => {
        throw new Error('no');
      },
    });
    expect(await open(failing, uri, npc)).toBe('npc for nobody');
  });

  test('a peer without a verifier ignores the IDENT', async () => {
    const b = node({ verify: false });
    b.registerService(npc, answer);
    const uri = await b.listenMemory();
    const a = node({ credential: npcCredential(), holderKey: npcKey });
    expect(await open(a, uri, npc)).toBe('npc for nobody');
  });

  test('a peer that never answers fails the connect', async () => {
    // A peer that reads and ignores every frame.
    const runtime = currentRuntime();
    const a = harnessNode({
      credential: npcCredential(),
      holderKey: npcKey,
      identityTimeout: 100,
      runtime: {
        ...runtime,
        dialers: {
          tcp: async () => {
            const [local, remote] = memoryTransportPair({ delay: 0 });
            void remote.readable.pipeTo(new WritableStream()).catch(() => {});
            return local;
          },
        },
      },
    });
    const e = await rejectsWith(a.connect('tcp://127.0.0.1:9'), StatusCode.unauthenticated);
    expect(e.status.reason).toContain('deadline');
  });

  test('the credential can be replaced; identifyOn presents it', async () => {
    const { s: b, uri } = await server({ policy: ChannelPolicies.scoped() });
    const accepted = collect(b);
    const a = node({ credential: npcCredential(), holderKey: npcKey });
    const connection = await a.connect(uri);
    a.credential = fakeCredential({
      identity: 'npc-7',
      holderKey: npcKey.publicKey,
      scopes: [
        [Right.open, 'npc'],
        [Right.open, 'chat'],
      ],
    });
    expect(await open(a, uri, chat)).toBe('closed permissionDenied');
    await a.identifyOn(connection);
    expect(accepted[0]?.peerIdentity?.scopes).toHaveLength(2);
    expect(await open(a, uri, chat)).toBe('chat for npc-7');
    expect(connection.localIdentity).toBe('npc-7');
    const other = workerCredential();
    expect(() => {
      a.credential = other;
    }).toThrow(RangeError);
    expect(() => new Switchboard({ credential: other })).toThrow(RangeError);
    expect(() => new Switchboard({ credential: other, holderKey: npcKey })).toThrow(RangeError);
    expect(() => new Switchboard().identifyOn(connection)).toThrow(Error);
    // Cleared: connections lose the local identity.
    a.credential = undefined;
    expect(connection.localIdentity).toBeUndefined();
  });

  test('expectedIdentityFor names the receiver', async () => {
    const { uri } = await server({
      credential: fakeCredential({ identity: 'gate', holderKey: gateKey.publicKey }),
      holderKey: gateKey,
      requireNamedIdent: true,
    });
    const asked: [string, ServiceRecord | undefined][] = [];
    const client = (expected: string | undefined) =>
      node({
        credential: npcCredential(),
        holderKey: npcKey,
        expectedIdentityFor: (endpoint, record) => {
          asked.push([endpoint, record]);
          return expected;
        },
      });
    const named = client('gate');
    expect(await open(named, uri, npc)).toBe('npc for npc-7');
    expect(asked).toEqual([[uri, undefined]]);
    // Another receiver, or none at a host that requires one: refused.
    for (const expected of ['other', undefined]) {
      await rejectsWith(client(expected).connect(uri), StatusCode.unauthenticated);
    }
    // An explicit receiver wins over the hook.
    const explicit = client('other');
    const connection = await explicit.dial(uri, { receiver: 'gate' });
    expect(connection.isOpen).toBe(true);
    // A hook that throws fails the connection (see below).
    const failing = node({
      credential: npcCredential(),
      holderKey: npcKey,
      expectedIdentityFor: () => {
        throw new Error('no');
      },
    });
    await rejectsWith(failing.connect(uri), StatusCode.internal);
  });

  test('an expectedIdentityFor that throws fails closed', async () => {
    // A host that accepts an IDENT naming nobody: the hook's failure must
    // not fall back to one.
    const { s: lenient, uri } = await server({
      credential: fakeCredential({ identity: 'gate', holderKey: gateKey.publicKey }),
      holderKey: gateKey,
    });
    const accepted = collect(lenient);
    const warnings: { message: string; error: unknown }[] = [];
    const logger: Logger = {
      log: (level, _name, message, error) => {
        if (level === 'warn' || level === 'error') warnings.push({ message, error });
      },
    };
    const sealed = () => {
      throw new TypeError('vault sealed');
    };
    const failing = node({
      credential: npcCredential(),
      holderKey: npcKey,
      expectedIdentityFor: sealed,
      logger,
    });
    const expectFailure = async (promise: Promise<unknown>) => {
      const e = await rejectsWith(promise, StatusCode.internal);
      expect(e.status.reason).toContain('expectedIdentityFor');
      expect(e.status.reason).not.toContain('vault sealed');
    };
    await expectFailure(failing.connect(uri));
    await expectFailure(failing.dial(uri));
    await expectFailure(failing.openChannelAt(uri, new ChannelAddress({ type: npc })));
    await until(() => accepted.length === 3);
    for (const connection of accepted) {
      expect(codeOf(await connection.done)).toBe('goingAway');
      expect(connection.peerIdentity).toBeUndefined();
    }
    const hook = warnings.filter((w) => w.message.includes('expectedIdentityFor'));
    expect(hook).toHaveLength(3);
    expect(hook.every((w) => w.error instanceof TypeError)).toBe(true);
    // An explicit receiver does not ask the hook.
    const explicit = await failing.dial(uri, { receiver: 'gate' });
    expect(explicit.isOpen).toBe(true);
    // identifyOn fails the same way, without an IDENT, and leaves the
    // connection as it is.
    const quiet = node({
      credential: npcCredential(),
      holderKey: npcKey,
      identifyFor: () => false,
      expectedIdentityFor: sealed,
    });
    const connection = await quiet.connect(uri);
    await expectFailure(quiet.identifyOn(connection));
    expect(connection.isOpen).toBe(true);
    expect(await open(quiet, uri, npc)).toBe('npc for nobody');
  });

  test('dial with an intent and a receiver; updateCredential presents the renewed credential where the node identified', async () => {
    const { s: b, uri: bUri } = await server({
      credential: fakeCredential({ identity: 'gate', holderKey: gateKey.publicKey }),
      holderKey: gateKey,
    });
    const { s: c, uri: cUri } = await server({ policy: ChannelPolicies.scoped() });
    const atB = collect(b);
    const atC = collect(c);
    const a = node({
      credential: npcCredential(),
      holderKey: npcKey,
      identifyFor: (endpoint) => endpoint === bUri,
    });
    const plain = node();
    await rejectsWith(
      plain.dial(bUri, { intent: new Uint8Array(1) }),
      StatusCode.failedPrecondition,
    );
    expect(() => a.dial(bUri, { intent: new Uint8Array(65) })).toThrow(RangeError);
    const toB = await a.dial(bUri, { intent: Uint8Array.of(7), receiver: 'gate' });
    // identifyFor says no for c: not identified there.
    const toC = await a.connect(cUri);
    await until(() => atB.length === 1 && atC.length === 1);
    const identified = (atB[0] as MuxConnection).peerIdentity;
    expect(identified?.intent).toEqual(Uint8Array.of(7));
    expect(identified?.receiver).toBe('gate');
    const renewed = fakeCredential({
      identity: 'npc-7',
      holderKey: npcKey.publicKey,
      scopes: [
        [Right.open, 'npc'],
        [Right.open, 'chat'],
      ],
    });
    await a.updateCredential(renewed);
    expect(a.credential).toBe(renewed);
    const after = (atB[0] as MuxConnection).peerIdentity;
    expect(after?.credential).toBe(renewed);
    expect(after?.intent).toEqual(Uint8Array.of(7));
    expect(after?.receiver).toBe('gate');
    expect((atC[0] as MuxConnection).peerIdentity).toBeUndefined();
    expect(toB.isOpen && toC.isOpen).toBe(true);
    expect(toB.localIdentity).toBe('npc-7');
  });

  test('updateCredential while the node is identifying on a connection: the renewed credential is presented there once done', async () => {
    const { s: b, uri } = await server();
    const accepted = collect(b);
    const renewed = fakeCredential({
      identity: 'npc-7',
      holderKey: npcKey.publicKey,
      scopes: [
        [Right.open, 'npc'],
        [Right.open, 'chat'],
      ],
    });
    // A new pooled connection: connect identifies on it before returning it.
    const a = node({ credential: npcCredential(), holderKey: npcKey });
    const connecting = a.connect(uri);
    await a.updateCredential(renewed);
    await connecting;
    expect(accepted[0]?.peerIdentity?.credential).toBe(renewed);
    // identifyOn.
    const quiet = node({
      credential: npcCredential(),
      holderKey: npcKey,
      identifyFor: () => false,
    });
    const connection = await quiet.connect(uri);
    await until(() => accepted.length === 2);
    expect(accepted[1]?.peerIdentity).toBeUndefined();
    const identifying = quiet.identifyOn(connection);
    await quiet.updateCredential(renewed);
    await identifying;
    expect(accepted[1]?.peerIdentity?.credential).toBe(renewed);
  });

  test('an open through the resolver tells the hook its record', async () => {
    const gate = node({
      credential: fakeCredential({ identity: 'gate', holderKey: gateKey.publicKey }),
      holderKey: gateKey,
      requireNamedIdent: true,
    });
    gate.registerService(npc, answer, { acceptAnyInstance: true });
    const uri = await gate.listenMemory();
    const records: (ServiceRecord | undefined)[] = [];
    const client = node({
      credential: npcCredential(),
      holderKey: npcKey,
      resolver: new StaticResolver([
        serviceRecord(new ServiceAddress(npc, 5), { endpoints: [uri] }),
      ]),
      expectedIdentityFor: (_endpoint, record) => {
        records.push(record);
        return 'gate';
      },
    });
    expect(await answerOf(await client.openChannel(npc))).toBe('npc for npc-7');
    // A second open reuses the pooled connection: no new identification.
    expect(await answerOf(await client.openChannel(new ServiceAddress(npc, 5)))).toBe(
      'npc for npc-7',
    );
    expect(records.map((r) => r?.address.toString())).toEqual(['npc/5']);
  });
});
