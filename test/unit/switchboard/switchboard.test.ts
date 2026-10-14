/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { describe, expect, test } from 'vitest';
import {
  ChannelAddress,
  ServiceAddress,
  type ServiceAddressLike,
  toServiceAddress,
} from '../../../src/core/address/index.ts';
import { utf8Decode } from '../../../src/core/bytes.ts';
import type { MuxChannel, MuxConnection } from '../../../src/core/mux/contract.ts';
import { Name } from '../../../src/core/name.ts';
import { serviceRecord } from '../../../src/core/naming/naming-protocol.ts';
import { Status, StatusCode, SwitchboardError } from '../../../src/core/status.ts';
import { ChannelPolicies } from '../../../src/core/switchboard/channel-policy.ts';
import type { IncomingChannel } from '../../../src/core/switchboard/contract.ts';
import { EndpointResolver, StaticResolver } from '../../../src/core/switchboard/resolver.ts';
import { Switchboard } from '../../../src/core/switchboard/switchboard.ts';
import { memoryTransportPair } from '../../../src/core/transport/memory-transport.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';
import {
  bytes,
  codeOf,
  currentRuntime,
  fast,
  first,
  node,
  onCleanup,
  pump,
  rawPeer,
  rejectsWith,
  svc,
  svcAddress,
  tagged,
  tagOf,
  until,
} from '../../support/node/harness.ts';
import type { ToyMuxConnection } from '../../support/node/toy-mux.ts';

/** Makes the server side send GOAWAY and waits until [connection] has seen it. */
async function peerGoesAway(connection: MuxConnection, serverSide: MuxConnection): Promise<void> {
  void serverSide.goAway();
  await until(() => connection.peerGoingAway);
}

/** Collects the connections [server] reports, from now on. */
function collect(server: Switchboard): MuxConnection[] {
  const seen: MuxConnection[] = [];
  void (async () => {
    for await (const connection of server.connections) {
      seen.push(connection);
    }
  })();
  return seen;
}

describe('construction', () => {
  test('validates its options', () => {
    for (const max of [0, 65537, 1.5]) {
      expect(() => new Switchboard({ maxConnectionsPerEndpoint: max })).toThrow(RangeError);
    }
    expect(() => new Switchboard({ connectTimeout: -1 })).toThrow(RangeError);
    expect(() => new Switchboard({ identityTimeout: -1 })).toThrow(RangeError);
    expect(new Switchboard({ maxConnectionsPerEndpoint: 65536 }).maxConnectionsPerEndpoint).toBe(
      65536,
    );
  });

  test('defaults', () => {
    const s = new Switchboard();
    expect(s.connectTimeout).toBe(10_000);
    expect(s.identityTimeout).toBe(10_000);
    expect(s.maxConnectionsPerEndpoint).toBe(4);
    expect(s.selection).toBe('roundRobin');
    expect(s.localConnections).toBe(true);
    expect(s.identifyOutgoing).toBe(true);
    expect(s.defaultPayload).toHaveLength(0);
    expect(s.listeningEndpoints).toEqual([]);
    expect(s.canBroker).toBe(false);
  });

  test('the node clock and logger reach the mux options unless they give their own', () => {
    const logged: string[] = [];
    const logger = { log: (_level: string, name: string) => void logged.push(name) };
    const clock = { now: () => 0, setTimeout: () => ({}), clearTimeout: () => {} };
    const s = new Switchboard({ clock, logger });
    expect(s.muxOptions.clock).toBe(clock);
    expect(s.muxOptions.logger).toBe(logger);
    const own = { now: () => 1, setTimeout: () => ({}), clearTimeout: () => {} };
    const t = new Switchboard({ clock, muxOptions: { clock: own } });
    expect(t.clock).toBe(clock);
    expect(t.muxOptions.clock).toBe(own);
    const u = new Switchboard({ muxOptions: { clock: own } });
    expect(u.clock).toBe(own);
  });

  test('a string or name is a service address', () => {
    const cases: [ServiceAddressLike, string][] = [
      ['echo', 'echo'],
      ['npc/1a2b', 'npc/1a2b'],
      [Name.of('npc'), 'npc'],
      [new ServiceAddress('npc', 5), 'npc/5'],
    ];
    for (const [like, text] of cases) {
      expect(toServiceAddress(like).toString()).toBe(text);
    }
    expect(() => toServiceAddress('npc/xyz')).toThrow(RangeError);
  });
});

describe('listening', () => {
  test('memory listeners are listed until close', async () => {
    const server = node();
    const a = await server.listenMemory({ name: 'Alpha' });
    const b = await server.listenMemory();
    expect(a).toBe('mem://alpha');
    expect(b).toMatch(/^mem:\/\/sb\d+-[0-9a-f]{6}$/);
    expect(server.listeningEndpoints).toEqual([a, b]);
    expect(server.isOwnEndpoint('mem://ALPHA')).toBe(true);
    expect(server.canBroker).toBe(true);
    await rejectsWith(server.listenMemory({ name: 'alpha' }), StatusCode.alreadyExists);
    await expect(server.listenMemory({ name: 'no spaces' })).rejects.toThrow(RangeError);
    await server.close();
    expect(server.listeningEndpoints).toEqual([]);
    expect(server.isOwnEndpoint(a)).toBe(false);
    await rejectsWith(server.listenMemory(), StatusCode.failedPrecondition);
    // The id is free again.
    const other = node();
    expect(await other.listenMemory({ name: 'alpha' })).toBe('mem://alpha');
  });
});

describe('dispatch', () => {
  async function setUp(): Promise<{ server: Switchboard; client: Switchboard; uri: string }> {
    const server = node();
    const uri = await server.listenMemory();
    return { server, client: node(), uri };
  }

  test('exact instance', async () => {
    const { server, client, uri } = await setUp();
    server.registerService(svc, tagged('five'), { instance: 5 });
    server.registerService(svc, tagged('six'), { instance: 6 });
    expect(await tagOf(await client.openChannelAt(uri, svcAddress({ instance: 6 })))).toBe('six');
    expect(await tagOf(await client.openChannelAt(uri, svcAddress({ instance: 5 })))).toBe('five');
  });

  test('any instance goes to the first registered', async () => {
    const { server, client, uri } = await setUp();
    const open = async (address: ChannelAddress) => tagOf(await client.openChannelAt(uri, address));
    server.registerService(svc, tagged('six'), { instance: 6 });
    server.registerService(svc, tagged('five'), { instance: 5 });
    expect(await open(svcAddress())).toBe('six');
    // An exact registration for instance 0 wins.
    server.registerService('svc', tagged('any'));
    expect(await open(svcAddress())).toBe('any');
    // Re-registering keeps the position but replaces the handler.
    server.unregisterService(svc);
    server.registerService(svc, tagged('six again'), { instance: 6 });
    expect(await open(svcAddress())).toBe('six again');
  });

  test('unknown instance or type is not found', async () => {
    const { server, client, uri } = await setUp();
    const open = async (address: ChannelAddress) => tagOf(await client.openChannelAt(uri, address));
    server.registerService(svc, tagged('five'), { instance: 5 });
    server.registerService(svc, tagged('any'));
    expect(await open(svcAddress({ instance: 7 }))).toBe('closed notFound');
    expect(await open(new ChannelAddress({ type: 'other' }))).toBe('closed notFound');
    server.unregisterService(svc, 5);
    expect(await open(svcAddress({ instance: 5 }))).toBe('closed notFound');
    // Unregistering what is not there is harmless.
    server.unregisterService('nothing', 3);
  });

  test('acceptAnyInstance serves instances no registration matches', async () => {
    const { server, client, uri } = await setUp();
    const open = async (address: ChannelAddress) => tagOf(await client.openChannelAt(uri, address));
    server.registerService(svc, tagged('five'), { instance: 5 });
    server.registerService(svc, tagged('worker'), { instance: 9, acceptAnyInstance: true });
    expect(await open(svcAddress({ instance: 0x1234 }))).toBe('worker');
    expect(await open(svcAddress({ instance: 5 }))).toBe('five');
    // Before the rule for instance 0.
    expect(await open(svcAddress())).toBe('worker');
    // Re-registering without the flag removes it.
    server.registerService(svc, tagged('worker'), { instance: 9 });
    expect(await open(svcAddress({ instance: 0x1234 }))).toBe('closed notFound');
    server.registerService(svc, tagged('worker'), { instance: 9, acceptAnyInstance: true });
    server.unregisterService(svc, 9);
    expect(await open(svcAddress({ instance: 0x1234 }))).toBe('closed notFound');
  });

  test('registration validates the instance', () => {
    const s = node();
    expect(() => s.registerService(svc, tagged('x'), { instance: -1 })).toThrow(RangeError);
    expect(() => s.registerService(svc, tagged('x'), { instance: 2 ** 48 })).toThrow(RangeError);
    expect(() => s.registerService('name too long', tagged('x'))).toThrow(RangeError);
  });

  test('default service for channels without a type', async () => {
    const { server, client, uri } = await setUp();
    const open = async (address: ChannelAddress) => tagOf(await client.openChannelAt(uri, address));
    server.registerService(svc, tagged('svc'));
    expect(await open(new ChannelAddress())).toBe('closed notFound');
    server.defaultService = tagged('default');
    expect(await open(new ChannelAddress())).toBe('default');
    // The minimal one-byte header and the empty open payload.
    const connection = await client.connect(uri);
    expect(await tagOf(connection.open(new Uint8Array(1)))).toBe('default');
    expect(await tagOf(connection.open(new Uint8Array(0)))).toBe('default');
    expect(await open(svcAddress())).toBe('svc');
  });

  test('catch-all takes what no service claims', async () => {
    const { server, client, uri } = await setUp();
    const open = async (address: ChannelAddress) => tagOf(await client.openChannelAt(uri, address));
    server.registerService(svc, tagged('five'), { instance: 5 });
    server.catchAll = tagged('catch-all');
    expect(await open(svcAddress({ instance: 7 }))).toBe('catch-all');
    expect(await open(new ChannelAddress({ type: 'other' }))).toBe('catch-all');
    expect(await open(new ChannelAddress())).toBe('catch-all');
    expect(await open(svcAddress({ instance: 5 }))).toBe('five');
    server.defaultService = tagged('default');
    expect(await open(new ChannelAddress())).toBe('default');
    server.catchAll = undefined;
    expect(await open(svcAddress({ instance: 7 }))).toBe('closed notFound');
  });

  test('malformed open payload is a channel protocol error', async () => {
    const { server, uri } = await setUp();
    server.registerService(svc, tagged('svc'));
    const raw = await node().connect(uri);
    const reserved = raw.open(Uint8Array.of(0x10));
    const truncated = raw.open(Uint8Array.of(0x01, 0x73, 0x76));
    expect(codeOf(await reserved.done)).toBe('protocolError');
    expect(codeOf(await truncated.done)).toBe('protocolError');
    // The connection itself is fine.
    expect(await tagOf(raw.open(svcAddress().encode()))).toBe('svc');
    expect(raw.isOpen).toBe(true);
  });

  test('a throwing handler closes the channel with INTERNAL', async () => {
    const { server, client, uri } = await setUp();
    server.registerService(svc, () => {
      throw new Error('sync');
    });
    server.registerService('async', async () => {
      await pump(1);
      throw new Error('async');
    });
    expect(await tagOf(await client.openChannelAt(uri, svcAddress()))).toBe('closed internal');
    expect(
      await tagOf(await client.openChannelAt(uri, new ChannelAddress({ type: 'async' }))),
    ).toBe('closed internal');
  });

  test('reject closes with the given status', async () => {
    const { server, client, uri } = await setUp();
    server.registerService(svc, (incoming) => {
      if (utf8Decode(incoming.address.payload) !== 'good') {
        void incoming.reject(Status.of(StatusCode.unauthenticated, 'bad'));
        return;
      }
      void incoming.channel.send(bytes('welcome'));
      void incoming.channel.close();
    });
    server.registerService('app', (incoming) => {
      void incoming.reject(new Status(300, 'app'));
    });
    const open = async (address: ChannelAddress) => tagOf(await client.openChannelAt(uri, address));
    expect(await open(svcAddress({ payload: 'bad' }))).toBe('closed unauthenticated');
    expect(await open(svcAddress({ payload: 'good' }))).toBe('welcome');
    const app = await client.openChannelAt(uri, new ChannelAddress({ type: 'app' }));
    const status = await app.done;
    expect(codeOf(status)).toBe('unknown');
    expect(status.reason).toBe('application status 300: app');
  });

  test('incoming channel exposes the header, payload and remote', async () => {
    const { server, client, uri } = await setUp();
    const seen = new Deferred<IncomingChannel>();
    server.registerService(
      svc,
      (incoming) => {
        seen.resolve(incoming);
        void incoming.channel.close();
      },
      { instance: 0x1a2b },
    );
    const own = await client.listenMemory({ name: 'client' });
    const channel = await client.openChannelAt(
      uri,
      new ChannelAddress({ type: svc, instance: 0x1a2b, shard: 42, payload: 'token' }),
    );
    const incoming = await seen.promise;
    expect(incoming.address.address?.equals(new ServiceAddress(svc, 0x1a2b))).toBe(true);
    expect(incoming.address.shard).toBe(42);
    expect(utf8Decode(incoming.address.payload)).toBe('token');
    // The accepting node sees a memory peer by its first memory endpoint.
    expect(incoming.remote).toBe(own);
    expect(incoming.connection.isInitiator).toBe(false);
    expect(incoming.channel.isLocallyOpened).toBe(false);
    expect(incoming.peerIdentity).toBeUndefined();
    expect(String(incoming)).toContain(own);
    expect(codeOf(await channel.done)).toBe('ok');
  });

  test('talk() fails until the Talk layer lands', async () => {
    const { server, client, uri } = await setUp();
    const failed = new Deferred<unknown>();
    server.registerService(svc, (incoming) => {
      try {
        incoming.talk();
      } catch (e) {
        failed.resolve(e);
      }
      void incoming.channel.close();
    });
    await (await client.openChannelAt(uri, svcAddress())).done;
    const error = (await failed.promise) as SwitchboardError;
    expect(error.is(StatusCode.unimplemented)).toBe(true);
  });
});

describe('listener policy', () => {
  const ns = Name.of('_ns');
  const secret = Name.of('secret');

  function setUp(): { server: Switchboard; client: Switchboard } {
    const server = node();
    server.registerService(ns, tagged('naming'), { instance: 1 });
    server.registerService(svc, tagged('svc'));
    server.registerService(secret, tagged('secret'));
    server.catchAll = tagged('catch-all');
    return { server, client: node() };
  }

  test('a policy is applied before every handler', async () => {
    const { server, client } = setUp();
    const open = async (uri: string, address: ChannelAddress) => client.openChannelAt(uri, address);
    const pub = await server.listenMemory({ policy: ChannelPolicies.allowTypes([svc]) });
    const internal = await server.listenMemory();
    for (const address of [
      new ChannelAddress({ type: ns, instance: 1 }),
      new ChannelAddress({ type: ns }),
      new ChannelAddress({ type: secret }),
      new ChannelAddress({ type: 'other' }),
      new ChannelAddress(),
    ]) {
      const status = await (await open(pub, address)).done;
      expect(codeOf(status), String(address)).toBe('permissionDenied');
      // Generic: nothing about the node or the address.
      expect(status.reason).toBe('permission denied');
    }
    expect(await tagOf(await open(pub, svcAddress()))).toBe('svc');
    // The internal listener has no policy: everything is reachable.
    for (const [address, tag] of [
      [new ChannelAddress({ type: ns, instance: 1 }), 'naming'],
      [new ChannelAddress({ type: secret }), 'secret'],
      [new ChannelAddress({ type: 'other' }), 'catch-all'],
      [svcAddress(), 'svc'],
    ] as const) {
      expect(await tagOf(await open(internal, address))).toBe(tag);
    }
  });

  test('denyReserved', async () => {
    const { server, client } = setUp();
    const uri = await server.listenMemory({ policy: ChannelPolicies.denyReserved });
    const refused = await client.openChannelAt(uri, new ChannelAddress({ type: ns, instance: 1 }));
    expect(codeOf(await refused.done)).toBe('permissionDenied');
    expect(await tagOf(await client.openChannelAt(uri, svcAddress()))).toBe('svc');
    expect(await tagOf(await client.openChannelAt(uri, new ChannelAddress({ type: 'x' })))).toBe(
      'catch-all',
    );
  });

  test('the policy sees the address and connection; a throwing policy refuses', async () => {
    const { server, client } = setUp();
    const seen: [ChannelAddress, MuxConnection][] = [];
    const uri = await server.listenMemory({
      policy: (address, connection) => {
        seen.push([address, connection]);
        if (address.type?.equals(secret)) {
          throw new Error('broken policy');
        }
        return true;
      },
    });
    expect(await tagOf(await client.openChannelAt(uri, svcAddress({ payload: 't' })))).toBe('svc');
    expect(seen).toHaveLength(1);
    const [[address, connection]] = seen as [[ChannelAddress, MuxConnection]];
    expect(address.address?.equals(new ServiceAddress(svc))).toBe(true);
    expect(utf8Decode(address.payload)).toBe('t');
    expect(connection.isInitiator).toBe(false);
    const refused = await client.openChannelAt(uri, new ChannelAddress({ type: secret }));
    expect(codeOf(await refused.done)).toBe('permissionDenied');
  });

  test('a policy throwing a SwitchboardError refuses with its code, unless it cannot close', async () => {
    const { server, client } = setUp();
    let code: StatusCode = StatusCode.resourceExhausted;
    const uri = await server.listenMemory({
      policy: () => {
        throw SwitchboardError.of(code, 'internal details');
      },
    });
    const status = await (await client.openChannelAt(uri, svcAddress())).done;
    expect(codeOf(status)).toBe('resourceExhausted');
    expect(status.reason).toBe('resource exhausted');
    for (const odd of [StatusCode.ok, StatusCode.connectionLost]) {
      code = odd;
      const refused = await (await client.openChannelAt(uri, svcAddress())).done;
      expect(codeOf(refused)).toBe('permissionDenied');
    }
  });

  test('allowTypes with untyped channels', async () => {
    const { server, client } = setUp();
    server.defaultService = tagged('default');
    const uri = await server.listenMemory({
      policy: ChannelPolicies.allowTypes([svc], { untyped: true }),
    });
    expect(await tagOf(await client.openChannelAt(uri, new ChannelAddress()))).toBe('default');
    const refused = await client.openChannelAt(uri, new ChannelAddress({ type: secret }));
    expect(codeOf(await refused.done)).toBe('permissionDenied');
  });

  test('connections initiated locally are trusted', async () => {
    const { server, client } = setUp();
    // The client listens with a strict policy, yet the server reaches its
    // reserved service over the connection the client initiated.
    client.registerService(ns, tagged('client naming'));
    await client.listenMemory({ policy: ChannelPolicies.allowTypes([]) });
    const serverUri = await server.listenMemory({ policy: ChannelPolicies.denyReserved });
    const accepted = first(server.connections);
    await client.connect(serverUri);
    const back = server.openChannelOn(await accepted, new ChannelAddress({ type: ns }));
    expect(await tagOf(back)).toBe('client naming');
  });

  test('unclaimed channels are rejected with a generic reason', async () => {
    const { server, client } = setUp();
    server.catchAll = undefined;
    const uri = await server.listenMemory();
    const status = await (await client.openChannelAt(uri, new ChannelAddress({ type: 'nope' })))
      .done;
    expect(codeOf(status)).toBe('notFound');
    expect(status.reason).toBe('not found');
  });
});

describe('bulk channels', () => {
  test('naming no open channel are refused FAILED_PRECONDITION', async () => {
    const server = node();
    const seen: ChannelAddress[] = [];
    const uri = await server.listenMemory({
      policy: (address) => {
        seen.push(address);
        return true;
      },
    });
    server.catchAll = tagged('catch-all');
    const connection = await node().connect(uri);
    const bulkHeader = new Uint8Array(10);
    bulkHeader[0] = 4242 & 0xff;
    bulkHeader[1] = 4242 >> 8;
    bulkHeader[6] = 1;
    const stray = connection.open(
      new ChannelAddress({ type: '_bulk', payload: bulkHeader }).encode(),
    );
    expect(codeOf(await stray.done)).toBe('failedPrecondition');
    // No policy saw it, and the connection is fine.
    expect(seen).toHaveLength(0);
    expect(connection.isOpen).toBe(true);
  });
});

describe('symmetric dispatch', () => {
  test('the server opens a channel back to a client service', async () => {
    const server = node();
    const uri = await server.listenMemory();
    const client = node({ defaultPayload: 'client' });
    const pushed: string[] = [];
    const arrived = new Deferred<void>();
    client.registerService('push', async (incoming) => {
      pushed.push(utf8Decode(incoming.address.payload));
      for await (const frame of incoming.channel.readable) {
        pushed.push(utf8Decode(frame));
      }
      arrived.resolve();
    });
    const accepted = collect(server);
    server.registerService('hello', async (incoming) => {
      // An accepted connection carries the default payload.
      const back = server.openChannelOn(incoming.connection, new ChannelAddress({ type: 'push' }));
      await back.send(incoming.address.payload);
      await back.close();
      await incoming.reject(Status.ok);
    });
    const hello = await client.openChannelAt(uri, new ChannelAddress({ type: 'hello' }));
    await arrived.promise;
    expect(pushed).toEqual(['', 'client']);
    expect(codeOf(await hello.done)).toBe('ok');
    // The server also reaches the client service through `connections`.
    expect(accepted).toHaveLength(1);
    const back = server.openChannelOn(
      accepted[0] as MuxConnection,
      new ChannelAddress({ type: 'push', payload: 'explicit' }),
    );
    await back.close();
    await until(() => pushed.length === 3);
    expect(pushed[2]).toBe('explicit');
  });
});

describe('connection pool', () => {
  async function setUp(): Promise<{
    server: Switchboard;
    client: Switchboard;
    uri: string;
    accepted: MuxConnection[];
  }> {
    const server = node();
    server.registerService(svc, tagged('svc'));
    const uri = await server.listenMemory({ name: 'pool' });
    const accepted = collect(server);
    return { server, client: node(), uri, accepted };
  }

  test('channels to one endpoint share a connection', async () => {
    const { client, uri, accepted } = await setUp();
    const a = await client.openChannelAt(uri, svcAddress());
    const b = await client.openChannelAt('MEM://Pool:7/path?x#svc', svcAddress());
    expect(a.connection).toBe(b.connection);
    expect(a.connection.isInitiator).toBe(true);
    expect(await tagOf(a)).toBe('svc');
    expect(await tagOf(b)).toBe('svc');
    expect(accepted).toHaveLength(1);
  });

  test('concurrent connects share one attempt', async () => {
    const { server, client, uri, accepted } = await setUp();
    const seen = new Set<MuxConnection>();
    server.registerService('who', (incoming) => {
      seen.add(incoming.connection);
      void incoming.channel.close();
    });
    const both = await Promise.all([client.connect(uri), client.connect(uri)]);
    expect(both[0]).toBe(both[1]);
    expect(await client.connect(uri)).toBe(both[0]);
    for (const connection of both) {
      await connection.open(new ChannelAddress({ type: 'who' }).encode()).done;
    }
    expect(seen.size).toBe(1);
    expect(accepted).toHaveLength(1);
  });

  test('a closed connection is replaced', async () => {
    const { client, uri, accepted } = await setUp();
    const firstConnection = await client.connect(uri);
    expect(await tagOf(firstConnection.open(svcAddress().encode()))).toBe('svc');
    await (accepted[0] as MuxConnection).close();
    expect(codeOf(await firstConnection.done)).toBe('connectionLost');
    const second = await client.connect(uri);
    expect(second).not.toBe(firstConnection);
    expect(await tagOf(second.open(svcAddress().encode()))).toBe('svc');
  });

  test('a connection whose peer sent GOAWAY is not reused', async () => {
    const { server, client, uri } = await setUp();
    const arrived = new Deferred<IncomingChannel>();
    server.registerService('hold', (incoming) => void arrived.resolve(incoming));
    const firstConnection = await client.connect(uri);
    const held = firstConnection.open(new ChannelAddress({ type: 'hold' }).encode());
    const serverSide = await arrived.promise;
    // The server keeps the connection while the held channel lives.
    const goingAway = serverSide.connection.goAway();
    await until(() => firstConnection.peerGoingAway);
    const channel = await client.openChannelAt(uri, svcAddress());
    expect(channel.connection).not.toBe(firstConnection);
    expect(await tagOf(channel)).toBe('svc');
    // The held channel still works on the old connection.
    void serverSide.channel.send(bytes('still here'));
    const reader = held.readable.getReader();
    expect(utf8Decode((await reader.read()).value as Uint8Array)).toBe('still here');
    reader.releaseLock();
    await held.close();
    await goingAway;
    expect(codeOf(await firstConnection.done)).toBe('goingAway');
  });

  test('an unknown memory endpoint is unavailable and not cached', async () => {
    const { client } = await setUp();
    await rejectsWith(client.connect('mem://later'), StatusCode.unavailable);
    await rejectsWith(client.openChannelAt('mem://later', svcAddress()), StatusCode.unavailable);
    // Something starts listening there: the next attempt succeeds.
    const revived = node();
    revived.registerService(svc, tagged('late'));
    await revived.listenMemory({ name: 'later' });
    expect(await tagOf(await client.openChannelAt('mem://later', svcAddress()))).toBe('late');
  });

  test('bad endpoints', async () => {
    const { client } = await setUp();
    await rejectsWith(client.connect('unix:///tmp/nothing'), StatusCode.unimplemented);
    // The core runtime has no socket dialers.
    await rejectsWith(client.connect('tcp://127.0.0.1:1'), StatusCode.unimplemented);
    await rejectsWith(client.connect('no scheme'), StatusCode.invalidArgument);
    await rejectsWith(client.connect('mem://'), StatusCode.invalidArgument);
  });

  test('a tcp endpoint needs a host and a port', async () => {
    const dialled: string[] = [];
    const client = node({
      runtime: {
        ...currentRuntime(),
        dialers: {
          tcp: async (endpoint) => {
            dialled.push(endpoint);
            throw new Error('refused');
          },
        },
      },
    });
    await rejectsWith(client.connect('tcp://127.0.0.1'), StatusCode.invalidArgument);
    const e = await rejectsWith(client.connect('tcp://127.0.0.1:9#frag'), StatusCode.unavailable);
    expect(e.status.reason).toContain('refused');
    // The fragment is not dialled.
    expect(dialled).toEqual(['tcp://127.0.0.1:9']);
  });

  test('more connections when the pooled ones are full', async () => {
    const small = node({ muxOptions: { maxChannels: 2 } });
    const held: IncomingChannel[] = [];
    small.registerService('hold', (incoming) => void held.push(incoming));
    const smallUri = await small.listenMemory();
    const pooled = node({ maxConnectionsPerEndpoint: 3 });
    const channels: MuxChannel[] = [];
    for (let i = 0; i < 6; i++) {
      const channel = await pooled.openChannelAt(smallUri, new ChannelAddress({ type: 'hold' }));
      // Let LIMITS arrive on a new connection before the next open.
      await channel.connection.ping();
      channels.push(channel);
    }
    const connections = new Set(channels.map((c) => c.connection));
    expect(connections.size).toBe(3);
    for (const connection of connections) {
      expect(connection.openChannelCount).toBe(2);
    }
    await until(() => held.length === 6);
    // At the cap and every connection is full.
    await rejectsWith(
      pooled.openChannelAt(smallUri, new ChannelAddress({ type: 'hold' })),
      StatusCode.resourceExhausted,
    );
    // A channel that ends makes room on its connection again.
    await (held[0] as IncomingChannel).channel.close();
    await (channels[0] as MuxChannel).done;
    const again = await pooled.openChannelAt(smallUri, new ChannelAddress({ type: 'hold' }));
    expect(again.connection).toBe((channels[0] as MuxChannel).connection);
  });

  test('a GOAWAY between connect and OPEN is retried once', async () => {
    const { server, uri, accepted } = await setUp();
    server.registerService(svc, tagged('one'), { instance: 1 });
    let beforeReturn: ((connection: MuxConnection) => Promise<void>) | undefined;
    class RacingSwitchboard extends Switchboard {
      override async connect(
        endpoint: string,
        options?: Parameters<Switchboard['connect']>[1],
      ): Promise<MuxConnection> {
        const connection = await super.connect(endpoint, options);
        const hook = beforeReturn;
        beforeReturn = undefined;
        await hook?.(connection);
        return connection;
      }
    }
    const racing = new RacingSwitchboard({
      runtime: currentRuntime(),
      muxOptions: fast,
      resolver: new StaticResolver([
        serviceRecord(new ServiceAddress(svc, 1), { endpoints: [uri] }),
      ]),
    });
    onCleanup(() => racing.close());
    const firstConnection = await racing.connect(uri);
    await firstConnection.ping();
    beforeReturn = async (c) => {
      expect(c).toBe(firstConnection);
      await peerGoesAway(c, accepted.at(-1) as MuxConnection);
    };
    const viaAt = await racing.openChannelAt(uri, svcAddress({ instance: 1 }));
    expect(viaAt.connection).not.toBe(firstConnection);
    expect(await tagOf(viaAt)).toBe('one');
    // The same through the resolver.
    const second = viaAt.connection;
    await second.ping();
    beforeReturn = async (c) => {
      expect(c).toBe(second);
      await peerGoesAway(c, accepted.at(-1) as MuxConnection);
    };
    const viaResolver = await racing.openChannel(new ServiceAddress(svc, 1));
    expect(viaResolver.connection).not.toBe(second);
    expect(await tagOf(viaResolver)).toBe('one');
    expect(accepted).toHaveLength(3);
  });

  test('connections reports initiated connections', async () => {
    const { client, uri } = await setUp();
    const initiated = first(client.connections);
    const connection = await client.connect(uri);
    expect(await initiated).toBe(connection);
  });

  test('a policy pools apart from calls without one', async () => {
    const { client, uri, accepted } = await setUp();
    const policy = ChannelPolicies.denyAll;
    const plain = await client.connect(uri);
    const strict = await client.connect(uri, { policy });
    expect(strict).not.toBe(plain);
    expect(await client.connect(uri, { policy })).toBe(strict);
    expect(await client.connect(uri, { policy: ChannelPolicies.allowAll })).not.toBe(strict);
    expect(await client.connect(uri)).toBe(plain);
    expect(accepted).toHaveLength(3);
  });

  test('a signal abandons the wait, not the shared attempt', async () => {
    const { client, uri } = await setUp();
    const controller = new AbortController();
    const reason = new Error('stop');
    controller.abort(reason);
    await expect(client.connect(uri, { signal: controller.signal })).rejects.toBe(reason);
    const late = new AbortController();
    const abandoned = client.connect(uri, { signal: late.signal });
    const shared = client.connect(uri);
    late.abort(reason);
    await expect(abandoned).rejects.toBe(reason);
    expect((await shared).isOpen).toBe(true);
  });
});

describe('openChannel', () => {
  async function setUp(): Promise<{
    seen: IncomingChannel[];
    oneUri: string;
    twoUri: string;
    both: () => StaticResolver;
  }> {
    const seen: IncomingChannel[] = [];
    const recording = (tag: string) => (incoming: IncomingChannel) => {
      seen.push(incoming);
      tagged(tag)(incoming);
    };
    const one = node();
    const two = node();
    one.registerService(svc, recording('one'), { instance: 1 });
    two.registerService(svc, recording('two'), { instance: 2 });
    const oneUri = await one.listenMemory();
    const twoUri = await two.listenMemory();
    // Deliberately out of order: selection sorts by instance.
    const both = () =>
      new StaticResolver([
        serviceRecord(new ServiceAddress(svc, 2), { endpoints: [twoUri] }),
        serviceRecord(new ServiceAddress(svc, 1), { endpoints: [oneUri] }),
      ]);
    return { seen, oneUri, twoUri, both };
  }

  test('needs a resolver', async () => {
    await rejectsWith(node().openChannel(svc), StatusCode.failedPrecondition);
  });

  test('nothing known is not found', async () => {
    const { both } = await setUp();
    const client = node({ resolver: both() });
    await rejectsWith(client.openChannel('other'), StatusCode.notFound);
    await rejectsWith(client.openChannel(new ServiceAddress(svc, 3)), StatusCode.notFound);
  });

  test('filters by instance', async () => {
    const { seen, both } = await setUp();
    const client = node({ resolver: both() });
    for (let i = 0; i < 3; i++) {
      expect(await tagOf(await client.openChannel('svc/2'))).toBe('two');
      expect(await tagOf(await client.openChannel(new ServiceAddress(svc, 1)))).toBe('one');
    }
    expect(seen.map((s) => s.address.instance)).toEqual([2, 1, 2, 1, 2, 1]);
  });

  test('round robin across instances', async () => {
    const { seen, both } = await setUp();
    const client = node({ resolver: both() });
    const tags: string[] = [];
    for (let i = 0; i < 4; i++) {
      tags.push(await tagOf(await client.openChannel(svc)));
    }
    expect(tags).toEqual(['one', 'two', 'one', 'two']);
    // The header names the selected instance, so dispatch is exact.
    expect(seen.map((s) => s.address.instance)).toEqual([1, 2, 1, 2]);
    expect(seen.every((s) => s.address.shard === undefined)).toBe(true);
  });

  test('random selection uses the injected source', async () => {
    const { seen, both } = await setUp();
    const draws = [0.99, 0, 0.5, 0.49];
    const client = node({
      resolver: both(),
      selection: 'random',
      random: () => draws.shift() ?? 0,
    });
    for (let i = 0; i < 4; i++) {
      await tagOf(await client.openChannel(svc));
    }
    expect(seen.map((s) => s.address.instance)).toEqual([2, 1, 2, 1]);
  });

  test('shard selection is deterministic', async () => {
    const { seen, both } = await setUp();
    const client = node({ resolver: both() });
    for (let round = 0; round < 2; round++) {
      for (const [shard, tag] of [
        [0, 'one'],
        [1, 'two'],
        [2, 'one'],
        [5, 'two'],
        [0xffffffff, 'two'],
      ] as const) {
        const channel = await client.openChannel(svc, { shard });
        expect(await tagOf(channel), `shard ${shard}`).toBe(tag);
      }
    }
    expect(seen.at(-1)?.address.shard).toBe(0xffffffff);
    expect(seen.at(-1)?.address.instance).toBe(2);
    await expect(client.openChannel(svc, { shard: 2 ** 32 })).rejects.toThrow(RangeError);
    await expect(client.openChannel(svc, { shard: -1 })).rejects.toThrow(RangeError);
  });

  test('fails over to the next endpoint, then the next instance', async () => {
    const { seen, oneUri } = await setUp();
    const dead = 'mem://dead';
    const client = node({
      resolver: new StaticResolver([
        serviceRecord(new ServiceAddress(svc, 1), { endpoints: [dead, oneUri] }),
        serviceRecord(new ServiceAddress(svc, 2), { endpoints: [dead] }),
        serviceRecord(new ServiceAddress(svc, 3), { endpoints: [] }),
      ]),
    });
    expect(await tagOf(await client.openChannel(new ServiceAddress(svc, 1)))).toBe('one');
    // Instance 2 is first by shard, unreachable; 3 has no endpoint.
    expect(await tagOf(await client.openChannel(svc, { shard: 1 }))).toBe('one');
    expect(seen.at(-1)?.address.instance).toBe(1);
    const e = await rejectsWith(
      client.openChannel(new ServiceAddress(svc, 2)),
      StatusCode.unavailable,
    );
    expect(e.status.reason).toContain('mem://dead');
    const none = await rejectsWith(
      client.openChannel(new ServiceAddress(svc, 3)),
      StatusCode.unavailable,
    );
    expect(none.status.reason).toContain('no endpoints');
  });

  test('a filter narrows the candidates', async () => {
    const { seen, oneUri, twoUri } = await setUp();
    const client = node({
      resolver: new StaticResolver([
        serviceRecord(new ServiceAddress(svc, 1), { endpoints: [oneUri], metadata: bytes('blue') }),
        serviceRecord(new ServiceAddress(svc, 2), {
          endpoints: [twoUri],
          metadata: bytes('green'),
        }),
      ]),
    });
    const green = (record: { metadata: Uint8Array }) => utf8Decode(record.metadata) === 'green';
    for (let i = 0; i < 3; i++) {
      expect(await tagOf(await client.openChannel(svc, { where: green }))).toBe('two');
    }
    expect(seen.map((s) => s.address.instance)).toEqual([2, 2, 2]);
    const e = await rejectsWith(
      client.openChannel(svc, { where: () => false }),
      StatusCode.notFound,
    );
    expect(e.status.reason).toContain('matches the filter');
    await expect(
      client.openChannel(svc, {
        where: () => {
          throw new TypeError('filter bug');
        },
      }),
    ).rejects.toThrow(TypeError);
  });

  test('default payload applied and overridable', async () => {
    const { seen, oneUri, both } = await setUp();
    const client = node({ resolver: both(), defaultPayload: 'cred' });
    await tagOf(await client.openChannel(new ServiceAddress(svc, 1)));
    await tagOf(await client.openChannel(new ServiceAddress(svc, 1), { payload: 'own' }));
    await tagOf(await client.openChannelAt(oneUri, svcAddress({ instance: 1 })));
    await tagOf(
      await client.openChannelAt(oneUri, svcAddress({ instance: 1, payload: 'explicit' })),
    );
    // An explicitly empty payload is the caller's.
    await tagOf(await client.openChannel(new ServiceAddress(svc, 1), { payload: '' }));
    expect(seen.map((s) => utf8Decode(s.address.payload))).toEqual([
      'cred',
      'own',
      'cred',
      'explicit',
      '',
    ]);
  });

  test('an endpoint resolver keeps the requested instance', async () => {
    const { seen, oneUri } = await setUp();
    const client = node({ resolver: new EndpointResolver(oneUri) });
    expect(await tagOf(await client.openChannel(new ServiceAddress(svc, 1)))).toBe('one');
    expect(await tagOf(await client.openChannel(svc))).toBe('one');
    expect(seen.map((s) => s.address.instance)).toEqual([1, 0]);
  });

  test('selectAndConnect reports the selected instance', async () => {
    const { twoUri, both } = await setUp();
    const client = node({ resolver: both() });
    const { record, connection } = await client.selectAndConnect(svc, { shard: 3 });
    expect(record.address.equals(new ServiceAddress(svc, 2))).toBe(true);
    expect(connection.isOpen).toBe(true);
    expect(await client.connect(twoUri)).toBe(connection);
  });

  test('excludeOwnEndpoints skips the node itself', async () => {
    const { oneUri } = await setUp();
    const self = node();
    const own = await self.listenMemory();
    self.resolver = new StaticResolver([
      serviceRecord(new ServiceAddress(svc, 1), { endpoints: [own] }),
      serviceRecord(new ServiceAddress(svc, 2), { endpoints: [oneUri] }),
    ]);
    const { record } = await self.selectAndConnect(svc, { shard: 0, excludeOwnEndpoints: true });
    expect(record.address.instance).toBe(2);
    const e = await rejectsWith(
      self.selectAndConnect(new ServiceAddress(svc, 1), { excludeOwnEndpoints: true }),
      StatusCode.unavailable,
    );
    expect(e.status.reason).toContain('this node');
  });

  test('openTalk fails until the Talk layer lands, and closes the channel', async () => {
    const { both } = await setUp();
    const client = node({ resolver: both() });
    await rejectsWith(client.openTalk('svc/1'), StatusCode.unimplemented);
  });
});

describe('close', () => {
  test('sends GOAWAY, stops listening and refuses new work', async () => {
    const server = new Switchboard({ runtime: currentRuntime(), muxOptions: fast });
    server.registerService(svc, () => {});
    const uri = await server.listenMemory();
    const client = node();
    const held = await client.openChannelAt(uri, svcAddress());
    await pump();
    const start = Date.now();
    await server.close();
    // Bounded by the grace period, not by the open channels.
    expect(Date.now() - start).toBeLessThan(2000);
    expect(codeOf(await held.connection.done)).toBe('goingAway');
    expect(codeOf(await held.done)).toBe('goingAway');
    await rejectsWith(client.connect(uri), StatusCode.unavailable);
    await rejectsWith(server.connect(uri), StatusCode.failedPrecondition);
    await rejectsWith(server.openChannelAt(uri, svcAddress()), StatusCode.failedPrecondition);
    await rejectsWith(server.dial(uri), StatusCode.failedPrecondition);
    await rejectsWith(server.openChannel(svc), StatusCode.failedPrecondition);
    expect(() => server.registerBinding({ schemes: ['x'], dial: () => Promise.reject() })).toThrow(
      SwitchboardError,
    );
    // Idempotent.
    expect(server.close()).toBe(server.close());
    await server[Symbol.asyncDispose]();
  });

  test('a client closing right after connecting sends GOAWAY', async () => {
    const server = node();
    const uri = await server.listenMemory();
    const seen: (string | undefined)[] = [];
    for (let i = 0; i < 20; i++) {
      const accepted = first(server.connections);
      const client = new Switchboard({ runtime: currentRuntime(), muxOptions: fast });
      await client.connect(uri);
      await client.close();
      seen.push(codeOf(await (await accepted).done));
    }
    expect(seen.every((code) => code === 'goingAway')).toBe(true);
  });

  test('a dial in progress gets GOAWAY, and close waits for it', async () => {
    const release = new Deferred<void>();
    const arrived = new Deferred<void>();
    const peer = new Deferred<MuxConnection>();
    const runtime = currentRuntime();
    const client = new Switchboard({
      muxOptions: fast,
      runtime: {
        ...runtime,
        dialers: {
          ws: async () => {
            arrived.resolve();
            await release.promise;
            const [local, remote] = memoryTransportPair({ delay: 0 });
            peer.resolve(runtime.mux(remote, { ...fast } as never, false));
            return local;
          },
        },
      },
    });
    const dial = rejectsWith(client.connect('ws://127.0.0.1:1/'), StatusCode.failedPrecondition);
    await arrived.promise;
    let closed = false;
    const closing = client.close().then(() => {
      closed = true;
    });
    await pump();
    expect(closed).toBe(false);
    release.resolve();
    const connection = await peer.promise;
    onCleanup(() => connection.close());
    await closing;
    await dial;
    // The peer was told: GOAWAY, not a dropped connection.
    expect(codeOf(await connection.done)).toBe('goingAway');
  });

  test('closing a client closes its pooled connections', async () => {
    const server = node();
    const uri = await server.listenMemory();
    const accepted = first(server.connections);
    const client = new Switchboard({ runtime: currentRuntime(), muxOptions: fast });
    const connection = await client.connect(uri);
    const serverSide = await accepted;
    const reported: MuxConnection[] = [];
    const ended = (async () => {
      for await (const c of client.connections) reported.push(c);
    })();
    await client.close();
    expect(connection.isOpen).toBe(false);
    expect(codeOf(await serverSide.done)).toBe('goingAway');
    await ended;
    expect(reported).toEqual([]);
  });
});

describe('accept', () => {
  const secret = Name.of('secret');

  function served(remotes: (string | undefined)[], options = {}): Switchboard {
    const server = node({ muxOptions: options });
    server.registerService(svc, (incoming) => {
      remotes.push(incoming.remote);
      tagged('svc')(incoming);
    });
    server.registerService(secret, tagged('secret'));
    return server;
  }

  test('adopts a transport like a listener: acceptor side, mux options, policy, dispatch, connections', async () => {
    const remotes: (string | undefined)[] = [];
    const server = served(remotes, { maxChannels: 7 });
    const announced = first(server.connections);
    const { peer, transport } = rawPeer();
    const connection = await server.accept(transport, {
      policy: ChannelPolicies.allowTypes([svc]),
    });
    expect(await announced).toBe(connection);
    expect(connection.isInitiator).toBe(false);
    await peer.ping();
    // The node's mux options, announced with LIMITS.
    expect(peer.peerMaxChannels).toBe(7);
    expect(await tagOf(peer.open(svcAddress().encode()))).toBe('svc');
    const refused = peer.open(new ChannelAddress({ type: secret }).encode());
    const status = await refused.done;
    expect(codeOf(status)).toBe('permissionDenied');
    expect(status.reason).toBe('permission denied');
    // Without a policy everything is reachable; the remote is as given.
    const other = rawPeer();
    await server.accept(other.transport, { remote: 'app peer 1' });
    expect(await tagOf(other.peer.open(new ChannelAddress({ type: secret }).encode()))).toBe(
      'secret',
    );
    expect(await tagOf(other.peer.open(svcAddress().encode()))).toBe('svc');
    expect(remotes).toEqual(['external', 'app peer 1']);
    // Symmetric dispatch: the node opens channels back on it.
    void (async () => {
      for await (const channel of peer.incoming) {
        void channel.send(bytes('peer'));
        void channel.close();
      }
    })();
    expect(await tagOf(server.openChannelOn(connection, new ChannelAddress()))).toBe('peer');
  });

  test('close sends GOAWAY to an accepted connection', async () => {
    const server = new Switchboard({ runtime: currentRuntime(), muxOptions: fast });
    const { peer, transport } = rawPeer();
    const connection = await server.accept(transport);
    await peer.ping();
    await server.close();
    expect(codeOf(await peer.done)).toBe('goingAway');
    expect(codeOf(await connection.done)).toBe('goingAway');
  });

  test('accept after close fails and closes the transport', async () => {
    const server = new Switchboard({ runtime: currentRuntime(), muxOptions: fast });
    await server.close();
    const { peer, transport } = rawPeer();
    await rejectsWith(server.accept(transport), StatusCode.failedPrecondition);
    // The peer is told, and the transport closed.
    expect(codeOf(await peer.done)).toBe('goingAway');
  });

  test('close waits for a connection accepted while closing', async () => {
    const server = new Switchboard({
      runtime: currentRuntime(),
      muxOptions: { ...fast, goAwayGrace: 5000 },
    });
    const held = new Deferred<IncomingChannel>();
    server.registerService('hold', (incoming) => void held.resolve(incoming));
    const one = rawPeer();
    await server.accept(one.transport);
    one.peer.open(new ChannelAddress({ type: 'hold' }).encode());
    const holding = await held.promise;
    let closed = false;
    const closing = server.close().then(() => {
      closed = true;
    });
    // The held channel keeps the node closing.
    await until(() => one.peer.peerGoingAway);
    const two = rawPeer();
    await rejectsWith(server.accept(two.transport), StatusCode.failedPrecondition);
    expect(codeOf(await two.peer.done)).toBe('goingAway');
    await pump();
    expect(closed).toBe(false);
    await holding.channel.close();
    expect(codeOf(await one.peer.done)).toBe('goingAway');
    await closing;
    expect(closed).toBe(true);
  });

  test('adopt takes a connection of any binding, once', async () => {
    const server = node();
    server.registerService(svc, tagged('svc'));
    const [local, remote] = memoryTransportPair({ delay: 0 });
    const peer = currentRuntime().mux(local, { ...fast } as never, true);
    onCleanup(() => peer.close());
    const connection = currentRuntime().mux(remote, server.muxOptions, false);
    expect(await server.adopt(connection, { remote: 'binding peer' })).toBe(connection);
    expect(await tagOf(peer.open(svcAddress().encode()))).toBe('svc');
    await expect(server.adopt(connection)).rejects.toThrow(TypeError);
    expect((connection as ToyMuxConnection).localIdentity).toBeUndefined();
  });
});
