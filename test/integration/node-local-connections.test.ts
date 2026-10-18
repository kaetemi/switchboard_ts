/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Connections a Node node makes to its own TCP and WebSocket listeners
// (localConnections): an in-process pair instead of a socket, accepted as
// the designated listener accepts a connection and otherwise established
// like any initiated connection; and the socket path with
// localConnections false. The tcp and ws variants of
// `test/local_connections_test.dart`; its mem variant is
// test/unit/switchboard/local-connections.test.ts. The Talk cases (the
// bulk echo, the PeerSet request) run over TCP, as in Dart.

import { createServer } from 'node:net';
import { beforeAll, describe, expect, test } from 'vitest';
import { ChannelAddress, ServiceAddress } from '../../src/core/address/index.ts';
import { utf8Decode } from '../../src/core/bytes.ts';
import { type Credential, Right } from '../../src/core/identity/contract.ts';
import type { MuxConnection } from '../../src/core/mux/contract.ts';
import { Name } from '../../src/core/name.ts';
import { serviceRecord } from '../../src/core/naming/naming-protocol.ts';
import { StatusCode } from '../../src/core/status.ts';
import { ChannelPolicies } from '../../src/core/switchboard/channel-policy.ts';
import type {
  ChannelHandler,
  ChannelPolicy,
  IncomingChannel,
} from '../../src/core/switchboard/contract.ts';
import { PeerSet } from '../../src/core/switchboard/peer-set.ts';
import { proxyHandler } from '../../src/core/switchboard/proxy.ts';
import { StaticResolver } from '../../src/core/switchboard/resolver.ts';
import { Deferred } from '../../src/core/util/deferred.ts';
import type { Switchboard, SwitchboardOptions } from '../../src/node/switchboard.ts';
import { bytes, rejectsWith, tagged, tagOf, until } from '../support/node/harness.ts';
import { hmacAuthority, issueCredential, TestVerifier } from '../support/node/test-authority.ts';
import { node, onCleanup, within } from './node-support.ts';

const svc = Name.of('svc');
const other = Name.of('other');
const back = Name.of('back');

const at = (type: Name) => new ChannelAddress({ type });

/** A listener of [scheme] on [n]. */
function listen(n: Switchboard, scheme: 'tcp' | 'ws', policy?: ChannelPolicy): Promise<string> {
  return scheme === 'tcp'
    ? n.listenTcp('127.0.0.1', 0, { policy })
    : n.listenWebSocket('127.0.0.1', 0, { path: '/mesh', policy });
}

/** A handler recording every channel it gets in [seen], then like {@link tagged}. */
function recording(seen: IncomingChannel[], tag: string): ChannelHandler {
  return (incoming) => {
    seen.push(incoming);
    return tagged(tag)(incoming);
  };
}

/** The connections [s] reports from now on. */
function collect(s: Switchboard): MuxConnection[] {
  const seen: MuxConnection[] = [];
  void (async () => {
    for await (const connection of s.connections) seen.push(connection);
  })();
  return seen;
}

for (const scheme of ['tcp', 'ws'] as const) {
  describe(`to its own ${scheme} listener`, () => {
    test('a node opens a channel to a service it hosts in-process; connections reports both ends', async () => {
      const n = node();
      const uri = await listen(n, scheme);
      const reported = collect(n);
      const seen: IncomingChannel[] = [];
      n.registerService(svc, recording(seen, 'hello'));
      const channel = await n.openChannelAt(uri, at(svc));
      expect(await tagOf(channel)).toBe('hello');
      expect(seen).toHaveLength(1);
      const incoming = seen[0] as IncomingChannel;
      expect(incoming.remote).toBe('local');
      expect(incoming.connection.isInitiator).toBe(false);
      expect(channel.connection.isInitiator).toBe(true);
      expect(incoming.connection).not.toBe(channel.connection);
      await until(() => reported.length === 2);
      expect(new Set(reported)).toEqual(new Set([incoming.connection, channel.connection]));
      expect(n.isOwnEndpoint(uri)).toBe(true);
    });

    test('the listener policy applies', async () => {
      const n = node();
      const uri = await listen(n, scheme, ChannelPolicies.allowTypes([svc]));
      n.registerService(svc, tagged('svc'));
      n.registerService(other, tagged('other'));
      expect(await tagOf(await n.openChannelAt(uri, at(svc)))).toBe('svc');
      expect(await tagOf(await n.openChannelAt(uri, at(other)))).toBe('closed permissionDenied');
    });

    test('a denying listener policy refuses the channel', async () => {
      const n = node();
      const uri = await listen(n, scheme, ChannelPolicies.denyAll);
      const seen: IncomingChannel[] = [];
      n.registerService(svc, recording(seen, 'svc'));
      expect(await tagOf(await n.openChannelAt(uri, at(svc)))).toBe('closed permissionDenied');
      expect(seen).toEqual([]);
    });

    test('the pool reuses the pair; close ends both ends', async () => {
      const n = node();
      const uri = await listen(n, scheme);
      const reported = collect(n);
      const held: IncomingChannel[] = [];
      n.registerService(svc, (incoming) => {
        held.push(incoming);
      });
      const connection = await n.connect(uri);
      expect(await n.connect(uri)).toBe(connection);
      const first = await n.openChannelAt(uri, at(svc));
      const second = await n.openChannelAt(uri, at(svc));
      expect(first.connection).toBe(connection);
      expect(second.connection).toBe(connection);
      await until(() => held.length === 2);
      const accepted = (held[0] as IncomingChannel).connection;
      expect((held[1] as IncomingChannel).connection).toBe(accepted);
      await until(() => reported.length === 2);

      await n.close();
      await connection.done;
      await accepted.done;
      expect(connection.isOpen).toBe(false);
      expect(accepted.isOpen).toBe(false);
      await first.done;
      // The listener stopped: no longer this node's, and nothing is dialled.
      expect(n.isOwnEndpoint(uri)).toBe(false);
      await rejectsWith(n.connect(uri), StatusCode.failedPrecondition);
    });

    test('a connection to itself ending ends the other side', async () => {
      const n = node();
      const uri = await listen(n, scheme);
      const held: IncomingChannel[] = [];
      n.registerService(svc, (incoming) => {
        held.push(incoming);
      });
      const channel = await n.openChannelAt(uri, at(svc));
      await until(() => held.length > 0);
      const accepted = (held[0] as IncomingChannel).connection;
      void channel.connection.goAway();
      await accepted.done;
      await channel.connection.done;
      // The pool replaces it with a new pair.
      const again = await n.openChannelAt(uri, at(svc));
      expect(again.connection).not.toBe(channel.connection);
      await until(() => held.length === 2);
      expect(held[1]?.remote).toBe('local');
    });

    test('dial gives a new in-process connection each time', async () => {
      const n = node();
      const uri = await listen(n, scheme);
      const seen: IncomingChannel[] = [];
      n.registerService(svc, recording(seen, 'svc'));
      const one = await n.dial(uri);
      const two = await n.dial(uri);
      expect(two).not.toBe(one);
      const pooled = await n.connect(uri);
      expect(pooled).not.toBe(one);
      expect(pooled).not.toBe(two);
      for (const connection of [one, two]) {
        expect(await tagOf(n.openChannelOn(connection, at(svc)))).toBe('svc');
      }
      expect(seen.map((s) => s.remote)).toEqual(['local', 'local']);
      expect(seen[0]?.connection).not.toBe(seen[1]?.connection);
      await one.goAway();
      await two.goAway();
    });

    test('with localConnections false, a socket as before', async () => {
      const n = node({ localConnections: false });
      const uri = await listen(n, scheme);
      const seen: IncomingChannel[] = [];
      n.registerService(svc, recording(seen, 'svc'));
      expect(await tagOf(await n.openChannelAt(uri, at(svc)))).toBe('svc');
      expect(n.isOwnEndpoint(uri)).toBe(true);
      expect(seen[0]?.remote).toMatch(/^127\.0\.0\.1:\d+$/);
    });
  });
}

test('the listener the endpoint designates is the one whose policy applies', async () => {
  const n = node();
  const tcp = await n.listenTcp('127.0.0.1', 0, { policy: ChannelPolicies.denyAll });
  const ws = await n.listenWebSocket('127.0.0.1', 0);
  n.registerService(svc, tagged('svc'));
  expect(await tagOf(await n.openChannelAt(tcp, at(svc)))).toBe('closed permissionDenied');
  expect(await tagOf(await n.openChannelAt(ws, at(svc)))).toBe('svc');
  // Another spelling of the TCP listener: its own pooled connection,
  // in-process too, under the same listener policy.
  const localhost = `tcp://localhost:${new URL(tcp).port}`;
  expect(n.isOwnEndpoint(localhost)).toBe(true);
  const spelled = await n.openChannelAt(localhost, at(svc));
  expect(await tagOf(spelled)).toBe('closed permissionDenied');
  expect(spelled.connection).not.toBe(await n.connect(tcp));
});

test('the initiated side has the outgoing policy', async () => {
  const n = node({ outgoingPolicy: ChannelPolicies.denyAll });
  const uri = await listen(n, 'tcp');
  n.registerService(back, tagged('back'));
  const answered = new Deferred<string>();
  n.registerService(svc, (incoming) => {
    // Symmetric dispatch: a channel back on the accepted side reaches the
    // initiated side, under its policy.
    const channel = incoming.connection.open(at(back).encode());
    tagOf(channel).then(
      (tag) => answered.resolve(tag),
      (error: unknown) => answered.reject(error),
    );
    void incoming.channel.close();
  });
  await n.openChannelAt(uri, at(svc));
  expect(await answered.promise).toBe('closed permissionDenied');
  // The accepted side has the listener's policy (none): the same channel
  // the other way is admitted.
  expect(await tagOf(await n.openChannelAt(uri, at(back)))).toBe('back');
});

test('endpointPolicy and credentialFor are asked about the endpoint as given', async () => {
  const asked: string[] = [];
  const n = node({
    endpointPolicy: (endpoint) => {
      asked.push(endpoint);
      return undefined;
    },
    credentialFor: (endpoint) => bytes(`for ${endpoint}`),
  });
  const uri = await listen(n, 'tcp');
  const payloads: string[] = [];
  n.registerService(svc, (incoming) => {
    payloads.push(utf8Decode(incoming.address.payload));
    void incoming.channel.close();
  });
  const port = new URL(uri).port;
  const localhost = `tcp://LOCALHOST:${port}`;
  await (await n.openChannelAt(localhost, at(svc))).done;
  expect(asked).toEqual([`tcp://localhost:${port}`]);
  expect(payloads).toEqual([`for ${localhost}`]);
});

describe('identity', () => {
  let credential: Credential;
  beforeAll(async () => {
    credential = await issueCredential({
      identity: 'hub',
      scopes: [[Right.open, 'svc']],
      by: hmacAuthority,
    });
  });

  function identified(options: SwitchboardOptions = {}): Switchboard {
    return node({
      credential,
      verifier: new TestVerifier(),
      identityTimeout: 5000,
      expectedIdentityFor: () => 'hub',
      muxOptions: { requireNamedIdent: true },
      ...options,
    });
  }

  for (const scheme of ['tcp', 'ws'] as const) {
    test(`the node identifies to itself; a scoped listener admits what its credential allows (${scheme})`, async () => {
      const n = identified();
      const uri = await listen(n, scheme, ChannelPolicies.scoped());
      const seen: IncomingChannel[] = [];
      n.registerService(svc, recording(seen, 'svc'));
      n.registerService(other, recording(seen, 'other'));
      const channel = await n.openChannelAt(uri, at(svc));
      expect(await tagOf(channel)).toBe('svc');
      expect(seen).toHaveLength(1);
      const incoming = seen[0] as IncomingChannel;
      expect(incoming.remote).toBe('local');
      expect(incoming.peerIdentity?.identity).toBe('hub');
      expect(incoming.peerIdentity?.receiver).toBe('hub');
      expect(channel.connection.localIdentity).toBe('hub');
      expect(await tagOf(await n.openChannelAt(uri, at(other)))).toBe('closed permissionDenied');
      expect(seen).toHaveLength(1);
    });
  }

  test('without identifyOutgoing, a scoped listener refuses the node once the identity wait is over', async () => {
    const n = identified({ identifyOutgoing: false, identityTimeout: 100 });
    const uri = await listen(n, 'tcp', ChannelPolicies.scoped());
    n.registerService(svc, tagged('svc'));
    const channel = await n.openChannelAt(uri, at(svc));
    expect(await tagOf(channel)).toBe('closed unauthenticated');
    expect(channel.connection.peerIdentity).toBeUndefined();
  });
});

test('a large bulk payload to itself, both ways, under flow control', async () => {
  const n = node();
  const uri = await listen(n, 'tcp');
  const bulk: boolean[] = [];
  n.registerService(svc, (incoming) => {
    void incoming.talk().serve((message) => {
      bulk.push(message.isBulk);
      void message.reply(message.payload);
    });
  });
  const talk = await n.openTalkAt(uri, at(svc));
  const payload = new Uint8Array(3 * 1024 * 1024 + 17);
  for (let i = 0; i < payload.length; i++) {
    payload[i] = (i * 31 + Math.floor(i / 2048)) & 0xff;
  }
  const reply = await within(talk.request('ECHO', payload), 20_000);
  expect(bulk).toEqual([true]);
  expect(reply.isBulk).toBe(true);
  const echoed = reply.payload;
  expect(echoed.length).toBe(payload.length);
  expect(Buffer.compare(Buffer.from(echoed), Buffer.from(payload)), 'the echo differs').toBe(0);
  await talk.close();
});

test('a PeerSet over its own instance comes online in-process', async () => {
  const table = new StaticResolver();
  onCleanup(() => table.close());
  const n = node({ resolver: table });
  const uri = await listen(n, 'tcp');
  const seen: IncomingChannel[] = [];
  n.registerService(
    svc,
    (incoming) => {
      seen.push(incoming);
      void incoming.talk().serve((message) => {
        if (message.expectsReply) {
          void message.reply('pong');
        }
      });
    },
    { instance: 1 },
  );
  table.add(serviceRecord(new ServiceAddress(svc, 1), { endpoints: [uri] }));
  const set = PeerSet.watch(n, svc, { channel: at(svc), policy: ChannelPolicies.denyAll });
  onCleanup(() => set.close());
  await until(() => set.online.length > 0);
  const peer = set.peers.get(1);
  expect(peer?.endpoint).toBe(uri);
  expect(seen[0]?.remote).toBe('local');
  expect(seen[0]?.address.address?.equals(new ServiceAddress(svc, 1))).toBe(true);
  // The set's connection is its own, not the pooled one.
  expect(peer?.connection).not.toBe(await n.connect(uri));
  const talk = await set.openTalk(1);
  expect((await talk.request('PING')).text()).toBe('pong');
  await talk.close();
});

test('the proxy still refuses to forward to its own listener', async () => {
  const table = new StaticResolver();
  onCleanup(() => table.close());
  const proxyNode = node({ resolver: table });
  const hits: IncomingChannel[] = [];
  proxyNode.registerService(svc, recording(hits, 'direct'), { instance: 1 });
  proxyNode.catchAll = proxyHandler(proxyNode);
  const uri = await listen(proxyNode, 'tcp');
  // A record of `other` pointing back at the proxy itself.
  table.add(serviceRecord(new ServiceAddress(other, 1), { endpoints: [uri] }));
  const client = node();
  const channel = await client.openChannelAt(uri, at(other));
  expect(await tagOf(channel)).toBe('closed unavailable');
  expect(hits).toEqual([]);
});

test('an endpoint of a port the node does not listen on is dialled as usual', async () => {
  const n = node();
  await listen(n, 'tcp');
  const server = createServer();
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  const port = typeof address === 'object' && address !== null ? address.port : 0;
  await new Promise<void>((resolve) => server.close(() => resolve()));
  const elsewhere = `tcp://127.0.0.1:${port}`;
  expect(n.isOwnEndpoint(elsewhere)).toBe(false);
  await rejectsWith(n.connect(elsewhere), StatusCode.unavailable);
});
