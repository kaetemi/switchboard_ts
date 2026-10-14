/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Connections a node makes to its own listeners (localConnections): an
// in-process pair instead of a socket, accepted as the designated listener
// accepts a connection (its policy, dispatch, `connections`, GOAWAY on
// close) and otherwise established like any initiated connection (pool,
// outgoing policy, identity, credentialFor). A port of the Dart
// local_connections_test.dart over `mem://` listeners; the TCP and
// WebSocket variants are in the Node integration tests, and the Talk,
// PeerSet and proxy cases follow those layers.

import { describe, expect, test } from 'vitest';
import { ChannelAddress } from '../../../src/core/address/index.ts';
import { utf8Decode } from '../../../src/core/bytes.ts';
import { Right } from '../../../src/core/identity/contract.ts';
import type { MuxConnection } from '../../../src/core/mux/contract.ts';
import { Name } from '../../../src/core/name.ts';
import { StatusCode } from '../../../src/core/status.ts';
import { ChannelPolicies } from '../../../src/core/switchboard/channel-policy.ts';
import type { ChannelHandler, IncomingChannel } from '../../../src/core/switchboard/contract.ts';
import type { Switchboard, SwitchboardOptions } from '../../../src/core/switchboard/switchboard.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';
import { FakeVerifier, fakeCredential } from '../../support/node/fake-identity.ts';
import {
  bytes,
  node,
  pump,
  rejectsWith,
  svc,
  tagged,
  tagOf,
  until,
} from '../../support/node/harness.ts';

const other = Name.of('other');
const back = Name.of('back');

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

const at = (type: Name) => new ChannelAddress({ type });

describe('to its own mem listener', () => {
  test('a node opens a channel to a service it hosts in-process; connections reports both ends', async () => {
    const n = node();
    const uri = await n.listenMemory();
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
    const uri = await n.listenMemory({ policy: ChannelPolicies.allowTypes([svc]) });
    n.registerService(svc, tagged('svc'));
    n.registerService(other, tagged('other'));
    expect(await tagOf(await n.openChannelAt(uri, at(svc)))).toBe('svc');
    expect(await tagOf(await n.openChannelAt(uri, at(other)))).toBe('closed permissionDenied');
  });

  test('a denying listener policy refuses the channel', async () => {
    const n = node();
    const uri = await n.listenMemory({ policy: ChannelPolicies.denyAll });
    const seen: IncomingChannel[] = [];
    n.registerService(svc, recording(seen, 'svc'));
    expect(await tagOf(await n.openChannelAt(uri, at(svc)))).toBe('closed permissionDenied');
    expect(seen).toEqual([]);
  });

  test('the pool reuses the pair; close ends both ends', async () => {
    const n = node();
    const uri = await n.listenMemory();
    const reported = collect(n);
    const held: IncomingChannel[] = [];
    n.registerService(svc, (incoming) => {
      held.push(incoming);
    });

    const connection = await n.connect(uri);
    expect(await n.connect(uri)).toBe(connection);
    const firstChannel = await n.openChannelAt(uri, at(svc));
    const second = await n.openChannelAt(uri, at(svc));
    expect(firstChannel.connection).toBe(connection);
    expect(second.connection).toBe(connection);
    await until(() => held.length === 2);
    const accepted = (held[0] as IncomingChannel).connection;
    expect((held[1] as IncomingChannel).connection).toBe(accepted);
    await pump(2);
    expect(reported).toHaveLength(2);

    await n.close();
    await connection.done;
    await accepted.done;
    expect(connection.isOpen).toBe(false);
    expect(accepted.isOpen).toBe(false);
    await firstChannel.done;
    // The listener stopped: no longer this node's, and nothing is dialled.
    expect(n.isOwnEndpoint(uri)).toBe(false);
    await rejectsWith(n.connect(uri), StatusCode.failedPrecondition);
  });

  test('a connection to itself ending ends the other side', async () => {
    const n = node();
    const uri = await n.listenMemory();
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
    const uri = await n.listenMemory();
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

  test('with localConnections false, through the registry as before', async () => {
    const n = node({ localConnections: false });
    const uri = await n.listenMemory();
    const seen: IncomingChannel[] = [];
    n.registerService(svc, recording(seen, 'svc'));
    expect(await tagOf(await n.openChannelAt(uri, at(svc)))).toBe('svc');
    expect(n.isOwnEndpoint(uri)).toBe(true);
    // The remote names the initiator's mem endpoint.
    expect(seen[0]?.remote).toBe(uri);
  });
});

test('the listener the endpoint designates is the one whose policy applies', async () => {
  const n = node();
  const denying = await n.listenMemory({ policy: ChannelPolicies.denyAll });
  const open = await n.listenMemory();
  n.registerService(svc, tagged('svc'));
  expect(await tagOf(await n.openChannelAt(denying, at(svc)))).toBe('closed permissionDenied');
  expect(await tagOf(await n.openChannelAt(open, at(svc)))).toBe('svc');
  // Another spelling of the denying listener: the same pooled connection,
  // under the same listener policy.
  const spelled = denying.toUpperCase().replace('MEM://', 'mem://');
  expect(n.isOwnEndpoint(spelled)).toBe(true);
  const channel = await n.openChannelAt(spelled, at(svc));
  expect(await tagOf(channel)).toBe('closed permissionDenied');
  expect(channel.connection).toBe(await n.connect(denying));
});

test('the initiated side has the outgoing policy', async () => {
  const n = node({ outgoingPolicy: ChannelPolicies.denyAll });
  const uri = await n.listenMemory();
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

test('endpointPolicy sees the pool key and credentialFor the endpoint as given', async () => {
  const asked: string[] = [];
  const n = node({
    endpointPolicy: (endpoint) => {
      asked.push(endpoint);
      return undefined;
    },
    credentialFor: (endpoint) => bytes(`for ${endpoint}`),
  });
  const uri = await n.listenMemory({ name: 'self' });
  const payloads: string[] = [];
  n.registerService(svc, (incoming) => {
    payloads.push(utf8Decode(incoming.address.payload));
    void incoming.channel.close();
  });
  await (await n.openChannelAt('MEM://SELF', at(svc))).done;
  expect(asked).toEqual([uri]);
  expect(payloads).toEqual(['for MEM://SELF']);
});

describe('identity', () => {
  const credential = fakeCredential({ identity: 'hub', scopes: [[Right.open, 'svc']] });

  function identified(options: SwitchboardOptions = {}): Switchboard {
    return node({
      credential,
      verifier: new FakeVerifier(),
      identityTimeout: 5000,
      expectedIdentityFor: () => 'hub',
      muxOptions: { requireNamedIdent: true },
      ...options,
    });
  }

  test('the node identifies to itself; a scoped listener admits what its credential allows', async () => {
    const n = identified();
    const uri = await n.listenMemory({ policy: ChannelPolicies.scoped() });
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

  test('without identifyOutgoing, a scoped listener refuses the node once the identity wait is over', async () => {
    const n = identified({ identifyOutgoing: false, identityTimeout: 100 });
    const uri = await n.listenMemory({ policy: ChannelPolicies.scoped() });
    n.registerService(svc, tagged('svc'));
    const channel = await n.openChannelAt(uri, at(svc));
    expect(await tagOf(channel)).toBe('closed unauthenticated');
    expect(channel.connection.peerIdentity).toBeUndefined();
  });
});
