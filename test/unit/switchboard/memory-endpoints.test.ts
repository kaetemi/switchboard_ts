/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// In-process listeners (`mem://<id>`): listenMemory, the registry of ids,
// dialling, pooling and close over them. A port of the Dart
// memory_endpoint_test.dart, over the toy mux and, from `over-frame-mux/`,
// over the frame mux. Two Dart cases live elsewhere: "a client closing
// right after connecting sends GOAWAY" is in `switchboard.test.ts` (over
// mem:// there too), and the "proxy over mem" group is the proxyHandler
// case "a record pointing back at the proxy is refused at once" in
// `proxy.test.ts`.

import { describe, expect, test } from 'vitest';
import { ChannelAddress } from '../../../src/core/address/index.ts';
import { utf8Decode } from '../../../src/core/bytes.ts';
import type { MuxChannel, MuxConnection } from '../../../src/core/mux/contract.ts';
import { Name } from '../../../src/core/name.ts';
import { StatusCode } from '../../../src/core/status.ts';
import { ChannelPolicies } from '../../../src/core/switchboard/channel-policy.ts';
import type { IncomingChannel } from '../../../src/core/switchboard/contract.ts';
import type { Switchboard } from '../../../src/core/switchboard/switchboard.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';
import {
  bytes,
  codeOf,
  memoryRegistry,
  node,
  rejectsWith,
  until,
} from '../../support/node/harness.ts';

const echo = Name.of('echo');

/** Answers every request with `<instance>:<payload>`. */
function serveEcho(incoming: IncomingChannel): void {
  void incoming.talk().serve((m) => {
    void m.reply(`${incoming.address.instance}:${m.text()}`);
  });
}

/** The next frame [channel] receives, as text; leaves the readable usable (no cancel). */
async function nextFrame(channel: MuxChannel): Promise<string> {
  const reader = channel.readable.getReader();
  try {
    const { value } = await reader.read();
    if (value === undefined) {
      throw new Error('no frame');
    }
    return utf8Decode(value);
  } finally {
    reader.releaseLock();
  }
}

/** One request on a fresh Talk channel to [address] at [endpoint]. */
async function ask(
  from: Switchboard,
  endpoint: string,
  address: ChannelAddress,
  payload: string,
): Promise<string> {
  const talk = await from.openTalkAt(endpoint, address);
  try {
    return (await talk.request('ECHO', payload)).text();
  } finally {
    await talk.close();
  }
}

/** The connections [s] reports from now on. */
function collect(s: Switchboard): MuxConnection[] {
  const seen: MuxConnection[] = [];
  void (async () => {
    for await (const connection of s.connections) seen.push(connection);
  })();
  return seen;
}

/** The id of a `mem://` URI. */
function idOf(uri: string): string {
  return new URL(uri).host;
}

describe('listenMemory', () => {
  test('returns mem:// URIs, listed until close', async () => {
    const server = node();
    const generated = await server.listenMemory();
    const another = await server.listenMemory();
    const named = await server.listenMemory({ name: 'Naming.Service_1' });
    const url = new URL(generated);
    expect(url.protocol).toBe('mem:');
    expect(url.host).not.toBe('');
    expect(url.port).toBe('');
    expect(url.pathname).toBe('');
    expect(idOf(another)).not.toBe(idOf(generated));
    // Names are compared, and carried, in lower case.
    expect(named).toBe('mem://naming.service_1');
    // Stable through the text form, as a naming service record needs.
    expect(new URL(generated).href).toBe(generated);
    expect(server.listeningEndpoints).toEqual([generated, another, named]);
    expect(new Set(memoryRegistry().ids)).toEqual(
      new Set([idOf(generated), idOf(another), idOf(named)]),
    );
    await server.close();
    expect(server.listeningEndpoints).toEqual([]);
    expect(memoryRegistry().ids).toEqual([]);
    await rejectsWith(server.listenMemory(), StatusCode.failedPrecondition);
  });

  test('a name is taken until its node closes; invalid names', async () => {
    const firstNode = node();
    await firstNode.listenMemory({ name: 'shared' });
    const second = node();
    await rejectsWith(second.listenMemory({ name: 'SHARED' }), StatusCode.alreadyExists);
    for (const bad of ['', 'a b', 'a/b', 'a:1', 'a#b', 'é']) {
      await expect(second.listenMemory({ name: bad }), bad).rejects.toThrow(RangeError);
    }
    expect(second.listeningEndpoints).toEqual([]);
    await firstNode.close();
    expect(await second.listenMemory({ name: 'shared' })).toBe('mem://shared');
  });

  test('generated ids are unique', async () => {
    const server = node();
    const ids = new Set<string>();
    for (let i = 0; i < 100; i++) {
      ids.add(await server.listenMemory());
    }
    expect(ids.size).toBe(100);
    expect(server.listeningEndpoints).toHaveLength(100);
  });
});

describe('connecting', () => {
  async function setUp(): Promise<{
    server: Switchboard;
    uri: string;
    accepted: MuxConnection[];
    client: Switchboard;
  }> {
    const server = node();
    server.registerService(echo, serveEcho, { instance: 3 });
    const uri = await server.listenMemory();
    const accepted = collect(server);
    return { server, uri, accepted, client: node() };
  }

  test('talk request and reply through dispatch', async () => {
    const { uri, accepted, client } = await setUp();
    expect(await ask(client, uri, new ChannelAddress({ type: echo, instance: 3 }), 'hi')).toBe(
      '3:hi',
    );
    // Any instance goes to the registered one.
    expect(await ask(client, uri, new ChannelAddress({ type: echo }), 'x')).toBe('0:x');
    expect(accepted).toHaveLength(1);
    expect(accepted[0]?.isInitiator).toBe(false);
    // Unclaimed types are not found, as over any listener.
    const missing = await client.openChannelAt(
      uri,
      new ChannelAddress({ type: Name.of('missing') }),
    );
    expect(codeOf(await missing.done)).toBe('notFound');
  });

  test('a request loop does not starve timers', async () => {
    // Frames go through the event loop, as from a socket: with microtask
    // delivery this loop would run to its cap before the timer.
    const { uri, client } = await setUp();
    let fired = false;
    setTimeout(() => {
      fired = true;
    }, 20);
    const talk = await client.openTalkAt(uri, new ChannelAddress({ type: echo }));
    let requests = 0;
    while (!fired && requests < 20000) {
      await talk.request('ECHO', `${requests++}`);
    }
    await talk.close();
    expect(fired).toBe(true);
    expect(requests).toBeGreaterThan(0);
    expect(requests).toBeLessThan(20000);
  });

  test('the accepting side sees the initiator by its mem endpoint', async () => {
    const { server, uri, client } = await setUp();
    const remotes: (string | undefined)[] = [];
    const who = Name.of('who');
    server.registerService(who, (incoming) => {
      remotes.push(incoming.remote);
      void incoming.channel.close();
    });
    await (await client.openChannelAt(uri, new ChannelAddress({ type: who }))).done;
    const named = node();
    await named.listenMemory({ name: 'client-a' });
    await (await named.openChannelAt(uri, new ChannelAddress({ type: who }))).done;
    expect(remotes).toEqual(['memory peer', 'mem://client-a']);
  });

  test('the listener policy applies', async () => {
    const { server, uri, client } = await setUp();
    const guarded = await server.listenMemory({ policy: ChannelPolicies.allowTypes([echo]) });
    const secret = Name.of('secret');
    server.registerService(secret, (incoming) => {
      void incoming.channel.close();
    });
    const refused = await client.openChannelAt(guarded, new ChannelAddress({ type: secret }));
    const status = await refused.done;
    expect(codeOf(status)).toBe('permissionDenied');
    expect(status.reason).toBe('permission denied');
    expect(await ask(client, guarded, new ChannelAddress({ type: echo }), 'ok')).toBe('0:ok');
    // The listener without a policy reaches everything.
    const open = await client.openChannelAt(uri, new ChannelAddress({ type: secret }));
    expect(codeOf(await open.done)).toBe('ok');
  });

  test('connections are pooled per id', async () => {
    const { uri, accepted, client } = await setUp();
    const both = await Promise.all([client.connect(uri), client.connect(uri)]);
    expect(both[0]).toBe(both[1]);
    expect(both[0].isInitiator).toBe(true);
    // Case, fragment, port, path and query do not matter.
    const id = idOf(uri);
    for (const spelling of [`MEM://${id.toUpperCase()}#echo/3`, `mem://${id}:9/x?y`]) {
      expect(await client.connect(spelling), spelling).toBe(both[0]);
    }
    const a = await client.openChannelAt(uri, new ChannelAddress({ type: echo }));
    const b = await client.openChannelAt(uri, new ChannelAddress({ type: echo }));
    expect(a.connection).toBe(b.connection);
    expect(accepted).toHaveLength(1);
    // A closed connection is replaced.
    await accepted[0]?.close();
    expect(codeOf(await both[0].done)).toBe('connectionLost');
    const again = await client.connect(uri);
    expect(again).not.toBe(both[0]);
    expect(await ask(client, uri, new ChannelAddress({ type: echo }), 'z')).toBe('0:z');
    expect(accepted).toHaveLength(2);
  });

  test('an unknown id is UNAVAILABLE, a missing one INVALID_ARGUMENT, and neither is cached', async () => {
    const { client } = await setUp();
    const unknown = 'mem://nobody-here';
    await rejectsWith(client.connect(unknown), StatusCode.unavailable);
    await rejectsWith(
      client.openChannelAt(unknown, new ChannelAddress({ type: echo })),
      StatusCode.unavailable,
    );
    await rejectsWith(client.connect('mem:'), StatusCode.invalidArgument);
    // Someone starts listening there: the next attempt succeeds.
    const revived = node();
    revived.registerService(echo, serveEcho);
    await revived.listenMemory({ name: 'nobody-here' });
    expect(await ask(client, unknown, new ChannelAddress({ type: echo }), 'l')).toBe('0:l');
  });

  test('close: GOAWAY on accepted connections, the id released at once', async () => {
    const { server, uri, client } = await setUp();
    const held = new Deferred<IncomingChannel>();
    const hold = Name.of('hold');
    server.registerService(hold, (incoming) => held.resolve(incoming));
    const channel = await client.openChannelAt(uri, new ChannelAddress({ type: hold }));
    const atServer = await held.promise;
    const connection = channel.connection;
    const closing = server.close();
    expect(memoryRegistry().lookup(idOf(uri))).toBeUndefined();
    expect(server.listeningEndpoints).toEqual([]);
    await until(() => connection.peerGoingAway);
    // The pooled connection is going away and nobody listens any more.
    await rejectsWith(client.connect(uri), StatusCode.unavailable);
    // The held channel still works on the old connection.
    void atServer.channel.send(bytes('still here'));
    expect(await nextFrame(channel)).toBe('still here');
    await channel.close();
    await closing;
    expect(codeOf(await connection.done)).toBe('goingAway');
    expect(codeOf(await channel.done)).toBe('ok');
  });

  test('isOwnEndpoint matches the id', async () => {
    const { server, uri, client } = await setUp();
    const id = idOf(uri);
    expect(server.isOwnEndpoint(uri)).toBe(true);
    expect(server.isOwnEndpoint(`MEM://${id.toUpperCase()}:5/x`)).toBe(true);
    expect(server.isOwnEndpoint(`mem://${id}x`)).toBe(false);
    expect(server.isOwnEndpoint(`tcp://${id}:1`)).toBe(false);
    expect(client.isOwnEndpoint(uri)).toBe(false);
  });
});
