/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { describe, expect, test } from 'vitest';
import { ChannelAddress, ServiceAddress } from '../../../src/core/address/index.ts';
import { bytesEqual, utf8Decode } from '../../../src/core/bytes.ts';
import type { MuxConnection } from '../../../src/core/mux/contract.ts';
import { Name } from '../../../src/core/name.ts';
import { serviceRecord } from '../../../src/core/naming/naming-protocol.ts';
import { StatusCode } from '../../../src/core/status.ts';
import { ChannelPolicies } from '../../../src/core/switchboard/channel-policy.ts';
import type { ChannelPolicy, ServiceRecord } from '../../../src/core/switchboard/contract.ts';
import { StaticResolver } from '../../../src/core/switchboard/resolver.ts';
import type { Switchboard, SwitchboardOptions } from '../../../src/core/switchboard/switchboard.ts';
import { memoryTransportPair } from '../../../src/core/transport/memory-transport.ts';
import {
  bytes,
  currentRuntime,
  first,
  node,
  rejectsWith,
  svc,
  tagged,
  tagOf,
  until,
} from '../../support/node/harness.ts';

const back = Name.of('back');
const meshSecret = bytes('mesh-secret');

/** Whether [needle] occurs anywhere in [haystack]. */
function containsBytes(haystack: Uint8Array, needle: Uint8Array): boolean {
  for (let i = 0; i + needle.length <= haystack.length; i++) {
    if (bytesEqual(haystack.subarray(i, i + needle.length), needle)) {
      return true;
    }
  }
  return false;
}

/** A node that records the OPEN payloads it receives and answers every channel with its name. */
class Target {
  readonly name: string;
  readonly id: number;
  sb!: Switchboard;
  uri!: string;
  readonly raw: Uint8Array[] = [];
  readonly accepted: MuxConnection[] = [];

  constructor(name: string, id = 1) {
    this.name = name;
    this.id = id;
  }

  /** The application payloads, as text. */
  get payloads(): string[] {
    return this.raw.map((open) => utf8Decode(ChannelAddress.decode(open).payload));
  }

  record(metadata?: Uint8Array): ServiceRecord {
    return serviceRecord(new ServiceAddress(svc, this.id), {
      endpoints: [this.uri],
      metadata: metadata,
    });
  }

  async start(): Promise<this> {
    this.sb = node();
    this.sb.registerService(
      svc,
      (incoming) => {
        this.raw.push(incoming.channel.openPayload);
        tagged(this.name)(incoming);
      },
      { acceptAnyInstance: true },
    );
    void (async () => {
      for await (const connection of this.sb.connections) this.accepted.push(connection);
    })();
    this.uri = await this.sb.listenMemory();
    return this;
  }

  /** Pushes a channel to `back` on the connection the peer accepted [index]th, and reads the answer. */
  push(index = 0): Promise<string> {
    return tagOf(
      this.sb.openChannelOn(
        this.accepted[index] as MuxConnection,
        new ChannelAddress({ type: back }),
      ),
    );
  }
}

const refused = 'closed permissionDenied';

describe('outgoing policy', () => {
  /** A client with a local service `back`, connected to the server with [policy]; what the server's push got. */
  async function pushed(
    server: Target,
    client: Switchboard,
    policy?: ChannelPolicy,
  ): Promise<string> {
    client.registerService(back, tagged('back'));
    await client.connect(server.uri, { policy });
    await until(() => server.accepted.length > 0);
    return server.push();
  }

  test('none: channels the peer pushes are dispatched', async () => {
    const server = await new Target('server').start();
    expect(await pushed(server, node())).toBe('back');
  });

  test('outgoingPolicy applies to every initiated connection', async () => {
    const server = await new Target('server').start();
    expect(await pushed(server, node({ outgoingPolicy: ChannelPolicies.denyAll }))).toBe(refused);
  });

  test('endpointPolicy chooses by endpoint', async () => {
    const server = await new Target('server').start();
    const seen: string[] = [];
    const client = node({
      endpointPolicy: (endpoint) => {
        seen.push(endpoint);
        return ChannelPolicies.denyAll;
      },
    });
    expect(await pushed(server, client)).toBe(refused);
    expect(seen).toEqual([server.uri]);
  });

  test('endpointPolicy sees the endpoint as the pool keys it: every spelling gets the same policy', async () => {
    const server = await new Target('server').start();
    const seen: string[] = [];
    // A WebSocket "dialer" that hands its transport to the server node.
    const wsDialled: string[] = [];
    const client = node({
      outgoingPolicy: ChannelPolicies.denyAll,
      endpointPolicy: (endpoint) => {
        seen.push(endpoint);
        return endpoint === server.uri ? ChannelPolicies.allowAll : undefined;
      },
      runtime: {
        ...currentRuntime(),
        dialers: {
          ws: async (endpoint) => {
            wsDialled.push(endpoint);
            const [local, remote] = memoryTransportPair({ delay: 0 });
            await server.sb.accept(remote);
            return local;
          },
        },
      },
    });
    client.registerService(back, tagged('back'));
    // A mem endpoint is its id: port, path, query and fragment ignored.
    const spelled = `${server.uri.toUpperCase().replace('MEM://', 'mem://')}:7/x?q#svc/1`;
    await client.dial(spelled);
    await client.connect(spelled);
    expect(seen).toEqual([server.uri, server.uri]);
    await until(() => server.accepted.length >= 2);
    expect(await server.push(0)).toBe('back');
    expect(await server.push(1)).toBe('back');
    // WebSocket: the default port and path filled in, the fragment
    // dropped; both spellings share the pooled connection.
    seen.length = 0;
    const one = await client.connect('ws://127.0.0.1#svc/1');
    const two = await client.connect('WS://127.0.0.1:80/');
    expect(two).toBe(one);
    await client.dial('ws://127.0.0.1/#x');
    expect(seen).toEqual(['ws://127.0.0.1:80/', 'ws://127.0.0.1:80/']);
    expect(wsDialled).toEqual(['ws://127.0.0.1', 'ws://127.0.0.1/']);
    // The path and query are kept apart.
    const other = await client.connect('ws://127.0.0.1/other?a=1');
    expect(other).not.toBe(one);
    expect(seen.at(-1)).toBe('ws://127.0.0.1:80/other?a=1');
  });

  test('connect(policy) applies to that connection', async () => {
    const server = await new Target('server').start();
    expect(await pushed(server, node(), ChannelPolicies.denyAll)).toBe(refused);
  });

  test('precedence: endpointPolicy over outgoingPolicy', async () => {
    const server = await new Target('server').start();
    const client = node({
      outgoingPolicy: ChannelPolicies.denyAll,
      endpointPolicy: () => ChannelPolicies.allowAll,
    });
    expect(await pushed(server, client)).toBe('back');
  });

  test('precedence: an undefined endpointPolicy leaves it to outgoingPolicy', async () => {
    const server = await new Target('server').start();
    const client = node({
      outgoingPolicy: ChannelPolicies.denyAll,
      endpointPolicy: () => undefined,
    });
    expect(await pushed(server, client)).toBe(refused);
  });

  test('precedence: an explicit policy over endpointPolicy', async () => {
    const server = await new Target('server').start();
    const client = node({ endpointPolicy: () => ChannelPolicies.denyAll });
    expect(await pushed(server, client, ChannelPolicies.allowAll)).toBe('back');
  });

  test('a throwing endpointPolicy refuses everything', async () => {
    const server = await new Target('server').start();
    const client = node({
      endpointPolicy: () => {
        throw new Error('broken');
      },
    });
    expect(await pushed(server, client)).toBe(refused);
  });

  test('an outgoing policy that allows some types', async () => {
    const server = await new Target('server').start();
    const client = node({ outgoingPolicy: ChannelPolicies.allowTypes([back]) });
    expect(await pushed(server, client)).toBe('back');
    client.registerService('other', tagged('other'));
    expect(
      await tagOf(
        server.sb.openChannelOn(
          server.accepted[0] as MuxConnection,
          new ChannelAddress({ type: 'other' }),
        ),
      ),
    ).toBe(refused);
  });

  test('connections are pooled per explicit policy', async () => {
    const server = await new Target('server').start();
    const client = node();
    const plain = await client.connect(server.uri);
    expect(await client.connect(server.uri)).toBe(plain);
    const strict = await client.connect(server.uri, { policy: ChannelPolicies.denyAll });
    expect(strict).not.toBe(plain);
    expect(await client.connect(server.uri, { policy: ChannelPolicies.denyAll })).toBe(strict);
    expect(await client.connect(server.uri)).toBe(plain);
  });

  test('dial: not pooled, adopted for dispatch, policy, GOAWAY on close', async () => {
    const server = await new Target('server').start();
    const client = node({ outgoingPolicy: ChannelPolicies.denyAll });
    client.registerService(back, tagged('back'));
    const reported: MuxConnection[] = [];
    void (async () => {
      for await (const connection of client.connections) reported.push(connection);
    })();
    const pooled = await client.connect(server.uri);
    const one = await client.dial(server.uri);
    const two = await client.dial(server.uri, { policy: ChannelPolicies.allowAll });
    expect(one).not.toBe(pooled);
    expect(two).not.toBe(one);
    expect(await client.connect(server.uri)).toBe(pooled);
    expect(one.isInitiator).toBe(true);
    await until(() => server.accepted.length === 3);
    expect(reported).toEqual([pooled, one, two]);
    expect(await server.push(1)).toBe(refused);
    expect(await server.push(2)).toBe('back');
    // Channels opened on a dialled connection follow the payload rules.
    expect(await tagOf(client.openChannelOn(one, new ChannelAddress({ type: svc })))).toBe(
      'server',
    );
    await client.close();
    await one.done;
    await two.done;
    await rejectsWith(client.dial(server.uri), StatusCode.failedPrecondition);
  });

  test('dial fails like connect', async () => {
    const client = node();
    await rejectsWith(client.dial('mem://nobody-listens-here'), StatusCode.unavailable);
    await rejectsWith(client.dial('foo://x'), StatusCode.unimplemented);
    expect(() => client.dial('mem://x', { intent: new Uint8Array(65) })).toThrow(RangeError);
    expect(() => client.dial('mem://x', { receiver: 'x'.repeat(256) })).toThrow(RangeError);
    // Identifying needs a credential.
    await rejectsWith(
      client.dial('mem://x', { intent: new Uint8Array(4) }),
      StatusCode.failedPrecondition,
    );
    await rejectsWith(client.dial('mem://x', { receiver: 'peer' }), StatusCode.failedPrecondition);
  });

  test('a dial abandoned by its signal sends the late connection away', async () => {
    const server = await new Target('server').start();
    const client = node();
    const controller = new AbortController();
    const attempt = client.dial(server.uri, { signal: controller.signal });
    controller.abort(new Error('gone'));
    await expect(attempt).rejects.toThrow('gone');
    await until(() => server.accepted.length === 1);
    expect((await (server.accepted[0] as MuxConnection).done).code).toBe(StatusCode.goingAway);
  });

  test('ChannelPolicies.denyAll refuses everything', () => {
    const connection = {} as MuxConnection;
    expect(ChannelPolicies.denyAll(new ChannelAddress(), connection)).toBe(false);
    expect(ChannelPolicies.denyAll(new ChannelAddress({ type: svc }), connection)).toBe(false);
  });
});

describe('credentialFor', () => {
  async function setUp(): Promise<{
    a: Target;
    b: Target;
    c: Target;
    resolver: StaticResolver;
    calls: [string, ServiceRecord | undefined][];
    consumer: (async?: boolean) => Switchboard;
    expectNoMeshSecret: () => void;
  }> {
    const a = await new Target('A', 1).start();
    const b = await new Target('B', 2).start();
    const c = await new Target('C', 3).start();
    const resolver = new StaticResolver([a.record(), b.record(), c.record()]);
    const calls: [string, ServiceRecord | undefined][] = [];
    /** A consumer whose hook knows the keys of A and B, not C. */
    const consumer = (async = false): Switchboard =>
      node({
        resolver,
        defaultPayload: meshSecret,
        credentialFor: (endpoint, record) => {
          calls.push([endpoint, record]);
          const key =
            endpoint === a.uri ? bytes('key-a') : endpoint === b.uri ? bytes('key-b') : undefined;
          return async ? new Promise((resolve) => setTimeout(() => resolve(key), 0)) : key;
        },
      });
    const expectNoMeshSecret = (): void => {
      for (const peer of [a, b, c]) {
        for (const open of peer.raw) {
          expect(containsBytes(open, meshSecret), peer.name).toBe(false);
        }
      }
    };
    return { a, b, c, resolver, calls, consumer, expectNoMeshSecret };
  }

  for (const async of [false, true]) {
    test(`openChannelAt: the key of each endpoint, never the default (${async ? 'asynchronous' : 'synchronous'} hook)`, async () => {
      const { a, b, c, calls, consumer, expectNoMeshSecret } = await setUp();
      const client = consumer(async);
      const address = new ChannelAddress({ type: svc });
      expect(await tagOf(await client.openChannelAt(a.uri, address))).toBe('A');
      expect(await tagOf(await client.openChannelAt(b.uri, address))).toBe('B');
      expect(await tagOf(await client.openChannelAt(c.uri, address))).toBe('C');
      expect(a.payloads).toEqual(['key-a']);
      expect(b.payloads).toEqual(['key-b']);
      // Unknown to the hook: empty, not the default.
      expect(c.payloads).toEqual(['']);
      expect(calls).toEqual([
        [a.uri, undefined],
        [b.uri, undefined],
        [c.uri, undefined],
      ]);
      expectNoMeshSecret();
    });

    test(`openChannel and openTalk: the hook sees the selected record (${async ? 'asynchronous' : 'synchronous'} hook)`, async () => {
      const { a, b, c, calls, consumer, expectNoMeshSecret } = await setUp();
      const client = consumer(async);
      expect(await tagOf(await client.openChannel(new ServiceAddress(svc, 1)))).toBe('A');
      expect(await tagOf(await client.openChannel(new ServiceAddress(svc, 2)))).toBe('B');
      const talk = await client.openTalk(new ServiceAddress(svc, 3));
      await talk.done;
      expect(a.payloads).toEqual(['key-a']);
      expect(b.payloads).toEqual(['key-b']);
      expect(c.payloads).toEqual(['']);
      expect(calls.map((call) => call[0])).toEqual([a.uri, b.uri, c.uri]);
      expect(calls.map((call) => call[1]?.address.instance)).toEqual([1, 2, 3]);
      expectNoMeshSecret();
    });
  }

  test('an explicit payload wins and the hook is not asked', async () => {
    const { a, b, calls, consumer } = await setUp();
    const client = consumer();
    await tagOf(
      await client.openChannelAt(a.uri, new ChannelAddress({ type: svc, payload: 'explicit' })),
    );
    await tagOf(await client.openChannel(new ServiceAddress(svc, 2), { payload: 'given' }));
    // An explicit empty payload for openChannel is sent as it is.
    await tagOf(
      await client.openChannel(new ServiceAddress(svc, 2), { payload: new Uint8Array(0) }),
    );
    expect(a.payloads).toEqual(['explicit']);
    expect(b.payloads).toEqual(['given', '']);
    expect(calls).toEqual([]);
  });

  test('per-worker keys from the record metadata', async () => {
    const { a, b, c, resolver, expectNoMeshSecret } = await setUp();
    resolver.add(a.record(bytes('meta-a')));
    resolver.add(b.record(bytes('meta-b')));
    const client = node({
      resolver,
      defaultPayload: meshSecret,
      credentialFor: (_endpoint, record) =>
        record !== undefined && record.metadata.length > 0 ? record.metadata : undefined,
    });
    for (let i = 0; i < 2; i++) {
      await tagOf(await client.openChannel(new ServiceAddress(svc, 1)));
      await tagOf(await client.openChannel(new ServiceAddress(svc, 2)));
    }
    // Any instance: whichever is selected gets its own key.
    for (let i = 0; i < 3; i++) {
      await tagOf(await client.openChannel(svc));
    }
    // No record (explicit endpoint): nothing.
    await tagOf(await client.openChannelAt(a.uri, new ChannelAddress({ type: svc })));
    expect(a.payloads.every((p) => p === 'meta-a' || p === '')).toBe(true);
    expect(a.payloads.at(-1)).toBe('');
    expect(a.payloads.filter((p) => p === 'meta-a')).toHaveLength(3);
    expect(b.payloads.every((p) => p === 'meta-b')).toBe(true);
    expect(c.payloads.every((p) => p === '')).toBe(true);
    expect(a.raw.length + b.raw.length + c.raw.length).toBe(8);
    expectNoMeshSecret();
  });

  test('a record for any instance stands for the instance asked for, metadata included', async () => {
    const { a, expectNoMeshSecret } = await setUp();
    const seen: (ServiceRecord | undefined)[] = [];
    const client = node({
      resolver: new StaticResolver([
        serviceRecord(new ServiceAddress(svc), {
          endpoints: [a.uri],
          metadata: bytes('worker-key'),
        }),
      ]),
      defaultPayload: meshSecret,
      credentialFor: (_endpoint, record) => {
        seen.push(record);
        return record === undefined || record.metadata.length === 0 ? undefined : record.metadata;
      },
    });
    await tagOf(await client.openChannel(svc));
    await tagOf(await client.openChannel(new ServiceAddress(svc, 5)));
    expect(seen.map((r) => r?.address.toString())).toEqual(['svc', 'svc/5']);
    expect(seen.every((r) => r !== undefined && utf8Decode(r.metadata) === 'worker-key')).toBe(
      true,
    );
    expect(a.payloads).toEqual(['worker-key', 'worker-key']);
    expect(a.raw.map((open) => ChannelAddress.decode(open).instance)).toEqual([0, 5]);
    expectNoMeshSecret();
  });

  test('openChannelOn: the hook for an initiated connection, empty on an accepted one, TypeError for an asynchronous hook', async () => {
    const { a, b, consumer, expectNoMeshSecret } = await setUp();
    const client = consumer();
    const connection = await client.connect(a.uri);
    expect(await tagOf(client.openChannelOn(connection, new ChannelAddress({ type: svc })))).toBe(
      'A',
    );
    expect(a.payloads).toEqual(['key-a']);
    expect(utf8Decode(await client.payloadFor(connection))).toBe('key-a');
    // Accepted: no destination to choose a credential for.
    const server = node({ defaultPayload: meshSecret, credentialFor: () => bytes('never') });
    const uri = await server.listenMemory();
    const accepted = first(server.connections);
    a.sb.registerService(back, (incoming) => {
      a.raw.push(incoming.channel.openPayload);
      tagged('A back')(incoming);
    });
    await a.sb.connect(uri);
    const pushed = server.openChannelOn(await accepted, new ChannelAddress({ type: back }));
    expect(await tagOf(pushed)).toBe('A back');
    expect(a.payloads.at(-1)).toBe('');
    expect(await server.payloadFor(await accepted)).toHaveLength(0);
    // An asynchronous hook cannot serve this synchronous call.
    const asyncClient = consumer(true);
    const asyncConnection = await asyncClient.connect(b.uri);
    expect(() =>
      asyncClient.openChannelOn(asyncConnection, new ChannelAddress({ type: svc })),
    ).toThrow(TypeError);
    expect(utf8Decode(await asyncClient.payloadFor(asyncConnection))).toBe('key-b');
    expectNoMeshSecret();
  });

  test('without the hook, the default payload as before', async () => {
    const { a, b, c, resolver } = await setUp();
    const client = node({ resolver, defaultPayload: meshSecret });
    await tagOf(await client.openChannel(new ServiceAddress(svc, 1)));
    await tagOf(await client.openChannelAt(b.uri, new ChannelAddress({ type: svc })));
    const connection = await client.connect(c.uri);
    await tagOf(client.openChannelOn(connection, new ChannelAddress({ type: svc })));
    expect(a.payloads).toEqual(['mesh-secret']);
    expect(b.payloads).toEqual(['mesh-secret']);
    expect(c.payloads).toEqual(['mesh-secret']);
    expect(await client.payloadFor(connection)).toEqual(meshSecret);
  });

  test('a hook that throws fails the open', async () => {
    const { a, resolver } = await setUp();
    const options: SwitchboardOptions = {
      resolver,
      defaultPayload: meshSecret,
      credentialFor: () => {
        throw new TypeError('no vault');
      },
    };
    const client = node(options);
    await expect(client.openChannel(new ServiceAddress(svc, 1))).rejects.toThrow('no vault');
    await expect(client.openChannelAt(a.uri, new ChannelAddress({ type: svc }))).rejects.toThrow(
      'no vault',
    );
    const rejecting = node({ ...options, credentialFor: () => Promise.reject(new Error('later')) });
    await expect(rejecting.openChannel(new ServiceAddress(svc, 1))).rejects.toThrow('later');
    expect(a.raw).toEqual([]);
  });
});
