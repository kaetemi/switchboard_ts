/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Ports of the Dart proxy tests (`proxy_test.dart`): pipeChannels between
// two connections, and proxyHandler on a frontend node in front of a
// backend, over the memory transport and the toy mux. The backends answer
// raw subframes (`WHO`, `KILL`) where the Dart backends answer Talk
// requests; the tests that need the Talk layer, bulk routing, the frame
// mux's flow control or slot routing are listed as todo until those land.

import { beforeEach, describe, expect, it } from 'vitest';
import { ChannelAddress, ServiceAddress } from '../../../src/core/address/index.ts';
import { bytesEqual, utf8Decode } from '../../../src/core/bytes.ts';
import type { MuxChannel, MuxConnection, MuxOptions } from '../../../src/core/mux/contract.ts';
import { Name } from '../../../src/core/name.ts';
import { serviceRecord } from '../../../src/core/naming/naming-protocol.ts';
import { Status, StatusCode, statusCodeName } from '../../../src/core/status.ts';
import type {
  ChannelPolicy,
  IncomingChannel,
  ServiceRecord,
} from '../../../src/core/switchboard/contract.ts';
import { pipeChannels, proxyHandler } from '../../../src/core/switchboard/proxy.ts';
import { EndpointResolver, StaticResolver } from '../../../src/core/switchboard/resolver.ts';
import { Switchboard } from '../../../src/core/switchboard/switchboard.ts';
import { memoryTransportPair } from '../../../src/core/transport/memory-transport.ts';
import { Frames, framesOf, nextText, Pull, pattern } from '../../support/node/channels.ts';
import {
  bytes,
  currentRuntime,
  fast,
  node,
  onCleanup,
  pump,
  until,
} from '../../support/node/harness.ts';
import { toyMux } from '../../support/node/toy-mux.ts';

const chat = Name.of('chat');

/** The name of [status]'s code. */
function codeOf(status: Status): string | undefined {
  return statusCodeName(status.code);
}

function expectCode(status: Status, code: StatusCode): void {
  expect(codeOf(status)).toBe(statusCodeName(code));
}

function expectStatus(status: Status, code: StatusCode, reason: string): void {
  expectCode(status, code);
  expect(status.reason).toBe(reason);
}

/** Two toy connections over a memory pair, closed after the test. */
function muxPair(
  initiator: Partial<MuxOptions> = fast,
  acceptor: Partial<MuxOptions> = fast,
): [MuxConnection, MuxConnection] {
  const [a, b] = memoryTransportPair({ delay: 0 });
  const initiating = toyMux(a, { ...fast, ...initiator }, true);
  const accepting = toyMux(b, { ...fast, ...acceptor }, false);
  onCleanup(async () => {
    await initiating.close();
    await accepting.close();
  });
  return [initiating, accepting];
}

/** client ⇄ (proxyIn | pipe | proxyOut) ⇄ backend. */
interface Piped {
  readonly clientConnection: MuxConnection;
  readonly proxyIn: MuxConnection;
  readonly proxyOut: MuxConnection;
  readonly backendConnection: MuxConnection;
  readonly client: MuxChannel;
  readonly inbound: MuxChannel;
  readonly backend: MuxChannel;
  readonly pipe: Promise<void>;
}

/** Sets up a piped channel; [early] runs on the client channel before the proxy attaches the pipe. */
async function piped(
  init: {
    early?: (client: MuxChannel) => void;
    options?: Partial<MuxOptions>;
    backendOptions?: Partial<MuxOptions>;
  } = {},
): Promise<Piped> {
  const options = init.options ?? fast;
  const [clientConnection, proxyIn] = muxPair(options, options);
  const [proxyOut, backendConnection] = muxPair(options, init.backendOptions ?? options);
  await clientConnection.ping();
  await proxyOut.ping();
  const atProxy = new Pull(proxyIn.incoming);
  const atBackend = new Pull(backendConnection.incoming);
  const client = clientConnection.open(bytes('header'));
  init.early?.(client);
  await pump();
  const inbound = await atProxy.next();
  const outbound = proxyOut.open(inbound.openPayload);
  const pipe = pipeChannels(inbound, outbound);
  const backend = await atBackend.next();
  expect(backend.openPayload).toEqual(bytes('header'));
  return {
    clientConnection,
    proxyIn,
    proxyOut,
    backendConnection,
    client,
    inbound,
    backend,
    pipe,
  };
}

/** The first [count] subframes of [channel]. */
async function take(channel: MuxChannel, count: number): Promise<Uint8Array[]> {
  const frames = new Frames(channel);
  const taken: Uint8Array[] = [];
  for (let i = 0; i < count; i++) {
    const frame = await frames.next();
    if (frame === undefined) break;
    taken.push(frame);
  }
  return taken;
}

describe('pipeChannels', () => {
  it.todo(
    'the loss of the backend while an item window is exhausted fails the requester with the ' +
      'channel status (needs Talk)',
  );
  it.todo('a bulk drain timeout forwards failure and cancels the twins (needs Talk bulk routing)');
  it.todo(
    'a bulk payload whose source ended waits for a reader slower than the close confirmation ' +
      'timeout (needs Talk)',
  );
  it.todo('failed parent cancels forwarded bulk twins (client: false) (needs Talk bulk routing)');
  it.todo('failed parent cancels forwarded bulk twins (client: true) (needs Talk bulk routing)');
  it.todo('a send rejected by delayed LIMITS closes both pipe ends (needs the frame mux)');

  it('forwards subframes both ways, in order', async () => {
    const p = await piped();
    const toBackend = Array.from({ length: 100 }, (_, i) => pattern(i, i));
    const toClient = Array.from({ length: 100 }, (_, i) => pattern(i + 1, -i));
    const atBackend = take(p.backend, 100);
    const atClient = take(p.client, 100);
    for (let i = 0; i < 100; i++) {
      void p.client.send(toBackend[i] as Uint8Array);
      void p.backend.send(toClient[i] as Uint8Array);
    }
    expect(await atBackend).toEqual(toBackend);
    expect(await atClient).toEqual(toClient);
    await p.client.close();
    await p.pipe;
  });

  it.todo('a graceful close follows the bulk payloads forwarded before it (needs Talk)');
  it.todo(
    'credit passes hop by hop: a slow far consumer slows the near sender, the proxy holds at ' +
      'most a window (needs the frame mux)',
  );
  it.todo(
    "a client that does not read holds the proxy's data within the window, and does not pause " +
      'the backend connection (needs the frame mux)',
  );

  it('forwards large subframes', async () => {
    const p = await piped({ options: { ...fast, initialWindow: 2 * 1024 * 1024 } });
    const big = pattern(900 * 1024, 7);
    const atBackend = take(p.backend, 1);
    void p.client.send(big);
    expect(bytesEqual((await atBackend)[0] ?? new Uint8Array(0), big)).toBe(true);
    const atClient = take(p.client, 1);
    void p.backend.send(big);
    expect(bytesEqual((await atClient)[0] ?? new Uint8Array(0), big)).toBe(true);
    await p.backend.close();
    await p.pipe;
  });

  it('client close status reaches the backend', async () => {
    const p = await piped();
    const atBackend = framesOf(p.backend);
    void p.client.send(bytes('last'));
    await p.client.close(Status.of(StatusCode.cancelled, 'bye'));
    expect(await atBackend).toEqual([bytes('last')]);
    expectStatus(await p.backend.done, StatusCode.cancelled, 'bye');
    expectCode(await p.client.done, StatusCode.cancelled);
    await p.pipe;
  });

  it('backend close status reaches the client', async () => {
    const p = await piped();
    const atClient = framesOf(p.client);
    void p.backend.send(bytes('result'));
    await p.backend.close(Status.of(StatusCode.aborted, 'conflict'));
    expect(await atClient).toEqual([bytes('result')]);
    expectStatus(await p.client.done, StatusCode.aborted, 'conflict');
    await p.pipe;
  });

  it('an OK close passes as OK', async () => {
    const p = await piped();
    await p.client.close();
    expectCode(await p.backend.done, StatusCode.ok);
    await p.pipe;
  });

  it('lost backend connection is UNAVAILABLE at the client', async () => {
    const p = await piped();
    await p.backendConnection.close();
    expectCode(await p.client.done, StatusCode.unavailable);
    await p.pipe;
  });

  it('lost client connection is UNAVAILABLE at the backend', async () => {
    const p = await piped();
    await p.clientConnection.close();
    expectCode(await p.backend.done, StatusCode.unavailable);
    await p.pipe;
  });

  it('an application code from a broken peer is sent as UNKNOWN', async () => {
    const p = await piped();
    // Mux CLOSE must not carry application codes; the toy peer sends one anyway.
    await p.backend.close(new Status(300));
    const status = await p.client.done;
    expectStatus(status, StatusCode.unknown, 'application status 300');
    await p.pipe;
  });

  it('frames sent before the pipe is attached are not lost', async () => {
    const p = await piped({
      early: (client) => {
        void client.send(bytes('one'));
        void client.send(new Uint8Array(0));
        void client.send(bytes('three'));
        void client.close(Status.of(StatusCode.cancelled));
      },
    });
    expect(await framesOf(p.backend)).toEqual([bytes('one'), new Uint8Array(0), bytes('three')]);
    expectCode(await p.backend.done, StatusCode.cancelled);
    await p.pipe;
  });

  it.todo('a subframe over the receiving peer limit closes both (needs the frame mux)');

  it('a stream already listened to closes both channels', async () => {
    const [clientConnection, proxyIn] = muxPair();
    const [proxyOut, backendConnection] = muxPair();
    const client = clientConnection.open(bytes('x'));
    const inbound = await new Pull(proxyIn.incoming).next();
    const atBackend = new Pull(backendConnection.incoming).next();
    const outbound = proxyOut.open(inbound.openPayload);
    inbound.readable.getReader();
    await pipeChannels(inbound, outbound);
    expectCode(await client.done, StatusCode.internal);
    expectCode(await (await atBackend).done, StatusCode.internal);
  });

  it.todo(
    'a far end that cancels its subscription without closing keeps the pipe waiting for done ' +
      '(a cancelled readable has no Dart counterpart until the frame mux defines it)',
  );
  it.todo('talk works through the pipe (needs Talk)');
});

/** Sends [command] on [channel] and reads the answer. */
async function ask(channel: MuxChannel, command: string): Promise<string> {
  await channel.send(bytes(command));
  return nextText(channel);
}

/** A backend handler answering `WHO` with what it sees of the address and `KILL` with ABORTED. */
function serveChat(record: IncomingChannel[]): (incoming: IncomingChannel) => void {
  return (incoming) => {
    record.push(incoming);
    const frames = new Frames(incoming.channel);
    void (async () => {
      for (;;) {
        const frame = await frames.next();
        if (frame === undefined) return;
        const a = incoming.address;
        switch (utf8Decode(frame)) {
          case 'WHO':
            void incoming.channel.send(
              bytes(
                `${a.type} ${a.instance} ${a.shard ?? 'null'} ${a.host ?? 'null'} ` +
                  utf8Decode(a.payload),
              ),
            );
            break;
          case 'KILL':
            void incoming.reject(Status.of(StatusCode.aborted, 'killed'));
            break;
        }
      }
    })();
  };
}

/** A node that is a copy of [uri] in upper case: another spelling of the same `mem` listener. */
function upper(uri: string): string {
  return uri.toUpperCase().replace('MEM://', 'mem://');
}

describe('proxyHandler', () => {
  let backend: Switchboard;
  let endpoint: Switchboard;
  let client: Switchboard;
  let backendUri: string;
  let endpointUri: string;
  let atBackend: IncomingChannel[];

  function record(type: Name, instance: number, endpoints: string[]): ServiceRecord {
    return serviceRecord(new ServiceAddress(type, instance), { endpoints });
  }

  function staticResolver(records: ServiceRecord[]): StaticResolver {
    const resolver = new StaticResolver(records);
    onCleanup(() => resolver.close());
    return resolver;
  }

  beforeEach(async () => {
    atBackend = [];
    backend = node({ defaultPayload: bytes('backend credential') });
    backendUri = await backend.listenMemory();
    backend.registerService(chat, serveChat(atBackend), { instance: 7 });
    const backends = staticResolver([
      record(chat, 7, [backendUri]),
      record(Name.of('down'), 1, ['mem://nobody-listens-here']),
    ]);
    endpoint = node({ defaultPayload: bytes('endpoint credential') });
    const allowed = [chat, Name.of('void'), Name.of('down')];
    endpoint.catchAll = proxyHandler(endpoint, {
      allow: (address) => allowed.some((type) => type.equals(address.type)),
      resolver: backends,
    });
    endpoint.registerService('api', (incoming) => {
      void (async () => {
        const frames = new Frames(incoming.channel);
        while ((await frames.next()) !== undefined) {
          void incoming.channel.send(bytes('local'));
        }
      })();
    });
    endpointUri = await endpoint.listenMemory();
    client = node({
      resolver: new EndpointResolver(endpointUri),
      defaultPayload: bytes('client token'),
    });
  });

  it('request and reply through the endpoint', async () => {
    const channel = await client.openChannel(chat);
    // The backend sees the client's own payload and the instance the proxy
    // selected; the shard slot passes through.
    expect(await ask(channel, 'WHO')).toBe('chat 7 null null client token');
    const sharded = await client.openChannel(chat, { shard: 9 });
    expect(await ask(sharded, 'WHO')).toBe('chat 7 9 null client token');
    // Local services on the endpoint are not proxied.
    const api = await client.openChannel('api');
    expect(await ask(api, 'X')).toBe('local');
    await channel.close();
    await sharded.close();
    await api.close();
  });

  it.todo('bulk payloads through the endpoint, both ways (needs Talk)');
  it.todo('bulk channels count toward the per-connection bound (needs Talk bulk routing)');
  it.todo('stream through the endpoint (needs Talk)');
  it.todo(
    'a stream with a finite item window through the endpoint: grants, a request served ' +
      'meanwhile, a cancel (needs Talk)',
  );

  it('rejections', async () => {
    const statusOf = async (type: string, instance = 0): Promise<Status> =>
      (await client.openChannel(new ServiceAddress(Name.of(type), instance))).done;
    expectCode(await statusOf('admin'), StatusCode.permissionDenied);
    expectCode(await statusOf('void'), StatusCode.notFound);
    expectCode(await statusOf('chat', 8), StatusCode.notFound);
    expectCode(await statusOf('down'), StatusCode.unavailable);
    const untyped = await client.openChannelAt(endpointUri, new ChannelAddress());
    expectCode(await untyped.done, StatusCode.permissionDenied);
  });

  it('a channel without a type is not found when allowed', async () => {
    const open = node();
    open.catchAll = proxyHandler(open);
    const uri = await open.listenMemory();
    const channel = await client.openChannelAt(uri, new ChannelAddress());
    expectCode(await channel.done, StatusCode.notFound);
    // No resolver and no host hint support: the proxy cannot forward,
    // which the client sees as UNAVAILABLE.
    const typed = await client.openChannelAt(uri, new ChannelAddress({ type: chat }));
    expectStatus(await typed.done, StatusCode.unavailable, 'unavailable');
  });

  it('backend close status reaches the client', async () => {
    const channel = await client.openChannel(chat);
    void channel.send(bytes('KILL'));
    expectStatus(await channel.done, StatusCode.aborted, 'killed');
  });

  it('client close status reaches the backend', async () => {
    const channel = await client.openChannel(chat);
    await ask(channel, 'WHO');
    await channel.close(Status.of(StatusCode.cancelled, 'leaving'));
    expectStatus(
      await (atBackend[0] as IncomingChannel).channel.done,
      StatusCode.cancelled,
      'leaving',
    );
  });

  it('backend connection loss is UNAVAILABLE at the client', async () => {
    const accepted = new Pull(backend.connections).next();
    const channel = await client.openChannel(chat);
    await ask(channel, 'WHO');
    await (await accepted).close();
    expectCode(await channel.done, StatusCode.unavailable);
  });

  it('backend shutdown is GOING_AWAY at the client', async () => {
    // The Dart test also has a Talk request pending fail GOING_AWAY.
    const channel = await client.openChannel(chat);
    await ask(channel, 'WHO');
    await backend.close();
    expectCode(await channel.done, StatusCode.goingAway);
  });

  it('host hint relay', async () => {
    const relay = node({ allowHostHint: true });
    relay.catchAll = proxyHandler(relay);
    const relayUri = await relay.listenMemory();
    for (const hint of [backendUri, upper(backendUri)]) {
      const channel = await client.openChannelAt(
        relayUri,
        new ChannelAddress({ type: chat, instance: 7, host: hint }),
      );
      // The relay strips the hint and keeps everything else.
      expect(await ask(channel, 'WHO')).toBe('chat 7 null null client token');
      await channel.close();
    }
    const bad = await client.openChannelAt(
      relayUri,
      new ChannelAddress({ type: chat, host: 'tcp://' }),
    );
    expectCode(await bad.done, StatusCode.invalidArgument);
    const dead = await client.openChannelAt(
      relayUri,
      new ChannelAddress({ type: chat, host: 'mem://nobody-listens-here' }),
    );
    expectCode(await dead.done, StatusCode.unavailable);
  });

  it('rejections carry no detail about the mesh', async () => {
    const down = await client.openChannel('down');
    const downStatus = await down.done;
    expectStatus(downStatus, StatusCode.unavailable, 'unavailable');
    const missing = await client.openChannel(new ServiceAddress(chat, 8));
    const missingStatus = await missing.done;
    expectStatus(missingStatus, StatusCode.notFound, 'not found');
    const denied = await client.openChannel('admin');
    expect((await denied.done).reason).toBe('permission denied');
    for (const status of [downStatus, missingStatus]) {
      expect(status.reason).not.toContain('mem');
      expect(status.reason).not.toContain(backendUri.slice('mem://'.length));
    }
  });

  it('the default allow refuses reserved types', async () => {
    const ns = Name.of('_ns');
    backend.registerService(
      ns,
      (incoming) => {
        void incoming.channel.send(bytes('naming'));
        void incoming.channel.close();
      },
      { instance: 1 },
    );
    const table = staticResolver([record(ns, 1, [backendUri]), record(chat, 7, [backendUri])]);
    const open = node();
    open.catchAll = proxyHandler(open, { resolver: table });
    const openUri = await open.listenMemory();
    const refused = await client.openChannelAt(openUri, new ChannelAddress({ type: ns }));
    expectCode(await refused.done, StatusCode.permissionDenied);
    const channel = await client.openChannelAt(openUri, new ChannelAddress({ type: chat }));
    expect(await ask(channel, 'WHO')).not.toBe('');
    await channel.close();
    // Proxying a reserved type takes an explicit allow.
    const explicit = node();
    explicit.catchAll = proxyHandler(explicit, {
      resolver: table,
      allow: (address) => ns.equals(address.type),
    });
    const explicitUri = await explicit.listenMemory();
    const allowed = await client.openChannelAt(explicitUri, new ChannelAddress({ type: ns }));
    expect(await nextText(allowed)).toBe('naming');
  });

  it('a record pointing back at the proxy is refused at once', async () => {
    const table = staticResolver([]);
    const loop = node({ resolver: table });
    let hops = 0;
    const proxy = proxyHandler(loop, { allow: (a) => chat.equals(a.type) });
    loop.catchAll = (incoming) => {
      hops++;
      return proxy(incoming);
    };
    const first = await loop.listenMemory();
    const second = await loop.listenMemory({ name: 'mesh' });
    table.add(record(chat, 9, [first]));
    // Other spellings of the same listeners.
    table.add(record(chat, 10, [upper(first), 'mem://MESH']));
    const looping = node({ resolver: new EndpointResolver(first) });
    for (const instance of [9, 10]) {
      hops = 0;
      const started = Date.now();
      const channel = await looping.openChannel(new ServiceAddress(chat, instance));
      expectStatus(await channel.done, StatusCode.unavailable, 'unavailable');
      expect(Date.now() - started).toBeLessThan(2000);
      expect(hops).toBe(1);
    }
    expect(loop.isOwnEndpoint(first)).toBe(true);
    expect(loop.isOwnEndpoint(second)).toBe(true);
    expect(loop.isOwnEndpoint('mem://other')).toBe(false);
    expect(loop.isOwnEndpoint(backendUri)).toBe(false);
  });

  it('a host hint pointing at the relay itself is refused', async () => {
    const relay = node({ allowHostHint: true });
    let hops = 0;
    const proxy = proxyHandler(relay);
    relay.catchAll = (incoming) => {
      hops++;
      return proxy(incoming);
    };
    const relayUri = await relay.listenMemory();
    expect(relay.isOwnEndpoint(upper(relayUri))).toBe(true);
    for (const hint of [relayUri, upper(relayUri)]) {
      hops = 0;
      const channel = await client.openChannelAt(
        relayUri,
        new ChannelAddress({ type: chat, host: hint }),
      );
      expectCode(await channel.done, StatusCode.unavailable);
      expect(hops).toBe(1);
    }
  });

  it('one client filling its allowance does not starve another', async () => {
    // The backend accepts 4 channels on a connection and the proxy keeps
    // one connection to it; each client may hold 3.
    const hold = Name.of('hold');
    const smallBackend = node({ muxOptions: { maxChannels: 4 } });
    const held: IncomingChannel[] = [];
    smallBackend.registerService(hold, (incoming) => void held.push(incoming), { instance: 1 });
    const smallUri = await smallBackend.listenMemory();
    const table = staticResolver([record(hold, 1, [smallUri])]);
    const proxyNode = node({ maxConnectionsPerEndpoint: 1 });
    proxyNode.catchAll = proxyHandler(proxyNode, {
      resolver: table,
      maxChannelsPerConnection: 3,
    });
    const proxyUri = await proxyNode.listenMemory();
    const toHold = new ChannelAddress({ type: hold });
    // Warm up the proxy's connection so that the backend's LIMITS are known.
    const warm = await client.openChannelAt(proxyUri, toHold);
    await until(() => held.length === 1);
    await (held[0] as IncomingChannel).channel.close();
    await warm.done;
    held.length = 0;

    const greedy = node();
    const greedyChannels: MuxChannel[] = [];
    for (let i = 0; i < 5; i++) {
      greedyChannels.push(await greedy.openChannelAt(proxyUri, toHold));
    }
    const refused = await Promise.all(greedyChannels.slice(3).map((channel) => channel.done));
    for (const status of refused) {
      expectCode(status, StatusCode.resourceExhausted);
    }
    await until(() => held.length === 3);
    // Another client still gets through.
    const polite = node();
    const channel = await polite.openChannelAt(proxyUri, toHold);
    await until(() => held.length === 4);
    void (held[3] as IncomingChannel).channel.send(bytes('served'));
    expect(await nextText(channel)).toBe('served');
    // A channel of the greedy client that ends frees its allowance, once
    // the proxy has closed both sides of it.
    const toBackend = await proxyNode.connect(smallUri);
    expect(toBackend.openChannelCount).toBe(4);
    await (greedyChannels[0] as MuxChannel).close();
    await until(() => toBackend.openChannelCount <= 3);
    await pump();
    const more = await greedy.openChannelAt(proxyUri, toHold);
    await until(() => held.length === 5);
    void (held[4] as IncomingChannel).channel.send(bytes('again'));
    expect(await nextText(more)).toBe('again');
  });

  it('a backend GOAWAY between connect and OPEN is retried once', async () => {
    const accepted: MuxConnection[] = [];
    void (async () => {
      for await (const connection of backend.connections) {
        accepted.push(connection);
      }
    })();
    const table = staticResolver([record(chat, 7, [backendUri])]);
    const racing = new RacingSwitchboard();
    onCleanup(() => racing.close());
    racing.catchAll = proxyHandler(racing, { resolver: table });
    const racingUri = await racing.listenMemory();
    // The proxy's pooled connection to the backend.
    const warm = await client.openChannelAt(racingUri, new ChannelAddress({ type: chat }));
    await ask(warm, 'WHO');
    await warm.close();
    const pooled = await racing.connect(backendUri);
    racing.beforeReturn = async (connection) => {
      expect(connection).toBe(pooled);
      void (accepted[0] as MuxConnection).goAway();
      await until(() => connection.peerGoingAway);
    };
    const channel = await client.openChannelAt(racingUri, new ChannelAddress({ type: chat }));
    expect(await ask(channel, 'WHO')).toBe('chat 7 null null client token');
    expect(accepted).toHaveLength(2);
    expect(racing.beforeReturn).toBeUndefined();
    await channel.close();
  });

  it('without allowHostHint the hint is ignored and stripped', async () => {
    const channel = await client.openChannelAt(
      endpointUri,
      new ChannelAddress({ type: chat, host: 'tcp://10.0.0.1:1' }),
    );
    expect(await ask(channel, 'WHO')).toBe('chat 7 null null client token');
    await channel.close();
  });
});

/**
 * A node whose next {@link connect} hands out the connection only after
 * {@link beforeReturn} ran on it, to make a backend's GOAWAY arrive
 * between connect() and the OPEN.
 */
class RacingSwitchboard extends Switchboard {
  /** Runs once, on the next connection {@link connect} returns. */
  beforeReturn: ((connection: MuxConnection) => Promise<void>) | undefined;

  constructor() {
    super({ runtime: currentRuntime(), muxOptions: fast });
  }

  override async connect(
    endpoint: string,
    options: {
      policy?: ChannelPolicy | undefined;
      record?: ServiceRecord | undefined;
      signal?: AbortSignal | undefined;
    } = {},
  ): Promise<MuxConnection> {
    const connection = await super.connect(endpoint, options);
    const hook = this.beforeReturn;
    this.beforeReturn = undefined;
    await hook?.(connection);
    return connection;
  }
}

describe('proxyHandler authorize', () => {
  let atBackend: IncomingChannel[];
  let backends: StaticResolver;
  let client: Switchboard;

  beforeEach(async () => {
    atBackend = [];
    const backend = node();
    backend.registerService(
      chat,
      (incoming) => {
        atBackend.push(incoming);
        void (async () => {
          const frames = new Frames(incoming.channel);
          while ((await frames.next()) !== undefined) {
            const a = incoming.address;
            void incoming.channel.send(
              bytes(`${a.instance} ${a.shard ?? 'null'} ${utf8Decode(a.payload)}`),
            );
          }
        })();
      },
      { instance: 7 },
    );
    const backendUri = await backend.listenMemory();
    backends = new StaticResolver([
      serviceRecord(new ServiceAddress(chat, 7), { endpoints: [backendUri] }),
    ]);
    onCleanup(() => backends.close());
  });

  /** A proxy with [authorize], and {@link client} connected to it. */
  async function start(
    authorize: (
      incoming: IncomingChannel,
    ) => ChannelAddress | null | undefined | Promise<ChannelAddress | null | undefined>,
    options: { authorizeTimeout?: number; budget?: number } = {},
  ): Promise<void> {
    const proxy = node();
    proxy.catchAll = proxyHandler(proxy, {
      authorize,
      authorizeTimeout: options.authorizeTimeout ?? 10_000,
      resolver: backends,
      maxChannelsPerConnection: options.budget ?? 256,
    });
    const uri = await proxy.listenMemory();
    client = node({ resolver: new EndpointResolver(uri), defaultPayload: bytes('token-5') });
  }

  it("a rewritten shard and payload reach the backend; the client's data waits for the verdict", async () => {
    const seen: ChannelAddress[] = [];
    await start(async (incoming) => {
      seen.push(incoming.address);
      await new Promise((resolve) => setTimeout(resolve, 20));
      const token = utf8Decode(incoming.address.payload);
      const account = Number(token.split('-').at(-1));
      return incoming.address.with({ shard: account, payload: bytes(`verified ${token}`) });
    });
    const channel = await client.openChannel(chat);
    // Sent at once, while the proxy is still authorizing.
    expect(await ask(channel, 'WHO')).toBe('7 5 verified token-5');
    expect(seen[0]?.payload).toEqual(bytes('token-5'));
    expect(seen[0]?.shard).toBeUndefined();
    expect(atBackend[0]?.address.shard).toBe(5);
    await channel.close();
  });

  it('null is UNAUTHENTICATED, a throw PERMISSION_DENIED', async () => {
    await start(async (incoming) => {
      switch (utf8Decode(incoming.address.payload)) {
        case 'anonymous':
          return null;
        case 'bad':
          throw new SyntaxError('bad credential');
        default:
          return incoming.address;
      }
    });
    const statusWith = async (payload: string): Promise<Status> =>
      (await client.openChannel(chat, { payload: bytes(payload) })).done;
    expectStatus(await statusWith('anonymous'), StatusCode.unauthenticated, 'unauthenticated');
    expectCode(await statusWith('bad'), StatusCode.permissionDenied);
    expect(atBackend).toHaveLength(0);
    // A synchronous answer works too.
    const channel = await client.openChannel(chat);
    expect(await ask(channel, 'WHO')).toBe('7 null token-5');
    await channel.close();
  });

  it('no answer within authorizeTimeout is UNAUTHENTICATED; pending authorizations count toward the budget', async () => {
    let answer: (address: ChannelAddress) => void = () => {};
    const pending = new Promise<ChannelAddress>((resolve) => {
      answer = resolve;
    });
    await start(() => pending, { authorizeTimeout: 30, budget: 1 });
    const first = await client.openChannel(chat);
    await pump();
    const second = await client.openChannel(chat);
    expectCode(await second.done, StatusCode.resourceExhausted);
    expectCode(await first.done, StatusCode.unauthenticated);
    // The late answer is ignored.
    answer(new ChannelAddress({ type: chat }));
    await pump();
    expect(atBackend).toHaveLength(0);
  });

  it('a channel its client closed while being authorized is dropped', async () => {
    let verdict: () => void = () => {};
    const decided = new Promise<void>((resolve) => {
      verdict = resolve;
    });
    let asked: () => void = () => {};
    const wasAsked = new Promise<void>((resolve) => {
      asked = resolve;
    });
    await start(async (incoming) => {
      asked();
      await decided;
      return incoming.address;
    });
    const channel = await client.openChannel(chat);
    await wasAsked;
    await channel.close(Status.of(StatusCode.cancelled));
    verdict();
    await pump();
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(atBackend).toHaveLength(0);
  });
});
