/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Port of the Dart `test/integration/naming_mesh_test.dart`: a mesh run by
// a naming service, over TCP, over WebSocket and over in-process `mem://`
// endpoints (every node in this process): a naming node hosting `_ns/1`,
// backend nodes `b1` (`npc`, `chat`) and `b2` (`npc`) joined with
// MeshNode, consumer nodes resolving through the mirrored table, a frontend
// endpoint proxying `chat` for clients that only know the endpoint, and a
// raw mux peer without a Switchboard; then the liveness of the naming
// channel through the mux keep-alive, over a TCP relay that can freeze.
//
// Not ported yet: the two `publishSharded` cases need the slot gate, which
// is not ported; they are listed as todo.

import { createServer, type Server, type Socket, connect as tcpConnect } from 'node:net';
import { afterAll, beforeEach, describe, expect, test } from 'vitest';
import { ChannelAddress, ServiceAddress } from '../../src/core/address/index.ts';
import { hexString, utf8Decode, utf8Encode } from '../../src/core/bytes.ts';
import { FrameMuxConnection } from '../../src/core/mux/frame-mux-connection.ts';
import { Name } from '../../src/core/name.ts';
import { Services, serviceRecordsEqual } from '../../src/core/naming/naming-protocol.ts';
import { NamingService } from '../../src/core/naming/naming-service.ts';
import { Status, StatusCode } from '../../src/core/status.ts';
import type {
  IncomingChannel,
  ServiceEvent,
  ServiceRecord,
} from '../../src/core/switchboard/contract.ts';
import { MeshNode } from '../../src/core/switchboard/mesh-node.ts';
import { proxyHandler } from '../../src/core/switchboard/proxy.ts';
import { EndpointResolver } from '../../src/core/switchboard/resolver.ts';
import type { TalkOptions } from '../../src/core/talk/contract.ts';
import { TalkChannel } from '../../src/core/talk/index.ts';
import { connectTcp } from '../../src/node/transport/tcp.ts';
import { connectWebSocket } from '../../src/node/transport/ws-client.ts';
import { rejectsWith } from '../support/node/harness.ts';
import {
  codeOf,
  delay,
  expectNoMemoryListeners,
  listen,
  rejectsWithOneOf,
  type Scheme,
  until,
} from './mesh-support.ts';
import { node, onCleanup, type Switchboard, within } from './node-support.ts';

/** Service and naming channels. */
const talkOptions: Partial<TalkOptions> = { requestTimeout: 2000, replyTimeout: 1000 };
const reconnectDelay = 20;

/** Upper bound for anything a test waits on; only reached on failure. */
const limit = 5000;

const npc = Name.of('npc');
const chat = Name.of('chat');

/** Joins [n] to the mesh with short test timeouts; left after the test. */
function join(n: Switchboard, naming: string): MeshNode {
  const mesh = MeshNode.join(n, naming, { reconnectDelay, resolveTimeout: limit, talkOptions });
  onCleanup(() => mesh.leave());
  return mesh;
}

/** A new node, closed after the test. */
function newNode(
  options: { resolver?: EndpointResolver; defaultPayload?: string } = {},
): Switchboard {
  return node({
    resolver: options.resolver,
    defaultPayload: options.defaultPayload,
    muxOptions: { goAwayGrace: 100 },
    talkOptions,
  });
}

function sameTable(
  a: ReadonlyMap<string, ServiceRecord>,
  b: ReadonlyMap<string, ServiceRecord>,
): boolean {
  if (a.size !== b.size) {
    return false;
  }
  for (const [key, record] of a) {
    const other = b.get(key);
    if (other === undefined || !serviceRecordsEqual(record, other)) {
      return false;
    }
  }
  return true;
}

/** The first DOWN of [address] the resolver of [mesh] reports from now on. */
function downOf(mesh: MeshNode, address: ServiceAddress): Promise<ServiceEvent> {
  return (async () => {
    for await (const event of mesh.resolver.events) {
      if (!event.up && event.record.address.equals(address)) {
        return event;
      }
    }
    throw new Error('the events ended');
  })();
}

/** The node hosting the naming service as `_ns/1`. */
class NamingNode {
  readonly node: Switchboard;
  readonly service: NamingService;
  readonly uri: string;

  private constructor(n: Switchboard, service: NamingService, uri: string) {
    this.node = n;
    this.service = service;
    this.uri = uri;
  }

  /**
   * Starts where [options.previous] was (anywhere without), retrying the
   * bind for a while (a port just released by a previous naming node).
   * [options.taken] ids are registered locally as `squat/<id>` before
   * anyone can connect. No assignment hold unless asked for.
   */
  static async start(
    scheme: Scheme,
    options: { previous?: string; taken?: readonly number[]; assignmentHold?: number } = {},
  ): Promise<NamingNode> {
    const n = newNode();
    const service = new NamingService({ assignmentHold: options.assignmentHold ?? 0 });
    onCleanup(() => service.close());
    n.registerService(Services.naming, service.handler, { instance: 1 });
    for (const id of options.taken ?? []) {
      service.registerLocal('squat', [], { instance: id });
    }
    const uri = await listen(n, scheme, { previous: options.previous });
    service.registerLocal(Services.naming, n.listeningEndpoints, { instance: 1 });
    return new NamingNode(n, service, uri);
  }

  async stop(): Promise<void> {
    await this.service.close();
    await this.node.close();
  }
}

/**
 * A backend node publishing services. Every service answers:
 *
 * - `WHO`: JSON with the node name, the service type, the instance, shard
 *   and application payload of the channel's address header;
 * - `COUNT` (stream): items `0`, `1`, `2`, then the final response `end`;
 * - `TICK` (stream): an item every 5 ms until the request goes away.
 */
class Backend {
  readonly name: string;
  readonly node: Switchboard;
  readonly uri: string;
  readonly mesh: MeshNode;
  /** Instance id of each published type, by `Name.key`. */
  readonly ids = new Map<string, number>();

  private constructor(name: string, n: Switchboard, uri: string, mesh: MeshNode) {
    this.name = name;
    this.node = n;
    this.uri = uri;
    this.mesh = mesh;
  }

  static async start(
    name: string,
    scheme: Scheme,
    naming: string,
    types: readonly Name[],
  ): Promise<Backend> {
    const n = newNode();
    const uri = await listen(n, scheme);
    const mesh = join(n, naming);
    const backend = new Backend(name, n, uri, mesh);
    for (const type of types) {
      backend.ids.set(type.key, await within(mesh.publish(type, backend.serve), limit));
    }
    return backend;
  }

  /** The instance id of [type]. */
  id(type: Name): number {
    const id = this.ids.get(type.key);
    if (id === undefined) {
      throw new Error(`${this.name} has no ${type}`);
    }
    return id;
  }

  readonly serve = (incoming: IncomingChannel): void => {
    const address = incoming.address;
    void incoming.talk().serve((m) => {
      switch (m.procedureName) {
        case 'WHO':
          void m.reply(
            JSON.stringify({
              node: this.name,
              type: `${address.type}`,
              instance: address.instance,
              shard: address.shard ?? null,
              payload: utf8Decode(address.payload),
            }),
          );
          break;
        case 'COUNT':
          for (let i = 0; i < 3; i++) {
            void m.replyItem(`${i}`);
          }
          void m.reply('end');
          break;
        case 'TICK': {
          let n = 0;
          const timer = setInterval(() => {
            if (!m.canReply) {
              clearInterval(timer);
              return;
            }
            void m.replyItem(`${n++}`).catch(() => {});
          }, 5);
          break;
        }
        default:
          void m.replyAbort(Status.of(StatusCode.unimplemented));
      }
    });
  };

  /** Leaves the mesh, then closes the node. */
  async stop(): Promise<void> {
    await this.mesh.leave();
    await this.node.close();
  }
}

/** One side of a {@link TcpRelay} connection. */
class Pipe {
  frozen = false;
  #closed = false;
  readonly #a: Socket;
  readonly #b: Socket;

  constructor(a: Socket, b: Socket) {
    this.#a = a;
    this.#b = b;
    for (const [from, to] of [
      [a, b],
      [b, a],
    ] as const) {
      from.on('data', (data: Buffer) => {
        if (!this.frozen && !this.#closed) {
          to.write(data);
        }
      });
      from.on('end', () => this.close());
      from.on('close', () => this.close());
      from.on('error', () => this.close());
    }
  }

  close(): void {
    if (this.#closed) {
      return;
    }
    this.#closed = true;
    this.#a.destroy();
    this.#b.destroy();
  }
}

/**
 * A TCP relay to a `tcp://` endpoint. {@link freeze} makes the connections
 * open at that moment go dead, as a hung host or a network partition does:
 * nothing is forwarded either way and nothing is closed, until one side
 * closes its own socket, which the relay passes on. Connections made
 * afterwards are relayed normally.
 */
class TcpRelay {
  readonly #server: Server;
  readonly #host: string;
  readonly #port: number;
  readonly #pipes: Pipe[] = [];

  private constructor(server: Server, target: URL) {
    this.#server = server;
    this.#host = target.hostname;
    this.#port = Number(target.port);
    server.on('connection', (client) => this.#accept(client));
  }

  static async start(target: string): Promise<TcpRelay> {
    const relay = new TcpRelay(createServer(), new URL(target));
    await new Promise<void>((resolve) => relay.#server.listen(0, '127.0.0.1', resolve));
    return relay;
  }

  get uri(): string {
    const address = this.#server.address();
    return `tcp://127.0.0.1:${typeof address === 'object' && address !== null ? address.port : 0}`;
  }

  /** Connections relayed so far. */
  get connections(): number {
    return this.#pipes.length;
  }

  #accept(client: Socket): void {
    // The client's bytes wait in its socket until the pipe reads them.
    const upstream = tcpConnect({ host: this.#host, port: this.#port });
    upstream.once('connect', () => this.#pipes.push(new Pipe(client, upstream)));
    upstream.once('error', () => client.destroy());
  }

  freeze(): void {
    for (const pipe of this.#pipes) {
      pipe.frozen = true;
    }
  }

  async close(): Promise<void> {
    await new Promise((resolve) => this.#server.close(resolve));
    for (const pipe of this.#pipes) {
      pipe.close();
    }
  }
}

/** One `WHO` request on a fresh channel from [from] to [to]. */
async function who(
  from: Switchboard,
  to: ServiceAddress,
  shard?: number,
): Promise<Record<string, unknown>> {
  const channel = await within(from.openTalk(to, shard === undefined ? {} : { shard }), limit);
  try {
    const reply = await channel.request('WHO');
    return JSON.parse(reply.text()) as Record<string, unknown>;
  } finally {
    await channel.close();
  }
}

async function nodeOf(from: Switchboard, to: ServiceAddress, shard?: number): Promise<string> {
  return (await who(from, to, shard))['node'] as string;
}

// Every node is closed by its test's clean-up, which releases its ids.
afterAll(() => {
  expectNoMemoryListeners();
});

test.todo(
  'publishSharded handles both SLOTS and HOLDING failures: needs publishSharded (the slot gate is not ported yet)',
);

test.todo(
  'publishSharded handles an immediate HOLDING failure: needs publishSharded (the slot gate is not ported yet)',
);

for (const scheme of ['tcp', 'ws', 'mem'] as const) {
  describe(`over ${scheme}`, () => {
    let naming: NamingNode;
    let b1: Backend;
    let b2: Backend;

    /** Naming node, `b1` with `npc` and `chat`, `b2` with `npc`. */
    beforeEach(async () => {
      naming = await NamingNode.start(scheme);
      b1 = await Backend.start('b1', scheme, naming.uri, [npc, chat]);
      b2 = await Backend.start('b2', scheme, naming.uri, [npc]);
    });

    /** A node that only consumes services, synced with the whole table. */
    async function client(): Promise<[Switchboard, MeshNode]> {
      const n = newNode();
      const mesh = join(n, naming.uri);
      await within(mesh.synced, limit);
      // `_ns/1`, `npc` twice, `chat`.
      await until(() => mesh.client.table.size === 4, 'full table', limit);
      return [n, mesh];
    }

    test('naming service is registered as _ns/1 and ids are global', () => {
      expect(
        naming.service.table.get(new ServiceAddress(Services.naming, 1).key)?.endpoints,
      ).toEqual([naming.uri]);
      const ids = new Set([...b1.ids.values(), ...b2.ids.values()]);
      expect(ids.size).toBe(3);
      expect(ids.has(1)).toBe(false);
      expect(b1.mesh.published.map(String).sort()).toEqual(
        [
          `${new ServiceAddress(npc, b1.id(npc))}`,
          `${new ServiceAddress(chat, b1.id(chat))}`,
        ].sort(),
      );
      expect(naming.service.table.get(new ServiceAddress(npc, b2.id(npc)).key)?.endpoints).toEqual([
        b2.uri,
      ]);
    });

    test('resolution: round robin, exact instance, shards, leave, notFound', async () => {
      const [n, mesh] = await client();

      const reply = await who(n, new ServiceAddress(chat));
      expect(reply['node']).toBe('b1');
      expect(reply['type']).toBe('chat');
      expect(reply['instance']).toBe(b1.id(chat));

      // Round robin over the two npc instances.
      const rotation: string[] = [];
      for (let i = 0; i < 6; i++) {
        rotation.push(await nodeOf(n, new ServiceAddress(npc)));
      }
      expect(rotation.filter((x) => x === 'b1')).toHaveLength(3);
      expect(rotation.filter((x) => x === 'b2')).toHaveLength(3);
      for (let i = 1; i < rotation.length; i++) {
        expect(rotation[i], `${rotation}`).not.toBe(rotation[i - 1]);
      }

      // An exact instance always reaches that instance, which sees its own
      // id in the header.
      for (let i = 0; i < 3; i++) {
        const exact = await who(n, new ServiceAddress(npc, b2.id(npc)));
        expect(exact['node']).toBe('b2');
        expect(exact['instance']).toBe(b2.id(npc));
      }

      // Shard s selects instance s mod 2 of the instances sorted by id.
      const sorted = [b1, b2].sort((x, y) => x.id(npc) - y.id(npc));
      for (let round = 0; round < 2; round++) {
        for (let shard = 0; shard < 5; shard++) {
          const at = await who(n, new ServiceAddress(npc), shard);
          const expected = sorted[shard % 2] as Backend;
          expect(at['node']).toBe(expected.name);
          expect(at['shard']).toBe(shard);
          expect(at['instance']).toBe(expected.id(npc));
        }
      }

      // Unknown types and instances fail locally.
      await rejectsWith(n.openTalk(new ServiceAddress(Name.of('nope'))), StatusCode.notFound);
      await rejectsWith(n.openTalk(new ServiceAddress(npc, 0xdead)), StatusCode.notFound);

      // b2 leaves: the client's resolver reports DOWN, and npc is only
      // served by b1 from then on.
      const gone = new ServiceAddress(npc, b2.id(npc));
      const down = downOf(mesh, gone);
      await b2.stop();
      await within(down, limit);
      expect(mesh.client.table.has(gone.key)).toBe(false);
      for (let i = 0; i < 4; i++) {
        expect(await nodeOf(n, new ServiceAddress(npc))).toBe('b1');
      }
      await rejectsWith(n.openTalk(gone), StatusCode.notFound);
    });

    test('unpublish removes a service from the mesh and the node', async () => {
      const [n, mesh] = await client();
      const address = new ServiceAddress(chat, b1.id(chat));
      const down = downOf(mesh, address);
      await b1.mesh.unpublish(chat, address.instance);
      await within(down, limit);
      expect(b1.mesh.published.map(String)).toEqual([`${new ServiceAddress(npc, b1.id(npc))}`]);
      await rejectsWith(n.openTalk(new ServiceAddress(chat)), StatusCode.notFound);
      // A stale peer reaching the node directly is turned away too.
      const stale = await n.openTalkAt(
        b1.uri,
        new ChannelAddress({ type: chat, instance: address.instance }),
      );
      await rejectsWith(stale.request('WHO'), StatusCode.notFound);
      await stale.close();
      await rejectsWith(b1.mesh.unpublish(chat, address.instance), StatusCode.notFound);
    });

    test('naming service restart: stale table, same ids, resync', async () => {
      const [n, mesh] = await client();
      const before = new Map(mesh.client.table);
      expect(await nodeOf(n, new ServiceAddress(chat))).toBe('b1');
      const previous = naming.uri;

      await naming.stop();
      await until(
        () => !mesh.client.isSynced && !b1.mesh.client.isSynced && !b2.mesh.client.isSynced,
        'naming service loss noticed',
        limit,
      );
      expect(mesh.client.isConnected).toBe(false);

      // The mesh keeps working from the stale table.
      expect(sameTable(mesh.client.table, before)).toBe(true);
      expect(await nodeOf(n, new ServiceAddress(chat))).toBe('b1');
      const rotation = new Set<string>();
      for (let i = 0; i < 4; i++) {
        rotation.add(await nodeOf(n, new ServiceAddress(npc)));
      }
      expect([...rotation].sort()).toEqual(['b1', 'b2']);
      expect(await nodeOf(n, new ServiceAddress(npc, b2.id(npc)))).toBe('b2');

      // A new naming service at the same endpoint. The backends register
      // their old ids again; every node resyncs.
      naming = await NamingNode.start(scheme, { previous });
      expect(naming.uri).toBe(before.get(new ServiceAddress(Services.naming, 1).key)?.endpoints[0]);
      await within(mesh.synced, limit);
      await within(b1.mesh.synced, limit);
      await within(b2.mesh.synced, limit);
      await until(() => sameTable(naming.service.table, before), 'backends re-registered', limit);
      await until(() => sameTable(mesh.client.table, before), 'client table rebuilt', limit);
      expect(mesh.client.isSynced).toBe(true);
      expect(b1.mesh.published.map(String).sort()).toEqual(
        [
          `${new ServiceAddress(npc, b1.id(npc))}`,
          `${new ServiceAddress(chat, b1.id(chat))}`,
        ].sort(),
      );

      // Business as usual, including a new registration.
      expect(await nodeOf(n, new ServiceAddress(chat))).toBe('b1');
      const extra = Name.of('extra');
      const id = await within(b2.mesh.publish(extra, b2.serve), limit);
      expect([...before.values()].map((r) => r.address.instance)).not.toContain(id);
      await until(
        () => mesh.client.table.has(new ServiceAddress(extra, id).key),
        'new registration mirrored',
        limit,
      );
      const reply = await who(n, new ServiceAddress(extra));
      expect(reply['node']).toBe('b2');
      expect(reply['instance']).toBe(id);
    });

    test('an id taken during an outage moves to a new id', async () => {
      const [n, mesh] = await client();
      const old = b2.id(npc);
      const previous = naming.uri;
      await naming.stop();
      await until(() => !b2.mesh.client.isSynced, 'loss noticed', limit);
      naming = await NamingNode.start(scheme, { previous, taken: [old] });
      // b2 asks for its old id, gets ALREADY_EXISTS, takes a new one, and
      // its local dispatch follows.
      await until(
        () => b2.mesh.published.some((a) => a.type.equals(npc)),
        'npc published again',
        limit,
      );
      await until(
        () => b2.mesh.published.length === 1 && b2.mesh.published[0]?.instance !== old,
        'npc moved',
        limit,
      );
      const moved = b2.mesh.published[0] as ServiceAddress;
      expect(naming.service.table.get(moved.key)?.endpoints).toEqual([b2.uri]);
      await until(() => mesh.client.table.has(moved.key), 'client sees the new id', limit);
      const reply = await who(n, moved);
      expect(reply['node']).toBe('b2');
      expect(reply['instance']).toBe(moved.instance);
      // The old id is no longer dispatched on b2.
      const stale = await n.openTalkAt(b2.uri, new ChannelAddress({ type: npc, instance: old }));
      await rejectsWith(stale.request('WHO'), StatusCode.notFound);
      await stale.close();
    });

    test('after a restart, newcomers do not get surviving ids', async () => {
      const survivors = new Set([...b1.ids.values(), ...b2.ids.values()]);
      const previous = naming.uri;
      await naming.stop();
      await until(
        () => !b1.mesh.client.isSynced && !b2.mesh.client.isSynced,
        'loss noticed',
        limit,
      );
      // Ids are assigned from 1 again, but only after the hold: the
      // survivors reconnect within it and reclaim theirs.
      naming = await NamingNode.start(scheme, { previous, assignmentHold: 500 });
      const b3 = await Backend.start('b3', scheme, naming.uri, [npc, chat]);
      expect([...b3.ids.values()].filter((id) => survivors.has(id))).toEqual([]);
      expect(naming.service.isHoldingAssignments).toBe(false);
      await until(() => naming.service.table.size === 6, 'everyone registered', limit);
      for (const backend of [b1, b2]) {
        expect(new Set(backend.mesh.published.map((a) => a.instance))).toEqual(
          new Set(backend.ids.values()),
        );
      }
      for (const b of [b1, b2, b3]) {
        for (const [key, id] of b.ids) {
          expect(
            naming.service.table.get(new ServiceAddress(Name.fromKey(key), id).key)?.endpoints,
          ).toEqual([b.uri]);
        }
      }
    });

    test('an unreachable naming service fails opens, bounded', async () => {
      // An endpoint nobody listens on any more.
      const probe = newNode();
      const dead = await listen(probe, scheme);
      await probe.close();
      const n = newNode();
      const mesh = MeshNode.join(n, dead, { reconnectDelay, resolveTimeout: 100, talkOptions });
      onCleanup(() => mesh.leave());
      const start = performance.now();
      await rejectsWith(
        within(n.openTalk(new ServiceAddress(chat)), limit),
        StatusCode.unavailable,
      );
      expect(performance.now() - start).toBeLessThan(1000);
      // A publish waits for the naming service; leaving cancels it.
      const pending = rejectsWith(
        mesh.publish(npc, () => {}),
        StatusCode.cancelled,
      );
      await mesh.leave();
      await pending;
      expect(n.resolver).toBeUndefined();
      await rejectsWith(
        mesh.publish(npc, () => {}),
        StatusCode.failedPrecondition,
      );
    });

    describe('frontend endpoint', () => {
      let endpoint: Switchboard;
      let endpointMesh: MeshNode;
      let endpointUri: string;
      let frontend: Switchboard;

      beforeEach(async () => {
        endpoint = newNode();
        endpointUri = await listen(endpoint, scheme);
        endpointMesh = join(endpoint, naming.uri);
        endpoint.catchAll = proxyHandler(endpoint, {
          allow: (address) => address.type?.equals(chat) ?? false,
        });
        await within(endpointMesh.synced, limit);
        const resolver = new EndpointResolver(endpointUri);
        onCleanup(() => resolver.close());
        frontend = newNode({ resolver, defaultPayload: 'session-42' });
      });

      test('talk and stream requests through the proxy', async () => {
        const talk = await within(frontend.openTalk(new ServiceAddress(chat)), limit);
        const reply = JSON.parse((await talk.request('WHO')).text()) as Record<string, unknown>;
        // The backend sees the frontend client's credential and its own
        // instance, filled in by the proxy.
        expect(reply['node']).toBe('b1');
        expect(reply['payload']).toBe('session-42');
        expect(reply['instance']).toBe(b1.id(chat));

        const stream = talk.streamRequest('COUNT');
        const items: string[] = [];
        for await (const item of stream) {
          items.push(item.text());
        }
        expect(items).toEqual(['0', '1', '2']);
        expect((await stream.done).text()).toBe('end');
        await talk.close();

        // An explicit instance passes through the endpoint resolver and the
        // proxy unchanged.
        const exact = await who(frontend, new ServiceAddress(chat, b1.id(chat)));
        expect(exact['instance']).toBe(b1.id(chat));
        expect(exact['payload']).toBe('session-42');
      });

      test('types outside the allowlist are refused', async () => {
        const talk = await frontend.openTalk(new ServiceAddress(npc));
        await rejectsWith(talk.request('WHO'), StatusCode.permissionDenied);
        expect(codeOf(await within(talk.done, limit))).toBe('permissionDenied');
      });

      test('a backend shutting down ends a proxied stream promptly', async () => {
        const talk = await frontend.openTalk(new ServiceAddress(chat));
        const stream = talk.streamRequest('TICK');
        const items: string[] = [];
        const ended = (async (): Promise<unknown> => {
          try {
            for await (const item of stream) {
              items.push(item.text());
            }
            return 'done without error';
          } catch (e) {
            return e;
          }
        })();
        await until(() => items.length >= 3, 'three items', limit);

        const start = performance.now();
        await within(b1.stop(), limit);
        const error = await within(ended, limit);
        expect(performance.now() - start).toBeLessThan(2000);
        await rejectsWithOneOf(Promise.reject(error), [
          StatusCode.goingAway,
          StatusCode.unavailable,
        ]);
        expect(items.slice(0, 3)).toEqual(['0', '1', '2']);
        expect(['goingAway', 'unavailable']).toContain(codeOf(await within(talk.done, limit)));

        // chat is gone from the mesh: once the endpoint has seen the DOWN,
        // its proxy refuses with NOT_FOUND.
        await until(
          () => ![...endpointMesh.client.table.values()].some((r) => r.address.type.equals(chat)),
          'chat gone at the endpoint',
          limit,
        );
        const again = await frontend.openTalk(new ServiceAddress(chat));
        await rejectsWith(again.request('WHO'), StatusCode.notFound);
        await again.close();
      });

      // A raw peer brings its own transport; a mem endpoint is reached only
      // through a Switchboard of this process.
      if (scheme !== 'mem') {
        test('a raw mux peer without a Switchboard', async () => {
          // What an embedded device does: its own transport and mux, and a
          // hand-built OPEN payload: HAS_SERVICE, `chat` padded to 8 bytes,
          // then the application payload.
          const open = Uint8Array.from([
            0x01,
            ...utf8Encode('chat'),
            0,
            0,
            0,
            0,
            ...utf8Encode('device-7'),
          ]);
          expect(hexString(open)).toBe(
            hexString(new ChannelAddress({ type: chat, payload: 'device-7' }).encode()),
          );
          const url = new URL(endpointUri);
          const transport =
            scheme === 'tcp'
              ? await connectTcp(url.hostname, Number(url.port))
              : await connectWebSocket(endpointUri);
          const connection = new FrameMuxConnection(transport, {
            isInitiator: true,
            keepAliveInterval: undefined,
            shortIdsOnly: true,
          });
          onCleanup(() => connection.close());
          const talk = new TalkChannel(connection.open(open), talkOptions);
          const reply = JSON.parse((await within(talk.request('WHO'), limit)).text()) as Record<
            string,
            unknown
          >;
          expect(reply['node']).toBe('b1');
          expect(reply['payload']).toBe('device-7');
          expect(reply['instance']).toBe(b1.id(chat));
          await talk.close();
          await connection.close();
        });
      }
    });
  });
}

describe('liveness', () => {
  test('a naming service that stops answering is detected by the mux keep-alive; the client reconnects and resyncs', async () => {
    const naming = await NamingNode.start('tcp');
    const relay = await TcpRelay.start(naming.uri);
    onCleanup(() => relay.close());
    // A node that probes a silent connection after 100 ms and gives it up
    // 100 ms later.
    const n = node({
      muxOptions: { goAwayGrace: 100, keepAliveInterval: 100, keepAliveTimeout: 100 },
      talkOptions,
    });
    const mesh = join(n, relay.uri);
    const endpoint = 'tcp://10.0.0.1:7000';
    await within(mesh.client.register(npc, [endpoint]), limit);
    await within(mesh.synced, limit);
    const firstSynced = mesh.synced;

    // Idle for many keep-alive rounds: the PINGs are answered, the
    // subscription needs nothing else.
    await delay(600);
    expect(mesh.client.isSynced).toBe(true);
    expect(relay.connections).toBe(1);

    // The naming service's host hangs.
    relay.freeze();
    await until(() => !mesh.client.isSynced, 'loss noticed', limit);
    expect(mesh.synced).not.toBe(firstSynced);
    // The stale table is kept meanwhile.
    expect(
      [...mesh.client.table.values()].filter((r) => r.endpoints.includes(endpoint)),
    ).toHaveLength(1);

    // New connections get through: reconnected, registered again, resynced,
    // with nothing left of the dead channel.
    await within(mesh.synced, limit);
    expect(relay.connections).toBeGreaterThanOrEqual(2);
    await until(
      () =>
        [...naming.service.table.values()].filter((r) => r.endpoints.includes(endpoint)).length ===
          1 && sameTable(mesh.client.table, naming.service.table),
      'registered again and mirrored',
      limit,
    );
    expect(naming.service.channelCount).toBe(1);
    expect(naming.service.watchCount).toBe(1);
  });
});
