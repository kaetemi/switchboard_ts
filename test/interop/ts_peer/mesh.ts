/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// The mesh parts of a peer (README, "Mesh"), as dart_peer/lib/src/mesh.dart
// has them: a naming service, naming clients with their slot calls and
// resolution, and the proxy and relay in front of another peer.

import {
  AssignResult,
  ChannelAddress,
  type MuxChannel,
  migrationPhaseName,
  Name,
  NamingClient,
  NamingResolver,
  NamingService,
  proxyHandler,
  RelayService,
  type Resolver,
  ServiceAddress,
  Services,
  type SlotHandler,
  SlotMode,
  StaticResolver,
  type Switchboard,
  serviceRecord,
  slotStateName,
} from '../../../src/node/index.ts';
import type { PeerContext } from './context.ts';
import {
  ArgumentProblem,
  arrayArg,
  boolArg,
  bytesOf,
  hex,
  intArg,
  type Json,
  objectArg,
  requiredString,
  stringArg,
} from './protocol.ts';

/** The `naming`, `proxy` and `relay` configuration. */
export interface MeshConfig {
  /** `{assignmentHoldMs}`: serve a naming service as `_ns` instance 1. */
  readonly naming: Json | undefined;
  /** `{endpoint, types, revealOwners}`: proxy the types to the endpoint. */
  readonly proxy: Json | undefined;
  /** `{endpoint, types}`: relay `_relay` channels for the types to the endpoint. */
  readonly relay: Json | undefined;
}

/** Reads the configuration's `naming`, `proxy` and `relay` objects. */
export function meshConfig(json: Json): MeshConfig {
  return {
    naming: objectArg(json, 'naming'),
    proxy: objectArg(json, 'proxy'),
    relay: objectArg(json, 'relay'),
  };
}

/** The types of [spec], default `echo`. */
function typesOf(spec: Json): string[] {
  return (arrayArg(spec, 'types') ?? ['echo']).map(String);
}

/** The records of [spec]'s types at instance 1 of its endpoint. */
function backend(spec: Json): StaticResolver {
  const endpoint = requiredString(spec, 'endpoint');
  return new StaticResolver(
    typesOf(spec).map((t) => serviceRecord(new ServiceAddress(t, 1), { endpoints: [endpoint] })),
  );
}

/**
 * What a peer's node serves beside its services: the naming service, the
 * proxy and the relay. Call {@link listening} once the node listens.
 */
export class MeshServices {
  readonly #naming: NamingService | undefined;

  /** Registers [config]'s services on [node]. */
  constructor(node: Switchboard, config: MeshConfig) {
    const { naming, proxy, relay } = config;
    if (naming !== undefined) {
      this.#naming = new NamingService({ assignmentHold: intArg(naming, 'assignmentHoldMs') ?? 0 });
      node.registerService(Services.naming, this.#naming.handler, { instance: 1 });
    }
    if (proxy !== undefined) {
      const handler = proxyHandler(node, {
        resolver: backend(proxy),
        revealOwners: boolArg(proxy, 'revealOwners'),
      });
      for (const t of typesOf(proxy)) {
        node.registerService(t, handler);
      }
    }
    if (relay !== undefined) {
      const service = new RelayService(node, {
        requireIdentity: false,
        allowEndpoints: true,
        resolver: backend(relay),
      });
      node.registerService(Services.relay, service.handler);
    }
  }

  /** Registers the naming service in its own table at [endpoints]. */
  listening(endpoints: readonly string[]): void {
    this.#naming?.registerLocal(Services.naming, endpoints, { instance: 1 });
  }

  /** Closes the naming service. */
  async close(): Promise<void> {
    await this.#naming?.close();
  }
}

/**
 * The slot handler of the peer's naming clients: every slot request is
 * accepted at once (`ASSIGN` answered holding) and reported as a
 * `naming.slot` event.
 */
function reportingSlots(ctx: PeerContext, handle: () => string): SlotHandler {
  const report = (request: string, type: Name, slot: number, epoch?: number, to?: number) =>
    ctx.out.event('naming.slot', {
      naming: handle(),
      request,
      type: type.toString(),
      slot,
      epoch: epoch ?? null,
      to: to ?? null,
    });
  return {
    onAssign: (r) => {
      report('assign', r.type, r.slot, r.epoch);
      return AssignResult.holding;
    },
    onDrain: (r) => report('drain', r.type, r.slot, r.epoch, r.to),
    onForward: (r) => report('forward', r.type, r.slot, r.epoch, r.to),
    onResume: (r) => report('resume', r.type, r.slot, r.epoch),
    onRevoke: (type, slot) => report('revoke', type, slot),
  };
}

/** A naming client of the peer, with the node that resolves through it. */
export class NamingHandle {
  /** The naming client. */
  readonly client: NamingClient;
  /** A node whose resolver is a NamingResolver over {@link client}. */
  readonly node: Switchboard;

  constructor(client: NamingClient, node: Switchboard) {
    this.client = client;
    this.node = node;
  }

  /** Closes the client and the node. */
  async close(): Promise<void> {
    await this.client.close();
    await this.node.close();
  }
}

/** The naming commands. */
export const namingOps = [
  'naming.open',
  'naming.synced',
  'naming.register',
  'naming.unregister',
  'naming.defineSlots',
  'naming.claim',
  'naming.release',
  'naming.locate',
  'naming.migrate',
  'naming.channel',
  'naming.close',
] as const;

const slotModes: Record<string, SlotMode> = { managed: SlotMode.managed, static: SlotMode.static };

/** The naming commands (README, "Mesh"). */
export class NamingCommands {
  readonly #ctx: PeerContext;
  readonly #newNode: (resolver: Resolver) => Switchboard;

  /** The naming commands of the peer [ctx]; [newNode] makes a client node with a resolver. */
  constructor(ctx: PeerContext, newNode: (resolver: Resolver) => Switchboard) {
    this.#ctx = ctx;
    this.#newNode = newNode;
  }

  /** Runs the naming command [op]; [adoptTalk] makes a Talk channel handle of a channel. */
  async command(op: string, cmd: Json, adoptTalk: (channel: MuxChannel) => Json): Promise<unknown> {
    const ctx = this.#ctx;
    if (op === 'naming.open') {
      return this.#open(cmd);
    }
    const n = ctx.handles.get(cmd['naming'], 'n', 'naming client');
    const client = n.client;
    const type = (): Name => Name.of(requiredString(cmd, 'type'));
    switch (op) {
      case 'naming.synced':
        await client.firstSynced;
        return null;
      case 'naming.register': {
        const endpoints = arrayArg(cmd, 'endpoints')?.map(String) ?? ctx.endpoints;
        const metadata = cmd['metadata'];
        const instance = await client.register(type(), endpoints, {
          instance: intArg(cmd, 'instance') ?? 0,
          ...(metadata === undefined || metadata === null ? {} : { metadata: bytesOf(metadata) }),
        });
        return { instance };
      }
      case 'naming.unregister':
        await client.unregister(type(), intArg(cmd, 'instance') ?? 0);
        return null;
      case 'naming.defineSlots': {
        const mode = slotModes[stringArg(cmd, 'mode') ?? 'managed'];
        if (mode === undefined) {
          throw new ArgumentProblem(`unknown slot mode ${stringArg(cmd, 'mode')}`);
        }
        await client.defineSlots(type(), {
          count: intArg(cmd, 'count') ?? 1,
          mode,
          lazy: boolArg(cmd, 'lazy'),
          shared: boolArg(cmd, 'shared'),
          capacity: intArg(cmd, 'capacity') ?? 1,
        });
        return null;
      }
      case 'naming.claim': {
        const epoch = await client.claim(type(), intArg(cmd, 'slot') ?? 0, {
          holding: boolArg(cmd, 'holding'),
        });
        return { epoch };
      }
      case 'naming.release':
        await client.release(type(), intArg(cmd, 'slot') ?? 0, {
          keepStorage: boolArg(cmd, 'keepStorage'),
        });
        return null;
      case 'naming.locate': {
        const r = await client.locate(type(), intArg(cmd, 'slot') ?? 0);
        return { state: slotStateName(r.state), owner: r.owner, epoch: r.epoch };
      }
      case 'naming.migrate': {
        const phases: string[] = [];
        for await (const item of client.migrate(type(), intArg(cmd, 'slot') ?? 0, {
          to: intArg(cmd, 'to') ?? 0,
        })) {
          phases.push(migrationPhaseName(item.phase));
        }
        return { phases };
      }
      case 'naming.channel': {
        const a = objectArg(cmd, 'address') ?? {};
        const payload = a['payload'];
        const channel = await n.node.openChannel(
          new ServiceAddress(requiredString(a, 'type'), intArg(a, 'instance') ?? 0),
          {
            shard: intArg(a, 'shard'),
            ...(payload === undefined || payload === null ? {} : { payload: bytesOf(payload) }),
          },
        );
        return adoptTalk(channel);
      }
      case 'naming.close':
        await n.close();
        return null;
      default:
        throw new ArgumentProblem(`unknown op ${op}`);
    }
  }

  async #open(cmd: Json): Promise<Json> {
    const ctx = this.#ctx;
    const endpoint = requiredString(cmd, 'endpoint');
    let node: Switchboard | undefined;
    const client = new NamingClient(
      async () => {
        if (node === undefined) {
          throw new Error('the resolver node is not made yet');
        }
        return node.openTalkAt(
          endpoint,
          new ChannelAddress({ type: Services.naming, instance: 1 }),
        );
      },
      { reconnectDelay: 100, watch: boolArg(cmd, 'watch', true) },
    );
    node = this.#newNode(new NamingResolver(client));
    const handle = ctx.handles.add('n', new NamingHandle(client, node));
    client.slotHandler = reportingSlots(ctx, () => handle);
    void (async () => {
      try {
        for await (const e of client.events) {
          ctx.out.event('naming.event', {
            naming: handle,
            up: e.up,
            type: e.record.address.type.toString(),
            instance: e.record.address.instance,
            endpoints: [...e.record.endpoints],
            metadata: hex(e.record.metadata),
          });
        }
      } catch {
        // The client closed.
      }
    })();
    await client.start();
    return { naming: handle };
  }
}
