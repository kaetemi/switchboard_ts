/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// The mesh parts of a peer (README, "Mesh"), as dart_peer/lib/src/mesh.dart
// has them: a naming service, naming clients with their slot calls and
// resolution, and the proxy and relay in front of another peer.

import {
  type AssignRequest,
  AssignResult,
  ChannelAddress,
  type DrainRequest,
  type ForwardRequest,
  type MuxChannel,
  migrationPhaseName,
  Name,
  NamingClient,
  NamingResolver,
  NamingService,
  type NamingServiceOptions,
  proxyHandler,
  RelayService,
  type Resolver,
  type ResumeRequest,
  ServiceAddress,
  Services,
  type SlotChannel,
  type SlotEntry,
  type SlotHandler,
  SlotMode,
  type SlotRequestContext,
  StaticResolver,
  StatusCode,
  type Switchboard,
  SwitchboardError,
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
  statusArg,
  stringArg,
} from './protocol.ts';

/** The `naming`, `proxy` and `relay` configuration. */
export interface MeshConfig {
  /**
   * `{assignmentHoldMs, holdingSettleMs, handoverTimeoutMs, assignBackoffMs,
   * assignBackoffMaxMs, holderGraceMs, resumeAttempts}`: serve a naming
   * service as `_ns` instance 1.
   */
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

/** The naming service's options in [json]; the library's defaults for those absent, but no assignment hold. */
function namingServiceOptions(json: Json): NamingServiceOptions {
  const options: { -readonly [K in keyof NamingServiceOptions]: NamingServiceOptions[K] } = {
    assignmentHold: intArg(json, 'assignmentHoldMs') ?? 0,
  };
  const set = (
    key:
      | 'holdingSettle'
      | 'handoverTimeout'
      | 'assignBackoff'
      | 'assignBackoffMax'
      | 'holderGrace'
      | 'resumeAttempts',
    arg: string,
  ) => {
    const value = intArg(json, arg);
    if (value !== undefined) {
      options[key] = value;
    }
  };
  set('holdingSettle', 'holdingSettleMs');
  set('handoverTimeout', 'handoverTimeoutMs');
  set('assignBackoff', 'assignBackoffMs');
  set('assignBackoffMax', 'assignBackoffMaxMs');
  set('holderGrace', 'holderGraceMs');
  set('resumeAttempts', 'resumeAttempts');
  return options;
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
      this.#naming = new NamingService(namingServiceOptions(naming));
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

/** The requests a naming client's slot handler serves, by their names in `naming.slot`. */
const slotRequests = ['assign', 'drain', 'forward', 'resume'] as const;
type SlotRequest = (typeof slotRequests)[number];

/**
 * The slot handler of the peer's naming clients (README, "Mesh"): it
 * reports every request as a `naming.slot` event and answers it as
 * `naming.slotAnswers` set, at once by default (`ASSIGN` answered
 * holding). An answer `wait` holds the request until `naming.slotRelease`
 * or until it is cancelled; an answer `{code, reason}` refuses it with that
 * status.
 */
class PeerSlots implements SlotHandler {
  readonly #ctx: PeerContext;
  readonly #handle: () => string;
  /** The answer to each request; absent: holding for `ASSIGN`, done for the others. */
  readonly #answers = new Map<SlotRequest, unknown>();
  /** The requests waiting for `naming.slotRelease`, by request, type and slot. */
  readonly #waiting = new Map<string, (answer: unknown) => void>();

  constructor(ctx: PeerContext, handle: () => string) {
    this.#ctx = ctx;
    this.#handle = handle;
  }

  /** Sets the answers [cmd] names (`assign`, `drain`, `forward`, `resume`). */
  setAnswers(cmd: Json): void {
    for (const request of slotRequests) {
      const answer = cmd[request];
      if (answer !== undefined) {
        this.#answers.set(request, checkAnswer(request, answer));
      }
    }
  }

  /**
   * Answers the waiting [request] for [slot] of [type] with [answer]
   * (default: at once, `ASSIGN` holding).
   */
  release(request: string, type: Name, slot: number, answer: unknown): void {
    const key = `${request} ${type} ${slot}`;
    const resume = this.#waiting.get(key);
    if (resume === undefined) {
      throw new ArgumentProblem(`no ${request} of ${type}/${slot} is waiting`);
    }
    if (answer === 'wait') {
      throw new ArgumentProblem('a released request cannot wait again');
    }
    this.#waiting.delete(key);
    resume(answer === undefined || answer === null ? undefined : checkAnswer(request, answer));
  }

  async onAssign(r: AssignRequest, c: SlotRequestContext): Promise<AssignResult> {
    const answer = await this.#serve('assign', r.type, r.slot, c, {
      epoch: r.epoch,
      to: null,
      holder: r.holder,
    });
    return answer === 'notHolding' ? AssignResult.notHolding : AssignResult.holding;
  }

  async onDrain(r: DrainRequest, c: SlotRequestContext): Promise<void> {
    await this.#serve('drain', r.type, r.slot, c, { epoch: r.epoch, to: r.to, holder: null });
  }

  async onForward(r: ForwardRequest, c: SlotRequestContext): Promise<void> {
    await this.#serve('forward', r.type, r.slot, c, { epoch: r.epoch, to: r.to, holder: null });
  }

  async onResume(r: ResumeRequest, c: SlotRequestContext): Promise<void> {
    await this.#serve('resume', r.type, r.slot, c, { epoch: r.epoch, to: null, holder: null });
  }

  onRevoke(type: Name, slot: number): void {
    this.#report('revoke', type, slot, { epoch: null, to: null, holder: null });
  }

  /** Reports [request] and answers it: resolves with the answer, or throws a refusal. */
  async #serve(
    request: SlotRequest,
    type: Name,
    slot: number,
    context: SlotRequestContext,
    fields: Json,
  ): Promise<unknown> {
    let answer = this.#answers.get(request);
    if (answer === 'wait') {
      const key = `${request} ${type} ${slot}`;
      let resume!: (answer: unknown) => void;
      const released = new Promise<unknown>((resolve) => {
        resume = resolve;
      });
      this.#waiting.set(key, resume);
      this.#report(request, type, slot, fields);
      answer = await Promise.race([released, context.onCancel.then(() => cancelled)]);
      if (answer === cancelled) {
        if (this.#waiting.get(key) === resume) {
          this.#waiting.delete(key);
        }
        throw SwitchboardError.of(StatusCode.cancelled, 'the request was cancelled');
      }
    } else {
      this.#report(request, type, slot, fields);
    }
    if (typeof answer === 'object' && answer !== null) {
      throw new SwitchboardError(statusArg(answer as Json));
    }
    return answer;
  }

  #report(request: string, type: Name, slot: number, fields: Json): void {
    this.#ctx.out.event('naming.slot', {
      naming: this.#handle(),
      request,
      type: type.toString(),
      slot,
      ...fields,
    });
  }
}

/** What a request held with `wait` resolves with when it is cancelled. */
const cancelled = Symbol('cancelled');

/** [answer] if it is one [request] can be given, else throws. */
function checkAnswer(request: string, answer: unknown): unknown {
  const named = request === 'assign' ? ['holding', 'notHolding', 'wait'] : ['ok', 'wait'];
  if (
    typeof answer === 'string'
      ? named.includes(answer)
      : typeof answer === 'object' && answer !== null && 'code' in answer
  ) {
    return answer;
  }
  throw new ArgumentProblem(`${request} cannot be answered ${JSON.stringify(answer)}`);
}

/** The fields of a mirrored slot table [entry], its state by name. */
function entryJson(entry: SlotEntry): Json {
  return {
    state: slotStateName(entry.state),
    owner: entry.owner,
    to: entry.to,
    holder: entry.holder,
    epoch: entry.epoch,
  };
}

/** A naming client of the peer, with the node that resolves through it. */
export class NamingHandle {
  /** The naming client. */
  readonly client: NamingClient;
  /** A node whose resolver is a NamingResolver over {@link client}. */
  readonly node: Switchboard;
  /** The client's slot handler. */
  readonly slots: PeerSlots;

  constructor(client: NamingClient, node: Switchboard, slots: PeerSlots) {
    this.client = client;
    this.node = node;
    this.slots = slots;
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
  'naming.declareHolding',
  'naming.slotAnswers',
  'naming.slotRelease',
  'naming.slotOwner',
  'naming.slotChannel',
  'naming.slotTalk',
  'naming.channel',
  'naming.close',
  'slot.send',
  'slot.close',
  'slot.state',
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
  async command(
    op: string,
    cmd: Json,
    adoptTalk: (channel: MuxChannel | SlotChannel) => Json,
  ): Promise<unknown> {
    const ctx = this.#ctx;
    if (op === 'naming.open') {
      return this.#open(cmd);
    }
    if (op.startsWith('slot.')) {
      return this.#slotCommand(op, cmd);
    }
    const handle = requiredString(cmd, 'naming');
    const n = ctx.handles.get(handle, 'n', 'naming client');
    const client = n.client;
    const type = (): Name => Name.of(requiredString(cmd, 'type'));
    const slot = (): number => intArg(cmd, 'slot') ?? 0;
    const payload = (): { payload?: Uint8Array } => {
      const p = cmd['payload'];
      return p === undefined || p === null ? {} : { payload: bytesOf(p) };
    };
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
        const [t, s] = [type(), slot()];
        const phases: string[] = [];
        for await (const item of client.migrate(t, s, { to: intArg(cmd, 'to') ?? 0 })) {
          const phase = migrationPhaseName(item.phase);
          phases.push(phase);
          ctx.out.event('naming.phase', { naming: handle, type: t.toString(), slot: s, phase });
        }
        return { phases };
      }
      case 'naming.declareHolding': {
        const slots = (arrayArg(cmd, 'slots') ?? []).map(Number);
        return { discard: await client.declareHolding(type(), slots) };
      }
      case 'naming.slotAnswers':
        n.slots.setAnswers(cmd);
        return null;
      case 'naming.slotRelease':
        n.slots.release(requiredString(cmd, 'request'), type(), slot(), cmd['answer']);
        return null;
      case 'naming.slotOwner': {
        const owner = await n.node.resolveSlotOwner(type(), slot(), {
          refresh: boolArg(cmd, 'refresh'),
        });
        return { owner: owner.instance };
      }
      case 'naming.slotChannel': {
        const channel = await n.node.openChannelToSlot(type(), slot(), payload());
        return { slotChannel: ctx.trackSlotChannel(channel) };
      }
      case 'naming.slotTalk':
        return adoptTalk(await n.node.openChannelToSlot(type(), slot(), payload()));
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

  /** The commands on a slot channel. */
  async #slotCommand(op: string, cmd: Json): Promise<unknown> {
    const channel = this.#ctx.handles.get(cmd['slotChannel'], 'sc', 'slot channel');
    switch (op) {
      case 'slot.send':
        await channel.send(bytesOf(cmd['hex']));
        return null;
      case 'slot.close':
        await channel.close(statusArg(cmd));
        return null;
      case 'slot.state':
        return { retried: channel.retried };
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
    const slots = new PeerSlots(ctx, () => handle);
    const handle = ctx.handles.add('n', new NamingHandle(client, node, slots));
    client.slotHandler = slots;
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
    void (async () => {
      try {
        for await (const e of client.slotEvents) {
          ctx.out.event('naming.table', {
            naming: handle,
            type: e.type.toString(),
            slot: e.slot,
            ...entryJson(e.entry),
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
