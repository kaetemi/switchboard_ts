/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Port of the Dart `test/integration/sharding/cluster.dart`, the shared
// setup of the sharding use case tests: a naming service and mesh nodes on
// 127.0.0.1 (TCP or WebSocket, port 0) or in this process (`mem`), with
// short timeouts, torn down in a fixed order.

import { afterEach } from 'vitest';
import type { MuxConnection, MuxOptions } from '../../../src/core/mux/contract.ts';
import type { Name } from '../../../src/core/name.ts';
import type { NamingClient } from '../../../src/core/naming/naming-client.ts';
import { Services, SlotState } from '../../../src/core/naming/naming-protocol.ts';
import { NamingService } from '../../../src/core/naming/naming-service.ts';
import type { SlotEvent } from '../../../src/core/naming/slot-table.ts';
import { StatusCode, SwitchboardError } from '../../../src/core/status.ts';
import type { Resolver } from '../../../src/core/switchboard/contract.ts';
import { MeshNode } from '../../../src/core/switchboard/mesh-node.ts';
import type { Switchboard as CoreSwitchboard } from '../../../src/core/switchboard/switchboard.ts';
import type { TalkOptions } from '../../../src/core/talk/contract.ts';
import { Switchboard } from '../../../src/node/switchboard.ts';
import { listen, type Scheme } from '../mesh-support.ts';

export type { Scheme };

/** No keep-alive and a short GOAWAY grace. */
export const mux: Partial<MuxOptions> = Object.freeze({
  goAwayGrace: 100,
  keepAliveInterval: undefined,
});

/** The Talk options of every node and naming channel. */
export const talkOptions: Partial<TalkOptions> = Object.freeze({
  requestTimeout: 2000,
  replyTimeout: 1000,
});

/** Delay before each reconnect to the naming service, in ms. */
export const reconnectDelay = 20;

/** Upper bound for anything a test waits on, in ms; only reached on failure. */
export const limit = 10_000;

/** Waits [ms] of real time. */
export function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Polls [condition] every few milliseconds; fails after {@link limit}. */
export async function until(condition: () => boolean, what = 'condition'): Promise<void> {
  const deadline = performance.now() + limit;
  while (!condition()) {
    if (performance.now() > deadline) {
      throw new Error(`${what} not met within ${limit} ms`);
    }
    await delay(5);
  }
}

/** Resolves [promise], or rejects after [ms] of real time. */
export function within<T>(promise: Promise<T>, ms = limit): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`not settled within ${ms} ms`)), ms);
    promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (error: unknown) => {
        clearTimeout(timer);
        reject(error);
      },
    );
  });
}

/**
 * A seeded generator of integers in `[0, n)` (Dart's `Random(seed).nextInt`;
 * not the same sequence): mulberry32.
 */
export function seededRandom(seed: number): (n: number) => number {
  let state = seed >>> 0;
  return (n) => {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return Math.floor((((t ^ (t >>> 14)) >>> 0) / 2 ** 32) * n);
  };
}

const retryCodes: ReadonlySet<number> = new Set([
  StatusCode.moved,
  StatusCode.unavailable,
  StatusCode.connectionLost,
  StatusCode.goingAway,
  StatusCode.cancelled,
]);

/**
 * Whether [error] is a failure a client retries after: the slot moved, its
 * owner is not there (yet), or the channel to it went away.
 */
export function retryable(error: unknown): boolean {
  return error instanceof SwitchboardError && retryCodes.has(error.code);
}

/** The code of [error] when it is a {@link SwitchboardError}. */
export function errorCode(error: unknown): number | undefined {
  return error instanceof SwitchboardError ? error.code : undefined;
}

const teardowns: (() => unknown)[] = [];
afterEach(async () => {
  for (const teardown of teardowns.splice(0).reverse()) {
    await teardown();
  }
});

/** Runs [teardown] after the current test, before those registered earlier (Dart's `addTearDown`). */
export function addTearDown(teardown: () => unknown): void {
  teardowns.push(teardown);
}

/**
 * A naming service and the nodes of one test.
 *
 * {@link close} (registered as a tear-down by {@link start}) stops the
 * naming service first, so that leaving nodes do not hand their slots
 * around, then leaves every mesh, then closes every node.
 */
export class Cluster {
  /** `tcp`, `ws` or `mem`. */
  readonly scheme: Scheme;

  #namingNode: Switchboard | undefined;
  #naming: NamingService | undefined;
  #namingUri: string | undefined;

  readonly #meshes: MeshNode[] = [];
  readonly #nodes: Switchboard[] = [];
  readonly #connections = new Map<CoreSwitchboard, MuxConnection[]>();

  constructor(scheme: Scheme) {
    this.scheme = scheme;
  }

  /** The node hosting the naming service. */
  get namingNode(): Switchboard {
    return started(this.#namingNode);
  }

  /** The naming service. */
  get naming(): NamingService {
    return started(this.#naming);
  }

  /** Where the naming service listens. */
  get namingUri(): string {
    return started(this.#namingUri);
  }

  /** Starts the naming service (holders of a lost instance kept [options.holderGrace] ms, default a minute). */
  async start(options: { holderGrace?: number } = {}): Promise<void> {
    addTearDown(() => this.close());
    const naming = new NamingService({
      assignmentHold: 0,
      holderGrace: options.holderGrace ?? 60_000,
      handoverTimeout: 5000,
    });
    this.#naming = naming;
    const namingNode = await this.node();
    this.#namingNode = namingNode;
    namingNode.registerService(Services.naming, naming.handler, { instance: 1 });
    const uri = namingNode.listeningEndpoints[0] as string;
    this.#namingUri = uri;
    naming.registerLocal(Services.naming, [uri], { instance: 1 });
  }

  /** A new node listening on 127.0.0.1, or in this process for `mem`. */
  async node(
    options: { resolver?: Resolver | undefined; listen?: boolean } = {},
  ): Promise<Switchboard> {
    const node = new Switchboard({ resolver: options.resolver, muxOptions: mux, talkOptions });
    this.#nodes.push(node);
    const connections: MuxConnection[] = [];
    this.#connections.set(node, connections);
    void (async () => {
      for await (const connection of node.connections) {
        connections.push(connection);
      }
    })();
    if (options.listen ?? true) {
      await listen(node, this.scheme);
    }
    return node;
  }

  /** Joins [node] to the mesh; [options] are the Talk options of its naming channel. */
  join(node: Switchboard, options: Partial<TalkOptions> = talkOptions): MeshNode {
    const mesh = MeshNode.join(node, this.namingUri, {
      reconnectDelay,
      resolveTimeout: limit,
      talkOptions: options,
      leaveTimeout: 2000,
    });
    this.#meshes.push(mesh);
    return mesh;
  }

  /** A new node joined to the mesh, synced; `talkOptions` are those of its naming channel. */
  async member(
    options: { listen?: boolean; talkOptions?: Partial<TalkOptions> } = {},
  ): Promise<MeshNode> {
    const listening = options.listen ?? true;
    const mesh = this.join(await this.node({ listen: listening }), options.talkOptions);
    await within(mesh.synced);
    return mesh;
  }

  /**
   * Stops [mesh]'s node the way a crash does: every connection is cut at
   * once, without GOAWAY and without leaving the mesh (the naming service
   * sees the registration channel drop). Its naming client is closed
   * afterwards, while nothing is reachable any more.
   */
  async crash(mesh: MeshNode): Promise<void> {
    const node = mesh.switchboard;
    const cut = (this.#connections.get(node) ?? []).map((connection) => connection.close());
    const closed = node.close();
    await Promise.all(cut);
    await closed;
    await mesh.leave();
  }

  async close(): Promise<void> {
    await this.#naming?.close();
    await this.#namingNode?.close();
    for (const mesh of this.#meshes) {
      await mesh.leave();
    }
    await Promise.all(this.#nodes.map((node) => node.close()));
  }
}

function started<T>(value: T | undefined): T {
  if (value === undefined) {
    throw new Error('the cluster is not started');
  }
  return value;
}

/**
 * Watches a mirror for migrations: how many slots migrate at once (at
 * most), and every slot that changed owner, in order. Stops after the
 * test.
 */
export class MigrationWatch {
  readonly type: Name;
  readonly #migrating = new Set<number>();

  /** The most slots seen migrating at the same time. */
  maxConcurrent = 0;

  /** Completed migrations: [slot, from, to]. */
  readonly moves: [number, number, number][] = [];

  constructor(client: NamingClient, type: Name) {
    this.type = type;
    const stop = client.on('slotEvent', (event) => this.#onEvent(event));
    addTearDown(stop);
  }

  #onEvent(event: SlotEvent): void {
    if (!event.type.equals(this.type)) {
      return;
    }
    if (event.entry.state === SlotState.migrating) {
      this.#migrating.add(event.slot);
      if (this.#migrating.size > this.maxConcurrent) {
        this.maxConcurrent = this.#migrating.size;
      }
    } else {
      this.#migrating.delete(event.slot);
    }
    if (
      event.previous.state === SlotState.migrating &&
      event.entry.state === SlotState.owned &&
      event.entry.owner !== event.previous.owner
    ) {
      this.moves.push([event.slot, event.previous.owner, event.entry.owner]);
    }
  }

  get idle(): boolean {
    return this.#migrating.size === 0;
  }
}
