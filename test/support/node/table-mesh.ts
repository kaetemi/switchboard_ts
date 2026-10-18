/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { ServiceAddress } from '../../../src/core/address/index.ts';
import { Right } from '../../../src/core/identity/contract.ts';
import type { MuxConnection } from '../../../src/core/mux/contract.ts';
import { type NameLike, toName } from '../../../src/core/name.ts';
import { serviceRecord } from '../../../src/core/naming/naming-protocol.ts';
import { StatusCode, SwitchboardError } from '../../../src/core/status.ts';
import { ChannelPolicies } from '../../../src/core/switchboard/channel-policy.ts';
import type {
  BrokeringResolver,
  ChannelHandler,
  ChannelPolicy,
  ServiceRecord,
} from '../../../src/core/switchboard/contract.ts';
import { StaticResolver } from '../../../src/core/switchboard/resolver.ts';
import type { Switchboard } from '../../../src/core/switchboard/switchboard.ts';
import { onCleanup } from './harness.ts';

/**
 * A stand-in for a mesh run by a naming service, for the node thread's unit
 * tests (the relay's), which check the node and keep the naming service out
 * of the way; the meshes with the real naming service and `MeshNode` are in
 * `test/integration/` and `test/unit/switchboard/mesh.test.ts`. One table
 * of records that every {@link MeshResolver} of the mesh mirrors at once,
 * instance ids assigned in order, and the naming service's `CONNECT` (an
 * instance registered without endpoints dials the requester back,
 * identifying with the requester's intent and naming it, under
 * {@link brokeredPolicy}). Like the naming service with
 * `requireCredential`, `CONNECT` needs a requester whose credential grants
 * `broker` for the type.
 */
export class TableMesh {
  /** The policy of the connections an instance dials for `CONNECT`, as `MeshNode` has. */
  brokeredPolicy: ChannelPolicy = ChannelPolicies.scoped();

  readonly #records = new Map<string, ServiceRecord>();
  readonly #owners = new Map<string, Switchboard>();
  readonly #resolvers = new Set<MeshResolver>();
  readonly #brokered = new Map<Switchboard, Set<MuxConnection>>();
  #nextId = 1;

  /** A resolver over the mesh's table for [node] (set later with {@link MeshResolver.requester}), closed after the test. */
  resolver(): MeshResolver {
    const resolver = new MeshResolver(this);
    for (const record of this.#records.values()) {
      resolver.add(record);
    }
    this.#resolvers.add(resolver);
    onCleanup(() => resolver.close());
    return resolver;
  }

  /** Every record of the mesh. */
  get records(): ServiceRecord[] {
    return [...this.#records.values()];
  }

  /**
   * Registers [handler] for [type] on [node] under a new instance id and
   * publishes its record, with [options.endpoints] (default: the node's
   * listening endpoints) and [options.metadata]. Returns the id.
   */
  publish(
    node: Switchboard,
    type: NameLike,
    handler: ChannelHandler,
    options: { endpoints?: readonly string[]; metadata?: Uint8Array } = {},
  ): number {
    const id = this.#nextId++;
    const address = new ServiceAddress(toName(type), id);
    node.registerService(address.type, handler, { instance: id });
    const record = serviceRecord(address, {
      endpoints: options.endpoints ?? node.listeningEndpoints,
      metadata: options.metadata,
    });
    this.#records.set(address.key, record);
    this.#owners.set(address.key, node);
    for (const resolver of this.#resolvers) {
      resolver.add(record);
    }
    return id;
  }

  /** Removes the record of [address] from every resolver. */
  unpublish(address: ServiceAddress): void {
    this.#records.delete(address.key);
    this.#owners.delete(address.key);
    for (const resolver of this.#resolvers) {
      resolver.remove(address);
    }
  }

  /** The connections [node] dialled for `CONNECT` that are still open. */
  brokeredConnections(node: Switchboard): MuxConnection[] {
    return [...(this.#brokered.get(node) ?? [])];
  }

  /** `CONNECT` on behalf of [requester]. */
  async connect(
    requester: Switchboard | undefined,
    address: ServiceAddress,
    endpoint: string,
    intent: Uint8Array,
  ): Promise<string> {
    const credential = requester?.credential;
    if (credential === undefined || !credential.allows(Right.broker, address.type)) {
      throw SwitchboardError.of(StatusCode.permissionDenied, `may not broker ${address.type}`);
    }
    const owner = this.#owners.get(address.key);
    if (owner === undefined) {
      throw SwitchboardError.of(StatusCode.notFound, `${address} is not registered`);
    }
    const identity = owner.credential?.identity;
    if (identity === undefined) {
      throw SwitchboardError.of(StatusCode.failedPrecondition, 'no credential to identify with');
    }
    const connection = await owner.dial(endpoint, {
      policy: this.brokeredPolicy,
      intent,
      receiver: credential.identity,
    });
    let brokered = this.#brokered.get(owner);
    if (brokered === undefined) {
      brokered = new Set();
      this.#brokered.set(owner, brokered);
    }
    const set = brokered;
    set.add(connection);
    void connection.done.then(() => set.delete(connection));
    return identity;
  }
}

/** A {@link TableMesh} resolver: the mesh's table, and `CONNECT` as its {@link requester}. */
export class MeshResolver extends StaticResolver implements BrokeringResolver {
  /** The node whose credential `CONNECT` is asked with. */
  requester: Switchboard | undefined;

  readonly #mesh: TableMesh;

  constructor(mesh: TableMesh) {
    super();
    this.#mesh = mesh;
  }

  connectTo(address: ServiceAddress, endpoint: string, intent: Uint8Array): Promise<string> {
    return this.#mesh.connect(this.requester, address, endpoint, intent);
  }
}
