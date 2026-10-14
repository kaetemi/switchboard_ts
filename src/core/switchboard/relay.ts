/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { ChannelAddress } from '../address/index.ts';
import { type PeerIdentity, Right } from '../identity/contract.ts';
import { isKnownStatusCode, StatusCode, SwitchboardError } from '../status.ts';
import type { ChannelHandler, IncomingChannel, Resolver, ServiceRecord } from './contract.ts';
import { ForwardingBound, type ForwardTarget, openForwarded } from './forwarding.ts';
import { genericStatus } from './generic-status.ts';
import { waitForIdentityChange } from './identity-wait.ts';
import { pipeChannelsWithin } from './proxy.ts';
import type { Switchboard } from './switchboard.ts';

const logName = 'Switchboard.Relay';

/** What a {@link RelayService} takes beside the node. */
export interface RelayServiceOptions {
  /** Whether consumers must identify to have channels relayed; default true. See {@link RelayService.requireIdentity}. */
  readonly requireIdentity?: boolean | undefined;
  /** Whether destinations whose records have endpoints are relayed too; default false. */
  readonly allowEndpoints?: boolean | undefined;
  /** Channels relayed at once per consumer connection; default 1024; 0: no bound. */
  readonly maxChannelsPerConnection?: number | undefined;
  /** The resolver of the destinations; default the node's. */
  readonly resolver?: Resolver | undefined;
}

/**
 * The `_relay` service (`Services.relay`): forwards channels to instances
 * registered without endpoints, which listen nowhere, on behalf of
 * consumers that listen nowhere either (wiki page "Polyverse Switchboard
 * Identity and Credentials", section "Relay"; use case 5c).
 *
 * A consumer opens a channel of type `_relay` whose application payload is
 * the complete open payload of the channel to relay: the address header of
 * the destination, then the application payload for it (`Switchboard`'s
 * relayed opens do this). For each such channel, {@link handler}:
 *
 * 1. Reads the peer identity of the connection it arrived on. Without one,
 *    the channel is held for the consumer to identify, at most the node's
 *    `Switchboard.identityTimeout`, so that a consumer whose OPEN overtakes
 *    its `IDENT` is checked under the identity the `IDENT` establishes; a
 *    channel that ends while held is dropped. A consumer still
 *    unidentified after the hold is refused with UNAUTHENTICATED when
 *    {@link requireIdentity} (the default), and admitted unchecked
 *    otherwise. A node without a verifier (`Switchboard.verifier`)
 *    identifies nobody: there, without {@link requireIdentity}, an
 *    unidentified consumer is admitted at once, without the hold.
 * 2. Decodes the inner open payload: malformed, without a service type,
 *    with a host hint, or addressing a reserved type (any name starting
 *    with `_`: no chains of relays, no naming service through a relay):
 *    INVALID_ARGUMENT.
 * 3. Checks that an identified consumer's credential has `open` for the
 *    inner type: PERMISSION_DENIED otherwise. The instance sees only the
 *    relay's identity, so the consumer's scopes are checked here. An
 *    unidentified consumer (admitted without {@link requireIdentity}) is
 *    not checked. A consumer that identifies (again) while the channel to
 *    the instance is being opened (steps 4 to 7) is checked again under
 *    its new identity: if that may not open the type, the channel to the
 *    instance is closed (CANCELLED) and the consumer's refused with
 *    PERMISSION_DENIED. With {@link requireIdentity}, a credential that
 *    expires meanwhile likewise closes the channel to the instance and
 *    refuses the consumer's with UNAUTHENTICATED. An identity that arrives
 *    (or expires) once the two channels are piped does not affect them.
 * 4. Bounds the channels it relays per consumer connection to
 *    {@link maxChannelsPerConnection} (0: no bound): RESOURCE_EXHAUSTED
 *    beyond, since every consumer shares the relay's connection to each
 *    instance.
 * 5. Resolves the inner address with {@link resolver} (default: the
 *    node's) as `Switchboard.selectAndConnect` does: the instance it
 *    names, else the owner of its shard slot through the slot table, else
 *    round robin. No record: NOT_FOUND. A record with endpoints:
 *    FAILED_PRECONDITION (the consumer can reach it directly), unless
 *    {@link allowEndpoints}, in which case the relay connects to its
 *    endpoints as a proxy does, never to this node's own listeners.
 * 6. Brokers a connection to the instance through the naming service's
 *    `CONNECT` with this node's listener and credential
 *    (`Switchboard.broker`), pooled by instance and shared by every
 *    consumer: one connection per instance however many consumers relay to
 *    it. A brokering failure is UNAVAILABLE.
 * 7. Opens the channel to the instance over it with the inner open payload
 *    (the selected instance filled in, the application payload unchanged)
 *    and pipes the two channels (`pipeChannels`): data unchanged both
 *    ways, the CLOSE status of either side forwarded to the other (a
 *    refusal by the instance relayed unchanged, MOVED and RELOCATED with
 *    their owner and epoch byte for byte), a lost connection on either
 *    side reported to the other as UNAVAILABLE.
 *
 * Refusals carry the status code and a generic reason; the details go to
 * the log (`Switchboard.Relay`, at debug and info).
 *
 * The relay node: a `Switchboard` that listens where consumers and
 * instances can reach it, joined to the mesh (`MeshNode.join`) with its own
 * credential, registered as `_relay` with its identity in the record's
 * metadata (`MeshNode.publishRelay`), and a verifier, so that it can tell
 * who connected. Its credential has `register _relay`, and `broker`,
 * `open` and `watch` for the types it relays: the instance checks the
 * relay's `open` scope, and the naming service its `broker` right. Give
 * its node `muxOptions: { requireNamedIdent: true }`, as the naming
 * service's host has, so that no identification a consumer meant for
 * another peer can be relayed to it (the constructor logs a warning
 * otherwise), and give its listener `ChannelPolicies.scoped()` (which
 * admits `_relay` for every identified peer and refuses everything else a
 * consumer has no `open` scope for).
 *
 * ```ts
 * const node = new Switchboard({
 *   muxOptions: { requireNamedIdent: true },
 *   credential: relayCredential,
 *   holderKey: relayKey,
 *   verifier,
 *   expectedIdentityFor: (endpoint) => (endpoint === namingEndpoint ? 'ns' : undefined),
 * });
 * await node.listenTcp('0.0.0.0', 9400, { policy: ChannelPolicies.scoped() }); // the Node entry's node
 * const mesh = MeshNode.join(node, namingEndpoint);
 * await mesh.publishRelay(new RelayService(node));
 * ```
 */
export class RelayService {
  /**
   * The node the relay forwards through: its resolver, listener and
   * credential broker the connections to the instances.
   */
  readonly switchboard: Switchboard;

  /**
   * Whether consumers must identify (`IDENT`) to have channels relayed.
   * Default true.
   *
   * An identified consumer is held to its `open` scopes either way: a
   * channel of a consumer that has not identified is held until it does,
   * at most the node's `Switchboard.identityTimeout`, whatever this says,
   * so that an OPEN that overtakes the consumer's `IDENT` is checked under
   * the identity the `IDENT` establishes, and a consumer that identifies
   * while its channel is being opened is checked again. Only what happens
   * after the hold differs: with this, a consumer still unidentified is
   * refused with UNAUTHENTICATED; without, it is admitted unchecked (a mesh
   * without identities, or moving to them), after waiting out the hold
   * (none with a timeout of zero). A node without a verifier
   * (`Switchboard.verifier`) identifies nobody, so there, without this, an
   * unidentified consumer is admitted at once.
   */
  readonly requireIdentity: boolean;

  /**
   * Whether destinations whose records have endpoints are relayed too
   * (connected to like a proxy does), for consumers whose network cannot
   * reach the mesh's endpoints. Default false: such a channel is refused
   * with FAILED_PRECONDITION.
   */
  readonly allowEndpoints: boolean;

  /** Largest number of channels relayed at once for one consumer connection; 0 for no bound. Default 1024. */
  readonly maxChannelsPerConnection: number;

  /** The resolver of the destinations; undefined for the node's. */
  readonly resolver: Resolver | undefined;

  readonly #bound: ForwardingBound;

  /**
   * A relay forwarding through [switchboard] (see the class
   * documentation). Logs a warning when [switchboard] does not require
   * named identifications (`MuxOptions.requireNamedIdent`), which a
   * relay's host should.
   *
   * @throws {RangeError} for a negative `maxChannelsPerConnection`.
   */
  constructor(switchboard: Switchboard, options: RelayServiceOptions = {}) {
    const max = options.maxChannelsPerConnection ?? 1024;
    if (!(max >= 0)) {
      throw new RangeError(`maxChannelsPerConnection must not be negative: ${max}`);
    }
    this.switchboard = switchboard;
    this.requireIdentity = options.requireIdentity ?? true;
    this.allowEndpoints = options.allowEndpoints ?? false;
    this.maxChannelsPerConnection = max;
    this.resolver = options.resolver;
    this.#bound = new ForwardingBound(max);
    if (!switchboard.muxOptions.requireNamedIdent) {
      this.#log(
        'warn',
        'relay: the node accepts identifications that name nobody, so an IDENT a consumer ' +
          "meant for another peer can be relayed to it; give the relay's node " +
          'muxOptions { requireNamedIdent: true }',
      );
    }
  }

  /** Serves `_relay` channels; register it as the handler of `Services.relay` (`MeshNode.publishRelay` does). */
  get handler(): ChannelHandler {
    return this.#relay;
  }

  readonly #relay = async (incoming: IncomingChannel): Promise<void> => {
    const connection = incoming.connection;
    let identity = incoming.peerIdentity;
    if (
      identity === undefined &&
      (this.requireIdentity || this.switchboard.verifier !== undefined)
    ) {
      // Held even when unidentified consumers may pass, so that an OPEN
      // that overtakes the IDENT is checked under the identity it
      // establishes.
      identity = await this.#identified(incoming);
      if (identity === undefined) {
        if (incoming.channel.state !== 'open') {
          this.#log('debug', `relay: ${incoming} ended while held for the consumer to identify`);
          return;
        }
        if (this.requireIdentity) {
          this.#log('info', `relay: ${incoming} refused, the consumer has not identified`);
          await incoming.reject(genericStatus(StatusCode.unauthenticated));
          return;
        }
      }
    }
    let inner: ChannelAddress;
    try {
      inner = ChannelAddress.decode(incoming.address.payload);
    } catch (e) {
      if (!(e instanceof SwitchboardError)) throw e;
      this.#log('debug', `relay: ${incoming} carries a malformed open payload`, e);
      await incoming.reject(genericStatus(StatusCode.invalidArgument));
      return;
    }
    const type = inner.type;
    if (type === undefined || inner.host !== undefined || type.isReserved) {
      this.#log(
        'debug',
        `relay: ${incoming} refused, it addresses ${inner}, which a relay does not forward to`,
      );
      await incoming.reject(genericStatus(StatusCode.invalidArgument));
      return;
    }
    const who = identity === undefined ? 'an unidentified consumer' : `"${identity.identity}"`;
    if (identity !== undefined && !identity.allows(Right.open, type)) {
      this.#log('info', `relay: ${incoming} refused, ${who} may not open ${type}`);
      await incoming.reject(genericStatus(StatusCode.permissionDenied));
      return;
    }
    if (!this.#bound.enter(connection)) {
      this.#log(
        'info',
        `relay: ${incoming} refused, ${this.#bound.countOf(connection)} channels of this ` +
          'connection are being relayed',
      );
      await incoming.reject(genericStatus(StatusCode.resourceExhausted));
      return;
    }
    try {
      let hasEndpoints = false;
      let target: ForwardTarget;
      try {
        target = await openForwarded(this.switchboard, inner, type, {
          resolver: this.resolver,
          where: this.allowEndpoints
            ? undefined
            : (record: ServiceRecord) => {
                if (record.endpoints.length === 0) {
                  return true;
                }
                hasEndpoints = true;
                return false;
              },
        });
      } catch (e) {
        if (!(e instanceof SwitchboardError)) throw e;
        let code = isKnownStatusCode(e.code) ? e.code : StatusCode.unavailable;
        if (code === StatusCode.notFound && hasEndpoints) {
          this.#log('debug', `relay: ${incoming} refused, ${inner.address} is reachable directly`);
          code = StatusCode.failedPrecondition;
        } else {
          this.#log('info', `relay: ${incoming} for ${who}: ${e.status}`);
        }
        await incoming.reject(genericStatus(code));
        return;
      }
      const channel = target.channel;
      // The consumer may have identified (again) while the channel to the
      // instance was being opened: the identity it has now is the one held
      // to its scopes. It may also have expired during that wait.
      const now = connection.peerIdentity;
      if (now === undefined && this.requireIdentity) {
        this.#log('info', `relay: ${incoming} refused, the consumer identity expired`);
        quietly(channel.close(genericStatus(StatusCode.cancelled)));
        await incoming.reject(genericStatus(StatusCode.unauthenticated));
        return;
      }
      if (now !== undefined && now !== identity && !now.allows(Right.open, type)) {
        this.#log(
          'info',
          `relay: ${incoming} refused, the consumer identified as "${now.identity}" while the ` +
            `channel to the instance was being opened, and may not open ${type}`,
        );
        quietly(channel.close(genericStatus(StatusCode.cancelled)));
        await incoming.reject(genericStatus(StatusCode.permissionDenied));
        return;
      }
      this.#log(
        'debug',
        `relay: ${incoming} for ${who} piped to ${target.header.address} on channel ` +
          `${channel.id} of ${channel.connection}`,
      );
      await pipeChannelsWithin(incoming.channel, channel, this.#bound);
    } finally {
      this.#bound.exit(connection);
    }
  };

  /**
   * The identity the consumer of [incoming] presents once it identifies,
   * within the node's `Switchboard.identityTimeout` (0: not waited for);
   * undefined when it did not, or the channel ended meanwhile. Closed
   * channels leave no per-channel listener on the pending identity event.
   */
  async #identified(incoming: IncomingChannel): Promise<PeerIdentity | undefined> {
    const connection = incoming.connection;
    const channel = incoming.channel;
    const timeout = this.switchboard.identityTimeout;
    const clock = this.switchboard.clock;
    if (timeout > 0) {
      this.#log('debug', `relay: ${incoming} held until the consumer identifies`);
    }
    const start = clock.now();
    while (connection.peerIdentity === undefined) {
      const left = timeout - (clock.now() - start);
      if (left <= 0 || channel.state !== 'open') {
        return undefined;
      }
      await waitForIdentityChange(channel, left, clock);
    }
    return connection.peerIdentity;
  }

  #log(level: 'debug' | 'info' | 'warn', message: string, error?: unknown): void {
    this.switchboard.logger.log(level, logName, message, error);
  }

  toString(): string {
    return 'RelayService';
  }
}

function quietly(promise: Promise<unknown>): void {
  promise.catch(() => {});
}
