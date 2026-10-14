/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * The `Switchboard` node of `@kaetemi/switchboard/web`: the core node for
 * browsers, which connects and listens on nothing (beyond the in-process
 * `mem://` listeners of the core).
 */

import type { QuicConnector } from '../core/quic/contract.ts';
import {
  QuicSwitchboardBinding,
  type QuicSwitchboardBindingOptions,
} from '../core/quic/quic-switchboard.ts';
import { StatusCode, SwitchboardError } from '../core/status.ts';
import { coreRuntime } from '../core/switchboard/core-runtime.ts';
import type { SwitchboardRuntime, TransportDialer } from '../core/switchboard/runtime.ts';
import {
  Switchboard as CoreSwitchboard,
  type SwitchboardOptions as CoreSwitchboardOptions,
} from '../core/switchboard/switchboard.ts';
import { connectWebSocket } from '../core/transport/websocket-transport.ts';
import type { Clock } from '../core/util/clock.ts';
import type { Logger } from '../core/util/logger.ts';

/** Options of the web {@link Switchboard}: the core node's, and how it reaches WebTransport. */
export interface SwitchboardOptions extends CoreSwitchboardOptions {
  /**
   * The QUIC provider the node dials `https://` (WebTransport) endpoints
   * through, with a `QuicSwitchboardBinding`: the browser's WebTransport
   * provider. Undefined: `https://` endpoints are not dialled
   * (`UNIMPLEMENTED`).
   */
  quic?: QuicConnector | undefined;
  /**
   * The options of the QUIC binding the {@link quic} provider dials
   * through; a browser verifies the server's certificate (or a pin of
   * `serverCertificateHashes`) as it always does.
   */
  quicBinding?: QuicSwitchboardBindingOptions | undefined;
}

/** What the dialers read from the node once it exists. */
interface WebContext {
  clock: Clock | undefined;
  logger: Logger | undefined;
}

/**
 * The runtime of the web entry: the core's, with the `ws` and `wss`
 * dialers over the browser's `WebSocket`, and `tcp` refused as
 * UNSUPPORTED (a browser has no sockets).
 */
function webRuntime(context: WebContext): SwitchboardRuntime {
  const core = coreRuntime();
  const ws: TransportDialer = (endpoint, settings) =>
    connectWebSocket(endpoint, {
      maxFrameSize: settings.maxFrameSize,
      timeout: settings.timeout,
      signal: settings.signal,
      ...(context.clock === undefined ? {} : { clock: context.clock }),
      ...(context.logger === undefined ? {} : { logger: context.logger }),
    });
  const tcp: TransportDialer = (endpoint) =>
    Promise.reject(
      SwitchboardError.of(StatusCode.unsupported, `a browser cannot dial TCP: ${endpoint}`),
    );
  return { ...core, dialers: { ...core.dialers, ws, wss: ws, tcp } };
}

/**
 * A node in a browser: the core `Switchboard` (dispatch, policies, the
 * connection pool, resolvers, identity) that connects over `wss://` and
 * `ws://` (the browser's `WebSocket`) and over `https://` (WebTransport,
 * through the `quic` option's provider), and listens on nothing; the
 * channels a server opens on a connection this node made are dispatched
 * to its services as usual. A `tcp://` endpoint is refused with
 * `UNSUPPORTED`. The message limit of the browser's WebSocket applies to
 * assembled messages: a server is trusted not to send more than the
 * node's `muxOptions.maxFrameSize`.
 *
 * ```ts
 * const node = new Switchboard({ resolver: new EndpointResolver('wss://edge.example.com/sb') });
 * await using talk = await node.openTalk('chat');
 * ```
 */
export class Switchboard extends CoreSwitchboard {
  /** The QUIC provider of the `quic` option, or undefined. */
  readonly quic: QuicConnector | undefined;

  /**
   * Creates a node. See {@link SwitchboardOptions}; a `runtime` of the
   * options replaces the web runtime.
   *
   * @throws {RangeError} as the core node does.
   */
  constructor(options: SwitchboardOptions = {}) {
    const context: WebContext = { clock: undefined, logger: undefined };
    const { quic, quicBinding, ...rest } = options;
    const bindings = [...(options.bindings ?? [])];
    if (quic !== undefined) {
      bindings.push(new QuicSwitchboardBinding(quic, quicBinding ?? {}));
    }
    super({ ...rest, runtime: options.runtime ?? webRuntime(context), bindings });
    context.clock = this.clock;
    context.logger = this.logger;
    this.quic = quic;
  }
}
