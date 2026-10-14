/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * {@link connectTransport}: a frame transport to an endpoint URI of a
 * scheme the frame mux runs over, on Node.
 */

import { StatusCode, SwitchboardError } from '../../core/status.ts';
import type { FrameTransport } from '../../core/transport/contract.ts';
import { parseEndpoint } from '../../core/transport/endpoint.ts';
import { MemoryEndpoints } from '../../core/transport/memory-endpoints.ts';
import { memoryTransportPair } from '../../core/transport/memory-transport.ts';
import type { Clock } from '../../core/util/clock.ts';
import type { Logger } from '../../core/util/logger.ts';
import { connectTcp } from './tcp.ts';
import { connectWebSocket } from './ws-client.ts';

/** The `mem://` listeners {@link connectTransport} looks up. */
export interface MemoryListenerLookup {
  /** The acceptor listening at [id], or undefined. */
  lookup(id: string): ((transport: FrameTransport, remote: string) => void) | undefined;
}

/** Options of {@link connectTransport}. Durations in milliseconds. */
export interface TransportConnectOptions {
  /**
   * Largest accepted incoming frame: the stream binding's limit for `tcp`
   * (0: none), the message limit for `ws` and `wss` (0: none). Default
   * 1 MiB; should agree with `MuxOptions.maxFrameSize`.
   */
  readonly maxFrameSize?: number | undefined;
  /** Bound on establishing the connection; 0 or undefined: none. */
  readonly timeout?: number | undefined;
  /** Abandons the attempt (it rejects with `signal.reason`). */
  readonly signal?: AbortSignal | undefined;
  /** The in-process listeners of `mem` endpoints; default the process-wide `MemoryEndpoints`. */
  readonly memoryEndpoints?: MemoryListenerLookup | undefined;
  /** How the in-process listener sees this side of a `mem` connection; default `memory peer`. */
  readonly memoryRemote?: string | undefined;
  /** Source of the transports' timers. */
  readonly clock?: Clock | undefined;
  /** Receives the transports' diagnostics. */
  readonly logger?: Logger | undefined;
}

/**
 * Connects to [endpoint] and resolves with a transport to run the frame mux
 * over as the initiator:
 *
 * - `tcp://host:port`: the stream binding ({@link connectTcp});
 * - `ws://` and `wss://host:port/path`: the WebSocket binding
 *   (`connectWebSocket`, over `ws` when installed); a fragment is ignored;
 * - `mem://id`: one side of an in-process pair whose other side the
 *   listener at that id accepts; frames go through the event queue, as
 *   from a socket.
 *
 * Rejects with a `SwitchboardError`: `INVALID_ARGUMENT` for a URI that is
 * not one, or a `tcp` endpoint without a host and a port, or a `mem`
 * endpoint without an id; `UNAVAILABLE` when the endpoint cannot be
 * reached (no listener at a `mem` id among them) or not within
 * [options.timeout]; `UNSUPPORTED` for another scheme: `quic` and `https`
 * (WebTransport) are the QUIC binding's, not a frame transport. Dart's
 * node refuses an unknown scheme with `UNIMPLEMENTED`; the design reserves
 * that for what is not written yet and answers `UNSUPPORTED` for what a
 * runtime cannot serve.
 */
export async function connectTransport(
  endpoint: string | URL,
  options: TransportConnectOptions = {},
): Promise<FrameTransport> {
  let parsed: ReturnType<typeof parseEndpoint>;
  try {
    parsed = parseEndpoint(endpoint);
  } catch (cause) {
    throw SwitchboardError.of(StatusCode.invalidArgument, `not an endpoint URI: ${endpoint}`, {
      cause,
    });
  }
  const common = {
    ...(options.maxFrameSize === undefined ? {} : { maxFrameSize: options.maxFrameSize }),
    ...(options.timeout === undefined ? {} : { timeout: options.timeout }),
    ...(options.signal === undefined ? {} : { signal: options.signal }),
    ...(options.clock === undefined ? {} : { clock: options.clock }),
    ...(options.logger === undefined ? {} : { logger: options.logger }),
  };
  switch (parsed.scheme) {
    case 'tcp': {
      const url = new URL(parsed.href);
      if (parsed.host === '' || url.port === '') {
        throw SwitchboardError.of(
          StatusCode.invalidArgument,
          `tcp endpoint needs a host and a port: ${parsed.href}`,
        );
      }
      return connectTcp(parsed.host, parsed.port, common);
    }
    case 'ws':
    case 'wss':
      return connectWebSocket(parsed.href, common);
    case 'mem': {
      if (parsed.host === '') {
        throw SwitchboardError.of(
          StatusCode.invalidArgument,
          `mem endpoint needs an id: ${parsed.href}`,
        );
      }
      if (options.signal?.aborted) {
        throw options.signal.reason;
      }
      const acceptor = (options.memoryEndpoints ?? MemoryEndpoints).lookup(parsed.host);
      if (acceptor === undefined) {
        throw SwitchboardError.of(
          StatusCode.unavailable,
          `cannot connect to ${parsed.key}: no memory listener with that id`,
        );
      }
      const [local, remote] = memoryTransportPair({
        delay: 0,
        ...(options.clock === undefined ? {} : { clock: options.clock }),
        ...(options.logger === undefined ? {} : { logger: options.logger }),
        remotes: [parsed.key, options.memoryRemote ?? 'memory peer'],
      });
      acceptor(remote, options.memoryRemote ?? 'memory peer');
      return local;
    }
    default:
      throw SwitchboardError.of(
        StatusCode.unsupported,
        `cannot connect to ${parsed.href}: ${parsed.scheme} is not a frame transport scheme`,
      );
  }
}
