/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import type { MuxConnection, MuxOptions } from '../mux/contract.ts';
import { StatusCode, SwitchboardError } from '../status.ts';
import type { FrameTransport } from '../transport/contract.ts';
import type { MemoryAcceptor } from '../transport/memory-endpoints.ts';

/**
 * What a node needs from the platform it runs on. The core node
 * (`Switchboard` in this directory) is runtime neutral: it runs the frame
 * mux over the transports it is given, keeps in-process (`mem://`)
 * endpoints, and dials further schemes through the dialers and bindings of
 * its runtime. The entry points supply it: `@kaetemi/switchboard/node`
 * adds TCP and the `ws` package, `@kaetemi/switchboard/web` the browser's
 * WebSocket and WebTransport.
 */

/** Runs the frame mux over [transport]: a connection the node accepted or dialled. */
export type MuxFactory = (
  transport: FrameTransport,
  options: MuxOptions,
  isInitiator: boolean,
) => MuxConnection;

/**
 * The registry of in-process listeners (`mem://<id>`) that every node using
 * it shares: a node listening with `listenMemory` registers there, and
 * `connect('mem://<id>')` on any node of the process reaches it. Ids are
 * compared in lower case. The process-wide `MemoryEndpoints` is one; a
 * test may give its nodes a registry of their own.
 */
export interface MemoryEndpointRegistry {
  /**
   * Registers [acceptor] under `options.name` (lower-cased), or under a new
   * id when no name is given, and returns its `mem://<id>` URI. Throws
   * `RangeError` for an invalid name and `SwitchboardError` ALREADY_EXISTS
   * for one in use.
   */
  register(acceptor: MemoryAcceptor, options?: { readonly name?: string }): string;
  /** The acceptor at [id], or undefined. */
  lookup(id: string): MemoryAcceptor | undefined;
  /** Removes the listener at [id], if any. */
  unregister(id: string): void;
}

/** What a transport dialer is given. */
export interface TransportDialSettings {
  /** The largest incoming frame the transport accepts (the node's frame limit). */
  readonly maxFrameSize: number;
  /** Bound on the attempt in milliseconds; 0 for none. The node also gives up then. */
  readonly timeout: number;
  readonly signal: AbortSignal | undefined;
}

/**
 * Dials [endpoint] (a URI of one of the schemes it is registered for) and
 * resolves with a transport the node runs the frame mux over as the
 * initiator. Rejects with a `SwitchboardError` (UNAVAILABLE when the
 * endpoint cannot be reached, INVALID_ARGUMENT for an unusable URI); the
 * node reports anything else as UNAVAILABLE.
 */
export type TransportDialer = (
  endpoint: string,
  settings: TransportDialSettings,
) => Promise<FrameTransport>;

/** The platform services of a node. */
export interface SwitchboardRuntime {
  /** The frame mux. */
  readonly mux: MuxFactory;
  /** An in-process transport pair whose frames go through the event queue, as from a socket. */
  readonly transportPair: () => readonly [FrameTransport, FrameTransport];
  /** The in-process listeners. */
  readonly memoryEndpoints: MemoryEndpointRegistry;
  /** Transport dialers by lower-case scheme (`ws`, `wss`, `tcp`). */
  readonly dialers: Readonly<Record<string, TransportDialer>>;
}

/** What a {@link SwitchboardBinding} is given to dial with. */
export interface BindingDialSettings {
  /** The node's mux options (its identity verifier among them). */
  readonly options: MuxOptions;
  /** The node's connect timeout in milliseconds; 0 for none. The node also gives up then and closes a late connection. */
  readonly timeout: number;
  /** The node's identity, for the connection's `localIdentity`; undefined without a credential. */
  readonly localIdentity: string | undefined;
  readonly signal: AbortSignal | undefined;
}

/**
 * A way for a node to reach the endpoints of further URI schemes through a
 * binding of the connection contract other than the frame mux, such as the
 * QUIC binding (`quic://`, and WebTransport at `https://`). The node then
 * treats the connection exactly like one it dialled itself: pooled by
 * normalised endpoint, its policy chosen as for any initiated connection,
 * identified on, dispatched, and sent GOAWAY by `close()`.
 */
export interface SwitchboardBinding {
  /** The lower-case schemes this binding dials. Read once, at registration. */
  readonly schemes: readonly string[];
  /**
   * Establishes a new connection to [endpoint] as its initiator, configured
   * with [settings], and resolves once it can open channels. The node owns
   * it from then on (it reads `incoming`).
   */
  dial(endpoint: string, settings: BindingDialSettings): Promise<MuxConnection>;
  /**
   * [endpoint] as the pool tells endpoints apart: two spellings of one
   * endpoint give the same string. Default: scheme and host lower-cased and
   * the port if given, without path, query or fragment; a binding whose
   * endpoints differ by path (WebTransport) keeps it.
   */
  normalise?(endpoint: string): string;
}

/** A runtime member that is not available: every use fails with UNIMPLEMENTED. */
export function unavailable(what: string): never {
  throw SwitchboardError.of(StatusCode.unimplemented, `${what} is not available in this runtime`);
}
