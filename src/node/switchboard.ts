/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * The `Switchboard` node of `@kaetemi/switchboard/node`: the core node
 * with the listeners and dialers of Node (and Bun and Deno through their
 * `node:` modules). Ports the `dart:io` half of `switchboard.dart`
 * (`listenTcp`, `listenWebSocket`, `acceptWebSocket`, the `tcp`, `ws` and
 * `wss` dialers), with QUIC and WebTransport through the QUIC binding.
 */

import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { networkInterfaces } from 'node:os';
import type { Duplex } from 'node:stream';
import type { MuxConnection } from '../core/mux/contract.ts';
import {
  type QuicConnector,
  type QuicListener,
  type QuicServerConfig,
  switchboardAlpn,
} from '../core/quic/contract.ts';
import type { QuicIdentityRules } from '../core/quic/quic-identity-rules.ts';
import type { QuicBindingOptions } from '../core/quic/quic-options.ts';
import {
  QuicSwitchboardBinding,
  type QuicSwitchboardBindingOptions,
  QuicSwitchboardListener,
} from '../core/quic/quic-switchboard.ts';
import type { ChannelPolicy } from '../core/switchboard/contract.ts';
import { coreRuntime } from '../core/switchboard/core-runtime.ts';
import { canonicalHost } from '../core/switchboard/endpoints.ts';
import type {
  SwitchboardRuntime,
  TransportDialer,
  TransportDialSettings,
} from '../core/switchboard/runtime.ts';
import {
  Switchboard as CoreSwitchboard,
  type SwitchboardOptions as CoreSwitchboardOptions,
} from '../core/switchboard/switchboard.ts';
import type { Clock } from '../core/util/clock.ts';
import type { Logger } from '../core/util/logger.ts';
import { nativeQuic, selfSignedCertificate } from './quic/native-provider.ts';
import { connectTcp, listenOn, listenTcp, uriHost } from './transport/tcp.ts';
import { connectWebSocket } from './transport/ws-client.ts';
import { requireWs } from './transport/ws-common.ts';
import {
  acceptWebSocket,
  handOverRequest,
  handOverUpgrade,
  isRequestFor,
  normalizeWebSocketPath,
  refuseUpgrade,
} from './transport/ws-server.ts';

/** A QUIC provider that can listen: the native provider (`nativeQuic()`) is one. */
export interface QuicListenProvider {
  /** Listens for raw QUIC connections on [host] and [port]; the endpoint is `quic://host:port`. */
  listen(
    host: string,
    port: number,
    config: QuicServerConfig,
    options?: { readonly advertisedHost?: string | undefined },
  ): Promise<QuicListener>;
  /** Listens for WebTransport sessions at `https://host:port/path`. */
  listenWebTransport(
    host: string,
    port: number,
    config: QuicServerConfig,
    options: {
      readonly path: string;
      readonly advertisedHost?: string | undefined;
      readonly allowedOrigins?: Iterable<string> | undefined;
      readonly protocols?: readonly string[] | undefined;
    },
  ): Promise<QuicListener>;
}

/** Options of the Node {@link Switchboard}: the core node's, and how it reaches QUIC. */
export interface SwitchboardOptions extends CoreSwitchboardOptions {
  /**
   * The QUIC provider the node dials `quic://` and `https://`
   * (WebTransport) endpoints through, with a `QuicSwitchboardBinding`; for
   * example `nativeQuic()`. Undefined: those schemes are not dialled
   * (`UNIMPLEMENTED`) until the node listens on QUIC or WebTransport.
   * {@link Switchboard.listenQuic} and {@link Switchboard.listenWebTransport}
   * listen with it when it can listen, and with `nativeQuic()` otherwise.
   */
  quic?: QuicConnector | undefined;
  /** The options of the QUIC binding the {@link quic} provider dials through. */
  quicBinding?: QuicSwitchboardBindingOptions | undefined;
}

/** What {@link Switchboard.listenTcp} takes. */
export interface ListenTcpOptions {
  /**
   * The listener policy of every channel arriving on a connection accepted
   * here; undefined allows everything, which is only safe for internal
   * listeners.
   */
  readonly policy?: ChannelPolicy | undefined;
}

/** What {@link Switchboard.listenWebSocket} takes. */
export interface ListenWebSocketOptions extends ListenTcpOptions {
  /** The path upgrades are accepted at, with or without a trailing slash; default `/`. */
  readonly path?: string | undefined;
  /**
   * Serves the HTTP requests for other paths (OAuth return routes,
   * `.well-known` files) instead of answering 403; it owns the response. A
   * handler that fails before answering makes it 500. {@link Switchboard.close}
   * waits for handlers in progress (at most `connectTimeout`).
   */
  readonly onOtherRequest?:
    | ((request: IncomingMessage, response: ServerResponse) => unknown)
    | undefined;
  /** Serves the upgrade requests for other paths instead of answering 403; it owns the socket. */
  readonly onOtherUpgrade?:
    | ((request: IncomingMessage, socket: Duplex, head: Uint8Array) => unknown)
    | undefined;
}

/** What {@link Switchboard.acceptWebSocket} takes. */
export interface AcceptWebSocketOptions extends ListenTcpOptions {
  /**
   * The largest incoming message, counted over its fragments before they
   * are buffered; default the node's `muxOptions.maxFrameSize` (1 MiB when
   * that is 0); 0 for none.
   */
  readonly maxFrameSize?: number | undefined;
}

/** A certificate and its private key, both DER: what `selfSignedCertificate()` returns. */
export interface ListenerCertificate {
  /** The certificate, DER, optionally followed by its chain. */
  readonly certificateDer: Uint8Array;
  /** The private key, DER (PKCS#8, SEC1 or PKCS#1). */
  readonly privateKeyDer: Uint8Array;
}

/** What {@link Switchboard.listenQuic} takes. Durations in milliseconds. */
export interface ListenQuicOptions extends ListenTcpOptions {
  /**
   * The certificate the listener presents. Default: a self-signed one for
   * the advertised host, generated by the native provider (node
   * certificates are self-signed; identity comes from `IDENT`).
   */
  readonly certificate?: ListenerCertificate | undefined;
  /** The host of the advertised endpoint (default: the host listened on). */
  readonly advertisedHost?: string | undefined;
  /** The provider to listen with; default the node's `quic` option when it can listen, else `nativeQuic()`. */
  readonly provider?: QuicListenProvider | undefined;
  /** The binding's options of every connection; their `identity` is replaced by {@link identityRules}. */
  readonly quicOptions?: Partial<QuicBindingOptions> | undefined;
  /** The identification rules of every connection; default `QuicIdentityRules.listener`. */
  readonly identityRules?: QuicIdentityRules | undefined;
  /** Whether the node identifies in turn to a peer whose `IDENT` verified; default true. */
  readonly identifyInReturn?: boolean | undefined;
  /** Streams a peer may have open at once; default 128. */
  readonly maxStreamsBidi?: number | undefined;
  /** Receive window of each stream; default 1 MiB. */
  readonly streamReceiveWindow?: number | undefined;
  /** Receive window of each connection; default 16 MiB. */
  readonly connectionReceiveWindow?: number | undefined;
  /** The QUIC idle timeout; default 30 000. */
  readonly idleTimeout?: number | undefined;
  /** The QUIC stack's own keep-alive; default 10 000. */
  readonly keepAlive?: number | undefined;
  /** Bound on a handshake; default 10 000. */
  readonly handshakeTimeout?: number | undefined;
}

/** What {@link Switchboard.listenWebTransport} takes. */
export interface ListenWebTransportOptions extends ListenQuicOptions {
  /** The path sessions are accepted on: absolute, without query or fragment. Default `/`. */
  readonly path?: string | undefined;
  /** The origins a browser's session may come from; undefined (the default) for any. */
  readonly allowedOrigins?: Iterable<string> | undefined;
}

/** Whether [address] is the wildcard of IPv4 or IPv6. */
function isWildcard(address: string): boolean {
  return address === '0.0.0.0' || address === '::';
}

/** Whether [provider] can listen (the native provider can, a browser's cannot). */
function canListen(provider: unknown): provider is QuicListenProvider {
  const p = provider as Partial<QuicListenProvider> | undefined;
  return typeof p?.listen === 'function' && typeof p.listenWebTransport === 'function';
}

/** The host and port of a `tcp://`, `ws://` or `wss://` endpoint, for the dialers. */
function hostAndPort(endpoint: string): { host: string; port: number } {
  const url = new URL(endpoint);
  const host = url.hostname.startsWith('[') ? url.hostname.slice(1, -1) : url.hostname;
  return { host, port: Number(url.port) };
}

/** What the node's dialers read from it once it exists. */
interface NodeContext {
  clock: Clock | undefined;
  logger: Logger | undefined;
}

/**
 * The runtime of the Node entry: the core's (the frame mux, in-process
 * pairs, the process-wide `mem://` listeners) with the `tcp`, `ws` and
 * `wss` dialers.
 */
function nodeRuntime(context: NodeContext): SwitchboardRuntime {
  const core = coreRuntime();
  const common = (settings: TransportDialSettings) => ({
    maxFrameSize: settings.maxFrameSize,
    timeout: settings.timeout,
    ...(settings.signal === undefined ? {} : { signal: settings.signal }),
    ...(context.clock === undefined ? {} : { clock: context.clock }),
    ...(context.logger === undefined ? {} : { logger: context.logger }),
  });
  const tcp: TransportDialer = (endpoint, settings) => {
    const { host, port } = hostAndPort(endpoint);
    return connectTcp(host, port, common(settings));
  };
  const ws: TransportDialer = (endpoint, settings) => connectWebSocket(endpoint, common(settings));
  return { ...core, dialers: { ...core.dialers, tcp, ws, wss: ws } };
}

/**
 * A mesh node on Node, Bun or Deno: the core `Switchboard` (dispatch,
 * policies, the connection pool, resolvers, identity, the proxy and the
 * relay) with listeners and dialers for every scheme:
 *
 * - `tcp://host:port`, the stream binding ({@link listenTcp});
 * - `ws://host:port/path`, the WebSocket binding over the `ws` package
 *   ({@link listenWebSocket}, {@link acceptWebSocket}); `wss://` is dialled
 *   (put TLS in front of a listener);
 * - `mem://id`, in-process ({@link listenMemory});
 * - `quic://host:port` and `https://host:port/path` (WebTransport), the
 *   QUIC binding: dialled through the `quic` option's provider (or, once
 *   the node listens on QUIC, the listener's), listened on with
 *   {@link listenQuic} and {@link listenWebTransport}.
 *
 * ```ts
 * const server = new Switchboard();
 * const endpoint = await server.listenTcp('127.0.0.1', 0); // 'tcp://127.0.0.1:41234'
 * const client = new Switchboard({ resolver: new EndpointResolver(endpoint) });
 * ```
 *
 * Listener options are objects; bound endpoints are returned as strings,
 * with the bound address and the actual port. A listener reachable by
 * untrusted peers must have a policy that refuses the reserved types
 * (`ChannelPolicies.denyReserved`).
 */
export class Switchboard extends CoreSwitchboard {
  readonly #quicBinding: QuicSwitchboardBindingOptions;
  #quic: QuicConnector | undefined;
  readonly #quicListeners: QuicSwitchboardListener[] = [];
  #closing: Promise<void> | undefined;

  /**
   * Creates a node. See {@link SwitchboardOptions}; a `runtime` of the
   * options replaces the Node runtime (its dialers among it).
   *
   * @throws {RangeError} as the core node does.
   */
  constructor(options: SwitchboardOptions = {}) {
    const context: NodeContext = { clock: undefined, logger: undefined };
    const { quic, quicBinding, ...rest } = options;
    const bindings = [...(options.bindings ?? [])];
    if (quic !== undefined) {
      bindings.push(new QuicSwitchboardBinding(quic, quicBinding ?? {}));
    }
    super({ ...rest, runtime: options.runtime ?? nodeRuntime(context), bindings });
    context.clock = this.clock;
    context.logger = this.logger;
    this.#quic = quic;
    this.#quicBinding = quicBinding ?? {};
  }

  /**
   * The QUIC provider the node dials `quic://` and `https://` endpoints
   * through: the `quic` option, or the provider a listener of
   * {@link listenQuic} or {@link listenWebTransport} took when there was
   * none; undefined before.
   */
  get quic(): QuicConnector | undefined {
    return this.#quic;
  }

  // Listening ---------------------------------------------------------------

  /**
   * Starts accepting TCP connections (the stream binding) on [address] (an
   * address, `0.0.0.0` or `::` for every interface; a name is resolved)
   * and [port] (0 picks a free one). Incoming frames are bounded by
   * `muxOptions.maxFrameSize`.
   *
   * Resolves with the bound `tcp://address:port` endpoint; when binding a
   * wildcard address, advertise a reachable host name instead. Rejects with
   * `SwitchboardError` FAILED_PRECONDITION after {@link close}, and with
   * the system's error when it cannot bind.
   */
  async listenTcp(address: string, port: number, options: ListenTcpOptions = {}): Promise<string> {
    this.checkOpen();
    const policy = options.policy;
    const listener = await listenTcp(address, port, {
      maxFrameSize: this.transportFrameLimit,
      clock: this.clock,
      logger: this.logger,
      onTransport: (transport, remote) => {
        this.adoptTransport(transport, { isInitiator: false, remote, policy });
      },
    });
    const hosts = await this.hostsOf(canonicalHost(listener.address));
    await this.addListener({
      uri: listener.endpoint,
      hosts,
      policy,
      stop: () => listener.close(),
    });
    return listener.endpoint;
  }

  /**
   * Starts accepting WebSocket connections on [address] and [port] (0
   * picks a free one) at `options.path`, over the `ws` package.
   *
   * Requests for the path, with or without a trailing slash, are upgraded
   * as {@link acceptWebSocket} does, every message bounded by
   * `muxOptions.maxFrameSize` (fragments included, before buffering) and
   * no compression negotiated; `switchboard` is selected when offered. A
   * client that offers only other subprotocols, or sends no valid key, gets
   * 400; a plain HTTP request for the path (or another WebSocket version)
   * gets 426; any other path gets 403, or goes to `onOtherRequest` and
   * `onOtherUpgrade`. Once {@link close} has started, upgrades are answered
   * 503.
   *
   * Resolves with the bound `ws://address:port/path` endpoint. Rejects
   * with `SwitchboardError` FAILED_PRECONDITION after {@link close},
   * UNSUPPORTED without the `ws` package, and the system's error when it
   * cannot bind.
   */
  async listenWebSocket(
    address: string,
    port: number,
    options: ListenWebSocketOptions = {},
  ): Promise<string> {
    this.checkOpen();
    await requireWs('listenWebSocket');
    const base = normalizeWebSocketPath(options.path ?? '/');
    const { policy, onOtherRequest, onOtherUpgrade } = options;
    const server = createServer();
    server.on('request', (request, response) => {
      if (isRequestFor(request, base)) {
        response.writeHead(426, { Upgrade: 'websocket', 'Content-Type': 'text/plain' });
        response.end('WebSocket upgrade required');
      } else if (onOtherRequest !== undefined) {
        this.track(handOverRequest(request, response, onOtherRequest, this.logger));
      } else {
        response.writeHead(403);
        response.end();
      }
    });
    server.on('upgrade', (request: IncomingMessage, socket: Duplex, head: Buffer) => {
      if (!isRequestFor(request, base)) {
        if (onOtherUpgrade !== undefined) {
          this.track(handOverUpgrade(request, socket, head, onOtherUpgrade, this.logger));
        } else {
          refuseUpgrade(socket, 403);
        }
        return;
      }
      const upgrade = this.#upgrade(request, socket, head, this.webSocketFrameLimit).then(
        ({ transport, remote }) => {
          this.adoptTransport(transport, { isInitiator: false, remote, policy });
        },
        (error: unknown) => {
          this.log('debug', `WebSocket request for ${request.url} failed: ${error}`);
        },
      );
      this.track(upgrade);
    });
    server.on('error', (error) => this.log('warn', `HTTP listener failed: ${error}`, error));
    let bound: { address: string; port: number };
    try {
      bound = await listenOn(server, address, port);
    } catch (error) {
      server.close();
      throw error;
    }
    const uri = `ws://${uriHost(bound.address)}:${bound.port}${base}`;
    const hosts = await this.hostsOf(canonicalHost(bound.address));
    await this.addListener({
      uri,
      hosts,
      policy,
      stop: async () => {
        // Stops listening; HTTP connections busy with a request are kept
        // until it is answered, idle ones are closed now.
        server.close();
        server.closeIdleConnections();
      },
      forceStop: async () => {
        server.closeAllConnections();
      },
    });
    return uri;
  }

  /**
   * Answers 503 once closing; otherwise upgrades [request] with
   * [maxFrameSize]. Rejects with FAILED_PRECONDITION when closing and
   * INVALID_ARGUMENT for a refused upgrade, both after answering it.
   */
  async #upgrade(
    request: IncomingMessage,
    socket: Duplex,
    head: Uint8Array,
    maxFrameSize: number,
  ): Promise<Awaited<ReturnType<typeof acceptWebSocket>>> {
    if (this.isClosing) {
      refuseUpgrade(socket, 503);
      throw this.closedError();
    }
    return acceptWebSocket(request, socket, head, {
      maxFrameSize,
      clock: this.clock,
      logger: this.logger,
    });
  }

  /**
   * Upgrades [request], an `upgrade` event of an application's own
   * `http.Server` with its [socket] and [head], to a WebSocket and
   * {@link accept}s it: the one-call form for an application that routes
   * its own server, for example to serve OAuth return routes or
   * `.well-known` files on the same port. The path is not checked.
   *
   * The upgrade is the one {@link listenWebSocket} performs, every
   * message bounded by `options.maxFrameSize`. `options.policy` is the
   * connection's listener policy, as for {@link accept}: **a connection
   * from untrusted peers must have one**. The peer's `address:port` is its
   * remote description.
   *
   * A request that is not a valid upgrade is answered 400 or 426 and the
   * call rejects with `SwitchboardError` INVALID_ARGUMENT. Once
   * {@link close} has started, the request is answered 503 and the call
   * rejects with FAILED_PRECONDITION; an upgrade in progress when it
   * starts completes and fails the same way, its connection sent GOAWAY at
   * once, and {@link close} waits for it. Without the `ws` package it
   * rejects with UNSUPPORTED (answered 501).
   *
   * @throws {RangeError} for a negative or fractional `maxFrameSize`,
   *   before touching the request.
   */
  acceptWebSocket(
    request: IncomingMessage,
    socket: Duplex,
    head: Uint8Array,
    options: AcceptWebSocketOptions = {},
  ): Promise<MuxConnection> {
    const max = options.maxFrameSize ?? this.webSocketFrameLimit;
    if (!Number.isInteger(max) || max < 0) {
      throw new RangeError(`maxFrameSize must be a non-negative integer, got ${max}`);
    }
    const accepted = this.#upgrade(request, socket, head, max).then(({ transport, remote }) =>
      this.accept(transport, { policy: options.policy, remote }),
    );
    this.track(accepted);
    return accepted;
  }

  /**
   * Starts accepting raw QUIC connections (ALPN `switchboard`) on
   * [address] and [port] (0 picks a free one), each run through the QUIC
   * binding and adopted under `options.policy`. The endpoint is declared as
   * one of the node's (`addListeningEndpoint`), and the node identifies in
   * return to peers whose exporter-bound `IDENT` verified
   * (`QuicSwitchboardListener`). Listens with `options.provider`, or the
   * `quic` option when it can listen, or `nativeQuic()`.
   *
   * Resolves with the endpoint, `quic://host:port` (the advertised host).
   * Rejects with `SwitchboardError` FAILED_PRECONDITION after
   * {@link close}, and as the provider does (a `QuicError`
   * 'unsupportedScheme' without the native addon).
   */
  async listenQuic(
    address: string,
    port: number,
    options: ListenQuicOptions = {},
  ): Promise<string> {
    this.checkOpen();
    const provider = this.#listenProvider(options.provider);
    const config = this.#serverConfig(address, options);
    const listener = await provider.listen(address, port, config, {
      advertisedHost: options.advertisedHost,
    });
    return this.#startQuic(provider, listener, address, options);
  }

  /**
   * Starts accepting WebTransport sessions at `https://host:port/path` on
   * [address] and [port] (UDP; 0 picks a free one), each run through the
   * QUIC binding as {@link listenQuic} does. The provider answers other
   * paths with 404, origins outside `options.allowedOrigins` with 403.
   *
   * Resolves with the endpoint, `https://host:port/path`. Rejects as
   * {@link listenQuic} does, and with a `TypeError` for a path that is not
   * absolute or has a query or fragment.
   */
  async listenWebTransport(
    address: string,
    port: number,
    options: ListenWebTransportOptions = {},
  ): Promise<string> {
    this.checkOpen();
    const provider = this.#listenProvider(options.provider);
    const config = this.#serverConfig(address, options);
    const listener = await provider.listenWebTransport(address, port, config, {
      path: options.path ?? '/',
      advertisedHost: options.advertisedHost,
      allowedOrigins: options.allowedOrigins,
      protocols: [switchboardAlpn],
    });
    return this.#startQuic(provider, listener, address, options);
  }

  #listenProvider(given: QuicListenProvider | undefined): QuicListenProvider {
    if (given !== undefined) {
      return given;
    }
    return canListen(this.#quic) ? this.#quic : nativeQuic();
  }

  /**
   * A node listening on QUIC without a provider to dial with dials through
   * the listener's from now on (when it can dial), so that the binding
   * normalises its endpoints (a WebTransport endpoint by its path among
   * them) and its own endpoints are recognised as such.
   */
  #adoptProvider(provider: QuicListenProvider): void {
    const connector = provider as Partial<QuicConnector>;
    const schemes = this.bindingSchemes;
    if (
      this.#quic !== undefined ||
      typeof connector.connect !== 'function' ||
      schemes.has(QuicSwitchboardBinding.quicScheme) ||
      schemes.has(QuicSwitchboardBinding.webTransportScheme)
    ) {
      return;
    }
    this.#quic = connector as QuicConnector;
    this.registerBinding(new QuicSwitchboardBinding(this.#quic, this.#quicBinding));
  }

  #serverConfig(address: string, options: ListenQuicOptions): QuicServerConfig {
    const certificate =
      options.certificate ?? selfSignedCertificate(options.advertisedHost ?? address);
    return {
      alpn: [switchboardAlpn],
      certificateDer: certificate.certificateDer,
      privateKeyDer: certificate.privateKeyDer,
      maxStreamsBidi: options.maxStreamsBidi,
      streamReceiveWindow: options.streamReceiveWindow,
      connectionReceiveWindow: options.connectionReceiveWindow,
      idleTimeout: options.idleTimeout,
      keepAlive: options.keepAlive,
      handshakeTimeout: options.handshakeTimeout,
    };
  }

  async #startQuic(
    provider: QuicListenProvider,
    listener: QuicListener,
    address: string,
    options: ListenQuicOptions,
  ): Promise<string> {
    let accepting: QuicSwitchboardListener;
    try {
      this.#adoptProvider(provider);
      accepting = await QuicSwitchboardListener.start(this, listener, {
        policy: options.policy,
        quicOptions: options.quicOptions,
        identityRules: options.identityRules,
        identifyInReturn: options.identifyInReturn,
        address: canonicalHost(address),
      });
    } catch (error) {
      await listener.close().catch(() => {});
      throw error;
    }
    if (this.isClosing) {
      await accepting.close();
      throw this.closedError();
    }
    this.#quicListeners.push(accepting);
    return accepting.endpoint;
  }

  /**
   * The host names that designate a listener bound to [address]: the
   * core's (the address, and the loopback names for loopback and wildcard
   * binds) and, for a wildcard bind, the addresses of the network
   * interfaces (IPv4 only for `0.0.0.0`), as Dart's node adds them.
   */
  protected override async hostsOf(address: string): Promise<Set<string>> {
    const hosts = await super.hostsOf(address);
    if (!isWildcard(address)) {
      return hosts;
    }
    try {
      for (const addresses of Object.values(networkInterfaces())) {
        for (const a of addresses ?? []) {
          if (address === '::' || a.family === 'IPv4') {
            hosts.add(canonicalHost(a.address));
          }
        }
      }
    } catch (error) {
      this.log('debug', `cannot list network interfaces: ${error}`);
    }
    return hosts;
  }

  /**
   * Shuts the node down as the core node does (stops listening, sends
   * GOAWAY on every connection and waits for them), and also stops the
   * QUIC and WebTransport listeners of {@link listenQuic} and
   * {@link listenWebTransport}. Never rejects.
   */
  override close(): Promise<void> {
    this.#closing ??= (async () => {
      const closing = super.close();
      await Promise.all(
        this.#quicListeners.splice(0).map((listener) =>
          listener.close().catch((error: unknown) => {
            this.log('debug', `closing ${listener} failed: ${error}`);
          }),
        ),
      );
      await closing;
    })();
    return this.#closing;
  }
}
