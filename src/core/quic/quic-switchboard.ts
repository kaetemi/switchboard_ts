/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * The QUIC and WebTransport binding inside a `Switchboard` node: the
 * {@link QuicSwitchboardBinding} a node dials `quic://` and `https://`
 * endpoints through, and the {@link QuicSwitchboardListener} that hands
 * the sessions of a provider's listener to a node. Ports
 * `quic_switchboard_binding.dart`.
 */

import type { PeerIdentity } from '../identity/contract.ts';
import type { MuxConnection } from '../mux/contract.ts';
import { StatusCode } from '../status.ts';
import type { ChannelPolicy } from '../switchboard/contract.ts';
import type { BindingDialSettings, SwitchboardBinding } from '../switchboard/runtime.ts';
import type { Switchboard } from '../switchboard/switchboard.ts';
import { parseEndpoint } from '../transport/endpoint.ts';
import type { LogLevel } from '../util/logger.ts';
import { quicLoggerName } from './binding-util.ts';
import type { QuicConnector, QuicListener, QuicSession } from './contract.ts';
import { QuicBindingConnection } from './quic-binding-connection.ts';
import { connectQuic, type QuicCertificateHash } from './quic-dial.ts';
import { QuicIdentityRules } from './quic-identity-rules.ts';
import { type QuicBindingOptions, quicBindingOptions } from './quic-options.ts';

/** How many connections {@link QuicSwitchboardBinding.dialled} and {@link QuicSwitchboardListener.accepted} keep. */
const kept = 64;

/** What a {@link QuicSwitchboardBinding} takes beside its connector. Durations in milliseconds. */
export interface QuicSwitchboardBindingOptions {
  /**
   * Whether the acceptor's certificate of a `quic` endpoint is verified
   * against the platform roots. Default false: node certificates are
   * self-signed, and identity comes from `IDENT`.
   */
  readonly verifyCertificate?: boolean | undefined;
  /**
   * SHA-256 hashes of the acceptable server certificates (a pin), which
   * authenticate the acceptor: 32 bytes each, or WebTransport's
   * `{ algorithm: 'sha-256', value }`.
   */
  readonly serverCertificateHashes?: readonly QuicCertificateHash[] | undefined;
  /** The binding's own options; their `identity` is replaced by the rules of each dial. */
  readonly quicOptions?: Partial<QuicBindingOptions> | undefined;
  /** The identification rules of every connection, instead of {@link QuicIdentityRules.initiator} computed per dial. */
  readonly identityRules?: QuicIdentityRules | undefined;
  /**
   * The QUIC idle timeout of the sessions the binding dials: one on which
   * nothing arrived for this long is lost (the stacks apply the shorter of
   * both ends' timeouts). 0: none of this side's. Default 30 000.
   */
  readonly idleTimeout?: number | undefined;
  /**
   * The interval of the QUIC stack's own keep-alive on the sessions the
   * binding dials: frames the stack sends while the connection is quiet,
   * so that a live path does not reach the idle timeout. They show that
   * the peer's stack answers, not that its application reads; the
   * binding's control `PING` is the node's `MuxOptions.keepAliveInterval`,
   * which this neither reads nor replaces. Default a third of
   * {@link idleTimeout}; 0: none.
   */
  readonly keepAlive?: number | undefined;
  /**
   * Whether the server certificate of an `https` endpoint is verified (the
   * platform roots, or the pin). Default true, as a browser does. A node
   * dialling another node's WebTransport listener, whose certificate is
   * self-signed, passes false (or a pin): the identification rules then
   * apply as they do over raw QUIC.
   */
  readonly webTransportCertificateVerified?: boolean | undefined;
}

/**
 * The {@link SwitchboardBinding} of the QUIC and WebTransport binding: a
 * node dials `quic://host:port` endpoints (raw QUIC, ALPN `switchboard`)
 * and `https://host:port/path` endpoints (WebTransport) through
 * [connector], and runs {@link QuicBindingConnection} over the sessions.
 *
 * ```ts
 * const node = new Switchboard({ bindings: [new QuicSwitchboardBinding(nativeQuic())] });
 * const channel = await node.openChannelAt('quic://edge.example:4433', address);
 * ```
 *
 * A connector that cannot dial a scheme fails with `unsupportedScheme`,
 * which the node reads as `UNIMPLEMENTED` and moves to a record's next
 * endpoint: the native provider dials both schemes, the browser's
 * WebTransport `https` only. A WebTransport server that answers with a
 * status other than 2xx (another path, an origin it refuses) fails the dial
 * with `UNAVAILABLE`.
 *
 * Between nodes the acceptor's certificate is self-signed and not verified
 * (`verifyCertificate` false, no pin), and the identification rules of the
 * specification apply where identity is in use
 * ({@link QuicIdentityRules.initiator}); a pin or a verified certificate
 * authenticates the acceptor and lifts them. `identityRules` overrides
 * them.
 *
 * A dial is bounded by the node's connect timeout
 * ({@link BindingDialSettings.timeout}), which is also the handshake
 * deadline of the session it asks for.
 */
export class QuicSwitchboardBinding implements SwitchboardBinding {
  /** The URI scheme of raw QUIC endpoints. */
  static readonly quicScheme = 'quic';

  /** The URI scheme of WebTransport endpoints. */
  static readonly webTransportScheme = 'https';

  /** The provider's connector. */
  readonly connector: QuicConnector;

  /** Whether the certificate of a `quic` endpoint is verified against the platform roots. */
  readonly verifyCertificate: boolean;

  /** The pinned certificate hashes. */
  readonly serverCertificateHashes: readonly QuicCertificateHash[];

  /** The binding's own options. */
  readonly quicOptions: Partial<QuicBindingOptions>;

  /** The identification rules of every connection, when given. */
  readonly identityRules: QuicIdentityRules | undefined;

  /** The QUIC idle timeout of the sessions dialled; 0: none. */
  readonly idleTimeout: number;

  /** The stack keep-alive of the sessions dialled; 0: none. */
  readonly keepAlive: number;

  /** Whether the certificate of an `https` endpoint is verified. */
  readonly webTransportCertificateVerified: boolean;

  readonly #dialled: QuicBindingConnection[] = [];

  /**
   * A binding dialling through [connector].
   *
   * @throws {RangeError} for a negative `idleTimeout` or `keepAlive`.
   */
  constructor(connector: QuicConnector, options: QuicSwitchboardBindingOptions = {}) {
    const idleTimeout = options.idleTimeout ?? 30_000;
    if (!(idleTimeout >= 0)) {
      throw new RangeError(`idleTimeout must not be negative: ${idleTimeout}`);
    }
    const keepAlive = options.keepAlive ?? Math.floor(idleTimeout / 3);
    if (!(keepAlive >= 0)) {
      throw new RangeError(`keepAlive must not be negative: ${keepAlive}`);
    }
    this.connector = connector;
    this.verifyCertificate = options.verifyCertificate ?? false;
    this.serverCertificateHashes = [...(options.serverCertificateHashes ?? [])];
    this.quicOptions = { ...options.quicOptions };
    this.identityRules = options.identityRules;
    this.idleTimeout = idleTimeout;
    this.keepAlive = keepAlive;
    this.webTransportCertificateVerified = options.webTransportCertificateVerified ?? true;
  }

  /** `quic` and `https`. */
  get schemes(): readonly string[] {
    return [QuicSwitchboardBinding.quicScheme, QuicSwitchboardBinding.webTransportScheme];
  }

  /** The most recent 64 connections dialled, for diagnostics and tests. */
  get dialled(): readonly QuicBindingConnection[] {
    return this.#dialled;
  }

  /**
   * Raw QUIC endpoints compare by scheme, host and port (as given);
   * WebTransport endpoints by scheme, host, port (443 by default) and path,
   * as the path names the listener.
   */
  normalise(endpoint: string): string {
    return parseEndpoint(endpoint).key;
  }

  /**
   * Dials [endpoint] with {@link connectQuic}, bounded by the settings'
   * timeout when it is positive: on expiry the dial fails with
   * `UNAVAILABLE`, and a session the provider still yields afterwards is
   * closed with `CANCELLED`, so that it never reaches {@link dialled}.
   */
  async dial(endpoint: string, settings: BindingDialSettings): Promise<MuxConnection> {
    const webTransport =
      parseEndpoint(endpoint).scheme === QuicSwitchboardBinding.webTransportScheme;
    const connection = await connectQuic(endpoint, this.connector, {
      options: settings.options,
      quicOptions: this.quicOptions,
      identityRules: this.identityRules,
      verifyCertificate: webTransport
        ? this.webTransportCertificateVerified
        : this.verifyCertificate,
      serverCertificateHashes: this.serverCertificateHashes,
      timeout: settings.timeout,
      idleTimeout: this.idleTimeout,
      keepAlive: this.keepAlive,
      localIdentity: settings.localIdentity,
      signal: settings.signal,
    });
    if (this.#dialled.length === kept) {
      this.#dialled.shift();
    }
    this.#dialled.push(connection);
    return connection;
  }

  toString(): string {
    return `QuicSwitchboardBinding(${this.connector})`;
  }
}

/** What {@link QuicSwitchboardListener.start} takes beside the node and the listener. */
export interface QuicSwitchboardListenerOptions {
  /**
   * The policy of the channels peers open; undefined allows everything. A
   * listener open to untrusted peers needs one that refuses the reserved
   * types.
   */
  readonly policy?: ChannelPolicy | undefined;
  /** The binding's options of every connection; their `identity` is replaced by {@link identityRules}. */
  readonly quicOptions?: Partial<QuicBindingOptions> | undefined;
  /**
   * The identification rules of every connection. Default
   * {@link QuicIdentityRules.listener}: a node listener's certificate is
   * self-signed. A listener that serves only browsers, which verify its
   * certificate, passes {@link QuicIdentityRules.none}.
   */
  readonly identityRules?: QuicIdentityRules | undefined;
  /**
   * Whether the node identifies in turn to a peer whose `IDENT` verified,
   * naming it. Default true.
   */
  readonly identifyInReturn?: boolean | undefined;
  /** Whether the listener's endpoint is declared to the node (`addListeningEndpoint`). Default true. */
  readonly declareEndpoint?: boolean | undefined;
  /**
   * The address the listener is bound to, for the node's matching of its
   * own endpoints; see `Switchboard.addListeningEndpoint`.
   */
  readonly address?: string | undefined;
}

/**
 * Accepts the sessions of a provider's {@link QuicListener} into a
 * `Switchboard` node: each one runs {@link QuicBindingConnection} and is
 * handed to `Switchboard.adopt` under the policy; the listener's endpoint
 * is declared with `Switchboard.addListeningEndpoint`, so that the node
 * lists it and knows it as its own.
 *
 * ```ts
 * const { certificateDer, privateKeyDer } = await selfSignedCertificate('node-1');
 * const listener = await QuicSwitchboardListener.start(
 *   node,
 *   await nativeQuic().listen('0.0.0.0', 4433, {
 *     alpn: [switchboardAlpn],
 *     certificateDer,
 *     privateKeyDer,
 *   }),
 *   { policy: ChannelPolicies.denyReserved },
 * );
 * ```
 *
 * A node listener's certificate is self-signed, so its clients cannot
 * verify it, and the identification rules of the specification apply by
 * default ({@link QuicIdentityRules.listener}): nonce-bound `IDENT`s are
 * refused, and with `identifyInReturn` (the default) a node with a
 * credential identifies in turn, naming the peer, once the peer's own
 * exporter-bound `IDENT` has verified, and to nobody else.
 */
export class QuicSwitchboardListener implements AsyncDisposable {
  /** The node the connections join. */
  readonly node: Switchboard;

  /** The provider's listener. */
  readonly listener: QuicListener;

  /** The policy of the channels peers open. */
  readonly policy: ChannelPolicy | undefined;

  /** The binding's options of every connection, their identity rules included. */
  readonly quicOptions: QuicBindingOptions;

  /** Whether the node identifies in turn to a peer whose `IDENT` verified. */
  readonly identifyInReturn: boolean;

  readonly #accepted: QuicBindingConnection[] = [];
  readonly #sessions: AsyncIterator<QuicSession>;
  readonly #declared: boolean;
  #closed = false;
  #closing: Promise<void> | undefined;

  private constructor(
    node: Switchboard,
    listener: QuicListener,
    options: QuicSwitchboardListenerOptions,
    declared: boolean,
  ) {
    this.node = node;
    this.listener = listener;
    this.policy = options.policy;
    this.quicOptions = quicBindingOptions({
      ...options.quicOptions,
      identity: options.identityRules ?? QuicIdentityRules.listener,
    });
    this.identifyInReturn = options.identifyInReturn ?? true;
    this.#declared = declared;
    this.#sessions = listener.sessions[Symbol.asyncIterator]();
  }

  /**
   * Starts accepting [listener]'s sessions into [node]. With
   * `declareEndpoint` (the default), the listener's endpoint is declared
   * to the node first.
   *
   * Rejects as `Switchboard.addListeningEndpoint` does; the provider's
   * listener is then left open.
   */
  static async start(
    node: Switchboard,
    listener: QuicListener,
    options: QuicSwitchboardListenerOptions = {},
  ): Promise<QuicSwitchboardListener> {
    const declare = options.declareEndpoint ?? true;
    if (declare) {
      await node.addListeningEndpoint(listener.endpoint, {
        policy: options.policy,
        address: options.address,
      });
    }
    const accepting = new QuicSwitchboardListener(node, listener, options, declare);
    void accepting.#acceptLoop();
    return accepting;
  }

  /** The endpoint peers dial. */
  get endpoint(): string {
    return this.listener.endpoint;
  }

  /** The most recent 64 connections accepted, for diagnostics and tests. */
  get accepted(): readonly QuicBindingConnection[] {
    return this.#accepted;
  }

  /** Whether {@link close} was called. */
  get isClosed(): boolean {
    return this.#closed;
  }

  get #isWebTransport(): boolean {
    return this.listener.endpoint.toLowerCase().startsWith('https:');
  }

  async #acceptLoop(): Promise<void> {
    try {
      for (;;) {
        const next = await this.#sessions.next();
        if (next.done === true) {
          return;
        }
        void this.#accept(next.value);
      }
    } catch (error) {
      if (!this.#closed) {
        this.#log('debug', `listener ${this.endpoint} failed`, error);
      }
    }
  }

  async #accept(session: QuicSession): Promise<void> {
    if (this.#closed) {
      session.close(StatusCode.goingAway, 'not accepting').catch(() => {});
      return;
    }
    let connection: QuicBindingConnection;
    try {
      connection = await QuicBindingConnection.start(session, {
        options: this.node.muxOptions,
        quicOptions: this.quicOptions,
        isWebTransport: this.#isWebTransport,
        localIdentity: this.node.credential?.identity,
      });
    } catch (error) {
      this.#log('warn', `cannot start a connection from ${session.remote}`, error);
      session.close(StatusCode.internal, 'internal error').catch(() => {});
      return;
    }
    if (this.#closed) {
      await connection.close();
      return;
    }
    if (this.#accepted.length === kept) {
      this.#accepted.shift();
    }
    this.#accepted.push(connection);
    try {
      await this.node.adopt(connection, { policy: this.policy, remote: session.remote });
    } catch (error) {
      this.#log('debug', `connection from ${session.remote} not adopted`, error);
      await connection.close();
      return;
    }
    if (this.identifyInReturn && this.node.credential !== undefined) {
      connection.peerIdentified.then(
        (identity) => this.#identifyInReturn(connection, identity),
        () => {},
      );
    }
  }

  /**
   * Identifies to [connection]'s peer, naming it, once its `IDENT`
   * verified: that verification is what asks this side to. Under
   * {@link QuicIdentityRules.identifyOnlyToIdentifiedPeer} (the listener's
   * default) only once its exporter-bound `IDENT` verified.
   */
  async #identifyInReturn(
    connection: QuicBindingConnection,
    identity: PeerIdentity,
  ): Promise<void> {
    const rules = connection.quicOptions.identity;
    if (
      !connection.isOpen ||
      (rules.identifyOnlyToIdentifiedPeer && !connection.peerAuthenticated) ||
      this.node.credential === undefined
    ) {
      return;
    }
    try {
      await this.node.identifyOn(connection, { receiver: identity.identity });
    } catch (error) {
      this.#log(
        'info',
        `identifying to "${identity.identity}" at ${connection.session.remote} failed`,
        error,
      );
    }
  }

  /**
   * Stops accepting sessions, withdraws the endpoint from the node and
   * closes the provider's listener; the connections accepted stay the
   * node's. Idempotent.
   */
  close(): Promise<void> {
    this.#closing ??= this.#close();
    return this.#closing;
  }

  async #close(): Promise<void> {
    this.#closed = true;
    void this.#sessions.return?.()?.catch(() => {});
    if (this.#declared) {
      this.node.removeListeningEndpoint(this.listener.endpoint);
    }
    try {
      await this.listener.close();
    } catch (error) {
      this.#log('debug', `closing listener ${this.endpoint}`, error);
    }
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }

  #log(level: LogLevel, message: string, error?: unknown): void {
    this.node.muxOptions.logger.log(level, quicLoggerName, message, error);
  }

  toString(): string {
    return `QuicSwitchboardListener(${this.endpoint}${this.#closed ? ', closed' : ''})`;
  }
}
