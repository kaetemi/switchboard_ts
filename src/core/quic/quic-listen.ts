/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import type { MuxOptions } from '../mux/contract.ts';
import { muxOptions } from '../mux/contract.ts';
import { StatusCode } from '../status.ts';
import { AsyncQueue } from '../util/async-queue.ts';
import type { LogLevel } from '../util/logger.ts';
import { checkQuicBindingOptions, quicLoggerName } from './binding-util.ts';
import type { QuicListener, QuicSession } from './contract.ts';
import { QuicBindingConnection } from './quic-binding-connection.ts';
import { QuicIdentityRules } from './quic-identity-rules.ts';
import type { QuicBindingOptions } from './quic-options.ts';
import { quicBindingOptions } from './quic-options.ts';

/** How {@link listenQuic} starts the connections it accepts. */
export interface QuicListenOptions {
  /** The options of every connection, completed with the defaults. */
  readonly options?: Partial<MuxOptions> | undefined;
  /** The binding's own options; their `identity` is replaced by {@link identityRules}. */
  readonly quicOptions?: Partial<QuicBindingOptions> | undefined;
  /**
   * The identification rules of every connection. Default
   * {@link QuicIdentityRules.listener}: a node listener's certificate is
   * self-signed, so its clients cannot verify it. A listener that serves
   * only browsers, which verify its certificate, passes
   * {@link QuicIdentityRules.none}.
   */
  readonly identityRules?: QuicIdentityRules | undefined;
  /** This side's identity, set on every connection before anything from the peer is handled. */
  readonly localIdentity?: string | undefined;
}

/** How many accepted connections {@link QuicBindingListener.accepted} keeps. */
const acceptedKept = 64;

/**
 * The sessions of a {@link QuicListener}, each one started as a
 * {@link QuicBindingConnection} (the acceptor's side) and delivered through
 * {@link connections}. The accepting half of `QuicSwitchboardListener` of
 * `quic_switchboard_binding.dart`; a node adopts what it yields, and
 * identifies in return where its rules allow:
 *
 * ```ts
 * await using quic = listenQuic(await provider.listen('quic://0.0.0.0:4433', server));
 * for await (const connection of quic.connections) node.adopt(connection);
 * ```
 *
 * Under {@link QuicIdentityRules.identifyOnlyToIdentifiedPeer} (the
 * listener's default) a node identifies to a peer only once the peer's own
 * exporter-bound `IDENT` verified: after {@link QuicBindingConnection.peerIdentified},
 * when {@link QuicBindingConnection.peerAuthenticated} holds.
 */
export class QuicBindingListener implements AsyncDisposable {
  /** The provider's listener. */
  readonly listener: QuicListener;

  /** Whether the listener accepts WebTransport sessions (an `https` endpoint) rather than raw QUIC. */
  readonly isWebTransport: boolean;

  /** The options of every connection. */
  readonly options: MuxOptions;

  /** The binding's options of every connection, their identity rules included. */
  readonly quicOptions: QuicBindingOptions;

  /** This side's identity on every connection. */
  readonly localIdentity: string | undefined;

  readonly #queue = new AsyncQueue<QuicBindingConnection>();
  readonly #accepted: QuicBindingConnection[] = [];
  readonly #starting = new Set<Promise<void>>();
  readonly #sessions: AsyncIterator<QuicSession>;
  #closed = false;
  #cancelled = false;
  #closing: Promise<void> | undefined;

  /**
   * Use {@link listenQuic}.
   *
   * @throws {RangeError} for options the binding refuses (as
   * {@link QuicBindingConnection.start} does), before [listener] is used; it
   * is left open.
   */
  constructor(listener: QuicListener, init: QuicListenOptions = {}) {
    this.listener = listener;
    this.isWebTransport = listener.endpoint.toLowerCase().startsWith('https:');
    this.options = muxOptions(init.options);
    this.quicOptions = quicBindingOptions({
      ...init.quicOptions,
      identity: init.identityRules ?? QuicIdentityRules.listener,
    });
    checkQuicBindingOptions(this.options, this.quicOptions);
    this.localIdentity = init.localIdentity;
    this.#queue.onCancel = () => {
      this.#cancelled = true;
      void this.close();
    };
    this.#sessions = listener.sessions[Symbol.asyncIterator]();
    void this.#acceptLoop();
  }

  /** The endpoint peers dial. */
  get endpoint(): string {
    return this.listener.endpoint;
  }

  /**
   * The connections accepted and started, each open when delivered. One
   * consumer; leaving the iteration closes the listener, and connections
   * started but not taken are closed. Ends once the listener closed.
   */
  get connections(): AsyncIterable<QuicBindingConnection> {
    return this.#queue;
  }

  /** The most recent 64 connections accepted, for diagnostics and tests. */
  get accepted(): readonly QuicBindingConnection[] {
    return this.#accepted;
  }

  /** Whether {@link close} was called. */
  get isClosed(): boolean {
    return this.#closed;
  }

  async #acceptLoop(): Promise<void> {
    try {
      for (;;) {
        const next = await this.#sessions.next();
        if (next.done === true) {
          break;
        }
        const session = next.value;
        if (this.#closed) {
          this.#refuse(session);
          continue;
        }
        const starting = this.#accept(session);
        this.#starting.add(starting);
        void starting.finally(() => this.#starting.delete(starting));
      }
    } catch (error) {
      if (!this.#closed) {
        this.#log('info', `listener ${this.endpoint} failed`, error);
      }
    }
    // The provider's listener stopped on its own: nothing more arrives.
    if (!this.#closed) {
      void this.close();
    }
  }

  #refuse(session: QuicSession): void {
    session.close(StatusCode.goingAway, 'not accepting').catch(() => {});
  }

  async #accept(session: QuicSession): Promise<void> {
    let connection: QuicBindingConnection;
    try {
      connection = await QuicBindingConnection.start(session, {
        options: this.options,
        quicOptions: this.quicOptions,
        isWebTransport: this.isWebTransport,
        localIdentity: this.localIdentity,
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
    if (!connection.isOpen) {
      // Ended while it started: the session, or the identification rules.
      return;
    }
    if (this.#accepted.length === acceptedKept) {
      this.#accepted.shift();
    }
    this.#accepted.push(connection);
    this.#queue.push(connection);
  }

  /**
   * Stops accepting sessions and closes the provider's listener. The
   * connections already delivered stay their consumer's; those started and
   * not yet taken stay in {@link connections} until it ends, unless the
   * consumer left it. Idempotent.
   */
  close(): Promise<void> {
    this.#closing ??= this.#close();
    return this.#closing;
  }

  async #close(): Promise<void> {
    this.#closed = true;
    void this.#sessions.return?.()?.catch(() => {});
    try {
      await this.listener.close();
    } catch (error) {
      this.#log('debug', `closing listener ${this.endpoint}`, error);
    }
    await Promise.allSettled([...this.#starting]);
    if (this.#cancelled) {
      // The consumer left: nobody takes what is buffered.
      await Promise.allSettled(this.#queue.drain().map((connection) => connection.close()));
    }
    this.#queue.end();
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }

  #log(level: LogLevel, message: string, error?: unknown): void {
    this.options.logger.log(level, quicLoggerName, message, error);
  }

  toString(): string {
    return `QuicBindingListener(${this.endpoint}${this.#closed ? ', closed' : ''})`;
  }
}

/**
 * Accepts the sessions of [listener], a provider's listener, as connections
 * of the binding; see {@link QuicBindingListener}.
 *
 * @throws {RangeError} for options the binding refuses, before [listener]
 * is used; it is left open.
 */
export function listenQuic(
  listener: QuicListener,
  options?: QuicListenOptions,
): QuicBindingListener {
  return new QuicBindingListener(listener, options);
}
