/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import type { ChannelAddress } from '../address/index.ts';
import type { MuxChannel, MuxConnection } from '../mux/contract.ts';
import { Status, StatusCode, SwitchboardError } from '../status.ts';
import { Broadcast } from '../switchboard/broadcast.ts';
import type { TalkChannel } from '../talk/contract.ts';
import type { Clock, TimerHandle } from '../util/clock.ts';
import { Deferred } from '../util/deferred.ts';
import type { LogLevel } from '../util/logger.ts';

/**
 * Close statuses after which a persistent channel is not re-opened: the
 * endpoint will refuse it again until the application changes something
 * (its credential, the address).
 */
function isTerminal(status: Status): boolean {
  switch (status.code) {
    case StatusCode.permissionDenied:
    case StatusCode.unauthenticated:
    case StatusCode.unimplemented:
    case StatusCode.notFound:
      return true;
    default:
      return false;
  }
}

/** What a persistent channel needs from its client. Internal. */
export interface PersistentHost {
  readonly clock: Clock;
  /** The lifetime beyond which a channel resets its backoff. */
  readonly maxBackoff: number;
  /** The OPEN payload of [address], with the client's default payload when it has none. */
  encode(address: ChannelAddress): Uint8Array;
  /** The delay after [failures] consecutive failures. */
  backoff(failures: number): number;
  /** The current connection, usable or not. */
  readonly connection: MuxConnection | undefined;
  /** The current connection if new channels may be opened on it. */
  readonly usableConnection: MuxConnection | undefined;
  /** Forgets a persistent channel that ended for good. */
  remove(entry: PersistentEntry): void;
  log(level: LogLevel, message: string, error?: unknown): void;
}

/** What the client drives of each persistent channel, whatever it delivers. Internal. */
export interface PersistentEntry {
  connectionUp(connection: MuxConnection): void;
  retire(connection: MuxConnection, status: Status): void;
  close(): Promise<void>;
}

/** How a {@link PersistentCore} turns mux channels into what the application receives. */
export interface PersistentKind<T extends object> {
  /** For logs: `PersistentChannel` or `PersistentTalk`. */
  readonly name: string;
  readonly wrap: (channel: MuxChannel) => T;
  readonly close: (value: T, status: Status) => Promise<void>;
  readonly doneOf: (value: T) => Promise<Status>;
}

/**
 * The re-opening logic shared by {@link PersistentChannel} and
 * {@link PersistentTalk}; [T] is what the application receives for each
 * mux channel. Internal.
 */
export class PersistentCore<T extends object> implements PersistentEntry {
  readonly address: ChannelAddress;
  /** Each newly opened value, after `onOpen` had it. */
  readonly opened = new Broadcast<T>();
  readonly #done = new Deferred<Status>();
  readonly #host: PersistentHost;
  readonly #kind: PersistentKind<T>;
  readonly #onOpen: (value: T) => unknown;
  readonly #onClosed: ((status: Status) => void) | undefined;

  /** The mux channel under {@link current}. */
  #channel: MuxChannel | undefined;
  current: T | undefined;

  /** Will not be opened again: closed by the application, or finished. */
  closing = false;
  #finished = false;
  #failures = 0;
  #livedLong = false;
  #retryTimer: TimerHandle | undefined;
  #lifetimeTimer: TimerHandle | undefined;

  constructor(
    host: PersistentHost,
    address: ChannelAddress,
    kind: PersistentKind<T>,
    onOpen: (value: T) => unknown,
    onClosed: ((status: Status) => void) | undefined,
  ) {
    this.#host = host;
    this.address = address;
    this.#kind = kind;
    this.#onOpen = onOpen;
    this.#onClosed = onClosed;
  }

  /** Resolves with the final status once the persistent channel ended for good. */
  get done(): Promise<Status> {
    return this.#done.promise;
  }

  /** A connection became usable: opens now unless already open. */
  connectionUp(connection: MuxConnection): void {
    if (this.closing || this.#channel !== undefined) {
      return;
    }
    this.#openOn(connection);
  }

  /**
   * The client leaves [connection] (it stopped, the endpoint sent GOAWAY,
   * or a forced reconnect): closes the channel on it with [status]; it is
   * opened again on the next connection.
   */
  retire(connection: MuxConnection, status: Status): void {
    this.#cancelRetry();
    const value = this.current;
    if (value === undefined || this.#channel?.connection !== connection) {
      return;
    }
    this.#closeQuietly(value, status);
  }

  close(): Promise<void> {
    if (!this.closing) {
      this.closing = true;
      this.#cancelRetry();
      const value = this.current;
      if (value === undefined) {
        this.#finish(Status.ok);
      } else {
        // The end of the channel finishes.
        this.#closeQuietly(value, Status.ok);
      }
    }
    return this.#done.promise.then(() => {});
  }

  #openOn(connection: MuxConnection): void {
    this.#cancelRetry();
    let channel: MuxChannel;
    try {
      channel = connection.open(this.#host.encode(this.address));
    } catch (e) {
      const status = statusOf(e);
      switch (status.code) {
        case StatusCode.failedPrecondition:
          this.#host.log('debug', `${this}: connection going away, waiting for the next`);
          break;
        case StatusCode.resourceExhausted:
          this.#host.log('info', `${this}: cannot open: ${status}`);
          this.#scheduleRetry();
          break;
        default:
          this.#host.log('warn', `${this}: cannot open: ${status}`, e);
          this.#finish(status);
          this.#callOnClosed(status);
      }
      return;
    }
    let value: T;
    try {
      value = this.#kind.wrap(channel);
    } catch (e) {
      const status = statusOf(e);
      this.#host.log('warn', `${this}: cannot wrap the channel: ${status}`, e);
      void channel.close(status).catch(() => {});
      this.#finish(status);
      this.#callOnClosed(status);
      return;
    }
    this.#channel = channel;
    this.current = value;
    this.#livedLong = false;
    const clock = this.#host.clock;
    this.#lifetimeTimer = clock.setTimeout(() => {
      this.#lifetimeTimer = undefined;
      this.#livedLong = true;
    }, this.#host.maxBackoff);
    this.#kind
      .doneOf(value)
      .then(
        (status) => this.#onEnded(channel, status),
        () => this.#onEnded(channel, Status.of(StatusCode.unknown, 'channel failed')),
      )
      .catch((e: unknown) => this.#host.log('warn', `${this}: handling the end failed`, e));
    this.#host.log('debug', `${this}: opened channel ${channel.id}`);
    this.#callOnOpen(value);
    if (!this.opened.isEnded && this.current === value) {
      this.opened.push(value);
    }
  }

  async #onEnded(channel: MuxChannel, status: Status): Promise<void> {
    if (this.#channel !== channel) {
      return;
    }
    const connection = channel.connection;
    if (this.#host.connection === connection && !connection.isOpen) {
      // The connection is ending: let the client report the disconnect first.
      await connection.done;
      if (this.#channel !== channel) {
        return;
      }
    }
    this.#channel = undefined;
    this.current = undefined;
    if (this.#lifetimeTimer !== undefined) {
      this.#host.clock.clearTimeout(this.#lifetimeTimer);
      this.#lifetimeTimer = undefined;
    }
    if (this.#livedLong) {
      this.#failures = 0;
    }
    const ends = this.closing || isTerminal(status);
    if (ends) {
      if (!this.closing) {
        this.#host.log('info', `${this}: ended with ${status}, not re-opening`);
      }
      this.#finish(status);
    } else {
      this.#host.log('debug', `${this}: channel ${channel.id} ended with ${status}`);
    }
    this.#callOnClosed(status);
    if (ends || this.closing) {
      return;
    }
    const usable = this.#host.usableConnection;
    if (usable === undefined) {
      // Opened again by the client on the next connection.
      return;
    }
    if (usable === connection) {
      this.#scheduleRetry();
    } else {
      this.#openOn(usable);
    }
  }

  #scheduleRetry(): void {
    this.#cancelRetry();
    const delay = this.#host.backoff(this.#failures++);
    this.#host.log('debug', `${this}: re-opening in ${delay} ms`);
    this.#retryTimer = this.#host.clock.setTimeout(() => {
      this.#retryTimer = undefined;
      if (this.closing || this.#channel !== undefined) {
        return;
      }
      const connection = this.#host.usableConnection;
      if (connection !== undefined) {
        this.#openOn(connection);
      }
    }, delay);
  }

  #cancelRetry(): void {
    if (this.#retryTimer !== undefined) {
      this.#host.clock.clearTimeout(this.#retryTimer);
      this.#retryTimer = undefined;
    }
  }

  #finish(status: Status): void {
    if (this.#finished) {
      return;
    }
    this.#finished = true;
    this.closing = true;
    this.#cancelRetry();
    if (this.#lifetimeTimer !== undefined) {
      this.#host.clock.clearTimeout(this.#lifetimeTimer);
      this.#lifetimeTimer = undefined;
    }
    this.#host.remove(this);
    this.#done.resolve(status);
    this.opened.end();
  }

  #closeQuietly(value: T, status: Status): void {
    try {
      this.#kind.close(value, status).catch((e: unknown) => {
        this.#host.log('debug', `${this}: close failed: ${e}`);
      });
    } catch (e) {
      this.#host.log('debug', `${this}: close failed: ${e}`);
    }
  }

  #callOnOpen(value: T): void {
    try {
      const result = this.#onOpen(value);
      if (result instanceof Promise) {
        result.catch((e: unknown) => this.#host.log('warn', `${this}: onOpen failed`, e));
      }
    } catch (e) {
      this.#host.log('warn', `${this}: onOpen failed`, e);
    }
  }

  #callOnClosed(status: Status): void {
    const callback = this.#onClosed;
    if (callback === undefined) {
      return;
    }
    try {
      callback(status);
    } catch (e) {
      this.#host.log('warn', `${this}: onClosed failed`, e);
    }
  }

  toString(): string {
    return `${this.#kind.name}(${this.address})`;
  }
}

/** The status of a failure: a `SwitchboardError`'s, else INTERNAL. */
function statusOf(error: unknown): Status {
  return error instanceof SwitchboardError
    ? error.status
    : Status.of(StatusCode.internal, error instanceof Error ? error.message : String(error));
}

/**
 * A channel of a `ReconnectingClient` that is opened again on every
 * connection, created by `ReconnectingClient.openPersistent`.
 *
 * Each time a connection is established the channel is opened and the
 * `onOpen` callback receives it; when it ends, `onClosed` receives its end
 * status. Every `onOpen` is followed by exactly one `onClosed`.
 *
 * - The connection ends: the channel is opened again on the next
 *   connection.
 * - The endpoint sends GOAWAY, or `reconnectNow({ force: true })`: the
 *   channel is closed with GOING_AWAY and, once it has ended, opened again
 *   on the next connection, which the client makes at once.
 * - The channel ends while the connection stays up (the endpoint closed
 *   it, or the application closed {@link current} itself): it is opened
 *   again after a backoff following the client's schedule, kept per
 *   persistent channel and reset by a channel that lasted longer than the
 *   client's `maxBackoff`. If the connection received GOAWAY, it waits for
 *   the next connection instead.
 * - The channel ends with PERMISSION_DENIED, UNAUTHENTICATED,
 *   UNIMPLEMENTED or NOT_FOUND: the persistent channel ends for good
 *   ({@link isClosed}, {@link done} resolves with that status,
 *   {@link opened} ends), since the endpoint would refuse it again. The
 *   application decides what to do, for example refresh the client's
 *   `defaultPayload` and call `openPersistent` again.
 * - {@link close} ends it for good.
 *
 * An OPEN that cannot be sent is handled the same way: on a connection that
 * received GOAWAY it waits for the next connection; beyond the endpoint's
 * channel limit (RESOURCE_EXHAUSTED) it is retried after the backoff; any
 * other refusal (an OPEN beyond the endpoint's frame limit) ends the
 * persistent channel with `onClosed` called once with that status.
 *
 * ```ts
 * const events = client.openPersistent('events', async (channel) => {
 *   for await (const frame of channel.readable) onEvent(frame);
 * }, { onClosed: (status) => log(`events channel ended: ${status}`) });
 * // Later:
 * await events.close();
 * ```
 */
export class PersistentChannel implements AsyncDisposable {
  readonly #core: PersistentCore<MuxChannel>;

  /** @internal Created by `ReconnectingClient.openPersistent`. */
  constructor(core: PersistentCore<MuxChannel>) {
    this.#core = core;
  }

  /** The address every channel is opened with. */
  get address(): ChannelAddress {
    return this.#core.address;
  }

  /** The open channel; undefined between connections, while waiting to re-open, and once closed. */
  get current(): MuxChannel | undefined {
    return this.#core.current;
  }

  /**
   * Each newly opened channel, after `onOpen` was called with it, to every
   * consumer iterating at the time. Ends when the persistent channel ends
   * for good.
   */
  get opened(): AsyncIterable<MuxChannel> {
    return this.#core.opened;
  }

  /** Whether the persistent channel has ended for good or {@link close} was called. */
  get isClosed(): boolean {
    return this.#core.closing;
  }

  /**
   * Resolves when the persistent channel has ended for good: with the end
   * status of its last channel (OK after {@link close}, unless the channel
   * ended otherwise), or the terminal status that ended it. Never rejects.
   */
  get done(): Promise<Status> {
    return this.#core.done;
  }

  /**
   * Ends the persistent channel: stops re-opening it and closes
   * {@link current} with OK. Resolves when the channel is closed and
   * `onClosed` has been called; calling it again resolves when the same has
   * happened. Never rejects.
   */
  close(): Promise<void> {
    return this.#core.close();
  }

  /** {@link close}. */
  [Symbol.asyncDispose](): Promise<void> {
    return this.close();
  }

  toString(): string {
    return this.#core.toString();
  }
}

/**
 * A Talk channel of a `ReconnectingClient` that is opened again on every
 * connection, created by `ReconnectingClient.openPersistentTalk`. Behaves
 * like {@link PersistentChannel}, each channel wrapped in a `TalkChannel`;
 * the status that decides whether it is opened again is the Talk channel's
 * end status (`TalkChannel.done`), so a channel abort with a terminal status
 * ends it too.
 *
 * ```ts
 * const feed = client.openPersistentTalk('feed', async (talk) => {
 *   void talk.serve(onFeedMessage);
 *   // Sent again on every connection.
 *   await talk.request('SUB', topic);
 * });
 * ```
 */
export class PersistentTalk implements AsyncDisposable {
  readonly #core: PersistentCore<TalkChannel>;

  /** @internal Created by `ReconnectingClient.openPersistentTalk`. */
  constructor(core: PersistentCore<TalkChannel>) {
    this.#core = core;
  }

  /** The address every channel is opened with. */
  get address(): ChannelAddress {
    return this.#core.address;
  }

  /** The open Talk channel; undefined between connections, while waiting to re-open, and once closed. */
  get current(): TalkChannel | undefined {
    return this.#core.current;
  }

  /** Each newly opened Talk channel, after `onOpen` was called with it. Ends when the persistent channel ends for good. */
  get opened(): AsyncIterable<TalkChannel> {
    return this.#core.opened;
  }

  /** Whether the persistent channel has ended for good or {@link close} was called. */
  get isClosed(): boolean {
    return this.#core.closing;
  }

  /**
   * Resolves when the persistent channel has ended for good: with the end
   * status of its last Talk channel (OK after {@link close}, unless the
   * channel ended otherwise), or the terminal status that ended it. Never
   * rejects.
   */
  get done(): Promise<Status> {
    return this.#core.done;
  }

  /**
   * Ends the persistent channel: stops re-opening it and closes
   * {@link current} with OK (outstanding requests fail with CANCELLED).
   * Resolves when the channel is closed and `onClosed` has been called.
   * Never rejects.
   */
  close(): Promise<void> {
    return this.#core.close();
  }

  /** {@link close}. */
  [Symbol.asyncDispose](): Promise<void> {
    return this.close();
  }

  toString(): string {
    return this.#core.toString();
  }
}
