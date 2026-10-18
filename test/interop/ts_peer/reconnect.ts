/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// The reconnecting client commands (README, "Reconnecting client"): a
// ReconnectingClient of this implementation to the other peer's listener,
// its states, pushed channels and persistent channels reported as events,
// and the channels it opens as handles the other client commands drive, as
// dart_peer/lib/src/reconnect.dart has them.

import {
  type ClientState,
  type ConnectionConnector,
  connectTransport,
  type MuxChannel,
  nativeQuic,
  nativeQuicAvailability,
  type PersistentChannel,
  type PersistentTalk,
  quicConnector,
  ReconnectingClient,
  type Status,
  type TalkChannel,
  type TransportConnector,
} from '../../../src/node/index.ts';
import { addressOf, messageJson, payloadOf, reportTalk } from './client.ts';
import type { PeerContext } from './context.ts';
import {
  ArgumentProblem,
  boolArg,
  bytesOf,
  errorJson,
  intArg,
  type Json,
  numberArg,
  objectArg,
  requiredString,
  statusJson,
} from './protocol.ts';
import { addressJson } from './services.ts';

/** The reconnecting client commands. */
export const reconnectOps = [
  'reconnect.open',
  'reconnect.talk',
  'reconnect.channel',
  'reconnect.persistent',
  'reconnect.stop',
  'reconnect.start',
  'reconnect.now',
  'reconnect.close',
  'persistent.close',
] as const;

/** A request sent on every channel of a persistent Talk channel, as it opens. */
interface Subscription {
  readonly procedure: string;
  readonly payload: Uint8Array;
}

/** The reconnecting client commands of a peer. */
export class ReconnectCommands {
  readonly #ctx: PeerContext;

  /** The commands of the peer [ctx]. */
  constructor(ctx: PeerContext) {
    this.#ctx = ctx;
  }

  /** Runs the reconnecting client command [op]. */
  async command(op: string, cmd: Json): Promise<unknown> {
    const h = this.#ctx.handles;
    switch (op) {
      case 'reconnect.open':
        return this.#open(cmd);
      case 'persistent.close': {
        const p = h.get(cmd['persistent'], 'p', 'persistent channel');
        await p.close();
        return statusJson(await p.done);
      }
    }
    const client = h.get(cmd['reconnect'], 'r', 'reconnecting client');
    switch (op) {
      case 'reconnect.talk':
        return this.#adoptTalk(
          await client.openTalk(addressOf(cmd), { openTimeout: intArg(cmd, 'openTimeoutMs') }),
        );
      case 'reconnect.channel': {
        const ch = await client.openChannel(addressOf(cmd), {
          openTimeout: intArg(cmd, 'openTimeoutMs'),
        });
        const connection = this.#ctx.trackConnection(ch.connection);
        return { channel: this.#ctx.trackChannel(ch), id: ch.id, connection };
      }
      case 'reconnect.persistent':
        return this.#persistent(client, cmd);
      case 'reconnect.stop':
        client.stop();
        return this.#stateJson(client, client.state);
      case 'reconnect.start':
        client.start();
        return null;
      case 'reconnect.now':
        client.reconnectNow({ force: boolArg(cmd, 'force') });
        return null;
      case 'reconnect.close':
        await client.close();
        return null;
      default:
        throw new ArgumentProblem(`unknown op ${op}`);
    }
  }

  /**
   * A reconnecting client to `endpoint`, with the peer's mux and Talk
   * options and the backoff the command names (the library's defaults
   * otherwise). Its state changes are `reconnect.state` events and the
   * channels the endpoint pushes `reconnect.incoming`.
   */
  #open(cmd: Json): Json {
    const ctx = this.#ctx;
    const client = new ReconnectingClient(this.#connector(requiredString(cmd, 'endpoint')), {
      muxOptions: ctx.config.mux,
      talkOptions: ctx.config.talk,
      initialBackoff: intArg(cmd, 'initialBackoffMs'),
      maxBackoff: intArg(cmd, 'maxBackoffMs'),
      backoffFactor: numberArg(cmd, 'backoffFactor'),
      jitter: numberArg(cmd, 'jitter'),
      connectTimeout: intArg(cmd, 'connectTimeoutMs'),
      defaultPayload: bytesOf(cmd['defaultPayload']),
      // Started below, once every state change is listened to.
      autoStart: false,
      ...(ctx.logger === undefined ? {} : { logger: ctx.logger }),
    });
    const handle = ctx.handles.add('r', client);
    client.on('state', (state) =>
      ctx.out.event('reconnect.state', { reconnect: handle, ...this.#stateJson(client, state) }),
    );
    void (async () => {
      for await (const incoming of client.incoming) {
        const connection = ctx.trackConnection(incoming.connection);
        ctx.trackChannel(incoming.channel, true, (channel) =>
          ctx.out.event('reconnect.incoming', {
            reconnect: handle,
            channel,
            connection,
            id: incoming.channel.id,
            ...addressJson(incoming.address),
          }),
        );
      }
    })();
    if (boolArg(cmd, 'autoStart', true)) {
      client.start();
    }
    return { reconnect: handle };
  }

  /**
   * Dials [endpoint]: the frame mux over a transport for `tcp://` and
   * `ws://`, the QUIC binding for `quic://` and `https://` (WebTransport),
   * without verifying the listener's certificate, as the client node does.
   */
  #connector(endpoint: string): TransportConnector | ConnectionConnector {
    const scheme = endpoint.substring(0, endpoint.indexOf(':')).toLowerCase();
    switch (scheme) {
      case 'tcp':
      case 'ws':
      case 'wss': {
        const maxFrameSize = this.#ctx.config.mux.maxFrameSize;
        return ({ signal }) => connectTransport(endpoint, { signal, maxFrameSize });
      }
      case 'quic':
      case 'https': {
        const quic = nativeQuicAvailability();
        if (!quic.available) {
          throw new ArgumentProblem(`${endpoint} needs the native QUIC addon: ${quic.reason}`);
        }
        return quicConnector(endpoint, nativeQuic(), { verifyCertificate: false });
      }
      default:
        throw new ArgumentProblem(`no reconnecting client for ${endpoint}`);
    }
  }

  /**
   * A persistent channel of [client] to `address`: a persistent Talk
   * channel unless `talk` is false. Each channel it opens is
   * `persistent.opened`, with `request` sent on it for a Talk channel and
   * its outcome `persistent.reply`; each end `persistent.closed`; the end
   * for good `persistent.done`.
   */
  #persistent(client: ReconnectingClient, cmd: Json): Json {
    const ctx = this.#ctx;
    const address = addressOf(cmd);
    const request = objectArg(cmd, 'request');
    const subscription: Subscription | undefined =
      request === undefined
        ? undefined
        : { procedure: requiredString(request, 'procedure'), payload: payloadOf(request) };
    // Set before any callback: onOpen is never called from openPersistent itself.
    let handle = '';
    const onClosed = (status: Status): void =>
      ctx.out.event('persistent.closed', { persistent: handle, ...statusJson(status) });
    let persistent: PersistentChannel | PersistentTalk;
    if (boolArg(cmd, 'talk', true)) {
      persistent = client.openPersistentTalk(
        address,
        (talk) => {
          const opened = this.#adoptTalk(talk);
          ctx.out.event('persistent.opened', { persistent: handle, ...opened });
          if (subscription !== undefined) {
            return this.#subscribe(handle, opened.talk, talk, subscription);
          }
          return undefined;
        },
        { onClosed },
      );
    } else {
      persistent = client.openPersistent(
        address,
        (channel) => {
          const connection = ctx.trackConnection(channel.connection);
          ctx.trackChannel(channel, true, (ch) =>
            ctx.out.event('persistent.opened', { persistent: handle, channel: ch, connection }),
          );
        },
        { onClosed },
      );
    }
    handle = ctx.handles.add('p', persistent);
    void persistent.done.then((status) =>
      ctx.out.event('persistent.done', { persistent: handle, ...statusJson(status) }),
    );
    return { persistent: handle };
  }

  /** Sends [subscription] on [talk] and reports its outcome as `persistent.reply`. */
  async #subscribe(
    persistent: string,
    handle: string,
    talk: TalkChannel,
    subscription: Subscription,
  ): Promise<void> {
    const ctx = this.#ctx;
    try {
      const reply = await talk.request(subscription.procedure, subscription.payload);
      ctx.out.event('persistent.reply', {
        persistent,
        talk: handle,
        ...(await messageJson(reply)),
      });
    } catch (e) {
      ctx.out.event('persistent.reply', { persistent, talk: handle, error: errorJson(e) });
    }
  }

  /** [talk], opened by a reconnecting client, as `{talk, channel, connection}` handles. */
  #adoptTalk(talk: TalkChannel): { talk: string; channel: string; connection: string } {
    const ctx = this.#ctx;
    const raw: MuxChannel | undefined = talk.raw;
    if (raw === undefined) {
      throw new Error('a reconnecting client opens Talk channels over mux channels');
    }
    const connection = ctx.trackConnection(raw.connection);
    const channel = ctx.trackChannel(raw, false);
    return { talk: reportTalk(ctx, talk), channel, connection };
  }

  /**
   * [state] as `reconnect.state` reports it: the phase, the attempt, the
   * last status, the delay of the attempt scheduled (`nextAttemptAt` less
   * `since`, rounded to milliseconds), and while connected the handle of
   * the connection.
   */
  #stateJson(client: ReconnectingClient, state: ClientState): Json {
    const connection = state.isConnected ? client.connection : undefined;
    const { nextAttemptAt, since } = state;
    return {
      phase: state.phase,
      attempt: state.attempt,
      lastStatus: state.lastStatus === undefined ? null : statusJson(state.lastStatus),
      retryMs:
        nextAttemptAt === undefined || since === undefined
          ? null
          : Math.round(nextAttemptAt - since),
      connection: connection === undefined ? null : this.#ctx.trackConnection(connection),
    };
  }
}
