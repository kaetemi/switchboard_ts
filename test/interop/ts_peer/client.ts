/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// The peer as the initiator (README, "Commands"): connections, channels and
// Talk channels to the other peer's listeners, as handles, on a node of their
// own that never listens, as dart_peer/lib/src/client.dart has them.

import { createHash } from 'node:crypto';
import {
  ChannelAddress,
  hexBytes,
  type MuxChannel,
  Name,
  type NativeQuicSession,
  nativeQuic,
  nativeQuicAvailability,
  QuicBindingConnection,
  type Resolver,
  Status,
  StatusCode,
  Switchboard,
  TalkChannel,
  type TalkMessage,
  utf8Encode,
} from '../../../src/node/index.ts';
import type { PeerContext } from './context.ts';
import type { NodeIdentity } from './identity.ts';
import { NamingCommands, namingOps } from './mesh.ts';
import {
  ArgumentProblem,
  boolArg,
  bytesOf,
  errorJson,
  hex,
  intArg,
  type Json,
  objectArg,
  requiredString,
  statusArg,
  stringArg,
} from './protocol.ts';
import { chunks, pattern } from './services.ts';

/** The client commands. */
export const clientOps = [
  'connect',
  'open',
  'talk.open',
  'talk.request',
  'talk.stream',
  'talk.send',
  'talk.close',
  'stream.grant',
  'stream.cancel',
  'channel.send',
  'channel.close',
  'connection.ping',
  'connection.goAway',
  'connection.close',
  'connection.drain',
  'connection.identify',
  'request',
] as const;

/** The client commands, on a node of their own that never listens. */
export class Client {
  readonly #ctx: PeerContext;
  #node: Switchboard | undefined;

  readonly #naming: NamingCommands;

  constructor(ctx: PeerContext) {
    this.#ctx = ctx;
    this.#naming = new NamingCommands(ctx, (resolver) => this.#newNode(resolver));
  }

  #identity: NodeIdentity | undefined;

  /** Sets the identity the client node is made with; before its first use. */
  identify(identity: NodeIdentity): void {
    this.#identity = identity;
  }

  /** Whether {@link command} answers [op]. */
  static answers(op: string): boolean {
    return (
      (clientOps as readonly string[]).includes(op) || (namingOps as readonly string[]).includes(op)
    );
  }

  get #client(): Switchboard {
    this.#node ??= this.#newNode(undefined);
    return this.#node;
  }

  /** A client node, as the shared one is, resolving with [resolver]. */
  #newNode(resolver: Resolver | undefined): Switchboard {
    const ctx = this.#ctx;
    const identity = this.#identity;
    return new Switchboard({
      resolver,
      localConnections: false,
      muxOptions: ctx.config.mux,
      talkOptions: ctx.config.talk,
      credential: identity?.credential,
      holderKey: identity?.holderKey,
      verifier: identity?.verifier,
      identifyOutgoing: ctx.config.identity.identifyOutgoing,
      identityTimeout: ctx.config.identity.identityTimeout,
      // Dials quic:// and https:// (WebTransport) without verifying the
      // listener's self-signed certificate, as the Dart peer does.
      ...(nativeQuicAvailability().available
        ? { quic: nativeQuic(), quicBinding: { webTransportCertificateVerified: false } }
        : {}),
      ...(ctx.logger === undefined ? {} : { logger: ctx.logger }),
    });
  }

  /** Runs the client command [op]. */
  async command(op: string, cmd: Json): Promise<unknown> {
    const h = this.#ctx.handles;
    if ((namingOps as readonly string[]).includes(op)) {
      return this.#naming.command(op, cmd, (channel) =>
        this.#adoptTalk(channel, this.#ctx.trackConnection(channel.connection)),
      );
    }
    switch (op) {
      case 'connect': {
        const intent = cmd['intent'];
        const c = await this.#client.dial(requiredString(cmd, 'endpoint'), {
          intent: intent === undefined || intent === null ? undefined : bytesOf(intent),
          receiver: stringArg(cmd, 'receiver'),
        });
        return { connection: this.#ctx.trackConnection(c, this.#client) };
      }
      case 'connection.identify': {
        const c = h.get(cmd['connection'], 'c', 'connection');
        const intent = cmd['intent'];
        await (this.#ctx.ownerOf(c) ?? this.#client).identifyOn(c, {
          intent: intent === undefined || intent === null ? undefined : bytesOf(intent),
          receiver: stringArg(cmd, 'receiver'),
        });
        return null;
      }
      case 'open': {
        const ch = h.get(cmd['connection'], 'c', 'connection').open(openPayload(cmd));
        return { channel: this.#ctx.trackChannel(ch), id: ch.id };
      }
      case 'talk.open':
        return this.#talkOpen(cmd);
      case 'talk.request': {
        const handle = requiredString(cmd, 'talk');
        const t = h.get(handle, 't', 'talk channel');
        const { payload, bulk } = payloadAndBulk(cmd);
        const r = t.startRequest(requiredString(cmd, 'procedure'), payload, {
          ...bulk,
          timeout: intArg(cmd, 'timeoutMs'),
          onExtend: (extension) =>
            this.#ctx.out.event('talk.extend', {
              talk: handle,
              requestId: r.requestId,
              ...extensionJson(extension),
            }),
        });
        return messageJson(await r.response, boolArg(cmd, 'digest'));
      }
      case 'talk.stream':
        return this.#talkStream(cmd);
      case 'talk.send': {
        const { payload, bulk } = payloadAndBulk(cmd);
        await h
          .get(cmd['talk'], 't', 'talk channel')
          .send(requiredString(cmd, 'procedure'), payload, bulk);
        return null;
      }
      case 'talk.close':
        await h.get(cmd['talk'], 't', 'talk channel').close(statusArg(cmd));
        return null;
      case 'stream.grant':
        h.get(cmd['stream'], 's', 'stream').grant(intArg(cmd, 'items') ?? 1);
        return null;
      case 'stream.cancel':
        h.get(cmd['stream'], 's', 'stream').cancel(
          cmd['code'] === undefined || cmd['code'] === null ? undefined : statusArg(cmd),
        );
        return null;
      case 'channel.send':
        await h.get(cmd['channel'], 'ch', 'channel').send(bytesOf(cmd['hex']));
        return null;
      case 'channel.close':
        await h.get(cmd['channel'], 'ch', 'channel').close(statusArg(cmd));
        return null;
      case 'connection.ping': {
        const rttMs = await h
          .get(cmd['connection'], 'c', 'connection')
          .ping(bytesOf(cmd['payload']));
        return { rttMs };
      }
      case 'connection.goAway':
        await h
          .get(cmd['connection'], 'c', 'connection')
          .goAway(statusArg(cmd, StatusCode.goingAway));
        return null;
      case 'connection.close':
        await h.get(cmd['connection'], 'c', 'connection').close();
        return null;
      case 'connection.drain': {
        const c = h.get(cmd['connection'], 'c', 'connection');
        const binding = c instanceof QuicBindingConnection ? c : undefined;
        const session = binding?.session;
        if (session === undefined || !binding?.isWebTransport || !('drain' in session)) {
          throw new ArgumentProblem('connection.drain needs a WebTransport connection');
        }
        (session as NativeQuicSession).drain();
        return null;
      }
      case 'request':
        return this.#request(cmd);
      default:
        throw new ArgumentProblem(`unknown op ${op}`);
    }
  }

  /**
   * A Talk channel to [openPayload]'s service at [endpoint], on a pooled
   * connection: what a `forward` service forwards to.
   */
  async openTalk(endpoint: string, openPayload: Uint8Array): Promise<TalkChannel> {
    const c = await this.#client.connect(endpoint);
    return new TalkChannel(c.open(openPayload), this.#ctx.config.talk);
  }

  /** Closes the client node. */
  async close(): Promise<void> {
    await this.#node?.close();
    this.#node = undefined;
  }

  async #talkOpen(cmd: Json): Promise<Json> {
    const ctx = this.#ctx;
    let connection = stringArg(cmd, 'connection');
    if (connection === undefined) {
      const c = await this.#client.dial(requiredString(cmd, 'endpoint'));
      connection = ctx.trackConnection(c, this.#client);
    }
    const ch = ctx.handles.get(connection, 'c', 'connection').open(openPayload(cmd));
    return this.#adoptTalk(ch, connection);
  }

  /** [ch] as a Talk channel handle on [connection]'s, its messages reported and its requests refused. */
  #adoptTalk(ch: MuxChannel, connection: string): Json {
    const ctx = this.#ctx;
    const channel = ctx.trackChannel(ch, false);
    const talk = new TalkChannel(ch, ctx.config.talk);
    const handle = ctx.handles.add('t', talk);
    void (async () => {
      try {
        for await (const m of talk.messages) {
          void (async () => {
            ctx.out.event('talk.message', {
              talk: handle,
              requestId: m.requestId,
              ...(await messageJson(m)),
            });
            if (m.canReply) {
              await m.replyAbort(Status.of(StatusCode.unimplemented));
            }
          })().catch(() => {});
        }
      } catch {
        // A channel abort from the far side ends the messages.
      }
    })();
    return { talk: handle, channel, connection };
  }

  #talkStream(cmd: Json): Json {
    const ctx = this.#ctx;
    const t = ctx.handles.get(cmd['talk'], 't', 'talk channel');
    const { payload, bulk } = payloadAndBulk(cmd);
    let handle: string | undefined;
    const s = t.streamRequest(requiredString(cmd, 'procedure'), payload, {
      ...bulk,
      timeout: intArg(cmd, 'timeoutMs'),
      window: intArg(cmd, 'window'),
      onExtend: (extension) =>
        ctx.out.event('talk.extend', { stream: handle, ...extensionJson(extension) }),
    });
    handle = ctx.handles.add('s', s);
    const digest = boolArg(cmd, 'digest');
    void (async () => {
      let index = 0;
      try {
        for await (const item of s.items) {
          ctx.out.event('talk.item', {
            stream: handle,
            index: index++,
            ...(await messageJson(item, digest)),
          });
        }
      } catch {
        // The outcome is reported by talk.done.
      }
      try {
        const last = await s.done;
        ctx.out.event('talk.done', {
          stream: handle,
          items: index,
          ...(await messageJson(last, digest)),
        });
      } catch (e) {
        ctx.out.event('talk.done', { stream: handle, items: index, error: errorJson(e) });
      }
    })();
    return { stream: handle, requestId: s.requestId };
  }

  async #request(cmd: Json): Promise<Json> {
    const ctx = this.#ctx;
    const c = await this.#client.dial(requiredString(cmd, 'endpoint'));
    try {
      const talk = new TalkChannel(
        c.open(
          new ChannelAddress({
            type: Name.of(requiredString(cmd, 'service')),
            instance: intArg(cmd, 'instance') ?? 0,
          }).encode(),
        ),
        ctx.config.talk,
      );
      try {
        const reply = await talk.request(requiredString(cmd, 'procedure'), payloadOf(cmd), {
          timeout: intArg(cmd, 'timeoutMs'),
        });
        return await messageJson(reply, boolArg(cmd, 'digest'));
      } finally {
        await talk.close();
      }
    } finally {
      await c.goAway();
    }
  }
}

/** The OPEN payload: `openHex` as is, else `address` encoded. */
export function openPayload(cmd: Json): Uint8Array {
  if (cmd['openHex'] !== undefined) {
    return bytesOf(cmd['openHex']);
  }
  const a = objectArg(cmd, 'address') ?? {};
  const type = stringArg(a, 'type');
  const typeHex = stringArg(a, 'typeHex');
  return new ChannelAddress({
    type:
      typeHex !== undefined
        ? Name.fromBytes(hexBytes(typeHex))
        : type === undefined
          ? undefined
          : Name.of(type),
    instance: intArg(a, 'instance') ?? 0,
    shard: intArg(a, 'shard'),
    host: stringArg(a, 'host'),
    payload: bytesOf(a['payload']),
  }).encode();
}

/**
 * A command's payload, and with `bulkPattern` (`{length, chunk, declared}`)
 * instead an empty payload and the `BIG` pattern of `length` bytes as a bulk
 * source in pieces of `chunk` (default 1000), its length declared unless
 * `declared` is false.
 */
export function payloadAndBulk(cmd: Json): {
  payload: Uint8Array;
  bulk: { bulk?: Iterable<Uint8Array>; bulkLength?: number };
} {
  const spec = objectArg(cmd, 'bulkPattern');
  if (spec === undefined) {
    return { payload: payloadOf(cmd), bulk: {} };
  }
  const length = intArg(spec, 'length') ?? 0;
  const source = chunks(pattern(length), intArg(spec, 'chunk') ?? 1000);
  return {
    payload: new Uint8Array(0),
    bulk: boolArg(spec, 'declared', true) ? { bulk: source, bulkLength: length } : { bulk: source },
  };
}

/** An `EXTEND` as the `talk.extend` event reports it, in milliseconds. */
function extensionJson(extension: { deadline?: number; renew?: number }): Json {
  return { deadlineMs: extension.deadline ?? null, renewMs: extension.renew ?? null };
}

/**
 * A command's payload: `payload` (hex), `text` (UTF-8) or `patternLength`
 * (the `BIG` pattern of that many bytes).
 */
export function payloadOf(cmd: Json): Uint8Array {
  const text = stringArg(cmd, 'text');
  if (text !== undefined) {
    return utf8Encode(text);
  }
  const length = intArg(cmd, 'patternLength');
  if (length !== undefined) {
    return pattern(length);
  }
  return bytesOf(cmd['payload']);
}

/**
 * A received message as JSON: its procedure, whether it came as bulk, and
 * the payload as hex, or with [digest] its size and SHA-256.
 */
export async function messageJson(m: TalkMessage, digest = false): Promise<Json> {
  const payload = await m.bytes();
  return {
    procedure: m.procedureName,
    bulk: m.isBulk,
    ...(digest
      ? { size: payload.length, sha256: createHash('sha256').update(payload).digest('hex') }
      : { payload: hex(payload) }),
  };
}
