/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// The interop services: fixed behaviour both peers implement identically
// (README, "Services"), as dart_peer/lib/src/services.dart serves them.

import { createHash } from 'node:crypto';
import {
  ChannelAddress,
  type ChannelHandler,
  forwardMessage,
  Name,
  Status,
  StatusCode,
  type Switchboard,
  SwitchboardError,
  type TalkChannel,
  type TalkMessage,
  utf8Decode,
  utf8Encode,
} from '../../../src/node/index.ts';
import type { Client } from './client.ts';
import type { ServiceSpec } from './config.ts';
import type { PeerContext } from './context.ts';
import { hex, parseInteger } from './protocol.ts';

/** A channel as a service handler receives it. */
type IncomingChannel = Parameters<ChannelHandler>[0];

/** Registers [spec] on [node]; [client] dials what a `forward` service forwards to. */
export function registerService(
  node: Switchboard,
  spec: ServiceSpec,
  ctx: PeerContext,
  client: Client,
): void {
  const handlers: Record<ServiceSpec['behaviour'], ChannelHandler> = {
    echo: (incoming) => serveEcho(incoming, spec, ctx),
    raw: (incoming) => serveRaw(incoming, spec, ctx, node),
    // Accepts the channel and never reads it: its window fills up.
    sink: () => {},
    forward: (incoming) => serveForward(incoming, spec, ctx, client),
  };
  node.registerService(spec.type, handlers[spec.behaviour], {
    instance: spec.instance,
    acceptAnyInstance: spec.anyInstance,
  });
}

/** Calls [handler] for every message of [talk], not awaited; ends quietly with it. */
function each(talk: TalkChannel, handler: (m: TalkMessage) => void): void {
  void (async () => {
    try {
      for await (const m of talk.messages) {
        handler(m);
      }
    } catch {
      // A channel abort from the peer ends the messages.
    }
  })();
}

/**
 * `forward`: every message of the channel is forwarded with forwardMessage
 * on a Talk channel of its own to the service the spec's `target` names (at
 * its `endpoint`, by default this peer's first listener), and every message
 * from there back the other way; a close on either side closes the other
 * with its status.
 */
function serveForward(
  incoming: IncomingChannel,
  spec: ServiceSpec,
  ctx: PeerContext,
  client: Client,
): void {
  const front = incoming.talk(ctx.config.talk);
  void (async () => {
    let back: TalkChannel;
    try {
      const endpoint = spec.target.endpoint ?? ctx.endpoints[0];
      if (endpoint === undefined) {
        throw SwitchboardError.of(StatusCode.unavailable, 'no endpoint to forward to');
      }
      back = await client.openTalk(
        endpoint,
        new ChannelAddress({
          type: Name.of(spec.target.type ?? 'echo'),
          instance: spec.target.instance,
        }).encode(),
      );
    } catch (e) {
      await front.close(
        e instanceof SwitchboardError ? e.status : Status.of(StatusCode.unavailable, `${e}`),
      );
      return;
    }
    each(front, (m) => void forwardMessage(m, back));
    each(back, (m) => void forwardMessage(m, front));
    void front.done.then((s) => back.close(s));
    void back.done.then((s) => front.close(s));
  })();
}

/** `raw`: the channel becomes a handle; its subframes and its end are events. */
function serveRaw(
  incoming: IncomingChannel,
  spec: ServiceSpec,
  ctx: PeerContext,
  node: Switchboard,
): void {
  const connection = ctx.trackConnection(incoming.connection, node);
  const handle = ctx.trackChannel(incoming.channel, false);
  ctx.out.event('channel.incoming', {
    channel: handle,
    connection,
    service: spec.type,
    id: incoming.channel.id,
    ...addressJson(incoming.address),
    identity: incoming.peerIdentity?.identity ?? null,
  });
  ctx.reportData(incoming.channel, handle);
}

/** [a] as the `channel.incoming` event names it. */
export function addressJson(a: ChannelAddress): Record<string, unknown> {
  return {
    type: a.type === undefined ? null : a.type.toString(),
    instance: a.instance,
    shard: a.shard ?? null,
    host: a.host ?? null,
    payload: hex(a.payload),
  };
}

/** `echo`: the interop procedures over Talk. */
function serveEcho(incoming: IncomingChannel, spec: ServiceSpec, ctx: PeerContext): void {
  const talk = incoming.talk(ctx.config.talk);
  each(talk, (m) => {
    echo(m, incoming, talk, spec, ctx).catch((e: unknown) => failed(m, e, ctx));
  });
}

async function failed(m: TalkMessage, error: unknown, ctx: PeerContext): Promise<void> {
  if (error instanceof SwitchboardError) {
    // The requester went away or cancelled; nothing to answer.
    return;
  }
  ctx.reportError(error);
  if (m.canReply) {
    await m.replyAbort(Status.of(StatusCode.internal, `${error}`)).catch(() => {});
  }
}

async function echo(
  m: TalkMessage,
  incoming: IncomingChannel,
  talk: TalkChannel,
  spec: ServiceSpec,
  ctx: PeerContext,
): Promise<void> {
  const payload = await m.bytes();
  if (!m.expectsReply) {
    ctx.out.event('service.message', {
      service: spec.type,
      procedure: m.procedureName,
      bulk: m.isBulk,
      payload: hex(payload),
    });
    return;
  }
  const text = utf8Decode(payload);
  const [verb, rest] = split(text);
  switch (m.procedureName) {
    case 'ECHO':
      await m.reply(payload);
      break;
    case 'UPPER':
      await m.reply(asciiUpper(payload));
      break;
    case 'SIZE':
      await m.reply(
        utf8Encode(`${payload.length} ${createHash('sha256').update(payload).digest('hex')}`),
      );
      break;
    case 'BIG':
      await m.reply(pattern(parseInteger(text)));
      break;
    case 'BIGSTRM':
      // A bulk payload streamed, its length not declared.
      await m.reply(undefined, { bulk: chunks(pattern(parseInteger(text)), 1000) });
      break;
    case 'COUNT': {
      const n = parseInteger(text);
      for (let i = 1; i <= n; i++) {
        await m.replyItem(utf8Encode(`${i}`));
      }
      await m.reply(utf8Encode('done'));
      break;
    }
    case 'BIGITEMS': {
      const n = parseInteger(verb);
      const item = pattern(parseInteger(rest));
      for (let i = 1; i <= n; i++) {
        await m.replyItem(item);
      }
      await m.reply(utf8Encode('done'));
      break;
    }
    case 'HOW':
      await m.reply(utf8Encode(`${m.isBulk ? 'bulk' : 'inline'} ${payload.length}`));
      break;
    case 'EXTEND': {
      const ms = parseInteger(verb);
      // Twice the delay, so the reply is well within the responder's own deadline.
      await m.extend({ deadline: 2 * ms });
      await new Promise((resolve) => setTimeout(resolve, ms));
      await m.reply(utf8Encode('extended'));
      break;
    }
    case 'STATUS':
      await m.replyAbort(Status.decode(payload));
      break;
    case 'ABORT':
      await m.replyAbort(new Status(parseInteger(verb), rest));
      break;
    case 'SLOW':
      await new Promise((resolve) => setTimeout(resolve, parseInteger(verb)));
      await m.reply(utf8Encode('slow'));
      break;
    case 'STALL':
      // Never answers; the requester times out or cancels.
      break;
    case 'NOTIFY':
      await talk.send('NOTE', payload);
      await m.reply(utf8Encode('sent'));
      break;
    case 'CLOSE':
      await m.reply(utf8Encode('closing'));
      await talk.close(new Status(parseInteger(verb), rest));
      break;
    case 'GOAWAY':
      await m.reply(utf8Encode('going'));
      void incoming.connection.goAway();
      break;
    case 'WHOAMI':
      await m.reply(utf8Encode(incoming.peerIdentity?.identity ?? ''));
      break;
    default:
      await m.replyAbort(Status.of(StatusCode.unimplemented));
  }
}

/** [bytes] in pieces of [size]. */
export function* chunks(bytes: Uint8Array, size: number): Generator<Uint8Array> {
  for (let at = 0; at < bytes.length; at += size) {
    yield bytes.slice(at, Math.min(at + size, bytes.length));
  }
}

/** The first space separated word of [text] and the rest after the space. */
function split(text: string): [string, string] {
  const at = text.indexOf(' ');
  return at < 0 ? [text, ''] : [text.substring(0, at), text.substring(at + 1)];
}

/** [bytes] with ASCII `a` to `z` upper-cased and every other byte as is. */
export function asciiUpper(bytes: Uint8Array): Uint8Array {
  return bytes.map((b) => (b >= 0x61 && b <= 0x7a ? b - 0x20 : b));
}

/** The pattern `BIG` answers: byte i is `(i * 31) & 0xFF`. */
export function pattern(length: number): Uint8Array {
  const out = new Uint8Array(length);
  for (let i = 0; i < length; i++) {
    out[i] = (i * 31) & 0xff;
  }
  return out;
}
