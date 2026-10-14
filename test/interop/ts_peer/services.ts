/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// The interop services: fixed behaviour both peers implement identically
// (README, "Services"), as dart_peer/lib/src/services.dart serves them.

import { createHash } from 'node:crypto';
import {
  type ChannelAddress,
  type ChannelHandler,
  Status,
  StatusCode,
  type Switchboard,
  SwitchboardError,
  type TalkChannel,
  type TalkMessage,
  utf8Decode,
  utf8Encode,
} from '../../../src/node/index.ts';
import type { ServiceSpec } from './config.ts';
import type { PeerContext } from './context.ts';
import { hex, parseInteger } from './protocol.ts';

/** A channel as a service handler receives it. */
type IncomingChannel = Parameters<ChannelHandler>[0];

/** Registers [spec] on [node]. */
export function registerService(node: Switchboard, spec: ServiceSpec, ctx: PeerContext): void {
  const handlers: Record<ServiceSpec['behaviour'], ChannelHandler> = {
    echo: (incoming) => serveEcho(incoming, spec, ctx),
    raw: (incoming) => serveRaw(incoming, spec, ctx),
    // Accepts the channel and never reads it: its window fills up.
    sink: () => {},
  };
  node.registerService(spec.type, handlers[spec.behaviour], { instance: spec.instance });
}

/** `raw`: the channel becomes a handle; its subframes and its end are events. */
function serveRaw(incoming: IncomingChannel, spec: ServiceSpec, ctx: PeerContext): void {
  const connection = ctx.trackConnection(incoming.connection);
  const handle = ctx.trackChannel(incoming.channel, false);
  ctx.out.event('channel.incoming', {
    channel: handle,
    connection,
    service: spec.type,
    id: incoming.channel.id,
    ...addressJson(incoming.address),
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
function serveEcho(incoming: IncomingChannel, _spec: ServiceSpec, ctx: PeerContext): void {
  const talk = incoming.talk(ctx.config.talk);
  void (async () => {
    try {
      for await (const m of talk.messages) {
        echo(m, incoming, talk, _spec, ctx).catch((e: unknown) => failed(m, e, ctx));
      }
    } catch {
      // A channel abort from the requester ends the messages.
    }
  })();
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
    case 'COUNT': {
      const n = parseInteger(text);
      for (let i = 1; i <= n; i++) {
        await m.replyItem(utf8Encode(`${i}`));
      }
      await m.reply(utf8Encode('done'));
      break;
    }
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
