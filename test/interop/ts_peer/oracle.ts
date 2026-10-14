/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// The codec oracle of the TS peer: the TS decoders and encoders behind the
// `decode`, `decodeMany` and `encode` commands, answering the same canonical
// JSON as dart_peer/lib/src/oracle.dart. oracle.test.ts compares these with
// the Dart peer's answers; a Dart driver can compare the reverse.

import {
  ChannelAddress,
  type MuxCommand,
  MuxControlMessage,
  MuxControlType,
  MuxCredit,
  MuxFrame,
  MuxIdent,
  MuxLimits,
  muxControlTypeName,
  Name,
  Status,
  StreamFrameDecoder,
  StreamFraming,
  TalkFrame,
  talkKindName,
} from '../../../src/node/index.ts';
import {
  ArgumentProblem,
  arrayArg,
  boolArg,
  bytesOf,
  errorJson,
  hex,
  intArg,
  type Json,
  requiredString,
  stringArg,
} from './protocol.ts';

/** The kinds {@link decodeKind} and {@link encodeKind} know. */
export const oracleKinds = ['name', 'status', 'address', 'mux', 'control', 'talk', 'stream'];

const muxCommandNames = ['data', 'open', 'close'] as const;

/**
 * Decodes [bytes] as [kind] with the TS codec. Throws what the codec throws
 * for malformed input, {@link ArgumentProblem} for an unknown kind.
 */
export function decodeKind(kind: string, bytes: Uint8Array): Json {
  switch (kind) {
    case 'name': {
      const n = Name.fromBytes(bytes);
      return { text: n.toString(), length: n.length, reencoded: hex(n.bytes) };
    }
    case 'status': {
      const s = Status.decode(bytes);
      const encoded = s.encode();
      return {
        code: s.code,
        reason: s.reason,
        body: hex(encoded.subarray(2)),
        reencoded: hex(encoded),
      };
    }
    case 'address': {
      const a = ChannelAddress.decode(bytes);
      return {
        type: a.type === undefined ? null : hex(a.type.bytes),
        instance: a.instance,
        shard: a.shard ?? null,
        host: a.host ?? null,
        payload: hex(a.payload),
        reencoded: hex(a.encode()),
      };
    }
    case 'mux': {
      const f = MuxFrame.decode(bytes);
      return {
        command: muxCommandNames[f.command],
        channelId: f.channelId,
        longForm: f.longForm,
        payload: hex(f.payload),
        reencoded: hex(f.encode()),
      };
    }
    case 'control': {
      const m = MuxControlMessage.decode(bytes);
      return {
        type: m.type,
        typeName: muxControlTypeName(m.type) ?? null,
        payload: hex(m.payload),
        fields: controlFields(m),
        reencoded: hex(m.encode()),
      };
    }
    case 'talk': {
      const f = TalkFrame.decode(bytes);
      return {
        kind: talkKindName(f.kind),
        procedure: f.procedure === undefined ? null : hex(f.procedure.bytes),
        requestId: f.requestId,
        responseId: f.responseId,
        stream: f.stream,
        window: f.window,
        bulk: f.bulk,
        payload: hex(f.payload),
        reencoded: hex(f.encode()),
      };
    }
    case 'stream': {
      const frames: string[] = [];
      const decoder = new StreamFrameDecoder();
      decoder.add(bytes, (f) => frames.push(hex(f)));
      return {
        preamble: decoder.preambleReceived,
        frames,
        buffered: decoder.bufferedBytes,
      };
    }
    default:
      throw new ArgumentProblem(`unknown oracle kind ${kind}`);
  }
}

/**
 * The typed fields of a control message of a known type, or null; a typed
 * payload that does not decode is reported as `error`.
 */
function controlFields(m: MuxControlMessage): Json | null {
  try {
    switch (m.type) {
      case MuxControlType.limits: {
        const l = MuxLimits.decode(m.payload);
        return {
          maxFrameSize: l.maxFrameSize,
          maxChannels: l.maxChannels,
          initialWindow: l.initialWindow,
        };
      }
      case MuxControlType.credit: {
        const c = MuxCredit.decode(m.payload);
        return { channelId: c.channelId, bytes: c.bytes };
      }
      case MuxControlType.goAway: {
        const s = m.goAwayStatus;
        return { code: s.code, reason: s.reason };
      }
      case MuxControlType.ident: {
        const i = MuxIdent.decode(m.payload);
        return {
          credential: hex(i.credential),
          intent: hex(i.intent),
          receiver: hex(i.receiver),
          proof: hex(i.proof),
        };
      }
      default:
        return null;
    }
  } catch (e) {
    return { error: errorJson(e) };
  }
}

/** Encodes [fields] as [kind] with the TS codec and answers the hex. */
export function encodeKind(kind: string, fields: Json): string {
  switch (kind) {
    case 'name':
      return hex(Name.of(requiredString(fields, 'text')).bytes);
    case 'status':
      return hex(
        new Status(intArg(fields, 'code') ?? 0, stringArg(fields, 'reason') ?? '').encode(),
      );
    case 'address': {
      const type = stringArg(fields, 'type');
      return hex(
        new ChannelAddress({
          type: type === undefined ? undefined : Name.fromBytes(bytesOf(type)),
          instance: intArg(fields, 'instance') ?? 0,
          shard: intArg(fields, 'shard'),
          host: stringArg(fields, 'host'),
          payload: bytesOf(fields['payload']),
        }).encode(),
      );
    }
    case 'mux': {
      const name = requiredString(fields, 'command');
      const command = muxCommandNames.indexOf(name as (typeof muxCommandNames)[number]);
      if (command < 0) {
        throw new ArgumentProblem(`unknown mux command ${name}`);
      }
      return hex(
        new MuxFrame(
          command as MuxCommand,
          intArg(fields, 'channelId') ?? 0,
          bytesOf(fields['payload']),
        ).encode(),
      );
    }
    case 'control':
      return hex(
        new MuxControlMessage(intArg(fields, 'type') ?? 0, bytesOf(fields['payload'])).encode(),
      );
    case 'stream': {
      const parts: Uint8Array[] = [];
      if (boolArg(fields, 'preamble', true)) {
        parts.push(StreamFraming.encodePreamble());
      }
      for (const f of arrayArg(fields, 'frames') ?? []) {
        parts.push(StreamFraming.encodeFrame(bytesOf(f)));
      }
      const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
      let at = 0;
      for (const p of parts) {
        out.set(p, at);
        at += p.length;
      }
      return hex(out);
    }
    default:
      throw new ArgumentProblem(`no encoder for oracle kind ${kind}`);
  }
}
