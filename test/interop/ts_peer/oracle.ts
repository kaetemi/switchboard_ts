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
  AssignRequest,
  AssignResponse,
  ByteFormatError,
  ChannelAddress,
  ClaimRequest,
  ClaimResponse,
  ConnectRequest,
  ConnectResponse,
  Credential,
  DialBackRequest,
  DrainRequest,
  decodeCloseBody,
  decodeDownEvent,
  decodeOpenBody,
  decodeServiceRecord,
  decodeUpEvent,
  encodeQuicVarint,
  encodeServiceEvent,
  encodeServiceRecord,
  ForwardRequest,
  fnv1a32,
  HoldingRequest,
  HoldingResponse,
  LocateRequest,
  LocateResponse,
  MigrateRequest,
  MovedStatus,
  type MuxCommand,
  MuxControlMessage,
  MuxControlType,
  MuxCredit,
  MuxFrame,
  MuxIdent,
  MuxLimits,
  muxControlTypeName,
  Name,
  PhaseItem,
  ProtocolError,
  QuicHello,
  quicErrorCode,
  quicHeaderCommand,
  quicHeaderLength,
  quicRecordHeaderSize,
  RegisterRequest,
  RegisterResponse,
  ReleaseRequest,
  ResumeRequest,
  rawQuicExporterLabel,
  readQuicRecordHeader,
  type ServiceRecord,
  SlotItem,
  SlotSpace,
  SlotsRequest,
  Status,
  StreamFrameDecoder,
  StreamFraming,
  slotForKey,
  TalkFrame,
  talkKindName,
  UnregisterRequest,
  utf8Encode,
  WatchRequest,
  webTransportCodeFromHttp3,
  webTransportCodeToHttp3,
  webTransportExporterContext,
  webTransportExporterLabel,
  webTransportTlsExporterLabel,
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
  statusJson,
  stringArg,
} from './protocol.ts';

/** The naming service payloads (`_ns`): one kind per message. */
export const namingKinds = [
  'record',
  'register',
  'registerResponse',
  'unregister',
  'watch',
  'up',
  'down',
  'connect',
  'connectResponse',
  'dialBack',
] as const;

/** The sharding payloads: one kind per message, and `moved` for a MOVED or RELOCATED status payload. */
export const shardingKinds = [
  'slotSpace',
  'slots',
  'holding',
  'holdingResponse',
  'claim',
  'claimResponse',
  'release',
  'locate',
  'locateResponse',
  'migrate',
  'phase',
  'assign',
  'assignResponse',
  'drain',
  'forward',
  'resume',
  'slot',
  'moved',
] as const;

/** The slot counts the `slotKey` kind maps a key to. */
export const slotKeyCounts = [1, 7, 16, 1000, 65536, 0xffffffff];

/** The kinds {@link decodeKind} and {@link encodeKind} know. */
export const oracleKinds: readonly string[] = [
  'name',
  'status',
  'address',
  'mux',
  'control',
  'talk',
  'stream',
  ...namingKinds,
  ...shardingKinds,
  'credential',
  'slotKey',
  'quicRecords',
  'quicHello',
  'quicCode',
  'exporter',
];

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
    case 'credential': {
      const c = Credential.decode(bytes);
      return {
        version: c.version,
        kind: c.kind,
        keyId: hex(c.keyId.bytes),
        identity: c.identity,
        issuedAt: c.issuedAt,
        expiresAt: c.expiresAt,
        holderKey: c.holderKey === undefined ? null : hex(c.holderKey),
        scopes: c.scopes.map((s) => ({ right: s.right, pattern: hex(s.pattern.bytes) })),
        signatureKind: c.signatureKind,
        signature: hex(c.signature),
        signed: hex(c.signedBytes),
        reencoded: hex(c.encode()),
      };
    }
    case 'slotKey':
      return { fnv1a32: fnv1a32(bytes), slots: slotKeyCounts.map((n) => slotForKey(bytes, n)) };
    case 'quicRecords':
      return quicRecords(bytes);
    case 'quicHello': {
      const h = QuicHello.decode(bytes);
      return {
        recordLimit: h.recordLimit,
        channels: h.channels,
        exporter: h.exporter,
        reencoded: hex(h.encode()),
      };
    }
    case 'quicCode': {
      // A status code, u16 little endian: its error code on raw QUIC, its
      // WebTransport code as HTTP/3 has it, both as QUIC varints.
      if (bytes.length !== 2) {
        throw new ByteFormatError('a status code is 2 bytes');
      }
      const code = (bytes[0] ?? 0) + (bytes[1] ?? 0) * 256;
      const http3 = webTransportCodeToHttp3(code);
      let raw: number | null;
      try {
        raw = quicErrorCode(new Status(code));
      } catch {
        raw = null;
      }
      return {
        raw,
        rawVarint: raw === null ? null : hex(encodeQuicVarint(raw)),
        http3,
        http3Varint: hex(encodeQuicVarint(http3)),
        back: webTransportCodeFromHttp3(http3) ?? null,
      };
    }
    case 'exporter': {
      // A WebTransport session id, u64 big endian: the TLS exporter context
      // of its exporter-bound IDENT, and the labels.
      if (bytes.length !== 8) {
        throw new ByteFormatError('a session id is 8 bytes');
      }
      const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
      const id = view.getUint32(0) * 0x100000000 + view.getUint32(4);
      return {
        context: hex(webTransportExporterContext(id)),
        webTransportLabel: hex(utf8Encode(webTransportTlsExporterLabel)),
        rawQuicLabel: hex(utf8Encode(rawQuicExporterLabel)),
        identLabel: hex(utf8Encode(webTransportExporterLabel)),
      };
    }
    default:
      if (isNamingKind(kind)) {
        const [fields, reencoded] = namingDecoders[kind](bytes);
        return { ...fields, reencoded: hex(reencoded) };
      }
      throw new ArgumentProblem(`unknown oracle kind ${kind}`);
  }
}

/**
 * A byte stream of QUIC records (a channel stream's, from its first byte):
 * every whole record's command and body, the channel id and open payload of
 * an OPEN, the status of a CLOSE, and how many bytes of an unfinished record
 * are left over. A reserved command, or a body an OPEN or CLOSE cannot have,
 * fails like the binding's reader.
 */
function quicRecords(bytes: Uint8Array): Json {
  const records: Json[] = [];
  let at = 0;
  while (bytes.length - at >= quicRecordHeaderSize) {
    const header = readQuicRecordHeader(bytes, at);
    const command = quicHeaderCommand(header);
    if (command > 2) {
      throw new ProtocolError('reserved record command');
    }
    const length = quicHeaderLength(header);
    if (bytes.length - at - quicRecordHeaderSize < length) {
      break;
    }
    const body = bytes.subarray(at + quicRecordHeaderSize, at + quicRecordHeaderSize + length);
    at += quicRecordHeaderSize + length;
    const record: Json = { command: recordCommandNames[command] ?? 'data', body: hex(body) };
    if (command === 1) {
      const { id, openPayload } = decodeOpenBody(body);
      record['channelId'] = id;
      record['openPayload'] = hex(openPayload);
    } else if (command === 2) {
      Object.assign(record, statusJson(decodeCloseBody(body)));
    }
    records.push(record);
  }
  return { records, buffered: bytes.length - at };
}

/** The record commands by code, as the Dart enum names them. */
const recordCommandNames = ['data', 'open', 'close'];

const nameHex = (n: Name): string => hex(n.bytes);

function recordJson(r: ServiceRecord): Json {
  return {
    type: nameHex(r.address.type),
    instance: r.address.instance,
    endpoints: [...r.endpoints],
    metadata: hex(r.metadata),
  };
}

function spaceJson(s: SlotSpace): Json {
  return { type: nameHex(s.type), count: s.count, mode: s.mode, lazy: s.lazy, shared: s.shared };
}

type NamingKind = (typeof namingKinds)[number] | (typeof shardingKinds)[number];

function isNamingKind(kind: string): kind is NamingKind {
  return (
    (namingKinds as readonly string[]).includes(kind) ||
    (shardingKinds as readonly string[]).includes(kind)
  );
}

/** Each naming and sharding message: its fields and the message encoded again. */
const namingDecoders: Record<NamingKind, (bytes: Uint8Array) => [Json, Uint8Array]> = {
  record: (b) => {
    const m = decodeServiceRecord(b);
    return [recordJson(m), encodeServiceRecord(m)];
  },
  register: (b) => {
    const m = RegisterRequest.decode(b);
    return [
      {
        type: nameHex(m.type),
        requestedInstance: m.requestedInstance,
        endpoints: [...m.endpoints],
        metadata: hex(m.metadata),
      },
      m.encode(),
    ];
  },
  registerResponse: (b) => {
    const m = RegisterResponse.decode(b);
    return [{ instance: m.instance }, m.encode()];
  },
  unregister: (b) => {
    const m = UnregisterRequest.decode(b);
    return [{ type: nameHex(m.type), instance: m.instance }, m.encode()];
  },
  watch: (b) => {
    const m = WatchRequest.decode(b);
    return [{ type: m.type === undefined ? null : nameHex(m.type) }, m.encode()];
  },
  up: (b) => {
    const e = decodeUpEvent(b);
    return [{ up: e.up, record: recordJson(e.record) }, encodeServiceEvent(e)];
  },
  down: (b) => {
    const e = decodeDownEvent(b);
    return [{ up: e.up, record: recordJson(e.record) }, encodeServiceEvent(e)];
  },
  connect: (b) => {
    const m = ConnectRequest.decode(b);
    return [
      {
        type: nameHex(m.type),
        instance: m.instance,
        endpoint: m.endpoint,
        intent: hex(m.intent),
      },
      m.encode(),
    ];
  },
  connectResponse: (b) => {
    const m = ConnectResponse.decode(b);
    return [{ identity: m.identity }, m.encode()];
  },
  dialBack: (b) => {
    const m = DialBackRequest.decode(b);
    return [{ requester: m.requester, endpoint: m.endpoint, intent: hex(m.intent) }, m.encode()];
  },
  slotSpace: (b) => {
    const m = SlotSpace.decode(b);
    return [spaceJson(m), m.encode()];
  },
  slots: (b) => {
    const m = SlotsRequest.decode(b);
    return [{ space: spaceJson(m.space), capacity: m.capacity }, m.encode()];
  },
  holding: (b) => {
    const m = HoldingRequest.decode(b);
    return [{ type: nameHex(m.type), slots: [...m.slots] }, m.encode()];
  },
  holdingResponse: (b) => {
    const m = HoldingResponse.decode(b);
    return [{ discard: [...m.discard] }, m.encode()];
  },
  claim: (b) => {
    const m = ClaimRequest.decode(b);
    return [
      { type: nameHex(m.type), slot: m.slot, holding: m.holding, epoch: m.epoch },
      m.encode(),
    ];
  },
  claimResponse: (b) => {
    const m = ClaimResponse.decode(b);
    return [{ epoch: m.epoch }, m.encode()];
  },
  release: (b) => {
    const m = ReleaseRequest.decode(b);
    return [{ type: nameHex(m.type), slot: m.slot, keepStorage: m.keepStorage }, m.encode()];
  },
  locate: (b) => {
    const m = LocateRequest.decode(b);
    return [{ type: nameHex(m.type), slot: m.slot }, m.encode()];
  },
  locateResponse: (b) => {
    const m = LocateResponse.decode(b);
    return [{ state: m.state, owner: m.owner, epoch: m.epoch }, m.encode()];
  },
  migrate: (b) => {
    const m = MigrateRequest.decode(b);
    return [{ type: nameHex(m.type), slot: m.slot, to: m.to }, m.encode()];
  },
  phase: (b) => {
    const m = PhaseItem.decode(b);
    return [{ phase: m.phase }, m.encode()];
  },
  assign: (b) => {
    const m = AssignRequest.decode(b);
    return [
      {
        type: nameHex(m.type),
        slot: m.slot,
        epoch: m.epoch,
        holder: m.holder,
        shared: m.shared,
      },
      m.encode(),
    ];
  },
  assignResponse: (b) => {
    const m = AssignResponse.decode(b);
    return [{ notHolding: m.notHolding }, m.encode()];
  },
  drain: (b) => {
    const m = DrainRequest.decode(b);
    return [{ type: nameHex(m.type), slot: m.slot, epoch: m.epoch, to: m.to }, m.encode()];
  },
  forward: (b) => {
    const m = ForwardRequest.decode(b);
    return [{ type: nameHex(m.type), slot: m.slot, epoch: m.epoch, to: m.to }, m.encode()];
  },
  resume: (b) => {
    const m = ResumeRequest.decode(b);
    return [{ type: nameHex(m.type), slot: m.slot, epoch: m.epoch }, m.encode()];
  },
  slot: (b) => {
    const m = SlotItem.decode(b);
    const e = m.entry;
    return [
      {
        type: nameHex(m.type),
        slot: m.slot,
        entry: { state: e.state, owner: e.owner, to: e.to, holder: e.holder, epoch: e.epoch },
      },
      m.encode(),
    ];
  },
  moved: (b) => {
    const m = MovedStatus.parse(b);
    const relocated = b.length >= 2 && (b[0] ?? 0) + (b[1] ?? 0) * 256 === 38;
    return [{ owner: m.owner, epoch: m.epoch, reason: m.reason }, m.encode({ relocated })];
  },
};

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
      case MuxControlType.goAway:
        return statusJson(m.goAwayStatus);
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
