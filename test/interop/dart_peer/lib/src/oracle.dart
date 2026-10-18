/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/// The codec oracle: the reference decoders and encoders behind the
/// `decode`, `decodeMany` and `encode` commands. Every decoded value is a
/// canonical JSON object, the same one the TS side builds from its own
/// codec, plus `reencoded`, the hex of encoding the decoded value again.
library;

import 'dart:convert';
import 'dart:typed_data';

import 'package:switchboard/core.dart';
import 'package:switchboard_quic/core.dart';

import 'protocol.dart';

/// The kinds [decodeKind] and [encodeKind] know.
const List<String> oracleKinds = [
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

/// The naming service payloads (`_ns`): one kind per message.
const List<String> namingKinds = [
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
];

/// The sharding payloads: one kind per message, and `moved` for the
/// fields of a `MOVED` or `RELOCATED` status payload.
const List<String> shardingKinds = [
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
];

/// The slot counts the `slotKey` kind maps a key to.
const List<int> slotKeyCounts = [1, 7, 16, 1000, 65536, 0xFFFFFFFF];

/// Decodes [bytes] as [kind] with the reference codec. Throws what the codec
/// throws for malformed input, [ArgumentError] for an unknown kind.
Map<String, Object?> decodeKind(String kind, Uint8List bytes) {
  switch (kind) {
    case 'name':
      final n = Name.fromBytes(bytes);
      return {
        'text': n.toString(),
        'length': n.length,
        'reencoded': hex(n.bytes),
      };
    case 'status':
      final s = Status.decode(bytes);
      final encoded = s.encode();
      return {
        'code': s.code,
        'reason': s.reason,
        'body': hex(encoded.sublist(2)),
        'reencoded': hex(encoded),
      };
    case 'address':
      final a = ChannelAddress.decode(bytes);
      return {
        'type': a.type == null ? null : hex(a.type!.bytes),
        'instance': a.instance,
        'shard': a.shard,
        'host': a.host,
        'payload': hex(a.payload),
        'reencoded': hex(a.encode()),
      };
    case 'mux':
      final f = MuxFrame.decode(bytes);
      return {
        'command': f.command.name,
        'channelId': f.channelId,
        'longForm': f.longForm,
        'payload': hex(f.payload),
        'reencoded': hex(f.encode()),
      };
    case 'control':
      final m = MuxControlMessage.decode(bytes);
      return {
        'type': m.type,
        'typeName': m.knownType?.name,
        'payload': hex(m.payload),
        'fields': _controlFields(m),
        'reencoded': hex(m.encode()),
      };
    case 'talk':
      final f = TalkFrame.decode(bytes);
      return {
        'kind': f.kind.name,
        'procedure': f.procedure == null ? null : hex(f.procedure!.bytes),
        'requestId': f.requestId,
        'responseId': f.responseId,
        'stream': f.stream,
        'window': f.window,
        'bulk': f.bulk,
        'payload': hex(f.payload),
        'reencoded': hex(f.encode()),
      };
    case 'stream':
      final frames = <String>[];
      final decoder = StreamFrameDecoder()
        ..add(bytes, (f) => frames.add(hex(f)));
      return {
        'preamble': decoder.preambleReceived,
        'frames': frames,
        'buffered': decoder.bufferedBytes,
      };
    case 'credential':
      final c = Credential.decode(bytes);
      return {
        'version': c.version,
        'kind': c.kind.code,
        'keyId': hex(c.keyId.bytes),
        'identity': c.identity,
        'issuedAt': c.issuedAt,
        'expiresAt': c.expiresAt,
        'holderKey': c.holderKey == null ? null : hex(c.holderKey!),
        'scopes': [
          for (final s in c.scopes)
            {'right': s.right.code, 'pattern': hex(s.pattern.bytes)},
        ],
        'signatureKind': c.signatureKind.code,
        'signature': hex(c.signature),
        'signed': hex(c.signedBytes),
        'reencoded': hex(c.encode()),
      };
    case 'slotKey':
      return {
        'fnv1a32': fnv1a32(bytes),
        'slots': [for (final n in slotKeyCounts) slotForKey(bytes, n)],
      };
    case 'quicRecords':
      return _quicRecords(bytes);
    case 'quicHello':
      final h = QuicHello.decode(bytes);
      return {
        'recordLimit': h.recordLimit,
        'channels': h.channels,
        'exporter': h.exporter,
        'reencoded': hex(h.encode()),
      };
    case 'quicCode':
      // A status code, u16 little endian: its error code on raw QUIC, its
      // WebTransport code as HTTP/3 has it, both as QUIC varints.
      if (bytes.length != 2) {
        throw const FormatException('a status code is 2 bytes');
      }
      final code = bytes[0] + bytes[1] * 256;
      final http3 = webTransportCodeToHttp3(code);
      int? raw;
      try {
        raw = quicErrorCode(Status(code));
      } on ArgumentError catch (_) {
        raw = null;
      }
      return {
        'raw': raw,
        'rawVarint': raw == null ? null : hex(encodeQuicVarint(raw)),
        'http3': http3,
        'http3Varint': hex(encodeQuicVarint(http3)),
        'back': webTransportCodeFromHttp3(http3),
      };
    case 'exporter':
      // A WebTransport session id, u64 big endian: the TLS exporter context
      // of its exporter-bound IDENT, and the labels.
      if (bytes.length != 8) {
        throw const FormatException('a session id is 8 bytes');
      }
      final id = ByteData.sublistView(bytes).getUint64(0);
      return {
        'context': hex(webTransportExporterContext(id)),
        'webTransportLabel': hex(utf8.encode(webTransportExporterLabel)),
        'rawQuicLabel': hex(utf8.encode(rawQuicExporterLabel)),
        'identLabel': hex(utf8.encode(webTransportIdentLabel)),
      };
    default:
      if (namingKinds.contains(kind) || shardingKinds.contains(kind)) {
        return _decodeNaming(kind, bytes);
      }
      throw ArgumentError('unknown oracle kind $kind');
  }
}

/// A byte stream of QUIC records (a channel stream's, from its first
/// byte): every whole record's command and body, the channel id and open
/// payload of an `OPEN`, the status of a `CLOSE`, and how many bytes of an
/// unfinished record are left over. A reserved command, or a body an `OPEN`
/// or `CLOSE` cannot have, fails like the binding's reader.
Map<String, Object?> _quicRecords(Uint8List bytes) {
  final records = <Map<String, Object?>>[];
  var at = 0;
  while (bytes.length - at >= quicRecordHeaderSize) {
    final header = readQuicRecordHeader(bytes, at);
    final command = QuicRecordCommand.fromCode(quicHeaderCommand(header));
    if (command == null) {
      throw ProtocolException('reserved record command');
    }
    final length = quicHeaderLength(header);
    if (bytes.length - at - quicRecordHeaderSize < length) {
      break;
    }
    final body = Uint8List.sublistView(
      bytes,
      at + quicRecordHeaderSize,
      at + quicRecordHeaderSize + length,
    );
    at += quicRecordHeaderSize + length;
    final record = <String, Object?>{
      'command': command.name,
      'body': hex(body),
    };
    switch (command) {
      case QuicRecordCommand.open:
        final (id, payload) = decodeOpenBody(body);
        record['channelId'] = id;
        record['openPayload'] = hex(payload);
      case QuicRecordCommand.close:
        record.addAll(statusJson(decodeCloseBody(body)));
      case QuicRecordCommand.data:
        break;
    }
    records.add(record);
  }
  return {'records': records, 'buffered': bytes.length - at};
}

String _name(Name n) => hex(n.bytes);

Map<String, Object?> _record(ServiceRecord r) => {
  'type': _name(r.address.type),
  'instance': r.address.instance,
  'endpoints': [for (final e in r.endpoints) '$e'],
  'metadata': hex(r.metadata),
};

Map<String, Object?> _space(SlotSpace s) => {
  'type': _name(s.type),
  'count': s.count,
  'mode': s.mode.code,
  'lazy': s.lazy,
  'shared': s.shared,
};

/// A naming or sharding payload decoded as the message [kind] names, with
/// its fields and the message encoded again.
Map<String, Object?> _decodeNaming(String kind, Uint8List bytes) {
  final (Map<String, Object?> fields, Uint8List reencoded) = switch (kind) {
    'record' => () {
      final m = ServiceRecord.decode(bytes);
      return (_record(m), m.encode());
    }(),
    'register' => () {
      final m = RegisterRequest.decode(bytes);
      return (
        {
          'type': _name(m.type),
          'requestedInstance': m.requestedInstance,
          'endpoints': [for (final e in m.endpoints) '$e'],
          'metadata': hex(m.metadata),
        },
        m.encode(),
      );
    }(),
    'registerResponse' => () {
      final m = RegisterResponse.decode(bytes);
      return (<String, Object?>{'instance': m.instance}, m.encode());
    }(),
    'unregister' => () {
      final m = UnregisterRequest.decode(bytes);
      return (
        <String, Object?>{'type': _name(m.type), 'instance': m.instance},
        m.encode(),
      );
    }(),
    'watch' => () {
      final m = WatchRequest.decode(bytes);
      return (
        <String, Object?>{'type': m.type == null ? null : _name(m.type!)},
        m.encode(),
      );
    }(),
    'up' || 'down' => () {
      final e = kind == 'up'
          ? ServiceEvent.decodeUp(bytes)
          : ServiceEvent.decodeDown(bytes);
      return (
        <String, Object?>{'up': e.up, 'record': _record(e.record)},
        e.encode(),
      );
    }(),
    'connect' => () {
      final m = ConnectRequest.decode(bytes);
      return (
        <String, Object?>{
          'type': _name(m.type),
          'instance': m.instance,
          'endpoint': '${m.endpoint}',
          'intent': hex(m.intent),
        },
        m.encode(),
      );
    }(),
    'connectResponse' => () {
      final m = ConnectResponse.decode(bytes);
      return (<String, Object?>{'identity': m.identity}, m.encode());
    }(),
    'dialBack' => () {
      final m = DialBackRequest.decode(bytes);
      return (
        <String, Object?>{
          'requester': m.requester,
          'endpoint': '${m.endpoint}',
          'intent': hex(m.intent),
        },
        m.encode(),
      );
    }(),
    'slotSpace' => () {
      final m = SlotSpace.decode(bytes);
      return (_space(m), m.encode());
    }(),
    'slots' => () {
      final m = SlotsRequest.decode(bytes);
      return (
        <String, Object?>{'space': _space(m.space), 'capacity': m.capacity},
        m.encode(),
      );
    }(),
    'holding' => () {
      final m = HoldingRequest.decode(bytes);
      return (
        <String, Object?>{'type': _name(m.type), 'slots': m.slots},
        m.encode(),
      );
    }(),
    'holdingResponse' => () {
      final m = HoldingResponse.decode(bytes);
      return (<String, Object?>{'discard': m.discard}, m.encode());
    }(),
    'claim' => () {
      final m = ClaimRequest.decode(bytes);
      return (
        <String, Object?>{
          'type': _name(m.type),
          'slot': m.slot,
          'holding': m.holding,
          'epoch': m.epoch,
        },
        m.encode(),
      );
    }(),
    'claimResponse' => () {
      final m = ClaimResponse.decode(bytes);
      return (<String, Object?>{'epoch': m.epoch}, m.encode());
    }(),
    'release' => () {
      final m = ReleaseRequest.decode(bytes);
      return (
        <String, Object?>{
          'type': _name(m.type),
          'slot': m.slot,
          'keepStorage': m.keepStorage,
        },
        m.encode(),
      );
    }(),
    'locate' => () {
      final m = LocateRequest.decode(bytes);
      return (
        <String, Object?>{'type': _name(m.type), 'slot': m.slot},
        m.encode(),
      );
    }(),
    'locateResponse' => () {
      final m = LocateResponse.decode(bytes);
      return (
        <String, Object?>{
          'state': m.state.code,
          'owner': m.owner,
          'epoch': m.epoch,
        },
        m.encode(),
      );
    }(),
    'migrate' => () {
      final m = MigrateRequest.decode(bytes);
      return (
        <String, Object?>{'type': _name(m.type), 'slot': m.slot, 'to': m.to},
        m.encode(),
      );
    }(),
    'phase' => () {
      final m = PhaseItem.decode(bytes);
      return (<String, Object?>{'phase': m.phase.code}, m.encode());
    }(),
    'assign' => () {
      final m = AssignRequest.decode(bytes);
      return (
        <String, Object?>{
          'type': _name(m.type),
          'slot': m.slot,
          'epoch': m.epoch,
          'holder': m.holder,
          'shared': m.shared,
        },
        m.encode(),
      );
    }(),
    'assignResponse' => () {
      final m = AssignResponse.decode(bytes);
      return (<String, Object?>{'notHolding': m.notHolding}, m.encode());
    }(),
    'drain' => () {
      final m = DrainRequest.decode(bytes);
      return (
        <String, Object?>{
          'type': _name(m.type),
          'slot': m.slot,
          'epoch': m.epoch,
          'to': m.to,
        },
        m.encode(),
      );
    }(),
    'forward' => () {
      final m = ForwardRequest.decode(bytes);
      return (
        <String, Object?>{
          'type': _name(m.type),
          'slot': m.slot,
          'epoch': m.epoch,
          'to': m.to,
        },
        m.encode(),
      );
    }(),
    'resume' => () {
      final m = ResumeRequest.decode(bytes);
      return (
        <String, Object?>{
          'type': _name(m.type),
          'slot': m.slot,
          'epoch': m.epoch,
        },
        m.encode(),
      );
    }(),
    'slot' => () {
      final m = SlotItem.decode(bytes);
      final e = m.entry;
      return (
        <String, Object?>{
          'type': _name(m.type),
          'slot': m.slot,
          'entry': {
            'state': e.state.code,
            'owner': e.owner,
            'to': e.to,
            'holder': e.holder,
            'epoch': e.epoch,
          },
        },
        m.encode(),
      );
    }(),
    'moved' => () {
      final m = MovedStatus.parse(bytes);
      final relocated = bytes.length >= 2 && bytes[0] + bytes[1] * 256 == 38;
      return (
        <String, Object?>{
          'owner': m.owner,
          'epoch': m.epoch,
          'reason': m.reason,
        },
        m.encode(relocated: relocated),
      );
    }(),
    _ => throw ArgumentError('unknown oracle kind $kind'),
  };
  return {...fields, 'reencoded': hex(reencoded)};
}

/// The typed fields of a control message of a known type, or null; a
/// typed payload that does not decode is reported as `error`.
Map<String, Object?>? _controlFields(MuxControlMessage m) {
  try {
    switch (m.knownType) {
      case MuxControlType.limits:
        final l = MuxLimits.decode(m.payload);
        return {
          'maxFrameSize': l.maxFrameSize,
          'maxChannels': l.maxChannels,
          'initialWindow': l.initialWindow,
        };
      case MuxControlType.credit:
        final c = MuxCredit.decode(m.payload);
        return {'channelId': c.channelId, 'bytes': c.bytes};
      case MuxControlType.goAway:
        return statusJson(m.goAwayStatus);
      case MuxControlType.ident:
        final i = MuxIdent.decode(m.payload);
        return {
          'credential': hex(i.credential),
          'intent': hex(i.intent),
          'receiver': hex(i.receiver),
          'proof': hex(i.proof),
        };
      case MuxControlType.ping ||
          MuxControlType.pong ||
          MuxControlType.nonce ||
          null:
        return null;
    }
  } on Object catch (e) {
    return {'error': errorJson(e)};
  }
}

/// Encodes [fields] as [kind] with the reference codec and answers the hex.
String encodeKind(String kind, Map<String, Object?> fields) {
  switch (kind) {
    case 'name':
      return hex(Name(requiredString(fields, 'text')).bytes);
    case 'status':
      return hex(
        Status(
          intArg(fields, 'code') ?? 0,
          stringArg(fields, 'reason') ?? '',
        ).encode(),
      );
    case 'address':
      final type = stringArg(fields, 'type');
      return hex(
        ChannelAddress(
          type: type == null ? null : Name.fromBytes(hexBytes(type)),
          instance: intArg(fields, 'instance') ?? 0,
          shard: intArg(fields, 'shard'),
          host: stringArg(fields, 'host'),
          payload: bytesOf(fields['payload']),
        ).encode(),
      );
    case 'mux':
      final command = MuxCommand.values.byName(
        requiredString(fields, 'command'),
      );
      return hex(
        MuxFrame(
          command,
          intArg(fields, 'channelId') ?? 0,
          bytesOf(fields['payload']),
        ).encode(),
      );
    case 'control':
      return hex(
        MuxControlMessage(
          intArg(fields, 'type') ?? 0,
          bytesOf(fields['payload']),
        ).encode(),
      );
    case 'stream':
      final frames = fields['frames'];
      final out = BytesBuilder(copy: false);
      if (boolArg(fields, 'preamble', fallback: true)) {
        out.add(StreamFraming.encodePreamble());
      }
      if (frames is List<Object?>) {
        for (final f in frames) {
          out.add(StreamFraming.encodeFrame(bytesOf(f)));
        }
      }
      return hex(out.takeBytes());
    default:
      throw ArgumentError('no encoder for oracle kind $kind');
  }
}
