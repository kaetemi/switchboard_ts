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

import 'dart:typed_data';

import 'package:switchboard/core.dart';

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
];

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
    default:
      throw ArgumentError('unknown oracle kind $kind');
  }
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
