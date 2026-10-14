/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/// The interop services: fixed behaviour both peers implement identically
/// (README, "Services"), so a scenario can run against either.
library;

import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:crypto/crypto.dart';
import 'package:switchboard/switchboard.dart';

import 'client.dart';
import 'config.dart';
import 'context.dart';
import 'protocol.dart';

/// Registers [spec] on [node]; [client] dials what a `forward` service
/// forwards to.
void registerService(
  Switchboard node,
  ServiceSpec spec,
  PeerContext ctx,
  Client client,
) {
  final type = Name(spec.type);
  final ChannelHandler handler = switch (spec.behaviour) {
    'echo' => (incoming) => _serveEcho(incoming, spec, ctx),
    'raw' => (incoming) => _serveRaw(incoming, spec, ctx),
    // Accepts the channel and never reads it: its window fills up.
    'sink' => (incoming) {},
    'forward' => (incoming) => _serveForward(incoming, spec, ctx, client),
    _ => throw ArgumentError('unknown service behaviour ${spec.behaviour}'),
  };
  node.registerService(type, handler, instance: spec.instance);
}

/// `forward`: every message of the channel is forwarded with
/// [forwardMessage] on a Talk channel of its own to the service the spec's
/// `target` names (at its `endpoint`, by default this peer's first
/// listener), and every message from there back the other way; a close on
/// either side closes the other with its status.
void _serveForward(
  IncomingChannel incoming,
  ServiceSpec spec,
  PeerContext ctx,
  Client client,
) {
  final front = incoming.talk(options: ctx.config.talk);
  unawaited(() async {
    final TalkChannel back;
    try {
      back = await client.openTalk(
        spec.targetEndpoint ?? ctx.endpoints.first,
        ChannelAddress(
          type: Name(spec.targetType ?? 'echo'),
          instance: spec.targetInstance,
        ).encode(),
      );
    } on Object catch (e) {
      await front.close(
        e is SwitchboardException
            ? e.status
            : Status.of(StatusCode.unavailable, '$e'),
      );
      return;
    }
    front.messages.listen(
      (m) => unawaited(forwardMessage(m, back)),
      onError: (Object _) {},
    );
    back.messages.listen(
      (m) => unawaited(forwardMessage(m, front)),
      onError: (Object _) {},
    );
    unawaited(front.done.then((s) => back.close(s)));
    unawaited(back.done.then((s) => front.close(s)));
  }());
}

/// `raw`: the channel becomes a handle; its subframes and its end are
/// events, and `channel.send` and `channel.close` drive it.
void _serveRaw(IncomingChannel incoming, ServiceSpec spec, PeerContext ctx) {
  final connection = ctx.trackConnection(incoming.connection);
  final handle = ctx.trackChannel(incoming.channel, listen: false);
  ctx.out.event('channel.incoming', {
    'channel': handle,
    'connection': connection,
    'service': spec.type,
    'id': incoming.channel.id,
    ..._addressJson(incoming.address),
  });
  incoming.channel.stream.listen(
    (subframe) => ctx.out.event('channel.data', {
      'channel': handle,
      'hex': hex(subframe),
    }),
    onError: (Object _) {},
  );
}

Map<String, Object?> _addressJson(ChannelAddress a) => {
  'type': a.type?.toString(),
  'instance': a.instance,
  'shard': a.shard,
  'host': a.host,
  'payload': hex(a.payload),
};

/// `echo`: the interop procedures over Talk.
void _serveEcho(IncomingChannel incoming, ServiceSpec spec, PeerContext ctx) {
  final talk = incoming.talk(options: ctx.config.talk);
  talk.messages.listen(
    (m) => unawaited(
      _echo(
        m,
        incoming,
        talk,
        spec,
        ctx,
      ).catchError((Object e, StackTrace st) => _failed(m, e, st, ctx)),
    ),
    onError: (Object _) {},
  );
}

Future<void> _failed(
  TalkMessage m,
  Object error,
  StackTrace stack,
  PeerContext ctx,
) async {
  if (error is SwitchboardException) {
    // The requester went away or cancelled; nothing to answer.
    return;
  }
  ctx.reportError(error, stack);
  if (m.canReply) {
    await m.replyAbort(Status.of(StatusCode.internal, '$error'));
  }
}

Future<void> _echo(
  TalkMessage m,
  IncomingChannel incoming,
  TalkChannel talk,
  ServiceSpec spec,
  PeerContext ctx,
) async {
  final payload = await m.payloadBytes();
  if (!m.expectsReply) {
    ctx.out.event('service.message', {
      'service': spec.type,
      'procedure': m.procedureName,
      'bulk': m.isBulk,
      'payload': hex(payload),
    });
    return;
  }
  final text = utf8.decode(payload, allowMalformed: true);
  final (verb, rest) = _split(text);
  switch (m.procedureName) {
    case 'ECHO':
      await m.reply(payload);
    case 'UPPER':
      await m.reply(asciiUpper(payload));
    case 'SIZE':
      await m.reply(
        utf8.encode('${payload.length} ${sha256.convert(payload)}'),
      );
    case 'BIG':
      await m.reply(pattern(int.parse(text.trim())));
    case 'BIGSTRM':
      // A bulk payload streamed, its length not declared.
      await m.replyBulk(_chunks(pattern(int.parse(text.trim())), 1000));
    case 'COUNT':
      final n = int.parse(text.trim());
      for (var i = 1; i <= n; i++) {
        await m.replyItem(utf8.encode('$i'));
      }
      await m.reply(utf8.encode('done'));
    case 'BIGITEMS':
      final n = int.parse(verb);
      final item = pattern(int.parse(rest.trim()));
      for (var i = 1; i <= n; i++) {
        await m.replyItem(item);
      }
      await m.reply(utf8.encode('done'));
    case 'HOW':
      await m.reply(
        utf8.encode('${m.isBulk ? 'bulk' : 'inline'} ${payload.length}'),
      );
    case 'EXTEND':
      final ms = int.parse(verb);
      // Twice the delay, so the reply is well within the responder's own
      // deadline.
      await m.extend(deadline: Duration(milliseconds: 2 * ms));
      await Future<void>.delayed(Duration(milliseconds: ms));
      await m.reply(utf8.encode('extended'));
    case 'STATUS':
      await m.replyAbort(Status.decode(payload));
    case 'ABORT':
      await m.replyAbort(Status(int.parse(verb), rest));
    case 'SLOW':
      await Future<void>.delayed(Duration(milliseconds: int.parse(verb)));
      await m.reply(utf8.encode('slow'));
    case 'STALL':
      // Never answers; the requester times out or cancels.
      break;
    case 'NOTIFY':
      await talk.send('NOTE', payload);
      await m.reply(utf8.encode('sent'));
    case 'CLOSE':
      await m.reply(utf8.encode('closing'));
      await talk.close(Status(int.parse(verb), rest));
    case 'GOAWAY':
      await m.reply(utf8.encode('going'));
      unawaited(incoming.connection.goAway());
    case 'WHOAMI':
      await m.reply(utf8.encode(incoming.peerIdentity?.identity ?? ''));
    default:
      await m.replyAbort(Status.of(StatusCode.unimplemented));
  }
}

/// [bytes] in pieces of [size].
Stream<List<int>> _chunks(Uint8List bytes, int size) async* {
  for (var at = 0; at < bytes.length; at += size) {
    yield bytes.sublist(
      at,
      at + size < bytes.length ? at + size : bytes.length,
    );
  }
}

/// The first space separated word of [text] and the rest after the space.
(String, String) _split(String text) {
  final at = text.indexOf(' ');
  return at < 0 ? (text, '') : (text.substring(0, at), text.substring(at + 1));
}

/// [bytes] with ASCII `a` to `z` upper-cased and every other byte as is.
Uint8List asciiUpper(Uint8List bytes) => Uint8List.fromList([
  for (final b in bytes) b >= 0x61 && b <= 0x7A ? b - 0x20 : b,
]);

/// The pattern `BIG` answers: byte i is `(i * 31) & 0xFF`.
Uint8List pattern(int length) {
  final out = Uint8List(length);
  for (var i = 0; i < length; i++) {
    out[i] = (i * 31) & 0xFF;
  }
  return out;
}
