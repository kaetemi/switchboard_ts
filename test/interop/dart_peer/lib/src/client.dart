/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/// The peer as the initiator (README, "Client commands"): connections,
/// channels and Talk channels to the other peer's listeners, as handles.
library;

import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:crypto/crypto.dart';
import 'package:switchboard/switchboard.dart';
import 'package:switchboard_quic/switchboard_quic.dart';
import 'package:switchboard_quic/switchboard_quic_native.dart';

import 'context.dart';
import 'protocol.dart';
import 'services.dart' show pattern;

/// The client commands, on a node of their own that never listens.
class Client {
  /// The client of the peer [ctx].
  Client(this.ctx);

  /// The peer's shared state.
  final PeerContext ctx;

  Switchboard? _node;

  /// The commands [command] answers.
  static const List<String> ops = [
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
    'request',
  ];

  Switchboard get _client => _node ??= Switchboard(
    localConnections: false,
    muxOptions: ctx.config.mux,
    talkOptions: ctx.config.talk,
    bindings: [
      QuicSwitchboardBinding(
        NativeQuicProvider(),
        webTransportCertificateVerified: false,
      ),
    ],
  );

  /// Runs the client command [op].
  Future<Object?> command(String op, Map<String, Object?> cmd) async {
    final h = ctx.handles;
    switch (op) {
      case 'connect':
        final c = await _client.dial(
          Uri.parse(requiredString(cmd, 'endpoint')),
        );
        return {'connection': ctx.trackConnection(c)};
      case 'open':
        final ch = h
            .get<MuxConnection>(cmd['connection'], 'connection')
            .open(_openPayload(cmd));
        return {'channel': ctx.trackChannel(ch), 'id': ch.id};
      case 'talk.open':
        return _talkOpen(cmd);
      case 'talk.request':
        final t = h.get<TalkChannel>(cmd['talk'], 'talk channel');
        final reply = await t.request(
          requiredString(cmd, 'procedure'),
          _payload(cmd),
          timeout: msArg(cmd, 'timeoutMs'),
        );
        return _messageJson(reply, digest: boolArg(cmd, 'digest'));
      case 'talk.stream':
        return _talkStream(cmd);
      case 'talk.send':
        await h
            .get<TalkChannel>(cmd['talk'], 'talk channel')
            .send(requiredString(cmd, 'procedure'), _payload(cmd));
        return null;
      case 'talk.close':
        await h
            .get<TalkChannel>(cmd['talk'], 'talk channel')
            .close(statusArg(cmd));
        return null;
      case 'stream.grant':
        h
            .get<TalkStream>(cmd['stream'], 'stream')
            .grant(intArg(cmd, 'items') ?? 1);
        return null;
      case 'stream.cancel':
        h
            .get<TalkStream>(cmd['stream'], 'stream')
            .cancel(cmd['code'] == null ? null : statusArg(cmd));
        return null;
      case 'channel.send':
        await h
            .get<MuxChannel>(cmd['channel'], 'channel')
            .send(bytesOf(cmd['hex']));
        return null;
      case 'channel.close':
        await h
            .get<MuxChannel>(cmd['channel'], 'channel')
            .close(statusArg(cmd));
        return null;
      case 'connection.ping':
        final rtt = await h
            .get<MuxConnection>(cmd['connection'], 'connection')
            .ping(bytesOf(cmd['payload']));
        return {'rttMs': rtt.inMicroseconds / 1000};
      case 'connection.goAway':
        await h
            .get<MuxConnection>(cmd['connection'], 'connection')
            .goAway(statusArg(cmd, code: StatusCode.goingAway.code));
        return null;
      case 'connection.close':
        await h.get<MuxConnection>(cmd['connection'], 'connection').close();
        return null;
      case 'request':
        return _request(cmd);
      default:
        throw ArgumentError('unknown op $op');
    }
  }

  /// Closes the client node.
  Future<void> close() async {
    await _node?.close();
    _node = null;
  }

  /// The OPEN payload: `openHex` as is, else `address` encoded.
  Uint8List _openPayload(Map<String, Object?> cmd) {
    if (cmd.containsKey('openHex')) {
      return bytesOf(cmd['openHex']);
    }
    final a = objectArg(cmd, 'address') ?? const {};
    final type = stringArg(a, 'type');
    final typeHex = stringArg(a, 'typeHex');
    return ChannelAddress(
      type: typeHex != null
          ? Name.fromBytes(hexBytes(typeHex))
          : type == null
          ? null
          : Name(type),
      instance: intArg(a, 'instance') ?? 0,
      shard: intArg(a, 'shard'),
      host: stringArg(a, 'host'),
      payload: bytesOf(a['payload']),
    ).encode();
  }

  /// A command's payload: `payload` (hex), `text` (UTF-8) or
  /// `patternLength` (the `BIG` pattern of that many bytes).
  static Uint8List _payload(Map<String, Object?> cmd) {
    final text = stringArg(cmd, 'text');
    if (text != null) {
      return utf8.encode(text);
    }
    final length = intArg(cmd, 'patternLength');
    if (length != null) {
      return pattern(length);
    }
    return bytesOf(cmd['payload']);
  }

  Future<Map<String, Object?>> _talkOpen(Map<String, Object?> cmd) async {
    final h = ctx.handles;
    String? connection = stringArg(cmd, 'connection');
    if (connection == null) {
      final c = await _client.dial(Uri.parse(requiredString(cmd, 'endpoint')));
      connection = ctx.trackConnection(c);
    }
    final ch = h
        .get<MuxConnection>(connection, 'connection')
        .open(_openPayload(cmd));
    final channel = ctx.trackChannel(ch, listen: false);
    final talk = TalkChannel(ch, options: ctx.config.talk);
    final handle = h.add('t', talk);
    talk.messages.listen((m) async {
      ctx.out.event('talk.message', {
        'talk': handle,
        'requestId': m.requestId,
        ...await _messageJson(m),
      });
      if (m.canReply) {
        await m.replyAbort(Status.of(StatusCode.unimplemented));
      }
    }, onError: (Object _) {});
    return {'talk': handle, 'channel': channel, 'connection': connection};
  }

  Map<String, Object?> _talkStream(Map<String, Object?> cmd) {
    final t = ctx.handles.get<TalkChannel>(cmd['talk'], 'talk channel');
    final s = t.streamRequest(
      requiredString(cmd, 'procedure'),
      _payload(cmd),
      timeout: msArg(cmd, 'timeoutMs'),
      window: intArg(cmd, 'window'),
    );
    final handle = ctx.handles.add('s', s);
    final digest = boolArg(cmd, 'digest');
    unawaited(() async {
      var index = 0;
      try {
        await for (final item in s.items) {
          ctx.out.event('talk.item', {
            'stream': handle,
            'index': index++,
            ...await _messageJson(item, digest: digest),
          });
        }
      } on Object catch (_) {
        // The outcome is reported by talk.done.
      }
      try {
        final last = await s.done;
        ctx.out.event('talk.done', {
          'stream': handle,
          'items': index,
          ...await _messageJson(last, digest: digest),
        });
      } on Object catch (e) {
        ctx.out.event('talk.done', {
          'stream': handle,
          'items': index,
          'error': errorJson(e),
        });
      }
    }());
    return {'stream': handle, 'requestId': s.requestId};
  }

  Future<Map<String, Object?>> _request(Map<String, Object?> cmd) async {
    final client = _client;
    final c = await client.dial(Uri.parse(requiredString(cmd, 'endpoint')));
    try {
      final talk = TalkChannel(
        c.open(
          ChannelAddress(
            type: Name(requiredString(cmd, 'service')),
            instance: intArg(cmd, 'instance') ?? 0,
          ).encode(),
        ),
        options: ctx.config.talk,
      );
      try {
        final reply = await talk.request(
          requiredString(cmd, 'procedure'),
          _payload(cmd),
          timeout: msArg(cmd, 'timeoutMs'),
        );
        return await _messageJson(reply, digest: boolArg(cmd, 'digest'));
      } finally {
        await talk.close();
      }
    } finally {
      await c.goAway();
    }
  }

  /// A received message as JSON: its procedure, whether it came as bulk,
  /// and the payload as hex, or with [digest] its size and SHA-256.
  static Future<Map<String, Object?>> _messageJson(
    TalkMessage m, {
    bool digest = false,
  }) async {
    final payload = await m.payloadBytes();
    return {
      'procedure': m.procedureName,
      'bulk': m.isBulk,
      if (digest) ...{
        'size': payload.length,
        'sha256': '${sha256.convert(payload)}',
      } else
        'payload': hex(payload),
    };
  }
}
