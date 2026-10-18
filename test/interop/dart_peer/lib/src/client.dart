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
import 'identity.dart';
import 'mesh.dart';
import 'protocol.dart';
import 'reconnect.dart';
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
    'connection.drain',
    'connection.identify',
    'request',
  ];

  NodeIdentity? _identity;

  /// Sets the identity the client node is made with; before its first use.
  Future<void> identify(NodeIdentity identity) async {
    _identity = identity;
  }

  Switchboard get _client => _node ??= _newNode(null);

  late final NamingCommands _naming = NamingCommands(ctx, _newNode);

  /// A client node, as [_client] is, resolving with [resolver].
  Switchboard _newNode(Resolver? resolver) => Switchboard(
    resolver: resolver,
    localConnections: false,
    muxOptions: ctx.config.mux,
    talkOptions: ctx.config.talk,
    credential: _identity?.credential,
    holderKey: _identity?.holderKey,
    verifier: _identity?.verifier,
    identifyOutgoing: ctx.config.identity.identifyOutgoing,
    identityTimeout: ctx.config.identity.identityTimeout,
    bindings: [
      QuicSwitchboardBinding(
        NativeQuicProvider(),
        webTransportCertificateVerified: false,
      ),
    ],
  );

  late final ReconnectCommands _reconnect = ReconnectCommands(ctx);

  /// Whether [command] answers [op].
  static bool answers(String op) =>
      ops.contains(op) ||
      NamingCommands.ops.contains(op) ||
      ReconnectCommands.ops.contains(op);

  /// Runs the client command [op].
  Future<Object?> command(String op, Map<String, Object?> cmd) async {
    final h = ctx.handles;
    if (NamingCommands.ops.contains(op)) {
      return _naming.command(
        op,
        cmd,
        (channel) async => channel is MuxChannel
            ? _adoptTalk(channel, ctx.trackConnection(channel.connection))
            : _adoptTalk(channel),
      );
    }
    if (ReconnectCommands.ops.contains(op)) {
      return _reconnect.command(op, cmd);
    }
    switch (op) {
      case 'connect':
        final intent = cmd['intent'];
        final c = await _client.dial(
          Uri.parse(requiredString(cmd, 'endpoint')),
          intent: intent == null ? null : bytesOf(intent),
          receiver: stringArg(cmd, 'receiver'),
        );
        return {'connection': ctx.trackConnection(c, _client)};
      case 'connection.identify':
        final c = h.get<MuxConnection>(cmd['connection'], 'connection');
        final intent = cmd['intent'];
        await (ctx.ownerOf(c) ?? _client).identifyOn(
          c,
          intent: intent == null ? null : bytesOf(intent),
          receiver: stringArg(cmd, 'receiver'),
        );
        return null;
      case 'open':
        final ch = h
            .get<MuxConnection>(cmd['connection'], 'connection')
            .open(_openPayload(cmd));
        return {'channel': ctx.trackChannel(ch), 'id': ch.id};
      case 'talk.open':
        return _talkOpen(cmd);
      case 'talk.request':
        final handle = requiredString(cmd, 'talk');
        final t = h.get<TalkChannel>(handle, 'talk channel');
        final (payload, bulk, bulkLength) = _payloadAndBulk(cmd);
        late final TalkRequest r;
        r = t.startRequest(
          requiredString(cmd, 'procedure'),
          payload,
          bulk: bulk,
          bulkLength: bulkLength,
          timeout: msArg(cmd, 'timeoutMs'),
          onExtend: (deadline, renew) => ctx.out.event('talk.extend', {
            'talk': handle,
            'requestId': r.requestId,
            ..._extensionJson(deadline, renew),
          }),
        );
        return messageJson(await r.response, digest: boolArg(cmd, 'digest'));
      case 'talk.stream':
        return _talkStream(cmd);
      case 'talk.send':
        final (payload, bulk, bulkLength) = _payloadAndBulk(cmd);
        await h
            .get<TalkChannel>(cmd['talk'], 'talk channel')
            .send(
              requiredString(cmd, 'procedure'),
              payload,
              bulk: bulk,
              bulkLength: bulkLength,
            );
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
      case 'connection.drain':
        final c = h.get<MuxConnection>(cmd['connection'], 'connection');
        final session = c is QuicBindingConnection ? c.session : null;
        if (session is! NativeQuicSession || !session.isWebTransport) {
          throw ArgumentError(
            'connection.drain needs a WebTransport connection',
          );
        }
        session.drain();
        return null;
      case 'request':
        return _request(cmd);
      default:
        throw ArgumentError('unknown op $op');
    }
  }

  /// A Talk channel to [openPayload]'s service at [endpoint], on a pooled
  /// connection: what a `forward` service forwards to.
  Future<TalkChannel> openTalk(String endpoint, Uint8List openPayload) async {
    final c = await _client.connect(Uri.parse(endpoint));
    return TalkChannel(c.open(openPayload), options: ctx.config.talk);
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
    return addressArg(cmd).encode();
  }

  /// A command's payload, and with `bulkPattern` (`{length, chunk,
  /// declared}`) instead an empty payload and the `BIG` pattern of
  /// `length` bytes as a bulk source in pieces of `chunk` (default 1000),
  /// its length declared unless `declared` is false.
  static (Uint8List, Stream<List<int>>?, int?) _payloadAndBulk(
    Map<String, Object?> cmd,
  ) {
    final spec = objectArg(cmd, 'bulkPattern');
    if (spec == null) {
      return (payloadOf(cmd), null, null);
    }
    final length = intArg(spec, 'length') ?? 0;
    final chunk = intArg(spec, 'chunk') ?? 1000;
    final bytes = pattern(length);
    Stream<List<int>> pieces() async* {
      for (var at = 0; at < length; at += chunk) {
        yield bytes.sublist(at, at + chunk < length ? at + chunk : length);
      }
    }

    return (
      Uint8List(0),
      pieces(),
      boolArg(spec, 'declared', fallback: true) ? length : null,
    );
  }

  /// An `EXTEND` as the `talk.extend` event reports it, in milliseconds.
  static Map<String, Object?> _extensionJson(
    Duration? deadline,
    Duration? renew,
  ) => {
    'deadlineMs': deadline?.inMilliseconds,
    'renewMs': renew?.inMilliseconds,
  };

  Future<Map<String, Object?>> _talkOpen(Map<String, Object?> cmd) async {
    final h = ctx.handles;
    String? connection = stringArg(cmd, 'connection');
    if (connection == null) {
      final c = await _client.dial(Uri.parse(requiredString(cmd, 'endpoint')));
      connection = ctx.trackConnection(c, _client);
    }
    final ch = h
        .get<MuxConnection>(connection, 'connection')
        .open(_openPayload(cmd));
    return _adoptTalk(ch, connection);
  }

  /// [ch] (a [MuxChannel] or a [SlotChannel]) as a Talk channel handle on
  /// [connection]'s, its messages reported and its requests refused; a slot
  /// channel has no one connection, and its handle is reported as
  /// `slotChannel`.
  Map<String, Object?> _adoptTalk(Object ch, [String? connection]) {
    final options = ctx.config.talk;
    final (channel, talk) = switch (ch) {
      final SlotChannel c => (
        ctx.trackSlotChannel(c, listen: false),
        TalkChannel(c, options: options),
      ),
      final MuxChannel c => (
        ctx.trackChannel(c, listen: false),
        TalkChannel(c, options: options),
      ),
      _ => throw ArgumentError.value(ch, 'ch', 'not a channel'),
    };
    final handle = reportTalk(ctx, talk);
    return ch is SlotChannel
        ? {'talk': handle, 'slotChannel': channel}
        : {'talk': handle, 'channel': channel, 'connection': connection};
  }

  Map<String, Object?> _talkStream(Map<String, Object?> cmd) {
    final t = ctx.handles.get<TalkChannel>(cmd['talk'], 'talk channel');
    final (payload, bulk, bulkLength) = _payloadAndBulk(cmd);
    String? handle;
    final s = t.streamRequest(
      requiredString(cmd, 'procedure'),
      payload,
      bulk: bulk,
      bulkLength: bulkLength,
      timeout: msArg(cmd, 'timeoutMs'),
      window: intArg(cmd, 'window'),
      onExtend: (deadline, renew) => ctx.out.event('talk.extend', {
        'stream': handle,
        ..._extensionJson(deadline, renew),
      }),
    );
    handle = ctx.handles.add('s', s);
    final digest = boolArg(cmd, 'digest');
    unawaited(() async {
      var index = 0;
      try {
        await for (final item in s.items) {
          ctx.out.event('talk.item', {
            'stream': handle,
            'index': index++,
            ...await messageJson(item, digest: digest),
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
          ...await messageJson(last, digest: digest),
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
          payloadOf(cmd),
          timeout: msArg(cmd, 'timeoutMs'),
        );
        return await messageJson(reply, digest: boolArg(cmd, 'digest'));
      } finally {
        await talk.close();
      }
    } finally {
      await c.goAway();
    }
  }
}

/// The command's `address`: `{type, typeHex, instance, shard, host,
/// payload}`, every field optional.
ChannelAddress addressArg(Map<String, Object?> cmd) {
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
  );
}

/// A command's payload: `payload` (hex), `text` (UTF-8) or `patternLength`
/// (the `BIG` pattern of that many bytes).
Uint8List payloadOf(Map<String, Object?> cmd) {
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

/// [talk] as a handle: the messages the far side sends on it are reported
/// as `talk.message`, and its requests refused with UNIMPLEMENTED.
String reportTalk(PeerContext ctx, TalkChannel talk) {
  final handle = ctx.handles.add('t', talk);
  talk.messages.listen((m) async {
    ctx.out.event('talk.message', {
      'talk': handle,
      'requestId': m.requestId,
      ...await messageJson(m),
    });
    if (m.canReply) {
      await m.replyAbort(Status.of(StatusCode.unimplemented));
    }
  }, onError: (Object _) {});
  return handle;
}

/// A received message as JSON: its procedure, whether it came as bulk, and
/// the payload as hex, or with [digest] its size and SHA-256.
Future<Map<String, Object?>> messageJson(
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
