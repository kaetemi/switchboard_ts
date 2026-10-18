/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/// The reconnecting client commands (README, "Reconnecting client"): a
/// [ReconnectingClient] of this implementation to the other peer's
/// listener, its states, pushed channels and persistent channels reported
/// as events, and the channels it opens as handles the other client
/// commands drive.
library;

import 'dart:async';
import 'dart:typed_data';

import 'package:switchboard/switchboard.dart';
import 'package:switchboard_quic/switchboard_quic.dart';
import 'package:switchboard_quic/switchboard_quic_native.dart';

import 'client.dart';
import 'context.dart';
import 'protocol.dart';
import 'services.dart' show addressJson;

/// The reconnecting client commands of a peer.
class ReconnectCommands {
  /// The commands of the peer [ctx].
  ReconnectCommands(this.ctx);

  /// The peer's shared state.
  final PeerContext ctx;

  /// The commands [command] answers.
  static const List<String> ops = [
    'reconnect.open',
    'reconnect.talk',
    'reconnect.channel',
    'reconnect.persistent',
    'reconnect.stop',
    'reconnect.start',
    'reconnect.now',
    'reconnect.close',
    'persistent.close',
  ];

  /// Runs the reconnecting client command [op].
  Future<Object?> command(String op, Map<String, Object?> cmd) async {
    final h = ctx.handles;
    switch (op) {
      case 'reconnect.open':
        return _open(cmd);
      case 'persistent.close':
        final handle = cmd['persistent'];
        final done = switch (h.get<Object>(handle, 'persistent channel')) {
          final PersistentChannel p => p.close().then((_) => p.done),
          final PersistentTalk p => p.close().then((_) => p.done),
          _ => throw ArgumentError('no persistent channel with handle $handle'),
        };
        return statusJson(await done);
    }
    final client = h.get<ReconnectingClient>(
      cmd['reconnect'],
      'reconnecting client',
    );
    final openTimeout =
        msArg(cmd, 'openTimeoutMs') ?? ReconnectingClient.defaultOpenTimeout;
    switch (op) {
      case 'reconnect.talk':
        return _adoptTalk(
          await client.openTalk(addressArg(cmd), openTimeout: openTimeout),
        );
      case 'reconnect.channel':
        final ch = await client.openChannel(
          addressArg(cmd),
          openTimeout: openTimeout,
        );
        final connection = ctx.trackConnection(ch.connection);
        return {
          'channel': ctx.trackChannel(ch),
          'id': ch.id,
          'connection': connection,
        };
      case 'reconnect.persistent':
        return _persistent(client, cmd);
      case 'reconnect.stop':
        client.stop();
        return _stateJson(client, client.state);
      case 'reconnect.start':
        client.start();
        return null;
      case 'reconnect.now':
        client.reconnectNow(force: boolArg(cmd, 'force'));
        return null;
      case 'reconnect.close':
        await client.close();
        return null;
      default:
        throw ArgumentError('unknown op $op');
    }
  }

  /// A reconnecting client to `endpoint`, with the peer's mux and Talk
  /// options and the backoff the command names (the library's defaults
  /// otherwise). Its state changes are `reconnect.state` events and the
  /// channels the endpoint pushes `reconnect.incoming`.
  Map<String, Object?> _open(Map<String, Object?> cmd) {
    final endpoint = Uri.parse(requiredString(cmd, 'endpoint'));
    // The library's defaults where the command names none.
    final initialBackoff =
        msArg(cmd, 'initialBackoffMs') ?? const Duration(milliseconds: 500);
    final maxBackoff =
        msArg(cmd, 'maxBackoffMs') ?? const Duration(seconds: 30);
    final backoffFactor = numberArg(cmd, 'backoffFactor') ?? 2;
    final jitter = numberArg(cmd, 'jitter') ?? 0.2;
    final connectTimeout =
        msArg(cmd, 'connectTimeoutMs') ?? const Duration(seconds: 10);
    final defaultPayload = bytesOf(cmd['defaultPayload']);
    final mux = ctx.config.mux;
    final talk = ctx.config.talk;
    // Started below, once every state change is listened to.
    final client = switch (endpoint.scheme.toLowerCase()) {
      'tcp' || 'ws' || 'wss' => ReconnectingClient(
        _transport(endpoint),
        muxOptions: mux,
        talkOptions: talk,
        initialBackoff: initialBackoff,
        maxBackoff: maxBackoff,
        backoffFactor: backoffFactor,
        jitter: jitter,
        connectTimeout: connectTimeout,
        defaultPayload: defaultPayload,
        autoStart: false,
      ),
      'quic' || 'https' => ReconnectingClient.connections(
        _quic(endpoint),
        muxOptions: mux,
        talkOptions: talk,
        initialBackoff: initialBackoff,
        maxBackoff: maxBackoff,
        backoffFactor: backoffFactor,
        jitter: jitter,
        connectTimeout: connectTimeout,
        defaultPayload: defaultPayload,
        autoStart: false,
      ),
      _ => throw ArgumentError('no reconnecting client for $endpoint'),
    };
    final handle = ctx.handles.add('r', client);
    // Every change, not the state current when listening.
    client.states
        .skip(1)
        .listen(
          (state) => ctx.out.event('reconnect.state', {
            'reconnect': handle,
            ..._stateJson(client, state),
          }),
        );
    client.incoming.listen((incoming) {
      final connection = ctx.trackConnection(incoming.connection);
      ctx.trackChannel(
        incoming.channel,
        announce: (channel) => ctx.out.event('reconnect.incoming', {
          'reconnect': handle,
          'channel': channel,
          'connection': connection,
          'id': incoming.channel.id,
          ...addressJson(incoming.address),
        }),
      );
    });
    if (boolArg(cmd, 'autoStart', fallback: true)) {
      client.start();
    }
    return {'reconnect': handle};
  }

  /// Transports to [endpoint] (`tcp://`, `ws://`) for the frame mux, with
  /// the frame limits the client node gives them.
  TransportConnector _transport(Uri endpoint) {
    final limit = ctx.config.mux.maxFrameSize;
    if (endpoint.scheme.toLowerCase() == 'tcp') {
      return () => StreamTransport.connectTcp(
        endpoint.host,
        endpoint.port,
        maxFrameSize: limit > 0 ? limit : 0xFFFFFFFF,
      );
    }
    return () => IOWebSocketTransport.connect(
      endpoint.removeFragment(),
      maxFrameSize: limit > 0
          ? limit
          : WebSocketServerTransport.defaultMaxFrameSize,
    );
  }

  /// Connections of the QUIC binding to [endpoint] (`quic://`, `https://`
  /// for WebTransport), without verifying the listener's certificate, as
  /// the client node dials them.
  static ConnectionConnector _quic(Uri endpoint) {
    final provider = NativeQuicProvider();
    return (options) => connectQuicBinding(
      provider,
      endpoint,
      options: options,
      verifyCertificate: false,
    );
  }

  /// A persistent channel of [client] to `address`: a persistent Talk
  /// channel unless `talk` is false. Each channel it opens is
  /// `persistent.opened`, with `request` sent on it for a Talk channel and
  /// its outcome `persistent.reply`; each end `persistent.closed`; the end
  /// for good `persistent.done`.
  Map<String, Object?> _persistent(
    ReconnectingClient client,
    Map<String, Object?> cmd,
  ) {
    final address = addressArg(cmd);
    final request = objectArg(cmd, 'request');
    final procedure = request == null
        ? null
        : requiredString(request, 'procedure');
    final payload = request == null ? null : payloadOf(request);
    // Set before any callback: onOpen is never called from openPersistent
    // itself.
    var handle = '';
    void onClosed(Status status) => ctx.out.event('persistent.closed', {
      'persistent': handle,
      ...statusJson(status),
    });
    final Object persistent;
    final Future<Status> done;
    if (boolArg(cmd, 'talk', fallback: true)) {
      final p = client.openPersistentTalk(address, (talk) async {
        final opened = _adoptTalk(talk);
        ctx.out.event('persistent.opened', {'persistent': handle, ...opened});
        if (procedure != null && payload != null) {
          await _subscribe(handle, opened['talk']!, talk, procedure, payload);
        }
      }, onClosed: onClosed);
      persistent = p;
      done = p.done;
    } else {
      final p = client.openPersistent(address, (channel) {
        final connection = ctx.trackConnection(channel.connection);
        ctx.trackChannel(
          channel,
          announce: (ch) => ctx.out.event('persistent.opened', {
            'persistent': handle,
            'channel': ch,
            'connection': connection,
          }),
        );
      }, onClosed: onClosed);
      persistent = p;
      done = p.done;
    }
    handle = ctx.handles.add('p', persistent);
    unawaited(
      done.then(
        (status) => ctx.out.event('persistent.done', {
          'persistent': handle,
          ...statusJson(status),
        }),
      ),
    );
    return {'persistent': handle};
  }

  /// Sends [procedure] with [payload] on [talk] and reports its outcome as
  /// `persistent.reply`.
  Future<void> _subscribe(
    String persistent,
    String handle,
    TalkChannel talk,
    String procedure,
    Uint8List payload,
  ) async {
    try {
      final reply = await talk.request(procedure, payload);
      ctx.out.event('persistent.reply', {
        'persistent': persistent,
        'talk': handle,
        ...await messageJson(reply),
      });
    } on Object catch (e) {
      ctx.out.event('persistent.reply', {
        'persistent': persistent,
        'talk': handle,
        'error': errorJson(e),
      });
    }
  }

  /// [talk], opened by a reconnecting client, as `{talk, channel,
  /// connection}` handles.
  Map<String, String> _adoptTalk(TalkChannel talk) {
    final raw = talk.raw;
    if (raw is! MuxChannel) {
      throw StateError(
        'a reconnecting client opens Talk channels over mux channels',
      );
    }
    final connection = ctx.trackConnection(raw.connection);
    final channel = ctx.trackChannel(raw, listen: false);
    return {
      'talk': reportTalk(ctx, talk),
      'channel': channel,
      'connection': connection,
    };
  }

  /// [state] as `reconnect.state` reports it: the phase, the attempt, the
  /// last status, the delay of the attempt scheduled (`nextAttemptAt` less
  /// `since`, rounded to milliseconds), and while connected the handle of
  /// the connection. The states arrive a microtask after they change: the
  /// client's connection is the state's while the state is current, and
  /// null is reported for one the client already left.
  Map<String, Object?> _stateJson(
    ReconnectingClient client,
    ClientState state,
  ) {
    final connection = state.isConnected && identical(client.state, state)
        ? client.connection
        : null;
    final next = state.nextAttemptAt;
    final since = state.since;
    final lastStatus = state.lastStatus;
    return {
      'phase': state.phase.name,
      'attempt': state.attempt,
      'lastStatus': lastStatus == null ? null : statusJson(lastStatus),
      'retryMs': next == null || since == null
          ? null
          : (next.difference(since).inMicroseconds / 1000).round(),
      'connection': connection == null ? null : ctx.trackConnection(connection),
    };
  }
}
