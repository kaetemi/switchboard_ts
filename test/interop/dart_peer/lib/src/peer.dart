/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/// The peer process: the node with its listeners, `READY`, and the
/// command loop on stdin (README, "Process protocol").
library;

import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:logging/logging.dart';
import 'package:switchboard/switchboard.dart';
import 'package:switchboard_quic/switchboard_quic.dart';
import 'package:switchboard_quic/switchboard_quic_native.dart';

import 'client.dart';
import 'config.dart';
import 'context.dart';
import 'identity.dart';
import 'mesh.dart';
import 'oracle.dart';
import 'protocol.dart';
import 'services.dart';

const String _usage = '''
Usage: dart_peer [--config <json>] [--log <level>] [--watchdog <seconds>]

The Dart Switchboard as a test peer: prints READY {json} with its
endpoints, then answers JSON-lines commands on stdin (one response line
each) and reports events. Exits when stdin closes. See
test/interop/README.md in switchboard_ts.''';

/// Runs the peer with the command line [args].
Future<void> runPeer(List<String> args) async {
  var configJson = '{}';
  String? logLevel;
  var watchdog = const Duration(minutes: 10);
  for (var i = 0; i < args.length; i++) {
    String value() => i + 1 < args.length
        ? args[++i]
        : throw ArgumentError('${args[i]} needs a value');
    switch (args[i]) {
      case 'serve':
        break;
      case '--config':
        configJson = value();
      case '--log':
        logLevel = value();
      case '--watchdog':
        watchdog = Duration(seconds: int.parse(value()));
      case '--help' || '-h':
        stdout.writeln(_usage);
        return;
      default:
        stderr.writeln('dart_peer: unknown argument ${args[i]}\n$_usage');
        exit(64);
    }
  }
  if (logLevel != null) {
    final level = Level.LEVELS.firstWhere(
      (l) => l.name.toLowerCase() == logLevel!.toLowerCase(),
      orElse: () => throw ArgumentError('unknown log level $logLevel'),
    );
    Logger.root.level = level;
    Logger.root.onRecord.listen(
      (r) => stderr.writeln(
        '${r.time.toIso8601String()} ${r.level.name} ${r.loggerName}: '
        '${r.message}${r.error == null ? '' : ' ${r.error}'}',
      ),
    );
  }
  final config = PeerConfig.fromJson(
    jsonDecode(configJson) as Map<String, Object?>,
  );
  final ctx = PeerContext(Output(), config);
  await runZonedGuarded(() => _Peer(ctx, watchdog).run(), ctx.reportError);
}

class _Peer {
  _Peer(this.ctx, this.watchdog) : client = Client(ctx);

  final PeerContext ctx;
  final Duration watchdog;
  final Client client;
  late final Switchboard node;
  late final MeshServices mesh;
  final List<QuicSwitchboardListener> _quicListeners = [];
  final List<QuicListener> _rawListeners = [];
  late final Map<String, Object?> _ready;
  bool _stopping = false;

  /// The connections the listeners accepted that have not ended, in the
  /// order accepted.
  final Set<MuxConnection> _accepted = Set.identity();

  Future<void> run() async {
    final timer = Timer(watchdog, () {
      stderr.writeln('dart_peer: watchdog after $watchdog, exiting');
      exit(3);
    });
    final id = ctx.config.identity;
    final identity = await NodeIdentity.of(id);
    node = Switchboard(
      localConnections: false,
      muxOptions: ctx.config.mux,
      talkOptions: ctx.config.talk,
      credential: identity.credential,
      holderKey: identity.holderKey,
      verifier: identity.verifier,
      identifyOutgoing: id.identifyOutgoing,
      identityTimeout: id.identityTimeout,
      // A proxy, a relay or a naming client of this node dials QUIC and
      // WebTransport endpoints too.
      bindings: [
        QuicSwitchboardBinding(
          NativeQuicProvider(),
          webTransportCertificateVerified: false,
        ),
      ],
    );
    await client.identify(identity);
    // Accepted connections are reported once their peer identified; with
    // identifyInReturn the node identifies in return on those whose peer
    // asked (the QUIC listener does so itself).
    node.connections.listen((c) {
      if (!c.isInitiator) {
        _accepted.add(c);
        unawaited(c.done.then((_) => _accepted.remove(c)));
      }
      unawaited(c.peerIdentified.then((_) => ctx.trackConnection(c, node)));
      if (id.identifyInReturn &&
          identity.credential != null &&
          c is! QuicBindingConnection) {
        unawaited(
          c.identityRequested.then(
            (_) => node.identifyOn(c).catchError((Object _) {}),
          ),
        );
      }
    });
    for (final s in ctx.config.services) {
      registerService(node, s, ctx, client);
    }
    mesh = MeshServices(node, ctx.config.mesh);
    _ready = {
      'protocol': peerProtocolVersion,
      'implementation': 'dart',
      ...await _listen(),
      'pid': pid,
      'oracle': oracleKinds,
    };
    mesh.listening(ctx.endpoints);
    ctx.out.line('READY ${jsonEncode(_ready)}');
    for (final signal in [ProcessSignal.sigterm, ProcessSignal.sigint]) {
      try {
        signal.watch().listen((_) => unawaited(_stop()));
      } on Object catch (_) {
        // Not every platform can watch every signal.
      }
    }
    await for (final line
        in stdin.transform(utf8.decoder).transform(const LineSplitter())) {
      if (line.trim().isNotEmpty) {
        unawaited(_handle(line));
      }
    }
    timer.cancel();
    await _stop();
  }

  Future<Map<String, Object?>> _listen() async {
    final loopback = InternetAddress.loopbackIPv4;
    final listen = ctx.config.listen;
    final path = ctx.config.path;
    final endpoints = <String, Object?>{};
    final policy = ctx.config.identity.channelPolicy;
    if (listen.contains('tcp')) {
      endpoints['tcp'] = '${await node.listenTcp(loopback, 0, policy: policy)}';
      ctx.endpoints.add(endpoints['tcp']! as String);
    }
    if (listen.contains('ws')) {
      endpoints['ws'] =
          '${await node.listenWebSocket(loopback, 0, path: path, policy: policy)}';
      ctx.endpoints.add(endpoints['ws']! as String);
    }
    if (listen.contains('quic') || listen.contains('webtransport')) {
      final provider = NativeQuicProvider();
      final cert = NativeQuicProvider.generateSelfSigned('127.0.0.1');
      final config = QuicServerConfig(
        alpn: const [switchboardAlpn],
        certificateDer: cert.certificateDer,
        privateKeyDer: cert.privateKeyDer,
      );
      Future<String> serve(QuicListener raw) async {
        _rawListeners.add(raw);
        final l = await QuicSwitchboardListener.start(
          node,
          raw,
          policy: policy,
          identityRules: switch (ctx.config.identity.quicRules) {
            'none' => QuicIdentityRules.none,
            'listener' => QuicIdentityRules.listener,
            final r => throw ArgumentError('unknown QUIC identity rules $r'),
          },
          identifyInReturn: ctx.config.identity.identifyInReturn,
        );
        _quicListeners.add(l);
        ctx.endpoints.add('${l.endpoint}');
        return '${l.endpoint}';
      }

      if (listen.contains('quic')) {
        endpoints['quic'] = await serve(
          await provider.listen('127.0.0.1', 0, config),
        );
      }
      if (listen.contains('webtransport')) {
        endpoints['webtransport'] = await serve(
          await provider.listenWebTransport('127.0.0.1', 0, config, path: path),
        );
      }
      endpoints['certificateSha256'] = hex(cert.sha256)
          .replaceAll(' ', '')
          .toLowerCase();
    }
    return endpoints;
  }

  Future<void> _handle(String line) async {
    Object? id;
    try {
      final cmd = jsonDecode(line) as Map<String, Object?>;
      id = cmd['id'];
      final op = requiredString(cmd, 'op');
      if (op == 'shutdown') {
        ctx.out.respond(id, null);
        await _stop();
        return;
      }
      ctx.out.respond(id, await _command(op, cmd));
    } on Object catch (e) {
      ctx.out.fail(id, e);
    }
  }

  Future<Object?> _command(String op, Map<String, Object?> cmd) async {
    switch (op) {
      case 'ping':
        return 'pong';
      case 'info':
        return {..._ready, 'handles': ctx.handles.length};
      case 'reset':
        return {'closed': await ctx.closeAll()};
      case 'decode':
        return decodeKind(requiredString(cmd, 'kind'), bytesOf(cmd['hex']));
      case 'decodeMany':
        final kind = requiredString(cmd, 'kind');
        return [
          for (final h in cmd['hexes'] as List<Object?>)
            _attempt(() => decodeKind(kind, bytesOf(h))),
        ];
      case 'encode':
        return encodeKind(
          requiredString(cmd, 'kind'),
          objectArg(cmd, 'fields') ?? const {},
        );
      case 'accepted.goAway':
        final status = statusArg(cmd, code: StatusCode.goingAway.code);
        return {'connections': _eachAccepted((c) => c.goAway(status))};
      case 'accepted.close':
        return {'connections': _eachAccepted((c) => c.close())};
      default:
        if (Client.answers(op)) {
          return client.command(op, cmd);
        }
        throw ArgumentError('unknown op $op');
    }
  }

  /// Runs [action] on every open connection the listeners accepted,
  /// without waiting for it, and answers their handles: each is tracked
  /// from then on, its GOAWAY and its end reported, whether or not its peer
  /// identified.
  List<String> _eachAccepted(Future<void> Function(MuxConnection c) action) {
    final handles = <String>[];
    for (final c in List.of(_accepted)) {
      if (c.isOpen) {
        handles.add(ctx.trackConnection(c, node));
        // Throws at once for a status a GOAWAY cannot carry; completes once
        // the connection ended.
        action(c).ignore();
      }
    }
    return handles;
  }

  static Map<String, Object?> _attempt(Map<String, Object?> Function() f) {
    try {
      return {'ok': true, 'value': f()};
    } on Object catch (e) {
      return {'ok': false, 'error': errorJson(e)};
    }
  }

  Future<void> _stop() async {
    if (_stopping) {
      return;
    }
    _stopping = true;
    Timer(const Duration(seconds: 5), () => exit(0));
    try {
      await ctx.closeAll();
      await client.close();
      await mesh.close();
      for (final l in _quicListeners) {
        await l.close();
      }
      await node.close();
      for (final l in _rawListeners) {
        await l.close();
      }
      if (_rawListeners.isNotEmpty) {
        NativeQuicProvider.shutdown();
      }
    } on Object catch (e, st) {
      stderr.writeln('dart_peer: shutdown: $e\n$st');
    }
    await ctx.out.flush();
    exit(0);
  }
}
