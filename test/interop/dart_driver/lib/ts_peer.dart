/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/// The TS peer as a process the Dart driver drives over the peer protocol
/// (test/interop/README.md): spawned with Node, `READY` read, commands as
/// JSON lines, events kept for waiting on.
library;

import 'dart:async';
import 'dart:convert';
import 'dart:io';

/// A failed command, with the peer's error object.
class PeerError implements Exception {
  /// The error of [op].
  PeerError(this.op, this.error);

  /// The command.
  final String op;

  /// The peer's `{kind, type, message, code, reason, body}`.
  final Map<String, Object?> error;

  /// The status code of a `status` error.
  int? get code => error['code'] as int?;

  @override
  String toString() => 'PeerError($op: ${error['type']}: ${error['message']})';
}

/// The repository root: `SWITCHBOARD_TS_ROOT`, else four levels above this
/// package's directory (test/interop/dart_driver).
String repositoryRoot() {
  final env = Platform.environment['SWITCHBOARD_TS_ROOT'];
  if (env != null && env.isNotEmpty) {
    return env;
  }
  return Directory.current.uri.resolve('../../../').toFilePath();
}

/// A running TS peer.
class TsPeer {
  TsPeer._(this._process, this.ready, this._stderr);

  final Process _process;

  /// What the peer printed in `READY`.
  final Map<String, Object?> ready;

  final Map<int, Completer<Object?>> _pending = {};
  final List<Map<String, Object?>> _events = [];
  final StreamController<Map<String, Object?>> _eventStream =
      StreamController.broadcast();
  final StringBuffer _stderr;
  int _nextId = 1;

  /// Starts the TS peer with [config]: Node (`SWITCHBOARD_NODE`, else `node`
  /// on PATH) running test/interop/ts_peer/main.ts.
  static Future<TsPeer> start(Map<String, Object?> config) async {
    final node = Platform.environment['SWITCHBOARD_NODE'] ?? 'node';
    final main = '${repositoryRoot()}test/interop/ts_peer/main.ts';
    final process = await Process.start(node, [
      '--disable-warning=ExperimentalWarning',
      main,
      '--config',
      jsonEncode(config),
      '--watchdog',
      '600',
    ]);
    final ready = Completer<Map<String, Object?>>();
    TsPeer? peer;
    // Lines after READY that arrive before the peer object exists.
    final early = <String>[];
    final errors = StringBuffer();
    process.stderr.transform(utf8.decoder).listen(errors.write);
    process.stdout
        .transform(utf8.decoder)
        .transform(const LineSplitter())
        .listen(
          (line) {
            if (!ready.isCompleted) {
              final at = line.indexOf('READY ');
              if (at >= 0) {
                ready.complete(
                  jsonDecode(line.substring(at + 6)) as Map<String, Object?>,
                );
              }
              return;
            }
            final p = peer;
            if (p == null) {
              early.add(line);
            } else {
              p._line(line);
            }
          },
          onDone: () {
            if (!ready.isCompleted) {
              ready.completeError(
                StateError('the TS peer exited before READY:\n$errors'),
              );
            }
          },
        );
    final info = await ready.future.timeout(const Duration(seconds: 60));
    final started = TsPeer._(process, info, errors);
    peer = started;
    early.forEach(started._line);
    return started;
  }

  void _line(String line) {
    if (!line.startsWith('{')) {
      return;
    }
    final message = jsonDecode(line) as Map<String, Object?>;
    if (message.containsKey('event')) {
      _events.add(message);
      _eventStream.add(message);
      return;
    }
    final pending = _pending.remove(message['id']);
    if (pending == null) {
      return;
    }
    if (message['ok'] == true) {
      pending.complete(message['value']);
    } else {
      pending.completeError(
        PeerError('?', message['error']! as Map<String, Object?>),
      );
    }
  }

  /// The endpoint the peer listens on for [transport].
  Uri endpoint(String transport) => Uri.parse(ready[transport]! as String);

  /// Sends [op] with [args] and completes with its value.
  Future<T> rpc<T>(String op, [Map<String, Object?> args = const {}]) async {
    final id = _nextId++;
    final completer = Completer<Object?>();
    _pending[id] = completer;
    _process.stdin.writeln(jsonEncode({...args, 'id': id, 'op': op}));
    try {
      return (await completer.future.timeout(const Duration(seconds: 30))) as T;
    } on PeerError catch (e) {
      throw PeerError(op, e.error);
    }
  }

  /// The first event satisfying [match], logged already or to come.
  Future<Map<String, Object?>> waitFor(
    bool Function(Map<String, Object?> event) match, {
    Duration timeout = const Duration(seconds: 10),
  }) {
    for (final e in _events) {
      if (match(e)) {
        return Future.value(e);
      }
    }
    return _eventStream.stream.firstWhere(match).timeout(timeout);
  }

  /// Every event so far.
  List<Map<String, Object?>> get events => List.unmodifiable(_events);

  /// The `peer.error` events.
  List<Map<String, Object?>> get errors =>
      _events.where((e) => e['event'] == 'peer.error').toList();

  /// What the peer wrote to stderr.
  String get diagnostics => '$_stderr';

  /// Closes everything the previous test created and forgets the events.
  Future<void> reset() async {
    await rpc<Object?>('reset');
    _events.clear();
  }

  /// Stops the peer.
  Future<void> stop() async {
    unawaited(rpc<Object?>('shutdown').catchError((Object _) => null));
    await _process.stdin.close();
    await _process.exitCode.timeout(
      const Duration(seconds: 5),
      onTimeout: () {
        _process.kill(ProcessSignal.sigkill);
        return -1;
      },
    );
  }
}
