/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// What the driver's test files share: the Dart node they dial with, the TS
// peer fixture of a group, and small helpers for waiting on what the Dart
// side observes.

import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:switchboard/switchboard.dart';
import 'package:switchboard_dart_driver/ts_peer.dart';
import 'package:switchboard_quic/switchboard_quic.dart';
import 'package:switchboard_quic/switchboard_quic_native.dart';
import 'package:test/test.dart';

const Duration limit = Duration(seconds: 10);

/// Connections that close at once once their channels are gone.
const MuxOptions fast = MuxOptions(goAwayGrace: Duration(milliseconds: 200));

/// A Dart node that dials every scheme, QUIC and WebTransport unverified.
Switchboard dartNode({
  Credential? credential,
  HolderKey? holderKey,
  MuxOptions muxOptions = fast,
  Resolver? resolver,
  Duration slotRefreshTimeout = const Duration(seconds: 5),
}) => Switchboard(
  localConnections: false,
  muxOptions: muxOptions,
  credential: credential,
  holderKey: holderKey,
  resolver: resolver,
  slotRefreshTimeout: slotRefreshTimeout,
  bindings: [
    QuicSwitchboardBinding(
      NativeQuicProvider(),
      webTransportCertificateVerified: false,
    ),
  ],
);

Uint8List text(String s) => Uint8List.fromList(utf8.encode(s));

String textOf(Uint8List bytes) => utf8.decode(bytes);

/// A matcher for a [SwitchboardException] with [code].
Matcher throwsCode(StatusCode code) => throwsA(
  isA<SwitchboardException>().having((e) => e.status.known, 'code', code),
);

/// A matcher for a [Status] with [code].
Matcher hasCode(StatusCode code) =>
    isA<Status>().having((s) => s.known, 'code', code);

/// A TS peer started once for a group, skipped with the reason it could
/// not start for.
class Fixture {
  TsPeer? peer;
  String? skip;

  TsPeer get ts => peer!;

  Future<void> start(Map<String, Object?> config) async {
    try {
      peer = await TsPeer.start(config);
    } on Object catch (e) {
      skip = 'the TS peer cannot serve this: $e';
    }
  }

  /// Whether the test runs; marks it skipped otherwise.
  bool get runs {
    final reason = skip;
    if (reason != null) {
      markTestSkipped(reason);
      return false;
    }
    return true;
  }

  Future<void> afterEach() async {
    final p = peer;
    if (p == null) {
      return;
    }
    final errors = p.errors;
    await p.reset();
    expect(errors, isEmpty, reason: 'errors the TS peer reported');
  }
}

/// The subframes of a channel as they arrive, for waiting on a count.
class Received {
  /// Listens to [stream] at once.
  Received(Stream<Uint8List> stream) {
    subscription = stream.listen(
      (data) {
        frames.add(data);
        _changed.add(null);
      },
      onError: (Object _) {},
      onDone: () {
        ended = true;
        _changed.add(null);
      },
    );
  }

  final List<Uint8List> frames = [];
  final StreamController<void> _changed = StreamController.broadcast();
  late final StreamSubscription<Uint8List> subscription;
  bool ended = false;

  /// Bytes received so far.
  int get length => frames.fold(0, (n, f) => n + f.length);

  /// Completes once [condition] holds, checked on every arrival.
  Future<void> until(bool Function() condition) async {
    if (condition()) {
      return;
    }
    await _changed.stream.firstWhere((_) => condition()).timeout(limit);
  }

  /// Completes once [count] subframes arrived.
  Future<void> count(int count) => until(() => frames.length >= count);

  Future<void> cancel() async {
    await subscription.cancel();
    await _changed.close();
  }
}

/// Completes once [condition] holds, checked whenever [changes] has an
/// event; fails after [limit].
Future<void> whenever(Stream<Object?> changes, bool Function() condition) {
  final done = Completer<void>();
  late final StreamSubscription<Object?> subscription;
  subscription = changes.listen((_) {
    if (!done.isCompleted && condition()) {
      done.complete();
    }
  });
  if (condition() && !done.isCompleted) {
    done.complete();
  }
  return done.future.timeout(limit).whenComplete(subscription.cancel);
}
