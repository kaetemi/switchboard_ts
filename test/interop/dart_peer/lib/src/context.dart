/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/// The state a peer's commands and services share: the output, the handles
/// and the tracking that turns what the node observes into events.
library;

import 'dart:async';
import 'dart:io';

import 'package:switchboard/core.dart';

import 'config.dart';
import 'protocol.dart';

/// Shared state of one peer process.
class PeerContext {
  /// The context of a peer writing to [out] with [config].
  PeerContext(this.out, this.config);

  /// Where responses and events go.
  final Output out;

  /// The configuration from the command line.
  final PeerConfig config;

  /// Every object a command or a service created.
  final Handles handles = Handles();

  /// Registers [connection] and reports its GOAWAY and its end as events.
  String trackConnection(MuxConnection connection) {
    final existing = handles.nameOf(connection);
    if (existing != null) {
      return existing;
    }
    final handle = handles.add('c', connection);
    unawaited(
      connection.peerGoAwayStatus.then(
        (s) => out.event('connection.goAway', {
          'connection': handle,
          ...statusJson(s),
        }),
      ),
    );
    unawaited(
      connection.done.then((s) {
        out.event('connection.closed', {
          'connection': handle,
          ...statusJson(s),
        });
      }),
    );
    return handle;
  }

  /// Registers [channel] and reports its end, and with [listen] its
  /// subframes, as events. A channel a Talk channel reads is tracked
  /// without [listen].
  String trackChannel(MuxChannel channel, {bool listen = true}) {
    final handle = handles.add('ch', channel);
    if (listen) {
      channel.stream.listen(
        (subframe) => out.event('channel.data', {
          'channel': handle,
          'hex': hex(subframe),
        }),
        onError: (Object _) {},
      );
    }
    unawaited(
      channel.done.then(
        (s) =>
            out.event('channel.closed', {'channel': handle, ...statusJson(s)}),
      ),
    );
    return handle;
  }

  /// Reports an error no command answers (a service's, say) as a
  /// `peer.error` event and on stderr, so a test can assert there were none.
  void reportError(Object error, StackTrace stack) {
    stderr.writeln('dart_peer: unhandled $error\n$stack');
    out.event('peer.error', {'error': errorJson(error)});
  }

  /// Closes every handle's object, newest first, and forgets them all.
  Future<int> closeAll() async {
    final objects = handles.all;
    handles.clear();
    for (final o in objects) {
      try {
        switch (o) {
          case final TalkChannel t:
            await t.close().timeout(_closeTimeout, onTimeout: () {});
          case final MuxChannel c:
            await c.close().timeout(_closeTimeout, onTimeout: () {});
          case final MuxConnection c:
            await c.close().timeout(_closeTimeout, onTimeout: () {});
          case final TalkStream s:
            s.cancel();
        }
      } on Object catch (_) {
        // Already closed or failing: nothing left to release.
      }
    }
    return objects.length;
  }

  static const Duration _closeTimeout = Duration(seconds: 2);
}
