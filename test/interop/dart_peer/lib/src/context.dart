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
import 'dart:typed_data';

import 'package:switchboard/switchboard.dart';

import 'config.dart';
import 'identity.dart';
import 'mesh.dart';
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

  /// The endpoints this peer listens on, in the order of `listen`.
  final List<String> endpoints = [];

  final Map<MuxConnection, Switchboard> _owners = Map.identity();

  /// The node [connection] belongs to, if known.
  Switchboard? ownerOf(MuxConnection connection) => _owners[connection];

  /// Registers [connection] (of [node], when known) and reports its
  /// identification, its GOAWAY and its end as events.
  String trackConnection(MuxConnection connection, [Switchboard? node]) {
    if (node != null) {
      _owners[connection] = node;
    }
    final existing = handles.nameOf(connection);
    if (existing != null) {
      return existing;
    }
    final handle = handles.add('c', connection);
    // Every valid IDENT, the first and those replacing it.
    void report(PeerIdentity identity) => out.event('connection.identified', {
      'connection': handle,
      ...identityJson(identity),
    });
    unawaited(
      connection.peerIdentified.then((first) async {
        report(first);
        var last = first;
        while (connection.isOpen) {
          await connection.identityChanged;
          final now = connection.peerIdentity;
          if (now != null && !identical(now, last)) {
            report(now);
            last = now;
          }
        }
      }),
    );
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
  /// subframes, as events; [announce] runs between the two, so that what
  /// it reports (`channel.incoming`, say) comes before the first subframe.
  /// A channel a Talk channel reads is tracked without [listen].
  ///
  /// Every `channel.data` comes before the `channel.closed`: the channel's
  /// `done` can complete while subframes that arrived before the CLOSE are
  /// still on their way through its stream.
  String trackChannel(
    MuxChannel channel, {
    bool listen = true,
    void Function(String handle)? announce,
  }) {
    final handle = handles.add('ch', channel);
    announce?.call(handle);
    _report(channel.stream, channel.done, handle, listen: listen);
    return handle;
  }

  /// Registers the slot channel [channel] and reports it as [trackChannel]
  /// does a channel, under its own handle: its subframes and end are
  /// `channel.data` and `channel.closed` across a replacement.
  String trackSlotChannel(SlotChannel channel, {bool listen = true}) {
    final handle = handles.add('sc', channel);
    _report(channel.stream, channel.done, handle, listen: listen);
    return handle;
  }

  /// Reports a channel's end ([done]), and with [listen] the subframes of
  /// its [stream] first, under [handle].
  void _report(
    Stream<Uint8List> stream,
    Future<Status> done,
    String handle, {
    required bool listen,
  }) {
    final read = Completer<void>();
    if (listen) {
      stream.listen(
        (subframe) => out.event('channel.data', {
          'channel': handle,
          'hex': hex(subframe),
        }),
        onError: (Object _) {},
        onDone: read.complete,
      );
    } else {
      read.complete();
    }
    unawaited(() async {
      final status = await done;
      await read.future;
      out.event('channel.closed', {'channel': handle, ...statusJson(status)});
    }());
  }

  /// Reports an error no command answers (a service's, say) as a
  /// `peer.error` event and on stderr, so a test can assert there were none.
  void reportError(Object error, StackTrace stack) {
    stderr.writeln('dart_peer: unhandled $error\n$stack');
    out.event('peer.error', {'error': errorJson(error)});
  }

  /// Closes every handle's object, newest first, and forgets them all.
  /// The reconnecting clients stop first: closing their connections and
  /// their persistent channels' channels would otherwise make them
  /// reconnect and open those again.
  Future<int> closeAll() async {
    final objects = handles.all;
    handles.clear();
    for (final o in objects) {
      if (o is ReconnectingClient) {
        o.stop();
      }
    }
    for (final o in objects) {
      try {
        switch (o) {
          case final TalkChannel t:
            await t.close().timeout(_closeTimeout, onTimeout: () {});
          case final MuxChannel c:
            await c.close().timeout(_closeTimeout, onTimeout: () {});
          case final SlotChannel c:
            await c.close().timeout(_closeTimeout, onTimeout: () {});
          case final MuxConnection c:
            await c.close().timeout(_closeTimeout, onTimeout: () {});
          case final TalkStream s:
            s.cancel();
          case final NamingHandle n:
            await n.close().timeout(_closeTimeout, onTimeout: () {});
          case final PersistentChannel p:
            await p.close().timeout(_closeTimeout, onTimeout: () {});
          case final PersistentTalk p:
            await p.close().timeout(_closeTimeout, onTimeout: () {});
          case final ReconnectingClient r:
            await r.close().timeout(_closeTimeout, onTimeout: () {});
        }
      } on Object catch (_) {
        // Already closed or failing: nothing left to release.
      }
    }
    return objects.length;
  }

  static const Duration _closeTimeout = Duration(seconds: 2);
}
