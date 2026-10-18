/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// The Dart ReconnectingClient against the TS peer as its endpoint
// (reconnecting_client_test.dart "GOAWAY from the endpoint" and "persistent
// channels"; QUIC web_binding_test.dart "ReconnectingClient.connections"):
// the client's own state machine, its persistent channels' ends and
// re-opens, and which connection each channel lives on, when the TS peer
// sends GOAWAY, closes a channel with a status, or drops the connection.
// None of that crosses the peer protocol, so the TS-driven cells cannot see
// it.

import 'dart:async';
import 'dart:typed_data';

import 'package:switchboard/switchboard.dart';
import 'package:switchboard_dart_peer/switchboard_dart_peer.dart';
import 'package:switchboard_quic/switchboard_quic.dart';
import 'package:switchboard_quic/switchboard_quic_native.dart';
import 'package:test/test.dart';

import 'support.dart';

/// What happened to a client and its channels, for waiting on.
class Changes {
  final StreamController<void> _changed = StreamController.broadcast();

  void note() => _changed.add(null);

  Future<void> until(bool Function() condition) =>
      whenever(_changed.stream, condition);

  Future<void> close() => _changed.close();
}

final ChannelAddress echo = ChannelAddress(type: Name('echo'), instance: 1);

ChannelAddress raw(String tag) =>
    ChannelAddress(type: Name('raw'), instance: 1, payload: text(tag));

void main() {
  for (final transport in ['tcp', 'ws', 'quic', 'webtransport']) {
    group('a Dart ReconnectingClient of the TS peer over $transport', () {
      final f = Fixture();
      late Changes changes;
      final clients = <ReconnectingClient>[];
      setUpAll(
        () => f.start({
          'listen': [transport],
        }),
      );
      tearDownAll(() async => f.peer?.stop());
      setUp(() => changes = Changes());
      tearDown(() async {
        for (final c in clients) {
          await c.close().timeout(limit);
        }
        clients.clear();
        await changes.close();
        await f.afterEach();
      });

      ReconnectingClient client() {
        final endpoint = f.ts.endpoint(transport);
        const backoff = Duration(milliseconds: 50);
        const max = Duration(seconds: 1);
        final ReconnectingClient c;
        if (transport == 'tcp') {
          c = ReconnectingClient(
            () => StreamTransport.connectTcp(endpoint.host, endpoint.port),
            muxOptions: fast,
            initialBackoff: backoff,
            maxBackoff: max,
            jitter: 0,
          );
        } else if (transport == 'ws') {
          c = ReconnectingClient(
            () => IOWebSocketTransport.connect(endpoint),
            muxOptions: fast,
            initialBackoff: backoff,
            maxBackoff: max,
            jitter: 0,
          );
        } else {
          final hash = bytesOf(f.ts.ready['certificateSha256']);
          c = ReconnectingClient.connections(
            (options) => connectQuicBinding(
              NativeQuicProvider(),
              endpoint,
              options: options,
              verifyCertificate: false,
              certificateHashes: [hash],
            ),
            muxOptions: fast,
            initialBackoff: backoff,
            maxBackoff: max,
            jitter: 0,
          );
        }
        clients.add(c);
        c.states.listen((_) => changes.note());
        return c;
      }

      test("the TS peer's GOAWAY: persistent channels move to a new "
          'connection at once, an application channel carries on over the '
          'old one', () async {
        if (!f.runs) return;
        final c = client();
        final opened = <TalkChannel>[];
        final closed = <Status>[];
        final subscribed = <String>[];
        final persistent = c.openPersistentTalk(
          echo,
          (talk) async {
            opened.add(talk);
            final reply = await talk.request('UPPER', text('sub'));
            subscribed.add(textOf(reply.payload));
            changes.note();
          },
          onClosed: (status) {
            closed.add(status);
            changes.note();
          },
        );
        await changes.until(() => subscribed.length == 1);
        final first = c.connection!;
        expect(c.state.phase, ClientPhase.connected);
        final own = await c.openTalk(echo).timeout(limit);
        expect((own.raw as MuxChannel).connection, same(first));
        final states = <ClientState>[];
        c.states.skip(1).listen(states.add);

        final going = await own.request('GOAWAY', Uint8List(0)).timeout(limit);
        expect(textOf(going.payload), 'going');
        expect(
          (await first.peerGoAwayStatus.timeout(limit)).known,
          StatusCode.goingAway,
        );
        // Connected again at once, reported with the GOAWAY's status; the
        // persistent channel closed GOING_AWAY and opened on the new one.
        await changes.until(() => subscribed.length == 2);
        expect(states.map((s) => s.phase), [
          ClientPhase.connecting,
          ClientPhase.connected,
        ]);
        expect(
          states.map((s) => s.lastStatus?.known),
          everyElement(StatusCode.goingAway),
        );
        expect(states.map((s) => s.attempt), [1, 1]);
        final second = c.connection!;
        expect(second, isNot(same(first)));
        expect(closed, [hasCode(StatusCode.goingAway)]);
        expect((opened.last.raw as MuxChannel).connection, same(second));
        expect(persistent.current, same(opened.last));
        expect(subscribed, ['SUB', 'SUB']);

        // The application's channel still works on the old connection, which
        // waits for it; new channels go to the new one.
        final still = await own.request('ECHO', text('still')).timeout(limit);
        expect(textOf(still.payload), 'still');
        expect(first.peerGoingAway, isTrue);
        final next = await c.openTalk(echo).timeout(limit);
        expect((next.raw as MuxChannel).connection, same(second));
        expect(
          textOf(
            (await next.request('ECHO', text('n')).timeout(limit)).payload,
          ),
          'n',
        );
        // Once it is closed, the old connection ends, and nothing else
        // reconnects.
        await own.close();
        expect((await first.done.timeout(limit)).known, StatusCode.goingAway);
        await opened.last.request('ECHO', text('barrier')).timeout(limit);
        expect(states, hasLength(2));
        expect(c.connection, same(second));
        expect(opened, hasLength(2));
      });

      test('a persistent channel the TS peer closes PERMISSION_DENIED stays '
          'closed, one closed UNAVAILABLE is re-opened; after the TS peer '
          'drops the connection only the live one comes back', () async {
        if (!f.runs) return;
        final c = client();
        final closedA = <Status>[];
        final closedB = <Status>[];
        final openedB = <MuxChannel>[];
        final a = c.openPersistent(
          raw('a'),
          (_) {},
          onClosed: (s) {
            closedA.add(s);
            changes.note();
          },
        );
        final b = c.openPersistent(
          raw('b'),
          (channel) {
            openedB.add(channel);
            changes.note();
          },
          onClosed: (s) {
            closedB.add(s);
            changes.note();
          },
        );
        bool incoming(Map<String, Object?> e, String tag) =>
            e['event'] == 'channel.incoming' && e['payload'] == hex(text(tag));
        final farA = await f.ts.waitFor((e) => incoming(e, 'a'));
        final farB = await f.ts.waitFor((e) => incoming(e, 'b'));
        expect(farA['connection'], farB['connection']);

        await f.ts.rpc<Object?>('channel.close', {
          'channel': farA['channel'],
          'code': StatusCode.permissionDenied.code,
          'reason': 'no',
        });
        expect(
          await a.done.timeout(limit),
          hasCode(StatusCode.permissionDenied),
        );
        expect(closedA, [hasCode(StatusCode.permissionDenied)]);
        expect(a.isClosed, isTrue);

        // Re-opened after the backoff, on the same connection.
        await f.ts.rpc<Object?>('channel.close', {
          'channel': farB['channel'],
          'code': StatusCode.unavailable.code,
          'reason': 'later',
        });
        final reopened = (await f.ts.waitForCount(
          (e) => incoming(e, 'b'),
          2,
        )).last;
        expect(reopened['connection'], farB['connection']);
        await changes.until(() => openedB.length == 2);
        expect(closedB, [hasCode(StatusCode.unavailable)]);
        expect(b.isClosed, isFalse);

        // The TS peer drops the connection: the client connects again, and
        // opens the live persistent channel there.
        final first = c.connection!;
        await f.ts.rpc<Object?>('connection.close', {
          'connection': farB['connection'],
        });
        final lost = (await first.done.timeout(limit)).known;
        final again = (await f.ts.waitForCount(
          (e) => incoming(e, 'b'),
          3,
        )).last;
        expect(again['connection'], isNot(farB['connection']));
        await changes.until(() => openedB.length == 3 && c.state.isConnected);
        expect(c.connection, isNot(same(first)));
        // Over a stream the transport just ends; over raw QUIC and
        // WebTransport the close is an application close (the session's
        // close capsule over WebTransport), read as a GOAWAY, as
        // scenarios/reconnect.test.ts has it.
        final end = transport == 'quic' || transport == 'webtransport'
            ? StatusCode.goingAway
            : StatusCode.connectionLost;
        expect(lost, end);
        expect(c.state.lastStatus?.known, end);
        expect(closedB, hasLength(2));
        // A round trip on the new connection: the closed channel was not
        // opened before it.
        final talk = await c.openTalk(echo).timeout(limit);
        await talk.request('ECHO', text('barrier')).timeout(limit);
        expect(f.ts.events.where((e) => incoming(e, 'a')), hasLength(1));
        expect(closedA, hasLength(1));
      });
    });
  }
}
