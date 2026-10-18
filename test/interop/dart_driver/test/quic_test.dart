/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// The Dart QUIC binding dialling the TS peer's raw QUIC and WebTransport
// listeners (QUIC native_integration_test.dart "64 channels in parallel on
// one connection, three times over", "GOAWAY with a code and a reason, from
// the listener", "a WT_DRAIN_SESSION reads as a GOAWAY with GOING_AWAY";
// web_binding_test.dart "the connection: HELLO both ways"): what the Dart
// binding read of the TS listener's HELLO, compared with a Dart listener
// configured as the Dart peer configures its own, the QUIC session's end as
// the native stack reports it, and the stream credit of ended channels.
// None of it is visible through the peer protocol.

import 'dart:async';
import 'dart:typed_data';

import 'package:switchboard/switchboard.dart';
import 'package:switchboard_quic/switchboard_quic.dart';
import 'package:switchboard_quic/switchboard_quic_native.dart';
import 'package:test/test.dart';

import 'support.dart';

final ChannelAddress echo = ChannelAddress(type: Name('echo'), instance: 1);

final Uint8List rawAddress = ChannelAddress(
  type: Name('raw'),
  instance: 1,
).encode();

/// A Dart QUIC listener of [transport] as the Dart peer listens (the
/// provider's defaults, no identification rules), serving `echo`.
Future<(Switchboard, QuicSwitchboardListener)> referenceListener(
  String transport,
) async {
  final provider = NativeQuicProvider();
  final cert = NativeQuicProvider.generateSelfSigned('127.0.0.1');
  final config = QuicServerConfig(
    alpn: const [switchboardAlpn],
    certificateDer: cert.certificateDer,
    privateKeyDer: cert.privateKeyDer,
  );
  final node = Switchboard(localConnections: false, muxOptions: fast);
  node.registerService(Name('echo'), (incoming) {
    incoming.talk().messages.listen((m) {
      if (m.expectsReply) {
        unawaited(m.reply(m.payload));
      }
    });
  });
  final QuicListener raw = transport == 'quic'
      ? await provider.listen('127.0.0.1', 0, config)
      : await provider.listenWebTransport('127.0.0.1', 0, config, path: '/sb');
  final listener = await QuicSwitchboardListener.start(
    node,
    raw,
    identityRules: QuicIdentityRules.none,
    identifyInReturn: false,
  );
  return (node, listener);
}

/// Whether [a] and [b] hold the same bytes.
bool same(List<int> a, List<int> b) {
  if (a.length != b.length) {
    return false;
  }
  for (var i = 0; i < a.length; i++) {
    if (a[i] != b[i]) {
      return false;
    }
  }
  return true;
}

void main() {
  for (final transport in ['quic', 'webtransport']) {
    group('the Dart QUIC binding against the TS peer over $transport', () {
      final f = Fixture();
      late QuicSwitchboardBinding binding;
      late Switchboard dart;
      Switchboard? referenceNode;
      QuicSwitchboardListener? reference;
      setUpAll(() async {
        await f.start({
          'listen': [transport],
          // The listener's GOAWAY ends the session once its grace expires.
          'mux': {'goAwayGraceMs': 200},
        });
        if (f.peer != null) {
          (referenceNode, reference) = await referenceListener(transport);
        }
      });
      tearDownAll(() async {
        await reference?.close();
        await referenceNode?.close();
        await f.peer?.stop();
      });
      setUp(() {
        binding = QuicSwitchboardBinding(
          NativeQuicProvider(),
          webTransportCertificateVerified: false,
        );
        dart = Switchboard(
          localConnections: false,
          muxOptions: fast,
          bindings: [binding],
        );
      });
      tearDown(() async {
        await dart.close();
        await f.afterEach();
      });

      test("the TS listener's HELLO is a Dart listener's; its GOAWAY with a "
          'code and a reason ends the QUIC session with them', () async {
        if (!f.runs) return;
        final connection =
            await dart.dial(f.ts.endpoint(transport)) as QuicBindingConnection;
        final dartListener =
            await dart.dial(reference!.endpoint) as QuicBindingConnection;
        await connection.ping().timeout(limit);
        await dartListener.ping().timeout(limit);
        expect(connection.isWebTransport, transport == 'webtransport');
        final ts = connection.peerHello!;
        final expected = dartListener.peerHello!;
        expect(
          [ts.recordLimit, ts.channels, ts.exporter],
          [expected.recordLimit, expected.channels, expected.exporter],
        );
        expect(connection.peerRecordLimit, dartListener.peerRecordLimit);
        expect(connection.peerMaxChannels, dartListener.peerMaxChannels);
        expect(connection.hasExporter, dartListener.hasExporter);
        await dartListener.goAway();

        final channel = connection.open(rawAddress);
        final far = await f.ts.waitFor((e) => e['event'] == 'channel.incoming');
        unawaited(
          f.ts
              .rpc<Object?>('connection.goAway', {
                'connection': far['connection'],
                'code': StatusCode.resourceExhausted.code,
                'reason': 'too busy',
              })
              .catchError((Object _) => null),
        );
        final status = await connection.peerGoAwayStatus.timeout(limit);
        expect([status.code, status.reason], [8, 'too busy']);
        // The open channel keeps the connection until the grace expires; the
        // listener then closes the session with the GOAWAY's code and reason.
        final end = await connection.session.done.timeout(limit);
        expect(
          (end.code, end.reason, end.byPeer, end.lost),
          (8, 'too busy', true, false),
        );
        expect(
          await channel.done.timeout(limit),
          hasCode(StatusCode.goingAway),
        );
        expect(
          await connection.done.timeout(limit),
          hasCode(StatusCode.goingAway),
        );
      });

      test('64 channels in parallel on one connection, three times over, '
          'every eighth with a bulk payload: the stream credit of ended '
          'channels comes back', () async {
        if (!f.runs) return;
        final endpoint = f.ts.endpoint(transport);
        for (var round = 0; round < 3; round++) {
          final results = await Future.wait([
            for (var i = 0; i < 64; i++)
              () async {
                final talk = await dart.openTalkAt(endpoint, echo);
                final payload = Uint8List.fromList([
                  for (var j = 0; j < (i % 8 == 0 ? 100000 + i : 100 + i); j++)
                    (j * 31 + i + round) & 0xFF,
                ]);
                final reply = await talk.request('ECHO', payload);
                final ok = same(await reply.payloadBytes(), payload);
                await talk.close();
                return ok;
              }(),
          ]).timeout(const Duration(seconds: 60));
          expect(results.every((ok) => ok), isTrue);
        }
        expect(binding.dialled, hasLength(1));
        final connection = binding.dialled.single;
        expect(connection.peerMaxChannels, greaterThanOrEqualTo(72));
        // The last closes are confirmed after their futures complete.
        final watch = Stopwatch()..start();
        while (connection.openChannelCount > 0 && watch.elapsed < limit) {
          await connection.ping().timeout(limit);
        }
        expect(connection.openChannelCount, 0);
        expect(connection.isOpen, isTrue);
      });

      if (transport == 'webtransport') {
        test('a WT_DRAIN_SESSION from the TS listener reads as a GOAWAY with '
            'GOING_AWAY; the Dart dialler closes once its channels are '
            'done', () async {
          if (!f.runs) return;
          final connection = await dart.dial(
            f.ts.endpoint(transport),
          ) as QuicBindingConnection;
          final rawChannel = connection.open(rawAddress);
          final talk = TalkChannel(connection.open(echo.encode()));
          await talk.request('ECHO', text('x')).timeout(limit);
          final far = await f.ts.waitFor(
            (e) => e['event'] == 'channel.incoming',
          );
          await f.ts.rpc<Object?>('connection.drain', {
            'connection': far['connection'],
          });
          expect(
            await connection.peerGoAwayStatus.timeout(limit),
            hasCode(StatusCode.goingAway),
          );
          expect(connection.peerGoingAway, isTrue);
          // The channels open at the drain still work, and no new one opens.
          final still = await talk.request('ECHO', text('still'));
          expect(textOf(still.payload), 'still');
          expect(
            () => connection.open(rawAddress),
            throwsCode(StatusCode.failedPrecondition),
          );
          await rawChannel.close();
          await talk.close().timeout(limit);
          expect(
            await connection.done.timeout(limit),
            hasCode(StatusCode.goingAway),
          );
          // Closed by the Dart dialler itself, not lost.
          final end = await connection.session.done.timeout(limit);
          expect((end.byPeer, end.lost), (false, false));
          expect(
            await f.ts.waitFor(
              (e) =>
                  e['event'] == 'channel.closed' &&
                  e['channel'] == far['channel'],
            ),
            containsPair('code', StatusCode.ok.code),
          );
        });
      }
    });
  }
}
