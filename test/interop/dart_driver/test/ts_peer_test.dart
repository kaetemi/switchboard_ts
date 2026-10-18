/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Dart-written scenarios against the TS peer: the Dart Switchboard in
// process, fully observable, as the Dart repositories' own two-peer tests
// have it, and the TS port behind the peer protocol (test/interop/README.md,
// "Dart driver"). The TS peer listens on each transport in turn; a group
// whose transport it cannot serve (QUIC without the native addon) is
// skipped with the reason.

import 'dart:async';
import 'dart:io' show InternetAddress;
import 'dart:math';
import 'dart:typed_data';

import 'package:crypto/crypto.dart';
import 'package:switchboard/switchboard.dart';
import 'package:switchboard_dart_peer/switchboard_dart_peer.dart';
import 'package:switchboard_quic/switchboard_quic.dart';
import 'package:switchboard_quic/switchboard_quic_native.dart';
import 'package:test/test.dart';

import 'support.dart';

void main() {
  for (final transport in ['tcp', 'ws', 'quic', 'webtransport']) {
    group('a Dart node against the TS peer over $transport', () {
      final f = Fixture();
      late Switchboard dart;
      setUpAll(
        () => f.start({
          'listen': [transport],
        }),
      );
      tearDownAll(() async => f.peer?.stop());
      setUp(() => dart = dartNode());
      tearDown(() async {
        await dart.close();
        await f.afterEach();
      });

      Future<TalkChannel> echo() => dart.openTalkAt(
        f.ts.endpoint(transport),
        ChannelAddress(type: Name('echo'), instance: 1),
      );

      test('requests on one channel, answered in any order', () async {
        if (!f.runs) return;
        final talk = await echo();
        final replies = await Future.wait([
          for (var i = 0; i < 100; i++) talk.request('UPPER', text('r$i')),
        ]).timeout(limit);
        for (var i = 0; i < 100; i++) {
          expect(textOf(await replies[i].payloadBytes()), 'R$i');
        }
      });

      test('a stream request with an item window and a grant', () async {
        if (!f.runs) return;
        final talk = await echo();
        final stream = talk.streamRequest('COUNT', text('4'), window: 2);
        final items = <String>[];
        final done = () async {
          await for (final item in stream.items) {
            items.add(textOf(await item.payloadBytes()));
          }
          return textOf(await (await stream.done).payloadBytes());
        }();
        while (items.length < 2) {
          await Future<void>.delayed(const Duration(milliseconds: 5));
        }
        // A round trip on the channel: no third item came meanwhile.
        await talk.request('ECHO', text('barrier')).timeout(limit);
        expect(items, ['1', '2']);
        stream.grant(2);
        expect(await done.timeout(limit), 'done');
        expect(items, ['1', '2', '3', '4']);
      });

      test('an abort with its exact status, a MOVED byte for byte', () async {
        if (!f.runs) return;
        final talk = await echo();
        await expectLater(
          talk.request('ABORT', text('300 too busy')),
          throwsA(
            isA<SwitchboardException>()
                .having((e) => e.status.code, 'code', 300)
                .having((e) => e.status.reason, 'reason', 'too busy'),
          ),
        );
        final moved = MovedStatus(owner: 0x1a2b, epoch: 7).encode();
        try {
          await talk.request('STATUS', moved);
          fail('no abort');
        } on SwitchboardException catch (e) {
          expect(e.status.encode(), moved);
          expect(MovedStatus.fromStatus(e.status).owner, 0x1a2b);
        }
      });

      test('bulk payloads both ways', () async {
        if (!f.runs) return;
        final talk = await echo();
        const down = 3 * 1024 * 1024 + 7;
        final big = await talk.request('BIG', text('$down')).timeout(limit);
        expect(big.isBulk, isTrue);
        expect(await big.payloadBytes(), pattern(down));
        const up = 2 * 1024 * 1024 + 3;
        final size = await talk.request('SIZE', pattern(up)).timeout(limit);
        expect(
          textOf(await size.payloadBytes()),
          '$up ${sha256.convert(pattern(up))}',
        );
      });

      test('a plain message from the responder', () async {
        if (!f.runs) return;
        final talk = await echo();
        final note = talk.messages.first;
        final reply = await talk.request('NOTIFY', text('note')).timeout(limit);
        expect(textOf(await reply.payloadBytes()), 'sent');
        final message = await note.timeout(limit);
        expect(message.procedureName, 'NOTE');
        expect(textOf(await message.payloadBytes()), 'note');
      });

      test("the TS peer's GOAWAY reaches the Dart connection", () async {
        if (!f.runs) return;
        final connection = await dart.dial(f.ts.endpoint(transport));
        final talk = TalkChannel(
          connection.open(
            ChannelAddress(type: Name('echo'), instance: 1).encode(),
          ),
        );
        await talk.request('GOAWAY', Uint8List(0)).timeout(limit);
        expect(
          (await connection.peerGoAwayStatus.timeout(limit)).known,
          StatusCode.goingAway,
        );
      });

      test('a raw channel both ways, and its close', () async {
        if (!f.runs) return;
        final connection = await dart.dial(f.ts.endpoint(transport));
        final channel = connection.open(
          ChannelAddress(
            type: Name('raw'),
            instance: 1,
            payload: text('hi'),
          ).encode(),
        );
        final incoming = await f.ts.waitFor(
          (e) => e['event'] == 'channel.incoming',
        );
        expect(incoming['payload'], hex(text('hi')));
        final far = incoming['channel'];
        final received = channel.stream.first;
        await f.ts.rpc<Object?>('channel.send', {
          'channel': far,
          'hex': 'AB CD',
        });
        expect(await received.timeout(limit), [0xab, 0xcd]);
        await channel.send(Uint8List.fromList([1, 2, 3]));
        await f.ts.waitFor(
          (e) => e['event'] == 'channel.data' && e['hex'] == '01 02 03',
        );
        await channel.close(Status.of(StatusCode.cancelled, 'bye'));
        final closed = await f.ts.waitFor(
          (e) => e['event'] == 'channel.closed' && e['channel'] == far,
        );
        expect(closed['code'], StatusCode.cancelled.code);
        expect(closed['reason'], 'bye');
      });
    });
  }

  for (final transport in ['tcp', 'quic']) {
    group('identity with the TS peer over $transport', () {
      final f = Fixture();
      setUpAll(
        () => f.start({
          'listen': [transport],
          'identity': {
            'verify': true,
            'credential': {'identity': 'ts-server'},
            'identifyInReturn': true,
            'quicRules': 'listener',
          },
        }),
      );
      tearDownAll(() async => f.peer?.stop());
      tearDown(f.afterEach);

      test(
        'a Dart holder-key credential identifies, and the TS peer in return',
        () async {
          if (!f.runs) return;
          final identity = await NodeIdentity.of(
            IdentityConfig.fromJson({
              'verify': true,
              'credential': {'identity': 'dart-client'},
            }),
          );
          final node = Switchboard(
            localConnections: false,
            muxOptions: fast,
            credential: identity.credential,
            holderKey: identity.holderKey,
            verifier: identity.verifier,
            bindings: [
              QuicSwitchboardBinding(
                NativeQuicProvider(),
                webTransportCertificateVerified: false,
              ),
            ],
          );
          addTearDown(node.close);
          final connection = await node.connect(f.ts.endpoint(transport));
          expect(
            (await connection.peerIdentified.timeout(limit)).identity,
            'ts-server',
          );
          final talk = await node.openTalkAt(
            f.ts.endpoint(transport),
            ChannelAddress(type: Name('echo'), instance: 1),
          );
          final reply = await talk
              .request('WHOAMI', Uint8List(0))
              .timeout(limit);
          expect(textOf(await reply.payloadBytes()), 'dart-client');
          final seen = await f.ts.waitFor(
            (e) => e['event'] == 'connection.identified',
          );
          expect(seen['provedPossession'], isTrue);
        },
      );
    });
  }

  for (final transport in ['tcp', 'ws']) {
    group('the TS peer against a Dart node over $transport', () {
      final f = Fixture();
      late Switchboard dart;
      late Uri endpoint;
      setUpAll(() => f.start({'listen': <String>[]}));
      tearDownAll(() async => f.peer?.stop());
      setUp(() async {
        dart = dartNode();
        // A service implemented here: upper-cases, and streams the letters.
        dart.registerService(Name('upper'), (incoming) {
          final talk = incoming.talk();
          talk.messages.listen((m) async {
            final payload = await m.payloadBytes();
            if (m.expectsStream) {
              for (final b in payload) {
                await m.replyItem(Uint8List.fromList([b]));
              }
            }
            await m.reply(asciiUpper(payload));
          });
        });
        endpoint = transport == 'tcp'
            ? await dart.listenTcp(InternetAddress.loopbackIPv4, 0)
            : await dart.listenWebSocket(
                InternetAddress.loopbackIPv4,
                0,
                path: '/sb',
              );
      });
      tearDown(() async {
        await f.afterEach();
        await dart.close();
      });

      test('a request of the TS peer to a Dart service', () async {
        if (!f.runs) return;
        final reply = await f.ts.rpc<Map<String, Object?>>('request', {
          'endpoint': '$endpoint',
          'service': 'upper',
          'procedure': 'X',
          'text': 'from ts',
        });
        expect(reply['payload'], hex(text('FROM TS')));
      });

      test('a stream request of the TS peer, and its items', () async {
        if (!f.runs) return;
        final opened = await f.ts.rpc<Map<String, Object?>>('talk.open', {
          'endpoint': '$endpoint',
          'address': {'type': 'upper'},
        });
        final started = await f.ts.rpc<Map<String, Object?>>('talk.stream', {
          'talk': opened['talk'],
          'procedure': 'X',
          'text': 'abc',
        });
        final done = await f.ts.waitFor(
          (e) => e['event'] == 'talk.done' && e['stream'] == started['stream'],
        );
        expect(done['items'], 3);
        expect(done['payload'], hex(text('ABC')));
      });
    });
  }

  group('codecs: Dart against the TS peer oracle', () {
    final f = Fixture();
    setUpAll(() => f.start({'listen': <String>[]}));
    tearDownAll(() async => f.peer?.stop());

    test('Talk frames the Dart side encodes decode alike in TS', () async {
      if (!f.runs) return;
      final rand = Random(0x5357);
      final frames = <Uint8List>[];
      while (frames.length < 2000) {
        final kind = TalkKind.values[rand.nextInt(4)];
        final request = rand.nextBool();
        final stream = rand.nextInt(5) == 0;
        try {
          frames.add(
            TalkFrame(
              kind: kind,
              procedure: rand.nextBool() ? Name('p${rand.nextInt(99)}') : null,
              requestId: request ? 1 + rand.nextInt(0xFFFFFE) : 0,
              responseId: !request || rand.nextInt(4) == 0
                  ? 1 + rand.nextInt(0xFFFFFE)
                  : 0,
              stream: stream,
              window: stream
                  ? rand.nextInt(1 << 32)
                  : TalkFrame.unlimitedWindow,
              payload: Uint8List.fromList(
                List.generate(rand.nextInt(12), (_) => rand.nextInt(256)),
              ),
            ).encode(),
          );
        } on Object catch (_) {
          // A combination the encoder refuses.
        }
      }
      final answers = await f.ts.rpc<List<Object?>>('decodeMany', {
        'kind': 'talk',
        'hexes': [for (final bytes in frames) hex(bytes)],
      });
      final disagreements = <String>[];
      for (var i = 0; i < frames.length; i++) {
        final a = answers[i]! as Map<String, Object?>;
        final bytes = frames[i];
        Object? dart;
        try {
          dart = hex(TalkFrame.decode(bytes).encode());
        } on Object catch (_) {
          dart = null;
        }
        final ts = a['ok'] == true
            ? (a['value']! as Map<String, Object?>)['reencoded']
            : null;
        if (dart != ts) {
          disagreements.add('${hex(bytes)}: Dart $dart, TS $ts');
        }
      }
      expect(disagreements.take(20), isEmpty);
    });
  });
}
