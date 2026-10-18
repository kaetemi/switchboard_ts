/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Limits and flow control seen from the Dart connection (mux_flow_test.dart
// "a slow consumer stalls its own channel, another one flows", "OPEN goes
// at once, DATA once the LIMITS arrived, within the window it announced";
// mux_test.dart "peer OPEN beyond maxChannels gets CLOSE
// RESOURCE_EXHAUSTED"). The TS-driven cells see what crosses the peer
// protocol; here the Dart side reads its own windows, what it holds for a
// reader that does not read, and the LIMITS the TS peer announced.

import 'dart:async';
import 'dart:io' show InternetAddress;
import 'dart:typed_data';

import 'package:switchboard/switchboard.dart';
import 'package:switchboard_dart_peer/switchboard_dart_peer.dart';
import 'package:test/test.dart';

import 'support.dart';

/// [length] bytes of [value].
Uint8List filled(int length, int value) =>
    Uint8List(length)..fillRange(0, length, value);

final Uint8List echoAddress = ChannelAddress(
  type: Name('echo'),
  instance: 1,
).encode();

void main() {
  for (final transport in ['tcp', 'ws']) {
    group('the TS peer sending to a Dart node over $transport', () {
      final f = Fixture();
      late Switchboard dart;
      late Uri endpoint;
      late List<IncomingChannel> incoming;
      late StreamController<void> arrived;
      // The window the Dart node announces: two subframes of 16000 bytes
      // (16016 each at cost) fit, a third does not.
      const window = 40000;
      setUpAll(() => f.start({'listen': <String>[]}));
      tearDownAll(() async => f.peer?.stop());
      setUp(() async {
        dart = dartNode(muxOptions: fast.copyWith(initialWindow: window));
        incoming = [];
        arrived = StreamController.broadcast();
        dart.registerService(Name('raw'), (channel) {
          incoming.add(channel);
          arrived.add(null);
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
        await arrived.close();
      });

      test('a paused Dart reader holds its window and stalls only its '
          'channel; the TS peer keeps sending on another', () async {
        if (!f.runs) return;
        final c = await f.ts.rpc<Map<String, Object?>>('connect', {
          'endpoint': '$endpoint',
        });
        Future<String> open() async =>
            (await f.ts.rpc<Map<String, Object?>>('open', {
                  'connection': c['connection'],
                  'address': {'type': 'raw'},
                }))['channel']!
                as String;
        final slow = await open();
        final quick = await open();
        await whenever(arrived.stream, () => incoming.length == 2);
        final [slowIn, quickIn] = incoming;
        final paused = Received(slowIn.channel.stream);
        addTearDown(paused.cancel);
        paused.subscription.pause();
        final flowing = Received(quickIn.channel.stream);
        addTearDown(flowing.cancel);

        // Three subframes on the paused channel: two fit the window, the
        // third waits for credit at the TS side.
        final sent = <int>[];
        final slowSends = [
          for (var i = 0; i < 3; i++)
            f.ts
                .rpc<Object?>('channel.send', {
                  'channel': slow,
                  'hex': hex(filled(16000, i)),
                })
                .then((_) => sent.add(i)),
        ];
        // Ten on the other channel all go, the Dart reader returning credit.
        for (var i = 0; i < 10; i++) {
          await f.ts.rpc<Object?>('channel.send', {
            'channel': quick,
            'hex': hex(filled(16000, 100 + i)),
          });
        }
        await flowing.count(10);
        // A round trip: whatever the TS peer sent before it has arrived.
        await f.ts.rpc<Object?>('connection.ping', {
          'connection': c['connection'],
        });
        expect(sent, unorderedEquals([0, 1]));
        expect(slowIn.channel.bufferedBytes, 2 * 16016);
        // On a channel the peer opened, the Dart side accepts the larger of
        // the window it announced and the 64 KiB a peer assumes before
        // reading its LIMITS (the OPEN may predate them); the TS peer keeps
        // to the announced one.
        expect(slowIn.channel.receiveWindow, 65536 - 2 * 16016);
        expect(quickIn.channel.bufferedBytes, 0);
        // The connection's backlog: those, and the OPEN payloads it holds.
        final connection = slowIn.connection;
        expect(
          connection.bufferedBytes,
          2 * 16016 + connection.openPayloadBytes,
        );
        expect(connection.isReceivePaused, isFalse);
        expect(
          [for (final d in flowing.frames) d.length == 16000 ? d.first : -1],
          [for (var i = 0; i < 10; i++) 100 + i],
        );

        // Reading again returns the credit, and the third one comes.
        paused.subscription.resume();
        await Future.wait(slowSends).timeout(limit);
        await paused.count(3);
        expect(
          [
            for (final d in paused.frames)
              d.length == 16000 && d.every((b) => b == d.first) ? d.first : -1,
          ],
          [0, 1, 2],
        );
        expect(slowIn.channel.bufferedBytes, 0);
      });
    });
  }

  group('the LIMITS of a TS peer taking two channels', () {
    final f = Fixture();
    late Switchboard dart;
    late Uri endpoint;
    setUpAll(() async {
      await f.start({
        'listen': ['tcp'],
        'mux': {'maxFrameSize': 4096, 'maxChannels': 2, 'initialWindow': 20000},
      });
      if (f.peer != null) {
        endpoint = f.ts.endpoint('tcp');
      }
    });
    tearDownAll(() async => f.peer?.stop());
    setUp(() => dart = dartNode());
    tearDown(() async {
      await dart.close();
      await f.afterEach();
    });

    test('as the Dart connection reads them; an OPEN sent before they '
        'arrived, beyond the channel limit, is refused RESOURCE_EXHAUSTED; a '
        'pooled node opens a second connection at the limit', () async {
      if (!f.runs) return;
      // Three OPENs before the TS peer's LIMITS can have been read: they go
      // at once, and the third is over the limit the TS peer enforces.
      final mux = MuxConnection(
        await StreamTransport.connectTcp(endpoint.host, endpoint.port),
        isInitiator: true,
        options: fast,
      );
      final opened = [for (var i = 0; i < 3; i++) mux.open(echoAddress)];
      expect(
        await opened[2].done.timeout(limit),
        hasCode(StatusCode.resourceExhausted),
      );
      for (var i = 0; i < 2; i++) {
        final reply = await TalkChannel(opened[i])
            .request('ECHO', text('$i'))
            .timeout(limit);
        expect(textOf(reply.payload), '$i');
      }
      final limits = mux.peerLimits!;
      expect(
        [limits.maxFrameSize, limits.maxChannels, limits.initialWindow],
        [4096, 2, 20000],
      );
      expect(mux.peerMaxChannels, 2);
      expect(mux.isOpen, isTrue);
      // The limit now known, the node refuses a third before sending it.
      expect(
        () => mux.open(echoAddress),
        throwsCode(StatusCode.resourceExhausted),
      );
      // The largest subframe: the frame limit less a short channel header.
      expect(opened[0].maxSubframeLength, 4096 - 3);
      await mux.close();

      // The node's pool: two channels on the first connection, the third
      // on another one.
      final first = await dart.connect(endpoint);
      await first.ping().timeout(limit);
      final talks = [
        for (var i = 0; i < 3; i++)
          await dart.openTalkAt(endpoint, ChannelAddress(type: Name('echo'))),
      ];
      final connections = [
        for (final t in talks) (t.raw as MuxChannel).connection,
      ];
      expect(connections[0], same(first));
      expect(connections[1], same(first));
      expect(connections[2], isNot(same(first)));
      for (var i = 0; i < 3; i++) {
        final reply = await talks[i]
            .request('ECHO', text('t$i'))
            .timeout(limit);
        expect(textOf(reply.payload), 't$i');
      }
    });

    test('a channel the TS peer does not read stalls at the window it '
        'announced, and only that channel', () async {
      if (!f.runs) return;
      final connection = await dart.dial(endpoint);
      await connection.ping().timeout(limit);
      final sink = connection.open(
        ChannelAddress(type: Name('sink'), instance: 1).encode(),
      );
      expect(sink.sendWindow, 20000);
      // Four subframes of 4000 bytes (4016 at cost) fit; the fifth waits.
      final sends = [for (var i = 0; i < 5; i++) sink.send(filled(4000, i))];
      var fifthSent = false;
      unawaited(
        sends[4].then((_) {
          fifthSent = true;
        }, onError: (Object _) {}),
      );
      await Future.wait(sends.take(4)).timeout(limit);
      expect(sink.sendWindow, 20000 - 4 * 4016);
      expect(sink.isWritable, isFalse);
      // Another channel carries three times the window meanwhile, the TS
      // peer returning its credit as its service reads.
      final talk = TalkChannel(connection.open(echoAddress));
      for (var i = 0; i < 15; i++) {
        final payload = filled(4000, i);
        final reply = await talk.request('SIZE', payload).timeout(limit);
        expect(textOf(reply.payload), startsWith('4000 '));
      }
      await connection.ping().timeout(limit);
      expect(fifthSent, isFalse);
      expect(sink.sendWindow, 20000 - 4 * 4016);
      expect(sink.canSend, isTrue);
    });
  });
}
