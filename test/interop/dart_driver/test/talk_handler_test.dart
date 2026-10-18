/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// A Talk service implemented here, requested by the TS peer (talk_test.dart
// "requester cancels: the channel answers CANCELLED at once and the
// responder sees onCancel", talk_window_test.dart "a finite window delivers
// exactly that many items, then nothing until a grant", talk_bulk_test.dart
// "over TCP"): the item window and the credit of a TS stream request as the
// Dart handler reads them, the TS requester's timeout and cancel reaching
// the handler, and TS bulk payloads against the Dart responder's per-message
// limit, which a request over it never reaches the handler past. The
// TS-driven cells see only what the TS requester gets back.

import 'dart:async';
import 'dart:convert';
import 'dart:io' show InternetAddress;

import 'package:crypto/crypto.dart';
import 'package:switchboard/switchboard.dart';
import 'package:switchboard_dart_driver/ts_peer.dart';
import 'package:switchboard_dart_peer/switchboard_dart_peer.dart';
import 'package:test/test.dart';

import 'support.dart';

/// The largest bulk payload the Dart service reassembles.
const int inlineLimit = 100000;

/// What the Dart service saw, for waiting on.
class Seen {
  final List<String> log = [];
  int stalls = 0;
  final StreamController<void> _changed = StreamController.broadcast();

  void add(String entry) {
    log.add(entry);
    _changed.add(null);
  }

  Future<void> until(bool Function() condition) =>
      whenever(_changed.stream, condition);

  Future<void> close() => _changed.close();
}

/// `<length> <sha256>` of [bytes].
String sizeOf(List<int> bytes) => '${bytes.length} ${sha256.convert(bytes)}';

/// The service: `ECHO`; `SIZE` of the reassembled payload; `STREAM`, the
/// size of a payload read as it arrives; `WIN`, items as the window allows;
/// `STALL`, never answered. Every request is logged as it is delivered.
void serve(TalkMessage m, Seen seen) {
  final how = m.isBulk ? 'bulk ${m.bulkLength}' : 'inline';
  switch (m.procedureName) {
    case 'ECHO':
      unawaited(m.reply(m.payload));
    case 'SIZE':
      seen.add('SIZE $how');
      unawaited(m.payloadBytes().then((bytes) => m.reply(text(sizeOf(bytes)))));
    case 'STREAM':
      seen.add('STREAM $how');
      unawaited(() async {
        final output = AccumulatorSink<Digest>();
        final input = sha256.startChunkedConversion(output);
        var length = 0;
        var chunks = 0;
        await for (final chunk in m.bulk) {
          input.add(chunk);
          length += chunk.length;
          chunks++;
        }
        input.close();
        seen.add('STREAM read in ${chunks > 1 ? 'pieces' : 'one'}');
        await m.reply(text('$length ${output.events.single}'));
      }());
    case 'WIN':
      unawaited(() async {
        seen.add('WIN stream ${m.expectsStream} window ${m.window}');
        await m.replyItem(text('1'));
        await m.replyItem(text('2'));
        seen.add('WIN window ${m.window}');
        final third = m.replyItem(text('3'));
        seen.add('WIN waiting');
        await third;
        seen.add('WIN window ${m.window}');
        await m.reply(text('done'));
      }());
    case 'STALL':
      final n = ++seen.stalls;
      seen.add('STALL $n ${m.expectsStream ? 'stream' : 'request'}');
      unawaited(
        m.onCancel.then(
          (_) => seen.add('STALL $n cancelled, canReply ${m.canReply}'),
        ),
      );
    default:
      unawaited(m.replyAbort(Status.of(StatusCode.unimplemented)));
  }
}

/// A sink keeping what was added, for the chunked SHA-256.
class AccumulatorSink<T> implements Sink<T> {
  final List<T> events = [];

  @override
  void add(T event) => events.add(event);

  @override
  void close() {}
}

void main() {
  for (final transport in ['tcp', 'ws']) {
    group('a Dart Talk service requested by the TS peer over $transport', () {
      final f = Fixture();
      late Switchboard dart;
      late Seen seen;
      late String talk;
      setUpAll(() => f.start({'listen': <String>[]}));
      tearDownAll(() async => f.peer?.stop());
      setUp(() async {
        if (f.peer == null) return;
        seen = Seen();
        dart = dartNode();
        dart.registerService(Name('svc'), (incoming) {
          incoming
              .talk(
                options: TalkOptions(
                  maxInlinePayload: inlineLimit,
                  streamBulk: (m) => m.procedureName == 'STREAM',
                ),
              )
              .messages
              .listen((m) => serve(m, seen));
        });
        final endpoint = transport == 'tcp'
            ? await dart.listenTcp(InternetAddress.loopbackIPv4, 0)
            : await dart.listenWebSocket(
                InternetAddress.loopbackIPv4,
                0,
                path: '/sb',
              );
        final opened = await f.ts.rpc<Map<String, Object?>>('talk.open', {
          'endpoint': '$endpoint',
          'address': {'type': 'svc'},
        });
        talk = opened['talk']! as String;
      });
      tearDown(() async {
        if (f.peer == null) return;
        await f.afterEach();
        await dart.close();
        await seen.close();
      });

      Future<Map<String, Object?>> request(Map<String, Object?> args) => f.ts
          .rpc<Map<String, Object?>>('talk.request', {'talk': talk, ...args});

      Future<PeerError> refused(Map<String, Object?> args) async {
        try {
          await request(args);
        } on PeerError catch (e) {
          return e;
        }
        fail('not refused: $args');
      }

      test("the TS requester's item window and credit, its timeout and its "
          'cancel, as the Dart handler sees them', () async {
        if (!f.runs) return;
        final started = await f.ts.rpc<Map<String, Object?>>('talk.stream', {
          'talk': talk,
          'procedure': 'WIN',
          'window': 2,
        });
        final stream = started['stream'];
        bool item(Map<String, Object?> e) =>
            e['event'] == 'talk.item' && e['stream'] == stream;
        await seen.until(() => seen.log.contains('WIN waiting'));
        // A round trip on the channel: the third item is not on the wire.
        await request({'procedure': 'ECHO', 'text': 'barrier'});
        expect(f.ts.events.where(item), hasLength(2));
        expect(seen.log, [
          'WIN stream true window 2',
          'WIN window 0',
          'WIN waiting',
        ]);
        await f.ts.rpc<Object?>('stream.grant', {'stream': stream, 'items': 3});
        final done = await f.ts.waitFor(
          (e) => e['event'] == 'talk.done' && e['stream'] == stream,
        );
        expect([done['items'], done['payload']], [3, hex(text('done'))]);
        // Three granted, one taken by the third item.
        expect(seen.log.last, 'WIN window 2');

        // The TS requester's timeout cancels the request at the Dart side.
        final timedOut = await refused({
          'procedure': 'STALL',
          'timeoutMs': 200,
        });
        expect(timedOut.code, StatusCode.deadlineExceeded.code);
        await seen.until(
          () => seen.log.contains('STALL 1 cancelled, canReply false'),
        );
        // So does its cancel of a stream request.
        final stalled = await f.ts.rpc<Map<String, Object?>>('talk.stream', {
          'talk': talk,
          'procedure': 'STALL',
          'window': 0,
        });
        await seen.until(() => seen.log.contains('STALL 2 stream'));
        await f.ts.rpc<Object?>('stream.cancel', {'stream': stalled['stream']});
        final cancelled = await f.ts.waitFor(
          (e) => e['event'] == 'talk.done' && e['stream'] == stalled['stream'],
        );
        expect(
          (cancelled['error']! as Map<String, Object?>)['code'],
          StatusCode.cancelled.code,
        );
        await seen.until(
          () => seen.log.contains('STALL 2 cancelled, canReply false'),
        );
      });

      test('TS bulk payloads against the Dart limit: reassembled up to it; '
          'beyond it, declared or not, refused RESOURCE_EXHAUSTED before the '
          'handler; read as it arrives, whatever its length', () async {
        if (!f.runs) return;
        const under = inlineLimit - 10000;
        final small = await request({
          'procedure': 'SIZE',
          'patternLength': under,
        });
        expect(textOf(bytesOf(small['payload'])), sizeOf(pattern(under)));
        expect(small['bulk'], isFalse);

        // Over it with its length declared: refused at once.
        final declared = await refused({
          'procedure': 'SIZE',
          'patternLength': 3 * inlineLimit,
        });
        expect(declared.code, StatusCode.resourceExhausted.code);
        // Over it, the length unknown: refused once that much arrived.
        final undeclared = await refused({
          'procedure': 'SIZE',
          'bulkPattern': {'length': 3 * inlineLimit, 'declared': false},
        });
        expect(undeclared.code, StatusCode.resourceExhausted.code);

        // The channel carries on, in order.
        final after = await request({'procedure': 'SIZE', 'text': 'after'});
        expect(textOf(bytesOf(after['payload'])), sizeOf(utf8.encode('after')));
        // A payload read as it arrives is not held to the limit.
        const big = 10 * inlineLimit + 7;
        final streamed = await request({
          'procedure': 'STREAM',
          'patternLength': big,
        });
        expect(textOf(bytesOf(streamed['payload'])), sizeOf(pattern(big)));
        expect(seen.log, [
          'SIZE bulk $under',
          'SIZE inline',
          'STREAM bulk $big',
          'STREAM read in pieces',
        ]);
      });
    });
  }
}
