/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Dart sharded instances, Dart naming clients and a Dart slot router
// against the TS peer's naming service, with the TS peer's `raw` service as
// the backend the router reaches (sharding_test.dart "CLAIM and RELEASE"
// and "MIGRATE", sharding_client_test.dart, slot_routing_test.dart
// "MOVED"). The TS peer's own naming clients accept every slot request, so
// the TS-driven cells never see a refusal; here the Dart SlotHandlers refuse,
// and the test reads what each was asked, in which order and with which
// fields, what the Dart mirror of the TS slot table shows, and how the Dart
// router's MOVED retry reads a TS backend's CLOSE.

import 'dart:async';

import 'package:switchboard/switchboard.dart';
import 'package:switchboard_dart_peer/switchboard_dart_peer.dart';
import 'package:test/test.dart';

import 'support.dart';

/// The type the TS peer's `raw` service serves at every instance.
final Name svc = Name('svc');

/// The slot requests the Dart instances were asked, in order, with the
/// instances named by [names].
class Log {
  final List<String> entries = [];
  final StreamController<void> _changed = StreamController.broadcast();
  final Map<int, String> names = {0: '0'};

  void add(String entry) {
    entries.add(entry);
    _changed.add(null);
  }

  String name(int instance) => names[instance] ?? '$instance';

  /// Completes once [count] entries are logged.
  Future<void> reaches(int count) =>
      whenever(_changed.stream, () => entries.length >= count);
}

/// A [SlotHandler] that logs every request and answers as told.
class Recorder extends SlotHandler {
  Recorder(this.name, this.log);

  final String name;
  final Log log;
  Future<AssignResult> Function(AssignRequest request) assign = (_) async =>
      AssignResult.holding;
  Future<void> Function(DrainRequest request) drain = (_) async {};

  @override
  Future<AssignResult> onAssign(
    AssignRequest request,
    SlotRequestContext context,
  ) {
    log.add(
      '$name ASSIGN ${request.slot} e${request.epoch} '
      'h${log.name(request.holder)}',
    );
    return assign(request);
  }

  @override
  Future<void> onDrain(DrainRequest request, SlotRequestContext context) {
    log.add(
      '$name DRAIN ${request.slot} e${request.epoch} '
      'to${log.name(request.to)}',
    );
    return drain(request);
  }

  @override
  Future<void> onForward(
    ForwardRequest request,
    SlotRequestContext context,
  ) async => log.add(
    '$name FORWARD ${request.slot} e${request.epoch} '
    'to${log.name(request.to)}',
  );

  @override
  Future<void> onResume(
    ResumeRequest request,
    SlotRequestContext context,
  ) async => log.add('$name RESUME ${request.slot} e${request.epoch}');

  @override
  Future<void> onRevoke(Name type, int slot) async =>
      log.add('$name REVOKE $slot');
}

/// A naming client of the TS naming service on a node of its own, whose
/// resolver is a [NamingResolver] over it.
class Client {
  Client._(this.node, this.naming);

  final Switchboard node;
  final NamingClient naming;

  static Future<Client> start(Uri endpoint) async {
    late final Switchboard node;
    final naming = NamingClient(
      () => node.openTalkAt(
        endpoint,
        ChannelAddress(type: Services.naming, instance: 1),
      ),
      reconnectDelay: const Duration(milliseconds: 100),
    );
    node = dartNode(
      resolver: NamingResolver(naming),
      slotRefreshTimeout: const Duration(milliseconds: 500),
    );
    await naming.start();
    await naming.firstSynced.timeout(limit);
    return Client._(node, naming);
  }

  /// Completes once the mirrored slot table shows [entry] for [slot].
  Future<void> mirrors(Name type, int slot, SlotEntry entry) =>
      whenever(
        naming.slotEvents,
        () => naming.slotTables[type]?[slot] == entry,
      ).catchError(
        (Object e) => fail('$type/$slot: ${naming.slotTables[type]?[slot]}'),
      );

  Future<void> close() async {
    await naming.close();
    await node.close();
  }
}

/// The phases a migration went through, then `failed <code>` if it failed.
Future<List<String>> migrate(
  NamingClient client,
  Name type,
  int slot,
  int to,
) async {
  final phases = <String>[];
  try {
    await for (final item in client.migrate(type, slot, to: to)) {
      phases.add(item.phase.name);
    }
  } on SwitchboardException catch (e) {
    phases.add('failed ${e.status.known?.name}');
  }
  return phases;
}

void main() {
  group('sharding against the TS naming service', () {
    final f = Fixture();
    late Uri endpoint;
    late Log log;
    final clients = <Client>[];
    setUpAll(() async {
      await f.start({
        'listen': ['tcp'],
        'naming': <String, Object?>{},
        'services': [
          {
            'type': 'svc',
            'behaviour': 'raw',
            'instance': 1,
            'anyInstance': true,
          },
        ],
      });
      if (f.peer != null) {
        endpoint = f.ts.endpoint('tcp');
      }
    });
    tearDownAll(() async => f.peer?.stop());
    setUp(() => log = Log());
    tearDown(() async {
      for (final c in clients) {
        await c.close();
      }
      clients.clear();
      await f.afterEach();
    });

    Future<Client> client() async {
      final c = await Client.start(endpoint);
      clients.add(c);
      return c;
    }

    /// A sharded instance [name] of [type] with the requested [id], its
    /// endpoint the TS peer's, a static space of 4 slots defined.
    Future<(Client, Recorder)> instance(String name, Name type, int id) async {
      final c = await client();
      final handler = Recorder(name, log);
      c.naming.slotHandler = handler;
      expect(
        await c.naming.register(type, [endpoint], instance: id).timeout(limit),
        id,
      );
      log.names[id] = name;
      await c.naming
          .defineSlots(type, count: 4, mode: SlotMode.static, capacity: 0)
          .timeout(limit);
      return (c, handler);
    }

    test('claims are confirmed through ASSIGN to the Dart handler: the '
        'holder it names, notHolding keeping the holder, a refusal failing '
        'the claim', () async {
      if (!f.runs) return;
      final type = Name('kv-c');
      final (a, _) = await instance('A', type, 911);
      final (b, bHandler) = await instance('B', type, 912);
      expect(await a.naming.claim(type, 1).timeout(limit), 1);
      expect(log.entries, ['A ASSIGN 1 e1 h0']);
      await b.mirrors(
        type,
        1,
        const SlotEntry.owned(911, holder: 911, epoch: 1),
      );
      // Claiming an owned slot again is answered without an ASSIGN.
      expect(await a.naming.claim(type, 1).timeout(limit), 1);
      expect(log.entries, hasLength(1));

      // A claimant holding the storage of a slot nobody holds.
      expect(await a.naming.claim(type, 2, holding: true).timeout(limit), 1);
      expect(log.entries.last, 'A ASSIGN 2 e1 hA');
      // Released keeping the storage: the next owner is told where it is,
      // and answering notHolding leaves the holder where it was.
      await a.naming.release(type, 2, keepStorage: true).timeout(limit);
      await b.mirrors(type, 2, const SlotEntry.free(holder: 911, epoch: 1));
      bHandler.assign = (_) async => AssignResult.notHolding;
      expect(await b.naming.claim(type, 2).timeout(limit), 2);
      expect(log.entries.last, 'B ASSIGN 2 e2 hA');
      await a.mirrors(
        type,
        2,
        const SlotEntry.owned(912, holder: 911, epoch: 2),
      );
      expect(b.naming.servedSlots(type), {2: 2});

      // A refused ASSIGN fails the claim with UNAVAILABLE, whatever the
      // handler's status, and the slot stays free.
      bHandler.assign = (_) async =>
          throw SwitchboardException.of(StatusCode.resourceExhausted, 'full');
      await expectLater(
        b.naming.claim(type, 3).timeout(limit),
        throwsCode(StatusCode.unavailable),
      );
      expect(log.entries.last, 'B ASSIGN 3 e1 h0');
      // A handler failing with anything else is answered UNAVAILABLE.
      bHandler.assign = (_) async => throw StateError('broken');
      await expectLater(
        b.naming.claim(type, 3).timeout(limit),
        throwsCode(StatusCode.unavailable),
      );
      expect(log.entries.last, 'B ASSIGN 3 e1 h0');
      expect(b.naming.servedSlots(type), {2: 2});
      expect(
        await a.naming.locate(type, 3).timeout(limit),
        isA<LocateResponse>()
            .having((r) => r.state, 'state', SlotState.free)
            .having((r) => r.owner, 'owner', 0),
      );
      expect(log.entries, [
        'A ASSIGN 1 e1 h0',
        'A ASSIGN 2 e1 hA',
        'B ASSIGN 2 e2 hA',
        'B ASSIGN 3 e1 h0',
        'B ASSIGN 3 e1 h0',
      ]);
    });

    test('a hand-over the Dart instances refuse is rolled back: RESUME to '
        'the old owner, its epoch unchanged', () async {
      if (!f.runs) return;
      final type = Name('kv-r');
      final (a, aHandler) = await instance('A', type, 921);
      final (b, bHandler) = await instance('B', type, 922);
      await a.naming.claim(type, 1).timeout(limit);
      await a.naming.claim(type, 2).timeout(limit);
      log.entries.clear();

      // The new owner refuses the ASSIGN.
      bHandler.assign = (_) async =>
          throw SwitchboardException.of(StatusCode.unavailable, 'not ready');
      expect(await migrate(a.naming, type, 1, 922).timeout(limit), [
        'draining',
        'assigning',
        'rolledBack',
        'failed unavailable',
      ]);
      await log.reaches(3);
      expect(log.entries, [
        'A DRAIN 1 e2 toB',
        'B ASSIGN 1 e2 hA',
        'A RESUME 1 e1',
      ]);
      await b.mirrors(
        type,
        1,
        const SlotEntry.owned(921, holder: 921, epoch: 1),
      );
      expect(a.naming.servedSlots(type), {1: 1, 2: 1});
      expect(b.naming.servedSlots(type), isEmpty);

      // The old owner refuses the DRAIN: nothing is assigned.
      log.entries.clear();
      aHandler.drain = (_) async =>
          throw SwitchboardException.of(StatusCode.aborted, 'busy');
      expect(await migrate(a.naming, type, 2, 922).timeout(limit), [
        'draining',
        'rolledBack',
        'failed unavailable',
      ]);
      await log.reaches(2);
      expect(log.entries, ['A DRAIN 2 e2 toB', 'A RESUME 2 e1']);
      await b.mirrors(
        type,
        2,
        const SlotEntry.owned(921, holder: 921, epoch: 1),
      );

      // Both accept: the hand-over goes through, in protocol order.
      log.entries.clear();
      aHandler.drain = (_) async {};
      bHandler.assign = (_) async => AssignResult.holding;
      expect(await migrate(a.naming, type, 1, 922).timeout(limit), [
        'draining',
        'assigning',
        'forwarding',
        'done',
      ]);
      await log.reaches(3);
      expect(log.entries, [
        'A DRAIN 1 e2 toB',
        'B ASSIGN 1 e2 hA',
        'A FORWARD 1 e2 toB',
      ]);
      await a.mirrors(
        type,
        1,
        const SlotEntry.owned(922, holder: 922, epoch: 2),
      );
      expect(b.naming.servedSlots(type), {1: 2});
    });

    test('slot routing through the TS naming service to a TS backend: MOVED '
        'retried once at the owner it names, RELOCATED never', () async {
      if (!f.runs) return;
      final (a, _) = await instance('A', svc, 931);
      await instance('B', svc, 932);
      final epoch = await a.naming.claim(svc, 1).timeout(limit);
      final router = await client();
      await router.mirrors(
        svc,
        1,
        SlotEntry.owned(931, holder: 931, epoch: epoch),
      );
      await whenever(
        router.naming.events,
        () => router.naming.table.containsKey(ServiceAddress(svc, 932)),
      );
      final node = router.node;
      expect(await node.resolveSlotOwner(svc, 1), ServiceAddress(svc, 931));
      // Asked of the naming service (LOCATE) rather than the mirror.
      expect(
        await node.resolveSlotOwner(svc, 1, refresh: true),
        ServiceAddress(svc, 931),
      );
      await expectLater(
        node.resolveSlotOwner(svc, 2, refresh: true),
        throwsCode(StatusCode.unavailable),
      );

      bool incoming(Map<String, Object?> e) => e['event'] == 'channel.incoming';
      bool at(Map<String, Object?> e, int instance) =>
          incoming(e) && e['instance'] == instance;

      // The owner rejects the channel with MOVED naming the other instance,
      // before anything was sent: the router opens it there instead.
      final channel = await node.openChannelToSlot(svc, 1);
      final received = Received(channel.stream);
      addTearDown(received.cancel);
      final first = await f.ts.waitFor((e) => at(e, 931));
      expect(first['shard'], 1);
      final moved = MovedStatus(owner: 932, epoch: epoch + 1);
      await f.ts.rpc<Object?>('channel.close', {
        'channel': first['channel'],
        'statusHex': hex(moved.encode()),
      });
      final second = await f.ts.waitFor((e) => at(e, 932));
      expect([second['shard'], second['payload']], [1, first['payload']]);
      await f.ts.rpc<Object?>('channel.send', {
        'channel': second['channel'],
        'hex': hex(text('B 1')),
      });
      await received.count(1);
      expect(textOf(received.frames.single), 'B 1');
      expect(channel.retried, isTrue);
      // The replacement carries on as the channel.
      await channel.send(text('x'));
      await f.ts.waitFor(
        (e) =>
            e['event'] == 'channel.data' &&
            e['channel'] == second['channel'] &&
            e['hex'] == hex(text('x')),
      );
      await channel.close();
      expect(await channel.done.timeout(limit), Status.ok);

      // RELOCATED: the slot moved after work started; never retried.
      final again = await node.openChannelToSlot(svc, 1);
      final third = (await f.ts.waitForCount((e) => at(e, 931), 2)).last;
      final relocated = moved.toStatus(relocated: true);
      await f.ts.rpc<Object?>('channel.close', {
        'channel': third['channel'],
        'statusHex': hex(relocated.encode()),
      });
      final status = await again.done.timeout(limit);
      expect(status.known, StatusCode.relocated);
      expect(MovedStatus.fromStatus(status), moved);
      expect(again.retried, isFalse);
      // Nothing went to the other instance: the next channel is the owner's.
      final last = await node.openChannelToSlot(svc, 1);
      await f.ts.waitForCount((e) => at(e, 931), 3);
      expect(f.ts.events.where((e) => at(e, 932)), hasLength(1));
      await last.close();
    });
  });
}
