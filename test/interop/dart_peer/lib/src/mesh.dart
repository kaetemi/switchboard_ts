/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/// The mesh parts of a peer (README, "Mesh"): a naming service, naming
/// clients with their slot calls and resolution, and the proxy and relay
/// in front of another peer.
library;

import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:switchboard/switchboard.dart';

import 'context.dart';
import 'protocol.dart';

/// The `naming`, `proxy` and `relay` configuration.
class MeshConfig {
  /// Reads the configuration's `naming`, `proxy` and `relay` objects.
  MeshConfig.fromJson(Map<String, Object?> json)
    : naming = objectArg(json, 'naming'),
      proxy = objectArg(json, 'proxy'),
      relay = objectArg(json, 'relay');

  /// None of them.
  const MeshConfig.none() : naming = null, proxy = null, relay = null;

  /// `{assignmentHoldMs, holdingSettleMs, handoverTimeoutMs,
  /// assignBackoffMs, assignBackoffMaxMs, holderGraceMs, resumeAttempts}`:
  /// serve a naming service as `_ns` instance 1.
  final Map<String, Object?>? naming;

  /// `{endpoint, types, revealOwners}`: proxy the types to the endpoint.
  final Map<String, Object?>? proxy;

  /// `{endpoint, types}`: relay `_relay` channels for the types to the
  /// endpoint.
  final Map<String, Object?>? relay;
}

/// The records of [types] at instance 1 of [endpoint].
StaticResolver _backend(Map<String, Object?> spec) {
  final endpoint = Uri.parse(requiredString(spec, 'endpoint'));
  return StaticResolver([
    for (final t in (spec['types'] as List<Object?>?) ?? const ['echo'])
      ServiceRecord(
        ServiceAddress(Name(t! as String), 1),
        endpoints: [endpoint],
      ),
  ]);
}

/// What a peer's node serves beside its services: the naming service,
/// the proxy and the relay. Call [listening] once the node listens.
class MeshServices {
  /// Registers [config]'s services on [node].
  MeshServices(this.node, MeshConfig config) {
    final naming = config.naming;
    if (naming != null) {
      // The library's defaults for what is absent, but no assignment hold.
      final service = NamingService(
        assignmentHold: msArg(naming, 'assignmentHoldMs') ?? Duration.zero,
        holdingSettle:
            msArg(naming, 'holdingSettleMs') ?? const Duration(seconds: 1),
        handoverTimeout:
            msArg(naming, 'handoverTimeoutMs') ?? const Duration(seconds: 60),
        assignBackoff:
            msArg(naming, 'assignBackoffMs') ??
            const Duration(milliseconds: 200),
        assignBackoffMax:
            msArg(naming, 'assignBackoffMaxMs') ?? const Duration(seconds: 10),
        holderGrace:
            msArg(naming, 'holderGraceMs') ?? const Duration(minutes: 5),
        resumeAttempts: intArg(naming, 'resumeAttempts') ?? 5,
      );
      node.registerService(Services.naming, service.handler, instance: 1);
      _naming = service;
    }
    final proxy = config.proxy;
    if (proxy != null) {
      final handler = proxyHandler(
        node,
        resolver: _backend(proxy),
        revealOwners: boolArg(proxy, 'revealOwners'),
      );
      for (final t in (proxy['types'] as List<Object?>?) ?? const ['echo']) {
        node.registerService(Name(t! as String), handler);
      }
    }
    final relay = config.relay;
    if (relay != null) {
      final service = RelayService(
        node,
        requireIdentity: false,
        allowEndpoints: true,
        resolver: _backend(relay),
      );
      node.registerService(Services.relay, service.handler);
    }
  }

  /// The node.
  final Switchboard node;

  NamingService? _naming;

  /// Registers the naming service in its own table at [endpoints].
  void listening(List<String> endpoints) {
    _naming?.registerLocal(Services.naming, [
      for (final e in endpoints) Uri.parse(e),
    ], instance: 1);
  }

  /// Closes the naming service.
  Future<void> close() async => _naming?.close();
}

/// The requests a naming client's slot handler serves, by their names in
/// `naming.slot`.
const List<String> _slotRequests = ['assign', 'drain', 'forward', 'resume'];

/// The slot handler of the peer's naming clients (README, "Mesh"): it
/// reports every request as a `naming.slot` event and answers it as
/// `naming.slotAnswers` set, at once by default (`ASSIGN` answered
/// holding). An answer `wait` holds the request until `naming.slotRelease`
/// or until it is cancelled; an answer `{code, reason}` refuses it with
/// that status.
class PeerSlots extends SlotHandler {
  PeerSlots._(this.ctx, this.handle);

  /// The peer's shared state.
  final PeerContext ctx;

  /// The naming client's handle.
  final String Function() handle;

  /// The answer to each request; absent: holding for `ASSIGN`, done for the
  /// others.
  final Map<String, Object> _answers = {};

  /// The requests waiting for `naming.slotRelease`, by request, type and
  /// slot.
  final Map<String, Completer<Object?>> _waiting = {};

  /// Sets the answers [cmd] names (`assign`, `drain`, `forward`, `resume`).
  void setAnswers(Map<String, Object?> cmd) {
    for (final request in _slotRequests) {
      final answer = cmd[request];
      if (answer != null) {
        _answers[request] = _checkAnswer(request, answer);
      }
    }
  }

  /// Answers the waiting [request] for [slot] of [type] with [answer]
  /// (default: at once, `ASSIGN` holding).
  void release(String request, Name type, int slot, Object? answer) {
    final key = '$request $type $slot';
    final waiting = _waiting[key];
    if (waiting == null) {
      throw ArgumentError('no $request of $type/$slot is waiting');
    }
    if (answer == 'wait') {
      throw ArgumentError('a released request cannot wait again');
    }
    _waiting.remove(key);
    waiting.complete(answer == null ? null : _checkAnswer(request, answer));
  }

  @override
  Future<AssignResult> onAssign(
    AssignRequest request,
    SlotRequestContext context,
  ) async {
    final answer = await _serve('assign', request.type, request.slot, context, {
      'epoch': request.epoch,
      'to': null,
      'holder': request.holder,
    });
    return answer == 'notHolding'
        ? AssignResult.notHolding
        : AssignResult.holding;
  }

  @override
  Future<void> onDrain(DrainRequest request, SlotRequestContext context) async {
    await _serve('drain', request.type, request.slot, context, {
      'epoch': request.epoch,
      'to': request.to,
      'holder': null,
    });
  }

  @override
  Future<void> onForward(
    ForwardRequest request,
    SlotRequestContext context,
  ) async {
    await _serve('forward', request.type, request.slot, context, {
      'epoch': request.epoch,
      'to': request.to,
      'holder': null,
    });
  }

  @override
  Future<void> onResume(
    ResumeRequest request,
    SlotRequestContext context,
  ) async {
    await _serve('resume', request.type, request.slot, context, {
      'epoch': request.epoch,
      'to': null,
      'holder': null,
    });
  }

  @override
  Future<void> onRevoke(Name type, int slot) async => _report(
    'revoke',
    type,
    slot,
    {'epoch': null, 'to': null, 'holder': null},
  );

  /// Reports [request] and answers it: completes with the answer, or throws
  /// a refusal.
  Future<Object?> _serve(
    String request,
    Name type,
    int slot,
    SlotRequestContext context,
    Map<String, Object?> fields,
  ) async {
    Object? answer = _answers[request];
    if (answer == 'wait') {
      final key = '$request $type $slot';
      final released = Completer<Object?>();
      _waiting[key] = released;
      _report(request, type, slot, fields);
      answer = await Future.any([
        released.future,
        context.onCancel.then((_) => _cancelled),
      ]);
      if (identical(answer, _cancelled)) {
        if (identical(_waiting[key], released)) {
          _waiting.remove(key);
        }
        throw SwitchboardException.of(
          StatusCode.cancelled,
          'the request was cancelled',
        );
      }
    } else {
      _report(request, type, slot, fields);
    }
    if (answer is Map<String, Object?>) {
      throw SwitchboardException(statusArg(answer));
    }
    return answer;
  }

  void _report(
    String request,
    Name type,
    int slot,
    Map<String, Object?> fields,
  ) => ctx.out.event('naming.slot', {
    'naming': handle(),
    'request': request,
    'type': type.toString(),
    'slot': slot,
    ...fields,
  });
}

/// What a request held with `wait` completes with when it is cancelled.
const Object _cancelled = Object();

/// [answer] if it is one [request] can be given, else throws.
Object _checkAnswer(String request, Object answer) {
  final named = request == 'assign'
      ? const ['holding', 'notHolding', 'wait']
      : const ['ok', 'wait'];
  if (answer is String
      ? named.contains(answer)
      : answer is Map && answer.containsKey('code')) {
    return answer;
  }
  throw ArgumentError('$request cannot be answered ${jsonEncode(answer)}');
}

/// The fields of a mirrored slot table [entry], its state by name.
Map<String, Object?> _entryJson(SlotEntry entry) => {
  'state': entry.state.name,
  'owner': entry.owner,
  'to': entry.to,
  'holder': entry.holder,
  'epoch': entry.epoch,
};

/// A naming client of the peer, with the node that resolves through it.
class NamingHandle {
  NamingHandle._(this.client, this.node, this.slots);

  /// The naming client.
  final NamingClient client;

  /// A node whose resolver is a [NamingResolver] over [client].
  final Switchboard node;

  /// The client's slot handler.
  final PeerSlots slots;

  /// Closes the client and the node.
  Future<void> close() async {
    await client.close();
    await node.close();
  }
}

/// The naming commands (README, "Mesh").
class NamingCommands {
  /// The naming commands of the peer [ctx]; [newNode] makes a client node
  /// with a resolver.
  NamingCommands(this.ctx, this.newNode);

  /// The peer's shared state.
  final PeerContext ctx;

  /// A client node resolving with [resolver].
  final Switchboard Function(Resolver resolver) newNode;

  /// The commands [command] answers.
  static const List<String> ops = [
    'naming.open',
    'naming.synced',
    'naming.register',
    'naming.unregister',
    'naming.defineSlots',
    'naming.claim',
    'naming.release',
    'naming.locate',
    'naming.migrate',
    'naming.declareHolding',
    'naming.slotAnswers',
    'naming.slotRelease',
    'naming.slotOwner',
    'naming.slotChannel',
    'naming.slotTalk',
    'naming.channel',
    'naming.close',
    'slot.send',
    'slot.close',
    'slot.state',
  ];

  /// Runs the naming command [op]; [adoptTalk] makes a Talk channel handle
  /// of a [MuxChannel] or a [SlotChannel].
  Future<Object?> command(
    String op,
    Map<String, Object?> cmd,
    Future<Map<String, Object?>> Function(Object channel) adoptTalk,
  ) async {
    if (op == 'naming.open') {
      return _open(cmd);
    }
    if (op.startsWith('slot.')) {
      return _slotCommand(op, cmd);
    }
    final handle = requiredString(cmd, 'naming');
    final n = ctx.handles.get<NamingHandle>(handle, 'naming client');
    final client = n.client;
    Name type() => Name(requiredString(cmd, 'type'));
    int slot() => intArg(cmd, 'slot') ?? 0;
    Uint8List? payload() =>
        cmd['payload'] == null ? null : bytesOf(cmd['payload']);
    switch (op) {
      case 'naming.synced':
        await client.firstSynced;
        return null;
      case 'naming.register':
        final endpoints = cmd['endpoints'] as List<Object?>?;
        final instance = await client.register(
          type(),
          [for (final e in endpoints ?? ctx.endpoints) Uri.parse(e! as String)],
          instance: intArg(cmd, 'instance') ?? 0,
          metadata: cmd['metadata'] == null ? null : bytesOf(cmd['metadata']),
        );
        return {'instance': instance};
      case 'naming.unregister':
        await client.unregister(type(), intArg(cmd, 'instance') ?? 0);
        return null;
      case 'naming.defineSlots':
        await client.defineSlots(
          type(),
          count: intArg(cmd, 'count') ?? 1,
          mode: SlotMode.values.byName(stringArg(cmd, 'mode') ?? 'managed'),
          lazy: boolArg(cmd, 'lazy'),
          shared: boolArg(cmd, 'shared'),
          capacity: intArg(cmd, 'capacity') ?? 1,
        );
        return null;
      case 'naming.claim':
        final epoch = await client.claim(
          type(),
          intArg(cmd, 'slot') ?? 0,
          holding: boolArg(cmd, 'holding'),
        );
        return {'epoch': epoch};
      case 'naming.release':
        await client.release(
          type(),
          intArg(cmd, 'slot') ?? 0,
          keepStorage: boolArg(cmd, 'keepStorage'),
        );
        return null;
      case 'naming.locate':
        final r = await client.locate(type(), intArg(cmd, 'slot') ?? 0);
        return {'state': r.state.name, 'owner': r.owner, 'epoch': r.epoch};
      case 'naming.migrate':
        final (t, s) = (type(), slot());
        final phases = <String>[];
        await for (final item in client.migrate(
          t,
          s,
          to: intArg(cmd, 'to') ?? 0,
        )) {
          phases.add(item.phase.name);
          ctx.out.event('naming.phase', {
            'naming': handle,
            'type': t.toString(),
            'slot': s,
            'phase': item.phase.name,
          });
        }
        return {'phases': phases};
      case 'naming.declareHolding':
        final slots = [
          for (final s in (cmd['slots'] as List<Object?>?) ?? const [])
            (s! as num).toInt(),
        ];
        return {'discard': await client.declareHolding(type(), slots)};
      case 'naming.slotAnswers':
        n.slots.setAnswers(cmd);
        return null;
      case 'naming.slotRelease':
        n.slots.release(
          requiredString(cmd, 'request'),
          type(),
          slot(),
          cmd['answer'],
        );
        return null;
      case 'naming.slotOwner':
        final owner = await n.node.resolveSlotOwner(
          type(),
          slot(),
          refresh: boolArg(cmd, 'refresh'),
        );
        return {'owner': owner.instance};
      case 'naming.slotChannel':
        final channel = await n.node.openChannelToSlot(
          type(),
          slot(),
          payload: payload(),
        );
        return {'slotChannel': ctx.trackSlotChannel(channel)};
      case 'naming.slotTalk':
        return adoptTalk(
          await n.node.openChannelToSlot(type(), slot(), payload: payload()),
        );
      case 'naming.channel':
        final a = objectArg(cmd, 'address') ?? const {};
        final channel = await n.node.openChannel(
          ServiceAddress(
            Name(requiredString(a, 'type')),
            intArg(a, 'instance') ?? 0,
          ),
          shard: intArg(a, 'shard'),
          payload: a['payload'] == null ? null : bytesOf(a['payload']),
        );
        return adoptTalk(channel);
      case 'naming.close':
        ctx.handles.remove(n);
        await n.close();
        return null;
      default:
        throw ArgumentError('unknown op $op');
    }
  }

  /// The commands on a slot channel.
  Future<Object?> _slotCommand(String op, Map<String, Object?> cmd) async {
    final channel = ctx.handles.get<SlotChannel>(
      cmd['slotChannel'],
      'slot channel',
    );
    switch (op) {
      case 'slot.send':
        await channel.send(bytesOf(cmd['hex']));
        return null;
      case 'slot.close':
        await channel.close(statusArg(cmd));
        return null;
      case 'slot.state':
        return {'retried': channel.retried};
      default:
        throw ArgumentError('unknown op $op');
    }
  }

  Future<Map<String, Object?>> _open(Map<String, Object?> cmd) async {
    final endpoint = Uri.parse(requiredString(cmd, 'endpoint'));
    late final Switchboard node;
    final client = NamingClient(
      () => node.openTalkAt(
        endpoint,
        ChannelAddress(type: Services.naming, instance: 1),
      ),
      reconnectDelay: const Duration(milliseconds: 100),
      watch: boolArg(cmd, 'watch', fallback: true),
    );
    node = newNode(NamingResolver(client));
    late final String handle;
    final slots = PeerSlots._(ctx, () => handle);
    handle = ctx.handles.add('n', NamingHandle._(client, node, slots));
    client.slotHandler = slots;
    client.events.listen(
      (e) => ctx.out.event('naming.event', {
        'naming': handle,
        'up': e.up,
        'type': e.record.address.type.toString(),
        'instance': e.record.address.instance,
        'endpoints': [for (final u in e.record.endpoints) '$u'],
        'metadata': hex(e.record.metadata),
      }),
      onError: (Object _) {},
    );
    client.slotEvents.listen(
      (e) => ctx.out.event('naming.table', {
        'naming': handle,
        'type': e.type.toString(),
        'slot': e.slot,
        ..._entryJson(e.entry),
      }),
      onError: (Object _) {},
    );
    await client.start();
    return {'naming': handle};
  }
}
