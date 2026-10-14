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

  /// `{assignmentHoldMs}`: serve a naming service as `_ns` instance 1.
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
      final service = NamingService(
        assignmentHold: msArg(naming, 'assignmentHoldMs') ?? Duration.zero,
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

/// The slot handler of the peer's naming clients: every slot request is
/// accepted at once (`ASSIGN` answered holding) and reported as a
/// `naming.slot` event.
class _ReportingSlots extends SlotHandler {
  _ReportingSlots(this.ctx, this.handle);

  final PeerContext ctx;
  final String Function() handle;

  void _report(String request, Name type, int slot, [int? epoch, int? to]) =>
      ctx.out.event('naming.slot', {
        'naming': handle(),
        'request': request,
        'type': type.toString(),
        'slot': slot,
        'epoch': epoch,
        'to': to,
      });

  @override
  Future<AssignResult> onAssign(
    AssignRequest request,
    SlotRequestContext context,
  ) async {
    _report('assign', request.type, request.slot, request.epoch);
    return AssignResult.holding;
  }

  @override
  Future<void> onDrain(
    DrainRequest request,
    SlotRequestContext context,
  ) async =>
      _report('drain', request.type, request.slot, request.epoch, request.to);

  @override
  Future<void> onForward(
    ForwardRequest request,
    SlotRequestContext context,
  ) async =>
      _report('forward', request.type, request.slot, request.epoch, request.to);

  @override
  Future<void> onResume(
    ResumeRequest request,
    SlotRequestContext context,
  ) async => _report('resume', request.type, request.slot, request.epoch);

  @override
  Future<void> onRevoke(Name type, int slot) async =>
      _report('revoke', type, slot);
}

/// A naming client of the peer, with the node that resolves through it.
class NamingHandle {
  NamingHandle._(this.client, this.node);

  /// The naming client.
  final NamingClient client;

  /// A node whose resolver is a [NamingResolver] over [client].
  final Switchboard node;

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
    'naming.channel',
    'naming.close',
  ];

  /// Runs the naming command [op].
  Future<Object?> command(
    String op,
    Map<String, Object?> cmd,
    Future<Map<String, Object?>> Function(MuxChannel channel) adoptTalk,
  ) async {
    if (op == 'naming.open') {
      return _open(cmd);
    }
    final n = ctx.handles.get<NamingHandle>(cmd['naming'], 'naming client');
    final client = n.client;
    Name type() => Name(requiredString(cmd, 'type'));
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
        final phases = <String>[];
        await for (final item in client.migrate(
          type(),
          intArg(cmd, 'slot') ?? 0,
          to: intArg(cmd, 'to') ?? 0,
        )) {
          phases.add(item.phase.name);
        }
        return {'phases': phases};
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
    final handle = ctx.handles.add('n', NamingHandle._(client, node));
    client.slotHandler = _ReportingSlots(ctx, () => handle);
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
    await client.start();
    return {'naming': handle};
  }
}
