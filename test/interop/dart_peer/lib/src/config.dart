/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/// The peer's configuration, from `--config <json>` (README, "Config").
library;

import 'package:switchboard/core.dart';

import 'protocol.dart';

/// The transports a peer can listen on.
const List<String> transports = ['tcp', 'ws', 'quic', 'webtransport'];

/// One registered service: a type, an instance id, and the behaviour that
/// serves it (`echo`, `raw`, `sink` or `forward`, README "Services").
class ServiceSpec {
  /// A service of [type] at [instance] behaving as [behaviour].
  const ServiceSpec(
    this.type,
    this.behaviour, {
    this.instance = 0,
    this.targetEndpoint,
    this.targetType,
    this.targetInstance = 0,
  });

  /// The service type.
  final String type;

  /// `echo` (the interop procedures), `raw` (channels as handles), `sink`
  /// (channels never read) or `forward` (Talk forwarded to a target).
  final String behaviour;

  /// The instance id; 0 registers the type's default handler.
  final int instance;

  /// `forward`: the endpoint forwarded to; null for this peer's first
  /// listener.
  final String? targetEndpoint;

  /// `forward`: the service type forwarded to; null for `echo`.
  final String? targetType;

  /// `forward`: the instance forwarded to.
  final int targetInstance;
}

/// Everything the command line configures.
class PeerConfig {
  /// A configuration; the defaults listen everywhere with the default
  /// services.
  PeerConfig({
    this.listen = transports,
    this.path = '/sb',
    this.mux = const MuxOptions(),
    this.talk = const TalkOptions(),
    this.services = defaultServices,
  });

  /// `echo`, `raw` and `sink` at instance 1.
  static const List<ServiceSpec> defaultServices = [
    ServiceSpec('echo', 'echo', instance: 1),
    ServiceSpec('raw', 'raw', instance: 1),
    ServiceSpec('sink', 'sink', instance: 1),
  ];

  /// Reads the JSON configuration; absent keys keep their defaults.
  factory PeerConfig.fromJson(Map<String, Object?> json) {
    final listen = json['listen'];
    final services = json['services'];
    return PeerConfig(
      listen: listen == null
          ? transports
          : [
              for (final t in listen as List<Object?>)
                if (transports.contains(t))
                  t as String
                else
                  throw ArgumentError('unknown transport $t'),
            ],
      path: stringArg(json, 'path') ?? '/sb',
      mux: _mux(objectArg(json, 'mux') ?? const {}),
      talk: _talk(objectArg(json, 'talk') ?? const {}),
      services: services == null
          ? defaultServices
          : [
              for (final s in services as List<Object?>)
                _service(s as Map<String, Object?>),
            ],
    );
  }

  /// The transports to listen on.
  final List<String> listen;

  /// The WebSocket and WebTransport path.
  final String path;

  /// The options of every connection.
  final MuxOptions mux;

  /// The Talk options of the services and of the client channels.
  final TalkOptions talk;

  /// The services the node registers.
  final List<ServiceSpec> services;

  static ServiceSpec _service(Map<String, Object?> s) {
    final target = objectArg(s, 'target') ?? const {};
    return ServiceSpec(
      requiredString(s, 'type'),
      stringArg(s, 'behaviour') ?? 'echo',
      instance: intArg(s, 'instance') ?? 0,
      targetEndpoint: stringArg(target, 'endpoint'),
      targetType: stringArg(target, 'type'),
      targetInstance: intArg(target, 'instance') ?? 0,
    );
  }

  static MuxOptions _mux(Map<String, Object?> j) {
    const d = MuxOptions();
    final keepAlive = intArg(j, 'keepAliveIntervalMs');
    return MuxOptions(
      maxFrameSize: intArg(j, 'maxFrameSize') ?? d.maxFrameSize,
      maxChannels: intArg(j, 'maxChannels') ?? d.maxChannels,
      shortIdsOnly: boolArg(j, 'shortIdsOnly', fallback: d.shortIdsOnly),
      keepAliveInterval: keepAlive == null
          ? d.keepAliveInterval
          : keepAlive == 0
          ? null
          : Duration(milliseconds: keepAlive),
      keepAliveTimeout: msArg(j, 'keepAliveTimeoutMs') ?? d.keepAliveTimeout,
      goAwayGrace: msArg(j, 'goAwayGraceMs') ?? d.goAwayGrace,
      announceLimits: boolArg(j, 'announceLimits', fallback: d.announceLimits),
      closeConfirmTimeout:
          msArg(j, 'closeConfirmTimeoutMs') ?? d.closeConfirmTimeout,
      identityTimeout: msArg(j, 'identityTimeoutMs') ?? d.identityTimeout,
      requireNamedIdent: boolArg(
        j,
        'requireNamedIdent',
        fallback: d.requireNamedIdent,
      ),
      initialWindow: intArg(j, 'initialWindow') ?? d.initialWindow,
      awaitPeerLimits: boolArg(
        j,
        'awaitPeerLimits',
        fallback: d.awaitPeerLimits,
      ),
    );
  }

  static TalkOptions _talk(Map<String, Object?> j) {
    const d = TalkOptions();
    return TalkOptions(
      requestTimeout: msArg(j, 'requestTimeoutMs') ?? d.requestTimeout,
      replyTimeout: msArg(j, 'replyTimeoutMs') ?? d.replyTimeout,
      bulkThreshold: intArg(j, 'bulkThreshold') ?? d.bulkThreshold,
      maxInlinePayload: intArg(j, 'maxInlinePayload') ?? d.maxInlinePayload,
      reassemblyBudget: intArg(j, 'reassemblyBudget') ?? d.reassemblyBudget,
      bulkIdleTimeout: msArg(j, 'bulkIdleTimeoutMs') ?? d.bulkIdleTimeout,
      creditTimeout: msArg(j, 'creditTimeoutMs') ?? d.creditTimeout,
    );
  }
}
