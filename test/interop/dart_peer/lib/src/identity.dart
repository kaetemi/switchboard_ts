/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/// The peer's identity (README, "Identity"): the test authorities both
/// peers share, the credential a peer presents, and the verifier and
/// policies it applies.
library;

import 'dart:convert';
import 'dart:typed_data';

import 'package:crypto/crypto.dart';
import 'package:switchboard/core.dart';

import 'protocol.dart';

/// The Ed25519 authority both peers trust: key id `auth1`, seed `3 * i`.
final Uint8List authoritySeed = Uint8List.fromList(
  List.generate(32, (i) => 3 * i),
);

/// An authority nobody trusts claiming the same key id: seed `5 * i + 1`.
final Uint8List rogueSeed = Uint8List.fromList(
  List.generate(32, (i) => 5 * i + 1),
);

/// The HMAC key both peers trust: key id `mesh1`, bytes 0 to 31.
final Uint8List meshKey = Uint8List.fromList(List.generate(32, (i) => i));

/// The holder key seed of [identity]: SHA-256 of `holder:<identity>`.
Uint8List holderSeed(String identity) =>
    Uint8List.fromList(sha256.convert(utf8.encode('holder:$identity')).bytes);

/// The credential a peer presents, from the `identity.credential` object.
class CredentialSpec {
  /// Reads `{identity, authority, holder, kind, scopes, forge}`.
  CredentialSpec.fromJson(Map<String, Object?> json)
    : identity = requiredString(json, 'identity'),
      authority = stringArg(json, 'authority') ?? 'ed25519',
      holder =
          json['holder'] as bool? ??
          (stringArg(json, 'authority') ?? 'ed25519') != 'hmac',
      kind = CredentialKind.values.byName(stringArg(json, 'kind') ?? 'node'),
      scopes = [
        for (final s
            in (json['scopes'] as List<Object?>?) ??
                const [
                  {'right': 'open', 'pattern': '*'},
                  {'right': 'register', 'pattern': '*'},
                  {'right': 'watch', 'pattern': '*'},
                  {'right': 'claim', 'pattern': '*'},
                ])
          Scope.of(
            Right.values.byName(
              requiredString(s as Map<String, Object?>, 'right'),
            ),
            requiredString(s, 'pattern'),
          ),
      ],
      forge = boolArg(json, 'forge');

  /// The identity it names.
  final String identity;

  /// `ed25519` (`auth1`), `hmac` (`mesh1`) or `rogue` (an untrusted `auth1`).
  final String authority;

  /// Whether it names a holder key (proof of possession).
  final bool holder;

  /// What it identifies.
  final CredentialKind kind;

  /// What it allows.
  final List<Scope> scopes;

  /// Whether its last signature byte is flipped.
  final bool forge;
}

/// The `identity` configuration.
class IdentityConfig {
  /// Reads the `identity` object; null for none.
  IdentityConfig.fromJson(Map<String, Object?> json)
    : verify = boolArg(json, 'verify'),
      credential = objectArg(json, 'credential') == null
          ? null
          : CredentialSpec.fromJson(objectArg(json, 'credential')!),
      identifyOutgoing = boolArg(json, 'identifyOutgoing', fallback: true),
      identifyInReturn = boolArg(json, 'identifyInReturn'),
      identityTimeout =
          msArg(json, 'identityTimeoutMs') ?? const Duration(seconds: 10),
      policy = stringArg(json, 'policy'),
      quicRules = stringArg(json, 'quicRules') ?? 'none';

  /// No identity: nothing presented, nothing verified.
  IdentityConfig.none()
    : verify = false,
      credential = null,
      identifyOutgoing = true,
      identifyInReturn = false,
      identityTimeout = const Duration(seconds: 10),
      policy = null,
      quicRules = 'none';

  /// Whether the peer verifies IDENTs with the trusted authorities.
  final bool verify;

  /// The credential the peer presents, or null.
  final CredentialSpec? credential;

  /// Whether the peer identifies on the connections it dials.
  final bool identifyOutgoing;

  /// Whether the peer identifies in return on accepted connections whose
  /// peer asked (sent NONCE first).
  final bool identifyInReturn;

  /// How long a channel waits for its connection's IDENT.
  final Duration identityTimeout;

  /// The listeners' policy: `scoped`, `requireIdentity`, `denyReserved`,
  /// or null for none.
  final String? policy;

  /// The QUIC listeners' identification rules: `none` or `listener`.
  final String quicRules;

  /// The listener policy, or null.
  ChannelPolicy? get channelPolicy => switch (policy) {
    null => null,
    'scoped' => ChannelPolicies.scoped(),
    'requireIdentity' => ChannelPolicies.requireIdentity(),
    'denyReserved' => ChannelPolicies.denyReserved,
    _ => throw ArgumentError('unknown policy $policy'),
  };
}

/// What a node is made with: its credential, holder key and verifier.
class NodeIdentity {
  NodeIdentity._(this.credential, this.holderKey, this.verifier);

  /// The node's credential, or null.
  final Credential? credential;

  /// The holder key of [credential], or null.
  final HolderKey? holderKey;

  /// The verifier, or null.
  final CredentialVerifier? verifier;

  /// Issues [config]'s credential and builds its verifier.
  static Future<NodeIdentity> of(IdentityConfig config) async {
    final authority = await CredentialIssuer.ed25519FromSeed(
      Name('auth1'),
      authoritySeed,
    );
    final hmac = CredentialIssuer.hmac(Name('mesh1'), meshKey);
    CredentialVerifier? verifier;
    if (config.verify) {
      verifier = CredentialVerifier();
      await verifier.addIssuer(authority);
      await verifier.addIssuer(hmac);
    }
    final spec = config.credential;
    if (spec == null) {
      return NodeIdentity._(null, null, verifier);
    }
    final issuer = switch (spec.authority) {
      'ed25519' => authority,
      'hmac' => hmac,
      'rogue' => await CredentialIssuer.ed25519FromSeed(
        Name('auth1'),
        rogueSeed,
      ),
      _ => throw ArgumentError('unknown authority ${spec.authority}'),
    };
    final key = spec.holder
        ? await HolderKey.fromSeed(holderSeed(spec.identity))
        : null;
    var credential = await issuer.issue(
      kind: spec.kind,
      identity: spec.identity,
      scopes: spec.scopes,
      holderKey: key?.publicKey,
    );
    if (spec.forge) {
      final bytes = credential.encode();
      bytes[bytes.length - 1] ^= 0x01;
      credential = Credential.decode(bytes);
    }
    return NodeIdentity._(credential, key, verifier);
  }
}

/// [identity] as the `connection.identified` event reports it.
Map<String, Object?> identityJson(PeerIdentity identity) => {
  'identity': identity.identity,
  'kind': identity.credential.kind.name,
  'provedPossession': identity.provedPossession,
  'intent': hex(identity.intent),
  'receiver': identity.receiver,
};
