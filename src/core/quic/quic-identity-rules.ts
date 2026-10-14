/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/** The fields of {@link QuicIdentityRules}; every one false when absent. */
export interface QuicIdentityRulesInit {
  readonly tlsAuthenticatesPeer?: boolean | undefined;
  readonly requireExporterBinding?: boolean | undefined;
  readonly awaitPeerIdentity?: boolean | undefined;
  readonly identifyOnlyToIdentifiedPeer?: boolean | undefined;
  readonly bearerOnlyToAuthenticatedPeer?: boolean | undefined;
}

/**
 * The identification rules of a connection of the QUIC and WebTransport
 * binding (wiki page "Polyverse Switchboard QUIC and WebTransport Binding",
 * sections "Exporter-bound proof" and "Certificates"). A port of
 * `QuicIdentityRules` of `quic_options.dart`.
 *
 * Between nodes, certificates are self-signed and the TLS handshake
 * authenticates nobody, so where identity is in use the connection is
 * authenticated by exporter-bound `IDENT`s, in each direction, and these
 * rules are requirements (MUST) of the specification. A mesh without
 * identity, or a connection whose peer the TLS handshake authenticated (a
 * browser verifying a frontend's certificate), relaxes them.
 * {@link QuicIdentityRules.initiator} and {@link QuicIdentityRules.listener}
 * give the specification's defaults; {@link QuicIdentityRules.none} none of
 * them.
 */
export class QuicIdentityRules {
  /**
   * Whether the TLS handshake authenticated the peer: the initiator
   * verified the acceptor's certificate against the platform roots or a
   * pin. A peer so authenticated is an authenticated end for
   * {@link bearerOnlyToAuthenticatedPeer} and
   * {@link identifyOnlyToIdentifiedPeer}.
   */
  readonly tlsAuthenticatesPeer: boolean;

  /**
   * Whether this side depends on the exporter binding: it neither sends nor
   * accepts a nonce-bound `IDENT`. An `IDENT` received while a `HELLO` lacks
   * `EXPORTER` is refused as one that fails verification (the connection
   * ends with `UNAUTHENTICATED`), and `identify` fails with
   * `UNAUTHENTICATED` without identifying. On an initiator, a `HELLO`
   * without `EXPORTER` (the peer's, or its own when its stack cannot compute
   * the exporter) closes the connection with `UNAUTHENTICATED`.
   */
  readonly requireExporterBinding: boolean;

  /**
   * Initiator: the connection reaches the endpoint's owner only once the
   * acceptor's exporter-bound `IDENT` naming this side has verified (naming
   * the identity this side named in its own `IDENT`, when it named one).
   * `identify` completes only then, and channels opened before wait for it
   * (bounded by `MuxOptions.identityTimeout`, after which they end with
   * `UNAUTHENTICATED`). An `IDENT` from the acceptor that names nobody is
   * refused.
   */
  readonly awaitPeerIdentity: boolean;

  /**
   * Acceptor: `identify` fails with `FAILED_PRECONDITION` unless the peer is
   * an authenticated end ({@link tlsAuthenticatesPeer}, or its exporter-bound
   * `IDENT` has verified), so that this side's credential goes only to
   * holders of the mesh's credentials and its proof cannot leave the
   * connection.
   */
  readonly identifyOnlyToIdentifiedPeer: boolean;

  /**
   * `identify` with a bearer credential, which carries no proof, fails with
   * `FAILED_PRECONDITION` unless the peer is an authenticated end. The same
   * holds for credentials in channel payloads, which the binding does not
   * see: the application reads `QuicBindingConnection.peerAuthenticated`
   * before sending one.
   */
  readonly bearerOnlyToAuthenticatedPeer: boolean;

  /** Rules with every requirement given explicitly; all off when absent. */
  constructor(init: QuicIdentityRulesInit = {}) {
    this.tlsAuthenticatesPeer = init.tlsAuthenticatesPeer ?? false;
    this.requireExporterBinding = init.requireExporterBinding ?? false;
    this.awaitPeerIdentity = init.awaitPeerIdentity ?? false;
    this.identifyOnlyToIdentifiedPeer = init.identifyOnlyToIdentifiedPeer ?? false;
    this.bearerOnlyToAuthenticatedPeer = init.bearerOnlyToAuthenticatedPeer ?? false;
    Object.freeze(this);
  }

  /**
   * No requirement: nonce-bound proofs are sent and accepted when a `HELLO`
   * lacks `EXPORTER`, as over the frame mux.
   */
  static readonly none: QuicIdentityRules = new QuicIdentityRules();

  /**
   * The specification's rules for a node listener whose clients may not
   * verify its certificate (every listener with a self-signed one): it
   * refuses nonce-bound `IDENT`s, identifies only to a peer whose
   * exporter-bound `IDENT` has verified, and presents a bearer credential
   * only to such a peer. A listener that serves only browsers, which verify
   * its certificate, has no need of them ({@link none}).
   */
  static readonly listener: QuicIdentityRules = new QuicIdentityRules({
    requireExporterBinding: true,
    identifyOnlyToIdentifiedPeer: true,
    bearerOnlyToAuthenticatedPeer: true,
  });

  /**
   * The specification's rules for an initiator. When the acceptor's
   * certificate is not verified (`certificateVerified` false: neither the
   * platform roots nor a pin) and identity is in use (the side has a
   * credential, `hasCredential`, or verifies the peer's,
   * `verifiesCredentials`), it neither sends nor accepts nonce-bound
   * `IDENT`s and closes the connection with `UNAUTHENTICATED` when a `HELLO`
   * lacks `EXPORTER`; while identity is in use it waits for the acceptor's
   * exporter-bound `IDENT` naming it before using the connection; and a
   * bearer credential goes only to an authenticated end. A verified
   * certificate authenticates the acceptor, and nothing more is required.
   */
  static initiator(options: {
    certificateVerified: boolean;
    hasCredential: boolean;
    verifiesCredentials: boolean;
  }): QuicIdentityRules {
    if (options.certificateVerified) {
      return new QuicIdentityRules({ tlsAuthenticatesPeer: true });
    }
    const inUse = options.hasCredential || options.verifiesCredentials;
    return new QuicIdentityRules({
      requireExporterBinding: inUse,
      awaitPeerIdentity: inUse,
      bearerOnlyToAuthenticatedPeer: true,
    });
  }

  /** A copy with the given fields replaced. */
  with(changes: QuicIdentityRulesInit): QuicIdentityRules {
    return new QuicIdentityRules({
      tlsAuthenticatesPeer: changes.tlsAuthenticatesPeer ?? this.tlsAuthenticatesPeer,
      requireExporterBinding: changes.requireExporterBinding ?? this.requireExporterBinding,
      awaitPeerIdentity: changes.awaitPeerIdentity ?? this.awaitPeerIdentity,
      identifyOnlyToIdentifiedPeer:
        changes.identifyOnlyToIdentifiedPeer ?? this.identifyOnlyToIdentifiedPeer,
      bearerOnlyToAuthenticatedPeer:
        changes.bearerOnlyToAuthenticatedPeer ?? this.bearerOnlyToAuthenticatedPeer,
    });
  }

  /** Whether [other] has the same rules. */
  equals(other: unknown): boolean {
    return (
      other instanceof QuicIdentityRules &&
      other.tlsAuthenticatesPeer === this.tlsAuthenticatesPeer &&
      other.requireExporterBinding === this.requireExporterBinding &&
      other.awaitPeerIdentity === this.awaitPeerIdentity &&
      other.identifyOnlyToIdentifiedPeer === this.identifyOnlyToIdentifiedPeer &&
      other.bearerOnlyToAuthenticatedPeer === this.bearerOnlyToAuthenticatedPeer
    );
  }

  toString(): string {
    const names = [
      'tlsAuthenticatesPeer',
      'requireExporterBinding',
      'awaitPeerIdentity',
      'identifyOnlyToIdentifiedPeer',
      'bearerOnlyToAuthenticatedPeer',
    ] as const;
    return `QuicIdentityRules(${names.filter((name) => this[name]).join(', ')})`;
  }
}
