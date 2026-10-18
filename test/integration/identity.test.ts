/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Identity between Node Switchboard nodes over real sockets, with real
// credentials: the Ed25519 authority, the rogue key pair and the mesh's HMAC
// issuer of `test/support/node/seeded-identity.ts` (Dart's
// `identity_node_test.dart`'s), verified by a real `CredentialVerifier`,
// holder keys from seeds.
//
// - Over TCP: the "over tcp" group of `identity_node_test.dart`, which the
//   unit port (`test/unit/switchboard/identity-node.test.ts`) runs over
//   mem:// only: mutual identity, the scoped policy, a worker, a bearer
//   credential of the HMAC issuer, another authority's credential, a forged
//   IDENT; and the receiver an IDENT names (`expectedIdentityFor`,
//   `requireNamedIdent`).
// - Over QUIC, raw and WebTransport, through the native provider: the same
//   identities exporter-bound both ways under the binding page's rules for
//   self-signed listeners (the listener identifies in return, the dialler
//   waits for it), `ChannelPolicies.scoped` and `requireIdentity`, a bearer
//   credential only to a pinned listener, `requireNamedIdent`. Skipped
//   with the reason without the addon; SWITCHBOARD_NATIVE=required fails.
// - The example "Identity, scoped policy, QUIC on Node" of docs/design.md
//   section 2, verbatim but for the import path (the sources, not the
//   package) and the free variables it names, defined before it.

import { afterAll, describe, expect, onTestFinished, test } from 'vitest';
import { ChannelAddress } from '../../src/core/address/index.ts';
import { bytesEqual, utf8Decode, utf8Encode } from '../../src/core/bytes.ts';
import { CredentialKind, Right } from '../../src/core/identity/contract.ts';
import type { Credential } from '../../src/core/identity/credential.ts';
import { CredentialIssuer } from '../../src/core/identity/credential-issuer.ts';
import { CredentialVerifier } from '../../src/core/identity/credential-verifier.ts';
import { Ed25519HolderKey } from '../../src/core/identity/holder-key.ts';
import { Scope as CredentialScope } from '../../src/core/identity/scope.ts';
import type { MuxChannel, MuxConnection, MuxOptions } from '../../src/core/mux/contract.ts';
import { Name, type NameLike } from '../../src/core/name.ts';
import { QuicBindingConnection } from '../../src/core/quic/quic-binding-connection.ts';
import { StatusCode, statusCodeName } from '../../src/core/status.ts';
import type { ChannelPolicy, IncomingChannel } from '../../src/core/switchboard/contract.ts';
import {
  ChannelPolicies,
  type NativeCertificate,
  NativeQuicProvider,
  nativeQuic,
  nativeQuicAvailability,
  Switchboard,
  type SwitchboardOptions,
  selfSignedCertificate,
} from '../../src/node/index.ts';
import { rejectsWith, until } from '../support/node/harness.ts';
import {
  authoritySeed,
  forged,
  hmacAuthority,
  issueCredential,
  rogue,
  TestVerifier,
} from '../support/node/seeded-identity.ts';

const availability = nativeQuicAvailability();
if (!availability.available && process.env['SWITCHBOARD_NATIVE'] === 'required') {
  throw new Error(`SWITCHBOARD_NATIVE=required, but ${availability.reason}`);
}

const limit = 5000;

/** Short GOAWAY grace, no keep-alive: Dart's `fast`. */
const fast: Partial<MuxOptions> = { goAwayGrace: 100, keepAliveInterval: undefined };

const npc = Name.of('npc');
const chat = Name.of('chat');
const naming = Name.of('_ns');

const npcKey = await Ed25519HolderKey.fromSeed(new Uint8Array(32).fill(1));
const gateKey = await Ed25519HolderKey.fromSeed(new Uint8Array(32).fill(2));
const workerKey = await Ed25519HolderKey.fromSeed(new Uint8Array(32).fill(3));

/** `npc-7`, which may open `npc` and nothing else; of [by], the authority by default. */
function npcCredential(by?: CredentialIssuer): Promise<Credential> {
  return issueCredential({
    identity: 'npc-7',
    holderKey: npcKey.publicKey,
    scopes: [[Right.open, 'npc']],
    ...(by === undefined ? {} : { by }),
  });
}

/** A worker: may register its type, open nothing. */
function workerCredential(): Promise<Credential> {
  return issueCredential({
    identity: 'worker-a1',
    holderKey: workerKey.publicKey,
    scopes: [[Right.register, 'worker-*']],
  });
}

/** `gate`, which may open anything. */
function gateCredential(): Promise<Credential> {
  return issueCredential({
    identity: 'gate',
    holderKey: gateKey.publicKey,
    scopes: [[Right.open, '*']],
  });
}

/** `npc-8`, a bearer credential of the mesh's HMAC issuer, which may open `npc`. */
function bearerCredential(): Promise<Credential> {
  return issueCredential({ identity: 'npc-8', scopes: [[Right.open, 'npc']], by: hmacAuthority });
}

interface NodeOptions extends SwitchboardOptions {
  /** Whether the node has the test verifier; default true. */
  readonly verify?: boolean;
  readonly requireNamedIdent?: boolean;
}

/** A Node node with the test verifier (unless [verify] is false), closed after the test. */
function node(options: NodeOptions = {}): Switchboard {
  const { verify, requireNamedIdent, ...rest } = options;
  const s = new Switchboard({
    verifier: (verify ?? true) ? new TestVerifier() : undefined,
    identityTimeout: limit,
    connectTimeout: limit,
    ...rest,
    muxOptions: { ...fast, requireNamedIdent: requireNamedIdent ?? false, ...rest.muxOptions },
  });
  onTestFinished(() => s.close());
  return s;
}

/** A handler answering with the peer's identity and closing. */
function answer(incoming: IncomingChannel): void {
  const who = incoming.peerIdentity?.identity ?? 'nobody';
  void incoming.channel.send(utf8Encode(`${incoming.address.type} for ${who}`)).catch(() => {});
  void incoming.channel.close().catch(() => {});
}

/** What an {@link answer} handler said, or the close status. */
async function answerOf(channel: MuxChannel): Promise<string> {
  const frames: Uint8Array[] = [];
  for await (const frame of channel.readable) frames.push(frame);
  const status = await channel.done;
  return frames.length === 0
    ? `closed ${statusCodeName(status.code) ?? status.code}`
    : utf8Decode(frames[0] as Uint8Array);
}

/** What the [type] service of [endpoint] answers [client]. */
async function open(client: Switchboard, endpoint: string, type: NameLike): Promise<string> {
  return answerOf(await client.openChannelAt(endpoint, new ChannelAddress({ type })));
}

/** The connections [s] reports from now on. */
function collect(s: Switchboard): MuxConnection[] {
  const seen: MuxConnection[] = [];
  void (async () => {
    for await (const connection of s.connections) seen.push(connection);
  })();
  return seen;
}

/** How a server listens and a client reaches it. */
interface Transport {
  readonly label: string;
  /** Has [server] listen on the loopback under [policy]; resolves with the endpoint. */
  listen(server: Switchboard, policy?: ChannelPolicy): Promise<string>;
  /** What a client node needs to dial the endpoint; [pin] pins the listener's certificate. */
  client(pin?: boolean): SwitchboardOptions;
}

/** A server answering `npc`, `chat` and `_ns` over [transport] with [policy]. */
async function server(
  transport: Transport,
  options: NodeOptions & { policy?: ChannelPolicy } = {},
): Promise<{ s: Switchboard; uri: string }> {
  const { policy, ...rest } = options;
  const s = node(rest);
  for (const type of [npc, chat, naming]) {
    s.registerService(type, answer);
  }
  return { s, uri: await transport.listen(s, policy) };
}

const tcp: Transport = {
  label: 'tcp',
  listen: (s, policy) => s.listenTcp('127.0.0.1', 0, { policy }),
  client: () => ({}),
};

describe('over tcp', () => {
  test('mutual identity', async () => {
    const { s: b, uri } = await server(tcp, {
      credential: await gateCredential(),
      holderKey: gateKey,
    });
    // The server identifies on the connections whose peers ask.
    const accepted: MuxConnection[] = [];
    void (async () => {
      for await (const connection of b.connections) {
        accepted.push(connection);
        void connection.identityRequested.then(() => b.identifyOn(connection));
      }
    })();
    const a = node({ credential: await npcCredential(), holderKey: npcKey });
    const connection = await a.connect(uri);
    // connect() returned after the server confirmed the IDENT.
    expect(accepted).toHaveLength(1);
    expect(accepted[0]?.peerIdentity?.identity).toBe('npc-7');
    expect(accepted[0]?.peerIdentity?.provedPossession).toBe(true);
    const gate = await connection.peerIdentified;
    expect(gate.identity).toBe('gate');
    expect(connection.peerIdentity).toBe(gate);
    expect(await open(a, uri, npc)).toBe('npc for npc-7');
  });

  test('a scoped policy admits open-scoped types only', async () => {
    const { uri } = await server(tcp, { policy: ChannelPolicies.scoped() });
    const a = node({ credential: await npcCredential(), holderKey: npcKey });
    expect(await open(a, uri, npc)).toBe('npc for npc-7');
    expect(await open(a, uri, chat)).toBe('closed permissionDenied');
    // The naming service checks the credential itself.
    expect(await open(a, uri, naming)).toBe('_ns for npc-7');
  });

  test('a worker, identified, can open nothing', async () => {
    const { uri } = await server(tcp, { policy: ChannelPolicies.scoped() });
    const w = node({ credential: await workerCredential(), holderKey: workerKey });
    expect(await open(w, uri, npc)).toBe('closed permissionDenied');
    expect(await open(w, uri, chat)).toBe('closed permissionDenied');
    expect(await open(w, uri, naming)).toBe('_ns for worker-a1');
  });

  test('a bearer credential works', async () => {
    const { uri } = await server(tcp, { policy: ChannelPolicies.scoped() });
    const a = node({ credential: await bearerCredential() });
    expect(await open(a, uri, npc)).toBe('npc for npc-8');
    expect(await open(a, uri, chat)).toBe('closed permissionDenied');
  });

  test('a credential of another authority fails the connect with UNAUTHENTICATED', async () => {
    const { s: b, uri } = await server(tcp, { policy: ChannelPolicies.scoped() });
    const accepted = collect(b);
    const a = node({ credential: await npcCredential(rogue), holderKey: npcKey });
    await rejectsWith(a.connect(uri), StatusCode.unauthenticated);
    await rejectsWith(
      a.openChannelAt(uri, new ChannelAddress({ type: npc })),
      StatusCode.unauthenticated,
    );
    await until(() => accepted.length === 2, 5000);
    for (const connection of accepted) {
      expect(statusCodeName((await connection.done).code)).toBe('unauthenticated');
    }
  });

  test('a forged IDENT gets GOAWAY UNAUTHENTICATED', async () => {
    const { uri } = await server(tcp);
    const a = node({ credential: forged(await npcCredential()), holderKey: npcKey });
    const dialled = collect(a);
    await rejectsWith(a.connect(uri), StatusCode.unauthenticated);
    await until(() => dialled.length === 1, 5000);
    const connection = dialled[0] as MuxConnection;
    expect(statusCodeName((await connection.peerGoAwayStatus).code)).toBe('unauthenticated');
    expect(statusCodeName((await connection.done).code)).toBe('goingAway');
  });

  test('requireNamedIdent: the IDENT names the server, as expectedIdentityFor says', async () => {
    const { uri } = await server(tcp, {
      credential: await gateCredential(),
      holderKey: gateKey,
      requireNamedIdent: true,
    });
    const named = node({
      credential: await npcCredential(),
      holderKey: npcKey,
      expectedIdentityFor: () => 'gate',
    });
    expect(await open(named, uri, npc)).toBe('npc for npc-7');
    const connection = await named.connect(uri);
    expect(connection.localIdentity).toBe('npc-7');
    // An IDENT naming nobody, or someone else, is refused.
    for (const expected of [undefined, 'other']) {
      const client = node({
        credential: await npcCredential(),
        holderKey: npcKey,
        expectedIdentityFor: () => expected,
      });
      await rejectsWith(client.connect(uri), StatusCode.unauthenticated);
    }
  });
});

describe.skipIf(!availability.available)(
  `over QUIC, exporter-bound${availability.available ? '' : ` (skipped: ${availability.reason})`}`,
  () => {
    let certificate: NativeCertificate | undefined;
    const cert = (): NativeCertificate => {
      certificate ??= selfSignedCertificate('127.0.0.1');
      return certificate;
    };

    afterAll(() => {
      NativeQuicProvider.shutdown();
    });

    const transports: readonly Transport[] = [
      {
        label: 'raw QUIC',
        listen: (s, policy) => s.listenQuic('127.0.0.1', 0, { policy, certificate: cert() }),
        client: (pin = false) => ({
          quic: nativeQuic(),
          quicBinding: pin ? { serverCertificateHashes: [cert().sha256] } : {},
        }),
      },
      {
        label: 'WebTransport',
        listen: (s, policy) =>
          s.listenWebTransport('127.0.0.1', 0, { path: '/sb', policy, certificate: cert() }),
        client: (pin = false) => ({
          quic: nativeQuic(),
          quicBinding: pin
            ? { serverCertificateHashes: [cert().sha256] }
            : { webTransportCertificateVerified: false },
        }),
      },
    ];

    for (const transport of transports) {
      describe(transport.label, () => {
        test('mutual identity, bound to the TLS exporter both ways; the listener names the dialler', async () => {
          const { s: b, uri } = await server(transport, {
            credential: await gateCredential(),
            holderKey: gateKey,
            policy: ChannelPolicies.scoped(),
          });
          const accepted = collect(b);
          const a = node({
            ...transport.client(),
            credential: await npcCredential(),
            holderKey: npcKey,
            expectedIdentityFor: () => 'gate',
          });
          expect(await open(a, uri, npc)).toBe('npc for npc-7');
          const connection = (await a.connect(uri)) as QuicBindingConnection;
          expect(connection).toBeInstanceOf(QuicBindingConnection);
          expect(connection.isWebTransport).toBe(transport.label === 'WebTransport');
          expect(connection.exporterBound).toBe(true);
          expect(connection.quicOptions.identity.awaitPeerIdentity).toBe(true);
          expect(connection.peerIdentity?.identity).toBe('gate');
          expect(connection.peerIdentity?.receiver).toBe('npc-7');
          expect(connection.peerIdentity?.provedPossession).toBe(true);
          expect(connection.peerIdentityBound).toBe(true);
          expect(connection.peerAuthenticated).toBe(true);
          await until(() => accepted.length === 1, 5000);
          const theirs = accepted[0] as QuicBindingConnection;
          expect(theirs.peerIdentity?.identity).toBe('npc-7');
          expect(theirs.peerIdentity?.receiver).toBe('gate');
          expect(theirs.peerIdentityBound).toBe(true);
          expect(
            bytesEqual(theirs.exporterForTesting ?? [], connection.exporterForTesting ?? [1]),
          ).toBe(true);
        });

        test('a scoped policy admits open-scoped types only; a worker can open nothing', async () => {
          const { uri } = await server(transport, {
            credential: await gateCredential(),
            holderKey: gateKey,
            policy: ChannelPolicies.scoped(),
          });
          const a = node({
            ...transport.client(),
            credential: await npcCredential(),
            holderKey: npcKey,
          });
          expect(await open(a, uri, npc)).toBe('npc for npc-7');
          expect(await open(a, uri, chat)).toBe('closed permissionDenied');
          expect(await open(a, uri, naming)).toBe('_ns for npc-7');
          const w = node({
            ...transport.client(),
            credential: await workerCredential(),
            holderKey: workerKey,
          });
          expect(await open(w, uri, npc)).toBe('closed permissionDenied');
          expect(await open(w, uri, chat)).toBe('closed permissionDenied');
          expect(await open(w, uri, naming)).toBe('_ns for worker-a1');
        });

        test('requireIdentity, combined with denyReserved; an anonymous dialler is refused UNAUTHENTICATED', async () => {
          const { uri } = await server(transport, {
            credential: await gateCredential(),
            holderKey: gateKey,
            identityTimeout: 500,
            policy: ChannelPolicies.all([
              ChannelPolicies.denyReserved,
              ChannelPolicies.requireIdentity(),
            ]),
          });
          const a = node({
            ...transport.client(),
            credential: await npcCredential(),
            holderKey: npcKey,
          });
          expect(await open(a, uri, chat)).toBe('chat for npc-7');
          expect(await open(a, uri, naming)).toBe('closed permissionDenied');
          const anonymous = node({ ...transport.client(), verify: false });
          expect(await open(anonymous, uri, chat)).toBe('closed unauthenticated');
        });

        test('a bearer credential of the HMAC issuer goes only to a pinned listener', async () => {
          const { uri } = await server(transport, {
            credential: await gateCredential(),
            holderKey: gateKey,
            policy: ChannelPolicies.scoped(),
          });
          // A self-signed listener is not authenticated: the bearer
          // credential is not sent, and nothing reaches the scoped service.
          const unpinned = node({ ...transport.client(), credential: await bearerCredential() });
          await rejectsWith(
            unpinned.openChannelAt(uri, new ChannelAddress({ type: npc })),
            StatusCode.unauthenticated,
          );
          const pinned = node({ ...transport.client(true), credential: await bearerCredential() });
          expect(await open(pinned, uri, npc)).toBe('npc for npc-8');
          expect(await open(pinned, uri, chat)).toBe('closed permissionDenied');
          const connection = (await pinned.connect(uri)) as QuicBindingConnection;
          expect(connection.peerAuthenticated).toBe(true);
          expect(connection.quicOptions.identity.tlsAuthenticatesPeer).toBe(true);
        });

        test("another authority's credential, or a forged one, is refused UNAUTHENTICATED", async () => {
          const { uri } = await server(transport, {
            credential: await gateCredential(),
            holderKey: gateKey,
            policy: ChannelPolicies.scoped(),
          });
          for (const credential of [await npcCredential(rogue), forged(await npcCredential())]) {
            const a = node({ ...transport.client(), credential, holderKey: npcKey });
            await rejectsWith(
              a.openChannelAt(uri, new ChannelAddress({ type: npc })),
              StatusCode.unauthenticated,
            );
          }
        });

        test('requireNamedIdent: the dialler names the listener, as expectedIdentityFor says', async () => {
          const { uri } = await server(transport, {
            credential: await gateCredential(),
            holderKey: gateKey,
            requireNamedIdent: true,
            policy: ChannelPolicies.scoped(),
          });
          const named = node({
            ...transport.client(),
            credential: await npcCredential(),
            holderKey: npcKey,
            expectedIdentityFor: () => 'gate',
          });
          expect(await open(named, uri, npc)).toBe('npc for npc-7');
          const unnamed = node({
            ...transport.client(),
            credential: await npcCredential(),
            holderKey: npcKey,
          });
          await rejectsWith(
            unnamed.openChannelAt(uri, new ChannelAddress({ type: npc })),
            StatusCode.unauthenticated,
          );
        });
      });
    }

    test('the design example: identity, scoped policy, QUIC on Node', async () => {
      // The free variable of the example is the authority's seed (the fixture's).
      // docs/design.md section 2, "Identity, scoped policy, QUIC on Node":
      const issuer = await CredentialIssuer.ed25519FromSeed('auth1', authoritySeed);
      const holderKey = await Ed25519HolderKey.generate();
      const verifier = new CredentialVerifier({ ed25519Keys: { auth1: await issuer.publicKey() } });

      const node = new Switchboard({
        credential: await issuer.issue({
          kind: CredentialKind.node,
          identity: 'edge-1',
          scopes: [CredentialScope.of(Right.open, '*')],
          holderKey: holderKey.publicKey,
        }),
        holderKey,
        verifier,
        quic: nativeQuic(),
      });
      const quic = await node.listenQuic('0.0.0.0', 4433, {
        certificate: selfSignedCertificate('edge.example.com'),
        policy: ChannelPolicies.scoped(),
      });
      // End of the example.

      onTestFinished(() => node.close());
      expect(quic).toBe('quic://0.0.0.0:4433');
      expect(node.listeningEndpoints).toEqual([quic]);
      expect(node.isOwnEndpoint('quic://127.0.0.1:4433')).toBe(true);
      node.registerService(npc, answer);
      // A node of the same mesh reaches it, both identified, exporter-bound.
      const client = new Switchboard({
        credential: await npcCredential(),
        holderKey: npcKey,
        verifier: new TestVerifier(),
        quic: nativeQuic(),
        expectedIdentityFor: () => 'edge-1',
        muxOptions: fast,
      });
      onTestFinished(() => client.close());
      expect(await open(client, 'quic://127.0.0.1:4433', npc)).toBe('npc for npc-7');
      const connection = (await client.connect('quic://127.0.0.1:4433')) as QuicBindingConnection;
      expect(connection.peerIdentity?.identity).toBe('edge-1');
      expect(connection.peerIdentityBound).toBe(true);
      // The scoped policy refuses what the credential does not allow.
      expect(await open(client, 'quic://127.0.0.1:4433', chat)).toBe('closed permissionDenied');
    });
  },
);
