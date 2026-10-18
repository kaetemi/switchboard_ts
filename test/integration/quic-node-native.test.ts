/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// The QUIC binding between Node Switchboard nodes over the real native
// provider: nodes in this process talking over UDP on the loopback,
// through quinn, every case twice: raw QUIC (quic://127.0.0.1:port) and
// WebTransport sessions (https://127.0.0.1:port/sb, HTTP/3 in the shim).
// A listening node with a self-signed certificate (`listenQuic`,
// `listenWebTransport`); dialling nodes with a `QuicSwitchboardBinding`
// (or the `quic` option), which do not verify the certificate, so the
// identification rules of the binding page apply
// (`QuicIdentityRules.initiator` and `.listener`). Credentials are real,
// from an Ed25519 test authority. A port of `switchboard_quic`'s
// `test/native_integration_test.dart` at the level of nodes;
// `quic-binding-native.test.ts` runs its connection-level half over
// `connectQuic` and `listenQuic`.
//
// Without the addon (`npm run build:native`, or SWITCHBOARD_QUIC_ADDON)
// the file is skipped with the reason; with SWITCHBOARD_NATIVE=required a
// missing addon fails it.

import { afterAll, beforeAll, describe, expect, onTestFinished, test } from 'vitest';
import { ChannelAddress, ServiceAddress } from '../../src/core/address/index.ts';
import { bytesEqual } from '../../src/core/bytes.ts';
import { CredentialKind, Right } from '../../src/core/identity/contract.ts';
import type { Credential } from '../../src/core/identity/credential.ts';
import { CredentialIssuer } from '../../src/core/identity/credential-issuer.ts';
import { CredentialVerifier } from '../../src/core/identity/credential-verifier.ts';
import { Ed25519HolderKey } from '../../src/core/identity/holder-key.ts';
import { Scope } from '../../src/core/identity/scope.ts';
import type { MuxChannel, MuxOptions } from '../../src/core/mux/contract.ts';
import { Name } from '../../src/core/name.ts';
import { serviceRecord } from '../../src/core/naming/naming-protocol.ts';
import {
  type QuicCertificateHash,
  type QuicListener,
  webTransportIdentLabel,
} from '../../src/core/quic/index.ts';
import { QuicBindingChannel } from '../../src/core/quic/quic-binding-channel.ts';
import { QuicBindingConnection } from '../../src/core/quic/quic-binding-connection.ts';
import { QuicIdentityRules } from '../../src/core/quic/quic-identity-rules.ts';
import { QuicSwitchboardBinding } from '../../src/core/quic/quic-switchboard.ts';
import { Status, StatusCode } from '../../src/core/status.ts';
import { ChannelPolicies } from '../../src/core/switchboard/channel-policy.ts';
import type { ChannelHandler, ChannelPolicy } from '../../src/core/switchboard/contract.ts';
import { proxyHandler } from '../../src/core/switchboard/proxy.ts';
import { StaticResolver } from '../../src/core/switchboard/resolver.ts';
import type { TalkChannel } from '../../src/core/talk/index.ts';
import {
  type NativeCertificate,
  NativeQuicProvider,
  type NativeQuicSession,
  type NativeWebTransportListener,
  nativeQuic,
  nativeQuicAvailability,
  selfSignedCertificate,
} from '../../src/node/quic/index.ts';
import {
  type ListenQuicOptions,
  type QuicListenProvider,
  Switchboard,
  type SwitchboardOptions,
} from '../../src/node/switchboard.ts';
import {
  bytes,
  expectCode,
  expectStatus,
  fast,
  limit,
  pattern,
  settle,
  text,
  until,
  within,
} from '../unit/quic/harness.ts';

const availability = nativeQuicAvailability();
if (!availability.available && process.env['SWITCHBOARD_NATIVE'] === 'required') {
  throw new Error(`SWITCHBOARD_NATIVE=required, but ${availability.reason}`);
}

const echo = Name.of('echo');
const npc = Name.of('npc');
const sink = Name.of('sink');

/** The two kinds of session. */
interface Mode {
  readonly label: string;
  readonly wt: boolean;
}

const modes: readonly Mode[] = [
  { label: 'raw QUIC', wt: false },
  { label: 'WebTransport', wt: true },
];

let cert: NativeCertificate;
let authority: CredentialIssuer;
let clientKey: Ed25519HolderKey;
let serverKey: Ed25519HolderKey;

/** A verifier of the test authority's credentials. */
async function verifier(): Promise<CredentialVerifier> {
  const v = new CredentialVerifier();
  await v.addIssuer(authority);
  return v;
}

/** A node credential of [identity] for [key], allowed to open anything. */
function credential(identity: string, key: Ed25519HolderKey): Promise<Credential> {
  return authority.issue({
    kind: CredentialKind.node,
    identity,
    scopes: [Scope.of(Right.open, '*')],
    holderKey: key.publicKey,
  });
}

/** A Node node with the fast options and short identity and connect bounds, closed after the test. */
function node(options: SwitchboardOptions = {}): Switchboard {
  const s = new Switchboard({
    identityTimeout: 2000,
    connectTimeout: 5000,
    ...options,
    muxOptions: { ...fast, ...options.muxOptions },
  });
  onTestFinished(() => s.close());
  return s;
}

/** What a listening node's QUIC listener is, once started. */
interface Listening {
  /** The endpoint dialled. */
  readonly endpoint: string;
  /** The connections the node adopted from it, in order. */
  readonly accepted: QuicBindingConnection[];
  /** The provider's listener. */
  readonly quic: QuicListener;
}

/** How a listening node listens. */
interface ListenInit {
  readonly policy?: ChannelPolicy | undefined;
  readonly rules?: QuicIdentityRules | undefined;
  readonly idle?: number | undefined;
  readonly keepAlive?: number | undefined;
}

/**
 * Has [server] listen on the loopback in [mode] (`listenQuic`, or
 * `listenWebTransport` at `/sb`) with the self-signed certificate and the
 * node's windows; the node closes the listener.
 */
async function listen(mode: Mode, server: Switchboard, init: ListenInit = {}): Promise<Listening> {
  const accepted: QuicBindingConnection[] = [];
  void (async () => {
    for await (const connection of server.connections) {
      if (connection instanceof QuicBindingConnection && !connection.isInitiator) {
        accepted.push(connection);
      }
    }
  })();
  // The native provider, keeping the listener it makes.
  let quic: QuicListener | undefined;
  const provider: QuicListenProvider = {
    async listen(host, port, config, options) {
      quic = await nativeQuic().listen(host, port, config, options);
      return quic;
    },
    async listenWebTransport(host, port, config, options) {
      quic = await nativeQuic().listenWebTransport(host, port, config, options);
      return quic;
    },
  };
  const options: ListenQuicOptions = {
    certificate: cert,
    provider,
    policy: init.policy,
    identityRules: init.rules,
    // The channels a peer may have open, plus its control stream.
    maxStreamsBidi: 129,
    streamReceiveWindow: server.muxOptions.initialWindow,
    connectionReceiveWindow: server.muxOptions.receiveHighWaterMarkBytes,
    idleTimeout: init.idle ?? 30_000,
    keepAlive: init.keepAlive ?? 10_000,
  };
  const endpoint = mode.wt
    ? await server.listenWebTransport('127.0.0.1', 0, { ...options, path: '/sb' })
    : await server.listenQuic('127.0.0.1', 0, options);
  if (quic === undefined) {
    throw new Error('the provider made no listener');
  }
  return { endpoint, accepted, quic };
}

/**
 * A dialling binding over the native provider: the certificate is not
 * verified (identity comes from IDENT), unless pinned.
 */
function dialler(
  init: {
    certificateHashes?: readonly QuicCertificateHash[];
    idleTimeout?: number;
    keepAlive?: number;
  } = {},
): QuicSwitchboardBinding {
  return new QuicSwitchboardBinding(nativeQuic(), {
    serverCertificateHashes: init.certificateHashes ?? [],
    idleTimeout: init.idleTimeout ?? 30_000,
    keepAlive: init.keepAlive,
    webTransportCertificateVerified: false,
  });
}

/** A Talk service answering every request with its payload. */
const echoService: ChannelHandler = (incoming) => {
  void incoming.talk().serve((m) => {
    if (m.expectsReply) {
      void m.reply(m.payload);
    }
  });
};

/** A handler answering with the peer's identity and closing. */
const answer: ChannelHandler = (incoming) => {
  const who = incoming.peerIdentity?.identity ?? 'nobody';
  void incoming.channel.send(bytes(`${incoming.address.type} for ${who}`)).catch(() => {});
  void incoming.channel.close().catch(() => {});
};

/** What an {@link answer} handler said, or the close status. */
async function answerOf(channel: MuxChannel): Promise<string> {
  const frames: Uint8Array[] = [];
  await within(
    (async () => {
      for await (const frame of channel.readable) frames.push(frame);
    })(),
  );
  const status = await within(channel.done);
  if (frames.length === 0) {
    return `closed ${status.name}`;
  }
  return text(frames[0] as Uint8Array);
}

/** The payload of the reply to the request [procedure] with [payload] on [talk]. */
async function ask(
  talk: TalkChannel,
  payload: Uint8Array,
  procedure = 'ECHO',
  ms = limit,
): Promise<Uint8Array> {
  return (await within(talk.request(procedure, payload, { timeout: ms }), ms)).payload;
}

/** The QUIC connection [talk] runs over. */
function connectionOf(talk: TalkChannel): QuicBindingConnection {
  return (talk.raw as MuxChannel).connection as QuicBindingConnection;
}

function rate(length: number, elapsed: number): string {
  const mbs = length / (elapsed / 1000) / 1e6;
  return `${length >> 20} MiB in ${elapsed.toFixed(0)} ms = ${mbs.toFixed(0)} MB/s`;
}

/** The cases of the binding between nodes, over [mode]. */
function cases(mode: Mode): void {
  const npcAddress = (): ChannelAddress => new ChannelAddress({ type: npc });
  const echoAddress = (): ChannelAddress => new ChannelAddress({ type: echo });

  describe('identity', () => {
    test('nodes identify both ways with exporter-bound proofs over the TLS exporter both ends derived', async () => {
      const server = node({
        verifier: await verifier(),
        credential: await credential('server-1', serverKey),
        holderKey: serverKey,
      });
      server.registerService(npc, answer);
      const listener = await listen(mode, server, { policy: ChannelPolicies.scoped() });
      const binding = dialler();
      const client = node({
        bindings: [binding],
        verifier: await verifier(),
        credential: await credential('npc-7', clientKey),
        holderKey: clientKey,
        expectedIdentityFor: () => 'server-1',
      });
      expect(await answerOf(await client.openChannelAt(listener.endpoint, npcAddress()))).toBe(
        'npc for npc-7',
      );
      expect(binding.dialled).toHaveLength(1);
      const initiated = binding.dialled[0] as QuicBindingConnection;
      await until(() => listener.accepted.length === 1);
      const accepted = listener.accepted[0] as QuicBindingConnection;
      expect(initiated.session.protocol).toBe('switchboard');
      expect(listener.endpoint.startsWith(mode.wt ? 'https://' : 'quic://')).toBe(true);
      expect(initiated.isWebTransport).toBe(mode.wt);
      expect(accepted.isWebTransport).toBe(mode.wt);
      expect(bytesEqual(initiated.session.peerCertificateHash ?? [], cert.sha256)).toBe(true);
      expect(initiated.quicOptions.identity.awaitPeerIdentity).toBe(true);
      expect(initiated.exporterBound).toBe(true);
      expect(accepted.exporterBound).toBe(true);
      expect(accepted.peerIdentity?.identity).toBe('npc-7');
      expect(accepted.peerIdentityBound).toBe(true);
      // The listener identified in turn, naming the client.
      expect(initiated.peerIdentity?.identity).toBe('server-1');
      expect(initiated.peerIdentity?.receiver).toBe('npc-7');
      expect(initiated.peerIdentityBound).toBe(true);
      expect(initiated.peerAuthenticated).toBe(true);
      // The proofs signed RFC 9266's tls-exporter: 32 bytes, the same at
      // both ends of the session.
      const exporter = initiated.exporterForTesting as Uint8Array;
      expect(exporter).toHaveLength(32);
      expect(bytesEqual(accepted.exporterForTesting ?? [], exporter)).toBe(true);
      // Another session derives another.
      const other = node({
        bindings: [dialler()],
        verifier: await verifier(),
        credential: await credential('npc-8', clientKey),
        holderKey: clientKey,
      });
      expect(await answerOf(await other.openChannelAt(listener.endpoint, npcAddress()))).toBe(
        'npc for npc-8',
      );
      await until(() => listener.accepted.length === 2);
      const second = listener.accepted[1]?.exporterForTesting as Uint8Array;
      expect(second).toHaveLength(32);
      expect(bytesEqual(second, exporter)).toBe(false);
    });

    test('a dialler without a credential is refused UNAUTHENTICATED where identity is required, and served where it is not', async () => {
      const server = node({ verifier: await verifier(), identityTimeout: 500 });
      server.registerService(npc, answer);
      const listener = await listen(mode, server, { policy: ChannelPolicies.scoped() });
      const binding = dialler();
      const client = node({ bindings: [binding] });
      const channel = await client.openChannelAt(listener.endpoint, npcAddress());
      expect(await answerOf(channel)).toBe('closed unauthenticated');
      expect(binding.dialled[0]?.peerIdentity).toBeUndefined();
      const open = node();
      open.registerService(npc, answer);
      const openListener = await listen(mode, open);
      expect(await answerOf(await client.openChannelAt(openListener.endpoint, npcAddress()))).toBe(
        'npc for nobody',
      );
    });

    test('a pinned dialler reaches the listener with the right certificate hash, and not with a wrong one', async () => {
      const server = node();
      server.registerService(npc, answer);
      const listener = await listen(mode, server);
      const pinned = dialler({ certificateHashes: [cert.sha256] });
      const client = node({ bindings: [pinned] });
      expect(await answerOf(await client.openChannelAt(listener.endpoint, npcAddress()))).toBe(
        'npc for nobody',
      );
      const connection = pinned.dialled[0] as QuicBindingConnection;
      expect(connection.quicOptions.identity.tlsAuthenticatesPeer).toBe(true);
      expect(connection.peerAuthenticated).toBe(true);
      expect(bytesEqual(connection.session.peerCertificateHash ?? [], cert.sha256)).toBe(true);
      const wrong = node({
        bindings: [dialler({ certificateHashes: [new Uint8Array(32).fill(0xa5)] })],
      });
      await expectCode(wrong.connect(listener.endpoint), StatusCode.unavailable);
    });
  });

  describe('channels', () => {
    test('Talk request and reply, dialled through the quic option', async () => {
      const server = node();
      server.registerService(echo, echoService);
      const listener = await listen(mode, server);
      const client = node({
        quic: nativeQuic(),
        quicBinding: { webTransportCertificateVerified: false },
      });
      const talk = await client.openTalkAt(listener.endpoint, echoAddress());
      expect(talk.raw).toBeInstanceOf(QuicBindingChannel);
      expect(text(await ask(talk, bytes('hello')))).toBe('hello');
      expect(await ask(talk, new Uint8Array(0))).toHaveLength(0);
      await within(talk.close());
      expect(connectionOf(talk).peerHello).toBeDefined();
      expect(connectionOf(talk).isWebTransport).toBe(mode.wt);
    });

    test('an 8 MiB bulk payload each way', async () => {
      const server = node();
      server.registerService(echo, echoService);
      const listener = await listen(mode, server);
      const client = node({ bindings: [dialler()] });
      const talk = await client.openTalkAt(listener.endpoint, echoAddress());
      const payload = pattern(8 << 20, 3);
      const start = performance.now();
      const reply = await within(talk.request('BIG', payload, { timeout: 60_000 }), 60_000);
      const elapsed = performance.now() - start;
      expect(reply.isBulk).toBe(true);
      expect(bytesEqual(reply.payload, payload)).toBe(true);
      console.log(
        `native binding (${mode.label}), Talk bulk echo between nodes: ` +
          `${rate(16 << 20, elapsed)} (8 MiB each way, 64 KiB stream windows)`,
      );
      // Two at once, their bulk streams racing.
      const twins = await within(
        Promise.all([
          talk.request('A', pattern(300_000, 1), { timeout: 30_000 }),
          talk.request('B', pattern(200_000, 2), { timeout: 30_000 }),
        ]),
        30_000,
      );
      expect(bytesEqual(twins[0].payload, pattern(300_000, 1))).toBe(true);
      expect(bytesEqual(twins[1].payload, pattern(200_000, 2))).toBe(true);
    }, 90_000);

    test('bursts of bulk messages are not refused: 30, 30 behind a 4 MiB one, and 100', async () => {
      // Each payload a bulk stream of its own, opened beside the parent
      // stream that carries the references: the streams of a burst arrive
      // side by side, ahead of the references, which the binding reads a
      // frame at a time, and behind a message held for its payload. Beyond
      // maxUnclaimedBulk (16) waiting, the rest were refused
      // RESOURCE_EXHAUSTED, without a lag (design §13 item 6).
      const server = node();
      const seen: number[] = [];
      server.registerService(sink, (incoming) => {
        void incoming.talk().serve((m) => {
          seen.push(m.payload.length);
        });
      });
      const listener = await listen(mode, server);
      const client = node({ bindings: [dialler()] });
      const talk = await client.openTalkAt(listener.endpoint, new ChannelAddress({ type: sink }));
      const send = (payload: Uint8Array): Promise<unknown> =>
        talk.send('PUT', payload).then(
          () => undefined,
          (e: unknown) => e,
        );
      const rounds: (readonly number[])[] = [
        Array(30).fill(50_000),
        [4 << 20, ...Array(30).fill(50_000)],
        Array(100).fill(50_000),
      ];
      let expected: number[] = [];
      for (const round of rounds) {
        const failures = (
          await within(Promise.all(round.map((n, i) => send(pattern(n, i)))), 60_000)
        ).filter((e) => e !== undefined);
        expect(failures).toEqual([]);
        expected = [...expected, ...round];
        await until(() => seen.length === expected.length);
        expect(seen).toEqual(expected);
      }
      expect(talk.isOpen).toBe(true);
    }, 90_000);

    test('whenWritable paces a publisher at the stream window', async () => {
      const server = node();
      let served: TalkChannel | undefined;
      server.registerService(echo, (incoming) => {
        // The receiver takes nothing until told to.
        served = incoming.talk();
      });
      const listener = await listen(mode, server);
      const client = node({ bindings: [dialler()] });
      const talk = await client.openTalkAt(listener.endpoint, echoAddress());
      const count = 400;
      let sent = 0;
      const publisher = (async () => {
        for (let i = 0; i < count; i++) {
          await talk.whenWritable;
          void talk.send('EVENT', new Uint8Array(1000));
          sent++;
        }
      })();
      await until(() => served !== undefined);
      // Until the publisher stalls (a loaded runner may take a while).
      const start = Date.now();
      let last = -1;
      while (sent !== last && Date.now() - start < limit) {
        last = sent;
        await settle(300);
      }
      // The receiver takes nothing: the stream window (64 KiB) and the
      // record the binding reads are all the publisher got out.
      expect(sent).toBe(last);
      expect(talk.isWritable).toBe(false);
      expect(sent).toBeLessThan(80);
      let received = 0;
      const reading = (async () => {
        for await (const _ of (served as TalkChannel).messages) {
          if (++received === count) {
            return;
          }
        }
      })();
      await within(publisher);
      await within(reading);
      expect(received).toBe(count);
    });

    test("a reader paused by its application observes the peer's reset at once", async () => {
      // The listener's own confirmation timeout is long: only the reset
      // ends its channel early.
      const server = node({ muxOptions: { closeConfirmTimeout: 30_000 } });
      let served: MuxChannel | undefined;
      server.registerService(sink, (incoming) => {
        // Takes nothing: the stream window fills and the dialler's writes
        // stall behind it.
        served = incoming.channel;
      });
      const listener = await listen(mode, server);
      const client = node({
        muxOptions: { closeConfirmTimeout: 300 },
        bindings: [dialler()],
      });
      const channel = await client.openChannelAt(
        listener.endpoint,
        new ChannelAddress({ type: sink }),
      );
      for (let i = 0; i < 10; i++) {
        void channel.send(pattern(30_000, i)).catch(() => {});
      }
      await until(() => served !== undefined);
      await settle(100);
      // A failure: STOP_SENDING at once, and the CLOSE record behind data
      // the listener does not read, which the confirmation timeout turns
      // into a reset.
      const start = Date.now();
      void channel.close(Status.of(StatusCode.cancelled, 'abandoned')).catch(() => {});
      expectStatus(await within(channel.done), StatusCode.cancelled);
      expectStatus(await within((served as MuxChannel).done, 5000), StatusCode.cancelled);
      expect(Date.now() - start).toBeLessThan(5000);
    });

    test('64 channels in parallel on one connection, three times over: the stream credit of ended channels comes back', async () => {
      const server = node();
      server.registerService(echo, echoService);
      // 129 streams: 128 channels and the control stream.
      const listener = await listen(mode, server);
      const binding = dialler();
      const client = node({ bindings: [binding] });
      for (let round = 0; round < 3; round++) {
        const results = await within(
          Promise.all(
            Array.from({ length: 64 }, async (_, i) => {
              const talk = await client.openTalkAt(listener.endpoint, echoAddress());
              // Every eighth one carries a bulk payload.
              const payload = pattern(i % 8 === 0 ? 100_000 + i : 100 + i, i + round);
              const reply = await ask(talk, payload, 'ECHO', 60_000);
              await talk.close();
              return bytesEqual(reply, payload);
            }),
          ),
          60_000,
        );
        expect(results.every((ok) => ok)).toBe(true);
      }
      expect(binding.dialled).toHaveLength(1);
      const connection = binding.dialled[0] as QuicBindingConnection;
      expect(connection.peerMaxChannels).toBe(128);
      await until(() => connection.openChannelCount === 0);
    }, 120_000);

    test('throughput of one channel', async () => {
      const measure = async (options: Partial<MuxOptions>, label: string): Promise<string> => {
        const total = 64 << 20;
        const server = node({ muxOptions: options });
        let got = (): void => {};
        const received = new Promise<void>((resolve) => {
          got = resolve;
        });
        server.registerService(sink, (incoming) => {
          void (async () => {
            let n = 0;
            for await (const data of incoming.channel.readable) {
              n += data.length;
              if (n >= total) {
                got();
              }
            }
          })().catch(() => {});
        });
        const listener = await listen(mode, server);
        const client = node({ muxOptions: options, bindings: [dialler()] });
        const channel = await client.openChannelAt(
          listener.endpoint,
          new ChannelAddress({ type: sink }),
        );
        const connection = channel.connection as QuicBindingConnection;
        await until(() => connection.peerHello !== undefined);
        const chunk = pattern(channel.maxSubframeLength);
        const start = performance.now();
        let sent = 0;
        while (sent < total) {
          await channel.whenWritable;
          // Two records per write: the binding batches what waits.
          for (let i = 0; i < 2 && sent < total; i++) {
            const n = Math.min(total - sent, chunk.length);
            void channel.send(n === chunk.length ? chunk : chunk.subarray(0, n));
            sent += n;
          }
        }
        await within(received, 60_000);
        const line =
          `native binding (${mode.label}), one channel between nodes: ` +
          `${rate(total, performance.now() - start)} (${label}, ${chunk.length}-byte records)`;
        await channel.close();
        return line;
      };
      console.log(await measure({}, '64 KiB stream window'));
      console.log(
        await measure({ closeConfirmTimeout: 5000, initialWindow: 1 << 20 }, '1 MiB stream window'),
      );
    }, 180_000);
  });

  describe('the end of a connection', () => {
    test('GOAWAY with a code and a reason, from the dialler', async () => {
      const server = node();
      server.registerService(echo, echoService);
      const listener = await listen(mode, server);
      const binding = dialler();
      const client = node({ bindings: [binding] });
      // An open channel: the receiver of the GOAWAY keeps the connection
      // while it lasts, so the close is the GOAWAY sender's, after its
      // grace.
      const talk = await client.openTalkAt(listener.endpoint, echoAddress());
      await ask(talk, bytes('x'));
      const connection = binding.dialled[0] as QuicBindingConnection;
      const accepted = listener.accepted[0] as QuicBindingConnection;
      const upgrade = Status.of(StatusCode.unavailable, 'upgrade');
      await within(connection.goAway(upgrade));
      expect((await within(accepted.peerGoAwayStatus)).equals(upgrade)).toBe(true);
      expect((await within(connection.done)).equals(upgrade)).toBe(true);
      const end = await within(accepted.session.done);
      expect([end.code, end.reason, end.byPeer, end.lost]).toEqual([14, 'upgrade', true, false]);
      expectStatus(await within((talk.raw as MuxChannel).done), StatusCode.unavailable);
    });

    test('GOAWAY with a code and a reason, from the listener', async () => {
      const server = node();
      server.registerService(echo, echoService);
      const listener = await listen(mode, server);
      const binding = dialler();
      const client = node({ bindings: [binding] });
      const talk = await client.openTalkAt(listener.endpoint, echoAddress());
      await ask(talk, bytes('x'));
      const connection = binding.dialled[0] as QuicBindingConnection;
      const accepted = listener.accepted[0] as QuicBindingConnection;
      const busy = Status.of(StatusCode.resourceExhausted, 'too busy');
      await within(accepted.goAway(busy));
      expect((await within(connection.peerGoAwayStatus)).equals(busy)).toBe(true);
      const end = await within(connection.session.done);
      expect([end.code, end.reason, end.byPeer, end.lost]).toEqual([8, 'too busy', true, false]);
      expectStatus(await within((talk.raw as MuxChannel).done), StatusCode.goingAway);
      expectStatus(await within(connection.done), StatusCode.goingAway);
    });

    test('the idle timeout ends the channels with CONNECTION_LOST', async () => {
      const server = node();
      server.registerService(echo, echoService);
      const idle = 800;
      const listener = await listen(mode, server, { idle, keepAlive: 0 });
      const binding = dialler({ idleTimeout: idle, keepAlive: 0 });
      // No keep-alive, neither the stack's nor the control PING (fast):
      // nothing is sent once the request is answered.
      const client = node({ bindings: [binding] });
      const talk = await client.openTalkAt(listener.endpoint, echoAddress());
      await ask(talk, bytes('x'));
      const connection = binding.dialled[0] as QuicBindingConnection;
      // quinn waits at least three probe timeouts, which a loaded runner
      // can stretch to seconds.
      expectStatus(await within((talk.raw as MuxChannel).done, 20_000), StatusCode.connectionLost);
      expectStatus(await within(connection.done), StatusCode.connectionLost);
      expect((await within(connection.session.done)).lost).toBe(true);
    }, 40_000);

    test('the stack keep-alive, a third of the idle timeout, holds a quiet connection open without the control PING', async () => {
      const server = node();
      server.registerService(echo, echoService);
      const idle = 800;
      const listener = await listen(mode, server, { idle, keepAlive: 0 });
      // The control PING is off (fast); the binding's stack keep-alive is
      // not.
      const binding = dialler({ idleTimeout: idle });
      const client = node({ bindings: [binding] });
      const talk = await client.openTalkAt(listener.endpoint, echoAddress());
      await ask(talk, bytes('x'));
      await settle(idle * 3);
      const connection = binding.dialled[0] as QuicBindingConnection;
      expect(connection.isOpen).toBe(true);
      expect(text(await ask(talk, bytes('still')))).toBe('still');
    });
  });

  test('the proxy: a frame mux backend behind a QUIC front, bulk twins forwarded in order', async () => {
    const backend = node();
    const received: Uint8Array[] = [];
    backend.registerService(
      echo,
      (incoming) => {
        void incoming.talk().serve((m) => {
          received.push(m.payload);
          if (m.expectsReply) {
            void m.reply(m.payload);
          }
        });
      },
      { instance: 1 },
    );
    const backendUri = await backend.listenMemory();
    const resolver = new StaticResolver([
      serviceRecord(new ServiceAddress(echo, 1), { endpoints: [backendUri] }),
    ]);
    onTestFinished(() => resolver.close());
    const proxy = node({ resolver });
    proxy.catchAll = proxyHandler(proxy);
    const front = await listen(mode, proxy, { rules: QuicIdentityRules.none });
    const client = node({ bindings: [dialler()] });
    const talk = await client.openTalkAt(front.endpoint, echoAddress());
    expect(talk.raw).toBeInstanceOf(QuicBindingChannel);
    expect(text(await ask(talk, bytes('through')))).toBe('through');
    const first = pattern(400_000, 1);
    const second = pattern(300_000, 2);
    const replies = await within(
      Promise.all([
        talk.request('A', first, { timeout: 30_000 }),
        talk.request('B', second, { timeout: 30_000 }),
      ]),
      30_000,
    );
    expect(bytesEqual(replies[0].payload, first)).toBe(true);
    expect(bytesEqual(replies[1].payload, second)).toBe(true);
    expect(received.map((payload) => payload.length)).toEqual([7, first.length, second.length]);
    expect(text(received[0] as Uint8Array)).toBe('through');
    expect(bytesEqual(received[1] as Uint8Array, first)).toBe(true);
    expect(bytesEqual(received[2] as Uint8Array, second)).toBe(true);
  }, 60_000);
}

/** The cases particular to WebTransport. */
function webTransportCases(mode: Mode): void {
  test('the exporter the proofs signed is the session’s, with the label switchboard-ident', async () => {
    const server = node({
      verifier: await verifier(),
      credential: await credential('server-1', serverKey),
      holderKey: serverKey,
    });
    server.registerService(npc, answer);
    const listener = await listen(mode, server, { policy: ChannelPolicies.scoped() });
    const binding = dialler();
    const client = node({
      bindings: [binding],
      verifier: await verifier(),
      credential: await credential('npc-7', clientKey),
      holderKey: clientKey,
    });
    expect(
      await answerOf(
        await client.openChannelAt(listener.endpoint, new ChannelAddress({ type: npc })),
      ),
    ).toBe('npc for npc-7');
    const initiated = binding.dialled[0] as QuicBindingConnection;
    const exported = await initiated.session.exportKeyingMaterial(
      webTransportIdentLabel,
      new Uint8Array(0),
      32,
    );
    expect(exported).toBeDefined();
    expect(bytesEqual(initiated.exporterForTesting ?? [], exported ?? [])).toBe(true);
    await until(() => listener.accepted.length === 1);
    const accepted = listener.accepted[0] as QuicBindingConnection;
    expect(bytesEqual(accepted.exporterForTesting ?? [], exported ?? [])).toBe(true);
    expect(initiated.peerIdentityBound).toBe(true);
    expect(accepted.peerIdentityBound).toBe(true);
  });

  test('a dial to another path is refused, and fails UNAVAILABLE', async () => {
    const server = node();
    server.registerService(npc, answer);
    const listener = await listen(mode, server);
    const client = node({ bindings: [dialler()] });
    const elsewhere = listener.endpoint.replace(/\/sb$/, '/elsewhere');
    await expectCode(client.connect(elsewhere), StatusCode.unavailable);
    expect((listener.quic as NativeWebTransportListener).answered.get(404)).toBe(1);
  });

  test('a WT_DRAIN_SESSION reads as a GOAWAY with GOING_AWAY, and the dialler closes once its channels are done', async () => {
    const server = node();
    server.registerService(echo, echoService);
    const listener = await listen(mode, server);
    const binding = dialler();
    const client = node({ bindings: [binding] });
    const talk = await client.openTalkAt(listener.endpoint, new ChannelAddress({ type: echo }));
    await ask(talk, bytes('x'));
    const connection = binding.dialled[0] as QuicBindingConnection;
    const accepted = listener.accepted[0] as QuicBindingConnection;
    (accepted.session as NativeQuicSession).drain();
    expectStatus(await within(connection.peerGoAwayStatus), StatusCode.goingAway);
    // The channel open at the drain still works; the dialler closes once it
    // is done.
    expect(text(await ask(talk, bytes('still')))).toBe('still');
    await within(talk.close());
    expectStatus(await within(connection.done), StatusCode.goingAway);
    const end = await within(accepted.session.done);
    expect(end.lost).toBe(false);
    expect(end.byPeer).toBe(true);
  });
}

describe.skipIf(!availability.available)(
  `Switchboard nodes over the native provider${availability.available ? '' : ` (skipped: ${availability.reason})`}`,
  () => {
    beforeAll(async () => {
      cert = selfSignedCertificate('localhost');
      authority = await CredentialIssuer.ed25519FromSeed(
        'auth1',
        Uint8Array.from({ length: 32 }, (_, i) => 3 * i),
      );
      clientKey = await Ed25519HolderKey.generate();
      serverKey = await Ed25519HolderKey.generate();
    });

    afterAll(() => {
      NativeQuicProvider.shutdown();
    });

    for (const mode of modes) {
      describe(mode.label, () => {
        cases(mode);
      });
    }

    describe('WebTransport only', () => {
      webTransportCases(modes[1] as Mode);
    });
  },
);
