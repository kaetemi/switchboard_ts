/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// The binding over the real native provider: connectQuic and listenQuic in
// this process, over UDP on the loopback through quinn, every case twice:
// raw QUIC (quic://127.0.0.1:port) and WebTransport sessions
// (https://127.0.0.1:port/sb, HTTP/3 in the shim). The listener has a
// self-signed certificate; the dialler does not verify it, so the
// identification rules of the binding page apply (QuicIdentityRules.initiator
// and .listener). The connection-level cases of switchboard_quic's
// test/native_integration_test.dart; its cases between nodes (Talk,
// policies, the proxy, throughput) are in quic-node-native.test.ts.
//
// Without the addon (`npm run build:native`) the file is skipped with the
// reason; with SWITCHBOARD_NATIVE=required (CI) a missing addon fails it.

import { afterAll, beforeAll, describe, expect, onTestFinished, test } from 'vitest';
import { bytesEqual } from '../../src/core/bytes.ts';
import type { HolderKey } from '../../src/core/identity/contract.ts';
import type { MuxChannel, MuxOptions } from '../../src/core/mux/contract.ts';
import {
  type QuicListener,
  type QuicServerConfig,
  switchboardAlpn,
  webTransportExporterLabel,
} from '../../src/core/quic/contract.ts';
import type { QuicBindingConnection } from '../../src/core/quic/quic-binding-connection.ts';
import { connectQuic, type QuicConnectOptions } from '../../src/core/quic/quic-dial.ts';
import { QuicIdentityRules } from '../../src/core/quic/quic-identity-rules.ts';
import { listenQuic, type QuicBindingListener } from '../../src/core/quic/quic-listen.ts';
import { Status, StatusCode, SwitchboardError } from '../../src/core/status.ts';
import {
  type NativeCertificate,
  NativeQuicProvider,
  type NativeQuicSession,
  type NativeWebTransportListener,
  nativeQuic,
  nativeQuicAvailability,
  selfSignedCertificate,
} from '../../src/node/quic/index.ts';
import { FakeVerifier, fakeCredential, generateHolderKey } from '../unit/quic/identity-fixtures.ts';

const availability = nativeQuicAvailability();
if (!availability.available && process.env['SWITCHBOARD_NATIVE'] === 'required') {
  throw new Error(`SWITCHBOARD_NATIVE=required, but ${availability.reason}`);
}

const limit = 10_000;

/** Options without the control PING and with short waits, as the unit tests' `fast`. */
const fast: Partial<MuxOptions> = {
  goAwayGrace: 200,
  keepAliveInterval: undefined,
  closeConfirmTimeout: 5000,
};

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const bytes = (value: string): Uint8Array => encoder.encode(value);
const text = (data: Uint8Array): string => decoder.decode(data);

function pattern(length: number, seed = 0): Uint8Array {
  const out = new Uint8Array(length);
  for (let i = 0; i < length; i++) {
    out[i] = (i * 7 + Math.floor(i / 251) + seed) & 0xff;
  }
  return out;
}

function within<T>(promise: Promise<T>, ms = limit): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`not settled within ${ms} ms`)), ms);
    promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (error: unknown) => {
        clearTimeout(timer);
        reject(error);
      },
    );
  });
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function until(condition: () => boolean, ms = limit): Promise<void> {
  const deadline = Date.now() + ms;
  while (!condition()) {
    if (Date.now() > deadline) {
      throw new Error(`condition not met within ${ms} ms`);
    }
    await delay(5);
  }
}

function concat(parts: readonly Uint8Array[]): Uint8Array {
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.length;
  }
  return out;
}

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
let clientKey: HolderKey;
let serverKey: HolderKey;

/** How the listener side of a case behaves. */
interface ServerOptions {
  options?: Partial<MuxOptions>;
  rules?: QuicIdentityRules;
  localIdentity?: string;
  idle?: number;
  keepAlive?: number;
  /** Echo every channel's chunks back, then close in turn (the default); false: deliver to [channels]. */
  echo?: boolean;
  /** Identifies in return, naming the peer, once the peer's exporter-bound IDENT verified. */
  identity?: { credential: ReturnType<typeof fakeCredential>; holderKey: HolderKey };
}

/** A listener of the binding over the native provider, with what it accepted. */
interface Server {
  readonly quic: QuicListener;
  readonly binding: QuicBindingListener;
  readonly endpoint: string;
  readonly accepted: QuicBindingConnection[];
  readonly channels: MuxChannel[];
}

/** Echoes [channel]'s chunks back, then closes it in turn. */
async function echoChannel(channel: MuxChannel): Promise<void> {
  try {
    for await (const chunk of channel.readable) {
      await channel.send(chunk);
    }
    await channel.close();
  } catch {
    // The connection ended.
  }
}

/** A native listener on the loopback with the self-signed certificate; closed after the test. */
async function listen(mode: Mode, init: ServerOptions = {}): Promise<Server> {
  const options = { ...fast, ...init.options };
  const config: QuicServerConfig = {
    alpn: [switchboardAlpn],
    certificateDer: cert.certificateDer,
    privateKeyDer: cert.privateKeyDer,
    // The channels a peer may have open, plus its control stream.
    maxStreamsBidi: 129,
    idleTimeout: init.idle ?? 30_000,
    keepAlive: init.keepAlive ?? 10_000,
  };
  const quic = mode.wt
    ? await nativeQuic().listenWebTransport('127.0.0.1', 0, config, { path: '/sb' })
    : await nativeQuic().listen('127.0.0.1', 0, config);
  const binding = listenQuic(quic, {
    options,
    identityRules: init.rules ?? QuicIdentityRules.listener,
    localIdentity: init.localIdentity,
  });
  onTestFinished(() => binding.close());
  const accepted: QuicBindingConnection[] = [];
  const channels: MuxChannel[] = [];
  void (async () => {
    for await (const connection of binding.connections) {
      accepted.push(connection);
      onTestFinished(() => connection.close());
      const identity = init.identity;
      if (identity !== undefined) {
        void connection.peerIdentified.then(async (peer) => {
          const rules = connection.quicOptions.identity;
          if (rules.identifyOnlyToIdentifiedPeer && !connection.peerAuthenticated) {
            return;
          }
          await connection
            .identify(identity.credential, {
              holderKey: identity.holderKey,
              receiver: peer.identity,
            })
            .catch(() => {});
        });
      }
      void (async () => {
        for await (const channel of connection.incoming) {
          channels.push(channel);
          if (init.echo ?? true) {
            void echoChannel(channel);
          }
        }
      })();
    }
  })();
  return { quic, binding, endpoint: quic.endpoint, accepted, channels };
}

/** Dials [server] without verifying its certificate (unless pinned); closed after the test. */
async function dial(server: Server, init: QuicConnectOptions = {}): Promise<QuicBindingConnection> {
  const connection = await connectQuic(server.endpoint, nativeQuic(), {
    verifyCertificate: false,
    timeout: 5000,
    ...init,
    options: { ...fast, ...init.options },
  });
  onTestFinished(() => connection.close());
  return connection;
}

/**
 * Sends [payload] on [channel] and reads until the echo of all of it came
 * back. A peer's CLOSE ends a channel both ways, so a side closes only
 * once what it waits for arrived.
 */
async function exchange(channel: MuxChannel, payload: Uint8Array, ms = limit): Promise<Uint8Array> {
  const reader = channel.readable.getReader();
  const sending = channel.send(payload);
  const chunks: Uint8Array[] = [];
  let received = 0;
  try {
    while (received < payload.length) {
      const result = await within(reader.read(), ms);
      if (result.done) {
        break;
      }
      chunks.push(result.value);
      received += result.value.length;
    }
    await within(sending, ms);
  } finally {
    reader.releaseLock();
  }
  return concat(chunks);
}

/** Echoes [payload] on a new channel of [connection], then closes the channel. */
async function echoed(
  connection: QuicBindingConnection,
  payload: Uint8Array,
  bulk = false,
): Promise<Uint8Array> {
  const channel = connection.open(bytes('echo'));
  if (bulk) {
    channel.priority = 'bulk';
  }
  const reply = await exchange(channel, payload, 60_000);
  await within(channel.close());
  return reply;
}

describe.skipIf(!availability.available)(
  `the binding over the native provider${availability.available ? '' : ` (skipped: ${availability.reason})`}`,
  () => {
    beforeAll(async () => {
      cert = selfSignedCertificate('localhost');
      clientKey = await generateHolderKey();
      serverKey = await generateHolderKey();
    });

    afterAll(() => {
      NativeQuicProvider.shutdown();
    });

    for (const mode of modes) {
      describe(mode.label, () => {
        test('connections identify both ways with exporter-bound proofs over the TLS exporter both ends derived', async () => {
          const server = await listen(mode, {
            options: { identityVerifier: new FakeVerifier() },
            localIdentity: 'server-1',
            identity: {
              credential: fakeCredential('server-1', serverKey.publicKey),
              holderKey: serverKey,
            },
          });
          const initiated = await dial(server, {
            options: { identityVerifier: new FakeVerifier() },
            localIdentity: 'npc-7',
          });
          expect(initiated.quicOptions.identity.awaitPeerIdentity).toBe(true);
          // Opened at once, the channel waits for the listener's identity.
          const channel = initiated.open(bytes('npc'));
          await within(
            initiated.identify(fakeCredential('npc-7', clientKey.publicKey), {
              holderKey: clientKey,
              receiver: 'server-1',
            }),
          );
          expect(text(await exchange(channel, bytes('hello')))).toBe('hello');
          await within(channel.close());
          await until(() => server.accepted.length === 1);
          const accepted = server.accepted[0] as QuicBindingConnection;
          const session = initiated.session as NativeQuicSession;
          expect(session.protocol).toBe('switchboard');
          expect(server.endpoint.startsWith(mode.wt ? 'https://' : 'quic://')).toBe(true);
          expect(initiated.isWebTransport).toBe(mode.wt);
          expect(accepted.isWebTransport).toBe(mode.wt);
          expect(bytesEqual(session.peerCertificateHash ?? [], cert.sha256)).toBe(true);
          expect(initiated.exporterBound).toBe(true);
          expect(accepted.exporterBound).toBe(true);
          expect(accepted.peerIdentity?.identity).toBe('npc-7');
          expect(accepted.peerIdentityBound).toBe(true);
          // The listener identified in turn, naming the client.
          await until(() => initiated.peerIdentity !== undefined);
          expect(initiated.peerIdentity?.identity).toBe('server-1');
          expect(initiated.peerIdentity?.receiver).toBe('npc-7');
          expect(initiated.peerIdentityBound).toBe(true);
          expect(initiated.peerAuthenticated).toBe(true);
          // The proofs signed the exporter: 32 bytes, the same at both ends.
          const exporter = initiated.exporterForTesting as Uint8Array;
          expect(exporter).toHaveLength(32);
          expect(bytesEqual(accepted.exporterForTesting ?? [], exporter)).toBe(true);
          // Another session derives another.
          const other = await dial(server);
          expect(text(await echoed(other, bytes('second')))).toBe('second');
          await until(() => server.accepted.length === 2);
          const second = server.accepted[1]?.exporterForTesting as Uint8Array;
          expect(second).toHaveLength(32);
          expect(bytesEqual(second, exporter)).toBe(false);
        });

        test("a dialler awaiting the listener's identity, which never comes, opens nothing and gives up at its identity timeout; one without a credential is served", async () => {
          const server = await listen(mode, { options: { identityVerifier: new FakeVerifier() } });
          const waiting = await dial(server, {
            options: { identityVerifier: new FakeVerifier(), identityTimeout: 500 },
            localIdentity: 'npc-7',
          });
          expect(waiting.quicOptions.identity.awaitPeerIdentity).toBe(true);
          const channel = waiting.open(bytes('npc'));
          void channel.send(bytes('early')).catch(() => {});
          const error = await within(
            waiting
              .identify(fakeCredential('npc-7', clientKey.publicKey), { holderKey: clientKey })
              .catch((e: unknown) => e),
          );
          expect(error).toBeInstanceOf(SwitchboardError);
          expect((error as SwitchboardError).code).toBe(StatusCode.deadlineExceeded);
          // The listener verified the dialler, but nothing opened.
          expect(server.accepted[0]?.peerIdentityBound).toBe(true);
          expect(server.channels).toHaveLength(0);
          expect(channel.isWritable).toBe(false);
          const plain = await dial(server);
          expect(plain.quicOptions.identity.awaitPeerIdentity).toBe(false);
          expect(text(await echoed(plain, bytes('served')))).toBe('served');
          expect(plain.peerIdentity).toBeUndefined();
        });

        test('a pinned dialler reaches the listener with the right certificate hash, and not with a wrong one', async () => {
          const server = await listen(mode);
          const pinned = await dial(server, { serverCertificateHashes: [cert.sha256] });
          expect(text(await echoed(pinned, bytes('pinned')))).toBe('pinned');
          expect(pinned.quicOptions.identity.tlsAuthenticatesPeer).toBe(true);
          expect(pinned.peerAuthenticated).toBe(true);
          expect(bytesEqual(pinned.session.peerCertificateHash ?? [], cert.sha256)).toBe(true);
          const error = await dial(server, {
            serverCertificateHashes: [
              { algorithm: 'sha-256', value: new Uint8Array(32).fill(0xa5) },
            ],
          }).catch((e: unknown) => e);
          expect(error).toBeInstanceOf(SwitchboardError);
          expect((error as SwitchboardError).code).toBe(StatusCode.unavailable);
        });

        test('an 8 MiB bulk payload each way', async () => {
          const server = await listen(mode);
          const connection = await dial(server);
          const payload = pattern(8 << 20, 3);
          const start = performance.now();
          const reply = await echoed(connection, payload, true);
          const elapsed = performance.now() - start;
          expect(reply.length).toBe(payload.length);
          expect(bytesEqual(reply, payload)).toBe(true);
          console.log(
            `native binding (${mode.label}), bulk echo: 16 MiB in ${elapsed.toFixed(0)} ms = ` +
              `${((16 << 20) / (elapsed / 1000) / 1e6).toFixed(0)} MB/s`,
          );
          // Two at once, racing.
          const twins = await Promise.all([
            echoed(connection, pattern(300000, 1), true),
            echoed(connection, pattern(200000, 2), true),
          ]);
          expect(bytesEqual(twins[0], pattern(300000, 1))).toBe(true);
          expect(bytesEqual(twins[1], pattern(200000, 2))).toBe(true);
        });

        test('64 channels in parallel on one connection', async () => {
          const server = await listen(mode);
          const connection = await dial(server);
          const results = await Promise.all(
            Array.from({ length: 64 }, (_, i) => echoed(connection, pattern(10000 + i, i))),
          );
          for (let i = 0; i < 64; i++) {
            expect(
              bytesEqual(results[i] as Uint8Array, pattern(10000 + i, i)),
              `channel ${i}`,
            ).toBe(true);
          }
        });

        test('GOAWAY with a code and a reason, from the dialler', async () => {
          const server = await listen(mode, { echo: false });
          const connection = await dial(server);
          // An open channel: the receiver of the GOAWAY keeps the connection
          // while it lasts, so the close is the GOAWAY sender's, after its
          // grace.
          const channel = connection.open(bytes('echo'));
          await within(channel.send(bytes('x')));
          await until(() => server.accepted.length === 1 && server.channels.length === 1);
          const accepted = server.accepted[0] as QuicBindingConnection;
          await within(connection.goAway(Status.of(StatusCode.unavailable, 'upgrade')));
          expect(
            (await within(accepted.peerGoAwayStatus)).equals(
              Status.of(StatusCode.unavailable, 'upgrade'),
            ),
          ).toBe(true);
          expect(
            (await within(connection.done)).equals(Status.of(StatusCode.unavailable, 'upgrade')),
          ).toBe(true);
          const end = await within(accepted.session.done);
          expect([end.code, end.reason, end.byPeer, end.lost]).toEqual([
            14,
            'upgrade',
            true,
            false,
          ]);
          expect((await within(channel.done)).code).toBe(StatusCode.unavailable);
        });

        test('GOAWAY with a code and a reason, from the listener', async () => {
          const server = await listen(mode, { echo: false });
          const connection = await dial(server);
          const channel = connection.open(bytes('echo'));
          await within(channel.send(bytes('x')));
          await until(() => server.accepted.length === 1 && server.channels.length === 1);
          const accepted = server.accepted[0] as QuicBindingConnection;
          await within(accepted.goAway(Status.of(StatusCode.resourceExhausted, 'too busy')));
          expect(
            (await within(connection.peerGoAwayStatus)).equals(
              Status.of(StatusCode.resourceExhausted, 'too busy'),
            ),
          ).toBe(true);
          const end = await within(connection.session.done);
          expect([end.code, end.reason, end.byPeer, end.lost]).toEqual([
            8,
            'too busy',
            true,
            false,
          ]);
          expect((await within(channel.done)).code).toBe(StatusCode.goingAway);
          expect((await within(connection.done)).code).toBe(StatusCode.goingAway);
        });

        test('the idle timeout ends the channels with CONNECTION_LOST', async () => {
          const idle = 800;
          const server = await listen(mode, { idle, keepAlive: 0, echo: false });
          // No keep-alive, neither the stack's nor the control PING: nothing
          // is sent once the channel is open.
          const connection = await dial(server, { idleTimeout: idle, keepAlive: 0 });
          const channel = connection.open(bytes('echo'));
          await within(channel.send(bytes('x')));
          // quinn waits at least three probe timeouts, which a loaded runner
          // can stretch to seconds.
          expect((await within(channel.done, 20_000)).code).toBe(StatusCode.connectionLost);
          expect((await within(connection.done)).code).toBe(StatusCode.connectionLost);
          expect((await within(connection.session.done)).lost).toBe(true);
        }, 30_000);

        test('the stack keep-alive, a third of the idle timeout, holds a quiet connection open without the control PING', async () => {
          const idle = 800;
          const server = await listen(mode, { idle, keepAlive: 0 });
          const connection = await dial(server, { idleTimeout: idle });
          expect(text(await echoed(connection, bytes('x')))).toBe('x');
          await delay(idle * 3);
          expect(connection.isOpen).toBe(true);
          expect(text(await echoed(connection, bytes('still')))).toBe('still');
        });
      });
    }

    describe('WebTransport only', () => {
      const mode = modes[1] as Mode;

      test('the exporter the proofs signed is the session’s, with the label switchboard-ident', async () => {
        const server = await listen(mode, {
          options: { identityVerifier: new FakeVerifier() },
          localIdentity: 'server-1',
          identity: {
            credential: fakeCredential('server-1', serverKey.publicKey),
            holderKey: serverKey,
          },
        });
        const initiated = await dial(server, {
          options: { identityVerifier: new FakeVerifier() },
          localIdentity: 'npc-7',
        });
        await within(
          initiated.identify(fakeCredential('npc-7', clientKey.publicKey), {
            holderKey: clientKey,
          }),
        );
        await until(() => initiated.peerIdentity !== undefined);
        const exported = await initiated.session.exportKeyingMaterial(
          webTransportExporterLabel,
          new Uint8Array(0),
          32,
        );
        expect(bytesEqual(initiated.exporterForTesting ?? [], exported ?? [1])).toBe(true);
        const accepted = server.accepted[0] as QuicBindingConnection;
        expect(bytesEqual(accepted.exporterForTesting ?? [], exported ?? [1])).toBe(true);
        expect(initiated.peerIdentityBound).toBe(true);
        expect(accepted.peerIdentityBound).toBe(true);
      });

      test('a dial to another path is refused, and fails UNAVAILABLE', async () => {
        const server = await listen(mode);
        const elsewhere = server.endpoint.replace(/\/sb$/, '/elsewhere');
        const error = await connectQuic(elsewhere, nativeQuic(), {
          verifyCertificate: false,
          options: fast,
        }).catch((e: unknown) => e);
        expect(error).toBeInstanceOf(SwitchboardError);
        expect((error as SwitchboardError).code).toBe(StatusCode.unavailable);
        expect((server.quic as NativeWebTransportListener).answered.get(404)).toBe(1);
      });

      test('a WT_DRAIN_SESSION reads as a GOAWAY with GOING_AWAY, and the dialler closes once its channels are done', async () => {
        const server = await listen(mode);
        const connection = await dial(server);
        const channel = connection.open(bytes('echo'));
        expect(text(await exchange(channel, bytes('x')))).toBe('x');
        await until(() => server.accepted.length === 1);
        const accepted = server.accepted[0] as QuicBindingConnection;
        (accepted.session as NativeQuicSession).drain();
        expect((await within(connection.peerGoAwayStatus)).code).toBe(StatusCode.goingAway);
        // The channel open at the drain still works; the dialler closes once
        // it is done.
        expect(text(await exchange(channel, bytes('still')))).toBe('still');
        await within(channel.close());
        expect((await within(connection.done)).code).toBe(StatusCode.goingAway);
        const end = await within(accepted.session.done);
        expect(end.lost).toBe(false);
        expect(end.byPeer).toBe(true);
      });
    });
  },
);
