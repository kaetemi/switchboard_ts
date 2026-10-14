/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// The native provider end to end over the loopback: both sides in this process, the shim's
// runtime underneath. A port of switchboard_quic's test/native_provider_test.dart, case by case
// and named after it: the cases of the contract run twice, over raw QUIC and over WebTransport
// sessions (HTTP/3 in the shim); the cases particular to either follow. Dart isolates become
// worker threads. The cases at the end are particular to JavaScript: loading without the addon,
// what a write copies, and whether a process exits on its own.
//
// Without the addon (`npm run build:native`) the file is skipped with the reason; with
// SWITCHBOARD_NATIVE=required (CI) a missing addon fails it instead.

import { createSocket } from 'node:dgram';
import { afterAll, afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import {
  type QuicClientConfig,
  QuicError,
  type QuicErrorKind,
  type QuicServerConfig,
  type QuicSession,
  type QuicStream,
  QuicStreamReset,
  QuicStreamStopped,
} from '../../src/core/quic/contract.ts';
import {
  certificateHash,
  type NativeCertificate,
  type NativeQuicListener,
  NativeQuicProvider,
  type NativeQuicSession,
  nativeQuic,
  nativeQuicAvailability,
  selfSignedCertificate,
} from '../../src/node/quic/index.ts';

// Environment variables: CI sets the first to make a missing addon fail the run instead of skipping
// it; the provider loads the addon from the second when it is set.
const requiredVariable = 'SWITCHBOARD_NATIVE';
const addonVariable = 'SWITCHBOARD_QUIC_ADDON';

const availability = nativeQuicAvailability();
if (!availability.available && process.env[requiredVariable] === 'required') {
  throw new Error(`SWITCHBOARD_NATIVE=required, but ${availability.reason}`);
}

const quicAlpn = 'switchboard';

// --- Helpers -----------------------------------------------------------------------------------

/** The two kinds of session the provider runs. */
interface Mode {
  readonly label: string;
  readonly wt: boolean;
  /** The label the binding asks for: RFC 9266's on raw QUIC, the application's over WebTransport. */
  readonly exporterLabel: string;
  /** The largest stream and close code this API carries. */
  readonly maxCode: number;
  readonly scheme: string;
}

const raw: Mode = {
  label: 'raw QUIC',
  wt: false,
  exporterLabel: 'EXPORTER-Channel-Binding',
  maxCode: Number.MAX_SAFE_INTEGER,
  scheme: 'quic',
};
const webTransport: Mode = {
  label: 'WebTransport',
  wt: true,
  exporterLabel: 'switchboard-ident',
  maxCode: 0xffffffff,
  scheme: 'https',
};

let cert: NativeCertificate;
const provider = nativeQuic();

function serverConfig(
  o: {
    maxStreams?: number;
    window?: number;
    connectionWindow?: number;
    idle?: number;
    keepAlive?: number;
    handshake?: number;
  } = {},
): QuicServerConfig {
  return {
    alpn: [quicAlpn],
    certificateDer: cert.certificateDer,
    privateKeyDer: cert.privateKeyDer,
    maxStreamsBidi: o.maxStreams ?? 128,
    streamReceiveWindow: o.window ?? 1 << 20,
    connectionReceiveWindow: o.connectionWindow ?? 16 << 20,
    idleTimeout: o.idle ?? 30_000,
    keepAlive: o.keepAlive ?? 10_000,
    handshakeTimeout: o.handshake ?? 10_000,
  };
}

function clientConfig(
  o: {
    alpn?: readonly string[];
    pins?: readonly Uint8Array[];
    verify?: boolean;
    window?: number;
    idle?: number;
    keepAlive?: number;
    handshake?: number;
  } = {},
): QuicClientConfig {
  return {
    alpn: o.alpn ?? [quicAlpn],
    serverCertificateHashes: o.pins ?? [],
    verifyCertificate: o.verify ?? false,
    streamReceiveWindow: o.window ?? 1 << 20,
    idleTimeout: o.idle ?? 30_000,
    keepAlive: o.keepAlive ?? 10_000,
    handshakeTimeout: o.handshake ?? 10_000,
  };
}

/** Run after the test, last registered first. */
const teardowns: (() => unknown)[] = [];

function addTearDown(f: () => unknown): void {
  teardowns.push(f);
}

async function runTearDowns(): Promise<void> {
  const errors: unknown[] = [];
  for (let f = teardowns.pop(); f !== undefined; f = teardowns.pop()) {
    try {
      await f();
    } catch (error) {
      errors.push(error);
    }
  }
  if (errors.length > 0) throw errors[0];
}

async function next<T>(it: AsyncIterator<T>): Promise<T> {
  const r = await it.next();
  if (r.done === true) throw new Error('the iteration ended');
  return r.value;
}

function iterate<T>(iterable: AsyncIterable<T>): AsyncIterator<T> {
  return iterable[Symbol.asyncIterator]();
}

/** A listener and its accepted sessions, closed after the test: raw QUIC, or WebTransport on `/sb`. */
class Server {
  readonly listener: NativeQuicListener;
  readonly mode: Mode;
  readonly sessions: AsyncIterator<NativeQuicSession>;
  readonly #accepted: QuicSession[] = [];

  private constructor(listener: NativeQuicListener, mode: Mode) {
    this.listener = listener;
    this.mode = mode;
    this.sessions = iterate(listener.sessions);
  }

  static async start(
    mode: Mode,
    config?: QuicServerConfig,
    allowedOrigins?: readonly string[],
    protocols: readonly string[] = ['switchboard'],
  ): Promise<Server> {
    const listener = mode.wt
      ? await provider.listenWebTransport('127.0.0.1', 0, config ?? serverConfig(), {
          path: '/sb',
          allowedOrigins,
          protocols,
        })
      : await provider.listen('127.0.0.1', 0, config ?? serverConfig());
    const server = new Server(listener, mode);
    addTearDown(() => server.close());
    return server;
  }

  get endpoint(): string {
    return this.listener.endpoint;
  }

  get port(): number {
    return this.listener.port;
  }

  accept(): Promise<NativeQuicSession> {
    const accepted = next(this.sessions).then((s) => {
      this.#accepted.push(s);
      return s;
    });
    accepted.catch(() => {});
    return accepted;
  }

  async close(): Promise<void> {
    for (const s of this.#accepted) await s.close(0, '');
    await this.listener.close();
  }
}

/** A client session and the server session it reached. */
class Pair {
  readonly client: NativeQuicSession;
  readonly server: NativeQuicSession;
  readonly serverStreams: AsyncIterator<QuicStream>;
  readonly clientStreams: AsyncIterator<QuicStream>;

  constructor(client: NativeQuicSession, server: NativeQuicSession) {
    this.client = client;
    this.server = server;
    this.serverStreams = iterate(server.incomingStreams);
    this.clientStreams = iterate(client.incomingStreams);
  }

  static async connect(server: Server, config?: QuicClientConfig): Promise<Pair> {
    const accepted = server.accept();
    const client = await provider.connect(server.endpoint, config ?? clientConfig());
    const pair = new Pair(client, await accepted);
    addTearDown(() => client.close(0, ''));
    return pair;
  }
}

function bytes(s: string): Uint8Array {
  return Uint8Array.from(s, (c) => c.charCodeAt(0));
}

function text(b: Uint8Array): string {
  return String.fromCharCode(...b);
}

function pattern(length: number, seed = 0): Uint8Array {
  const b = new Uint8Array(length);
  for (let i = 0; i < length; i++) b[i] = (i * 31 + seed) & 0xff;
  return b;
}

function sameBytes(a: Uint8Array, b: Uint8Array): boolean {
  return Buffer.from(a.buffer, a.byteOffset, a.length).equals(
    Buffer.from(b.buffer, b.byteOffset, b.length),
  );
}

async function readAll(s: QuicStream, max = 65536): Promise<Uint8Array> {
  const chunks: Uint8Array[] = [];
  for (let c = await s.read(max); c !== undefined; c = await s.read(max)) chunks.push(c);
  const out = new Uint8Array(chunks.reduce((n, c) => n + c.length, 0));
  let o = 0;
  for (const c of chunks) {
    out.set(c, o);
    o += c.length;
  }
  return out;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Resolves with true if [p] settles within [ms], false otherwise. */
function completesWithin(p: Promise<unknown>, ms: number): Promise<boolean> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  return Promise.race([
    p.then(
      () => true,
      () => true,
    ),
    new Promise<boolean>((resolve) => {
      timer = setTimeout(() => resolve(false), ms);
    }),
  ]).finally(() => clearTimeout(timer));
}

/** [p], or a rejection once [ms] passed. */
function within<T>(p: Promise<T>, ms: number): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  return Promise.race([
    p,
    new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`timed out after ${ms} ms`)), ms);
    }),
  ]).finally(() => clearTimeout(timer));
}

async function failureOf(p: Promise<unknown>): Promise<unknown> {
  return p.then(
    () => undefined,
    (error: unknown) => error,
  );
}

async function expectKind(p: Promise<unknown>, kind: QuicErrorKind): Promise<QuicError> {
  const error = await failureOf(p);
  expect(error).toBeInstanceOf(QuicError);
  expect((error as QuicError).kind).toBe(kind);
  return error as QuicError;
}

function isReset(code: number | undefined): (e: unknown) => boolean {
  return (e) => e instanceof QuicStreamReset && e.code === code;
}

function isStopped(code: number | undefined): (e: unknown) => boolean {
  return (e) => e instanceof QuicStreamStopped && e.code === code;
}

function isSessionEnded(e: unknown): boolean {
  return e instanceof QuicError && e.kind === 'sessionEnded';
}

/** A UDP socket on the IPv4 loopback that never answers; counts what it receives. */
async function silentPeer(): Promise<{ port: number; received: () => number }> {
  const socket = createSocket('udp4');
  let received = 0;
  socket.on('message', () => received++);
  await new Promise<void>((resolve) => socket.bind(0, '127.0.0.1', resolve));
  addTearDown(() => socket.close());
  return { port: socket.address().port, received: () => received };
}

// --- Tests -------------------------------------------------------------------------------------

describe('native provider without its addon', () => {
  test('connecting rejects and listening fails with unsupportedScheme, naming the package', async () => {
    const saved = process.env[addonVariable];
    process.env[addonVariable] = '/nonexistent/sbq-node.node';
    vi.resetModules();
    try {
      // A fresh instance of the module, whose loader has not run yet: importing never loads.
      const m = (await import(
        '../../src/node/quic/native-provider.ts'
      )) as typeof import('../../src/node/quic/native-provider.ts');
      const availability = m.nativeQuicAvailability();
      expect(availability.available).toBe(false);
      const unsupported = { name: 'QuicError', kind: 'unsupportedScheme' };
      const target = `${process.platform}-${process.arch}`;
      for (const endpoint of ['quic://127.0.0.1:9', 'https://127.0.0.1:9/sb']) {
        const error = await failureOf(m.nativeQuic().connect(endpoint, clientConfig()));
        expect(error).toMatchObject(unsupported);
        expect((error as Error).message).toContain(`@polyverse/switchboard-quic-${target}`);
      }
      const server = {
        alpn: [quicAlpn],
        certificateDer: new Uint8Array(1),
        privateKeyDer: new Uint8Array(1),
      };
      expect(await failureOf(m.nativeQuic().listen('127.0.0.1', 0, server))).toMatchObject(
        unsupported,
      );
      expect(
        await failureOf(m.nativeQuic().listenWebTransport('127.0.0.1', 0, server, { path: '/sb' })),
      ).toMatchObject(unsupported);
      expect(() => m.selfSignedCertificate('localhost')).toThrow(/switchboard-quic/);
      // Hashing needs no addon.
      expect(m.certificateHash(new Uint8Array(0)).length).toBe(32);
    } finally {
      if (saved === undefined) {
        delete process.env[addonVariable];
      } else {
        process.env[addonVariable] = saved;
      }
      vi.resetModules();
    }
  });
});

describe.skipIf(!availability.available)(
  `native provider${availability.available ? '' : ` (skipped: ${availability.reason})`}`,
  () => {
    beforeEach(() => {
      cert ??= selfSignedCertificate('localhost');
    });
    afterEach(runTearDowns);
    afterAll(() => NativeQuicProvider.shutdown());

    describe('certificates and configurations', () => {
      test('self-signed certificate', () => {
        expect(cert.certificateDer[0]).toBe(0x30);
        expect(cert.privateKeyDer.length).toBeGreaterThan(0);
        expect(cert.sha256.length).toBe(32);
        expect(certificateHash(cert.certificateDer)).toEqual(cert.sha256);
        const other = selfSignedCertificate('10.0.0.1');
        expect(other.sha256).not.toEqual(cert.sha256);
      });

      test('unsupported schemes and unreachable endpoints', async () => {
        await expectKind(
          provider.connect('wss://127.0.0.1:4433/sb', clientConfig()),
          'unsupportedScheme',
        );
        await expectKind(
          provider.connectWebTransport('quic://127.0.0.1:4433', clientConfig()),
          'unsupportedScheme',
        );
        expect(
          await failureOf(provider.connect('quic://127.0.0.1', clientConfig())),
        ).toBeInstanceOf(QuicError);
        for (const endpoint of ['quic://127.0.0.1:9', 'https://127.0.0.1:9/sb']) {
          await expectKind(provider.connect(endpoint, clientConfig({ idle: 300 })), 'unreachable');
        }
        // WebTransport fields are printable ASCII.
        expect(
          await failureOf(
            provider.connect('https://127.0.0.1:9/sb', clientConfig({ alpn: ['é'] })),
          ),
        ).toBeInstanceOf(TypeError);
      });

      test('the handshake deadline ends a connect to a peer that never answers, even without an idle timeout', async () => {
        // Bound, so that nothing reports the port unreachable, and silent.
        const silent = await silentPeer();
        for (const scheme of ['quic', 'https']) {
          const started = performance.now();
          await expectKind(
            provider.connect(
              `${scheme}://127.0.0.1:${silent.port}/sb`,
              clientConfig({ idle: 0, keepAlive: 0, handshake: 300 }),
            ),
            'unreachable',
          );
          const elapsed = performance.now() - started;
          expect(elapsed).toBeGreaterThanOrEqual(290);
          expect(elapsed).toBeLessThan(2000);
        }
        expect(silent.received()).toBeGreaterThan(0);
      });

      test('a negative duration in a configuration is a RangeError, and a deadline under a millisecond is still a deadline', async () => {
        const negative = -0.001;
        for (const nowhere of ['quic://127.0.0.1:9', 'https://127.0.0.1:9/sb']) {
          for (const config of [
            clientConfig({ idle: negative }),
            clientConfig({ keepAlive: negative }),
            clientConfig({ handshake: negative }),
          ]) {
            expect(await failureOf(provider.connect(nowhere, config))).toBeInstanceOf(RangeError);
          }
        }
        for (const config of [
          serverConfig({ idle: negative }),
          serverConfig({ keepAlive: negative }),
          serverConfig({ handshake: negative }),
        ]) {
          expect(await failureOf(provider.listen('127.0.0.1', 0, config))).toBeInstanceOf(
            RangeError,
          );
          expect(
            await failureOf(provider.listenWebTransport('127.0.0.1', 0, config, { path: '/sb' })),
          ).toBeInstanceOf(RangeError);
        }
        expect(
          await failureOf(
            provider.listenWebTransport('127.0.0.1', 0, serverConfig(), { path: 'sb' }),
          ),
        ).toBeInstanceOf(TypeError);
        // Rounded up to a millisecond, not down to none: without an idle timeout nothing else
        // would end this connect.
        const silent = await silentPeer();
        await expectKind(
          within(
            provider.connect(
              `quic://127.0.0.1:${silent.port}`,
              clientConfig({ idle: 0, keepAlive: 0, handshake: 0.001 }),
            ),
            2000,
          ),
          'unreachable',
        );
      });
    });

    for (const mode of [raw, webTransport]) {
      describe(mode.label, () => contract(mode));
    }
  },
);

/** The cases of the provider contract, over [mode]. */
function contract(mode: Mode): void {
  const label = mode.exporterLabel;

  describe('handshake', () => {
    test('unverified client: handshake, protocol, remote', async () => {
      const server = await Server.start(mode);
      const p = await Pair.connect(server);
      expect(p.client.isInitiator).toBe(true);
      expect(p.server.isInitiator).toBe(false);
      expect(p.client.protocol).toBe(quicAlpn);
      expect(p.server.protocol).toBe(quicAlpn);
      expect(p.client.remote).toBe(`127.0.0.1:${server.port}`);
      expect(p.server.remote.startsWith('127.0.0.1:')).toBe(true);
      expect(p.client.peerCertificateHash).toEqual(cert.sha256);
      expect(p.server.peerCertificateHash).toBeUndefined();
      expect(p.client.maxIncomingStreams).toBe(128);
      expect(new URL(server.endpoint).protocol).toBe(`${mode.scheme}:`);
      expect(p.client.isWebTransport).toBe(mode.wt);
      expect(p.client.stats?.udpTxBytes).toBeGreaterThan(0);
    });

    test('pinned client: the right hash connects', async () => {
      const server = await Server.start(mode);
      const other = selfSignedCertificate('localhost');
      const p = await Pair.connect(server, clientConfig({ pins: [other.sha256, cert.sha256] }));
      expect(p.client.peerCertificateHash).toEqual(cert.sha256);
    });

    test('pinned client: a wrong hash is rejected', async () => {
      const server = await Server.start(mode);
      const other = selfSignedCertificate('localhost');
      await expectKind(
        provider.connect(server.endpoint, clientConfig({ pins: [other.sha256] })),
        'certificateRejected',
      );
    });

    test('platform verification rejects a self-signed certificate', async () => {
      const server = await Server.start(mode);
      await expectKind(
        provider.connect(server.endpoint, clientConfig({ verify: true })),
        'certificateRejected',
      );
    });

    test('IPv6 loopback', async (context) => {
      let listener: NativeQuicListener | undefined;
      try {
        listener = mode.wt
          ? await provider.listenWebTransport('::1', 0, serverConfig(), { path: '/sb' })
          : await provider.listen('::1', 0, serverConfig());
      } catch (error) {
        if (!(error instanceof QuicError)) throw error;
        context.skip(`no IPv6 loopback: ${error.message}`);
      }
      if (listener === undefined) return;
      addTearDown(() => listener.close());
      expect(listener.endpoint.startsWith(`${mode.scheme}://[::1]:`)).toBe(true);
      const accepted = next(iterate(listener.sessions));
      const client = await provider.connect(listener.endpoint, clientConfig());
      addTearDown(() => client.close(0, ''));
      const server = await accepted;
      addTearDown(() => server.close(0, ''));
      expect(client.remote).toBe(`[::1]:${listener.port}`);
      expect(server.remote.startsWith('[::1]:')).toBe(true);
    });

    test('exporter: equal on both ends, different across sessions', async () => {
      const server = await Server.start(mode);
      const p1 = await Pair.connect(server);
      const p2 = await Pair.connect(server);
      const ex = async (s: QuicSession, l = '') =>
        (await s.exportKeyingMaterial(l === '' ? label : l, new Uint8Array(0), 32)) as Uint8Array;
      const c1 = await ex(p1.client);
      const s1 = await ex(p1.server);
      const c2 = await ex(p2.client);
      const s2 = await ex(p2.server);
      expect(c1.length).toBe(32);
      expect(c1).toEqual(s1);
      expect(c2).toEqual(s2);
      expect(c1).not.toEqual(c2);
      expect(await ex(p1.client, 'other label')).not.toEqual(c1);
      const ctx = await p1.client.exportKeyingMaterial(label, Uint8Array.of(1, 2, 3), 32);
      expect(ctx).not.toEqual(c1);
      // More than the exporter can produce (255 hash lengths).
      expect(
        await failureOf(p1.client.exportKeyingMaterial(label, new Uint8Array(0), 1 << 16)),
      ).toBeInstanceOf(RangeError);
    });
  });

  describe('streams', () => {
    let p: Pair;
    beforeEach(async () => {
      const server = await Server.start(mode);
      p = await Pair.connect(server);
    });

    test('a second read while one is pending throws an Error', async () => {
      const cs = await p.client.openStream();
      // A write first: nothing of the stream is visible to the peer before it.
      await cs.write(bytes('hi'));
      // Nothing from the peer yet: the read stays pending, a second is rejected.
      const pending = cs.read(10);
      expect(() => cs.read(10)).toThrow('a read is pending');
      const ss = await next(p.serverStreams);
      await ss.write(bytes('x'));
      expect(await pending).toEqual(bytes('x'));
    });

    test('both directions with FIN; read returns undefined at FIN', async () => {
      const cs = await p.client.openStream();
      await cs.write(bytes('hello '));
      await cs.write(bytes('world'));
      await cs.finish();
      const ss = await next(p.serverStreams);
      expect(text(await readAll(ss))).toBe('hello world');
      expect(await ss.read(10)).toBeUndefined();
      await ss.write(bytes('back'));
      await ss.finish();
      expect(text(await readAll(cs))).toBe('back');
      expect(await cs.read(10)).toBeUndefined();
      // Finish, then stopped resolves with undefined once acknowledged.
      expect(await cs.stopped).toBeUndefined();
      expect(await ss.stopped).toBeUndefined();
      await expect(cs.write(bytes('late'))).rejects.toThrow('finished or reset');
    });

    test('finish is idempotent; write after finish rejects', async () => {
      const cs = await p.client.openStream();
      await cs.write(bytes('fin'));
      const f = cs.finish();
      // The same promise on a second call; writes are rejected at once, before the FIN has
      // reached the peer.
      expect(cs.finish()).toBe(f);
      await f;
      await expect(cs.write(bytes('late'))).rejects.toThrow('finished or reset');
      const ss = await next(p.serverStreams);
      expect(await readAll(ss)).toEqual(bytes('fin'));
    });

    test('a stream with nothing but FIN', async () => {
      const cs = await p.client.openStream();
      await cs.finish();
      const ss = await next(p.serverStreams);
      expect(await readAll(ss)).toEqual(new Uint8Array(0));
    });

    test('writes take views and empty arrays', async () => {
      const cs = await p.client.openStream();
      const data = pattern(100_000, 3);
      await cs.write(data.subarray(0, 10));
      await cs.write(data.slice(10, 50_000));
      await cs.write(data.subarray(50_000));
      await cs.write(new Uint8Array(0));
      await cs.finish();
      const ss = await next(p.serverStreams);
      expect(sameBytes(await readAll(ss), data)).toBe(true);
    });

    test('a stream the server opens', async () => {
      const ss = await p.server.openStream();
      await ss.write(bytes('push'));
      await ss.finish();
      const cs = await next(p.clientStreams);
      expect(text(await readAll(cs))).toBe('push');
      await cs.finish();
      expect(await readAll(ss)).toEqual(new Uint8Array(0));
    });

    test('reset is observed as QuicStreamReset with the code', async () => {
      const cs = await p.client.openStream();
      await cs.write(bytes('abc'));
      const ss = await next(p.serverStreams);
      cs.reset(9);
      expect(await cs.stopped).toBeUndefined();
      expect(await failureOf(readAll(ss))).toSatisfy(isReset(9));
      expect(await failureOf(ss.read(10))).toSatisfy(isReset(9));
      await expect(cs.write(bytes('x'))).rejects.toThrow('finished or reset');
    });

    test('a reset is observed by resetReceived while no read is pending, its unread data discarded', async () => {
      const cs = await p.client.openStream();
      await cs.write(pattern(5000));
      const ss = await next(p.serverStreams);
      // The reader takes one byte and pauses; the rest stays in the stack.
      expect(await ss.read(1)).toEqual(pattern(1));
      let reset = false;
      void ss.resetReceived.then(() => {
        reset = true;
      });
      await sleep(50);
      expect(reset).toBe(false);
      cs.reset(12);
      expect(await within(ss.resetReceived, 5000)).toBe(12);
      expect(await failureOf(ss.read(10))).toSatisfy(isReset(12));
    });

    test('a reset seen by a pending read resolves resetReceived too; FIN never does', async () => {
      const cs = await p.client.openStream();
      await cs.write(bytes('a'));
      const ss = await next(p.serverStreams);
      expect(await ss.read(10)).toEqual(bytes('a'));
      const read = ss.read(10);
      cs.reset(4);
      expect(await failureOf(read)).toSatisfy(isReset(4));
      expect(await ss.resetReceived).toBe(4);
      const fin = await p.client.openStream();
      await fin.write(bytes('done'));
      await fin.finish();
      const finished = await next(p.serverStreams);
      expect(await readAll(finished)).toEqual(bytes('done'));
      expect(await completesWithin(finished.resetReceived, 200)).toBe(false);
    });

    test('stop is observed as QuicStreamStopped, stopped resolves with the code', async () => {
      const cs = await p.client.openStream();
      await cs.write(bytes('x'));
      const ss = await next(p.serverStreams);
      expect(await ss.read(10)).toEqual(bytes('x'));
      const pending = ss.read(10);
      ss.stop(7);
      // The stopping side's read direction ends at once, with its own code.
      expect(await failureOf(pending)).toSatisfy(isReset(7));
      expect(await cs.stopped).toBe(7);
      expect(await failureOf(cs.write(bytes('y')))).toSatisfy(isStopped(7));
      expect(await failureOf(cs.finish())).toBeInstanceOf(QuicStreamStopped);
      // The other direction still works.
      await ss.write(bytes('z'));
      await ss.finish();
      expect(await readAll(cs)).toEqual(bytes('z'));
    });

    test("the opener stops at once: the acceptor's stopped has the code", async () => {
      // The stream and the STOP_SENDING arrive together; the stream is announced before its
      // events, so neither is dropped.
      for (let i = 0; i < 20; i++) {
        const cs = await p.client.openStream();
        await cs.write(bytes('x'));
        cs.stop(5);
        const ss = await next(p.serverStreams);
        expect(await within(ss.stopped, 5000)).toBe(5);
        expect(await failureOf(ss.write(bytes('y')))).toSatisfy(isStopped(5));
        expect(await ss.read(10)).toEqual(bytes('x'));
        cs.reset(0);
        expect(await failureOf(ss.read(10))).toSatisfy(isReset(0));
      }
    });

    test('a second stop is a no-op; reads keep failing with the code', async () => {
      const cs = await p.client.openStream();
      await cs.write(bytes('x'));
      const ss = await next(p.serverStreams);
      await ss.read(10);
      ss.stop(7);
      ss.stop(7);
      for (let i = 0; i < 2; i++) {
        expect(await failureOf(ss.read(10))).toSatisfy(isReset(7));
      }
    });

    test('codes beyond the range are argument errors', async () => {
      const max = mode.maxCode;
      const cs = await p.client.openStream();
      await cs.write(bytes('x'));
      const ss = await next(p.serverStreams);
      expect(() => cs.reset(max + 1)).toThrow(RangeError);
      expect(() => cs.reset(-1)).toThrow(RangeError);
      expect(() => cs.reset(1.5)).toThrow(RangeError);
      expect(() => ss.stop(max + 1)).toThrow(RangeError);
      expect(() => p.client.close(max + 1, '')).toThrow(RangeError);
      // Nothing was sent: the largest code still goes through afterwards.
      cs.reset(max);
      expect(await failureOf(readAll(ss))).toSatisfy(isReset(max));
    });

    test('a write waiting for credit fails when the peer stops', async () => {
      const cs = await p.client.openStream();
      const write = cs.write(pattern(4 << 20));
      const ss = await next(p.serverStreams);
      expect(await completesWithin(write, 200)).toBe(false);
      ss.stop(3);
      expect(await failureOf(write)).toSatisfy(isStopped(3));
    });

    test('backpressure: a 4 MiB write stalls until the peer reads', async () => {
      const data = pattern(4 << 20, 5);
      const cs = await p.client.openStream();
      const write = cs.write(data);
      const ss = await next(p.serverStreams);
      expect(await completesWithin(write, 300)).toBe(false);
      const reading = readAll(ss, 1 << 20);
      await write;
      await cs.finish();
      expect(sameBytes(await reading, data)).toBe(true);
    });

    test('pending operations fail when the session ends', async () => {
      const cs = await p.client.openStream();
      await cs.write(bytes('x'));
      const ss = await next(p.serverStreams);
      expect(await ss.read(1)).toEqual(bytes('x'));
      const read = ss.read(10);
      const stopped = ss.stopped;
      // Over WebTransport the closing side resets and stops the session's streams with
      // WT_SESSION_GONE, which may reach the peer before the close itself: a reset or
      // STOP_SENDING without an application code.
      const ended = (e: unknown) =>
        isSessionEnded(e) || (mode.wt && (isReset(undefined)(e) || isStopped(undefined)(e)));
      // Both outcomes are taken before closing; the peer may finish before the WebTransport
      // close has been acknowledged and close() resolves.
      const readFailure = failureOf(read);
      const stoppedFailure = failureOf(stopped);
      await p.client.close(5, 'gone');
      expect(await readFailure).toSatisfy(ended);
      expect(await stoppedFailure).toSatisfy(ended);
      // A reset without a code resolves resetReceived with undefined.
      const reset = await ss.resetReceived.then(
        (code) => code,
        (e: unknown) => e,
      );
      expect(mode.wt ? reset === undefined || isSessionEnded(reset) : isSessionEnded(reset)).toBe(
        true,
      );
      expect(await failureOf(cs.resetReceived)).toSatisfy(isSessionEnded);
      await p.server.done;
      expect(await failureOf(p.server.openStream())).toSatisfy(isSessionEnded);
      expect(await failureOf(ss.write(bytes('x')))).toSatisfy(isSessionEnded);
      expect(await failureOf(p.client.openStream())).toSatisfy(isSessionEnded);
    });
  });
}
