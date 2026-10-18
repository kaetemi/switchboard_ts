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

import { spawn } from 'node:child_process';
import { createSocket, type Socket } from 'node:dgram';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Worker } from 'node:worker_threads';
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
  type NativeWebTransportListener,
  nativeQuic,
  nativeQuicAvailability,
  quinnPriority,
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

const providerUrl = new URL('../../src/node/quic/native-provider.ts', import.meta.url).href;
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

async function collect<T>(iterable: AsyncIterable<T>): Promise<T[]> {
  const out: T[] = [];
  for await (const v of iterable) out.push(v);
  return out;
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

/**
 * A UDP relay to a listener on the IPv4 loopback, standing for a path the test controls: a port
 * forwarded by NAT, a slow link, a peer that is gone. Every datagram is passed on after [delay];
 * what the client sends is dropped while [dropFromClient] is set.
 */
class UdpRelay {
  readonly #socket: Socket;
  readonly #target: number;
  readonly #delay: number;
  readonly #timers = new Set<ReturnType<typeof setTimeout>>();
  #client: { address: string; port: number } | undefined;
  dropFromClient = false;

  private constructor(socket: Socket, target: number, delay: number) {
    this.#socket = socket;
    this.#target = target;
    this.#delay = delay;
    socket.on('message', (data, from) => this.#relay(data, from));
  }

  /** Binds 127.0.0.1 on any port and relays to [target]; closed after the test. */
  static async start(target: number, delay = 0): Promise<UdpRelay> {
    const socket = createSocket('udp4');
    await new Promise<void>((resolve) => socket.bind(0, '127.0.0.1', resolve));
    const relay = new UdpRelay(socket, target, delay);
    addTearDown(() => relay.close());
    return relay;
  }

  get port(): number {
    return this.#socket.address().port;
  }

  #relay(data: Buffer, from: { address: string; port: number }): void {
    let to: { address: string; port: number };
    if (from.port === this.#target && from.address === '127.0.0.1') {
      if (this.#client === undefined) return;
      to = this.#client;
    } else {
      this.#client = { address: from.address, port: from.port };
      if (this.dropFromClient) return;
      to = { address: '127.0.0.1', port: this.#target };
    }
    const send = () => this.#socket.send(data, to.port, to.address);
    if (this.#delay === 0) {
      send();
    } else {
      const timer = setTimeout(() => {
        this.#timers.delete(timer);
        send();
      }, this.#delay);
      this.#timers.add(timer);
    }
  }

  close(): void {
    for (const t of this.#timers) clearTimeout(t);
    this.#socket.close();
  }
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

/**
 * Runs [body] (the body of an async function of `m`, the provider module, and `data`) in a new
 * worker thread, which has to exit by itself afterwards; resolves with what it returned.
 */
function inWorker<T>(body: string, data: unknown): Promise<T> {
  const code = `
    const { parentPort, workerData } = require('node:worker_threads');
    (async () => {
      const m = await import(workerData.providerUrl);
      const data = workerData.data;
      ${body}
    })().then(
      (value) => parentPort.postMessage({ value }),
      (error) => parentPort.postMessage({ error: String(error && error.stack || error) }),
    );
  `;
  const worker = new Worker(code, { eval: true, workerData: { providerUrl, data } });
  return new Promise<T>((resolve, reject) => {
    let reply: { value?: T; error?: string } | undefined;
    worker.on('message', (m: { value?: T; error?: string }) => {
      reply = m;
    });
    worker.on('error', reject);
    worker.on('exit', (exitCode) => {
      if (reply?.error !== undefined) {
        reject(new Error(reply.error));
      } else if (exitCode !== 0 || reply === undefined) {
        reject(new Error(`the worker exited with ${exitCode} without a reply`));
      } else {
        resolve(reply.value as T);
      }
    });
  });
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
        expect((error as Error).message).toContain(`@kaetemi/switchboard-quic-${target}`);
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

    describe('raw QUIC only', () => {
      test('no unidirectional streams and no draining signal', async () => {
        const server = await Server.start(raw);
        const p = await Pair.connect(server);
        const serverUni = collect(p.server.incomingUnidirectionalStreams);
        const clientUni = collect(p.client.incomingUnidirectionalStreams);
        let draining = false;
        void p.server.draining.then(() => {
          draining = true;
        });
        void p.client.draining.then(() => {
          draining = true;
        });
        // drain() does nothing on raw QUIC.
        p.client.drain();
        // The binding's traffic goes on as usual meanwhile.
        const cs = await p.client.openStream();
        await cs.write(bytes('x'));
        await cs.finish();
        expect(await readAll(await next(p.serverStreams))).toEqual(bytes('x'));
        await p.client.close(0, '');
        await p.server.done;
        // Both are done with the session, having delivered nothing.
        expect(await within(clientUni, 5000)).toEqual([]);
        expect(await within(serverUni, 5000)).toEqual([]);
        await sleep(0);
        expect(draining).toBe(false);
      });
    });

    describe('WebTransport only', () => {
      test('the request: path, origin, protocols, draft', async () => {
        const server = await Server.start(webTransport);
        expect(server.endpoint).toBe(`https://127.0.0.1:${server.port}/sb`);
        const accepted = server.accept();
        const client = await provider.connectWebTransport(
          `https://127.0.0.1:${server.port}/sb?room=1#ignored`,
          clientConfig({ alpn: ['other', 'switchboard'] }),
          { origin: 'https://app.example' },
        );
        addTearDown(() => client.close(0, ''));
        const session = await accepted;
        expect(session.isWebTransport).toBe(true);
        expect(session.requestPath).toBe('/sb?room=1');
        expect(session.requestOrigin).toBe('https://app.example');
        expect(session.requestAuthority).toBe(`127.0.0.1:${server.port}`);
        expect(session.requestProtocols).toEqual(['other', 'switchboard']);
        // This provider's client sends the draft-02 header too.
        expect(session.requestDraft).toBe(2);
        // The first offered protocol the listener serves, in the client's order.
        expect(client.protocol).toBe('switchboard');
        expect(session.protocol).toBe('switchboard');
        expect(client.requestPath).toBeUndefined();
      });

      test('refusals: another path 404, an origin not allowed 403, a protocol not served 400', async () => {
        const server = await Server.start(webTransport, undefined, ['https://app.example']);
        const listener = server.listener as NativeWebTransportListener;
        const refused = async (
          path: string,
          status: number,
          o: { origin?: string; alpn?: readonly string[] } = {},
        ) => {
          const error = await expectKind(
            provider.connectWebTransport(
              `https://127.0.0.1:${server.port}${path}`,
              clientConfig({ alpn: o.alpn ?? [quicAlpn] }),
              { origin: o.origin },
            ),
            'refused',
          );
          expect(error.message).toContain(`${status}`);
        };
        await refused('/elsewhere', 404);
        await refused('/sb/', 404);
        await refused('/sb', 403, { origin: 'https://evil.example' });
        await refused('/sb', 400, { alpn: ['other'] });
        expect(listener.answered).toEqual(
          new Map([
            [404, 2],
            [403, 1],
            [400, 1],
          ]),
        );
        // An allowed origin, and no origin at all (not a browser), pass.
        for (const origin of ['https://app.example', undefined]) {
          const accepted = server.accept();
          const client = await provider.connectWebTransport(server.endpoint, clientConfig(), {
            origin,
          });
          await accepted;
          await client.close(0, '');
        }
        expect(listener.answered.get(200)).toBe(2);
      });

      test('partial stream headers arm writable notifications for writes and FIN', async () => {
        for (const window of [1, 2]) {
          const server = await Server.start(webTransport, serverConfig({ window }));
          const p = await Pair.connect(server, clientConfig({ window }));
          const empty = await p.client.openStream();
          await within(empty.finish(), 5000);
          expect(await within(readAll(await next(p.serverStreams)), 5000)).toEqual(
            new Uint8Array(0),
          );
          const received = next(p.serverStreams).then((s) => readAll(s));
          const stream = await p.client.openStream();
          await within(stream.write(bytes('data')), 5000);
          await stream.finish();
          expect(await received).toEqual(bytes('data'));
        }
      });

      test('the last session close survives context shutdown with a small CONNECT window', async () => {
        const listener = await provider.listenWebTransport(
          '127.0.0.1',
          0,
          serverConfig({ window: 768 }),
          {
            path: '/sb',
          },
        );
        addTearDown(() => listener.close());
        // Delay each UDP flight. The close exceeds one stream window and therefore cannot finish
        // before the provider is allowed to tear down the client's context. The relay has a
        // port of its own on 127.0.0.1.
        const relay = await UdpRelay.start(listener.port, 100);
        const incoming = next(iterate(listener.sessions));
        const endpoint = `https://127.0.0.1:${relay.port}/sb`;
        const client = inWorker<void>(
          `
          const session = await m.nativeQuic().connect(data.endpoint, {
            alpn: ['switchboard'],
            verifyCertificate: false,
          });
          await session.close(9, 'x'.repeat(1024));
          // Give the context's cleanup a turn before the worker ends.
          await new Promise((resolve) => setTimeout(resolve, 100));
          `,
          { endpoint },
        );
        const session = await incoming;
        await within(client, 10_000);
        const end = await within(session.done, 5000);
        expect([end.lost, end.code, end.reason]).toEqual([false, 9, 'x'.repeat(1024)]);
      });

      test('binary protocol lengths 127, 128 and 255 survive the ABI', async () => {
        for (const length of [127, 128, 255]) {
          const protocol = 'p'.repeat(length);
          const server = await Server.start(webTransport, undefined, undefined, [protocol]);
          const p = await Pair.connect(server, clientConfig({ alpn: [protocol] }));
          expect(p.client.protocol).toBe(protocol);
          expect(p.server.requestProtocols).toEqual([protocol]);
        }
      });

      test('userinfo and out-of-range ports are refused before dialing', async () => {
        // Userinfo and a port the URL cannot carry make a malformed endpoint, port 0 an
        // impossible one.
        expect(
          await failureOf(provider.connect('https://user@localhost/sb', clientConfig())),
        ).toBeInstanceOf(TypeError);
        expect(
          await failureOf(provider.connect('https://localhost:65536/sb', clientConfig())),
        ).toBeInstanceOf(TypeError);
        expect(
          await failureOf(provider.connect('https://localhost:0/sb', clientConfig())),
        ).toBeInstanceOf(RangeError);
      });

      test('the authority is not checked: other names and ports reach the listener, which routes by path', async () => {
        // Advertised under a name the client does not use, and reached by another name and, as
        // through a forwarded port, at another port.
        const listener = await provider.listenWebTransport('127.0.0.1', 0, serverConfig(), {
          path: '/sb',
          advertisedHost: 'node.invalid',
        });
        addTearDown(() => listener.close());
        expect(new URL(listener.endpoint).hostname).toBe('node.invalid');
        const incoming = iterate(listener.sessions);
        const forwarded = await UdpRelay.start(listener.port);
        for (const [host, port] of [
          ['127.0.0.1', listener.port],
          ['localhost', listener.port],
          ['127.0.0.1', forwarded.port],
        ] as const) {
          const client = await provider.connect(
            `https://${host}:${port}/sb?node=1`,
            clientConfig(),
          );
          const server = await next(incoming);
          expect(server.requestAuthority).toBe(`${host}:${port}`);
          expect(server.requestPath).toBe('/sb?node=1');
          await client.close(0, '');
          await server.done;
        }
        const error = await failureOf(
          provider.connect(`https://127.0.0.1:${forwarded.port}/elsewhere`, clientConfig()),
        );
        expect((error as Error).message).toContain('404');
        expect(listener.answered).toEqual(
          new Map([
            [200, 3],
            [404, 1],
          ]),
        );
      });

      test('allowed origins match the exact string', async () => {
        const server = await Server.start(webTransport, undefined, ['https://app.example']);
        for (const origin of [
          'https://app.example/',
          'https://APP.example',
          'https://app.example:443',
          'http://app.example',
          'https://app.example:444',
        ]) {
          const error = await failureOf(
            provider.connectWebTransport(server.endpoint, clientConfig(), { origin }),
          );
          expect((error as Error).message).toContain('403');
        }
      });

      test('no protocol offered: accepted without one', async () => {
        const server = await Server.start(webTransport);
        const p = await Pair.connect(server, clientConfig({ alpn: [] }));
        expect(p.client.protocol).toBeUndefined();
        expect(p.server.protocol).toBeUndefined();
        expect(p.server.requestProtocols).toEqual([]);
      });

      test('draining both ways, and the session goes on', async () => {
        const server = await Server.start(webTransport);
        const p = await Pair.connect(server);
        let serverDraining = false;
        void p.server.draining.then(() => {
          serverDraining = true;
        });
        p.client.drain();
        await within(p.server.draining, 5000);
        expect(serverDraining).toBe(true);
        p.server.drain();
        await within(p.client.draining, 5000);
        const cs = await p.client.openStream();
        await cs.write(bytes('still'));
        await cs.finish();
        expect(await readAll(await next(p.serverStreams))).toEqual(bytes('still'));
      });

      test('codes beyond a u32 are argument errors; the largest goes through, and close codes are WebTransport codes', async () => {
        const server = await Server.start(webTransport);
        const p = await Pair.connect(server);
        const cs = await p.client.openStream();
        await cs.write(bytes('x'));
        const ss = await next(p.serverStreams);
        expect(() => cs.reset(0x100000000)).toThrow(RangeError);
        expect(() => ss.stop(0x100000000)).toThrow(RangeError);
        expect(() => p.client.close(0x100000000, '')).toThrow(RangeError);
        cs.reset(0xffffffff);
        expect(await failureOf(readAll(ss))).toSatisfy(isReset(0xffffffff));
        await p.server.close(0xffffffff, 'the largest');
        const end = await p.client.done;
        expect([end.code, end.reason, end.byPeer]).toEqual([0xffffffff, 'the largest', true]);
      });

      test('the closed listener accepts no new session', async () => {
        const server = await Server.start(webTransport);
        const p = await Pair.connect(server);
        await server.listener.close();
        expect(
          await failureOf(provider.connect(server.endpoint, clientConfig({ handshake: 2000 }))),
        ).toBeInstanceOf(QuicError);
        // The open session is left to its owners.
        const cs = await p.client.openStream();
        await cs.write(bytes('open'));
        await cs.finish();
        expect(await readAll(await next(p.serverStreams))).toEqual(bytes('open'));
      });
    });

    describe('JavaScript', () => {
      test('a write copies what the stack has not taken yet: the buffer may be reused at once', async () => {
        const server = await Server.start(raw);
        const p = await Pair.connect(server);
        const cs = await p.client.openStream();
        // Larger than the stream window: most of it waits for credit.
        const data = pattern(4 << 20, 9);
        const buffer = data.slice();
        const write = cs.write(buffer);
        buffer.fill(0);
        const reading = readAll(await next(p.serverStreams), 1 << 20);
        await write;
        await cs.finish();
        expect(sameBytes(await reading, data)).toBe(true);
      });

      test('writes take subarrays and Node buffers', async () => {
        const server = await Server.start(raw);
        const p = await Pair.connect(server);
        const cs = await p.client.openStream();
        const data = pattern(100_000, 3);
        const backing = new Uint8Array(data.length + 10);
        backing.set(data, 10);
        await cs.write(backing.subarray(10, 20));
        await cs.write(Buffer.from(data.subarray(10, 50_000)));
        await cs.write(backing.subarray(50_010));
        await cs.finish();
        expect(sameBytes(await readAll(await next(p.serverStreams)), data)).toBe(true);
      });

      test('a raw QUIC code up to 2^53 - 1 is carried exactly both ways', async () => {
        const server = await Server.start(raw);
        const p = await Pair.connect(server);
        for (const code of [2 ** 53 - 17, 2 ** 53 - 16, 2 ** 53 - 1]) {
          const cs = await p.client.openStream();
          await cs.write(bytes('x'));
          const ss = await next(p.serverStreams);
          expect(await ss.read(10)).toEqual(bytes('x'));
          ss.stop(code);
          expect(await within(cs.stopped, 5000)).toBe(code);
          const other = await p.server.openStream();
          await other.write(bytes('y'));
          other.reset(code);
          const peer = await next(p.clientStreams);
          expect(await within(peer.resetReceived, 5000)).toBe(code);
        }
      });

      test('the process exits on its own once its sessions and listeners are closed, and stays alive while a listener is open', async () => {
        const script = (keepListening: boolean) => `
          const m = await import(${JSON.stringify(providerUrl)});
          const provider = m.nativeQuic();
          const cert = m.selfSignedCertificate('localhost');
          const config = { alpn: ['switchboard'], certificateDer: cert.certificateDer, privateKeyDer: cert.privateKeyDer };
          for (const wt of [false, true]) {
            const listener = wt
              ? await provider.listenWebTransport('127.0.0.1', 0, config, { path: '/sb' })
              : await provider.listen('127.0.0.1', 0, config);
            const accepted = listener.sessions[Symbol.asyncIterator]().next();
            const client = await provider.connect(listener.endpoint, { alpn: ['switchboard'], verifyCertificate: false });
            const peer = (await accepted).value;
            const s = await client.openStream();
            await s.write(new Uint8Array([1, 2, 3]));
            await s.finish();
            const t = (await peer.incomingStreams[Symbol.asyncIterator]().next()).value;
            while ((await t.read(10)) !== undefined) {}
            await client.close(0, 'done');
            await peer.done;
            if (!${keepListening} || wt) await listener.close();
          }
          console.log(${keepListening} ? 'listening' : 'closed');
        `;
        // The child runs from a module file: a runtime that takes `-e` code for a script (Deno)
        // would not accept its top-level `await`.
        const directory = await mkdtemp(join(tmpdir(), 'switchboard-exit-'));
        addTearDown(() => rm(directory, { recursive: true, force: true }));
        const run = async (keepListening: boolean) => {
          const file = join(directory, `child-${keepListening ? 'listening' : 'closed'}.mjs`);
          await writeFile(file, script(keepListening));
          const child = spawn(process.execPath, ['--no-warnings', file], {
            stdio: ['ignore', 'pipe', 'pipe'],
          });
          let out = '';
          child.stdout.on('data', (d: Buffer) => {
            out += d.toString();
          });
          child.stderr.on('data', (d: Buffer) => {
            out += d.toString();
          });
          const exited = new Promise<number | null>((resolve) => child.on('exit', resolve));
          return { child, exited, output: () => out };
        };
        const closing = await run(false);
        expect(await within(closing.exited, 20_000)).toBe(0);
        expect(closing.output()).toContain('closed');
        const listening = await run(true);
        addTearDown(() => listening.child.kill());
        for (let i = 0; i < 200 && !listening.output().includes('listening'); i++) {
          if (listening.child.exitCode !== null) break;
          await sleep(50);
        }
        expect(listening.output()).toContain('listening');
        expect(await completesWithin(listening.exited, 1500)).toBe(false);
        expect(listening.child.exitCode).toBeNull();
      }, 60_000);
    });
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

  describe('priorities', () => {
    // The contract's classes on quinn's strict levels: 2 (control) strictly first, 1 (ordinary)
    // and 0 (bulk) on one level, interleaved.
    test("the classes on quinn's levels", () => {
      expect(quinnPriority(1)).toBe(quinnPriority(0));
      expect(quinnPriority(2)).toBeGreaterThan(quinnPriority(1));
      expect(quinnPriority(3)).toBeGreaterThan(quinnPriority(2));
      expect(quinnPriority(-1)).toBeLessThan(quinnPriority(0));
      expect(quinnPriority(2 ** 40)).toBe(0x7fffffff);
    });

    // How far ahead of a stream of class [first] one of class [second] finishes, as a fraction of
    // the transfer, with [first] handed to the stack first and [second] right after, in the same
    // turn. Windows hold everything, so the server's stack takes (and acknowledges) all data
    // without its application reading: the order in which the two finishes are acknowledged is
    // the order the sender transmitted them in. Round robin finishes both within a packet or two
    // of each other; strict priority sends the second one first, and the first one's last bytes
    // go out well later.
    async function lead(server: Server, first: number, second: number): Promise<number> {
      const p = await Pair.connect(server);
      const a = await p.client.openStream();
      const b = await p.client.openStream();
      a.priority = first;
      b.priority = second;
      const data = pattern(4 << 20);
      const started = performance.now();
      let ackedA = 0;
      let ackedB = 0;
      const done = Promise.all([
        a.stopped.then(() => {
          ackedA = performance.now() - started;
        }),
        b.stopped.then(() => {
          ackedB = performance.now() - started;
        }),
      ]);
      await Promise.all([
        a.write(data).then(() => a.finish()),
        b.write(data).then(() => b.finish()),
      ]);
      await within(done, 20_000);
      for (let i = 0; i < 2; i++) {
        expect(sameBytes(await readAll(await next(p.serverStreams), 1 << 20), data)).toBe(true);
      }
      await p.client.close(0, '');
      return (ackedA - ackedB) / Math.max(ackedA, ackedB);
    }

    // A measurement that loses a packet at the very end waits for a probe timeout and says
    // nothing about the scheduler: up to three tries.
    async function measure(
      first: number,
      second: number,
      ok: (v: number) => boolean,
    ): Promise<number> {
      const server = await Server.start(
        mode,
        serverConfig({ window: 8 << 20, connectionWindow: 32 << 20 }),
      );
      let v = 0;
      for (let i = 0; i < 3; i++) {
        v = await lead(server, first, second);
        console.info(
          `${mode.label}: priority class ${second} ahead of class ${first} by ${(v * 100).toFixed(1)}% of the transfer`,
        );
        if (ok(v)) break;
      }
      return v;
    }

    test('control goes strictly before bulk', async () => {
      const ok = (v: number) => v > 0.1;
      const v = await measure(0, 2, ok);
      expect(ok(v), `control ahead by ${v} of the transfer`).toBe(true);
    });

    test('ordinary and bulk interleave', async () => {
      const ok = (v: number) => Math.abs(v) < 0.1;
      const v = await measure(0, 1, ok);
      expect(ok(v), `ordinary ahead by ${v} of the transfer`).toBe(true);
    });
  });

  test('stream limit: openStream waits until a stream closes', async () => {
    const server = await Server.start(mode, serverConfig({ maxStreams: 2 }));
    const p = await Pair.connect(server);
    const s1 = await p.client.openStream();
    const s2 = await p.client.openStream();
    await s1.write(bytes('1'));
    await s2.write(bytes('2'));
    const third = p.client.openStream();
    expect(await completesWithin(third, 300)).toBe(false);
    // End the first stream in both directions: the peer grants a new one. (The streams of a
    // WebTransport session may arrive out of their order: each one's header is read on its own.)
    await s1.finish();
    const a = await next(p.serverStreams);
    const b = await next(p.serverStreams);
    const firstOfA = await a.read(1);
    const r1 = firstOfA?.[0] === 0x31 ? a : b;
    if (r1 === b) expect(await b.read(1)).toEqual(bytes('1'));
    expect(await readAll(r1)).toEqual(new Uint8Array(0));
    await r1.finish();
    const s3 = await within(third, 5000);
    await s3.write(bytes('3'));
    // Raising the limit at runtime lets a waiting open through as well.
    const fourth = p.client.openStream();
    expect(await completesWithin(fourth, 200)).toBe(false);
    p.server.setMaxIncomingStreams(3);
    expect(p.server.maxIncomingStreams).toBe(3);
    await within(fourth, 5000);
  });

  describe('session end', () => {
    test('close with code and reason is seen by the peer', async () => {
      const server = await Server.start(mode);
      const p = await Pair.connect(server);
      const reason = `bad record ${'é'.repeat(600)}`;
      await p.client.close(42, reason);
      const mine = await p.client.done;
      const theirs = await p.server.done;
      expect(mine.code).toBe(42);
      expect(mine.byPeer).toBe(false);
      expect(mine.lost).toBe(false);
      expect(theirs.code).toBe(42);
      expect(theirs.byPeer).toBe(true);
      expect(theirs.lost).toBe(false);
      // Cut to 1024 bytes at a character boundary, possibly more by QUIC.
      expect(reason.startsWith(theirs.reason)).toBe(true);
      expect(theirs.reason.startsWith('bad record é')).toBe(true);
      expect(new TextEncoder().encode(theirs.reason).length).toBeLessThanOrEqual(1024);
      // Idempotent.
      await p.client.close(1, 'again');
    });

    test('a close completes when the peer is gone: at once on raw QUIC, after a bounded wait for its acknowledgement over WebTransport', async () => {
      const server = await Server.start(mode);
      const relay = await UdpRelay.start(server.port);
      const accepted = server.accept();
      const url = new URL(server.endpoint);
      url.port = String(relay.port);
      const client = await provider.connect(
        mode.wt ? url.href : `quic://127.0.0.1:${relay.port}`,
        clientConfig(),
      );
      await accepted;
      // Nothing the client sends arrives any more, its close included.
      relay.dropFromClient = true;
      const started = performance.now();
      await within(client.close(9, 'gone'), 5000);
      const waited = performance.now() - started;
      const end = await client.done;
      expect([end.code, end.byPeer, end.lost]).toEqual([9, false, false]);
      if (mode.wt) {
        // No acknowledgement can come: three round trips, at least 0.2 s, at most 2 s.
        expect(waited).toBeGreaterThan(150);
        expect(waited).toBeLessThan(3000);
      } else {
        expect(waited).toBeLessThan(500);
      }
    });

    test('the server closes', async () => {
      const server = await Server.start(mode);
      const p = await Pair.connect(server);
      await p.server.close(33, 'going away');
      const end = await p.client.done;
      expect([end.code, end.reason, end.byPeer]).toEqual([33, 'going away', true]);
    });

    test('lost on idle timeout', async () => {
      const idle = 300;
      const server = await Server.start(mode, serverConfig({ idle, keepAlive: 0 }));
      const p = await Pair.connect(server, clientConfig({ idle, keepAlive: 0 }));
      // quinn waits at least three probe timeouts, which a handshake on a loaded runner can
      // stretch to seconds: the bound is generous.
      const client = await within(p.client.done, 20_000);
      const peer = await within(p.server.done, 20_000);
      expect(client.lost).toBe(true);
      expect(client.code).toBeUndefined();
      expect(peer.lost).toBe(true);
    });

    test('the exporter fails once the session has ended', async () => {
      const server = await Server.start(mode);
      const p = await Pair.connect(server);
      await p.client.close(0, '');
      await p.client.done;
      await expectKind(p.client.exportKeyingMaterial(label, new Uint8Array(0), 32), 'sessionEnded');
    });

    test('keep-alive holds an idle connection open', async () => {
      const idle = 400;
      const keepAlive = 100;
      const server = await Server.start(mode, serverConfig({ idle, keepAlive }));
      const p = await Pair.connect(server, clientConfig({ idle, keepAlive }));
      expect(await completesWithin(p.client.done, 1000)).toBe(false);
      await p.client.close(0, '');
    });
  });

  test('256 streams concurrently', async () => {
    const server = await Server.start(mode, serverConfig({ maxStreams: 300 }));
    const p = await Pair.connect(server);
    const echo = (async () => {
      const tasks: Promise<void>[] = [];
      for (let i = 0; i < 256; i++) {
        const s = await next(p.serverStreams);
        tasks.push(
          (async () => {
            const data = await readAll(s);
            await s.write(data);
            await s.finish();
          })(),
        );
      }
      await Promise.all(tasks);
    })();
    const results = await Promise.all(
      Array.from({ length: 256 }, async (_, i) => {
        const s = await p.client.openStream();
        const data = pattern(16 << 10, i);
        await s.write(data);
        await s.finish();
        const back = await readAll(s);
        return back.length === data.length && back[1] === data[1];
      }),
    );
    await echo;
    expect(results.every((ok) => ok)).toBe(true);
  }, 60_000);

  test('64 MiB through one stream', async () => {
    const server = await Server.start(mode);
    const p = await Pair.connect(server);
    const total = 64 << 20;
    const chunk = pattern(256 << 10);
    const started = performance.now();
    const tx = await p.client.openStream();
    const writer = (async () => {
      for (let sent = 0; sent < total; sent += chunk.length) await tx.write(chunk);
      await tx.finish();
    })();
    const rx = await next(p.serverStreams);
    let received = 0;
    for (let c = await rx.read(1 << 20); c !== undefined; c = await rx.read(1 << 20)) {
      received += c.length;
    }
    await writer;
    const ms = performance.now() - started;
    expect(received).toBe(total);
    console.info(
      `native provider throughput (${mode.label}): 64 MiB in ${ms.toFixed(0)} ms = ${(total / ms / 1000).toFixed(0)} MB/s (one stream, 1 MiB window)`,
    );
  }, 60_000);

  test('events while JavaScript is inside shim calls do not deadlock: 200 iterations of connect, open, write, read, close', async () => {
    const server = await Server.start(mode);
    const data = pattern(64 << 10);
    for (let i = 0; i < 200; i++) {
      const accepted = server.accept();
      const client = await provider.connect(server.endpoint, clientConfig());
      const peer = await accepted;
      const echo = next(iterate(peer.incomingStreams)).then(async (s) => {
        const got = await readAll(s);
        await s.write(got);
        await s.finish();
      });
      const s = await client.openStream();
      await s.write(data);
      await s.finish();
      const back = await readAll(s);
      expect(back.length).toBe(data.length);
      await echo;
      await client.close(0, `iteration ${i}`);
      await peer.done;
    }
  }, 120_000);

  test('another worker thread has a context of its own, and exits when done', async () => {
    const server = await Server.start(mode);
    const echoed = (async () => {
      const session = await server.accept();
      const s = await next(iterate(session.incomingStreams));
      const got = await readAll(s);
      await s.write(got);
      await s.finish();
      return session.done;
    })();
    const reply = await within(
      inWorker<string>(
        `
        const client = await m.nativeQuic().connect(data.endpoint, {
          alpn: ['switchboard'],
          verifyCertificate: false,
        });
        const s = await client.openStream();
        await s.write(new TextEncoder().encode('from another worker'));
        await s.finish();
        const back = [];
        for (let c = await s.read(65536); c !== undefined; c = await s.read(65536)) back.push(...c);
        await client.close(9, 'bye');
        return new TextDecoder().decode(Uint8Array.from(back));
        `,
        { endpoint: server.endpoint },
      ),
      20_000,
    );
    expect(reply).toBe('from another worker');
    const end = await within(echoed, 5000);
    expect([end.code, end.byPeer]).toEqual([9, true]);
  });

  test('shutdown ends every session of the environment', async () => {
    const server = await Server.start(mode);
    const p = await Pair.connect(server);
    const cs = await p.client.openStream();
    const read = failureOf(cs.read(10));
    NativeQuicProvider.shutdown();
    const end = await p.client.done;
    expect(end.byPeer).toBe(false);
    expect(end.reason).toBe('shutdown');
    expect(await read).toBeInstanceOf(QuicError);
    // The provider starts again on next use.
    const again = await Server.start(mode);
    const q = await Pair.connect(again);
    expect(q.client.protocol).toBe(quicAlpn);
  });
}
