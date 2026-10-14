// The server side of the browser tests, a port of `switchboard_quic`'s
// `tool/web_server.dart`, run in the test's own Node process: native
// WebTransport listeners on 127.0.0.1 with a fresh self-signed certificate
// (ECDSA P-256, valid 14 days, what a browser accepts for
// `serverCertificateHashes`), any origin allowed, the protocol `switchboard`:
//
// * /provider: sessions for the provider tests, each behaving as its query
//   says (`?case=echo&id=...`; see `serve`) and recording what the server saw
//   under its id. The page reads the records through a function the harness
//   exposes (Dart answered them over a `?case=report` session);
// * /limited: the same, on a listener that grants two streams at once;
// * /sb: a node's listener (QuicIdentityRules.none, so that the nonce-bound
//   IDENTs of a browser without exportKeyingMaterial are accepted) with Talk
//   services: `echo`, `control` (TAG, WHOAMI, GOAWAY, CLOSE) and `issue` (a
//   credential for a holder key, so that a browser can identify). Dart runs a
//   Switchboard node behind QuicSwitchboardListener; here the connections of
//   listenQuic are served directly, with Talk's bulk routing installed on
//   each, until the node's Talk layer is wired (`talk-layer.ts`);
// * a UDP port that never answers, for a handshake that times out.
//
// Credentials are the stand-ins of `test/unit/quic/identity-fixtures.ts`
// (the core has no credential codec or issuer yet): `issue` answers the
// encoding of a fake credential for the holder key it is sent, and the
// listener's verifier accepts exactly those.
import { createSocket } from 'node:dgram';
import { ChannelAddress } from '../../src/core/address/index.ts';
import { hexString } from '../../src/core/bytes.ts';
import type { MuxChannel } from '../../src/core/mux/contract.ts';
import { Name } from '../../src/core/name.ts';
import {
  QuicError,
  type QuicServerConfig,
  type QuicStream,
  QuicStreamReset,
  QuicStreamStopped,
  switchboardAlpn,
} from '../../src/core/quic/contract.ts';
import type { QuicBindingConnection } from '../../src/core/quic/quic-binding-connection.ts';
import { QuicIdentityRules } from '../../src/core/quic/quic-identity-rules.ts';
import { listenQuic } from '../../src/core/quic/quic-listen.ts';
import { Status, StatusCode } from '../../src/core/status.ts';
import type { TalkMessage } from '../../src/core/talk/contract.ts';
import { installBulkRouting, TalkChannel } from '../../src/core/talk/index.ts';
import {
  type NativeQuicSession,
  nativeQuic,
  selfSignedCertificate,
} from '../../src/node/quic/index.ts';
import { FakeVerifier, fakeCredential } from '../unit/quic/identity-fixtures.ts';

/** One thing the server saw, as JSON. */
export type ServerRecord = Readonly<Record<string, unknown>>;

/** What the page needs to know of the server: its endpoints and certificate. */
export interface ServerDescription {
  /** The provider tests' listener (`?case=...&id=...`). */
  readonly provider: string;
  /** The same, granting two streams at once. */
  readonly limited: string;
  /** The node's WebTransport listener. */
  readonly node: string;
  /** A UDP port that never answers. */
  readonly silent: string;
  /** The SHA-256 of the server's certificate, hex. */
  readonly certificateSha256: string;
  /** The handshake deadline of the server's listeners, in ms. */
  readonly handshakeTimeoutMs: number;
}

/** The running server. */
export interface WebServer {
  readonly description: ServerDescription;
  /** What the server recorded under [id]. */
  records(id: string): readonly ServerRecord[];
  close(): Promise<void>;
}

/** The listeners' handshake deadline, which the tests outlive. */
const handshakeTimeout = 2000;

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** [n] bytes of a pattern the browser checks. */
function pattern(n: number): Uint8Array {
  const out = new Uint8Array(n);
  for (let i = 0; i < n; i++) out[i] = (i * 31 + (i >> 8)) & 0xff;
  return out;
}

async function readAll(stream: QuicStream): Promise<Uint8Array> {
  const chunks: Uint8Array[] = [];
  let length = 0;
  for (;;) {
    const chunk = await stream.read(65536);
    if (chunk === undefined) break;
    chunks.push(chunk);
    length += chunk.length;
  }
  const out = new Uint8Array(length);
  let at = 0;
  for (const c of chunks) {
    out.set(c, at);
    at += c.length;
  }
  return out;
}

async function readExactly(stream: QuicStream, n: number): Promise<Uint8Array> {
  const out = new Uint8Array(n);
  let got = 0;
  while (got < n) {
    const chunk = await stream.read(n - got);
    if (chunk === undefined) throw new Error(`FIN after ${got} bytes`);
    out.set(chunk, got);
    got += chunk.length;
  }
  return out;
}

const hex = (bytes: Uint8Array): string => hexString(bytes).replaceAll(' ', '');

/** Starts the listeners and the node; resolves once they are bound. */
export async function startWebServer(): Promise<WebServer> {
  const records = new Map<string, ServerRecord[]>();
  const record = (id: string, event: ServerRecord): void => {
    let list = records.get(id);
    if (list === undefined) {
      list = [];
      records.set(id, list);
    }
    list.push(event);
  };

  const provider = nativeQuic();
  const cert = selfSignedCertificate('127.0.0.1');
  // A short handshake deadline (it also bounds the HTTP/3 exchanges), so that the tests show a
  // quiet session outliving it.
  const config = (streams = 129): QuicServerConfig => ({
    alpn: [switchboardAlpn],
    certificateDer: cert.certificateDer,
    privateKeyDer: cert.privateKeyDer,
    maxStreamsBidi: streams,
    handshakeTimeout,
  });

  const raw = await provider.listenWebTransport('127.0.0.1', 0, config(), { path: '/provider' });
  const limited = await provider.listenWebTransport('127.0.0.1', 0, config(2), {
    path: '/limited',
  });
  for (const listener of [raw, limited]) {
    void (async () => {
      for await (const session of listener.sessions) void serve(session, record);
    })().catch(() => {});
  }

  const sb = await provider.listenWebTransport('127.0.0.1', 0, config(), { path: '/sb' });
  const node = listenQuic(sb, {
    identityRules: QuicIdentityRules.none,
    options: { identityVerifier: new FakeVerifier() },
  });
  const connections = new Set<QuicBindingConnection>();
  const services = nodeServices(record);
  void (async () => {
    for await (const connection of node.connections) {
      connections.add(connection);
      void connection.done.then(() => connections.delete(connection));
      void dispatch(connection, services).catch(() => {});
    }
  })().catch(() => {});

  const silent = createSocket('udp4');
  await new Promise<void>((resolve, reject) => {
    silent.once('error', reject);
    silent.bind(0, '127.0.0.1', () => resolve());
  });
  silent.on('message', () => {});

  const description: ServerDescription = Object.freeze({
    provider: `https://127.0.0.1:${raw.port}/provider`,
    limited: `https://127.0.0.1:${limited.port}/limited`,
    node: `https://127.0.0.1:${sb.port}/sb`,
    silent: `https://127.0.0.1:${silent.address().port}/sb`,
    certificateSha256: hex(cert.sha256),
    handshakeTimeoutMs: handshakeTimeout,
  });

  return {
    description,
    records: (id) => records.get(id) ?? [],
    async close() {
      silent.close();
      await node.close();
      await Promise.allSettled([...connections].map((c) => c.close()));
      await Promise.allSettled([raw.close(), limited.close()]);
    },
  };
}

// --- /provider and /limited ----------------------------------------------------------------------

/** Records [event] under [id]. */
type Recorder = (id: string, event: ServerRecord) => void;

/** One session of the provider tests, as its query's `case` says, recording under its `id`. */
async function serve(session: NativeQuicSession, record: Recorder): Promise<void> {
  const query = new URL(session.requestPath ?? '/', 'https://server').searchParams;
  const kind = query.get('case') ?? '';
  const id = query.get('id') ?? '';
  record(id, {
    event: 'session',
    case: kind,
    protocol: session.protocol ?? null,
    origin: session.requestOrigin ?? null,
  });
  let exporter: Uint8Array | undefined;
  try {
    exporter = await session.exportKeyingMaterial('switchboard-ident', new Uint8Array(0), 32);
  } catch {
    exporter = undefined;
  }
  record(id, { event: 'exporter', value: exporter === undefined ? null : hex(exporter) });
  void session.done.then((end) => {
    record(id, {
      event: 'end',
      code: end.code ?? null,
      reason: end.reason,
      byPeer: end.byPeer,
      lost: end.lost,
    });
  });
  if (kind === 'open') {
    try {
      const s = await session.openStream();
      await s.write(encoder.encode('from server'));
      await s.finish();
      record(id, { event: 'opened' });
    } catch (error) {
      record(id, { event: 'open failed', error: String(error) });
    }
  }
  let index = 0;
  try {
    for await (const stream of session.incomingStreams) {
      void serveStream(session, stream, kind, id, index++, record);
    }
  } catch {
    // The session ended.
  }
}

async function serveStream(
  session: NativeQuicSession,
  stream: QuicStream,
  kind: string,
  id: string,
  i: number,
  record: Recorder,
): Promise<void> {
  record(id, { event: 'stream', stream: i });
  stream.resetReceived.then(
    (code) => record(id, { event: 'reset', stream: i, code: code ?? null }),
    () => {},
  );
  stream.stopped.then(
    (code) =>
      record(id, {
        event: code === undefined ? 'acknowledged' : 'stopped',
        stream: i,
        code: code ?? null,
      }),
    (error: unknown) => {
      if (error instanceof QuicStreamStopped) {
        record(id, { event: 'stopped', stream: i, code: error.code ?? null });
      }
    },
  );
  try {
    switch (kind) {
      case 'echo': {
        const bytes = await readAll(stream);
        record(id, { event: 'fin', stream: i, bytes: bytes.length });
        await stream.write(bytes);
        await stream.finish();
        break;
      }
      case 'sink': {
        const bytes = await readAll(stream);
        record(id, { event: 'fin', stream: i, bytes: bytes.length });
        break;
      }
      case 'send': {
        const header = await readExactly(stream, 4);
        const size = new DataView(header.buffer, header.byteOffset, 4).getUint32(0);
        await stream.write(pattern(size));
        await stream.finish();
        record(id, { event: 'sent', stream: i, bytes: size });
        break;
      }
      case 'reset':
        await stream.read(65536);
        await stream.write(encoder.encode('partial'));
        await new Promise((resolve) => setTimeout(resolve, 100));
        stream.reset(14);
        record(id, { event: 'reset sent', stream: i });
        break;
      case 'stop':
        await stream.read(65536);
        stream.stop(9);
        record(id, { event: 'stop sent', stream: i });
        break;
      case 'close':
        await stream.read(65536);
        await session.close(9, 'server bye');
        break;
      case 'drain':
        await stream.read(65536);
        session.drain();
        record(id, { event: 'drain sent' });
        break;
      case 'hold':
        break;
    }
  } catch (error) {
    if (error instanceof QuicStreamReset) {
      record(id, { event: 'read reset', stream: i, code: error.code ?? null });
    } else if (error instanceof QuicStreamStopped) {
      record(id, { event: 'write stopped', stream: i, code: error.code ?? null });
    } else if (error instanceof QuicError) {
      record(id, { event: 'stream failed', stream: i, error: error.kind });
    }
  }
}

// --- /sb -----------------------------------------------------------------------------------------

type Service = (talk: TalkChannel, connection: QuicBindingConnection) => void;

/** Serves [talk]'s messages with [handler], until the channel ends. */
function messages(talk: TalkChannel, handler: (message: TalkMessage) => unknown): void {
  void (async () => {
    for await (const message of talk.messages) {
      try {
        await handler(message);
      } catch {
        // The channel or its connection ended.
      }
    }
  })().catch(() => {});
}

/** The node's Talk services, recording through [record]. */
function nodeServices(record: Recorder): Map<string, Service> {
  const control = (m: TalkMessage, connection: QuicBindingConnection): unknown => {
    const text = decoder.decode(m.payload);
    switch (m.procedureName) {
      case 'TAG':
        record(text, { event: 'tagged', remote: connection.remote });
        void connection.done.then((status) => {
          record(text, { event: 'connection done', code: status.code, reason: status.reason });
        });
        void connection.peerGoAwayStatus.then(
          (status) => record(text, { event: 'peer goaway', code: status.code }),
          () => {},
        );
        return m.expectsReply ? m.reply(new Uint8Array(0)) : undefined;
      case 'WHOAMI':
        return m.expectsReply ? m.reply(connection.peerIdentity?.identity ?? '') : undefined;
      case 'GOAWAY':
        setTimeout(() => void connection.goAway().catch(() => {}), 50);
        return m.expectsReply ? m.reply(new Uint8Array(0)) : undefined;
      case 'CLOSE':
        setTimeout(() => void connection.close().catch(() => {}), 50);
        return undefined;
      default:
        return m.expectsReply ? m.replyAbort(Status.of(StatusCode.unimplemented)) : undefined;
    }
  };
  return new Map<string, Service>([
    [
      Name.of('echo').key,
      (talk) => messages(talk, (m) => (m.expectsReply ? m.reply(m.payload) : undefined)),
    ],
    [
      Name.of('issue').key,
      (talk) =>
        messages(talk, (m) =>
          m.expectsReply
            ? m.reply(fakeCredential('browser-1', m.payload.slice()).encode())
            : undefined,
        ),
    ],
    [Name.of('control').key, (talk, connection) => messages(talk, (m) => control(m, connection))],
  ]);
}

/** The node's side of one connection: Talk's bulk routing, then each channel to its service. */
async function dispatch(
  connection: QuicBindingConnection,
  services: ReadonlyMap<string, Service>,
): Promise<void> {
  installBulkRouting(connection);
  for await (const channel of connection.incoming) {
    const service = serviceOf(services, channel);
    if (service === undefined) {
      void channel.close(Status.of(StatusCode.unimplemented)).catch(() => {});
      continue;
    }
    service(new TalkChannel(channel), connection);
  }
}

function serviceOf(
  services: ReadonlyMap<string, Service>,
  channel: MuxChannel,
): Service | undefined {
  try {
    const type = ChannelAddress.decode(channel.openPayload).type;
    return type === undefined ? undefined : services.get(type.key);
  } catch {
    return undefined;
  }
}
