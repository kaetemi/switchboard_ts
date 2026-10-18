// The web provider over an in-memory implementation of the W3C WebTransport API: the
// parts of `switchboard_quic`'s `test/web_provider_test.dart` that need no
// browser (the browser runs in `test/browser/webtransport.test.ts`), and
// what a browser rarely shows: send orders and groups, `draining`,
// `exportKeyingMaterial` and `getStats` where a browser has them, the
// handshake deadline and the session-gone grace on a manual clock, the
// `openStream` backoff, and the two forms of the `WebTransportError`
// constructor. The in-memory API models Chromium where nothing else is said: no
// optional member, `createBidirectionalStream` rejecting without stream
// credit, the dictionary form of `WebTransportError` with an octet code, a
// writer whose `desiredSize` starts at 1, and `closed` resolving at once on
// a local close.
import { describe, expect, test } from 'vitest';
import { muxOptions } from '../../../src/core/mux/contract.ts';
import {
  type QuicClientConfig,
  QuicError,
  type QuicErrorKind,
  type QuicStream,
  QuicStreamReset,
  QuicStreamStopped,
} from '../../../src/core/quic/contract.ts';
import { StatusCode, SwitchboardError } from '../../../src/core/status.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';
import { ManualClock } from '../../../src/testing/manual-clock.ts';
import {
  readApiError,
  StreamErrors,
  type WebTransportCloseInit,
  type WebTransportErrorConstructor,
  type WebTransportInit,
  type WebTransportLike,
  type WebTransportStreamInit,
} from '../../../src/web/quic/webtransport-api.ts';
import {
  WebTransportProvider,
  type WebTransportProviderOptions,
  WebTransportSession,
  WebTransportStream,
  webTransport,
  webTransportConnector,
} from '../../../src/web/quic/webtransport-provider.ts';

// --- The in-memory API ---------------------------------------------------------------------------

/** Chromium's `WebTransportError`: only `new WebTransportError({ message, streamErrorCode })`, the code an octet. */
class ChromiumError extends Error {
  readonly source: string;
  readonly streamErrorCode: number | null;
  constructor(init: { message?: string; streamErrorCode?: number; source?: string } = {}) {
    if (typeof init !== 'object' || init === null) {
      throw new TypeError("parameter 1 is not of type 'WebTransportErrorInit'");
    }
    super(init.message ?? '');
    this.name = 'WebTransportError';
    this.source = init.source ?? 'stream';
    this.streamErrorCode =
      init.streamErrorCode === undefined ? null : Math.min(init.streamErrorCode, 255);
  }
}

/** The specification's `WebTransportError`: `new WebTransportError(message, { streamErrorCode })`, a `u32`. */
class SpecError extends Error {
  readonly source = 'stream';
  readonly streamErrorCode: number | null;
  constructor(message = '', options: { streamErrorCode?: number } = {}) {
    super(String(message));
    this.name = 'WebTransportError';
    this.streamErrorCode = options.streamErrorCode ?? null;
  }
}

/** The error a stream of the in-memory API reports for [source] with [code]. */
function streamError(code: number | null, source = 'stream'): ChromiumError {
  const e = new ChromiumError({ message: `${source} error`, source });
  (e as { streamErrorCode: number | null }).streamErrorCode = code;
  return e;
}

const codeOf = (reason: unknown): number | null | undefined =>
  reason === undefined ? undefined : (readApiError(reason).streamErrorCode ?? null);

/** The server's end of one bidirectional stream of the in-memory API. */
class ServerStream {
  readonly readable: ReadableStream<Uint8Array>;
  readonly writable: WritableStream<Uint8Array> & { sendOrder?: number; sendGroup?: unknown };
  #toClient!: ReadableStreamDefaultController<Uint8Array>;
  #fromClient!: WritableStreamDefaultController;
  /** What the client wrote, in order. */
  readonly received: Uint8Array[] = [];
  /** The client's FIN arrived. */
  fin = false;
  /** The code of the client's RESET_STREAM (abort); null for an abort without a code. */
  resetCode: number | null | undefined;
  /** The code of the client's STOP_SENDING (cancel); null for a cancel without a code. */
  stopCode: number | null | undefined;
  /** While set, the client's writes wait (the server reads nothing). */
  #held: Deferred<void> | undefined;

  constructor(options: { sendOrder?: boolean } = {}) {
    this.readable = new ReadableStream<Uint8Array>(
      {
        start: (c) => {
          this.#toClient = c;
        },
        cancel: (reason: unknown) => {
          this.stopCode = codeOf(reason) ?? null;
        },
      },
      { highWaterMark: 0 },
    );
    const writable: WritableStream<Uint8Array> & { sendOrder?: number; sendGroup?: unknown } =
      new WritableStream<Uint8Array>({
        start: (c) => {
          this.#fromClient = c;
        },
        write: async (chunk) => {
          if (this.#held !== undefined) await this.#held.promise;
          this.received.push(chunk.slice());
        },
        close: () => {
          this.fin = true;
        },
        abort: (reason: unknown) => {
          this.resetCode = codeOf(reason) ?? null;
        },
      });
    if (options.sendOrder === true) {
      writable.sendOrder = 0;
      writable.sendGroup = null;
    }
    this.writable = writable;
  }

  /** Everything the client wrote. */
  get bytes(): Uint8Array {
    return concat(this.received);
  }

  send(bytes: Uint8Array | string): void {
    this.#toClient.enqueue(typeof bytes === 'string' ? utf8(bytes) : bytes);
  }

  finish(): void {
    this.#toClient.close();
  }

  /** RESET_STREAM with [code] (null: none). */
  reset(code: number | null, source = 'stream'): void {
    this.#toClient.error(streamError(code, source));
  }

  /** STOP_SENDING with [code] (null: none); a write in flight fails with it. */
  stop(code: number | null, source = 'stream'): void {
    const error = streamError(code, source);
    this.#held?.reject(error);
    this.#held = undefined;
    this.#fromClient.error(error);
  }

  /** The readable fails with [error], which is no WebTransportError. */
  fail(error: unknown): void {
    this.#toClient.error(error);
  }

  hold(): void {
    if (this.#held !== undefined) return;
    // Rejected by a STOP_SENDING, whether a write waits on it or not (a Deferred is handled).
    this.#held = new Deferred<void>();
  }

  release(): void {
    this.#held?.resolve();
    this.#held = undefined;
  }
}

/** What an in-memory session has beyond Chromium's. */
interface SessionFeatures {
  readonly protocol?: boolean;
  readonly draining?: boolean;
  readonly exporter?: (label: Uint8Array, context?: Uint8Array) => Promise<unknown>;
  readonly stats?: () => Promise<unknown>;
  readonly sendGroups?: boolean;
  readonly sendOrder?: boolean;
}

/** One in-memory session, and the server's controls of it. */
class MemoryWebTransport implements WebTransportLike {
  readonly url: string;
  readonly options: WebTransportInit;
  readonly ready: Promise<unknown>;
  readonly closed: Promise<unknown>;
  readonly incomingBidirectionalStreams: ReadableStream<unknown>;
  readonly incomingUnidirectionalStreams: ReadableStream<unknown>;
  readonly protocol?: unknown;
  readonly draining?: unknown;
  readonly exportKeyingMaterial?: (label: Uint8Array, context?: Uint8Array) => Promise<unknown>;
  readonly getStats?: () => Promise<unknown>;
  readonly createSendGroup?: () => unknown;
  readonly #ready = new Deferred<void>();
  readonly #closed = new Deferred<unknown>();
  readonly #drain = new Deferred<void>();
  readonly #features: SessionFeatures;
  #pulling: ReadableStreamDefaultController<unknown> | undefined;
  #toClient!: ReadableStreamDefaultController<unknown>;
  #uniToClient!: ReadableStreamDefaultController<unknown>;
  /** Streams the client may still open (MAX_STREAMS). */
  credit = 128;
  /** The client's streams, in the order opened. */
  readonly opened: ServerStream[] = [];
  /** The server's streams the client took from `incomingBidirectionalStreams`. */
  taken = 0;
  readonly #queued: ServerStream[] = [];
  /** Every `close` the client called. */
  readonly closes: (WebTransportCloseInit | undefined)[] = [];
  /** Every `createBidirectionalStream` the client called. */
  readonly attempts: (WebTransportStreamInit | undefined)[] = [];
  /** Send groups created. */
  readonly groups: object[] = [];
  #ended = false;

  constructor(url: string, options: WebTransportInit, features: SessionFeatures) {
    this.url = url;
    this.options = options;
    this.#features = features;
    this.ready = this.#ready.promise;
    this.closed = this.#closed.promise;
    // A browser marks these handled: they reject with the session.
    this.ready.catch(() => {});
    this.closed.catch(() => {});
    this.incomingBidirectionalStreams = new ReadableStream<unknown>(
      {
        start: (c) => {
          this.#toClient = c;
        },
        pull: (c) => {
          const next = this.#queued.shift();
          if (next !== undefined) {
            this.taken++;
            c.enqueue({ readable: next.readable, writable: next.writable });
          } else {
            this.#pulling = c;
          }
        },
      },
      { highWaterMark: 0 },
    );
    this.incomingUnidirectionalStreams = new ReadableStream<unknown>({
      start: (c) => {
        this.#uniToClient = c;
      },
    });
    if (features.protocol === true) this.protocol = '';
    if (features.draining === true) this.draining = this.#drain.promise;
    if (features.exporter !== undefined) this.exportKeyingMaterial = features.exporter;
    if (features.stats !== undefined) this.getStats = features.stats;
    if (features.sendGroups === true) {
      this.createSendGroup = () => {
        const group = { id: this.groups.length };
        this.groups.push(group);
        return group;
      };
    }
  }

  createBidirectionalStream(options?: WebTransportStreamInit): Promise<unknown> {
    this.attempts.push(options);
    if (this.#ended) return Promise.reject(streamError(null, 'session'));
    if (this.credit <= 0) {
      return Promise.reject(new DOMException('no stream credit', 'InvalidStateError'));
    }
    this.credit--;
    const stream = new ServerStream({ sendOrder: this.#features.sendOrder === true });
    this.opened.push(stream);
    return Promise.resolve({ readable: stream.readable, writable: stream.writable });
  }

  close(closeInfo?: WebTransportCloseInit): void {
    this.closes.push(closeInfo);
    if (this.#ended) return;
    this.#end();
    this.#closed.resolve({ closeCode: closeInfo?.closeCode ?? 0, reason: closeInfo?.reason ?? '' });
  }

  // The server's side.

  /** The server's 2xx: the session is established, selecting [protocol] where the session has `protocol`. */
  accept(protocol = 'switchboard'): void {
    if (this.#features.protocol === true) {
      (this as { protocol?: unknown }).protocol = protocol;
    }
    this.#ready.resolve();
  }

  /** The handshake fails as a browser reports it. */
  refuse(): void {
    const error = new ChromiumError({ message: 'Opening handshake failed.', source: 'session' });
    this.#end();
    this.#ready.reject(error);
    this.#closed.reject(error);
  }

  /** The server opens a bidirectional stream. */
  openStream(): ServerStream {
    const stream = new ServerStream();
    const pulling = this.#pulling;
    if (pulling !== undefined) {
      this.#pulling = undefined;
      this.taken++;
      pulling.enqueue({ readable: stream.readable, writable: stream.writable });
    } else {
      this.#queued.push(stream);
    }
    return stream;
  }

  /** The server opens a unidirectional stream; resolves with the client's STOP_SENDING code. */
  openUnidirectional(): Promise<number | null> {
    const stopped = new Deferred<number | null>();
    this.#uniToClient.enqueue(
      new ReadableStream<Uint8Array>({
        cancel: (reason: unknown) => {
          stopped.resolve(codeOf(reason) ?? null);
        },
      }),
    );
    return stopped.promise;
  }

  /** The server's close: the streams report the session's end, then `closed` resolves. */
  serverClose(code: number, reason: string): void {
    this.#end();
    this.#closed.resolve({ closeCode: code, reason });
  }

  /** The connection is lost: `closed` rejects. */
  lose(message: string): void {
    this.#end();
    this.#closed.reject(new ChromiumError({ message, source: 'session' }));
  }

  /** The streams report the session gone, and `closed` settles later or never. */
  streamsGone(): void {
    for (const s of this.opened) {
      try {
        s.reset(null, 'session');
      } catch {
        // Ended already.
      }
    }
  }

  /** The server drains the session. */
  drain(): void {
    this.#drain.resolve();
  }

  #end(): void {
    if (this.#ended) return;
    this.#ended = true;
    for (const s of this.opened) {
      for (const end of [() => s.reset(null, 'session'), () => s.stop(null, 'session')]) {
        try {
          end();
        } catch {
          // Ended already.
        }
      }
    }
    try {
      this.#toClient.close();
      this.#uniToClient.close();
    } catch {
      // Closed already.
    }
  }
}

/** A `WebTransport` constructor of in-memory sessions, recording the sessions it created. */
function memoryWebTransportApi(features: SessionFeatures = {}) {
  const sessions: MemoryWebTransport[] = [];
  let failNext: Error | undefined;
  class RecordingWebTransport extends MemoryWebTransport {
    constructor(url: string, options: WebTransportInit) {
      if (failNext !== undefined) {
        const error = failNext;
        failNext = undefined;
        throw error;
      }
      super(url, options, features);
      sessions.push(this);
    }
  }
  return {
    WebTransport: RecordingWebTransport,
    sessions,
    get last(): MemoryWebTransport {
      const last = sessions.at(-1);
      if (last === undefined) throw new Error('no session');
      return last;
    },
    failNextWith(error: Error): void {
      failNext = error;
    },
  };
}

// --- Helpers -------------------------------------------------------------------------------------

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const utf8 = (text: string): Uint8Array => encoder.encode(text);
const text = (bytes: Uint8Array): string => decoder.decode(bytes);

function concat(chunks: readonly Uint8Array[]): Uint8Array {
  const out = new Uint8Array(chunks.reduce((n, c) => n + c.length, 0));
  let at = 0;
  for (const c of chunks) {
    out.set(c, at);
    at += c.length;
  }
  return out;
}

/** The pattern `tool/web_server.dart` sends. */
function pattern(n: number): Uint8Array {
  const out = new Uint8Array(n);
  for (let i = 0; i < n; i++) out[i] = (i * 31 + (i >> 8)) & 0xff;
  return out;
}

const config = (init: Partial<QuicClientConfig> = {}): QuicClientConfig => ({
  alpn: ['switchboard'],
  handshakeTimeout: 0,
  ...init,
});

/** Lets the microtasks and a macrotask run. */
const tick = (ms = 0): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

/** Whether [promise] is still pending after the queued work ran. */
async function pending(promise: Promise<unknown>, ms = 0): Promise<boolean> {
  let settled = false;
  promise.then(
    () => {
      settled = true;
    },
    () => {
      settled = true;
    },
  );
  await tick(ms);
  return !settled;
}

/** The error [promise] rejects with. */
async function rejection(promise: Promise<unknown>): Promise<unknown> {
  try {
    await promise;
  } catch (error) {
    return error;
  }
  throw new Error('resolved');
}

async function expectQuic(promise: Promise<unknown>, kind: QuicErrorKind): Promise<QuicError> {
  const error = await rejection(promise);
  expect(error).toBeInstanceOf(QuicError);
  expect((error as QuicError).kind).toBe(kind);
  return error as QuicError;
}

async function expectReset(promise: Promise<unknown>, code: number | undefined): Promise<void> {
  const error = await rejection(promise);
  expect(error).toBeInstanceOf(QuicStreamReset);
  expect((error as QuicStreamReset).code).toBe(code);
}

/** Everything [stream] delivers up to FIN. */
async function readAll(stream: QuicStream, max = 65536): Promise<Uint8Array> {
  const chunks: Uint8Array[] = [];
  for (;;) {
    const chunk = await stream.read(max);
    if (chunk === undefined) return concat(chunks);
    chunks.push(chunk);
  }
}

interface Setup {
  readonly api: ReturnType<typeof memoryWebTransportApi>;
  readonly provider: WebTransportProvider;
  readonly clock: ManualClock | undefined;
}

function setup(
  features: SessionFeatures = {},
  options: Omit<WebTransportProviderOptions, 'WebTransport'> & { manual?: boolean } = {},
): Setup {
  const api = memoryWebTransportApi(features);
  const clock = options.manual === true ? new ManualClock() : undefined;
  const provider = webTransport({
    WebTransport: api.WebTransport,
    WebTransportError: options.WebTransportError ?? (ChromiumError as WebTransportErrorConstructor),
    ...(clock === undefined ? {} : { clock }),
  });
  return { api, provider, clock };
}

/** An established session over a fresh in-memory API. */
async function open(
  features: SessionFeatures = {},
  options: Parameters<typeof setup>[1] = {},
): Promise<Setup & { session: WebTransportSession; transport: MemoryWebTransport }> {
  const s = setup(features, options);
  const connecting = s.provider.connect('https://127.0.0.1:4433/provider?case=echo#x', config());
  s.api.last.accept();
  const session = await connecting;
  return { ...s, session, transport: s.api.last };
}

// --- Tests ---------------------------------------------------------------------------------------

describe('the WebTransport API surface', () => {
  test('readApiError reads WebTransportErrors, DOMExceptions and anything else', () => {
    expect(readApiError(streamError(14))).toEqual({
      name: 'WebTransportError',
      message: 'stream error',
      source: 'stream',
      streamErrorCode: 14,
    });
    expect(readApiError(streamError(null, 'session')).streamErrorCode).toBeUndefined();
    expect(readApiError(new DOMException('gone', 'InvalidStateError'))).toMatchObject({
      name: 'InvalidStateError',
      message: 'gone',
      source: undefined,
    });
    expect(readApiError('text')).toMatchObject({ name: '', message: 'text' });
  });

  test("StreamErrors probes the constructor's form and the largest code it carries", () => {
    const chromium = new StreamErrors(ChromiumError as WebTransportErrorConstructor);
    expect(chromium.maxCode).toBe(255);
    expect(codeOf(chromium.create(42, 'RESET_STREAM'))).toBe(42);
    expect(() => chromium.create(256, 'x')).toThrow(RangeError);
    const spec = new StreamErrors(SpecError as WebTransportErrorConstructor);
    expect(spec.maxCode).toBe(0xffffffff);
    expect(codeOf(spec.create(0xffffffff, 'x'))).toBe(0xffffffff);
    const none = new StreamErrors(undefined);
    expect(none.maxCode).toBe(0);
    expect(none.create(0, 'x')).toBeUndefined();
    expect(() => none.create(1, 'x')).toThrow(RangeError);
  });
});

describe('connecting', () => {
  test('the session: the selected protocol, the initiator, the remote, what the browser does not tell', async () => {
    const { session, transport, provider } = await open({ protocol: true });
    expect(provider.isSupported).toBe(true);
    expect(session).toBeInstanceOf(WebTransportSession);
    expect(session.protocol).toBe('switchboard');
    expect(session.isInitiator).toBe(true);
    expect(session.remote).toBe('127.0.0.1:4433');
    expect(session.maxIncomingStreams).toBeUndefined();
    expect(session.peerCertificateHash).toBeUndefined();
    expect(String(session)).toBe('WebTransportSession(127.0.0.1:4433)');
    // The fragment is dropped; the query stays.
    expect(transport.url).toBe('https://127.0.0.1:4433/provider?case=echo');
    expect(transport.options).toEqual({
      allowPooling: false,
      requireUnreliable: false,
      protocols: ['switchboard'],
    });
    expect(session.capabilities).toEqual({
      protocol: true,
      draining: false,
      exportKeyingMaterial: false,
      getStats: false,
      createSendGroup: false,
    });
  });

  test('Chromium 141 has none of the optional members: no protocol is known', async () => {
    const { session } = await open();
    expect(session.protocol).toBeUndefined();
    expect(Object.values(session.capabilities)).toEqual([false, false, false, false, false]);
  });

  test('a session that offers no protocol: none in the options, none selected', async () => {
    const { api, provider } = setup({ protocol: true });
    const connecting = provider.connect('https://[::1]/sb', config({ alpn: [] }));
    api.last.accept('');
    const session = await connecting;
    expect(api.last.options.protocols).toBeUndefined();
    expect(session.protocol).toBeUndefined();
    expect(session.remote).toBe('[::1]:443');
  });

  test('pinned certificate hashes go to the browser as sha-256 hashes, copied', async () => {
    const { api, provider } = setup();
    const hash = new Uint8Array(32).fill(7);
    const connecting = provider.connect(
      'https://127.0.0.1:1/sb',
      config({ serverCertificateHashes: [hash] }),
    );
    hash.fill(0);
    api.last.accept();
    await connecting;
    const pins = api.last.options.serverCertificateHashes;
    expect(pins).toHaveLength(1);
    expect(pins?.[0]?.algorithm).toBe('sha-256');
    expect([...(pins?.[0]?.value ?? [])]).toEqual(new Array(32).fill(7));
  });

  test('schemes and arguments', async () => {
    const { api, provider } = setup();
    for (const uri of ['quic://127.0.0.1:4433', 'wss://127.0.0.1/sb']) {
      await expectQuic(provider.connect(uri, config()), 'unsupportedScheme');
    }
    const reject = async (promise: Promise<unknown>, type: ErrorConstructor) =>
      expect(await rejection(promise)).toBeInstanceOf(type);
    await reject(provider.connect('not a uri', config()), TypeError);
    await reject(provider.connect('https://user:pw@127.0.0.1/sb', config()), TypeError);
    await reject(
      provider.connect('https://127.0.0.1:1/sb', config({ idleTimeout: -1 })),
      RangeError,
    );
    await reject(provider.connect('https://127.0.0.1:1/sb', config({ keepAlive: -1 })), RangeError);
    await reject(
      provider.connect('https://127.0.0.1:1/sb', config({ handshakeTimeout: -1 })),
      RangeError,
    );
    await reject(
      provider.connect(
        'https://127.0.0.1:1/sb',
        config({ serverCertificateHashes: [new Uint8Array(16)] }),
      ),
      RangeError,
    );
    // Chromium sends no WT-Available-Protocols and checks nothing: the provider does.
    for (const alpn of [['a', 'a'], [''], ['é'], ['x'.repeat(513)]]) {
      await reject(provider.connect('https://127.0.0.1:1/sb', config({ alpn })), TypeError);
    }
    // The browser's own refusal (a SyntaxError) is an argument error too.
    api.failNextWith(new DOMException('bad', 'SyntaxError'));
    await reject(provider.connect('https://127.0.0.1:1/sb', config()), TypeError);
    expect(api.sessions).toHaveLength(0);
    // Without the API: unsupported.
    const missing = webTransport({ WebTransport: undefined });
    expect(missing.isSupported).toBe(WebTransportProvider.isSupported);
    if (!missing.isSupported) {
      await expectQuic(missing.connect('https://127.0.0.1/sb', config()), 'unsupportedScheme');
    }
  });

  test('a refused path, a wrong certificate hash, a closed port: the connect fails handshakeFailed', async () => {
    const { api, provider } = setup();
    const connecting = provider.connect('https://127.0.0.1:4433/nope', config());
    api.last.refuse();
    const error = await expectQuic(connecting, 'handshakeFailed');
    expect(error.message).toContain('Opening handshake failed.');
  });

  test('closed before ready: handshakeFailed', async () => {
    const { api, provider } = setup();
    const connecting = provider.connect('https://127.0.0.1:4433/sb', config());
    api.last.serverClose(0, '');
    await expectQuic(connecting, 'handshakeFailed');
  });

  test('the handshake deadline: unreachable, and the browser is told to give up', async () => {
    const { api, provider, clock } = setup({}, { manual: true });
    const connecting = provider.connect(
      'https://127.0.0.1:4433/sb',
      config({ handshakeTimeout: 500 }),
    );
    clock?.advance(499);
    expect(await pending(connecting)).toBe(true);
    clock?.advance(1);
    const error = await expectQuic(connecting, 'unreachable');
    expect(error.message).toContain('500 ms');
    expect(api.last.closes).toEqual([undefined]);
    // A late ready changes nothing.
    api.last.accept();
    await tick();
    expect(clock?.pendingCount).toBe(0);
  });

  test('the default deadline is 10 s; 0 is none', async () => {
    const { api, provider, clock } = setup({}, { manual: true });
    const bounded = provider.connect('https://127.0.0.1:4433/sb', {
      alpn: ['switchboard'],
    });
    const unbounded = provider.connect('https://127.0.0.1:4433/sb', config());
    clock?.advance(10_000);
    await expectQuic(bounded, 'unreachable');
    expect(await pending(unbounded)).toBe(true);
    api.last.accept();
    await unbounded;
  });
});

describe('streams', () => {
  test('a stream echoed, FIN both ways, acknowledgement', async () => {
    const { session, transport } = await open();
    const stream = await session.openStream();
    expect(stream).toBeInstanceOf(WebTransportStream);
    expect(transport.attempts).toEqual([{ waitUntilAvailable: true }]);
    stream.priority = 1;
    const mine = utf8('hello ');
    await stream.write(mine);
    mine.fill(0); // The provider wrote a copy.
    await stream.write(utf8('switchboard'));
    await stream.finish();
    expect(await stream.stopped).toBeUndefined();
    const server = transport.opened[0] as ServerStream;
    expect(text(server.bytes)).toBe('hello switchboard');
    expect(server.fin).toBe(true);
    server.send('hello ');
    server.send('switchboard');
    server.finish();
    expect(text(await readAll(stream))).toBe('hello switchboard');
    expect(await stream.read(10)).toBeUndefined();
    expect(await stream.read(10)).toBeUndefined();
    expect(await rejection(stream.write(utf8('more')))).toBeInstanceOf(Error);
    // finish is idempotent.
    await stream.finish();
  });

  test('read(max) returns at most max bytes, keeps the rest of a chunk, then FIN', async () => {
    const { session, transport } = await open();
    const stream = await session.openStream();
    await stream.write(new Uint8Array(4));
    const server = transport.opened[0] as ServerStream;
    const size = 300_000;
    const data = pattern(size);
    for (let at = 0; at < size; at += 65536) server.send(data.slice(at, at + 65536));
    server.send(new Uint8Array(0)); // An empty chunk is skipped.
    server.finish();
    const got: Uint8Array[] = [];
    const sizes = [1, 7, 4, 1000, 65536, 3, 100_000];
    for (let i = 0; ; i++) {
      const max = sizes[i % sizes.length] as number;
      const chunk = await stream.read(max);
      if (chunk === undefined) break;
      expect(chunk.length).toBeGreaterThan(0);
      expect(chunk.length).toBeLessThanOrEqual(max);
      got.push(chunk);
    }
    expect(concat(got)).toEqual(data);
    expect(await stream.read(1)).toBeUndefined();
    expect(() => stream.read(0)).toThrow(RangeError);
    expect(() => stream.read(1.5)).toThrow(RangeError);
  });

  test('only one read may be pending; the session end fails it', async () => {
    const { session } = await open();
    const stream = await session.openStream();
    await stream.write(utf8('x'));
    const reading = stream.read(10);
    expect(() => stream.read(10)).toThrow(Error);
    await session.close(0, '');
    await expectQuic(reading, 'sessionEnded');
  });

  test('reset(42) reaches the server as 42; writes after it are errors', async () => {
    const { session, transport } = await open();
    const stream = await session.openStream();
    await stream.write(utf8('abc'));
    stream.reset(42);
    stream.reset(43); // A no-op.
    expect(await stream.stopped).toBeUndefined();
    expect(await rejection(stream.write(utf8('d')))).toBeInstanceOf(Error);
    expect(await rejection(stream.finish())).toBeInstanceOf(Error);
    await tick();
    expect((transport.opened[0] as ServerStream).resetCode).toBe(42);
  });

  test('a reset fails the writes still queued', async () => {
    const { session, transport } = await open();
    const stream = await session.openStream();
    (transport.opened[0] as ServerStream).hold();
    const first = stream.write(utf8('one'));
    const second = stream.write(utf8('two'));
    await tick();
    stream.reset(1);
    expect(await rejection(first)).toBeInstanceOf(Error);
    expect(await rejection(second)).toBeInstanceOf(Error);
  });

  test("the server's reset arrives as 14, also for a reader that is not reading", async () => {
    const { session, transport } = await open();
    const stream = await session.openStream();
    await stream.write(utf8('go'));
    (transport.opened[0] as ServerStream).reset(14);
    // Nobody reads: the reset is still observed.
    expect(await stream.resetReceived).toBe(14);
    await expectReset(stream.read(100), 14);
    await expectReset(stream.read(100), 14);
  });

  test('a reset without a code, or no WebTransportError, has none', async () => {
    const { session, transport } = await open();
    const a = await session.openStream();
    const b = await session.openStream();
    (transport.opened[0] as ServerStream).reset(null);
    expect(await a.resetReceived).toBeUndefined();
    await expectReset(a.read(1), undefined);
    // A readable errored with something else entirely.
    (transport.opened[1] as ServerStream).fail(new TypeError('broken'));
    expect(await b.resetReceived).toBeUndefined();
    await expectReset(b.read(1), undefined);
  });

  test('stop(5) reaches the server as STOP_SENDING 5, and ends the read direction at once', async () => {
    const { session, transport } = await open();
    const stream = await session.openStream();
    await stream.write(utf8('x'));
    const reading = stream.read(100);
    stream.stop(5);
    stream.stop(6); // A no-op.
    await expectReset(reading, 5);
    await expectReset(stream.read(100), 5);
    await tick();
    expect((transport.opened[0] as ServerStream).stopCode).toBe(5);
    expect(await pending(stream.resetReceived)).toBe(true);
    // Writing goes on.
    await stream.write(utf8('y'));
  });

  test("the server's STOP_SENDING 9 resolves stopped with 9 and fails writes", async () => {
    const { session, transport } = await open();
    const stream = await session.openStream();
    await stream.write(utf8('first'));
    (transport.opened[0] as ServerStream).stop(9);
    expect(await stream.stopped).toBe(9);
    const error = await rejection(stream.write(utf8('second')));
    expect(error).toBeInstanceOf(QuicStreamStopped);
    expect((error as QuicStreamStopped).code).toBe(9);
    expect(await rejection(stream.finish())).toBeInstanceOf(QuicStreamStopped);
  });

  test('a STOP_SENDING without a code rejects stopped: never mistaken for an acknowledgement', async () => {
    const { session, transport } = await open();
    const stream = await session.openStream();
    await stream.write(utf8('x'));
    (transport.opened[0] as ServerStream).stop(null);
    const error = await rejection(stream.stopped);
    expect(error).toBeInstanceOf(QuicStreamStopped);
    expect((error as QuicStreamStopped).code).toBeUndefined();
  });

  test('a STOP_SENDING while a write waits fails that write with it', async () => {
    const { session, transport } = await open();
    const stream = await session.openStream();
    const server = transport.opened[0] as ServerStream;
    server.hold();
    const writing = stream.write(utf8('held'));
    await tick();
    server.stop(4);
    const error = await rejection(writing);
    expect(error).toBeInstanceOf(QuicStreamStopped);
    expect((error as QuicStreamStopped).code).toBe(4);
    expect(await stream.stopped).toBe(4);
  });

  test('writes wait for the writer, in order, and fail when the session ends', async () => {
    const { session, transport } = await open();
    const stream = await session.openStream();
    const server = transport.opened[0] as ServerStream;
    server.hold();
    const first = stream.write(utf8('1'));
    const second = stream.write(utf8('2'));
    const third = stream.write(utf8('3'));
    expect(await pending(first, 10)).toBe(true);
    expect(await pending(third)).toBe(true);
    server.release();
    await Promise.all([first, second, third]);
    expect(text(server.bytes)).toBe('123');
    server.hold();
    const stalled = stream.write(new Uint8Array(65536));
    expect(await pending(stalled, 10)).toBe(true);
    await session.close(0, '');
    await expectQuic(stalled, 'sessionEnded');
  });

  test('write takes a Uint8Array only', async () => {
    const { session } = await open();
    const stream = await session.openStream();
    expect(() => stream.write('text' as unknown as Uint8Array)).toThrow(TypeError);
  });
});

describe('the end of a session', () => {
  test('close(7, reason) is a local application close; what follows fails sessionEnded', async () => {
    const { session, transport } = await open();
    const stream = await session.openStream();
    await stream.write(utf8('x'));
    await session.close(7, 'browser done');
    expect(transport.closes).toEqual([{ closeCode: 7, reason: 'browser done' }]);
    expect(await session.done).toEqual({
      code: 7,
      reason: 'browser done',
      byPeer: false,
      lost: false,
    });
    await session.close(8, 'again'); // Idempotent.
    expect(transport.closes).toHaveLength(1);
    await expectQuic(session.openStream(), 'sessionEnded');
    await expectQuic(stream.write(utf8('y')), 'sessionEnded');
    await expectQuic(stream.read(1), 'sessionEnded');
  });

  test('a reason is cut to 1024 bytes of UTF-8 at a character boundary; codes are u32', async () => {
    const { session, transport } = await open();
    expect(() => session.close(-1, '')).toThrow(RangeError);
    expect(() => session.close(0x100000000, '')).toThrow(RangeError);
    expect(() => session.close(1.5, '')).toThrow(RangeError);
    await session.close(300, 'é'.repeat(600));
    expect(transport.closes[0]).toEqual({ closeCode: 300, reason: 'é'.repeat(512) });
    expect((await session.done).reason).toBe('é'.repeat(512));
  });

  test("the server's close is the session's end with its code and reason; what is pending fails", async () => {
    const { session, transport } = await open();
    const stream = await session.openStream();
    const reset = rejection(stream.resetReceived);
    const stopped = rejection(stream.stopped);
    const reading = stream.read(10);
    await stream.write(utf8('x'));
    const opening = session.openStream();
    transport.credit = 0;
    const waiting = session.openStream();
    transport.serverClose(9, 'server bye');
    expect(await session.done).toEqual({
      code: 9,
      reason: 'server bye',
      byPeer: true,
      lost: false,
    });
    await expectQuic(reading, 'sessionEnded');
    expect((await reset) as QuicError).toBeInstanceOf(QuicError);
    expect(((await stopped) as QuicError).kind).toBe('sessionEnded');
    await opening.catch(() => {});
    await expectQuic(waiting, 'sessionEnded');
    await expectQuic(session.openStream(), 'sessionEnded');
    await expectQuic(
      session.exportKeyingMaterial('switchboard-ident', new Uint8Array(0), 32),
      'sessionEnded',
    );
  });

  test('a lost connection: closed rejects, the session ends lost', async () => {
    const { session, transport } = await open();
    const stream = await session.openStream();
    transport.lose('network gone');
    const end = await session.done;
    expect(end.lost).toBe(true);
    expect(end.code).toBeUndefined();
    expect(end.reason).toContain('network gone');
    await expectQuic(stream.read(1), 'sessionEnded');
  });

  test('streams that report the session gone end it as lost after two seconds without closed', async () => {
    const { session, transport, clock } = await open({}, { manual: true });
    const stream = await session.openStream();
    transport.streamsGone();
    await expectQuic(stream.read(1), 'sessionEnded');
    expect(await pending(session.done)).toBe(true);
    clock?.advance(1999);
    expect(await pending(session.done)).toBe(true);
    clock?.advance(1);
    const end = await session.done;
    expect(end.lost).toBe(true);
    expect(end.reason).toBe('session error');
  });

  test('a session-gone report followed by closed: closed decides', async () => {
    const { session, transport, clock } = await open({}, { manual: true });
    await session.openStream();
    transport.streamsGone();
    await tick();
    transport.serverClose(3, 'later');
    expect(await session.done).toEqual({ code: 3, reason: 'later', byPeer: true, lost: false });
    expect(clock?.pendingCount).toBe(0);
  });
});

describe('the session', () => {
  test('draining stays pending without the promise, and resolves on the server drain with it', async () => {
    const chromium = await open();
    expect(await pending(chromium.session.draining, 10)).toBe(true);
    const { session, transport } = await open({ draining: true });
    expect(session.capabilities.draining).toBe(true);
    expect(await pending(session.draining)).toBe(true);
    transport.drain();
    await session.draining;
  });

  test('a drain before the session was established resolves draining once it is', async () => {
    const { api, provider } = setup({ draining: true });
    const connecting = provider.connect('https://127.0.0.1:4433/sb', config());
    api.last.drain();
    await tick();
    api.last.accept();
    const session = await connecting;
    await session.draining;
  });

  test('the exporter: undefined without exportKeyingMaterial, the browser value with it', async () => {
    const chromium = await open();
    expect(
      await chromium.session.exportKeyingMaterial('switchboard-ident', new Uint8Array(0), 32),
    ).toBeUndefined();
    expect(
      await rejection(chromium.session.exportKeyingMaterial('x', new Uint8Array(0), 0)),
    ).toBeInstanceOf(RangeError);
    const calls: [string, number[]][] = [];
    const { session } = await open({
      exporter: async (label, context) => {
        calls.push([text(label), [...(context ?? [])]]);
        return new Uint8Array(32).fill(label.length).buffer;
      },
    });
    const value = await session.exportKeyingMaterial('switchboard-ident', Uint8Array.of(1, 2), 32);
    expect(value).toEqual(new Uint8Array(32).fill(17));
    expect(calls).toEqual([['switchboard-ident', [1, 2]]]);
    // Another length than the browser's: undefined.
    expect(await session.exportKeyingMaterial('x', new Uint8Array(0), 16)).toBeUndefined();
    const failing = await open({ exporter: () => Promise.reject(new Error('no')) });
    expect(await failing.session.exportKeyingMaterial('x', new Uint8Array(0), 32)).toBeUndefined();
  });

  test('stats where the browser has getStats', async () => {
    const chromium = await open();
    expect(await chromium.session.stats()).toBeUndefined();
    const { session, transport } = await open({
      stats: async () => ({ bytesSent: 10.7, packetsLost: 2, smoothedRtt: 1.25, minRtt: 'x' }),
    });
    expect(await session.stats()).toEqual({
      bytesSent: 10,
      packetsSent: undefined,
      packetsLost: 2,
      bytesReceived: undefined,
      packetsReceived: undefined,
      smoothedRtt: 1.25,
      rttVariation: undefined,
      minRtt: undefined,
      estimatedSendRate: undefined,
    });
    transport.serverClose(0, '');
    await session.done;
    expect(await session.stats()).toBeUndefined();
    const failing = await open({ stats: () => Promise.reject(new Error('stub')) });
    expect(await failing.session.stats()).toBeUndefined();
  });

  test('two sessions at once are independent', async () => {
    const s = setup();
    const a = s.provider.connect('https://127.0.0.1:1/a', config());
    s.api.last.accept();
    const b = s.provider.connect('https://127.0.0.1:1/b', config());
    s.api.last.accept();
    const [sa, sb] = [await a, await b];
    await sa.close(1, 'a');
    expect(await pending(sb.done)).toBe(true);
    const stream = await sb.openStream();
    await stream.write(utf8('gamma'));
    const server = s.api.sessions[1]?.opened[0] as ServerStream;
    expect(text(server.bytes)).toBe('gamma');
  });
});

describe("the server's streams", () => {
  test('a stream the server opens arrives on incomingStreams, taken only while someone waits', async () => {
    const { session, transport } = await open();
    const first = transport.openStream();
    transport.openStream();
    await tick();
    // Nobody iterates: the streams wait in the browser.
    expect(transport.taken).toBe(0);
    const streams = session.incomingStreams[Symbol.asyncIterator]();
    const next = await streams.next();
    expect(next.done).toBe(false);
    await tick();
    expect(transport.taken).toBe(1);
    const stream = next.value as QuicStream;
    first.send('from server');
    first.finish();
    expect(text(await readAll(stream))).toBe('from server');
    await stream.write(utf8('back'));
    await stream.finish();
    expect(text(first.bytes)).toBe('back');
    expect(first.fin).toBe(true);
    // One consumer.
    const waiting = streams.next();
    expect(await rejection(streams.next())).toBeInstanceOf(Error);
    expect((await waiting).done).toBe(false);
    expect(transport.taken).toBe(2);
  });

  test('a stream read for a consumer that left is discarded: STOP_SENDING and RESET_STREAM 0', async () => {
    const { session, transport } = await open();
    const streams = session.incomingStreams[Symbol.asyncIterator]();
    const waiting = streams.next();
    await tick();
    await streams.return?.();
    expect((await waiting).done).toBe(true);
    const late = transport.openStream();
    await tick(5);
    expect(late.stopCode).toBe(0);
    expect(late.resetCode).toBe(0);
    expect((await streams.next()).done).toBe(true);
  });

  test('incomingStreams ends with the session', async () => {
    const { session, transport } = await open();
    const streams = session.incomingStreams[Symbol.asyncIterator]();
    const waiting = streams.next();
    transport.serverClose(0, '');
    expect((await waiting).done).toBe(true);
  });

  test('incomingUnidirectionalStreams: delivered for the application to stop, ending with the session', async () => {
    const { session, transport } = await open();
    const streams = session.incomingUnidirectionalStreams[Symbol.asyncIterator]();
    const stopped = transport.openUnidirectional();
    const next = await streams.next();
    expect(next.done).toBe(false);
    if (next.done !== true) {
      next.value.stop(StatusCode.unsupported);
      next.value.stop(1); // Idempotent.
    }
    expect(await stopped).toBe(StatusCode.unsupported);
    const waiting = streams.next();
    await session.close(0, '');
    expect((await waiting).done).toBe(true);
  });
});

describe('stream credit', () => {
  test('openStream waits for stream credit, in order, asking again at 5 to 200 ms', async () => {
    const { session, transport, clock } = await open({}, { manual: true });
    transport.credit = 2;
    const first = await session.openStream();
    const second = await session.openStream();
    const third = session.openStream();
    const fourth = session.openStream();
    await tick();
    const attempts = (): number => transport.attempts.length;
    expect(attempts()).toBe(3);
    // The waits double from 5 ms up to 200 ms.
    const waits: number[] = [];
    for (let i = 0; i < 8; i++) {
      const before = clock?.now() ?? 0;
      const count = attempts();
      while (attempts() === count) {
        clock?.advance(1);
        await tick();
      }
      waits.push((clock?.now() ?? 0) - before);
    }
    expect(waits).toEqual([5, 10, 20, 40, 80, 160, 200, 200]);
    expect(await pending(third)).toBe(true);
    // Credit returns: the first waiting call gets the stream.
    transport.credit = 1;
    clock?.advance(200);
    await tick();
    expect(await third).toBeInstanceOf(WebTransportStream);
    expect(await pending(fourth)).toBe(true);
    transport.credit = 1;
    clock?.advance(5);
    await tick();
    expect(await fourth).toBeInstanceOf(WebTransportStream);
    expect([first, second].every((s) => s instanceof WebTransportStream)).toBe(true);
  });
});

describe('codes and priorities', () => {
  test('stream codes: what the browser carries, and no more', async () => {
    expect(WebTransportProvider.sendOrderOf(2)).toBe(1);
    const { session, provider } = await open();
    expect(provider.maxStreamErrorCode).toBe(255);
    const stream = await session.openStream();
    expect(() => stream.reset(-1)).toThrow(RangeError);
    expect(() => stream.stop(0x100000000)).toThrow(RangeError);
    expect(() => stream.reset(256)).toThrow(RangeError);
    expect(() => stream.stop(256)).toThrow(RangeError);
    stream.reset(255);
    stream.stop(255);
    const spec = await open({}, { WebTransportError: SpecError as WebTransportErrorConstructor });
    expect(spec.provider.maxStreamErrorCode).toBe(0xffffffff);
    const wide = await spec.session.openStream();
    wide.reset(0xffffffff);
    await tick();
    expect((spec.transport.opened[0] as ServerStream).resetCode).toBe(0xffffffff);
  });

  test('without a WebTransportError constructor only code 0 goes out, as a reason that is none', async () => {
    const api = memoryWebTransportApi();
    const provider = webTransport({
      WebTransport: api.WebTransport,
      WebTransportError: undefined,
    });
    if (WebTransportProvider.maxStreamErrorCode !== 0) return; // A runtime that has one.
    const connecting = provider.connect('https://127.0.0.1:1/sb', config());
    api.last.accept();
    const session = await connecting;
    expect(provider.maxStreamErrorCode).toBe(0);
    const stream = await session.openStream();
    expect(() => stream.reset(1)).toThrow(RangeError);
    stream.reset(0);
    await tick();
    expect((api.last.opened[0] as ServerStream).resetCode).toBeNull();
  });

  test('priorities: send orders and groups where the browser has them', async () => {
    expect(WebTransportProvider.sendOrderOf(2)).toBe(1);
    expect(WebTransportProvider.sendOrderOf(1)).toBe(0);
    expect(WebTransportProvider.sendOrderOf(0)).toBe(0);
    expect(WebTransportProvider.sendOrderOf(-3)).toBe(-3);
    const chromium = await open();
    const plain = await chromium.session.openStream();
    expect(plain.hasSendOrder).toBe(false);
    expect(chromium.session.hasSendGroups).toBe(false);
    plain.priority = 2;
    expect(plain.priority).toBe(2);
    expect(() => {
      plain.priority = 1.5;
    }).toThrow(RangeError);
    const { session, transport } = await open({ sendGroups: true, sendOrder: true });
    expect(session.hasSendGroups).toBe(true);
    const control = await session.openStream();
    const ordinary = await session.openStream();
    const bulk = await session.openStream();
    expect(control.hasSendOrder).toBe(true);
    control.priority = 2;
    ordinary.priority = 1;
    bulk.priority = 0;
    const [c, o, b] = transport.opened as [ServerStream, ServerStream, ServerStream];
    expect([c.writable.sendOrder, o.writable.sendOrder, b.writable.sendOrder]).toEqual([1, 0, 0]);
    // Classes 2 and 1 share a group; class 0 has its own.
    expect(transport.groups).toHaveLength(2);
    expect(c.writable.sendGroup).toBe(transport.groups[0]);
    expect(o.writable.sendGroup).toBe(transport.groups[0]);
    expect(b.writable.sendGroup).toBe(transport.groups[1]);
  });
});

describe('connectors', () => {
  test('webTransportConnector dials https endpoints through the provider, offering switchboard', async () => {
    const { api, provider } = setup();
    const connector = webTransportConnector('https://127.0.0.1:4433/sb', {
      provider,
      serverCertificateHashes: [{ algorithm: 'sha-256', value: new Uint8Array(32).fill(1) }],
      timeout: 0,
    });
    const connecting = connector.connect(muxOptions(), { signal: new AbortController().signal });
    await tick();
    expect(api.last.url).toBe('https://127.0.0.1:4433/sb');
    expect(api.last.options.protocols).toEqual(['switchboard']);
    expect(api.last.options.serverCertificateHashes?.[0]?.value[0]).toBe(1);
    api.last.refuse();
    const error = await rejection(connecting);
    expect(error).toBeInstanceOf(SwitchboardError);
    expect((error as SwitchboardError).status.code).toBe(StatusCode.unavailable);
    expect((error as SwitchboardError).cause).toBeInstanceOf(QuicError);
  });
});
