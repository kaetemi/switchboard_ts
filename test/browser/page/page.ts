// The page of the browser tests: the web entry point and what the tests
// share in the browser, a port of `switchboard_quic`'s `test/web_harness.dart`.
// Vite serves it; `test/browser/webtransport.test.ts` drives it through
// Playwright, running each test's body here with `run`. The server's
// description arrives through `setServer`; its records through the
// `serverRecords` function the harness exposes. Every unhandled promise
// rejection of the page is kept, for the suite to fail on.
import * as sb from '../../../src/web/index.ts';
import type { ServerDescription, ServerRecord } from '../web-server.ts';

/** The longest any one wait of a test takes. */
const limit = 10_000;

/** The promise rejections nobody handled in this page, by their reason. */
const unhandledRejections: string[] = [];
globalThis.addEventListener('unhandledrejection', (event) => {
  const reason: unknown = event.reason;
  unhandledRejections.push(
    reason instanceof Error ? `${reason.name}: ${reason.message}` : String(reason),
  );
});

/** The server's description, with its certificate hash as bytes. */
type Server = ServerDescription & { readonly hash: Uint8Array };

let server: Server | undefined;

function currentServer(): Server {
  if (server === undefined) throw new Error('no server');
  return server;
}
const cleanups: (() => Promise<unknown>)[] = [];

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** [promise], failing after [ms] (the limit by default). */
function within<T>(promise: Promise<T>, ms = limit, what = 'a step'): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`${what} timed out after ${ms} ms`)), ms);
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

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

/** What an error is, for the checks: its class name, a QUIC kind, a stream code, a status. */
interface ErrorShape {
  readonly name: string;
  readonly message: string;
  readonly kind: string | undefined;
  readonly code: number | undefined;
  readonly status: number | undefined;
}

function shapeOf(error: unknown): ErrorShape {
  if (!(error instanceof Error)) {
    return {
      name: typeof error,
      message: String(error),
      kind: undefined,
      code: undefined,
      status: undefined,
    };
  }
  return {
    name: error.name,
    message: error.message,
    kind: error instanceof sb.QuicError ? error.kind : undefined,
    code:
      error instanceof sb.QuicStreamReset || error instanceof sb.QuicStreamStopped
        ? error.code
        : undefined,
    status: error instanceof sb.SwitchboardError ? error.status.code : undefined,
  };
}

function same(a: unknown, b: unknown): boolean {
  const plain = (v: unknown): unknown => (v instanceof Uint8Array ? [...v] : v);
  return (
    JSON.stringify(a, (_, v: unknown) => plain(v)) ===
    JSON.stringify(b, (_, v: unknown) => plain(v))
  );
}

const h = {
  limit,
  within,
  sleep,

  /** Fails unless [condition]. */
  ok(condition: unknown, what: string): void {
    if (!condition) throw new Error(`expected ${what}`);
  },

  /** Fails unless [actual] equals [expected] (as JSON; bytes as arrays). */
  eq(actual: unknown, expected: unknown, what = 'the value'): void {
    if (!same(actual, expected)) {
      throw new Error(
        `${what}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`,
      );
    }
  },

  /** The error [action] throws synchronously; fails when it throws none. */
  throws(action: () => unknown, what = 'the call'): ErrorShape {
    try {
      action();
    } catch (error) {
      return shapeOf(error);
    }
    throw new Error(`${what} did not throw`);
  },

  /** The error [promise] rejects with; fails when it resolves or takes longer than the limit. */
  async rejects(promise: Promise<unknown>, what = 'the promise'): Promise<ErrorShape> {
    try {
      await within(promise, limit, what);
    } catch (error) {
      if (error instanceof Error && error.message === `${what} timed out after ${limit} ms`) {
        throw error;
      }
      return shapeOf(error);
    }
    throw new Error(`${what} resolved`);
  },

  /** Whether [promise] is still pending after [ms] (false as soon as it settles). */
  pendingAfter(promise: Promise<unknown>, ms: number): Promise<boolean> {
    return new Promise((resolve) => {
      const timer = setTimeout(() => resolve(true), ms);
      const settled = (): void => {
        clearTimeout(timer);
        resolve(false);
      };
      promise.then(settled, settled);
    });
  },

  /** Waits until [condition] holds, or fails after [ms]. */
  async until(condition: () => boolean, ms = limit): Promise<void> {
    const deadline = performance.now() + ms;
    while (!condition()) {
      if (performance.now() > deadline) throw new Error('timed out waiting');
      await sleep(10);
    }
  },

  bytes: (text: string): Uint8Array => encoder.encode(text),
  text: (bytes: Uint8Array): string => decoder.decode(bytes),

  /** The pattern the server sends. */
  pattern(n: number): Uint8Array {
    const out = new Uint8Array(n);
    for (let i = 0; i < n; i++) out[i] = (i * 31 + (i >> 8)) & 0xff;
    return out;
  },

  /** An id for the records of one test. */
  newId(): string {
    return Array.from({ length: 12 }, () => Math.floor(Math.random() * 36).toString(36)).join('');
  },

  /** Everything [stream] delivers up to FIN. */
  async readAll(stream: sb.QuicStream, max = 65536): Promise<Uint8Array> {
    const chunks: Uint8Array[] = [];
    for (;;) {
      const chunk = await within(stream.read(max), limit, 'a read');
      if (chunk === undefined) return sb.concatBytes(...chunks);
      chunks.push(chunk);
    }
  },

  /** Runs [cleanup] after the test, as Dart's `addTearDown`. */
  addTearDown(cleanup: () => Promise<unknown>): void {
    cleanups.push(cleanup);
  },

  /** Runs the tear-downs of the test, the last added first. */
  async tearDown(): Promise<void> {
    const errors: unknown[] = [];
    for (let c = cleanups.pop(); c !== undefined; c = cleanups.pop()) {
      try {
        await within(c(), limit, 'a tear-down');
      } catch (error) {
        errors.push(error);
      }
    }
    if (errors.length > 0) throw errors[0];
  },

  setServer(description: ServerDescription): void {
    server = { ...description, hash: sb.hexBytes(description.certificateSha256) };
  },

  /** The server. */
  get server(): Server {
    return currentServer();
  },

  /** The client configuration of the server's sessions. */
  config(handshake = limit): sb.QuicClientConfig {
    return {
      alpn: ['switchboard'],
      serverCertificateHashes: [currentServer().hash],
      handshakeTimeout: handshake,
    };
  },

  /** [base] (the provider listener by default) with the query of [kind] and [id]. */
  url(kind: string, id: string, base?: string): string {
    const url = new URL(base ?? currentServer().provider);
    url.search = new URLSearchParams({ case: kind, id }).toString();
    return url.href;
  },

  /** A session of [kind] on [base] (the provider listener by default), whose records go under [id]; closed after the test. */
  async open(kind: string, id: string, base?: string): Promise<sb.WebTransportSession> {
    const session = await within(sb.webTransport().connect(h.url(kind, id, base), h.config()));
    h.addTearDown(() => session.close(0, ''));
    return session;
  },

  /** What the server recorded under [id]. */
  report(id: string): Promise<readonly ServerRecord[]> {
    return (
      globalThis as unknown as { serverRecords(id: string): Promise<ServerRecord[]> }
    ).serverRecords(id);
  },

  /** The first record under [id] for which [test] holds, asking until the limit passed. */
  async reported(id: string, test: (event: ServerRecord) => boolean): Promise<ServerRecord> {
    const deadline = performance.now() + limit;
    for (;;) {
      const events = await h.report(id);
      const found = events.find(test);
      if (found !== undefined) return found;
      if (performance.now() > deadline) {
        throw new Error(`the server did not record it; it recorded ${JSON.stringify(events)}`);
      }
      await sleep(50);
    }
  },

  /** The unhandled rejections the page saw. */
  unhandledRejections(): readonly string[] {
    return [...unhandledRejections];
  },
};

/** The binding tests' helpers, as `web_binding_test.dart` defines them. */
const node = {
  /** The channel address of [service]. */
  address(service: string): sb.ChannelAddress {
    return new sb.ChannelAddress({ type: sb.Name.of(service) });
  },

  /**
   * A connection to the node, closed after the test. The node's bulk channels go to the Talk
   * channels they belong to: a Talk channel installs that routing on its connection.
   */
  async dial(options?: Partial<sb.MuxOptions>): Promise<sb.QuicBindingConnection> {
    const s = currentServer();
    const connection = await within(
      sb.connectQuic(s.node, sb.webTransport(), {
        options,
        serverCertificateHashes: [s.hash],
      }),
    );
    h.addTearDown(() => connection.close());
    return connection;
  },

  /** A Talk channel to [service] on [connection]. */
  talkTo(connection: sb.MuxConnection, service: string): sb.TalkChannel {
    return new sb.TalkChannel(connection.open(node.address(service).encode()));
  },

  /** A ReconnectingClient of the node over WebTransport, closed after the test. */
  client(): sb.ReconnectingClient {
    const s = currentServer();
    const client = new sb.ReconnectingClient(
      sb.webTransportConnector(s.node, { serverCertificateHashes: [s.hash] }),
      { initialBackoff: 50, maxBackoff: 1000, jitter: 0 },
    );
    h.addTearDown(() => client.close());
    return client;
  },
};

const api = { sb, h, node };

/** What a test's body receives in the page. */
export type PageApi = typeof api;

(globalThis as unknown as { switchboardTests: PageApi }).switchboardTests = api;
