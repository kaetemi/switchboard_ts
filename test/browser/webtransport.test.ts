// The web provider and the binding over it in a browser: Playwright's
// headless Chromium or Firefox (the project's browser, see
// `vitest.config.ts`) against the native WebTransport listeners of
// `web-server.ts` (a port of `tool/web_server.dart`) in this process, the
// page served by Vite from the sources. Ports of `switchboard_quic`'s
// `test/web_provider_test.dart` (the provider against the contract: the
// protocol, echo, read(max), FIN, resets and STOP_SENDING both ways with
// their codes, the application close both ways, draining, refusals, the
// handshake deadline, the exporter, the server's streams, the stream limit,
// backpressure, two sessions at once, the code ranges of the browser) and
// `test/web_binding_test.dart` (QuicBindingConnection through connectQuic:
// HELLO, a channel, Talk through the node's service, a nonce-bound IDENT,
// the node's GOAWAY, a close from the browser; ReconnectingClient over
// webTransportConnector, with openTalk and openPersistentTalk), against a
// Switchboard node's listener, with `test/web_harness.dart` as the guide
// (`page/page.ts`). Each test's body runs in the page (see `harness.ts`).
//
// Browsers differ in the optional members of the W3C API (`protocol`,
// `draining`, `exportKeyingMaterial`, `getStats`, `createSendGroup`,
// `sendOrder`): where Dart expects what Chrome 149 has, the tests check
// what the session's capabilities say, and the first test logs them. Where a
// browser's version has a member the tests need to reach a path (the
// `protocols` negotiation from Chromium 143 and Firefox 155, the exporter
// from Firefox 155), they also check that the session has it, so that the
// path is not skipped unnoticed. Where a browser departs from the drafts
// against the native server (Firefox 157: STOP_SENDING, the close,
// WT_DRAIN_SESSION and the exporter's context; see `departures`), the tests
// that touch it check the departure, and fail once the browser no longer
// departs (the close, a race, is checked whichever way it goes).
//
// Without the native addon, Playwright or the project's browser the file
// skips with the reason; with SWITCHBOARD_BROWSER=required (CI) it fails
// instead.
import { afterAll, afterEach, beforeAll, describe, expect, inject, test } from 'vitest';
import type { WebTransportCapabilities } from '../../src/web/quic/webtransport-provider.ts';
import {
  type BrowserTests,
  browserAvailability,
  browserTitles,
  startBrowserTests,
} from './harness.ts';

const requiredVariable = 'SWITCHBOARD_BROWSER';

const browserName = inject('browserName');
const title = browserTitles[browserName];
const availability = browserAvailability(browserName);
if (!availability.available && process.env[requiredVariable] === 'required') {
  throw new Error(`${requiredVariable}=required, but ${availability.reason}`);
}

let browser: BrowserTests | undefined;

/** Runs [body] in the page. */
const run: BrowserTests['run'] = (body, arg) => {
  if (browser === undefined) throw new Error('the browser did not start');
  return browser.run(body, arg);
};

/**
 * The first version of each browser with a capability the tests need to reach a path: the
 * `protocols` negotiation (`WT-Available-Protocols`, `wt.protocol`) and `exportKeyingMaterial`.
 */
const firstVersionWith: {
  readonly [name in typeof browserName]: Partial<Record<keyof WebTransportCapabilities, number>>;
} = {
  chromium: { protocol: 143 },
  firefox: { protocol: 155, exportKeyingMaterial: 155 },
};

/**
 * Where a browser departs from the drafts against the native server, as CI sees it, with the
 * cause read in the browser's sources. A test the departure touches checks the departure itself,
 * so that once the browser stops departing the test fails and the entry goes (with its line in
 * the browser notes of `docs/wiki/Runtimes-and-QUIC.md`).
 */
const departures = {
  /**
   * Cancelling a stream's readable sends no STOP_SENDING: the server never sees one, while the
   * page's own reads end with the code at once. (Firefox: the parent-process path from the
   * readable's `cancel()` to `SendStopSending` is not wired up.)
   */
  noStopSending: 'cancelling a readable sends no STOP_SENDING',
  /**
   * The page's close reaches the server only when it outruns the connection's: the browser closes
   * its dedicated connection as soon as the session has closed (Firefox:
   * `Http3Session::CloseWebTransportConn`), and the shim ends every session of a connection that
   * closes as lost at once, so a WT_CLOSE_SESSION that arrives with the connection's close is not
   * read. The server then sees a lost connection, without the code or the reason (CI saw both
   * outcomes). The page's own end is the local close either way. Native peers race the same way
   * when the dialler closes its last session (the WebTransport case of the interop close scenario).
   */
  closeRace:
    "the page's close races the browser's connection close, and may reach the server as a loss",
  /**
   * `draining` exists but the server's WT_DRAIN_SESSION does not resolve it: the browser reads only
   * WT_CLOSE_SESSION on the CONNECT stream and resolves `draining` on an HTTP/3 GOAWAY (Firefox:
   * neqo's `WebTransportFrame` and `drain_webtransport_sessions`), which the shim does not send.
   */
  drainIgnored: "the server's WT_DRAIN_SESSION does not resolve draining",
  /**
   * `exportKeyingMaterial` writes the session ID into the exporter's context as a QUIC varint
   * (Firefox: neqo's `webtransport_export_keying_material`) where the draft's "WebTransport
   * Exporter Context" has `WebTransport Session ID (64)`, as the shim writes it: the browser's
   * exporter is not the server's, and the server refuses an exporter-bound IDENT.
   */
  exporterContext: "the exporter's context carries the session ID as a varint",
} as const;

type Departure = keyof typeof departures;

/** The departures of each browser, as of the version Playwright brings (Firefox 157 with 1.64.1). */
const departuresOf: { readonly [name in typeof browserName]: readonly Departure[] } = {
  chromium: [],
  firefox: ['noStopSending', 'closeRace', 'drainIgnored', 'exporterContext'],
};

/** Whether the project's browser departs from the drafts in [departure]. */
const departs = (departure: Departure): boolean => departuresOf[browserName].includes(departure);

/** The capabilities the browser's version must have, which the session reports in [seen]. */
function expectCapabilitiesOfVersion(seen: WebTransportCapabilities): void {
  if (browser === undefined) throw new Error('the browser did not start');
  const major = Number.parseInt(browser.browserVersion, 10);
  for (const [capability, version] of Object.entries(firstVersionWith[browserName])) {
    if (major >= version) {
      expect(
        seen[capability as keyof WebTransportCapabilities],
        `${capability} in ${title} ${major}`,
      ).toBe(true);
    }
  }
}

describe.skipIf(!availability.available)(
  `WebTransport in ${title}${availability.available ? '' : ` (skipped: ${availability.reason})`}`,
  () => {
    beforeAll(async () => {
      if (!availability.available) return;
      browser = await startBrowserTests(availability.playwright, browserName);
      console.info(`browser tests: ${title} ${browser.browserVersion}`);
      for (const departure of departuresOf[browserName]) {
        console.info(`browser tests: ${title} departs: ${departures[departure]}`);
      }
    });

    afterEach(async () => {
      await browser?.tearDown();
    });

    afterAll(async () => {
      if (browser === undefined) return;
      try {
        // Unhandled rejections are reported after a task or two.
        const unhandled = await run(async ({ h }) => {
          await h.sleep(500);
          return h.unhandledRejections();
        });
        expect(unhandled).toEqual([]);
      } finally {
        await browser.close();
      }
    });

    describe('web provider', () => {
      test('the browser has WebTransport, and the server selects switchboard where the browser tells', async () => {
        const seen = await run(async ({ sb, h }) => {
          h.ok(sb.WebTransportProvider.isSupported, 'the WebTransport API');
          const session = await h.open('echo', h.newId());
          h.eq(
            session.protocol,
            session.capabilities.protocol ? 'switchboard' : undefined,
            'the protocol',
          );
          h.eq(session.isInitiator, true, 'isInitiator');
          h.eq(session.remote, `127.0.0.1:${new URL(h.server.provider).port}`, 'the remote');
          h.eq(session.maxIncomingStreams, undefined, 'maxIncomingStreams');
          h.eq(session.peerCertificateHash, undefined, 'peerCertificateHash');
          return {
            capabilities: session.capabilities,
            maxStreamErrorCode: sb.WebTransportProvider.maxStreamErrorCode,
          };
        });
        console.info(
          `web provider: ${title} ${JSON.stringify(seen.capabilities)}, stream error codes up to ${seen.maxStreamErrorCode}`,
        );
        expectCapabilitiesOfVersion(seen.capabilities);
      });

      test('a session that offers no protocol: none selected', async () => {
        await run(async ({ sb, h }) => {
          const session = await h.within(
            sb.webTransport().connect(h.url('echo', h.newId()), {
              alpn: [],
              serverCertificateHashes: [h.server.hash],
            }),
          );
          h.eq(session.protocol, undefined, 'the protocol');
          await session.close(0, '');
        });
      });

      test('protocols: the browser offers them, the server selects the first it serves and refuses a session offering none it serves', async () => {
        const seen = await run(async ({ sb, h }) => {
          const id = h.newId();
          const session = await h.within(
            sb.webTransport().connect(h.url('echo', id), {
              alpn: ['other', 'switchboard'],
              serverCertificateHashes: [h.server.hash],
            }),
          );
          h.addTearDown(() => session.close(0, ''));
          const request = await h.reported(id, (e) => e['event'] === 'session');
          if (session.capabilities.protocol) {
            // WT-Available-Protocols, in the order given; the answer's WT-Protocol.
            h.eq(request['protocols'], ['other', 'switchboard'], 'what the server saw offered');
            h.eq(request['protocol'], 'switchboard', 'what the server selected');
            h.eq(session.protocol, 'switchboard', 'what the browser reports');
            const refused = await h.rejects(
              sb.webTransport().connect(h.url('echo', h.newId()), {
                alpn: ['other'],
                serverCertificateHashes: [h.server.hash],
              }),
            );
            h.eq(refused.kind, 'handshakeFailed', 'a session offering only another protocol');
          } else {
            // A browser without the protocols option offers none: the server selects none.
            h.eq(request['protocols'], [], 'what the server saw offered');
            h.eq(request['protocol'], null, 'what the server selected');
            h.eq(session.protocol, undefined, 'what the browser reports');
          }
          return { capabilities: session.capabilities, draft: request['draft'] };
        });
        console.info(`web provider: ${title} signals the WebTransport draft ${seen.draft}`);
        expectCapabilitiesOfVersion(seen.capabilities);
      });

      test('a stream echoed, FIN both ways, acknowledgement', async () => {
        await run(async ({ sb, h }) => {
          const id = h.newId();
          const session = await h.open('echo', id);
          const stream = await session.openStream();
          stream.priority = sb.quicOrdinaryPriority;
          await stream.write(h.bytes('hello '));
          await stream.write(h.bytes('switchboard'));
          await stream.finish();
          h.eq(await h.within(stream.stopped), undefined, 'stopped');
          h.eq(h.text(await h.readAll(stream)), 'hello switchboard', 'the echo');
          h.eq(await stream.read(10), undefined, 'a read after FIN');
          h.eq(await stream.read(10), undefined, 'another read after FIN');
          h.eq((await h.rejects(stream.write(h.bytes('more')))).name, 'Error', 'a late write');
          await h.reported(id, (e) => e['event'] === 'fin' && e['bytes'] === 17);
        });
      });

      test('read(max) returns at most max bytes, keeps the rest of a chunk, then FIN', async () => {
        await run(async ({ sb, h }) => {
          const session = await h.open('send', h.newId());
          const stream = await session.openStream();
          const size = 300_000;
          const header = new Uint8Array(4);
          new DataView(header.buffer).setUint32(0, size);
          await stream.write(header);
          await stream.finish();
          const chunks: Uint8Array[] = [];
          const sizes = [1, 7, 4, 1000, 65536, 3, 100_000];
          for (let i = 0; ; i++) {
            const max = sizes[i % sizes.length] as number;
            const chunk = await h.within(stream.read(max));
            if (chunk === undefined) break;
            h.ok(chunk.length > 0 && chunk.length <= max, `1 to ${max} bytes, not ${chunk.length}`);
            chunks.push(chunk);
          }
          h.ok(sb.bytesEqual(sb.concatBytes(...chunks), h.pattern(size)), 'the pattern');
          h.eq(await stream.read(1), undefined, 'a read after FIN');
          h.eq(h.throws(() => stream.read(0)).name, 'RangeError', 'read(0)');
        });
      });

      test("a quiet session outlives the server's handshake deadline (which bounds its HTTP/3 exchanges)", async () => {
        await run(async ({ h }) => {
          const session = await h.open('echo', h.newId());
          await h.sleep(h.server.handshakeTimeoutMs + 1500);
          const stream = await session.openStream();
          await stream.write(h.bytes('still here'));
          await stream.finish();
          h.eq(h.text(await h.readAll(stream)), 'still here', 'the echo');
          h.ok(await h.pendingAfter(session.done, 0), 'the session still open');
        });
      });

      test('only one read may be pending', async () => {
        await run(async ({ h }) => {
          const session = await h.open('hold', h.newId());
          const stream = await session.openStream();
          await stream.write(h.bytes('x'));
          const pending = h.rejects(stream.read(10));
          h.eq(h.throws(() => stream.read(10)).name, 'Error', 'a second read');
          await session.close(0, '');
          h.eq((await pending).kind, 'sessionEnded', 'the pending read');
        });
      });

      test('reset(42) reaches the server as 42; writes after it are errors', async () => {
        await run(async ({ h }) => {
          const id = h.newId();
          const session = await h.open('sink', id);
          const stream = await session.openStream();
          await stream.write(h.bytes('abc'));
          // Once the server has the stream: quinn drops what it buffered of a stream once its reset
          // arrives, so a reset that reaches the server before it read the stream's header loses
          // the header, and without RESET_STREAM_AT the server cannot tell which session the
          // stream belonged to (Chromium sends the header, the data and the reset about a
          // millisecond apart).
          await h.reported(id, (e) => e['event'] === 'stream');
          stream.reset(42);
          stream.reset(43); // A no-op.
          h.eq(await h.within(stream.stopped), undefined, 'stopped');
          h.eq((await h.rejects(stream.write(h.bytes('d')))).name, 'Error', 'a write');
          h.eq((await h.rejects(stream.finish())).name, 'Error', 'a finish');
          await h.reported(id, (e) => e['event'] === 'reset' && e['code'] === 42);
        });
      });

      test("the server's reset arrives as 14, also for a reader that is not reading", async () => {
        await run(async ({ h }) => {
          const session = await h.open('reset', h.newId());
          const stream = await session.openStream();
          await stream.write(h.bytes('go'));
          // Nobody reads: the reset is still observed.
          h.eq(await h.within(stream.resetReceived), 14, 'resetReceived');
          for (const what of ['a read', 'another read']) {
            const e = await h.rejects(stream.read(100));
            h.eq([e.name, e.code], ['QuicStreamReset', 14], what);
          }
        });
      });

      test('stop(5) reaches the server as STOP_SENDING 5, and ends the read direction at once', async () => {
        await run(async ({ h }, noStopSending) => {
          const id = h.newId();
          const session = await h.open('sink', id);
          const stream = await session.openStream();
          await stream.write(h.bytes('x'));
          const pending = h.rejects(stream.read(100));
          stream.stop(5);
          stream.stop(6); // A no-op.
          const first = await pending;
          h.eq([first.name, first.code], ['QuicStreamReset', 5], 'the pending read');
          const later = await h.rejects(stream.read(100));
          h.eq([later.name, later.code], ['QuicStreamReset', 5], 'a later read');
          if (noStopSending) {
            // The departure: nothing reaches the server (which records a STOP_SENDING within
            // milliseconds where the browser sends one).
            await h.sleep(1000);
            const stops = (await h.report(id)).filter((e) => e['event'] === 'stopped');
            h.eq(stops, [], 'what the server saw');
          } else {
            await h.reported(id, (e) => e['event'] === 'stopped' && e['code'] === 5);
          }
          h.ok(await h.pendingAfter(stream.resetReceived, 0), 'resetReceived pending');
        }, departs('noStopSending'));
      });

      test("the server's STOP_SENDING 9 resolves stopped with 9 and fails writes", async () => {
        await run(async ({ h }) => {
          const session = await h.open('stop', h.newId());
          const stream = await session.openStream();
          await stream.write(h.bytes('first'));
          h.eq(await h.within(stream.stopped), 9, 'stopped');
          const write = await h.rejects(stream.write(h.bytes('second')));
          h.eq([write.name, write.code], ['QuicStreamStopped', 9], 'a write');
          h.eq((await h.rejects(stream.finish())).name, 'QuicStreamStopped', 'a finish');
        });
      });

      test('close(7, reason) reaches the server; the session ends as a local application close', async () => {
        const reached = await run(async ({ h }, closeRace) => {
          const id = h.newId();
          const session = await h.open('sink', id);
          const stream = await session.openStream();
          await stream.write(h.bytes('x'));
          await h.within(session.close(7, 'browser done'));
          const end = await session.done;
          h.eq(end, { code: 7, reason: 'browser done', byPeer: false, lost: false }, 'the end');
          await session.close(8, 'again'); // Idempotent.
          h.eq((await h.rejects(session.openStream())).kind, 'sessionEnded', 'openStream');
          h.eq((await h.rejects(stream.write(h.bytes('y')))).kind, 'sessionEnded', 'a write');
          const seen = await h.reported(id, (e) => e['event'] === 'end');
          if (closeRace && seen['lost'] === true) {
            // The departure: the connection's close came first.
            h.eq([seen['code'], seen['byPeer']], [null, false], 'seen');
            return false;
          }
          h.eq([seen['code'], seen['reason'], seen['byPeer']], [7, 'browser done', true], 'seen');
          return true;
        }, departs('closeRace'));
        console.info(
          `web provider: ${title}'s close(7) ${reached ? 'reached' : 'did not reach'} the server`,
        );
      });

      test('a reason is cut to 1024 bytes of UTF-8 at a character boundary', async () => {
        const reached = await run(async ({ h }, closeRace) => {
          const id = h.newId();
          const session = await h.open('sink', id);
          await session.close(300, 'é'.repeat(600));
          // The browser reports the close it sent: the reason as the provider cut it.
          h.eq(
            await session.done,
            { code: 300, reason: 'é'.repeat(512), byPeer: false, lost: false },
            'the end',
          );
          h.eq(h.throws(() => session.close(-1, '')).name, 'RangeError', 'close(-1)');
          h.eq(h.throws(() => session.close(0x100000000, '')).name, 'RangeError', 'close(2^32)');
          const seen = await h.reported(id, (e) => e['event'] === 'end');
          if (closeRace && seen['lost'] === true) {
            // The departure: the connection's close came first.
            h.eq([seen['code'], seen['byPeer']], [null, false], 'seen');
            return false;
          }
          h.eq(
            [seen['code'], seen['reason'], seen['byPeer']],
            [300, 'é'.repeat(512), true],
            'seen',
          );
          return true;
        }, departs('closeRace'));
        console.info(
          `web provider: ${title}'s close(300) ${reached ? 'reached' : 'did not reach'} the server`,
        );
      });

      test("the server's close is the session's end with its code and reason; what is pending fails", async () => {
        await run(async ({ h }) => {
          const session = await h.open('close', h.newId());
          const stream = await session.openStream();
          const reset = h.rejects(stream.resetReceived);
          const stopped = h.rejects(stream.stopped);
          const pending = h.rejects(stream.read(10));
          await stream.write(h.bytes('x'));
          const end = await h.within(session.done);
          h.eq(end, { code: 9, reason: 'server bye', byPeer: true, lost: false }, 'the end');
          h.eq((await pending).kind, 'sessionEnded', 'the pending read');
          h.eq((await reset).kind, 'sessionEnded', 'resetReceived');
          h.eq((await stopped).kind, 'sessionEnded', 'stopped');
          h.eq((await h.rejects(session.openStream())).kind, 'sessionEnded', 'openStream');
          const exporting = session.exportKeyingMaterial(
            'switchboard-ident',
            new Uint8Array(0),
            32,
          );
          h.eq((await h.rejects(exporting)).kind, 'sessionEnded', 'the exporter');
        });
      });

      test("draining resolves on the server's drain where the browser has the promise, and stays pending where it has not", async () => {
        await run(async ({ h }, drainIgnored) => {
          const id = h.newId();
          const session = await h.open('drain', id);
          const stream = await session.openStream();
          await stream.write(h.bytes('x'));
          await h.reported(id, (e) => e['event'] === 'drain sent');
          if (drainIgnored) {
            // The departure: the browser has the promise, and leaves it pending.
            h.ok(session.capabilities.draining, 'the draining promise');
            h.ok(await h.pendingAfter(session.draining, 300), 'draining pending');
          } else if (session.capabilities.draining) {
            await h.within(session.draining);
          } else {
            h.ok(await h.pendingAfter(session.draining, 300), 'draining pending');
          }
        }, departs('drainIgnored'));
      });

      test('a refused path, a wrong certificate hash, a closed port: the connect fails', async () => {
        await run(async ({ sb, h }) => {
          const provider = sb.webTransport();
          const refused = new URL(h.server.provider);
          refused.pathname = '/nope';
          const path = await h.rejects(provider.connect(refused.href, h.config()));
          h.eq(path.kind, 'handshakeFailed', 'a refused path');
          const hash = await h.rejects(
            provider.connect(h.server.provider, {
              alpn: ['switchboard'],
              serverCertificateHashes: [new Uint8Array(32)],
            }),
          );
          h.eq(hash.kind, 'handshakeFailed', 'a wrong hash');
          const closed = new URL(h.server.provider);
          closed.port = '1';
          const port = await h.rejects(provider.connect(closed.href, h.config()));
          h.eq(port.kind, 'handshakeFailed', 'a closed port');
        });
      });

      test('the handshake deadline: a server that never answers', async () => {
        await run(async ({ sb, h }) => {
          const start = performance.now();
          const error = await h.rejects(sb.webTransport().connect(h.server.silent, h.config(500)));
          h.eq(error.kind, 'unreachable', 'the error');
          h.ok(performance.now() - start < 5000, 'a failure within 5 s');
        });
      });

      test('schemes and arguments', async () => {
        await run(async ({ sb, h }) => {
          const provider = sb.webTransport();
          const config = { alpn: ['switchboard'] };
          for (const uri of ['quic://127.0.0.1:4433', 'wss://127.0.0.1/sb']) {
            h.eq((await h.rejects(provider.connect(uri, config))).kind, 'unsupportedScheme', uri);
          }
          // Dart's ArgumentErrors: a RangeError for a value out of range, a TypeError for a
          // malformed one.
          const negative = provider.connect('https://127.0.0.1:1/sb', {
            ...config,
            idleTimeout: -1,
          });
          h.eq((await h.rejects(negative)).name, 'RangeError', 'a negative idle timeout');
          const short = provider.connect('https://127.0.0.1:1/sb', {
            ...config,
            serverCertificateHashes: [new Uint8Array(16)],
          });
          h.eq((await h.rejects(short)).name, 'RangeError', 'a 16-byte hash');
          // Chromium before 143 sends no WT-Available-Protocols and refuses nothing: the provider
          // checks the protocols itself.
          const twice = provider.connect('https://127.0.0.1:1/sb', { alpn: ['a', 'a'] });
          h.eq((await h.rejects(twice)).name, 'TypeError', 'a protocol offered twice');
        });
      });

      test("the exporter: undefined without exportKeyingMaterial, the server's value with it", async () => {
        await run(async ({ sb, h }, exporterContext) => {
          const id = h.newId();
          const session = await h.open('sink', id);
          const exporter = await session.exportKeyingMaterial(
            'switchboard-ident',
            new Uint8Array(0),
            32,
          );
          const seen = await h.reported(id, (e) => e['event'] === 'exporter');
          if (!session.capabilities.exportKeyingMaterial) {
            h.eq(exporter, undefined, 'the exporter');
            h.eq((seen['value'] as string).length, 64, "the server's exporter");
          } else {
            h.eq(exporter?.length, 32, 'the exporter');
            const again = await session.exportKeyingMaterial(
              'switchboard-ident',
              new Uint8Array(0),
              32,
            );
            h.eq(again, exporter, 'the exporter asked again');
            const value = sb.hexString(exporter ?? []).replaceAll(' ', '');
            if (exporterContext) {
              // The departure: the browser derives another value.
              h.eq((seen['value'] as string).length, 64, "the server's exporter");
              h.ok(value !== seen['value'], "a value that is not the server's");
            } else {
              h.eq(value, seen['value'], 'the value');
            }
          }
          const zero = session.exportKeyingMaterial('x', new Uint8Array(0), 0);
          h.eq((await h.rejects(zero)).name, 'RangeError', 'a length of 0');
        }, departs('exporterContext'));
      });

      test('a stream the server opens arrives on incomingStreams', async () => {
        await run(async ({ h }) => {
          const session = await h.open('open', h.newId());
          const next = await h.within(session.incomingStreams[Symbol.asyncIterator]().next());
          if (next.done === true) throw new Error('no stream');
          h.eq(h.text(await h.readAll(next.value)), 'from server', 'the stream');
          await next.value.finish();
        });
      });

      test('incomingUnidirectionalStreams: the native server cannot open one; the iteration ends with the session', async () => {
        await run(async ({ h }) => {
          const session = await h.open('hold', h.newId());
          const counting = (async () => {
            let n = 0;
            for await (const _ of session.incomingUnidirectionalStreams) n++;
            return n;
          })();
          await session.close(0, '');
          h.eq(await h.within(counting), 0, 'the streams');
        });
      });

      test('openStream waits for stream credit, in order', async () => {
        await run(async ({ h }) => {
          const session = await h.open('echo', h.newId(), h.server.limited);
          const first = await session.openStream();
          const second = await session.openStream();
          await first.write(h.bytes('1'));
          await second.write(h.bytes('2'));
          const third = session.openStream();
          const fourth = session.openStream();
          h.ok(await h.pendingAfter(third, 500), 'the third open waiting');
          // The first stream ends both ways: its credit returns.
          await first.finish();
          h.eq(h.text(await h.readAll(first)), '1', 'the first');
          const opened = await h.within(third);
          await opened.write(h.bytes('3'));
          await opened.finish();
          h.ok(await h.pendingAfter(fourth, 0), 'the fourth open waiting');
          await second.finish();
          h.eq(h.text(await h.readAll(second)), '2', 'the second');
          h.eq(h.text(await h.readAll(opened)), '3', 'the third');
          const last = await h.within(fourth);
          await last.write(h.bytes('4'));
          await last.finish();
          h.eq(h.text(await h.readAll(last)), '4', 'the fourth');
        });
      });

      test('writes wait while the server reads nothing, and fail when the session ends', async () => {
        const written = await run(async ({ h }) => {
          const session = await h.open('hold', h.newId());
          const stream = await session.openStream();
          const chunk = new Uint8Array(65536);
          let written = 0;
          let stalled: Promise<void> | undefined;
          for (let i = 0; i < 256; i++) {
            const w = stream.write(chunk);
            if (await h.pendingAfter(w, 300)) {
              stalled = w;
              break;
            }
            written += chunk.length;
          }
          if (stalled === undefined) throw new Error(`wrote ${written} bytes unhindered`);
          h.ok(written < 16 << 20, `fewer than 16 MiB written, not ${written}`);
          await session.close(0, '');
          h.eq((await h.rejects(stalled)).kind, 'sessionEnded', 'the stalled write');
          return written;
        });
        console.info(`web provider: writes stalled after ${written} bytes`);
      });

      test('two sessions at once', async () => {
        await run(async ({ h }) => {
          const a = await h.open('echo', h.newId());
          const b = await h.open('echo', h.newId());
          const onA = await a.openStream();
          const onB = await b.openStream();
          await onA.write(h.bytes('alpha'));
          await onB.write(h.bytes('beta'));
          await onB.finish();
          await onA.finish();
          h.eq(h.text(await h.readAll(onB)), 'beta', 'b');
          h.eq(h.text(await h.readAll(onA)), 'alpha', 'a');
          await a.close(1, 'a');
          h.ok(await h.pendingAfter(b.done, 0), 'b still open');
          const again = await b.openStream();
          await again.write(h.bytes('gamma'));
          await again.finish();
          h.eq(h.text(await h.readAll(again)), 'gamma', 'b again');
        });
      });

      test('stream codes: what the browser carries, and no more', async () => {
        await run(async ({ sb, h }) => {
          const session = await h.open('hold', h.newId());
          const stream = await session.openStream();
          const max = sb.WebTransportProvider.maxStreamErrorCode;
          h.ok(max >= 255, `codes up to 255 at least, not ${max}`);
          h.eq(h.throws(() => stream.reset(-1)).name, 'RangeError', 'reset(-1)');
          h.eq(h.throws(() => stream.stop(0x100000000)).name, 'RangeError', 'stop(2^32)');
          if (max < 0xffffffff) {
            h.eq(h.throws(() => stream.reset(max + 1)).name, 'RangeError', 'reset(max + 1)');
            h.eq(h.throws(() => stream.stop(max + 1)).name, 'RangeError', 'stop(max + 1)');
          }
          stream.reset(255);
          stream.stop(255);
        });
      });

      test('priorities: send orders where the browser has them', async () => {
        const seen = await run(async ({ sb, h }) => {
          h.eq(sb.WebTransportProvider.sendOrderOf(sb.quicControlPriority), 1, 'control');
          h.eq(sb.WebTransportProvider.sendOrderOf(sb.quicOrdinaryPriority), 0, 'ordinary');
          h.eq(sb.WebTransportProvider.sendOrderOf(sb.quicBulkPriority), 0, 'bulk');
          const session = await h.open('echo', h.newId());
          const stream = await session.openStream();
          if (!(stream instanceof sb.WebTransportStream))
            throw new Error('not a WebTransportStream');
          stream.priority = sb.quicControlPriority;
          await stream.write(h.bytes('p'));
          await stream.finish();
          h.eq(h.text(await h.readAll(stream)), 'p', 'the echo');
          return { sendOrders: stream.hasSendOrder, sendGroups: session.hasSendGroups };
        });
        console.info(
          `web provider: send orders ${seen.sendOrders}, send groups ${seen.sendGroups}`,
        );
      });

      test("stats where the browser's getStats gives them, undefined where it has none or fails", async () => {
        const seen = await run(async ({ h }) => {
          const session = await h.open('echo', h.newId());
          const stats = await session.stats();
          // What the browser's own getStats does, on a session of the page's own.
          let raw = 'no getStats';
          if (session.capabilities.getStats) {
            const transport = new WebTransport(h.url('echo', h.newId()), {
              protocols: ['switchboard'],
              serverCertificateHashes: [
                { algorithm: 'sha-256', value: new Uint8Array(h.server.hash) },
              ],
            }) as WebTransport & { getStats(): Promise<unknown> };
            transport.closed.catch(() => {});
            try {
              await h.within(transport.ready);
              const value = await h.within(transport.getStats());
              raw = typeof value === 'object' && value !== null ? 'stats' : `a ${typeof value}`;
            } catch (error) {
              raw = error instanceof Error ? `${error.name}: ${error.message}` : String(error);
            } finally {
              transport.close();
            }
          }
          h.eq(stats !== undefined, raw === 'stats', `stats (getStats: ${raw})`);
          return { raw, stats };
        });
        console.info(`web provider: ${title} getStats: ${seen.raw}, ${JSON.stringify(seen.stats)}`);
      });
    });

    describe('the binding over the web provider', () => {
      test('the connection: HELLO both ways, the session WebTransport, the certificate authenticating the node', async () => {
        await run(async ({ sb, h, node }) => {
          const connection = await node.dial();
          await h.within(connection.ping());
          h.ok(connection.peerHello !== undefined, 'the peer HELLO');
          h.eq(connection.isInitiator, true, 'isInitiator');
          h.eq(connection.isWebTransport, true, 'isWebTransport');
          const session = connection.session;
          if (!(session instanceof sb.WebTransportSession)) throw new Error('not the web provider');
          h.eq(
            session.protocol,
            session.capabilities.protocol ? 'switchboard' : undefined,
            'the protocol',
          );
          h.eq(connection.quicOptions.identity.tlsAuthenticatesPeer, true, 'tlsAuthenticatesPeer');
          h.eq(connection.peerAuthenticated, true, 'peerAuthenticated');
          // Exporter-bound only where the browser has exportKeyingMaterial.
          h.eq(connection.hasExporter, session.capabilities.exportKeyingMaterial, 'hasExporter');
          h.eq(connection.exporterBound, connection.hasExporter, 'exporterBound');
        });
      });

      test("a channel and a Talk request through the node's service", async () => {
        await run(async ({ sb, h, node }) => {
          const connection = await node.dial();
          const talk = node.talkTo(connection, 'echo');
          h.ok(talk.raw instanceof sb.QuicBindingChannel, 'a QUIC channel');
          const reply = await h.within(talk.request('ECHO', h.bytes('hello')));
          h.eq(h.text(reply.payload), 'hello', 'the reply');
          const big = h.pattern(200_000);
          const bulk = await h.within(talk.request('BIG', big));
          h.ok(sb.bytesEqual(bulk.payload, big), 'the bulk reply');
          await h.within(talk.close());
          h.eq(connection.openChannelCount, 0, 'openChannelCount');
        });
      });

      test('an IDENT from the browser (exporter-bound where it exports keying material, nonce-bound elsewhere), accepted by a listener with QuicIdentityRules.none', async () => {
        await run(async ({ sb, h, node }, exporterContext) => {
          const connection = await node.dial();
          const key = await sb.Ed25519HolderKey.generate();
          const issuing = node.talkTo(connection, 'issue');
          const issued = await h.within(issuing.request('ISSUE', key.publicKey));
          const credential = sb.Credential.decode(issued.payload);
          await issuing.close();
          h.eq(credential.identity, 'browser-1', 'the identity issued');
          if (exporterContext) {
            // The departure: the proof is bound to the browser's exporter, which is not the
            // node's, and the node refuses it.
            h.eq(connection.exporterBound, true, 'exporterBound');
            const refused = await h.rejects(connection.identify(credential, { holderKey: key }));
            h.eq(refused.status, sb.StatusCode.unauthenticated, 'the IDENT');
            return;
          }
          await h.within(connection.identify(credential, { holderKey: key }));
          const asking = node.talkTo(connection, 'control');
          const who = await h.within(asking.request('WHOAMI', new Uint8Array(0)));
          h.eq(h.text(who.payload), 'browser-1', 'WHOAMI');
          await asking.close();
        }, departs('exporterContext'));
      });

      test("the node's GOAWAY: the browser stops opening, finishes, and the connection closes", async () => {
        await run(async ({ sb, h, node }) => {
          const id = h.newId();
          const connection = await node.dial();
          const asking = node.talkTo(connection, 'control');
          await h.within(asking.request('TAG', h.bytes(id)));
          await h.within(asking.request('GOAWAY', new Uint8Array(0)));
          const status = await h.within(connection.peerGoAwayStatus);
          h.eq(status.known, sb.StatusCode.goingAway, 'the GOAWAY');
          h.eq(connection.peerGoingAway, true, 'peerGoingAway');
          const refused = h.throws(() => connection.open(node.address('echo').encode()));
          h.eq(refused.name, 'SwitchboardError', 'an open after it');
          await asking.close();
          const done = await h.within(connection.done);
          h.eq(done.known, sb.StatusCode.goingAway, 'the end of the connection');
          const end = await h.within(connection.session.done);
          h.eq(end.lost, false, 'a session lost');
          const seen = await h.reported(id, (e) => e['event'] === 'connection done');
          h.eq(seen['code'], sb.StatusCode.goingAway, 'what the node saw');
        });
      });

      test('a close from the browser: the node reads it as GOING_AWAY', async () => {
        const reached = await run(async ({ sb, h, node }, closeRace) => {
          const id = h.newId();
          const connection = await node.dial();
          const asking = node.talkTo(connection, 'control');
          await h.within(asking.request('TAG', h.bytes(id)));
          await h.within(asking.close());
          await h.within(connection.close());
          const end = await h.within(connection.session.done);
          h.eq([end.code, end.byPeer], [0, false], 'the end of the session');
          h.eq(connection.isOpen, false, 'isOpen');
          const seen = await h.reported(id, (e) => e['event'] === 'connection done');
          if (closeRace && seen['code'] === sb.StatusCode.connectionLost) {
            // The departure: the connection's close came first, and close() sends no GOAWAY
            // that would have told the node otherwise (design section 13, items 7 and 8).
            return false;
          }
          h.eq(seen['code'], sb.StatusCode.goingAway, 'what the node saw');
          return true;
        }, departs('closeRace'));
        console.info(
          `the binding: ${title}'s close ${reached ? 'reached' : 'did not reach'} the node as GOING_AWAY`,
        );
      });

      describe('ReconnectingClient over webTransportConnector', () => {
        test('connects and serves a request', async () => {
          await run(async ({ sb, h, node }) => {
            const client = node.client();
            const talk = await h.within(client.openTalk(node.address('echo')));
            h.ok(client.connection instanceof sb.QuicBindingConnection, 'a QUIC connection');
            const reply = await h.within(talk.request('ECHO', h.bytes('one')));
            h.eq(h.text(reply.payload), 'one', 'the reply');
            await talk.close();
          });
        });

        test('reconnects after the node closes the connection, and serves again', async () => {
          await run(async ({ sb, h, node }) => {
            const client = node.client();
            const ready: InstanceType<typeof sb.TalkChannel>[] = [];
            client.openPersistentTalk(node.address('echo'), async (talk) => {
              ready.push(talk);
            });
            await h.until(() => ready.length === 1);
            const first = client.connection;
            const latest = () => ready[ready.length - 1] as (typeof ready)[number];
            const a = await h.within(latest().request('ECHO', h.bytes('a')));
            h.eq(h.text(a.payload), 'a', 'the first reply');
            const asking = await h.within(client.openTalk(node.address('control')));
            await asking.send('CLOSE', new Uint8Array(0));
            await h.until(() => ready.length === 2);
            h.ok(client.connection !== first, 'a new connection');
            h.ok(client.state.isConnected, 'connected');
            const b = await h.within(latest().request('ECHO', h.bytes('b')));
            h.eq(h.text(b.payload), 'b', 'the second reply');
          });
        });

        test("moves to a new connection on the node's GOAWAY", async () => {
          await run(async ({ sb, h, node }) => {
            const client = node.client();
            const talk = await h.within(client.openTalk(node.address('control')));
            const first = client.connection;
            if (first === undefined) throw new Error('no connection');
            await h.within(talk.request('GOAWAY', new Uint8Array(0)));
            await talk.close();
            await h.until(() => client.connection !== undefined && client.connection !== first);
            const again = await h.within(client.openTalk(node.address('echo')));
            const reply = await h.within(again.request('ECHO', h.bytes('c')));
            h.eq(h.text(reply.payload), 'c', 'the reply');
            await again.close();
            h.eq(
              (await h.within(first.done)).known,
              sb.StatusCode.goingAway,
              'the first connection',
            );
          });
        });
      });
    });
  },
);
