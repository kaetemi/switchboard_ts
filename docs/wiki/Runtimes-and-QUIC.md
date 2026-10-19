# Runtimes and QUIC

## Runtimes

| Runtime | Import | Transports | QUIC and WebTransport | Tested |
| --- | --- | --- | --- | --- |
| Node.js 22, 24, 26 | `@kaetemi/switchboard/node` | TCP, WebSocket client and server, in-memory | native provider | CI: unit and integration |
| Bun 1.x | `@kaetemi/switchboard/node` | same, through Bun's Node compatibility | native provider | CI: unit; TCP, WebSocket and QUIC by hand |
| Deno 2.x | `@kaetemi/switchboard/node` | same, through `node:` compatibility | native provider | CI (Deno 2.9): unit; the built package's TCP, WebSocket, QUIC and WebTransport round trips |
| Browsers | `@kaetemi/switchboard/web` | WebSocket | WebTransport API | CI: Chromium 156 and Firefox 157 |
| Edge runtimes | `@kaetemi/switchboard` with your own transport | WebSocket where offered | none | not a target |

The core entry `@kaetemi/switchboard` uses only Web platform APIs that every runtime shares (typed arrays, Web Streams, `AbortSignal`, WebCrypto, timers) and never imports `node:` modules or `ws`.

**Deno:** `deno run --allow-net --allow-read --allow-env --allow-ffi app.ts`. `--allow-ffi` loads the QUIC addon (`--allow-net` does not constrain its sockets); the WebSocket server needs `ws` in a `node_modules` directory (npm specifiers, or `--node-modules-dir=manual` next to an installed `ws` for a lone script).

On Deno, CI runs the unit project (`deno run -A node_modules/vitest/vitest.mjs run --project unit`) and the built package as a consumer installs it, from its tarballs with the prebuilt addon and `ws`, under exactly those permissions (`test/runtime/smoke.mjs`: Talk requests over TCP, WebSocket, raw QUIC and WebTransport). What does not run there: the integration project passes on Deno 2.9 except one case, the native provider's child-process test, because Deno runs `node --input-type=module -e` as a script, where top-level `await` fails (the package is not involved; the case passes when the same code runs as a module file), so the project stays out of the Deno job. The interop and browser suites drive Dart and Playwright from Node and are not meant for Deno.

**Node 22** runs the compiled package; running TypeScript that uses `await using` directly needs Node 24 or later, or `tsc` (Node 22's type stripping cannot parse it).

## Endpoints

| Scheme | Transport | Where |
| --- | --- | --- |
| `tcp://host:port` | TCP with the stream binding | Node, Bun, Deno |
| `ws://`, `wss://host:port/path` | WebSocket, subprotocol `switchboard` | everywhere; servers on Node, Bun, Deno with `ws` |
| `mem://name` | in-process pair, for tests and local wiring | everywhere |
| `quic://host:port` | raw QUIC, ALPN `switchboard` | native provider |
| `https://host:port/path` | WebTransport over HTTP/3, protocol `switchboard` | native provider; browsers dial only |

A node refuses a scheme it has no dialer or provider for with `UNIMPLEMENTED` (the browser node refuses `tcp` with `UNSUPPORTED`) and moves on to a service record's next endpoint. A fragment (`ws://host/path#npc/1a2b`) is ignored for connecting.

## How QUIC is provided

No JavaScript runtime offers the QUIC features Switchboard needs (bidirectional streams with application reset codes, close with code and reason, TLS keying-material export for exporter-bound identity, certificate pinning): Node's QUIC is not in any official release, Bun and Deno expose nothing usable, and browsers offer only WebTransport. So the package brings two providers behind one contract (`QuicConnector`, `QuicListener`):

Two rules of the binding at the end of a session concern every provider. A side that goes away writes a PING right after its GOAWAY and closes only once the peer answered it (or the connection ended, or `closeConfirmTimeout` passed), so that the GOAWAY is read before the session goes; against a peer that reads nothing that can take up to `closeConfirmTimeout` (30 s), unless the keep-alive ends the connection first (see [Channels and Talk](Channels-and-Talk)). Over WebTransport, the side ending a session resets and stops its streams without an application code (WT_SESSION_GONE), which can arrive before the session's close: a channel holds such a reset or STOP_SENDING for up to 2 s, delivering and sending nothing meanwhile, and ends with the session's status when the close follows (GOING_AWAY after a graceful one), UNKNOWN only when it does not. One consequence: a codeless STOP_SENDING the peer sends after the channel's own failure close is held too, which delays the channel's `done` by up to 2 s, with its status already final.

### Native provider (Node, Bun, Deno)

An N-API addon written in Rust over `quinn` and `rustls`, mirroring the `sbq` shim that the Dart reference implementation uses, so both implementations share one QUIC stack and one WebTransport dialect. It serves raw QUIC and WebTransport, as client and as server, with the TLS exporter, certificate pins, self-signed certificate generation and stream priorities.

```ts
import {
  certificateHash,
  EndpointResolver,
  nativeQuic,
  Switchboard,
  selfSignedCertificate,
} from '@kaetemi/switchboard/node';

const certificate = selfSignedCertificate('127.0.0.1'); // ECDSA P-256, 14 days; synchronous
const server = new Switchboard({ quic: nativeQuic() });
server.registerService('echo', (incoming) => incoming.talk().serve((m) => m.reply(m.text())));
const quic = await server.listenQuic('127.0.0.1', 0, { certificate }); // quic://127.0.0.1:port
const wt = await server.listenWebTransport('127.0.0.1', 0, { path: '/sb', certificate }); // https://...

for (const endpoint of [quic, wt]) {
  const client = new Switchboard({
    resolver: new EndpointResolver(endpoint),
    quic: nativeQuic(),
    quicBinding: { serverCertificateHashes: [certificateHash(certificate.certificateDer)] }, // a pin
  });
  {
    await using talk = await client.openTalk('echo');
    console.log((await talk.request('X', `over ${endpoint}`)).text());
  }
  await client.close();
}
await server.close();
```

- `listenQuic(address, port, options)` and `listenWebTransport(address, port, { path, allowedOrigins, ... })` take a `certificate` (`{ certificateDer, privateKeyDer }`; default a self-signed one for the advertised host), `advertisedHost`, `policy`, `identityRules`, `identifyInReturn`, the stack's windows and timeouts, and a `provider` (default the node's `quic` option when it can listen, else `nativeQuic()`). They fill in the `switchboard` ALPN and WebTransport protocol themselves. A node that listens on QUIC without a `quic` option dials through the listener's provider from then on.
- Between nodes, certificates are self-signed: the dialler does not verify them (`quicBinding.verifyCertificate` false) and identity comes from exporter-bound `IDENT`s ([Identity](Identity)); a pin (`serverCertificateHashes`, 32 bytes or `{ algorithm: 'sha-256', value }`) authenticates the acceptor. A node dialling another node's WebTransport listener passes a pin or `webTransportCertificateVerified: false`.
- The lower level: `connectQuic(endpoint, nativeQuic(), options)` returns a `QuicBindingConnection`; `listenQuic(await nativeQuic().listen(host, port, config))` accepts sessions as connections; `quicConnector(endpoint, provider, init)` gives a `ReconnectingClient` its connector ([Clients](Clients)).

**Prebuilt binaries** ship as optional dependencies, one per platform:

| Platform | Package |
| --- | --- |
| Linux x64 (glibc) | `@kaetemi/switchboard-quic-linux-x64-gnu` |
| Linux arm64 (glibc) | `@kaetemi/switchboard-quic-linux-arm64-gnu` |
| Linux x64 (musl) | `@kaetemi/switchboard-quic-linux-x64-musl` |
| Linux arm64 (musl) | `@kaetemi/switchboard-quic-linux-arm64-musl` |
| macOS arm64 | `@kaetemi/switchboard-quic-darwin-arm64` |
| macOS x64 | `@kaetemi/switchboard-quic-darwin-x64` |
| Windows x64 | `@kaetemi/switchboard-quic-win32-x64-msvc` |

The addon loads on first use, never at import: `$SWITCHBOARD_QUIC_ADDON` when set (and nothing else), else a local build at `native/sbq-node.node` (`npm run build:native`, which needs Rust 1.88 or later), else the package for the platform. Without one, connecting rejects and listening fails with `QuicError` `'unsupportedScheme'` naming the package to install, a node reads it as `UNIMPLEMENTED` and moves on, and everything else works; `nativeQuicAvailability()` says whether the addon loads and why not. On musl Linux (Alpine) the loader picks the `-musl` package; that addon needs `libgcc_s.so.1`, which Node on Alpine needs too. An idle process with no live session or listener is not held open by the addon.

### WebTransport provider (browsers)

The W3C WebTransport API, as a client only.

```ts
import {
  connectQuic,
  EndpointResolver,
  ReconnectingClient,
  Switchboard,
  webTransport,
  webTransportConnector,
} from '@kaetemi/switchboard/web';

// A reconnecting client:
const client = new ReconnectingClient(
  webTransportConnector('https://edge.example.com:4433/sb', {
    serverCertificateHashes: [{ algorithm: 'sha-256', value: certificateSha256 }],
  }),
);

// A browser node that dials https:// endpoints through the provider:
const node = new Switchboard({
  resolver: new EndpointResolver('https://edge.example.com:4433/sb'),
  quic: webTransport(),
  quicBinding: { serverCertificateHashes: [certificateSha256] },
});

// One connection:
const connection = await connectQuic('https://edge.example.com:4433/sb', webTransport(), {
  serverCertificateHashes: [certificateSha256],
});
```

`webTransport(init?)` returns the provider, a `QuicConnector` (`WebTransportProvider`; `init` injects a `WebTransport` constructor and a clock, for tests); `webTransportConnector(url, init)` is `quicConnector(url, webTransport(), init)`. `WebTransportProvider.isSupported` tells whether the page has the API (a secure context).

Browser notes: the `switchboard` protocol is negotiated from Chrome 143, Firefox 155 and Safari 26.4 (older browsers connect without one, which the server accepts); stream priorities only where `sendOrder` exists; Chromium carries stream error codes up to 255, which is all the binding uses. A browser reports a refused session, an unreachable server and a rejected certificate alike, as `handshakeFailed`. A pinned certificate (`serverCertificateHashes`) must be valid for at most 14 days, which `selfSignedCertificate` gives by default. The provider feature-tests every optional member of the API.

What CI shows of each browser (Playwright 1.64.1, against the native server; both signal WebTransport draft-02):

| | Chromium 156 | Firefox 157 |
| --- | --- | --- |
| `protocol` | yes | yes |
| `exportKeyingMaterial` | no: identity is nonce-bound, which is safe because the browser verified the certificate | yes, but not the draft's: see below |
| `draining` | no | yes, but not on the server's WT_DRAIN_SESSION |
| `sendOrder`, `createSendGroup` | no | yes |
| `getStats` | no | present, rejects (not implemented): `stats()` gives `undefined` |
| STOP_SENDING from `stop()` | yes | none sent |
| the page's close at the server | the code and reason | a race: the code and reason, or a lost connection |

Firefox departs from the drafts in four places, each traced in its sources (the browser tests check each as such, `departures` in `test/browser/webtransport.test.ts`):

- **The exporter.** Firefox writes the session ID into the exporter's context as a QUIC varint, where the draft's "WebTransport Exporter Context" has a 64-bit session ID, as the shim (and so every node, TS or Dart) does. Its exporter is therefore not the node's, and **a node refuses a Firefox page's `IDENT` with `UNAUTHENTICATED`**: a Firefox page can connect and talk, but not identify. How to resolve it is open (`docs/design.md`, section 13, item 7).
- **The close.** Firefox closes its dedicated connection as soon as the session has closed, and the shim ends the sessions of a closed connection at once, so the server reads the page's WT_CLOSE_SESSION only when it arrives first; otherwise the session ends as lost, without the code or the reason. A page's `close()` sends no GOAWAY, so the node then reads CONNECTION_LOST; a page's `goAway()` (and a node's or client's close, which go away first) writes the GOAWAY with a PING behind it and closes only once the node answered, so the node reads GOING_AWAY whichever close comes first. Native peers race the same way when the dialler closes its last session.
- **Drain.** Firefox ignores WT_DRAIN_SESSION and resolves `draining` only on an HTTP/3 GOAWAY, which the shim does not send; the binding's own GOAWAY record is unaffected.
- **STOP_SENDING.** Cancelling a stream's readable sends none, so the node is not told that the page stopped reading a stream and keeps sending until flow control or the channel's end stops it; the page's own reads end at once.

### Later options

`node:quic` and Deno's built-in QUIC can become further providers behind the same contract once they offer keying-material export and pinning; nothing above the provider would change.
