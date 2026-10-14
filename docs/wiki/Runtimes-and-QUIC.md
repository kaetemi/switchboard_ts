# Runtimes and QUIC

## Runtimes

| Runtime | Import | Transports | QUIC and WebTransport | Tested |
| --- | --- | --- | --- | --- |
| Node.js 22, 24, 26 | `@kaetemi/switchboard/node` | TCP, WebSocket client and server, in-memory | native provider | CI: unit and integration |
| Bun 1.x | `@kaetemi/switchboard/node` | same, through Bun's Node compatibility | native provider | CI: unit; TCP, WebSocket and QUIC by hand |
| Deno 2.x | `@kaetemi/switchboard/node` | same, through `node:` compatibility | native provider | by hand (Deno 2.9) |
| Browsers | `@kaetemi/switchboard/web` | WebSocket | WebTransport API | CI: Chromium 141 |
| Edge runtimes | `@kaetemi/switchboard` with your own transport | WebSocket where offered | none | not a target |

The core entry `@kaetemi/switchboard` uses only Web platform APIs that every runtime shares (typed arrays, Web Streams, `AbortSignal`, WebCrypto, timers) and never imports `node:` modules or `ws`.

**Deno:** `deno run --allow-net --allow-read --allow-env --allow-ffi app.ts`. `--allow-ffi` loads the QUIC addon (`--allow-net` does not constrain its sockets); the WebSocket server needs `ws` in a `node_modules` directory (npm specifiers, or `--node-modules-dir=manual` next to an installed `ws` for a lone script).

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
| macOS arm64 | `@kaetemi/switchboard-quic-darwin-arm64` |
| macOS x64 | `@kaetemi/switchboard-quic-darwin-x64` |
| Windows x64 | `@kaetemi/switchboard-quic-win32-x64-msvc` |

The addon loads on first use, never at import: `$SWITCHBOARD_QUIC_ADDON` when set (and nothing else), else a local build at `native/sbq-node.node` (`npm run build:native`, which needs Rust 1.88 or later), else the package for the platform. Without one, connecting rejects and listening fails with `QuicError` `'unsupportedScheme'` naming the package to install, a node reads it as `UNIMPLEMENTED` and moves on, and everything else works; `nativeQuicAvailability()` says whether the addon loads and why not. Musl Linux has no prebuild yet (the loader looks for `-linux-<arch>-musl`; build it locally). An idle process with no live session or listener is not held open by the addon.

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

Browser notes: the `switchboard` protocol is negotiated from Chrome 143, Firefox 155 and Safari 26.4 (older browsers connect without one, which the server accepts); keying-material export exists on Firefox 155 and later, elsewhere identity is nonce-bound, which is safe because the browser verified the certificate; stream priorities only where `sendOrder` exists; Chromium carries stream error codes up to 255, which is all the binding uses. Chromium 141 (what CI runs) has none of `protocol`, `draining`, `exportKeyingMaterial`, `getStats`, `createSendGroup` or `sendOrder`; the provider feature-tests each. A browser reports a refused session, an unreachable server and a rejected certificate alike, as `handshakeFailed`. A pinned certificate (`serverCertificateHashes`) must be valid for at most 14 days, which `selfSignedCertificate` gives by default.

### Later options

`node:quic` and Deno's built-in QUIC can become further providers behind the same contract once they offer keying-material export and pinning; nothing above the provider would change.
