# Runtimes and QUIC

## Runtimes

| Runtime | Import | Transports | QUIC and WebTransport |
| --- | --- | --- | --- |
| Node.js 22, 24, 26 | `@kaetemi/switchboard/node` | TCP, WebSocket (client and server), in-memory | native provider |
| Bun 1.x | `@kaetemi/switchboard/node` | same, through Bun's Node compatibility | native provider |
| Deno 2.x | `@kaetemi/switchboard/node` with `--allow-net --allow-ffi` | same, through `node:` compatibility | native provider |
| Browsers | `@kaetemi/switchboard/web` | WebSocket | WebTransport API |
| Edge runtimes | `@kaetemi/switchboard` with your own transport | WebSocket where offered | none |

The core entry `@kaetemi/switchboard` uses only Web platform APIs that every runtime shares (typed arrays, Web Streams, `AbortSignal`, WebCrypto, timers) and never imports `node:` modules or `ws`.

## Endpoints

| Scheme | Transport | Where |
| --- | --- | --- |
| `tcp://host:port` | TCP with the stream binding | Node, Bun, Deno |
| `ws://`, `wss://host:port/path` | WebSocket, subprotocol `switchboard` | everywhere (servers on Node, Bun, Deno with `ws`) |
| `mem://name` | in-process pair, for tests and local wiring | everywhere |
| `quic://host:port` | raw QUIC, ALPN `switchboard` | native provider |
| `https://host:port/path` | WebTransport over HTTP/3 | native provider, browsers |

A node moves on to a service record's next endpoint when a scheme is unsupported on its runtime.

## How QUIC is provided

No JavaScript runtime offers the QUIC features Switchboard needs (bidirectional streams with application reset codes, close with code and reason, TLS keying-material export for exporter-bound identity, certificate pinning): Node's QUIC is not in any official release, Bun and Deno expose nothing usable, and browsers offer only WebTransport. So the package brings two providers behind one contract:

### Native provider (Node, Bun, Deno)

An N-API addon written in Rust over `quinn` and `rustls`, mirroring the `sbq` shim that the Dart reference implementation uses, so both implementations share one QUIC stack and one WebTransport dialect. It serves raw QUIC and WebTransport, as client and as server, with the TLS exporter, certificate pins, self-signed certificate generation and stream priorities.

```ts
import { Switchboard, nativeQuic, selfSignedCertificate } from '@kaetemi/switchboard/node';

const node = new Switchboard({ quic: nativeQuic() });
const endpoint = await node.listenQuic('0.0.0.0', 4433, { certificate: await selfSignedCertificate('edge.example.com') });
const wt = await node.listenWebTransport('0.0.0.0', 4433, { path: '/sb', certificate });
```

Prebuilt binaries ship as optional dependencies per platform (Linux x64 and arm64, macOS arm64 and x64, Windows x64). The addon loads on first use; without a binary for the platform, QUIC endpoints report `UNSUPPORTED` and everything else works. Deno needs `--allow-ffi`; its `--allow-net` does not constrain the addon's sockets.

### WebTransport provider (browsers)

The W3C WebTransport API, as a client only.

```ts
import { ReconnectingClient, quicConnector, webTransport } from '@kaetemi/switchboard/web';
// webTransport() is the provider; quicConnector gives a client its connector.
const client = new ReconnectingClient(
  quicConnector('https://edge.example.com:4433/sb', webTransport(), {
    serverCertificateHashes: [{ algorithm: 'sha-256', value: hash }],
  }),
);
```

Browser notes: the `switchboard` protocol is negotiated from Chrome 143, Firefox 155 and Safari 26.4 (older browsers connect without one, which the server accepts); keying-material export exists on Firefox 155 and later, elsewhere identity is nonce-bound, which is safe because the browser verified the certificate; stream priorities only where `sendOrder` exists; Chromium carries stream error codes up to 255, which is all the binding uses.

### Later options

`node:quic` and Deno's built-in QUIC can become further providers behind the same contract once they offer keying-material export and pinning; nothing above the provider would change.
