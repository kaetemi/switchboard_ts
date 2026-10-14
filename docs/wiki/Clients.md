# Clients

A frontend application (a browser page, a desktop or mobile app, a CLI) usually talks to one endpoint and wants that connection kept up: reconnecting after every loss, re-opening its subscriptions on each new connection, and receiving what the endpoint pushes. `ReconnectingClient` does that, over any transport. For a node that resolves services and opens channels anywhere, use a `Switchboard` instead (the browser entry has a connect-only one); both are runtime neutral.

## A reconnecting client

```ts
import {
  ChannelPolicies,
  ClientPhase,
  connectWebSocket,
  ReconnectingClient,
  Switchboard,
} from '@kaetemi/switchboard/node';

// An endpoint with a feed and a profile service.
const server = new Switchboard();
server.registerService('feed', (incoming) =>
  incoming.talk().serve(async (m) => {
    if (m.procedureName === 'SUB') {
      await m.reply('subscribed');
      await incoming.talk().send('NEWS', `about ${m.text()}`);
    }
  }),
);
server.registerService('profile', (incoming) => incoming.talk().serve((m) => m.reply('me')));
const endpoint = await server.listenWebSocket('127.0.0.1', 0, {
  path: '/sb',
  policy: ChannelPolicies.denyReserved,
});

// The client: a transport connector, and the credential every channel carries.
const client = new ReconnectingClient(({ signal }) => connectWebSocket(endpoint, { signal }), {
  defaultPayload: 'credential bytes',
});
client.on('state', (state) => console.log(state.phase, String(state.lastStatus ?? '')));

// A one-off request: waits for the connection if needed.
{
  await using talk = await client.openTalk('profile');
  console.log((await talk.request('GET')).text()); // me
}

// A subscription, set up again on every connection.
const feed = client.openPersistentTalk(
  'feed',
  async (talk) => {
    void talk.serve((m) => console.log(m.procedureName, m.text())); // NEWS about ships
    console.log((await talk.request('SUB', 'ships')).text()); // subscribed
  },
  { onClosed: (status) => console.log('feed closed', String(status)) },
);

await new Promise((resolve) => setTimeout(resolve, 100)); // ... later
console.log(client.state.phase === ClientPhase.connected, feed.current !== undefined);
await feed.close();
await client.close();
await server.close();
```

The same client in a browser imports from `@kaetemi/switchboard/web`, whose `connectWebSocket` uses the browser's `WebSocket`; on Node, Bun and Deno the node entry's uses the `ws` package when it is installed (which bounds message size before buffering) and the global `WebSocket` otherwise.

## Connectors

The first argument says how to reach the endpoint:

- a **transport connector**, a function `({ signal }) => Promise<FrameTransport>`: the frame mux runs over each transport, as the initiator. `({ signal }) => connectWebSocket(url, { signal })`, `({ signal }) => connectTcp(host, port, { signal })` (node entry), or `({ signal }) => connectTransport(endpoint, { signal })` for any of `tcp`, `ws`, `wss` and `mem`;
- a **connection connector**, an object `{ connect(muxOptions, { signal }) }` resolving with a connection of another binding: this is how QUIC and WebTransport plug in.

```ts
import { ReconnectingClient, webTransportConnector } from '@kaetemi/switchboard/web';

// A browser over WebTransport; the hash pins a self-signed certificate (14 days at most).
const client = new ReconnectingClient(
  webTransportConnector('https://edge.example.com:4433/sb', {
    serverCertificateHashes: [{ algorithm: 'sha-256', value: certificateSha256 }],
  }),
);
```

```ts
import { nativeQuic, quicConnector, ReconnectingClient } from '@kaetemi/switchboard/node';

// Node, Bun or Deno over raw QUIC (or WebTransport, with an https:// endpoint).
const client = new ReconnectingClient(
  quicConnector('quic://edge.example.com:4433', nativeQuic(), {
    serverCertificateHashes: [certificateSha256],
  }),
);
```

`webTransportConnector(url, init)` is `quicConnector(url, webTransport(), init)`; its `init` takes the options of `connectQuic` (`serverCertificateHashes`, `verifyCertificate`, `timeout`, `idleTimeout`, `keepAlive`, `quicOptions`, `identityRules`, `localIdentity`) and an optional `provider`. See [Runtimes and QUIC](Runtimes-and-QUIC).

## State and backoff

`client.state` is a `ClientState`: `phase` (`ClientPhase.disconnected`, `connecting`, `connected`), `attempt`, `lastStatus` (why the last connection or attempt ended), `since`, `nextAttemptAt` and `retryIn` (on the client's monotonic clock). `client.on('state', listener)` reports every change (and returns the function that removes the listener); `for await (const state of client.states)` gets the current state first, then every change, and ends after `close()`. `client.connection` is the current connection, `client.connected` a promise of the next one.

The first attempt after `start()` is immediate (the constructor calls it unless `autoStart: false`). After `n` consecutive failures the delay is `min(initialBackoff * backoffFactor^n, maxBackoff)` times a random factor in `[1 - jitter, 1 + jitter]` (defaults 500 ms, 2, 30 s, 0.2); a connection that lasted longer than `maxBackoff` resets the sequence. `connectTimeout` (10 s) bounds each attempt. `reconnectNow()` attempts at once (after the application resumed, or the network came back); `reconnectNow({ force: true })` also leaves a connection that may be stale. `stop()` disconnects and stops reconnecting until `start()`.

When the endpoint sends GOAWAY `GOING_AWAY` (it restarts or drains), the client moves to a new connection at once without waiting for the old one to end: persistent channels are closed with `GOING_AWAY` there and opened again on the new one, and waiting opens go to the new one. A planned restart is not a failure; a GOAWAY carrying an error is handled as a loss.

## Opening channels

`openChannel(address, options)` and `openTalk(address, options)` wait for a usable connection (at most `openTimeout`, 30 s by default; 0 fails at once; `signal` abandons the wait) and open the channel there. The address is a service address (`'profile'`, `'npc/1a2b'`) or a `ChannelAddress`; a channel opened with an empty payload carries `defaultPayload`, which the application may replace at any time (after a credential refresh, say): channels opened afterwards carry the new value, re-opened persistent ones included.

## Persistent channels

`openPersistent(address, onOpen, { onClosed })` and `openPersistentTalk(address, onOpen, { talkOptions, onClosed })` open a channel on every connection, for subscriptions and other long-lived exchanges. `onOpen` receives each new channel (never synchronously from the call): read it and send what sets it up there. `onClosed` receives each channel's end status; every `onOpen` is followed by exactly one `onClosed`.

- The connection ends: the channel is opened again on the next connection.
- The endpoint sends GOAWAY (or `reconnectNow({ force: true })`): the channel is closed with `GOING_AWAY` and opened again on the next connection, which the client makes at once.
- The channel ends while the connection stays up: it is opened again after a backoff of its own.
- It ends with `PERMISSION_DENIED`, `UNAUTHENTICATED`, `UNIMPLEMENTED` or `NOT_FOUND`: the persistent channel ends for good (`isClosed`, `done` resolves with the status), since the endpoint would refuse it again. Refresh the credential and open it again.

The returned `PersistentChannel` or `PersistentTalk` has `address`, `current` (the channel of the current connection, if any), `opened` (an async iterable of each channel), `isClosed`, `done` and `close()`, and is `AsyncDisposable`.

## Channels the endpoint opens

`for await (const incoming of client.incoming)` takes the channels the endpoint pushes, across reconnects, as `IncomingChannel`s (dispatch is symmetric). One consumer; once it stops, or after `close()`, further channels are closed with `UNAVAILABLE`.

## Closing

`close()` stops, closes every persistent channel for good, rejects pending opens with `CANCELLED`, ends `states` and `incoming`, and resolves once every connection has closed (after GOAWAY, when the channels the application opened are done, at most `muxOptions.goAwayGrace`). `await using client = new ReconnectingClient(...)` closes it at the end of the block.
