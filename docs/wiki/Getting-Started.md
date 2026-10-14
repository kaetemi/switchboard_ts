# Getting Started

## Install

```sh
npm install @kaetemi/switchboard
npm install ws            # Node, Bun, Deno: WebSocket servers (and a client that bounds message size)
```

Node.js 22 or newer, Bun 1.x or Deno 2.x; any evergreen browser for the web entry. ES modules only: `import`, not `require`. The QUIC addon for your platform comes as an optional dependency (see [Runtimes and QUIC](Runtimes-and-QUIC)); everything else is TypeScript.

The examples use `await using`. Node 24 and later, Bun and Deno run them as TypeScript directly; Node 22's built-in type stripping cannot parse `await using`, so on Node 22 compile with `tsc` first, which lowers it.

## A service and a client

```ts
import { EndpointResolver, Status, StatusCode, Switchboard } from '@kaetemi/switchboard/node';

const server = new Switchboard();
server.registerService('echo', async (incoming) => {
  const talk = incoming.talk();
  for await (const message of talk.messages) {
    switch (message.procedureName) {
      case 'UPPER':
        await message.reply(message.text().toUpperCase());
        break;
      case 'COUNT':
        for (let i = 1; i <= Number(message.text()); i++) await message.replyItem(String(i));
        await message.reply('done');
        break;
      default:
        await message.replyAbort(Status.of(StatusCode.unimplemented));
    }
  }
});
const endpoint = await server.listenTcp('127.0.0.1', 0); // 'tcp://127.0.0.1:41234'

const client = new Switchboard({ resolver: new EndpointResolver(endpoint) });
{
  await using talk = await client.openTalk('echo');

  const reply = await talk.request('UPPER', 'hello');
  console.log(reply.text()); // HELLO

  const stream = talk.streamRequest('COUNT', '3');
  for await (const item of stream) console.log(item.text()); // 1, 2, 3
  console.log((await stream.done).text()); // done

  await talk.request('NOPE').catch((e: unknown) => console.log(String(e))); // TalkAbortError: unimplemented
} // the Talk channel closes here

await client.close();
await server.close();
```

Payloads are bytes (`Uint8Array`) or text; a string is sent as UTF-8. A received message has `text()`, `json()` and `bytes()`. Close channels before their node (here, by leaving the block): a node's `close()` sends GOAWAY and gives channels still open up to `goAwayGrace` (10 s) to finish. A listening node keeps the process alive until it is closed.

`EndpointResolver` sends every type to one endpoint, which is what a client of a frontend wants; a node in a mesh resolves types through the naming service instead ([Naming and Sharding](Naming-and-Sharding)).

## WebSocket

```ts
import { ChannelPolicies, Switchboard } from '@kaetemi/switchboard/node';

const server = new Switchboard();
const endpoint = await server.listenWebSocket('0.0.0.0', 8080, {
  path: '/sb',
  policy: ChannelPolicies.denyReserved,
});
console.log(endpoint); // ws://0.0.0.0:8080/sb: clients connect to ws://host:8080/sb
```

A listener reachable by untrusted peers needs a policy that refuses the reserved types (`_ns` and the rest of the `_` namespace), or a stricter one ([Identity](Identity)). `listenWebSocket` answers other paths with 403 unless `onOtherRequest` and `onOtherUpgrade` serve them; `acceptWebSocket(request, socket, head, { policy })` upgrades the requests of an application's own `http.Server`. For `wss://`, put TLS in front (the node dials `wss://` itself).

## Raw channels

A channel carries subframes with credit-based flow control. Reading is pull based: credit returns when you take a chunk, so a channel you do not read applies backpressure to the sender.

```ts
const channel = await client.openChannel('blob', { shard: 7 });
await channel.send(new Uint8Array(1024)); // resolves once accepted into the send window
for await (const chunk of channel) handle(chunk); // or channel.readable.pipeTo(destination)
await channel.close();
```

[Channels and Talk](Channels-and-Talk) has the details: windows, `writable`, statuses, the Talk options.

## A browser client

```ts
import { connectWebSocket, ReconnectingClient, webTransportConnector } from '@kaetemi/switchboard/web';

const client = new ReconnectingClient(({ signal }) =>
  connectWebSocket('wss://edge.example.com/sb', { signal }),
);
// or over WebTransport, a self-signed certificate pinned by its SHA-256:
const overWebTransport = new ReconnectingClient(
  webTransportConnector('https://edge.example.com:4433/sb', {
    serverCertificateHashes: [{ algorithm: 'sha-256', value: certificateSha256 }],
  }),
);

client.openPersistentTalk('chat', async (talk) => {
  for await (const message of talk.messages) render(message.json());
});
for await (const state of client.states) console.log(state.phase); // disconnected, connecting, connected
```

The reconnecting client re-opens persistent channels after every reconnect and backs off between attempts ([Clients](Clients)). The web entry also has a connect-only `Switchboard` that dials `ws`, `wss` and, with `quic: webTransport()`, `https` endpoints.

## Identity and policies

```ts
import { ChannelPolicies, Switchboard } from '@kaetemi/switchboard/node';

const node = new Switchboard({
  credential, // issued by the mesh authority's CredentialIssuer
  holderKey, // the Ed25519HolderKey the credential names; omitted for a bearer credential
  verifier, // a CredentialVerifier with the authority's key: checks the peers' credentials
});
await node.listenTcp('0.0.0.0', 4000, { policy: ChannelPolicies.scoped() });
```

[Identity](Identity) shows how to issue and verify credentials, and what the policies admit.

## Cancellation and lifetimes

Every waiting call takes an `AbortSignal` (`{ signal }`), and every connection, channel, Talk channel, listener, client and node supports `await using`. `done` on a connection or channel resolves with its end `Status`; it never rejects.

## Bun and Deno

```sh
bun app.ts
deno run --allow-net --allow-read --allow-env --allow-ffi app.ts
```

Deno needs `--allow-ffi` for the QUIC addon (its `--allow-net` does not constrain the addon's sockets), and the WebSocket server needs `ws` in a `node_modules` directory: with npm specifiers, or for a lone script `--node-modules-dir=manual` next to an installed `ws`.
