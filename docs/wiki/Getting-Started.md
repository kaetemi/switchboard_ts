# Getting Started

## Install

```sh
npm install @polyverse/switchboard
npm install ws            # Node only: WebSocket servers (and a client with a receive limit)
```

Node.js 22 or newer. ES modules only: `import`, not `require`.

## A service and a client

```ts
import { EndpointResolver, Status, StatusCode, Switchboard } from '@polyverse/switchboard/node';

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
await using talk = await client.openTalk('echo');

const reply = await talk.request('UPPER', 'hello');
console.log(reply.text()); // HELLO

const stream = talk.streamRequest('COUNT', '3');
for await (const item of stream) console.log(item.text()); // 1, 2, 3
console.log((await stream.done).text()); // done

await client.close();
await server.close();
```

Payloads are bytes (`Uint8Array`) or text; a string is sent as UTF-8. A received message has `text()`, `json()` and `bytes()`.

## WebSocket

```ts
const endpoint = await server.listenWebSocket('0.0.0.0', 8080, { path: '/sb', policy: ChannelPolicies.denyReserved });
// clients connect to ws://host:8080/sb; an internet-facing listener sets a policy that refuses reserved types
```

## Raw channels

A channel carries subframes with credit-based flow control. Reading is pull based: credit returns when you take a chunk, so a channel you do not read applies backpressure to the sender.

```ts
const channel = await client.openChannel('blob', { shard: 7 });
await channel.send(new Uint8Array(1024));          // resolves once accepted into the send window
for await (const chunk of channel) handle(chunk);  // or channel.readable.pipeTo(destination)
await channel.close();
```

## A browser client

```ts
import { ReconnectingClient, connectWebSocket, webTransport } from '@polyverse/switchboard/web';

const client = new ReconnectingClient(() => connectWebSocket('wss://edge.example.com/sb'));
// or over WebTransport:
// const client = new ReconnectingClient(webTransport('https://edge.example.com:4433/sb', { serverCertificateHashes }));

client.openPersistentTalk('chat', async (talk) => {
  for await (const message of talk.messages) render(message.json());
});
for await (const state of client.states) console.log(state.phase); // disconnected, connecting, connected
```

The reconnecting client re-opens persistent channels after every reconnect and backs off between attempts.

## Identity and policies

```ts
const node = new Switchboard({
  credential,              // issued by a CredentialIssuer
  holderKey,               // the Ed25519 key the credential names, or omitted for a bearer credential
  verifier,                // checks peers' credentials: HMAC-SHA256 secret or Ed25519 issuer keys
});
await node.listenTcp('0.0.0.0', 4000, { policy: ChannelPolicies.scoped() });
```

## Cancellation and lifetimes

Every waiting call takes an `AbortSignal` (`{ signal }`), and every connection, channel, listener and node supports `await using`. `done` on a connection or channel resolves with its end `Status`.
