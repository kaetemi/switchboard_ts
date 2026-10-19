# Channels and Talk

Everything in Switchboard travels on **channels**: ordered, flow-controlled streams of subframes, many to a connection, opened by either side. **Talk** is the message layer on top of a channel: requests and replies, streams of items, plain messages, aborts, timeouts and bulk payloads. This page covers both; the protocol is specified on the wiki pages `mux` and `talk`.

## Connections and channels

A node opens channels for you (`openChannel`, `openTalk`, see [API Overview](API-Overview)), but the layer underneath is usable on its own. `FrameMuxConnection` runs the frame mux over any `FrameTransport` (TCP, WebSocket, an in-memory pair); `QuicBindingConnection` carries the same contract over QUIC streams. Both implement `MuxConnection`, and everything above programs against it.

```ts
import {
  ChannelAddress,
  FrameMuxConnection,
  memoryTransportPair,
  utf8Decode,
} from '@kaetemi/switchboard';

const [a, b] = memoryTransportPair();
const client = new FrameMuxConnection(a, { isInitiator: true });
const server = new FrameMuxConnection(b, { isInitiator: false });

// The OPEN payload is an address header plus an application payload.
const channel = client.open(new ChannelAddress({ type: 'blob', payload: 'hello' }).encode());
await channel.send(new TextEncoder().encode('first subframe'));

// Channels the peer opened, in order. One consumer, for the connection's lifetime.
const accepted = server.incoming[Symbol.asyncIterator]();
const incoming = (await accepted.next()).value!;
console.log(utf8Decode(ChannelAddress.decode(incoming.openPayload).payload)); // hello
for await (const subframe of incoming) {
  console.log(utf8Decode(subframe)); // first subframe
  break;
}
await incoming.close();
console.log(String(await channel.done)); // ok

await client.close();
await server.close();
```

`open()` returns the channel at once and throws synchronously for misuse (`RESOURCE_EXHAUSTED` when no id is free or the peer's channel limit is reached, `FAILED_PRECONDITION` after GOAWAY). A refusal by the peer arrives as the channel's close status: `done` resolves (never rejects) with the first non-OK status sent or received, or `CONNECTION_LOST` and `GOING_AWAY` when the connection went.

### Reading

Reading is pull based. A channel is an `AsyncIterable<Uint8Array>` of its subframes, and also has `readable`, a `ReadableStream<Uint8Array>` with a high-water mark of 0 for `pipeTo` and `pipeThrough`. Taking a subframe is what returns its flow-control credit to the sender, so a channel nobody reads applies backpressure to its sender once the window is used up, and only to that channel.

- `break` out of `for await` (or cancelling `readable`) stops delivery: what arrives afterwards is discarded, and the channel stays open. Close it with `close(status?)`.
- Read or close every channel you are given. What channels buffer unread, the ones the peer closed included, counts toward `receiveHighWaterMarkBytes`, and above it the connection stops reading its transport for every channel. `break` (or cancelling `readable`) drops what a channel buffered, and so does `close()` on a channel that has ended and that nothing reads; a channel closed while still open keeps what arrives for a reader until it ends, so cancel its `readable` if nothing will read it.
- `incoming` has one consumer. Stopping its iteration closes the channels not taken yet, and every later one, with `UNAVAILABLE`: keep one loop (or one iterator) for the connection's lifetime.
- `manualCredit = true` hands credit back to you: report what was consumed with `consumed(bytes)`; `grant(bytes)` raises the window at once. The Talk layer uses this to return credit only when the application took a message.

### Writing and flow control

`send(subframe)` copies the bytes and resolves once the subframe is accepted into the send window; a sender that awaits it is paced by the receiver. `writable` is the same as a `WritableStream<Uint8Array>` (`write` is a `send`, `close()` closes with OK, `abort(status)` closes with that status).

```ts
console.log(channel.sendWindow, channel.maxSubframeLength, channel.isWritable);
await channel.whenWritable;
const writer = channel.writable.getWriter();
await writer.write(new Uint8Array(1024));
await writer.close();
```

| Member | Meaning |
| --- | --- |
| `sendWindow` | cost this side may still send before credit |
| `maxSubframeLength` | the largest subframe `send()` takes on an ordinary channel (32752 with the default 64 KiB window); larger is `FRAME_TOO_LARGE` |
| `priority` | `'normal'`, or `'bulk'`: `send()` cuts payloads into `bulkChunkSize` chunks and the scheduler sends them after ordinary frames |
| `isWritable`, `whenWritable` | room for the largest subframe, and the promise of it |
| `receiveWindow`, `bufferedBytes` | what the peer may still send, and what arrived and was not read |

Close what you wrote through `writable` with `writer.close()`, which follows the writes before it: a write reaches the channel a microtask or more after `writer.write()` returns, so a `channel.close()` in the same turn goes first. A write to a channel that can no longer send rejects with `FAILED_PRECONDITION` and errors `writable`. Every promise the library hands out is pre-caught, so dropping one never produces an unhandled rejection.

### Connection options

`MuxOptions`, given as `muxOptions` to a node or client, or to `FrameMuxConnection` directly. Durations in milliseconds.

| Option | Default | |
| --- | --- | --- |
| `maxFrameSize` | 1 MiB | largest frame accepted, announced in LIMITS |
| `initialWindow` | 64 KiB | the window granted to every channel |
| `maxChannels` | 65536 | open channels, and separately the incoming backlog; 0 unlimited |
| `keepAliveInterval`, `keepAliveTimeout` | 10 000, 10 000 | PING after silence; close if nothing answers; `undefined` disables the PING |
| `goAwayGrace` | 10 000 | how long `goAway()` (and a node's `close()`) waits for open channels |
| `closeConfirmTimeout` | 30 000 | how long `channel.close()` waits for the peer's CLOSE; over QUIC also how long `goAway()` waits for the peer to show it read the GOAWAY (0: no bound) |
| `receiveHighWaterMarkBytes` | 16 MiB | stop reading the transport while this much is unread |
| `identityVerifier`, `identityTimeout`, `requireNamedIdent` | none, 10 000, false | see [Identity](Identity) |
| `clock`, `logger` | system clock, silent | injectable everywhere |

### Lifetimes

Connections and channels are `AsyncDisposable`: `await using channel = ...` closes the channel at the end of the block. `connection.goAway(status?)` sends GOAWAY and waits for the open channels up to `goAwayGrace` before closing; over QUIC and WebTransport a PING follows the GOAWAY, and the close also waits for its PONG (or the end of the connection, at most `closeConfirmTimeout` after the GOAWAY), so that the peer reads the GOAWAY before the session goes. Against a peer that reads nothing on the connection, `goAway()`, and so a node's `close()`, can therefore take up to `closeConfirmTimeout` (30 s) rather than `goAwayGrace`, unless the keep-alive ends the connection first; with `closeConfirmTimeout` at 0 it waits until the keep-alive or the peer ends it. `connection.close()` closes the transport now, without waiting for either. `connection.ping(payload?, { signal })` resolves with the round trip in milliseconds.

## Talk

A `TalkChannel` wraps a channel. A node gives you one with `node.openTalk(address)`, and a handler with `incoming.talk()`; on a raw channel, `new TalkChannel(channel, options)`.

### Serving

```ts
import { Status, StatusCode, Switchboard } from '@kaetemi/switchboard/node';

const node = new Switchboard();
node.registerService('demo', (incoming) =>
  incoming.talk().serve(async (message) => {
    switch (message.procedureName) {
      case 'UPPER':
        return message.reply(message.text().toUpperCase());
      case 'SLOW':
        await message.extend({ renew: 2000 }); // the answer may take 2 s more
        return message.reply('late');
      case 'TICKS':
        return message.replyStream(['1', '2', '3'], { trailer: 'end' });
      case 'BIG':
        return message.reply(new Uint8Array(100_000)); // sent as a bulk payload
      default:
        return message.replyAbort(Status.of(StatusCode.unimplemented));
    }
  }),
);
```

`serve(handler)` takes every message and calls the handler for each without waiting between them, and resolves when the channel ends. It is the guarded form: a handler that throws or rejects has its request answered `ABORT INTERNAL`. A `for await (const message of talk.messages)` loop works too, but takes one message at a time, and an exception in its body leaves the request unanswered until the responder timeout. `messages` has one consumer and is iterated once; stopping the iteration answers the requests not taken, and every later one, `ABORT UNIMPLEMENTED`.

A received `TalkMessage` has `procedure` (a `Name`) and `procedureName`, `payload`, `text()`, `json()`, `bytes()`, `expectsReply`, `expectsStream`, `isCancelled`, `signal` (a request's is aborted when the requester cancels, the responder timeout passes or the channel closes: pass it to what the handler awaits; a plain message's never aborts) and the reply family: `reply`, `replyItem`, `replyStream`, `replyAbort`, `extend`, the chained `replyRequest`, `startReplyRequest`, `replyStreamRequest` and their item forms, and `setReplyTimeout`.

### Requests

```ts
const talk = await node.openTalk('demo');

const reply = await talk.request('UPPER', 'hi'); // payloads are bytes or text (UTF-8)
console.log(reply.text()); // HI

const slow = await talk.request('SLOW', undefined, {
  timeout: 1000, // the requester timeout; the EXTEND moves it
  onExtend: ({ renew }) => console.log('extended', renew),
});

const controller = new AbortController();
const pending = talk.request('SLOW', undefined, { signal: controller.signal });
controller.abort(); // sends a cancel; the request rejects with CANCELLED
await pending.catch(() => {});

const handle = talk.startRequest('UPPER', 'x'); // { requestId, response, deadline, cancel() }
console.log((await handle.response).text());

await talk.send('EVENT', JSON.stringify({ seen: true })); // a plain message, no reply
```

A request rejects with `TalkAbortError` (a `SwitchboardError`) when the responder answers ABORT, with `DEADLINE_EXCEEDED` when the requester timeout passes, and with the channel's status when the channel ends first. `ordered: true` delivers the answer in wire order with the channel's messages.

### Streams and item windows

```ts
const ticks = talk.streamRequest('TICKS', undefined, { window: 2 });
let taken = 0;
for await (const item of ticks) {
  console.log(item.text());
  if (++taken === 2) ticks.grant(2); // the responder may send two more
}
console.log((await ticks.done).text()); // the final reply: end
```

A `TalkStream` is the async iterable of its items (also `ticks.items`), with `done`, the final reply. Without `window` the responder sends as fast as the channel allows; with one, it may send that many items before this side grants more, and nothing is granted on your behalf. Stopping the iteration cancels the request. On the responder side `message.window` and `message.whenCredit` tell where the window stands, and a stream request that stays exhausted beyond `creditTimeout` (60 s) is aborted with `DEADLINE_EXCEEDED`.

### Bulk payloads

A message whose payload does not fit one subframe (`maxSubframeLength`, or `bulkThreshold`) is sent transparently on a bulk channel of its own, opened next to the Talk channel and routed to it: the receiver sees one message whose `isBulk` is true. A byte source that should never sit in memory goes in the `bulk` option:

```ts
async function* chunks(): AsyncIterable<Uint8Array> {
  for (let i = 0; i < 16; i++) yield new Uint8Array(64 * 1024);
}
await talk.send('UPLOAD', undefined, { bulk: chunks(), bulkLength: 16 * 64 * 1024 });

const big = await talk.request('BIG');
console.log(big.isBulk, big.bulkLength, (await big.bytes()).length);
```

By default the receiver reassembles the payload before delivering the message (up to `maxInlinePayload`, 16 MiB, within a `reassemblyBudget` per channel). With `streamBulk: (message) => boolean` in the Talk options, the selected messages are delivered at once and their payload read from `message.bulk`, a `ReadableStream<Uint8Array>` whose credit goes back as you read.

### Forwarding

`forwardMessage(message, target)` forwards a message and its whole chain (the reply, items, chained requests, EXTEND, aborts, cancels, bulk payloads) to another Talk channel, with flow control passed through hop by hop and no timeouts of its own:

```ts
import { EndpointResolver, forwardMessage, Switchboard } from '@kaetemi/switchboard/node';

const gateway = new Switchboard({ resolver: new EndpointResolver(backendEndpoint) });
gateway.registerService('echo', async (incoming) => {
  await using upstream = await gateway.openTalk('echo');
  await incoming.talk().serve((message) => void forwardMessage(message, upstream));
});
```

Call it inside a `serve` handler, as above, so the message's frame credit is passed on too.

### Options

`TalkOptions`, given as `talkOptions` to a node, a client or `openTalk`, or to the `TalkChannel` constructor. Durations in milliseconds.

| Option | Default | |
| --- | --- | --- |
| `requestTimeout` | 15 000 | requester side |
| `replyTimeout` | 10 000 | responder side |
| `minExtension`, `maxExtension` | 1000, 3 600 000 | the clamp on declared extensions |
| `extendBuffer` | 5000 | added on the wire by `extend()` |
| `maxIncomingRequests`, `maxOutgoingRequests` | 1024, 1024 | beyond: `RESOURCE_EXHAUSTED` |
| `bulkThreshold`, `bulkChunkSize` | the channel's `maxSubframeLength`, 64 KiB | when a payload goes bulk, and its chunks (a positive integer, else `RangeError`) |
| `maxInlinePayload`, `reassemblyBudget` | 16 MiB, 16 MiB | reassembly bounds |
| `bulkWindow`, `maxUnclaimedBulk` | 1 MiB, 16 | the window of a bulk channel, and bulk channels ahead of their messages |
| `bulkOpenTimeout`, `bulkIdleTimeout` | 10 000, 30 000 | waiting for a bulk channel, and a stalled one |
| `streamBulk` | none | which bulk messages are delivered as a stream |
| `creditTimeout` | 60 000 | how long a stream request may stay without item credit |

### Closing

`await using talk = await node.openTalk(...)` closes the Talk channel at the end of the block. `talk.close(status?)` closes it (outstanding requests fail with the status, or `CANCELLED` for OK; with OK, bulk payloads being sent finish first); `talk.abort(status)` sends a channel abort, which the peer's `messages` iteration throws as a `TalkAbortError` with `isChannelAbort`; `talk.done` resolves with the end status. Close Talk channels before closing their node: the node's `close()` waits up to `goAwayGrace` for channels still open.
