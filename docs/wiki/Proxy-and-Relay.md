# Proxy and Relay

Three ways a node forwards channels it does not serve itself, and a way to keep a connection to every instance of a type:

- **The proxy** (`proxyHandler`): a frontend endpoint that clients reach with one address; it authorizes each channel, resolves the service it addresses in the mesh behind it and pipes the two channels together.
- **`pipeChannels`**: the piping on its own, for any two channels.
- **The relay** (`RelayService`, `RelayConfig`): forwards channels between a consumer and an instance that both listen nowhere.
- **`PeerSet`**: one connection per instance of a type, kept up.

The protocol is specified on the wiki pages `proxying` and `relay`.

## The proxy

```ts
import {
  ChannelPolicies,
  EndpointResolver,
  proxyHandler,
  ServiceAddress,
  StaticResolver,
  Switchboard,
  serviceRecord,
  utf8Decode,
} from '@kaetemi/switchboard/node';

// A backend service, and the mesh's table of it (a NamingResolver in a real mesh).
const backend = new Switchboard();
backend.registerService(
  'profile',
  (incoming) =>
    incoming.talk().serve((m) => m.reply(`profile of ${utf8Decode(incoming.address.payload)}`)),
  { instance: 1 },
);
const backendEndpoint = await backend.listenMemory();
const mesh = new StaticResolver([
  serviceRecord(new ServiceAddress('profile', 1), { endpoints: [backendEndpoint] }),
]);

// The frontend endpoint: every channel goes through the proxy.
const edge = new Switchboard({ resolver: mesh });
edge.catchAll = proxyHandler(edge, {
  authorize: async (incoming) => {
    const session = utf8Decode(incoming.address.payload);
    if (session !== 'session-token') return undefined; // UNAUTHENTICATED
    return incoming.address.with({ payload: 'user-7' }); // what the backends trust
  },
});
const edgeEndpoint = await edge.listenMemory({ policy: ChannelPolicies.denyReserved });

// A client knows only the endpoint; its default payload is its session credential.
const client = new Switchboard({
  resolver: new EndpointResolver(edgeEndpoint),
  defaultPayload: 'session-token',
});
{
  await using talk = await client.openTalk('profile');
  console.log((await talk.request('GET')).text()); // profile of user-7
}
await client.close();
await edge.close();
await backend.close();
```

For each channel the proxy (installed as `catchAll`, or registered for a type):

1. checks `allow(address)` (default: every type outside the reserved `_` namespace, so the mesh's naming service is not reachable through it): refused `PERMISSION_DENIED`;
2. bounds the channels being forwarded per client connection (`maxChannelsPerConnection`, 256): `RESOURCE_EXHAUSTED` beyond;
3. calls `authorize(incoming)` if given (bounded by `authorizeTimeout`, 10 s): it returns the address to forward to, the channel's own or a rewritten one (the shard set from the verified account, the payload replaced by a credential the backends trust), or undefined to refuse with `UNAUTHENTICATED`; a throw refuses with `PERMISSION_DENIED`. Meanwhile the channel is not read;
4. resolves the destination with `resolver` (default the node's), as the node's own opens do: by slot for a type with a slot table, never to the node's own listeners; with a host hint in the address and `Switchboard.allowHostHint` set, it connects to that host instead (**which makes the node an open relay**);
5. opens the channel there with the same open payload (the instance set, the host hint removed) and joins the two with `pipeChannels`. For a slot-routed channel a `MOVED` before the first subframe is retried at the new owner without the client noticing.

Refusals carry the status code and a generic reason only; the details are logged (`Switchboard.Router`). `MOVED` and `RELOCATED` reach the client without their owner and epoch fields unless `revealOwners`. Further options: `allowExplicitInstance`, `allowNoSlot`, `maxLocatesPerConnection` and `locateRefillInterval` (the bucket of `LOCATE`s a client connection may cause). A proxy should not grant its clients more window or frame size than its backends grant it: subframes are forwarded whole.

## pipeChannels

```ts
import { ChannelAddress, pipeChannels, Switchboard } from '@kaetemi/switchboard/node';

node.registerService('tunnel', async (incoming) => {
  const upstream = await node.openChannelAt(target, new ChannelAddress({ type: 'echo' }));
  await pipeChannels(incoming.channel, upstream); // resolves when both are done; never rejects
});
```

`pipeChannels(a, b)` forwards subframes both ways with flow control passed through (a reader that stops on one side stalls the sender on the other), forwards the close status of either side to the other (`CONNECTION_LOST` as `UNAVAILABLE`), and forwards Talk bulk channels as bulk channels, so `BULK` messages pass unchanged. Neither channel's `readable` may be in use already.

## The relay

Instances that register without endpoints listen nowhere; a consumer that can accept connections reaches them by brokering (the naming service's `CONNECT` has the instance dial the consumer back). A consumer that listens nowhere either, such as a client behind NAT, goes through a **relay** instead: a node that listens where both can reach it, registered as `_relay` with its identity in the record's metadata.

```ts
import {
  ChannelPolicies,
  type Credential,
  type CredentialVerifier,
  type Ed25519HolderKey,
  MeshNode,
  RelayConfig,
  RelayService,
  Switchboard,
} from '@kaetemi/switchboard/node';

// The relay node: its own credential (`register _relay`, and `broker`, `open`, `watch`
// for the types it relays), a verifier, named identities only, a scoped listener.
export async function relayNode(
  namingEndpoint: string,
  relayCredential: Credential,
  relayKey: Ed25519HolderKey,
  verifier: CredentialVerifier,
): Promise<MeshNode> {
  const node = new Switchboard({
    muxOptions: { requireNamedIdent: true },
    credential: relayCredential,
    holderKey: relayKey,
    verifier,
    expectedIdentityFor: (endpoint) => (endpoint === namingEndpoint ? 'ns' : undefined),
  });
  await node.listenTcp('0.0.0.0', 9400, { policy: ChannelPolicies.scoped() });
  const mesh = MeshNode.join(node, namingEndpoint);
  await mesh.publishRelay(new RelayService(node));
  return mesh;
}

// A consumer that listens nowhere reaches instances without endpoints through the relays.
export function relayedConsumer(credential: Credential, holderKey: Ed25519HolderKey): Switchboard {
  return new Switchboard({ credential, holderKey, relay: new RelayConfig() });
}
```

`RelayService(node, { requireIdentity, allowEndpoints, maxChannelsPerConnection, resolver })` serves `_relay` channels whose application payload is the open payload of the channel to relay. It holds a channel until its consumer identifies (`requireIdentity`, default true), refuses inner channels of reserved types or with a host hint (`INVALID_ARGUMENT`), checks the consumer's `open` scope for the inner type (`PERMISSION_DENIED`), brokers a connection to the instance through the naming service's `CONNECT` (one per instance however many consumers), and pipes the two channels. By default it relays only to instances registered without endpoints; a destination with endpoints is refused `FAILED_PRECONDITION`, since the consumer can reach it directly, unless `allowEndpoints`.

`RelayConfig({ type, endpoints, identity })` is a node's `relay` option: the relays are the records of `type` (default `_relay`) in the node's resolver, round robin with failover, each named by the identity in its metadata; or explicit `endpoints` with the `identity` they present. A node that cannot broker (it listens nowhere) opens channels to instances without endpoints through the relay; `node.resolveRelays()` lists the relays it would try.

## PeerSet

```ts
import { Name, PeerSet, ServiceAddress, StaticResolver, Switchboard, serviceRecord } from '@kaetemi/switchboard/node';

const gpu = Name.of('gpu');
const resolver = new StaticResolver();
const workers: Switchboard[] = [];
for (const instance of [1, 2]) {
  const worker = new Switchboard();
  worker.registerService(gpu, (incoming) => incoming.talk().serve((m) => m.reply(`gpu/${instance}`)), {
    instance,
  });
  resolver.add(
    serviceRecord(new ServiceAddress(gpu, instance), { endpoints: [await worker.listenMemory()] }),
  );
  workers.push(worker);
}

const consumer = new Switchboard({ resolver });
const fleet = PeerSet.watch(consumer, gpu);
await fleet.ready; // the records are added; their connections are being made
for await (const event of fleet.events) {
  console.log(String(event)); // PeerEvent(online gpu/1), ...
  if (fleet.online.length === 2) break;
}
for (const peer of fleet.online) {
  await using talk = await fleet.openTalk(peer.address.instance);
  console.log((await talk.request('WHO')).text());
}
await fleet.close(); // before the node
await consumer.close();
for (const worker of workers) await worker.close();
```

`PeerSet.watch(node, type, options)` follows the resolver's records of the type and keeps one connection per instance (dialled with `Switchboard.dial`, never pooled), each with its own backoff, as a `ReconnectingClient` does: a worker that reboots disturbs nothing else, and a GOAWAY `GOING_AWAY` from it makes the set reconnect at once. Instances without endpoints are brokered, or relayed through the node's `relay`. A record that leaves the resolver is held for `removalHoldDown` (15 s) before the peer is removed, so a restarting naming service does not cut every job in flight.

A peer is **online** only once usable: connected, `onConnect(peer)` run, the per-peer `channel` (an address opened on every connection) opened and `onOpen(peer, channel)` run. `peers` (by instance), `online`, `events` and `on('event')` (`added`, `online`, `offline`, `updated`, `held`, `removed`, with the status), `ready`, `openChannel(instance, { payload })` and `openTalk(instance, options)` work on them; `Peer` has `address`, `record`, `state`, `lastStatus`, `endpoint`, `viaRelay`, `connection`, `channel`, `isOnline`. Options: `resolver`, `onConnect`, `channel`, `onOpen`, `policy`, the backoff settings (`initialBackoff` 500, `maxBackoff` 30 000, `backoffFactor` 2, `jitter` 0.2), `connectTimeout` (10 000), `removalHoldDown`, `random`, `clock`, `logger`.
