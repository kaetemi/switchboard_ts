# Naming and Sharding

A **naming service** (`_ns`) hands out instance ids and keeps the table of the live service instances of a mesh; nodes register what they serve, and resolve what they open, through it. **Sharding** spreads a type's state over its instances by **slots**: a key maps to one of a fixed number of slots, each slot is owned by one instance at a time, and channels for a slot go to its owner, with hand-overs when slots move. The protocol is specified on the wiki pages `naming` and `sharding`.

## A mesh in one page

```ts
import { MeshNode, NamingService, ServiceAddress, Services, Switchboard } from '@kaetemi/switchboard/node';

// The naming node hosts the service as `_ns/1`.
const namingNode = new Switchboard();
const naming = new NamingService();
namingNode.registerService(Services.naming, naming.handler, { instance: 1 });
const namingEndpoint = await namingNode.listenTcp('127.0.0.1', 0);
naming.registerLocal(Services.naming, namingNode.listeningEndpoints, { instance: 1 });

// A backend listens, joins the mesh and publishes a service; the naming service assigns its id.
const backend = new Switchboard();
await backend.listenTcp('127.0.0.1', 0);
const backendMesh = MeshNode.join(backend, namingEndpoint);
const id = await backendMesh.publish('chat', (incoming) =>
  incoming.talk().serve((message) => message.reply(`chat/${id}: ${message.text()}`)),
);

// A consumer joins too: the naming resolver becomes its resolver.
const consumer = new Switchboard();
const consumerMesh = MeshNode.join(consumer, namingEndpoint);
await consumerMesh.synced;
{
  await using any = await consumer.openTalk('chat'); // any instance, round robin
  console.log((await any.request('SAY', 'hello')).text());
  await using exact = await consumer.openTalk(new ServiceAddress('chat', id)); // this instance
  console.log((await exact.request('SAY', 'again')).text());
}

await consumerMesh.leave();
await consumer.close();
await backendMesh.leave(); // unregisters; the naming service publishes DOWN
await backend.close();
await naming.close();
await namingNode.close();
```

A record is owned by the channel that registered it: when that channel closes for any reason, the record goes and `DOWN` is published. There is no other liveness mechanism. For a while after it starts (`assignmentHold`, 2 s by default), the service holds new registrations so that instances reconnecting after its restart reclaim their ids first.

## The naming service

`new NamingService(options)` serves the Talk channels its `handler` (or `serve(talk, ...)`) is given. Options (durations in ms): `assignmentHold` (2000), `verifier` (check credentials: the peer identity of the connection, else a bearer credential in the channel's payload), `requireCredential` (refuse channels without an identity), `issuer` (re-issue credentials for `RENEW`; a `CredentialIssuer` will do) and `renewable`, `brokerTimeout` for `CONNECT`, and the slot settings `holderGrace`, `handoverTimeout`, `maxSlotCount`, `assignBackoff`, `assignBackoffMax`, `holdingSettle`, `handoverMaxDuration`, `resumeAttempts`; `clock`, `wallTime`, `logger`. An identified channel may do what its credential's scopes grant (`register`, `watch`, `claim`, `migrate`, `broker` for the types), and a channel whose credential expires is ended unless renewed. `registerLocal(type, endpoints, { instance, metadata })` adds records the service owns itself, such as `_ns/1`. `table`, `records(type)`, `slotTables`, `events` and `on('event')` show its state.

A naming service's listener should not be open to untrusted peers without a policy: `ChannelPolicies.scoped()` admits `_ns` for every identified peer, and the service checks the naming rights itself (see [Identity](Identity)).

## MeshNode

`MeshNode.join(node, namingEndpoint, options)` is the usual setup of a backend node. It creates a `NamingClient` that reaches the naming service through the node (`namingClientFor`), sets the node's `resolver` to a `NamingResolver` over it, and starts the client; it returns at once, keeps reconnecting in the background, and `synced` resolves once the table is mirrored.

| Member | |
| --- | --- |
| `publish(type, handler, { instance, endpoints })` | registers a local service (instance 0: assigned; endpoints: the node's listening endpoints at the time of the call, so listen first) and dispatches `(type, id)` to the handler; resolves with the id |
| `publishRelay(relay, options)` | publishes a `RelayService` as `_relay` with the node's identity as metadata ([Proxy and Relay](Proxy-and-Relay)) |
| `unpublish(type, instance)` | unregisters and stops dispatching |
| `publishSharded(type, lifecycle, { count, mode, ... })` | publishes a sharded type behind a `SlotGate` (see [Serving slots](#serving-slots-slotgate)); resolves with the gate |
| `claimSlot(type, slot, { holding })`, `releaseSlot(type, slot, { keepStorage })`, `gates` | the slots of the sharded types published here |
| `migrateSlot(type, slot, { to })`, `slotOwners(type)` | move a slot of any type, and the instances serving a type's slots |
| `client`, `resolver`, `synced`, `published`, `brokeredConnections`, `nextRenewalAt` | the parts and the state |
| `leave()` | hands the node's slots over (or releases them), stops renewal and `CONNECT`, closes the resolver and the client (every registration goes, with `DOWN`), stops local dispatch; resolves with a `LeaveReport`. `await using mesh = MeshNode.join(...)` leaves at the end of the block |

Options: `reconnectDelay` (1000), `resolveTimeout` (5000), `talkOptions`, `leaveTimeout` (5000), `brokeredPolicy` (the policy of connections the node dials for a relayed `CONNECT`; default `ChannelPolicies.scoped()`), `renewCredential` (default true: the node's credential is renewed through `RENEW` at two thirds of its lifetime and presented again on every connection), `watch` (false for a worker whose credential has no `watch` scope), `wallTime`, `clock`, `logger`. Call `leave()` before closing the node.

An instance can also register **without endpoints** (`publish(type, handler, { endpoints: [] })`): it listens nowhere, and a consumer reaches it through the naming service's `CONNECT`, which has the instance dial the consumer back (the node does this for you: `Switchboard.broker`, used by the open methods when a record has no endpoints), or through a relay.

## The naming client and resolver

`NamingClient` is the client half on its own, over any way of opening a Talk channel to `_ns`: `register(type, endpoints, { instance, onAssigned, metadata })`, `unregister`, the mirrored `table`, `records(type)`, `record(address)`, `events` and `on('event')`, `isSynced`, `synced`, `firstSynced`, `renew()`, `connectTo(...)`, and the slot requests `defineSlots`, `declareHolding`, `claim`, `release`, `locate`, `migrate`, with a `slotHandler` for the requests the service sends to instances. `namingClientFor(node, namingEndpoint, options)` makes one that goes through a node. `NamingResolver(client, { resolveTimeout })` is a `Resolver` over its table, a `BrokeringResolver` (`connectTo`) and a `SlotResolver` (the slot tables).

## Slots

A sharded type has a **slot space**: `count` slots numbered from 0, in `managed` mode (the naming service assigns free slots to the instances and rebalances) or `static` mode (instances claim the slots they are configured for; the service records, arbitrates and orchestrates), optionally `lazy` (assigned on first `LOCATE`) and `shared` (state recoverable from shared storage). Keys map to slots the same way in every language:

```ts
import { slotForKey, slotForText } from '@kaetemi/switchboard';

console.log(slotForText('player-42', 64)); // FNV-1a 32 of the UTF-8 bytes, modulo the count: 49
console.log(slotForKey(new TextEncoder().encode('player-42'), 64)); // the same
```

### Routing by slot

A node whose resolver knows the type's slot table (a `NamingResolver`, or a `StaticResolver` with configured slots) routes a channel with a shard slot to the slot's owner (the old owner while the slot migrates); for a type without a slot table the slot is a plain hint (`candidates[slot mod n]`).

```ts
import {
  Name,
  ServiceAddress,
  SlotEntry,
  SlotMode,
  SlotSpace,
  StaticResolver,
  Switchboard,
  serviceRecord,
  slotForText,
} from '@kaetemi/switchboard/node';

const npc = Name.of('npc');
const workers = [new Switchboard(), new Switchboard()];
const resolver = new StaticResolver();
for (const [i, worker] of workers.entries()) {
  const instance = i + 1;
  worker.registerService(
    npc,
    (incoming) =>
      incoming.talk().serve((m) => m.reply(`npc/${instance} slot ${incoming.address.shard}`)),
    { instance },
  );
  const endpoint = await worker.listenMemory();
  resolver.add(serviceRecord(new ServiceAddress(npc, instance), { endpoints: [endpoint] }));
}
// Four slots: 0 and 1 on npc/1, 2 and 3 on npc/2.
resolver.defineSlots(new SlotSpace(npc, { count: 4, mode: SlotMode.static }));
for (let slot = 0; slot < 4; slot++) {
  resolver.setSlot(npc, slot, SlotEntry.owned(slot < 2 ? 1 : 2, { epoch: 1 }));
}

const router = new Switchboard({ resolver });
const slot = slotForText('player-42', 4); // 1
console.log(String(await router.resolveSlotOwner(npc, slot))); // npc/1
{
  await using talk = await router.openTalkToSlot(npc, slot);
  console.log((await talk.request('WHO')).text()); // npc/1 slot 1
}
await router.close();
for (const worker of workers) await worker.close();
```

`openChannelToSlot(type, slot, options)` and `openTalkToSlot` add the `MOVED` retry of the sharding page: if the owner rejects the channel with `MOVED` before anything was sent or received, it is opened once more at the owner the rejection names (or the resolver's newer entry, or one found with `LOCATE`), holding what was sent meanwhile (at most one window). A `MOVED` after the first subframe, or a `RELOCATED` (the owner was serving the channel when the slot moved), ends the channel: `MOVED` means nothing was processed, so the caller may open again and resend. `resolveSlotOwner(type, slot, { refresh })` asks where a slot is. The proxy routes by slot the same way ([Proxy and Relay](Proxy-and-Relay)).

### Serving slots: SlotGate

On the instances, a `SlotGate` serves a sharded type's slots for a `SlotLifecycle` you write: it answers the naming service's `ASSIGN`, `DRAIN`, `FORWARD` and `RESUME`, queues channels for slots that are loading or locked, forwards them to the new owner after a hand-over, and rejects channels for slots it does not serve with `MOVED`.

```ts
import { AssignResult, type IncomingChannel, SlotLifecycle, type SlotLoadOptions } from '@kaetemi/switchboard/node';

class Npcs extends SlotLifecycle {
  readonly state = new Map<number, Map<string, string>>();

  async load(slot: number, options: SlotLoadOptions): Promise<AssignResult> {
    // options.holder: where the slot's state is (0 nowhere, this instance, another one).
    this.state.set(slot, new Map());
    return AssignResult.notHolding; // a stateless service, or state in shared storage
  }

  override unload(slot: number): void {
    this.state.delete(slot);
  }

  serve(channel: IncomingChannel, slot: number): unknown {
    return channel.talk().serve((message) => message.reply(`slot ${slot}`));
  }
}
```

`load` resolves once the slot can be served (`AssignResult.holding` when this instance now keeps its storage); `drain(slot, { to, epoch })` makes the state available to the new owner before a hand-over; `unload` drops it; `discard(slots)` drops local storage the naming service reports as no longer held. The gate (`new SlotGate(node, namingClient, type, { lifecycle, forwardGrace, drainTimeout, maxQueuedChannels, maxQueuedRequests, trackChannels, noSlotHandler })`) also has `servedSlots`, `stateOf(slot)`, `claim(slot)`, `release(slot, { keepStorage })`, `serveRequest` for requests inside long-lived channels, `movedStatus` and `relocatedStatus`; `SlotGates` combines the gates of several types into one slot handler of a naming client.

`MeshNode` publishes a sharded type in one call: `publishSharded(type, lifecycle, options)` creates the gate, registers the type with the gate's handler (as `publish` does, with `instance` and `endpoints`; stateful services set a stable configured instance so that holders survive a reboot), defines or confirms the slot space (`count`, `mode`, `lazy`, `shared`, and this instance's `capacity`, 0 taking no slots from the allocator), declares the slots whose storage this instance has (`holding`), and resolves with the gate; the gate's own options (`forwardGrace`, `drainTimeout`, `maxQueuedChannels`, `maxQueuedRequests`, `trackChannels`, `noSlotHandler`) go in the same object. `claimSlot(type, slot, { holding })` claims a free slot (loaded before it resolves; resolves with its epoch) and `releaseSlot(type, slot, { keepStorage })` releases one; `gates` (a `SlotGates`, `gates.get(type)`) holds the gates and is the naming client's slot handler. `leave()` hands the node's slots over to other instances where it can, then releases those it could not hand over (keeping their storage), each step bounded by `leaveTimeout` (5 s); its `LeaveReport` has `handedOver`, `released` and `dropped`, maps keyed by `Name.key` of sorted slot numbers, and `isComplete` when every slot was handed over.

```ts
import { MeshNode, SlotMode, slotForText, Switchboard } from '@kaetemi/switchboard/node';

const node = new Switchboard();
await node.listenTcp('127.0.0.1', 0);
const mesh = MeshNode.join(node, namingEndpoint);
const gate = await mesh.publishSharded('npc', new Npcs(), { count: 64, mode: SlotMode.managed });
// The naming service assigns the slots of a managed space; gate.servedSlots maps slot to epoch.

// Any node of the mesh reaches a slot's owner (a LOCATE assigns a free slot first):
const consumer = new Switchboard();
const consumerMesh = MeshNode.join(consumer, namingEndpoint);
await consumerMesh.synced;
{
  await using talk = await consumer.openTalkToSlot('npc', slotForText('player-42', 64));
  console.log((await talk.request('WHO')).text()); // slot 49
}
await consumerMesh.leave();
await consumer.close();

const report = await mesh.leave(); // hands the slots over, or releases them keeping their storage
console.log(String(report)); // LeaveReport(handed over {}, released {npc: [49]}, dropped {})
console.log(gate.servedSlots.size); // 0
await node.close();
```

### Migrations

`mesh.migrateSlot(type, slot, { to })` (or `NamingClient.migrate`) moves a slot through the hand-over: the old owner's gate locks the slot and waits for its work in flight (`drainTimeout`), `drain` makes the state available, the new owner loads it, the old gate forwards queued and late channels for `forwardGrace`, then unloads. The iteration yields the phases and ends when the migration is done.
