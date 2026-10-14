# Identity

A connection's peer proves who it is with a **credential**: a small signed document naming an identity, a kind (node, client, device), the **scopes** it holds (rights over service types) and an expiry, issued by the mesh's **authority**. The peer presents it in an `IDENT` that is bound to the connection (its nonces, and over QUIC the TLS exporter), so a credential seen on one connection cannot be replayed on another. Receivers verify credentials offline with the authority's key. The protocol is the wiki page `identity`; this page is the TypeScript API, all of it in `@kaetemi/switchboard` (and re-exported by `/node` and `/web`).

## The authority

Two kinds of mesh, chosen per mesh:

- **Ed25519**: the authority keeps a key pair and receivers verify with its public key; credentials can name a **holder key**, the Ed25519 key of the holder, which then has to sign every `IDENT` (proof of possession).
- **HMAC-SHA256**: one secret shared by the mesh; cheapest, but anyone holding the key can issue, and the credentials are **bearer** only (no holder key, no proof).

```ts
import { CredentialIssuer } from '@kaetemi/switchboard';

const authority = await CredentialIssuer.ed25519FromSeed('auth1', seed); // the 32-byte private key
const fromPair = CredentialIssuer.ed25519('auth1', keyPair); // a WebCrypto CryptoKeyPair
const mesh = CredentialIssuer.hmac('mesh1', secret); // at least 16 bytes; 32 random bytes recommended
console.log(String(authority), String(fromPair), String(mesh));
```

The first argument is the key id written into every credential, by which receivers find the key to verify it with (an 8-byte name).

## Holder keys

```ts
import { Ed25519HolderKey } from '@kaetemi/switchboard';

const holder = await Ed25519HolderKey.generate();
const fixed = await Ed25519HolderKey.fromSeed(holderSeed);
const stored = await Ed25519HolderKey.fromKeyPair(
  await crypto.subtle.generateKey('Ed25519', false, ['sign', 'verify']), // non-extractable is fine
);
console.log(holder.publicKey.length, fixed.publicKey.length, stored.publicKey.length); // 32 32 32
```

The private key never leaves the holder (nor WebCrypto). Anything with a 32-byte `publicKey` and an async `sign(message)` (the `HolderKey` interface) can stand in, for a key kept in a token or another process.

## Issuing

```ts
import { Credential, CredentialKind, Right, Scope } from '@kaetemi/switchboard';

const credential = await authority.issue({
  kind: CredentialKind.node,
  identity: 'worker-a1',
  scopes: [Scope.of(Right.register, 'worker-*'), Scope.of(Right.open, 'echo')],
  holderKey: holder.publicKey, // omit for a bearer credential (the only kind HMAC issues)
  lifetime: 3_600_000, // ms; default 8 hours; null never expires (discouraged)
});
console.log(String(credential)); // Credential(node "worker-a1", key auth1, expires ..., register worker-*, open echo)

const bytes = credential.encode();
const again = Credential.decode(bytes); // synchronous, byte-exact; ProtocolError for malformed bytes
console.log(again.equals(credential), again.isBearer, again.isExpired());
```

`issuedAt` (ms since the epoch, default now) can be given too; times are rounded down to the second on the wire. An issued credential is at most `Credential.maxLength` (637) bytes, the most an `IDENT` carries.

### Scopes and rights

A scope grants a **right** over the service types its **pattern** matches. The rights are `Right.register`, `open`, `watch`, `claim`, `migrate`, `broker` and `admin` (which allows everything). Patterns are names:

| Pattern | Matches |
| --- | --- |
| `worker-*` | any name starting with `worker-`, the empty suffix included: `worker-a1` and `worker-`, not `worker` |
| `*` | every name, the empty one too |
| `n*c` | only `n*c`: a `*` that is not last is an ordinary byte |
| `echo` | only `echo` |

`credential.allows(Right.open, 'echo')` checks a right; a decoded credential drops scopes whose right this implementation does not know (they could only grant more).

## Verifying

```ts
import { CredentialVerifier } from '@kaetemi/switchboard';

const verifier = new CredentialVerifier({ ed25519Keys: { auth1: await authority.publicKey() } });
await verifier.addIssuer(mesh); // the key of an issuer in the same process (here the HMAC key)

const verified = await verifier.verify(credential.encode()); // rejects UNAUTHENTICATED 'invalid credential'
console.log(verified.identity, verifier.cached(credential.encode())?.identity);

verifier.removeKey('mesh1'); // credentials of that key no longer verify
```

- `verify(bytes, { now })` checks the signature against the key its key id names, that an HMAC credential is a bearer one, and the expiry; every refusal is the same `SwitchboardError` `UNAUTHENTICATED` with the reason `invalid credential`, and the cause goes to the `logger` (`Switchboard.Identity`, at `debug`), never to the peer.
- Verified credentials are cached by their bytes (least recently used, `cacheSize` 1024 by default; 0 disables), so a credential costs one signature check however many channels carry it; concurrent checks of the same bytes share one. Expiry is checked on every call. `cached(bytes, { now })` answers synchronously from the cache, for paths that cannot wait.
- Rotation: add the new key under its own id, and remove the old one once the credentials it signed have expired. Changing the keys clears the cache.
- `now` is wall time in milliseconds since the epoch (default `wallTime`, `Date.now`), as for `credential.isExpired(now)`.

## Identity on nodes

A node identifies on the connections it initiates with its `credential` (and the `holderKey` the credential names), and checks the `IDENT`s its peers send with its `verifier`. Policies decide what an identified peer may open.

```ts
import { ChannelPolicies, EndpointResolver, Switchboard } from '@kaetemi/switchboard/node';

const server = new Switchboard({ verifier });
server.registerService('echo', (incoming) => {
  console.log(incoming.peerIdentity?.identity, incoming.peerIdentity?.provedPossession); // worker-a1 true
  return incoming.talk().serve((message) => message.reply(message.text()));
});
const endpoint = await server.listenTcp('127.0.0.1', 0, { policy: ChannelPolicies.scoped() });

const client = new Switchboard({
  resolver: new EndpointResolver(endpoint),
  credential,
  holderKey: holder,
  verifier,
});
{
  await using talk = await client.openTalk('echo'); // allowed: the credential has `open echo`
  console.log((await talk.request('PING', 'hi')).text());
}
await client.close();
await server.close();
```

| Policy | Admits |
| --- | --- |
| `ChannelPolicies.scoped()` | a channel only if the peer's credential has `open` for its type (`_ns` and `_relay` for any identified peer, which check rights themselves); an unidentified peer is refused `UNAUTHENTICATED`, which holds its channels for `identityTimeout` (10 s) so an `IDENT` that is still on its way counts |
| `ChannelPolicies.scoped({ requireIdentity: false })` | the same for identified peers; unidentified peers pass unchecked (a mesh moving to identities) |
| `ChannelPolicies.requireIdentity()` | every channel of an identified peer |
| `ChannelPolicies.all([a, b])` | what every policy admits |
| `ChannelPolicies.denyReserved` | everything outside the reserved `_` types: the least for any listener untrusted peers can reach |

A refused channel is closed `PERMISSION_DENIED` (or the code a policy throws). `connection.peerIdentity` (and `incoming.peerIdentity`) is the `PeerIdentity` of the last valid `IDENT`: `identity`, `kind`, `scopes`, `allows(right, name)`, `provedPossession`, `receiver`, `intent`, `verifiedAt`, `isExpired(now)`.

Node options that shape identification: `identifyOutgoing` (default true) and `identifyFor(endpoint)`; `expectedIdentityFor(endpoint, record)`, the identity an `IDENT` names as its receiver; `identityTimeout`; `credentialFor(endpoint, record)` chooses the application payload of the channels the node opens, for a credential carried there instead (the naming service accepts a bearer credential in the payload of its channel). `node.updateCredential(credential)` re-identifies on every connection the node identified on (a `MeshNode` renews the node's credential through the naming service at two thirds of its lifetime and does this).

## On a connection

The same exchange on a connection directly:

```ts
import { FrameMuxConnection, memoryTransportPair, Name, Right } from '@kaetemi/switchboard';

const [a, b] = memoryTransportPair();
const left = new FrameMuxConnection(a, { isInitiator: true });
const right = new FrameMuxConnection(b, {
  isInitiator: false,
  identityVerifier: verifier, // without one, IDENTs are ignored (NONCE is still answered)
  requireNamedIdent: true, // refuse an IDENT that names no receiver
});
right.localIdentity = 'gate';

await left.identify(credential, { holderKey: holder, receiver: 'gate' });
const peer = await right.peerIdentified;
console.log(peer.identity, peer.receiver, peer.allows(Right.register, Name.of('worker-b'))); // worker-a1 gate true
await left.close();
await right.close();
```

`identify(credential, { holderKey, receiver, intent, timeout, signal })` runs the NONCE exchange and sends the `IDENT`. A holder-key credential needs its key pair (another key, or none, is refused before anything is sent); a holder key given with a bearer credential is ignored, as in Dart. `intent` (at most 64 bytes) is empty unless answering a naming service's `CONNECT`. An `IDENT` that fails verification ends the connection with GOAWAY `UNAUTHENTICATED`; its reason never reaches the wire.

## Exporter-bound identity over QUIC

Over the QUIC and WebTransport binding the proof also signs the TLS session's exporter (`SWBIDTLS` instead of `SWBIDENT`; the label `EXPORTER-Channel-Binding` on raw QUIC, `switchboard-ident` on WebTransport), so it cannot be relayed by a man in the middle. Between nodes, certificates are self-signed and TLS authenticates nobody, so where identity is in use the binding applies the specification's rules (`QuicIdentityRules`):

- **A node listener** (`QuicIdentityRules.listener`, the default of `listenQuic` and `listenWebTransport`) refuses nonce-bound `IDENT`s, identifies in return only to a peer whose exporter-bound `IDENT` verified (`identifyInReturn`, default true), and presents a bearer credential only to such a peer.
- **A dialling node** (`QuicIdentityRules.initiator`, computed per dial) whose acceptor's certificate is not verified (no pin, no platform roots) and that uses identity requires the exporter binding and waits for the acceptor's exporter-bound `IDENT` naming it before the connection counts; channels opened before wait for it, up to `identityTimeout`.
- **A pinned or verified certificate** (`quicBinding: { serverCertificateHashes: [...] }`, or `verifyCertificate`) authenticates the acceptor and lifts those requirements, as a browser does: browsers verify certificates (or the `serverCertificateHashes` pin), and Chromium has no exporter, so a browser's `IDENT`s are nonce-bound, which is safe there.

`QuicIdentityRules.none` drops every requirement (nonce-bound proofs over QUIC, as over the frame mux), for a listener that serves only browsers, which verify its certificate. `QuicBindingConnection.peerAuthenticated` tells whether the peer is an authenticated end before an application sends a credential in a channel payload.
