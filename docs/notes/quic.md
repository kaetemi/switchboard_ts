# Switchboard TS: QUIC and WebTransport porting notes

Phase 1 notes of the QUIC transport thread, for the lead's design. Written 2026-10-14 from `kaetemi/switchboard_quic` `c5b3265`, the binding page in `kaetemi/sway` `a5c2dbc`, and runtime probes run in this session. Sections 9 and 12 are the survey and the recommendation; sections 1 to 8 are what any TS implementation has to reproduce.

## 0. Summary

* **The binding is pure logic and ports as-is.** The Dart package is three layers: the binding (records, a stream per channel, the control stream with `HELLO`, identity rules, GOAWAY and close; about 4.1k lines), a small provider contract (the QUIC surface the binding needs), and providers. The binding and the contract port to plain TS that runs in every runtime; the wiki's test vectors and an in-memory provider make it testable without a network.
* **No runtime ships a QUIC API we can build on for nodes, except through native code.** Node's `node:quic` is compiled out of every official binary (22, 24 and 26.11.1 checked) and has no WebTransport. Bun has nothing. Deno 2.9 has raw QUIC and WebTransport behind `--unstable-net`, but no TLS exporter and no certificate pinning, which the node-to-node identity rules need.
* **Recommendation**, detailed in section 12:
  * **Browsers:** the W3C `WebTransport` API (a port of the Dart web provider). Chrome/Edge 97+, Firefox 114+, Safari 26.4+; the `protocols` option from Chrome 143, Firefox 155, Safari 26.4; `exportKeyingMaterial` in Firefox 155 only.
  * **Node, Bun and Deno:** one Node-API addon written with napi-rs over the existing `sbq` Rust core (quinn 0.11 plus its own HTTP/3 and WebTransport layer). Its doorbell becomes a non-blocking `ThreadsafeFunction`. This gives raw QUIC and WebTransport, client and server, the TLS exporter, pins, unverified mode, priorities and certificate generation, with the reference's own wire behaviour. Such an addon over the real `sbq` core was built here and completed a raw QUIC handshake with equal exporters at both ends in Node 22.22, 24.21 and 26.11, Bun 1.4.2 and Deno 2.9.6. The one change to `sbq` it needs is `"rlib"` in its `crate-type`.
  * **Later, behind feature detection:** `node:quic` once official builds include it, and Deno's built-in QUIC for deployments with verified certificates.
* **Stopgap that also works today:** the unchanged `sbq` cdylib through koffi (Node 22/26, Bun, Deno), `bun:ffi` or `Deno.dlopen`. All three were run here against the real shim. Node 26's own `node:ffi` aborts the process when the shim's notifier thread rings, so it cannot drive the shim as it stands.
* **Decisions for the lead** are in section 12.2: the provider contract's shape (keep Dart's pull-based contract), where the Rust code lives, how the addon is distributed, the runtime matrix, and how the QUIC work of Phase 2 splits into three milestones.

## 1. Sources

| What | Where | Commit |
| --- | --- | --- |
| Specification: "Polyverse Switchboard QUIC and WebTransport Binding" | `kaetemi/sway`, `wiki/switchboard/quic-binding.md` (483 lines, with test vectors) | `a5c2dbc` |
| Dart reference: binding, provider contract, native and web providers, Rust shim | `kaetemi/switchboard_quic` | `c5b3265` |
| Dart core it is tested against (`tool/core_commit.txt`) | `kaetemi/switchboard` | `3816e6f` |

Where the Dart code and the wiki disagree, the wiki wins (its own rule). Everything below was read from these commits; claims about runtimes in section 9 were checked here on 2026-10-14 unless marked otherwise.

The Dart package has three layers, and the port should keep the split:

1. **The binding** (`lib/src/binding/`, about 4.1k lines): records, channels on streams, the control stream, `HELLO`, identity rules, GOAWAY and close. Written once against a provider contract; knows nothing about any QUIC stack. Pure logic, runs anywhere (it already runs compiled to JS under `dart test -P node`).
2. **The provider contract** (`lib/src/provider/quic_provider.dart`, 395 lines): the smallest QUIC surface the binding needs, in RFC 9000 and W3C WebTransport vocabulary.
3. **Providers**: native (`lib/src/native/`, FFI to a Rust shim `native/sbq` over quinn 0.11 with its own HTTP/3 + WebTransport layer, about 10.7k lines of Rust) and web (`lib/src/web/`, the browser's W3C `WebTransport`).

Plus `QuicSwitchboardBinding` / `QuicSwitchboardListener` (node glue: dial by URI scheme, adopt accepted sessions) and `connectQuicBinding` (the dial on its own, what a browser frontend hands to `ReconnectingClient.connections`).

## 2. How Switchboard maps onto QUIC

The binding replaces both the transport frame contract and the frame mux. Everything above the mux (Talk, addressing, identity, naming, proxying, relays, sharding) is unchanged and sees the same channel contract (`MuxConnection` / `MuxChannel`; core's `BindingConnection` is the abstract base).

| Switchboard | QUIC / WebTransport |
| --- | --- |
| Connection | One raw QUIC connection (ALPN `switchboard`), or one WebTransport session (several sessions pooled on one QUIC connection are separate Switchboard connections, each with its own control stream, ids and exporter) |
| Initiator / acceptor | QUIC client or WebTransport client / server. Decides id parity and who opens the control stream |
| Channel | One bidirectional stream. Opener's direction starts with an OPEN record (`u48` channel id + open payload) |
| Subframe | One DATA record |
| Channel close | Each side ends its own direction: CLOSE record + FIN (normal, carries the full status payload), or `RESET_STREAM` with the status code |
| Control channel (channel 0) | The control stream: the initiator's first stream, OPEN record naming channel 0 with empty payload, then DATA records each holding one mux control message, both directions on that one stream |
| `LIMITS`, `CREDIT` | Not sent (protocol error if received). Replaced by `HELLO` + QUIC flow control and stream limits |
| Channel cap | `initial_max_streams_bidi` / `MAX_STREAMS` (+1 on the acceptor for the control stream), stated in `HELLO` too (advisory) |
| Unidirectional streams, datagrams | Not used. A received uni stream gets `STOP_SENDING` `UNSUPPORTED` (36) and is ignored; datagrams dropped. Raw QUIC sets `initial_max_streams_uni` 0 and no `max_datagram_frame_size` |

Endpoint URIs: `quic://host:port` (raw QUIC, port required, no path) and `https://host:port/path` (WebTransport, port defaults to 443). A connector that cannot dial a scheme fails with `unsupportedScheme`, which a node reads as `UNIMPLEMENTED` and moves to the record's next endpoint (so a browser that has only WebTransport, or a runtime without QUIC, uses the same service records). Pool key: scheme + host + port, plus path for `https` (`QuicSwitchboardBinding.normalise`). Certificate hashes are never part of an endpoint URI.

### 2.1 Records

```
u32 header (little endian): body length in bits 0-29, command in bits 30-31
body: exactly length bytes
```

| Command | Value | Body | Where |
| --- | --- | --- | --- |
| DATA | 0 | one subframe (possibly empty); on the control stream one control message | after the opener's OPEN, before the direction's CLOSE |
| OPEN | 1 (`0x40` in byte 3) | `u48` channel id, then the open payload | first record of the opener's direction only |
| CLOSE | 2 (`0x80` in byte 3) | status payload, absent = `OK` | last record of a direction, then FIN |
| reserved | 3 | | protocol error |

JS reads a header with `view.getUint32(off, true)`, then `>>> 30` and `& 0x3FFFFFFF` (the wiki says exactly this). Never use `>> 30` (sign). A `u48` is `getUint32(off, true) + getUint16(off + 4, true) * 2 ** 32`; it fits a JS number exactly.

**Record limit**: the largest body a receiver accepts on a channel stream, announced in its `HELLO`; at least and by default 32752 (half the mux's default 64 KiB window less 16). Senders never wait for `HELLO`: they send up to 32752 at once, larger only after the peer raised it. A header over the limit is `FRAME_TOO_LARGE` for the whole connection, checked before the body is read. The control stream's limit is one control message: `1 + 1024` bytes (type byte + payload). A CLOSE status payload is kept within 1024 bytes (reason cut at a UTF-8 boundary).

### 2.2 Control stream and `HELLO`

* The initiator opens the control stream before any channel and writes OPEN(0) + its `HELLO` in the same flight (one write). The acceptor recognises it by the OPEN record, never by stream id, writes its own `HELLO` first in its direction as soon as the stream arrives, and never opens a control stream of its own.
* `HELLO` (type `0x08`): `u32` record limit (>= 32752; above `0x3FFFFFFF` reads as `0x3FFFFFFF`), `u32` channels (0 = no stated limit; advisory), `u8` flags (`0x01` EXPORTER), further fields ignored. Exactly one per direction, first. Shorter than 9 bytes, limit below 32752, a second `HELLO`, anything before it, `LIMITS` (0x04), `CREDIT` (0x07), an empty record, a payload over 1024: protocol error. Unknown control types are ignored.
* Channels may arrive before the control stream; they are handled under the defaults meanwhile. An acceptor without the peer's `HELLO` within `controlStreamTimeout` (default: `MuxOptions.keepAliveInterval`, else 10 s; the Dart code applies it on both sides) closes with `PROTOCOL_ERROR`.
* CLOSE record, reset, `STOP_SENDING` or FIN on the control stream: protocol error, except over WebTransport a reset/stop **without** an application code (`WT_SESSION_GONE` of a terminating session can overtake the session close): wait up to 2 s (`webTransportSessionGoneGrace`) for the session to end, and only then call it a protocol error.
* Control output backpressure: if this side's queued control output exceeds `controlOutputMark` (64 KiB), it stops reading the peer's control direction until it drains (a peer that sends PINGs without reading cannot pile up PONGs). A local enqueue beyond mark + one max record fails the connection `RESOURCE_EXHAUSTED`.
* The acceptor's `HELLO` channels value = its stream limit less one (the control stream), capped by `MuxOptions.maxChannels`.

### 2.3 Wire vectors to port verbatim

The wiki's "Test vectors" section (records, negative records, control stream incl. `HELLO`, `PING`, `PONG`, `GOAWAY`, `NONCE`, `IDENT`, negative control messages, bulk OPENs, the status code to HTTP/3 code table, the exporter label/context bytes, and the exporter-bound Ed25519 proof **X** with its signature) is the conformance suite. `test/vectors_test.dart` checks each one; port that file first, as table-driven tests. The worked example (a browser opening `chat/1`, a 100000-byte reply as a bulk payload) is also a test there.

## 3. Connection setup, TLS, ALPN, WebTransport

### 3.1 Raw QUIC

* QUIC v1, TLS 1.3 only, ALPN `switchboard`, SNI = the URI host. No 0-RTT, ever (servers refuse early data: a replayed OPEN would be served twice). No client certificates.
* Client verification modes (the shim's): platform roots; pinned SHA-256 of the leaf certificate DER (constant-time compare); or none (the handshake signature is still checked). Between nodes the default is **none**: node certificates are self-signed and identity comes from exporter-bound `IDENT`s.
* Transport parameters: bidi streams = channels accepted (+1 on the acceptor), uni 0, no datagrams; stream receive window = `MuxOptions.initialWindow` (64 KiB default), connection window = `MuxOptions.receiveHighWaterMarkBytes` (16 MiB); idle timeout 30 s; stack keep-alive (QUIC PING) a third of it (10 s); handshake deadline 10 s (quinn alone would only bound a handshake by the idle timeout, so the shim enforces it; a dial to a black hole must fail, and a half-open handshake must not be held by a listener).
* The reference listens raw QUIC and WebTransport on **two UDP ports**: quinn fixes transport parameters per listener before ALPN is known, and HTTP/3 needs uni streams + the datagram parameter that raw QUIC must not grant. A stack that can choose parameters per ALPN may share one port.

### 3.2 WebTransport over HTTP/3

* QUIC with ALPN `h3`; HTTP/3 SETTINGS; extended CONNECT (`:protocol webtransport`) to `https://host:port/path`, offering `WT-Available-Protocols: "switchboard"` (Structured Field strings); the server selects it in `WT-Protocol`. A session where the server selected another protocol is not a Switchboard connection (the binding closes it `UNSUPPORTED`); no protocol selected is accepted.
* Draft versions in the wild (as the shim found them): Chrome 149 speaks draft-02 signalling (`SETTINGS_ENABLE_WEBTRANSPORT` `0x2b603742`, `H3_DATAGRAM` `0x33` and `0xffd277`, request header `sec-webtransport-http3-draft02: 1`); web-transport-quinn 0.13.2 speaks draft-07 (`SETTINGS_WEBTRANSPORT_MAX_SESSIONS` `0xc671706a`). The shim advertises and answers both. Later drafts (`SETTINGS_WT_ENABLED`, `webtransport-h3`) need `RESET_STREAM_AT`, which quinn 0.11 lacks, so they are not offered. Session flow control is never enabled (no `WT_INITIAL_MAX_*` settings), so a client uses one session per QUIC connection.
* Server answers (the reference listener): another path 404 (query ignored); `Origin` not in `allowedOrigins` 403 (no `Origin` = not a browser, passes; null list = any; exact string match on the serialized origin); offered protocols of which none is served 400; after close 503; unanswered within the handshake deadline 503. `:authority` is **not** checked (nodes are reached under other names/ports than advertised). Accepted sessions expose `requestPath`, `requestAuthority`, `requestOrigin`, `requestProtocols`, `requestDraft`.
* A client treats any non-2xx as a failed connect (`refused` in the provider, `UNAVAILABLE` at the binding). The reference client sends one session per QUIC connection, never pools (the W3C `allowPooling: false`).
* Stream and session codes are WebTransport application codes (u32), carried on the wire as HTTP/3 codes `0x52e4a40fa8db + n + floor(n / 0x1e)` (reserved codepoints skipped). A received HTTP/3 code outside that range (e.g. `WT_SESSION_GONE` resets) arrives as "no code" (`null`).
* Close: `WT_CLOSE_SESSION` capsule (u32 code, reason <= 1024 bytes) + FIN on the CONNECT stream. The reference's close completes once the peer acknowledged it or after a bounded wait (three RTTs, 0.2 to 2 s), and only then resets the session's streams with `WT_SESSION_GONE` and (client) closes the QUIC connection. A server leaves the QUIC connection close to the client for that wait: Chrome reports "Connection lost." if the connection close arrives before it has reported the session closed.
* Drain: `WT_DRAIN_SESSION` or HTTP/3 `GOAWAY` = the W3C `draining` promise = a `GOAWAY` with `GOING_AWAY` for the binding.
* Not done anywhere in the reference: datagrams, `RESET_STREAM_AT`, session flow control. Consequence: a stream reset before its WebTransport stream header reached the peer is invisible to the peer's binding (it cannot tell the session); the binding treats that like a stream given up before its OPEN.

### 3.3 Certificates and identity (the part that constrains provider choice)

* Exporter of a connection: 32 bytes of TLS keying material.
  * Raw QUIC: RFC 9266 `tls-exporter`: label `EXPORTER-Channel-Binding`, empty context, 32 bytes.
  * WebTransport: the session exporter: TLS label `EXPORTER-WebTransport`, context `u64 session ID (big endian) || len8 label || label || len8 context || context`, application label `switchboard-ident`, empty application context. (W3C API: `wt.exportKeyingMaterial(label, context)`.)
* `HELLO` sets `EXPORTER` exactly when this side can compute it. If both `HELLO`s carry it, `IDENT` proofs sign `"SWBIDTLS" || sender nonce || receiver nonce || exporter || len8 intent || intent || len8 receiver || receiver`; otherwise the nonce-bound `"SWBIDENT"` form of the identity page.
* `QuicIdentityRules` (port as-is; five booleans): `initiator({certificateVerified, hasCredential, verifiesCredentials})` and `listener` give the spec's defaults; `none` relaxes everything. With an unverified acceptor certificate and identity in use, the initiator requires the exporter binding, identifies first, waits for the acceptor's exporter-bound `IDENT` naming it before any channel opens, and refuses nonce-bound proofs; a node listener refuses nonce-bound `IDENT`s and identifies only to a peer whose exporter-bound `IDENT` verified. A listener serving only browsers (which verify its certificate) uses `none`.
* **Consequence for providers**: a provider that cannot export keying material can still carry Switchboard, but cannot authenticate a node with a self-signed certificate. It must then pin certificates or verify them against roots. So TLS exporter support is a hard requirement for a node-to-node provider, and a nice-to-have for browser clients. (This rules out Deno's built-in QUIC for node use, see section 9.)
* Browser certificate hashes (`serverCertificateHashes`): X.509v3, ECDSA P-256 only, validity current and at most 14 days, SHA-256 of the DER. The shim's `generateSelfSigned(subject, validityDays = 14)` makes exactly that, valid from five minutes ago. Rotation: a new cert every 7 days valid 14, publish its hash next to the current one, serve it a day later. The hashes come from the application, never the endpoint URI. Note for test fixtures: a self-signed certificate with `CA:TRUE` (OpenSSL's default for `req -x509`) is rejected by rustls/webpki as `CaUsedAsEndEntity`; generate with `basicConstraints=critical,CA:FALSE`.
* Chrome 149 (and Chromium 141 here) has no `exportKeyingMaterial`: browser `HELLO`s carry no `EXPORTER`, proofs are nonce-bound, which is safe because the browser verified the certificate.

## 4. Stream multiplexing rules

* **Channel ids**: initiator even from 2, acceptor odd from 3; reserved 0 (control), 1, `0xFFFF`, `0xFFFF00000000`, `0xFFFFFFFFFFFF`. Allocated in increasing order, reserved skipped, **never reused** on a connection (a `_bulk` stream may arrive after its parent ended and must not land on a reused id). A side that exhausts its parity (2^47) opens new channels on a new connection (`RESOURCE_EXHAUSTED`).
* **Opening**: open a stream, write OPEN first (a WebTransport stream is invisible to the peer until written), MAY follow with DATA in the same write, use the channel at once; rejection arrives later as the channel's close. Open waits for stream credit in the stack, bounded by `openStreamTimeout` (10 s) then `RESOURCE_EXHAUSTED`. On the initiator, channel opens wait for the control stream to be open (it goes first), and, with `awaitPeerIdentity`, for the acceptor's identification (bounded by `MuxOptions.identityTimeout`, then `UNAUTHENTICATED`).
* **Receiving an OPEN**: read the first record within `openStreamTimeout` (else stop+reset `DEADLINE_EXCEEDED`); at most `maxPendingStreams` (128) streams being read for their OPEN, beyond that stop+reset `RESOURCE_EXHAUSTED`. Protocol errors (connection-wide): first record not OPEN, FIN before OPEN complete, body < 6 bytes, wrong parity, reserved id, channel 0 from the acceptor or a second time, the id of a channel that is open. A stream reset before its OPEN completed is ignored. Admission: after this side sent `GOAWAY`, refuse `GOING_AWAY` unless it is a `_bulk` continuation of an open (or drain-retained) parent; beyond `maxChannels` or the open-payload budget (`min(maxOpenPayloadBytes, receiveHighWaterMarkBytes / 2)`) refuse `RESOURCE_EXHAUSTED`.
* **Rejecting**: CLOSE record with the status + FIN, plus `STOP_SENDING` with the same code, bounded by `closeConfirmTimeout`, else stop+reset.
* **`openAfter`** (Talk's bulk ordering): a child channel's stream is opened only once everything the parent was given before the call has been handed to the stack. Streams are unordered, so the peer may still see the child first (`hasOrderedChannels` is false for this binding).
* **Bulk payloads** (`_bulk` channels): a `_bulk` stream can arrive before its parent's OPEN, after its parent ended, or out of number order. The rules (held backlog of 16 for 10 s, `FAILED_PRECONDITION` / `CANCELLED` / `RESOURCE_EXHAUSTED` outcomes, matching references to streams by number, drain holds for ended parents via `BindingConnection.retainForDrain`) live in core's Talk, not in the binding; the binding only admits continuations after `GOAWAY` and keeps drain holds in its idle check. Whoever ports Talk owns those rules; the QUIC binding must expose `retainedForDrain` / `hasDrainHolds` / `drainStateChanged` as the core contract defines them.
* **Ordering**: records ordered within a stream only. `PING` is a barrier for the control stream only. A channel opened after an `IDENT` may be dispatched before the `IDENT` takes effect (the identity hold covers it); the reference node opens identity-sensitive channels only after the `PONG` of a `PING` sent right after its `IDENT`. While an `IDENT` is being verified, later control messages are held (at most `maxHeldControlMessages`, 1024), channels are not.

## 5. Flow control and backpressure

This is where a JS port is most likely to go wrong. The rule: **unread data stays in the QUIC stack, so the stack's stream window is the channel's window.**

* Reads are pull-based. `RecordReader.next(limit)` pulls exactly one record: the 4-byte header first (checked against the limit before any body byte is read), then the body in reads of at most 256 KiB, allocating in proportion to what arrived (a header announcing 1 GiB plus one byte must not allocate 1 GiB). FIN between records = end; FIN inside a record = protocol error; a reset inside a record discards the partial record (not an error: the peer closed with the reset's code).
* A channel reads its next record only once the layer above consumed the previous one: when `unconsumed (queued cost + held) == 0`, or below `readAhead`. Costs are counted per record, `length + 4`. `manualCredit` + `consumed(bytes, subframes)` lets Talk return credit itself; `grant(bytes)` reads further ahead (how a bulk transfer gets a bigger effective window); an `OK` close by this side adds one `initialWindow` of read-ahead so the peer's CLOSE record is found behind data nobody takes.
* Talk over this binding (added after phase 1, design section 13 item 6): a message Talk holds for its payload would hold the references behind it in the stack while the bulk streams of a burst arrive beside them, and past `maxUnclaimedBulk` (16) Talk refused the rest, even without a lag over the native provider. The binding keeps reading lazily; Talk reads on for itself, on unordered connections only: while bulk channels wait unclaimed and fewer than 16 references wait for their channels, it reports the frames it holds consumed ahead of their delivery (`consumed`, not `grant`), up to one `initialWindow`. A fixed `grant` ahead was tried and is wrong here: the references usually overtake their bulk streams, so reading them all ahead leaves more than 16 waiting for their channels, a channel PROTOCOL_ERROR. The Dart reference keeps the gap.
* In WHATWG-stream terms this maps well onto a `ReadableStream` with `highWaterMark: 0` whose `pull()` reads one record (or onto an async iterator), but the cost accounting (`receiveWindow`, `heldBytes`, `consumed`, `grant`) is part of the core channel contract and must exist whatever the delivery API is.
* Writes: each channel queues records and hands them to the provider in batches of up to `writeBatchBytes` (64 KiB; a larger record goes alone), one provider write at a time. `send()` resolves once the stack has taken the record (exact backpressure; the stack waits for stream and connection credit). `isWritable` = open, stream attached, OPEN written, no write in flight, queue empty; `whenWritable` resolves when that becomes true. `sendWindow` is an estimate only (the stack's window is invisible). Bulk (`MuxPriority.bulk`) subframes are split into records of `min(bulkChunkSize 64 KiB, peer record limit)`; ordinary subframes over the peer's record limit are `FRAME_TOO_LARGE`.
* Windows the dial asks for: stream window = `initialWindow` (64 KiB), connection window = `receiveHighWaterMarkBytes` (16 MiB), max bidi = `maxChannels` (or 65536). Note `QuicClientConfig`'s own default stream window is 1 MiB; the binding overrides it.
* Priorities: provider classes 2 = control (strictly first), 1 = ordinary, 0 = bulk, where 1 and 0 must not starve each other. quinn has strict levels only, so the native provider maps 2 to level 1 and both 1 and 0 to level 0 (interleaved round robin). W3C: `sendOrder` = class - 1 for classes above 1 (control 1, ordinary and bulk 0), and where `createSendGroup` exists, classes 2 and 1 in one group and 0 in another. Chrome 149 has neither `sendOrder` nor send groups.

## 6. Close and error handling

### 6.1 Channels

* `close(status)`: state open to half-closed-local; the CLOSE record goes after everything already queued, then FIN. A non-`OK` close also sends `STOP_SENDING` with the code (this side will not read what is still coming). An `OK` close keeps delivering incoming data until the peer's close.
* The peer's close is its CLOSE record, its reset, or its `STOP_SENDING` (early, code only). On `STOP_SENDING` this side's direction ends at once (the stack resets it with the same code, RFC 9000 3.5; the provider does this itself) and it keeps reading until the peer's direction ends; if the peer's CLOSE record then arrives with the same code, its full status payload (reason, `MOVED`/`RELOCATED` fields) replaces the code-only status.
* A side that learns of the peer's close first closes in turn at once: `OK` CLOSE + FIN, or a reset if a write is in flight (it may be stalled at the peer's window).
* End status = the first non-`OK` status seen or sent. The channel ends when both directions have ended.
* Confirmation timeout (`closeConfirmTimeout`, 30 s): `STOP_SENDING DEADLINE_EXCEEDED` (forces the peer's reset), reset own direction if its CLOSE is stuck behind unread data (an `OK` close becoming `DEADLINE_EXCEEDED`), and report the channel closed. After the peer's CLOSE record, the reader waits for FIN within the same timeout, then stops the stream.
* Received codes: 0 to 255 are that status; larger, or none (`null`), read as `UNKNOWN`. Only 0 to 255 are ever sent; `CONNECTION_LOST` goes as `UNAVAILABLE`; application codes (256+) are never sent in CLOSE/GOAWAY.

### 6.2 Connections

| Event | Binding behaviour |
| --- | --- |
| `goAway(status)` | Send `GOAWAY`; refuse new channels (`GOING_AWAY`, except bulk continuations); wait until idle or `goAwayGrace`; then `close()` |
| `close()` | Wait (within `keepAliveTimeout`) until the directions this side finished are acknowledged (QUIC `stopped`/FIN-acked), then an application close: code 0 when graceful, else the code of this side's `GOAWAY` status. Channels still open end `CONNECTION_LOST` (or the GOAWAY status if either side sent one) |
| Protocol error | Application close at once with `PROTOCOL_ERROR` (32) and a reason; **no `GOAWAY` first** (it could be lost with its stream) |
| `IDENT` fails verification / binding required but missing | Close `UNAUTHENTICATED` (16), generic reason |
| Local transport failure | Close with `UNAVAILABLE` (14) |
| Keep-alive: no answer to the control `PING` within `keepAliveTimeout` after `keepAliveInterval` of silence | Close `DEADLINE_EXCEEDED`; channels `CONNECTION_LOST`. Silence while this side's own control reading is paused (output above the mark) is not counted |
| Peer's application close (raw `CONNECTION_CLOSE` 0x1d, `WT_CLOSE_SESSION`, CONNECT FIN = code 0) | A `GOAWAY` with that status (0 reads as `GOING_AWAY`), then the end of the transport |
| Anything else (QUIC transport error, idle timeout, stateless reset, CONNECT stream reset) | Lost: channels end `CONNECTION_LOST` |

Both stacks discard in-flight stream data on an application close, which is why `close()` waits for acknowledgements first. The QUIC idle timeout (30 s) must exceed the control keep-alive interval (10 s); the stack keep-alive (QUIC PING, 10 s) keeps a quiet connection alive but proves only that the peer's stack answers, so the control `PING` remains the application's probe.

### 6.3 Binding options and defaults (`QuicBindingOptions`)

| Option | Default | Meaning |
| --- | --- | --- |
| `recordLimit` | 32752 | announced in `HELLO`, 32752 to `0x3FFFFFFF` |
| `identity` | `QuicIdentityRules.none` (dial and listener replace it) | see 3.3 |
| `controlStreamTimeout` | `keepAliveInterval` or 10 s | peer's `HELLO` deadline; also bounds the exporter request at start |
| `openStreamTimeout` | 10 s | stream credit wait for an open; also the OPEN read deadline |
| `controlOutputMark` | 64 KiB | stop reading peer control while own control output exceeds it |
| `maxHeldControlMessages` | 1024 | held during `IDENT` verification, then `RESOURCE_EXHAUSTED` |
| `maxPendingStreams` | 128 | streams whose OPEN is still being read |
| `writeBatchBytes` | 64 KiB | records per provider write |

Fixed: control-stream session-gone grace 2 s; record reader chunk 256 KiB.

## 7. The provider contract (what any TS provider must implement)

Port the semantics exactly; they were amended five times in review and each amendment is load-bearing.

* `QuicConnector.connect(endpoint, config)`: fails with `QuicException` kinds `unsupportedScheme`, `unreachable` (incl. handshake past `handshakeTimeout`), `handshakeFailed`, `certificateRejected`, `refused` (non-2xx WebTransport response), `sessionEnded`, `streamLimit`, `internal`. Cannot be cancelled in Dart: a caller that gives up closes the session the promise still yields (`CANCELLED`). In TS an `AbortSignal` is the natural form, with the same rule underneath.
* `QuicListener`: `endpoint`, `sessions` (after handshake), `close()` (open sessions stay with their owners).
* `QuicSession`: `isInitiator`, `protocol` (ALPN or `WT-Protocol`; null when none), `remote`, `openStream()` (waits for credit, FIFO; a provider whose stack rejects without credit retries itself; a caller that gives up resets what it still yields), `incomingStreams` (taken from the stack only while listened to and not paused, so unaccepted streams keep holding the peer's stream credit), `incomingUnidirectionalStreams`, `draining` (never rejects; pending forever on raw QUIC), `maxIncomingStreams` (null where the stack decides), `exportKeyingMaterial(label, context, length)` (null when unsupported; the provider applies WebTransport's per-session derivation itself), `peerCertificateHash`, `close(code, reason)` (idempotent, reason cut to 1024 bytes at a character boundary, completes when the session has ended), `done` -> `{code, reason, byPeer, lost}`.
* `QuicStream`: `read(max)` (1..max bytes, `null` at FIN and forever after, rejects `QuicStreamReset(code)` on reset and forever after, one pending read at a time); `resetReceived` (resolves on the peer's reset **even while no read is pending**: a paused reader must still see an abort; FIN does not resolve it); `write(bytes)` (copies, queued in order, resolves when the stack took it, waits for credit, rejects `QuicStreamStopped(code)` after the peer's `STOP_SENDING`, state error after finish/reset); `finish()` (idempotent); `reset(code)` (also after finish; no-op once ended; out-of-range or uncarriable codes are argument errors, never silently changed); `stop(code)` (ends the read direction locally at once: pending and later reads reject `QuicStreamReset(code)`); `priority` (class 2/1/0, see 5); `stopped` (resolves with the peer's `STOP_SENDING` code, or null when every byte after FIN was acknowledged, or null at once after a local reset; a `STOP_SENDING` without a code rejects `QuicStreamStopped(null)` so it is never mistaken for an ack). The provider answers a `STOP_SENDING` by resetting with the same code itself.
* When a session ends, every pending operation on it and its streams rejects with `QuicException(sessionEnded)`; `done` tells how it ended.
* Codes: raw QUIC 0 to 2^62 - 1, WebTransport 0 to 2^32 - 1, browsers may carry fewer (Chrome 149: 255). In TS, 2^62 - 1 exceeds `Number.MAX_SAFE_INTEGER`: either type codes as `number` and range-check to 2^53 - 1 (the binding only sends 0 to 255 and reads everything above 255 as `UNKNOWN` anyway), or use `bigint` at the provider boundary. Recommend `number` with a documented bound.
* Configs: `QuicClientConfig {alpn, serverCertificateHashes, verifyCertificate, maxStreamsBidi, streamReceiveWindow, connectionReceiveWindow, idleTimeout, keepAlive, handshakeTimeout}`, `QuicServerConfig {alpn, certificateDer, privateKeyDer, ...same}`. Durations zero = none, negative = argument error. Providers that cannot configure something (browsers) ignore it.

The **in-memory provider** (`test/memory_provider.dart`, 1029 lines) is what makes the binding testable without a network: independent per-direction delivery with configurable delays (streams overtake each other and the control stream), stream and connection flow control, a stream limit, resets and `STOP_SENDING` with and without codes, FIN, and an exporter equal at both ends and different per session (or none, like a browser). Port it alongside the binding; most binding tests run on it.

## 8. The two Dart providers, as porting input

### 8.1 Native provider (`native_provider.dart` over `native/sbq`)

The shim's flat C ABI (version 4) is designed for exactly this kind of embedding:

* Synchronous calls that never wait on the network: `SBQ_WOULD_BLOCK` and a later event (`READABLE`, `WRITABLE`, `OPENED`). Events (`INCOMING`, `CONNECTED`, `CLOSED`, `STREAM`, `OPENED`, `READABLE`, `WRITABLE`, `STOPPED`, `FINISHED_ACKED`, `WT_REQUEST`, `DRAINING`, `UNI_STREAM`) queue per context; a **doorbell callback is called from a Rust notifier thread** when the queue goes non-empty; the embedder then calls `sbq_poll` until it returns fewer events than asked. The doorbell is never called with a quinn lock held. `sbq_shutdown` waits until no doorbell call is in progress, so it must not block the thread that runs doorbell callbacks.
* Handles are 64-bit table ids (not pointers). Buffers are copied in and out; unread data stays in quinn. Results: `SBQ_FIN` -1, errors -2..-9, `SBQ_NO_CODE` -10, `SBQ_RETRY` -11, and `code = -result - 16` for results at or below -16.
* `sbq_stream_poll_reset` is how a paused reader still observes a reset; the Dart provider calls it whenever no read is pending.
* The Dart provider is mostly event dispatch plus per-stream read/write queues and is a straightforward port; the 10.7k lines of Rust stay Rust.

### 8.2 Web provider (`web_provider.dart`, `web_interop.dart`)

A near line-for-line port target for the browser, with these browser facts it encodes (Chrome 149 as tested):

* `new WebTransport(url, {protocols, serverCertificateHashes, allowPooling: false, requireUnreliable: false})`, wait for `ready` within the handshake deadline (else `unreachable`). Refusal, unreachable and bad certificate all reject `ready` with "Opening handshake failed.": map all to `handshakeFailed`.
* Feature-test everything optional at run time: `protocol`, `draining`, `exportKeyingMaterial`, `getStats`, `createSendGroup`, `sendOrder` (Chrome 149 has `protocol` only).
* `WebTransportError` constructor: try the spec form `(message, {streamErrorCode})`, then Chrome's `({message, streamErrorCode})`; probe the largest code it carries (Chrome: an octet, 255); larger codes are argument errors.
* `createBidirectionalStream` rejects without stream credit instead of waiting (whatever `waitUntilAvailable` says): retry from 5 to 200 ms backoff, FIFO.
* `read(max)` takes at most one browser chunk and keeps the rest; stream resets and the peer's `STOP_SENDING` are read from the reader's and writer's `closed` rejections (`WebTransportError` with `source: 'stream'` and `streamErrorCode`), so a paused reader still sees them. Streams report the session's end (`source: 'session'`) before `closed` settles: treat as session gone, lost if `closed` does not settle within 2 s.
* `wt.close()` resolves `closed` at once; the browser sends the close itself. `stopped` resolves when the writer's `closed` resolves after FIN (the browser does not report acknowledgement).
* Every promise gets a rejection handler, and both test suites assert that the page saw no unhandled rejection. In JS this matters even more: Node's default for an unhandled rejection is to crash the process.

## 9. QUIC from TypeScript today: runtime survey

Status on 2026-10-14. Section 9.1 lists what was executed in this session. Sections 9.2 to 9.8 summarise the runtimes and libraries, from those runs and from primary sources (Node docs and changelogs on `main`, MDN browser-compat-data 8.1.5 of 2026-10-08, Apple release notes, npm and crates.io metadata). Anything not confirmed is marked *unverified*. The container is IPv4-only, which explains two of the failures below.

| Runtime | Raw QUIC, custom ALPN | WebTransport | TLS exporter | Pinning or no-verify | Base for Switchboard |
| --- | --- | --- | --- | --- | --- |
| Node 22, 24, 26 (official builds) | no (`node:quic` compiled out) | no | n/a | n/a | Node-API addon |
| Browsers | impossible | Chrome/Edge 97, Firefox 114, Safari 26.4 | Firefox 155 only | `serverCertificateHashes` | W3C `WebTransport` |
| Deno 2.9 | yes, `--unstable-net` | client and server, `--unstable-net` | no | no (CA roots only) | the addon; built-in QUIC only with CA-verified certificates |
| Bun 1.4 | no | no | n/a | n/a | the addon |
| Cloudflare Workers | no UDP | no | n/a | n/a | WebSocket only |

### 9.1 What was run here (2026-10-14, Linux x86-64, IPv4-only container)

Every line below was executed in this session, and the probe scripts are small enough to rerun.

| Probe | Result |
| --- | --- |
| Node 22.22.0 (installed): `import('node:quic')` | No such built-in module. |
| Node 24.21.0 and 26.11.1 (official linux-x64 binaries, checksums verified): `process.features.quic`, and `import('node:quic')` with and without `--experimental-quic` | `quic` is `false` or absent. The import fails with `ERR_UNKNOWN_BUILTIN_MODULE` either way: official binaries are not built with QUIC. |
| Node 26.11.1 `node:ffi` (experimental, on by default since 26.9.0, prints an ExperimentalWarning, needs `--allow-ffi` under the permission model) | Loads `libsbq.so` and calls into it; signatures are written as `{arguments: [...], return: '...'}`.<br><br>**A callback invoked from a foreign thread aborts the process** ("Callbacks can only be invoked on the system thread they were created on"). The shim rings its doorbell from its notifier thread, so `node:ffi` cannot drive the shim as it stands.<br><br>A workaround did deliver 5 of 5 rings, on Linux only: a C trampoline that calls libuv's `uv_async_send` on the loop from `ffi.getCurrentEventLoop()`. |
| Node 22.22.0 + koffi 3.3.3 + `libsbq.so` (the unchanged shim, built from `native/sbq` with `cargo build --release --locked`, Rust 1.97) | Works: koffi queues the doorbell from the notifier thread to the JS thread, and listen, connect, `INCOMING` and `CONNECTED` all fire. `sbq_shutdown` must be called through koffi's `.async`: the notifier blocks until the JS callback has run, and shutdown waits for the notifier. |
| Bun 1.4.2 + `bun:ffi` + `libsbq.so` | Works with `new JSCallback(fn, {threadsafe: true})`, which does not block: events arrive, and the raw QUIC exporter (`EXPORTER-Channel-Binding`) is equal on both ends. |
| Deno 2.9.6 + `Deno.dlopen` + `Deno.UnsafeCallback.threadSafe` + `libsbq.so` | Works, and the exporter is equal on both ends. `sbq_shutdown` must be declared `nonblocking: true`, or it deadlocks against the pending doorbell (the thread-safe callback blocks the notifier until the JS thread has run it). |
| A minimal napi-rs 3 addon: a `ThreadsafeFunction` called from a spawned thread, the doorbell pattern | Loads and delivers every call in **Node 22.22.0, Node 26.11.1, Bun 1.4.2 and Deno 2.9.6**. Deno loads it via `createRequire` with `--allow-ffi`. |
| **A napi-rs 3 addon over the real `sbq` core**. The `sbq` source is unchanged except that `"rlib"` is added to its `crate-type`. The addon calls the `sbq_*` functions Rust to Rust, and the doorbell is a non-blocking `ThreadsafeFunction`. Built in 40 s cold (`--release`, quinn 0.11.12). | Works in **Node 22.22.0, 24.21.0 and 26.11.1, Bun 1.4.2, and Deno 2.9.6** (`--allow-ffi --allow-read`):<br>• the certificate is generated, then raw QUIC listen and connect succeed, with `INCOMING` and `CONNECTED` delivered;<br>• the exporter is equal on both ends;<br>• `sbq_shutdown` called **synchronously** on the JS thread returns 0;<br>• the process exits on its own once the thread-safe function is dropped.<br><br>One `.node` file per platform serves every server runtime. |
| Deno 2.9.6 raw QUIC (`--unstable-net`): `Deno.QuicEndpoint`, `listen({cert, key, alpnProtocols})`, `Deno.connectQuic({..., caCerts, alpnProtocols, endpoint})` | Works: ALPN `switchboard` is negotiated, a bidirectional echo runs over WHATWG streams, send streams have `sendOrder` and a `bigint` `id`, and the peer sees `close({closeCode, reason})`.<br><br>Missing: **no TLS exporter, no certificate hash pinning, no "do not verify" mode.** The only trust option is `caCerts` (PEM). A self-signed end-entity certificate passed as its own CA works; one with `CA:TRUE` fails with `CaUsedAsEndEntity`.<br><br>The default client endpoint is `[::]`, which fails on an IPv4-only host ("Address family not supported"). Passing `endpoint: new Deno.QuicEndpoint({hostname: '0.0.0.0'})` fixes it. |
| Deno 2.9.6 `WebTransport` client (`--unstable-net`) | Exists, but `new WebTransport(...)` fails on this IPv4-only host for the same reason, and it takes no endpoint option. `Deno.upgradeWebTransport(conn)` exists for the server side. |
| `@fails-components/webtransport` 1.6.9 (quiche + BoringSSL) | Client and server work in Node 22.22, Node 26.11 and Bun 1.4.2: certificate hash, bidirectional stream, datagram. In Deno 2.9.6 it fails (`socketInt.getSendQueueCount is not a function`) because it relies on Node `dgram` internals. Its client also succeeded against the `@moq/web-transport` (quinn) server, but hung at `ready` against Deno's own WebTransport server. |
| `@moq/web-transport` 0.1.5 (napi-rs over web-transport-quinn) | Its server works in Bun. Its client panicked binding `[::]` on this IPv4-only host. Its entry point is raw `.ts`, which Node 22/26 and Deno refuse to load from `node_modules`. |
| Bun 1.4.2 | No `WebTransport` global and no QUIC or HTTP/3 client API (only `Bun.udpSocket`). |
| Node 22 / 24 / 26 | No `WebTransport` global. |
| Chromium installed for Playwright | Chromium 141.0.7390.37 under `/opt/pw-browsers`. It predates the `protocols` option. The Dart suites were run against Chrome 149. |
| Dart SDK | Not installed. 3.13.5 is downloadable from `storage.googleapis.com/dart-archive`. |

### 9.2 Node.js

* **`node:quic`.**
  * Status:
    * Added in v23.8.0 at Stability 1.0, "Early development", behind the runtime flag `--experimental-quic`.
    * Since PR #61444 (merged 2026-01-21) it is also behind a **compile-time** flag, `./configure --experimental-quic`, which is off by default. The compile flag is "temporary", added because of vulnerability reports against the unfinished implementation (jasnell in #62876, 2026-04-21).
    * Code tagged for 24.x is compiled out there too, and 22.x gets none of it (`dont-land-on-v22.x`).
    * The QUIC team's current agenda is a Fetch server API (#66269, 2026-10-01).
  * API on `main` (usable only in a custom build):
    * `quic.connect` and `quic.listen` return promises.
    * Clients pass one ALPN string and servers a list.
    * Reads are async iterators of `Uint8Array[]` batches (`node:stream/iter`), neither Node streams nor WHATWG streams.
    * Writes go through `stream.writer`. `write()` *rejects* when the budget (64 KB by default) is full, and the caller awaits `ondrain`.
    * Datagrams are supported.
    * HTTP/3 comes via nghttp3. **WebTransport is "not implemented".**
  * TLS:
    * `verifyPeer: 'manual'` plus `session.peerCertificate` (an `X509Certificate`, added in 26.2.0 and 24.20.0) allow pinning and self-signed peers.
    * The docs on `main` describe **no keying material exporter**. A `node:quic` provider could therefore pin certificates or verify them against roots, but could not carry exporter-bound `IDENT`s (`HELLO` without `EXPORTER`).
* **`node:ffi`.**
  * Added in 26.1.0 behind `--experimental-ffi`, on by default since 26.9.0 (#65475), still Stability 1. It is absent from 24.x and 22.x.
  * Foreign-thread callbacks abort the process (see 9.1). The `uv_async` workaround needs `ffi.getCurrentEventLoop()` (26.6.0+) and is unverified on Windows and macOS.
* **LTS lines.**
  * 24 is Active LTS ("Krypton").
  * 22 is in maintenance until April 2027.
  * 26 is Current; its promotion to LTS around late October 2026 is *unverified*.

### 9.3 Browsers (W3C WebTransport)

Per BCD 8.1.5:

| Feature | Chrome/Edge | Firefox | Safari |
| --- | --- | --- | --- |
| `WebTransport`, bidi/uni streams, datagrams | 97 | 114 | **26.4** (2026-03-24) |
| `serverCertificateHashes` | 100 | 125 | 26.4 |
| `protocols` option and `wt.protocol` | **143** (2025-12-02) | **155** (2026-09-01) | 26.4 |
| `exportKeyingMaterial` | no | **155** | no |
| `draining` | no | 155 | 26.4 |
| `sendOrder`, `createSendGroup` | no | 114 / 155 | 26.4 |
| `congestionControl` | no | 114 | 26.4 |
| `getStats` | no | 114 (a stub that throws; still so in 157: "not implemented") | 26.4 |
| `datagrams.createWritable()` / `datagrams.writable` (deprecated) | no / 97 | 155 / 114 | 26.4 / no |

* Safari's support: "Added support for WebTransport. (165721145)" (Safari 26.4 release notes). MDN has marked WebTransport Baseline since March 2026, and it is an Interop 2026 focus area.
* `serverCertificateHashes` constraints:
  * SHA-256 of the DER;
  * certificate validity of at most 14 days;
  * ECDSA P-256 must be supported.
  * The shim's `generateSelfSigned` meets all three (section 3.3).
* `protocols` goes out as `WT-Available-Protocols`; the server answers with `WT-Protocol`. The binding accepts a session where no protocol was selected, so Chrome 97 to 142 still works without it. The interop thread checked this with Chromium 141 against the Dart listener.
* **Firefox 155 is the first browser with `exportKeyingMaterial`.**
  * The Dart web provider feature-detects it, so a Firefox page would send `HELLO` with `EXPORTER`, and its `IDENT` proofs would be exporter-bound.
  * Whether Firefox's per-session derivation equals the shim's (section 3.3) is *unverified*. It should be the first browser interop test of Phase 2.
  * If they differ, the proof fails and the connection closes `UNAUTHENTICATED`.
  * **Answered (2026-10-18, Firefox 157 in CI):** they differ, and the node refuses the `IDENT`. Firefox writes the session ID into the context as a QUIC varint (neqo's `webtransport_export_keying_material`); the draft's struct, and the shim, have `WebTransport Session ID (64)`. Design section 13, item 7 has the options, and `docs/wiki/Runtimes-and-QUIC.md` what else Firefox does differently (the close, WT_DRAIN_SESSION, STOP_SENDING).
* Wire drafts:
  * The latest is `draft-ietf-webtrans-http3-16` (July 2026).
  * The shim advertises draft-02 and draft-07 signalling (section 3.2).
  * Which dialect Chrome 143+ and Firefox 155 negotiate in practice is *unverified*. The interop matrix covers it.
  * **Answered (2026-10-18):** Chromium 156 and Firefox 157 both signal draft-02 to the shim (the browser tests log it).

### 9.4 Deno (2.9.6)

* **Built-in API.**
  * `--unstable-net` exposes `Deno.QuicEndpoint`, `connectQuic`, `QuicConn`, `QuicListener`/`QuicIncoming`, `upgradeWebTransport` and the `WebTransport` global, all typed `@experimental`.
  * The engine is quinn 0.11.8 with web-transport-proto 0.2.7.
  * Streams are WHATWG streams.
  * Other options: `sendOrder`, `waitUntilAvailable`, datagrams, 0-RTT, congestion control, keep-alive and idle timeouts.
* **Missing for Switchboard nodes:**
  * no exporter;
  * no pinning;
  * no unverified mode;
  * no `protocols` on its WebTransport client;
  * no endpoint option on that client, which binds `[::]`.
* **Fit.** The built-in API can serve raw QUIC between Deno nodes whose certificates verify against configured CAs. It cannot reproduce the reference's self-signed, exporter-bound node identity.
* **Node-API addons** load from npm packages with `--allow-ffi` (verified with koffi and with the `sbq` addon above). Native code is outside Deno's permission system: once the addon is loaded, `--allow-net` does not constrain the sockets it opens.

### 9.5 Bun (1.4.2; npm latest 1.4.3)

* No QUIC or WebTransport API.
* HTTP/3 is experimental and server-side only: `Bun.serve({tls, http3: true})` on lsquic. There is also `fetch(..., {protocol: 'http3'})`. Neither exposes streams or datagrams.
* Node-API is supported, and Bun's docs call it "the most stable way to interact with native code". koffi, fails-components and the `sbq` addon all worked.
* `bun:ffi` `JSCallback({threadsafe: true})` works, but Bun's docs describe it as "experimental, with known bugs".

### 9.6 npm libraries

| Package (latest) | Engine | Assessment |
| --- | --- | --- |
| `@fails-components/webtransport` 1.6.9 (2026-10-09), BSD-3 | Google quiche (C++) + BoringSSL, node-addon-api, I/O on `node:dgram` | The most mature (since 2022, releases about monthly). WebTransport only, no raw ALPN. Supports `WT-Available-Protocols`. Prebuilds for 5 targets; a source build needs clang, go and perl and takes more than 15 minutes. Fails in Deno. You must `await quicheLoaded` first. A second QUIC stack with its own WebTransport dialect. |
| `@moq/web-transport` 0.1.5 (2026-09-27) | napi-rs over web-transport-quinn and quinn 0.11.12 | W3C-shaped API with hashes, `protocols` and disable-verify. Raw `.ts` entry point, so only Bun or a bundler loads it. WebTransport only. |
| `@chainsafe/libp2p-quic` 2.1.5 (2026-10-05) | napi 3 + quinn 0.11.12 | libp2p-specific TLS, so not usable as is. A good reference for a 7-target napi-rs + quinn build matrix, musl included. |
| `@currentspace/http3` 0.9.2 (2026-09-15) | Rust quiche 0.29 + boring via napi | Raw QUIC with custom ALPN, datagrams, mTLS and HTTP/3. Node 24 or later, 3 targets, and a wasm32-wasip1 build. Young (created 2026-03). Not tested here. |
| `rwebtransport` 0.2.2, `@webtransport-bun/webtransport` 0.3.0 | quiche / wtransport | Small. Not tested here. |
| `@matrixai/quic` 2.0.9 (2025-03), `ffi-napi` 4.0.3 (2021) | | Stale; do not use. |
| koffi 3.3.3 (2026-10-10), MIT | Node-API FFI with 20 prebuilt targets | Drives the unchanged `sbq` cdylib in Node, Bun and Deno (9.1). Its callbacks from foreign threads block until JS runs. |
| `ffi-rs` 1.3.7 | napi-rs FFI | Its documentation says callbacks become thread-safe functions. Not tested. |

* No maintained msquic or ngtcp2 binding exists on npm.
* A WASM build of quinn-proto (sans-IO) or of quiche, with UDP from `node:dgram`, `Deno.listenDatagram` or `Bun.udpSocket`, is possible. `@currentspace/http3` ships one. It would be a no-prebuild fallback, not a primary path: crypto runs in WASM, and timers and UDP are pumped from JS.

### 9.7 Reusing `sbq`: mechanisms compared

The question for each mechanism is what happens when the shim's notifier thread rings the doorbell.

| Mechanism | Runtimes | Doorbell from the notifier thread | Shutdown |
| --- | --- | --- | --- |
| **napi-rs addon over the `sbq` crate** (Rust to Rust; a non-blocking `ThreadsafeFunction`) | Node 22/24/26, Bun, Deno (`--allow-ffi`) | Queued; the notifier does not wait | Synchronous `sbq_shutdown` on the JS thread is fine |
| koffi over the cdylib | Node 22/26, Bun, Deno | Queued; **the notifier blocks** until JS has run it | Must use `.async`, or it deadlocks |
| `Deno.dlopen` + `UnsafeCallback.threadSafe` | Deno | Queued; **blocks** | Must be `nonblocking: true` |
| `bun:ffi` `JSCallback({threadsafe: true})` | Bun | Queued; does not block | Synchronous is fine |
| `node:ffi` | Node 26.9+ only | **Aborts the process** | n/a (needs the `uv_async` trampoline) |

`sbq_shutdown` "waits for a doorbell call in progress" (`lib.rs`). With any blocking variant, the JS thread must never call it synchronously.

The napi addon also gives:

* one native artifact per platform instead of two (koffi plus the cdylib);
* napi-generated `.d.ts`;
* napi-rs 3's `Weak` thread-safe functions, which let the event pump stop keeping the process alive while nothing is open. These are in the napi 3 source; *not exercised here*.

### 9.8 Edge runtimes

* Cloudflare Workers have no UDP, QUIC or WebTransport. HTTP/3 ends at the edge, and outbound sockets are TCP only (`cloudflare:sockets`). `node:dgram` is a stub (workerd #4463).
* Deno Deploy and similar platforms are *unverified*.
* Treat edge runtimes as WebSocket-only.

### 9.9 Sources

* Node: `doc/api/quic.md` and `doc/api/ffi.md` on `main`; PRs #61444 and #62876; issue #66269; `CHANGELOG_V26.md` (26.1.0 FFI, 26.6.0 `getCurrentEventLoop` #64323, 26.9.0 FFI on by default #65475); the v26.3.0 release notes.
* Browsers:
  * MDN browser-compat-data 8.1.5 (`api.WebTransport*`);
  * MDN WebTransport;
  * the Safari 26.4 release notes;
  * `web-platform-tests/interop` 2026;
  * the W3C WebTransport spec (`w3c/webtransport` `index.bs`);
  * `draft-ietf-webtrans-http3`.
* Deno: `deno types --unstable-net` (2.9.6). Bun: `bun-types` 1.4.3 (`serve.d.ts`, `docs/runtime/ffi.mdx`). koffi 3.3.3 `doc/callbacks.md`.
* npm registry metadata and the READMEs of every package above; the crates.io index for quinn, napi and web-transport-proto; the NAPI-RS v3 announcement.
* Cloudflare docs: `workers/reference/protocols`; workerd #4463.

## 10. What the Dart tests cover (and what to port)

| Suite | Runs on | Covers | Port to TS as |
| --- | --- | --- | --- |
| `vectors_test.dart` | in-memory provider; also compiled to JS | every byte vector of the wiki page, negative vectors as connection errors, the example | unit tests, first |
| `binding_test.dart` (1436 lines) | in-memory provider; also JS | channel ids, subframes and record limits, every close path (CLOSE+FIN, rejection, resets, `STOP_SENDING`, confirmation timeouts, stuck CLOSE), flow control and `manualCredit`, priorities, `openAfter`, GOAWAY and connection end, keep-alive and control-stream rules (incl. WebTransport no-code resets), resource limits, identity (exporter, nonce-bound, required binding, awaiting the acceptor, identify-only-to-identified, bearer rules, nonce errors) | unit tests on the in-memory provider |
| `switchboard_test.dart`, `review_switchboard_test.dart` | in-memory provider, VM | two nodes end to end (Talk, 3 MiB bulk with overtaking streams, many channels over jittered streams, `whenWritable` pacing), dial timeouts and late sessions, stack keep-alive settings, identity incl. a man in the middle relaying or stripping `EXPORTER`, the proxy between frame mux and binding both ways, GOAWAY/close/loss | node-level tests once the core port has `Switchboard` |
| `native_provider_test.dart` (1517 lines) | sbq over UDP loopback, each case on raw QUIC and WebTransport | the provider contract (certs, schemes, handshake deadline, negative durations, pins, platform verification, IPv6, exporter equality, read/write/FIN/reset/stop and their codes, resets observed while paused, backpressure with a 4 MiB write, pending ops failing on session end), priorities, stream limits, session end kinds, idle timeout, keep-alive, 256 concurrent streams, 64 MiB on one stream, no deadlock under 200 events during calls, shutdown; WebTransport request fields, refusals (404/403/400/503), authority not checked, origin matching, draining, code ranges | contract tests run against every TS provider that can listen (native), parameterised by mode |
| `native_integration_test.dart` | binding over sbq | exporter-bound mutual identification, unidentified dialler refused, pins, Talk, 8 MiB bulk, pacing, paused reader sees reset, 64 parallel channels x3, throughput, GOAWAY both ways, idle timeout, stack keep-alive, proxy, exporter label, refused path, `WT_DRAIN_SESSION` | integration tests, TS to TS |
| `web_provider_test.dart`, `web_binding_test.dart` | headless Chrome against `tool/web_server.dart` (Dart native listeners + a node with Talk services) | the W3C provider against the contract, the binding and `ReconnectingClient` in a browser, no unhandled rejections | browser tests (Playwright Chromium is installed here: Chromium 141 under `/opt/pw-browsers`) |
| `tool/native_smoke.dart`, `aot_smoke.sh`, `browser_smoke.sh` | | smoke runs | not needed beyond CI smoke |

**Cross-implementation tests (TS against Dart)** come almost for free on the QUIC side:

* `tool/web_server.dart` already is a Dart WebTransport server for browser tests (listeners with behaviours selected by query, a records report endpoint, a stream-limited listener, a Switchboard node with `echo`/`control`/`issue` Talk services, a black-hole UDP port), printing `READY {json}` with URLs and the certificate hash. The TS web-provider tests can run against it unchanged, which validates the TS browser provider against the Dart server.
* For raw QUIC and server-to-server WebTransport: a small Dart program (like `tool/native_smoke.dart`) as the peer, TS dialling Dart and Dart dialling TS, both identity directions with exporter-bound proofs (which checks that both stacks derive the same exporter for a session).
* Needs in CI: the Dart SDK 3.13.5 (downloadable here from `storage.googleapis.com/dart-archive`, checked), a Rust toolchain for the shim (Dart's build hook runs `cargo build`; Rust 1.97 is installed here), and core checked out at `3816e6f` next to `switchboard_quic`.

## 11. JS/TS porting pitfalls specific to this binding

* **Unhandled rejections**: the Dart code calls `.ignore()` on every promise nobody awaits (`_controlReady`, `_mutualIdentification`, write completers, `stopped`, ...). Each needs a `.catch(() => {})` (or equivalent) in TS, or Node crashes.
* **Scheduling**: Dart `scheduleMicrotask` maps to `queueMicrotask`; `Timer.run` is a macrotask (`setTimeout(fn, 0)`), used deliberately for idle checks "after channel listeners have handled their end". Do not collapse the two.
* **Synchronous controllers**: `StreamController(sync: true)` with `onListen`/`onPause`/`onResume`/`onCancel` drives incoming channel delivery and "take streams from the stack only while listened". Whatever stream abstraction the core picks, the pull/pause semantics must survive.
* **`Future.timeout` / `Future.any`**: becomes `Promise.race` with timers that are cleared, and in Node, timers that do not keep the process alive where the Dart code set `keepIsolateAlive = false` (the native event pump should `unref` while no session or listener is live).
* **Integers**: u48 ids and u32 headers are exact in JS numbers; the WebTransport session ID in the exporter context is a `u64` written as two `u32`s (as the Dart code does); QUIC codes above 2^53 need bigint or a bound (see 7).
* **UTF-8 truncation** of reasons at a character boundary (1024 bytes): `TextEncoder` then back off continuation bytes, as `_truncateUtf8` does.
* **`Uint8List.sublistView`**: zero-copy views; JS `subarray`. Writes must copy (`write` takes ownership of nothing).

## 12. Recommendation

### 12.1 Per runtime

| Runtime | Provider | What it gives | Trade-offs |
| --- | --- | --- | --- |
| **Browsers** | W3C `WebTransport`: a port of `web_provider.dart` | WebTransport client; `serverCertificateHashes`; the exporter on Firefox 155+ | No raw QUIC and no listening.<br><br>`protocols` from Chrome 143, Firefox 155 and Safari 26.4. Older Chrome connects without a selected protocol, which the binding accepts.<br><br>Priorities only where `sendOrder` exists (Firefox, Safari). Chrome carries stream codes up to 255 only, which is enough because the binding sends 0 to 255.<br><br>Elsewhere `IDENT` is nonce-bound, which is safe because the browser verified the certificate. |
| **Node 22, 24, 26** | One Node-API addon (napi-rs 3) over the `sbq` Rust core: the **native provider**, a port of `native_provider.dart` | Raw QUIC (`quic://`) and WebTransport (`https://`), as client and server. Also the TLS exporter, pins and unverified mode, priorities, stream limits, the handshake deadline, WebTransport request fields and refusals, and certificate generation.<br><br>It is the **same QUIC stack and the same 10.7k lines of reviewed shim** as the Dart reference, so the wire behaviour matches the reference by construction. | Rust in the repository, a per-platform build matrix, and prebuilt packages.<br><br>A platform without a prebuild has no QUIC. The connector then reports `unsupportedScheme`, and the node moves on to the record's next endpoint (`wss://`, `tcp://`). Nothing breaks. |
| **Bun 1.4** | The same addon | Same | Relies on Bun's Node-API, which was verified here with this exact addon. |
| **Deno 2.9** | The same addon, from npm, with `--allow-ffi` | Same | `--allow-ffi` is required. Deno's `--allow-net` does not constrain native sockets, so document that. |
| Deno, optional later | Built-in `Deno.connectQuic` / `QuicEndpoint` | No native code | `--unstable-net` only.<br><br>No exporter, pins or unverified mode, so it suits only deployments whose node certificates verify against CAs.<br><br>Its WebTransport client binds `[::]` and lacks `protocols`. |
| Node, later | `node:quic`, enabled when `process.features.quic` is true | No native code | Not in any official binary today. It has no exporter in the docs on `main`, but pinning works via `verifyPeer: 'manual'`. Its stream model differs (batch iterators, writes that reject when full). |
| Cloudflare Workers and other edges | None | | WebSocket only. |

**Why not the alternatives:**

* **koffi, `bun:ffi` or `Deno.dlopen` over the unchanged cdylib.**
  * It works today, but needs two native artifacts per platform (koffi and the cdylib), three loaders, and the blocking-callback deadlock rules of 9.7.
  * It stays useful as a **fallback**. If the addon mirrors the C ABI (12.2, point 2), the TS native provider is identical over either, so a koffi loader can be added later at no design cost.
* **`@fails-components/webtransport`.** It adds a second QUIC stack (quiche) with its own WebTransport dialect, which would need its own interop parity with the reference. It also has no raw QUIC and fails in Deno.
* **`@moq/web-transport`.** Its entry point is raw `.ts`, and it is WebTransport only.
* **A fresh quinn binding.** It would re-derive what `sbq` already settled in review: handshake deadlines, draft-02 and draft-07 dialects, refusal statuses, close ordering, priorities, and the reset-while-paused poll.
* **WASM.** A possible no-prebuild fallback later. Crypto would run in WASM, and UDP and timers would be pumped from JS.

### 12.2 Decisions for the lead

1. **The provider contract keeps the Dart shape, typed for TS.**
   * Pull-based `read(max): Promise<Uint8Array | null>`, `write`, `finish`, `reset`, `stop`, `resetReceived`, `stopped`, `priority`, and session `done`, with section 7's semantics unchanged.
   * The binding's correctness rests on pulling exactly one record at a time and on per-record cost accounting (section 5). The in-memory provider and the binding tests port line for line against this shape.
   * WHATWG-stream stacks get adapters: the browser provider already is one, and so would be Deno's built-in QUIC.
   * Whether channels expose WHATWG streams *above* the binding is the core's API question, not the provider's.
   * Note for the lead: the survey agent proposed a WHATWG-streams contract instead. I recommend against it, for the reasons above.
   * TS specifics:
     * codes as `number`, documented up to 2^53 - 1;
     * `connect(endpoint, config, {signal})` with an `AbortSignal` (a late session is closed `CANCELLED`, as in Dart);
     * incoming streams as a pull, `acceptStream(): Promise<QuicStream | null>`, so streams are taken from the stack only while someone asks, which is the Dart pause rule in its natural TS form;
     * every internal promise handled (section 11).
2. **Where the Rust lives.** Kaetemi approved reusing the `sbq` Rust core for the TS port (2026-10-14).
   * A small napi-rs crate in `switchboard_ts` (the lead names the path) depends on `sbq` as a Cargo git dependency at a pinned `switchboard_quic` commit (Cargo finds the `sbq` package inside the repository).
   * The crate mirrors the C ABI one function for one function. Handles are JS numbers (table ids), and `sbq_poll` fills an event array. The doorbell is a non-blocking thread-safe function, and a `Weak` one keeps the pump from holding the process open while nothing is live.
   * This keeps `sbq` the single source of truth for both languages, and the TS native provider a direct port of the Dart one.
   * **It needs one upstream line**: `crate-type = ["cdylib", "staticlib", "rlib"]` in `switchboard_quic/native/sbq/Cargo.toml`. That is Kaetemi's repository; it does not change the Dart build, which still gets its cdylib. That is the only change the probe in 9.1 made.
   * Until it lands, vendor `native/sbq` at the pinned commit with that single change, plus a CI check that the vendored tree equals upstream except that line.
3. **Distribution.**
   * Use `@napi-rs/cli` per-platform packages as `optionalDependencies` for the platforms `switchboard_quic`'s `release.yml` builds: linux x64 and arm64 (gnu), macOS arm64 and x64, windows x64 (with the same `+crt-static`). musl is optional; `@chainsafe/libp2p-quic` shows a 7-target quinn matrix.
   * Load the addon lazily, on the first native connector or listener. A missing binary is `unsupportedScheme`, not a crash.
   * Browser bundles must never reach the addon: use conditional exports (`browser`, `deno`, `bun`, `node`) or separate entry points (the lead's call).
   * CI pins Rust 1.99.0, as `switchboard_quic` does.
4. **Runtime matrix for tests.**
   * Node 22, 24 and 26.
   * Bun 1.4 and Deno 2.9 for the native provider's contract tests.
   * Playwright **Chrome for Testing 143 or later** for the browser provider: the Chromium 141 installed here lacks `protocols`.
   * Firefox 155 or later for the exporter path, if Playwright's Firefox build is that recent (*unverified*). Since 2026-10-18 CI runs Playwright 1.64.1: Chromium 156 and Firefox 157.
   * Never assume IPv6 in tests or defaults: bind and dial `127.0.0.1` explicitly.
5. **Phase 2 seams within the QUIC work.** Each part is its own milestone and draft PR, in this order:
   1. **Binding, pure TS.**
      * Records, `RecordReader`, control and `HELLO`, exporter context helpers, `QuicIdentityRules`, channel, connection, options.
      * Plus the in-memory provider, the vector tests, and the port of `binding_test.dart`.
      * It needs core's binding seam (section 15 of `core.md`: `MuxConnection` / `MuxChannel` / `BindingConnection`, `MuxOptions`, status codes, control message codecs, the `IDENT` proof). If the lead fixes that interface early, the binding can be written against it before the core implementation lands.
   2. **Browser provider.** A port of `web_provider.dart`, with Playwright tests against `switchboard_quic/tool/web_server.dart` (the Dart server, unchanged) and then the TS server.
   3. **Native addon and native provider.** The crate, the TS provider, the contract tests (a port of `native_provider_test.dart`, each case over raw QUIC and WebTransport), the integration tests (a port of `native_integration_test.dart`), and the QUIC and WebTransport cells of the interop thread's matrix.
   * The node glue (`QuicSwitchboardBinding`, listener adoption, `connectQuicBinding`) follows the core's `Switchboard` port.
6. **First interop questions for Phase 2:**
   * Firefox 155's `exportKeyingMaterial` against the shim's per-session derivation (answered in 9.3: they differ);
   * which WebTransport dialect Chrome 143+ and Firefox 155 negotiate with the shim (answered in 9.3: draft-02);
   * Safari 26.4 (no CI runner here).

**For Kaetemi, outside the lead's remit:** the `rlib` line in `sbq` (point 2), and whether `switchboard_quic` should publish prebuilt shims. It has no releases yet, so every consumer builds with cargo.
