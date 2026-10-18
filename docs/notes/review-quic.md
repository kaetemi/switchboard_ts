# Review: the QUIC and WebTransport layer

_An independent review of `src/core/quic/**`, `src/node/quic/**`, `src/web/quic/**`, `src/testing/memory-quic-provider.ts` and `native/src/lib.rs` against the Dart sources of `switchboard_quic`, `docs/design.md` (3.1, 6.10, 12, 13.7, 13.8) and the QUIC parts of `dart-design.md`. The two end-of-session races of design 13.8 (GOAWAY acknowledgement, the codeless-reset grace) are another thread's and not covered. Each finding has a `test.fails` case on this branch that passes once it is fixed (each was checked against a candidate fix, then reverted): `npm run check` stays green, and the fix turns `test.fails` into `test`._

The binding itself (`QuicBindingConnection`, `QuicBindingChannel`, records, the record reader, HELLO, `QuicIdentityRules`) is a function-by-function port of the Dart and matched it everywhere I compared; what follows is in the TypeScript-only surface (Web Streams, Node Buffers, signals) and the providers.

## Findings, most severe first

### 1. The native provider does not copy a Node Buffer it is still writing
- **Severity:** wire or data loss.
- **Where:** `src/node/quic/native-provider.ts:1682`, `Stream.write`: what the stack did not take yet is "copied" with `w.bytes.slice(w.offset)`; `Buffer.prototype.slice` is a view on the same memory.
- **Path:** a Node program on `nativeQuic()` streams (the interop probe, a raw-QUIC tool, anything below the binding) writes a Buffer larger than the peer's stream window, which Node's own APIs hand out (`fs.read` into a reused Buffer, a pooled `Buffer.allocUnsafe`), and reuses it once `write` returned, as the provider documents it may. In the test 979 200 of 1 048 576 bytes arrived as the bytes written afterwards. The binding is not affected: it always writes arrays it built itself.
- **Test:** `test/integration/review-quic-native.test.ts`, "a Node Buffer written while the stream window is full may be reused at once, as a Uint8Array may" (runs with `SWITCHBOARD_QUIC_ADDON`). The existing case of `quic-native.test.ts` writes a Uint8Array, whose `slice` copies.
- **Fix:** `w.bytes = Uint8Array.prototype.slice.call(w.bytes, w.offset)`.
- **Dart:** no; `Uint8List.sublist` copies.

### 2. A channel's `writable` sends a string as zero bytes, and an ArrayBuffer wedges the channel
- **Severity:** wire or data loss (string); hang (ArrayBuffer).
- **Where:** `src/core/quic/quic-binding-channel.ts:283` (the writable's `write` is `this.send(chunk)` unchecked); `send` pushes the item (`:434`) before `#pump` writes its record header, so a chunk without a numeric `length` throws the header's RangeError on every later `#pump`.
- **Path:** JavaScript code pipes text into a channel (`stringStream.pipeTo(channel.writable)`) or writes an `ArrayBuffer`. A string goes out as a DATA record of NUL bytes, one per UTF-16 unit, and the write resolves. An ArrayBuffer leaves the channel unable to send or close: every later `send()` and `close()` throws `RangeError: a record body is 0 to 1073741823 bytes, not undefined` (`close()` must never throw). The frame mux refuses both with `TypeError: channel N takes Uint8Array subframes` (`frame-mux-channel.ts:477`), so the same code works over TCP and fails over QUIC.
- **Test:** `test/unit/quic/review-writable-chunks.test.ts`, both cases.
- **Fix:** the frame mux's `instanceof Uint8Array` check in the writable's `write`; also check (or catch) in `send()` before the item joins `#out`.
- **Dart:** not applicable (typed `Uint8List`).

### 3. The binding keeps Buffers it says it copies: OPEN payloads reach the wire as the caller later changed them
- **Severity:** wire or data loss.
- **Where:** `src/core/quic/quic-binding-connection.ts:1355` (`open()` keeps `openPayload.slice()`, a view when the payload is a Buffer); the OPEN record is encoded in `#pump` once the stream opened, asynchronously. The same `.slice()` keeps a PING payload (`:1428`, used to match its PONG) and an `identify` intent (`:1830`, signed and sent after the NONCE wait). The in-memory provider's write keeps the caller's Buffer as well (`src/testing/memory-quic-provider.ts:682`).
- **Path:** a Node caller opens a channel with a Buffer and reuses it at once (design section 5: a Buffer is accepted wherever a Uint8Array is, OPEN payloads are copied). The peer sees the reused bytes and `channel.openPayload` changes with them; a PING whose Buffer is reused never resolves until the connection ends.
- **Test:** `test/unit/quic/review-buffer-copies.test.ts`, all three cases (OPEN payload, PING, in-memory provider write).
- **Fix:** copy with `Uint8Array.prototype.slice.call(x)` (or `new Uint8Array(x)`) at those four places.
- **Dart:** no (`Uint8List.fromList` copies). Outside this layer: the frame mux has the same `.slice()` (`frame-mux-connection.ts:591` intent, `:773` OPEN payload); it encodes the OPEN at once, so only `openPayload` and the intent alias there. For the mux reviewer.

### 4. Binding options out of range are refused only after the dial, and the dialled session is left open
- **Severity:** hang or leak.
- **Where:** `src/core/quic/quic-dial.ts:204`: `QuicBindingConnection.start` validates `quicOptions` (record limit, positive bounds) after `connector.connect` returned; its synchronous RangeError rejects the dial and nobody closes the session. `src/core/quic/quic-listen.ts:86`: `listenQuic` takes the same options unchecked and then refuses every session it accepts (INTERNAL, a warning per session).
- **Path:** `connectQuic(endpoint, nativeQuic(), { quicOptions: { recordLimit: 1000 } })` rejects with the RangeError while a live session, which holds the native provider's event loop, stays open until the acceptor's HELLO timeout (10 s by default; never against a peer with `controlStreamTimeout: 0`). A misconfigured listener accepts the handshake and closes every connection.
- **Test:** `test/unit/quic/review-dial.test.ts`, "binding options out of range are refused without leaving a session open" and "listenQuic refuses binding options out of range at once, as connectQuic should".
- **Fix:** validate the binding options (a shared helper of `start`) in `connectQuic` and `QuicBindingListener`'s constructor before anything is dialled or accepted; at least close the session when `start` throws.
- **Dart:** the same (validated in `start`, after the connect); the wiki does not rule on it.

### 5. `connectQuic`'s signal is not watched while the binding starts
- **Severity:** wrong behaviour (a promise that settles late).
- **Where:** `src/core/quic/quic-dial.ts:204-213`: `withAbort` covers `connector.connect` only; `QuicBindingConnection.start` (the exporter, bounded by `controlStreamTimeout`, 10 s by default; on the initiator the control stream's open) runs unwatched, and `signal.aborted` is read once it finished.
- **Path:** a `ReconnectingClient` (through `quicConnector`) or a node's dial is aborted while a session's exporter is slow (the W3C `exportKeyingMaterial` is asynchronous; Firefox has one): the dial rejects only when the exporter wait ends, up to 10 s later, and the session stays open meanwhile. Design section 5: an AbortSignal on every operation that waits, connect first.
- **Test:** `test/unit/quic/review-dial.test.ts`, "an abort while the binding starts rejects the dial at once and closes the session".
- **Fix:** race `start` with the signal as the connect is (`withAbort`), closing the session (and the connection when it arrives) on the abort.
- **Dart:** not applicable (no signals).

## Unverified suspicions
- **Provider throws escape from the channel.** `QuicBindingChannel` calls `stream.stop`/`stream.reset` unguarded (`#stopReceiving` `:917`, `#resetDirection` `:900`, `#attach`); only the connection's `#release` catches. The contract lets `reset` throw RangeError for a code the stack cannot carry, and the WebTransport provider does for a browser without a constructible `WebTransportError` (`maxCode` 0): `close(failure)` would then throw, and `#onConfirmTimeout` would throw out of a timer. Not reachable in Chromium or Firefox (`maxCode` 255 or 2^32 - 1, the binding sends 0 to 38), so not tested.

## Fine, but surprising
- **Exporter labels:** the values on the wire match Dart and the wiki (`EXPORTER-Channel-Binding` with an empty context on raw QUIC; `switchboard-ident` as the application label over WebTransport, to which the shim adds `EXPORTER-WebTransport` and the session-ID context; the vectors and the interop oracle check both). Only the names are swapped: TS `webTransportExporterLabel` (`contract.ts:247`) is Dart's `webTransportIdentLabel`, TS `webTransportTlsExporterLabel` (`exporter.ts:30`) is Dart's `webTransportExporterLabel`. Code ported from Dart under the same name gets the other value. Rename them to Dart's names, or list the rename in design section 12.
- **A verifier-only initiator cannot talk to a default listener.** `QuicIdentityRules.initiator` sets `awaitPeerIdentity` when the dialler only verifies (`verifiesCredentials`, no credential), `#mutualIdentification` resolves only at the end of this side's own `identify()`, and the listener identifies only to a peer that identified first: every open waits `identityTimeout` and ends UNAUTHENTICATED. It follows the rules as Dart and the Identity page state them; worth a sentence there.
- `maxChannels` counts this side's own channels too when admitting the peer's (both bindings, Dart alike), while HELLO's `channels` reads as "channels opened by the peer". `peerMaxChannels` is advisory over QUIC (opens wait for stream credit) where the frame mux's `open()` refuses at the peer's LIMITS; both documented.
- Breaking out of `incoming` refuses later `_bulk` channels for live Talk channels too (`UNAVAILABLE` is checked before the adopters), in the frame mux the same way.
- The addon mirrors the C ABI faithfully: the lock order cannot deadlock (`ring` holds the shim's bell lock then `BELLS`; `init` holds `BELLS` across `sbq_init`, which takes no bell lock; `shutdown_ctx` calls `sbq_shutdown` without `BELLS`); the doorbell is a weak thread-safe function referenced only while a session or listener lives, and dropped by `shutdown`; handles come from the shim's process-wide counter and are never reused; `poll` fills exactly the events returned; results beyond 2^53 come back as BigInt. Event fields `a` and `b` are f64, so a STOP_SENDING or close code above 2^53 rounds (documented in the provider's header). The event pump stops a batch only when the context shut down, which happens once no session or listener is live (or on `NativeQuicProvider.shutdown()`).
- `quinnPriority` puts ordinary and bulk streams on one quinn level (round robin), as Dart does; bulk is not after ordinary on the wire.
- The doc example of `QuicSwitchboardListener` (`quic-switchboard.ts:256`) awaits `selfSignedCertificate`, which is synchronous (design 6.10); harmless.
- The WebTransport provider takes the server's streams from the browser only while someone waits, but the binding always waits, so the browser's stream backlog never holds anything back; and `incomingBidirectionalStreams` ending without the session leaves a waiting `next()` pending until the session ends, as in Dart.

Tests: 3174 passed, 2 skipped, 1 todo before; 9 `test.fails` added (`review-writable-chunks` 2, `review-buffer-copies` 3, `review-dial` 3, `review-quic-native` 1), each run 10 times, stable.
