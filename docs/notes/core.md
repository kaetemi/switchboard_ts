# Switchboard protocol core: porting notes for TypeScript

_Written by the "Port protocol core" thread. Notes, not design: the lead thread owns the package layout and public API._

These notes cover the protocol core of the Dart reference implementation: transports, the mux, status codes, the handshake and identity, the binding seam used by QUIC, plus the Talk and addressing wire formats that sit directly on top. They describe how things work and what a port has to watch for. They do not propose a package layout or public API; that is the lead thread's call. Section 20 lists the places where the Dart API shape does not suit JS/TS, as input to that design.

**Sources.** Two sources, read at their current heads:

* The normative spec is the wiki, which lives in `kaetemi/sway` under `wiki/switchboard/`: `home.md`, `transport.md`, `mux.md`, `status-codes.md`, `talk.md`, `addressing.md`, `identity.md`, `embedded-profile.md` and `test-vectors.md`.
* The reference implementation is `kaetemi/switchboard` at `3816e6f` ("Talk item windows and credits"), mainly `lib/src/{bytes,status,name}.dart`, `lib/src/mux/*`, `lib/src/transport/*`, `lib/src/talk/talk_frame.dart`, `lib/src/talk/talk_bulk.dart` and `lib/src/address/channel_address.dart`. The Dart design notes are in `doc/dart-design.md`.

When the wiki and the Dart code disagree, the wiki wins. The vector page says so explicitly: "When a vector and an implementation disagree, the vector wins unless this page is amended." No disagreement between the two was found in the core, and a script cross-check of every wiki vector the Dart tests assert found none either.

The QUIC/WebTransport binding (`quic-binding.md`) belongs to the QUIC thread. Section 15 only describes the seam it plugs into.

---

## 0. The points that shape a TS design most

1. **All integers are little endian.** The widths are u8, u16, u24, u32 and u48. Channel ids are u48 and real ids go above 2^32 (vector `00 02 00 00 00 FF FF` is id 0xFFFF00000002). Never apply JS bitwise operators to an id, a window or a CREDIT byte count. Split a u48 as `lo + hi * 2**32`. A `number` is exact for every u48. `bigint` isn't needed and would make ids awkward to use as Map keys (§2).
2. **One mux frame is one transport frame.** The transport delivers whole frames. The mux never sees partial frames, and any frame-size limit is enforced before the frame is allocated (§4).
3. **Two error scopes.**
   * A *connection* protocol error ends everything: GOAWAY `PROTOCOL_ERROR`, close, and stop processing input.
   * A *channel* protocol error (bad Talk header, bad address header) only closes that channel with CLOSE `PROTOCOL_ERROR`.

   Mixing them up is the easiest interop bug to write (§13).
4. **Flow control is per channel, credit based, and charged at payload length + 16.**
   * Credit returns when the *consumer takes delivery*, not when bytes arrive.
   * The consumption point drives everything. Dart's single-subscription `Stream`, buffered until listened, maps poorly onto JS `EventEmitter` push delivery. A pull-based async iterator or `ReadableStream` with an explicit "delivered" hook maps well (§10, §20).
5. **Timing matters to the semantics.** The keep-alive timers, the close-confirm timeout, the awaitPeerLimits wait, the identity timeout and Talk's whole timeout regime all depend on time. The Dart tests drive these with `fake_async`. A port needs an injectable clock and timer source from day one, or roughly a third of the Dart tests can't be ported (§20.4).
6. **Decoders keep status payloads byte-exact.** A status received in CLOSE/GOAWAY/ABORT keeps the raw bytes after its code, so a proxy relays them unchanged, malformed UTF-8 included. A port that decodes the reason to a string and re-encodes it breaks relaying and the MOVED/RELOCATED fields (§3).
7. **The binding seam is an abstract connection/channel interface, not a frame interface.** QUIC replaces the transport and mux layers together. Talk, addressing and everything above them must depend only on the abstract `MuxConnection` / `MuxChannel` contract and `hasOrderedChannels` (§15).
8. **The crypto is async in JS.** WebCrypto's Ed25519, HMAC-SHA256 and SHA-256 all return promises. The Dart IDENT verification path already pauses the transport and holds later frames while verifying, so the async shape is already in the protocol. The credential *codec* must stay synchronous (§8, §20).

---

## 1. Dart source map

| Dart file | Lines | Content | TS concern |
| --- | --- | --- | --- |
| `bytes.dart` | 229 | `ByteReader` / `ByteWriter` (u8…u48, name, len8 string, take, rest), `hexBytes` / `hexString` | trivial; `DataView` plus a u48 helper |
| `status.dart` | 198 | `StatusCode` enum, `Status` (raw body kept), `SwitchboardException`, `ProtocolException` | error classes; code table |
| `name.dart` | 124 | 8-byte `Name` | needs a primitive map key |
| `monotonic.dart` | 32 | monotonic clock (Stopwatch, or a `package:clock` override) | injectable clock |
| `mux/mux_frame.dart` | 537 | frame header, control message, LIMITS, CREDIT codecs | pure codec |
| `mux/mux_protocol.dart` | 453 | status truncation and wire mapping, the IDENT codec, proof message, `verifyIdent` | pure codec plus async crypto |
| `mux/mux_channel.dart` | 440 | abstract `MuxChannel` contract, `MuxPriority`, `MuxChannelState` | **the binding seam** |
| `mux/binding_connection.dart` | 111 | abstract base for non-frame bindings (QUIC) | **the binding seam** |
| `mux/mux_connection.dart` | 2306 | `MuxOptions`, `MuxConnection` (frame mux): handshake, ids, OPEN handling, control, scheduler, keep-alive, shutdown | the bulk of the port |
| `mux/frame_mux_channel.dart` | 1362 | `FrameMuxChannel`: send window, pending sends, receive window, credit batching, close | the bulk of the port |
| `transport/transport_capabilities.dart` | 75 | `FrameLimited`, `AbortableTransport`, `OutputBufferedTransport`, `OutputReadyTransport` | optional capability interfaces |
| `transport/stream_framing.dart` | 211 | preamble and length-prefix codec, incremental decoder | pure codec |
| `transport/framed_byte_transport.dart` | 678 | shared machinery for byte-stream transports: linger, throttle, coalescing, close timeout | Node `net.Socket` |
| `transport/stream_transport.dart` | 273 | TCP/socket wrapper | Node `net` |
| `transport/web_socket_transport.dart` | 341 | WebSocket client | browser `WebSocket`, Node 22 global `WebSocket`, or `ws` |
| `transport/web_socket_server.dart` | 767 | WebSocket server with upgrade checks | Node `ws` (or a raw upgrade) |
| `transport/memory_transport.dart` | 227 | in-memory pair for tests | test harness |
| `talk/talk_frame.dart` | 546 | Talk header codec and rules | pure codec |
| `talk/talk_bulk.dart` | 542 | bulk reference, `_bulk` open payload, `BulkRoutes` | codec plus routing |
| `talk/talk_channel.dart` | 4878 | Talk runtime (requests, streams, windows, timeouts, bulk) | large; probably its own work item |
| `address/channel_address.dart` | 166 | OPEN payload address header | pure codec |
| `identity/credential.dart` | 495 | credential codec plus scope patterns | pure codec |
| `identity/credential_verifier.dart`, `credential_issuer.dart`, `ed25519.dart`, `holder_key.dart`, `secure_random*.dart` | — | verification, issuing, a pure-Dart Ed25519, randomness | WebCrypto |

---

## 2. Primitive codecs

**Integers.**
* All little endian.
* u24 is three bytes, `b0 | b1<<8 | b2<<16`. That's safe in JS: the maximum is 2^24−1 and it is never negative.
* u32 must be read with `DataView.getUint32(o, true)` or `(... ) >>> 0`. `b3 << 24` goes negative in JS.
* u48: `lo = getUint32(o, true); hi = getUint16(o + 4, true); value = hi * 0x1_0000_0000 + lo`. Write it with `lo = v % 2**32`, `hi = Math.floor(v / 2**32)`. Dart's `ByteWriter.u48` uses `~/ 0x100000000`. Encoders range-check with `Number.isInteger(v) && v >= 0 && v <= 0xFFFF_FFFF_FFFF`.
* Dart `ByteReader` throws `FormatException` on truncation, and every codec converts that to `ProtocolException`. The TS reader should throw an internal truncation error that each decoder maps to its protocol error.

**Name** (`name.dart`, wiki `test-vectors.md` "Names"):
* Exactly 8 bytes of UTF-8, zero padded. The text is the bytes up to the first zero.
* Decoding rejects a non-zero byte after a zero byte, e.g. `61 00 62 00 00 00 00 00`.
* Encoding rejects text longer than 8 bytes of UTF-8 (never truncate) and text containing U+0000.
* Comparison is bytewise.
* Names starting with `_` (0x5F) are reserved: `_ns`, `_bulk` and `_relay` are in use.
* The empty name is all zeros and is valid. As a Talk procedure it means "absent"; as an address service it means the default service.
* The Dart `Name.fromBytes` doesn't validate UTF-8. Bytes such as `FF` are allowed in a name read off the wire, so `toString` must not throw. Recommendation: key maps on a 16-hex-digit string or on a `bigint` from the 8 bytes. The text form isn't unique for malformed UTF-8, so it's a poor key.

**len8 string.** u8 length, then that many bytes. It is used for the IDENT intent and receiver, the address host hint, credential identities, naming endpoints and the CONNECT intent. `ByteWriter.string8` throws over 255 bytes.

**UTF-8 decoding** comes in two modes, and a port has to pick per field:

| Field | Mode |
| --- | --- |
| Status reasons | lenient: replacement, `allowMalformed: true` |
| Credential identity strings, CONNECT requester | strict: malformed is a protocol error or invalid credential (vector `01 FF`) |
| Address host hint, IDENT receiver compared as bytes | lenient: Dart's `ByteReader.string8` decodes with `allowMalformed: true` (the IDENT receiver check compares bytes, not strings) |

In JS, use `new TextDecoder('utf-8', {fatal: true})` for strict decoding and the default decoder for lenient.

**Hex helpers for tests.** The Dart `hexString` gives uppercase, space-separated bytes (`"02 02 00 AA"`). The wiki vectors use the same format, so a TS `hex()` / `unhex()` pair (tolerating extra spaces, as in `'0C 00 00 00  12 02 00'`) lets vectors be pasted in verbatim.

---

## 3. Status codes (wiki `status-codes.md`, Dart `status.dart`, `mux_protocol.dart`)

One u16 table is used everywhere: mux CLOSE, GOAWAY, Talk ABORT, and the QUIC resets.

| Code | Name | Notes |
| --- | --- | --- |
| 0 | OK | |
| 1–16 | gRPC codes | CANCELLED 1, UNKNOWN 2, INVALID_ARGUMENT 3, DEADLINE_EXCEEDED 4, NOT_FOUND 5, ALREADY_EXISTS 6, PERMISSION_DENIED 7, RESOURCE_EXHAUSTED 8, FAILED_PRECONDITION 9, ABORTED 10, OUT_OF_RANGE 11, UNIMPLEMENTED 12, INTERNAL 13, UNAVAILABLE 14, DATA_LOSS 15, UNAUTHENTICATED 16 |
| 17–31 | reserved | |
| 32 (0x20) | PROTOCOL_ERROR | |
| 33 (0x21) | GOING_AWAY | |
| 34 (0x22) | CONNECTION_LOST | **Local only**. Sent on the wire as UNAVAILABLE (14). |
| 35 (0x23) | FRAME_TOO_LARGE | |
| 36 (0x24) | UNSUPPORTED | |
| 37 (0x25) | MOVED | fixed fields: u48 owner, u32 epoch (sharding) |
| 38 (0x26) | RELOCATED | same fixed fields |
| 39–255 | reserved | |
| ≥256 | application codes | **Talk ABORT only.** Never in CLOSE or GOAWAY. |

**Status payload layout.** `u16 code`, then any fixed fields the code defines, then a UTF-8 reason taking the rest.

**Decoding (`Status.decode`):**
* An empty payload is OK. This applies to mux CLOSE and GOAWAY.
* A 1-byte payload is malformed: FormatException in Dart, mapped by the carrier to a protocol error.
* A 2-byte payload is the code with no reason.
* With more than 2 bytes, the reason is decoded leniently, **and the raw bytes after the code are kept**.
  * `encode()` re-emits those raw bytes, so relaying is byte-exact.
  * Equality compares code plus raw bytes.
  * `toString` prints hex for reasons with control characters or U+FFFD.
* An unknown code (17–31, 39–255) decodes fine; `known` is null.
* For MOVED/RELOCATED, a payload too short for the 10 fixed bytes reads as "owner and epoch unknown". It isn't an error.
* For other codes, bytes that look like fields are simply reason bytes. Vector: `0A 00 2B 1A 00 00 00 00 07 00 00 00` is ABORTED with a 10-byte reason.

**Talk ABORT differs** (`TalkFrame.status`):
* An empty payload means **UNKNOWN**, not OK.
* A 1-byte payload is a channel protocol error.
* An abort carrying OK is treated as UNKNOWN.

**Sending CLOSE/GOAWAY (`statusForWire` / `fitStatus` / `truncateStatus`):**
1. `checkMuxStatus` throws ArgumentError for application codes (≥256) or a negative code. That is a local API error, so nothing is sent.
2. CONNECTION_LOST is rewritten to UNAVAILABLE by patching only the two code bytes. Any body bytes are kept.
3. The status is shortened so the *encoded* payload is at most `min(1024, peerMaxFrameSize − headerOverhead)` bytes:
   * The cut lands at `2 + room`, then backs off at most 3 UTF-8 continuation bytes (`(b & 0xC0) == 0x80`) so it starts a character.
   * For MOVED/RELOCATED, if the cut would fall inside the 10 fixed bytes, everything after the code is dropped.
   * When room ≤ 0, only the code is kept.
   * Fast path: when `reason.length * 3 <= room`, the status fits as is. JS `string.length` is also UTF-16 code units, so the bound holds the same way.
   * Overhead is 3 (short CLOSE header) or 7 (long) for CLOSE; for GOAWAY it is the 3-byte channel-0 header plus 1 type byte = 4.
   * Dart test values: after LIMITS 64/1, a CLOSE reason of 'z'×100 on a short id is cut to 'z'×59 (64 − 3 header − 2 code). Under limit 32, a GOAWAY 'w'×100 is cut to 'w'×26 (32 − 4 − 2).
4. **Receiving CLOSE/GOAWAY (`statusFromWire`).** An application code ≥256 becomes UNKNOWN, with the original status as text in the reason (`"code 300: text"`), so it can be relayed in a CLOSE of our own. A malformed payload (1 byte) is a connection protocol error.

**Exceptions.**
* `SwitchboardException` carries a `Status`. `ProtocolException` is its subclass with PROTOCOL_ERROR.
* Dart uses `ArgumentError` for local misuse (bad values) and `StateError` for misuse of state (send after close and so on).
* In TS: a `SwitchboardError extends Error` with `.status` and `.code`, a `ProtocolError` subclass, and plain `RangeError` / `TypeError` for misuse. Section 20 has more.

---

## 4. Transport layer (wiki `transport.md`, Dart `transport/*`)

### 4.1 Contract the mux needs

In Dart a transport is a `StreamChannel<Uint8List>`: a stream of incoming frames plus a sink for outgoing ones.

**Delivery.**
* Each event is exactly one frame of at least 1 byte.
* A zero-length frame is a protocol error *at the mux*. The stream binding never produces one: length 0 is a protocol error there.
* The transport never coalesces or splits frames.

**Errors and close.**
* An error is delivered as an error event followed by done.
* After an incoming error, **the sink stays writable** until the listener has seen the error and the end, so the mux can still write its GOAWAY `PROTOCOL_ERROR` (MemoryTransport tests rely on this).
* Cancelling the incoming subscription doesn't stop reading at the socket level. The transport keeps reading and discarding, so the peer's close is still noticed (linger).
* Closing the sink is a graceful close. The stream binding first flushes, then half-closes (FIN), then lingers reading until the peer closes or `closeTimeout` (10 s) passes, then destroys. The WebSocket binding sends close code 1000.

**Optional capabilities** (`transport_capabilities.dart`, found by `is` checks; in TS use duck typing, a symbol-keyed method, or an explicit capabilities object):

| Capability | Members | Used for |
| --- | --- | --- |
| `FrameLimited` | `maxFrameSize` (0 = unlimited) | The mux announces `min(own, transport)` in LIMITS and logs a warning if the transport's limit is smaller than `MuxOptions.maxFrameSize`. |
| `AbortableTransport` | `abort()` | `close()` falls back to abort when the graceful close doesn't finish within keepAliveTimeout. |
| `OutputBufferedTransport` | `bufferedOutputBytes`, `acceptedOutputBytes` (monotonic count of bytes the OS took), `isInputThrottled` | Keep-alive treats output progress as liveness while the peer is reading slowly; the input throttle backs off reads. |
| `OutputReadyTransport` | `isOutputReady`, `outputReady` (a future that completes when ready) | Enables the mux's **output scheduler** (§11). Without it the mux writes every frame straight to the sink, in the order produced. |

### 4.2 Stream binding (TCP, Unix sockets, pipes)

**Preamble.** Both sides send it first, independently, and do not wait for the peer's.

```
offset size field
0      4    magic   "SWBD" = 53 57 42 44
4      2    u16 LE  version = 1
6      2    u16 LE  flags (sender sets 0; receiver ignores)
```

**Frames.** `u32 LE length`, then `length` bytes. One frame is one mux frame.

**Decoder rules** (`StreamFrameDecoder`, incremental). They are checked in this order:
1. Preamble magic ≠ `SWBD` is a ProtocolException (PROTOCOL_ERROR).
2. Version ≠ 1 is a `SwitchboardException(UNSUPPORTED)`, **not** a ProtocolException.
3. Flags are ignored, so `53 57 42 44 01 00 FF FF` is fine.
4. Length 0 is a ProtocolException, even with `maxFrameSize` 0 (unlimited).
5. Length > `maxFrameSize` (default 1 MiB, 0 = unlimited) is `FRAME_TOO_LARGE`, decided **from the 4 length bytes alone**, before the body arrives or is allocated.
   * `00 00 00 80` (2^31) and `FF FF FF FF` must both read as large positive numbers. Use `getUint32`, never `| 0` or `<< 24`.
6. Frames completed before the offending bytes in the same chunk are **delivered first**, then the error. Vector: preamble, `01 00 00 00 02`, then `00 00 00 00` delivers frame `02`, then the ProtocolException.
7. Delivered frames are copies. Dart keeps one growing buffer and copies frames out of it. In TS, `subarray` views into a reused buffer would be overwritten, so either copy or allocate a fresh buffer per frame.

**Behaviour of Dart `FramedByteTransport`** (shared by stream and WS server).
* **Output coalescing.** Small frames are concatenated into writes of up to 64 KiB. This is purely a performance measure.
* **Output high-water mark.** `outputHighWaterMark` defaults to 16 MiB of unflushed output. Above it the transport stops reading input until half has drained. This stops a peer that sends PINGs without reading from making us buffer unbounded PONGs. It is exposed as `isInputThrottled`.
* **Linger** after close, as above. `closeTimeout` is 10 s, then destroy (abort).
* **TCP specifics** (`fromSocket` / `connectTcp`): TCP_NODELAY on, and abort is `socket.destroy()`. A connect failure surfaces as the platform socket error (`SocketException`).
  * Node equivalents: `socket.setNoDelay(true)`; `write()` returns false, then wait for `'drain'`; `writableLength` for buffered bytes; `bytesWritten` for accepted bytes; `pause()` / `resume()` for throttling; `end()` for FIN; `destroy()` for abort.
  * Node's `'end'` event (peer FIN) needs `allowHalfOpen: true` if we want to keep writing after the peer's FIN. Dart lingers on *our* close, not theirs.

### 4.3 WebSocket binding

**Framing.**
* Subprotocol `switchboard`.
* Each binary message carries one mux frame. A **text message is a protocol error**.
* WebSocket close code 1000 on a graceful close.
* The mux sends its GOAWAY first, then closes, then keeps reading.

**Client** (Dart `WebSocketTransport`):
* Built on `web_socket_channel`.
* Implements `FrameLimited` only, so the scheduler is not used there.
* A connect timeout is UNAVAILABLE.
* In the browser there is no drain event, only `bufferedAmount`. An output-ready capability would have to poll it, or not be offered; the Dart web client doesn't offer it.

**Server** (Dart `WebSocketServer`). From the doc comments:
* Refuse `permessage-deflate`.
* Bound the whole message size, **counting all fragments, before assembly**; `maxMessageFragments` is 4096.
* HTTP status codes:
  * 426 when the request is not an upgrade, or the WebSocket version ≠ 13.
  * 400 for a bad `Sec-WebSocket-Key`, or when subprotocols are offered without `switchboard`.
* WebSocket close codes:
  * 1002 on WebSocket protocol violations.
  * 1003 on a text message.
  * 1009 on oversize.
* In Node with `ws`: `new WebSocketServer({ perMessageDeflate: false, maxPayload, handleProtocols })`.
  * `maxPayload` bounds the assembled message, and ws checks it incrementally per fragment. That matches "bound before assembly".
  * `ws` has no fragment-count limit, so 4096 tiny fragments can't be bounded exactly that way.

### 4.4 Frame size

* Each mux frame is one transport frame.
* The 3- or 7-byte mux header counts toward the limit.
* The receiver's limit is enforced by the transport before allocation, and again by the mux: an incoming frame over `MuxOptions.maxFrameSize` ends the connection with GOAWAY `FRAME_TOO_LARGE`.
* The sender learns the peer's limit from LIMITS. `send()` of a payload whose frame would exceed it, after splitting rules (§10), fails locally with `FRAME_TOO_LARGE`.
  * Dart test: after LIMITS 64/1, `send(62 bytes)` fails, while `send(61)` gives one 64-byte frame.

### 4.5 MemoryTransport (tests)

* `MemoryTransport.pair({delay, maxFrameSize})` returns two connected ends.
* Frames are copied.
* `maxFrameSize` makes it `FrameLimited`.
* A failed side stays writable, as above.
* The Dart mux tests mostly run over this, or over `RawPeer` (§19), which reads and writes raw mux frames byte for byte.

---

## 5. Mux frames (wiki `mux.md`, Dart `mux_frame.dart`)

### 5.1 Header

```
offset size  field
0      1     u8 flags
1      2|6   channel id: u16 LE if flags & 0x02 (SHORT), else u48 LE
3|7    ...   payload (rest of the transport frame)
```

| Bits | Meaning | On receive |
| --- | --- | --- |
| `0x02` SHORT | 2-byte id | |
| `0x30` CMD | `(flags & 0x30) >> 4`: 0 DATA, 1 OPEN, 2 CLOSE, 3 reserved | 3 is a protocol error |
| `0x01`, `0x04`, `0x08` | reserved, *incompatible* (Dart mask `0x0D`) | any set is a protocol error |
| `0x40`, `0x80` | reserved, *compatible* (Dart mask `0xC0`) | ignored |

So DATA is `0x02` (short) or `0x00` (long), OPEN is `0x12` or `0x10`, and CLOSE is `0x22` or `0x20`.

**Encoding.**
* Use the short form iff id ≤ 0xFFFF. Encoders never set compatible bits.
* The id must be an integer 0 ≤ id ≤ 0xFFFFFFFFFFFF, else ArgumentError.

**Decoding.** Dart checks in this order, each a ProtocolException:
1. Empty frame.
2. Incompatible reserved flag bits.
3. Command 3.
4. Truncated header: fewer than 3 bytes short, or 7 long.
5. Reserved id.
6. OPEN or CLOSE on channel 0.

A short id sent in long form (e.g. `00 02 00 00 00 00 00 AA`) decodes as the same channel (id 2). Dart records `longForm = true` so that `shortIdsOnly` can refuse it.

**Reserved ids** (`MuxFrame.isReservedId`): **1**, **0xFFFF**, **0xFFFF00000000** and **0xFFFFFFFFFFFF**. Any frame on them is a protocol error, for any command, DATA included.

Channel **0** is the control channel. Only DATA is allowed on it; its payload is a control message (§6).

**`headerSizeFor(id)`** is 3 when id ≤ 0xFFFF, else 7. It is used for status truncation and subframe sizing.

### 5.2 Commands

* **OPEN** opens channel `id`. The payload is the open payload: opaque to the mux, an address header in practice (§17).
* **DATA** carries one subframe. It may be empty, and an empty subframe still costs 16 bytes of window.
* **CLOSE** carries an optional status payload. Empty means OK; 1 byte is a protocol error.

The receiver rules are in §9.

---

## 6. Control messages (DATA on channel 0)

```
u8 type, then the control payload
```

| Type | Name | Payload | Rules |
| --- | --- | --- | --- |
| 0x01 | PING | opaque | Sender ≤125 bytes (Dart `MuxControlMessage.ping` throws ArgumentError above). The receiver answers **any** PING it accepts with a PONG carrying the same bytes. |
| 0x02 | PONG | opaque | Completes the pending `ping()` with the same payload. Unsolicited ones are ignored. |
| 0x03 | GOAWAY | status | The sender shortens it to fit (§3). The receiver maps app codes (`statusFromWire`). A second GOAWAY is ignored. A malformed (1-byte) status is a protocol error. |
| 0x04 | LIMITS | `u32 maxFrameSize, u32 maxChannels, u32 initialWindow`, more may follow | At least 12 bytes; extra trailing bytes are ignored. initialWindow 0 is a protocol error. maxFrameSize and maxChannels 0 mean "no stated limit"; both are advisory. |
| 0x05 | NONCE | 32 bytes | §8 |
| 0x06 | IDENT | `u16 credLen, cred, len8 intent (≤64), len8 receiver, proof = rest` | §8 |
| 0x07 | CREDIT | `u48 channelId, u32 bytes` | Exactly 10 bytes. Dart encode: id ≤ 2^48−1, 0 ≤ bytes ≤ 2^32−1. |
| 0x08 | HELLO | QUIC binding only | Over the frame mux it is never sent, and ignored as unknown. |
| other | — | — | **Ignored** (logged). Unknown types decode fine. |

**Connection protocol errors on the control channel:**
* DATA on channel 0 with an empty payload (no type byte).
* A control payload (after the type byte) of more than **1024** bytes. 1024 itself is fine.
* LIMITS shorter than 12 bytes, or with initialWindow 0.
* CREDIT that isn't exactly 10 bytes.
* A malformed GOAWAY status.
* The NONCE and IDENT envelope violations of §8.

**LIMITS encode** (`MuxLimits.encode`):
* ArgumentError for any negative value or one above 2^32−1.
* ArgumentError for initialWindow 0.
* Full u32 values round-trip: `limits(0xFFFFFFFF, 0x80000000, 0xFFFFFFFF)` is `FF FF FF FF 00 00 00 80 FF FF FF FF`.

---

## 7. Connection start-up

### 7.1 Sequence

There is no negotiation round trip. Both sides start at once and send:

1. Stream binding only: the 8-byte preamble.
2. LIMITS as the **first mux frame**, when `announceLimits` (default true): `LIMITS(maxFrameSize = min(own, transport FrameLimited), maxChannels, initialWindow)`.
3. Anything else. A side that wants identity may send NONCE right away.

Roles:
* **Initiator** (the side that connected) allocates **even** ids from 2.
* **Acceptor** allocates **odd** ids from 3.

The role is a constructor flag (`isInitiator`), not negotiated.

**The Dart constructor:**
* Validates options. Out-of-range values throw ArgumentError.
* Logs the FrameLimited warning.
* Sets up the LIMITS wait *before* sending LIMITS, because a synchronous transport can answer within the write.
* Sends LIMITS, then starts keep-alive.

A side that doesn't announce (`announceLimits: false`, as the embedded profile and raw harness tests do) is assumed to accept 64 KiB windows and to have no stated frame limit.

### 7.2 awaitPeerLimits (default true)

**While waiting.**
* DATA on channels opened *before the peer's first frame arrived* is held. Their send window is 0 and `sendWindow` reports the 64 KiB assumption.
* OPEN and CLOSE go out at once.

**What ends the wait:**
* (a) The peer's LIMITS.
* (b) Any other first frame from the peer. A peer that announces sends LIMITS first, so one that sends anything else first doesn't announce, and the default applies.
* (c) `keepAliveTimeout`. Dart arms this as `_limitsTimer`.

**After the wait.**
* Held channels get the announced window, or the 64 KiB default.
* A subframe accepted during the wait that no longer fits the announced window or frame limit fails its `send` future with FRAME_TOO_LARGE.
* `MuxChannel.awaitingLimits` and `limitsKnown` (a future) expose this state.

With `awaitPeerLimits: false`, DATA goes at once against the 64 KiB assumption, and channels opened before LIMITS arrived keep that window.

### 7.3 Window bookkeeping that depends on LIMITS

* **Send window of a channel.** It starts at the peer's initialWindow *as known when the channel opened*. A later LIMITS affects only channels opened after it was received.
* **Receive window of a locally opened channel** = own initialWindow, with credit returned against it. The peer learns of the channel only after our LIMITS, which went first.
* **Receive window of a peer-opened channel** = `max(ownInitialWindow, 64 KiB)`, because the peer may have opened it before our LIMITS reached it. Credit is returned against `min(ownInitialWindow, 64 KiB)` (`creditWindow`), so a sender holding either window never stalls.
* **Rejected ids** keep that same `max(...)` as their remaining window (§9.4).

### 7.4 Limits change mid-connection

A later LIMITS replaces `_peerLimits`, which affects:
* the peer frame limit, used for subframe sizing and status truncation from then on;
* the initial window, for channels opened after it.

Dart test: after LIMITS(64, 1, …), `send(62)` throws FRAME_TOO_LARGE, while `send(61)` produces a 64-byte frame (3 + 61).

---

## 8. Identity (wiki `identity.md`, Dart `mux_connection.dart`, `mux_protocol.dart`, `identity/*`)

### 8.1 Wire rules

**NONCE:**
* At most one per side per connection. The pair is fixed for the connection's life.
* Receiving one that isn't 32 bytes is a protocol error, as are a second NONCE from the peer and one equal to our own nonce.
* When the peer's NONCE arrives and we haven't sent ours, we send ours immediately. The connection's `identityRequested` future then completes: the peer asked us to identify. A listener must not answer with an IDENT on its own; the application decides.
* Both sides sending at once is fine: neither answers again.
* Randomness: 32 bytes from a CSPRNG (`crypto.getRandomValues`).

**IDENT envelope** (`MuxIdent.decode`). Each of these is a protocol error, **whether or not a verifier is configured**, so that malformed input doesn't reveal whether a node checks identity:
* IDENT arriving before we have both our NONCE sent and the peer's received.
* Truncated: the credential length, the credential, the intent length or intent, or the receiver length or receiver.
* An intent over 64 bytes.

The proof is everything after the receiver. The codec doesn't check its length; verification does (0 for bearer, 64 for holder-key).

**Without a verifier** (`identityVerifier == null`), a well-formed IDENT is ignored: no credential or receiver check, no state change.

**With a verifier:**
1. Pause the transport. Frames already decoded go to `_heldFrames`.
2. The receiver check comes first (`identReceiverProblem`):
   * A non-empty receiver must equal `localIdentity` byte for byte.
   * A side with no local identity refuses every IDENT that names a receiver.
   * `requireNamedIdent` refuses an empty receiver.
3. Then the credential is verified (signature, expiry, issuer key) and the proof checked:
   * The proof is a 64-byte Ed25519 signature for a holder-key credential.
   * The proof must be empty for a bearer credential.
4. On failure:
   * Send GOAWAY `UNAUTHENTICATED` with the generic reason `"identification failed"`; the cause goes only to the log.
   * Drop the held frames unprocessed and shut down.
   * Connection `done` is UNAUTHENTICATED.
5. On success:
   * Set `peerIdentity`, which a later IDENT can replace only if it verifies.
   * Complete `peerIdentified`, signal the identity event, process the held frames in order, and resume.
   * A channel opened right after the IDENT is dispatched under the new identity.

**Proof message:**

```
"SWBIDENT" (53 57 42 49 44 45 4E 54)
‖ sender nonce (32, the IDENT sender's)
‖ receiver nonce (32)
‖ u8 len(intent) ‖ intent
‖ u8 len(receiver) ‖ receiver
```

The exporter-bound variant (QUIC only) is `"SWBIDTLS" ‖ nonces ‖ 32-byte TLS exporter ‖ len8 intent ‖ …`; that belongs to the QUIC thread.

Vector P in wiki `test-vectors.md` uses sender nonce 32×`11`, receiver nonce 32×`22`, intent `01 02 03 04` and receiver `consumer`, giving an 86-byte message.

**`identify()`** (the client side of the handshake):
1. Send NONCE if not sent yet.
2. Wait for the peer's NONCE.
3. Send IDENT: credential, intent, receiver, and the proof signed with the holder key.
4. Send PING right after it. Its PONG means the peer accepted (or doesn't check identity), while GOAWAY UNAUTHENTICATED means it refused.

The wait is bounded by `identityTimeout` (default 10 s; zero means unbounded) or a per-call timeout.

**Keep-alive** doesn't probe while an IDENT is being verified.

### 8.2 Credential (summary; full layout in wiki `identity.md`, vectors C1/C2 in `test-vectors.md`)

```
u8  version = 1
u8  kind (1..3; 0 and 4+ invalid)
8B  key id (Name rules)
len8 identity (strict UTF-8)
u32 issued at (unix s)
u32 expires at (unix s)
u8  holder key length (0 or 32), then the key
u8  scope count, then per scope: u8 right, 8-byte name pattern
u8  signature kind: 1 = HMAC-SHA256 (32 bytes), 2 = Ed25519 (64 bytes)
signature (covers all bytes before it)
```

Rules:
* The total is at most 637 bytes. A trailing byte is invalid.
* HMAC with a holder key is invalid even if the MAC verifies.
* Unknown rights are dropped on decode. The signature still covers them.
* The scope pattern `*` matches at the end only; see the wiki table, e.g. `n*c` matches only the literal `n*c`.
* Test keys come from RFC 8032 §7.1: the authority is test 1, the holder test 2. Signatures are deterministic, so the vectors are byte-exact.

**JS crypto mapping:**
* Ed25519 sign/verify: `crypto.subtle.importKey('raw', pub, {name: 'Ed25519'}, …)` for public keys.
  * Private seeds need PKCS#8 wrapping: the prefix `302e020100300506032b657004220420` plus the 32-byte seed.
  * Node 22 and current browsers support Ed25519 in WebCrypto.
* HMAC-SHA256 via `subtle.sign('HMAC')`.
* All of these are **async**. Dart's `verifyIdent` is already async, so that matches.
* The credential **codec** (parse and scope checks) stays sync. Only the signature checks are async.
* Dart caches verified credentials; a TS port should too, keyed by the credential bytes.

---

## 9. Channel lifecycle

### 9.1 Id allocation (`_allocateId` / `_scan`)

* Each side allocates from its parity: initiator even from 2, acceptor odd from 3.
* **Increment within the short range** (≤ 0xFFFF), wrapping from the top of the short range back to the first id. This keeps long-lived connections on short ids.
* Skip ids that are reserved, open (`_links`), or awaiting the peer's CLOSE (`_awaitingClose`).
* **Enter the long range only when every short id of our parity is in use.** It starts at 0x10000 (even) or 0x10001 (odd), wraps up to 0xFFFFFFFFFFFF and skips reserved ids.
* With `shortIdsOnly`, there's no long range: RESOURCE_EXHAUSTED `"no free channel id"`.
* Exhaustion of both ranges is also RESOURCE_EXHAUSTED.
* `open()` throws synchronously in Dart, so this is a sync error.
* An id is reusable only once **mutually closed**: our CLOSE was sent and the peer's received.

### 9.2 States

| State | Means | Can send | Receives |
| --- | --- | --- | --- |
| OPEN | after OPEN sent or received | yes | DATA, CLOSE |
| HALF_CLOSED_LOCAL | our CLOSE queued or sent, the peer's not yet received | no | **DATA is still delivered**, and the peer's CLOSE |
| CLOSED | both CLOSEs exchanged, or the connection ended, or the confirm timed out | no | — |

There is no observable half-closed-remote state. Receiving the peer's CLOSE while OPEN sends the confirming CLOSE and goes straight to CLOSED.

### 9.3 Receiver rules per frame

**OPEN id (from the peer):**
* Wrong parity is a protocol error. Peer ids must be odd if we're the initiator, even otherwise.
* An id that is open, half-closed or awaiting close (rejected or timed out) is a protocol error.
* Otherwise it is admitted, or *rejected* (§9.4).
* An admitted channel:
  * gets send window = the peer's current initialWindow, and receive window and credit window per §7.3;
  * keeps a **copy** of its open payload, which counts toward buffered bytes and the open payload budget;
  * goes to the `incoming` queue.

**DATA id:**
* Open channel whose OPEN we have written, and whose peer CLOSE hasn't arrived: charge the cost against the receive window.
  * Beyond the window is a protocol error (GOAWAY PROTOCOL_ERROR).
  * Otherwise queue it for delivery. If the channel is discarding (the reader cancelled) or ending, count it as consumed at once.
* Id in `_awaitingClose` (we closed it without a channel): drop it, but charge it against the remembered window; beyond that window is a protocol error.
* Anything else is a protocol error. That covers unknown ids, ids after the peer's CLOSE, and a locally opened channel whose OPEN isn't written yet.

**CLOSE id:**
* Malformed status is a protocol error.
* An open channel whose peer CLOSE hasn't arrived goes through `receiveClose(status)`:
  * If we haven't sent our CLOSE, send the confirming CLOSE (empty, meaning OK) **after** freeing the id. That is the order in `MuxChannelLink.receiveClose`: release, then confirm.
  * Drop our queued DATA for the channel. Pending sends fail FAILED_PRECONDITION.
  * Deliver whatever the peer sent before its CLOSE, then end the stream.
  * `done` completes with the end status.
* An id in `_awaitingClose`: the confirmation of a CLOSE we sent without a channel. Free the id and drop any queued DATA.
* Anything else (unknown, or a second CLOSE) is a protocol error.

**End status of a channel** (`done`): the **first non-OK status sent or received**, else OK.
* If the confirm timed out with OK, it is DEADLINE_EXCEEDED (§9.5).
* If the connection ended first, it is the connection's channel status (§12.3).

### 9.4 Rejections (`_handleOpen` → `_reject`)

Checked in this order:

| Condition | CLOSE status |
| --- | --- |
| We sent GOAWAY, unless the OPEN is a `_bulk` channel whose parent is open and hasn't received the peer's CLOSE (`_continuesOpenChannel`) | GOING_AWAY |
| The application cancelled `incoming` | UNAVAILABLE `"not accepting channels"` |
| `maxChannels > 0` and (open count ≥ maxChannels **or** undelivered backlog ≥ maxChannels) | RESOURCE_EXHAUSTED `"at most N channels"` |
| open payload budget exceeded (`_openPayloadBudget` = min(maxOpenPayloadBytes, receiveHighWaterMark / 2), counting only the positive ones) | RESOURCE_EXHAUSTED |

A rejection:
* sends CLOSE with the status, shortened to fit;
* keeps **only** the id and its remaining receive window in `_awaitingClose`; no channel object, no payload;
* drops later DATA for the id, but charges it to that window (§9.3).

The peer's confirming CLOSE frees the id.

If `_awaitingClose.length >= maxPendingRejections` (default 1024, 0 means unlimited) when a new rejection is needed, the connection fails with GOAWAY RESOURCE_EXHAUSTED.

Dart test `mux_flow_test`: after a rejection, two 32752-byte DATA frames are accepted (2 × 32768 = 65536, the window), and a third, even empty, is a protocol error.

### 9.5 Local close (`MuxChannel.close(status)`)

* Application codes throw ArgumentError (sync).
* The CLOSE is queued **behind the DATA handed over before it**, including subframes still waiting for credit (`_pendingClose`). The state becomes HALF_CLOSED_LOCAL.
* Once our CLOSE is queued we send no more CREDIT. Once the peer's CLOSE has arrived we send no CREDIT either.
* **closeConfirmTimeout** (default 30 s; zero waits forever):
  * The channel is closed locally: `done` completes and it no longer counts as open.
  * If our CLOSE was still waiting behind credit-starved DATA, that DATA is dropped (the pending sends fail FAILED_PRECONDITION) and the CLOSE goes out at once. An OK close becomes **DEADLINE_EXCEEDED**, on the wire and in `done`.
  * The id stays reserved in `_awaitingClose` until the peer's CLOSE arrives, or the connection ends. It counts toward maxPendingRejections.
* The `sink` (StreamSink) maps onto these operations:
  * `add` is send without awaiting; it is silently dropped once the channel can't send.
  * `addError(SwitchboardException)` closes with that status.
  * `close()` closes OK.

### 9.6 `openAfter` (bulk channels)

`connection.open(payload, after: parent)`, or `channel.openAfter`, orders a child's OPEN within the parent's frame queue, so it goes out just before the parent's next DATA. A Talk BULK message waiting for credit therefore doesn't leave its bulk channel open long before it.

Special rules:
* It is not refused after either side's GOAWAY, unlike `open`.
* If the parent stops before the child's OPEN is written, the child ends CANCELLED and its OPEN is never written.
* CREDIT for an id whose OPEN isn't written yet is ignored. The peer can't know the id yet.

### 9.7 Incoming delivery

`incoming` is a single-subscription stream of peer-opened channels. A channel waits there (undelivered backlog) until the application takes it, even if the peer has already closed it.
* The backlog is capped at maxChannels (§9.4).
* Cancelling the incoming subscription:
  * closes every undelivered channel UNAVAILABLE;
  * makes later OPENs get UNAVAILABLE;
  * keeps the connection running.

**`_bulk` footgun.** A `_bulk` channel arrives on `incoming` like any other. A raw consumer of `incoming` that runs Talk must hand it to `TalkChannel.adoptBulk`, or route it via the `BulkRoutes` the Switchboard dispatcher uses. Otherwise it sits there. In TS the layer above the mux should own this routing so raw users can't get it wrong (§20).

---

## 10. Flow control and backpressure

**Cost** = payload length + 16 (`MuxCredit.costOf`; `frameOverhead` 16). `costOf(0)` = 16 and `costOf(100)` = 116.

### 10.1 Send side (`FrameMuxChannel`)

**Window state.**
* `sendWindow` starts at the initial window (§7.3), grows with CREDIT, and shrinks by cost when a frame is *handed to the connection*. Dart's `_reservedSendCost` tracks cost handed over but not yet written by the scheduler.
* A CREDIT that makes `sendWindow + reservedSendCost > 2^32 − 1` is a **connection protocol error**.
* CREDIT is ignored after the peer's CLOSE, after our CLOSE is queued, or when the channel is closed.

**Subframe size limit (`maxSubframeLength`).**
* Formula: `min(frameCostLimit − 16, peerMaxFrameSize − headerSize)`.
* `frameCostLimit` is half the initial send window, but at least 16 when the window allows it.
* With defaults that is 32768 − 16 = **32752** bytes.
* This is the largest frame guaranteed to fit once the receiver has consumed everything: credit comes back at the half-window point.

**`send(subframe)`.** Returns a Promise in TS terms.
* Throws **synchronously** with FAILED_PRECONDITION if `!canSend`, and FRAME_TOO_LARGE if the subframe is longer than `maxSubframeLength`.
* If nothing is queued and the cost fits the window, it is handed over at once and the returned future is already complete.
* Otherwise it is queued FIFO, and the future completes when it is handed over.
* Handing over **copies** the bytes. The caller may reuse its buffer only after the future completes.
* If the channel can no longer send (peer closed, connection ended, confirm timeout), the future rejects with FAILED_PRECONDITION. That rejection is **never reported as unhandled**; in TS, attach an internal no-op catch.

**Bulk-priority channels** (`priority = MuxPriority.bulk`):
* A non-empty `send` is a byte run, cut into chunks of at most `bulkChunkSize` (64 KiB), the peer's frame limit, and the window at hand.
* Chunks are never smaller than half the initial window allows, unless the remainder is.
* The future completes when the last chunk is handed over.
* FRAME_TOO_LARGE only when the peer's frame limit leaves no room for one payload byte.

**Writability.**
* `isWritable` = `canSend && no pending sends && sendWindow ≥ costOf(maxSubframeLength)`. It is false while LIMITS is awaited.
* `whenWritable` resolves when that becomes true, and rejects FAILED_PRECONDITION when the channel can no longer send. It never reports an unhandled error.
* Neither reflects the output scheduler.

**Backpressure contract.** A sender that awaits each `send` holds at most one window handed over plus one waiting frame. A sender that doesn't await, like `sink.add`, queues without bound in the channel. What the peer *receives* is bounded by the window either way.

### 10.2 Receive side

* Each DATA is charged against `receiveWindow`; beyond it is a connection protocol error (§9.3).
* **Delivery is consumption** by default.
  * Credit is counted when the subframe is handed to the listener: the synchronous `StreamController` add, while the listener is active and not paused.
  * Buffered-but-undelivered data is not consumed.
  * A paused or absent listener stalls the channel's credit and nothing else.
* **Batching.** Credit is sent when unreturned ≥ `max(1, creditWindow / 2)` (`_consume`). There is **no timer**: a channel whose consumer takes less than half a window never returns credit until it does. This is by design; the window is twice the frame limit.
* **`manualCredit`** (used by Talk):
  * Delivered subframes become `heldBytes` instead of consumed.
  * The listener calls `consumed(bytes, {subframes})` when the application has taken the message. Cost = bytes + 16 × subframes.
  * Reporting more than is held throws StateError; a negative value throws ArgumentError.
* **`grant(bytes)`** raises the window beyond the initial one, for large transfers.
  * It sends CREDIT at once for `bytes` plus whatever was unreturned, and grows `creditWindow`.
  * It throws ArgumentError if bytes ≤ 0, or if the window plus queued plus held plus credit would pass 2^32 − 1.
  * It does nothing once CREDIT can no longer go out.
  * Dart test: `grant` inside an automatic listener counts the frame just delivered.
* A cancelled reader (`stream` subscription cancelled) makes the channel *discard*: queued and later DATA count as consumed at once, so the peer isn't stalled.
* After the channel ends, `consumed` and `grant` are no-ops.

### 10.3 Connection-level backstops

* **Receive high-water mark** (`receiveHighWaterMarkBytes`, default 16 MiB, 0 disables).
  * It counts the sum of queued-undelivered cost over all channels plus held open payloads.
  * Above it the mux pauses the transport subscription; it resumes at ≤ half.
  * Keep-alive doesn't probe while paused.
  * Talk's own held messages, under manualCredit, don't count. The per-channel window bounds them.
* **Output high-water mark** lives in the transport (§4.2): 16 MiB unflushed pauses input until half has drained.
* **Open payload budget:** §9.4.

---

## 11. Output scheduling (local policy; nothing on the wire)

The scheduler is active only when the transport implements `OutputReadyTransport`. Otherwise every frame is written to the sink immediately, in production order.

**Tiers:**
1. **Control** (channel 0): always first. PING/PONG, GOAWAY, LIMITS, CREDIT, NONCE/IDENT and OPEN/CLOSE use `_sendFrame` and are written at once, unless a CLOSE has DATA queued before it on its channel.
2. **Ordinary** channels: one frame per channel per turn, round-robin.
3. **Bulk** channels: one bulk frame after every `bulkZipper` (default 4) ordinary frames while bulk work waits, so ordinary traffic can't starve a transfer.

Within a channel, order is never changed.

**Implementation.**
* Dart uses intrusive linked lists of per-channel queues (`_outQueues`, `_ordinaryTurns`, `_bulkTurns`) and pumps while `isOutputReady`, then awaits `outputReady`.
* In TS, avoid `Set` iteration as a turn list (delete-and-reinsert during iteration degrades) and arrays with `shift()`. An intrusive doubly linked list, or a ring-buffer deque, keeps it O(1).
* `priority` can change at runtime (`reprioritize`): the channel moves between tiers.
* Dart test `mux_schedule_test` uses `GatedTransport`, an OutputReadyTransport with an `allow(n)` allowance, to check exact interleavings byte for byte.

---

## 12. Keep-alive, GOAWAY, shutdown and end statuses

### 12.1 Keep-alive

* `keepAliveInterval`: 10 s by default; null disables.
* One timer, lazily re-armed from a "since last receive" stopwatch, so frames don't reset timers.
* After `interval` of silence, send PING (counter payload) and arm `keepAliveTimeout` (10 s).
* Any received frame cancels the probe.
* When the probe times out, shut down with the lost status (§12.3), reason `"keep-alive timeout"`.
* No probe, and the stopwatch is reset, while:
  * receive is paused (high-water mark);
  * an IDENT is verifying;
  * output progressed. That means the transport is `OutputBufferedTransport` and (input throttled, or the scheduler holds frames), and `acceptedOutputBytes` moved since the last check: the peer is reading our backlog.
* The timer is armed *before* writing the PING, because a synchronous transport can answer within the write.

### 12.2 `ping()`, `goAway()`, `close()`

**`ping(payload?)`:**
* Payload ≤125 bytes, else a sync ArgumentError.
* Resolves with the round-trip time when the matching PONG arrives (matched by payload bytes, first match).
* Rejects FAILED_PRECONDITION if the connection is closed.
* More than `maxPendingPings` (1024) outstanding rejects the oldest with DEADLINE_EXCEEDED.
* At shutdown, all pending pings reject with the connection's status.

**`goAway(status = GOING_AWAY)`:**
* Application codes throw ArgumentError (sync).
* It sends GOAWAY once; a second call doesn't resend. CONNECTION_LOST goes on the wire as UNAVAILABLE.
* New peer OPENs are then refused with GOING_AWAY, except `_bulk` for open parents.
* It waits until no channels are open or `goAwayGrace` (10 s) passes, then `close()`. The promise never rejects.

**`close()`:**
* Immediate. All channels end with the lost status, and no CLOSE frames are sent.
* It flushes held frames, closes the sink, and cancels the subscription.
* If that doesn't finish within `keepAliveTimeout`, it aborts (`AbortableTransport`).
* Idempotent; never rejects.

**After the peer's GOAWAY:**
* `_checkIdle` closes the connection when no channels are open, so the side shutting down doesn't wait its grace period.
* `open()` throws FAILED_PRECONDITION after either side's GOAWAY ("connection is closing" / "peer is going away"). Only `openAfter` continuations (bulk channels for open parents) are still allowed.

### 12.3 End statuses

| How it ended | `connection.done` | Each open channel's `done` |
| --- | --- | --- |
| We detected a protocol violation (`_fail`): PROTOCOL_ERROR, FRAME_TOO_LARGE, UNSUPPORTED, RESOURCE_EXHAUSTED (too many rejections), UNAUTHENTICATED (IDENT), INTERNAL (bug while handling a frame) | that status (also sent as GOAWAY) | the lost status (below) |
| Transport error carrying PROTOCOL_ERROR, FRAME_TOO_LARGE or UNSUPPORTED (e.g. from the stream decoder) | same as above: we send GOAWAY with it | lost status |
| Any other transport error, transport done, keep-alive timeout, `close()` | lost status | lost status |

**Lost status** (`_lostStatus`):
1. The peer's GOAWAY. If its code was GOING_AWAY, the peer's status itself. Otherwise GOING_AWAY with the peer's status as text in the reason.
2. Else our own graceful `goAway` status.
3. Else CONNECTION_LOST.

Dart byte-level tests check each protocol-error case:
* exactly one GOAWAY `02 00 00 03 20 00 …` is written, with PROTOCOL_ERROR and a reason;
* nothing more is written, not even a PONG for a later PING in the same batch;
* the transport closes;
* `done` is PROTOCOL_ERROR;
* locally opened channels end CONNECTION_LOST.

---

## 13. Error semantics

### 13.1 Connection-level (GOAWAY with the status, then close; stop processing input)

| Trigger | Status |
| --- | --- |
| Any mux header decode error (§5.1) | PROTOCOL_ERROR |
| Frame longer than our `maxFrameSize` | FRAME_TOO_LARGE |
| Long-form id with `shortIdsOnly` | UNSUPPORTED |
| OPEN with wrong parity, or on an id in use, rejected or awaiting close | PROTOCOL_ERROR |
| DATA or CLOSE on an id that isn't open (or after the peer's CLOSE) | PROTOCOL_ERROR |
| DATA beyond the receive window, open or awaiting close | PROTOCOL_ERROR |
| CREDIT pushing a window past 2^32 − 1 | PROTOCOL_ERROR |
| Malformed CLOSE or GOAWAY status (1 byte) | PROTOCOL_ERROR |
| Empty control message; control payload > 1024; bad LIMITS; bad CREDIT length | PROTOCOL_ERROR |
| NONCE wrong length, a second one, or equal to ours | PROTOCOL_ERROR |
| IDENT before both nonces, truncated, or intent > 64 | PROTOCOL_ERROR |
| IDENT failing verification or the receiver check | UNAUTHENTICATED ("identification failed") |
| More than `maxPendingRejections` unconfirmed rejected ids | RESOURCE_EXHAUSTED |
| Stream binding: bad magic or length 0 | PROTOCOL_ERROR |
| Stream binding: wrong version | UNSUPPORTED |
| Stream binding: length > limit | FRAME_TOO_LARGE |
| WebSocket: text message | protocol error (WS close 1003 on the server) |
| Unexpected exception while handling a frame | INTERNAL |

### 13.2 Channel-level (CLOSE with PROTOCOL_ERROR or another status; the connection continues)

| Trigger | Action |
| --- | --- |
| Malformed address header in OPEN (reserved bits `0xF0`, truncation, bad host) | Dispatcher closes CLOSE PROTOCOL_ERROR |
| Any Talk header or kind-rule violation (§16) | Talk closes the channel CLOSE PROTOCOL_ERROR |
| Rejections (§9.4) | CLOSE GOING_AWAY, UNAVAILABLE or RESOURCE_EXHAUSTED |
| `_bulk` OPEN naming no open parent | FAILED_PRECONDITION |
| `_bulk` OPEN with too many unclaimed | RESOURCE_EXHAUSTED |

### 13.3 Local API errors (nothing sent)

**Synchronous throws** in Dart:

| Error | Raised by |
| --- | --- |
| ArgumentError | bad values: app code in CLOSE or GOAWAY, PING > 125, negative or too-large numbers, `grant` overflow |
| StateError | `consumed` beyond held, accessors on the wrong frame kind |
| SwitchboardException FAILED_PRECONDITION | `send` when `!canSend` |
| SwitchboardException FRAME_TOO_LARGE | `send` oversize |
| SwitchboardException RESOURCE_EXHAUSTED | `open` with no free id, or with the peer's announced maxChannels reached (Dart enforces the advisory cap locally) |
| SwitchboardException FAILED_PRECONDITION | `open` while closing or after either side's GOAWAY (except `openAfter` continuations) |
| SwitchboardException FRAME_TOO_LARGE | `open` whose OPEN frame exceeds the peer's frame limit |

**Asynchronous failures:**
* `send` futures reject FAILED_PRECONDITION when the channel ends first.
* `ping()` rejects as in §12.2.
* `identify()` rejects UNAUTHENTICATED, DEADLINE_EXCEEDED, or the connection status.

**A JS decision for the lead:** whether sync-throw cases stay sync throws, or become rejected promises from async functions. Dart deliberately throws synchronously for programmer errors and rejects only for runtime conditions. Keeping that split is the most faithful, but it means `send()` can't be a plain `async function`.

---

## 14. Resource limits and defaults (`MuxOptions`)

| Option | Default | Meaning |
| --- | --- | --- |
| `maxFrameSize` | 1 MiB (`1048576`) | Largest incoming frame, header included. Announced; 0 = no limit at the mux. |
| `maxChannels` | 65536 | Open-channel cap and, separately, undelivered-backlog cap. Announced; 0 = unlimited. |
| `shortIdsOnly` | false | Embedded style: short ids only, and refuse long-form frames with GOAWAY UNSUPPORTED. |
| `keepAliveInterval` | 10 s | null disables keep-alive |
| `keepAliveTimeout` | 10 s | Probe timeout; also the transport close timeout and the LIMITS wait bound |
| `goAwayGrace` | 10 s | How long `goAway` waits for channels |
| `announceLimits` | true | Send LIMITS first |
| `maxPendingRejections` | 1024 | Unconfirmed rejected or timed-out ids; 0 = unlimited |
| `receiveHighWaterMarkBytes` | 16 MiB | Pause reading above; resume at half; 0 = never |
| `closeConfirmTimeout` | 30 s | 0 = forever |
| `maxOpenPayloadBytes` | 16 MiB | Effective min(this, mark / 2) = 8 MiB with the defaults; 0 = only mark / 2 |
| `identityVerifier` | null | null ignores IDENTs |
| `identityTimeout` | 10 s | `identify()` bound; 0 = unbounded |
| `requireNamedIdent` | false | Refuse IDENT with an empty receiver |
| `initialWindow` | 64 KiB | 1 … 2^32 − 1; announced |
| `bulkZipper` | 4 | ≥ 1 |
| `bulkChunkSize` | 64 KiB | ≥ 1 |
| `awaitPeerLimits` | true | §7.2 |

Constants in other files:

| Constant | Value |
| --- | --- |
| `maxPendingPings` | 1024 |
| `maxControlPayloadLength` | 1024 |
| `MuxControlMessage.maxPingPayload` | 125 |
| `MuxLimits.defaultInitialWindow` | 65536 |
| `MuxLimits.maxWindow` | 0xFFFFFFFF |
| `MuxCredit.frameOverhead` | 16 |
| Stream transport `maxFrameSize` | 1 MiB |
| `outputHighWaterMark` | 16 MiB |
| `closeTimeout` | 10 s |
| Output coalescing | 64 KiB |
| WebSocket server `maxMessageFragments` | 4096 |
| Talk-level limits | see §16 |

---

## 15. The binding seam (for the QUIC thread and the lead)

`MuxConnection` (abstract interface in Dart: the frame mux *is* a `MuxConnection`, and so is a `BindingConnection`) and `MuxChannel` (`mux_channel.dart`) are what Talk, addressing, the Switchboard dispatcher and proxies use. QUIC/WebTransport implements them directly, with its own records and streams, and **doesn't** use mux frames, CREDIT or the output scheduler.

**`MuxChannel` members:**

| Group | Members |
| --- | --- |
| Identity | `connection`, `id`, `isLocallyOpened`, `openPayload` |
| State | `state`, `canSend` |
| Priority | `priority` (ordinary or bulk; settable) |
| Receive | `bufferedBytes`, `stream` (incoming subframes), `hasListener`, `receiveWindow`, `manualCredit`, `heldBytes`, `consumed(bytes, {subframes})`, `grant(bytes)` |
| Send | `sink`, `sendWindow`, `sendCost(length)`, `maxSubframeLength`, `send(subframe)`, `isWritable`, `whenWritable` |
| Lifecycle | `done` (a Future of the end status), `close([status])` |
| Ordering | `openAfter` |
| LIMITS | `awaitingLimits`, `limitsKnown` |

**`MuxConnection` members** used above the mux:
* `incoming`, `open(payload, {after, priority, manualCredit})`, `done`, `goAway`, `close`, `ping`;
* `isInitiator`;
* `identify`, `peerIdentity`, `peerIdentified`, `identityRequested`, `localIdentity`;
* `bufferedBytes`, `options`;
* `hasOrderedChannels`.

**`BindingConnection`** (`binding_connection.dart`):
* An abstract base implementing `MuxConnection` with neutral diagnostics.
* `hasOrderedChannels = false`: streams are independent, so a `_bulk` OPEN may arrive before its parent's or after it ended. `BulkRoutes` then holds or lingers per the QUIC page.
* **Drain holds:** `retainForDrain`, `hasDrainHolds`, `retainedForDrain`, `drainStateChanged`. They let a QUIC connection keep itself alive during GOAWAY while bulk transfers finish.

**Layering leak to fix in TS.** `MuxConnection._continuesOpenChannel` decodes the *address header* to spot `_bulk` OPENs after GOAWAY, so the mux depends on addressing. A cleaner TS seam is a hook, `admitAfterGoAway(openPayload) → boolean`, installed by the layer that knows about `_bulk`. The wire behaviour stays the same.

---

## 16. Talk: wire digest and behaviours (wiki `talk.md`, Dart `talk/talk_frame.dart`, `talk_bulk.dart`, `talk_channel.dart`)

Talk is the message layer on one mux channel. **One Talk message is one DATA subframe**, or one QUIC record. Nothing frames Talk messages inside a subframe.

### 16.1 Header

```
u8 flags
[8B procedure]        if HAS_PROCEDURE 0x01
[u24 request id]      if HAS_REQUEST  0x02   (0 invalid; max 0xFFFFFF)
[u24 response id]     if HAS_RESPONSE 0x04   (0 invalid)
[u32 item window]     if STREAM       0x08   (0..0xFFFFFFFE finite, 0xFFFFFFFF unlimited)
payload               rest; with BULK 0x40 exactly a 10-byte bulk reference
flags: KIND = (flags & 0x30) >> 4: 0 MESSAGE, 1 STREAM_ITEM, 2 ABORT, 3 EXTEND; 0x80 reserved
```

The request id comes before the response id, and the window comes after both.

### 16.2 Kind rules

Any violation is a **channel** protocol error. `_ruleViolation` and `_streamViolation` are shared by encode and decode, so the encoder refuses what a receiver would reject.

| Kind | Ids | Meaning | Rules |
| --- | --- | --- | --- |
| MESSAGE | none | plain message | needs a procedure |
| MESSAGE | request | request | needs a procedure |
| MESSAGE | response | final response; end of stream for a STREAM request (may carry a trailer) | procedure optional |
| MESSAGE | both | chained response that is also a request | procedure optional |
| STREAM_ITEM | response (+ optional request) | non-final reply to a STREAM request | HAS_RESPONSE required |
| ABORT | response | abort response | — |
| ABORT | request | cancel own request | — |
| ABORT | none | channel abort | — |
| ABORT | both | — | invalid |
| EXTEND | response | extend | payload 0 or 8 bytes: `u32 deadline ms, u32 renew ms` (0 means unchanged); empty restarts the default timeout |
| EXTEND | request | **credit** for own STREAM request | payload exactly 4 bytes, `u32 items ≥ 1` |
| EXTEND | none or both | — | invalid |

Rules that apply across kinds:
* **STREAM** needs HAS_REQUEST, and isn't allowed on ABORT or EXTEND. The window field is read only with STREAM. Without STREAM the frame's window is the default (unlimited) and isn't encoded; encoding a finite window without STREAM is an ArgumentError.
* **BULK** isn't allowed on ABORT or EXTEND. Its reference must be 10 bytes: `u32 number ≥ 1, u48 length` (0xFFFFFFFFFFFF means unknown).
* **ABORT payload.** Empty means UNKNOWN, 1 byte is invalid, OK is read as UNKNOWN. Application codes (≥256) are allowed here and only here.
* **Procedure** decoding follows the Name rules, so a byte after the zero padding is invalid.
* **Payload view.** The decoded payload is a view into the input (`subarray`), not a copy. A test relies on this.

**`extendPayload(deadline, renew)`** rounds up to whole ms, sends values under 1 ms as 1 (0 means unchanged), and clamps to 0xFFFFFFFF. Only a negative value throws. **`creditPayload(items)`** accepts 1…0xFFFFFFFF.

### 16.3 Behaviour (see wiki for the full text; Dart `TalkOptions` defaults)

**Ordering.** Within a channel, everything is delivered in send order. Responses keep their position among plain messages and requests, and a BULK message is delivered at the position of its BULK frame. Dart offers `ordered:` request answers so the application can observe that order.
* **JS hazard.** Dart achieves this with per-microtask stream delivery. A TS design has to decide explicitly how responses (promises) interleave with the message stream (async iterator). Either deliver both through one ordered queue, or document that promise resolution order alone isn't enough.

**Timeouts.**

| Setting | Default |
| --- | --- |
| Requester gap | 15 s |
| Responder gap | 10 s |
| Extend buffer | 5 s |
| Floor (`minExtension`) | 1 s |
| Clamp (`maxExtension`) | 1 h |
| `creditTimeout` | 60 s |

* The declared regime fires at `max(extendPoint + deadline, lastReply + renew)`.
* While a stream's item window is exhausted, neither side's timeout runs.
* **JS hazard.** Delays over 2^31−1 ms overflow `setTimeout`. Dart re-arms in chunks (`_maxTimerDelay = 0x7FFFFFFF` ms) and so must TS: the 1 h clamp can be disabled with `Duration.zero`, which lets a peer-declared deadline reach u32 ms (~49.7 days).

**Limits.**

| Option | Default | Effect |
| --- | --- | --- |
| `maxIncomingRequests` | 1024 | beyond it, `ABORT RESOURCE_EXHAUSTED` at once |
| `maxOutgoingRequests` | 1024 | beyond it, a local RESOURCE_EXHAUSTED; nothing is sent |

* Unknown procedure on a request: `ABORT UNIMPLEMENTED`. On a plain message: ignored.
* A response to an unknown id is ignored, unless it carries HAS_REQUEST; then `ABORT NOT_FOUND` goes to that request.
* A new request reusing an id we still consider outstanding is not an error. The old request is abandoned and the new one served.
* Requesters reuse ids only after 2^24 requests.

**Item windows** (STREAM requests):
* The responder counts every STREAM_ITEM. An item beyond the window is a channel protocol error on the responder's part, so the requester closes the channel PROTOCOL_ERROR.
* The final response, ABORT and EXTEND are never counted.
* Credit is protocol-checked: a credit of 0, a credit for an unlimited window, or one taking the remaining window to 0xFFFFFFFF or beyond is a channel protocol error.
* A credit for an id that isn't outstanding is ignored. A credit for a non-stream request is a channel protocol error.

**Flow control.** Talk uses mux `manualCredit`: credit is returned when the *application* takes a message or item, not when Talk reads the frame. Frames Talk consumes itself are credited at once: completed responses, aborts, EXTEND, and frames for ids it gave up on.

**Bulk payloads.**

| Option | Default | Meaning |
| --- | --- | --- |
| `bulkThreshold` | channel `maxSubframeLength` | above this, send as bulk; a message over the inline limit goes bulk transparently whatever its kind |
| `bulkChunkSize` | 64 KiB | |
| `maxInlinePayload` | 16 MiB | reassembly cap |
| `reassemblyBudget` | 16 MiB | per channel |
| `bulkWindow` | 1 MiB | grant for a declared length |
| `maxUnclaimedBulk` | 16 | |
| `bulkOpenTimeout` | 10 s | |
| `bulkIdleTimeout` | 30 s | |

* The bulk channel is an ordinary mux channel. It is opened with `openAfter(parent)` and marked bulk priority, and its OPEN payload is the `_bulk` address header (§17).
* Data flows in chunks, and CLOSE OK ends the transfer. A length mismatch is PROTOCOL_ERROR for the payload, not the channel.
* The receiver modes are reassembled (the default) or streamed (`streamBulk`).

**Channel protocol errors** close only the channel, with CLOSE PROTOCOL_ERROR.

**Message chain proxying** rewrites only the ids and passes everything else through, EXTEND payloads included.

---

## 17. Address header and reserved types (wiki `addressing.md`, Dart `channel_address.dart`)

The OPEN payload starts with an address header. The mux itself doesn't decode it, apart from the `_bulk` leak in §15.

```
u8 flags: HAS_SERVICE 0x01, HAS_INSTANCE 0x02, HAS_SHARD 0x04, HAS_HOST 0x08; 0xF0 reserved
[8B service type]     if HAS_SERVICE
[u48 instance]        if HAS_INSTANCE
[u32 shard]           if HAS_SHARD
[len8 host hint]      if HAS_HOST
application payload   rest
```

**Decoding.**
* An empty OPEN payload (zero bytes) means the default service with no payload, and re-encodes as `00`.
* Reserved bits, truncation, or a malformed host are channel errors: CLOSE PROTOCOL_ERROR.

**Encoding.**
* HAS_INSTANCE is set only when the instance is non-zero.
* HAS_SHARD is set when a shard is given, 0 included.
* The application payload is copied.

**Reserved types:**

| Type | Payload | Rules |
| --- | --- | --- |
| `_ns` | — | the naming service |
| `_bulk` | `u48 parent channel id, u32 bulk number ≥ 1`; trailing bytes ignored; shorter is PE | instance 0, no shard |
| `_relay` | the complete inner open payload | |

---

## 18. Test vector catalogue

Port all of wiki `test-vectors.md` as table-driven tests; it is the conformance reference. This section reproduces the core rows, then adds every byte-exact case from the Dart tests that the wiki lacks. The naming, sharding and credential rows are in the wiki only, plus Appendix A. The hex format is uppercase and space-separated, as in the Dart `hexString`.

### 18.1 Stream binding

| Case | Bytes | Expected |
| --- | --- | --- |
| Preamble v1 | `53 57 42 44 01 00 00 00` | wiki |
| Frame with body `02 02 00 AA` | `04 00 00 00 02 02 00 AA` | wiki |
| Preamble flags ignored | `53 57 42 44 01 00 FF FF 01 00 00 00 02` | one frame `02` |
| Wrong magic | `53 57 42 45 01 00 00 00` | ProtocolException |
| Version 2 | `53 57 42 44 02 00 00 00` | SwitchboardException UNSUPPORTED, *not* ProtocolException |
| Length 0 | preamble + `00 00 00 00` | ProtocolException, even with maxFrameSize 0 |
| Limit 4, length 5 | preamble + `05 00 00 00` (no body) | FRAME_TOO_LARGE before the body |
| Length 2^31 | preamble + `00 00 00 80` | FRAME_TOO_LARGE (must not read negative) |
| Length 2^32−1 | preamble + `FF FF FF FF` | FRAME_TOO_LARGE |
| Frames before an error | preamble + `01 00 00 00 02` + `00 00 00 00` | frame `02` delivered, then ProtocolException |
| Embedded OPEN framing | `0C 00 00 00 12 02 00 01 64 65 76 00 00 00 00 00` | frame `12 02 00 01 64 65 76 00 00 00 00 00` |
| Over the stream transport limit (mux test) | limit 64; preamble + `00 10 00 00` | the mux writes exactly one frame: GOAWAY FRAME_TOO_LARGE; `done` FRAME_TOO_LARGE |

### 18.2 Mux frames

| Case | Bytes | Decoded |
| --- | --- | --- |
| DATA short 2 `01 02 03` | `02 02 00 01 02 03` | DATA ch 2 |
| DATA short 2 empty | `02 02 00` | |
| DATA long 0x010000 empty | `00 00 00 01 00 00 00` | |
| DATA long 0x1234567890AB `FF` | `00 AB 90 78 56 34 12 FF` | |
| DATA 0xFFFE (largest short even) | `02 FE FF` | |
| DATA id 2 in long form `AA` | `00 02 00 00 00 00 00 AA` | ch 2, longForm true |
| DATA id 0x100000002 | `00 02 00 00 00 01 00` | |
| DATA id 0xFFFF00000002 | `00 02 00 00 00 FF FF` | |
| DATA id 0xFFFFFFFFFFFE | `00 FE FF FF FF FF FF` | |
| DATA id 0x80000000 | `00 00 00 00 80 00 00` | (sign bit of a JS int32) |
| OPEN short 3 `AA` | `12 03 00 AA` | |
| OPEN short 2 empty | `12 02 00` | |
| OPEN long 0x10000 `AA` | `10 00 00 01 00 00 00 AA` | the first long id, after 0x7FFF short ids |
| CLOSE OK explicit | `22 02 00 00 00` | OK |
| CLOSE empty | `22 02 00` | OK |
| CLOSE NOT_FOUND "x" | `22 02 00 05 00 78` | |
| Compatible bits | `42 02 00 01` | DATA ch 2, payload `01` |
| Compatible bits | `82 02 00` | DATA ch 2, empty |
| Compatible bits | `D2 03 00` | OPEN ch 3 |

**Negatives** (ProtocolException):

| Bytes | Why |
| --- | --- |
| `03 02 00` | flag 0x01 |
| `06 02 00` | flag 0x04 |
| `0A 02 00` | flag 0x08 |
| `32 02 00` | command 3 |
| `02 02` | truncated short header |
| `00 01 02 03 04 05` | truncated long header |
| `12 00 00` | OPEN on 0 |
| `22 00 00` | CLOSE on 0 |
| `02 01 00` | id 1 |
| `12 01 00` | OPEN on id 1 |
| `02 FF FF` | id 0xFFFF |
| `00 00 00 00 00 FF FF` | id 0xFFFF00000000 |
| `00 FF FF FF FF FF FF` | id 0xFFFFFFFFFFFF |
| `00 00 00 00 00 FF FF 01 DE AD` | reserved id even with a control-looking payload |
| (empty) | empty frame |

**Encoder checks:**
* `costOf(0)` is 16 and `costOf(100)` is 116.
* An id ≥ 2^48 throws ArgumentError.

### 18.3 Control messages

| Case | Bytes (whole frame) |
| --- | --- |
| PING `DE AD` | `02 00 00 01 DE AD` |
| PONG `DE AD` | `02 00 00 02 DE AD` |
| GOAWAY GOING_AWAY | `02 00 00 03 21 00` |
| LIMITS 1024 / 4 / 64 KiB | `02 00 00 04 00 04 00 00 04 00 00 00 00 00 01 00` |
| LIMITS 4096 / 7 / 64 KiB (first frame of a mux with those options) | `02 00 00 04 00 10 00 00 07 00 00 00 00 00 01 00` |
| CREDIT ch 2, 32768 | `02 00 00 07 02 00 00 00 00 00 00 80 00 00` |
| NONCE 32×`11` | `02 00 00 05` + 32×`11` |
| IDENT C1 bearer | `02 00 00 06 56 00` + C1 (86 B) + `00 00` |
| IDENT C2, intent, receiver, P | `02 00 00 06 88 00` + C2 (136 B) + `04 01 02 03 04 08 63 6F 6E 73 75 6D 65 72` + P (64 B) |

**Payload-level** (after the type byte):

| Case | Payload | Result |
| --- | --- | --- |
| LIMITS, trailing fields | `00 04 00 00 04 00 00 00 00 00 01 00 99 99` | 1024 / 4 / 65536 |
| LIMITS, full u32 | `FF FF FF FF 00 00 00 80 FF FF FF FF` | 0xFFFFFFFF / 0x80000000 / 0xFFFFFFFF |
| CREDIT, maxima | `FE FF FF FF FF FF FF FF FF FF` | ch 0xFFFFFFFFFFFE, 0xFFFFFFFF bytes |
| Unknown type | `7F 01` (whole control message) | decodes; type 0x7F, payload `01` |

**Negatives** (PE):
* LIMITS 8 bytes: `00 04 00 00 04 00 00 00`
* LIMITS 7 bytes: `00 04 00 00 04 00 00`
* LIMITS window 0: `00 04 00 00 04 00 00 00 00 00 00 00`
* CREDIT 8 bytes: `02 00 00 00 00 00 00 80`
* CREDIT 11 bytes
* Empty control message (no type)
* Control payload of 1025 bytes (1024 is fine)

**Encode refusals** (ArgumentError):
* LIMITS window 0 or 0x100000000, or a negative field
* CREDIT id 0x1000000000000, bytes 0x100000000, or a negative value
* PING payload over 125 bytes
* NONCE not 32 bytes

**IDENT payload negatives** (PE): `56`, `56 00 01 01`, `02 00 AA BB`, `00 00 05 01 02`, `00 00 41` + 65×`00`, `00 00 01 AA`, `00 00 00 04 61 62`.

### 18.4 Connection-level sequences (byte-exact Dart tests)

Set-up for these: `rawPair()` is a mux under test against a raw peer over MemoryTransport, using `rawOptions`: no keep-alive, `announceLimits: false`, `awaitPeerLimits: false`. The mux is the **initiator** unless stated otherwise. "→" is a frame the mux writes; "←" a frame the raw peer sends.

**Channel lifetime** (`mux_test` › wire bytes of a channel lifetime; the mux is the **acceptor**):
* We open and close a channel:
  * `open(AA)` → `12 03 00 AA`
  * `send(01 02 03)` → `02 03 00 01 02 03`
  * `close(NOT_FOUND "x")` → `22 03 00 05 00 78`
  * ← `22 03 00`, and our `done` is NOT_FOUND "x"
* The peer opens and closes a channel:
  * ← `12 02 00 BB`, then ← `02 02 00 07` (delivered `07`)
  * ← `22 02 00 05 00 78`
  * → `22 02 00` (empty confirm), and the channel's `done` is NOT_FOUND "x"
* Empty open and close:
  * `open(empty)` → `12 05 00`
  * `close()` → `22 05 00`
  * ← `22 05 00 00 00`, and `done` is OK

**Protocol-error cases** (`mux_test` › protocol errors).
* Each case starts with `open(empty)` → `12 02 00`.
* The peer then sends the frames below, followed by PING `02 00 00 01 AA`.
* Expected: exactly one GOAWAY PROTOCOL_ERROR, and it is the last frame written. No PONG. Transport closed. `done` PROTOCOL_ERROR. The local channel's `done` is CONNECTION_LOST.

| Case | Frames |
| --- | --- |
| DATA on an unknown channel | `02 03 00 01` |
| OPEN on an id in use | `12 03 00`, `12 03 00` |
| OPEN with the wrong parity | `12 02 00` |
| command 3 | `32 02 00` |
| OPEN on 0 | `12 00 00` |
| CLOSE on 0 | `22 00 00` |
| DATA on id 1 | `02 01 00` |
| DATA on 0xFFFF | `02 FF FF` |
| reserved flag | `03 02 00` |
| truncated header | `02 03` |
| CLOSE on an unknown channel | `22 03 00` |
| DATA after the peer's CLOSE | `12 03 00`, `22 03 00`, `02 03 00` |
| CLOSE twice | `12 03 00`, `22 03 00`, `22 03 00` |
| malformed CLOSE status | `12 03 00`, `22 03 00 05` |
| control without type | `02 00 00` |
| malformed GOAWAY | `02 00 00 03 21` |
| malformed LIMITS | `02 00 00 04 00 01` |

Further cases from the same file:
* **Control payload of 1025 bytes:** PING `02 00 00 01` + 1025×`00` → GOAWAY PROTOCOL_ERROR.
* **OPEN on a locally half-closed id:**
  * ← `12 03 00`; we close → `22 03 00`
  * ← `02 03 00 01`: this is fine, DATA after our CLOSE
  * ← `12 03 00` → GOAWAY PROTOCOL_ERROR. The channel's `done` is CONNECTION_LOST.
* **Frame over maxFrameSize 16:** ← `12 03 00`, ← `02 03 00` + 14×`00` (17 bytes) → GOAWAY FRAME_TOO_LARGE; `done` FRAME_TOO_LARGE.
* **`shortIdsOnly` with a long-form frame:** ← `10 03 00 00 00 00 00` → GOAWAY UNSUPPORTED; `done` UNSUPPORTED.
* **Long form addresses a short-opened channel:**
  * ← `12 03 00`, ← `00 03 00 00 00 00 00 77` (delivers `77`)
  * ← `20 03 00 00 00 00 00` → `22 03 00`, and `done` is OK
* **Long id coexisting with id 3** (`mux_vectors_test`):
  * ← OPEN `10 03 00 00 00 01 00` (id 0x100000003), ← DATA `00 03 00 00 00 01 00 AA`
  * our close → `20 03 00 00 00 01 00`
  * id 3 is separate
* **CREDIT for a channel that isn't open is ignored:** ← `02 00 00 07 02 00 00 00 00 00 00 80 00 00` (CREDIT ch 2, nothing open), then ← PING `AA` → `02 00 00 02 AA`.
* **Compatible bits on a live connection:** ← `52 03 00` (OPEN 3), ← `82 03 00 01` (DATA `01`), ← `C2 00 00 01 DE AD` (PING) → PONG `02 00 00 02 DE AD`.
* **PING answering:**
  * ← `02 00 00 01 DE AD` → `02 00 00 02 DE AD`
  * an unsolicited PONG `02 00 00 02 01` and an unknown type `02 00 00 7F 01 02` are ignored
  * an empty PING `02 00 00 01` → `02 00 00 02`
  * PINGs of 126…1024 bytes are answered
* **`ping(BE EF)`** → `02 00 00 01 BE EF`. A PONG `00` doesn't match; PONG `BE EF` resolves it.
* **Second GOAWAY ignored:**
  * ← `12 03 00`, ← `02 00 00 03 21 00`, ← `02 00 00 03 05 00`, ← PING `01` → PONG `01`
  * `peerGoingAway` is true
  * ← `22 03 00` → `22 03 00`, then the connection closes: idle after GOAWAY
  * channel `done` OK, mux `done` GOING_AWAY
* **GOAWAY with another status:** ← `02 00 00 03 20 00 78`. `done` is GOING_AWAY, with the reason containing "x".
* **`goAway()` with no channels:** → `02 00 00 03 21 00`, then close.
* **LIMITS mid-connection** (initiator, rawPair):
  * `open(empty)` → `12 02 00`; send(200) goes out
  * ← LIMITS(64, 1), settled by ← PING `5E` → PONG `5E`
  * `send(62)` throws FRAME_TOO_LARGE; `send(61)` writes a 64-byte frame
  * `open()` throws RESOURCE_EXHAUSTED: the peer said 1 channel, and Dart enforces the advisory cap locally
  * `close(CANCELLED, 'z'×100)` writes a 64-byte CLOSE carrying `'z'×59`
  * after ← LIMITS(4096, 8): a 1000-byte OPEN and a 4000-byte send go through; the next channel is id 4, so → `22 04 00`
  * after ← LIMITS(32, 8): GOAWAY 'w'×100 is cut to 'w'×26

**Application codes from the peer** (Appendix A.12):
* ← CLOSE `22 02 00 2C 01 61` → confirm `22 02 00`. The channel's `done` is UNKNOWN "code 300: a".
* ← GOAWAY `02 00 00 03 2C 01`. The mux `done` is GOING_AWAY "unknown: code 300".

**Flow** (`mux_flow_test`):
* A rejected or closed id with the default 64 KiB window accepts two DATA frames of 32752 bytes of payload each (cost 2 × 32768); a third, even empty, is a protocol error.
* `grant` inside an automatic listener counts the delivered frame. An ArgumentError fires at maxWindow.

### 18.5 Talk, address, identity, naming, sharding and status

* Talk positives and negatives: wiki "Talk messages", plus Appendix A.1.
* Address headers: wiki "Address headers", plus Appendix A.5.
* Credentials C1, C2 and P, invalid credentials, and scope patterns: wiki "Credentials", plus Appendix A.6 and A.7.
* Status codec and truncation: Appendix A.2 and A.12.
* Bytes and Name: Appendix A.3 and A.4, plus wiki "Names".
* Naming, sharding and slot keys (FNV-1a 32): wiki, plus Appendix A.9–A.11. These are outside the core but are byte vectors the port must hit.

The **embedded round trip** is from `test/integration/embedded_test.dart` and wiki `embedded-profile.md`. Every frame below is stream-binding framed.

| Step | Bytes |
| --- | --- |
| Preamble, each side | `53 57 42 44 01 00 00 00` |
| OPEN | `0C 00 00 00 12 02 00 01 64 65 76 00 00 00 00 00` (service "dev") |
| Request 1 "PING" | `0F 00 00 00 02 02 00 03 50 49 4E 47 00 00 00 00 01 00 00` |
| Reply | `07 00 00 00 02 02 00 04 01 00 00` |
| CLOSE | `03 00 00 00 22 02 00` |

---

## 19. Dart tests to port (inventory)

The counts are `test(` call sites; parameterized loops expand to more tests.

| File | Lines | Tests | Groups / what it covers | Needs |
| --- | --- | --- | --- | --- |
| `vectors/transport_vectors_test` | 161 | 11 | stream binding vectors (§18.1) | pure |
| `vectors/mux_vectors_test` | 482 | 20 | frame and control codec plus connection-level negatives (§18.2–18.4) | pure plus raw peer |
| `vectors/talk_vectors_test` | 705 | 16 | Talk codec | pure |
| `vectors/address_vectors_test` | 197 | 5 | address codec, `_bulk` | pure |
| `vectors/identity_vectors_test` | 437 | 14 | credentials, IDENT, proof | crypto |
| `vectors/exporter_identity_vectors_test` | 150 | 8 | SWBIDTLS (QUIC) | crypto |
| `vectors/binding_protocol_test` | 141 | 3 | IDENT gating; status helpers | raw peer |
| `bytes_test`, `status_test`, `name_test` | 73 / 92 / 54 | 4 / 6 / 5 | primitives | pure |
| `address_test` | 206 | 17 | ServiceAddress, ChannelAddress | pure |
| `mux_test` | 1610 | 74 | channels; rejection and limits; channel ids; protocol errors; control channel; GOAWAY; byte counters; keep-alive; connection end; transport failures end to end; WebSocket; real transports | MemoryTransport, raw peer, fake time, TCP, WS |
| `mux_flow_test` | 1019 | 35 | send window; readiness; DATA waits for the peer LIMITS; receive window; between two connections | raw peer, fake time |
| `mux_limits_test` | 1068 | 34 | rejected OPENs; undelivered incoming channels; receive buffers; connection high-water mark; output backpressure over TCP; reasons; CONNECTION_LOST stays local; application codes from the peer; close confirmation timeout; open payload budget | raw peer, fake time, TCP |
| `mux_schedule_test` | 872 | 28 | output scheduler; bulk channels; openAfter | GatedTransport |
| `transport_test` | 894 | 41 | MemoryTransport; StreamTransport over byte pipes and TCP; WebSocketTransport (two flavours) | Node `net`, `ws` |
| `web_socket_server_test` | 1033 | 31 | handshake; frames; limits and protocol errors; ping interval; interoperability; listener | `ws` / http |
| `identity_test` | 953 | 36 | issuer; verifier; connection identity; receiver binding; protocol errors with and without a verifier | crypto, fake time |
| `binding_seam_test`, `binding_bulk_test`, `binding_drain_test`, `binding_review_test` | 665 / 580 / 90 / 710 | 15 / 19 / 3 / 22 | the seam over `fake_binding.dart` (1435 lines, an unordered binding) | QUIC thread / seam |
| `talk_test` | 3678 | 123 | Talk runtime | fake time |
| `talk_bulk_test`, `talk_forward_test`, `talk_window_test`, `talk_credit_test` | 1960 / 1893 / 1575 / 561 | 60 / 49 / 41 / 18 | bulk, forwarding, item windows, awaitable sends | fake time |
| `mux_talk_test` | 104 | 4 | Talk over mux smoke | |

**Harness pieces to port first** (`test/mux_harness.dart`):
* `quiet`: no keep-alive, goAwayGrace 2 s.
* `rawOptions`.
* `muxPair`.
* `RawPeer`, built on `rawPair`: `send(hex)`, `nextHex()`, `next()`, `nextControl(type)` and `rest()`.
* `GatedTransport` / `gatedPair`: an OutputReadyTransport released by `allow(n)`.

Also needed: an injectable clock and timers (§20.4) and a hex helper (§2).

The Dart node preset (`dart test -P node`) already runs the vectors, the bytes, identity, name and status tests, the reconnecting client and the Talk window tests compiled to JS. Those are the cases that were known to be sensitive to JS number arithmetic.

---

## 20. JS/TS porting hazards and Dart API awkwardness

These are inputs for the lead's API design. Recommendations are marked **rec**; the API decisions belong to the lead.

### 20.1 Numbers
* No bitwise operators on u32/u48 values (§2). Watch `flags & 0x30` style code: fine on bytes, wrong on ids.
* The Dart `id.isOdd` translates to `id % 2 === 1`, not `id & 1`. `&` happens to work below 2^31 only.
* **rec:** number ids and an `assertU48` helper. Map keys work as numbers.
* Names need a primitive key: **rec** a 16-hex string or a bigint from the 8 bytes, with a `Name` class exposing `key`.

### 20.2 Bytes
* Dart `Uint8List.sublistView` maps to `subarray`, a view. Dart `sublist` / `fromList` map to `slice`, a copy.
* Keep the Dart semantics:
  * Talk and mux decode return **views**. A test relies on this.
  * Stream framing **copies** frames out of its buffer.
  * The OPEN payload is **copied**.
  * `send` copies on hand-over.
* Node `Buffer` is a `Uint8Array` subclass, but `Buffer.slice` is a view (deprecated). Normalize to `Uint8Array` at the transport boundary. Node `net` delivers `Buffer` chunks from a pooled allocator, so copy anything retained.

### 20.3 Async and errors
* `Completer` maps to `Promise.withResolvers()` (Node 22 and browsers ok) or a small Deferred.
* Dart `.ignore()` / "never reported as unhandled" maps to attaching `p.catch(() => {})` to internal promises while still returning `p`.
  * This applies to every `send`, `whenWritable` and `done` promise the library creates and the caller may drop.
  * Without it, Node prints unhandled-rejection warnings, or crashes with `--unhandled-rejections=strict`.
* `scheduleMicrotask` maps to `queueMicrotask`.
* `Future.timeout` maps to `AbortSignal.timeout` or a race.
* **Sync throw vs rejection** (§13.3): Dart throws synchronously for misuse in `send`, `open`, `goAway`, `close(status)` and `ping`. **rec:** keep those methods non-`async` (return a promise, throw sync), or document the change.
* Error classes:
  * **rec:** `class SwitchboardError extends Error { status; get code() }`, plus `ProtocolError`.
  * Use `RangeError` / `TypeError` for argument misuse.
  * Don't add `cause` chains that leak the IDENT failure reason to the wire.

### 20.4 Time
* All timers must come from an injectable scheduler: keep-alive, LIMITS wait, close confirm, goAway grace, identity, transport close timeout, and all of Talk.
  * The Dart tests use `fake_async` everywhere, and `package:clock` for `monotonicNow`.
  * **rec:** a `Clock` interface `{ now(): number; setTimeout(fn, ms): handle; clearTimeout(h) }`, defaulting to `performance.now()` and global timers. Tests use a manual clock, or vitest/sinon fake timers.
* `setTimeout` caps at 2^31−1 ms; re-arm in chunks as Dart Talk does.
* In Node, `unref()` keep-alive timers so an idle connection doesn't hold the process open, or make that an option. Dart doesn't have the concept.
* Durations become milliseconds as numbers (`keepAliveIntervalMs`), or a typed duration. Dart `null` disables keep-alive and `Duration.zero` means "forever" or "disabled" for some options. **rec:** keep that exact sentinel table, documented per option, or use explicit `Infinity` / `false`.

### 20.5 Streams and consumption (the big one)
* Dart `MuxChannel.stream` is a **single-subscription** stream, synchronous (`sync: true`), fed only while a listener is active and not paused. Consumption means delivery to the listener, which drives credit.
* `incoming` is the same shape for channels.
* **rec:** pull-based **async iterators** (`for await (const chunk of channel)`) map 1:1:
  * `next()` = take delivery, so return credit.
  * Not calling `next()` = paused.
  * `return()` = cancel, so discard.
  * WHATWG `ReadableStream` with `highWaterMark: 0` and `pull` also works, and gives `pipeTo` and backpressure for free.
  * Node `EventEmitter` / `Readable` in flowing mode would break credit semantics unless paused explicitly.
* The send side: `send()` returns a promise for "handed over", and `whenWritable` exists. **rec:** a `WritableStream` sink whose `write()` awaits `send()`, plus the explicit method. The Dart `sink.add` (fire and forget) is a footgun for unbounded queueing.
* Talk's `ordered:` semantics depend on Dart's per-microtask stream delivery (§16.3).

### 20.6 Resource management
* `close()` / `goAway()` / `done` map well to `Symbol.asyncDispose`: `await using conn = …`. TS 5.2+ downlevels it.
* **rec:** support `AbortSignal` on `identify`, `ping` and connect.

### 20.7 Capability discovery
* Dart uses `is FrameLimited` and the like. **rec:** an optional `capabilities` object on the transport interface, or optional members checked with `typeof t.abort === 'function'`.

### 20.8 Transports
* **Browser WebSocket:**
  * set `binaryType = 'arraybuffer'`;
  * `bufferedAmount` is pollable only, so no OutputReady capability;
  * subprotocol via the constructor;
  * no control over permessage-deflate (the browser offers it; the Dart server refuses it);
  * no frame size limit on receive, so enforce in `onmessage`.
* **Node 22** has a global `WebSocket` (undici). For servers, and for `maxPayload`, use `ws` with `perMessageDeflate: false`.
* **Node `net.Socket`:** `setNoDelay`, write/drain, `writableLength`, `bytesWritten`, pause/resume, `end` + linger + `destroy` (§4.2).

### 20.9 Crypto
* WebCrypto calls are async (§8.2).
* `getRandomValues` is limited to 65536 bytes per call (Appendix A.7).
* Ed25519 private import needs PKCS#8.
* Node 22 and modern browsers have Ed25519 in `crypto.subtle`. Dart carries a pure Ed25519 for the web; TS shouldn't need one.

### 20.10 API shape notes from the Dart code
* **The `_bulk` routing footgun** (§9.7): raw `incoming` consumers who use Talk must call `adoptBulk`. **rec:** the Talk layer (or the dispatcher) installs the `_bulk` filter on the connection, so raw consumers never see `_bulk` channels. That also gives the `admitAfterGoAway` hook from §15 a home.
* **Procedure arguments** accept `String | Name` in Dart, with duplicates. **rec:** accept `string | Name` and normalize once.
* **Enums:** prefer `const` objects with `as const` over TS `enum`, for erasable syntax and `--isolatedModules`.
* **`MuxOptions`:** a plain options object with defaults merged. Validate like the Dart constructor: ArgumentError becomes RangeError.
* **Logging:** Dart uses `package:logging` with names `Switchboard.Mux`, `Switchboard.Talk` and so on. **rec:** an injectable `logger` option with debug/info/warn levels; never `console` by default.
* **Diagnostics** the tests use: `isOpen`, `peerLimits`, `peerGoingAway`, `peerGoAwayStatus`, `sentBytes`, `receivedBytes`, `bufferedBytes`.

---

## 21. Interop testing against Dart

Dart isn't installed in this container (`dart: command not found`; Node v22.22.0 is). The test harness thread will need the Dart SDK in CI.

**Easiest interop targets in the Dart repo:**
* `example/echo_tcp.dart`: a Switchboard server over TCP with service `echo`, procedures `UPPER`, and `COUNT` (3 items, then `done`).
* `example/raw_layers.dart`: mux plus Talk over memory.
* `test/integration/embedded_test.dart`: the byte-exact embedded round trip.

**Settings for byte-exact exchanges** against a Dart peer:
* `announceLimits: false` and `awaitPeerLimits: false` on the side under test. Otherwise LIMITS is the first frame and DATA waits.
* `shortIdsOnly` to match the embedded profile.
* No keep-alive (`keepAliveInterval: null`).

**Cross-checks worth having:**
* TS initiator ↔ Dart acceptor, and the reverse, over TCP and WebSocket.
* NONCE/IDENT with C2's holder key, both directions.
* A bulk transfer above 32752 bytes, so it goes through the `_bulk` path.
* GOAWAY drain.
* A protocol-error injection, checking that the Dart side reports PROTOCOL_ERROR.

---

## Appendix A. Dart test vectors not on the wiki page

Extracted from the Dart test sources and cross-checked against the wiki by script. Sections A.1–A.11 follow the vector test files; A.12 covers status truncation from `mux_limits_test.dart`.

This lists every concrete vector or byte-exact assertion in the Dart reference tests that is missing from `wiki/switchboard/test-vectors.md`, plus extra decoded expectations the Dart tests attach to vectors the wiki already has. Hex is copied from the Dart sources unchanged. When a test builds bytes by interpolation (`$endpoint`, `$hmacCredential`, `List.filled(65, '00')`), the expansion is written out from the constants in the same file.

**Derived** marks bytes the test never writes out. The test checks them through a round trip or a field assertion, and the encoding here was computed from the layout. Use these as convenient expectations, not as canonical vectors.

Source root: `test/` in `kaetemi/switchboard`. A case is cited as `file › group › test`.

Every wiki-row vector in these files was checked programmatically against the wiki. All of them match byte for byte, C1, C2, P, the RFC 8032 keys and X included. X lives on the `quic-binding.md` page.

### Error vocabulary

- **PE**: `ProtocolException`. It always carries `StatusCode.protocolError` (32). Some tests check the code, others only the type.
- **FormatException**, **ArgumentError**, **RangeError**, **StateError**: the Dart exception types. In TS these are thrown errors; how a TS port names them is up to the design.
- **UNAUTHENTICATED**: `SwitchboardException` with `StatusCode.unauthenticated` (16).

### Talk header key (needed for the Talk tables)

The flags byte is laid out as follows:

| Bits | Meaning |
| --- | --- |
| `0x01` | HAS_PROCEDURE |
| `0x02` | HAS_REQUEST |
| `0x04` | HAS_RESPONSE |
| `0x08` | STREAM |
| `0x30` | kind: 0 message, 1 stream item, 2 abort, 3 extend |
| `0x40` | BULK |
| `0x80` | reserved |

The fields follow the flags byte in this order:

1. `[procedure 8]`
2. `[request id u24]`
3. `[response id u24]`
4. `[window u32, only with STREAM]`
5. `[payload]`

The request id comes before the response id. The window comes after both ids. A BULK reference (u32 number, u48 length) is the payload, so it comes after the window.

---

### A.1 `vectors/talk_vectors_test.dart`

**Wiki coverage.** All 20 positive "Talk messages" rows and all 17 "Negative vectors (channel protocol error)" rows. The negatives check `e.code == PROTOCOL_ERROR`. Each positive row is checked three ways: decode equals the expected frame, decode→encode gives the same bytes, and the expected frame encodes to the same bytes.

#### 1a. Extra decoded expectations on wiki rows

| Case | Bytes | Expected | Test |
| --- | --- | --- | --- |
| Bulk ref of "PUT" request | `43 50 55 54 00 00 00 00 00 01 00 00 01 00 00 00 00 00 10 00 00 00` | `.bulkReference == TalkBulkReference(1, length: 1048576)` | Talk vectors, positive › bulk references decode |
| Bulk ref, length unknown | `44 01 00 00 01 00 00 00 FF FF FF FF FF FF` | `.bulkReference.number == 1`, `.length == null` (u48 `FFFFFFFFFFFF` is unknown) | same |
| Bulk ref, 0 bytes | `54 02 00 00 02 00 00 00 00 00 00 00 00 00` | `.number == 2`, `.length == 0` | same |
| bulkReference on a non-BULK frame | `04 01 00 00` | `.bulkReference` throws **StateError** | same |
| Abort status | `24 01 00 00 05 00 6E 6F 70 65` | `.status == Status(NOT_FOUND, "nope")` | Talk vectors, positive › abort statuses decode |
| Cancel status | `22 02 00 00 01 00` | `.status.known == CANCELLED` | same |
| Channel abort status | `20 10 00` | `.status.known == UNAUTHENTICATED` | same |
| EXTEND values | `34 01 00 00 A0 BB 0D 00 E0 93 04 00` | `.extension == (deadline: 15 min, renew: 5 min)` | Talk decoding rules beyond the wiki vectors › EXTEND values decode |
| EXTEND, empty payload | `34 01 00 00` | `.extension == (deadline: null, renew: null)`, empty payload, meaning "restart the default timeout" | same |
| Credit fields | `32 02 00 00 08 00 00 00` | round trip; `kind == extend`, `isCredit == true`, `requestId == 2`, `hasResponse == false`, `creditItems == 8`; `.extension` throws **StateError** | Item windows on the wire › a credit names a request of its sender and counts items |
| EXTEND is not a credit | `34 01 00 00` | `isCredit == false`; `.creditItems` throws **StateError** | same |
| Response with absent procedure | `04 01 00 00` | `.procedure == null`. The test title says "reads as the empty name", but the assertion is `isNull` | Talk decoding rules beyond the wiki vectors › a missing procedure reads as the empty name |
| Non-stream frames carry no window | `03 47 45 54 5F 50 52 4F 46 01 00 00`, `15 49 54 45 4D 00 00 00 00 02 00 00 07`, `07 4D 4F 52 45 00 00 00 00 07 00 00 01 00 00` | round trip; `stream == false`; `window == 0xFFFFFFFF` (the default `unlimitedWindow`; the window is part of frame equality) | Item windows on the wire › frames that are not stream requests carry no window |

#### 1b. Extra positive vectors

Each row decodes, and round-trips where stated.

| Case | Bytes | Expected | Test |
| --- | --- | --- | --- |
| Empty abort payload on a response | `24 01 00 00` | `.status == Status.of(UNKNOWN)` (code 2), **not** OK | Talk vectors, positive › abort statuses decode |
| Empty channel abort | `20` | `.status.known == UNKNOWN` | same |
| EXTEND, deadline unchanged, renew 300000 ms | `34 01 00 00 00 00 00 00 E0 93 04 00` | `.extension == (deadline: null, renew: 5 min)` (decode only) | Talk decoding rules beyond the wiki vectors › EXTEND values decode |
| EXTEND, both fields 0 | `34 01 00 00 00 00 00 00 00 00 00 00` | `.extension == (null, null)`; `payload.length == 8`. The payload length is what separates "unchanged" from "restart" | same |
| EXTEND, largest deadline | `34 01 00 00 FF FF FF FF 01 00 00 00` | `deadline == 0xFFFFFFFF ms` (4294967295), `renew == 1 ms` | same |
| Final response to 3 that is stream request 4, unlimited | `0E 04 00 00 03 00 00 FF FF FF FF` | `stream`, `requestId 4`, `responseId 3`, `window == 0xFFFFFFFF` | Talk decoding rules beyond the wiki vectors › chained stream request and item request decode |
| Stream item to 3 that is request 5 | `16 05 00 00 03 00 00 AA` | `kind streamItem`, `requestId 5`, `responseId 3`, payload `AA`. Also round-trips, with `stream false` and window unlimited (see the frames-without-window test) | same, and Item windows › frames that are not stream requests carry no window |
| Payload is a view | `04 01 00 00 01 02 03` | payload `01 02 03`. Setting input byte 4 to `0x7F` afterwards makes `payload[0] == 0x7F`, so decode must not copy (in TS, `subarray`) | Talk decoding rules beyond the wiki vectors › payload is a view, not a copy |
| Stream request, window 0 | `0B 4C 49 53 54 00 00 00 00 02 00 00 00 00 00 00` | round trip; `window == 0`; payload empty. Not equal to the same frame with an unlimited window | Item windows on the wire › a stream request carries its window after its ids |
| Largest finite window, plus payload | `0B 4C 49 53 54 00 00 00 00 02 00 00 FE FF FF FF AA BB` | round trip; `window == 0xFFFFFFFE`; payload `AA BB`; `toString()` contains `window=4294967294` | same |
| Encoded from fields: "LIST", req 2, STREAM, window 3, payload `01` | `0B 4C 49 53 54 00 00 00 00 02 00 00 03 00 00 00 01` | encode of `TalkFrame(kind: message, procedure: LIST, requestId: 2, stream: true, window: 3, payload: 01)` | same |
| Final response to 3 that is stream request 4, window 2 | `0E 04 00 00 03 00 00 02 00 00 00 CC` | round trip; `kind message`, `requestId 4`, `responseId 3`, `stream`, `window 2`, payload `CC` | Item windows › a chained response that is a stream request carries its window after both ids |
| Stream item to 3 that is stream request 5 "SUB", window 4 | `1F 53 55 42 00 00 00 00 00 05 00 00 03 00 00 04 00 00 00 AA` | round trip; `kind streamItem`, procedure `SUB`, `requestId 5`, `responseId 3`, `window 4`, payload `AA` | Item windows › an item that is a stream request carries its window after both ids |
| The same with BULK, no procedure | `5E 05 00 00 03 00 00 00 00 00 00 02 00 00 00 00 00 00 00 00 00` | round trip; `bulk`, `window 0`, `bulkReference == (2, length 0)`; the reference follows the window | same |
| `withBulk` keeps the window | built: `TalkFrame(message, "PUT", req 1, stream, window 7).withBulk(TalkBulkReference(1))` | `window == 7`; `decode(encode) == frame`. **Derived** bytes: `4B 50 55 54 00 00 00 00 00 01 00 00 07 00 00 00 01 00 00 00 FF FF FF FF FF FF` | same |
| Credit with a procedure | `33 41 00 00 00 00 00 00 00 02 00 00 01 00 00 00` | round trip; `creditItems == 1`; the procedure ("A") is optional and ignored | Item windows › a credit names a request of its sender and counts items |
| Largest credit, request id 0xFFFFFF | `32 FF FF FF FF FF FF FF` | round trip; `creditItems == 0xFFFFFFFF`. That this overflows a window is for the channel to decide, not the codec | same |

#### 1c. Extra negative decode vectors

All are **PE**, from group *Talk decoding rules beyond the wiki vectors*. The test name is the case name.

| Case | Bytes | Why |
| --- | --- | --- |
| Reserved bit 0x80 | `81 48 45 4C 4C 4F 00 00 00` | reserved flag |
| BULK with an empty reference | `41 48 45 4C 4C 4F 00 00 00` | the reference must be exactly 10 bytes |
| BULK with an 11-byte reference | `41 48 45 4C 4C 4F 00 00 00 01 00 00 00 00 00 00 00 00 00 00` | 11 bytes |
| BULK reference to bulk number 0 | `41 48 45 4C 4C 4F 00 00 00 00 00 00 00 05 00 00 00 00 00` | number 0, length 5 |
| BULK on a cancel | `62 02 00 00 01 00` | BULK on kind abort with a request id |
| BULK on a channel abort | `60 10 00` | BULK on abort with no ids |
| Empty input | (zero bytes) | no flags byte |
| Response id 0 | `04 00 00 00` | id 0 |
| Chained with response id 0 | `06 01 00 00 00 00 00` | request 1, response 0 |
| EXTEND with STREAM and no request | `3C 01 00 00` | STREAM without HAS_REQUEST, on extend |
| Credit with STREAM and a window | `3A 02 00 00 FF FF FF FF 08 00 00 00` | STREAM on extend |
| Cancel with STREAM and a window | `2A 02 00 00 FF FF FF FF 01 00` | STREAM on abort |
| Credit with an empty payload | `32 02 00 00` | a credit needs exactly 4 bytes |
| Credit with a 3-byte payload | `32 02 00 00 08 00 00` | 3 bytes |
| Credit with a 5-byte payload | `32 02 00 00 08 00 00 00 00` | 5 bytes |
| Credit with a procedure and 0 items | `33 41 00 00 00 00 00 00 00 02 00 00 00 00 00 00` | 0 items |
| Chained stream request without its window | `0E 04 00 00 03 00 00` | window missing |
| Item that is a stream request, truncated window | `1E 05 00 00 03 00 00 04 00 00` | 3 of the 4 window bytes |
| EXTEND with a 2-byte payload | `34 01 00 00 AA BB` | must be 0 or 8 bytes |
| EXTEND with a 9-byte payload | `34 01 00 00 01 00 00 00 01 00 00 00 00` | 9 bytes |
| STREAM_ITEM with only a request id | `13 48 45 4C 4C 4F 00 00 00 01 00 00` | an item needs HAS_RESPONSE |
| Request without procedure | `02 01 00 00` | |
| ABORT with a one byte payload | `24 01 00 00 05` | a status payload cannot be 1 byte |
| Truncated request id | `03 48 45 4C 4C 4F 00 00 00 01 00` | |
| Truncated response id | `04 01 00` | |
| Procedure with a byte after the padding | `01 61 00 62 00 00 00 00 00` | invalid name |

Together with the wiki rows, the edge cases come out as follows:

- **Abort payload length.** 0 bytes means UNKNOWN, 1 byte is PE, 2 bytes or more is a status.
- **EXTEND payload length.** 0 and 8 bytes are valid. 2, 4 (wiki) and 9 bytes are PE.
- **Credit payload length.** Only 4 bytes is valid. 0, 3, 5 and 8 (wiki) bytes are PE. A count of 0 items is PE.
- **Ids.** A request id or response id of 0 is PE. 0xFFFFFF is valid, as the wiki shows.

#### 1d. Encoder and helper vectors

| Case | Call | Expected | Test |
| --- | --- | --- | --- |
| Largest bulk reference | `TalkBulkReference(0xFFFFFFFF, length: 0xFFFFFFFFFFFE).encode()` | `FF FF FF FF FE FF FF FF FF FF`; decodes back equal ("survive the 48-bit arithmetic on the web") | Talk vectors, positive › bulk references decode |
| Extend payload, nothing | `extendPayload()` | `''` (empty) | Talk decoding rules beyond the wiki vectors › EXTEND values encode in whole milliseconds, rounded up |
| Rounded up | `extendPayload(deadline: 1500 µs)` | `02 00 00 00 00 00 00 00` | same |
| Under 1 ms is 1 ms (0 would mean "unchanged") | `extendPayload(renew: Duration.zero)` | `00 00 00 00 01 00 00 00` | same |
| Clamped to u32 | `extendPayload(deadline: 60 days)` | `FF FF FF FF 00 00 00 00` | same |
| Negative | `extendPayload(deadline: -1 s)` | **ArgumentError** | same |
| `.extension` on a non-EXTEND frame | `TalkFrame(kind: message, responseId: 1).extension` | **StateError** | Talk decoding rules beyond the wiki vectors › EXTEND values decode |
| Credit payload | `creditPayload(1)` | `01 00 00 00` | Item windows › a credit names a request… |
| Largest credit payload | `creditPayload(0xFFFFFFFF)` | `FF FF FF FF` | same |
| Credit payload of 0 | `creditPayload(0)` | **ArgumentError** | same |
| Credit payload over u32 | `creditPayload(0x100000000)` | **ArgumentError** | same |

#### 1e. `TalkFrame.encode` refusals

All are **ArgumentError**, from group *TalkFrame.encode refuses invalid frames*. Fields not given are the defaults: no procedure, ids 0, `stream: false`, `window: 0xFFFFFFFF`, empty payload.

| Test name | Frame |
| --- | --- |
| plain message without procedure | `kind: message` |
| request without procedure | `kind: message, requestId: 1` |
| stream without request | `kind: message, procedure: A, stream: true` |
| item without response | `kind: streamItem` |
| abort with both ids | `kind: abort, requestId: 1, responseId: 1` |
| extend without an id | `kind: extend` |
| extend with both ids | `kind: extend, requestId: 1, responseId: 1` |
| credit without a payload | `kind: extend, requestId: 1` |
| credit with an extend payload | `kind: extend, requestId: 1, payload: extendPayload(deadline: 1 h)` (8 bytes) |
| credit of 0 items | `kind: extend, requestId: 1, payload: 4 zero bytes` |
| credit with STREAM | `kind: extend, requestId: 1, stream: true, payload: creditPayload(1)` |
| cancel with STREAM | `kind: abort, requestId: 1, stream: true` |
| window without STREAM | `kind: message, procedure: A, requestId: 1, window: 4` |
| negative window | `…, stream: true, window: -1` |
| window over u32 | `…, stream: true, window: 0x100000000` |
| extend with a 1-byte payload | `kind: extend, responseId: 1, payload: 1 zero byte` |
| request id over u24 | `kind: message, procedure: A, requestId: 0x1000000` |
| negative response id | `kind: message, responseId: -1` |

#### 1f. Behaviour (not a codec)

None at connection level. At the API level:

- The decoded payload is a view of the input buffer (see the payload-view row in 1b).
- `withBulk` keeps `window`.
- Frame equality includes `window`, `bulk` and the payload bytes.

---

### A.2 `status_test.dart`

**Wiki coverage.** The test-vectors page has no Status section. Status bytes appear on it only inside rows: the mux CLOSE `… 05 00 78` row, the MOVED and RELOCATED rows, and ABORTED `0A 00 2B 1A 00 00 00 00 07 00 00 00`. Everything below is extra.

| Case | Bytes / call | Expected | Test |
| --- | --- | --- | --- |
| Encode NOT_FOUND "x" | `Status.of(notFound, 'x').encode()` | `05 00 78` | Status › encodes code and reason |
| Encode OK | `Status.ok.encode()` | `00 00` (OK encodes as 2 bytes, not empty) | same |
| Encode an arbitrary u16 | `Status(0x1234).encode()` | `34 12` | same |
| Decode NOT_FOUND "x" | `05 00 78` | `== Status.of(notFound, 'x')` | Status › decodes |
| Decode empty | (zero bytes) | `== Status.ok` | same |
| Decode GOING_AWAY | `21 00` | `.known == goingAway` | same |
| Application code 256 | `00 01` | `.known == null`, `.isApplicationCode == true` (code ≥ 256) | same |
| One-byte payload | `05` | **FormatException** | same |
| MOVED with non-UTF-8 fields kept byte for byte | `25 00 FE DC BA 98 76 54 01 00 00 80 78` | `.known == moved`; `encode()` returns these exact bytes; `decode(encode()) == status`; hashCode equals a second decode's | Status › keeps the bytes after the code as received |
| Same reason string, different bytes | `25 00 FF DC BA 98 76 54 01 00 00 80 78` | `.reason` equals the previous one's (FE and FF both become U+FFFD), yet the two are **not equal**: equality compares the raw body bytes | same |
| Decoded equals built from text | `05 00 78` | `== Status.of(notFound, 'x')`; re-encodes to `05 00 78` | same |
| Code only | `05 00` | `== Status(5)` (empty reason) | same |
| toString, text | `Status.of(notFound, 'x')` | `notFound: x` | Status › toString shows binary reasons as hex |
| toString, no reason | `Status.of(notFound)` | `notFound` | same |
| toString, MOVED | `25 00 2B 1A 00 00 00 00 07 00 00 00` | `moved: [2b 1a 00 00 00 00 07 00 00 00]` (lowercase hex in brackets when the reason has control chars, DEL or U+FFFD) | same |
| toString, RELOCATED | `26 00 2B 1A 00 00 00 00 07 00 00 00` | `relocated: [2b 1a 00 00 00 00 07 00 00 00]` | same |
| toString, application code | `00 01 FF` | `code 256: [ff]` | same |
| Code over u16 | `Status(0x10000).encode()` | **ArgumentError** | Status › rejects codes outside u16 |
| Numbering | `StatusCode.unauthenticated.code == 16`, `protocolError.code == 32`, `fromCode(14) == unavailable`, `fromCode(17) == null` | | Status › gRPC numbering |

Read as `MovedStatus`, the FE-variant MOVED wire above would give owner `0x547698BADCFE`, epoch `0x80000001` and reason `x`. That is **derived**: this test does not assert it.

Behaviour: none at connection level. Decoding is lossless: the body is kept, and `reason` is a lossy UTF-8 view of it.

---

### A.3 `bytes_test.dart`

**Wiki coverage.** None; these are primitive codecs.

| Case | Bytes / call | Expected | Test |
| --- | --- | --- | --- |
| Writer, all widths, little endian | `u8(0xAB) u16(0x1234) u24(0x123456) u32(0x12345678) u48(0x1234567890AB) name('api') string8('hi') bytes([1,2,3])` | `AB 34 12 56 34 12 78 56 34 12 AB 90 78 56 34 12 61 70 69 00 00 00 00 00 02 68 69 01 02 03`; the reader reads back the same values; `rest() == [1,2,3]`; `isDone` | writer and reader round trip little endian |
| u16 truncated | `01` read as `u16()` | **FormatException** | reader throws on truncation |
| string8 truncated | `03 61` read as `string8()` | **FormatException** | same |
| name on empty input | `` read as `name()` | **FormatException** | same |
| hexBytes is lenient | `hexBytes('0a 0B0c')` | `[10, 11, 12]` (mixed case, spaces optional) | hex helpers |
| Odd digit count | `hexBytes('abc')` | **FormatException** | same |
| **JS 32-bit arithmetic** | `u32(0xFFFFFFFF) u48(0xFFFFFFFFFFFF) u48(0x100000002) u48(0xFFFF00000000)` | `FF FF FF FF FF FF FF FF FF FF 02 00 00 00 01 00 00 00 00 00 FF FF`; reads back `0xFFFFFFFF`, `0xFFFFFFFFFFFF`, `0x100000002`, `0xFFFF00000000` | u32 and u48 high values survive 32-bit arithmetic |

Source notes: `string8` throws ArgumentError over 255 bytes, and every reader error is a FormatException naming the field and offset. Neither is tested here.

---

### A.4 `name_test.dart`

**Wiki coverage.** All five "Names" rows (empty, `api`, `REGISTER`, `_ns`, `é`), and the rejection of `61 00 62 00 00 00 00 00`. The wiki says only "MUST be rejected"; the test expects **FormatException**.

| Case | Bytes / call | Expected | Test |
| --- | --- | --- | --- |
| Round trips | `''`, `'a'`, `'api'`, `'REGISTER'`, `'é'`, `'_ns'` | `Name.fromBytes(Name(s).bytes)` has `toString() == s`, `== Name(s)`, and the same hashCode | Name › round trips |
| 9 ASCII bytes | `Name('123456789')` | **ArgumentError** | Name › rejects too long and embedded zero |
| 5 chars, 10 UTF-8 bytes | `Name('ééééé')` | **ArgumentError** (the length limit is in UTF-8 bytes, not characters) | same |
| Embedded NUL | `Name('a\u0000b')` | **ArgumentError** | same |
| Byte after a zero | `61 00 62 00 00 00 00 00` | `Name.fromBytes` → **FormatException** | Name › rejects bad wire bytes |
| Wrong length (3 bytes) | `61 00 00` | `Name.fromBytes` → **FormatException** (must be exactly 8 bytes) | same |
| Properties | | `Name('').isEmpty`; `Name.empty == Name('')`; `Name('_ns').isReserved` (first byte `0x5F`); `Name('ns').isReserved == false`; `Name('REGISTER').length == 8`; `Name('é').length == 2` (byte length) | Name › properties |

---

### A.5 `vectors/address_vectors_test.dart`

**Wiki coverage.**

- Address header rows: default, `api` with payload, `npc` instance 0x1A2B, `chat` shard 7, `api` host hint, everything, and the empty open payload.
- Both `_relay` rows and the `_bulk` row.
- The three address negatives. These are only checked as `isA<ProtocolException>`.

Every positive row asserts all of `type`, `instance`, `shard`, `host` and `payload`, then `encode()` back to the bytes.

| Case | Bytes | Expected | Test |
| --- | --- | --- | --- |
| Empty open payload re-encodes | (zero bytes) | `type == null`, `instance 0`, `shard null`, `host null`, payload empty; **`encode() == 00`** (the wiki states only the decode) | empty open payload decodes as default |
| `_relay` outer fields | both wiki `_relay` rows | outer `type == Name('_relay')` (`Services.relay`), `instance 0`, `shard null`, `host null`; the outer payload is the inner bytes (`03 77 6F 72 6B 65 72 2D 67 2A 00 00 00 00 00 01 02` and `05 63 68 61 74 00 00 00 00 07 00 00 00`); the inner decodes to (worker-g, 0x2A, payload `01 02`) and (chat, shard 7); `ChannelAddress(type: _relay, payload: inner.encode()).encode()` equals the outer bytes | relay open payloads › "worker-g" instance 0x2A, app payload 01 02 / "chat" shard 7 |
| `_bulk` fields | `01 5F 62 75 6C 6B 00 00 00 02 00 00 00 00 00 01 00 00 00` | `type == Name('_bulk')`, `instance 0`, `shard null`, `host null`; `TalkBulkOpen.isBulk`; `parentId 2`, `number 1`; `TalkBulkOpen(2, 1).encode()` equals the bytes | _bulk, parent channel 2, bulk number 1 |
| `_bulk`, long parent, largest number (48-bit on the web) | `TalkBulkOpen(0xFFFFFFFFFFFE, 0xFFFFFFFF)` | decode(encode) gives `parentId 0xFFFFFFFFFFFE`, `number 0xFFFFFFFF`. **Derived** bytes: `01 5F 62 75 6C 6B 00 00 00 FE FF FF FF FF FF FF FF FF FF` | same |
| `_bulk` truncated (9-byte app payload) | `01 5F 62 75 6C 6B 00 00 00 02 00 00 00 00 00 01 00 00` | `ChannelAddress.decode` succeeds; `TalkBulkOpen.decode` → **PE** | same |
| `_bulk` bulk number 0 | `01 5F 62 75 6C 6B 00 00 00 02 00 00 00 00 00 00 00 00 00` | `TalkBulkOpen.decode` → **PE** | same |

Untested source rule: `TalkBulkOpen.decode` ignores bytes after its 10.

Behaviour: none.

---

### A.6 `vectors/identity_vectors_test.dart`

**Wiki coverage.**

- C1 and C2 bytes, the RFC 8032 seeds and keys, and P with its 86-byte signed message.
- The NONCE row and both IDENT rows.
- All the "Invalid credentials" rows: version 2, kind 0 and 4, key id byte 5, identity byte 11, holder key length byte 24, signature kinds 3 and 2, truncated, trailing byte, and 638 bytes.
- The three "Also refused" cases and the unknown right at byte 44.
- All the scope-pattern rows.
- The seven IDENT payload negatives.
- The "proof covers lengths" case.

#### 6a. Extra decoded expectations on wiki rows

| Case | Expected | Test |
| --- | --- | --- |
| C1 issued | `CredentialIssuer.hmac(Name('k1'), 00..1F).issue(kind: node, identity: 'npc-7', scopes: [register npc, open *, watch npc-*], issuedAt: 1767225600 s, lifetime: 8 h)` encodes exactly C1 | credential layout › HMAC-SHA256 credential with a fixed key |
| C1 decoded | `version 1`, `kind node`, `keyId 'k1'`, `identity 'npc-7'`, `issuedAt 1767225600`, `expiresAt 1767254400`, `holderKey null`, `isBearer true`, scopes in order `[register 'npc', open '*', watch 'npc-*']` (right codes 1, 2, 3), `signatureKind hmacSha256`, equal to the issued credential, re-encodes to C1, `signedBytes` = the first 54 bytes, `issuedAtTime == 2026-01-01T00:00:00Z`, `expiresAtTime == 2026-01-01T08:00:00Z`; `CredentialVerifier(hmacKeys: {k1: key}).verify(C1, now: 1767225660 s)` returns the decoded credential | same |
| C2 issued | `CredentialIssuer.ed25519(Name('auth1'), keyPairFromSeed(authority seed))` and `ed25519FromSeed` both have `publicKey()` = the authority key; `HolderKey.fromSeed(holder seed).publicKey` = the holder key; `issue(kind: node, identity: 'worker-a1', scopes: [register 'worker-*'], holderKey, issuedAt)` **with no lifetime** gives C2. The default lifetime is 8 h (`CredentialIssuer.defaultLifetime`) | credential layout › Ed25519 credential with a holder key, from fixed seeds |
| C2 decoded | `keyId 'auth1'`, `identity 'worker-a1'`, `holderKey` = the holder key, `isBearer false`, `scopes [register 'worker-*']`, `signatureKind ed25519`; verifies at 1767225660 s with `ed25519Keys: {auth1: authority key}` | same |
| Byte 44 = `08` | scopes `[register npc, open *]`; **`encode()` returns the patched bytes unchanged** (the unknown scope is kept); `signedBytes` = patched bytes `[0, 54)` | credential layout › a scope with an unknown right is ignored, the bytes kept |
| Every malformed negative | `Credential.decode` → **PE**, *and* `verifier.verify(bytes)` → **UNAUTHENTICATED** | credential layout › negative (malformed) › … |
| C1, last byte XOR 0x01 (`C9`→`C8`) | verify → **UNAUTHENTICATED** | credential layout › negative (signature) › a flipped signature bit |
| C2, byte 70 XOR 0x01 (`2A`→`2B`) | `Credential.decode(bytes).scopes.single.pattern == Name('worker-+')`; verify → **UNAUTHENTICATED** | negative (signature) › a flipped bit in a signed field |
| HMAC credential with a holder key | `Credential(kind: node, keyId: 'k1', identity: 'x', issuedAt: 1767225600, expiresAt: 1767254400, holderKey: <holder key>, signatureKind: hmacSha256, signature: HMAC-SHA256(00..1F, signedBytes))` → verify **UNAUTHENTICATED** (bytes built at runtime) | negative (signature) › an HMAC credential that names a holder key |
| IDENT C1 payload decode | `56 00` + C1 + `00 00` → credential = C1, intent, receiver and proof all empty | connection identity control messages › IDENT, bearer credential, no intent, no receiver, no proof |
| IDENT C2 frame decode | the wiki frame decodes through `MuxFrame` → `MuxControlMessage` → `MuxIdent` back to credential C2, intent `01 02 03 04`, receiver `63 6F 6E 73 75 6D 65 72`, proof P | connection identity control messages › IDENT, holder-key credential, intent, receiver, proof |
| P signed message | `53 57 42 49 44 45 4E 54` + 32×`11` + 32×`22` + `04 01 02 03 04 08 63 6F 6E 73 75 6D 65 72`, length 86; `HolderKey.sign` → P | same |

#### 6b. Extra vectors

| Case | Bytes / call | Expected | Test |
| --- | --- | --- | --- |
| Empty credential | (zero bytes) | `Credential.decode` → **PE**; verify → **UNAUTHENTICATED** | credential layout › negative (malformed) › empty |
| 638 bytes | 638 zero bytes (`Uint8List(638)`) | **PE** / **UNAUTHENTICATED**. The wiki says "any 638 bytes"; the test uses zeros | negative (malformed) › longer than 637 bytes |
| Intent of 65 bytes, as written | `00 00 41` followed by 65 × `00` | `MuxIdent.decode` → **PE** | negative (protocol error) › intent of 65 bytes |
| Full 8-byte pattern, no wildcard | `Scope.of(open, 'abcdefgh').matches(Name('abcdefgh'))` | `true` | scope patterns › "abcdefgh" matches "abcdefgh" |
| Admin allows everything | `Credential(kind: node, keyId: 'k', identity: 'root', issuedAt: 0, expiresAt: 0, scopes: [admin 'nothing'], hmacSha256, 32 zero-byte sig)` | `allows(open, 'x') == true`; `allows(broker, '_ns') == true` (right codes: register 1, open 2, watch 3, claim 4, migrate 5, broker 6, admin 7) | scope patterns › admin allows everything |

#### 6c. Behaviour

None in this file. The wiki says the connection-state checks live in `test/identity_test.dart`. Its group *connection identity › protocol errors with / without a verifier* runs each case once with a verifier and once without. Each expects a GOAWAY with PROTOCOL_ERROR and `done` with PROTOCOL_ERROR:

| Case | Frames sent |
| --- | --- |
| NONCE of 31 bytes | `control(nonce, 31 zero bytes)` |
| a second NONCE | `control(nonce, 32 zeros)` twice |
| IDENT before the nonces | an IDENT of a bearer credential, sent first |
| a truncated IDENT | NONCE, then IDENT payload `05 00 01 02` |
| an IDENT with an intent over 64 bytes | NONCE, then IDENT `[credLen u16][cred] 41 <65 zeros> 00` |

---

### A.7 `vectors/exporter_identity_vectors_test.dart`

**Wiki coverage.** Everything is on `quic-binding.md`, "Test vectors", **X**, not on `test-vectors.md`: the 118-byte `SWBIDTLS` message and the signature X, both byte-identical. The prose there also covers these outcomes:

- X does not verify as a nonce-bound proof.
- P does not verify as an exporter-bound proof.
- X with the nonces swapped does not verify.

Extras:

| Case | Call | Expected | Test |
| --- | --- | --- | --- |
| Label constant | `MuxIdent.exporterProofLabel` | `'SWBIDTLS'.codeUnits`, i.e. `53 57 42 49 44 54 4C 53` | the signed message and its signature |
| Message length | `MuxIdent.proofMessage(sender 32×11, receiver 32×22, intent 01 02 03 04, receiver 'consumer', exporter: 32×33)` | 118 bytes, equal to the wiki message; `HolderKey.fromSeed(holder seed).sign(msg)` → X | same |
| X verifies | `verifyIdent(MuxIdent(C2, intent, receiver, X), verifier with auth1 key, senderNonce 32×11, receiverNonce 32×22, localIdentity 'consumer', exporter 32×33)` at clock 1767225660 s | `identity 'worker-a1'`, `receiver 'consumer'`, `intent 01 02 03 04` | X verifies as an exporter-bound proof |
| P still verifies nonce-bound | the same with P and no exporter | `identity 'worker-a1'` | same |
| X without exporter | | **UNAUTHENTICATED** | X does not verify as a nonce-bound proof |
| P with exporter | | **UNAUTHENTICATED** | P does not verify as an exporter-bound proof |
| X, nonces swapped | sender 32×22, receiver 32×11 | **UNAUTHENTICATED** | X with the nonces swapped … does not verify |
| X, other exporter | exporter 32×`34` | **UNAUTHENTICATED** | X with another exporter does not verify |
| Exporter of 31 bytes | `proofMessage(…, exporter: 31 bytes)` | **ArgumentError** | an exporter of another length is refused |
| Exporter of 33 bytes | `verifyIdent(…, exporter: 33 bytes)` | **ArgumentError** (not UNAUTHENTICATED) | same |
| secureRandomBytes beyond the Web Crypto quota | `secureRandomBytes(n)` for n in 0, 32, 65536, 65537 | the length is n. In TS, `crypto.getRandomValues` is capped at 65536 bytes per call, so chunk the calls | secure random bytes support lengths beyond the Web Crypto quota |

---

### A.8 `vectors/binding_protocol_test.dart`

**Wiki coverage.** No codec vectors. All three tests exercise behaviour. The frame bytes below are **derived** from the calls in the tests.

**Test 1: "IDENT verification gates OPEN, PING and a queued IDENT: false" / "…: true".**

Setup: the mux is the acceptor, with a `HeldVerifier` whose `verify` returns a pending future.

The raw peer sends:

1. NONCE (`02 00 00 05` + 32 × `00`), then waits for the mux's NONCE.
2. In one burst: IDENT with an empty credential (**derived** `02 00 00 06 00 00 00 00`), OPEN on channel 2 with a 1-byte payload (**derived** `12 02 00 00`), PING `07` (**derived** `02 00 00 01 07`), and the same IDENT again.

While the verifier is pending:

- Nothing has been delivered on `incoming`.
- `verifier.calls == 1`: the second IDENT is queued, not verified in parallel.

If verification succeeds with a credential whose identity is `'verified'`:

- PONG with payload `[7]` arrives.
- The channel arrives with `peerIdentity.identity == 'verified'`. Both the OPEN and the PONG are processed *after* the identity takes effect.
- Then the queued IDENT is verified (`calls == 2`).

If verification fails with UNAUTHENTICATED `'refused'`:

- No PONG is ever sent and nothing opens.
- `calls == 1`.
- `connection.done` has code UNAUTHENTICATED.

**Test 2: "wrong receiver closes synchronously without entering the verifier".**

- The acceptor has `localIdentity = 'receiver'`, over synchronous stream controllers.
- After NONCE (32 × `00`), it receives `MuxIdent(credential: empty, receiver: 'somebody-else')`. The payload is **derived** as `00 00 00 0D 73 6F 6D 65 62 6F 64 79 2D 65 6C 73 65`.
- `connection.isOpen` is false immediately, in the same synchronous call, and `verifier.calls == 0`. The receiver check comes before any async verification.

**Test 3: "public binding status helpers preserve payloads and map local codes".**

- Input: `MovedStatus(owner: 0x123456789ABC, epoch: 0xFEDCBA98, reason: 'é' × 900).toStatus()`. The **derived** head is `25 00 BC 9A 78 56 34 12 98 BA DC FE`, followed by 1800 reason bytes.
- `statusForWire(...)` keeps the encoded length within 1024. **Derived**: exactly 1024, with 506 `é`.
- `MovedStatus.fromStatus(wire)` keeps owner `0x123456789ABC` and epoch `0xFEDCBA98`, and the reason has no U+FFFD (the cut fell on a character boundary).
- `statusForWire(Status.of(CONNECTION_LOST)).known == UNAVAILABLE`.
- `statusForWire(Status(256))` → **ArgumentError**: application codes are refused in CLOSE and GOAWAY.
- `statusFromWire(Status(256)).known == UNKNOWN`. Per the source, the reason becomes the original `toString()`, `code 256`.

---

### A.9 `vectors/naming_vectors_test.dart`

**Wiki coverage.**

- Every "Naming service payloads" row.
- The "ends after its endpoints" note (`6E 70 63 00 00 00 00 00 05 00 00 00 00 00 00` re-encodes with `00 00` appended).
- The `RENEW` and `CONNECT` procedure names.
- All four CONNECT rows. The tests write them as `$endpoint` interpolation, which is the same composition the wiki uses.
- The CONNECT negatives, though some appear on the wiki only as fragments; full bytes below.

`$endpoint` = `13 74 63 70 3A 2F 2F 31 30 2E 30 2E 30 2E 37 3A 39 33 30 30`.

#### 9a. Extra decoded expectations on wiki rows

| Case | Expected | Test |
| --- | --- | --- |
| REGISTER | `RegisterRequest.decode`: `type 'npc'`, `requestedInstance 0`, `endpoints [Uri 'tcp://10.0.0.5:9101']`, `metadata` empty, round trip | REGISTER npc, assign id, one endpoint |
| REGISTER response | `RegisterResponse.decode(...).instance == 5`; `RegisterResponse(5).encode()` gives the bytes | REGISTER response, instance 5 |
| WATCH / UNWATCH | both use the `WatchRequest` codec; empty → `type null`, `encode()` empty | WATCH all / UNWATCH all / WATCH type npc / UNWATCH type npc |
| UP | `ServiceEvent.decodeUp`: `up true`, `address (npc, 5)`, endpoints empty, metadata empty or `[1,2,3]`, round trip | UP npc instance 5, … |
| DOWN | `ServiceEvent.decodeDown`: `up false`, `address (npc, 5)`, round trip | DOWN npc instance 5 |
| CONNECT request | `77 6F 72 6B 65 72 2D 67 2A 00 00 00 00 00 13 74 63 70 3A 2F 2F 31 30 2E 30 2E 30 2E 37 3A 39 33 30 30 04 01 02 03 04` → `ConnectRequest`: `type 'worker-g'`, `instance 0x2A`, `endpoint Uri 'tcp://10.0.0.7:9300'`, `intent [1,2,3,4]`, round trip | CONNECT › request: … |
| CONNECT response | `ConnectResponse.decode(...).identity == 'worker-a1'`, round trip | CONNECT › response: identity worker-a1 |
| Dial-back | `0A 63 6F 6E 73 75 6D 65 72 2D 31 13 74 63 70 3A 2F 2F 31 30 2E 30 2E 30 2E 37 3A 39 33 30 30 04 01 02 03 04` → `DialBackRequest`: `requester 'consumer-1'`, the endpoint, `intent [1,2,3,4]`, round trip | CONNECT › to the instance: requester consumer-1, … |
| Dial-back, anonymous | `00 13 74 63 70 3A 2F 2F 31 30 2E 30 2E 30 2E 37 3A 39 33 30 30 00` → `requester ''`, `intent` empty, round trip | CONNECT › to the instance: no requester identity, no intent |

#### 9b. Extra vectors

| Case | Bytes | Expected | Test |
| --- | --- | --- | --- |
| WATCH with the all-zero name | `00 00 00 00 00 00 00 00` (`Uint8List(8)`) | `WatchRequest.decode(...).type == null` (all types) | WATCH all-zero name is all |
| UNWATCH procedure name | `55 4E 57 41 54 43 48 00` | `Procedures.unwatch.bytes` | procedure names are as specified |
| Other names | | `Procedures.register.bytes == 52 45 47 49 53 54 45 52`; `Procedures.unregister.toString() == 'UNREGSTR'`; `Procedures.watch.toString() == 'WATCH'`; `Services.naming.bytes == 5F 6E 73 00 00 00 00 00` | same |
| CONNECT, intent of 65 bytes | `77 6F 72 6B 65 72 2D 67 2A 00 00 00 00 00 13 74 63 70 3A 2F 2F 31 30 2E 30 2E 30 2E 37 3A 39 33 30 30 41` followed by 65 × `00` | `ConnectRequest.decode` → **PE** | CONNECT › negative: request with an intent of 65 bytes |
| CONNECT without its intent | `77 6F 72 6B 65 72 2D 67 2A 00 00 00 00 00 13 74 63 70 3A 2F 2F 31 30 2E 30 2E 30 2E 37 3A 39 33 30 30` | **PE** | CONNECT › negative: request without its intent |
| CONNECT, endpoint without a scheme | `77 6F 72 6B 65 72 2D 67 2A 00 00 00 00 00 01 78 00` | **PE**. The wiki gives only the `01 78` fragment; the test adds an empty intent `00` | CONNECT › negative: request with an endpoint without a scheme |
| Dial-back, requester not UTF-8 | `01 FF 13 74 63 70 3A 2F 2F 31 30 2E 30 2E 30 2E 37 3A 39 33 30 30 00` | `DialBackRequest.decode` → **PE** | CONNECT › negative: requester that is not UTF-8 |
| CONNECT response cut short | `09 77 6F` (on the wiki) | `ConnectResponse.decode` → **PE** | CONNECT › negative: response cut short |

Behaviour: none.

---

### A.10 `vectors/sharding_vectors_test.dart`

**Wiki coverage.**

- All 29 "Sharding payloads" rows.
- All 7 negatives: SLOTSPC mode 2, SLOT state 3, HOLDING short, PHASE 0, PHASE 6, LOCATE state 3, CLAIM without epoch.
- The "unknown flag bits" SLOTSPC row (`… 00 FE` → shared recovery, not lazy).
- MOVED 0x1A2B/7, MOVED unknown, RELOCATED 0x1A2B/7.
- The ABORTED-has-no-fields row.

Each row asserts a decoded value and then that it encodes back to the same bytes.

#### 10a. Extra decoded expectations on wiki rows

| Row | Expected fields |
| --- | --- |
| SLOTS kv | `space == SlotSpace('kv', count: 1024)` (managed, not lazy, not shared), `capacity 1` |
| SLOTS userq | `space == SlotSpace('userq', count: 65536, lazy: true)`, `capacity 0` |
| SLOTSPC room | `SlotSpace('room', count: 256, shared: true)` |
| SLOTSPC discord | `SlotSpace('discord', count: 16, mode: static)` |
| HOLDING kv | `type 'kv'`, `slots [3, 258]` |
| HOLDING response | `discard [258]` / `discard []` |
| CLAIM discord | `type 'discord'`, `slot 5`, `holding false`, `epoch 0` |
| CLAIM kv | `slot 7`, `holding true`, `epoch 9` |
| CLAIM response | `epoch 7` |
| RELEASE | `type 'kv'`, `slot 7`, `keepStorage true` |
| LOCATE | `type 'userq'`, `slot 0x1234` |
| LOCATE response | `LocateResponse(owned, 0x1A2B, 7)` / `LocateResponse(free, 0, 3)` |
| MIGRATE | `type 'zone'`, `slot 4`, `to 0xB` / `to 0` |
| PHASE | `01` → `draining`; `05` → `rolledBack` |
| ASSIGN kv | `type 'kv'`, `slot 7`, `epoch 8`, `holder 2`, `shared false` |
| ASSIGN room | `holder 0`, `shared true` |
| ASSIGN response | empty → `notHolding false`; `01` → `notHolding true` |
| DRAIN | `type 'kv'`, `slot 7`, `epoch 9`, `to 3` |
| FORWARD | `slot 7`, `epoch 9`, `to 3` |
| RESUME | `type 'kv'`, `slot 7`, `epoch 8` |
| SLOT rows | `SlotItem('kv', 7, SlotEntry.owned(2, holder: 2, epoch: 8))`, `…migrating(2, 3, holder: 2, epoch: 8)`, `…free(holder: 2, epoch: 8)`; and `SlotItem('x', 0xFFFFFFFF, SlotEntry.owned(0xFFFFFFFFFFFF, holder: 0x123456789ABC, epoch: 0xFFFFFFFF))` (48-bit and 32-bit maxima for JS) |
| MOVED 0x1A2B/7 | `MovedStatus(owner: 0x1A2B, epoch: 7).encode()` gives the bytes; `parse` gives it back; `hasOwner true`; `ownerOf('userq') == ServiceAddress('userq', 0x1A2B)` |
| RELOCATED 0x1A2B/7 | `encode(relocated: true)` gives the bytes; `parse` gives an *equal* `MovedStatus` (it does not record which of the two codes it came from); `toStatus(relocated: true).known == relocated`; `carriesFields(that) == true` |
| ABORTED with field-shaped body | `MovedStatus.parse(0A 00 2B 1A …)` == `MovedStatus.unknown`; `carriesFields(Status.of(aborted)) == false` |
| MOVED unknown | `MovedStatus.unknown.encode()` gives the bytes; `MovedStatus() == unknown`; `hasOwner false`; `ownerOf('userq') == null` |

#### 10b. Extra vectors and rules

| Case | Bytes / call | Expected | Test |
| --- | --- | --- | --- |
| **Truncation rule** | every vector above except the empty ASSIGN response and the `01` ASSIGN response | every strict prefix (lengths 0 to n−1) → **PE** | codecs › every structure: truncation is a protocol error, trailing bytes are ignored |
| **Trailing bytes ignored** | each vector followed by `EE 01` | decodes, and re-encodes to the original bytes without the trailer | same |
| ASSIGN response unknown flags | `FE` | `notHolding false` | codecs › unknown flag bits are ignored |
| CLAIM unknown flags | `6B 76 00 00 00 00 00 00 07 00 00 00 FE 00 00 00 00` | `holding false` (only bit 0 counts) | same |
| HOLDING response, count over the payload | `FF FF FF FF 01 00 00 00` | **PE** before reading the slots (no large allocation) | codecs › a slot list longer than the payload is refused before reading |
| Encode range: CLAIM slot over u32 | `ClaimRequest('kv', 0x100000000).encode()` | **RangeError** | codecs › values outside u32 and u48 are refused on encode |
| Encode range: owner over u48 | `SlotItem('kv', 1, SlotEntry.owned(0x1000000000000)).encode()` | **RangeError** | same |
| Encode range: negative epoch | `ClaimResponse(-1).encode()` | **RangeError** | same |
| Procedure names | | in order `SLOTS HOLDING CLAIM RELEASE LOCATE MIGRATE PHASE ASSIGN DRAIN FORWARD RESUME SLOTSPC SLOT`; `Procedures.slotSpace.bytes == 53 4C 4F 54 53 50 43 00` | codecs › procedure names |
| Status codes | | `moved.code 37`, `fromCode(37) == moved`, `relocated.code 38`, `fromCode(38) == relocated`, `fromCode(39) == null` | same |
| MOVED, largest values, reason "é" | `MovedStatus(owner: 0xFFFFFFFFFFFF, epoch: 0xFFFFFFFF, reason: 'é')` | `encode()` → `25 00 FF FF FF FF FF FF FF FF FF FF C3 A9`; `encode(relocated: true)` → `26 00 FF FF FF FF FF FF FF FF FF FF C3 A9`; parsing either gives the same MovedStatus | MOVED and RELOCATED status › largest values, a reason, and RELOCATED |
| Owner over u48 | `MovedStatus(owner: 0x1000000000000)` | **RangeError**, in the constructor | same |
| Epoch over u32 | `MovedStatus(epoch: 0x100000000)` | **RangeError** | same |
| Through Status, losslessly | `MovedStatus(owner: 0xFEDCBA987654, epoch: 0x80000001)`, with `relocated` false and true | `toStatus().known` is moved or relocated; `status.encode() == moved.encode(relocated)`; `fromStatus` gives the same MovedStatus back; `Status.decode(status.encode())` re-encodes identically, equals `status`, and `fromStatus` still works. **Derived** MOVED bytes: `25 00 54 76 98 BA DC FE 01 00 00 80` (the fields are not valid UTF-8, which tests lossless relay) | MOVED and RELOCATED status › through Status, losslessly |
| MOVED code with no fields | `MovedStatus.fromStatus(Status.of(moved))` (wire `25 00`) | `MovedStatus.unknown` | same |
| Fields under another code | the same bytes with byte 0 = `0E` (UNAVAILABLE). **Derived**: `0E 00 54 76 98 BA DC FE 01 00 00 80` | `fromStatus` → `MovedStatus.unknown` | same |
| **Hostile input never throws** | `` / `25` / `25 00` / `25 00 2B 1A 00 00 00 00 07 00 00` / `26 00 2B 1A 00 00 00 00 07 00 00` / `0E 00 2B 1A 00 00 00 00 07 00 00 00` / `0A 00 2B 1A 00 00 00 00 07 00 00 00` / `25 01 2B 1A 00 00 00 00 07 00 00 00` / `27 00 2B 1A 00 00 00 00 07 00 00 00` | `MovedStatus.parse` → `MovedStatus.unknown` for each: empty, 1 byte, code only, fields truncated by one byte (MOVED and RELOCATED), UNAVAILABLE, ABORTED, code 0x0125 = 293, code 39 | MOVED and RELOCATED status › hostile input never throws |
| Malformed UTF-8 reason | `25 00 2B 1A 00 00 00 00 07 00 00 00 FF` | `MovedStatus(owner: 0x1A2B, epoch: 7, reason: '�')` (lossy decode, no throw) | same |

Behaviour: none (the truncation and trailing-byte rules above are codec-wide).

---

### A.11 `vectors/slot_key_vectors_test.dart`

**Wiki coverage.** None on any wiki page. `sharding.md` and `dart-reference.md` name FNV-1a 32 but give no vectors.

| Case | Input | Expected | Test |
| --- | --- | --- | --- |
| FNV-1a 32 of empty | `[]` | `0x811c9dc5` | FNV-1a 32 vectors |
| `'a'` | `61` | `0xe40c292c` | same |
| `'foobar'` | ASCII | `0xbf9cf968` | same |
| `'user:42'` | ASCII | `0x2f6b7b82` | same |
| Bytes 0..255 | `[0, 1, …, 255]` | `0x90a458c5` | same |
| Elements taken modulo 256 | `[0x161]` | `== fnv1a32('a')` | same |
| Slot of a text key | `slotForText('user:42', 65536)` | `0x2f6b7b82 % 65536 == 31618` | slot of a key |
| One slot | `slotForKey('foobar', 1)` | `0` | same |
| UTF-8 of text | `slotForText('é', 7)` | `== slotForKey([0xC3, 0xA9], 7)` | same |
| Zero slots | `slotForKey([1], 0)` | **RangeError** | same |

The JS note is in the source: "the 32-bit multiplication must stay exact". Use `Math.imul` and `>>> 0`.

---

### A.12 Status truncation in `mux_limits_test.dart`

For `truncateStatus`, `statusForWire` and `fitStatus`: apart from `binding_protocol_test.dart` (section 8), the only tests of them are in `test/mux_limits_test.dart`, group *reasons* and the two groups after it. The helpers are defined in `lib/src/mux/mux_protocol.dart`.

The cut rule:

1. With `room = maxPayload − 2`, cut the encoded bytes at `2 + room`.
2. Back off over at most 3 continuation bytes (`10xxxxxx`), so the cut lands on a character boundary.
3. For MOVED and RELOCATED: if the cut is below `2 + 10`, return the bare code (`Status(code)`). The fields are never kept partially.
4. If the reason is empty or already fits, return the same object.

`fitStatus` caps the payload at `min(1024, peerMaxFrameSize − overhead)` and rewrites CONNECTION_LOST (34) to UNAVAILABLE (14), keeping the body bytes.

| Case | Input | Expected | Test |
| --- | --- | --- | --- |
| UTF-8 safe, 1-byte chars | `Status.of(UNAVAILABLE, 'x' × 2000)`, `truncateStatus(…, 1024)` | encoded ≤ 1024 and > 1023; body is valid UTF-8 equal to `.reason`; reason is only `x`. **Derived** length 1024 | reasons › truncation is UTF-8 safe |
| 2-byte chars | `'é' × 2000` | ≤ 1024 and > 1022. **Derived** 1024 | same |
| 3-byte chars | `'€' × 2000` | ≤ 1024 and > 1021. **Derived** 1022 | same |
| 4-byte chars | `'😀' × 2000` (a surrogate pair in UTF-16) | ≤ 1024 and > 1020. **Derived** 1022 | same |
| Short reason | `Status.of(UNAVAILABLE, 'ok')` | `truncateStatus(s, 1024)` is *identical* (same object); `truncateStatus(s, 2) == Status(14)` (reason dropped) | same |
| GOAWAY payload cap | `MuxControlMessage.goAway(Status.of(UNAVAILABLE, 'x' × 2000)).payload` | length 1024 (`maxControlPayload`) | same |
| MOVED fields kept | `MovedStatus(owner: 0xFEDCBA987654, epoch: 7, reason: '€' × 400).toStatus()`, `truncateStatus(…, 1024)` | length in 1022..1024; the first 12 bytes are unchanged (**derived** `25 00 54 76 98 BA DC FE 07 00 00 00`); `fromStatus` gives owner `0xFEDCBA987654` and epoch 7; reason only `€`. **Derived** length 1023 | reasons › truncation keeps fixed fields before the reason |
| MOVED, max 12 | `truncateStatus(moved, 12)` | the fields alone; `fromStatus(...).owner == 0xFEDCBA987654` | same |
| MOVED, max 11 | `truncateStatus(moved, 11)` | `== Status(37)` (bare code, no partial fields) | same |
| MOVED, max 2 | `truncateStatus(moved, 2)` | `== Status(37)` | same |
| RELOCATED | the same body under code 38 (`[38, 0, …moved body]`) | max 12 keeps the owner; max 11 gives `Status(38)` | same |
| RELOCATED across a CLOSE | `MovedStatus(owner: 0x80FF80FF80FF, epoch: 0x80808080).toStatus(relocated: true)` | the peer's `done.encode()` equals the sent bytes; `fromStatus` round-trips. **Derived** bytes `26 00 FF 80 FF 80 FF 80 80 80 80 80` | reasons › a RELOCATED status crosses a CLOSE byte for byte |
| Oversized GOAWAY | `goAway(Status.of(UNAVAILABLE, 'x' × 1100))` | the peer stays open (`peerGoingAway`); its final `done` is GOING_AWAY with a reason that starts with `unavailable: xxx` and is under 1100 UTF-8 bytes. GOING_AWAY reports the remote status's `toString()` as its reason | reasons › an 1100 byte GOAWAY reason does not break a conforming peer |
| CLOSE within the peer frame limit | acceptor `maxFrameSize: 64`; `close(Status.of(CANCELLED, 'y' × 100))` | the peer receives `Status.of(CANCELLED, 'y' × 59)` (64 − 3-byte header − 2-byte code); the local `done` is the full status | reasons › a CLOSE reason fits the peer frame limit |
| CONNECTION_LOST on CLOSE | `close(Status.of(CONNECTION_LOST, 'upstream gone'))` | the wire CLOSE carries `Status.of(UNAVAILABLE, 'upstream gone')`; locally `done` keeps CONNECTION_LOST | CONNECTION_LOST stays local › CLOSE sends UNAVAILABLE and retains the local status |
| CONNECTION_LOST on GOAWAY | `goAway(Status.of(CONNECTION_LOST, 'upstream gone'))` | the GOAWAY status is UNAVAILABLE 'upstream gone'; `done` keeps CONNECTION_LOST | … › GOAWAY sends UNAVAILABLE and retains the local status |
| Only the code changes | `Status.decode(22 00 FF 41)` sent in a CLOSE | the CLOSE payload is `0E 00 FF 41` | … › only the code changes: the bytes after it are relayed |
| Application code in a received CLOSE | the peer sends `22 02 00 2C 01 61` | the mux confirms with `22 02 00`; `channel.done == Status.of(UNKNOWN, 'code 300: a')`; relaying it in our own CLOSE works without ArgumentError | application codes from the peer › CLOSE: reported as UNKNOWN and relayable |
| Application code in a received GOAWAY | the peer sends `02 00 00 03 2C 01` | `mux.done == Status.of(GOING_AWAY, 'unknown: code 300')` | … › GOAWAY: reported as GOING_AWAY with the code |

---

### Counts

Extra vectors and assertions per file, not counting wiki rows:

| File | Count | Breakdown |
| --- | --- | --- |
| talk_vectors_test | 85 | 13 extra expectations on wiki rows, 17 positive vectors, 26 negative decode vectors, 11 encoder and helper vectors, 18 encode refusals |
| status_test | 19 | |
| bytes_test | 7 | |
| name_test | 7 | |
| address_vectors_test | 6 | |
| identity_vectors_test | 12 expectation groups and 5 vectors | |
| exporter_identity_vectors_test | 11 | |
| binding_protocol_test | 3 behaviour tests | 0 codec vectors |
| naming_vectors_test | 9 expectations and 8 vectors | |
| sharding_vectors_test | 25 expectation rows and 18 vectors or rules | |
| slot_key_vectors_test | 10 | |
| §12 (mux_limits_test) | 19 | |

### Things a port can trip on

1. **Empty status payloads differ by layer.** `Status.decode` of an empty payload is OK, and so is a mux CLOSE with no payload. An empty Talk ABORT payload is **UNKNOWN**.
2. **Status equality and hashing.** Equality compares the raw bytes after the code, not the decoded reason. `hashCode` uses only (code, reason). `reason` is a lossy UTF-8 view, with U+FFFD for malformed sequences, while `encode()` writes the original bytes back.
3. **Sharding decoders ignore trailing bytes.** The test appends `EE 01` to every vector and expects a clean round trip. Every strict prefix must still be a PE.
4. **`extendPayload` almost never throws.** It rounds up to whole milliseconds, writes 0 as 1 ms (0 means "unchanged"), and clamps anything above u32 to `0xFFFFFFFF`. Only a negative duration throws.
5. **MOVED and RELOCATED truncation is all or nothing.** The cut keeps all 10 field bytes or drops the whole body to a bare code. Partial fields never appear.
6. **The Dart test names "a missing procedure reads as the empty name", but the assertion is `procedure == null`.**
7. **`secureRandomBytes` must chunk past 65536 bytes.** That is the Web Crypto `getRandomValues` limit per call.
8. **Received application codes become UNKNOWN.** In CLOSE or GOAWAY, `statusFromWire` turns them into UNKNOWN with the reason `code N[: reason]`. A GOAWAY then surfaces locally as GOING_AWAY with `remote.toString()` as its reason.
9. **C2 depends on the issuer's default lifetime of 8 h.** The Ed25519 issue call passes no lifetime.

---

## 22. Port log (decisions as ported)

**PR #5 (merged): transports.** FRAME_TOO_LARGE is a `SwitchboardError`, not a `ProtocolError`. The codec keeps the Dart names (`StreamFraming`, `StreamFrameDecoder`). Frames are copied when the writable takes them. An output that fails means "output gone" (Dart `_onOutputCancel`): queued output is dropped and the readable is not failed. On close without linger, input reading stops at once, but the input is cancelled only after the output has finished, so the peer's pipe is not torn down under it.

**PR 2: mux codecs, IDENT, binding base.**
* `mux-frame.ts` and `mux-protocol.ts` keep the Dart split and names: `MuxFrame`, `MuxCommand`, `MuxControlMessage`, `MuxControlType`, `MuxLimits`, `MuxCredit`, `MuxIdent`, `verifyIdent`, `identReceiverProblem`, and the status rules. `ArgumentError` is `RangeError`; `ProtocolException` is `ProtocolError`. HELLO stays with the QUIC thread (`quic/hello.ts`), as in Dart. The control messages live in `test/vectors/mux.test.ts`, as in `mux_vectors_test.dart`, not in a separate `control.test.ts`.
* There is no `ident-proof.ts`: the proof message is `MuxIdent.proofMessage`, as in Dart.
* `verifyIdent(ident, { verifier, senderNonce, receiverNonce, localIdentity, requireNamedIdent, exporter, now })`. `now` is wall time in ms since the epoch: it becomes `verifiedAt` and is passed to the verifier. The default is `Date.now()`, because the injected `Clock` is monotonic.
* Ed25519 runs over WebCrypto. The seed is imported as PKCS#8, and the public key is read from a JWK export. The signing key is then re-imported as non-extractable. RFC 8032 tests 1 to 3 and the vectors P and X are checked byte for byte.
* `BindingConnection` is the Dart base: drain holds and neutral diagnostics. The QUIC binding compiles and passes its tests against it, with its stubs swapped for the core modules (checked on a scratch copy of its branch). The identity exchange, `adoptIncoming` and `admitAfterGoAway` stay per binding, as in Dart. The QUIC binding already has its own, with exporter binding and held control messages, and the frame mux will have its own, with held frames.
* `fitStatus` is exported from the module but not from the package (Dart `@internal`).
* Until the credential codec lands, `test/support/identity/stub-verifier.ts` stands in for the verifier. It knows credentials by their bytes.

**Frame mux tests (branch `claude/mux-tests`).**
* Time is a `ManualClock` stepped with `elapseAsync`, not `advance`: the mux re-arms its keep-alive, grace and close timers in promise continuations (the read loop, the close sequence), and `elapseAsync` lets those run between two timers, as `fake_async` does. A test that never elapses fires no timer. (This was a `SteppingClock` of the tests' own until the fixtures branch below.)
* The memory pair hands a frame over as the writable takes it and the mux reads it a microtask later; the Dart tests whose peer answers during a write use `AnsweringTransport`, which queues the answer during the write. A `GatedTransport` must count frames as the mux writes them, so it hooks the writer the mux takes: a `WritableStream` calls its sink at once only for the first of several writes in one turn.
* Credit goes back per read: Dart's listener takes every buffered subframe and returns one batch, here credit goes as soon as half the window is consumed (the flow tests count 500 where Dart counts 600, 6000 where Dart counts 10000). A paused Dart subscription is a reader that does not read; a read already waiting takes the next subframe.
* `FrameMuxChannel.readable` and `writable` are made on first use. A writable made just before a write hands that write over a turn later, so a `send()` in the same turn goes first.
* The connection keeps at most 1024 writes in flight to the transport and queues the rest itself, in order: Node's `WritableStream` dequeues with `Array#shift`, quadratic beyond some 16K queued writes. While frames wait there, the scheduler holds DATA as for a transport that is not ready; close hands them over before closing.
* A `writer.write()` promise of a channel's writable is the stream's, not the library's: dropped, it reports an unhandled rejection when the channel stops sending (item 8 of design §12). Code ported from Dart's fire-and-forget `sink.add` should use `send()` or catch.
* `MuxChannel` declares `[Symbol.asyncIterator]` (design §2 and §6.3): `readSubframes` in `mux-protocol.ts`, over a reader rather than the readable's own iterator, which not every browser has. A `break` cancels the readable (item 10): the channel stays open.
* A `StreamTransport` whose output ends on its own (the peer's FIN, a failure, an abort) errors its writable with the reason, so that `writer.closed` settles: a WritableStream cannot be closed from its sink. Dart drops later writes silently; here they are refused (a close of the errored stream with a TypeError).

**Transports and fixtures (branch `claude/transports-fixtures-medczm`, third round).** The small items of `docs/handoff.md` section 7.
* `TcpTransport.bufferedOutputBytes` after an abort: a destroyed `net.Socket` drops its buffer, but `writableLength` keeps counting it until the handle closes, several turns later (measured on Node 22: still counted after `setImmediate`, 0 in the `close` listener; Bun zeroes it at once). The transport added `writableLength`, so it reported buffered bytes after the abort, and `acceptedOutputBytes` (queue taken minus `writableLength`) then jumped up by the dropped bytes, counting them as sent. A `SocketBacklog` in `tcp.ts` notes `writableLength` when the transport destroys the socket (its abort hook, the output's abort, the input's cancel) and from the socket's `error` event (a failure destroys the socket a turn before it reports the error, before the handle closes); from then on nothing is buffered and the noted bytes stay unsent. Left: a write that fails (EPIPE) is subtracted from `writableLength` before the destroy, so those bytes count as accepted; nothing reads the count after a failure.
* The memory pair's writable now settles when the other side closes: its readable's end errors the writable (CONNECTION_LOST, or the FRAME_TOO_LARGE that closed the pair on that side's behalf), as design section 12 item 11 has it for the stream transport. Frames written while a delayed close is still on its way are dropped, as before; after the error, writes reject. The port of "closing one side ends both streams and drops later frames" asserts the rejection where Dart's sink drops silently. No other suite noticed: the mux, Talk and node tests already handle an errored writable from the stream transport.
* `ws` 8.21 added `maxFragments` (default 16384 on a server; `maxBufferedChunks` beside it), so Dart's `maxMessageFragments` (4096) is now `ws-server.ts`'s `maxMessageFragments`, passed to the server, and `ws-common.ts` reads `WS_ERR_TOO_MANY_BUFFERED_PARTS` (shared by both limits) as FRAME_TOO_LARGE. Differences from Dart, in the test and the doc comment: `ws` closes with 1008, Dart with 1009 (`ws` closes before it emits the error, so the code cannot be changed from outside); `ws` counts a fragment after reading its payload, Dart on its header. The peer dependency is `ws ^8.18.0`, and an older `ws` ignores the option: no cap there. Raising the range to `^8.21.0` would make the cap certain; that is `package.json`, so it is left to the lead. Dart's "costs about its size" checks its own message buffers (`messageBuffersForTesting`); `ws` keeps a view of the socket's chunk per fragment and shows nothing to count, so only the deliveries are ported. Bun's stand-in for `ws` has no fragment cap (the cap case is skipped there; the many-fragments case passes).
* One mux harness: `test/support/identity/mux-harness.ts` is gone; the identity tests use `test/unit/mux/support.ts` (where the vector tests already take it from). The harness's pair helpers take any `Clock`; the identity tests pass the system clock, as Dart's run on real time (WebCrypto work, real identity timeouts). `control()` is local to the identity test, as in Dart.
* `SteppingClock` and the naming tests' `TestClock` are gone: `ManualClock.elapseAsync` (and `pendingCount`, `ManualClock.settle`) replace them. It settles three event-loop turns between timers where they settled two and one; every suite passes unchanged (and on Bun). `mesh.test.ts`'s `ManualWallClock` hooked `advance()` to move a faked system time; `elapseAsync` does not call `advance`, so `ManualWallClock` spies on `Date.now` to read the clock instead. `TimerClock` (Talk tests, it lists the pending delays) stays.
* "a forged IDENT gets GOAWAY UNAUTHENTICATED" (unit) flips the last byte of a valid credential, as Dart does; the helper is `forged()` in `test/support/node/seeded-identity.ts`, shared with the integration port. `test/support/identity/stub-verifier.ts` stays for the mux IDENT tests.
* Item 7 of the work split (the close-scenario races of CI run 157) went to the interop thread with the lead's hand-off, not here.
