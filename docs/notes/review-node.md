# Review: the node and the clients

Review of `src/core/{switchboard,client,identity}/**`, `src/{node,web}/switchboard.ts` against the
Dart reference (pinned 6ed87b4), `dart-design.md` and design sections 5, 6.4, 6.7, 6.9, 7 and 12.
Every finding has a test under `test/unit/<layer>/review-*.test.ts`
marked `test.fails` (passes while the defect is present, so `npm run check` stays green; the fix
removes `.fails`), run 10 times and checked to fail on its intended assertion.

**Status after the fixes (`claude/review-node`, one commit per finding):** all nine are fixed and their cases are plain tests (no `test.fails` is left); the design lists the one deviation the review turned up (section 12, item 17). Each finding says how below. The three unverified suspicions stayed open and are recorded in `docs/notes/node.md`.

## Findings, most severe first

1. **A wildcard listener of the Node entry is not recognised by the machine's host name.**
   Wrong behaviour (a forwarding loop). `src/node/switchboard.ts:608` (`hostsOf`) adds the
   interface addresses of a `0.0.0.0`/`::` bind but not `os.hostname()`, although the core's JSDoc
   (`switchboard.ts:857`, `:886`) promises it and Dart's `_hostsOf` adds `Platform.localHostname`.
   Path: a node listens on `0.0.0.0` and advertises `tcp://<hostname>:p` (what the `listenTcp` doc
   tells it to do); a record of its own reaches its proxy, relay or slot gate forwarding, whose
   `excludeOwnEndpoints` misses it, so it would connect to itself and forward the channel again
   (bounded only by `maxChannelsPerConnection` on that connection); `localConnections` dials a
   socket instead of the pair (the loop is inferred; `isOwnEndpoint` is tested). Test:
   `switchboard/review-wildcard-hostname.test.ts`, "a wildcard TCP listener is recognised by the host
   name". Fix: add `canonicalHost(hostname())` for wildcard binds. Dart is correct.
   **Status: fixed.** `hostsOf` adds `canonicalHost(hostname())` for a wildcard bind, as the core's JSDoc and Dart do.

2. **`ReconnectingClient.connected` resolves with a connection a state listener left.**
   Wrong behaviour. `src/core/client/reconnecting-client.ts:869-870`: `#onConnection` emits
   `connected` (synchronously, `:695`) before it resolves `#connected`. A listener that calls `stop()`
   or `reconnectNow({ force: true })` on `connected` runs `#halt`/`#leave` while `#connected` is still
   unsettled, so it is not replaced; `#onConnection` then resolves it with the left connection, and
   the next connection cannot resolve it: `await client.connected` hands out a connection that was
   sent GOAWAY until the next disconnect. Path: an app pausing or forcing a reconnect from its
   state listener. Tests: `client/review-state-reentrancy.test.ts`,
   "stop() from a connected listener, then start(): connected is the new connection" and
   "reconnectNow({ force }) from a connected listener: connected is the replacement". Fix: resolve
   `#connected` before `#setState`, or resolve only if `this.#connection === connection` afterwards.
   Dart: no (its `states` is an asynchronous broadcast stream). The synchronous `on('state')` is not
   in design section 12 (item 16 lists `PeerSet` only); list it, with the guard.
   **Status: fixed.** `#onConnection` resolves `connected` before the state event is emitted, so a listener that stops or forces a reconnect replaces it; the synchronous `on('state')` is design section 12, item 17.

3. **Credential expiry has no single wall clock.** Wrong behaviour where wall time is injected.
   `CredentialVerifier({ wallTime })` checks expiry at the time it is given, but
   `MuxConnection.peerIdentity` (`src/core/mux/frame-mux-connection.ts:507`,
   `src/core/quic/quic-binding-connection.ts:506`) calls `isExpired()`, i.e. `Date.now()`, and
   `MuxOptions` has no wall time. Path: a node whose verifier runs on another wall time (a trusted
   time source, virtual time in a test) accepts an IDENT and then treats the peer as unidentified:
   `requireIdentity`/`scoped` hold and refuse its channels, the brokered pool re-brokers it. Test:
   `identity/review-wall-time.test.ts`, "a peer identified under the verifier's wall time stays
   identified". Fix: a `wallTime` in `MuxOptions` (default the identity verifier's) for
   `peerIdentity` (a mux contract change); the suites mock `Date.now` today. Dart reads one clock
   (`package:clock`) in both places.
   **Status: fixed.** `MuxOptions.wallTime` (optional; a contract change) is what `peerIdentity` reads on both bindings, and the IDENT's verification when it is given; a node takes its verifier's `wallTime` when its mux options give none, and `MeshNode`'s renewal defaults to the node's wall time. Design section 6.4 and section 11 ("Wall time").

4. **An open aborted with a `SwitchboardError` reason rejects UNAVAILABLE.** Wrong status.
   `src/core/switchboard/switchboard.ts:2416`, `:2435`, `:2454` (`#connectFirst`) and `:2542`
   (`#connectRelay`) treat every `SwitchboardError` from `connect`, the signal's reason included, as
   a failed endpoint and go on to the next endpoint and instance, each re-thrown by `checkAborted`:
   the caller gets "no reachable instance (...: cancelled ...)" instead of `signal.reason`
   (`OpenChannelOptions.signal`). Path: `controller.abort(SwitchboardError.of(CANCELLED))`, as the
   library aborts its own attempts. Test: `switchboard/review-abort-reason.test.ts`, "an open aborted
   with a SwitchboardError reason rejects with that reason". Fix: in those catches, rethrow when
   `options.signal?.aborted`.
   **Status: fixed.** Each of those catches rethrows the signal's reason once the signal has aborted.

5. **The default runtime's in-process transport pairs ignore the node's clock.** Timer misuse.
   `src/core/switchboard/core-runtime.ts:22` builds `memoryTransportPair({ delay: 0 })` on the system
   clock, and `SwitchboardRuntime.transportPair` takes no clock, so a `Switchboard({ clock })`
   delivers its `mem://` and own-listener (`localConnections`, the default) frames on real timers
   while its mux, pool and identity timers run on the injected clock. Design section 5: every timer
   from the `Clock` of the options. A Node-entry user cannot supply a clock-aware runtime without
   losing the `tcp`/`ws` dialers (`nodeRuntime` is not exported); the suites use `steppingRuntime`.
   Test: `switchboard/review-runtime-clock.test.ts`, "a node on a manual clock delivers in-process
   frames on that clock". Fix: `transportPair(clock)` in the runtime, the node passing `this.clock`.
   Dart: no (`MemoryTransport` uses `Timer`, which `fake_async` drives).
   **Status: fixed.** `SwitchboardRuntime.transportPair(clock)` (a contract change) and the node passes its clock; the node suites' harness runtime keeps real timers, which their manual clocks rely on.

6. **An endpoint declared while `close()` starts outlives the close.** Wrong behaviour, low.
   `src/core/switchboard/switchboard.ts:759-790` checks open, awaits `hostsOf`, then pushes
   (`:783`); a `close()` during that await clears the lists first, so the endpoint stays in
   `listeningEndpoints`, `isOwnEndpoint` holds and `canBroker` is true on the closed node, and the
   call resolves (the QUIC listeners recover through `#startQuic`'s `isClosing` check). Test: `switchboard/review-listening-close.test.ts`, "an endpoint declared
   while close() starts is not the closed node own". Fix: push before the await (as Dart does) or
   re-check `#closing` after it. Dart is correct.
   **Status: fixed.** The endpoint is recorded before the await and the hosts are added after it, as Dart does.

7. **`adopt` of a connection whose `incoming` is read elsewhere is not refused.** Misuse not
   reported, low. `src/core/switchboard/switchboard.ts:1077`, `:1108`: `#dispatchAll` fails on the
   queue's second consumer and logs it at debug; `adopt` resolves and nothing is ever dispatched.
   Dart throws `StateError`. Test: `switchboard/review-adopt-misuse.test.ts`, "adopting a connection
   whose incoming is read elsewhere is refused". Fix: take `incoming`'s iterator in
   `#adoptConnection` and throw (plain `Error`) when it already has a consumer, before adopting.
   **Status: fixed.** `adopt` takes `incoming`'s iterator before it changes anything and rejects with a plain `Error` when it has a consumer already; the connection is not adopted.

8. **`identifyOn` and `resolveRelays` take no `AbortSignal`.** Ergonomics. `switchboard.ts:666`,
   `:2484`: both wait (NONCE and PONG up to `identityTimeout`; the resolver) but cannot be abandoned,
   against design section 5 ("connect, open, identify, ping, request, resolve") and section 12 item
   7, while `MuxConnection.identify` takes one. Test: `switchboard/review-identify-signal.test.ts`,
   "identifyOn rejects with the reason of an aborted signal, sending nothing". Fix: a `signal`
   passed to `identify` and to `resolve`/`abortable`.
   **Status: fixed.** `identifyOn` and `resolveRelays` take a `signal`; an aborted one rejects with its reason, sending nothing.

9. **Two Dart cases neither ported nor marked.** Tests. `outgoing_test.dart` "the proxy forwards
   the client payload, never the hook or the default" and "credentialFor and the MOVED retry: each
   owner gets its own key" are missing from `test/unit/switchboard/outgoing.test.ts` without a
   `Not ported` note. Ported one to one in `switchboard/review-outgoing-gaps.test.ts`; both pass (a
   gap only); move them into `outgoing.test.ts` (and its `over-frame-mux/` run).
   **Status: done.** Both cases are in `test/unit/switchboard/outgoing.test.ts` (and so in its `over-frame-mux/` run); `review-outgoing-gaps.test.ts` is gone.

## Unverified suspicions

- `PeerCore.readopt` reports `online`/`offline` synchronously, then calls `update`: a listener closing
  the set on that event gets `updated` after `removed`. Contrived; not tested.
- A `Logger` that throws inside the node's catch paths (`#dispatchAll`, `#holdForIdentity`, the
  proxy's `forward`) turns a `void` promise into an unhandled rejection; `MeshNode` guards its logger.
- The Node entry's `close()` closes the QUIC listeners while the core's GOAWAY waits run; interop
  reads GOING_AWAY over QUIC, so the shim seems to keep accepted sessions (unchecked).

## Fine, but surprising

- The port is line for line with Dart in dispatch, policies, the pool, `#dial` and identification,
  brokering, the proxy's pipe and bulk ordering, the relay, `PeerSet`'s state machine, `MeshNode`'s
  leave and renewal, `Backoff`, `Credential`, `Scope` and the verifier's cache. Probes found no timer
  left after `close()` with a hanging dial, a channel held for identity, a pending `authorize` or a
  relay hold, and a peer removed while dialling gets its late connection sent GOAWAY.
- `openChannelOn` throws `TypeError` for an asynchronous `credentialFor` (Dart `StateError`, which
  the convention maps to a plain `Error`); `IncomingChannel.talk(options)` merges with the node's
  options where Dart replaces them.
- `addListeningEndpoint`/`listenMemory` document `@throws RangeError` but reject (as Dart's futures
  do); `Ed25519HolderKey.generate()` throws synchronously without WebCrypto; `#broker` draws its
  intent with `crypto.getRandomValues`, not `secureRandomBytes`.
