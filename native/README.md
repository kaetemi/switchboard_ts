# Native QUIC provider

The N-API addon that gives `@polyverse/switchboard/node` raw QUIC and WebTransport on Node.js,
Bun and Deno. The addon is a thin binding to the QUIC shim of `switchboard_quic` (`sbq`, Rust on
quinn and h3); the provider and every protocol rule above it are TypeScript
(`src/node/quic/native-provider.ts`, a port of `switchboard_quic`'s `native_provider.dart`). See
`docs/design.md` section 3.1 and `docs/work-split.md` thread 5.

```
native/
  Cargo.toml, Cargo.lock   the napi-rs crate `sbq-node` (cdylib); the lock file is committed
  build.rs                 napi-build
  src/lib.rs               the addon: the sbq C ABI, function for function
  build.mjs                `npm run build:native`: cargo build, then the copy to sbq-node.node
  sbq-vendor/              switchboard_quic's native/sbq at the pinned commit (see below)
  sbq-vendor.commit        that commit
  npm/<target>/            per-platform package manifests, published as optionalDependencies (to come)
```

## Building locally

You need Rust (`switchboard_quic` and CI use 1.99.0; the crate needs at least 1.88) and Node.js.

```sh
npm run build:native            # cargo build --release --locked, then native/sbq-node.node
node native/build.mjs --debug   # the dev profile: faster to build, much slower to run
node native/build.mjs --target aarch64-unknown-linux-gnu --out /tmp/sbq-node.node
```

`build.mjs` copies what cargo wrote (`libsbq_node.so`, `libsbq_node.dylib` or `sbq_node.dll`,
under `$CARGO_TARGET_DIR` when it is set) to `native/sbq-node.node`, which git ignores. Then:

```sh
npx vitest run --project integration test/integration/quic-native.test.ts
```

Without the addon that file skips its cases and names the reason; `SWITCHBOARD_NATIVE=required`
(as in CI) makes a missing addon fail it instead.

The provider loads the addon on first use, never at import, and tries in turn:

1. `$SWITCHBOARD_QUIC_ADDON`, when it is set (and then nothing else);
2. `native/sbq-node.node` in this package (the local build);
3. the per-platform package `@polyverse/switchboard-quic-<target>` (table below).

When none loads, connecting rejects and listening fails with
`QuicError('unsupportedScheme', ...)`, whose message names the package for this platform;
`nativeQuicAvailability()` says why without trying to connect. The rest of the library works
without the addon. Bun loads it the same way; Deno needs
`--allow-ffi --allow-read --allow-env`.

## Supported targets

The prebuilt targets follow `switchboard_quic`'s release workflow:

| Platform            | Rust target                  | Package                                       |
| ------------------- | ---------------------------- | --------------------------------------------- |
| Linux x64 (glibc)   | `x86_64-unknown-linux-gnu`   | `@polyverse/switchboard-quic-linux-x64-gnu`   |
| Linux arm64 (glibc) | `aarch64-unknown-linux-gnu`  | `@polyverse/switchboard-quic-linux-arm64-gnu` |
| macOS arm64         | `aarch64-apple-darwin`       | `@polyverse/switchboard-quic-darwin-arm64`    |
| macOS x64           | `x86_64-apple-darwin`        | `@polyverse/switchboard-quic-darwin-x64`      |
| Windows x64         | `x86_64-pc-windows-msvc`     | `@polyverse/switchboard-quic-win32-x64-msvc`  |

Windows builds link the C runtime statically, as `switchboard_quic` does, so that the addon needs
no Visual C++ redistributable:

```sh
CARGO_TARGET_X86_64_PC_WINDOWS_MSVC_RUSTFLAGS="-C target-feature=+crt-static" npm run build:native
```

On musl Linux the loader looks for `-linux-<arch>-musl`, for which nothing is prebuilt; build it
locally. Only Linux x64 is built and tested in CI (the `native` job).

## How `sbq` is reused

The addon calls the shim Rust to Rust, which needs the `rlib` crate type that `switchboard_quic`'s
`native/sbq/Cargo.toml` does not declare (it builds `cdylib` and `staticlib`). Until that line
lands upstream, `native/sbq-vendor/` is a copy of `native/sbq` at the commit in
`native/sbq-vendor.commit`, without `target/` and `prebuilt/`, and with one change: `"rlib"` added
to `crate-type`. Nothing else in the copy is edited, and the pin lives outside it, so that the
copy can move as it is to a shim repository shared with the Dart package; fixes go upstream first.

```sh
node tool/sync-sbq.mjs [<switchboard_quic checkout>]          # copy again at the pin
node tool/sync-sbq.mjs --commit <rev> [<checkout>]            # move the pin, then copy
node tool/sync-sbq.mjs --check [<checkout>]                   # exit 1 when the copy differs
```

The checkout defaults to `$SWITCHBOARD_QUIC`, else `../switchboard_quic`; it only needs to
contain the pinned commit. CI checks out `kaetemi/switchboard_quic` at the pin and runs
`--check`, so the copy cannot drift; the job's `ref` must change with the pin. Once upstream
builds an `rlib` (or the shim has a repository of its own), the copy gives way to a git
dependency at the same commit.

## The addon's JavaScript surface

Every function of `sbq.h` (ABI version 4) has a camelCase counterpart taking the same arguments
in the same order; `src/node/quic/native-provider.ts` declares them (`SbqAddon`). The addon keeps
no state of its own beyond each context's doorbell, and knows nothing of Switchboard.

- Handles, counts, codes, lengths and durations (milliseconds) are numbers; handles are small
  table ids.
- A result is the integer the C function returns: a number, or a BigInt when its magnitude
  exceeds 2^53 - 1 (only the code of a reset or STOP_SENDING at or above 2^53 - 16 does), so no
  result is ever rounded.
- A buffer in is a `Uint8Array` (any view, Node `Buffer` included) the shim copies during the
  call. A buffer out is a `Uint8Array` the call fills, its length the capacity; calls that return
  a string or bytes of unknown length return the full length, so the caller can retry with a
  larger buffer.
- `poll(ctx, events)` fills a `Float64Array` with events, four numbers each (kind, handle, a, b),
  and returns how many it wrote; `connStats` fills a `Float64Array` of nine.
- `init(doorbell, workerThreads)` starts a context. The doorbell is a non-blocking thread-safe
  function the shim's notifier thread queues a call to, with the context id, when events become
  pending; the provider then polls until a batch comes back short. The doorbell never waits on
  JavaScript, so `shutdown(ctx)` may be called synchronously at any time.
- `setActive(ctx, active)` decides whether the doorbell holds the event loop open. The provider
  sets it while it has a live session or listener, so a process exits once everything is
  closed, and shuts the context down when the last one ends.
- A worker thread or process that ends shuts down the contexts it started and left open.
