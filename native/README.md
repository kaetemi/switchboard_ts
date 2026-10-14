# Native QUIC provider

The N-API addon that gives `@polyverse/switchboard/node` raw QUIC and WebTransport on Node.js, Bun and Deno. Owned by the "Port QUIC transport" thread; see `docs/design.md` section 3.1 and `docs/work-split.md` thread 5.

Planned layout:

```
native/
  Cargo.toml          the napi-rs crate, mirroring the sbq C ABI function for function
  src/lib.rs
  sbq-vendor/         switchboard_quic/native/sbq at the commit in sbq-vendor/COMMIT, crate-type plus "rlib";
                      refreshed by tool/sync-sbq.mjs, checked in CI; no other edits, so it can move to a
                      dedicated shim repository shared with the Dart package once the port is ready
  npm/<target>/       per-platform package manifests published as optionalDependencies
```

Build locally with a Rust toolchain (`cargo`, 1.99 as `switchboard_quic` pins) and `npm run build:native` once the crate exists. Without the addon, QUIC endpoints report `UNSUPPORTED` and the rest of the library works.
