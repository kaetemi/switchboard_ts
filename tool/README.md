# Maintenance scripts

- `extract-wiki-vectors.mjs` (interop thread): regenerates `test/vectors/wiki-vectors.json` from a local checkout of the Switchboard wiki (`kaetemi/sway`, `wiki/switchboard/`), recording the commit inside.
- `dart-peer.mjs` (interop thread): builds or locates the Dart test peer.
- `sync-sbq.mjs` (QUIC thread): copies `switchboard_quic/native/sbq` at a pinned commit into `native/sbq-vendor/`.
