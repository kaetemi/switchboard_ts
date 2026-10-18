# Contributing

## Tooling

- TypeScript 7, `tsc` for the build, `module: NodeNext`, `.ts` import extensions (rewritten on emit), strict with `exactOptionalPropertyTypes` and `noUncheckedIndexedAccess`, `verbatimModuleSyntax`, `erasableSyntaxOnly`.
- Vitest 5 for tests, Biome 2 for lint and format (single quotes, 100 columns, no `any`, `type` imports).
- Rust (1.88 or later; CI uses 1.99.0) only for the native QUIC addon (`npm run build:native`); a Dart SDK and Rust for the interop peer; Playwright's Chromium and Firefox for the browser tests.

Before every push:

```sh
npm run check                             # lint, typecheck of src, unit and integration tests
npx tsc -p tsconfig.test.json --noEmit    # the tests type-check too (CI does not run this one)
```

and, where the change touches them, `npm run test:browser`, `npm run test:interop` and the native QUIC files (`SWITCHBOARD_QUIC_ADDON=... npx vitest run --project integration test/integration/quic-*.test.ts`). See [Testing and Interop](Testing-and-Interop).

## Conventions

- `src/core` is runtime neutral: no `node:` imports, no `ws`, no DOM-only APIs. Runtime specifics live in `src/node` and `src/web`.
- Each layer's public interface is in its `contract.ts`; implementations export from the directory's `index.ts`. A contract change is its own commit with a short rationale.
- The Dart reference implementation defines the behaviour; its tests are the checklist, ported one to one and named after the Dart file (`mux_flow_test.dart` becomes `mux-flow.test.ts`). A Dart test that cannot be ported is left as `// Not ported: <reason>` or `test.todo` with the reason, never skipped silently. The wiki defines the wire; its vectors are asserted by id.
- Every exported symbol has a JSDoc comment. The source header comment is the one in the existing files.
- `const` objects instead of enums; options objects with frozen defaults and a merge helper; `RangeError` and `TypeError` for misuse, plain `Error` where Dart has `StateError`; `process.env['X']` (bracket access to index signatures).
- No `console`; use the `logger` option. No global timers; use the `clock` option.
- Test doubles are named by what they are or do, never by their status as stand-ins: `Memory*` for an in-memory implementation of a contract (`MemoryQuicNetwork`, `MemoryMuxConnection`), an adjective for a double defined by one behaviour (`HeldVerifier`, `ScriptedEndpoint`, `GatedTransport`). Catch-all labels for test doubles are not used in code, comments or documents ("stub" is fine); time a test controls is virtual time. Third-party names stay as they are. The Dart repositories share the rule and the names (`docs/design.md`, section 9).
- Internal promises a caller may drop are pre-caught so nothing is an unhandled rejection.
- Every observable difference from Dart is listed in `docs/design.md`, section 12.

## Branches and reviews

`main` is the integration branch. Work happens on topic branches and lands on `main` with real merge commits (no squash, no rebase), after `npm run check` and the test typecheck pass on the merged tree; keep a branch's history granular, one commit per milestone, and continue a merged branch from `main` with a merge. Commit messages explain what and why, and carry the co-author attribution the project uses; nothing about hosts or credentials goes in them. The Dart repositories are read-only from this project: findings about them are reported, not patched here.

The documentation lives in the repository: `docs/design.md` (the design and its decisions), `docs/handoff.md` (the state of the port), and these pages under `docs/wiki/`, written as GitHub wiki Markdown so they can be pushed to the wiki as they are.
