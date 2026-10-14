# Contributing

## Tooling

- TypeScript 7, `tsc` for the build, `module: NodeNext`, `.ts` import extensions, strict with `exactOptionalPropertyTypes` and `noUncheckedIndexedAccess`, `verbatimModuleSyntax`, `erasableSyntaxOnly`.
- Vitest for tests, Biome for lint and format (single quotes, 100 columns, no `any`, `type` imports).
- `npm run check` before every push: lint, typecheck, unit and integration tests.

## Conventions

- `src/core` is runtime neutral: no `node:` imports, no `ws`, no DOM-only APIs. Runtime specifics live in `src/node` and `src/web`.
- Each layer's public interface is in its `contract.ts`; implementations export from the directory's `index.ts`.
- The Dart reference implementation defines the behaviour; its tests are the checklist, ported one to one and named after the Dart file. The wiki defines the wire; its vectors are asserted by id.
- Every exported symbol has a JSDoc line. The source header comment is the one in the existing files.
- `const` objects instead of enums; options objects with frozen defaults and a merge helper; `RangeError` for misuse.
- No `console`; use the `logger` option. No global timers; use the `clock` option.
- Internal promises a caller may drop are pre-caught so nothing is an unhandled rejection.

## Branches and reviews

During the port, work lands on the integration branch `claude/switchboard-ts-design-s5r9na` through pull requests from per-layer branches, reviewed and merged by the lead in dependency order; `main` is initialised from it at the first green end-to-end state. Afterwards, pull requests target `main`.

Commit on every milestone, with clear messages and the co-author attribution the project uses. The Dart repositories are not changed from this project.
