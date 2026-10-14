import { defineConfig } from 'vitest/config';

// Three projects: `unit` (fast, in-process, every runtime), `integration`
// (sockets on the loopback, two nodes end to end) and `interop` (the Dart
// reference implementation as a peer; needs a Dart SDK and skips without
// one). `npm test` runs the first two; `npm run test:interop` and the CI run
// the third explicitly. The browser tests (the web provider in headless
// Chromium) have their own configuration, test/browser/vitest.config.ts,
// run by `npm run test:browser`.
export default defineConfig({
  test: {
    testTimeout: 10_000,
    hookTimeout: 20_000,
    projects: [
      {
        extends: true,
        test: {
          name: 'unit',
          include: ['src/**/*.test.ts', 'test/unit/**/*.test.ts', 'test/vectors/**/*.test.ts'],
        },
      },
      {
        extends: true,
        test: {
          name: 'integration',
          include: ['test/integration/**/*.test.ts'],
          testTimeout: 30_000,
        },
      },
      {
        extends: true,
        test: {
          name: 'interop',
          include: ['test/interop/**/*.test.ts'],
          globalSetup: ['test/interop/global-setup.ts'],
          testTimeout: 120_000,
          hookTimeout: 180_000,
        },
      },
    ],
  },
});
