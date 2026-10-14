import { defineConfig } from 'vitest/config';

// Four projects: `unit` (fast, in-process, every runtime), `integration`
// (sockets on the loopback, two nodes end to end), `interop` (the Dart
// reference implementation as a peer; needs a Dart SDK and skips without
// one) and `browser` (the web provider in headless Chromium; needs
// Playwright's Chromium and skips without it). `npm test` runs the first
// two; `npm run test:interop` and the CI run the others explicitly.
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
