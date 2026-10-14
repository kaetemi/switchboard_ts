import { fileURLToPath } from 'node:url';
import { defineConfig } from 'vitest/config';

// The browser tests (`npm run test:browser`): the web provider and the
// binding in Playwright's headless Chromium, against native WebTransport
// listeners in the test process, the page served by Vite. Without the
// native addon, Playwright or its Chromium the file skips with the reason;
// SWITCHBOARD_BROWSER=required (CI) fails instead. Kept apart from the root
// configuration's projects, which `npm test` runs.
export default defineConfig({
  root: fileURLToPath(new URL('../..', import.meta.url)),
  test: {
    name: 'browser',
    include: ['test/browser/**/*.test.ts'],
    testTimeout: 30_000,
    hookTimeout: 60_000,
  },
});
