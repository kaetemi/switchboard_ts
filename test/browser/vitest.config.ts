import { fileURLToPath } from 'node:url';
import { defineConfig } from 'vitest/config';
import type { BrowserName } from './harness.ts';

// The browser tests (`npm run test:browser`): the web provider and the
// binding in Playwright's headless Chromium and Firefox, one project each
// (`--project chromium`, `--project firefox`), against native WebTransport
// listeners in the test process, the page served by Vite. Without the native
// addon, Playwright or a project's browser that project skips with the
// reason; SWITCHBOARD_BROWSER=required (CI) fails instead. The projects run
// one after the other (each starts its own server and browser). Kept apart
// from the root configuration's projects, which `npm test` runs.
const project = (browserName: BrowserName) => ({
  extends: true,
  test: { name: browserName, provide: { browserName } },
});

export default defineConfig({
  root: fileURLToPath(new URL('../..', import.meta.url)),
  test: {
    include: ['test/browser/**/*.test.ts'],
    testTimeout: 30_000,
    hookTimeout: 60_000,
    fileParallelism: false,
    projects: [project('chromium'), project('firefox')],
  },
});
