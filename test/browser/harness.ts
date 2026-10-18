// The Node side of the browser tests: the server of `web-server.ts`, Vite
// serving the page (`page/index.html`, whose module imports the web entry
// point from the sources), and Playwright's Chromium or Firefox driving it
// (the project's `browserName`, see `vitest.config.ts`). A test's
// body is a function of the page's API (`page/page.ts`) that the harness
// sends to the page as source and runs there: it may use its parameters
// only, never a binding of the test file (Vitest rewrites those, and the
// page has none of them).
//
// Playwright is resolved at run time through `require`, so that the global
// install of a container (NODE_PATH) serves as well as the dev dependency;
// its types are the few members used here.
import { existsSync } from 'node:fs';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import { createServer } from 'vite';
import { nativeQuicAvailability } from '../../src/node/quic/index.ts';
import type { PageApi } from './page/page.ts';
import { startWebServer, type WebServer } from './web-server.ts';

/** The members of a Playwright page the harness uses. */
interface Page {
  goto(url: string): Promise<unknown>;
  evaluate<R, A>(fn: (arg: A) => R | Promise<R>, arg: A): Promise<R>;
  exposeFunction(name: string, fn: (...args: never[]) => unknown): Promise<void>;
  waitForFunction(fn: () => boolean): Promise<unknown>;
  on(event: 'console', listener: (message: { type(): string; text(): string }) => void): void;
  on(event: 'pageerror', listener: (error: Error) => void): void;
}

/** The members of a Playwright browser the harness uses. */
interface Browser {
  version(): string;
  newPage(): Promise<Page>;
  close(): Promise<void>;
}

/** The browsers the tests run in, as Playwright names them. */
export type BrowserName = 'chromium' | 'firefox';

declare module 'vitest' {
  export interface ProvidedContext {
    /** The browser of the project (`vitest.config.ts`). */
    browserName: BrowserName;
  }
}

/** The members of a Playwright browser type the harness uses. */
interface BrowserType {
  executablePath(): string;
  launch(options?: { readonly headless?: boolean }): Promise<Browser>;
}

/** The members of the Playwright module the harness uses. */
type Playwright = { readonly [name in BrowserName]: BrowserType };

/** The browser's name as people write it. */
export const browserTitles: { readonly [name in BrowserName]: string } = {
  chromium: 'Chromium',
  firefox: 'Firefox',
};

/** Whether the browser tests can run here: Playwright, its browser and the native addon. */
export type BrowserAvailability =
  | { readonly available: true; readonly playwright: Playwright }
  | { readonly available: false; readonly reason: string };

/** Whether the tests can run in [name]: the native addon (the server), Playwright and the browser. */
export function browserAvailability(name: BrowserName): BrowserAvailability {
  const native = nativeQuicAvailability();
  if (!native.available) return { available: false, reason: native.reason };
  let playwright: Playwright;
  try {
    playwright = createRequire(import.meta.url)('playwright') as Playwright;
  } catch {
    return {
      available: false,
      reason:
        'Playwright is not installed (npm ci installs the dev dependency; NODE_PATH may name a global install)',
    };
  }
  const executable = playwright[name].executablePath();
  if (!existsSync(executable)) {
    return {
      available: false,
      reason: `no ${browserTitles[name]} at ${executable} (npx playwright install ${name})`,
    };
  }
  return { available: true, playwright };
}

/** A running page, its server, and how to run a test's body in it. */
export interface BrowserTests {
  readonly server: WebServer;
  /** The browser. */
  readonly browserName: BrowserName;
  /** Its version, as the browser reports it. */
  readonly browserVersion: string;
  /**
   * Runs [body] in the page with the page's API and [arg] (JSON-like), and resolves with what it
   * returns (JSON-like). [body] may use its parameters only.
   */
  run<R, A = undefined>(body: (api: PageApi, arg: A) => Promise<R>, arg?: A): Promise<R>;
  /** Runs the tear-downs the last test added in the page. */
  tearDown(): Promise<void>;
  close(): Promise<void>;
}

const root = fileURLToPath(new URL('../..', import.meta.url));

/** Starts the server, Vite and the browser [name], and opens the page. */
export async function startBrowserTests(
  playwright: Playwright,
  name: BrowserName,
): Promise<BrowserTests> {
  const log = process.env['SWITCHBOARD_BROWSER_LOG'] === '1';
  const server = await startWebServer();
  const vite = await createServer({
    configFile: false,
    envFile: false,
    root,
    logLevel: log ? 'info' : 'error',
    clearScreen: false,
    appType: 'mpa',
    server: { host: '127.0.0.1', port: 0, hmr: false, watch: null },
    optimizeDeps: { noDiscovery: true, include: [] },
  });
  let browser: Browser | undefined;
  try {
    await vite.listen();
    const address = vite.httpServer?.address();
    if (address === null || address === undefined || typeof address === 'string') {
      throw new Error('Vite is not listening on a port');
    }
    browser = await playwright[name].launch({ headless: true });
    const page = await browser.newPage();
    page.on('pageerror', (error) => {
      if (log) console.error(`page error: ${error.message}`);
    });
    page.on('console', (message) => {
      if (log) console.error(`page ${message.type()}: ${message.text()}`);
    });
    await page.exposeFunction('serverRecords', (id: string) => server.records(id));
    await page.goto(`http://127.0.0.1:${address.port}/test/browser/page/index.html`);
    await page.waitForFunction(() => 'switchboardTests' in globalThis);
    await page.evaluate((description) => {
      (globalThis as unknown as { switchboardTests: PageApi }).switchboardTests.h.setServer(
        description,
      );
    }, server.description);
    const opened = browser;
    return {
      server,
      browserName: name,
      browserVersion: opened.version(),
      run<R, A>(body: (api: PageApi, arg: A) => Promise<R>, arg?: A): Promise<R> {
        return page.evaluate(
          ({ source, arg }) => {
            const api = (globalThis as unknown as { switchboardTests: PageApi }).switchboardTests;
            const run = new Function(`return (${source});`)() as (
              api: PageApi,
              arg: unknown,
            ) => Promise<R>;
            return run(api, arg);
          },
          { source: body.toString(), arg: arg ?? null },
        );
      },
      tearDown: () =>
        page.evaluate(
          () =>
            (globalThis as unknown as { switchboardTests: PageApi }).switchboardTests.h.tearDown(),
          null,
        ),
      async close() {
        await opened.close().catch(() => {});
        await vite.close();
        await server.close();
      },
    };
  } catch (error) {
    await browser?.close().catch(() => {});
    await vite.close();
    await server.close();
    throw error;
  }
}
