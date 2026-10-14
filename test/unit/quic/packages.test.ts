// The prebuilt QUIC addons' packages agree with each other: the manifests
// under native/npm/<target>/, the table of targets in native/README.md, the
// prefix the loader requires (src/node/quic/native-provider.ts), the
// optionalDependencies of package.json and the release workflow
// (.github/workflows/release.yml). The release workflow runs this file before
// it builds anything.
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { describe, expect, test } from 'vitest';

const root = new URL('../../../', import.meta.url);
const read = (path: string): string => readFileSync(new URL(path, root), 'utf8');
const json = (path: string): Record<string, unknown> =>
  JSON.parse(read(path)) as Record<string, unknown>;

const prefix = '@kaetemi/switchboard-quic-';
const main = json('package.json');
const version = main['version'] as string;

/** The README's table: package suffix to Rust target. */
const table = new Map<string, string>(
  [
    ...read('native/README.md').matchAll(
      /^\|[^|]*\|\s*`([^`]+)`\s*\|\s*`@kaetemi\/switchboard-quic-([^`]+)`\s*\|$/gm,
    ),
  ].map((m) => [m[2] as string, m[1] as string]),
);

const directories = readdirSync(new URL('native/npm/', root), { withFileTypes: true })
  .filter((entry) => entry.isDirectory())
  .map((entry) => entry.name)
  .sort();

/** The loader's package suffix of a platform, as `platformTarget()` in the loader names it. */
function suffixOf(os: string, cpu: string, libc: string | undefined): string {
  if (os === 'linux') return `linux-${cpu}-${libc === 'musl' ? 'musl' : 'gnu'}`;
  if (os === 'win32') return `win32-${cpu}-msvc`;
  return `${os}-${cpu}`;
}

describe('the per-platform packages', () => {
  test("the five targets of switchboard_quic's release, as the README lists them", () => {
    expect([...table.keys()].sort()).toEqual([
      'darwin-arm64',
      'darwin-x64',
      'linux-arm64-gnu',
      'linux-x64-gnu',
      'win32-x64-msvc',
    ]);
    expect([...table.values()].sort()).toEqual([
      'aarch64-apple-darwin',
      'aarch64-unknown-linux-gnu',
      'x86_64-apple-darwin',
      'x86_64-pc-windows-msvc',
      'x86_64-unknown-linux-gnu',
    ]);
    expect(directories).toEqual([...table.keys()].sort());
  });

  test('the loader requires the packages by this prefix', () => {
    expect(read('src/node/quic/native-provider.ts')).toContain(
      `const packagePrefix = '${prefix.slice(0, -1)}';`,
    );
  });

  for (const directory of directories) {
    test(`native/npm/${directory}: name, version, platform, the one file`, () => {
      const manifest = json(`native/npm/${directory}/package.json`);
      expect(manifest['name']).toBe(`${prefix}${directory}`);
      expect(manifest['version']).toBe(version);
      expect(manifest['license']).toBe(main['license']);
      expect(manifest['main']).toBe('sbq-node.node');
      expect(manifest['files']).toEqual(['sbq-node.node']);
      const os = manifest['os'] as string[];
      const cpu = manifest['cpu'] as string[];
      const libc = manifest['libc'] as string[] | undefined;
      expect(os).toHaveLength(1);
      expect(cpu).toHaveLength(1);
      expect(suffixOf(os[0] as string, cpu[0] as string, libc?.[0])).toBe(directory);
      expect(libc).toEqual(os[0] === 'linux' ? ['glibc'] : undefined);
      const repository = manifest['repository'] as Record<string, string>;
      expect(repository['url']).toBe((main['repository'] as Record<string, string>)['url']);
      expect(repository['directory']).toBe(`native/npm/${directory}`);
      expect(manifest['publishConfig']).toEqual({ access: 'public' });
      expect(manifest['engines']).toEqual(main['engines']);
      // Nothing to run at install: the addon is the package.
      expect(manifest['scripts']).toBeUndefined();
      expect(existsSync(new URL(`native/npm/${directory}/index.js`, root))).toBe(false);
    });
  }

  test('package.json lists each as an optional dependency, pinned to its version', () => {
    expect(main['optionalDependencies']).toEqual(
      Object.fromEntries(directories.map((directory) => [`${prefix}${directory}`, version])),
    );
  });

  test('the release workflow builds, packs and publishes each, by hand only', () => {
    const workflow = read('.github/workflows/release.yml');
    const matrix = new Map(
      [...workflow.matchAll(/- \{ target: ([\w-]+), triple: ([\w-]+), runner: [\w.-]+ \}/g)].map(
        (m) => [m[1] as string, m[2] as string],
      ),
    );
    expect(matrix).toEqual(table);
    const lists = [
      ...workflow.matchAll(/for target in ([\w -]+); do/g),
      ...workflow.matchAll(/targets=\(([\w -]+)\)/g),
    ];
    expect(lists).toHaveLength(2);
    for (const list of lists) {
      expect((list[1] as string).trim().split(/\s+/).sort()).toEqual(directories);
    }
    // Only workflow_dispatch triggers it; publishing needs its input.
    const on = workflow.slice(workflow.indexOf('\non:'), workflow.indexOf('\npermissions:'));
    expect(on).toContain('workflow_dispatch:');
    expect(on).not.toMatch(/\n {2}(push|pull_request|schedule|release|workflow_run):/);
    expect(workflow).toContain('if: inputs.publish');
    expect(workflow).toContain('npm publish');
    expect(workflow).toContain('--provenance');
  });
});
