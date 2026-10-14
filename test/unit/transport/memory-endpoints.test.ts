// The `mem://` listener registry, `memory_endpoints.dart`. The node level
// behaviour (`listenMemory`, connecting, closing) is ported with the node
// from `memory_endpoint_test.dart`.
import { afterEach, describe, expect, test } from 'vitest';
import { StatusCode, SwitchboardError } from '../../../src/core/status.ts';
import { MemoryEndpoints } from '../../../src/core/transport/memory-endpoints.ts';

const registered: string[] = [];

function register(name?: string): string {
  const uri = MemoryEndpoints.register(() => {}, name === undefined ? {} : { name });
  registered.push(uri.slice('mem://'.length));
  return uri;
}

afterEach(() => {
  for (const id of registered.splice(0)) {
    MemoryEndpoints.unregister(id);
  }
  expect(MemoryEndpoints.ids).toEqual([]);
});

describe('MemoryEndpoints', () => {
  test('generated ids are unique mem:// URIs', () => {
    const a = register();
    const b = register();
    expect(a).toMatch(/^mem:\/\/sb\d+-[0-9a-f]{6}$/);
    expect(b).not.toBe(a);
    expect(new URL(a).hostname).toBe(a.slice('mem://'.length));
    expect([...MemoryEndpoints.ids].sort()).toEqual(
      [a, b].map((u) => u.slice('mem://'.length)).sort(),
    );
  });

  test('names are lower-cased and compared in lower case', () => {
    expect(register('Naming.Service_1')).toBe('mem://naming.service_1');
    expect(MemoryEndpoints.lookup('NAMING.SERVICE_1')).toBeTypeOf('function');
    expect(MemoryEndpoints.lookup('other')).toBeUndefined();
  });

  test('a name is taken until it is unregistered', () => {
    register('shared');
    let error: unknown;
    try {
      MemoryEndpoints.register(() => {}, { name: 'SHARED' });
    } catch (e) {
      error = e;
    }
    expect(error).toBeInstanceOf(SwitchboardError);
    expect((error as SwitchboardError).code).toBe(StatusCode.alreadyExists);
    MemoryEndpoints.unregister('Shared');
    registered.length = 0;
    expect(register('shared')).toBe('mem://shared');
  });

  test('invalid names are a RangeError', () => {
    for (const bad of ['', 'a b', 'a/b', 'a:1', 'a#b', 'é']) {
      expect(() => MemoryEndpoints.register(() => {}, { name: bad }), bad).toThrow(RangeError);
    }
    expect(MemoryEndpoints.ids).toEqual([]);
  });

  test('lookup returns the registered acceptor', () => {
    const seen: string[] = [];
    const uri = MemoryEndpoints.register((_, remote) => seen.push(remote), { name: 'svc' });
    registered.push('svc');
    expect(uri).toBe('mem://svc');
    MemoryEndpoints.lookup('svc')?.(
      { readable: new ReadableStream(), writable: new WritableStream() },
      'memory peer',
    );
    expect(seen).toEqual(['memory peer']);
    expect(MemoryEndpoints.scheme).toBe('mem');
  });
});
