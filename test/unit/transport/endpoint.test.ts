// Endpoint URIs as the connection pool tells them apart: Dart's
// `Switchboard._normalised`, with the QUIC binding's rules for `quic` and
// `https`. No Dart test file of its own; the Dart pool tests exercise it.
import { describe, expect, test } from 'vitest';
import {
  EndpointScheme,
  isKnownEndpointScheme,
  parseEndpoint,
} from '../../../src/core/transport/endpoint.ts';

describe('parseEndpoint', () => {
  test('tcp: scheme and host lower-cased, the port kept', () => {
    const e = parseEndpoint('TCP://Example.COM:4000');
    expect(e).toMatchObject({ scheme: 'tcp', host: 'example.com', port: 4000, path: '' });
    expect(e.query).toBeUndefined();
    expect(e.key).toBe('tcp://example.com:4000');
    expect(parseEndpoint('tcp://h').port).toBe(0);
    expect(parseEndpoint('tcp://h/ignored?x#y').key).toBe('tcp://h:0');
  });

  test('IPv6 hosts lose their brackets, the key keeps them', () => {
    const e = parseEndpoint('tcp://[::1]:4000');
    expect(e.host).toBe('::1');
    expect(e.key).toBe('tcp://[::1]:4000');
  });

  test('ws and wss: default ports, the path and query name the listener', () => {
    expect(parseEndpoint('ws://Example.com').key).toBe('ws://example.com:80/');
    expect(parseEndpoint('ws://example.com:80/').key).toBe('ws://example.com:80/');
    const e = parseEndpoint('wss://h/mesh?shard=2#frag');
    expect(e).toMatchObject({
      scheme: 'wss',
      host: 'h',
      port: 443,
      path: '/mesh',
      query: 'shard=2',
    });
    expect(e.key).toBe('wss://h:443/mesh?shard=2');
    expect(parseEndpoint('ws://h:8080/a').port).toBe(8080);
    expect(parseEndpoint('ws://h/?').query).toBe('');
    expect(parseEndpoint('ws://h/?').key).toBe('ws://h:80/?');
    expect(parseEndpoint('ws://h/').query).toBeUndefined();
  });

  test('mem: the id alone, lower-cased', () => {
    const e = parseEndpoint('mem://Naming.Service_1');
    expect(e).toMatchObject({ scheme: 'mem', host: 'naming.service_1', port: 0, path: '' });
    expect(e.key).toBe('mem://naming.service_1');
    expect(parseEndpoint('mem://a:5/x?y').key).toBe('mem://a');
    expect(parseEndpoint('mem://').host).toBe('');
  });

  test('quic: the port only when given', () => {
    expect(parseEndpoint('quic://Host').key).toBe('quic://host');
    expect(parseEndpoint('quic://Host').port).toBe(0);
    expect(parseEndpoint('quic://h:4433/p?q').key).toBe('quic://h:4433');
  });

  test('https (WebTransport): port 443 by default, the path kept, no query', () => {
    const e = parseEndpoint('https://H/wt?x=1');
    expect(e).toMatchObject({ scheme: 'https', host: 'h', port: 443, path: '/wt' });
    expect(e.query).toBeUndefined();
    expect(e.key).toBe('https://h:443/wt');
    expect(parseEndpoint('https://h:443').key).toBe('https://h:443/');
  });

  test('other schemes: host and port', () => {
    const e = parseEndpoint('Foo://A:9/p?q');
    expect(e.scheme).toBe('foo');
    expect(e.key).toBe('foo://a:9');
    expect(isKnownEndpointScheme(e.scheme)).toBe(false);
  });

  test('URL objects are accepted and href is kept', () => {
    const e = parseEndpoint(new URL('ws://h:81/x'));
    expect(e.href).toBe('ws://h:81/x');
    expect(e.key).toBe('ws://h:81/x');
  });

  test('a string that is not a URI is a TypeError', () => {
    expect(() => parseEndpoint('not a uri')).toThrow(TypeError);
    expect(() => parseEndpoint('')).toThrow(TypeError);
  });

  test('the known schemes', () => {
    expect(Object.values(EndpointScheme).sort()).toEqual([
      'https',
      'mem',
      'quic',
      'tcp',
      'ws',
      'wss',
    ]);
    for (const scheme of Object.values(EndpointScheme)) {
      expect(isKnownEndpointScheme(scheme)).toBe(true);
    }
    expect(isKnownEndpointScheme('TCP')).toBe(false);
  });
});
