// Vectors from the wiki page "Polyverse Switchboard Test Vectors", section "Address headers".
import { describe, expect, test } from 'vitest';
import { ChannelAddress, ServiceAddress } from '../../src/core/address/index.ts';
import { hexBytes, hexString } from '../../src/core/bytes.ts';
import { Name } from '../../src/core/name.ts';
import { ProtocolError } from '../../src/core/status.ts';

describe('address headers', () => {
  const vectors: [string, ChannelAddress, string][] = [
    ['default service, no payload', new ChannelAddress(), '00'],
    [
      'service "api", app payload 01 02',
      new ChannelAddress({ type: 'api', payload: hexBytes('01 02') }),
      '01 61 70 69 00 00 00 00 00 01 02',
    ],
    [
      'service "npc", instance 0x1A2B',
      new ChannelAddress({ type: 'npc', instance: 0x1a2b }),
      '03 6E 70 63 00 00 00 00 00 2B 1A 00 00 00 00',
    ],
    [
      'service "chat", shard 7',
      new ChannelAddress({ type: 'chat', shard: 7 }),
      '05 63 68 61 74 00 00 00 00 07 00 00 00',
    ],
    [
      'service "api", host hint "b.example"',
      new ChannelAddress({ type: 'api', host: 'b.example' }),
      '09 61 70 69 00 00 00 00 00 09 62 2E 65 78 61 6D 70 6C 65',
    ],
    [
      'everything: "ab", instance 1, shard 2, host "h", payload FF',
      new ChannelAddress({ type: 'ab', instance: 1, shard: 2, host: 'h', payload: hexBytes('FF') }),
      '0F 61 62 00 00 00 00 00 00 01 00 00 00 00 00 02 00 00 00 01 68 FF',
    ],
    [
      '_bulk, parent channel 2, bulk number 1',
      new ChannelAddress({ type: '_bulk', payload: hexBytes('02 00 00 00 00 00 01 00 00 00') }),
      '01 5F 62 75 6C 6B 00 00 00 02 00 00 00 00 00 01 00 00 00',
    ],
    [
      '_relay, inner "worker-g" instance 0x2A, app payload 01 02',
      new ChannelAddress({
        type: '_relay',
        payload: new ChannelAddress({
          type: 'worker-g',
          instance: 0x2a,
          payload: hexBytes('01 02'),
        }).encode(),
      }),
      '01 5F 72 65 6C 61 79 00 00 03 77 6F 72 6B 65 72 2D 67 2A 00 00 00 00 00 01 02',
    ],
  ];

  test.each(vectors)('%s', (_name, address, hex) => {
    expect(hexString(address.encode())).toBe(hex);
    const decoded = ChannelAddress.decode(hexBytes(hex));
    expect(decoded.type?.key).toBe(address.type?.key);
    expect(decoded.instance).toBe(address.instance);
    expect(decoded.shard).toBe(address.shard);
    expect(decoded.host).toBe(address.host);
    expect(hexString(decoded.payload)).toBe(hexString(address.payload));
  });

  test('an empty OPEN payload decodes as the default address', () => {
    const a = ChannelAddress.decode(new Uint8Array(0));
    expect(a.type).toBeUndefined();
    expect(a.address).toBeUndefined();
    expect(a.payload.length).toBe(0);
  });

  test.each([
    ['reserved bit 0x10', '10'],
    ['truncated service name', '01 61 70'],
    ['host hint longer than payload', '08 05 61'],
    ['truncated instance', '02 2B 1A'],
    ['a name with a non-zero byte after a zero byte', '01 61 00 62 00 00 00 00 00'],
  ])('negative: %s', (_name, hex) => {
    expect(() => ChannelAddress.decode(hexBytes(hex))).toThrow(ProtocolError);
  });

  test('the 48-bit instance is exact', () => {
    const a = new ChannelAddress({ type: 'x', instance: 0xffffffffffff });
    expect(ChannelAddress.decode(a.encode()).instance).toBe(0xffffffffffff);
    expect(() => new ChannelAddress({ instance: 0x1000000000000 })).toThrow(RangeError);
    expect(() => new ChannelAddress({ shard: 0x100000000 })).toThrow(RangeError);
    expect(() => new ChannelAddress({ host: 'h'.repeat(256) })).toThrow(RangeError);
  });

  test('with() replaces and clears fields', () => {
    const a = new ChannelAddress({ type: 'chat', shard: 7, host: 'h', payload: 'p' });
    const b = a.with({ instance: 9, clearHost: true });
    expect(b.instance).toBe(9);
    expect(b.host).toBeUndefined();
    expect(b.shard).toBe(7);
    expect(a.host).toBe('h');
    expect(b.with({ clearShard: true }).shard).toBeUndefined();
  });
});

describe('service addresses', () => {
  test('text form', () => {
    expect(new ServiceAddress(Name.of('npc'), 0x1a2b).toString()).toBe('npc/1a2b');
    expect(new ServiceAddress('npc').toString()).toBe('npc');
    expect(ServiceAddress.parse('npc/1a2b').instance).toBe(0x1a2b);
    expect(ServiceAddress.parse('npc').isAny).toBe(true);
    expect(ServiceAddress.parse('npc/0').isAny).toBe(true);
    expect(() => ServiceAddress.parse('/1')).toThrow(RangeError);
    expect(() => ServiceAddress.parse('npc/xyz')).toThrow(RangeError);
    expect(() => ServiceAddress.parse('npc/1234567890123')).toThrow(RangeError);
    expect(() => new ServiceAddress('npc', -1)).toThrow(RangeError);
  });

  test('equality and keys', () => {
    const a = new ServiceAddress('npc', 5);
    const b = ServiceAddress.parse('npc/5');
    expect(a.equals(b)).toBe(true);
    expect(a.key).toBe(b.key);
    expect(a.equals(new ServiceAddress('npc', 6))).toBe(false);
    expect(ChannelAddress.to(a, 'x').address?.equals(a)).toBe(true);
  });
});
