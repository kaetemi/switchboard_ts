// Vectors from the wiki page "Polyverse Switchboard Test Vectors", section
// "Naming service payloads", and the Dart `test/vectors/naming_vectors_test.dart`.
import { describe, expect, test } from 'vitest';
import { ServiceAddress } from '../../src/core/address/index.ts';
import { hexBytes, hexString } from '../../src/core/bytes.ts';
import { Name } from '../../src/core/name.ts';
import {
  ConnectRequest,
  ConnectResponse,
  DialBackRequest,
  decodeDownEvent,
  decodeUpEvent,
  encodeServiceEvent,
  Procedures,
  RegisterRequest,
  RegisterResponse,
  Services,
  UnregisterRequest,
  WatchRequest,
} from '../../src/core/naming/index.ts';
import { ProtocolError } from '../../src/core/status.ts';

const npc = Name.of('npc');

describe('naming service payloads', () => {
  test('REGISTER npc, assign id, one endpoint', () => {
    const bytes = hexBytes(
      '6E 70 63 00 00 00 00 00 00 00 00 00 00 00 01 13 74 63 70 3A 2F 2F 31 ' +
        '30 2E 30 2E 30 2E 35 3A 39 31 30 31 00 00',
    );
    const r = RegisterRequest.decode(bytes);
    expect(r.type.equals(npc)).toBe(true);
    expect(r.requestedInstance).toBe(0);
    expect(r.endpoints).toEqual(['tcp://10.0.0.5:9101']);
    expect(r.metadata.length).toBe(0);
    expect(hexString(r.encode())).toBe(hexString(bytes));
  });

  test('REGISTER response, instance 5', () => {
    const bytes = hexBytes('05 00 00 00 00 00');
    expect(RegisterResponse.decode(bytes).instance).toBe(5);
    expect(hexString(new RegisterResponse(5).encode())).toBe(hexString(bytes));
  });

  test('UNREGSTR npc instance 5', () => {
    const bytes = hexBytes('6E 70 63 00 00 00 00 00 05 00 00 00 00 00');
    const r = UnregisterRequest.decode(bytes);
    expect(r.type.equals(npc)).toBe(true);
    expect(r.instance).toBe(5);
    expect(hexString(r.encode())).toBe(hexString(bytes));
  });

  // UNWATCH carries the payload of the WATCH it gives back: the same codec.
  test.each(['WATCH', 'UNWATCH'])('%s all (empty payload)', () => {
    const r = WatchRequest.decode(new Uint8Array(0));
    expect(r.type).toBeUndefined();
    expect(r.encode().length).toBe(0);
  });

  test.each(['WATCH', 'UNWATCH'])('%s type npc', () => {
    const bytes = hexBytes('6E 70 63 00 00 00 00 00');
    const r = WatchRequest.decode(bytes);
    expect(r.type?.equals(npc)).toBe(true);
    expect(hexString(r.encode())).toBe(hexString(bytes));
  });

  test('WATCH all-zero name is all', () => {
    expect(WatchRequest.decode(new Uint8Array(8)).type).toBeUndefined();
  });

  test('UP npc instance 5, no endpoints', () => {
    const bytes = hexBytes('6E 70 63 00 00 00 00 00 05 00 00 00 00 00 00 00 00');
    const e = decodeUpEvent(bytes);
    expect(e.up).toBe(true);
    expect(e.record.address.equals(new ServiceAddress(npc, 5))).toBe(true);
    expect(e.record.endpoints).toEqual([]);
    expect(e.record.metadata.length).toBe(0);
    expect(hexString(encodeServiceEvent(e))).toBe(hexString(bytes));
  });

  test('UP npc instance 5, no endpoints, metadata 01 02 03', () => {
    const bytes = hexBytes('6E 70 63 00 00 00 00 00 05 00 00 00 00 00 00 03 00 01 02 03');
    const e = decodeUpEvent(bytes);
    expect(e.up).toBe(true);
    expect(e.record.address.equals(new ServiceAddress(npc, 5))).toBe(true);
    expect(e.record.endpoints).toEqual([]);
    expect([...e.record.metadata]).toEqual([1, 2, 3]);
    expect(hexString(encodeServiceEvent(e))).toBe(hexString(bytes));
  });

  test('a record without the metadata field has empty metadata', () => {
    const bytes = hexBytes('6E 70 63 00 00 00 00 00 05 00 00 00 00 00 00');
    const e = decodeUpEvent(bytes);
    expect(e.record.address.equals(new ServiceAddress(npc, 5))).toBe(true);
    expect(e.record.metadata.length).toBe(0);
    expect(hexString(encodeServiceEvent(e))).toBe(`${hexString(bytes)} 00 00`);
  });

  test('DOWN npc instance 5', () => {
    const bytes = hexBytes('6E 70 63 00 00 00 00 00 05 00 00 00 00 00');
    const e = decodeDownEvent(bytes);
    expect(e.up).toBe(false);
    expect(e.record.address.equals(new ServiceAddress(npc, 5))).toBe(true);
    expect(hexString(encodeServiceEvent(e))).toBe(hexString(bytes));
  });

  test('procedure names are as specified', () => {
    expect(hexString(Procedures.register.bytes)).toBe('52 45 47 49 53 54 45 52');
    expect(Procedures.unregister.toString()).toBe('UNREGSTR');
    expect(Procedures.watch.toString()).toBe('WATCH');
    expect(hexString(Procedures.unwatch.bytes)).toBe('55 4E 57 41 54 43 48 00');
    expect(hexString(Services.naming.bytes)).toBe('5F 6E 73 00 00 00 00 00');
    expect(hexString(Procedures.renew.bytes)).toBe('52 45 4E 45 57 00 00 00');
    expect(hexString(Procedures.connect.bytes)).toBe('43 4F 4E 4E 45 43 54 00');
  });
});

describe('CONNECT', () => {
  const endpoint = '13 74 63 70 3A 2F 2F 31 30 2E 30 2E 30 2E 37 3A 39 33 30 30';

  test('request: worker-g instance 0x2A, tcp://10.0.0.7:9300, intent 01 02 03 04', () => {
    const bytes = hexBytes(`77 6F 72 6B 65 72 2D 67 2A 00 00 00 00 00 ${endpoint} 04 01 02 03 04`);
    const r = ConnectRequest.decode(bytes);
    expect(r.type.equals(Name.of('worker-g'))).toBe(true);
    expect(r.instance).toBe(0x2a);
    expect(r.endpoint).toBe('tcp://10.0.0.7:9300');
    expect([...r.intent]).toEqual([1, 2, 3, 4]);
    expect(hexString(r.encode())).toBe(hexString(bytes));
  });

  test('response: identity worker-a1', () => {
    const bytes = hexBytes('09 77 6F 72 6B 65 72 2D 61 31');
    expect(ConnectResponse.decode(bytes).identity).toBe('worker-a1');
    expect(hexString(new ConnectResponse('worker-a1').encode())).toBe(hexString(bytes));
  });

  test('to the instance: requester consumer-1, the endpoint, the intent', () => {
    const bytes = hexBytes(`0A 63 6F 6E 73 75 6D 65 72 2D 31 ${endpoint} 04 01 02 03 04`);
    const r = DialBackRequest.decode(bytes);
    expect(r.requester).toBe('consumer-1');
    expect(r.endpoint).toBe('tcp://10.0.0.7:9300');
    expect([...r.intent]).toEqual([1, 2, 3, 4]);
    expect(hexString(r.encode())).toBe(hexString(bytes));
  });

  test('to the instance: no requester identity, no intent', () => {
    const bytes = hexBytes(`00 ${endpoint} 00`);
    const r = DialBackRequest.decode(bytes);
    expect(r.requester).toBe('');
    expect(r.intent.length).toBe(0);
    expect(hexString(r.encode())).toBe(hexString(bytes));
  });

  const zeros = Array.from({ length: 65 }, () => '00').join(' ');
  test.each<[string, string, (bytes: Uint8Array) => unknown]>([
    [
      'request with an intent of 65 bytes',
      `77 6F 72 6B 65 72 2D 67 2A 00 00 00 00 00 ${endpoint} 41 ${zeros}`,
      ConnectRequest.decode,
    ],
    [
      'request without its intent',
      `77 6F 72 6B 65 72 2D 67 2A 00 00 00 00 00 ${endpoint}`,
      ConnectRequest.decode,
    ],
    [
      'request with an endpoint without a scheme',
      '77 6F 72 6B 65 72 2D 67 2A 00 00 00 00 00 01 78 00',
      ConnectRequest.decode,
    ],
    ['requester that is not UTF-8', `01 FF ${endpoint} 00`, DialBackRequest.decode],
    ['response cut short', '09 77 6F', ConnectResponse.decode],
  ])('negative: %s', (_name, hex, decode) => {
    expect(() => decode(hexBytes(hex))).toThrow(ProtocolError);
  });
});
