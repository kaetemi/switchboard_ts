// Vectors from the wiki page "Polyverse Switchboard Test Vectors", section
// "Naming service payloads", and the Dart `test/vectors/naming_vectors_test.dart`.
// The wiki's are read from test/vectors/wiki-vectors.json by id (through
// test/support/vectors.ts), so test/vectors/coverage.test.ts checks both
// naming tables; the fragments its paragraph gives are checked inside the
// messages built around them.
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
import { wikiBytes, wikiVariants, wikiVector } from '../support/vectors.ts';

const npc = Name.of('npc');

describe('naming service payloads', () => {
  test('REGISTER npc, assign id, one endpoint', () => {
    const bytes = wikiBytes(
      'test-vectors/naming-service-payloads/register-npc-assign-id-one-endpoint-tcp-10-0-0-5-9101',
    );
    const r = RegisterRequest.decode(bytes);
    expect(r.type.equals(npc)).toBe(true);
    expect(r.requestedInstance).toBe(0);
    expect(r.endpoints).toEqual(['tcp://10.0.0.5:9101']);
    expect(r.metadata.length).toBe(0);
    expect(hexString(r.encode())).toBe(hexString(bytes));
  });

  test('REGISTER response, instance 5', () => {
    const bytes = wikiBytes('test-vectors/naming-service-payloads/register-response-instance-5');
    expect(RegisterResponse.decode(bytes).instance).toBe(5);
    expect(hexString(new RegisterResponse(5).encode())).toBe(hexString(bytes));
  });

  test('UNREGSTR npc instance 5', () => {
    const bytes = wikiBytes('test-vectors/naming-service-payloads/unregstr-npc-instance-5');
    const r = UnregisterRequest.decode(bytes);
    expect(r.type.equals(npc)).toBe(true);
    expect(r.instance).toBe(5);
    expect(hexString(r.encode())).toBe(hexString(bytes));
  });

  // UNWATCH carries the payload of the WATCH it gives back: the same codec.
  test.each([
    ['WATCH', wikiBytes('test-vectors/naming-service-payloads/watch-all-empty-payload')],
    ['UNWATCH', wikiBytes('test-vectors/naming-service-payloads/unwatch-all-empty-payload')],
  ])('%s all (empty payload)', (_name, bytes) => {
    expect(bytes.length).toBe(0);
    const r = WatchRequest.decode(bytes);
    expect(r.type).toBeUndefined();
    expect(r.encode().length).toBe(0);
  });

  test.each([
    ['WATCH', wikiBytes('test-vectors/naming-service-payloads/watch-type-npc')],
    ['UNWATCH', wikiBytes('test-vectors/naming-service-payloads/unwatch-type-npc')],
  ])('%s type npc', (_name, bytes) => {
    const r = WatchRequest.decode(bytes);
    expect(r.type?.equals(npc)).toBe(true);
    expect(hexString(r.encode())).toBe(hexString(bytes));
  });

  test('WATCH all-zero name is all', () => {
    expect(WatchRequest.decode(new Uint8Array(8)).type).toBeUndefined();
  });

  test('UP npc instance 5, no endpoints', () => {
    const bytes = wikiBytes('test-vectors/naming-service-payloads/up-npc-instance-5-no-endpoints');
    const e = decodeUpEvent(bytes);
    expect(e.up).toBe(true);
    expect(e.record.address.equals(new ServiceAddress(npc, 5))).toBe(true);
    expect(e.record.endpoints).toEqual([]);
    expect(e.record.metadata.length).toBe(0);
    expect(hexString(encodeServiceEvent(e))).toBe(hexString(bytes));
  });

  test('UP npc instance 5, no endpoints, metadata 01 02 03', () => {
    const bytes = wikiBytes(
      'test-vectors/naming-service-payloads/up-npc-instance-5-no-endpoints-metadata-01-02-03',
    );
    const e = decodeUpEvent(bytes);
    expect(e.up).toBe(true);
    expect(e.record.address.equals(new ServiceAddress(npc, 5))).toBe(true);
    expect(e.record.endpoints).toEqual([]);
    expect([...e.record.metadata]).toEqual([1, 2, 3]);
    expect(hexString(encodeServiceEvent(e))).toBe(hexString(bytes));
  });

  test('a record without the metadata field has empty metadata', () => {
    const bytes = wikiBytes(
      'test-vectors/naming-service-payloads/prose-6e-70-63-00-00-00-00-00-05-00-00-00-00-00-00',
    );
    const e = decodeUpEvent(bytes);
    expect(e.record.address.equals(new ServiceAddress(npc, 5))).toBe(true);
    expect(e.record.metadata.length).toBe(0);
    expect(hexString(encodeServiceEvent(e))).toBe(`${hexString(bytes)} 00 00`);
  });

  test('DOWN npc instance 5', () => {
    const bytes = wikiBytes('test-vectors/naming-service-payloads/down-npc-instance-5');
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
    // The wiki's two names, RENEW then CONNECT.
    const [renew, connect] = wikiVariants(
      'test-vectors/naming-service-payloads/procedure-names-renew-connect',
    );
    expect(hexString(Procedures.renew.bytes)).toBe(hexString(renew as Uint8Array));
    expect(hexString(Procedures.connect.bytes)).toBe(hexString(connect as Uint8Array));
  });
});

describe('CONNECT', () => {
  const endpoint = '13 74 63 70 3A 2F 2F 31 30 2E 30 2E 30 2E 37 3A 39 33 30 30';

  test('request: worker-g instance 0x2A, tcp://10.0.0.7:9300, intent 01 02 03 04', () => {
    const bytes = wikiBytes(
      'test-vectors/naming-service-payloads/connect-request-worker-g-instance-0x2a-tcp-10-0-0-7-9300-intent-01-02-03-04',
    );
    expect(hexString(bytes)).toBe(
      `77 6F 72 6B 65 72 2D 67 2A 00 00 00 00 00 ${endpoint} 04 01 02 03 04`,
    );
    const r = ConnectRequest.decode(bytes);
    expect(r.type.equals(Name.of('worker-g'))).toBe(true);
    expect(r.instance).toBe(0x2a);
    expect(r.endpoint).toBe('tcp://10.0.0.7:9300');
    expect([...r.intent]).toEqual([1, 2, 3, 4]);
    expect(hexString(r.encode())).toBe(hexString(bytes));
  });

  test('response: identity worker-a1', () => {
    const bytes = wikiBytes(
      'test-vectors/naming-service-payloads/connect-response-identity-worker-a1',
    );
    expect(ConnectResponse.decode(bytes).identity).toBe('worker-a1');
    expect(hexString(new ConnectResponse('worker-a1').encode())).toBe(hexString(bytes));
  });

  test('to the instance: requester consumer-1, the endpoint, the intent', () => {
    const bytes = wikiBytes(
      'test-vectors/naming-service-payloads/connect-to-the-instance-requester-consumer-1-the-same-endpoint-and-intent',
    );
    expect(hexString(bytes)).toBe(`0A 63 6F 6E 73 75 6D 65 72 2D 31 ${endpoint} 04 01 02 03 04`);
    const r = DialBackRequest.decode(bytes);
    expect(r.requester).toBe('consumer-1');
    expect(r.endpoint).toBe('tcp://10.0.0.7:9300');
    expect([...r.intent]).toEqual([1, 2, 3, 4]);
    expect(hexString(r.encode())).toBe(hexString(bytes));
  });

  test('to the instance: no requester identity, no intent', () => {
    const bytes = wikiBytes(
      'test-vectors/naming-service-payloads/connect-to-the-instance-no-requester-identity-no-intent',
    );
    expect(hexString(bytes)).toBe(`00 ${endpoint} 00`);
    const r = DialBackRequest.decode(bytes);
    expect(r.requester).toBe('');
    expect(r.intent.length).toBe(0);
    expect(hexString(r.encode())).toBe(hexString(bytes));
  });

  const zeros = Array.from({ length: 65 }, () => '00').join(' ');
  // The paragraph below the table gives fragments: the intent length `41`,
  // the endpoint `01 78`, the requester `01 FF`, and the whole response
  // `09 77 6F`. Each is checked in the message built around it.
  const intent65 = wikiVector('test-vectors/naming-service-payloads/prose-41').hex as string;
  const noScheme = wikiVector('test-vectors/naming-service-payloads/prose-01-78').hex as string;
  const notUtf8 = wikiVector('test-vectors/naming-service-payloads/prose-01-ff').hex as string;
  const cutShort = wikiVector('test-vectors/naming-service-payloads/prose-09-77-6f').hex as string;
  test.each<[string, string, (bytes: Uint8Array) => unknown]>([
    [
      'request with an intent of 65 bytes',
      `77 6F 72 6B 65 72 2D 67 2A 00 00 00 00 00 ${endpoint} ${intent65} ${zeros}`,
      ConnectRequest.decode,
    ],
    [
      'request without its intent',
      `77 6F 72 6B 65 72 2D 67 2A 00 00 00 00 00 ${endpoint}`,
      ConnectRequest.decode,
    ],
    [
      'request with an endpoint without a scheme',
      `77 6F 72 6B 65 72 2D 67 2A 00 00 00 00 00 ${noScheme} 00`,
      ConnectRequest.decode,
    ],
    ['requester that is not UTF-8', `${notUtf8} ${endpoint} 00`, DialBackRequest.decode],
    ['response cut short', cutShort, ConnectResponse.decode],
  ])('negative: %s', (_name, hex, decode) => {
    expect(() => decode(hexBytes(hex))).toThrow(ProtocolError);
  });

  test('the fragments are what the negative cases say', () => {
    expect([intent65, noScheme, notUtf8, cutShort]).toEqual(['41', '01 78', '01 FF', '09 77 6F']);
  });
});
