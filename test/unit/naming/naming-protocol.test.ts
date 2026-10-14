// Port of the Dart `test/naming_protocol_test.dart`: the record, REGISTER,
// UNREGSTR, WATCH and event codecs, endpoint validation and a fuzz pass;
// plus the endpoint differential against the Dart reference.
import { readFileSync } from 'node:fs';
import { describe, expect, test } from 'vitest';
import { ServiceAddress } from '../../../src/core/address/index.ts';
import { ByteWriter, hexString } from '../../../src/core/bytes.ts';
import { Name } from '../../../src/core/name.ts';
import {
  checkEndpoint,
  decodeDownEvent,
  decodeServiceRecord,
  decodeUpEvent,
  encodeServiceEvent,
  encodeServiceRecord,
  endpointProblem,
  maxMetadataLength,
  Procedures,
  RegisterRequest,
  RegisterResponse,
  serviceEventProcedure,
  serviceRecord,
  serviceRecordsEqual,
  serviceRecordToString,
  UnregisterRequest,
  upEvent,
  WatchRequest,
} from '../../../src/core/naming/index.ts';
import { ProtocolError } from '../../../src/core/status.ts';
import type { ServiceEvent, ServiceRecord } from '../../../src/core/switchboard/contract.ts';
import { fuzzInput, Random } from '../../support/naming/fuzz.ts';

const npc = Name.of('npc');
const uris = ['ws://10.0.0.5:9100/ws', 'tcp://h:9101'];
const bytes = (values: number[]): Uint8Array => Uint8Array.from(values);

function equalRecords(a: ServiceRecord, b: ServiceRecord): void {
  expect(
    serviceRecordsEqual(a, b),
    `${serviceRecordToString(a)} = ${serviceRecordToString(b)}`,
  ).toBe(true);
}

describe('ServiceRecord', () => {
  test('round trip with multiple endpoints', () => {
    const r = serviceRecord(new ServiceAddress(npc, 0xabcdef), { endpoints: uris });
    equalRecords(decodeServiceRecord(encodeServiceRecord(r)), r);
  });

  test('zero endpoints', () => {
    const r = serviceRecord(new ServiceAddress(npc, 1));
    equalRecords(decodeServiceRecord(encodeServiceRecord(r)), r);
    expect(decodeServiceRecord(encodeServiceRecord(r)).endpoints).toEqual([]);
  });

  test('255 endpoints', () => {
    const r = serviceRecord(new ServiceAddress(npc, 1), {
      endpoints: Array.from({ length: 255 }, (_, i) => `tcp://h${i}:1`),
    });
    const back = decodeServiceRecord(encodeServiceRecord(r));
    expect(back.endpoints.length).toBe(255);
    equalRecords(back, r);
  });

  test('256 endpoints rejected on encode', () => {
    const r = serviceRecord(new ServiceAddress(npc, 1), {
      endpoints: Array.from({ length: 256 }, () => 'tcp://h:1'),
    });
    expect(() => encodeServiceRecord(r)).toThrow(RangeError);
  });

  test('percent-encoded non-ASCII URI', () => {
    // The text form the Dart reference sends for `ws://example.org/é/日本`.
    const r = serviceRecord(new ServiceAddress(npc, 1), {
      endpoints: ['ws://example.org/%C3%A9/%E6%97%A5%E6%9C%AC'],
    });
    const back = decodeServiceRecord(encodeServiceRecord(r));
    equalRecords(back, r);
    expect(back.endpoints[0]).toBe(r.endpoints[0]);
  });

  test('instance 0 rejected on decode and encode', () => {
    const encoded = encodeServiceRecord(serviceRecord(new ServiceAddress(npc, 1)));
    encoded.fill(0, 8, 14);
    expect(() => decodeServiceRecord(encoded)).toThrow(ProtocolError);
    expect(() => encodeServiceRecord(serviceRecord(new ServiceAddress(npc)))).toThrow(RangeError);
  });

  test('trailing bytes ignored', () => {
    const r = serviceRecord(new ServiceAddress(npc, 1), { endpoints: uris });
    equalRecords(decodeServiceRecord(Uint8Array.of(...encodeServiceRecord(r), 1, 2, 3)), r);
  });

  test('truncation at every boundary', () => {
    const record = serviceRecord(new ServiceAddress(npc, 1), {
      endpoints: uris,
      metadata: bytes([1, 2, 3]),
    });
    const encoded = encodeServiceRecord(record);
    // Name, instance, endpoints: the layout before metadata was added.
    const withoutMetadata = encoded.length - 2 - 3;
    for (let n = 0; n < encoded.length; n++) {
      const prefix = encoded.subarray(0, n);
      if (n === withoutMetadata) {
        const old = decodeServiceRecord(prefix);
        expect(old.endpoints).toEqual(uris);
        expect(old.metadata.length).toBe(0);
        continue;
      }
      expect(() => decodeServiceRecord(prefix), `prefix ${n}`).toThrow(ProtocolError);
    }
    equalRecords(decodeServiceRecord(encoded), record);
  });

  describe('metadata', () => {
    test('round trip and equality', () => {
      const address = new ServiceAddress(npc, 7);
      const record = serviceRecord(address, { endpoints: uris, metadata: bytes([0, 0xff, 7]) });
      const back = decodeServiceRecord(encodeServiceRecord(record));
      equalRecords(back, record);
      expect([...back.metadata]).toEqual([0, 0xff, 7]);
      expect(
        serviceRecordsEqual(
          record,
          serviceRecord(address, { endpoints: uris, metadata: bytes([0, 0xff]) }),
        ),
      ).toBe(false);
      expect(serviceRecordsEqual(record, serviceRecord(address, { endpoints: uris }))).toBe(false);
      expect(
        serviceRecordsEqual(
          serviceRecord(address, { metadata: new Uint8Array(0) }),
          serviceRecord(address),
        ),
      ).toBe(true);
    });

    test('the empty field is two zero bytes', () => {
      const encoded = encodeServiceRecord(serviceRecord(new ServiceAddress(npc, 1)));
      expect(encoded.length).toBe(8 + 6 + 1 + 2);
      expect([...encoded.subarray(15)]).toEqual([0, 0]);
    });

    test('at most maxMetadataLength bytes', () => {
      expect(maxMetadataLength).toBe(4096);
      const address = new ServiceAddress(npc, 1);
      const largest = serviceRecord(address, {
        metadata: new Uint8Array(maxMetadataLength).fill(0x5a),
      });
      equalRecords(decodeServiceRecord(encodeServiceRecord(largest)), largest);
      const tooLarge = new Uint8Array(maxMetadataLength + 1);
      expect(() => encodeServiceRecord(serviceRecord(address, { metadata: tooLarge }))).toThrow(
        RangeError,
      );
      expect(() => new RegisterRequest(npc, { metadata: tooLarge }).encode()).toThrow(RangeError);
      // A decoder refuses what an encoder would.
      const w = new ByteWriter()
        .name(npc)
        .u48(1)
        .u8(0)
        .u16(maxMetadataLength + 1)
        .bytes(tooLarge)
        .toBytes();
      expect(() => decodeServiceRecord(w)).toThrow(ProtocolError);
      expect(() => RegisterRequest.decode(w)).toThrow(ProtocolError);
    });

    test('a field cut short is a protocol error', () => {
      const full = encodeServiceRecord(
        serviceRecord(new ServiceAddress(npc, 1), { metadata: bytes([1, 2, 3]) }),
      );
      // One byte of the length, the length without all its bytes.
      for (const n of [16, 17, 18, 19]) {
        expect(() => decodeServiceRecord(full.subarray(0, n)), `prefix ${n}`).toThrow(
          ProtocolError,
        );
      }
    });

    test('trailing bytes after the metadata are ignored', () => {
      const record = serviceRecord(new ServiceAddress(npc, 1), { metadata: bytes([9]) });
      equalRecords(
        decodeServiceRecord(Uint8Array.of(...encodeServiceRecord(record), 1, 2)),
        record,
      );
    });

    test('a decoded record does not share the input buffer', () => {
      const encoded = encodeServiceRecord(
        serviceRecord(new ServiceAddress(npc, 1), { metadata: bytes([1, 2, 3]) }),
      );
      const record = decodeServiceRecord(encoded);
      encoded.fill(0);
      expect([...record.metadata]).toEqual([1, 2, 3]);
    });
  });

  test('records are frozen', () => {
    const record = serviceRecord(new ServiceAddress(npc, 1), { endpoints: uris });
    expect(Object.isFrozen(record)).toBe(true);
    expect(Object.isFrozen(record.endpoints)).toBe(true);
  });
});

describe('RegisterRequest', () => {
  test('round trip', () => {
    const encoded = new RegisterRequest(npc, {
      requestedInstance: 0xffffffffffff,
      endpoints: uris,
    }).encode();
    const r = RegisterRequest.decode(encoded);
    expect(r.type.equals(npc)).toBe(true);
    expect(r.requestedInstance).toBe(0xffffffffffff);
    expect(r.endpoints).toEqual(uris);
  });

  test('zero endpoints and zero instance are valid', () => {
    const r = RegisterRequest.decode(new RegisterRequest(npc).encode());
    expect(r.requestedInstance).toBe(0);
    expect(r.endpoints).toEqual([]);
    expect(r.metadata.length).toBe(0);
  });

  test('metadata round trip', () => {
    const r = RegisterRequest.decode(
      new RegisterRequest(npc, { endpoints: uris, metadata: bytes([4, 5]) }).encode(),
    );
    expect(r.endpoints).toEqual(uris);
    expect([...r.metadata]).toEqual([4, 5]);
  });

  test('truncation at every byte boundary throws ProtocolError, except before the metadata', () => {
    const encoded = new RegisterRequest(npc, {
      requestedInstance: 7,
      endpoints: uris,
      metadata: bytes([1]),
    }).encode();
    const withoutMetadata = encoded.length - 2 - 1;
    for (let n = 0; n < encoded.length; n++) {
      const prefix = encoded.subarray(0, n);
      if (n === withoutMetadata) {
        expect(RegisterRequest.decode(prefix).metadata.length).toBe(0);
        continue;
      }
      expect(() => RegisterRequest.decode(prefix), `prefix ${n}`).toThrow(ProtocolError);
    }
    expect(RegisterRequest.decode(encoded).endpoints).toEqual(uris);
    expect([...RegisterRequest.decode(encoded).metadata]).toEqual([1]);
  });

  test('an instance outside u48 is refused on encode', () => {
    expect(() => new RegisterRequest(npc, { requestedInstance: 2 ** 48 }).encode()).toThrow(
      RangeError,
    );
  });
});

describe('RegisterResponse', () => {
  test('round trip and truncation', () => {
    const encoded = new RegisterResponse(0x123456789a).encode();
    expect(RegisterResponse.decode(encoded).instance).toBe(0x123456789a);
    for (let n = 0; n < 6; n++) {
      expect(() => RegisterResponse.decode(encoded.subarray(0, n))).toThrow(ProtocolError);
    }
  });
});

describe('UnregisterRequest', () => {
  test('round trip and truncation', () => {
    const encoded = new UnregisterRequest(npc, 42).encode();
    const r = UnregisterRequest.decode(encoded);
    expect(r.type.equals(npc)).toBe(true);
    expect(r.instance).toBe(42);
    for (let n = 0; n < encoded.length; n++) {
      expect(() => UnregisterRequest.decode(encoded.subarray(0, n))).toThrow(ProtocolError);
    }
  });
});

describe('WatchRequest', () => {
  test('no type is the empty payload', () => {
    expect(new WatchRequest().encode().length).toBe(0);
    expect(WatchRequest.decode(new Uint8Array(0)).type).toBeUndefined();
    expect(new WatchRequest('').type).toBeUndefined();
  });

  test('typed round trip', () => {
    expect(WatchRequest.decode(new WatchRequest(npc).encode()).type?.equals(npc)).toBe(true);
    expect(new WatchRequest('npc').type?.equals(npc)).toBe(true);
  });

  test('short payload is a protocol error', () => {
    expect(() => WatchRequest.decode(new Uint8Array(3))).toThrow(ProtocolError);
  });

  test('a name with a zero byte inside is a protocol error', () => {
    expect(() => WatchRequest.decode(Uint8Array.of(0x61, 0, 0x62, 0, 0, 0, 0, 0))).toThrow(
      ProtocolError,
    );
  });
});

describe('ServiceEvent', () => {
  test('UP round trip', () => {
    const e = upEvent(serviceRecord(new ServiceAddress(npc, 5), { endpoints: uris }));
    const back = decodeUpEvent(encodeServiceEvent(e));
    expect(back.up).toBe(true);
    equalRecords(back.record, e.record);
    expect(serviceEventProcedure(e).equals(Procedures.up)).toBe(true);
  });

  test('DOWN round trip drops endpoints', () => {
    const e: ServiceEvent = {
      up: false,
      record: serviceRecord(new ServiceAddress(npc, 5), { endpoints: uris }),
    };
    const back = decodeDownEvent(encodeServiceEvent(e));
    expect(back.up).toBe(false);
    expect(back.record.address.equals(new ServiceAddress(npc, 5))).toBe(true);
    expect(back.record.endpoints).toEqual([]);
    expect(serviceEventProcedure(e).equals(Procedures.down)).toBe(true);
    expect(hexString(encodeServiceEvent(back))).toBe(hexString(encodeServiceEvent(e)));
  });
});

describe('endpoint validation', () => {
  // `./>:x` has no scheme (the Dart reference reads it as a path).
  const relative = './>:x';
  // A backslash and a brace: the Dart reference rewrites both, and the
  // text form `tcp://%7B:` parses back without the empty port.
  const unstable = 'tcp:/.\\\\{:';
  const schemeless = '//10.0.0.5:9101/ws';
  // No authority and path `//` once the dot segments are removed; its text
  // form `tcp://` has an empty authority.
  const asymmetric = 'tcp:/../..//';

  function record(endpoint: string): Uint8Array {
    return new ByteWriter().name(npc).u48(5).u8(1).string8(endpoint).toBytes();
  }

  test('checkEndpoint accepts usual endpoints', () => {
    for (const uri of [
      ...uris,
      'wss://example.org:443/a/b?x=1#npc/2',
      'ws://[::1]:9100/',
      'ws://%C3%BC.example/%C3%A4',
      `tcp://${'a'.repeat(249)}`,
      'mem://name',
      'quic://10.0.0.5:4433',
      'https://edge.example.com:4433/sb',
      'ws://h/a/./b',
      'tcp://[fe80::1%25eth0]:1',
      'tcp://u:p@h:1',
      'urn:polyverse:ns',
    ]) {
      expect(() => checkEndpoint(uri), uri).not.toThrow();
    }
  });

  test('checkEndpoint refuses what a watcher could not decode', () => {
    for (const text of [
      relative,
      unstable,
      schemeless,
      asymmetric,
      `tcp://${'a'.repeat(250)}`,
      'ws://example.org/é',
      'tcp://h h',
      'tcp://h/a%zz',
      'tcp://h:x',
      'tcp://[zz]',
      'tcp://[::1',
      'tcp://h]',
      'tcp://h:9223372036854775808',
      'file:/x',
      'data:,x',
      '',
      'tcp:/./x',
    ]) {
      expect(() => checkEndpoint(text), text).toThrow(TypeError);
      expect(endpointProblem(text), text).toBeDefined();
    }
  });

  test('decoders refuse such endpoints', () => {
    for (const text of [relative, unstable, schemeless, asymmetric]) {
      const encoded = record(text);
      expect(() => decodeServiceRecord(encoded), text).toThrow(ProtocolError);
      expect(() => decodeUpEvent(encoded), text).toThrow(ProtocolError);
      // REGISTER has the same layout as a record.
      expect(() => RegisterRequest.decode(encoded), text).toThrow(ProtocolError);
    }
    expect(decodeServiceRecord(record('tcp://h:1')).endpoints).toEqual(['tcp://h:1']);
  });

  test('an endpoint that is not UTF-8 is a protocol error', () => {
    const encoded = new ByteWriter().name(npc).u48(5).u8(1).u8(2).bytes([0x61, 0xff]).toBytes();
    expect(() => decodeServiceRecord(encoded)).toThrow(ProtocolError);
  });

  test('rejects endpoints that would not re-encode within 255 bytes', () => {
    const raw = `tcp://h/${'a b'.repeat(80)}`;
    expect(raw.length).toBeLessThanOrEqual(255);
    expect(() => decodeServiceRecord(record(raw))).toThrow(ProtocolError);
  });

  // Endpoints run through the Dart reference's decoder (`Uri.parse` and the
  // stability check of `ServiceRecord.checkEndpoint`), from a grammar-based
  // generator of schemes, hosts, ports, segments, queries and fragments with
  // random mutations; see `fixtures/endpoint-oracle.dart`. Each row is the
  // input and the text form the reference sends, or null when it refuses it.
  test('agrees with the Dart reference', () => {
    const rows = JSON.parse(
      readFileSync(new URL('./fixtures/endpoint-oracle.json', import.meta.url), 'utf8'),
    ) as [string, string | null][];
    expect(rows.length).toBeGreaterThan(500);
    let accepted = 0;
    for (const [input, normal] of rows) {
      // Whatever is accepted here, the reference accepts.
      if (endpointProblem(input) === undefined) {
        accepted++;
        expect(normal, input).not.toBeNull();
      }
      // Whatever the reference sends is accepted here, except for the known
      // differences: data URIs, ports beyond 64 bits (which the reference
      // reads lazily, so it only refuses some of them), and dot segments
      // without an authority (which it only sometimes removes).
      if (
        normal !== null &&
        !/^data:/i.test(normal) &&
        !/:-?0*[0-9]{19,}(?:[/?#]|$)/.test(normal) &&
        !/^[^/]*:(?!\/\/)(?:[^?#]*\/)?\.{1,2}(?:[/?#]|$)/.test(normal)
      ) {
        expect(endpointProblem(normal), `${normal} from ${input}`).toBeUndefined();
      }
    }
    expect(accepted).toBeGreaterThan(100);
  });
});

describe('fuzz', () => {
  const decoders: [string, (bytes: Uint8Array) => unknown, (value: never) => Uint8Array][] = [
    ['ServiceRecord', decodeServiceRecord, (v: ServiceRecord) => encodeServiceRecord(v)],
    ['RegisterRequest', RegisterRequest.decode, (v: RegisterRequest) => v.encode()],
    ['RegisterResponse', RegisterResponse.decode, (v: RegisterResponse) => v.encode()],
    ['UnregisterRequest', UnregisterRequest.decode, (v: UnregisterRequest) => v.encode()],
    ['WatchRequest', WatchRequest.decode, (v: WatchRequest) => v.encode()],
    ['ServiceEvent.up', decodeUpEvent, (v: ServiceEvent) => encodeServiceEvent(v)],
    ['ServiceEvent.down', decodeDownEvent, (v: ServiceEvent) => encodeServiceEvent(v)],
  ];

  test.each(decoders.map((d, i) => [d[0], d, i] as const))(
    '%s: only ProtocolError escapes; decoded values are stable',
    (name, [, decode, encode], index) => {
      const random = new Random(index + 1);
      let decoded = 0;
      for (let i = 0; i < 3000; i++) {
        const input = fuzzInput(random, i);
        let value: unknown;
        try {
          value = decode(input);
        } catch (e) {
          if (e instanceof ProtocolError) {
            continue;
          }
          throw new Error(`${name} threw ${e} for ${hexString(input)}`);
        }
        decoded++;
        // Whatever decodes encodes again, and that decodes to the same.
        const again = encode(value as never);
        const back = decode(again);
        expect(hexString(encode(back as never)), hexString(input)).toBe(hexString(again));
      }
      expect(decoded).toBeGreaterThan(0);
    },
  );
});
