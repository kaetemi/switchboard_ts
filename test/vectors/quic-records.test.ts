// The test vectors of the wiki page "Polyverse Switchboard QUIC and
// WebTransport Binding", section "Test vectors", and the records of its
// "Example", as codecs byte for byte: a port of the codec half of
// `switchboard_quic`'s `test/vectors_test.dart`. The negative vectors that
// need connection state run through a connection of the binding in
// `quic-connection.test.ts`. The control messages of the core (PING, PONG,
// GOAWAY, NONCE, IDENT) are checked here as the records that carry them.
// The wiki's vectors are read from `wiki-vectors.json` by id (through
// `test/support/vectors.ts`), so `coverage.test.ts` checks every table of
// the page; test names carry the ids.
import { describe, expect, test } from 'vitest';
import { ChannelAddress } from '../../src/core/address/channel-address.ts';
import { concatBytes, hexBytes, hexString } from '../../src/core/bytes.ts';
import { Name } from '../../src/core/name.ts';
import {
  decodeCloseBody,
  decodeOpenBody,
  encodeCloseRecord,
  encodeDataRecord,
  encodeOpenRecord,
  encodeQuicRecord,
  encodeQuicVarint,
  QuicHello,
  QuicRecordCommand,
  type QuicStream,
  quicDefaultRecordLimit,
  quicErrorCode,
  quicHeaderCommand,
  quicHeaderLength,
  quicHelloType,
  quicMaxRecordLength,
  RecordReader,
  RecordTooLarge,
  rawQuicExporterLabel,
  readQuicRecordHeader,
  statusOfQuicErrorCode,
  webTransportCodeFromHttp3,
  webTransportCodeToHttp3,
  webTransportExporterContext,
  webTransportTlsExporterLabel,
  writeQuicRecordHeader,
} from '../../src/core/quic/index.ts';
import { ProtocolError, Status, StatusCode } from '../../src/core/status.ts';
import { FakeQuicSession } from '../../src/testing/fake-quic-provider.ts';
import { type WikiVector, wikiBytes, wikiVector, wikiVectors } from '../support/vectors.ts';

/** C1 of the page "Polyverse Switchboard Test Vectors". */
const c1 =
  '01 01 6B 31 00 00 00 00 00 00 05 6E 70 63 2D 37 ' +
  '00 B9 55 69 80 29 56 69 00 03 01 6E 70 63 00 00 ' +
  '00 00 00 02 2A 00 00 00 00 00 00 00 03 6E 70 63 ' +
  '2D 2A 00 00 00 01 84 DB E9 02 5A 34 E5 C3 7A C8 ' +
  'BC C2 B7 7C 7B 6D E6 84 51 7A F1 7B C8 41 66 34 ' +
  '8A BE 22 D9 E3 C9';

const hex = (data: ArrayLike<number>): string => hexString(data);

/** A stream that returns [data] in chunks of at most [chunk] bytes, and records how much was asked of it. */
class ScriptedStream implements QuicStream {
  offset = 0;
  readBytes = 0;
  priority = 0;
  readonly stopped = new Promise<number | undefined>(() => {});
  readonly resetReceived = new Promise<number | undefined>(() => {});

  readonly data: Uint8Array;
  readonly chunk: number;

  constructor(data: Uint8Array, chunk = 1 << 30) {
    this.data = data;
    this.chunk = chunk;
  }

  async read(max: number): Promise<Uint8Array | undefined> {
    if (this.offset >= this.data.length) {
      return undefined;
    }
    const n = Math.min(this.data.length - this.offset, max, this.chunk);
    const out = this.data.subarray(this.offset, this.offset + n);
    this.offset += n;
    this.readBytes += n;
    return out;
  }

  async write(): Promise<void> {}
  async finish(): Promise<void> {}
  reset(): void {}
  stop(): void {}
}

const decode = (bytes: string, limit = 1 << 20) =>
  new RecordReader(new ScriptedStream(hexBytes(bytes))).next(limit);

/** A control message on the wire: its type byte, then its payload. */
const control = (type: number, payload: Uint8Array): Uint8Array =>
  concatBytes(Uint8Array.of(type), payload);

describe('records', () => {
  test('quic-binding/test-vectors/records/data-subframe-01-02-03', async () => {
    const vector = hex(wikiBytes('quic-binding/test-vectors/records/data-subframe-01-02-03'));
    expect(hex(encodeDataRecord([1, 2, 3]))).toBe(vector);
    const record = await decode(vector);
    expect(record?.command).toBe(QuicRecordCommand.data);
    expect([...(record?.body ?? [])]).toEqual([1, 2, 3]);
  });

  test('quic-binding/test-vectors/records/data-empty-subframe', async () => {
    const vector = hex(wikiBytes('quic-binding/test-vectors/records/data-empty-subframe'));
    expect(hex(encodeDataRecord([]))).toBe(vector);
    const record = await decode(vector);
    expect(record?.command).toBe(QuicRecordCommand.data);
    expect(record?.body.length).toBe(0);
  });

  test('quic-binding/test-vectors/records/data-header-of-a-32752-byte-subframe', () => {
    const vector = hex(
      wikiBytes('quic-binding/test-vectors/records/data-header-of-a-32752-byte-subframe'),
    );
    const header = new Uint8Array(4);
    writeQuicRecordHeader(header, 0, QuicRecordCommand.data, 32752);
    expect(hex(header)).toBe(vector);
    expect(hex(encodeDataRecord(new Uint8Array(32752)).subarray(0, 4))).toBe(hex(header));
  });

  test('quic-binding/test-vectors/records/open-channel-0-the-control-stream-empty-open-payload', async () => {
    const vector = hex(
      wikiBytes(
        'quic-binding/test-vectors/records/open-channel-0-the-control-stream-empty-open-payload',
      ),
    );
    expect(hex(encodeOpenRecord(0, []))).toBe(vector);
    const record = await decode(vector);
    expect(record?.command).toBe(QuicRecordCommand.open);
    const { id, openPayload } = decodeOpenBody(record?.body ?? new Uint8Array(0));
    expect(id).toBe(0);
    expect(openPayload.length).toBe(0);
  });

  test('quic-binding/test-vectors/records/open-channel-2-open-payload-00-default-service', () => {
    const vector = hex(
      wikiBytes('quic-binding/test-vectors/records/open-channel-2-open-payload-00-default-service'),
    );
    expect(hex(encodeOpenRecord(2, new ChannelAddress().encode()))).toBe(vector);
  });

  test('quic-binding/test-vectors/records/open-channel-3-service-api-application-payload-01-02', async () => {
    const payload = new ChannelAddress({ type: 'api', payload: Uint8Array.of(1, 2) }).encode();
    const vector = hex(
      wikiBytes(
        'quic-binding/test-vectors/records/open-channel-3-service-api-application-payload-01-02',
      ),
    );
    expect(hex(encodeOpenRecord(3, payload))).toBe(vector);
    const record = await decode(vector);
    const { id, openPayload } = decodeOpenBody(record?.body ?? new Uint8Array(0));
    expect(id).toBe(3);
    const address = ChannelAddress.decode(openPayload);
    expect(address.type?.equals(Name.of('api'))).toBe(true);
    expect([...address.payload]).toEqual([1, 2]);
  });

  test('quic-binding/test-vectors/records/open-channel-0x1234567890ab-empty-open-payload', async () => {
    const vector = hex(
      wikiBytes('quic-binding/test-vectors/records/open-channel-0x1234567890ab-empty-open-payload'),
    );
    expect(hex(encodeOpenRecord(0x1234567890ab, []))).toBe(vector);
    const record = await decode(vector);
    expect(decodeOpenBody(record?.body ?? new Uint8Array(0)).id).toBe(0x1234567890ab);
  });

  test('quic-binding/test-vectors/records/close-no-status-payload-ok', async () => {
    const vector = hex(wikiBytes('quic-binding/test-vectors/records/close-no-status-payload-ok'));
    expect(hex(encodeCloseRecord(Status.ok))).toBe(vector);
    const record = await decode(vector);
    expect(record?.command).toBe(QuicRecordCommand.close);
    expect(decodeCloseBody(record?.body ?? new Uint8Array(0)).equals(Status.ok)).toBe(true);
  });

  test('quic-binding/test-vectors/records/close-ok-no-reason', async () => {
    const vector = hex(wikiBytes('quic-binding/test-vectors/records/close-ok-no-reason'));
    expect(hex(encodeQuicRecord(QuicRecordCommand.close, Status.ok.encode()))).toBe(vector);
    const record = await decode(vector);
    expect(decodeCloseBody(record?.body ?? new Uint8Array(0)).isOk).toBe(true);
  });

  test('quic-binding/test-vectors/records/close-not-found-reason-x', async () => {
    const vector = hex(wikiBytes('quic-binding/test-vectors/records/close-not-found-reason-x'));
    const status = Status.of(StatusCode.notFound, 'x');
    expect(hex(encodeCloseRecord(status))).toBe(vector);
    const record = await decode(vector);
    expect(decodeCloseBody(record?.body ?? new Uint8Array(0)).equals(status)).toBe(true);
  });

  test('quic-binding/test-vectors/records/close-moved-owner-0x1a2b-epoch-7', async () => {
    // MOVED with its fixed fields (u48 owner 0x1A2B, u32 epoch 7) and no reason.
    const status = Status.decode(hexBytes('25 00 2B 1A 00 00 00 00 07 00 00 00'));
    const vector = hex(
      wikiBytes('quic-binding/test-vectors/records/close-moved-owner-0x1a2b-epoch-7'),
    );
    expect(hex(encodeCloseRecord(status))).toBe(vector);
    const decoded = decodeCloseBody((await decode(vector))?.body ?? new Uint8Array(0));
    expect(decoded.known).toBe(StatusCode.moved);
    expect(hex(decoded.body)).toBe('2B 1A 00 00 00 00 07 00 00 00');
  });

  test('FF FF FF 3F is DATA of 1073741823 bytes, over every limit, and FF FF FF 7F OPEN of as many; the body is not read', async () => {
    for (const [vector, command] of [
      [wikiBytes('quic-binding/test-vectors/records/prose-ff-ff-ff-3f'), QuicRecordCommand.data],
      [wikiBytes('quic-binding/test-vectors/records/prose-ff-ff-ff-7f'), QuicRecordCommand.open],
    ] as const) {
      const stream = new ScriptedStream(concatBytes(vector, [1, 2, 3]));
      const header = readQuicRecordHeader(vector);
      expect(quicHeaderCommand(header)).toBe(command);
      expect(quicHeaderLength(header)).toBe(1073741823);
      const error = await new RecordReader(stream).next(quicMaxRecordLength - 1).catch((e) => e);
      expect(error).toBeInstanceOf(RecordTooLarge);
      expect((error as RecordTooLarge).length).toBe(1073741823);
      expect((error as RecordTooLarge).command).toBe(command);
      expect(stream.readBytes).toBe(4);
    }
  });

  test('a record arriving in pieces is reassembled; only its bytes are read', async () => {
    const stream = new ScriptedStream(hexBytes('05 00 00 00 01 02 03 04 05 00 00 00 80'), 2);
    const reader = new RecordReader(stream);
    const record = await reader.next(100);
    expect([...(record?.body ?? [])]).toEqual([1, 2, 3, 4, 5]);
    expect(stream.readBytes).toBe(9);
    expect((await reader.next(100))?.command).toBe(QuicRecordCommand.close);
    expect(await reader.next(100)).toBeUndefined();
  });

  test('a body larger than the read chunk is read in chunks of at most 256 KiB', async () => {
    const length = 600 * 1024;
    const data = concatBytes(encodeDataRecord(new Uint8Array(length).fill(7)));
    const asked: number[] = [];
    const stream = new ScriptedStream(data);
    const read = stream.read.bind(stream);
    stream.read = (max: number) => {
      asked.push(max);
      return read(max);
    };
    const record = await new RecordReader(stream).next(1 << 20);
    expect(record?.body.length).toBe(length);
    expect(record?.body.every((b) => b === 7)).toBe(true);
    expect(asked).toEqual([4, 256 * 1024, 256 * 1024, 88 * 1024]);
  });

  test('expectEnd: FIN or a reset after the CLOSE record end the direction; a record is a protocol error', async () => {
    await expect(new RecordReader(new ScriptedStream(new Uint8Array(0))).expectEnd()).resolves.toBe(
      undefined,
    );
    await expect(
      new RecordReader(new ScriptedStream(hexBytes('00 00 00 00'))).expectEnd(),
    ).rejects.toBeInstanceOf(ProtocolError);
  });

  describe('negative (protocol error)', () => {
    test('quic-binding/test-vectors/records/command-3', async () => {
      const vector = hex(wikiBytes('quic-binding/test-vectors/records/command-3'));
      await expect(decode(vector)).rejects.toBeInstanceOf(ProtocolError);
    });

    test('quic-binding/test-vectors/records/close-with-a-one-byte-body', async () => {
      const vector = hex(wikiBytes('quic-binding/test-vectors/records/close-with-a-one-byte-body'));
      const record = await decode(vector);
      expect(() => decodeCloseBody(record?.body ?? new Uint8Array(0))).toThrow(ProtocolError);
    });

    test('quic-binding/test-vectors/records/open-with-a-truncated-channel-id', async () => {
      const vector = hex(
        wikiBytes('quic-binding/test-vectors/records/open-with-a-truncated-channel-id'),
      );
      const record = await decode(vector);
      expect(() => decodeOpenBody(record?.body ?? new Uint8Array(0))).toThrow(ProtocolError);
    });

    test('FIN inside a record', async () => {
      await expect(decode('03 00 00 00 01')).rejects.toBeInstanceOf(ProtocolError);
      await expect(decode('03 00')).rejects.toBeInstanceOf(ProtocolError);
    });
  });

  test('argument errors: lengths outside the header, ids outside u48, application codes', () => {
    expect(() =>
      encodeQuicRecord(QuicRecordCommand.data, { length: quicMaxRecordLength + 1 }),
    ).toThrow(RangeError);
    expect(() => encodeOpenRecord(2 ** 48, [])).toThrow(RangeError);
    expect(() => encodeOpenRecord(-1, [])).toThrow(RangeError);
    expect(() => encodeCloseRecord(new Status(256))).toThrow(RangeError);
  });

  test('a CLOSE status is prepared for the wire: CONNECTION_LOST as UNAVAILABLE, the payload within 1024 bytes on a character boundary', () => {
    expect(hex(encodeCloseRecord(Status.of(StatusCode.connectionLost, 'x')))).toBe(
      '03 00 00 80 0E 00 78',
    );
    const long = Status.of(StatusCode.internal, `a${'é'.repeat(600)}`);
    const record = encodeCloseRecord(long);
    const body = record.subarray(4);
    expect(body.length).toBeLessThanOrEqual(1024);
    const decoded = decodeCloseBody(body);
    expect(decoded.known).toBe(StatusCode.internal);
    expect(decoded.reason.endsWith('é')).toBe(true);
    expect(decoded.reason.includes('�')).toBe(false);
  });

  test('a received application code in a CLOSE reads as UNKNOWN', () => {
    const decoded = decodeCloseBody(hexBytes('00 01 78'));
    expect(decoded.known).toBe(StatusCode.unknown);
  });
});

describe('control stream', () => {
  test('quic-binding/test-vectors/control-stream/hello-record-limit-32752-no-stated-channel-limit-no-flags', () => {
    const hello = new QuicHello();
    const vector = hex(
      wikiBytes(
        'quic-binding/test-vectors/control-stream/hello-record-limit-32752-no-stated-channel-limit-no-flags',
      ),
    );
    expect(hex(encodeDataRecord(hello.encodeMessage()))).toBe(vector);
    expect(QuicHello.decode(hexBytes(vector).subarray(5)).equals(hello)).toBe(true);
    expect(hexBytes(vector)[4]).toBe(quicHelloType);
  });

  test('quic-binding/test-vectors/control-stream/hello-record-limit-1-mib-100-channels-exporter', () => {
    const hello = new QuicHello({ recordLimit: 1 << 20, channels: 100, exporter: true });
    const vector = hex(
      wikiBytes(
        'quic-binding/test-vectors/control-stream/hello-record-limit-1-mib-100-channels-exporter',
      ),
    );
    expect(hex(encodeDataRecord(hello.encodeMessage()))).toBe(vector);
    expect(QuicHello.decode(hexBytes(vector).subarray(5)).equals(hello)).toBe(true);
  });

  test('quic-binding/test-vectors/control-stream/hello-with-a-further-field-aa-ignored', () => {
    const vector = hex(
      wikiBytes('quic-binding/test-vectors/control-stream/hello-with-a-further-field-aa-ignored'),
    );
    const decoded = QuicHello.decode(hexBytes(vector).subarray(5));
    expect(decoded.equals(new QuicHello())).toBe(true);
  });

  test('HELLO: other flag bits are ignored, a record limit above 0x3FFFFFFF reads as it', () => {
    const decoded = QuicHello.decode(hexBytes('FF FF FF FF 00 00 00 00 FE'));
    expect(decoded.recordLimit).toBe(quicMaxRecordLength);
    expect(decoded.exporter).toBe(false);
  });

  test('HELLO: a record limit below 32752 or outside u32 is refused when built', () => {
    expect(() => new QuicHello({ recordLimit: quicDefaultRecordLimit - 1 })).toThrow(RangeError);
    expect(() => new QuicHello({ recordLimit: 2 ** 32 })).toThrow(RangeError);
    expect(() => new QuicHello({ channels: -1 })).toThrow(RangeError);
    expect(new QuicHello({ recordLimit: 0xffffffff }).recordLimit).toBe(0xffffffff);
  });

  test('quic-binding/test-vectors/control-stream/ping-payload-de-ad and pong-payload-de-ad', () => {
    expect(hex(encodeDataRecord(control(0x01, hexBytes('DE AD'))))).toBe(
      hex(wikiBytes('quic-binding/test-vectors/control-stream/ping-payload-de-ad')),
    );
    expect(hex(encodeDataRecord(control(0x02, hexBytes('DE AD'))))).toBe(
      hex(wikiBytes('quic-binding/test-vectors/control-stream/pong-payload-de-ad')),
    );
  });

  test('quic-binding/test-vectors/control-stream/goaway-going-away-no-reason', () => {
    const vector = hex(
      wikiBytes('quic-binding/test-vectors/control-stream/goaway-going-away-no-reason'),
    );
    expect(hex(encodeDataRecord(control(0x03, Status.of(StatusCode.goingAway).encode())))).toBe(
      vector,
    );
  });

  test('quic-binding/test-vectors/control-stream/nonce-32-bytes-of-11', () => {
    const vector = hex(wikiBytes('quic-binding/test-vectors/control-stream/nonce-32-bytes-of-11'));
    expect(vector).toBe(`21 00 00 00 05 ${Array(32).fill('11').join(' ')}`);
    expect(hex(encodeDataRecord(control(0x05, new Uint8Array(32).fill(0x11))))).toBe(vector);
  });

  test('quic-binding/test-vectors/control-stream/ident-credential-c1-of-test-vectors-no-intent-no-receiver-no-proof', () => {
    // IDENT: u16 credential length, the credential, len8 intent, len8 receiver, proof.
    const credential = hexBytes(c1);
    const payload = concatBytes(
      Uint8Array.of(credential.length, 0),
      credential,
      Uint8Array.of(0, 0),
    );
    const vector = hex(
      wikiBytes(
        'quic-binding/test-vectors/control-stream/ident-credential-c1-of-test-vectors-no-intent-no-receiver-no-proof',
      ),
    );
    expect(vector).toBe(`5B 00 00 00 06 56 00 ${c1} 00 00`);
    expect(hex(encodeDataRecord(control(0x06, payload)))).toBe(vector);
  });

  test('negative HELLOs: 8 bytes, a record limit of 1024', () => {
    for (const vector of wikiVectors(
      'quic-binding/test-vectors/control-stream/prose-09-00-00-00-08-f0-7f-00-00-00-00-00-00',
      'quic-binding/test-vectors/control-stream/prose-0a-00-00-00-08-00-04-00-00-00-00-00-00-00',
    )) {
      expect(() => QuicHello.decode(hexBytes(vector.hex ?? '').subarray(5))).toThrow(ProtocolError);
    }
  });
});

describe('bulk channels', () => {
  test('quic-binding/test-vectors/bulk-channels/open-channel-3-bulk-for-parent-channel-2-bulk-number-1', () => {
    const vector = hex(
      wikiBytes(
        'quic-binding/test-vectors/bulk-channels/open-channel-3-bulk-for-parent-channel-2-bulk-number-1',
      ),
    );
    // The `_bulk` open payload of the page "Test Vectors", unchanged.
    const payload = new ChannelAddress({
      type: '_bulk',
      payload: hexBytes('02 00 00 00 00 00 01 00 00 00'),
    }).encode();
    expect(hex(encodeOpenRecord(3, payload))).toBe(vector);
  });

  test('quic-binding/test-vectors/bulk-channels/open-channel-4-bulk-for-parent-channel-2-bulk-number-2', () => {
    const vector = hex(
      wikiBytes(
        'quic-binding/test-vectors/bulk-channels/open-channel-4-bulk-for-parent-channel-2-bulk-number-2',
      ),
    );
    const payload = new ChannelAddress({
      type: '_bulk',
      payload: hexBytes('02 00 00 00 00 00 02 00 00 00'),
    }).encode();
    expect(hex(encodeOpenRecord(4, payload))).toBe(vector);
  });
});

describe('status codes', () => {
  const table: [number, string, number, WikiVector][] = [
    [0, '00', 0x52e4a40fa8db, wikiVector('quic-binding/test-vectors/status-codes/0-ok')],
    [1, '01', 0x52e4a40fa8dc, wikiVector('quic-binding/test-vectors/status-codes/1-cancelled')],
    [5, '05', 0x52e4a40fa8e0, wikiVector('quic-binding/test-vectors/status-codes/5-not-found')],
    [
      8,
      '08',
      0x52e4a40fa8e3,
      wikiVector('quic-binding/test-vectors/status-codes/8-resource-exhausted'),
    ],
    [14, '0E', 0x52e4a40fa8e9, wikiVector('quic-binding/test-vectors/status-codes/14-unavailable')],
    [
      16,
      '10',
      0x52e4a40fa8eb,
      wikiVector('quic-binding/test-vectors/status-codes/16-unauthenticated'),
    ],
    [
      32,
      '20',
      0x52e4a40fa8fc,
      wikiVector('quic-binding/test-vectors/status-codes/32-protocol-error'),
    ],
    [33, '21', 0x52e4a40fa8fd, wikiVector('quic-binding/test-vectors/status-codes/33-going-away')],
    [
      35,
      '23',
      0x52e4a40fa8ff,
      wikiVector('quic-binding/test-vectors/status-codes/35-frame-too-large'),
    ],
    [36, '24', 0x52e4a40fa900, wikiVector('quic-binding/test-vectors/status-codes/36-unsupported')],
    [37, '25', 0x52e4a40fa901, wikiVector('quic-binding/test-vectors/status-codes/37-moved')],
    [
      255,
      '40 FF',
      0x52e4a40fa9e2,
      wikiVector('quic-binding/test-vectors/status-codes/255-the-largest-sent'),
    ],
  ];
  for (const [status, raw, http3, vector] of table) {
    test(vector.id, () => {
      expect(quicErrorCode(new Status(status))).toBe(status);
      expect(hex(encodeQuicVarint(status))).toBe(raw);
      expect(webTransportCodeToHttp3(status)).toBe(http3);
      expect(hex(encodeQuicVarint(http3))).toBe(vector.hex);
      expect(webTransportCodeFromHttp3(http3)).toBe(status);
      expect(statusOfQuicErrorCode(status).code).toBe(status);
    });
  }

  test('the mapping skips the reserved HTTP/3 codepoints', () => {
    expect(webTransportCodeToHttp3(29)).toBe(0x52e4a40fa8f8);
    expect(webTransportCodeToHttp3(30)).toBe(0x52e4a40fa8fa);
    expect(webTransportCodeFromHttp3(0x52e4a40fa8f9)).toBeUndefined();
    for (let code = 0; code < 1000; code++) {
      expect(webTransportCodeFromHttp3(webTransportCodeToHttp3(code))).toBe(code);
    }
    expect(webTransportCodeFromHttp3(0x52e4a40fa8da)).toBeUndefined();
    expect(webTransportCodeFromHttp3(webTransportCodeToHttp3(0xffffffff))).toBe(0xffffffff);
    expect(webTransportCodeFromHttp3(webTransportCodeToHttp3(0xffffffff) + 1)).toBeUndefined();
  });

  test('a received code of 256 or more, or none, reads as UNKNOWN', () => {
    expect(statusOfQuicErrorCode(256).known).toBe(StatusCode.unknown);
    expect(statusOfQuicErrorCode(0x52e4a40fa8db).known).toBe(StatusCode.unknown);
    expect(statusOfQuicErrorCode(undefined).known).toBe(StatusCode.unknown);
  });

  test('only codes 0 to 255 are sent: CONNECTION_LOST as UNAVAILABLE, application codes never', () => {
    expect(quicErrorCode(Status.of(StatusCode.connectionLost))).toBe(StatusCode.unavailable);
    expect(() => quicErrorCode(new Status(256))).toThrow(RangeError);
  });

  test('variable-length integers of every size, and refusals', () => {
    expect(hex(encodeQuicVarint(63))).toBe('3F');
    expect(hex(encodeQuicVarint(64))).toBe('40 40');
    expect(hex(encodeQuicVarint(16383))).toBe('7F FF');
    expect(hex(encodeQuicVarint(16384))).toBe('80 00 40 00');
    expect(hex(encodeQuicVarint(0x3fffffff))).toBe('BF FF FF FF');
    expect(hex(encodeQuicVarint(0x40000000))).toBe('C0 00 00 00 40 00 00 00');
    expect(() => encodeQuicVarint(-1)).toThrow(RangeError);
    expect(() => encodeQuicVarint(2 ** 53)).toThrow(RangeError);
  });
});

describe('exporter', () => {
  test('quic-binding/test-vectors/exporter/webtransport-tls-exporter-label-exporter-webtransport-21-bytes', () => {
    const vector = hex(
      wikiBytes(
        'quic-binding/test-vectors/exporter/webtransport-tls-exporter-label-exporter-webtransport-21-bytes',
      ),
    );
    expect(hex(new TextEncoder().encode(webTransportTlsExporterLabel))).toBe(vector);
  });

  test('quic-binding/test-vectors/exporter/webtransport-tls-exporter-context-session-id-0-27-bytes', () => {
    const vector = hex(
      wikiBytes(
        'quic-binding/test-vectors/exporter/webtransport-tls-exporter-context-session-id-0-27-bytes',
      ),
    );
    expect(hex(webTransportExporterContext(0))).toBe(vector);
  });

  test('quic-binding/test-vectors/exporter/webtransport-tls-exporter-context-session-id-4', () => {
    const vector = hex(
      wikiBytes(
        'quic-binding/test-vectors/exporter/webtransport-tls-exporter-context-session-id-4',
      ),
    );
    expect(hex(webTransportExporterContext(4))).toBe(vector);
    expect(hex(webTransportExporterContext(2 ** 40 + 4).subarray(0, 8))).toBe(
      '00 00 01 00 00 00 00 04',
    );
    expect(() => webTransportExporterContext(-1)).toThrow(RangeError);
    expect(() => webTransportExporterContext(0, 'x'.repeat(256))).toThrow(RangeError);
  });

  test('quic-binding/test-vectors/exporter/raw-quic-tls-exporter-label-exporter-channel-binding-24-bytes-empty-context', () => {
    const vector = hex(
      wikiBytes(
        'quic-binding/test-vectors/exporter/raw-quic-tls-exporter-label-exporter-channel-binding-24-bytes-empty-context',
      ),
    );
    expect(hex(new TextEncoder().encode(rawQuicExporterLabel))).toBe(vector);
  });

  test('both ends of a session derive the same exporter, two sessions different ones', async () => {
    const [a, b] = FakeQuicSession.pair();
    const [c] = FakeQuicSession.pair();
    const empty = new Uint8Array(0);
    const x = await a.exportKeyingMaterial(rawQuicExporterLabel, empty, 32);
    expect(hex((await b.exportKeyingMaterial(rawQuicExporterLabel, empty, 32)) ?? [])).toBe(
      hex(x ?? []),
    );
    expect(hex((await c.exportKeyingMaterial(rawQuicExporterLabel, empty, 32)) ?? [])).not.toBe(
      hex(x ?? []),
    );
  });
});

describe('example', () => {
  test('the browser opens chat/1 and asks for its HISTORY', () => {
    // Its control stream: the OPEN record and its HELLO.
    expect(
      hex(concatBytes(encodeOpenRecord(0, []), encodeDataRecord(new QuicHello().encodeMessage()))),
    ).toBe('06 00 00 40 00 00 00 00 00 00 0A 00 00 00 08 F0 7F 00 00 00 00 00 00 00');
    // The hub's HELLO.
    expect(
      hex(
        encodeDataRecord(
          new QuicHello({ recordLimit: 1 << 20, channels: 100, exporter: true }).encodeMessage(),
        ),
      ),
    ).toBe('0A 00 00 00 08 00 00 10 00 64 00 00 00 01');
    // Channel 2 to chat/1; the request record carries the Talk frame as is.
    expect(
      hex(encodeOpenRecord(2, new ChannelAddress({ type: 'chat', instance: 1 }).encode())),
    ).toBe('15 00 00 40 02 00 00 00 00 00 03 63 68 61 74 00 00 00 00 01 00 00 00 00 00');
    expect(hex(encodeDataRecord(hexBytes('03 48 49 53 54 4F 52 59 00 01 00 00 14 00')))).toBe(
      '0E 00 00 00 03 48 49 53 54 4F 52 59 00 01 00 00 14 00',
    );
  });

  test('the hub answers with a bulk payload on channel 3', () => {
    // The payload in records of the browser's limit, then the close.
    const headers: string[] = [];
    let left = 100000;
    while (left > 0) {
      const n = Math.min(left, quicDefaultRecordLimit);
      const header = new Uint8Array(4);
      writeQuicRecordHeader(header, 0, QuicRecordCommand.data, n);
      headers.push(hex(header));
      left -= n;
    }
    expect(headers).toEqual(['F0 7F 00 00', 'F0 7F 00 00', 'F0 7F 00 00', 'D0 06 00 00']);
    expect(hex(encodeCloseRecord(Status.ok))).toBe('00 00 00 80');
  });
});
