// Vectors from the wiki page "Polyverse Switchboard Test Vectors", section
// "Talk messages", and the Dart `vectors/talk_vectors_test.dart` beyond them.
import { describe, expect, test } from 'vitest';
import { ChannelAddress } from '../../src/core/address/index.ts';
import { hexBytes, hexString } from '../../src/core/bytes.ts';
import { Name } from '../../src/core/name.ts';
import { ProtocolError, Status, StatusCode } from '../../src/core/status.ts';
import { TalkBulkOpen, TalkBulkReference } from '../../src/core/talk/talk-bulk.ts';
import { TalkFrame, TalkKind } from '../../src/core/talk/talk-frame.ts';

const minute = 60_000;

/** Positive wiki vectors, with the frame each one must decode to. */
const positive: [string, string, TalkFrame][] = [
  [
    'Plain message "HELLO", payload 01',
    '01 48 45 4C 4C 4F 00 00 00 01',
    new TalkFrame({ kind: TalkKind.message, procedure: Name.of('HELLO'), payload: hexBytes('01') }),
  ],
  [
    'Request "GET_PROF" id 1, empty payload',
    '03 47 45 54 5F 50 52 4F 46 01 00 00',
    new TalkFrame({ kind: TalkKind.message, procedure: Name.of('GET_PROF'), requestId: 1 }),
  ],
  [
    'Final response to 1, no procedure, payload FF',
    '04 01 00 00 FF',
    new TalkFrame({ kind: TalkKind.message, responseId: 1, payload: hexBytes('FF') }),
  ],
  [
    'Stream request "LIST" id 2, unlimited window',
    '0B 4C 49 53 54 00 00 00 00 02 00 00 FF FF FF FF',
    new TalkFrame({
      kind: TalkKind.message,
      procedure: Name.of('LIST'),
      requestId: 2,
      stream: true,
    }),
  ],
  [
    'Stream request "LIST" id 2, window 16',
    '0B 4C 49 53 54 00 00 00 00 02 00 00 10 00 00 00',
    new TalkFrame({
      kind: TalkKind.message,
      procedure: Name.of('LIST'),
      requestId: 2,
      stream: true,
      window: 16,
    }),
  ],
  [
    'Stream item "ITEM" to 2, payload 07',
    '15 49 54 45 4D 00 00 00 00 02 00 00 07',
    new TalkFrame({
      kind: TalkKind.streamItem,
      procedure: Name.of('ITEM'),
      responseId: 2,
      payload: hexBytes('07'),
    }),
  ],
  [
    'End of stream (final response) to 2, empty',
    '04 02 00 00',
    new TalkFrame({ kind: TalkKind.message, responseId: 2 }),
  ],
  [
    'Abort response to 1, NOT_FOUND, "nope"',
    '24 01 00 00 05 00 6E 6F 70 65',
    new TalkFrame({
      kind: TalkKind.abort,
      responseId: 1,
      payload: Status.of(StatusCode.notFound, 'nope').encode(),
    }),
  ],
  [
    'Extend response to 1 (restart the default timeout)',
    '34 01 00 00',
    new TalkFrame({ kind: TalkKind.extend, responseId: 1 }),
  ],
  [
    'Extend response to 1, deadline 900000 ms, renew 300000 ms',
    '34 01 00 00 A0 BB 0D 00 E0 93 04 00',
    new TalkFrame({
      kind: TalkKind.extend,
      responseId: 1,
      payload: TalkFrame.extendPayload({ deadline: 15 * minute, renew: 5 * minute }),
    }),
  ],
  [
    'Extend response to 1, deadline 900000 ms, renew unchanged',
    '34 01 00 00 A0 BB 0D 00 00 00 00 00',
    new TalkFrame({
      kind: TalkKind.extend,
      responseId: 1,
      payload: TalkFrame.extendPayload({ deadline: 15 * minute }),
    }),
  ],
  [
    'Credit for own stream request 2, 8 more items',
    '32 02 00 00 08 00 00 00',
    new TalkFrame({ kind: TalkKind.extend, requestId: 2, payload: TalkFrame.creditPayload(8) }),
  ],
  [
    'The same credit forwarded by a message chain proxy whose own request toward the backend is 5',
    '32 05 00 00 08 00 00 00',
    new TalkFrame({ kind: TalkKind.extend, requestId: 5, payload: TalkFrame.creditPayload(8) }),
  ],
  [
    'Cancel own request 2',
    '22 02 00 00 01 00',
    new TalkFrame({
      kind: TalkKind.abort,
      requestId: 2,
      payload: Status.of(StatusCode.cancelled).encode(),
    }),
  ],
  [
    'Channel abort, UNAUTHENTICATED, no reason',
    '20 10 00',
    new TalkFrame({
      kind: TalkKind.abort,
      payload: Status.of(StatusCode.unauthenticated).encode(),
    }),
  ],
  [
    'Chained: response to 1 that is request 7 "MORE"',
    '07 4D 4F 52 45 00 00 00 00 07 00 00 01 00 00',
    new TalkFrame({
      kind: TalkKind.message,
      procedure: Name.of('MORE'),
      requestId: 7,
      responseId: 1,
    }),
  ],
  [
    'Request id 0xFFFFFF "X"',
    '03 58 00 00 00 00 00 00 00 FF FF FF',
    new TalkFrame({ kind: TalkKind.message, procedure: Name.of('X'), requestId: 0xffffff }),
  ],
  [
    'Request "PUT" id 1 with a bulk payload: bulk number 1, 1048576 bytes',
    '43 50 55 54 00 00 00 00 00 01 00 00 01 00 00 00 00 00 10 00 00 00',
    new TalkFrame({
      kind: TalkKind.message,
      procedure: Name.of('PUT'),
      requestId: 1,
      bulk: true,
      payload: new TalkBulkReference(1, 1048576).encode(),
    }),
  ],
  [
    'Final response to 1 with a bulk payload: bulk number 1, length unknown',
    '44 01 00 00 01 00 00 00 FF FF FF FF FF FF',
    new TalkFrame({
      kind: TalkKind.message,
      responseId: 1,
      bulk: true,
      payload: new TalkBulkReference(1).encode(),
    }),
  ],
  [
    'Stream item to 2 with a bulk payload: bulk number 2, 0 bytes',
    '54 02 00 00 02 00 00 00 00 00 00 00 00 00',
    new TalkFrame({
      kind: TalkKind.streamItem,
      responseId: 2,
      bulk: true,
      payload: new TalkBulkReference(2, 0).encode(),
    }),
  ],
];

/** Negative wiki vectors (each must be a channel protocol error). */
const negative: [string, string][] = [
  ['BULK on an abort', '64 01 00 00 05 00'],
  ['BULK on an extend', '74 01 00 00'],
  ['BULK with a 4-byte reference', '43 50 55 54 00 00 00 00 00 01 00 00 01 00 00 00'],
  ['STREAM without HAS_REQUEST', '09 48 45 4C 4C 4F 00 00 00'],
  ['Stream request truncated before its window', '0B 4C 49 53 54 00 00 00 00 02 00 00 FF FF'],
  ['Request id 0', '03 48 45 4C 4C 4F 00 00 00 00 00 00'],
  ['STREAM_ITEM without HAS_RESPONSE', '11 48 45 4C 4C 4F 00 00 00'],
  ['EXTEND with both ids', '36 01 00 00 02 00 00'],
  ['EXTEND with neither id', '30'],
  ['Credit with an 8-byte payload', '32 02 00 00 A0 BB 0D 00 E0 93 04 00'],
  ['Credit of 0 items', '32 02 00 00 00 00 00 00'],
  ['STREAM on a cancel', '2A 02 00 00 01 00'],
  ['STREAM on a credit', '3A 02 00 00 08 00 00 00'],
  ['EXTEND with a 4-byte payload', '34 01 00 00 A0 BB 0D 00'],
  ['ABORT with both ids', '26 01 00 00 02 00 00 01 00'],
  ['Plain message without procedure', '00 01'],
  ['Truncated procedure', '01 48 45'],
];

/** Further violations of the flag rules in talk.md that are not wiki vectors. */
const negativeExtra: [string, string][] = [
  ['Reserved bit 0x80', '81 48 45 4C 4C 4F 00 00 00'],
  ['BULK with an empty reference', '41 48 45 4C 4C 4F 00 00 00'],
  ['BULK with an 11-byte reference', '41 48 45 4C 4C 4F 00 00 00 01 00 00 00 00 00 00 00 00 00 00'],
  ['BULK reference to bulk number 0', '41 48 45 4C 4C 4F 00 00 00 00 00 00 00 05 00 00 00 00 00'],
  ['BULK on a cancel', '62 02 00 00 01 00'],
  ['BULK on a channel abort', '60 10 00'],
  ['Empty input', ''],
  ['Response id 0', '04 00 00 00'],
  ['Chained with response id 0', '06 01 00 00 00 00 00'],
  ['EXTEND with STREAM and no request', '3C 01 00 00'],
  ['Credit with STREAM and a window', '3A 02 00 00 FF FF FF FF 08 00 00 00'],
  ['Cancel with STREAM and a window', '2A 02 00 00 FF FF FF FF 01 00'],
  ['Credit with an empty payload', '32 02 00 00'],
  ['Credit with a 3-byte payload', '32 02 00 00 08 00 00'],
  ['Credit with a 5-byte payload', '32 02 00 00 08 00 00 00 00'],
  ['Credit with a procedure and 0 items', '33 41 00 00 00 00 00 00 00 02 00 00 00 00 00 00'],
  ['Chained stream request without its window', '0E 04 00 00 03 00 00'],
  ['Item that is a stream request, truncated window', '1E 05 00 00 03 00 00 04 00 00'],
  ['EXTEND with a 2-byte payload', '34 01 00 00 AA BB'],
  ['EXTEND with a 9-byte payload', '34 01 00 00 01 00 00 00 01 00 00 00 00'],
  ['STREAM_ITEM with only a request id', '13 48 45 4C 4C 4F 00 00 00 01 00 00'],
  ['Request without procedure', '02 01 00 00'],
  ['ABORT with a one byte payload', '24 01 00 00 05'],
  ['Truncated request id', '03 48 45 4C 4C 4F 00 00 00 01 00'],
  ['Truncated response id', '04 01 00'],
  ['Procedure with a byte after the padding', '01 61 00 62 00 00 00 00 00'],
];

function decodeHex(hex: string): TalkFrame {
  return TalkFrame.decode(hexBytes(hex));
}

/** Decodes [hex], checks it encodes back to the same bytes, and returns the frame. */
function roundTrip(hex: string): TalkFrame {
  const frame = decodeHex(hex);
  expect(hexString(frame.encode())).toBe(hexString(hexBytes(hex)));
  return frame;
}

function expectProtocolError(action: () => unknown): void {
  let thrown: unknown;
  try {
    action();
  } catch (e) {
    thrown = e;
  }
  expect(thrown).toBeInstanceOf(ProtocolError);
  expect((thrown as ProtocolError).code).toBe(StatusCode.protocolError);
}

describe('Talk vectors, positive', () => {
  test.each(positive)('%s', (_name, hex, expected) => {
    const bytes = hexBytes(hex);
    const frame = TalkFrame.decode(bytes);
    expect(frame.equals(expected), `${frame} equals ${expected}`).toBe(true);
    expect(hexString(frame.encode())).toBe(hexString(bytes));
    expect(hexString(expected.encode())).toBe(hexString(bytes));
  });

  test('bulk references decode', () => {
    expect(
      decodeHex(
        '43 50 55 54 00 00 00 00 00 01 00 00 01 00 00 00 00 00 10 00 00 00',
      ).bulkReference.equals(new TalkBulkReference(1, 1048576)),
    ).toBe(true);
    const unknown = decodeHex('44 01 00 00 01 00 00 00 FF FF FF FF FF FF').bulkReference;
    expect(unknown.number).toBe(1);
    expect(unknown.length).toBeUndefined();
    const empty = decodeHex('54 02 00 00 02 00 00 00 00 00 00 00 00 00').bulkReference;
    expect(empty.number).toBe(2);
    expect(empty.length).toBe(0);
    // The largest values survive the 48-bit arithmetic.
    const largest = new TalkBulkReference(TalkBulkReference.maxNumber, TalkBulkReference.maxLength);
    expect(hexString(largest.encode())).toBe('FF FF FF FF FE FF FF FF FF FF');
    expect(TalkBulkReference.decode(largest.encode()).equals(largest)).toBe(true);
    expect(() => decodeHex('04 01 00 00').bulkReference).toThrow(Error);
  });

  test('abort statuses decode', () => {
    expect(
      decodeHex('24 01 00 00 05 00 6E 6F 70 65').status.equals(
        Status.of(StatusCode.notFound, 'nope'),
      ),
    ).toBe(true);
    expect(decodeHex('22 02 00 00 01 00').status.known).toBe(StatusCode.cancelled);
    expect(decodeHex('20 10 00').status.known).toBe(StatusCode.unauthenticated);
    // An empty abort payload means UNKNOWN (talk.md, ABORT), not OK.
    expect(decodeHex('24 01 00 00').status.equals(Status.of(StatusCode.unknown))).toBe(true);
    expect(decodeHex('20').status.known).toBe(StatusCode.unknown);
  });
});

describe('Talk vectors, negative', () => {
  test.each(negative)('%s', (_name, hex) => {
    expectProtocolError(() => decodeHex(hex));
  });
});

describe('Talk decoding rules beyond the wiki vectors', () => {
  test.each(negativeExtra)('%s', (_name, hex) => {
    expectProtocolError(() => decodeHex(hex));
  });

  test('EXTEND values decode', () => {
    expect(decodeHex('34 01 00 00 A0 BB 0D 00 E0 93 04 00').extension).toEqual({
      deadline: 15 * minute,
      renew: 5 * minute,
    });
    expect(decodeHex('34 01 00 00 00 00 00 00 E0 93 04 00').extension).toEqual({
      deadline: undefined,
      renew: 5 * minute,
    });
    // An empty payload and two zero fields both declare nothing; the
    // payload tells a restart of the default timeout from "unchanged".
    expect(decodeHex('34 01 00 00').extension).toEqual({ deadline: undefined, renew: undefined });
    const zeros = decodeHex('34 01 00 00 00 00 00 00 00 00 00 00');
    expect(zeros.extension).toEqual({ deadline: undefined, renew: undefined });
    expect(zeros.payload.length).toBe(8);
    expect(decodeHex('34 01 00 00 FF FF FF FF 01 00 00 00').extension).toEqual({
      deadline: 0xffffffff,
      renew: 1,
    });
    expect(() => new TalkFrame({ kind: TalkKind.message, responseId: 1 }).extension).toThrow(Error);
  });

  test('EXTEND values encode in whole milliseconds, rounded up', () => {
    const payload = (values?: { deadline?: number; renew?: number }): string =>
      hexString(TalkFrame.extendPayload(values));
    expect(payload()).toBe('');
    expect(payload({ deadline: 1.5 })).toBe('02 00 00 00 00 00 00 00');
    // Under 1 ms is 1 ms: 0 would mean "unchanged".
    expect(payload({ renew: 0 })).toBe('00 00 00 00 01 00 00 00');
    expect(payload({ deadline: 60 * 24 * 60 * minute })).toBe('FF FF FF FF 00 00 00 00');
    expect(() => payload({ deadline: -1000 })).toThrow(RangeError);
    expect(() => payload({ renew: Number.NaN })).toThrow(RangeError);
  });

  test('a missing procedure reads as absent', () => {
    expect(decodeHex('04 01 00 00').procedure).toBeUndefined();
  });

  test('chained stream request and item request decode', () => {
    // Final response to 3 that is stream request 4, unlimited window.
    const chained = decodeHex('0E 04 00 00 03 00 00 FF FF FF FF');
    expect(chained.stream).toBe(true);
    expect(chained.requestId).toBe(4);
    expect(chained.responseId).toBe(3);
    expect(chained.window).toBe(TalkFrame.unlimitedWindow);
    // Stream item to 3 that is request 5.
    const item = decodeHex('16 05 00 00 03 00 00 AA');
    expect(item.kind).toBe(TalkKind.streamItem);
    expect(item.requestId).toBe(5);
    expect(item.responseId).toBe(3);
    expect(hexString(item.payload)).toBe('AA');
  });

  test('payload is a view, not a copy', () => {
    const bytes = hexBytes('04 01 00 00 01 02 03');
    const frame = TalkFrame.decode(bytes);
    expect(hexString(frame.payload)).toBe('01 02 03');
    bytes[4] = 0x7f;
    expect(frame.payload[0]).toBe(0x7f);
  });
});

describe('Item windows on the wire', () => {
  test('a stream request carries its window after its ids', () => {
    // Window 0: nothing until a grant.
    const zero = roundTrip('0B 4C 49 53 54 00 00 00 00 02 00 00 00 00 00 00');
    expect(zero.window).toBe(0);
    expect(zero.payload.length).toBe(0);
    // The largest finite window, with a payload after it.
    const largest = roundTrip('0B 4C 49 53 54 00 00 00 00 02 00 00 FE FF FF FF AA BB');
    expect(largest.window).toBe(TalkFrame.unlimitedWindow - 1);
    expect(hexString(largest.payload)).toBe('AA BB');
    expect(largest.toString()).toContain('window=4294967294');
    const unlimited = roundTrip('0B 4C 49 53 54 00 00 00 00 02 00 00 FF FF FF FF');
    expect(unlimited.window).toBe(TalkFrame.unlimitedWindow);
    // The window is part of the frame.
    expect(zero.equals(unlimited)).toBe(false);
    // Built rather than decoded.
    expect(
      hexString(
        new TalkFrame({
          kind: TalkKind.message,
          procedure: Name.of('LIST'),
          requestId: 2,
          stream: true,
          window: 3,
          payload: hexBytes('01'),
        }).encode(),
      ),
    ).toBe('0B 4C 49 53 54 00 00 00 00 02 00 00 03 00 00 00 01');
  });

  test('a chained response that is a stream request carries its window after both ids', () => {
    // Final response to 3 that is stream request 4, window 2.
    const chained = roundTrip('0E 04 00 00 03 00 00 02 00 00 00 CC');
    expect(chained.kind).toBe(TalkKind.message);
    expect(chained.requestId).toBe(4);
    expect(chained.responseId).toBe(3);
    expect(chained.stream).toBe(true);
    expect(chained.window).toBe(2);
    expect(hexString(chained.payload)).toBe('CC');
  });

  test('an item that is a stream request carries its window after both ids', () => {
    // Stream item to 3 that is stream request 5 "SUB", window 4.
    const item = roundTrip('1F 53 55 42 00 00 00 00 00 05 00 00 03 00 00 04 00 00 00 AA');
    expect(item.kind).toBe(TalkKind.streamItem);
    expect(item.procedure?.equals(Name.of('SUB'))).toBe(true);
    expect(item.requestId).toBe(5);
    expect(item.responseId).toBe(3);
    expect(item.window).toBe(4);
    expect(hexString(item.payload)).toBe('AA');
    // The same, with a bulk payload: the reference follows the window.
    const bulk = roundTrip('5E 05 00 00 03 00 00 00 00 00 00 02 00 00 00 00 00 00 00 00 00');
    expect(bulk.bulk).toBe(true);
    expect(bulk.window).toBe(0);
    expect(bulk.bulkReference.equals(new TalkBulkReference(2, 0))).toBe(true);
    // withBulk keeps the window.
    const windowed = new TalkFrame({
      kind: TalkKind.message,
      procedure: Name.of('PUT'),
      requestId: 1,
      stream: true,
      window: 7,
    }).withBulk(new TalkBulkReference(1));
    expect(windowed.window).toBe(7);
    expect(TalkFrame.decode(windowed.encode()).equals(windowed)).toBe(true);
    expect(hexString(windowed.encode())).toBe(
      '4B 50 55 54 00 00 00 00 00 01 00 00 07 00 00 00 01 00 00 00 FF FF FF FF FF FF',
    );
  });

  test('a credit names a request of its sender and counts items', () => {
    const credit = roundTrip('32 02 00 00 08 00 00 00');
    expect(credit.kind).toBe(TalkKind.extend);
    expect(credit.isCredit).toBe(true);
    expect(credit.requestId).toBe(2);
    expect(credit.hasResponse).toBe(false);
    expect(credit.creditItems).toBe(8);
    // A credit is not an extension, and an extension not a credit.
    expect(() => credit.extension).toThrow(Error);
    const extend = decodeHex('34 01 00 00');
    expect(extend.isCredit).toBe(false);
    expect(() => extend.creditItems).toThrow(Error);
    // A procedure is optional and ignored.
    const named = roundTrip('33 41 00 00 00 00 00 00 00 02 00 00 01 00 00 00');
    expect(named.creditItems).toBe(1);
    // The largest count decodes; that it overflows any window is the
    // channel's to say.
    expect(roundTrip('32 FF FF FF FF FF FF FF').creditItems).toBe(TalkFrame.unlimitedWindow);
    expect(hexString(TalkFrame.creditPayload(1))).toBe('01 00 00 00');
    expect(hexString(TalkFrame.creditPayload(0xffffffff))).toBe('FF FF FF FF');
    expect(() => TalkFrame.creditPayload(0)).toThrow(RangeError);
    expect(() => TalkFrame.creditPayload(0x100000000)).toThrow(RangeError);
  });

  test('frames that are not stream requests carry no window', () => {
    for (const hex of [
      '03 47 45 54 5F 50 52 4F 46 01 00 00',
      '15 49 54 45 4D 00 00 00 00 02 00 00 07',
      '07 4D 4F 52 45 00 00 00 00 07 00 00 01 00 00',
      '16 05 00 00 03 00 00 AA',
    ]) {
      const frame = roundTrip(hex);
      expect(frame.stream).toBe(false);
      expect(frame.window).toBe(TalkFrame.unlimitedWindow);
    }
  });
});

describe('TalkFrame.encode refuses invalid frames', () => {
  const a = Name.of('A');
  const cases: [string, () => TalkFrame][] = [
    ['plain message without procedure', () => new TalkFrame({ kind: TalkKind.message })],
    ['request without procedure', () => new TalkFrame({ kind: TalkKind.message, requestId: 1 })],
    [
      'stream without request',
      () => new TalkFrame({ kind: TalkKind.message, procedure: a, stream: true }),
    ],
    ['item without response', () => new TalkFrame({ kind: TalkKind.streamItem })],
    [
      'abort with both ids',
      () => new TalkFrame({ kind: TalkKind.abort, requestId: 1, responseId: 1 }),
    ],
    ['extend without an id', () => new TalkFrame({ kind: TalkKind.extend })],
    [
      'extend with both ids',
      () => new TalkFrame({ kind: TalkKind.extend, requestId: 1, responseId: 1 }),
    ],
    ['credit without a payload', () => new TalkFrame({ kind: TalkKind.extend, requestId: 1 })],
    [
      'credit with an extend payload',
      () =>
        new TalkFrame({
          kind: TalkKind.extend,
          requestId: 1,
          payload: TalkFrame.extendPayload({ deadline: 60 * minute }),
        }),
    ],
    [
      'credit of 0 items',
      () => new TalkFrame({ kind: TalkKind.extend, requestId: 1, payload: new Uint8Array(4) }),
    ],
    [
      'credit with STREAM',
      () =>
        new TalkFrame({
          kind: TalkKind.extend,
          requestId: 1,
          stream: true,
          payload: TalkFrame.creditPayload(1),
        }),
    ],
    [
      'cancel with STREAM',
      () => new TalkFrame({ kind: TalkKind.abort, requestId: 1, stream: true }),
    ],
    [
      'window without STREAM',
      () => new TalkFrame({ kind: TalkKind.message, procedure: a, requestId: 1, window: 4 }),
    ],
    [
      'negative window',
      () =>
        new TalkFrame({
          kind: TalkKind.message,
          procedure: a,
          requestId: 1,
          stream: true,
          window: -1,
        }),
    ],
    [
      'window over u32',
      () =>
        new TalkFrame({
          kind: TalkKind.message,
          procedure: a,
          requestId: 1,
          stream: true,
          window: 0x100000000,
        }),
    ],
    [
      'extend with a 1-byte payload',
      () => new TalkFrame({ kind: TalkKind.extend, responseId: 1, payload: new Uint8Array(1) }),
    ],
    [
      'request id over u24',
      () => new TalkFrame({ kind: TalkKind.message, procedure: a, requestId: 0x1000000 }),
    ],
    ['negative response id', () => new TalkFrame({ kind: TalkKind.message, responseId: -1 })],
    [
      'a bulk reference that is not one',
      () =>
        new TalkFrame({
          kind: TalkKind.message,
          procedure: a,
          bulk: true,
          payload: new Uint8Array(10),
        }),
    ],
  ];
  test.each(cases)('%s', (_name, build) => {
    expect(() => build().encode()).toThrow(RangeError);
  });
});

describe('_bulk open payloads', () => {
  test('_bulk, parent channel 2, bulk number 1', () => {
    const bytes = hexBytes('01 5F 62 75 6C 6B 00 00 00 02 00 00 00 00 00 01 00 00 00');
    const address = ChannelAddress.decode(bytes);
    expect(address.type?.equals(TalkBulkOpen.type)).toBe(true);
    expect(address.type?.equals(Name.of('_bulk'))).toBe(true);
    expect(address.instance).toBe(0);
    expect(address.shard).toBeUndefined();
    expect(address.host).toBeUndefined();
    expect(TalkBulkOpen.isBulk(address)).toBe(true);
    const open = TalkBulkOpen.decode(address);
    expect(open.parentId).toBe(2);
    expect(open.number).toBe(1);
    expect(hexString(new TalkBulkOpen(2, 1).encode())).toBe(hexString(bytes));
  });

  test('a long parent id and the largest number', () => {
    const long = new TalkBulkOpen(0xfffffffffffe, 0xffffffff);
    const decoded = TalkBulkOpen.decode(ChannelAddress.decode(long.encode()));
    expect(decoded.parentId).toBe(0xfffffffffffe);
    expect(decoded.number).toBe(0xffffffff);
  });

  test('bytes after the two fields are ignored', () => {
    const open = TalkBulkOpen.decode(
      ChannelAddress.decode(
        hexBytes('01 5F 62 75 6C 6B 00 00 00 02 00 00 00 00 00 01 00 00 00 EE'),
      ),
    );
    expect(open.parentId).toBe(2);
    expect(open.number).toBe(1);
  });

  test.each([
    ['truncated', '01 5F 62 75 6C 6B 00 00 00 02 00 00 00 00 00 01 00 00'],
    ['bulk number 0', '01 5F 62 75 6C 6B 00 00 00 02 00 00 00 00 00 00 00 00 00'],
  ])('%s: a protocol error', (_name, hex) => {
    expectProtocolError(() => TalkBulkOpen.decode(ChannelAddress.decode(hexBytes(hex))));
  });

  test('other types are not bulk channels', () => {
    expect(TalkBulkOpen.isBulk(new ChannelAddress({ type: 'bulk' }))).toBe(false);
    expect(TalkBulkOpen.isBulk(new ChannelAddress())).toBe(false);
  });

  test('constructors check their ranges', () => {
    expect(() => new TalkBulkOpen(-1, 1)).toThrow(RangeError);
    expect(() => new TalkBulkOpen(2 ** 48, 1)).toThrow(RangeError);
    expect(() => new TalkBulkOpen(0, 0)).toThrow(RangeError);
    expect(() => new TalkBulkReference(0)).toThrow(RangeError);
    expect(() => new TalkBulkReference(1, TalkBulkReference.unknownLength)).toThrow(RangeError);
    expect(() => new TalkBulkReference(2 ** 32)).toThrow(RangeError);
  });
});
