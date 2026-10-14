// Mux frames and control messages: the wiki page "Polyverse Switchboard
// Test Vectors", section "Mux frames", plus the decoding rules of the
// "Switchboard Mux" page. Ports the codec groups of
// `test/vectors/mux_vectors_test.dart`; its "connection" group needs the
// frame mux and is ported with it.
import { describe, expect, test } from 'vitest';
import { hexBytes, hexString } from '../../src/core/bytes.ts';
import {
  MuxCommand,
  MuxControlMessage,
  MuxControlType,
  MuxCredit,
  MuxFrame,
  MuxLimits,
} from '../../src/core/mux/mux-frame.ts';
import { ProtocolError, Status, StatusCode } from '../../src/core/status.ts';

const empty = new Uint8Array(0);

describe('mux frames', () => {
  function vector(name: string, frame: MuxFrame, hex: string): void {
    test(name, () => {
      expect(hexString(frame.encode())).toBe(hex);
      expect(frame.encodedLength).toBe(hexBytes(hex).length);
      const decoded = MuxFrame.decode(hexBytes(hex));
      expect(decoded.command).toBe(frame.command);
      expect(decoded.channelId).toBe(frame.channelId);
      expect(hexString(decoded.payload)).toBe(hexString(frame.payload));
    });
  }

  vector(
    'DATA, short id 2, payload 01 02 03',
    MuxFrame.data(2, hexBytes('01 02 03')),
    '02 02 00 01 02 03',
  );
  vector('DATA, short id 2, empty payload', MuxFrame.data(2, empty), '02 02 00');
  vector(
    'DATA, long id 0x010000, empty payload',
    MuxFrame.data(0x010000, empty),
    '00 00 00 01 00 00 00',
  );
  vector(
    'DATA, long id 0x1234567890AB, payload FF',
    MuxFrame.data(0x1234567890ab, hexBytes('FF')),
    '00 AB 90 78 56 34 12 FF',
  );
  vector('OPEN, short id 3, payload AA', MuxFrame.open(3, hexBytes('AA')), '12 03 00 AA');
  vector('OPEN, short id 2, empty payload', MuxFrame.open(2, empty), '12 02 00');
  vector('CLOSE, short id 2, status OK, no reason', MuxFrame.close(2, Status.ok), '22 02 00 00 00');
  vector('CLOSE, short id 2, no payload (means OK)', MuxFrame.close(2), '22 02 00');
  vector(
    'CLOSE, short id 2, NOT_FOUND, reason "x"',
    MuxFrame.close(2, Status.of(StatusCode.notFound, 'x')),
    '22 02 00 05 00 78',
  );
  vector(
    'Control PING, payload DE AD',
    MuxControlMessage.ping(hexBytes('DE AD')).toFrame(),
    '02 00 00 01 DE AD',
  );
  vector(
    'Control PONG, payload DE AD',
    MuxControlMessage.pong(hexBytes('DE AD')).toFrame(),
    '02 00 00 02 DE AD',
  );
  vector(
    'Control GOAWAY, GOING_AWAY, no reason',
    MuxControlMessage.goAway(Status.of(StatusCode.goingAway)).toFrame(),
    '02 00 00 03 21 00',
  );
  vector(
    'Control LIMITS, 1024 bytes, 4 channels, 64 KiB window',
    MuxControlMessage.limits(
      new MuxLimits({ maxFrameSize: 1024, maxChannels: 4, initialWindow: 65536 }),
    ).toFrame(),
    '02 00 00 04 00 04 00 00 04 00 00 00 00 00 01 00',
  );
  vector(
    'Control CREDIT, channel 2, 32768 bytes',
    MuxControlMessage.credit(new MuxCredit(2, 32768)).toFrame(),
    '02 00 00 07 02 00 00 00 00 00 00 80 00 00',
  );

  test('CLOSE payloads decode to their status', () => {
    const statusOf = (hex: string): Status => Status.decode(MuxFrame.decode(hexBytes(hex)).payload);
    expect(statusOf('22 02 00 00 00').equals(Status.ok)).toBe(true);
    expect(statusOf('22 02 00').equals(Status.ok)).toBe(true);
    expect(statusOf('22 02 00 05 00 78').equals(Status.of(StatusCode.notFound, 'x'))).toBe(true);
  });

  test('control messages decode', () => {
    const control = (hex: string): MuxControlMessage => {
      const frame = MuxFrame.decode(hexBytes(hex));
      expect(frame.channelId).toBe(0);
      expect(frame.command).toBe(MuxCommand.data);
      return MuxControlMessage.decode(frame.payload);
    };

    const ping = control('02 00 00 01 DE AD');
    expect(ping.knownType).toBe(MuxControlType.ping);
    expect(hexString(ping.payload)).toBe('DE AD');
    const pong = control('02 00 00 02 DE AD');
    expect(pong.knownType).toBe(MuxControlType.pong);
    expect(hexString(pong.payload)).toBe('DE AD');
    const goAway = control('02 00 00 03 21 00');
    expect(goAway.knownType).toBe(MuxControlType.goAway);
    expect(goAway.goAwayStatus.equals(Status.of(StatusCode.goingAway))).toBe(true);
    const limits = control('02 00 00 04 00 04 00 00 04 00 00 00 00 00 01 00');
    expect(limits.knownType).toBe(MuxControlType.limits);
    expect(
      MuxLimits.decode(limits.payload).equals(
        new MuxLimits({ maxFrameSize: 1024, maxChannels: 4, initialWindow: 65536 }),
      ),
    ).toBe(true);
    const credit = control('02 00 00 07 02 00 00 00 00 00 00 80 00 00');
    expect(credit.knownType).toBe(MuxControlType.credit);
    expect(MuxCredit.decode(credit.payload).equals(new MuxCredit(2, 32768))).toBe(true);
  });

  test('LIMITS ignores trailing fields', () => {
    expect(
      MuxLimits.decode(hexBytes('00 04 00 00 04 00 00 00 00 00 01 00 99 99')).equals(
        new MuxLimits({ maxFrameSize: 1024, maxChannels: 4, initialWindow: 65536 }),
      ),
    ).toBe(true);
  });

  test('LIMITS and CREDIT carry full u32 and u48 values', () => {
    // Bitwise operators are 32-bit in JavaScript.
    const limits = new MuxLimits({
      maxFrameSize: 0xffffffff,
      maxChannels: 0x80000000,
      initialWindow: 0xffffffff,
    });
    expect(hexString(limits.encode())).toBe('FF FF FF FF 00 00 00 80 FF FF FF FF');
    expect(MuxLimits.decode(limits.encode()).equals(limits)).toBe(true);
    const credit = new MuxCredit(0xfffffffffffe, 0xffffffff);
    expect(hexString(credit.encode())).toBe('FE FF FF FF FF FF FF FF FF FF');
    expect(MuxCredit.decode(credit.encode()).equals(credit)).toBe(true);
    const high = new MuxCredit(0x100000002, 0x80000000);
    expect(MuxCredit.decode(high.encode()).channelId).toBe(0x100000002);
    expect(MuxCredit.decode(high.encode()).bytes).toBe(0x80000000);
  });

  test('LIMITS and CREDIT refuse values that do not fit', () => {
    for (const limits of [
      new MuxLimits({ maxFrameSize: 0, maxChannels: 0, initialWindow: 0 }),
      new MuxLimits({ maxFrameSize: 0, maxChannels: 0, initialWindow: 0x100000000 }),
      new MuxLimits({ maxFrameSize: -1, maxChannels: 0 }),
      new MuxLimits({ maxFrameSize: 1.5, maxChannels: 0 }),
    ]) {
      expect(() => limits.encode(), `${limits}`).toThrow(RangeError);
    }
    for (const credit of [
      new MuxCredit(-1, 0),
      new MuxCredit(0x1000000000000, 0),
      new MuxCredit(2, -1),
      new MuxCredit(2, 0x100000000),
      new MuxCredit(2.5, 0),
    ]) {
      expect(() => credit.encode(), `${credit}`).toThrow(RangeError);
    }
    expect(MuxCredit.costOf(0)).toBe(16);
    expect(MuxCredit.costOf(100)).toBe(116);
  });

  test('a LIMITS without a window has the default one', () => {
    expect(new MuxLimits({ maxFrameSize: 0, maxChannels: 0 }).initialWindow).toBe(65536);
    expect(MuxLimits.defaultInitialWindow).toBe(65536);
  });

  test('PING and NONCE refuse payloads of the wrong size', () => {
    expect(MuxControlMessage.ping(new Uint8Array(125)).payload).toHaveLength(125);
    expect(() => MuxControlMessage.ping(new Uint8Array(126))).toThrow(RangeError);
    expect(() => MuxControlMessage.nonce(new Uint8Array(31))).toThrow(RangeError);
    expect(() => MuxControlMessage.nonce(new Uint8Array(33))).toThrow(RangeError);
    expect(MuxControlMessage.ping().payload).toHaveLength(0);
  });

  test('GOAWAY shortens its status to the control payload limit', () => {
    const message = MuxControlMessage.goAway(Status.of(StatusCode.unavailable, 'x'.repeat(2000)));
    expect(message.payload).toHaveLength(1024);
    expect(message.goAwayStatus.code).toBe(StatusCode.unavailable);
  });

  test('a malformed GOAWAY status is a protocol error', () => {
    expect(() => new MuxControlMessage(MuxControlType.goAway, hexBytes('21')).goAwayStatus).toThrow(
      ProtocolError,
    );
    expect(new MuxControlMessage(MuxControlType.goAway).goAwayStatus.isOk).toBe(true);
  });

  test('unknown control types decode', () => {
    const message = MuxControlMessage.decode(hexBytes('7F 01'));
    expect(message.type).toBe(0x7f);
    expect(message.knownType).toBeUndefined();
    expect(hexString(message.payload)).toBe('01');
    expect(`${message}`).toBe('MuxControlMessage(127, 01)');
  });

  test('short form whenever the id fits', () => {
    expect(hexString(MuxFrame.data(0xfffe, empty).encode())).toBe('02 FE FF');
    expect(hexString(MuxFrame.data(0x10000, empty).encode())).toBe('00 00 00 01 00 00 00');
    expect(MuxFrame.headerSizeFor(0xffff)).toBe(3);
    expect(MuxFrame.headerSizeFor(0x10000)).toBe(7);
    // A short id sent in the long form is the same id.
    const long = MuxFrame.decode(hexBytes('00 02 00 00 00 00 00 AA'));
    expect(long.channelId).toBe(2);
    expect(long.longForm).toBe(true);
    expect(MuxFrame.decode(hexBytes('02 02 00 AA')).longForm).toBe(false);
  });

  test('ids above 32 bits do not alias short ids', () => {
    // Bitwise operators are 32-bit in JavaScript; these ids must survive
    // the round trip.
    for (const [id, hex] of [
      [0x100000002, '00 02 00 00 00 01 00'],
      [0x1234567890ab, '00 AB 90 78 56 34 12'],
      [0xffff00000002, '00 02 00 00 00 FF FF'],
      [0xfffffffffffe, '00 FE FF FF FF FF FF'],
      [0x80000000, '00 00 00 00 80 00 00'],
    ] as const) {
      expect(hexString(MuxFrame.data(id, empty).encode())).toBe(hex);
      const decoded = MuxFrame.decode(hexBytes(hex));
      expect(decoded.channelId, hex).toBe(id);
      expect(decoded.longForm).toBe(true);
    }
    expect(MuxFrame.decode(hexBytes('00 02 00 00 00 01 00')).channelId).not.toBe(2);
  });

  test('ids that do not fit a u48 are refused by the encoder', () => {
    for (const id of [-1, 0x1000000000000, 2.5, Number.NaN]) {
      expect(() => MuxFrame.data(id, empty).encode(), `${id}`).toThrow(RangeError);
    }
  });

  test('compatible reserved bits are ignored', () => {
    const frame = MuxFrame.decode(hexBytes('42 02 00 01'));
    expect(frame.command).toBe(MuxCommand.data);
    expect(frame.channelId).toBe(2);
    expect(hexString(frame.payload)).toBe('01');
    expect(MuxFrame.decode(hexBytes('82 02 00')).channelId).toBe(2);
    expect(MuxFrame.decode(hexBytes('D2 03 00')).command).toBe(MuxCommand.open);
  });

  test('reserved ids', () => {
    for (const id of [1, 0xffff, 0xffff00000000, 0xffffffffffff]) {
      expect(MuxFrame.isReservedId(id), `${id}`).toBe(true);
    }
    for (const id of [0, 2, 0xfffe, 0x10000, 0xffff00000001, 0xfffffffffffe]) {
      expect(MuxFrame.isReservedId(id), `${id}`).toBe(false);
    }
  });

  test('frames print their command, id and payload', () => {
    expect(`${MuxFrame.open(3, hexBytes('AA'))}`).toBe('MuxFrame(open, 3, AA)');
    expect(`${MuxControlMessage.ping(hexBytes('DE AD'))}`).toBe('MuxControlMessage(ping, DE AD)');
    expect(`${new MuxCredit(2, 32768)}`).toBe('MuxCredit(2, 32768)');
  });
});

// Negative vectors: each must be a protocol error.
const negative: readonly (readonly [string, string])[] = [
  ['Reserved incompatible flag 0x01', '03 02 00'],
  ['Command 3', '32 02 00'],
  ['Truncated short header', '02 02'],
  ['Truncated long header', '00 01 02 03 04 05'],
  ['OPEN on channel 0', '12 00 00'],
  ['DATA on channel 1', '02 01 00'],
];

// Further decoding rules from the mux page.
const negativeRules: readonly (readonly [string, string])[] = [
  ['Reserved incompatible flag 0x04', '06 02 00'],
  ['Reserved incompatible flag 0x08', '0A 02 00'],
  ['CLOSE on channel 0', '22 00 00'],
  ['OPEN on channel 1', '12 01 00'],
  ['DATA on reserved id 0xFFFF', '02 FF FF'],
  ['DATA on reserved id 0xFFFF00000000', '00 00 00 00 00 FF FF'],
  ['DATA on reserved id 0xFFFFFFFFFFFF', '00 FF FF FF FF FF FF'],
  ['DATA with payload on reserved 0xFFFF00000000', '00 00 00 00 00 FF FF 01 DE AD'],
  ['Empty frame', ''],
];

describe('mux negative vectors, decoder', () => {
  for (const [name, hex] of [...negative, ...negativeRules]) {
    test(name, () => {
      expect(() => MuxFrame.decode(hexBytes(hex))).toThrow(ProtocolError);
    });
  }

  test('control payload over 1024 bytes', () => {
    const over = new Uint8Array(1 + 1025);
    over[0] = 0x7f;
    expect(() => MuxControlMessage.decode(over)).toThrow(ProtocolError);
    const limit = new Uint8Array(1 + 1024);
    limit[0] = 0x7f;
    expect(MuxControlMessage.decode(limit).payload).toHaveLength(1024);
  });

  test('control message without type', () => {
    expect(() => MuxControlMessage.decode(empty)).toThrow(ProtocolError);
  });

  test('truncated LIMITS', () => {
    expect(() => MuxLimits.decode(hexBytes('00 04 00 00 04 00 00'))).toThrow(ProtocolError);
  });

  // Control payloads that are protocol errors (after the type byte).
  for (const [name, hex, decode] of [
    ['LIMITS shorter than 12 bytes', '00 04 00 00 04 00 00 00', MuxLimits.decode],
    ['LIMITS with a zero window', '00 04 00 00 04 00 00 00 00 00 00 00', MuxLimits.decode],
    ['CREDIT that is not 10 bytes', '02 00 00 00 00 00 00 80', MuxCredit.decode],
    ['CREDIT of 11 bytes', '02 00 00 00 00 00 00 80 00 00 00', MuxCredit.decode],
  ] as const) {
    test(name, () => {
      expect(() => decode(hexBytes(hex))).toThrow(ProtocolError);
    });
  }
});
