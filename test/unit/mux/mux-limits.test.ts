// Status reasons on the wire: the codec half of the "reasons" and
// "CONNECTION_LOST stays local" groups of `test/mux_limits_test.dart`
// (core notes, appendix A.12). The connection groups of that file need the
// frame mux and are ported with it.
import { describe, expect, test } from 'vitest';
import { hexBytes, hexString } from '../../../src/core/bytes.ts';
import { MuxControlMessage } from '../../../src/core/mux/mux-frame.ts';
import {
  checkMuxStatus,
  decodeStatusPayload,
  fitStatus,
  statusForWire,
  statusFromWire,
  truncateStatus,
} from '../../../src/core/mux/mux-protocol.ts';
import { MovedStatus } from '../../../src/core/naming/naming-protocol.ts';
import { ProtocolError, Status, StatusCode } from '../../../src/core/status.ts';

const strictUtf8 = new TextDecoder('utf-8', { fatal: true });

describe('reasons', () => {
  test('truncation is UTF-8 safe', () => {
    for (const [char, bytes, length] of [
      ['x', 1, 1024],
      ['é', 2, 1024],
      ['€', 3, 1022],
      ['😀', 4, 1022],
    ] as const) {
      const status = Status.of(StatusCode.unavailable, char.repeat(2000));
      const cut = truncateStatus(status, 1024);
      const encoded = cut.encode();
      expect(encoded.length, char).toBeLessThanOrEqual(1024);
      expect(encoded.length, char).toBeGreaterThan(1024 - bytes);
      expect(encoded.length, char).toBe(length);
      expect(strictUtf8.decode(encoded.subarray(2))).toBe(cut.reason);
      expect(cut.reason.replaceAll(char, ''), char).toBe('');
    }
    const short = Status.of(StatusCode.unavailable, 'ok');
    expect(truncateStatus(short, 1024)).toBe(short);
    expect(truncateStatus(short, 2).equals(new Status(14))).toBe(true);
    expect(
      MuxControlMessage.goAway(Status.of(StatusCode.unavailable, 'x'.repeat(2000))).payload,
    ).toHaveLength(MuxControlMessage.maxControlPayload);
  });

  test('truncation keeps fixed fields before the reason', () => {
    const moved = new MovedStatus({
      owner: 0xfedcba987654,
      epoch: 7,
      reason: '€'.repeat(400),
    }).toStatus();
    const cut = truncateStatus(moved, 1024);
    const encoded = cut.encode();
    expect(encoded.length).toBeGreaterThanOrEqual(1022);
    expect(encoded.length).toBeLessThanOrEqual(1024);
    expect(encoded.length).toBe(1023);
    expect(hexString(encoded.subarray(0, 12))).toBe('25 00 54 76 98 BA DC FE 07 00 00 00');
    expect(hexString(encoded.subarray(0, 12))).toBe(hexString(moved.encode().subarray(0, 12)));
    const fields = MovedStatus.fromStatus(cut);
    expect(fields.owner).toBe(0xfedcba987654);
    expect(fields.epoch).toBe(7);
    expect(fields.reason.replaceAll('€', '')).toBe('');
    // No room for the reason: the fields alone; no room for the fields:
    // nothing, never part of them.
    expect(MovedStatus.fromStatus(truncateStatus(moved, 12)).owner).toBe(0xfedcba987654);
    expect(truncateStatus(moved, 11).equals(new Status(37))).toBe(true);
    expect(truncateStatus(moved, 2).equals(new Status(37))).toBe(true);
    // RELOCATED has the same fields.
    const relocated = Status.decode(Uint8Array.of(38, 0, ...moved.encode().subarray(2)));
    expect(MovedStatus.fromStatus(truncateStatus(relocated, 12)).owner).toBe(0xfedcba987654);
    expect(truncateStatus(relocated, 11).equals(new Status(38))).toBe(true);
  });

  test('a status fits the peer frame limit after the header', () => {
    // A CLOSE on a short id to a peer that takes frames of 64 bytes: 64,
    // less the 3 byte header, less the 2 byte code.
    const fitted = fitStatus(Status.of(StatusCode.cancelled, 'y'.repeat(100)), 3, 64);
    expect(fitted.equals(Status.of(StatusCode.cancelled, 'y'.repeat(59)))).toBe(true);
    // No stated limit: the 1024 byte bound alone.
    expect(
      fitStatus(Status.of(StatusCode.cancelled, 'y'.repeat(2000)), 3, 0).encode(),
    ).toHaveLength(1024);
    // A large limit leaves the 1024 byte bound.
    expect(
      fitStatus(Status.of(StatusCode.cancelled, 'y'.repeat(2000)), 3, 1 << 20).encode(),
    ).toHaveLength(1024);
  });
});

describe('CONNECTION_LOST stays local', () => {
  test('on the wire it is UNAVAILABLE, the reason kept', () => {
    const local = Status.of(StatusCode.connectionLost, 'upstream gone');
    const wire = statusForWire(local);
    expect(wire.equals(Status.of(StatusCode.unavailable, 'upstream gone'))).toBe(true);
    expect(local.known).toBe(StatusCode.connectionLost);
  });

  test('only the code changes: the bytes after it are relayed', () => {
    const relayed = Status.decode(hexBytes('22 00 FF 41'));
    expect(hexString(statusForWire(relayed).encode())).toBe('0E 00 FF 41');
  });
});

describe('application codes', () => {
  test('are refused in a CLOSE or GOAWAY of this side', () => {
    expect(() => checkMuxStatus(new Status(256))).toThrow(RangeError);
    expect(() => checkMuxStatus(new Status(0xffff, 'app'))).toThrow(RangeError);
    expect(() => checkMuxStatus(new Status(255))).not.toThrow();
  });

  test('from the peer are reported as UNKNOWN and relayable', () => {
    const received = statusFromWire(Status.decode(hexBytes('2C 01 61')));
    expect(received.equals(Status.of(StatusCode.unknown, 'code 300: a'))).toBe(true);
    expect(() => statusForWire(received)).not.toThrow();
    const known = Status.of(StatusCode.notFound, 'x');
    expect(statusFromWire(known)).toBe(known);
  });
});

test('a one byte status payload is a protocol error', () => {
  expect(() => decodeStatusPayload(hexBytes('05'), 'CLOSE')).toThrow(ProtocolError);
  expect(() => decodeStatusPayload(hexBytes('05'), 'CLOSE')).toThrow('malformed CLOSE status');
  expect(decodeStatusPayload(new Uint8Array(0), 'CLOSE').isOk).toBe(true);
});
