// Status payloads: the wiki pages "Polyverse Switchboard Status Codes" and
// "Test Vectors" (the CLOSE payloads of the mux vectors).
import { describe, expect, test } from 'vitest';
import { hexBytes, hexString } from '../../src/core/bytes.ts';
import {
  isStatus,
  ProtocolError,
  Status,
  StatusCode,
  StatusFormatError,
  SwitchboardError,
  statusCodeName,
} from '../../src/core/status.ts';

describe('status codes', () => {
  test('codes 0 to 16 are the gRPC codes, 32 upward are Switchboard specific', () => {
    expect(StatusCode.ok).toBe(0);
    expect(StatusCode.unauthenticated).toBe(16);
    expect(StatusCode.protocolError).toBe(32);
    expect(StatusCode.goingAway).toBe(33);
    expect(StatusCode.connectionLost).toBe(34);
    expect(StatusCode.frameTooLarge).toBe(35);
    expect(StatusCode.unsupported).toBe(36);
    expect(StatusCode.moved).toBe(37);
    expect(StatusCode.relocated).toBe(38);
    expect(statusCodeName(5)).toBe('notFound');
    expect(statusCodeName(17)).toBeUndefined();
    expect(statusCodeName(300)).toBeUndefined();
  });
});

describe('status payloads', () => {
  test('OK, no reason', () => {
    expect(hexString(Status.ok.encode())).toBe('00 00');
    expect(Status.decode(hexBytes('00 00')).equals(Status.ok)).toBe(true);
  });

  test('an empty payload means OK', () => {
    const s = Status.decode(new Uint8Array(0));
    expect(s.isOk).toBe(true);
    expect(s.code).toBe(0);
  });

  test('NOT_FOUND, reason "x"', () => {
    const s = Status.of(StatusCode.notFound, 'x');
    expect(hexString(s.encode())).toBe('05 00 78');
    const d = Status.decode(hexBytes('05 00 78'));
    expect(d.equals(s)).toBe(true);
    expect(d.name).toBe('notFound');
    expect(d.toString()).toBe('notFound: x');
  });

  test('GOING_AWAY, no reason', () => {
    expect(hexString(Status.of(StatusCode.goingAway).encode())).toBe('21 00');
    expect(Status.decode(hexBytes('21 00')).is(StatusCode.goingAway)).toBe(true);
  });

  test('a one-byte payload cannot be a status', () => {
    expect(() => Status.decode(hexBytes('05'))).toThrow(StatusFormatError);
  });

  test('a decoded status re-encodes to its exact bytes, fixed fields included', () => {
    // MOVED with owner 0x1A2B, epoch 3 and no reason: fields are not interpreted here.
    const bytes = hexBytes('25 00 2B 1A 00 00 00 00 03 00 00 00');
    const s = Status.decode(bytes);
    expect(s.code).toBe(StatusCode.moved);
    expect(hexString(s.encode())).toBe(hexString(bytes));
    expect(s.toString()).toBe('moved: [2b 1a 00 00 00 00 03 00 00 00]');
  });

  test('application codes', () => {
    const s = new Status(300, 'app');
    expect(s.isApplicationCode).toBe(true);
    expect(s.known).toBeUndefined();
    expect(hexString(s.encode())).toBe('2C 01 61 70 70');
    expect(s.toString()).toBe('code 300: app');
  });

  test('codes outside u16 are refused', () => {
    expect(() => new Status(0x10000)).toThrow(RangeError);
    expect(() => new Status(-1)).toThrow(RangeError);
  });

  test('UTF-8 reasons survive a round trip', () => {
    const s = Status.of(StatusCode.internal, 'héllo');
    expect(Status.decode(s.encode()).reason).toBe('héllo');
  });
});

describe('errors', () => {
  test('SwitchboardError carries its status', () => {
    const e = SwitchboardError.of(StatusCode.unavailable, 'later');
    expect(e.code).toBe(14);
    expect(e.statusName).toBe('unavailable');
    expect(e.message).toBe('unavailable: later');
    expect(e.is(StatusCode.unavailable)).toBe(true);
    expect(isStatus(e, StatusCode.unavailable)).toBe(true);
    expect(isStatus(e, StatusCode.notFound)).toBe(false);
    expect(isStatus(new Error('x'), StatusCode.unavailable)).toBe(false);
  });

  test('ProtocolError is PROTOCOL_ERROR', () => {
    const e = new ProtocolError('bad frame');
    expect(e).toBeInstanceOf(SwitchboardError);
    expect(e.status.code).toBe(StatusCode.protocolError);
    expect(e.status.reason).toBe('bad frame');
    expect(e.name).toBe('ProtocolError');
  });
});
