// What every binding shares: the public status helpers. Ports the codec
// part of `test/vectors/binding_protocol_test.dart`; its IDENT gating
// tests need the frame mux and are ported with it.
import { expect, test } from 'vitest';
import {
  maxControlPayloadLength,
  statusForWire,
  statusFromWire,
} from '../../src/core/mux/mux-protocol.ts';
import { MovedStatus } from '../../src/core/naming/naming-protocol.ts';
import { Status, StatusCode } from '../../src/core/status.ts';

test('public binding status helpers preserve payloads and map local codes', () => {
  const moved = new MovedStatus({
    owner: 0x123456789abc,
    epoch: 0xfedcba98,
    reason: 'é'.repeat(900),
  });
  const wire = statusForWire(moved.toStatus());
  expect(wire.encode().length).toBeLessThanOrEqual(maxControlPayloadLength);
  const decoded = MovedStatus.fromStatus(wire);
  expect(decoded.owner).toBe(moved.owner);
  expect(decoded.epoch).toBe(moved.epoch);
  expect(decoded.reason).not.toContain('�');
  expect(statusForWire(Status.of(StatusCode.connectionLost)).known).toBe(StatusCode.unavailable);
  expect(() => statusForWire(new Status(256))).toThrow(RangeError);
  expect(statusFromWire(new Status(256)).known).toBe(StatusCode.unknown);
});
