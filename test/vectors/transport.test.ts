// The stream binding: the wiki page "Polyverse Switchboard Test Vectors",
// section "Transport, stream binding", plus the rules of the "Switchboard
// Transport" page. Ports `test/vectors/transport_vectors_test.dart`.
import { describe, expect, test } from 'vitest';
import { hexBytes, hexString } from '../../src/core/bytes.ts';
import { MuxCommand, MuxFrame } from '../../src/core/mux/mux-frame.ts';
import { ProtocolError, StatusCode, SwitchboardError } from '../../src/core/status.ts';
import { StreamFrameDecoder, StreamFraming } from '../../src/core/transport/stream-framing.ts';

const preamble = '53 57 42 44 01 00 00 00';

/** Decodes [hex] (preamble included) and returns the frames as hex. */
function decodeStream(hex: string, maxFrameSize?: number): string[] {
  const decoder = new StreamFrameDecoder(maxFrameSize === undefined ? {} : { maxFrameSize });
  const frames: string[] = [];
  decoder.add(hexBytes(hex), (frame) => frames.push(hexString(frame)));
  return frames;
}

/** The error [run] throws. */
function thrown(run: () => unknown): unknown {
  try {
    run();
  } catch (error) {
    return error;
  }
  throw new Error('expected a throw');
}

function expectCode(error: unknown, code: StatusCode): void {
  expect(error).toBeInstanceOf(SwitchboardError);
  expect((error as SwitchboardError).code).toBe(code);
}

describe('transport, stream binding', () => {
  test('preamble, version 1', () => {
    expect(hexString(StreamFraming.encodePreamble())).toBe(preamble);
    const decoder = new StreamFrameDecoder();
    decoder.add(hexBytes(preamble), () => {
      throw new Error('no frame expected');
    });
    expect(decoder.preambleReceived).toBe(true);
    expect(decoder.bufferedBytes).toBe(0);
  });

  test('frame carrying body 02 02 00 AA', () => {
    expect(hexString(StreamFraming.encodeFrame(hexBytes('02 02 00 AA')))).toBe(
      '04 00 00 00 02 02 00 AA',
    );
    expect(decodeStream(`${preamble} 04 00 00 00 02 02 00 AA`)).toEqual(['02 02 00 AA']);
  });

  test('embedded profile round trip framing', () => {
    // From the embedded profile page: transport length 12, mux OPEN ch 2.
    const open = hexBytes('12 02 00 01 64 65 76 00 00 00 00 00');
    expect(hexString(StreamFraming.encodeFrame(open))).toBe(
      '0C 00 00 00 12 02 00 01 64 65 76 00 00 00 00 00',
    );
    expect(decodeStream(`${preamble} 0C 00 00 00 12 02 00 01 64 65 76 00 00 00 00 00`)).toEqual([
      '12 02 00 01 64 65 76 00 00 00 00 00',
    ]);
    expect(MuxFrame.decode(open).command).toBe(MuxCommand.open);
  });

  test('preamble flags are ignored', () => {
    expect(decodeStream('53 57 42 44 01 00 FF FF 01 00 00 00 02')).toEqual(['02']);
  });

  test('wrong magic is a protocol error', () => {
    expect(thrown(() => decodeStream('53 57 42 45 01 00 00 00'))).toBeInstanceOf(ProtocolError);
  });

  test('unsupported version is UNSUPPORTED, not a protocol error', () => {
    const error = thrown(() => decodeStream('53 57 42 44 02 00 00 00'));
    expectCode(error, StatusCode.unsupported);
    expect(error).not.toBeInstanceOf(ProtocolError);
  });

  test('length 0 is a protocol error', () => {
    expect(thrown(() => decodeStream(`${preamble} 00 00 00 00`))).toBeInstanceOf(ProtocolError);
    expect(() => StreamFraming.encodeFrame(new Uint8Array(0))).toThrow(RangeError);
  });

  test('length over the limit is FRAME_TOO_LARGE without the body', () => {
    // Only the length is present; the body is never needed.
    expectCode(
      thrown(() => decodeStream(`${preamble} 05 00 00 00`, 4)),
      StatusCode.frameTooLarge,
    );
    expect(decodeStream(`${preamble} 04 00 00 00 02 02 00 AA`, 4)).toHaveLength(1);
  });

  test('maxFrameSize zero means no limit', () => {
    expect(decodeStream(`${preamble} 04 00 00 00 02 02 00 AA`, 0)).toEqual(['02 02 00 AA']);
    // Unlimited frame size does not permit an empty transport frame.
    expect(thrown(() => decodeStream(`${preamble} 00 00 00 00`, 0))).toBeInstanceOf(ProtocolError);
  });

  test('lengths with the top bit set are not negative', () => {
    // 0x80000000: bitwise operations are 32-bit signed in JavaScript.
    expectCode(
      thrown(() => decodeStream(`${preamble} 00 00 00 80`)),
      StatusCode.frameTooLarge,
    );
    expectCode(
      thrown(() => decodeStream(`${preamble} FF FF FF FF`)),
      StatusCode.frameTooLarge,
    );
  });

  test('frames before an error are delivered first', () => {
    const decoder = new StreamFrameDecoder();
    const frames: string[] = [];
    const error = thrown(() =>
      decoder.add(hexBytes(`${preamble} 01 00 00 00 02 00 00 00 00`), (frame) =>
        frames.push(hexString(frame)),
      ),
    );
    expect(error).toBeInstanceOf(ProtocolError);
    expect(frames).toEqual(['02']);
  });
});
