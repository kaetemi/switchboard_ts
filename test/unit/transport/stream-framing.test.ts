// The stream binding codec as Web Streams transforms, and the incremental
// decoder across chunk boundaries. No Dart counterpart: the Dart codec is
// covered by `transport_vectors_test.dart` (test/vectors/transport.test.ts)
// and the transport tests.
import { describe, expect, test } from 'vitest';
import { hexBytes, hexString } from '../../../src/core/bytes.ts';
import { ProtocolError, StatusCode, SwitchboardError } from '../../../src/core/status.ts';
import {
  StreamFrameDecoder,
  StreamFrameDecoderStream,
  StreamFrameEncoderStream,
  StreamFraming,
} from '../../../src/core/transport/stream-framing.ts';
import { collect, rechunk, rejection } from './support.ts';

const preambleHex = '53 57 42 44 01 00 00 00';

function source(...chunks: string[]): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) {
        controller.enqueue(hexBytes(chunk));
      }
      controller.close();
    },
  });
}

describe('StreamFrameDecoder', () => {
  test('keeps a partial frame across chunks and copies frames out', () => {
    const decoder = new StreamFrameDecoder();
    const frames: Uint8Array[] = [];
    decoder.add(hexBytes('53 57 42'), (f) => frames.push(f));
    expect(decoder.preambleReceived).toBe(false);
    expect(decoder.bufferedBytes).toBe(3);
    decoder.add(hexBytes('44 01 00 00 00 03 00'), (f) => frames.push(f));
    expect(decoder.preambleReceived).toBe(true);
    expect(decoder.bufferedBytes).toBe(2);
    const chunk = hexBytes('00 00 AA BB CC 01 00 00 00 DD');
    decoder.add(chunk, (f) => frames.push(f));
    chunk.fill(0);
    expect(frames.map(hexString)).toEqual(['AA BB CC', 'DD']);
    expect(decoder.bufferedBytes).toBe(0);
  });

  test('a large frame split in many chunks', () => {
    const decoder = new StreamFrameDecoder();
    const body = new Uint8Array(100_000).map((_, i) => i & 0xff);
    const bytes = new Uint8Array([
      ...StreamFraming.encodePreamble(),
      ...StreamFraming.encodeFrame(body),
    ]);
    const frames: Uint8Array[] = [];
    for (let i = 0; i < bytes.length; i += 999) {
      decoder.add(bytes.subarray(i, i + 999), (f) => frames.push(f));
    }
    expect(frames).toHaveLength(1);
    expect(frames[0]).toEqual(body);
    expect(decoder.bufferedBytes).toBe(0);
  });

  test('the limit is checked before the body arrives', () => {
    const decoder = new StreamFrameDecoder({ maxFrameSize: 3 });
    decoder.add(hexBytes(`${preambleHex} 03 00 00 00 01 02 03`), () => {});
    let error: unknown;
    try {
      decoder.add(hexBytes('04 00 00 00'), () => {});
    } catch (e) {
      error = e;
    }
    expect(error).toBeInstanceOf(SwitchboardError);
    expect((error as SwitchboardError).code).toBe(StatusCode.frameTooLarge);
  });

  test('a negative limit is a RangeError', () => {
    expect(() => new StreamFrameDecoder({ maxFrameSize: -1 })).toThrow(RangeError);
  });
});

describe('StreamFrameEncoderStream', () => {
  test('writes the preamble, then one length-prefixed frame per chunk', async () => {
    const out = await collect(source('AA', 'BB CC').pipeThrough(new StreamFrameEncoderStream()));
    expect(out.map(hexString)).toEqual([preambleHex, '01 00 00 00 AA', '02 00 00 00 BB CC']);
  });

  test('an empty chunk errors the stream', async () => {
    const piped = source('AA', '').pipeThrough(new StreamFrameEncoderStream());
    expect(await rejection(collect(piped))).toBeInstanceOf(RangeError);
  });
});

describe('StreamFrameDecoderStream', () => {
  test('one chunk per frame, whatever the chunking', async () => {
    const bytes = `${preambleHex} 01 00 00 00 AA 02 00 00 00 BB CC`;
    const whole = await collect(source(bytes).pipeThrough(new StreamFrameDecoderStream()));
    expect(whole.map(hexString)).toEqual(['AA', 'BB CC']);
    const bytewise = await collect(
      source(bytes).pipeThrough(rechunk(1)).pipeThrough(new StreamFrameDecoderStream()),
    );
    expect(bytewise.map(hexString)).toEqual(['AA', 'BB CC']);
  });

  test('round trip through the encoder', async () => {
    const frames = ['01', '02 03', 'FF FF FF FF'];
    const out = await collect(
      source(...frames)
        .pipeThrough(new StreamFrameEncoderStream())
        .pipeThrough(new StreamFrameDecoderStream()),
    );
    expect(out.map(hexString)).toEqual(frames);
  });

  test('frames before a violation are read first, then the error', async () => {
    const decoded = source(`${preambleHex} 01 00 00 00 02 00 00 00 00`).pipeThrough(
      new StreamFrameDecoderStream(),
    );
    const reader = decoded.getReader();
    expect(hexString((await reader.read()).value ?? [])).toBe('02');
    expect(await rejection(reader.read())).toBeInstanceOf(ProtocolError);
  });

  test('the frame limit applies', async () => {
    const decoded = source(`${preambleHex} 05 00 00 00`).pipeThrough(
      new StreamFrameDecoderStream({ maxFrameSize: 4 }),
    );
    const error = await rejection(collect(decoded));
    expect((error as SwitchboardError).code).toBe(StatusCode.frameTooLarge);
  });

  test('the decoder reads no further ahead than its reader', async () => {
    const decoder = new StreamFrameDecoderStream();
    const writer = decoder.writable.getWriter();
    await writer.write(hexBytes(`${preambleHex} 01 00 00 00 AA 01 00 00 00 BB`));
    // The next chunk waits until the frames decoded so far are read.
    let taken = false;
    const second = writer.write(hexBytes('01 00 00 00 CC')).then(() => {
      taken = true;
    });
    await new Promise((resolve) => setImmediate(resolve));
    expect(taken).toBe(false);
    const reader = decoder.readable.getReader();
    expect(hexString((await reader.read()).value ?? [])).toBe('AA');
    expect(hexString((await reader.read()).value ?? [])).toBe('BB');
    await second;
    expect(hexString((await reader.read()).value ?? [])).toBe('CC');
    await writer.close();
    expect((await reader.read()).done).toBe(true);
  });
});
