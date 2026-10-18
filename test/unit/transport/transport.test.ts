// The in-memory pair and the stream binding over byte pipes. Ports the
// `MemoryTransport` and `StreamTransport over byte pipes` groups of
// `test/transport_test.dart`; the TCP and WebSocket groups are ported with
// the Node transports.
import { describe, expect, test } from 'vitest';
import { hexBytes, hexString } from '../../../src/core/bytes.ts';
import { StatusCode, SwitchboardError } from '../../../src/core/status.ts';
import { memoryTransportPair } from '../../../src/core/transport/memory-transport.ts';
import { StreamFraming } from '../../../src/core/transport/stream-framing.ts';
import { StreamTransport, streamTransport } from '../../../src/core/transport/stream-transport.ts';
import { ManualClock } from '../../../src/testing/manual-clock.ts';
import {
  ByteSink,
  ByteSource,
  bytesOf,
  collect,
  pump,
  RawPeer,
  rejection,
  seeded,
  streamPair,
} from './support.ts';

const preambleHex = '53 57 42 44 01 00 00 00';

function expectCode(error: unknown, code: StatusCode): void {
  expect(error).toBeInstanceOf(SwitchboardError);
  expect((error as SwitchboardError).code).toBe(code);
}

/** Reads one chunk and returns it as hex, or `done`. */
async function next(reader: ReadableStreamDefaultReader<Uint8Array>): Promise<string> {
  const result = await reader.read();
  return result.done ? 'done' : hexString(result.value);
}

describe('MemoryTransport', () => {
  test('frames flow both ways in order and are copied', async () => {
    const [a, b] = memoryTransportPair();
    const qa = a.readable.getReader();
    const qb = b.readable.getReader();
    const wa = a.writable.getWriter();
    const wb = b.writable.getWriter();
    const frame = hexBytes('02 02 00 01');
    await wa.write(frame);
    frame[3] = 0xff;
    void wa.write(hexBytes('02 02 00 02'));
    void wb.write(hexBytes('02 03 00 03'));
    expect(await next(qb)).toBe('02 02 00 01');
    expect(await next(qb)).toBe('02 02 00 02');
    expect(await next(qa)).toBe('02 03 00 03');
    await wa.close();
    expect(await next(qa)).toBe('done');
    expect(await next(qb)).toBe('done');
  });

  test('closing one side ends both streams and drops later frames', async () => {
    const [a, b] = memoryTransportPair();
    const bDone = collect(b.readable);
    const aDone = collect(a.readable);
    await b.writable.close();
    expect(await aDone).toEqual([]);
    expect(await bDone).toEqual([]);
    // Dart's other sink drops silently and completes `done`. Here the other
    // writable is errored, so that its `closed` settles (design section 12,
    // item 11): later frames are refused rather than dropped.
    const wa = a.writable.getWriter();
    expectCode(await rejection(wa.closed), StatusCode.connectionLost);
    expectCode(await rejection(wa.write(hexBytes('02 02 00'))), StatusCode.connectionLost);
    await rejection(wa.close());
  });

  test('the other side learns of the close after the frames in flight', async () => {
    const clock = new ManualClock();
    const [a, b] = memoryTransportPair({ delay: 5, clock });
    const qa = a.readable.getReader();
    const qb = b.readable.getReader();
    const wa = a.writable.getWriter();
    const wb = b.writable.getWriter();
    let bClosed: unknown;
    wb.closed.catch((error: unknown) => {
      bClosed = error;
    });
    await wa.write(hexBytes('01'));
    const closing = wa.close();
    // The pair is closing: what b writes now is dropped, its writable still
    // open while the frame and the close are on their way to it.
    await wb.write(hexBytes('02'));
    clock.advance(4);
    await pump();
    expect(bClosed).toBeUndefined();
    clock.advance(1);
    expect(await next(qb)).toBe('01');
    expect(await next(qb)).toBe('done');
    expectCode(await rejection(wb.closed), StatusCode.connectionLost);
    expect(bClosed).toBeDefined();
    await closing;
    expect(await next(qa)).toBe('done');
  });

  test('delay keeps order and delivers the close last', async () => {
    const clock = new ManualClock();
    const [a, b] = memoryTransportPair({ delay: 5, clock });
    const received = collect(b.readable);
    const wa = a.writable.getWriter();
    void wa.write(hexBytes('01'));
    void wa.write(hexBytes('02'));
    void wa.write(hexBytes('03'));
    await wa.close();
    clock.advance(4);
    await pump();
    expect(clock.pendingCount).toBe(4);
    clock.advance(1);
    expect((await received).map(hexString)).toEqual(['01', '02', '03']);
    expect(clock.now()).toBe(5);
  });

  test('a delay of 0 still goes through the clock', async () => {
    const clock = new ManualClock();
    const [a, b] = memoryTransportPair({ delay: 0, clock });
    const qb = b.readable.getReader();
    await a.writable.getWriter().write(hexBytes('01'));
    expect(clock.pendingCount).toBe(1);
    clock.advance(0);
    expect(await next(qb)).toBe('01');
  });

  test('maxFrameSize fails the receiving side', async () => {
    const [a, b] = memoryTransportPair({ maxFrameSize: 4 });
    const queue = b.readable.getReader();
    const wa = a.writable.getWriter();
    void wa.write(hexBytes('01 02 03 04'));
    void wa.write(hexBytes('01 02 03 04 05'));
    expect(await next(queue)).toBe('01 02 03 04');
    expectCode(await rejection(next(queue)), StatusCode.frameTooLarge);
    expectCode(await rejection(queue.closed), StatusCode.frameTooLarge);
    expect(await collect(a.readable)).toEqual([]);
  });

  test('maxFrameSize keeps the failed side writable until it reacted', async () => {
    const [a, b] = memoryTransportPair({ maxFrameSize: 8 });
    expect(a.maxFrameSize).toBe(8);
    expect(b.maxFrameSize).toBe(8);
    const qa = a.readable.getReader();
    const wb = b.writable.getWriter();
    const data: Uint8Array[] = [];
    const errors: unknown[] = [];
    const bDone = (async () => {
      try {
        for await (const frame of b.readable) {
          data.push(frame);
        }
      } catch (error) {
        errors.push(error);
        // A mux sends GOAWAY from here; it must reach the sender.
        void wb.write(hexBytes('02 00 00 03 23 00'));
      }
    })();
    const wa = a.writable.getWriter();
    void wa.write(new Uint8Array(9));
    // Dropped: the receiving side has failed.
    void wa.write(hexBytes('01'));
    await bDone;
    expect(data).toEqual([]);
    expect(errors).toHaveLength(1);
    expectCode(errors[0], StatusCode.frameTooLarge);
    expect(await next(qa)).toBe('02 00 00 03 23 00');
    // Closed on b's behalf once its reader had the error: b's writable is
    // errored with that error, a's with the close.
    expect(await next(qa)).toBe('done');
    expectCode(await rejection(wb.closed), StatusCode.frameTooLarge);
    expectCode(await rejection(wa.closed), StatusCode.connectionLost);
  });

  test('a failed side that nobody reads closes the pair at once', async () => {
    const [a, b] = memoryTransportPair({ maxFrameSize: 2 });
    await a.writable.getWriter().write(hexBytes('01 02 03'));
    expect(await collect(a.readable)).toEqual([]);
    expectCode(await rejection(collect(b.readable)), StatusCode.frameTooLarge);
  });

  test('without maxFrameSize there is no limit', async () => {
    const [a, b] = memoryTransportPair();
    expect(a.maxFrameSize).toBe(0);
    await b.writable.close();
  });

  test('maxFrameSize zero means no limit', async () => {
    const [a, b] = memoryTransportPair({ maxFrameSize: 0 });
    const received = b.readable.getReader();
    await a.writable.getWriter().write(hexBytes('02 02 00 AA'));
    expect(await next(received)).toBe('02 02 00 AA');
    expect(b.maxFrameSize).toBe(0);
  });

  test('aborting a writable closes the pair and rejects its closed promise', async () => {
    const [a, b] = memoryTransportPair();
    const wa = a.writable.getWriter();
    const boom = new Error('boom');
    await wa.abort(boom);
    expect(await rejection(wa.closed)).toBe(boom);
    await rejection(wa.close());
    expect(await collect(b.readable)).toEqual([]);
    expect(await collect(a.readable)).toEqual([]);
  });

  test('aborting drops the frames in flight', async () => {
    const clock = new ManualClock();
    const [a, b] = memoryTransportPair({ delay: 5, clock });
    const wa = a.writable.getWriter();
    await wa.write(hexBytes('01'));
    await wa.abort(new Error('boom'));
    expect(clock.pendingCount).toBe(0);
    expect(await collect(b.readable)).toEqual([]);
  });

  test('the remotes are the given ones', () => {
    const [a, b] = memoryTransportPair({ remotes: ['mem://server', 'memory peer'] });
    expect(a.remote).toBe('mem://server');
    expect(b.remote).toBe('memory peer');
    const [c] = memoryTransportPair();
    expect(c.remote).toBeUndefined();
    expect('remote' in c).toBe(false);
  });

  test('options are validated', () => {
    expect(() => memoryTransportPair({ delay: -1 })).toThrow(RangeError);
    expect(() => memoryTransportPair({ maxFrameSize: -1 })).toThrow(RangeError);
    expect(() => memoryTransportPair({ maxFrameSize: 1.5 })).toThrow(RangeError);
  });
});

describe('StreamTransport over byte pipes', () => {
  test('writes the preamble immediately', async () => {
    const peer = new RawPeer();
    expect(await peer.read(8)).toBe(preambleHex);
    const writer = peer.transport.writable.getWriter();
    void writer.write(hexBytes('02 02 00 AA'));
    expect(await peer.read(8)).toBe('04 00 00 00 02 02 00 AA');
    // Closing resolves once the output has been drained and closed.
    const closed = writer.close();
    expect(await peer.fromTransport.hasNext()).toBe(false);
    await closed;
  });

  test('round trip between two wrapped endpoints', async () => {
    const [a, b] = streamPair();
    const qa = a.readable.getReader();
    const qb = b.readable.getReader();
    const wa = a.writable.getWriter();
    void wa.write(hexBytes('02 02 00 01'));
    void wa.write(bytesOf(70000));
    void b.writable.getWriter().write(hexBytes('02 03 00 02'));
    expect(await next(qb)).toBe('02 02 00 01');
    const big = await qb.read();
    expect(big.value).toEqual(bytesOf(70000));
    expect(await next(qa)).toBe('02 03 00 02');
    const closed = wa.close();
    expect(await next(qb)).toBe('done');
    expect(await next(qa)).toBe('done');
    await closed;
  });

  test('several frames in one chunk', async () => {
    const peer = new RawPeer();
    const queue = peer.transport.readable.getReader();
    peer.toTransport.add(`${preambleHex} 01 00 00 00 AA 02 00 00 00 BB CC 03 00 00 00 DD EE FF`);
    expect(await next(queue)).toBe('AA');
    expect(await next(queue)).toBe('BB CC');
    expect(await next(queue)).toBe('DD EE FF');
    peer.toTransport.end();
    expect(await next(queue)).toBe('done');
  });

  test('one byte chunks', async () => {
    const [a, b] = streamPair({ chunkSize: 1 });
    const received = collect(b.readable);
    // Read too, so that b's output has a reader when b closes it.
    const returned = collect(a.readable);
    const frames = Array.from({ length: 39 }, (_, i) => bytesOf(i + 1, i + 1));
    const writer = a.writable.getWriter();
    for (const frame of frames) {
      void writer.write(frame);
    }
    await writer.close();
    expect(await received).toEqual(frames);
    expect(await returned).toEqual([]);
  });

  test('arbitrary chunk boundaries', async () => {
    const random = seeded(7);
    const frames = Array.from({ length: 200 }, (_, i) => bytesOf(1 + random(3000), i));
    const peer = new RawPeer();
    const received = collect(peer.transport.readable);
    const parts = [StreamFraming.encodePreamble(), ...frames.map(StreamFraming.encodeFrame)];
    const all = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
    let offset = 0;
    for (const p of parts) {
      all.set(p, offset);
      offset += p.length;
    }
    let pos = 0;
    while (pos < all.length) {
      const n = Math.min(1 + random(5000), all.length - pos);
      peer.toTransport.add(all.slice(pos, pos + n));
      pos += n;
    }
    peer.toTransport.end();
    expect(await received).toEqual(frames);
  });

  for (const [name, hex, code] of [
    ['wrong magic', '53 57 42 58 01 00 00 00 01 00 00 00 AA', StatusCode.protocolError],
    ['unsupported version', '53 57 42 44 02 00 00 00 01 00 00 00 AA', StatusCode.unsupported],
    ['zero length frame', `${preambleHex} 00 00 00 00`, StatusCode.protocolError],
  ] as const) {
    test(`${name} fails the stream with ${code}`, async () => {
      const peer = new RawPeer();
      const queue = peer.transport.readable.getReader();
      expect(await peer.read(8)).toBe(preambleHex);
      peer.toTransport.add(hex);
      expectCode(await rejection(queue.read()), code);
      expectCode(await rejection(queue.closed), code);
      // The output closes once the error was handed over.
      expect(await peer.fromTransport.hasNext()).toBe(false);
    });
  }

  test('frame over the limit fails with FRAME_TOO_LARGE', async () => {
    const peer = new RawPeer({ maxFrameSize: 16 });
    const events: unknown[] = [];
    const writer = peer.transport.writable.getWriter();
    const done = (async () => {
      try {
        for await (const frame of peer.transport.readable) {
          events.push(hexString(frame));
        }
      } catch (error) {
        events.push(error);
        // A mux sends GOAWAY from here; it must still go out.
        void writer.write(hexBytes('02 00 00 03 23 00'));
      }
    })();
    // Length only: the body is never needed to fail.
    peer.toTransport.add(`${preambleHex} 01 00 00 00 AA 11 00 00 00`);
    await done;
    expect(events).toHaveLength(2);
    expect(events[0]).toBe('AA');
    expectCode(events[1], StatusCode.frameTooLarge);
    expect(await peer.read(8)).toBe(preambleHex);
    expect(await peer.read(10)).toBe('06 00 00 00 02 00 00 03 23 00');
    expect(await peer.fromTransport.hasNext()).toBe(false);
  });

  test('with a manual clock the output closes on the next turn after the error', async () => {
    const clock = new ManualClock();
    const peer = new RawPeer({ clock });
    const queue = peer.transport.readable.getReader();
    peer.toTransport.add(`${preambleHex} 00 00 00 00`);
    expectCode(await rejection(queue.read()), StatusCode.protocolError);
    await pump();
    expect(peer.fromTransport.isClosed).toBe(false);
    clock.advance(0);
    expect(await peer.read(8)).toBe(preambleHex);
    expect(await peer.fromTransport.hasNext()).toBe(false);
  });

  test('input end closes the stream and the output', async () => {
    const peer = new RawPeer();
    const queue = peer.transport.readable.getReader();
    peer.toTransport.add(`${preambleHex} 01 00 00 00 AA 05 00`);
    expect(await next(queue)).toBe('AA');
    peer.toTransport.end();
    expect(await next(queue)).toBe('done');
    expect(await peer.read(8)).toBe(preambleHex);
    expect(await peer.fromTransport.hasNext()).toBe(false);
    // Dart drops a later frame silently. A WritableStream cannot stay
    // writable once its output is gone and still settle its closed
    // promise: the writable is errored, so a write is refused with
    // CONNECTION_LOST, the close as the stream refuses a close of an
    // errored stream, and nothing more goes out.
    const writer = peer.transport.writable.getWriter();
    expectCode(await rejection(writer.closed), StatusCode.connectionLost);
    expectCode(await rejection(writer.write(hexBytes('01'))), StatusCode.connectionLost);
    expect(await rejection(writer.close())).toBeInstanceOf(TypeError);
    expect(peer.fromTransport.bytes).toHaveLength(8);
  });

  test('the writable settles once the transport closed its output on its own', async () => {
    // A consumer waiting on `closed` learns that the output is gone: when
    // the peer closed, when the output failed, when the transport aborted.
    const closedBy = async (end: (peer: RawPeer) => void): Promise<unknown> => {
      const peer = new RawPeer();
      // The input is read only as the readable is.
      void collect(peer.transport.readable).catch(() => {});
      const writer = peer.transport.writable.getWriter();
      await writer.write(hexBytes('AA'));
      end(peer);
      return rejection(writer.closed);
    };
    const finEnded = await closedBy((peer) => peer.toTransport.end());
    expectCode(finEnded, StatusCode.connectionLost);
    expect((finEnded as SwitchboardError).status.reason).toBe('the peer closed the connection');
    expectCode(await closedBy((peer) => peer.transport.abort()), StatusCode.connectionLost);
    const broken = new Error('connection reset');
    expect(await closedBy((peer) => peer.toTransport.fail(broken))).toBe(broken);
    // Closed by its own user, it closes as asked.
    const peer = new RawPeer();
    const writer = peer.transport.writable.getWriter();
    await writer.close();
    await writer.closed;
    peer.toTransport.end();
    await pump();
    await writer.closed;
  });

  test('an input error ends the stream with it and closes the output', async () => {
    const peer = new RawPeer();
    const queue = peer.transport.readable.getReader();
    const broken = new Error('connection reset');
    peer.toTransport.add(`${preambleHex} 01 00 00 00 AA`);
    expect(await next(queue)).toBe('AA');
    peer.toTransport.fail(broken);
    expect(await rejection(queue.read())).toBe(broken);
    expect(await peer.read(8)).toBe(preambleHex);
    expect(await peer.fromTransport.hasNext()).toBe(false);
  });

  test('an output that fails drops what waits and leaves the input to end', async () => {
    const input = new ByteSource();
    let writes = 0;
    const output = new WritableStream<Uint8Array>({
      write() {
        writes++;
        if (writes > 1) {
          throw new Error('connection reset');
        }
      },
    });
    const transport = streamTransport({ readable: input.readable, writable: output });
    const queue = transport.readable.getReader();
    const writer = transport.writable.getWriter();
    await writer.write(hexBytes('AA'));
    await pump();
    // The write that meets the failure is taken, or refused with it as the
    // failure errors the writable meanwhile: Dart drops it silently.
    const second = await writer.write(hexBytes('BB')).then(
      () => undefined,
      (error: unknown) => error,
    );
    if (second !== undefined) {
      expect((second as Error).message).toBe('connection reset');
    }
    expect(transport.bufferedOutputBytes).toBe(0);
    expect(transport.isOutputReady).toBe(true);
    expect(writes).toBe(2);
    input.add(`${preambleHex} 01 00 00 00 CC`);
    expect(await next(queue)).toBe('CC');
    input.end();
    expect(await next(queue)).toBe('done');
    // The output is gone: the writable is errored with its failure.
    expect(((await rejection(writer.closed)) as Error).message).toBe('connection reset');
    expect(await rejection(writer.close())).toBeInstanceOf(TypeError);
  });

  test('closing the sink ends the stream', async () => {
    const peer = new RawPeer();
    const received = collect(peer.transport.readable);
    const closed = peer.transport.writable.close();
    peer.toTransport.add(`${preambleHex} 01 00 00 00 AA`);
    expect(await received).toEqual([]);
    expect(await peer.read(8)).toBe(preambleHex);
    expect(await peer.fromTransport.hasNext()).toBe(false);
    await closed;
  });

  test('cancelling the readable keeps reading until the peer closes', async () => {
    const input = new ByteSource();
    const output = new ByteSink();
    const transport = streamTransport({ readable: input.readable, writable: output.writable });
    const reader = transport.readable.getReader();
    const pending = reader.read();
    await pump();
    expect(transport.isInputStarted).toBe(true);
    await reader.cancel();
    expect((await pending).done).toBe(true);
    // Still read, and dropped.
    input.add(`${preambleHex} 01 00 00 00 BB`);
    await pump();
    expect(input.unread).toBe(0);
    const writer = transport.writable.getWriter();
    void writer.write(hexBytes('AA'));
    input.end();
    await writer.close();
    expect(hexString(output.bytes)).toBe(`${preambleHex} 01 00 00 00 AA`);
    expect(output.isClosed).toBe(true);
  });

  test('the defaults and the frame limit', () => {
    expect(StreamTransport.defaultMaxFrameSize).toBe(1024 * 1024);
    expect(StreamFraming.defaultMaxFrameSize).toBe(1024 * 1024);
    expect(StreamTransport.defaultOutputHighWaterMark).toBe(16 * 1024 * 1024);
    expect(StreamTransport.defaultCloseTimeout).toBe(10_000);
    expect(hexString(StreamFraming.encodePreamble({ version: 2, flags: 0x0102 }))).toBe(
      '53 57 42 44 02 00 02 01',
    );
    expect(hexString(StreamFraming.magic)).toBe('53 57 42 44');
    expect(StreamFraming.version).toBe(1);
    const peer = new RawPeer({ maxFrameSize: 77, remote: 'tcp://peer:1' });
    expect(peer.transport.maxFrameSize).toBe(77);
    expect(peer.transport.remote).toBe('tcp://peer:1');
    expect(() => new RawPeer({ maxFrameSize: -1 })).toThrow(RangeError);
    expect(() => new RawPeer({ closeTimeout: -1 })).toThrow(RangeError);
    expect(() => new RawPeer({ outputHighWaterMark: 0.5 })).toThrow(RangeError);
  });

  test('output that does not keep up throttles the input', async () => {
    const peer = new RawPeer({ outputHighWaterMark: 1000, paused: true });
    const transport = peer.transport;
    await pump();
    // The preamble went to the byte sink, which holds it.
    expect(transport.acceptedOutputBytes).toBe(8);
    expect(transport.isInputThrottled).toBe(false);
    const writer = transport.writable.getWriter();
    for (let i = 0; i < 9; i++) {
      await writer.write(new Uint8Array(100));
    }
    // Nine frames of 104 bytes wait for the output.
    expect(transport.bufferedOutputBytes).toBe(9 * 104);
    expect(transport.isInputThrottled).toBe(false);
    await writer.write(new Uint8Array(100));
    expect(transport.bufferedOutputBytes).toBe(10 * 104);
    expect(transport.isInputThrottled).toBe(true);
    expect(transport.isOutputReady).toBe(false);
    // While throttled, the input is not read.
    const received = collect(transport.readable);
    peer.toTransport.add(preambleHex);
    await pump();
    expect(peer.toTransport.unread).toBe(1);
    let ready = false;
    const readiness = transport.outputReady.then(() => {
      ready = true;
    });
    await pump();
    expect(ready).toBe(false);

    peer.fromTransport.resume();
    await readiness;
    expect(transport.isOutputReady).toBe(true);
    await transport.outputReady;
    expect(transport.bufferedOutputBytes).toBe(0);
    expect(transport.acceptedOutputBytes).toBe(8 + 10 * 104);
    expect(transport.isInputThrottled).toBe(false);
    await pump();
    expect(peer.toTransport.unread).toBe(0);
    await peer.read(8 + 10 * 104);
    expect(peer.fromTransport.chunks[0]).toEqual(StreamFraming.encodePreamble());
    // Coalesced into one write.
    expect(peer.fromTransport.chunks).toHaveLength(2);

    peer.toTransport.end();
    expect(await received).toEqual([]);
    // The peer closed: so did the output, and the writable with it.
    expectCode(await rejection(writer.closed), StatusCode.connectionLost);
    expect(await rejection(writer.close())).toBeInstanceOf(TypeError);
  });

  test('a close that cannot drain the output aborts', async () => {
    const clock = new ManualClock();
    let aborts = 0;
    const peer = new RawPeer({
      clock,
      closeTimeout: 50,
      paused: true,
      abort: () => {
        aborts++;
      },
    });
    const transport = peer.transport;
    const received = collect(transport.readable);
    const writer = transport.writable.getWriter();
    await writer.write(hexBytes('02 02 00'));
    expect(transport.bufferedOutputBytes).toBe(7);
    expect(transport.isOutputReady).toBe(false);
    const readiness = transport.outputReady;
    const closed = writer.close();
    await readiness;
    expect(transport.isOutputReady).toBe(true);
    await pump();
    expect(aborts).toBe(0);
    clock.advance(49);
    expect(aborts).toBe(0);
    clock.advance(1);
    await closed;
    expect(aborts).toBe(1);
    expect(transport.bufferedOutputBytes).toBe(0);
    expect(await received).toEqual([]);
    transport.abort();
    expect(aborts).toBe(1);
    // The byte sink is aborted once its write in progress returns.
    peer.fromTransport.resume();
    await pump();
    expect(peer.fromTransport.isAborted).toBe(true);
    expect(hexString(peer.fromTransport.bytes)).toBe(preambleHex);
  });

  test('aborting the writable closes and rejects its closed promise', async () => {
    const peer = new RawPeer();
    const writer = peer.transport.writable.getWriter();
    const boom = new Error('boom');
    await writer.abort(boom);
    expect(await rejection(writer.closed)).toBe(boom);
    expect(await peer.read(8)).toBe(preambleHex);
    expect(await peer.fromTransport.hasNext()).toBe(false);
  });

  test('an empty frame is a RangeError and closes the transport', async () => {
    const peer = new RawPeer();
    const writer = peer.transport.writable.getWriter();
    expect(await rejection(writer.write(new Uint8Array(0)))).toBeInstanceOf(RangeError);
    expect(await peer.read(8)).toBe(preambleHex);
    expect(await peer.fromTransport.hasNext()).toBe(false);
  });

  test('abort drops the output, ends the stream and tears the pipes down', async () => {
    const peer = new RawPeer({ paused: true });
    const queue = peer.transport.readable.getReader();
    peer.toTransport.add(`${preambleHex} 01 00 00 00 AA`);
    expect(await next(queue)).toBe('AA');
    const writer = peer.transport.writable.getWriter();
    await writer.write(hexBytes('01 02'));
    expect(peer.transport.bufferedOutputBytes).toBe(6);
    peer.transport.abort();
    expect(peer.transport.bufferedOutputBytes).toBe(0);
    expect(peer.transport.isOutputReady).toBe(true);
    expect(await next(queue)).toBe('done');
    expectCode(await rejection(writer.closed), StatusCode.connectionLost);
    expect(await rejection(writer.close())).toBeInstanceOf(TypeError);
    peer.fromTransport.resume();
    await pump();
    expect(peer.fromTransport.isAborted).toBe(true);
    expect(hexString(peer.fromTransport.bytes)).toBe(preambleHex);
  });

  describe('linger', () => {
    test('reads and drops input after the close until the peer closes', async () => {
      const clock = new ManualClock();
      const peer = new RawPeer({ clock, linger: true });
      const received = collect(peer.transport.readable);
      peer.toTransport.add(`${preambleHex} 01 00 00 00 AA`);
      await pump();
      const closed = peer.transport.writable.close();
      expect(await peer.read(8)).toBe(preambleHex);
      expect(await peer.fromTransport.hasNext()).toBe(false);
      await closed;
      // Still drained: the peer's late frames do not pile up unread.
      peer.toTransport.add('01 00 00 00 BB');
      await pump();
      expect(peer.toTransport.unread).toBe(0);
      expect(clock.pendingCount).toBe(1);
      peer.toTransport.end();
      await pump();
      expect(clock.pendingCount).toBe(0);
      expect((await received).map(hexString)).toEqual(['AA']);
      expect(peer.fromTransport.isAborted).toBe(false);
    });

    test('aborts when the peer does not close within the close timeout', async () => {
      const clock = new ManualClock();
      let aborts = 0;
      const peer = new RawPeer({
        clock,
        linger: true,
        closeTimeout: 100,
        abort: () => {
          aborts++;
        },
      });
      await peer.transport.writable.close();
      expect(await peer.read(8)).toBe(preambleHex);
      expect(peer.transport.isInputStarted).toBe(true);
      clock.advance(99);
      expect(aborts).toBe(0);
      clock.advance(1);
      expect(aborts).toBe(1);
    });

    test('without linger the input is cancelled on close', async () => {
      const peer = new RawPeer();
      await peer.transport.writable.close();
      peer.toTransport.add(preambleHex);
      expect(peer.toTransport.unread).toBe(0);
      expect(peer.transport.isInputStarted).toBe(false);
    });
  });
});
