// The in-memory QUIC provider the binding's tests run on: its streams,
// flow control, resets, STOP_SENDING, stream limit, delays and session ends
// must behave as the provider contract says, or the binding tests prove
// nothing.
import { describe, expect, test } from 'vitest';
import {
  QuicError,
  type QuicStream,
  QuicStreamReset,
  QuicStreamStopped,
} from '../../../src/core/quic/contract.ts';
import {
  MemoryQuicNetwork,
  MemoryQuicSession,
  type MemoryQuicStream,
} from '../../../src/testing/memory-quic-provider.ts';

const bytes = (...values: number[]) => Uint8Array.of(...values);

async function readAll(stream: QuicStream): Promise<number[]> {
  const out: number[] = [];
  for (;;) {
    const chunk = await stream.read(1 << 20);
    if (chunk === undefined) {
      return out;
    }
    out.push(...chunk);
  }
}

async function nextStream(session: MemoryQuicSession): Promise<QuicStream> {
  const it = session.incomingStreams[Symbol.asyncIterator]();
  const next = await it.next();
  if (next.done) {
    throw new Error('no stream');
  }
  return next.value;
}

describe('in-memory QUIC provider', () => {
  test('a stream becomes visible to the peer with its first bytes, delivered in order to FIN', async () => {
    const [a, b] = MemoryQuicSession.pair();
    const stream = await a.openStream();
    expect(b.announced).toEqual([]);
    await stream.write(bytes(1, 2, 3));
    await stream.write(bytes(4));
    await stream.finish();
    const remote = await nextStream(b);
    expect(await readAll(remote)).toEqual([1, 2, 3, 4]);
    expect(await stream.stopped).toBeUndefined();
    expect(b.announced).toEqual([0]);
  });

  test('read(max) returns at most max bytes and keeps the rest', async () => {
    const [a, b] = MemoryQuicSession.pair();
    const stream = await a.openStream();
    await stream.write(bytes(1, 2, 3, 4, 5));
    const remote = await nextStream(b);
    expect([...((await remote.read(2)) ?? [])]).toEqual([1, 2]);
    expect([...((await remote.read(10)) ?? [])]).toEqual([3, 4, 5]);
    await expect(remote.read(0)).rejects.toBeInstanceOf(RangeError);
  });

  test('one read at a time', async () => {
    const [a, b] = MemoryQuicSession.pair();
    const stream = await a.openStream();
    await stream.write(bytes(1));
    const remote = await nextStream(b);
    await remote.read(1);
    const pending = remote.read(1);
    await expect(remote.read(1)).rejects.toThrow('a read is pending');
    await stream.finish();
    expect(await pending).toBeUndefined();
  });

  test('a write waits for the stream window until the receiver reads', async () => {
    const [a, b] = MemoryQuicSession.pair({ acceptor: { streamWindow: 4 } });
    const stream = await a.openStream();
    let written = false;
    const write = stream.write(bytes(1, 2, 3, 4, 5, 6)).then(() => {
      written = true;
    });
    const remote = await nextStream(b);
    await new Promise((r) => setTimeout(r, 10));
    expect(written).toBe(false);
    expect([...((await remote.read(100)) ?? [])]).toEqual([1, 2, 3, 4]);
    await write;
    expect([...((await remote.read(100)) ?? [])]).toEqual([5, 6]);
  });

  test('the connection window bounds all streams together', async () => {
    const [a, b] = MemoryQuicSession.pair({ acceptor: { connectionWindow: 6 } });
    const s1 = await a.openStream();
    const s2 = await a.openStream();
    await s1.write(bytes(1, 2, 3, 4));
    let second = false;
    const w2 = s2.write(bytes(5, 6, 7, 8)).then(() => {
      second = true;
    });
    const r1 = await nextStream(b);
    await new Promise((r) => setTimeout(r, 10));
    expect(second).toBe(false);
    await r1.read(100);
    await w2;
  });

  test('a reset reaches the reader, even one with no read pending, and drops the unread bytes', async () => {
    const [a, b] = MemoryQuicSession.pair();
    const stream = await a.openStream();
    await stream.write(bytes(1, 2));
    const remote = await nextStream(b);
    stream.reset(7);
    expect(await remote.resetReceived).toBe(7);
    const error = await remote.read(10).catch((e) => e);
    expect(error).toBeInstanceOf(QuicStreamReset);
    expect((error as QuicStreamReset).code).toBe(7);
    expect(await stream.stopped).toBeUndefined();
  });

  test('a reset without a code arrives as undefined', async () => {
    const [a, b] = MemoryQuicSession.pair();
    const stream = (await a.openStream()) as MemoryQuicStream;
    await stream.write(bytes(1));
    const remote = await nextStream(b);
    stream.resetWithoutCode();
    expect(await remote.resetReceived).toBeUndefined();
  });

  test('STOP_SENDING fails the writes of the sender, resolves stopped with its code, and the stack resets with it', async () => {
    const [a, b] = MemoryQuicSession.pair({ acceptor: { streamWindow: 2 } });
    const stream = await a.openStream();
    const write = stream.write(bytes(1, 2, 3, 4));
    const remote = await nextStream(b);
    remote.stop(9);
    await expect(remote.read(1)).rejects.toBeInstanceOf(QuicStreamReset);
    const error = await write.catch((e) => e);
    expect(error).toBeInstanceOf(QuicStreamStopped);
    expect((error as QuicStreamStopped).code).toBe(9);
    expect(await stream.stopped).toBe(9);
    expect(await remote.resetReceived).toBe(9);
    await expect(stream.write(bytes(1))).rejects.toBeInstanceOf(QuicStreamStopped);
  });

  test('STOP_SENDING without a code rejects stopped, so it is never taken for an acknowledgement', async () => {
    const [a, b] = MemoryQuicSession.pair();
    const stream = await a.openStream();
    await stream.write(bytes(1));
    const remote = (await nextStream(b)) as MemoryQuicStream;
    remote.stopWithoutCode();
    const error = await stream.stopped.catch((e) => e);
    expect(error).toBeInstanceOf(QuicStreamStopped);
    expect((error as QuicStreamStopped).code).toBeUndefined();
  });

  test('a write after finish or reset fails; finish is idempotent; reset after FIN acknowledged is a no-op', async () => {
    const [a, b] = MemoryQuicSession.pair();
    const stream = await a.openStream();
    await stream.write(bytes(1));
    const f1 = stream.finish();
    expect(stream.finish()).toBe(f1);
    await f1;
    await expect(stream.write(bytes(2))).rejects.toThrow('ended');
    const remote = await nextStream(b);
    expect(await readAll(remote)).toEqual([1]);
    stream.reset(3);
    expect(await stream.stopped).toBeUndefined();
  });

  test('the stream limit: an open waits until the peer released a stream (both directions ended)', async () => {
    const [a, b] = MemoryQuicSession.pair({ acceptor: { maxStreams: 1 } });
    const first = await a.openStream();
    let opened = false;
    const second = a.openStream().then((s) => {
      opened = true;
      return s;
    });
    await first.write(bytes(1));
    await first.finish();
    const remote = await nextStream(b);
    await new Promise((r) => setTimeout(r, 10));
    expect(opened).toBe(false);
    expect(await readAll(remote)).toEqual([1]);
    await remote.finish();
    expect(await readAll(first)).toEqual([]);
    await second;
    expect(opened).toBe(true);
  });

  test('delays per stream let a later stream overtake an earlier one', async () => {
    const network = new MemoryQuicNetwork({
      delay: (stream) => (stream.ordinal === 0 ? 30 : 0),
    });
    const [a, b] = MemoryQuicSession.pair({ network });
    const s0 = await a.openStream();
    const s1 = await a.openStream();
    await s0.write(bytes(0));
    await s1.write(bytes(1));
    const first = await nextStream(b);
    expect([...((await first.read(1)) ?? [])]).toEqual([1]);
    expect(b.announced).toEqual([1]);
  });

  test('a session close reaches the peer as byPeer and fails what is pending', async () => {
    const [a, b] = MemoryQuicSession.pair();
    const stream = await a.openStream();
    await stream.write(bytes(1));
    const remote = await nextStream(b);
    await remote.read(1);
    const pending = remote.read(1);
    await a.close(16, 'bye');
    expect(await a.done).toEqual({ code: 16, reason: 'bye', byPeer: false, lost: false });
    expect(await b.done).toEqual({ code: 16, reason: 'bye', byPeer: true, lost: false });
    const error = await pending.catch((e) => e);
    expect(error).toBeInstanceOf(QuicError);
    expect((error as QuicError).kind).toBe('sessionEnded');
    await expect(a.openStream()).rejects.toBeInstanceOf(QuicError);
    await expect(remote.resetReceived).rejects.toBeInstanceOf(QuicError);
    expect(a.closes).toEqual([[16, 'bye']]);
  });

  test('a close reason is cut to 1024 bytes at a character boundary', async () => {
    const [a] = MemoryQuicSession.pair();
    await a.close(0, 'é'.repeat(1000));
    const end = await a.done;
    expect(new TextEncoder().encode(end.reason).length).toBe(1024);
  });

  test('a lost session ends both sides as lost', async () => {
    const [a, b] = MemoryQuicSession.pair();
    a.lose('idle');
    expect((await a.done).lost).toBe(true);
    expect((await b.done).lost).toBe(true);
    expect((await b.done).code).toBeUndefined();
  });

  test('exporters: equal at both ends, undefined without one', async () => {
    const [a, b] = MemoryQuicSession.pair({ initiator: { exporter: false } });
    const empty = new Uint8Array(0);
    expect(await a.exportKeyingMaterial('x', empty, 32)).toBeUndefined();
    expect((await b.exportKeyingMaterial('x', empty, 32))?.length).toBe(32);
  });

  test('unidirectional streams and draining', async () => {
    const [a, b] = MemoryQuicSession.pair();
    const uni = a.openUnidirectional();
    const it = b.incomingUnidirectionalStreams[Symbol.asyncIterator]();
    const next = await it.next();
    next.value?.stop(36);
    expect(await uni.stopped.promise).toBe(36);
    a.drainPeer();
    await b.draining;
  });

  describe('connector and listener', () => {
    const config = { alpn: ['switchboard'] };

    test('dials a listener; the listener yields the other end', async () => {
      const network = new MemoryQuicNetwork();
      const listener = network.listen('quic://node.example:4433');
      const session = await network.connector().connect('quic://node.example:4433', config);
      const it = listener.sessions[Symbol.asyncIterator]();
      const accepted = (await it.next()).value;
      expect(session.isInitiator).toBe(true);
      expect(accepted?.isInitiator).toBe(false);
      await listener.close();
      expect((await it.next()).done).toBe(true);
    });

    test('refuses other schemes, unknown endpoints, unpinned certificates, negative durations', async () => {
      const network = new MemoryQuicNetwork();
      network.listen('quic://node.example:4433', { certificateHash: new Uint8Array(32).fill(1) });
      const connector = network.connector();
      const kind = (p: Promise<unknown>) => p.catch((e: QuicError) => e.kind);
      expect(await kind(connector.connect('https://node.example:4433/sb', config))).toBe(
        'unsupportedScheme',
      );
      expect(await kind(connector.connect('quic://other.example:4433', config))).toBe(
        'unreachable',
      );
      expect(
        await kind(
          connector.connect('quic://node.example:4433', {
            ...config,
            serverCertificateHashes: [new Uint8Array(32)],
          }),
        ),
      ).toBe('certificateRejected');
      await connector.connect('quic://node.example:4433', {
        ...config,
        serverCertificateHashes: [new Uint8Array(32).fill(1)],
      });
      await expect(
        connector.connect('quic://node.example:4433', { ...config, idleTimeout: -1 }),
      ).rejects.toBeInstanceOf(RangeError);
    });

    test('the client configuration sets what the initiator grants', async () => {
      const network = new MemoryQuicNetwork();
      network.listen('https://node.example:4433/sb');
      const connector = network.connector({ webTransport: true });
      const session = (await connector.connect('https://node.example:4433/sb', {
        ...config,
        maxStreamsBidi: 7,
        streamReceiveWindow: 1000,
      })) as MemoryQuicSession;
      expect(session.maxIncomingStreams).toBe(7);
      expect(session.config.streamWindow).toBe(1000);
      expect(connector.configs.length).toBe(1);
    });
  });
});
