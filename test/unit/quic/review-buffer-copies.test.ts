// Review finding (QUIC binding, docs/notes/review-quic.md): what the binding "copies" with
// `.slice()` is not copied when the caller passes a Node Buffer, whose `slice` is a view on the
// same memory (design section 5 accepts a Buffer wherever a Uint8Array goes). The OPEN record of a
// channel is written once its stream opens, asynchronously, so a Buffer the caller reuses after
// `open()` returned reaches the wire changed, and `channel.openPayload` changes with it
// (quic-binding-connection.ts, `#openLocal`); the frame mux writes the OPEN frame at once, so only
// its `openPayload` aliases. A PING payload is kept the same way to match its PONG, so a reused
// Buffer leaves the ping unanswered until the connection closes. The in-memory provider's write
// keeps the caller's Buffer too (memory-quic-provider.ts, `MemoryQuicDirection.write`), against the
// provider contract ("resolves once the stack accepted them (copied)"). Each case is `test.fails`
// until the copies are real (`Uint8Array.prototype.slice.call`, or `new Uint8Array(x)`).
// In-memory provider, virtual time.
import { Buffer } from 'node:buffer';
import { describe, expect, test } from 'vitest';
import type { MuxOptions } from '../../../src/core/mux/contract.ts';
import { QuicBindingConnection } from '../../../src/core/quic/quic-binding-connection.ts';
import { ManualClock } from '../../../src/testing/manual-clock.ts';
import { MemoryQuicNetwork, MemoryQuicSession } from '../../../src/testing/memory-quic-provider.ts';

const text = (data: Uint8Array): string => new TextDecoder().decode(data);

/** A connected pair of the binding over the in-memory provider on [clock]. */
async function connectionPair(
  clock: ManualClock,
): Promise<{ client: QuicBindingConnection; server: QuicBindingConnection }> {
  const network = new MemoryQuicNetwork({ clock });
  const [a, b] = MemoryQuicSession.pair({ network });
  const options: Partial<MuxOptions> = { clock, keepAliveInterval: undefined };
  const accepting = QuicBindingConnection.start(b, { options });
  const dialling = QuicBindingConnection.start(a, { options });
  await clock.elapseAsync(0);
  return { client: await dialling, server: await accepting };
}

describe('review: Buffers the binding and the in-memory provider keep', () => {
  test.fails('an OPEN payload passed as a Buffer is copied: reusing the Buffer after open() changes neither the record nor openPayload', async () => {
    const clock = new ManualClock();
    const { client, server } = await connectionPair(clock);
    const arriving = server.incoming[Symbol.asyncIterator]().next();
    const payload = Buffer.from('hello');
    const channel = client.open(payload);
    payload.fill(0x78);
    await clock.elapseAsync(1);
    const next = await arriving;
    if (next.done === true) throw new Error('no channel');
    expect(text(channel.openPayload)).toBe('hello');
    expect(text(next.value.openPayload)).toBe('hello');
    void client.close();
    void server.close();
    await clock.elapseAsync(60_000);
  });

  test.fails('a PING payload passed as a Buffer still matches its PONG once the Buffer is reused', async () => {
    const clock = new ManualClock();
    const { client, server } = await connectionPair(clock);
    const payload = Buffer.from('ping');
    let answered = false;
    client.ping(payload).then(
      () => {
        answered = true;
      },
      () => {},
    );
    payload.fill(0);
    await clock.elapseAsync(10);
    expect(answered).toBe(true);
    void client.close();
    void server.close();
    await clock.elapseAsync(60_000);
  });

  test.fails('the in-memory provider copies a Buffer written while the stream window is full', async () => {
    const clock = new ManualClock();
    const network = new MemoryQuicNetwork({ clock });
    const [a, b] = MemoryQuicSession.pair({ network, acceptor: { streamWindow: 4 } });
    const stream = await a.openStream();
    const data = Buffer.from('abcdefgh');
    const writing = stream.write(data);
    writing.catch(() => {});
    // The first four bytes went within the window; the rest waits for credit.
    data.fill(0x30);
    const incoming = b.incomingStreams[Symbol.asyncIterator]();
    const arriving = incoming.next();
    await clock.elapseAsync(1);
    const next = await arriving;
    if (next.done === true) throw new Error('no stream');
    const received: Uint8Array[] = [];
    for (let total = 0; total < 8; ) {
      const reading = next.value.read(8);
      await clock.elapseAsync(1);
      const chunk = await reading;
      if (chunk === undefined) break;
      received.push(chunk.slice());
      total += chunk.length;
    }
    expect(text(Buffer.concat(received))).toBe('abcdefgh');
    a.lose();
    await clock.elapseAsync(1);
  });
});
