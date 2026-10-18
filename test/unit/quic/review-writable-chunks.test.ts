// Review finding (QUIC binding, docs/notes/review-quic.md): a channel's `writable` takes
// whatever is written to it. The frame mux refuses a chunk that is not a Uint8Array with a
// TypeError (the writable's `write` in frame-mux-channel.ts); QuicBindingChannel's writable hands
// it to `send()` unchecked (quic-binding-channel.ts, the constructor), so a string goes out as a
// DATA record of zero bytes, one per UTF-16 code unit, and an ArrayBuffer leaves an output item
// whose record header cannot be written: every later `send()`, and `close()`, then throws a
// RangeError and the channel can neither send nor close. Both cases are `test.fails` until the
// binding refuses such chunks as the frame mux does; the fix turns them into plain tests.
// In-memory provider, virtual time.
import { describe, expect, test } from 'vitest';
import type { MuxChannel, MuxOptions } from '../../../src/core/mux/contract.ts';
import { QuicBindingConnection } from '../../../src/core/quic/quic-binding-connection.ts';
import { ManualClock } from '../../../src/testing/manual-clock.ts';
import { MemoryQuicNetwork, MemoryQuicSession } from '../../../src/testing/memory-quic-provider.ts';

const text = (data: Uint8Array): string => new TextDecoder().decode(data);
const bytes = (value: string): Uint8Array => new TextEncoder().encode(value);

/** A connected pair over the in-memory provider on [clock], and a channel the client opened, as both ends see it. */
async function channelPair(clock: ManualClock): Promise<{
  client: QuicBindingConnection;
  server: QuicBindingConnection;
  local: MuxChannel;
  remote: MuxChannel;
}> {
  const network = new MemoryQuicNetwork({ clock });
  const [a, b] = MemoryQuicSession.pair({ network });
  const options: Partial<MuxOptions> = { clock, keepAliveInterval: undefined };
  const accepting = QuicBindingConnection.start(b, { options });
  const dialling = QuicBindingConnection.start(a, { options });
  await clock.elapseAsync(0);
  const client = await dialling;
  const server = await accepting;
  const arriving = server.incoming[Symbol.asyncIterator]().next();
  const local = client.open(new Uint8Array(0));
  await clock.elapseAsync(1);
  const next = await arriving;
  if (next.done === true) throw new Error('no channel');
  return { client, server, local, remote: next.value };
}

/** What [action] throws synchronously, or undefined; a promise it returns is marked handled. */
function thrown(action: () => unknown): unknown {
  try {
    const result = action();
    if (result instanceof Promise) result.catch(() => {});
  } catch (error) {
    return error;
  }
  return undefined;
}

describe('review: QuicBindingChannel.writable takes Uint8Array chunks only', () => {
  test.fails('a string written to the writable is refused with a TypeError, not sent as zero bytes', async () => {
    const clock = new ManualClock();
    const { client, server, local, remote } = await channelPair(clock);
    const writer = local.writable.getWriter();
    let refused: unknown;
    writer.write('hi' as unknown as Uint8Array).catch((error: unknown) => {
      refused = error;
    });
    await clock.elapseAsync(1);
    writer.releaseLock();
    // The channel goes on: the next subframe is the first one the peer reads.
    await local.send(bytes('ok'));
    const reader = remote.readable.getReader();
    const reading = reader.read();
    await clock.elapseAsync(1);
    const first = await reading;
    expect(refused).toBeInstanceOf(TypeError);
    expect(first.done).toBe(false);
    expect(text(first.value as Uint8Array)).toBe('ok');
    void client.close();
    void server.close();
    await clock.elapseAsync(60_000);
  });

  test.fails('an ArrayBuffer written to the writable is refused, and the channel can still send and close', async () => {
    const clock = new ManualClock();
    const { client, server, local, remote } = await channelPair(clock);
    const writer = local.writable.getWriter();
    let refused: unknown;
    writer.write(new ArrayBuffer(4) as unknown as Uint8Array).catch((error: unknown) => {
      refused = error;
    });
    await clock.elapseAsync(1);
    writer.releaseLock();
    expect(refused).toBeInstanceOf(TypeError);
    // Today every later send and the close throw the RangeError of the record header.
    expect(thrown(() => local.send(bytes('ok')))).toBeUndefined();
    const reader = remote.readable.getReader();
    const reading = reader.read();
    await clock.elapseAsync(1);
    expect(text((await reading).value as Uint8Array)).toBe('ok');
    expect(thrown(() => local.close())).toBeUndefined();
    void client.close();
    void server.close();
    await clock.elapseAsync(60_000);
  });
});
