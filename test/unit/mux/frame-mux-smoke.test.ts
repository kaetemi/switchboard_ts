// A first end-to-end check of the frame mux over the memory pair: open,
// subframes both ways, credit, close, ping, GOAWAY and connection loss.
// The ports of the Dart mux tests (mux_test.dart and the others) follow.
import { describe, expect, test } from 'vitest';
import { utf8Decode, utf8Encode } from '../../../src/core/bytes.ts';
import type { MuxChannel } from '../../../src/core/mux/contract.ts';
import { FrameMuxConnection } from '../../../src/core/mux/frame-mux-connection.ts';
import { Status, StatusCode } from '../../../src/core/status.ts';
import { memoryTransportPair } from '../../../src/core/transport/memory-transport.ts';

const quiet = { keepAliveInterval: undefined, goAwayGrace: 2000 } as const;

function pair(): [FrameMuxConnection, FrameMuxConnection] {
  const [a, b] = memoryTransportPair();
  return [
    new FrameMuxConnection(a, { isInitiator: true, ...quiet }),
    new FrameMuxConnection(b, { isInitiator: false, ...quiet }),
  ];
}

async function nextIncoming(connection: FrameMuxConnection): Promise<MuxChannel> {
  const iterator = connection.incoming[Symbol.asyncIterator]();
  const next = await iterator.next();
  if (next.done) {
    throw new Error('no incoming channel');
  }
  return next.value;
}

describe('frame mux over the memory pair', () => {
  test('a channel carries subframes both ways and closes mutually', async () => {
    const [a, b] = pair();
    const channel = a.open(utf8Encode('hello'));
    expect(channel.id).toBe(2);
    await channel.send(utf8Encode('one'));
    const incoming = await nextIncoming(b);
    expect(utf8Decode(incoming.openPayload)).toBe('hello');
    const reader = incoming.readable.getReader();
    expect(utf8Decode((await reader.read()).value as Uint8Array)).toBe('one');
    await incoming.send(utf8Encode('two'));
    const back = channel.readable.getReader();
    expect(utf8Decode((await back.read()).value as Uint8Array)).toBe('two');
    await channel.close();
    expect((await reader.read()).done).toBe(true);
    expect((await back.read()).done).toBe(true);
    expect((await channel.done).isOk).toBe(true);
    expect((await incoming.done).isOk).toBe(true);
    expect(a.openChannelCount).toBe(0);
    expect(b.openChannelCount).toBe(0);
    await a.close();
    expect((await a.done).known).toBe(StatusCode.connectionLost);
    expect((await b.done).known).toBe(StatusCode.connectionLost);
  });

  test('a sender waits for credit while the reader does not read', async () => {
    const [a, b] = pair();
    const channel = a.open(new Uint8Array(0));
    const chunk = new Uint8Array(channel.maxSubframeLength);
    await channel.send(chunk);
    // 65536 - 2 * 32768 leaves no room for a third frame of this size.
    const second = channel.send(chunk);
    await second;
    let sent = false;
    const third = channel.send(chunk).then(() => {
      sent = true;
    });
    const incoming = await nextIncoming(b);
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(sent).toBe(false);
    const reader = incoming.readable.getReader();
    await reader.read();
    await reader.read();
    await third;
    expect(sent).toBe(true);
    await a.close();
  });

  test('ping, GOAWAY and a CLOSE with a status', async () => {
    const [a, b] = pair();
    expect(await a.ping()).toBeGreaterThanOrEqual(0);
    const channel = a.open(utf8Encode('x'));
    const incoming = await nextIncoming(b);
    void incoming.close(Status.of(StatusCode.notFound, 'nobody here'));
    const status = await channel.done;
    expect(status.known).toBe(StatusCode.notFound);
    expect(status.reason).toBe('nobody here');
    await b.goAway();
    expect((await a.done).known).toBe(StatusCode.goingAway);
    expect(a.peerGoingAway).toBe(true);
  });

  test('the writable sends and closes', async () => {
    const [a, b] = pair();
    const channel = a.open(utf8Encode('w'));
    const writer = channel.writable.getWriter();
    await writer.write(utf8Encode('via the writable'));
    const incoming = await nextIncoming(b);
    const reader = incoming.readable.getReader();
    expect(utf8Decode((await reader.read()).value as Uint8Array)).toBe('via the writable');
    const closed = writer.close();
    expect((await reader.read()).done).toBe(true);
    await closed;
    await a.close();
  });
});
