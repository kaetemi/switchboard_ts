/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Port of the Dart `test/integration/embedded_test.dart`: the minimal
// round trip of the wiki page "Polyverse Switchboard Embedded Profile",
// byte for byte, between a raw TCP socket (a device without a Switchboard)
// and a Node node. Over TCP only, as in Dart: the point is the bytes on a
// socket, which the in-process transport does not carry.

import { createServer, type Socket, connect as tcpConnect } from 'node:net';
import { describe, expect, test } from 'vitest';
import { hexBytes, hexString } from '../../src/core/bytes.ts';
import type { MuxOptions } from '../../src/core/mux/contract.ts';
import { Name } from '../../src/core/name.ts';
import { TalkChannel } from '../../src/core/talk/index.ts';
import { delay } from './mesh-support.ts';
import { node, onCleanup, type Switchboard } from './node-support.ts';

const preamble = '53 57 42 44 01 00 00 00';
// transport length 12, mux OPEN ch 2: 01 'd' 'e' 'v' 00 00 00 00 00
const open = '0C 00 00 00  12 02 00  01 64 65 76 00 00 00 00 00';
// transport length 15, mux DATA ch 2, talk request 1 "PING"
const request = '0F 00 00 00  02 02 00  03 50 49 4E 47 00 00 00 00 01 00 00';
// transport length 7, mux DATA ch 2, talk final response to 1
const reply = '07 00 00 00 02 02 00 04 01 00 00';
// transport length 3, mux CLOSE ch 2 (status absent: OK)
const close = '03 00 00 00 22 02 00';

/** The bytes a socket receives, taken in order. */
class ByteQueue {
  readonly #chunks: Uint8Array[] = [];
  #length = 0;
  #ended = false;
  #waiters: (() => void)[] = [];

  constructor(socket: Socket) {
    socket.on('data', (data: Buffer) => {
      this.#chunks.push(new Uint8Array(data));
      this.#length += data.length;
      this.#wake();
    });
    const end = (): void => {
      this.#ended = true;
      this.#wake();
    };
    socket.on('end', end);
    socket.on('close', end);
    socket.on('error', end);
  }

  #wake(): void {
    for (const waiter of this.#waiters.splice(0)) {
      waiter();
    }
  }

  #wait(): Promise<void> {
    return new Promise((resolve) => this.#waiters.push(resolve));
  }

  /** The next [count] bytes; rejects if the socket ends first. */
  async take(count: number): Promise<Uint8Array> {
    while (this.#length < count) {
      if (this.#ended) {
        throw new Error(`ended with ${this.#length} of ${count} bytes`);
      }
      await this.#wait();
    }
    const out = new Uint8Array(count);
    let filled = 0;
    while (filled < count) {
      const chunk = this.#chunks[0] as Uint8Array;
      const used = Math.min(chunk.length, count - filled);
      out.set(chunk.subarray(0, used), filled);
      filled += used;
      if (used === chunk.length) {
        this.#chunks.shift();
      } else {
        this.#chunks[0] = chunk.subarray(used);
      }
    }
    this.#length -= count;
    return out;
  }

  /** Whether more bytes come: true once one is there, false once the socket ended without. */
  async hasNext(): Promise<boolean> {
    while (this.#length === 0 && !this.#ended) {
      await this.#wait();
    }
    return this.#length > 0;
  }

  /** One transport frame (length prefix and body). */
  async frame(): Promise<Uint8Array> {
    const prefix = await this.take(4);
    const length = new DataView(prefix.buffer, prefix.byteOffset).getUint32(0, true);
    const body = await this.take(length);
    const out = new Uint8Array(4 + length);
    out.set(prefix);
    out.set(body, 4);
    return out;
  }
}

/** A node serving `dev` (an empty reply to every request) on TCP, and a raw socket connected to it. */
async function start(
  options: Partial<MuxOptions>,
): Promise<{ endpoint: Switchboard; socket: Socket; input: ByteQueue }> {
  const endpoint = node({ muxOptions: options });
  endpoint.registerService(Name.of('dev'), (incoming) => {
    void incoming.talk().serve((message) => {
      if (message.expectsReply) {
        void message.reply();
      }
    });
  });
  const uri = new URL(await endpoint.listenTcp('127.0.0.1', 0));
  const socket = tcpConnect({ host: uri.hostname, port: Number(uri.port) });
  socket.setNoDelay(true);
  onCleanup(() => socket.destroy());
  const input = new ByteQueue(socket);
  await new Promise<void>((resolve, reject) => {
    socket.once('connect', resolve);
    socket.once('error', reject);
  });
  return { endpoint, socket, input };
}

describe('the embedded profile over TCP', () => {
  test('exact bytes, endpoint announcing no limits', async () => {
    const { socket, input } = await start({ announceLimits: false, keepAliveInterval: undefined });
    socket.write(hexBytes(`${preamble} ${open} ${request}`));
    expect(hexString(await input.take(8))).toBe(preamble);
    expect(hexString(await input.take(11))).toBe(reply);
    // The device closes the channel; the endpoint confirms.
    socket.write(hexBytes(close));
    expect(hexString(await input.take(7))).toBe(close);
    // Then the device hangs up.
    socket.end();
    expect(await input.hasNext()).toBe(false);
  });

  test('a device announcing a 256 byte window is held to it from the first channel on', async () => {
    // The device listens; the node connects and talks at once, before the
    // device's LIMITS can have arrived.
    const device = createServer();
    const accepted = new Promise<Socket>((resolve) => device.once('connection', resolve));
    await new Promise<void>((resolve) => device.listen(0, '127.0.0.1', resolve));
    onCleanup(() => new Promise((resolve) => device.close(resolve)));
    const address = device.address();
    const port = typeof address === 'object' && address !== null ? address.port : 0;
    const n = node({ muxOptions: { keepAliveInterval: undefined } });
    const connection = await n.connect(`tcp://127.0.0.1:${port}`);
    const talk = new TalkChannel(connection.open(hexBytes('01 64 65 76 00 00 00 00 00')));
    for (let i = 0; i < 3; i++) {
      void talk.send('PUT', new Uint8Array(100)).catch(() => {});
    }
    const peer = await accepted;
    peer.setNoDelay(true);
    onCleanup(() => peer.destroy());
    const bytes = new ByteQueue(peer);

    expect(hexString(await bytes.take(8))).toBe(preamble);
    // The node's LIMITS, then the OPEN; no DATA yet.
    expect(hexString((await bytes.frame()).subarray(4))).toMatch(/^02 00 00 04/);
    expect(hexString((await bytes.frame()).subarray(4))).toMatch(/^12 02 00/);
    await delay(50);
    // The device announces: no frame limit, 4 channels, a 256 byte window.
    peer.write(hexBytes(`${preamble} 10 00 00 00 02 00 00 04 00 00 00 00 04 00 00 00 00 01 00 00`));
    // Each message costs 109 + 16 bytes: two fit the window, the third
    // waits for the device's credit.
    let used = 0;
    for (let i = 0; i < 2; i++) {
      const data = (await bytes.frame()).subarray(4);
      expect([...data.subarray(0, 3)]).toEqual([0x02, 0x02, 0x00]);
      used += data.length - 3 + 16;
    }
    expect(used).toBeLessThanOrEqual(256);
    const third = await Promise.race([bytes.hasNext(), delay(100).then(() => 'timeout')]);
    expect(third).toBe('timeout');
    // CREDIT for channel 2: the third message goes.
    peer.write(
      hexBytes(
        `0E 00 00 00 02 00 00 07 02 00 00 00 00 00 ${hexString([used & 0xff, used >> 8, 0, 0])}`,
      ),
    );
    const last = (await bytes.frame()).subarray(4);
    expect([...last.subarray(0, 3)]).toEqual([0x02, 0x02, 0x00]);
    expect(last.length - 3).toBe(109);
  });

  test('a device skipping the LIMITS control frame', async () => {
    const { socket, input } = await start({ keepAliveInterval: undefined });
    // Bytes may arrive split anywhere.
    for (const byte of hexBytes(`${preamble} ${open} ${request}`)) {
      socket.write(Uint8Array.of(byte));
    }
    expect(hexString(await input.take(8))).toBe(preamble);
    // LIMITS on the control channel: 02 00 00 | 04 | u32 | u32 | u32.
    expect(hexString(await input.frame())).toMatch(/^10 00 00 00 02 00 00 04/);
    expect(hexString(await input.frame())).toBe(reply);
    socket.end();
    expect(await input.hasNext()).toBe(false);
  });
});
