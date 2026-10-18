/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Review finding (native provider, docs/notes/review-quic.md): a write copies what the stack has
// not taken yet so that the caller may reuse its buffer at once (`Stream.write` in
// src/node/quic/native-provider.ts), but the copy is `w.bytes.slice(w.offset)`, and a Node
// Buffer's `slice` is a view on the same memory: a Buffer reused while the write waits for credit
// goes out changed. The case is `test.fails` until the copy is real (for example
// `Uint8Array.prototype.slice.call(w.bytes, w.offset)`); the test of the same name in
// quic-native.test.ts writes a Uint8Array, which `slice` does copy.
//
// Without the addon the file is skipped, as the other native files are; with
// SWITCHBOARD_NATIVE=required a missing addon fails it instead.

import { Buffer } from 'node:buffer';
import { describe, expect, test } from 'vitest';
import type { QuicStream } from '../../src/core/quic/contract.ts';
import {
  type NativeQuicSession,
  nativeQuic,
  nativeQuicAvailability,
  selfSignedCertificate,
} from '../../src/node/quic/index.ts';

const availability = nativeQuicAvailability();
if (!availability.available && process.env['SWITCHBOARD_NATIVE'] === 'required') {
  throw new Error(`SWITCHBOARD_NATIVE=required, but ${availability.reason}`);
}

describe.skipIf(!availability.available)(
  `review: the native provider's write copies (${availability.available ? availability.path : availability.reason})`,
  () => {
    test.fails('a Node Buffer written while the stream window is full may be reused at once, as a Uint8Array may', async () => {
      const provider = nativeQuic();
      const certificate = selfSignedCertificate('127.0.0.1');
      const listener = await provider.listen('127.0.0.1', 0, {
        alpn: ['switchboard'],
        certificateDer: certificate.certificateDer,
        privateKeyDer: certificate.privateKeyDer,
        streamReceiveWindow: 64 * 1024,
      });
      const sessions = listener.sessions[Symbol.asyncIterator]();
      const client = await provider.connect(listener.endpoint, {
        alpn: ['switchboard'],
        verifyCertificate: false,
      });
      try {
        const server = (await sessions.next()).value as NativeQuicSession;
        const streams = server.incomingStreams[Symbol.asyncIterator]();
        const stream = await client.openStream();
        // Sixteen times the stream window: most of it waits for credit.
        const data = new Uint8Array(1 << 20);
        for (let i = 0; i < data.length; i++) data[i] = (i * 7 + 3) & 0xff;
        const buffer = Buffer.from(data);
        const writing = stream.write(buffer);
        buffer.fill(0);
        const peer = (await streams.next()).value as QuicStream;
        const chunks: Uint8Array[] = [];
        const reading = (async () => {
          for (;;) {
            const chunk = await peer.read(1 << 16);
            if (chunk === undefined) return;
            chunks.push(chunk);
          }
        })();
        await writing;
        await stream.finish();
        await reading;
        const received = Buffer.concat(chunks);
        expect(received.length).toBe(data.length);
        let differing = 0;
        for (let i = 0; i < data.length; i++) if (received[i] !== data[i]) differing++;
        expect(differing).toBe(0);
        await server.close(0, '');
      } finally {
        await client.close(0, '');
        await listener.close();
      }
    });
  },
);
