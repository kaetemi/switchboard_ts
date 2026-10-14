/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Raw byte conformance: a scripted peer sends hand-built frames to a
// server's TCP and WebSocket listeners and checks the reaction byte for byte.
// Each script runs against every server implementation; on the Dart server
// it pins the expected reaction to the reference.

import { afterAll, beforeAll, describe, expect, test } from 'vitest';
import { concatBytes, hexString } from '../../../src/core/bytes.ts';
import { StatusCode } from '../../../src/core/status.ts';
import { wikiBytes, wikiVectorsIn } from '../../support/vectors.ts';
import type { Peer } from '../launcher.ts';
import { tsTransportGaps } from '../matrix.ts';
import { startPeer, unavailable } from '../peers.ts';
import type { Implementation } from '../protocol.ts';
import {
  frame,
  le,
  name8,
  preamble,
  type RawConnection,
  type Received,
  rawTcp,
  rawWebSocket,
  status,
  utf8,
} from './raw.ts';

const servers: readonly Implementation[] = ['dart', 'ts'];
const control = { ping: 1, pong: 2, goAway: 3, limits: 4, credit: 7 } as const;
const maxFrameSize = 4096;

function skipReason(server: Implementation): string | undefined {
  const missing = unavailable(server);
  if (missing !== undefined) {
    return `${server} peer: ${missing}`;
  }
  return server === 'ts' ? tsTransportGaps.tcp : undefined;
}

/** A Talk request: flags, procedure, request id, payload. */
function request(procedure: string, id: number, payload: Uint8Array, window?: number): Uint8Array {
  return window === undefined
    ? concatBytes([0x03], name8(procedure), le(id, 3), payload)
    : concatBytes([0x0b], name8(procedure), le(id, 3), le(window, 4), payload);
}

/** The next frame that is not a keep-alive PING. */
function nextFrame(c: RawConnection, timeoutMs = 5_000): Promise<Received> {
  return c.nextMatching((f) => f.control !== control.ping, timeoutMs);
}

/** Expects GOAWAY with [code], then the end of the connection. */
async function expectGoAway(c: RawConnection, code: number): Promise<void> {
  const f = await c.nextMatching((x) => x.control === control.goAway);
  expect(hexString(f.payload.subarray(1, 3))).toBe(hexString(le(code, 2)));
  await c.ended;
}

for (const server of servers) {
  const reason = skipReason(server);
  describe.skipIf(reason !== undefined)(
    `raw conformance against the ${server} server${reason === undefined ? '' : ` (skipped: ${reason})`}`,
    () => {
      let peer: Peer;
      beforeAll(async () => {
        peer = await startPeer(server, {
          config: { listen: ['tcp', 'ws'], mux: { maxFrameSize, keepAliveIntervalMs: 0 } },
        });
      });
      afterAll(async () => {
        await peer?.stop();
      });

      for (const transport of ['tcp', 'ws'] as const) {
        const open = async (): Promise<RawConnection> => {
          const c =
            transport === 'tcp'
              ? await rawTcp(peer.endpoint('tcp'))
              : await rawWebSocket(peer.endpoint('ws'));
          // Every connection starts with the server's LIMITS.
          const limits = await c.next();
          expect(limits.control).toBe(control.limits);
          c.send(
            frame.control(control.limits, concatBytes(le(1 << 20, 4), le(0, 4), le(65536, 4))),
          );
          return c;
        };

        describe(transport, () => {
          test('the server announces its limits first', async () => {
            const c =
              transport === 'tcp'
                ? await rawTcp(peer.endpoint('tcp'))
                : await rawWebSocket(peer.endpoint('ws'));
            const limits = await c.next();
            expect(limits.channel).toBe(0);
            expect(limits.hex).toBe(
              hexString(
                concatBytes([control.limits], le(maxFrameSize, 4), le(65536, 4), le(65536, 4)),
              ),
            );
            c.end();
          });

          test('a Talk exchange on an echo channel, byte for byte', async () => {
            const c = await open();
            c.send(frame.open(2, concatBytes([0x01], name8('echo'))));
            c.send(frame.data(2, request('ECHO', 1, utf8('hi'))));
            expect((await nextFrame(c)).hex).toBe('04 01 00 00 68 69');
            c.send(frame.data(2, request('COUNT', 2, utf8('2'), 0xffffffff)));
            const items = [await nextFrame(c), await nextFrame(c), await nextFrame(c)];
            expect(items.map((f) => f.hex)).toEqual([
              '14 02 00 00 31',
              '14 02 00 00 32',
              '04 02 00 00 64 6F 6E 65',
            ]);
            c.send(frame.data(2, request('ABORT', 3, utf8('5 nope'))));
            expect((await nextFrame(c)).hex).toBe('24 03 00 00 05 00 6E 6F 70 65');
            c.send(frame.close(2));
            const close = await nextFrame(c);
            expect([close.command, close.channel]).toEqual(['close', 2]);
            c.end();
          });

          test('PING is answered with a PONG carrying the same payload', async () => {
            const c = await open();
            c.send(frame.control(control.ping, utf8('probe')));
            const pong = await c.nextMatching((f) => f.control === control.pong);
            expect(pong.hex).toBe(hexString(concatBytes([control.pong], utf8('probe'))));
            c.end();
          });

          test('an OPEN for a type nobody serves is closed with NOT_FOUND', async () => {
            const c = await open();
            c.send(frame.open(2, concatBytes([0x01], name8('nobody'))));
            const close = await nextFrame(c);
            expect([close.command, close.channel]).toEqual(['close', 2]);
            expect(hexString(close.payload.subarray(0, 2))).toBe(
              hexString(le(StatusCode.notFound, 2)),
            );
            c.end();
          });

          test('a frame over the announced maximum is FRAME_TOO_LARGE', async () => {
            const c = await open();
            c.send(frame.open(2, concatBytes([0x01], name8('raw'))));
            c.send(frame.data(2, new Uint8Array(maxFrameSize + 1)));
            if (transport === 'ws' && server === 'ts') {
              // A deviation of the TS WebSocket listener (acceptWebSocket in
              // src/node/transport/ws-server.ts): the `ws` package refuses a
              // message over maxPayload itself and closes with 1009 before
              // the mux hears of it, so no GOAWAY precedes the close.
              expect(await c.ended).toEqual({ code: 1009 });
              return;
            }
            await expectGoAway(c, StatusCode.frameTooLarge);
            if (transport === 'ws') {
              expect(await c.ended).toEqual({ code: 1009 });
            }
          });

          test('DATA beyond the channel window is a protocol error', async () => {
            const c = await open();
            // A sink channel is never read, so no credit comes back. Each
            // DATA costs its payload plus 16 bytes against the 64 KiB window.
            c.send(frame.open(2, concatBytes([0x01], name8('sink'))));
            for (let cost = 0; cost <= 65536; cost += 4000 + 16) {
              c.send(frame.data(2, new Uint8Array(4000)));
            }
            await expectGoAway(c, StatusCode.protocolError);
          });

          test('DATA up to the channel window is accepted', async () => {
            const c = await open();
            c.send(frame.open(2, concatBytes([0x01], name8('sink'))));
            for (let i = 0; i < 16; i++) {
              c.send(frame.data(2, new Uint8Array(4096 - 16)));
            }
            c.send(frame.control(control.ping, utf8('after')));
            const pong = await c.nextMatching((f) => f.control === control.pong);
            expect(pong.hex).toBe(hexString(concatBytes([control.pong], utf8('after'))));
            c.end();
          });

          test('after GOAWAY from the client, the server closes once no channel is left', async () => {
            const c = await open();
            c.send(frame.open(2, concatBytes([0x01], name8('echo'))));
            c.send(frame.control(control.goAway, status(StatusCode.goingAway)));
            // The open channel is still served.
            c.send(frame.data(2, request('ECHO', 1, utf8('still'))));
            expect((await nextFrame(c)).hex).toBe(
              hexString(concatBytes([0x04, 0x01, 0x00, 0x00], utf8('still'))),
            );
            c.send(frame.close(2));
            const close = await nextFrame(c);
            expect([close.command, close.channel]).toEqual(['close', 2]);
            await c.ended;
          });

          test('after GOAWAY from the server, an OPEN is refused with GOING_AWAY', async () => {
            const c = await open();
            c.send(frame.open(2, concatBytes([0x01], name8('echo'))));
            c.send(frame.data(2, request('GOAWAY', 1, new Uint8Array(0))));
            const goAway = await c.nextMatching((f) => f.control === control.goAway);
            expect(hexString(goAway.payload.subarray(1, 3))).toBe(
              hexString(le(StatusCode.goingAway, 2)),
            );
            c.send(frame.open(4, concatBytes([0x01], name8('echo'))));
            const refused = await c.nextMatching((f) => f.command === 'close' && f.channel === 4);
            expect(hexString(refused.payload.subarray(0, 2))).toBe(
              hexString(le(StatusCode.goingAway, 2)),
            );
            c.end();
          });

          // The negative mux vectors of the wiki, each on a live connection.
          test.each(
            wikiVectorsIn('test-vectors/mux-frames/').filter((v) => v.negative && !v.prose),
          )('negative vector "$case" is a protocol error', async (v) => {
            const c = await open();
            c.send(wikiBytes(v.id));
            await expectGoAway(c, StatusCode.protocolError);
          });
        });
      }

      describe('a server taking two channels', () => {
        let small: Peer;
        beforeAll(async () => {
          small = await startPeer(server, {
            config: { listen: ['tcp'], mux: { maxChannels: 2, keepAliveIntervalMs: 0 } },
          });
        });
        afterAll(async () => {
          await small?.stop();
        });

        test('a third OPEN is refused with RESOURCE_EXHAUSTED, the others served', async () => {
          const c = await rawTcp(small.endpoint('tcp'));
          const limits = await c.next();
          expect(hexString(limits.payload.subarray(5, 9))).toBe(hexString(le(2, 4)));
          for (const id of [2, 4, 6]) {
            c.send(frame.open(id, concatBytes([0x01], name8('echo'))));
          }
          const refused = await c.nextMatching((f) => f.command === 'close');
          expect([refused.channel, hexString(refused.payload.subarray(0, 2))]).toEqual([
            6,
            hexString(le(StatusCode.resourceExhausted, 2)),
          ]);
          c.send(frame.close(6));
          c.send(frame.data(4, request('ECHO', 1, utf8('4'))));
          expect((await nextFrame(c)).hex).toBe('04 01 00 00 34');
          c.end();
        });
      });

      describe('tcp only', () => {
        test('the server sends the preamble, version 1', async () => {
          const c = await rawTcp(peer.endpoint('tcp'));
          expect(hexString(await c.serverPreamble)).toBe(hexString(preamble));
          c.end();
        });

        test('a preamble of another version ends the connection', async () => {
          const c = await rawTcp(peer.endpoint('tcp'), {
            preamble: Uint8Array.of(0x53, 0x57, 0x42, 0x44, 0x02, 0x00, 0x00, 0x00),
          });
          await c.ended;
        });

        test('bytes that are no preamble end the connection', async () => {
          const c = await rawTcp(peer.endpoint('tcp'), { preamble: utf8('GET / HT') });
          await c.ended;
        });
      });

      describe('ws only', () => {
        test('the server selects the switchboard subprotocol', async () => {
          const c = await rawWebSocket(peer.endpoint('ws'));
          expect(c.protocol).toBe('switchboard');
          c.end();
        });
      });
    },
  );
}
