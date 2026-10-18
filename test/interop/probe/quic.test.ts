/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// The QUIC probe, the conformance probe's counterpart for the QUIC binding
// (scenario 24 of docs/notes/interop.md): a scripted peer dials each
// server's raw QUIC and WebTransport listeners through the native provider
// and writes and reads the binding's records by hand (probe/quic.ts): the
// control stream and HELLO, OPEN, DATA and CLOSE records, rejections with
// STOP_SENDING, resets, protocol errors, GOAWAY, application closes and
// WT_DRAIN_SESSION. It then listens itself and lets each peer dial it, to
// read the client's records. Each script runs against every implementation;
// against the Dart peer it pins the expected reaction to the reference.
//
// None of this goes through a peer command that names records: those
// commands drive an implementation's API, which only ever writes valid
// records in its own order. A raw probe is the one way to put malformed,
// early, late or reset records on the wire, and to see the bytes, codes and
// stream signals an implementation answers with.

import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, test } from 'vitest';
import { bytesEqual, concatBytes, hexBytes, hexString } from '../../../src/core/bytes.ts';
import { StatusCode } from '../../../src/core/status.ts';
import { NativeQuicProvider } from '../../../src/node/quic/index.ts';
import { wikiBytes, wikiVectorsIn } from '../../support/vectors.ts';
import type { Peer } from '../launcher.ts';
import { tsTransportGaps } from '../matrix.ts';
import { startPeer, unavailable } from '../peers.ts';
import type { Implementation, PeerConfig, PeerEvent } from '../protocol.ts';
import {
  control,
  type DialOptions,
  defaultRecordLimit,
  dial,
  exporterFlag,
  hello,
  ProbeListener,
  type ProbeSession,
  type ReceivedRecord,
  type RecordStream,
  record,
  recordCommand,
  recordHeader,
  within,
} from './quic.ts';
import { le, name8, utf8 } from './raw.ts';

const servers: readonly Implementation[] = ['dart', 'ts'];
const quicTransports = ['quic', 'webtransport'] as const;
type QuicTransport = (typeof quicTransports)[number];

/** The channels a listener's HELLO states: its stream limit (128 by default) less the control stream. */
const listenerChannels = 127;

/** Why the probe cannot run against [server]: a missing peer, or no native QUIC here. */
function skipReason(server: Implementation): string | undefined {
  const missing = unavailable(server);
  if (missing !== undefined) {
    return `${server} peer: ${missing}`;
  }
  // The probe itself runs on the native provider, whichever the server.
  const gap = tsTransportGaps.quic;
  return gap === undefined ? undefined : `the probe needs ${gap}`;
}

/** The open payload of a channel to [type]'s default instance. */
function address(type: string): Uint8Array {
  return concatBytes([0x01], name8(type));
}

/** A Talk request: flags, procedure, request id, payload. */
function request(procedure: string, id: number, payload: Uint8Array, window?: number): Uint8Array {
  return window === undefined
    ? concatBytes([0x03], name8(procedure), le(id, 3), payload)
    : concatBytes([0x0b], name8(procedure), le(id, 3), le(window, 4), payload);
}

/** A status payload: code and UTF-8 reason. */
function status(code: number, reason = ''): Uint8Array {
  return concatBytes(le(code, 2), utf8(reason));
}

/** A control record as hex. */
function controlHex(type: number, payload: Uint8Array = new Uint8Array(0)): string {
  return hexString(record.control(type, payload));
}

/** The `BIG` pattern of [n] bytes: byte i is `(i * 31) & 0xFF`. */
function big(n: number): Uint8Array {
  return Uint8Array.from({ length: n }, (_, i) => (i * 31) & 0xff);
}

/** The status code at the start of a CLOSE record's body; undefined for an empty body (`OK`). */
function closeCode(r: ReceivedRecord): number | undefined {
  return r.body.length < 2 ? undefined : (r.body[0] ?? 0) + (r.body[1] ?? 0) * 256;
}

/** How a session's end compares: the application code and who closed. */
interface Ending {
  readonly code: number | undefined;
  readonly byPeer: boolean;
  readonly lost: boolean;
}

/** Expects the far side to have closed [s] with the application code [code]. */
async function expectClosedBy(s: ProbeSession, code: number, timeoutMs = 5_000): Promise<void> {
  const end = await s.ended(timeoutMs);
  expect<Ending>({ code: end.code, byPeer: end.byPeer, lost: end.lost }).toEqual({
    code,
    byPeer: true,
    lost: false,
  });
}

/** Expects the session of [c] to be open still: a PING on its control stream is answered. */
async function expectAlive(c: RecordStream, payload: string): Promise<void> {
  await c.write(record.control(control.ping, utf8(payload)));
  const pong = await c.recordMatching((r) => r.type === control.pong);
  expect(pong.hex).toBe(controlHex(control.pong, utf8(payload)));
}

/**
 * Confirms the peer's close of [ch]: a CLOSE record and FIN, or, when the
 * peer stopped this side's direction already, a reset with [code].
 */
async function confirmClose(ch: RecordStream, code = 0): Promise<void> {
  try {
    await ch.end(record.close());
  } catch {
    ch.reset(code);
  }
}

/** An event with the handles of a raw channel. */
type ChannelEvent = PeerEvent & { readonly channel: string; readonly connection: string };

for (const server of servers) {
  const reason = skipReason(server);
  describe.skipIf(reason !== undefined)(
    `QUIC probe against the ${server} peer${reason === undefined ? '' : ` (skipped: ${reason})`}`,
    () => {
      /** The probe sessions of the test, closed after it. */
      const sessions: ProbeSession[] = [];

      /**
       * Starts a peer with [config] for the suite around the call; it is
       * reset after each test, which fails on a `peer.error`.
       */
      const usePeer = (config: PeerConfig): (() => Peer) => {
        let peer: Peer | undefined;
        beforeAll(async () => {
          peer = await startPeer(server, { config });
        });
        beforeEach(({ onTestFailed }) => {
          onTestFailed(() => {
            if (peer !== undefined) {
              console.error(peer.diagnostics());
            }
          });
        });
        afterEach(async () => {
          await Promise.all(sessions.splice(0).map((s) => s.close(0, '').catch(() => {})));
          if (peer !== undefined) {
            const errors = peer.errors;
            await peer.reset();
            expect(errors, 'errors the peer reported outside any command').toEqual([]);
          }
        });
        afterAll(async () => {
          await peer?.stop();
        });
        return () => {
          if (peer === undefined) {
            throw new Error('the peer starts in beforeAll; use it inside tests');
          }
          return peer;
        };
      };

      /** A probe session to [peer]'s [transport] listener, closed after the test. */
      const connect = async (
        peer: Peer,
        transport: QuicTransport,
        options?: DialOptions,
      ): Promise<ProbeSession> => {
        const sha = peer.ready.certificateSha256;
        if (sha === undefined) {
          throw new Error('the peer names no certificate');
        }
        const s = await dial(peer.endpoint(transport), sha, options);
        sessions.push(s);
        return s;
      };

      /** The application close code of status [code] on [transport], by the peer's oracle. */
      const wireCode = async (
        peer: Peer,
        transport: QuicTransport,
        code: number,
      ): Promise<number> => {
        const v = await peer.rpc<{ raw: number | null; back: number }>('decode', {
          kind: 'quicCode',
          hex: hexString(le(code, 2)),
        });
        // Raw QUIC carries the status as the error code; WebTransport as the
        // HTTP/3 code of the WebTransport code, which the provider reads back.
        return transport === 'quic' ? (v.raw ?? -1) : v.back;
      };

      describe('the probe dials the peer', () => {
        const peerOf = usePeer({
          listen: ['quic', 'webtransport'],
          mux: { keepAliveIntervalMs: 0, closeConfirmTimeoutMs: 500 },
        });

        for (const transport of quicTransports) {
          describe(transport, () => {
            /** A session with its control stream open and the server's HELLO read. */
            const open = async (
              helloPayload: Uint8Array | null = hello(),
              options?: DialOptions,
            ): Promise<{ s: ProbeSession; c: RecordStream; serverHello: ReceivedRecord }> => {
              const s = await connect(peerOf(), transport, options);
              const c = await s.openControl(helloPayload);
              const serverHello = await c.record();
              return { s, c, serverHello };
            };

            /** Expects the server to close [s] with PROTOCOL_ERROR. */
            const expectProtocolError = async (s: ProbeSession): Promise<void> => {
              await expectClosedBy(
                s,
                await wireCode(peerOf(), transport, StatusCode.protocolError),
              );
            };

            /** A raw channel the server accepted: its stream, and the peer's handles. */
            const rawChannel = async (
              s: ProbeSession,
              id = 2,
              ...more: Uint8Array[]
            ): Promise<{ ch: RecordStream; channel: string; connection: string }> => {
              const ch = await s.open(record.open(id, address('raw')), ...more);
              const e = await peerOf().events.waitFor<ChannelEvent>({
                event: 'channel.incoming',
                id,
              });
              return { ch, channel: e.channel, connection: e.connection };
            };

            describe('control stream and HELLO', () => {
              test('the server answers the control stream with its HELLO first, byte for byte', async () => {
                const { c, serverHello } = await open();
                // The default record limit, the channels the listener takes
                // (its stream limit less the control stream), and EXPORTER:
                // both stacks expose the exporter, so both MUST set it.
                expect(serverHello.hex).toBe(
                  controlHex(
                    control.hello,
                    hello(defaultRecordLimit, listenerChannels, exporterFlag),
                  ),
                );
                await c.write(
                  wikiBytes('quic-binding/test-vectors/control-stream/ping-payload-de-ad'),
                );
                expect((await c.record()).hex).toBe(
                  hexString(
                    wikiBytes('quic-binding/test-vectors/control-stream/pong-payload-de-ad'),
                  ),
                );
              });

              test("the server sends its HELLO without waiting for the probe's", async () => {
                const { c, serverHello } = await open(null);
                expect(serverHello.type).toBe(control.hello);
                await c.write(record.control(control.hello, hello()));
                await expectAlive(c, 'late hello');
              });

              test('a HELLO with a further field is accepted, the field ignored', async () => {
                const s = await connect(peerOf(), transport);
                const c = await s.open(
                  record.open(0),
                  wikiBytes(
                    'quic-binding/test-vectors/control-stream/hello-with-a-further-field-aa-ignored',
                  ),
                );
                expect((await c.record()).type).toBe(control.hello);
                await expectAlive(c, 'further');
              });

              // The wiki's negative control vectors: a HELLO in place of the
              // probe's, the others after a valid one.
              test.each(
                wikiVectorsIn('quic-binding/test-vectors/control-stream/').filter(
                  (v) => v.negative,
                ),
              )('$case is a protocol error', async (v) => {
                const bytes = wikiBytes(v.id);
                const { s, c } = await open(bytes[4] === control.hello ? null : hello());
                await c.write(bytes);
                await expectProtocolError(s);
              });

              test('a control message before HELLO is a protocol error', async () => {
                const { s, c } = await open(null);
                await c.write(record.control(control.ping, utf8('first')));
                await expectProtocolError(s);
              });

              test('a second HELLO is a protocol error', async () => {
                const { s, c } = await open();
                await c.write(record.control(control.hello, hello()));
                await expectProtocolError(s);
              });

              test('a second OPEN record on the control stream is a protocol error', async () => {
                const { s, c } = await open();
                await c.write(record.open(0));
                await expectProtocolError(s);
              });

              test('a second channel-0 OPEN on another stream is a protocol error', async () => {
                const { s, c } = await open();
                await expectAlive(c, 'first');
                await s.open(record.open(0), record.control(control.hello, hello()));
                await expectProtocolError(s);
              });

              test('a CLOSE record on the control stream is a protocol error', async () => {
                const { s, c } = await open();
                await c.write(record.close());
                await expectProtocolError(s);
              });

              test('FIN on the control stream is a protocol error', async () => {
                const { s, c } = await open();
                await c.end();
                await expectProtocolError(s);
              });

              test('a reset of the control stream with a code is a protocol error', async () => {
                const { s, c } = await open();
                c.reset(StatusCode.cancelled);
                await expectProtocolError(s);
              });

              test('STOP_SENDING on the control stream with a code is a protocol error', async () => {
                const { s, c } = await open();
                c.stop(StatusCode.cancelled);
                await expectProtocolError(s);
              });
            });

            describe('channels and records', () => {
              test('a Talk exchange on an echo channel, record by record, closed both ways', async () => {
                const { s } = await open();
                const ch = await s.open(
                  record.open(2, address('echo')),
                  record.data(request('ECHO', 1, utf8('hi'))),
                );
                expect((await ch.record()).hex).toBe('06 00 00 00 04 01 00 00 68 69');
                await ch.write(record.data(request('COUNT', 2, utf8('2'), 0xffffffff)));
                const items = [await ch.record(), await ch.record(), await ch.record()];
                expect(items.map((r) => r.hex)).toEqual([
                  '05 00 00 00 14 02 00 00 31',
                  '05 00 00 00 14 02 00 00 32',
                  '08 00 00 00 04 02 00 00 64 6F 6E 65',
                ]);
                await ch.write(record.data(request('ABORT', 3, utf8('5 nope'))));
                expect((await ch.record()).hex).toBe('0A 00 00 00 24 03 00 00 05 00 6E 6F 70 65');
                // The probe closes OK; the server confirms with a CLOSE
                // record without a status payload, then FIN.
                await ch.end(record.close());
                const { records, end } = await ch.drain();
                expect(records.map((r) => r.hex)).toEqual([
                  hexString(
                    wikiBytes('quic-binding/test-vectors/records/close-no-status-payload-ok'),
                  ),
                ]);
                expect(end).toEqual({ kind: 'fin', partial: '' });
              });

              test('a channel opened before the control stream is served under the defaults', async () => {
                const s = await connect(peerOf(), transport);
                // A subframe of the default record limit exactly, both ways
                // within it.
                const payload = big(defaultRecordLimit - 12);
                const ch = await s.open(
                  record.open(2, address('echo')),
                  record.data(request('ECHO', 1, payload)),
                );
                const reply = await ch.record();
                expect(hexString(reply.bytes.subarray(0, 8))).toBe(
                  hexString(
                    concatBytes(recordHeader(recordCommand.data, 4 + payload.length), [4, 1, 0, 0]),
                  ),
                );
                expect(bytesEqual(reply.body.subarray(4), payload)).toBe(true);
                const c = await s.openControl();
                expect((await c.record()).type).toBe(control.hello);
              });

              test('a record header over the record limit is FRAME_TOO_LARGE, its body never sent', async () => {
                const { s } = await open();
                await s.open(
                  record.open(2, address('echo')),
                  recordHeader(recordCommand.data, defaultRecordLimit + 1),
                );
                await expectClosedBy(
                  s,
                  await wireCode(peerOf(), transport, StatusCode.frameTooLarge),
                );
              });

              test("the server sends a reply inline up to the probe's record limit", async () => {
                const { s, c } = await open(hello(1 << 20));
                // A PING answered after the HELLO: the HELLO has taken effect.
                await expectAlive(c, 'after the hello');
                const ch = await s.open(
                  record.open(2, address('echo')),
                  record.data(request('BIG', 1, utf8('40000'))),
                );
                const reply = await ch.record();
                expect(hexString(reply.bytes.subarray(0, 8))).toBe(
                  hexString(concatBytes(recordHeader(recordCommand.data, 4 + 40000), [4, 1, 0, 0])),
                );
                expect(bytesEqual(reply.body.subarray(4), big(40000))).toBe(true);
              });

              test("a reply over the probe's record limit comes on a bulk channel, the server's first odd id", async () => {
                const { s } = await open();
                const ch = await s.open(
                  record.open(2, address('echo')),
                  record.data(request('BIG', 1, utf8('40000'))),
                );
                const bulk = await s.incoming();
                expect((await bulk.record()).hex).toBe(
                  hexString(
                    wikiBytes(
                      'quic-binding/test-vectors/bulk-channels/open-channel-3-bulk-for-parent-channel-2-bulk-number-1',
                    ),
                  ),
                );
                // The final response with BULK: bulk number 1, 40000 bytes.
                expect((await ch.record()).bodyHex).toBe(
                  hexString(concatBytes([0x44, 1, 0, 0], le(1, 4), le(40000, 6))),
                );
                const { records, end } = await bulk.drain();
                const data = records.slice(0, -1);
                expect(
                  data.every((r) => r.command === 'data' && r.body.length <= defaultRecordLimit),
                ).toBe(true);
                expect(bytesEqual(concatBytes(...data.map((r) => r.body)), big(40000))).toBe(true);
                expect(records.at(-1)?.hex).toBe('00 00 00 80');
                expect(end).toEqual({ kind: 'fin', partial: '' });
                await bulk.end(record.close());
              });

              // The wiki's negative record vectors on a channel's stream: an
              // OPEN record as the stream's first bytes, the others after a
              // valid OPEN. The one given in words follows.
              test.each(
                wikiVectorsIn('quic-binding/test-vectors/records/').filter(
                  (v) => v.negative && v.hex !== null,
                ),
              )('$case is a protocol error', async (v) => {
                const bytes = wikiBytes(v.id);
                const first = (bytes[3] ?? 0) >> 6 === recordCommand.open;
                const { s } = await open();
                await s.open(...(first ? [] : [record.open(2, address('raw'))]), bytes);
                await expectProtocolError(s);
              });

              test('a stream whose first record is DATA is a protocol error', async () => {
                const { s } = await open();
                await s.open(record.data());
                await expectProtocolError(s);
              });

              test("an OPEN of the acceptor's parity is a protocol error", async () => {
                const { s } = await open();
                await s.open(record.open(3, address('raw')));
                await expectProtocolError(s);
              });

              test('an OPEN of a channel that is open is a protocol error', async () => {
                const { s } = await open();
                await rawChannel(s, 2);
                await s.open(record.open(2, address('raw')));
                await expectProtocolError(s);
              });

              test('FIN before the OPEN record is complete is a protocol error', async () => {
                const { s } = await open();
                const ch = await s.open(record.open(2, address('raw')).subarray(0, 8));
                await ch.end();
                await expectProtocolError(s);
              });

              test('FIN without a CLOSE record is a protocol error', async () => {
                const { s } = await open();
                const { ch } = await rawChannel(s, 2, record.data(utf8('x')));
                await ch.end();
                await expectProtocolError(s);
              });

              test('FIN inside a record is a protocol error', async () => {
                const { s } = await open();
                const { ch } = await rawChannel(s, 2);
                await ch.end(hexBytes('05 00 00 00 01 02'));
                await expectProtocolError(s);
              });

              test('a stream reset before its OPEN record is complete is ignored', async () => {
                const { s, c } = await open();
                const ch = await s.open(record.open(2, address('raw')).subarray(0, 8));
                // A round trip on the control stream: the partial OPEN,
                // written before the PING, has had its time to arrive.
                await expectAlive(c, 'partial');
                ch.reset(StatusCode.cancelled);
                await expectAlive(c, 'after the reset');
                expect(peerOf().events.filter('channel.incoming')).toEqual([]);
              });
            });

            describe('closes, rejections and resets', () => {
              test('an OPEN for a type nobody serves is rejected: a CLOSE record with NOT_FOUND, FIN and STOP_SENDING', async () => {
                const { s } = await open();
                const ch = await s.open(
                  record.open(2, address('nobody')),
                  record.data(request('ECHO', 1, utf8('x'))),
                );
                const { records, end } = await ch.drain();
                expect(records.map((r) => [r.command, closeCode(r)])).toEqual([
                  ['close', StatusCode.notFound],
                ]);
                expect(end).toEqual({ kind: 'fin', partial: '' });
                expect(await ch.stopped()).toEqual({ kind: 'stop', code: StatusCode.notFound });
                ch.reset(StatusCode.notFound);
              });

              test("the probe's CLOSE record reaches the service with its status, and the server confirms OK", async () => {
                const { s } = await open();
                const { ch, channel } = await rawChannel(s, 2, record.data(utf8('a')));
                await peerOf().events.waitFor({ event: 'channel.data', channel, hex: '61' });
                await ch.end(
                  wikiBytes('quic-binding/test-vectors/records/close-not-found-reason-x'),
                );
                expect(
                  await peerOf().events.waitFor({ event: 'channel.closed', channel }),
                ).toMatchObject({ code: StatusCode.notFound, reason: 'x' });
                const { records, end } = await ch.drain();
                expect(records.map((r) => r.hex)).toEqual(['00 00 00 80']);
                expect(end).toEqual({ kind: 'fin', partial: '' });
              });

              test('the fields of MOVED travel in the CLOSE record', async () => {
                const { s } = await open();
                const { ch, channel } = await rawChannel(s, 2);
                await ch.end(
                  wikiBytes('quic-binding/test-vectors/records/close-moved-owner-0x1a2b-epoch-7'),
                );
                expect(
                  await peerOf().events.waitFor({ event: 'channel.closed', channel }),
                ).toMatchObject({ code: StatusCode.moved, body: '2B 1A 00 00 00 00 07 00 00 00' });
                expect((await ch.drain()).records.map((r) => r.hex)).toEqual(['00 00 00 80']);
              });

              test("the server's CLOSE record carries the status payload byte for byte", async () => {
                const { s } = await open();
                const cases = [
                  {
                    id: 2,
                    args: { code: StatusCode.notFound, reason: 'x' },
                    expected: wikiBytes(
                      'quic-binding/test-vectors/records/close-not-found-reason-x',
                    ),
                  },
                  {
                    id: 4,
                    args: { statusHex: '25 00 2B 1A 00 00 00 00 07 00 00 00' },
                    expected: wikiBytes(
                      'quic-binding/test-vectors/records/close-moved-owner-0x1a2b-epoch-7',
                    ),
                  },
                  {
                    id: 6,
                    args: { code: StatusCode.ok },
                    expected: wikiBytes(
                      'quic-binding/test-vectors/records/close-no-status-payload-ok',
                    ),
                  },
                ];
                for (const { id, args, expected } of cases) {
                  const { ch, channel } = await rawChannel(s, id);
                  const closing = peerOf().rpc('channel.close', { channel, ...args });
                  const { records, end } = await ch.drain();
                  expect(records.map((r) => r.hex)).toEqual([hexString(expected)]);
                  expect(end).toEqual({ kind: 'fin', partial: '' });
                  const code = closeCode(records[0] as ReceivedRecord) ?? StatusCode.ok;
                  await confirmClose(ch, code);
                  // A failure close MAY come with STOP_SENDING, with its own
                  // status (both implementations send it); an OK close whose
                  // incoming data is still delivered does not.
                  expect(
                    code === StatusCode.ok
                      ? [{ kind: 'acked' }]
                      : [{ kind: 'stop', code }, { kind: 'acked' }],
                  ).toContainEqual(await ch.stopped());
                  await closing;
                  expect(
                    await peerOf().events.waitFor({ event: 'channel.closed', channel }),
                  ).toMatchObject({ code });
                }
              });

              test("a reset is the probe's close by its code: the partial record is discarded, the server confirms", async () => {
                const { s } = await open();
                const { ch, channel } = await rawChannel(
                  s,
                  2,
                  record.data(utf8('whole')),
                  hexBytes('05 00 00 00 01 02'),
                );
                await peerOf().events.waitFor({
                  event: 'channel.data',
                  channel,
                  hex: hexString(utf8('whole')),
                });
                ch.reset(StatusCode.permissionDenied);
                expect(
                  await peerOf().events.waitFor({ event: 'channel.closed', channel }),
                ).toMatchObject({ code: StatusCode.permissionDenied });
                expect(peerOf().events.filter({ event: 'channel.data', channel })).toHaveLength(1);
                const { records, end } = await ch.drain();
                expect(records.map((r) => r.hex)).toEqual(['00 00 00 80']);
                expect(end).toEqual({ kind: 'fin', partial: '' });
              });

              test('a reset code above 255 reads as UNKNOWN', async () => {
                const { s } = await open();
                const { ch, channel } = await rawChannel(s, 2);
                ch.reset(300);
                expect(
                  await peerOf().events.waitFor({ event: 'channel.closed', channel }),
                ).toMatchObject({ code: StatusCode.unknown });
              });

              test("STOP_SENDING is the probe's close arriving early; the CLOSE record after it is the status reported", async () => {
                const { s } = await open();
                const { ch, channel } = await rawChannel(s, 2);
                ch.stop(StatusCode.notFound);
                await ch.end(record.close(status(StatusCode.notFound, 'gone')));
                expect(
                  await peerOf().events.waitFor({ event: 'channel.closed', channel }),
                ).toMatchObject({ code: StatusCode.notFound, reason: 'gone' });
              });

              test("the server's confirmation timeout: STOP_SENDING DEADLINE_EXCEEDED, the close OK since it went out", async () => {
                const { s } = await open();
                const { ch, channel } = await rawChannel(s, 2);
                const closing = peerOf().rpc('channel.close', { channel });
                const { records, end } = await ch.drain();
                expect(records.map((r) => r.hex)).toEqual(['00 00 00 80']);
                expect(end).toEqual({ kind: 'fin', partial: '' });
                // The probe never confirms; the peer's timeout is 500 ms.
                expect(await ch.stopped()).toEqual({
                  kind: 'stop',
                  code: StatusCode.deadlineExceeded,
                });
                await closing;
                expect(
                  await peerOf().events.waitFor({ event: 'channel.closed', channel }),
                ).toMatchObject({ code: StatusCode.ok });
                ch.reset(StatusCode.deadlineExceeded);
              });

              test('the server resets a direction whose CLOSE is stuck behind data the probe does not read', async () => {
                const { s } = await open(hello(), { streamReceiveWindow: 1000 });
                const { ch, channel } = await rawChannel(s, 2);
                const sending = peerOf()
                  .rpc('channel.send', { channel, hex: hexString(big(5000)) })
                  .catch(() => {});
                const closing = peerOf().rpc('channel.close', { channel });
                // Nothing is read until the peer's timeout (500 ms) passed.
                expect(await ch.stopped()).toEqual({
                  kind: 'stop',
                  code: StatusCode.deadlineExceeded,
                });
                const { records, end } = await ch.drain();
                expect(records.every((r) => r.command === 'data')).toBe(true);
                expect(end).toEqual({ kind: 'reset', code: StatusCode.deadlineExceeded });
                await Promise.all([sending, closing]);
                expect(
                  await peerOf().events.waitFor({ event: 'channel.closed', channel }),
                ).toMatchObject({ code: StatusCode.deadlineExceeded });
                ch.reset(StatusCode.deadlineExceeded);
              });
            });

            describe('GOAWAY, application close and drain', () => {
              test('GOAWAY from the server: its record, then OPENs refused GOING_AWAY with STOP_SENDING, the open channel served, then the close', async () => {
                const { s, c } = await open();
                const ch = await s.open(
                  record.open(2, address('echo')),
                  record.data(request('GOAWAY', 1, new Uint8Array(0))),
                );
                expect((await ch.record()).bodyHex).toBe(
                  hexString(concatBytes([4, 1, 0, 0], utf8('going'))),
                );
                const goAway = await c.recordMatching((r) => r.type === control.goAway);
                expect(goAway.hex).toBe(
                  hexString(
                    wikiBytes(
                      'quic-binding/test-vectors/control-stream/goaway-going-away-no-reason',
                    ),
                  ),
                );
                const late = await s.open(
                  record.open(4, address('echo')),
                  record.data(request('ECHO', 1, utf8('late'))),
                );
                const refused = await late.drain();
                expect(refused.records.map((r) => [r.command, closeCode(r)])).toEqual([
                  ['close', StatusCode.goingAway],
                ]);
                expect(refused.end).toEqual({ kind: 'fin', partial: '' });
                expect(await late.stopped()).toEqual({ kind: 'stop', code: StatusCode.goingAway });
                late.reset(StatusCode.goingAway);
                await ch.write(record.data(request('ECHO', 2, utf8('still'))));
                expect((await ch.record()).bodyHex).toBe(
                  hexString(concatBytes([4, 2, 0, 0], utf8('still'))),
                );
                // Read at once: the server closes the session once its
                // confirmation is acknowledged, and what the probe's stack
                // holds unread then is lost with it.
                const confirming = ch.drain();
                await ch.end(record.close());
                expect((await confirming).records.map((r) => r.hex)).toEqual(['00 00 00 80']);
                // Its channels done, the server closes, with code 0 after a
                // graceful drain.
                await expectClosedBy(s, await wireCode(peerOf(), transport, StatusCode.ok));
              });

              test('GOAWAY from the probe: the server reports its status, serves the open channel, and closes once it is done', async () => {
                const { s, c } = await open();
                const { ch, channel, connection } = await rawChannel(s, 2);
                await c.write(
                  record.control(control.goAway, status(StatusCode.unavailable, 'bye')),
                );
                expect(
                  await peerOf().events.waitFor({ event: 'connection.goAway', connection }),
                ).toMatchObject({ code: StatusCode.unavailable, reason: 'bye' });
                await ch.write(record.data(utf8('after')));
                await peerOf().events.waitFor({
                  event: 'channel.data',
                  channel,
                  hex: hexString(utf8('after')),
                });
                const confirming = ch.drain();
                await ch.end(record.close());
                expect((await confirming).records.map((r) => r.hex)).toEqual(['00 00 00 80']);
                await expectClosedBy(s, await wireCode(peerOf(), transport, StatusCode.ok));
              });

              test.each([
                { code: 0, why: '', goAway: { code: StatusCode.goingAway, reason: '' } },
                {
                  code: StatusCode.unavailable,
                  why: 'maintenance',
                  goAway: { code: StatusCode.unavailable, reason: 'maintenance' },
                },
                { code: 300, why: 'odd', goAway: { code: StatusCode.unknown } },
              ])(
                'an application close with code $code counts as a GOAWAY with its status',
                async ({ code, why, goAway }) => {
                  const { s } = await open();
                  const { channel, connection } = await rawChannel(s, 2);
                  await s.close(code, why);
                  expect(
                    await peerOf().events.waitFor({ event: 'connection.goAway', connection }),
                  ).toMatchObject(goAway);
                  expect(
                    await peerOf().events.waitFor({ event: 'connection.closed', connection }),
                  ).toMatchObject({ code: StatusCode.goingAway });
                  expect(
                    await peerOf().events.waitFor({ event: 'channel.closed', channel }),
                  ).toMatchObject({ code: StatusCode.goingAway });
                },
              );

              if (transport === 'webtransport') {
                test('WT_DRAIN_SESSION from the probe reads as GOAWAY GOING_AWAY; the server closes once the channel is done', async () => {
                  const { s } = await open();
                  const { ch, channel, connection } = await rawChannel(s, 2);
                  s.session.drain();
                  expect(
                    await peerOf().events.waitFor({ event: 'connection.goAway', connection }),
                  ).toMatchObject({ code: StatusCode.goingAway });
                  await ch.write(record.data(utf8('after')));
                  await peerOf().events.waitFor({
                    event: 'channel.data',
                    channel,
                    hex: hexString(utf8('after')),
                  });
                  const confirming = ch.drain();
                  await ch.end(record.close());
                  expect((await confirming).records.map((r) => r.hex)).toEqual(['00 00 00 80']);
                  await expectClosedBy(s, await wireCode(peerOf(), transport, StatusCode.ok));
                });
              }
            });
          });
        }
      });

      describe('the probe dials a peer taking two channels', () => {
        const peerOf = usePeer({
          listen: ['quic', 'webtransport'],
          mux: { keepAliveIntervalMs: 0, maxChannels: 2 },
        });

        for (const transport of quicTransports) {
          test(`${transport}: the HELLO states two channels; a third OPEN is refused RESOURCE_EXHAUSTED with STOP_SENDING`, async () => {
            const s = await connect(peerOf(), transport);
            const c = await s.openControl();
            expect((await c.record()).hex).toBe(
              controlHex(control.hello, hello(defaultRecordLimit, 2, exporterFlag)),
            );
            // Two channels, admitted once they answered.
            const admitted: RecordStream[] = [];
            for (const id of [2, 4]) {
              const ch = await s.open(
                record.open(id, address('echo')),
                record.data(request('ECHO', 1, utf8(`${id}`))),
              );
              expect((await ch.record()).bodyHex).toBe(
                hexString(concatBytes([4, 1, 0, 0], utf8(`${id}`))),
              );
              admitted.push(ch);
            }
            const third = await s.open(record.open(6, address('echo')));
            const { records, end } = await third.drain();
            expect(records.map((r) => [r.command, closeCode(r)])).toEqual([
              ['close', StatusCode.resourceExhausted],
            ]);
            expect(end).toEqual({ kind: 'fin', partial: '' });
            expect(await third.stopped()).toEqual({
              kind: 'stop',
              code: StatusCode.resourceExhausted,
            });
            third.reset(StatusCode.resourceExhausted);
            await admitted[1]?.write(record.data(request('ECHO', 2, utf8('on'))));
            expect((await admitted[1]?.record())?.bodyHex).toBe(
              hexString(concatBytes([4, 2, 0, 0], utf8('on'))),
            );
          });
        }
      });

      describe('the probe dials a peer with a keep-alive of 300 ms', () => {
        const peerOf = usePeer({
          listen: ['quic', 'webtransport'],
          mux: { keepAliveIntervalMs: 300, keepAliveTimeoutMs: 300 },
        });

        for (const transport of quicTransports) {
          test(`${transport}: a session without a control stream in time is closed PROTOCOL_ERROR`, async () => {
            const s = await connect(peerOf(), transport);
            await expectClosedBy(s, await wireCode(peerOf(), transport, StatusCode.protocolError));
          });

          test(`${transport}: a probe that answers no PING is closed DEADLINE_EXCEEDED`, async () => {
            const s = await connect(peerOf(), transport);
            const c = await s.openControl();
            expect((await c.record()).type).toBe(control.hello);
            // Keep-alive is local policy; the code is the reference's.
            await expectClosedBy(
              s,
              await wireCode(peerOf(), transport, StatusCode.deadlineExceeded),
            );
          });

          test(`${transport}: the keep-alive PING comes on the control stream, and answered keeps the session`, async () => {
            const s = await connect(peerOf(), transport);
            const c = await s.openControl();
            expect((await c.record()).type).toBe(control.hello);
            for (let i = 0; i < 3; i++) {
              const ping = await c.record(2_000);
              expect(ping.type).toBe(control.ping);
              await c.write(record.control(control.pong, ping.body.subarray(1)));
            }
            await expectAlive(c, 'kept');
          });
        }
      });

      describe('the peer dials the probe', () => {
        const peerOf = usePeer({
          listen: [],
          mux: { keepAliveIntervalMs: 0, closeConfirmTimeoutMs: 500 },
        });

        for (const transport of quicTransports) {
          describe(transport, () => {
            let listener: ProbeListener | undefined;
            beforeAll(async () => {
              listener = await ProbeListener.start(transport);
            });
            afterAll(async () => {
              await listener?.close();
            });

            /**
             * The peer dials the probe: the session, its control stream (its
             * OPEN record checked), the client's HELLO and the peer's
             * connection handle. The probe answers with its HELLO.
             */
            const accept = async (): Promise<{
              s: ProbeSession;
              c: RecordStream;
              connection: string;
              clientHello: ReceivedRecord;
            }> => {
              const l = listener;
              if (l === undefined) {
                throw new Error('the listener starts in beforeAll');
              }
              const connecting = peerOf().rpc<{ connection: string }>('connect', {
                endpoint: l.endpoint,
              });
              const s = await l.accept();
              sessions.push(s);
              const c = await s.incoming();
              expect((await c.record()).hex).toBe(
                hexString(
                  wikiBytes(
                    'quic-binding/test-vectors/records/open-channel-0-the-control-stream-empty-open-payload',
                  ),
                ),
              );
              const clientHello = await c.record();
              await c.write(record.control(control.hello, hello()));
              const { connection } = await connecting;
              return { s, c, connection, clientHello };
            };

            /** A raw channel the peer opens to the probe: its stream, OPEN record read, and handle. */
            const peerChannel = async (
              s: ProbeSession,
              connection: string,
            ): Promise<{
              ch: RecordStream;
              channel: string;
              id: number;
              opened: ReceivedRecord;
            }> => {
              const opening = peerOf().rpc<{ channel: string; id: number }>('open', {
                connection,
                address: { type: 'echo' },
              });
              const ch = await s.incoming();
              const opened = await ch.record();
              const { channel, id } = await opening;
              return { ch, channel, id, opened };
            };

            test('the client opens the control stream with OPEN of channel 0 and its HELLO', async () => {
              const { clientHello } = await accept();
              // A dialler grants its peer as many streams as it takes
              // channels (MuxOptions.maxChannels, 65536 by default) and
              // states that; EXPORTER, as both stacks expose the exporter.
              expect(clientHello.hex).toBe(
                controlHex(control.hello, hello(defaultRecordLimit, 65536, exporterFlag)),
              );
            });

            test("the client's OPEN, DATA and CLOSE records, byte for byte", async () => {
              const { s, connection } = await accept();
              const { ch, channel, id, opened } = await peerChannel(s, connection);
              expect(id).toBe(2);
              expect(opened.hex).toBe(hexString(record.open(2, address('echo'))));
              await peerOf().rpc('channel.send', { channel, hex: '01 02 03' });
              expect((await ch.record()).hex).toBe(
                hexString(wikiBytes('quic-binding/test-vectors/records/data-subframe-01-02-03')),
              );
              await peerOf().rpc('channel.send', { channel, hex: '' });
              expect((await ch.record()).hex).toBe(
                hexString(wikiBytes('quic-binding/test-vectors/records/data-empty-subframe')),
              );
              await ch.write(record.data(hexBytes('AA BB CC')));
              await peerOf().events.waitFor({ event: 'channel.data', channel, hex: 'AA BB CC' });
              const closing = peerOf().rpc('channel.close', {
                channel,
                code: StatusCode.notFound,
                reason: 'x',
              });
              const { records, end } = await ch.drain();
              expect(records.map((r) => r.hex)).toEqual([
                hexString(wikiBytes('quic-binding/test-vectors/records/close-not-found-reason-x')),
              ]);
              expect(end).toEqual({ kind: 'fin', partial: '' });
              await confirmClose(ch);
              await closing;
              expect(
                await peerOf().events.waitFor({ event: 'channel.closed', channel }),
              ).toMatchObject({ code: StatusCode.notFound, reason: 'x' });
              // The next channel takes the next even id.
              const next = await peerChannel(s, connection);
              expect([next.id, next.opened.channel]).toEqual([4, 4]);
            });

            test("the probe's rejection, a CLOSE record and STOP_SENDING: the client reports the record's status", async () => {
              const { s, connection } = await accept();
              const { ch, channel } = await peerChannel(s, connection);
              ch.stop(StatusCode.notFound);
              await ch.end(record.close(status(StatusCode.notFound, 'no such service')));
              expect(
                await peerOf().events.waitFor({ event: 'channel.closed', channel }),
              ).toMatchObject({ code: StatusCode.notFound, reason: 'no such service' });
            });

            test("the probe's reset reads as its code at the client, which confirms with a CLOSE record", async () => {
              const { s, connection } = await accept();
              const { ch, channel } = await peerChannel(s, connection);
              ch.reset(StatusCode.permissionDenied);
              expect(
                await peerOf().events.waitFor({ event: 'channel.closed', channel }),
              ).toMatchObject({ code: StatusCode.permissionDenied });
              const { records, end } = await ch.drain();
              expect(records.map((r) => r.hex)).toEqual(['00 00 00 80']);
              expect(end).toEqual({ kind: 'fin', partial: '' });
            });

            test("the client's PING, answered by the probe", async () => {
              const { c, connection } = await accept();
              const pinging = peerOf().rpc<{ rttMs: number }>('connection.ping', {
                connection,
                payload: 'DE AD',
              });
              expect((await c.recordMatching((r) => r.type === control.ping)).hex).toBe(
                hexString(wikiBytes('quic-binding/test-vectors/control-stream/ping-payload-de-ad')),
              );
              await c.write(
                wikiBytes('quic-binding/test-vectors/control-stream/pong-payload-de-ad'),
              );
              expect(typeof (await pinging).rttMs).toBe('number');
            });

            test("the client's GOAWAY, byte for byte; it closes with its status once its channel is done", async () => {
              const { s, c, connection } = await accept();
              // A channel open keeps the GOAWAY's sender waiting.
              const { ch, channel } = await peerChannel(s, connection);
              const going = peerOf().rpc('connection.goAway', {
                connection,
                code: StatusCode.unavailable,
                reason: 'x',
              });
              expect((await c.recordMatching((r) => r.type === control.goAway)).hex).toBe(
                controlHex(control.goAway, status(StatusCode.unavailable, 'x')),
              );
              const confirming = ch.drain();
              await ch.end(record.close());
              expect((await confirming).records.map((r) => r.hex)).toEqual(['00 00 00 80']);
              expect(
                await peerOf().events.waitFor({ event: 'channel.closed', channel }),
              ).toMatchObject({ code: StatusCode.ok });
              // Not GOING_AWAY: the close carries the GOAWAY's status, as both
              // implementations do (code 0 is for a graceful drain).
              await expectClosedBy(s, await wireCode(peerOf(), transport, StatusCode.unavailable));
              await going;
            });

            // The spec: an application close discards what is in flight, "so
            // a side closes only once the data it wrote on every stream has
            // been acknowledged". The Dart binding awaits only the directions
            // it finished, not the control stream, and closes a GOAWAY's
            // session at once when no channel is open: the GOAWAY is lost
            // about every other time. (TS has the same code path and has not
            // lost it here.)
            const dartGoAwayLost =
              server === 'dart'
                ? ' (skipped: Dart deviation: the session closes before its GOAWAY record is acknowledged, which loses it)'
                : '';
            test.skipIf(dartGoAwayLost !== '')(
              `with no channel open, the client's GOAWAY arrives before its close${dartGoAwayLost}`,
              async () => {
                const { s, c, connection } = await accept();
                const going = peerOf().rpc('connection.goAway', {
                  connection,
                  code: StatusCode.unavailable,
                  reason: 'x',
                });
                expect((await c.recordMatching((r) => r.type === control.goAway)).hex).toBe(
                  controlHex(control.goAway, status(StatusCode.unavailable, 'x')),
                );
                await expectClosedBy(
                  s,
                  await wireCode(peerOf(), transport, StatusCode.unavailable),
                );
                await going;
              },
            );

            test('an application close from the probe counts as a GOAWAY with its status at the client', async () => {
              const { s, connection } = await accept();
              await s.close(StatusCode.unavailable, 'maintenance');
              expect(
                await peerOf().events.waitFor({ event: 'connection.goAway', connection }),
              ).toMatchObject({ code: StatusCode.unavailable, reason: 'maintenance' });
              expect(
                await peerOf().events.waitFor({ event: 'connection.closed', connection }),
              ).toMatchObject({ code: StatusCode.goingAway });
            });

            if (transport === 'webtransport') {
              test('WT_DRAIN_SESSION from the probe reads as GOAWAY GOING_AWAY at the client, which closes once its channel is done', async () => {
                const { s, connection } = await accept();
                const { ch, channel } = await peerChannel(s, connection);
                s.session.drain();
                expect(
                  await peerOf().events.waitFor({ event: 'connection.goAway', connection }),
                ).toMatchObject({ code: StatusCode.goingAway });
                await ch.write(record.data(utf8('still')));
                await peerOf().events.waitFor({
                  event: 'channel.data',
                  channel,
                  hex: hexString(utf8('still')),
                });
                const closing = peerOf().rpc('channel.close', { channel });
                expect((await ch.drain()).records.map((r) => r.hex)).toEqual(['00 00 00 80']);
                await confirmClose(ch);
                await closing;
                await expectClosedBy(s, await wireCode(peerOf(), transport, StatusCode.ok));
              });

              test("the peer's connection.drain sends WT_DRAIN_SESSION to the probe", async () => {
                const { s, connection } = await accept();
                await peerOf().rpc('connection.drain', { connection });
                await within(s.session.draining, 5_000, 'no WT_DRAIN_SESSION');
              });
            }
          });
        }
      });
    },
  );
}

afterAll(() => NativeQuicProvider.shutdown());
