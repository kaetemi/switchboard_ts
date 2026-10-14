// The negative test vectors of the wiki page "Polyverse Switchboard QUIC
// and WebTransport Binding", section "Test vectors", that depend on
// connection state: a port of the connection half of `switchboard_quic`'s
// `test/vectors_test.dart`, each vector written by a raw peer into a
// connection of the binding. The codecs byte for byte (and the exporter
// equal at both ends of a session) are in `quic-records.test.ts`.
import { describe, expect, test } from 'vitest';
import { hexBytes, hexString } from '../../src/core/bytes.ts';
import type { MuxChannel, MuxConnection } from '../../src/core/mux/contract.ts';
import { MuxControlMessage, MuxControlType } from '../../src/core/mux/mux-frame.ts';
import type { QuicStream } from '../../src/core/quic/contract.ts';
import { quicHelloType } from '../../src/core/quic/hello.ts';
import { StatusCode } from '../../src/core/status.ts';
import { bindingAndRaw, expectStatus, within } from '../unit/quic/harness.ts';

/** Takes every channel [connection] delivers, handing each to [onChannel] (Dart's `incoming.listen`). */
function acceptAll(connection: MuxConnection, onChannel: (channel: MuxChannel) => void): void {
  void (async () => {
    for await (const channel of connection.incoming) {
      onChannel(channel);
    }
  })();
}

/** Reads [channel] to its end, dropping what arrives (Dart's `stream.listen((_) {})`). */
function drainChannel(channel: MuxChannel): void {
  void (async () => {
    const reader = channel.readable.getReader();
    for (;;) {
      const next = await reader.read();
      if (next.done) {
        return;
      }
    }
  })().catch(() => {});
}

describe('records', () => {
  describe('negative (protocol error)', () => {
    // Through a connection: each ends it with PROTOCOL_ERROR.
    for (const [name, vector] of [
      ['command 3', '06 00 00 40 02 00 00 00 00 00 00 00 00 C0'],
      ['CLOSE with a one-byte body', '06 00 00 40 02 00 00 00 00 00 01 00 00 80 00'],
      ['a stream whose first record is DATA', '00 00 00 00'],
      ['OPEN with a truncated channel id', '05 00 00 40 02 00 00 00 00'],
      ['OPEN of reserved channel 1', '06 00 00 40 01 00 00 00 00 00'],
      ['OPEN of reserved channel 0xFFFF', '06 00 00 40 FF FF 00 00 00 00'],
      ['a record after a CLOSE record', '06 00 00 40 02 00 00 00 00 00 00 00 00 80 00 00 00 00'],
      ['a second OPEN record', '06 00 00 40 02 00 00 00 00 00 06 00 00 40 04 00 00 00 00 00'],
    ] as const) {
      test(`${name} ends the connection`, async () => {
        const [connection, raw] = await bindingAndRaw();
        acceptAll(connection, () => {});
        const stream = await raw.openRaw(hexBytes(vector));
        if (name === 'a record after a CLOSE record') {
          await stream.finish();
        }
        expectStatus(await within(connection.done), StatusCode.protocolError);
        const end = await raw.end;
        expect(end.code).toBe(StatusCode.protocolError);
        expect(end.byPeer).toBe(true);
      });
    }

    test('a channel id of the wrong parity, a second channel-0 OPEN, and a FIN without a CLOSE record end the connection', async () => {
      for (const [vector, fin] of [
        ['06 00 00 40 03 00 00 00 00 00', false],
        ['06 00 00 40 00 00 00 00 00 00', false],
        ['06 00 00 40 02 00 00 00 00 00 01 00 00 00 07', true],
      ] as const) {
        const [connection, raw] = await bindingAndRaw();
        // Read, so that the binding reads on to the FIN.
        acceptAll(connection, drainChannel);
        const stream = await raw.openRaw(hexBytes(vector));
        if (fin) {
          await stream.finish();
        }
        expectStatus(await within(connection.done), StatusCode.protocolError);
      }
    });

    test('channel 0 on a stream the acceptor opened ends the connection', async () => {
      const [connection, raw] = await bindingAndRaw({ bindingIsInitiator: true });
      await raw.openRaw(hexBytes('06 00 00 40 00 00 00 00 00 00'));
      expectStatus(await within(connection.done), StatusCode.protocolError);
    });
  });
});

describe('control stream', () => {
  describe('negative (protocol error)', () => {
    for (const [name, vector] of [
      ['a HELLO of 8 bytes', '09 00 00 00 08 F0 7F 00 00 00 00 00 00'],
      ['a HELLO with a record limit of 1024', '0A 00 00 00 08 00 04 00 00 00 00 00 00 00'],
      ['an empty control record', '00 00 00 00'],
      ['a control message before HELLO', '03 00 00 00 01 DE AD'],
    ] as const) {
      test(`${name} as the first message ends the connection`, async () => {
        const [connection, raw] = await bindingAndRaw({ rawControl: false });
        await raw.openControl({ sendHello: false });
        await (raw.control as QuicStream).write(hexBytes(vector));
        expectStatus(await within(connection.done), StatusCode.protocolError);
        expect((await raw.end).code).toBe(StatusCode.protocolError);
      });
    }

    for (const [name, vector] of [
      ['LIMITS', '0D 00 00 00 04 00 04 00 00 04 00 00 00 00 00 01 00'],
      ['CREDIT', '0B 00 00 00 07 02 00 00 00 00 00 00 80 00 00'],
      ['an empty control record', '00 00 00 00'],
      ['a second HELLO', '0A 00 00 00 08 F0 7F 00 00 00 00 00 00 00'],
      ['a CLOSE record on the control stream', '00 00 00 80'],
      ['a control payload over 1024 bytes', `02 04 00 00 01 ${Array(1025).fill('00').join(' ')}`],
    ] as const) {
      test(`${name} after HELLO ends the connection`, async () => {
        const [connection, raw] = await bindingAndRaw();
        await (raw.control as QuicStream).write(hexBytes(vector));
        expectStatus(await within(connection.done), StatusCode.protocolError);
      });
    }

    test('FIN, a reset or STOP_SENDING on the control stream ends the connection', async () => {
      for (const action of ['fin', 'reset', 'stop'] as const) {
        const [connection, raw] = await bindingAndRaw();
        // The binding has the control stream: its HELLO came back.
        expect((await raw.nextControl()).type).toBe(quicHelloType);
        const control = raw.control as QuicStream;
        switch (action) {
          case 'fin':
            await control.finish();
            break;
          case 'reset':
            control.reset(0);
            break;
          case 'stop':
            control.stop(0);
            break;
        }
        const status = await within(connection.done);
        expect(status.code, `${action}: ${status}`).toBe(StatusCode.protocolError);
      }
    });

    test('an unknown control type is ignored', async () => {
      const [connection, raw] = await bindingAndRaw();
      expect((await raw.nextControl()).type).toBe(quicHelloType);
      await raw.sendControl(new MuxControlMessage(0x7e, new Uint8Array(3)));
      await raw.sendControl(MuxControlMessage.ping(hexBytes('DE AD')));
      const pong = await raw.nextControl();
      expect(pong.type).toBe(MuxControlType.pong);
      expect(hexString(pong.payload)).toBe('DE AD');
      expect(connection.isOpen).toBe(true);
    });
  });
});
