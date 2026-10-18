/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Connection limits and liveness across the pairings (mux_test.dart,
// mux_limits_test.dart): PING payloads, the frame limit a server announces
// reaching the client's Talk layer, the channel limit, keep-alive between
// live peers, and many channels at once.

import { expect, test } from 'vitest';
import { hexString } from '../../../src/core/bytes.ts';
import { StatusCode } from '../../../src/core/status.ts';
import { describeCells } from '../matrix.ts';
import type { MessageInfo } from '../protocol.ts';
import { connect, hex, openTalk, pattern, text } from './support.ts';

describeCells('mux', {}, (peers) => {
  test('PING payloads up to 125 bytes are answered; a longer one is refused before sending', async () => {
    const connection = await connect(peers);
    for (const n of [0, 1, 125]) {
      const { rttMs } = await peers.client.rpc<{ rttMs: number }>('connection.ping', {
        connection,
        payload: hexString(pattern(n)),
      });
      expect(rttMs).toBeGreaterThanOrEqual(0);
    }
    const error = await peers.client.rpcError('connection.ping', {
      connection,
      payload: hexString(pattern(126)),
    });
    expect(error.kind).toBe('argument');
  });

  test('64 channels at once on one connection, three times over', async () => {
    const connection = await connect(peers);
    for (let round = 0; round < 3; round++) {
      const channels = await Promise.all(
        Array.from({ length: 64 }, () => openTalk(peers, 'echo', connection)),
      );
      const replies = await Promise.all(
        channels.map(({ talk }, i) =>
          peers.client.rpc<MessageInfo>('talk.request', {
            talk,
            procedure: 'ECHO',
            text: `${round}.${i}`,
          }),
        ),
      );
      expect(replies.map((r) => r.payload)).toEqual(channels.map((_, i) => hex(`${round}.${i}`)));
      await Promise.all(channels.map(({ talk }) => peers.client.rpc('talk.close', { talk })));
    }
  });
});

describeCells(
  'mux with a small frame limit and two channels at the server',
  {
    transports: ['tcp', 'ws'],
    server: { mux: { maxFrameSize: 4096, maxChannels: 2 } },
  },
  (peers) => {
    test('the announced frame limit bounds what the client sends inline', async () => {
      // The largest subframe: the frame limit less a short channel header.
      const inline = 4096 - 3 - 12;
      const { talk } = await openTalk(peers);
      for (const [n, how] of [
        [inline, 'inline'],
        [inline + 1, 'bulk'],
      ] as const) {
        const reply = await peers.client.rpc<MessageInfo>('talk.request', {
          talk,
          procedure: 'HOW',
          patternLength: n,
        });
        expect(text(reply.payload)).toBe(`${how} ${n}`);
      }
    });

    test('the client opens no more channels than the server takes', async () => {
      const connection = await connect(peers);
      // The server's LIMITS is its first frame: it has arrived with the PONG.
      await peers.client.rpc('connection.ping', { connection });
      await openTalk(peers, 'echo', connection);
      await openTalk(peers, 'echo', connection);
      const refused = await peers.client.rpcError('open', {
        connection,
        address: { type: 'echo' },
      });
      expect(refused).toMatchObject({ kind: 'status', code: StatusCode.resourceExhausted });
    });
  },
);

describeCells(
  'mux with fast keep-alive',
  {
    server: { mux: { keepAliveIntervalMs: 100, keepAliveTimeoutMs: 300 } },
    client: { mux: { keepAliveIntervalMs: 100, keepAliveTimeoutMs: 300 } },
  },
  (peers) => {
    test('live peers keep an idle connection open', async () => {
      const { talk, connection } = await openTalk(peers);
      await new Promise((resolve) => setTimeout(resolve, 1000));
      expect(peers.client.events.filter({ event: 'connection.closed', connection })).toEqual([]);
      const reply = await peers.client.rpc<MessageInfo>('talk.request', {
        talk,
        procedure: 'ECHO',
        text: 'alive',
      });
      expect(reply.payload).toBe(hex('alive'));
    });
  },
);
