/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Message chain forwarding (forwardMessage, talk_forward_test.dart): the
// server's `fwd` service forwards every message to its own `echo` service
// over the cell's transport and relays everything back, so requests, items
// with their window, aborts byte for byte, EXTEND, bulk payloads and closes
// cross an intermediary of the server's implementation.

import { expect, test } from 'vitest';
import { hexString } from '../../../src/core/bytes.ts';
import { StatusCode } from '../../../src/core/status.ts';
import { wikiBytes } from '../../support/vectors.ts';
import { describeCells } from '../matrix.ts';
import type { MessageInfo, TalkItemEvent } from '../protocol.ts';
import { defaultServices, hex, openTalk, pattern, sha256 } from './support.ts';

describeCells(
  'forward',
  {
    server: {
      services: [
        ...defaultServices,
        { type: 'fwd', behaviour: 'forward', target: { type: 'echo', instance: 1 } },
      ],
    },
  },
  (peers) => {
    test('a request through the intermediary', async () => {
      const { talk } = await openTalk(peers, 'fwd');
      const reply = await peers.client.rpc<MessageInfo>('talk.request', {
        talk,
        procedure: 'UPPER',
        text: 'forwarded',
      });
      expect(reply).toEqual({ procedure: '', bulk: false, payload: hex('FORWARDED') });
    });

    test('stream items keep the requester window end to end', async () => {
      const { talk } = await openTalk(peers, 'fwd');
      const { stream } = await peers.client.rpc<{ stream: string }>('talk.stream', {
        talk,
        procedure: 'COUNT',
        text: '4',
        window: 2,
      });
      await peers.client.events.waitForCount({ event: 'talk.item', stream }, 2);
      await peers.client.rpc('talk.request', { talk, procedure: 'ECHO', text: 'barrier' });
      expect(peers.client.events.filter({ event: 'talk.item', stream })).toHaveLength(2);
      await peers.client.rpc('stream.grant', { stream, items: 2 });
      const done = await peers.client.events.waitFor({ event: 'talk.done', stream });
      const items = peers.client.events.filter<TalkItemEvent>({ event: 'talk.item', stream });
      expect(items.map((i) => i.payload)).toEqual(['1', '2', '3', '4'].map(hex));
      expect(done).toMatchObject({ items: 4, payload: hex('done') });
    });

    test('aborts pass through with their status, MOVED byte for byte', async () => {
      const { talk } = await openTalk(peers, 'fwd');
      expect(
        await peers.client.rpcError('talk.request', { talk, procedure: 'ABORT', text: '7 no' }),
      ).toMatchObject({ kind: 'status', code: StatusCode.permissionDenied, reason: 'no' });
      const moved = wikiBytes('test-vectors/sharding-payloads/moved-owner-0x1a2b-epoch-7');
      expect(
        await peers.client.rpcError('talk.request', {
          talk,
          procedure: 'STATUS',
          payload: hexString(moved),
        }),
      ).toMatchObject({ code: StatusCode.moved, body: hexString(moved.subarray(2)) });
      expect(
        await peers.client.rpcError('talk.request', { talk, procedure: 'NOPE' }),
      ).toMatchObject({ code: StatusCode.unimplemented });
    });

    test('an EXTEND passes through with its values', async () => {
      const { talk } = await openTalk(peers, 'fwd');
      const reply = await peers.client.rpc<MessageInfo>('talk.request', {
        talk,
        procedure: 'EXTEND',
        text: '600',
        timeoutMs: 250,
      });
      expect(reply.payload).toBe(hex('extended'));
      expect(await peers.client.events.waitFor({ event: 'talk.extend', talk })).toMatchObject({
        deadlineMs: 2 * 600 + 5000,
      });
    });

    test('bulk payloads through the intermediary, both ways', async () => {
      const { talk } = await openTalk(peers, 'fwd');
      const n = 300_000;
      const [up, down] = await Promise.all([
        peers.client.rpc<MessageInfo>('talk.request', {
          talk,
          procedure: 'SIZE',
          patternLength: n,
        }),
        peers.client.rpc<MessageInfo>('talk.request', {
          talk,
          procedure: 'BIG',
          text: String(n),
          digest: true,
        }),
      ]);
      expect(up.payload).toBe(hex(`${n} ${sha256(pattern(n))}`));
      expect(down).toEqual({ procedure: '', bulk: true, size: n, sha256: sha256(pattern(n)) });
    });

    test('plain messages both ways', async () => {
      const { talk } = await openTalk(peers, 'fwd');
      await peers.client.rpc('talk.send', { talk, procedure: 'NOTE', text: 'to the backend' });
      expect(await peers.server.events.waitFor('service.message')).toMatchObject({
        procedure: 'NOTE',
        payload: hex('to the backend'),
      });
      await peers.client.rpc('talk.request', { talk, procedure: 'NOTIFY', text: 'back' });
      expect(await peers.client.events.waitFor({ event: 'talk.message', talk })).toMatchObject({
        procedure: 'NOTE',
        payload: hex('back'),
      });
    });

    test('a timeout of the requester reaches the intermediary as a cancel', async () => {
      const { talk } = await openTalk(peers, 'fwd');
      expect(
        await peers.client.rpcError('talk.request', { talk, procedure: 'STALL', timeoutMs: 200 }),
      ).toMatchObject({ code: StatusCode.deadlineExceeded });
      // The channel carries on.
      const reply = await peers.client.rpc<MessageInfo>('talk.request', {
        talk,
        procedure: 'ECHO',
        text: 'next',
      });
      expect(reply.payload).toBe(hex('next'));
    });

    test('the backend closing its channel closes the forwarded one with its status', async () => {
      const { talk, channel } = await openTalk(peers, 'fwd');
      await peers.client.rpc('talk.request', {
        talk,
        procedure: 'CLOSE',
        text: `${StatusCode.aborted} backend done`,
      });
      expect(await peers.client.events.waitFor({ event: 'channel.closed', channel })).toMatchObject(
        { code: StatusCode.aborted, reason: 'backend done' },
      );
    });
  },
);
