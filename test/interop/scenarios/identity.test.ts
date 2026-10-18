/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Identity across the pairings (identity_test.dart, identity_node_test.dart,
// the QUIC binding_test.dart): NONCE and IDENT both ways with credentials of
// the authorities both peers share (README, "Identity"), holder keys with
// their proof and bearer credentials, intents and named receivers, refused
// credentials, channels held until IDENT, scoped policies. Over raw QUIC and
// WebTransport the listener applies the specification's rules
// (QuicIdentityRules.listener), so every IDENT there is exporter-bound: a
// proof one implementation signs over a different exporter than the other
// derives would fail these cells.

import { expect, test } from 'vitest';
import { StatusCode } from '../../../src/core/status.ts';
import { type CellPeers, describeCells } from '../matrix.ts';
import type { MessageInfo, PeerConfig, PeerEvent } from '../protocol.ts';
import { connect, openTalk, text } from './support.ts';

/** A server that verifies, presents `server-1` and identifies in return. */
const server: PeerConfig = {
  identity: {
    verify: true,
    credential: { identity: 'server-1' },
    identifyInReturn: true,
    quicRules: 'listener',
  },
};

/** Whether the cell runs over the QUIC binding. */
function quic(peers: CellPeers): boolean {
  return peers.cell.transport === 'quic' || peers.cell.transport === 'webtransport';
}

/** What the server's echo service says the client is. */
async function whoami(peers: CellPeers, connection?: string): Promise<string> {
  const { talk } = await openTalk(peers, 'echo', connection);
  const reply = await peers.client.rpc<MessageInfo>('talk.request', { talk, procedure: 'WHOAMI' });
  return text(reply.payload);
}

/** The server's `connection.identified` event for the identity [identity]. */
function identifiedAtServer(peers: CellPeers, identity: string): Promise<PeerEvent> {
  return peers.server.events.waitFor(
    (e) => e.event === 'connection.identified' && e['identity'] === identity,
  );
}

describeCells(
  'identity: holder keys both ways',
  {
    server,
    client: { identity: { verify: true, credential: { identity: 'npc-7' } } },
  },
  (peers) => {
    test('the client proves its holder key; the service sees its identity', async () => {
      expect(await whoami(peers)).toBe('npc-7');
      expect(await identifiedAtServer(peers, 'npc-7')).toMatchObject({
        kind: 'node',
        provedPossession: true,
        intent: '',
      });
    });

    test('the server identifies in return', async () => {
      const connection = await connect(peers);
      expect(
        await peers.client.events.waitFor({ event: 'connection.identified', connection }),
      ).toMatchObject({ identity: 'server-1', provedPossession: true });
    });

    test('an intent reaches the server', async () => {
      await peers.client.rpc('connect', { endpoint: peers.endpoint, intent: '01 02 03' });
      expect(await identifiedAtServer(peers, 'npc-7')).toMatchObject({ intent: '01 02 03' });
    });

    test('naming the server as the receiver', async () => {
      const connection = await peers.client.rpc<{ connection: string }>('connect', {
        endpoint: peers.endpoint,
        receiver: 'server-1',
      });
      expect(await identifiedAtServer(peers, 'npc-7')).toMatchObject({ receiver: 'server-1' });
      expect(await whoami(peers, connection.connection)).toBe('npc-7');
    });

    test('naming another receiver fails the connection with UNAUTHENTICATED', async () => {
      const error = await peers.client.rpcError('connect', {
        endpoint: peers.endpoint,
        receiver: 'someone-else',
      });
      expect(error).toMatchObject({ kind: 'status', code: StatusCode.unauthenticated });
    });

    test('identifying again on a live connection, with an intent', async () => {
      const connection = await connect(peers);
      await identifiedAtServer(peers, 'npc-7');
      await peers.client.rpc('connection.identify', { connection, intent: 'AA' });
      await peers.server.events.waitFor(
        (e) => e.event === 'connection.identified' && e['intent'] === 'AA',
      );
      expect(await whoami(peers, connection)).toBe('npc-7');
    });
  },
);

describeCells(
  'identity: a bearer credential',
  {
    server,
    client: { identity: { verify: true, credential: { identity: 'npc-8', authority: 'hmac' } } },
  },
  (peers) => {
    test('a bearer credential identifies without a proof, but not to an unauthenticated QUIC listener', async () => {
      if (quic(peers)) {
        // The initiator's rules over QUIC: a bearer credential, which carries
        // no proof, goes only to an authenticated end, and a listener with a
        // self-signed certificate identifies only to an identified peer.
        const error = await peers.client.rpcError('connect', { endpoint: peers.endpoint });
        expect(error).toMatchObject({ kind: 'status', code: StatusCode.unauthenticated });
        expect(error.reason).toContain('a bearer credential goes only to an authenticated peer');
        return;
      }
      expect(await whoami(peers)).toBe('npc-8');
      expect(await identifiedAtServer(peers, 'npc-8')).toMatchObject({
        provedPossession: false,
      });
    });
  },
);

describeCells(
  'identity: refused credentials',
  { server, client: { identity: { credential: { identity: 'rogue-1', authority: 'rogue' } } } },
  (peers) => {
    test('a credential of another authority fails the connection with UNAUTHENTICATED', async () => {
      const error = await peers.client.rpcError('connect', { endpoint: peers.endpoint });
      expect(error).toMatchObject({ kind: 'status', code: StatusCode.unauthenticated });
    });
  },
);

describeCells(
  'identity: a forged IDENT',
  { server, client: { identity: { credential: { identity: 'npc-7', forge: true } } } },
  (peers) => {
    test('a credential whose signature was altered fails with UNAUTHENTICATED', async () => {
      const error = await peers.client.rpcError('connect', { endpoint: peers.endpoint });
      expect(error).toMatchObject({ kind: 'status', code: StatusCode.unauthenticated });
    });
  },
);

describeCells(
  'identity: channels held until IDENT',
  {
    server: {
      identity: { ...server.identity, policy: 'scoped', identityTimeoutMs: 300 },
    },
    client: {
      identity: {
        verify: true,
        identifyOutgoing: false,
        credential: { identity: 'npc-9', scopes: [{ right: 'open', pattern: 'echo' }] },
      },
    },
    transports: ['tcp', 'ws'],
  },
  (peers) => {
    test('a channel opened before IDENT is held, then admitted', async () => {
      const connection = await connect(peers);
      const { talk } = await openTalk(peers, 'echo', connection);
      const reply = peers.client.rpc<MessageInfo>('talk.request', { talk, procedure: 'WHOAMI' });
      await peers.client.rpc('connection.identify', { connection });
      expect(text((await reply).payload)).toBe('npc-9');
    });

    test('a channel held past the identity timeout is refused UNAUTHENTICATED', async () => {
      const connection = await connect(peers);
      const started = Date.now();
      const { channel } = await openTalk(peers, 'echo', connection);
      expect(await peers.client.events.waitFor({ event: 'channel.closed', channel })).toMatchObject(
        {
          code: StatusCode.unauthenticated,
        },
      );
      expect(Date.now() - started).toBeGreaterThanOrEqual(250);
    });

    test('a scoped policy admits the types the credential may open', async () => {
      const connection = await connect(peers);
      await peers.client.rpc('connection.identify', { connection });
      expect(await whoami(peers, connection)).toBe('npc-9');
      const { channel } = await openTalk(peers, 'raw', connection);
      expect(await peers.client.events.waitFor({ event: 'channel.closed', channel })).toMatchObject(
        {
          code: StatusCode.permissionDenied,
        },
      );
    });
  },
);
