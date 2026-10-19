/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Closing channels and connections, and GOAWAY, across the pairings
// (mux_test.dart, mux_limits_test.dart, switchboard_test.dart): the status
// a CLOSE carries and how the far side reads it, outstanding requests of a
// closed channel, a connection closed under its channels, GOAWAY from either
// side with the open channels finishing first.

import { expect, test } from 'vitest';
import { StatusCode } from '../../../src/core/status.ts';
import { type CellPeers, describeCells } from '../matrix.ts';
import type { ChannelIncomingEvent, MessageInfo, PeerEvent } from '../protocol.ts';
import { connect, hex, openTalk, rawTalk } from './support.ts';

/** A raw channel from the client to the server's `raw` service. */
async function rawChannel(
  peers: CellPeers,
  connection?: string,
): Promise<{ connection: string; channel: string; far: string; farConnection: string }> {
  const c = connection ?? (await connect(peers));
  const seen = peers.server.events.filter('channel.incoming').length;
  const { channel } = await peers.client.rpc<{ channel: string }>('open', {
    connection: c,
    address: { type: 'raw' },
  });
  const [incoming] = (
    await peers.server.events.waitForCount<ChannelIncomingEvent>('channel.incoming', seen + 1)
  ).slice(seen);
  if (incoming === undefined) {
    throw new Error('no channel.incoming');
  }
  return { connection: c, channel, far: incoming.channel, farConnection: incoming.connection };
}

/** Whether the cell runs over the QUIC binding. */
function quic(peers: CellPeers): boolean {
  return peers.cell.transport === 'quic' || peers.cell.transport === 'webtransport';
}

/** The code and reason of a `channel.closed` or `connection.closed` event. */
function statusOf(event: PeerEvent): [unknown, unknown] {
  return [event['code'], event['reason']];
}

function closed(peer: CellPeers['server'], channel: string): Promise<PeerEvent> {
  return peer.events.waitFor({ event: 'channel.closed', channel });
}

describeCells('close', {}, (peers) => {
  test('a CLOSE carries its code and reason to the far side, which confirms it', async () => {
    const { channel, far } = await rawChannel(peers);
    await peers.client.rpc('channel.close', {
      channel,
      code: StatusCode.resourceExhausted,
      reason: 'full',
    });
    expect(await closed(peers.server, far)).toMatchObject({
      code: StatusCode.resourceExhausted,
      reason: 'full',
    });
    expect(await closed(peers.client, channel)).toMatchObject({
      code: StatusCode.resourceExhausted,
    });
  });

  test('a CLOSE from the server side, with OK', async () => {
    const { channel, far } = await rawChannel(peers);
    await peers.server.rpc('channel.close', { channel: far });
    expect(await closed(peers.client, channel)).toMatchObject({ code: StatusCode.ok, reason: '' });
    expect(await closed(peers.server, far)).toMatchObject({ code: StatusCode.ok });
  });

  test('both sides closing at once', async () => {
    const { channel, far } = await rawChannel(peers);
    await Promise.all([
      peers.client.rpc('channel.close', { channel, code: StatusCode.cancelled }),
      peers.server.rpc('channel.close', { channel: far, code: StatusCode.aborted }),
    ]);
    const [client, server] = await Promise.all([
      closed(peers.client, channel),
      closed(peers.server, far),
    ]);
    // Each side reports the first non-OK status it sent or received.
    expect([StatusCode.cancelled, StatusCode.aborted]).toContain(client['code']);
    expect([StatusCode.cancelled, StatusCode.aborted]).toContain(server['code']);
  });

  test('an application status is refused for a CLOSE before anything is sent', async () => {
    const { channel, far } = await rawChannel(peers);
    const error = await peers.client.rpcError('channel.close', { channel, code: 300, reason: 'a' });
    expect(error.kind).toBe('argument');
    // The channel is still open both ways.
    await peers.client.rpc('channel.send', { channel, hex: '01' });
    await peers.server.events.waitFor({ event: 'channel.data', channel: far, hex: '01' });
  });

  test('subframes sent before the CLOSE arrive before it', async () => {
    const { channel, far } = await rawChannel(peers);
    for (const b of ['01', '02', '03']) {
      await peers.client.rpc('channel.send', { channel, hex: b });
    }
    await peers.client.rpc('channel.close', { channel });
    await closed(peers.server, far);
    const data = peers.server.events.filter({ event: 'channel.data', channel: far });
    expect(data.map((d) => d['hex'])).toEqual(['01', '02', '03']);
  });

  test('closing the connection ends its channels: CONNECTION_LOST, or GOING_AWAY over QUIC', async () => {
    const { connection, channel, far, farConnection } = await rawChannel(peers);
    await peers.client.rpc('connection.close', { connection });
    expect(await closed(peers.client, channel)).toMatchObject({
      code: StatusCode.connectionLost,
    });
    const farChannel = statusOf(await closed(peers.server, far));
    const farEnd = statusOf(
      await peers.server.events.waitFor({ event: 'connection.closed', connection: farConnection }),
    );
    if (peers.cell.transport === 'webtransport') {
      // The close of a WebTransport session races two things in the native
      // QUIC stack both implementations use: the stack resets and stops
      // the session's streams with WT_SESSION_GONE, which carries no
      // application code and can overtake the CLOSE_WEBTRANSPORT_SESSION
      // capsule, and the dialler's provider shuts its native context down
      // once its last session ended, which can close the HTTP/3 connection
      // before the capsule is read (the far connection is then lost,
      // "shutdown"; close() sends no GOAWAY that would say otherwise). The
      // binding page holds such a reset for the session's end, so that the
      // far channel ends as the far connection does, as both
      // implementations do (design section 13, item 8).
      const shutdown = [StatusCode.connectionLost, 'shutdown'];
      const graceful = [StatusCode.goingAway, ''];
      expect([graceful, shutdown]).toContainEqual(farEnd);
      expect(farChannel).toEqual(farEnd);
      return;
    }
    // Over raw QUIC the close is an application close of the connection,
    // which the far side reads as a GOAWAY; over a stream the transport just
    // ends.
    const code = quic(peers) ? StatusCode.goingAway : StatusCode.connectionLost;
    expect(farChannel[0]).toBe(code);
    expect(farEnd[0]).toBe(code);
  });

  test('outstanding requests fail when the responder closes the channel', async () => {
    const { talk, channel } = await openTalk(peers);
    const stalled = peers.client.rpcError('talk.request', { talk, procedure: 'STALL' });
    const reply = await peers.client.rpc<MessageInfo>('talk.request', {
      talk,
      procedure: 'CLOSE',
      text: `${StatusCode.unavailable} restarting`,
    });
    expect(reply.payload).toBe(hex('closing'));
    expect(await closed(peers.client, channel)).toMatchObject({
      code: StatusCode.unavailable,
      reason: 'restarting',
    });
    expect(await stalled).toMatchObject({ kind: 'status', code: StatusCode.unavailable });
  });

  test('closing a Talk channel reaches the far side with its status', async () => {
    const { talk, channel, far } = await rawTalk(peers);
    await peers.client.rpc('talk.close', { talk, code: StatusCode.cancelled, reason: 'done' });
    expect(await closed(peers.server, far)).toMatchObject({
      code: StatusCode.cancelled,
      reason: 'done',
    });
    expect(await closed(peers.client, channel)).toMatchObject({ code: StatusCode.cancelled });
  });

  test('a request on a closed Talk channel fails at once', async () => {
    const { talk } = await openTalk(peers);
    await peers.client.rpc('talk.close', { talk });
    const error = await peers.client.rpcError('talk.request', { talk, procedure: 'ECHO' });
    expect(error).toMatchObject({
      kind: 'status',
      code: StatusCode.failedPrecondition,
      reason: 'channel closed',
    });
  });
});

describeCells('goaway', {}, (peers) => {
  test("after the server's GOAWAY: the open channel finishes, a new one is refused", async () => {
    const connection = await connect(peers);
    const { talk, channel } = await openTalk(peers, 'echo', connection);
    const reply = await peers.client.rpc<MessageInfo>('talk.request', {
      talk,
      procedure: 'GOAWAY',
    });
    expect(reply.payload).toBe(hex('going'));
    expect(
      await peers.client.events.waitFor({ event: 'connection.goAway', connection }),
    ).toMatchObject({ code: StatusCode.goingAway });
    // The open channel is still served.
    const still = await peers.client.rpc<MessageInfo>('talk.request', {
      talk,
      procedure: 'ECHO',
      text: 'still',
    });
    expect(still.payload).toBe(hex('still'));
    // No new channel on this connection.
    const refused = await peers.client.rpcError('open', { connection, address: { type: 'echo' } });
    expect(refused).toMatchObject({ kind: 'status', code: StatusCode.failedPrecondition });
    // Once the last channel is gone, the connection ends.
    await peers.client.rpc('talk.close', { talk });
    await closed(peers.client, channel);
    expect(
      await peers.client.events.waitFor({ event: 'connection.closed', connection }),
    ).toMatchObject({ code: StatusCode.goingAway });
    // A new connection is served.
    const again = await openTalk(peers);
    const answer = await peers.client.rpc<MessageInfo>('talk.request', {
      talk: again.talk,
      procedure: 'ECHO',
      text: 'again',
    });
    expect(answer.payload).toBe(hex('again'));
  });

  test("after the client's GOAWAY: the server finishes the open channel, then the connection ends", async () => {
    const { connection, channel, far, farConnection } = await rawChannel(peers);
    // goAway() resolves once the connection ended: not awaited here.
    const going = peers.client.rpc('connection.goAway', { connection, reason: 'leaving' });
    expect(
      await peers.server.events.waitFor({ event: 'connection.goAway', connection: farConnection }),
    ).toMatchObject({ code: StatusCode.goingAway, reason: 'leaving' });
    // The open channel carries on both ways.
    await peers.client.rpc('channel.send', { channel, hex: 'AA' });
    await peers.server.events.waitFor({ event: 'channel.data', channel: far, hex: 'AA' });
    await peers.server.rpc('channel.send', { channel: far, hex: 'BB' });
    await peers.client.events.waitFor({ event: 'channel.data', channel, hex: 'BB' });
    // Closing the last channel ends the connection.
    await peers.server.rpc('channel.close', { channel: far });
    await closed(peers.client, channel);
    await going;
    const [client, server] = await Promise.all([
      peers.client.events.waitFor({ event: 'connection.closed', connection }),
      peers.server.events.waitFor({ event: 'connection.closed', connection: farConnection }),
    ]);
    // Both ends read it as the GOAWAY it is.
    expect([client['code'], server['code']]).toEqual([StatusCode.goingAway, StatusCode.goingAway]);
  });

  test('the server opens no channel after the client sent GOAWAY', async () => {
    const { connection, farConnection } = await rawChannel(peers);
    void peers.client.rpc('connection.goAway', { connection }).catch(() => {});
    await peers.server.events.waitFor({ event: 'connection.goAway', connection: farConnection });
    const refused = await peers.server.rpcError('open', {
      connection: farConnection,
      address: { type: 'echo' },
    });
    expect(refused).toMatchObject({ kind: 'status', code: StatusCode.failedPrecondition });
  });

  test('a GOAWAY with another code keeps it', async () => {
    const { connection, farConnection } = await rawChannel(peers);
    void peers.client
      .rpc('connection.goAway', {
        connection,
        code: StatusCode.unavailable,
        reason: 'maintenance',
      })
      .catch(() => {});
    expect(
      await peers.server.events.waitFor({ event: 'connection.goAway', connection: farConnection }),
    ).toMatchObject({ code: StatusCode.unavailable, reason: 'maintenance' });
  });

  test('an application status is refused for a GOAWAY before anything is sent', async () => {
    const connection = await connect(peers);
    const error = await peers.client.rpcError('connection.goAway', {
      connection,
      code: 300,
      reason: 'x',
    });
    expect(error.kind).toBe('argument');
    await peers.client.rpc('connection.ping', { connection });
  });

  test('a GOAWAY on an idle connection ends it', async () => {
    const connection = await connect(peers);
    await peers.client.rpc('connection.goAway', { connection });
    expect(
      await peers.client.events.waitFor({ event: 'connection.closed', connection }),
    ).toMatchObject({ code: StatusCode.goingAway });
  });
});
