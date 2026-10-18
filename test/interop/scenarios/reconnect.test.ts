/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Scenario 23: the ReconnectingClient of one implementation against a
// server of either (reconnecting_client_test.dart, the QUIC
// web_binding_test.dart): the server closing the connection and sending
// GOAWAY, the persistent channels and Talk subscriptions opened again on
// the next connection, the application's channels finishing on the old one,
// opens waiting for a connection, stop, start, close and a forced
// reconnect, and the channels the server pushes. What is asserted is what
// crosses the wire and the client's states by code, never how long
// anything took: the backoff runs without jitter, so its delays are exact.

import { afterEach, expect, test } from 'vitest';
import { StatusCode } from '../../../src/core/status.ts';
import { type CellPeers, describeCells } from '../matrix.ts';
import type {
  ChannelIncomingEvent,
  MessageInfo,
  PeerEvent,
  PersistentOpenedEvent,
  ReconnectIncomingEvent,
  ReconnectStateEvent,
} from '../protocol.ts';
import { hex } from './support.ts';

/** The first retry's delay; no jitter, so that every delay is exact. */
const initialBackoffMs = 100;

let serial = 0;

/** An address payload no other test uses, so that stale events of earlier tests never match. */
function tag(): string {
  serial++;
  return hex(`reconnect ${serial}`);
}

/** Whether the cell runs over the QUIC binding. */
function quic(peers: CellPeers): boolean {
  return peers.cell.transport === 'quic' || peers.cell.transport === 'webtransport';
}

/**
 * How the client reads the server closing the connection: over a stream
 * the transport just ends (CONNECTION_LOST); over raw QUIC and
 * WebTransport the close is an application close the client reads as a
 * GOAWAY, which it takes for a planned restart (GOING_AWAY).
 */
function lostOnClose(peers: CellPeers): number {
  return quic(peers) ? StatusCode.goingAway : StatusCode.connectionLost;
}

/** A reconnecting client of the client peer to the server, started. */
async function openClient(
  peers: CellPeers,
  options: Record<string, unknown> = {},
): Promise<string> {
  const { reconnect } = await peers.client.rpc<{ reconnect: string }>('reconnect.open', {
    endpoint: peers.endpoint,
    initialBackoffMs,
    jitter: 0,
    ...options,
  });
  return reconnect;
}

/** What both implementations agree on of a state: phase, attempt, last status code, retry delay. */
function brief(s: ReconnectStateEvent): {
  phase: string;
  attempt: number;
  last: number | null;
  retryMs: number | null;
} {
  return {
    phase: s.phase,
    attempt: s.attempt,
    last: s.lastStatus?.code ?? null,
    retryMs: s.retryMs,
  };
}

/** Every state [reconnect] reported so far. */
function states(peers: CellPeers, reconnect: string): ReconnectStateEvent[] {
  return peers.client.events.filter<ReconnectStateEvent>({ event: 'reconnect.state', reconnect });
}

/** The [n]th state of [reconnect] (from 1), once reported. */
async function nthState(
  peers: CellPeers,
  reconnect: string,
  n: number,
): Promise<ReconnectStateEvent> {
  const all = await peers.client.events.waitForCount<ReconnectStateEvent>(
    { event: 'reconnect.state', reconnect },
    n,
  );
  return all[n - 1] as ReconnectStateEvent;
}

/** The [n]th time [reconnect] connected (from 1). */
async function connected(peers: CellPeers, reconnect: string, n = 1): Promise<ReconnectStateEvent> {
  const all = await peers.client.events.waitForCount<ReconnectStateEvent>(
    { event: 'reconnect.state', reconnect, phase: 'connected' },
    n,
  );
  return all[n - 1] as ReconnectStateEvent;
}

/** The [n]th channel (from 1) [persistent] opened. */
async function opened(peers: CellPeers, persistent: string, n = 1): Promise<PersistentOpenedEvent> {
  const all = await peers.client.events.waitForCount<PersistentOpenedEvent>(
    { event: 'persistent.opened', persistent },
    n,
  );
  return all[n - 1] as PersistentOpenedEvent;
}

/** The [n]th event (from 1) of [persistent] named [event]. */
async function nthOf(
  peers: CellPeers,
  event: string,
  persistent: string,
  n = 1,
): Promise<PeerEvent> {
  const all = await peers.client.events.waitForCount({ event, persistent }, n);
  return all[n - 1] as PeerEvent;
}

/** The [n]th channel (from 1) the server's `raw` service accepted with [payload]. */
async function accepted(peers: CellPeers, payload: string, n = 1): Promise<ChannelIncomingEvent> {
  const all = await peers.server.events.waitForCount<ChannelIncomingEvent>(
    { event: 'channel.incoming', payload },
    n,
  );
  return all[n - 1] as ChannelIncomingEvent;
}

/** A persistent channel of [reconnect] to the server's `raw` service with [payload]. */
async function persistentRaw(
  peers: CellPeers,
  reconnect: string,
  payload: string,
): Promise<string> {
  const { persistent } = await peers.client.rpc<{ persistent: string }>('reconnect.persistent', {
    reconnect,
    address: { type: 'raw', payload },
    talk: false,
  });
  return persistent;
}

/** A Talk channel to the server's `echo` through [reconnect]. */
function echoTalk(
  peers: CellPeers,
  reconnect: string,
  options: Record<string, unknown> = {},
): Promise<{ talk: string; channel: string; connection: string }> {
  return peers.client.rpc('reconnect.talk', { reconnect, address: { type: 'echo' }, ...options });
}

/**
 * An `ECHO` round trip on a Talk channel of its own through [reconnect],
 * closed afterwards, and the connection it went over. Once answered, the
 * server's node has adopted that connection, so that `accepted.goAway` and
 * `accepted.close` find it: a client can be connected over QUIC before the
 * listener hands the connection to its node.
 */
async function idleRoundTrip(peers: CellPeers, reconnect: string): Promise<string> {
  const t = await echoTalk(peers, reconnect);
  expect(await echo(peers, t.talk, 'ping')).toBe(hex('ping'));
  await peers.client.rpc('talk.close', { talk: t.talk });
  await peers.client.events.waitFor({ event: 'channel.closed', channel: t.channel });
  return t.connection;
}

/** `ECHO` of [text] on [talk]: the payload it answers, as text in hex. */
async function echo(peers: CellPeers, talk: string, text: string): Promise<string | undefined> {
  const reply = await peers.client.rpc<MessageInfo>('talk.request', {
    talk,
    procedure: 'ECHO',
    text,
  });
  return reply.payload;
}

describeCells('reconnecting client', {}, (peers) => {
  // The event logs of a failed test, before the cell's reset forgets them
  // (hooks after a test run newest first).
  afterEach(({ task }) => {
    if (task.result?.state === 'fail') {
      for (const peer of [peers.server, peers.client]) {
        const lines = peer.events.all.map((e) => JSON.stringify(e)).join('\n');
        console.error(`${peer.implementation} peer events:\n${lines}`);
      }
    }
  });

  test('connects, reports its states and serves requests on the connection', async () => {
    const reconnect = await openClient(peers);
    const a = await echoTalk(peers, reconnect);
    expect(await echo(peers, a.talk, 'one')).toBe(hex('one'));
    const up = await connected(peers, reconnect);
    expect(states(peers, reconnect).map(brief)).toEqual([
      { phase: 'connecting', attempt: 1, last: null, retryMs: null },
      { phase: 'connected', attempt: 1, last: null, retryMs: null },
    ]);
    expect(up.connection).toBe(a.connection);
    // Opens go to the current connection.
    const b = await echoTalk(peers, reconnect);
    expect(b.connection).toBe(a.connection);
    expect(await echo(peers, b.talk, 'two')).toBe(hex('two'));
  });

  test('the server closes the connection: reconnects and opens the persistent channel again', async () => {
    const reconnect = await openClient(peers);
    const payload = tag();
    const persistent = await persistentRaw(peers, reconnect, payload);
    const first = await opened(peers, persistent);
    const far = await accepted(peers, payload);
    await peers.client.rpc('channel.send', { channel: first.channel, hex: '01' });
    await peers.server.events.waitFor({ event: 'channel.data', channel: far.channel, hex: '01' });
    await connected(peers, reconnect);

    await peers.server.rpc('connection.close', { connection: far.connection });
    const lost = lostOnClose(peers);
    expect(await nthOf(peers, 'persistent.closed', persistent)).toMatchObject({ code: lost });
    expect(
      await peers.client.events.waitFor({
        event: 'connection.closed',
        connection: first.connection,
      }),
    ).toMatchObject({ code: lost });

    // Opened again on the next connection, where data flows both ways.
    const second = await opened(peers, persistent, 2);
    const farAgain = await accepted(peers, payload, 2);
    expect(second.connection).not.toBe(first.connection);
    expect(farAgain.connection).not.toBe(far.connection);
    await peers.client.rpc('channel.send', { channel: second.channel, hex: '05' });
    await peers.server.events.waitFor({
      event: 'channel.data',
      channel: farAgain.channel,
      hex: '05',
    });
    await peers.server.rpc('channel.send', { channel: farAgain.channel, hex: '06' });
    await peers.client.events.waitFor({
      event: 'channel.data',
      channel: second.channel,
      hex: '06',
    });

    await connected(peers, reconnect, 2);
    const after = states(peers, reconnect).slice(2).map(brief);
    if (lost === StatusCode.connectionLost) {
      // A loss: the first retry after the initial backoff.
      expect(after).toEqual([
        { phase: 'disconnected', attempt: 0, last: lost, retryMs: initialBackoffMs },
        { phase: 'connecting', attempt: 1, last: lost, retryMs: null },
        { phase: 'connected', attempt: 1, last: lost, retryMs: null },
      ]);
    } else {
      // A planned restart: connecting again at once.
      expect(after).toEqual([
        { phase: 'connecting', attempt: 1, last: lost, retryMs: null },
        { phase: 'connected', attempt: 1, last: lost, retryMs: null },
      ]);
    }

    // Closed for good: the server's end closes with OK.
    expect(await peers.client.rpc('persistent.close', { persistent })).toMatchObject({
      code: StatusCode.ok,
    });
    expect(await nthOf(peers, 'persistent.closed', persistent, 2)).toMatchObject({
      code: StatusCode.ok,
    });
    expect(await nthOf(peers, 'persistent.done', persistent)).toMatchObject({
      code: StatusCode.ok,
    });
    expect(
      await peers.server.events.waitFor({ event: 'channel.closed', channel: farAgain.channel }),
    ).toMatchObject({ code: StatusCode.ok });
    expect(peers.client.events.filter({ event: 'persistent.opened', persistent })).toHaveLength(2);
  });

  test("the server's GOAWAY: new opens go to a new connection, the old one finishes its work", async () => {
    const reconnect = await openClient(peers);
    const old = await echoTalk(peers, reconnect);
    await connected(peers, reconnect);
    // In flight on the old connection when the GOAWAY comes.
    const slow = peers.client.rpc<MessageInfo>('talk.request', {
      talk: old.talk,
      procedure: 'SLOW',
      text: '300',
    });
    const going = await peers.client.rpc<MessageInfo>('talk.request', {
      talk: old.talk,
      procedure: 'GOAWAY',
    });
    expect(going.payload).toBe(hex('going'));
    expect(
      await peers.client.events.waitFor({ event: 'connection.goAway', connection: old.connection }),
    ).toMatchObject({ code: StatusCode.goingAway });

    // A planned restart: a new connection at once, reported with the GOAWAY status.
    await connected(peers, reconnect, 2);
    expect(states(peers, reconnect).slice(2).map(brief)).toEqual([
      { phase: 'connecting', attempt: 1, last: StatusCode.goingAway, retryMs: null },
      { phase: 'connected', attempt: 1, last: StatusCode.goingAway, retryMs: null },
    ]);
    const fresh = await echoTalk(peers, reconnect);
    expect(fresh.connection).not.toBe(old.connection);
    expect(await echo(peers, fresh.talk, 'new')).toBe(hex('new'));

    // The old channel finishes what was in flight and is still served.
    expect((await slow).payload).toBe(hex('slow'));
    expect(await echo(peers, old.talk, 'old')).toBe(hex('old'));
    // Once it is done, the old connection ends, and the client stays where it is.
    await peers.client.rpc('talk.close', { talk: old.talk });
    expect(
      await peers.client.events.waitFor({ event: 'connection.closed', connection: old.connection }),
    ).toMatchObject({ code: StatusCode.goingAway });
    expect(await echo(peers, fresh.talk, 'still')).toBe(hex('still'));
    expect(states(peers, reconnect)).toHaveLength(4);
  });

  test('GOAWAY on an idle connection reconnects at once; again soon after, it backs off', async () => {
    const reconnect = await openClient(peers);
    const first = await connected(peers, reconnect);
    expect(first.connection).not.toBeNull();
    expect(await idleRoundTrip(peers, reconnect)).toBe(first.connection);
    const { connections } = await peers.server.rpc<{ connections: string[] }>('accepted.goAway');
    expect(
      await peers.client.events.waitFor({
        event: 'connection.goAway',
        connection: first.connection,
      }),
    ).toMatchObject({ code: StatusCode.goingAway });
    // The idle connection ends at once on both sides.
    expect(
      await peers.client.events.waitFor({
        event: 'connection.closed',
        connection: first.connection,
      }),
    ).toMatchObject({ code: StatusCode.goingAway });
    await peers.server.events.waitFor(
      (e) =>
        e.event === 'connection.closed' &&
        connections.includes(String(e['connection'])) &&
        e['code'] === StatusCode.goingAway,
    );
    const second = await connected(peers, reconnect, 2);
    expect(second.connection).not.toBe(first.connection);
    expect(states(peers, reconnect).slice(2).map(brief)).toEqual([
      { phase: 'connecting', attempt: 1, last: StatusCode.goingAway, retryMs: null },
      { phase: 'connected', attempt: 1, last: StatusCode.goingAway, retryMs: null },
    ]);

    // The new connection goes away too before it lasted maxBackoff: not a
    // planned restart any more, the backoff applies.
    expect(await idleRoundTrip(peers, reconnect)).toBe(second.connection);
    await peers.server.rpc('accepted.goAway');
    await connected(peers, reconnect, 3);
    expect(states(peers, reconnect).slice(4).map(brief)).toEqual([
      { phase: 'disconnected', attempt: 0, last: StatusCode.goingAway, retryMs: initialBackoffMs },
      { phase: 'connecting', attempt: 1, last: StatusCode.goingAway, retryMs: null },
      { phase: 'connected', attempt: 1, last: StatusCode.goingAway, retryMs: null },
    ]);
    const talk = await echoTalk(peers, reconnect);
    expect(await echo(peers, talk.talk, 'up')).toBe(hex('up'));
  });

  test('a persistent Talk channel sends its subscription again on every connection', async () => {
    const reconnect = await openClient(peers);
    const { persistent } = await peers.client.rpc<{ persistent: string }>('reconnect.persistent', {
      reconnect,
      address: { type: 'echo' },
      request: { procedure: 'ECHO', text: 'subscribe' },
    });
    const first = await opened(peers, persistent);
    expect(await nthOf(peers, 'persistent.reply', persistent)).toMatchObject({
      talk: first.talk,
      payload: hex('subscribe'),
    });

    // The server's GOAWAY: the client closes the channel with GOING_AWAY, so
    // that the old connection goes idle and ends, and opens it again on the
    // new connection. (Not the echo service's GOAWAY on this channel: over
    // QUIC the GOAWAY on the control stream can overtake the reply on the
    // channel's stream, and the client closes the channel under it.)
    await peers.server.rpc('accepted.goAway');
    expect(await nthOf(peers, 'persistent.closed', persistent)).toMatchObject({
      code: StatusCode.goingAway,
    });
    const second = await opened(peers, persistent, 2);
    expect(second.connection).not.toBe(first.connection);
    expect(await nthOf(peers, 'persistent.reply', persistent, 2)).toMatchObject({
      talk: second.talk,
      payload: hex('subscribe'),
    });
    expect(
      await peers.client.events.waitFor({
        event: 'connection.closed',
        connection: first.connection,
      }),
    ).toMatchObject({ code: StatusCode.goingAway });

    // The server closes the new connection: the same on the next one.
    await peers.server.rpc('accepted.close');
    expect(await nthOf(peers, 'persistent.closed', persistent, 2)).toMatchObject({
      code: lostOnClose(peers),
    });
    const third = await opened(peers, persistent, 3);
    expect([first.connection, second.connection]).not.toContain(third.connection);
    expect(await nthOf(peers, 'persistent.reply', persistent, 3)).toMatchObject({
      talk: third.talk,
      payload: hex('subscribe'),
    });
    expect(await echo(peers, String(third.talk), 'on it')).toBe(hex('on it'));

    expect(await peers.client.rpc('persistent.close', { persistent })).toMatchObject({
      code: StatusCode.ok,
    });
    expect(await nthOf(peers, 'persistent.done', persistent)).toMatchObject({
      code: StatusCode.ok,
    });
  });

  test('opens issued while not connected wait for the next connection', async () => {
    const reconnect = await openClient(peers, { initialBackoffMs: 300 });
    const first = await echoTalk(peers, reconnect);

    // Stopped: an open that may not wait fails at once; one that may waits for start().
    expect(await peers.client.rpc('reconnect.stop', { reconnect })).toMatchObject({
      phase: 'disconnected',
      attempt: 0,
      lastStatus: { code: StatusCode.cancelled },
      retryMs: null,
      connection: null,
    });
    expect(
      await peers.client.rpcError('reconnect.talk', {
        reconnect,
        address: { type: 'echo' },
        openTimeoutMs: 0,
      }),
    ).toMatchObject({ kind: 'status', code: StatusCode.unavailable });
    const waiting = echoTalk(peers, reconnect);
    await peers.client.rpc('reconnect.start', { reconnect });
    const resumed = await waiting;
    expect(resumed.connection).not.toBe(first.connection);
    expect(await echo(peers, resumed.talk, 'resumed')).toBe(hex('resumed'));

    // The server closes the connection: an open issued as soon as the client
    // left it is served on the next one.
    const up = await connected(peers, reconnect, 2);
    await peers.server.rpc('accepted.close');
    await nthState(peers, reconnect, states(peers, reconnect).indexOf(up) + 2);
    const next = await echoTalk(peers, reconnect);
    expect([first.connection, resumed.connection]).not.toContain(next.connection);
    expect(await echo(peers, next.talk, 'next')).toBe(hex('next'));
  });

  test('stop() sends GOAWAY and stops reconnecting; start() resumes', async () => {
    const reconnect = await openClient(peers);
    const payload = tag();
    const persistent = await persistentRaw(peers, reconnect, payload);
    const far = await accepted(peers, payload);
    await opened(peers, persistent);
    await connected(peers, reconnect);

    expect(await peers.client.rpc('reconnect.stop', { reconnect })).toMatchObject({
      phase: 'disconnected',
      attempt: 0,
      lastStatus: { code: StatusCode.cancelled },
      retryMs: null,
    });
    // The server reads the GOAWAY; the persistent channel was closed with
    // GOING_AWAY, so the connection went idle and ended.
    expect(
      await peers.server.events.waitFor({ event: 'connection.goAway', connection: far.connection }),
    ).toMatchObject({ code: StatusCode.goingAway });
    expect(
      await peers.server.events.waitFor({ event: 'channel.closed', channel: far.channel }),
    ).toMatchObject({ code: StatusCode.goingAway });
    expect(await nthOf(peers, 'persistent.closed', persistent)).toMatchObject({
      code: StatusCode.goingAway,
    });
    expect(
      await peers.server.events.waitFor({ event: 'connection.closed', connection: far.connection }),
    ).toMatchObject({ code: StatusCode.goingAway });
    expect(peers.client.events.filter({ event: 'persistent.done', persistent })).toEqual([]);

    await peers.client.rpc('reconnect.start', { reconnect });
    const again = await accepted(peers, payload, 2);
    expect(again.connection).not.toBe(far.connection);
    await opened(peers, persistent, 2);
    await connected(peers, reconnect, 2);
    expect(states(peers, reconnect).slice(2).map(brief)).toEqual([
      { phase: 'disconnected', attempt: 0, last: StatusCode.cancelled, retryMs: null },
      { phase: 'connecting', attempt: 1, last: StatusCode.cancelled, retryMs: null },
      { phase: 'connected', attempt: 1, last: StatusCode.cancelled, retryMs: null },
    ]);
    // No reconnect happened while stopped.
    expect(peers.server.events.filter({ event: 'channel.incoming', payload })).toHaveLength(2);
  });

  test('close() ends the client: GOAWAY, the persistent channel ends for good, nothing opens after', async () => {
    const reconnect = await openClient(peers);
    const payload = tag();
    const persistent = await persistentRaw(peers, reconnect, payload);
    const far = await accepted(peers, payload);
    await connected(peers, reconnect);

    await peers.client.rpc('reconnect.close', { reconnect });
    const last = states(peers, reconnect).at(-1);
    expect(last === undefined ? undefined : brief(last)).toEqual({
      phase: 'disconnected',
      attempt: 0,
      last: StatusCode.cancelled,
      retryMs: null,
    });
    // close() stops first: the persistent channel was closed with GOING_AWAY.
    expect(await nthOf(peers, 'persistent.done', persistent)).toMatchObject({
      code: StatusCode.goingAway,
    });
    expect(peers.client.events.filter({ event: 'persistent.closed', persistent })).toMatchObject([
      { code: StatusCode.goingAway },
    ]);
    expect(
      await peers.server.events.waitFor({ event: 'connection.goAway', connection: far.connection }),
    ).toMatchObject({ code: StatusCode.goingAway });
    expect(
      await peers.server.events.waitFor({ event: 'channel.closed', channel: far.channel }),
    ).toMatchObject({ code: StatusCode.goingAway });
    expect(
      await peers.server.events.waitFor({ event: 'connection.closed', connection: far.connection }),
    ).toMatchObject({ code: StatusCode.goingAway });

    // Nothing opens after close().
    expect(
      await peers.client.rpcError('reconnect.talk', { reconnect, address: { type: 'echo' } }),
    ).toMatchObject({ kind: 'status', code: StatusCode.failedPrecondition });
    expect(
      await peers.client.rpcError('reconnect.persistent', { reconnect, address: { type: 'echo' } }),
    ).toMatchObject({ kind: 'status', code: StatusCode.failedPrecondition });
  });

  test('close() fails the opens still waiting and waits for the channels the application opened', async () => {
    const reconnect = await openClient(peers);
    const payload = tag();
    const own = await peers.client.rpc<{ channel: string; connection: string }>(
      'reconnect.channel',
      { reconnect, address: { type: 'raw', payload } },
    );
    const far = await accepted(peers, payload);
    await peers.client.rpc('reconnect.stop', { reconnect });
    const waiting = peers.client.rpcError('reconnect.talk', {
      reconnect,
      address: { type: 'echo' },
    });
    const closing = peers.client.rpc('reconnect.close', { reconnect });
    expect(await waiting).toMatchObject({ kind: 'status', code: StatusCode.cancelled });
    expect(
      await peers.server.events.waitFor({ event: 'connection.goAway', connection: far.connection }),
    ).toMatchObject({ code: StatusCode.goingAway });

    // The application's channel carries on both ways until it is closed.
    await peers.server.rpc('channel.send', { channel: far.channel, hex: 'AB' });
    await peers.client.events.waitFor({ event: 'channel.data', channel: own.channel, hex: 'AB' });
    await peers.client.rpc('channel.send', { channel: own.channel, hex: 'CD' });
    await peers.server.events.waitFor({ event: 'channel.data', channel: far.channel, hex: 'CD' });
    await peers.server.rpc('channel.close', { channel: far.channel });
    expect(
      await peers.client.events.waitFor({ event: 'channel.closed', channel: own.channel }),
    ).toMatchObject({ code: StatusCode.ok });
    // Then the connection ends and close() completes.
    await closing;
    expect(
      await peers.client.events.waitFor({ event: 'connection.closed', connection: own.connection }),
    ).toMatchObject({ code: StatusCode.goingAway });
    expect(
      await peers.server.events.waitFor({ event: 'connection.closed', connection: far.connection }),
    ).toMatchObject({ code: StatusCode.goingAway });
  });

  test('a forced reconnect moves the persistent channel; the old connection gets GOAWAY and finishes', async () => {
    const reconnect = await openClient(peers);
    const payload = tag();
    const persistent = await persistentRaw(peers, reconnect, payload);
    const far = await accepted(peers, payload);
    const ownPayload = tag();
    const own = await peers.client.rpc<{ channel: string; connection: string }>(
      'reconnect.channel',
      { reconnect, address: { type: 'raw', payload: ownPayload } },
    );
    const farOwn = await accepted(peers, ownPayload);
    const first = await opened(peers, persistent);
    await connected(peers, reconnect);

    await peers.client.rpc('reconnect.now', { reconnect, force: true });
    expect(
      await peers.server.events.waitFor({ event: 'connection.goAway', connection: far.connection }),
    ).toMatchObject({ code: StatusCode.goingAway });
    expect(
      await peers.server.events.waitFor({ event: 'channel.closed', channel: far.channel }),
    ).toMatchObject({ code: StatusCode.goingAway });
    expect(await nthOf(peers, 'persistent.closed', persistent)).toMatchObject({
      code: StatusCode.goingAway,
    });
    const again = await accepted(peers, payload, 2);
    expect(again.connection).not.toBe(far.connection);
    const second = await opened(peers, persistent, 2);
    expect(second.connection).not.toBe(first.connection);
    await connected(peers, reconnect, 2);
    expect(states(peers, reconnect).slice(2).map(brief)).toEqual([
      { phase: 'connecting', attempt: 1, last: StatusCode.goingAway, retryMs: null },
      { phase: 'connected', attempt: 1, last: StatusCode.goingAway, retryMs: null },
    ]);

    // The application's channel carries on over the old connection.
    await peers.server.rpc('channel.send', { channel: farOwn.channel, hex: '11' });
    await peers.client.events.waitFor({ event: 'channel.data', channel: own.channel, hex: '11' });
    // Once it is closed, the old connection ends on both sides.
    await peers.client.rpc('channel.close', { channel: own.channel });
    expect(
      await peers.client.events.waitFor({ event: 'connection.closed', connection: own.connection }),
    ).toMatchObject({ code: StatusCode.goingAway });
    expect(
      await peers.server.events.waitFor({ event: 'connection.closed', connection: far.connection }),
    ).toMatchObject({ code: StatusCode.goingAway });
  });

  test('a persistent channel the server closes is opened again; a terminal status ends it', async () => {
    const reconnect = await openClient(peers);
    const payload = tag();
    const persistent = await persistentRaw(peers, reconnect, payload);
    const first = await opened(peers, persistent);
    const far = await accepted(peers, payload);

    // Closed by the server: opened again on the same connection after the backoff.
    await peers.server.rpc('channel.close', {
      channel: far.channel,
      code: StatusCode.unavailable,
      reason: 'later',
    });
    expect(await nthOf(peers, 'persistent.closed', persistent)).toMatchObject({
      code: StatusCode.unavailable,
      reason: 'later',
    });
    const second = await opened(peers, persistent, 2);
    const farAgain = await accepted(peers, payload, 2);
    expect(second.connection).toBe(first.connection);
    expect(farAgain.connection).toBe(far.connection);

    // PERMISSION_DENIED: the endpoint would refuse it again, so it ends for good.
    await peers.server.rpc('channel.close', {
      channel: farAgain.channel,
      code: StatusCode.permissionDenied,
    });
    expect(await nthOf(peers, 'persistent.done', persistent)).toMatchObject({
      code: StatusCode.permissionDenied,
    });
    expect(peers.client.events.filter({ event: 'persistent.closed', persistent })).toMatchObject([
      { code: StatusCode.unavailable },
      { code: StatusCode.permissionDenied },
    ]);
    // The connection stayed up throughout.
    await connected(peers, reconnect);
    expect(states(peers, reconnect)).toHaveLength(2);
  });

  test('channels the server opens arrive on incoming, across reconnects', async () => {
    const reconnect = await openClient(peers);
    const payload = tag();
    await persistentRaw(peers, reconnect, payload);
    const far = await accepted(peers, payload);

    const pushed = await peers.server.rpc<{ channel: string }>('open', {
      connection: far.connection,
      address: { type: 'notify', payload: '01' },
    });
    await peers.server.rpc('channel.send', { channel: pushed.channel, hex: '2A' });
    const first = await peers.client.events.waitFor<ReconnectIncomingEvent>({
      event: 'reconnect.incoming',
      reconnect,
      payload: '01',
    });
    expect(first).toMatchObject({ type: 'notify', instance: 0 });
    await peers.client.events.waitFor({ event: 'channel.data', channel: first.channel, hex: '2A' });

    // A malformed OPEN payload is refused with PROTOCOL_ERROR, not delivered.
    const bad = await peers.server.rpc<{ channel: string }>('open', {
      connection: far.connection,
      openHex: 'F0',
    });
    expect(
      await peers.server.events.waitFor({ event: 'channel.closed', channel: bad.channel }),
    ).toMatchObject({ code: StatusCode.protocolError });

    // On the next connection after the server closed this one.
    await peers.server.rpc('connection.close', { connection: far.connection });
    const farAgain = await accepted(peers, payload, 2);
    const pushedAgain = await peers.server.rpc<{ channel: string }>('open', {
      connection: farAgain.connection,
      address: { type: 'notify', payload: '02' },
    });
    const second = await peers.client.events.waitFor<ReconnectIncomingEvent>({
      event: 'reconnect.incoming',
      reconnect,
      payload: '02',
    });
    expect(second.connection).not.toBe(first.connection);
    await peers.client.rpc('channel.send', { channel: second.channel, hex: '07' });
    await peers.server.events.waitFor({
      event: 'channel.data',
      channel: pushedAgain.channel,
      hex: '07',
    });
    expect(peers.client.events.filter({ event: 'reconnect.incoming', reconnect })).toHaveLength(2);
  });
});
