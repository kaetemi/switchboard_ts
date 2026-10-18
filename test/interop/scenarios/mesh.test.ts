/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Mixed-language chains and meshes (proxy_test.dart, relay_test.dart,
// talk_forward_test.dart, integration/naming_mesh_test.dart): a client, an
// intermediary (a proxy, a relay, a forwarding service, a naming service)
// and a backend, each played by either implementation, so that every
// pairing of neighbours and every intermediary is crossed.

import { expect, test } from 'vitest';
import { ChannelAddress } from '../../../src/core/address/channel-address.ts';
import { hexString } from '../../../src/core/bytes.ts';
import { StatusCode } from '../../../src/core/status.ts';
import { wikiBytes } from '../../support/vectors.ts';
import { type ChainPeers, type ChainRole, describeChains } from '../chain.ts';
import type { ChannelIncomingEvent, MessageInfo, PeerEvent, TalkItemEvent } from '../protocol.ts';
import { hex, pattern, sha256 } from './support.ts';

const moved = wikiBytes('test-vectors/sharding-payloads/moved-owner-0x1a2b-epoch-7');

const backend: ChainRole = { name: 'backend', config: () => ({}) };
const client: ChainRole = { name: 'client', config: () => ({ listen: [] }) };

/** A Talk channel from the client to [address] at [role]. */
async function talk(
  peers: ChainPeers,
  role: string,
  address: Record<string, unknown>,
): Promise<{ talk: string; channel: string }> {
  return peers.peer('client').rpc('talk.open', { endpoint: peers.endpoint(role), address });
}

async function request(peers: ChainPeers, t: string, procedure: string, args = {}) {
  return peers.peer('client').rpc<MessageInfo>('talk.request', { talk: t, procedure, ...args });
}

function closedAtClient(peers: ChainPeers, channel: string): Promise<PeerEvent> {
  return peers.peer('client').events.waitFor({ event: 'channel.closed', channel });
}

/** The tests every intermediary passes: Talk end to end through it. */
function endToEnd(peers: ChainPeers, via: string, address: () => Record<string, unknown>): void {
  test('a request and its reply', async () => {
    const { talk: t } = await talk(peers, via, address());
    expect((await request(peers, t, 'UPPER', { text: 'through' })).payload).toBe(hex('THROUGH'));
  });

  test('a stream request keeps its item window end to end', async () => {
    const c = peers.peer('client');
    const { talk: t } = await talk(peers, via, address());
    const { stream } = await c.rpc<{ stream: string }>('talk.stream', {
      talk: t,
      procedure: 'COUNT',
      text: '4',
      window: 2,
    });
    await c.events.waitForCount({ event: 'talk.item', stream }, 2);
    await request(peers, t, 'ECHO', { text: 'barrier' });
    expect(c.events.filter({ event: 'talk.item', stream })).toHaveLength(2);
    await c.rpc('stream.grant', { stream, items: 2 });
    const done = await c.events.waitFor({ event: 'talk.done', stream });
    expect(
      c.events.filter<TalkItemEvent>({ event: 'talk.item', stream }).map((i) => i.payload),
    ).toEqual(['1', '2', '3', '4'].map(hex));
    expect(done).toMatchObject({ items: 4, payload: hex('done') });
  });

  test('bulk payloads both ways', async () => {
    const { talk: t } = await talk(peers, via, address());
    const n = 300_000;
    const [up, down] = await Promise.all([
      request(peers, t, 'SIZE', { patternLength: n }),
      request(peers, t, 'BIG', { text: String(n), digest: true }),
    ]);
    expect(up.payload).toBe(hex(`${n} ${sha256(pattern(n))}`));
    expect(down).toEqual({ procedure: '', bulk: true, size: n, sha256: sha256(pattern(n)) });
  });

  test('an abort response with MOVED, byte for byte', async () => {
    const { talk: t } = await talk(peers, via, address());
    const error = await peers.peer('client').rpcError('talk.request', {
      talk: t,
      procedure: 'STATUS',
      payload: hexString(moved),
    });
    expect(error).toMatchObject({ code: StatusCode.moved, body: hexString(moved.subarray(2)) });
  });

  test('an EXTEND of the backend reaches the client', async () => {
    const { talk: t } = await talk(peers, via, address());
    const reply = await request(peers, t, 'EXTEND', { text: '600', timeoutMs: 250 });
    expect(reply.payload).toBe(hex('extended'));
    expect(
      await peers.peer('client').events.waitFor({ event: 'talk.extend', talk: t }),
    ).toMatchObject({ deadlineMs: 2 * 600 + 5000 });
  });

  test('the backend closing its channel closes the client one with its status', async () => {
    const { talk: t, channel } = await talk(peers, via, address());
    await request(peers, t, 'CLOSE', { text: `${StatusCode.aborted} backend done` });
    expect(await closedAtClient(peers, channel)).toMatchObject({
      code: StatusCode.aborted,
      reason: 'backend done',
    });
  });
}

/** A raw channel from the client through [via] to the backend's raw service. */
async function rawThrough(
  peers: ChainPeers,
  via: string,
  address: Record<string, unknown>,
): Promise<{ channel: string; far: string }> {
  const c = peers.peer('client');
  const { connection } = await c.rpc<{ connection: string }>('connect', {
    endpoint: peers.endpoint(via),
  });
  const { channel } = await c.rpc<{ channel: string }>('open', { connection, address });
  const incoming = await peers
    .peer('backend')
    .events.waitFor<ChannelIncomingEvent>('channel.incoming');
  return { channel, far: incoming.channel };
}

describeChains(
  'proxy',
  {
    roles: [
      backend,
      {
        name: 'proxy',
        config: (p) => ({
          services: [],
          proxy: { endpoint: p.endpoint('backend'), types: ['echo', 'raw'] },
        }),
      },
      client,
    ],
  },
  (peers) => {
    endToEnd(peers, 'proxy', () => ({ type: 'echo' }));

    test('subframes and closes pass through a raw channel', async () => {
      const { channel, far } = await rawThrough(peers, 'proxy', { type: 'raw', payload: '0A' });
      await peers.peer('client').rpc('channel.send', { channel, hex: '01 02' });
      await peers
        .peer('backend')
        .events.waitFor({ event: 'channel.data', channel: far, hex: '01 02' });
      await peers.peer('backend').rpc('channel.send', { channel: far, hex: 'FF' });
      await peers.peer('client').events.waitFor({ event: 'channel.data', channel, hex: 'FF' });
      await peers.peer('client').rpc('channel.close', { channel, code: StatusCode.cancelled });
      expect(
        await peers.peer('backend').events.waitFor({ event: 'channel.closed', channel: far }),
      ).toMatchObject({ code: StatusCode.cancelled });
    });

    test("a backend's CLOSE MOVED reaches the client without its owner and epoch", async () => {
      const { channel, far } = await rawThrough(peers, 'proxy', { type: 'raw' });
      await peers
        .peer('backend')
        .rpc('channel.close', { channel: far, statusHex: hexString(moved) });
      expect(await closedAtClient(peers, channel)).toMatchObject({
        code: StatusCode.moved,
        body: hexString(new Uint8Array(10)),
      });
    });

    test('a type the proxy does not forward is NOT_FOUND', async () => {
      const { channel } = await talk(peers, 'proxy', { type: 'sink' });
      expect(await closedAtClient(peers, channel)).toMatchObject({ code: StatusCode.notFound });
    });
  },
);

describeChains(
  'relay',
  {
    roles: [
      backend,
      {
        name: 'relay',
        config: (p) => ({
          services: [],
          relay: { endpoint: p.endpoint('backend'), types: ['echo', 'raw'] },
        }),
      },
      client,
    ],
  },
  (peers) => {
    /** A `_relay` address carrying the open payload of [inner]. */
    const relayed = (inner: Record<string, unknown>) => ({
      type: '_relay',
      payload: hexString(new ChannelAddress(inner).encode()),
    });

    endToEnd(peers, 'relay', () => relayed({ type: 'echo' }));

    test("a backend's CLOSE MOVED is relayed byte for byte", async () => {
      const { channel, far } = await rawThrough(peers, 'relay', relayed({ type: 'raw' }));
      await peers
        .peer('backend')
        .rpc('channel.close', { channel: far, statusHex: hexString(moved) });
      expect(await closedAtClient(peers, channel)).toMatchObject({
        code: StatusCode.moved,
        body: hexString(moved.subarray(2)),
      });
    });

    test('an inner type the relay knows no instance of is NOT_FOUND', async () => {
      const { channel } = await talk(peers, 'relay', relayed({ type: 'sink' }));
      expect(await closedAtClient(peers, channel)).toMatchObject({ code: StatusCode.notFound });
    });

    test('an inner reserved type, or a malformed inner payload, is INVALID_ARGUMENT', async () => {
      for (const address of [relayed({ type: '_ns' }), { type: '_relay', payload: '80' }]) {
        const { channel } = await talk(peers, 'relay', address);
        expect(await closedAtClient(peers, channel)).toMatchObject({
          code: StatusCode.invalidArgument,
        });
      }
    });
  },
);

describeChains(
  'forward',
  {
    roles: [
      backend,
      {
        name: 'forwarder',
        config: (p) => ({
          services: [
            {
              type: 'fwd',
              behaviour: 'forward',
              target: { endpoint: p.endpoint('backend'), type: 'echo', instance: 1 },
            },
          ],
        }),
      },
      client,
    ],
  },
  (peers) => {
    endToEnd(peers, 'forwarder', () => ({ type: 'fwd' }));
  },
);

describeChains(
  'naming mesh',
  {
    roles: [
      { name: 'naming', config: () => ({ naming: {}, services: [] }) },
      {
        name: 'backend',
        config: () => ({
          services: [{ type: 'echo', behaviour: 'echo', instance: 1, anyInstance: true }],
        }),
      },
      client,
    ],
  },
  (peers) => {
    /** The backend registers its echo service with the naming service. */
    async function publish(): Promise<{ naming: string; instance: number }> {
      const b = peers.peer('backend');
      const { naming } = await b.rpc<{ naming: string }>('naming.open', {
        endpoint: peers.endpoint('naming'),
      });
      const { instance } = await b.rpc<{ instance: number }>('naming.register', {
        naming,
        type: 'echo',
      });
      return { naming, instance };
    }

    /** A naming client of the client, synced. */
    async function consumer(): Promise<string> {
      const c = peers.peer('client');
      const { naming } = await c.rpc<{ naming: string }>('naming.open', {
        endpoint: peers.endpoint('naming'),
      });
      await c.rpc('naming.synced', { naming });
      return naming;
    }

    test("the client resolves the backend's service through the naming service", async () => {
      const { instance } = await publish();
      const n = await consumer();
      expect(
        await peers
          .peer('client')
          .events.waitFor({ event: 'naming.event', naming: n, type: 'echo', up: true }),
      ).toMatchObject({ instance, endpoints: [peers.endpoint('backend')] });
      const { talk: t } = await peers
        .peer('client')
        .rpc<{ talk: string }>('naming.channel', { naming: n, address: { type: 'echo' } });
      expect((await request(peers, t, 'UPPER', { text: 'meshed' })).payload).toBe(hex('MESHED'));
    });

    test('the backend leaving is seen by the client, and its type no longer resolves', async () => {
      const { naming } = await publish();
      const n = await consumer();
      const c = peers.peer('client');
      await c.events.waitFor({ event: 'naming.event', naming: n, type: 'echo', up: true });
      await peers.peer('backend').rpc('naming.close', { naming });
      await c.events.waitFor({ event: 'naming.event', naming: n, type: 'echo', up: false });
      const error = await c.rpcError('naming.channel', { naming: n, address: { type: 'echo' } });
      expect(error).toMatchObject({ kind: 'status', code: StatusCode.notFound });
    });
  },
);
