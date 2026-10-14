/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// The naming service across the pairings (naming_test.dart,
// integration/naming_mesh_test.dart, sharding_client_test.dart): the server
// peer serves `_ns` instance 1, the client peer's naming clients register,
// watch, resolve through it and use a static slot space. A Dart naming
// service serves TS clients and a TS naming service serves Dart clients.
//
// The naming service lives as long as the cell, so each test uses types of
// its own.

import { expect, test } from 'vitest';
import { StatusCode } from '../../../src/core/status.ts';
import { type CellPeers, describeCells } from '../matrix.ts';
import type { MessageInfo, PeerEvent } from '../protocol.ts';
import { hex } from './support.ts';

/** A naming client of the client peer, synced with the server's naming service. */
async function naming(peers: CellPeers, peer = peers.client): Promise<string> {
  const { naming: n } = await peer.rpc<{ naming: string }>('naming.open', {
    endpoint: peers.endpoint,
  });
  await peer.rpc('naming.synced', { naming: n });
  return n;
}

/** The `naming.event` of [n] for [type] that is UP when [up]. */
function event(peers: CellPeers, n: string, type: string, up: boolean): Promise<PeerEvent> {
  return peers.client.events.waitFor({ event: 'naming.event', naming: n, type, up });
}

describeCells(
  'naming',
  {
    server: {
      naming: {},
      // The echo service also serves the instance the naming service assigns it.
      services: [
        { type: 'echo', behaviour: 'echo', instance: 1, anyInstance: true },
        { type: 'raw', behaviour: 'raw', instance: 1 },
      ],
    },
  },
  (peers) => {
    test('the naming service lists itself at its endpoint', async () => {
      const n = await naming(peers);
      expect(await event(peers, n, '_ns', true)).toMatchObject({
        instance: 1,
        endpoints: [peers.endpoint],
        metadata: '',
      });
    });

    test('a registration gets an id; a requested id in use is ALREADY_EXISTS', async () => {
      const [a, b] = [await naming(peers), await naming(peers)];
      const { instance } = await peers.client.rpc<{ instance: number }>('naming.register', {
        naming: a,
        type: 'reg-a',
        endpoints: ['tcp://10.0.0.5:9101'],
      });
      expect(instance).toBeGreaterThanOrEqual(1);
      const taken = await peers.client.rpcError('naming.register', {
        naming: b,
        type: 'reg-a',
        instance,
        endpoints: ['tcp://10.0.0.6:9101'],
      });
      expect(taken).toMatchObject({ kind: 'status', code: StatusCode.alreadyExists });
      const requested = await peers.client.rpc<{ instance: number }>('naming.register', {
        naming: b,
        type: 'reg-a',
        instance: 900,
        endpoints: ['tcp://10.0.0.6:9101'],
      });
      expect(requested.instance).toBe(900);
    });

    test('a watcher sees the snapshot, then live UP and DOWN', async () => {
      const owner = await naming(peers);
      const { instance } = await peers.client.rpc<{ instance: number }>('naming.register', {
        naming: owner,
        type: 'watch-a',
        endpoints: ['tcp://10.0.0.5:9101', 'ws://10.0.0.5:9102/sb'],
        metadata: '01 02 03',
      });
      const watcher = await naming(peers);
      expect(await event(peers, watcher, 'watch-a', true)).toMatchObject({
        instance,
        endpoints: ['tcp://10.0.0.5:9101', 'ws://10.0.0.5:9102/sb'],
        metadata: '01 02 03',
      });
      const second = await peers.client.rpc<{ instance: number }>('naming.register', {
        naming: owner,
        type: 'watch-b',
        endpoints: [],
      });
      expect(await event(peers, watcher, 'watch-b', true)).toMatchObject({
        instance: second.instance,
        endpoints: [],
      });
      await peers.client.rpc('naming.unregister', { naming: owner, type: 'watch-a', instance });
      expect(await event(peers, watcher, 'watch-a', false)).toMatchObject({ instance });
    });

    test("closing the registering client's channel removes its records", async () => {
      const owner = await naming(peers);
      const watcher = await naming(peers);
      const { instance } = await peers.client.rpc<{ instance: number }>('naming.register', {
        naming: owner,
        type: 'gone-a',
        endpoints: ['tcp://10.0.0.5:9101'],
      });
      await event(peers, watcher, 'gone-a', true);
      await peers.client.rpc('naming.close', { naming: owner });
      expect(await event(peers, watcher, 'gone-a', false)).toMatchObject({ instance });
    });

    test('resolving through the naming service reaches a service registered there', async () => {
      // The server registers its own echo service, at the instance it serves.
      const { naming: own } = await peers.server.rpc<{ naming: string }>('naming.open', {
        endpoint: peers.endpoint,
      });
      await peers.server.rpc('naming.register', { naming: own, type: 'echo' });
      const n = await naming(peers);
      await event(peers, n, 'echo', true);
      const { talk } = await peers.client.rpc<{ talk: string }>('naming.channel', {
        naming: n,
        address: { type: 'echo' },
      });
      const reply = await peers.client.rpc<MessageInfo>('talk.request', {
        talk,
        procedure: 'UPPER',
        text: 'resolved',
      });
      expect(reply.payload).toBe(hex('RESOLVED'));
    });

    test('a type nobody registered does not resolve: NOT_FOUND', async () => {
      const n = await naming(peers);
      const error = await peers.client.rpcError('naming.channel', {
        naming: n,
        address: { type: 'nobody' },
      });
      expect(error).toMatchObject({ kind: 'status', code: StatusCode.notFound });
    });

    test('a static slot space: claim, locate, release', async () => {
      const owner = await naming(peers);
      const other = await naming(peers);
      const { instance } = await peers.client.rpc<{ instance: number }>('naming.register', {
        naming: owner,
        type: 'kv-s',
        endpoints: ['tcp://10.0.0.5:9101'],
      });
      await peers.client.rpc('naming.defineSlots', {
        naming: owner,
        type: 'kv-s',
        count: 16,
        mode: 'static',
      });
      const { epoch } = await peers.client.rpc<{ epoch: number }>('naming.claim', {
        naming: owner,
        type: 'kv-s',
        slot: 3,
      });
      expect(epoch).toBeGreaterThanOrEqual(1);
      expect(
        await peers.client.rpc('naming.locate', { naming: other, type: 'kv-s', slot: 3 }),
      ).toEqual({ state: 'owned', owner: instance, epoch });
      expect(
        await peers.client.rpc('naming.locate', { naming: other, type: 'kv-s', slot: 4 }),
      ).toMatchObject({ state: 'free', owner: 0 });
      // Another instance cannot take an owned slot.
      await peers.client.rpc('naming.register', {
        naming: other,
        type: 'kv-s',
        endpoints: ['tcp://10.0.0.6:9101'],
      });
      await peers.client.rpc('naming.defineSlots', {
        naming: other,
        type: 'kv-s',
        count: 16,
        mode: 'static',
      });
      const refused = await peers.client.rpcError('naming.claim', {
        naming: other,
        type: 'kv-s',
        slot: 3,
      });
      expect(refused).toMatchObject({
        kind: 'status',
        code: StatusCode.alreadyExists,
        reason: `slot kv-s/3 is owned by kv-s/${instance}`,
      });
      await peers.client.rpc('naming.release', { naming: owner, type: 'kv-s', slot: 3 });
      expect(
        await peers.client.rpc('naming.locate', { naming: other, type: 'kv-s', slot: 3 }),
      ).toMatchObject({ state: 'free' });
      const taken = await peers.client.rpc<{ epoch: number }>('naming.claim', {
        naming: other,
        type: 'kv-s',
        slot: 3,
      });
      expect(taken.epoch).toBeGreaterThan(epoch);
    });
  },
);
