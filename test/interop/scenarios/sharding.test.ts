/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Sharding across the pairings (scenario 21; sharding_test.dart,
// sharding_client_test.dart, slot_routing_test.dart): the server peer
// serves the naming service, the client peer's naming clients are the
// sharded instances and the router. A Dart naming service allocates,
// migrates and rolls back for TS instances, and the reverse.
//
// The instances register the server's endpoint, where a `raw` service
// takes every instance of the routed types: a test sees each channel a
// router opens there (`channel.incoming`, with the instance and shard it
// was addressed to) and answers it, greeting it or rejecting it with
// `MOVED` or `RELOCATED`, as the backends of slot_routing_test.dart do.
//
// The naming service lives as long as the cell, so each test uses types of
// its own.

import { expect, test } from 'vitest';
import { concatBytes, hexString } from '../../../src/core/bytes.ts';
import { MovedStatus } from '../../../src/core/naming/naming-protocol.ts';
import { StatusCode } from '../../../src/core/status.ts';
import { wikiBytes } from '../../support/vectors.ts';
import { type CellPeers, describeCells } from '../matrix.ts';
import { le, name8, utf8 } from '../probe/raw.ts';
import type { ChannelIncomingEvent, MessageInfo, PeerEvent } from '../protocol.ts';
import { hex, nextSubframe } from './support.ts';

/** The types slot-routed channels are opened to; the server's `raw` service takes all their instances. */
const routed = ['rt-own', 'rt-free', 'rt-lazy', 'rt-mv', 'rt-ask', 'rt-tbl', 'rt-none', 'rt-once'];
const routedToo = ['rt-reloc', 'rt-sent', 'rt-recv'];

/** The Talk test's type: `raw` at instances 901 and 902. */
const talkType = 'rt-talk';

/** How long the naming service waits for an instance's answer: what a refused LOCATE takes. */
const handoverTimeoutMs = 2000;

/** The state, owner and epoch `naming.locate` answers. */
interface Located {
  readonly state: string;
  readonly owner: number;
  readonly epoch: number;
}

/** A naming client of the client peer, synced with the server's naming service. */
async function naming(peers: CellPeers): Promise<string> {
  const { naming: n } = await peers.client.rpc<{ naming: string }>('naming.open', {
    endpoint: peers.endpoint,
  });
  await peers.client.rpc('naming.synced', { naming: n });
  return n;
}

/** Registers [n] as an instance of [type] at the server's endpoint: its id. */
async function register(peers: CellPeers, n: string, type: string, instance = 0): Promise<number> {
  const r = await peers.client.rpc<{ instance: number }>('naming.register', {
    naming: n,
    type,
    instance,
    endpoints: [peers.endpoint],
  });
  return r.instance;
}

/** Defines [type]'s slot space from [n]. */
async function define(
  peers: CellPeers,
  n: string,
  type: string,
  space: { count: number; mode?: string; lazy?: boolean; capacity?: number },
): Promise<void> {
  await peers.client.rpc('naming.defineSlots', { naming: n, type, mode: 'managed', ...space });
}

/** An instance of [type] with its space defined: its naming client and id. */
async function instance(
  peers: CellPeers,
  type: string,
  space: { count: number; mode?: string; lazy?: boolean; capacity?: number },
): Promise<{ n: string; id: number }> {
  const n = await naming(peers);
  const id = await register(peers, n, type);
  await define(peers, n, type, space);
  return { n, id };
}

/** Where [n]'s naming service says [slot] of [type] is. */
function locate(peers: CellPeers, n: string, type: string, slot: number): Promise<Located> {
  return peers.client.rpc<Located>('naming.locate', { naming: n, type, slot });
}

/** The slot requests [n]'s handler got for [type], as `[request, slot, epoch, to, holder]`. */
function requests(peers: CellPeers, n: string, type: string): unknown[][] {
  return peers.client.events
    .filter({ event: 'naming.slot', naming: n, type })
    .map((e) => [e['request'], e['slot'], e['epoch'], e['to'], e['holder']]);
}

/** Waits for [n]'s handler to get [request] for [slot] of [type]. */
function request(
  peers: CellPeers,
  n: string,
  type: string,
  request: string,
  slot: number,
): Promise<PeerEvent> {
  return peers.client.events.waitFor({ event: 'naming.slot', naming: n, type, request, slot });
}

/** The phases `naming.phase` reported for [slot] of [type] from [n]. */
function phases(peers: CellPeers, n: string, type: string, slot: number): unknown[] {
  return peers.client.events
    .filter({ event: 'naming.phase', naming: n, type, slot })
    .map((e) => e['phase']);
}

/** A refusal `naming.slotAnswers` takes. */
const refusal = (code: StatusCode, reason: string) => ({ code, reason });

/**
 * A `MOVED` (or with [relocated] `RELOCATED`) status naming [owner] at
 * [epoch]: its payload as `channel.close` takes it, and its body (the bytes
 * after the code) as `channel.closed` reports it.
 */
function moved(owner: number, epoch: number, relocated = false): { hex: string; body: string } {
  const bytes = new MovedStatus({ owner, epoch }).toStatus({ relocated }).encode();
  return { hex: hexString(bytes), body: hexString(bytes.subarray(2)) };
}

/** The [index]th (from 1) channel the server's `raw` service got for [type]. */
async function incoming(peers: CellPeers, type: string, index = 1): Promise<ChannelIncomingEvent> {
  const all = await peers.server.events.waitForCount<ChannelIncomingEvent>(
    { event: 'channel.incoming', type },
    index,
  );
  return all[index - 1] as ChannelIncomingEvent;
}

/**
 * A static space of [type] (8 slots) with instance `a` owning slot 3, a
 * registered instance `b` and a router `r` that saw the claim.
 */
async function claimed(
  peers: CellPeers,
  type: string,
): Promise<{ a: string; ia: number; b: string; ib: number; r: string; epoch: number }> {
  const c = peers.client;
  const a = await instance(peers, type, { count: 8, mode: 'static', capacity: 0 });
  const b = await instance(peers, type, { count: 8, mode: 'static', capacity: 0 });
  const { epoch } = await c.rpc<{ epoch: number }>('naming.claim', { naming: a.n, type, slot: 3 });
  const r = await naming(peers);
  await c.events.waitFor({ event: 'naming.table', naming: r, type, slot: 3, owner: a.id });
  return { a: a.n, ia: a.id, b: b.n, ib: b.id, r, epoch };
}

/** A router's slot channel to [slot] of [type]: its handle. */
async function slotChannel(
  peers: CellPeers,
  r: string,
  type: string,
  slot: number,
  payload?: string,
) {
  const { slotChannel: sc } = await peers.client.rpc<{ slotChannel: string }>(
    'naming.slotChannel',
    {
      naming: r,
      type,
      slot,
      ...(payload === undefined ? {} : { payload }),
    },
  );
  return sc;
}

/** The end of the slot channel [sc] at the router: `[code, body]`. */
async function ended(peers: CellPeers, sc: string): Promise<[unknown, unknown]> {
  const e = await peers.client.events.waitFor({ event: 'channel.closed', channel: sc });
  return [e['code'], e['body']];
}

describeCells(
  'sharding',
  {
    server: {
      naming: { handoverTimeoutMs },
      services: [
        { type: 'echo', behaviour: 'echo', instance: 1 },
        ...[...routed, ...routedToo].map((type) => ({
          type,
          behaviour: 'raw' as const,
          instance: 1,
          anyInstance: true,
        })),
        { type: talkType, behaviour: 'raw', instance: 901 },
        { type: talkType, behaviour: 'raw', instance: 902 },
      ],
    },
  },
  (peers) => {
    test('lazy: the first LOCATE assigns a slot, by spare capacity', async () => {
      const t = 'lz-cap';
      const a = await instance(peers, t, { count: 64, lazy: true, capacity: 1 });
      const b = await instance(peers, t, { count: 64, lazy: true, capacity: 3 });
      expect(a.id).toBeLessThan(b.id);
      const r = await naming(peers);
      const located: Located[] = [];
      for (let s = 0; s < 8; s++) {
        located.push(await locate(peers, r, t, s));
      }
      expect(located.map((l) => l.state)).toEqual(Array(8).fill('owned'));
      expect(located.map((l) => l.epoch)).toEqual(Array(8).fill(1));
      // As sharding_test.dart has it for instances 1 (capacity 1) and 2 (3).
      const [x, y] = [a.id, b.id];
      expect(located.map((l) => l.owner)).toEqual([y, x, y, y, y, x, y, y]);
      // Each slot was assigned once, to its owner, fresh.
      expect(requests(peers, a.n, t)).toEqual([1, 5].map((s) => ['assign', s, 1, null, 0]));
      expect(requests(peers, b.n, t)).toEqual(
        [0, 2, 3, 4, 6, 7].map((s) => ['assign', s, 1, null, 0]),
      );
      // Located again: the same answer, no new assignment.
      expect(await locate(peers, r, t, 0)).toEqual({ state: 'owned', owner: y, epoch: 1 });
      expect(requests(peers, b.n, t)).toHaveLength(6);
      expect(
        await peers.client.rpcError('naming.locate', { naming: r, type: t, slot: 64 }),
      ).toMatchObject({ kind: 'status', code: StatusCode.notFound });
    });

    test('the holder of a slot gets it back before spare capacity', async () => {
      const t = 'lz-hold';
      const a = await instance(peers, t, { count: 64, lazy: true, capacity: 1 });
      const b = await instance(peers, t, { count: 64, lazy: true, capacity: 10 });
      expect(
        await peers.client.rpc('naming.declareHolding', { naming: a.n, type: t, slots: [20] }),
      ).toEqual({ discard: [] });
      const r = await naming(peers);
      expect(await locate(peers, r, t, 20)).toEqual({ state: 'owned', owner: a.id, epoch: 1 });
      expect(await locate(peers, r, t, 21)).toEqual({ state: 'owned', owner: b.id, epoch: 1 });
      expect(requests(peers, a.n, t)).toEqual([['assign', 20, 1, null, a.id]]);
      expect(requests(peers, b.n, t)).toEqual([['assign', 21, 1, null, 0]]);
    });

    test('capacity 0 takes no slots from LOCATE or the allocator', async () => {
      const t = 'cap0-l';
      const a = await instance(peers, t, { count: 16, lazy: true, capacity: 0 });
      expect(await locate(peers, a.n, t, 5)).toEqual({ state: 'free', owner: 0, epoch: 0 });
      const b = await instance(peers, t, { count: 16, lazy: true, capacity: 1 });
      expect(await locate(peers, a.n, t, 5)).toEqual({ state: 'owned', owner: b.id, epoch: 1 });
      // Eager: everything goes to the instance with capacity.
      const e = 'cap0-e';
      const c = await instance(peers, e, { count: 4, capacity: 0 });
      const d = await instance(peers, e, { count: 4, capacity: 1 });
      await peers.client.events.waitForCount({ event: 'naming.slot', naming: d.n, type: e }, 4);
      for (let s = 0; s < 4; s++) {
        expect(await locate(peers, c.n, e, s)).toMatchObject({ state: 'owned', owner: d.id });
      }
      expect(requests(peers, c.n, e)).toEqual([]);
    });

    test('eager: every slot is assigned, in proportion to capacity', async () => {
      const t = 'eager';
      const a = await instance(peers, t, { count: 8, capacity: 1 });
      const b = await instance(peers, t, { count: 8, capacity: 3 });
      // Within the settle window both get their share at once; a late
      // second instance gets it one migration at a time: 6 ASSIGNs either way.
      await peers.client.events.waitForCount(
        { event: 'naming.slot', naming: b.n, type: t, request: 'assign' },
        6,
      );
      const owners: number[] = [];
      for (let s = 0; s < 8; s++) {
        const l = await locate(peers, a.n, t, s);
        expect(l.state).toBe('owned');
        owners.push(l.owner);
      }
      expect(owners.filter((o) => o === a.id)).toHaveLength(2);
      expect(owners.filter((o) => o === b.id)).toHaveLength(6);
    });

    test('an ASSIGN refused fails LOCATE with UNAVAILABLE; the slot stays free', async () => {
      const t = 'lz-ref';
      const a = await instance(peers, t, { count: 16, lazy: true, capacity: 1 });
      await peers.client.rpc('naming.slotAnswers', {
        naming: a.n,
        assign: refusal(StatusCode.unavailable, 'not ready'),
      });
      const r = await naming(peers);
      const error = await peers.client.rpcError('naming.locate', { naming: r, type: t, slot: 3 });
      expect(error).toMatchObject({ kind: 'status', code: StatusCode.unavailable });
      expect(error.reason).toContain('not ready');
      expect(requests(peers, a.n, t)[0]).toEqual(['assign', 3, 1, null, 0]);
      // The refusing instance waits out its backoff; another takes the
      // slot, as its first owner.
      const b = await instance(peers, t, { count: 16, lazy: true, capacity: 1 });
      expect(await locate(peers, r, t, 3)).toEqual({ state: 'owned', owner: b.id, epoch: 1 });
    });

    test('a hand-over seen from the instances and a router, in protocol order', async () => {
      const t = 'mg-ok';
      const { a, ia, b, ib, r, epoch } = await claimed(peers, t);
      const c = peers.client;
      const { phases: done } = await c.rpc<{ phases: string[] }>('naming.migrate', {
        naming: r,
        type: t,
        slot: 3,
        to: ib,
      });
      expect(done).toEqual(['draining', 'assigning', 'forwarding', 'done']);
      expect(phases(peers, r, t, 3)).toEqual(done);
      expect(requests(peers, a, t)).toEqual([
        ['assign', 3, epoch, null, 0],
        ['drain', 3, epoch + 1, ib, null],
        ['forward', 3, epoch + 1, ib, null],
      ]);
      expect(requests(peers, b, t)).toEqual([['assign', 3, epoch + 1, null, ia]]);
      // Every mirror goes through the migration to the new owner.
      for (const n of [a, b, r]) {
        await c.events.waitFor({ event: 'naming.table', naming: n, type: t, slot: 3, owner: ib });
        const seen = c.events
          .filter({ event: 'naming.table', naming: n, type: t, slot: 3 })
          .map((e) => [e['state'], e['owner'], e['to'], e['holder'], e['epoch']]);
        expect(seen.slice(-2)).toEqual([
          ['migrating', ia, ib, ia, epoch],
          ['owned', ib, 0, ib, epoch + 1],
        ]);
      }
      expect(await c.rpc('naming.slotOwner', { naming: r, type: t, slot: 3 })).toEqual({
        owner: ib,
      });
    });

    test('an ASSIGN refused rolls the hand-over back: RESUME, epoch unchanged', async () => {
      const t = 'mg-asg';
      const { a, ia, b, ib, r, epoch } = await claimed(peers, t);
      const c = peers.client;
      await c.rpc('naming.slotAnswers', {
        naming: b,
        assign: refusal(StatusCode.unavailable, 'not ready'),
      });
      const error = await c.rpcError('naming.migrate', { naming: r, type: t, slot: 3, to: ib });
      expect(error).toMatchObject({ kind: 'status', code: StatusCode.unavailable });
      expect(phases(peers, r, t, 3)).toEqual(['draining', 'assigning', 'rolledBack']);
      await request(peers, a, t, 'resume', 3);
      expect(requests(peers, a, t)).toEqual([
        ['assign', 3, epoch, null, 0],
        ['drain', 3, epoch + 1, ib, null],
        ['resume', 3, epoch, null, null],
      ]);
      expect(requests(peers, b, t)).toEqual([['assign', 3, epoch + 1, null, ia]]);
      expect(await locate(peers, r, t, 3)).toEqual({ state: 'owned', owner: ia, epoch });
    });

    test('a DRAIN refused rolls the hand-over back: RESUME, nothing assigned', async () => {
      const t = 'mg-drn';
      const { a, ia, b, ib, r, epoch } = await claimed(peers, t);
      const c = peers.client;
      await c.rpc('naming.slotAnswers', { naming: a, drain: refusal(StatusCode.aborted, 'busy') });
      const error = await c.rpcError('naming.migrate', { naming: r, type: t, slot: 3, to: ib });
      expect(error).toMatchObject({ kind: 'status', code: StatusCode.unavailable });
      expect(phases(peers, r, t, 3)).toEqual(['draining', 'rolledBack']);
      await request(peers, a, t, 'resume', 3);
      expect(requests(peers, a, t)).toEqual([
        ['assign', 3, epoch, null, 0],
        ['drain', 3, epoch + 1, ib, null],
        ['resume', 3, epoch, null, null],
      ]);
      expect(requests(peers, b, t)).toEqual([]);
      expect(await locate(peers, r, t, 3)).toEqual({ state: 'owned', owner: ia, epoch });
    });

    test('the old owner going down mid-hand-over frees the slot', async () => {
      const t = 'mg-old';
      const { a, b, ib, r, epoch } = await claimed(peers, t);
      const c = peers.client;
      await c.rpc('naming.slotAnswers', { naming: a, drain: 'wait' });
      const migration = c.rpcError('naming.migrate', { naming: r, type: t, slot: 3, to: ib });
      await request(peers, a, t, 'drain', 3);
      await c.rpc('naming.close', { naming: a });
      expect(await migration).toMatchObject({ kind: 'status', code: StatusCode.unavailable });
      expect(phases(peers, r, t, 3)).toEqual(['draining', 'rolledBack']);
      expect(await locate(peers, r, t, 3)).toEqual({ state: 'free', owner: 0, epoch });
      expect(requests(peers, b, t)).toEqual([]);
    });

    test('the new owner going down mid-ASSIGN: RESUME to the old', async () => {
      const t = 'mg-new';
      const { a, ia, b, ib, r, epoch } = await claimed(peers, t);
      const c = peers.client;
      await c.rpc('naming.slotAnswers', { naming: b, assign: 'wait' });
      const migration = c.rpcError('naming.migrate', { naming: r, type: t, slot: 3, to: ib });
      await request(peers, b, t, 'assign', 3);
      await c.rpc('naming.close', { naming: b });
      expect(await migration).toMatchObject({ kind: 'status', code: StatusCode.unavailable });
      await request(peers, a, t, 'resume', 3);
      expect(requests(peers, a, t)).toEqual([
        ['assign', 3, epoch, null, 0],
        ['drain', 3, epoch + 1, ib, null],
        ['resume', 3, epoch, null, null],
      ]);
      expect(await locate(peers, r, t, 3)).toEqual({ state: 'owned', owner: ia, epoch });
    });

    test('one hand-over per space at a time; the others queue', async () => {
      const t = 'mg-q';
      const { a, ib, r, epoch } = await claimed(peers, t);
      const c = peers.client;
      const second = await c.rpc<{ epoch: number }>('naming.claim', {
        naming: a,
        type: t,
        slot: 2,
      });
      await c.rpc('naming.slotAnswers', { naming: a, drain: 'wait' });
      const first = c.rpc<{ phases: string[] }>('naming.migrate', {
        naming: r,
        type: t,
        slot: 3,
        to: ib,
      });
      await request(peers, a, t, 'drain', 3);
      const queued = c.rpc<{ phases: string[] }>('naming.migrate', {
        naming: r,
        type: t,
        slot: 2,
        to: ib,
      });
      // The same slot again, running or queued: already migrating.
      for (const slot of [3, 2]) {
        expect(
          await c.rpcError('naming.migrate', { naming: r, type: t, slot, to: ib }),
        ).toMatchObject({ kind: 'status', code: StatusCode.failedPrecondition });
      }
      expect(phases(peers, r, t, 2)).toEqual([]);
      expect(requests(peers, a, t).filter((q) => q[1] === 2)).toEqual([
        ['assign', 2, second.epoch, null, 0],
      ]);
      await c.rpc('naming.slotAnswers', { naming: a, drain: 'ok' });
      await c.rpc('naming.slotRelease', { naming: a, type: t, slot: 3, request: 'drain' });
      expect((await first).phases).toEqual(['draining', 'assigning', 'forwarding', 'done']);
      expect((await queued).phases).toEqual(['draining', 'assigning', 'forwarding', 'done']);
      expect(requests(peers, a, t).filter((q) => q[0] !== 'assign')).toEqual([
        ['drain', 3, epoch + 1, ib, null],
        ['forward', 3, epoch + 1, ib, null],
        ['drain', 2, second.epoch + 1, ib, null],
        ['forward', 2, second.epoch + 1, ib, null],
      ]);
    });

    test('MIGRATE refusals', async () => {
      const t = 'mg-ref';
      const { b, ia, ib, r } = await claimed(peers, t);
      const c = peers.client;
      const migrate = async (type: string, slot: number, to: number) =>
        (await c.rpcError('naming.migrate', { naming: r, type, slot, to })).code;
      expect(await migrate('mg-nope', 3, ib)).toBe(StatusCode.notFound);
      expect(await migrate(t, 8, ib)).toBe(StatusCode.notFound);
      // To its owner, to an instance not registered, of a free slot.
      expect(await migrate(t, 3, ia)).toBe(StatusCode.failedPrecondition);
      expect(await migrate(t, 3, 99_999)).toBe(StatusCode.failedPrecondition);
      expect(await migrate(t, 4, ib)).toBe(StatusCode.failedPrecondition);
      // Nobody with capacity for the allocator to choose.
      expect(await migrate(t, 3, 0)).toBe(StatusCode.failedPrecondition);
      // The allocator chooses among the instances with capacity.
      await define(peers, b, t, { count: 8, mode: 'static', capacity: 1 });
      await c.rpc('naming.migrate', { naming: r, type: t, slot: 3, to: 0 });
      expect(await locate(peers, r, t, 3)).toMatchObject({ state: 'owned', owner: ib });
    });

    test('a slot routes to its owner, which resolveSlotOwner names', async () => {
      const t = 'rt-own';
      const { ia, r } = await claimed(peers, t);
      const c = peers.client;
      expect(await c.rpc('naming.slotOwner', { naming: r, type: t, slot: 3 })).toEqual({
        owner: ia,
      });
      const sc = await slotChannel(peers, r, t, 3, hex('token'));
      const first = await incoming(peers, t);
      expect(first).toMatchObject({ instance: ia, shard: 3, payload: hex('token') });
      await peers.server.rpc('channel.send', { channel: first.channel, hex: hex('A 3') });
      expect(await c.events.waitFor({ event: 'channel.data', channel: sc })).toMatchObject({
        hex: hex('A 3'),
      });
      await c.rpc('slot.send', { slotChannel: sc, hex: hex('x') });
      expect(
        await peers.server.events.waitFor({ event: 'channel.data', channel: first.channel }),
      ).toMatchObject({ hex: hex('x') });
      expect(await c.rpc('slot.state', { slotChannel: sc })).toEqual({ retried: false });
      await c.rpc('slot.close', { slotChannel: sc });
      expect(await ended(peers, sc)).toEqual([0, '']);
    });

    test('a free slot: UNAVAILABLE in a static space, located in a managed one', async () => {
      const t = 'rt-free';
      const c = peers.client;
      await instance(peers, t, { count: 4, mode: 'static', capacity: 0 });
      const r = await naming(peers);
      for (const op of ['naming.slotOwner', 'naming.slotChannel']) {
        expect(await c.rpcError(op, { naming: r, type: t, slot: 2 })).toMatchObject({
          kind: 'status',
          code: StatusCode.unavailable,
        });
      }
      expect(await c.rpcError('naming.slotOwner', { naming: r, type: t, slot: 4 })).toMatchObject({
        kind: 'status',
        code: StatusCode.outOfRange,
      });
      expect(
        await c.rpcError('naming.slotOwner', { naming: r, type: 'rt-none2', slot: 0 }),
      ).toMatchObject({ kind: 'status', code: StatusCode.notFound });
      // A managed space: the router's open locates the slot, which is
      // assigned on the way.
      const l = 'rt-lazy';
      const a = await instance(peers, l, { count: 4, lazy: true, capacity: 1 });
      await slotChannel(peers, await naming(peers), l, 1);
      expect(await incoming(peers, l)).toMatchObject({ instance: a.id, shard: 1 });
      expect(requests(peers, a.n, l)).toEqual([['assign', 1, 1, null, 0]]);
    });

    test('MOVED before anything was sent: retried once at the owner it names', async () => {
      const t = 'rt-mv';
      const { ib, r, epoch } = await claimed(peers, t);
      const c = peers.client;
      const sc = await slotChannel(peers, r, t, 3, hex('token'));
      const first = await incoming(peers, t);
      await peers.server.rpc('channel.close', {
        channel: first.channel,
        statusHex: moved(ib, epoch + 1).hex,
      });
      // The open payload is sent again, to the owner the rejection names.
      const second = await incoming(peers, t, 2);
      expect(second).toMatchObject({ instance: ib, shard: 3, payload: hex('token') });
      await peers.server.rpc('channel.send', { channel: second.channel, hex: hex('B 3') });
      expect(await c.events.waitFor({ event: 'channel.data', channel: sc })).toMatchObject({
        hex: hex('B 3'),
      });
      expect(await c.rpc('slot.state', { slotChannel: sc })).toEqual({ retried: true });
      // The replacement carries on as the channel.
      await c.rpc('slot.send', { slotChannel: sc, hex: hex('x') });
      expect(
        await peers.server.events.waitFor({ event: 'channel.data', channel: second.channel }),
      ).toMatchObject({ hex: hex('x') });
      expect(c.events.filter({ event: 'channel.closed', channel: sc })).toEqual([]);
    });

    test('MOVED without an owner: the router asks, and follows a hand-over', async () => {
      const t = 'rt-ask';
      const { a, ib, r } = await claimed(peers, t);
      const c = peers.client;
      const sc = await slotChannel(peers, r, t, 3);
      const first = await incoming(peers, t);
      // The slot moves on while the first channel waits.
      await c.rpc('naming.migrate', { naming: a, type: t, slot: 3, to: ib });
      await c.events.waitFor({ event: 'naming.table', naming: r, type: t, slot: 3, owner: ib });
      await peers.server.rpc('channel.close', {
        channel: first.channel,
        statusHex: moved(0, 0).hex,
      });
      expect(await incoming(peers, t, 2)).toMatchObject({ instance: ib, shard: 3 });
      expect(await c.rpc('slot.state', { slotChannel: sc })).toEqual({ retried: true });
    });

    test('MOVED: a newer table entry wins over the owner it names', async () => {
      const t = 'rt-tbl';
      const { a, ia, ib, r, epoch } = await claimed(peers, t);
      const c = peers.client;
      const sc = await slotChannel(peers, r, t, 3);
      const first = await incoming(peers, t);
      await c.rpc('naming.migrate', { naming: a, type: t, slot: 3, to: ib });
      await c.events.waitFor({ event: 'naming.table', naming: r, type: t, slot: 3, owner: ib });
      // Older than the router's table: the table's owner gets the retry.
      await peers.server.rpc('channel.close', {
        channel: first.channel,
        statusHex: moved(ia, epoch).hex,
      });
      expect(await incoming(peers, t, 2)).toMatchObject({ instance: ib, shard: 3 });
      expect(await c.rpc('slot.state', { slotChannel: sc })).toEqual({ retried: true });
    });

    test('MOVED with nowhere else to go is surfaced', async () => {
      const t = 'rt-none';
      const { r } = await claimed(peers, t);
      const sc = await slotChannel(peers, r, t, 3);
      const first = await incoming(peers, t);
      // No owner named, and the naming service still names the rejecting one.
      await peers.server.rpc('channel.close', {
        channel: first.channel,
        statusHex: moved(0, 0).hex,
      });
      expect(await ended(peers, sc)).toEqual([StatusCode.moved, moved(0, 0).body]);
      expect(peers.client.events.filter({ event: 'channel.data', channel: sc })).toEqual([]);
      expect(await peers.client.rpc('slot.state', { slotChannel: sc })).toEqual({ retried: true });
      expect(peers.server.events.filter({ event: 'channel.incoming', type: t })).toHaveLength(1);
    });

    test('MOVED is retried only once', async () => {
      const t = 'rt-once';
      const { ia, ib, r, epoch } = await claimed(peers, t);
      const sc = await slotChannel(peers, r, t, 3);
      const first = await incoming(peers, t);
      await peers.server.rpc('channel.close', {
        channel: first.channel,
        statusHex: moved(ib, epoch + 1).hex,
      });
      const second = await incoming(peers, t, 2);
      expect(second).toMatchObject({ instance: ib });
      await peers.server.rpc('channel.close', {
        channel: second.channel,
        statusHex: moved(ia, epoch + 2).hex,
      });
      expect(await ended(peers, sc)).toEqual([StatusCode.moved, moved(ia, epoch + 2).body]);
      expect(peers.server.events.filter({ event: 'channel.incoming', type: t })).toHaveLength(2);
    });

    test('RELOCATED is never retried; its fields name the owner', async () => {
      const t = 'rt-reloc';
      const { ib, r, epoch } = await claimed(peers, t);
      const sc = await slotChannel(peers, r, t, 3);
      const first = await incoming(peers, t);
      const status = moved(ib, epoch + 1, true);
      await peers.server.rpc('channel.close', { channel: first.channel, statusHex: status.hex });
      expect(await ended(peers, sc)).toEqual([StatusCode.relocated, status.body]);
      expect(await peers.client.rpc('slot.state', { slotChannel: sc })).toEqual({ retried: false });
      expect(peers.server.events.filter({ event: 'channel.incoming', type: t })).toHaveLength(1);
    });

    test('MOVED after the first subframe sent is surfaced, its fields intact', async () => {
      const t = 'rt-sent';
      const { ib, r, epoch } = await claimed(peers, t);
      const sc = await slotChannel(peers, r, t, 3);
      const first = await incoming(peers, t);
      await peers.client.rpc('slot.send', { slotChannel: sc, hex: hex('x') });
      await peers.server.events.waitFor({ event: 'channel.data', channel: first.channel });
      const status = moved(ib, epoch + 1);
      await peers.server.rpc('channel.close', { channel: first.channel, statusHex: status.hex });
      expect(await ended(peers, sc)).toEqual([StatusCode.moved, status.body]);
      expect(await peers.client.rpc('slot.state', { slotChannel: sc })).toEqual({ retried: false });
      expect(peers.server.events.filter({ event: 'channel.incoming', type: t })).toHaveLength(1);
    });

    test('MOVED after the first subframe received is surfaced', async () => {
      const t = 'rt-recv';
      const { ib, r, epoch } = await claimed(peers, t);
      const sc = await slotChannel(peers, r, t, 3);
      const first = await incoming(peers, t);
      await peers.server.rpc('channel.send', { channel: first.channel, hex: hex('A 3') });
      await peers.client.events.waitFor({ event: 'channel.data', channel: sc });
      await peers.server.rpc('channel.close', {
        channel: first.channel,
        statusHex: moved(ib, epoch + 1).hex,
      });
      expect((await ended(peers, sc))[0]).toBe(StatusCode.moved);
      expect(await peers.client.rpc('slot.state', { slotChannel: sc })).toEqual({ retried: false });
      expect(peers.server.events.filter({ event: 'channel.incoming', type: t })).toHaveLength(1);
    });

    test('Talk to a slot: retried before the first request, then answered there', async () => {
      const c = peers.client;
      const a = await naming(peers);
      const b = await naming(peers);
      expect(await register(peers, a, talkType, 901)).toBe(901);
      expect(await register(peers, b, talkType, 902)).toBe(902);
      for (const n of [a, b]) {
        await define(peers, n, talkType, { count: 4, mode: 'static', capacity: 0 });
      }
      const { epoch } = await c.rpc<{ epoch: number }>('naming.claim', {
        naming: a,
        type: talkType,
        slot: 1,
      });
      const r = await naming(peers);
      await c.events.waitFor({ event: 'naming.table', naming: r, type: talkType, owner: 901 });
      const { talk, slotChannel: sc } = await c.rpc<{ talk: string; slotChannel: string }>(
        'naming.slotTalk',
        { naming: r, type: talkType, slot: 1 },
      );
      // 901 rejects, and 902 takes the retry with the same open payload.
      const first = await incoming(peers, talkType);
      expect(first).toMatchObject({ instance: 901, shard: 1 });
      await peers.server.rpc('channel.close', {
        channel: first.channel,
        statusHex: moved(902, epoch + 1).hex,
      });
      // The slot channel is marked retried before it opens the replacement.
      const second = await incoming(peers, talkType, 2);
      expect(second).toMatchObject({ instance: 902, shard: 1, payload: first.payload });
      expect(await c.rpc('slot.state', { slotChannel: sc })).toEqual({ retried: true });
      // The first request goes there, and its answer comes back from there.
      const reply = c.rpc<MessageInfo>('talk.request', { talk, procedure: 'UPPER', text: 'moved' });
      expect(await nextSubframe(peers, second.channel, 0)).toBe(
        hexString(concatBytes([0x03], name8('UPPER'), le(1, 3), utf8('moved'))),
      );
      await peers.server.rpc('channel.send', {
        channel: second.channel,
        hex: hexString(
          wikiBytes('test-vectors/talk-messages/final-response-to-1-no-procedure-payload-ff'),
        ),
      });
      expect(await reply).toEqual({ procedure: '', bulk: false, payload: 'FF' });
    });
  },
);
