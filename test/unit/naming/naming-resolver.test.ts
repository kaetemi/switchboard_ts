/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Port of the Dart `naming_resolver_test.dart`.

import { afterEach, beforeEach, describe, expect, test } from 'vitest';
import { ServiceAddress } from '../../../src/core/address/index.ts';
import { Name } from '../../../src/core/name.ts';
import { NamingClient } from '../../../src/core/naming/naming-client.ts';
import { NamingResolver } from '../../../src/core/naming/naming-resolver.ts';
import { StatusCode } from '../../../src/core/status.ts';
import { ManualClock } from '../../../src/testing/manual-clock.ts';
import {
  Connector,
  describeEvent,
  Harness,
  ms10,
  ms50,
  pump,
  reconnectDelay,
  register,
  rejectsWith,
  timeout,
  until,
  uriA,
  uriB,
  within,
} from '../../support/naming/harness.ts';

describe('NamingResolver', () => {
  let h: Harness;
  let connector: Connector;
  let client: NamingClient;
  let resolver: NamingResolver;

  beforeEach(() => {
    h = new Harness();
    connector = new Connector(h);
    client = new NamingClient(connector.call, { reconnectDelay });
    resolver = new NamingResolver(client, { resolveTimeout: ms50 });
  });

  afterEach(async () => {
    await resolver.close();
    await h.close();
  });

  test('is a Resolver', () => {
    expect(typeof resolver.resolve).toBe('function');
    expect(typeof resolver.events[Symbol.asyncIterator]).toBe('function');
    expect(typeof resolver.close).toBe('function');
    expect(typeof resolver.connectTo).toBe('function');
  });

  test('resolves after sync', async () => {
    const [raw] = h.link();
    const npc = await register(raw, 'npc', { endpoints: [uriA] });
    await register(raw, 'api');
    const own = client.register(Name.of('npc'), [uriB]);
    // Resolution started before the client even connects waits for sync.
    const patient = new NamingResolver(client, { resolveTimeout: timeout });
    const early = patient.resolve(Name.of('npc'));
    void client.start();
    const records = await early;
    expect(records.map((r) => r.address.toString())).toEqual([
      new ServiceAddress(Name.of('npc'), npc).toString(),
      new ServiceAddress(Name.of('npc'), await own).toString(),
    ]);
    expect(records[0]?.endpoints).toEqual([uriA]);
    expect(await resolver.resolve(Name.of('none'))).toEqual([]);
    await resolver.ready;
  });

  test('UNAVAILABLE if never synced within the resolve timeout', async () => {
    connector.down = true;
    void client.start();
    await rejectsWith(resolver.resolve(Name.of('npc')), StatusCode.unavailable);
    // Recovers once the service is reachable.
    connector.down = false;
    await within(resolver.ready);
    expect(await resolver.resolve(Name.of('npc'))).toEqual([]);
  });

  test('serves the stale table while resyncing after a loss', async () => {
    await client.start();
    await client.register(Name.of('npc'), [uriA]);
    expect(await resolver.resolve(Name.of('npc'))).toHaveLength(1);
    connector.down = true;
    await connector.servers.at(-1)?.close();
    await until(() => !client.isSynced);
    // Synced once before: the mirrored table keeps answering, unblocked.
    expect(await resolver.resolve(Name.of('npc'))).toHaveLength(1);
    connector.down = false;
    await within(resolver.ready);
    expect(await resolver.resolve(Name.of('npc'))).toHaveLength(1);
  });

  test('serves the stale table after a loss even if never used', async () => {
    await client.start();
    await client.register(Name.of('npc'), [uriA]);
    await within(client.synced);
    connector.down = true;
    await connector.servers.at(-1)?.close();
    await until(() => !client.isSynced);
    // Nothing was resolved before the loss; the table was synced, so it is
    // served without waiting for the resolve timeout.
    expect(await within(resolver.resolve(Name.of('npc')), ms10 * 4)).toHaveLength(1);
  });

  test('a resolver made after a loss serves the stale table', async () => {
    await client.start();
    await client.register(Name.of('npc'), [uriA]);
    await within(client.synced);
    connector.down = true;
    await connector.servers.at(-1)?.close();
    await until(() => !client.isSynced);
    const afterLoss = new NamingResolver(client, { resolveTimeout: timeout });
    expect(await within(afterLoss.resolve(Name.of('npc')), ms10 * 4)).toHaveLength(1);
    await within(afterLoss.ready, ms10 * 4);
  });

  test('before any sync: waits, bounded, then UNAVAILABLE', async () => {
    connector.down = true;
    void client.start();
    const started = performance.now();
    await rejectsWith(resolver.resolve(Name.of('npc')), StatusCode.unavailable);
    const elapsed = performance.now() - started;
    // Timers may fire up to a millisecond early on some runtimes.
    expect(elapsed).toBeGreaterThanOrEqual(ms50 - 1);
    expect(elapsed).toBeLessThan(timeout);
  });

  test('ready completes on the first sync and stays complete', async () => {
    let ready = false;
    void resolver.ready.then(() => {
      ready = true;
    });
    await pump();
    expect(ready).toBe(false);
    await client.start();
    await within(client.synced);
    await pump();
    expect(ready).toBe(true);
    connector.down = true;
    await connector.servers.at(-1)?.close();
    await until(() => !client.isSynced);
    // The stale table is served; ready does not wait for the next sync.
    await within(resolver.ready, ms10 * 4);
  });

  test('ready fails with CANCELLED if closed before the first sync', async () => {
    connector.down = true;
    void client.start();
    const ready = resolver.ready;
    await until(() => connector.calls >= 2);
    await resolver.close();
    await rejectsWith(ready, StatusCode.cancelled);
  });

  test('events pass through', async () => {
    const events: string[] = [];
    void (async () => {
      for await (const e of resolver.events) events.push(describeEvent(e));
    })();
    await client.start();
    await resolver.ready;
    const [raw] = h.link();
    const id = await register(raw, 'api', { endpoints: [uriB] });
    await raw.close();
    await until(() => events.length === 2);
    expect(events).toEqual([`UP api/${id.toString(16)} ${uriB}`, `DOWN api/${id.toString(16)}`]);
  });

  test('close closes the client and fails later resolves', async () => {
    await client.start();
    await resolver.ready;
    const seen: unknown[] = [];
    const eventsDone = (async () => {
      for await (const e of resolver.events) seen.push(e);
    })();
    await resolver.close();
    expect(client.isClosed).toBe(true);
    expect(client.isConnected).toBe(false);
    await eventsDone;
    expect(seen).toEqual([]);
    await rejectsWith(resolver.resolve(Name.of('npc')), StatusCode.failedPrecondition);
    await until(() => h.service.channelCount === 0);
  });

  test('close stops reconnecting and leaves no timers', async () => {
    const clock = new ManualClock();
    const h = new Harness({ clock });
    const connector = new Connector(h);
    connector.down = true;
    const client = new NamingClient(connector.call, { reconnectDelay: ms50, clock });
    const resolver = new NamingResolver(client, { resolveTimeout: ms50 });
    void client.start();
    let error: unknown;
    resolver.resolve(Name.of('npc')).catch((e: unknown) => {
      error = e;
    });
    await clock.elapseAsync(500);
    await rejectsWith(Promise.reject(error), StatusCode.unavailable);
    const calls = connector.calls;
    expect(calls).toBeGreaterThan(5);
    void resolver.close();
    await pump();
    expect(clock.pendingCount).toBe(0);
    await clock.elapseAsync(1000);
    expect(connector.calls).toBe(calls);
    void h.close();
    await pump();
    expect(clock.pendingCount).toBe(0);
  });
});
