/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Port of the Dart `sharding_client_test.dart`: the instance side of
// sharding in NamingClient: the slot handler, the remembered slot state and
// its restoration after a naming service restart, and the mirrored slot
// tables.

import { afterEach, beforeEach, describe, expect, test } from 'vitest';
import { ServiceAddress } from '../../../src/core/address/index.ts';
import { Name } from '../../../src/core/name.ts';
import {
  AssignResult,
  NamingClient,
  type SlotHandler,
  type SlotRequestContext,
  type TalkConnector,
} from '../../../src/core/naming/naming-client.ts';
import {
  AssignRequest,
  DrainRequest,
  ForwardRequest,
  LocateResponse,
  MigrateRequest,
  migrationPhaseName,
  PhaseItem,
  ResumeRequest,
  SlotEntry,
  SlotMode,
  SlotSpace,
  SlotState,
} from '../../../src/core/naming/naming-protocol.ts';
import { NamingResolver } from '../../../src/core/naming/naming-resolver.ts';
import {
  type SlotDrainOptions,
  SlotGate,
  SlotGateState,
  SlotLifecycle,
  type SlotLoadOptions,
} from '../../../src/core/naming/slot-gate.ts';
import type { SlotTable } from '../../../src/core/naming/slot-table.ts';
import { Status, StatusCode, SwitchboardError, statusCodeName } from '../../../src/core/status.ts';
import type { IncomingChannel } from '../../../src/core/switchboard/contract.ts';
import { Switchboard } from '../../../src/core/switchboard/switchboard.ts';
import {
  defaultTalkOptions,
  type TalkOptions,
  talkOptions,
} from '../../../src/core/talk/contract.ts';
import { TalkChannel } from '../../../src/core/talk/talk-channel.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';
import type { FakeFrame, FakeTalkChannel } from '../../support/naming/fake-talk.ts';
import {
  Connector,
  clientOptions,
  delay,
  Harness,
  isRequestFor,
  LossyConnector,
  ms10,
  ms50,
  pump,
  reconnectDelay,
  rejectsWith,
  serverOptions,
  until,
  within,
} from '../../support/naming/harness.ts';
import {
  Instance,
  kv,
  RecordingHandler,
  room,
  ScriptedNaming,
  SwitchableConnector,
  zone,
} from '../../support/naming/sharding-harness.ts';
import { TestClock } from '../../support/naming/test-clock.ts';
import { frameLink } from '../../support/talk/frame-link.ts';

// SlotEntry, SlotSpace and LocateResponse compare by value, as in Dart.
expect.addEqualityTesters([
  (a: unknown, b: unknown): boolean | undefined => {
    for (const [x, y] of [
      [a, b],
      [b, a],
    ]) {
      if (x instanceof SlotEntry || x instanceof SlotSpace || x instanceof LocateResponse) {
        return x.equals(y);
      }
    }
    return undefined;
  },
]);

function staticSpace(type: Name, count: number): SlotSpace {
  return new SlotSpace(type, { count, mode: SlotMode.static });
}

/** Checks that [value] is a `SwitchboardError` of [code] (Dart's `isStatus`). */
function expectStatus(value: unknown, code: StatusCode): void {
  expect(value, `expected a ${statusCodeName(code)} error`).toBeInstanceOf(SwitchboardError);
  expect(statusCodeName((value as SwitchboardError).code)).toBe(statusCodeName(code));
}

/** The names of the phases of a migration, once it is done. */
async function phaseNames(phases: AsyncIterable<PhaseItem>): Promise<string[]> {
  const names: string[] = [];
  for await (const p of phases) {
    names.push(migrationPhaseName(p.phase));
  }
  return names;
}

/** Reads a migration to its end, ignoring its outcome (Dart's `drain().catchError`). */
async function drain(phases: AsyncIterable<PhaseItem>): Promise<void> {
  try {
    for await (const _ of phases) {
      // Read and dropped.
    }
  } catch {
    // Ignored.
  }
}

/** The extension an `EXTEND` frame carries (Dart's `TalkFrame.extension`). */
function extension(frame: FakeFrame): { deadline: number | undefined; renew: number | undefined } {
  if (frame.kind !== 'extend') {
    throw new Error(`not an EXTEND: ${frame.kind}`);
  }
  return { deadline: frame.deadline, renew: frame.renew };
}

/**
 * The Talk defaults with a request limit of [max]: Dart's
 * `TalkOptions(maxOutgoingRequests: max)` (the harness merges a link's
 * options over its client options, so the timeouts are set back here).
 */
function limited(max: number): Partial<TalkOptions> {
  return {
    requestTimeout: defaultTalkOptions.requestTimeout,
    replyTimeout: defaultTalkOptions.replyTimeout,
    maxOutgoingRequests: max,
  };
}

let h: Harness;
let log: string[];
const clients: NamingClient[] = [];
const tearDowns: (() => Promise<void>)[] = [];

/** Runs [action] after the test, before the shared tear-down (Dart's `addTearDown`). */
function addTearDown(action: () => Promise<void>): void {
  tearDowns.push(action);
}

function newClient(connect: TalkConnector): NamingClient {
  const client = new NamingClient(connect, { reconnectDelay });
  clients.push(client);
  return client;
}

/** A started client registered as [type]/[id] with a recording handler. */
async function sharded(
  name: string,
  type: Name,
  id: number,
  options: { connect?: TalkConnector } = {},
): Promise<[NamingClient, RecordingHandler]> {
  const client = newClient(options.connect ?? new Connector(h).call);
  const handler = new RecordingHandler(name, log);
  client.slotHandler = handler;
  await client.start();
  await client.register(type, [], { instance: id });
  await within(client.synced);
  return [client, handler];
}

beforeEach(() => {
  h = new Harness();
  log = [];
});

afterEach(async () => {
  for (const action of tearDowns.splice(0).reverse()) {
    await action();
  }
  for (const client of clients) {
    await client.close();
  }
  clients.length = 0;
  await h.close();
});

test('claim through ASSIGN; the mirror and the resolver follow', async () => {
  const [a] = await sharded('A', zone, 1);
  const router = newClient(new Connector(h).call);
  const resolver = new NamingResolver(router);
  const events: string[] = [];
  router.on('slotEvent', (e) => events.push(`${e.slot}: ${e.entry}`));
  await router.start();
  await within(router.synced);
  expect(router.slotTables.size).toBe(0);
  expect(router.slotOwners(zone)).toEqual(new Set());
  await a.defineSlots(zone, { count: 4, mode: SlotMode.static, capacity: 0 });
  expect(await a.claim(zone, 1)).toBe(1);
  expect(log).toEqual(['A ASSIGN 1 e1 h0']);
  expect(a.servedSlots(zone)).toEqual(new Map([[1, 1]]));
  await until(() => router.slotOwner(zone, 1) !== undefined);
  expect(router.slotTable(zone)?.space).toEqual(staticSpace(zone, 4));
  expect(router.slotOwner(zone, 1)).toEqual(SlotEntry.owned(1, { holder: 1, epoch: 1 }));
  expect(router.slotOwner(zone, 2)).toBeUndefined();
  expect(router.slotOwner(Name.of('nope'), 1)).toBeUndefined();
  expect(router.slotOwners(zone)).toEqual(new Set([1]));
  expect(resolver.slotOwner(zone, 1)?.owner).toBe(1);
  expect(resolver.slotOwners(zone)).toEqual(new Set([1]));
  expect(resolver.slotTable(zone)).toBe(router.slotTable(zone));
  // The client's own mirror sees it too.
  expect(a.slotOwners(zone)).toEqual(new Set([1]));
  await a.release(zone, 1);
  await until(() => router.slotOwner(zone, 1) === undefined);
  expect(a.servedSlots(zone)).toEqual(new Map());
  expect(router.slotOwners(zone)).toEqual(new Set());
  expect(events).toEqual(['1: owned 1 holder 1 epoch 1', '1: free holder 0 epoch 1']);
});

test('migration seen from the instances, in protocol order', async () => {
  const [a] = await sharded('A', zone, 1);
  const [b] = await sharded('B', zone, 2);
  const router = newClient(new Connector(h).call);
  await router.start();
  await within(router.synced);
  await a.defineSlots(zone, { count: 4, mode: SlotMode.static, capacity: 0 });
  await b.defineSlots(zone, { count: 4, mode: SlotMode.static, capacity: 0 });
  await a.claim(zone, 1);
  await until(() => router.slotOwners(zone).size > 0);
  log.length = 0;
  // The old owner learns of the new owner from its own mirror.
  a.on('slotEvent', (e) => log.push(`A sees ${e.slot}: ${e.entry}`));
  const owners: Set<number>[] = [];
  router.on('slotEvent', () => owners.push(router.slotOwners(zone)));
  const phases = await within(phaseNames(router.migrate(zone, 1, { to: 2 })));
  expect(phases).toEqual(['draining', 'assigning', 'forwarding', 'done']);
  expect(log).toEqual([
    'A sees 1: migrating 1->2 holder 1 epoch 1',
    'A DRAIN 1 e2 to2',
    'B ASSIGN 1 e2 h1',
    'A sees 1: owned 2 holder 2 epoch 2',
    'A FORWARD 1 e2 to2',
  ]);
  expect(a.servedSlots(zone)).toEqual(new Map());
  expect(b.servedSlots(zone)).toEqual(new Map([[1, 2]]));
  await until(() => owners.length === 2);
  expect(owners).toEqual([new Set([1]), new Set([2])]);
  expect(router.slotOwners(zone)).toEqual(new Set([2]));
});

test('requests are answered from the handler, or refused', async () => {
  const connector = new Connector(h);
  const client = newClient(connector.call);
  await client.start();
  await within(client.synced);
  const server = connector.servers.at(-1) as FakeTalkChannel;
  const ask = async (procedure: string, payload: Uint8Array): Promise<number[]> =>
    Array.from((await server.request(procedure, payload)).payload);
  const assign = new AssignRequest(zone, 1, { epoch: 3 }).encode();
  await rejectsWith(ask('ASSIGN', assign), StatusCode.unimplemented);
  const handler = new RecordingHandler('A', log);
  client.slotHandler = handler;
  expect(await ask('ASSIGN', assign)).toEqual([]);
  expect(client.servedSlots(zone)).toEqual(new Map([[1, 3]]));
  handler.assign = async () => AssignResult.notHolding;
  expect(await ask('ASSIGN', new AssignRequest(zone, 2, { epoch: 1 }).encode())).toEqual([1]);
  handler.assign = async () => {
    throw SwitchboardError.of(StatusCode.resourceExhausted, 'full');
  };
  await rejectsWith(
    ask('ASSIGN', new AssignRequest(zone, 3, { epoch: 1 }).encode()),
    StatusCode.resourceExhausted,
  );
  handler.assign = async () => {
    throw new Error('broken');
  };
  await rejectsWith(
    ask('ASSIGN', new AssignRequest(zone, 3, { epoch: 1 }).encode()),
    StatusCode.unavailable,
  );
  expect(client.servedSlots(zone)).toEqual(
    new Map([
      [1, 3],
      [2, 1],
    ]),
  );
  await rejectsWith(ask('DRAIN', new Uint8Array(2)), StatusCode.invalidArgument);
  expect(await ask('DRAIN', new DrainRequest(zone, 1, { epoch: 4, to: 9 }).encode())).toEqual([]);
  expect(await ask('RESUME', new ResumeRequest(zone, 1, { epoch: 3 }).encode())).toEqual([]);
  expect(await ask('FORWARD', new ForwardRequest(zone, 2, { epoch: 2, to: 9 }).encode())).toEqual(
    [],
  );
  expect(client.servedSlots(zone)).toEqual(new Map([[1, 3]]));
  await rejectsWith(ask('FROB', new Uint8Array(0)), StatusCode.unimplemented);
  expect(log).toEqual([
    'A ASSIGN 1 e3 h0',
    'A ASSIGN 2 e1 h0',
    'A ASSIGN 3 e1 h0',
    'A ASSIGN 3 e1 h0',
    'A DRAIN 1 e4 to9',
    'A RESUME 1 e3',
    'A FORWARD 2 e2 to9',
  ]);
});

test('a slow ASSIGN that declares its estimate outlasts the timeouts', async () => {
  const [a, handler] = await sharded('A', zone, 1);
  await a.defineSlots(zone, { count: 4, mode: SlotMode.static, capacity: 0 });
  // Longer than the service's handover timeout, the client's reply and
  // request timeouts.
  const slow = 700;
  expect(slow).toBeGreaterThan(talkOptions(serverOptions).replyTimeout * 3);
  handler.assign = (r) => {
    handler.contexts.get(`ASSIGN ${r.slot}`)?.extend({ deadline: 2000 });
    return delay(slow).then(() => AssignResult.holding);
  };
  const started = performance.now();
  expect(await a.claim(zone, 2)).toBe(1);
  // Timers may fire up to a millisecond early on some runtimes.
  expect(performance.now() - started).toBeGreaterThanOrEqual(slow - 1);
  expect(h.service.slotTable(zone)?.get(2).owner).toBe(1);
});

test('managed: capacity 1 by default, served slots tracked', async () => {
  const [a] = await sharded('A', kv, 1);
  await a.defineSlots(kv, { count: 4 });
  await until(() => a.servedSlots(kv).size === 4);
  expect(log).toEqual([
    'A ASSIGN 0 e1 h0',
    'A ASSIGN 1 e1 h0',
    'A ASSIGN 2 e1 h0',
    'A ASSIGN 3 e1 h0',
  ]);
  // A conflicting definition is refused and forgotten.
  await rejectsWith(a.defineSlots(kv, { count: 8 }), StatusCode.failedPrecondition);
  expect(await a.locate(kv, 2)).toEqual(new LocateResponse(SlotState.owned, 1, 1));
  await rejectsWith(a.locate(kv, 9), StatusCode.notFound);
  await rejectsWith(a.claim(Name.of(''), 1), StatusCode.invalidArgument);
  await expect(a.defineSlots(kv, { count: 0 })).rejects.toThrow(RangeError);
});

test('defineSlots and declareHolding wait for the registration', async () => {
  const client = newClient(new Connector(h).call);
  const handler = new RecordingHandler('A', log);
  client.slotHandler = handler;
  const defined = client.defineSlots(zone, { count: 4, mode: SlotMode.static, capacity: 0 });
  const declared = client.declareHolding(zone, [2, 9]);
  const registered = client.register(zone, [], { instance: 1 });
  await client.start();
  expect(await within(registered)).toBe(1);
  await within(defined);
  expect(await within(declared)).toEqual([9]);
  expect(h.service.slotTable(zone)?.get(2)).toEqual(SlotEntry.free({ holder: 1 }));
  // Claimed with the storage it holds.
  expect(await client.claim(zone, 2, { holding: true })).toBe(1);
  expect(log).toEqual(['A ASSIGN 2 e1 h1']);
});

test('a router without a registration defines a space', async () => {
  const router = newClient(new Connector(h).call);
  await router.start();
  await router.defineSlots(Name.of('discord'), { count: 16, mode: SlotMode.static });
  expect(h.service.slotTable(Name.of('discord'))?.count).toBe(16);
  await rejectsWith(router.claim(Name.of('discord'), 1), StatusCode.failedPrecondition);
});

test('naming service restart: resume, re-send, re-claim by epoch', async () => {
  const connectA = new SwitchableConnector(h);
  const connectB = new SwitchableConnector(h);
  const [a, handlerA] = await sharded('A', zone, 1, { connect: connectA.call });
  const [b] = await sharded('B', zone, 2, { connect: connectB.call });
  const router = newClient(new Connector(h).call);
  await router.start();
  for (const client of [a, b]) {
    await client.defineSlots(zone, { count: 4, mode: SlotMode.static, capacity: 0 });
  }
  expect(await a.declareHolding(zone, [3])).toEqual([]);
  await a.claim(zone, 1, { holding: true });
  // The FORWARD to A never arrives: the naming service dies in between,
  // after B confirmed and the table says B.
  connectA.dropToClient = isRequestFor('FORWARD');
  void drain(router.migrate(zone, 1, { to: 2 }));
  await until(() => h.service.slotTable(zone)?.get(1).owner === 2);
  expect(log).toEqual(['A ASSIGN 1 e1 h1', 'A DRAIN 1 e2 to2', 'B ASSIGN 1 e2 h1']);
  expect(a.servedSlots(zone)).toEqual(new Map([[1, 1]]));
  expect(b.servedSlots(zone)).toEqual(new Map([[1, 2]]));
  log.length = 0;
  const restarted = new Harness({ assignmentHold: 100 });
  addTearDown(() => restarted.close());
  connectA.harness = restarted;
  connectA.dropToClient = () => false;
  connectB.harness = restarted;
  await h.close();
  // A unlocks the slot it was draining and serves it again.
  await until(() => log.includes('A RESUME 1 e1'));
  // Both claim it back; B's later epoch wins, A must stop serving it.
  await until(() => handlerA.revoked.length > 0);
  expect(handlerA.revoked).toEqual(['zone/1']);
  expect(log).toEqual(['A RESUME 1 e1', 'B ASSIGN 1 e3 h2', 'A REVOKE 1']);
  // A's HOLDING after the restart listed slot 1, held by B now.
  await until(() => handlerA.discarded.length > 0);
  expect(handlerA.discarded).toEqual(['zone: 1']);
  expect(a.servedSlots(zone)).toEqual(new Map());
  expect(b.servedSlots(zone)).toEqual(new Map([[1, 3]]));
  const table = restarted.service.slotTable(zone) as SlotTable;
  expect(table.space).toEqual(staticSpace(zone, 4));
  expect(table.get(1)).toEqual(SlotEntry.owned(2, { holder: 2, epoch: 3 }));
  // A's declared storage was restored after the claims.
  await until(() => table.get(3).holder === 1);
  expect(table.get(3)).toEqual(SlotEntry.free({ holder: 1 }));
});

test('re-claims served slots after a restart without conflict', async () => {
  const connect = new SwitchableConnector(h);
  const [a] = await sharded('A', kv, 1, { connect: connect.call });
  await a.defineSlots(kv, { count: 2 });
  await until(() => a.servedSlots(kv).size === 2);
  log.length = 0;
  const restarted = new Harness({
    assignmentHold: ms50,
    holdingSettle: Harness.productionSettle,
  });
  addTearDown(() => restarted.close());
  connect.harness = restarted;
  await h.close();
  await until(() => log.length === 2);
  // Claimed back with the holding flag and the last epoch: epoch 2.
  expect(log).toEqual(['A ASSIGN 0 e2 h1', 'A ASSIGN 1 e2 h1']);
  expect(a.servedSlots(kv)).toEqual(
    new Map([
      [0, 2],
      [1, 2],
    ]),
  );
  await until(() => a.isSynced && a.slotOwners(kv).size === 1);
  expect(a.slotOwner(kv, 0)).toEqual(SlotEntry.owned(1, { holder: 1, epoch: 2 }));
});

test('re-claims more served slots than the Talk request limit', async () => {
  const connect = new SwitchableConnector(h);
  const [a] = await sharded('A', kv, 1, { connect: connect.call });
  await a.defineSlots(kv, { count: 1100 });
  await until(() => a.servedSlots(kv).size === 1100);
  log.length = 0;
  const restarted = new Harness({
    assignmentHold: ms50,
    holdingSettle: Harness.productionSettle,
  });
  addTearDown(() => restarted.close());
  connect.harness = restarted;
  await h.close();
  await until(() => log.length === 1100);
  // Every slot comes back to its holder, none starts fresh: the claims
  // within the window keep their epoch (1, now 2); the others reach the
  // service after its hold and come back through HOLDING.
  const assigned = new Map<number, string>();
  for (const line of log) {
    const parts = line.split(' ');
    assigned.set(Number.parseInt(parts[2] as string, 10), parts.slice(3).join(' '));
  }
  expect(new Set(assigned.keys())).toEqual(new Set(Array.from({ length: 1100 }, (_, s) => s)));
  expect(new Set(assigned.values())).toEqual(new Set(['e2 h1', 'e1 h1']));
  expect([...assigned.values()].filter((v) => v === 'e2 h1').length).toBeGreaterThanOrEqual(256);
  await until(() => a.servedSlots(kv).size === 1100);
  expect(restarted.service.slotTable(kv)?.slotsOf(1)).toHaveLength(1100);
});

test('served slots come back to their holder after the hold', async () => {
  const connect = new SwitchableConnector(h);
  const [a] = await sharded('A', kv, 1, { connect: connect.call });
  await a.defineSlots(kv, { count: 8 });
  await until(() => a.servedSlots(kv).size === 8);
  log.length = 0;
  // A restarted naming service whose hold is over (none at all) when the
  // instance is back: SLOTS gives it capacity before its HOLDING is
  // processed, and the settle window keeps the allocator from starting
  // those slots fresh (holder 0) meanwhile.
  const restarted = new Harness({ holdingSettle: Harness.productionSettle });
  addTearDown(() => restarted.close());
  connect.harness = restarted;
  await h.close();
  await until(() => log.length === 8);
  await pump();
  expect(log).toEqual(Array.from({ length: 8 }, (_, s) => `A ASSIGN ${s} e1 h1`));
  expect(restarted.service.slotTable(kv)?.slotsOf(1)).toHaveLength(8);
});

test('HOLDING goes ahead of SLOTS when the mirror shows the space', async () => {
  const sent: string[] = [];
  const lossy = new LossyConnector(h);
  lossy.dropToService = (frame) => {
    if (frame.kind === 'message' && frame.requestId !== 0) {
      const procedure = frame.procedure.toString();
      if (procedure === 'SLOTS' || procedure === 'HOLDING') {
        sent.push(procedure);
      }
    }
    return false;
  };
  const [b] = await sharded('B', kv, 2);
  await b.defineSlots(kv, { count: 4, capacity: 0 });
  const a = newClient(lossy.call);
  a.slotHandler = new RecordingHandler('A', log);
  await a.start();
  await within(a.synced);
  await until(() => a.slotTable(kv) !== undefined);
  // Defined and declared before the registration is back: sent
  // together once it is.
  const registered = a.register(kv, [], { instance: 1 });
  const defined = a.defineSlots(kv, { count: 4 });
  const discard = a.declareHolding(kv, [2]);
  await registered;
  await defined;
  expect(await discard).toEqual([]);
  expect(sent).toEqual(['HOLDING', 'SLOTS']);
  await until(() => log.length === 4);
  expect(log).toContain('A ASSIGN 2 e1 h1');
  // Without the space in the mirror: SLOTS first (HOLDING would fail).
  sent.length = 0;
  const c = newClient(lossy.call);
  c.slotHandler = new RecordingHandler('C', log);
  await c.start();
  await within(c.synced);
  const registeredC = c.register(zone, [], { instance: 3 });
  const definedC = c.defineSlots(zone, { count: 4, capacity: 0 });
  const discardC = c.declareHolding(zone, [1]);
  await registeredC;
  await definedC;
  expect(await discardC).toEqual([]);
  expect(sent).toEqual(['SLOTS', 'HOLDING']);
});

test("with the service's settle window: declared storage first, fresh slots once quiet", async () => {
  // The service's default window, as a fresh deployment has it.
  const h = new Harness({ holdingSettle: Harness.productionSettle });
  addTearDown(() => h.close());
  const [a] = await sharded('A', kv, 1, { connect: new Connector(h).call });
  const started = performance.now();
  // SLOTS and HOLDING back to back, as MeshNode.publishSharded sends them.
  const defined = a.defineSlots(kv, { count: 4 });
  const declared = a.declareHolding(kv, [2]);
  await defined;
  expect(await declared).toEqual([]);
  await until(() => log.length > 0);
  expect(log).toEqual(['A ASSIGN 2 e1 h1']);
  await delay(300);
  expect(log, 'fresh slots wait for the window').toHaveLength(1);
  await delay(800);
  await until(() => log.length === 4);
  // Timers may fire up to a millisecond early on some runtimes.
  expect(performance.now() - started).toBeGreaterThanOrEqual(Harness.productionSettle - 1);
  for (const line of log.slice(1)) {
    expect(line).toMatch(/e1 h0$/);
  }
  expect(new Set(a.servedSlots(kv).keys())).toEqual(new Set([0, 1, 2, 3]));
});

test('discards of a HOLDING sent again after a reconnect reach the handler', async () => {
  const connect = new Connector(h);
  const [a, handlerA] = await sharded('A', zone, 1, { connect: connect.call });
  await a.defineSlots(zone, { count: 4, mode: SlotMode.static, capacity: 0 });
  expect(await a.declareHolding(zone, [2, 3])).toEqual([]);
  expect(handlerA.discarded).toEqual([]);
  // A goes away; meanwhile B takes slot 2 (fetching it from A) and is
  // its holder from then on.
  connect.down = true;
  await connect.servers.at(-1)?.close();
  await until(() => !h.service.table.has(new ServiceAddress(zone, 1).key));
  const b = new Instance(h, zone, 2, log);
  await b.start({ space: staticSpace(zone, 4), capacity: 0 });
  await b.claim(2);
  expect(log).toEqual(['2 ASSIGN 2 e1 h1']);
  expect(h.service.slotTable(zone)?.get(2).holder).toBe(2);
  connect.down = false;
  await until(() => handlerA.discarded.length > 0);
  expect(handlerA.discarded).toEqual(['zone: 2']);
  expect(h.service.slotTable(zone)?.get(3)).toEqual(SlotEntry.free({ holder: 1 }));
});

test('close while a slot is locked resumes it', async () => {
  const [a, handlerA] = await sharded('A', zone, 1);
  const [b, handlerB] = await sharded('B', zone, 2);
  for (const client of [a, b]) {
    await client.defineSlots(zone, { count: 4, mode: SlotMode.static, capacity: 0 });
  }
  await a.claim(zone, 1);
  // B never confirms: the slot stays locked at A.
  handlerB.assign = () => new Deferred<AssignResult>().promise;
  const router = newClient(new Connector(h).call);
  await router.start();
  void drain(router.migrate(zone, 1, { to: 2 }));
  await until(() => log.includes('B ASSIGN 1 e2 h1'));
  expect(handlerA.revoked).toEqual([]);
  await a.close();
  await until(() => log.includes('A RESUME 1 e1'));
  expect(log).toEqual([
    'A ASSIGN 1 e1 h0',
    'A DRAIN 1 e2 to2',
    'B ASSIGN 1 e2 h1',
    'A RESUME 1 e1',
  ]);
});

test('a handler running longer than slotHandlerMaxDuration is answered DEADLINE_EXCEEDED', async () => {
  const clock = new TestClock();
  const h = new Harness({ clock, handoverTimeout: 5000 });
  const log: string[] = [];
  const client = async (id: number, handler: RecordingHandler): Promise<NamingClient> => {
    const c = new NamingClient(new Connector(h).call, {
      reconnectDelay,
      slotHandlerMaxDuration: 1000,
      clock,
    });
    c.slotHandler = handler;
    void c.start();
    void c.register(zone, [], { instance: id });
    await clock.elapse(ms10);
    void c.defineSlots(zone, { count: 4, mode: SlotMode.static, capacity: 0 });
    await clock.elapse(ms10);
    return c;
  };

  const handlerA = new RecordingHandler('A', log);
  const handlerB = new RecordingHandler('B', log);
  const a = await client(1, handlerA);
  const b = await client(2, handlerB);
  // An ASSIGN that never completes in time.
  const loading = new Deferred<AssignResult>();
  handlerA.assign = () => loading.promise;
  let claimed: unknown;
  a.claim(zone, 1).then(
    (e) => {
      claimed = e;
    },
    (e: unknown) => {
      claimed = e;
    },
  );
  await clock.elapse(990);
  expect(claimed).toBeUndefined();
  await clock.elapse(ms50);
  expectStatus(claimed, StatusCode.unavailable);
  expect((claimed as SwitchboardError).status.reason).toContain('deadlineExceeded');
  // Its late outcome is not served.
  loading.resolve(AssignResult.holding);
  await clock.elapse(ms10);
  expect(handlerA.revoked).toEqual(['zone/1']);
  expect(a.servedSlots(zone)).toEqual(new Map());
  // A DRAIN that never completes: the migration is rolled back.
  handlerA.assign = async () => AssignResult.holding;
  void a.claim(zone, 2);
  await clock.elapse(ms10);
  handlerA.drain = () => new Deferred<void>().promise;
  log.length = 0;
  let migrated: unknown;
  void (async () => {
    try {
      for await (const _ of b.migrate(zone, 2, { to: 2 })) {
        // Read and dropped.
      }
      migrated ??= 'done';
    } catch (e) {
      migrated = e;
    }
  })();
  await clock.elapse(1100);
  expect(log).toEqual(['A DRAIN 2 e2 to2', 'A RESUME 2 e1']);
  expectStatus(migrated, StatusCode.unavailable);
  expect(h.service.slotTable(zone)?.get(2).owner).toBe(1);
  void a.close();
  void b.close();
  void h.close();
  await pump();
  await clock.elapse(ms50 * 2);
  expect(clock.pendingCount).toBe(0);
});

test('rejects a negative slotHandlerMaxDuration', () => {
  expect(
    () =>
      new NamingClient(
        async () => {
          throw new Error('unused');
        },
        { slotHandlerMaxDuration: -1000 },
      ),
  ).toThrow(RangeError);
});

test('close leaves no timers while a handler runs', async () => {
  const clock = new TestClock();
  const h = new Harness({ clock });
  const log: string[] = [];
  const client = new NamingClient(new Connector(h).call, { reconnectDelay, clock });
  const handler = new RecordingHandler('A', log);
  handler.assign = () => new Deferred<AssignResult>().promise;
  client.slotHandler = handler;
  void client.start();
  void client.register(zone, [], { instance: 1 });
  await clock.elapse(ms10);
  void client.defineSlots(zone, { count: 2, mode: SlotMode.static, capacity: 0 });
  await clock.elapse(ms10);
  client.claim(zone, 0).catch(() => {});
  await clock.elapse(1000);
  expect(log).toEqual(['A ASSIGN 0 e1 h0']);
  void client.close();
  void h.close();
  await pump();
  await clock.elapse(ms50 * 2);
  expect(clock.pendingCount).toBe(0);
});

test('a definition sent before the registration is sent again with it, with the capacity', async () => {
  const client = newClient(new Connector(h).call);
  client.slotHandler = new RecordingHandler('A', log);
  await client.start();
  await within(client.synced);
  // Not registering kv yet: defines the space only.
  await within(client.defineSlots(kv, { count: 2 }));
  expect(h.service.slotTable(kv)?.count).toBe(2);
  expect(log).toEqual([]);
  await within(client.register(kv, [], { instance: 1 }));
  await until(() => client.servedSlots(kv).size === 2);
  expect(log).toEqual(['A ASSIGN 0 e1 h0', 'A ASSIGN 1 e1 h0']);
});

test('declareHolding of a type this client does not register is sent at once and refused', async () => {
  const router = newClient(new Connector(h).call);
  await router.start();
  await within(router.synced);
  await rejectsWith(within(router.declareHolding(room, [1])), StatusCode.notFound);
  await router.defineSlots(room, { count: 4, mode: SlotMode.static });
  await rejectsWith(within(router.declareHolding(room, [1])), StatusCode.failedPrecondition);
  // Made while disconnected: sent after the connect.
  const later = newClient(new Connector(h).call);
  const declared = later.declareHolding(room, [2]);
  await later.start();
  await rejectsWith(within(declared), StatusCode.failedPrecondition);
  expect(h.service.slotTable(room)?.entries.size).toBe(0);
});

describe("the channel's request limit", () => {
  async function closeWithoutTimers(clock: TestClock, client: NamingClient): Promise<void> {
    void client.close();
    await pump();
    await clock.elapse(ms50 * 2);
    expect(clock.pendingCount).toBe(0);
  }

  test("a router's SLOTS refused for it is sent once it allows", async () => {
    const clock = new TestClock();
    // The WATCH holds the only request the channel takes.
    const naming = new ScriptedNaming({ maxOutgoingRequests: 1, clock });
    const client = new NamingClient(naming.connect, { reconnectDelay, clock });
    void client.start();
    await clock.elapse(ms10);
    expect(naming.log).toEqual(['WATCH']);
    let defined = false;
    void client.defineSlots(room, { count: 4, mode: SlotMode.static, capacity: 0 }).then(() => {
      defined = true;
    });
    await clock.elapse(ms50);
    expect(naming.log).toEqual(['WATCH']);
    expect(defined).toBe(false);
    naming.watch.resolve();
    await clock.elapse(ms50);
    // Sent by itself, once, on the same channel.
    expect(naming.log).toEqual(['WATCH', 'SLOTS room']);
    expect(defined).toBe(true);
    expect(client.isSynced).toBe(true);
    expect(naming.connects).toBe(1);
    await closeWithoutTimers(clock, client);
  });

  test('the slot state is restored in order, what was refused sent later and only once', async () => {
    const clock = new TestClock();
    const naming = new ScriptedNaming({ maxOutgoingRequests: 2, clock });
    const client = new NamingClient(naming.connect, { reconnectDelay, clock });
    client.slotHandler = new RecordingHandler('A', log);
    void client.start();
    await clock.elapse(ms10);
    // Served before the registration, as after a naming service restart.
    for (const slot of [0, 1, 2]) {
      naming.server
        .request('ASSIGN', new AssignRequest(zone, slot, { epoch: 4 }).encode())
        .catch(() => {});
    }
    await clock.elapse(ms10);
    expect(client.servedSlots(zone)).toEqual(
      new Map([
        [0, 4],
        [1, 4],
        [2, 4],
      ]),
    );
    // With the WATCH in flight, one request at a time goes out.
    void client.register(zone, [], { instance: 1 });
    let defined = false;
    void client.defineSlots(zone, { count: 4, mode: SlotMode.static, capacity: 0 }).then(() => {
      defined = true;
    });
    let discard: number[] | undefined;
    void client.declareHolding(zone, [3]).then((d) => {
      discard = d;
    });
    await clock.elapse(200);
    expect(naming.log).toEqual([
      'WATCH',
      'REGISTER zone/1',
      'SLOTS zone',
      'HOLDING zone 0,1,2,3',
      'CLAIM zone/0 e4',
      'CLAIM zone/1 e4',
      'CLAIM zone/2 e4',
    ]);
    expect(defined).toBe(true);
    expect(discard).toEqual([]);
    expect(client.servedSlots(zone)).toEqual(
      new Map([
        [0, 4],
        [1, 4],
        [2, 4],
      ]),
    );
    expect(log.filter((l) => l.includes('REVOKE'))).toEqual([]);
    naming.watch.resolve();
    await clock.elapse(ms10);
    expect(client.isSynced).toBe(true);
    expect(naming.connects).toBe(1);
    await closeWithoutTimers(clock, client);
  });

  test('a WATCH refused for it waits instead of dropping the channel', async () => {
    const clock = new TestClock();
    const h = new Harness({ clock });
    const connector = new Connector(h, { options: limited(1) });
    const client = new NamingClient(connector.call, { reconnectDelay, clock });
    // Restored before the WATCH, which then finds the limit reached.
    let defined = false;
    void client.defineSlots(room, { count: 4, mode: SlotMode.static, capacity: 0 }).then(() => {
      defined = true;
    });
    void client.start();
    await clock.elapse(ms50);
    expect(defined).toBe(true);
    expect(client.isSynced).toBe(true);
    expect(client.slotTable(room)?.count).toBe(4);
    expect(connector.calls).toBe(1);
    await closeWithoutTimers(clock, client);
    void h.close();
    await pump();
  });

  test('claims, releases and locates beyond it wait for their turn', async () => {
    const clock = new TestClock();
    const h = new Harness({ clock });
    const client = new NamingClient(new Connector(h, { options: limited(4) }).call, {
      reconnectDelay,
      clock,
    });
    client.slotHandler = new RecordingHandler('A', log);
    void client.start();
    void client.register(zone, [], { instance: 1 });
    await clock.elapse(ms10);
    void client.defineSlots(zone, { count: 16, mode: SlotMode.static, capacity: 0 });
    await clock.elapse(ms10);
    const slots = Array.from({ length: 16 }, (_, slot) => slot);
    let epochs: number[] | undefined;
    void Promise.all(slots.map((slot) => client.claim(zone, slot))).then((e) => {
      epochs = e;
    });
    await clock.elapse(ms50);
    expect(epochs).toEqual(Array(16).fill(1));
    expect(client.servedSlots(zone).size).toBe(16);
    let located: LocateResponse[] | undefined;
    void Promise.all(slots.map((slot) => client.locate(zone, slot))).then((l) => {
      located = l;
    });
    await clock.elapse(ms50);
    expect(located).toEqual(Array(16).fill(new LocateResponse(SlotState.owned, 1, 1)));
    let released = false;
    void Promise.all(slots.map((slot) => client.release(zone, slot, { keepStorage: true }))).then(
      () => {
        released = true;
      },
    );
    await clock.elapse(ms50);
    expect(released).toBe(true);
    expect(h.service.slotTable(zone)?.owners).toEqual(new Set());
    await closeWithoutTimers(clock, client);
    void h.close();
    await pump();
  });

  test('calls waiting for it fail with UNAVAILABLE when the channel is lost', async () => {
    const clock = new TestClock();
    const naming = new ScriptedNaming({ maxOutgoingRequests: 2, clock });
    naming.answerClaims = false;
    const client = new NamingClient(naming.connect, { reconnectDelay: 10_000, clock });
    void client.start();
    await clock.elapse(ms10);
    // One in flight beside the WATCH, two waiting.
    const outcomes = new Map<number, unknown>();
    for (const slot of [0, 1, 2]) {
      client.claim(zone, slot).then(
        (epoch) => {
          outcomes.set(slot, epoch);
        },
        (e: unknown) => {
          outcomes.set(slot, e);
        },
      );
    }
    await clock.elapse(ms50);
    expect(naming.log).toEqual(['WATCH', 'CLAIM zone/0 e0']);
    expect(outcomes.size).toBe(0);
    void naming.server.close();
    await clock.elapse(ms10);
    // The one in flight with the channel's end, the others with
    // UNAVAILABLE, never sent.
    expect(outcomes.size).toBe(3);
    expect(outcomes.get(0)).toBeInstanceOf(SwitchboardError);
    expectStatus(outcomes.get(1), StatusCode.unavailable);
    expectStatus(outcomes.get(2), StatusCode.unavailable);
    expect(naming.log).toHaveLength(2);
    await closeWithoutTimers(clock, client);
  });
});

describe('declared deadlines, no heartbeat', () => {
  const handover = 60_000;
  const clientBuffer = talkOptions(clientOptions).extendBuffer;

  /**
   * A client registered as `zone/[id]` over a connector that records the
   * EXTENDs it sends to the naming service in [extends_]; its slot handler
   * is [handler].
   */
  async function start(
    clock: TestClock,
    h: Harness,
    id: number,
    handler: SlotHandler | undefined,
    extends_: FakeFrame[],
    options: {
      slotHandlerMaxDuration?: number;
      /** Makes the slot handler for the client, when [handler] is undefined (a slot gate). */
      handlerFor?: (client: NamingClient) => SlotHandler;
    } = {},
  ): Promise<NamingClient> {
    const connector = new LossyConnector(h);
    connector.dropToService = (frame) => {
      if (frame.kind === 'extend') {
        extends_.push(frame);
      }
      return false;
    };
    const c = new NamingClient(connector.call, {
      reconnectDelay,
      slotHandlerMaxDuration: options.slotHandlerMaxDuration ?? 600_000,
      clock,
    });
    c.slotHandler = handler ?? options.handlerFor?.(c);
    void c.start();
    void c.register(zone, [], { instance: id });
    await clock.elapse(ms10);
    void c.defineSlots(zone, { count: 4, mode: SlotMode.static, capacity: 0 });
    await clock.elapse(ms10);
    return c;
  }

  async function finish(clock: TestClock, h: Harness, clients: NamingClient[]): Promise<void> {
    for (const c of clients) {
      void c.close();
    }
    void h.close();
    await pump();
    await clock.elapse(ms50 * 2);
    expect(clock.pendingCount).toBe(0);
  }

  test('a load that declares 5 min outlasts a 60 s handover timeout with one EXTEND', async () => {
    const clock = new TestClock();
    const h = new Harness({ clock, handoverTimeout: handover });
    const loading = new Deferred<AssignResult>();
    let seen: SlotRequestContext | undefined;
    const lifecycle = new Loader((_slot, context) => {
      seen = context;
      context?.extend({ deadline: 300_000 });
      return loading.promise;
    });
    const sb = new Switchboard();
    const extends_: FakeFrame[] = [];
    let gate: SlotGate | undefined;
    const c = await start(clock, h, 1, undefined, extends_, {
      handlerFor: (client) => {
        gate = new SlotGate(sb, client, zone, { lifecycle, instance: 1, clock });
        return gate;
      },
    });
    let epoch: number | undefined;
    let error: unknown;
    c.claim(zone, 2).then(
      (e) => {
        epoch = e;
      },
      (e: unknown) => {
        error = e;
      },
    );
    await clock.elapse(ms10);
    const context = seen as SlotRequestContext;
    expect(context).toBeDefined();
    expect([context.type.toString(), context.slot, context.epoch]).toEqual(['zone', 2, 1]);
    await clock.elapse(4 * 60_000);
    expect(error).toBeUndefined();
    expect(epoch).toBeUndefined();
    expect(context.isCancelled).toBe(false);
    expect(gate?.stateOf(2)).toBe(SlotGateState.loading);
    loading.resolve(AssignResult.holding);
    await clock.elapse(ms10);
    expect(epoch).toBe(1);
    expect(gate?.serves(2)).toBe(true);
    // Exactly one EXTEND, the estimate plus the client's buffer.
    expect(extends_.map(extension)).toEqual([
      { deadline: 300_000 + clientBuffer, renew: undefined },
    ]);
    void gate?.close();
    void sb.close();
    await finish(clock, h, [c]);
  });

  test('a load that declares nothing and runs past the handover timeout is cancelled, and sends nothing', async () => {
    const clock = new TestClock();
    const h = new Harness({ clock, handoverTimeout: handover });
    const log: string[] = [];
    const handler = new RecordingHandler('A', log);
    const loading = new Deferred<AssignResult>();
    handler.assign = () => loading.promise;
    const extends_: FakeFrame[] = [];
    const c = await start(clock, h, 1, handler, extends_);
    let error: unknown;
    c.claim(zone, 2).then(
      () => {},
      (e: unknown) => {
        error = e;
      },
    );
    await clock.elapse(ms10);
    const context = handler.contexts.get('ASSIGN 2') as SlotRequestContext;
    let cancelled = false;
    void context.onCancel.then(() => {
      cancelled = true;
    });
    await clock.elapse(handover - ms50);
    expect(error).toBeUndefined();
    expect(cancelled).toBe(false);
    await clock.elapse(ms50 * 2);
    expect(cancelled).toBe(true);
    expect(context.isCancelled).toBe(true);
    expectStatus(error, StatusCode.unavailable);
    // Extending what can no longer be answered does nothing.
    context.extend({ deadline: 60_000 });
    await clock.elapse(ms10);
    expect(extends_).toEqual([]);
    loading.resolve(AssignResult.holding);
    await clock.elapse(ms10);
    expect(handler.revoked).toEqual(['zone/2']);
    await finish(clock, h, [c]);
  });

  test("the handler deadline is the instance's own: it gives up first, and the estimate is lowered to slotHandlerMaxDuration", async () => {
    const clock = new TestClock();
    const h = new Harness({ clock, handoverTimeout: handover });
    const log: string[] = [];
    const handler = new RecordingHandler('A', log);
    handler.assign = (r) => {
      handler.contexts.get(`ASSIGN ${r.slot}`)?.extend({ deadline: 3_600_000 });
      return new Deferred<AssignResult>().promise;
    };
    const extends_: FakeFrame[] = [];
    const c = await start(clock, h, 1, handler, extends_, { slotHandlerMaxDuration: 120_000 });
    let error: unknown;
    c.claim(zone, 1).then(
      () => {},
      (e: unknown) => {
        error = e;
      },
    );
    await clock.elapse(ms10);
    expect(extends_.map(extension)).toEqual([
      { deadline: 120_000 + clientBuffer, renew: undefined },
    ]);
    await clock.elapse(120_000 - ms50);
    expect(error).toBeUndefined();
    await clock.elapse(ms50 * 2);
    expectStatus(error, StatusCode.unavailable);
    expect((error as SwitchboardError).status.reason).toContain('deadlineExceeded');
    await finish(clock, h, [c]);
  });

  test('a DRAIN whose wait for work in flight times out drains anyway, and the migration completes', async () => {
    const drainTimeout = 30_000;
    const clock = new TestClock();
    const h = new Harness({ clock, handoverTimeout: handover });
    const sbA = new Switchboard();
    const sbB = new Switchboard();
    const lifeA = new Keeper();
    let gateA: SlotGate | undefined;
    let gateB: SlotGate | undefined;
    const extendsA: FakeFrame[] = [];
    const a = await start(clock, h, 1, undefined, extendsA, {
      handlerFor: (client) => {
        gateA = new SlotGate(sbA, client, zone, {
          lifecycle: lifeA,
          drainTimeout,
          instance: 1,
          clock,
        });
        return gateA;
      },
    });
    const b = await start(clock, h, 2, undefined, [], {
      handlerFor: (client) => {
        gateB = new SlotGate(sbB, client, zone, { lifecycle: new Keeper(), instance: 2, clock });
        return gateB;
      },
    });
    const owner = gateA as SlotGate;
    void a.claim(zone, 2).catch(() => {});
    await clock.elapse(ms10);
    expect(owner.serves(2)).toBe(true);
    // A request for the slot that outlives drainTimeout.
    const untimed: Partial<TalkOptions> = { requestTimeout: 0, replyTimeout: 0, clock };
    const [near, far] = frameLink();
    const caller = new TalkChannel(near, untimed);
    const callee = new TalkChannel(far, untimed);
    const held = new Deferred<void>();
    void callee.serve((m) => owner.serveRequest(m, 2, () => held.promise));
    caller.request('GET').catch(() => {});
    await clock.elapse(ms10);
    const [operator] = h.link();
    const phases: string[] = [];
    const migrate = operator.streamRequest(
      'MIGRATE',
      new MigrateRequest(zone, 2, { to: 2 }).encode(),
    );
    void (async () => {
      try {
        for await (const m of migrate.items) {
          phases.push(migrationPhaseName(PhaseItem.decode(m.payload).phase));
        }
      } catch {
        // Read through `done`.
      }
    })();
    let outcome: unknown;
    migrate.done.then(
      () => {
        outcome = 'done';
      },
      (e: unknown) => {
        outcome = e;
      },
    );
    await clock.elapse(drainTimeout - ms50);
    expect(phases).toEqual(['draining']);
    expect(lifeA.drained).toEqual([]);
    await clock.elapse(ms50 * 2);
    // Drained anyway, on a DRAIN that is still current.
    expect(lifeA.drained).toEqual(['drain 2 e2 to2 cancelled false']);
    await clock.elapse(ms10);
    expect(outcome).toBe('done');
    expect(phases).toEqual(['draining', 'assigning', 'forwarding', 'done']);
    expect(h.service.slotTable(zone)?.get(2).owner).toBe(2);
    // The wait, a second more and the client's buffer; then an empty EXTEND
    // giving the drain the naming service's default timeout.
    expect(extendsA.map(extension)).toEqual([
      { deadline: drainTimeout + 1000 + clientBuffer, renew: undefined },
      { deadline: undefined, renew: undefined },
    ]);
    held.resolve();
    void caller.close();
    void callee.close();
    void operator.close();
    void gateA?.close();
    void gateB?.close();
    void sbA.close();
    void sbB.close();
    await finish(clock, h, [a, b]);
  });

  test('MIGRATE passes the deadline the old owner declares on DRAIN on to the requester', async () => {
    const clock = new TestClock();
    const h = new Harness({ clock, handoverTimeout: handover });
    const log: string[] = [];
    const handlerA = new RecordingHandler('A', log);
    const handlerB = new RecordingHandler('B', log);
    const draining = new Deferred<void>();
    handlerA.drain = (r) => {
      handlerA.contexts.get(`DRAIN ${r.slot}`)?.extend({ deadline: 180_000 });
      return draining.promise;
    };
    const extendsA: FakeFrame[] = [];
    const a = await start(clock, h, 1, handlerA, extendsA);
    const b = await start(clock, h, 2, handlerB, []);
    void a.claim(zone, 2);
    await clock.elapse(ms10);
    // The operator, on a raw channel.
    const [operator] = h.link();
    const declared: [number | undefined, number | undefined][] = [];
    const phases: string[] = [];
    const migrate = operator.streamRequest(
      'MIGRATE',
      new MigrateRequest(zone, 2, { to: 2 }).encode(),
      { onExtend: (e) => declared.push([e.deadline, e.renew]) },
    );
    void (async () => {
      try {
        for await (const m of migrate.items) {
          phases.push(migrationPhaseName(PhaseItem.decode(m.payload).phase));
        }
      } catch {
        // Read through `done`.
      }
    })();
    let outcome: unknown;
    migrate.done.then(
      () => {
        outcome = 'done';
      },
      (e: unknown) => {
        outcome = e;
      },
    );
    await clock.elapse(ms10);
    const buffer = talkOptions(serverOptions).extendBuffer;
    expect(phases).toEqual(['draining']);
    expect(declared).toEqual([
      // The DRAIN was sent: the service's handover timeout.
      [handover + buffer, undefined],
      // The old owner's estimate, with its buffer and the service's.
      [180_000 + clientBuffer + buffer, undefined],
    ]);
    // Far past the operator's own request timeout, and the handover
    // timeout.
    await clock.elapse(120_000);
    expect(outcome).toBeUndefined();
    draining.resolve();
    await clock.elapse(ms10);
    expect(outcome).toBe('done');
    expect(phases).toEqual(['draining', 'assigning', 'forwarding', 'done']);
    // Each later step declared the handover timeout.
    expect(declared.slice(2)).toEqual([
      [handover + buffer, undefined],
      [handover + buffer, undefined],
    ]);
    expect(extendsA).toHaveLength(1);
    expect(h.service.slotTable(zone)?.get(2).owner).toBe(2);
    void operator.close();
    await finish(clock, h, [a, b]);
  });
});

/** A lifecycle whose loads [onLoad] answers, and that refuses every channel. */
class Loader extends SlotLifecycle {
  readonly onLoad: (slot: number, context: SlotRequestContext | undefined) => Promise<AssignResult>;

  constructor(
    onLoad: (slot: number, context: SlotRequestContext | undefined) => Promise<AssignResult>,
  ) {
    super();
    this.onLoad = onLoad;
  }

  override load(slot: number, options: SlotLoadOptions): Promise<AssignResult> {
    return this.onLoad(slot, options.context);
  }

  override serve(channel: IncomingChannel): void {
    void channel.reject(Status.of(StatusCode.unavailable));
  }
}

/** A lifecycle that loads at once without holding, keeps the channels it serves open, and records its drains. */
class Keeper extends SlotLifecycle {
  readonly drained: string[] = [];

  override async load(): Promise<AssignResult> {
    return AssignResult.notHolding;
  }

  override async drain(slot: number, options: SlotDrainOptions): Promise<void> {
    this.drained.push(
      `drain ${slot} e${options.epoch} to${options.to} cancelled ${options.context?.isCancelled}`,
    );
  }

  override serve(): void {}
}
