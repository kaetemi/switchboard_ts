/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Port of the Dart `naming_identity_test.dart`: the naming service and
// identity (wiki page "Polyverse Switchboard Identity and Credentials",
// stage B): scope enforcement per `_ns` channel (connection identity, or a
// credential in the open payload), RENEW (with fake time), the end of a
// channel whose credential expires, slot spaces defined again after a
// restart, and CONNECT brokering.
//
// The Dart test runs over mem:// nodes with real credentials. Until the
// protocol core's identity and the node land, the port serves in-memory
// Talk channels with fake connections and fake credentials
// (`test/support/naming/fake-identity.ts`): a node is its credential and its
// connection to each naming service, a valid `IDENT` a new identity on that
// connection (`updateCredential`, `identify`), and a `MeshNode` its naming
// client. What only the node does (the `IDENT` exchange itself, brokered
// connections, MeshNode's credential renewal, PeerSet) is not ported; those
// tests are skipped with the reason.

import { afterEach, describe, expect, test } from 'vitest';
import { ServiceAddress } from '../../../src/core/address/index.ts';
import { utf8Encode } from '../../../src/core/bytes.ts';
import {
  type Credential,
  CredentialKind,
  type CredentialVerifier,
  Right,
  type Scope,
} from '../../../src/core/identity/contract.ts';
import { Credential as Codec } from '../../../src/core/identity/credential.ts';
import { CredentialIssuer } from '../../../src/core/identity/credential-issuer.ts';
import { CredentialVerifier as Verifier } from '../../../src/core/identity/credential-verifier.ts';
import { Name } from '../../../src/core/name.ts';
import {
  AssignResult,
  NamingClient,
  type SlotHandler,
} from '../../../src/core/naming/naming-client.ts';
import {
  ClaimRequest,
  ConnectRequest,
  type DialBackRequest,
  HoldingRequest,
  LocateRequest,
  MigrateRequest,
  Procedures,
  RegisterRequest,
  Services,
  SlotMode,
  SlotSpace,
  SlotState,
  SlotsRequest,
  UnregisterRequest,
  WatchRequest,
} from '../../../src/core/naming/naming-protocol.ts';
import type {
  NamingCredentialIssuer,
  NamingService,
} from '../../../src/core/naming/naming-service.ts';
import type { SlotTable } from '../../../src/core/naming/slot-table.ts';
import {
  type Status,
  StatusCode,
  SwitchboardError,
  statusCodeName,
} from '../../../src/core/status.ts';
import type { TalkChannel, TalkOptions } from '../../../src/core/talk/contract.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';
import {
  decodeFakeCredential,
  FakeIssuer,
  FakeNamingConnection,
  FakeNamingVerifier,
  scopeOf,
} from '../../support/naming/fake-identity.ts';
import {
  delay,
  describeEvent,
  describe as describeMessage,
  Harness,
  pump,
  rejectsWith,
  until,
  within,
} from '../../support/naming/harness.ts';
import { TestClock } from '../../support/naming/test-clock.ts';

/** The Talk options of every node of the Dart test. */
const talk: Partial<TalkOptions> = { requestTimeout: 2000, replyTimeout: 1000 };
const limit = 5000;
const second = 1000;
const minute = 60 * second;
const hour = 60 * minute;

const workerType = Name.of('worker-g');
const npc = Name.of('npc');
const empty = new Uint8Array(0);

/** The public key of the holder key every holder-key credential names. */
const holderKey = Uint8Array.from({ length: 32 }, (_, i) => 200 - i);

/**
 * The wall clock (`package:clock` in the Dart test): the real time, or a
 * test clock's in the tests on fake time. Read by the issuers, the
 * verifiers, the connections and the naming services.
 */
let wall: () => number = Date.now;
const now = (): number => wall();

/** Issues holder-key credentials (the Dart test's Ed25519 authority). */
const authority = new FakeIssuer({ keyId: 'auth1', now });
/** Issues bearer credentials (the Dart test's HMAC issuer). */
const hmac = new FakeIssuer({ keyId: 'mesh1', bearerOnly: true, now });

const workerScopes: Scope[] = [scopeOf(Right.register, 'worker-*')];
const consumerScopes: Scope[] = [
  scopeOf(Right.watch, '*'),
  scopeOf(Right.open, 'worker-*'),
  scopeOf(Right.broker, 'worker-*'),
];
const adminScopes: Scope[] = [scopeOf(Right.admin, '*')];

function issue(
  identity: string,
  scopes: readonly Scope[],
  options: { lifetime?: number; bearer?: boolean } = {},
): Promise<Credential> {
  const bearer = options.bearer ?? false;
  return (bearer ? hmac : authority).issue({
    kind: CredentialKind.node,
    identity,
    scopes,
    holderKey: bearer ? undefined : holderKey,
    lifetime: options.lifetime ?? 8 * hour,
  });
}

function verifier(): CredentialVerifier {
  return new FakeNamingVerifier({ now });
}

const teardowns: (() => unknown)[] = [];

/** Runs [action] after the test, in reverse order of registration (Dart `addTearDown`). */
function addTearDown(action: () => unknown): void {
  teardowns.push(action);
}

afterEach(async () => {
  for (const action of teardowns.splice(0).reverse()) {
    await action();
  }
  wall = Date.now;
});

/** The only element of [values]. */
function single<T>(values: Iterable<T>): T {
  const all = [...values];
  expect(all).toHaveLength(1);
  return all[0] as T;
}

/** [value], which must be defined. */
function must<T>(value: T | undefined): T {
  expect(value).toBeDefined();
  return value as T;
}

/** How a request ended: `ok`, or the name of its status code. */
type Outcome = string;

const ok: Outcome = 'ok';
const denied: Outcome = 'permissionDenied';
const unauthenticated: Outcome = 'unauthenticated';

function outcomeOf(code: number): Outcome {
  return statusCodeName(code) ?? `code ${code}`;
}

/** A node: its credential, and its connection to each naming service. */
class Node {
  credential: Credential | undefined;
  readonly #connections = new Map<Mesh, FakeNamingConnection>();
  /** The connections this node identified on. */
  readonly #identified = new Set<FakeNamingConnection>();

  constructor(credential?: Credential) {
    this.credential = credential;
  }

  /** The node's open connection to [mesh], made (and identified, with a credential) if needed. */
  connect(mesh: Mesh): FakeNamingConnection {
    const existing = this.#connections.get(mesh);
    if (existing?.isOpen === true) {
      return existing;
    }
    const credential = this.credential;
    const connection = new FakeNamingConnection({ credential, now });
    if (credential !== undefined) {
      this.#identified.add(connection);
    }
    this.#connections.set(mesh, connection);
    mesh.connections.push(connection);
    return connection;
  }

  /** Replaces the credential and presents it on every open connection the node identified on. */
  async updateCredential(credential: Credential): Promise<void> {
    this.credential = credential;
    await Promise.all(
      [...this.#identified]
        .filter((connection) => connection.isOpen)
        .map((connection) => connection.identify(credential)),
    );
  }

  /** Every GOAWAY the node's connections were sent. */
  get goAways(): Status[] {
    return [...this.#connections.values()].flatMap((connection) => connection.goAways);
  }

  async close(): Promise<void> {
    await Promise.all([...this.#connections.values()].map((connection) => connection.close()));
  }
}

/** A node, closed after the test unless [close] is false (for fake time, where the test closes it). */
function node(options: { credential?: Credential; close?: boolean } = {}): Node {
  const n = new Node(options.credential);
  if (options.close ?? true) {
    addTearDown(() => n.close());
  }
  return n;
}

/** What {@link Mesh.start} takes. */
interface MeshOptions {
  readonly requireCredential?: boolean;
  readonly withIssuer?: boolean;
  /** The issuer of `RENEW` when `withIssuer`; default the fake authority. */
  readonly issuer?: NamingCredentialIssuer;
  readonly payloadVerifier?: CredentialVerifier;
  readonly renewable?: (credential: Credential) => boolean;
  readonly brokerTimeout?: number;
  readonly assignmentHold?: number;
  readonly close?: boolean;
  readonly clock?: TestClock;
}

/** A naming service the nodes connect to. */
class Mesh {
  readonly harness: Harness;
  /** The connections of the nodes to this naming service. */
  readonly connections: FakeNamingConnection[] = [];
  #closed = false;

  private constructor(harness: Harness) {
    this.harness = harness;
  }

  static start(options: MeshOptions = {}): Mesh {
    const harness = new Harness({
      assignmentHold: options.assignmentHold ?? 0,
      holdingSettle: 0,
      // The Dart service's defaults, which the harness shortens.
      handoverTimeout: 60_000,
      verifier: options.payloadVerifier ?? verifier(),
      requireCredential: options.requireCredential ?? true,
      issuer: (options.withIssuer ?? true) ? (options.issuer ?? authority) : undefined,
      renewable: options.renewable,
      brokerTimeout: options.brokerTimeout ?? 5000,
      wallTime: now,
      ...(options.clock === undefined ? {} : { clock: options.clock }),
    });
    const mesh = new Mesh(harness);
    if (options.close ?? true) {
      addTearDown(() => mesh.close());
    }
    return mesh;
  }

  get service(): NamingService {
    return this.harness.service;
  }

  get isClosed(): boolean {
    return this.#closed;
  }

  /** A raw `_ns` channel from [client] over its connection, with [payload] as the application payload of its OPEN. */
  link(client: Node, payload?: Uint8Array): TalkChannel {
    const [channel] = this.harness.link({
      connection: client.connect(this),
      ...(payload === undefined ? {} : { credential: payload }),
      options: talk,
      serverOptions: talk,
    });
    return channel;
  }

  async close(): Promise<void> {
    if (this.#closed) {
      return;
    }
    this.#closed = true;
    await this.harness.close();
    await Promise.all(this.connections.map((connection) => connection.close()));
  }
}

/** A raw `_ns` channel from [client] to [mesh], closed after the test. */
function channelTo(client: Node, mesh: Mesh, options: { payload?: Uint8Array } = {}): TalkChannel {
  const channel = mesh.link(client, options.payload);
  addTearDown(() => channel.close());
  return channel;
}

/**
 * The naming client of [client] to [mesh] (a function: the naming service
 * found there now), as `MeshNode.join` and `namingClientFor` make it;
 * started, and closed after the test. Connecting fails while that naming
 * service is closed.
 */
function join(
  client: Node,
  mesh: Mesh | (() => Mesh),
  options: { watch?: boolean; reconnectDelay?: number } = {},
): NamingClient {
  const target = typeof mesh === 'function' ? mesh : () => mesh;
  const naming = new NamingClient(
    async () => {
      const current = target();
      if (current.isClosed) {
        throw SwitchboardError.of(StatusCode.unavailable, 'naming service down');
      }
      return current.link(client);
    },
    { reconnectDelay: options.reconnectDelay ?? second, watch: options.watch ?? true },
  );
  addTearDown(() => naming.close());
  naming.start().catch(() => {});
  return naming;
}

/** Takes the messages of [channel] and drops them (Dart `messages.listen((_) {})`). */
function drain(channel: TalkChannel, into?: (line: string) => void): void {
  void (async () => {
    try {
      for await (const message of channel.messages) {
        into?.(describeMessage(message));
      }
    } catch {
      // The channel ended.
    }
  })();
}

/** How a request ended. */
async function call(
  channel: TalkChannel,
  procedure: Name,
  payload: Uint8Array,
  options: { stream?: boolean } = {},
): Promise<Outcome> {
  try {
    if (options.stream === true) {
      await within(
        channel.streamRequest(procedure.toString(), payload, { name: procedure }).done,
        limit,
      );
    } else {
      await within(channel.request(procedure.toString(), payload, { name: procedure }), limit);
    }
    return ok;
  } catch (error) {
    if (error instanceof SwitchboardError) {
      return outcomeOf(error.code);
    }
    throw error;
  }
}

function registerPayload(type: Name, instance = 0): Uint8Array {
  return new RegisterRequest(type, { requestedInstance: instance }).encode();
}

/** Every request a channel can make about [type] (CONNECT to [instance], default a missing one), and how it ends. */
async function survey(
  channel: TalkChannel,
  type: Name,
  instance = 0,
): Promise<Record<string, Outcome>> {
  return {
    'WATCH *': await call(channel, Procedures.watch, empty),
    'WATCH type': await call(channel, Procedures.watch, new WatchRequest(type).encode()),
    LOOKUP: await call(channel, Procedures.lookup, new WatchRequest(type).encode(), {
      stream: true,
    }),
    SLOTS: await call(
      channel,
      Procedures.slots,
      new SlotsRequest(new SlotSpace(type, { count: 4 }), { capacity: 0 }).encode(),
    ),
    CLAIM: await call(channel, Procedures.claim, new ClaimRequest(type, 0).encode()),
    LOCATE: await call(channel, Procedures.locate, new LocateRequest(type, 0).encode()),
    MIGRATE: await call(channel, Procedures.migrate, new MigrateRequest(type, 0).encode(), {
      stream: true,
    }),
    CONNECT: await call(
      channel,
      Procedures.connect,
      new ConnectRequest(type, instance === 0 ? 0xabc : instance, 'mem://nowhere').encode(),
    ),
    // Last: a registration would make the claim send ASSIGN to this channel.
    REGISTER: await call(channel, Procedures.register, registerPayload(type)),
  };
}

/** The addresses of the records in [service]'s table, as text. */
function addresses(service: NamingService): string[] {
  return [...service.table.values()].map((record) => record.address.toString());
}

/** Holds just the naming OPEN check across the connection's expiry. */
class DelayedVerifier implements CredentialVerifier {
  readonly delegate: CredentialVerifier;
  readonly ready: Promise<void>;

  constructor(delegate: CredentialVerifier, ready: Promise<void>) {
    this.delegate = delegate;
    this.ready = ready;
  }

  async verify(bytes: Uint8Array, options?: { now?: number }): Promise<Credential> {
    await this.ready;
    return this.delegate.verify(bytes, options);
  }
}

/**
 * The slot handler MeshNode.publishSharded installs (its slot gates) with a
 * slot lifecycle that has nothing to load or serve (the Dart test's `Idle`).
 */
const idle: SlotHandler = {
  onAssign: () => AssignResult.notHolding,
  onDrain: () => {},
  onForward: () => {},
  onResume: () => {},
};

/** A wall time in whole seconds for the tests on fake time. */
const epoch = 1_800_000_000_000;

/** A test clock that is also the wall clock until the end of the test. */
function fakeTime(): TestClock {
  const clock = new TestClock(epoch);
  wall = () => clock.now();
  return clock;
}

/** The mem:// endpoints the nodes listen on. */
const listening = new Set<string>();
let listeners = 0;

/** A mem:// endpoint [client] listens on (`listenMemory`). */
function listen(_client: Node): string {
  const endpoint = `mem://listener-${++listeners}`;
  listening.add(endpoint);
  return endpoint;
}

/**
 * MeshNode's dial-back for a `CONNECT` the naming service relays: here only
 * the dial, which fails where nothing listens.
 */
async function dialBack(request: DialBackRequest): Promise<void> {
  if (!listening.has(request.endpoint)) {
    throw SwitchboardError.of(StatusCode.unavailable, `nothing listens at ${request.endpoint}`);
  }
}

describe('scopes', () => {
  test('a worker credential can register its type (and define its slot space) and nothing else', async () => {
    const mesh = Mesh.start();
    const worker = node({ credential: await issue('worker-a1', workerScopes) });
    const c = channelTo(worker, mesh);
    expect(await survey(c, workerType)).toEqual({
      REGISTER: ok,
      'WATCH *': denied,
      'WATCH type': denied,
      LOOKUP: denied,
      SLOTS: ok,
      CLAIM: denied,
      LOCATE: denied,
      MIGRATE: denied,
      CONNECT: denied,
    });
    expect(await call(c, Procedures.register, registerPayload(npc))).toBe(denied);
    expect(
      await call(
        c,
        Procedures.slots,
        new SlotsRequest(new SlotSpace(npc, { count: 4 }), { capacity: 0 }).encode(),
      ),
    ).toBe(denied);
    // Its own record it may take back; not another channel's.
    const id = single(mesh.service.table.values()).address.instance;
    expect(
      await call(c, Procedures.unregister, new UnregisterRequest(workerType, id).encode()),
    ).toBe(ok);
    expect(mesh.service.table.size).toBe(0);
  });

  test('a consumer credential can watch and broker', async () => {
    const mesh = Mesh.start();
    const consumer = node({ credential: await issue('consumer-1', consumerScopes) });
    const c = channelTo(consumer, mesh);
    expect(await survey(c, workerType)).toEqual({
      REGISTER: denied,
      'WATCH *': ok,
      'WATCH type': ok,
      LOOKUP: ok,
      SLOTS: denied,
      // No slot space: past the scope check, NOT_FOUND.
      CLAIM: denied,
      LOCATE: 'notFound',
      MIGRATE: denied,
      // No such record: past the scope check.
      CONNECT: 'notFound',
    });
    expect(await call(c, Procedures.connect, new ConnectRequest(npc, 1, 'mem://x').encode())).toBe(
      denied,
    );
  });

  test('admin can do everything', async () => {
    const mesh = Mesh.start();
    const admin = node({ credential: await issue('root', adminScopes) });
    const c = channelTo(admin, mesh);
    expect(await survey(c, workerType)).toEqual({
      REGISTER: ok,
      'WATCH *': ok,
      'WATCH type': ok,
      LOOKUP: ok,
      SLOTS: ok,
      // Past the scope checks: a claim needs a registration with capacity,
      // a migration an owned slot, CONNECT a record.
      CLAIM: 'failedPrecondition',
      LOCATE: ok,
      MIGRATE: 'failedPrecondition',
      CONNECT: 'notFound',
    });
  });

  test(
    'a slot space is defined, redefined while blank and confirmed with claim or register, ' +
      'never with watch alone',
    async () => {
      const mesh = Mesh.start();
      const watcher = node({
        credential: await issue('consumer-3', [scopeOf(Right.watch, '*')]),
      });
      const worker = node({
        credential: await issue('worker-a1', [...workerScopes, scopeOf(Right.claim, 'worker-*')]),
      });
      const registrar = node({ credential: await issue('worker-b2', workerScopes) });
      const claimer = node({
        credential: await issue('worker-c3', [scopeOf(Right.claim, 'worker-*')]),
      });
      const admin = node({ credential: await issue('root', adminScopes) });
      const v = channelTo(watcher, mesh);
      const w = channelTo(worker, mesh);
      const r = channelTo(registrar, mesh);
      const c = channelTo(claimer, mesh);
      const a = channelTo(admin, mesh);
      const space = new SlotSpace(workerType, { count: 4 });
      const wider = new SlotSpace(workerType, { count: 8 });
      const slots = (s: SlotSpace): Uint8Array => new SlotsRequest(s, { capacity: 0 }).encode();
      const defined = (): SlotSpace | undefined => mesh.service.slotTable(workerType)?.space;
      // watch defines nothing; register alone defines (what an instance does
      // after a restart of the naming service).
      expect(await call(v, Procedures.slots, slots(space))).toBe(denied);
      expect(defined()).toBeUndefined();
      expect(await call(r, Procedures.slots, slots(space))).toBe(ok);
      expect(defined()).toEqual(space);
      // Confirmed with claim or register, not with watch.
      expect(await call(c, Procedures.slots, slots(space))).toBe(ok);
      expect(await call(v, Procedures.slots, slots(space))).toBe(denied);
      // While blank, redefined under the same rule.
      expect(await call(v, Procedures.slots, slots(wider))).toBe(denied);
      expect(defined()).toEqual(space);
      expect(await call(c, Procedures.slots, slots(wider))).toBe(ok);
      expect(defined()).toEqual(wider);
      expect(await call(r, Procedures.slots, slots(space))).toBe(ok);
      expect(defined()).toEqual(space);
      // A held slot ends the blank state: a redefinition is refused for
      // everyone, admin included; HOLDING still needs claim.
      expect(await call(w, Procedures.register, registerPayload(workerType))).toBe(ok);
      expect(await call(w, Procedures.slots, slots(space))).toBe(ok);
      expect(await call(w, Procedures.holding, new HoldingRequest(workerType, [0]).encode())).toBe(
        ok,
      );
      expect(await call(r, Procedures.holding, new HoldingRequest(workerType, [1]).encode())).toBe(
        denied,
      );
      for (const channel of [w, a]) {
        expect(await call(channel, Procedures.slots, slots(wider))).toBe('failedPrecondition');
      }
      expect(await call(a, Procedures.slots, slots(space))).toBe(ok);
      expect(defined()).toEqual(space);
    },
  );

  test(
    'after a naming service restart, an instance defines its slot space again with its own ' +
      'credential',
    async () => {
      // The Dart test listens on the same mem:// name again; here the
      // worker's connector follows `current`.
      const mesh = Mesh.start();
      let current = mesh;
      const worker = node({
        credential: await issue('worker-a1', [...workerScopes, scopeOf(Right.claim, 'worker-*')]),
      });
      const joined = join(worker, () => current, { watch: false, reconnectDelay: 20 });
      const space = new SlotSpace(workerType, { count: 4, mode: SlotMode.static });
      // MeshNode.publishSharded: the slot handler, the registration, then
      // SLOTS.
      joined.slotHandler = idle;
      await within(joined.register(workerType, []), limit);
      await within(
        joined.defineSlots(workerType, { count: space.count, mode: space.mode, capacity: 0 }),
        limit,
      );
      await within(joined.claim(workerType, 2), limit);
      const id = single(mesh.service.table.values()).address.instance;
      expect(mesh.service.slotTable(workerType)?.get(2).owner).toBe(id);
      await mesh.close();
      // Nobody with admin around: the instance's own SLOTS defines the space
      // on the new naming service, and its slot is claimed back.
      const restarted = Mesh.start();
      current = restarted;
      await until(() => restarted.service.slotTable(workerType)?.get(2).owner === id, limit);
      expect(restarted.service.slotTable(workerType)?.space).toEqual(space);
      expect(addresses(restarted.service)).toEqual([new ServiceAddress(workerType, id).toString()]);
      await within(joined.close(), limit);
    },
  );

  test('a relay record names the identity of the channel registering it', async () => {
    const mesh = Mesh.start({ requireCredential: false });
    const relayType = Services.relay;
    const relayRecord = (identity: string): Uint8Array =>
      new RegisterRequest(relayType, {
        endpoints: ['mem://relay'],
        metadata: utf8Encode(identity),
      }).encode();
    const relay = node({
      credential: await issue('relay-1', [scopeOf(Right.register, relayType)]),
    });
    const r = channelTo(relay, mesh);
    // Consumers name the metadata as the receiver of their IDENT: a relay
    // naming another node could pass their identification on.
    for (const other of ['ns', 'worker-a1', '']) {
      expect(await call(r, Procedures.register, relayRecord(other))).toBe(denied);
    }
    expect(mesh.service.table.size).toBe(0);
    expect(await call(r, Procedures.register, relayRecord('relay-1'))).toBe(ok);
    expect(new TextDecoder().decode(single(mesh.service.table.values()).metadata)).toBe('relay-1');
    // Admin is held to it too; an unidentified channel, which may do
    // everything here, is not checked.
    const admin = node({ credential: await issue('root', adminScopes) });
    const a = channelTo(admin, mesh);
    expect(await call(a, Procedures.register, relayRecord('relay-1'))).toBe(denied);
    const anonymous = node();
    const n = channelTo(anonymous, mesh);
    expect(await call(n, Procedures.register, relayRecord('relay-2'))).toBe(ok);
    expect(mesh.service.table.size).toBe(2);
  });

  test('the connection identity takes precedence over a credential in the open payload', async () => {
    const mesh = Mesh.start();
    // Identified by IDENT as "weak", which has no scope; its channel's
    // payload carries a valid bearer credential that grants everything.
    const weak = node({ credential: await issue('weak', []) });
    const wide = await issue('wide', adminScopes, { bearer: true });
    const c = channelTo(weak, mesh, { payload: wide.encode() });
    const results = await survey(c, workerType);
    expect(new Set(Object.values(results))).toEqual(new Set([denied]));
    expect(mesh.service.table.size).toBe(0);
    expect(mesh.service.slotTable(workerType)).toBeUndefined();
    // The same payload identifies a channel whose connection has no
    // identity.
    const anonymous = node();
    const a = channelTo(anonymous, mesh, { payload: wide.encode() });
    expect(await call(a, Procedures.watch, empty)).toBe(ok);
    expect(await call(a, Procedures.register, registerPayload(workerType))).toBe(ok);
  });

  test('without a credential: UNAUTHENTICATED when one is required, everything otherwise', async () => {
    const strict = Mesh.start();
    const lenient = Mesh.start({ requireCredential: false });
    const anonymous = node();
    const s = channelTo(anonymous, strict);
    const results = await survey(s, workerType);
    expect(new Set(Object.values(results))).toEqual(new Set([unauthenticated]));
    expect(await call(s, Procedures.renew, empty)).toBe(results['WATCH *']);
    const l = channelTo(anonymous, lenient);
    expect(await call(l, Procedures.register, registerPayload(workerType))).toBe(ok);
    expect(await call(l, Procedures.watch, empty)).toBe(ok);
    // An identified channel is held to its scopes even then.
    const worker = node({ credential: await issue('worker-a1', workerScopes) });
    const w = channelTo(worker, lenient);
    expect(await call(w, Procedures.watch, empty)).toBe(denied);
  });

  test('a bearer credential in the open payload identifies a channel; a holder-key one does not', async () => {
    const mesh = Mesh.start();
    // The client node has no credential of its own: no IDENT.
    const client = node();
    const bearer = await issue('consumer-2', consumerScopes, { bearer: true });
    const c = channelTo(client, mesh, { payload: bearer.encode() });
    expect(await call(c, Procedures.watch, empty)).toBe(ok);
    expect(await call(c, Procedures.register, registerPayload(workerType))).toBe(denied);
    const held = await issue('consumer-3', consumerScopes);
    const h = channelTo(client, mesh, { payload: held.encode() });
    expect(await call(h, Procedures.watch, empty)).toBe(unauthenticated);
    const junk = channelTo(client, mesh, { payload: utf8Encode('secret') });
    expect(await call(junk, Procedures.watch, empty)).toBe(unauthenticated);
  });

  test(
    'a node whose IDENT names the naming service is accepted by a host that requires it; ' +
      'MeshNode works under the scopes',
    async () => {
      // Not ported: the host's `requireNamedIdent`, the worker's
      // `expectedIdentityFor` naming "ns", and the host refusing a node
      // whose IDENT names nobody (`connect` failing UNAUTHENTICATED) are the
      // node's IDENT exchange. The naming half is: a MeshNode under worker
      // scopes, with and without watching.
      const mesh = Mesh.start({ withIssuer: false });
      const worker = node({ credential: await issue('worker-a1', workerScopes) });
      const joined = join(worker, mesh, { watch: false });
      const id = await within(joined.register(workerType, []), limit);
      expect(addresses(mesh.service)).toEqual([new ServiceAddress(workerType, id).toString()]);
      expect(joined.isSynced).toBe(false);
      // A client that watches anyway is refused once, and keeps its channel
      // and its registrations.
      const watching = join(worker, mesh);
      await within(watching.register(Name.of('worker-h'), []), limit);
      await delay(100);
      expect(watching.isConnected).toBe(true);
      expect(mesh.service.table.size).toBe(2);
    },
  );
});

describe('a replacement credential', () => {
  const watcherScopes: Scope[] = [scopeOf(Right.register, 'worker-*'), scopeOf(Right.watch, '*')];
  const otherType = Name.of('worker-h');

  test('of another identity ends the naming channel as an expiry would, without GOAWAY', async () => {
    const mesh = Mesh.start();
    const changes: string[] = [];
    mesh.service.on('event', (e) => changes.push(describeEvent(e)));
    const worker = node({ credential: await issue('worker-a1', watcherScopes) });
    const connection = worker.connect(mesh);
    const c = channelTo(worker, mesh);
    expect(await call(c, Procedures.watch, empty)).toBe(ok);
    expect(await call(c, Procedures.register, registerPayload(workerType))).toBe(ok);
    const address = single(mesh.service.table.values()).address;
    // The same holder, presenting another identity it holds.
    await worker.updateCredential(await issue('worker-b2', watcherScopes));
    const ended = await within(c.done, limit);
    expect(ended.name).toBe('unauthenticated');
    expect(ended.reason).toBe('identity changed');
    expect(mesh.service.table.size).toBe(0);
    expect(mesh.service.watchCount).toBe(0);
    expect(changes).toEqual([`UP ${address}`, `DOWN ${address}`]);
    // The connection is validly identified: it stays, and serves a new
    // naming channel under the new identity. (The Dart test also checks
    // that the worker's side of it sees no identity of the naming host,
    // which has no credential: the node's side, not modelled here.)
    expect(connection.isOpen).toBe(true);
    const again = channelTo(worker, mesh);
    expect(await call(again, Procedures.register, registerPayload(workerType))).toBe(ok);
    expect(connection.goAways).toEqual([]);
    expect(connection.isOpen).toBe(true);
  });

  test('of another identity: a MeshNode registers again under it', async () => {
    const mesh = Mesh.start();
    const changes: string[] = [];
    mesh.service.on('event', (e) => changes.push(describeEvent(e)));
    const worker = node({ credential: await issue('worker-a1', workerScopes) });
    const joined = join(worker, mesh, { watch: false, reconnectDelay: 20 });
    const id = await within(joined.register(workerType, []), limit);
    await worker.updateCredential(await issue('worker-b2', workerScopes));
    await until(() => changes.length === 3, limit);
    const address = new ServiceAddress(workerType, id);
    expect(changes).toEqual([`UP ${address}`, `DOWN ${address}`, `UP ${address}`]);
    expect(mesh.service.channelCount).toBe(1);
  });

  test('of another identity also ends a channel its payload identified', async () => {
    const mesh = Mesh.start();
    const client = node();
    const connection = client.connect(mesh);
    const bearer = await issue('consumer-2', consumerScopes, { bearer: true });
    const c = channelTo(client, mesh, { payload: bearer.encode() });
    expect(await call(c, Procedures.watch, empty)).toBe(ok);
    expect(mesh.service.watchCount).toBe(1);
    await connection.identify(await issue('worker-a1', watcherScopes));
    expect((await within(c.done, limit)).name).toBe('unauthenticated');
    expect(mesh.service.watchCount).toBe(0);
    expect(connection.isOpen).toBe(true);
  });

  test(
    'of the same identity with narrower scopes ends the subscriptions and records they do ' +
      'not grant, silently',
    async () => {
      const mesh = Mesh.start();
      const changes: string[] = [];
      mesh.service.on('event', (e) => changes.push(describeEvent(e)));
      const worker = node({ credential: await issue('worker-a1', watcherScopes) });
      const c = channelTo(worker, mesh);
      const received: string[] = [];
      drain(c, (line) => received.push(line));
      expect(await call(c, Procedures.watch, empty)).toBe(ok);
      expect(await call(c, Procedures.watch, new WatchRequest(workerType).encode())).toBe(ok);
      expect(await call(c, Procedures.watch, new WatchRequest(otherType).encode())).toBe(ok);
      expect(await call(c, Procedures.register, registerPayload(workerType))).toBe(ok);
      expect(await call(c, Procedures.register, registerPayload(otherType))).toBe(ok);
      const addressOf = (type: Name): ServiceAddress =>
        single([...mesh.service.table.values()].filter((r) => r.address.type.equals(type))).address;
      const kept = addressOf(workerType);
      const lost = addressOf(otherType);
      await until(() => received.length === 2, limit);
      expect(received).toEqual([`UP ${kept}`, `UP ${lost}`]);
      expect(mesh.service.watchCount).toBe(3);
      // Still worker-a1, now only for worker-g.
      await worker.updateCredential(
        await issue('worker-a1', [
          scopeOf(Right.register, workerType),
          scopeOf(Right.watch, workerType),
        ]),
      );
      await until(() => mesh.service.table.size === 1, limit);
      expect(addresses(mesh.service)).toEqual([kept.toString()]);
      expect(changes).toEqual([`UP ${kept}`, `UP ${lost}`, `DOWN ${lost}`]);
      // WATCH * and WATCH worker-h are gone, without an event; the DOWN of
      // the record was published after they ended.
      expect(mesh.service.watchCount).toBe(1);
      expect(c.isOpen).toBe(true);
      // Under the new scopes: worker-g events still arrive, others not.
      const admin = node({ credential: await issue('root', adminScopes) });
      const a = channelTo(admin, mesh);
      expect(await call(a, Procedures.register, registerPayload(otherType))).toBe(ok);
      expect(await call(a, Procedures.register, registerPayload(workerType))).toBe(ok);
      const added = single(
        [...mesh.service.table.values()].filter(
          (x) => x.address.type.equals(workerType) && !x.address.equals(kept),
        ),
      ).address;
      await until(() => received.length === 3, limit);
      await delay(20);
      expect(received).toEqual([`UP ${kept}`, `UP ${lost}`, `UP ${added}`]);
      expect(await call(c, Procedures.watch, empty)).toBe(denied);
      expect(await call(c, Procedures.register, registerPayload(otherType))).toBe(denied);
      // The same scopes again change nothing.
      await worker.updateCredential(
        await issue('worker-a1', [
          scopeOf(Right.register, workerType),
          scopeOf(Right.watch, workerType),
        ]),
      );
      expect(await call(c, Procedures.unwatch, new WatchRequest(workerType).encode())).toBe(ok);
      expect(addresses(mesh.service)).toEqual(
        expect.arrayContaining([kept.toString(), added.toString()]),
      );
    },
  );

  test(
    'a record that keeps register keeps its slots; one that loses it goes DOWN and frees ' +
      'them as a lost channel would',
    async () => {
      const mesh = Mesh.start();
      const both = [...workerScopes, scopeOf(Right.claim, 'worker-*')];
      const worker = node({ credential: await issue('worker-a1', both) });
      const joined = join(worker, mesh, { watch: false });
      // MeshNode.publishSharded and claimSlot.
      joined.slotHandler = idle;
      await within(joined.register(workerType, []), limit);
      await within(
        joined.defineSlots(workerType, { count: 4, mode: SlotMode.static, capacity: 0 }),
        limit,
      );
      await within(joined.claim(workerType, 2), limit);
      const id = single(mesh.service.table.values()).address.instance;
      const table = (): SlotTable => must(mesh.service.slotTable(workerType));
      expect(table().get(2).owner).toBe(id);
      // claim gone, register kept: SLOTS is still granted, nothing moves.
      await worker.updateCredential(await issue('worker-a1', workerScopes));
      await delay(20);
      expect(single(mesh.service.table.values()).address.instance).toBe(id);
      expect(table().get(2).owner).toBe(id);
      // register gone: the record goes DOWN and its slot is free, the space
      // stays.
      await worker.updateCredential(await issue('worker-a1', [scopeOf(Right.claim, 'worker-*')]));
      await until(() => mesh.service.table.size === 0, limit);
      expect(table().get(2).owner).toBe(0);
      expect(table().get(2).state).toBe(SlotState.free);
      expect(table().space.count).toBe(4);
    },
  );

  test('a REGISTER held for the assignment hold is refused once its type is no longer granted', async () => {
    const clock = fakeTime();
    const mesh = Mesh.start({ assignmentHold: minute, clock, close: false });
    const worker = node({ credential: await issue('worker-a1', workerScopes), close: false });
    const c = mesh.link(worker);
    // Without the helper's timeout: the hold outlasts it.
    const register = (type: Name): Promise<StatusCode | undefined> =>
      c
        .request(Procedures.register.toString(), registerPayload(type), {
          name: Procedures.register,
        })
        .then<StatusCode | undefined, StatusCode | undefined>(
          () => undefined,
          (e: unknown) =>
            e instanceof SwitchboardError ? (e.code as StatusCode) : StatusCode.unknown,
        );
    let held: StatusCode | undefined;
    let allowed: StatusCode | undefined = StatusCode.unknown;
    void register(otherType).then((code) => {
      held = code;
    });
    void register(workerType).then((code) => {
      allowed = code;
    });
    await clock.elapse(second);
    expect(mesh.service.table.size).toBe(0);
    expect(held).toBeUndefined();
    void issue('worker-a1', [scopeOf(Right.register, workerType)]).then((credential) =>
      worker.updateCredential(credential),
    );
    await clock.elapse(second);
    // At once, not when the hold ends.
    expect(held).toBe(StatusCode.permissionDenied);
    expect(allowed).toBe(StatusCode.unknown);
    expect(mesh.service.table.size).toBe(0);
    await clock.elapse(minute);
    expect(allowed).toBeUndefined();
    expect(single(mesh.service.table.values()).address.type.equals(workerType)).toBe(true);
    void worker.close();
    void mesh.close();
    await clock.elapse(30 * second);
    expect(clock.pendingCount).toBe(0);
  });
});

describe('expiry and RENEW', () => {
  test('connection expiry is enforced while the OPEN credential verifies', async () => {
    const clock = fakeTime();
    const checked = new Deferred<void>();
    const mesh = Mesh.start({
      close: false,
      clock,
      payloadVerifier: new DelayedVerifier(verifier(), checked.promise),
    });
    const worker = node({
      credential: await issue('worker-a1', workerScopes, { lifetime: hour }),
      close: false,
    });
    const payload = await issue('wide', adminScopes, { bearer: true });
    const channel = mesh.link(worker, payload.encode());
    let closed: Status | undefined;
    void channel.done.then((status) => {
      closed = status;
    });
    await clock.elapse(second);
    expect(mesh.service.channelCount).toBe(1);
    await clock.elapse(hour);
    expect(closed?.name).toBe('unauthenticated');
    expect(mesh.service.channelCount).toBe(0);
    checked.resolve();
    await pump();
    expect(mesh.service.channelCount).toBe(0);
    void worker.close();
    void mesh.close();
    await clock.elapse(30 * second);
    expect(clock.pendingCount).toBe(0);
  });

  test('connection expiry cannot fall back to the OPEN credential', async () => {
    const clock = fakeTime();
    const mesh = Mesh.start({ close: false, clock });
    const changes: boolean[] = [];
    mesh.service.on('event', (e) => changes.push(e.up));
    const worker = node({
      credential: await issue('worker-a1', workerScopes, { lifetime: hour }),
      close: false,
    });
    const payload = await issue('wide', adminScopes, { bearer: true });
    const channel = mesh.link(worker, payload.encode());
    let closed: Status | undefined;
    void channel.done.then((status) => {
      closed = status;
    });
    void channel.request('REGISTER', registerPayload(workerType)).catch(() => {});
    await clock.elapse(second);
    expect(mesh.service.table.size).toBe(1);
    await clock.elapse(hour);
    expect(closed?.name).toBe('unauthenticated');
    expect(worker.goAways.map((s) => s.name)).toEqual(['unauthenticated']);
    expect(mesh.service.table.size).toBe(0);
    expect(changes).toEqual([true, false]);
    void worker.close();
    void mesh.close();
    await clock.elapse(30 * second);
    expect(clock.pendingCount).toBe(0);
  });

  test('a replacement IDENT updates expiry without another request', async () => {
    const clock = fakeTime();
    const mesh = Mesh.start({ close: false, clock });
    const worker = node({ credential: await issue('worker-a1', workerScopes), close: false });
    const channel = mesh.link(worker);
    let closed: Status | undefined;
    void channel.done.then((status) => {
      closed = status;
    });
    void (async () => {
      await channel.request('REGISTER', registerPayload(workerType));
      await worker.updateCredential(await issue('worker-a1', workerScopes, { lifetime: hour }));
    })().catch(() => {});
    await clock.elapse(second);
    expect(mesh.service.table.size).toBe(1);
    await clock.elapse(hour);
    expect(closed?.name).toBe('unauthenticated');
    expect(mesh.service.table.size).toBe(0);
    void worker.close();
    void mesh.close();
    await clock.elapse(30 * second);
    expect(clock.pendingCount).toBe(0);
  });

  test('connection expiry sends GOAWAY even with an expired payload', async () => {
    const clock = fakeTime();
    const mesh = Mesh.start({ close: false, clock });
    const client = node({
      credential: await issue('worker-a1', workerScopes, { lifetime: 2 * minute }),
      close: false,
    });
    const payload = await issue('worker-a1', workerScopes, { bearer: true, lifetime: minute });
    const connection = client.connect(mesh);
    const channel = mesh.link(client, payload.encode());
    let ended: Status | undefined;
    void channel.done.then((status) => {
      ended = status;
    });
    void channel.request('REGISTER', registerPayload(workerType)).catch(() => {});
    await clock.elapse(second);
    expect(mesh.service.table.size).toBe(1);
    await clock.elapse(2 * minute);
    expect(ended?.name).toBe('unauthenticated');
    expect(connection.goAways[0]?.name).toBe('unauthenticated');
    expect(mesh.service.table.size).toBe(0);
    void client.close();
    void mesh.close();
    await clock.elapse(30 * second);
    expect(clock.pendingCount).toBe(0);
  });

  for (const initiallyIdentified of [false, true]) {
    test(`an idle session follows IDENT expiry (initially identified: ${initiallyIdentified})`, async () => {
      const clock = fakeTime();
      const mesh = Mesh.start({ requireCredential: false, close: false, clock });
      const client = node({
        ...(initiallyIdentified ? { credential: await issue('worker-a1', workerScopes) } : {}),
        close: false,
      });
      const connection = client.connect(mesh);
      const channel = mesh.link(client);
      let ended: Status | undefined;
      void channel.done.then((status) => {
        ended = status;
      });
      let identified = false;
      void (async () => {
        await channel.request('REGISTER', registerPayload(workerType));
        await connection.identify(await issue('worker-a1', workerScopes, { lifetime: minute }));
        identified = true;
      })().catch(() => {});
      await clock.elapse(second);
      expect(identified).toBe(true);
      expect(mesh.service.table.size).toBe(1);
      // No further request: expiry must follow the new IDENT even when the
      // session was anonymous, or had a later deadline.
      await clock.elapse(minute);
      expect(mesh.service.table.size).toBe(0);
      expect(ended?.name).toBe('unauthenticated');
      expect(connection.goAways[0]?.name).toBe('unauthenticated');
      void channel.close();
      void client.close();
      void mesh.close();
      await clock.elapse(30 * second);
      expect(clock.pendingCount).toBe(0);
    });
  }

  test("RENEW re-issues the channel's credential", async () => {
    // Real credentials: the client decodes the answer with `Credential.decode`.
    const realIssuer = await CredentialIssuer.ed25519FromSeed(
      'auth1',
      Uint8Array.from({ length: 32 }, (_, i) => 7 * i),
    );
    const realVerifier = new Verifier();
    await realVerifier.addIssuer(realIssuer);
    const realIssue = (identity: string, lifetime?: number): Promise<Credential> =>
      realIssuer.issue({
        kind: CredentialKind.node,
        identity,
        scopes: workerScopes,
        holderKey,
        lifetime,
      });
    const mesh = Mesh.start({ issuer: realIssuer, payloadVerifier: realVerifier });
    const original = await realIssue('worker-a1', hour);
    const worker = node({ credential: original });
    const client = join(worker, mesh);
    await until(() => client.isConnected);
    for (const current of [undefined, original]) {
      const renewed = await within(client.renew({ current }), limit);
      expect(renewed.identity).toBe('worker-a1');
      expect(renewed.scopes).toEqual(original.scopes);
      expect(renewed.holderKey).toEqual(original.holderKey);
      expect(renewed.kind).toBe(original.kind);
      expect(renewed.expiresAt - renewed.issuedAt).toBe(3600);
      expect(renewed.issuedAt).toBeGreaterThanOrEqual(original.issuedAt);
    }
    // Another identity's credential is refused, an invalid one too.
    await rejectsWith(
      client.renew({ current: await realIssue('worker-b2') }),
      StatusCode.permissionDenied,
    );
    const forged = original.encode();
    forged[forged.length - 1] = (forged[forged.length - 1] as number) ^ 1;
    await rejectsWith(client.renew({ current: Codec.decode(forged) }), StatusCode.unauthenticated);
    // A naming service without an issuer does not renew.
    const plain = Mesh.start({ withIssuer: false, payloadVerifier: realVerifier });
    const other = join(worker, plain);
    await until(() => other.isConnected);
    await rejectsWith(other.renew(), StatusCode.unimplemented);
    // Not connected.
    const idle = new NamingClient(async () => mesh.link(worker));
    addTearDown(() => idle.close());
    await rejectsWith(idle.renew(), StatusCode.unavailable);
  });

  test(
    'a payload credential is renewed in place; one that is not ends its channel, and only ' +
      'its channel, at its expiry',
    async () => {
      const clock = fakeTime();
      const mesh = Mesh.start({ close: false, clock });
      const client = node({ close: false });
      const bearer = await issue('consumer-2', consumerScopes, { bearer: true, lifetime: hour });
      const renewing = mesh.link(client, bearer.encode());
      const lapsing = mesh.link(client, bearer.encode());
      let lapsed: Status | undefined;
      void lapsing.done.then((status) => {
        lapsed = status;
      });
      void lapsing.request('WATCH', empty).catch(() => {});
      await clock.elapse(50 * minute);
      let renewed: Credential | undefined;
      renewing
        .request('RENEW', empty, { name: Procedures.renew })
        .then((answer) => {
          renewed = decodeFakeCredential(answer.payload);
        })
        .catch(() => {});
      await clock.elapse(second);
      expect(renewed?.identity).toBe('consumer-2');
      expect(renewed?.isBearer).toBe(true);
      expect(renewed?.scopes).toEqual(bearer.scopes);
      expect(must(renewed).expiresAt).toBeGreaterThan(bearer.expiresAt);
      // Past the first expiry: the channel that renewed goes on, the other
      // is closed; the connection carries on.
      await clock.elapse(11 * minute);
      expect(lapsed?.name).toBe('unauthenticated');
      let watched: StatusCode | undefined = StatusCode.unknown;
      renewing
        .request('WATCH', empty)
        .then(() => {
          watched = undefined;
        })
        .catch((e: unknown) => {
          watched = (e as SwitchboardError).code as StatusCode;
        });
      await clock.elapse(second);
      expect(watched).toBeUndefined();
      expect(renewing.isOpen).toBe(true);
      void renewing.close();
      void client.close();
      void mesh.close();
      await clock.elapse(30 * second);
      expect(clock.pendingCount).toBe(0);
    },
  );

  test('RENEW of another credential of the same identity does not become the payload credential', async () => {
    const mesh = Mesh.start();
    const client = node();
    const bearer = await issue('x', workerScopes, { bearer: true });
    // Same identity and kind, wider, and bound to a holder key: it
    // identifies only by IDENT, and proves nothing in a payload.
    const held = await issue('x', adminScopes);
    const c = channelTo(client, mesh, { payload: bearer.encode() });
    expect(await call(c, Procedures.watch, empty)).toBe(denied);
    const answer = await within(
      c.request('RENEW', held.encode(), { name: Procedures.renew }),
      limit,
    );
    const renewed = decodeFakeCredential(answer.payload);
    expect(renewed.holderKey).toEqual(held.holderKey);
    expect(renewed.scopes).toEqual(held.scopes);
    // The channel is still identified by its own bearer credential.
    expect(await call(c, Procedures.watch, empty)).toBe(denied);
    expect(await call(c, Procedures.register, registerPayload(workerType))).toBe(ok);
  });

  // Not ported: NamingClient.renew waits for the credential codec; MeshNode's
  // renewal schedule (`nextRenewalAt`, `Switchboard.credential`) is the node's.
  test.skip(
    'MeshNode renews at two thirds of the lifetime; a credential that is not renewed ends ' +
      'the session at its expiry',
    () => {},
  );
});

describe('CONNECT', () => {
  /** A mesh, an outbound-only worker registered without endpoints, and a consumer listening. */
  async function setUp(options: { consumer?: Scope[] } = {}): Promise<{
    mesh: Mesh;
    worker: Node;
    workerMesh: NamingClient;
    id: number;
    consumer: Node;
    consumerMesh: NamingClient;
  }> {
    const mesh = Mesh.start();
    const worker = node({ credential: await issue('worker-a1', workerScopes) });
    const workerMesh = join(worker, mesh, { watch: false });
    workerMesh.connectHandler = dialBack;
    const id = await within(workerMesh.register(workerType, []), limit);
    const consumer = node({
      credential: await issue('consumer-1', options.consumer ?? consumerScopes),
    });
    listen(consumer);
    const consumerMesh = join(consumer, mesh);
    consumerMesh.connectHandler = dialBack;
    await within(consumerMesh.synced, limit);
    return { mesh, worker, workerMesh, id, consumer, consumerMesh };
  }

  // Not ported: the brokered connection (Switchboard.openTalk through the
  // resolver's CONNECT, MeshNode's dial-back over a real connection, its
  // pooling and channel policy) is the node's.
  test.skip('the consumer opens a channel to an outbound-only worker', () => {});

  // Not ported: the dial-back connection and its GOAWAY
  // (MeshNode.brokeredConnections) are the node's.
  test.skip('a dial-back already received is closed when CONNECT fails', () => {});

  test('refusals', async () => {
    const { mesh, id } = await setUp();
    const consumer = node({ credential: await issue('consumer-2', consumerScopes) });
    const here = listen(consumer);
    const client = join(consumer, mesh);
    await until(() => client.isConnected, limit);
    // No such instance, or not of that type.
    await rejectsWith(
      client.connectTo(workerType, id + 100, here, new Uint8Array(4)),
      StatusCode.notFound,
    );
    await rejectsWith(
      client.connectTo(Name.of('worker-x'), id, here, new Uint8Array(4)),
      StatusCode.notFound,
    );
    // A record with endpoints needs no brokering.
    const lister = node({ credential: await issue('worker-b2', workerScopes) });
    const listerMesh = join(lister, mesh, { watch: false });
    listerMesh.connectHandler = dialBack;
    const listed = await within(listerMesh.register(workerType, ['tcp://10.0.0.9:1']), limit);
    await rejectsWith(
      client.connectTo(workerType, listed, here, new Uint8Array(4)),
      StatusCode.notFound,
    );
    // The worker cannot reach the endpoint.
    await rejectsWith(
      client.connectTo(workerType, id, 'mem://nobody-listens-here', new Uint8Array(4)),
      StatusCode.unavailable,
    );
    // Malformed.
    await expect(client.connectTo(workerType, id, here, new Uint8Array(65))).rejects.toThrow(
      RangeError,
    );
    // Without the broker right.
    const watcher = node({
      credential: await issue('consumer-3', [scopeOf(Right.watch, '*')]),
    });
    const w = join(watcher, mesh);
    await until(() => w.isConnected, limit);
    await rejectsWith(
      w.connectTo(workerType, id, here, new Uint8Array(4)),
      StatusCode.permissionDenied,
    );
  });

  test('a registrant without an identity cannot be brokered', async () => {
    const mesh = Mesh.start({ requireCredential: false });
    const anonymous = node();
    const c = channelTo(anonymous, mesh);
    expect(await call(c, Procedures.register, registerPayload(workerType))).toBe(ok);
    const id = single(mesh.service.table.values()).address.instance;
    expect(
      await call(c, Procedures.connect, new ConnectRequest(workerType, id, 'mem://x').encode()),
    ).toBe('failedPrecondition');
  });

  test('an instance that does not dial back: UNAVAILABLE in time', async () => {
    const mesh = Mesh.start({ brokerTimeout: 200 });
    // A registrant that never answers CONNECT.
    const silent = node({ credential: await issue('worker-a1', workerScopes) });
    const r = channelTo(silent, mesh);
    drain(r);
    expect(await call(r, Procedures.register, registerPayload(workerType))).toBe(ok);
    const id = single(mesh.service.table.values()).address.instance;
    const consumer = node({ credential: await issue('consumer-1', consumerScopes) });
    const here = listen(consumer);
    const joined = join(consumer, mesh);
    await within(joined.synced, limit);
    // The Dart test brokers twice through the node (`broker`, then
    // `openChannel`, both UNAVAILABLE); here the CONNECT each of them asks
    // the naming service.
    for (let i = 0; i < 2; i++) {
      await rejectsWith(
        within(joined.connectTo(workerType, id, here, new Uint8Array(16)), limit),
        StatusCode.unavailable,
      );
    }
  });

  // Not ported: the late dial-back's GOAWAY from the consumer, the
  // consumer's connectTimeout and the instance's failed dial are the node's.
  test.skip(
    "a dial-back that arrives after the consumer gave up is sent GOAWAY, and the instance's " +
      'dial fails',
    () => {},
  );

  // Not ported: Switchboard.broker's preconditions (a brokering resolver, a
  // listener, an instance, a verifier) are the node's.
  test.skip('broker refuses without a brokering resolver, a listener, an instance or a verifier', () => {});

  // Not ported: PeerSet is the node's.
  test.skip('PeerSet brokers, and brokers again after a loss', () => {});
});
