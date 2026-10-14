/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Port of the Dart `slot_gate_credential_test.dart`: requests a slot gate
// forwards to the new owner (queued at DRAIN, or arriving during the
// forwarding grace) present the credential of the channel they arrived on,
// the application payload of its OPEN, and never the forwarding node's own
// default payload: a client request relayed by a frontend reaches the new
// owner as the client's. Three nodes over `mem` endpoints: the frontend
// that opens the clients' channels, the old owner (kv/1, with the gate) and
// the new owner (kv/2), both backends with a service token as their default
// payload. Over the toy mux and, from `over-frame-mux/`, over the frame
// mux. The idle second of the forwarding channels runs on the gate's own
// clock (Dart waits for it in real time).

import { beforeEach, expect, test } from 'vitest';
import { ChannelAddress, ServiceAddress } from '../../../src/core/address/index.ts';
import { utf8Decode } from '../../../src/core/bytes.ts';
import { Name } from '../../../src/core/name.ts';
import { NamingClient } from '../../../src/core/naming/naming-client.ts';
import { AssignResult } from '../../../src/core/naming/naming-client-slots.ts';
import {
  AssignRequest,
  DrainRequest,
  ForwardRequest,
  serviceRecord,
} from '../../../src/core/naming/naming-protocol.ts';
import { SlotGate, SlotGateState, SlotLifecycle } from '../../../src/core/naming/slot-gate.ts';
import {
  Status,
  StatusCode,
  type SwitchboardError,
  statusCodeName,
} from '../../../src/core/status.ts';
import type { IncomingChannel } from '../../../src/core/switchboard/contract.ts';
import { StaticResolver } from '../../../src/core/switchboard/resolver.ts';
import type { Switchboard } from '../../../src/core/switchboard/switchboard.ts';
import type { TalkChannel, TalkMessage } from '../../../src/core/talk/contract.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';
import { ManualClock } from '../../../src/testing/manual-clock.ts';
import { bytes, node, onCleanup } from '../../support/node/harness.ts';

const kv = Name.of('kv');
const limit = 5000;

function text(data: Uint8Array): string {
  return utf8Decode(data);
}

/** Waits until [condition] holds; fails after [limit] milliseconds. */
async function until(condition: () => boolean): Promise<void> {
  const deadline = performance.now() + limit;
  while (!condition()) {
    if (performance.now() > deadline) {
      throw new Error(`condition not met within ${limit} ms`);
    }
    await new Promise((resolve) => setTimeout(resolve, 2));
  }
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Answers [m] as a backend that checks the [credential] of its caller:
 * `APPLY` is a client procedure, refused to a service token with
 * `PERMISSION_DENIED` and to no token with `UNAUTHENTICATED`; a client token
 * gets `<who>:<token>:<payload>`.
 */
function answer(m: TalkMessage, credential: Uint8Array, who: string): void {
  if (!m.canReply) {
    return;
  }
  const token = text(credential);
  if (token.length === 0) {
    void m.replyAbort(Status.of(StatusCode.unauthenticated));
  } else if (!token.startsWith('client-')) {
    void m.replyAbort(Status.of(StatusCode.permissionDenied, `not for ${token}`));
  } else {
    void m.reply(`${who}:${token}:${m.text()}`);
  }
}

/** The old owner's application: every channel is long-lived, its requests gated by `SlotGate.serveRequest` and answered by {@link answer}. */
class Backend extends SlotLifecycle {
  /** Channels served. */
  served = 0;
  /**
   * The caller's credential when it is not the channel's (a relay that
   * passes it in-band): checked here and given to `SlotGate.serveRequest`.
   */
  credential: Uint8Array | undefined;

  override async load(): Promise<AssignResult> {
    return AssignResult.holding;
  }

  override serve(channel: IncomingChannel, slot: number): void {
    this.served++;
    this.gate.detach(channel);
    void channel
      .talk()
      .serve((m) =>
        this.gate.serveRequest(
          m,
          slot,
          (request) => answer(request, this.credential ?? channel.address.payload, 'A'),
          { payload: this.credential },
        ),
      );
  }
}

/** The answer to `APPLY payload` on [talk]: the reply text, or the name of the abort code. */
function apply(talk: TalkChannel, payload: string): Promise<string | undefined> {
  return talk.request('APPLY', payload).then(
    (m) => m.text(),
    (e: unknown) => statusCodeName((e as SwitchboardError).code),
  );
}

let front: Switchboard;
let from: Switchboard;
let fromUri: string;
let backend: Backend;
let gate: SlotGate;
let clock: ManualClock;
/** Channels the new owner received. */
let arrived: IncomingChannel[];
/** While set, the new owner holds `APPLY hold` until it resolves. */
let hold: Deferred<void> | undefined;

beforeEach(async () => {
  hold = undefined;
  arrived = [];
  const to = node({ defaultPayload: bytes('service') });
  to.registerService(
    kv,
    (incoming) => {
      arrived.push(incoming);
      void incoming.talk().serve(async (m) => {
        if (m.text() === 'hold') {
          await hold?.promise;
        }
        answer(m, incoming.address.payload, 'B');
      });
    },
    { instance: 2 },
  );
  const toUri = await to.listenMemory();
  const resolver = new StaticResolver([
    serviceRecord(new ServiceAddress(kv, 2), { endpoints: [toUri] }),
  ]);
  onCleanup(() => resolver.close());
  from = node({ defaultPayload: bytes('service'), resolver });
  fromUri = await from.listenMemory();
  const client = new NamingClient(async () => {
    throw new Error('not connected');
  });
  onCleanup(() => client.close());
  backend = new Backend();
  clock = new ManualClock();
  gate = new SlotGate(from, client, kv, {
    lifecycle: backend,
    instance: 1,
    trackChannels: false,
    clock,
  });
  const created = gate;
  onCleanup(() => created.close());
  from.registerService(kv, gate.handler, { instance: 1 });
  await gate.onAssign(new AssignRequest(kv, 1, { epoch: 1, holder: 0, shared: false }));
  // A frontend: each channel carries its client's token, none its own.
  front = node();
});

/** A client's channel to slot 1, relayed by the frontend with [token]. */
function relay(token: string): Promise<TalkChannel> {
  return front.openTalkAt(
    fromUri,
    new ChannelAddress({ type: kv, instance: 1, shard: 1, payload: bytes(token) }),
  );
}

async function handOver(): Promise<void> {
  await gate.onDrain(new DrainRequest(kv, 1, { epoch: 2, to: 2 }));
  await gate.onForward(new ForwardRequest(kv, 1, { epoch: 2, to: 2 }));
}

test("a request forwarded during the grace period presents its client's token", async () => {
  const talk = await relay('client-77');
  expect(await apply(talk, 'a')).toBe('A:client-77:a');
  await handOver();
  expect(gate.stateOf(1)).toBe(SlotGateState.forwarding);
  // Forwarded with the old owner's service token, it would be refused.
  expect(await apply(talk, 'b')).toBe('B:client-77:b');
  expect(arrived).toHaveLength(1);
  expect(text((arrived[0] as IncomingChannel).address.payload)).toBe('client-77');
  expect(arrived[0]?.address.instance).toBe(2);
  expect(arrived[0]?.address.shard).toBe(1);
  await talk.close();
});

test("requests queued at DRAIN are forwarded with their clients' tokens on FORWARD", async () => {
  const first = await relay('client-77');
  const second = await relay('client-88');
  expect(await apply(first, 'a')).toBe('A:client-77:a');
  expect(await apply(second, 'a')).toBe('A:client-88:a');
  await gate.onDrain(new DrainRequest(kv, 1, { epoch: 2, to: 2 }));
  const queued = [apply(first, 'q'), apply(second, 'r'), apply(first, 's')];
  await delay(10);
  expect(arrived).toEqual([]);
  await gate.onForward(new ForwardRequest(kv, 1, { epoch: 2, to: 2 }));
  expect(await Promise.all(queued)).toEqual(['B:client-77:q', 'B:client-88:r', 'B:client-77:s']);
  expect(arrived.map((c) => text(c.address.payload)).sort()).toEqual(['client-77', 'client-88']);
  await first.close();
  await second.close();
});

test('one forwarding channel per token, each closed once idle', async () => {
  const a = await relay('client-77');
  const b = await relay('client-77');
  const c = await relay('client-88');
  for (const talk of [a, b, c]) {
    await apply(talk, 'prime');
  }
  expect(backend.served).toBe(3);
  await handOver();
  expect(await Promise.all([apply(a, 'a'), apply(b, 'b'), apply(c, 'c')])).toEqual([
    'B:client-77:a',
    'B:client-77:b',
    'B:client-88:c',
  ]);
  // The two channels with the same token share one.
  expect(arrived.map((channel) => text(channel.address.payload)).sort()).toEqual([
    'client-77',
    'client-88',
  ]);
  // About a second without a request: each is closed. The gate's clock has
  // the grace period and the two idle timers.
  await until(() => clock.pendingCount === 3);
  clock.advance(1000);
  for (const forwarded of [...arrived]) {
    expect((await forwarded.channel.done).isOk).toBe(true);
  }
  expect(await apply(c, 'again')).toBe('B:client-88:again');
  expect(arrived).toHaveLength(3);
  expect(text((arrived.at(-1) as IncomingChannel).address.payload)).toBe('client-88');
  for (const talk of [a, b, c]) {
    await talk.close();
  }
});

test("a client without a token is forwarded without one, not with the node's", async () => {
  const talk = await relay('');
  expect(await apply(talk, 'a')).toBe('unauthenticated');
  expect(backend.served).toBe(1);
  await handOver();
  expect(await apply(talk, 'b')).toBe('unauthenticated');
  expect(arrived).toHaveLength(1);
  expect(arrived[0]?.address.payload).toHaveLength(0);
  await talk.close();
});

test("an explicit payload is presented instead of the channel's", async () => {
  // A relay that passes its client's token in-band, on a channel that
  // carries its own service token.
  backend.credential = bytes('client-5');
  const talk = await relay('service');
  expect(await apply(talk, 'a')).toBe('A:client-5:a');
  await gate.onDrain(new DrainRequest(kv, 1, { epoch: 2, to: 2 }));
  const queued = apply(talk, 'q');
  await delay(10);
  await gate.onForward(new ForwardRequest(kv, 1, { epoch: 2, to: 2 }));
  expect(await queued).toBe('B:client-5:q');
  expect(await apply(talk, 'g')).toBe('B:client-5:g');
  expect(arrived).toHaveLength(1);
  expect(text((arrived[0] as IncomingChannel).address.payload)).toBe('client-5');
  await talk.close();
});

test('a forwarding channel outlives the slot until its request is answered', async () => {
  const talk = await relay('client-77');
  expect(await apply(talk, 'a')).toBe('A:client-77:a');
  await handOver();
  hold = new Deferred<void>();
  const held = apply(talk, 'hold');
  await until(() => arrived.length > 0);
  await gate.onRevoke(kv, 1);
  expect(gate.stateOf(1)).toBeUndefined();
  await delay(10);
  const forwarded = arrived[0] as IncomingChannel;
  expect(forwarded.channel.canSend).toBe(true);
  hold.resolve();
  expect(await held).toBe('B:client-77:hold');
  expect((await forwarded.channel.done).isOk).toBe(true);
  await talk.close();
});
