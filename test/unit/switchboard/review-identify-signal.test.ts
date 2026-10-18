/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Review finding (node and clients layer), ergonomics, fixed: design
// section 5 gives every operation that waits an `AbortSignal` ("connect,
// open, identify, ping, request, resolve"; section 12 item 7), and
// `MuxConnection.identify` takes one, but `Switchboard.identifyOn` (which
// waits for the NONCE exchange and the PONG, up to `identityTimeout`) and
// `Switchboard.resolveRelays` (which waits for the resolver) took none: a
// caller could not abandon them. Both now take `signal` and reject with its
// reason.

import { expect, test } from 'vitest';
import { CredentialKind, Right } from '../../../src/core/identity/contract.ts';
import { Ed25519HolderKey } from '../../../src/core/identity/holder-key.ts';
import { Scope } from '../../../src/core/identity/scope.ts';
import { muxOptions } from '../../../src/core/mux/contract.ts';
import { FrameMuxConnection } from '../../../src/core/mux/frame-mux-connection.ts';
import type { Resolver, ServiceRecord } from '../../../src/core/switchboard/contract.ts';
import { RelayConfig } from '../../../src/core/switchboard/relay-config.ts';
import { memoryTransportPair } from '../../../src/core/transport/memory-transport.ts';
import { fast, node, onCleanup, pump } from '../../support/node/harness.ts';
import { authority, TestVerifier } from '../../support/node/seeded-identity.ts';

test('identifyOn rejects with the reason of an aborted signal, sending nothing', async () => {
  const key = await Ed25519HolderKey.fromSeed(new Uint8Array(32).fill(7));
  const credential = await authority.issue({
    kind: CredentialKind.node,
    identity: 'npc-7',
    scopes: [Scope.of(Right.open, '*')],
    holderKey: key.publicKey,
  });
  const server = node({ verifier: new TestVerifier() });
  const uri = await server.listenMemory();
  const client = node({ credential, holderKey: key, identifyOutgoing: false });
  const connection = await client.connect(uri);
  const reason = new Error('the caller gave up');
  let error: unknown;
  try {
    await client.identifyOn(connection, { signal: AbortSignal.abort(reason) });
  } catch (e) {
    error = e;
  }
  expect(error).toBe(reason);
});

test('identifyOn rejects with the reason of a signal that aborts during the exchange', async () => {
  const key = await Ed25519HolderKey.fromSeed(new Uint8Array(32).fill(7));
  const credential = await authority.issue({
    kind: CredentialKind.node,
    identity: 'npc-7',
    scopes: [Scope.of(Right.open, '*')],
    holderKey: key.publicKey,
  });
  const client = node({ credential, holderKey: key });
  // A peer that reads every frame and answers none, the NONCE included.
  const [a, b] = memoryTransportPair({ delay: 0 });
  void b.readable.pipeTo(new WritableStream()).catch(() => {});
  const connection = new FrameMuxConnection(a, {
    ...muxOptions({ ...fast, keepAliveTimeout: 100 }),
    isInitiator: true,
  });
  onCleanup(() => connection.close());
  await client.adopt(connection);
  const controller = new AbortController();
  const identifying = client.identifyOn(connection, { signal: controller.signal });
  await pump(5);
  const reason = new Error('the caller gave up');
  controller.abort(reason);
  await expect(identifying).rejects.toBe(reason);
});

test('resolveRelays rejects with the reason of a signal that aborts while the resolver waits', async () => {
  const asked: string[] = [];
  const resolver: Resolver = {
    resolve: (type) => {
      asked.push(type.toString());
      return new Promise<ServiceRecord[]>(() => {});
    },
    events: { [Symbol.asyncIterator]: () => ({ next: () => new Promise(() => {}) }) },
    ready: Promise.resolve(),
    close: () => Promise.resolve(),
  };
  const n = node({ relay: new RelayConfig() });
  const controller = new AbortController();
  const resolving = n.resolveRelays({ resolver, signal: controller.signal });
  await pump(2);
  expect(asked).toHaveLength(1);
  const reason = new Error('the caller gave up');
  controller.abort(reason);
  await expect(resolving).rejects.toBe(reason);
  // Aborted already: the resolver is not asked.
  await expect(n.resolveRelays({ resolver, signal: AbortSignal.abort(reason) })).rejects.toBe(
    reason,
  );
  expect(asked).toHaveLength(1);
});
