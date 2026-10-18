/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Review finding (node and clients layer), fixed: credential expiry is read
// on one wall clock. `CredentialVerifier({ wallTime })` checks expiry at the
// wall time it is given, and `MuxConnection.peerIdentity` (frame mux and
// QUIC binding alike) checked the same credential with `isExpired()`, that
// is `Date.now()`, so a node whose verifier runs on another wall time (a
// trusted time source, or virtual time in a test) admitted an IDENT and
// then treated the peer as unidentified: its policies held and refused the
// peer's channels. `MuxOptions.wallTime` is now the clock of both, and the
// node takes its verifier's when the mux options give none. Dart reads one
// clock (`package:clock`) in both places.

import { expect, test } from 'vitest';
import { ChannelAddress } from '../../../src/core/address/index.ts';
import { CredentialKind, Right } from '../../../src/core/identity/contract.ts';
import { Ed25519HolderKey } from '../../../src/core/identity/holder-key.ts';
import { Scope } from '../../../src/core/identity/scope.ts';
import { QuicBindingConnection } from '../../../src/core/quic/quic-binding-connection.ts';
import { ChannelPolicies } from '../../../src/core/switchboard/channel-policy.ts';
import { MemoryQuicSession } from '../../../src/testing/memory-quic-provider.ts';
import { node, svc, tagged, tagOf, useMux } from '../../support/node/harness.ts';
import { authority, TestVerifier } from '../../support/node/seeded-identity.ts';

useMux('frame');

/** 2020-01-01T00:00:00Z: the wall time of the verifier, years before `Date.now()`. */
const wall = Date.UTC(2020, 0, 1);

test("a peer identified under the verifier's wall time stays identified", async () => {
  const key = await Ed25519HolderKey.fromSeed(new Uint8Array(32).fill(9));
  const credential = await authority.issue({
    kind: CredentialKind.node,
    identity: 'worker-1',
    scopes: [Scope.of(Right.open, '*')],
    holderKey: key.publicKey,
    issuedAt: wall,
    lifetime: 3_600_000,
  });
  const verifier = new TestVerifier({ wallTime: () => wall + 1000 });
  // The verifier accepts the credential at its wall time.
  expect((await verifier.verify(credential.encode())).identity).toBe('worker-1');
  const server = node({ verifier, identityTimeout: 200 });
  server.registerService(svc, tagged('served'));
  const uri = await server.listenMemory({ policy: ChannelPolicies.requireIdentity() });
  const client = node({ credential, holderKey: key });
  const connection = await client.connect(uri);
  // The IDENT verified (connect identified the node), so the server knows the peer...
  const channel = connection.open(new ChannelAddress({ type: svc }).encode());
  // ...and its peerIdentity reads the credential on the verifier's wall time,
  // so the policy lets the channel through.
  expect(await tagOf(channel)).toBe('served');
});

test('the QUIC binding reads peerIdentity on MuxOptions.wallTime', async () => {
  const key = await Ed25519HolderKey.fromSeed(new Uint8Array(32).fill(9));
  const credential = await authority.issue({
    kind: CredentialKind.node,
    identity: 'worker-1',
    scopes: [Scope.of(Right.open, '*')],
    holderKey: key.publicKey,
    issuedAt: wall,
    lifetime: 3_600_000,
  });
  const quiet = { keepAliveInterval: undefined, goAwayGrace: 100 };
  const [a, b] = MemoryQuicSession.pair();
  const accepting = QuicBindingConnection.start(b, {
    options: { ...quiet, identityVerifier: new TestVerifier(), wallTime: () => wall + 1000 },
  });
  const connection = await QuicBindingConnection.start(a, { options: quiet });
  const accepted = await accepting;
  try {
    await connection.identify(credential, { holderKey: key });
    const identity = await accepted.peerIdentified;
    // Verified at the connection's wall time, and still identified on it.
    expect(identity.verifiedAt).toBe(wall + 1000);
    expect(accepted.peerIdentity?.identity).toBe('worker-1');
  } finally {
    await Promise.all([connection.close(), accepted.close()]);
  }
});
