/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Review finding (node and clients layer): credential expiry has no single
// wall clock. `CredentialVerifier({ wallTime })` checks expiry at the wall
// time it is given, but `MuxConnection.peerIdentity` (frame mux and QUIC
// binding alike) checks the same credential with `isExpired()`, that is
// `Date.now()`, and `MuxOptions` has no wall time to pass. A node whose
// verifier runs on another wall time (a trusted time source, or virtual
// time in a test) admits an IDENT and then treats the peer as unidentified:
// its policies hold and refuse the peer's channels. Dart reads one clock
// (`package:clock`) in both places.
//
// Marked `test.fails`: it passes while the defect is present. The fix
// removes `.fails`.

import { expect, test } from 'vitest';
import { ChannelAddress } from '../../../src/core/address/index.ts';
import { CredentialKind, Right } from '../../../src/core/identity/contract.ts';
import { Ed25519HolderKey } from '../../../src/core/identity/holder-key.ts';
import { Scope } from '../../../src/core/identity/scope.ts';
import { ChannelPolicies } from '../../../src/core/switchboard/channel-policy.ts';
import { node, svc, tagged, tagOf, useMux } from '../../support/node/harness.ts';
import { authority, TestVerifier } from '../../support/node/seeded-identity.ts';

useMux('frame');

/** 2020-01-01T00:00:00Z: the wall time of the verifier, years before `Date.now()`. */
const wall = Date.UTC(2020, 0, 1);

test.fails("a peer identified under the verifier's wall time stays identified", async () => {
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
  // ...but its peerIdentity reads the credential expired by Date.now(): the
  // policy holds the channel and refuses it. Fails: 'closed unauthenticated'.
  expect(await tagOf(channel)).toBe('served');
});
