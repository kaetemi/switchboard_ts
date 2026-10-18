/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Review finding (node and clients layer), ergonomics: design section 5
// gives every operation that waits an `AbortSignal` ("connect, open,
// identify, ping, request, resolve"; section 12 item 7), and
// `MuxConnection.identify` takes one, but `Switchboard.identifyOn` (which
// waits for the NONCE exchange and the PONG, up to `identityTimeout`) and
// `Switchboard.resolveRelays` (which waits for the resolver) take none: a
// caller cannot abandon them. The options are cast here, since the
// signatures have no `signal`.
//
// Marked `test.fails`: it passes while the defect is present. The fix
// removes `.fails`.

import { expect, test } from 'vitest';
import { CredentialKind, Right } from '../../../src/core/identity/contract.ts';
import { Ed25519HolderKey } from '../../../src/core/identity/holder-key.ts';
import { Scope } from '../../../src/core/identity/scope.ts';
import { node } from '../../support/node/harness.ts';
import { authority, TestVerifier } from '../../support/node/seeded-identity.ts';

test.fails('identifyOn rejects with the reason of an aborted signal, sending nothing', async () => {
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
  const options = { signal: AbortSignal.abort(reason) } as Parameters<typeof client.identifyOn>[1];
  let error: unknown;
  try {
    await client.identifyOn(connection, options);
  } catch (e) {
    error = e;
  }
  // Fails: the signal is ignored and the identification completes.
  expect(error).toBe(reason);
});
