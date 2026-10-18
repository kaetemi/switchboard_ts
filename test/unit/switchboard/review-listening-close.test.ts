/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Review finding (node and clients layer): `addListeningEndpoint` checks
// that the node is open, awaits `hostsOf`, and only then lists the endpoint
// and records it as the node's own. A `close()` that starts during that
// await clears the lists first, so the endpoint is added after close: it
// is listed by `listeningEndpoints` ("until close"), recognised by
// `isOwnEndpoint` ("nothing matches once close has started") and makes
// `canBroker` true on a closed node, and the call resolves instead of
// failing. Dart records the endpoint before its await, so `close` clears it.
//
// Marked `test.fails`: it passes while the defect is present. The fix
// removes `.fails`.

import { expect, test } from 'vitest';
import { node } from '../../support/node/harness.ts';

test.fails('an endpoint declared while close() starts is not the closed node own', async () => {
  const n = node();
  const declaring = n.addListeningEndpoint('quic://127.0.0.1:4433');
  const closing = n.close();
  await declaring.catch(() => {});
  await closing;
  // Fails: ['quic://127.0.0.1:4433'].
  expect(n.listeningEndpoints).toEqual([]);
  expect(n.isOwnEndpoint('quic://127.0.0.1:4433')).toBe(false);
  expect(n.canBroker).toBe(false);
});
