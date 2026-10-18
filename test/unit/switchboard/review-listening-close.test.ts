/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Review finding (node and clients layer), fixed: `addListeningEndpoint`
// checked that the node is open, awaited `hostsOf`, and only then listed the
// endpoint and recorded it as the node's own, so a `close()` that started
// during that await cleared the lists first and the endpoint was added
// after close: listed by `listeningEndpoints` ("until close"), recognised by
// `isOwnEndpoint` ("nothing matches once close has started"), and making
// `canBroker` true on a closed node. It now records the endpoint before the
// await, as Dart does, so `close` clears it.

import { expect, test } from 'vitest';
import { node } from '../../support/node/harness.ts';

test('an endpoint declared while close() starts is not the closed node own', async () => {
  const n = node();
  const declaring = n.addListeningEndpoint('quic://127.0.0.1:4433');
  const closing = n.close();
  await declaring.catch(() => {});
  await closing;
  // The close forgot it with the rest.
  expect(n.listeningEndpoints).toEqual([]);
  expect(n.isOwnEndpoint('quic://127.0.0.1:4433')).toBe(false);
  expect(n.canBroker).toBe(false);
});
