/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Review finding (node and clients layer), fixed: a node of the Node entry
// that listens on a wildcard address recognises its own host name. `hostsOf`
// in `src/node/switchboard.ts` adds `os.hostname()` with the interface
// addresses of a wildcard bind, as the core's JSDoc of `hostsOf` and
// `isOwnEndpoint` say and as Dart's `_hostsOf` does
// (`Platform.localHostname`). Without it, a wildcard listener advertised
// under the machine's name (what `listenTcp` tells the caller to do) was
// not the node's own: the proxy, the relay and slot gate forwarding could
// connect to themselves through it, and `localConnections` dialled a socket
// instead of the in-process pair.

import { hostname } from 'node:os';
import { expect, test } from 'vitest';
import { Switchboard } from '../../../src/node/switchboard.ts';

test('a wildcard TCP listener is recognised by the host name', async () => {
  const node = new Switchboard({ muxOptions: { keepAliveInterval: undefined } });
  try {
    const endpoint = await node.listenTcp('0.0.0.0', 0);
    const port = new URL(endpoint).port;
    expect(node.isOwnEndpoint(`tcp://127.0.0.1:${port}`)).toBe(true);
    // The machine's name is among the listener's hosts.
    expect(node.isOwnEndpoint(`tcp://${hostname().toLowerCase()}:${port}`)).toBe(true);
  } finally {
    await node.close();
  }
});
