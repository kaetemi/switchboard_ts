/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Review finding (node and clients layer): a node of the Node entry that
// listens on a wildcard address does not recognise its own host name.
// `hostsOf` in `src/node/switchboard.ts` adds the interface addresses of a
// wildcard bind but not `os.hostname()`, although the core's JSDoc of
// `hostsOf` and `isOwnEndpoint` say the Node entry adds "the host name and
// the addresses of the network interfaces", as Dart's `_hostsOf` does
// (`Platform.localHostname`). A wildcard listener advertised under the
// machine's name (what `listenTcp` tells the caller to do) is then not the
// node's own: the proxy, the relay and slot gate forwarding may connect to
// themselves through it, and `localConnections` dials a socket instead of
// the in-process pair.
//
// Marked `test.fails`: it passes while the defect is present. The fix
// removes `.fails`.

import { hostname } from 'node:os';
import { expect, test } from 'vitest';
import { Switchboard } from '../../../src/node/switchboard.ts';

test.fails('a wildcard TCP listener is recognised by the host name', async () => {
  const node = new Switchboard({ muxOptions: { keepAliveInterval: undefined } });
  try {
    const endpoint = await node.listenTcp('0.0.0.0', 0);
    const port = new URL(endpoint).port;
    expect(node.isOwnEndpoint(`tcp://127.0.0.1:${port}`)).toBe(true);
    // Fails: the machine's name is not among the listener's hosts.
    expect(node.isOwnEndpoint(`tcp://${hostname().toLowerCase()}:${port}`)).toBe(true);
  } finally {
    await node.close();
  }
});
