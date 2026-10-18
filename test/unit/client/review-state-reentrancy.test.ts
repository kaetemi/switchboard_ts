/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Review finding (node and clients layer): `ReconnectingClient` emits
// `on('state')` synchronously inside its transitions (Dart's `states` is an
// asynchronous broadcast stream, and design section 12 lists synchronous
// events for `PeerSet` only). `#onConnection` reports `connected` before it
// resolves `connected`, so a listener that stops the client, or forces a
// reconnect, when it sees `connected` leaves `connected` resolved with the
// connection it just left; the next connection never resolves it, and
// `await client.connected` hands out a connection that was sent GOAWAY.
//
// Marked `test.fails`: it passes while the defect is present. The fix
// removes `.fails`.

import { expect, test } from 'vitest';
import { ClientPhase, ReconnectingClient } from '../../../src/core/client/index.ts';
import { type MuxConnection, muxOptions } from '../../../src/core/mux/contract.ts';
import type { TransportConnector } from '../../../src/core/transport/contract.ts';
import { memoryTransportPair } from '../../../src/core/transport/memory-transport.ts';
import { ManualClock } from '../../../src/testing/manual-clock.ts';
import { testMux } from '../../support/node/harness.ts';

const quiet = { keepAliveInterval: undefined, keepAliveTimeout: 100, goAwayGrace: 100 };

/** A connector whose every transport an endpoint connection accepts. */
function endpoint(clock: ManualClock): { connect: TransportConnector; accepted: MuxConnection[] } {
  const accepted: MuxConnection[] = [];
  const options = muxOptions({ ...quiet, clock });
  return {
    accepted,
    connect: async () => {
      const [client, server] = memoryTransportPair();
      accepted.push(testMux()(server, options, false));
      return client;
    },
  };
}

function clientOf(clock: ManualClock, connect: TransportConnector): ReconnectingClient {
  return new ReconnectingClient(connect, {
    muxOptions: quiet,
    jitter: 0,
    clock,
    mux: testMux(),
  });
}

test.fails('stop() from a connected listener, then start(): connected is the new connection', async () => {
  const clock = new ManualClock();
  const { connect } = endpoint(clock);
  const client = clientOf(clock, connect);
  let first: MuxConnection | undefined;
  const off = client.on('state', (state) => {
    if (state.phase === ClientPhase.connected && first === undefined) {
      first = client.connection;
      client.stop();
    }
  });
  await ManualClock.settle();
  off();
  expect(first).toBeDefined();
  expect(client.state.phase).toBe(ClientPhase.disconnected);
  client.start();
  await ManualClock.settle();
  expect(client.state.phase).toBe(ClientPhase.connected);
  const current = client.connection;
  expect(current).toBeDefined();
  expect(current).not.toBe(first);
  const connected = await client.connected;
  const closed = client.close();
  await clock.elapseAsync(500);
  await closed;
  // Fails: `connected` still holds the connection the listener stopped.
  expect(connected).toBe(current);
});

test.fails('reconnectNow({ force }) from a connected listener: connected is the replacement', async () => {
  const clock = new ManualClock();
  const { connect } = endpoint(clock);
  const client = clientOf(clock, connect);
  let first: MuxConnection | undefined;
  const off = client.on('state', (state) => {
    if (state.phase === ClientPhase.connected && first === undefined) {
      first = client.connection;
      client.reconnectNow({ force: true });
    }
  });
  await ManualClock.settle();
  off();
  expect(client.state.phase).toBe(ClientPhase.connected);
  const current = client.connection;
  expect(current).not.toBe(first);
  expect(first?.isOpen).toBe(false);
  const connected = await client.connected;
  const closed = client.close();
  await clock.elapseAsync(500);
  await closed;
  // Fails: `connected` resolved with the connection the client left.
  expect(connected).toBe(current);
});
