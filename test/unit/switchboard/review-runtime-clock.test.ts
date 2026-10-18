/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Review finding (node and clients layer), fixed: the in-process transport
// pairs of the node's default runtime run on the node's clock.
// `coreRuntime()` built them with `memoryTransportPair({ delay: 0 })`, that
// is on the system clock, and `SwitchboardRuntime.transportPair` took no
// clock, so a `Switchboard({ clock })` delivered the frames of its `mem://`
// connections and of its connections to its own listeners
// (`localConnections`, the default) on real timers while its mux, pool and
// identity timers ran on the injected clock. Design section 5: every timer
// comes from the `Clock` in the options. The runtime's `transportPair` now
// takes the node's clock, and `coreRuntime()` builds the pair on it (the
// Node and web runtimes spread the core's). Dart's `MemoryTransport` uses
// `Timer`, which `fake_async` drives. The node suites' own runtime
// (`testRuntime` in the harness) keeps real timers, which their manual
// clocks rely on; `steppingRuntime` in `peer-set.test.ts` steps them.

import { expect, test } from 'vitest';
import { ChannelAddress } from '../../../src/core/address/index.ts';
import { Switchboard } from '../../../src/core/switchboard/switchboard.ts';
import { ManualClock } from '../../../src/testing/manual-clock.ts';
import { fast, svc } from '../../support/node/harness.ts';

test('a node on a manual clock delivers in-process frames on that clock', async () => {
  const clock = new ManualClock();
  const server = new Switchboard({ clock, muxOptions: fast });
  const client = new Switchboard({ clock, muxOptions: fast });
  let delivered = false;
  server.registerService(svc, (incoming) => {
    delivered = true;
    void incoming.channel.close();
  });
  const uri = await server.listenMemory();
  const connection = await client.connect(uri);
  connection.open(new ChannelAddress({ type: svc }).encode());
  // Real time passes; the node's clock does not move.
  await new Promise((resolve) => setTimeout(resolve, 20));
  // The OPEN waits for the node's clock, not the system clock's timers.
  const early = delivered;
  // The clock's turn delivers it.
  await clock.elapseAsync(0);
  const onTheClock = delivered;
  const closing = Promise.all([client.close(), server.close()]);
  await clock.elapseAsync(1000);
  await closing;
  expect(early).toBe(false);
  expect(onTheClock).toBe(true);
});
