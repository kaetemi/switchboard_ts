/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Review finding (node and clients layer): the in-process transport pairs
// of the node's default runtime ignore the node's clock. `coreRuntime()`
// builds them with `memoryTransportPair({ delay: 0 })`, that is on the
// system clock, and `SwitchboardRuntime.transportPair` takes no clock, so a
// `Switchboard({ clock })` delivers the frames of its `mem://` connections
// and of its connections to its own listeners (`localConnections`, the
// default) on real timers while its mux, pool and identity timers run on
// the injected clock. Design section 5: every timer comes from the `Clock`
// in the options. Dart's `MemoryTransport` uses `Timer`, which
// `fake_async` drives. The node test suites work around it with a runtime
// of their own (`steppingRuntime` in `peer-set.test.ts`).
//
// Marked `test.fails`: it passes while the defect is present. The fix
// removes `.fails`.

import { expect, test } from 'vitest';
import { ChannelAddress } from '../../../src/core/address/index.ts';
import { Switchboard } from '../../../src/core/switchboard/switchboard.ts';
import { ManualClock } from '../../../src/testing/manual-clock.ts';
import { fast, svc } from '../../support/node/harness.ts';

test.fails('a node on a manual clock delivers in-process frames on that clock', async () => {
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
  // Fails: the OPEN went through the system clock's timers.
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
