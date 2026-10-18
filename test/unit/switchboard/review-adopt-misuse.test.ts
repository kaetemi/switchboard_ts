/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Review finding (node and clients layer), fixed: `Switchboard.adopt` of a
// connection whose `incoming` something else reads already resolved, and
// the node then never dispatched a channel on it: `#dispatchAll` failed on
// the queue's second consumer and only logged it at debug. Dart's `adopt`
// throws `StateError` (listening twice to a single-subscription stream). The
// node now takes `incoming`'s iterator before it adopts anything, so the
// binding's refusal of a second consumer (a plain `Error`) rejects `adopt`
// and the connection is not adopted.

import { expect, test } from 'vitest';
import { muxOptions } from '../../../src/core/mux/contract.ts';
import { FrameMuxConnection } from '../../../src/core/mux/frame-mux-connection.ts';
import { memoryTransportPair } from '../../../src/core/transport/memory-transport.ts';
import { fast, node, onCleanup, pump } from '../../support/node/harness.ts';

test('adopting a connection whose incoming is read elsewhere is refused', async () => {
  const [a, b] = memoryTransportPair({ delay: 0 });
  const accepted = new FrameMuxConnection(a, { ...muxOptions(fast), isInitiator: false });
  const peer = new FrameMuxConnection(b, { ...muxOptions(fast), isInitiator: true });
  onCleanup(() => Promise.all([peer.close(), accepted.close()]));
  void (async () => {
    for await (const channel of accepted.incoming) {
      void channel.close();
    }
  })();
  await pump(2);
  const n = node();
  let error: unknown;
  try {
    await n.adopt(accepted);
  } catch (e) {
    error = e;
  }
  // Refused with a plain Error, and not adopted: a second attempt is refused
  // the same way, not as a connection the node owns already (TypeError).
  expect(error).toBeInstanceOf(Error);
  expect(error).not.toBeInstanceOf(TypeError);
  await expect(n.adopt(accepted)).rejects.toThrow('one consumer');
});
