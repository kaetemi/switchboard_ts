/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Review finding (node and clients layer): `Switchboard.adopt` of a
// connection whose `incoming` something else reads already resolves, and
// the node then never dispatches a channel on it: `#dispatchAll` fails on
// the queue's second consumer and only logs it at debug. Dart's `adopt`
// throws `StateError` (listening twice to a single-subscription stream), so
// the misuse is reported to the caller.
//
// Marked `test.fails`: it passes while the defect is present. The fix
// removes `.fails`.

import { expect, test } from 'vitest';
import { muxOptions } from '../../../src/core/mux/contract.ts';
import { FrameMuxConnection } from '../../../src/core/mux/frame-mux-connection.ts';
import { memoryTransportPair } from '../../../src/core/transport/memory-transport.ts';
import { fast, node, onCleanup, pump } from '../../support/node/harness.ts';

test.fails('adopting a connection whose incoming is read elsewhere is refused', async () => {
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
  // Fails: adopt resolves; the node silently dispatches nothing on it.
  expect(error).toBeInstanceOf(Error);
});
