/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { FrameMuxConnection } from '../mux/frame-mux-connection.ts';
import { MemoryEndpoints } from '../transport/memory-endpoints.ts';
import { memoryTransportPair } from '../transport/memory-transport.ts';
import type { SwitchboardRuntime } from './runtime.ts';

/**
 * The runtime of the core node: the frame mux, in-process transport pairs
 * whose frames arrive on the next turn of the node's clock (as from a
 * socket), the process-wide `mem://` listeners, and no socket dialers (the
 * entry points add them).
 */
export function coreRuntime(): SwitchboardRuntime {
  return {
    mux: (transport, options, isInitiator) =>
      new FrameMuxConnection(transport, { ...options, isInitiator }),
    transportPair: (clock) => memoryTransportPair({ delay: 0, clock }),
    memoryEndpoints: MemoryEndpoints,
    dialers: {},
  };
}
