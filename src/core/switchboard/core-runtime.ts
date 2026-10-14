/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { MemoryEndpoints } from '../transport/memory-endpoints.ts';
import { memoryTransportPair } from '../transport/memory-transport.ts';
import { type SwitchboardRuntime, unavailable } from './runtime.ts';

/**
 * The runtime of the core node: the frame mux, in-process transport pairs
 * whose frames arrive on the next turn of the event loop (as from a
 * socket), the process-wide `mem://` listeners, and no socket dialers (the
 * entry points add them).
 */
export function coreRuntime(): SwitchboardRuntime {
  return {
    mux: () => unavailable('the frame mux'),
    transportPair: () => memoryTransportPair({ delay: 0 }),
    memoryEndpoints: MemoryEndpoints,
    dialers: {},
  };
}
