/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { sharedMemoryEndpoints } from './memory-endpoints.ts';
import { type SwitchboardRuntime, unavailable } from './runtime.ts';

/**
 * The runtime of the core node: the frame mux, in-process transport pairs,
 * the process-wide `mem://` listeners, and no socket dialers (the entry
 * points add them).
 */
export function coreRuntime(): SwitchboardRuntime {
  return {
    mux: () => unavailable('the frame mux'),
    transportPair: () => unavailable('the in-memory transport pair'),
    memoryEndpoints: sharedMemoryEndpoints,
    dialers: {},
  };
}
