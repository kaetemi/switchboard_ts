/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * `@kaetemi/switchboard/testing`: what tests of the library and of its
 * consumers share. The in-memory transport pair and the memory endpoints
 * are in the core; this entry adds the manual clock, the in-memory QUIC provider
 * (independent stream delivery with configurable delays, flow control, a
 * stream limit, resets and an exporter) and the harness helpers that the
 * threads agree to share. Each thread adds its own under its name.
 */

export { ManualClock } from './manual-clock.ts';
export * from './memory-quic-provider.ts';
