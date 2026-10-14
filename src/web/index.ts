/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * `@kaetemi/switchboard/web`: the browser parts, and everything of the
 * core, so that a frontend reaches a node with one import. The web QUIC
 * provider over the W3C WebTransport API is not here yet: its first piece
 * (`quic/webtransport-api.ts`) is in progress and stays unexported until
 * the provider and its browser test land.
 *
 * Owned by the "QUIC transport" thread.
 */

export * from '../core/index.ts';
// The browser node and its options replace the core's of the same names.
export { Switchboard, type SwitchboardOptions } from './switchboard.ts';
