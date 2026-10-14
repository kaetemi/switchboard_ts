/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * `@kaetemi/switchboard/web`: the browser parts, and everything of the core, so that a frontend
 * reaches a node with one import. It adds the connect-only browser `Switchboard` (over `ws`, `wss`
 * and, with a QUIC provider, `https`), and the web QUIC provider over the W3C WebTransport API
 * (`webTransport()`, for `connectQuic` and `quicConnector`, and `webTransportConnector` for a
 * `ReconnectingClient`), with its sessions, streams and the structural types of the API.
 */

export * from '../core/index.ts';
export * from './quic/index.ts';
// The browser node and its options replace the core's of the same names.
export { Switchboard, type SwitchboardOptions } from './switchboard.ts';
