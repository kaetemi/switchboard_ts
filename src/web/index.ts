/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * `@kaetemi/switchboard/web`: the browser parts, and everything of the core, so that a frontend
 * reaches a node with one import. It adds the web QUIC provider over the W3C WebTransport API
 * (`webTransport()`, for `connectQuic` and `quicConnector`, and `webTransportConnector` for a
 * `ReconnectingClient`), with its sessions, streams and the structural types of the API.
 *
 * Owned by the "QUIC transport" thread (`quic/`).
 */

export * from '../core/index.ts';
export * from './quic/index.ts';
