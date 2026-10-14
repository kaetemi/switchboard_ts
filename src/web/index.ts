/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * `@polyverse/switchboard/web`: the browser parts. The web QUIC provider
 * over the W3C WebTransport API (`WebQuicProvider`, `connectWebTransport`)
 * and everything of the core, so that a frontend reaches a node over
 * WebTransport or WebSocket with one import.
 *
 * Owned by the "QUIC transport" thread.
 */

export * from '../core/index.ts';
