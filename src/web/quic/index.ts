/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * The QUIC parts of `@kaetemi/switchboard/web`: the web provider over the browser's W3C
 * WebTransport API (`webTransport()`, a `QuicConnector` for `connectQuic` and `quicConnector`),
 * `webTransportConnector` for a `ReconnectingClient`, the provider's sessions and streams, and
 * the structural types of the API it uses (what a test or another runtime injects in place of
 * the browser's `WebTransport`).
 *
 * Owned by the "QUIC transport" thread.
 */

export type {
  BidirectionalStreamLike,
  WebTransportCloseInit,
  WebTransportConstructor,
  WebTransportErrorConstructor,
  WebTransportHashInit,
  WebTransportInit,
  WebTransportLike,
  WebTransportStreamInit,
} from './webtransport-api.ts';
export {
  type WebTransportCapabilities,
  type WebTransportConnectorOptions,
  WebTransportProvider,
  type WebTransportProviderOptions,
  WebTransportReceiveStream,
  WebTransportSession,
  type WebTransportStats,
  WebTransportStream,
  webTransport,
  webTransportConnector,
} from './webtransport-provider.ts';
