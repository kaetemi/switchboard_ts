/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * The QUIC parts of `@polyverse/switchboard/node`: the native QUIC provider (raw QUIC and
 * WebTransport, client and listener, over the N-API addon in `native/`), its certificates, and
 * the check whether the addon is available here. The QUIC listener and binding for nodes join
 * them here.
 *
 * Owned by the "QUIC transport" thread.
 */

export {
  certificateHash,
  type NativeCertificate,
  type NativeListenOptions,
  type NativeQuicAvailability,
  type NativeQuicListener,
  NativeQuicProvider,
  type NativeQuicSession,
  type NativeQuicStats,
  type NativeWebTransportConnectOptions,
  type NativeWebTransportListener,
  type NativeWebTransportListenOptions,
  nativeQuic,
  nativeQuicAvailability,
  quinnPriority,
  selfSignedCertificate,
} from './native-provider.ts';
