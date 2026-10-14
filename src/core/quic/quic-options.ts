/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { QuicIdentityRules } from './quic-identity-rules.ts';
import { quicDefaultRecordLimit } from './records.ts';

/**
 * What a connection of the QUIC and WebTransport binding needs beyond the
 * `MuxOptions` every binding shares: the record limit, the identification
 * rules, and the bounds of this binding's own. A port of
 * `QuicBindingOptions` of `quic_options.dart`. Durations in milliseconds.
 */
export interface QuicBindingOptions {
  /**
   * The largest record body this side accepts on a channel's stream,
   * announced in `HELLO`: at least {@link quicDefaultRecordLimit}, which is
   * the default, and at most `quicMaxRecordLength`. A record over it
   * ends the connection with `FRAME_TOO_LARGE`, without its body being
   * read. A proxy toward frame mux backends announces no more than the
   * largest subframe they take (32752 bytes with the default window).
   */
  readonly recordLimit: number;

  /** The identification rules; see {@link QuicIdentityRules}. Default none. */
  readonly identity: QuicIdentityRules;

  /**
   * How long after the connection started either side waits for the peer's
   * first `HELLO` before closing the connection with `PROTOCOL_ERROR`.
   * Undefined, the default: `MuxOptions.keepAliveInterval`, or 10 000
   * without keep-alive. 0 waits without bound.
   */
  readonly controlStreamTimeout: number | undefined;

  /**
   * How long opening a channel waits for stream credit from the peer (the
   * stack waits for `MAX_STREAMS`) before the channel fails with
   * `RESOURCE_EXHAUSTED`. 0 waits without bound. Default 10 000.
   */
  readonly openStreamTimeout: number;

  /**
   * Bytes of control messages waiting for the control stream above which
   * this side stops reading the peer's direction of the control stream,
   * until they drained: a peer that sends `PING`s without reading cannot
   * make `PONG`s pile up. Default 64 KiB.
   */
  readonly controlOutputMark: number;

  /**
   * Most control messages held while an `IDENT` is verified; beyond it the
   * connection ends with `RESOURCE_EXHAUSTED`. Default 1024.
   */
  readonly maxHeldControlMessages: number;

  /**
   * Streams whose first OPEN record is still being read. Further streams
   * are stopped and reset with `RESOURCE_EXHAUSTED`. Their reads are also
   * bounded by {@link openStreamTimeout}, as are outgoing stream opens.
   * Default 128.
   */
  readonly maxPendingStreams: number;

  /**
   * Records waiting on a stream are handed to the provider together, in one
   * write of up to this many bytes (a record larger than it goes alone).
   * Default 64 KiB.
   */
  readonly writeBatchBytes: number;
}

/** The defaults of {@link QuicBindingOptions}: the specification's. */
export const defaultQuicBindingOptions: QuicBindingOptions = Object.freeze({
  recordLimit: quicDefaultRecordLimit,
  identity: QuicIdentityRules.none,
  controlStreamTimeout: undefined,
  openStreamTimeout: 10_000,
  controlOutputMark: 64 * 1024,
  maxHeldControlMessages: 1024,
  maxPendingStreams: 128,
  writeBatchBytes: 64 * 1024,
});

/** [options] completed with the defaults. */
export function quicBindingOptions(options?: Partial<QuicBindingOptions>): QuicBindingOptions {
  return options === undefined
    ? defaultQuicBindingOptions
    : Object.freeze({ ...defaultQuicBindingOptions, ...options });
}
