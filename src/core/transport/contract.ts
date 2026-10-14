/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * The transport frame contract (wiki page "Polyverse Switchboard
 * Transport"): one mux frame per chunk, in order, in both directions,
 * between exactly two peers. The frame mux (`src/core/mux`) runs over any
 * object that satisfies it: the in-memory pair, the WebSocket transports,
 * the stream transport (TCP), or a transport an application brings.
 *
 * Both directions are Web Streams. A chunk of `readable` is one complete
 * frame; a chunk written to `writable` is one frame, which the transport
 * frames or sends as one message. Backpressure is the streams': a `write`
 * resolves once the transport took the frame, and the mux reads frames only
 * as fast as it consumes them.
 *
 * The optional members are the capabilities of the Dart reference
 * (`FrameLimited`, `AbortableTransport`, `OutputBufferedTransport`,
 * `OutputReadyTransport`); a transport implements those it can.
 */
export interface FrameTransport {
  /** Incoming frames, one per chunk. Ends when the peer closed; errors on a transport failure. */
  readonly readable: ReadableStream<Uint8Array>;

  /** Outgoing frames, one per chunk. `close()` closes this side; `abort()` destroys the transport. */
  readonly writable: WritableStream<Uint8Array>;

  /** The largest incoming frame accepted, 0 or absent for none (`FrameLimited`). */
  readonly maxFrameSize?: number;

  /** A description of the peer for logs (`ip:port`, a URL, `memory`). */
  readonly remote?: string;

  /** Destroys the transport now, without draining (`AbortableTransport`). */
  abort?(): void;

  /** Bytes written and not yet accepted by the connection (`OutputBufferedTransport`). */
  readonly bufferedOutputBytes?: number;

  /** Total bytes the connection accepted; grows while the peer reads. */
  readonly acceptedOutputBytes?: number;

  /** Whether input is paused because too much output waits. */
  readonly isInputThrottled?: boolean;

  /** Whether a frame written now goes out without waiting behind earlier output (`OutputReadyTransport`). */
  readonly isOutputReady?: boolean;

  /** Resolves when {@link isOutputReady} becomes true (at once if it is); never rejects. */
  readonly outputReady?: Promise<void>;
}

/** A function that produces a connected transport, for clients that reconnect. */
export type TransportConnector = (options: { signal?: AbortSignal }) => Promise<FrameTransport>;

/** Default upper limit on an incoming frame: 1 MiB. */
export const defaultMaxFrameSize = 1024 * 1024;

/** The WebSocket subprotocol of the WebSocket binding. */
export const webSocketSubprotocol = 'switchboard';
