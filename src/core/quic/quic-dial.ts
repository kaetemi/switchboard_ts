/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import type { MuxOptions } from '../mux/contract.ts';
import { muxOptions } from '../mux/contract.ts';
import { Status, StatusCode, SwitchboardError } from '../status.ts';
import { parseEndpoint } from '../transport/endpoint.ts';
import { withTimeout } from './binding-util.ts';
import type { QuicClientConfig, QuicConnector, QuicSession } from './contract.ts';
import { QuicError, switchboardAlpn } from './contract.ts';
import { QuicBindingConnection } from './quic-binding-connection.ts';
import { QuicIdentityRules } from './quic-identity-rules.ts';
import type { QuicBindingOptions } from './quic-options.ts';

/** How {@link connectQuic} dials. Durations in milliseconds. */
export interface QuicConnectOptions {
  /**
   * The connection's options. Its windows and channel limit also become the
   * session's {@link QuicClientConfig} where the provider can configure its
   * stack.
   */
  readonly options?: Partial<MuxOptions> | undefined;
  /** The binding's own options; their `identity` is replaced by the rules of the dial. */
  readonly quicOptions?: Partial<QuicBindingOptions> | undefined;
  /** The identification rules, instead of {@link QuicIdentityRules.initiator} computed for the dial. */
  readonly identityRules?: QuicIdentityRules | undefined;
  /** Whether the acceptor's certificate is verified against the platform roots. Default true. */
  readonly verifyCertificate?: boolean | undefined;
  /**
   * SHA-256 hashes of acceptable server certificates (DER): a pin, which
   * authenticates the acceptor. Each is the 32 bytes, or the form a
   * browser's `WebTransportOptions` takes, `{ algorithm: 'sha-256', value }`.
   */
  readonly serverCertificateHashes?: readonly QuicCertificateHash[] | undefined;
  /**
   * Bound on the connect, and the handshake deadline asked of the provider.
   * 0: none of this side's. Default 10 000.
   */
  readonly timeout?: number | undefined;
  /** The stack's idle timeout, where the provider can set it (a browser cannot). 0: none of this side's. Default 30 000. */
  readonly idleTimeout?: number | undefined;
  /** The stack's own keep-alive interval. Default a third of {@link idleTimeout}; 0: none. */
  readonly keepAlive?: number | undefined;
  /** This side's identity, set before anything from the peer is handled. */
  readonly localIdentity?: string | undefined;
  /**
   * Gives up the dial: it rejects with the signal's reason, and a session
   * the provider still yields is closed with `CANCELLED`.
   */
  readonly signal?: AbortSignal | undefined;
}

/**
 * A pinned certificate's SHA-256 hash: the 32 bytes, or the
 * `{ algorithm: 'sha-256', value }` form of the browser's WebTransport
 * options.
 */
export type QuicCertificateHash =
  | Uint8Array
  | { readonly algorithm: string; readonly value: ArrayBuffer | ArrayBufferView };

/**
 * Dials [endpoint] through [connector] and starts a
 * {@link QuicBindingConnection} over the session as its initiator: a
 * `quic://host:port` endpoint (raw QUIC, ALPN `switchboard`) or an
 * `https://host:port/path` endpoint (a WebTransport session offering the
 * protocol `switchboard`). A port of `connectQuicBinding` of
 * `quic_dial.dart`; what a browser gives a reconnecting client over the
 * web provider:
 *
 * ```ts
 * const connection = await connectQuic('https://node.example:4433/sb', webTransport(), {
 *   serverCertificateHashes: [hash], // a self-signed certificate
 * });
 * ```
 *
 * The acceptor's certificate counts as verified when `verifyCertificate`
 * is true (the default: the platform roots) or `serverCertificateHashes` pin it,
 * and the identification rules are then {@link QuicIdentityRules.initiator}
 * for a verified certificate (nothing more is required; a browser always
 * verifies, so its nonce-bound `IDENT`s are safe); with neither, they are
 * the specification's rules for an unverified acceptor where identity is in
 * use (`localIdentity`, or an identity verifier in `options`).
 * `identityRules` overrides them.
 *
 * On expiry of `timeout` the dial fails with `UNAVAILABLE`, and a session
 * the provider still yields is closed with `CANCELLED`, since a connect
 * cannot be cancelled.
 *
 * Rejects with a {@link SwitchboardError}: `UNIMPLEMENTED` when the
 * connector cannot dial the scheme, `INVALID_ARGUMENT` for a `quic`
 * endpoint without a host or port, `UNAUTHENTICATED` when the
 * identification rules ended the connection while it started, and
 * `UNAVAILABLE` for any other failure.
 *
 * @throws {TypeError} if [endpoint] is not an absolute URI, or for a
 *   certificate hash whose algorithm is not `sha-256`.
 * @throws {RangeError} for a negative `timeout`, `idleTimeout` or `keepAlive`.
 */
export function connectQuic(
  endpoint: string | URL,
  connector: QuicConnector,
  options: QuicConnectOptions = {},
): Promise<QuicBindingConnection> {
  const parsed = parseEndpoint(endpoint);
  const timeout = options.timeout ?? 10_000;
  const idleTimeout = options.idleTimeout ?? 30_000;
  const keepAlive = options.keepAlive ?? Math.floor(idleTimeout / 3);
  for (const [name, value] of [
    ['timeout', timeout],
    ['idleTimeout', idleTimeout],
    ['keepAlive', keepAlive],
  ] as const) {
    if (!(value >= 0)) {
      throw new RangeError(`${name} must not be negative: ${value}`);
    }
  }
  const hashes = (options.serverCertificateHashes ?? []).map(certificateHashBytes);
  const webTransport = parsed.scheme === 'https';
  const href = parsed.href;
  if (!webTransport && (parsed.host === '' || parsed.port === 0)) {
    return Promise.reject(
      SwitchboardError.of(
        StatusCode.invalidArgument,
        `a quic endpoint names a host and a port: ${href}`,
      ),
    );
  }
  return dial(parsed.key, href, webTransport, connector, options, hashes, {
    timeout,
    idleTimeout,
    keepAlive,
  });
}

async function dial(
  target: string,
  href: string,
  webTransport: boolean,
  connector: QuicConnector,
  init: QuicConnectOptions,
  hashes: readonly Uint8Array[],
  durations: { timeout: number; idleTimeout: number; keepAlive: number },
): Promise<QuicBindingConnection> {
  const signal = init.signal;
  signal?.throwIfAborted();
  const options = muxOptions(init.options);
  const verifyCertificate = init.verifyCertificate ?? true;
  const config: QuicClientConfig = {
    alpn: [switchboardAlpn],
    serverCertificateHashes: hashes,
    verifyCertificate,
    maxStreamsBidi: options.maxChannels > 0 ? options.maxChannels : 65536,
    streamReceiveWindow: options.initialWindow,
    connectionReceiveWindow:
      options.receiveHighWaterMarkBytes > 0 ? options.receiveHighWaterMarkBytes : 16 << 20,
    idleTimeout: durations.idleTimeout,
    keepAlive: durations.keepAlive,
    handshakeTimeout: durations.timeout,
  };
  let session: QuicSession;
  try {
    const connecting = connector.connect(target, config);
    session = await withAbort(
      signal,
      withTimeout(options.clock, connecting, durations.timeout, () => {
        abandon(connecting);
        throw SwitchboardError.of(
          StatusCode.unavailable,
          `cannot reach ${href}: not connected within ${durations.timeout} ms`,
        );
      }),
      () => abandon(connecting),
    );
  } catch (error) {
    if (error instanceof QuicError) {
      throw SwitchboardError.of(
        error.kind === 'unsupportedScheme' ? StatusCode.unimplemented : StatusCode.unavailable,
        `cannot reach ${href}: ${error.message}`,
        { cause: error },
      );
    }
    if (error instanceof SwitchboardError && error.is(StatusCode.unavailable)) {
      throw error;
    }
    if (signal?.aborted && error === signal.reason) {
      throw error;
    }
    throw SwitchboardError.of(StatusCode.unavailable, `cannot reach ${href}: ${String(error)}`, {
      cause: error,
    });
  }
  const rules =
    init.identityRules ??
    QuicIdentityRules.initiator({
      certificateVerified: verifyCertificate || hashes.length > 0,
      hasCredential: init.localIdentity !== undefined,
      verifiesCredentials: options.identityVerifier !== undefined,
    });
  const connection = await QuicBindingConnection.start(session, {
    options,
    quicOptions: { ...init.quicOptions, identity: rules },
    isWebTransport: webTransport,
    localIdentity: init.localIdentity,
  });
  if (signal?.aborted) {
    await connection.close();
    throw signal.reason;
  }
  if (!connection.isOpen) {
    // Ended while it started: the session, or the identification rules.
    const status = await connection.done;
    throw new SwitchboardError(
      status.known === StatusCode.unauthenticated
        ? status
        : Status.of(StatusCode.unavailable, `${href}: ${status}`),
    );
  }
  return connection;
}

/**
 * [promise], or a rejection with [signal]'s reason once it aborts first,
 * after which [onAbort] runs.
 */
function withAbort<T>(
  signal: AbortSignal | undefined,
  promise: Promise<T>,
  onAbort: () => void,
): Promise<T> {
  if (signal === undefined) {
    return promise;
  }
  return new Promise<T>((resolve, reject) => {
    const aborted = (): void => {
      reject(signal.reason);
      onAbort();
    };
    signal.addEventListener('abort', aborted, { once: true });
    promise.then(
      (value) => {
        signal.removeEventListener('abort', aborted);
        resolve(value);
      },
      (error: unknown) => {
        signal.removeEventListener('abort', aborted);
        reject(error);
      },
    );
  });
}

/** Closes the session a given-up connect still yields, and swallows its failure. */
function abandon(connecting: Promise<QuicSession>): void {
  connecting.then(
    (session) => session.close(StatusCode.cancelled, 'dial abandoned').catch(() => {}),
    () => {},
  );
}

function certificateHashBytes(hash: QuicCertificateHash): Uint8Array {
  if (hash instanceof Uint8Array) {
    return hash;
  }
  if (hash.algorithm.toLowerCase() !== 'sha-256') {
    throw new TypeError(`a certificate hash is SHA-256, got ${hash.algorithm}`);
  }
  const value = hash.value;
  return ArrayBuffer.isView(value)
    ? new Uint8Array(value.buffer, value.byteOffset, value.byteLength)
    : new Uint8Array(value);
}
