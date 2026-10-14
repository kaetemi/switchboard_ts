/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * Endpoint URIs: `tcp://host:port`, `ws://` and `wss://host:port/path`,
 * `mem://id`, `quic://host:port` and `https://host:port/path`
 * (WebTransport). {@link parseEndpoint} classifies them and gives the form a
 * connection pool tells endpoints apart by, as Dart's `Switchboard`
 * normalises them (with the QUIC binding's rules for `quic` and `https`).
 * Which schemes a runtime can dial is the connector's business: it refuses
 * the others.
 */

/** The endpoint schemes the library knows. */
export const EndpointScheme = Object.freeze({
  /** The stream binding over TCP. */
  tcp: 'tcp',
  /** The WebSocket binding. */
  ws: 'ws',
  /** The WebSocket binding over TLS. */
  wss: 'wss',
  /** An in-process listener (`MemoryEndpoints`). */
  mem: 'mem',
  /** The QUIC binding. */
  quic: 'quic',
  /** The QUIC binding over WebTransport. */
  https: 'https',
} as const);

/** One of the {@link EndpointScheme} values. */
export type EndpointScheme = (typeof EndpointScheme)[keyof typeof EndpointScheme];

const knownSchemes: ReadonlySet<string> = new Set(Object.values(EndpointScheme));

/** Whether [scheme] (lower case, without the colon) is one the library knows. */
export function isKnownEndpointScheme(scheme: string): scheme is EndpointScheme {
  return knownSchemes.has(scheme);
}

/** An endpoint URI, taken apart and normalised. */
export interface Endpoint {
  /** The scheme in lower case, without the colon. */
  readonly scheme: string;
  /**
   * The host in lower case, an IPv6 address without its brackets; for a
   * `mem` endpoint, its id. Empty when the URI has none.
   */
  readonly host: string;
  /**
   * The port, the default filled in for `ws` (80), `wss` (443) and `https`
   * (443); 0 when there is none (never for `mem`).
   */
  readonly port: number;
  /** The path, `/` when empty, for `ws`, `wss` and `https`; empty otherwise. */
  readonly path: string;
  /** The query without its `?`, for `ws` and `wss` only; `undefined` when there is none. */
  readonly query: string | undefined;
  /**
   * The normalised text form: scheme and host lower-cased, the port
   * explicit, the path and query only where they name the listener, no
   * fragment, a `mem` endpoint its id alone. Equal for every spelling of one
   * endpoint; what a pool and an endpoint policy key on.
   */
  readonly key: string;
  /** The URI as parsed. */
  readonly href: string;
}

const defaultPorts: Readonly<Record<string, number>> = { ws: 80, wss: 443, https: 443 };

/**
 * Parses and normalises [endpoint].
 *
 * @throws {TypeError} if [endpoint] is not an absolute URI.
 */
export function parseEndpoint(endpoint: string | URL): Endpoint {
  let url: URL;
  if (endpoint instanceof URL) {
    url = endpoint;
  } else {
    try {
      url = new URL(endpoint);
    } catch (cause) {
      throw new TypeError(`not an endpoint URI: ${JSON.stringify(endpoint)}`, { cause });
    }
  }
  const scheme = url.protocol.slice(0, -1).toLowerCase();
  // Special schemes (ws, wss, https) arrive lower-cased; others do not.
  const bracketed = url.hostname.toLowerCase();
  const host = bracketed.startsWith('[') ? bracketed.slice(1, -1) : bracketed;
  const href = url.href;
  if (scheme === EndpointScheme.mem) {
    return { scheme, host, port: 0, path: '', query: undefined, key: `mem://${bracketed}`, href };
  }
  const port = url.port === '' ? (defaultPorts[scheme] ?? 0) : Number(url.port);
  const authority = `${scheme}://${bracketed}`;
  if (scheme === EndpointScheme.ws || scheme === EndpointScheme.wss) {
    const path = url.pathname === '' ? '/' : url.pathname;
    const query = queryOf(url);
    const tail = query === undefined ? '' : `?${query}`;
    return { scheme, host, port, path, query, key: `${authority}:${port}${path}${tail}`, href };
  }
  if (scheme === EndpointScheme.https) {
    const path = url.pathname === '' ? '/' : url.pathname;
    return { scheme, host, port, path, query: undefined, key: `${authority}:${port}${path}`, href };
  }
  if (scheme === EndpointScheme.quic) {
    // The QUIC binding keeps the port only when the URI gives one.
    const key = url.port === '' ? authority : `${authority}:${port}`;
    return { scheme, host, port, path: '', query: undefined, key, href };
  }
  return { scheme, host, port, path: '', query: undefined, key: `${authority}:${port}`, href };
}

/** The query of [url] without its `?`; empty for a bare `?`, `undefined` for none. */
function queryOf(url: URL): string | undefined {
  if (url.search !== '') {
    return url.search.slice(1);
  }
  const hash = url.href.indexOf('#');
  const beforeHash = hash < 0 ? url.href : url.href.slice(0, hash);
  return beforeHash.endsWith('?') ? '' : undefined;
}
