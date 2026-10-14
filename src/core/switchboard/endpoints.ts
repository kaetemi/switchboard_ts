/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { StatusCode, SwitchboardError } from '../status.ts';

/**
 * Endpoint URIs as the node tells them apart: the pool key, what the
 * endpoint hooks are asked about, and the matching of the node's own
 * listeners. Internal.
 */

/** An endpoint URI taken apart. */
export interface NodeEndpoint {
  /** As given. */
  readonly uri: string;
  /** Lower case, without the colon. */
  readonly scheme: string;
  /** Lower case, IPv6 without brackets; for `mem`, the id. */
  readonly host: string;
  /** The explicit port, or undefined. */
  readonly port: number | undefined;
  /** The path as given (may be empty). */
  readonly path: string;
  /** The query without `?`, or undefined. */
  readonly query: string | undefined;
}

/** The scheme of in-process endpoints. */
export const memoryScheme = 'mem';

/** The schemes the frame mux carries. */
export const frameSchemes: ReadonlySet<string> = new Set(['ws', 'wss', 'tcp', memoryScheme]);

/**
 * Takes [uri] apart. Throws `SwitchboardError` INVALID_ARGUMENT for a URI
 * without a scheme.
 */
export function parseNodeEndpoint(uri: string): NodeEndpoint {
  let url: URL;
  try {
    url = new URL(uri);
  } catch {
    throw SwitchboardError.of(StatusCode.invalidArgument, `not an endpoint URI: ${uri}`);
  }
  const scheme = url.protocol.slice(0, -1).toLowerCase();
  // The WHATWG parser drops a scheme's default port; recover an explicit
  // one from the authority so that `ws://h:80` keeps its port as given.
  let port = url.port === '' ? undefined : Number(url.port);
  if (port === undefined) {
    const explicit = /^[a-z][a-z0-9+.-]*:\/\/(?:[^@/?#]*@)?(\[[^\]]*\]|[^:/?#]*):(\d+)/i.exec(uri);
    if (explicit?.[2] !== undefined) {
      port = Number(explicit[2]);
    }
  }
  return {
    uri,
    scheme,
    host: canonicalHost(decodeHost(url.hostname)),
    port,
    path: url.pathname,
    query: url.search === '' ? undefined : url.search.slice(1),
  };
}

function decodeHost(host: string): string {
  try {
    return decodeURIComponent(host);
  } catch {
    return host;
  }
}

/** [host] lower-cased, IPv6 literals without brackets and in their canonical form. */
export function canonicalHost(host: string): string {
  const lower = host.toLowerCase();
  const bare = lower.startsWith('[') && lower.endsWith(']') ? lower.slice(1, -1) : lower;
  if (bare.includes(':')) {
    try {
      return new URL(`http://[${bare}]/`).hostname.slice(1, -1);
    } catch {
      return bare;
    }
  }
  return bare;
}

/** The port, with the defaults of `ws` (80), `wss` and `https` (443) filled in; 0 for none, and for `mem`. */
export function portOf(endpoint: NodeEndpoint): number {
  if (endpoint.scheme === memoryScheme) {
    return 0;
  }
  if (endpoint.port !== undefined) {
    return endpoint.port;
  }
  switch (endpoint.scheme) {
    case 'ws':
      return 80;
    case 'wss':
    case 'https':
      return 443;
    default:
      return 0;
  }
}

/** [path] with a leading slash and without trailing slashes (`/` stays). */
export function normalizePath(path: string): string {
  let p = path.startsWith('/') ? path : `/${path}`;
  while (p.length > 1 && p.endsWith('/')) {
    p = p.slice(0, -1);
  }
  return p;
}

function hostPart(host: string): string {
  return host.includes(':') ? `[${host}]` : host;
}

/**
 * [endpoint] as the pool tells endpoints apart, for the frame-mux schemes
 * and the schemes without a binding of their own: scheme and host
 * lower-cased, the default port filled in, the path (`/` when empty) and
 * query kept for WebSocket URIs only, no fragment; a `mem` URI is its id
 * alone.
 */
export function normaliseEndpoint(endpoint: NodeEndpoint): string {
  const { scheme, host } = endpoint;
  if (scheme === memoryScheme) {
    return `${scheme}://${host}`;
  }
  const port = portOf(endpoint);
  const authority = `${scheme}://${hostPart(host)}${port === 0 ? '' : `:${port}`}`;
  if (scheme !== 'ws' && scheme !== 'wss') {
    return authority;
  }
  const path = endpoint.path.length === 0 ? '/' : endpoint.path;
  return `${authority}${path}${endpoint.query === undefined ? '' : `?${endpoint.query}`}`;
}

/**
 * The default normalisation of a binding's endpoints: scheme and host
 * lower-cased and the port if given, without path, query or fragment.
 */
export function normaliseBindingEndpoint(endpoint: NodeEndpoint): string {
  const port = endpoint.port;
  return `${endpoint.scheme}://${hostPart(endpoint.host)}${port === undefined ? '' : `:${port}`}`;
}
