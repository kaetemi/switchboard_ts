/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * `@polyverse/switchboard`: the runtime-neutral core. Status codes, names,
 * byte helpers, addressing, the transport and channel contracts, the frame
 * mux, Talk, credentials and connection identity, the naming protocol,
 * client and service, resolvers, the reconnecting client, and the QUIC and
 * WebTransport binding over the provider contract. Nothing here imports a
 * Node, Bun, Deno or browser API that the others lack; the entry points
 * `@polyverse/switchboard/node` and `@polyverse/switchboard/web` add the
 * runtime specific parts.
 *
 * Each directory's `index.ts` is owned by the thread porting that layer
 * and re-exports its public surface; this file only gathers them.
 */

export * from './address/index.ts';
export * from './bytes.ts';
export * from './client/index.ts';
export * from './identity/index.ts';
export * from './mux/index.ts';
export * from './name.ts';
export * from './naming/index.ts';
export * from './quic/index.ts';
export * from './status.ts';
export * from './switchboard/index.ts';
export * from './talk/index.ts';
export * from './transport/index.ts';
export * from './util/index.ts';
