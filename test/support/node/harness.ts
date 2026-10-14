/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { afterEach, expect } from 'vitest';
import { ChannelAddress, ServiceAddress } from '../../../src/core/address/index.ts';
import { type PayloadLike, utf8Decode, utf8Encode } from '../../../src/core/bytes.ts';
import type { MuxChannel, MuxConnection, MuxOptions } from '../../../src/core/mux/contract.ts';
import { Name } from '../../../src/core/name.ts';
import {
  type Status,
  StatusCode,
  SwitchboardError,
  statusCodeName,
} from '../../../src/core/status.ts';
import type { ChannelHandler } from '../../../src/core/switchboard/contract.ts';
import type { SwitchboardRuntime } from '../../../src/core/switchboard/runtime.ts';
import { Switchboard, type SwitchboardOptions } from '../../../src/core/switchboard/switchboard.ts';
import type { FrameTransport } from '../../../src/core/transport/contract.ts';
import { memoryTransportPair } from '../../../src/core/transport/memory-transport.ts';
import { MemoryEndpointTable } from './memory-table.ts';
import { toyMux } from './toy-mux.ts';

/**
 * The node thread's test harness: nodes over the toy mux and the test
 * transport pair, with their own `mem://` registry, closed after each test.
 */

/** Short grace so that closing a node with open channels is quick, and no keep-alive. */
export const fast: Partial<MuxOptions> = Object.freeze({
  goAwayGrace: 100,
  keepAliveInterval: undefined,
});

/** The service type most tests use. */
export const svc = Name.of('svc');

/** A runtime over the toy mux with [memoryEndpoints] (default: a new table). */
export function testRuntime(
  memoryEndpoints: MemoryEndpointTable = new MemoryEndpointTable(),
): SwitchboardRuntime {
  return {
    mux: toyMux,
    transportPair: () => memoryTransportPair({ delay: 0 }),
    memoryEndpoints,
    dialers: {},
  };
}

/** The `mem://` registry the nodes of the current test share. */
let registry = new MemoryEndpointTable();
const cleanups: (() => Promise<unknown> | unknown)[] = [];

afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) {
    await cleanup();
  }
  // Every node of the test is closed: each released its mem:// ids.
  const leaked = registry.ids;
  registry = new MemoryEndpointTable();
  if (leaked.length > 0) {
    throw new Error(`memory listeners leaked: ${leaked.join(', ')}`);
  }
});

/** Runs [cleanup] after the current test. */
export function onCleanup(cleanup: () => Promise<unknown> | unknown): void {
  cleanups.push(cleanup);
}

/** A node of the current test (shared `mem://` registry, fast mux options), closed after it. */
export function node(options: SwitchboardOptions = {}): Switchboard {
  const s = new Switchboard({
    runtime: testRuntime(registry),
    ...options,
    muxOptions: { ...fast, ...options.muxOptions },
  });
  onCleanup(() => s.close());
  return s;
}

/** The `mem://` registry of the nodes of the current test. */
export function memoryRegistry(): MemoryEndpointTable {
  return registry;
}

/** The runtime of the nodes of the current test. */
export function currentRuntime(): SwitchboardRuntime {
  return testRuntime(registry);
}

/** A raw toy-mux peer on one side of a transport pair; the other side is returned for the node. */
export function rawPeer(options: Partial<MuxOptions> = fast): {
  peer: MuxConnection;
  transport: FrameTransport;
} {
  const [local, remote] = memoryTransportPair({ delay: 0 });
  const peer = toyMux(local, options, true);
  onCleanup(() => peer.close());
  return { peer, transport: remote };
}

/** UTF-8 bytes of [text]. */
export function bytes(text: string): Uint8Array {
  return utf8Encode(text);
}

/** A channel address of `svc`. */
export function svcAddress(
  init: { instance?: number; payload?: PayloadLike } = {},
): ChannelAddress {
  return new ChannelAddress({ type: svc, instance: init.instance, payload: init.payload });
}

/** A handler that sends [tag] and closes. */
export function tagged(tag: string): ChannelHandler {
  return (incoming) => {
    void incoming.channel.send(bytes(tag));
    void incoming.channel.close();
  };
}

/** The tag a {@link tagged} handler sent, or `closed <code name>`. */
export async function tagOf(channel: MuxChannel): Promise<string> {
  const frames: Uint8Array[] = [];
  for await (const frame of channel.readable) {
    frames.push(frame);
  }
  const status = await channel.done;
  if (frames.length === 0) {
    return `closed ${statusCodeName(status.code) ?? status.code}`;
  }
  return frames.map((frame) => utf8Decode(frame)).join('');
}

/** The well-known name of [status]'s code. */
export function codeOf(status: Status): string | undefined {
  return statusCodeName(status.code);
}

/** Asserts that [promise] rejects with a `SwitchboardError` of [code]; returns the error. */
export async function rejectsWith(
  promise: Promise<unknown>,
  code: StatusCode,
): Promise<SwitchboardError> {
  let error: unknown;
  try {
    await promise;
  } catch (e) {
    error = e;
  }
  expect(error).toBeInstanceOf(SwitchboardError);
  const e = error as SwitchboardError;
  expect(statusCodeName(e.code)).toBe(statusCodeName(code));
  return e;
}

/** The first value [iterable] yields. */
export async function first<T>(iterable: AsyncIterable<T>): Promise<T> {
  for await (const value of iterable) {
    return value;
  }
  throw new Error('ended without a value');
}

/** Waits for macrotasks until [condition] holds (at most [limit] rounds). */
export async function until(condition: () => boolean, limit = 1000): Promise<void> {
  for (let i = 0; !condition(); i++) {
    if (i >= limit) {
      throw new Error('condition never held');
    }
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
}

/** Lets pending macrotasks run (the test transport delivers frames through them). */
export async function pump(rounds = 20): Promise<void> {
  for (let i = 0; i < rounds; i++) {
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
}

/** A service address of `svc`. */
export function svcInstance(instance = 0): ServiceAddress {
  return new ServiceAddress(svc, instance);
}

export { StatusCode };
