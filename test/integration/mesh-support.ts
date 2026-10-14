/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// What the integration tests of meshes share (the ports of the Dart
// `test/integration` files): listening over the scheme of a run, polling
// with a deadline, a test authority with its verifiers, and the Talk
// helpers of the Dart tests.

import { expect } from 'vitest';
import { type Credential, CredentialKind } from '../../src/core/identity/contract.ts';
import { CredentialIssuer } from '../../src/core/identity/credential-issuer.ts';
import { CredentialVerifier } from '../../src/core/identity/credential-verifier.ts';
import type { Ed25519HolderKey } from '../../src/core/identity/holder-key.ts';
import type { Scope } from '../../src/core/identity/scope.ts';
import { type Status, StatusCode, statusCodeName } from '../../src/core/status.ts';
import type { ChannelPolicy } from '../../src/core/switchboard/contract.ts';
import type { TalkChannel } from '../../src/core/talk/contract.ts';
import { MemoryEndpoints } from '../../src/core/transport/memory-endpoints.ts';
import type { Switchboard } from '../../src/node/switchboard.ts';

/** The transports a mesh runs over: TCP and WebSocket on the loopback, or in-process `mem://` endpoints. */
export type Scheme = 'tcp' | 'ws' | 'mem';

/**
 * Listens on 127.0.0.1 with the stream binding (`tcp`) or WebSocket (`ws`,
 * at `/mesh`), or in-process (`mem`), behind [options.policy]. With
 * [options.previous], where that listener was (its port, or its mem id),
 * retrying the bind for a while (a port just released); otherwise on a
 * free port or a new id.
 */
export async function listen(
  node: Switchboard,
  scheme: Scheme,
  options: { previous?: string | undefined; policy?: ChannelPolicy | undefined } = {},
): Promise<string> {
  const previous = options.previous === undefined ? undefined : new URL(options.previous);
  const policy = options.policy;
  for (let attempt = 0; ; attempt++) {
    try {
      switch (scheme) {
        case 'tcp':
          return await node.listenTcp('127.0.0.1', Number(previous?.port ?? 0), { policy });
        case 'ws':
          return await node.listenWebSocket('127.0.0.1', Number(previous?.port ?? 0), {
            path: '/mesh',
            policy,
          });
        case 'mem':
          return await node.listenMemory({ name: previous?.host, policy });
      }
    } catch (error) {
      const code = (error as { code?: unknown }).code;
      if (code !== 'EADDRINUSE' || attempt >= 100) {
        throw error;
      }
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
  }
}

/** Upper bound for anything a test waits on; only reached on failure. */
export const limit = 10_000;

/** Polls [condition] every 5 ms; fails after [ms]. */
export async function until(
  condition: () => boolean,
  what = 'condition',
  ms = limit,
): Promise<void> {
  const deadline = performance.now() + ms;
  while (!condition()) {
    if (performance.now() > deadline) {
      throw new Error(`${what} not met within ${ms} ms`);
    }
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

/** Waits [ms]. */
export function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** 32 bytes `f(0) .. f(31)` (Dart's `List.generate(32, f)`), each taken modulo 256. */
export function seed(f: (i: number) => number): Uint8Array {
  return Uint8Array.from({ length: 32 }, (_, i) => f(i) & 0xff);
}

/** A mesh authority (an Ed25519 issuer from a seed) and verifiers that trust it. */
export class Authority {
  readonly issuer: CredentialIssuer;

  private constructor(issuer: CredentialIssuer) {
    this.issuer = issuer;
  }

  /** The authority `auth1` of the Ed25519 key of [bytes]. */
  static async fromSeed(bytes: Uint8Array): Promise<Authority> {
    return new Authority(await CredentialIssuer.ed25519FromSeed('auth1', bytes));
  }

  /** A node credential for [identity], held by [holder], with [scopes]. */
  issue(identity: string, holder: Ed25519HolderKey, scopes: readonly Scope[]): Promise<Credential> {
    return this.issuer.issue({
      kind: CredentialKind.node,
      identity,
      scopes,
      holderKey: holder.publicKey,
    });
  }

  /** A verifier that trusts this authority. */
  async verifier(): Promise<CredentialVerifier> {
    const v = new CredentialVerifier();
    await v.addIssuer(this.issuer);
    return v;
  }
}

/** The well-known name of [status]'s code. */
export function codeOf(status: Status): string | undefined {
  return statusCodeName(status.code);
}

/** Asserts that [promise] rejects with a `SwitchboardError` of one of [codes]; returns the error. */
export async function rejectsWithOneOf(
  promise: Promise<unknown>,
  codes: readonly StatusCode[],
): Promise<unknown> {
  let error: unknown;
  try {
    await promise;
  } catch (e) {
    error = e;
  }
  expect(error).toBeDefined();
  const code = (error as { status?: Status }).status?.code;
  expect(codes.map((c) => statusCodeName(c))).toContain(
    code === undefined ? String(error) : statusCodeName(code),
  );
  return error;
}

/** One request [procedure] with [payload] on [talk]; its answer as text. */
export async function ask(talk: TalkChannel, procedure: string, payload = ''): Promise<string> {
  return (await talk.request(procedure, payload)).text();
}

/** Asserts that every `mem://` listener of the process is closed (Dart's `MemoryEndpoints.ids` check). */
export function expectNoMemoryListeners(): void {
  expect(MemoryEndpoints.ids, 'a memory listener leaked').toEqual([]);
}

export { StatusCode };
