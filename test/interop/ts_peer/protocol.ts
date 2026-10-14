/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// The peer protocol's plumbing on the TS side, as dart_peer/lib/src/protocol.dart
// has it on the Dart side: JSON lines on stdout, errors as JSON, handles, and
// the argument readers every command uses (test/interop/README.md).

import {
  ByteFormatError,
  hexBytes,
  hexString,
  Status,
  StatusFormatError,
  SwitchboardError,
} from '../../../src/node/index.ts';

/** Version of the peer protocol, reported in `READY`. */
export const peerProtocolVersion = 1;

/** Bytes as upper-case hex separated by spaces, as the wiki writes them. */
export function hex(bytes: Uint8Array): string {
  return hexString(bytes);
}

/** A malformed command: reported with kind `argument`, as Dart's `ArgumentError`. */
export class ArgumentProblem extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ArgumentError';
  }
}

/** Bytes from hex, spaces optional; null, undefined and empty are no bytes. */
export function bytesOf(value: unknown): Uint8Array {
  if (value === undefined || value === null) {
    return new Uint8Array(0);
  }
  if (typeof value !== 'string') {
    throw new ArgumentProblem(`hex string expected, got ${typeof value}`);
  }
  return hexBytes(value);
}

/** A JSON object as the commands carry it. */
export type Json = Record<string, unknown>;

/**
 * One JSON line per call on stdout. Lines are written whole, so responses
 * and events never interleave.
 */
export class Output {
  /** Writes a raw line (only `READY` uses it). */
  line(text: string): void {
    process.stdout.write(`${text}\n`);
  }

  /** The successful response to command [id]. */
  respond(id: unknown, value: unknown): void {
    this.#write({ id, ok: true, value });
  }

  /** The failed response to command [id]. */
  fail(id: unknown, error: unknown): void {
    this.#write({ id, ok: false, error: errorJson(error) });
  }

  /** An unsolicited event. */
  event(name: string, fields: Json): void {
    this.#write({ event: name, ...fields });
  }

  #write(message: Json): void {
    process.stdout.write(`${JSON.stringify(message)}\n`);
  }

  /** Resolves once everything written so far reached stdout. */
  flush(): Promise<void> {
    return new Promise((resolve) => {
      process.stdout.write('', () => resolve());
    });
  }
}

/**
 * [error] as the protocol's error object: `kind` is `status` for a
 * Switchboard status (with its `code` and `reason`), `format` for a malformed
 * input, `argument` for a bad command, `other` otherwise.
 */
export function errorJson(error: unknown): Json {
  const type = error instanceof Error ? error.constructor.name : typeof error;
  const message = error instanceof Error ? error.message : String(error);
  if (error instanceof SwitchboardError) {
    return {
      kind: 'status',
      type,
      code: error.status.code,
      reason: error.status.reason,
      message: `${error}`,
    };
  }
  if (
    error instanceof ByteFormatError ||
    error instanceof StatusFormatError ||
    error instanceof SyntaxError
  ) {
    return { kind: 'format', type, message };
  }
  if (
    error instanceof ArgumentProblem ||
    error instanceof RangeError ||
    error instanceof TypeError
  ) {
    return { kind: 'argument', type, message };
  }
  return { kind: 'other', type, message };
}

/** A status as JSON: code and reason. */
export function statusJson(status: Status): Json {
  return { code: status.code, reason: status.reason };
}

/** The status a command names: `code` (default [code]) and `reason`. */
export function statusArg(cmd: Json, code = 0): Status {
  return new Status(intArg(cmd, 'code') ?? code, stringArg(cmd, 'reason') ?? '');
}

/** An optional integer argument. */
export function intArg(cmd: Json, key: string): number | undefined {
  const v = cmd[key];
  if (v === undefined || v === null) {
    return undefined;
  }
  if (typeof v === 'number' && Number.isInteger(v)) {
    return v;
  }
  throw new ArgumentProblem(`${key}: integer expected, got ${JSON.stringify(v)}`);
}

/** An optional string argument. */
export function stringArg(cmd: Json, key: string): string | undefined {
  const v = cmd[key];
  if (v === undefined || v === null) {
    return undefined;
  }
  if (typeof v === 'string') {
    return v;
  }
  throw new ArgumentProblem(`${key}: string expected, got ${JSON.stringify(v)}`);
}

/** A required string argument. */
export function requiredString(cmd: Json, key: string): string {
  const v = stringArg(cmd, key);
  if (v === undefined) {
    throw new ArgumentProblem(`${key} is required`);
  }
  return v;
}

/** An optional boolean argument. */
export function boolArg(cmd: Json, key: string, fallback = false): boolean {
  const v = cmd[key];
  if (v === undefined || v === null) {
    return fallback;
  }
  if (typeof v === 'boolean') {
    return v;
  }
  throw new ArgumentProblem(`${key}: boolean expected, got ${JSON.stringify(v)}`);
}

/** An optional object argument. */
export function objectArg(cmd: Json, key: string): Json | undefined {
  const v = cmd[key];
  if (v === undefined || v === null) {
    return undefined;
  }
  if (typeof v === 'object' && !Array.isArray(v)) {
    return v as Json;
  }
  throw new ArgumentProblem(`${key}: object expected, got ${JSON.stringify(v)}`);
}

/** An optional array argument. */
export function arrayArg(cmd: Json, key: string): unknown[] | undefined {
  const v = cmd[key];
  if (v === undefined || v === null) {
    return undefined;
  }
  if (Array.isArray(v)) {
    return v;
  }
  throw new ArgumentProblem(`${key}: array expected, got ${JSON.stringify(v)}`);
}

/** A decimal integer, strictly, as Dart's `int.parse` reads one. */
export function parseInteger(text: string): number {
  const t = text.trim();
  if (!/^[+-]?\d+$/.test(t)) {
    throw new SyntaxError(`invalid integer: ${JSON.stringify(text)}`);
  }
  return Number(t);
}

/**
 * The objects commands and services create, by string handle (`c1`, `ch4`,
 * `t2`, `s3`). Numbers are never reused within a process, so a handle in a
 * log names one object. The prefix says what the object is.
 */
export class Handles<Kinds extends Record<string, object>> {
  readonly #objects = new Map<string, { prefix: keyof Kinds & string; object: object }>();
  readonly #names = new Map<object, string>();
  readonly #counters = new Map<string, number>();

  /** Registers [object] under a new handle starting with [prefix]. */
  add<P extends keyof Kinds & string>(prefix: P, object: Kinds[P]): string {
    const existing = this.#names.get(object);
    if (existing !== undefined) {
      return existing;
    }
    const n = (this.#counters.get(prefix) ?? 0) + 1;
    this.#counters.set(prefix, n);
    const handle = `${prefix}${n}`;
    this.#objects.set(handle, { prefix, object });
    this.#names.set(object, handle);
    return handle;
  }

  /** The handle of [object], if it has one. */
  nameOf(object: object): string | undefined {
    return this.#names.get(object);
  }

  /** The object of [handle], which must have been added with [prefix]. */
  get<P extends keyof Kinds & string>(handle: unknown, prefix: P, what: string): Kinds[P] {
    const entry = typeof handle === 'string' ? this.#objects.get(handle) : undefined;
    if (entry === undefined || entry.prefix !== prefix) {
      throw new ArgumentProblem(`no ${what} with handle ${String(handle)}`);
    }
    return entry.object as Kinds[P];
  }

  /** Every registered object with its prefix, newest first. */
  get all(): { prefix: keyof Kinds & string; object: object }[] {
    return [...this.#objects.values()].reverse();
  }

  /** Forgets every object; the counters keep counting. */
  clear(): void {
    this.#objects.clear();
    this.#names.clear();
  }

  /** The number of registered objects. */
  get length(): number {
    return this.#objects.size;
  }
}
