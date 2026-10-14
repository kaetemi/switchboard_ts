/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

import { ServiceAddress } from '../address/service-address.ts';
import {
  ByteFormatError,
  ByteReader,
  ByteWriter,
  bytesEqual,
  maxU48,
  utf8Decode,
  utf8Encode,
} from '../bytes.ts';
import { Name, type NameLike, toName } from '../name.ts';
import { ProtocolError, Status, StatusCode } from '../status.ts';
import type { ServiceEvent, ServiceRecord } from '../switchboard/contract.ts';

/**
 * The payloads of the naming service (wiki pages "Polyverse Switchboard
 * Naming Service" and "Polyverse Switchboard Sharding"): the `_ns`
 * procedures, their requests and responses, the events of a `WATCH`
 * subscription, the slot messages and the fields of `MOVED` and
 * `RELOCATED`. Every decoder ignores trailing bytes after a complete
 * structure, for forward compatibility, and throws {@link ProtocolError}
 * for anything else it cannot read.
 */

/** Talk procedure names of the naming service. */
export const Procedures = Object.freeze({
  /** Register a service instance (request). */
  register: Name.of('REGISTER'),
  /** Remove a registration (request). */
  unregister: Name.of('UNREGSTR'),
  /** Subscribe to the service table (request): the snapshot as events, the reply, then live events. */
  watch: Name.of('WATCH'),
  /** End a subscription made with `WATCH` (request). */
  unwatch: Name.of('UNWATCH'),
  /** One-shot table query (stream request). */
  lookup: Name.of('LOOKUP'),
  /** Event and `LOOKUP` item: a service record came up. */
  up: Name.of('UP'),
  /** Event: a service instance went away. */
  down: Name.of('DOWN'),
  /** Define or confirm a slot space (request). */
  slots: Name.of('SLOTS'),
  /** Declare the slots whose storage this instance holds (request). */
  holding: Name.of('HOLDING'),
  /** Take a free slot (request). */
  claim: Name.of('CLAIM'),
  /** Give up an owned slot (request). */
  release: Name.of('RELEASE'),
  /** Find, and in a managed space assign, the owner of a slot (request). */
  locate: Name.of('LOCATE'),
  /** Move a slot to another instance (stream request). */
  migrate: Name.of('MIGRATE'),
  /** Stream item of `MIGRATE`: progress of the hand-over. */
  phase: Name.of('PHASE'),
  /** From the naming service to an instance: serve a slot (request). */
  assign: Name.of('ASSIGN'),
  /** From the naming service to the owner: lock a slot and make its state available (request). */
  drain: Name.of('DRAIN'),
  /** From the naming service to the old owner: the new owner serves, hand the queue over (request). */
  forward: Name.of('FORWARD'),
  /** From the naming service to the owner: the migration was abandoned, unlock and serve (request). */
  resume: Name.of('RESUME'),
  /** Event and `LOOKUP` item: a slot space definition. */
  slotSpace: Name.of('SLOTSPC'),
  /** Event and `LOOKUP` item: the state of one slot. */
  slot: Name.of('SLOT'),
  /** Ask for a renewed credential (request). */
  renew: Name.of('RENEW'),
  /** From a consumer: have an instance dial it. From the naming service to the instance: dial the consumer. */
  connect: Name.of('CONNECT'),
});

/** Well-known service types. */
export const Services = Object.freeze({
  /** The naming service. */
  naming: Name.of('_ns'),
  /**
   * The relay for instances that listen nowhere: the application payload
   * of a `_relay` channel is the complete open payload of the channel to
   * relay, and the metadata of a `_relay` record is the relay's identity.
   */
  relay: Name.of('_relay'),
  /** Bulk channels: the payload of one Talk message of another channel of the same connection. */
  bulk: Name.of('_bulk'),
});

/** Largest application metadata blob a service record (and a `REGISTER`) carries, in bytes. */
export const maxMetadataLength = 4096;

/** Largest slot number, slot count, capacity and epoch (`u32`). */
export const maxU32 = 0xffffffff;

/** Longest intent of a `CONNECT`: the intent of an `IDENT`. */
export const maxConnectIntentLength = 64;

const noBytes = new Uint8Array(0);
const strictUtf8 = new TextDecoder('utf-8', { fatal: true });

/** Runs a decoder, turning a truncation or a malformed field into a {@link ProtocolError}. */
function guard<T>(what: string, body: () => T): T {
  try {
    return body();
  } catch (e) {
    if (e instanceof ProtocolError) {
      throw e;
    }
    if (e instanceof ByteFormatError || e instanceof RangeError) {
      throw new ProtocolError(`bad ${what}: ${e.message}`, { cause: e });
    }
    throw e;
  }
}

function checkU32(value: number, name: string): void {
  if (!Number.isInteger(value) || value < 0 || value > maxU32) {
    throw new RangeError(`${name} ${value} is outside 0 to ${maxU32}`);
  }
}

function checkU48(value: number, name: string): void {
  if (!Number.isInteger(value) || value < 0 || value > maxU48) {
    throw new RangeError(`${name} ${value} is outside 0 to ${maxU48}`);
  }
}

function hex(value: number): string {
  return value.toString(16);
}

// ---------------------------------------------------------------------------
// Endpoints

const schemePattern = /^[A-Za-z][A-Za-z0-9+.-]*:/;
// One character of a path segment, query or fragment, or a percent escape.
const pchars = "A-Za-z0-9\\-._~!$&'()*+,;=:@";
const segmentPattern = new RegExp(`^(?:[${pchars}]|%[0-9A-Fa-f]{2})*$`);
const queryPattern = new RegExp(`^(?:[${pchars}/?]|%[0-9A-Fa-f]{2})*$`);
const userInfoPattern = /^(?:[A-Za-z0-9\-._~!$&'()*+,;=:]|%[0-9A-Fa-f]{2})*$/;
const regNamePattern = /^(?:[A-Za-z0-9\-._~!$&'()*+,;=]|%[0-9A-Fa-f]{2})*$/;
const ipvFuturePattern = /^v[0-9A-Fa-f]+\.[A-Za-z0-9\-._~!$&'()*+,;=:]+$/;
const zonePattern = /^(?:[A-Za-z0-9\-._~]|%[0-9A-Fa-f]{2})*$/;
const portPattern = /^(?:-?[0-9]+)?$/;
const h16 = '[0-9A-Fa-f]{1,4}';
const ipv4Part = '(?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])';
const ipv4 = `${ipv4Part}(?:\\.${ipv4Part}){3}`;
const ls32 = `(?:${h16}:${h16}|${ipv4})`;
const ipv6Pattern = new RegExp(
  `^(?:(?:${h16}:){6}${ls32}` +
    `|::(?:${h16}:){5}${ls32}` +
    `|(?:${h16})?::(?:${h16}:){4}${ls32}` +
    `|(?:(?:${h16}:){0,1}${h16})?::(?:${h16}:){3}${ls32}` +
    `|(?:(?:${h16}:){0,2}${h16})?::(?:${h16}:){2}${ls32}` +
    `|(?:(?:${h16}:){0,3}${h16})?::${h16}:${ls32}` +
    `|(?:(?:${h16}:){0,4}${h16})?::${ls32}` +
    `|(?:(?:${h16}:){0,5}${h16})?::${h16}` +
    `|(?:(?:${h16}:){0,6}${h16})?::)$`,
);

/** Whether a path segment is `.` or `..`, also when written with percent escapes. */
function isDotSegment(segment: string): boolean {
  const plain = segment.replace(/%2[eE]/g, '.');
  return plain === '.' || plain === '..';
}

/**
 * Checks the characters of [path]. Without an authority (`tcp:/a/b`),
 * `.` and `..` segments are refused too: the reference removes them, and
 * the result can start with `//`, which reads back as an authority
 * (`tcp:/../..//` is `tcp://`). After an authority they are harmless.
 */
function pathProblem(path: string, hasAuthority: boolean): string | undefined {
  for (const segment of path.split('/')) {
    if (!segmentPattern.test(segment)) {
      return 'a character that is not allowed in a path';
    }
    if (!hasAuthority && isDotSegment(segment)) {
      return 'a dot segment in a path without an authority';
    }
  }
  return undefined;
}

function authorityProblem(authority: string): string | undefined {
  const at = authority.lastIndexOf('@');
  if (at >= 0 && !userInfoPattern.test(authority.substring(0, at))) {
    return 'a character that is not allowed in the user information';
  }
  const hostPort = at >= 0 ? authority.substring(at + 1) : authority;
  let host: string;
  let port: string | undefined;
  if (hostPort.startsWith('[')) {
    const close = hostPort.indexOf(']');
    if (close < 0) {
      return 'an IP literal without its closing bracket';
    }
    if (!ipLiteralIsValid(hostPort.substring(1, close))) {
      return 'an invalid IP literal';
    }
    const after = hostPort.substring(close + 1);
    if (after.length > 0 && !after.startsWith(':')) {
      return 'characters after the IP literal';
    }
    host = '';
    port = after.length > 0 ? after.substring(1) : undefined;
  } else {
    const colon = hostPort.indexOf(':');
    host = colon < 0 ? hostPort : hostPort.substring(0, colon);
    port = colon < 0 ? undefined : hostPort.substring(colon + 1);
  }
  if (!regNamePattern.test(host)) {
    return 'a character that is not allowed in the host';
  }
  if (port !== undefined && !portIsValid(port)) {
    return 'a port that is not a number';
  }
  return undefined;
}

/** An IPv6 address, with an optional zone (RFC 6874, `%25` then the zone), or an IPvFuture literal. */
function ipLiteralIsValid(literal: string): boolean {
  if (ipvFuturePattern.test(literal)) {
    return true;
  }
  const zone = literal.indexOf('%25');
  if (zone >= 0) {
    return (
      ipv6Pattern.test(literal.substring(0, zone)) && zonePattern.test(literal.substring(zone + 3))
    );
  }
  return ipv6Pattern.test(literal);
}

/**
 * Empty, or a decimal number, optionally negative, within the 64-bit range
 * the Dart reference reads ports in. The reference sends what it read
 * (`tcp://h:-1` included), so what it sends is accepted here.
 */
function portIsValid(port: string): boolean {
  if (!portPattern.test(port)) {
    return false;
  }
  const negative = port.startsWith('-');
  const digits = (negative ? port.substring(1) : port).replace(/^0+/, '');
  const limit = negative ? '9223372036854775808' : '9223372036854775807';
  return digits.length < limit.length || (digits.length === limit.length && digits <= limit);
}

/**
 * Why [endpoint] cannot be carried in a service record, or undefined if it
 * can. See {@link checkEndpoint}.
 */
export function endpointProblem(endpoint: string): string | undefined {
  if (!schemePattern.test(endpoint)) {
    return 'no scheme';
  }
  if (utf8Encode(endpoint).length > 0xff) {
    return 'longer than 255 bytes';
  }
  if (/^data:/i.test(endpoint)) {
    // The reference parses a data URI as a MIME type and its data.
    return 'a data URI';
  }
  let rest = endpoint.substring(endpoint.indexOf(':') + 1);
  const hash = rest.indexOf('#');
  if (hash >= 0) {
    if (!queryPattern.test(rest.substring(hash + 1))) {
      return 'a character that is not allowed in the fragment';
    }
    rest = rest.substring(0, hash);
  }
  const question = rest.indexOf('?');
  if (question >= 0) {
    if (!queryPattern.test(rest.substring(question + 1))) {
      return 'a character that is not allowed in the query';
    }
    rest = rest.substring(0, question);
  }
  if (rest.startsWith('//')) {
    const slash = rest.indexOf('/', 2);
    const authority = slash < 0 ? rest.substring(2) : rest.substring(2, slash);
    const problem = authorityProblem(authority);
    if (problem !== undefined) {
      return problem;
    }
    return slash < 0 ? undefined : pathProblem(rest.substring(slash + 1), true);
  }
  if (/^file:/i.test(endpoint)) {
    // The reference rewrites a file URI without an authority (`file:/x` is
    // `file:///x`), and not always stably.
    return 'a file URI without an authority';
  }
  return pathProblem(rest.startsWith('/') ? rest.substring(1) : rest, false);
}

/**
 * Throws unless [endpoint] can be carried in a service record: an absolute
 * URI (with a scheme) of at most 255 bytes, written in the characters a URI
 * allows (RFC 3986: no spaces, no non-ASCII, percent escapes complete), an
 * authority (when there is one) of user information, a host name or IP
 * literal and a numeric port, and a path without `.` or `..` segments.
 *
 * Endpoints are carried as written. Every endpoint this accepts is one the
 * Dart reference accepts and decodes to a URI whose text form is stable,
 * and every endpoint the Dart reference sends (its normalised text form)
 * is accepted, so records pass between the two in both directions. The
 * decoders refuse (with {@link ProtocolError}) every endpoint this
 * refuses, and the naming service refuses them with INVALID_ARGUMENT.
 *
 * @throws {TypeError} naming the problem.
 */
export function checkEndpoint(endpoint: string): void {
  const problem = endpointProblem(endpoint);
  if (problem !== undefined) {
    throw new TypeError(`endpoint "${endpoint}": ${problem}`);
  }
}

function readEndpoint(r: ByteReader): string {
  const length = r.u8('endpoint length');
  let text: string;
  try {
    text = strictUtf8.decode(r.take(length, 'endpoint'));
  } catch {
    throw new ProtocolError('invalid endpoint uri: not UTF-8');
  }
  const problem = endpointProblem(text);
  if (problem !== undefined) {
    throw new ProtocolError(`invalid endpoint uri: ${problem}`);
  }
  return text;
}

function writeEndpoints(w: ByteWriter, endpoints: readonly string[]): void {
  if (endpoints.length > 0xff) {
    throw new RangeError(`${endpoints.length} endpoints, more than 255`);
  }
  w.u8(endpoints.length);
  for (const endpoint of endpoints) {
    w.string8(endpoint);
  }
}

function readEndpoints(r: ByteReader): string[] {
  const count = r.u8('endpoint count');
  const endpoints: string[] = [];
  for (let i = 0; i < count; i++) {
    endpoints.push(readEndpoint(r));
  }
  return endpoints;
}

function writeMetadata(w: ByteWriter, metadata: Uint8Array): void {
  if (metadata.length > maxMetadataLength) {
    throw new RangeError(`metadata of ${metadata.length} bytes, more than ${maxMetadataLength}`);
  }
  w.u16(metadata.length);
  w.bytes(metadata);
}

/**
 * Reads the metadata field after the endpoints. A payload that ends right
 * after its endpoints (the layout before the field was added) has empty
 * metadata; a field cut short, or longer than {@link maxMetadataLength},
 * is a protocol error. The result is a copy.
 */
function readMetadata(r: ByteReader): Uint8Array {
  if (r.isDone) {
    return noBytes;
  }
  const length = r.u16('metadata length');
  if (length > maxMetadataLength) {
    throw new ProtocolError(`metadata of ${length} bytes, more than ${maxMetadataLength}`);
  }
  return r.take(length, 'metadata').slice();
}

// ---------------------------------------------------------------------------
// Service records and events

/** What {@link serviceRecord} takes beside the address. */
export interface ServiceRecordInit {
  /** Endpoint URIs in order of preference; default none. */
  readonly endpoints?: readonly string[] | undefined;
  /** Opaque application data published with the record, at most 4096 bytes; default empty. */
  readonly metadata?: Uint8Array | undefined;
}

/**
 * A service record: [address] with a non-zero instance, its endpoints and
 * its metadata. The arrays are frozen copies; the metadata is not copied
 * and must not be modified afterwards.
 */
export function serviceRecord(
  address: ServiceAddress,
  init: ServiceRecordInit = {},
): ServiceRecord {
  return Object.freeze({
    address,
    endpoints: Object.freeze([...(init.endpoints ?? [])]),
    metadata: init.metadata ?? noBytes,
  });
}

/**
 * Encodes a service record: name, u48 instance, u8 count, count times len8
 * URI, u16 metadata length, metadata.
 *
 * @throws {RangeError} if the instance is 0, there are more than 255
 * endpoints, an endpoint is longer than 255 bytes, or the metadata is
 * longer than {@link maxMetadataLength}.
 */
export function encodeServiceRecord(record: ServiceRecord): Uint8Array {
  if (record.address.isAny) {
    throw new RangeError(`record ${record.address}: instance must be non-zero`);
  }
  const w = new ByteWriter(32 + record.metadata.length);
  w.name(record.address.type);
  w.u48(record.address.instance);
  writeEndpoints(w, record.endpoints);
  writeMetadata(w, record.metadata);
  return w.toBytes();
}

/**
 * Decodes a service record; trailing bytes are ignored. A record without
 * the metadata field (nothing after the endpoints) has empty metadata. The
 * metadata is a copy.
 *
 * @throws {ProtocolError} on truncation, instance 0, an endpoint that
 * {@link checkEndpoint} refuses, or metadata longer than 4096 bytes.
 */
export function decodeServiceRecord(bytes: Uint8Array): ServiceRecord {
  return guard('service record', () => {
    const r = new ByteReader(bytes);
    const type = r.name('type');
    const instance = r.u48('instance');
    if (instance === 0) {
      throw new ProtocolError('bad service record: instance is 0');
    }
    const endpoints = readEndpoints(r);
    return serviceRecord(new ServiceAddress(type, instance), {
      endpoints,
      metadata: readMetadata(r),
    });
  });
}

/** Equal address, the same endpoints in the same order, and equal metadata bytes. */
export function serviceRecordsEqual(a: ServiceRecord, b: ServiceRecord): boolean {
  if (!a.address.equals(b.address) || a.endpoints.length !== b.endpoints.length) {
    return false;
  }
  for (let i = 0; i < a.endpoints.length; i++) {
    if (a.endpoints[i] !== b.endpoints[i]) {
      return false;
    }
  }
  return bytesEqual(a.metadata, b.metadata);
}

/** The record as text for logs. */
export function serviceRecordToString(record: ServiceRecord): string {
  const endpoints = `[${record.endpoints.join(', ')}]`;
  return record.metadata.length === 0
    ? `ServiceRecord(${record.address}, ${endpoints})`
    : `ServiceRecord(${record.address}, ${endpoints}, ${record.metadata.length} metadata bytes)`;
}

/** An `UP` event for [record]. */
export function upEvent(record: ServiceRecord): ServiceEvent {
  return Object.freeze({ up: true, record });
}

/** A `DOWN` event for [address]: a record without endpoints or metadata. */
export function downEvent(address: ServiceAddress): ServiceEvent {
  return Object.freeze({ up: false, record: serviceRecord(address) });
}

/** Decodes the payload of an `UP` event (a full record). @throws {ProtocolError} as {@link decodeServiceRecord}. */
export function decodeUpEvent(bytes: Uint8Array): ServiceEvent {
  return upEvent(decodeServiceRecord(bytes));
}

/** Decodes the payload of a `DOWN` event (name and instance). @throws {ProtocolError} on truncation. */
export function decodeDownEvent(bytes: Uint8Array): ServiceEvent {
  const u = UnregisterRequest.decode(bytes);
  return downEvent(new ServiceAddress(u.type, u.instance));
}

/** The procedure of [event]: `UP` or `DOWN`. */
export function serviceEventProcedure(event: ServiceEvent): Name {
  return event.up ? Procedures.up : Procedures.down;
}

/** Encodes the payload of [event]: the full record for `UP`, name and instance for `DOWN`. */
export function encodeServiceEvent(event: ServiceEvent): Uint8Array {
  return event.up
    ? encodeServiceRecord(event.record)
    : new UnregisterRequest(event.record.address.type, event.record.address.instance).encode();
}

/** Equal kind and equal records ({@link serviceRecordsEqual}). */
export function serviceEventsEqual(a: ServiceEvent, b: ServiceEvent): boolean {
  return a.up === b.up && serviceRecordsEqual(a.record, b.record);
}

// ---------------------------------------------------------------------------
// Registration and subscription

/** `REGISTER` request payload: the layout of a service record with the requested instance in place of the instance. */
export class RegisterRequest {
  /** Service type to register. */
  readonly type: Name;
  /** Requested instance id, 0 to have one assigned. */
  readonly requestedInstance: number;
  /** Endpoints the instance listens on. */
  readonly endpoints: readonly string[];
  /** Application metadata for the record. */
  readonly metadata: Uint8Array;

  /** A request; `requestedInstance` 0 (the default) asks the naming service to assign one. */
  constructor(
    type: NameLike,
    init: { requestedInstance?: number; endpoints?: readonly string[]; metadata?: Uint8Array } = {},
  ) {
    this.type = toName(type);
    this.requestedInstance = init.requestedInstance ?? 0;
    this.endpoints = Object.freeze([...(init.endpoints ?? [])]);
    this.metadata = init.metadata ?? noBytes;
  }

  /** Encodes the payload. @throws {RangeError} for over 255 endpoints, an instance outside u48, or metadata over 4096 bytes. */
  encode(): Uint8Array {
    const w = new ByteWriter(32 + this.metadata.length);
    w.name(this.type);
    w.u48(this.requestedInstance);
    writeEndpoints(w, this.endpoints);
    writeMetadata(w, this.metadata);
    return w.toBytes();
  }

  /**
   * Decodes the payload; trailing bytes are ignored. A request without the
   * metadata field has empty metadata.
   *
   * @throws {ProtocolError} on truncation, an endpoint {@link checkEndpoint}
   * refuses, or metadata over 4096 bytes.
   */
  static decode(bytes: Uint8Array): RegisterRequest {
    return guard('REGISTER request', () => {
      const r = new ByteReader(bytes);
      const type = r.name('type');
      const requestedInstance = r.u48('instance');
      const endpoints = readEndpoints(r);
      return new RegisterRequest(type, { requestedInstance, endpoints, metadata: readMetadata(r) });
    });
  }
}

/** `REGISTER` final response payload: the assigned instance (u48). */
export class RegisterResponse {
  /** The assigned instance id. */
  readonly instance: number;

  constructor(instance: number) {
    this.instance = instance;
  }

  /** Encodes the payload. @throws {RangeError} outside u48. */
  encode(): Uint8Array {
    return new ByteWriter(6).u48(this.instance).toBytes();
  }

  /** Decodes the payload; trailing bytes are ignored. @throws {ProtocolError} on truncation. */
  static decode(bytes: Uint8Array): RegisterResponse {
    return guard(
      'REGISTER response',
      () => new RegisterResponse(new ByteReader(bytes).u48('instance')),
    );
  }
}

/** `UNREGSTR` request payload (name, u48 instance), also the layout of a `DOWN` event. */
export class UnregisterRequest {
  /** Service type. */
  readonly type: Name;
  /** Instance id. */
  readonly instance: number;

  constructor(type: NameLike, instance: number) {
    this.type = toName(type);
    this.instance = instance;
  }

  /** Encodes the payload. @throws {RangeError} outside u48. */
  encode(): Uint8Array {
    return new ByteWriter(14).name(this.type).u48(this.instance).toBytes();
  }

  /** Decodes the payload; trailing bytes are ignored. @throws {ProtocolError} on truncation. */
  static decode(bytes: Uint8Array): UnregisterRequest {
    return guard('UNREGSTR request', () => {
      const r = new ByteReader(bytes);
      const type = r.name('type');
      return new UnregisterRequest(type, r.u48('instance'));
    });
  }
}

/** `WATCH` and `UNWATCH` request payload, also the `LOOKUP` payload: a type, or nothing for every type. */
export class WatchRequest {
  /** Type to watch, or undefined for every type. */
  readonly type: Name | undefined;

  /** A request; no [type] (or an empty one) watches every type. */
  constructor(type?: NameLike) {
    const name = type === undefined ? undefined : toName(type);
    this.type = name === undefined || name.isEmpty ? undefined : name;
  }

  /** Encodes the payload: empty for every type. */
  encode(): Uint8Array {
    return this.type === undefined
      ? new Uint8Array(0)
      : new ByteWriter(8).name(this.type).toBytes();
  }

  /** Decodes the payload. Empty or an all-zero name means every type; trailing bytes are ignored. */
  static decode(bytes: Uint8Array): WatchRequest {
    if (bytes.length === 0) {
      return new WatchRequest();
    }
    return guard('WATCH request', () => new WatchRequest(new ByteReader(bytes).name('type')));
  }
}

// ---------------------------------------------------------------------------
// Sharding

/** How a slot space is governed. */
export const SlotMode = {
  /** The naming service assigns free slots and rebalances. */
  managed: 0,
  /** Instances claim the slots they are configured for; the naming service records, arbitrates and orchestrates. */
  static: 1,
} as const;

/** A slot mode: one of the values of {@link SlotMode}. */
export type SlotMode = (typeof SlotMode)[keyof typeof SlotMode];

/** The name of a slot mode, for logs. */
export function slotModeName(mode: SlotMode): string {
  return mode === SlotMode.static ? 'static' : 'managed';
}

/** The state of one slot. */
export const SlotState = {
  /** Nobody serves it. */
  free: 0,
  /** Served by its owner. */
  owned: 1,
  /** Hand-over in progress; routed to the old owner (`from`). */
  migrating: 2,
} as const;

/** A slot state: one of the values of {@link SlotState}. */
export type SlotState = (typeof SlotState)[keyof typeof SlotState];

const slotStateNames = ['free', 'owned', 'migrating'] as const;

/** The name of a slot state, for logs. */
export function slotStateName(state: SlotState): string {
  return slotStateNames[state];
}

function slotStateOf(code: number): SlotState {
  if (code === 0 || code === 1 || code === 2) {
    return code;
  }
  throw new ProtocolError(`unknown slot state ${code}`);
}

/** Progress of a migration, reported in `PHASE` items of `MIGRATE`. */
export const MigrationPhase = {
  /** `DRAIN` sent to the old owner. */
  draining: 1,
  /** `ASSIGN` sent to the new owner. */
  assigning: 2,
  /** The new owner serves; `FORWARD` sent to the old owner. */
  forwarding: 3,
  /** The hand-over is complete. */
  done: 4,
  /** The hand-over failed and was rolled back. */
  rolledBack: 5,
} as const;

/** A migration phase: one of the values of {@link MigrationPhase}. */
export type MigrationPhase = (typeof MigrationPhase)[keyof typeof MigrationPhase];

const phaseNames = ['', 'draining', 'assigning', 'forwarding', 'done', 'rolledBack'] as const;

/** The name of a migration phase, for logs. */
export function migrationPhaseName(phase: MigrationPhase): string {
  return phaseNames[phase];
}

/**
 * A slot space definition, also the payload of a `SLOTSPC` event: name
 * type, u32 count, u8 mode, u8 flags (0x01 lazy, 0x02 shared recovery).
 * Unknown flag bits are ignored when decoding.
 */
export class SlotSpace {
  /** Flag bit: slots are assigned on first `LOCATE` (managed mode). */
  static readonly lazyFlag = 0x01;
  /** Flag bit: shared recovery; state is recoverable from shared storage. */
  static readonly sharedFlag = 0x02;

  /** The sharded service type. */
  readonly type: Name;
  /** Number of slots, numbered `0 .. count - 1`. */
  readonly count: number;
  /** Managed or static. */
  readonly mode: SlotMode;
  /** Slots are assigned when first located rather than up front. */
  readonly lazy: boolean;
  /** Shared recovery: a slot whose owner went down is reassigned at once. Otherwise holder-only. */
  readonly shared: boolean;

  constructor(
    type: NameLike,
    init: { count: number; mode?: SlotMode; lazy?: boolean; shared?: boolean },
  ) {
    this.type = toName(type);
    this.count = init.count;
    this.mode = init.mode ?? SlotMode.managed;
    this.lazy = init.lazy ?? false;
    this.shared = init.shared ?? false;
  }

  /** The flags byte. */
  get flags(): number {
    return (this.lazy ? SlotSpace.lazyFlag : 0) | (this.shared ? SlotSpace.sharedFlag : 0);
  }

  /** @internal Writes the `SLOTSPC` layout. */
  writeTo(w: ByteWriter): void {
    checkU32(this.count, 'count');
    w.name(this.type);
    w.u32(this.count);
    w.u8(this.mode);
    w.u8(this.flags);
  }

  /** @internal Reads the `SLOTSPC` layout. */
  static readFrom(r: ByteReader): SlotSpace {
    const type = r.name('type');
    const count = r.u32('count');
    const mode = r.u8('mode');
    if (mode !== SlotMode.managed && mode !== SlotMode.static) {
      throw new ProtocolError(`unknown slot mode ${mode}`);
    }
    const flags = r.u8('flags');
    return new SlotSpace(type, {
      count,
      mode,
      lazy: (flags & SlotSpace.lazyFlag) !== 0,
      shared: (flags & SlotSpace.sharedFlag) !== 0,
    });
  }

  /** Encodes the `SLOTSPC` payload. @throws {RangeError} if the count is outside u32. */
  encode(): Uint8Array {
    const w = new ByteWriter(14);
    this.writeTo(w);
    return w.toBytes();
  }

  /** Decodes a `SLOTSPC` payload; trailing bytes are ignored. @throws {ProtocolError} on truncation or an unknown mode. */
  static decode(bytes: Uint8Array): SlotSpace {
    return guard('SLOTSPC', () => SlotSpace.readFrom(new ByteReader(bytes)));
  }

  equals(other: unknown): boolean {
    return (
      other instanceof SlotSpace &&
      other.type.equals(this.type) &&
      other.count === this.count &&
      other.mode === this.mode &&
      other.lazy === this.lazy &&
      other.shared === this.shared
    );
  }

  toString(): string {
    return (
      `SlotSpace(${this.type}, ${this.count}, ${slotModeName(this.mode)}` +
      `${this.lazy ? ', lazy' : ''}${this.shared ? ', shared' : ', holder-only'})`
    );
  }
}

/** `SLOTS` request payload: a {@link SlotSpace} and this instance's capacity (the `SLOTSPC` layout, then u32 capacity). */
export class SlotsRequest {
  /** The space to define or confirm. */
  readonly space: SlotSpace;
  /** The caller's share weight in a managed space; 0 takes no slots (it only defines or confirms the space). */
  readonly capacity: number;

  constructor(space: SlotSpace, init: { capacity: number }) {
    this.space = space;
    this.capacity = init.capacity;
  }

  /** Encodes the payload. @throws {RangeError} for values outside u32. */
  encode(): Uint8Array {
    checkU32(this.capacity, 'capacity');
    const w = new ByteWriter(18);
    this.space.writeTo(w);
    w.u32(this.capacity);
    return w.toBytes();
  }

  /** Decodes the payload; trailing bytes are ignored. @throws {ProtocolError} on truncation or an unknown mode. */
  static decode(bytes: Uint8Array): SlotsRequest {
    return guard('SLOTS request', () => {
      const r = new ByteReader(bytes);
      const space = SlotSpace.readFrom(r);
      return new SlotsRequest(space, { capacity: r.u32('capacity') });
    });
  }
}

function writeSlots(w: ByteWriter, slots: readonly number[]): void {
  checkU32(slots.length, 'slot count');
  w.u32(slots.length);
  for (const slot of slots) {
    checkU32(slot, 'slot');
    w.u32(slot);
  }
}

function readSlots(r: ByteReader): readonly number[] {
  const n = r.u32('slot count');
  if (n > Math.floor(r.remaining / 4)) {
    throw new ProtocolError(`truncated slot list of ${n}`);
  }
  const slots: number[] = [];
  for (let i = 0; i < n; i++) {
    slots.push(r.u32('slot'));
  }
  return Object.freeze(slots);
}

/** `HOLDING` request payload: name type, u32 n, u32 slot × n. */
export class HoldingRequest {
  /** The sharded type. */
  readonly type: Name;
  /** Slots whose storage the caller holds. */
  readonly slots: readonly number[];

  constructor(type: NameLike, slots: readonly number[]) {
    this.type = toName(type);
    this.slots = slots;
  }

  /** Encodes the payload. @throws {RangeError} for a slot outside u32. */
  encode(): Uint8Array {
    const w = new ByteWriter(12 + 4 * this.slots.length);
    w.name(this.type);
    writeSlots(w, this.slots);
    return w.toBytes();
  }

  /** Decodes the payload; trailing bytes are ignored. @throws {ProtocolError} on truncation. */
  static decode(bytes: Uint8Array): HoldingRequest {
    return guard('HOLDING request', () => {
      const r = new ByteReader(bytes);
      const type = r.name('type');
      return new HoldingRequest(type, readSlots(r));
    });
  }
}

/** `HOLDING` final response payload: the slots whose holder is no longer the caller (u32 m, u32 slot × m). */
export class HoldingResponse {
  /** Slots whose local storage the caller should discard. */
  readonly discard: readonly number[];

  constructor(discard: readonly number[]) {
    this.discard = discard;
  }

  /** Encodes the payload. @throws {RangeError} for a slot outside u32. */
  encode(): Uint8Array {
    const w = new ByteWriter(4 + 4 * this.discard.length);
    writeSlots(w, this.discard);
    return w.toBytes();
  }

  /** Decodes the payload; trailing bytes are ignored. @throws {ProtocolError} on truncation. */
  static decode(bytes: Uint8Array): HoldingResponse {
    return guard('HOLDING response', () => new HoldingResponse(readSlots(new ByteReader(bytes))));
  }
}

/**
 * `CLAIM` request payload: name type, u32 slot, u8 flags (0x01 holding),
 * u32 epoch. The epoch is the claimant's last known epoch of the slot (0
 * if none); the naming service uses it to resolve conflicting claims after
 * a restart.
 */
export class ClaimRequest {
  /** Flag bit: the claimant holds the slot's storage. */
  static readonly holdingFlag = 0x01;

  /** The sharded type. */
  readonly type: Name;
  /** The slot to take. */
  readonly slot: number;
  /** The claimant holds the slot's storage. */
  readonly holding: boolean;
  /** The claimant's last known epoch of the slot, 0 if none. */
  readonly epoch: number;

  constructor(type: NameLike, slot: number, init: { holding?: boolean; epoch?: number } = {}) {
    this.type = toName(type);
    this.slot = slot;
    this.holding = init.holding ?? false;
    this.epoch = init.epoch ?? 0;
  }

  /** Encodes the payload. @throws {RangeError} for values outside u32. */
  encode(): Uint8Array {
    checkU32(this.slot, 'slot');
    checkU32(this.epoch, 'epoch');
    return new ByteWriter(17)
      .name(this.type)
      .u32(this.slot)
      .u8(this.holding ? ClaimRequest.holdingFlag : 0)
      .u32(this.epoch)
      .toBytes();
  }

  /** Decodes the payload; trailing bytes are ignored. @throws {ProtocolError} on truncation. */
  static decode(bytes: Uint8Array): ClaimRequest {
    return guard('CLAIM request', () => {
      const r = new ByteReader(bytes);
      const type = r.name('type');
      const slot = r.u32('slot');
      const flags = r.u8('flags');
      return new ClaimRequest(type, slot, {
        holding: (flags & ClaimRequest.holdingFlag) !== 0,
        epoch: r.u32('epoch'),
      });
    });
  }
}

/** `CLAIM` final response payload: the slot's epoch (u32). */
export class ClaimResponse {
  /** The slot's epoch with the claimant as owner. */
  readonly epoch: number;

  constructor(epoch: number) {
    this.epoch = epoch;
  }

  /** Encodes the payload. @throws {RangeError} outside u32. */
  encode(): Uint8Array {
    checkU32(this.epoch, 'epoch');
    return new ByteWriter(4).u32(this.epoch).toBytes();
  }

  /** Decodes the payload; trailing bytes are ignored. @throws {ProtocolError} on truncation. */
  static decode(bytes: Uint8Array): ClaimResponse {
    return guard('CLAIM response', () => new ClaimResponse(new ByteReader(bytes).u32('epoch')));
  }
}

/** `RELEASE` request payload: name type, u32 slot, u8 flags (0x01 keep storage). */
export class ReleaseRequest {
  /** Flag bit: the caller keeps the storage and stays holder. */
  static readonly keepStorageFlag = 0x01;

  /** The sharded type. */
  readonly type: Name;
  /** The slot to give up. */
  readonly slot: number;
  /** The caller keeps the slot's storage and stays its holder. */
  readonly keepStorage: boolean;

  constructor(type: NameLike, slot: number, init: { keepStorage?: boolean } = {}) {
    this.type = toName(type);
    this.slot = slot;
    this.keepStorage = init.keepStorage ?? false;
  }

  /** Encodes the payload. @throws {RangeError} outside u32. */
  encode(): Uint8Array {
    checkU32(this.slot, 'slot');
    return new ByteWriter(13)
      .name(this.type)
      .u32(this.slot)
      .u8(this.keepStorage ? ReleaseRequest.keepStorageFlag : 0)
      .toBytes();
  }

  /** Decodes the payload; trailing bytes are ignored. @throws {ProtocolError} on truncation. */
  static decode(bytes: Uint8Array): ReleaseRequest {
    return guard('RELEASE request', () => {
      const r = new ByteReader(bytes);
      const type = r.name('type');
      const slot = r.u32('slot');
      const flags = r.u8('flags');
      return new ReleaseRequest(type, slot, {
        keepStorage: (flags & ReleaseRequest.keepStorageFlag) !== 0,
      });
    });
  }
}

/** `LOCATE` request payload: name type, u32 slot. */
export class LocateRequest {
  /** The sharded type. */
  readonly type: Name;
  /** The slot to locate. */
  readonly slot: number;

  constructor(type: NameLike, slot: number) {
    this.type = toName(type);
    this.slot = slot;
  }

  /** Encodes the payload. @throws {RangeError} outside u32. */
  encode(): Uint8Array {
    checkU32(this.slot, 'slot');
    return new ByteWriter(12).name(this.type).u32(this.slot).toBytes();
  }

  /** Decodes the payload; trailing bytes are ignored. @throws {ProtocolError} on truncation. */
  static decode(bytes: Uint8Array): LocateRequest {
    return guard('LOCATE request', () => {
      const r = new ByteReader(bytes);
      const type = r.name('type');
      return new LocateRequest(type, r.u32('slot'));
    });
  }
}

/**
 * `LOCATE` final response payload: u8 state, u48 owner, u32 epoch. While
 * migrating, the owner is the old owner (`from`), which keeps receiving
 * the slot's traffic.
 */
export class LocateResponse {
  /** The slot's state. */
  readonly state: SlotState;
  /** The instance to route to, 0 when the slot is free. */
  readonly owner: number;
  /** The slot's epoch. */
  readonly epoch: number;

  constructor(state: SlotState, owner: number, epoch: number) {
    this.state = state;
    this.owner = owner;
    this.epoch = epoch;
  }

  /** Encodes the payload. @throws {RangeError} for values out of range. */
  encode(): Uint8Array {
    checkU48(this.owner, 'owner');
    checkU32(this.epoch, 'epoch');
    return new ByteWriter(11).u8(this.state).u48(this.owner).u32(this.epoch).toBytes();
  }

  /** Decodes the payload; trailing bytes are ignored. @throws {ProtocolError} on truncation or an unknown state. */
  static decode(bytes: Uint8Array): LocateResponse {
    return guard('LOCATE response', () => {
      const r = new ByteReader(bytes);
      const state = slotStateOf(r.u8('state'));
      const owner = r.u48('owner');
      return new LocateResponse(state, owner, r.u32('epoch'));
    });
  }

  equals(other: unknown): boolean {
    return (
      other instanceof LocateResponse &&
      other.state === this.state &&
      other.owner === this.owner &&
      other.epoch === this.epoch
    );
  }

  toString(): string {
    return `LocateResponse(${slotStateName(this.state)}, ${hex(this.owner)}, ${this.epoch})`;
  }
}

/** `MIGRATE` request payload: name type, u32 slot, u48 to (0 lets the allocator choose). */
export class MigrateRequest {
  /** The sharded type. */
  readonly type: Name;
  /** The slot to move. */
  readonly slot: number;
  /** The new owner, 0 to let the allocator choose. */
  readonly to: number;

  constructor(type: NameLike, slot: number, init: { to?: number } = {}) {
    this.type = toName(type);
    this.slot = slot;
    this.to = init.to ?? 0;
  }

  /** Encodes the payload. @throws {RangeError} for values out of range. */
  encode(): Uint8Array {
    checkU32(this.slot, 'slot');
    checkU48(this.to, 'to');
    return new ByteWriter(18).name(this.type).u32(this.slot).u48(this.to).toBytes();
  }

  /** Decodes the payload; trailing bytes are ignored. @throws {ProtocolError} on truncation. */
  static decode(bytes: Uint8Array): MigrateRequest {
    return guard('MIGRATE request', () => {
      const r = new ByteReader(bytes);
      const type = r.name('type');
      const slot = r.u32('slot');
      return new MigrateRequest(type, slot, { to: r.u48('to') });
    });
  }
}

/** `PHASE` item payload of a `MIGRATE` stream: u8 phase. */
export class PhaseItem {
  /** The phase reached. */
  readonly phase: MigrationPhase;

  constructor(phase: MigrationPhase) {
    this.phase = phase;
  }

  /** Encodes the payload. */
  encode(): Uint8Array {
    return Uint8Array.of(this.phase);
  }

  /** Decodes the payload; trailing bytes are ignored. @throws {ProtocolError} on truncation or an unknown phase. */
  static decode(bytes: Uint8Array): PhaseItem {
    return guard('PHASE', () => {
      const code = new ByteReader(bytes).u8('phase');
      if (code < MigrationPhase.draining || code > MigrationPhase.rolledBack) {
        throw new ProtocolError(`unknown migration phase ${code}`);
      }
      return new PhaseItem(code as MigrationPhase);
    });
  }

  equals(other: unknown): boolean {
    return other instanceof PhaseItem && other.phase === this.phase;
  }

  toString(): string {
    return `PhaseItem(${migrationPhaseName(this.phase)})`;
  }
}

/**
 * `ASSIGN` request payload, from the naming service to an instance: name
 * type, u32 slot, u32 epoch, u48 holder, u8 flags (0x01 shared recovery).
 */
export class AssignRequest {
  /** Flag bit: state may be loaded from shared storage. */
  static readonly sharedFlag = 0x01;

  /** The sharded type. */
  readonly type: Name;
  /** The slot to serve. */
  readonly slot: number;
  /** The slot's epoch once this instance serves it. */
  readonly epoch: number;
  /** Where the slot's state is: 0 nowhere, this instance (reclaim), another (fetch it from there). */
  readonly holder: number;
  /** Shared recovery: state may be loaded from shared storage instead of the holder. */
  readonly shared: boolean;

  constructor(
    type: NameLike,
    slot: number,
    init: { epoch: number; holder?: number; shared?: boolean },
  ) {
    this.type = toName(type);
    this.slot = slot;
    this.epoch = init.epoch;
    this.holder = init.holder ?? 0;
    this.shared = init.shared ?? false;
  }

  /** Encodes the payload. @throws {RangeError} for values out of range. */
  encode(): Uint8Array {
    checkU32(this.slot, 'slot');
    checkU32(this.epoch, 'epoch');
    checkU48(this.holder, 'holder');
    return new ByteWriter(23)
      .name(this.type)
      .u32(this.slot)
      .u32(this.epoch)
      .u48(this.holder)
      .u8(this.shared ? AssignRequest.sharedFlag : 0)
      .toBytes();
  }

  /** Decodes the payload; trailing bytes are ignored. @throws {ProtocolError} on truncation. */
  static decode(bytes: Uint8Array): AssignRequest {
    return guard('ASSIGN request', () => {
      const r = new ByteReader(bytes);
      const type = r.name('type');
      const slot = r.u32('slot');
      const epoch = r.u32('epoch');
      const holder = r.u48('holder');
      const flags = r.u8('flags');
      return new AssignRequest(type, slot, {
        epoch,
        holder,
        shared: (flags & AssignRequest.sharedFlag) !== 0,
      });
    });
  }

  toString(): string {
    return (
      `ASSIGN ${this.type}/${this.slot} epoch ${this.epoch} holder ${hex(this.holder)}` +
      `${this.shared ? ' shared' : ''}`
    );
  }
}

/**
 * `ASSIGN` final response payload: empty (the instance now holds the slot's
 * storage), or u8 flags with 0x01 "not holding" (the previous holder is
 * kept; a stateless service always answers this).
 */
export class AssignResponse {
  /** Flag bit: not holding. */
  static readonly notHoldingFlag = 0x01;

  /** The instance does not hold the slot's storage. */
  readonly notHolding: boolean;

  constructor(init: { notHolding?: boolean } = {}) {
    this.notHolding = init.notHolding ?? false;
  }

  /** Encodes the payload: empty unless not holding. */
  encode(): Uint8Array {
    return this.notHolding ? Uint8Array.of(AssignResponse.notHoldingFlag) : new Uint8Array(0);
  }

  /** Decodes the payload. Empty means holding; unknown flag bits and trailing bytes are ignored. */
  static decode(bytes: Uint8Array): AssignResponse {
    return new AssignResponse({
      notHolding: bytes.length > 0 && ((bytes[0] as number) & AssignResponse.notHoldingFlag) !== 0,
    });
  }
}

function encodeHandover(type: Name, slot: number, epoch: number, to: number): Uint8Array {
  checkU32(slot, 'slot');
  checkU32(epoch, 'epoch');
  checkU48(to, 'to');
  return new ByteWriter(22).name(type).u32(slot).u32(epoch).u48(to).toBytes();
}

function decodeHandover(
  what: string,
  bytes: Uint8Array,
): { type: Name; slot: number; epoch: number; to: number } {
  return guard(what, () => {
    const r = new ByteReader(bytes);
    const type = r.name('type');
    const slot = r.u32('slot');
    const epoch = r.u32('epoch');
    return { type, slot, epoch, to: r.u48('to') };
  });
}

/**
 * `DRAIN` request payload, from the naming service to the owner: name type,
 * u32 slot, u32 epoch (the slot's epoch after the migration), u48 to.
 */
export class DrainRequest {
  /** The sharded type. */
  readonly type: Name;
  /** The slot to drain. */
  readonly slot: number;
  /** The epoch the slot will have after the migration. */
  readonly epoch: number;
  /** The new owner. */
  readonly to: number;

  constructor(type: NameLike, slot: number, init: { epoch: number; to: number }) {
    this.type = toName(type);
    this.slot = slot;
    this.epoch = init.epoch;
    this.to = init.to;
  }

  /** Encodes the payload. @throws {RangeError} for values out of range. */
  encode(): Uint8Array {
    return encodeHandover(this.type, this.slot, this.epoch, this.to);
  }

  /** Decodes the payload; trailing bytes are ignored. @throws {ProtocolError} on truncation. */
  static decode(bytes: Uint8Array): DrainRequest {
    const h = decodeHandover('DRAIN request', bytes);
    return new DrainRequest(h.type, h.slot, h);
  }

  toString(): string {
    return `DRAIN ${this.type}/${this.slot} epoch ${this.epoch} to ${hex(this.to)}`;
  }
}

/**
 * `FORWARD` request payload, from the naming service to the old owner: name
 * type, u32 slot, u32 epoch (the new epoch), u48 to.
 */
export class ForwardRequest {
  /** The sharded type. */
  readonly type: Name;
  /** The slot handed over. */
  readonly slot: number;
  /** The slot's new epoch. */
  readonly epoch: number;
  /** The new owner, which now serves the slot. */
  readonly to: number;

  constructor(type: NameLike, slot: number, init: { epoch: number; to: number }) {
    this.type = toName(type);
    this.slot = slot;
    this.epoch = init.epoch;
    this.to = init.to;
  }

  /** Encodes the payload. @throws {RangeError} for values out of range. */
  encode(): Uint8Array {
    return encodeHandover(this.type, this.slot, this.epoch, this.to);
  }

  /** Decodes the payload; trailing bytes are ignored. @throws {ProtocolError} on truncation. */
  static decode(bytes: Uint8Array): ForwardRequest {
    const h = decodeHandover('FORWARD request', bytes);
    return new ForwardRequest(h.type, h.slot, h);
  }

  toString(): string {
    return `FORWARD ${this.type}/${this.slot} epoch ${this.epoch} to ${hex(this.to)}`;
  }
}

/**
 * `RESUME` request payload, from the naming service to the owner of a slot
 * whose migration was abandoned: name type, u32 slot, u32 epoch (unchanged).
 */
export class ResumeRequest {
  /** The sharded type. */
  readonly type: Name;
  /** The slot to unlock. */
  readonly slot: number;
  /** The slot's epoch, unchanged by the abandoned migration. */
  readonly epoch: number;

  constructor(type: NameLike, slot: number, init: { epoch: number }) {
    this.type = toName(type);
    this.slot = slot;
    this.epoch = init.epoch;
  }

  /** Encodes the payload. @throws {RangeError} for values out of range. */
  encode(): Uint8Array {
    checkU32(this.slot, 'slot');
    checkU32(this.epoch, 'epoch');
    return new ByteWriter(16).name(this.type).u32(this.slot).u32(this.epoch).toBytes();
  }

  /** Decodes the payload; trailing bytes are ignored. @throws {ProtocolError} on truncation. */
  static decode(bytes: Uint8Array): ResumeRequest {
    return guard('RESUME request', () => {
      const r = new ByteReader(bytes);
      const type = r.name('type');
      const slot = r.u32('slot');
      return new ResumeRequest(type, slot, { epoch: r.u32('epoch') });
    });
  }

  toString(): string {
    return `RESUME ${this.type}/${this.slot} epoch ${this.epoch}`;
  }
}

/** What a {@link SlotEntry} takes beside its state. */
export interface SlotEntryInit {
  /** The owner, or while migrating the old owner (`from`); 0 when free. */
  readonly owner?: number | undefined;
  /** The new owner while migrating, else 0. */
  readonly to?: number | undefined;
  /** The instance holding the slot's durable state, 0 if none. */
  readonly holder?: number | undefined;
  /** Incremented at every ownership change. */
  readonly epoch?: number | undefined;
}

/**
 * The state of one slot in a slot table. For {@link SlotState.migrating},
 * {@link owner} is the old owner (`from`), which keeps receiving the
 * slot's traffic, and {@link to} the new one.
 */
export class SlotEntry {
  /** The state. */
  readonly state: SlotState;
  /** The instance traffic for the slot goes to: the owner, or the old owner while migrating; 0 when free. */
  readonly owner: number;
  /** The new owner while migrating, else 0. */
  readonly to: number;
  /** The instance holding the slot's durable state, 0 if none. */
  readonly holder: number;
  /** Incremented at every ownership change. */
  readonly epoch: number;

  constructor(state: SlotState, init: SlotEntryInit = {}) {
    this.state = state;
    this.owner = init.owner ?? 0;
    this.to = init.to ?? 0;
    this.holder = init.holder ?? 0;
    this.epoch = init.epoch ?? 0;
  }

  /** A free slot. */
  static free(init: { holder?: number; epoch?: number } = {}): SlotEntry {
    return new SlotEntry(SlotState.free, init);
  }

  /** A slot served by [owner]. */
  static owned(owner: number, init: { holder?: number; epoch?: number } = {}): SlotEntry {
    return new SlotEntry(SlotState.owned, { ...init, owner });
  }

  /** A slot being handed over from [from] to [to]. */
  static migrating(
    from: number,
    to: number,
    init: { holder?: number; epoch?: number } = {},
  ): SlotEntry {
    return new SlotEntry(SlotState.migrating, { ...init, owner: from, to });
  }

  /** A slot nobody serves, holds or has ever served: free, holder 0, epoch 0. */
  static readonly unassigned: SlotEntry = new SlotEntry(SlotState.free);

  /** True when free. */
  get isFree(): boolean {
    return this.state === SlotState.free;
  }

  /** True when free with holder 0: such slots are left out of snapshots. */
  get isEmpty(): boolean {
    return this.state === SlotState.free && this.holder === 0;
  }

  equals(other: unknown): boolean {
    return (
      other instanceof SlotEntry &&
      other.state === this.state &&
      other.owner === this.owner &&
      other.to === this.to &&
      other.holder === this.holder &&
      other.epoch === this.epoch
    );
  }

  toString(): string {
    switch (this.state) {
      case SlotState.free:
        return `free holder ${hex(this.holder)} epoch ${this.epoch}`;
      case SlotState.owned:
        return `owned ${hex(this.owner)} holder ${hex(this.holder)} epoch ${this.epoch}`;
      default:
        return (
          `migrating ${hex(this.owner)}->${hex(this.to)} holder ${hex(this.holder)} ` +
          `epoch ${this.epoch}`
        );
    }
  }
}

/**
 * `SLOT` event payload, one slot of a type and its state: name type, u32
 * slot, u8 state, u48 owner (or from), u48 to, u48 holder, u32 epoch.
 */
export class SlotItem {
  /** The sharded type. */
  readonly type: Name;
  /** The slot number. */
  readonly slot: number;
  /** The slot's state. */
  readonly entry: SlotEntry;

  constructor(type: NameLike, slot: number, entry: SlotEntry) {
    this.type = toName(type);
    this.slot = slot;
    this.entry = entry;
  }

  /** Encodes the payload. @throws {RangeError} for values out of range. */
  encode(): Uint8Array {
    const e = this.entry;
    checkU32(this.slot, 'slot');
    checkU48(e.owner, 'owner');
    checkU48(e.to, 'to');
    checkU48(e.holder, 'holder');
    checkU32(e.epoch, 'epoch');
    return new ByteWriter(35)
      .name(this.type)
      .u32(this.slot)
      .u8(e.state)
      .u48(e.owner)
      .u48(e.to)
      .u48(e.holder)
      .u32(e.epoch)
      .toBytes();
  }

  /** Decodes the payload; trailing bytes are ignored. @throws {ProtocolError} on truncation or an unknown state. */
  static decode(bytes: Uint8Array): SlotItem {
    return guard('SLOT', () => {
      const r = new ByteReader(bytes);
      const type = r.name('type');
      const slot = r.u32('slot');
      const state = slotStateOf(r.u8('state'));
      const owner = r.u48('owner');
      const to = r.u48('to');
      const holder = r.u48('holder');
      const epoch = r.u32('epoch');
      return new SlotItem(type, slot, new SlotEntry(state, { owner, to, holder, epoch }));
    });
  }

  equals(other: unknown): boolean {
    return (
      other instanceof SlotItem &&
      other.type.equals(this.type) &&
      other.slot === this.slot &&
      other.entry.equals(this.entry)
    );
  }

  toString(): string {
    return `SLOT ${this.type}/${this.slot} ${this.entry}`;
  }
}

/**
 * The fixed fields of a `MOVED` or `RELOCATED` status (wiki pages
 * "Polyverse Switchboard Status Codes" and "Polyverse Switchboard
 * Sharding", section "Routing"): after the `u16` code, `u48 owner` (the
 * slot's current owner, 0 = unknown) and `u32 epoch` (the slot's current
 * epoch, 0 = unknown), then the usual optional UTF-8 reason. The owner's
 * type is the one the channel was addressed to.
 *
 * `MOVED` (37) means nothing on the channel (or in the request) was
 * processed, so it may be sent again to the owner; `RELOCATED` (38) means
 * the slot moved after work had started, and is never retried
 * automatically.
 */
export class MovedStatus {
  /** Bytes of the fixed fields after the code: `u48 owner`, `u32 epoch`. */
  static readonly fieldsLength = 10;

  /** The slot's current owner instance, 0 if unknown. */
  readonly owner: number;
  /** The slot's current epoch, 0 if unknown. */
  readonly epoch: number;
  /** The human readable reason after the fields, for logs; may be empty. */
  readonly reason: string;

  /**
   * Fields; owner and epoch 0 mean unknown.
   *
   * @throws {RangeError} if [owner] is not a u48 or [epoch] not a u32.
   */
  constructor(init: { owner?: number; epoch?: number; reason?: string } = {}) {
    this.owner = init.owner ?? 0;
    this.epoch = init.epoch ?? 0;
    this.reason = init.reason ?? '';
    checkU48(this.owner, 'owner');
    checkU32(this.epoch, 'epoch');
  }

  /** No owner, no epoch, no reason. */
  static readonly unknown: MovedStatus = new MovedStatus();

  /** Whether the owner is known. */
  get hasOwner(): boolean {
    return this.owner !== 0;
  }

  /** The owner as an address of [type] (the channel's type), or undefined when unknown. */
  ownerOf(type: NameLike): ServiceAddress | undefined {
    return this.owner === 0 ? undefined : new ServiceAddress(type, this.owner);
  }

  /** Whether [status] is a `MOVED` or a `RELOCATED`, the two codes that carry these fields. */
  static carriesFields(status: Status): boolean {
    return status.code === StatusCode.moved || status.code === StatusCode.relocated;
  }

  /** The status payload: the code (MOVED, or RELOCATED when [relocated]), the fields and the reason. */
  encode(options: { relocated?: boolean } = {}): Uint8Array {
    const w = new ByteWriter(2 + MovedStatus.fieldsLength + this.reason.length * 3)
      .u16(options.relocated ? StatusCode.relocated : StatusCode.moved)
      .u48(this.owner)
      .u32(this.epoch);
    if (this.reason.length > 0) {
      w.bytes(utf8Encode(this.reason));
    }
    return w.toBytes();
  }

  /** A MOVED status carrying the fields, or a RELOCATED one when [relocated]. */
  toStatus(options: { relocated?: boolean } = {}): Status {
    return Status.decode(this.encode(options));
  }

  /**
   * Reads the fields of a status payload. Never throws: a payload whose code
   * is neither MOVED (37) nor RELOCATED (38), or that is too short for the
   * fields, reads as {@link unknown}. A malformed reason is decoded with
   * replacement characters.
   */
  static parse(statusPayload: Uint8Array): MovedStatus {
    if (statusPayload.length < 2 + MovedStatus.fieldsLength) {
      return MovedStatus.unknown;
    }
    const r = new ByteReader(statusPayload);
    const code = r.u16();
    if (code !== StatusCode.moved && code !== StatusCode.relocated) {
      return MovedStatus.unknown;
    }
    const owner = r.u48();
    const epoch = r.u32();
    const rest = r.rest();
    return new MovedStatus({ owner, epoch, reason: rest.length === 0 ? '' : utf8Decode(rest) });
  }

  /** Reads the fields of [status] (see {@link parse}): unknown unless a MOVED or RELOCATED long enough to carry them. */
  static fromStatus(status: Status): MovedStatus {
    if (!MovedStatus.carriesFields(status)) {
      return MovedStatus.unknown;
    }
    return MovedStatus.parse(status.encode());
  }

  equals(other: unknown): boolean {
    return (
      other instanceof MovedStatus &&
      other.owner === this.owner &&
      other.epoch === this.epoch &&
      other.reason === this.reason
    );
  }

  toString(): string {
    const where = this.owner === 0 ? 'unknown owner' : `owner ${hex(this.owner)}`;
    const when = this.epoch === 0 ? '' : `, epoch ${this.epoch}`;
    return `MovedStatus(${where}${when}${this.reason.length === 0 ? '' : `, ${this.reason}`})`;
  }
}

// ---------------------------------------------------------------------------
// Reverse connections

/** Reads a `len8` UTF-8 identity, strictly: malformed UTF-8 is an error. */
function readIdentity(r: ByteReader, what: string): string {
  const bytes = r.take(r.u8(`${what} length`), what);
  try {
    return strictUtf8.decode(bytes);
  } catch {
    throw new ProtocolError(`${what} is not UTF-8`);
  }
}

function writeIdentity(w: ByteWriter, identity: string, what: string): void {
  const bytes = utf8Encode(identity);
  if (bytes.length > 0xff) {
    throw new RangeError(`${what} "${identity}" longer than 255 bytes`);
  }
  w.u8(bytes.length);
  w.bytes(bytes);
}

function readIntent(r: ByteReader): Uint8Array {
  const length = r.u8('intent length');
  if (length > maxConnectIntentLength) {
    throw new ProtocolError(`intent of ${length} bytes, more than ${maxConnectIntentLength}`);
  }
  return r.take(length, 'intent').slice();
}

function writeIntent(w: ByteWriter, intent: Uint8Array): void {
  if (intent.length > maxConnectIntentLength) {
    throw new RangeError(`intent of ${intent.length} bytes, more than ${maxConnectIntentLength}`);
  }
  w.u8(intent.length);
  w.bytes(intent);
}

function writeEndpoint(w: ByteWriter, endpoint: string): void {
  checkEndpoint(endpoint);
  w.string8(endpoint);
}

/**
 * `CONNECT` request payload, from a consumer to the naming service: have
 * instance [instance] of [type] dial [endpoint] and identify there with
 * [intent]. Wire: name type, u48 instance, len8 endpoint URI, len8 intent
 * (at most 64 bytes).
 */
export class ConnectRequest {
  /** The type of the instance. */
  readonly type: Name;
  /** The instance to dial back. */
  readonly instance: number;
  /** Where the instance dials: the consumer's listener. */
  readonly endpoint: string;
  /** Opaque, chosen by the consumer; the instance's `IDENT` carries it. */
  readonly intent: Uint8Array;

  constructor(
    type: NameLike,
    instance: number,
    endpoint: string,
    init: { intent?: Uint8Array } = {},
  ) {
    this.type = toName(type);
    this.instance = instance;
    this.endpoint = endpoint;
    this.intent = init.intent ?? noBytes;
  }

  /**
   * Encodes the payload.
   *
   * @throws {TypeError} for an endpoint {@link checkEndpoint} refuses,
   * {@link RangeError} for an intent over 64 bytes or an instance outside u48.
   */
  encode(): Uint8Array {
    checkU48(this.instance, 'instance');
    const w = new ByteWriter(64 + this.intent.length);
    w.name(this.type);
    w.u48(this.instance);
    writeEndpoint(w, this.endpoint);
    writeIntent(w, this.intent);
    return w.toBytes();
  }

  /** Decodes the payload; trailing bytes are ignored. @throws {ProtocolError} on truncation, a refused endpoint, or an intent over 64 bytes. */
  static decode(bytes: Uint8Array): ConnectRequest {
    return guard('CONNECT request', () => {
      const r = new ByteReader(bytes);
      const type = r.name('type');
      const instance = r.u48('instance');
      const endpoint = readEndpoint(r);
      return new ConnectRequest(type, instance, endpoint, { intent: readIntent(r) });
    });
  }

  toString(): string {
    return (
      `ConnectRequest(${new ServiceAddress(this.type, this.instance)}, ${this.endpoint}, ` +
      `intent of ${this.intent.length} bytes)`
    );
  }
}

/**
 * `CONNECT` final response payload, from the naming service to the
 * consumer: the identity of the channel that registered the instance,
 * which the instance presents when it dials back. Wire: len8 identity.
 */
export class ConnectResponse {
  /** The identity the instance identifies with. */
  readonly identity: string;

  constructor(identity: string) {
    this.identity = identity;
  }

  /** Encodes the payload. @throws {RangeError} for an identity over 255 bytes of UTF-8. */
  encode(): Uint8Array {
    const w = new ByteWriter(1 + this.identity.length * 3);
    writeIdentity(w, this.identity, 'identity');
    return w.toBytes();
  }

  /** Decodes the payload; trailing bytes are ignored. @throws {ProtocolError} on truncation or malformed UTF-8. */
  static decode(bytes: Uint8Array): ConnectResponse {
    return guard(
      'CONNECT response',
      () => new ConnectResponse(readIdentity(new ByteReader(bytes), 'identity')),
    );
  }
}

/**
 * `CONNECT` request payload, from the naming service to an instance over
 * its registration channel: dial [endpoint], identify with [intent], and
 * name [requester] as the receiver of the `IDENT`. Wire: len8 requester
 * identity (empty when the consumer has none), len8 endpoint URI, len8
 * intent (at most 64 bytes).
 */
export class DialBackRequest {
  /** The identity of the consumer that asked, empty when it has none. */
  readonly requester: string;
  /** Where to dial. */
  readonly endpoint: string;
  /** The intent of the `IDENT` to send there. */
  readonly intent: Uint8Array;

  constructor(requester: string, endpoint: string, init: { intent?: Uint8Array } = {}) {
    this.requester = requester;
    this.endpoint = endpoint;
    this.intent = init.intent ?? noBytes;
  }

  /**
   * Encodes the payload.
   *
   * @throws {RangeError} for a requester over 255 bytes or an intent over 64
   * bytes, {@link TypeError} for an endpoint {@link checkEndpoint} refuses.
   */
  encode(): Uint8Array {
    const w = new ByteWriter(64 + this.requester.length * 3 + this.intent.length);
    writeIdentity(w, this.requester, 'requester');
    writeEndpoint(w, this.endpoint);
    writeIntent(w, this.intent);
    return w.toBytes();
  }

  /** Decodes the payload; trailing bytes are ignored. @throws {ProtocolError} on truncation, malformed UTF-8, a refused endpoint, or an intent over 64 bytes. */
  static decode(bytes: Uint8Array): DialBackRequest {
    return guard('CONNECT request', () => {
      const r = new ByteReader(bytes);
      const requester = readIdentity(r, 'requester');
      const endpoint = readEndpoint(r);
      return new DialBackRequest(requester, endpoint, { intent: readIntent(r) });
    });
  }

  toString(): string {
    return `DialBackRequest("${this.requester}", ${this.endpoint}, intent of ${this.intent.length} bytes)`;
  }
}
