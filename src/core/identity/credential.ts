/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * The credential codec (wiki page "Polyverse Switchboard Identity and
 * Credentials", section "Credential"). Ports `Credential` and the
 * `CredentialKind` and `SignatureKind` helpers of `credential.dart`. The
 * codec is synchronous and verifies nothing: {@link CredentialIssuer} signs,
 * {@link CredentialVerifier} checks.
 */

import { ByteFormatError, ByteReader, ByteWriter, bytesEqual } from '../bytes.ts';
import { type Name, type NameLike, toName } from '../name.ts';
import { ProtocolError } from '../status.ts';
import {
  type Credential as CredentialContract,
  CredentialKind,
  Right,
  type Scope as ScopeContract,
  SignatureKind,
} from './contract.ts';
import { rightFromCode, Scope } from './scope.ts';

const utf8Encoder = new TextEncoder();
// Strict, and a leading BOM is dropped, as Dart's `utf8.decode` does.
const strictUtf8 = new TextDecoder('utf-8', { fatal: true });

const kindNames = new Map<number, keyof typeof CredentialKind>(
  Object.entries(CredentialKind).map(([name, code]) => [code, name as keyof typeof CredentialKind]),
);

const signatureKindNames = new Map<number, keyof typeof SignatureKind>(
  Object.entries(SignatureKind).map(([name, code]) => [code, name as keyof typeof SignatureKind]),
);

const signatureLengths = new Map<number, number>([
  [SignatureKind.hmacSha256, 32],
  [SignatureKind.ed25519, 64],
]);

/** The name of credential kind [kind] (`node`, `client`, `device`), or undefined for another code. */
export function credentialKindName(kind: number): keyof typeof CredentialKind | undefined {
  return kindNames.get(kind);
}

/** The name of signature kind [kind] (`hmacSha256`, `ed25519`), or undefined for another code. */
export function signatureKindName(kind: number): keyof typeof SignatureKind | undefined {
  return signatureKindNames.get(kind);
}

/**
 * The length in bytes of a signature of [kind]: 32 for HMAC-SHA256, 64 for
 * Ed25519 (Dart `SignatureKind.signatureLength`).
 *
 * @throws {RangeError} for a code that is not one of {@link SignatureKind}.
 */
export function signatureLength(kind: SignatureKind): number {
  const length = signatureLengths.get(kind);
  if (length === undefined) {
    throw new RangeError(`unknown signature kind ${kind}`);
  }
  return length;
}

/** What a {@link Credential} is made of. */
export interface CredentialInit {
  /** What the credential identifies. */
  readonly kind: CredentialKind;
  /** Which issuer key signs it. */
  readonly keyId: NameLike;
  /** The holder's identity, at most 255 bytes of UTF-8. */
  readonly identity: string;
  /** When it was issued, in Unix seconds (`u32`). */
  readonly issuedAt: number;
  /** When it expires, in Unix seconds (`u32`); 0 means never. */
  readonly expiresAt: number;
  /** The holder's 32-byte Ed25519 public key; undefined for a bearer credential. */
  readonly holderKey?: Uint8Array | undefined;
  /** What the holder may do; default none. At most 255. */
  readonly scopes?: readonly ScopeContract[] | undefined;
  /** How it is signed. */
  readonly signatureKind: SignatureKind;
  /** The signature over {@link Credential.signedBytes}, of the length of `signatureKind`. Not checked. */
  readonly signature: Uint8Array;
}

/**
 * A signed statement of the mesh authority: who the holder is
 * ({@link identity}, {@link kind}) and what it may do ({@link scopes}),
 * until {@link expiresAt}.
 *
 * The binary layout is the one of the wiki page "Polyverse Switchboard
 * Identity and Credentials", section "Credential": every field in order,
 * then the signature over every preceding byte. A decoded credential keeps
 * its bytes, so that {@link encode} and {@link signedBytes} reproduce them
 * exactly, even when decoding dropped scopes with unknown rights.
 *
 * Implements the contract's `Credential`; the package exports this class
 * under that name.
 */
export class Credential implements CredentialContract {
  /** The layout version this implementation reads and writes. */
  static readonly currentVersion = 1;

  /**
   * Largest encoded credential: 637 bytes, so that an `IDENT` carrying it
   * with the largest intent (64 bytes), the longest receiver (255 bytes)
   * and a proof (64 bytes) fits the 1024-byte limit of a control payload
   * (`2 + 637 + 1 + 64 + 1 + 255 + 64`).
   */
  static readonly maxLength = 637;

  /** Length of a holder key (an Ed25519 public key). */
  static readonly holderKeyLength = 32;

  /** The layout version, {@link Credential.currentVersion}. */
  readonly version: number;

  /** What the credential identifies. */
  readonly kind: CredentialKind;

  /** Which issuer key signed it; the verifier looks the key up by this. */
  readonly keyId: Name;

  /** The holder's identity, such as `npc-7` or `worker-a1`. */
  readonly identity: string;

  /** When it was issued, in Unix seconds. Informational: receivers check only {@link expiresAt}. */
  readonly issuedAt: number;

  /** When it expires, in Unix seconds; 0 means never (discouraged). */
  readonly expiresAt: number;

  /** The holder's Ed25519 public key (32 bytes), or undefined for a bearer credential. Must not be modified. */
  readonly holderKey: Uint8Array | undefined;

  /** What the holder may do. Frozen. */
  readonly scopes: readonly Scope[];

  /** How it is signed. */
  readonly signatureKind: SignatureKind;

  /** The signature over {@link signedBytes}. Must not be modified. */
  readonly signature: Uint8Array;

  // The exact bytes a decoded credential came from (or the encoding, once
  // made), and for a decoded one where its signature starts.
  #encoded: Uint8Array | undefined;
  #signedLength: number | undefined;

  /**
   * A credential with the given fields; the signature is not checked. The
   * holder key and the signature are copied, the scopes made
   * {@link Scope}s.
   *
   * @throws {RangeError} for an unknown kind or signature kind, an identity
   *   over 255 bytes of UTF-8, a time outside `u32`, a holder key that is
   *   not 32 bytes, more than 255 scopes or a scope with an unknown right, a
   *   signature whose length is not the one of the signature kind, or an
   *   encoding longer than {@link Credential.maxLength}.
   */
  constructor(init: CredentialInit) {
    if (credentialKindName(init.kind) === undefined) {
      throw new RangeError(`unknown credential kind ${init.kind}`);
    }
    const length = signatureLength(init.signatureKind);
    this.version = Credential.currentVersion;
    this.kind = init.kind;
    this.keyId = toName(init.keyId);
    this.identity = init.identity;
    this.issuedAt = init.issuedAt;
    this.expiresAt = init.expiresAt;
    this.holderKey = init.holderKey?.slice();
    this.scopes = Object.freeze(
      (init.scopes ?? []).map((s) => (s instanceof Scope ? s : new Scope(s.right, s.pattern))),
    );
    this.signatureKind = init.signatureKind;
    this.signature = init.signature.slice();
    if (utf8Encoder.encode(this.identity).length > 0xff) {
      throw new RangeError('identity longer than 255 bytes of UTF-8');
    }
    for (const [name, value] of [
      ['issuedAt', this.issuedAt],
      ['expiresAt', this.expiresAt],
    ] as const) {
      if (!Number.isInteger(value) || value < 0 || value > 0xffffffff) {
        throw new RangeError(`${name} must fit in u32, got ${value}`);
      }
    }
    if (this.holderKey !== undefined && this.holderKey.length !== Credential.holderKeyLength) {
      throw new RangeError(
        `holderKey must be ${Credential.holderKeyLength} bytes, got ${this.holderKey.length}`,
      );
    }
    if (this.scopes.length > 0xff) {
      throw new RangeError(`at most 255 scopes, got ${this.scopes.length}`);
    }
    if (this.signature.length !== length) {
      throw new RangeError(
        `${signatureKindName(this.signatureKind)} signatures are ${length} bytes, ` +
          `got ${this.signature.length}`,
      );
    }
    const encoded = this.#encodeSigned().length + this.signature.length;
    if (encoded > Credential.maxLength) {
      throw new RangeError(
        `credential of ${encoded} bytes exceeds the maximum of ${Credential.maxLength}`,
      );
    }
  }

  /** Whether the credential is a bearer credential: whoever has the bytes is the holder. False when it names a {@link holderKey}. */
  get isBearer(): boolean {
    return this.holderKey === undefined;
  }

  /** {@link issuedAt} as a time. */
  get issuedAtTime(): Date {
    return new Date(this.issuedAt * 1000);
  }

  /** {@link expiresAt} as a time, or undefined for a credential that never expires. */
  get expiresAtTime(): Date | undefined {
    return this.expiresAt === 0 ? undefined : new Date(this.expiresAt * 1000);
  }

  /**
   * Whether the credential has expired at [now] (milliseconds since the
   * epoch; default `Date.now()`): {@link expiresAt} is not 0 and [now], in
   * whole seconds, is at or after it.
   */
  isExpired(now: number = Date.now()): boolean {
    return this.expiresAt !== 0 && Math.floor(now / 1000) >= this.expiresAt;
  }

  /** Whether a scope grants [right] for [name]. A scope with `Right.admin` grants everything. */
  allows(right: Right, name: NameLike): boolean {
    const n = toName(name);
    for (const scope of this.scopes) {
      if (scope.right === Right.admin || (scope.right === right && scope.matches(n))) {
        return true;
      }
    }
    return false;
  }

  /** The bytes the signature covers: every byte of the encoding before the signature, the signature kind included. A copy. */
  get signedBytes(): Uint8Array {
    const encoded = this.#encoded;
    const signedLength = this.#signedLength;
    if (encoded !== undefined && signedLength !== undefined) {
      return encoded.slice(0, signedLength);
    }
    return this.#encodeSigned();
  }

  /** The encoding: {@link signedBytes} followed by the {@link signature}. A decoded credential encodes to exactly the bytes it was decoded from. A copy. */
  encode(): Uint8Array {
    let encoded = this.#encoded;
    if (encoded === undefined) {
      const signed = this.#encodeSigned();
      encoded = new Uint8Array(signed.length + this.signature.length);
      encoded.set(signed);
      encoded.set(this.signature, signed.length);
      this.#encoded = encoded;
    }
    return encoded.slice();
  }

  #encodeSigned(): Uint8Array {
    const holder = this.holderKey;
    const w = new ByteWriter(64 + this.scopes.length * Scope.encodedLength)
      .u8(this.version)
      .u8(this.kind)
      .name(this.keyId)
      .string8(this.identity)
      .u32(this.issuedAt)
      .u32(this.expiresAt)
      .u8(holder?.length ?? 0);
    if (holder !== undefined) {
      w.bytes(holder);
    }
    w.u8(this.scopes.length);
    for (const scope of this.scopes) {
      w.u8(scope.right).name(scope.pattern);
    }
    w.u8(this.signatureKind);
    return w.toBytes();
  }

  /**
   * Decodes a credential. Does not verify the signature.
   *
   * Scopes with a right this implementation does not know are dropped (they
   * could only grant more); {@link encode} still returns [bytes] exactly.
   * [bytes] is copied.
   *
   * @throws {ProtocolError} for an encoding longer than
   *   {@link Credential.maxLength}, a version other than
   *   {@link Credential.currentVersion}, an unknown kind or signature kind,
   *   an identity that is not UTF-8, an invalid name (key id or pattern), a
   *   holder key length other than 0 or 32, truncation, and bytes after the
   *   signature.
   */
  static decode(bytes: Uint8Array): Credential {
    if (bytes.length > Credential.maxLength) {
      throw new ProtocolError(
        `credential of ${bytes.length} bytes exceeds ${Credential.maxLength}`,
      );
    }
    const copy = bytes.slice();
    const r = new ByteReader(copy);
    let init: CredentialInit;
    let signedLength: number;
    try {
      const version = r.u8('version');
      if (version !== Credential.currentVersion) {
        throw new ProtocolError(`unsupported credential version ${version}`);
      }
      const kind = r.u8('kind');
      if (credentialKindName(kind) === undefined) {
        throw new ProtocolError(`unknown credential kind ${kind}`);
      }
      const keyId = r.name('key id');
      const identityLength = r.u8('identity length');
      const identity = strictUtf8.decode(r.take(identityLength, 'identity'));
      const issuedAt = r.u32('issued at');
      const expiresAt = r.u32('expires at');
      const holderLength = r.u8('holder key length');
      if (holderLength !== 0 && holderLength !== Credential.holderKeyLength) {
        throw new ProtocolError(`holder key of ${holderLength} bytes`);
      }
      const holderKey = holderLength === 0 ? undefined : r.take(holderLength, 'holder key');
      const count = r.u8('scope count');
      const scopes: Scope[] = [];
      for (let i = 0; i < count; i++) {
        const right = rightFromCode(r.u8('right'));
        const pattern = r.name('pattern');
        if (right !== undefined) {
          scopes.push(new Scope(right, pattern));
        }
      }
      const signatureKind = r.u8('signature kind');
      if (signatureKindName(signatureKind) === undefined) {
        throw new ProtocolError(`unknown signature kind ${signatureKind}`);
      }
      signedLength = r.offset;
      const signature = r.take(signatureLength(signatureKind as SignatureKind), 'signature');
      if (!r.isDone) {
        throw new ProtocolError(`${r.remaining} bytes after the signature`);
      }
      init = {
        kind: kind as CredentialKind,
        keyId,
        identity,
        issuedAt,
        expiresAt,
        holderKey,
        scopes,
        signatureKind: signatureKind as SignatureKind,
        signature,
      };
    } catch (e) {
      if (e instanceof ProtocolError) {
        throw e;
      }
      // Truncation (ByteFormatError), an invalid name (RangeError), an
      // identity that is not UTF-8 (TypeError).
      const reason = e instanceof TypeError ? 'identity is not UTF-8' : (e as Error).message;
      if (e instanceof ByteFormatError || e instanceof RangeError || e instanceof TypeError) {
        throw new ProtocolError(`malformed credential: ${reason}`, { cause: e });
      }
      throw e;
    }
    // Every decoded field passes the constructor's checks: the scopes kept
    // are no more than were read, the identity re-encodes to the same bytes.
    const credential = new Credential(init);
    credential.#encoded = copy;
    credential.#signedLength = signedLength;
    return credential;
  }

  /** Equal when [other] is a credential whose encoding is equal. */
  equals(other: unknown): boolean {
    return other instanceof Credential && bytesEqual(this.encode(), other.encode());
  }

  /** The kind, identity, key id, expiry and scopes, for logs. Never the signature. */
  toString(): string {
    const expiry = this.expiresAtTime;
    return (
      `Credential(${credentialKindName(this.kind)} "${this.identity}", key ${this.keyId}, ` +
      `${expiry === undefined ? 'never expires' : `expires ${expiry.toISOString()}`}` +
      `${this.isBearer ? ', bearer' : ''}` +
      `${this.scopes.length === 0 ? '' : `, ${this.scopes.join(', ')}`})`
    );
  }
}
