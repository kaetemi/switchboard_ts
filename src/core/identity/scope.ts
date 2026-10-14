/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * Scopes: a right over the service types a name pattern matches (wiki page
 * "Polyverse Switchboard Identity and Credentials", section "Scopes").
 * Ports `Scope` and the `Right` helpers of `credential.dart`; the right
 * codes are {@link Right} of the identity contract.
 */

import { Name, type NameLike, toName } from '../name.ts';
import { Right, type Scope as ScopeContract } from './contract.ts';

const rightNames = new Map<number, keyof typeof Right>(
  Object.entries(Right).map(([name, code]) => [code, name as keyof typeof Right]),
);

/**
 * The name of [right] as the wiki and Dart's `Right` enum spell it
 * (`register`, `open`, ...), or undefined for a code this implementation
 * does not know.
 */
export function rightName(right: number): keyof typeof Right | undefined {
  return rightNames.get(right);
}

/**
 * The right of wire code [code], or undefined for a code this
 * implementation does not know (Dart `Right.fromCode`): a scope with such a
 * right is dropped when a credential is decoded.
 */
export function rightFromCode(code: number): Right | undefined {
  return rightNames.has(code) ? (code as Right) : undefined;
}

/**
 * A right over the service types a name pattern matches.
 *
 * A pattern is a name. A `*` as its last non-zero byte matches any suffix,
 * the empty one included: `worker-*` matches `worker-a1` and `worker-` but
 * not `worker`; `*` alone matches every name, the empty name too. Any other
 * pattern matches exactly that name. A `*` anywhere else is an ordinary
 * byte: `n*c` matches only `n*c`.
 */
export class Scope implements ScopeContract {
  /** Encoded size: the right byte and the 8-byte pattern. */
  static readonly encodedLength = 1 + Name.byteLength;

  /** The wildcard byte, `*`. */
  static readonly wildcard = 0x2a;

  /** The right granted. */
  readonly right: Right;

  /** The name pattern. */
  readonly pattern: Name;

  /**
   * A scope granting [right] for the names [pattern] matches; a pattern
   * given as text is {@link Name.of} it.
   *
   * @throws {RangeError} for a right that is not one of {@link Right}, and
   *   for a pattern text {@link Name.of} refuses.
   */
  constructor(right: Right, pattern: NameLike) {
    if (rightFromCode(right) === undefined) {
      throw new RangeError(`unknown right ${right}`);
    }
    this.right = right;
    this.pattern = toName(pattern);
  }

  /**
   * A scope granting [right] for [pattern], such as
   * `Scope.of(Right.register, 'worker-*')` (Dart `Scope.of`).
   *
   * @throws {RangeError} like the constructor.
   */
  static of(right: Right, pattern: NameLike): Scope {
    return new Scope(right, pattern);
  }

  /** Whether {@link pattern} matches [name] (see {@link Scope}). */
  matches(name: NameLike): boolean {
    return Scope.patternMatches(this.pattern, toName(name));
  }

  /** Whether the name pattern [pattern] matches [name] (see {@link Scope}). */
  static patternMatches(pattern: Name, name: Name): boolean {
    const p = pattern.bytes;
    const n = pattern.length;
    if (n > 0 && p[n - 1] === Scope.wildcard) {
      const q = name.bytes;
      for (let i = 0; i < n - 1; i++) {
        if (q[i] !== p[i]) {
          return false;
        }
      }
      return true;
    }
    return pattern.equals(name);
  }

  /** Equal when [other] is a scope with the same right and pattern. */
  equals(other: unknown): boolean {
    return (
      other instanceof Scope && other.right === this.right && other.pattern.equals(this.pattern)
    );
  }

  /** The right and the pattern, as in `register worker-*`. */
  toString(): string {
    return `${rightName(this.right) ?? `right ${this.right}`} ${this.pattern}`;
  }
}
