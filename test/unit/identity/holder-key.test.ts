// What the WebCrypto port adds to the Dart identity API, with no Dart
// counterpart: holder keys and issuers over WebCrypto key pairs (a
// non-extractable private key), the verifier's key tables, wall time and
// logger, and what the classes print.
import { describe, expect, test } from 'vitest';
import { hexBytes, hexString } from '../../../src/core/bytes.ts';
import { CredentialKind, Right } from '../../../src/core/identity/contract.ts';
import { CredentialIssuer } from '../../../src/core/identity/credential-issuer.ts';
import { CredentialVerifier } from '../../../src/core/identity/credential-verifier.ts';
import { ed25519Verify } from '../../../src/core/identity/ed25519.ts';
import { Ed25519HolderKey } from '../../../src/core/identity/holder-key.ts';
import { Scope } from '../../../src/core/identity/scope.ts';
import { Name } from '../../../src/core/name.ts';
import { StatusCode, SwitchboardError } from '../../../src/core/status.ts';
import type { Logger, LogLevel } from '../../../src/core/util/logger.ts';
import { holderPublic, holderSeed } from '../../support/identity/identity-vectors.ts';

/** A WebCrypto Ed25519 key pair whose private key cannot be exported. */
async function generated(): Promise<CryptoKeyPair> {
  return (await crypto.subtle.generateKey('Ed25519', false, ['sign', 'verify'])) as CryptoKeyPair;
}

describe('Ed25519HolderKey', () => {
  test('fromSeed gives the RFC 8032 public key; generate a fresh one', async () => {
    const fixed = await Ed25519HolderKey.fromSeed(holderSeed);
    expect(hexString(fixed.publicKey)).toBe(holderPublic);
    const a = await Ed25519HolderKey.generate();
    const b = await Ed25519HolderKey.generate();
    expect(a.publicKey).toHaveLength(32);
    expect(hexString(a.publicKey)).not.toBe(hexString(b.publicKey));
    const message = hexBytes('01 02 03');
    expect(await ed25519Verify(a.publicKey, message, await a.sign(message))).toBe(true);
    await expect(Ed25519HolderKey.fromSeed(new Uint8Array(31))).rejects.toThrow(RangeError);
  });

  test('fromKeyPair wraps a non-extractable WebCrypto key pair', async () => {
    const pair = await generated();
    expect(pair.privateKey.extractable).toBe(false);
    const key = await Ed25519HolderKey.fromKeyPair(pair);
    const raw = new Uint8Array(await crypto.subtle.exportKey('raw', pair.publicKey));
    expect(key.publicKey).toEqual(raw);
    const message = hexBytes('AA BB');
    expect(await ed25519Verify(key.publicKey, message, await key.sign(message))).toBe(true);
  });

  test('fromKeyPair refuses another algorithm, or a key that cannot sign', async () => {
    const ecdsa = (await crypto.subtle.generateKey({ name: 'ECDSA', namedCurve: 'P-256' }, false, [
      'sign',
      'verify',
    ])) as CryptoKeyPair;
    await expect(Ed25519HolderKey.fromKeyPair(ecdsa)).rejects.toThrow(TypeError);
    const pair = await generated();
    // The public key in both places: it cannot sign.
    await expect(
      Ed25519HolderKey.fromKeyPair({ privateKey: pair.publicKey, publicKey: pair.publicKey }),
    ).rejects.toThrow(TypeError);
  });
});

describe('CredentialIssuer over WebCrypto', () => {
  test('an issuer over a generated key pair issues what its public key verifies', async () => {
    const issuer = CredentialIssuer.ed25519('gen1', await generated());
    const holder = await Ed25519HolderKey.generate();
    const credential = await issuer.issue({
      kind: CredentialKind.device,
      identity: 'sensor-3',
      scopes: [Scope.of(Right.open, 'telemetr')],
      holderKey: holder.publicKey,
    });
    const verifier = new CredentialVerifier({ ed25519Keys: { gen1: await issuer.publicKey() } });
    const verified = await verifier.verify(credential.encode());
    expect(verified.identity).toBe('sensor-3');
    expect(verified.kind).toBe(CredentialKind.device);
    expect(verified.holderKey).toEqual(holder.publicKey);
  });

  test('the factories refuse what they cannot sign with', async () => {
    const pair = await generated();
    expect(() =>
      CredentialIssuer.ed25519('k', { privateKey: pair.publicKey, publicKey: pair.publicKey }),
    ).toThrow(TypeError);
    await expect(CredentialIssuer.ed25519FromSeed('k', new Uint8Array(33))).rejects.toThrow(
      RangeError,
    );
    expect(() => CredentialIssuer.hmac('too-long-id', new Uint8Array(32))).toThrow(RangeError);
  });

  test('the issue time defaults to the wall time, rounded down to the second', async () => {
    const issuer = CredentialIssuer.hmac('k', new Uint8Array(16), {
      wallTime: () => 1_767_225_600_999,
    });
    const credential = await issuer.issue({ kind: CredentialKind.client, identity: 'x' });
    expect(credential.issuedAt).toBe(1_767_225_600);
    expect(credential.expiresAt).toBe(1_767_225_600 + 8 * 3600);
    await expect(
      issuer.issue({ kind: CredentialKind.client, identity: 'x', lifetime: Number.NaN }),
    ).rejects.toThrow(RangeError);
  });

  test('toString names the kind and the key id, never the key', () => {
    const key = Uint8Array.from({ length: 16 }, (_, i) => 0xa0 + i);
    const issuer = CredentialIssuer.hmac('mesh1', key);
    expect(`${issuer}`).toBe('CredentialIssuer(hmacSha256, key mesh1)');
  });
});

describe('CredentialVerifier options', () => {
  const key = Uint8Array.from({ length: 32 }, (_, i) => i);

  test('key tables: a Map with names or text, or a plain object', async () => {
    const issuer = CredentialIssuer.hmac('k1', key);
    const credential = await issuer.issue({ kind: CredentialKind.node, identity: 'x' });
    for (const hmacKeys of [
      new Map([[Name.of('k1'), key]]),
      new Map([['k1', key]]),
      [['k1', key] as const],
      { k1: key },
    ]) {
      const verifier = new CredentialVerifier({ hmacKeys });
      expect(verifier.keyIds.map(String)).toEqual(['k1']);
      expect((await verifier.verify(credential.encode())).identity).toBe('x');
    }
    expect(() => new CredentialVerifier({ cacheSize: -1 })).toThrow(RangeError);
    expect(() => new CredentialVerifier({ cacheSize: 1.5 })).toThrow(RangeError);
  });

  test('expiry at the wall time of the options when a call gives none', async () => {
    let wall = Date.UTC(2026, 0, 1);
    const issuer = CredentialIssuer.hmac('k1', key, { wallTime: () => wall });
    const verifier = new CredentialVerifier({ hmacKeys: { k1: key }, wallTime: () => wall });
    const credential = await issuer.issue({
      kind: CredentialKind.node,
      identity: 'x',
      lifetime: 60_000,
    });
    await verifier.verify(credential.encode());
    expect(verifier.cached(credential.encode())).toBeDefined();
    wall += 60_000;
    expect(verifier.cached(credential.encode())).toBeUndefined();
    await expect(verifier.verify(credential.encode())).rejects.toThrow(SwitchboardError);
    // An explicit time wins.
    expect((await verifier.verify(credential.encode(), { now: wall - 1000 })).identity).toBe('x');
  });

  test('the cause of a refusal goes to the logger, the error stays generic', async () => {
    const lines: [LogLevel, string, string][] = [];
    const logger: Logger = {
      log: (level, name, message) => {
        lines.push([level, name, message]);
      },
    };
    const verifier = new CredentialVerifier({ hmacKeys: { k1: key }, logger });
    const other = await CredentialIssuer.hmac('k2', key).issue({
      kind: CredentialKind.node,
      identity: 'npc-7',
    });
    const error = await verifier.verify(other.encode()).then(
      () => undefined,
      (e: unknown) => e as SwitchboardError,
    );
    expect(error?.code).toBe(StatusCode.unauthenticated);
    expect(error?.status.reason).toBe('invalid credential');
    expect(lines).toEqual([
      ['debug', 'Switchboard.Identity', 'credential of "npc-7": unknown key id k2'],
    ]);
    await verifier.verify(new Uint8Array(3)).catch(() => {});
    expect(lines[1]?.[2]).toMatch(/^malformed credential: /);
    expect(`${verifier}`).toBe('CredentialVerifier(keys k1, 0 cached)');
  });
});
