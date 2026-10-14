// Credentials and scope patterns: the "Credentials" section of the wiki
// page "Polyverse Switchboard Test Vectors" (C1, C2, the invalid
// credentials table, the scope patterns). Ports the "credential layout" and
// "scope patterns" groups of `test/vectors/identity_vectors_test.dart`; its
// "connection identity control messages" group is `identity.test.ts`.
import { describe, expect, test } from 'vitest';
import { concatBytes, hexBytes, hexString } from '../../src/core/bytes.ts';
import { CredentialKind, Right, SignatureKind } from '../../src/core/identity/contract.ts';
import { Credential } from '../../src/core/identity/credential.ts';
import { CredentialIssuer, hmacSha256 } from '../../src/core/identity/credential-issuer.ts';
import { CredentialVerifier } from '../../src/core/identity/credential-verifier.ts';
import { Ed25519HolderKey } from '../../src/core/identity/holder-key.ts';
import { Scope } from '../../src/core/identity/scope.ts';
import { Name } from '../../src/core/name.ts';
import { ProtocolError, StatusCode, SwitchboardError } from '../../src/core/status.ts';
import {
  at,
  authorityPublic,
  authoritySeed,
  ed25519Credential,
  expiresAt,
  hmacCredential,
  holderPublic,
  holderSeed,
  issuedAt,
} from '../support/identity/identity-vectors.ts';
import { wikiBlob, wikiBytes, wikiVariants, wikiVector } from '../support/vectors.ts';

const hmacKey = Uint8Array.from({ length: 32 }, (_, i) => i);

async function expectCode(promise: Promise<unknown>, code: StatusCode): Promise<void> {
  const error = await promise.then(
    () => undefined,
    (e: unknown) => e,
  );
  expect(error).toBeInstanceOf(SwitchboardError);
  expect((error as SwitchboardError).code).toBe(code);
}

/** The Ed25519 key pair of [seed] with [publicKey], as WebCrypto keys. */
async function cryptoKeyPair(seed: Uint8Array, publicKey: Uint8Array): Promise<CryptoKeyPair> {
  const pkcs8 = concatBytes(hexBytes('30 2E 02 01 00 30 05 06 03 2B 65 70 04 22 04 20'), seed);
  return {
    privateKey: await crypto.subtle.importKey(
      'pkcs8',
      pkcs8 as Uint8Array<ArrayBuffer>,
      'Ed25519',
      false,
      ['sign'],
    ),
    publicKey: await crypto.subtle.importKey(
      'raw',
      publicKey as Uint8Array<ArrayBuffer>,
      'Ed25519',
      true,
      ['verify'],
    ),
  };
}

describe('credential layout', () => {
  test('the wiki blobs are the vectors of the Dart test', () => {
    expect(hexString(wikiBlob('C1'))).toBe(hmacCredential);
    expect(hexString(wikiBlob('C2'))).toBe(ed25519Credential);
    expect(wikiBlob('C2.authority-seed')).toEqual(authoritySeed);
    expect(hexString(wikiBlob('C2.authority-key'))).toBe(authorityPublic);
    expect(wikiBlob('C2.holder-seed')).toEqual(holderSeed);
    expect(hexString(wikiBlob('C2.holder-key'))).toBe(holderPublic);
  });

  test('HMAC-SHA256 credential with a fixed key', async () => {
    const issuer = CredentialIssuer.hmac(Name.of('k1'), hmacKey);
    const issued = await issuer.issue({
      kind: CredentialKind.node,
      identity: 'npc-7',
      scopes: [
        Scope.of(Right.register, 'npc'),
        Scope.of(Right.open, '*'),
        Scope.of(Right.watch, 'npc-*'),
      ],
      issuedAt: issuedAt * 1000,
      lifetime: 8 * 3600 * 1000,
    });
    expect(hexString(issued.encode())).toBe(hmacCredential);

    const decoded = Credential.decode(hexBytes(hmacCredential));
    expect(decoded.version).toBe(1);
    expect(decoded.kind).toBe(CredentialKind.node);
    expect(decoded.keyId.equals(Name.of('k1'))).toBe(true);
    expect(decoded.identity).toBe('npc-7');
    expect(decoded.issuedAt).toBe(issuedAt);
    expect(decoded.expiresAt).toBe(expiresAt);
    expect(decoded.holderKey).toBeUndefined();
    expect(decoded.isBearer).toBe(true);
    expect(decoded.scopes).toEqual([
      Scope.of(Right.register, 'npc'),
      Scope.of(Right.open, '*'),
      Scope.of(Right.watch, 'npc-*'),
    ]);
    expect(decoded.scopes.map((s) => s.right)).toEqual([1, 2, 3]);
    expect(decoded.signatureKind).toBe(SignatureKind.hmacSha256);
    expect(decoded.equals(issued)).toBe(true);
    expect(hexString(decoded.encode())).toBe(hmacCredential);
    expect(hexString(decoded.signedBytes)).toBe(hmacCredential.substring(0, 54 * 3 - 1));
    expect(decoded.issuedAtTime.toISOString()).toBe('2026-01-01T00:00:00.000Z');
    expect(decoded.expiresAtTime?.toISOString()).toBe('2026-01-01T08:00:00.000Z');

    const verifier = new CredentialVerifier({ hmacKeys: new Map([[Name.of('k1'), hmacKey]]) });
    expect((await verifier.verify(hexBytes(hmacCredential), { now: at })).equals(decoded)).toBe(
      true,
    );
  });

  test('Ed25519 credential with a holder key, from fixed seeds', async () => {
    const authority = await cryptoKeyPair(authoritySeed, hexBytes(authorityPublic));
    const issuer = CredentialIssuer.ed25519(Name.of('auth1'), authority);
    expect(hexString(await issuer.publicKey())).toBe(authorityPublic);
    const fromSeed = await CredentialIssuer.ed25519FromSeed(Name.of('auth1'), authoritySeed);
    expect(hexString(await fromSeed.publicKey())).toBe(authorityPublic);
    const holder = await Ed25519HolderKey.fromSeed(holderSeed);
    expect(hexString(holder.publicKey)).toBe(holderPublic);
    // No lifetime: the default of 8 hours.
    expect(CredentialIssuer.defaultLifetime).toBe(8 * 3600 * 1000);
    for (const signer of [issuer, fromSeed]) {
      const issued = await signer.issue({
        kind: CredentialKind.node,
        identity: 'worker-a1',
        scopes: [Scope.of(Right.register, 'worker-*')],
        holderKey: holder.publicKey,
        issuedAt: issuedAt * 1000,
      });
      expect(hexString(issued.encode())).toBe(ed25519Credential);
    }

    const decoded = Credential.decode(hexBytes(ed25519Credential));
    expect(decoded.keyId.equals(Name.of('auth1'))).toBe(true);
    expect(decoded.identity).toBe('worker-a1');
    expect(hexString(decoded.holderKey ?? new Uint8Array(0))).toBe(holderPublic);
    expect(decoded.isBearer).toBe(false);
    expect(decoded.scopes).toEqual([Scope.of(Right.register, 'worker-*')]);
    expect(decoded.signatureKind).toBe(SignatureKind.ed25519);

    const verifier = new CredentialVerifier({ ed25519Keys: { auth1: hexBytes(authorityPublic) } });
    expect((await verifier.verify(hexBytes(ed25519Credential), { now: at })).equals(decoded)).toBe(
      true,
    );
  });

  test('a scope with an unknown right is ignored, the bytes kept', () => {
    // The HMAC vector with the third scope's right changed to 0x08.
    const bytes = hexBytes(hmacCredential);
    bytes[44] = 0x08;
    const decoded = Credential.decode(bytes);
    expect(decoded.scopes).toEqual([Scope.of(Right.register, 'npc'), Scope.of(Right.open, '*')]);
    expect(decoded.encode()).toEqual(bytes);
    expect(hexString(decoded.signedBytes)).toBe(hexString(bytes.subarray(0, 54)));
  });

  describe('negative (malformed)', () => {
    function negative(name: string, bytes: () => Uint8Array): void {
      test(name, async () => {
        expect(() => Credential.decode(bytes())).toThrow(ProtocolError);
        const verifier = new CredentialVerifier({ hmacKeys: { k1: hmacKey } });
        await expectCode(verifier.verify(bytes(), { now: at }), StatusCode.unauthenticated);
      });
    }

    function patched(offset: number, value: number): Uint8Array {
      const bytes = hexBytes(hmacCredential);
      bytes[offset] = value;
      return bytes;
    }

    test('the wiki rows are the patched vectors', () => {
      const c1 = hexBytes(hmacCredential);
      expect(wikiBytes('test-vectors/credentials/version-2')).toEqual(patched(0, 0x02));
      expect(wikiVariants('test-vectors/credentials/kind-0-kind-4')).toEqual([
        patched(1, 0x00),
        patched(1, 0x04),
      ]);
      expect(wikiBytes('test-vectors/credentials/key-id-with-a-byte-after-a-zero')).toEqual(
        patched(5, 0x62),
      );
      expect(wikiBytes('test-vectors/credentials/identity-not-utf-8')).toEqual(patched(11, 0xff));
      expect(wikiBytes('test-vectors/credentials/holder-key-length-5')).toEqual(patched(24, 0x05));
      expect(wikiBytes('test-vectors/credentials/signature-kind-3')).toEqual(patched(53, 0x03));
      expect(
        wikiBytes('test-vectors/credentials/signature-kind-2-with-a-32-byte-signature'),
      ).toEqual(patched(53, 0x02));
      expect(wikiBytes('test-vectors/credentials/truncated')).toEqual(
        c1.subarray(0, c1.length - 1),
      );
      expect(wikiBytes('test-vectors/credentials/trailing-byte')).toEqual(concatBytes(c1, [0]));
      expect(wikiVector('test-vectors/credentials/longer-than-637-bytes').text).toBe(
        'any 638 bytes',
      );
    });

    negative('version 2', () => wikiBytes('test-vectors/credentials/version-2'));
    negative(
      'kind 0',
      () => wikiVariants('test-vectors/credentials/kind-0-kind-4')[0] as Uint8Array,
    );
    negative(
      'kind 4',
      () => wikiVariants('test-vectors/credentials/kind-0-kind-4')[1] as Uint8Array,
    );
    negative('key id with a byte after a zero', () =>
      wikiBytes('test-vectors/credentials/key-id-with-a-byte-after-a-zero'),
    );
    negative('identity that is not UTF-8', () =>
      wikiBytes('test-vectors/credentials/identity-not-utf-8'),
    );
    negative('holder key length 5', () =>
      wikiBytes('test-vectors/credentials/holder-key-length-5'),
    );
    negative('signature kind 3', () => wikiBytes('test-vectors/credentials/signature-kind-3'));
    negative('signature kind 2 with a 32-byte signature', () =>
      wikiBytes('test-vectors/credentials/signature-kind-2-with-a-32-byte-signature'),
    );
    negative('truncated signature', () => wikiBytes('test-vectors/credentials/truncated'));
    negative('a byte after the signature', () =>
      wikiBytes('test-vectors/credentials/trailing-byte'),
    );
    negative('empty', () => new Uint8Array(0));
    negative('longer than 637 bytes', () => new Uint8Array(638));
  });

  describe('negative (signature)', () => {
    test('a flipped signature bit', async () => {
      const verifier = new CredentialVerifier({ hmacKeys: { k1: hmacKey } });
      const bytes = hexBytes(hmacCredential);
      bytes[bytes.length - 1] = (bytes[bytes.length - 1] as number) ^ 0x01;
      await expectCode(verifier.verify(bytes, { now: at }), StatusCode.unauthenticated);
    });

    test('a flipped bit in a signed field', async () => {
      const verifier = new CredentialVerifier({
        ed25519Keys: { auth1: hexBytes(authorityPublic) },
      });
      const bytes = hexBytes(ed25519Credential);
      // The last byte of the scope pattern: worker-* becomes worker-+.
      bytes[70] = (bytes[70] as number) ^ 0x01;
      const scopes = Credential.decode(bytes).scopes;
      expect(scopes).toHaveLength(1);
      expect(scopes[0]?.pattern.equals(Name.of('worker-+'))).toBe(true);
      await expectCode(verifier.verify(bytes, { now: at }), StatusCode.unauthenticated);
    });

    test('an HMAC credential that names a holder key', async () => {
      const fields = {
        kind: CredentialKind.node,
        keyId: Name.of('k1'),
        identity: 'x',
        issuedAt,
        expiresAt,
        holderKey: hexBytes(holderPublic),
        signatureKind: SignatureKind.hmacSha256,
      };
      const draft = new Credential({ ...fields, signature: new Uint8Array(32) });
      const signed = new Credential({
        ...fields,
        signature: await hmacSha256(hmacKey, draft.signedBytes),
      });
      const verifier = new CredentialVerifier({ hmacKeys: { k1: hmacKey } });
      await expectCode(verifier.verify(signed.encode(), { now: at }), StatusCode.unauthenticated);
    });
  });
});

describe('scope patterns', () => {
  for (const [pattern, name, matches] of [
    ['*', 'anything', true],
    ['*', '', true],
    ['*', '_ns', true],
    ['npc', 'npc', true],
    ['npc', 'npc-1', false],
    ['npc', 'np', false],
    ['npc-*', 'npc-1', true],
    ['npc-*', 'npc-', true],
    ['npc-*', 'npc', false],
    ['npc-*', 'npcx', false],
    ['n*c', 'n*c', true],
    ['n*c', 'nxc', false],
    ['abcdefg*', 'abcdefgh', true],
    ['abcdefg*', 'abcdefg', true],
    ['abcdefgh', 'abcdefgh', true],
    ['', '', true],
    ['', 'a', false],
  ] as const) {
    test(`"${pattern}" ${matches ? 'matches' : 'does not match'} "${name}"`, () => {
      expect(Scope.of(Right.open, pattern).matches(Name.of(name))).toBe(matches);
    });
  }

  test('admin allows everything', () => {
    const credential = new Credential({
      kind: CredentialKind.node,
      keyId: Name.of('k'),
      identity: 'root',
      issuedAt: 0,
      expiresAt: 0,
      scopes: [Scope.of(Right.admin, 'nothing')],
      signatureKind: SignatureKind.hmacSha256,
      signature: new Uint8Array(32),
    });
    expect(credential.allows(Right.open, Name.of('x'))).toBe(true);
    expect(credential.allows(Right.broker, Name.of('_ns'))).toBe(true);
    // The right codes of the wiki.
    expect(Right).toEqual({
      register: 1,
      open: 2,
      watch: 3,
      claim: 4,
      migrate: 5,
      broker: 6,
      admin: 7,
    });
  });
});

// Not in the Dart file: what the TS constructor and helpers refuse or
// print, where Dart's types already rule the input out.
describe('credential codec (TypeScript)', () => {
  const fields = {
    kind: CredentialKind.node,
    keyId: 'k',
    identity: 'x',
    issuedAt: 0,
    expiresAt: 0,
    signatureKind: SignatureKind.hmacSha256,
    signature: new Uint8Array(32),
  } as const;

  test('the constructor refuses codes and lengths the layout cannot carry', () => {
    expect(() => new Credential({ ...fields, kind: 4 as CredentialKind })).toThrow(RangeError);
    expect(() => new Credential({ ...fields, signatureKind: 3 as SignatureKind })).toThrow(
      RangeError,
    );
    expect(() => new Credential({ ...fields, signature: new Uint8Array(64) })).toThrow(RangeError);
    expect(() => new Credential({ ...fields, issuedAt: 2 ** 32 })).toThrow(RangeError);
    expect(() => new Credential({ ...fields, expiresAt: -1 })).toThrow(RangeError);
    expect(() => new Credential({ ...fields, expiresAt: 1.5 })).toThrow(RangeError);
    expect(() => new Credential({ ...fields, holderKey: new Uint8Array(31) })).toThrow(RangeError);
    expect(() => new Credential({ ...fields, identity: 'é'.repeat(128) })).toThrow(RangeError);
    expect(() => new Scope(8 as Right, 'x')).toThrow(RangeError);
  });

  test('a decoded credential owns its bytes', () => {
    const bytes = hexBytes(hmacCredential);
    const decoded = Credential.decode(bytes);
    bytes.fill(0);
    expect(hexString(decoded.encode())).toBe(hmacCredential);
    const copy = decoded.encode();
    copy.fill(0);
    expect(hexString(decoded.encode())).toBe(hmacCredential);
  });

  test('expiry is at the second, and never for 0', () => {
    const credential = Credential.decode(hexBytes(hmacCredential));
    expect(credential.isExpired(expiresAt * 1000 - 1)).toBe(false);
    expect(credential.isExpired(expiresAt * 1000)).toBe(true);
    const forever = new Credential(fields);
    expect(forever.isExpired(2 ** 40)).toBe(false);
    expect(forever.expiresAtTime).toBeUndefined();
  });

  test('toString names the fields, never the signature', () => {
    expect(`${Credential.decode(hexBytes(hmacCredential))}`).toBe(
      'Credential(node "npc-7", key k1, expires 2026-01-01T08:00:00.000Z, bearer, ' +
        'register npc, open *, watch npc-*)',
    );
    expect(`${Credential.decode(hexBytes(ed25519Credential))}`).toBe(
      'Credential(node "worker-a1", key auth1, expires 2026-01-01T08:00:00.000Z, register worker-*)',
    );
  });
});
