// Ed25519 over WebCrypto against RFC 8032, section 7.1, tests 1 to 3. No
// Dart counterpart: Dart uses the pure Dart implementation of
// `package:cryptography`, which its own package tests.
import { describe, expect, test } from 'vitest';
import { hexBytes, hexString } from '../../../src/core/bytes.ts';
import {
  constantTimeEquals,
  ed25519KeyPairFromSeed,
  ed25519Sign,
  ed25519Verify,
} from '../../../src/core/identity/ed25519.ts';

const rfc8032 = [
  {
    seed: '9D 61 B1 9D EF FD 5A 60 BA 84 4A F4 92 EC 2C C4 44 49 C5 69 7B 32 69 19 70 3B AC 03 1C AE 7F 60',
    publicKey:
      'D7 5A 98 01 82 B1 0A B7 D5 4B FE D3 C9 64 07 3A 0E E1 72 F3 DA A6 23 25 AF 02 1A 68 F7 07 51 1A',
    message: '',
    signature:
      'E5 56 43 00 C3 60 AC 72 90 86 E2 CC 80 6E 82 8A 84 87 7F 1E B8 E5 D9 74 D8 73 E0 65 22 49 01 55 ' +
      '5F B8 82 15 90 A3 3B AC C6 1E 39 70 1C F9 B4 6B D2 5B F5 F0 59 5B BE 24 65 51 41 43 8E 7A 10 0B',
  },
  {
    seed: '4C CD 08 9B 28 FF 96 DA 9D B6 C3 46 EC 11 4E 0F 5B 8A 31 9F 35 AB A6 24 DA 8C F6 ED 4F B8 A6 FB',
    publicKey:
      '3D 40 17 C3 E8 43 89 5A 92 B7 0A A7 4D 1B 7E BC 9C 98 2C CF 2E C4 96 8C C0 CD 55 F1 2A F4 66 0C',
    message: '72',
    signature:
      '92 A0 09 A9 F0 D4 CA B8 72 0E 82 0B 5F 64 25 40 A2 B2 7B 54 16 50 3F 8F B3 76 22 23 EB DB 69 DA ' +
      '08 5A C1 E4 3E 15 99 6E 45 8F 36 13 D0 F1 1D 8C 38 7B 2E AE B4 30 2A EE B0 0D 29 16 12 BB 0C 00',
  },
  {
    seed: 'C5 AA 8D F4 3F 9F 83 7B ED B7 44 2F 31 DC B7 B1 66 D3 85 35 07 6F 09 4B 85 CE 3A 2E 0B 44 58 F7',
    publicKey:
      'FC 51 CD 8E 62 18 A1 A3 8D A4 7E D0 02 30 F0 58 08 16 ED 13 BA 33 03 AC 5D EB 91 15 48 90 80 25',
    message: 'AF 82',
    signature:
      '62 91 D6 57 DE EC 24 02 48 27 E6 9C 3A BE 01 A3 0C E5 48 A2 84 74 3A 44 5E 36 80 D7 DB 5A C3 AC ' +
      '18 FF 9B 53 8D 16 F2 90 AE 67 F7 60 98 4D C6 59 4A 7C 15 E9 71 6E D2 8D C0 27 BE CE EA 1E C4 0A',
  },
];

describe('Ed25519 (RFC 8032)', () => {
  rfc8032.forEach((vector, i) => {
    test(`test ${i + 1}: public key, signature, verification`, async () => {
      const keys = await ed25519KeyPairFromSeed(hexBytes(vector.seed));
      expect(hexString(keys.publicKey)).toBe(vector.publicKey);
      const message = hexBytes(vector.message);
      const signature = await ed25519Sign(message, keys.privateKey);
      expect(hexString(signature)).toBe(vector.signature);
      expect(await ed25519Verify(keys.publicKey, message, signature)).toBe(true);
      const other = Uint8Array.of(...message, 0);
      expect(await ed25519Verify(keys.publicKey, other, signature)).toBe(false);
    });
  });

  test('the private key cannot be exported', async () => {
    const keys = await ed25519KeyPairFromSeed(hexBytes(rfc8032[0]?.seed ?? ''));
    expect(keys.privateKey.extractable).toBe(false);
    await expect(crypto.subtle.exportKey('jwk', keys.privateKey)).rejects.toThrow();
  });

  test('a seed of another length is refused', async () => {
    await expect(ed25519KeyPairFromSeed(new Uint8Array(31))).rejects.toThrow(RangeError);
  });

  test('keys and signatures of the wrong length, and garbage, do not verify', async () => {
    const vector = rfc8032[1];
    const publicKey = hexBytes(vector?.publicKey ?? '');
    const message = hexBytes(vector?.message ?? '');
    const signature = hexBytes(vector?.signature ?? '');
    expect(await ed25519Verify(publicKey.subarray(1), message, signature)).toBe(false);
    expect(await ed25519Verify(publicKey, message, signature.subarray(1))).toBe(false);
    expect(await ed25519Verify(new Uint8Array(32).fill(0xff), message, signature)).toBe(false);
    expect(await ed25519Verify(publicKey, message, new Uint8Array(64).fill(0xff))).toBe(false);
    // On a shared buffer too.
    const shared = new Uint8Array(new SharedArrayBuffer(64));
    shared.set(signature);
    expect(await ed25519Verify(publicKey, message, shared)).toBe(true);
  });

  test('constantTimeEquals', () => {
    expect(constantTimeEquals([1, 2, 3], [1, 2, 3])).toBe(true);
    expect(constantTimeEquals([1, 2, 3], [1, 2, 4])).toBe(false);
    expect(constantTimeEquals([1, 2], [1, 2, 3])).toBe(false);
    expect(constantTimeEquals([], [])).toBe(true);
  });
});
