// Alignment audit of the wire layers: the identity layer "copied" a caller's bytes with `.slice()`,
// which on a Node Buffer is a view on the same memory (design section 5 accepts a Buffer wherever a
// Uint8Array goes; handoff section 6: never `x.slice()` on a value that may be a Buffer). Dart copies
// at each place (`Uint8List.fromList`). The verifier imports an HMAC key on its first use, so a key
// handed over as a Buffer and wiped afterwards left a verifier that accepted credentials signed with
// the wiped (all-zero) key; `CredentialIssuer.hmac` signed with whatever the Buffer held later; a
// credential decoded from a Buffer changed with it, `encode()` included. Each place copies with the
// `Uint8Array` constructor now. Real time: the crypto is WebCrypto.
import { Buffer } from 'node:buffer';
import { describe, expect, test } from 'vitest';
import { CredentialKind, Right } from '../../../src/core/identity/contract.ts';
import { Credential } from '../../../src/core/identity/credential.ts';
import { CredentialIssuer } from '../../../src/core/identity/credential-issuer.ts';
import { CredentialVerifier } from '../../../src/core/identity/credential-verifier.ts';
import { Scope } from '../../../src/core/identity/scope.ts';
import { StatusCode, SwitchboardError } from '../../../src/core/status.ts';

const realKey = Uint8Array.from({ length: 32 }, (_, i) => i + 1);
const zeroKey = new Uint8Array(32);
const seed = Uint8Array.from({ length: 32 }, (_, i) => 3 * i);

async function bearer(issuer: CredentialIssuer, identity = 'worker'): Promise<Credential> {
  return issuer.issue({
    kind: CredentialKind.node,
    identity,
    scopes: [Scope.of(Right.admin, '*')],
  });
}

async function expectRefused(verifier: CredentialVerifier, bytes: Uint8Array): Promise<void> {
  const error = await verifier.verify(bytes).then(
    () => undefined,
    (e: unknown) => e,
  );
  expect(error).toBeInstanceOf(SwitchboardError);
  expect((error as SwitchboardError).code).toBe(StatusCode.unauthenticated);
}

describe('review: Buffers the identity layer keeps', () => {
  test('an HMAC key added as a Buffer is copied: wiping the Buffer keeps the key, and the wiped key forges nothing', async () => {
    const buffer = Buffer.from(realKey);
    const verifier = new CredentialVerifier();
    verifier.addHmacKey('mesh', buffer);
    buffer.fill(0);
    const genuine = await bearer(CredentialIssuer.hmac('mesh', realKey));
    const forged = await bearer(CredentialIssuer.hmac('mesh', zeroKey), 'admin');
    expect((await verifier.verify(genuine.encode())).identity).toBe('worker');
    await expectRefused(verifier, forged.encode());
  });

  test('HMAC keys given to the constructor as Buffers are copied', async () => {
    const buffer = Buffer.from(realKey);
    const verifier = new CredentialVerifier({ hmacKeys: [['mesh', buffer]] });
    buffer.fill(0);
    const genuine = await bearer(CredentialIssuer.hmac('mesh', realKey));
    const forged = await bearer(CredentialIssuer.hmac('mesh', zeroKey), 'admin');
    expect((await verifier.verify(genuine.encode())).identity).toBe('worker');
    await expectRefused(verifier, forged.encode());
  });

  test('an issuer HMAC key passed as a Buffer is copied: reusing the Buffer does not change what it signs with', async () => {
    const buffer = Buffer.from(realKey);
    const issuer = CredentialIssuer.hmac('mesh', buffer);
    buffer.fill(0);
    const verifier = new CredentialVerifier({ hmacKeys: [['mesh', realKey]] });
    const credential = await bearer(issuer);
    expect((await verifier.verify(credential.encode())).identity).toBe('worker');
  });

  test('an Ed25519 public key added as a Buffer is copied', async () => {
    const issuer = await CredentialIssuer.ed25519FromSeed('auth', seed);
    const buffer = Buffer.from(await issuer.publicKey());
    const verifier = new CredentialVerifier();
    verifier.addEd25519Key('auth', buffer);
    buffer.fill(0);
    const credential = await bearer(issuer);
    expect((await verifier.verify(credential.encode())).identity).toBe('worker');
  });

  test('a credential decoded from a Buffer is copied: reusing the Buffer changes neither encode(), its signature nor its holder key', async () => {
    const issuer = await CredentialIssuer.ed25519FromSeed('auth', seed);
    const holderKey = Uint8Array.from({ length: 32 }, (_, i) => 200 - i);
    const issued = await issuer.issue({ kind: CredentialKind.client, identity: 'ann', holderKey });
    const bytes = issued.encode();
    const buffer = Buffer.from(bytes);
    const decoded = Credential.decode(buffer);
    buffer.fill(0xff);
    expect(decoded.encode()).toEqual(bytes);
    expect(decoded.signature).toEqual(issued.signature);
    expect(decoded.holderKey).toEqual(holderKey);
    decoded.encode().fill(0);
    expect(decoded.encode()).toEqual(bytes);
  });

  test('a credential constructed from Buffers keeps copies of its holder key and signature', async () => {
    const issued = await (await CredentialIssuer.ed25519FromSeed('auth', seed)).issue({
      kind: CredentialKind.client,
      identity: 'ann',
      holderKey: Uint8Array.from({ length: 32 }, (_, i) => i),
    });
    const holderKey = Buffer.from(issued.holderKey as Uint8Array);
    const signature = Buffer.from(issued.signature);
    const rebuilt = new Credential({
      kind: issued.kind,
      keyId: issued.keyId,
      identity: issued.identity,
      issuedAt: issued.issuedAt,
      expiresAt: issued.expiresAt,
      holderKey,
      scopes: issued.scopes,
      signatureKind: issued.signatureKind,
      signature,
    });
    holderKey.fill(0);
    signature.fill(0);
    expect(rebuilt.holderKey).toEqual(issued.holderKey);
    expect(rebuilt.signature).toEqual(issued.signature);
    expect(rebuilt.encode()).toEqual(issued.encode());
  });

  test('verify() takes a copy of a Buffer the caller reuses at once', async () => {
    const issuer = CredentialIssuer.hmac('mesh', realKey);
    const verifier = new CredentialVerifier({ hmacKeys: [['mesh', realKey]] });
    const buffer = Buffer.from((await bearer(issuer)).encode());
    const verifying = verifier.verify(buffer);
    buffer.fill(0);
    expect((await verifying).identity).toBe('worker');
  });
});
