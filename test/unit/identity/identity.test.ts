// Credentials (issuer, verifier, key rotation, the cache) and connection
// identity on the mux control channel (NONCE and IDENT), over the memory
// transport. Ports `test/identity_test.dart` one to one; the byte layouts
// are in `test/vectors/credential.test.ts` and `identity.test.ts`.
import { beforeAll, describe, expect, test } from 'vitest';
import { concatBytes, hexBytes, hexString, utf8Encode } from '../../../src/core/bytes.ts';
import {
  CredentialKind,
  type PeerIdentity,
  Right,
  type Scope as ScopeContract,
} from '../../../src/core/identity/contract.ts';
import { Credential } from '../../../src/core/identity/credential.ts';
import { CredentialIssuer } from '../../../src/core/identity/credential-issuer.ts';
import { CredentialVerifier } from '../../../src/core/identity/credential-verifier.ts';
import { Ed25519HolderKey } from '../../../src/core/identity/holder-key.ts';
import { Scope } from '../../../src/core/identity/scope.ts';
import type { MuxChannel } from '../../../src/core/mux/contract.ts';
import type { FrameMuxConnection } from '../../../src/core/mux/frame-mux-connection.ts';
import { MuxControlMessage, MuxControlType } from '../../../src/core/mux/mux-frame.ts';
import { MuxIdent } from '../../../src/core/mux/mux-protocol.ts';
import { Name } from '../../../src/core/name.ts';
import { type Status, StatusCode, SwitchboardError } from '../../../src/core/status.ts';
import { systemClock } from '../../../src/core/util/clock.ts';
import {
  muxPair as harnessMuxPair,
  rawPair as harnessRawPair,
  isControl,
  type PairOptions,
  quiet,
  type RawPairOptions,
  type RawPeer,
  rawOptions,
} from '../mux/support.ts';

const meshKey = Uint8Array.from({ length: 32 }, (_, i) => i);
const otherKey = Uint8Array.from({ length: 32 }, (_, i) => 100 + i);
const authoritySeed = Uint8Array.from({ length: 32 }, (_, i) => 7 * i);
const empty = new Uint8Array(0);
const minute = 60_000;
const hour = 60 * minute;

// The mux harness's pairs, on real time: Dart's identity tests run on the
// real clock (credential checks are WebCrypto work, and the identity
// timeouts are real ones), where the harness defaults to a stepping clock.
const muxPair = (options: PairOptions = {}) => harnessMuxPair({ clock: systemClock, ...options });
const rawPair = (options: RawPairOptions = {}) =>
  harnessRawPair({ clock: systemClock, ...options });

/** The frame of a control message of [type] carrying [payload], in hex. */
function control(type: MuxControlType, payload: Uint8Array): string {
  return hexString(new MuxControlMessage(type, payload).toFrame().encode());
}

let hmacIssuer: CredentialIssuer;
let edIssuer: CredentialIssuer;
let holder: Ed25519HolderKey;

/** A bearer credential for `npc-7`. */
function bearer(
  options: {
    issuer?: CredentialIssuer;
    identity?: string;
    issuedAt?: number;
    lifetime?: number | null;
  } = {},
): Promise<Credential> {
  return (options.issuer ?? hmacIssuer).issue({
    kind: CredentialKind.node,
    identity: options.identity ?? 'npc-7',
    scopes: [Scope.of(Right.open, 'npc'), Scope.of(Right.register, 'npc')],
    issuedAt: options.issuedAt,
    lifetime: options.lifetime === undefined ? hour : options.lifetime,
  });
}

/** A credential for `worker-a1` naming [holder]'s key. */
function held(identity = 'worker-a1'): Promise<Credential> {
  return edIssuer.issue({
    kind: CredentialKind.node,
    identity,
    scopes: [Scope.of(Right.register, 'worker-*')],
    holderKey: holder.publicKey,
  });
}

async function verifierFor(
  issuers: CredentialIssuer[],
  options: { cacheSize?: number } = {},
): Promise<CredentialVerifier> {
  const verifier = new CredentialVerifier(options);
  for (const issuer of issuers) {
    await verifier.addIssuer(issuer);
  }
  return verifier;
}

/** A verifier that counts the credentials it is asked to verify. */
class CountingVerifier extends CredentialVerifier {
  calls = 0;

  override verify(bytes: Uint8Array, options?: { now?: number | undefined }): Promise<Credential> {
    this.calls++;
    return super.verify(bytes, options);
  }
}

/** Two connected muxes; [acceptorVerifier] checks the initiator's IDENT, [initiatorVerifier] the acceptor's. */
function identityPair(
  options: {
    acceptorVerifier?: CredentialVerifier;
    initiatorVerifier?: CredentialVerifier;
    identityTimeout?: number;
  } = {},
): [FrameMuxConnection, FrameMuxConnection] {
  const identityTimeout = options.identityTimeout ?? 5000;
  return muxPair({
    initiator: { ...quiet, identityVerifier: options.initiatorVerifier, identityTimeout },
    acceptor: { ...quiet, identityVerifier: options.acceptorVerifier, identityTimeout },
  });
}

/** What [promise] rejects with; fails the test if it resolves. */
async function rejection(promise: Promise<unknown>): Promise<unknown> {
  return promise.then(
    () => {
      throw new Error('expected a rejection');
    },
    (e: unknown) => e,
  );
}

/** Dart `throwsStatus(code)`: [promise] rejects with a {@link SwitchboardError} of [code]. */
async function expectStatus(
  promise: Promise<unknown>,
  code: StatusCode,
): Promise<SwitchboardError> {
  const error = await rejection(promise);
  expect(error).toBeInstanceOf(SwitchboardError);
  expect((error as SwitchboardError).code).toBe(code);
  return error as SwitchboardError;
}

/** Dart `hasCode(code)`. */
function expectCode(status: Status, code: StatusCode): void {
  expect(status.known).toBe(code);
}

/** The first channel of [connection]'s `incoming`. */
async function firstIncoming(connection: FrameMuxConnection): Promise<MuxChannel> {
  const next = await connection.incoming[Symbol.asyncIterator]().next();
  if (next.done === true) {
    throw new Error('no incoming channel');
  }
  return next.value;
}

function filled(length: number, value: number): Uint8Array {
  return new Uint8Array(length).fill(value);
}

beforeAll(async () => {
  hmacIssuer = CredentialIssuer.hmac(Name.of('mesh1'), meshKey);
  edIssuer = await CredentialIssuer.ed25519FromSeed(Name.of('auth1'), authoritySeed);
  holder = await Ed25519HolderKey.generate();
});

describe('issuer', () => {
  test('lifetime, issue time and never expiring', async () => {
    const at = Date.UTC(2026, 2, 1, 12, 0, 0, 999);
    const credential = await bearer({ issuedAt: at, lifetime: 90 * minute });
    expect(credential.issuedAtTime.getTime()).toBe(Date.UTC(2026, 2, 1, 12));
    expect(credential.expiresAtTime?.getTime()).toBe(Date.UTC(2026, 2, 1, 13, 30));
    expect(credential.isExpired(Date.UTC(2026, 2, 1, 13, 29))).toBe(false);
    expect(credential.isExpired(Date.UTC(2026, 2, 1, 13, 30))).toBe(true);
    const forever = await bearer({ lifetime: null });
    expect(forever.expiresAt).toBe(0);
    expect(forever.expiresAtTime).toBeUndefined();
    expect(forever.isExpired(Date.UTC(2100, 0))).toBe(false);
  });

  test('refuses what the layout or the signature kind cannot carry', async () => {
    expect(() => CredentialIssuer.hmac(Name.of('k'), new Uint8Array(15))).toThrow(RangeError);
    await expect(
      hmacIssuer.issue({ kind: CredentialKind.node, identity: 'x', holderKey: holder.publicKey }),
    ).rejects.toThrow(RangeError);
    await expect(
      hmacIssuer.issue({ kind: CredentialKind.node, identity: 'x'.repeat(256) }),
    ).rejects.toThrow(RangeError);
    await expect(
      hmacIssuer.issue({ kind: CredentialKind.node, identity: 'x', lifetime: 0 }),
    ).rejects.toThrow(RangeError);
    // 22 bytes of fields, 95 scopes of 9 bytes, the signature kind and a
    // 32-byte MAC: longer than an IDENT can carry.
    await expect(
      hmacIssuer.issue({
        kind: CredentialKind.node,
        identity: 'x',
        scopes: Array.from({ length: 95 }, () => Scope.of(Right.open, 'x')),
      }),
    ).rejects.toThrow(RangeError);
    await expect(
      edIssuer.issue({ kind: CredentialKind.node, identity: 'x', holderKey: new Uint8Array(31) }),
    ).rejects.toThrow(RangeError);
    // Dart StateError.
    const error = await rejection(hmacIssuer.publicKey());
    expect(error).toBeInstanceOf(Error);
    expect((error as Error).constructor).toBe(Error);
  });

  test('the largest credential fits an IDENT with intent and proof', async () => {
    const credential = await edIssuer.issue({
      kind: CredentialKind.node,
      identity: 'x'.repeat(255),
      holderKey: holder.publicKey,
      // 308 bytes of fields, the signature kind and a 64-byte signature
      // leave room for this many scopes.
      scopes: Array.from(
        { length: Math.floor((Credential.maxLength - 373) / Scope.encodedLength) },
        () => Scope.of(Right.open, 'x'),
      ),
    });
    expect(credential.encode().length).toBeLessThanOrEqual(Credential.maxLength);
    const ident = new MuxIdent({
      credential: credential.encode(),
      intent: new Uint8Array(MuxIdent.maxIntentLength),
      proof: new Uint8Array(MuxIdent.proofLength),
    });
    expect(ident.encode().length).toBeLessThanOrEqual(MuxControlMessage.maxControlPayload);
  });
});

describe('verifier', () => {
  test('verifies both kinds and caches by bytes', async () => {
    const verifier = await verifierFor([hmacIssuer, edIssuer]);
    expect(verifier.keyIds.map(String).sort()).toEqual(['auth1', 'mesh1']);
    const a = await bearer();
    const b = await held();
    expect((await verifier.verify(a.encode())).equals(a)).toBe(true);
    expect((await verifier.verify(b.encode())).equals(b)).toBe(true);
    expect(verifier.cachedCount).toBe(2);
    expect((await verifier.verify(b.encode())).equals(b)).toBe(true);
    expect(verifier.cachedCount).toBe(2);
    // Concurrent checks of new bytes share one check and all succeed.
    const c = await held('worker-a2');
    const results = await Promise.all(Array.from({ length: 5 }, () => verifier.verify(c.encode())));
    for (const result of results) {
      expect(result.equals(c)).toBe(true);
      expect(result).toBe(results[0]);
    }
    expect(verifier.cachedCount).toBe(3);
  });

  test('cached answers synchronously what verify checked', async () => {
    const verifier = await verifierFor([hmacIssuer, edIssuer]);
    const a = await bearer();
    const bytes = a.encode();
    expect(verifier.cached(bytes)).toBeUndefined();
    expect((await verifier.verify(bytes)).equals(a)).toBe(true);
    expect(verifier.cached(bytes)?.equals(a)).toBe(true);
    expect(verifier.cached(concatBytes(bytes, [1]))).toBeUndefined();
    // Expired at the time asked about, and after key rotation.
    expect(
      verifier.cached(bytes, { now: (a.expiresAtTime?.getTime() ?? 0) + 1000 }),
    ).toBeUndefined();
    verifier.removeKey(Name.of('mesh1'));
    expect(verifier.cached(bytes)).toBeUndefined();
  });

  test('the cache is bounded, least recently used first', async () => {
    const verifier = await verifierFor([hmacIssuer], { cacheSize: 2 });
    const one = await bearer({ identity: 'one' });
    const two = await bearer({ identity: 'two' });
    const three = await bearer({ identity: 'three' });
    await verifier.verify(one.encode());
    await verifier.verify(two.encode());
    await verifier.verify(one.encode());
    await verifier.verify(three.encode());
    expect(verifier.cachedCount).toBe(2);
    // Not asserted by the Dart test: the least recently used went.
    expect(verifier.cached(one.encode())).toBeDefined();
    expect(verifier.cached(two.encode())).toBeUndefined();
    const none = await verifierFor([hmacIssuer], { cacheSize: 0 });
    await none.verify(one.encode());
    expect(none.cachedCount).toBe(0);
  });

  test('key rotation: current and previous key, then removal', async () => {
    const next = CredentialIssuer.hmac(Name.of('mesh2'), otherKey);
    const verifier = await verifierFor([hmacIssuer, next]);
    const old = await bearer();
    const renewed = await bearer({ issuer: next });
    expect((await verifier.verify(old.encode())).equals(old)).toBe(true);
    expect((await verifier.verify(renewed.encode())).equals(renewed)).toBe(true);
    expect(verifier.removeKey(Name.of('mesh1'))).toBe(true);
    expect(verifier.removeKey(Name.of('mesh1'))).toBe(false);
    expect(verifier.cachedCount).toBe(0);
    await expectStatus(verifier.verify(old.encode()), StatusCode.unauthenticated);
    expect((await verifier.verify(renewed.encode())).equals(renewed)).toBe(true);
  });

  test('a replaced key no longer verifies what the old one signed', async () => {
    const verifier = await verifierFor([hmacIssuer]);
    const credential = await bearer();
    await verifier.verify(credential.encode());
    verifier.addHmacKey(Name.of('mesh1'), otherKey);
    await expectStatus(verifier.verify(credential.encode()), StatusCode.unauthenticated);
  });

  test('unknown key id, wrong key kind, expired, generic reason', async () => {
    const verifier = await verifierFor([hmacIssuer]);
    const unknown = await bearer({ issuer: CredentialIssuer.hmac(Name.of('mesh9'), meshKey) });
    // An Ed25519 credential under the id of the HMAC key.
    const mismatched = await (
      await CredentialIssuer.ed25519FromSeed(Name.of('mesh1'), authoritySeed)
    ).issue({ kind: CredentialKind.node, identity: 'x' });
    const expired = await bearer({ issuedAt: Date.now() - 2 * hour });
    for (const credential of [unknown, mismatched, expired]) {
      const error = await expectStatus(
        verifier.verify(credential.encode()),
        StatusCode.unauthenticated,
      );
      expect(error.status.reason).toBe('invalid credential');
    }
    // Expiry is checked against the given time, cached or not.
    const current = await bearer();
    await verifier.verify(current.encode());
    await expectStatus(
      verifier.verify(current.encode(), { now: Date.now() + 2 * hour }),
      StatusCode.unauthenticated,
    );
  });

  test('refuses bad keys', () => {
    expect(() => new CredentialVerifier({ hmacKeys: { k: new Uint8Array(8) } })).toThrow(
      RangeError,
    );
    expect(() => new CredentialVerifier({ ed25519Keys: { k: new Uint8Array(31) } })).toThrow(
      RangeError,
    );
    expect(
      () =>
        new CredentialVerifier({
          hmacKeys: { k: meshKey },
          ed25519Keys: { k: new Uint8Array(32) },
        }),
    ).toThrow(RangeError);
  });
});

describe('connection identity', () => {
  test('bearer credential: identified without a proof', async () => {
    const verifier = await verifierFor([hmacIssuer]);
    const [a, b] = identityPair({ acceptorVerifier: verifier });
    const credential = await bearer();
    let requested = false;
    void b.identityRequested.then(() => {
      requested = true;
    });
    await a.identify(credential);
    const identity = await b.peerIdentified;
    expect(identity.identity).toBe('npc-7');
    expect(identity.kind).toBe(CredentialKind.node);
    expect(identity.provedPossession).toBe(false);
    expect(identity.intent).toHaveLength(0);
    expect((identity.credential as Credential).equals(credential)).toBe(true);
    expect(identity.allows(Right.open, Name.of('npc'))).toBe(true);
    expect(identity.allows(Right.open, Name.of('chat'))).toBe(false);
    expect(b.peerIdentity).toBe(identity);
    expect(a.peerIdentity).toBeUndefined();
    expect(requested).toBe(true);
    expect(a.isOpen && b.isOpen).toBe(true);
    await a.close();
    await b.done;
  });

  test('holder-key credential with a proof and an intent', async () => {
    const verifier = await verifierFor([edIssuer]);
    const [a, b] = identityPair({ acceptorVerifier: verifier });
    await a.identify(await held(), { holderKey: holder, intent: hexBytes('01 02 03') });
    const identity = b.peerIdentity as PeerIdentity;
    expect(identity.identity).toBe('worker-a1');
    expect(identity.provedPossession).toBe(true);
    expect(hexString(identity.intent)).toBe('01 02 03');
    await a.close();
    await b.done;
  });

  test('both directions, independently and at once', async () => {
    const verifier = await verifierFor([hmacIssuer, edIssuer]);
    const [a, b] = identityPair({ acceptorVerifier: verifier, initiatorVerifier: verifier });
    const left = await bearer({ identity: 'left' });
    const right = await held();
    await Promise.all([a.identify(left), b.identify(right, { holderKey: holder })]);
    expect(b.peerIdentity?.identity).toBe('left');
    expect(a.peerIdentity?.identity).toBe('worker-a1');
    expect(a.peerIdentity?.provedPossession).toBe(true);
    await a.close();
    await b.done;
  });

  test('a second valid IDENT replaces the first', async () => {
    const verifier = await verifierFor([hmacIssuer]);
    const [a, b] = identityPair({ acceptorVerifier: verifier });
    await a.identify(await bearer({ identity: 'first' }));
    await a.identify(await bearer({ identity: 'second' }));
    expect(b.peerIdentity?.identity).toBe('second');
    expect((await b.peerIdentified).identity).toBe('first');
    await a.close();
    await b.done;
  });

  test('frames after an IDENT wait for its verification', async () => {
    const verifier = await verifierFor([edIssuer]);
    const [b, raw] = rawPair({
      muxIsInitiator: false,
      options: { ...rawOptions, identityVerifier: verifier },
    });
    const senderNonce = filled(32, 0x11);
    raw.send(control(MuxControlType.nonce, senderNonce));
    const answer = await raw.nextControl(MuxControlType.nonce);
    const credential = await held();
    const intent = new Uint8Array(0);
    const proof = await holder.sign(
      MuxIdent.proofMessage(senderNonce, answer.payload, intent, empty),
    );
    const opened = firstIncoming(b).then((channel) => channel.connection.peerIdentity);
    // IDENT and OPEN in one burst: the Ed25519 check takes a while, and the
    // OPEN must not overtake it.
    raw.send(
      control(
        MuxControlType.ident,
        new MuxIdent({ credential: credential.encode(), intent, proof }).encode(),
      ),
    );
    raw.send('12 02 00 00');
    const seen = await opened;
    expect(seen?.identity).toBe('worker-a1');
    await b.close();
  });

  test('a replayed IDENT from another connection is rejected', async () => {
    // First connection: an honest identification, captured by a raw peer
    // that answers with its own nonce.
    const [a, first] = rawPair();
    const identifying = a.identify(await held(), { holderKey: holder });
    const aNonce = await first.nextControl(MuxControlType.nonce);
    first.send(control(MuxControlType.nonce, filled(32, 0x22)));
    const ident = await first.nextControl(MuxControlType.ident);
    const ping = await first.nextControl(MuxControlType.ping);
    first.send(control(MuxControlType.pong, ping.payload));
    await identifying;
    await a.close();

    // Second connection: the attacker replays the nonce and the IDENT of
    // the first, but the receiver's nonce is a new one.
    const verifier = await verifierFor([edIssuer]);
    const [b, raw] = rawPair({
      muxIsInitiator: false,
      options: { ...rawOptions, identityVerifier: verifier },
    });
    raw.send(control(MuxControlType.nonce, aNonce.payload));
    const bNonce = await raw.nextControl(MuxControlType.nonce);
    expect(hexString(bNonce.payload)).not.toBe(hexString(aNonce.payload));
    raw.send(control(MuxControlType.ident, ident.payload));
    const goAway = await raw.nextControl(MuxControlType.goAway);
    expectCode(goAway.goAwayStatus, StatusCode.unauthenticated);
    expect(goAway.goAwayStatus.reason).toBe('identification failed');
    expectCode(await b.done, StatusCode.unauthenticated);
    expect(b.peerIdentity).toBeUndefined();
  });

  test('a peer that echoes our NONCE cannot send our IDENT back to be identified as us', async () => {
    const credential = await edIssuer.issue({
      kind: CredentialKind.node,
      identity: 'admin-1',
      scopes: [Scope.of(Right.admin, '*')],
      holderKey: holder.publicKey,
    });
    // A node that checks identities identifies, naming nobody, to a peer it
    // dialled.
    const [a, m] = rawPair({
      options: { ...rawOptions, identityVerifier: await verifierFor([edIssuer]) },
    });
    a.localIdentity = 'admin-1';
    const identifying = a.identify(credential, { holderKey: holder });
    identifying.catch(() => {});
    const nonce = await m.nextControl(MuxControlType.nonce);
    // The same nonce back: a proof over SWBIDENT || N || N || intent ||
    // receiver would verify both ways.
    m.send(control(MuxControlType.nonce, nonce.payload));
    const goAway = await m.nextControl(MuxControlType.goAway);
    expectCode(goAway.goAwayStatus, StatusCode.protocolError);
    await expectStatus(identifying, StatusCode.protocolError);
    expectCode(await a.done, StatusCode.protocolError);
    expect(a.peerIdentity).toBeUndefined();
  });

  test('a replayed bearer IDENT is accepted, by design', async () => {
    // A bearer credential is a password (wiki "Security considerations"):
    // its IDENT carries no proof, so nothing binds it to the nonces of the
    // connection it was sent on, and whoever saw it presents it again until
    // it expires. Only a holder-key credential resists the replay (the test
    // above); bearer credentials stay within one trust domain. Changing
    // this means changing the wire format, not a check.
    const [a, first] = rawPair();
    const identifying = a.identify(await bearer());
    await first.nextControl(MuxControlType.nonce);
    first.send(control(MuxControlType.nonce, filled(32, 0x22)));
    const ident = await first.nextControl(MuxControlType.ident);
    const ping = await first.nextControl(MuxControlType.ping);
    first.send(control(MuxControlType.pong, ping.payload));
    await identifying;
    await a.close();

    const verifier = await verifierFor([hmacIssuer]);
    const [b, raw] = rawPair({
      muxIsInitiator: false,
      options: { ...rawOptions, identityVerifier: verifier },
    });
    raw.send(control(MuxControlType.nonce, filled(32, 0x33)));
    await raw.nextControl(MuxControlType.nonce);
    raw.send(control(MuxControlType.ident, ident.payload));
    raw.send(control(MuxControlType.ping, hexBytes('07')));
    const pong = await raw.nextControl(MuxControlType.pong);
    expect(hexString(pong.payload)).toBe('07');
    expect(b.peerIdentity?.identity).toBe('npc-7');
    expect(b.peerIdentity?.provedPossession).toBe(false);
    expect(b.isOpen).toBe(true);
    await b.close();
  });

  test('a failing IDENT ends the connection: one verification per connection', async () => {
    // An unidentified peer costs the receiver at most one credential check
    // per connection: the IDENTs it queues behind a failing one are dropped
    // unverified with the connection.
    const verifier = new CountingVerifier();
    await verifier.addIssuer(hmacIssuer);
    const [b, raw] = rawPair({
      muxIsInitiator: false,
      options: { ...rawOptions, identityVerifier: verifier },
    });
    raw.send(control(MuxControlType.nonce, new Uint8Array(32)));
    await raw.nextControl(MuxControlType.nonce);
    for (let i = 0; i < 20; i++) {
      // Distinct, well-formed, with a signature that does not verify.
      const forged = (await bearer({ identity: `npc-${i}` })).encode();
      forged[forged.length - 1] = (forged[forged.length - 1] as number) ^ 1;
      raw.send(control(MuxControlType.ident, new MuxIdent({ credential: forged }).encode()));
    }
    const goAway = await raw.nextControl(MuxControlType.goAway);
    expectCode(goAway.goAwayStatus, StatusCode.unauthenticated);
    expectCode(await b.done, StatusCode.unauthenticated);
    expect(verifier.calls).toBe(1);
    expect(b.peerIdentity).toBeUndefined();
  });

  describe('receiver binding', () => {
    /** A mux accepting IDENTs as [identity] (undefined: no credential). */
    function receiverPair(
      verifier: CredentialVerifier,
      options: { identity?: string | undefined; requireNamedIdent?: boolean } = {},
    ): [FrameMuxConnection, RawPeer] {
      const [b, raw] = rawPair({
        muxIsInitiator: false,
        options: {
          ...rawOptions,
          identityVerifier: verifier,
          requireNamedIdent: options.requireNamedIdent ?? false,
        },
      });
      b.localIdentity = 'identity' in options ? options.identity : 't';
      return [b, raw];
    }

    /**
     * [a] identifies to a malicious peer `m` (naming it), which relays the
     * handshake live to the honest node `t`: the nonces are passed both
     * ways, and the IDENT goes to `t` as [rewrite] makes it.
     */
    async function relay(rewrite: (ident: MuxIdent) => MuxIdent): Promise<FrameMuxConnection> {
      const verifier = await verifierFor([edIssuer]);
      const [a, toA] = rawPair();
      const [t, toT] = receiverPair(verifier);
      const identifying = a.identify(await held(), { holderKey: holder, receiver: 'm' });
      identifying.catch(() => {});
      const aNonce = await toA.nextControl(MuxControlType.nonce);
      toT.send(control(MuxControlType.nonce, aNonce.payload));
      const tNonce = await toT.nextControl(MuxControlType.nonce);
      toA.send(control(MuxControlType.nonce, tNonce.payload));
      const ident = await toA.nextControl(MuxControlType.ident);
      toT.send(control(MuxControlType.ident, rewrite(MuxIdent.decode(ident.payload)).encode()));
      const goAway = await toT.nextControl(MuxControlType.goAway);
      expectCode(goAway.goAwayStatus, StatusCode.unauthenticated);
      expect(goAway.goAwayStatus.reason).toBe('identification failed');
      expectCode(await t.done, StatusCode.unauthenticated);
      expect(t.peerIdentity).toBeUndefined();
      await a.close();
      return t;
    }

    test('a relayed handshake is refused by the third node', async () => {
      // As sent: it names m, not t.
      await relay((ident) => ident);
    });

    test('a relay cannot rename the receiver', async () => {
      // Naming t, or nobody: the proof covers the receiver.
      for (const name of ['t', '']) {
        await relay(
          (ident) =>
            new MuxIdent({
              credential: ident.credential,
              intent: ident.intent,
              receiver: utf8Encode(name),
              proof: ident.proof,
            }),
        );
      }
      // Nor move the name into the intent: the proof covers the lengths.
      await relay(
        (ident) =>
          new MuxIdent({
            credential: ident.credential,
            intent: concatBytes(ident.intent, ident.receiver),
            proof: ident.proof,
          }),
      );
    });

    test('the named receiver accepts; another refuses', async () => {
      const verifier = await verifierFor([edIssuer, hmacIssuer]);
      for (const credential of [await held(), await bearer()]) {
        const [a, b] = identityPair({ acceptorVerifier: verifier });
        b.localIdentity = 'gate';
        await a.identify(credential, { holderKey: holder, receiver: 'gate' });
        expect(b.peerIdentity?.identity).toBe(credential.identity);
        expect(b.peerIdentity?.receiver).toBe('gate');
        await a.close();
        await b.done;

        const [c, d] = identityPair({ acceptorVerifier: verifier });
        d.localIdentity = 'gate';
        await expectStatus(
          c.identify(credential, { holderKey: holder, receiver: 'other' }),
          StatusCode.unauthenticated,
        );
        expectCode(await d.done, StatusCode.unauthenticated);
        await c.done;
      }
    });

    test('a receiver without an identity refuses a named IDENT', async () => {
      const verifier = await verifierFor([hmacIssuer]);
      const [a, b] = identityPair({ acceptorVerifier: verifier });
      await expectStatus(
        a.identify(await bearer(), { receiver: 'gate' }),
        StatusCode.unauthenticated,
      );
      expectCode(await b.done, StatusCode.unauthenticated);
      await a.done;
    });

    test('an unnamed IDENT is accepted unless requireNamedIdent', async () => {
      const verifier = await verifierFor([hmacIssuer]);
      const [a, b] = identityPair({ acceptorVerifier: verifier });
      b.localIdentity = 'gate';
      await a.identify(await bearer());
      expect(b.peerIdentity?.identity).toBe('npc-7');
      expect(b.peerIdentity?.receiver).toBe('');
      await a.close();
      await b.done;

      const [c, d] = muxPair({
        acceptor: { ...quiet, identityVerifier: verifier, requireNamedIdent: true },
      });
      d.localIdentity = 'gate';
      await expectStatus(c.identify(await bearer()), StatusCode.unauthenticated);
      expectCode(await d.done, StatusCode.unauthenticated);
      await c.done;

      const [e, f] = muxPair({
        acceptor: { ...quiet, identityVerifier: verifier, requireNamedIdent: true },
      });
      f.localIdentity = 'gate';
      await e.identify(await bearer(), { receiver: 'gate' });
      expect(f.peerIdentity?.receiver).toBe('gate');
      await e.close();
      await f.done;
    });
  });

  test('a bearer IDENT with a proof is rejected', async () => {
    const verifier = await verifierFor([hmacIssuer]);
    const [b, raw] = rawPair({
      muxIsInitiator: false,
      options: { ...rawOptions, identityVerifier: verifier },
    });
    raw.send(control(MuxControlType.nonce, new Uint8Array(32)));
    await raw.nextControl(MuxControlType.nonce);
    raw.send(
      control(
        MuxControlType.ident,
        new MuxIdent({ credential: (await bearer()).encode(), proof: new Uint8Array(64) }).encode(),
      ),
    );
    expectCode(await b.done, StatusCode.unauthenticated);
  });

  for (const [name, make] of [
    ['an expired credential', () => bearer({ issuedAt: Date.now() - 2 * hour })],
    [
      'an unknown key id',
      () => bearer({ issuer: CredentialIssuer.hmac(Name.of('mesh9'), meshKey) }),
    ],
    [
      'a forged signature',
      async () => {
        const good = (await bearer()).encode();
        good[good.length - 1] = (good[good.length - 1] as number) ^ 0x80;
        return Credential.decode(good);
      },
    ],
  ] as const satisfies readonly (readonly [string, () => Promise<Credential>])[]) {
    test(`${name} ends the connection with GOAWAY UNAUTHENTICATED`, async () => {
      const verifier = await verifierFor([hmacIssuer]);
      const [a, b] = identityPair({ acceptorVerifier: verifier });
      const peerStatus = a.peerGoAwayStatus;
      await expectStatus(a.identify(await make()), StatusCode.unauthenticated);
      expectCode(await peerStatus, StatusCode.unauthenticated);
      expectCode(await b.done, StatusCode.unauthenticated);
      expect(b.peerIdentity).toBeUndefined();
      await a.done;
    });
  }

  test('without a verifier an IDENT is ignored', async () => {
    const [a, b] = identityPair();
    await a.identify(await bearer());
    expect(b.peerIdentity).toBeUndefined();
    expect(b.isOpen).toBe(true);
    // A NONCE is still answered: both sides have one.
    await b.identify(await bearer({ identity: 'other' }));
    await a.close();
    await b.done;
  });

  test('no NONCE from the peer within the timeout', async () => {
    const [a, raw] = rawPair({ options: { ...rawOptions, identityTimeout: 50 } });
    await expectStatus(a.identify(await bearer()), StatusCode.deadlineExceeded);
    expect(isControl(await raw.next(), MuxControlType.nonce)).toBe(true);
    await a.close();

    // An explicit timeout wins over the option.
    const [b] = rawPair({ options: { ...rawOptions, identityTimeout: 20_000 } });
    const started = Date.now();
    await expectStatus(b.identify(await bearer(), { timeout: 20 }), StatusCode.deadlineExceeded);
    expect(Date.now() - started).toBeLessThan(10_000);
    await b.close();
  });

  test('no confirmation within the timeout', async () => {
    const [a, raw] = rawPair({ options: { ...rawOptions, identityTimeout: 100 } });
    const identifying = expectStatus(a.identify(await bearer()), StatusCode.deadlineExceeded);
    await raw.nextControl(MuxControlType.nonce);
    raw.send(control(MuxControlType.nonce, new Uint8Array(32)));
    await raw.nextControl(MuxControlType.ident);
    await raw.nextControl(MuxControlType.ping);
    await identifying;
    await a.close();
  });

  test('the connection ending fails identify with its status', async () => {
    const [a, raw] = rawPair();
    const identifying = expectStatus(a.identify(await bearer()), StatusCode.connectionLost);
    await raw.nextControl(MuxControlType.nonce);
    await raw.close();
    await identifying;
    await expectStatus(a.identify(await bearer()), StatusCode.failedPrecondition);
  });

  test('argument checks', async () => {
    const [a, b] = identityPair();
    const credential = await held();
    await expect(a.identify(credential)).rejects.toThrow(RangeError);
    await expect(
      a.identify(credential, { holderKey: await Ed25519HolderKey.generate() }),
    ).rejects.toThrow(RangeError);
    await expect(a.identify(await bearer(), { intent: new Uint8Array(65) })).rejects.toThrow(
      RangeError,
    );
    await expect(a.identify(await bearer(), { receiver: 'x'.repeat(256) })).rejects.toThrow(
      RangeError,
    );
    await a.close();
    await b.done;
  });

  // The envelope rules hold whether or not the receiver can verify
  // credentials: a side without a verifier ignores only well-formed IDENTs,
  // so a malformed one does not tell whether it checks identity.
  for (const verifying of [true, false]) {
    describe(`protocol errors ${verifying ? 'with' : 'without'} a verifier`, () => {
      async function expectProtocolError(frames: string[]): Promise<void> {
        const [b, raw] = rawPair({
          muxIsInitiator: false,
          options: verifying
            ? { ...rawOptions, identityVerifier: await verifierFor([hmacIssuer]) }
            : rawOptions,
        });
        for (const frame of frames) {
          raw.send(frame);
        }
        const goAway = await raw.nextControl(MuxControlType.goAway);
        expectCode(goAway.goAwayStatus, StatusCode.protocolError);
        expectCode(await b.done, StatusCode.protocolError);
      }

      test('NONCE of 31 bytes', async () => {
        await expectProtocolError([control(MuxControlType.nonce, new Uint8Array(31))]);
      });

      test('a second NONCE', async () => {
        await expectProtocolError([
          control(MuxControlType.nonce, new Uint8Array(32)),
          control(MuxControlType.nonce, new Uint8Array(32)),
        ]);
      });

      test('IDENT before the nonces', async () => {
        await expectProtocolError([
          control(
            MuxControlType.ident,
            new MuxIdent({ credential: (await bearer()).encode() }).encode(),
          ),
        ]);
      });

      test('a truncated IDENT', async () => {
        await expectProtocolError([
          control(MuxControlType.nonce, new Uint8Array(32)),
          control(MuxControlType.ident, hexBytes('05 00 01 02')),
        ]);
      });

      test('an IDENT with an intent over 64 bytes', async () => {
        const credential = (await bearer()).encode();
        await expectProtocolError([
          control(MuxControlType.nonce, new Uint8Array(32)),
          control(
            MuxControlType.ident,
            concatBytes(
              [credential.length & 0xff, credential.length >> 8],
              credential,
              [65],
              new Uint8Array(65),
              [0],
            ),
          ),
        ]);
      });
    });
  }
});

/** Scopes as the contract types them still match (the issuer takes any). */
test('an issuer takes scopes of the contract shape', async () => {
  const scope: ScopeContract = {
    right: Right.watch,
    pattern: Name.of('npc-*'),
    matches: () => false,
  };
  const credential = await hmacIssuer.issue({
    kind: CredentialKind.client,
    identity: 'x',
    scopes: [scope],
  });
  expect(credential.scopes[0]).toBeInstanceOf(Scope);
  expect(credential.allows(Right.watch, 'npc-7')).toBe(true);
});
