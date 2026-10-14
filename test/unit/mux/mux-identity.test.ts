// Connection identity on the frame mux's control channel (NONCE and
// IDENT): the "connection identity" group of `test/identity_test.dart`,
// which exercises `FrameMuxConnection.identify` and the IDENT exchange.
//
// The credential codec, issuer and verifier are not ported yet (the
// identity thread's step), so the credentials here are stand-ins that a
// stub verifier knows by their bytes (`test/support/identity`); the proofs
// of possession are real Ed25519 signatures with the RFC 8032 holder key.
// What the Dart tests make with the issuer (an expired credential, an
// unknown key id, a forged signature) is a credential the verifier
// refuses. The issuer and verifier groups of that file come with the
// codec; the identity thread's port of the file can take this group over.
import { describe, expect, test } from 'vitest';
import { bytesEqual, hexBytes, hexString, utf8Encode } from '../../../src/core/bytes.ts';
import {
  type Credential,
  CredentialKind,
  type HolderKey,
  Right,
  type Scope,
} from '../../../src/core/identity/contract.ts';
import { ed25519KeyPairFromSeed, ed25519Sign } from '../../../src/core/identity/ed25519.ts';
import type { MuxOptions } from '../../../src/core/mux/contract.ts';
import type { FrameMuxConnection } from '../../../src/core/mux/frame-mux-connection.ts';
import { MuxControlMessage, MuxControlType } from '../../../src/core/mux/mux-frame.ts';
import { MuxIdent } from '../../../src/core/mux/mux-protocol.ts';
import { Name } from '../../../src/core/name.ts';
import { StatusCode } from '../../../src/core/status.ts';
import { authoritySeed, holderSeed } from '../../support/identity/identity-vectors.ts';
import { StubVerifier, stubCredential } from '../../support/identity/stub-verifier.ts';
import {
  clockOf,
  elapseUntil,
  empty,
  expectCode,
  expectRejectsStatus,
  isControl,
  muxPair,
  quiet,
  type RawPeer,
  rawOptions,
  rawPair,
  rejection,
  takeIncoming,
  tracked,
} from './support.ts';

const holderKeys = await ed25519KeyPairFromSeed(holderSeed);
/** The holder of the holder-key credentials: RFC 8032 test 2. */
const holder: HolderKey = {
  publicKey: holderKeys.publicKey,
  sign: (message) => ed25519Sign(message, holderKeys.privateKey),
};
const otherKeys = await ed25519KeyPairFromSeed(authoritySeed);
/** Another key pair, not the one the credentials name. */
const otherHolder: HolderKey = {
  publicKey: otherKeys.publicKey,
  sign: (message) => ed25519Sign(message, otherKeys.privateKey),
};

function scope(right: Right, pattern: string): Scope {
  const name = Name.of(pattern);
  const prefix = pattern.endsWith('*') ? pattern.slice(0, -1) : undefined;
  return {
    right,
    pattern: name,
    matches: (other) => (prefix === undefined ? other.equals(name) : `${other}`.startsWith(prefix)),
  };
}

let serial = 0;

/** A stand-in credential the verifiers of this file know; unique bytes each. */
function credential(
  identity: string,
  options: { holderKey?: Uint8Array; scopes?: Scope[]; expiresAt?: number } = {},
): Credential {
  const scopes = options.scopes ?? [];
  const base = stubCredential({
    identity,
    holderKey: options.holderKey,
    expiresAt: options.expiresAt ?? 0,
    bytes: utf8Encode(`${identity}#${serial++}`),
  });
  return {
    ...base,
    scopes,
    allows: (right, name) =>
      scopes.some((s) => (s.right === right || s.right === Right.admin) && s.matches(name)),
  };
}

/** A bearer credential for `npc-7` (or [identity]). */
function bearer(identity = 'npc-7', expiresAt = 0): Credential {
  return credential(identity, {
    scopes: [scope(Right.open, 'npc'), scope(Right.register, 'npc')],
    expiresAt,
  });
}

/** A credential for `worker-a1` naming the holder's key. */
function held(identity = 'worker-a1'): Credential {
  return credential(identity, {
    holderKey: holder.publicKey,
    scopes: [scope(Right.register, 'worker-*')],
  });
}

/** A verifier that knows [credentials]; it counts its calls. */
function verifierFor(...credentials: Credential[]): StubVerifier {
  return new StubVerifier(...credentials);
}

/** Two connected muxes; [acceptorVerifier] checks the initiator's IDENT, [initiatorVerifier] the acceptor's. */
function identityPair(
  options: {
    acceptorVerifier?: StubVerifier;
    initiatorVerifier?: StubVerifier;
    identityTimeout?: number;
  } = {},
): [FrameMuxConnection, FrameMuxConnection] {
  const identityTimeout = options.identityTimeout ?? 5000;
  return muxPair({
    initiator: { ...quiet, identityVerifier: options.initiatorVerifier, identityTimeout },
    acceptor: { ...quiet, identityVerifier: options.acceptorVerifier, identityTimeout },
  });
}

/** The frame of a control message of [type] carrying [payload]. */
function control(type: MuxControlType, payload: Uint8Array): Uint8Array {
  return new MuxControlMessage(type, payload).toFrame().encode();
}

function filled(length: number, value: number): Uint8Array {
  return new Uint8Array(length).fill(value);
}

describe('connection identity', () => {
  test('bearer credential: identified without a proof', async () => {
    const credentialOfA = bearer();
    const [a, b] = identityPair({ acceptorVerifier: verifierFor(credentialOfA) });
    const requested = tracked(b.identityRequested);
    await a.identify(credentialOfA);
    const identity = await b.peerIdentified;
    expect(identity.identity).toBe('npc-7');
    expect(identity.kind).toBe(CredentialKind.node);
    expect(identity.provedPossession).toBe(false);
    expect(identity.intent).toHaveLength(0);
    expect(identity.credential).toBe(credentialOfA);
    expect(identity.allows(Right.open, Name.of('npc'))).toBe(true);
    expect(identity.allows(Right.open, Name.of('chat'))).toBe(false);
    expect(b.peerIdentity).toBe(identity);
    expect(a.peerIdentity).toBeUndefined();
    expect(requested.settled).toBe(true);
    expect(a.isOpen && b.isOpen).toBe(true);
    await a.close();
    await b.done;
  });

  test('holder-key credential with a proof and an intent', async () => {
    const credentialOfA = held();
    const [a, b] = identityPair({ acceptorVerifier: verifierFor(credentialOfA) });
    await a.identify(credentialOfA, { holderKey: holder, intent: hexBytes('01 02 03') });
    const identity = b.peerIdentity;
    expect(identity?.identity).toBe('worker-a1');
    expect(identity?.provedPossession).toBe(true);
    expect(hexString(identity?.intent ?? empty)).toBe('01 02 03');
    await a.close();
    await b.done;
  });

  test('both directions, independently and at once', async () => {
    const left = bearer('left');
    const right = held();
    const verifier = verifierFor(left, right);
    const [a, b] = identityPair({ acceptorVerifier: verifier, initiatorVerifier: verifier });
    await Promise.all([a.identify(left), b.identify(right, { holderKey: holder })]);
    expect(b.peerIdentity?.identity).toBe('left');
    expect(a.peerIdentity?.identity).toBe('worker-a1');
    expect(a.peerIdentity?.provedPossession).toBe(true);
    await a.close();
    await b.done;
  });

  test('a second valid IDENT replaces the first', async () => {
    const first = bearer('first');
    const second = bearer('second');
    const [a, b] = identityPair({ acceptorVerifier: verifierFor(first, second) });
    await a.identify(first);
    await a.identify(second);
    expect(b.peerIdentity?.identity).toBe('second');
    expect((await b.peerIdentified).identity).toBe('first');
    await a.close();
    await b.done;
  });

  test('frames after an IDENT wait for its verification', async () => {
    const credentialOfPeer = held();
    const [b, raw] = rawPair({
      muxIsInitiator: false,
      options: { ...rawOptions, identityVerifier: verifierFor(credentialOfPeer) },
    });
    const senderNonce = filled(32, 0x11);
    raw.sendBytes(control(MuxControlType.nonce, senderNonce));
    const answer = await raw.nextControl(MuxControlType.nonce);
    const intent = empty;
    const proof = await holder.sign(
      MuxIdent.proofMessage(senderNonce, answer.payload, intent, empty),
    );
    const seen: (string | undefined)[] = [];
    const opened = (async () => {
      for await (const channel of b.incoming) {
        seen.push(channel.connection.peerIdentity?.identity);
        return;
      }
    })();
    // IDENT and OPEN in one burst: the Ed25519 check takes a while, and the
    // OPEN must not overtake it.
    raw.sendBytes(
      control(
        MuxControlType.ident,
        new MuxIdent({ credential: credentialOfPeer.encode(), intent, proof }).encode(),
      ),
    );
    raw.send('12 02 00 00');
    await opened;
    expect(seen).toEqual(['worker-a1']);
    await b.close();
  });

  test('a replayed IDENT from another connection is rejected', async () => {
    // First connection: an honest identification, captured by a raw peer
    // that answers with its own nonce.
    const credentialOfA = held();
    const [a, first] = rawPair();
    const identifying = a.identify(credentialOfA, { holderKey: holder });
    const aNonce = await first.nextControl(MuxControlType.nonce);
    first.sendBytes(control(MuxControlType.nonce, filled(32, 0x22)));
    const ident = await first.nextControl(MuxControlType.ident);
    const ping = await first.nextControl(MuxControlType.ping);
    first.sendBytes(control(MuxControlType.pong, ping.payload));
    await identifying;
    await a.close();

    // Second connection: the attacker replays the nonce and the IDENT of
    // the first, but the receiver's nonce is a new one.
    const [b, raw] = rawPair({
      muxIsInitiator: false,
      options: { ...rawOptions, identityVerifier: verifierFor(credentialOfA) },
    });
    raw.sendBytes(control(MuxControlType.nonce, aNonce.payload));
    const bNonce = await raw.nextControl(MuxControlType.nonce);
    expect(hexString(bNonce.payload)).not.toBe(hexString(aNonce.payload));
    raw.sendBytes(control(MuxControlType.ident, ident.payload));
    const goAway = await raw.nextControl(MuxControlType.goAway);
    expectCode(goAway.goAwayStatus, StatusCode.unauthenticated);
    expect(goAway.goAwayStatus.reason).toBe('identification failed');
    expectCode(await b.done, StatusCode.unauthenticated);
    expect(b.peerIdentity).toBeUndefined();
  });

  test('a peer that echoes our NONCE cannot send our IDENT back to be identified as us', async () => {
    const admin = credential('admin-1', {
      holderKey: holder.publicKey,
      scopes: [scope(Right.admin, '*')],
    });
    // A node that checks identities identifies, naming nobody, to a peer
    // it dialled.
    const [a, m] = rawPair({
      options: { ...rawOptions, identityVerifier: verifierFor(admin) },
    });
    a.localIdentity = 'admin-1';
    const identifying = a.identify(admin, { holderKey: holder });
    identifying.catch(() => {});
    const nonce = await m.nextControl(MuxControlType.nonce);
    // The same nonce back: a proof over SWBIDENT || N || N || intent ||
    // receiver would verify both ways.
    m.sendBytes(control(MuxControlType.nonce, nonce.payload));
    const goAway = await m.nextControl(MuxControlType.goAway);
    expectCode(goAway.goAwayStatus, StatusCode.protocolError);
    await expectRejectsStatus(identifying, StatusCode.protocolError);
    expectCode(await a.done, StatusCode.protocolError);
    expect(a.peerIdentity).toBeUndefined();
  });

  test('a replayed bearer IDENT is accepted, by design', async () => {
    // A bearer credential is a password (wiki "Security considerations"):
    // its IDENT carries no proof, so nothing binds it to the nonces of the
    // connection it was sent on.
    const credentialOfA = bearer();
    const [a, first] = rawPair();
    const identifying = a.identify(credentialOfA);
    await first.nextControl(MuxControlType.nonce);
    first.sendBytes(control(MuxControlType.nonce, filled(32, 0x22)));
    const ident = await first.nextControl(MuxControlType.ident);
    const ping = await first.nextControl(MuxControlType.ping);
    first.sendBytes(control(MuxControlType.pong, ping.payload));
    await identifying;
    await a.close();

    const [b, raw] = rawPair({
      muxIsInitiator: false,
      options: { ...rawOptions, identityVerifier: verifierFor(credentialOfA) },
    });
    raw.sendBytes(control(MuxControlType.nonce, filled(32, 0x33)));
    await raw.nextControl(MuxControlType.nonce);
    raw.sendBytes(control(MuxControlType.ident, ident.payload));
    raw.sendBytes(control(MuxControlType.ping, hexBytes('07')));
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
    const verifier = verifierFor();
    const [b, raw] = rawPair({
      muxIsInitiator: false,
      options: { ...rawOptions, identityVerifier: verifier },
    });
    raw.sendBytes(control(MuxControlType.nonce, new Uint8Array(32)));
    await raw.nextControl(MuxControlType.nonce);
    for (let i = 0; i < 20; i++) {
      // Distinct, well-formed, unknown to the verifier.
      raw.sendBytes(
        control(
          MuxControlType.ident,
          new MuxIdent({ credential: bearer(`npc-${i}`).encode() }).encode(),
        ),
      );
    }
    const goAway = await raw.nextControl(MuxControlType.goAway);
    expectCode(goAway.goAwayStatus, StatusCode.unauthenticated);
    expectCode(await b.done, StatusCode.unauthenticated);
    expect(verifier.calls).toHaveLength(1);
    expect(b.peerIdentity).toBeUndefined();
  });

  describe('receiver binding', () => {
    /** A mux accepting IDENTs as [identity] (undefined: no credential). */
    function receiverPair(
      verifier: StubVerifier,
      identity: string | undefined = 't',
      requireNamedIdent = false,
    ): [FrameMuxConnection, RawPeer] {
      const [b, raw] = rawPair({
        muxIsInitiator: false,
        options: { ...rawOptions, identityVerifier: verifier, requireNamedIdent },
      });
      b.localIdentity = identity;
      return [b, raw];
    }

    /**
     * a identifies to a malicious peer `m` (naming it), which relays the
     * handshake live to the honest node `t`: the nonces are passed both
     * ways, and the IDENT goes to `t` as [rewrite] makes it.
     */
    async function relay(rewrite: (ident: MuxIdent) => MuxIdent): Promise<void> {
      const credentialOfA = held();
      const [a, toA] = rawPair();
      const [t, toT] = receiverPair(verifierFor(credentialOfA));
      const identifying = a.identify(credentialOfA, { holderKey: holder, receiver: 'm' });
      identifying.catch(() => {});
      const aNonce = await toA.nextControl(MuxControlType.nonce);
      toT.sendBytes(control(MuxControlType.nonce, aNonce.payload));
      const tNonce = await toT.nextControl(MuxControlType.nonce);
      toA.sendBytes(control(MuxControlType.nonce, tNonce.payload));
      const ident = await toA.nextControl(MuxControlType.ident);
      toT.sendBytes(
        control(MuxControlType.ident, rewrite(MuxIdent.decode(ident.payload)).encode()),
      );
      const goAway = await toT.nextControl(MuxControlType.goAway);
      expectCode(goAway.goAwayStatus, StatusCode.unauthenticated);
      expect(goAway.goAwayStatus.reason).toBe('identification failed');
      expectCode(await t.done, StatusCode.unauthenticated);
      expect(t.peerIdentity).toBeUndefined();
      await a.close();
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
            intent: Uint8Array.of(...ident.intent, ...ident.receiver),
            proof: ident.proof,
          }),
      );
    });

    test('the named receiver accepts; another refuses', async () => {
      for (const make of [held, bearer]) {
        const credentialOfA = make();
        const verifier = verifierFor(credentialOfA);
        const [a, b] = identityPair({ acceptorVerifier: verifier });
        b.localIdentity = 'gate';
        await a.identify(credentialOfA, { holderKey: holder, receiver: 'gate' });
        expect(b.peerIdentity?.identity).toBe(credentialOfA.identity);
        expect(b.peerIdentity?.receiver).toBe('gate');
        await a.close();
        await b.done;

        const [c, d] = identityPair({ acceptorVerifier: verifier });
        d.localIdentity = 'gate';
        await expectRejectsStatus(
          c.identify(credentialOfA, { holderKey: holder, receiver: 'other' }),
          StatusCode.unauthenticated,
        );
        expectCode(await d.done, StatusCode.unauthenticated);
        await c.done;
      }
    });

    test('a receiver without an identity refuses a named IDENT', async () => {
      const credentialOfA = bearer();
      const [a, b] = identityPair({ acceptorVerifier: verifierFor(credentialOfA) });
      await expectRejectsStatus(
        a.identify(credentialOfA, { receiver: 'gate' }),
        StatusCode.unauthenticated,
      );
      expectCode(await b.done, StatusCode.unauthenticated);
      await a.done;
    });

    test('an unnamed IDENT is accepted unless requireNamedIdent', async () => {
      const credentialOfA = bearer();
      const verifier = verifierFor(credentialOfA);
      const [a, b] = identityPair({ acceptorVerifier: verifier });
      b.localIdentity = 'gate';
      await a.identify(credentialOfA);
      expect(b.peerIdentity?.identity).toBe('npc-7');
      expect(b.peerIdentity?.receiver).toBe('');
      await a.close();
      await b.done;

      const strict: Partial<MuxOptions> = {
        ...quiet,
        identityVerifier: verifier,
        requireNamedIdent: true,
      };
      const [c, d] = muxPair({ acceptor: strict });
      d.localIdentity = 'gate';
      await expectRejectsStatus(c.identify(credentialOfA), StatusCode.unauthenticated);
      expectCode(await d.done, StatusCode.unauthenticated);
      await c.done;

      const [e, f] = muxPair({ acceptor: strict });
      f.localIdentity = 'gate';
      await e.identify(credentialOfA, { receiver: 'gate' });
      expect(f.peerIdentity?.receiver).toBe('gate');
      await e.close();
      await f.done;
    });
  });

  test('a bearer IDENT with a proof is rejected', async () => {
    const credentialOfPeer = bearer();
    const [b, raw] = rawPair({
      muxIsInitiator: false,
      options: { ...rawOptions, identityVerifier: verifierFor(credentialOfPeer) },
    });
    raw.sendBytes(control(MuxControlType.nonce, new Uint8Array(32)));
    await raw.nextControl(MuxControlType.nonce);
    raw.sendBytes(
      control(
        MuxControlType.ident,
        new MuxIdent({ credential: credentialOfPeer.encode(), proof: new Uint8Array(64) }).encode(),
      ),
    );
    expectCode(await b.done, StatusCode.unauthenticated);
  });

  for (const [name, make] of [
    // Issued two hours ago for an hour.
    ['an expired credential', () => bearer('npc-7', Math.floor(Date.now() / 1000) - 3600)],
    // Neither is known to the verifier, as a credential of an unknown key
    // or with a forged signature is not.
    ['an unknown key id', () => bearer()],
    ['a forged signature', () => bearer()],
  ] as const) {
    test(`${name} ends the connection with GOAWAY UNAUTHENTICATED`, async () => {
      const made = make();
      // The verifier knows the expired one, and another credential only for
      // the other two.
      const verifier = verifierFor(name === 'an expired credential' ? made : bearer());
      const [a, b] = identityPair({ acceptorVerifier: verifier });
      const peerStatus = a.peerGoAwayStatus;
      await expectRejectsStatus(a.identify(made), StatusCode.unauthenticated);
      expectCode(await peerStatus, StatusCode.unauthenticated);
      expectCode(await b.done, StatusCode.unauthenticated);
      expect(b.peerIdentity).toBeUndefined();
      await a.done;
    });
  }

  test('without a verifier an IDENT is ignored', async () => {
    const [a, b] = identityPair();
    await a.identify(bearer());
    expect(b.peerIdentity).toBeUndefined();
    expect(b.isOpen).toBe(true);
    // A NONCE is still answered: both sides have one.
    await b.identify(bearer('other'));
    await a.close();
    await b.done;
  });

  test('no NONCE from the peer within the timeout', async () => {
    const [a, raw] = rawPair({ options: { ...rawOptions, identityTimeout: 50 } });
    const identifying = a.identify(bearer());
    expect(await elapseUntil(clockOf(a), identifying)).toBe(50);
    await expectRejectsStatus(identifying, StatusCode.deadlineExceeded);
    expect(isControl(await raw.next(), MuxControlType.nonce)).toBe(true);
    await a.close();

    // An explicit timeout wins over the option.
    const [b] = rawPair({ options: { ...rawOptions, identityTimeout: 20000 } });
    const bounded = b.identify(bearer(), { timeout: 20 });
    expect(await elapseUntil(clockOf(b), bounded)).toBe(20);
    await expectRejectsStatus(bounded, StatusCode.deadlineExceeded);
    await b.close();
  });

  test('no confirmation within the timeout', async () => {
    const [a, raw] = rawPair({ options: { ...rawOptions, identityTimeout: 100 } });
    const identifying = a.identify(bearer());
    await raw.nextControl(MuxControlType.nonce);
    raw.sendBytes(control(MuxControlType.nonce, new Uint8Array(32)));
    await raw.nextControl(MuxControlType.ident);
    await raw.nextControl(MuxControlType.ping);
    expect(await elapseUntil(clockOf(a), identifying)).toBeLessThanOrEqual(100);
    await expectRejectsStatus(identifying, StatusCode.deadlineExceeded);
    await a.close();
  });

  test('the connection ending fails identify with its status', async () => {
    const [a, raw] = rawPair();
    const identifying = a.identify(bearer());
    await raw.nextControl(MuxControlType.nonce);
    await raw.close();
    await expectRejectsStatus(identifying, StatusCode.connectionLost);
    await expectRejectsStatus(a.identify(bearer()), StatusCode.failedPrecondition);
  });

  test('argument checks', async () => {
    // identify is async: the misuse rejects (with a RangeError, Dart's
    // ArgumentError) rather than throwing.
    const [a, b] = identityPair();
    const credentialOfA = held();
    expect(await rejection(a.identify(credentialOfA))).toBeInstanceOf(RangeError);
    expect(await rejection(a.identify(credentialOfA, { holderKey: otherHolder }))).toBeInstanceOf(
      RangeError,
    );
    expect(await rejection(a.identify(bearer(), { intent: new Uint8Array(65) }))).toBeInstanceOf(
      RangeError,
    );
    expect(await rejection(a.identify(bearer(), { receiver: 'x'.repeat(256) }))).toBeInstanceOf(
      RangeError,
    );
    expect(bytesEqual(holder.publicKey, otherHolder.publicKey)).toBe(false);
    await a.close();
    await b.done;
  });

  // The envelope rules hold whether or not the receiver can verify
  // credentials: a side without a verifier ignores only well-formed
  // IDENTs, so a malformed one does not tell whether it checks identity.
  for (const verifying of [true, false]) {
    describe(`protocol errors ${verifying ? 'with' : 'without'} a verifier`, () => {
      async function expectProtocolError(frames: Uint8Array[]): Promise<void> {
        const [b, raw] = rawPair({
          muxIsInitiator: false,
          options: verifying ? { ...rawOptions, identityVerifier: verifierFor() } : rawOptions,
        });
        for (const frame of frames) {
          raw.sendBytes(frame);
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
          control(MuxControlType.ident, new MuxIdent({ credential: bearer().encode() }).encode()),
        ]);
      });

      test('a truncated IDENT', async () => {
        await expectProtocolError([
          control(MuxControlType.nonce, new Uint8Array(32)),
          control(MuxControlType.ident, hexBytes('05 00 01 02')),
        ]);
      });

      test('an IDENT with an intent over 64 bytes', async () => {
        const bytes = bearer().encode();
        await expectProtocolError([
          control(MuxControlType.nonce, new Uint8Array(32)),
          control(
            MuxControlType.ident,
            Uint8Array.of(
              bytes.length & 0xff,
              bytes.length >> 8,
              ...bytes,
              65,
              ...new Uint8Array(65),
              0,
            ),
          ),
        ]);
      });
    });
  }
});

test('the harness: incoming channels of an identified peer', async () => {
  // Not a Dart test: a channel opened right after identify() resolved sees
  // the identity at the peer.
  const credentialOfA = bearer();
  const [a, b] = identityPair({ acceptorVerifier: verifierFor(credentialOfA) });
  const channels = takeIncoming(b);
  await a.identify(credentialOfA);
  a.open(empty);
  await a.ping();
  expect(channels).toHaveLength(1);
  expect(channels[0]?.connection.peerIdentity?.identity).toBe('npc-7');
  await a.close();
});
