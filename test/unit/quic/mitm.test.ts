// A man in the middle between two Switchboard nodes over the QUIC binding,
// on the fake provider: the group "a man in the middle" of the identity
// tests of `switchboard_quic`'s `test/switchboard_test.dart`. The relay
// accepts the victim's sessions at `quic://mitm:4433`, dials the real
// listener for each, and copies every stream both ways; it may clear the
// EXPORTER flag of both HELLOs. Each relayed session has an exporter of its
// own, so an exporter-bound IDENT relayed this way never verifies, and the
// identity rules make sure that stripping the flags (forcing nonce-bound
// proofs) fails too; only without the rules on either side does the
// identification pass through the relay.
import { beforeAll, describe, expect, test } from 'vitest';
import { ChannelAddress } from '../../../src/core/address/index.ts';
import { type Credential, type HolderKey, Right } from '../../../src/core/identity/contract.ts';
import { Ed25519HolderKey } from '../../../src/core/identity/holder-key.ts';
import { Name } from '../../../src/core/name.ts';
import { type QuicStream, QuicStreamReset } from '../../../src/core/quic/contract.ts';
import { QuicHello } from '../../../src/core/quic/hello.ts';
import { QuicIdentityRules } from '../../../src/core/quic/quic-identity-rules.ts';
import {
  QuicSwitchboardBinding,
  QuicSwitchboardListener,
} from '../../../src/core/quic/quic-switchboard.ts';
import { StatusCode } from '../../../src/core/status.ts';
import { ChannelPolicies } from '../../../src/core/switchboard/channel-policy.ts';
import type { ChannelHandler } from '../../../src/core/switchboard/contract.ts';
import type { Switchboard, SwitchboardOptions } from '../../../src/core/switchboard/switchboard.ts';
import { FakeQuicNetwork } from '../../../src/testing/fake-quic-provider.ts';
import { issueCredential, TestVerifier } from '../../support/node/fake-identity.ts';
import { node as harnessNode, onCleanup, tagOf } from '../../support/node/harness.ts';
import { bytes, expectCode, fast } from './harness.ts';

const npc = Name.of('npc');
const npcAddress = (): ChannelAddress => new ChannelAddress({ type: npc });
const mitmEndpoint = 'quic://mitm:4433';

let clientKey: HolderKey;
let serverKey: HolderKey;
let clientCredential: Credential;
let serverCredential: Credential;

beforeAll(async () => {
  clientKey = await Ed25519HolderKey.generate();
  serverKey = await Ed25519HolderKey.generate();
  clientCredential = await credential('npc-7', clientKey);
  serverCredential = await credential('server-1', serverKey);
});

/** A node credential of [identity] for [key], allowed to open anything. */
function credential(identity: string, key: HolderKey): Promise<Credential> {
  return issueCredential({ identity, holderKey: key.publicKey, scopes: [[Right.open, '*']] });
}

/** A node closed after the test, with the binding tests' options. */
function node(options: SwitchboardOptions = {}): Switchboard {
  return harnessNode({
    identityTimeout: 2000,
    connectTimeout: 5000,
    ...options,
    muxOptions: { ...fast, ...options.muxOptions },
  });
}

/** Answers with the channel's type and the peer's identity, and closes. */
const answer: ChannelHandler = (incoming) => {
  const who = incoming.peerIdentity?.identity ?? 'nobody';
  void incoming.channel.send(bytes(`${incoming.address.type} for ${who}`)).catch(() => {});
  void incoming.channel.close().catch(() => {});
};

/**
 * Relays every session dialled to `quic://mitm:4433` to [target], stream by stream, clearing the
 * EXPORTER flags of both HELLOs when [strip].
 */
function mitm(network: FakeQuicNetwork, target: string, strip: boolean): void {
  const listener = network.listen(mitmEndpoint);
  onCleanup(() => listener.close());
  const connector = network.connector();
  // The control stream is the first stream either way: its HELLO flags at offset 23 from the
  // initiator (after the OPEN record), 13 from the acceptor.
  const pump = async (from: QuicStream, to: QuicStream, flagAt?: number): Promise<void> => {
    let offset = 0;
    try {
      for (;;) {
        const chunk = await from.read(1 << 16);
        if (chunk === undefined) {
          await to.finish();
          return;
        }
        const copy = chunk.slice();
        if (strip && flagAt !== undefined) {
          const at = flagAt - offset;
          if (at >= 0 && at < copy.length) {
            copy[at] = (copy[at] as number) & ~QuicHello.flagExporter;
          }
        }
        offset += chunk.length;
        await to.write(copy);
      }
    } catch (error) {
      if (error instanceof QuicStreamReset) {
        to.reset(error.code ?? 0);
      }
      // Otherwise either session ended.
    }
  };
  void (async () => {
    for await (const victim of listener.sessions) {
      void (async () => {
        const upstream = await connector.connect(target, { alpn: ['switchboard'] });
        void upstream.done.then((end) => victim.close(end.code ?? 0, end.reason).catch(() => {}));
        void victim.done.then((end) => upstream.close(end.code ?? 0, end.reason).catch(() => {}));
        void (async () => {
          let first = true;
          for await (const down of victim.incomingStreams) {
            const control = first;
            first = false;
            const up = await upstream.openStream();
            void pump(down, up, control ? 23 : undefined);
            void pump(up, down, control ? 13 : undefined);
          }
        })().catch(() => {});
        void (async () => {
          for await (const up of upstream.incomingStreams) {
            const down = await victim.openStream();
            void pump(up, down);
            void pump(down, up);
          }
        })().catch(() => {});
      })().catch(() => {});
    }
  })().catch(() => {});
}

/** The real listener at `quic://[host]:4433`: a node identified as `server-1`, admitting what the scoped policy admits. */
async function target(
  network: FakeQuicNetwork,
  host: string,
  identityRules: QuicIdentityRules = QuicIdentityRules.listener,
): Promise<string> {
  const server = node({
    verifier: new TestVerifier(),
    credential: serverCredential,
    holderKey: serverKey,
  });
  server.registerService(npc, answer);
  const listener = await QuicSwitchboardListener.start(
    server,
    network.listen(`quic://${host}:4433`),
    { policy: ChannelPolicies.scoped(), identityRules },
  );
  onCleanup(() => listener.close());
  return listener.endpoint;
}

/** The victim: a node identified as `npc-7` that expects to reach `server-1`. */
function victim(network: FakeQuicNetwork, identityRules?: QuicIdentityRules): Switchboard {
  return node({
    bindings: [
      new QuicSwitchboardBinding(
        network.connector(),
        identityRules === undefined ? {} : { identityRules },
      ),
    ],
    verifier: new TestVerifier(),
    credential: clientCredential,
    holderKey: clientKey,
    expectedIdentityFor: () => 'server-1',
  });
}

describe('identity', () => {
  describe('a man in the middle', () => {
    test('relaying the HELLOs as they are: the exporters differ, the identification fails', async () => {
      const network = new FakeQuicNetwork();
      const real = await target(network, 'real-1');
      mitm(network, real, false);
      const client = victim(network);
      // The victim reaches the real listener directly.
      expect(await tagOf(await client.openChannelAt(real, npcAddress()))).toBe('npc for npc-7');
      await expectCode(
        client.openChannelAt(mitmEndpoint, npcAddress()),
        StatusCode.unauthenticated,
      );
    });

    test('stripping the EXPORTER flags makes the identification fail, never succeed', async () => {
      const network = new FakeQuicNetwork();
      const real = await target(network, 'real-2');
      mitm(network, real, true);
      const client = victim(network);
      await expectCode(
        client.openChannelAt(mitmEndpoint, npcAddress()),
        StatusCode.unauthenticated,
      );
    });

    test('without the rules on either side, stripping the flags relays the identification: what the rules prevent', async () => {
      const network = new FakeQuicNetwork();
      const real = await target(network, 'real-3', QuicIdentityRules.none);
      mitm(network, real, true);
      const client = victim(network, QuicIdentityRules.none);
      expect(await tagOf(await client.openChannelAt(mitmEndpoint, npcAddress()))).toBe(
        'npc for npc-7',
      );
    });
  });
});
