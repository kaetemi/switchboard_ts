// Switchboard nodes over the QUIC binding, end to end over the fake
// provider: QuicSwitchboardBinding and QuicSwitchboardListener, the dial's
// bounds and stack settings, identity under the rules of the binding page,
// and the ends of connections. A port of `switchboard_quic`'s
// `test/switchboard_test.dart` and `review_switchboard_test.dart`, as far as
// the TS core reaches: the cases built on Talk (bulk payloads overtaking
// their parents, whenWritable through Talk), the proxy and SlotChannel wait
// for those layers and are listed as todo; the others carry their payloads
// on raw channels instead of Talk requests.
import { beforeAll, describe, expect, test } from 'vitest';
import { ChannelAddress, ServiceAddress } from '../../../src/core/address/index.ts';
import { ReconnectingClient } from '../../../src/core/client/reconnecting-client.ts';
import { type HolderKey, Right } from '../../../src/core/identity/contract.ts';
import type { MuxChannel, MuxOptions } from '../../../src/core/mux/contract.ts';
import { Name } from '../../../src/core/name.ts';
import { serviceRecord } from '../../../src/core/naming/naming-protocol.ts';
import { QuicBindingChannel } from '../../../src/core/quic/quic-binding-channel.ts';
import type { QuicBindingConnection } from '../../../src/core/quic/quic-binding-connection.ts';
import { quicConnector } from '../../../src/core/quic/quic-dial.ts';
import { QuicIdentityRules } from '../../../src/core/quic/quic-identity-rules.ts';
import {
  QuicSwitchboardBinding,
  QuicSwitchboardListener,
  type QuicSwitchboardListenerOptions,
} from '../../../src/core/quic/quic-switchboard.ts';
import { Status, StatusCode } from '../../../src/core/status.ts';
import { ChannelPolicies } from '../../../src/core/switchboard/channel-policy.ts';
import type { ChannelHandler } from '../../../src/core/switchboard/contract.ts';
import { StaticResolver } from '../../../src/core/switchboard/resolver.ts';
import type { Switchboard, SwitchboardOptions } from '../../../src/core/switchboard/switchboard.ts';
import {
  type FakeEndConfig,
  type FakeQuicConnector,
  FakeQuicNetwork,
  FakeQuicSession,
} from '../../../src/testing/fake-quic-provider.ts';
import { FakeVerifier, fakeCredential } from '../../support/node/fake-identity.ts';
import { node as harnessNode, onCleanup, tagOf } from '../../support/node/harness.ts';
import {
  bytes,
  concat,
  expectCode,
  expectStatus,
  fast,
  limit,
  pattern,
  settle,
  text,
  until,
  within,
} from './harness.ts';
import { generateHolderKey } from './identity-fixtures.ts';

const echo = Name.of('echo');
const npc = Name.of('npc');

let clientKey: HolderKey;
let serverKey: HolderKey;

beforeAll(async () => {
  clientKey = await generateHolderKey();
  serverKey = await generateHolderKey();
});

/** A node credential of [identity] for [key], allowed to open anything. */
function credential(identity: string, key: HolderKey) {
  return fakeCredential({ identity, holderKey: key.publicKey, scopes: [[Right.open, '*']] });
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

/** A listener of [server] at `quic://[host]:4433`, closed after the test. */
async function listen(
  network: FakeQuicNetwork,
  server: Switchboard,
  host: string,
  options: QuicSwitchboardListenerOptions & { config?: Partial<FakeEndConfig> } = {},
): Promise<QuicSwitchboardListener> {
  const listener = await QuicSwitchboardListener.start(
    server,
    network.listen(`quic://${host}:4433`, options.config),
    options,
  );
  onCleanup(() => listener.close());
  return listener;
}

/** Echoes every chunk of a channel back on it. */
const echoService: ChannelHandler = (incoming) => {
  const channel = incoming.channel;
  void (async () => {
    try {
      for await (const chunk of channel.readable) {
        await channel.send(chunk);
      }
    } catch {
      // The channel ended.
    }
  })();
};

/** Answers with the channel's type and the peer's identity, and closes. */
const answer: ChannelHandler = (incoming) => {
  const who = incoming.peerIdentity?.identity ?? 'nobody';
  void incoming.channel.send(bytes(`${incoming.address.type} for ${who}`)).catch(() => {});
  void incoming.channel.close().catch(() => {});
};

/** Sends [payload] on [channel] and reads until as many bytes came back. */
async function exchange(channel: MuxChannel, payload: Uint8Array, ms = limit): Promise<Uint8Array> {
  const reader = channel.readable.getReader();
  // Without Talk a send is one subframe: at most the peer's record limit.
  const sending = (async () => {
    for (let at = 0; at < payload.length || at === 0; at += 16384) {
      await channel.send(payload.subarray(at, at + 16384));
    }
  })();
  const chunks: Uint8Array[] = [];
  let received = 0;
  try {
    while (received < payload.length) {
      const result = await within(reader.read(), ms);
      if (result.done) {
        break;
      }
      chunks.push(result.value);
      received += result.value.length;
    }
    await within(sending, ms);
  } finally {
    reader.releaseLock();
  }
  return concat(chunks);
}

/** Echoes [payload] on [channel], then closes it. */
async function echoed(channel: MuxChannel, payload: Uint8Array, ms = limit): Promise<Uint8Array> {
  const reply = await exchange(channel, payload, ms);
  await within(channel.close());
  return reply;
}

const echoAddress = (): ChannelAddress => new ChannelAddress({ type: echo });
const npcAddress = (): ChannelAddress => new ChannelAddress({ type: npc });

describe('two nodes', () => {
  test('a request and its reply, each channel on a stream of its own, pooled by endpoint', async () => {
    const network = new FakeQuicNetwork();
    const server = node();
    server.registerService(echo, echoService);
    const listener = await listen(network, server, 'echo-1');
    expect(server.listeningEndpoints).toContain(listener.endpoint);
    expect(server.isOwnEndpoint('quic://echo-1:4433')).toBe(true);
    const binding = new QuicSwitchboardBinding(network.connector());
    const client = node({ bindings: [binding] });
    expect(client.bindingSchemes).toEqual(new Set(['quic', 'https']));
    const channel = await client.openChannelAt(listener.endpoint, echoAddress());
    expect(channel).toBeInstanceOf(QuicBindingChannel);
    expect(text(await echoed(channel, bytes('hello')))).toBe('hello');
    const again = await client.openChannelAt('QUIC://Echo-1:4433#echo', echoAddress());
    expect(text(await echoed(again, bytes('again')))).toBe('again');
    expect(binding.dialled).toHaveLength(1);
    const connection = binding.dialled[0] as QuicBindingConnection;
    expect(connection.isInitiator).toBe(true);
    expect(connection.exporterBound).toBe(true);
    expect(listener.accepted[0]?.peerHello).toBeDefined();
    // Raw QUIC: the session was dialled with the binding's ALPN and the
    // node's windows.
    const config = (binding.connector as FakeQuicConnector).configs[0];
    expect(config?.alpn).toEqual(['switchboard']);
    expect(config?.streamReceiveWindow).toBe(client.muxOptions.initialWindow);
    expect(config?.verifyCertificate).toBe(false);
  });

  test.todo('a 3 MiB Talk bulk payload each way, its streams overtaking the parent (needs Talk)');

  test('many channels at once over jittered streams: beyond the channels a listener states the pool opens another connection, and a burst of opens waits for stream credit', async () => {
    const network = new FakeQuicNetwork({
      delay: (info, fromInitiator) =>
        (((info.ordinal * 7919 + (fromInitiator ? 13 : 0)) % 97) * 50) / 1000,
    });
    const server = node();
    server.registerService(echo, echoService);
    const listener = await listen(network, server, 'many-1', { config: { maxStreams: 101 } });
    const binding = new QuicSwitchboardBinding(network.connector());
    const client = node({ bindings: [binding] });
    // 120 channels kept open: beyond the 100 the listener states, the pool
    // dials another connection.
    const channels: MuxChannel[] = [];
    for (let i = 0; i < 120; i++) {
      channels.push(await client.openChannelAt(listener.endpoint, echoAddress()));
      if (i === 0) {
        await until(() => binding.dialled[0]?.peerHello !== undefined);
      }
    }
    expect(binding.dialled[0]?.peerMaxChannels).toBe(100);
    expect(binding.dialled).toHaveLength(2);
    // Exchanges on all of them at once; larger payloads on the channels of
    // the second connection.
    const results = await within(
      Promise.all(
        channels.map(async (channel, i) => {
          const payload = pattern(i >= 100 ? 70000 + i : 100 + i, i);
          const reply = await exchange(channel, payload, 60_000);
          return reply.length === payload.length && reply.every((b, j) => b === payload[j]);
        }),
      ),
      60_000,
    );
    expect(results.every((ok) => ok)).toBe(true);
    await Promise.all(channels.map((channel) => channel.close()));
    // As many opened at once on a pooled connection wait for stream credit
    // instead of failing.
    const burst = await within(
      Promise.all(
        Array.from({ length: 150 }, async (_, i) => {
          const channel = await client.openChannelAt(listener.endpoint, echoAddress());
          const reply = await echoed(channel, pattern(10, i), 60_000);
          return reply.every((b, j) => b === pattern(10, i)[j]) && reply.length === 10;
        }),
      ),
      60_000,
    );
    expect(burst.every((ok) => ok)).toBe(true);
  });

  test('whenWritable paces a sender at the stream window', async () => {
    const network = new FakeQuicNetwork();
    const server = node();
    let served: MuxChannel | undefined;
    server.registerService(echo, (incoming) => {
      served = incoming.channel;
    });
    const listener = await listen(network, server, 'pace-1', {
      config: { streamWindow: 64 * 1024 },
    });
    const client = node({ bindings: [new QuicSwitchboardBinding(network.connector())] });
    const channel = await client.openChannelAt(listener.endpoint, echoAddress());
    const count = 400;
    let sent = 0;
    const publisher = (async () => {
      for (let i = 0; i < count; i++) {
        await channel.whenWritable;
        void channel.send(new Uint8Array(1000)).catch(() => {});
        sent++;
      }
    })();
    await until(() => served !== undefined);
    await settle(100);
    // The receiver takes nothing: the stack's window, plus the record being
    // read, is all the sender got out.
    expect(channel.isWritable).toBe(false);
    expect(sent).toBeLessThan(80);
    const stalled = sent;
    await settle(100);
    expect(sent).toBe(stalled);
    let received = 0;
    const reading = (async () => {
      for await (const chunk of (served as MuxChannel).readable) {
        received += chunk.length;
        if (received === count * 1000) {
          return;
        }
      }
    })();
    await within(publisher);
    await within(reading);
    expect(received).toBe(count * 1000);
  });

  test('https endpoints go to the connector, which may not dial them: the node moves to the next endpoint', async () => {
    const network = new FakeQuicNetwork();
    const server = node();
    server.registerService(echo, echoService, { instance: 1 });
    const listener = await listen(network, server, 'both-1');
    const resolver = new StaticResolver([
      serviceRecord(new ServiceAddress(echo, 1), {
        endpoints: ['https://both-1/sb', listener.endpoint],
      }),
    ]);
    onCleanup(() => resolver.close());
    const binding = new QuicSwitchboardBinding(network.connector());
    const client = node({ bindings: [binding], resolver });
    await expectCode(client.connect('https://both-1/sb'), StatusCode.unimplemented);
    const channel = await client.openChannel(new ServiceAddress(echo));
    expect(text(await echoed(channel, bytes('fell back')))).toBe('fell back');
    // WebTransport endpoints compare with their path, 443 by default.
    expect(binding.normalise('HTTPS://Hub.Example/sb#x')).toBe('https://hub.example:443/sb');
    expect(binding.normalise('quic://Hub:9/x')).toBe('quic://hub:9');
  });

  test('a quic endpoint without a port is refused, an unreachable one is unavailable', async () => {
    const client = node({
      bindings: [new QuicSwitchboardBinding(new FakeQuicNetwork().connector())],
    });
    await expectCode(client.connect('quic://nowhere'), StatusCode.invalidArgument);
    await expectCode(client.connect('quic://nowhere:1'), StatusCode.unavailable);
  });
});

describe('dialling', () => {
  const settings = (options: Partial<MuxOptions>, timeout: number) => ({
    options: { ...fast, ...options } as MuxOptions,
    timeout,
    localIdentity: undefined,
    signal: undefined,
  });

  test('a connect that outlasts the dial timeout fails UNAVAILABLE, and the session it yields later is closed CANCELLED', async () => {
    const network = new FakeQuicNetwork();
    const server = node();
    const listener = await listen(network, server, 'slow-1');
    const connector = network.connector();
    let open = (): void => {};
    connector.connectGate = new Promise((resolve) => {
      open = resolve;
    });
    const binding = new QuicSwitchboardBinding(connector);
    // A direct caller, without a node's own timeout around the dial.
    const error = await binding
      .dial(listener.endpoint, settings(server.muxOptions, 100))
      .catch((e: unknown) => e);
    expect(String(error)).toContain('not connected within 100 ms');
    expect(connector.configs[0]?.handshakeTimeout).toBe(100);
    expect(network.pairs).toHaveLength(0);
    // The handshake completes after all: its session is closed at once.
    open();
    await until(() => network.pairs.length > 0);
    const [client, acceptor] = network.pairs[0] as [FakeQuicSession, FakeQuicSession];
    const end = await within(client.done);
    expect(client.closes).toEqual([[StatusCode.cancelled, 'dial abandoned']]);
    expect([end.code, end.byPeer]).toEqual([StatusCode.cancelled, false]);
    expect((await within(acceptor.done)).code).toBe(StatusCode.cancelled);
    expect(binding.dialled).toHaveLength(0);
  });

  test('through a node: its connect timeout bounds the dial, and the late session is closed CANCELLED', async () => {
    const network = new FakeQuicNetwork();
    const server = node();
    server.registerService(echo, echoService);
    const listener = await listen(network, server, 'slow-2');
    const connector = network.connector();
    let open = (): void => {};
    connector.connectGate = new Promise((resolve) => {
      open = resolve;
    });
    const binding = new QuicSwitchboardBinding(connector);
    const client = node({ bindings: [binding], connectTimeout: 100 });
    await expectCode(client.connect(listener.endpoint), StatusCode.unavailable);
    expect(connector.configs[0]?.handshakeTimeout).toBe(100);
    open();
    await until(() => network.pairs.length > 0);
    const [session] = network.pairs[0] as [FakeQuicSession, FakeQuicSession];
    await within(session.done);
    expect(session.closes).toEqual([[StatusCode.cancelled, 'dial abandoned']]);
    expect(binding.dialled).toHaveLength(0);
    // The next dial, not held, connects.
    connector.connectGate = undefined;
    const channel = await client.openChannelAt(listener.endpoint, echoAddress());
    expect(text(await echoed(channel, bytes('late')))).toBe('late');
    expect(binding.dialled).toHaveLength(1);
  });

  test('without a dial timeout the connect is not bounded, nor its handshake', async () => {
    const network = new FakeQuicNetwork();
    const server = node();
    const listener = await listen(network, server, 'slow-3');
    const connector = network.connector();
    let open = (): void => {};
    connector.connectGate = new Promise((resolve) => {
      open = resolve;
    });
    const binding = new QuicSwitchboardBinding(connector);
    const dialling = binding.dial(listener.endpoint, settings(server.muxOptions, 0));
    await settle(150);
    expect(connector.configs[0]?.handshakeTimeout).toBe(0);
    open();
    const connection = await within(dialling);
    onCleanup(() => connection.close());
    expect(binding.dialled).toEqual([connection]);
  });

  test("the stack keep-alive is the binding's: a third of the idle timeout by default, none without one, or as given, whatever the control PING interval", async () => {
    const network = new FakeQuicNetwork();
    const dialledWith = async (binding: QuicSwitchboardBinding, options: Partial<MuxOptions>) => {
      // Nothing listens: the dial fails, and the connector kept the
      // configuration it was asked for.
      await expectCode(
        binding.dial('quic://nowhere:1', settings(options, limit)),
        StatusCode.unavailable,
      );
      return (binding.connector as FakeQuicConnector).configs[0];
    };
    for (const interval of [undefined, 1000, 10_000, 300_000]) {
      const options = { keepAliveInterval: interval };
      const byDefault = await dialledWith(new QuicSwitchboardBinding(network.connector()), options);
      expect(byDefault?.idleTimeout).toBe(30_000);
      expect(byDefault?.keepAlive).toBe(10_000);
      expect(byDefault?.handshakeTimeout).toBe(limit);
      const shorter = await dialledWith(
        new QuicSwitchboardBinding(network.connector(), { idleTimeout: 3000 }),
        options,
      );
      expect(shorter?.keepAlive).toBe(1000);
      const noIdle = await dialledWith(
        new QuicSwitchboardBinding(network.connector(), { idleTimeout: 0 }),
        options,
      );
      expect(noIdle?.idleTimeout).toBe(0);
      expect(noIdle?.keepAlive).toBe(0);
      const given = await dialledWith(
        new QuicSwitchboardBinding(network.connector(), { keepAlive: 4000 }),
        options,
      );
      expect(given?.keepAlive).toBe(4000);
      const off = await dialledWith(
        new QuicSwitchboardBinding(network.connector(), { keepAlive: 0 }),
        options,
      );
      expect(off?.keepAlive).toBe(0);
      expect(off?.idleTimeout).toBe(30_000);
    }
  });

  test('a negative idle timeout or keep-alive is a RangeError', () => {
    const connector = new FakeQuicNetwork().connector();
    expect(() => new QuicSwitchboardBinding(connector, { idleTimeout: -1 })).toThrow(RangeError);
    expect(() => new QuicSwitchboardBinding(connector, { keepAlive: -1 })).toThrow(RangeError);
  });
});

describe('identity', () => {
  test('nodes identify both ways with exporter-bound proofs, and the scoped policy admits the identified peer', async () => {
    const network = new FakeQuicNetwork();
    const server = node({
      verifier: new FakeVerifier(),
      credential: credential('server-1', serverKey),
      holderKey: serverKey,
    });
    server.registerService(npc, answer);
    const listener = await listen(network, server, 'id-1', { policy: ChannelPolicies.scoped() });
    const binding = new QuicSwitchboardBinding(network.connector());
    const client = node({
      bindings: [binding],
      verifier: new FakeVerifier(),
      credential: credential('npc-7', clientKey),
      holderKey: clientKey,
      expectedIdentityFor: () => 'server-1',
    });
    expect(await tagOf(await client.openChannelAt(listener.endpoint, npcAddress()))).toBe(
      'npc for npc-7',
    );
    const initiated = binding.dialled[0] as QuicBindingConnection;
    const accepted = listener.accepted[0] as QuicBindingConnection;
    expect(initiated.exporterBound).toBe(true);
    expect(accepted.peerIdentity?.identity).toBe('npc-7');
    expect(accepted.peerIdentityBound).toBe(true);
    // The listener identified in turn, naming the client.
    await until(() => initiated.peerIdentity !== undefined);
    expect(initiated.peerIdentity?.identity).toBe('server-1');
    expect(initiated.peerIdentity?.receiver).toBe('npc-7');
    expect(initiated.peerIdentityBound).toBe(true);
    expect(initiated.peerAuthenticated).toBe(true);
    expect(initiated.quicOptions.identity.awaitPeerIdentity).toBe(true);
  });

  test('a channel that overtakes the IDENT on its own stream is held by the node until the peer identified', async () => {
    // The control stream (0) is slow: the channel's stream arrives first.
    const network = new FakeQuicNetwork({ delay: (info) => (info.ordinal === 0 ? 60 : 0) });
    let asked = 0;
    const scoped = ChannelPolicies.scoped();
    const server = node({ verifier: new FakeVerifier() });
    server.registerService(npc, answer);
    const listener = await listen(network, server, 'hold-1', {
      policy: (address, connection) => {
        asked++;
        return scoped(address, connection);
      },
    });
    const client = node({
      bindings: [
        new QuicSwitchboardBinding(network.connector(), {
          identityRules: new QuicIdentityRules({ requireExporterBinding: true }),
        }),
      ],
      credential: credential('npc-7', clientKey),
      holderKey: clientKey,
      identifyOutgoing: false,
    });
    const connection = await client.connect(listener.endpoint);
    const identified = client.identifyOn(connection);
    // Opened while the identification is under way: its stream arrives
    // before the IDENT.
    await until(() => (connection as QuicBindingConnection).exporterBound);
    const channel = client.openChannelOn(connection, npcAddress());
    expect(await tagOf(channel)).toBe('npc for npc-7');
    await within(identified);
    expect(asked).toBe(2);
  });

  test('an initiator with a credential reaches only an acceptor that identifies in turn', async () => {
    const network = new FakeQuicNetwork();
    // A listener without a credential cannot identify.
    const server = node({ verifier: new FakeVerifier() });
    server.registerService(npc, answer);
    const listener = await listen(network, server, 'id-2');
    const client = node({
      bindings: [new QuicSwitchboardBinding(network.connector())],
      verifier: new FakeVerifier(),
      credential: credential('npc-7', clientKey),
      holderKey: clientKey,
      identityTimeout: 300,
    });
    await expectCode(
      client.openChannelAt(listener.endpoint, npcAddress()),
      StatusCode.unauthenticated,
    );
    // One whose identity is not the one expected is refused as well.
    const other = node({
      verifier: new FakeVerifier(),
      credential: credential('server-x', serverKey),
      holderKey: serverKey,
    });
    other.registerService(npc, answer);
    const elsewhere = await listen(network, other, 'id-3');
    const strict = node({
      bindings: [new QuicSwitchboardBinding(network.connector())],
      verifier: new FakeVerifier(),
      credential: credential('npc-7', clientKey),
      holderKey: clientKey,
      expectedIdentityFor: () => 'server-1',
    });
    await expectCode(
      strict.openChannelAt(elsewhere.endpoint, npcAddress()),
      StatusCode.unauthenticated,
    );
  });

  test('a node without the exporter cannot identify to a self-signed listener, and a nonce-bound IDENT is refused', async () => {
    const network = new FakeQuicNetwork();
    const server = node({ verifier: new FakeVerifier() });
    server.registerService(npc, answer);
    const listener = await listen(network, server, 'id-4', { policy: ChannelPolicies.scoped() });
    // Browser-like: the stack has no exporter. Under the rules the
    // initiator closes at once.
    const browserLike = network.connector({ config: { exporter: false } });
    const careful = node({
      bindings: [new QuicSwitchboardBinding(browserLike)],
      verifier: new FakeVerifier(),
      credential: credential('npc-7', clientKey),
      holderKey: clientKey,
    });
    await expectCode(
      careful.openChannelAt(listener.endpoint, npcAddress()),
      StatusCode.unauthenticated,
    );
    // Without the rules it sends a nonce-bound IDENT, which the listener
    // refuses: the connection ends UNAUTHENTICATED.
    const binding = new QuicSwitchboardBinding(browserLike, {
      identityRules: QuicIdentityRules.none,
    });
    const careless = node({
      bindings: [binding],
      credential: credential('npc-7', clientKey),
      holderKey: clientKey,
    });
    await expectCode(
      careless.openChannelAt(listener.endpoint, npcAddress()),
      StatusCode.unauthenticated,
    );
    const dialled = binding.dialled[0] as QuicBindingConnection;
    expectStatus(await within(dialled.done), StatusCode.goingAway);
    expectStatus(await within(dialled.peerGoAwayStatus), StatusCode.unauthenticated);
    // A browser-like client without a credential talks all the same, with
    // what its policy admits.
    const open = node({ bindings: [new QuicSwitchboardBinding(browserLike)] });
    const server2 = node();
    server2.registerService(npc, answer);
    const listener2 = await listen(network, server2, 'id-5');
    expect(await tagOf(await open.openChannelAt(listener2.endpoint, npcAddress()))).toBe(
      'npc for nobody',
    );
  });

  test('a bearer credential goes only to an authenticated end: a pin, not a self-signed certificate', async () => {
    const network = new FakeQuicNetwork();
    const bearer = fakeCredential({ identity: 'bearer-1', scopes: [[Right.open, '*']] });
    const hash = Uint8Array.from({ length: 32 }, (_, i) => i);
    const server = node({ verifier: new FakeVerifier() });
    server.registerService(npc, answer);
    const listener = await listen(network, server, 'bearer-1', {
      policy: ChannelPolicies.scoped(),
      config: { certificateHash: hash },
      // The listener's clients pin its certificate.
      identityRules: QuicIdentityRules.none,
    });
    const unpinned = node({
      bindings: [new QuicSwitchboardBinding(network.connector())],
      credential: bearer,
    });
    await expectCode(
      unpinned.openChannelAt(listener.endpoint, npcAddress()),
      StatusCode.unauthenticated,
    );
    const pinned = node({
      bindings: [
        new QuicSwitchboardBinding(network.connector(), { serverCertificateHashes: [hash] }),
      ],
      credential: bearer,
    });
    expect(await tagOf(await pinned.openChannelAt(listener.endpoint, npcAddress()))).toBe(
      'npc for bearer-1',
    );
    // A wrong pin does not reach the listener at all.
    const wrong = node({
      bindings: [
        new QuicSwitchboardBinding(network.connector(), {
          serverCertificateHashes: [new Uint8Array(32)],
        }),
      ],
      credential: bearer,
    });
    await expectCode(wrong.connect(listener.endpoint), StatusCode.unavailable);
  });

  test.todo(
    'a man in the middle: relayed HELLOs fail, stripped EXPORTER flags fail, and only without the rules does the identification pass (not ported yet)',
  );
});

describe('the proxy between the frame mux and the binding', () => {
  test.todo(
    'a frame mux client through a proxy to a QUIC backend (needs the frame mux, Talk and the proxy)',
  );
  test.todo(
    'a QUIC client through a proxy to a frame mux backend (needs the frame mux, Talk and the proxy)',
  );
});

describe('the end of a connection', () => {
  test("a node closing sends GOAWAY: the peer's channels end GOING_AWAY, and the session closes with code 0", async () => {
    const network = new FakeQuicNetwork();
    const server = node();
    server.registerService(echo, echoService);
    const listener = await listen(network, server, 'end-1', {
      identityRules: QuicIdentityRules.none,
    });
    const binding = new QuicSwitchboardBinding(network.connector());
    const client = node({ bindings: [binding] });
    const channel = await client.openChannelAt(listener.endpoint, echoAddress());
    await echoed(channel, bytes('x'));
    const connection = binding.dialled[0] as QuicBindingConnection;
    await server.close();
    expectStatus(await within(connection.peerGoAwayStatus), StatusCode.goingAway);
    expectStatus(await within(connection.done), StatusCode.goingAway);
    const [, accepted] = network.pairs[0] as [FakeQuicSession, FakeQuicSession];
    expect(accepted.closes[0]?.[0]).toBe(0);
  });

  test('a GOAWAY with a status reaches the peer, and the session close carries it with its reason', async () => {
    const network = new FakeQuicNetwork();
    const server = node();
    server.registerService(echo, echoService);
    const listener = await listen(network, server, 'end-2', {
      identityRules: QuicIdentityRules.none,
    });
    const binding = new QuicSwitchboardBinding(network.connector());
    const client = node({ bindings: [binding] });
    await client.connect(listener.endpoint);
    const connection = binding.dialled[0] as QuicBindingConnection;
    await until(() => listener.accepted.length === 1);
    const accepted = listener.accepted[0] as QuicBindingConnection;
    await connection.goAway(Status.of(StatusCode.unavailable, 'upgrade'));
    expect(await within(accepted.peerGoAwayStatus)).toEqual(
      Status.of(StatusCode.unavailable, 'upgrade'),
    );
    const [initiator] = network.pairs[0] as [FakeQuicSession, FakeQuicSession];
    expect(initiator.closes[0]).toEqual([14, 'upgrade']);
    expect(await within(connection.done)).toEqual(Status.of(StatusCode.unavailable, 'upgrade'));
  });

  test('a lost session ends the channels with CONNECTION_LOST', async () => {
    const network = new FakeQuicNetwork();
    const server = node();
    server.registerService(echo, echoService);
    const listener = await listen(network, server, 'end-3', {
      identityRules: QuicIdentityRules.none,
    });
    const client = node({ bindings: [new QuicSwitchboardBinding(network.connector())] });
    const channel = await client.openChannelAt(listener.endpoint, echoAddress());
    await exchange(channel, bytes('x'));
    const [initiator] = network.pairs[0] as [FakeQuicSession, FakeQuicSession];
    initiator.lose('idle timeout');
    expectStatus(await within(channel.done), StatusCode.connectionLost);
  });
});

describe('review cases', () => {
  test('closing the listener while a session starts keeps it from being adopted', async () => {
    const server = node();
    const network = new FakeQuicNetwork();
    const provider = network.listen('quic://listener:4433');
    const listener = await QuicSwitchboardListener.start(server, provider);
    const [a, b] = FakeQuicSession.pair({ network });
    let open = (): void => {};
    b.exporterGate = new Promise((resolve) => {
      open = resolve;
    });
    provider.acceptSession(b);
    await until(() => b.exporterLabels.length > 0);
    await listener.close();
    open();
    expect((await within(a.done)).code).toBe(0);
    expect(listener.accepted).toHaveLength(0);
    expect(server.listeningEndpoints).not.toContain('quic://listener:4433');
  });

  test('a ReconnectingClient reopens its persistent channel over a new QUIC session after a loss', async () => {
    const server = node();
    server.registerService(echo, echoService);
    const network = new FakeQuicNetwork();
    const listener = await listen(network, server, 'again-1', {
      identityRules: QuicIdentityRules.none,
    });
    const client = new ReconnectingClient(
      quicConnector(listener.endpoint, network.connector(), { verifyCertificate: false }),
      { muxOptions: fast, initialBackoff: 10, jitter: 0 },
    );
    onCleanup(() => client.close());
    const ready: MuxChannel[] = [];
    client.openPersistent(echoAddress(), async (channel) => {
      ready.push(channel);
    });
    await until(() => ready.length === 1);
    expect(text(await exchange(ready[0] as MuxChannel, bytes('one')))).toBe('one');
    const [session] = network.pairs[0] as [FakeQuicSession, FakeQuicSession];
    session.lose();
    await until(() => ready.length === 2);
    expect(listener.accepted).toHaveLength(2);
    expect(text(await exchange(ready[1] as MuxChannel, bytes('two')))).toBe('two');
  });

  test.todo('SlotChannel accounts held sends at QUIC record cost (needs SlotChannel)');
});
