// Switchboard nodes over the QUIC binding, end to end over the fake
// provider: QuicSwitchboardBinding and QuicSwitchboardListener, the dial's
// bounds and stack settings, identity under the rules of the binding page,
// Talk and its bulk payloads over independent streams, the proxy between
// the frame mux and the binding, and the ends of connections. A port of
// `switchboard_quic`'s `test/switchboard_test.dart` and
// `review_switchboard_test.dart`; the nodes' `mem://` sides run over the
// frame mux, as Dart's do. The man in the middle group is not ported yet
// (todo).
import { beforeAll, describe, expect, test } from 'vitest';
import { ChannelAddress, ServiceAddress } from '../../../src/core/address/index.ts';
import { ReconnectingClient } from '../../../src/core/client/reconnecting-client.ts';
import { type HolderKey, Right } from '../../../src/core/identity/contract.ts';
import type { MuxChannel, MuxOptions } from '../../../src/core/mux/contract.ts';
import { FrameMuxChannel } from '../../../src/core/mux/frame-mux-channel.ts';
import { FrameMuxConnection } from '../../../src/core/mux/frame-mux-connection.ts';
import { MuxControlType } from '../../../src/core/mux/mux-frame.ts';
import { Name } from '../../../src/core/name.ts';
import { serviceRecord } from '../../../src/core/naming/naming-protocol.ts';
import { SlotChannel } from '../../../src/core/naming/slot-channel.ts';
import { QuicBindingChannel } from '../../../src/core/quic/quic-binding-channel.ts';
import type { QuicBindingConnection } from '../../../src/core/quic/quic-binding-connection.ts';
import { quicConnector } from '../../../src/core/quic/quic-dial.ts';
import { QuicIdentityRules } from '../../../src/core/quic/quic-identity-rules.ts';
import {
  QuicSwitchboardBinding,
  QuicSwitchboardListener,
  type QuicSwitchboardListenerOptions,
} from '../../../src/core/quic/quic-switchboard.ts';
import {
  encodeCloseRecord,
  encodeDataRecord,
  encodeOpenRecord,
} from '../../../src/core/quic/records.ts';
import { Status, StatusCode } from '../../../src/core/status.ts';
import { ChannelPolicies } from '../../../src/core/switchboard/channel-policy.ts';
import type { ChannelHandler } from '../../../src/core/switchboard/contract.ts';
import { pipeChannels, proxyHandler } from '../../../src/core/switchboard/proxy.ts';
import { StaticResolver } from '../../../src/core/switchboard/resolver.ts';
import type { Switchboard, SwitchboardOptions } from '../../../src/core/switchboard/switchboard.ts';
import type { TalkChannel as TalkChannelLike } from '../../../src/core/talk/contract.ts';
import {
  installBulkRouting,
  TalkBulkOpen,
  TalkBulkReference,
  TalkChannel,
  TalkFrame,
  TalkKind,
  type TalkMessage,
} from '../../../src/core/talk/index.ts';
import { memoryTransportPair } from '../../../src/core/transport/memory-transport.ts';
import {
  type FakeEndConfig,
  type FakeQuicConnector,
  FakeQuicNetwork,
  FakeQuicSession,
} from '../../../src/testing/fake-quic-provider.ts';
import { FakeVerifier, fakeCredential } from '../../support/node/fake-identity.ts';
import { node as harnessNode, onCleanup, tagOf, useMux } from '../../support/node/harness.ts';
import {
  bindingAndRaw,
  bytes,
  collect,
  concat,
  connectionPair,
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

// The nodes' in-process (`mem://`) connections run over the frame mux.
useMux('frame');

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

/** A Talk service answering every request with its payload. */
const echoService: ChannelHandler = (incoming) => {
  void incoming.talk().serve((m) => {
    if (m.expectsReply) {
      void m.reply(m.payload);
    }
  });
};

/** Whether [a] and [b] hold the same bytes. */
function same(a: Uint8Array, b: Uint8Array): boolean {
  return a.length === b.length && a.every((value, i) => value === b[i]);
}

/** The payload of the reply to the request [procedure] with [payload] on [talk]. */
async function ask(
  talk: TalkChannel,
  payload: Uint8Array,
  procedure = 'ECHO',
  ms = limit,
): Promise<Uint8Array> {
  return (await within(talk.request(procedure, payload, { timeout: ms }), ms)).payload;
}

/** Answers with the channel's type and the peer's identity, and closes. */
const answer: ChannelHandler = (incoming) => {
  const who = incoming.peerIdentity?.identity ?? 'nobody';
  void incoming.channel.send(bytes(`${incoming.address.type} for ${who}`)).catch(() => {});
  void incoming.channel.close().catch(() => {});
};

const echoAddress = (): ChannelAddress => new ChannelAddress({ type: echo });
const npcAddress = (): ChannelAddress => new ChannelAddress({ type: npc });

describe('two nodes', () => {
  test('Talk request and reply, each channel on a stream of its own, pooled by endpoint', async () => {
    const network = new FakeQuicNetwork();
    const server = node();
    server.registerService(echo, echoService);
    const listener = await listen(network, server, 'echo-1');
    expect(server.listeningEndpoints).toContain(listener.endpoint);
    expect(server.isOwnEndpoint('quic://echo-1:4433')).toBe(true);
    const binding = new QuicSwitchboardBinding(network.connector());
    const client = node({ bindings: [binding] });
    expect(client.bindingSchemes).toEqual(new Set(['quic', 'https']));
    const talk = await client.openTalkAt(listener.endpoint, echoAddress());
    expect(talk.raw).toBeInstanceOf(QuicBindingChannel);
    expect(text(await ask(talk, bytes('hello')))).toBe('hello');
    const again = await client.openTalkAt('QUIC://Echo-1:4433#echo', echoAddress());
    expect(text(await ask(again, bytes('again')))).toBe('again');
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

  test('a 3 MiB bulk payload each way, its streams overtaking the parent and arriving out of order', async () => {
    // The first channel's stream is slow both ways: the bulk streams opened
    // after it arrive first; the others jitter.
    const network = new FakeQuicNetwork({
      delay: (info) => (info.ordinal === 1 ? 60 : (info.ordinal * 7) % 5),
    });
    const server = node();
    server.registerService(echo, echoService);
    const listener = await listen(network, server, 'bulk-1');
    const client = node({ bindings: [new QuicSwitchboardBinding(network.connector())] });
    const talk = await client.openTalkAt(listener.endpoint, echoAddress());
    const payload = pattern(3 * 1024 * 1024, 3);
    const reply = await within(talk.request('BIG', payload, { timeout: 30_000 }), 30_000);
    expect(reply.isBulk).toBe(true);
    expect(same(reply.payload, payload)).toBe(true);
    // The bulk stream (2) arrived before its parent (1).
    const [, accepted] = network.pairs[0] as [FakeQuicSession, FakeQuicSession];
    expect(accepted.announced.indexOf(2)).toBeLessThan(accepted.announced.indexOf(1));
    // Two at once, numbers 1 and 2 racing each other.
    const twins = await within(
      Promise.all([
        talk.request('A', pattern(300_000, 1), { timeout: 30_000 }),
        talk.request('B', pattern(200_000, 2), { timeout: 30_000 }),
      ]),
      30_000,
    );
    expect(same((twins[0] as TalkMessage).payload, pattern(300_000, 1))).toBe(true);
    expect(same((twins[1] as TalkMessage).payload, pattern(200_000, 2))).toBe(true);
  }, 60_000);

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
    const talks: TalkChannel[] = [];
    for (let i = 0; i < 120; i++) {
      talks.push(await client.openTalkAt(listener.endpoint, echoAddress()));
      if (i === 0) {
        await until(() => binding.dialled[0]?.peerHello !== undefined);
      }
    }
    expect(binding.dialled[0]?.peerMaxChannels).toBe(100);
    expect(binding.dialled).toHaveLength(2);
    // Requests on all of them at once; bulk payloads on the channels of the
    // second connection, the first having no stream left for them.
    const results = await within(
      Promise.all(
        talks.map(async (talk, i) => {
          const payload = pattern(i >= 100 ? 70000 + i : 100 + i, i);
          return same(await ask(talk, payload, 'ECHO', 60_000), payload);
        }),
      ),
      60_000,
    );
    expect(results.every((ok) => ok)).toBe(true);
    await Promise.all(talks.map((talk) => talk.close()));
    // And as many opened at once on a pooled connection wait for stream
    // credit instead of failing.
    const burst = await within(
      Promise.all(
        Array.from({ length: 150 }, async (_, i) => {
          const talk = await client.openTalkAt(listener.endpoint, echoAddress());
          const reply = await ask(talk, pattern(10, i), 'ECHO', 60_000);
          await talk.close();
          return same(reply, pattern(10, i));
        }),
      ),
      60_000,
    );
    expect(burst.every((ok) => ok)).toBe(true);
  }, 120_000);

  test('whenWritable paces a publisher at the stream window', async () => {
    const network = new FakeQuicNetwork();
    const server = node();
    let served: TalkChannelLike | undefined;
    server.registerService(echo, (incoming) => {
      // The receiver takes nothing until told to.
      served = incoming.talk();
    });
    const listener = await listen(network, server, 'pace-1', {
      config: { streamWindow: 64 * 1024 },
    });
    const client = node({ bindings: [new QuicSwitchboardBinding(network.connector())] });
    const talk = await client.openTalkAt(listener.endpoint, echoAddress());
    const count = 400;
    let sent = 0;
    const publisher = (async () => {
      for (let i = 0; i < count; i++) {
        await talk.whenWritable;
        void talk.send('EVENT', new Uint8Array(1000));
        sent++;
      }
    })();
    await until(() => served !== undefined);
    await settle(100);
    // The receiver takes nothing: the stack's window, plus the record being
    // read, is all the publisher got out.
    expect(talk.isWritable).toBe(false);
    expect(sent).toBeLessThan(80);
    const stalled = sent;
    await settle(100);
    expect(sent).toBe(stalled);
    let received = 0;
    const reading = (async () => {
      for await (const _ of (served as TalkChannelLike).messages) {
        if (++received === count) {
          return;
        }
      }
    })();
    await within(publisher);
    await within(reading);
    expect(received).toBe(count);
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
    const talk = await client.openTalk(new ServiceAddress(echo));
    expect(text(await ask(talk, bytes('fell back')))).toBe('fell back');
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
    const talk = await client.openTalkAt(listener.endpoint, echoAddress());
    expect(text(await ask(talk, bytes('late')))).toBe('late');
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
  test('a frame mux client through a proxy to a QUIC backend, bulk payloads included', async () => {
    const network = new FakeQuicNetwork({
      delay: (info) => (info.ordinal === 1 ? 30 : 0),
    });
    const backend = node();
    backend.registerService(echo, echoService, { instance: 1 });
    const backendListener = await listen(network, backend, 'backend-1', {
      identityRules: QuicIdentityRules.none,
    });
    const resolver = new StaticResolver([
      serviceRecord(new ServiceAddress(echo, 1), { endpoints: [backendListener.endpoint] }),
    ]);
    onCleanup(() => resolver.close());
    const binding = new QuicSwitchboardBinding(network.connector());
    const proxy = node({ bindings: [binding], resolver });
    proxy.catchAll = proxyHandler(proxy);
    const proxyUri = await proxy.listenMemory();
    const client = node();
    const talk = await client.openTalkAt(proxyUri, echoAddress());
    expect(talk.raw).toBeInstanceOf(FrameMuxChannel);
    expect(text(await ask(talk, bytes('through')))).toBe('through');
    const payload = pattern(3 * 1024 * 1024, 5);
    const big = await within(talk.request('BIG', payload, { timeout: 30_000 }), 30_000);
    expect(big.isBulk).toBe(true);
    expect(same(big.payload, payload)).toBe(true);
    const twins = await within(
      Promise.all([
        talk.request('A', pattern(200_000, 1), { timeout: 30_000 }),
        talk.request('B', pattern(150_000, 2), { timeout: 30_000 }),
      ]),
      30_000,
    );
    expect(same((twins[0] as TalkMessage).payload, pattern(200_000, 1))).toBe(true);
    expect(same((twins[1] as TalkMessage).payload, pattern(150_000, 2))).toBe(true);
  }, 60_000);

  test('a QUIC client through a proxy to a frame mux backend: bulk twins forwarded in order', async () => {
    // The parent stream is slow: the two bulk streams of the client reach
    // the proxy before their references, the second one first.
    const network = new FakeQuicNetwork({
      delay: (info) => (info.ordinal === 1 ? 50 : info.ordinal === 2 ? 25 : 0),
    });
    const backend = node();
    const received: Uint8Array[] = [];
    backend.registerService(
      echo,
      (incoming) => {
        void incoming.talk().serve((m) => {
          received.push(m.payload);
          if (m.expectsReply) {
            void m.reply(`${m.payload.length}`);
          }
        });
      },
      { instance: 1 },
    );
    const backendUri = await backend.listenMemory();
    const resolver = new StaticResolver([
      serviceRecord(new ServiceAddress(echo, 1), { endpoints: [backendUri] }),
    ]);
    onCleanup(() => resolver.close());
    const proxy = node({ resolver });
    proxy.catchAll = proxyHandler(proxy);
    const proxyListener = await listen(network, proxy, 'proxy-1', {
      identityRules: QuicIdentityRules.none,
    });
    const client = node({ bindings: [new QuicSwitchboardBinding(network.connector())] });
    const talk = await client.openTalkAt(proxyListener.endpoint, echoAddress());
    const first = pattern(120_000, 1);
    const second = pattern(90_000, 2);
    const replies = await within(
      Promise.all([
        talk.request('A', first, { timeout: 30_000 }),
        talk.request('B', second, { timeout: 30_000 }),
      ]),
      30_000,
    );
    expect(text((replies[0] as TalkMessage).payload)).toBe('120000');
    expect(text((replies[1] as TalkMessage).payload)).toBe('90000');
    expect(received).toHaveLength(2);
    expect(same(received[0] as Uint8Array, first)).toBe(true);
    expect(same(received[1] as Uint8Array, second)).toBe(true);
    // The second bulk stream reached the proxy first.
    const [, accepted] = network.pairs[0] as [FakeQuicSession, FakeQuicSession];
    expect(accepted.announced.indexOf(3)).toBeLessThan(accepted.announced.indexOf(2));
  }, 60_000);
});

/** The open payload of bulk channel [number] of the channel [parentId]. */
function bulkOpen(parentId: number, number: number): Uint8Array {
  return new TalkBulkOpen(parentId, number).encode();
}

/** A Talk `BULK` message [procedure] whose payload of [length] bytes is on bulk channel [number]. */
function bulkMessage(procedure: string, number: number, length: number): Uint8Array {
  return new TalkFrame({
    kind: TalkKind.message,
    procedure: Name.of(procedure),
    bulk: true,
    payload: new TalkBulkReference(number, length).encode(),
  }).encode();
}

describe("Talk's bulk routing on a QUIC connection", () => {
  test('after GOAWAY, a bulk channel for an open channel is admitted and goes to its Talk channel; a new channel is refused GOING_AWAY', async () => {
    const [connection, raw] = await bindingAndRaw();
    // What a Talk channel on the connection installs.
    installBulkRouting(connection);
    const atBinding = collect(connection);
    const parent = await raw.openRaw(encodeOpenRecord(2, []));
    await until(() => atBinding.length === 1);
    const talk = new TalkChannel(atBinding[0] as MuxChannel);
    void connection.goAway();
    await raw.nextControlOf(MuxControlType.goAway);
    const late = await raw.openRaw(encodeOpenRecord(4, []));
    expect(await within(late.stopped)).toBe(StatusCode.goingAway);
    // The message, then its payload on a bulk stream opened after the GOAWAY.
    await parent.write(encodeDataRecord(bulkMessage('PUT', 1, 5)));
    const bulk = await raw.openRaw(encodeOpenRecord(6, bulkOpen(2, 1)));
    await bulk.write(concat([encodeDataRecord(bytes('hello')), encodeCloseRecord(Status.ok)]));
    await bulk.finish();
    const messages = talk.messages[Symbol.asyncIterator]();
    const message = (await within(messages.next())).value as TalkMessage;
    expect(message.procedureName).toBe('PUT');
    expect(text(await within(message.bytes()))).toBe('hello');
    // Routed to the Talk channel, never delivered to `incoming`.
    expect(atBinding).toHaveLength(1);
    await messages.return?.();
  });

  test('after GOAWAY, a bulk channel whose parent ended gracefully while a pipe waits for it is admitted (the parent is retained for the drain); one for a parent never admitted is not', async () => {
    const [connection, raw] = await bindingAndRaw({ options: { ...fast, goAwayGrace: 5000 } });
    // What the node installs on every connection.
    installBulkRouting(connection);
    const atBinding = collect(connection);
    // A proxy's pipe from the binding's channel 2 to a frame mux backend.
    const [out, back] = memoryTransportPair({ delay: 0 });
    const proxyOut = new FrameMuxConnection(out, { ...fast, isInitiator: true });
    const backend = new FrameMuxConnection(back, { ...fast, isInitiator: false });
    onCleanup(async () => {
      await proxyOut.close();
      await backend.close();
    });
    const atBackend = backend.incoming[Symbol.asyncIterator]();
    const parent = await raw.openRaw(encodeOpenRecord(2, []));
    // Another channel keeps the connection busy meanwhile.
    await raw.openRaw(encodeOpenRecord(4, []));
    await until(() => atBinding.length === 2);
    const pipe = pipeChannels(atBinding[0] as MuxChannel, proxyOut.open(new Uint8Array(0)));
    const far = (await within(atBackend.next())).value as MuxChannel;
    const messages = new TalkChannel(far).messages[Symbol.asyncIterator]();
    // A message referencing bulk channel 1, then the parent's graceful
    // close, which overtakes the bulk stream: the pipe forwarded both and
    // waits for the bulk channel, its parent retained for the drain.
    await parent.write(
      concat([encodeDataRecord(bulkMessage('PUT', 1, 5)), encodeCloseRecord(Status.ok)]),
    );
    await parent.finish();
    await until(() => connection.channelWithId(2) === undefined);
    expect(connection.retainedForDrain(2)).toBe(true);
    void connection.goAway();
    await raw.nextControlOf(MuxControlType.goAway);
    const bulk = await raw.openRaw(encodeOpenRecord(6, bulkOpen(2, 1)));
    await bulk.write(concat([encodeDataRecord(bytes('hello')), encodeCloseRecord(Status.ok)]));
    await bulk.finish();
    // Forwarded to the backend with its message.
    const message = (await within(messages.next())).value as TalkMessage;
    expect(message.procedureName).toBe('PUT');
    expect(text(await within(message.bytes()))).toBe('hello');
    const stray = await raw.openRaw(encodeOpenRecord(8, bulkOpen(10, 1)));
    expect(await within(stray.stopped)).toBe(StatusCode.goingAway);
    await messages.return?.();
    await within(pipe);
  });
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
    const talk = await client.openTalkAt(listener.endpoint, echoAddress());
    await ask(talk, bytes('x'));
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
    const talk = await client.openTalkAt(listener.endpoint, echoAddress());
    await ask(talk, bytes('x'));
    const [initiator] = network.pairs[0] as [FakeQuicSession, FakeQuicSession];
    initiator.lose('idle timeout');
    expectStatus(await within((talk.raw as MuxChannel).done), StatusCode.connectionLost);
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
    const ready: TalkChannel[] = [];
    client.openPersistentTalk(echoAddress(), async (talk) => {
      ready.push(talk);
    });
    await until(() => ready.length === 1);
    expect(text(await ask(ready[0] as TalkChannel, bytes('one')))).toBe('one');
    const [session] = network.pairs[0] as [FakeQuicSession, FakeQuicSession];
    session.lose();
    await until(() => ready.length === 2);
    expect(listener.accepted).toHaveLength(2);
    expect(text(await ask(ready[1] as TalkChannel, bytes('two')))).toBe('two');
  });

  test('SlotChannel accounts held sends at QUIC record cost', async () => {
    const [a, b] = await connectionPair();
    void (async () => {
      for await (const channel of b.incoming) {
        void channel.close(Status.of(StatusCode.moved));
      }
    })();
    const first = a.open(echoAddress().encode());
    let reopen = (_channel: MuxChannel | undefined): void => {};
    const reopened = new Promise<MuxChannel | undefined>((resolve) => {
      reopen = resolve;
    });
    onCleanup(() => reopen(undefined));
    // The internal seam behind the public slot-opening API.
    const slot = new SlotChannel(echo, 3, first, () => reopened, { maxHeldBytes: 100 });
    expect((await within(first.done)).code).toBe(StatusCode.moved);
    // Two records of 40 bytes fit in 100 at record cost (the frame mux
    // would charge 56 each).
    const sends = [slot.send(new Uint8Array(40)), slot.send(new Uint8Array(40))];
    await expectCode(slot.send(new Uint8Array(40)), StatusCode.resourceExhausted);
    expect((await within(slot.done)).code).toBe(StatusCode.resourceExhausted);
    for (const sending of sends) {
      await expectCode(sending, StatusCode.failedPrecondition);
    }
  });
});
