/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Ports of the Dart proxy tests (`proxy_test.dart`): pipeChannels between
// two connections, and proxyHandler on a frontend node in front of a
// backend, over the memory transport, over the toy mux and, from
// `over-frame-mux/`, over the frame mux. The cases that need the frame
// mux's flow control, LIMITS, close confirmation or output scheduler (and
// the bulk payloads that go bulk by the channel's window) run over the
// frame mux only.
//
// Not ported yet: the groups "proxyHandler with slot tables" and
// "proxyHandler: channels without a shard slot" (Dart `proxy_test.dart`
// from line 1338), which need the node's slot routing (`slot-layer.ts`).

import { beforeEach, describe, expect, it } from 'vitest';
import { ChannelAddress, ServiceAddress } from '../../../src/core/address/index.ts';
import { bytesEqual, utf8Decode } from '../../../src/core/bytes.ts';
import {
  type MuxChannel,
  type MuxConnection,
  type MuxOptions,
  muxOptions,
} from '../../../src/core/mux/contract.ts';
import {
  MuxCommand,
  MuxControlMessage,
  MuxFrame,
  MuxLimits,
} from '../../../src/core/mux/mux-frame.ts';
import { Name } from '../../../src/core/name.ts';
import { serviceRecord } from '../../../src/core/naming/naming-protocol.ts';
import { Status, StatusCode, statusCodeName } from '../../../src/core/status.ts';
import type {
  ChannelPolicy,
  IncomingChannel,
  ServiceRecord,
} from '../../../src/core/switchboard/contract.ts';
import { pipeChannels, proxyHandler } from '../../../src/core/switchboard/proxy.ts';
import { EndpointResolver, StaticResolver } from '../../../src/core/switchboard/resolver.ts';
import { Switchboard } from '../../../src/core/switchboard/switchboard.ts';
import { TalkBulkOpen, TalkChannel, type TalkMessage } from '../../../src/core/talk/index.ts';
import type { FrameTransport } from '../../../src/core/transport/contract.ts';
import { memoryTransportPair } from '../../../src/core/transport/memory-transport.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';
import { Frames, framesOf, nextText, Pull, pattern } from '../../support/node/channels.ts';
import { GatedTransport } from '../../support/node/gated-transport.ts';
import {
  bytes,
  currentRuntime,
  fast,
  node,
  onCleanup,
  overFrameMux,
  pump,
  rejectsWith,
  testMux,
  until,
} from '../../support/node/harness.ts';

const chat = Name.of('chat');

/** The name of [status]'s code. */
function codeOf(status: Status): string | undefined {
  return statusCodeName(status.code);
}

function expectCode(status: Status, code: StatusCode): void {
  expect(codeOf(status)).toBe(statusCodeName(code));
}

function expectStatus(status: Status, code: StatusCode, reason: string): void {
  expectCode(status, code);
  expect(status.reason).toBe(reason);
}

/** A connection of the run's mux over [transport], closed after the test. */
function connection(
  transport: FrameTransport,
  isInitiator: boolean,
  options: Partial<MuxOptions> = fast,
): MuxConnection {
  const made = testMux()(transport, muxOptions({ ...fast, ...options }), isInitiator);
  onCleanup(() => made.close());
  return made;
}

/** Two connections of the run's mux over a memory pair, closed after the test. */
function muxPair(
  initiator: Partial<MuxOptions> = fast,
  acceptor: Partial<MuxOptions> = fast,
): [MuxConnection, MuxConnection] {
  const [a, b] = memoryTransportPair({ delay: 0 });
  return [connection(a, true, initiator), connection(b, false, acceptor)];
}

/** Whether [channel] is a Talk bulk channel. */
function isBulk(channel: MuxChannel): boolean {
  return TalkBulkOpen.isBulk(ChannelAddress.decode(channel.openPayload));
}

/** The bulk channel the peer of [on] opened, once it is there. */
async function bulkOf(on: MuxConnection): Promise<MuxChannel> {
  let found: MuxChannel | undefined;
  await until(() => {
    found = [...on.channels].find((channel) => !channel.isLocallyOpened && isBulk(channel));
    return found !== undefined;
  });
  return found as MuxChannel;
}

/** client ⇄ (proxyIn | pipe | proxyOut) ⇄ backend. */
interface Piped {
  readonly clientConnection: MuxConnection;
  readonly proxyIn: MuxConnection;
  readonly proxyOut: MuxConnection;
  readonly backendConnection: MuxConnection;
  readonly client: MuxChannel;
  readonly inbound: MuxChannel;
  readonly backend: MuxChannel;
  readonly pipe: Promise<void>;
}

/** Sets up a piped channel; [early] runs on the client channel before the proxy attaches the pipe. */
async function piped(
  init: {
    early?: (client: MuxChannel) => void;
    options?: Partial<MuxOptions>;
    backendOptions?: Partial<MuxOptions>;
  } = {},
): Promise<Piped> {
  const options = init.options ?? fast;
  const [clientConnection, proxyIn] = muxPair(options, options);
  const [proxyOut, backendConnection] = muxPair(options, init.backendOptions ?? options);
  await clientConnection.ping();
  await proxyOut.ping();
  const atProxy = new Pull(proxyIn.incoming);
  const atBackend = new Pull(backendConnection.incoming);
  const client = clientConnection.open(bytes('header'));
  init.early?.(client);
  await pump();
  const inbound = await atProxy.next();
  const outbound = proxyOut.open(inbound.openPayload);
  const pipe = pipeChannels(inbound, outbound);
  const backend = await atBackend.next();
  expect(backend.openPayload).toEqual(bytes('header'));
  return {
    clientConnection,
    proxyIn,
    proxyOut,
    backendConnection,
    client,
    inbound,
    backend,
    pipe,
  };
}

/** The first [count] subframes of [channel]. */
async function take(channel: MuxChannel, count: number): Promise<Uint8Array[]> {
  const frames = new Frames(channel);
  const taken: Uint8Array[] = [];
  for (let i = 0; i < count; i++) {
    const frame = await frames.next();
    if (frame === undefined) break;
    taken.push(frame);
  }
  return taken;
}

describe('pipeChannels', () => {
  it('the loss of the backend while an item window is exhausted fails the requester with the channel status', async () => {
    const p = await piped();
    const clientTalk = new TalkChannel(p.client);
    const backendTalk = new TalkChannel(p.backend);
    const held = new Deferred<TalkMessage>();
    void backendTalk.serve((m) => void held.resolve(m));
    const s = clientTalk.streamRequest('LIST', undefined, { window: 1 });
    const items: string[] = [];
    void (async () => {
      try {
        for await (const item of s.items) items.push(item.text());
      } catch {
        // The stream fails with the channel.
      }
    })();
    const m = await held.promise;
    await m.replyItem('a');
    const waiting = m.replyItem('b');
    await until(() => items.length > 0);
    expect(s.window).toBe(0);
    await backendTalk.close(Status.of(StatusCode.aborted, 'backend lost'));
    const status = await clientTalk.done;
    expectCode(status, StatusCode.aborted);
    const failure = await rejectsWith(s.done, StatusCode.aborted);
    expect(failure.status.equals(status)).toBe(true);
    await rejectsWith(waiting, StatusCode.aborted);
    expect(items).toEqual(['a']);
    await p.pipe;
  });

  if (overFrameMux()) {
    it('a bulk drain timeout forwards failure and cancels the twins', async () => {
      const options = { ...fast, closeConfirmTimeout: 30 };
      const [client, proxyIn] = muxPair(options, options);
      const [proxyOut, backend] = muxPair(options, options);
      const atProxy = new Pull(proxyIn.incoming);
      const atBackend = new Pull(backend.incoming);
      const parent = client.open(new Uint8Array(0));
      const inbound = await atProxy.next();
      const outbound = proxyOut.open(new Uint8Array(0));
      const farParent = await atBackend.next();
      void pipeChannels(inbound, outbound);
      const bulk = parent.openAfter(new TalkBulkOpen(parent.id, 1).encode());
      TalkChannel.adoptBulk(await atProxy.next());
      const twin = await atBackend.next();
      await parent.close();
      expectCode(await farParent.done, StatusCode.deadlineExceeded);
      expectCode(await bulk.done, StatusCode.cancelled);
      expectCode(await twin.done, StatusCode.cancelled);
    });

    it('a bulk payload whose source ended waits for a reader slower than the close confirmation timeout', async () => {
      const options = { ...fast, closeConfirmTimeout: 100 };
      const [client, proxyIn] = muxPair(options, options);
      const [proxyOut, backend] = muxPair(options, options);
      await client.ping();
      await proxyOut.ping();
      const atProxy = new Pull(proxyIn.incoming);
      const atBackend = new Pull(backend.incoming);
      const sender = new TalkChannel(client.open(new Uint8Array(0)));
      const inbound = await atProxy.next();
      const pipe = pipeChannels(inbound, proxyOut.open(inbound.openPayload));
      // A Talk channel installs bulk routing on its connection: the twin
      // goes to it and never to `incoming` (Dart adopts it by hand).
      const reader = new TalkChannel(await atBackend.next(), { streamBulk: () => true });
      const messages = new Pull(reader.messages);
      void sender.send('BIG', undefined, { bulk: [pattern(300_000)], bulkLength: 300_000 });
      const source = await atProxy.next();
      TalkChannel.adoptBulk(source);
      const twin = await bulkOf(backend);
      const message = await messages.next();
      const stream = message.bulk.getReader();
      // A reader that takes one chunk and pauses (Dart listens and pauses;
      // a pull stream attaches on its first read): the bulk window is
      // raised for it.
      const received: Uint8Array[] = [];
      const firstRead = stream.read();
      // The sender is done and its bulk channel closed, while the twin
      // still waits for the reader's credit for the end of the payload.
      await new Promise((resolve) => setTimeout(resolve, 500));
      expect(source.state).toBe('closed');
      expect(twin.state).toBe('open');
      const first = await firstRead;
      if (first.value !== undefined) received.push(first.value);
      for (;;) {
        const { value, done } = await stream.read();
        if (done) break;
        received.push(value);
      }
      const whole = new Uint8Array(received.reduce((n, chunk) => n + chunk.length, 0));
      let offset = 0;
      for (const chunk of received) {
        whole.set(chunk, offset);
        offset += chunk.length;
      }
      expect(bytesEqual(whole, pattern(300_000))).toBe(true);
      expectCode(await twin.done, StatusCode.ok);
      await sender.close();
      await pipe;
      await messages.cancel();
    });
  }

  for (const fromClient of [false, true]) {
    it(`failed parent cancels forwarded bulk twins (client: ${fromClient})`, async () => {
      const [client, proxyIn] = muxPair();
      const [proxyOut, backend] = muxPair();
      const atProxy = new Pull(proxyIn.incoming);
      const atBackend = new Pull(backend.incoming);
      const atClient = new Pull(client.incoming);
      const parent = client.open(new Uint8Array(0));
      const inbound = await atProxy.next();
      const outbound = proxyOut.open(new Uint8Array(0));
      const farParent = await atBackend.next();
      void pipeChannels(inbound, outbound);
      const opener = fromClient ? parent : farParent;
      const opened = opener.openAfter(new TalkBulkOpen(opener.id, 1).encode());
      const proxyBulk = fromClient
        ? await atProxy.next()
        : await new Pull(proxyOut.incoming).next();
      TalkChannel.adoptBulk(proxyBulk);
      const twin = fromClient ? await atBackend.next() : await atClient.next();
      await parent.close(Status.of(StatusCode.cancelled));
      await pump();
      expect(opened.canSend).toBe(false);
      expect(twin.canSend).toBe(false);
      expectCode(await opened.done, StatusCode.cancelled);
      expectCode(await twin.done, StatusCode.cancelled);
    });
  }

  if (overFrameMux()) {
    it('a send rejected by delayed LIMITS closes both pipe ends', async () => {
      const [clientConnection, proxyIn] = muxPair();
      const [outTransport, peerTransport] = memoryTransportPair({ delay: 0 });
      const proxyOut = connection(outTransport, true);
      const wire = peerTransport.readable.getReader();
      const peer = peerTransport.writable.getWriter();
      const incoming = new Pull(proxyIn.incoming);
      const client = clientConnection.open(new Uint8Array(0));
      const inbound = await incoming.next();
      const outbound = proxyOut.open(new Uint8Array(0));
      void pipeChannels(inbound, outbound);
      await client.send(new Uint8Array(1000));
      await pump();
      expect(inbound.heldBytes).toBe(1016);
      await peer.write(
        MuxControlMessage.limits(
          new MuxLimits({ maxFrameSize: 256, maxChannels: 4, initialWindow: 256 }),
        )
          .toFrame()
          .encode(),
      );
      await pump();
      expect(outbound.canSend).toBe(false);
      expectCode(await client.done, StatusCode.frameTooLarge);
      const sent: MuxFrame[] = [];
      await proxyOut.close();
      for (;;) {
        const { value, done } = await wire.read();
        if (done) break;
        sent.push(MuxFrame.decode(value));
      }
      const closes = sent.filter(
        (f) => f.channelId === outbound.id && f.command === MuxCommand.close,
      );
      expect(closes).toHaveLength(1);
      expectCode(Status.decode((closes[0] as MuxFrame).payload), StatusCode.frameTooLarge);
    });
  }

  it('forwards subframes both ways, in order', async () => {
    const p = await piped();
    const toBackend = Array.from({ length: 100 }, (_, i) => pattern(i, i));
    const toClient = Array.from({ length: 100 }, (_, i) => pattern(i + 1, -i));
    const atBackend = take(p.backend, 100);
    const atClient = take(p.client, 100);
    for (let i = 0; i < 100; i++) {
      void p.client.send(toBackend[i] as Uint8Array);
      void p.backend.send(toClient[i] as Uint8Array);
    }
    expect(await atBackend).toEqual(toBackend);
    expect(await atClient).toEqual(toClient);
    await p.client.close();
    await p.pipe;
  });

  if (overFrameMux()) {
    it('a graceful close follows the bulk payloads forwarded before it', async () => {
      const [c1, p1] = memoryTransportPair({ delay: 0 });
      const client = connection(c1, true);
      const proxyIn = connection(p1, false);
      const [p2, b2] = memoryTransportPair({ delay: 0 });
      const gate = new GatedTransport(p2);
      gate.allow(1000);
      const proxyOut = connection(gate, true);
      const backend = connection(b2, false);
      const received: string[] = [];
      const backendDone = new Deferred<Status>();
      void (async () => {
        for await (const channel of backend.incoming) {
          if (isBulk(channel)) {
            TalkChannel.adoptBulk(channel);
            continue;
          }
          const talk = new TalkChannel(channel);
          void talk.serve((m) => void received.push(`${m.procedureName} ${m.payload.length}`));
          void talk.done.then((status) => backendDone.resolve(status));
        }
      })();
      void (async () => {
        for await (const channel of proxyIn.incoming) {
          if (isBulk(channel)) {
            TalkChannel.adoptBulk(channel);
            continue;
          }
          void pipeChannels(channel, proxyOut.open(channel.openPayload));
        }
      })();
      await client.ping();
      await proxyOut.ping();
      // The proxy's output toward the backend is congested: the bulk
      // payload waits in its scheduler when the client's CLOSE arrives.
      gate.allow(-1_000_000);
      const talk = new TalkChannel(client.open(new Uint8Array(0)));
      void talk.send('BIG', new Uint8Array(100_000));
      await talk.close();
      gate.allow(1_000_000);
      expect((await backendDone.promise).equals(Status.ok)).toBe(true);
      expect(received).toEqual(['BIG 100000']);
    });

    it('credit passes hop by hop: a slow far consumer slows the near sender, the proxy holds at most a window', async () => {
      const p = await piped();
      // The backend does not read yet.
      const backend = p.backend.readable.getReader();
      let sent = 0;
      const sending = (async () => {
        for (let i = 0; i < 100; i++) {
          const subframe = new Uint8Array(8000);
          subframe[0] = i;
          await p.client.send(subframe);
          sent++;
        }
      })();
      for (let i = 0; i < 5; i++) {
        await p.clientConnection.ping();
        await p.proxyOut.ping();
      }
      // The backend holds its window (8 subframes of 8016 bytes of cost),
      // the proxy at most the window it grants the client (the subframes
      // waiting for the backend's credit), and the client waits for the
      // proxy's.
      expect(sent).toBeGreaterThan(8);
      expect(sent * 8016).toBeLessThanOrEqual(2 * 65536);
      expect(p.backend.bufferedBytes).toBe(8 * 8016);
      expect(p.inbound.heldBytes).toBeLessThanOrEqual(65536);
      expect(p.inbound.heldBytes).toBeGreaterThan(0);
      // Held for the other side, not counted by the connection's mark:
      // only the OPEN payload is.
      expect(p.proxyIn.bufferedBytes).toBe(p.inbound.openPayload.length);
      expect(p.client.sendWindow).toBeLessThan(8016);
      const received: number[] = [];
      const reading = (async () => {
        for (;;) {
          const { value, done } = await backend.read();
          if (done) return;
          received.push(value[0] as number);
        }
      })();
      await sending;
      await p.client.close();
      await p.pipe;
      await reading;
      expect(received).toEqual(Array.from({ length: 100 }, (_, i) => i));
      expect(p.inbound.heldBytes).toBe(0);
    });

    it("a client that does not read holds the proxy's data within the window, and does not pause the backend connection", async () => {
      const p = await piped({ options: { ...fast, receiveHighWaterMarkBytes: 20_000 } });
      // The client never reads.
      for (let i = 0; i < 40; i++) {
        void p.backend.send(new Uint8Array(8000));
      }
      for (let i = 0; i < 5; i++) {
        await p.backendConnection.ping();
        await p.proxyOut.ping();
      }
      // The proxy holds what the client has no window for, without
      // counting it toward the backend connection's mark: other channels
      // on it, and its keep-alive, are not stalled.
      expect(p.inbound.heldBytes).toBe(0);
      expect(p.proxyOut.isReceivePaused).toBe(false);
      const outbound = [...p.proxyOut.channels];
      expect(outbound).toHaveLength(1);
      expect((outbound[0] as MuxChannel).heldBytes).toBeGreaterThanOrEqual(1);
      expect((outbound[0] as MuxChannel).heldBytes).toBeLessThanOrEqual(65536);
      expect(p.clientConnection.isReceivePaused).toBe(true);
      await p.client.readable.cancel();
      await p.backend.close();
      await p.pipe;
    });
  }

  it('forwards large subframes', async () => {
    // Windows of 2 MiB on every connection: a subframe costs at most half
    // the window.
    const p = await piped({ options: { ...fast, initialWindow: 2 * 1024 * 1024 } });
    const big = pattern(900 * 1024, 7);
    const atBackend = take(p.backend, 1);
    void p.client.send(big);
    expect(bytesEqual((await atBackend)[0] ?? new Uint8Array(0), big)).toBe(true);
    const atClient = take(p.client, 1);
    void p.backend.send(big);
    expect(bytesEqual((await atClient)[0] ?? new Uint8Array(0), big)).toBe(true);
    await p.backend.close();
    await p.pipe;
  });

  it('client close status reaches the backend', async () => {
    const p = await piped();
    const atBackend = framesOf(p.backend);
    void p.client.send(bytes('last'));
    await p.client.close(Status.of(StatusCode.cancelled, 'bye'));
    expect(await atBackend).toEqual([bytes('last')]);
    expectStatus(await p.backend.done, StatusCode.cancelled, 'bye');
    expectCode(await p.client.done, StatusCode.cancelled);
    await p.pipe;
  });

  it('backend close status reaches the client', async () => {
    const p = await piped();
    const atClient = framesOf(p.client);
    void p.backend.send(bytes('result'));
    await p.backend.close(Status.of(StatusCode.aborted, 'conflict'));
    expect(await atClient).toEqual([bytes('result')]);
    expectStatus(await p.client.done, StatusCode.aborted, 'conflict');
    await p.pipe;
  });

  it('an OK close passes as OK', async () => {
    const p = await piped();
    await p.client.close();
    expectCode(await p.backend.done, StatusCode.ok);
    await p.pipe;
  });

  it('lost backend connection is UNAVAILABLE at the client', async () => {
    const p = await piped();
    await p.backendConnection.close();
    expectCode(await p.client.done, StatusCode.unavailable);
    await p.pipe;
  });

  it('lost client connection is UNAVAILABLE at the backend', async () => {
    const p = await piped();
    await p.clientConnection.close();
    expectCode(await p.backend.done, StatusCode.unavailable);
    await p.pipe;
  });

  if (overFrameMux()) {
    it('an application code from a broken peer is sent as UNKNOWN', async () => {
      const [clientConnection, proxyIn] = muxPair();
      const [proxyTransport, raw] = memoryTransportPair({ delay: 0 });
      const proxyOut = connection(proxyTransport, true);
      const frames = raw.readable.getReader();
      const peer = raw.writable.getWriter();
      const client = clientConnection.open(bytes('x'));
      const inbound = await new Pull(proxyIn.incoming).next();
      const pipe = pipeChannels(inbound, proxyOut.open(inbound.openPayload));
      let frame: MuxFrame;
      do {
        const { value } = await frames.read();
        frame = MuxFrame.decode(value as Uint8Array);
      } while (frame.command !== MuxCommand.open);
      // Mux CLOSE must not carry application codes; this peer does anyway.
      await peer.write(MuxFrame.close(frame.channelId, new Status(300)).encode());
      expectCode(await client.done, StatusCode.unknown);
      await pipe;
      await peer.close();
      await frames.cancel();
    });
  } else {
    it('an application code from a broken peer is sent as UNKNOWN', async () => {
      const p = await piped();
      // Mux CLOSE must not carry application codes; the toy peer sends one anyway.
      await p.backend.close(new Status(300));
      const status = await p.client.done;
      expectStatus(status, StatusCode.unknown, 'application status 300');
      await p.pipe;
    });
  }

  it('frames sent before the pipe is attached are not lost', async () => {
    const p = await piped({
      early: (client) => {
        void client.send(bytes('one'));
        void client.send(new Uint8Array(0));
        void client.send(bytes('three'));
        void client.close(Status.of(StatusCode.cancelled));
      },
    });
    expect(await framesOf(p.backend)).toEqual([bytes('one'), new Uint8Array(0), bytes('three')]);
    expectCode(await p.backend.done, StatusCode.cancelled);
    await p.pipe;
  });

  if (overFrameMux()) {
    it('a subframe over the receiving peer limit closes both', async () => {
      const p = await piped({ backendOptions: { ...fast, maxFrameSize: 1000 } });
      void p.client.send(pattern(2000));
      expectCode(await p.client.done, StatusCode.frameTooLarge);
      expectCode(await p.backend.done, StatusCode.frameTooLarge);
      await p.pipe;
    });
  }

  it('a stream already listened to closes both channels', async () => {
    const [clientConnection, proxyIn] = muxPair();
    const [proxyOut, backendConnection] = muxPair();
    const client = clientConnection.open(bytes('x'));
    const inbound = await new Pull(proxyIn.incoming).next();
    const atBackend = new Pull(backendConnection.incoming).next();
    const outbound = proxyOut.open(inbound.openPayload);
    inbound.readable.getReader();
    await pipeChannels(inbound, outbound);
    expectCode(await client.done, StatusCode.internal);
    expectCode(await (await atBackend).done, StatusCode.internal);
  });

  if (overFrameMux()) {
    it('a far end that cancels its subscription without closing keeps the pipe waiting for done', async () => {
      const p = await piped();
      // Cancelling drops further DATA but does not close the channel.
      await p.backend.readable.cancel();
      let finished = false;
      void p.pipe.then(() => {
        finished = true;
      });
      void p.client.send(bytes('dropped'));
      await p.clientConnection.ping();
      await p.backendConnection.ping();
      await pump();
      expect(finished).toBe(false);
      expect(p.client.state).toBe('open');
      expect(p.backend.state).toBe('open');
      // The other direction still flows.
      void p.backend.send(bytes('still'));
      expect(await nextText(p.client)).toBe('still');
      await p.backend.close();
      await p.pipe;
      expect(finished).toBe(true);
      expectCode(await p.client.done, StatusCode.ok);
    });
  }

  it('talk works through the pipe', async () => {
    const p = await piped();
    const backend = new TalkChannel(p.backend);
    const items: string[] = [];
    void backend.serve((m) => {
      const s = m.replyStreamRequest(m.payload, { procedure: 'CHAIN' });
      void (async () => {
        try {
          for await (const item of s.items) items.push(item.text());
        } catch {
          // The channel closed.
        }
      })();
    });
    const client = new TalkChannel(p.client);
    const reply = await client.request('ASK', 'q');
    expect(reply.procedureName).toBe('CHAIN');
    expect(reply.expectsStream).toBe(true);
    void reply.replyItem('item');
    void reply.reply();
    await until(() => items.length > 0);
    expect(items).toEqual(['item']);
    await client.close();
    await p.pipe;
    expectCode(await backend.done, StatusCode.ok);
  });
});

/** The answer of the request [procedure] with [payload] on [talk], as text. */
async function ask(talk: TalkChannel, procedure: string, payload?: Uint8Array): Promise<string> {
  return (await talk.request(procedure, payload)).text();
}

/** A node that is a copy of [uri] in upper case: another spelling of the same `mem` listener. */
function upper(uri: string): string {
  return uri.toUpperCase().replace('MEM://', 'mem://');
}

describe('proxyHandler', () => {
  let backend: Switchboard;
  let endpoint: Switchboard;
  let client: Switchboard;
  let backendUri: string;
  let endpointUri: string;
  let atBackend: IncomingChannel[];
  // The TICKS source of the backend: the items it produced, and its end.
  let ticksProduced: number;
  let ticksEnded: Deferred<void>;

  function record(type: Name, instance: number, endpoints: string[]): ServiceRecord {
    return serviceRecord(new ServiceAddress(type, instance), { endpoints });
  }

  function staticResolver(records: ServiceRecord[]): StaticResolver {
    const resolver = new StaticResolver(records);
    onCleanup(() => resolver.close());
    return resolver;
  }

  /** The TICKS items: endless, paced by the requester's item window. */
  async function* ticks(): AsyncGenerator<Uint8Array> {
    try {
      for (let i = 0; ; i++) {
        ticksProduced++;
        yield bytes(`${i}`);
      }
    } finally {
      ticksEnded.resolve();
    }
  }

  beforeEach(async () => {
    atBackend = [];
    ticksProduced = 0;
    ticksEnded = new Deferred<void>();
    backend = node({ defaultPayload: bytes('backend credential') });
    backendUri = await backend.listenMemory();
    backend.registerService(
      chat,
      (incoming) => {
        atBackend.push(incoming);
        void incoming.talk().serve((m) => {
          switch (m.procedureName) {
            case 'WHO': {
              const a = incoming.address;
              void m.reply(
                `${a.type} ${a.instance} ${a.shard ?? 'null'} ${a.host ?? 'null'} ` +
                  utf8Decode(a.payload),
              );
              break;
            }
            case 'COUNT':
              for (let i = 1; i <= 3; i++) {
                void m.replyItem(`${i}`);
              }
              void m.reply('done');
              break;
            case 'KILL':
              void incoming.reject(Status.of(StatusCode.aborted, 'killed'));
              break;
            case 'PUT': {
              const number = m.frame.bulk ? m.frame.bulkReference.number : 0;
              const intact = bytesEqual(m.payload, pattern(m.payload.length));
              void m.reply(`${m.payload.length} ${m.isBulk} ${number}${intact ? '' : ' corrupt'}`);
              break;
            }
            case 'GET':
              void m.reply(undefined, { bulk: [pattern(300_000)], bulkLength: 300_000 });
              break;
            case 'TICKS':
              void m.replyStream(ticks());
              break;
          }
        });
      },
      { instance: 7 },
    );
    const backends = staticResolver([
      record(chat, 7, [backendUri]),
      record(Name.of('down'), 1, ['mem://nobody-listens-here']),
    ]);
    endpoint = node({ defaultPayload: bytes('endpoint credential') });
    const allowed = [chat, Name.of('void'), Name.of('down')];
    endpoint.catchAll = proxyHandler(endpoint, {
      allow: (address) => allowed.some((type) => type.equals(address.type)),
      resolver: backends,
    });
    endpoint.registerService('api', (incoming) => {
      void incoming.talk().serve((m) => void m.reply('local'));
    });
    endpointUri = await endpoint.listenMemory();
    client = node({
      resolver: new EndpointResolver(endpointUri),
      defaultPayload: bytes('client token'),
    });
  });

  it('request and reply through the endpoint', async () => {
    const talk = await client.openTalk(chat);
    // The backend sees the client's own payload and the instance the proxy
    // selected; the shard slot passes through.
    expect(await ask(talk, 'WHO')).toBe('chat 7 null null client token');
    const sharded = await client.openTalk(chat, { shard: 9 });
    expect(await ask(sharded, 'WHO')).toBe('chat 7 9 null client token');
    // Local services on the endpoint are not proxied.
    const api = await client.openTalk('api');
    expect(await ask(api, 'X')).toBe('local');
    await talk.close();
    await sharded.close();
    await api.close();
  });

  if (overFrameMux()) {
    it('bulk payloads through the endpoint, both ways', async () => {
      const talk = await client.openTalk(chat);
      // The bulk number is kept: the BULK message passes unchanged.
      for (const number of [1, 2]) {
        expect(await ask(talk, 'PUT', pattern(200_000))).toBe(`200000 true ${number}`);
      }
      const streamed = await talk.request('PUT', undefined, {
        bulk: [pattern(50_000), pattern(50_000, 50_000 * 31)],
      });
      expect(streamed.text()).toBe('100000 true 3');
      const got = await talk.request('GET');
      expect(got.isBulk).toBe(true);
      expect(bytesEqual(got.payload, pattern(300_000))).toBe(true);
      // Inline still inline.
      expect(await ask(talk, 'PUT', pattern(10))).toBe('10 false 0');
      await talk.close();
    });
  }

  it('bulk channels count toward the per-connection bound', async () => {
    const tight = node();
    tight.catchAll = proxyHandler(tight, {
      allow: (address) => chat.equals(address.type),
      resolver: staticResolver([record(chat, 7, [backendUri])]),
      maxChannelsPerConnection: 2,
    });
    const tightUri = await tight.listenMemory();
    const c = node({ resolver: new EndpointResolver(tightUri) });
    const parent = await c.openChannel(chat);
    // The parent is one, a first bulk channel the second.
    const first = parent.openAfter(new TalkBulkOpen(parent.id, 1).encode());
    const second = parent.openAfter(new TalkBulkOpen(parent.id, 2).encode());
    expectCode(await second.done, StatusCode.resourceExhausted);
    expect(first.state).toBe('open');
    await first.close();
    await parent.close();
  });

  it('stream through the endpoint', async () => {
    const talk = await client.openTalk(new ServiceAddress(chat, 7));
    const stream = talk.streamRequest('COUNT');
    const items: string[] = [];
    for await (const item of stream.items) items.push(item.text());
    expect(items).toEqual(['1', '2', '3']);
    expect((await stream.done).text()).toBe('done');
    await talk.close();
  });

  it('a stream with a finite item window through the endpoint: grants, a request served meanwhile, a cancel', async () => {
    const talk = await client.openTalk(new ServiceAddress(chat, 7));
    const s = talk.streamRequest('TICKS', undefined, { window: 2 });
    const items: string[] = [];
    void (async () => {
      try {
        for await (const item of s.items) items.push(item.text());
      } catch {
        // Cancelled.
      }
    })();
    const quiet = (): Promise<void> => new Promise((resolve) => setTimeout(resolve, 50));
    while (items.length < 2) {
      await quiet();
    }
    await quiet();
    // Nothing beyond the window; one item read ahead at the backend.
    expect(items).toEqual(['0', '1']);
    expect(ticksProduced).toBe(3);
    // The channel serves other calls while the stream waits.
    expect(await ask(talk, 'WHO')).toMatch(/^chat 7/);
    s.grant(3);
    while (items.length < 5) {
      await quiet();
    }
    await quiet();
    expect(items).toEqual(['0', '1', '2', '3', '4']);
    expect(ticksProduced).toBe(6);
    s.cancel();
    await rejectsWith(s.done, StatusCode.cancelled);
    await ticksEnded.promise;
    await quiet();
    expect(items).toHaveLength(5);
    await talk.close();
  });

  it('rejections', async () => {
    const statusOf = async (type: string, instance = 0): Promise<Status> =>
      (await client.openChannel(new ServiceAddress(Name.of(type), instance))).done;
    expectCode(await statusOf('admin'), StatusCode.permissionDenied);
    expectCode(await statusOf('void'), StatusCode.notFound);
    expectCode(await statusOf('chat', 8), StatusCode.notFound);
    expectCode(await statusOf('down'), StatusCode.unavailable);
    const untyped = await client.openChannelAt(endpointUri, new ChannelAddress());
    expectCode(await untyped.done, StatusCode.permissionDenied);
  });

  it('a channel without a type is not found when allowed', async () => {
    const open = node();
    open.catchAll = proxyHandler(open);
    const uri = await open.listenMemory();
    const channel = await client.openChannelAt(uri, new ChannelAddress());
    expectCode(await channel.done, StatusCode.notFound);
    // No resolver and no host hint support: the proxy cannot forward,
    // which the client sees as UNAVAILABLE.
    const typed = await client.openChannelAt(uri, new ChannelAddress({ type: chat }));
    expectStatus(await typed.done, StatusCode.unavailable, 'unavailable');
  });

  it('backend close status reaches the client', async () => {
    const talk = await client.openTalk(chat);
    void talk.send('KILL');
    expectStatus(await talk.done, StatusCode.aborted, 'killed');
  });

  it('client close status reaches the backend', async () => {
    const talk = await client.openTalk(chat);
    await talk.request('WHO');
    await talk.close(Status.of(StatusCode.cancelled, 'leaving'));
    expect(atBackend).toHaveLength(1);
    expectStatus(
      await (atBackend[0] as IncomingChannel).channel.done,
      StatusCode.cancelled,
      'leaving',
    );
  });

  it('backend connection loss is UNAVAILABLE at the client', async () => {
    const accepted = new Pull(backend.connections).next();
    const talk = await client.openTalk(chat);
    await talk.request('WHO');
    await (await accepted).close();
    expectCode(await talk.done, StatusCode.unavailable);
  });

  it('backend shutdown is GOING_AWAY at the client', async () => {
    const talk = await client.openTalk(chat);
    await talk.request('WHO');
    const pending = rejectsWith(talk.request('NEVER'), StatusCode.goingAway);
    await backend.close();
    await pending;
    expectCode(await talk.done, StatusCode.goingAway);
  });

  it('host hint relay', async () => {
    const relay = node({ allowHostHint: true });
    relay.catchAll = proxyHandler(relay);
    const relayUri = await relay.listenMemory();
    for (const hint of [backendUri, upper(backendUri)]) {
      const talk = await client.openTalkAt(
        relayUri,
        new ChannelAddress({ type: chat, instance: 7, host: hint }),
      );
      // The relay strips the hint and keeps everything else.
      expect(await ask(talk, 'WHO')).toBe('chat 7 null null client token');
      await talk.close();
    }
    const bad = await client.openChannelAt(
      relayUri,
      new ChannelAddress({ type: chat, host: 'tcp://' }),
    );
    expectCode(await bad.done, StatusCode.invalidArgument);
    const dead = await client.openChannelAt(
      relayUri,
      new ChannelAddress({ type: chat, host: 'mem://nobody-listens-here' }),
    );
    expectCode(await dead.done, StatusCode.unavailable);
  });

  it('rejections carry no detail about the mesh', async () => {
    const down = await client.openChannel('down');
    const downStatus = await down.done;
    expectStatus(downStatus, StatusCode.unavailable, 'unavailable');
    const missing = await client.openChannel(new ServiceAddress(chat, 8));
    const missingStatus = await missing.done;
    expectStatus(missingStatus, StatusCode.notFound, 'not found');
    const denied = await client.openChannel('admin');
    expect((await denied.done).reason).toBe('permission denied');
    for (const status of [downStatus, missingStatus]) {
      expect(status.reason).not.toContain('mem');
      expect(status.reason).not.toContain(backendUri.slice('mem://'.length));
    }
  });

  it('the default allow refuses reserved types', async () => {
    const ns = Name.of('_ns');
    backend.registerService(
      ns,
      (incoming) => {
        void incoming.channel.send(bytes('naming'));
        void incoming.channel.close();
      },
      { instance: 1 },
    );
    const table = staticResolver([record(ns, 1, [backendUri]), record(chat, 7, [backendUri])]);
    const open = node();
    open.catchAll = proxyHandler(open, { resolver: table });
    const openUri = await open.listenMemory();
    const refused = await client.openChannelAt(openUri, new ChannelAddress({ type: ns }));
    expectCode(await refused.done, StatusCode.permissionDenied);
    const talk = await client.openTalkAt(openUri, new ChannelAddress({ type: chat }));
    expect((await talk.request('WHO')).payload.length).toBeGreaterThan(0);
    await talk.close();
    // Proxying a reserved type takes an explicit allow.
    const explicit = node();
    explicit.catchAll = proxyHandler(explicit, {
      resolver: table,
      allow: (address) => ns.equals(address.type),
    });
    const explicitUri = await explicit.listenMemory();
    const allowed = await client.openChannelAt(explicitUri, new ChannelAddress({ type: ns }));
    expect(await nextText(allowed)).toBe('naming');
  });

  it('a record pointing back at the proxy is refused at once', async () => {
    const table = staticResolver([]);
    const loop = node({ resolver: table });
    let hops = 0;
    const proxy = proxyHandler(loop, { allow: (a) => chat.equals(a.type) });
    loop.catchAll = (incoming) => {
      hops++;
      return proxy(incoming);
    };
    const first = await loop.listenMemory();
    const second = await loop.listenMemory({ name: 'mesh' });
    table.add(record(chat, 9, [first]));
    // Other spellings of the same listeners.
    table.add(record(chat, 10, [upper(first), 'mem://MESH']));
    const looping = node({ resolver: new EndpointResolver(first) });
    for (const instance of [9, 10]) {
      hops = 0;
      const started = Date.now();
      const channel = await looping.openChannel(new ServiceAddress(chat, instance));
      expectStatus(await channel.done, StatusCode.unavailable, 'unavailable');
      expect(Date.now() - started).toBeLessThan(2000);
      expect(hops).toBe(1);
    }
    expect(loop.isOwnEndpoint(first)).toBe(true);
    expect(loop.isOwnEndpoint(second)).toBe(true);
    expect(loop.isOwnEndpoint('mem://other')).toBe(false);
    expect(loop.isOwnEndpoint(backendUri)).toBe(false);
  });

  it('a host hint pointing at the relay itself is refused', async () => {
    const relay = node({ allowHostHint: true });
    let hops = 0;
    const proxy = proxyHandler(relay);
    relay.catchAll = (incoming) => {
      hops++;
      return proxy(incoming);
    };
    const relayUri = await relay.listenMemory();
    expect(relay.isOwnEndpoint(upper(relayUri))).toBe(true);
    for (const hint of [relayUri, upper(relayUri)]) {
      hops = 0;
      const channel = await client.openChannelAt(
        relayUri,
        new ChannelAddress({ type: chat, host: hint }),
      );
      expectCode(await channel.done, StatusCode.unavailable);
      expect(hops).toBe(1);
    }
  });

  it('one client filling its allowance does not starve another', async () => {
    // The backend accepts 4 channels on a connection and the proxy keeps
    // one connection to it; each client may hold 3.
    const hold = Name.of('hold');
    const smallBackend = node({ muxOptions: { maxChannels: 4 } });
    const held: IncomingChannel[] = [];
    smallBackend.registerService(hold, (incoming) => void held.push(incoming), { instance: 1 });
    const smallUri = await smallBackend.listenMemory();
    const table = staticResolver([record(hold, 1, [smallUri])]);
    const proxyNode = node({ maxConnectionsPerEndpoint: 1 });
    proxyNode.catchAll = proxyHandler(proxyNode, {
      resolver: table,
      maxChannelsPerConnection: 3,
    });
    const proxyUri = await proxyNode.listenMemory();
    const toHold = new ChannelAddress({ type: hold });
    // Warm up the proxy's connection so that the backend's LIMITS are known.
    const warm = await client.openChannelAt(proxyUri, toHold);
    await until(() => held.length === 1);
    await (held[0] as IncomingChannel).channel.close();
    await warm.done;
    held.length = 0;

    const greedy = node();
    const greedyChannels: MuxChannel[] = [];
    for (let i = 0; i < 5; i++) {
      greedyChannels.push(await greedy.openChannelAt(proxyUri, toHold));
    }
    const refused = await Promise.all(greedyChannels.slice(3).map((channel) => channel.done));
    for (const status of refused) {
      expectCode(status, StatusCode.resourceExhausted);
    }
    await until(() => held.length === 3);
    // Another client still gets through.
    const polite = node();
    const channel = await polite.openChannelAt(proxyUri, toHold);
    await until(() => held.length === 4);
    void (held[3] as IncomingChannel).channel.send(bytes('served'));
    expect(await nextText(channel)).toBe('served');
    // A channel of the greedy client that ends frees its allowance, once
    // the proxy has closed both sides of it.
    const toBackend = await proxyNode.connect(smallUri);
    expect(toBackend.openChannelCount).toBe(4);
    await (greedyChannels[0] as MuxChannel).close();
    await until(() => toBackend.openChannelCount <= 3);
    await pump();
    const more = await greedy.openChannelAt(proxyUri, toHold);
    await until(() => held.length === 5);
    void (held[4] as IncomingChannel).channel.send(bytes('again'));
    expect(await nextText(more)).toBe('again');
  });

  it('a backend GOAWAY between connect and OPEN is retried once', async () => {
    const accepted: MuxConnection[] = [];
    void (async () => {
      for await (const connection of backend.connections) {
        accepted.push(connection);
      }
    })();
    const table = staticResolver([record(chat, 7, [backendUri])]);
    const racing = new RacingSwitchboard();
    onCleanup(() => racing.close());
    racing.catchAll = proxyHandler(racing, { resolver: table });
    const racingUri = await racing.listenMemory();
    // The proxy's pooled connection to the backend.
    const warm = await client.openTalkAt(racingUri, new ChannelAddress({ type: chat }));
    await warm.request('WHO');
    await warm.close();
    const pooled = await racing.connect(backendUri);
    racing.beforeReturn = async (connection) => {
      expect(connection).toBe(pooled);
      void (accepted[0] as MuxConnection).goAway();
      await until(() => connection.peerGoingAway);
    };
    const talk = await client.openTalkAt(racingUri, new ChannelAddress({ type: chat }));
    expect(await ask(talk, 'WHO')).toBe('chat 7 null null client token');
    expect(accepted).toHaveLength(2);
    expect(racing.beforeReturn).toBeUndefined();
    await talk.close();
  });

  it('without allowHostHint the hint is ignored and stripped', async () => {
    const talk = await client.openTalkAt(
      endpointUri,
      new ChannelAddress({ type: chat, host: 'tcp://10.0.0.1:1' }),
    );
    expect(await ask(talk, 'WHO')).toBe('chat 7 null null client token');
    await talk.close();
  });
});

/**
 * A node whose next {@link connect} hands out the connection only after
 * {@link beforeReturn} ran on it, to make a backend's GOAWAY arrive
 * between connect() and the OPEN.
 */
class RacingSwitchboard extends Switchboard {
  /** Runs once, on the next connection {@link connect} returns. */
  beforeReturn: ((connection: MuxConnection) => Promise<void>) | undefined;

  constructor() {
    super({ runtime: currentRuntime(), muxOptions: fast });
  }

  override async connect(
    endpoint: string,
    options: {
      policy?: ChannelPolicy | undefined;
      record?: ServiceRecord | undefined;
      signal?: AbortSignal | undefined;
    } = {},
  ): Promise<MuxConnection> {
    const connection = await super.connect(endpoint, options);
    const hook = this.beforeReturn;
    this.beforeReturn = undefined;
    await hook?.(connection);
    return connection;
  }
}

describe('proxyHandler authorize', () => {
  let atBackend: IncomingChannel[];
  let backends: StaticResolver;
  let client: Switchboard;

  beforeEach(async () => {
    atBackend = [];
    const backend = node();
    backend.registerService(
      chat,
      (incoming) => {
        atBackend.push(incoming);
        void incoming.talk().serve((m) => {
          const a = incoming.address;
          void m.reply(`${a.instance} ${a.shard ?? 'null'} ${utf8Decode(a.payload)}`);
        });
      },
      { instance: 7 },
    );
    const backendUri = await backend.listenMemory();
    backends = new StaticResolver([
      serviceRecord(new ServiceAddress(chat, 7), { endpoints: [backendUri] }),
    ]);
    onCleanup(() => backends.close());
  });

  /** A proxy with [authorize], and {@link client} connected to it. */
  async function start(
    authorize: (
      incoming: IncomingChannel,
    ) => ChannelAddress | null | undefined | Promise<ChannelAddress | null | undefined>,
    options: { authorizeTimeout?: number; budget?: number } = {},
  ): Promise<void> {
    const proxy = node();
    proxy.catchAll = proxyHandler(proxy, {
      authorize,
      authorizeTimeout: options.authorizeTimeout ?? 10_000,
      resolver: backends,
      maxChannelsPerConnection: options.budget ?? 256,
    });
    const uri = await proxy.listenMemory();
    client = node({ resolver: new EndpointResolver(uri), defaultPayload: bytes('token-5') });
  }

  it("a rewritten shard and payload reach the backend; the client's data waits for the verdict", async () => {
    const seen: ChannelAddress[] = [];
    await start(async (incoming) => {
      seen.push(incoming.address);
      await new Promise((resolve) => setTimeout(resolve, 20));
      const token = utf8Decode(incoming.address.payload);
      const account = Number(token.split('-').at(-1));
      return incoming.address.with({ shard: account, payload: bytes(`verified ${token}`) });
    });
    const talk = await client.openTalk(chat);
    // Sent at once, while the proxy is still authorizing.
    expect(await ask(talk, 'WHO')).toBe('7 5 verified token-5');
    expect(seen).toHaveLength(1);
    expect(seen[0]?.payload).toEqual(bytes('token-5'));
    expect(seen[0]?.shard).toBeUndefined();
    expect(atBackend[0]?.address.shard).toBe(5);
    await talk.close();
  });

  it('null is UNAUTHENTICATED, a throw PERMISSION_DENIED', async () => {
    await start(async (incoming) => {
      switch (utf8Decode(incoming.address.payload)) {
        case 'anonymous':
          return null;
        case 'bad':
          throw new SyntaxError('bad credential');
        default:
          return incoming.address;
      }
    });
    const statusWith = async (payload: string): Promise<Status> =>
      (await client.openChannel(chat, { payload: bytes(payload) })).done;
    expectStatus(await statusWith('anonymous'), StatusCode.unauthenticated, 'unauthenticated');
    expectCode(await statusWith('bad'), StatusCode.permissionDenied);
    expect(atBackend).toHaveLength(0);
    // A synchronous answer works too.
    const talk = await client.openTalk(chat);
    expect(await ask(talk, 'WHO')).toBe('7 null token-5');
    await talk.close();
  });

  it('no answer within authorizeTimeout is UNAUTHENTICATED; pending authorizations count toward the budget', async () => {
    let answer: (address: ChannelAddress) => void = () => {};
    const pending = new Promise<ChannelAddress>((resolve) => {
      answer = resolve;
    });
    await start(() => pending, { authorizeTimeout: 30, budget: 1 });
    const first = await client.openChannel(chat);
    await pump();
    const second = await client.openChannel(chat);
    expectCode(await second.done, StatusCode.resourceExhausted);
    expectCode(await first.done, StatusCode.unauthenticated);
    // The late answer is ignored.
    answer(new ChannelAddress({ type: chat }));
    await pump();
    expect(atBackend).toHaveLength(0);
  });

  it('a channel its client closed while being authorized is dropped', async () => {
    let verdict: () => void = () => {};
    const decided = new Promise<void>((resolve) => {
      verdict = resolve;
    });
    let asked: () => void = () => {};
    const wasAsked = new Promise<void>((resolve) => {
      asked = resolve;
    });
    await start(async (incoming) => {
      asked();
      await decided;
      return incoming.address;
    });
    const channel = await client.openChannel(chat);
    await wasAsked;
    await channel.close(Status.of(StatusCode.cancelled));
    verdict();
    await pump();
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(atBackend).toHaveLength(0);
  });
});
