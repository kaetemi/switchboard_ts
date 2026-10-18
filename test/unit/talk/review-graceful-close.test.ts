/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Review findings (Talk): a graceful close of the parent loses bulk
// payloads the peer sent whole before it. Talk reads the parent and each
// bulk channel through ReadableStreams of their own, so when the mux takes
// several channels' frames in one go (the memory mux here; the native QUIC
// provider dispatches a batch of stream events in one callback) the
// parent's end can be processed before a bulk channel's reader drained
// what the mux had already received. In Dart the mux delivers in wire
// order, so a bulk channel that ended before the parent's CLOSE ended
// before Talk saw that CLOSE.
//
// 1. `#terminate` (`#endBulk`) abandons every BulkIn still in `_bulkIns`.
//    A streamed payload (`streamBulk`) whose bulk channel had closed but
//    whose reader had not reached its end is abandoned while its message
//    is still delivered: `message.bulk` then throws "the bulk payload is
//    read already" and the bytes are gone. `#peerEnded` waits for the
//    reassemblies only. (Over the frame mux the readers stayed in step in
//    every probe: it reads the transport a frame per await.)
// 2. Over unordered channels, `#linger` returns at once when every
//    attached bulk channel is in state `closed` (`#bulkSettled`), without
//    waiting for the reassemblies `#peerEnded` waits for over ordered ones;
//    `#terminate` then drops the message whose payload was still being
//    read from the closed channel's queue.
// 3. Over unordered channels, a bulk channel the mux routes between the
//    parent's end and `#linger` registering the lingering target (Talk's
//    reader lags the mux, then awaits `raw.done`) finds no open parent and
//    no lingering target: it is held as a channel whose parent has not
//    arrived (`PendingParents`), never handed to Talk, and refused after
//    `heldBulkTimeout`; the message waiting for it is dropped when
//    `bulkOpenTimeout` ends the linger. Dart awaits `done` before
//    `_linger` too.
//
// A peer that sends the parent's CLOSE right behind its bulk channels is
// allowed ("a peer need only have sent them", dart-design.md); a Talk
// sender waits for their confirmation first, a raw peer or a relay need
// not. The tests are `test.fails` while the defects are there: the fixes
// flip them to `test`.

import { expect, test } from 'vitest';
import { MuxFrame } from '../../../src/core/mux/mux-frame.ts';
import { Name } from '../../../src/core/name.ts';
import { Status } from '../../../src/core/status.ts';
import type { TalkMessage } from '../../../src/core/talk/contract.ts';
import { TalkBulkOpen, TalkBulkReference } from '../../../src/core/talk/talk-bulk.ts';
import { installBulkRouting } from '../../../src/core/talk/talk-bulk-routes.ts';
import { TalkChannel } from '../../../src/core/talk/talk-channel.ts';
import { TalkFrame, TalkKind } from '../../../src/core/talk/talk-frame.ts';
import { ManualClock } from '../../../src/testing/manual-clock.ts';
import { collect, pattern } from '../../support/talk/bulk-peers.ts';
import { nextIncoming, rawMuxPair } from '../../support/talk/frame-mux-peers.ts';
import { bulkMessage, bulkOpen } from '../../support/talk/frames.ts';
import { expectBytes, flush, Outcome, Queue } from '../../support/talk/helpers.ts';
import { memoryMuxPair } from '../../support/talk/memory-mux.ts';

const empty = new Uint8Array(0);

/** 40 chunks of 1000 bytes: a reader takes a few microtasks per chunk. */
const chunks = 40;
const payload = pattern(chunks * 1000);

/** A plain message PUT whose payload is bulk 1, [length] bytes. */
function put(length: number): Uint8Array {
  return new TalkFrame({ kind: TalkKind.message, procedure: Name.of('PUT') })
    .withBulk(new TalkBulkReference(1, length))
    .encode();
}

for (const ordered of [false, true]) {
  test.fails(`a streamed payload sent whole before the parent closed is still readable after the end, ${
    ordered ? 'ordered' : 'unordered'
  } channels`, async () => {
    const [client, server] = memoryMuxPair({ ordered });
    installBulkRouting(client);
    installBulkRouting(server);
    const raw = client.open(empty);
    const talk = new TalkChannel(await server.accept(), { streamBulk: () => true });
    // Held, then delivered at once in wire order (`release`).
    server.holding.add(raw.id);
    const bulk = raw.openAfter(bulkOpen(raw, 1));
    server.holding.add(bulk.id);
    for (let i = 0; i < chunks; i++) {
      void bulk.send(payload.subarray(i * 1000, (i + 1) * 1000));
    }
    void bulk.close();
    void raw.send(bulkMessage('PUT', 1, { length: payload.length }));
    void raw.close();
    await flush();
    server.release();
    expect((await talk.done).isOk).toBe(true);
    const message = await new Queue(talk.messages).next();
    const read = new Outcome((async () => collect(message.bulk))());
    await flush();
    await client.close();
    await server.close();
    expect(read.error).toBeUndefined();
    expectBytes(read.value as Uint8Array, payload);
  });
}

test('control: a reassembled payload sent whole before the parent closed is delivered (frame mux)', async () => {
  const [connection, peer] = rawMuxPair();
  peer.send(MuxFrame.open(2, empty));
  const talk = new TalkChannel(await nextIncoming(connection));
  const frames = [MuxFrame.open(4, new TalkBulkOpen(2, 1).encode())];
  for (let i = 0; i < chunks; i++) {
    frames.push(MuxFrame.data(4, payload.subarray(i * 1000, (i + 1) * 1000)));
  }
  frames.push(MuxFrame.close(4), MuxFrame.data(2, put(payload.length)), MuxFrame.close(2));
  peer.send(...frames);
  const messages = new Queue(talk.messages);
  expectBytes((await messages.next()).payload, payload);
});

test.fails('a reassembled payload sent whole before the parent closed is delivered (unordered channels)', async () => {
  const [client, server] = memoryMuxPair({ ordered: false });
  installBulkRouting(client);
  installBulkRouting(server);
  const raw = client.open(empty);
  const talk = new TalkChannel(await server.accept());
  server.holding.add(raw.id);
  const bulk = raw.openAfter(bulkOpen(raw, 1));
  server.holding.add(bulk.id);
  for (let i = 0; i < chunks; i++) {
    void bulk.send(payload.subarray(i * 1000, (i + 1) * 1000));
  }
  void bulk.close();
  void raw.send(bulkMessage('PUT', 1, { length: payload.length }));
  void raw.close();
  await flush();
  server.release();
  const messages = new Queue(talk.messages);
  const delivered = await messages.hasNext();
  await client.close();
  await server.close();
  expect(delivered).toBe(true);
  expectBytes((await messages.next()).payload, payload);
});

test.fails('a bulk channel routed just after its parent ended, before Talk lingers, is still handed to the message waiting for it (unordered channels)', async () => {
  const clock = new ManualClock();
  const [client, server] = memoryMuxPair({ ordered: false, mux: { clock } });
  installBulkRouting(client);
  installBulkRouting(server);
  const raw = client.open(empty);
  const serverRaw = await server.accept();
  const talk = new TalkChannel(serverRaw);
  const delivered: TalkMessage[] = [];
  void talk.serve((m) => {
    delivered.push(m);
  });
  // The bulk channel's frames lag behind the parent's (a stream each).
  const bulk = raw.openAfter(bulkOpen(raw, 1));
  server.holding.add(bulk.id);
  void bulk.send(Uint8Array.of(1, 2, 3));
  void bulk.close();
  await raw.send(bulkMessage('NOTE', 1, { length: 3 }));
  await flush();
  // The parent closes gracefully; the bulk channel arrives right after the
  // mux ended the parent, as the next stream's records may.
  void serverRaw.done.then(() => server.release(bulk.id));
  void raw.close();
  await flush(10);
  // Talk lingers at most bulkOpenTimeout (10 s) for a channel that never
  // reached it; well within that, the payload is there.
  await clock.elapseAsync(1000);
  const lost = delivered.length === 0;
  await clock.elapseAsync(20_000);
  await client.close();
  await server.close();
  expect(lost).toBe(false);
  expect([...(delivered[0] as TalkMessage).payload]).toEqual([1, 2, 3]);
  expect((await talk.done).equals(Status.ok)).toBe(true);
});
