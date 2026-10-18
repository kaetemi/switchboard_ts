/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Review finding (Talk): over a connection whose channels are not ordered
// (the QUIC binding), the allowance `#unreadReferences` adds to
// `maxUnclaimedBulk` counts the frames Talk "may still read on"
// (`#readOnRoom`: a whole `initialWindow` while nothing was read on) as if
// they were frames that had arrived. With a parent that sent nothing, a
// peer kept 16 + initialWindow / sendCost(11) unreferenced bulk channels
// open per Talk channel (2443 over the memory mux, 4385 over the binding,
// whose overhead is 4 bytes), each holding up to its window, where Dart and
// the frame mux refuse the 17th RESOURCE_EXHAUSTED at once. The bound now:
// the allowance counts references at the smallest BULK frame (14 bytes),
// and beyond `maxUnclaimedBulk` the unreferenced channels admitted on it
// hold no more than `reassemblyBudget` together (design section 12, item
// 14, and section 13, item 6): an idle parent admits at most 16 plus the
// budget's worth of windows (16 + 256 at the defaults), and the bursts of
// `talk-quic.test.ts` and `quic-node-native.test.ts` still go through.
// Ordered channels do not read on: 16, as in Dart.

import { expect, test } from 'vitest';
import { defaultMuxOptions, type MuxChannel } from '../../../src/core/mux/contract.ts';
import type { QuicStream } from '../../../src/core/quic/contract.ts';
import { RecordReader } from '../../../src/core/quic/record-reader.ts';
import {
  decodeCloseBody,
  encodeDataRecord,
  encodeOpenRecord,
  type QuicRecord,
  QuicRecordCommand,
  quicDefaultRecordLimit,
} from '../../../src/core/quic/records.ts';
import { StatusCode } from '../../../src/core/status.ts';
import { defaultTalkOptions, type TalkOptions } from '../../../src/core/talk/contract.ts';
import { TalkBulkOpen } from '../../../src/core/talk/talk-bulk.ts';
import { installBulkRouting } from '../../../src/core/talk/talk-bulk-routes.ts';
import { TalkChannel } from '../../../src/core/talk/talk-channel.ts';
import { bulkOpen } from '../../support/talk/frames.ts';
import { flush } from '../../support/talk/helpers.ts';
import { memoryMuxPair } from '../../support/talk/memory-mux.ts';
import { bindingAndRaw, concat, nextChannel } from '../quic/harness.ts';

const empty = new Uint8Array(0);

/** Unreferenced bulk channels opened for an idle parent. */
const opened = 40;

/** A reassembly budget of 8 windows: 8 unreferenced channels beyond the 16, where Talk reads on. */
const budget = 8 * defaultMuxOptions.initialWindow;

/** How many of the 40 are refused RESOURCE_EXHAUSTED: 24 under Dart's bound of 16, 16 where the budget admits 8 more. */
const refusedOrdered = opened - 16;
const refusedReadingOn = opened - 16 - 8;

/** Opens [count] unreferenced bulk channels for an idle Talk channel with [options] over the memory mux; the ids of those refused RESOURCE_EXHAUSTED. */
async function refusedOverMemory(
  ordered: boolean,
  count: number,
  options: Partial<TalkOptions>,
): Promise<number[]> {
  const [client, server] = memoryMuxPair({ ordered });
  installBulkRouting(client);
  installBulkRouting(server);
  const raw = client.open(empty);
  const talk = new TalkChannel(await server.accept(), options);
  void talk.serve(() => {});
  await flush();
  const bulks: MuxChannel[] = [];
  for (let n = 1; n <= count; n++) {
    const bulk = raw.openAfter(bulkOpen(raw, n));
    bulk.priority = 'bulk';
    // What arrived counts with what its window still lets the peer send.
    void bulk.send(new Uint8Array(30_000)).catch(() => {});
    bulks.push(bulk);
  }
  await flush(20);
  const refused: number[] = [];
  for (const bulk of bulks) {
    if (bulk.state === 'closed' && (await bulk.done).code === StatusCode.resourceExhausted) {
      refused.push(bulk.id);
    }
  }
  await client.close();
  await server.close();
  return refused;
}

test('bulk channels beyond maxUnclaimedBulk for an idle parent are refused, ordered channels (16)', async () => {
  expect(await refusedOverMemory(true, opened, { reassemblyBudget: budget })).toHaveLength(
    refusedOrdered,
  );
});

test('bulk channels beyond maxUnclaimedBulk for an idle parent are refused, unordered channels (16 and the budget)', async () => {
  expect(await refusedOverMemory(false, opened, { reassemblyBudget: budget })).toHaveLength(
    refusedReadingOn,
  );
});

test('with the default options an idle parent holds at most 16 unreferenced bulk channels and the budget, unordered channels', async () => {
  // 16 + 16 MiB / 64 KiB, where 2443 were admitted before the bound.
  const beyond = Math.floor(defaultTalkOptions.reassemblyBudget / defaultMuxOptions.initialWindow);
  const admitted = defaultTalkOptions.maxUnclaimedBulk + beyond;
  expect(await refusedOverMemory(false, admitted + 20, {})).toHaveLength(20);
});

test('with no reassembly budget an idle parent holds 16 unreferenced bulk channels, as in Dart, unordered channels', async () => {
  expect(await refusedOverMemory(false, opened, { reassemblyBudget: 0 })).toHaveLength(
    refusedOrdered,
  );
});

test('over the QUIC binding, bulk streams beyond maxUnclaimedBulk and the budget for an idle parent are refused', async () => {
  const [connection, peer] = await bindingAndRaw();
  installBulkRouting(connection);
  const arrived = nextChannel(connection);
  await peer.openRaw(encodeOpenRecord(2, empty));
  const talk = new TalkChannel(await arrived, { reassemblyBudget: budget });
  void talk.serve(() => {});
  const streams: QuicStream[] = [];
  for (let n = 1; n <= opened; n++) {
    streams.push(
      await peer.openRaw(
        concat([
          encodeOpenRecord(2 + 2 * n, new TalkBulkOpen(2, n).encode()),
          encodeDataRecord(new Uint8Array(1000)),
        ]),
      ),
    );
  }
  // The first record the binding writes back on each stream, if any
  // within 500 ms of real time: a CLOSE for a refused one.
  const answers = await Promise.all(
    streams.map((stream) =>
      Promise.race([
        new RecordReader(stream).next(quicDefaultRecordLimit),
        new Promise<undefined>((resolve) => setTimeout(() => resolve(undefined), 500)),
      ]),
    ),
  );
  const refused = answers.filter(
    (record: QuicRecord | undefined) =>
      record?.command === QuicRecordCommand.close &&
      decodeCloseBody(record.body).code === StatusCode.resourceExhausted,
  );
  await connection.close();
  expect(refused).toHaveLength(refusedReadingOn);
});
