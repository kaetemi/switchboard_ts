/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Review finding (Talk): over a connection whose channels are not ordered
// (the QUIC binding), the allowance `#unreadReferences` adds to
// `maxUnclaimedBulk` counts the frames Talk "may still read on"
// (`#readOnRoom`: a whole `initialWindow` while nothing was read on) as if
// they were frames that had arrived. With a parent that sends nothing, a
// peer keeps 16 + initialWindow / sendCost(11) unreferenced bulk channels
// open per Talk channel (2443 over the memory mux, 4385 over the binding,
// whose overhead is 4 bytes), each holding up to its window, where Dart and
// the frame mux refuse the 17th RESOURCE_EXHAUSTED at once. Design section
// 13, item 6, "Bounds", says such a peer "is refused once the parent's
// unread frames can no longer carry them"; with no frame at all on the
// parent, nothing is refused. The tests are `test.fails` while the defect
// is there: the fix flips them to `test`.

import { expect, test } from 'vitest';
import type { MuxChannel } from '../../../src/core/mux/contract.ts';
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
import { TalkBulkOpen } from '../../../src/core/talk/talk-bulk.ts';
import { installBulkRouting } from '../../../src/core/talk/talk-bulk-routes.ts';
import { TalkChannel } from '../../../src/core/talk/talk-channel.ts';
import { bulkOpen } from '../../support/talk/frames.ts';
import { flush } from '../../support/talk/helpers.ts';
import { memoryMuxPair } from '../../support/talk/memory-mux.ts';
import { bindingAndRaw, concat, nextChannel } from '../quic/harness.ts';

const empty = new Uint8Array(0);

/** How many of 40 unreferenced bulk channels, opened for an idle parent, are refused RESOURCE_EXHAUSTED: 24 under the bound of 16. */
const opened = 40;
const refusedUnderTheBound = opened - 16;

for (const ordered of [true, false]) {
  const run = ordered ? test : test.fails;
  run(
    `bulk channels beyond maxUnclaimedBulk for an idle parent are refused, ${
      ordered ? 'ordered channels (control)' : 'unordered channels'
    }`,
    async () => {
      const [client, server] = memoryMuxPair({ ordered });
      installBulkRouting(client);
      installBulkRouting(server);
      const raw = client.open(empty);
      const talk = new TalkChannel(await server.accept());
      void talk.serve(() => {});
      await flush();
      const bulks: MuxChannel[] = [];
      for (let n = 1; n <= opened; n++) {
        const bulk = raw.openAfter(bulkOpen(raw, n));
        bulk.priority = 'bulk';
        // Each holds what its window lets the peer send: 30 KB here.
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
      expect(refused).toHaveLength(refusedUnderTheBound);
    },
  );
}

test.fails('over the QUIC binding, bulk streams beyond maxUnclaimedBulk for an idle parent are refused', async () => {
  const [connection, peer] = await bindingAndRaw();
  installBulkRouting(connection);
  const arrived = nextChannel(connection);
  await peer.openRaw(encodeOpenRecord(2, empty));
  const talk = new TalkChannel(await arrived);
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
  expect(refused).toHaveLength(refusedUnderTheBound);
});
