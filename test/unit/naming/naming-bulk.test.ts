/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Port of the Dart `naming_bulk_test.dart`: naming payloads larger than a
// frame go as bulk payloads on their own; the naming service and client
// read `payload`, which a reassembled bulk payload fills like an inline one.
// As in Dart, over nodes on `mem` listeners: the naming service is served
// by a host node, and each client reaches it over a Talk channel of its own
// node (what `MeshNode.join` opens). Over the toy mux and, from
// `over-frame-mux/`, over the frame mux. A payload goes bulk by the
// channel's window, which only the frame mux has (the toy mux takes frames
// up to its frame limit, 1 MiB): over the toy mux the same payloads go
// inline, and `isBulk` is checked for that.

import { beforeEach, expect, test } from 'vitest';
import { ChannelAddress, ServiceAddress } from '../../../src/core/address/index.ts';
import { Name } from '../../../src/core/name.ts';
import { NamingClient } from '../../../src/core/naming/naming-client.ts';
import {
  decodeServiceRecord,
  maxMetadataLength,
  Procedures,
  RegisterRequest,
  Services,
  WatchRequest,
} from '../../../src/core/naming/naming-protocol.ts';
import { NamingService } from '../../../src/core/naming/naming-service.ts';
import type { ServiceEvent } from '../../../src/core/switchboard/contract.ts';
import type { TalkMessage } from '../../../src/core/talk/contract.ts';
import { within } from '../../support/naming/harness.ts';
import { node, onCleanup, overFrameMux } from '../../support/node/harness.ts';

const limit = 5000;

let service: NamingService;
let naming: string;

beforeEach(async () => {
  const host = node();
  service = new NamingService({ assignmentHold: 0 });
  onCleanup(() => service.close());
  host.registerService(Services.naming, service.handler, { instance: 1 });
  naming = await host.listenMemory();
});

/** The naming service's address on the host. */
function namingAddress(): ChannelAddress {
  return new ChannelAddress({ type: Services.naming, instance: 1 });
}

/** A new node joined to the naming service: its naming client (`MeshNode.join`). */
function join(): NamingClient {
  const sb = node();
  const client = new NamingClient(() => sb.openTalkAt(naming, namingAddress()), {
    reconnectDelay: 1000,
  });
  onCleanup(() => client.close());
  client.start().catch(() => {});
  return client;
}

async function collect(items: AsyncIterable<TalkMessage>): Promise<TalkMessage[]> {
  const out: TalkMessage[] = [];
  for await (const item of items) {
    out.push(item);
  }
  return out;
}

test('a REGISTER of 40 KiB goes bulk; the record reaches watchers and LOOKUP whole', async () => {
  const publisher = join();
  const watcher = join();
  await within(watcher.synced, limit);
  // Metadata is at most 4096 bytes; 160 endpoints of 240 bytes make the rest
  // of the 40 KiB.
  const endpoints = Array.from(
    { length: 160 },
    (_, i) => `tcp://host-${i}-${'x'.repeat(210)}.example:${9000 + i}`,
  );
  const metadata = Uint8Array.from({ length: maxMetadataLength }, (_, i) => i % 251);
  const type = Name.of('big');
  const request = new RegisterRequest(type, { endpoints, metadata }).encode();
  expect(request.length).toBeGreaterThan(40 * 1024);
  const up = new Promise<ServiceEvent>((resolve) => {
    const off = watcher.on('event', (e) => {
      if (e.up && e.record.address.type.equals(type)) {
        off();
        resolve(e);
      }
    });
  });
  const id = await within(publisher.register(type, endpoints, { metadata }), limit);
  const address = new ServiceAddress(type, id);
  const record = service.table.get(address.key);
  expect(record).toBeDefined();
  expect(record?.endpoints).toEqual(endpoints);
  expect(record?.metadata).toEqual(metadata);
  // The UP event to the watcher is as large: a bulk plain message.
  const seen = await within(up, limit);
  expect(seen.record.address.toString()).toBe(address.toString());
  expect(seen.record.endpoints).toEqual(endpoints);
  expect(seen.record.metadata).toEqual(metadata);
  // A LOOKUP item, a bulk stream item.
  const talk = await within(node().openTalkAt(naming, namingAddress()), limit);
  const lookup = talk.streamRequest(Procedures.lookup.toString(), new WatchRequest(type).encode());
  const items = await within(collect(lookup.items), limit);
  expect(items).toHaveLength(1);
  const item = items[0] as TalkMessage;
  const found = decodeServiceRecord(item.payload);
  // Bulk where the window sends it bulk (see the file comment).
  expect(item.isBulk).toBe(overFrameMux());
  expect(found.endpoints).toEqual(endpoints);
  expect(found.metadata).toEqual(metadata);
  await talk.close();
});

test('a HOLDING of 20000 slots goes bulk and round-trips', async () => {
  const mesh = join();
  const zone = Name.of('zone');
  await within(mesh.register(zone, ['tcp://127.0.0.1:1']), limit);
  await within(mesh.defineSlots(zone, { count: 20000 }), limit);
  const slots = Array.from({ length: 20000 }, (_, i) => i);
  // Before the answer, the client's mirror (it watches) gets a SLOT event
  // for each of the 20000 slots, whose holder changed: seconds under load,
  // so a longer bound than Dart's.
  const discard = await within(mesh.declareHolding(zone, slots), 4 * limit);
  expect(discard).toEqual([]);
  // Declared again: still held.
  expect(await within(mesh.declareHolding(zone, slots), 4 * limit)).toEqual([]);
}, 60_000);
