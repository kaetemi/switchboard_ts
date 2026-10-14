/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Port of the Dart `naming_bulk_test.dart`: naming payloads larger than a
// frame go as bulk payloads on their own; the naming service and client
// read `payload`, which a reassembled bulk payload fills like an inline one.
//
// The Dart test runs over mem:// nodes, whose Talk channels send these
// payloads bulk. The in-memory Talk pair the port runs over delivers every
// payload inline, so here the naming half is checked: the service and the
// client carry the same payloads, as large, whole. Whether they go bulk is
// the Talk layer's (`isBulk` is not checked).

import { afterEach, beforeEach, expect, test } from 'vitest';
import { ServiceAddress } from '../../../src/core/address/index.ts';
import { Name } from '../../../src/core/name.ts';
import { NamingClient } from '../../../src/core/naming/naming-client.ts';
import {
  decodeServiceRecord,
  maxMetadataLength,
  Procedures,
  RegisterRequest,
  WatchRequest,
} from '../../../src/core/naming/naming-protocol.ts';
import type { ServiceEvent } from '../../../src/core/switchboard/contract.ts';
import type { TalkMessage } from '../../../src/core/talk/contract.ts';
import { Harness, within } from '../../support/naming/harness.ts';

const limit = 5000;

let h: Harness;
let clients: NamingClient[];

beforeEach(() => {
  // NamingService(assignmentHold: Duration.zero) with the Dart defaults the
  // harness shortens.
  h = new Harness({
    assignmentHold: 0,
    handoverTimeout: 60_000,
    holdingSettle: Harness.productionSettle,
  });
  clients = [];
});

afterEach(async () => {
  await Promise.all(clients.map((client) => client.close()));
  await h.close();
});

/** A node joined to the naming service: its naming client (`MeshNode.join`). */
function join(): NamingClient {
  const client = new NamingClient(async () => h.link()[0], { reconnectDelay: 1000 });
  clients.push(client);
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
  const record = h.service.table.get(address.key);
  expect(record).toBeDefined();
  expect(record?.endpoints).toEqual(endpoints);
  expect(record?.metadata).toEqual(metadata);
  // The UP event to the watcher is as large: a bulk plain message.
  const seen = await within(up, limit);
  expect(seen.record.address.toString()).toBe(address.toString());
  expect(seen.record.endpoints).toEqual(endpoints);
  expect(seen.record.metadata).toEqual(metadata);
  // A LOOKUP item, a bulk stream item.
  const [talk] = h.link();
  const lookup = talk.streamRequest(Procedures.lookup.toString(), new WatchRequest(type).encode());
  const items = await within(collect(lookup.items), limit);
  expect(items).toHaveLength(1);
  const found = decodeServiceRecord((items[0] as TalkMessage).payload);
  // Not ported: `items.single.isBulk` (see the file comment).
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
  const discard = await within(mesh.declareHolding(zone, slots), limit);
  expect(discard).toEqual([]);
  // Declared again: still held.
  expect(await within(mesh.declareHolding(zone, slots), limit)).toEqual([]);
});
