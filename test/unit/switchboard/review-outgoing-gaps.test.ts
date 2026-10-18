/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Review finding (node and clients layer), a coverage gap: two cases of the
// Dart `outgoing_test.dart` are neither ported in `outgoing.test.ts` nor
// marked `Not ported` there: "the proxy forwards the client payload, never
// the hook or the default" and the group "credentialFor and the MOVED
// retry", "each owner gets its own key". Ported here one to one; they pass,
// so this records the gap, not a defect.

import { expect, test } from 'vitest';
import { ChannelAddress, ServiceAddress } from '../../../src/core/address/index.ts';
import { bytesEqual, utf8Decode } from '../../../src/core/bytes.ts';
import {
  MovedStatus,
  SlotEntry,
  SlotSpace,
  serviceRecord,
} from '../../../src/core/naming/naming-protocol.ts';
import { proxyHandler } from '../../../src/core/switchboard/proxy.ts';
import { StaticResolver } from '../../../src/core/switchboard/resolver.ts';
import { bytes, node, onCleanup, svc, tagged, tagOf } from '../../support/node/harness.ts';

const meshSecret = bytes('mesh-secret');

function contains(haystack: Uint8Array, needle: Uint8Array): boolean {
  for (let i = 0; i + needle.length <= haystack.length; i++) {
    if (bytesEqual(haystack.subarray(i, i + needle.length), needle)) {
      return true;
    }
  }
  return false;
}

/** The text of the first subframe [channel] receives. */
async function firstText(channel: { readable: ReadableStream<Uint8Array> }): Promise<string> {
  const reader = channel.readable.getReader();
  try {
    const { value } = await reader.read();
    return value === undefined ? '' : utf8Decode(value);
  } finally {
    reader.releaseLock();
  }
}

test('the proxy forwards the client payload, never the hook or the default', async () => {
  const backend = node();
  const raw: Uint8Array[] = [];
  backend.registerService(
    svc,
    (incoming) => {
      raw.push(incoming.channel.openPayload);
      tagged('A')(incoming);
    },
    { acceptAnyInstance: true },
  );
  const backendUri = await backend.listenMemory();
  const proxy = node({ defaultPayload: meshSecret, credentialFor: () => bytes('proxy-key') });
  const resolver = new StaticResolver([
    serviceRecord(new ServiceAddress(svc, 1), { endpoints: [backendUri] }),
  ]);
  onCleanup(() => resolver.close());
  proxy.resolver = resolver;
  proxy.catchAll = proxyHandler(proxy);
  const uri = await proxy.listenMemory();
  const client = node();
  await tagOf(
    await client.openChannelAt(uri, new ChannelAddress({ type: svc, payload: 'client-token' })),
  );
  await tagOf(await client.openChannelAt(uri, new ChannelAddress({ type: svc })));
  expect(raw.map((open) => utf8Decode(ChannelAddress.decode(open).payload))).toEqual([
    'client-token',
    '',
  ]);
  for (const open of raw) {
    expect(contains(open, meshSecret)).toBe(false);
    expect(contains(open, bytes('proxy-key'))).toBe(false);
  }
});

test('credentialFor and the MOVED retry: each owner gets its own key', async () => {
  const owner1 = node();
  const owner2 = node();
  const seen: string[] = [];
  let movedEpoch = 2;
  owner1.registerService(
    svc,
    (incoming) => {
      seen.push(`1 ${utf8Decode(incoming.address.payload)}`);
      void incoming.reject(new MovedStatus({ owner: 2, epoch: movedEpoch }).toStatus());
    },
    { instance: 1 },
  );
  owner2.registerService(
    svc,
    (incoming) => {
      seen.push(`2 ${utf8Decode(incoming.address.payload)}`);
      void incoming.channel.send(bytes('owner 2'));
    },
    { instance: 2 },
  );
  const uri1 = await owner1.listenMemory();
  const uri2 = await owner2.listenMemory();
  const resolver = new StaticResolver([
    serviceRecord(new ServiceAddress(svc, 1), { endpoints: [uri1] }),
    serviceRecord(new ServiceAddress(svc, 2), { endpoints: [uri2] }),
  ]);
  onCleanup(() => resolver.close());
  resolver.defineSlots(new SlotSpace(svc, { count: 4 }));
  resolver.setSlot(svc, 1, SlotEntry.owned(1, { epoch: 1 }));
  const client = node({
    resolver,
    defaultPayload: meshSecret,
    credentialFor: (_endpoint, record) => bytes(`key-${record?.address.instance}`),
  });
  const channel = await client.openChannelToSlot(svc, 1);
  expect(await firstText(channel)).toBe('owner 2');
  expect(channel.retried).toBe(true);
  expect(seen).toEqual(['1 key-1', '2 key-2']);
  await channel.close();
  // With a payload, both get it.
  seen.length = 0;
  resolver.setSlot(svc, 1, SlotEntry.owned(1, { epoch: 3 }));
  // The next hand-over must be newer than the table's return to 1.
  movedEpoch = 4;
  const given = await client.openChannelToSlot(svc, 1, { payload: 'given' });
  expect(await firstText(given)).toBe('owner 2');
  expect(seen).toEqual(['1 given', '2 given']);
  await given.close();
});
