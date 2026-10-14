/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// Port of the Dart `naming_test.dart`: the naming service and the naming
// client.

import { afterEach, beforeEach, describe, expect, onTestFinished, test } from 'vitest';
import { maxInstance, ServiceAddress } from '../../../src/core/address/index.ts';
import { ByteWriter, utf8Decode, utf8Encode } from '../../../src/core/bytes.ts';
import type { PeerIdentity } from '../../../src/core/identity/contract.ts';
import { Name } from '../../../src/core/name.ts';
import { NamingClient } from '../../../src/core/naming/naming-client.ts';
import {
  DialBackRequest,
  decodeServiceRecord,
  encodeServiceRecord,
  maxMetadataLength,
  Procedures,
  RegisterRequest,
  RegisterResponse,
  Services,
  serviceRecordsEqual,
  WatchRequest,
} from '../../../src/core/naming/naming-protocol.ts';
import { type NamingConnection, NamingService } from '../../../src/core/naming/naming-service.ts';
import { Status, StatusCode, SwitchboardError, statusCodeName } from '../../../src/core/status.ts';
import type { ServiceRecord } from '../../../src/core/switchboard/contract.ts';
import {
  defaultTalkOptions,
  type TalkChannel,
  type TalkMessage,
  talkOptions,
} from '../../../src/core/talk/contract.ts';
import { Deferred } from '../../../src/core/util/deferred.ts';
import type { FakeFrame } from '../../support/naming/fake-talk.ts';
import {
  bytes,
  Connector,
  clientOptions,
  delay,
  describeEvent,
  describe as describeMessage,
  fuzzInput,
  Harness,
  isFinalResponse,
  isRequestFor,
  type LinkChannel,
  LossyConnector,
  ms10,
  ms50,
  pump,
  reconnectDelay,
  register,
  rejectsWith,
  SeededRandom,
  serverOptions,
  talkPair,
  throwsWith,
  unregister,
  until,
  uriA,
  uriB,
  Watcher,
  within,
} from '../../support/naming/harness.ts';
import { TestClock } from '../../support/naming/test-clock.ts';

describe('NamingService', () => {
  let h: Harness;

  beforeEach(() => {
    h = new Harness();
  });

  afterEach(async () => {
    await h.close();
  });

  test('assigns increasing ids across types, starting at 1', async () => {
    const [c] = h.link();
    expect(await register(c, 'npc')).toBe(1);
    expect(await register(c, 'api')).toBe(2);
    expect(await register(c, 'npc')).toBe(3);
    expect(addresses(h.service.table)).toEqual(['npc/1', 'api/2', 'npc/3']);
  });

  test('honours a requested id and skips it when assigning', async () => {
    const [c] = h.link();
    expect(await register(c, 'npc', { instance: 3 })).toBe(3);
    expect(await register(c, 'npc')).toBe(1);
    expect(await register(c, 'npc')).toBe(2);
    expect(await register(c, 'npc')).toBe(4);
    expect(await register(c, 'big', { instance: maxInstance })).toBe(maxInstance);
  });

  test('wraps at the u48 limit and skips ids in use', async () => {
    const [c] = h.link();
    expect(await register(c, 'npc')).toBe(1);
    h.service.nextInstance = maxInstance;
    expect(await register(c, 'npc')).toBe(maxInstance);
    expect(await register(c, 'npc')).toBe(2);
  });

  test('a requested id in use is ALREADY_EXISTS', async () => {
    const [a] = h.link();
    const [b] = h.link();
    expect(await register(a, 'npc', { instance: 5 })).toBe(5);
    // Another channel, same type.
    await rejectsWith(register(b, 'npc', { instance: 5 }), StatusCode.alreadyExists);
    // Ids are unique across types, even on the owning channel.
    await rejectsWith(register(a, 'api', { instance: 5 }), StatusCode.alreadyExists);
    expect(h.service.table.size).toBe(1);
  });

  test('re-registering from the owner replaces the endpoints', async () => {
    const [a] = h.link();
    const events: string[] = [];
    const off = h.service.on('event', (e) => events.push(describeEvent(e)));
    expect(await register(a, 'npc', { endpoints: [uriA] })).toBe(1);
    expect(await register(a, 'npc', { instance: 1, endpoints: [uriB] })).toBe(1);
    // Unchanged: no event.
    expect(await register(a, 'npc', { instance: 1, endpoints: [uriB] })).toBe(1);
    await pump();
    expect(single(h.service.table).endpoints).toEqual([uriB]);
    expect(events).toEqual([`UP npc/1 ${uriA}`, `UP npc/1 ${uriB}`]);
    off();
  });

  test('re-registering with different metadata publishes UP', async () => {
    const [a] = h.link();
    const watcher = new Watcher(a);
    expect(await watcher.next).toBe('WATCHED');
    expect(await register(a, 'gpu', { endpoints: [uriA], metadata: text('m1') })).toBe(1);
    expect(await watcher.next).toBe(`UP gpu/1 ${uriA} meta m1`);
    // Same endpoints, other metadata: UP.
    await register(a, 'gpu', { instance: 1, endpoints: [uriA], metadata: text('m2') });
    expect(await watcher.next).toBe(`UP gpu/1 ${uriA} meta m2`);
    // Nothing changed: silent. Then metadata removed: UP.
    await register(a, 'gpu', { instance: 1, endpoints: [uriA], metadata: text('m2') });
    await register(a, 'gpu', { instance: 1, endpoints: [uriA] });
    expect(await watcher.next).toBe(`UP gpu/1 ${uriA}`);
    expect(single(h.service.table).metadata).toHaveLength(0);
    // Local registrations too.
    expect(
      h.service.registerLocal(Name.of('x'), [uriB], { instance: 9, metadata: text('l') }),
    ).toBe(9);
    expect(await watcher.next).toBe(`UP x/9 ${uriB} meta l`);
    h.service.registerLocal(Name.of('x'), [uriB], { instance: 9, metadata: text('l') });
    h.service.registerLocal(Name.of('x'), [uriB], { instance: 9 });
    expect(await watcher.next).toBe(`UP x/9 ${uriB}`);
  });

  test('metadata larger than 4096 bytes is INVALID_ARGUMENT', async () => {
    const [c] = h.link();
    const w = new ByteWriter()
      .name(Name.of('gpu'))
      .u48(0)
      .u8(0)
      .u16(maxMetadataLength + 1)
      .bytes(new Uint8Array(maxMetadataLength + 1));
    await rejectsWith(c.request('REGISTER', w.toBytes()), StatusCode.invalidArgument);
    throwsWith(
      () =>
        h.service.registerLocal(Name.of('gpu'), [], {
          metadata: new Uint8Array(maxMetadataLength + 1),
        }),
      StatusCode.invalidArgument,
    );
    expect(h.service.table.size).toBe(0);
    expect(await register(c, 'gpu', { metadata: new Uint8Array(maxMetadataLength) })).toBe(1);
    expect(single(h.service.table).metadata).toHaveLength(4096);
  });

  test('close completes while a listener of events is paused', async () => {
    const service = new NamingService({ assignmentHold: 0 });
    // Listened to, and never read from: paused.
    const events = service.events[Symbol.asyncIterator]();
    const slots = service.slotEvents[Symbol.asyncIterator]();
    service.registerLocal(Name.of('npc'), [uriA]);
    await within(service.close());
    await events.return?.();
    await slots.return?.();
  });

  test('follows the IDENTs of a connection with one listener, however many channels it carried', async () => {
    const connection = new IdentityEvents();
    for (let i = 0; i < 20; i++) {
      const [client, server] = talkPair({ a: clientOptions, b: serverOptions });
      h.service.serve(server, { connection });
      await register(client, 'npc');
      await client.close();
    }
    await until(() => h.service.channelCount === 0);
    expect(connection.waiting).toBe(1);
    // An IDENT once no channel is left: the service stops listening.
    connection.changeIdentity();
    await pump();
    expect(connection.waiting).toBe(0);
  });

  test('a REGISTER without the metadata field has empty metadata', async () => {
    const [c] = h.link();
    const watcher = new Watcher(c);
    expect(await watcher.next).toBe('WATCHED');
    const old = new ByteWriter().name(Name.of('npc')).u48(0).u8(1).string8(uriA);
    await c.request('REGISTER', old.toBytes());
    expect(await watcher.next).toBe(`UP npc/1 ${uriA}`);
    expect(single(h.service.table).metadata).toHaveLength(0);
  });

  test('a registrar registers records on behalf of others; they live as long as its channel', async () => {
    const [registrar] = h.link();
    const [consumer] = h.link();
    const watcher = new Watcher(consumer, { type: 'gpu' });
    expect(await watcher.next).toBe('WATCHED');
    // Three workers that never talk to the naming service, each at its
    // own endpoint with its own capabilities, under ids the registrar
    // chose.
    const workers: [number, string, string][] = [
      [0x101, 'tcp://10.1.0.1:7000', '{"models":["a"]}'],
      [0x102, 'tcp://10.1.0.2:7000', '{"models":["a","b"]}'],
      [0x103, 'ws://10.1.0.3:7000/ws', '{"models":["c"]}'],
    ];
    for (const [id, uri, meta] of workers) {
      expect(
        await register(registrar, 'gpu', {
          instance: id,
          endpoints: [uri],
          metadata: text(meta),
        }),
      ).toBe(id);
    }
    const ups = workers.map(([id, uri, meta]) => `UP gpu/${hex(id)} ${uri} meta ${meta}`);
    expect(await watcher.take(3)).toEqual(ups);
    // A late watcher gets the same records in its snapshot.
    const late = new Watcher(h.link()[0], { type: 'gpu' });
    expect(await late.take(4)).toEqual([...ups, 'WATCHED']);
    // Retiring one worker.
    await unregister(registrar, 'gpu', 0x102);
    expect(await watcher.next).toBe('DOWN gpu/102');
    // The registrar's channel goes: every record it made goes with it.
    await registrar.close();
    expect(await watcher.take(2)).toEqual(['DOWN gpu/101', 'DOWN gpu/103']);
    expect(h.service.table.size).toBe(0);
  });

  test('rejects invalid requests', async () => {
    const [c] = h.link();
    await rejectsWith(register(c, ''), StatusCode.invalidArgument);
    await rejectsWith(c.request('REGISTER', bytes([0x6e, 0x70])), StatusCode.invalidArgument);
    await rejectsWith(c.request('UNREGSTR', new Uint8Array(3)), StatusCode.invalidArgument);
    // A name with a byte after its terminating zero, a short name.
    for (const procedure of ['WATCH', 'UNWATCH']) {
      for (const payload of [bytes([0x61, 0, 0x62, 0, 0, 0, 0, 0]), bytes([0x61])]) {
        await rejectsWith(c.request(procedure, payload), StatusCode.invalidArgument);
      }
      // Not stream requests.
      await rejectsWith(
        c.streamRequest(procedure, new Uint8Array(0)).done,
        StatusCode.invalidArgument,
      );
    }
    // Nothing to give back.
    await rejectsWith(c.request('UNWATCH', new Uint8Array(0)), StatusCode.notFound);
    expect(h.service.watchCount).toBe(0);
    await rejectsWith(c.request('LOOKUP', Name.of('npc').bytes), StatusCode.invalidArgument);
    await rejectsWith(c.request('FROB', new Uint8Array(0)), StatusCode.unimplemented);
    // Decodes, but re-encodes longer than 255 bytes: refused, and the
    // table stays encodable.
    const long = `tcp://h/${'a b'.repeat(80)}`;
    const raw = Uint8Array.from([
      ...Name.of('npc').bytes,
      ...new Uint8Array(6),
      1,
      long.length,
      ...utf8Encode(long),
    ]);
    await rejectsWith(c.request('REGISTER', raw), StatusCode.invalidArgument);
    throwsWith(() => h.service.registerLocal(Name.of('npc'), [long]), StatusCode.invalidArgument);
    expect(h.service.table.size).toBe(0);
    // Plain messages are ignored, the channel stays usable.
    void c.send('REGISTER', new RegisterRequest(Name.of('npc')).encode());
    void c.send('FROB', new Uint8Array(0));
    expect(await register(c, 'npc')).toBe(1);
    expect(h.service.table.size).toBe(1);
  });

  test('unregister: NOT_FOUND, PERMISSION_DENIED, then DOWN', async () => {
    const [a] = h.link();
    const [b] = h.link();
    const watcher = new Watcher(b);
    expect(await watcher.next).toBe('WATCHED');
    const id = await register(a, 'npc');
    expect(await watcher.next).toBe('UP npc/1');
    await rejectsWith(unregister(a, 'npc', 9), StatusCode.notFound);
    // Right id, wrong type.
    await rejectsWith(unregister(a, 'api', id), StatusCode.notFound);
    await rejectsWith(unregister(b, 'npc', id), StatusCode.permissionDenied);
    await unregister(a, 'npc', id);
    expect(await watcher.next).toBe('DOWN npc/1');
    expect(h.service.table.size).toBe(0);
    await rejectsWith(unregister(a, 'npc', id), StatusCode.notFound);
  });

  test('watch: snapshot events, the reply, then live UP and DOWN', async () => {
    const [a] = h.link();
    const [b] = h.link();
    await register(a, 'npc', { endpoints: [uriA] });
    await register(a, 'api');
    const watcher = new Watcher(b);
    expect(await watcher.take(3)).toEqual([`UP npc/1 ${uriA}`, 'UP api/2', 'WATCHED']);
    const id = await register(a, 'npc', { endpoints: [uriB] });
    expect(await watcher.next).toBe(`UP npc/3 ${uriB}`);
    await register(a, 'npc', { instance: id, endpoints: [uriA] });
    expect(await watcher.next).toBe(`UP npc/3 ${uriA}`);
    await unregister(a, 'api', 2);
    expect(await watcher.next).toBe('DOWN api/2');
    expect(h.service.watchCount).toBe(1);
  });

  test('a watch on the registering channel has the UP before the reply', async () => {
    const [a] = h.link({ sync: true });
    const watcher = new Watcher(a);
    expect(await watcher.next).toBe('WATCHED');
    const id = await register(a, 'npc');
    // Sent before the reply, and the reply is read in order.
    expect(watcher.log).toEqual(['WATCHED', `UP npc/${hex(id)}`]);
  });

  test('the reply follows the whole snapshot, even when it arrives in one burst', async () => {
    const [a] = h.link();
    for (let i = 0; i < 20; i++) {
      await register(a, 'npc');
    }
    const watcher = new Watcher(h.link({ sync: true })[0]);
    const snapshot: string[] = [];
    for (let id = 1; id <= 20; id++) snapshot.push(`UP npc/${hex(id)}`);
    expect(await watcher.take(21)).toEqual([...snapshot, 'WATCHED']);
    await register(a, 'api');
    expect(await watcher.next).toBe('UP api/15');
  });

  test('watch filtered by type', async () => {
    const [a] = h.link();
    const [b] = h.link();
    await register(a, 'npc');
    await register(a, 'api');
    const watcher = new Watcher(b, { type: 'api' });
    expect(await watcher.take(2)).toEqual(['UP api/2', 'WATCHED']);
    await register(a, 'npc');
    await register(a, 'api');
    await unregister(a, 'npc', 1);
    await unregister(a, 'api', 2);
    expect(await watcher.take(2)).toEqual(['UP api/4', 'DOWN api/2']);
    // An all-zero name watches everything: a second subscription on the
    // same channel, with its own snapshot.
    await watcher.watch({ payload: new Uint8Array(8) });
    expect(await watcher.take(3)).toEqual(['UP npc/3', 'UP api/4', 'WATCHED']);
    expect(h.service.watchCount).toBe(2);
  });

  test('subscriptions of one channel: each change is sent once', async () => {
    const [a] = h.link();
    const [b] = h.link();
    const watcher = new Watcher(b, { type: 'npc' });
    await watcher.watch({ type: 'api' });
    await watcher.watch();
    expect(await watcher.take(3)).toEqual(['WATCHED', 'WATCHED', 'WATCHED']);
    expect(h.service.watchCount).toBe(3);
    await register(a, 'npc');
    await register(a, 'api');
    await register(a, 'gpu');
    await unregister(a, 'npc', 1);
    await watcher.unwatch();
    // Once each, though npc and api match two subscriptions.
    expect(await watcher.take(5)).toEqual([
      'UP npc/1',
      'UP api/2',
      'UP gpu/3',
      'DOWN npc/1',
      'UNWATCHED',
    ]);
    expect(h.service.watchCount).toBe(2);
    // Without the subscription to every type, gpu is not covered.
    await register(a, 'gpu');
    await register(a, 'api');
    await watcher.unwatch({ type: 'api' });
    await register(a, 'api');
    await register(a, 'npc');
    await watcher.unwatch({ type: 'npc' });
    expect(await watcher.take(4)).toEqual(['UP api/5', 'UNWATCHED', 'UP npc/7', 'UNWATCHED']);
    expect(h.service.watchCount).toBe(0);
    // Closing the channel ends every subscription it holds.
    await watcher.watch({ type: 'npc' });
    await watcher.watch({ type: 'api' });
    expect(h.service.watchCount).toBe(2);
    await b.close();
    await until(() => h.service.watchCount === 0);
    expect(h.service.channelCount).toBe(1);
  });

  test('a channel holds at most maxWatchesPerChannel subscriptions', async () => {
    const [a] = h.link();
    const max = NamingService.maxWatchesPerChannel;
    await Promise.all(
      Array.from({ length: max }, (_, i) =>
        a.request('WATCH', new WatchRequest(Name.of(`t${i}`)).encode()),
      ),
    );
    expect(h.service.watchCount).toBe(max);
    await rejectsWith(a.request('WATCH', new Uint8Array(0)), StatusCode.resourceExhausted);
    // One it holds can be taken again; once given back, another fits.
    await a.request('WATCH', new WatchRequest(Name.of('t0')).encode());
    await a.request('UNWATCH', new WatchRequest(Name.of('t1')).encode());
    await a.request('WATCH', new Uint8Array(0));
    expect(h.service.watchCount).toBe(max);
  });

  test('LOOKUP streams the records of a type, then ends', async () => {
    const [a] = h.link();
    await register(a, 'npc', { endpoints: [uriA, uriB] });
    await register(a, 'api');
    await register(a, 'npc');
    const lookup = a.streamRequest('LOOKUP', Name.of('npc').bytes);
    const items = (await collect(lookup.items)).map(describeMessage);
    expect(items).toEqual([`UP npc/1 ${uriA},${uriB}`, 'UP npc/3']);
    expect((await lookup.done).payload).toHaveLength(0);
    const none = a.streamRequest('LOOKUP', Name.of('none').bytes);
    expect(await collect(none.items)).toEqual([]);
    await rejectsWith(
      a.streamRequest('LOOKUP', new Uint8Array(0)).done,
      StatusCode.invalidArgument,
    );
    // An all-zero name is "every type" for WATCH, but LOOKUP needs one.
    await rejectsWith(
      a.streamRequest('LOOKUP', new Uint8Array(8)).done,
      StatusCode.invalidArgument,
    );
    expect(h.service.watchCount).toBe(0);
  });

  test('WATCH and UNWATCH of one filter are counted', async () => {
    const [a] = h.link();
    const [b, server] = h.link();
    const watcher = new Watcher(b);
    expect(await watcher.next).toBe('WATCHED');
    expect(h.service.watchCount).toBe(1);
    await register(a, 'npc');
    expect(await watcher.next).toBe('UP npc/1');
    // The same filter again: a fresh snapshot, one subscription held
    // twice.
    await watcher.watch();
    expect(await watcher.take(2)).toEqual(['UP npc/1', 'WATCHED']);
    expect(h.service.watchCount).toBe(1);
    // Given back once: still subscribed.
    await watcher.unwatch();
    expect(await watcher.next).toBe('UNWATCHED');
    await register(a, 'npc');
    expect(await watcher.next).toBe('UP npc/2');
    expect(h.service.watchCount).toBe(1);
    // Given back again: nothing after the reply.
    await watcher.unwatch();
    expect(await watcher.next).toBe('UNWATCHED');
    expect(h.service.watchCount).toBe(0);
    await register(a, 'npc');
    await unregister(a, 'npc', 1);
    // A third time: nothing to give back.
    await rejectsWith(watcher.unwatch(), StatusCode.notFound);
    expect(await watcher.next).toBe('UNWATCH notFound');
    // Nothing arrived meanwhile: next is the snapshot of a new WATCH.
    await watcher.watch();
    expect(await watcher.take(3)).toEqual(['UP npc/2', 'UP npc/3', 'WATCHED']);
    // Nothing is left outstanding on either side.
    expect(b.outgoingRequestCount).toBe(0);
    expect(server.incomingRequestCount).toBe(0);
  });

  test('channel close removes its records; watchers see DOWN', async () => {
    const [a] = h.link();
    const [b] = h.link();
    const [c] = h.link();
    await register(a, 'npc');
    await register(b, 'api');
    await register(a, 'npc');
    const watcherA = new Watcher(a);
    const watcherC = new Watcher(c);
    expect(await watcherC.take(4)).toEqual(['UP npc/1', 'UP api/2', 'UP npc/3', 'WATCHED']);
    expect(await watcherA.take(4)).toHaveLength(4);
    expect(h.service.watchCount).toBe(2);
    await a.close();
    expect(await watcherC.take(2)).toEqual(['DOWN npc/1', 'DOWN npc/3']);
    expect(addresses(h.service.table)).toEqual(['api/2']);
    await until(() => h.service.watchCount === 1);
    expect(h.service.channelCount).toBe(2);
    // The server side closing has the same effect.
    await nth(h.servers, 1).close();
    expect(await watcherC.next).toBe('DOWN api/2');
    expect(h.service.table.size).toBe(0);
  });

  test('local registrations', async () => {
    const [a] = h.link();
    const watcher = new Watcher(a);
    expect(await watcher.next).toBe('WATCHED');
    expect(h.service.registerLocal(Services.naming, [uriA], { instance: 1 })).toBe(1);
    expect(await watcher.next).toBe(`UP _ns/1 ${uriA}`);
    expect(await register(a, 'npc')).toBe(2);
    expect(await watcher.next).toBe('UP npc/2');
    await rejectsWith(register(a, '_ns', { instance: 1 }), StatusCode.alreadyExists);
    await rejectsWith(unregister(a, '_ns', 1), StatusCode.permissionDenied);
    throwsWith(
      () => h.service.unregisterLocal(new ServiceAddress(Name.of('npc'), 2)),
      StatusCode.permissionDenied,
    );
    throwsWith(
      () => h.service.registerLocal(Name.of('x'), [], { instance: 2 }),
      StatusCode.alreadyExists,
    );
    h.service.unregisterLocal(new ServiceAddress(Services.naming, 1));
    expect(await watcher.next).toBe('DOWN _ns/1');
  });

  test('refuses endpoints a watcher could not decode', async () => {
    const [c] = h.link();
    const watcher = new Watcher(c);
    expect(await watcher.next).toBe('WATCHED');
    for (const endpoint of [
      './>:x', // no scheme
      '//10.0.0.5:9101/ws', // no scheme
      String.raw`tcp:/.\\{:`, // backslashes and a brace
      'tcp:/../..//', // dot segments
    ]) {
      const w = new ByteWriter().name(Name.of('npc')).u48(0).u8(1).string8(endpoint);
      await rejectsWith(c.request('REGISTER', w.toBytes()), StatusCode.invalidArgument);
      throwsWith(
        () => h.service.registerLocal(Name.of('npc'), [endpoint]),
        StatusCode.invalidArgument,
      );
    }
    expect(h.service.table.size).toBe(0);
    // The watch saw nothing, and still works.
    expect(await register(c, 'npc', { endpoints: [uriA] })).toBe(1);
    expect(await watcher.next).toBe(`UP npc/1 ${uriA}`);
  });

  test('fuzz: every record the service accepts, a watcher decodes', async () => {
    const client = new NamingClient(new Connector(h).call, { reconnectDelay });
    await client.start();
    await within(client.synced);
    const [raw] = h.link();
    const random = new SeededRandom(1);
    let accepted = 0;
    for (let batch = 0; batch < 4; batch++) {
      const results = await Promise.all(
        Array.from({ length: 500 }, (_, i) =>
          raw.request('REGISTER', fuzzInput(random, i | random.nextInt(2))).then(
            () => true,
            (e: unknown) => {
              expect(e).toBeInstanceOf(SwitchboardError);
              expect([
                statusCodeName(StatusCode.invalidArgument),
                statusCodeName(StatusCode.alreadyExists),
              ]).toContain(statusCodeName((e as SwitchboardError).code));
              return false;
            },
          ),
        ),
      );
      accepted += results.filter((ok) => ok).length;
    }
    expect(accepted).toBeGreaterThan(50);
    expect(h.service.table.size).toBe(accepted);
    // Every UP decoded on the watching side: the mirror is complete.
    await until(() => client.table.size === accepted);
    expect(client.table).toEqual(h.service.table);
    for (const record of h.service.table.values()) {
      expect(serviceRecordsEqual(decodeServiceRecord(encodeServiceRecord(record)), record)).toBe(
        true,
      );
    }
    await client.close();
  });

  test('serve() of a channel already listened to keeps nothing', async () => {
    const [channel] = talkPair({ a: serverOptions, b: clientOptions });
    const taken = channel.messages[Symbol.asyncIterator]();
    expect(() => h.service.serve(channel)).toThrow(Error);
    expect(h.service.channelCount).toBe(0);
    await taken.return?.();
    await channel.close();
    await pump();
    expect(h.service.channelCount).toBe(0);
  });

  test('REGISTER racing a WATCH snapshot: exactly one UP', async () => {
    // The snapshot, the reply and the subscription happen in one step, so a
    // registration lands either in the snapshot or after the reply.
    for (let round = 0; round < 50; round++) {
      const h = new Harness();
      const [a] = h.link();
      const [b] = h.link({ sync: round % 2 === 0 });
      const ups: string[] = [];
      let replied = false;
      void (async () => {
        try {
          for await (const m of b.messages) {
            if (m.procedureName === 'UP') {
              ups.push(replied ? 'live' : 'snapshot');
            }
          }
        } catch {
          // The channel ended.
        }
      })();
      const registered = register(a, 'npc');
      const watched = b.request('WATCH', new Uint8Array(0), { ordered: true }).then(() => {
        replied = true;
      });
      if (round % 4 >= 2) {
        await pump();
      }
      await registered;
      await watched;
      await until(() => ups.length > 0);
      await pump();
      expect(ups, `round ${round}`).toHaveLength(1);
      await h.close();
    }
  });

  test('close ends subscriptions and closes channels with GOING_AWAY', async () => {
    const [a] = h.link();
    const [b] = h.link();
    await register(a, 'npc');
    const watcher = new Watcher(b);
    expect(await watcher.take(2)).toEqual(['UP npc/1', 'WATCHED']);
    const eventsDone = collect(h.service.events);
    await h.service.close();
    await a.done;
    await b.done;
    expect(a.isOpen).toBe(false);
    expect((await nth(h.servers, 0).done).known).toBe(StatusCode.goingAway);
    expect(h.service.watchCount).toBe(0);
    expect(h.service.table.size).toBe(0);
    expect((await eventsDone).map(describeEvent)).toEqual(['DOWN npc/1']);
    // The subscriptions ended first: no DOWN went to the channels.
    expect(watcher.log).toEqual(['UP npc/1', 'WATCHED']);
    // Channels served after close are closed at once.
    const [c, cServer] = h.link();
    await c.done;
    expect(c.isOpen).toBe(false);
    expect((await cServer.done).known).toBe(StatusCode.goingAway);
  });
});

describe('idle subscriptions', () => {
  test('an idle subscription costs no traffic and lives on', async () => {
    const clock = new TestClock();
    // Default options everywhere.
    const ns = new NamingService({ clock });
    const [server, peer] = talkPair({ a: { clock }, b: { clock } });
    ns.serve(server);
    peer.write({
      kind: 'message',
      procedure: Name.of('WATCH'),
      payload: new Uint8Array(0),
      requestId: 1,
      responseId: 0,
      expectsStream: false,
      item: false,
    });
    const frames = server.sent;
    await clock.elapse(1000);
    // The reply to an empty snapshot, and nothing else: no EXTEND.
    expect(frames).toHaveLength(1);
    const reply = messageFrame(frames[0]);
    expect(reply.item).toBe(false);
    expect(reply.responseId).toBe(1);
    expect(reply.payload).toHaveLength(0);
    await clock.elapse(5 * 60_000);
    expect(frames).toHaveLength(1);
    expect(ns.watchCount).toBe(1);
    // Still live: a change arrives as a one-way message.
    ns.registerLocal(Name.of('npc'), []);
    await pump();
    expect(frames).toHaveLength(2);
    const up = messageFrame(frames[1]);
    expect(up.procedure.equals(Procedures.up)).toBe(true);
    expect(up.requestId).toBe(0);
    expect(up.responseId).toBe(0);
    void ns.close();
    await pump();
    expect(clock.pendingCount).toBe(0);
  });

  test('outlives every request and reply timeout', async () => {
    const clock = new TestClock();
    const h = new Harness({ clock });
    const [a] = h.link();
    const lines: string[] = [];
    void (async () => {
      try {
        for await (const m of a.messages) lines.push(describeMessage(m));
      } catch {
        // The channel ended.
      }
    })();
    a.request('WATCH', new Uint8Array(0), { ordered: true }).then(
      () => lines.push('WATCHED'),
      () => {},
    );
    // 10 times the watcher's request timeout, 15 times the service's reply
    // timeout.
    await clock.elapse(clientRequestTimeout * 10);
    expect(lines).toEqual(['WATCHED']);
    expect(h.service.watchCount).toBe(1);
    const [b] = h.link();
    void register(b, 'npc');
    await clock.elapse(ms10);
    expect(lines).toEqual(['WATCHED', 'UP npc/1']);
    await clock.elapse(clientRequestTimeout * 10);
    expect(h.service.watchCount).toBe(1);
    expect(a.isOpen).toBe(true);
    void h.close();
    await pump();
    expect(clock.pendingCount).toBe(0);
  });
});

describe('assignment hold', () => {
  const hold = 2000;

  test('holds REGISTER for any id, not for a given id', async () => {
    const clock = new TestClock();
    const h = new Harness({ assignmentHold: hold, clock });
    expect(h.service.isHoldingAssignments).toBe(true);
    const [a] = h.link();
    const [b] = h.link();
    let any: number | undefined;
    let error: unknown;
    register(a, 'npc').then(
      (id) => {
        any = id;
      },
      (e: unknown) => {
        error = e;
      },
    );
    let given: number | undefined;
    void register(b, 'api', { instance: 1 }).then((id) => {
      given = id;
    });
    // Invalid requests are refused at once.
    let invalid: unknown;
    register(a, '').then(
      () => {},
      (e: unknown) => {
        invalid = e;
      },
    );
    await clock.elapse(ms10);
    expect(given).toBe(1);
    expectStatus(invalid, StatusCode.invalidArgument);
    expect(any).toBeUndefined();
    // Kept alive well past the request and reply timeouts.
    await clock.elapse(hold - ms50);
    expect(any).toBeUndefined();
    expect(error).toBeUndefined();
    expect(h.service.isHoldingAssignments).toBe(true);
    await clock.elapse(ms50 * 2);
    expect(h.service.isHoldingAssignments).toBe(false);
    expect(any).toBe(2);
    // Over: assigned at once.
    let later: number | undefined;
    void register(b, 'npc').then((id) => {
      later = id;
    });
    await clock.elapse(ms10);
    expect(later).toBe(3);
    void h.close();
    await pump();
    expect(clock.pendingCount).toBe(0);
  });

  test('a held REGISTER gets one EXTEND, declaring the time left in the hold', async () => {
    const clock = new TestClock();
    const h = new Harness({ assignmentHold: hold, clock });
    await clock.elapse(500);
    const [a] = h.link();
    const declared: [number | undefined, number | undefined][] = [];
    let id: number | undefined;
    void a
      .startRequest('REGISTER', new RegisterRequest(Name.of('npc')).encode(), {
        onExtend: (extension) => declared.push([extension.deadline, extension.renew]),
      })
      .response.then((m) => {
        id = RegisterResponse.decode(m.payload).instance;
      });
    await clock.elapse(ms10);
    // 1.5 s were left at receipt; the wire carries the service node's
    // buffer on top.
    expect(declared).toEqual([[1500 + talkOptions(serverOptions).extendBuffer, undefined]]);
    // Well past the request and reply timeouts: nothing more is sent.
    await clock.elapse(1480);
    expect(id).toBeUndefined();
    expect(declared).toHaveLength(1);
    await clock.elapse(ms10 * 2);
    expect(id).toBe(1);
    expect(declared).toHaveLength(1);
    void h.close();
    await pump();
    expect(clock.pendingCount).toBe(0);
  });

  test('a service surviving a restart keeps its id', async () => {
    const clock = new TestClock();
    let h = new Harness({ clock });
    const connector = new Connector(h);
    const survivor = new NamingClient(connector.call, { reconnectDelay: 300, clock });
    const assigned: number[] = [];
    void survivor.start();
    void survivor.register(Name.of('npc'), [uriA], { onAssigned: (id) => assigned.push(id) });
    await clock.elapse(ms10);
    expect(assigned).toEqual([1]);

    // The naming service restarts; a newcomer connects before the
    // survivor's reconnect delay is over.
    const old = h;
    h = new Harness({ assignmentHold: hold, clock });
    connector.harness = h;
    void old.close();
    const newcomer = new NamingClient(new Connector(h).call, { reconnectDelay, clock });
    void newcomer.start();
    let fresh: number | undefined;
    void newcomer.register(Name.of('api'), [uriB]).then((id) => {
      fresh = id;
    });
    await clock.elapse(ms10);
    // The newcomer is synced, its registration held.
    expect(newcomer.isSynced).toBe(true);
    expect(fresh).toBeUndefined();
    await clock.elapse(500);
    expect(survivor.isSynced).toBe(true);
    expect(addresses(h.service.table)).toEqual(['npc/1']);
    await clock.elapse(hold);
    expect(fresh).toBe(2);
    expect(assigned).toEqual([1]);
    expect(addresses(h.service.table)).toEqual(['npc/1', 'api/2']);

    void survivor.close();
    void newcomer.close();
    void h.close();
    await pump();
    expect(clock.pendingCount).toBe(0);
  });

  test('dropped and closed: held requests go, no timers stay', async () => {
    const clock = new TestClock();
    const h = new Harness({ assignmentHold: hold, clock });
    const [a] = h.link();
    const [b] = h.link();
    let aError: unknown;
    let bError: unknown;
    register(a, 'npc').then(
      () => {},
      (e: unknown) => {
        aError = e;
      },
    );
    register(b, 'npc').then(
      () => {},
      (e: unknown) => {
        bError = e;
      },
    );
    await clock.elapse(ms10);
    void a.close();
    await clock.elapse(ms10);
    expectStatus(aError, StatusCode.cancelled);
    void h.service.close();
    await pump();
    expectStatus(bError, StatusCode.goingAway);
    expect(h.service.table.size).toBe(0);
    void h.close();
    await pump();
    expect(clock.pendingCount).toBe(0);
  });

  test('a held REGISTER the requester cancels is never registered', async () => {
    const clock = new TestClock();
    const h = new Harness({ assignmentHold: hold, clock });
    const [a, server] = h.link();
    let error: unknown;
    a.request('REGISTER', new RegisterRequest(Name.of('npc')).encode(), { timeout: 0 }).then(
      () => {},
      (e: unknown) => {
        error = e;
      },
    );
    await clock.elapse(ms10);
    // A stream request is the way to cancel a request explicitly.
    const stream = a.streamRequest('REGISTER', new RegisterRequest(Name.of('api')).encode());
    let streamError: unknown;
    stream.done.then(
      () => {},
      (e: unknown) => {
        streamError = e;
      },
    );
    await clock.elapse(ms10);
    stream.cancel();
    await clock.elapse(ms10);
    expectStatus(streamError, StatusCode.cancelled);
    expect(server.incomingRequestCount).toBe(1);
    await clock.elapse(hold);
    expect(error).toBeUndefined();
    expect(addresses(h.service.table)).toEqual(['npc/1']);
    expect(server.incomingRequestCount).toBe(0);
    void h.close();
    await pump();
    expect(clock.pendingCount).toBe(0);
  });

  test('rejects a negative hold', () => {
    expect(() => new NamingService({ assignmentHold: -1000 })).toThrow(RangeError);
  });
});

describe('NamingClient', () => {
  let h: Harness;
  let connector: Connector;
  const clients: NamingClient[] = [];

  function newClient(connector: Connector): NamingClient {
    const client = new NamingClient(connector.call, { reconnectDelay });
    clients.push(client);
    return client;
  }

  beforeEach(() => {
    h = new Harness();
    connector = new Connector(h);
  });

  afterEach(async () => {
    for (const client of clients) {
      await client.close();
    }
    clients.length = 0;
    await h.close();
  });

  test('registers, syncs and mirrors the table', async () => {
    const [raw] = h.link();
    await register(raw, 'api', { endpoints: [uriB] });
    const client = newClient(connector);
    const events: string[] = [];
    client.on('event', (e) => events.push(describeEvent(e)));
    const registered = client.register(Name.of('npc'), [uriA]);
    expect(client.isConnected).toBe(false);
    await client.start();
    expect(client.isConnected).toBe(true);
    expect(await within(registered)).toBe(2);
    await within(client.synced);
    expect(client.isSynced).toBe(true);
    expect(addresses(client.table)).toEqual(['api/1', 'npc/2']);
    expect(client.record(address('npc', 2))?.endpoints).toEqual([uriA]);
    await register(raw, 'api');
    await until(() => client.table.size === 3);
    await unregister(raw, 'api', 1);
    await until(() => client.table.size === 2);
    // Snapshot in table order.
    expect(events).toEqual([`UP api/1 ${uriB}`, `UP npc/2 ${uriA}`, 'UP api/3', 'DOWN api/1']);
  });

  test('synced means complete, even over a link that delivers in bursts', async () => {
    const [raw] = h.link();
    for (let i = 0; i < 20; i++) {
      await register(raw, 'npc');
    }
    const client = newClient(new Connector(h, { sync: true }));
    let atSync: number | undefined;
    client.synced.then(
      () => {
        atSync = client.table.size;
      },
      () => {},
    );
    await client.start();
    await within(client.synced);
    expect(atSync).toBe(20);
    // The events after the reply are live.
    const id = await register(raw, 'api');
    await until(() => client.table.size === 21);
    expect([...client.table.values()].at(-1)?.address.toString()).toBe(`api/${hex(id)}`);
    // A registration through the client is in its table once done.
    const own = await client.register(Name.of('own'), []);
    expect(client.table.has(address('own', own).key)).toBe(true);
  });

  test('register after sync is mirrored when it completes', async () => {
    const client = newClient(connector);
    await client.start();
    await within(client.synced);
    const id = await client.register(Name.of('npc'), [uriA]);
    // The UP item precedes the response on the same channel.
    expect(client.record(address('npc', id))?.endpoints).toEqual([uriA]);
    expect(await client.register(Name.of('npc'), [], { instance: 7 })).toBe(7);
    await rejectsWith(
      client.register(Name.of('api'), [], { instance: 7 }),
      StatusCode.alreadyExists,
    );
    await rejectsWith(client.register(Name.of(''), []), StatusCode.invalidArgument);
    expect(h.service.table.size).toBe(2);
  });

  test('unregister', async () => {
    const client = newClient(connector);
    await client.start();
    const id = await client.register(Name.of('npc'), [uriA]);
    await within(client.synced);
    await rejectsWith(client.unregister(Name.of('npc'), id + 1), StatusCode.notFound);
    await client.unregister(Name.of('npc'), id);
    expect(h.service.table.size).toBe(0);
    await until(() => client.table.size === 0);
    // Not re-registered after a reconnect.
    await last(connector.servers).close();
    await until(() => connector.calls === 2 && client.isSynced);
    expect(h.service.table.size).toBe(0);
  });

  test('two clients see each other', async () => {
    const a = newClient(connector);
    const b = newClient(connector);
    const idA = await a.start().then(() => a.register(Name.of('npc'), [uriA]));
    const idB = await b.start().then(() => b.register(Name.of('api'), [uriB]));
    await until(() => a.table.size === 2 && b.table.size === 2);
    expect([...a.table.keys()].sort()).toEqual([...b.table.keys()].sort());
    expect(a.record(address('api', idB))?.endpoints).toEqual([uriB]);
    expect(b.record(address('npc', idA))?.endpoints).toEqual([uriA]);
    const downs: string[] = [];
    b.on('event', (e) => {
      if (!e.up) downs.push(describeEvent(e));
    });
    await a.close();
    await until(() => downs.length > 0);
    expect(downs).toEqual([`DOWN npc/${hex(idA)}`]);
    expect(addresses(b.table)).toEqual([`api/${hex(idB)}`]);
  });

  test('reconnects, re-registers the same ids, resyncs with DOWN', async () => {
    const client = newClient(connector);
    const other = newClient(new Connector(h));
    await client.start();
    const id = await client.register(Name.of('npc'), [uriA]);
    await other.start();
    const otherId = await other.register(Name.of('api'), [uriB]);
    await until(() => client.table.size === 2);
    const firstSynced = client.synced;

    const events: string[] = [];
    client.on('event', (e) => events.push(describeEvent(e)));
    connector.down = true;
    await last(connector.servers).close();
    await until(() => !client.isConnected);
    expect(client.isSynced).toBe(false);
    // The stale table is kept.
    expect(client.table.size).toBe(2);
    expect(client.synced).not.toBe(firstSynced);
    // Meanwhile the service lost our record and the other one went away.
    expect(addresses(h.service.table)).toEqual([`api/${hex(otherId)}`]);
    await other.close();
    expect(h.service.table.size).toBe(0);
    await until(() => connector.calls >= 3);

    connector.down = false;
    await within(client.synced);
    expect(client.isConnected).toBe(true);
    expect(addresses(h.service.table)).toEqual([`npc/${hex(id)}`]);
    expect(addresses(client.table)).toEqual([`npc/${hex(id)}`]);
    expect(events).toEqual([`DOWN api/${hex(otherId)}`]);
  });

  test('a taken id is replaced by a new one on reconnect', async () => {
    const client = newClient(connector);
    await client.start();
    const id = await client.register(Name.of('npc'), [uriA]);
    await within(client.synced);
    connector.down = true;
    await last(connector.servers).close();
    await until(() => h.service.table.size === 0);
    const [thief] = h.link();
    expect(await register(thief, 'api', { instance: id })).toBe(id);
    connector.down = false;
    await until(() => client.isSynced && client.table.size === 2);
    const mine = [...h.service.table.values()].filter((r) => r.address.type.equals(Name.of('npc')));
    expect(mine).toHaveLength(1);
    const instance = nth(mine, 0).address.instance;
    expect(instance).not.toBe(id);
    // The new id is the one remembered from now on.
    await client.unregister(Name.of('npc'), instance);
    expect(addresses(h.service.table)).toEqual([`api/${hex(id)}`]);
  });

  test('register while disconnected completes after reconnect', async () => {
    const client = newClient(connector);
    connector.down = true;
    void client.start();
    await until(() => connector.calls >= 2);
    const registered = client.register(Name.of('npc'), [uriA], { instance: 4 });
    let done = false;
    void registered.then(() => {
      done = true;
    });
    await delay(reconnectDelay * 3);
    expect(done).toBe(false);
    expect(client.isConnected).toBe(false);
    connector.down = false;
    expect(await within(registered)).toBe(4);
    expect(addresses(h.service.table)).toEqual(['npc/4']);
    await within(client.synced);
  });

  test('service close: GOING_AWAY, reconnect to a new service', async () => {
    const client = newClient(connector);
    await client.start();
    const id = await client.register(Name.of('npc'), [uriA]);
    await within(client.synced);
    const calls = connector.calls;
    await h.service.close();
    await until(() => !client.isConnected);
    // Keeps trying against the closed service.
    await until(() => connector.calls >= calls + 2);
    expect(client.isSynced).toBe(false);
    const old = h;
    h = new Harness();
    connector.harness = h;
    await old.close();
    await within(client.synced);
    expect(addresses(h.service.table)).toEqual([`npc/${hex(id)}`]);
    expect(addresses(client.table)).toEqual([`npc/${hex(id)}`]);
  });

  test('a WATCH without a reply is a loss of the service', async () => {
    // The service never answers.
    const silent: LinkChannel[] = [];
    let calls = 0;
    const client = new NamingClient(
      async () => {
        calls++;
        const [client, server] = talkPair({
          a: { requestTimeout: ms50 },
          b: { replyTimeout: 0 },
          dropBtoA: () => true,
        });
        silent.push(server);
        return client;
      },
      { reconnectDelay },
    );
    clients.push(client);
    await client.start();
    await until(() => calls >= 3);
    expect(client.isSynced).toBe(false);
    await client.close();
    for (const server of silent) {
      await server.close();
    }
  });

  test('connect errors are retried', async () => {
    const client = newClient(connector);
    connector.down = true;
    let started = false;
    void client.start().then(() => {
      started = true;
    });
    await until(() => connector.calls >= 3);
    expect(started).toBe(false);
    connector.down = false;
    await within(client.synced);
    expect(started).toBe(true);
  });

  test('close fails pending futures and stops', async () => {
    const client = newClient(connector);
    connector.down = true;
    const started = client.start();
    const registered = client.register(Name.of('npc'), []);
    const synced = client.synced;
    await until(() => connector.calls >= 2);
    await client.close();
    await started;
    await rejectsWith(registered, StatusCode.cancelled);
    await rejectsWith(synced, StatusCode.cancelled);
    const calls = connector.calls;
    await delay(reconnectDelay * 3);
    expect(connector.calls).toBe(calls);
    await rejectsWith(client.register(Name.of('npc'), []), StatusCode.failedPrecondition);
    expect(await collect(client.events)).toEqual([]);
  });

  test('hasSynced and firstSynced outlive a loss', async () => {
    const client = newClient(connector);
    expect(client.hasSynced).toBe(false);
    let first = false;
    void client.firstSynced.then(() => {
      first = true;
    });
    await client.start();
    await within(client.synced);
    await pump();
    expect(first).toBe(true);
    expect(client.hasSynced).toBe(true);
    const firstSynced = client.firstSynced;
    connector.down = true;
    await last(connector.servers).close();
    await until(() => !client.isConnected);
    expect(client.isSynced).toBe(false);
    expect(client.hasSynced).toBe(true);
    expect(client.firstSynced).toBe(firstSynced);
    await client.firstSynced;
    let resynced = false;
    // Fails with CANCELLED when the client is closed after the test.
    client.synced.then(
      () => {
        resynced = true;
      },
      () => {},
    );
    await pump();
    expect(resynced).toBe(false);
  });

  test('firstSynced fails with CANCELLED if closed before any sync', async () => {
    connector.down = true;
    const client = newClient(connector);
    void client.start();
    const firstSynced = client.firstSynced;
    await until(() => connector.calls >= 2);
    await client.close();
    await rejectsWith(firstSynced, StatusCode.cancelled);
    expect(client.hasSynced).toBe(false);
  });

  test('metadata is published, mirrored and re-registered', async () => {
    const client = newClient(connector);
    const watcher = newClient(new Connector(h));
    await watcher.start();
    await client.start();
    const meta = text('{"models":["a"]}');
    const id = await client.register(Name.of('gpu'), [uriA], { metadata: meta });
    // The caller's buffer is copied.
    meta.fill(0);
    const gpu = address('gpu', id);
    await until(() => watcher.record(gpu) !== undefined);
    expect(utf8Decode(recordOf(watcher.table, gpu).metadata)).toBe('{"models":["a"]}');
    // A new registration of the same id replaces the metadata.
    const events: string[] = [];
    watcher.on('event', (e) => events.push(describeEvent(e)));
    await client.register(Name.of('gpu'), [uriA], {
      instance: id,
      metadata: text('{"models":["b"]}'),
    });
    await until(() => events.length > 0);
    expect(events).toEqual([`UP gpu/${hex(id)} ${uriA} meta {"models":["b"]}`]);
    // After a reconnect the remembered registration carries it again.
    await last(connector.servers).close();
    await until(() => connector.calls === 2 && client.isSynced);
    await until(() => h.service.table.size > 0);
    expect(utf8Decode(recordOf(h.service.table, gpu).metadata)).toBe('{"models":["b"]}');
    await expect(
      client.register(Name.of('gpu'), [uriA], { metadata: new Uint8Array(maxMetadataLength + 1) }),
    ).rejects.toBeInstanceOf(RangeError);
  });

  test('register refuses endpoints a watcher could not decode', async () => {
    const client = newClient(connector);
    for (const endpoint of ['./>:x', '//10.0.0.5:9101/ws', String.raw`tcp:/.\\{:`]) {
      await expect(
        client.register(Name.of('npc'), [uriA, endpoint]),
        endpoint,
      ).rejects.toBeInstanceOf(TypeError);
    }
    await client.start();
    await within(client.synced);
    expect(h.service.table.size).toBe(0);
  });

  test('a re-registration refused on a live channel is retried', async () => {
    // Refuses the second REGISTER once.
    let registers = 0;
    const scripted = new ScriptedConnector((m) => {
      if (m.procedureName !== 'REGISTER') {
        return;
      }
      registers++;
      if (registers === 2) {
        void m.replyAbort(Status.of(StatusCode.internal, 'try again'));
      } else {
        const id = RegisterRequest.decode(m.payload).requestedInstance;
        void m.reply(new RegisterResponse(id === 0 ? 1 : id).encode());
      }
    });
    onTestFinished(() => scripted.close());
    const client = new NamingClient(scripted.call, { reconnectDelay });
    clients.push(client);
    const assigned: number[] = [];
    await client.start();
    expect(
      await client.register(Name.of('npc'), [uriA], { onAssigned: (id) => assigned.push(id) }),
    ).toBe(1);
    await within(client.synced);
    await nth(scripted.servers, 0).close();
    // Reconnected; the re-registration is refused, then retried on the
    // same channel after the reconnect delay.
    await until(() => registers === 3);
    await delay(reconnectDelay * 2);
    expect(registers).toBe(3);
    expect(scripted.servers).toHaveLength(2);
    expect(client.isConnected).toBe(true);
    expect(client.isSynced).toBe(true);
    expect(assigned).toEqual([1]);
  });

  test('a relayed CONNECT whose dial-back fails is answered UNAVAILABLE', async () => {
    const scripted = new ScriptedConnector(() => {});
    onTestFinished(() => scripted.close());
    const client = new NamingClient(scripted.call, { reconnectDelay });
    clients.push(client);
    const failures: unknown[] = [
      SwitchboardError.of(StatusCode.failedPrecondition, 'left mesh'),
      SwitchboardError.of(StatusCode.deadlineExceeded, 'no answer'),
      SwitchboardError.of(StatusCode.unauthenticated, 'refused'),
      new Error('bug'),
    ];
    let calls = 0;
    client.connectHandler = async () => {
      throw failures[calls++];
    };
    await client.start();
    await within(client.synced);
    expect(scripted.servers).toHaveLength(1);
    const server = nth(scripted.servers, 0);
    for (let i = 0; i < failures.length; i++) {
      await rejectsWith(
        server.request('CONNECT', new DialBackRequest('consumer', uriA).encode(), {
          name: Procedures.connect,
        }),
        StatusCode.unavailable,
      );
    }
    expect(calls).toBe(failures.length);
  });

  test('a refused UNREGSTR drops the channel', async () => {
    const registered: string[] = [];
    const scripted = new ScriptedConnector((m) => {
      switch (m.procedureName) {
        case 'REGISTER': {
          const request = RegisterRequest.decode(m.payload);
          registered.push(request.type.toString());
          const id = request.requestedInstance;
          void m.reply(new RegisterResponse(id === 0 ? registered.length : id).encode());
          break;
        }
        case 'UNREGSTR':
          void m.replyAbort(Status.of(StatusCode.internal, 'cannot'));
      }
    });
    onTestFinished(() => scripted.close());
    const client = new NamingClient(scripted.call, { reconnectDelay });
    clients.push(client);
    await client.start();
    await client.register(Name.of('api'), [uriB]);
    const id = await client.register(Name.of('npc'), [uriA]);
    await within(client.synced);
    // The record may still be registered: the channel goes, and the record
    // with it.
    await rejectsWith(client.unregister(Name.of('npc'), id), StatusCode.internal);
    await until(() => scripted.servers.length === 2 && client.isSynced);
    expect(nth(scripted.servers, 0).isOpen).toBe(false);
    // Only the remembered registration is made again.
    expect(registered).toEqual(['api', 'npc', 'api']);
  });

  test('a REGISTER without an answer drops the channel: no phantom', async () => {
    const lossy = new LossyConnector(h);
    const client = new NamingClient(lossy.call, { reconnectDelay });
    clients.push(client);
    const published: string[] = [];
    h.service.on('event', (e) => published.push(describeEvent(e)));
    await client.start();
    const kept = await client.register(Name.of('api'), [uriB]);
    await within(client.synced);
    // The naming service registers it; the answer is lost.
    lossy.dropToClient = isFinalResponse;
    await rejectsWith(client.register(Name.of('npc'), [uriA]), StatusCode.deadlineExceeded);
    lossy.dropToClient = () => false;
    await until(() => lossy.calls === 2 && client.isSynced);
    expect(published).toEqual([
      `UP api/${hex(kept)} ${uriB}`,
      `UP npc/2 ${uriA}`,
      // Dropped with the channel, and the remembered one restored.
      `DOWN api/${hex(kept)}`,
      'DOWN npc/2',
      `UP api/${hex(kept)} ${uriB}`,
    ]);
    expect(addresses(h.service.table)).toEqual([`api/${hex(kept)}`]);
    expect(addresses(client.table)).toEqual([`api/${hex(kept)}`]);
    // Not remembered.
    await rejectsWith(client.unregister(Name.of('npc'), 2), StatusCode.notFound);
  });

  test('an UNREGSTR without an answer drops the channel', async () => {
    const lossy = new LossyConnector(h);
    const client = new NamingClient(lossy.call, { reconnectDelay });
    clients.push(client);
    await client.start();
    const kept = await client.register(Name.of('api'), [uriB]);
    const gone = await client.register(Name.of('npc'), [uriA]);
    await within(client.synced);
    // The request never reaches the naming service.
    lossy.dropToService = isRequestFor('UNREGSTR');
    await rejectsWith(client.unregister(Name.of('npc'), gone), StatusCode.deadlineExceeded);
    lossy.dropToService = () => false;
    await until(() => lossy.calls === 2 && client.isSynced);
    // The record went away with the channel and is not registered again.
    expect(addresses(h.service.table)).toEqual([`api/${hex(kept)}`]);
    expect(addresses(client.table)).toEqual([`api/${hex(kept)}`]);
  });

  test('the same id registered twice in flight: the later one wins', async () => {
    const client = newClient(connector);
    await client.start();
    await within(client.synced);
    const first = client.register(Name.of('npc'), [uriA], { instance: 7 });
    const second = client.register(Name.of('npc'), [uriB], { instance: 7 });
    expect(await first).toBe(7);
    expect(await second).toBe(7);
    expect(single(h.service.table).endpoints).toEqual([uriB]);
    await client.unregister(Name.of('npc'), 7);
    expect(h.service.table.size).toBe(0);
    await rejectsWith(client.unregister(Name.of('npc'), 7), StatusCode.notFound);
    // Nothing comes back after a reconnect.
    await last(connector.servers).close();
    await until(() => connector.calls === 2 && client.isSynced);
    expect(h.service.table.size).toBe(0);
  });

  test('registering a registered id again replaces it', async () => {
    const client = newClient(connector);
    await client.start();
    const id = await client.register(Name.of('npc'), [uriA]);
    const assigned: number[] = [];
    expect(
      await client.register(Name.of('npc'), [uriB], {
        instance: id,
        onAssigned: (i) => assigned.push(i),
      }),
    ).toBe(id);
    expect(assigned).toEqual([id]);
    expect(single(h.service.table).endpoints).toEqual([uriB]);
    // Only the newer registration is remembered.
    await last(connector.servers).close();
    await until(() => connector.calls === 2 && client.isSynced);
    expect(single(h.service.table).endpoints).toEqual([uriB]);
    await client.unregister(Name.of('npc'), id);
    expect(h.service.table.size).toBe(0);
  });

  test('a replacement refused before sending drops the old record', async () => {
    const limited = new Connector(h, { options: oneRequestAtATime });
    const client = newClient(limited);
    await client.start();
    await within(client.synced);
    const type = Name.of('npc');
    await client.register(type, [uriA], { instance: 1 });
    // This request holds the only outgoing request slot while a replacement
    // of the already registered record is attempted.
    const other = client.register(type, [uriA], { instance: 2 });
    await rejectsWith(client.register(type, [uriB], { instance: 1 }), StatusCode.resourceExhausted);
    await within(other);
    await until(() => !h.service.table.has(new ServiceAddress(type, 1).key));
    await until(() => limited.calls === 2 && client.isSynced);
    expect([...h.service.table.keys()]).toEqual([new ServiceAddress(type, 2).key]);
  });

  test('a refused replacement also retires an in-flight REGISTER', async () => {
    const limited = new Connector(h, { options: oneRequestAtATime });
    const client = newClient(limited);
    await client.start();
    await within(client.synced);
    const type = Name.of('npc');
    const first = client.register(type, [uriA], { instance: 1 });
    const failed = rejectsWith(first, StatusCode.resourceExhausted);
    await rejectsWith(client.register(type, [uriB], { instance: 1 }), StatusCode.resourceExhausted);
    await failed;
    await until(() => limited.calls === 2 && client.isSynced);
    expect(h.service.table.size).toBe(0);
  });

  test('a refused replacement of a registration never made keeps the channel', async () => {
    const [other] = h.link();
    expect(await register(other, 'npc', { instance: 5 })).toBe(5);
    const client = newClient(connector);
    connector.down = true;
    void client.start();
    await until(() => connector.calls >= 2);
    // Neither is sent before the next connect.
    const first = client.register(Name.of('npc'), [uriA], { instance: 5 });
    const firstFailed = rejectsWith(first, StatusCode.alreadyExists);
    const replacement = client.register(Name.of('npc'), [uriB], { instance: 5 });
    const failedCalls = connector.calls;
    connector.down = false;
    await rejectsWith(replacement, StatusCode.alreadyExists);
    await firstFailed;
    await within(client.synced);
    // Nothing of this client's was published: the channel stays.
    await delay(reconnectDelay * 5);
    expect(connector.calls).toBe(failedCalls + 1);
    expect(client.isSynced).toBe(true);
  });

  test('a replacement unregistered in flight, then refused, drops the old record', async () => {
    const h = new Harness({ assignmentHold: 1000 });
    onTestFinished(() => h.close());
    // On the first channel, the naming service takes one request at a time
    // from this client.
    let calls = 0;
    const client = new NamingClient(
      async () => {
        calls++;
        const [client, server] = talkPair({
          a: clientOptions,
          b: calls === 1 ? { replyTimeout: 200, maxIncomingRequests: 1 } : serverOptions,
        });
        h.service.serve(server);
        return client;
      },
      { reconnectDelay },
    );
    clients.push(client);
    await client.start();
    await within(client.synced);
    const type = Name.of('npc');
    await client.register(type, [uriA], { instance: 1 });
    // Held until the assignment hold ends: the service refuses what follows
    // on the channel meanwhile.
    const held = client.register(Name.of('api'), [uriA]);
    await pump();
    const replacement = client.register(type, [uriB], { instance: 1 });
    await client.unregister(type, 1);
    await rejectsWith(replacement, StatusCode.cancelled);
    await until(() => !h.service.table.has(new ServiceAddress(type, 1).key));
    // The held registration is made again on the next channel.
    await within(held, 3000);
    expect(calls).toBe(2);
    expect([...h.service.table.values()].map((r) => r.address.type.toString())).toEqual(['api']);
  });

  test(
    'register, unregister and register again of one id while the first REGISTER is in ' +
      'flight keeps the second record',
    async () => {
      const client = newClient(connector);
      await client.start();
      await within(client.synced);
      const published: string[] = [];
      h.service.on('event', (e) => published.push(describeEvent(e)));
      const type = Name.of('npc');
      const first = client.register(type, [uriA], { instance: 7 });
      const cancelled = rejectsWith(first, StatusCode.cancelled);
      await client.unregister(type, 7);
      expect(await client.register(type, [uriB], { instance: 7 })).toBe(7);
      await cancelled;
      await delay(ms50);
      // The first REGISTER's late answer sends no UNREGSTR: the second
      // REGISTER, after it on the channel, replaced its record.
      expect(published).toEqual([`UP npc/7 ${uriA}`, `UP npc/7 ${uriB}`]);
      expect(single(h.service.table).endpoints).toEqual([uriB]);
      expect(connector.calls).toBe(1);
      await client.unregister(type, 7);
      expect(h.service.table.size).toBe(0);
    },
  );

  test('a superseded registration given a new id does not stay published', async () => {
    const client = newClient(connector);
    await client.start();
    await within(client.synced);
    const type = Name.of('npc');
    const assigned: number[] = [];
    expect(
      await client.register(type, [uriA], { instance: 5, onAssigned: (id) => assigned.push(id) }),
    ).toBe(5);
    // The naming service restarts; meanwhile another channel takes 5.
    connector.down = true;
    const h2 = new Harness({ assignmentHold: 300 });
    onTestFinished(() => h2.close());
    connector.harness = h2;
    await h.service.close();
    await until(() => !client.isConnected);
    const [other] = h2.link();
    expect(await register(other, 'npc', { instance: 5 })).toBe(5);
    connector.down = false;
    // REGISTER 5 is refused, the REGISTER for any id that follows is held.
    await until(() => client.isConnected);
    await delay(ms50);
    // 5 is free again and the application registers it again: the held
    // registration is superseded, and its answer comes with another id.
    await unregister(other, 'npc', 5);
    expect(await client.register(type, [uriB], { instance: 5 })).toBe(5);
    await delay(400);
    expect([...h2.service.table.keys()]).toEqual([new ServiceAddress(type, 5).key]);
    expect(single(h2.service.table).endpoints).toEqual([uriB]);
    expect(assigned).toEqual([5]);
  });

  test(
    'a remembered registration waiting to be made again keeps its endpoints when its id goes ' +
      'to a new registration',
    async () => {
      const limited = new Connector(h, {
        options: { replyTimeout: 200, requestTimeout: 300, maxOutgoingRequests: 2 },
      });
      const client = new NamingClient(limited.call, { reconnectDelay: 300 });
      clients.push(client);
      await client.start();
      await within(client.synced);
      const type = Name.of('npc');
      const uris = [1, 2, 3].map((i) => `tcp://10.0.0.${i}:9000`);
      const [uri1, uri2, uri3] = uris as [string, string, string];
      const third: number[] = [];
      expect(await client.register(type, [uri1])).toBe(1);
      expect(await client.register(type, [uri2])).toBe(2);
      expect(await client.register(type, [uri3], { onAssigned: (id) => third.push(id) })).toBe(3);
      // A fresh naming service: 1 and 2 are registered again, 3 waits for
      // its retry (the channel sends two requests at a time).
      const h2 = new Harness();
      onTestFinished(() => h2.close());
      limited.harness = h2;
      await h.service.close();
      await until(() => h2.service.table.size === 2);
      expect(await client.register(type, [uriB])).toBe(3);
      await until(() => h2.service.table.size === 4);
      const published = new Map(
        [...h2.service.table.values()].map((r) => [r.address.instance, r.endpoints]),
      );
      expect(published).toEqual(
        new Map([
          [1, [uri1]],
          [2, [uri2]],
          [3, [uriB]],
          [4, [uri3]],
        ]),
      );
      expect(third).toEqual([3, 4]);
    },
  );

  test(
    'close and unregister of superseded registrations whose futures were dropped report no ' +
      'error',
    async () => {
      const client = newClient(connector);
      connector.down = true;
      void client.start();
      await until(() => connector.calls >= 1);
      const type = Name.of('npc');
      for (const instance of [8, 9]) {
        // Superseded: its future fails like the next one's, cancelled.
        void client.register(type, [uriA], { instance });
        client.register(type, [uriB], { instance }).catch(() => {});
      }
      await client.unregister(type, 8);
      await client.close();
      await pump();
    },
  );

  test('close completes while a listener of events is paused', async () => {
    const client = newClient(connector);
    await client.start();
    await within(client.synced);
    const closed = new Deferred<void>();
    const listening = (async () => {
      for await (const _ of client.events) {
        // The iteration is paused while this runs.
        await within(client.close());
        closed.resolve();
      }
    })();
    // Its UP comes first, and closing cancels the REGISTER.
    client.register(Name.of('npc'), [uriA]).catch(() => {});
    await within(closed.promise);
    await within(listening);
  });

  test('unregister while the REGISTER is in flight', async () => {
    const client = newClient(connector);
    await client.start();
    await within(client.synced);
    const published: string[] = [];
    h.service.on('event', (e) => published.push(describeEvent(e)));
    const registered = client.register(Name.of('npc'), [uriA], { instance: 7 });
    // Completes at once: nothing is registered on the channel yet.
    await client.unregister(Name.of('npc'), 7);
    await rejectsWith(registered, StatusCode.cancelled);
    // The naming service registered it anyway; the late answer is followed
    // by an UNREGSTR.
    await until(() => published.length === 2);
    expect(published).toEqual([`UP npc/7 ${uriA}`, 'DOWN npc/7']);
    expect(h.service.table.size).toBe(0);
    await last(connector.servers).close();
    await until(() => connector.calls === 2 && client.isSynced);
    expect(h.service.table.size).toBe(0);
  });

  test('resync: a record whose endpoints changed meanwhile is UP', async () => {
    const [raw] = h.link();
    const id = await register(raw, 'api', { endpoints: [uriA] });
    const client = newClient(connector);
    await client.start();
    await within(client.synced);
    const events: string[] = [];
    client.on('event', (e) => events.push(describeEvent(e)));
    connector.down = true;
    await last(connector.servers).close();
    await until(() => !client.isConnected);
    await register(raw, 'api', { instance: id, endpoints: [uriB] });
    connector.down = false;
    await within(client.synced);
    expect(events).toEqual([`UP api/${hex(id)} ${uriB}`]);
    expect(single(client.table).endpoints).toEqual([uriB]);
  });

  test('close leaves no timers, connected or reconnecting', async () => {
    const clock = new TestClock();
    const h = new Harness({ clock });
    const connector = new Connector(h);
    const client = new NamingClient(connector.call, { reconnectDelay, clock });
    void client.start();
    void client.register(Name.of('npc'), [uriA]);
    await clock.elapse(ms10);
    expect(client.isSynced).toBe(true);
    await clock.elapse(1000);
    expect(client.isSynced).toBe(true);
    // Lose the service and close during the reconnect delay.
    connector.down = true;
    void last(connector.servers).close();
    await clock.elapse(reconnectDelay * 3);
    expect(client.isConnected).toBe(false);
    void client.close();
    await pump();
    expect(clock.pendingCount).toBe(0);

    // Connected.
    const second = new NamingClient(new Connector(h).call, { reconnectDelay, clock });
    void second.start();
    await clock.elapse(ms10);
    expect(second.isSynced).toBe(true);
    expect(h.service.watchCount).toBe(1);
    void second.close();
    await pump();
    expect(h.service.watchCount).toBe(0);
    expect(clock.pendingCount).toBe(0);
    void h.close();
    await pump();
    expect(clock.pendingCount).toBe(0);
  });
});

/** The client's requester timeout, which the idle subscription outlives. */
const clientRequestTimeout = clientOptions.requestTimeout as number;

/**
 * Default Talk options but one outgoing request at a time (the harness
 * links start from {@link clientOptions}, so the default timeouts are given).
 */
const oneRequestAtATime = {
  requestTimeout: defaultTalkOptions.requestTimeout,
  replyTimeout: defaultTalkOptions.replyTimeout,
  maxOutgoingRequests: 1,
};

/**
 * A `TalkConnector` to a scripted naming service: `WATCH` is answered at
 * once (an empty snapshot); every other request goes to [handle].
 */
class ScriptedConnector {
  readonly handle: (request: TalkMessage) => void;
  /** Server sides of the links handed out. */
  readonly servers: LinkChannel[] = [];

  constructor(handle: (request: TalkMessage) => void) {
    this.handle = handle;
  }

  readonly call = async (): Promise<TalkChannel> => {
    const [client, server] = talkPair({ a: clientOptions, b: serverOptions });
    this.servers.push(server);
    void (async () => {
      try {
        for await (const m of server.messages) {
          if (m.procedureName === 'WATCH') {
            void m.reply(new Uint8Array(0));
          } else {
            this.handle(m);
          }
        }
      } catch {
        // The channel ended.
      }
    })();
    return client;
  };

  async close(): Promise<void> {
    await Promise.all(this.servers.map((server) => server.close()));
  }
}

/**
 * A connection that never identifies until {@link changeIdentity}, and
 * counts the listeners waiting for its next `IDENT`.
 */
class IdentityEvents implements NamingConnection {
  #event = new Deferred<void>();
  /** Listeners on the current {@link identityChanged}. */
  waiting = 0;
  readonly isOpen = true;
  readonly peerIdentity: PeerIdentity | undefined = undefined;

  /** A valid `IDENT` (of nobody, as far as {@link peerIdentity} goes). */
  changeIdentity(): void {
    const event = this.#event;
    this.#event = new Deferred<void>();
    this.waiting = 0;
    event.resolve();
  }

  get identityChanged(): Promise<void> {
    return new Counted(this.#event.promise, () => {
      this.waiting++;
    });
  }

  goAway(): Promise<void> {
    return Promise.resolve();
  }
}

/** [inner], calling [onListen] for every listener. */
class Counted implements Promise<void> {
  readonly [Symbol.toStringTag] = 'Promise';
  readonly #inner: Promise<undefined>;
  readonly #onListen: () => void;

  constructor(inner: Promise<void>, onListen: () => void) {
    this.#inner = inner.then(() => undefined);
    this.#onListen = onListen;
  }

  // biome-ignore lint/suspicious/noThenProperty: a Promise of its own, counting its listeners.
  then<R1 = void, R2 = never>(
    onfulfilled?: ((value: undefined) => R1 | PromiseLike<R1>) | null,
    onrejected?: ((reason: unknown) => R2 | PromiseLike<R2>) | null,
  ): Promise<R1 | R2> {
    this.#onListen();
    return this.#inner.then(onfulfilled, onrejected);
  }

  catch<R = never>(
    onrejected?: ((reason: unknown) => R | PromiseLike<R>) | null,
  ): Promise<undefined | R> {
    return this.then(undefined, onrejected);
  }

  finally(onfinally?: (() => void) | null): Promise<undefined> {
    return this.#inner.finally(onfinally);
  }
}

/** [value] as UTF-8 bytes. */
function text(value: string): Uint8Array {
  return utf8Encode(value);
}

/** [id] as a service address prints it. */
function hex(id: number): string {
  return id.toString(16);
}

function address(type: string, instance: number): ServiceAddress {
  return new ServiceAddress(Name.of(type), instance);
}

/** The addresses of [table] as text, in table order. */
function addresses(table: ReadonlyMap<string, ServiceRecord>): string[] {
  return [...table.values()].map((r) => r.address.toString());
}

/** The one record of [table]. */
function single(table: ReadonlyMap<string, ServiceRecord>): ServiceRecord {
  expect(table.size).toBe(1);
  return [...table.values()][0] as ServiceRecord;
}

/** The record of [at] in [table]. */
function recordOf(table: ReadonlyMap<string, ServiceRecord>, at: ServiceAddress): ServiceRecord {
  const record = table.get(at.key);
  expect(record, `${at}`).toBeDefined();
  return record as ServiceRecord;
}

function nth<T>(list: readonly T[], index: number): T {
  expect(list.length).toBeGreaterThan(index);
  return list[index] as T;
}

function last<T>(list: readonly T[]): T {
  return nth(list, list.length - 1);
}

/** Every value of [iterable] until it ends; iteration starts at once. */
async function collect<T>(iterable: AsyncIterable<T>): Promise<T[]> {
  const out: T[] = [];
  for await (const value of iterable) {
    out.push(value);
  }
  return out;
}

/** [frame], which must be a message (a request, a response or a one-way message). */
function messageFrame(frame: FakeFrame | undefined): Extract<FakeFrame, { kind: 'message' }> {
  expect(frame?.kind).toBe('message');
  return frame as Extract<FakeFrame, { kind: 'message' }>;
}

/** Checks that [error] is a `SwitchboardError` of [code] (the Dart tests' `isStatus`). */
function expectStatus(error: unknown, code: StatusCode): void {
  expect(error).toBeInstanceOf(SwitchboardError);
  expect(statusCodeName((error as SwitchboardError).code)).toBe(statusCodeName(code));
}
